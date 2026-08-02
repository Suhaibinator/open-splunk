package plan

import (
	"errors"
	"fmt"
	"math"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/Suhaibinator/open-splunk/internal/eventfields"
	"github.com/Suhaibinator/open-splunk/internal/ianatimezone"
	"github.com/Suhaibinator/open-splunk/internal/searchtimebounds"
	"github.com/Suhaibinator/open-splunk/internal/spl"
	"github.com/Suhaibinator/open-splunk/internal/splpath"
	"github.com/Suhaibinator/open-splunk/internal/splregex"
	"github.com/Suhaibinator/open-splunk/internal/splrelativetime"
	"github.com/Suhaibinator/open-splunk/internal/spltimeformat"
	"github.com/Suhaibinator/open-splunk/internal/splwildcard"
)

const (
	// Ingestion permits a leaf below 16 nested objects: 17 path segments of 256
	// bytes each. Dots and backslashes require one escape byte in SPL, so the
	// full query spelling can be at most 17*(2*256)+16 separators.
	maxFieldNameBytes        = eventfields.MaximumNormalizedFieldNameBytes
	maxFieldPathSegments     = 17
	maxFieldPathSegmentBytes = 256
	maxTimechartBuckets      = 10_000
	maxTimechartSpan         = 24 * time.Hour
	timechartSeriesLimit     = 10
	maxTimechartSeries       = 12
	// Chart's row axis is runtime data rather than a plan-time constant, so
	// it carries its own explicit ceiling. Splunk truncates at the
	// installation-configurable maxresultrows; this backend instead fails
	// atomically, exactly as the timechart bucket ceiling does.
	maxChartRows = 10_000
	// Splunk documents chart's column-series defaults as limit=top 10 with
	// useother=true and usenull=true, so at most twelve series exist.
	chartSeriesLimit             = 10
	maxChartSeries               = 12
	maxDedupFields               = 16
	maxExtractionOutputsPerQuery = 64
	// Parsed eval/where trees are already bounded by the 1,024-token source
	// and 32 predicate leaves. Revalidate a wider occurrence/depth budget at
	// the AST trust boundary so forged trees and compact shared DAGs cannot
	// drive unbounded recursive conversion.
	maxConvertedExpressionNodes = 2048
	maxConvertedExpressionDepth = 1024
)

// Scope is the server-resolved security and snapshot boundary for a search.
// AuthorizedIndexes must come from trusted control-plane state, never SPL.
type Scope struct {
	TenantID          string
	AuthorizedIndexes []string
	RequestedIndexes  []string
	Earliest          time.Time
	Latest            time.Time
	// SearchStart is the immutable server clock value captured when the search
	// is admitted. It must not be derived from either storage cutoff below.
	SearchStart     time.Time
	SearchTimezone  string
	IndexTimeCutoff time.Time
	// VisibilityCutoff must be resolved by the storage writer when the search
	// job starts. A pointer distinguishes an empty-table cutoff of zero from a
	// caller that forgot to establish an immutable snapshot.
	VisibilityCutoff *uint64
}

// Build performs semantic analysis and emits a security-constrained plan.
func Build(query *spl.Query, scope Scope) (*Query, error) {
	if query == nil {
		return nil, &Diagnostic{Code: "SPL_INVALID_QUERY", Message: "query is nil"}
	}
	rexPatterns, err := compileRexPatterns(query)
	if err != nil {
		return nil, err
	}
	indexes, err := resolveIndexes(scope, query, rexPatterns)
	if err != nil {
		return nil, err
	}
	if scope.TenantID == "" {
		return nil, &Diagnostic{Code: "SPL_INVALID_SCOPE", Message: "tenant scope is empty", Range: query.Range}
	}
	earliest := scope.Earliest.UTC()
	latest := scope.Latest.UTC()
	searchStart := scope.SearchStart.UTC()
	cutoff := scope.IndexTimeCutoff.UTC()
	if earliest.IsZero() || latest.IsZero() || !earliest.Before(latest) {
		return nil, &Diagnostic{Code: "SPL_INVALID_TIME_RANGE", Message: "time range must be a non-empty half-open interval", Range: query.Range}
	}
	if cutoff.IsZero() {
		return nil, &Diagnostic{Code: "SPL_INVALID_TIME_RANGE", Message: "index-time cutoff is required", Range: query.Range}
	}
	if searchStart.IsZero() {
		return nil, &Diagnostic{Code: "SPL_INVALID_SEARCH_START", Message: "search-start anchor is required", Range: query.Range}
	}
	if _, err := ianatimezone.Load(scope.SearchTimezone); err != nil {
		return nil, &Diagnostic{
			Code:    "SPL_INVALID_SEARCH_TIMEZONE",
			Message: "effective search timezone is invalid",
			Range:   query.Range,
		}
	}
	if scope.VisibilityCutoff == nil {
		return nil, &Diagnostic{Code: "SPL_INVALID_SNAPSHOT", Message: "storage visibility cutoff is required", Range: query.Range}
	}

	result := &Query{
		EffectiveIndexes: indexes,
		SearchStart:      searchStart,
		SearchTimezone:   strings.Clone(scope.SearchTimezone),
	}
	result.Operators = append(result.Operators, &Scan{
		TenantID:         scope.TenantID,
		Indexes:          append([]string(nil), indexes...),
		Earliest:         earliest,
		Latest:           latest,
		IndexTimeCutoff:  cutoff,
		VisibilityCutoff: *scope.VisibilityCutoff,
		Range:            query.Range,
	})
	if query.Search != nil {
		expression, convertErr := convertExpression(query.Search)
		if convertErr != nil {
			return nil, convertErr
		}
		result.Operators = append(result.Operators, &Filter{Expression: expression, Range: query.Search.SourceRange()})
	}

	outputSchemaKnown := false
	canonicalTimeAvailable := true
	extractionOutputCount := 0
	spathEvaluationWorkUnits := 0
	expressionBudget := splExpressionResourceBudget{}
	for commandIndex, command := range query.Commands {
		switch command := command.(type) {
		case *spl.SearchCommand:
			expression, convertErr := convertExpression(command.Expression)
			if convertErr != nil {
				return nil, convertErr
			}
			result.Operators = append(result.Operators, &Filter{Expression: expression, Range: command.Range})
		case *spl.WhereCommand:
			expression, convertErr := convertWhereExpression(
				command.Expression,
				&expressionBudget,
			)
			if convertErr != nil {
				return nil, convertErr
			}
			result.Operators = append(result.Operators, &Filter{Expression: expression, Range: command.Range})
		case *spl.EvalCommand:
			assignments := make([]ExtendAssignment, 0, len(command.Assignments))
			for _, assignment := range command.Assignments {
				if complexityErr := validateSPLScalarExpressionComplexity(
					assignment.Expression,
					&expressionBudget,
				); complexityErr != nil {
					return nil, complexityErr
				}
				if splScalarMayReturnBooleanFunction(assignment.Expression) {
					return nil, &Diagnostic{
						Code:    "SPL_UNSUPPORTED_EVAL_EXPRESSION",
						Message: "search-mode eval cannot directly assign a Boolean result",
						Range:   assignment.Expression.SourceRange(),
					}
				}
				output, fieldErr := ResolveField(assignment.Field, assignment.FieldRange)
				if fieldErr != nil {
					return nil, fieldErr
				}
				expression, expressionErr := convertScalarExpressionUnchecked(
					assignment.Expression,
				)
				if expressionErr != nil {
					return nil, expressionErr
				}
				assignments = append(assignments, ExtendAssignment{
					Output:     output,
					Expression: expression,
					Range:      assignment.Range,
				})
				if outputSchemaKnown && !slices.Contains(result.OutputFields, assignment.Field) {
					result.OutputFields = append(result.OutputFields, assignment.Field)
				}
				if assignment.Field == "_time" {
					canonicalTimeAvailable = false
				}
			}
			result.Operators = append(result.Operators, &Extend{Assignments: assignments, Range: command.Range})
		case *spl.RexCommand:
			if command.MaxMatch != 1 {
				return nil, &Diagnostic{
					Code:        "SPL_UNSUPPORTED_REX_SYNTAX",
					Message:     "rex currently supports only the first match (max_match=1)",
					Range:       command.Range,
					Suggestions: []string{"omit max_match or use max_match=1"},
				}
			}
			compiled, ok := rexPatterns[command]
			if !ok {
				return nil, &Diagnostic{
					Code:    "SPL_INVALID_QUERY",
					Message: "rex pattern was not prepared",
					Range:   command.Range,
				}
			}
			extractionOutputCount += len(compiled.Captures)
			if extractionOutputCount > maxExtractionOutputsPerQuery {
				return nil, &Diagnostic{
					Code:    "SPL_QUERY_TOO_COMPLEX",
					Message: fmt.Sprintf("search creates more than %d extraction output fields", maxExtractionOutputsPerQuery),
					Range:   command.Range,
				}
			}
			if !outputSchemaKnown && command.Field == "fields" {
				return nil, &Diagnostic{
					Code:    "SPL_AMBIGUOUS_REX_FIELD",
					Message: "rex cannot read the event result's reserved fields payload without an exact upstream schema",
					Range:   command.FieldRange,
				}
			}
			input, inputErr := ResolveField(command.Field, command.FieldRange)
			if inputErr != nil {
				return nil, inputErr
			}
			captures := make([]ExtractCapture, 0, len(compiled.Captures))
			for _, capture := range compiled.Captures {
				if !outputSchemaKnown && capture.Name == "fields" {
					return nil, &Diagnostic{
						Code:    "SPL_AMBIGUOUS_REX_FIELD",
						Message: "rex cannot replace the event result's reserved fields payload without an exact upstream schema",
						Range:   command.PatternRange,
					}
				}
				output, outputErr := ResolveField(capture.Name, command.PatternRange)
				if outputErr != nil {
					return nil, outputErr
				}
				captures = append(captures, ExtractCapture{
					Output: output,
					// #nosec G115 -- validated rex patterns contain at most 16 capture groups.
					Group: uint16(capture.Group),
				})
				if outputSchemaKnown && !slices.Contains(result.OutputFields, capture.Name) {
					result.OutputFields = append(result.OutputFields, capture.Name)
				}
				if capture.Name == "_time" {
					canonicalTimeAvailable = false
				}
			}
			result.Operators = append(result.Operators, &Extract{
				Input:    input,
				Pattern:  compiled.Pattern,
				Captures: captures,
				Range:    command.Range,
			})
		case *spl.SpathCommand:
			if command == nil {
				return nil, &Diagnostic{Code: "SPL_INVALID_QUERY", Message: "spath command is nil"}
			}
			steps, pathErr := splpath.ParseJSON(command.Path)
			if pathErr != nil {
				code := "SPL_INVALID_QUERY"
				var bounded *splpath.Error
				if errors.As(pathErr, &bounded) && bounded.Kind == splpath.ErrorKindTooComplex {
					code = "SPL_QUERY_TOO_COMPLEX"
				}
				return nil, &Diagnostic{
					Code:    code,
					Message: "spath path metadata is invalid",
					Range:   command.PathRange,
				}
			}
			if !slices.Equal(steps, command.Steps) {
				return nil, &Diagnostic{
					Code:    "SPL_INVALID_QUERY",
					Message: "spath path metadata does not match its source",
					Range:   command.PathRange,
				}
			}
			spathEvaluationWorkUnits += splpath.EvaluationWorkUnits(steps)
			if spathEvaluationWorkUnits > splpath.MaximumEvaluationWorkUnits {
				return nil, &Diagnostic{
					Code: "SPL_QUERY_TOO_COMPLEX",
					Message: fmt.Sprintf(
						"spath stages require more than %d JSON evaluation work units per row",
						splpath.MaximumEvaluationWorkUnits,
					),
					Range: command.Range,
				}
			}
			extractionOutputCount++
			if extractionOutputCount > maxExtractionOutputsPerQuery {
				return nil, &Diagnostic{
					Code:    "SPL_QUERY_TOO_COMPLEX",
					Message: fmt.Sprintf("search creates more than %d extraction output fields", maxExtractionOutputsPerQuery),
					Range:   command.Range,
				}
			}
			if !outputSchemaKnown && (command.Input == "fields" || command.Output == "fields") {
				fieldRange := command.InputRange
				if command.Output == "fields" {
					fieldRange = command.OutputRange
				}
				return nil, &Diagnostic{
					Code:    "SPL_AMBIGUOUS_SPATH_FIELD",
					Message: "spath cannot use the event result's reserved fields payload without an exact upstream schema",
					Range:   fieldRange,
					Suggestions: []string{
						"select an exact ordinary field with table before spath",
						"produce a closed stats schema before using a field named fields",
					},
				}
			}
			input, inputErr := ResolveField(command.Input, command.InputRange)
			if inputErr != nil {
				return nil, inputErr
			}
			output, outputErr := ResolveField(command.Output, command.OutputRange)
			if outputErr != nil {
				return nil, outputErr
			}
			result.Operators = append(result.Operators, &ExtractJSON{
				Input:  input,
				Output: output,
				Path:   command.Path,
				Steps:  slices.Clone(steps),
				Range:  command.Range,
			})
			if outputSchemaKnown && !slices.Contains(result.OutputFields, command.Output) {
				result.OutputFields = append(result.OutputFields, command.Output)
			}
			if command.Output == "_time" {
				canonicalTimeAvailable = false
			}
		case *spl.BinCommand:
			if !outputSchemaKnown && command.Field == "fields" {
				return nil, &Diagnostic{
					Code:    "SPL_AMBIGUOUS_BIN_FIELD",
					Message: "bin cannot read the event result's reserved fields payload without an exact upstream schema",
					Range:   command.FieldRange,
					Suggestions: []string{
						"select an exact ordinary field with table before bin",
						"produce a closed stats schema before bin fields",
					},
				}
			}
			if !outputSchemaKnown && command.Output == "fields" {
				return nil, &Diagnostic{
					Code:    "SPL_AMBIGUOUS_BIN_FIELD",
					Message: "bin cannot replace the event result's reserved fields payload without an exact upstream schema",
					Range:   command.OutputRange,
					Suggestions: []string{
						"select an exact output schema with table before binning AS fields",
						"choose another bin output field",
					},
				}
			}
			input, inputErr := ResolveField(command.Field, command.FieldRange)
			if inputErr != nil {
				return nil, inputErr
			}
			output, outputErr := ResolveField(command.Output, command.OutputRange)
			if outputErr != nil {
				return nil, outputErr
			}

			switch command.Span.Kind {
			case spl.BinSpanKindNumeric, spl.BinSpanKindTime:
				if command.Field == "_time" {
					if !canonicalTimeAvailable {
						return nil, unsupportedBinTimeField(command.FieldRange)
					}
					span, spanErr := fixedBinSpan(command.Span)
					if spanErr != nil {
						return nil, spanErr
					}
					result.Operators = append(result.Operators, &TimeBucket{
						Field:  input,
						Output: output,
						Span:   span,
						Range:  command.Range,
					})
					break
				}
				if command.Span.Kind == spl.BinSpanKindTime {
					return nil, &Diagnostic{
						Code:        "SPL_UNSUPPORTED_BIN_TIME_FIELD",
						Message:     "bin spans with time units require the exact canonical _time field",
						Range:       command.FieldRange,
						Suggestions: []string{"use a unitless numeric span for non-time fields", "bin _time span=5m"},
					}
				}
				span, spanErr := fixedNumericBinSpan(command.Span)
				if spanErr != nil {
					return nil, spanErr
				}
				result.Operators = append(result.Operators, &NumericBucket{
					Input:  input,
					Output: output,
					Span:   span,
					Range:  command.Range,
				})
			default:
				return nil, &Diagnostic{
					Code:    "SPL_UNSUPPORTED_BIN_SYNTAX",
					Message: "bin span kind is invalid",
					Range:   command.Span.Range,
				}
			}

			if outputSchemaKnown && !slices.Contains(result.OutputFields, command.Output) {
				result.OutputFields = append(result.OutputFields, command.Output)
			}
			if command.Output == "_time" {
				canonicalTimeAvailable = false
			}
		case *spl.RenameCommand:
			assignments, renameErr := convertRenameAssignments(command)
			if renameErr != nil {
				return nil, renameErr
			}
			if !outputSchemaKnown {
				for index, assignment := range assignments {
					syntax := command.Assignments[index]
					if syntax.Source == "fields" || syntax.Destination == "fields" {
						return nil, &Diagnostic{
							Code:    "SPL_AMBIGUOUS_RENAME_FIELD",
							Message: "rename cannot use the event result's reserved fields payload without an exact upstream schema",
							Range:   syntax.Range,
						}
					}
					if (!assignment.Source.Canonical && len(assignment.Source.Path) != 1) ||
						(!assignment.Destination.Canonical && len(assignment.Destination.Path) != 1) {
						return nil, &Diagnostic{
							Code:        "SPL_UNSUPPORTED_RENAME_PATH",
							Message:     "rename on an open event schema currently supports top-level exact fields only",
							Range:       syntax.Range,
							Suggestions: []string{"select an exact schema with table before renaming a dotted output field"},
						}
					}
				}
			}
			if outputSchemaKnown {
				result.OutputFields = renameKnownOutputFields(result.OutputFields, command.Assignments)
			}
			for _, assignment := range command.Assignments {
				if assignment.Source == "_time" || assignment.Destination == "_time" {
					canonicalTimeAvailable = false
				}
			}
			result.Operators = append(result.Operators, &Rename{Assignments: assignments, Range: command.Range})
		case *spl.FieldsCommand:
			fields, fieldErr := convertFields(command.Fields, command.Range)
			if fieldErr != nil {
				return nil, fieldErr
			}
			mode := ProjectModeInclude
			if command.Exclude {
				mode = ProjectModeExclude
			}
			result.Operators = append(result.Operators, &Project{Mode: mode, Fields: fields, Range: command.Range})
			if outputSchemaKnown {
				result.OutputFields = projectKnownOutputFields(result.OutputFields, command.Fields, command.Exclude)
				if len(result.OutputFields) == 0 {
					return nil, &Diagnostic{
						Code:        "SPL_EMPTY_PROJECTION",
						Message:     "fields removes every column from the transforming result",
						Range:       command.Range,
						Suggestions: []string{"retain at least one stats or table output field"},
					}
				}
			}
			if command.Exclude && slices.Contains(command.Fields, "_time") {
				canonicalTimeAvailable = false
			}
		case *spl.TableCommand:
			fields, fieldErr := convertFields(command.Fields, command.Range)
			if fieldErr != nil {
				return nil, fieldErr
			}
			result.OutputFields = append([]string(nil), command.Fields...)
			outputSchemaKnown = true
			canonicalTimeAvailable = canonicalTimeAvailable && slices.Contains(command.Fields, "_time")
			result.Operators = append(result.Operators, &Project{Mode: ProjectModeTable, Fields: fields, Range: command.Range})
		case *spl.SortCommand:
			keys := make([]SortKey, 0, len(command.Fields))
			for _, field := range command.Fields {
				ref, fieldErr := ResolveField(field.Field, field.Range)
				if fieldErr != nil {
					return nil, fieldErr
				}
				keys = append(keys, SortKey{Field: ref, Descending: field.Descending})
			}
			limit := command.Limit
			if !command.LimitSpecified {
				limit = 10_000
			}
			result.Operators = append(result.Operators, &Sort{Keys: keys, Limit: limit, Range: command.Range})
		case *spl.DedupCommand:
			if command.Count == 0 || len(command.Fields) == 0 || len(command.Fields) > maxDedupFields {
				return nil, &Diagnostic{
					Code:    "SPL_UNSUPPORTED_DEDUP_SYNTAX",
					Message: "dedup requires a positive count and between 1 and 16 exact fields",
					Range:   command.Range,
				}
			}
			keys := make([]FieldRef, 0, len(command.Fields))
			seen := make(map[string]struct{}, len(command.Fields))
			for _, field := range command.Fields {
				if !outputSchemaKnown && field.Name == "fields" {
					return nil, &Diagnostic{
						Code:    "SPL_AMBIGUOUS_DEDUP_FIELD",
						Message: "dedup cannot use the event result's reserved fields payload without an exact upstream schema",
						Range:   field.Range,
						Suggestions: []string{
							"select an exact ordinary field with table before dedup",
							"produce a closed stats schema before dedup fields",
						},
					}
				}
				if _, duplicate := seen[field.Name]; duplicate {
					return nil, &Diagnostic{
						Code:    "SPL_UNSUPPORTED_DEDUP_SYNTAX",
						Message: fmt.Sprintf("dedup field %q is duplicated", field.Name),
						Range:   field.Range,
					}
				}
				seen[field.Name] = struct{}{}
				key, fieldErr := ResolveField(field.Name, field.Range)
				if fieldErr != nil {
					return nil, fieldErr
				}
				keys = append(keys, key)
			}
			result.Operators = append(result.Operators, &Deduplicate{Count: command.Count, Keys: keys, Range: command.Range})
		case *spl.LimitCommand:
			result.Operators = append(result.Operators, &Limit{Count: command.Count, FromEnd: command.Name() == "tail", Range: command.Range})
		case *spl.EventStatsCommand:
			if command == nil {
				return nil, &Diagnostic{
					Code:    "SPL_INVALID_QUERY",
					Message: "eventstats command is nil",
				}
			}
			if len(command.GroupBy) > spl.MaximumStatsGroupFields {
				return nil, &Diagnostic{
					Code: "SPL_QUERY_TOO_COMPLEX",
					Message: fmt.Sprintf(
						"eventstats BY contains more than %d grouping fields",
						spl.MaximumStatsGroupFields,
					),
					Range: command.Range,
				}
			}
			aggregate := command.Aggregate
			if aggregate.Alias == "" {
				return nil, &Diagnostic{
					Code: "SPL_UNSUPPORTED_EVENTSTATS_AGGREGATE",
					Message: "eventstats currently supports exactly one count, " +
						"count(field), count(eval(predicate)), min(field), max(field), sum(field), or avg(field) AS output measure",
					Range: aggregate.Range,
				}
			}
			measure := AggregateMeasure{Output: aggregate.Alias}
			switch aggregate.Function {
			case spl.AggregateFunctionCount:
				if aggregate.Input != "" ||
					aggregate.InputRange != (spl.Range{}) ||
					aggregate.Predicate != nil ||
					aggregate.Percentile != 0 ||
					(!aggregate.ExplicitAlias && aggregate.Alias != "count") {
					return nil, &Diagnostic{
						Code: "SPL_UNSUPPORTED_EVENTSTATS_AGGREGATE",
						Message: "eventstats row count requires no input " +
							"metadata and uses either its count default or an explicit alias",
						Range: aggregate.Range,
					}
				}
				measure.Function = AggregateFunctionCountRows
			case spl.AggregateFunctionCountValues:
				var inputErr error
				measure, inputErr = buildEventStatsFieldMeasure(
					aggregate,
					AggregateFunctionCountValues,
					"count(field)",
				)
				if inputErr != nil {
					return nil, inputErr
				}
			case spl.AggregateFunctionMinimum, spl.AggregateFunctionMaximum,
				spl.AggregateFunctionSum,
				spl.AggregateFunctionAverage:
				var function AggregateFunction
				var form string
				switch aggregate.Function {
				case spl.AggregateFunctionMinimum:
					function = AggregateFunctionMinimum
					form = "min(field)"
				case spl.AggregateFunctionMaximum:
					function = AggregateFunctionMaximum
					form = "max(field)"
				case spl.AggregateFunctionSum:
					function = AggregateFunctionSum
					form = "sum(field)"
				case spl.AggregateFunctionAverage:
					function = AggregateFunctionAverage
					form = "avg(field)"
				}
				var inputErr error
				measure, inputErr = buildEventStatsFieldMeasure(
					aggregate,
					function,
					form,
				)
				if inputErr != nil {
					return nil, inputErr
				}
			case spl.AggregateFunctionCountPredicate:
				predicateMeasure, predicateErr := buildCountPredicateMeasure(
					aggregate,
					outputSchemaKnown,
					&expressionBudget,
					countPredicateMeasureDiagnostics{
						unsupportedCode: "SPL_UNSUPPORTED_EVENTSTATS_AGGREGATE",
						invalidMessage: "eventstats count(eval(...)) requires one " +
							"predicate, an explicit alias, and no field or percentile metadata",
						ambiguousCode: "SPL_AMBIGUOUS_EVENTSTATS_FIELD",
						reservedMessage: "eventstats cannot read the event result's " +
							"reserved fields payload without an exact upstream schema",
					},
				)
				if predicateErr != nil {
					return nil, predicateErr
				}
				measure = predicateMeasure
			default:
				return nil, &Diagnostic{
					Code: "SPL_UNSUPPORTED_EVENTSTATS_AGGREGATE",
					Message: "eventstats currently supports exactly one count, " +
						"count(field), count(eval(predicate)), min(field), max(field), sum(field), or avg(field) AS output measure",
					Range: aggregate.Range,
				}
			}
			if !outputSchemaKnown {
				if aggregate.Alias == "fields" {
					return nil, &Diagnostic{
						Code: "SPL_AMBIGUOUS_EVENTSTATS_FIELD",
						Message: "eventstats cannot replace the event result's " +
							"reserved fields payload without an exact upstream schema",
						Range: aggregate.AliasRange,
					}
				}
				if aggregate.Input == "fields" {
					return nil, &Diagnostic{
						Code: "SPL_AMBIGUOUS_EVENTSTATS_FIELD",
						Message: "eventstats cannot read the event result's " +
							"reserved fields payload without an exact upstream schema",
						Range: aggregate.InputRange,
					}
				}
				for _, group := range command.GroupBy {
					if group.Name == "fields" {
						return nil, &Diagnostic{
							Code: "SPL_AMBIGUOUS_EVENTSTATS_FIELD",
							Message: "eventstats cannot group by the event result's " +
								"reserved fields payload without an exact upstream schema",
							Range: group.Range,
						}
					}
				}
			}
			if _, aliasErr := ResolveField(
				aggregate.Alias,
				aggregate.AliasRange,
			); aliasErr != nil {
				return nil, aliasErr
			}
			groupBy, groupErr := convertStatsGroupFields(
				"eventstats",
				command.GroupBy,
			)
			if groupErr != nil {
				return nil, groupErr
			}
			result.Operators = append(
				result.Operators,
				&EventAggregate{
					GroupBy: groupBy,
					Measure: measure,
					Range:   command.Range,
				},
			)
			if outputSchemaKnown &&
				!slices.Contains(result.OutputFields, aggregate.Alias) {
				result.OutputFields = append(
					result.OutputFields,
					aggregate.Alias,
				)
			}
			if aggregate.Alias == "_time" {
				canonicalTimeAvailable = false
			}
		case *spl.StatsCommand:
			if len(command.Aggregates) == 0 {
				return nil, &Diagnostic{
					Code:    "SPL_EXPECTED_AGGREGATE",
					Message: "stats requires an aggregate function",
					Range:   command.Range,
				}
			}
			if len(command.Aggregates) > spl.MaximumStatsMeasures {
				return nil, &Diagnostic{
					Code:    "SPL_QUERY_TOO_COMPLEX",
					Message: fmt.Sprintf("stats contains more than %d aggregate measures", spl.MaximumStatsMeasures),
					Range:   command.Range,
				}
			}
			if len(command.GroupBy) > spl.MaximumStatsGroupFields {
				return nil, &Diagnostic{
					Code:    "SPL_QUERY_TOO_COMPLEX",
					Message: fmt.Sprintf("stats BY contains more than %d grouping fields", spl.MaximumStatsGroupFields),
					Range:   command.Range,
				}
			}
			if !outputSchemaKnown {
				for _, aggregate := range command.Aggregates {
					if aggregate.Input == "fields" {
						return nil, &Diagnostic{
							Code:    "SPL_AMBIGUOUS_STATS_FIELD",
							Message: "stats cannot read the event result's reserved fields payload without an exact upstream schema",
							Range:   aggregate.InputRange,
						}
					}
				}
				for _, group := range command.GroupBy {
					if group.Name == "fields" {
						return nil, &Diagnostic{
							Code:    "SPL_AMBIGUOUS_STATS_FIELD",
							Message: "stats cannot group by the event result's reserved fields payload without an exact upstream schema",
							Range:   group.Range,
						}
					}
				}
			}
			groupBy, groupErr := convertStatsGroupFields(
				"stats",
				command.GroupBy,
			)
			if groupErr != nil {
				return nil, groupErr
			}
			seenOutputs := make(map[string]struct{}, len(groupBy)+len(command.Aggregates))
			outputFields := make([]string, 0, len(groupBy)+len(command.Aggregates))
			for _, group := range groupBy {
				seenOutputs[group.Name] = struct{}{}
				outputFields = append(outputFields, group.Name)
			}
			measures := make([]AggregateMeasure, 0, len(command.Aggregates))
			for _, aggregate := range command.Aggregates {
				if aggregate.Function != spl.AggregateFunctionCountPredicate &&
					aggregate.Predicate != nil {
					return nil, &Diagnostic{
						Code:    "SPL_UNSUPPORTED_STATS_AGGREGATE",
						Message: "only count(eval(...)) can contain predicate metadata",
						Range:   aggregate.Range,
					}
				}
				if aggregate.Function == spl.AggregateFunctionPercentile {
					if aggregate.Percentile < 1 || aggregate.Percentile > 99 {
						return nil, &Diagnostic{
							Code:    "SPL_UNSUPPORTED_STATS_AGGREGATE",
							Message: "percentile suffix must be an integer from 1 through 99",
							Range:   aggregate.Range,
						}
					}
				} else if aggregate.Percentile != 0 {
					return nil, &Diagnostic{
						Code:    "SPL_UNSUPPORTED_STATS_AGGREGATE",
						Message: "non-percentile stats aggregate contains percentile metadata",
						Range:   aggregate.Range,
					}
				}
				if _, aliasErr := ResolveField(aggregate.Alias, aggregate.AliasRange); aliasErr != nil {
					return nil, aliasErr
				}
				if _, duplicate := seenOutputs[aggregate.Alias]; duplicate {
					return nil, &Diagnostic{
						Code:    "SPL_DUPLICATE_FIELD",
						Message: fmt.Sprintf("aggregate output field %q is duplicated", aggregate.Alias),
						Range:   aggregate.AliasRange,
					}
				}
				seenOutputs[aggregate.Alias] = struct{}{}
				measure := AggregateMeasure{Output: aggregate.Alias}
				switch aggregate.Function {
				case spl.AggregateFunctionCount:
					if aggregate.Input != "" || aggregate.InputRange != (spl.Range{}) {
						return nil, &Diagnostic{
							Code:    "SPL_UNSUPPORTED_STATS_AGGREGATE",
							Message: "argument-free count cannot contain input metadata",
							Range:   aggregate.Range,
						}
					}
					measure.Function = AggregateFunctionCountRows
				case spl.AggregateFunctionCountPredicate:
					predicateMeasure, predicateErr := buildCountPredicateMeasure(
						aggregate,
						outputSchemaKnown,
						&expressionBudget,
						countPredicateMeasureDiagnostics{
							unsupportedCode: "SPL_UNSUPPORTED_STATS_AGGREGATE",
							invalidMessage:  "count(eval(...)) requires one predicate, an explicit alias, and no field or percentile metadata",
							ambiguousCode:   "SPL_AMBIGUOUS_STATS_FIELD",
							reservedMessage: "stats cannot read the event result's reserved fields payload without an exact upstream schema",
						},
					)
					if predicateErr != nil {
						return nil, predicateErr
					}
					measure = predicateMeasure
				case spl.AggregateFunctionCountValues:
					if aggregate.Input == "" || aggregate.InputRange == (spl.Range{}) {
						return nil, &Diagnostic{
							Code:    "SPL_UNSUPPORTED_STATS_AGGREGATE",
							Message: "count(field) requires one exact input field",
							Range:   aggregate.Range,
						}
					}
					input, inputErr := ResolveField(aggregate.Input, aggregate.InputRange)
					if inputErr != nil {
						return nil, inputErr
					}
					measure.Function = AggregateFunctionCountValues
					measure.Input = input
				case spl.AggregateFunctionPercentile, spl.AggregateFunctionSum,
					spl.AggregateFunctionAverage, spl.AggregateFunctionDistinctCount,
					spl.AggregateFunctionValues, spl.AggregateFunctionList,
					spl.AggregateFunctionMinimum,
					spl.AggregateFunctionMaximum,
					spl.AggregateFunctionEarliest,
					spl.AggregateFunctionLatest:
					if aggregate.Input == "" || aggregate.InputRange == (spl.Range{}) {
						return nil, &Diagnostic{
							Code:    "SPL_UNSUPPORTED_STATS_AGGREGATE",
							Message: "stats aggregate requires one exact input field",
							Range:   aggregate.Range,
						}
					}
					if (aggregate.Function == spl.AggregateFunctionEarliest ||
						aggregate.Function == spl.AggregateFunctionLatest) &&
						!canonicalTimeAvailable {
						return nil, &Diagnostic{
							Code:        "SPL_UNSUPPORTED_STATS_TIME_FIELD",
							Message:     "earliest and latest require the unmodified canonical _time field",
							Range:       aggregate.Range,
							Suggestions: []string{"run stats earliest or latest before removing, replacing, or transforming _time"},
						}
					}
					input, inputErr := ResolveField(aggregate.Input, aggregate.InputRange)
					if inputErr != nil {
						return nil, inputErr
					}
					measure.Input = input
					switch aggregate.Function {
					case spl.AggregateFunctionPercentile:
						measure.Function = AggregateFunctionPercentile
						measure.Percentile = aggregate.Percentile
					case spl.AggregateFunctionSum:
						measure.Function = AggregateFunctionSum
					case spl.AggregateFunctionAverage:
						measure.Function = AggregateFunctionAverage
					case spl.AggregateFunctionDistinctCount:
						measure.Function = AggregateFunctionDistinctCount
					case spl.AggregateFunctionValues:
						measure.Function = AggregateFunctionValues
					case spl.AggregateFunctionList:
						measure.Function = AggregateFunctionList
					case spl.AggregateFunctionMinimum:
						measure.Function = AggregateFunctionMinimum
					case spl.AggregateFunctionMaximum:
						measure.Function = AggregateFunctionMaximum
					case spl.AggregateFunctionEarliest:
						measure.Function = AggregateFunctionEarliest
					case spl.AggregateFunctionLatest:
						measure.Function = AggregateFunctionLatest
					}
				default:
					return nil, &Diagnostic{
						Code:    "SPL_UNSUPPORTED_STATS_AGGREGATE",
						Message: "unsupported stats aggregate",
						Range:   aggregate.Range,
					}
				}
				measures = append(measures, measure)
				outputFields = append(outputFields, aggregate.Alias)
			}
			result.OutputFields = outputFields
			outputSchemaKnown = true
			result.Operators = append(result.Operators, &Aggregate{
				GroupBy:  groupBy,
				Measures: measures,
				Range:    command.Range,
			})
			canonicalTimeAvailable = false
		case *spl.TopCommand, *spl.RareCommand:
			var commandName, fieldName string
			var fieldRange, commandRange spl.Range
			var limit uint64
			leastFrequent := false
			switch command := command.(type) {
			case *spl.TopCommand:
				commandName = command.Name()
				fieldName = command.Field
				fieldRange = command.FieldRange
				commandRange = command.Range
				limit = command.Limit
			case *spl.RareCommand:
				commandName = command.Name()
				fieldName = command.Field
				fieldRange = command.FieldRange
				commandRange = command.Range
				limit = command.Limit
				leastFrequent = true
			}
			canonicalTimeAvailable = false
			field, fieldErr := ResolveField(fieldName, fieldRange)
			if fieldErr != nil {
				return nil, fieldErr
			}
			if fieldName == "count" || fieldName == "percent" {
				return nil, &Diagnostic{
					Code:    "SPL_DUPLICATE_FIELD",
					Message: fmt.Sprintf("%s field %q collides with a generated output field", commandName, fieldName),
					Range:   fieldRange,
				}
			}
			countField, countErr := ResolveField("count", commandRange)
			if countErr != nil {
				return nil, countErr
			}
			result.OutputFields = []string{fieldName, "count", "percent"}
			outputSchemaKnown = true
			result.Operators = append(result.Operators,
				&Aggregate{
					GroupBy: []FieldRef{field},
					Measures: []AggregateMeasure{{
						Function: AggregateFunctionCountRows,
						Output:   "count",
					}},
					Range: commandRange,
				},
				&Window{
					Function: WindowFunctionPercentOfTotal,
					Input:    countField,
					Output:   "percent",
					Range:    commandRange,
				},
				&Sort{
					Keys: []SortKey{
						{Field: countField, Descending: !leastFrequent},
						{Field: field, Descending: true, Mode: SortValueModeLexical},
					},
					Limit: limit,
					Range: commandRange,
				},
			)
		case *spl.TimechartCommand:
			if command == nil {
				return nil, &Diagnostic{
					Code:    "SPL_INVALID_QUERY",
					Message: "timechart command is nil",
				}
			}
			if commandIndex+1 != len(query.Commands) {
				next := query.Commands[commandIndex+1]
				return nil, &Diagnostic{
					Code:        "SPL_UNSUPPORTED_TIMECHART_PIPELINE",
					Message:     "timechart must be the final pipeline command in this compatibility version",
					Range:       next.SourceRange(),
					Suggestions: []string{"move timechart to the final pipeline stage"},
				}
			}
			measure, measureErr := buildTimechartMeasure(command, outputSchemaKnown)
			if measureErr != nil {
				return nil, measureErr
			}
			if !canonicalTimeAvailable {
				return nil, &Diagnostic{
					Code:        "SPL_UNSUPPORTED_TIMECHART_TIME_FIELD",
					Message:     "timechart requires the unmodified canonical _time field",
					Range:       command.Range,
					Suggestions: []string{"run timechart before removing, replacing, or transforming _time"},
				}
			}
			span, spanErr := fixedTimechartSpan(command.Span)
			if spanErr != nil {
				return nil, spanErr
			}
			firstBucket, bucketCount, bucketErr := fixedTimechartBuckets(earliest, latest, span, command.Span.Range)
			if bucketErr != nil {
				return nil, bucketErr
			}
			timeField, timeErr := ResolveField("_time", command.Range)
			if timeErr != nil {
				return nil, timeErr
			}
			var split *TimechartSplit
			if command.SplitBy != nil {
				resolved, splitErr := ResolveField(
					command.SplitBy.Name,
					command.SplitBy.Range,
				)
				if splitErr != nil {
					return nil, splitErr
				}
				split = &TimechartSplit{
					Field:        resolved,
					SeriesLimit:  timechartSeriesLimit,
					IncludeNull:  true,
					IncludeOther: true,
					NullLabel:    "NULL",
					OtherLabel:   "OTHER",
				}
				result.OutputFields = nil
				result.DynamicOutput = &DynamicSeriesOutput{
					FixedFields: []string{"_time"},
					MaxSeries:   maxTimechartSeries,
				}
			} else {
				result.OutputFields = []string{"_time", measure.Output}
				result.DynamicOutput = nil
			}
			result.Operators = append(result.Operators, &Timechart{
				Time:           timeField,
				Split:          split,
				Measure:        measure,
				Span:           span,
				FirstBucket:    firstBucket,
				BucketCount:    bucketCount,
				FixedRange:     true,
				Continuous:     true,
				IncludePartial: true,
				Range:          command.Range,
			})
		case *spl.ChartCommand:
			if commandIndex+1 != len(query.Commands) {
				next := query.Commands[commandIndex+1]
				return nil, &Diagnostic{
					Code:        "SPL_UNSUPPORTED_CHART_PIPELINE",
					Message:     "chart must be the final pipeline command in this compatibility version",
					Range:       next.SourceRange(),
					Suggestions: []string{"move chart to the final pipeline stage"},
				}
			}
			if command.Function != spl.AggregateFunctionCount {
				return nil, &Diagnostic{
					Code:    "SPL_UNSUPPORTED_CHART_AGGREGATE",
					Message: "unsupported chart aggregate",
					Range:   command.AggregateRange,
				}
			}
			// usenull and useother default to true, so NULL and OTHER are
			// always reachable public column names. A field spelled like one
			// of them would collide with a series deterministically.
			for _, axis := range []spl.StatsGroupField{command.Over, command.SplitBy} {
				if axis.Name == "NULL" || axis.Name == "OTHER" {
					return nil, &Diagnostic{
						Code:        "SPL_UNSUPPORTED_CHART_FIELD_TYPE",
						Message:     fmt.Sprintf("%q and %q are reserved chart series names", "NULL", "OTHER"),
						Range:       axis.Range,
						Suggestions: []string{"rename the field before chart"},
					}
				}
				if !outputSchemaKnown && axis.Name == "fields" {
					return nil, &Diagnostic{
						Code:    "SPL_UNSUPPORTED_CHART_FIELD_TYPE",
						Message: "chart cannot use the event result's reserved fields payload without an exact upstream schema",
						Range:   axis.Range,
						Suggestions: []string{
							"select an exact ordinary field with table before chart",
							"produce a closed stats schema before charting fields",
						},
					}
				}
			}
			canonicalTimeAvailable = false
			over, overErr := ResolveField(command.Over.Name, command.Over.Range)
			if overErr != nil {
				return nil, overErr
			}
			splitBy, splitErr := ResolveField(command.SplitBy.Name, command.SplitBy.Range)
			if splitErr != nil {
				return nil, splitErr
			}
			if over.Name == splitBy.Name {
				return nil, &Diagnostic{
					Code:    "SPL_DUPLICATE_FIELD",
					Message: fmt.Sprintf("chart row and column field %q is repeated", splitBy.Name),
					Range:   command.SplitBy.Range,
				}
			}
			result.OutputFields = nil
			result.DynamicOutput = &DynamicSeriesOutput{
				FixedFields: []string{over.Name},
				MaxSeries:   maxChartSeries,
			}
			result.Operators = append(result.Operators, &Chart{
				Over:         over,
				SplitBy:      splitBy,
				Function:     AggregateFunctionCountRows,
				RowLimit:     maxChartRows,
				SeriesLimit:  chartSeriesLimit,
				IncludeNull:  true,
				IncludeOther: true,
				NullLabel:    "NULL",
				OtherLabel:   "OTHER",
				Range:        command.Range,
			})
		default:
			return nil, &Diagnostic{
				Code:    "SPL_UNSUPPORTED_COMMAND",
				Message: fmt.Sprintf("unsupported command %q", command.Name()),
				Range:   command.SourceRange(),
			}
		}
	}
	return result, nil
}

func buildTimechartMeasure(
	command *spl.TimechartCommand,
	outputSchemaKnown bool,
) (AggregateMeasure, error) {
	if command == nil {
		return AggregateMeasure{}, &Diagnostic{
			Code:    "SPL_INVALID_QUERY",
			Message: "timechart command is nil",
		}
	}
	aggregate := command.Aggregate
	if aggregate.Predicate != nil {
		return AggregateMeasure{}, &Diagnostic{
			Code:    "SPL_UNSUPPORTED_TIMECHART_AGGREGATE",
			Message: "timechart aggregate cannot contain predicate metadata",
			Range:   aggregate.Range,
		}
	}
	switch aggregate.Function {
	case spl.AggregateFunctionCount:
		if aggregate.Input != "" ||
			aggregate.InputRange != (spl.Range{}) ||
			aggregate.Percentile != 0 ||
			aggregate.Alias != "count" ||
			aggregate.ExplicitAlias {
			return AggregateMeasure{}, &Diagnostic{
				Code: "SPL_UNSUPPORTED_TIMECHART_AGGREGATE",
				Message: "timechart count must be argument-free and use its " +
					"unaliased count output",
				Range: aggregate.Range,
			}
		}
		return AggregateMeasure{
			Function: AggregateFunctionCountRows,
			Output:   "count",
		}, nil
	case spl.AggregateFunctionPercentile:
		if aggregate.Input == "" ||
			aggregate.InputRange == (spl.Range{}) ||
			aggregate.Percentile < 1 ||
			aggregate.Percentile > 99 ||
			aggregate.Alias == "" {
			return AggregateMeasure{}, &Diagnostic{
				Code: "SPL_UNSUPPORTED_TIMECHART_AGGREGATE",
				Message: "timechart percentile requires one exact input field, " +
					"an integer level from 1 through 99, and one output",
				Range: aggregate.Range,
			}
		}
		canonicalOutput := "perc" + strconv.Itoa(int(aggregate.Percentile)) +
			"(" + aggregate.Input + ")"
		return buildUnsplitTimechartFieldMeasure(
			command,
			AggregateFunctionPercentile,
			canonicalOutput,
			aggregate.Percentile,
			outputSchemaKnown,
		)
	case spl.AggregateFunctionSum, spl.AggregateFunctionAverage:
		if aggregate.Input == "" ||
			aggregate.InputRange == (spl.Range{}) ||
			aggregate.Percentile != 0 ||
			aggregate.Alias == "" {
			return AggregateMeasure{}, &Diagnostic{
				Code: "SPL_UNSUPPORTED_TIMECHART_AGGREGATE",
				Message: "timechart sum and average require one exact input " +
					"field, no percentile metadata, and one output",
				Range: aggregate.Range,
			}
		}
		function := AggregateFunctionSum
		canonicalName := "sum"
		if aggregate.Function == spl.AggregateFunctionAverage {
			function = AggregateFunctionAverage
			canonicalName = "avg"
		}
		return buildUnsplitTimechartFieldMeasure(
			command,
			function,
			canonicalName+"("+aggregate.Input+")",
			0,
			outputSchemaKnown,
		)
	default:
		return AggregateMeasure{}, &Diagnostic{
			Code:    "SPL_UNSUPPORTED_TIMECHART_AGGREGATE",
			Message: "unsupported timechart aggregate",
			Range:   aggregate.Range,
		}
	}
}

func buildUnsplitTimechartFieldMeasure(
	command *spl.TimechartCommand,
	function AggregateFunction,
	canonicalOutput string,
	percentile uint8,
	outputSchemaKnown bool,
) (AggregateMeasure, error) {
	aggregate := command.Aggregate
	if command.SplitBy != nil {
		return AggregateMeasure{}, &Diagnostic{
			Code:    "SPL_UNSUPPORTED_TIMECHART_SYNTAX",
			Message: "this timechart aggregate does not support a BY split field",
			Range:   command.SplitBy.Range,
		}
	}
	if !aggregate.ExplicitAlias && aggregate.Alias != canonicalOutput {
		return AggregateMeasure{}, &Diagnostic{
			Code:    "SPL_UNSUPPORTED_TIMECHART_AGGREGATE",
			Message: "unaliased timechart aggregate output must use its canonical name",
			Range:   aggregate.Range,
		}
	}
	if !outputSchemaKnown && aggregate.Input == "fields" {
		return AggregateMeasure{}, &Diagnostic{
			Code:    "SPL_AMBIGUOUS_TIMECHART_FIELD",
			Message: "timechart cannot read the event result's reserved fields payload without an exact upstream schema",
			Range:   aggregate.InputRange,
		}
	}
	input, inputErr := ResolveField(aggregate.Input, aggregate.InputRange)
	if inputErr != nil {
		return AggregateMeasure{}, inputErr
	}
	if _, outputErr := ResolveField(aggregate.Alias, aggregate.AliasRange); outputErr != nil {
		return AggregateMeasure{}, outputErr
	}
	if aggregate.Alias == "_time" {
		return AggregateMeasure{}, &Diagnostic{
			Code:    "SPL_DUPLICATE_FIELD",
			Message: "timechart aggregate output collides with the _time axis",
			Range:   aggregate.AliasRange,
		}
	}
	return AggregateMeasure{
		Function:   function,
		Input:      input,
		Percentile: percentile,
		Output:     aggregate.Alias,
	}, nil
}

func fixedTimechartSpan(span spl.TimeSpan) (time.Duration, error) {
	duration, err := fixedDurationSpan(
		span,
		"SPL_UNSUPPORTED_TIMECHART_SYNTAX",
		"timechart",
	)
	if err != nil {
		return 0, err
	}
	if duration > maxTimechartSpan {
		return 0, &Diagnostic{
			Code:        "SPL_UNSUPPORTED_TIMECHART_SYNTAX",
			Message:     "timechart spans greater than 24 hours are not supported",
			Range:       span.Range,
			Suggestions: []string{"use a fixed span from 1s through 24h"},
		}
	}
	return duration, nil
}

func fixedNumericBinSpan(span spl.BinSpan) (uint64, error) {
	if span.Kind != spl.BinSpanKindNumeric || span.Unit != spl.TimeSpanUnitInvalid {
		return 0, &Diagnostic{
			Code:    "SPL_UNSUPPORTED_BIN_SYNTAX",
			Message: "numeric bin spans must be unitless",
			Range:   span.Range,
		}
	}
	if span.Magnitude == 0 || span.Magnitude > MaximumNumericBinSpan {
		return 0, &Diagnostic{
			Code:    "SPL_NUMBER_OUT_OF_RANGE",
			Message: fmt.Sprintf("numeric bin span must be between 1 and %d", MaximumNumericBinSpan),
			Range:   span.Range,
		}
	}
	return span.Magnitude, nil
}

func fixedBinSpan(span spl.BinSpan) (time.Duration, error) {
	var unit spl.TimeSpanUnit
	switch span.Kind {
	case spl.BinSpanKindNumeric:
		if _, err := fixedNumericBinSpan(span); err != nil {
			return 0, err
		}
		unit = spl.TimeSpanUnitSecond
	case spl.BinSpanKindTime:
		unit = span.Unit
	default:
		return 0, &Diagnostic{
			Code:    "SPL_UNSUPPORTED_BIN_SYNTAX",
			Message: "bin span kind is invalid",
			Range:   span.Range,
		}
	}
	duration, err := fixedDurationSpan(
		spl.TimeSpan{
			Magnitude: span.Magnitude,
			Unit:      unit,
			Range:     span.Range,
		},
		"SPL_UNSUPPORTED_BIN_SYNTAX",
		"bin",
	)
	if err != nil {
		return 0, err
	}
	if duration >= 24*time.Hour {
		return 0, &Diagnostic{
			Code:    "SPL_UNSUPPORTED_BIN_SYNTAX",
			Message: "bin spans of one day or more require timezone-aware alignment",
			Range:   span.Range,
			Suggestions: []string{
				"use a fixed span shorter than 24 hours",
			},
		}
	}
	return duration, nil
}

func unsupportedBinTimeField(sourceRange spl.Range) *Diagnostic {
	return &Diagnostic{
		Code:        "SPL_UNSUPPORTED_BIN_TIME_FIELD",
		Message:     "bin requires the unmodified canonical _time field",
		Range:       sourceRange,
		Suggestions: []string{"run bin before removing, replacing, transforming, or previously binning _time"},
	}
}

func fixedDurationSpan(span spl.TimeSpan, syntaxCode, commandName string) (time.Duration, error) {
	var unit time.Duration
	switch span.Unit {
	case spl.TimeSpanUnitSecond:
		unit = time.Second
	case spl.TimeSpanUnitMinute:
		unit = time.Minute
	case spl.TimeSpanUnitHour:
		unit = time.Hour
	default:
		return 0, &Diagnostic{
			Code:    syntaxCode,
			Message: "unsupported " + commandName + " span unit",
			Range:   span.Range,
		}
	}
	// #nosec G115 -- unit is one of the positive time.Second, time.Minute, or time.Hour constants.
	maximumMagnitude := uint64(math.MaxInt64) / uint64(unit)
	if span.Magnitude == 0 || span.Magnitude > maximumMagnitude {
		return 0, &Diagnostic{
			Code:    "SPL_NUMBER_OUT_OF_RANGE",
			Message: commandName + " span is outside the supported duration range",
			Range:   span.Range,
		}
	}
	// #nosec G115 -- the maximumMagnitude check above proves the conversion and multiplication fit in int64.
	return time.Duration(int64(span.Magnitude)) * unit, nil
}

func fixedTimechartBuckets(earliest, latest time.Time, span time.Duration, sourceRange spl.Range) (time.Time, uint64, error) {
	spanSeconds := int64(span / time.Second)
	if spanSeconds <= 0 {
		return time.Time{}, 0, &Diagnostic{
			Code:    "SPL_INVALID_ARGUMENT",
			Message: "timechart span must be at least one second",
			Range:   sourceRange,
		}
	}
	firstSeconds := floorInt64(earliest.Unix(), spanSeconds) * spanSeconds
	deltaSeconds := latest.Unix() - firstSeconds
	if deltaSeconds < 0 {
		return time.Time{}, 0, &Diagnostic{
			Code:    "SPL_INVALID_TIME_RANGE",
			Message: "timechart range cannot be represented",
			Range:   sourceRange,
		}
	}
	// #nosec G115 -- deltaSeconds is nonnegative and spanSeconds is positive above.
	bucketCount := uint64(deltaSeconds / spanSeconds)
	if deltaSeconds%spanSeconds != 0 || latest.Nanosecond() != 0 {
		bucketCount++
	}
	if bucketCount == 0 {
		// Build has already established a non-empty search interval; retain a
		// defensive check so malformed plans cannot generate numbers(0).
		return time.Time{}, 0, &Diagnostic{
			Code:    "SPL_INVALID_TIME_RANGE",
			Message: "timechart requires a non-empty bucket range",
			Range:   sourceRange,
		}
	}
	if bucketCount > maxTimechartBuckets {
		return time.Time{}, 0, &Diagnostic{
			Code:    "SPL_QUERY_TOO_COMPLEX",
			Message: fmt.Sprintf("timechart produces more than %d fixed-range buckets", maxTimechartBuckets),
			Range:   sourceRange,
		}
	}
	return time.Unix(firstSeconds, 0).UTC(), bucketCount, nil
}

func floorInt64(value, divisor int64) int64 {
	quotient := value / divisor
	if value%divisor < 0 {
		quotient--
	}
	return quotient
}

func projectKnownOutputFields(current, requested []string, exclude bool) []string {
	requestedSet := make(map[string]struct{}, len(requested))
	for _, name := range requested {
		requestedSet[name] = struct{}{}
	}
	if exclude {
		result := make([]string, 0, len(current))
		for _, name := range current {
			if _, remove := requestedSet[name]; !remove {
				result = append(result, name)
			}
		}
		return result
	}

	available := make(map[string]struct{}, len(current))
	for _, name := range current {
		available[name] = struct{}{}
	}
	result := make([]string, 0, len(requested)+2)
	for _, name := range requested {
		if _, ok := available[name]; ok {
			result = append(result, name)
		}
	}
	for _, implicit := range []string{"_time", "_raw"} {
		if _, ok := available[implicit]; ok && !slices.Contains(result, implicit) {
			result = append(result, implicit)
		}
	}
	return result
}

func renameKnownOutputFields(current []string, assignments []spl.RenameAssignment) []string {
	result := append([]string(nil), current...)
	for _, assignment := range assignments {
		if !slices.Contains(result, assignment.Source) {
			// Splunk nulls an existing destination when the source is absent.
			// The column therefore remains part of a known result schema.
			continue
		}
		next := make([]string, 0, len(result))
		for _, name := range result {
			switch name {
			case assignment.Source:
				next = append(next, assignment.Destination)
			case assignment.Destination:
				// A present source replaces an existing destination.
			default:
				next = append(next, name)
			}
		}
		result = next
	}
	return result
}

func convertRenameAssignments(command *spl.RenameCommand) ([]RenameAssignment, error) {
	if command == nil || len(command.Assignments) == 0 {
		return nil, &Diagnostic{Code: "SPL_INVALID_RENAME", Message: "rename requires at least one assignment"}
	}
	result := make([]RenameAssignment, 0, len(command.Assignments))
	seenSources := make(map[string]struct{}, len(command.Assignments))
	seenDestinations := make(map[string]struct{}, len(command.Assignments))
	for _, assignment := range command.Assignments {
		if assignment.Source == assignment.Destination {
			return nil, &Diagnostic{
				Code:    "SPL_INVALID_RENAME",
				Message: fmt.Sprintf("rename source and destination are both %q", assignment.Source),
				Range:   assignment.Range,
			}
		}
		if _, duplicate := seenSources[assignment.Source]; duplicate {
			return nil, &Diagnostic{
				Code:    "SPL_DUPLICATE_RENAME_SOURCE",
				Message: fmt.Sprintf("rename source field %q is repeated", assignment.Source),
				Range:   assignment.SourceRange,
			}
		}
		if _, duplicate := seenDestinations[assignment.Destination]; duplicate {
			return nil, &Diagnostic{
				Code:    "SPL_DUPLICATE_RENAME_TARGET",
				Message: fmt.Sprintf("rename destination field %q is repeated", assignment.Destination),
				Range:   assignment.DestinationRange,
			}
		}
		source, err := ResolveField(assignment.Source, assignment.SourceRange)
		if err != nil {
			return nil, err
		}
		destination, err := ResolveField(assignment.Destination, assignment.DestinationRange)
		if err != nil {
			return nil, err
		}
		seenSources[assignment.Source] = struct{}{}
		seenDestinations[assignment.Destination] = struct{}{}
		result = append(result, RenameAssignment{
			Source:      source,
			Destination: destination,
			Range:       assignment.Range,
		})
	}
	return result, nil
}

func convertStatsGroupFields(
	commandName string,
	fields []spl.StatsGroupField,
) ([]FieldRef, error) {
	result := make([]FieldRef, 0, len(fields))
	seen := make(map[string]struct{}, len(fields))
	for _, field := range fields {
		if _, duplicate := seen[field.Name]; duplicate {
			return nil, &Diagnostic{
				Code: "SPL_DUPLICATE_FIELD",
				Message: fmt.Sprintf(
					"%s grouping field %q is repeated",
					commandName,
					field.Name,
				),
				Range: field.Range,
			}
		}
		seen[field.Name] = struct{}{}
		resolved, err := ResolveField(field.Name, field.Range)
		if err != nil {
			return nil, err
		}
		result = append(result, resolved)
	}
	return result, nil
}

func resolveIndexes(
	scope Scope,
	query *spl.Query,
	rexPatterns map[*spl.RexCommand]splregex.ExtractionPattern,
) ([]string, error) {
	authorized := normalizedSet(scope.AuthorizedIndexes)
	if len(authorized) == 0 {
		return nil, &Diagnostic{Code: "SPL_INDEX_FORBIDDEN", Message: "search is not authorized for any index", Range: query.Range}
	}

	effective := authorized
	if len(scope.RequestedIndexes) > 0 {
		effective = make(map[string]struct{}, len(scope.RequestedIndexes))
		for _, requested := range scope.RequestedIndexes {
			name := strings.TrimSpace(requested)
			if _, ok := authorized[name]; !ok {
				return nil, &Diagnostic{Code: "SPL_INDEX_FORBIDDEN", Message: fmt.Sprintf("index %q is outside the authorized scope", name), Range: query.Range}
			}
			effective[name] = struct{}{}
		}
	}

	references := positiveIndexReferences(query, rexPatterns)
	for _, reference := range references {
		if strings.Contains(reference.value, "*") {
			return nil, &Diagnostic{
				Code:    "SPL_UNSUPPORTED_INDEX_SELECTOR",
				Message: "wildcard index selectors are not supported in compatibility version 0.1",
				Range:   reference.sourceRange,
			}
		}
		if _, ok := authorized[reference.value]; !ok {
			return nil, &Diagnostic{Code: "SPL_INDEX_FORBIDDEN", Message: fmt.Sprintf("index %q is outside the authorized scope", reference.value), Range: reference.sourceRange}
		}
		if _, ok := effective[reference.value]; !ok {
			return nil, &Diagnostic{Code: "SPL_INDEX_FORBIDDEN", Message: fmt.Sprintf("index %q is outside the requested scope", reference.value), Range: reference.sourceRange}
		}
	}

	indexes := make([]string, 0, len(effective))
	for index := range effective {
		indexes = append(indexes, index)
	}
	sort.Strings(indexes)
	return indexes, nil
}

type indexReference struct {
	value       string
	sourceRange spl.Range
}

func positiveIndexReferences(
	query *spl.Query,
	rexPatterns map[*spl.RexCommand]splregex.ExtractionPattern,
) []indexReference {
	var references []indexReference
	collect := func(expression spl.Expr) {
		collectPositiveIndexReferences(expression, false, &references)
	}
	if query.Search != nil {
		collect(query.Search)
	}
	for _, command := range query.Commands {
		switch command := command.(type) {
		case *spl.EvalCommand:
			for _, assignment := range command.Assignments {
				if assignment.Field == "index" {
					return references
				}
			}
		case *spl.RexCommand:
			compiled := rexPatterns[command]
			for _, capture := range compiled.Captures {
				if capture.Name == "index" {
					return references
				}
			}
		case *spl.SpathCommand:
			if command != nil && command.Output == "index" {
				return references
			}
		case *spl.RenameCommand:
			for _, assignment := range command.Assignments {
				if assignment.Source == "index" || assignment.Destination == "index" {
					return references
				}
			}
		case *spl.EventStatsCommand:
			if command == nil || command.Aggregate.Alias == "index" {
				return references
			}
		case *spl.StatsCommand, *spl.TopCommand, *spl.RareCommand, *spl.TimechartCommand, *spl.ChartCommand:
			return references
		}
		if search, ok := command.(*spl.SearchCommand); ok {
			collect(search.Expression)
		}
	}
	return references
}

func compileRexPatterns(query *spl.Query) (map[*spl.RexCommand]splregex.ExtractionPattern, error) {
	patterns := make(map[*spl.RexCommand]splregex.ExtractionPattern)
	for _, command := range query.Commands {
		rex, ok := command.(*spl.RexCommand)
		if !ok {
			continue
		}
		compiled, err := compileRexPattern(rex)
		if err != nil {
			return nil, err
		}
		patterns[rex] = compiled
	}
	return patterns, nil
}

func compileRexPattern(command *spl.RexCommand) (splregex.ExtractionPattern, error) {
	if command == nil {
		return splregex.ExtractionPattern{}, &Diagnostic{
			Code:    "SPL_INVALID_QUERY",
			Message: "rex command is nil",
		}
	}
	compiled, err := splregex.CompileExtractionPattern(command.Pattern)
	if err == nil {
		return compiled, nil
	}
	code := "SPL_UNSUPPORTED_REGEX"
	message := "rex regular expression is outside the supported named-capture RE2-compatible subset"
	if splregex.IsExtractionComplexityError(err) {
		code = "SPL_QUERY_TOO_COMPLEX"
		message = "rex regular expression exceeds the supported pattern or capture-group limit"
	}
	return splregex.ExtractionPattern{}, &Diagnostic{
		Code:    code,
		Message: message,
		Range:   command.PatternRange,
	}
}

func collectPositiveIndexReferences(expression spl.Expr, negated bool, destination *[]indexReference) {
	switch expression := expression.(type) {
	case *spl.BinaryExpr:
		collectPositiveIndexReferences(expression.Left, negated, destination)
		collectPositiveIndexReferences(expression.Right, negated, destination)
	case *spl.NotExpr:
		collectPositiveIndexReferences(expression.Operand, !negated, destination)
	case *spl.ComparisonExpr:
		if !negated && expression.Field == "index" && expression.Op == spl.CompareOpEqual {
			*destination = append(*destination, indexReference{value: expression.Value.Text, sourceRange: expression.Range})
		}
	}
}

func normalizedSet(values []string) map[string]struct{} {
	result := make(map[string]struct{}, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value != "" {
			result[value] = struct{}{}
		}
	}
	return result
}

func convertExpression(expression spl.Expr) (Expression, error) {
	switch expression := expression.(type) {
	case *spl.BinaryExpr:
		left, err := convertExpression(expression.Left)
		if err != nil {
			return nil, err
		}
		right, err := convertExpression(expression.Right)
		if err != nil {
			return nil, err
		}
		op := BooleanOpAnd
		if expression.Op == spl.BoolOpOr {
			op = BooleanOpOr
		}
		return &BooleanExpression{Op: op, Left: left, Right: right, Range: expression.Range}, nil
	case *spl.NotExpr:
		operand, err := convertExpression(expression.Operand)
		if err != nil {
			return nil, err
		}
		return &NotExpression{Operand: operand, Range: expression.Range}, nil
	case *spl.TermExpr:
		return &TextExpression{Value: expression.Value, Quoted: expression.Quoted, Wildcard: strings.Contains(expression.Value, "*"), Range: expression.Range}, nil
	case *spl.ComparisonExpr:
		field, err := ResolveField(expression.Field, expression.Range)
		if err != nil {
			return nil, err
		}
		value, err := convertValue(expression.Value)
		if err != nil {
			return nil, err
		}
		return &ComparisonExpression{Field: field, Op: convertComparisonOp(expression.Op), Value: value, Range: expression.Range}, nil
	default:
		return nil, &Diagnostic{Code: "SPL_UNSUPPORTED_EXPRESSION", Message: fmt.Sprintf("unsupported expression type %T", expression), Range: expression.SourceRange()}
	}
}

func convertWhereExpression(
	expression spl.WhereExpr,
	budget *splExpressionResourceBudget,
) (Expression, error) {
	if err := validateSPLWhereExpressionComplexity(
		expression,
		budget,
	); err != nil {
		return nil, err
	}
	return convertWhereExpressionUnchecked(expression)
}

func convertWhereExpressionUnchecked(expression spl.WhereExpr) (Expression, error) {
	if expression == nil {
		return nil, &Diagnostic{
			Code:    "SPL_UNSUPPORTED_WHERE_EXPRESSION",
			Message: "where expression is missing",
		}
	}
	switch expression := expression.(type) {
	case *spl.WhereBoolExpr:
		if expression == nil {
			return nil, &Diagnostic{
				Code:    "SPL_UNSUPPORTED_WHERE_EXPRESSION",
				Message: "Boolean where expression is missing",
			}
		}
		left, err := convertWhereExpressionUnchecked(expression.Left)
		if err != nil {
			return nil, err
		}
		right, err := convertWhereExpressionUnchecked(expression.Right)
		if err != nil {
			return nil, err
		}
		var op BooleanOp
		switch expression.Op {
		case spl.BoolOpAnd:
			op = BooleanOpAnd
		case spl.BoolOpOr:
			op = BooleanOpOr
		default:
			return nil, &Diagnostic{
				Code:    "SPL_UNSUPPORTED_WHERE_EXPRESSION",
				Message: "where Boolean expression has an invalid operator",
				Range:   expression.Range,
			}
		}
		return &BooleanExpression{Op: op, Left: left, Right: right, Range: expression.Range}, nil
	case *spl.WhereNotExpr:
		if expression == nil {
			return nil, &Diagnostic{
				Code:    "SPL_UNSUPPORTED_WHERE_EXPRESSION",
				Message: "negated where expression is missing",
			}
		}
		operand, err := convertWhereExpressionUnchecked(expression.Operand)
		if err != nil {
			return nil, err
		}
		return &NotExpression{Operand: operand, Range: expression.Range}, nil
	case *spl.WhereComparisonExpr:
		if expression == nil {
			return nil, &Diagnostic{
				Code:    "SPL_UNSUPPORTED_WHERE_EXPRESSION",
				Message: "where comparison is missing",
			}
		}
		op := convertComparisonOp(expression.Op)
		if op == ComparisonOpInvalid {
			return nil, &Diagnostic{
				Code:    "SPL_UNSUPPORTED_WHERE_EXPRESSION",
				Message: "where comparison has an invalid operator",
				Range:   expression.Range,
			}
		}
		left, err := convertScalarExpressionUnchecked(expression.Left)
		if err != nil {
			return nil, err
		}
		right, err := convertScalarExpressionUnchecked(expression.Right)
		if err != nil {
			return nil, err
		}
		return &EvalComparisonExpression{
			Left:  left,
			Op:    op,
			Right: right,
			Range: expression.Range,
		}, nil
	case *spl.WhereScalarPredicateExpr:
		if expression == nil {
			return nil, &Diagnostic{
				Code:    "SPL_UNSUPPORTED_WHERE_EXPRESSION",
				Message: "where scalar predicate is missing",
			}
		}
		value, err := convertScalarExpressionUnchecked(expression.Value)
		if err != nil {
			return nil, err
		}
		if !scalarExpressionCanBeDirectPredicate(value) {
			return nil, &Diagnostic{
				Code:    "SPL_UNSUPPORTED_WHERE_EXPRESSION",
				Message: "where scalar predicate must return Boolean",
				Range:   expression.Range,
			}
		}
		return &ScalarPredicateExpression{Value: value, Range: expression.Range}, nil
	default:
		return nil, &Diagnostic{Code: "SPL_UNSUPPORTED_WHERE_EXPRESSION", Message: fmt.Sprintf("unsupported where expression type %T", expression), Range: expression.SourceRange()}
	}
}

func nilSPLWhereExpression(expression spl.WhereExpr) bool {
	if expression == nil {
		return true
	}
	switch expression := expression.(type) {
	case *spl.WhereBoolExpr:
		return expression == nil
	case *spl.WhereNotExpr:
		return expression == nil
	case *spl.WhereComparisonExpr:
		return expression == nil
	case *spl.WhereScalarPredicateExpr:
		return expression == nil
	default:
		return false
	}
}

func nilSPLScalarExpression(expression spl.ScalarExpr) bool {
	if expression == nil {
		return true
	}
	switch expression := expression.(type) {
	case *spl.ScalarFieldExpr:
		return expression == nil
	case *spl.ScalarLiteralExpr:
		return expression == nil
	case *spl.ScalarCallExpr:
		return expression == nil
	case *spl.ScalarIfExpr:
		return expression == nil
	case *spl.ScalarCaseExpr:
		return expression == nil
	default:
		return false
	}
}

func splQuotedStringLiteral(
	expression spl.ScalarExpr,
	fallbackRange spl.Range,
) (*spl.ScalarLiteralExpr, spl.Range, bool) {
	sourceRange := fallbackRange
	if !nilSPLScalarExpression(expression) {
		sourceRange = expression.SourceRange()
	}
	literal, ok := expression.(*spl.ScalarLiteralExpr)
	if !ok || literal == nil ||
		literal.Value.Kind != spl.LiteralKindString ||
		!literal.Value.Quoted {
		return nil, sourceRange, false
	}
	return literal, sourceRange, true
}

type splExpressionComplexityValidator struct {
	nodes  int
	active map[any]struct{}
	budget *splExpressionResourceBudget
}

type splExpressionResourceBudget struct {
	concatenationOperands int
}

func validateSPLWhereExpressionComplexity(
	expression spl.WhereExpr,
	budget *splExpressionResourceBudget,
) error {
	validator := splExpressionComplexityValidator{
		active: make(map[any]struct{}),
		budget: budget,
	}
	return validator.validateWhere(expression, 1)
}

func validateSPLScalarExpressionComplexity(
	expression spl.ScalarExpr,
	budget *splExpressionResourceBudget,
) error {
	validator := splExpressionComplexityValidator{
		active: make(map[any]struct{}),
		budget: budget,
	}
	return validator.validateScalar(expression, 1)
}

func (v *splExpressionComplexityValidator) validateWhere(
	expression spl.WhereExpr,
	depth int,
) error {
	if nilSPLWhereExpression(expression) {
		return nil
	}
	if err := v.enter(expression, depth, expression.SourceRange()); err != nil {
		return err
	}
	defer v.leave(expression)

	switch expression := expression.(type) {
	case *spl.WhereBoolExpr:
		if err := v.validateWhere(expression.Left, depth+1); err != nil {
			return err
		}
		return v.validateWhere(expression.Right, depth+1)
	case *spl.WhereNotExpr:
		return v.validateWhere(expression.Operand, depth+1)
	case *spl.WhereComparisonExpr:
		if err := v.validateScalar(expression.Left, depth+1); err != nil {
			return err
		}
		return v.validateScalar(expression.Right, depth+1)
	case *spl.WhereScalarPredicateExpr:
		return v.validateScalar(expression.Value, depth+1)
	default:
		return nil
	}
}

func (v *splExpressionComplexityValidator) validateScalar(
	expression spl.ScalarExpr,
	depth int,
) error {
	if nilSPLScalarExpression(expression) {
		return nil
	}
	if err := v.enter(expression, depth, expression.SourceRange()); err != nil {
		return err
	}
	defer v.leave(expression)

	switch expression := expression.(type) {
	case *spl.ScalarCallExpr:
		if expression.Function == spl.ScalarFunctionCoalesce &&
			len(expression.Arguments) > spl.MaximumCoalesceArguments {
			return splExpressionComplexityError(
				fmt.Sprintf(
					"coalesce contains more than %d arguments",
					spl.MaximumCoalesceArguments,
				),
				expression.Range,
			)
		}
		if expression.Function == spl.ScalarFunctionConcat &&
			len(expression.Arguments) > spl.MaximumConcatenationOperands {
			return splExpressionComplexityError(
				fmt.Sprintf(
					"concatenation contains more than %d operands",
					spl.MaximumConcatenationOperands,
				),
				expression.Range,
			)
		}
		if expression.Function == spl.ScalarFunctionConcat &&
			v.budget != nil {
			if v.budget.concatenationOperands >
				spl.MaximumConcatenationOperandsPerQuery-len(expression.Arguments) {
				return splExpressionComplexityError(
					fmt.Sprintf(
						"concatenation contains more than %d operand occurrences per query",
						spl.MaximumConcatenationOperandsPerQuery,
					),
					expression.Range,
				)
			}
			v.budget.concatenationOperands += len(expression.Arguments)
		}
		if len(expression.Arguments) > maxConvertedExpressionNodes {
			return splExpressionComplexityError(
				"scalar call exceeds the structural node budget",
				expression.Range,
			)
		}
		for _, argument := range expression.Arguments {
			if err := v.validateScalar(argument, depth+1); err != nil {
				return err
			}
		}
	case *spl.ScalarIfExpr:
		if err := v.validateWhere(expression.Condition, depth+1); err != nil {
			return err
		}
		if err := v.validateScalar(expression.True, depth+1); err != nil {
			return err
		}
		return v.validateScalar(expression.False, depth+1)
	case *spl.ScalarCaseExpr:
		if len(expression.Branches) > spl.MaximumCaseBranches {
			return splExpressionComplexityError(
				fmt.Sprintf(
					"case contains more than %d condition/value pairs",
					spl.MaximumCaseBranches,
				),
				expression.Range,
			)
		}
		for _, branch := range expression.Branches {
			if err := v.validateWhere(branch.Condition, depth+1); err != nil {
				return err
			}
			if err := v.validateScalar(branch.Value, depth+1); err != nil {
				return err
			}
		}
	}
	return nil
}

func (v *splExpressionComplexityValidator) enter(
	node any,
	depth int,
	sourceRange spl.Range,
) error {
	if depth > maxConvertedExpressionDepth {
		return splExpressionComplexityError(
			fmt.Sprintf(
				"eval/where expression nesting exceeds %d levels",
				maxConvertedExpressionDepth,
			),
			sourceRange,
		)
	}
	if _, cyclic := v.active[node]; cyclic {
		return splExpressionComplexityError(
			"eval/where expression graph contains a cycle",
			sourceRange,
		)
	}
	v.nodes++
	if v.nodes > maxConvertedExpressionNodes {
		return splExpressionComplexityError(
			fmt.Sprintf(
				"eval/where expression contains more than %d structural nodes",
				maxConvertedExpressionNodes,
			),
			sourceRange,
		)
	}
	v.active[node] = struct{}{}
	return nil
}

func (v *splExpressionComplexityValidator) leave(node any) {
	delete(v.active, node)
}

func splExpressionComplexityError(message string, sourceRange spl.Range) error {
	return &Diagnostic{
		Code:    "SPL_QUERY_TOO_COMPLEX",
		Message: message,
		Range:   sourceRange,
	}
}

func buildEventStatsFieldMeasure(
	aggregate spl.StatsAggregate,
	function AggregateFunction,
	form string,
) (AggregateMeasure, error) {
	if aggregate.Input == "" ||
		aggregate.InputRange == (spl.Range{}) ||
		aggregate.Predicate != nil ||
		aggregate.Percentile != 0 ||
		!aggregate.ExplicitAlias {
		return AggregateMeasure{}, &Diagnostic{
			Code: "SPL_UNSUPPORTED_EVENTSTATS_AGGREGATE",
			Message: "eventstats " + form + " requires one exact " +
				"input field and an explicit alias",
			Range: aggregate.Range,
		}
	}
	input, err := ResolveField(aggregate.Input, aggregate.InputRange)
	if err != nil {
		return AggregateMeasure{}, err
	}
	return AggregateMeasure{
		Function: function,
		Input:    input,
		Output:   aggregate.Alias,
	}, nil
}

type countPredicateMeasureDiagnostics struct {
	unsupportedCode string
	invalidMessage  string
	ambiguousCode   string
	reservedMessage string
}

// buildCountPredicateMeasure converts the common count(eval(...)) contract
// while leaving command-specific diagnostics at the stats/eventstats callers.
func buildCountPredicateMeasure(
	aggregate spl.StatsAggregate,
	outputSchemaKnown bool,
	budget *splExpressionResourceBudget,
	diagnostics countPredicateMeasureDiagnostics,
) (AggregateMeasure, error) {
	if aggregate.Input != "" ||
		aggregate.InputRange != (spl.Range{}) ||
		aggregate.Percentile != 0 ||
		!aggregate.ExplicitAlias ||
		nilSPLWhereExpression(aggregate.Predicate) {
		return AggregateMeasure{}, &Diagnostic{
			Code:    diagnostics.unsupportedCode,
			Message: diagnostics.invalidMessage,
			Range:   aggregate.Range,
		}
	}
	predicate, err := convertWhereExpression(aggregate.Predicate, budget)
	if err != nil {
		return AggregateMeasure{}, err
	}
	if !outputSchemaKnown {
		if fieldRange, referencesReserved := predicateFieldRange(predicate, "fields"); referencesReserved {
			return AggregateMeasure{}, &Diagnostic{
				Code:    diagnostics.ambiguousCode,
				Message: diagnostics.reservedMessage,
				Range:   fieldRange,
			}
		}
	}
	return AggregateMeasure{
		Function:  AggregateFunctionCountPredicate,
		Predicate: predicate,
		Output:    aggregate.Alias,
	}, nil
}

// predicateFieldRange finds the first reference to name in deterministic
// expression order. Conditional aggregate planning uses it to preserve the
// reserved open-event "fields" payload boundary just as it does for exact
// inputs.
func predicateFieldRange(expression Expression, name string) (spl.Range, bool) {
	var visitExpression func(Expression) (spl.Range, bool)
	var visitScalar func(ScalarExpression) (spl.Range, bool)
	visitScalar = func(expression ScalarExpression) (spl.Range, bool) {
		switch expression := expression.(type) {
		case *ScalarFieldExpression:
			if expression != nil && expression.Field.Name == name {
				return expression.Field.Range, true
			}
		case *ScalarCallExpression:
			if expression == nil {
				return spl.Range{}, false
			}
			for _, argument := range expression.Arguments {
				if sourceRange, ok := visitScalar(argument); ok {
					return sourceRange, true
				}
			}
		case *ScalarIfExpression:
			if expression == nil {
				return spl.Range{}, false
			}
			if sourceRange, ok := visitExpression(expression.Condition); ok {
				return sourceRange, true
			}
			if sourceRange, ok := visitScalar(expression.True); ok {
				return sourceRange, true
			}
			return visitScalar(expression.False)
		case *ScalarCaseExpression:
			if expression == nil {
				return spl.Range{}, false
			}
			for _, branch := range expression.Branches {
				if sourceRange, ok := visitExpression(branch.Condition); ok {
					return sourceRange, true
				}
				if sourceRange, ok := visitScalar(branch.Value); ok {
					return sourceRange, true
				}
			}
		}
		return spl.Range{}, false
	}
	visitExpression = func(expression Expression) (spl.Range, bool) {
		switch expression := expression.(type) {
		case *BooleanExpression:
			if expression == nil {
				return spl.Range{}, false
			}
			if sourceRange, ok := visitExpression(expression.Left); ok {
				return sourceRange, true
			}
			return visitExpression(expression.Right)
		case *NotExpression:
			if expression != nil {
				return visitExpression(expression.Operand)
			}
		case *EvalComparisonExpression:
			if expression == nil {
				return spl.Range{}, false
			}
			if sourceRange, ok := visitScalar(expression.Left); ok {
				return sourceRange, true
			}
			return visitScalar(expression.Right)
		case *ScalarPredicateExpression:
			if expression != nil {
				return visitScalar(expression.Value)
			}
		}
		return spl.Range{}, false
	}
	return visitExpression(expression)
}

func convertScalarExpressionUnchecked(expression spl.ScalarExpr) (ScalarExpression, error) {
	if expression == nil {
		return nil, &Diagnostic{
			Code:    "SPL_UNSUPPORTED_EVAL_EXPRESSION",
			Message: "scalar expression is missing",
		}
	}
	switch expression := expression.(type) {
	case *spl.ScalarFieldExpr:
		if expression == nil {
			return nil, &Diagnostic{
				Code:    "SPL_UNSUPPORTED_EVAL_EXPRESSION",
				Message: "scalar field expression is missing",
			}
		}
		field, err := ResolveField(expression.Field, expression.Range)
		if err != nil {
			return nil, err
		}
		return &ScalarFieldExpression{Field: field, Range: expression.Range}, nil
	case *spl.ScalarLiteralExpr:
		if expression == nil {
			return nil, &Diagnostic{
				Code:    "SPL_UNSUPPORTED_EVAL_EXPRESSION",
				Message: "scalar literal expression is missing",
			}
		}
		value, err := convertValue(expression.Value)
		if err != nil {
			return nil, err
		}
		return &ScalarLiteralExpression{Value: value, Range: expression.Range}, nil
	case *spl.ScalarCallExpr:
		if expression == nil {
			return nil, &Diagnostic{
				Code:    "SPL_UNSUPPORTED_EVAL_EXPRESSION",
				Message: "scalar call expression is missing",
			}
		}
		expectedArguments := 0
		hasExactArity := false
		functionName := ""
		switch expression.Function {
		case spl.ScalarFunctionConcat:
			functionName = "concatenation"
			if len(expression.Arguments) < 2 {
				return nil, &Diagnostic{
					Code:    "SPL_INVALID_EVAL_ARITY",
					Message: "concatenation requires at least two operands",
					Range:   expression.Range,
				}
			}
			if len(expression.Arguments) > spl.MaximumConcatenationOperands {
				return nil, &Diagnostic{
					Code: "SPL_QUERY_TOO_COMPLEX",
					Message: fmt.Sprintf(
						"concatenation contains more than %d operands",
						spl.MaximumConcatenationOperands,
					),
					Range: expression.Range,
				}
			}
		case spl.ScalarFunctionNow:
			expectedArguments = 0
			hasExactArity = true
			functionName = "now"
		case spl.ScalarFunctionStrftime:
			expectedArguments = 2
			hasExactArity = true
			functionName = "strftime"
		case spl.ScalarFunctionStrptime:
			expectedArguments = 2
			hasExactArity = true
			functionName = "strptime"
		case spl.ScalarFunctionRelativeTime:
			expectedArguments = 2
			hasExactArity = true
			functionName = "relative_time"
		case spl.ScalarFunctionToNumber:
			expectedArguments = 1
			hasExactArity = true
			functionName = "tonumber"
		case spl.ScalarFunctionToString:
			expectedArguments = 1
			hasExactArity = true
			functionName = "tostring"
		case spl.ScalarFunctionRound:
			functionName = "round"
			if len(expression.Arguments) < 1 || len(expression.Arguments) > 2 {
				return nil, &Diagnostic{
					Code:    "SPL_INVALID_EVAL_ARITY",
					Message: "round requires one or two arguments",
					Range:   expression.Range,
				}
			}
		case spl.ScalarFunctionCeil:
			expectedArguments = 1
			hasExactArity = true
			functionName = "ceil"
		case spl.ScalarFunctionFloor:
			expectedArguments = 1
			hasExactArity = true
			functionName = "floor"
		case spl.ScalarFunctionMVCount:
			expectedArguments = 1
			hasExactArity = true
			functionName = "mvcount"
		case spl.ScalarFunctionMatch:
			expectedArguments = 2
			hasExactArity = true
			functionName = "match"
		case spl.ScalarFunctionLike:
			expectedArguments = 2
			hasExactArity = true
			functionName = "like"
		case spl.ScalarFunctionReplace:
			expectedArguments = 3
			hasExactArity = true
			functionName = "replace"
		case spl.ScalarFunctionIsNull:
			expectedArguments = 1
			hasExactArity = true
			functionName = "isnull"
		case spl.ScalarFunctionIsNotNull:
			expectedArguments = 1
			hasExactArity = true
			functionName = "isnotnull"
		case spl.ScalarFunctionCoalesce:
			functionName = "coalesce"
			if len(expression.Arguments) == 0 {
				return nil, &Diagnostic{
					Code:    "SPL_INVALID_EVAL_ARITY",
					Message: "coalesce requires at least one argument",
					Range:   expression.Range,
				}
			}
			if len(expression.Arguments) > spl.MaximumCoalesceArguments {
				return nil, &Diagnostic{
					Code: "SPL_QUERY_TOO_COMPLEX",
					Message: fmt.Sprintf(
						"coalesce contains more than %d arguments",
						spl.MaximumCoalesceArguments,
					),
					Range: expression.Range,
				}
			}
		case spl.ScalarFunctionLower:
			expectedArguments = 1
			hasExactArity = true
			functionName = "lower"
		case spl.ScalarFunctionUpper:
			expectedArguments = 1
			hasExactArity = true
			functionName = "upper"
		case spl.ScalarFunctionLength:
			expectedArguments = 1
			hasExactArity = true
			functionName = "len"
		case spl.ScalarFunctionSubstring:
			functionName = "substr"
			if len(expression.Arguments) < 2 || len(expression.Arguments) > 3 {
				return nil, &Diagnostic{
					Code:    "SPL_INVALID_EVAL_ARITY",
					Message: "substr requires two or three arguments",
					Range:   expression.Range,
				}
			}
		}
		if hasExactArity && len(expression.Arguments) != expectedArguments {
			argumentNoun := "arguments"
			switch expectedArguments {
			case 0:
				return nil, &Diagnostic{
					Code:    "SPL_INVALID_EVAL_ARITY",
					Message: functionName + " requires no arguments",
					Range:   expression.Range,
				}
			case 1:
				argumentNoun = "argument"
			}
			return nil, &Diagnostic{
				Code: "SPL_INVALID_EVAL_ARITY",
				Message: fmt.Sprintf(
					"%s requires exactly %d %s",
					functionName,
					expectedArguments,
					argumentNoun,
				),
				Range: expression.Range,
			}
		}
		if expression.Function == spl.ScalarFunctionToNumber ||
			expression.Function == spl.ScalarFunctionReplace ||
			expression.Function == spl.ScalarFunctionLower ||
			expression.Function == spl.ScalarFunctionUpper ||
			expression.Function == spl.ScalarFunctionLength {
			for _, argument := range expression.Arguments {
				if splScalarMayReturnBooleanFunction(argument) {
					return nil, &Diagnostic{
						Code:    "SPL_UNSUPPORTED_EVAL_EXPRESSION",
						Message: "search-mode scalar functions cannot consume a Boolean result",
						Range:   argument.SourceRange(),
					}
				}
			}
		}
		if expression.Function == spl.ScalarFunctionConcat {
			for _, argument := range expression.Arguments {
				if nilSPLScalarExpression(argument) {
					return nil, &Diagnostic{
						Code:    "SPL_UNSUPPORTED_EVAL_EXPRESSION",
						Message: "concatenation operand is missing",
						Range:   expression.Range,
					}
				}
				if splScalarMayReturnBooleanValue(argument) {
					return nil, &Diagnostic{
						Code:    "SPL_UNSUPPORTED_EVAL_EXPRESSION",
						Message: "concatenation cannot consume a Boolean result",
						Range:   argument.SourceRange(),
					}
				}
			}
		}
		switch expression.Function {
		case spl.ScalarFunctionRound,
			spl.ScalarFunctionCeil,
			spl.ScalarFunctionFloor:
			if splScalarMayReturnBooleanFunction(expression.Arguments[0]) {
				functionName := ""
				switch expression.Function {
				case spl.ScalarFunctionRound:
					functionName = "round"
				case spl.ScalarFunctionCeil:
					functionName = "ceil"
				case spl.ScalarFunctionFloor:
					functionName = "floor"
				}
				return nil, &Diagnostic{
					Code:    "SPL_UNSUPPORTED_EVAL_EXPRESSION",
					Message: functionName + " cannot consume a Boolean result",
					Range:   expression.Arguments[0].SourceRange(),
				}
			}
		}
		if expression.Function == spl.ScalarFunctionRound {
			if len(expression.Arguments) == 2 &&
				!spl.SupportedRoundPrecision(expression.Arguments[1]) {
				return nil, &Diagnostic{
					Code: "SPL_UNSUPPORTED_ROUND_PRECISION",
					Message: fmt.Sprintf(
						"round precision must be a literal integer from 0 through %d",
						spl.MaximumRoundPrecision,
					),
					Range: expression.Arguments[1].SourceRange(),
				}
			}
		}
		if expression.Function == spl.ScalarFunctionSubstring {
			if splScalarMayReturnBooleanFunction(expression.Arguments[0]) {
				return nil, &Diagnostic{
					Code:    "SPL_UNSUPPORTED_EVAL_EXPRESSION",
					Message: "search-mode scalar functions cannot consume a Boolean result",
					Range:   expression.Arguments[0].SourceRange(),
				}
			}
			for index := 1; index < len(expression.Arguments); index++ {
				if nilSPLScalarExpression(expression.Arguments[index]) {
					return nil, &Diagnostic{
						Code:    "SPL_UNSUPPORTED_EVAL_EXPRESSION",
						Message: "scalar expression is missing",
						Range:   expression.Range,
					}
				}
				literal, ok := expression.Arguments[index].(*spl.ScalarLiteralExpr)
				if !ok || literal == nil ||
					literal.Value.Kind != spl.LiteralKindInteger {
					return nil, &Diagnostic{
						Code: "SPL_UNSUPPORTED_SUBSTRING_INDEX",
						Message: "substr start and length must be literal integers " +
							"in compatibility version 0.1",
						Range: expression.Arguments[index].SourceRange(),
					}
				}
			}
		}
		if expression.Function == spl.ScalarFunctionMatch {
			if splScalarMayReturnBooleanFunction(expression.Arguments[0]) {
				return nil, &Diagnostic{
					Code:    "SPL_UNSUPPORTED_EVAL_EXPRESSION",
					Message: "match cannot consume a Boolean result",
					Range:   expression.Arguments[0].SourceRange(),
				}
			}
			pattern, patternRange, ok := splQuotedStringLiteral(
				expression.Arguments[1],
				expression.Range,
			)
			if !ok {
				return nil, &Diagnostic{
					Code:    "SPL_UNSUPPORTED_EVAL_EXPRESSION",
					Message: "match regular expression must be a quoted string literal",
					Range:   patternRange,
				}
			}
			if _, err := splregex.CompileMatchPattern(pattern.Value.Text); err != nil {
				if splregex.IsMatchComplexityError(err) {
					return nil, &Diagnostic{
						Code: "SPL_QUERY_TOO_COMPLEX",
						Message: fmt.Sprintf(
							"match regular expression exceeds the %d-byte or %d-work-unit limit",
							splregex.MaximumMatchPatternBytes,
							splregex.MaximumMatchProgramWorkUnits,
						),
						Range: pattern.Range,
					}
				}
				return nil, &Diagnostic{
					Code:    "SPL_UNSUPPORTED_REGEX",
					Message: "match regular expression is outside the supported RE2-compatible subset",
					Range:   pattern.Range,
				}
			}
		}
		if expression.Function == spl.ScalarFunctionLike {
			if splScalarMayReturnBooleanFunction(expression.Arguments[0]) {
				return nil, &Diagnostic{
					Code:    "SPL_UNSUPPORTED_EVAL_EXPRESSION",
					Message: "like cannot consume a Boolean result",
					Range:   expression.Arguments[0].SourceRange(),
				}
			}
			pattern, patternRange, ok := splQuotedStringLiteral(
				expression.Arguments[1],
				expression.Range,
			)
			if !ok {
				return nil, &Diagnostic{
					Code:    "SPL_UNSUPPORTED_EVAL_EXPRESSION",
					Message: "like pattern must be a quoted string literal",
					Range:   patternRange,
				}
			}
			if _, err := splwildcard.CompileLikePattern(pattern.Value.Text); err != nil {
				if splwildcard.IsLikeComplexityError(err) {
					return nil, &Diagnostic{
						Code: "SPL_QUERY_TOO_COMPLEX",
						Message: fmt.Sprintf(
							"like pattern exceeds the %d-byte or %d-work-unit limit",
							splwildcard.MaximumLikePatternBytes,
							splwildcard.MaximumLikePatternWorkUnits,
						),
						Range: pattern.Range,
					}
				}
				return nil, &Diagnostic{
					Code:    "SPL_UNSUPPORTED_LIKE_PATTERN",
					Message: "like pattern must be valid UTF-8 without NUL bytes or an unpaired terminal backslash",
					Range:   pattern.Range,
				}
			}
		}
		if expression.Function == spl.ScalarFunctionStrftime {
			if splScalarMayReturnBooleanFunction(expression.Arguments[0]) {
				return nil, &Diagnostic{
					Code:    "SPL_UNSUPPORTED_EVAL_EXPRESSION",
					Message: "strftime cannot consume a Boolean time value",
					Range:   expression.Arguments[0].SourceRange(),
				}
			}
			format, formatRange, ok := splQuotedStringLiteral(
				expression.Arguments[1],
				expression.Range,
			)
			if !ok {
				return nil, &Diagnostic{
					Code:    "SPL_UNSUPPORTED_EVAL_EXPRESSION",
					Message: "strftime format must be a quoted string literal",
					Range:   formatRange,
				}
			}
			if _, err := spltimeformat.CompileStrftimeFormat(format.Value.Text); err != nil {
				if errors.Is(err, spltimeformat.ErrStrftimeFormatTooLarge) {
					return nil, &Diagnostic{
						Code: "SPL_QUERY_TOO_COMPLEX",
						Message: fmt.Sprintf(
							"strftime format exceeds the %d-byte, %d-work-unit, or %d-output-byte limit",
							spltimeformat.MaximumStrftimeFormatBytes,
							spltimeformat.MaximumStrftimeWorkUnits,
							spltimeformat.MaximumStrftimeOutputBytes,
						),
						Range: format.Range,
					}
				}
				return nil, &Diagnostic{
					Code: "SPL_UNSUPPORTED_TIME_FORMAT",
					Message: "strftime format is outside the supported locale-stable " +
						"date/time variable subset",
					Range: format.Range,
				}
			}
		}
		if expression.Function == spl.ScalarFunctionStrptime {
			if splScalarMayReturnBooleanFunction(expression.Arguments[0]) {
				return nil, &Diagnostic{
					Code:    "SPL_UNSUPPORTED_EVAL_EXPRESSION",
					Message: "strptime cannot consume a Boolean text value",
					Range:   expression.Arguments[0].SourceRange(),
				}
			}
			format, formatRange, ok := splQuotedStringLiteral(
				expression.Arguments[1],
				expression.Range,
			)
			if !ok {
				return nil, &Diagnostic{
					Code:    "SPL_UNSUPPORTED_EVAL_EXPRESSION",
					Message: "strptime format must be a quoted string literal",
					Range:   formatRange,
				}
			}
			if _, err := spltimeformat.CompileStrptimeFormat(format.Value.Text); err != nil {
				if errors.Is(err, spltimeformat.ErrStrptimeFormatTooLarge) {
					return nil, &Diagnostic{
						Code: "SPL_QUERY_TOO_COMPLEX",
						Message: fmt.Sprintf(
							"strptime format exceeds the %d-byte or %d-work-unit limit",
							spltimeformat.MaximumStrptimeFormatBytes,
							spltimeformat.MaximumStrptimeWorkUnits,
						),
						Range: format.Range,
					}
				}
				return nil, &Diagnostic{
					Code: "SPL_UNSUPPORTED_TIME_FORMAT",
					Message: "strptime format is outside the supported deterministic " +
						"full-date parsing subset",
					Range: format.Range,
				}
			}
		}
		if expression.Function == spl.ScalarFunctionRelativeTime {
			if splScalarMayReturnBooleanFunction(expression.Arguments[0]) {
				return nil, &Diagnostic{
					Code:    "SPL_UNSUPPORTED_EVAL_EXPRESSION",
					Message: "relative_time cannot consume a Boolean time value",
					Range:   expression.Arguments[0].SourceRange(),
				}
			}
			specifier, specifierRange, ok := splQuotedStringLiteral(
				expression.Arguments[1],
				expression.Range,
			)
			if !ok {
				return nil, &Diagnostic{
					Code:    "SPL_UNSUPPORTED_EVAL_EXPRESSION",
					Message: "relative_time specifier must be a quoted string literal",
					Range:   specifierRange,
				}
			}
			if _, err := splrelativetime.CompileSpecifier(
				specifier.Value.Text,
			); err != nil {
				if errors.Is(err, splrelativetime.ErrSpecifierTooLarge) {
					return nil, &Diagnostic{
						Code: "SPL_QUERY_TOO_COMPLEX",
						Message: fmt.Sprintf(
							"relative_time specifier exceeds the %d-byte or %d-work-unit limit",
							splrelativetime.MaximumSpecifierBytes,
							splrelativetime.MaximumSpecifierWorkUnits,
						),
						Range: specifier.Range,
					}
				}
				if errors.Is(err, splrelativetime.ErrMagnitudeOutOfRange) {
					return nil, &Diagnostic{
						Code: "SPL_NUMBER_OUT_OF_RANGE",
						Message: "relative_time magnitude exceeds the supported " +
							searchtimebounds.YearRangeDescription + " timestamp span",
						Range: specifier.Range,
					}
				}
				return nil, &Diagnostic{
					Code: "SPL_UNSUPPORTED_RELATIVE_TIME_SPECIFIER",
					Message: "relative_time specifier is outside the supported " +
						"bounded offset-and-snap subset",
					Range: specifier.Range,
				}
			}
		}
		arguments := make([]ScalarExpression, 0, len(expression.Arguments))
		for _, argument := range expression.Arguments {
			converted, err := convertScalarExpressionUnchecked(argument)
			if err != nil {
				return nil, err
			}
			arguments = append(arguments, converted)
		}
		var function ScalarFunction
		switch expression.Function {
		case spl.ScalarFunctionConcat:
			function = ScalarFunctionConcat
		case spl.ScalarFunctionNow:
			function = ScalarFunctionNow
		case spl.ScalarFunctionStrftime:
			function = ScalarFunctionStrftime
		case spl.ScalarFunctionStrptime:
			function = ScalarFunctionStrptime
		case spl.ScalarFunctionRelativeTime:
			function = ScalarFunctionRelativeTime
		case spl.ScalarFunctionToNumber:
			function = ScalarFunctionToNumber
		case spl.ScalarFunctionToString:
			function = ScalarFunctionToString
		case spl.ScalarFunctionRound:
			function = ScalarFunctionRound
		case spl.ScalarFunctionCeil:
			function = ScalarFunctionCeil
		case spl.ScalarFunctionFloor:
			function = ScalarFunctionFloor
		case spl.ScalarFunctionMVCount:
			function = ScalarFunctionMVCount
		case spl.ScalarFunctionMatch:
			function = ScalarFunctionMatch
		case spl.ScalarFunctionLike:
			function = ScalarFunctionLike
		case spl.ScalarFunctionReplace:
			function = ScalarFunctionReplace
		case spl.ScalarFunctionIsNull:
			function = ScalarFunctionIsNull
		case spl.ScalarFunctionIsNotNull:
			function = ScalarFunctionIsNotNull
		case spl.ScalarFunctionCoalesce:
			function = ScalarFunctionCoalesce
		case spl.ScalarFunctionLower:
			function = ScalarFunctionLower
		case spl.ScalarFunctionUpper:
			function = ScalarFunctionUpper
		case spl.ScalarFunctionLength:
			function = ScalarFunctionLength
		case spl.ScalarFunctionSubstring:
			function = ScalarFunctionSubstring
		default:
			return nil, &Diagnostic{Code: "SPL_UNSUPPORTED_EVAL_FUNCTION", Message: "unsupported scalar function", Range: expression.Range}
		}
		return &ScalarCallExpression{Function: function, Arguments: arguments, Range: expression.Range}, nil
	case *spl.ScalarIfExpr:
		if expression == nil {
			return nil, &Diagnostic{
				Code:    "SPL_UNSUPPORTED_EVAL_EXPRESSION",
				Message: "if expression is missing",
			}
		}
		condition, err := convertWhereExpressionUnchecked(expression.Condition)
		if err != nil {
			return nil, err
		}
		trueValue, err := convertScalarExpressionUnchecked(expression.True)
		if err != nil {
			return nil, err
		}
		falseValue, err := convertScalarExpressionUnchecked(expression.False)
		if err != nil {
			return nil, err
		}
		return &ScalarIfExpression{
			Condition: condition,
			True:      trueValue,
			False:     falseValue,
			Range:     expression.Range,
		}, nil
	case *spl.ScalarCaseExpr:
		if expression == nil {
			return nil, &Diagnostic{
				Code:    "SPL_UNSUPPORTED_EVAL_EXPRESSION",
				Message: "case expression is missing",
			}
		}
		if len(expression.Branches) == 0 {
			return nil, &Diagnostic{
				Code:    "SPL_INVALID_EVAL_ARITY",
				Message: "case requires one or more condition/value pairs",
				Range:   expression.Range,
			}
		}
		if len(expression.Branches) > spl.MaximumCaseBranches {
			return nil, &Diagnostic{
				Code: "SPL_QUERY_TOO_COMPLEX",
				Message: fmt.Sprintf(
					"case contains more than %d condition/value pairs",
					spl.MaximumCaseBranches,
				),
				Range: expression.Range,
			}
		}
		branches := make([]ScalarCaseBranch, 0, len(expression.Branches))
		for _, branch := range expression.Branches {
			condition, err := convertWhereExpressionUnchecked(branch.Condition)
			if err != nil {
				return nil, err
			}
			value, err := convertScalarExpressionUnchecked(branch.Value)
			if err != nil {
				return nil, err
			}
			branches = append(branches, ScalarCaseBranch{
				Condition: condition,
				Value:     value,
				Range:     branch.Range,
			})
		}
		return &ScalarCaseExpression{
			Branches: branches,
			Range:    expression.Range,
		}, nil
	default:
		return nil, &Diagnostic{Code: "SPL_UNSUPPORTED_EVAL_EXPRESSION", Message: fmt.Sprintf("unsupported scalar expression type %T", expression), Range: expression.SourceRange()}
	}
}

func splScalarMayReturnBooleanFunction(expression spl.ScalarExpr) bool {
	switch expression := expression.(type) {
	case *spl.ScalarCallExpr:
		if expression == nil {
			return false
		}
		if expression.Function.ReturnsBoolean() {
			return true
		}
		if expression.Function == spl.ScalarFunctionCoalesce {
			for _, argument := range expression.Arguments {
				if splScalarMayReturnBooleanFunction(argument) {
					return true
				}
			}
		}
		return false
	case *spl.ScalarIfExpr:
		return expression != nil &&
			(splScalarMayReturnBooleanFunction(expression.True) ||
				splScalarMayReturnBooleanFunction(expression.False))
	case *spl.ScalarCaseExpr:
		if expression == nil {
			return false
		}
		for _, branch := range expression.Branches {
			if splScalarMayReturnBooleanFunction(branch.Value) {
				return true
			}
		}
		return false
	default:
		return false
	}
}

func splScalarMayReturnBooleanValue(expression spl.ScalarExpr) bool {
	switch expression := expression.(type) {
	case *spl.ScalarLiteralExpr:
		return expression != nil &&
			expression.Value.Kind == spl.LiteralKindBool
	case *spl.ScalarCallExpr:
		if expression == nil {
			return false
		}
		if expression.Function.ReturnsBoolean() {
			return true
		}
		if expression.Function == spl.ScalarFunctionCoalesce {
			for _, argument := range expression.Arguments {
				if splScalarMayReturnBooleanValue(argument) {
					return true
				}
			}
		}
		return false
	case *spl.ScalarIfExpr:
		return expression != nil &&
			(splScalarMayReturnBooleanValue(expression.True) ||
				splScalarMayReturnBooleanValue(expression.False))
	case *spl.ScalarCaseExpr:
		if expression == nil {
			return false
		}
		for _, branch := range expression.Branches {
			if splScalarMayReturnBooleanValue(branch.Value) {
				return true
			}
		}
		return false
	default:
		return false
	}
}

func scalarFunctionReturnsBoolean(expression ScalarExpression) bool {
	switch expression := expression.(type) {
	case *ScalarCallExpression:
		if expression == nil {
			return false
		}
		if expression.Function.ReturnsBoolean() {
			return true
		}
		if expression.Function == ScalarFunctionCoalesce {
			return coalesceScalarExpressionReturnsBoolean(expression.Arguments)
		}
		return false
	case *ScalarLiteralExpression:
		return expression != nil && expression.Value.Kind == ValueKindBool
	case *ScalarIfExpression:
		return expression != nil &&
			scalarFunctionReturnsBoolean(expression.True) &&
			scalarFunctionReturnsBoolean(expression.False)
	case *ScalarCaseExpression:
		return expression != nil &&
			caseScalarFunctionReturnsBoolean(expression.Branches)
	default:
		return false
	}
}

func scalarExpressionCanBeDirectPredicate(expression ScalarExpression) bool {
	switch expression := expression.(type) {
	case *ScalarCallExpression:
		if expression == nil {
			return false
		}
		if expression.Function.ReturnsBoolean() {
			return true
		}
		if expression.Function == ScalarFunctionCoalesce {
			return coalesceScalarExpressionReturnsBoolean(expression.Arguments)
		}
		return false
	case *ScalarIfExpression:
		return expression != nil && scalarFunctionReturnsBoolean(expression)
	case *ScalarCaseExpression:
		return expression != nil && scalarFunctionReturnsBoolean(expression)
	default:
		return false
	}
}

func coalesceScalarExpressionReturnsBoolean(arguments []ScalarExpression) bool {
	foundBoolean := false
	for _, argument := range arguments {
		if literal, ok := argument.(*ScalarLiteralExpression); ok &&
			literal != nil &&
			literal.Value.Kind == ValueKindNull {
			continue
		}
		if !scalarFunctionReturnsBoolean(argument) {
			return false
		}
		foundBoolean = true
	}
	return foundBoolean
}

func caseScalarFunctionReturnsBoolean(branches []ScalarCaseBranch) bool {
	foundBoolean := false
	for _, branch := range branches {
		if literal, ok := branch.Value.(*ScalarLiteralExpression); ok &&
			literal != nil &&
			literal.Value.Kind == ValueKindNull {
			continue
		}
		if !scalarFunctionReturnsBoolean(branch.Value) {
			return false
		}
		foundBoolean = true
	}
	return foundBoolean
}

func convertComparisonOp(op spl.CompareOp) ComparisonOp {
	switch op {
	case spl.CompareOpEqual:
		return ComparisonOpEqual
	case spl.CompareOpNotEqual:
		return ComparisonOpNotEqual
	case spl.CompareOpLess:
		return ComparisonOpLess
	case spl.CompareOpLessEqual:
		return ComparisonOpLessEqual
	case spl.CompareOpGreater:
		return ComparisonOpGreater
	case spl.CompareOpGreaterEqual:
		return ComparisonOpGreaterEqual
	default:
		return ComparisonOpInvalid
	}
}

func convertValue(literal spl.Literal) (Value, error) {
	switch literal.Kind {
	case spl.LiteralKindString:
		return Value{Kind: ValueKindString, String: literal.Text, Quoted: literal.Quoted, SourceText: literal.Text}, nil
	case spl.LiteralKindInteger:
		if strings.HasPrefix(literal.Text, "-") {
			value, err := strconv.ParseInt(literal.Text, 10, 64)
			if err != nil {
				return Value{}, &Diagnostic{Code: "SPL_NUMBER_OUT_OF_RANGE", Message: "signed integer literal is outside the supported 64-bit range", Range: literal.Range}
			}
			return Value{Kind: ValueKindInt64, Int64: value, SourceText: literal.Text}, nil
		}
		value, err := strconv.ParseUint(strings.TrimPrefix(literal.Text, "+"), 10, 64)
		if err != nil {
			return Value{}, &Diagnostic{Code: "SPL_NUMBER_OUT_OF_RANGE", Message: "unsigned integer literal is outside the supported 64-bit range", Range: literal.Range}
		}
		if value <= math.MaxInt64 {
			return Value{Kind: ValueKindInt64, Int64: int64(value), SourceText: literal.Text}, nil
		}
		return Value{Kind: ValueKindUint64, Uint64: value, SourceText: literal.Text}, nil
	case spl.LiteralKindFloat:
		value, err := strconv.ParseFloat(literal.Text, 64)
		if err != nil || math.IsInf(value, 0) || math.IsNaN(value) {
			return Value{}, &Diagnostic{Code: "SPL_NUMBER_OUT_OF_RANGE", Message: "floating-point literal is not finite", Range: literal.Range}
		}
		return Value{Kind: ValueKindFloat64, Float64: value, SourceText: literal.Text}, nil
	case spl.LiteralKindBool:
		return Value{Kind: ValueKindBool, Bool: strings.EqualFold(literal.Text, "true"), SourceText: literal.Text}, nil
	case spl.LiteralKindNull:
		return Value{Kind: ValueKindNull, SourceText: literal.Text}, nil
	default:
		return Value{}, &Diagnostic{Code: "SPL_INVALID_LITERAL", Message: "invalid comparison literal", Range: literal.Range}
	}
}

func convertFields(names []string, sourceRange spl.Range) ([]FieldRef, error) {
	fields := make([]FieldRef, 0, len(names))
	seen := make(map[string]struct{}, len(names))
	for _, name := range names {
		if _, ok := seen[name]; ok {
			return nil, &Diagnostic{Code: "SPL_DUPLICATE_FIELD", Message: fmt.Sprintf("field %q is repeated", name), Range: sourceRange}
		}
		seen[name] = struct{}{}
		field, err := ResolveField(name, sourceRange)
		if err != nil {
			return nil, err
		}
		fields = append(fields, field)
	}
	return fields, nil
}

// ResolveField parses deterministic dotted dynamic access. A backslash escapes
// a literal dot or backslash within one path segment.
func ResolveField(name string, sourceRange spl.Range) (FieldRef, error) {
	if eventfields.IsCanonicalSPLField(name) {
		return FieldRef{Name: name, Canonical: true, Range: sourceRange}, nil
	}
	if name == "" || !utf8.ValidString(name) {
		return FieldRef{}, &Diagnostic{Code: "SPL_INVALID_FIELD", Message: "field name must be non-empty UTF-8", Range: sourceRange}
	}
	if len(name) > maxFieldNameBytes {
		return FieldRef{}, &Diagnostic{
			Code:    "SPL_QUERY_TOO_COMPLEX",
			Message: fmt.Sprintf("field name exceeds %d UTF-8 bytes", maxFieldNameBytes),
			Range:   sourceRange,
		}
	}
	if strings.HasPrefix(strings.ToLower(name), "__os_") {
		return FieldRef{}, &Diagnostic{Code: "SPL_RESERVED_FIELD", Message: "field name uses the compiler-private __os_ namespace", Range: sourceRange}
	}
	if strings.Contains(name, "*") {
		return FieldRef{}, &Diagnostic{Code: "SPL_UNSUPPORTED_FIELD_PATTERN", Message: "wildcard field-name patterns are not supported in compatibility version 0.1", Range: sourceRange}
	}
	path, err := splitFieldPath(name)
	if err != nil {
		return FieldRef{}, &Diagnostic{Code: "SPL_INVALID_FIELD", Message: err.Error(), Range: sourceRange}
	}
	if len(path) > maxFieldPathSegments {
		return FieldRef{}, &Diagnostic{
			Code:    "SPL_QUERY_TOO_COMPLEX",
			Message: fmt.Sprintf("field path contains more than %d segments", maxFieldPathSegments),
			Range:   sourceRange,
		}
	}
	for _, segment := range path {
		if len(segment) > maxFieldPathSegmentBytes {
			return FieldRef{}, &Diagnostic{
				Code:    "SPL_QUERY_TOO_COMPLEX",
				Message: fmt.Sprintf("field path segment exceeds %d UTF-8 bytes", maxFieldPathSegmentBytes),
				Range:   sourceRange,
			}
		}
	}
	return FieldRef{Name: name, Path: path, Range: sourceRange}, nil
}

func splitFieldPath(name string) ([]string, error) {
	var path []string
	var segment strings.Builder
	escaped := false
	for _, r := range name {
		if escaped {
			if r != '.' && r != '\\' {
				return nil, fmt.Errorf("field %q contains unsupported escape \\%c", name, r)
			}
			segment.WriteRune(r)
			escaped = false
			continue
		}
		switch r {
		case '\\':
			escaped = true
		case '.':
			if segment.Len() == 0 {
				return nil, fmt.Errorf("field %q contains an empty path segment", name)
			}
			path = append(path, segment.String())
			segment.Reset()
		default:
			if unicode.IsControl(r) {
				return nil, fmt.Errorf("field %q contains a control character", name)
			}
			segment.WriteRune(r)
		}
	}
	if escaped {
		return nil, fmt.Errorf("field %q ends with an incomplete escape", name)
	}
	if segment.Len() == 0 {
		return nil, fmt.Errorf("field %q contains an empty path segment", name)
	}
	path = append(path, segment.String())
	return path, nil
}
