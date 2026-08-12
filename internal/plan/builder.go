package plan

import (
	"errors"
	"fmt"
	"math"
	"reflect"
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
	maxFieldNameBytes                   = eventfields.MaximumNormalizedFieldNameBytes
	maxFieldPathSegments                = 17
	maxFieldPathSegmentBytes            = 256
	maxTimechartBuckets                 = 10_000
	maxTimechartSpan                    = 24 * time.Hour
	timechartSeriesLimit                = 10
	maxTimechartSeries                  = 12
	eventStatsSupportedAggregateMessage = "eventstats currently supports exactly one count, " +
		"count(field), count(eval(predicate)), pN(field), percN(field), min(field), " +
		"max(field), earliest(field), latest(field), sum(field), avg(field), " +
		"dc(field), values(field), or list(field) AS output measure"
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
			fields, fieldErr := convertTableFields(command)
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
			if aggregate.Sparkline != nil {
				return nil, &Diagnostic{
					Code:    "SPL_UNSUPPORTED_EVENTSTATS_AGGREGATE",
					Message: "sparkline is supported only by stats in this compatibility slice",
					Range:   aggregate.Range,
				}
			}
			if aggregate.InputGlob != nil || aggregate.AliasGlob != nil ||
				aggregate.InputQuoted || aggregate.AliasQuoted ||
				aggregate.AliasSourceDerived || aggregate.AliasWildcardDerived {
				return nil, &Diagnostic{
					Code:    "SPL_UNSUPPORTED_EVENTSTATS_AGGREGATE",
					Message: "quoted stats-only field provenance is not supported by eventstats",
					Range:   aggregate.Range,
				}
			}
			if aggregate.Alias == "" {
				return nil, &Diagnostic{
					Code:    "SPL_UNSUPPORTED_EVENTSTATS_AGGREGATE",
					Message: eventStatsSupportedAggregateMessage,
					Range:   aggregate.Range,
				}
			}
			measure := AggregateMeasure{Output: aggregate.Alias}
			switch aggregate.Function {
			case spl.AggregateFunctionCount:
				if aggregate.Input != "" ||
					aggregate.InputRange != (spl.Range{}) ||
					aggregate.InputExpression != nil ||
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
			case spl.AggregateFunctionPercentile:
				var inputErr error
				measure, inputErr = buildEventStatsFieldMeasure(
					aggregate,
					AggregateFunctionPercentile,
					"pN(field) or percN(field)",
				)
				if inputErr != nil {
					return nil, inputErr
				}
			case spl.AggregateFunctionMinimum, spl.AggregateFunctionMaximum,
				spl.AggregateFunctionEarliest,
				spl.AggregateFunctionLatest,
				spl.AggregateFunctionSum,
				spl.AggregateFunctionAverage,
				spl.AggregateFunctionDistinctCount,
				spl.AggregateFunctionValues,
				spl.AggregateFunctionList:
				var function AggregateFunction
				var form string
				switch aggregate.Function {
				case spl.AggregateFunctionMinimum:
					function = AggregateFunctionMinimum
					form = "min(field)"
				case spl.AggregateFunctionMaximum:
					function = AggregateFunctionMaximum
					form = "max(field)"
				case spl.AggregateFunctionEarliest:
					function = AggregateFunctionEarliest
					form = "earliest(field)"
				case spl.AggregateFunctionLatest:
					function = AggregateFunctionLatest
					form = "latest(field)"
				case spl.AggregateFunctionSum:
					function = AggregateFunctionSum
					form = "sum(field)"
				case spl.AggregateFunctionAverage:
					function = AggregateFunctionAverage
					form = "avg(field)"
				case spl.AggregateFunctionDistinctCount:
					function = AggregateFunctionDistinctCount
					form = "dc(field)"
				case spl.AggregateFunctionValues:
					function = AggregateFunctionValues
					form = "values(field)"
				case spl.AggregateFunctionList:
					function = AggregateFunctionList
					form = "list(field)"
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
					Code:    "SPL_UNSUPPORTED_EVENTSTATS_AGGREGATE",
					Message: eventStatsSupportedAggregateMessage,
					Range:   aggregate.Range,
				}
			}
			if (aggregate.Function == spl.AggregateFunctionEarliest ||
				aggregate.Function == spl.AggregateFunctionLatest) &&
				!canonicalTimeAvailable {
				return nil, &Diagnostic{
					Code: "SPL_UNSUPPORTED_EVENTSTATS_TIME_FIELD",
					Message: "eventstats earliest and latest require the " +
						"unmodified canonical _time field",
					Range: aggregate.Range,
					Suggestions: []string{
						"run eventstats earliest or latest before removing, replacing, or transforming _time",
					},
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
		case *spl.StreamStatsCommand:
			if command == nil {
				return nil, &Diagnostic{
					Code:    "SPL_INVALID_QUERY",
					Message: "streamstats command is nil",
				}
			}
			if len(command.GroupBy) > spl.MaximumStatsGroupFields {
				return nil, &Diagnostic{
					Code: "SPL_QUERY_TOO_COMPLEX",
					Message: fmt.Sprintf(
						"streamstats BY contains more than %d grouping fields",
						spl.MaximumStatsGroupFields,
					),
					Range: command.Range,
				}
			}
			if command.Window > spl.MaximumStreamStatsWindow {
				return nil, &Diagnostic{
					Code: "SPL_UNSUPPORTED_STREAMSTATS_SYNTAX",
					Message: fmt.Sprintf(
						"streamstats window must be between 0 and %d rows",
						spl.MaximumStreamStatsWindow,
					),
					Range: command.Range,
				}
			}
			if (!command.CurrentSpecified && !command.Current) ||
				(!command.WindowSpecified && command.Window != 0) ||
				(!command.GlobalSpecified && !command.Global) {
				return nil, &Diagnostic{
					Code:    "SPL_UNSUPPORTED_STREAMSTATS_SYNTAX",
					Message: "streamstats option values are inconsistent with their default metadata",
					Range:   command.Range,
				}
			}
			if len(command.GroupBy) > 0 && command.Window > 0 &&
				(!command.GlobalSpecified || command.Global) {
				return nil, &Diagnostic{
					Code:    "SPL_UNSUPPORTED_STREAMSTATS_SYNTAX",
					Message: "streamstats with BY and a positive window requires explicit global=false",
					Range:   command.Range,
				}
			}

			aggregate := command.Aggregate
			if aggregate.Sparkline != nil {
				return nil, &Diagnostic{
					Code:    "SPL_UNSUPPORTED_STREAMSTATS_AGGREGATE",
					Message: "sparkline is supported only by stats in this compatibility slice",
					Range:   aggregate.Range,
				}
			}
			if aggregate.InputGlob != nil || aggregate.AliasGlob != nil ||
				aggregate.InputQuoted || aggregate.AliasQuoted ||
				aggregate.AliasSourceDerived || aggregate.AliasWildcardDerived {
				return nil, &Diagnostic{
					Code:    "SPL_UNSUPPORTED_STREAMSTATS_AGGREGATE",
					Message: "quoted stats-only field provenance is not supported by streamstats",
					Range:   aggregate.Range,
				}
			}
			measure := AggregateMeasure{Output: aggregate.Alias}
			switch aggregate.Function {
			case spl.AggregateFunctionCount:
				if aggregate.Input != "" ||
					aggregate.InputRange != (spl.Range{}) ||
					aggregate.InputExpression != nil ||
					aggregate.Predicate != nil ||
					aggregate.Percentile != 0 ||
					aggregate.Alias == "" ||
					(!aggregate.ExplicitAlias && aggregate.Alias != "count") {
					return nil, &Diagnostic{
						Code: "SPL_UNSUPPORTED_STREAMSTATS_AGGREGATE",
						Message: "streamstats row count requires no input metadata " +
							"and uses its count default or one explicit alias",
						Range: aggregate.Range,
					}
				}
				measure.Function = AggregateFunctionCountRows
			case spl.AggregateFunctionCountPredicate:
				predicateMeasure, predicateErr := buildCountPredicateMeasure(
					aggregate,
					outputSchemaKnown,
					&expressionBudget,
					countPredicateMeasureDiagnostics{
						unsupportedCode: "SPL_UNSUPPORTED_STREAMSTATS_AGGREGATE",
						invalidMessage: "streamstats count(eval(...)) requires one " +
							"predicate, an explicit alias, and no field or percentile metadata",
						ambiguousCode: "SPL_AMBIGUOUS_STREAMSTATS_FIELD",
						reservedMessage: "streamstats cannot read the event result's " +
							"reserved fields payload without an exact upstream schema",
					},
				)
				if predicateErr != nil {
					return nil, predicateErr
				}
				measure = predicateMeasure
			case spl.AggregateFunctionCountValues, spl.AggregateFunctionSum,
				spl.AggregateFunctionAverage, spl.AggregateFunctionMinimum,
				spl.AggregateFunctionMaximum, spl.AggregateFunctionEarliest,
				spl.AggregateFunctionLatest:
				form := "count"
				function := AggregateFunctionCountValues
				switch aggregate.Function {
				case spl.AggregateFunctionSum:
					form = "sum"
					function = AggregateFunctionSum
				case spl.AggregateFunctionAverage:
					form = "avg"
					function = AggregateFunctionAverage
				case spl.AggregateFunctionMinimum:
					form = "min"
					function = AggregateFunctionMinimum
				case spl.AggregateFunctionMaximum:
					form = "max"
					function = AggregateFunctionMaximum
				case spl.AggregateFunctionEarliest:
					form = "earliest"
					function = AggregateFunctionEarliest
				case spl.AggregateFunctionLatest:
					form = "latest"
					function = AggregateFunctionLatest
				}
				if aggregate.Input == "" ||
					aggregate.InputRange == (spl.Range{}) ||
					aggregate.InputExpression != nil ||
					aggregate.Predicate != nil ||
					aggregate.Percentile != 0 ||
					aggregate.Alias == "" ||
					(!aggregate.ExplicitAlias &&
						aggregate.Alias != form+"("+aggregate.Input+")") {
					return nil, &Diagnostic{
						Code: "SPL_UNSUPPORTED_STREAMSTATS_AGGREGATE",
						Message: fmt.Sprintf(
							"streamstats %s(field) requires one exact input with its canonical default or one explicit alias",
							form,
						),
						Range: aggregate.Range,
					}
				}
				if !validStreamAggregateFieldName(aggregate.Input) {
					return nil, &Diagnostic{
						Code:    "SPL_UNSUPPORTED_STREAMSTATS_SYNTAX",
						Message: fmt.Sprintf("streamstats %s(field) requires one exact unquoted input field", form),
						Range:   aggregate.InputRange,
					}
				}
				input, inputErr := ResolveField(
					aggregate.Input,
					aggregate.InputRange,
				)
				if inputErr != nil {
					return nil, inputErr
				}
				measure.Function = function
				measure.Input = input
			default:
				return nil, &Diagnostic{
					Code: "SPL_UNSUPPORTED_STREAMSTATS_AGGREGATE",
					Message: "streamstats currently supports exactly one count, " +
						"count(field), count(eval(predicate)), sum(field), avg(field), min(field), max(field), " +
						"earliest(field), or latest(field) aggregate",
					Range: aggregate.Range,
				}
			}
			if (aggregate.Function == spl.AggregateFunctionEarliest ||
				aggregate.Function == spl.AggregateFunctionLatest) &&
				!canonicalTimeAvailable {
				return nil, &Diagnostic{
					Code: "SPL_UNSUPPORTED_STREAMSTATS_TIME_FIELD",
					Message: "streamstats earliest and latest require the " +
						"unmodified canonical _time field",
					Range: aggregate.Range,
					Suggestions: []string{
						"run streamstats earliest or latest before removing, replacing, or transforming _time",
					},
				}
			}
			validOutput := validStreamAggregateFieldName(aggregate.Alias)
			if !aggregate.ExplicitAlias && validStreamAggregateOutputName(measure) {
				validOutput = true
			}
			if !validOutput {
				return nil, &Diagnostic{
					Code:    "SPL_UNSUPPORTED_STREAMSTATS_SYNTAX",
					Message: "streamstats AS requires one exact unquoted output field",
					Range:   aggregate.AliasRange,
				}
			}
			if !outputSchemaKnown && aggregate.Alias == "fields" {
				return nil, &Diagnostic{
					Code: "SPL_AMBIGUOUS_STREAMSTATS_FIELD",
					Message: "streamstats cannot replace the event result's reserved " +
						"fields payload without an exact upstream schema",
					Range: aggregate.AliasRange,
				}
			}
			if !outputSchemaKnown && aggregate.Input == "fields" {
				return nil, &Diagnostic{
					Code: "SPL_AMBIGUOUS_STREAMSTATS_FIELD",
					Message: "streamstats cannot read the event result's reserved " +
						"fields payload without an exact upstream schema",
					Range: aggregate.InputRange,
				}
			}
			if _, aliasErr := ResolveField(
				aggregate.Alias,
				aggregate.AliasRange,
			); aliasErr != nil {
				return nil, aliasErr
			}
			for _, group := range command.GroupBy {
				if group.Quoted || !validStreamAggregateFieldName(group.Name) {
					return nil, &Diagnostic{
						Code:    "SPL_UNSUPPORTED_STREAMSTATS_SYNTAX",
						Message: "streamstats BY requires exact unquoted grouping fields",
						Range:   group.Range,
					}
				}
				if !outputSchemaKnown {
					if group.Name == "fields" {
						return nil, &Diagnostic{
							Code: "SPL_AMBIGUOUS_STREAMSTATS_FIELD",
							Message: "streamstats cannot group by the event result's " +
								"reserved fields payload without an exact upstream schema",
							Range: group.Range,
						}
					}
				}
			}
			groupBy, groupErr := convertStatsGroupFields(
				"streamstats",
				command.GroupBy,
			)
			if groupErr != nil {
				return nil, groupErr
			}
			result.Operators = append(
				result.Operators,
				&StreamAggregate{
					GroupBy:        groupBy,
					Measure:        measure,
					IncludeCurrent: command.Current,
					WindowRows:     command.Window,
					Global:         command.Global,
					Range:          command.Range,
				},
			)
			if outputSchemaKnown &&
				!slices.Contains(result.OutputFields, aggregate.Alias) {
				result.OutputFields = append(result.OutputFields, aggregate.Alias)
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
			aggregates, wildcardErr := expandStatsWildcardAggregates(
				command.Aggregates,
				outputSchemaKnown,
				result.OutputFields,
			)
			if wildcardErr != nil {
				return nil, wildcardErr
			}
			if len(command.GroupBy) > spl.MaximumStatsGroupFields {
				return nil, &Diagnostic{
					Code:    "SPL_QUERY_TOO_COMPLEX",
					Message: fmt.Sprintf("stats BY contains more than %d grouping fields", spl.MaximumStatsGroupFields),
					Range:   command.Range,
				}
			}
			statsOptions, optionsErr := buildStatsOptions(
				command.Options,
				command.Range,
			)
			if optionsErr != nil {
				return nil, optionsErr
			}
			if !outputSchemaKnown {
				for _, aggregate := range aggregates {
					if aggregate.Input == "fields" ||
						(aggregate.Sparkline != nil && aggregate.Sparkline.Input == "fields") {
						inputRange := aggregate.InputRange
						if aggregate.Sparkline != nil {
							inputRange = aggregate.Sparkline.InputRange
						}
						return nil, &Diagnostic{
							Code:    "SPL_AMBIGUOUS_STATS_FIELD",
							Message: "stats cannot read the event result's reserved fields payload without an exact upstream schema",
							Range:   inputRange,
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
			seenOutputs := make(map[string]struct{}, len(groupBy)+len(aggregates))
			outputFields := make([]string, 0, len(groupBy)+len(aggregates))
			for _, group := range groupBy {
				seenOutputs[group.Name] = struct{}{}
				outputFields = append(outputFields, group.Name)
			}
			measures := make([]AggregateMeasure, 0, len(aggregates))
			seenSources := make([]AggregateMeasure, 0, len(aggregates))
			for _, aggregate := range aggregates {
				if aggregate.InputGlob != nil || aggregate.AliasGlob != nil {
					return nil, &Diagnostic{
						Code:    "SPL_UNSUPPORTED_STATS_AGGREGATE",
						Message: "unexpanded stats wc-field metadata is invalid",
						Range:   aggregate.Range,
					}
				}
				if aggregate.Sparkline != nil &&
					(aggregate.Function != spl.AggregateFunctionInvalid ||
						aggregate.Input != "" || aggregate.InputRange != (spl.Range{}) ||
						aggregate.InputQuoted ||
						aggregate.InputExpression != nil || aggregate.Predicate != nil ||
						aggregate.Percentile != 0) {
					return nil, &Diagnostic{
						Code:    "SPL_UNSUPPORTED_STATS_AGGREGATE",
						Message: "sparkline metadata is mutually exclusive with ordinary aggregate metadata",
						Range:   aggregate.Range,
					}
				}
				if aggregate.InputExpression != nil &&
					nilSPLScalarExpression(aggregate.InputExpression) {
					return nil, &Diagnostic{
						Code:    "SPL_UNSUPPORTED_STATS_AGGREGATE",
						Message: "stats aggregate contains a missing scalar input expression",
						Range:   aggregate.Range,
					}
				}
				if aggregate.InputExpression != nil &&
					(aggregate.Input != "" || aggregate.InputRange != (spl.Range{}) ||
						aggregate.InputQuoted || aggregate.Predicate != nil) {
					return nil, &Diagnostic{
						Code:    "SPL_UNSUPPORTED_STATS_AGGREGATE",
						Message: "stats aggregate scalar input is mutually exclusive with exact-field and predicate metadata",
						Range:   aggregate.Range,
					}
				}
				if aggregate.Function != spl.AggregateFunctionCountPredicate &&
					aggregate.Predicate != nil {
					return nil, &Diagnostic{
						Code:    "SPL_UNSUPPORTED_STATS_AGGREGATE",
						Message: "only count(eval(...)) can contain predicate metadata",
						Range:   aggregate.Range,
					}
				}
				if aggregate.Input == "" && aggregate.InputQuoted {
					return nil, &Diagnostic{
						Code:    "SPL_UNSUPPORTED_STATS_AGGREGATE",
						Message: "stats aggregate contains quoted-input provenance without an exact input field",
						Range:   aggregate.Range,
					}
				}
				if statsAggregateUsesPercentileSuffix(aggregate.Function) {
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
						Message: "stats aggregate without a percentile suffix contains percentile metadata",
						Range:   aggregate.Range,
					}
				}
				if aggregate.Sparkline != nil {
					validAliasMetadata := aggregate.Alias != "" &&
						aggregate.AliasRange != (spl.Range{}) &&
						aggregate.Range != (spl.Range{}) &&
						aggregate.Range.Start == aggregate.Sparkline.Range.Start
					if aggregate.ExplicitAlias {
						validAliasMetadata = validAliasMetadata &&
							aggregate.Range.End == aggregate.AliasRange.End
					} else {
						validAliasMetadata = validAliasMetadata &&
							aggregate.Alias == "sparkline" &&
							aggregate.Range == aggregate.Sparkline.Range &&
							aggregate.AliasRange == aggregate.Sparkline.Range
					}
					if !validAliasMetadata {
						return nil, &Diagnostic{
							Code:    "SPL_UNSUPPORTED_STATS_AGGREGATE",
							Message: "stats sparkline alias metadata is invalid",
							Range:   aggregate.Range,
						}
					}
				}
				if aggregate.AliasQuoted &&
					(!aggregate.ExplicitAlias || aggregate.AliasSourceDerived ||
						aggregate.AliasWildcardDerived) {
					return nil, &Diagnostic{
						Code:    "SPL_UNSUPPORTED_STATS_AGGREGATE",
						Message: "quoted stats output provenance requires an explicit AS alias",
						Range:   aggregate.AliasRange,
					}
				}
				if aggregate.AliasSourceDerived &&
					(aggregate.ExplicitAlias || aggregate.AliasQuoted ||
						aggregate.AliasWildcardDerived ||
						(aggregate.InputExpression == nil &&
							aggregate.Function != spl.AggregateFunctionCountPredicate) ||
						aggregate.Range == (spl.Range{}) ||
						aggregate.AliasRange != aggregate.Range) {
					return nil, &Diagnostic{
						Code:    "SPL_UNSUPPORTED_STATS_AGGREGATE",
						Message: "source-derived stats output provenance is invalid",
						Range:   aggregate.AliasRange,
					}
				}
				if aggregate.AliasWildcardDerived &&
					!spl.ValidStatsWildcardDerivedAlias(aggregate) {
					return nil, &Diagnostic{
						Code:    "SPL_UNSUPPORTED_STATS_AGGREGATE",
						Message: "wc-field-derived stats output provenance is invalid",
						Range:   aggregate.AliasRange,
					}
				}
				hasEvalSource := aggregate.InputExpression != nil ||
					aggregate.Function == spl.AggregateFunctionCountPredicate
				if hasEvalSource && !aggregate.ExplicitAlias &&
					!aggregate.AliasSourceDerived && !aggregate.AliasWildcardDerived {
					return nil, &Diagnostic{
						Code:    "SPL_UNSUPPORTED_STATS_AGGREGATE",
						Message: "implicit stats eval output requires source-derived alias provenance",
						Range:   aggregate.AliasRange,
					}
				}
				literalOutput := aggregate.AliasQuoted || aggregate.AliasSourceDerived ||
					aggregate.AliasWildcardDerived
				if literalOutput {
					if aggregate.AliasRange == (spl.Range{}) ||
						!spl.IsStatsLiteralOutputName(aggregate.Alias) {
						return nil, &Diagnostic{
							Code:    "SPL_INVALID_FIELD",
							Message: "stats literal output field is empty, invalid, private, reserved, or too long",
							Range:   aggregate.AliasRange,
						}
					}
				} else if _, aliasErr := ResolveField(aggregate.Alias, aggregate.AliasRange); aliasErr != nil {
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
				measure := AggregateMeasure{
					Output:        aggregate.Alias,
					OutputLiteral: literalOutput,
				}
				if aggregate.Sparkline != nil {
					if !canonicalTimeAvailable {
						return nil, &Diagnostic{
							Code:    "SPL_UNSUPPORTED_STATS_TIME_FIELD",
							Message: "stats sparkline requires the unmodified canonical _time field",
							Range:   aggregate.Sparkline.Range,
							Suggestions: []string{
								"run stats sparkline before removing, replacing, or transforming _time",
							},
						}
					}
					sparkline, sparklineErr := buildStatsSparklineMeasure(
						aggregate.Sparkline,
					)
					if sparklineErr != nil {
						return nil, sparklineErr
					}
					measure.Sparkline = sparkline
				} else {
					switch aggregate.Function {
					case spl.AggregateFunctionCount:
						if aggregate.Input != "" || aggregate.InputRange != (spl.Range{}) ||
							aggregate.InputQuoted || aggregate.InputExpression != nil {
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
								unsupportedCode:    "SPL_UNSUPPORTED_STATS_AGGREGATE",
								invalidMessage:     "count(eval(...)) requires one predicate, a valid explicit or source-derived output, and no field or percentile metadata",
								ambiguousCode:      "SPL_AMBIGUOUS_STATS_FIELD",
								reservedMessage:    "stats cannot read the event result's reserved fields payload without an exact upstream schema",
								allowImplicitAlias: true,
							},
						)
						if predicateErr != nil {
							return nil, predicateErr
						}
						measure = predicateMeasure
					case spl.AggregateFunctionCountValues:
						if aggregate.Input == "" || aggregate.InputRange == (spl.Range{}) ||
							aggregate.InputExpression != nil {
							return nil, &Diagnostic{
								Code:    "SPL_UNSUPPORTED_STATS_AGGREGATE",
								Message: "count(field) requires one exact input field",
								Range:   aggregate.Range,
							}
						}
						if aggregate.InputQuoted {
							if !spl.IsStatsLiteralFieldReference(aggregate.Input) {
								return nil, &Diagnostic{
									Code:    "SPL_INVALID_FIELD",
									Message: "quoted stats aggregate input is invalid",
									Range:   aggregate.InputRange,
								}
							}
						} else if !spl.IsExactUnquotedFieldName(aggregate.Input) {
							return nil, &Diagnostic{
								Code:    "SPL_UNSUPPORTED_STATS_AGGREGATE",
								Message: "stats aggregate input is not an exact unquoted field",
								Range:   aggregate.InputRange,
							}
						}
						input, inputErr := resolveStatsInputField(
							aggregate.Input,
							aggregate.InputRange,
							aggregate.InputQuoted,
						)
						if inputErr != nil {
							return nil, inputErr
						}
						measure.Function = AggregateFunctionCountValues
						measure.Input = input
					default:
						function, supported := convertStatsFieldAggregateFunction(
							aggregate.Function,
						)
						if !supported {
							return nil, &Diagnostic{
								Code:    "SPL_UNSUPPORTED_STATS_AGGREGATE",
								Message: "unsupported stats aggregate",
								Range:   aggregate.Range,
							}
						}
						hasExactInput := aggregate.Input != "" ||
							aggregate.InputRange != (spl.Range{})
						hasExpressionInput := aggregate.InputExpression != nil
						if hasExactInput == hasExpressionInput ||
							(hasExactInput && (aggregate.Input == "" ||
								aggregate.InputRange == (spl.Range{}))) {
							return nil, &Diagnostic{
								Code:    "SPL_UNSUPPORTED_STATS_AGGREGATE",
								Message: "stats aggregate requires exactly one exact-field or eval scalar input",
								Range:   aggregate.Range,
							}
						}
						if statsAggregateRequiresCanonicalTime(aggregate.Function) &&
							!canonicalTimeAvailable {
							return nil, &Diagnostic{
								Code: "SPL_UNSUPPORTED_STATS_TIME_FIELD",
								Message: "time-sensitive stats aggregates require the " +
									"unmodified canonical _time field",
								Range: aggregate.Range,
								Suggestions: []string{
									"run stats earliest or latest before removing, replacing, or transforming _time",
									"run the time-sensitive stats aggregate before removing, replacing, or transforming _time",
								},
							}
						}
						measure.Function = function
						if hasExpressionInput {
							if complexityErr := validateSPLScalarExpressionComplexity(
								aggregate.InputExpression,
								&expressionBudget,
							); complexityErr != nil {
								return nil, complexityErr
							}
							expression, expressionErr := convertScalarExpressionUnchecked(
								aggregate.InputExpression,
							)
							if expressionErr != nil {
								return nil, expressionErr
							}
							if !outputSchemaKnown {
								fieldRange, referencesReserved, inventoryErr := predicateFieldRange(
									&ScalarPredicateExpression{
										Value: expression,
										Range: expression.SourceRange(),
									},
									"fields",
								)
								if inventoryErr != nil {
									return nil, inventoryErr
								}
								if referencesReserved {
									return nil, &Diagnostic{
										Code:    "SPL_AMBIGUOUS_STATS_FIELD",
										Message: "stats cannot read the event result's reserved fields payload without an exact upstream schema",
										Range:   fieldRange,
									}
								}
							}
							measure.InputExpression = expression
						} else {
							if aggregate.InputQuoted {
								if !spl.IsStatsLiteralFieldReference(aggregate.Input) {
									return nil, &Diagnostic{
										Code:    "SPL_INVALID_FIELD",
										Message: "quoted stats aggregate input is invalid",
										Range:   aggregate.InputRange,
									}
								}
							} else if !spl.IsExactUnquotedFieldName(aggregate.Input) {
								return nil, &Diagnostic{
									Code:    "SPL_UNSUPPORTED_STATS_AGGREGATE",
									Message: "stats aggregate input is not an exact unquoted field",
									Range:   aggregate.InputRange,
								}
							}
							input, inputErr := resolveStatsInputField(
								aggregate.Input,
								aggregate.InputRange,
								aggregate.InputQuoted,
							)
							if inputErr != nil {
								return nil, inputErr
							}
							measure.Input = input
						}
						if statsAggregateUsesPercentileSuffix(aggregate.Function) {
							measure.Percentile = aggregate.Percentile
						}
					}
				}
				for _, source := range seenSources {
					if sameStatsAggregateSource(source, measure) {
						return nil, &Diagnostic{
							Code:    "SPL_DUPLICATE_STATS_AGGREGATE",
							Message: "the same stats aggregate source cannot be renamed to multiple output fields",
							Range:   aggregate.Range,
						}
					}
				}
				seenSources = append(seenSources, measure)
				measures = append(measures, measure)
				outputFields = append(outputFields, aggregate.Alias)
			}
			result.OutputFields = outputFields
			outputSchemaKnown = true
			result.Operators = append(result.Operators, &Aggregate{
				GroupBy:      groupBy,
				Measures:     measures,
				StatsOptions: statsOptions,
				Range:        command.Range,
			})
			canonicalTimeAvailable = false
		case *spl.TopCommand, *spl.RareCommand:
			var commandName string
			var frequencyFields []spl.FrequencyField
			var commandRange spl.Range
			var limit uint64
			leastFrequent := false
			switch command := command.(type) {
			case *spl.TopCommand:
				if command == nil {
					return nil, &Diagnostic{
						Code:    "SPL_INVALID_QUERY",
						Message: "top command is nil",
					}
				}
				commandName = command.Name()
				frequencyFields = command.Fields
				commandRange = command.Range
				limit = command.Limit
			case *spl.RareCommand:
				if command == nil {
					return nil, &Diagnostic{
						Code:    "SPL_INVALID_QUERY",
						Message: "rare command is nil",
					}
				}
				commandName = command.Name()
				frequencyFields = command.Fields
				commandRange = command.Range
				limit = command.Limit
				leastFrequent = true
			}
			if len(frequencyFields) == 0 {
				return nil, &Diagnostic{
					Code:    "SPL_EXPECTED_FIELD",
					Message: commandName + " requires at least one field",
					Range:   commandRange,
				}
			}
			if len(frequencyFields) > spl.MaximumFrequencyFields {
				return nil, &Diagnostic{
					Code: "SPL_QUERY_TOO_COMPLEX",
					Message: fmt.Sprintf(
						"%s contains more than %d fields",
						commandName,
						spl.MaximumFrequencyFields,
					),
					Range: commandRange,
				}
			}
			canonicalTimeAvailable = false
			outputFields := make([]string, 0, len(frequencyFields)+2)
			for _, frequencyField := range frequencyFields {
				fieldName := frequencyField.Name
				fieldRange := frequencyField.Range
				if fieldName == "count" || fieldName == "percent" {
					return nil, &Diagnostic{
						Code:    "SPL_DUPLICATE_FIELD",
						Message: fmt.Sprintf("%s field %q collides with a generated output field", commandName, fieldName),
						Range:   fieldRange,
					}
				}
				outputFields = append(outputFields, fieldName)
			}
			fields, fieldErr := convertStatsGroupFields(
				commandName,
				frequencyFields,
			)
			if fieldErr != nil {
				return nil, fieldErr
			}
			countField, countErr := ResolveField("count", commandRange)
			if countErr != nil {
				return nil, countErr
			}
			result.OutputFields = append(outputFields, "count", "percent")
			outputSchemaKnown = true
			sortKeys := make([]SortKey, 0, len(fields)+1)
			sortKeys = append(sortKeys, SortKey{
				Field:      countField,
				Descending: !leastFrequent,
			})
			for _, field := range fields {
				sortKeys = append(sortKeys, SortKey{
					Field:      field,
					Descending: true,
					Mode:       SortValueModeLexical,
				})
			}
			result.Operators = append(result.Operators,
				&Aggregate{
					GroupBy: fields,
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
					Keys:  sortKeys,
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
				if command.SplitBy.Quoted {
					return nil, &Diagnostic{
						Code:    "SPL_UNSUPPORTED_TIMECHART_FIELD_TYPE",
						Message: "timechart split fields must use unquoted exact-field syntax",
						Range:   command.SplitBy.Range,
					}
				}
				resolved, splitErr := ResolveField(
					command.SplitBy.Name,
					command.SplitBy.Range,
				)
				if splitErr != nil {
					return nil, splitErr
				}
				if measure.Input.Name != "" && resolved.Name == measure.Input.Name {
					return nil, &Diagnostic{
						Code:    "SPL_DUPLICATE_FIELD",
						Message: fmt.Sprintf("timechart aggregate input and split field %q are repeated", resolved.Name),
						Range:   command.SplitBy.Range,
						Suggestions: []string{
							"use a different split field or copy the aggregate input before timechart",
						},
					}
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
			measure, measureErr := buildChartMeasure(command, outputSchemaKnown)
			if measureErr != nil {
				return nil, measureErr
			}
			// usenull and useother default to true, so NULL and OTHER are
			// always reachable public column names. A field spelled like one
			// of them would collide with a series deterministically.
			for _, axis := range []spl.StatsGroupField{command.Over, command.SplitBy} {
				if axis.Quoted {
					return nil, &Diagnostic{
						Code:    "SPL_UNSUPPORTED_CHART_FIELD_TYPE",
						Message: "chart axes must use unquoted exact-field syntax",
						Range:   axis.Range,
					}
				}
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
			if measure.Function != AggregateFunctionCountRows &&
				measure.Input.Name == over.Name {
				return nil, &Diagnostic{
					Code:    "SPL_DUPLICATE_FIELD",
					Message: fmt.Sprintf("chart aggregate input and row field %q are repeated", over.Name),
					Range:   command.Over.Range,
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
				Measure:      measure,
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
	if predicates, ok := query.ParsedEvalPredicateCount(); ok {
		result.parsedEvalPredicates = predicates
		result.parsedSPL = true
	}
	if sourceDigest, ok := query.ParsedSourceDigest(); ok {
		result.parsedSourceDigest = sourceDigest
	}
	return result, nil
}

func buildChartMeasure(
	command *spl.ChartCommand,
	outputSchemaKnown bool,
) (AggregateMeasure, error) {
	if command == nil {
		return AggregateMeasure{}, &Diagnostic{
			Code:    "SPL_INVALID_QUERY",
			Message: "chart command is nil",
		}
	}
	aggregate := command.Aggregate
	invalid := func(message string) (AggregateMeasure, error) {
		return AggregateMeasure{}, &Diagnostic{
			Code:    "SPL_UNSUPPORTED_CHART_AGGREGATE",
			Message: message,
			Range:   aggregate.Range,
		}
	}
	if aggregate.Range == (spl.Range{}) || aggregate.AliasRange == (spl.Range{}) ||
		aggregate.Sparkline != nil ||
		aggregate.InputGlob != nil || aggregate.AliasGlob != nil ||
		aggregate.Predicate != nil || aggregate.InputExpression != nil ||
		aggregate.InputQuoted || aggregate.AliasQuoted ||
		aggregate.AliasSourceDerived || aggregate.AliasWildcardDerived {
		return invalid("chart aggregate metadata is invalid")
	}

	switch aggregate.Function {
	case spl.AggregateFunctionCount:
		if aggregate.Input != "" || aggregate.InputRange != (spl.Range{}) ||
			aggregate.Percentile != 0 || aggregate.Alias != "count" ||
			aggregate.ExplicitAlias {
			return invalid("chart count must be argument-free and unaliased")
		}
		return AggregateMeasure{
			Function: AggregateFunctionCountRows,
			Output:   "count",
		}, nil
	case spl.AggregateFunctionCountValues,
		spl.AggregateFunctionPercentile,
		spl.AggregateFunctionSum,
		spl.AggregateFunctionAverage:
		canonicalName := "sum"
		function := AggregateFunctionSum
		switch aggregate.Function {
		case spl.AggregateFunctionCountValues:
			canonicalName = "count"
			function = AggregateFunctionCountValues
		case spl.AggregateFunctionPercentile:
			if aggregate.Percentile < 1 || aggregate.Percentile > 99 {
				return invalid("chart percentile must be from 1 through 99")
			}
			canonicalName = "perc" + strconv.Itoa(int(aggregate.Percentile))
			function = AggregateFunctionPercentile
		case spl.AggregateFunctionAverage:
			canonicalName = "avg"
			function = AggregateFunctionAverage
		}
		canonicalOutput := canonicalName + "(" + aggregate.Input + ")"
		if aggregate.Input == "" || aggregate.InputRange == (spl.Range{}) ||
			(function != AggregateFunctionPercentile && aggregate.Percentile != 0) ||
			aggregate.ExplicitAlias ||
			aggregate.Alias != canonicalOutput {
			return invalid("chart field aggregates require one exact input field and no alias")
		}
		if !spl.IsExactUnquotedFieldName(aggregate.Input) {
			return invalid("chart field aggregates require one exact unquoted input field")
		}
		input, err := ResolveField(aggregate.Input, aggregate.InputRange)
		if err != nil {
			return AggregateMeasure{}, err
		}
		if !outputSchemaKnown && input.Name == "fields" {
			return AggregateMeasure{}, &Diagnostic{
				Code:    "SPL_AMBIGUOUS_CHART_FIELD",
				Message: "chart cannot read the event result's reserved fields payload without an exact upstream schema",
				Range:   aggregate.InputRange,
				Suggestions: []string{
					"select an exact ordinary field with table before chart",
					"produce a closed stats schema before charting fields",
				},
			}
		}
		return AggregateMeasure{
			Function:   function,
			Input:      input,
			Percentile: aggregate.Percentile,
			Output:     canonicalOutput,
		}, nil
	default:
		return invalid("unsupported chart aggregate")
	}
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
	if aggregate.Sparkline != nil ||
		aggregate.InputGlob != nil || aggregate.AliasGlob != nil ||
		aggregate.Predicate != nil || aggregate.InputExpression != nil ||
		aggregate.InputQuoted || aggregate.AliasQuoted ||
		aggregate.AliasSourceDerived || aggregate.AliasWildcardDerived {
		return AggregateMeasure{}, &Diagnostic{
			Code:    "SPL_UNSUPPORTED_TIMECHART_AGGREGATE",
			Message: "timechart aggregate cannot contain predicate or scalar-expression metadata",
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
		return buildTimechartFieldMeasure(
			command,
			AggregateFunctionPercentile,
			canonicalOutput,
			aggregate.Percentile,
			outputSchemaKnown,
		)
	case spl.AggregateFunctionCountValues, spl.AggregateFunctionSum,
		spl.AggregateFunctionAverage:
		if aggregate.Input == "" ||
			aggregate.InputRange == (spl.Range{}) ||
			aggregate.Percentile != 0 ||
			aggregate.Alias == "" {
			return AggregateMeasure{}, &Diagnostic{
				Code: "SPL_UNSUPPORTED_TIMECHART_AGGREGATE",
				Message: "timechart field aggregate requires one exact input " +
					"field, no percentile metadata, and one output",
				Range: aggregate.Range,
			}
		}
		function := AggregateFunctionCountValues
		canonicalName := "count"
		switch aggregate.Function {
		case spl.AggregateFunctionSum:
			function = AggregateFunctionSum
			canonicalName = "sum"
		case spl.AggregateFunctionAverage:
			function = AggregateFunctionAverage
			canonicalName = "avg"
		}
		return buildTimechartFieldMeasure(
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

func buildTimechartFieldMeasure(
	command *spl.TimechartCommand,
	function AggregateFunction,
	canonicalOutput string,
	percentile uint8,
	outputSchemaKnown bool,
) (AggregateMeasure, error) {
	aggregate := command.Aggregate
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
		if field.Quoted {
			if commandName != "stats" || !spl.IsStatsLiteralFieldReference(field.Name) {
				return nil, &Diagnostic{
					Code:    "SPL_INVALID_FIELD",
					Message: commandName + " grouping field has invalid quoted-field provenance",
					Range:   field.Range,
				}
			}
		} else if commandName == "stats" &&
			!spl.IsExactUnquotedFieldName(field.Name) {
			return nil, &Diagnostic{
				Code:    "SPL_UNSUPPORTED_STATS_SYNTAX",
				Message: "stats BY requires exact quoted or unquoted fields; wildcard fields are not supported",
				Range:   field.Range,
			}
		}
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
		resolved, err := resolveStatsInputField(field.Name, field.Range, field.Quoted)
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
		case *spl.StreamStatsCommand:
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
		return nil, &Diagnostic{Code: "SPL_UNSUPPORTED_EXPRESSION", Message: fmt.Sprintf("unsupported expression type %T", expression), Range: safeSPLNodeRange(expression)}
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
	case *spl.WhereMembershipExpr:
		if expression == nil {
			return nil, &Diagnostic{
				Code:    "SPL_UNSUPPORTED_MEMBERSHIP_SYNTAX",
				Message: "membership expression is missing",
			}
		}
		if !validExpressionRangeOrZero(expression.Range) {
			return nil, &Diagnostic{
				Code:    "SPL_UNSUPPORTED_MEMBERSHIP_SYNTAX",
				Message: "membership expression has an invalid source range",
				Range:   expression.Range,
			}
		}
		if len(expression.Candidates) < 1 {
			return nil, &Diagnostic{
				Code:    "SPL_UNSUPPORTED_MEMBERSHIP_SYNTAX",
				Message: "membership requires at least one candidate",
				Range:   expression.Range,
			}
		}
		if len(expression.Candidates) > spl.MaximumMembershipCandidates {
			return nil, splExpressionComplexityError(
				fmt.Sprintf(
					"membership contains more than %d candidates",
					spl.MaximumMembershipCandidates,
				),
				expression.Range,
			)
		}
		value, err := convertScalarExpressionUnchecked(expression.Value)
		if err != nil {
			return nil, err
		}
		candidates := make([]ScalarExpression, len(expression.Candidates))
		for index, candidate := range expression.Candidates {
			converted, candidateErr := convertScalarExpressionUnchecked(candidate)
			if candidateErr != nil {
				return nil, candidateErr
			}
			candidates[index] = converted
		}
		return &MembershipExpression{
			Value:      value,
			Candidates: candidates,
			Negated:    expression.Negated,
			Range:      expression.Range,
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
		return nil, &Diagnostic{Code: "SPL_UNSUPPORTED_WHERE_EXPRESSION", Message: fmt.Sprintf("unsupported where expression type %T", expression), Range: safeSPLNodeRange(expression)}
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
	case *spl.WhereMembershipExpr:
		return expression == nil
	case *spl.WhereScalarPredicateExpr:
		return expression == nil
	default:
		return nilInterfaceValue(expression)
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
	case *spl.ScalarUnaryExpr:
		return expression == nil
	case *spl.ScalarBinaryExpr:
		return expression == nil
	case *spl.ScalarCallExpr:
		return expression == nil
	case *spl.ScalarIfExpr:
		return expression == nil
	case *spl.ScalarCaseExpr:
		return expression == nil
	default:
		return nilInterfaceValue(expression)
	}
}

func nilInterfaceValue(value any) bool {
	if value == nil {
		return true
	}
	reflected := reflect.ValueOf(value)
	switch reflected.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map,
		reflect.Pointer, reflect.Slice:
		return reflected.IsNil()
	default:
		return false
	}
}

func safeSPLNodeRange(node spl.Node) spl.Range {
	if nilInterfaceValue(node) {
		return spl.Range{}
	}
	return node.SourceRange()
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
	arithmeticOperators   int
	membershipCandidates  int
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
	return validator.validateScalar(expression, 1, 0)
}

func (v *splExpressionComplexityValidator) validateWhere(
	expression spl.WhereExpr,
	depth int,
) error {
	if nilSPLWhereExpression(expression) {
		return nil
	}
	if err := v.enter(expression, depth, safeSPLNodeRange(expression)); err != nil {
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
		if err := v.validateScalar(expression.Left, depth+1, 0); err != nil {
			return err
		}
		return v.validateScalar(expression.Right, depth+1, 0)
	case *spl.WhereMembershipExpr:
		if !validExpressionRangeOrZero(expression.Range) {
			return &Diagnostic{
				Code:    "SPL_UNSUPPORTED_MEMBERSHIP_SYNTAX",
				Message: "membership expression has an invalid source range",
				Range:   expression.Range,
			}
		}
		if len(expression.Candidates) < 1 {
			return &Diagnostic{
				Code:    "SPL_UNSUPPORTED_MEMBERSHIP_SYNTAX",
				Message: "membership requires at least one candidate",
				Range:   expression.Range,
			}
		}
		if len(expression.Candidates) > spl.MaximumMembershipCandidates {
			return splExpressionComplexityError(
				fmt.Sprintf(
					"membership contains more than %d candidates",
					spl.MaximumMembershipCandidates,
				),
				expression.Range,
			)
		}
		if err := v.chargeMembershipCandidates(
			len(expression.Candidates),
			expression.Range,
		); err != nil {
			return err
		}
		if err := v.validateScalar(expression.Value, depth+1, 0); err != nil {
			return err
		}
		for _, candidate := range expression.Candidates {
			if err := v.validateScalar(candidate, depth+1, 0); err != nil {
				return err
			}
		}
		return nil
	case *spl.WhereScalarPredicateExpr:
		return v.validateScalar(expression.Value, depth+1, 0)
	default:
		return &Diagnostic{
			Code:    "SPL_UNSUPPORTED_WHERE_EXPRESSION",
			Message: fmt.Sprintf("unsupported where expression type %T", expression),
			Range:   safeSPLNodeRange(expression),
		}
	}
}

func (v *splExpressionComplexityValidator) validateScalar(
	expression spl.ScalarExpr,
	depth int,
	unaryChain int,
) error {
	if nilSPLScalarExpression(expression) {
		return nil
	}
	if err := v.enter(expression, depth, safeSPLNodeRange(expression)); err != nil {
		return err
	}
	defer v.leave(expression)

	switch expression := expression.(type) {
	case *spl.ScalarFieldExpr, *spl.ScalarLiteralExpr:
		return nil
	case *spl.ScalarUnaryExpr:
		if !validExpressionRangeOrZero(expression.Range) {
			return &Diagnostic{
				Code:    "SPL_UNSUPPORTED_ARITHMETIC_SYNTAX",
				Message: "unary arithmetic expression has an invalid source range",
				Range:   expression.Range,
			}
		}
		if convertScalarUnaryOp(expression.Op) == ScalarUnaryOpInvalid {
			return &Diagnostic{
				Code:    "SPL_UNSUPPORTED_ARITHMETIC_SYNTAX",
				Message: "unary arithmetic expression has an invalid operator",
				Range:   expression.Range,
			}
		}
		if unaryChain >= spl.MaximumUnaryOperatorChain {
			return splExpressionComplexityError(
				fmt.Sprintf(
					"unary arithmetic chain exceeds %d operators",
					spl.MaximumUnaryOperatorChain,
				),
				expression.Range,
			)
		}
		if err := v.chargeArithmeticOperator(expression.Range); err != nil {
			return err
		}
		return v.validateScalar(expression.Operand, depth+1, unaryChain+1)
	case *spl.ScalarBinaryExpr:
		if !validExpressionRangeOrZero(expression.Range) {
			return &Diagnostic{
				Code:    "SPL_UNSUPPORTED_ARITHMETIC_SYNTAX",
				Message: "binary arithmetic expression has an invalid source range",
				Range:   expression.Range,
			}
		}
		if convertScalarBinaryOp(expression.Op) == ScalarBinaryOpInvalid {
			return &Diagnostic{
				Code:    "SPL_UNSUPPORTED_ARITHMETIC_SYNTAX",
				Message: "binary arithmetic expression has an invalid operator",
				Range:   expression.Range,
			}
		}
		if err := v.chargeArithmeticOperator(expression.Range); err != nil {
			return err
		}
		if err := v.validateScalar(expression.Left, depth+1, 0); err != nil {
			return err
		}
		return v.validateScalar(expression.Right, depth+1, 0)
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
			if err := v.validateScalar(argument, depth+1, 0); err != nil {
				return err
			}
		}
		return nil
	case *spl.ScalarIfExpr:
		if err := v.validateWhere(expression.Condition, depth+1); err != nil {
			return err
		}
		if err := v.validateScalar(expression.True, depth+1, 0); err != nil {
			return err
		}
		return v.validateScalar(expression.False, depth+1, 0)
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
			if err := v.validateScalar(branch.Value, depth+1, 0); err != nil {
				return err
			}
		}
		return nil
	default:
		return &Diagnostic{
			Code:    "SPL_UNSUPPORTED_EVAL_EXPRESSION",
			Message: fmt.Sprintf("unsupported scalar expression type %T", expression),
			Range:   safeSPLNodeRange(expression),
		}
	}
}

func (v *splExpressionComplexityValidator) chargeArithmeticOperator(
	sourceRange spl.Range,
) error {
	if v.budget == nil {
		return nil
	}
	if v.budget.arithmeticOperators >= spl.MaximumArithmeticOperatorsPerQuery {
		return splExpressionComplexityError(
			fmt.Sprintf(
				"arithmetic contains more than %d operator occurrences per query",
				spl.MaximumArithmeticOperatorsPerQuery,
			),
			sourceRange,
		)
	}
	v.budget.arithmeticOperators++
	return nil
}

func (v *splExpressionComplexityValidator) chargeMembershipCandidates(
	count int,
	sourceRange spl.Range,
) error {
	if v.budget == nil {
		return nil
	}
	if v.budget.membershipCandidates >
		spl.MaximumMembershipCandidatesPerQuery-count {
		return splExpressionComplexityError(
			fmt.Sprintf(
				"membership contains more than %d candidate occurrences per query",
				spl.MaximumMembershipCandidatesPerQuery,
			),
			sourceRange,
		)
	}
	v.budget.membershipCandidates += count
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
	if node == nil || !reflect.TypeOf(node).Comparable() {
		return &Diagnostic{
			Code:    "SPL_UNSUPPORTED_EVAL_EXPRESSION",
			Message: fmt.Sprintf("unsupported expression node %T", node),
			Range:   sourceRange,
		}
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

func statsAggregateUsesPercentileSuffix(function spl.AggregateFunction) bool {
	switch function {
	case spl.AggregateFunctionPercentile,
		spl.AggregateFunctionExactPercentile,
		spl.AggregateFunctionUpperPercentile:
		return true
	default:
		return false
	}
}

func buildStatsSparklineMeasure(
	spec *spl.StatsSparkline,
) (*SparklineMeasure, error) {
	if spec == nil || spec.Range == (spl.Range{}) {
		return nil, &Diagnostic{
			Code:    "SPL_UNSUPPORTED_STATS_AGGREGATE",
			Message: "stats sparkline metadata is missing",
		}
	}
	span, spanErr := convertStatsSparklineSpan(spec.Span, spec.Range)
	if spanErr != nil {
		return nil, spanErr
	}
	function, requiresInput, supported := convertStatsSparklineFunction(spec.Function)
	if !supported {
		return nil, &Diagnostic{
			Code:    "SPL_UNSUPPORTED_STATS_AGGREGATE",
			Message: "unsupported aggregate inside stats sparkline",
			Range:   spec.Range,
		}
	}
	hasInput := spec.Input != "" || spec.InputQuoted ||
		spec.InputRange != (spl.Range{}) || spec.InputGlob != nil
	if spec.InputGlob != nil {
		return nil, &Diagnostic{
			Code:    "SPL_UNSUPPORTED_STATS_AGGREGATE",
			Message: "stats sparkline contains unexpanded wc-field metadata",
			Range:   spec.InputGlob.Range,
		}
	}
	if hasInput != requiresInput ||
		(hasInput && (spec.Input == "" || spec.InputRange == (spl.Range{}))) {
		return nil, &Diagnostic{
			Code:    "SPL_UNSUPPORTED_STATS_AGGREGATE",
			Message: "stats sparkline inner aggregate contains invalid input metadata",
			Range:   spec.Range,
		}
	}
	var input FieldRef
	if hasInput {
		if spec.InputQuoted {
			if !spl.IsStatsLiteralFieldReference(spec.Input) {
				return nil, &Diagnostic{
					Code:    "SPL_INVALID_FIELD",
					Message: "quoted stats sparkline input is invalid",
					Range:   spec.InputRange,
				}
			}
		} else if !spl.IsExactUnquotedFieldName(spec.Input) {
			return nil, &Diagnostic{
				Code:    "SPL_UNSUPPORTED_STATS_AGGREGATE",
				Message: "stats sparkline input is not an exact unquoted field",
				Range:   spec.InputRange,
			}
		}
		var inputErr error
		input, inputErr = resolveStatsInputField(
			spec.Input,
			spec.InputRange,
			spec.InputQuoted,
		)
		if inputErr != nil {
			return nil, inputErr
		}
	}
	timeField, timeErr := ResolveField("_time", spec.Range)
	if timeErr != nil {
		return nil, timeErr
	}
	return &SparklineMeasure{
		Function:      function,
		Input:         input,
		Time:          timeField,
		Span:          span,
		MaximumPoints: spl.MaximumStatsSparklinePoints,
	}, nil
}

func convertStatsSparklineFunction(
	function spl.AggregateFunction,
) (AggregateFunction, bool, bool) {
	switch function {
	case spl.AggregateFunctionCount:
		return AggregateFunctionCountRows, false, true
	case spl.AggregateFunctionCountValues:
		return AggregateFunctionCountValues, true, true
	case spl.AggregateFunctionDistinctCount:
		return AggregateFunctionDistinctCount, true, true
	case spl.AggregateFunctionAverage:
		return AggregateFunctionAverage, true, true
	case spl.AggregateFunctionStandardDeviationSample:
		return AggregateFunctionStandardDeviationSample, true, true
	case spl.AggregateFunctionStandardDeviationPopulation:
		return AggregateFunctionStandardDeviationPopulation, true, true
	case spl.AggregateFunctionVarianceSample:
		return AggregateFunctionVarianceSample, true, true
	case spl.AggregateFunctionVariancePopulation:
		return AggregateFunctionVariancePopulation, true, true
	case spl.AggregateFunctionSum:
		return AggregateFunctionSum, true, true
	case spl.AggregateFunctionSumSquares:
		return AggregateFunctionSumSquares, true, true
	case spl.AggregateFunctionMinimum:
		return AggregateFunctionMinimum, true, true
	case spl.AggregateFunctionMaximum:
		return AggregateFunctionMaximum, true, true
	case spl.AggregateFunctionRange:
		return AggregateFunctionRange, true, true
	default:
		return AggregateFunctionInvalid, false, false
	}
}

func convertStatsSparklineSpan(
	span spl.SparklineSpan,
	sourceRange spl.Range,
) (SparklineSpan, error) {
	invalid := func(message string) (SparklineSpan, error) {
		return SparklineSpan{}, &Diagnostic{
			Code:    "SPL_UNSUPPORTED_STATS_AGGREGATE",
			Message: message,
			Range:   sourceRange,
		}
	}
	switch span.Kind {
	case spl.SparklineSpanKindAutomatic:
		if span.Magnitude != 0 || span.Unit != spl.SparklineSpanUnitInvalid ||
			span.Range != (spl.Range{}) {
			return invalid("automatic stats sparkline span contains explicit metadata")
		}
		return SparklineSpan{Kind: SparklineSpanKindAutomatic}, nil
	case spl.SparklineSpanKindExplicit:
		if span.Magnitude == 0 || span.Range == (spl.Range{}) {
			return invalid("explicit stats sparkline span metadata is incomplete")
		}
		unit, unitsPerSecond, supported := convertStatsSparklineSpanUnit(span.Unit)
		if !supported {
			return invalid("explicit stats sparkline span unit is invalid")
		}
		if unitsPerSecond != 0 &&
			(span.Magnitude >= unitsPerSecond || unitsPerSecond%span.Magnitude != 0) {
			return invalid("stats sparkline subsecond span must divide one second evenly")
		}
		return SparklineSpan{
			Kind:      SparklineSpanKindExplicit,
			Magnitude: span.Magnitude,
			Unit:      unit,
		}, nil
	default:
		return invalid("stats sparkline span kind is invalid")
	}
}

func convertStatsSparklineSpanUnit(
	unit spl.SparklineSpanUnit,
) (SparklineSpanUnit, uint64, bool) {
	switch unit {
	case spl.SparklineSpanUnitMicrosecond:
		return SparklineSpanUnitMicrosecond, 1_000_000, true
	case spl.SparklineSpanUnitMillisecond:
		return SparklineSpanUnitMillisecond, 1_000, true
	case spl.SparklineSpanUnitCentisecond:
		return SparklineSpanUnitCentisecond, 100, true
	case spl.SparklineSpanUnitDecisecond:
		return SparklineSpanUnitDecisecond, 10, true
	case spl.SparklineSpanUnitSecond:
		return SparklineSpanUnitSecond, 0, true
	case spl.SparklineSpanUnitMinute:
		return SparklineSpanUnitMinute, 0, true
	case spl.SparklineSpanUnitHour:
		return SparklineSpanUnitHour, 0, true
	case spl.SparklineSpanUnitDay:
		return SparklineSpanUnitDay, 0, true
	case spl.SparklineSpanUnitMonth:
		return SparklineSpanUnitMonth, 0, true
	default:
		return SparklineSpanUnitInvalid, 0, false
	}
}

func buildStatsOptions(
	options spl.StatsOptions,
	commandRange spl.Range,
) (*StatsOptions, error) {
	invalid := func(message string, sourceRange spl.Range) (*StatsOptions, error) {
		if sourceRange == (spl.Range{}) {
			sourceRange = commandRange
		}
		return nil, &Diagnostic{
			Code:    "SPL_UNSUPPORTED_STATS_SYNTAX",
			Message: message,
			Range:   sourceRange,
		}
	}

	if options.PartitionsSpecified {
		if options.PartitionsRange == (spl.Range{}) {
			return invalid("stats partitions metadata is invalid", options.PartitionsRange)
		}
	} else if options.Partitions != 0 || options.PartitionsRange != (spl.Range{}) {
		return invalid("unspecified stats partitions contains authored metadata", options.PartitionsRange)
	}
	if options.AllNumericSpecified {
		if options.AllNumericRange == (spl.Range{}) {
			return invalid("stats allnum metadata is invalid", options.AllNumericRange)
		}
	} else if options.AllNumeric || options.AllNumericRange != (spl.Range{}) {
		return invalid("unspecified stats allnum contains authored metadata", options.AllNumericRange)
	}
	if options.DelimiterSpecified {
		if options.DelimiterRange == (spl.Range{}) ||
			!utf8.ValidString(options.Delimiter) {
			return invalid("stats delim metadata is invalid", options.DelimiterRange)
		}
	} else if options.Delimiter != "" || options.DelimiterRange != (spl.Range{}) {
		return invalid("unspecified stats delim contains authored metadata", options.DelimiterRange)
	}
	if options.DeduplicateSplitValuesSpecified {
		if options.DeduplicateSplitValuesRange == (spl.Range{}) {
			return invalid(
				"stats dedup_splitvals metadata is invalid",
				options.DeduplicateSplitValuesRange,
			)
		}
	} else if options.DeduplicateSplitValues ||
		options.DeduplicateSplitValuesRange != (spl.Range{}) {
		return invalid(
			"unspecified stats dedup_splitvals contains authored metadata",
			options.DeduplicateSplitValuesRange,
		)
	}

	partitions := spl.DefaultStatsPartitions
	if options.Partitions > uint64(spl.MaximumStatsPartitions) {
		partitions = spl.MaximumStatsPartitions
	} else if options.Partitions > 0 {
		partitions = uint8(options.Partitions)
	}
	delimiter := spl.DefaultStatsDelimiter
	if options.DelimiterSpecified {
		delimiter = options.Delimiter
	}
	return &StatsOptions{
		Partitions:             partitions,
		AllNumeric:             options.AllNumeric,
		Delimiter:              delimiter,
		DeduplicateSplitValues: options.DeduplicateSplitValues,
	}, nil
}

func statsAggregateRequiresCanonicalTime(function spl.AggregateFunction) bool {
	switch function {
	case spl.AggregateFunctionEarliest,
		spl.AggregateFunctionLatest,
		spl.AggregateFunctionEarliestTime,
		spl.AggregateFunctionLatestTime,
		spl.AggregateFunctionRate:
		return true
	default:
		return false
	}
}

func convertStatsFieldAggregateFunction(
	function spl.AggregateFunction,
) (AggregateFunction, bool) {
	switch function {
	case spl.AggregateFunctionPercentile:
		return AggregateFunctionPercentile, true
	case spl.AggregateFunctionExactPercentile:
		return AggregateFunctionExactPercentile, true
	case spl.AggregateFunctionUpperPercentile:
		return AggregateFunctionUpperPercentile, true
	case spl.AggregateFunctionMedian:
		return AggregateFunctionMedian, true
	case spl.AggregateFunctionSum:
		return AggregateFunctionSum, true
	case spl.AggregateFunctionAverage:
		return AggregateFunctionAverage, true
	case spl.AggregateFunctionRange:
		return AggregateFunctionRange, true
	case spl.AggregateFunctionSumSquares:
		return AggregateFunctionSumSquares, true
	case spl.AggregateFunctionStandardDeviationSample:
		return AggregateFunctionStandardDeviationSample, true
	case spl.AggregateFunctionStandardDeviationPopulation:
		return AggregateFunctionStandardDeviationPopulation, true
	case spl.AggregateFunctionVarianceSample:
		return AggregateFunctionVarianceSample, true
	case spl.AggregateFunctionVariancePopulation:
		return AggregateFunctionVariancePopulation, true
	case spl.AggregateFunctionDistinctCount:
		return AggregateFunctionDistinctCount, true
	case spl.AggregateFunctionEstimatedDistinctCount:
		return AggregateFunctionEstimatedDistinctCount, true
	case spl.AggregateFunctionEstimatedDistinctCountError:
		return AggregateFunctionEstimatedDistinctCountError, true
	case spl.AggregateFunctionValues:
		return AggregateFunctionValues, true
	case spl.AggregateFunctionList:
		return AggregateFunctionList, true
	case spl.AggregateFunctionMinimum:
		return AggregateFunctionMinimum, true
	case spl.AggregateFunctionMaximum:
		return AggregateFunctionMaximum, true
	case spl.AggregateFunctionMode:
		return AggregateFunctionMode, true
	case spl.AggregateFunctionFirst:
		return AggregateFunctionFirst, true
	case spl.AggregateFunctionLast:
		return AggregateFunctionLast, true
	case spl.AggregateFunctionEarliest:
		return AggregateFunctionEarliest, true
	case spl.AggregateFunctionLatest:
		return AggregateFunctionLatest, true
	case spl.AggregateFunctionEarliestTime:
		return AggregateFunctionEarliestTime, true
	case spl.AggregateFunctionLatestTime:
		return AggregateFunctionLatestTime, true
	case spl.AggregateFunctionRate:
		return AggregateFunctionRate, true
	default:
		return AggregateFunctionInvalid, false
	}
}

func buildEventStatsFieldMeasure(
	aggregate spl.StatsAggregate,
	function AggregateFunction,
	form string,
) (AggregateMeasure, error) {
	percentile := uint8(0)
	if function == AggregateFunctionPercentile {
		if aggregate.Percentile < 1 || aggregate.Percentile > 99 {
			return AggregateMeasure{}, &Diagnostic{
				Code: "SPL_UNSUPPORTED_EVENTSTATS_AGGREGATE",
				Message: "eventstats percentile suffix must be an integer " +
					"from 1 through 99",
				Range: aggregate.Range,
			}
		}
		percentile = aggregate.Percentile
	} else if aggregate.Percentile != 0 {
		return AggregateMeasure{}, &Diagnostic{
			Code: "SPL_UNSUPPORTED_EVENTSTATS_AGGREGATE",
			Message: "non-percentile eventstats aggregate contains " +
				"percentile metadata",
			Range: aggregate.Range,
		}
	}
	if aggregate.Input == "" ||
		aggregate.InputRange == (spl.Range{}) ||
		aggregate.InputGlob != nil || aggregate.AliasGlob != nil ||
		aggregate.InputExpression != nil ||
		aggregate.Predicate != nil ||
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
		Function:   function,
		Input:      input,
		Output:     aggregate.Alias,
		Percentile: percentile,
	}, nil
}

type countPredicateMeasureDiagnostics struct {
	unsupportedCode    string
	invalidMessage     string
	ambiguousCode      string
	reservedMessage    string
	allowImplicitAlias bool
}

// buildCountPredicateMeasure converts the common count(eval(...)) contract
// while leaving command-specific diagnostics at the aggregate-command callers.
func buildCountPredicateMeasure(
	aggregate spl.StatsAggregate,
	outputSchemaKnown bool,
	budget *splExpressionResourceBudget,
	diagnostics countPredicateMeasureDiagnostics,
) (AggregateMeasure, error) {
	if aggregate.Input != "" ||
		aggregate.InputGlob != nil || aggregate.AliasGlob != nil ||
		aggregate.InputQuoted ||
		aggregate.InputRange != (spl.Range{}) ||
		aggregate.InputExpression != nil ||
		aggregate.Percentile != 0 ||
		(!aggregate.ExplicitAlias && !diagnostics.allowImplicitAlias) ||
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
		fieldRange, referencesReserved, inventoryErr := predicateFieldRange(
			predicate,
			"fields",
		)
		if inventoryErr != nil {
			return AggregateMeasure{}, inventoryErr
		}
		if referencesReserved {
			return AggregateMeasure{}, &Diagnostic{
				Code:    diagnostics.ambiguousCode,
				Message: diagnostics.reservedMessage,
				Range:   fieldRange,
			}
		}
	}
	return AggregateMeasure{
		Function:      AggregateFunctionCountPredicate,
		Predicate:     predicate,
		Output:        aggregate.Alias,
		OutputLiteral: aggregate.AliasQuoted || aggregate.AliasSourceDerived,
	}, nil
}

// predicateFieldRange finds the first reference to name in deterministic
// expression order. Conditional aggregate planning uses it to preserve the
// reserved open-event "fields" payload boundary just as it does for exact
// inputs.
func predicateFieldRange(
	expression Expression,
	name string,
) (spl.Range, bool, error) {
	var visitExpression func(Expression) (spl.Range, bool, error)
	var visitScalar func(ScalarExpression) (spl.Range, bool, error)
	visitScalar = func(expression ScalarExpression) (spl.Range, bool, error) {
		switch expression := expression.(type) {
		case *ScalarFieldExpression:
			if expression == nil {
				return spl.Range{}, false, errors.New("inspect predicate fields: scalar field expression is nil")
			}
			if expression.Field.Name == name {
				return expression.Field.Range, true, nil
			}
			return spl.Range{}, false, nil
		case *ScalarLiteralExpression:
			if expression == nil {
				return spl.Range{}, false, errors.New("inspect predicate fields: scalar literal expression is nil")
			}
			return spl.Range{}, false, nil
		case *ScalarUnaryExpression:
			if expression == nil {
				return spl.Range{}, false, errors.New("inspect predicate fields: scalar unary expression is nil")
			}
			return visitScalar(expression.Operand)
		case *ScalarBinaryExpression:
			if expression == nil {
				return spl.Range{}, false, errors.New("inspect predicate fields: scalar binary expression is nil")
			}
			if sourceRange, ok, err := visitScalar(expression.Left); ok || err != nil {
				return sourceRange, ok, err
			}
			return visitScalar(expression.Right)
		case *ScalarCallExpression:
			if expression == nil {
				return spl.Range{}, false, errors.New("inspect predicate fields: scalar call expression is nil")
			}
			for _, argument := range expression.Arguments {
				if sourceRange, ok, err := visitScalar(argument); ok || err != nil {
					return sourceRange, ok, err
				}
			}
			return spl.Range{}, false, nil
		case *ScalarIfExpression:
			if expression == nil {
				return spl.Range{}, false, errors.New("inspect predicate fields: scalar if expression is nil")
			}
			if sourceRange, ok, err := visitExpression(expression.Condition); ok || err != nil {
				return sourceRange, ok, err
			}
			if sourceRange, ok, err := visitScalar(expression.True); ok || err != nil {
				return sourceRange, ok, err
			}
			return visitScalar(expression.False)
		case *ScalarCaseExpression:
			if expression == nil {
				return spl.Range{}, false, errors.New("inspect predicate fields: scalar case expression is nil")
			}
			for _, branch := range expression.Branches {
				if sourceRange, ok, err := visitExpression(branch.Condition); ok || err != nil {
					return sourceRange, ok, err
				}
				if sourceRange, ok, err := visitScalar(branch.Value); ok || err != nil {
					return sourceRange, ok, err
				}
			}
			return spl.Range{}, false, nil
		default:
			return spl.Range{}, false, fmt.Errorf(
				"inspect predicate fields: unsupported scalar expression %T",
				expression,
			)
		}
	}
	visitExpression = func(expression Expression) (spl.Range, bool, error) {
		switch expression := expression.(type) {
		case *BooleanExpression:
			if expression == nil {
				return spl.Range{}, false, errors.New("inspect predicate fields: Boolean expression is nil")
			}
			if sourceRange, ok, err := visitExpression(expression.Left); ok || err != nil {
				return sourceRange, ok, err
			}
			return visitExpression(expression.Right)
		case *NotExpression:
			if expression == nil {
				return spl.Range{}, false, errors.New("inspect predicate fields: not expression is nil")
			}
			return visitExpression(expression.Operand)
		case *TextExpression:
			if expression == nil {
				return spl.Range{}, false, errors.New("inspect predicate fields: text expression is nil")
			}
			if name == "_raw" {
				return expression.Range, true, nil
			}
			return spl.Range{}, false, nil
		case *ComparisonExpression:
			if expression == nil {
				return spl.Range{}, false, errors.New("inspect predicate fields: comparison expression is nil")
			}
			if expression.Field.Name == name {
				return expression.Field.Range, true, nil
			}
			return spl.Range{}, false, nil
		case *EvalComparisonExpression:
			if expression == nil {
				return spl.Range{}, false, errors.New("inspect predicate fields: eval comparison expression is nil")
			}
			if sourceRange, ok, err := visitScalar(expression.Left); ok || err != nil {
				return sourceRange, ok, err
			}
			return visitScalar(expression.Right)
		case *ScalarPredicateExpression:
			if expression == nil {
				return spl.Range{}, false, errors.New("inspect predicate fields: scalar predicate expression is nil")
			}
			return visitScalar(expression.Value)
		case *MembershipExpression:
			if expression == nil {
				return spl.Range{}, false, errors.New("inspect predicate fields: membership expression is nil")
			}
			if sourceRange, ok, err := visitScalar(expression.Value); ok || err != nil {
				return sourceRange, ok, err
			}
			for _, candidate := range expression.Candidates {
				if sourceRange, ok, err := visitScalar(candidate); ok || err != nil {
					return sourceRange, ok, err
				}
			}
			return spl.Range{}, false, nil
		default:
			return spl.Range{}, false, fmt.Errorf(
				"inspect predicate fields: unsupported expression %T",
				expression,
			)
		}
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
		var (
			field FieldRef
			err   error
		)
		if expression.Quoted {
			field, err = ResolveQuotedField(expression.Field, expression.Range)
		} else {
			field, err = ResolveField(expression.Field, expression.Range)
		}
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
	case *spl.ScalarUnaryExpr:
		if expression == nil {
			return nil, &Diagnostic{
				Code:    "SPL_UNSUPPORTED_ARITHMETIC_SYNTAX",
				Message: "unary arithmetic expression is missing",
			}
		}
		if !validExpressionRangeOrZero(expression.Range) {
			return nil, &Diagnostic{
				Code:    "SPL_UNSUPPORTED_ARITHMETIC_SYNTAX",
				Message: "unary arithmetic expression has an invalid source range",
				Range:   expression.Range,
			}
		}
		op := convertScalarUnaryOp(expression.Op)
		if op == ScalarUnaryOpInvalid {
			return nil, &Diagnostic{
				Code:    "SPL_UNSUPPORTED_ARITHMETIC_SYNTAX",
				Message: "unary arithmetic expression has an invalid operator",
				Range:   expression.Range,
			}
		}
		if splScalarHasStaticallyUnsupportedArithmeticType(expression.Operand) {
			return nil, &Diagnostic{
				Code:    "SPL_UNSUPPORTED_ARITHMETIC_VALUE_TYPE",
				Message: "arithmetic cannot consume the statically known operand type",
				Range:   splScalarExpressionRange(expression.Operand, expression.Range),
			}
		}
		operand, err := convertScalarExpressionUnchecked(expression.Operand)
		if err != nil {
			return nil, err
		}
		return &ScalarUnaryExpression{
			Op:      op,
			Operand: operand,
			Range:   expression.Range,
		}, nil
	case *spl.ScalarBinaryExpr:
		if expression == nil {
			return nil, &Diagnostic{
				Code:    "SPL_UNSUPPORTED_ARITHMETIC_SYNTAX",
				Message: "binary arithmetic expression is missing",
			}
		}
		if !validExpressionRangeOrZero(expression.Range) {
			return nil, &Diagnostic{
				Code:    "SPL_UNSUPPORTED_ARITHMETIC_SYNTAX",
				Message: "binary arithmetic expression has an invalid source range",
				Range:   expression.Range,
			}
		}
		op := convertScalarBinaryOp(expression.Op)
		if op == ScalarBinaryOpInvalid {
			return nil, &Diagnostic{
				Code:    "SPL_UNSUPPORTED_ARITHMETIC_SYNTAX",
				Message: "binary arithmetic expression has an invalid operator",
				Range:   expression.Range,
			}
		}
		for _, operand := range []spl.ScalarExpr{expression.Left, expression.Right} {
			if splScalarHasStaticallyUnsupportedArithmeticType(operand) {
				return nil, &Diagnostic{
					Code:    "SPL_UNSUPPORTED_ARITHMETIC_VALUE_TYPE",
					Message: "arithmetic cannot consume the statically known operand type",
					Range:   splScalarExpressionRange(operand, expression.Range),
				}
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
		return &ScalarBinaryExpression{
			Op:    op,
			Left:  left,
			Right: right,
			Range: expression.Range,
		}, nil
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
		case spl.ScalarFunctionMVSort:
			expectedArguments = 1
			hasExactArity = true
			functionName = "mvsort"
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
			expression.Function == spl.ScalarFunctionMVSort ||
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
		case spl.ScalarFunctionMVSort:
			function = ScalarFunctionMVSort
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
		return nil, &Diagnostic{Code: "SPL_UNSUPPORTED_EVAL_EXPRESSION", Message: fmt.Sprintf("unsupported scalar expression type %T", expression), Range: safeSPLNodeRange(expression)}
	}
}

func splScalarMayReturnBooleanFunction(expression spl.ScalarExpr) bool {
	return spl.ScalarExpressionMayReturnBooleanFunction(expression)
}

func splScalarMayReturnBooleanValue(expression spl.ScalarExpr) bool {
	switch expression := expression.(type) {
	case *spl.ScalarFieldExpr, *spl.ScalarUnaryExpr, *spl.ScalarBinaryExpr:
		return false
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

func splScalarExpressionRange(
	expression spl.ScalarExpr,
	fallback spl.Range,
) spl.Range {
	if nilSPLScalarExpression(expression) {
		return fallback
	}
	return expression.SourceRange()
}

func splScalarHasStaticallyUnsupportedArithmeticType(
	expression spl.ScalarExpr,
) bool {
	if nilSPLScalarExpression(expression) ||
		splScalarMayReturnBooleanValue(expression) {
		return !nilSPLScalarExpression(expression)
	}
	switch expression := expression.(type) {
	case *spl.ScalarFieldExpr:
		// Field type and canonical-time lineage are properties of the current
		// pipeline state. The backend validator owns that evidence so a field
		// overwritten by an earlier eval is not misclassified from its name.
		return false
	case *spl.ScalarLiteralExpr:
		return expression != nil && expression.Value.Kind == spl.LiteralKindString
	case *spl.ScalarCallExpr:
		if expression == nil {
			return false
		}
		switch expression.Function {
		case spl.ScalarFunctionReplace,
			spl.ScalarFunctionLower,
			spl.ScalarFunctionUpper,
			spl.ScalarFunctionMVSort,
			spl.ScalarFunctionSubstring,
			spl.ScalarFunctionToString,
			spl.ScalarFunctionStrftime,
			spl.ScalarFunctionConcat:
			return true
		case spl.ScalarFunctionCoalesce:
			foundValue := false
			for _, argument := range expression.Arguments {
				if literal, ok := argument.(*spl.ScalarLiteralExpr); ok &&
					literal != nil && literal.Value.Kind == spl.LiteralKindNull {
					continue
				}
				foundValue = true
				if !splScalarHasStaticallyUnsupportedArithmeticType(argument) {
					return false
				}
			}
			return foundValue
		default:
			return false
		}
	case *spl.ScalarIfExpr:
		return expression != nil &&
			splScalarHasStaticallyUnsupportedArithmeticType(expression.True) &&
			splScalarHasStaticallyUnsupportedArithmeticType(expression.False)
	case *spl.ScalarCaseExpr:
		if expression == nil || len(expression.Branches) == 0 {
			return false
		}
		for _, branch := range expression.Branches {
			if !splScalarHasStaticallyUnsupportedArithmeticType(branch.Value) {
				return false
			}
		}
		return true
	case *spl.ScalarUnaryExpr, *spl.ScalarBinaryExpr:
		return false
	default:
		return false
	}
}

func scalarFunctionReturnsBoolean(expression ScalarExpression) bool {
	switch expression := expression.(type) {
	case *ScalarFieldExpression, *ScalarUnaryExpression, *ScalarBinaryExpression:
		return false
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
	case *ScalarFieldExpression,
		*ScalarLiteralExpression,
		*ScalarUnaryExpression,
		*ScalarBinaryExpression:
		return false
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

func convertScalarUnaryOp(op spl.ScalarUnaryOp) ScalarUnaryOp {
	switch op {
	case spl.ScalarUnaryOpPositive:
		return ScalarUnaryOpPositive
	case spl.ScalarUnaryOpNegative:
		return ScalarUnaryOpNegative
	default:
		return ScalarUnaryOpInvalid
	}
}

func convertScalarBinaryOp(op spl.ScalarBinaryOp) ScalarBinaryOp {
	switch op {
	case spl.ScalarBinaryOpMultiply:
		return ScalarBinaryOpMultiply
	case spl.ScalarBinaryOpDivide:
		return ScalarBinaryOpDivide
	case spl.ScalarBinaryOpRemainder:
		return ScalarBinaryOpRemainder
	case spl.ScalarBinaryOpAdd:
		return ScalarBinaryOpAdd
	case spl.ScalarBinaryOpSubtract:
		return ScalarBinaryOpSubtract
	default:
		return ScalarBinaryOpInvalid
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

func convertTableFields(command *spl.TableCommand) ([]FieldRef, error) {
	if command == nil {
		return nil, &Diagnostic{Code: "SPL_INVALID_QUERY", Message: "table command is nil"}
	}
	if len(command.QuotedFields) == 0 && len(command.FieldRanges) == 0 {
		return convertFields(command.Fields, command.Range)
	}
	if len(command.QuotedFields) != len(command.Fields) ||
		len(command.FieldRanges) != len(command.Fields) {
		return nil, &Diagnostic{
			Code:    "SPL_INVALID_FIELD",
			Message: "table field quote and source-range metadata is inconsistent",
			Range:   command.Range,
		}
	}
	fields := make([]FieldRef, 0, len(command.Fields))
	seen := make(map[string]struct{}, len(command.Fields))
	for index, name := range command.Fields {
		if _, duplicate := seen[name]; duplicate {
			return nil, &Diagnostic{
				Code:    "SPL_DUPLICATE_FIELD",
				Message: fmt.Sprintf("field %q is repeated", name),
				Range:   command.FieldRanges[index],
			}
		}
		seen[name] = struct{}{}
		var (
			field FieldRef
			err   error
		)
		if command.QuotedFields[index] {
			field, err = ResolveQuotedField(name, command.FieldRanges[index])
		} else {
			field, err = ResolveField(name, command.FieldRanges[index])
		}
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

// ResolveQuotedField resolves repository-standard single-quoted exact-field
// syntax. Canonical event paths retain their ordinary metadata. A safe stats
// literal name that cannot be a canonical path (for example ".com") receives
// one exact logical segment so downstream stages can bind it by visible Name
// without minting storage-path authority.
func ResolveQuotedField(name string, sourceRange spl.Range) (FieldRef, error) {
	if spl.IsExactQuotedFieldName(name) {
		return ResolveField(name, sourceRange)
	}
	if !spl.IsStatsLiteralFieldReference(name) {
		return FieldRef{}, &Diagnostic{
			Code:    "SPL_INVALID_FIELD",
			Message: "single-quoted field is not a safe exact field reference",
			Range:   sourceRange,
		}
	}
	return FieldRef{Name: name, Path: []string{name}, Range: sourceRange}, nil
}

func resolveStatsInputField(
	name string,
	sourceRange spl.Range,
	quoted bool,
) (FieldRef, error) {
	if quoted {
		return ResolveQuotedField(name, sourceRange)
	}
	return ResolveField(name, sourceRange)
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
