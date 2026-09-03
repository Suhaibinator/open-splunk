package plan

import (
	"errors"
	"fmt"
	"net/netip"
	"reflect"
	"slices"
	"strings"
	"time"
	"unicode/utf8"

	"fortio.org/safecast"

	"github.com/Suhaibinator/open-splunk/internal/eventfields"
	"github.com/Suhaibinator/open-splunk/internal/ianatimezone"
	"github.com/Suhaibinator/open-splunk/internal/nilcheck"
	"github.com/Suhaibinator/open-splunk/internal/spl"
	"github.com/Suhaibinator/open-splunk/internal/splpath"
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
	timechartSeriesLimit                = spl.MaximumTimechartSeriesLimit
	maxTimechartSeries                  = timechartSeriesLimit + 2
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
	// SearchJobID is the immutable public identifier allocated by admission.
	// It is required only when addinfo is present; non-executing validation and
	// suggestion callers leave it empty and opt into an explicit null placeholder.
	SearchJobID string
	// AllowUnboundSearchJobID admits addinfo only for non-executing validation
	// and suggestion planning. It never grants executable/public SID authority.
	AllowUnboundSearchJobID bool
	Earliest                time.Time
	Latest                  time.Time
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
	searchLocation, err := ianatimezone.Load(scope.SearchTimezone)
	if err != nil {
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
	mvExpandOrdinal := 0
	expressionBudget := splExpressionResourceBudget{}
	// publishOutputField records one command output in the exact output schema
	// when that schema is still known.
	publishOutputField := func(name string) {
		if outputSchemaKnown && !slices.Contains(result.OutputFields, name) {
			result.OutputFields = append(result.OutputFields, name)
		}
	}
	// publishOutputFieldAndTrackTime additionally retires the canonical time
	// column when the command overwrites _time.
	publishOutputFieldAndTrackTime := func(name string) {
		publishOutputField(name)
		if name == "_time" {
			canonicalTimeAvailable = false
		}
	}
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
				publishOutputFieldAndTrackTime(assignment.Field)
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

					Group: safecast.MustConv[uint16](capture.Group),
				})
				publishOutputFieldAndTrackTime(capture.Name)
			}
			result.Operators = append(result.Operators, &Extract{
				Input:    input,
				Pattern:  compiled.Pattern,
				Captures: captures,
				Range:    command.Range,
			})
		case *spl.RegexCommand:
			operator, buildErr := buildRegexCommand(command, outputSchemaKnown)
			if buildErr != nil {
				return nil, buildErr
			}
			result.Operators = append(result.Operators, operator)
		case *spl.ReverseCommand:
			if command == nil || command.Range == (spl.Range{}) {
				return nil, &Diagnostic{
					Code:    "SPL_UNSUPPORTED_REVERSE_SYNTAX",
					Message: "reverse does not accept arguments or options",
					Range:   safeSPLNodeRange(command),
				}
			}
			result.Operators = append(result.Operators, &Reverse{Range: command.Range})
		case *spl.AccumCommand:
			operator, buildErr := buildAccumCommand(command, outputSchemaKnown)
			if buildErr != nil {
				return nil, buildErr
			}
			result.Operators = append(result.Operators, operator)
			publishOutputFieldAndTrackTime(command.Output)
		case *spl.StrcatCommand:
			operator, buildErr := buildStrcatCommand(
				command,
				outputSchemaKnown,
				&expressionBudget,
			)
			if buildErr != nil {
				return nil, buildErr
			}
			result.Operators = append(result.Operators, operator)
			publishOutputFieldAndTrackTime(command.Destination)
		case *spl.AddInfoCommand:
			operator, buildErr := buildAddInfoCommand(command, scope)
			if buildErr != nil {
				return nil, buildErr
			}
			result.Operators = append(result.Operators, operator)
			for _, field := range []string{
				"info_min_time",
				"info_max_time",
				"info_search_time",
				"info_sid",
			} {
				publishOutputField(field)
			}
		case *spl.FillNullCommand:
			if !outputSchemaKnown {
				if command != nil && command.AllFields {
					return nil, &Diagnostic{
						Code:    "SPL_AMBIGUOUS_FILLNULL_FIELD",
						Message: "fillnull without a field list needs an exact upstream schema (for example after stats or table); list the fields to fill on raw events",
						Range:   command.Range,
					}
				}
				for _, field := range command.Fields {
					if field.Name == "fields" {
						return nil, &Diagnostic{
							Code:    "SPL_AMBIGUOUS_FILLNULL_FIELD",
							Message: "fillnull cannot replace the event result's reserved fields payload without an exact upstream schema",
							Range:   field.Range,
						}
					}
				}
			}
			var (
				operator *FillNull
				buildErr error
			)
			if command != nil && command.AllFields {
				operator, buildErr = buildFillNullOverSchema(command, result.OutputFields)
			} else {
				operator, buildErr = buildFillNull(command)
			}
			if buildErr != nil {
				return nil, buildErr
			}
			result.Operators = append(result.Operators, operator)
			for _, field := range operator.Fields {
				publishOutputFieldAndTrackTime(field.Name)
			}
		case *spl.AddTotalsCommand:
			if !outputSchemaKnown {
				if command.Output == "fields" {
					return nil, &Diagnostic{
						Code:    "SPL_AMBIGUOUS_ADDTOTALS_FIELD",
						Message: "addtotals cannot replace the event result's reserved fields payload without an exact upstream schema",
						Range:   command.OutputRange,
					}
				}
				for _, field := range command.Fields {
					if field.Name == "fields" {
						return nil, &Diagnostic{
							Code:    "SPL_AMBIGUOUS_ADDTOTALS_FIELD",
							Message: "addtotals cannot read the event result's reserved fields payload without an exact upstream schema",
							Range:   field.Range,
						}
					}
				}
			}
			operator, buildErr := buildRowTotal(command)
			if buildErr != nil {
				return nil, buildErr
			}
			result.Operators = append(result.Operators, operator)
			publishOutputFieldAndTrackTime(command.Output)
		case *spl.DeltaCommand:
			if !outputSchemaKnown && (command.Field == "fields" || command.Output == "fields") {
				fieldRange := command.FieldRange
				if command.Output == "fields" {
					fieldRange = command.OutputRange
				}
				return nil, &Diagnostic{
					Code:    "SPL_AMBIGUOUS_DELTA_FIELD",
					Message: "delta cannot use the event result's reserved fields payload without an exact upstream schema",
					Range:   fieldRange,
				}
			}
			operator, buildErr := buildOrderedDelta(command)
			if buildErr != nil {
				return nil, buildErr
			}
			result.Operators = append(result.Operators, operator)
			publishOutputFieldAndTrackTime(command.Output)
		case *spl.MakeMVCommand:
			if !outputSchemaKnown && command.Field == "fields" {
				return nil, &Diagnostic{
					Code:    "SPL_AMBIGUOUS_MAKEMV_FIELD",
					Message: "makemv cannot replace the event result's reserved fields payload without an exact upstream schema",
					Range:   command.FieldRange,
				}
			}
			operator, buildErr := buildMakeMultivalue(command)
			if buildErr != nil {
				return nil, buildErr
			}
			result.Operators = append(result.Operators, operator)
			publishOutputField(command.Field)
		case *spl.MVExpandCommand:
			if !outputSchemaKnown && command.Field == "fields" {
				return nil, &Diagnostic{
					Code:    "SPL_AMBIGUOUS_MVEXPAND_FIELD",
					Message: "mvexpand cannot expand the event result's reserved fields payload without an exact upstream schema",
					Range:   command.FieldRange,
				}
			}
			mvExpandOrdinal++
			operator, buildErr := buildExpandMultivalue(command, mvExpandOrdinal)
			if buildErr != nil {
				return nil, buildErr
			}
			result.Operators = append(result.Operators, operator)
			publishOutputField(command.Field)
		case *spl.NoMVCommand:
			if !outputSchemaKnown && command.Field == "fields" {
				return nil, &Diagnostic{
					Code:    "SPL_AMBIGUOUS_NOMV_FIELD",
					Message: "nomv cannot present the event result's reserved fields payload without an exact upstream schema",
					Range:   command.FieldRange,
				}
			}
			operator, buildErr := buildNoMultivalue(command)
			if buildErr != nil {
				return nil, buildErr
			}
			result.Operators = append(result.Operators, operator)
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
			publishOutputFieldAndTrackTime(command.Output)
		case *spl.LookupCommand:
			operator, buildErr := buildLookupCommand(command, outputSchemaKnown)
			if buildErr != nil {
				return nil, buildErr
			}
			result.Operators = append(result.Operators, operator)
			for _, output := range operator.Outputs {
				publishOutputField(output.EventField.Name)
				if operator.WriteMode == LookupWriteModeOverwrite &&
					output.EventField.Name == "_time" {
					canonicalTimeAvailable = false
				}
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
					span, calendar, spanErr := timeBucketSpan(command.Span)
					if spanErr != nil {
						return nil, spanErr
					}
					result.Operators = append(result.Operators, &TimeBucket{
						Field:    input,
						Output:   output,
						Span:     span,
						Calendar: calendar,
						Range:    command.Range,
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

			publishOutputFieldAndTrackTime(command.Output)
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
			fields, patterns, fieldErr := convertFieldsCommand(command)
			if fieldErr != nil {
				return nil, fieldErr
			}
			mode := ProjectModeInclude
			if command.Exclude {
				mode = ProjectModeExclude
			}
			result.Operators = append(result.Operators, &Project{
				Mode: mode, Fields: fields, Patterns: patterns, Range: command.Range,
			})
			if outputSchemaKnown {
				result.OutputFields = projectKnownOutputFields(
					result.OutputFields,
					command.Fields,
					command.WildcardFields,
					command.Exclude,
				)
				if len(result.OutputFields) == 0 {
					return nil, &Diagnostic{
						Code:        "SPL_EMPTY_PROJECTION",
						Message:     "fields removes every column from the transforming result",
						Range:       command.Range,
						Suggestions: []string{"retain at least one stats or table output field"},
					}
				}
			}
			if command.Exclude && fieldsCommandSelectsName(command, "_time") {
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
			keys, keysErr := convertSortFields(command.Fields)
			if keysErr != nil {
				return nil, keysErr
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
			if len(command.SortBy) > 0 {
				// sortby establishes the order dedup selects from, and that order
				// survives as the relation's order afterwards. It is unbounded:
				// a Splunk-style default sort limit would truncate the input
				// before the key tuples are examined.
				sortKeys, sortErr := convertSortFields(command.SortBy)
				if sortErr != nil {
					return nil, sortErr
				}
				result.Operators = append(result.Operators, &Sort{Keys: sortKeys, Range: command.SortByRange})
			}
			result.Operators = append(result.Operators, &Deduplicate{
				Count:       command.Count,
				Keys:        keys,
				Consecutive: command.Consecutive,
				Range:       command.Range,
			})
		case *spl.LimitCommand:
			result.Operators = append(result.Operators, &Limit{Count: command.Count, FromEnd: command.Name() == "tail", Range: command.Range})
		case *spl.EventStatsCommand:
			if buildErr := buildEventStatsCommand(
				result,
				command,
				outputSchemaKnown,
				canonicalTimeAvailable,
				&expressionBudget,
				publishOutputFieldAndTrackTime,
			); buildErr != nil {
				return nil, buildErr
			}
		case *spl.StreamStatsCommand:
			if buildErr := buildStreamStatsCommand(
				result,
				command,
				outputSchemaKnown,
				canonicalTimeAvailable,
				&expressionBudget,
				publishOutputFieldAndTrackTime,
			); buildErr != nil {
				return nil, buildErr
			}
		case *spl.StatsCommand:
			if buildErr := buildStatsCommand(
				result,
				command,
				outputSchemaKnown,
				canonicalTimeAvailable,
				&expressionBudget,
			); buildErr != nil {
				return nil, buildErr
			}
			outputSchemaKnown = true
			canonicalTimeAvailable = false
		case *spl.TopCommand, *spl.RareCommand:
			frequency, frequencyErr := buildFrequencyOperators(command)
			if frequencyErr != nil {
				return nil, frequencyErr
			}
			canonicalTimeAvailable = false
			result.OutputFields = frequency.outputFields
			outputSchemaKnown = true
			result.Operators = append(result.Operators, frequency.operators...)
		case *spl.TimechartCommand:
			if buildErr := buildTimechartCommand(
				result,
				query,
				commandIndex,
				command,
				outputSchemaKnown,
				canonicalTimeAvailable,
				earliest,
				latest,
				searchLocation,
			); buildErr != nil {
				return nil, buildErr
			}
		case *spl.ChartCommand:
			if buildErr := buildChartCommand(
				result,
				query,
				commandIndex,
				command,
				outputSchemaKnown,
			); buildErr != nil {
				return nil, buildErr
			}
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

func safeSPLNodeRange(node spl.Node) spl.Range {
	if nilcheck.IsNil(node) {
		return spl.Range{}
	}
	return node.SourceRange()
}

func splQuotedStringLiteral(
	expression spl.ScalarExpr,
	fallbackRange spl.Range,
) (*spl.ScalarLiteralExpr, spl.Range, bool) {
	sourceRange := fallbackRange
	if !nilcheck.IsNil(expression) {
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

// validateSPLToStringFormat requires the tostring format to be one of the
// quoted literals with an exact lowering; the parser enforces the same set so
// this only guards constructed plans.
func validateSPLToStringFormat(expression spl.ScalarExpr, fallbackRange spl.Range) error {
	literal, sourceRange, ok := splQuotedStringLiteral(expression, fallbackRange)
	if !ok || !slices.Contains(spl.SupportedToStringFormats, literal.Value.Text) {
		return &Diagnostic{
			Code:    "SPL_UNSUPPORTED_TOSTRING_FORMAT",
			Message: `tostring supports only the quoted "commas" and "duration" formats`,
			Range:   sourceRange,
		}
	}
	return nil
}

// validateSPLTrimCharacters requires the explicit trim character set to be a
// bounded non-empty valid UTF-8 quoted literal.
func validateSPLTrimCharacters(
	function string,
	expression spl.ScalarExpr,
	fallbackRange spl.Range,
) error {
	literal, sourceRange, ok := splQuotedStringLiteral(expression, fallbackRange)
	if !ok {
		return &Diagnostic{
			Code:    "SPL_UNSUPPORTED_TRIM_CHARACTERS",
			Message: function + " characters must be a quoted string literal",
			Range:   sourceRange,
		}
	}
	if literal.Value.Text == "" || !utf8.ValidString(literal.Value.Text) {
		return &Diagnostic{
			Code:    "SPL_UNSUPPORTED_TRIM_CHARACTERS",
			Message: function + " characters must be a non-empty valid UTF-8 string",
			Range:   literal.Range,
		}
	}
	if len(literal.Value.Text) > spl.MaximumTrimCharactersBytes {
		return &Diagnostic{
			Code: "SPL_QUERY_TOO_COMPLEX",
			Message: fmt.Sprintf(
				"%s characters exceed the %d-byte limit",
				function,
				spl.MaximumTrimCharactersBytes,
			),
			Range: literal.Range,
		}
	}
	return nil
}

// validateSPLCIDRPrefix requires the cidrmatch prefix literal to parse as an
// IPv4 or IPv6 CIDR block; the caller has already proven it is a quoted literal.
func validateSPLCIDRPrefix(expression spl.ScalarExpr) error {
	literal, ok := expression.(*spl.ScalarLiteralExpr)
	if !ok || literal == nil {
		return &Diagnostic{
			Code:    "SPL_UNSUPPORTED_CIDR_PREFIX",
			Message: "cidrmatch prefix must be a quoted string literal",
		}
	}
	if _, err := netip.ParsePrefix(literal.Value.Text); err != nil {
		return &Diagnostic{
			Code:    "SPL_UNSUPPORTED_CIDR_PREFIX",
			Message: "cidrmatch prefix must be an IPv4 or IPv6 CIDR block such as 10.0.0.0/8",
			Range:   literal.Range,
		}
	}
	return nil
}

func validateSPLMVDelimiter(
	function string,
	expression spl.ScalarExpr,
	fallbackRange spl.Range,
) error {
	literal, sourceRange, ok := splQuotedStringLiteral(expression, fallbackRange)
	if !ok {
		return &Diagnostic{
			Code:    "SPL_UNSUPPORTED_EVAL_EXPRESSION",
			Message: function + " delimiter must be a quoted string literal",
			Range:   sourceRange,
		}
	}
	if !utf8.ValidString(literal.Value.Text) {
		return &Diagnostic{
			Code:    "SPL_UNSUPPORTED_EVAL_EXPRESSION",
			Message: function + " delimiter must be valid UTF-8",
			Range:   literal.Range,
		}
	}
	if len(literal.Value.Text) > spl.MaximumMVDelimiterBytes {
		return &Diagnostic{
			Code: "SPL_QUERY_TOO_COMPLEX",
			Message: fmt.Sprintf(
				"%s delimiter exceeds the %d-byte limit",
				function,
				spl.MaximumMVDelimiterBytes,
			),
			Range: literal.Range,
		}
	}
	return nil
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
	if nilcheck.IsNil(expression) {
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
	if nilcheck.IsNil(expression) {
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
		if expression.Function == spl.ScalarFunctionMVAppend &&
			len(expression.Arguments) > spl.MaximumMVAppendArguments {
			return splExpressionComplexityError(
				fmt.Sprintf(
					"mvappend contains more than %d arguments",
					spl.MaximumMVAppendArguments,
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
