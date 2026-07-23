package plan

import (
	"fmt"
	"math"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/Suhaibinator/open-splunk/internal/eventfields"
	"github.com/Suhaibinator/open-splunk/internal/spl"
)

const (
	// Ingestion permits a leaf below 16 nested objects: 17 path segments of 256
	// bytes each. Dots and backslashes require one escape byte in SPL, so the
	// full query spelling can be at most 17*(2*256)+16 separators.
	maxFieldNameBytes        = 8720
	maxFieldPathSegments     = 17
	maxFieldPathSegmentBytes = 256
	maxTimechartBuckets      = 10_000
	maxTimechartSpan         = 24 * time.Hour
	timechartSeriesLimit     = 10
	maxTimechartSeries       = 12
	maxDedupFields           = 16
)

// Scope is the server-resolved security and snapshot boundary for a search.
// AuthorizedIndexes must come from trusted control-plane state, never SPL.
type Scope struct {
	TenantID          string
	AuthorizedIndexes []string
	RequestedIndexes  []string
	Earliest          time.Time
	Latest            time.Time
	IndexTimeCutoff   time.Time
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
	indexes, err := resolveIndexes(scope, query)
	if err != nil {
		return nil, err
	}
	if scope.TenantID == "" {
		return nil, &Diagnostic{Code: "SPL_INVALID_SCOPE", Message: "tenant scope is empty", Range: query.Range}
	}
	earliest := scope.Earliest.UTC()
	latest := scope.Latest.UTC()
	cutoff := scope.IndexTimeCutoff.UTC()
	if earliest.IsZero() || latest.IsZero() || !earliest.Before(latest) {
		return nil, &Diagnostic{Code: "SPL_INVALID_TIME_RANGE", Message: "time range must be a non-empty half-open interval", Range: query.Range}
	}
	if cutoff.IsZero() {
		return nil, &Diagnostic{Code: "SPL_INVALID_TIME_RANGE", Message: "index-time cutoff is required", Range: query.Range}
	}
	if scope.VisibilityCutoff == nil {
		return nil, &Diagnostic{Code: "SPL_INVALID_SNAPSHOT", Message: "storage visibility cutoff is required", Range: query.Range}
	}

	result := &Query{EffectiveIndexes: indexes}
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
	for commandIndex, command := range query.Commands {
		switch command := command.(type) {
		case *spl.SearchCommand:
			expression, convertErr := convertExpression(command.Expression)
			if convertErr != nil {
				return nil, convertErr
			}
			result.Operators = append(result.Operators, &Filter{Expression: expression, Range: command.Range})
		case *spl.WhereCommand:
			expression, convertErr := convertWhereExpression(command.Expression)
			if convertErr != nil {
				return nil, convertErr
			}
			result.Operators = append(result.Operators, &Filter{Expression: expression, Range: command.Range})
		case *spl.EvalCommand:
			assignments := make([]ExtendAssignment, 0, len(command.Assignments))
			for _, assignment := range command.Assignments {
				output, fieldErr := ResolveField(assignment.Field, assignment.FieldRange)
				if fieldErr != nil {
					return nil, fieldErr
				}
				expression, expressionErr := convertScalarExpression(assignment.Expression)
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
		case *spl.StatsCommand:
			canonicalTimeAvailable = false
			groupBy, groupErr := convertStatsGroupFields(command.GroupBy)
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
					measure.Function = AggregateFunctionCountRows
				case spl.AggregateFunctionP95:
					input, inputErr := ResolveField(aggregate.Input, aggregate.InputRange)
					if inputErr != nil {
						return nil, inputErr
					}
					measure.Function = AggregateFunctionPercentile
					measure.Input = input
					measure.Percentile = 0.95
				case spl.AggregateFunctionSum, spl.AggregateFunctionAverage:
					input, inputErr := ResolveField(aggregate.Input, aggregate.InputRange)
					if inputErr != nil {
						return nil, inputErr
					}
					measure.Input = input
					if aggregate.Function == spl.AggregateFunctionSum {
						measure.Function = AggregateFunctionSum
					} else {
						measure.Function = AggregateFunctionAverage
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
			if commandIndex+1 != len(query.Commands) {
				next := query.Commands[commandIndex+1]
				return nil, &Diagnostic{
					Code:        "SPL_UNSUPPORTED_TIMECHART_PIPELINE",
					Message:     "timechart must be the final pipeline command in this compatibility version",
					Range:       next.SourceRange(),
					Suggestions: []string{"move timechart to the final pipeline stage"},
				}
			}
			if command.Function != spl.AggregateFunctionCount {
				return nil, &Diagnostic{
					Code:    "SPL_UNSUPPORTED_TIMECHART_AGGREGATE",
					Message: "unsupported timechart aggregate",
					Range:   command.AggregateRange,
				}
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
			splitBy, splitErr := ResolveField(command.SplitBy.Name, command.SplitBy.Range)
			if splitErr != nil {
				return nil, splitErr
			}
			result.OutputFields = nil
			result.DynamicOutput = &DynamicSeriesOutput{
				FixedFields: []string{"_time"},
				MaxSeries:   maxTimechartSeries,
			}
			result.Operators = append(result.Operators, &Timechart{
				Time:           timeField,
				SplitBy:        splitBy,
				Function:       AggregateFunctionCountRows,
				Span:           span,
				FirstBucket:    firstBucket,
				BucketCount:    bucketCount,
				SeriesLimit:    timechartSeriesLimit,
				IncludeNull:    true,
				IncludeOther:   true,
				NullLabel:      "NULL",
				OtherLabel:     "OTHER",
				FixedRange:     true,
				Continuous:     true,
				IncludePartial: true,
				Range:          command.Range,
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

func fixedTimechartSpan(span spl.TimeSpan) (time.Duration, error) {
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
			Code:    "SPL_UNSUPPORTED_TIMECHART_SYNTAX",
			Message: "unsupported timechart span unit",
			Range:   span.Range,
		}
	}
	if span.Magnitude == 0 || span.Magnitude > uint64(math.MaxInt64/int64(unit)) {
		return 0, &Diagnostic{
			Code:    "SPL_NUMBER_OUT_OF_RANGE",
			Message: "timechart span is outside the supported duration range",
			Range:   span.Range,
		}
	}
	duration := time.Duration(span.Magnitude) * unit
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

func convertStatsGroupFields(fields []spl.StatsGroupField) ([]FieldRef, error) {
	result := make([]FieldRef, 0, len(fields))
	seen := make(map[string]struct{}, len(fields))
	for _, field := range fields {
		if _, duplicate := seen[field.Name]; duplicate {
			return nil, &Diagnostic{
				Code:    "SPL_DUPLICATE_FIELD",
				Message: fmt.Sprintf("stats grouping field %q is repeated", field.Name),
				Range:   field.Range,
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

func resolveIndexes(scope Scope, query *spl.Query) ([]string, error) {
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

	for _, reference := range positiveIndexReferences(query) {
		if strings.Contains(reference.value, "*") {
			return nil, &Diagnostic{
				Code:    "SPL_UNSUPPORTED_INDEX_SELECTOR",
				Message: "wildcard index selectors are not supported in compatibility version 0.1",
				Range:   reference.range_,
			}
		}
		if _, ok := authorized[reference.value]; !ok {
			return nil, &Diagnostic{Code: "SPL_INDEX_FORBIDDEN", Message: fmt.Sprintf("index %q is outside the authorized scope", reference.value), Range: reference.range_}
		}
		if _, ok := effective[reference.value]; !ok {
			return nil, &Diagnostic{Code: "SPL_INDEX_FORBIDDEN", Message: fmt.Sprintf("index %q is outside the requested scope", reference.value), Range: reference.range_}
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
	value  string
	range_ spl.Range
}

func positiveIndexReferences(query *spl.Query) []indexReference {
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
		case *spl.RenameCommand:
			for _, assignment := range command.Assignments {
				if assignment.Source == "index" || assignment.Destination == "index" {
					return references
				}
			}
		case *spl.StatsCommand, *spl.TopCommand, *spl.RareCommand, *spl.TimechartCommand:
			return references
		}
		if search, ok := command.(*spl.SearchCommand); ok {
			collect(search.Expression)
		}
	}
	return references
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
			*destination = append(*destination, indexReference{value: expression.Value.Text, range_: expression.Range})
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

func convertWhereExpression(expression spl.WhereExpr) (Expression, error) {
	switch expression := expression.(type) {
	case *spl.WhereBoolExpr:
		left, err := convertWhereExpression(expression.Left)
		if err != nil {
			return nil, err
		}
		right, err := convertWhereExpression(expression.Right)
		if err != nil {
			return nil, err
		}
		op := BooleanOpAnd
		if expression.Op == spl.BoolOpOr {
			op = BooleanOpOr
		}
		return &BooleanExpression{Op: op, Left: left, Right: right, Range: expression.Range}, nil
	case *spl.WhereNotExpr:
		operand, err := convertWhereExpression(expression.Operand)
		if err != nil {
			return nil, err
		}
		return &NotExpression{Operand: operand, Range: expression.Range}, nil
	case *spl.WhereComparisonExpr:
		left, err := convertScalarExpression(expression.Left)
		if err != nil {
			return nil, err
		}
		right, err := convertScalarExpression(expression.Right)
		if err != nil {
			return nil, err
		}
		return &EvalComparisonExpression{
			Left:  left,
			Op:    convertComparisonOp(expression.Op),
			Right: right,
			Range: expression.Range,
		}, nil
	default:
		return nil, &Diagnostic{Code: "SPL_UNSUPPORTED_WHERE_EXPRESSION", Message: fmt.Sprintf("unsupported where expression type %T", expression), Range: expression.SourceRange()}
	}
}

func convertScalarExpression(expression spl.ScalarExpr) (ScalarExpression, error) {
	switch expression := expression.(type) {
	case *spl.ScalarFieldExpr:
		field, err := ResolveField(expression.Field, expression.Range)
		if err != nil {
			return nil, err
		}
		return &ScalarFieldExpression{Field: field, Range: expression.Range}, nil
	case *spl.ScalarLiteralExpr:
		value, err := convertValue(expression.Value)
		if err != nil {
			return nil, err
		}
		return &ScalarLiteralExpression{Value: value, Range: expression.Range}, nil
	case *spl.ScalarCallExpr:
		arguments := make([]ScalarExpression, 0, len(expression.Arguments))
		for _, argument := range expression.Arguments {
			converted, err := convertScalarExpression(argument)
			if err != nil {
				return nil, err
			}
			arguments = append(arguments, converted)
		}
		function := ScalarFunctionInvalid
		switch expression.Function {
		case spl.ScalarFunctionToNumber:
			function = ScalarFunctionToNumber
		case spl.ScalarFunctionReplace:
			function = ScalarFunctionReplace
		default:
			return nil, &Diagnostic{Code: "SPL_UNSUPPORTED_EVAL_FUNCTION", Message: "unsupported scalar function", Range: expression.Range}
		}
		return &ScalarCallExpression{Function: function, Arguments: arguments, Range: expression.Range}, nil
	default:
		return nil, &Diagnostic{Code: "SPL_UNSUPPORTED_EVAL_EXPRESSION", Message: fmt.Sprintf("unsupported scalar expression type %T", expression), Range: expression.SourceRange()}
	}
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
			if r < 0x20 || r == 0x7f {
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
