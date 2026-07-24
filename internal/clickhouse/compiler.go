package clickhouse

import (
	"errors"
	"fmt"
	"reflect"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/eventfields"
	"github.com/Suhaibinator/open-splunk/internal/plan"
	"github.com/Suhaibinator/open-splunk/internal/splregex"
)

const (
	internalFieldsColumn               = "__os_fields"
	internalFieldNamesColumn           = "__os_field_names"
	internalFieldTypesColumn           = "__os_field_types"
	internalFieldMetadataVersionColumn = "__os_field_metadata_version"
	internalSortTimeColumn             = "__os_sort_time"
	internalSortIDColumn               = "__os_sort_event_id"
	// Timechart physical columns are an executor-only transport. Runtime series
	// names are data, never SQL identifiers, and are expanded into the public
	// wide schema only after the complete bounded result has been validated.
	// The zero-based ordinal keeps epoch-aligned bucket starts out of
	// DateTime64 conversions: the first bucket may precede ClickHouse's
	// practical lower bound even though every selected event is representable.
	TimechartOrdinalColumn = "__os_timechart_ordinal"
	TimechartNamesColumn   = "__os_timechart_names"
	TimechartCountsColumn  = "__os_timechart_counts"
	TimechartInvalidColumn = "__os_timechart_invalid"
	// MaximumRexCapturedBytesPerRow bounds the sum of all capture-group bytes
	// produced by every rex stage for one event. It prevents overlapping named
	// groups and repeated stages from amplifying a maximum-sized event without
	// a query-local ceiling.
	MaximumRexCapturedBytesPerRow = 4 << 20
	maxCompiledQueryBytes         = 256 << 10
	maxTimechartLabelBytes        = 256

	// UnsupportedStatsByValueMarker is emitted by the scalar-only stats BY
	// guard so the executor can classify the ClickHouse exception without
	// exposing generated SQL or storage details.
	UnsupportedStatsByValueMarker = "open-splunk: stats BY requires a scalar field"
	// UnsupportedDedupValueMarker is emitted when a complete dedup key contains
	// a runtime list or object. It is intentionally stable for executor-side
	// classification and is never returned verbatim to clients.
	UnsupportedDedupValueMarker = "open-splunk: dedup requires scalar fields"
	// RexCaptureLimitMarker lets the executor map the deliberate throwIf guard
	// to a stable resource-limit result without exposing generated SQL.
	RexCaptureLimitMarker = "open-splunk: rex capture bytes exceed the per-row limit"
	// UnsupportedNumericBinValueMarker is emitted when a mathematically correct
	// numeric bucket cannot be represented by the input field's fixed type, or
	// when a floating-point input or result is not finite.
	UnsupportedNumericBinValueMarker = "open-splunk: numeric bin value is outside the supported range"
)

var physicalIdentifier = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

// Compiler lowers backend-neutral logical plans to parameterized ClickHouse
// SQL. Database and table are trusted configuration and still pass a strict
// identifier allowlist; all user-authored values are query parameters.
type Compiler struct {
	Database string
	Table    string
}

// CompiledQuery is executable SQL plus ordered bind arguments and public
// result fields. Internal helper columns never appear in OutputFields.
type CompiledQuery struct {
	SQL          string
	Args         []any
	OutputFields []string
	Timechart    *TimechartOutput
}

// TimechartOutput describes the bounded runtime-wide result contract. The SQL
// result itself has fixed private columns; OutputFields contains only the
// fixed public prefix, currently _time.
type TimechartOutput struct {
	FirstBucket   time.Time
	Span          time.Duration
	BucketCount   uint64
	MaxSeries     uint16
	MaxLabelBytes uint16
}

// Compile compiles one plan without mutating it.
func (c Compiler) Compile(query *plan.Query) (CompiledQuery, error) {
	return c.compileWithFinalizer(query, finalizeOrdinaryQuery, true)
}

type queryFinalizer func(
	fragment string,
	state compileState,
	args []any,
	scan *plan.Scan,
	aliasSequence int,
) (CompiledQuery, error)

// compileEventAnalysis proves that the final relation still consists of
// individual events before exposing it to an analysis-specific projection.
func (c Compiler) compileEventAnalysis(query *plan.Query, finalize queryFinalizer) (CompiledQuery, error) {
	if err := plan.ValidateFieldAnalysisEligibility(query); err != nil {
		return CompiledQuery{}, err
	}
	return c.compileWithFinalizer(query, finalize, false)
}

// compileWithFinalizer lowers every logical operator once, then delegates the
// final projection. permitTerminalTimechart is reserved for ordinary search
// compilation; event analyses must consume only the proven event relation.
func (c Compiler) compileWithFinalizer(query *plan.Query, finalize queryFinalizer, permitTerminalTimechart bool) (CompiledQuery, error) {
	if query == nil || len(query.Operators) == 0 {
		return CompiledQuery{}, errors.New("compile ClickHouse query: logical plan is empty")
	}
	if finalize == nil {
		return CompiledQuery{}, errors.New("compile ClickHouse query: finalizer is required")
	}
	database := c.Database
	if database == "" {
		database = "open_splunk"
	}
	table := c.Table
	if table == "" {
		table = "events"
	}
	if !physicalIdentifier.MatchString(database) || !physicalIdentifier.MatchString(table) {
		return CompiledQuery{}, errors.New("compile ClickHouse query: database and table must be simple identifiers")
	}
	if isNilPlanOperator(query.Operators[0]) {
		return CompiledQuery{}, errors.New("compile ClickHouse query: first operator must be a non-nil Scan")
	}
	scan, ok := query.Operators[0].(*plan.Scan)
	if !ok {
		return CompiledQuery{}, errors.New("compile ClickHouse query: first operator must be Scan")
	}
	fragment, state, args, err := compileScan(database, table, scan)
	if err != nil {
		return CompiledQuery{}, err
	}

	aliasSequence := 0
	remainingOperators := query.Operators[1:]
	for operatorIndex, operator := range remainingOperators {
		if isNilPlanOperator(operator) {
			return CompiledQuery{}, fmt.Errorf("compile ClickHouse query: operator %d is nil", operatorIndex+1)
		}
		aliasSequence++
		alias := quoteIdentifier(fmt.Sprintf("_stage_%d", aliasSequence))
		switch operator := operator.(type) {
		case *plan.Filter:
			predicate, predicateArgs, compileErr := compileExpression(operator.Expression, state)
			if compileErr != nil {
				return CompiledQuery{}, compileErr
			}
			fragment = "SELECT * FROM (" + fragment + ") AS " + alias + " WHERE " + predicate
			args = append(args, predicateArgs...)
		case *plan.Project:
			projection, nextState, projectionArgs, compileErr := compileProjection(operator, state)
			if compileErr != nil {
				return CompiledQuery{}, compileErr
			}
			fragment = "SELECT " + strings.Join(projection, ", ") + " FROM (" + fragment + ") AS " + alias
			args = append(args, projectionArgs...)
			state = nextState
		case *plan.Extend:
			if len(operator.Assignments) == 0 {
				return CompiledQuery{}, errors.New("compile ClickHouse extend: no assignments")
			}
			for index, assignment := range operator.Assignments {
				value, compileErr := compileScalarValue(assignment.Expression, state)
				if compileErr != nil {
					return CompiledQuery{}, compileErr
				}
				fragment = upsertFieldProjectionSQL(
					fragment,
					state,
					assignment.Output.Name,
					value.valueSQL,
					alias,
				)
				// Extend is emitted in an outer SELECT, so its placeholders occur
				// before every placeholder already present in the nested fragment.
				// Sequential assignments add another outer SELECT and therefore
				// prepend in reverse nesting order as well.
				args = prependArguments(value.valueArgs, args)
				state = extendCompileState(state, assignment.Output, value)
				if index+1 < len(operator.Assignments) {
					aliasSequence++
					alias = quoteIdentifier(fmt.Sprintf("_stage_%d", aliasSequence))
				}
			}
		case *plan.TimeBucket:
			bucketed, nextState, prefixArgs, compileErr := compileTimeBucket(
				fragment,
				state,
				scan,
				operator,
				alias,
			)
			if compileErr != nil {
				return CompiledQuery{}, compileErr
			}
			fragment = bucketed
			args = prependArguments(prefixArgs, args)
			state = nextState
		case *plan.NumericBucket:
			bucketed, nextState, prefixArgs, compileErr := compileNumericBucket(
				fragment,
				state,
				operator,
				alias,
				aliasSequence,
			)
			if compileErr != nil {
				return CompiledQuery{}, compileErr
			}
			fragment = bucketed
			args = prependArguments(prefixArgs, args)
			state = nextState
		case *plan.Extract:
			extracted, nextState, prefixArgs, additionalAliases, compileErr := compileExtract(
				fragment,
				operator,
				state,
				aliasSequence,
			)
			if compileErr != nil {
				return CompiledQuery{}, compileErr
			}
			fragment = extracted
			args = prependArguments(prefixArgs, args)
			state = nextState
			aliasSequence += additionalAliases
		case *plan.Rename:
			if len(operator.Assignments) == 0 {
				return CompiledQuery{}, errors.New("compile ClickHouse rename: no assignments")
			}
			seenSources := make(map[string]struct{}, len(operator.Assignments))
			seenDestinations := make(map[string]struct{}, len(operator.Assignments))
			for index, assignment := range operator.Assignments {
				if assignment.Source.Name == assignment.Destination.Name {
					return CompiledQuery{}, errors.New("compile ClickHouse rename: source and destination must differ")
				}
				if _, duplicate := seenSources[assignment.Source.Name]; duplicate {
					return CompiledQuery{}, errors.New("compile ClickHouse rename: source field is repeated")
				}
				if _, duplicate := seenDestinations[assignment.Destination.Name]; duplicate {
					return CompiledQuery{}, errors.New("compile ClickHouse rename: destination field is repeated")
				}
				seenSources[assignment.Source.Name] = struct{}{}
				seenDestinations[assignment.Destination.Name] = struct{}{}
				if index > 0 {
					aliasSequence++
					alias = quoteIdentifier(fmt.Sprintf("_stage_%d", aliasSequence))
				}
				projection, nextState, changed, compileErr := compileRenameAssignment(assignment, state)
				if compileErr != nil {
					return CompiledQuery{}, compileErr
				}
				state = nextState
				if changed {
					fragment = "SELECT " + strings.Join(projection, ", ") + " FROM (" + fragment + ") AS " + alias
				}
			}
		case *plan.Aggregate:
			projection, predicates, groups, nextState, aggregateArgs, compileErr := compileAggregate(operator, state)
			if compileErr != nil {
				return CompiledQuery{}, compileErr
			}
			if len(nextState.preAggregateValidationColumns) > 0 {
				// Materialize whole-input validation windows before filtering incomplete
				// group tuples. Otherwise a missing sibling key could hide an
				// unsupported container value.
				fragment = "SELECT *, " + strings.Join(nextState.preAggregateValidationColumns, ", ") + " FROM (" + fragment + ") AS " + alias
				args = prependArguments(nextState.preAggregateValidationArgs, args)
				aliasSequence++
				alias = quoteIdentifier(fmt.Sprintf("_stage_%d", aliasSequence))
				nextState.preAggregateValidationColumns = nil
				nextState.preAggregateValidationArgs = nil
			}
			if len(predicates) > 0 {
				// Keep validation and missing/null elimination in a distinct
				// pre-aggregation scope after whole-input flags are materialized.
				fragment = "SELECT * FROM (" + fragment + ") AS " + alias + " WHERE " + strings.Join(predicates, " AND ")
				aliasSequence++
				alias = quoteIdentifier(fmt.Sprintf("_stage_%d", aliasSequence))
			}
			if len(nextState.preAggregateColumns) > 0 {
				// Materialize grouping keys and numeric measure inputs only after
				// sparse group tuples have been discarded.
				fragment = "SELECT *, " + strings.Join(nextState.preAggregateColumns, ", ") + " FROM (" + fragment + ") AS " + alias
				args = prependArguments(nextState.preAggregateArgs, args)
				aliasSequence++
				alias = quoteIdentifier(fmt.Sprintf("_stage_%d", aliasSequence))
				nextState.preAggregateColumns = nil
				nextState.preAggregateArgs = nil
			}
			fragment = "SELECT " + strings.Join(projection, ", ") + " FROM (" + fragment + ") AS " + alias
			if len(groups) > 0 {
				fragment += " GROUP BY " + strings.Join(groups, ", ")
			}
			args = append(args, aggregateArgs...)
			state = nextState
		case *plan.Timechart:
			if !permitTerminalTimechart {
				return CompiledQuery{}, errors.New("compile ClickHouse query: timechart is unavailable for event analysis")
			}
			if operatorIndex+1 != len(remainingOperators) {
				return CompiledQuery{}, errors.New("compile ClickHouse timechart: operator must be terminal")
			}
			compiled, compileErr := compileTimechart(fragment, state, args, operator, query.DynamicOutput, alias)
			if compileErr != nil {
				return CompiledQuery{}, compileErr
			}
			if len(compiled.SQL) > maxCompiledQueryBytes {
				return CompiledQuery{}, &plan.Diagnostic{
					Code:    "SPL_QUERY_TOO_COMPLEX",
					Message: fmt.Sprintf("compiled query exceeds %d bytes", maxCompiledQueryBytes),
					Range:   operator.Range,
				}
			}
			return compiled, nil
		case *plan.Window:
			expression, nextState, compileErr := compileWindow(operator, state)
			if compileErr != nil {
				return CompiledQuery{}, compileErr
			}
			fragment = "SELECT *, " + expression + " AS " + quoteIdentifier(operator.Output) + " FROM (" + fragment + ") AS " + alias
			state = nextState
		case *plan.Sort:
			materialized, sortKeys, order, compileErr := compileSort(operator.Keys, state, aliasSequence)
			if compileErr != nil {
				return CompiledQuery{}, compileErr
			}
			fragment = "SELECT *, " + strings.Join(materialized, ", ") + " FROM (" + fragment + ") AS " + alias + " ORDER BY " + order
			if operator.Limit > 0 {
				fragment += " LIMIT ?"
				args = append(args, operator.Limit)
			}
			state.order = sortKeys
		case *plan.Deduplicate:
			deduplicated, prefixArgs, currentOrder, additionalAliases, compileErr := compileDeduplicate(fragment, operator, state, aliasSequence)
			if compileErr != nil {
				return CompiledQuery{}, compileErr
			}
			fragment = deduplicated
			args = prependArguments(prefixArgs, args)
			args = append(args, operator.Count)
			state.order = currentOrder
			aliasSequence += additionalAliases
		case *plan.Limit:
			keys := state.order
			if len(keys) == 0 {
				keys = stableCompiledSortKeys()
			}
			if operator.FromEnd {
				reversed, compileErr := compileMaterializedOrder(keys, true)
				if compileErr != nil {
					return CompiledQuery{}, compileErr
				}
				fragment = "SELECT * FROM (" + fragment + ") AS " + alias + " ORDER BY " + reversed + " LIMIT ?"
				args = append(args, operator.Count)
				state.order = reverseCompiledSortKeys(keys)
			} else {
				order, compileErr := compileMaterializedOrder(keys, false)
				if compileErr != nil {
					return CompiledQuery{}, compileErr
				}
				fragment = "SELECT * FROM (" + fragment + ") AS " + alias + " ORDER BY " + order + " LIMIT ?"
				args = append(args, operator.Count)
				state.order = append([]compiledSortKey(nil), keys...)
			}
		default:
			return CompiledQuery{}, fmt.Errorf("compile ClickHouse query: unsupported logical operator %T", operator)
		}
	}

	compiled, err := finalize(fragment, state, args, scan, aliasSequence)
	if err != nil {
		return CompiledQuery{}, err
	}
	if len(compiled.SQL) > maxCompiledQueryBytes {
		return CompiledQuery{}, &plan.Diagnostic{
			Code:    "SPL_QUERY_TOO_COMPLEX",
			Message: fmt.Sprintf("compiled query exceeds %d bytes", maxCompiledQueryBytes),
			Range:   scan.Range,
		}
	}
	return compiled, nil
}

func isNilPlanOperator(operator plan.Operator) bool {
	if operator == nil {
		return true
	}
	value := reflect.ValueOf(operator)
	return value.Kind() == reflect.Pointer && value.IsNil()
}

func finalizeOrdinaryQuery(
	fragment string,
	state compileState,
	args []any,
	scan *plan.Scan,
	aliasSequence int,
) (CompiledQuery, error) {
	projection, outputFields, err := finalProjection(state)
	if err != nil {
		return CompiledQuery{}, err
	}
	aliasSequence++
	fragment = "SELECT " + strings.Join(projection, ", ") + " FROM (" + fragment + ") AS " + quoteIdentifier(fmt.Sprintf("_stage_%d", aliasSequence))
	finalOrder := defaultCompiledOrder(state)
	if len(finalOrder) > 0 {
		order, orderErr := compileMaterializedOrder(finalOrder, false)
		if orderErr != nil {
			return CompiledQuery{}, orderErr
		}
		fragment += " ORDER BY " + order
	}
	return CompiledQuery{SQL: fragment, Args: args, OutputFields: outputFields}, nil
}

func prependArguments(prefix, existing []any) []any {
	if len(prefix) == 0 {
		return existing
	}
	result := make([]any, 0, len(prefix)+len(existing))
	result = append(result, prefix...)
	return append(result, existing...)
}

func compileTimeBucket(
	fragment string,
	state compileState,
	scan *plan.Scan,
	operator *plan.TimeBucket,
	alias string,
) (string, compileState, []any, error) {
	if operator == nil {
		return "", compileState{}, nil, errors.New("compile ClickHouse time bucket: operator is nil")
	}
	if err := validateCanonicalFieldRef("time bucket", "input", operator.Field); err != nil {
		return "", compileState{}, nil, err
	}
	if err := validateCanonicalFieldRef("time bucket", "output", operator.Output); err != nil {
		return "", compileState{}, nil, err
	}
	if operator.Field.Name != "_time" || !operator.Field.Canonical {
		return "", compileState{}, nil, errors.New("compile ClickHouse time bucket: canonical _time field is required")
	}
	if operator.Span < time.Second || operator.Span >= 24*time.Hour || operator.Span%time.Second != 0 {
		return "", compileState{}, nil, errors.New("compile ClickHouse time bucket: fixed span must be at least one second and shorter than 24 hours")
	}
	if !state.eventRows {
		return "", compileState{}, nil, &plan.Diagnostic{
			Code:    "SPL_UNSUPPORTED_BIN_INPUT",
			Message: "bin requires source event rows",
			Range:   operator.Range,
		}
	}
	field, ok, err := resolveCompiledField(operator.Field, state)
	if err != nil {
		return "", compileState{}, nil, err
	}
	if !ok || field.kind != fieldKindTime || !field.canonicalTime {
		return "", compileState{}, nil, &plan.Diagnostic{
			Code:        "SPL_UNSUPPORTED_BIN_TIME_FIELD",
			Message:     "bin requires the unmodified canonical _time field",
			Range:       operator.Range,
			Suggestions: []string{"run bin before removing, replacing, transforming, or previously binning _time"},
		}
	}
	if scan == nil || !SupportsSearchTimeRange(scan.Earliest, scan.Latest) {
		return "", compileState{}, nil, errors.New("compile ClickHouse time bucket: scan range is invalid")
	}
	spanNanoseconds := int64(operator.Span)
	firstBucketTicks := floorBucketTicks(scan.Earliest.UnixNano(), spanNanoseconds)
	if firstBucketTicks < MinimumSearchTime().UnixNano() {
		return "", compileState{}, nil, &plan.Diagnostic{
			Code:    "SPL_UNSUPPORTED_BIN_TIME_RANGE",
			Message: "the first epoch-aligned bin falls before the supported timestamp range",
			Range:   operator.Range,
			Suggestions: []string{
				"use a smaller fixed span",
				"move the search earliest time forward",
			},
		}
	}

	ticks := "reinterpretAsInt64(" + field.valueSQL + ")"
	bucketTicks := "(" + epochFloorBucketNumberSQL(ticks) + ") * ?"
	value := "fromUnixTimestamp64Nano(" + bucketTicks + ", 'UTC')"
	fragment, next := compileBucketProjection(fragment, state, operator.Field.Name, operator.Output, value, field, alias)
	return fragment, next, []any{spanNanoseconds, spanNanoseconds, spanNanoseconds}, nil
}

func compileNumericBucket(
	fragment string,
	state compileState,
	operator *plan.NumericBucket,
	alias string,
	stage int,
) (string, compileState, []any, error) {
	if operator == nil {
		return "", compileState{}, nil, errors.New("compile ClickHouse numeric bucket: operator is nil")
	}
	if err := validateCanonicalFieldRef("numeric bucket", "input", operator.Input); err != nil {
		return "", compileState{}, nil, err
	}
	if err := validateCanonicalFieldRef("numeric bucket", "output", operator.Output); err != nil {
		return "", compileState{}, nil, err
	}
	if operator.Span == 0 || operator.Span > plan.MaximumNumericBinSpan {
		return "", compileState{}, nil, errors.New("compile ClickHouse numeric bucket: span must be between 1 and 2^53-1")
	}
	if operator.Input.Name == "_time" && operator.Input.Canonical {
		return "", compileState{}, nil, errors.New("compile ClickHouse numeric bucket: canonical _time cannot be a numeric input")
	}

	field, ok, err := resolveCompiledField(operator.Input, state)
	if err != nil {
		return "", compileState{}, nil, err
	}
	if !ok || field.kind != fieldKindNumber {
		return "", compileState{}, nil, unsupportedNumericBinFieldType(operator)
	}

	// WITH expression aliases can be inherited by nested subqueries in
	// ClickHouse. Make them stage-unique so consecutive bin operators cannot
	// capture one another's span or candidate.
	spanAlias := quoteIdentifier(fmt.Sprintf("__os_numeric_bin_span_%d", stage))
	candidateAlias := quoteIdentifier(fmt.Sprintf("__os_numeric_bin_candidate_%d", stage))
	input := field.valueSQL
	var spanSQL, candidateSQL, valueSQL string
	if intermediate, ok := signedBucketIntermediateType(field.numberType); ok {
		wide := "to" + intermediate + "(" + input + ")"
		spanSQL = "to" + intermediate + "(CAST(? AS UInt64))"
		bucket := "(intDiv(" + wide + ", " + spanAlias + ") - if(" +
			wide + " < 0 AND " + wide + " % " + spanAlias + " != 0, 1, 0)) * " + spanAlias
		candidateSQL = "accurateCastOrNull(" + bucket + ", '" + field.numberType + "')"
		valueSQL = guardedNumericBucketSQL(input, candidateAlias, field.numberType, "", "")
	} else if intermediate, ok := unsignedBucketIntermediateType(field.numberType); ok {
		wide := "to" + intermediate + "(" + input + ")"
		spanSQL = "to" + intermediate + "(CAST(? AS UInt64))"
		bucket := "intDiv(" + wide + ", " + spanAlias + ") * " + spanAlias
		// An unsigned bucket is never larger than its input, so this narrowing
		// cast is mathematically safe and needs no per-row range guard.
		candidateSQL = "CAST(" + bucket + " AS " + field.numberType + ")"
		valueSQL = candidateAlias
	} else if field.numberType == "Float64" {
		spanSQL = "toFloat64(CAST(? AS UInt64))"
		bucket := "floor(toFloat64(" + input + ") / " + spanAlias + ") * " + spanAlias
		candidateSQL = bucket
		finite := "isFinite(toFloat64(" + input + ")) AND isFinite(assumeNotNull(" + candidateAlias + "))"
		normalized := "if(assumeNotNull(" + candidateAlias + ") = toFloat64(0), toFloat64(0), assumeNotNull(" + candidateAlias + "))"
		valueSQL = guardedNumericBucketSQL(input, candidateAlias, "Float64", finite, normalized)
	} else {
		return "", compileState{}, nil, unsupportedNumericBinFieldType(operator)
	}

	fragment = "WITH " + spanSQL + " AS " + spanAlias + ", " +
		candidateSQL + " AS " + candidateAlias + " " +
		upsertFieldProjectionSQL(fragment, state, operator.Output.Name, valueSQL, alias)
	next := updateBucketCompileState(state, operator.Input.Name, operator.Output, field)
	return fragment, next, []any{operator.Span}, nil
}

func guardedNumericBucketSQL(input, candidate, numberType, additionalGuard, normalizedValue string) string {
	unsupported := "CAST(throwIf(toUInt8(1), '" + UnsupportedNumericBinValueMarker + "') AS " + numberType + ")"
	value := "assumeNotNull(" + candidate + ")"
	guard := "isNotNull(" + candidate + ")"
	if additionalGuard != "" {
		guard += " AND " + additionalGuard
	}
	if normalizedValue != "" {
		value = normalizedValue
	}
	return "if(isNull(" + input + "), " + input + ", if(" + guard + ", " + value + ", " + unsupported + "))"
}

func signedBucketIntermediateType(numberType string) (string, bool) {
	// Int256 is intentionally absent. ClickHouse has no wider exact signed
	// type in which to calculate the bucket below minInt256 before the guarded
	// cast back to the source type. The compiler never exposes Int256 as a
	// fixed result field.
	switch numberType {
	case "Int8", "Int16", "Int32", "Int64":
		return "Int128", true
	case "Int128":
		return "Int256", true
	default:
		return "", false
	}
}

func unsignedBucketIntermediateType(numberType string) (string, bool) {
	switch numberType {
	case "UInt8", "UInt16", "UInt32", "UInt64":
		return "UInt64", true
	case "UInt128":
		return "UInt128", true
	case "UInt256":
		return "UInt256", true
	default:
		return "", false
	}
}

func unsupportedNumericBinFieldType(operator *plan.NumericBucket) error {
	return &plan.Diagnostic{
		Code:    "SPL_UNSUPPORTED_BIN_FIELD_TYPE",
		Message: "bin with a numeric span requires a known fixed numeric field",
		Range:   operator.Input.Range,
		Suggestions: []string{
			"create a numeric field with eval before bin",
			"aggregate to a fixed numeric field with stats before bin",
		},
	}
}

func validateCanonicalFieldRef(operation, role string, field plan.FieldRef) error {
	resolved, err := plan.ResolveField(field.Name, field.Range)
	if err != nil {
		return fmt.Errorf("compile ClickHouse %s: invalid %s field: %w", operation, role, err)
	}
	if resolved.Name != field.Name || resolved.Canonical != field.Canonical ||
		!slices.Equal(resolved.Path, field.Path) {
		return fmt.Errorf("compile ClickHouse %s: %s field metadata is not canonical", operation, role)
	}
	return nil
}

func compileBucketProjection(
	fragment string,
	state compileState,
	inputName string,
	output plan.FieldRef,
	value string,
	source fieldState,
	alias string,
) (string, compileState) {
	return upsertFieldProjectionSQL(fragment, state, output.Name, value, alias),
		updateBucketCompileState(state, inputName, output, source)
}

func upsertFieldProjectionSQL(
	fragment string,
	state compileState,
	outputName string,
	value string,
	alias string,
) string {
	name := quoteIdentifier(outputName)
	if _, replacing := state.visible[outputName]; replacing {
		return "SELECT * REPLACE (" + value + " AS " + name + ") FROM (" + fragment + ") AS " + alias
	}
	return "SELECT *, " + value + " AS " + name + " FROM (" + fragment + ") AS " + alias
}

func updateBucketCompileState(state compileState, inputName string, output plan.FieldRef, source fieldState) compileState {
	next := cloneCompileState(state)
	if exposesRawFieldsPayload(state) && !output.Canonical {
		// A calculated top-level field shadows any same-named value still held
		// in the immutable event payload. Publishing that payload would expose
		// two contradictory values for one SPL field.
		next.publicOrder = slices.DeleteFunc(next.publicOrder, func(name string) bool { return name == "fields" })
	}
	if inputName != output.Name && !slices.Contains(next.publicOrder, inputName) {
		next.publicOrder = append(next.publicOrder, inputName)
	}
	if !slices.Contains(next.publicOrder, output.Name) {
		next.publicOrder = append(next.publicOrder, output.Name)
	}
	delete(next.blocked, output.Name)
	next.visible[output.Name] = fieldState{
		valueSQL:      quoteIdentifier(output.Name),
		existsSQL:     source.existsSQL,
		existsArgs:    append([]any(nil), source.existsArgs...),
		kind:          source.kind,
		numberType:    source.numberType,
		numericSort:   source.numericSort,
		canonicalTime: false,
		caseSensitive: false,
	}
	next.privateColumns = livePrivateColumns(next.privateColumns, next.visible)
	return next
}

func floorBucketTicks(value, span int64) int64 {
	quotient := value / span
	if value%span < 0 {
		quotient--
	}
	return quotient * span
}

func compileTimechart(
	fragment string,
	state compileState,
	args []any,
	operator *plan.Timechart,
	dynamic *plan.DynamicSeriesOutput,
	alias string,
) (CompiledQuery, error) {
	if operator == nil || operator.Function != plan.AggregateFunctionCountRows {
		return CompiledQuery{}, errors.New("compile ClickHouse timechart: count operator is required")
	}
	if dynamic == nil || !slices.Equal(dynamic.FixedFields, []string{"_time"}) || dynamic.MaxSeries == 0 {
		return CompiledQuery{}, errors.New("compile ClickHouse timechart: dynamic output contract is invalid")
	}
	if operator.Span < time.Second || operator.Span > 24*time.Hour || operator.Span%time.Second != 0 || operator.FirstBucket.Nanosecond() != 0 ||
		operator.FirstBucket.IsZero() || operator.BucketCount == 0 || operator.BucketCount > 10_000 || operator.SeriesLimit != 10 ||
		dynamic.MaxSeries != 12 || uint32(operator.SeriesLimit)+2 != uint32(dynamic.MaxSeries) || !operator.IncludeNull || !operator.IncludeOther ||
		operator.NullLabel != "NULL" || operator.OtherLabel != "OTHER" || !operator.FixedRange ||
		!operator.Continuous || !operator.IncludePartial {
		return CompiledQuery{}, errors.New("compile ClickHouse timechart: bounded defaults are invalid")
	}
	spanSeconds := int64(operator.Span / time.Second)
	if spanSeconds <= 0 || operator.FirstBucket.Unix()%spanSeconds != 0 {
		return CompiledQuery{}, errors.New("compile ClickHouse timechart: first bucket is not epoch aligned")
	}
	firstBucketNumber, gridOK := ordinalGridFirstBucketNumber(operator.FirstBucket.Unix(), spanSeconds, operator.BucketCount)
	if !gridOK {
		return CompiledQuery{}, errors.New("compile ClickHouse timechart: bucket grid overflows")
	}
	if !state.eventRows {
		return CompiledQuery{}, &plan.Diagnostic{
			Code:    "SPL_UNSUPPORTED_TIMECHART_INPUT",
			Message: "timechart requires event rows with the canonical _time field",
			Range:   operator.Range,
		}
	}
	timeField, ok, err := resolveCompiledField(operator.Time, state)
	if err != nil {
		return CompiledQuery{}, err
	}
	if !ok || operator.Time.Name != "_time" || timeField.kind != fieldKindTime || !timeField.canonicalTime {
		return CompiledQuery{}, &plan.Diagnostic{
			Code:    "SPL_UNSUPPORTED_TIMECHART_TIME_FIELD",
			Message: "timechart requires the unmodified canonical _time field",
			Range:   operator.Range,
		}
	}

	splitField, splitExists, err := resolveCompiledField(operator.SplitBy, state)
	if err != nil {
		return CompiledQuery{}, err
	}
	if !splitExists {
		// A projected-away split field is missing for every retained event. SPL's
		// default usenull=true therefore produces a NULL series rather than
		// resurrecting the private source document.
		splitField = fieldState{
			valueSQL:  "CAST(NULL AS Nullable(String))",
			existsSQL: "0",
			kind:      fieldKindString,
		}
	}
	if splitField.kind != fieldKindString && splitField.kind != fieldKindDynamic {
		return CompiledQuery{}, &plan.Diagnostic{
			Code:        "SPL_UNSUPPORTED_TIMECHART_FIELD_TYPE",
			Message:     "timechart split fields currently support strings and missing values",
			Range:       operator.Range,
			Suggestions: []string{"convert the split field to a string before timechart"},
		}
	}

	existsSQL := splitField.existsSQL
	if existsSQL == "" {
		existsSQL = "1"
	}
	valueTypeSQL := "if(isNull(" + splitField.valueSQL + "), 'None', 'String')"
	if splitField.kind == fieldKindDynamic {
		valueTypeSQL = dynamicTypeExpression(splitField)
	}
	// The exact-presence placeholder occurs before the nested scoped fragment.
	// Descendant detection is emitted in the following CTE so exact leaves do
	// not pay for a second field_names scan.
	args = prependArguments(splitField.existsArgs, args)
	if splitField.kind == fieldKindDynamic && splitField.descendantSQL != "" {
		args = append(args, splitField.descendantArgs...)
	}

	q := quoteIdentifier
	source := q("__os_timechart_source")
	prepared := q("__os_timechart_prepared")
	classified := q("__os_timechart_classified")
	canonicalized := q("__os_timechart_canonicalized")
	counts := q("__os_timechart_group_counts")
	top := q("__os_timechart_top")
	collapsed := q("__os_timechart_collapsed")
	domainRows := q("__os_timechart_domain_rows")
	domain := q("__os_timechart_domain")
	collisions := q("__os_timechart_normalization_collisions")
	bucketMaps := q("__os_timechart_bucket_maps")
	validation := q("__os_timechart_validation")
	grid := q("__os_timechart_grid")

	eventTime := q("__os_tc_event_time")
	value := q("__os_tc_value")
	present := q("__os_tc_present")
	descendant := q("__os_tc_descendant")
	valueType := q("__os_tc_value_type")
	ticks := q("__os_tc_ticks")
	label := q("__os_tc_label")
	bucketNumber := q("__os_tc_bucket_number")
	kind := q("__os_tc_kind")
	frequency := q("__os_tc_count")
	encoded := q("__os_tc_encoded")
	normalized := q("__os_tc_normalized")
	sortLabel := q("__os_tc_sort_label")
	countMap := q("__os_tc_count_map")
	invalid := q("__os_tc_invalid")
	ordinal := q(TimechartOrdinalColumn)

	spanNanoseconds := int64(operator.Span)
	bucketNumberExpression := epochFloorBucketNumberSQL(ticks)
	validLabel := "isValidUTF8(" + label + ") AND length(" + label + ") BETWEEN 1 AND " +
		strconv.Itoa(maxTimechartLabelBytes) + " AND " + label + " NOT IN ('NULL', 'OTHER')"

	var sql strings.Builder
	sql.Grow(len(fragment) + 8_192)
	sql.WriteString("WITH ")
	sql.WriteString(source)
	sql.WriteString(" AS (SELECT ")
	sql.WriteString(timeField.valueSQL + " AS " + eventTime + ", ")
	sql.WriteString(splitField.valueSQL + " AS " + value + ", ")
	sql.WriteString("toUInt8(" + existsSQL + ") AS " + present + ", ")
	sql.WriteString(valueTypeSQL + " AS " + valueType)
	if splitField.kind == fieldKindDynamic && splitField.descendantSQL != "" {
		sql.WriteString(", " + q(internalFieldNamesColumn))
	}
	sql.WriteString(" FROM (")
	sql.WriteString(fragment)
	sql.WriteString(") AS " + alias + "), ")

	sql.WriteString(prepared)
	sql.WriteString(" AS (SELECT *, ")
	if splitField.kind == fieldKindDynamic && splitField.descendantSQL != "" {
		sql.WriteString("toUInt8(if(" + present + " != 0, 0, " + splitField.descendantSQL + ")) AS " + descendant + ", ")
	} else {
		sql.WriteString("toUInt8(0) AS " + descendant + ", ")
	}
	sql.WriteString("reinterpretAsInt64(" + eventTime + ") AS " + ticks + ", ")
	sql.WriteString("if(" + present + " != 0 AND isNotNull(" + value + ") AND " + valueType + " = 'String', ")
	sql.WriteString("assumeNotNull(toString(" + value + ")), CAST('' AS String)) AS " + label)
	sql.WriteString(" FROM " + source + "), ")

	sql.WriteString(classified)
	sql.WriteString(" AS (SELECT " + bucketNumberExpression + " AS " + bucketNumber + ", ")
	sql.WriteString("multiIf(" + descendant + " != 0, toUInt8(3), " + present + " = 0 OR isNull(" + value + ") OR " + valueType + " = 'None', toUInt8(1), ")
	sql.WriteString(valueType + " != 'String', toUInt8(3), NOT (" + validLabel + "), toUInt8(3), toUInt8(0)) AS " + kind + ", " + label)
	sql.WriteString(" FROM " + prepared + "), ")

	sql.WriteString(canonicalized)
	sql.WriteString(" AS (SELECT " + bucketNumber + ", " + kind + ", if(" + kind + " = 0, " + label + ", CAST('' AS String)) AS " + label)
	sql.WriteString(" FROM " + classified + "), ")

	sql.WriteString(counts)
	sql.WriteString(" AS MATERIALIZED (SELECT " + bucketNumber + ", " + kind + ", " + label + ", count() AS " + frequency)
	sql.WriteString(" FROM " + canonicalized + " GROUP BY " + bucketNumber + ", " + kind + ", " + label + "), ")

	sql.WriteString(top)
	sql.WriteString(" AS MATERIALIZED (SELECT " + label + ", sum(" + frequency + ") AS " + frequency + " FROM " + counts)
	sql.WriteString(" WHERE " + kind + " = 0 GROUP BY " + label + " ORDER BY " + frequency + " DESC, " + label + " ASC LIMIT ")
	sql.WriteString(strconv.FormatUint(uint64(operator.SeriesLimit), 10))
	sql.WriteString("), ")

	sql.WriteString(collapsed)
	sql.WriteString(" AS (SELECT " + bucketNumber + ", multiIf(" + kind + " = 1, '1:', ")
	sql.WriteString(label + " IN (SELECT " + label + " FROM " + top + "), concat('0:', " + label + "), '2:') AS " + encoded + ", ")
	sql.WriteString("sum(" + frequency + ") AS " + frequency + " FROM " + counts + " WHERE " + kind + " IN (0, 1) GROUP BY " + bucketNumber + ", " + encoded + "), ")

	sql.WriteString(domainRows)
	sql.WriteString(" AS (SELECT toUInt8(0) AS sort_kind, if(startsWith(" + label + ", '_'), concat('VALUE', " + label + "), " + label + ") AS " + sortLabel + ", concat('0:', " + label + ") AS " + encoded + " FROM " + top)
	sql.WriteString(" UNION ALL SELECT toUInt8(1), CAST('' AS String), CAST('1:' AS String) WHERE (SELECT count() FROM " + counts + " WHERE " + kind + " = 1) > 0")
	sql.WriteString(" UNION ALL SELECT toUInt8(2), CAST('' AS String), CAST('2:' AS String) WHERE (SELECT count() FROM " + counts + " WHERE " + kind + " = 0 AND " + label + " NOT IN (SELECT " + label + " FROM " + top + ")) > 0), ")

	sql.WriteString(domain)
	sql.WriteString(" AS (SELECT arrayMap(item -> item.3, arraySort(item -> (item.1, item.2), groupArray((sort_kind, " + sortLabel + ", " + encoded + ")))) AS names FROM " + domainRows + "), ")

	sql.WriteString(collisions)
	sql.WriteString(" AS (SELECT if(startsWith(" + label + ", '_'), concat('VALUE', " + label + "), " + label + ") AS " + normalized)
	sql.WriteString(" FROM " + counts + " WHERE " + kind + " = 0 GROUP BY " + normalized + " HAVING uniqExact(" + label + ") > 1 LIMIT 1), ")

	sql.WriteString(bucketMaps)
	sql.WriteString(" AS (SELECT " + bucketNumber + ", mapFromArrays(groupArray(" + encoded + "), groupArray(" + frequency + ")) AS " + countMap)
	sql.WriteString(" FROM " + collapsed + " GROUP BY " + bucketNumber + "), ")

	sql.WriteString(validation)
	sql.WriteString(" AS (SELECT toUInt8(sumIf(" + frequency + ", " + kind + " = 3) > 0 OR ifNull((SELECT count() FROM " + collisions + "), toUInt64(0)) > 0) AS " + invalid + " FROM " + counts + "), ")

	sql.WriteString(grid)
	sql.WriteString(" AS (" + ordinalGridSQL(ordinal, bucketNumber) + ") ")

	sql.WriteString("SELECT " + grid + "." + ordinal + " AS " + ordinal + ", " + domain + ".names AS " + q(TimechartNamesColumn) + ", ")
	sql.WriteString("arrayMap(name -> ifNull(" + bucketMaps + "." + countMap + "[name], toUInt64(0)), " + domain + ".names) AS " + q(TimechartCountsColumn) + ", ")
	sql.WriteString(validation + "." + invalid + " AS " + q(TimechartInvalidColumn) + " FROM " + grid + " CROSS JOIN " + domain + " CROSS JOIN " + validation)
	sql.WriteString(" LEFT JOIN " + bucketMaps + " ON " + bucketMaps + "." + bucketNumber + " = " + grid + "." + bucketNumber + " ORDER BY " + grid + "." + ordinal + " ASC")

	args = appendOrdinalGridArgs(args, spanNanoseconds, firstBucketNumber, operator.BucketCount)
	return CompiledQuery{
		SQL:          sql.String(),
		Args:         args,
		OutputFields: slices.Clone(dynamic.FixedFields),
		Timechart: &TimechartOutput{
			FirstBucket:   operator.FirstBucket.UTC(),
			Span:          operator.Span,
			BucketCount:   operator.BucketCount,
			MaxSeries:     dynamic.MaxSeries,
			MaxLabelBytes: maxTimechartLabelBytes,
		},
	}, nil
}

type compileState struct {
	visible                       map[string]fieldState
	publicOrder                   []string
	privateColumns                []string
	rexCapturedBytesSQL           string
	allowDynamic                  bool
	eventRows                     bool
	blocked                       map[string]struct{}
	blockedPrefixes               map[string]struct{}
	order                         []compiledSortKey
	tieBreakers                   []compiledSortKey
	preAggregateValidationColumns []string
	preAggregateValidationArgs    []any
	preAggregateColumns           []string
	preAggregateArgs              []any
}

type fieldKind uint8

const (
	fieldKindInvalid fieldKind = iota
	fieldKindDynamic
	fieldKindString
	fieldKindNumber
	fieldKindBool
	fieldKindTime
)

type fieldState struct {
	valueSQL       string
	dynamicTypeSQL string
	storedTypeSQL  string
	existsSQL      string
	existsArgs     []any
	descendantSQL  string
	descendantArgs []any
	kind           fieldKind
	caseSensitive  bool
	numberType     string
	numericSort    bool
	canonicalTime  bool
}

type compiledSortKey struct {
	valueSQL   string
	descending bool
	nullsFirst bool
}

func compileScan(database, table string, scan *plan.Scan) (string, compileState, []any, error) {
	if scan.TenantID == "" || len(scan.Indexes) == 0 || scan.Earliest.IsZero() || scan.Latest.IsZero() || !scan.Earliest.Before(scan.Latest) || scan.IndexTimeCutoff.IsZero() {
		return "", compileState{}, nil, errors.New("compile ClickHouse query: Scan has an invalid security or time scope")
	}
	selects := []string{
		aliasPhysical("event_id", "event_id"),
		aliasPhysical("index_name", "index"),
		aliasPhysical("event_time", "_time"),
		aliasPhysical("index_time", "_indextime"),
		aliasPhysical("host", "host"),
		aliasPhysical("source", "source"),
		aliasPhysical("sourcetype", "sourcetype"),
		aliasPhysical("service", "service"),
		aliasPhysical("severity", "severity"),
		aliasPhysical("level", "level"),
		aliasPhysical("body", "message"),
		aliasPhysical("raw", "_raw"),
		aliasPhysical("trace_id", "trace_id"),
		aliasPhysical("span_id", "span_id"),
		aliasPhysical("collector_id", "collector_id"),
		aliasPhysical("batch_id", "batch_id"),
		aliasPhysical("fields", internalFieldsColumn),
		aliasPhysical("field_names", internalFieldNamesColumn),
		aliasPhysical("field_types", internalFieldTypesColumn),
		aliasPhysical("field_metadata_version", internalFieldMetadataVersionColumn),
		aliasPhysical("event_time", internalSortTimeColumn),
		aliasPhysical("event_id", internalSortIDColumn),
	}
	placeholders := strings.TrimSuffix(strings.Repeat("?, ", len(scan.Indexes)), ", ")
	where := []string{
		quoteIdentifier("tenant_id") + " = ?",
		quoteIdentifier("index_name") + " IN (" + placeholders + ")",
		quoteIdentifier("event_time") + " >= parseDateTime64BestEffort(?, 9, 'UTC')",
		quoteIdentifier("event_time") + " < parseDateTime64BestEffort(?, 9, 'UTC')",
		quoteIdentifier("index_time") + " <= parseDateTime64BestEffort(?, 3, 'UTC')",
		quoteIdentifier("visibility_seq") + " <= ?",
	}
	args := make([]any, 0, len(scan.Indexes)+5)
	args = append(args, scan.TenantID)
	for _, index := range scan.Indexes {
		args = append(args, index)
	}
	// clickhouse-go infers a bare time.Time placeholder as DateTime, which has
	// only second precision. Bind canonical text and parse it explicitly so a
	// DateTime64(3) cutoff cannot exclude rows committed earlier in the same
	// second. Text also avoids UnixNano overflow for supported pre-epoch and
	// upper-bound DateTime64 values.
	args = append(args,
		formatDateTime64Nanoseconds(scan.Earliest),
		formatDateTime64Nanoseconds(scan.Latest),
		formatDateTime64Milliseconds(scan.IndexTimeCutoff),
		scan.VisibilityCutoff,
	)

	visible := make(map[string]fieldState, len(canonicalColumnNames))
	for _, field := range canonicalColumnNames {
		visible[field] = canonicalState(field)
	}
	state := compileState{
		visible:         visible,
		publicOrder:     append([]string(nil), defaultPublicFields...),
		allowDynamic:    true,
		eventRows:       true,
		blocked:         make(map[string]struct{}),
		blockedPrefixes: make(map[string]struct{}),
		tieBreakers: []compiledSortKey{
			{valueSQL: quoteIdentifier(internalSortIDColumn), descending: true},
		},
	}
	return "SELECT " + strings.Join(selects, ", ") + " FROM " + quoteIdentifier(database) + "." + quoteIdentifier(table) + " WHERE " + strings.Join(where, " AND "), state, args, nil
}

func formatDateTime64Nanoseconds(value time.Time) string {
	return value.UTC().Format("2006-01-02 15:04:05.000000000")
}

func formatDateTime64Milliseconds(value time.Time) string {
	return value.UTC().Truncate(time.Millisecond).Format("2006-01-02 15:04:05.000")
}

var canonicalColumnNames = eventfields.CanonicalSPLFieldNames()

var defaultPublicFields = []string{
	"_time", "_raw", "index", "host", "source", "sourcetype", "service", "level", "message", "trace_id", "span_id", "event_id", "_indextime", "fields",
}

func canonicalState(field string) fieldState {
	value := quoteIdentifier(field)
	kind := fieldKindString
	switch field {
	case "severity":
		kind = fieldKindNumber
	case "_time", "_indextime":
		kind = fieldKindTime
	}
	// Canonical columns exist in the event schema even when their value is
	// nullable. This preserves explicit-null comparisons; field=* separately
	// requires a non-null value.
	state := fieldState{valueSQL: value, existsSQL: "1", kind: kind, caseSensitive: field == "index"}
	if field == "severity" {
		state.numberType = "UInt8"
	}
	if field == "_time" {
		state.canonicalTime = true
	}
	return state
}

func compileExpression(expression plan.Expression, state compileState) (string, []any, error) {
	switch expression := expression.(type) {
	case *plan.BooleanExpression:
		left, leftArgs, err := compileExpression(expression.Left, state)
		if err != nil {
			return "", nil, err
		}
		right, rightArgs, err := compileExpression(expression.Right, state)
		if err != nil {
			return "", nil, err
		}
		operator := "AND"
		if expression.Op == plan.BooleanOpOr {
			operator = "OR"
		}
		return "(" + left + " " + operator + " " + right + ")", append(leftArgs, rightArgs...), nil
	case *plan.NotExpression:
		operand, args, err := compileExpression(expression.Operand, state)
		if err != nil {
			return "", nil, err
		}
		return "NOT (" + operand + ")", args, nil
	case *plan.TextExpression:
		raw, ok := state.visible["_raw"]
		if !ok {
			return "0", nil, nil
		}
		if expression.Value == "*" {
			return "isNotNull(" + raw.valueSQL + ")", nil, nil
		}
		if expression.Wildcard {
			return "match(toString(" + raw.valueSQL + "), ?)", []any{freeTextRegex(expression.Value, expression.Quoted)}, nil
		}
		if !expression.Quoted {
			return "match(toString(" + raw.valueSQL + "), ?)", []any{freeTextRegex(expression.Value, false)}, nil
		}
		return "positionCaseInsensitiveUTF8(toString(" + raw.valueSQL + "), ?) > 0", []any{expression.Value}, nil
	case *plan.ComparisonExpression:
		field, ok, err := resolveCompiledField(expression.Field, state)
		if err != nil {
			return "", nil, err
		}
		if !ok {
			return "0", nil, nil
		}
		return compileComparison(expression, field)
	case *plan.EvalComparisonExpression:
		return compileEvalComparison(expression, state)
	default:
		return "", nil, fmt.Errorf("compile ClickHouse predicate: unsupported expression %T", expression)
	}
}

type compiledScalar struct {
	valueSQL       string
	valueArgs      []any
	existsSQL      string
	existsArgs     []any
	dynamicTypeSQL string
	storedTypeSQL  string
	descendantSQL  string
	descendantArgs []any
	kind           fieldKind
	numberType     string
	literal        *plan.Value
}

func compileScalarValue(expression plan.ScalarExpression, state compileState) (compiledScalar, error) {
	switch expression := expression.(type) {
	case *plan.ScalarFieldExpression:
		field, ok, err := resolveCompiledField(expression.Field, state)
		if err != nil {
			return compiledScalar{}, err
		}
		if !ok {
			return compiledScalar{
				valueSQL:  "CAST(NULL AS Nullable(String))",
				existsSQL: "0",
				kind:      fieldKindString,
			}, nil
		}
		return compiledScalarFromField(field), nil
	case *plan.ScalarLiteralExpression:
		value := expression.Value
		kind := fieldKindString
		numberType := ""
		valueSQL := ""
		var argument any
		switch value.Kind {
		case plan.ValueKindString:
			valueSQL, argument = "CAST(? AS String)", value.String
		case plan.ValueKindInt64:
			kind, numberType = fieldKindNumber, "Int64"
			valueSQL, argument = "CAST(? AS Int64)", value.Int64
		case plan.ValueKindUint64:
			kind, numberType = fieldKindNumber, "UInt64"
			valueSQL, argument = "CAST(? AS UInt64)", value.Uint64
		case plan.ValueKindFloat64:
			kind, numberType = fieldKindNumber, "Float64"
			valueSQL, argument = "CAST(? AS Float64)", value.Float64
		case plan.ValueKindBool:
			kind = fieldKindBool
			valueSQL, argument = "CAST(? AS Bool)", value.Bool
		case plan.ValueKindNull:
			return compiledScalar{
				valueSQL:  "CAST(NULL AS Nullable(String))",
				existsSQL: "1",
				kind:      fieldKindInvalid,
				literal:   &value,
			}, nil
		default:
			return compiledScalar{}, errors.New("compile ClickHouse scalar expression: invalid literal")
		}
		return compiledScalar{
			valueSQL:   valueSQL,
			valueArgs:  []any{argument},
			existsSQL:  "1",
			kind:       kind,
			numberType: numberType,
			literal:    &value,
		}, nil
	case *plan.ScalarCallExpression:
		switch expression.Function {
		case plan.ScalarFunctionReplace:
			return compileReplaceScalar(expression, state)
		case plan.ScalarFunctionToNumber:
			return compileToNumberScalar(expression, state)
		default:
			return compiledScalar{}, fmt.Errorf("compile ClickHouse scalar expression: unsupported function %d", expression.Function)
		}
	default:
		return compiledScalar{}, fmt.Errorf("compile ClickHouse scalar expression: unsupported expression %T", expression)
	}
}

func compileReplaceScalar(expression *plan.ScalarCallExpression, state compileState) (compiledScalar, error) {
	if len(expression.Arguments) != 3 {
		return compiledScalar{}, errors.New("compile ClickHouse replace: expected three arguments")
	}
	input, err := compileScalarValue(expression.Arguments[0], state)
	if err != nil {
		return compiledScalar{}, err
	}
	pattern, ok := scalarStringLiteral(expression.Arguments[1])
	if !ok {
		return compiledScalar{}, errors.New("compile ClickHouse replace: regular expression must be a string literal")
	}
	if pattern == "" {
		return compiledScalar{}, errors.New("compile ClickHouse replace: empty regular expressions are not supported")
	}
	if err := splregex.ValidateReplacePattern(pattern); err != nil {
		return compiledScalar{}, fmt.Errorf("compile ClickHouse replace: regular expression is outside the supported RE2 subset: %w", err)
	}
	replacement, ok := scalarStringLiteral(expression.Arguments[2])
	if !ok {
		return compiledScalar{}, errors.New("compile ClickHouse replace: replacement must be a string literal")
	}
	inputSQL, inputArgs := compiledStringScalar(input)
	return compiledScalar{
		valueSQL:  "replaceRegexpAll(" + inputSQL + ", ?, ?)",
		valueArgs: append(inputArgs, pattern, replacement),
		existsSQL: "1",
		kind:      fieldKindString,
	}, nil
}

func compileToNumberScalar(expression *plan.ScalarCallExpression, state compileState) (compiledScalar, error) {
	if len(expression.Arguments) != 1 {
		return compiledScalar{}, errors.New("compile ClickHouse tonumber: expected one argument")
	}
	input, err := compileScalarValue(expression.Arguments[0], state)
	if err != nil {
		return compiledScalar{}, err
	}
	inputSQL, inputArgs := compiledStringScalar(input)
	return compiledScalar{
		valueSQL:   "ifNotFinite(toFloat64OrNull(" + inputSQL + "), CAST(NULL AS Nullable(Float64)))",
		valueArgs:  inputArgs,
		existsSQL:  "1",
		kind:       fieldKindNumber,
		numberType: "Float64",
	}, nil
}

func compiledStringScalar(value compiledScalar) (string, []any) {
	if value.kind == fieldKindDynamic {
		return "if(" + value.existsSQL + ", dynamicElement(" + value.valueSQL + ", 'String'), CAST(NULL AS Nullable(String)))",
			append(append([]any(nil), value.existsArgs...), value.valueArgs...)
	}
	if value.existsSQL != "" && value.existsSQL != "1" {
		return "if(" + value.existsSQL + ", toString(" + value.valueSQL + "), CAST(NULL AS Nullable(String)))",
			append(append([]any(nil), value.existsArgs...), value.valueArgs...)
	}
	if value.kind == fieldKindString {
		return value.valueSQL, append([]any(nil), value.valueArgs...)
	}
	if value.kind == fieldKindTime {
		return "toString(" + numericScalarSQL(value, false) + ")", append([]any(nil), value.valueArgs...)
	}
	return "toString(" + value.valueSQL + ")", append([]any(nil), value.valueArgs...)
}

func scalarStringLiteral(expression plan.ScalarExpression) (string, bool) {
	literal, ok := expression.(*plan.ScalarLiteralExpression)
	if !ok || literal.Value.Kind != plan.ValueKindString {
		return "", false
	}
	return literal.Value.String, true
}

func extendCompileState(state compileState, output plan.FieldRef, value compiledScalar) compileState {
	next := state
	next.visible = make(map[string]fieldState, len(state.visible)+1)
	for name, field := range state.visible {
		next.visible[name] = field
	}
	next.publicOrder = append([]string(nil), state.publicOrder...)
	next.blocked = cloneSet(state.blocked)
	next.blockedPrefixes = cloneSet(state.blockedPrefixes)
	delete(next.blocked, output.Name)
	if !slices.Contains(next.publicOrder, output.Name) {
		next.publicOrder = append(next.publicOrder, output.Name)
	}
	field := fieldState{
		valueSQL:       quoteIdentifier(output.Name),
		existsSQL:      "1",
		descendantSQL:  value.descendantSQL,
		descendantArgs: append([]any(nil), value.descendantArgs...),
		storedTypeSQL:  value.storedTypeSQL,
		kind:           value.kind,
		// An eval output named index is calculated data, not the physical scan
		// selector. It follows its expression type and ordinary comparison rules.
		caseSensitive: false,
		numberType:    value.numberType,
	}
	if value.kind == fieldKindDynamic {
		field.dynamicTypeSQL = "dynamicType(" + field.valueSQL + ")"
	}
	next.visible[output.Name] = field
	next.privateColumns = livePrivateColumns(next.privateColumns, next.visible)
	return next
}

type compiledExtractCapture struct {
	planCapture      plan.ExtractCapture
	valueSQL         string
	existsColumn     string
	existsProjection string
	typeColumn       string
	typeProjection   string
}

func extractPrivateColumns(captures []compiledExtractCapture) []string {
	columns := make([]string, 0, len(captures)*2)
	for _, capture := range captures {
		columns = append(columns, capture.existsColumn, capture.typeColumn)
	}
	return columns
}

func compileExtract(
	fragment string,
	operator *plan.Extract,
	state compileState,
	stage int,
) (string, compileState, []any, int, error) {
	validated, err := validateExtractOperator(operator)
	if err != nil {
		return "", compileState{}, nil, 0, err
	}
	openEventSchema := state.eventRows && state.allowDynamic
	if openEventSchema && operator.Input.Name == "fields" {
		return "", compileState{}, nil, 0, &plan.Diagnostic{
			Code:    "SPL_AMBIGUOUS_REX_FIELD",
			Message: "rex cannot read the event result's reserved fields payload without an exact upstream schema",
			Range:   operator.Range,
		}
	}

	inputSQL, eligibleSQL, inputArgs, err := compileExtractInput(operator.Input, state)
	if err != nil {
		return "", compileState{}, nil, 0, err
	}
	eligibleAlias := quoteIdentifier(fmt.Sprintf("__os_rex_eligible_%d", stage))
	inputAlias := quoteIdentifier(fmt.Sprintf("__os_rex_input_%d", stage))
	groupsAlias := quoteIdentifier(fmt.Sprintf("__os_rex_groups_%d", stage))
	matchedAlias := quoteIdentifier(fmt.Sprintf("__os_rex_matched_%d", stage))
	capturedBytesAlias := quoteIdentifier(fmt.Sprintf("__os_rex_captured_bytes_%d", stage))
	innerAlias := quoteIdentifier(fmt.Sprintf("_stage_%d", stage))
	inputExpressions := []string{
		"toUInt8(ifNull(" + eligibleSQL + ", 0)) AS " + eligibleAlias,
		"if(" + eligibleAlias + " != 0, assumeNotNull(" + inputSQL + "), CAST('' AS String)) AS " + inputAlias,
	}
	inputFragment := "SELECT *, " + strings.Join(inputExpressions, ", ") + " FROM (" + fragment + ") AS " + innerAlias
	groupExpression := "if(" + eligibleAlias + " != 0, extractGroups(" + inputAlias +
		", ?), CAST([], 'Array(String)')) AS " + groupsAlias
	groupFragment := "SELECT *, " + groupExpression + " FROM (" + inputFragment + ") AS " +
		quoteIdentifier(fmt.Sprintf("_rex_input_%d", stage))
	capturedBytesExpression := "arraySum(value -> toUInt64(length(value)), " + groupsAlias + ")"
	if state.rexCapturedBytesSQL != "" {
		capturedBytesExpression = "toUInt64(" + state.rexCapturedBytesSQL + ") + " + capturedBytesExpression
	}
	bytesFragment := "SELECT *, " + capturedBytesExpression + " AS " + capturedBytesAlias +
		" FROM (" + groupFragment + ") AS " + quoteIdentifier(fmt.Sprintf("_rex_groups_%d", stage))
	matchedExpression := "toUInt8(if(" + capturedBytesAlias + " > toUInt64(" +
		strconv.FormatUint(MaximumRexCapturedBytesPerRow, 10) + "), " +
		"throwIf(toUInt8(1), '" + RexCaptureLimitMarker + "') = 0, " +
		eligibleAlias + " != 0 AND notEmpty(" + groupsAlias + "))) AS " + matchedAlias
	// Keep the extraction and byte guard streaming. The pinned ClickHouse
	// integration test uses EXPLAIN actions=1 to prove that common-expression
	// elimination still executes extractGroups exactly once for all captures.
	fragment = "SELECT *, " + matchedExpression + " FROM (" + bytesFragment + ") AS " +
		quoteIdentifier(fmt.Sprintf("_rex_bytes_%d", stage))
	// The groups SELECT appears textually before its nested input SELECT, so
	// the pattern placeholder precedes source-presence placeholders.
	innerArgs := append([]any{validated.Pattern}, inputArgs...)

	next := cloneCompileState(state)
	next.rexCapturedBytesSQL = capturedBytesAlias
	if exposesRawFieldsPayload(state) {
		// The stored convenience object cannot be rewritten cheaply per event.
		// Drop it once a rex output can shadow one of its immutable members.
		next.publicOrder = slices.DeleteFunc(next.publicOrder, func(name string) bool { return name == "fields" })
	}
	captures := make([]compiledExtractCapture, 0, len(operator.Captures))
	existenceArgs := make([]any, 0)
	typeArgs := make([]any, 0)
	seenOutputs := make(map[string]struct{}, len(operator.Captures))
	for index, capture := range operator.Captures {
		if openEventSchema && capture.Output.Name == "fields" {
			return "", compileState{}, nil, 0, &plan.Diagnostic{
				Code:    "SPL_AMBIGUOUS_REX_FIELD",
				Message: "rex cannot replace the event result's reserved fields payload without an exact upstream schema",
				Range:   operator.Range,
			}
		}
		if _, duplicate := seenOutputs[capture.Output.Name]; duplicate {
			return "", compileState{}, nil, 0, errors.New("compile ClickHouse extract: output field is repeated")
		}
		seenOutputs[capture.Output.Name] = struct{}{}
		previous, previousKnown, resolveErr := resolveCompiledField(capture.Output, state)
		if resolveErr != nil {
			return "", compileState{}, nil, 0, resolveErr
		}

		capturedValue := "arrayElement(" + groupsAlias + ", " + strconv.Itoa(int(capture.Group)) + ")"
		valueSQL := ""
		kind := fieldKindDynamic
		switch {
		case !previousKnown:
			kind = fieldKindString
			valueSQL = "if(" + matchedAlias + " != 0, " + capturedValue + ", CAST(NULL AS Nullable(String)))"
		case previous.kind == fieldKindString:
			kind = fieldKindString
			valueSQL = "if(" + matchedAlias + " != 0, " + capturedValue + ", " + previous.valueSQL + ")"
		default:
			valueSQL = "if(" + matchedAlias + " != 0, CAST(" + capturedValue + " AS Dynamic), CAST(" + previous.valueSQL + " AS Dynamic))"
		}

		previousExists := "0"
		if previousKnown {
			previousExists = previous.existsSQL
			if previousExists == "" {
				previousExists = "1"
			}
			existenceArgs = append(existenceArgs, previous.existsArgs...)
			if previous.kind == fieldKindDynamic && previous.descendantSQL != "" {
				previousExists = "((" + previousExists + ") OR (" + previous.descendantSQL + "))"
				existenceArgs = append(existenceArgs, previous.descendantArgs...)
			}
		}
		existsName := fmt.Sprintf("__os_rex_exists_%d_%d", stage, index)
		existsAlias := quoteIdentifier(existsName)
		existsSQL := "toUInt8(if(" + matchedAlias + " != 0, 1, ifNull(" + previousExists + ", 0))) AS " + existsAlias

		previousTypeSQL := "toUInt8(0)"
		if previousKnown {
			var previousTypeArgs []any
			previousTypeSQL, previousTypeArgs, resolveErr = knownFieldStoredTypeSQL(previous)
			if resolveErr != nil {
				return "", compileState{}, nil, 0, fmt.Errorf(
					"compile ClickHouse extract: resolve prior type for %q: %w",
					capture.Output.Name,
					resolveErr,
				)
			}
			typeArgs = append(typeArgs, previousTypeArgs...)
		}
		typeName := fmt.Sprintf("__os_rex_type_%d_%d", stage, index)
		typeAlias := quoteIdentifier(typeName)
		typeSQL := "toUInt8(if(" + matchedAlias + " != 0, toUInt8(" +
			strconv.Itoa(int(eventfields.StoredValueTypeString)) + "), " +
			previousTypeSQL + ")) AS " + typeAlias
		captures = append(captures, compiledExtractCapture{
			planCapture:      capture,
			valueSQL:         valueSQL,
			existsColumn:     existsAlias,
			existsProjection: existsSQL,
			typeColumn:       typeAlias,
			typeProjection:   typeSQL,
		})

		delete(next.blocked, capture.Output.Name)
		if !slices.Contains(next.publicOrder, capture.Output.Name) {
			next.publicOrder = append(next.publicOrder, capture.Output.Name)
		}
		output := quoteIdentifier(capture.Output.Name)
		field := fieldState{
			valueSQL:       output,
			existsSQL:      existsAlias,
			storedTypeSQL:  typeAlias,
			descendantSQL:  previous.descendantSQL,
			descendantArgs: append([]any(nil), previous.descendantArgs...),
			kind:           kind,
			// A capture named index is calculated data and never regains the
			// physical scan selector's case or authorization semantics.
			caseSensitive: false,
		}
		if kind == fieldKindDynamic {
			field.dynamicTypeSQL = "dynamicType(" + output + ")"
		}
		next.visible[capture.Output.Name] = field
	}

	valueByName := make(map[string]string, len(captures))
	existenceExpressions := make([]string, 0, len(captures))
	typeExpressions := make([]string, 0, len(captures))
	for _, capture := range captures {
		valueByName[capture.planCapture.Output.Name] = capture.valueSQL
		existenceExpressions = append(existenceExpressions, capture.existsProjection)
		typeExpressions = append(typeExpressions, capture.typeProjection)
	}
	liveOldPrivateColumns := livePrivateColumns(state.privateColumns, next.visible)
	next.privateColumns = append(
		append([]string(nil), liveOldPrivateColumns...),
		extractPrivateColumns(captures)...,
	)
	projection := make([]string, 0, len(next.visible)+len(captures)+8)
	for _, name := range orderedVisibleNames(next) {
		publicName := quoteIdentifier(name)
		if valueSQL, captured := valueByName[name]; captured {
			projection = append(projection, valueSQL+" AS "+publicName)
			continue
		}
		field, ok := state.visible[name]
		if !ok {
			return "", compileState{}, nil, 0, fmt.Errorf("compile ClickHouse extract: field %q has no input value", name)
		}
		if field.valueSQL == publicName {
			projection = append(projection, publicName)
		} else {
			projection = append(projection, field.valueSQL+" AS "+publicName)
		}
	}
	projectionState := next
	projectionState.privateColumns = liveOldPrivateColumns
	projection = appendPrivateEventProjection(projection, projectionState)
	projection = append(projection, existenceExpressions...)
	projection = append(projection, typeExpressions...)
	outerAlias := quoteIdentifier(fmt.Sprintf("_stage_%d", stage+1))
	fragment = "SELECT " + strings.Join(projection, ", ") + " FROM (" + fragment + ") AS " + outerAlias

	prefixArgs := make([]any, 0, len(existenceArgs)+len(typeArgs)+len(innerArgs))
	prefixArgs = append(prefixArgs, existenceArgs...)
	prefixArgs = append(prefixArgs, typeArgs...)
	prefixArgs = append(prefixArgs, innerArgs...)
	return fragment, next, prefixArgs, 1, nil
}

func validateExtractOperator(operator *plan.Extract) (splregex.ExtractionPattern, error) {
	if operator == nil || operator.Input.Name == "" || len(operator.Captures) == 0 ||
		!strings.HasPrefix(operator.Pattern, "(?-s)") {
		return splregex.ExtractionPattern{}, errors.New("compile ClickHouse extract: operator is invalid")
	}
	if err := validateCanonicalFieldRef("extract", "input", operator.Input); err != nil {
		return splregex.ExtractionPattern{}, err
	}
	withoutDefaultFlags := strings.TrimPrefix(operator.Pattern, "(?-s)")
	validated, err := splregex.CompileExtractionPattern(withoutDefaultFlags)
	if err != nil || validated.Pattern != operator.Pattern {
		if err == nil {
			err = errors.New("pattern is not in canonical form")
		}
		return splregex.ExtractionPattern{}, fmt.Errorf("compile ClickHouse extract: invalid pattern: %w", err)
	}
	if len(operator.Captures) != len(validated.Captures) {
		return splregex.ExtractionPattern{}, errors.New("compile ClickHouse extract: capture metadata does not match pattern")
	}
	seen := make(map[string]struct{}, len(operator.Captures))
	for index, capture := range operator.Captures {
		expected := validated.Captures[index]
		if capture.Output.Name == "" || capture.Group == 0 || int(capture.Group) != expected.Group ||
			capture.Output.Name != expected.Name {
			return splregex.ExtractionPattern{}, errors.New("compile ClickHouse extract: capture metadata does not match pattern")
		}
		if err := validateCanonicalFieldRef("extract", "output", capture.Output); err != nil {
			return splregex.ExtractionPattern{}, err
		}
		if _, duplicate := seen[capture.Output.Name]; duplicate {
			return splregex.ExtractionPattern{}, errors.New("compile ClickHouse extract: output field is repeated")
		}
		seen[capture.Output.Name] = struct{}{}
	}
	return validated, nil
}

func compileExtractInput(input plan.FieldRef, state compileState) (valueSQL, eligibleSQL string, args []any, err error) {
	field, ok, err := resolveCompiledField(input, state)
	if err != nil {
		return "", "", nil, err
	}
	if !ok {
		return "CAST(NULL AS Nullable(String))", "0", nil, nil
	}
	existsSQL := field.existsSQL
	if existsSQL == "" {
		existsSQL = "1"
	}
	switch field.kind {
	case fieldKindString:
		return field.valueSQL,
			"(" + existsSQL + " AND isNotNull(" + field.valueSQL + ") AND isValidUTF8(" + field.valueSQL + "))",
			append([]any(nil), field.existsArgs...),
			nil
	case fieldKindDynamic:
		value := "dynamicElement(" + field.valueSQL + ", 'String')"
		typeSQL := field.dynamicTypeSQL
		if typeSQL == "" {
			typeSQL = "dynamicType(" + field.valueSQL + ")"
		}
		return value,
			"(" + existsSQL + " AND " + typeSQL + " = 'String' AND isNotNull(" + field.valueSQL + ") AND isValidUTF8(" + value + "))",
			append([]any(nil), field.existsArgs...),
			nil
	default:
		// v0.1 does not stringify numeric, Boolean, time, multivalue, or
		// container sources. They behave exactly like a non-match.
		return "CAST(NULL AS Nullable(String))", "0", nil, nil
	}
}

func compileRenameAssignment(assignment plan.RenameAssignment, state compileState) ([]string, compileState, bool, error) {
	if assignment.Source.Name == "" || assignment.Destination.Name == "" || assignment.Source.Name == assignment.Destination.Name {
		return nil, compileState{}, false, errors.New("compile ClickHouse rename: assignment is invalid")
	}
	openEventSchema := state.eventRows && state.allowDynamic
	if openEventSchema && (assignment.Source.Name == "fields" || assignment.Destination.Name == "fields") {
		return nil, compileState{}, false, &plan.Diagnostic{
			Code:    "SPL_AMBIGUOUS_RENAME_FIELD",
			Message: "rename cannot use the event result's reserved fields payload without an exact upstream schema",
			Range:   assignment.Range,
		}
	}
	if openEventSchema && ((!assignment.Source.Canonical && len(assignment.Source.Path) != 1) ||
		(!assignment.Destination.Canonical && len(assignment.Destination.Path) != 1)) {
		return nil, compileState{}, false, &plan.Diagnostic{
			Code:        "SPL_UNSUPPORTED_RENAME_PATH",
			Message:     "rename on an open event schema currently supports top-level exact fields only",
			Range:       assignment.Range,
			Suggestions: []string{"select an exact schema with table before renaming a dotted output field"},
		}
	}
	source, sourceExists, err := resolveCompiledField(assignment.Source, state)
	if err != nil {
		return nil, compileState{}, false, err
	}
	_, destinationExists, err := resolveCompiledField(assignment.Destination, state)
	if err != nil {
		return nil, compileState{}, false, err
	}
	if !sourceExists && !destinationExists {
		// With a closed schema, missing-to-missing is an exact no-op. An open
		// event schema resolves dynamic sources above and preserves per-row
		// missingness through the source existence expression.
		return nil, state, false, nil
	}
	if !sourceExists {
		source = fieldState{
			valueSQL:  "CAST(NULL AS Nullable(String))",
			existsSQL: "0",
			kind:      fieldKindString,
		}
	}

	next := cloneCompileState(state)
	delete(next.visible, assignment.Source.Name)
	delete(next.visible, assignment.Destination.Name)
	next.blocked[assignment.Source.Name] = struct{}{}
	next.blockedPrefixes[assignment.Source.Name] = struct{}{}
	next.blockedPrefixes[assignment.Destination.Name] = struct{}{}
	delete(next.blocked, assignment.Destination.Name)
	next.publicOrder = renamePublicOrder(
		state.publicOrder,
		assignment.Source.Name,
		assignment.Destination.Name,
		sourceExists && state.visible[assignment.Source.Name].valueSQL != "",
	)
	if exposesRawFieldsPayload(state) {
		// The public fields object is an Open Splunk convenience representation,
		// not a native SPL field. Publishing its immutable storage copy after a
		// rename would expose the old name and any overwritten destination. Drop
		// only that public convenience column; keep both private columns unchanged
		// so unrelated dynamic fields remain available to downstream SPL.
		next.publicOrder = slices.DeleteFunc(next.publicOrder, func(name string) bool { return name == "fields" })
	}

	destination := projectedRenameField(source, assignment.Destination.Name)
	next.visible[assignment.Destination.Name] = destination
	next.privateColumns = livePrivateColumns(next.privateColumns, next.visible)
	projection := renameProjection(state, next, assignment.Destination.Name, source)
	if len(projection) == 0 {
		return nil, compileState{}, false, errors.New("compile ClickHouse rename: projection has no fields")
	}
	return projection, next, true, nil
}

func cloneCompileState(state compileState) compileState {
	next := state
	next.visible = make(map[string]fieldState, len(state.visible)+1)
	for name, field := range state.visible {
		next.visible[name] = field
	}
	next.publicOrder = append([]string(nil), state.publicOrder...)
	next.privateColumns = append([]string(nil), state.privateColumns...)
	next.blocked = cloneSet(state.blocked)
	next.blockedPrefixes = cloneSet(state.blockedPrefixes)
	next.order = append([]compiledSortKey(nil), state.order...)
	next.tieBreakers = append([]compiledSortKey(nil), state.tieBreakers...)
	next.preAggregateValidationColumns = append([]string(nil), state.preAggregateValidationColumns...)
	next.preAggregateValidationArgs = append([]any(nil), state.preAggregateValidationArgs...)
	next.preAggregateColumns = append([]string(nil), state.preAggregateColumns...)
	next.preAggregateArgs = append([]any(nil), state.preAggregateArgs...)
	return next
}

func renamePublicOrder(current []string, source, destination string, sourceIsPublic bool) []string {
	result := make([]string, 0, len(current)+1)
	if sourceIsPublic && slices.Contains(current, source) {
		for _, name := range current {
			switch name {
			case source:
				result = append(result, destination)
			case destination:
			default:
				result = append(result, name)
			}
		}
		return result
	}
	result = append(result, current...)
	if !slices.Contains(result, destination) {
		result = append(result, destination)
	}
	return result
}

func projectedRenameField(source fieldState, destination string) fieldState {
	value := quoteIdentifier(destination)
	result := fieldState{
		valueSQL:       value,
		storedTypeSQL:  source.storedTypeSQL,
		existsSQL:      rewriteExistenceForProjection(source, destination),
		existsArgs:     append([]any(nil), source.existsArgs...),
		descendantSQL:  source.descendantSQL,
		descendantArgs: append([]any(nil), source.descendantArgs...),
		kind:           source.kind,
		// A field renamed to index is calculated pipeline data, not the
		// authorization-constrained physical index selector.
		caseSensitive: false,
		numberType:    source.numberType,
		numericSort:   source.numericSort,
	}
	if source.kind == fieldKindDynamic {
		result.dynamicTypeSQL = "dynamicType(" + value + ")"
	}
	return result
}

func exposesRawFieldsPayload(state compileState) bool {
	if !state.eventRows || !state.allowDynamic || !slices.Contains(state.publicOrder, "fields") {
		return false
	}
	_, explicitlyVisible := state.visible["fields"]
	return !explicitlyVisible
}

func renameProjection(state, next compileState, destination string, source fieldState) []string {
	names := orderedVisibleNames(next)
	projection := make([]string, 0, len(names)+6+len(state.order)+len(state.tieBreakers))
	for _, name := range names {
		field := state.visible[name]
		if name == destination {
			field = source
		}
		publicName := quoteIdentifier(name)
		if field.valueSQL == publicName {
			projection = append(projection, publicName)
		} else {
			projection = append(projection, field.valueSQL+" AS "+publicName)
		}
	}
	return appendPrivateEventProjection(projection, next)
}

func orderedVisibleNames(state compileState) []string {
	names := make([]string, 0, len(state.visible))
	seen := make(map[string]struct{}, len(state.visible))
	appendVisible := func(name string) {
		if _, duplicate := seen[name]; duplicate {
			return
		}
		if _, visible := state.visible[name]; !visible {
			return
		}
		seen[name] = struct{}{}
		names = append(names, name)
	}
	for _, name := range state.publicOrder {
		appendVisible(name)
	}
	for _, name := range canonicalColumnNames {
		appendVisible(name)
	}
	extra := make([]string, 0, len(state.visible)-len(names))
	for name := range state.visible {
		if _, included := seen[name]; !included {
			extra = append(extra, name)
		}
	}
	sort.Strings(extra)
	for _, name := range extra {
		appendVisible(name)
	}
	return names
}

func livePrivateColumns(columns []string, visible map[string]fieldState) []string {
	if len(columns) == 0 || len(visible) == 0 {
		return nil
	}
	live := make([]string, 0, len(columns))
	for _, column := range columns {
		for _, field := range visible {
			if field.existsSQL == column || field.storedTypeSQL == column {
				live = append(live, column)
				break
			}
		}
	}
	return live
}

// appendPrivateEventProjection keeps the immutable source document, its
// aligned leaf metadata, and deterministic ordering state available to later
// event-preserving operators. Explicit projections must never expose these
// columns publicly, but they also must not discard them before an event
// analysis finalizer consumes the relation.
func appendPrivateEventProjection(projection []string, state compileState) []string {
	privateColumns := make([]string, 0, 7+len(state.privateColumns)+len(state.order)+len(state.tieBreakers))
	if state.eventRows {
		privateColumns = append(privateColumns,
			quoteIdentifier(internalFieldsColumn),
			quoteIdentifier(internalFieldNamesColumn),
			quoteIdentifier(internalFieldTypesColumn),
			quoteIdentifier(internalFieldMetadataVersionColumn),
			quoteIdentifier(internalSortTimeColumn),
			quoteIdentifier(internalSortIDColumn),
		)
	}
	privateColumns = append(privateColumns, state.privateColumns...)
	if state.rexCapturedBytesSQL != "" {
		privateColumns = append(privateColumns, state.rexCapturedBytesSQL)
	}
	for _, key := range state.order {
		privateColumns = append(privateColumns, key.valueSQL)
	}
	for _, key := range state.tieBreakers {
		privateColumns = append(privateColumns, key.valueSQL)
	}
	for _, column := range privateColumns {
		if !slices.Contains(projection, column) {
			projection = append(projection, column)
		}
	}
	return projection
}

func compileEvalComparison(expression *plan.EvalComparisonExpression, state compileState) (string, []any, error) {
	left, err := compileComparisonScalar(expression.Left, state)
	if err != nil {
		return "", nil, err
	}
	right, err := compileComparisonScalar(expression.Right, state)
	if err != nil {
		return "", nil, err
	}
	operator, err := comparisonSQL(expression.Op)
	if err != nil {
		return "", nil, err
	}
	if expression.Op == plan.ComparisonOpNotEqual {
		operator = "!="
	}

	core, coreArgs := evalComparisonCore(left, right, operator)
	// Eval expressions use three-valued logic. Preserve null for a missing or
	// null operand so NOT(NULL) remains NULL and the final WHERE rejects it;
	// coercing the comparison to false here would make NOT missing=value match.
	predicate := "if((" + left.existsSQL + ") AND (" + right.existsSQL + "), " + core + ", CAST(NULL AS Nullable(Bool)))"
	args := make([]any, 0, len(left.existsArgs)+len(right.existsArgs)+len(coreArgs))
	args = append(args, left.existsArgs...)
	args = append(args, right.existsArgs...)
	args = append(args, coreArgs...)
	return predicate, args, nil
}

func compileComparisonScalar(expression plan.ScalarExpression, state compileState) (compiledScalar, error) {
	return compileScalarValue(expression, state)
}

func evalComparisonCore(left, right compiledScalar, operator string) (string, []any) {
	if comparisonOperatorIsOrdered(operator) && (left.kind == fieldKindBool || right.kind == fieldKindBool) {
		return "CAST(NULL AS Nullable(Bool))", nil
	}
	if left.kind == fieldKindDynamic || right.kind == fieldKindDynamic {
		return dynamicEvalComparisonCore(left, right, operator)
	}
	if !fixedScalarKindsComparable(left.kind, right.kind) {
		return "CAST(NULL AS Nullable(Bool))", nil
	}
	leftSQL := left.valueSQL
	rightSQL := right.valueSQL
	if scalarUsesNumericComparison(left, right) {
		integer := scalarIntegerComparison(left, right)
		leftSQL = numericScalarSQL(left, integer)
		rightSQL = numericScalarSQL(right, integer)
	} else if left.kind == fieldKindString || right.kind == fieldKindString {
		// Eval/where string comparisons are case-sensitive. This intentionally
		// differs from the search command's lowerUTF8 comparison behavior.
		leftSQL = stringScalarSQL(left)
		rightSQL = stringScalarSQL(right)
	}
	return leftSQL + " " + operator + " " + rightSQL, comparisonValueArgs(left, right)
}

func fixedScalarKindsComparable(left, right fieldKind) bool {
	if left == fieldKindInvalid || right == fieldKindInvalid {
		return false
	}
	// Bool participates only in Bool-v-Bool equality comparisons. ClickHouse otherwise
	// coerces Bool to 0/1, producing results that disagree with runtime-typed
	// Dynamic comparisons and SPL eval's type semantics.
	if (left == fieldKindBool) != (right == fieldKindBool) {
		return false
	}
	return left == right || left == fieldKindNumber || right == fieldKindNumber ||
		left == fieldKindString || right == fieldKindString
}

func dynamicEvalComparisonCore(left, right compiledScalar, operator string) (string, []any) {
	const nullBool = "CAST(NULL AS Nullable(Bool))"
	leftDynamic := left.kind == fieldKindDynamic
	rightDynamic := right.kind == fieldKindDynamic
	if leftDynamic && rightDynamic {
		leftType := dynamicScalarTypeSQL(left)
		rightType := dynamicScalarTypeSQL(right)
		integerCondition := "(" + dynamicIntegerTypePredicate(leftType) + " AND " + dynamicIntegerTypePredicate(rightType) + ")"
		numericCondition := "(" + dynamicNumericValuePredicate(left) + " AND " + dynamicNumericValuePredicate(right) + ")"
		stringCondition := "(" + leftType + " = 'String' AND " + rightType + " = 'String')"
		boolCondition := "(" + leftType + " = 'Bool' AND " + rightType + " = 'Bool')"
		boolComparison := nullBool
		argumentOccurrences := 3
		if !comparisonOperatorIsOrdered(operator) {
			boolComparison = dynamicBoolScalarSQL(left) + " " + operator + " " + dynamicBoolScalarSQL(right)
			argumentOccurrences++
		}
		result := "multiIf(" +
			integerCondition + ", " + scalarComparisonSQL(left, right, operator, true) + ", " +
			numericCondition + ", " + scalarComparisonSQL(left, right, operator, false) + ", " +
			stringCondition + ", " + dynamicStringScalarSQL(left) + " " + operator + " " + dynamicStringScalarSQL(right) + ", " +
			boolCondition + ", " + boolComparison + ", " +
			nullBool + ")"
		args := make([]any, 0, argumentOccurrences*(len(left.valueArgs)+len(right.valueArgs)))
		for range argumentOccurrences {
			args = append(args, comparisonValueArgs(left, right)...)
		}
		return result, args
	}

	dynamic := left
	fixed := right
	if rightDynamic {
		dynamic, fixed = right, left
	}
	typeSQL := dynamicScalarTypeSQL(dynamic)
	comparison := func(dynamicSQL, fixedSQL string) string {
		if leftDynamic {
			return dynamicSQL + " " + operator + " " + fixedSQL
		}
		return fixedSQL + " " + operator + " " + dynamicSQL
	}
	switch fixed.kind {
	case fieldKindNumber:
		if fixedNumberTypeIsInteger(fixed.numberType) {
			integer := comparison(numericScalarSQL(dynamic, true), numericScalarSQL(fixed, true))
			floating := comparison(numericScalarSQL(dynamic, false), numericScalarSQL(fixed, false))
			result := "multiIf(" + dynamicIntegerTypePredicate(typeSQL) + ", " + integer + ", " +
				dynamicNumericValuePredicate(dynamic) + ", " + floating + ", " + nullBool + ")"
			args := comparisonValueArgs(left, right)
			return result, append(args, comparisonValueArgs(left, right)...)
		}
		return "if(" + dynamicNumericValuePredicate(dynamic) + ", " +
			comparison(numericScalarSQL(dynamic, false), numericScalarSQL(fixed, false)) + ", " + nullBool + ")", comparisonValueArgs(left, right)
	case fieldKindTime:
		return "if(" + dynamicNumericValuePredicate(dynamic) + ", " +
			comparison(numericScalarSQL(dynamic, false), numericScalarSQL(fixed, false)) + ", " + nullBool + ")", comparisonValueArgs(left, right)
	case fieldKindString:
		return "if(" + typeSQL + " = 'String', " +
			comparison(dynamicStringScalarSQL(dynamic), stringScalarSQL(fixed)) + ", " + nullBool + ")", comparisonValueArgs(left, right)
	case fieldKindBool:
		return "if(" + typeSQL + " = 'Bool', " +
			comparison(dynamicBoolScalarSQL(dynamic), fixed.valueSQL) + ", " + nullBool + ")", comparisonValueArgs(left, right)
	default:
		return nullBool, nil
	}
}

func comparisonOperatorIsOrdered(operator string) bool {
	return operator != "=" && operator != "!="
}

func comparisonValueArgs(left, right compiledScalar) []any {
	args := make([]any, 0, len(left.valueArgs)+len(right.valueArgs))
	args = append(args, left.valueArgs...)
	return append(args, right.valueArgs...)
}

func scalarComparisonSQL(left, right compiledScalar, operator string, integer bool) string {
	return numericScalarSQL(left, integer) + " " + operator + " " + numericScalarSQL(right, integer)
}

func dynamicScalarTypeSQL(value compiledScalar) string {
	if value.dynamicTypeSQL != "" {
		return value.dynamicTypeSQL
	}
	return "dynamicType(" + value.valueSQL + ")"
}

func dynamicIntegerTypePredicate(typeSQL string) string {
	return typeSQL + " IN ('Int8', 'Int16', 'Int32', 'Int64', 'Int128', 'Int256', 'UInt8', 'UInt16', 'UInt32', 'UInt64', 'UInt128', 'UInt256')"
}

func dynamicNumericTypePredicate(typeSQL string) string {
	return "(" + dynamicIntegerTypePredicate(typeSQL) + " OR startsWith(" + typeSQL + ", 'Float') OR startsWith(" + typeSQL + ", 'Decimal'))"
}

func dynamicNumericValuePredicate(value compiledScalar) string {
	return "(" + dynamicNumericTypePredicate(dynamicScalarTypeSQL(value)) + " OR " + dynamicTaggedDecimalCondition(value) + ")"
}

func dynamicTaggedDecimalCondition(value compiledScalar) string {
	typeSQL := dynamicScalarTypeSQL(value)
	mapSQL := dynamicTaggedMapSQL(value)
	typeKey := "concat(char(0), 'open_splunk_type')"
	valueKey := "concat(char(0), 'open_splunk_value')"
	return "(" + typeSQL + " = 'Map(String, String)'" +
		" AND length(" + mapSQL + ") = 2" +
		" AND mapContains(" + mapSQL + ", " + typeKey + ")" +
		" AND mapContains(" + mapSQL + ", " + valueKey + ")" +
		" AND " + mapSQL + "[" + typeKey + "] = 'decimal/v1')"
}

func dynamicTaggedMapSQL(value compiledScalar) string {
	return "dynamicElement(" + value.valueSQL + ", 'Map(String, String)')"
}

func dynamicTaggedDecimalFloatSQL(value compiledScalar) string {
	valueKey := "concat(char(0), 'open_splunk_value')"
	return finiteFloatOrNullSQL(dynamicTaggedMapSQL(value) + "[" + valueKey + "]")
}

func dynamicStringScalarSQL(value compiledScalar) string {
	return "dynamicElement(" + value.valueSQL + ", 'String')"
}

func dynamicBoolScalarSQL(value compiledScalar) string {
	return "dynamicElement(" + value.valueSQL + ", 'Bool')"
}

func scalarUsesNumericComparison(left, right compiledScalar) bool {
	return left.kind == fieldKindNumber || right.kind == fieldKindNumber
}

func scalarIntegerComparison(left, right compiledScalar) bool {
	return fixedNumberTypeIsInteger(left.numberType) && fixedNumberTypeIsInteger(right.numberType)
}

func numericScalarSQL(value compiledScalar, integer bool) string {
	if integer {
		if fixedNumberTypeIsInteger(value.numberType) {
			if value.literal != nil {
				return "accurateCastOrNull(" + value.valueSQL + ", 'Int256')"
			}
			return "toInt256(" + value.valueSQL + ")"
		}
		return "accurateCastOrNull(toString(" + value.valueSQL + "), 'Int256')"
	}
	if value.kind == fieldKindTime {
		return "(toFloat64(toUnixTimestamp64Nano(" + value.valueSQL + ")) / 1000000000)"
	}
	if value.kind == fieldKindDynamic {
		return "if(" + dynamicTaggedDecimalCondition(value) + ", " + dynamicTaggedDecimalFloatSQL(value) +
			", toFloat64OrNull(toString(" + value.valueSQL + ")))"
	}
	if value.kind == fieldKindNumber {
		return "toFloat64(" + value.valueSQL + ")"
	}
	return "toFloat64OrNull(toString(" + value.valueSQL + "))"
}

func stringScalarSQL(value compiledScalar) string {
	if value.literal != nil && value.literal.Kind == plan.ValueKindString {
		return value.valueSQL
	}
	if value.kind == fieldKindDynamic {
		return dynamicStringScalarSQL(value)
	}
	return "toString(" + value.valueSQL + ")"
}

func compileComparison(expression *plan.ComparisonExpression, field fieldState) (string, []any, error) {
	exists := field.existsSQL
	args := append([]any(nil), field.existsArgs...)
	if expression.Value.Kind == plan.ValueKindNull {
		equal := "(" + exists + " AND isNull(" + field.valueSQL + "))"
		if expression.Op == plan.ComparisonOpEqual {
			return equal, args, nil
		}
		if expression.Op == plan.ComparisonOpNotEqual {
			return "(" + exists + " AND NOT isNull(" + field.valueSQL + "))", args, nil
		}
		return "", nil, errors.New("compile ClickHouse predicate: null only supports = and !=")
	}

	text := comparisonSourceText(expression.Value)
	if expression.Value.Kind == plan.ValueKindString && text == "*" &&
		(expression.Op == plan.ComparisonOpEqual || expression.Op == plan.ComparisonOpNotEqual) {
		if expression.Op == plan.ComparisonOpNotEqual {
			// SPL field!=* excludes missing fields and every present value,
			// including explicit null, so it cannot match an event.
			return "0", nil, nil
		}
		if field.kind == fieldKindDynamic {
			presence := "((" + exists + ") AND isNotNull(" + field.valueSQL + "))"
			if field.descendantSQL != "" {
				presence = "(" + presence + " OR (" + field.descendantSQL + "))"
				args = append(args, field.descendantArgs...)
			}
			return presence, args, nil
		}
		return "(" + exists + " AND isNotNull(" + field.valueSQL + "))", args, nil
	}

	operator, err := comparisonSQL(expression.Op)
	if err != nil {
		return "", nil, err
	}
	var predicate string
	argumentOccurrences := 1
	if expression.Op == plan.ComparisonOpEqual || expression.Op == plan.ComparisonOpNotEqual {
		predicate, argumentOccurrences = equalityPredicate(expression, field, text)
	} else {
		predicate, argumentOccurrences, err = relationalPredicate(expression, field, operator)
		if err != nil {
			return "", nil, err
		}
	}
	argument := any(text)
	if expression.Value.Kind == plan.ValueKindString && strings.Contains(text, "*") &&
		(expression.Op == plan.ComparisonOpEqual || expression.Op == plan.ComparisonOpNotEqual) {
		argument = wildcardRegex(text, true)
	}
	for range argumentOccurrences {
		args = append(args, argument)
	}
	if expression.Op == plan.ComparisonOpNotEqual {
		// SPL field!=value excludes missing fields while treating a present null
		// as unequal to a non-null value. ifNull collapses SQL's UNKNOWN here.
		return "(" + exists + " AND NOT ifNull(" + predicate + ", 0))", args, nil
	}
	return "(" + exists + " AND ifNull(" + predicate + ", 0))", args, nil
}

func equalityPredicate(expression *plan.ComparisonExpression, field fieldState, text string) (string, int) {
	valueSQL := field.valueSQL
	if expression.Value.Kind == plan.ValueKindString && strings.Contains(text, "*") {
		return "match(toString(" + valueSQL + "), ?)", 1
	}
	if field.caseSensitive {
		return valueSQL + " = ?", 1
	}
	if field.kind == fieldKindTime {
		if expression.Value.Kind == plan.ValueKindString {
			return valueSQL + " = parseDateTime64BestEffortOrNull(?, 9, 'UTC')", 1
		}
		return "(toFloat64(toUnixTimestamp64Nano(" + valueSQL + ")) / 1000000000) = toFloat64OrNull(?)", 1
	}
	if left, right, ok := fixedNumberComparisonOperands(field, expression.Value.Kind); ok {
		return left + " = " + right, 1
	}
	base := "lowerUTF8(toString(" + valueSQL + ")) = lowerUTF8(?)"
	if field.kind != fieldKindDynamic {
		return base, 1
	}
	dynamic := compiledScalarFromField(field)
	typeSQL := dynamicScalarTypeSQL(dynamic)
	guard := dynamicLiteralGuard(typeSQL, expression.Value.Kind)
	if expression.Value.Kind == plan.ValueKindInt64 || expression.Value.Kind == plan.ValueKindUint64 {
		exact := "accurateCastOrNull(toString(" + valueSQL + "), 'Int256') = accurateCastOrNull(?, 'Int256')"
		decimal := dynamicTaggedDecimalFloatSQL(dynamic) + " = toFloat64OrNull(?)"
		return "multiIf(" + dynamicIntegerTypePredicate(typeSQL) + ", " + exact + ", " +
			dynamicTaggedDecimalCondition(dynamic) + ", " + decimal + ", 0)", 2
	} else if expression.Value.Kind == plan.ValueKindFloat64 {
		guard = "(" + guard + " OR " + dynamicTaggedDecimalCondition(dynamic) + ")"
		base = numericScalarSQL(dynamic, false) + " = toFloat64OrNull(?)"
	}
	return "(" + guard + " AND " + base + ")", 1
}

func relationalPredicate(expression *plan.ComparisonExpression, field fieldState, operator string) (string, int, error) {
	if expression.Value.Kind == plan.ValueKindBool {
		return "", 0, errors.New("compile ClickHouse predicate: booleans do not support ordered comparison")
	}
	if field.kind == fieldKindTime {
		if expression.Value.Kind == plan.ValueKindString {
			return field.valueSQL + " " + operator + " parseDateTime64BestEffortOrNull(?, 9, 'UTC')", 1, nil
		}
		return "(toFloat64(toUnixTimestamp64Nano(" + field.valueSQL + ")) / 1000000000) " + operator + " toFloat64OrNull(?)", 1, nil
	}
	if expression.Value.Kind == plan.ValueKindString {
		switch field.kind {
		case fieldKindString:
			if field.caseSensitive {
				return field.valueSQL + " " + operator + " ?", 1, nil
			}
			return "lowerUTF8(toString(" + field.valueSQL + ")) " + operator + " lowerUTF8(?)", 1, nil
		case fieldKindDynamic:
			typeSQL := dynamicTypeExpression(field)
			valueSQL := "dynamicElement(" + field.valueSQL + ", 'String')"
			comparison := "lowerUTF8(" + valueSQL + ") " + operator + " lowerUTF8(?)"
			return "(" + typeSQL + " = 'String' AND " + comparison + ")", 1, nil
		}
	}
	if left, right, ok := fixedNumberComparisonOperands(field, expression.Value.Kind); ok {
		return left + " " + operator + " " + right, 1, nil
	}
	if field.kind == fieldKindDynamic &&
		(expression.Value.Kind == plan.ValueKindInt64 || expression.Value.Kind == plan.ValueKindUint64) {
		typeSQL := dynamicTypeExpression(field)
		exact := "accurateCastOrNull(toString(" + field.valueSQL + "), 'Int256') " + operator + " accurateCastOrNull(?, 'Int256')"
		fallback := numericScalarSQL(compiledScalarFromField(field), false) + " " + operator + " toFloat64OrNull(?)"
		return "multiIf(" + dynamicIntegerTypePredicate(typeSQL) + ", " + exact + ", " + fallback + ")", 2, nil
	}
	if field.kind == fieldKindDynamic {
		return numericScalarSQL(compiledScalarFromField(field), false) + " " + operator + " toFloat64OrNull(?)", 1, nil
	}
	return "toFloat64OrNull(toString(" + field.valueSQL + ")) " + operator + " toFloat64OrNull(?)", 1, nil
}

func compiledScalarFromField(field fieldState) compiledScalar {
	return compiledScalar{
		valueSQL:       field.valueSQL,
		existsSQL:      field.existsSQL,
		existsArgs:     append([]any(nil), field.existsArgs...),
		dynamicTypeSQL: field.dynamicTypeSQL,
		storedTypeSQL:  field.storedTypeSQL,
		descendantSQL:  field.descendantSQL,
		descendantArgs: append([]any(nil), field.descendantArgs...),
		kind:           field.kind,
		numberType:     field.numberType,
	}
}

func fixedNumberComparisonOperands(field fieldState, literalKind plan.ValueKind) (left, right string, ok bool) {
	if field.numberType == "" {
		return "", "", false
	}
	if literalKind != plan.ValueKindInt64 && literalKind != plan.ValueKindUint64 && literalKind != plan.ValueKindFloat64 {
		return "", "", false
	}
	if literalKind != plan.ValueKindFloat64 && fixedNumberTypeIsInteger(field.numberType) {
		return "toInt256(" + field.valueSQL + ")", "accurateCastOrNull(?, 'Int256')", true
	}
	return "toFloat64(" + field.valueSQL + ")", "toFloat64OrNull(?)", true
}

func fixedNumberTypeIsInteger(numberType string) bool {
	return strings.HasPrefix(numberType, "Int") || strings.HasPrefix(numberType, "UInt")
}

func dynamicLiteralGuard(typeSQL string, kind plan.ValueKind) string {
	switch kind {
	case plan.ValueKindInt64, plan.ValueKindUint64:
		return typeSQL + " IN ('Int8', 'Int16', 'Int32', 'Int64', 'Int128', 'Int256', 'UInt8', 'UInt16', 'UInt32', 'UInt64', 'UInt128', 'UInt256')"
	case plan.ValueKindFloat64:
		return "(startsWith(" + typeSQL + ", 'Float') OR startsWith(" + typeSQL + ", 'Decimal'))"
	case plan.ValueKindBool:
		return typeSQL + " = 'Bool'"
	case plan.ValueKindString:
		return typeSQL + " = 'String'"
	default:
		return "0"
	}
}

func dynamicTypeExpression(field fieldState) string {
	if field.dynamicTypeSQL != "" {
		return field.dynamicTypeSQL
	}
	return "dynamicType(" + field.valueSQL + ")"
}

func comparisonSourceText(value plan.Value) string {
	if value.SourceText != "" {
		return value.SourceText
	}
	switch value.Kind {
	case plan.ValueKindString:
		return value.String
	case plan.ValueKindInt64:
		return fmt.Sprintf("%d", value.Int64)
	case plan.ValueKindUint64:
		return fmt.Sprintf("%d", value.Uint64)
	case plan.ValueKindFloat64:
		return fmt.Sprintf("%g", value.Float64)
	case plan.ValueKindBool:
		if value.Bool {
			return "true"
		}
		return "false"
	case plan.ValueKindNull:
		return "null"
	default:
		return ""
	}
}

func comparisonSQL(operator plan.ComparisonOp) (string, error) {
	switch operator {
	case plan.ComparisonOpEqual, plan.ComparisonOpNotEqual:
		return "=", nil
	case plan.ComparisonOpLess:
		return "<", nil
	case plan.ComparisonOpLessEqual:
		return "<=", nil
	case plan.ComparisonOpGreater:
		return ">", nil
	case plan.ComparisonOpGreaterEqual:
		return ">=", nil
	default:
		return "", errors.New("compile ClickHouse predicate: invalid comparison operator")
	}
}

func resolveCompiledField(field plan.FieldRef, state compileState) (fieldState, bool, error) {
	if existing, ok := state.visible[field.Name]; ok {
		return existing, true, nil
	}
	if _, blocked := state.blocked[field.Name]; blocked || field.Canonical || !state.allowDynamic {
		return fieldState{}, false, nil
	}
	for prefix := range state.blockedPrefixes {
		if field.Name == prefix || strings.HasPrefix(field.Name, prefix+".") {
			return fieldState{}, false, nil
		}
	}
	if len(field.Path) == 0 {
		return fieldState{}, false, fmt.Errorf("compile ClickHouse field %q: dynamic path is empty", field.Name)
	}
	value := quoteIdentifier(internalFieldsColumn)
	for _, segment := range field.Path {
		if segment == "" {
			return fieldState{}, false, fmt.Errorf("compile ClickHouse field %q: dynamic path has empty segment", field.Name)
		}
		value += "." + quoteIdentifier(encodePhysicalPathSegment(segment))
	}
	return fieldState{
		valueSQL:       value,
		dynamicTypeSQL: "dynamicType(" + value + ")",
		existsSQL:      "has(" + quoteIdentifier(internalFieldNamesColumn) + ", ?)",
		existsArgs:     []any{normalizedDynamicPath(field.Path)},
		descendantSQL: "arrayExists(name -> startsWith(name, ?), " +
			quoteIdentifier(internalFieldNamesColumn) + ")",
		descendantArgs: []any{normalizedDynamicPath(field.Path) + "."},
		kind:           fieldKindDynamic,
	}, true, nil
}

func compileProjection(operator *plan.Project, state compileState) ([]string, compileState, []any, error) {
	next := compileState{
		visible:             make(map[string]fieldState),
		privateColumns:      append([]string(nil), state.privateColumns...),
		rexCapturedBytesSQL: state.rexCapturedBytesSQL,
		allowDynamic:        operator.Mode == plan.ProjectModeExclude && state.allowDynamic,
		eventRows:           state.eventRows,
		blocked:             cloneSet(state.blocked),
		blockedPrefixes:     cloneSet(state.blockedPrefixes),
		order:               append([]compiledSortKey(nil), state.order...),
		tieBreakers:         append([]compiledSortKey(nil), state.tieBreakers...),
	}
	var names []string
	switch operator.Mode {
	case plan.ProjectModeInclude, plan.ProjectModeTable:
		for _, field := range operator.Fields {
			names = append(names, field.Name)
		}
		if operator.Mode == plan.ProjectModeInclude {
			for _, implicit := range []string{"_time", "_raw"} {
				if _, exists := state.visible[implicit]; exists && !slices.Contains(names, implicit) {
					names = append(names, implicit)
				}
			}
		}
	case plan.ProjectModeExclude:
		excluded := make(map[string]struct{}, len(operator.Fields))
		for _, field := range operator.Fields {
			excluded[field.Name] = struct{}{}
			next.blocked[field.Name] = struct{}{}
		}
		for _, name := range state.publicOrder {
			if name == "fields" && state.eventRows {
				if _, visible := state.visible[name]; !visible {
					continue // avoid leaking excluded dynamic members in the public object
				}
			}
			if _, remove := excluded[name]; !remove {
				names = append(names, name)
			}
		}
	default:
		return nil, compileState{}, nil, errors.New("compile ClickHouse projection: invalid mode")
	}

	projection := make([]string, 0, len(names)+6)
	args := make([]any, 0)
	for _, name := range names {
		var ref plan.FieldRef
		for _, candidate := range operator.Fields {
			if candidate.Name == name {
				ref = candidate
				break
			}
		}
		if ref.Name == "" {
			ref = plan.FieldRef{Name: name, Canonical: true}
		}
		compiled, ok, err := resolveCompiledField(ref, state)
		if err != nil {
			return nil, compileState{}, nil, err
		}
		if !ok {
			if operator.Mode != plan.ProjectModeTable {
				continue
			}
			// table declares an exact output schema. Preserve requested fields
			// that a prior transforming stage removed as nullable missing columns
			// instead of silently changing the result shape.
			compiled = fieldState{
				valueSQL:  "CAST(NULL AS Nullable(String))",
				existsSQL: "0",
				kind:      fieldKindString,
			}
		}
		publicName := quoteIdentifier(name)
		if compiled.valueSQL == publicName {
			projection = append(projection, publicName)
		} else {
			projection = append(projection, compiled.valueSQL+" AS "+publicName)
		}
		next.visible[name] = fieldState{
			valueSQL: publicName, dynamicTypeSQL: compiled.dynamicTypeSQL,
			storedTypeSQL:  compiled.storedTypeSQL,
			existsSQL:      rewriteExistenceForProjection(compiled, name),
			existsArgs:     append([]any(nil), compiled.existsArgs...),
			descendantSQL:  compiled.descendantSQL,
			descendantArgs: append([]any(nil), compiled.descendantArgs...),
			kind:           compiled.kind,
			caseSensitive:  compiled.caseSensitive,
			numberType:     compiled.numberType,
			numericSort:    compiled.numericSort,
			canonicalTime:  compiled.canonicalTime,
		}
		next.publicOrder = append(next.publicOrder, name)
	}
	next.privateColumns = livePrivateColumns(next.privateColumns, next.visible)
	return appendPrivateEventProjection(projection, next), next, args, nil
}

func rewriteExistenceForProjection(field fieldState, name string) string {
	if field.existsSQL == "1" {
		return "1"
	}
	if strings.HasPrefix(field.existsSQL, "isNotNull(") {
		return "isNotNull(" + quoteIdentifier(name) + ")"
	}
	return field.existsSQL
}

func compileAggregate(operator *plan.Aggregate, state compileState) (
	projection []string,
	predicates []string,
	groups []string,
	next compileState,
	args []any,
	err error,
) {
	if operator == nil || len(operator.Measures) == 0 {
		return nil, nil, nil, compileState{}, nil, errors.New("compile ClickHouse aggregate: no measures")
	}
	next = compileState{
		visible:      make(map[string]fieldState, len(operator.GroupBy)+len(operator.Measures)),
		allowDynamic: false,
		eventRows:    false,
		blocked:      make(map[string]struct{}),
	}
	seen := make(map[string]struct{}, len(operator.GroupBy)+len(operator.Measures))
	dynamicGroupInvalid := make([]string, 0, len(operator.GroupBy))
	var dynamicGroupInvalidArgs []any
	for _, group := range operator.GroupBy {
		if _, duplicate := seen[group.Name]; duplicate {
			return nil, nil, nil, compileState{}, nil, fmt.Errorf("compile ClickHouse aggregate: output field %q is duplicated", group.Name)
		}
		seen[group.Name] = struct{}{}
		field, ok, resolveErr := resolveCompiledField(group, state)
		if resolveErr != nil {
			return nil, nil, nil, compileState{}, nil, resolveErr
		}
		if !ok {
			// A transforming command retains its declared output schema even when
			// an upstream projection removed the grouping field. SPL emits no
			// groups in that case; use a typed NULL plus an always-false predicate
			// rather than resurrecting the private source document or surfacing an
			// internal compiler error.
			field = fieldState{
				valueSQL:  "CAST(NULL AS Nullable(String))",
				existsSQL: "0",
				kind:      fieldKindString,
			}
		}
		valueSQL := field.valueSQL
		kind := field.kind
		numericSort := field.numericSort
		supportedSQL := ""
		if kind == fieldKindDynamic {
			// SPL fields are compared and grouped by their lexical value. Dynamic
			// scalar storage types therefore intentionally converge on the same
			// UTF-8 group key (for example integer 500 and string "500").
			// Missing and explicit-null values are removed below.
			supported, lexical := statsByScalarExpressions(field)
			// Unsupported containers use one private placeholder group. A scoped
			// whole-input window below fails the search before any key is exposed.
			valueAlias := quoteIdentifier(fmt.Sprintf("__os_group_value_%d", len(groups)))
			next.preAggregateColumns = append(next.preAggregateColumns,
				"if("+supported+", "+lexical+", '') AS "+valueAlias,
			)
			valueSQL = valueAlias
			kind = fieldKindString
			numericSort = true
			supportedSQL = supported
		}
		groupOutput := fmt.Sprintf("__os_group_%d", len(groups))
		projection = append(projection, valueSQL+" AS "+quoteIdentifier(groupOutput))
		presence := "(" + field.existsSQL + " AND isNotNull(" + field.valueSQL + "))"
		presenceArgs := append([]any(nil), field.existsArgs...)
		if field.kind == fieldKindDynamic && field.descendantSQL != "" {
			// Non-empty objects are stored as flattened leaf paths, so the parent
			// itself is absent from field_names. Retain those rows until the scoped
			// aggregate support check can reject the container explicitly.
			presence = "(" + presence + " OR " + field.descendantSQL + ")"
			presenceArgs = append(presenceArgs, field.descendantArgs...)
		}
		if supportedSQL != "" {
			// Validate each key against its own presence rather than the combined
			// group eligibility predicate. A container must fail the whole scoped
			// search even when another BY key is missing on the same row.
			dynamicGroupInvalid = append(dynamicGroupInvalid, "("+presence+") AND NOT ("+supportedSQL+")")
			dynamicGroupInvalidArgs = append(dynamicGroupInvalidArgs, presenceArgs...)
		}
		predicates = append(predicates, presence)
		args = append(args, presenceArgs...)
		groups = append(groups, valueSQL)
		privateGroup := quoteIdentifier(groupOutput)
		next.visible[group.Name] = fieldState{
			valueSQL: privateGroup, existsSQL: "1", kind: kind,
			caseSensitive: field.caseSensitive, numberType: field.numberType,
			numericSort: numericSort,
		}
		next.publicOrder = append(next.publicOrder, group.Name)
		next.order = append(next.order, compiledSortKey{valueSQL: privateGroup})
		next.tieBreakers = append(next.tieBreakers, compiledSortKey{valueSQL: privateGroup})
	}
	numericInputs := make(map[string]string)
	for _, measure := range operator.Measures {
		if _, duplicate := seen[measure.Output]; duplicate {
			return nil, nil, nil, compileState{}, nil, fmt.Errorf("compile ClickHouse aggregate: output field %q is duplicated", measure.Output)
		}
		seen[measure.Output] = struct{}{}
		output := quoteIdentifier(measure.Output)
		measureState := fieldState{valueSQL: output, existsSQL: "1", kind: fieldKindNumber}
		switch measure.Function {
		case plan.AggregateFunctionCountRows:
			projection = append(projection, "count() AS "+output)
			measureState.numberType = "UInt64"
		case plan.AggregateFunctionPercentile:
			if measure.Percentile != 0.95 {
				return nil, nil, nil, compileState{}, nil, fmt.Errorf("compile ClickHouse aggregate: unsupported percentile %g", measure.Percentile)
			}
			input, ok, resolveErr := resolveCompiledField(measure.Input, state)
			if resolveErr != nil {
				return nil, nil, nil, compileState{}, nil, resolveErr
			}
			inputSQL := "CAST(NULL AS Nullable(Float64))"
			if ok {
				inputSQL = percentileInputSQL(input)
			}
			projection = append(projection, "quantileGKOrNull(100, 0.95)("+inputSQL+") AS "+output)
			measureState.numberType = "Float64"
		case plan.AggregateFunctionSum, plan.AggregateFunctionAverage:
			inputAlias, cached := numericInputs[measure.Input.Name]
			if !cached {
				input, ok, resolveErr := resolveCompiledField(measure.Input, state)
				if resolveErr != nil {
					return nil, nil, nil, compileState{}, nil, resolveErr
				}
				inputSQL := "CAST([], 'Array(Float64)')"
				var inputArgs []any
				if ok {
					inputSQL, inputArgs = numericArrayInputSQL(input)
				}
				inputAlias = quoteIdentifier(fmt.Sprintf("__os_measure_values_%d", len(numericInputs)))
				numericInputs[measure.Input.Name] = inputAlias
				next.preAggregateColumns = append(next.preAggregateColumns, inputSQL+" AS "+inputAlias)
				next.preAggregateArgs = append(next.preAggregateArgs, inputArgs...)
			}
			countSQL := "sum(length(" + inputAlias + "))"
			sumSQL := "sum(arraySum(" + inputAlias + "))"
			nullFloat := "CAST(NULL AS Nullable(Float64))"
			valueSQL := "if(" + countSQL + " = 0, " + nullFloat + ", toFloat64(" + sumSQL + "))"
			if measure.Function == plan.AggregateFunctionAverage {
				valueSQL = "if(" + countSQL + " = 0, " + nullFloat + ", toFloat64(" + sumSQL + ") / toFloat64(" + countSQL + "))"
			}
			projection = append(projection, valueSQL+" AS "+output)
			measureState.numberType = "Float64"
		default:
			return nil, nil, nil, compileState{}, nil, fmt.Errorf("compile ClickHouse aggregate: unsupported function %d", measure.Function)
		}
		next.visible[measure.Output] = measureState
		next.publicOrder = append(next.publicOrder, measure.Output)
		if len(next.order) == 0 {
			next.order = append(next.order, compiledSortKey{valueSQL: quoteIdentifier(measure.Output)})
		}
	}
	if len(dynamicGroupInvalid) > 0 {
		anyUnsupportedColumn := quoteIdentifier("__os_stats_by_any_unsupported")
		invalid := "(" + strings.Join(dynamicGroupInvalid, ") OR (") + ")"
		next.preAggregateValidationColumns = append(next.preAggregateValidationColumns,
			"max(CAST("+invalid+" AS UInt8)) OVER () AS "+anyUnsupportedColumn,
		)
		next.preAggregateValidationArgs = append(next.preAggregateValidationArgs, dynamicGroupInvalidArgs...)
		eligible := "(" + strings.Join(predicates, " AND ") + ")"
		predicates = []string{
			"if(" + anyUnsupportedColumn + " != 0, throwIf(toUInt8(1), '" + UnsupportedStatsByValueMarker + "') = 0, " + eligible + ")",
		}
	}
	return projection, predicates, groups, next, args, nil
}

func percentileInputSQL(field fieldState) string {
	nullFloat := "CAST(NULL AS Nullable(Float64))"
	switch field.kind {
	case fieldKindNumber:
		return "ifNotFinite(toFloat64(" + field.valueSQL + "), " + nullFloat + ")"
	case fieldKindTime:
		return "ifNotFinite(toFloat64(toUnixTimestamp64Nano(" + field.valueSQL + ")) / 1000000000, " + nullFloat + ")"
	case fieldKindDynamic:
		return dynamicFiniteFloatOrNullSQL(field.valueSQL, dynamicTypeExpression(field))
	case fieldKindString:
		return finiteFloatOrNullSQL(field.valueSQL)
	default:
		return nullFloat
	}
}

func numericArrayInputSQL(field fieldState) (string, []any) {
	empty := "CAST([], 'Array(Float64)')"
	scalar := percentileInputSQL(field)
	scalarArray := "arrayMap(value -> assumeNotNull(value), arrayFilter(value -> isNotNull(value), [" + scalar + "]))"
	value := scalarArray
	if field.kind == fieldKindDynamic {
		element := dynamicFiniteFloatOrNullSQL("element", "dynamicType(element)")
		array := "arrayMap(value -> assumeNotNull(value), arrayFilter(value -> isNotNull(value), " +
			"arrayMap(element -> " + element + ", dynamicElement(" + field.valueSQL + ", 'Array(Dynamic)'))))"
		value = "if(" + dynamicTypeExpression(field) + " = 'Array(Dynamic)', " + array + ", " + scalarArray + ")"
	}
	if field.existsSQL == "" || field.existsSQL == "1" {
		return value, nil
	}
	return "if(" + field.existsSQL + ", " + value + ", " + empty + ")", append([]any(nil), field.existsArgs...)
}

func dynamicFiniteFloatOrNullSQL(valueSQL, typeSQL string) string {
	value := compiledScalar{valueSQL: valueSQL, dynamicTypeSQL: typeSQL, kind: fieldKindDynamic}
	numericOrString := "(" + typeSQL + " = 'String' OR " + dynamicNumericTypePredicate(typeSQL) + ")"
	converted := finiteFloatOrNullSQL("toString(" + valueSQL + ")")
	decimalTag := dynamicTaggedDecimalCondition(value)
	decimal := dynamicTaggedDecimalFloatSQL(value)
	return "multiIf(" + numericOrString + ", " + converted + ", " + decimalTag + ", " + decimal +
		", CAST(NULL AS Nullable(Float64)))"
}

func finiteFloatOrNullSQL(valueSQL string) string {
	return "ifNotFinite(toFloat64OrNull(" + valueSQL + "), CAST(NULL AS Nullable(Float64)))"
}

func statsByScalarExpressions(field fieldState) (supported, lexical string) {
	typeSQL := dynamicTypeExpression(field)
	mapSQL := "dynamicElement(" + field.valueSQL + ", 'Map(String, String)')"
	typeKey := "concat(char(0), 'open_splunk_type')"
	valueKey := "concat(char(0), 'open_splunk_value')"
	extended := "(" + typeSQL + " = 'Map(String, String)'" +
		" AND length(" + mapSQL + ") = 2" +
		" AND mapContains(" + mapSQL + ", " + typeKey + ")" +
		" AND mapContains(" + mapSQL + ", " + valueKey + ")" +
		" AND " + mapSQL + "[" + typeKey + "] IN ('bytes/v1', 'timestamp/v1', 'duration/v1', 'decimal/v1'))"
	// None is excluded deliberately. Missing and explicit-null leaves are
	// removed before aggregation, while a flattened object parent reads as None
	// at its literal path and must set the unsupported-container flag.
	supported = "(" + typeSQL + " IN ('String', 'Int64', 'UInt64', 'Float64', 'Bool') OR " + extended + ")"
	lexical = "if(" + typeSQL + " = 'Map(String, String)', " + mapSQL + "[" + valueKey + "], toString(" + field.valueSQL + "))"
	return supported, lexical
}

func compileDeduplicate(
	fragment string,
	operator *plan.Deduplicate,
	state compileState,
	stage int,
) (deduplicated string, prefixArgs []any, currentOrder []compiledSortKey, additionalAliases int, err error) {
	if operator == nil || operator.Count == 0 || len(operator.Keys) == 0 || len(operator.Keys) > 16 {
		return "", nil, nil, 0, errors.New("compile ClickHouse deduplicate: positive count and 1 through 16 keys are required")
	}

	materialized := make([]string, 0, len(operator.Keys)*3)
	presentColumns := make([]string, 0, len(operator.Keys))
	keyColumns := make([]string, 0, len(operator.Keys))
	invalidValues := make([]string, 0, len(operator.Keys))
	helperColumns := make([]string, 0, len(operator.Keys)*3+1)
	seen := make(map[string]struct{}, len(operator.Keys))
	for index, key := range operator.Keys {
		if state.eventRows && state.allowDynamic && key.Name == "fields" {
			return "", nil, nil, 0, &plan.Diagnostic{
				Code:    "SPL_AMBIGUOUS_DEDUP_FIELD",
				Message: "dedup cannot use the event result's reserved fields payload without an exact upstream schema",
				Range:   key.Range,
			}
		}
		if _, duplicate := seen[key.Name]; duplicate {
			return "", nil, nil, 0, fmt.Errorf("compile ClickHouse deduplicate: key %q is duplicated", key.Name)
		}
		seen[key.Name] = struct{}{}

		field, exists, resolveErr := resolveCompiledField(key, state)
		if resolveErr != nil {
			return "", nil, nil, 0, resolveErr
		}
		if !exists {
			// A prior projection is authoritative. Keep a typed missing key so the
			// eligibility predicate removes every row without consulting the
			// private event document.
			field = fieldState{
				valueSQL:  "CAST(NULL AS Nullable(String))",
				existsSQL: "0",
				kind:      fieldKindString,
			}
		}

		presentName := quoteIdentifier(fmt.Sprintf("__os_dedup_present_%d_%d", stage, index))
		keyName := quoteIdentifier(fmt.Sprintf("__os_dedup_key_%d_%d", stage, index))
		present := "0"
		if exists {
			fieldExists := field.existsSQL
			if fieldExists == "" {
				fieldExists = "1"
			}
			present = "(" + fieldExists + " AND isNotNull(" + field.valueSQL + "))"
			prefixArgs = append(prefixArgs, field.existsArgs...)
			if field.kind == fieldKindDynamic && field.descendantSQL != "" {
				// Flattened non-empty objects do not have an exact parent leaf. Keep
				// them present until the whole-input unsupported-value guard runs.
				present = "(" + present + " OR " + field.descendantSQL + ")"
				prefixArgs = append(prefixArgs, field.descendantArgs...)
			}
		}
		materialized = append(materialized, "toUInt8("+present+") AS "+presentName)
		presentColumns = append(presentColumns, presentName)
		helperColumns = append(helperColumns, presentName)

		keyValue := field.valueSQL
		if field.kind == fieldKindDynamic {
			supported, lexical := statsByScalarExpressions(field)
			supportedName := quoteIdentifier(fmt.Sprintf("__os_dedup_supported_%d_%d", stage, index))
			materialized = append(materialized,
				"toUInt8("+supported+") AS "+supportedName,
				"if("+supported+", "+lexical+", '') AS "+keyName,
			)
			helperColumns = append(helperColumns, supportedName)
			invalidValues = append(invalidValues, presentName+" != 0 AND "+supportedName+" = 0")
		} else {
			materialized = append(materialized, keyValue+" AS "+keyName)
		}
		keyColumns = append(keyColumns, keyName)
		helperColumns = append(helperColumns, keyName)
	}

	preparedAlias := quoteIdentifier(fmt.Sprintf("_stage_%d", stage))
	deduplicated = "SELECT *, " + strings.Join(materialized, ", ") + " FROM (" + fragment + ") AS " + preparedAlias
	eligible := make([]string, 0, len(presentColumns))
	for _, present := range presentColumns {
		eligible = append(eligible, present+" != 0")
	}
	predicate := "(" + strings.Join(eligible, " AND ") + ")"

	if len(invalidValues) > 0 {
		additionalAliases++
		validationAlias := quoteIdentifier(fmt.Sprintf("_stage_%d", stage+additionalAliases))
		anyUnsupported := quoteIdentifier(fmt.Sprintf("__os_dedup_any_unsupported_%d", stage))
		helperColumns = append(helperColumns, anyUnsupported)
		invalid := "(" + strings.Join(invalidValues, ") OR (") + ")"
		deduplicated = "SELECT *, max(CAST(" + invalid + " AS UInt8)) OVER () AS " + anyUnsupported +
			" FROM (" + deduplicated + ") AS " + validationAlias
		// Put validation and eligibility in the two branches of one predicate.
		// The window flag is computed over the complete scoped input first, so an
		// unsupported key cannot be hidden by a missing value in another key.
		predicate = "if(" + anyUnsupported + " != 0, throwIf(toUInt8(1), '" + UnsupportedDedupValueMarker + "') = 0, " + predicate + ")"
	}

	currentOrder = defaultCompiledOrder(state)
	if len(currentOrder) == 0 {
		return "", nil, nil, 0, errors.New("compile ClickHouse deduplicate: input has no deterministic order")
	}
	order, orderErr := compileMaterializedOrder(currentOrder, false)
	if orderErr != nil {
		return "", nil, nil, 0, orderErr
	}
	additionalAliases++
	dedupAlias := quoteIdentifier(fmt.Sprintf("_stage_%d", stage+additionalAliases))
	deduplicated = "SELECT * EXCEPT (" + strings.Join(helperColumns, ", ") + ") FROM (" + deduplicated + ") AS " + dedupAlias + " WHERE " + predicate +
		" ORDER BY " + order + " LIMIT ? BY " + strings.Join(keyColumns, ", ")
	return deduplicated, prefixArgs, currentOrder, additionalAliases, nil
}

func compileWindow(operator *plan.Window, state compileState) (string, compileState, error) {
	if operator == nil || operator.Output == "" {
		return "", compileState{}, errors.New("compile ClickHouse window: output field is required")
	}
	if _, exists := state.visible[operator.Output]; exists {
		return "", compileState{}, fmt.Errorf("compile ClickHouse window: output field %q is duplicated", operator.Output)
	}
	input, ok, err := resolveCompiledField(operator.Input, state)
	if err != nil {
		return "", compileState{}, err
	}
	if !ok || input.kind != fieldKindNumber {
		return "", compileState{}, fmt.Errorf("compile ClickHouse window: input field %q must be numeric", operator.Input.Name)
	}
	if operator.Function != plan.WindowFunctionPercentOfTotal {
		return "", compileState{}, fmt.Errorf("compile ClickHouse window: unsupported function %d", operator.Function)
	}

	// Aggregate groups always have a strictly positive count, so an empty input
	// produces no row on which division could occur. Cast before multiplication
	// to avoid integer overflow and retain the unrounded SPL percentage.
	total := "sum(" + input.valueSQL + ") OVER ()"
	expression := "toFloat64(" + input.valueSQL + ") * 100.0 / toFloat64(" + total + ")"
	next := state
	next.visible = make(map[string]fieldState, len(state.visible)+1)
	for name, field := range state.visible {
		next.visible[name] = field
	}
	next.publicOrder = append([]string(nil), state.publicOrder...)
	output := quoteIdentifier(operator.Output)
	next.visible[operator.Output] = fieldState{
		valueSQL: output, existsSQL: "1", kind: fieldKindNumber, numberType: "Float64",
	}
	next.publicOrder = append(next.publicOrder, operator.Output)
	return expression, next, nil
}

func compileSort(keys []plan.SortKey, state compileState, stage int) ([]string, []compiledSortKey, string, error) {
	if len(keys) == 0 {
		return nil, nil, "", errors.New("compile ClickHouse sort: no keys")
	}
	materialized := make([]string, 0, len(keys)+len(state.tieBreakers))
	compiled := make([]compiledSortKey, 0, len(keys)+len(state.tieBreakers))
	explicitValues := make(map[string]struct{}, len(keys))
	for i, key := range keys {
		field, ok, err := resolveCompiledField(key.Field, state)
		if err != nil {
			return nil, nil, "", err
		}
		if !ok {
			// SPL permits sorting by a field that is missing from every row. Use
			// one typed NULL key and retain the pipeline's stable row identity;
			// never resurrect event columns after a transforming command.
			field = fieldState{
				valueSQL:  "CAST(NULL AS Nullable(String))",
				existsSQL: "0",
				kind:      fieldKindString,
			}
		}
		explicitValues[field.valueSQL] = struct{}{}
		alias := fmt.Sprintf("__os_order_%d_%d", stage, i)
		sortValue := field.valueSQL
		switch key.Mode {
		case plan.SortValueModeAuto:
			if field.kind == fieldKindDynamic || field.numericSort {
				sortValue = dynamicSortValue(field.valueSQL, field.kind == fieldKindDynamic)
			}
		case plan.SortValueModeLexical:
			sortValue = "toString(" + field.valueSQL + ")"
		default:
			return nil, nil, "", fmt.Errorf("compile ClickHouse sort: invalid value mode %d", key.Mode)
		}
		materialized = append(materialized, sortValue+" AS "+quoteIdentifier(alias))
		compiled = append(compiled, compiledSortKey{valueSQL: quoteIdentifier(alias), descending: key.Descending})
	}
	// Preserve a stable row identity without assuming the input still consists
	// of events. Event pipelines use event_id; transforming pipelines use their
	// unique grouping tuple, and a global aggregate needs no tie-breaker.
	for index, tie := range state.tieBreakers {
		if _, explicit := explicitValues[tie.valueSQL]; explicit {
			continue
		}
		tieAlias := fmt.Sprintf("__os_order_%d_tie_%d", stage, index)
		materialized = append(materialized, tie.valueSQL+" AS "+quoteIdentifier(tieAlias))
		tie.valueSQL = quoteIdentifier(tieAlias)
		compiled = append(compiled, tie)
	}
	order, err := compileMaterializedOrder(compiled, false)
	if err != nil {
		return nil, nil, "", err
	}
	return materialized, compiled, order, nil
}

func dynamicSortValue(valueSQL string, dynamicValue bool) string {
	text := "toString(" + valueSQL + ")"
	number := finiteFloatOrNullSQL(text)
	if dynamicValue {
		dynamic := compiledScalar{
			valueSQL:       valueSQL,
			dynamicTypeSQL: "dynamicType(" + valueSQL + ")",
			kind:           fieldKindDynamic,
		}
		number = "ifNotFinite(" + numericScalarSQL(dynamic, false) + ", CAST(NULL AS Nullable(Float64)))"
	}
	integer := "accurateCastOrNull(" + text + ", 'Int256')"
	// Dynamic itself is intentionally forbidden in ClickHouse ORDER BY. A
	// fixed tuple also gives SPL-like numeric ordering for numeric values and
	// strings. The Int256 tie-break preserves adjacent integral values that
	// collapse to the same Float64 beyond 2^53. Nonnumeric scalars sort before
	// missing/explicit null.
	return "tuple(" +
		"if(isNull(" + valueSQL + "), toUInt8(2), if(isNotNull(" + number + "), toUInt8(0), toUInt8(1))), " +
		"ifNull(" + number + ", 0.), " +
		"if(isNotNull(" + integer + "), toUInt8(0), toUInt8(1)), " +
		"ifNull(" + integer + ", toInt256(0)), " +
		"ifNull(" + text + ", '')" +
		")"
}

func compileMaterializedOrder(keys []compiledSortKey, reverse bool) (string, error) {
	if len(keys) == 0 {
		return "", errors.New("compile ClickHouse sort: no keys")
	}
	parts := make([]string, 0, len(keys))
	for _, key := range keys {
		descending := key.descending
		nullsFirst := key.nullsFirst
		if reverse {
			descending = !descending
			nullsFirst = !nullsFirst
		}
		direction := "ASC"
		if descending {
			direction = "DESC"
		}
		nulls := "NULLS LAST"
		if nullsFirst {
			nulls = "NULLS FIRST"
		}
		parts = append(parts, key.valueSQL+" "+direction+" "+nulls)
	}
	return strings.Join(parts, ", "), nil
}

func reverseCompiledSortKeys(keys []compiledSortKey) []compiledSortKey {
	result := make([]compiledSortKey, len(keys))
	for i, key := range keys {
		key.descending = !key.descending
		key.nullsFirst = !key.nullsFirst
		result[i] = key
	}
	return result
}

func stableCompiledSortKeys() []compiledSortKey {
	return []compiledSortKey{
		{valueSQL: quoteIdentifier(internalSortTimeColumn), descending: true},
		{valueSQL: quoteIdentifier(internalSortIDColumn), descending: true},
	}
}

func defaultCompiledOrder(state compileState) []compiledSortKey {
	if len(state.order) > 0 {
		return state.order
	}
	if state.eventRows {
		return stableCompiledSortKeys()
	}
	return state.tieBreakers
}

func finalProjection(state compileState) ([]string, []string, error) {
	projection := make([]string, 0, len(state.publicOrder))
	output := make([]string, 0, len(state.publicOrder))
	seen := make(map[string]struct{}, len(state.publicOrder))
	for _, name := range state.publicOrder {
		if _, exists := seen[name]; exists {
			continue
		}
		seen[name] = struct{}{}
		field, visible := state.visible[name]
		if name == "fields" && !visible && state.eventRows {
			projection = append(projection, quoteIdentifier(internalFieldsColumn)+" AS "+quoteIdentifier("fields"))
			output = append(output, name)
			continue
		}
		if !visible {
			continue
		}
		publicName := quoteIdentifier(name)
		if field.valueSQL == publicName {
			projection = append(projection, publicName)
		} else {
			projection = append(projection, field.valueSQL+" AS "+publicName)
		}
		output = append(output, name)
	}
	if len(projection) == 0 {
		return nil, nil, errors.New("compile ClickHouse query: projection has no visible fields")
	}
	return projection, output, nil
}

func aliasPhysical(physical, alias string) string {
	return quoteIdentifier(physical) + " AS " + quoteIdentifier(alias)
}

func quoteIdentifier(identifier string) string {
	const hexadecimal = "0123456789ABCDEF"
	var quoted strings.Builder
	quoted.Grow(len(identifier) + 2)
	quoted.WriteByte('"')
	for index := 0; index < len(identifier); index++ {
		value := identifier[index]
		switch value {
		case '\\', '"', '?', '$', '{', '}':
			// clickhouse-go's legacy binder recognizes ?, $N, and {name:type}
			// without parsing SQL quoting. ClickHouse decodes hexadecimal escapes
			// inside quoted identifiers, so keep bind markers out of the client-side
			// query while preserving the exact server-visible column name.
			quoted.WriteString(`\x`)
			quoted.WriteByte(hexadecimal[value>>4])
			quoted.WriteByte(hexadecimal[value&0x0f])
		default:
			quoted.WriteByte(value)
		}
	}
	quoted.WriteByte('"')
	return quoted.String()
}

func normalizedDynamicPath(path []string) string {
	parts := make([]string, len(path))
	for i, segment := range path {
		segment = strings.ReplaceAll(segment, `\`, `\\`)
		segment = strings.ReplaceAll(segment, `.`, `\.`)
		parts[i] = segment
	}
	return strings.Join(parts, ".")
}

func wildcardRegex(value string, caseInsensitive bool) string {
	var result strings.Builder
	if caseInsensitive {
		result.WriteString("(?i)")
	}
	result.WriteByte('^')
	for _, r := range value {
		switch r {
		case '*':
			result.WriteString(".*")
		case '.', '+', '?', '(', ')', '[', ']', '{', '}', '^', '$', '|', '\\':
			result.WriteByte('\\')
			result.WriteRune(r)
		default:
			result.WriteRune(r)
		}
	}
	result.WriteByte('$')
	return result.String()
}

func freeTextRegex(value string, quoted bool) string {
	var result strings.Builder
	result.WriteString("(?i)")
	if !quoted {
		result.WriteString("(?:^|[^[:alnum:]_])")
	}
	for _, r := range value {
		if r == '*' {
			if quoted {
				result.WriteString(".*")
			} else {
				result.WriteString("[[:alnum:]_]*")
			}
			continue
		}
		if strings.ContainsRune(`.+?()[]{}^$|\\`, r) {
			result.WriteByte('\\')
		}
		result.WriteRune(r)
	}
	if !quoted {
		result.WriteString("(?:$|[^[:alnum:]_])")
	}
	return result.String()
}

func cloneSet(source map[string]struct{}) map[string]struct{} {
	result := make(map[string]struct{}, len(source))
	for key := range source {
		result[key] = struct{}{}
	}
	return result
}
