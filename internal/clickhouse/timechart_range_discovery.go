package clickhouse

import (
	"context"
	"errors"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/plan"
)

type compiledTimechartRangeDiscovery struct {
	operator     plan.Timechart
	scan         plan.Scan
	searchStart  time.Time
	timezone     string
	compiler     Compiler
	continuation *compiledTimechartContinuation
}

func compileTimechartRangeSource(relation compiledRelation, state compileState, args []any, operator *plan.Timechart, scan *plan.Scan, stage int) (CompiledQuery, error) {
	if err := validateTimechartMeasure(operator, state); err != nil {
		return CompiledQuery{}, err
	}
	projection, next, prefix, err := compileProjection(&plan.Project{Mode: plan.ProjectModeTable, Fields: []plan.FieldRef{operator.Time}, Range: operator.Range}, state, "", stage)
	if err != nil {
		return CompiledQuery{}, err
	}
	appendInput := func(name, expression string, field fieldState, binds []any) {
		projection = append(projection, expression+" AS "+quoteIdentifier(name))
		field.valueSQL = quoteIdentifier(name)
		next.visible[name] = field
		next.publicOrder = append(next.publicOrder, name)
		prefix = append(prefix, binds...)
	}
	if operator.Measure.Input.Name != "" {
		field, _, resolveErr := resolveCompiledField(operator.Measure.Input, state)
		if resolveErr != nil {
			return CompiledQuery{}, resolveErr
		}
		if operator.Measure.Function == plan.AggregateFunctionCountValues {
			expression, binds, resolveErr := resolveCountValueInput(operator.Measure.Input, state)
			if resolveErr != nil {
				return CompiledQuery{}, resolveErr
			}
			appendInput(timechartObservedMeasure, expression, fieldState{kind: fieldKindNumber, numberType: "UInt64", existsSQL: "1", numericIntegral: true}, binds)
		} else {
			expression, binds := numericArrayInputSQL(field)
			// Preserve the compiler's numeric eligibility and multiplicity before
			// public result decoding discards semantic tags or flattened parents.
			expression = "CAST(arrayMap(sample -> CAST(sample AS Dynamic), " + expression + ") AS Dynamic)"
			appendInput(timechartObservedMeasure, expression, fieldState{kind: fieldKindDynamic, existsSQL: "1"}, binds)
		}
	}
	if operator.Split != nil {
		expression, binds, resolveErr := observedTimechartSplitInput(operator.Split.Field, state)
		if resolveErr != nil {
			return CompiledQuery{}, resolveErr
		}
		appendInput(timechartObservedSplit, expression, fieldState{kind: fieldKindString, existsSQL: "1"}, binds)
	}
	if state.context.mvExpandWorkSQL != "" {
		appendInput(timechartObservedWork, state.context.mvExpandWorkSQL, fieldState{kind: fieldKindNumber, numberType: "UInt64", existsSQL: "1", numericIntegral: true}, nil)
	}
	projected := "SELECT " + strings.Join(projection, ", ") + " FROM (" + relation.sql + ")"
	relation = relation.selectFrom(projected, operator.Range)
	field := next.visible["_time"]
	field.timeBucketEndSQL = ""
	next.visible["_time"] = field
	relation, next, err = addObservedTimechartWorkHeader(relation, next, scan)
	if err != nil {
		return CompiledQuery{}, err
	}
	return finalizeOrdinaryQuery(relation, next, prependArguments(prefix, args), scan, stage)
}

func newTimechartRangeDiscovery(operator *plan.Timechart, scan *plan.Scan, query *plan.Query, compiler Compiler, continuation *compiledTimechartContinuation) *compiledTimechartRangeDiscovery {
	copied := *operator
	copied.Time.Path = slices.Clone(operator.Time.Path)
	copied.Measure.Input.Path = slices.Clone(operator.Measure.Input.Path)
	copied.GridBoundaries = slices.Clone(operator.GridBoundaries)
	if operator.Split != nil {
		split := *operator.Split
		split.Field.Path = slices.Clone(operator.Split.Field.Path)
		copied.Split = &split
	}
	copiedScan := *scan
	copiedScan.Indexes = slices.Clone(scan.Indexes)
	compiler.relationInput = nil
	return &compiledTimechartRangeDiscovery{operator: copied, scan: copiedScan, searchStart: query.SearchStart, timezone: query.SearchTimezone, compiler: compiler, continuation: continuation}
}

func (compiled CompiledQuery) continueObservedTimechart(ctx context.Context, input *compiledRelationInput) (CompiledQuery, error) {
	discovery := compiled.rangeDiscovery
	if len(input.columns) != len(compiled.OutputFields) {
		return CompiledQuery{}, errors.New("timechart input discovery schema is invalid")
	}
	for i, column := range input.columns {
		if column.Name != compiled.OutputFields[i] {
			return CompiledQuery{}, errors.New("timechart input discovery column is invalid")
		}
		if column.Name == timechartObservedMeasure {
			expected := "Dynamic"
			if discovery.operator.Measure.Function == plan.AggregateFunctionCountValues {
				expected = "UInt64"
			}
			if column.Type != expected {
				return CompiledQuery{}, errors.New("timechart input discovery contribution type is invalid")
			}
		}
	}
	operator := discovery.operator
	if operator.Measure.Input.Name != "" {
		operator.Measure.Input = plan.FieldRef{Name: timechartObservedMeasure, Path: []string{timechartObservedMeasure}, Range: operator.Measure.Input.Range}
		input.timechartOccurrences = operator.Measure.Function == plan.AggregateFunctionCountValues
	}
	if operator.Split != nil {
		split := *operator.Split
		split.Field = plan.FieldRef{Name: timechartObservedSplit, Path: []string{timechartObservedSplit}, Range: split.Field.Range}
		operator.Split = &split
	}
	for i, column := range input.columns {
		if column.Name != timechartObservedWork {
			continue
		}
		var received uint64
		seen := false
		for _, row := range input.rows {
			work, ok := row[i].(uint64)
			if !ok {
				return CompiledQuery{}, errors.New("timechart discovery work receipt is invalid")
			}
			if err := validateTimechartWork(work, compiled.timechartWorkFloor); err != nil {
				return CompiledQuery{}, err
			}
			if seen && received != work {
				return CompiledQuery{}, errors.New("timechart discovery work receipt is inconsistent")
			}
			received, seen = work, true
			input.mvExpandRows = max(input.mvExpandRows, work)
		}
	}
	marker := -1
	for i, column := range input.columns {
		if column.Name == timechartObservedInputRow {
			marker = i
			input.observedWorkHeader = true
		}
	}
	observedRows, headers := 0, 0
	var earliest, latest time.Time
	for _, row := range input.rows {
		if marker >= 0 {
			value, ok := row[marker].(uint64)
			if !ok || value > 1 {
				return CompiledQuery{}, errors.New("observed timechart input marker is invalid")
			}
			if value == 0 {
				headers++
				continue
			}
		}
		observedRows++
		timestamp, ok := row[0].(time.Time)
		if !ok {
			return CompiledQuery{}, errors.New("timechart input extent requires timestamp _time")
		}
		if earliest.IsZero() || timestamp.Before(earliest) {
			earliest = timestamp
		}
		if latest.IsZero() || timestamp.After(latest) {
			latest = timestamp
		}
	}
	if marker >= 0 && headers != 1 {
		return CompiledQuery{}, errors.New("observed timechart work header is missing or duplicated")
	}
	if earliest.IsZero() {
		earliest = discovery.scan.Earliest
		latest = earliest
	}
	latest = latest.Add(time.Nanosecond)
	if err := plan.ResolveTimechartGrid(&operator, earliest, latest, discovery.timezone); err != nil {
		return CompiledQuery{}, err
	}
	fields, dynamic := timechartStageOutput(&operator)
	query := &plan.Query{Operators: []plan.Operator{&discovery.scan, &operator}, OutputFields: fields, DynamicOutput: dynamic, SearchStart: discovery.searchStart, SearchTimezone: discovery.timezone, EffectiveIndexes: slices.Clone(discovery.scan.Indexes)}
	compiler := discovery.compiler
	compiler.relationInput = input
	result, err := compiler.CompileContext(ctx, query)
	if err != nil {
		return CompiledQuery{}, err
	}
	result.emptyTimechartInput = observedRows == 0
	result.continuation = discovery.continuation
	result.atomicResult = true
	return result, nil
}

// RequiresTimechartInputDiscovery distinguishes the bounded input capture from a result pivot.
func (compiled CompiledQuery) RequiresTimechartInputDiscovery() bool {
	return compiled.rangeDiscovery != nil
}

// HasEmptyTimechartInput is sealed evidence that observed-range input was empty.
func (compiled CompiledQuery) HasEmptyTimechartInput() bool { return compiled.emptyTimechartInput }

const (
	timechartObservedMeasure = "timechart_observed_measure"
	timechartObservedSplit   = "timechart_observed_split"
)

// Capture only valid split labels and missingness. The reserved NULL label is
// an invalid-input witness, so the ordinary pivot validation rejects every bad
// source row even when a later filter, series limit, or head would hide it.
func observedTimechartSplitInput(input plan.FieldRef, state compileState) (string, []any, error) {
	field, resolved, err := resolveCompiledField(input, state)
	if err != nil {
		return "", nil, err
	}
	if !resolved {
		return "CAST(NULL AS Nullable(String))", nil, nil
	}
	if field.kind == fieldKindInvalid {
		field.kind, field.valueSQL = fieldKindString, "CAST(NULL AS Nullable(String))"
	}
	if field.kind != fieldKindString && field.kind != fieldKindDynamic {
		return "", nil, &plan.Diagnostic{Code: "SPL_UNSUPPORTED_TIMECHART_FIELD_TYPE", Message: "timechart split fields currently support strings and missing values", Range: input.Range, Suggestions: []string{"convert the split field to a string before timechart"}}
	}
	present := field.existsSQL
	if present == "" {
		present = "1"
	}
	descendant, kind := "0", "'String'"
	binds := append([]any(nil), field.existsArgs...)
	if field.kind == fieldKindDynamic {
		kind = dynamicTypeExpression(field)
		if field.descendantSQL != "" {
			descendant = field.descendantSQL
			binds = append(binds, field.descendantArgs...)
		}
	}
	label := "assumeNotNull(toString(value))"
	valid := "isValidUTF8(" + label + ") AND length(" + label + ") BETWEEN 1 AND " + strconv.Itoa(maxTimechartLabelBytes) + " AND " + label + " NOT IN ('NULL', 'OTHER')"
	body := "multiIf(present = 0 AND descendant != 0, 'NULL', present = 0 OR isNull(value) OR kind = 'None', CAST(NULL AS Nullable(String)), kind != 'String', 'NULL', if(" + valid + ", " + label + ", 'NULL'))"
	return bindSQLExpressions([]string{"value", "kind", "present", "descendant"}, []string{field.valueSQL, kind, present, descendant}, body), binds, nil
}
