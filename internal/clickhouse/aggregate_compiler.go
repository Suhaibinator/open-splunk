package clickhouse

import (
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strconv"
	"strings"

	"github.com/Suhaibinator/open-splunk/internal/eventfields"
	"github.com/Suhaibinator/open-splunk/internal/plan"
	"github.com/Suhaibinator/open-splunk/internal/spl"
)

func compileExactScalarGroup(
	group plan.FieldRef,
	state compileState,
	multivalueOperation string,
) (compiledExactScalarGroup, error) {
	field, exists, resolveErr := resolveCompiledField(group, state)
	if resolveErr != nil {
		return compiledExactScalarGroup{}, resolveErr
	}
	if !exists {
		// A prior projection is authoritative. The typed missing key preserves
		// the declared downstream schema without consulting private event data.
		field = fieldState{
			valueSQL:   "CAST(NULL AS Nullable(String))",
			existsSQL:  "0",
			kind:       fieldKindString,
			alwaysNull: true,
		}
	}
	if isNativeMultivalueKind(field.kind) {
		return compiledExactScalarGroup{}, unsupportedMultivalueUsage(
			multivalueOperation,
			group.Range,
		)
	}

	existsSQL := field.existsSQL
	if existsSQL == "" {
		existsSQL = "1"
	}
	presenceSQL := "(" + existsSQL + " AND isNotNull(" + field.valueSQL + "))"
	presenceArgs := append([]any(nil), field.existsArgs...)
	if field.kind == fieldKindDynamic && field.descendantSQL != "" {
		// Flattened non-empty objects have no exact parent leaf. Keep them
		// present until the scoped scalar validation rejects the container.
		presenceSQL = "(" + presenceSQL + " OR " + field.descendantSQL + ")"
		presenceArgs = append(presenceArgs, field.descendantArgs...)
	}
	compiled := compiledExactScalarGroup{
		field:        field,
		keySQL:       field.valueSQL,
		presenceSQL:  presenceSQL,
		presenceArgs: presenceArgs,
	}
	if field.kind == fieldKindDynamic {
		supported, lexical := statsByScalarExpressions(field)
		compiled.keySQL = "if(" + supported + ", " + lexical + ", '')"
		compiled.unsupportedSQL = "(" + presenceSQL + ") AND NOT (" + supported + ")"
		compiled.unsupportedArgs = presenceArgs
	}
	return compiled, nil
}

// exactScalarGroupClassificationSQL binds the Dynamic support predicate once
// and publishes the complete row-local BY contract as:
//
//	(key, present, unsupported)
//
// Windowed Dynamic eventstats extrema consume this tuple from a separate
// preparation layer. That keeps key construction and independent container
// validation from expanding the tagged-envelope classifier twice.
func exactScalarGroupClassificationSQL(
	group compiledExactScalarGroup,
) (string, []any) {
	if group.field.kind != fieldKindDynamic {
		return "tuple(" + group.keySQL + ", toUInt8(" +
				group.presenceSQL + "), toUInt8(0))",
			append([]any(nil), group.presenceArgs...)
	}

	presenceVariable := "__os_eventstats_group_present"
	typeVariable := "__os_eventstats_group_type"
	supportedVariable := "__os_eventstats_group_supported"
	supportedSQL, lexicalSQL := statsByScalarExpressionsFor(
		group.field.valueSQL,
		typeVariable,
	)
	classification := "tuple(if(" + supportedVariable + ", " + lexicalSQL +
		", CAST('' AS String)), toUInt8(" + presenceVariable + "), toUInt8(" +
		presenceVariable + " AND NOT (" + supportedVariable + ")))"
	classification = bindSQLExpressions(
		[]string{presenceVariable, supportedVariable},
		[]string{group.presenceSQL, supportedSQL},
		classification,
	)
	classification = bindSQLExpressions(
		[]string{typeVariable},
		[]string{dynamicTypeExpression(group.field)},
		classification,
	)
	return classification, append([]any(nil), group.presenceArgs...)
}

type compiledEventStatsGroup struct {
	scalar   compiledExactScalarGroup
	keyAlias string
}

type eventAggregateMeasureSpec struct {
	function        plan.AggregateFunction
	percentile      uint8
	materialized    bool
	numberType      string
	numericIntegral bool
	valuePrefix     string
}

func streamAggregateFieldFunctionForm(
	function plan.AggregateFunction,
) (string, bool) {
	switch function {
	case plan.AggregateFunctionCountValues:
		return "count", true
	case plan.AggregateFunctionSum:
		return "sum", true
	case plan.AggregateFunctionAverage:
		return "avg", true
	case plan.AggregateFunctionMinimum:
		return "min", true
	case plan.AggregateFunctionMaximum:
		return "max", true
	case plan.AggregateFunctionEarliest:
		return "earliest", true
	case plan.AggregateFunctionLatest:
		return "latest", true
	default:
		return "", false
	}
}

func validateStreamAggregate(
	operator *plan.StreamAggregate,
	state compileState,
) (plan.FieldRef, error) {
	if operator == nil {
		return plan.FieldRef{}, errors.New(
			"compile ClickHouse streamstats: operator is missing",
		)
	}
	if err := validateNonStatsAggregateMeasureMetadata("streamstats", operator.Measure); err != nil {
		return plan.FieldRef{}, err
	}
	if len(operator.GroupBy) > spl.MaximumStatsGroupFields {
		return plan.FieldRef{}, fmt.Errorf(
			"compile ClickHouse streamstats: more than %d grouping fields",
			spl.MaximumStatsGroupFields,
		)
	}
	if operator.WindowRows > spl.MaximumStreamStatsWindow {
		return plan.FieldRef{}, fmt.Errorf(
			"compile ClickHouse streamstats: window exceeds %d rows",
			spl.MaximumStreamStatsWindow,
		)
	}
	if len(operator.GroupBy) > 0 && operator.WindowRows > 0 && operator.Global {
		return plan.FieldRef{}, errors.New(
			"compile ClickHouse streamstats: grouped positive windows require global=false",
		)
	}
	measure := operator.Measure
	if measure.Output == "" ||
		(measure.Function != plan.AggregateFunctionCountPredicate &&
			(measure.Predicate != nil || measure.Percentile != 0)) {
		return plan.FieldRef{}, errors.New(
			"compile ClickHouse streamstats: aggregate contains unsupported metadata",
		)
	}
	if (measure.Function == plan.AggregateFunctionEarliest ||
		measure.Function == plan.AggregateFunctionLatest) &&
		!hasCanonicalEventTime(state) {
		return plan.FieldRef{}, &plan.Diagnostic{
			Code: "SPL_UNSUPPORTED_STREAMSTATS_TIME_FIELD",
			Message: "streamstats earliest and latest require event rows " +
				"with the unmodified canonical _time field",
			Range: measure.Input.Range,
			Suggestions: []string{
				"run streamstats earliest or latest before removing, replacing, or transforming _time",
			},
		}
	}
	switch measure.Function {
	case plan.AggregateFunctionCountRows:
		if measure.Input.Name != "" ||
			measure.Input.Canonical ||
			measure.Input.Path != nil ||
			measure.Input.Range != (spl.Range{}) {
			return plan.FieldRef{}, errors.New(
				"compile ClickHouse streamstats: argument-free count contains input metadata",
			)
		}
	case plan.AggregateFunctionCountPredicate:
		if err := validateConditionalCountMeasure(
			measure,
			state,
			"streamstats",
			"SPL_AMBIGUOUS_STREAMSTATS_FIELD",
			"streamstats cannot read the event result's reserved fields payload without an exact upstream schema",
		); err != nil {
			return plan.FieldRef{}, err
		}
	case plan.AggregateFunctionCountValues, plan.AggregateFunctionSum,
		plan.AggregateFunctionAverage, plan.AggregateFunctionMinimum,
		plan.AggregateFunctionMaximum, plan.AggregateFunctionEarliest,
		plan.AggregateFunctionLatest:
		form, supported := streamAggregateFieldFunctionForm(measure.Function)
		if !supported {
			return plan.FieldRef{}, errors.New(
				"compile ClickHouse streamstats: field aggregate form is unsupported",
			)
		}
		if !spl.IsExactUnquotedFieldName(measure.Input.Name) {
			return plan.FieldRef{}, errors.New(
				"compile ClickHouse streamstats: " + form +
					" input must be one exact unquoted field",
			)
		}
		if err := validateCanonicalFieldRef(
			"streamstats",
			form+" input",
			measure.Input,
		); err != nil {
			return plan.FieldRef{}, err
		}
		if state.eventRows && state.allowDynamic && measure.Input.Name == "fields" {
			return plan.FieldRef{}, &plan.Diagnostic{
				Code: "SPL_AMBIGUOUS_STREAMSTATS_FIELD",
				Message: "streamstats cannot read the event result's reserved " +
					"fields payload without an exact upstream schema",
				Range: measure.Input.Range,
			}
		}
	default:
		return plan.FieldRef{}, errors.New(
			"compile ClickHouse streamstats: only count, count(field), count(eval(...)), sum(field), avg(field), min(field), max(field), earliest(field), and latest(field) are supported",
		)
	}
	defaultOutput := ""
	if form, supported := streamAggregateFieldFunctionForm(measure.Function); supported {
		defaultOutput = form + "(" + measure.Input.Name + ")"
	}
	validOutput := spl.IsExactUnquotedFieldName(measure.Output) ||
		(defaultOutput != "" && measure.Output == defaultOutput)
	if !validOutput {
		return plan.FieldRef{}, errors.New(
			"compile ClickHouse streamstats: output must be one exact unquoted field",
		)
	}
	output, err := plan.ResolveField(measure.Output, operator.Range)
	if err != nil {
		return plan.FieldRef{}, fmt.Errorf(
			"compile ClickHouse streamstats: invalid output field %q: %w",
			measure.Output,
			err,
		)
	}
	if state.eventRows && state.allowDynamic && output.Name == "fields" {
		return plan.FieldRef{}, &plan.Diagnostic{
			Code: "SPL_AMBIGUOUS_STREAMSTATS_FIELD",
			Message: "streamstats cannot replace the event result's reserved " +
				"fields payload without an exact upstream schema",
			Range: output.Range,
		}
	}
	seen := make(map[string]struct{}, len(operator.GroupBy))
	for _, group := range operator.GroupBy {
		if !spl.IsExactUnquotedFieldName(group.Name) {
			return plan.FieldRef{}, errors.New(
				"compile ClickHouse streamstats: grouping fields must be exact and unquoted",
			)
		}
		if err := validateCanonicalFieldRef(
			"streamstats",
			"grouping",
			group,
		); err != nil {
			return plan.FieldRef{}, err
		}
		if _, duplicate := seen[group.Name]; duplicate {
			return plan.FieldRef{}, fmt.Errorf(
				"compile ClickHouse streamstats: grouping field %q is repeated",
				group.Name,
			)
		}
		seen[group.Name] = struct{}{}
		if state.eventRows && state.allowDynamic && group.Name == "fields" {
			return plan.FieldRef{}, &plan.Diagnostic{
				Code: "SPL_AMBIGUOUS_STREAMSTATS_FIELD",
				Message: "streamstats cannot group by the event result's reserved " +
					"fields payload without an exact upstream schema",
				Range: group.Range,
			}
		}
	}
	return output, nil
}

func streamAggregateCompileState(
	state compileState,
	output plan.FieldRef,
	outputState fieldState,
	grouped bool,
	stage int,
	order []compiledSortKey,
	tieBreakers []compiledSortKey,
) compileState {
	next := cloneCompileState(state)
	if exposesRawFieldsPayload(state) && !output.Canonical {
		dropRawFieldsPayload(&next)
	}
	delete(next.blocked, output.Name)
	if !slices.Contains(next.publicOrder, output.Name) {
		next.publicOrder = append(next.publicOrder, output.Name)
	}
	existsSQL := "1"
	if grouped {
		existsSQL = quoteIdentifier(fmt.Sprintf(
			"__os_streamstats_exists_%d",
			stage,
		))
	}
	outputState.valueSQL = quoteIdentifier(output.Name)
	outputState.existsSQL = existsSQL
	// Every streamstats result is derived. Even min(_time) is not the immutable
	// event timestamp required by canonical-time consumers such as timechart.
	outputState.canonicalTime = false
	next.visible[output.Name] = outputState
	// The running value may replace a public field that supplied the incoming
	// order. Every key was snapshotted before replacement, so make those private
	// sequences the durable pipeline order and stable event identity. A later
	// explicit sort consumes tieBreakers independently from order.
	next.order = append([]compiledSortKey(nil), order...)
	next.tieBreakers = append([]compiledSortKey(nil), tieBreakers...)
	next.privateColumns = livePrivateColumns(next.privateColumns, next.visible)
	if outputState.storedTypeSQL != "" {
		next.privateColumns = append(next.privateColumns, outputState.storedTypeSQL)
	}
	if grouped {
		next.privateColumns = append(next.privateColumns, existsSQL)
	}
	return next
}

// streamStatsStringArrayExtremaMeasureSQL folds a fixed Array(String) row to
// the same constant-size winner tuple used by Dynamic extrema. The stream
// window is still measured in source rows: multivalue members never expand the
// relation or consume independent frame positions.
func streamStatsStringArrayExtremaMeasureSQL(
	function plan.AggregateFunction,
	valuesSQL string,
	rowEligibleSQL string,
) string {
	if rowEligibleSQL == "" {
		rowEligibleSQL = "1"
	}
	state := "__os_streamstats_extrema_state"
	value := "value"
	candidateSQL := statsExtremaScalarCandidateSQL(
		value,
		statsExtremaScalarNumberSQL(value),
		"0",
	)
	step := extremaFoldWinnerStateSQL(
		function,
		state,
		candidateSQL,
		"",
	)
	empty := eventStatsExtremaEmptyRowStateSQL("0")
	fold := "arrayFold((" + state + ", " + value + ") -> " + step + ", " +
		valuesSQL + ", " + empty + ")"
	return "if(" + rowEligibleSQL + ", " + fold + ", " + empty + ")"
}

func streamStatsFrameSQL(includeCurrent bool, window uint64) string {
	if window == 0 {
		if includeCurrent {
			return "ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW"
		}
		return "ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING"
	}
	if includeCurrent {
		if window == 1 {
			return "ROWS BETWEEN CURRENT ROW AND CURRENT ROW"
		}
		return "ROWS BETWEEN " + strconv.FormatUint(window-1, 10) +
			" PRECEDING AND CURRENT ROW"
	}
	return "ROWS BETWEEN " + strconv.FormatUint(window, 10) +
		" PRECEDING AND 1 PRECEDING"
}

func chronologicalAggregateFunction(function plan.AggregateFunction) bool {
	return function == plan.AggregateFunctionEarliest ||
		function == plan.AggregateFunctionLatest
}

func newChronologicalAggregateOutput(state compileState, name string) bool {
	if name == "" || slices.Contains(state.publicOrder, name) {
		return false
	}
	_, visible := state.visible[name]
	return !visible
}

func independentChronologicalAggregateOutputs(
	state compileState,
	firstInput, secondInput, firstOutput, secondOutput string,
) bool {
	if !newChronologicalAggregateOutput(state, firstOutput) ||
		!newChronologicalAggregateOutput(state, secondOutput) ||
		firstOutput == secondOutput {
		return false
	}
	// Restrict fusion to pure sibling publications. In particular, neither
	// measure may observe a value authored by the other logical stage, and an
	// output may not replace a source field whose pre-replacement value the
	// sibling consumes.
	for _, output := range []string{firstOutput, secondOutput} {
		if output == firstInput || output == secondInput {
			return false
		}
	}
	return true
}

func dynamicChronologicalInputs(
	state compileState,
	first, second plan.FieldRef,
) bool {
	firstField, firstExists, firstErr := resolveCompiledField(first, state)
	secondField, secondExists, secondErr := resolveCompiledField(second, state)
	return firstErr == nil && secondErr == nil && firstExists && secondExists &&
		firstField.kind == fieldKindDynamic && secondField.kind == fieldKindDynamic
}

func canFuseChronologicalEventAggregates(
	first, second *plan.EventAggregate,
	state compileState,
) bool {
	if first == nil || second == nil || len(first.GroupBy) != 0 ||
		len(second.GroupBy) != 0 ||
		!chronologicalAggregateFunction(first.Measure.Function) ||
		!chronologicalAggregateFunction(second.Measure.Function) ||
		!independentChronologicalAggregateOutputs(
			state,
			first.Measure.Input.Name,
			second.Measure.Input.Name,
			first.Measure.Output,
			second.Measure.Output,
		) {
		return false
	}
	return dynamicChronologicalInputs(
		state,
		first.Measure.Input,
		second.Measure.Input,
	)
}

func canFuseChronologicalStreamAggregates(
	first, second *plan.StreamAggregate,
	state compileState,
) bool {
	if first == nil || second == nil || len(first.GroupBy) != 0 ||
		len(second.GroupBy) != 0 ||
		first.IncludeCurrent != second.IncludeCurrent ||
		first.WindowRows != second.WindowRows || first.Global != second.Global ||
		!chronologicalAggregateFunction(first.Measure.Function) ||
		!chronologicalAggregateFunction(second.Measure.Function) ||
		!independentChronologicalAggregateOutputs(
			state,
			first.Measure.Input.Name,
			second.Measure.Input.Name,
			first.Measure.Output,
			second.Measure.Output,
		) {
		return false
	}
	return dynamicChronologicalInputs(
		state,
		first.Measure.Input,
		second.Measure.Input,
	)
}

type fusedChronologicalPublication struct {
	name             string
	valueSQL         string
	storedTypeSQL    string
	validationColumn string
	validationSQL    string
}

func fusedChronologicalProjection(
	state, next compileState,
	publications []fusedChronologicalPublication,
) ([]string, error) {
	byName := make(map[string]fusedChronologicalPublication, len(publications))
	for _, publication := range publications {
		if publication.name == "" || publication.valueSQL == "" ||
			publication.storedTypeSQL == "" || publication.validationColumn == "" ||
			publication.validationSQL == "" {
			return nil, errors.New(
				"compile ClickHouse chronological fusion: publication is incomplete",
			)
		}
		if _, duplicate := byName[publication.name]; duplicate {
			return nil, errors.New(
				"compile ClickHouse chronological fusion: output is repeated",
			)
		}
		byName[publication.name] = publication
	}

	names := orderedVisibleNames(next)
	projection := make([]string, 0, len(names)+16+len(next.privateColumns))
	for _, name := range names {
		publicName := quoteIdentifier(name)
		if publication, authored := byName[name]; authored {
			projection = append(
				projection,
				publication.valueSQL+" AS "+publicName,
			)
			continue
		}
		field, present := state.visible[name]
		if !present {
			return nil, fmt.Errorf(
				"compile ClickHouse chronological fusion: input field %q is unavailable",
				name,
			)
		}
		projection = appendVisibleFieldProjection(projection, field, publicName)
	}

	projectionState := next
	projectionState.privateColumns = livePrivateColumns(
		state.privateColumns,
		next.visible,
	)
	projection = appendPrivateEventProjection(projection, projectionState)
	for _, publication := range publications {
		output := next.visible[publication.name]
		if output.storedTypeSQL == "" {
			return nil, errors.New(
				"compile ClickHouse chronological fusion: output type sidecar is missing",
			)
		}
		projection = append(
			projection,
			publication.storedTypeSQL+" AS "+output.storedTypeSQL,
			"toUInt8("+publication.validationSQL+") AS "+
				publication.validationColumn,
		)
	}
	return projection, nil
}

func fusedChronologicalOutputState(
	output plan.FieldRef,
	input fieldState,
	typeColumn string,
) fieldState {
	return fieldState{
		kind:           fieldKindDynamic,
		dynamicTypeSQL: "dynamicType(" + quoteIdentifier(output.Name) + ")",
		storedTypeSQL:  typeColumn,
		maxStringBytes: fieldStateStringByteBound(input),
	}
}

// transferDeferredChronologicalValidation moves validation ownership to a
// later complete barrier only when every moved column is still a private
// column of that barrier's input. The earlier barrier remains the semantic
// source relation, but no longer needs a second top-level consumer solely to
// repeat validation work.
func transferDeferredChronologicalValidation(
	barriers []compiledChronologicalBarrier,
	available []string,
) ([]compiledChronologicalBarrier, []string) {
	if len(barriers) == 0 || len(available) == 0 {
		return barriers, nil
	}
	transferred := make([]string, 0, len(available))
	for index := range barriers {
		barrier := &barriers[index]
		retained := make([]string, 0, len(barrier.validationColumns))
		for _, column := range barrier.validationColumns {
			if slices.Contains(available, column) {
				transferred = append(transferred, column)
				continue
			}
			retained = append(retained, column)
		}
		barrier.validationColumns = retained
		if len(retained) == 0 && barrier.fanout == 2 {
			// The ungrouped fused eventstats source is row-preserving and has one
			// remaining consumer after its validation columns move forward.
			barrier.fanout = 1
		}
	}
	return barriers, transferred
}

// compileFusedChronologicalEventAggregates lowers two independent sibling
// eventstats chronological measures over one bounded input and one global
// window. Both row-local poison bits remain independently named on the shared
// barrier, so the final validation consumer still sees the complete relation
// even if a later command removes every public row.
func compileFusedChronologicalEventAggregates(
	relation compiledRelation,
	first, second *plan.EventAggregate,
	state compileState,
	firstStage, secondStage int,
) (compiledRelation, compileState, []any, *pendingChronologicalBarrier, error) {
	if !canFuseChronologicalEventAggregates(first, second, state) ||
		secondStage != firstStage+1 {
		return compiledRelation{}, compileState{}, nil, nil, errors.New(
			"compile ClickHouse fused eventstats chronology: contract is invalid",
		)
	}

	firstOutput, firstErr := validateEventAggregate(first, state)
	if firstErr != nil {
		return compiledRelation{}, compileState{}, nil, nil, firstErr
	}
	firstInput, firstExists, resolveErr := resolveCompiledField(
		first.Measure.Input,
		state,
	)
	if resolveErr != nil {
		return compiledRelation{}, compileState{}, nil, nil, resolveErr
	}
	firstCandidate, firstArgs, firstValidated, candidateErr :=
		singleChronologicalCandidateSQL(
			first.Measure.Function,
			firstInput,
			firstExists,
		)
	if candidateErr != nil {
		return compiledRelation{}, compileState{}, nil, nil, candidateErr
	}
	if !firstValidated {
		return compiledRelation{}, compileState{}, nil, nil, errors.New(
			"compile ClickHouse fused eventstats chronology: first input is not runtime validated",
		)
	}
	firstType := quoteIdentifier(fmt.Sprintf(
		"__os_eventstats_extrema_type_%d",
		firstStage,
	))
	firstState := eventAggregateCompileState(
		state,
		firstOutput,
		fusedChronologicalOutputState(firstOutput, firstInput, firstType),
		false,
		firstStage,
	)

	secondOutput, secondErr := validateEventAggregate(second, firstState)
	if secondErr != nil {
		return compiledRelation{}, compileState{}, nil, nil, secondErr
	}
	secondInput, secondExists, resolveErr := resolveCompiledField(
		second.Measure.Input,
		firstState,
	)
	if resolveErr != nil {
		return compiledRelation{}, compileState{}, nil, nil, resolveErr
	}
	secondCandidate, secondArgs, secondValidated, candidateErr :=
		singleChronologicalCandidateSQL(
			second.Measure.Function,
			secondInput,
			secondExists,
		)
	if candidateErr != nil {
		return compiledRelation{}, compileState{}, nil, nil, candidateErr
	}
	if !secondValidated {
		return compiledRelation{}, compileState{}, nil, nil, errors.New(
			"compile ClickHouse fused eventstats chronology: second input is not runtime validated",
		)
	}
	secondType := quoteIdentifier(fmt.Sprintf(
		"__os_eventstats_extrema_type_%d",
		secondStage,
	))
	next := eventAggregateCompileState(
		firstState,
		secondOutput,
		fusedChronologicalOutputState(secondOutput, secondInput, secondType),
		false,
		secondStage,
	)

	firstMeasure := quoteIdentifier(fmt.Sprintf(
		"__os_eventstats_measure_%d",
		firstStage,
	))
	secondMeasure := quoteIdentifier(fmt.Sprintf(
		"__os_eventstats_measure_%d",
		secondStage,
	))
	sourceAlias := quoteIdentifier(fmt.Sprintf(
		"__os_eventstats_fused_source_%d",
		secondStage,
	))
	rowKey := immutableChronologicalRowKeySQL()
	preparedSQL := "SELECT *, tuple(" + firstCandidate + ", " + rowKey +
		") AS " + firstMeasure + ", tuple(" + secondCandidate + ", " +
		rowKey + ") AS " + secondMeasure + " FROM (" + relation.sql +
		") AS " + sourceAlias + " LIMIT " +
		strconv.FormatUint(MaximumEventStatsInputRows+1, 10)

	firstAggregate, aggregateErr := singleChronologicalAggregateSQL(
		first.Measure.Function,
		firstMeasure,
	)
	if aggregateErr != nil {
		return compiledRelation{}, compileState{}, nil, nil, aggregateErr
	}
	secondAggregate, aggregateErr := singleChronologicalAggregateSQL(
		second.Measure.Function,
		secondMeasure,
	)
	if aggregateErr != nil {
		return compiledRelation{}, compileState{}, nil, nil, aggregateErr
	}
	rawCount := quoteIdentifier(fmt.Sprintf(
		"__os_eventstats_fused_raw_count_%d",
		secondStage,
	))
	firstWinner := quoteIdentifier(fmt.Sprintf(
		"__os_eventstats_fused_winner_%d",
		firstStage,
	))
	secondWinner := quoteIdentifier(fmt.Sprintf(
		"__os_eventstats_fused_winner_%d",
		secondStage,
	))
	preparedAlias := quoteIdentifier(fmt.Sprintf(
		"__os_eventstats_fused_prepared_%d",
		secondStage,
	))
	windowSQL := "SELECT *, count() OVER () AS " + rawCount + ", " +
		firstAggregate + " OVER () AS " + firstWinner + ", " +
		secondAggregate + " OVER () AS " + secondWinner + " FROM (" +
		preparedSQL + ") AS " + preparedAlias

	inputCount := quoteIdentifier(fmt.Sprintf(
		"__os_eventstats_input_count_%d",
		secondStage,
	))
	windowAlias := quoteIdentifier(fmt.Sprintf(
		"__os_eventstats_fused_window_%d",
		secondStage,
	))
	boundedSQL := "SELECT *, " + boundedEventStatsCountSQL(rawCount) +
		" AS " + inputCount + " FROM (" + windowSQL + ") AS " + windowAlias
	resultAlias := quoteIdentifier(fmt.Sprintf(
		"__os_eventstats_fused_result_%d",
		secondStage,
	))
	firstValidation := quoteIdentifier(fmt.Sprintf(
		"__os_eventstats_validation_%d",
		firstStage,
	))
	secondValidation := quoteIdentifier(fmt.Sprintf(
		"__os_eventstats_validation_%d",
		secondStage,
	))
	publications := []fusedChronologicalPublication{
		{
			name:             firstOutput.Name,
			valueSQL:         chronologicalPublishedValueSQL(resultAlias + "." + firstWinner),
			storedTypeSQL:    chronologicalPublishedTypeSQL(resultAlias + "." + firstWinner),
			validationColumn: firstValidation,
			validationSQL: "tupleElement(tupleElement(" + resultAlias + "." +
				firstMeasure + ", 1), 4)",
		},
		{
			name:             secondOutput.Name,
			valueSQL:         chronologicalPublishedValueSQL(resultAlias + "." + secondWinner),
			storedTypeSQL:    chronologicalPublishedTypeSQL(resultAlias + "." + secondWinner),
			validationColumn: secondValidation,
			validationSQL: "tupleElement(tupleElement(" + resultAlias + "." +
				secondMeasure + ", 1), 4)",
		},
	}
	projection, projectionErr := fusedChronologicalProjection(
		state,
		next,
		publications,
	)
	if projectionErr != nil {
		return compiledRelation{}, compileState{}, nil, nil, projectionErr
	}
	next.deferredChronologicalValidation = append(
		next.deferredChronologicalValidation,
		firstValidation,
		secondValidation,
	)
	maximumRows := strconv.FormatUint(MaximumEventStatsInputRows, 10)
	resultSQL := "SELECT " + strings.Join(projection, ", ") + " FROM (" +
		boundedSQL + ") AS " + resultAlias + " WHERE " + resultAlias + "." +
		inputCount + " <= " + maximumRows
	resultDepth := relation.depth + 4
	enriched := compiledRelation{
		sql:        resultSQL,
		depth:      resultDepth,
		ownerRange: second.Range,
	}

	resultInputName := quoteIdentifier(fmt.Sprintf(
		"__os_eventstats_result_input_%d",
		secondStage,
	))
	barrierName := quoteIdentifier(fmt.Sprintf(
		"__os_eventstats_result_%d",
		secondStage,
	))
	barrierDepth := relationalNodeDepth(resultDepth)
	barrier := &pendingChronologicalBarrier{
		name: barrierName,
		sql:  "SELECT * FROM " + resultInputName,
		prerequisiteDefinitions: []string{
			resultInputName + " AS MATERIALIZED (" + resultSQL + ")",
		},
		validationColumns: []string{firstValidation, secondValidation},
		fanout:            2,
		depth:             barrierDepth,
		ownerRange:        second.Range,
	}
	publishedAlias := quoteIdentifier(fmt.Sprintf(
		"__os_eventstats_rows_result_%d",
		secondStage,
	))
	publishedSQL := "SELECT * FROM " + barrierName + " AS " + publishedAlias
	enriched.depth = barrierDepth
	prefixArgs := append(append([]any(nil), firstArgs...), secondArgs...)
	return enriched.selectFrom(publishedSQL, second.Range), next, prefixArgs, barrier, nil
}

// compileFusedChronologicalStreamAggregates captures the established order
// once and evaluates two independent windows with the same frame. Sequential
// streamstats semantics are unchanged because neither measure consumes or
// replaces the sibling's input or output.
func compileFusedChronologicalStreamAggregates(
	relation compiledRelation,
	first, second *plan.StreamAggregate,
	state compileState,
	firstStage, secondStage int,
) (compiledRelation, compileState, []any, *pendingChronologicalBarrier, error) {
	if !canFuseChronologicalStreamAggregates(first, second, state) ||
		secondStage != firstStage+1 {
		return compiledRelation{}, compileState{}, nil, nil, errors.New(
			"compile ClickHouse fused streamstats chronology: contract is invalid",
		)
	}

	firstOutput, firstErr := validateStreamAggregate(first, state)
	if firstErr != nil {
		return compiledRelation{}, compileState{}, nil, nil, firstErr
	}
	firstInput, firstExists, resolveErr := resolveCompiledField(
		first.Measure.Input,
		state,
	)
	if resolveErr != nil {
		return compiledRelation{}, compileState{}, nil, nil, resolveErr
	}
	firstCandidate, firstArgs, firstValidated, candidateErr :=
		singleChronologicalCandidateSQL(
			first.Measure.Function,
			firstInput,
			firstExists,
		)
	if candidateErr != nil {
		return compiledRelation{}, compileState{}, nil, nil, candidateErr
	}
	if !firstValidated {
		return compiledRelation{}, compileState{}, nil, nil, errors.New(
			"compile ClickHouse fused streamstats chronology: first input is not runtime validated",
		)
	}

	orderKeys := append([]compiledSortKey(nil), defaultCompiledOrder(state)...)
	tieBreakers := append([]compiledSortKey(nil), state.tieBreakers...)
	orderProjection := make([]string, 0, len(orderKeys)+len(tieBreakers))
	for index := range orderKeys {
		captured := quoteIdentifier(fmt.Sprintf(
			"__os_streamstats_order_%d_%d",
			firstStage,
			index,
		))
		orderProjection = append(
			orderProjection,
			orderKeys[index].valueSQL+" AS "+captured,
		)
		orderKeys[index].valueSQL = captured
	}
	for index := range tieBreakers {
		captured := quoteIdentifier(fmt.Sprintf(
			"__os_streamstats_tie_breaker_%d_%d",
			firstStage,
			index,
		))
		orderProjection = append(
			orderProjection,
			tieBreakers[index].valueSQL+" AS "+captured,
		)
		tieBreakers[index].valueSQL = captured
	}
	orderSQL := ""
	if len(orderKeys) > 0 {
		var orderErr error
		orderSQL, orderErr = compileMaterializedOrder(orderKeys, false)
		if orderErr != nil {
			return compiledRelation{}, compileState{}, nil, nil, fmt.Errorf(
				"compile ClickHouse fused streamstats order: %w",
				orderErr,
			)
		}
	}

	firstType := quoteIdentifier(fmt.Sprintf(
		"__os_streamstats_chronological_type_%d",
		firstStage,
	))
	firstState := streamAggregateCompileState(
		state,
		firstOutput,
		fusedChronologicalOutputState(firstOutput, firstInput, firstType),
		false,
		firstStage,
		orderKeys,
		tieBreakers,
	)
	secondOutput, secondErr := validateStreamAggregate(second, firstState)
	if secondErr != nil {
		return compiledRelation{}, compileState{}, nil, nil, secondErr
	}
	secondInput, secondExists, resolveErr := resolveCompiledField(
		second.Measure.Input,
		firstState,
	)
	if resolveErr != nil {
		return compiledRelation{}, compileState{}, nil, nil, resolveErr
	}
	secondCandidate, secondArgs, secondValidated, candidateErr :=
		singleChronologicalCandidateSQL(
			second.Measure.Function,
			secondInput,
			secondExists,
		)
	if candidateErr != nil {
		return compiledRelation{}, compileState{}, nil, nil, candidateErr
	}
	if !secondValidated {
		return compiledRelation{}, compileState{}, nil, nil, errors.New(
			"compile ClickHouse fused streamstats chronology: second input is not runtime validated",
		)
	}
	secondType := quoteIdentifier(fmt.Sprintf(
		"__os_streamstats_chronological_type_%d",
		secondStage,
	))
	next := streamAggregateCompileState(
		firstState,
		secondOutput,
		fusedChronologicalOutputState(secondOutput, secondInput, secondType),
		false,
		secondStage,
		orderKeys,
		tieBreakers,
	)

	firstMeasure := quoteIdentifier(fmt.Sprintf(
		"__os_streamstats_measure_%d",
		firstStage,
	))
	secondMeasure := quoteIdentifier(fmt.Sprintf(
		"__os_streamstats_measure_%d",
		secondStage,
	))
	rowKey := immutableChronologicalRowKeySQL()
	sourceAlias := quoteIdentifier(fmt.Sprintf(
		"__os_streamstats_fused_source_%d",
		secondStage,
	))
	orderedProjection := []string{
		"*",
		"tuple(" + firstCandidate + ", " + rowKey + ") AS " + firstMeasure,
		"tuple(" + secondCandidate + ", " + rowKey + ") AS " + secondMeasure,
	}
	orderedProjection = append(orderedProjection, orderProjection...)
	orderedInput := "SELECT " + strings.Join(orderedProjection, ", ") +
		" FROM (" + relation.sql + ") AS " + sourceAlias
	if orderSQL != "" {
		orderedInput += " ORDER BY " + orderSQL
	}
	orderedInput += " LIMIT " + strconv.FormatUint(MaximumStreamStatsInputRows+1, 10)

	windowParts := make([]string, 0, 2)
	if orderSQL != "" {
		windowParts = append(windowParts, "ORDER BY "+orderSQL)
	} else {
		windowParts = append(windowParts, "ORDER BY tuple()")
	}
	windowParts = append(
		windowParts,
		streamStatsFrameSQL(first.IncludeCurrent, first.WindowRows),
	)
	windowClause := strings.Join(windowParts, " ")
	firstAggregate, aggregateErr := singleChronologicalAggregateSQL(
		first.Measure.Function,
		firstMeasure,
	)
	if aggregateErr != nil {
		return compiledRelation{}, compileState{}, nil, nil, aggregateErr
	}
	secondAggregate, aggregateErr := singleChronologicalAggregateSQL(
		second.Measure.Function,
		secondMeasure,
	)
	if aggregateErr != nil {
		return compiledRelation{}, compileState{}, nil, nil, aggregateErr
	}
	inputCount := quoteIdentifier(fmt.Sprintf(
		"__os_streamstats_input_count_%d",
		secondStage,
	))
	firstWinner := quoteIdentifier(fmt.Sprintf(
		"__os_streamstats_value_%d",
		firstStage,
	))
	secondWinner := quoteIdentifier(fmt.Sprintf(
		"__os_streamstats_value_%d",
		secondStage,
	))
	preparedAlias := quoteIdentifier(fmt.Sprintf(
		"__os_streamstats_fused_prepared_%d",
		secondStage,
	))
	windowSQL := "SELECT *, count() OVER () AS " + inputCount + ", " +
		firstAggregate + " OVER (" + windowClause + ") AS " + firstWinner +
		", " + secondAggregate + " OVER (" + windowClause + ") AS " +
		secondWinner + " FROM (" + orderedInput + ") AS " + preparedAlias

	windowAlias := quoteIdentifier(fmt.Sprintf(
		"__os_streamstats_fused_window_%d",
		secondStage,
	))
	firstValidation := quoteIdentifier(fmt.Sprintf(
		"__os_streamstats_validation_%d",
		firstStage,
	))
	secondValidation := quoteIdentifier(fmt.Sprintf(
		"__os_streamstats_validation_%d",
		secondStage,
	))
	var transferredValidation []string
	next.chronologicalBarriers, transferredValidation =
		transferDeferredChronologicalValidation(
			next.chronologicalBarriers,
			state.deferredChronologicalValidation,
		)
	allValidation := append(
		append([]string(nil), transferredValidation...),
		firstValidation,
		secondValidation,
	)
	publications := []fusedChronologicalPublication{
		{
			name:             firstOutput.Name,
			valueSQL:         chronologicalPublishedValueSQL(windowAlias + "." + firstWinner),
			storedTypeSQL:    chronologicalPublishedTypeSQL(windowAlias + "." + firstWinner),
			validationColumn: firstValidation,
			validationSQL: "tupleElement(tupleElement(" + windowAlias + "." +
				firstMeasure + ", 1), 4)",
		},
		{
			name:             secondOutput.Name,
			valueSQL:         chronologicalPublishedValueSQL(windowAlias + "." + secondWinner),
			storedTypeSQL:    chronologicalPublishedTypeSQL(windowAlias + "." + secondWinner),
			validationColumn: secondValidation,
			validationSQL: "tupleElement(tupleElement(" + windowAlias + "." +
				secondMeasure + ", 1), 4)",
		},
	}
	projection, projectionErr := fusedChronologicalProjection(
		state,
		next,
		publications,
	)
	if projectionErr != nil {
		return compiledRelation{}, compileState{}, nil, nil, projectionErr
	}
	next.deferredChronologicalValidation = append(
		[]string(nil),
		allValidation...,
	)
	maximumRows := strconv.FormatUint(MaximumStreamStatsInputRows, 10)
	guard := "if(" + windowAlias + "." + inputCount + " > toUInt64(" +
		maximumRows + "), throwIf(toUInt8(1), '" +
		StreamStatsInputLimitMarker + "'), toUInt8(0))"
	resultSQL := "SELECT " + strings.Join(projection, ", ") + " FROM (" +
		windowSQL + ") AS " + windowAlias + " WHERE " + guard + " = 0"
	resultDepth := relation.depth + 3
	enriched := compiledRelation{
		sql:        resultSQL,
		depth:      resultDepth,
		ownerRange: second.Range,
	}

	resultInputName := quoteIdentifier(fmt.Sprintf(
		"__os_streamstats_result_input_%d",
		secondStage,
	))
	barrierName := quoteIdentifier(fmt.Sprintf(
		"__os_streamstats_result_%d",
		secondStage,
	))
	barrierDepth := relationalNodeDepth(resultDepth)
	barrier := &pendingChronologicalBarrier{
		name: barrierName,
		sql:  "SELECT * FROM " + resultInputName,
		prerequisiteDefinitions: []string{
			resultInputName + " AS MATERIALIZED (" + resultSQL + ")",
		},
		validationColumns: allValidation,
		fanout:            1,
		depth:             barrierDepth,
		ownerRange:        second.Range,
	}
	publishedAlias := quoteIdentifier(fmt.Sprintf(
		"__os_streamstats_rows_result_%d",
		secondStage,
	))
	publishedSQL := "SELECT * FROM " + barrierName + " AS " + publishedAlias
	enriched.depth = barrierDepth
	prefixArgs := append(append([]any(nil), firstArgs...), secondArgs...)
	return enriched.selectFrom(publishedSQL, second.Range), next, prefixArgs, barrier, nil
}

// compileStreamAggregate lowers one running count, true-only predicate count,
// numeric sum/average, mixed extremum, or chronological selection over frames in the order already
// established by the pipeline. Its retained relation is capped at one sentinel
// beyond the public limit; row overflow, Dynamic BY poison, and aggregate
// measure poison are forced through the deferred barrier before downstream
// operators can hide them.
func compileStreamAggregate(
	relation compiledRelation,
	operator *plan.StreamAggregate,
	state compileState,
	stage int,
) (compiledRelation, compileState, []any, *pendingChronologicalBarrier, error) {
	output, validateErr := validateStreamAggregate(operator, state)
	if validateErr != nil {
		return compiledRelation{}, compileState{}, nil, nil, validateErr
	}
	function := operator.Measure.Function
	isConditionalCount := function == plan.AggregateFunctionCountPredicate
	isExtrema := function == plan.AggregateFunctionMinimum ||
		function == plan.AggregateFunctionMaximum
	isChronological := function == plan.AggregateFunctionEarliest ||
		function == plan.AggregateFunctionLatest

	orderKeys := append([]compiledSortKey(nil), defaultCompiledOrder(state)...)
	tieBreakers := append([]compiledSortKey(nil), state.tieBreakers...)
	orderProjection := make([]string, 0, len(orderKeys)+len(tieBreakers))
	for index := range orderKeys {
		captured := quoteIdentifier(fmt.Sprintf(
			"__os_streamstats_order_%d_%d",
			stage,
			index,
		))
		orderProjection = append(
			orderProjection,
			orderKeys[index].valueSQL+" AS "+captured,
		)
		orderKeys[index].valueSQL = captured
	}
	for index := range tieBreakers {
		captured := quoteIdentifier(fmt.Sprintf(
			"__os_streamstats_tie_breaker_%d_%d",
			stage,
			index,
		))
		orderProjection = append(
			orderProjection,
			tieBreakers[index].valueSQL+" AS "+captured,
		)
		tieBreakers[index].valueSQL = captured
	}
	orderSQL := ""
	if len(orderKeys) > 0 {
		var orderErr error
		orderSQL, orderErr = compileMaterializedOrder(orderKeys, false)
		if orderErr != nil {
			return compiledRelation{}, compileState{}, nil, nil, fmt.Errorf(
				"compile ClickHouse streamstats order: %w",
				orderErr,
			)
		}
	}

	groupClassifications := make([]string, 0, len(operator.GroupBy))
	groupClassificationAliases := make([]string, 0, len(operator.GroupBy))
	groupAliases := make([]string, 0, len(operator.GroupBy))
	groupPresence := make([]string, 0, len(operator.GroupBy))
	groupUnsupported := make([]string, 0, len(operator.GroupBy))
	groupArgs := make([]any, 0, len(operator.GroupBy)*2)
	for index, group := range operator.GroupBy {
		scalar, compileErr := compileExactScalarGroup(
			group,
			state,
			"streamstats BY",
		)
		if compileErr != nil {
			return compiledRelation{}, compileState{}, nil, nil, compileErr
		}
		classification, classificationArgs := exactScalarGroupClassificationSQL(
			scalar,
		)
		classificationAlias := quoteIdentifier(fmt.Sprintf(
			"__os_streamstats_group_classification_%d",
			index,
		))
		groupAlias := quoteIdentifier(fmt.Sprintf(
			"__os_streamstats_group_%d",
			index,
		))
		groupClassifications = append(
			groupClassifications,
			classification+" AS "+classificationAlias,
		)
		groupClassificationAliases = append(
			groupClassificationAliases,
			classificationAlias,
		)
		groupAliases = append(groupAliases, groupAlias)
		groupPresence = append(
			groupPresence,
			"tupleElement("+classificationAlias+", 2) != 0",
		)
		groupUnsupported = append(
			groupUnsupported,
			"tupleElement("+classificationAlias+", 3) != 0",
		)
		groupArgs = append(groupArgs, classificationArgs...)
	}

	eligibleAlias := quoteIdentifier(fmt.Sprintf(
		"__os_streamstats_eligible_%d",
		stage,
	))
	unsupportedAlias := quoteIdentifier(fmt.Sprintf(
		"__os_streamstats_unsupported_%d",
		stage,
	))
	durableState := state
	predicateState := state
	var predicatePreparation aggregatePredicatePreparation
	if isConditionalCount {
		var preparationErr error
		predicatePreparation, preparationErr = prepareAggregatePredicate(
			state,
			operator.Measure.Predicate,
			stage,
			"streamstats",
		)
		if preparationErr != nil {
			return compiledRelation{}, compileState{}, nil, nil, preparationErr
		}
		durableState = predicatePreparation.durableState
		predicateState = predicatePreparation.predicateState
	}
	measureAlias := ""
	measureProjection := ""
	var stageMeasureArgs []any
	if operator.Measure.Function == plan.AggregateFunctionCountValues ||
		operator.Measure.Function == plan.AggregateFunctionSum ||
		operator.Measure.Function == plan.AggregateFunctionAverage {
		input, exists, resolveErr := resolveCompiledField(operator.Measure.Input, state)
		if resolveErr != nil {
			return compiledRelation{}, compileState{}, nil, nil, resolveErr
		}
		measureSQL := "toUInt64(0)"
		var contributionArgs []any
		switch operator.Measure.Function {
		case plan.AggregateFunctionCountValues:
			if exists {
				measureSQL, contributionArgs = countValueInputSQL(input)
			}
		case plan.AggregateFunctionSum, plan.AggregateFunctionAverage:
			measureSQL = "CAST([], 'Array(Float64)')"
			if exists {
				measureSQL, contributionArgs = numericArrayInputSQL(input)
			}
		}
		measureAlias = quoteIdentifier(fmt.Sprintf(
			"__os_streamstats_measure_%d",
			stage,
		))
		measureProjection = measureSQL + " AS " + measureAlias
		stageMeasureArgs = contributionArgs
	}
	if isConditionalCount {
		predicateSQL, predicateArgs, compileErr := compileExpression(
			operator.Measure.Predicate,
			predicateState,
		)
		if compileErr != nil {
			return compiledRelation{}, compileState{}, nil, nil, compileErr
		}
		measureAlias = quoteIdentifier(fmt.Sprintf(
			"__os_streamstats_measure_%d",
			stage,
		))
		measureProjection = "toUInt64(ifNull(" + predicateSQL + ", 0)) AS " +
			measureAlias
		stageMeasureArgs = predicateArgs
	}

	maximumRows := strconv.FormatUint(MaximumStreamStatsInputRows, 10)
	sentinelRows := strconv.FormatUint(MaximumStreamStatsInputRows+1, 10)
	sourceAlias := quoteIdentifier(fmt.Sprintf(
		"__os_streamstats_source_%d",
		stage,
	))
	orderedInput := "SELECT *"
	if len(groupClassifications) > 0 {
		orderedInput += ", " + strings.Join(groupClassifications, ", ")
	}
	if measureProjection != "" && !isConditionalCount {
		orderedInput += ", " + measureProjection
	}
	if len(orderProjection) > 0 {
		orderedInput += ", " + strings.Join(orderProjection, ", ")
	}
	orderedInput += " FROM (" + relation.sql + ") AS " + sourceAlias
	if orderSQL != "" {
		orderedInput += " ORDER BY " + orderSQL
	}
	orderedInput += " LIMIT " + sentinelRows

	preparedSQL := orderedInput
	preparedLayers := 0
	if len(predicatePreparation.bindings) > 0 {
		// Limit the deterministic input to the public bound plus one sentinel
		// before evaluating calculated predicate producers. A singleton ARRAY
		// JOIN then gives each producer a named dependency that ClickHouse cannot
		// inline back through the predicate without changing row cardinality.
		predicateBindingAlias := quoteIdentifier(fmt.Sprintf(
			"__os_streamstats_predicate_binding_source_%d",
			stage,
		))
		preparedSQL = "SELECT *, " +
			strings.Join(predicatePreparation.boundColumns, ", ") +
			" FROM (" + preparedSQL + ") AS " + predicateBindingAlias +
			" ARRAY JOIN " + strings.Join(predicatePreparation.bindings, ", ")
		preparedLayers++
	}
	if len(predicatePreparation.exactColumns) > 0 {
		// Exact-numeric keys can depend on the singleton aliases above, so keep
		// them in their own post-limit layer before compiling the predicate.
		exactPredicateAlias := quoteIdentifier(fmt.Sprintf(
			"__os_streamstats_predicate_exact_source_%d",
			stage,
		))
		preparedSQL = "SELECT *, " +
			strings.Join(predicatePreparation.exactColumns, ", ") +
			" FROM (" + preparedSQL + ") AS " + exactPredicateAlias
		preparedLayers++
	}
	if len(groupAliases) > 0 {
		preparedProjection := []string{
			"* EXCEPT (" + strings.Join(groupClassificationAliases, ", ") + ")",
		}
		for index, groupAlias := range groupAliases {
			preparedProjection = append(
				preparedProjection,
				"tupleElement("+quoteIdentifier(fmt.Sprintf(
					"__os_streamstats_group_classification_%d",
					index,
				))+", 1) AS "+groupAlias,
			)
		}
		preparedProjection = append(
			preparedProjection,
			"toUInt8("+strings.Join(groupPresence, " AND ")+") AS "+eligibleAlias,
			"toUInt8(("+strings.Join(groupUnsupported, ") OR (")+")) AS "+unsupportedAlias,
		)
		preparedAlias := quoteIdentifier(fmt.Sprintf(
			"__os_streamstats_classified_%d",
			stage,
		))
		preparedSQL = "SELECT " + strings.Join(preparedProjection, ", ") +
			" FROM (" + preparedSQL + ") AS " + preparedAlias
		preparedLayers++
	}
	if isConditionalCount {
		privatePredicateColumns := append(
			append([]string(nil), predicatePreparation.boundColumns...),
			predicatePreparation.exactAliases...,
		)
		predicateProjection := "*"
		if len(privatePredicateColumns) > 0 {
			predicateProjection = "* EXCEPT (" +
				strings.Join(privatePredicateColumns, ", ") + ")"
		}
		predicateAlias := quoteIdentifier(fmt.Sprintf(
			"__os_streamstats_predicate_source_%d",
			stage,
		))
		preparedSQL = "SELECT " + predicateProjection + ", " +
			measureProjection + " FROM (" + preparedSQL + ") AS " + predicateAlias
		preparedLayers++
	}

	outputState := fieldState{
		kind:            fieldKindNumber,
		numberType:      "UInt64",
		numericIntegral: true,
	}
	extremaNullType := ""
	measureValidationSQL := ""
	var extremaScratchColumns []string
	if operator.Measure.Function == plan.AggregateFunctionSum ||
		operator.Measure.Function == plan.AggregateFunctionAverage {
		outputState.numberType = "Float64"
		outputState.numericIntegral = false
	}
	if isExtrema {
		measureAlias = quoteIdentifier(fmt.Sprintf(
			"__os_streamstats_measure_%d",
			stage,
		))
		rowEligibleSQL := "1"
		if len(groupAliases) > 0 {
			rowEligibleSQL = eligibleAlias + " != 0"
		}
		input, exists, resolveErr := resolveCompiledField(operator.Measure.Input, state)
		if resolveErr != nil {
			return compiledRelation{}, compileState{}, nil, nil, resolveErr
		}
		measureSQL := eventStatsExtremaEmptyRowStateSQL("0")
		var measureArgs []any
		outputState = fieldState{
			kind:           fieldKindDynamic,
			dynamicTypeSQL: "dynamicType(" + quoteIdentifier(output.Name) + ")",
			storedTypeSQL: quoteIdentifier(fmt.Sprintf(
				"__os_streamstats_extrema_type_%d",
				stage,
			)),
		}
		if exists {
			outputState.maxStringBytes = fieldStateStringByteBound(input)
			switch input.kind {
			case fieldKindNumber, fieldKindBool, fieldKindTime:
				eligibleSQL, eligibleArgs, fixed := fixedExtremaEligibilitySQL(input)
				if !fixed {
					return compiledRelation{}, compileState{}, nil, nil, errors.New(
						"compile ClickHouse streamstats extrema: fixed input is invalid",
					)
				}
				eligibleSQL = "(" + rowEligibleSQL + ") AND (" + eligibleSQL + ")"
				measureSQL = "tuple(" + input.valueSQL + ", toUInt8(" +
					eligibleSQL + "))"
				measureArgs = eligibleArgs
				outputState = fieldState{
					maxStringBytes:  fieldStateStringByteBound(input),
					kind:            input.kind,
					caseSensitive:   input.caseSensitive,
					numberType:      input.numberType,
					numericSort:     input.numericSort,
					numericIntegral: input.numericIntegral,
				}
				var nullTypeErr error
				extremaNullType, nullTypeErr = nullableEventStatsExtremaType(input)
				if nullTypeErr != nil {
					return compiledRelation{}, compileState{}, nil, nil, nullTypeErr
				}
			case fieldKindString:
				valueAlias := quoteIdentifier(fmt.Sprintf(
					"__os_streamstats_extrema_string_%d",
					stage,
				))
				numberAlias := quoteIdentifier(fmt.Sprintf(
					"__os_streamstats_extrema_number_%d",
					stage,
				))
				valueSQL, valueArgs := statsScalarStringInputSQL(input)
				scalarAlias := quoteIdentifier(fmt.Sprintf(
					"__os_streamstats_extrema_scalar_%d",
					stage,
				))
				preparedSQL = "SELECT *, " + valueSQL + " AS " + valueAlias +
					", " + statsExtremaScalarNumberSQL(valueAlias) + " AS " +
					numberAlias + " FROM (" + preparedSQL + ") AS " + scalarAlias
				preparedLayers++
				extremaScratchColumns = append(
					extremaScratchColumns,
					valueAlias,
					numberAlias,
				)
				measureSQL = "if(" + rowEligibleSQL + ", " +
					statsExtremaScalarCandidateSQL(
						valueAlias,
						numberAlias,
						fixedStringExtremaRawBytesSQL(input),
					) + ", " +
					"tuple(" + eventStatsExtremaEmptyOrderingKeySQL() +
					", toUInt8(" + strconv.Itoa(int(statsExtremaPublicationLexical)) +
					"), toFloat64(0), CAST('' AS String), toUInt8(0)))"
				measureArgs = valueArgs
			case fieldKindDynamic:
				measureSQL, measureArgs = eventStatsExtremaDynamicMeasureSQL(
					operator.Measure.Function,
					input,
					rowEligibleSQL,
				)
				measureValidationSQL = "toUInt8(tupleElement(" + measureAlias + ", 6))"
			case fieldKindStringArray, fieldKindDynamicArray:
				valuesSQL, valuesArgs := stringArrayInputSQL(input)
				measureSQL = streamStatsStringArrayExtremaMeasureSQL(
					operator.Measure.Function,
					valuesSQL,
					rowEligibleSQL,
				)
				measureArgs = valuesArgs
			default:
				return compiledRelation{}, compileState{}, nil, nil, errors.New(
					"compile ClickHouse streamstats extrema: input state is invalid",
				)
			}
		}
		measureStageAlias := quoteIdentifier(fmt.Sprintf(
			"__os_streamstats_measure_source_%d",
			stage,
		))
		measureSourceProjection := "*"
		if len(extremaScratchColumns) > 0 {
			measureSourceProjection = "* EXCEPT (" +
				strings.Join(extremaScratchColumns, ", ") + ")"
		}
		preparedSQL = "SELECT " + measureSourceProjection + ", " + measureSQL + " AS " + measureAlias +
			" FROM (" + preparedSQL + ") AS " + measureStageAlias
		preparedLayers++
		stageMeasureArgs = measureArgs
	}
	if isChronological {
		measureAlias = quoteIdentifier(fmt.Sprintf(
			"__os_streamstats_measure_%d",
			stage,
		))
		rowEligibleSQL := "1"
		if len(groupAliases) > 0 {
			rowEligibleSQL = eligibleAlias + " != 0"
		}
		input, exists, resolveErr := resolveCompiledField(operator.Measure.Input, state)
		if resolveErr != nil {
			return compiledRelation{}, compileState{}, nil, nil, resolveErr
		}
		candidateSQL, candidateArgs, runtimeValidated, candidateErr :=
			singleChronologicalCandidateSQL(
				operator.Measure.Function,
				input,
				exists,
			)
		if candidateErr != nil {
			return compiledRelation{}, compileState{}, nil, nil, candidateErr
		}
		if len(groupAliases) > 0 {
			candidateSQL = "if(" + rowEligibleSQL + ", " + candidateSQL +
				", " + emptySingleChronologicalCandidateSQL() + ")"
		}
		measureSQL := "tuple(" + candidateSQL + ", " +
			immutableChronologicalRowKeySQL() + ")"
		measureStageAlias := quoteIdentifier(fmt.Sprintf(
			"__os_streamstats_measure_source_%d",
			stage,
		))
		preparedSQL = "SELECT *, " + measureSQL + " AS " + measureAlias +
			" FROM (" + preparedSQL + ") AS " + measureStageAlias
		preparedLayers++
		stageMeasureArgs = candidateArgs
		outputState = fieldState{
			kind:           fieldKindDynamic,
			dynamicTypeSQL: "dynamicType(" + quoteIdentifier(output.Name) + ")",
			storedTypeSQL: quoteIdentifier(fmt.Sprintf(
				"__os_streamstats_chronological_type_%d",
				stage,
			)),
		}
		if exists {
			outputState.maxStringBytes = fieldStateStringByteBound(input)
		}
		if runtimeValidated {
			measureValidationSQL = "toUInt8(tupleElement(tupleElement(" +
				measureAlias + ", 1), 4))"
		}
	}

	// Extrema and chronological projections are textually outside the bounded BY
	// classification subquery, so its placeholders precede group arguments.
	prefixArgs := make([]any, 0, len(groupArgs)+len(stageMeasureArgs))
	if isConditionalCount || isExtrema || isChronological {
		prefixArgs = append(prefixArgs, stageMeasureArgs...)
		prefixArgs = append(prefixArgs, groupArgs...)
	} else {
		prefixArgs = append(prefixArgs, groupArgs...)
		prefixArgs = append(prefixArgs, stageMeasureArgs...)
	}

	inputName := quoteIdentifier(fmt.Sprintf(
		"__os_streamstats_input_%d",
		stage,
	))
	inputCount := quoteIdentifier(fmt.Sprintf(
		"__os_streamstats_input_count_%d",
		stage,
	))
	windowValue := quoteIdentifier(fmt.Sprintf(
		"__os_streamstats_value_%d",
		stage,
	))
	windowParts := make([]string, 0, 3)
	if len(groupAliases) > 0 {
		partition := append([]string{eligibleAlias}, groupAliases...)
		windowParts = append(
			windowParts,
			"PARTITION BY "+strings.Join(partition, ", "),
		)
	}
	if orderSQL != "" {
		windowParts = append(windowParts, "ORDER BY "+orderSQL)
	} else {
		// A supported relation without order keys is a global aggregate and has
		// at most one row. Pin a syntactically complete window order without
		// pretending that a wider unordered relation is deterministic.
		windowParts = append(windowParts, "ORDER BY tuple()")
	}
	windowParts = append(
		windowParts,
		streamStatsFrameSQL(operator.IncludeCurrent, operator.WindowRows),
	)
	windowClause := strings.Join(windowParts, " ")
	windowExpression := "count() OVER (" + windowClause + ")"
	switch operator.Measure.Function {
	case plan.AggregateFunctionCountValues, plan.AggregateFunctionCountPredicate:
		windowExpression = "toUInt64(ifNull(sum(toUInt128(" + measureAlias +
			")) OVER (" + windowClause + "), toUInt128(0)))"
	case plan.AggregateFunctionSum, plan.AggregateFunctionAverage:
		numericAggregate, supported := numericArrayAggregateSQL(
			operator.Measure.Function,
			measureAlias,
		)
		if !supported {
			return compiledRelation{}, compileState{}, nil, nil, errors.New(
				"compile ClickHouse streamstats: numeric aggregate is unsupported",
			)
		}
		windowExpression = "CAST(" + numericAggregate + " OVER (" +
			windowClause + ") AS Nullable(Float64))"
	case plan.AggregateFunctionMinimum, plan.AggregateFunctionMaximum:
		if extremaNullType != "" {
			aggregateName := "minIfOrNull"
			if operator.Measure.Function == plan.AggregateFunctionMaximum {
				aggregateName = "maxIfOrNull"
			}
			windowExpression = aggregateName + "(tupleElement(" + measureAlias +
				", 1), tupleElement(" + measureAlias + ", 2) != 0) OVER (" +
				windowClause + ")"
		} else {
			windowExpression = statsExtremaScalarAggregateWinnerSQL(
				operator.Measure.Function,
				measureAlias,
			) + " OVER (" + windowClause + ")"
		}
	case plan.AggregateFunctionEarliest, plan.AggregateFunctionLatest:
		chronologicalAggregate, aggregateErr := singleChronologicalAggregateSQL(
			operator.Measure.Function,
			measureAlias,
		)
		if aggregateErr != nil {
			return compiledRelation{}, compileState{}, nil, nil, aggregateErr
		}
		windowExpression = chronologicalAggregate + " OVER (" + windowClause + ")"
	default:
		if !operator.IncludeCurrent {
			windowExpression = "ifNull(" + windowExpression + ", toUInt64(0))"
		}
	}

	windowProjection := []string{
		"*",
		"count() OVER () AS " + inputCount,
		windowExpression + " AS " + windowValue,
	}
	unsupportedAny := ""
	if len(groupAliases) > 0 {
		unsupportedAny = quoteIdentifier(fmt.Sprintf(
			"__os_streamstats_any_unsupported_%d",
			stage,
		))
		windowProjection = append(
			windowProjection,
			"max(toUInt8("+unsupportedAlias+" != 0)) OVER () AS "+unsupportedAny,
		)
	}
	validationColumn := ""
	if measureValidationSQL != "" {
		validationColumn = quoteIdentifier(fmt.Sprintf(
			"__os_streamstats_validation_%d",
			stage,
		))
		windowProjection = append(
			windowProjection,
			measureValidationSQL+" AS "+validationColumn,
		)
	}
	windowAlias := quoteIdentifier(fmt.Sprintf(
		"__os_streamstats_window_%d",
		stage,
	))
	windowSource := inputName
	if measureValidationSQL != "" {
		preparedAlias := quoteIdentifier(fmt.Sprintf(
			"__os_streamstats_prepared_%d",
			stage,
		))
		windowSource = "(" + preparedSQL + ") AS " + preparedAlias
	}
	windowSQL := "SELECT " + strings.Join(windowProjection, ", ") +
		" FROM " + windowSource

	next := streamAggregateCompileState(
		durableState,
		output,
		outputState,
		len(groupAliases) > 0,
		stage,
		orderKeys,
		tieBreakers,
	)
	rawOutputValue := windowAlias + "." + windowValue
	outputValue := rawOutputValue
	outputStoredType := ""
	usesDynamicExtrema := isExtrema && extremaNullType == ""
	if usesDynamicExtrema {
		outputValue = statsExtremaScalarValueSQL(outputValue)
		outputStoredType = statsExtremaScalarStoredTypeSQL(
			rawOutputValue,
		)
	}
	if isChronological {
		outputValue = chronologicalPublishedValueSQL(rawOutputValue)
		outputStoredType = chronologicalPublishedTypeSQL(rawOutputValue)
	}
	outputExists := "1"
	if len(groupAliases) > 0 {
		outputExists = windowAlias + "." + eligibleAlias + " != 0"
		nullType := "UInt64"
		if operator.Measure.Function == plan.AggregateFunctionSum ||
			operator.Measure.Function == plan.AggregateFunctionAverage {
			nullType = "Float64"
		}
		if isExtrema {
			if extremaNullType != "" {
				nullType = extremaNullType
				outputValue = "if(" + outputExists + ", " + outputValue +
					", CAST(NULL AS Nullable(" + nullType + ")))"
			}
		}
		if usesDynamicExtrema || isChronological {
			outputValue = "if(" + outputExists + ", " + outputValue +
				", CAST(NULL AS Dynamic))"
			outputStoredType = "if(" + outputExists + ", " +
				outputStoredType + ", toUInt8(" +
				strconv.Itoa(int(eventfields.StoredValueTypeNull)) + "))"
		} else if !isExtrema {
			outputValue = "if(" + outputExists + ", " + outputValue +
				", CAST(NULL AS Nullable(" + nullType + ")))"
		}
	}
	outputValidation := ""
	if validationColumn != "" {
		outputValidation = windowAlias + "." + validationColumn
	}
	projection := eventAggregateProjection(
		durableState,
		next,
		output.Name,
		outputValue,
		outputStoredType,
		outputExists,
		validationColumn,
		outputValidation,
		windowAlias,
	)
	guard := "if(" + windowAlias + "." + inputCount + " > toUInt64(" +
		maximumRows + "), throwIf(toUInt8(1), '" +
		StreamStatsInputLimitMarker + "'), toUInt8(0))"
	if unsupportedAny != "" {
		guard = "if(" + windowAlias + "." + inputCount + " > toUInt64(" +
			maximumRows + "), throwIf(toUInt8(1), '" +
			StreamStatsInputLimitMarker + "'), if(" + windowAlias + "." +
			unsupportedAny + " != 0, throwIf(toUInt8(1), '" +
			UnsupportedStatsByValueMarker + "'), toUInt8(0)))"
	}
	resultSQL := "SELECT " + strings.Join(projection, ", ") + " FROM (" +
		windowSQL + ") AS " + windowAlias + " WHERE " + guard + " = 0"

	depth := relation.depth + 3 + preparedLayers
	enriched := compiledRelation{
		sql:        resultSQL,
		depth:      depth,
		ownerRange: operator.Range,
	}
	barrierName := quoteIdentifier(fmt.Sprintf(
		"__os_streamstats_result_%d",
		stage,
	))
	barrierSQL := resultSQL
	barrierDepth := depth
	prerequisiteDefinitions := []string{
		inputName + " AS MATERIALIZED (" + preparedSQL + ")",
	}
	if validationColumn != "" {
		resultInputName := quoteIdentifier(fmt.Sprintf(
			"__os_streamstats_result_input_%d",
			stage,
		))
		barrierSQL = "SELECT * FROM " + resultInputName
		barrierDepth = relationalNodeDepth(depth)
		prerequisiteDefinitions = []string{
			resultInputName + " AS MATERIALIZED (" + resultSQL + ")",
		}
		enriched.depth = barrierDepth
	}
	barrier := &pendingChronologicalBarrier{
		name:                    barrierName,
		sql:                     barrierSQL,
		prerequisiteDefinitions: prerequisiteDefinitions,
		fanout:                  1,
		depth:                   barrierDepth,
		ownerRange:              operator.Range,
	}
	if validationColumn != "" {
		barrier.validationColumns = []string{validationColumn}
	}
	publishedAlias := quoteIdentifier(fmt.Sprintf(
		"__os_streamstats_rows_result_%d",
		stage,
	))
	publishedSQL := "SELECT * FROM " + barrierName + " AS " + publishedAlias
	if validationColumn != "" {
		publishedSQL = "SELECT * EXCEPT (" + validationColumn + ") FROM " +
			barrierName + " AS " + publishedAlias
	}
	return enriched.selectFrom(publishedSQL, operator.Range), next, prefixArgs, barrier, nil
}

func eventAggregateMeasureSpecFor(
	measure plan.AggregateMeasure,
) (eventAggregateMeasureSpec, error) {
	spec := eventAggregateMeasureSpec{
		function:        measure.Function,
		percentile:      measure.Percentile,
		numberType:      "UInt64",
		numericIntegral: true,
	}
	switch measure.Function {
	case plan.AggregateFunctionCountRows:
		return spec, nil
	case plan.AggregateFunctionCountValues,
		plan.AggregateFunctionCountPredicate:
		spec.materialized = true
		spec.valuePrefix = "__os_eventstats_value_count_"
		return spec, nil
	case plan.AggregateFunctionDistinctCount:
		spec.materialized = true
		spec.valuePrefix = "__os_eventstats_value_dc_"
		return spec, nil
	case plan.AggregateFunctionValues:
		spec.materialized = true
		spec.numberType = ""
		spec.numericIntegral = false
		spec.valuePrefix = "__os_eventstats_value_values_"
		return spec, nil
	case plan.AggregateFunctionList:
		spec.materialized = true
		spec.numberType = ""
		spec.numericIntegral = false
		spec.valuePrefix = "__os_eventstats_value_list_"
		return spec, nil
	case plan.AggregateFunctionPercentile:
		if measure.Percentile < 1 || measure.Percentile > 99 {
			return eventAggregateMeasureSpec{}, fmt.Errorf(
				"compile ClickHouse eventstats: invalid percentile level %d",
				measure.Percentile,
			)
		}
		spec.materialized = true
		spec.numberType = "Float64"
		spec.numericIntegral = false
		spec.valuePrefix = "__os_eventstats_value_percentile_"
		return spec, nil
	case plan.AggregateFunctionSum, plan.AggregateFunctionAverage:
		spec.materialized = true
		spec.numberType = "Float64"
		spec.numericIntegral = false
		if measure.Function == plan.AggregateFunctionSum {
			spec.valuePrefix = "__os_eventstats_value_sum_"
		} else {
			spec.valuePrefix = "__os_eventstats_value_avg_"
		}
		return spec, nil
	case plan.AggregateFunctionMinimum, plan.AggregateFunctionMaximum:
		spec.materialized = true
		spec.numberType = ""
		spec.numericIntegral = false
		if measure.Function == plan.AggregateFunctionMinimum {
			spec.valuePrefix = "__os_eventstats_value_min_"
		} else {
			spec.valuePrefix = "__os_eventstats_value_max_"
		}
		return spec, nil
	case plan.AggregateFunctionEarliest, plan.AggregateFunctionLatest:
		spec.materialized = true
		spec.numberType = ""
		spec.numericIntegral = false
		if measure.Function == plan.AggregateFunctionEarliest {
			spec.valuePrefix = "__os_eventstats_value_earliest_"
		} else {
			spec.valuePrefix = "__os_eventstats_value_latest_"
		}
		return spec, nil
	default:
		return eventAggregateMeasureSpec{}, fmt.Errorf(
			"compile ClickHouse eventstats: unsupported function %d",
			measure.Function,
		)
	}
}

func (spec eventAggregateMeasureSpec) aggregateSQL(
	inputSQL string,
) (string, error) {
	switch spec.function {
	case plan.AggregateFunctionCountValues,
		plan.AggregateFunctionCountPredicate:
		return "toUInt64(sum(toUInt128(" + inputSQL + ")))", nil
	case plan.AggregateFunctionDistinctCount:
		return distinctCountCardinalitySQL(
			"tupleElement(" + inputSQL + ", 1)",
		), nil
	case plan.AggregateFunctionValues:
		return exactDistinctStringSetSQL(
			"tupleElement("+inputSQL+", 1)",
			uint64(MaximumStatsValuesPerGroup),
		), nil
	case plan.AggregateFunctionList:
		return boundedOrderedStringListSQL(
			"tupleElement(" + inputSQL + ", 1)",
		), nil
	case plan.AggregateFunctionPercentile:
		return singlePercentileArrayAggregateSQL(spec.percentile, inputSQL), nil
	case plan.AggregateFunctionSum, plan.AggregateFunctionAverage:
		if sql, supported := numericArrayAggregateSQL(spec.function, inputSQL); supported {
			return sql, nil
		}
	}
	return "", fmt.Errorf(
		"compile ClickHouse eventstats: function %d has no materialized measure",
		spec.function,
	)
}

func nullableEventStatsExtremaType(field fieldState) (string, error) {
	switch field.kind {
	case fieldKindNumber:
		if field.numberType != "" {
			return field.numberType, nil
		}
	case fieldKindBool:
		return "Bool", nil
	case fieldKindTime:
		if field.numberType != "" {
			return field.numberType, nil
		}
	}
	return "", fmt.Errorf(
		"compile ClickHouse eventstats extrema: fixed input has unsupported type %d/%q",
		field.kind,
		field.numberType,
	)
}

// fixedExtremaEligibilitySQL is the common row contract for native extrema.
// Keeping fixed Number, Bool, and Time values in their physical type avoids a
// lossy String/Float64 round trip; only present, non-null values participate,
// and non-finite floating-point numbers are omitted.
func fixedExtremaEligibilitySQL(field fieldState) (string, []any, bool) {
	switch field.kind {
	case fieldKindNumber, fieldKindBool, fieldKindTime:
	default:
		return "", nil, false
	}
	existsSQL := field.existsSQL
	if existsSQL == "" {
		existsSQL = "1"
	}
	eligibleSQL := "(" + existsSQL + ") AND isNotNull(" + field.valueSQL + ")"
	if field.kind == fieldKindNumber && strings.HasPrefix(field.numberType, "Float") {
		eligibleSQL += " AND isFinite(" + field.valueSQL + ")"
	}
	return eligibleSQL, append([]any(nil), field.existsArgs...), true
}

func compileEventAggregate(
	relation compiledRelation,
	operator *plan.EventAggregate,
	state compileState,
	stage int,
) (compiledRelation, compileState, []any, *pendingChronologicalBarrier, error) {
	output, validateErr := validateEventAggregate(operator, state)
	if validateErr != nil {
		return compiledRelation{}, compileState{}, nil, nil, validateErr
	}
	if state.eventRows && state.allowDynamic && output.Name == "fields" {
		return compiledRelation{}, compileState{}, nil, nil, &plan.Diagnostic{
			Code: "SPL_AMBIGUOUS_EVENTSTATS_FIELD",
			Message: "eventstats cannot replace the event result's " +
				"reserved fields payload without an exact upstream schema",
			Range: output.Range,
		}
	}

	measure := operator.Measure
	measureSpec, specErr := eventAggregateMeasureSpecFor(measure)
	if specErr != nil {
		return compiledRelation{}, compileState{}, nil, nil, specErr
	}
	measureAggregateSQL := measureSpec.aggregateSQL
	measurePublishValueSQL := func(valueSQL string) string { return valueSQL }
	measurePublishTypeSQL := func(string) string { return "" }
	measureNullSQL := ""
	if measureSpec.numberType != "" {
		measureNullSQL = "CAST(NULL AS Nullable(" + measureSpec.numberType + "))"
	}
	outputState := fieldState{
		kind:            fieldKindNumber,
		numberType:      measureSpec.numberType,
		numericIntegral: measureSpec.numericIntegral,
	}
	var measureInputColumns []string
	measureInputSQL := ""
	var measureInputArgs []any
	var measureValidationSQL func(string, string) string
	measureUsesValuesValidation := false
	listInputExists := false
	measureUsesGroupEligibility := false
	var eventStatsPrerequisiteDefinitions []string
	prefixArgumentsAfterExisting := false
	durableState := state
	switch measure.Function {
	case plan.AggregateFunctionCountPredicate:
		sentinelRows := strconv.FormatUint(MaximumEventStatsInputRows+1, 10)
		predicatePreparation, preparationErr := prepareAggregatePredicate(
			state,
			measure.Predicate,
			stage,
			"eventstats",
		)
		if preparationErr != nil {
			return compiledRelation{}, compileState{}, nil, nil, preparationErr
		}
		durableState = predicatePreparation.durableState
		predicateState := predicatePreparation.predicateState
		predicateColumns := append(
			append([]string(nil), predicatePreparation.boundColumns...),
			predicatePreparation.exactColumns...,
		)
		if len(predicateColumns) > 0 {
			materialized := quoteIdentifier(fmt.Sprintf(
				"__os_eventstats_predicate_input_%d",
				stage,
			))
			alias := quoteIdentifier(fmt.Sprintf("__os_eventstats_predicate_rows_%d", stage))
			predicateSQL := "SELECT *, " + strings.Join(predicateColumns, ", ") +
				" FROM (" + relation.sql + ") AS " + alias
			if len(predicatePreparation.bindings) > 0 {
				predicateSQL += " ARRAY JOIN " + strings.Join(predicatePreparation.bindings, ", ")
			}
			predicateSQL += " LIMIT " + sentinelRows
			eventStatsPrerequisiteDefinitions = append(
				eventStatsPrerequisiteDefinitions,
				materialized+" AS MATERIALIZED ("+predicateSQL+")",
			)
			relation = relation.selectFrom(
				"SELECT * FROM "+materialized,
				operator.Range,
			)
			// Hoisting moves the predicate fence ahead of the eventstats input
			// definition. Its already-compiled relation arguments must therefore
			// precede the predicate/group arguments introduced by this stage.
			prefixArgumentsAfterExisting = true
			if err := validateRelationalDepth(relation.depth, relation.ownerRange); err != nil {
				return compiledRelation{}, compileState{}, nil, nil, err
			}
		}
		predicateSQL, predicateArgs, compileErr := compileExpression(
			measure.Predicate,
			predicateState,
		)
		if compileErr != nil {
			return compiledRelation{}, compileState{}, nil, nil, compileErr
		}
		measureInputSQL = "toUInt64(ifNull(" + predicateSQL + ", 0))"
		measureInputArgs = predicateArgs
	case plan.AggregateFunctionCountValues, plan.AggregateFunctionPercentile,
		plan.AggregateFunctionSum,
		plan.AggregateFunctionAverage, plan.AggregateFunctionMinimum,
		plan.AggregateFunctionMaximum, plan.AggregateFunctionEarliest,
		plan.AggregateFunctionLatest, plan.AggregateFunctionDistinctCount,
		plan.AggregateFunctionValues, plan.AggregateFunctionList:
		if durableState.eventRows && durableState.allowDynamic && measure.Input.Name == "fields" {
			return compiledRelation{}, compileState{}, nil, nil, &plan.Diagnostic{
				Code: "SPL_AMBIGUOUS_EVENTSTATS_FIELD",
				Message: "eventstats cannot read the event result's " +
					"reserved fields payload without an exact upstream schema",
				Range: measure.Input.Range,
			}
		}
		input, exists, resolveErr := resolveCompiledField(measure.Input, durableState)
		if resolveErr != nil {
			return compiledRelation{}, compileState{}, nil, nil, resolveErr
		}
		switch measure.Function {
		case plan.AggregateFunctionCountValues:
			measureInputSQL = "toUInt64(0)"
			if exists {
				measureInputSQL, measureInputArgs = countValueInputSQL(input)
			}
		case plan.AggregateFunctionPercentile, plan.AggregateFunctionSum,
			plan.AggregateFunctionAverage:
			measureInputSQL = "CAST([], 'Array(Float64)')"
			if exists {
				measureInputSQL, measureInputArgs = numericArrayInputSQL(input)
			}
		case plan.AggregateFunctionDistinctCount, plan.AggregateFunctionValues,
			plan.AggregateFunctionList:
			emptyValues := "CAST([], 'Array(String)')"
			measureInputSQL = "tuple(" + emptyValues + ", toUInt8(0))"
			if exists {
				if input.kind == fieldKindDynamic {
					rowEligibleSQL := "1"
					if len(operator.GroupBy) > 0 {
						rowEligibleSQL = quoteIdentifier(fmt.Sprintf(
							"__os_eventstats_eligible_%d",
							stage,
						)) + " != 0"
						measureUsesGroupEligibility = true
					}
					measureInputSQL, measureInputArgs =
						eventStatsExactStringDynamicMeasureSQL(
							input,
							rowEligibleSQL,
						)
				} else {
					valuesSQL, valuesArgs := stringArrayInputSQL(input)
					measureInputSQL = "tuple(" + valuesSQL + ", toUInt8(0))"
					measureInputArgs = valuesArgs
				}
			}
			switch measure.Function {
			case plan.AggregateFunctionDistinctCount:
				measureValidationSQL = eventStatsDistinctCountValidationSQL
			case plan.AggregateFunctionValues:
				measureUsesValuesValidation = true
				measureNullSQL = emptyValues
				outputState = fieldState{
					kind:                  fieldKindStringArray,
					mvSortedLexicographic: true,
					stringOrBytes:         true,
				}
			case plan.AggregateFunctionList:
				listInputExists = exists
				measureNullSQL = emptyValues
				outputState = fieldState{
					kind:          fieldKindStringArray,
					stringOrBytes: true,
				}
			default:
				return compiledRelation{}, compileState{}, nil, nil, fmt.Errorf(
					"compile ClickHouse eventstats: exact-string function %d has no publication contract",
					measure.Function,
				)
			}
		case plan.AggregateFunctionMinimum, plan.AggregateFunctionMaximum:
			extremaFunction := measure.Function
			outputState = fieldState{
				kind:           fieldKindDynamic,
				dynamicTypeSQL: "dynamicType(" + quoteIdentifier(output.Name) + ")",
			}
			measureNullSQL = "CAST(NULL AS Dynamic)"
			measurePublishTypeSQL = statsExtremaStoredTypeSQL
			if exists {
				outputState.maxStringBytes = fieldStateStringByteBound(input)
			}
			switch {
			case exists && (input.kind == fieldKindNumber ||
				input.kind == fieldKindBool || input.kind == fieldKindTime):
				eligibleSQL, eligibleArgs, fixed := fixedExtremaEligibilitySQL(input)
				if !fixed {
					return compiledRelation{}, compileState{}, nil, nil, errors.New(
						"compile ClickHouse eventstats extrema: fixed input is invalid",
					)
				}
				measureInputSQL = "tuple(" + input.valueSQL + ", toUInt8(" +
					eligibleSQL + "))"
				measureInputArgs = eligibleArgs
				measureAggregateSQL = func(inputSQL string) (string, error) {
					aggregateName := "minIfOrNull"
					if extremaFunction == plan.AggregateFunctionMaximum {
						aggregateName = "maxIfOrNull"
					}
					return aggregateName + "(tupleElement(" + inputSQL +
						", 1), tupleElement(" + inputSQL + ", 2) != 0)", nil
				}
				outputState = fieldState{
					maxStringBytes:  fieldStateStringByteBound(input),
					kind:            input.kind,
					caseSensitive:   input.caseSensitive,
					numberType:      input.numberType,
					numericSort:     input.numericSort,
					numericIntegral: input.numericIntegral,
				}
				nullType, nullTypeErr := nullableEventStatsExtremaType(input)
				if nullTypeErr != nil {
					return compiledRelation{}, compileState{}, nil, nil, nullTypeErr
				}
				measureNullSQL = "CAST(NULL AS Nullable(" + nullType + "))"
				measurePublishTypeSQL = func(string) string { return "" }
			case exists && input.kind == fieldKindString:
				valueAlias := quoteIdentifier(fmt.Sprintf(
					"__os_eventstats_extrema_string_%d",
					stage,
				))
				numberAlias := quoteIdentifier(fmt.Sprintf(
					"__os_eventstats_extrema_number_%d",
					stage,
				))
				valueSQL, valueArgs := statsScalarStringInputSQL(input)
				measureInputColumns = append(
					measureInputColumns,
					valueSQL+" AS "+valueAlias,
					statsExtremaScalarNumberSQL(valueAlias)+" AS "+numberAlias,
				)
				measureInputArgs = valueArgs
				measureInputSQL = statsExtremaScalarCandidateSQL(
					valueAlias,
					numberAlias,
					fixedStringExtremaRawBytesSQL(input),
				)
				measureAggregateSQL = func(inputSQL string) (string, error) {
					return statsExtremaScalarAggregateWinnerSQL(
						extremaFunction,
						inputSQL,
					), nil
				}
				measurePublishValueSQL = statsExtremaScalarValueSQL
				measurePublishTypeSQL = statsExtremaScalarStoredTypeSQL
			case exists && input.kind == fieldKindDynamic:
				rowEligibleSQL := "1"
				if len(operator.GroupBy) > 0 {
					rowEligibleSQL = quoteIdentifier(fmt.Sprintf(
						"__os_eventstats_eligible_%d",
						stage,
					)) + " != 0"
					measureUsesGroupEligibility = true
				}
				measureInputSQL, measureInputArgs =
					eventStatsExtremaDynamicMeasureSQL(
						extremaFunction,
						input,
						rowEligibleSQL,
					)
				measureAggregateSQL = func(inputSQL string) (string, error) {
					return statsExtremaScalarAggregateWinnerSQL(
						extremaFunction,
						inputSQL,
					), nil
				}
				measurePublishValueSQL = statsExtremaScalarValueSQL
				measurePublishTypeSQL = statsExtremaScalarStoredTypeSQL
				measureValidationSQL = func(inputSQL, _ string) string {
					return "maxOrDefault(toUInt8(tupleElement(" + inputSQL +
						", 6)))"
				}
			default:
				valuesSQL := "CAST([], 'Array(String)')"
				if exists {
					valuesSQL, measureInputArgs = stringArrayInputSQL(input)
				}
				measureInputSQL = statsExtremaCandidatesSQL(valuesSQL)
				measureAggregateSQL = func(inputSQL string) (string, error) {
					return statsExtremaAggregateSQL(
						extremaFunction,
						inputSQL,
					), nil
				}
			}
		case plan.AggregateFunctionEarliest, plan.AggregateFunctionLatest:
			chronologicalFunction := measure.Function
			candidateSQL, candidateArgs, runtimeValidated, candidateErr :=
				singleChronologicalCandidateSQL(
					chronologicalFunction,
					input,
					exists,
				)
			if candidateErr != nil {
				return compiledRelation{}, compileState{}, nil, nil, candidateErr
			}
			if len(operator.GroupBy) > 0 {
				rowEligibleSQL := quoteIdentifier(fmt.Sprintf(
					"__os_eventstats_eligible_%d",
					stage,
				)) + " != 0"
				candidateSQL = "if(" + rowEligibleSQL + ", " + candidateSQL +
					", " + emptySingleChronologicalCandidateSQL() + ")"
				measureUsesGroupEligibility = true
			}
			measureInputSQL = "tuple(" + candidateSQL + ", " +
				immutableChronologicalRowKeySQL() + ")"
			measureInputArgs = candidateArgs
			measureAggregateSQL = func(inputSQL string) (string, error) {
				return singleChronologicalAggregateSQL(
					chronologicalFunction,
					inputSQL,
				)
			}
			outputState = fieldState{
				kind:           fieldKindDynamic,
				dynamicTypeSQL: "dynamicType(" + quoteIdentifier(output.Name) + ")",
			}
			if exists {
				outputState.maxStringBytes = fieldStateStringByteBound(input)
			}
			measureNullSQL = "CAST(NULL AS Dynamic)"
			measurePublishValueSQL = chronologicalPublishedValueSQL
			measurePublishTypeSQL = chronologicalPublishedTypeSQL
			if runtimeValidated {
				measureValidationSQL = eventStatsChronologicalValidationSQL
			}
		}
	}

	groups := make([]compiledEventStatsGroup, 0, len(operator.GroupBy))
	seenGroups := make(map[string]struct{}, len(operator.GroupBy))
	for index, group := range operator.GroupBy {
		if validateErr := validateCanonicalFieldRef("eventstats", "group", group); validateErr != nil {
			return compiledRelation{}, compileState{}, nil, nil, validateErr
		}
		if _, duplicate := seenGroups[group.Name]; duplicate {
			return compiledRelation{}, compileState{}, nil, nil, fmt.Errorf(
				"compile ClickHouse eventstats: grouping field %q is repeated",
				group.Name,
			)
		}
		seenGroups[group.Name] = struct{}{}
		if durableState.eventRows && durableState.allowDynamic && group.Name == "fields" {
			return compiledRelation{}, compileState{}, nil, nil, &plan.Diagnostic{
				Code:    "SPL_AMBIGUOUS_EVENTSTATS_FIELD",
				Message: "eventstats cannot group by the event result's reserved fields payload without an exact upstream schema",
				Range:   group.Range,
			}
		}

		scalar, compileErr := compileExactScalarGroup(group, durableState, "eventstats BY")
		if compileErr != nil {
			return compiledRelation{}, compileState{}, nil, nil, compileErr
		}
		groups = append(groups, compiledEventStatsGroup{
			scalar:   scalar,
			keyAlias: quoteIdentifier(fmt.Sprintf("__os_eventstats_group_%d", index)),
		})
	}
	if outputState.kind == fieldKindDynamic {
		outputState.storedTypeSQL = quoteIdentifier(fmt.Sprintf(
			"__os_eventstats_extrema_type_%d",
			stage,
		))
	}
	next := eventAggregateCompileState(
		durableState,
		output,
		outputState,
		len(groups) > 0,
		stage,
	)
	inputName := quoteIdentifier(fmt.Sprintf("__os_eventstats_input_%d", stage))
	totalName := quoteIdentifier(fmt.Sprintf("__os_eventstats_total_%d", stage))
	inputAlias := quoteIdentifier(fmt.Sprintf("__os_eventstats_rows_%d", stage))
	totalAlias := quoteIdentifier(fmt.Sprintf("__os_eventstats_total_row_%d", stage))
	totalColumn := quoteIdentifier(fmt.Sprintf("__os_eventstats_input_count_%d", stage))
	validationColumn := ""
	if measureValidationSQL != nil || measureUsesValuesValidation ||
		listInputExists {
		validationColumn = quoteIdentifier(fmt.Sprintf(
			"__os_eventstats_validation_%d",
			stage,
		))
	}
	maximumRows := strconv.FormatUint(MaximumEventStatsInputRows, 10)
	sentinelRows := strconv.FormatUint(MaximumEventStatsInputRows+1, 10)
	windowedDynamicExtrema := (measure.Function == plan.AggregateFunctionMinimum ||
		measure.Function == plan.AggregateFunctionMaximum) &&
		outputState.kind == fieldKindDynamic && measureValidationSQL != nil

	inputProjection := []string{"*"}
	classificationProjection := []string{"*"}
	var classificationArgs []any
	measureAlias := ""
	if measureSpec.materialized {
		measureAlias = quoteIdentifier(fmt.Sprintf("__os_eventstats_measure_%d", stage))
		inputProjection = append(inputProjection, measureInputColumns...)
		if !measureUsesGroupEligibility {
			inputProjection = append(
				inputProjection,
				measureInputSQL+" AS "+measureAlias,
			)
		}
	}
	var eligibilityArgs, unsupportedArgs []any
	eligibility := make([]string, 0, len(groups))
	unsupported := make([]string, 0, len(groups))
	for _, group := range groups {
		if windowedDynamicExtrema {
			classification, args := exactScalarGroupClassificationSQL(group.scalar)
			classificationProjection = append(
				classificationProjection,
				classification+" AS "+group.keyAlias,
			)
			classificationArgs = append(classificationArgs, args...)
			eligibility = append(
				eligibility,
				"tupleElement("+group.keyAlias+", 2) != 0",
			)
			unsupported = append(
				unsupported,
				"tupleElement("+group.keyAlias+", 3) != 0",
			)
			continue
		}
		inputProjection = append(
			inputProjection,
			group.scalar.keySQL+" AS "+group.keyAlias,
		)
		eligibility = append(eligibility, group.scalar.presenceSQL)
		eligibilityArgs = append(eligibilityArgs, group.scalar.presenceArgs...)
		if group.scalar.unsupportedSQL != "" {
			unsupported = append(unsupported, group.scalar.unsupportedSQL)
			unsupportedArgs = append(unsupportedArgs, group.scalar.unsupportedArgs...)
		}
	}
	eligibleAlias := quoteIdentifier(fmt.Sprintf("__os_eventstats_eligible_%d", stage))
	unsupportedAlias := quoteIdentifier(fmt.Sprintf("__os_eventstats_unsupported_%d", stage))
	if len(groups) > 0 {
		inputProjection = append(
			inputProjection,
			"toUInt8("+strings.Join(eligibility, " AND ")+") AS "+eligibleAlias,
		)
		unsupportedSQL := "0"
		if len(unsupported) > 0 {
			unsupportedSQL = "(" + strings.Join(unsupported, ") OR (") + ")"
		}
		inputProjection = append(
			inputProjection,
			"toUInt8("+unsupportedSQL+") AS "+unsupportedAlias,
		)
	}
	if measureSpec.materialized && measureUsesGroupEligibility {
		// Keep the BY eligibility alias textually before the Dynamic fold that it
		// guards. ClickHouse aliases are visible throughout a SELECT projection;
		// the pinned integration suite proves that this reference also preserves
		// short-circuit traversal for incomplete group rows.
		inputProjection = append(
			inputProjection,
			measureInputSQL+" AS "+measureAlias,
		)
	}

	inputSourceAlias := quoteIdentifier(fmt.Sprintf("__os_eventstats_source_%d", stage))
	inputSourceSQL := relation.sql
	inputLimitSQL := " LIMIT " + sentinelRows
	if windowedDynamicExtrema && len(groups) > 0 {
		classificationAlias := quoteIdentifier(fmt.Sprintf(
			"__os_eventstats_group_source_%d",
			stage,
		))
		inputSourceSQL = "SELECT " + strings.Join(classificationProjection, ", ") +
			" FROM (" + relation.sql + ") AS " + inputSourceAlias +
			" LIMIT " + sentinelRows
		inputSourceAlias = classificationAlias
		inputLimitSQL = ""
	}
	inputSQL := "SELECT " + strings.Join(inputProjection, ", ") + " FROM (" +
		inputSourceSQL + ") AS " + inputSourceAlias + inputLimitSQL
	prefixArgs := make(
		[]any,
		0,
		len(measureInputArgs)+len(eligibilityArgs)+len(unsupportedArgs)+
			len(classificationArgs),
	)
	if windowedDynamicExtrema && len(groups) > 0 {
		// The outer prepared projection (including the measure fold) appears
		// textually before the classified source subquery and its BY arguments.
		prefixArgs = append(prefixArgs, measureInputArgs...)
		prefixArgs = append(prefixArgs, classificationArgs...)
	} else if measureUsesGroupEligibility {
		prefixArgs = append(prefixArgs, eligibilityArgs...)
		prefixArgs = append(prefixArgs, unsupportedArgs...)
		prefixArgs = append(prefixArgs, measureInputArgs...)
	} else {
		prefixArgs = append(prefixArgs, measureInputArgs...)
		prefixArgs = append(prefixArgs, eligibilityArgs...)
		prefixArgs = append(prefixArgs, unsupportedArgs...)
	}
	if measure.Function == plan.AggregateFunctionCountRows && len(groups) == 0 {
		return compileWindowedGlobalEventStatsCount(
			relation,
			operator,
			durableState,
			next,
			output,
			stage,
			inputName,
			inputSQL,
			prefixArgs,
		)
	}
	if windowedDynamicExtrema {
		return compileWindowedDynamicEventStatsExtrema(
			relation,
			operator,
			durableState,
			next,
			output,
			outputState,
			stage,
			groups,
			inputSQL,
			prefixArgs,
		)
	}
	aggregateInputName := inputName
	aggregateMeasureAlias := measureAlias
	listRowStateAlias := ""
	var listDefinitions []string
	if measure.Function == plan.AggregateFunctionList && listInputExists {
		orderKeys := defaultCompiledOrder(durableState)
		if len(orderKeys) == 0 {
			return compiledRelation{}, compileState{}, nil, nil, errors.New(
				"compile ClickHouse eventstats list order: input has no deterministic row identity",
			)
		}
		orderSQL, orderErr := compileMaterializedOrder(orderKeys, false)
		if orderErr != nil {
			return compiledRelation{}, compileState{}, nil, nil, fmt.Errorf(
				"compile ClickHouse eventstats list order: %w",
				orderErr,
			)
		}
		windowParts := make([]string, 0, 2)
		if len(groups) > 0 {
			// Incomplete BY rows normalize missing keys to the same physical
			// value as a present empty String. Partition eligibility separately
			// so a fixed scalar or Array(String) measure from an incomplete row
			// cannot consume the complete empty-key group's first-100 prefix.
			groupKeys := make([]string, 0, len(groups)+1)
			groupKeys = append(groupKeys, eligibleAlias)
			for _, group := range groups {
				groupKeys = append(groupKeys, group.keyAlias)
			}
			windowParts = append(
				windowParts,
				"PARTITION BY "+strings.Join(groupKeys, ", "),
			)
		}
		windowParts = append(windowParts, "ORDER BY "+orderSQL)
		windowOrder := strings.Join(windowParts, " ")
		rowOrdinal := quoteIdentifier(fmt.Sprintf(
			"__os_eventstats_list_row_ordinal_%d",
			stage,
		))
		priorElements := quoteIdentifier(fmt.Sprintf(
			"__os_eventstats_list_prior_elements_%d",
			stage,
		))
		priorBytes := quoteIdentifier(fmt.Sprintf(
			"__os_eventstats_list_prior_bytes_%d",
			stage,
		))
		valuesSQL := "tupleElement(" + measureAlias + ", 1)"
		frame := " OVER (" + windowOrder +
			" ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)"
		windowName := quoteIdentifier(fmt.Sprintf(
			"__os_eventstats_list_window_%d",
			stage,
		))
		maximumListValues := strconv.FormatUint(
			MaximumStatsListValuesPerGroup,
			10,
		)
		// priorBytes only influences rows reached before the first-100 element
		// ceiling. If fewer than 100 elements precede the current row, slicing
		// every preceding row to 100 members is identity-preserving; after that
		// point priorBytes is ignored. Keep this secondary payload walk bounded
		// even when a retained source row holds a very large multivalue.
		boundedByteValuesSQL := "arraySlice(" + valuesSQL + ", 1, " +
			maximumListValues + ")"
		windowSQL := "SELECT *, row_number() OVER (" + windowOrder + ") AS " +
			rowOrdinal + ", ifNull(sum(toUInt128(length(" + valuesSQL + ")))" +
			frame + ", toUInt128(0)) AS " + priorElements + ", ifNull(sum(" +
			stringArrayPayloadBytesSQL(boundedByteValuesSQL) + ")" + frame +
			", toUInt128(0)) AS " + priorBytes + " FROM " + inputName

		listRowStateAlias = quoteIdentifier(fmt.Sprintf(
			"__os_eventstats_list_row_state_%d",
			stage,
		))
		candidateName := quoteIdentifier(fmt.Sprintf(
			"__os_eventstats_list_candidates_%d",
			stage,
		))
		candidateSQL := "SELECT *, " + boundedOrderedStringRowStateSQL(
			rowOrdinal,
			valuesSQL,
			priorElements,
			priorBytes,
		) + " AS " + listRowStateAlias + " FROM " + windowName
		listDefinitions = append(
			listDefinitions,
			windowName+" AS ("+windowSQL+")",
			candidateName+" AS MATERIALIZED ("+candidateSQL+")",
		)
		aggregateInputName = candidateName
		aggregateMeasureAlias = listRowStateAlias
	}
	totalProjection := []string{
		boundedEventStatsCountSQL("count()") + " AS " + totalColumn,
	}
	valueColumn := totalColumn
	typeColumn := ""
	publishAggregateResult := outputState.kind == fieldKindDynamic
	publishesValues := measure.Function == plan.AggregateFunctionValues
	publishesList := measure.Function == plan.AggregateFunctionList
	valueElementsColumn := ""
	valueBytesColumn := ""
	if publishesValues || listInputExists {
		valueElementsColumn = quoteIdentifier(fmt.Sprintf(
			"__os_eventstats_value_elements_%d",
			stage,
		))
		valueBytesColumn = quoteIdentifier(fmt.Sprintf(
			"__os_eventstats_value_bytes_%d",
			stage,
		))
	}
	if measureSpec.materialized && len(groups) == 0 {
		rawValueColumn := quoteIdentifier(measureSpec.valuePrefix + strconv.Itoa(stage))
		aggregateSQL, aggregateErr := measureAggregateSQL(aggregateMeasureAlias)
		if aggregateErr != nil {
			return compiledRelation{}, compileState{}, nil, nil, aggregateErr
		}
		if publishesList && !listInputExists {
			aggregateSQL = emptyOrderedStringListSQL()
		}
		totalProjection = append(
			totalProjection,
			aggregateSQL+" AS "+rawValueColumn,
		)
		valueColumn = rawValueColumn
		if publishAggregateResult {
			valueColumn = quoteIdentifier(fmt.Sprintf(
				"__os_eventstats_published_value_%d",
				stage,
			))
			typeColumn = outputState.storedTypeSQL
			totalProjection = append(
				totalProjection,
				measurePublishValueSQL(rawValueColumn)+" AS "+valueColumn,
				measurePublishTypeSQL(rawValueColumn)+" AS "+typeColumn,
			)
		}
		if publishesValues {
			totalProjection = append(
				totalProjection,
				"toUInt64(length("+rawValueColumn+")) AS "+valueElementsColumn,
				stringArrayPayloadBytesSQL(rawValueColumn)+" AS "+valueBytesColumn,
				eventStatsValuesValidationSQL(
					measureAlias,
					valueElementsColumn,
					valueBytesColumn,
				)+" AS "+validationColumn,
			)
			valueColumn = quoteIdentifier(fmt.Sprintf(
				"__os_eventstats_published_value_%d",
				stage,
			))
			totalProjection = append(
				totalProjection,
				"if(toUInt8("+validationColumn+") = 0, arraySort("+
					rawValueColumn+"), "+measureNullSQL+") AS "+valueColumn,
			)
		} else if publishesList {
			valueColumn = quoteIdentifier(fmt.Sprintf(
				"__os_eventstats_published_value_%d",
				stage,
			))
			if listInputExists {
				totalProjection = append(
					totalProjection,
					"toUInt64(length("+rawValueColumn+")) AS "+valueElementsColumn,
					orderedStringListPayloadBytesSQL(rawValueColumn)+" AS "+valueBytesColumn,
					eventStatsListValidationSQL(
						measureAlias,
						listRowStateAlias,
						valueElementsColumn,
						valueBytesColumn,
					)+" AS "+validationColumn,
				)
				totalProjection = append(
					totalProjection,
					"if(toUInt8("+validationColumn+") = 0, "+
						orderedStringListValuesSQL(rawValueColumn)+", "+
						measureNullSQL+") AS "+valueColumn,
				)
			} else {
				totalProjection = append(
					totalProjection,
					orderedStringListValuesSQL(rawValueColumn)+" AS "+valueColumn,
				)
			}
		} else if measureValidationSQL != nil {
			totalProjection = append(
				totalProjection,
				measureValidationSQL(measureAlias, rawValueColumn)+
					" AS "+validationColumn,
			)
		}
	}
	// A list stage materializes its fully prepared candidate relation instead
	// of the raw bounded input. The total, grouped aggregate, output, and atomic
	// validation branches can then share one ordered-window execution. This is
	// still exactly one fence: finalization keeps the earliest materialization
	// and inlines this candidate if an earlier deferred stage already owns it.
	inputClause := " AS MATERIALIZED ("
	if publishesList && listInputExists {
		inputClause = " AS ("
	}
	definitions := []string{inputName + inputClause + inputSQL + ")"}
	definitions = append(definitions, listDefinitions...)
	definitions = append(
		definitions,
		totalName+" AS (SELECT "+strings.Join(totalProjection, ", ")+
			" FROM "+aggregateInputName+")",
	)

	outputValue := totalAlias + "." + valueColumn
	outputValueElements := "toUInt64(0)"
	outputValueBytes := "toUInt128(0)"
	if (publishesValues || listInputExists) && len(groups) == 0 {
		outputValueElements = totalAlias + "." + valueElementsColumn
		outputValueBytes = totalAlias + "." + valueBytesColumn
	}
	outputStoredType := ""
	if typeColumn != "" {
		outputStoredType = totalAlias + "." + typeColumn
	}
	outputValidation := ""
	if measureValidationSQL != nil || measureUsesValuesValidation ||
		listInputExists {
		outputValidation = totalAlias + "." + validationColumn
	}
	outputExistsSQL := "1"
	fromSQL := aggregateInputName + " AS " + inputAlias + " CROSS JOIN " +
		totalName + " AS " + totalAlias
	if len(groups) > 0 {
		groupCountsName := quoteIdentifier(fmt.Sprintf("__os_eventstats_counts_%d", stage))
		groupCountsAlias := quoteIdentifier(fmt.Sprintf("__os_eventstats_group_row_%d", stage))
		groupCountColumn := quoteIdentifier(fmt.Sprintf("__os_eventstats_group_count_%d", stage))
		groupKeys := make([]string, 0, len(groups))
		joinPredicates := make([]string, 0, len(groups))
		for _, group := range groups {
			groupKeys = append(groupKeys, group.keyAlias)
			joinPredicates = append(
				joinPredicates,
				inputAlias+"."+group.keyAlias+" = "+groupCountsAlias+"."+group.keyAlias,
			)
		}
		validGroup := eligibleAlias + " != 0"
		if len(unsupported) > 0 {
			validGroup = "if(" + unsupportedAlias + " != 0, throwIf(toUInt8(1), '" +
				UnsupportedStatsByValueMarker + "') = 0, " + validGroup + ")"
		}
		groupValueSQL := "toUInt64(count())"
		if measureSpec.materialized {
			var groupValueErr error
			groupValueSQL, groupValueErr = measureAggregateSQL(aggregateMeasureAlias)
			if groupValueErr != nil {
				return compiledRelation{}, compileState{}, nil, nil, groupValueErr
			}
			if publishesList && !listInputExists {
				groupValueSQL = emptyOrderedStringListSQL()
			}
		}
		groupProjection := strings.Join(groupKeys, ", ") + ", " +
			groupValueSQL + " AS " + groupCountColumn
		groupValueColumn := groupCountColumn
		if publishesValues {
			groupProjection += ", toUInt64(length(" + groupCountColumn +
				")) AS " + valueElementsColumn + ", " +
				stringArrayPayloadBytesSQL(groupCountColumn) + " AS " + valueBytesColumn
			groupProjection += ", " + eventStatsValuesValidationSQL(
				measureAlias,
				valueElementsColumn,
				valueBytesColumn,
			) + " AS " + validationColumn
			groupValueColumn = quoteIdentifier(fmt.Sprintf(
				"__os_eventstats_published_value_%d",
				stage,
			))
			groupProjection += ", if(toUInt8(" + validationColumn +
				") = 0, arraySort(" + groupCountColumn + "), " +
				measureNullSQL + ") AS " + groupValueColumn
		} else if publishesList {
			groupValueColumn = quoteIdentifier(fmt.Sprintf(
				"__os_eventstats_published_value_%d",
				stage,
			))
			if listInputExists {
				groupProjection += ", toUInt64(length(" + groupCountColumn +
					")) AS " + valueElementsColumn + ", " +
					orderedStringListPayloadBytesSQL(groupCountColumn) +
					" AS " + valueBytesColumn
				groupProjection += ", " + eventStatsListValidationSQL(
					measureAlias,
					listRowStateAlias,
					valueElementsColumn,
					valueBytesColumn,
				) + " AS " + validationColumn
				groupProjection += ", if(toUInt8(" + validationColumn +
					") = 0, " + orderedStringListValuesSQL(groupCountColumn) +
					", " + measureNullSQL + ") AS " + groupValueColumn
			} else {
				groupProjection += ", " +
					orderedStringListValuesSQL(groupCountColumn) +
					" AS " + groupValueColumn
			}
		} else if measureValidationSQL != nil {
			groupProjection += ", " + measureValidationSQL(
				measureAlias,
				groupCountColumn,
			) +
				" AS " + validationColumn
		}
		groupTypeColumn := ""
		if publishAggregateResult {
			groupValueColumn = quoteIdentifier(fmt.Sprintf(
				"__os_eventstats_published_value_%d",
				stage,
			))
			groupTypeColumn = outputState.storedTypeSQL
			groupProjection += ", " + measurePublishValueSQL(groupCountColumn) +
				" AS " + groupValueColumn + ", " +
				measurePublishTypeSQL(groupCountColumn) + " AS " + groupTypeColumn
		}
		definitions = append(
			definitions,
			groupCountsName+" AS (SELECT "+groupProjection+
				" FROM "+aggregateInputName+" WHERE "+validGroup+
				" GROUP BY "+strings.Join(groupKeys, ", ")+")",
		)
		fromSQL += " LEFT JOIN " + groupCountsName + " AS " + groupCountsAlias +
			" ON " + strings.Join(joinPredicates, " AND ")
		outputExistsSQL = inputAlias + "." + eligibleAlias + " != 0"
		outputValue = "if(" + outputExistsSQL + ", " + groupCountsAlias + "." +
			groupValueColumn + ", " + measureNullSQL + ")"
		if publishesValues || listInputExists {
			outputValueElements = "if(" + outputExistsSQL + ", " + groupCountsAlias +
				"." + valueElementsColumn + ", toUInt64(0))"
			outputValueBytes = "if(" + outputExistsSQL + ", " + groupCountsAlias +
				"." + valueBytesColumn + ", toUInt128(0))"
		}
		if groupTypeColumn != "" {
			outputStoredType = "if(" + outputExistsSQL + ", " + groupCountsAlias +
				"." + groupTypeColumn +
				", toUInt8(" + strconv.Itoa(int(eventfields.StoredValueTypeNull)) + "))"
		}
		if measureValidationSQL != nil || measureUsesValuesValidation ||
			listInputExists {
			outputValidation = "if(" + outputExistsSQL + ", " + groupCountsAlias +
				"." + validationColumn + ", toUInt8(0))"
		}
	}
	if publishesValues || publishesList {
		// Empty values/list results are physically [] but logically absent to SPL.
		// Keep the presence expression bound to the public output alias so later
		// projections and direct copies preserve the fixed multivalue contract.
		outputExistsSQL = "notEmpty(" + quoteIdentifier(output.Name) + ")"
	}
	if publishesValues {
		outputValidation = eventStatsValuesAnnotatedResultValidationSQL(
			outputValidation,
			outputValueElements,
			outputValueBytes,
		)
	} else if listInputExists {
		outputValidation = eventStatsListAnnotatedResultValidationSQL(
			outputValidation,
			outputValueElements,
			outputValueBytes,
		)
	}

	projection := eventAggregateProjection(
		durableState,
		next,
		output.Name,
		outputValue,
		outputStoredType,
		outputExistsSQL,
		validationColumn,
		outputValidation,
		inputAlias,
	)
	resultSQL := "SELECT " +
		strings.Join(projection, ", ") + " FROM " + fromSQL +
		" WHERE " + totalAlias + "." + totalColumn + " <= " + maximumRows
	enriched := compiledRelation{
		sql:        resultSQL,
		depth:      relation.depth + 3 + len(listDefinitions),
		ownerRange: operator.Range,
	}

	barrierName := quoteIdentifier(fmt.Sprintf(
		"__os_eventstats_result_%d",
		stage,
	))
	barrier := &pendingChronologicalBarrier{
		name: barrierName,
		// Defer every eventstats stage into the final flat CTE graph. A later
		// validating extrema can then compose with count/sum/average stages in
		// either order without nesting one MATERIALIZED input inside another.
		sql: resultSQL,
		prerequisiteDefinitions: append(
			append(
				[]string(nil),
				eventStatsPrerequisiteDefinitions...,
			),
			definitions...,
		),
		prefixArgumentsAfterExisting: prefixArgumentsAfterExisting,
		fanout:                       2,
		depth:                        enriched.depth,
		ownerRange:                   operator.Range,
	}
	if len(groups) > 0 {
		barrier.fanout = 3
	}
	if validationColumn != "" {
		barrier.validationColumns = []string{validationColumn}
	}
	publishedAlias := quoteIdentifier(fmt.Sprintf(
		"__os_eventstats_rows_result_%d",
		stage,
	))
	publishedSQL := "SELECT * FROM " + barrierName + " AS " + publishedAlias
	if validationColumn != "" {
		publishedSQL = "SELECT * EXCEPT (" + validationColumn + ") FROM " +
			barrierName + " AS " + publishedAlias
	}
	return enriched.selectFrom(publishedSQL, operator.Range), next, prefixArgs, barrier, nil
}

// compileWindowedGlobalEventStatsCount publishes an argument-free global
// count from the same bounded input row stream. The general eventstats graph
// has two consumers for that input (the aggregate and the row publication),
// but count() OVER () can attach the identical total while preserving every
// input row. Keeping the sentinel input as the one prerequisite fence removes
// the cross-join fanout without weakening the input-row guard.
func compileWindowedGlobalEventStatsCount(
	relation compiledRelation,
	operator *plan.EventAggregate,
	state compileState,
	next compileState,
	output plan.FieldRef,
	stage int,
	inputName string,
	inputSQL string,
	prefixArgs []any,
) (compiledRelation, compileState, []any, *pendingChronologicalBarrier, error) {
	if operator == nil ||
		operator.Measure.Function != plan.AggregateFunctionCountRows ||
		len(operator.GroupBy) != 0 || output.Name == "" || stage < 0 ||
		inputName == "" || inputSQL == "" || len(prefixArgs) != 0 {
		return compiledRelation{}, compileState{}, nil, nil, errors.New(
			"compile ClickHouse global eventstats count: contract is invalid",
		)
	}

	rawTotal := quoteIdentifier(fmt.Sprintf(
		"__os_eventstats_raw_count_%d",
		stage,
	))
	validationColumn := quoteIdentifier(fmt.Sprintf(
		"__os_eventstats_validation_%d",
		stage,
	))
	windowAlias := quoteIdentifier(fmt.Sprintf(
		"__os_eventstats_window_%d",
		stage,
	))
	windowSQL := "SELECT *, count() OVER () AS " + rawTotal + " FROM " + inputName
	maximumRows := strconv.FormatUint(MaximumEventStatsInputRows, 10)
	projection := eventAggregateProjection(
		state,
		next,
		output.Name,
		boundedEventStatsCountSQL(windowAlias+"."+rawTotal),
		"",
		"1",
		validationColumn,
		"toUInt8("+boundedEventStatsCountSQL(windowAlias+"."+rawTotal)+
			" > "+maximumRows+")",
		windowAlias,
	)
	resultSQL := "SELECT " + strings.Join(projection, ", ") + " FROM (" +
		windowSQL + ") AS " + windowAlias
	enriched := compiledRelation{
		sql:        resultSQL,
		depth:      relation.depth + 3,
		ownerRange: operator.Range,
	}

	barrierName := quoteIdentifier(fmt.Sprintf(
		"__os_eventstats_result_%d",
		stage,
	))
	barrier := &pendingChronologicalBarrier{
		name: barrierName,
		sql:  resultSQL,
		prerequisiteDefinitions: []string{
			inputName + " AS MATERIALIZED (" + inputSQL + ")",
		},
		validationColumns: []string{validationColumn},
		fanout:            1,
		depth:             enriched.depth,
		ownerRange:        operator.Range,
	}
	publishedAlias := quoteIdentifier(fmt.Sprintf(
		"__os_eventstats_rows_result_%d",
		stage,
	))
	publishedSQL := "SELECT * EXCEPT (" + validationColumn + ") FROM " +
		barrierName + " AS " + publishedAlias
	return enriched.selectFrom(publishedSQL, operator.Range), next, nil, barrier, nil
}

// compileWindowedDynamicEventStatsExtrema keeps the bounded Dynamic row fold,
// winner aggregate, row-count guard, publication, and validation inside one
// materialized result input. The public barrier is a cheap pass-through, which
// lets the established prerequisite graph keep its result, analysis source,
// final input, and validation CTEs ordinary. ClickHouse 26.3 can then reuse the
// complete evaluated event relation without planning a materialized CTE chain.
func compileWindowedDynamicEventStatsExtrema(
	relation compiledRelation,
	operator *plan.EventAggregate,
	state compileState,
	next compileState,
	output plan.FieldRef,
	outputState fieldState,
	stage int,
	groups []compiledEventStatsGroup,
	preparedSQL string,
	prefixArgs []any,
) (compiledRelation, compileState, []any, *pendingChronologicalBarrier, error) {
	if operator == nil ||
		(operator.Measure.Function != plan.AggregateFunctionMinimum &&
			operator.Measure.Function != plan.AggregateFunctionMaximum) ||
		outputState.kind != fieldKindDynamic || outputState.storedTypeSQL == "" ||
		preparedSQL == "" {
		return compiledRelation{}, compileState{}, nil, nil, errors.New(
			"compile ClickHouse eventstats extrema window: contract is invalid",
		)
	}

	inputAlias := quoteIdentifier(fmt.Sprintf("__os_eventstats_rows_%d", stage))
	measureAlias := quoteIdentifier(fmt.Sprintf("__os_eventstats_measure_%d", stage))
	eligibleAlias := quoteIdentifier(fmt.Sprintf("__os_eventstats_eligible_%d", stage))
	unsupportedAlias := quoteIdentifier(fmt.Sprintf("__os_eventstats_unsupported_%d", stage))
	totalColumn := quoteIdentifier(fmt.Sprintf("__os_eventstats_input_count_%d", stage))
	rawTotalColumn := quoteIdentifier(fmt.Sprintf("__os_eventstats_raw_count_%d", stage))
	rawValueColumn := quoteIdentifier(fmt.Sprintf(
		"__os_eventstats_raw_extrema_%d",
		stage,
	))
	publishedValueColumn := quoteIdentifier(fmt.Sprintf(
		"__os_eventstats_published_value_%d",
		stage,
	))
	typeColumn := outputState.storedTypeSQL
	validationColumn := quoteIdentifier(fmt.Sprintf(
		"__os_eventstats_validation_%d",
		stage,
	))

	partition := ""
	if len(groups) > 0 {
		partitionKeys := make([]string, 0, len(groups)+1)
		partitionKeys = append(partitionKeys, eligibleAlias)
		for _, group := range groups {
			partitionKeys = append(partitionKeys, group.keyAlias)
		}
		partition = "PARTITION BY " + strings.Join(partitionKeys, ", ")
	}
	window := " OVER (" + partition + ")"
	winner := statsExtremaScalarAggregateWinnerSQL(
		operator.Measure.Function,
		measureAlias,
	) + window

	preparedAlias := quoteIdentifier(fmt.Sprintf(
		"__os_eventstats_prepared_%d",
		stage,
	))
	windowSQL := "SELECT *, count() OVER () AS " + rawTotalColumn + ", " +
		winner + " AS " + rawValueColumn + " FROM (" + preparedSQL + ") AS " +
		preparedAlias

	// The final chronological validation envelope already reduces this hidden
	// bit across every row of the complete materialized result. Keeping the
	// row-local poison flag avoids a second window aggregate while preserving
	// whole-result atomicity behind downstream projection, filtering, or LIMIT.
	validation := "toUInt8(tupleElement(" + measureAlias + ", 6))"
	hasUnsupportedGroup := false
	for _, group := range groups {
		hasUnsupportedGroup = hasUnsupportedGroup || group.scalar.unsupportedSQL != ""
	}
	if hasUnsupportedGroup {
		// Validate each BY key independently of combined group eligibility. An
		// object/list key must poison the complete scoped result even when a
		// different key is missing on the same row.
		validation = "if(" + unsupportedAlias + " != 0, throwIf(toUInt8(1), '" +
			UnsupportedStatsByValueMarker + "'), " + validation + ")"
	}

	windowAlias := quoteIdentifier(fmt.Sprintf(
		"__os_eventstats_window_%d",
		stage,
	))
	discarded := []string{
		measureAlias,
		rawTotalColumn,
		rawValueColumn,
	}
	materializedSQL := "SELECT * EXCEPT (" + strings.Join(discarded, ", ") + "), " +
		boundedEventStatsCountSQL(rawTotalColumn) + " AS " + totalColumn + ", " +
		statsExtremaScalarValueSQL(rawValueColumn) + " AS " + publishedValueColumn +
		", " + statsExtremaScalarStoredTypeSQL(rawValueColumn) + " AS " + typeColumn +
		", toUInt8(" + validation + ") AS " + validationColumn + " FROM (" +
		windowSQL + ") AS " + windowAlias

	outputExistsSQL := "1"
	outputValue := inputAlias + "." + publishedValueColumn
	outputStoredType := inputAlias + "." + typeColumn
	if len(groups) > 0 {
		outputExistsSQL = inputAlias + "." + eligibleAlias + " != 0"
		outputValue = "if(" + outputExistsSQL + ", " + outputValue +
			", CAST(NULL AS Dynamic))"
		outputStoredType = "if(" + outputExistsSQL + ", " + outputStoredType +
			", toUInt8(" + strconv.Itoa(int(eventfields.StoredValueTypeNull)) + "))"
	}
	projection := eventAggregateProjection(
		state,
		next,
		output.Name,
		outputValue,
		outputStoredType,
		outputExistsSQL,
		validationColumn,
		inputAlias+"."+validationColumn,
		inputAlias,
	)
	maximumRows := strconv.FormatUint(MaximumEventStatsInputRows, 10)
	resultSQL := "SELECT " + strings.Join(projection, ", ") + " FROM (" +
		materializedSQL + ") AS " + inputAlias + " WHERE " + inputAlias + "." +
		totalColumn + " <= " + maximumRows
	resultDepth := relation.depth + 4
	if len(groups) > 0 {
		// Grouped window extrema classify every BY field in one additional
		// projection below the prepared measure relation. Account for that
		// dependency even though the classifier is embedded in preparedSQL.
		resultDepth++
	}
	enriched := compiledRelation{
		sql:        resultSQL,
		depth:      resultDepth,
		ownerRange: operator.Range,
	}

	resultInputName := quoteIdentifier(fmt.Sprintf(
		"__os_eventstats_result_input_%d",
		stage,
	))
	barrierName := quoteIdentifier(fmt.Sprintf(
		"__os_eventstats_result_%d",
		stage,
	))
	barrierSQL := "SELECT * FROM " + resultInputName
	barrierDepth := relationalNodeDepth(enriched.depth)
	barrier := &pendingChronologicalBarrier{
		name: barrierName,
		sql:  barrierSQL,
		prerequisiteDefinitions: []string{
			resultInputName + " AS MATERIALIZED (" + resultSQL + ")",
		},
		validationColumns: []string{validationColumn},
		fanout:            2,
		depth:             barrierDepth,
		ownerRange:        operator.Range,
	}
	if len(groups) > 0 {
		barrier.fanout = 3
	}
	publishedAlias := quoteIdentifier(fmt.Sprintf(
		"__os_eventstats_rows_result_%d",
		stage,
	))
	publishedSQL := "SELECT * EXCEPT (" + validationColumn + ") FROM " +
		barrierName + " AS " + publishedAlias
	enriched.depth = barrierDepth
	return enriched.selectFrom(publishedSQL, operator.Range), next, prefixArgs, barrier, nil
}

func validateEventAggregate(
	operator *plan.EventAggregate,
	state compileState,
) (plan.FieldRef, error) {
	if operator == nil {
		return plan.FieldRef{}, errors.New("compile ClickHouse eventstats: operator is missing")
	}
	if err := validateNonStatsAggregateMeasureMetadata("eventstats", operator.Measure); err != nil {
		return plan.FieldRef{}, err
	}
	if len(operator.GroupBy) > spl.MaximumStatsGroupFields {
		return plan.FieldRef{}, fmt.Errorf(
			"compile ClickHouse eventstats: more than %d grouping fields",
			spl.MaximumStatsGroupFields,
		)
	}
	measure := operator.Measure
	if measure.Function == plan.AggregateFunctionEarliest ||
		measure.Function == plan.AggregateFunctionLatest {
		if !hasCanonicalEventTime(state) {
			return plan.FieldRef{}, &plan.Diagnostic{
				Code: "SPL_UNSUPPORTED_EVENTSTATS_TIME_FIELD",
				Message: "eventstats earliest and latest require event rows " +
					"with the unmodified canonical _time field",
				Range: measure.Input.Range,
				Suggestions: []string{
					"run eventstats earliest or latest before removing, replacing, or transforming _time",
				},
			}
		}
	}
	switch measure.Function {
	case plan.AggregateFunctionCountRows:
		if measure.Input.Name != "" ||
			measure.Input.Canonical ||
			measure.Input.Path != nil ||
			measure.Input.Range != (spl.Range{}) ||
			measure.Predicate != nil ||
			measure.Percentile != 0 {
			return plan.FieldRef{}, errors.New(
				"compile ClickHouse eventstats: argument-free count contains unsupported metadata",
			)
		}
	case plan.AggregateFunctionCountValues, plan.AggregateFunctionSum,
		plan.AggregateFunctionAverage, plan.AggregateFunctionMinimum,
		plan.AggregateFunctionMaximum, plan.AggregateFunctionEarliest,
		plan.AggregateFunctionLatest, plan.AggregateFunctionDistinctCount,
		plan.AggregateFunctionValues, plan.AggregateFunctionList:
		form := "count(field)"
		switch measure.Function {
		case plan.AggregateFunctionSum:
			form = "sum(field)"
		case plan.AggregateFunctionAverage:
			form = "avg(field)"
		case plan.AggregateFunctionMinimum:
			form = "min(field)"
		case plan.AggregateFunctionMaximum:
			form = "max(field)"
		case plan.AggregateFunctionEarliest:
			form = "earliest(field)"
		case plan.AggregateFunctionLatest:
			form = "latest(field)"
		case plan.AggregateFunctionDistinctCount:
			form = "dc(field)"
		case plan.AggregateFunctionValues:
			form = "values(field)"
		case plan.AggregateFunctionList:
			form = "list(field)"
		}
		if measure.Predicate != nil || measure.Percentile != 0 {
			return plan.FieldRef{}, fmt.Errorf(
				"compile ClickHouse eventstats: %s contains unsupported predicate or percentile metadata",
				form,
			)
		}
		if err := validateCanonicalFieldRef(
			"eventstats",
			"input",
			measure.Input,
		); err != nil {
			return plan.FieldRef{}, err
		}
	case plan.AggregateFunctionPercentile:
		if measure.Predicate != nil ||
			measure.Percentile < 1 || measure.Percentile > 99 {
			return plan.FieldRef{}, errors.New(
				"compile ClickHouse eventstats: pN(field) contains unsupported predicate or percentile metadata",
			)
		}
		if err := validateCanonicalFieldRef(
			"eventstats",
			"input",
			measure.Input,
		); err != nil {
			return plan.FieldRef{}, err
		}
	case plan.AggregateFunctionCountPredicate:
		if err := validateConditionalCountMeasure(
			measure,
			state,
			"eventstats",
			"SPL_AMBIGUOUS_EVENTSTATS_FIELD",
			"eventstats cannot read the event result's reserved fields payload without an exact upstream schema",
		); err != nil {
			return plan.FieldRef{}, err
		}
	default:
		return plan.FieldRef{}, errors.New(
			"compile ClickHouse eventstats: only count, count(field), count(eval(...)), dc(field), values(field), list(field), min(field), max(field), earliest(field), latest(field), sum(field), avg(field), or pN/percN(field) is supported",
		)
	}
	output, err := plan.ResolveField(measure.Output, operator.Range)
	if err != nil {
		return plan.FieldRef{}, fmt.Errorf(
			"compile ClickHouse eventstats: invalid output field %q: %w",
			measure.Output,
			err,
		)
	}
	return output, nil
}

// validateNonStatsAggregateMeasureMetadata closes the compiler trust boundary
// for plan nodes whose AggregateMeasure predates stats scalar inputs, literal
// outputs, and sparklines. Those arms are stats-only. Related commands have
// their own bounded predicate support, so Predicate remains command-validated
// by their existing paths rather than being rejected here.
func validateNonStatsAggregateMeasureMetadata(
	command string,
	measure plan.AggregateMeasure,
) error {
	if measure.Sparkline != nil || measure.InputExpression != nil || measure.OutputLiteral {
		return fmt.Errorf(
			"compile ClickHouse %s: aggregate contains stats-only sparkline, scalar-input, or literal-output metadata",
			command,
		)
	}
	return nil
}

func eventAggregateCompileState(
	state compileState,
	output plan.FieldRef,
	outputState fieldState,
	grouped bool,
	stage int,
) compileState {
	next := cloneCompileState(state)
	if exposesRawFieldsPayload(state) && !output.Canonical {
		dropRawFieldsPayload(&next)
	}
	delete(next.blocked, output.Name)
	if !slices.Contains(next.publicOrder, output.Name) {
		next.publicOrder = append(next.publicOrder, output.Name)
	}
	existsSQL := "1"
	hasLogicalPresence := grouped || isNativeMultivalueKind(outputState.kind)
	if hasLogicalPresence {
		existsSQL = quoteIdentifier(fmt.Sprintf("__os_eventstats_exists_%d", stage))
	}
	outputState.valueSQL = quoteIdentifier(output.Name)
	outputState.existsSQL = existsSQL
	next.visible[output.Name] = outputState
	next.privateColumns = livePrivateColumns(next.privateColumns, next.visible)
	if outputState.storedTypeSQL != "" {
		next.privateColumns = append(next.privateColumns, outputState.storedTypeSQL)
	}
	if hasLogicalPresence {
		next.privateColumns = append(next.privateColumns, existsSQL)
	}
	return next
}

func eventAggregateProjection(
	state, next compileState,
	outputName, outputValue, outputStoredTypeSQL, outputExistsSQL string,
	validationColumn, outputValidationSQL, relationAlias string,
) []string {
	names := orderedVisibleNames(next)
	projection := make([]string, 0, len(names)+12+len(next.privateColumns))
	for _, name := range names {
		publicName := quoteIdentifier(name)
		if name == outputName {
			projection = append(projection, outputValue+" AS "+publicName)
			continue
		}
		field := state.visible[name]
		if field.valueSQL == publicName {
			if pathsOverlap, ok := logicalFieldNamesOverlap(outputName, name); ok && pathsOverlap && relationAlias != "" {
				projection = append(
					projection,
					relationAlias+"."+publicName+" AS "+publicName,
				)
			} else {
				projection = append(projection, publicName)
			}
		} else {
			projection = append(projection, field.valueSQL+" AS "+publicName)
		}
	}
	projectionState := next
	projectionState.privateColumns = livePrivateColumns(state.privateColumns, next.visible)
	projection = appendPrivateEventProjection(projection, projectionState)
	if outputStoredTypeSQL != "" {
		projection = append(
			projection,
			outputStoredTypeSQL+" AS "+next.visible[outputName].storedTypeSQL,
		)
	}
	if outputExistsSQL != "1" {
		projection = append(
			projection,
			"toUInt8("+outputExistsSQL+") AS "+next.visible[outputName].existsSQL,
		)
	}
	if validationColumn != "" && outputValidationSQL != "" {
		projection = append(
			projection,
			"toUInt8("+outputValidationSQL+") AS "+validationColumn,
		)
	}
	return projection
}

func boundedEventStatsCountSQL(countSQL string) string {
	maximum := strconv.FormatUint(MaximumEventStatsInputRows, 10)
	return "arrayElement(arrayMap(total -> total + toUInt64(throwIf(toUInt8(total > " +
		maximum + "), '" + EventStatsInputLimitMarker + "')), [toUInt64(" +
		countSQL + ")]), 1)"
}

// eventStatsDistinctCountValidationSQL keeps both failure classes inside the
// deferred whole-result validation graph. The value set itself contains at
// most one sentinel beyond the supported cardinality, while unsupported
// containers contribute no strings and retain a separate constant-size bit.
func eventStatsDistinctCountValidationSQL(inputSQL, cardinalitySQL string) string {
	maximum := strconv.FormatUint(MaximumStatsDistinctValuesPerGroup, 10)
	unsupported := "maxOrDefault(toUInt8(tupleElement(" + inputSQL + ", 2)))"
	return "if(" + unsupported + " != 0, " +
		"throwIf(toUInt8(1), '" + UnsupportedStatsMeasureValueMarker + "'), " +
		"if(" + cardinalitySQL + " > toUInt64(" + maximum + "), " +
		"throwIf(toUInt8(1), '" + ExactDistinctLimitMarker + "'), " +
		"toUInt8(0)))"
}

// eventStatsValuesValidationSQL validates one complete global or grouped
// exact-string cell before it can be copied onto source rows. The set itself
// retains only one count sentinel; the raw byte ceiling remains independently
// enforced after aggregation under ClickHouse's query-memory limit.
func eventStatsValuesValidationSQL(
	inputSQL, valueElementsSQL, valueBytesSQL string,
) string {
	unsupported := "maxOrDefault(toUInt8(tupleElement(" + inputSQL + ", 2)))"
	maximumValues := strconv.FormatUint(MaximumStatsValuesPerGroup, 10)
	maximumBytes := strconv.FormatUint(MaximumStatsValuesBytesPerGroup, 10)
	return "if(" + unsupported + " != 0, " +
		"throwIf(toUInt8(1), '" + UnsupportedStatsMeasureValueMarker + "'), " +
		"if(" + valueElementsSQL + " > toUInt64(" + maximumValues + "), " +
		"throwIf(toUInt8(1), '" + EventStatsValuesLimitMarker + "'), " +
		"if(" + valueBytesSQL + " > toUInt128(" +
		maximumBytes + "), throwIf(toUInt8(1), '" +
		EventStatsValuesBytesLimitMarker + "'), toUInt8(0))))"
}

// eventStatsValuesAnnotatedResultValidationSQL bounds the serialized
// amplification created when a values cell is repeated on every eligible
// source row. Its inputs are scalar counts materialized once per aggregate
// scope, so the window sums never rescan the arrays they account for.
func eventStatsValuesAnnotatedResultValidationSQL(
	cellValidationSQL, valueElementsSQL, valueBytesSQL string,
) string {
	return eventStatsAnnotatedArrayResultValidationSQL(
		cellValidationSQL,
		valueElementsSQL,
		valueBytesSQL,
		MaximumStatsValuesPerResult,
		MaximumStatsValuesBytesPerResult,
		EventStatsValuesLimitMarker,
		EventStatsValuesBytesLimitMarker,
	)
}

func eventStatsAnnotatedArrayResultValidationSQL(
	cellValidationSQL string,
	valueElementsSQL string,
	valueBytesSQL string,
	maximumElements uint64,
	maximumBytes uint64,
	elementLimitMarker string,
	bytesLimitMarker string,
) string {
	totalElements := "sum(toUInt128(" + valueElementsSQL + ")) OVER ()"
	totalBytes := "sum(toUInt128(" + valueBytesSQL + ")) OVER ()"
	resultValidation := "if(" + totalElements + " > toUInt128(" +
		strconv.FormatUint(maximumElements, 10) + "), " +
		"throwIf(toUInt8(1), '" + elementLimitMarker + "'), " +
		"if(" + totalBytes + " > toUInt128(" +
		strconv.FormatUint(maximumBytes, 10) + "), " +
		"throwIf(toUInt8(1), '" + bytesLimitMarker + "'), toUInt8(0)))"
	if cellValidationSQL == "" {
		return resultValidation
	}
	return "if(toUInt8(" + cellValidationSQL + ") != 0, toUInt8(1), " +
		resultValidation + ")"
}

// eventStatsListValidationSQL validates the complete retained scope before
// publishing its first-100 ordered prefix. Unsupported members are tracked
// independently from the bounded prefix, so a poisoned value after the first
// 100 still fails the whole command. The row state carries a constant-size bit
// when that selected prefix crossed the byte ceiling.
func eventStatsListValidationSQL(
	inputSQL, rowStateSQL, valueElementsSQL, valueBytesSQL string,
) string {
	unsupported := "maxOrDefault(toUInt8(tupleElement(" + inputSQL + ", 2)))"
	bytesOverflow := "maxOrDefault(toUInt8(tupleElement(" + rowStateSQL + ", 2)))"
	maximumValues := strconv.FormatUint(MaximumStatsListValuesPerGroup, 10)
	maximumBytes := strconv.FormatUint(MaximumStatsListBytesPerGroup, 10)
	return "if(" + unsupported + " != 0, " +
		"throwIf(toUInt8(1), '" + UnsupportedStatsMeasureValueMarker + "'), " +
		"if(" + valueElementsSQL + " > toUInt64(" + maximumValues + "), " +
		"throwIf(toUInt8(1), '" + EventStatsListLimitMarker + "'), " +
		"if(" + bytesOverflow + " != 0 OR " + valueBytesSQL +
		" > toUInt128(" + maximumBytes + "), throwIf(toUInt8(1), '" +
		EventStatsListBytesLimitMarker + "'), toUInt8(0))))"
}

// eventStatsListAnnotatedResultValidationSQL bounds the serialized
// amplification caused by copying one selected ordered list onto every source
// row. Element and byte counts are scalar aggregate metadata, so these windows
// never walk the public arrays again.
func eventStatsListAnnotatedResultValidationSQL(
	cellValidationSQL, valueElementsSQL, valueBytesSQL string,
) string {
	return eventStatsAnnotatedArrayResultValidationSQL(
		cellValidationSQL,
		valueElementsSQL,
		valueBytesSQL,
		MaximumStatsListValuesPerResult,
		MaximumStatsListBytesPerResult,
		EventStatsListLimitMarker,
		EventStatsListBytesLimitMarker,
	)
}

func validateAggregatePredicateMeasures(
	operator *plan.Aggregate,
	state compileState,
) error {
	if operator == nil {
		return errors.New("compile ClickHouse aggregate: aggregate is missing")
	}
	for _, measure := range operator.Measures {
		if measure.Function != plan.AggregateFunctionCountPredicate {
			if measure.Predicate != nil {
				return fmt.Errorf(
					"compile ClickHouse aggregate: function %d contains predicate metadata",
					measure.Function,
				)
			}
			continue
		}
		if err := validateConditionalCountMeasure(
			measure,
			state,
			"aggregate",
			"SPL_AMBIGUOUS_STATS_FIELD",
			"stats cannot read the event result's reserved fields payload without an exact upstream schema",
		); err != nil {
			return err
		}
	}
	return nil
}

func validateConditionalCountMeasure(
	measure plan.AggregateMeasure,
	state compileState,
	command, reservedCode, reservedMessage string,
) error {
	prefix := "compile ClickHouse " + command + ": "
	if measure.Input.Name != "" ||
		measure.Input.Canonical ||
		measure.Input.Path != nil ||
		measure.Input.Range != (spl.Range{}) ||
		measure.InputExpression != nil ||
		measure.Percentile != 0 {
		return errors.New(
			prefix + "count(eval(...)) contains unsupported field, scalar-input, or percentile metadata",
		)
	}
	if nilPlanExpression(measure.Predicate) {
		return errors.New(prefix + "count(eval(...)) predicate is missing")
	}
	if err := validateIfCondition(measure.Predicate); err != nil {
		return fmt.Errorf(prefix+"invalid count(eval(...)) predicate: %w", err)
	}
	if state.eventRows && state.allowDynamic {
		if sourceRange, reserved := predicateFieldSourceRange(
			measure.Predicate,
			"fields",
		); reserved {
			return &plan.Diagnostic{
				Code:    reservedCode,
				Message: reservedMessage,
				Range:   sourceRange,
			}
		}
	}
	return nil
}

func validateAggregateCardinality(operator *plan.Aggregate) error {
	if operator == nil || len(operator.Measures) == 0 {
		return errors.New("compile ClickHouse aggregate: no measures")
	}
	if _, err := effectiveStatsOptions(operator); err != nil {
		return err
	}
	if operator.StatsOptions != nil {
		if err := plan.ValidateStatsAggregateSourceUniqueness(operator.Measures); err != nil {
			return fmt.Errorf("compile ClickHouse aggregate: %w", err)
		}
	}
	if len(operator.Measures) > spl.MaximumStatsMeasures {
		return fmt.Errorf(
			"compile ClickHouse aggregate: more than %d measures",
			spl.MaximumStatsMeasures,
		)
	}
	if len(operator.GroupBy) > spl.MaximumStatsGroupFields {
		return fmt.Errorf(
			"compile ClickHouse aggregate: more than %d group fields",
			spl.MaximumStatsGroupFields,
		)
	}
	return nil
}

func hasCanonicalEventTime(state compileState) bool {
	timeField, ok := state.visible["_time"]
	return state.eventRows && ok && timeField.kind == fieldKindTime &&
		timeField.canonicalTime
}

func compileAggregate(operator *plan.Aggregate, state compileState) (
	projection []string,
	predicates []string,
	groups []string,
	next compileState,
	args []any,
	err error,
) {
	if cardinalityErr := validateAggregateCardinality(operator); cardinalityErr != nil {
		return nil, nil, nil, compileState{}, nil, cardinalityErr
	}
	if validateErr := validateAggregatePredicateMeasures(operator, state); validateErr != nil {
		return nil, nil, nil, compileState{}, nil, validateErr
	}
	return compileAggregateValidated(operator, state)
}

// compileAggregateValidated lowers an aggregate after the caller has checked
// its cardinality and conditional-predicate structure. The pipeline compiler
// performs that preflight before predicate materialization; compileAggregate
// remains the defensive entry point for direct package callers and tests.
func compileAggregateValidated(operator *plan.Aggregate, state compileState) (
	projection []string,
	predicates []string,
	groups []string,
	next compileState,
	args []any,
	err error,
) {
	statsOptions, optionsErr := effectiveStatsOptions(operator)
	if optionsErr != nil {
		return nil, nil, nil, compileState{}, nil, optionsErr
	}
	for _, measure := range operator.Measures {
		if measure.Function != plan.AggregateFunctionEarliest &&
			measure.Function != plan.AggregateFunctionLatest &&
			measure.Function != plan.AggregateFunctionEarliestTime &&
			measure.Function != plan.AggregateFunctionLatestTime &&
			measure.Function != plan.AggregateFunctionRate {
			continue
		}
		if !hasCanonicalEventTime(state) {
			sourceRange := measure.Input.Range
			if measure.InputExpression != nil &&
				!nilScalarExpression(measure.InputExpression) {
				sourceRange = measure.InputExpression.SourceRange()
			}
			return nil, nil, nil, compileState{}, nil, &plan.Diagnostic{
				Code:        "SPL_UNSUPPORTED_STATS_TIME_FIELD",
				Message:     "stats time functions require event rows with the unmodified canonical _time field",
				Range:       sourceRange,
				Suggestions: []string{"run the stats time function before removing, replacing, or transforming _time"},
			}
		}
	}
	next = compileState{
		visible:               make(map[string]fieldState, len(operator.GroupBy)+len(operator.Measures)),
		context:               state.context,
		allowDynamic:          false,
		eventRows:             false,
		blocked:               make(map[string]struct{}),
		chronologicalBarriers: append([]compiledChronologicalBarrier(nil), state.chronologicalBarriers...),
	}
	if state.mvExpandQueryRowsSQL != "" {
		// The first expansion's whole-stage charge is constant on every output
		// row. Collapse that private authority through transforming aggregates so
		// a later expansion can add its complete output count rather than resetting
		// the query-wide ceiling. maxOrDefault also yields zero for a global
		// aggregate over an empty input.
		projection = append(
			projection,
			"maxOrDefault("+state.mvExpandQueryRowsSQL+") AS "+state.mvExpandQueryRowsSQL,
		)
		next.mvExpandQueryRowsSQL = state.mvExpandQueryRowsSQL
		next.privateColumns = append(next.privateColumns, state.mvExpandQueryRowsSQL)
	}
	// Even a group-less aggregate produces a deterministic zero-or-one-row
	// relation. Give it a durable constant lineage immediately; grouped
	// aggregates replace this key with their exact group tuple below.
	if len(operator.GroupBy) == 0 {
		ordinal := quoteIdentifier("__os_aggregate_ordinal")
		projection = append(projection, "toUInt8(0) AS "+ordinal)
		next.order = []compiledSortKey{{valueSQL: ordinal}}
		next.tieBreakers = []compiledSortKey{{valueSQL: ordinal}}
	}
	seen := make(map[string]struct{}, len(operator.GroupBy)+len(operator.Measures))
	dynamicGroupInvalid := make([]string, 0, len(operator.GroupBy))
	var dynamicGroupInvalidArgs []any
	for _, group := range operator.GroupBy {
		if err := validateCanonicalFieldRef("aggregate", "group", group); err != nil {
			return nil, nil, nil, compileState{}, nil, err
		}
		if state.eventRows && state.allowDynamic && group.Name == "fields" {
			return nil, nil, nil, compileState{}, nil, &plan.Diagnostic{
				Code:    "SPL_AMBIGUOUS_STATS_FIELD",
				Message: "stats cannot group by the event result's reserved fields payload without an exact upstream schema",
				Range:   group.Range,
			}
		}
		if _, duplicate := seen[group.Name]; duplicate {
			return nil, nil, nil, compileState{}, nil, fmt.Errorf("compile ClickHouse aggregate: output field %q is duplicated", group.Name)
		}
		seen[group.Name] = struct{}{}
		var multivalueGroup compiledStatsMultivalueGroup
		expanded := false
		var compileErr error
		if operator.StatsOptions != nil {
			multivalueGroup, expanded, compileErr = compileStatsMultivalueGroup(
				group,
				state,
				statsOptions.DeduplicateSplitValues,
			)
			if compileErr != nil {
				return nil, nil, nil, compileState{}, nil, compileErr
			}
		}
		if expanded {
			ordinal := len(next.publicOrder)
			valuesAlias := quoteIdentifier(fmt.Sprintf("__os_group_values_%d", ordinal))
			valueAlias := quoteIdentifier(fmt.Sprintf("__os_group_value_%d", ordinal))
			next.preAggregateColumns = append(
				next.preAggregateColumns,
				multivalueGroup.valuesSQL+" AS "+valuesAlias,
			)
			next.preAggregateArgs = append(
				next.preAggregateArgs,
				multivalueGroup.valuesArgs...,
			)
			next.preAggregateGroupExpansions = append(
				next.preAggregateGroupExpansions,
				compiledStatsGroupExpansion{
					valuesAlias: valuesAlias,
					valueAlias:  valueAlias,
				},
			)
			groupOutput := fmt.Sprintf("__os_group_%d", ordinal)
			projection = append(
				projection,
				valueAlias+" AS "+quoteIdentifier(groupOutput),
			)
			groups = append(groups, valueAlias)
			if multivalueGroup.unsupportedSQL != "" {
				dynamicGroupInvalid = append(
					dynamicGroupInvalid,
					multivalueGroup.unsupportedSQL,
				)
				dynamicGroupInvalidArgs = append(
					dynamicGroupInvalidArgs,
					multivalueGroup.unsupportedArgs...,
				)
			}
			if multivalueGroup.field.kind == fieldKindDynamic {
				// Keep the established scalar-presence predicate as a redundant
				// eligibility fence. Empty arrays already disappear in ARRAY JOIN,
				// while this preserves missing/null and flattened-parent validation
				// contracts (including their source-located arguments).
				scalarPresence, presenceErr := compileExactScalarGroup(
					group,
					state,
					"stats BY",
				)
				if presenceErr != nil {
					return nil, nil, nil, compileState{}, nil, presenceErr
				}
				predicates = append(predicates, scalarPresence.presenceSQL)
				args = append(args, scalarPresence.presenceArgs...)
			}
			privateGroup := quoteIdentifier(groupOutput)
			semanticBytesSQL := ""
			if multivalueGroup.field.kind == fieldKindStringArray &&
				multivalueGroup.field.stringOrBytes {
				semanticBytesSQL = quoteIdentifier(fmt.Sprintf(
					"__os_group_semantic_bytes_%d",
					ordinal,
				))
				semanticValueSQL := "toUInt8(NOT isValidUTF8(" + valueAlias + "))"
				projection = append(
					projection,
					semanticValueSQL+" AS "+semanticBytesSQL,
				)
				groups = append(groups, semanticValueSQL)
				next.privateColumns = append(next.privateColumns, semanticBytesSQL)
			}
			numericSort := multivalueGroup.field.numericSort
			if multivalueGroup.field.kind == fieldKindDynamic {
				numericSort = true
			}
			next.visible[group.Name] = fieldState{
				valueSQL:       privateGroup,
				maxStringBytes: fieldStateStringByteBound(multivalueGroup.field),
				// Re-derive text eligibility from the durable aggregate output.
				// The ARRAY JOIN element alias is out of scope after aggregation,
				// while projections and renames can safely rebind this expression.
				textEligibleSQL: func() string {
					if multivalueGroup.field.kind == fieldKindStringArray &&
						multivalueGroup.field.stringOrBytes {
						return "isValidUTF8(" + privateGroup + ")"
					}
					return ""
				}(),
				semanticBytesSQL:            semanticBytesSQL,
				semanticBytesByUTF8Validity: semanticBytesSQL != "",
				existsSQL:                   "1",
				// Fixed values()/list() arrays preserve arbitrary String bytes.
				// ARRAY JOIN changes only cardinality; it must not silently narrow
				// an invalid-UTF-8 member from Bytes provenance to String.
				stringOrBytes: multivalueGroup.field.kind == fieldKindStringArray &&
					multivalueGroup.field.stringOrBytes,
				kind:          fieldKindString,
				caseSensitive: multivalueGroup.field.caseSensitive,
				numericSort:   numericSort,
			}
			next.publicOrder = append(next.publicOrder, group.Name)
			next.order = append(next.order, compiledSortKey{valueSQL: privateGroup})
			next.tieBreakers = append(next.tieBreakers, compiledSortKey{valueSQL: privateGroup})
			continue
		}
		scalarGroup, compileErr := compileExactScalarGroup(group, state, "stats BY")
		if compileErr != nil {
			return nil, nil, nil, compileState{}, nil, compileErr
		}
		field := scalarGroup.field
		valueSQL := scalarGroup.keySQL
		kind := field.kind
		numericSort := field.numericSort
		maxStringBytes := fieldStateStringByteBound(field)
		if kind == fieldKindDynamic {
			// Unsupported containers use one private placeholder group. A scoped
			// whole-input window below fails the search before any key is exposed.
			valueAlias := quoteIdentifier(fmt.Sprintf("__os_group_value_%d", len(groups)))
			next.preAggregateColumns = append(next.preAggregateColumns,
				scalarGroup.keySQL+" AS "+valueAlias,
			)
			valueSQL = valueAlias
			kind = fieldKindString
			numericSort = true
		}
		ordinal := len(next.publicOrder)
		groupOutput := fmt.Sprintf("__os_group_%d", ordinal)
		projection = append(projection, valueSQL+" AS "+quoteIdentifier(groupOutput))
		if scalarGroup.unsupportedSQL != "" {
			// Validate each key against its own presence rather than the combined
			// group eligibility predicate. A container must fail the whole scoped
			// search even when another BY key is missing on the same row.
			dynamicGroupInvalid = append(dynamicGroupInvalid, scalarGroup.unsupportedSQL)
			dynamicGroupInvalidArgs = append(
				dynamicGroupInvalidArgs,
				scalarGroup.unsupportedArgs...,
			)
		}
		predicates = append(predicates, scalarGroup.presenceSQL)
		args = append(args, scalarGroup.presenceArgs...)
		groups = append(groups, valueSQL)
		privateGroup := quoteIdentifier(groupOutput)
		textEligibleSQL := field.textEligibleSQL
		semanticBytesSQL := ""
		if field.kind == fieldKindString && field.stringOrBytes {
			if field.semanticBytesSQL == "" {
				return nil, nil, nil, compileState{}, nil, errors.New(
					"compile ClickHouse aggregate: String-or-Bytes group lacks semantic Bytes provenance",
				)
			}
			semanticBytesSQL = quoteIdentifier(fmt.Sprintf(
				"__os_group_semantic_bytes_%d",
				ordinal,
			))
			semanticValueSQL := "toUInt8(ifNull(" + field.semanticBytesSQL + ", 0))"
			projection = append(
				projection,
				semanticValueSQL+" AS "+semanticBytesSQL,
			)
			groups = append(groups, semanticValueSQL)
			next.privateColumns = append(next.privateColumns, semanticBytesSQL)
			textEligibleSQL = "(ifNull(" + semanticBytesSQL +
				", 0) = 0 AND isValidUTF8(" + privateGroup + "))"
		}
		next.visible[group.Name] = fieldState{
			valueSQL: privateGroup, maxStringBytes: maxStringBytes,
			textEligibleSQL:             textEligibleSQL,
			semanticBytesSQL:            semanticBytesSQL,
			textEligibleBySemanticBytes: semanticBytesSQL != "",
			existsSQL:                   "1",
			stringOrBytes:               field.stringOrBytes,
			stringOrBytesNullable:       field.stringOrBytesNullable,
			kind:                        kind,
			caseSensitive:               field.caseSensitive, numberType: field.numberType,
			numericSort: numericSort, alwaysNull: field.alwaysNull,
		}
		next.publicOrder = append(next.publicOrder, group.Name)
		next.order = append(next.order, compiledSortKey{valueSQL: privateGroup})
		next.tieBreakers = append(next.tieBreakers, compiledSortKey{valueSQL: privateGroup})
	}
	return compileAggregateMeasures(
		operator,
		state,
		statsOptions,
		projection,
		predicates,
		groups,
		next,
		args,
		seen,
		dynamicGroupInvalid,
		dynamicGroupInvalidArgs,
	)
}

type aggregateExpressionInput struct {
	valueSQL              string
	valueArgs             []any
	kind                  fieldKind
	numberType            string
	dynamicDomain         dynamicScalarDomain
	maxStringBytes        uint64
	numericIntegral       bool
	mvCountOneOrNull      bool
	mvSortedLexicographic bool
	alwaysNull            bool
	ieeeComparison        bool
	ordinal               int
	field                 fieldState
	numericAlias          string
	stringAlias           string
}

type aggregateInputCacheKey struct {
	fieldName         string
	expressionOrdinal int
	expression        bool
}

type aggregateScalarStringInput struct {
	ordinal        int
	valueAlias     string
	numberAlias    string
	candidateAlias string
	rawBytesSQL    string
	extremaReady   bool
}

type aggregateExtremaResultKey struct {
	input    aggregateInputCacheKey
	function plan.AggregateFunction
}

type aggregateExtremaResult struct {
	winnerAlias string
	typeAlias   string
}

type aggregateChronologicalInput struct {
	candidatesAlias string
	validationAlias string
	multiple        bool
}

type aggregateChronologicalResultKey struct {
	input    aggregateInputCacheKey
	function plan.AggregateFunction
}

type aggregateChronologicalResult struct {
	winnerAlias string
	typeAlias   string
}

type aggregatePercentileState struct {
	column    string
	positions map[uint8]int
}

type aggregateOrderedStringList struct {
	listColumn     string
	overflowColumn string
}

type aggregateConditionalCountInput struct {
	predicateSQL  string
	predicateArgs []any
	alias         string
}

type aggregateSparklineBucketInput struct {
	spec  statsSparklineBucketSpec
	alias string
}

type aggregateMeasureLowering struct {
	operator                     *plan.Aggregate
	state                        compileState
	statsOptions                 plan.StatsOptions
	projection                   []string
	predicates                   []string
	groups                       []string
	next                         compileState
	args                         []any
	seen                         map[string]struct{}
	dynamicGroupInvalid          []string
	dynamicGroupInvalidArgs      []any
	numericInputs                map[string]string
	aggregateExpressionInputs    []*aggregateExpressionInput
	stringInputs                 map[string]string
	allNumericInvalidInputs      map[aggregateInputCacheKey]string
	scalarStringInputs           map[aggregateInputCacheKey]*aggregateScalarStringInput
	countInputs                  map[string]string
	conditionalCountInputs       []aggregateConditionalCountInput
	extremaInputs                map[aggregateInputCacheKey]string
	scalarExtremaResults         map[aggregateExtremaResultKey]aggregateExtremaResult
	dynamicExtremaResults        map[aggregateExtremaResultKey]aggregateExtremaResult
	chronologicalInputs          map[aggregateInputCacheKey]aggregateChronologicalInput
	chronologicalResults         map[aggregateChronologicalResultKey]aggregateChronologicalResult
	chronologicalInputDirections map[string]chronologicalDirections
	chronologicalRowKey          string
	exactStringSets              map[aggregateInputCacheKey]string
	distinctCounts               map[aggregateInputCacheKey]string
	orderedStringLists           map[aggregateInputCacheKey]aggregateOrderedStringList
	valuesInputs                 map[aggregateInputCacheKey]struct{}
	extremaMeasureInputs         map[string]struct{}
	numericArrayConsumers        map[string]struct{}
	percentileLevels             map[string][]uint8
	percentileStates             map[string]aggregatePercentileState
	listRowOrdinal               string
	listWindowOrder              string
	sparklineBuckets             map[plan.SparklineSpan]aggregateSparklineBucketInput
}

func compileAggregateMeasures(
	operator *plan.Aggregate,
	state compileState,
	statsOptions plan.StatsOptions,
	projection []string,
	predicates []string,
	groups []string,
	next compileState,
	args []any,
	seen map[string]struct{},
	dynamicGroupInvalid []string,
	dynamicGroupInvalidArgs []any,
) ([]string, []string, []string, compileState, []any, error) {
	lowering := aggregateMeasureLowering{
		operator:                     operator,
		state:                        state,
		statsOptions:                 statsOptions,
		projection:                   projection,
		predicates:                   predicates,
		groups:                       groups,
		next:                         next,
		args:                         args,
		seen:                         seen,
		dynamicGroupInvalid:          dynamicGroupInvalid,
		dynamicGroupInvalidArgs:      dynamicGroupInvalidArgs,
		numericInputs:                make(map[string]string),
		aggregateExpressionInputs:    make([]*aggregateExpressionInput, 0),
		stringInputs:                 make(map[string]string),
		allNumericInvalidInputs:      make(map[aggregateInputCacheKey]string),
		scalarStringInputs:           make(map[aggregateInputCacheKey]*aggregateScalarStringInput),
		countInputs:                  make(map[string]string),
		conditionalCountInputs:       make([]aggregateConditionalCountInput, 0),
		extremaInputs:                make(map[aggregateInputCacheKey]string),
		scalarExtremaResults:         make(map[aggregateExtremaResultKey]aggregateExtremaResult),
		dynamicExtremaResults:        make(map[aggregateExtremaResultKey]aggregateExtremaResult),
		chronologicalInputs:          make(map[aggregateInputCacheKey]aggregateChronologicalInput),
		chronologicalResults:         make(map[aggregateChronologicalResultKey]aggregateChronologicalResult),
		chronologicalInputDirections: make(map[string]chronologicalDirections),
		exactStringSets:              make(map[aggregateInputCacheKey]string),
		distinctCounts:               make(map[aggregateInputCacheKey]string),
		orderedStringLists:           make(map[aggregateInputCacheKey]aggregateOrderedStringList),
		valuesInputs:                 make(map[aggregateInputCacheKey]struct{}),
		extremaMeasureInputs:         make(map[string]struct{}),
		numericArrayConsumers:        make(map[string]struct{}),
		percentileLevels:             make(map[string][]uint8),
		percentileStates:             make(map[string]aggregatePercentileState),
		sparklineBuckets:             make(map[plan.SparklineSpan]aggregateSparklineBucketInput),
	}
	lowering.collectMeasureInputs()
	if err := lowering.compileMeasures(); err != nil {
		return nil, nil, nil, compileState{}, nil, err
	}
	if err := lowering.finalizeDynamicGroups(); err != nil {
		return nil, nil, nil, compileState{}, nil, err
	}
	return lowering.projection, lowering.predicates, lowering.groups, lowering.next, lowering.args, nil
}

func (lowering *aggregateMeasureLowering) collectMeasureInputs() {
	for _, measure := range lowering.operator.Measures {
		if measure.Function == plan.AggregateFunctionEarliest ||
			measure.Function == plan.AggregateFunctionLatest ||
			measure.Function == plan.AggregateFunctionFirst ||
			measure.Function == plan.AggregateFunctionLast ||
			measure.Function == plan.AggregateFunctionEarliestTime ||
			measure.Function == plan.AggregateFunctionLatestTime {
			directions := lowering.chronologicalInputDirections[measure.Input.Name]
			if measure.Function == plan.AggregateFunctionEarliest ||
				measure.Function == plan.AggregateFunctionFirst ||
				measure.Function == plan.AggregateFunctionEarliestTime {
				directions.earliest = true
			} else {
				directions.latest = true
			}
			lowering.chronologicalInputDirections[measure.Input.Name] = directions
		}
		if measure.Function == plan.AggregateFunctionValues && measure.InputExpression == nil {
			lowering.valuesInputs[lowering.fieldInputCacheKey(measure.Input.Name)] = struct{}{}
		}
		if measure.Function == plan.AggregateFunctionMinimum ||
			measure.Function == plan.AggregateFunctionMaximum {
			lowering.extremaMeasureInputs[measure.Input.Name] = struct{}{}
		}
		if measure.InputExpression == nil && (measure.Function == plan.AggregateFunctionSum ||
			measure.Function == plan.AggregateFunctionAverage ||
			measure.Function == plan.AggregateFunctionExactPercentile ||
			measure.Function == plan.AggregateFunctionUpperPercentile ||
			measure.Function == plan.AggregateFunctionMedian ||
			measure.Function == plan.AggregateFunctionRange ||
			measure.Function == plan.AggregateFunctionSumSquares ||
			measure.Function == plan.AggregateFunctionStandardDeviationSample ||
			measure.Function == plan.AggregateFunctionStandardDeviationPopulation ||
			measure.Function == plan.AggregateFunctionVarianceSample ||
			measure.Function == plan.AggregateFunctionVariancePopulation ||
			measure.Function == plan.AggregateFunctionRate) {
			lowering.numericArrayConsumers[measure.Input.Name] = struct{}{}
		}
		if measure.Function == plan.AggregateFunctionPercentile &&
			measure.InputExpression == nil &&
			measure.Percentile >= 1 && measure.Percentile <= 99 &&
			!slices.Contains(lowering.percentileLevels[measure.Input.Name], measure.Percentile) {
			lowering.percentileLevels[measure.Input.Name] = append(
				lowering.percentileLevels[measure.Input.Name],
				measure.Percentile,
			)
		}
	}
}

func (lowering *aggregateMeasureLowering) fieldInputCacheKey(name string) aggregateInputCacheKey {
	return aggregateInputCacheKey{fieldName: name}
}

func (lowering *aggregateMeasureLowering) expressionInputCacheKey(
	ordinal int,
) aggregateInputCacheKey {
	return aggregateInputCacheKey{
		expressionOrdinal: ordinal,
		expression:        true,
	}
}

func (lowering *aggregateMeasureLowering) numericInputForResolved(
	ref plan.FieldRef,
	input fieldState,
	exists bool,
) string {
	if inputAlias, cached := lowering.numericInputs[ref.Name]; cached {
		return inputAlias
	}
	inputSQL := "CAST([], 'Array(Float64)')"
	var inputArgs []any
	if exists {
		inputSQL, inputArgs = numericArrayInputSQL(input)
	}
	inputAlias := quoteIdentifier(fmt.Sprintf(
		"__os_measure_values_%d",
		len(lowering.numericInputs),
	))
	lowering.numericInputs[ref.Name] = inputAlias
	lowering.next.preAggregateColumns = append(
		lowering.next.preAggregateColumns,
		inputSQL+" AS "+inputAlias,
	)
	lowering.next.preAggregateArgs = append(lowering.next.preAggregateArgs, inputArgs...)
	return inputAlias
}

func (lowering *aggregateMeasureLowering) numericInputFor(
	ref plan.FieldRef,
) (string, error) {
	if inputAlias, cached := lowering.numericInputs[ref.Name]; cached {
		return inputAlias, nil
	}
	input, exists, err := resolveCompiledField(ref, lowering.state)
	if err != nil {
		return "", err
	}
	return lowering.numericInputForResolved(ref, input, exists), nil
}

func (lowering *aggregateMeasureLowering) aggregateExpressionInputFor(
	expression plan.ScalarExpression,
) (*aggregateExpressionInput, error) {
	if nilScalarExpression(expression) {
		return nil, errors.New("compile ClickHouse aggregate: scalar eval input is missing")
	}
	compiled, err := compileScalarValue(expression, lowering.state)
	if err != nil {
		return nil, fmt.Errorf("compile ClickHouse aggregate scalar eval input: %w", err)
	}
	for _, cached := range lowering.aggregateExpressionInputs {
		if cached.valueSQL == compiled.valueSQL &&
			reflect.DeepEqual(cached.valueArgs, compiled.valueArgs) &&
			cached.field.textEligibleSQL == compiled.textEligibleSQL &&
			cached.field.semanticBytesSQL == compiled.semanticBytesSQL &&
			cached.field.textEligibleBySemanticBytes == compiled.textEligibleBySemanticBytes &&
			cached.field.stringOrBytes == compiled.stringOrBytes &&
			cached.field.stringOrBytesNullable == compiled.stringOrBytesNullable &&
			cached.kind == compiled.kind &&
			cached.numberType == compiled.numberType &&
			cached.dynamicDomain == compiled.dynamicDomain &&
			cached.maxStringBytes == compiled.maxStringBytes &&
			cached.numericIntegral == compiled.numericIntegral &&
			cached.mvCountOneOrNull == compiled.mvCountOneOrNull &&
			cached.mvSortedLexicographic == compiled.mvSortedLexicographic &&
			cached.alwaysNull == compiled.alwaysNull &&
			cached.ieeeComparison == compiled.ieeeComparison {
			return cached, nil
		}
	}
	ordinal := len(lowering.aggregateExpressionInputs)
	valueAlias := quoteIdentifier(fmt.Sprintf(
		"__os_measure_numeric_expression_value_%d",
		ordinal,
	))
	lowering.next.preAggregateColumns = append(
		lowering.next.preAggregateColumns,
		compiled.valueSQL+" AS "+valueAlias,
	)
	lowering.next.preAggregateArgs = append(
		lowering.next.preAggregateArgs,
		compiled.valueArgs...,
	)
	semanticBytesSQL := compiled.semanticBytesSQL
	if compiled.kind == fieldKindString && compiled.stringOrBytes {
		if semanticBytesSQL == "" {
			return nil, errors.New(
				"compile ClickHouse aggregate scalar eval input: String-or-Bytes value lacks semantic Bytes provenance",
			)
		}
		semanticAlias := quoteIdentifier(fmt.Sprintf(
			"__os_measure_expression_semantic_bytes_%d",
			ordinal,
		))
		lowering.next.preAggregateColumns = append(
			lowering.next.preAggregateColumns,
			"toUInt8(ifNull("+semanticBytesSQL+", 0)) AS "+semanticAlias,
		)
		lowering.next.preAggregateArgs = append(
			lowering.next.preAggregateArgs,
			compiled.semanticBytesArgs...,
		)
		semanticBytesSQL = semanticAlias
	}
	materialized := fieldState{
		valueSQL:                    valueAlias,
		textEligibleSQL:             compiled.textEligibleSQL,
		semanticBytesSQL:            semanticBytesSQL,
		semanticBytesByUTF8Validity: compiled.semanticBytesByUTF8Validity,
		textEligibleBySemanticBytes: compiled.textEligibleBySemanticBytes,
		stringOrBytes:               compiled.stringOrBytes,
		stringOrBytesNullable:       compiled.stringOrBytesNullable,
		existsSQL:                   "1",
		kind:                        compiled.kind,
		numberType:                  compiled.numberType,
		maxStringBytes:              compiled.maxStringBytes,
		numericIntegral:             compiled.numericIntegral,
		mvCountOneOrNull:            compiled.mvCountOneOrNull,
		mvSortedLexicographic:       compiled.mvSortedLexicographic,
		alwaysNull:                  compiled.alwaysNull,
		dynamicDomain:               compiled.dynamicDomain,
		ieeeComparison:              compiled.ieeeComparison,
	}
	if compiled.kind == fieldKindDynamic {
		materialized.dynamicTypeSQL = "dynamicType(" + valueAlias + ")"
	}
	cached := &aggregateExpressionInput{
		valueSQL:              compiled.valueSQL,
		valueArgs:             append([]any(nil), compiled.valueArgs...),
		kind:                  compiled.kind,
		numberType:            compiled.numberType,
		dynamicDomain:         compiled.dynamicDomain,
		maxStringBytes:        compiled.maxStringBytes,
		numericIntegral:       compiled.numericIntegral,
		mvCountOneOrNull:      compiled.mvCountOneOrNull,
		mvSortedLexicographic: compiled.mvSortedLexicographic,
		alwaysNull:            compiled.alwaysNull,
		ieeeComparison:        compiled.ieeeComparison,
		ordinal:               ordinal,
		field:                 materialized,
	}
	lowering.aggregateExpressionInputs = append(lowering.aggregateExpressionInputs, cached)
	return cached, nil
}

func (lowering *aggregateMeasureLowering) numericInputForExpression(
	expression plan.ScalarExpression,
) (string, error) {
	cached, err := lowering.aggregateExpressionInputFor(expression)
	if err != nil {
		return "", err
	}
	if cached.numericAlias != "" {
		return cached.numericAlias, nil
	}
	inputSQL, inputArgs := numericArrayInputSQL(cached.field)
	inputAlias := quoteIdentifier(fmt.Sprintf(
		"__os_measure_numeric_expression_%d",
		cached.ordinal,
	))
	lowering.next.preAggregateColumns = append(
		lowering.next.preAggregateColumns,
		inputSQL+" AS "+inputAlias,
	)
	lowering.next.preAggregateArgs = append(lowering.next.preAggregateArgs, inputArgs...)
	cached.numericAlias = inputAlias
	return inputAlias, nil
}

func (lowering *aggregateMeasureLowering) allNumericInvalidInputFor(
	key aggregateInputCacheKey,
	input fieldState,
	exists bool,
) string {
	if !lowering.statsOptions.AllNumeric {
		return ""
	}
	if cached, ok := lowering.allNumericInvalidInputs[key]; ok {
		return cached
	}
	if !exists {
		lowering.allNumericInvalidInputs[key] = ""
		return ""
	}
	invalidSQL, invalidArgs := statsAllNumericInvalidSQL(input)
	if invalidSQL == "toUInt8(0)" {
		lowering.allNumericInvalidInputs[key] = ""
		return ""
	}
	alias := quoteIdentifier(fmt.Sprintf(
		"__os_measure_all_numeric_invalid_%d",
		len(lowering.allNumericInvalidInputs),
	))
	lowering.allNumericInvalidInputs[key] = alias
	lowering.next.preAggregateColumns = append(
		lowering.next.preAggregateColumns,
		invalidSQL+" AS "+alias,
	)
	lowering.next.preAggregateArgs = append(lowering.next.preAggregateArgs, invalidArgs...)
	return alias
}

func (lowering *aggregateMeasureLowering) allNumericInvalidFor(
	measure plan.AggregateMeasure,
	key aggregateInputCacheKey,
) (string, error) {
	if !lowering.statsOptions.AllNumeric || !statsUsesAllNumericPolicy(measure.Function) {
		return "", nil
	}
	if measure.InputExpression != nil {
		cached, err := lowering.aggregateExpressionInputFor(measure.InputExpression)
		if err != nil {
			return "", err
		}
		return lowering.allNumericInvalidInputFor(
			key,
			cached.field,
			!cached.field.alwaysNull,
		), nil
	}
	input, exists, err := resolveCompiledField(measure.Input, lowering.state)
	if err != nil {
		return "", err
	}
	return lowering.allNumericInvalidInputFor(key, input, exists), nil
}

func (lowering *aggregateMeasureLowering) countInputFor(
	ref plan.FieldRef,
) (string, error) {
	if inputAlias, cached := lowering.countInputs[ref.Name]; cached {
		return inputAlias, nil
	}
	input, exists, err := resolveCompiledField(ref, lowering.state)
	if err != nil {
		return "", err
	}
	inputSQL := "toUInt64(0)"
	var inputArgs []any
	if exists {
		inputSQL, inputArgs = countValueInputSQL(input)
	}
	inputAlias := quoteIdentifier(fmt.Sprintf(
		"__os_measure_count_%d",
		len(lowering.countInputs),
	))
	lowering.countInputs[ref.Name] = inputAlias
	lowering.next.preAggregateColumns = append(
		lowering.next.preAggregateColumns,
		inputSQL+" AS "+inputAlias,
	)
	lowering.next.preAggregateArgs = append(lowering.next.preAggregateArgs, inputArgs...)
	return inputAlias, nil
}

func (lowering *aggregateMeasureLowering) conditionalCountInputFor(
	expression plan.Expression,
) (string, error) {
	predicateSQL, predicateArgs, err := compileExpression(expression, lowering.state)
	if err != nil {
		return "", err
	}
	for _, cached := range lowering.conditionalCountInputs {
		if cached.predicateSQL == predicateSQL &&
			reflect.DeepEqual(cached.predicateArgs, predicateArgs) {
			return cached.alias, nil
		}
	}
	alias := quoteIdentifier(fmt.Sprintf(
		"__os_measure_conditional_count_%d",
		len(lowering.conditionalCountInputs),
	))
	lowering.conditionalCountInputs = append(
		lowering.conditionalCountInputs,
		aggregateConditionalCountInput{
			predicateSQL:  predicateSQL,
			predicateArgs: append([]any(nil), predicateArgs...),
			alias:         alias,
		},
	)
	lowering.next.preAggregateColumns = append(
		lowering.next.preAggregateColumns,
		"toUInt64(ifNull("+predicateSQL+", 0)) AS "+alias,
	)
	lowering.next.preAggregateArgs = append(lowering.next.preAggregateArgs, predicateArgs...)
	return alias, nil
}

func (lowering *aggregateMeasureLowering) listRowOrdinalFor() (string, string, error) {
	if lowering.listRowOrdinal != "" {
		return lowering.listRowOrdinal, lowering.listWindowOrder, nil
	}
	orderKeys := defaultCompiledOrder(lowering.state)
	if len(orderKeys) == 0 {
		return "", "", errors.New(
			"compile ClickHouse list order: input has no deterministic row identity",
		)
	}
	orderSQL, err := compileMaterializedOrder(orderKeys, false)
	if err != nil {
		return "", "", fmt.Errorf("compile ClickHouse list order: %w", err)
	}
	windowParts := make([]string, 0, 2)
	if len(lowering.groups) > 0 {
		windowParts = append(
			windowParts,
			"PARTITION BY "+strings.Join(lowering.groups, ", "),
		)
	}
	windowParts = append(windowParts, "ORDER BY "+orderSQL)
	lowering.listWindowOrder = strings.Join(windowParts, " ")
	lowering.listRowOrdinal = quoteIdentifier("__os_list_row_ordinal")
	lowering.next.preAggregateListWindowColumns = append(
		lowering.next.preAggregateListWindowColumns,
		"row_number() OVER ("+lowering.listWindowOrder+") AS "+lowering.listRowOrdinal,
	)
	return lowering.listRowOrdinal, lowering.listWindowOrder, nil
}

func (lowering *aggregateMeasureLowering) scalarStringInputFor(
	key aggregateInputCacheKey,
	input fieldState,
) *aggregateScalarStringInput {
	if cached, ok := lowering.scalarStringInputs[key]; ok {
		return cached
	}
	ordinal := len(lowering.scalarStringInputs)
	inputSQL, inputArgs := statsScalarStringInputSQL(input)
	inputAlias := quoteIdentifier(fmt.Sprintf(
		"__os_measure_scalar_string_%d",
		ordinal,
	))
	cached := &aggregateScalarStringInput{
		ordinal:     ordinal,
		valueAlias:  inputAlias,
		rawBytesSQL: fixedStringExtremaRawBytesSQL(input),
	}
	lowering.scalarStringInputs[key] = cached
	lowering.next.preAggregateColumns = append(
		lowering.next.preAggregateColumns,
		inputSQL+" AS "+inputAlias,
	)
	lowering.next.preAggregateArgs = append(lowering.next.preAggregateArgs, inputArgs...)
	return cached
}

func (lowering *aggregateMeasureLowering) stringInputFor(
	ref plan.FieldRef,
) (string, error) {
	if inputSQL, cached := lowering.stringInputs[ref.Name]; cached {
		return inputSQL, nil
	}
	input, exists, err := resolveCompiledField(ref, lowering.state)
	if err != nil {
		return "", err
	}
	inputSQL := "CAST([], 'Array(String)')"
	if exists {
		var inputArgs []any
		if _, sharesScalar := lowering.extremaMeasureInputs[ref.Name]; sharesScalar &&
			input.kind == fieldKindString && input.textEligibleSQL == "" {
			scalarInput := lowering.scalarStringInputFor(lowering.fieldInputCacheKey(ref.Name), input)
			inputSQL = compactNullableArraySQL("[" + scalarInput.valueAlias + "]")
		} else {
			inputSQL, inputArgs = stringArrayInputSQL(input)
		}
		inputAlias := quoteIdentifier(fmt.Sprintf(
			"__os_measure_strings_%d",
			len(lowering.stringInputs),
		))
		lowering.stringInputs[ref.Name] = inputAlias
		lowering.next.preAggregateColumns = append(
			lowering.next.preAggregateColumns,
			inputSQL+" AS "+inputAlias,
		)
		lowering.next.preAggregateArgs = append(lowering.next.preAggregateArgs, inputArgs...)
		inputSQL = inputAlias
	}
	lowering.stringInputs[ref.Name] = inputSQL
	return inputSQL, nil
}

func (lowering *aggregateMeasureLowering) stringInputForExpression(
	expression plan.ScalarExpression,
) (string, error) {
	cached, err := lowering.aggregateExpressionInputFor(expression)
	if err != nil {
		return "", err
	}
	if cached.stringAlias != "" {
		return cached.stringAlias, nil
	}
	inputSQL, inputArgs := stringArrayInputSQL(cached.field)
	inputAlias := quoteIdentifier(fmt.Sprintf(
		"__os_measure_string_expression_%d",
		cached.ordinal,
	))
	lowering.next.preAggregateColumns = append(
		lowering.next.preAggregateColumns,
		inputSQL+" AS "+inputAlias,
	)
	lowering.next.preAggregateArgs = append(lowering.next.preAggregateArgs, inputArgs...)
	cached.stringAlias = inputAlias
	return inputAlias, nil
}

func (lowering *aggregateMeasureLowering) chronologicalRowKeyFor() string {
	if lowering.chronologicalRowKey != "" {
		return lowering.chronologicalRowKey
	}
	lowering.chronologicalRowKey = quoteIdentifier("__os_chronological_row_key")
	lowering.next.preAggregateColumns = append(
		lowering.next.preAggregateColumns,
		immutableChronologicalRowKeySQL()+" AS "+lowering.chronologicalRowKey,
	)
	return lowering.chronologicalRowKey
}

func (lowering *aggregateMeasureLowering) chronologicalInputForResolved(
	key aggregateInputCacheKey,
	input fieldState,
	exists bool,
	directions chronologicalDirections,
) aggregateChronologicalInput {
	if cached, ok := lowering.chronologicalInputs[key]; ok {
		return cached
	}
	ordinal := len(lowering.chronologicalInputs)
	compiled := aggregateChronologicalInput{}
	candidatesSQL, candidateArgs, runtimeValidated := chronologicalCandidatesSQL(
		input,
		exists,
		directions,
	)
	compiled.candidatesAlias = quoteIdentifier(fmt.Sprintf(
		"__os_chronological_candidates_%d",
		ordinal,
	))
	compiled.multiple = exists &&
		(input.kind == fieldKindDynamic || isNativeMultivalueKind(input.kind))
	lowering.next.preAggregateColumns = append(
		lowering.next.preAggregateColumns,
		candidatesSQL+" AS "+compiled.candidatesAlias,
	)
	lowering.next.preAggregateArgs = append(
		lowering.next.preAggregateArgs,
		candidateArgs...,
	)
	if runtimeValidated {
		compiled.validationAlias = quoteIdentifier(fmt.Sprintf(
			"__os_stats_chronological_any_unsupported_%d",
			ordinal,
		))
		lowering.projection = append(
			lowering.projection,
			"max(toUInt8(tupleElement("+compiled.candidatesAlias+", 4))) AS "+
				compiled.validationAlias,
		)
	}
	lowering.chronologicalInputs[key] = compiled
	return compiled
}

func (lowering *aggregateMeasureLowering) chronologicalInputFor(
	ref plan.FieldRef,
) (aggregateChronologicalInput, error) {
	input, exists, err := resolveCompiledField(ref, lowering.state)
	if err != nil {
		return aggregateChronologicalInput{}, err
	}
	return lowering.chronologicalInputForResolved(
		lowering.fieldInputCacheKey(ref.Name),
		input,
		exists,
		lowering.chronologicalInputDirections[ref.Name],
	), nil
}

func (lowering *aggregateMeasureLowering) chronologicalInputForExpression(
	expression plan.ScalarExpression,
) (aggregateChronologicalInput, error) {
	cached, err := lowering.aggregateExpressionInputFor(expression)
	if err != nil {
		return aggregateChronologicalInput{}, err
	}
	return lowering.chronologicalInputForResolved(
		lowering.expressionInputCacheKey(cached.ordinal),
		cached.field,
		!cached.field.alwaysNull,
		chronologicalDirections{earliest: true, latest: true},
	), nil
}

func (lowering *aggregateMeasureLowering) percentileInputFor(
	ref plan.FieldRef,
) (string, bool, error) {
	if _, sharedWithArrayConsumer := lowering.numericArrayConsumers[ref.Name]; sharedWithArrayConsumer {
		inputAlias, err := lowering.numericInputFor(ref)
		return inputAlias, true, err
	}
	input, exists, err := resolveCompiledField(ref, lowering.state)
	if err != nil {
		return "", false, err
	}
	if exists && (input.kind == fieldKindDynamic || isNativeMultivalueKind(input.kind)) {
		return lowering.numericInputForResolved(ref, input, true), true, nil
	}
	inputSQL := "CAST(NULL AS Nullable(Float64))"
	if exists {
		inputSQL = percentileInputSQL(input)
		if input.existsSQL != "" && input.existsSQL != "1" {
			inputSQL = "if(" + input.existsSQL + ", " + inputSQL +
				", CAST(NULL AS Nullable(Float64)))"
			lowering.next.preAggregateArgs = append(
				lowering.next.preAggregateArgs,
				input.existsArgs...,
			)
		}
	}
	inputAlias := quoteIdentifier(fmt.Sprintf(
		"__os_measure_percentile_value_%d",
		len(lowering.percentileStates),
	))
	lowering.next.preAggregateColumns = append(
		lowering.next.preAggregateColumns,
		inputSQL+" AS "+inputAlias,
	)
	return inputAlias, false, nil
}

func (lowering *aggregateMeasureLowering) compileMeasures() error {
	for measureIndex, measure := range lowering.operator.Measures {
		if err := lowering.validateOutput(measure); err != nil {
			return err
		}
		if measure.Sparkline != nil {
			if err := lowering.compileSparklineMeasure(measureIndex, measure); err != nil {
				return err
			}
			continue
		}
		if err := lowering.compileScalarMeasure(measureIndex, measure); err != nil {
			return err
		}
	}
	return nil
}

func (lowering *aggregateMeasureLowering) validateOutput(
	measure plan.AggregateMeasure,
) error {
	if measure.OutputLiteral {
		if lowering.operator.StatsOptions == nil ||
			!spl.IsStatsLiteralOutputName(measure.Output) {
			return fmt.Errorf(
				"compile ClickHouse aggregate: invalid literal output field %q",
				measure.Output,
			)
		}
		return nil
	}
	if _, err := plan.ResolveField(measure.Output, spl.Range{}); err != nil {
		return fmt.Errorf(
			"compile ClickHouse aggregate: invalid output field %q: %w",
			measure.Output,
			err,
		)
	}
	return nil
}

func (lowering *aggregateMeasureLowering) compileScalarMeasure(
	measureIndex int,
	measure plan.AggregateMeasure,
) error {
	hasFieldInput := measure.Input.Name != "" || measure.Input.Canonical ||
		len(measure.Input.Path) != 0 || measure.Input.Range != (spl.Range{})
	hasExpressionInput := measure.InputExpression != nil
	if hasExpressionInput && nilScalarExpression(measure.InputExpression) {
		return errors.New("compile ClickHouse aggregate: scalar eval input is a typed nil")
	}
	supportsExpressionInput := false
	switch measure.Function {
	case plan.AggregateFunctionCountRows:
		if hasFieldInput || hasExpressionInput || measure.Percentile != 0 {
			return errors.New(
				"compile ClickHouse aggregate: count contains unsupported input metadata",
			)
		}
	case plan.AggregateFunctionCountPredicate:
	case plan.AggregateFunctionCountValues:
		if hasExpressionInput || measure.Percentile != 0 {
			return errors.New(
				"compile ClickHouse aggregate: count(field) contains scalar-input or percentile metadata",
			)
		}
	case plan.AggregateFunctionPercentile,
		plan.AggregateFunctionExactPercentile,
		plan.AggregateFunctionUpperPercentile:
		supportsExpressionInput = true
		if measure.Percentile < 1 || measure.Percentile > 99 {
			return fmt.Errorf(
				"compile ClickHouse aggregate: unsupported percentile %d",
				measure.Percentile,
			)
		}
	case plan.AggregateFunctionSum, plan.AggregateFunctionAverage,
		plan.AggregateFunctionMedian,
		plan.AggregateFunctionRange, plan.AggregateFunctionSumSquares,
		plan.AggregateFunctionStandardDeviationSample,
		plan.AggregateFunctionStandardDeviationPopulation,
		plan.AggregateFunctionVarianceSample,
		plan.AggregateFunctionVariancePopulation,
		plan.AggregateFunctionRate:
		supportsExpressionInput = true
		if measure.Percentile != 0 {
			return fmt.Errorf(
				"compile ClickHouse aggregate: function %d contains percentile metadata",
				measure.Function,
			)
		}
	case plan.AggregateFunctionDistinctCount,
		plan.AggregateFunctionEstimatedDistinctCount,
		plan.AggregateFunctionEstimatedDistinctCountError,
		plan.AggregateFunctionValues,
		plan.AggregateFunctionList,
		plan.AggregateFunctionMinimum, plan.AggregateFunctionMaximum,
		plan.AggregateFunctionMode,
		plan.AggregateFunctionFirst, plan.AggregateFunctionLast,
		plan.AggregateFunctionEarliest, plan.AggregateFunctionLatest,
		plan.AggregateFunctionEarliestTime,
		plan.AggregateFunctionLatestTime:
		supportsExpressionInput = true
		if measure.Percentile != 0 {
			return fmt.Errorf(
				"compile ClickHouse aggregate: function %d contains percentile metadata",
				measure.Function,
			)
		}
	default:
		return fmt.Errorf(
			"compile ClickHouse aggregate: unsupported function %d",
			measure.Function,
		)
	}
	if measure.Function != plan.AggregateFunctionCountRows &&
		measure.Function != plan.AggregateFunctionCountPredicate {
		if hasFieldInput == hasExpressionInput {
			return errors.New(
				"compile ClickHouse aggregate: measure requires exactly one field or scalar eval input",
			)
		}
		if hasExpressionInput && !supportsExpressionInput {
			return fmt.Errorf(
				"compile ClickHouse aggregate: function %d does not support a scalar eval input",
				measure.Function,
			)
		}
		if hasFieldInput {
			if err := validateCanonicalFieldRef("aggregate", "input", measure.Input); err != nil {
				return err
			}
			if lowering.state.eventRows && lowering.state.allowDynamic &&
				measure.Input.Name == "fields" {
				return &plan.Diagnostic{
					Code:    "SPL_AMBIGUOUS_STATS_FIELD",
					Message: "stats cannot read the event result's reserved fields payload without an exact upstream schema",
					Range:   measure.Input.Range,
				}
			}
		} else if lowering.state.eventRows && lowering.state.allowDynamic {
			if sourceRange, reserved := predicateFieldSourceRange(
				&plan.ScalarPredicateExpression{Value: measure.InputExpression},
				"fields",
			); reserved {
				return &plan.Diagnostic{
					Code:    "SPL_AMBIGUOUS_STATS_FIELD",
					Message: "stats cannot read the event result's reserved fields payload without an exact upstream schema",
					Range:   sourceRange,
				}
			}
		}
	}
	inputKey := lowering.fieldInputCacheKey(measure.Input.Name)
	if hasExpressionInput {
		cached, err := lowering.aggregateExpressionInputFor(measure.InputExpression)
		if err != nil {
			return err
		}
		inputKey = lowering.expressionInputCacheKey(cached.ordinal)
	}
	if _, duplicate := lowering.seen[measure.Output]; duplicate {
		return fmt.Errorf(
			"compile ClickHouse aggregate: output field %q is duplicated",
			measure.Output,
		)
	}
	lowering.seen[measure.Output] = struct{}{}
	output := quoteIdentifier(measure.Output)
	measureState := fieldState{valueSQL: output, existsSQL: "1", kind: fieldKindNumber}
	allNumericInvalidAlias, err := lowering.allNumericInvalidFor(measure, inputKey)
	if err != nil {
		return err
	}
	measureState, err = lowering.measure(
		measureIndex,
		measure,
		inputKey,
		output,
		measureState,
		allNumericInvalidAlias,
	)
	if err != nil {
		return err
	}
	switch measure.Function {
	case plan.AggregateFunctionMinimum, plan.AggregateFunctionMaximum,
		plan.AggregateFunctionFirst, plan.AggregateFunctionLast,
		plan.AggregateFunctionEarliest, plan.AggregateFunctionLatest:
		input, exists, inputErr := lowering.resolveMeasureInput(measure)
		if inputErr != nil {
			return inputErr
		}
		if exists {
			measureState.maxStringBytes = fieldStateStringByteBound(input)
		}
	}
	if measure.Function == plan.AggregateFunctionValues ||
		measure.Function == plan.AggregateFunctionList {
		measureState.flatMultivalueDelimiter = strings.Clone(lowering.statsOptions.Delimiter)
		measureState.hasFlatMultivalueDelimiter = true
	}
	lowering.next.visible[measure.Output] = measureState
	lowering.next.publicOrder = append(lowering.next.publicOrder, measure.Output)
	if len(lowering.next.order) == 0 {
		orderSQL := quoteIdentifier(measure.Output)
		if measureState.kind == fieldKindDynamic {
			orderSQL = quoteIdentifier("__os_aggregate_order")
			lowering.projection = append(lowering.projection, "toUInt8(0) AS "+orderSQL)
		}
		lowering.next.order = append(lowering.next.order, compiledSortKey{valueSQL: orderSQL})
	}
	return nil
}

func (lowering *aggregateMeasureLowering) finalizeDynamicGroups() error {
	if len(lowering.dynamicGroupInvalid) == 0 {
		return nil
	}
	if lowering.next.context == nil {
		return errors.New("compile ClickHouse aggregate: runtime validation context is missing")
	}
	lowering.next.context.atomicResult = true
	anyUnsupportedColumn := quoteIdentifier("__os_stats_by_any_unsupported")
	invalid := "(" + strings.Join(lowering.dynamicGroupInvalid, ") OR (") + ")"
	lowering.next.preAggregateValidationColumns = append(
		lowering.next.preAggregateValidationColumns,
		"max(CAST("+invalid+" AS UInt8)) OVER () AS "+anyUnsupportedColumn,
	)
	lowering.next.preAggregateValidationArgs = append(
		lowering.next.preAggregateValidationArgs,
		lowering.dynamicGroupInvalidArgs...,
	)
	eligible := "1"
	if len(lowering.predicates) > 0 {
		eligible = "(" + strings.Join(lowering.predicates, " AND ") + ")"
	}
	lowering.predicates = []string{
		"if(" + anyUnsupportedColumn + " != 0, throwIf(toUInt8(1), '" +
			UnsupportedStatsByValueMarker + "') = 0, " + eligible + ")",
	}
	return nil
}

func (lowering *aggregateMeasureLowering) compileSparklineMeasure(
	measureIndex int,
	measure plan.AggregateMeasure,
) error {
	if lowering.operator.StatsOptions == nil ||
		measure.Function != plan.AggregateFunctionInvalid ||
		measure.Input.Name != "" || measure.Input.Canonical ||
		measure.Input.Path != nil || measure.Input.Range != (spl.Range{}) ||
		measure.InputExpression != nil || measure.Predicate != nil ||
		measure.Percentile != 0 {
		return errors.New(
			"compile ClickHouse aggregate: sparkline and scalar aggregate metadata overlap",
		)
	}
	if lowering.state.context == nil || lowering.state.context.searchEarliest.IsZero() ||
		lowering.state.context.searchLatest.IsZero() {
		return errors.New(
			"compile ClickHouse stats sparkline: search time range is unavailable",
		)
	}
	sparkline := measure.Sparkline
	if err := validateCanonicalFieldRef("stats sparkline", "time", sparkline.Time); err != nil {
		return err
	}
	if sparkline.Time.Name != "_time" || !sparkline.Time.Canonical ||
		sparkline.Time.Path != nil {
		return errors.New(
			"compile ClickHouse stats sparkline: time field is not canonical _time",
		)
	}
	timeField, timeExists, err := resolveCompiledField(sparkline.Time, lowering.state)
	if err != nil {
		return err
	}
	if !timeExists || timeField.kind != fieldKindTime || !timeField.canonicalTime {
		return &plan.Diagnostic{
			Code:    "SPL_UNSUPPORTED_STATS_TIME_FIELD",
			Message: "stats sparkline requires event rows with the unmodified canonical _time field",
			Range:   sparkline.Time.Range,
		}
	}
	hasInput := sparkline.Input.Name != "" || sparkline.Input.Canonical ||
		sparkline.Input.Path != nil || sparkline.Input.Range != (spl.Range{})
	if hasInput {
		if err := validateCanonicalFieldRef("stats sparkline", "input", sparkline.Input); err != nil {
			return err
		}
		if lowering.state.eventRows && lowering.state.allowDynamic &&
			sparkline.Input.Name == "fields" {
			return &plan.Diagnostic{
				Code:    "SPL_AMBIGUOUS_STATS_FIELD",
				Message: "stats sparkline cannot read the event result's reserved fields payload without an exact upstream schema",
				Range:   sparkline.Input.Range,
			}
		}
	}
	bucket, cached := lowering.sparklineBuckets[sparkline.Span]
	if !cached {
		spec, specErr := statsSparklineBucketSpecFor(
			sparkline.Span,
			lowering.state.context.searchEarliest,
			lowering.state.context.searchLatest,
			sparkline.MaximumPoints,
			timeField.valueSQL,
			lowering.state.context.searchTimezone,
		)
		if specErr != nil {
			return specErr
		}
		bucket = aggregateSparklineBucketInput{
			spec: spec,
			alias: quoteIdentifier(fmt.Sprintf(
				"__os_sparkline_bucket_%d",
				len(lowering.sparklineBuckets),
			)),
		}
		lowering.sparklineBuckets[sparkline.Span] = bucket
		lowering.next.preAggregateColumns = append(
			lowering.next.preAggregateColumns,
			spec.BucketSQL+" AS "+bucket.alias,
		)
		lowering.next.preAggregateArgs = append(
			lowering.next.preAggregateArgs,
			spec.BucketArgs...,
		)
	}
	partition := append(append([]string(nil), lowering.groups...), bucket.alias)
	partitionSQL := strings.Join(partition, ", ")
	inputSQL := ""
	expectedInput := statsSparklineInputNone
	missing := statsSparklineMissingEmpty
	switch sparkline.Function {
	case plan.AggregateFunctionCountRows:
		if hasInput {
			return errors.New(
				"compile ClickHouse stats sparkline: row count contains an input field",
			)
		}
		missing = statsSparklineMissingZero
	case plan.AggregateFunctionCountValues:
		if !hasInput {
			return errors.New(
				"compile ClickHouse stats sparkline: count(field) input is missing",
			)
		}
		inputSQL, err = lowering.countInputFor(sparkline.Input)
		expectedInput = statsSparklineInputOccurrenceCount
		missing = statsSparklineMissingZero
	case plan.AggregateFunctionDistinctCount,
		plan.AggregateFunctionMinimum,
		plan.AggregateFunctionMaximum:
		if !hasInput {
			return errors.New(
				"compile ClickHouse stats sparkline: string aggregate input is missing",
			)
		}
		inputSQL, err = lowering.stringInputFor(sparkline.Input)
		expectedInput = statsSparklineInputStringArray
		if sparkline.Function == plan.AggregateFunctionDistinctCount {
			missing = statsSparklineMissingZero
		}
	case plan.AggregateFunctionAverage,
		plan.AggregateFunctionStandardDeviationSample,
		plan.AggregateFunctionStandardDeviationPopulation,
		plan.AggregateFunctionVarianceSample,
		plan.AggregateFunctionVariancePopulation,
		plan.AggregateFunctionSum,
		plan.AggregateFunctionSumSquares,
		plan.AggregateFunctionRange:
		if !hasInput {
			return errors.New(
				"compile ClickHouse stats sparkline: numeric aggregate input is missing",
			)
		}
		inputSQL, err = lowering.numericInputFor(sparkline.Input)
		expectedInput = statsSparklineInputFloat64Array
	default:
		return fmt.Errorf(
			"compile ClickHouse stats sparkline: unsupported function %d",
			sparkline.Function,
		)
	}
	if err != nil {
		return err
	}
	compiled, supported := statsSparklineWindowAggregateSQL(
		sparkline.Function,
		inputSQL,
		partitionSQL,
	)
	if !supported || compiled.Input != expectedInput {
		return errors.New(
			"compile ClickHouse stats sparkline: aggregate lowering is invalid",
		)
	}
	windowSQL := compiled.SQL
	if lowering.statsOptions.AllNumeric && statsUsesAllNumericPolicy(sparkline.Function) {
		input, exists, inputErr := resolveCompiledField(sparkline.Input, lowering.state)
		if inputErr != nil {
			return inputErr
		}
		invalidAlias := lowering.allNumericInvalidInputFor(
			lowering.fieldInputCacheKey(sparkline.Input.Name),
			input,
			exists,
		)
		if invalidAlias != "" {
			windowSQL = "if(max(" + invalidAlias + ") OVER (PARTITION BY " +
				partitionSQL + ") != 0, CAST(NULL AS Nullable(Float64)), " +
				windowSQL + ")"
		}
	}
	if _, duplicate := lowering.seen[measure.Output]; duplicate {
		return fmt.Errorf(
			"compile ClickHouse aggregate: output field %q is duplicated",
			measure.Output,
		)
	}
	lowering.seen[measure.Output] = struct{}{}
	windowAlias := quoteIdentifier(fmt.Sprintf(
		"__os_sparkline_window_%d",
		measureIndex,
	))
	lowering.next.preAggregateSparklineWindows = append(
		lowering.next.preAggregateSparklineWindows,
		windowSQL+" AS "+windowAlias,
	)
	recordsSQL, ok := statsSparklineBucketRecordsSQL(
		bucket.alias,
		windowAlias,
		sparkline.MaximumPoints,
	)
	if !ok {
		return errors.New(
			"compile ClickHouse stats sparkline: bucket record lowering is invalid",
		)
	}
	recordsAlias := quoteIdentifier(fmt.Sprintf(
		"__os_sparkline_records_%d",
		measureIndex,
	))
	lowering.projection = append(lowering.projection, recordsSQL+" AS "+recordsAlias)
	output := quoteIdentifier(measure.Output)
	lowering.next.postAggregateSparklines = append(
		lowering.next.postAggregateSparklines,
		compiledStatsSparklineMeasure{
			recordsColumn: recordsAlias,
			outputColumn:  output,
			spec:          bucket.spec,
			missing:       missing,
		},
	)
	lowering.next.visible[measure.Output] = fieldState{
		valueSQL:       output,
		existsSQL:      "1",
		kind:           fieldKindStringArray,
		maxStringBytes: MaximumStatsSparklineBytesPerCell,
		statsSparkline: true,
		stringOrBytes: sparkline.Function == plan.AggregateFunctionMinimum ||
			sparkline.Function == plan.AggregateFunctionMaximum,
	}
	lowering.next.publicOrder = append(lowering.next.publicOrder, measure.Output)
	if len(lowering.next.order) == 0 {
		lowering.next.order = append(lowering.next.order, compiledSortKey{valueSQL: output})
	}
	return nil
}

func (lowering *aggregateMeasureLowering) measure(
	measureIndex int,
	measure plan.AggregateMeasure,
	inputKey aggregateInputCacheKey,
	output string,
	measureState fieldState,
	allNumericInvalidAlias string,
) (fieldState, error) {
	switch measure.Function {
	case plan.AggregateFunctionCountRows,
		plan.AggregateFunctionCountPredicate,
		plan.AggregateFunctionCountValues:
		return lowering.count(measure, output, measureState)
	case plan.AggregateFunctionPercentile,
		plan.AggregateFunctionExactPercentile,
		plan.AggregateFunctionUpperPercentile,
		plan.AggregateFunctionMedian,
		plan.AggregateFunctionRate,
		plan.AggregateFunctionSum,
		plan.AggregateFunctionAverage,
		plan.AggregateFunctionRange,
		plan.AggregateFunctionSumSquares,
		plan.AggregateFunctionStandardDeviationSample,
		plan.AggregateFunctionStandardDeviationPopulation,
		plan.AggregateFunctionVarianceSample,
		plan.AggregateFunctionVariancePopulation:
		return lowering.numeric(measure, output, measureState, allNumericInvalidAlias)
	case plan.AggregateFunctionMinimum, plan.AggregateFunctionMaximum:
		return lowering.extrema(measureIndex, measure, inputKey, output, measureState)
	case plan.AggregateFunctionFirst, plan.AggregateFunctionLast,
		plan.AggregateFunctionEarliest, plan.AggregateFunctionLatest,
		plan.AggregateFunctionEarliestTime, plan.AggregateFunctionLatestTime:
		return lowering.chronological(measure, inputKey, output, measureState)
	case plan.AggregateFunctionEstimatedDistinctCount,
		plan.AggregateFunctionEstimatedDistinctCountError,
		plan.AggregateFunctionMode:
		return lowering.distribution(measureIndex, measure, output, measureState)
	case plan.AggregateFunctionDistinctCount, plan.AggregateFunctionValues:
		return lowering.distinct(measure, inputKey, output, measureState)
	case plan.AggregateFunctionList:
		return lowering.list(measure, inputKey, output, measureState)
	default:
		return fieldState{}, fmt.Errorf(
			"compile ClickHouse aggregate: unsupported function %d",
			measure.Function,
		)
	}
}

func (lowering *aggregateMeasureLowering) count(
	measure plan.AggregateMeasure,
	output string,
	measureState fieldState,
) (fieldState, error) {
	switch measure.Function {
	case plan.AggregateFunctionCountRows:
		lowering.projection = append(lowering.projection, "count() AS "+output)
		measureState.numberType = "UInt64"
	case plan.AggregateFunctionCountPredicate:
		inputAlias, err := lowering.conditionalCountInputFor(measure.Predicate)
		if err != nil {
			return fieldState{}, err
		}
		lowering.projection = append(
			lowering.projection,
			"toUInt64(sum(toUInt128("+inputAlias+"))) AS "+output,
		)
		measureState.numberType = "UInt64"
	case plan.AggregateFunctionCountValues:
		inputAlias, err := lowering.countInputFor(measure.Input)
		if err != nil {
			return fieldState{}, err
		}
		lowering.projection = append(
			lowering.projection,
			"toUInt64(sum(toUInt128("+inputAlias+"))) AS "+output,
		)
		measureState.numberType = "UInt64"
	}
	return measureState, nil
}

func (lowering *aggregateMeasureLowering) numeric(
	measure plan.AggregateMeasure,
	output string,
	measureState fieldState,
	allNumericInvalidAlias string,
) (fieldState, error) {
	if measure.Function == plan.AggregateFunctionPercentile {
		return lowering.percentile(measure, output, measureState, allNumericInvalidAlias)
	}
	inputAlias, err := lowering.numericMeasureInput(measure)
	if err != nil {
		return fieldState{}, err
	}
	switch measure.Function {
	case plan.AggregateFunctionExactPercentile,
		plan.AggregateFunctionUpperPercentile,
		plan.AggregateFunctionMedian:
		compiled, supported := statsDistributionArrayAggregateSQL(
			measure.Function,
			measure.Percentile,
			inputAlias,
		)
		if !supported || compiled.Result != statsDistributionResultNullableFloat64 {
			return fieldState{}, errors.New(
				"compile ClickHouse aggregate: percentile distribution lowering is invalid",
			)
		}
		lowering.projection = append(
			lowering.projection,
			statsAllNumericResultSQL(compiled.SQL, allNumericInvalidAlias)+" AS "+output,
		)
	case plan.AggregateFunctionRate:
		timeField, timeExists := lowering.state.visible["_time"]
		if !timeExists || timeField.kind != fieldKindTime || !timeField.canonicalTime {
			return fieldState{}, errors.New(
				"compile ClickHouse aggregate: rate has no canonical _time input",
			)
		}
		lowering.projection = append(
			lowering.projection,
			statsAllNumericResultSQL(
				statsRateAggregateSQL(
					inputAlias,
					lowering.chronologicalRowKeyFor(),
					percentileInputSQL(timeField),
				),
				allNumericInvalidAlias,
			)+" AS "+output,
		)
	default:
		valueSQL, supported := statsNumericArrayAggregateSQL(measure.Function, inputAlias)
		if !supported {
			return fieldState{}, errors.New(
				"compile ClickHouse aggregate: numeric array function is invalid",
			)
		}
		lowering.projection = append(
			lowering.projection,
			statsAllNumericResultSQL(valueSQL, allNumericInvalidAlias)+" AS "+output,
		)
	}
	measureState.numberType = "Float64"
	return measureState, nil
}

func (lowering *aggregateMeasureLowering) numericMeasureInput(
	measure plan.AggregateMeasure,
) (string, error) {
	if measure.InputExpression != nil {
		return lowering.numericInputForExpression(measure.InputExpression)
	}
	return lowering.numericInputFor(measure.Input)
}

func (lowering *aggregateMeasureLowering) percentile(
	measure plan.AggregateMeasure,
	output string,
	measureState fieldState,
	allNumericInvalidAlias string,
) (fieldState, error) {
	if measure.InputExpression != nil {
		inputAlias, err := lowering.numericInputForExpression(measure.InputExpression)
		if err != nil {
			return fieldState{}, err
		}
		lowering.projection = append(
			lowering.projection,
			statsAllNumericResultSQL(
				singlePercentileArrayAggregateSQL(measure.Percentile, inputAlias),
				allNumericInvalidAlias,
			)+" AS "+output,
		)
		measureState.numberType = "Float64"
		return measureState, nil
	}
	percentiles, cached := lowering.percentileStates[measure.Input.Name]
	if !cached {
		inputAlias, inputIsArray, err := lowering.percentileInputFor(measure.Input)
		if err != nil {
			return fieldState{}, err
		}
		levels := lowering.percentileLevels[measure.Input.Name]
		if len(levels) == 0 {
			return fieldState{}, errors.New(
				"compile ClickHouse aggregate: percentile input has no valid levels",
			)
		}
		levelSQL := make([]string, 0, len(levels))
		positions := make(map[uint8]int, len(levels))
		for index, level := range levels {
			levelSQL = append(levelSQL, statsPercentileLevelSQL(level))
			positions[level] = index + 1
		}
		percentiles = aggregatePercentileState{
			column: quoteIdentifier(fmt.Sprintf(
				"__os_stats_percentiles_%d",
				len(lowering.percentileStates),
			)),
			positions: positions,
		}
		lowering.percentileStates[measure.Input.Name] = percentiles
		aggregateFunction := "quantilesGKOrNull"
		if inputIsArray {
			aggregateFunction += "Array"
		}
		lowering.projection = append(
			lowering.projection,
			aggregateFunction+"(100, "+strings.Join(levelSQL, ", ")+")("+
				inputAlias+") AS "+percentiles.column,
		)
	}
	position, ok := percentiles.positions[measure.Percentile]
	if !ok {
		return fieldState{}, errors.New(
			"compile ClickHouse aggregate: percentile level was not collected",
		)
	}
	lowering.projection = append(
		lowering.projection,
		statsAllNumericResultSQL(
			"arrayElementOrNull("+percentiles.column+", "+strconv.Itoa(position)+")",
			allNumericInvalidAlias,
		)+" AS "+output,
	)
	measureState.numberType = "Float64"
	return measureState, nil
}

func (lowering *aggregateMeasureLowering) extrema(
	measureIndex int,
	measure plan.AggregateMeasure,
	inputKey aggregateInputCacheKey,
	output string,
	measureState fieldState,
) (fieldState, error) {
	input, ok, err := lowering.resolveMeasureInput(measure)
	if err != nil {
		return fieldState{}, err
	}
	if eligible, eligibleArgs, fixed := fixedExtremaEligibilitySQL(input); ok && fixed {
		function := "minIfOrNull"
		if measure.Function == plan.AggregateFunctionMaximum {
			function = "maxIfOrNull"
		}
		lowering.projection = append(
			lowering.projection,
			function+"("+input.valueSQL+", "+eligible+") AS "+output,
		)
		lowering.args = append(lowering.args, eligibleArgs...)
		measureState.kind = input.kind
		measureState.numberType = input.numberType
		measureState.caseSensitive = input.caseSensitive
		return measureState, nil
	}
	if ok && input.kind == fieldKindString {
		return lowering.scalarExtrema(measureIndex, measure, inputKey, output, input)
	}
	return lowering.dynamicExtrema(measureIndex, measure, inputKey, output, input, ok)
}

func (lowering *aggregateMeasureLowering) scalarExtrema(
	measureIndex int,
	measure plan.AggregateMeasure,
	inputKey aggregateInputCacheKey,
	output string,
	input fieldState,
) (fieldState, error) {
	scalarInput := lowering.scalarStringInputFor(inputKey, input)
	if !scalarInput.extremaReady {
		scalarInput.numberAlias = quoteIdentifier(fmt.Sprintf(
			"__os_measure_extrema_number_%d",
			scalarInput.ordinal,
		))
		scalarInput.candidateAlias = quoteIdentifier(fmt.Sprintf(
			"__os_measure_extrema_scalar_%d",
			scalarInput.ordinal,
		))
		lowering.next.preAggregateColumns = append(
			lowering.next.preAggregateColumns,
			statsExtremaScalarNumberSQL(scalarInput.valueAlias)+" AS "+scalarInput.numberAlias,
			statsExtremaScalarCandidateSQL(
				scalarInput.valueAlias,
				scalarInput.numberAlias,
				scalarInput.rawBytesSQL,
			)+" AS "+scalarInput.candidateAlias,
		)
		scalarInput.extremaReady = true
	}
	resultKey := aggregateExtremaResultKey{
		input:    inputKey,
		function: measure.Function,
	}
	result, cached := lowering.scalarExtremaResults[resultKey]
	if !cached {
		result = aggregateExtremaResult{
			winnerAlias: quoteIdentifier(fmt.Sprintf(
				"__os_stats_extrema_winner_%d",
				measureIndex,
			)),
			typeAlias: quoteIdentifier(fmt.Sprintf(
				"__os_stats_extrema_type_%d",
				measureIndex,
			)),
		}
		lowering.scalarExtremaResults[resultKey] = result
		lowering.projection = append(
			lowering.projection,
			statsExtremaScalarAggregateWinnerSQL(
				measure.Function,
				scalarInput.candidateAlias,
			)+" AS "+result.winnerAlias,
		)
		lowering.next.privateColumns = append(
			lowering.next.privateColumns,
			result.typeAlias,
		)
	}
	lowering.next.postAggregateScalarExtrema = append(
		lowering.next.postAggregateScalarExtrema,
		compiledScalarExtremaMeasure{
			winnerColumn: result.winnerAlias,
			typeColumn:   result.typeAlias,
			outputColumn: output,
		},
	)
	return fieldState{
		valueSQL:       output,
		dynamicTypeSQL: "dynamicType(" + output + ")",
		storedTypeSQL:  result.typeAlias,
		existsSQL:      "1",
		kind:           fieldKindDynamic,
	}, nil
}

func (lowering *aggregateMeasureLowering) dynamicExtrema(
	measureIndex int,
	measure plan.AggregateMeasure,
	inputKey aggregateInputCacheKey,
	output string,
	input fieldState,
	inputExists bool,
) (fieldState, error) {
	candidates, cached := lowering.extremaInputs[inputKey]
	if !cached {
		candidates = quoteIdentifier(fmt.Sprintf(
			"__os_measure_extrema_%d",
			len(lowering.extremaInputs),
		))
		lowering.extremaInputs[inputKey] = candidates
		var candidateSQL string
		var candidateArgs []any
		if inputExists && input.kind == fieldKindDynamic {
			candidateSQL, candidateArgs = statsExtremaDynamicCandidatesSQL(input)
		} else {
			stringInputSQL, err := lowering.stringMeasureInput(measure)
			if err != nil {
				return fieldState{}, err
			}
			candidateSQL = statsExtremaCandidatesSQL(stringInputSQL)
		}
		lowering.next.preAggregateColumns = append(
			lowering.next.preAggregateColumns,
			candidateSQL+" AS "+candidates,
		)
		lowering.next.preAggregateArgs = append(
			lowering.next.preAggregateArgs,
			candidateArgs...,
		)
	}
	resultKey := aggregateExtremaResultKey{
		input:    inputKey,
		function: measure.Function,
	}
	result, cached := lowering.dynamicExtremaResults[resultKey]
	if !cached {
		result = aggregateExtremaResult{
			winnerAlias: quoteIdentifier(fmt.Sprintf(
				"__os_stats_extrema_winner_%d",
				measureIndex,
			)),
			typeAlias: quoteIdentifier(fmt.Sprintf(
				"__os_stats_extrema_type_%d",
				measureIndex,
			)),
		}
		lowering.dynamicExtremaResults[resultKey] = result
		lowering.projection = append(
			lowering.projection,
			statsExtremaAggregateSQL(measure.Function, candidates)+
				" AS "+result.winnerAlias,
			statsExtremaStoredTypeSQL(result.winnerAlias)+
				" AS "+result.typeAlias,
		)
		lowering.next.privateColumns = append(lowering.next.privateColumns, result.typeAlias)
	}
	lowering.projection = append(lowering.projection, result.winnerAlias+" AS "+output)
	return fieldState{
		valueSQL:       output,
		dynamicTypeSQL: "dynamicType(" + output + ")",
		storedTypeSQL:  result.typeAlias,
		existsSQL:      "1",
		kind:           fieldKindDynamic,
	}, nil
}

func (lowering *aggregateMeasureLowering) resolveMeasureInput(
	measure plan.AggregateMeasure,
) (fieldState, bool, error) {
	if measure.InputExpression != nil {
		cached, err := lowering.aggregateExpressionInputFor(measure.InputExpression)
		if err != nil {
			return fieldState{}, false, err
		}
		return cached.field, !cached.field.alwaysNull, nil
	}
	return resolveCompiledField(measure.Input, lowering.state)
}

func (lowering *aggregateMeasureLowering) stringMeasureInput(
	measure plan.AggregateMeasure,
) (string, error) {
	if measure.InputExpression != nil {
		return lowering.stringInputForExpression(measure.InputExpression)
	}
	return lowering.stringInputFor(measure.Input)
}

func (lowering *aggregateMeasureLowering) chronological(
	measure plan.AggregateMeasure,
	inputKey aggregateInputCacheKey,
	output string,
	measureState fieldState,
) (fieldState, error) {
	input, err := lowering.chronologicalMeasureInput(measure)
	if err != nil {
		return fieldState{}, err
	}
	if measure.Function == plan.AggregateFunctionEarliestTime ||
		measure.Function == plan.AggregateFunctionLatestTime {
		timeField, timeExists := lowering.state.visible["_time"]
		if !timeExists || timeField.kind != fieldKindTime || !timeField.canonicalTime {
			return fieldState{}, errors.New(
				"compile ClickHouse aggregate: occurrence time has no canonical _time input",
			)
		}
		valueSQL, valueErr := statsOccurrenceTimeAggregateSQL(
			measure.Function,
			input.candidatesAlias,
			lowering.chronologicalRowKeyFor(),
			percentileInputSQL(timeField),
		)
		if valueErr != nil {
			return fieldState{}, valueErr
		}
		lowering.projection = append(lowering.projection, valueSQL+" AS "+output)
		measureState.numberType = "Float64"
		return measureState, nil
	}
	rowKey := lowering.chronologicalRowKeyFor()
	if measure.Function == plan.AggregateFunctionFirst ||
		measure.Function == plan.AggregateFunctionLast {
		rowKey, _, err = lowering.listRowOrdinalFor()
		if err != nil {
			return fieldState{}, err
		}
	}
	resultKey := aggregateChronologicalResultKey{
		input:    inputKey,
		function: measure.Function,
	}
	result, cached := lowering.chronologicalResults[resultKey]
	if !cached {
		ordinal := len(lowering.chronologicalResults)
		result = aggregateChronologicalResult{
			winnerAlias: quoteIdentifier(fmt.Sprintf(
				"__os_chronological_winner_%d",
				ordinal,
			)),
			typeAlias: quoteIdentifier(fmt.Sprintf(
				"__os_chronological_type_%d",
				ordinal,
			)),
		}
		lowering.chronologicalResults[resultKey] = result
		aggregateSQL, aggregateErr := chronologicalAggregateSQL(
			measure.Function,
			input.candidatesAlias,
			rowKey,
			input.multiple,
		)
		if aggregateErr != nil {
			return fieldState{}, aggregateErr
		}
		lowering.projection = append(
			lowering.projection,
			aggregateSQL+" AS "+result.winnerAlias,
		)
		lowering.next.privateColumns = append(lowering.next.privateColumns, result.typeAlias)
	}
	lowering.next.postAggregateChronological = append(
		lowering.next.postAggregateChronological,
		compiledChronologicalMeasure{
			winnerColumn:     result.winnerAlias,
			validationColumn: input.validationAlias,
			typeColumn:       result.typeAlias,
			outputColumn:     output,
		},
	)
	return fieldState{
		valueSQL:       output,
		dynamicTypeSQL: "dynamicType(" + output + ")",
		storedTypeSQL:  result.typeAlias,
		existsSQL:      "1",
		kind:           fieldKindDynamic,
	}, nil
}

func (lowering *aggregateMeasureLowering) chronologicalMeasureInput(
	measure plan.AggregateMeasure,
) (aggregateChronologicalInput, error) {
	if measure.InputExpression != nil {
		return lowering.chronologicalInputForExpression(measure.InputExpression)
	}
	return lowering.chronologicalInputFor(measure.Input)
}

func (lowering *aggregateMeasureLowering) distribution(
	measureIndex int,
	measure plan.AggregateMeasure,
	output string,
	measureState fieldState,
) (fieldState, error) {
	inputSQL, err := lowering.stringMeasureInput(measure)
	if err != nil {
		return fieldState{}, err
	}
	modeInput, modeInputKnown, err := lowering.resolveMeasureInput(measure)
	if err != nil {
		return fieldState{}, err
	}
	compiled, supported := statsDistributionArrayAggregateSQL(
		measure.Function,
		measure.Percentile,
		inputSQL,
	)
	if !supported {
		return fieldState{}, errors.New(
			"compile ClickHouse aggregate: string distribution lowering is invalid",
		)
	}
	modeSemanticBytesSQL := ""
	if measure.Function == plan.AggregateFunctionMode && modeInputKnown &&
		modeInput.stringOrBytes &&
		(modeInput.kind == fieldKindString || modeInput.kind == fieldKindStringArray) {
		if modeInput.semanticBytesSQL == "" && modeInput.kind == fieldKindString {
			return fieldState{}, errors.New(
				"compile ClickHouse aggregate: mode String-or-Bytes input lacks semantic Bytes provenance",
			)
		}
		modeValuesInput := quoteIdentifier(fmt.Sprintf(
			"__os_measure_mode_values_%d",
			measureIndex,
		))
		modeSemanticInput := quoteIdentifier(fmt.Sprintf(
			"__os_measure_semantic_bytes_%d",
			measureIndex,
		))
		modeExistsSQL := modeInput.existsSQL
		if modeExistsSQL == "" {
			modeExistsSQL = "1"
		}
		modeValuesSQL := "if(" + modeExistsSQL + " AND isNotNull(" +
			modeInput.valueSQL + "), [assumeNotNull(" + modeInput.valueSQL +
			")], CAST([], 'Array(String)'))"
		modeSemanticSQL := "if(" + modeExistsSQL + " AND isNotNull(" +
			modeInput.valueSQL + "), [toUInt8(ifNull(" +
			modeInput.semanticBytesSQL + ", 0))], CAST([], 'Array(UInt8)'))"
		if modeInput.kind == fieldKindStringArray {
			modeValuesSQL = "if(" + modeExistsSQL + ", " +
				modeInput.valueSQL + ", CAST([], 'Array(String)'))"
			modeSemanticSQL = "arrayMap(value -> toUInt8(NOT isValidUTF8(value)), " +
				modeValuesSQL + ")"
		}
		lowering.next.preAggregateColumns = append(
			lowering.next.preAggregateColumns,
			modeValuesSQL+" AS "+modeValuesInput,
			modeSemanticSQL+" AS "+modeSemanticInput,
		)
		lowering.next.preAggregateArgs = append(
			lowering.next.preAggregateArgs,
			modeInput.existsArgs...,
		)
		lowering.next.preAggregateArgs = append(
			lowering.next.preAggregateArgs,
			modeInput.existsArgs...,
		)
		modeLowering := statsExactModeWithSemanticBytesSQL(
			modeValuesInput,
			modeSemanticInput,
		)
		compiled.SQL = modeLowering.ValueSQL
		modeSemanticOutput := quoteIdentifier(fmt.Sprintf(
			"__os_mode_semantic_bytes_%d",
			measureIndex,
		))
		lowering.projection = append(
			lowering.projection,
			modeLowering.SemanticBytesSQL+" AS "+modeSemanticOutput,
		)
		lowering.next.privateColumns = append(
			lowering.next.privateColumns,
			modeSemanticOutput,
		)
		modeSemanticBytesSQL = modeSemanticOutput
	}
	lowering.projection = append(lowering.projection, compiled.SQL+" AS "+output)
	switch compiled.Result {
	case statsDistributionResultUInt64:
		measureState.numberType = "UInt64"
	case statsDistributionResultFloat64:
		measureState.numberType = "Float64"
	case statsDistributionResultNullableString:
		measureState.kind = fieldKindString
		measureState.numberType = ""
		measureState.existsSQL = "isNotNull(" + output + ")"
		if measure.Function == plan.AggregateFunctionMode {
			measureState.stringOrBytes = true
			measureState.stringOrBytesNullable = true
			measureState.semanticBytesByUTF8Validity = modeSemanticBytesSQL == ""
			measureState.semanticBytesSQL = modeSemanticBytesSQL
			if measureState.semanticBytesSQL == "" {
				measureState.semanticBytesSQL = "toUInt8(isNotNull(" + output +
					") AND NOT isValidUTF8(assumeNotNull(" + output + ")))"
			}
			measureState.textEligibleSQL = "(ifNull(" +
				measureState.semanticBytesSQL + ", 0) = 0 AND isNotNull(" +
				output + ") AND isValidUTF8(assumeNotNull(" + output + ")))"
			measureState.textEligibleBySemanticBytes = true
		}
		if measure.InputExpression != nil || modeInputKnown {
			measureState.maxStringBytes = fieldStateStringByteBound(modeInput)
		}
	default:
		return fieldState{}, errors.New(
			"compile ClickHouse aggregate: distribution result kind is invalid",
		)
	}
	return measureState, nil
}

func (lowering *aggregateMeasureLowering) distinct(
	measure plan.AggregateMeasure,
	inputKey aggregateInputCacheKey,
	output string,
	measureState fieldState,
) (fieldState, error) {
	inputSQL, err := lowering.stringMeasureInput(measure)
	if err != nil {
		return fieldState{}, err
	}
	_, publishesValues := lowering.valuesInputs[inputKey]
	if measure.Function == plan.AggregateFunctionDistinctCount && !publishesValues {
		cardinalityColumn, cached := lowering.distinctCounts[inputKey]
		if !cached {
			cardinalityColumn = quoteIdentifier(fmt.Sprintf(
				"__os_dc_cardinality_%d",
				len(lowering.distinctCounts),
			))
			lowering.distinctCounts[inputKey] = cardinalityColumn
			lowering.projection = append(
				lowering.projection,
				distinctCountCardinalitySQL(inputSQL)+" AS "+cardinalityColumn,
			)
		}
		lowering.next.postAggregateDistinctCounts = append(
			lowering.next.postAggregateDistinctCounts,
			compiledDistinctCount{
				cardinalityColumn: cardinalityColumn,
				outputColumn:      output,
			},
		)
		measureState.numberType = "UInt64"
		return measureState, nil
	}
	setColumn, cached := lowering.exactStringSets[inputKey]
	if !cached {
		setColumn = quoteIdentifier(fmt.Sprintf(
			"__os_exact_strings_%d",
			len(lowering.exactStringSets),
		))
		lowering.exactStringSets[inputKey] = setColumn
		lowering.projection = append(
			lowering.projection,
			exactDistinctStringSetSQL(inputSQL, uint64(MaximumStatsValuesPerGroup))+
				" AS "+setColumn,
		)
	}
	lowering.next.postAggregateExactStrings = append(
		lowering.next.postAggregateExactStrings,
		compiledExactStringMeasure{
			setColumn:    setColumn,
			outputColumn: output,
			function:     measure.Function,
		},
	)
	if measure.Function == plan.AggregateFunctionDistinctCount {
		measureState.numberType = "UInt64"
	} else {
		measureState.kind = fieldKindStringArray
		measureState.mvSortedLexicographic = true
		measureState.stringOrBytes = true
		measureState.existsSQL = "notEmpty(" + output + ")"
	}
	return measureState, nil
}

func (lowering *aggregateMeasureLowering) list(
	measure plan.AggregateMeasure,
	inputKey aggregateInputCacheKey,
	output string,
	measureState fieldState,
) (fieldState, error) {
	_, inputExists, err := lowering.resolveMeasureInput(measure)
	if err != nil {
		return fieldState{}, err
	}
	if !inputExists {
		lowering.projection = append(
			lowering.projection,
			"groupArrayArray(1)(CAST([], 'Array(String)')) AS "+output,
		)
		measureState.kind = fieldKindStringArray
		measureState.stringOrBytes = true
		measureState.existsSQL = "notEmpty(" + output + ")"
		return measureState, nil
	}
	inputSQL, err := lowering.stringMeasureInput(measure)
	if err != nil {
		return fieldState{}, err
	}
	rowOrdinal, windowOrder, err := lowering.listRowOrdinalFor()
	if err != nil {
		return fieldState{}, err
	}
	list, cached := lowering.orderedStringLists[inputKey]
	if !cached {
		ordinal := len(lowering.orderedStringLists)
		priorElements := quoteIdentifier(fmt.Sprintf(
			"__os_list_prior_elements_%d",
			ordinal,
		))
		priorBytes := quoteIdentifier(fmt.Sprintf(
			"__os_list_prior_bytes_%d",
			ordinal,
		))
		frame := " OVER (" + windowOrder +
			" ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)"
		lowering.next.preAggregateListWindowColumns = append(
			lowering.next.preAggregateListWindowColumns,
			"ifNull(sum(toUInt128(length("+inputSQL+")))"+frame+
				", toUInt128(0)) AS "+priorElements,
			"ifNull(sum("+stringArrayPayloadBytesSQL(inputSQL)+")"+frame+
				", toUInt128(0)) AS "+priorBytes,
		)
		rowState := quoteIdentifier(fmt.Sprintf(
			"__os_list_row_state_%d",
			ordinal,
		))
		lowering.next.preAggregateListCandidateColumns = append(
			lowering.next.preAggregateListCandidateColumns,
			boundedOrderedStringRowStateSQL(
				rowOrdinal,
				inputSQL,
				priorElements,
				priorBytes,
			)+" AS "+rowState,
		)
		list.listColumn = quoteIdentifier(fmt.Sprintf(
			"__os_ordered_strings_%d",
			ordinal,
		))
		list.overflowColumn = quoteIdentifier(fmt.Sprintf(
			"__os_ordered_strings_bytes_overflow_%d",
			ordinal,
		))
		lowering.orderedStringLists[inputKey] = list
		lowering.projection = append(
			lowering.projection,
			boundedOrderedStringListSQL("tupleElement("+rowState+", 1)")+
				" AS "+list.listColumn,
			"max(tupleElement("+rowState+", 2)) AS "+list.overflowColumn,
		)
	}
	lowering.next.postAggregateOrderedStrings = append(
		lowering.next.postAggregateOrderedStrings,
		compiledOrderedStringMeasure{
			listColumn:     list.listColumn,
			overflowColumn: list.overflowColumn,
			outputColumn:   output,
		},
	)
	measureState.kind = fieldKindStringArray
	measureState.stringOrBytes = true
	measureState.existsSQL = "notEmpty(" + output + ")"
	return measureState, nil
}

func resolveCountValueInput(
	input plan.FieldRef,
	state compileState,
) (string, []any, error) {
	field, resolved, err := resolveCompiledField(input, state)
	if err != nil {
		return "", nil, err
	}
	if !resolved {
		return "toUInt64(0)", nil, nil
	}
	inputSQL, args := countValueInputSQL(field)
	return inputSQL, args, nil
}

func countValueInputSQL(field fieldState) (string, []any) {
	if field.kind == fieldKindStringArray {
		// A fixed multivalue is physically non-null and its empty representation
		// has cardinality zero, so its logical presence predicate is unnecessary.
		return "toUInt64(length(" + field.valueSQL + "))", nil
	}
	if field.kind == fieldKindDynamicArray {
		cardinality := "toUInt64(arrayCount(element -> dynamicType(element) != 'None', " +
			field.valueSQL + "))"
		if field.existsSQL != "" && field.existsSQL != "1" {
			return "if(" + field.existsSQL + ", " + cardinality +
				", toUInt64(0))", append([]any(nil), field.existsArgs...)
		}
		return cardinality, nil
	}

	existsSQL := field.existsSQL
	if existsSQL == "" {
		existsSQL = "1"
	}
	args := append([]any(nil), field.existsArgs...)
	if field.kind != fieldKindDynamic {
		return "toUInt64((" + existsSQL + ") AND isNotNull(" + field.valueSQL + "))", args
	}

	typeSQL := dynamicTypeExpression(field)
	arrayCount := dynamicNonNullArrayCardinalitySQL(field.valueSQL)
	descendantCount := "toUInt64(0)"
	if field.descendantSQL != "" {
		// Non-empty typed objects are stored as flattened leaves. The object
		// parent is still one present field occurrence. Calculated field copies
		// can retain those descendants while binding an exact Dynamic None, so
		// descendant presence must also be the None fallback.
		descendantCount = "if(" + field.descendantSQL + ", toUInt64(1), toUInt64(0))"
		args = append(args, field.descendantArgs...)
	}
	return "if((" + existsSQL + ") AND " + typeSQL + " != 'None', " +
		"multiIf(" +
		typeSQL + " = 'Array(Dynamic)', " + arrayCount + ", " +
		"startsWith(" + typeSQL + ", 'Array('), " +
		"ifNull(toUInt64(length(" + field.valueSQL + ")), toUInt64(0)), " +
		"toUInt64(1)), " +
		descendantCount + ")", args
}

func dynamicNonNullArrayCardinalitySQL(valueSQL string) string {
	return "toUInt64(arrayCount(element -> dynamicType(element) != 'None', " +
		"dynamicElement(" + valueSQL + ", 'Array(Dynamic)')))"
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

func statsPercentileLevelSQL(percentile uint8) string {
	if percentile%10 == 0 {
		return "0." + strconv.Itoa(int(percentile/10))
	}
	return fmt.Sprintf("0.%02d", percentile)
}

func singlePercentileArrayAggregateSQL(
	percentile uint8,
	inputSQL string,
) string {
	return "arrayElementOrNull(quantilesGKOrNullArray(100, " +
		statsPercentileLevelSQL(percentile) + ")(" + inputSQL + "), 1)"
}

func numericArrayAggregateSQL(function plan.AggregateFunction, inputSQL string) (string, bool) {
	switch function {
	case plan.AggregateFunctionSum:
		return "sumOrNullArray(" + inputSQL + ")", true
	case plan.AggregateFunctionAverage:
		return "avgOrNullArray(" + inputSQL + ")", true
	default:
		return "", false
	}
}

func numericArrayInputSQL(field fieldState) (string, []any) {
	empty := "CAST([], 'Array(Float64)')"
	if field.kind == fieldKindStringArray {
		value := compactNullableArraySQL(
			"arrayMap(element -> " + finiteFloatOrNullSQL("element") + ", " + field.valueSQL + ")",
		)
		if field.existsSQL == "" || field.existsSQL == "1" {
			return value, nil
		}
		return "if(" + field.existsSQL + ", " + value + ", " + empty + ")",
			append([]any(nil), field.existsArgs...)
	}
	if field.kind == fieldKindDynamicArray {
		value := compactNullableArraySQL(
			"arrayMap(element -> " + dynamicFiniteFloatOrNullSQL(
				"element", "dynamicType(element)",
			) + ", " + field.valueSQL + ")",
		)
		if field.existsSQL == "" || field.existsSQL == "1" {
			return value, nil
		}
		return "if(" + field.existsSQL + ", " + value + ", " + empty + ")",
			append([]any(nil), field.existsArgs...)
	}
	scalar := percentileInputSQL(field)
	scalarArray := compactNullableArraySQL("[" + scalar + "]")
	value := scalarArray
	if field.kind == fieldKindDynamic {
		element := dynamicFiniteFloatOrNullSQL("element", "dynamicType(element)")
		array := compactNullableArraySQL(
			"arrayMap(element -> " + element + ", dynamicElement(" + field.valueSQL + ", 'Array(Dynamic)'))",
		)
		value = "if(" + dynamicTypeExpression(field) + " = 'Array(Dynamic)', " + array + ", " + scalarArray + ")"
	}
	if field.existsSQL == "" || field.existsSQL == "1" {
		return value, nil
	}
	return "if(" + field.existsSQL + ", " + value + ", " + empty + ")", append([]any(nil), field.existsArgs...)
}

func stringArrayInputSQL(field fieldState) (string, []any) {
	empty := "CAST([], 'Array(String)')"
	if field.kind == fieldKindStringArray {
		if field.existsSQL == "" || field.existsSQL == "1" {
			return field.valueSQL, nil
		}
		return "if(" + field.existsSQL + ", " + field.valueSQL + ", " + empty + ")",
			append([]any(nil), field.existsArgs...)
	}
	if field.kind == fieldKindDynamicArray {
		value := "arrayMap(element -> " + nativeMVCanonicalTextSQL("element") +
			", arrayFilter(element -> dynamicType(element) != 'None', " +
			field.valueSQL + "))"
		if field.existsSQL == "" || field.existsSQL == "1" {
			return value, nil
		}
		return "if(" + field.existsSQL + ", " + value + ", " + empty + ")",
			append([]any(nil), field.existsArgs...)
	}
	if field.kind == fieldKindDynamic {
		state, args := dynamicStringArrayStateSQL(field, "1")
		stateAlias := "__os_dynamic_string_array_state"
		body := "if(throwIf(toUInt8(tupleElement(" + stateAlias + ", 2)), '" +
			UnsupportedStatsMeasureValueMarker + "') = 0, tupleElement(" +
			stateAlias + ", 1), " + empty + ")"
		return bindSQLExpressions(
			[]string{stateAlias},
			[]string{state},
			body,
		), args
	}
	scalar := statsTextEligibleScalarStringOrNullSQL(field)
	value := compactNullableArraySQL("[" + scalar + "]")
	if field.existsSQL == "" || field.existsSQL == "1" {
		return value, nil
	}
	return "if(" + field.existsSQL + ", " + value + ", " + empty + ")", append([]any(nil), field.existsArgs...)
}

// dynamicStringArrayStateSQL normalizes one Dynamic field into an exact String
// array plus an unsupported-container bit. Callers choose whether to throw
// immediately or retain the bit for deferred whole-result validation. A false
// row-eligibility expression short-circuits member inspection.
func dynamicStringArrayStateSQL(
	field fieldState,
	rowEligibleSQL string,
) (string, []any) {
	emptyValues := "CAST([], 'Array(String)')"
	empty := "tuple(" + emptyValues + ", toUInt8(0))"
	scalar := compileDynamicMeasureScalar(field)
	invalid := "tuple(" + emptyValues + ", toUInt8(1))"
	scalarState := "tuple(if(" + scalar.eligibleSQL + ", [" +
		scalar.lexicalSQL + "], " + emptyValues + "), toUInt8(" +
		scalar.invalidSQL + "))"

	element := fieldState{
		valueSQL:       "element",
		dynamicTypeSQL: "dynamicType(element)",
		kind:           fieldKindDynamic,
	}
	member := compileDynamicMeasureScalar(element)
	nullString := "CAST(NULL AS Nullable(String))"
	dynamicValues := "dynamicElement(" + field.valueSQL + ", 'Array(Dynamic)')"
	memberStates := "arrayMap(element -> tuple(" +
		"if(" + member.eligibleSQL + ", CAST(" + member.lexicalSQL +
		" AS Nullable(String)), " + nullString + "), toUInt8(" +
		member.invalidSQL + ")), " + dynamicValues + ")"
	memberStatesAlias := "__os_dynamic_string_member_states"
	arrayState := bindSQLExpressions(
		[]string{memberStatesAlias},
		[]string{memberStates},
		"tuple("+compactNullableArraySQL(
			"arrayMap(member -> tupleElement(member, 1), "+memberStatesAlias+")",
		)+", toUInt8(arrayExists(member -> tupleElement(member, 2) != 0, "+
			memberStatesAlias+")))",
	)

	existsSQL, descendantSQL, args := dynamicPresenceOperands(field)
	if rowEligibleSQL == "" {
		rowEligibleSQL = "1"
	}
	value := "multiIf(" +
		"row_eligible = 0, " + empty + ", " +
		"descendant_present != 0, " + invalid + ", " +
		"field_present = 0 OR " + scalar.typeSQL + " = 'None', " + empty + ", " +
		scalar.typeSQL + " = 'Array(Dynamic)', " + arrayState + ", " +
		scalarState + ")"
	return "arrayElement(arrayMap((row_eligible, field_present, descendant_present) -> " +
		value + ", [toUInt8(" + rowEligibleSQL + ")], [toUInt8(" + existsSQL +
		")], [toUInt8(" + descendantSQL + ")]), 1)", args
}

// eventStatsExactStringDynamicMeasureSQL normalizes the shared eventstats dc,
// values, and list input while retaining unsupported data as a constant-size
// bit, so downstream projection or limits cannot hide failure.
func eventStatsExactStringDynamicMeasureSQL(
	field fieldState,
	rowEligibleSQL string,
) (string, []any) {
	return dynamicStringArrayStateSQL(field, rowEligibleSQL)
}

type chronologicalDirections struct {
	earliest bool
	latest   bool
}

func emptyChronologicalCandidatesSQL() string {
	return "tuple(CAST('' AS String), CAST('' AS String), " +
		"toUInt8(0), toUInt8(0), toUInt64(0), toUInt64(0))"
}

func immutableChronologicalRowKeySQL() string {
	return "tuple(" + strings.Join([]string{
		quoteIdentifier(internalSortTimeColumn),
		quoteIdentifier(internalSortIDColumn),
		quoteIdentifier(internalSortVisibilityColumn),
		quoteIdentifier(internalSortSourceIdentityColumn),
	}, ", ") + ")"
}

func emptySingleChronologicalCandidateSQL() string {
	return "tuple(CAST('' AS String), toUInt64(0), toUInt8(0), toUInt8(0))"
}

// singleChronologicalCandidateSQL reduces one event field to the only
// direction a single-measure chronological aggregate consumes: selected lexical
// value, original one-based member ordinal, eligible bit, and invalid bit.
// Unlike multi-measure stats, this avoids selecting and retaining the opposite
// end of every multivalue.
func singleChronologicalCandidateSQL(
	function plan.AggregateFunction,
	field fieldState,
	exists bool,
) (string, []any, bool, error) {
	arrayIndexSelector := "arrayFirstIndex"
	fixedArrayIndex := "1"
	switch function {
	case plan.AggregateFunctionEarliest:
	case plan.AggregateFunctionLatest:
		arrayIndexSelector = "arrayLastIndex"
		fixedArrayIndex = "-1"
	default:
		return "", nil, false, fmt.Errorf(
			"compile ClickHouse chronological candidate: unsupported function %d",
			function,
		)
	}

	empty := emptySingleChronologicalCandidateSQL()
	if !exists {
		return empty, nil, false, nil
	}
	if field.kind == fieldKindDynamicArray {
		field.valueSQL = "arrayMap(element -> " + nativeMVCanonicalTextSQL("element") +
			", arrayFilter(element -> dynamicType(element) != 'None', " +
			field.valueSQL + "))"
		field.kind = fieldKindStringArray
		return singleChronologicalCandidateSQL(function, field, true)
	}

	if field.kind == fieldKindStringArray {
		values := field.valueSQL
		var args []any
		if field.existsSQL != "" && field.existsSQL != "1" {
			values = "if(" + field.existsSQL + ", " + values +
				", CAST([], 'Array(String)'))"
			args = append(args, field.existsArgs...)
		}
		count := "toUInt64(length(" + values + "))"
		ordinal := "toUInt64(1)"
		if function == plan.AggregateFunctionLatest {
			ordinal = count
		}
		return "tuple(" +
				"if(" + count + " != 0, arrayElement(" + values + ", " +
				fixedArrayIndex + "), CAST('' AS String)), " +
				"if(" + count + " != 0, " + ordinal + ", toUInt64(0)), " +
				"toUInt8(" + count + " != 0), toUInt8(0))",
			args,
			false,
			nil
	}

	if field.kind != fieldKindDynamic {
		value := statsScalarStringOrNullSQL(field)
		existsSQL := field.existsSQL
		if existsSQL == "" {
			existsSQL = "1"
		}
		value = "if(" + existsSQL + ", " + value +
			", CAST(NULL AS Nullable(String)))"
		present := "isNotNull(" + value + ")"
		return "tuple(" +
				"ifNull(" + value + ", CAST('' AS String)), " +
				"if(" + present + ", toUInt64(1), toUInt64(0)), " +
				"toUInt8(" + present + "), toUInt8(0))",
			append([]any(nil), field.existsArgs...),
			false,
			nil
	}

	typeSQL := dynamicTypeExpression(field)
	scalarSupported, scalarLexical := statsByScalarExpressions(field)
	element := fieldState{
		valueSQL:       "element",
		dynamicTypeSQL: "dynamicType(element)",
		kind:           fieldKindDynamic,
	}
	elementSupported, _ := statsByScalarExpressions(element)
	elementType := dynamicTypeExpression(element)
	elementEligible := "(" + elementType + " != 'None' AND " + elementSupported + ")"
	elementInvalid := "(" + elementType + " != 'None' AND NOT (" + elementSupported + "))"
	values := "dynamicElement(" + field.valueSQL + ", 'Array(Dynamic)')"
	ordinal := "toUInt64(" + arrayIndexSelector + "(element -> " +
		elementEligible + ", " + values + "))"
	memberInvalid := "toUInt8(arrayExists(element -> " + elementInvalid +
		", " + values + "))"
	selectedElement := fieldState{
		valueSQL:       "arrayElement(" + values + ", selected_ordinal)",
		dynamicTypeSQL: "dynamicType(arrayElement(" + values + ", selected_ordinal))",
		kind:           fieldKindDynamic,
	}
	_, selectedLexical := statsByScalarExpressions(selectedElement)
	selectedState := "arrayElement(arrayMap(" +
		"(selected_ordinal, member_invalid) -> tuple(" +
		"if(selected_ordinal != 0, " + selectedLexical + ", CAST('' AS String)), " +
		"selected_ordinal, toUInt8(selected_ordinal != 0), member_invalid), " +
		"[" + ordinal + "], [" + memberInvalid + "]), 1)"
	scalar := "tuple(" + scalarLexical +
		", toUInt64(1), toUInt8(1), toUInt8(0))"
	invalid := "tuple(CAST('' AS String), toUInt64(0), toUInt8(0), toUInt8(1))"

	existsSQL, descendantSQL, args := dynamicPresenceOperands(field)
	value := "multiIf(" +
		"descendant_present != 0, " + invalid + ", " +
		"field_present = 0 OR " + typeSQL + " = 'None', " + empty + ", " +
		typeSQL + " = 'Array(Dynamic)', " + selectedState + ", " +
		scalarSupported + ", " + scalar + ", " +
		invalid + ")"
	return "arrayElement(arrayMap((field_present, descendant_present) -> " + value +
			", [toUInt8(" + existsSQL + ")], [toUInt8(" + descendantSQL + ")]), 1)",
		args,
		true,
		nil
}

func singleChronologicalAggregateSQL(
	function plan.AggregateFunction,
	inputSQL string,
) (string, error) {
	candidate := "tupleElement(" + inputSQL + ", 1)"
	rowKey := "tupleElement(" + inputSQL + ", 2)"
	aggregate := "argMinOrNullIf"
	switch function {
	case plan.AggregateFunctionEarliest:
	case plan.AggregateFunctionLatest:
		aggregate = "argMaxOrNullIf"
	default:
		return "", fmt.Errorf(
			"compile ClickHouse single chronological aggregate: unsupported function %d",
			function,
		)
	}
	value := "tupleElement(" + candidate + ", 1)"
	ordinal := "tupleElement(" + candidate + ", 2)"
	present := "tupleElement(" + candidate + ", 3)"
	key := "tuple(" + rowKey + ", " + ordinal + ")"
	return aggregate + "(" + value + ", " + key + ", " + present + " != 0)", nil
}

func chronologicalAggregateSQL(
	function plan.AggregateFunction,
	candidatesSQL string,
	rowKeySQL string,
	multiple bool,
) (string, error) {
	eligible := "tupleElement(" + candidatesSQL + ", 3)"
	value := "tupleElement(" + candidatesSQL + ", 1)"
	ordinal := "toUInt64(1)"
	aggregate := "argMinOrNullIf"
	switch function {
	case plan.AggregateFunctionEarliest, plan.AggregateFunctionFirst:
		if multiple {
			ordinal = "tupleElement(" + candidatesSQL + ", 5)"
		}
	case plan.AggregateFunctionLatest, plan.AggregateFunctionLast:
		value = "tupleElement(" + candidatesSQL + ", 2)"
		if multiple {
			ordinal = "tupleElement(" + candidatesSQL + ", 6)"
		}
		aggregate = "argMaxOrNullIf"
	default:
		return "", fmt.Errorf(
			"compile ClickHouse chronological aggregate: unsupported function %d",
			function,
		)
	}
	key := "tuple(" + rowKeySQL + ", " + ordinal + ")"
	return aggregate + "(" + value + ", " + key + ", " + eligible + " != 0)", nil
}

func statsOccurrenceTimeAggregateSQL(
	function plan.AggregateFunction,
	candidatesSQL string,
	rowKeySQL string,
	timeSQL string,
) (string, error) {
	aggregate := "argMinOrNullIf"
	switch function {
	case plan.AggregateFunctionEarliestTime:
	case plan.AggregateFunctionLatestTime:
		aggregate = "argMaxOrNullIf"
	default:
		return "", fmt.Errorf(
			"compile ClickHouse occurrence-time aggregate: unsupported function %d",
			function,
		)
	}
	eligible := "tupleElement(" + candidatesSQL + ", 3) != 0"
	invalid := "tupleElement(" + candidatesSQL + ", 4) != 0"
	winner := aggregate + "(" + timeSQL + ", " + rowKeySQL + ", " + eligible + ")"
	return "if(max(toUInt8(" + invalid + ")) != 0, toFloat64(throwIf(" +
		"toUInt8(1), '" + UnsupportedStatsMeasureValueMarker + "')), " + winner + ")", nil
}

// statsRateAggregateSQL implements the documented no-reset endpoint formula.
// Splunk's separate "largest value reset" behavior is not specified well
// enough by the pinned reference to reproduce without a differential oracle;
// that case remains explicitly tracked in the stats parity inventory.
func statsRateAggregateSQL(inputSQL, rowKeySQL, timeSQL string) string {
	firstValue := "arrayElementOrNull(" + inputSQL + ", 1)"
	lastValue := "arrayElementOrNull(" + inputSQL + ", -1)"
	firstEligible := "isNotNull(" + firstValue + ")"
	lastEligible := "isNotNull(" + lastValue + ")"
	earliestValue := "argMinOrNullIf(" + firstValue + ", " + rowKeySQL + ", " + firstEligible + ")"
	latestValue := "argMaxOrNullIf(" + lastValue + ", " + rowKeySQL + ", " + lastEligible + ")"
	earliestTime := "argMinOrNullIf(" + timeSQL + ", " + rowKeySQL + ", " + firstEligible + ")"
	latestTime := "argMaxOrNullIf(" + timeSQL + ", " + rowKeySQL + ", " + lastEligible + ")"
	pointCount := "countIf(" + firstEligible + " OR " + lastEligible + ")"
	duration := "(" + latestTime + " - " + earliestTime + ")"
	nullFloat := "CAST(NULL AS Nullable(Float64))"
	return "if(" + pointCount + " < 2 OR isNull(" + duration + ") OR " + duration +
		" = 0, " + nullFloat + ", ifNotFinite((" + latestValue + " - " +
		earliestValue + ") / " + duration + ", " + nullFloat + "))"
}

func eventStatsChronologicalValidationSQL(inputSQL, _ string) string {
	return "maxOrDefault(toUInt8(tupleElement(tupleElement(" + inputSQL +
		", 1), 4)))"
}

func chronologicalPublishedValueSQL(winnerSQL string) string {
	nonNull := "assumeNotNull(" + winnerSQL + ")"
	return "if(isNull(" + winnerSQL + "), CAST(NULL AS Dynamic), if(" +
		"isValidUTF8(" + nonNull + "), CAST(" + nonNull + " AS Dynamic), " +
		bytesEnvelopePayloadDynamicSQL(rawStdBase64EncodeSQL(nonNull)) + "))"
}

func chronologicalPublishedTypeSQL(winnerSQL string) string {
	return statsExtremaStoredTypeFromConditionsSQL(
		"isNull("+winnerSQL+")",
		"0",
		"1",
		"assumeNotNull("+winnerSQL+")",
	)
}

// chronologicalCandidatesSQL normalizes one event field to a constant-size
// tuple: requested first and/or last eligible lexical value, an eligible bit,
// unsupported-container bit, and the original one-based requested ordinals.
// Each requested direction uses one bounded index pass over a Dynamic
// multivalue; guarded indexed lookup avoids repeating the eligibility pass or
// retaining either an Array ordering key or normalized member array.
func chronologicalCandidatesSQL(
	field fieldState,
	exists bool,
	directions chronologicalDirections,
) (string, []any, bool) {
	empty := emptyChronologicalCandidatesSQL()
	if !exists {
		return empty, nil, false
	}
	if field.kind == fieldKindDynamicArray {
		field.valueSQL = "arrayMap(element -> " + nativeMVCanonicalTextSQL("element") +
			", arrayFilter(element -> dynamicType(element) != 'None', " +
			field.valueSQL + "))"
		field.kind = fieldKindStringArray
		return chronologicalCandidatesSQL(field, true, directions)
	}

	if field.kind == fieldKindStringArray {
		values := field.valueSQL
		var args []any
		if field.existsSQL != "" && field.existsSQL != "1" {
			values = "if(" + field.existsSQL + ", " + values +
				", CAST([], 'Array(String)'))"
			args = append(args, field.existsArgs...)
		}
		count := "toUInt64(length(" + values + "))"
		firstValue := "CAST('' AS String)"
		firstOrdinal := "toUInt64(0)"
		if directions.earliest {
			firstValue = "if(" + count + " != 0, arrayElement(" + values +
				", 1), CAST('' AS String))"
			firstOrdinal = "if(" + count + " != 0, toUInt64(1), toUInt64(0))"
		}
		lastValue := "CAST('' AS String)"
		lastOrdinal := "toUInt64(0)"
		if directions.latest {
			lastValue = "if(" + count + " != 0, arrayElement(" + values +
				", -1), CAST('' AS String))"
			lastOrdinal = count
		}
		eligibleOrdinal := firstOrdinal
		if !directions.earliest {
			eligibleOrdinal = lastOrdinal
		}
		return "tuple(" + firstValue + ", " + lastValue + ", toUInt8(" +
				eligibleOrdinal + " != 0), toUInt8(0), " + firstOrdinal + ", " +
				lastOrdinal + ")",
			args,
			false
	}

	if field.kind != fieldKindDynamic {
		value := statsScalarStringOrNullSQL(field)
		existsSQL := field.existsSQL
		if existsSQL == "" {
			existsSQL = "1"
		}
		value = "if(" + existsSQL + ", " + value +
			", CAST(NULL AS Nullable(String)))"
		present := "isNotNull(" + value + ")"
		firstValue := "CAST('' AS String)"
		firstOrdinal := "toUInt64(0)"
		if directions.earliest {
			firstValue = "ifNull(" + value + ", CAST('' AS String))"
			firstOrdinal = "if(" + present + ", toUInt64(1), toUInt64(0))"
		}
		lastValue := "CAST('' AS String)"
		lastOrdinal := "toUInt64(0)"
		if directions.latest {
			lastValue = "ifNull(" + value + ", CAST('' AS String))"
			lastOrdinal = "if(" + present + ", toUInt64(1), toUInt64(0))"
		}
		return "tuple(" + firstValue + ", " + lastValue + ", toUInt8(" +
				present + "), toUInt8(0), " + firstOrdinal + ", " + lastOrdinal + ")",
			append([]any(nil), field.existsArgs...),
			false
	}

	typeSQL := dynamicTypeExpression(field)
	scalarSupported, scalarLexical := statsByScalarExpressions(field)
	element := fieldState{
		valueSQL:       "element",
		dynamicTypeSQL: "dynamicType(element)",
		kind:           fieldKindDynamic,
	}
	elementSupported, _ := statsByScalarExpressions(element)
	elementType := dynamicTypeExpression(element)
	elementEligible := "(" + elementType + " != 'None' AND " + elementSupported + ")"
	elementInvalid := "(" + elementType + " != 'None' AND NOT (" + elementSupported + "))"
	values := "dynamicElement(" + field.valueSQL + ", 'Array(Dynamic)')"
	firstOrdinal := "toUInt64(0)"
	if directions.earliest {
		firstOrdinal = "toUInt64(arrayFirstIndex(element -> " + elementEligible +
			", " + values + "))"
	}
	lastOrdinal := "toUInt64(0)"
	if directions.latest {
		lastOrdinal = "toUInt64(arrayLastIndex(element -> " + elementEligible +
			", " + values + "))"
	}
	memberInvalid := "toUInt8(arrayExists(element -> " + elementInvalid +
		", " + values + "))"
	firstLexical := "CAST('' AS String)"
	if directions.earliest {
		firstElement := fieldState{
			valueSQL:       "arrayElement(" + values + ", first_ordinal)",
			dynamicTypeSQL: "dynamicType(arrayElement(" + values + ", first_ordinal))",
			kind:           fieldKindDynamic,
		}
		_, lexical := statsByScalarExpressions(firstElement)
		firstLexical = "if(first_ordinal != 0, " + lexical +
			", CAST('' AS String))"
	}
	lastLexical := "CAST('' AS String)"
	if directions.latest {
		lastElement := fieldState{
			valueSQL:       "arrayElement(" + values + ", last_ordinal)",
			dynamicTypeSQL: "dynamicType(arrayElement(" + values + ", last_ordinal))",
			kind:           fieldKindDynamic,
		}
		_, lexical := statsByScalarExpressions(lastElement)
		lastLexical = "if(last_ordinal != 0, " + lexical +
			", CAST('' AS String))"
	}
	eligibleOrdinal := "first_ordinal"
	if !directions.earliest {
		eligibleOrdinal = "last_ordinal"
	}
	selected := "arrayElement(arrayMap(" +
		"(first_ordinal, last_ordinal, member_invalid) -> tuple(" +
		firstLexical + ", " + lastLexical + ", toUInt8(" + eligibleOrdinal +
		" != 0), member_invalid, first_ordinal, last_ordinal), " +
		"[" + firstOrdinal + "], [" + lastOrdinal + "], [" + memberInvalid + "]), 1)"
	scalarFirst := "CAST('' AS String)"
	scalarFirstOrdinal := "toUInt64(0)"
	if directions.earliest {
		scalarFirst = scalarLexical
		scalarFirstOrdinal = "toUInt64(1)"
	}
	scalarLast := "CAST('' AS String)"
	scalarLastOrdinal := "toUInt64(0)"
	if directions.latest {
		scalarLast = scalarLexical
		scalarLastOrdinal = "toUInt64(1)"
	}
	scalar := "tuple(" + scalarFirst + ", " + scalarLast +
		", toUInt8(1), toUInt8(0), " + scalarFirstOrdinal + ", " +
		scalarLastOrdinal + ")"
	invalid := "tuple(CAST('' AS String), CAST('' AS String), toUInt8(0), " +
		"toUInt8(1), toUInt64(0), toUInt64(0))"

	existsSQL, descendantSQL, args := dynamicPresenceOperands(field)
	value := "multiIf(" +
		"descendant_present != 0, " + invalid + ", " +
		"field_present = 0 OR " + typeSQL + " = 'None', " + empty + ", " +
		typeSQL + " = 'Array(Dynamic)', " + selected + ", " +
		scalarSupported + ", " + scalar + ", " +
		invalid + ")"
	return "arrayElement(arrayMap((field_present, descendant_present) -> " + value +
			", [toUInt8(" + existsSQL + ")], [toUInt8(" + descendantSQL + ")]), 1)",
		args,
		true
}

func statsScalarStringInputSQL(field fieldState) (string, []any) {
	value := statsScalarStringOrNullSQL(field)
	existsSQL := field.existsSQL
	if existsSQL == "" || existsSQL == "1" {
		return value, nil
	}
	return "if(" + existsSQL + ", " + value +
		", CAST(NULL AS Nullable(String)))", append([]any(nil), field.existsArgs...)
}

func fixedStringExtremaRawBytesSQL(field fieldState) string {
	if field.kind != fieldKindString || field.textEligibleSQL == "" {
		return "0"
	}
	return "NOT ifNull(" + field.textEligibleSQL + ", 0)"
}

func statsExtremaScalarNumberSQL(valueSQL string) string {
	value := "ifNull(" + valueSQL + ", CAST('' AS String))"
	return "if(isNotNull(" + valueSQL + "), " + statsExtremaNumericOrNullSQL(value) +
		", CAST(NULL AS Nullable(Float64)))"
}

func statsExtremaNormalizedNumberSQL(numberSQL string) string {
	return "if(assumeNotNull(" + numberSQL +
		") = 0, toFloat64(0), assumeNotNull(" + numberSQL + "))"
}

const (
	statsExtremaPublicationFloat uint8 = iota
	statsExtremaPublicationDecimal
	statsExtremaPublicationLexical
	statsExtremaPublicationEncodedBytes
)

func statsExtremaOrderingKeySQL(
	valueSQL string,
	exactKeySQL string,
	typeTieBreakSQL string,
) string {
	exact := exactNumericKeyValueSQL(exactKeySQL)
	numeric := exactNumericKeyEligibleSQL(exactKeySQL)
	return "tuple(toUInt8(NOT (" + numeric + ")), " +
		"if(" + numeric + ", tupleElement(" + exact + ", 1), toUInt8(1)), " +
		"if(" + numeric + ", tupleElement(" + exact + ", 2), toInt64(0)), " +
		"if(" + numeric + ", tupleElement(" + exact + ", 3), CAST('' AS String)), " +
		"if(" + numeric + ", CAST('' AS String), " + valueSQL + "), toUInt8(" +
		typeTieBreakSQL + "))"
}

func statsExtremaExactFloatPublicationSQL(numberSQL, exactKeySQL string) string {
	roundTrip := exactNumericOrderingKeySQL(
		"toString(" + statsExtremaNormalizedNumberSQL(numberSQL) + ")",
	)
	return statsExtremaExactFloatKeyMatchSQL(
		numberSQL,
		exactKeySQL,
		roundTrip,
	)
}

func statsExtremaExactFloatKeyMatchSQL(
	numberSQL, exactKeySQL, floatKeySQL string,
) string {
	return "isNotNull(" + numberSQL + ") AND " +
		exactNumericKeyEligibleSQL(exactKeySQL) + " AND " +
		exactKeySQL + " = " + floatKeySQL
}

func statsExtremaScalarCandidateSQL(
	valueSQL string,
	numberSQL string,
	rawBytesSQL string,
) string {
	value := "ifNull(" + valueSQL + ", CAST('' AS String))"
	if rawBytesSQL == "" {
		rawBytesSQL = "0"
	}
	rawBytes := "__os_stats_extrema_scalar_raw_bytes"
	candidate := statsExtremaPublicationCandidateSQL(
		statsExtremaPublicationCandidateInput{
			publicationValueSQL: "if(" + rawBytes + ", " +
				rawStdBase64EncodeSQL(value) + ", " + value + ")",
			orderingValueSQL: value,
			numberSQL: "if(" + rawBytes + ", CAST(NULL AS Nullable(Float64)), " +
				numberSQL + ")",
			exactTextSQL: "if(" + rawBytes + ", CAST('' AS String), " +
				boundedExactNumericOrderingInputSQL(value) + ")",
			lexicalPublicationKindSQL: "if(" + rawBytes + ", toUInt8(" +
				strconv.Itoa(int(statsExtremaPublicationEncodedBytes)) + "), toUInt8(" +
				strconv.Itoa(int(statsExtremaPublicationLexical)) + "))",
			eligibleSQL: "isNotNull(" + valueSQL + ")",
		},
	)
	return bindSQLExpressions(
		[]string{rawBytes},
		[]string{"toUInt8(ifNull(" + rawBytesSQL + ", 0)) != 0"},
		candidate,
	)
}

// statsExtremaPublicationCandidateSQL lowers one already-classified scalar to
// the fixed candidate tuple shared by scalar String extrema and row-local
// Dynamic eventstats folds:
//
//	(exact ordering key, publication kind, Float64 publication,
//	 publication text, eligible bit)
//
// The publication tuple deliberately contains no Dynamic value. This lets
// argMinOrNullIf represent an empty aggregate explicitly without attempting to
// construct Nullable(Dynamic), which ClickHouse does not support.
type statsExtremaPublicationCandidateInput struct {
	publicationValueSQL       string
	orderingValueSQL          string
	numberSQL                 string
	exactTextSQL              string
	lexicalPublicationKindSQL string
	eligibleSQL               string
}

func statsExtremaPublicationCandidateSQL(
	input statsExtremaPublicationCandidateInput,
) string {
	valueVariable := "__os_stats_extrema_value"
	orderingValueVariable := "__os_stats_extrema_ordering_value"
	numberVariable := "__os_stats_extrema_number"
	exactTextVariable := "__os_stats_extrema_exact_text"
	exactVariable := "__os_stats_extrema_exact_key"
	floatVariable := "__os_stats_extrema_float_key"
	exactFloatVariable := "__os_stats_extrema_exact_float"
	exactFloat := statsExtremaExactFloatKeyMatchSQL(
		numberVariable,
		exactVariable,
		floatVariable,
	)
	numeric := exactNumericKeyEligibleSQL(exactVariable)
	decimalInput := "if(" + numeric + ", " + valueVariable + ", CAST('0' AS String))"
	publicationKind := "toUInt8(multiIf(NOT (" + numeric + "), " +
		input.lexicalPublicationKindSQL + ", " +
		exactFloatVariable + ", " +
		strconv.Itoa(int(statsExtremaPublicationFloat)) + ", " +
		strconv.Itoa(int(statsExtremaPublicationDecimal)) + "))"
	ordering := statsExtremaOrderingKeySQL(
		orderingValueVariable,
		exactVariable,
		"if("+numeric+", toUInt8(0), "+input.lexicalPublicationKindSQL+")",
	)
	publicationNumber := "if(isNotNull(" + numberVariable + "), " +
		statsExtremaNormalizedNumberSQL(numberVariable) + ", toFloat64(0))"
	publicationText := "multiIf(NOT (" + numeric + "), " + valueVariable + ", " +
		exactFloatVariable + ", CAST('' AS String), " +
		decimalInput + ")"
	candidate := "tuple(" + ordering + ", " + publicationKind + ", " +
		publicationNumber + ", " + publicationText + ", toUInt8(" +
		input.eligibleSQL + "))"
	candidate = bindSQLExpressions(
		[]string{exactFloatVariable},
		[]string{exactFloat},
		candidate,
	)
	candidate = bindSQLExpressions(
		[]string{exactVariable, floatVariable},
		[]string{
			exactNumericOrderingKeySQL(exactTextVariable),
			trustedFiniteFloatOrderingKeySQL(
				"ifNull(" + numberVariable + ", toFloat64(0))",
			),
		},
		candidate,
	)
	return bindSQLExpressions(
		[]string{valueVariable, orderingValueVariable, numberVariable, exactTextVariable},
		[]string{
			input.publicationValueSQL,
			input.orderingValueSQL,
			input.numberSQL,
			input.exactTextSQL,
		},
		candidate,
	)
}

func statsExtremaScalarAggregateWinnerSQL(
	function plan.AggregateFunction,
	candidateSQL string,
) string {
	name := "argMinOrNullIf"
	if function == plan.AggregateFunctionMaximum {
		name = "argMaxOrNullIf"
	}
	key := "tupleElement(" + candidateSQL + ", 1)"
	eligible := "tupleElement(" + candidateSQL + ", 5) != 0"
	publication := "tuple(tupleElement(" + candidateSQL + ", 2), tupleElement(" +
		candidateSQL + ", 3), tupleElement(" + candidateSQL + ", 4))"
	return name + "(" + publication + ", " + key + ", " + eligible + ")"
}

func statsExtremaScalarValueSQL(extremeWinnerSQL string) string {
	nonNull := "assumeNotNull(" + extremeWinnerSQL + ")"
	kind := "tupleElement(" + nonNull + ", 1)"
	number := "tupleElement(" + nonNull + ", 2)"
	text := "tupleElement(" + nonNull + ", 3)"
	return "if(isNull(" + extremeWinnerSQL + "), CAST(NULL AS Dynamic), multiIf(" +
		kind + " = " + strconv.Itoa(int(statsExtremaPublicationFloat)) +
		", CAST(" + number + " AS Dynamic), " +
		kind + " = " + strconv.Itoa(int(statsExtremaPublicationDecimal)) +
		", " + decimalEnvelopeDynamicSQL(text) + ", " +
		kind + " = " + strconv.Itoa(int(statsExtremaPublicationEncodedBytes)) +
		", " + bytesEnvelopePayloadDynamicSQL(text) + ", " +
		"CAST(" + text + " AS Dynamic)))"
}

func statsExtremaScalarStoredTypeSQL(extremeWinnerSQL string) string {
	nonNull := "assumeNotNull(" + extremeWinnerSQL + ")"
	kind := "tupleElement(" + nonNull + ", 1)"
	lexical := "tupleElement(" + nonNull + ", 3)"
	ordinary := statsExtremaStoredTypeWithDecimalSQL(
		"isNull("+extremeWinnerSQL+")",
		kind+" = "+strconv.Itoa(int(statsExtremaPublicationFloat)),
		kind+" = "+strconv.Itoa(int(statsExtremaPublicationDecimal)),
		kind+" = "+strconv.Itoa(int(statsExtremaPublicationLexical)),
		lexical,
	)
	return "if(" + kind + " = " +
		strconv.Itoa(int(statsExtremaPublicationEncodedBytes)) +
		", toUInt8(" + strconv.Itoa(int(eventfields.StoredValueTypeBytes)) +
		"), " + ordinary + ")"
}

func statsExtremaCandidatesSQL(valuesSQL string) string {
	candidate := statsExtremaCandidateSQL(
		"value",
		"value",
		boundedExactNumericOrderingInputSQL("value"),
		"toUInt8("+strconv.Itoa(int(statsExtremaPublicationLexical))+")",
	)
	return "arrayMap(value -> " + candidate + ", " + valuesSQL + ")"
}

func statsExtremaCandidateSQL(
	publicationValueSQL string,
	orderingValueSQL string,
	exactTextSQL string,
	lexicalPublicationKindSQL string,
) string {
	publicationValue := "__os_stats_extrema_publication_value"
	orderingValue := "__os_stats_extrema_ordering_value"
	exactText := "__os_stats_extrema_exact_text"
	lexicalKind := "__os_stats_extrema_lexical_kind"
	number := "if(" + lexicalKind + " = toUInt8(" +
		strconv.Itoa(int(statsExtremaPublicationEncodedBytes)) + "), " +
		"CAST(NULL AS Nullable(Float64)), " +
		statsExtremaNumericOrNullSQL(publicationValue) + ")"
	exact := exactNumericOrderingKeySQL(exactText)
	exactFloat := statsExtremaExactFloatPublicationSQL("number", "exact_key")
	numeric := exactNumericKeyEligibleSQL("exact_key")
	decimalInput := "if(" + numeric + ", " + publicationValue +
		", CAST('0' AS String))"
	lexicalCandidate := "if(" + lexicalKind + " = toUInt8(" +
		strconv.Itoa(int(statsExtremaPublicationEncodedBytes)) + "), " +
		bytesEnvelopePayloadDynamicSQL(publicationValue) + ", CAST(" +
		publicationValue + " AS Dynamic))"
	candidate := "multiIf(NOT (" + numeric + "), " + lexicalCandidate + ", " +
		exactFloat + ", CAST(" + statsExtremaNormalizedNumberSQL("number") +
		" AS Dynamic), " + decimalEnvelopeDynamicSQL(decimalInput) + ")"
	key := statsExtremaOrderingKeySQL(
		orderingValue,
		"exact_key",
		"if("+numeric+", toUInt8(0), "+lexicalKind+")",
	)
	bound := bindSQLExpressions(
		[]string{"number", "exact_key"},
		[]string{number, exact},
		"tuple("+candidate+", "+key+")",
	)
	return bindSQLExpressions(
		[]string{publicationValue, orderingValue, exactText, lexicalKind},
		[]string{
			publicationValueSQL,
			orderingValueSQL,
			exactTextSQL,
			lexicalPublicationKindSQL,
		},
		bound,
	)
}

type compiledDynamicMeasureScalar struct {
	valueSQL     string
	typeSQL      string
	supportedSQL string
	lexicalSQL   string
	exactTextSQL string
	eligibleSQL  string
	invalidSQL   string
}

// compileDynamicMeasureScalar centralizes the scalar/member classification
// used by transforming stats and row-preserving eventstats. None is missing;
// supported scalar values are eligible unless an upstream text guard excludes
// them; unsupported non-None values poison the enclosing measure.
func compileDynamicMeasureScalar(field fieldState) compiledDynamicMeasureScalar {
	typeSQL := dynamicTypeExpression(field)
	supportedSQL, lexicalSQL := statsByScalarExpressions(field)
	eligibleSQL := "(" + typeSQL + " != 'None' AND " + supportedSQL + ")"
	if field.textEligibleSQL != "" {
		eligibleSQL = "(" + eligibleSQL + " AND ifNull(" +
			field.textEligibleSQL + ", 0))"
	}
	return compiledDynamicMeasureScalar{
		valueSQL:     field.valueSQL,
		typeSQL:      typeSQL,
		supportedSQL: supportedSQL,
		lexicalSQL:   lexicalSQL,
		exactTextSQL: exactNumericScalarTextSQL(compiledScalarFromField(field)),
		eligibleSQL:  eligibleSQL,
		invalidSQL: "(" + typeSQL + " != 'None' AND NOT (" +
			supportedSQL + "))",
	}
}

// dynamicExtremaNormalizedTupleSQL preserves the distinction between a
// bytes/v1 envelope's RawStd payload and a Dynamic String that already carries
// Bytes provenance. Both order on raw bytes and publish one canonical bytes/v1
// payload; ordinary lexical and numeric candidates retain their existing text.
//
// The tuple is:
//
//	(publication text, ordering text, exact-numeric text, lexical kind)
func dynamicExtremaNormalizedTupleSQL(
	field fieldState,
	scalar compiledDynamicMeasureScalar,
) string {
	lexical := "__os_stats_extrema_dynamic_lexical"
	exactText := "__os_stats_extrema_dynamic_exact_text"
	encodedBytes := "__os_stats_extrema_dynamic_encoded_bytes"
	rawBytes := "__os_stats_extrema_dynamic_raw_bytes"
	bytesValue := "__os_stats_extrema_dynamic_bytes"
	ordering := "__os_stats_extrema_dynamic_ordering"
	publication := "__os_stats_extrema_dynamic_publication"

	dynamic := compiledScalar{
		valueSQL:       field.valueSQL,
		dynamicTypeSQL: scalar.typeSQL,
		kind:           fieldKindDynamic,
	}
	encodedBytesSQL := dynamicTaggedEnvelopeCondition(dynamic, "bytes/v1")
	rawBytesSQL := "0"
	if field.storedTypeSQL != "" {
		rawBytesSQL = "(" + scalar.typeSQL + " = 'String' AND toUInt8(" +
			field.storedTypeSQL + ") = toUInt8(" +
			strconv.Itoa(int(eventfields.StoredValueTypeBytes)) + "))"
	}
	orderingSQL := "if(" + encodedBytes + ", " +
		rawStdBase64DecodeSQL(lexical) + ", " + lexical + ")"
	publicationSQL := "multiIf(" + encodedBytes + ", " + lexical + ", " +
		rawBytes + ", " + rawStdBase64EncodeSQL(ordering) + ", " + lexical + ")"
	lexicalKind := "if(" + bytesValue + ", toUInt8(" +
		strconv.Itoa(int(statsExtremaPublicationEncodedBytes)) + "), toUInt8(" +
		strconv.Itoa(int(statsExtremaPublicationLexical)) + "))"
	body := "tuple(" + publication + ", " + ordering + ", if(" + bytesValue +
		", CAST('' AS String), " + exactText + "), " + lexicalKind + ")"
	body = bindSQLExpressions(
		[]string{publication},
		[]string{publicationSQL},
		body,
	)
	body = bindSQLExpressions(
		[]string{bytesValue},
		[]string{"(" + encodedBytes + " OR " + rawBytes + ")"},
		body,
	)
	body = bindSQLExpressions(
		[]string{ordering},
		[]string{orderingSQL},
		body,
	)
	body = bindSQLExpressions(
		[]string{encodedBytes, rawBytes},
		[]string{encodedBytesSQL, rawBytesSQL},
		body,
	)
	return bindSQLExpressions(
		[]string{lexical, exactText},
		[]string{scalar.lexicalSQL, scalar.exactTextSQL},
		body,
	)
}

func statsExtremaDynamicCandidatesSQL(field fieldState) (string, []any) {
	empty := "CAST([], 'Array(Tuple(String, String, String, UInt8))')"
	scalar := compileDynamicMeasureScalar(field)
	scalarInput := "if(" + scalar.eligibleSQL + ", [" +
		dynamicExtremaNormalizedTupleSQL(field, scalar) + "], " + empty + ")"

	elementField := fieldState{
		valueSQL:       "element",
		dynamicTypeSQL: "dynamicType(element)",
		kind:           fieldKindDynamic,
	}
	element := compileDynamicMeasureScalar(elementField)
	elementInput := "if(throwIf(toUInt8(" + element.invalidSQL + "), '" +
		UnsupportedStatsMeasureValueMarker + "') = 0, " +
		dynamicExtremaNormalizedTupleSQL(elementField, element) +
		", tuple(CAST('' AS String), CAST('' AS String), CAST('' AS String), " +
		"toUInt8(" + strconv.Itoa(int(statsExtremaPublicationLexical)) + ")))"
	arrayValues := "arrayFilter(element -> " + element.typeSQL +
		" != 'None', dynamicElement(" + field.valueSQL + ", 'Array(Dynamic)'))"
	arrayInput := "arrayMap(element -> " + elementInput + ", " + arrayValues + ")"

	existsSQL, descendantSQL, args := dynamicPresenceOperands(field)
	topLevelUnsupported := "(field_present != 0 AND " + scalar.typeSQL +
		" != 'None' AND " + scalar.typeSQL + " != 'Array(Dynamic)' AND NOT (" +
		scalar.supportedSQL + "))"
	invalid := "(" + topLevelUnsupported + " OR descendant_present != 0)"
	value := "multiIf(" + scalar.typeSQL + " = 'None', " + empty + ", " +
		scalar.typeSQL + " = 'Array(Dynamic)', " + arrayInput + ", " +
		scalarInput + ")"
	body := "if(throwIf(toUInt8(" + invalid + "), '" +
		UnsupportedStatsMeasureValueMarker + "') = 0, if(field_present != 0, " +
		value + ", " + empty + "), " + empty + ")"
	inputs := bindSQLExpressions(
		[]string{"field_present", "descendant_present"},
		[]string{"toUInt8(" + existsSQL + ")", "toUInt8(" + descendantSQL + ")"},
		body,
	)
	input := "__os_stats_extrema_input"
	candidate := statsExtremaCandidateSQL(
		"tupleElement("+input+", 1)",
		"tupleElement("+input+", 2)",
		"tupleElement("+input+", 3)",
		"tupleElement("+input+", 4)",
	)
	return "arrayMap(" + input + " -> " + candidate + ", " + inputs + ")", args
}

func eventStatsExtremaEmptyOrderingKeySQL() string {
	return "tuple(toUInt8(1), toUInt8(1), toInt64(0), " +
		"CAST('' AS String), CAST('' AS String), toUInt8(" +
		strconv.Itoa(int(statsExtremaPublicationLexical)) + "))"
}

func eventStatsExtremaEmptyRowStateSQL(invalidSQL string) string {
	return "tuple(" + eventStatsExtremaEmptyOrderingKeySQL() + ", toUInt8(" +
		strconv.Itoa(int(statsExtremaPublicationLexical)) + "), toFloat64(0), " +
		"CAST('' AS String), toUInt8(0), toUInt8(" + invalidSQL + "))"
}

// extremaFoldWinnerStateSQL merges one normalized five-element candidate into
// the shared six-element row state. Dynamic callers supply the candidate's
// unsupported-value bit; fixed String arrays leave it empty.
func extremaFoldWinnerStateSQL(
	function plan.AggregateFunction,
	stateSQL string,
	candidateSQL string,
	invalidSQL string,
) string {
	// Keep the established private alias stable because compiler-shape tests and
	// query diagnostics use it to identify the normalized extrema candidate.
	candidate := "__os_eventstats_extrema_candidate"
	comparison := "<"
	if function == plan.AggregateFunctionMaximum {
		comparison = ">"
	}
	replace := "(tupleElement(" + candidate + ", 5) != 0 AND (tupleElement(" +
		stateSQL + ", 5) = 0 OR tupleElement(" + candidate + ", 1) " + comparison +
		" tupleElement(" + stateSQL + ", 1)))"
	fields := make([]string, 0, 6)
	for index := 1; index <= 4; index++ {
		position := strconv.Itoa(index)
		fields = append(fields, "if("+replace+", tupleElement("+candidate+", "+
			position+"), tupleElement("+stateSQL+", "+position+"))")
	}
	fields = append(
		fields,
		"toUInt8(tupleElement("+stateSQL+", 5) != 0 OR tupleElement("+
			candidate+", 5) != 0)",
	)
	invalidState := "tupleElement(" + stateSQL + ", 6)"
	if invalidSQL != "" {
		invalidState = "toUInt8(" + invalidState + " != 0 OR (" + invalidSQL + "))"
	}
	fields = append(fields, invalidState)
	return bindSQLExpressions(
		[]string{candidate},
		[]string{candidateSQL},
		"tuple("+strings.Join(fields, ", ")+")",
	)
}

func eventStatsExtremaFoldStepSQL(
	function plan.AggregateFunction,
	stateSQL string,
	value fieldState,
	eligibilityGuardSQL string,
) string {
	typeVariable := "__os_eventstats_extrema_type"
	supportedVariable := "__os_eventstats_extrema_supported"
	supportedSQL, lexicalSQL := statsByScalarExpressionsFor(
		value.valueSQL,
		typeVariable,
	)
	exactTextSQL := exactNumericScalarTextSQL(compiledScalar{
		valueSQL:       value.valueSQL,
		dynamicTypeSQL: typeVariable,
		kind:           fieldKindDynamic,
	})
	eligibleSQL := "(" + typeVariable + " != 'None' AND " +
		supportedVariable + ")"
	if eligibilityGuardSQL != "" {
		eligibleSQL = "(" + eligibleSQL + " AND ifNull(" +
			eligibilityGuardSQL + ", 0))"
	}
	invalidSQL := "(" + typeVariable + " != 'None' AND NOT (" +
		supportedVariable + "))"
	scalar := compiledDynamicMeasureScalar{
		valueSQL:     value.valueSQL,
		typeSQL:      typeVariable,
		supportedSQL: supportedVariable,
		lexicalSQL:   lexicalSQL,
		exactTextSQL: exactTextSQL,
		eligibleSQL:  eligibleSQL,
		invalidSQL:   invalidSQL,
	}
	normalizedVariable := "__os_eventstats_extrema_normalized"
	publicationValue := "tupleElement(" + normalizedVariable + ", 1)"
	orderingValue := "tupleElement(" + normalizedVariable + ", 2)"
	exactText := "tupleElement(" + normalizedVariable + ", 3)"
	lexicalKind := "tupleElement(" + normalizedVariable + ", 4)"
	numberSQL := "if(" + lexicalKind + " = toUInt8(" +
		strconv.Itoa(int(statsExtremaPublicationEncodedBytes)) + "), " +
		"CAST(NULL AS Nullable(Float64)), " +
		statsExtremaNumericOrNullSQL(publicationValue) + ")"
	candidateSQL := statsExtremaPublicationCandidateSQL(
		statsExtremaPublicationCandidateInput{
			publicationValueSQL:       publicationValue,
			orderingValueSQL:          orderingValue,
			numberSQL:                 numberSQL,
			exactTextSQL:              exactText,
			lexicalPublicationKindSQL: lexicalKind,
			eligibleSQL:               "1",
		},
	)
	candidateSQL = bindSQLExpressions(
		[]string{normalizedVariable},
		[]string{dynamicExtremaNormalizedTupleSQL(value, scalar)},
		candidateSQL,
	)
	emptyCandidate := "tuple(" + eventStatsExtremaEmptyOrderingKeySQL() +
		", toUInt8(" + strconv.Itoa(int(statsExtremaPublicationLexical)) +
		"), toFloat64(0), CAST('' AS String), toUInt8(0))"
	result := extremaFoldWinnerStateSQL(
		function,
		stateSQL,
		"if("+eligibleSQL+", "+candidateSQL+", "+emptyCandidate+")",
		invalidSQL,
	)
	result = bindSQLExpressions(
		[]string{supportedVariable},
		[]string{supportedSQL},
		result,
	)
	return bindSQLExpressions(
		[]string{typeVariable},
		[]string{dynamicTypeExpression(value)},
		result,
	)
}

// eventStatsExtremaDynamicMeasureSQL folds one Dynamic row to a constant-size
// winner tuple plus an invalid-container bit. Dynamic multivalue members are
// visited once; no candidate array or second validation walk is retained. A
// grouped query gates the entire fold with its already-bound BY eligibility,
// so an incomplete group row cannot spend work or contribute poison.
func eventStatsExtremaDynamicMeasureSQL(
	function plan.AggregateFunction,
	field fieldState,
	rowEligibleSQL string,
) (string, []any) {
	if rowEligibleSQL == "" {
		rowEligibleSQL = "1"
	}
	topLevelType := "__os_eventstats_extrema_top_level_type"
	elementStoredTypeSQL := ""
	if field.storedTypeSQL != "" {
		elementStoredTypeSQL = "if(" + topLevelType +
			" = 'Array(Dynamic)', toUInt8(0), toUInt8(" +
			field.storedTypeSQL + "))"
	}
	element := fieldState{
		valueSQL:       "element",
		dynamicTypeSQL: "dynamicType(element)",
		storedTypeSQL:  elementStoredTypeSQL,
		kind:           fieldKindDynamic,
	}
	fieldValue := "__os_eventstats_extrema_field_value"
	eligibilityGuardSQL := ""
	if field.textEligibleSQL != "" {
		// A scalar text guard applies to the singleton top-level value. Array
		// members retain their existing independent eligibility contract.
		eligibilityGuardSQL = "(" + topLevelType +
			" = 'Array(Dynamic)' OR ifNull(" + field.textEligibleSQL + ", 0))"
	}

	existsSQL, descendantSQL, args := dynamicPresenceOperands(field)
	empty := eventStatsExtremaEmptyRowStateSQL("0")
	initial := eventStatsExtremaEmptyRowStateSQL("descendant_present != 0")
	memberState := "__os_eventstats_extrema_state"
	values := "multiIf(field_present = 0 OR " + topLevelType +
		" = 'None', arraySlice([" + fieldValue + "], 1, 0), " + topLevelType +
		" = 'Array(Dynamic)', dynamicElement(" + fieldValue +
		", 'Array(Dynamic)'), [" + fieldValue + "])"
	rowState := "arrayFold((" + memberState + ", element) -> " +
		eventStatsExtremaFoldStepSQL(
			function,
			memberState,
			element,
			eligibilityGuardSQL,
		) + ", " + values +
		", " + initial + ")"
	gated := "if(" + rowEligibleSQL + ", " + rowState + ", " + empty + ")"
	return bindSQLExpressions(
		[]string{
			"field_present",
			"descendant_present",
			topLevelType,
			fieldValue,
		},
		[]string{
			"toUInt8(" + existsSQL + ")",
			"toUInt8(" + descendantSQL + ")",
			dynamicTypeExpression(field),
			field.valueSQL,
		},
		gated,
	), args
}

func statsExtremaNumericOrNullSQL(valueSQL string) string {
	limit := strconv.Itoa(MaximumExactNumericBinTextBytes)
	bounded := "if(length(" + valueSQL + ") <= " + limit + ", " + valueSQL +
		", CAST('' AS String))"
	numeric := "isValidUTF8(" + valueSQL + ") AND length(" + valueSQL + ") <= " +
		limit + " AND " + valueSQL + " = trimBoth(" + valueSQL + ") AND match(" +
		bounded + ", " + decimalNumericStringPattern + ")"
	converted := finiteFloatOrNullSQL(canonicalNumericTextSQL(bounded))
	return "if(" + numeric + ", " + converted + ", CAST(NULL AS Nullable(Float64)))"
}

func statsExtremaAggregateSQL(function plan.AggregateFunction, candidatesSQL string) string {
	name := "argMinArray"
	if function == plan.AggregateFunctionMaximum {
		name = "argMaxArray"
	}
	return name + "(arrayMap(candidate -> tupleElement(candidate, 1), " + candidatesSQL +
		"), arrayMap(candidate -> tupleElement(candidate, 2), " + candidatesSQL + "))"
}

func statsExtremaStoredTypeSQL(valueSQL string) string {
	valueVariable := "__os_stats_extrema_stored_value"
	typeSQL := "dynamicType(" + valueVariable + ")"
	stringSQL := "dynamicElement(" + valueVariable + ", 'String')"
	value := compiledScalar{
		valueSQL:       valueVariable,
		dynamicTypeSQL: typeSQL,
		kind:           fieldKindDynamic,
	}
	decimal := dynamicTaggedEnvelopeCondition(value, "decimal/v1")
	bytesValue := dynamicTaggedEnvelopeCondition(value, "bytes/v1")
	body := "multiIf(" +
		typeSQL + " = 'None', toUInt8(" +
		strconv.Itoa(int(eventfields.StoredValueTypeNull)) + "), " +
		typeSQL + " = 'Float64', toUInt8(" +
		strconv.Itoa(int(eventfields.StoredValueTypeDouble)) + "), " +
		decimal + ", toUInt8(" +
		strconv.Itoa(int(eventfields.StoredValueTypeDecimal)) + "), " +
		bytesValue + ", toUInt8(" +
		strconv.Itoa(int(eventfields.StoredValueTypeBytes)) + "), " +
		typeSQL + " = 'String' AND isValidUTF8(" + stringSQL + "), toUInt8(" +
		strconv.Itoa(int(eventfields.StoredValueTypeString)) + "), " +
		typeSQL + " = 'String', toUInt8(" +
		strconv.Itoa(int(eventfields.StoredValueTypeBytes)) + "), toUInt8(0))"
	return bindSQLExpressions(
		[]string{valueVariable},
		[]string{valueSQL},
		body,
	)
}

func statsExtremaStoredTypeFromConditionsSQL(
	nullConditionSQL string,
	numberConditionSQL string,
	stringConditionSQL string,
	stringSQL string,
) string {
	return statsExtremaStoredTypeWithDecimalSQL(
		nullConditionSQL,
		numberConditionSQL,
		"0",
		stringConditionSQL,
		stringSQL,
	)
}

func statsExtremaStoredTypeWithDecimalSQL(
	nullConditionSQL string,
	numberConditionSQL string,
	decimalConditionSQL string,
	stringConditionSQL string,
	stringSQL string,
) string {
	return "multiIf(" +
		nullConditionSQL + ", toUInt8(" + strconv.Itoa(int(eventfields.StoredValueTypeNull)) + "), " +
		numberConditionSQL + ", toUInt8(" + strconv.Itoa(int(eventfields.StoredValueTypeDouble)) + "), " +
		decimalConditionSQL + ", toUInt8(" + strconv.Itoa(int(eventfields.StoredValueTypeDecimal)) + "), " +
		stringConditionSQL + " AND isValidUTF8(" + stringSQL + "), toUInt8(" +
		strconv.Itoa(int(eventfields.StoredValueTypeString)) + "), " +
		stringConditionSQL + ", toUInt8(" + strconv.Itoa(int(eventfields.StoredValueTypeBytes)) + "), " +
		"toUInt8(0))"
}

func compileStatsSparklineResults(
	relation compiledRelation,
	measures []compiledStatsSparklineMeasure,
	ownerRange spl.Range,
	stage int,
) (compiledRelation, int, error) {
	if len(measures) == 0 {
		return relation, 0, nil
	}

	excluded := make([]string, 0, len(measures))
	projection := make([]string, 1, 1+len(measures))
	for _, measure := range measures {
		if measure.recordsColumn == "" || measure.outputColumn == "" {
			return compiledRelation{}, 0, errors.New(
				"compile ClickHouse stats sparkline: publication metadata is invalid",
			)
		}
		published, ok := statsSparklinePublishSQL(
			measure.recordsColumn,
			measure.spec,
			measure.missing,
		)
		if !ok {
			return compiledRelation{}, 0, errors.New(
				"compile ClickHouse stats sparkline: publication lowering is invalid",
			)
		}
		excluded = append(excluded, measure.recordsColumn)
		projection = append(projection, published+" AS "+measure.outputColumn)
	}
	projection[0] = "* EXCEPT (" + strings.Join(excluded, ", ") + ")"
	alias := quoteIdentifier(fmt.Sprintf("_stage_%d", stage+1))
	sql := "SELECT " + strings.Join(projection, ", ") + " FROM (" +
		relation.sql + ") AS " + alias
	return relation.selectFrom(sql, ownerRange), 1, nil
}

func compileChronologicalResults(
	relation compiledRelation,
	measures []compiledChronologicalMeasure,
	ownerRange spl.Range,
	stage int,
) (compiledRelation, int, *pendingChronologicalBarrier) {
	if len(measures) == 0 {
		return relation, 0, nil
	}

	excluded := make([]string, 0, len(measures)*2)
	seenExcluded := make(map[string]struct{}, len(measures)*2)
	validations := make([]string, 0, len(measures))
	seenValidations := make(map[string]struct{}, len(measures))
	for _, measure := range measures {
		if _, seen := seenExcluded[measure.winnerColumn]; !seen {
			seenExcluded[measure.winnerColumn] = struct{}{}
			excluded = append(excluded, measure.winnerColumn)
		}
		if measure.validationColumn == "" {
			continue
		}
		if _, seen := seenExcluded[measure.validationColumn]; !seen {
			seenExcluded[measure.validationColumn] = struct{}{}
			excluded = append(excluded, measure.validationColumn)
		}
		if _, seen := seenValidations[measure.validationColumn]; !seen {
			seenValidations[measure.validationColumn] = struct{}{}
			validations = append(validations, measure.validationColumn)
		}
	}

	projection := []string{"* EXCEPT (" + strings.Join(excluded, ", ") + ")"}
	publishedTypes := make(map[string]struct{}, len(measures))
	for _, measure := range measures {
		projection = append(
			projection,
			chronologicalPublishedValueSQL(measure.winnerColumn)+
				" AS "+measure.outputColumn,
		)
		if _, published := publishedTypes[measure.winnerColumn]; published {
			continue
		}
		publishedTypes[measure.winnerColumn] = struct{}{}
		projection = append(
			projection,
			chronologicalPublishedTypeSQL(measure.winnerColumn)+
				" AS "+measure.typeColumn,
		)
	}

	alias := quoteIdentifier(fmt.Sprintf("_stage_%d", stage+1))
	if len(validations) == 0 {
		sql := "SELECT " + strings.Join(projection, ", ") +
			" FROM (" + relation.sql + ") AS " + alias
		return relation.selectFrom(sql, ownerRange), 1, nil
	}

	materialized := quoteIdentifier(fmt.Sprintf("__os_chronological_input_%d", stage+1))
	sql := "SELECT " + strings.Join(projection, ", ") +
		" FROM " + materialized + " AS " + alias
	published := relation.selectFrom(sql, ownerRange)
	return published, 1, &pendingChronologicalBarrier{
		name:              materialized,
		sql:               relation.sql,
		validationColumns: validations,
		fanout:            1,
		depth:             relation.depth,
		ownerRange:        ownerRange,
	}
}

func compileScalarExtremaResults(
	relation compiledRelation,
	measures []compiledScalarExtremaMeasure,
	ownerRange spl.Range,
	stage int,
) (compiledRelation, int) {
	if len(measures) == 0 {
		return relation, 0
	}

	winners := make([]string, 0, len(measures))
	seenWinners := make(map[string]struct{}, len(measures))
	projection := make([]string, 1, 1+len(measures)*2)
	for _, measure := range measures {
		if _, seen := seenWinners[measure.winnerColumn]; !seen {
			seenWinners[measure.winnerColumn] = struct{}{}
			winners = append(winners, measure.winnerColumn)
		}
	}
	projection[0] = "* EXCEPT (" + strings.Join(winners, ", ") + ")"

	publishedTypes := make(map[string]struct{}, len(winners))
	for _, measure := range measures {
		projection = append(
			projection,
			statsExtremaScalarValueSQL(measure.winnerColumn)+" AS "+measure.outputColumn,
		)
		if _, published := publishedTypes[measure.winnerColumn]; !published {
			publishedTypes[measure.winnerColumn] = struct{}{}
			projection = append(
				projection,
				statsExtremaScalarStoredTypeSQL(measure.winnerColumn)+" AS "+measure.typeColumn,
			)
		}
	}

	alias := quoteIdentifier(fmt.Sprintf("_stage_%d", stage+1))
	sql := "SELECT " + strings.Join(projection, ", ") +
		" FROM (" + relation.sql + ") AS " + alias
	return relation.selectFrom(sql, ownerRange), 1
}

func compactNullableArraySQL(valuesSQL string) string {
	return "arrayMap(value -> assumeNotNull(value), arrayFilter(value -> isNotNull(value), " + valuesSQL + "))"
}

func statsScalarStringOrNullSQL(field fieldState) string {
	nullString := "CAST(NULL AS Nullable(String))"
	switch field.kind {
	case fieldKindDynamic:
		supported, lexical := statsByScalarExpressions(field)
		return "if(" + supported + ", " + lexical + ", " + nullString + ")"
	case fieldKindString, fieldKindNumber, fieldKindBool, fieldKindTime:
		return "CAST(toString(" + field.valueSQL + ") AS Nullable(String))"
	default:
		return nullString
	}
}

func statsTextEligibleScalarStringOrNullSQL(field fieldState) string {
	value := statsScalarStringOrNullSQL(field)
	if field.textEligibleSQL == "" {
		return value
	}
	nullString := "CAST(NULL AS Nullable(String))"
	value = "if(ifNull(" + field.textEligibleSQL + ", 0), " +
		value + ", " + nullString + ")"
	return value
}

func boundedDistinctCountSQL(inputSQL string) string {
	maximum := strconv.FormatUint(MaximumStatsDistinctValuesPerGroup, 10)
	cardinality := distinctCountCardinalitySQL(inputSQL)
	return "arrayElement(arrayMap(cardinality -> cardinality + toUInt64(throwIf(toUInt8(cardinality > " +
		maximum + "), '" + ExactDistinctLimitMarker + "')), [" + cardinality + "]), 1)"
}

func distinctCountCardinalitySQL(inputSQL string) string {
	return "toUInt64(length(" + exactDistinctStringSetSQL(
		inputSQL,
		uint64(MaximumStatsDistinctValuesPerGroup),
	) + "))"
}

func exactDistinctStringSetSQL(inputSQL string, maximum uint64) string {
	sentinel := strconv.FormatUint(maximum+1, 10)
	return "groupUniqArrayArray(" + sentinel + ")(" + inputSQL + ")"
}

func stringArrayPayloadBytesSQL(valuesSQL string) string {
	return "arrayFold((bytes, value) -> bytes + toUInt128(length(value)), " +
		valuesSQL + ", toUInt128(0))"
}

func orderedStringMembersSQL(valuesSQL string) string {
	return "arrayMap((value, element_index, cumulative_bytes) -> " +
		"tuple(value, toUInt128(element_index), cumulative_bytes), " +
		valuesSQL + ", arrayEnumerate(" + valuesSQL + "), " +
		"arrayCumSum(arrayMap(value -> toUInt128(length(value)), " + valuesSQL + ")))"
}

func boundedOrderedStringRowStateSQL(
	rowOrdinalSQL string,
	valuesSQL string,
	priorElementsSQL string,
	priorBytesSQL string,
) string {
	maximumValues := strconv.FormatUint(MaximumStatsListValuesPerGroup, 10)
	maximumBytes := strconv.FormatUint(MaximumStatsListBytesPerGroup, 10)
	remainingValues := "if(" + priorElementsSQL + " < toUInt128(" + maximumValues +
		"), arraySlice(" + valuesSQL + ", 1, toUInt64(toUInt128(" + maximumValues +
		") - " + priorElementsSQL + ")), CAST([], 'Array(String)'))"
	remaining := "__os_list_remaining_values"
	members := orderedStringMembersSQL(remaining)
	member := "member"
	bytes := priorBytesSQL + " + tupleElement(" + member + ", 3)"
	candidates := "arrayMap(" + member + " -> tuple(" + rowOrdinalSQL +
		", toUInt64(tupleElement(" + member + ", 2)), tupleElement(" + member +
		", 1)), arrayFilter(" + member + " -> " + bytes + " <= toUInt128(" +
		maximumBytes + "), " + members + "))"
	overflow := "toUInt8(" + priorElementsSQL + " < toUInt128(" + maximumValues +
		") AND " + priorBytesSQL + " + " + stringArrayPayloadBytesSQL(remaining) +
		" > toUInt128(" + maximumBytes + "))"
	return "arrayElement(arrayMap(" + remaining + " -> tuple(" + candidates +
		", " + overflow + "), [" + remainingValues + "]), 1)"
}

func boundedOrderedStringListSQL(candidatesSQL string) string {
	return "groupArraySortedArray(" +
		strconv.FormatUint(MaximumStatsListValuesPerGroup, 10) +
		")(" + candidatesSQL + ")"
}

func emptyOrderedStringListSQL() string {
	return "CAST([], 'Array(Tuple(UInt64, UInt64, String))')"
}

func orderedStringListValuesSQL(listSQL string) string {
	return "arrayMap(item -> tupleElement(item, 3), " + listSQL + ")"
}

func orderedStringListPayloadBytesSQL(listSQL string) string {
	return "arrayFold((bytes, item) -> bytes + toUInt128(length(tupleElement(item, 3))), " +
		listSQL + ", toUInt128(0))"
}

func compileBoundedOrderedStringResults(
	relation compiledRelation,
	measures []compiledOrderedStringMeasure,
	existingValues []string,
	ownerRange spl.Range,
	stage int,
) (compiledRelation, int) {
	if len(measures) == 0 {
		return relation, 0
	}

	listColumns := make([]string, 0, len(measures))
	overflowColumns := make([]string, 0, len(measures))
	materialized := make(map[string]string, len(measures))
	seenLists := make(map[string]struct{}, len(measures))
	for _, measure := range measures {
		if _, seen := seenLists[measure.listColumn]; seen {
			continue
		}
		seenLists[measure.listColumn] = struct{}{}
		listColumns = append(listColumns, measure.listColumn)
		overflowColumns = append(overflowColumns, measure.overflowColumn)
		materialized[measure.listColumn] = quoteIdentifier(fmt.Sprintf(
			"__os_list_strings_%d",
			len(materialized),
		))
	}

	windowColumns := make([]string, 0, len(listColumns)+4)
	byteConditions := make([]string, 0, len(listColumns))
	for index, listColumn := range listColumns {
		windowColumns = append(
			windowColumns,
			orderedStringListValuesSQL(listColumn)+" AS "+
				materialized[listColumn],
		)
		byteConditions = append(
			byteConditions,
			overflowColumns[index]+" != 0",
			orderedStringListPayloadBytesSQL(listColumn)+" > toUInt128("+
				strconv.FormatUint(MaximumStatsListBytesPerGroup, 10)+")",
		)
	}

	var rowElementTotal strings.Builder
	rowElementTotal.WriteString("toUInt128(0)")
	var rowByteTotal strings.Builder
	rowByteTotal.WriteString("toUInt128(0)")
	for _, measure := range measures {
		// Public aliases count independently even when their physical ordered
		// aggregate state is shared.
		rowElementTotal.WriteString(" + toUInt128(length(")
		rowElementTotal.WriteString(measure.listColumn)
		rowElementTotal.WriteString("))")
		rowByteTotal.WriteString(" + ")
		rowByteTotal.WriteString(orderedStringListPayloadBytesSQL(measure.listColumn))
	}
	for _, valuesColumn := range existingValues {
		// values() has already passed its own exact-state barrier. Include each
		// public values alias again so list() cannot bypass the combined
		// transforming-row and transport budgets.
		rowElementTotal.WriteString(" + toUInt128(length(")
		rowElementTotal.WriteString(valuesColumn)
		rowElementTotal.WriteString("))")
		rowByteTotal.WriteString(" + ")
		rowByteTotal.WriteString(stringArrayPayloadBytesSQL(valuesColumn))
	}

	elementOverflow := quoteIdentifier("__os_stats_list_any_overflow")
	totalElements := quoteIdentifier("__os_stats_list_total_elements")
	bytesOverflow := quoteIdentifier("__os_stats_list_bytes_any_overflow")
	totalBytes := quoteIdentifier("__os_stats_list_total_bytes")
	windowColumns = append(
		windowColumns,
		"max(toUInt8("+rowElementTotal.String()+" > toUInt128("+
			strconv.FormatUint(MaximumStatsValuesPerGroup, 10)+
			"))) OVER () AS "+elementOverflow,
		"sum("+rowElementTotal.String()+") OVER () AS "+totalElements,
		"max(toUInt8(("+strings.Join(byteConditions, " OR ")+") OR "+
			rowByteTotal.String()+" > toUInt128("+
			strconv.FormatUint(MaximumStatsValuesBytesPerGroup, 10)+
			"))) OVER () AS "+bytesOverflow,
		"sum("+rowByteTotal.String()+") OVER () AS "+totalBytes,
	)

	windowAlias := quoteIdentifier(fmt.Sprintf("_stage_%d", stage+1))
	windowSQL := "SELECT *, " + strings.Join(windowColumns, ", ") +
		" FROM (" + relation.sql + ") AS " + windowAlias
	relation = relation.selectFrom(windowSQL, ownerRange)

	excluded := append([]string(nil), listColumns...)
	excluded = append(excluded, overflowColumns...)
	for _, listColumn := range listColumns {
		excluded = append(excluded, materialized[listColumn])
	}
	excluded = append(
		excluded,
		elementOverflow,
		totalElements,
		bytesOverflow,
		totalBytes,
	)
	projection := []string{"* EXCEPT (" + strings.Join(excluded, ", ") + ")"}
	for _, measure := range measures {
		projection = append(
			projection,
			materialized[measure.listColumn]+" AS "+measure.outputColumn,
		)
	}

	publishAlias := quoteIdentifier(fmt.Sprintf("_stage_%d", stage+2))
	publishSQL := "SELECT " + strings.Join(projection, ", ") + " FROM (" + relation.sql +
		") AS " + publishAlias +
		" WHERE throwIf(toUInt8(" + elementOverflow +
		" != 0 OR " + totalElements + " > toUInt128(" +
		strconv.FormatUint(MaximumStatsListValuesPerResult, 10) +
		")), '" + StatsListLimitMarker + "') = 0" +
		" AND throwIf(toUInt8(" + bytesOverflow +
		" != 0 OR " + totalBytes + " > toUInt128(" +
		strconv.FormatUint(MaximumStatsListBytesPerResult, 10) +
		")), '" + StatsListBytesLimitMarker + "') = 0"
	return relation.selectFrom(publishSQL, ownerRange), 2
}

func compileBoundedExactStringResults(
	relation compiledRelation,
	measures []compiledExactStringMeasure,
	counts []compiledDistinctCount,
	ownerRange spl.Range,
	stage int,
) (compiledRelation, int) {
	if len(measures) == 0 {
		return relation, 0
	}

	setColumns := make([]string, 0, len(measures))
	valuesMeasures := make([]compiledExactStringMeasure, 0, len(measures))
	valuesSetColumns := make([]string, 0, len(measures))
	seenSets := make(map[string]struct{}, len(measures))
	seenValuesSets := make(map[string]struct{}, len(measures))
	for _, measure := range measures {
		if _, seen := seenSets[measure.setColumn]; !seen {
			seenSets[measure.setColumn] = struct{}{}
			setColumns = append(setColumns, measure.setColumn)
		}
		if measure.function == plan.AggregateFunctionValues {
			valuesMeasures = append(valuesMeasures, measure)
			if _, seen := seenValuesSets[measure.setColumn]; !seen {
				seenValuesSets[measure.setColumn] = struct{}{}
				valuesSetColumns = append(valuesSetColumns, measure.setColumn)
			}
		}
	}

	windowColumns := make([]string, 0, 7)
	sortedValuesSets := make(map[string]string, len(valuesSetColumns))
	sortedSetColumns := make([]string, 0, len(valuesSetColumns))
	for index, setColumn := range valuesSetColumns {
		sorted := quoteIdentifier(fmt.Sprintf("__os_sorted_exact_strings_%d", index))
		sortedValuesSets[setColumn] = sorted
		sortedSetColumns = append(sortedSetColumns, sorted)
		windowColumns = append(windowColumns, "arraySort("+setColumn+") AS "+sorted)
	}

	cardinalityOverflow := ""
	cardinalityColumns := make([]string, 0, len(counts))
	seenCardinalities := make(map[string]struct{}, len(counts))
	for _, count := range counts {
		if _, seen := seenCardinalities[count.cardinalityColumn]; seen {
			continue
		}
		seenCardinalities[count.cardinalityColumn] = struct{}{}
		cardinalityColumns = append(cardinalityColumns, count.cardinalityColumn)
	}
	if len(cardinalityColumns) > 0 {
		maximumDistinctValues := strconv.FormatUint(MaximumStatsDistinctValuesPerGroup, 10)
		cardinalityConditions := make([]string, 0, len(cardinalityColumns))
		for _, cardinalityColumn := range cardinalityColumns {
			cardinalityConditions = append(
				cardinalityConditions,
				cardinalityColumn+" > toUInt64("+maximumDistinctValues+")",
			)
		}
		cardinalityOverflow = quoteIdentifier("__os_stats_distinct_any_overflow")
		windowColumns = append(
			windowColumns,
			"max(toUInt8("+strings.Join(cardinalityConditions, " OR ")+
				")) OVER () AS "+cardinalityOverflow,
		)
	}

	valuesOverflow := ""
	valuesTotalElements := ""
	valuesBytesOverflow := ""
	valuesTotalBytes := ""
	if len(valuesSetColumns) > 0 {
		maximumValues := strconv.FormatUint(MaximumStatsValuesPerGroup, 10)
		valueConditions := make([]string, 0, len(valuesSetColumns))
		byteConditions := make([]string, 0, len(valuesSetColumns))
		for _, setColumn := range valuesSetColumns {
			valueConditions = append(
				valueConditions,
				"length("+setColumn+") > toUInt64("+maximumValues+")",
			)
			byteConditions = append(
				byteConditions,
				stringArrayPayloadBytesSQL(setColumn)+" > toUInt128("+
					strconv.FormatUint(MaximumStatsValuesBytesPerGroup, 10)+")",
			)
		}
		valuesOverflow = quoteIdentifier("__os_stats_values_any_overflow")
		valuesTotalElements = quoteIdentifier("__os_stats_values_total_elements")
		valuesBytesOverflow = quoteIdentifier("__os_stats_values_bytes_any_overflow")
		valuesTotalBytes = quoteIdentifier("__os_stats_values_total_bytes")

		var rowElementTotal strings.Builder
		rowElementTotal.WriteString("toUInt128(0)")
		var rowByteTotal strings.Builder
		rowByteTotal.WriteString("toUInt128(0)")
		for _, measure := range valuesMeasures {
			// Deliberately retain duplicates: two public aliases create two
			// recursive list cells even when their aggregate state is shared.
			rowElementTotal.WriteString(" + toUInt128(length(")
			rowElementTotal.WriteString(measure.setColumn)
			rowElementTotal.WriteString("))")
			rowByteTotal.WriteString(" + ")
			rowByteTotal.WriteString(stringArrayPayloadBytesSQL(measure.setColumn))
		}
		windowColumns = append(
			windowColumns,
			"max(toUInt8(("+strings.Join(valueConditions, " OR ")+") OR "+rowElementTotal.String()+
				" > toUInt128("+strconv.FormatUint(MaximumStatsValuesPerGroup, 10)+
				"))) OVER () AS "+valuesOverflow,
			"sum("+rowElementTotal.String()+") OVER () AS "+valuesTotalElements,
			"max(toUInt8(("+strings.Join(byteConditions, " OR ")+") OR "+rowByteTotal.String()+
				" > toUInt128("+strconv.FormatUint(MaximumStatsValuesBytesPerGroup, 10)+
				"))) OVER () AS "+valuesBytesOverflow,
			"sum("+rowByteTotal.String()+") OVER () AS "+valuesTotalBytes,
		)
	}

	windowAlias := quoteIdentifier(fmt.Sprintf("_stage_%d", stage+1))
	windowSQL := "SELECT *, " + strings.Join(windowColumns, ", ") +
		" FROM (" + relation.sql + ") AS " + windowAlias
	relation = relation.selectFrom(windowSQL, ownerRange)

	excluded := append([]string(nil), setColumns...)
	excluded = append(excluded, sortedSetColumns...)
	excluded = append(excluded, cardinalityColumns...)
	if cardinalityOverflow != "" {
		excluded = append(excluded, cardinalityOverflow)
	}
	if valuesOverflow != "" {
		excluded = append(excluded, valuesOverflow, valuesTotalElements)
	}
	if valuesBytesOverflow != "" {
		excluded = append(excluded, valuesBytesOverflow, valuesTotalBytes)
	}
	projection := []string{"* EXCEPT (" + strings.Join(excluded, ", ") + ")"}
	for _, measure := range measures {
		switch measure.function {
		case plan.AggregateFunctionDistinctCount:
			projection = append(
				projection,
				"toUInt64(length("+measure.setColumn+")) AS "+measure.outputColumn,
			)
		case plan.AggregateFunctionValues:
			projection = append(
				projection,
				sortedValuesSets[measure.setColumn]+" AS "+measure.outputColumn,
			)
		}
	}
	for _, count := range counts {
		projection = append(
			projection,
			count.cardinalityColumn+" AS "+count.outputColumn,
		)
	}

	validations := make([]string, 0, 3)
	if cardinalityOverflow != "" {
		validations = append(
			validations,
			"throwIf(toUInt8("+cardinalityOverflow+" != 0), '"+
				ExactDistinctLimitMarker+"') = 0",
		)
	}
	if valuesBytesOverflow != "" {
		validations = append(
			validations,
			"throwIf(toUInt8("+valuesOverflow+" != 0 OR "+valuesTotalElements+
				" > toUInt128("+strconv.FormatUint(MaximumStatsValuesPerResult, 10)+")), '"+
				StatsValuesLimitMarker+"') = 0",
		)
		validations = append(
			validations,
			"throwIf(toUInt8("+valuesBytesOverflow+" != 0 OR "+valuesTotalBytes+
				" > toUInt128("+strconv.FormatUint(MaximumStatsValuesBytesPerResult, 10)+")), '"+
				StatsValuesBytesLimitMarker+"') = 0",
		)
	}
	publishAlias := quoteIdentifier(fmt.Sprintf("_stage_%d", stage+2))
	publishSQL := "SELECT " + strings.Join(projection, ", ") + " FROM (" + relation.sql +
		") AS " + publishAlias + " WHERE " + strings.Join(validations, " AND ")
	return relation.selectFrom(publishSQL, ownerRange), 2
}

func compileBoundedDistinctCountResults(
	relation compiledRelation,
	counts []compiledDistinctCount,
	ownerRange spl.Range,
	stage int,
) (compiledRelation, int) {
	if len(counts) == 0 {
		return relation, 0
	}
	maximum := strconv.FormatUint(MaximumStatsDistinctValuesPerGroup, 10)
	cardinalityColumns := make([]string, 0, len(counts))
	overflowConditions := make([]string, 0, len(counts))
	seenCardinalities := make(map[string]struct{}, len(counts))
	for _, count := range counts {
		if _, seen := seenCardinalities[count.cardinalityColumn]; seen {
			continue
		}
		seenCardinalities[count.cardinalityColumn] = struct{}{}
		cardinalityColumns = append(cardinalityColumns, count.cardinalityColumn)
		overflowConditions = append(
			overflowConditions,
			count.cardinalityColumn+" > toUInt64("+maximum+")",
		)
	}
	overflowColumn := quoteIdentifier("__os_stats_dc_any_overflow")
	windowAlias := quoteIdentifier(fmt.Sprintf("_stage_%d", stage+1))
	windowSQL := "SELECT *, max(toUInt8(" + strings.Join(overflowConditions, " OR ") +
		")) OVER () AS " + overflowColumn + " FROM (" + relation.sql + ") AS " + windowAlias
	relation = relation.selectFrom(windowSQL, ownerRange)

	excluded := append(cardinalityColumns, overflowColumn)
	projection := []string{"* EXCEPT (" + strings.Join(excluded, ", ") + ")"}
	for _, count := range counts {
		projection = append(projection, count.cardinalityColumn+" AS "+count.outputColumn)
	}
	publishAlias := quoteIdentifier(fmt.Sprintf("_stage_%d", stage+2))
	publishSQL := "SELECT " + strings.Join(projection, ", ") + " FROM (" + relation.sql +
		") AS " + publishAlias + " WHERE throwIf(toUInt8(" + overflowColumn +
		" != 0), '" + ExactDistinctLimitMarker + "') = 0"
	return relation.selectFrom(publishSQL, ownerRange), 2
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

func finiteDynamicFloatOrNullSQL(valueSQL string) string {
	return "ifNotFinite(accurateCastOrNull(" + valueSQL +
		", 'Float64'), CAST(NULL AS Nullable(Float64)))"
}

func statsByScalarExpressions(field fieldState) (supported, lexical string) {
	return statsByScalarExpressionsFor(field.valueSQL, dynamicTypeExpression(field))
}

func statsByScalarExpressionsFor(
	valueSQL, typeSQL string,
) (supported, lexical string) {
	mapSQL := "dynamicElement(" + valueSQL + ", 'Map(String, String)')"
	valueKey := "concat(char(0), 'open_splunk_value')"
	value := compiledScalar{
		valueSQL:       valueSQL,
		dynamicTypeSQL: typeSQL,
		kind:           fieldKindDynamic,
	}
	extended := dynamicTaggedScalarEnvelopeCondition(value)
	// None is excluded deliberately. Missing and explicit-null leaves are
	// removed before aggregation, while a flattened object parent reads as None
	// at its literal path and must set the unsupported-container flag.
	supported = "(" + typeSQL + " IN ('String', 'Float64', 'Bool') OR " +
		dynamicIntegerTypePredicate(typeSQL) + " OR " + extended + ")"
	lexical = "if(" + typeSQL + " = 'Map(String, String)', " + mapSQL + "[" +
		valueKey + "], toString(" + valueSQL + "))"
	return supported, lexical
}
