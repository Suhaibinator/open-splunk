package clickhouse

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/Suhaibinator/open-splunk/internal/plan"
	"github.com/Suhaibinator/open-splunk/internal/spl"
	"github.com/Suhaibinator/open-splunk/internal/splpath"
)

func spathWildcardStepStateSQL(
	currentNodesSQL string,
	step splpath.Step,
	terminal bool,
) string {
	key := "__os_spath_wildcard_key"
	candidate := "__os_spath_wildcard_candidate"
	raw := ""
	matched := "notEmpty(" + candidate + ")"
	switch step.Selector {
	case splpath.ArraySelectorNone:
		raw = "arrayFilter(raw -> notEmpty(raw), arrayMap(node -> " +
			"JSONExtractRaw(node, " + key + "), " + currentNodesSQL + "))"
	case splpath.ArraySelectorFixed:
		// JSON path array indexes are one-based in ClickHouse.
		index := strconv.FormatUint(step.Index+1, 10)
		raw = "arrayFilter(raw -> notEmpty(raw), arrayMap(node -> if(" +
			"toString(JSONType(node, " + key + ")) = 'Array', JSONExtractRaw(node, " +
			key + ", toInt64(" + index + ")), CAST('' AS String)), " +
			currentNodesSQL + "))"
	case splpath.ArraySelectorWildcard:
		raw = "arrayFlatten(arrayMap(node -> if(toString(JSONType(node, " + key +
			")) = 'Array', JSONExtractArrayRaw(node, " + key +
			"), CAST([], 'Array(String)')), " + currentNodesSQL + "))"
		if terminal {
			// A terminal wildcard that reaches an empty JSON array is a real,
			// present-empty match even though it contributes no leaf node.
			matched = "arrayExists(node -> toString(JSONType(node, " + key +
				")) = 'Array', " + currentNodesSQL + ")"
		}
	default:
		return "tuple(CAST([], 'Array(String)'), toUInt8(0))"
	}
	if !terminal {
		matched = "0"
	}
	body := bindSQLExpressions(
		[]string{candidate},
		[]string{raw},
		"tuple("+candidate+", toUInt8("+matched+"))",
	)
	return bindSQLExpressions(
		[]string{key},
		[]string{"CAST(? AS String)"},
		body,
	)
}

func compileSpathPriorMVState(field fieldState, known bool) (nativeMVState, error) {
	if !known {
		return nativeMVState{
			sql: "tuple(" + emptyNativeMVSQL() + ", toUInt8(0), toUInt8(0), toUInt8(0))",
		}, nil
	}
	input := compiledScalarFromField(field)
	if isNativeMultivalueKind(field.kind) || field.kind == fieldKindDynamic ||
		field.kind == fieldKindInvalid {
		return compileNativeMVState(input, false)
	}
	existsSQL := field.existsSQL
	if existsSQL == "" {
		existsSQL = "1"
	}
	descendantSQL := field.descendantSQL
	if descendantSQL == "" {
		descendantSQL = "0"
	}
	stateSQL := bindSQLExpressions(
		[]string{"value", "field_present", "descendant_present"},
		[]string{field.valueSQL, "toUInt8(" + existsSQL + ")", "toUInt8(" + descendantSQL + ")"},
		"tuple("+emptyNativeMVSQL()+", toUInt8(field_present != 0), toUInt8(0), toUInt8("+
			"descendant_present != 0 OR (field_present != 0 AND isNotNull(value))))",
	)
	args := append([]any(nil), input.valueArgs...)
	args = append(args, field.existsArgs...)
	args = append(args, field.descendantArgs...)
	return nativeMVState{sql: stateSQL, args: args}, nil
}

func compileExtractJSONWildcard(
	relation compiledRelation,
	operator *plan.ExtractJSON,
	steps []splpath.Step,
	state compileState,
	stage int,
) (compiledRelation, compileState, []any, int, error) {
	if operator == nil || len(steps) == 0 || !splpath.HasWildcard(steps) {
		return compiledRelation{}, compileState{}, nil, 0, errors.New(
			"compile ClickHouse wildcard spath: operator is invalid",
		)
	}
	if state.context == nil {
		return compiledRelation{}, compileState{}, nil, 0, errors.New(
			"compile ClickHouse wildcard spath: query context is required",
		)
	}
	openEventSchema := state.eventRows && state.allowDynamic
	if openEventSchema && (operator.Input.Name == "fields" || operator.Output.Name == "fields") {
		return compiledRelation{}, compileState{}, nil, 0, &plan.Diagnostic{
			Code:    "SPL_AMBIGUOUS_SPATH_FIELD",
			Message: "spath cannot use the event result's reserved fields payload without an exact upstream schema",
			Range:   operator.Range,
		}
	}

	inputSQL, sourceEligibleSQL, inputArgs, err := compileExtractInput(operator.Input, state)
	if err != nil {
		return compiledRelation{}, compileState{}, nil, 0, err
	}
	inputField, inputKnown, err := resolveCompiledField(operator.Input, state)
	if err != nil {
		return compiledRelation{}, compileState{}, nil, 0, err
	}
	sourceMayExtract := inputKnown &&
		(inputField.kind == fieldKindString || inputField.kind == fieldKindDynamic)

	sourceEligibleAlias := quoteIdentifier(fmt.Sprintf("__os_spath_mv_source_eligible_%d", stage))
	inputAlias := quoteIdentifier(fmt.Sprintf("__os_spath_mv_input_%d", stage))
	eligibleAlias := quoteIdentifier(fmt.Sprintf("__os_spath_mv_eligible_%d", stage))
	tokenCountAlias := quoteIdentifier(fmt.Sprintf("__os_spath_mv_token_count_%d", stage))
	guardAlias := quoteIdentifier(fmt.Sprintf("__os_spath_mv_guard_%d", stage))
	initialStateAlias := quoteIdentifier(fmt.Sprintf("__os_spath_mv_state_%d_0", stage))

	sourceSQL := "SELECT *, toUInt8(ifNull(" + sourceEligibleSQL + ", 0)) AS " +
		sourceEligibleAlias + ", if(" + sourceEligibleAlias + " != 0, assumeNotNull(" +
		inputSQL + "), CAST('' AS String)) AS " + inputAlias + " FROM (" +
		relation.sql + ") AS " + quoteIdentifier(fmt.Sprintf("_stage_%d_spath_mv_source", stage))
	relation = relation.selectFrom(sourceSQL, operator.Range)
	prefixArgs := append([]any(nil), inputArgs...)

	overInput := sourceEligibleAlias + " != 0 AND length(" + inputAlias + ") > " +
		strconv.Itoa(MaximumSpathInputBytes)
	eligibleSQL := "toUInt8(if(" + overInput + ", throwIf(toUInt8(" + overInput +
		"), '" + SpathInputLimitMarker + "') = 0, " + sourceEligibleAlias +
		" != 0)) AS " + eligibleAlias
	// A JSON token consumes at least one input byte. Inputs no larger than the
	// token ceiling therefore cannot exceed it, so avoid the regex scan on the
	// common small-input path just as fixed-selector spath does.
	needsTokenPreflight := eligibleAlias + " != 0 AND length(" + inputAlias + ") > " +
		strconv.Itoa(MaximumSpathJSONTokens)
	preflightInput := "if(" + needsTokenPreflight + ", " + inputAlias +
		", CAST('' AS String))"
	tokenCountSQL := "if(" + needsTokenPreflight + ", countMatches(" + preflightInput +
		", ?), toUInt64(0)) AS " + tokenCountAlias
	preflightSQL := "SELECT *, " + eligibleSQL + ", " + tokenCountSQL + " FROM (" +
		relation.sql + ") AS " + quoteIdentifier(fmt.Sprintf("_stage_%d_spath_mv_preflight", stage))
	relation = relation.selectFrom(preflightSQL, operator.Range)
	prefixArgs = prependArguments([]any{spathJSONTokenPattern}, prefixArgs)

	overTokens := eligibleAlias + " != 0 AND " + tokenCountAlias + " > " +
		strconv.Itoa(MaximumSpathJSONTokens)
	guardSQL := "toUInt8(if(" + overTokens + ", throwIf(toUInt8(" + overTokens +
		"), '" + SpathJSONLexemeLimitMarker + "') = 0, " + eligibleAlias +
		" != 0 AND isValidJSON(" + inputAlias + "))) AS " + guardAlias
	initialSQL := "tuple(if(" + guardAlias + " != 0, [" + inputAlias +
		"], CAST([], 'Array(String)')), toUInt8(0)) AS " + initialStateAlias
	guardedSQL := "SELECT *, " + guardSQL + ", " + initialSQL + " FROM (" +
		relation.sql + ") AS " + quoteIdentifier(fmt.Sprintf("_stage_%d_spath_mv_guard", stage))
	relation = relation.selectFrom(guardedSQL, operator.Range)

	currentState := initialStateAlias
	helperColumns := []string{
		sourceEligibleAlias, inputAlias, eligibleAlias, tokenCountAlias, guardAlias,
		initialStateAlias,
	}
	for index, step := range steps {
		nextState := quoteIdentifier(fmt.Sprintf("__os_spath_mv_state_%d_%d", stage, index+1))
		stepSQL := spathWildcardStepStateSQL(
			"tupleElement("+currentState+", 1)",
			step,
			index+1 == len(steps),
		)
		traversalSQL := "SELECT *, " + stepSQL + " AS " + nextState + " FROM (" +
			relation.sql + ") AS " + quoteIdentifier(fmt.Sprintf(
			"_stage_%d_spath_mv_step_%d", stage, index,
		))
		relation = relation.selectFrom(traversalSQL, operator.Range)
		prefixArgs = prependArguments([]any{step.Key}, prefixArgs)
		currentState = nextState
		helperColumns = append(helperColumns, nextState)
	}

	previous, previousKnown, err := resolveCompiledField(operator.Output, state)
	if err != nil {
		return compiledRelation{}, compileState{}, nil, 0, err
	}
	if previousKnown {
		if err := validateKnowledgeFieldSidecars(
			previous.relativeFieldNamesSQL,
			previous.relativeFieldTypesSQL,
			previous.fieldMetadataVersionSQL,
		); err != nil {
			return compiledRelation{}, compileState{}, nil, 0, fmt.Errorf(
				"compile ClickHouse wildcard spath prior output %q: %w",
				operator.Output.Name,
				err,
			)
		}
	}
	priorState, err := compileSpathPriorMVState(previous, previousKnown)
	if err != nil {
		return compiledRelation{}, compileState{}, nil, 0, err
	}

	rawNodes := "tupleElement(traversal_state, 1)"
	matched := "tupleElement(traversal_state, 2) != 0"
	number := "match(raw, number_pattern)"
	supported := "(" + number + " OR raw IN ('null', 'true', 'false') OR " +
		"startsWith(raw, char(34)))"
	newInvalid := "arrayExists(raw -> NOT (" + supported + "), " + rawNodes + ")"
	newMember := "if(" + number + ", " + spathJSONNumberDynamicSQL("raw") +
		", JSONExtract(raw, 'Dynamic'))"
	newValues := "arrayMap(raw -> " + newMember + ", " + rawNodes + ")"
	// Preflight the exact canonical payload one scalar at a time, before the
	// Array(Dynamic) publication is constructed. In particular, an exactly
	// representable JSON exponent may become a longer canonical Float spelling,
	// while decoded strings may become shorter than their escaped JSON source.
	newMemberBytes := bindSQLExpressions(
		[]string{"member"},
		[]string{newMember},
		"toUInt128(length("+nativeMVCanonicalTextSQL("member")+"))",
	)
	newPayload := "arrayFold((bytes, raw) -> bytes + " + newMemberBytes + ", " +
		rawNodes + ", toUInt128(0))"
	previousValues := "tupleElement(prior_state, 1)"
	previousExists := "tupleElement(prior_state, 2) != 0"
	previousPresent := "tupleElement(prior_state, 3) != 0"
	previousInvalid := "tupleElement(prior_state, 4) != 0"
	selectedValues := "if(" + matched + ", " + newValues + ", " + previousValues + ")"
	selectedInvalid := "if(" + matched + ", " + newInvalid + ", " + previousInvalid + ")"
	selectedMembers := "if(" + matched + ", toUInt64(length(" + rawNodes +
		")), toUInt64(length(" + previousValues + ")))"
	selectedPayload := "if(" + matched + ", " + newPayload + ", " +
		nativeMVArrayPayloadBytesSQL(previousValues) + ")"
	guardedValues := nativeMVPreflightSQL(
		selectedValues,
		selectedInvalid,
		selectedMembers,
		selectedPayload,
		emptyNativeMVSQL(),
	)
	selectedPresent := "toUInt8(if(" + matched + ", 1, " + previousPresent + "))"
	selectedExists := "toUInt8(if(" + matched + ", 1, " + previousExists + "))"
	finalStateBody := "tuple(" + guardedValues + ", " + selectedExists + ", " +
		selectedPresent + ")"
	finalStateExpression := bindSQLExpressions(
		[]string{"traversal_state", "prior_state", "number_pattern"},
		[]string{currentState, priorState.sql, "CAST(? AS String)"},
		finalStateBody,
	)
	finalStateAlias := quoteIdentifier(fmt.Sprintf("__os_spath_mv_final_%d", stage))
	finalStateSQL := "SELECT *, " + finalStateExpression + " AS " + finalStateAlias +
		" FROM (" + relation.sql + ") AS " +
		quoteIdentifier(fmt.Sprintf("_stage_%d_spath_mv_final", stage))
	relation = relation.selectFrom(finalStateSQL, operator.Range)
	prefixArgs = prependArguments(
		append(append([]any(nil), priorState.args...), spathJSONNumberPattern),
		prefixArgs,
	)
	helperColumns = append(helperColumns, finalStateAlias)

	outputStateAlias := quoteIdentifier(fmt.Sprintf("__os_spath_mv_output_state_%d", stage))
	outputName := quoteIdentifier(operator.Output.Name)
	baseProjection := "* EXCEPT (" + strings.Join(helperColumns, ", ") + ")"
	publication := upsertWildcardFieldProjection(
		baseProjection,
		state,
		operator.Output.Name,
		"tupleElement("+finalStateAlias+", 1)",
		quoteIdentifier(fmt.Sprintf("_stage_%d_spath_mv_publish", stage)),
		authoredFieldPhysicallyPublic(state, operator.Output.Name),
	)
	publishAlias := quoteIdentifier(fmt.Sprintf("_stage_%d_spath_mv_publish", stage))
	validationInput := quoteIdentifier(fmt.Sprintf("__os_spath_mv_validation_%d", stage))
	publishSQL := "WITH " + validationInput + " AS MATERIALIZED (" + relation.sql +
		") SELECT " + publication + ", tuple(toUInt8(tupleElement(" + finalStateAlias +
		", 2)), toUInt8(tupleElement(" + finalStateAlias + ", 3))) AS " +
		outputStateAlias + " FROM " + validationInput + " AS " +
		publishAlias + " WHERE ignore(" + finalStateAlias + ") = 0"
	relation = relation.selectFrom(publishSQL, operator.Range)

	nextBase := cloneCompileState(state)
	if sourceMayExtract && exposesRawFieldsPayload(state) {
		dropRawFieldsPayload(&nextBase)
	}
	nextBase.privateColumns = append(nextBase.privateColumns, outputStateAlias)
	next, err := extendCompileState(
		nextBase,
		operator.Output,
		compiledScalar{
			valueSQL:                     outputName,
			existsSQL:                    "tupleElement(" + outputStateAlias + ", 1) != 0",
			optionalMultivaluePresentSQL: "tupleElement(" + outputStateAlias + ", 2) != 0",
			kind:                         fieldKindDynamicArray,
			maxStringBytes:               spl.MaximumNativeMVPayloadBytes,
			materializeForPredicate: sourceMayExtract ||
				previous.materializeForPredicate,
		},
		false,
	)
	if err != nil {
		return compiledRelation{}, compileState{}, nil, 0, err
	}
	markNativeMVRuntimeValidation(state)
	return relation, next, prefixArgs, 1, nil
}
