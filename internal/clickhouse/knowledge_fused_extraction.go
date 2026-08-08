package clickhouse

import (
	"errors"
	"fmt"
	"slices"
	"strconv"

	"github.com/Suhaibinator/open-splunk/internal/eventfields"
	"github.com/Suhaibinator/open-splunk/internal/knowledgeprogram"
	"github.com/Suhaibinator/open-splunk/internal/plan"
	"github.com/Suhaibinator/open-splunk/internal/spl"
)

const (
	knowledgeExtractionPreviousValueElement      = 1
	knowledgeExtractionPreviousPresentElement    = 2
	knowledgeExtractionPreviousStoredTypeElement = 3
	knowledgeExtractionPreviousDescendantElement = 4
)

// compiledKnowledgeExtractionOperation proves the exact interleaved typed
// operation order emitted by one physical extraction stage.
type compiledKnowledgeExtractionOperation struct {
	kind  knowledgeprogram.OperatorKind
	regex knowledgeprogram.RegexExtraction
	json  knowledgeprogram.JSONExtraction
}

// compiledKnowledgeFusedExtractionProjection is a relation-neutral two-layer
// extraction stage. The central compiler emits bindingProjection and the
// singleton ARRAY JOIN bindings over the frozen input, then projection in an
// outer SELECT. suffixArgs follow every argument owned by the input relation.
type compiledKnowledgeFusedExtractionProjection struct {
	bindingProjection         []string
	projection                []string
	arrayJoinBindings         []string
	state                     compileState
	suffixArgs                []any
	selectorCharges           compiledKnowledgeSelectorChargeColumns
	capturedBytes             string
	emittedOperations         []compiledKnowledgeExtractionOperation
	emittedOperatorCount      uint32
	emittedOutputCount        uint32
	emittedRegexPrograms      uint32
	emittedRegexWorkUnits     uint64
	emittedJSONEvaluationWork uint32
}

type compiledKnowledgeExtractionOutput struct {
	destination    plan.FieldRef
	selector       knowledgeprogram.Selector
	overwrite      knowledgeprogram.OverwriteBehavior
	producedSQL    string
	valueSQL       string
	storedTypeSQL  string
	maxStringBytes uint64
}

type compiledKnowledgeExtractionObject struct {
	proof              compiledKnowledgeExtractionOperation
	bindingAlias       string
	bindingSQL         string
	args               []any
	outputs            []compiledKnowledgeExtractionOutput
	selectorInputSQL   string
	selectorQuerySQL   string
	capturedBytesSQL   string
	regexWorkUnits     uint64
	jsonEvaluationWork uint32
}

type compiledKnowledgeExtractionDestination struct {
	destination          plan.FieldRef
	candidates           []compiledKnowledgeExtractionOutput
	previousAlias        string
	previousSQL          string
	previousArgs         []any
	valueSQL             string
	existsAlias          string
	existsProjection     string
	typeAlias            string
	typeProjection       string
	descendantAlias      string
	descendantProjection string
	maxStringBytes       uint64
}

// compileKnowledgeExtractionStage lowers every regex and JSON object in the
// exact Program.OperatorKinds order against one frozen input relation.
func compileKnowledgeExtractionStage(
	state compileState,
	program knowledgeprogram.Program,
	stage int,
	priorCharges compiledKnowledgeSelectorChargeColumns,
) (compiledKnowledgeFusedExtractionProjection, error) {
	if program.IsZero() || stage < 0 {
		return compiledKnowledgeFusedExtractionProjection{}, errors.New(
			"compile ClickHouse knowledge extraction stage: invalid program or stage",
		)
	}
	if err := validateKnowledgePriorSelectorCharges(priorCharges); err != nil {
		return compiledKnowledgeFusedExtractionProjection{}, err
	}
	if err := validateKnowledgeExtractionInputState(state); err != nil {
		return compiledKnowledgeFusedExtractionProjection{}, err
	}
	objects, err := compileKnowledgeExtractionObjects(program, stage)
	if err != nil {
		return compiledKnowledgeFusedExtractionProjection{}, err
	}
	if len(objects) == 0 {
		return compiledKnowledgeFusedExtractionProjection{}, errors.New(
			"compile ClickHouse knowledge extraction stage: no extraction objects",
		)
	}
	if len(objects) > knowledgeprogram.MaximumObjects {
		return compiledKnowledgeFusedExtractionProjection{}, errors.New(
			"compile ClickHouse knowledge extraction stage: too many extraction objects",
		)
	}

	next := cloneCompileState(state)
	if exposesRawFieldsPayload(state) {
		dropRawFieldsPayload(&next)
	}
	groups := make([]compiledKnowledgeExtractionDestination, 0)
	groupByName := make(map[string]int)
	outputCount := 0
	for _, object := range objects {
		for _, output := range object.outputs {
			outputCount++
			if outputCount > knowledgeprogram.MaximumExtractionOutputs {
				return compiledKnowledgeFusedExtractionProjection{}, errors.New(
					"compile ClickHouse knowledge extraction stage: too many extraction outputs",
				)
			}
			name := output.destination.Name
			groupIndex, ok := groupByName[name]
			if !ok {
				groupIndex = len(groups)
				groupByName[name] = groupIndex
				groups = append(groups, compiledKnowledgeExtractionDestination{destination: output.destination})
			}
			for _, previous := range groups[groupIndex].candidates {
				if !previous.selector.ProvablyDisjoint(output.selector) {
					return compiledKnowledgeFusedExtractionProjection{}, fmt.Errorf(
						"compile ClickHouse knowledge extraction stage: repeated destination %q is not provably disjoint",
						name,
					)
				}
			}
			groups[groupIndex].candidates = append(groups[groupIndex].candidates, output)
		}
	}
	for index := range groups {
		_, previousSQL, previousArgs, previousBound, previousErr :=
			compileKnowledgeExtractionPrevious(groups[index].destination.Name, state)
		if previousErr != nil {
			return compiledKnowledgeFusedExtractionProjection{}, previousErr
		}
		groups[index].previousAlias = quoteIdentifier(fmt.Sprintf(
			"__os_ko_extract_previous_%d_%d",
			stage,
			index,
		))
		groups[index].previousSQL = previousSQL
		groups[index].previousArgs = previousArgs
		groups[index].maxStringBytes = previousBound
		finalizeKnowledgeExtractionDestination(&groups[index], stage, index)
		name := groups[index].destination.Name
		delete(next.blocked, name)
		if !slices.Contains(next.publicOrder, name) {
			next.publicOrder = append(next.publicOrder, name)
		}
		publicName := quoteIdentifier(name)
		next.visible[name] = fieldState{
			valueSQL:                publicName,
			maxStringBytes:          groups[index].maxStringBytes,
			textEligibleSQL:         groups[index].typeAlias + " = toUInt8(" + strconv.Itoa(int(eventfields.StoredValueTypeString)) + ")",
			dynamicTypeSQL:          "dynamicType(" + publicName + ")",
			storedTypeSQL:           groups[index].typeAlias,
			existsSQL:               groups[index].existsAlias,
			descendantSQL:           groups[index].descendantAlias,
			kind:                    fieldKindDynamic,
			materializeForPredicate: true,
		}
	}

	liveOldPrivate := livePrivateColumns(state.privateColumns, next.visible)
	next.privateColumns = append([]string(nil), liveOldPrivate...)
	for _, group := range groups {
		next.privateColumns = append(next.privateColumns, group.existsAlias, group.typeAlias, group.descendantAlias)
	}

	bindingProjection := make([]string, 0, len(state.visible)+len(objects)+12)
	for _, name := range orderedVisibleNames(state) {
		field := state.visible[name]
		publicName := quoteIdentifier(name)
		if field.valueSQL == publicName {
			bindingProjection = append(bindingProjection, publicName)
		} else {
			bindingProjection = append(bindingProjection, field.valueSQL+" AS "+publicName)
		}
	}
	bindingProjection = appendPrivateEventProjection(bindingProjection, state)
	if priorCharges.inputBytes != "" {
		if !slices.Contains(bindingProjection, priorCharges.inputBytes) {
			bindingProjection = append(bindingProjection, priorCharges.inputBytes)
		}
		if !slices.Contains(bindingProjection, priorCharges.queryUnits) {
			bindingProjection = append(bindingProjection, priorCharges.queryUnits)
		}
	}
	bindings := make([]string, 0, len(objects)+len(groups))
	suffixArgs := make([]any, 0)
	inputCharges := make([]string, 0, len(objects)+1)
	queryCharges := make([]string, 0, len(objects)+1)
	captureCharges := make([]string, 0, len(objects)+1)
	if priorCharges.inputBytes != "" {
		inputCharges = append(inputCharges, "toUInt128("+priorCharges.inputBytes+")")
		queryCharges = append(queryCharges, "toUInt128("+priorCharges.queryUnits+")")
	}
	if state.rexCapturedBytesSQL != "" {
		captureCharges = append(captureCharges, "toUInt128("+state.rexCapturedBytesSQL+")")
	}
	proof := make([]compiledKnowledgeExtractionOperation, 0, len(objects))
	var emittedOutputCount, regexPrograms uint32
	var regexWork uint64
	var jsonWork uint32
	for _, object := range objects {
		bindingProjection = append(bindingProjection, object.bindingAlias)
		bindings = append(bindings, "["+object.bindingSQL+"] AS "+object.bindingAlias)
		suffixArgs = append(suffixArgs, object.args...)
		inputCharges = append(inputCharges, object.selectorInputSQL)
		queryCharges = append(queryCharges, object.selectorQuerySQL)
		if object.capturedBytesSQL != "" {
			captureCharges = append(captureCharges, object.capturedBytesSQL)
			regexPrograms++
			regexWork += object.regexWorkUnits
		}
		jsonWork += object.jsonEvaluationWork
		emittedOutputCount += uint32(len(object.outputs))
		proof = append(proof, object.proof)
	}
	for _, group := range groups {
		bindingProjection = append(bindingProjection, group.previousAlias)
		bindings = append(bindings, "["+group.previousSQL+"] AS "+group.previousAlias)
		suffixArgs = append(suffixArgs, group.previousArgs...)
	}

	projection := make([]string, 0, len(next.visible)+len(groups)*3+4)
	groupByDestination := make(map[string]compiledKnowledgeExtractionDestination, len(groups))
	for _, group := range groups {
		groupByDestination[group.destination.Name] = group
	}
	for _, name := range orderedVisibleNames(next) {
		publicName := quoteIdentifier(name)
		if group, generated := groupByDestination[name]; generated {
			projection = append(projection, group.valueSQL+" AS "+publicName)
			continue
		}
		if _, ok := state.visible[name]; !ok {
			return compiledKnowledgeFusedExtractionProjection{}, fmt.Errorf(
				"compile ClickHouse knowledge extraction stage: field %q has no frozen input value",
				name,
			)
		}
		projection = append(projection, publicName)
	}
	projectionState := next
	projectionState.privateColumns = liveOldPrivate
	if regexPrograms != 0 {
		projectionState.rexCapturedBytesSQL = ""
	}
	projection = appendPrivateEventProjection(projection, projectionState)
	for _, group := range groups {
		projection = append(projection, group.existsProjection, group.typeProjection, group.descendantProjection)
	}
	chargeColumns := compiledKnowledgeSelectorChargeColumns{
		inputBytes: quoteIdentifier(fmt.Sprintf("__os_ko_selector_input_bytes_%d", stage)),
		queryUnits: quoteIdentifier(fmt.Sprintf("__os_ko_selector_query_units_%d", stage)),
	}
	projection = append(projection,
		knowledgeUInt128Sum(inputCharges)+" AS "+chargeColumns.inputBytes,
		knowledgeUInt128Sum(queryCharges)+" AS "+chargeColumns.queryUnits,
	)
	capturedBytes := state.rexCapturedBytesSQL
	if regexPrograms != 0 {
		capturedBytes = quoteIdentifier(fmt.Sprintf("__os_ko_rex_captured_bytes_%d", stage))
		projection = append(projection, knowledgeUInt128Sum(captureCharges)+" AS "+capturedBytes)
	}
	next.rexCapturedBytesSQL = capturedBytes
	return compiledKnowledgeFusedExtractionProjection{
		bindingProjection:         bindingProjection,
		projection:                projection,
		arrayJoinBindings:         bindings,
		state:                     next,
		suffixArgs:                suffixArgs,
		selectorCharges:           chargeColumns,
		capturedBytes:             capturedBytes,
		emittedOperations:         proof,
		emittedOperatorCount:      uint32(len(objects)),
		emittedOutputCount:        emittedOutputCount,
		emittedRegexPrograms:      regexPrograms,
		emittedRegexWorkUnits:     regexWork,
		emittedJSONEvaluationWork: jsonWork,
	}, nil
}

func validateKnowledgeExtractionInputState(state compileState) error {
	if !state.eventRows || !state.allowDynamic || len(state.blocked) != 0 ||
		len(state.blockedPrefixes) != 0 {
		return errors.New(
			"compile ClickHouse knowledge extraction stage: input is not the complete event relation",
		)
	}
	for _, name := range []string{"_raw", "index", "host", "source", "sourcetype"} {
		field, ok := state.visible[name]
		if !ok || field.valueSQL != quoteIdentifier(name) {
			return fmt.Errorf(
				"compile ClickHouse knowledge extraction stage: canonical input %q is unavailable",
				name,
			)
		}
	}
	return nil
}

func compileKnowledgeExtractionObjects(
	program knowledgeprogram.Program,
	stage int,
) ([]compiledKnowledgeExtractionObject, error) {
	regex := program.RegexExtractions()
	jsonObjects := program.JSONExtractions()
	regexIndex, jsonIndex := 0, 0
	objects := make([]compiledKnowledgeExtractionObject, 0, len(regex)+len(jsonObjects))
	pastExtractions := false
	for _, kind := range program.OperatorKinds() {
		switch kind {
		case knowledgeprogram.OperatorConditionalExtract:
			if pastExtractions || regexIndex >= len(regex) {
				return nil, errors.New("compile ClickHouse knowledge extraction stage: regex order disagrees")
			}
			object, err := compileKnowledgeRegexExtractionObject(regex[regexIndex], stage, len(objects))
			if err != nil {
				return nil, err
			}
			objects = append(objects, object)
			regexIndex++
		case knowledgeprogram.OperatorConditionalExtractJSON:
			if pastExtractions || jsonIndex >= len(jsonObjects) {
				return nil, errors.New("compile ClickHouse knowledge extraction stage: JSON order disagrees")
			}
			object, err := compileKnowledgeJSONExtractionObject(jsonObjects[jsonIndex], stage, len(objects))
			if err != nil {
				return nil, err
			}
			objects = append(objects, object)
			jsonIndex++
		case knowledgeprogram.OperatorCopyFieldAlias, knowledgeprogram.OperatorParallelExtend:
			pastExtractions = true
		default:
			return nil, errors.New("compile ClickHouse knowledge extraction stage: operator kind is invalid")
		}
	}
	if regexIndex != len(regex) || jsonIndex != len(jsonObjects) {
		return nil, errors.New("compile ClickHouse knowledge extraction stage: extraction inventory disagrees")
	}
	return objects, nil
}

func compileKnowledgeRegexExtractionObject(
	operation knowledgeprogram.RegexExtraction,
	stage, objectIndex int,
) (compiledKnowledgeExtractionObject, error) {
	if operation.Origin().StageOrdinal() != uint32(objectIndex) {
		return compiledKnowledgeExtractionObject{}, errors.New("compile ClickHouse knowledge extraction stage: regex provenance order disagrees")
	}
	compiled, err := compileKnowledgeRegexExtraction(operation)
	if err != nil {
		return compiledKnowledgeExtractionObject{}, err
	}
	alias := quoteIdentifier(fmt.Sprintf("__os_ko_extract_binding_%d_%d", stage, objectIndex))
	resultSQL := alias
	outputs := make([]compiledKnowledgeExtractionOutput, 0, len(compiled.captures))
	for _, capture := range compiled.captures {
		destination, err := resolveKnowledgeExtractionDestination(capture.name)
		if err != nil {
			return compiledKnowledgeExtractionObject{}, err
		}
		outputs = append(outputs, compiledKnowledgeExtractionOutput{
			destination: destination, selector: operation.Selector(), overwrite: operation.Overwrite(),
			producedSQL:    capture.presentSQL(resultSQL),
			valueSQL:       "CAST(" + capture.valueSQL(resultSQL) + " AS Dynamic)",
			storedTypeSQL:  "toUInt8(" + strconv.Itoa(int(eventfields.StoredValueTypeString)) + ")",
			maxStringBytes: uint64(MaximumRexCapturedBytesPerRow),
		})
	}
	return compiledKnowledgeExtractionObject{
		proof:        compiledKnowledgeExtractionOperation{kind: knowledgeprogram.OperatorConditionalExtract, regex: compiled.operation},
		bindingAlias: alias, bindingSQL: compiled.sql, args: append([]any(nil), compiled.args...), outputs: outputs,
		selectorInputSQL: compiled.selectorInputBytesSQL(resultSQL),
		selectorQuerySQL: compiled.selectorQueryUnitsSQL(resultSQL),
		capturedBytesSQL: compiled.capturedBytesSQL(resultSQL), regexWorkUnits: compiled.programWorkUnits,
	}, nil
}

func compileKnowledgeJSONExtractionObject(
	operation knowledgeprogram.JSONExtraction,
	stage, objectIndex int,
) (compiledKnowledgeExtractionObject, error) {
	if operation.Origin().StageOrdinal() != uint32(objectIndex) {
		return compiledKnowledgeExtractionObject{}, errors.New("compile ClickHouse knowledge extraction stage: JSON provenance order disagrees")
	}
	compiled, err := compileKnowledgeJSONExtraction(operation)
	if err != nil {
		return compiledKnowledgeExtractionObject{}, err
	}
	alias := quoteIdentifier(fmt.Sprintf("__os_ko_extract_binding_%d_%d", stage, objectIndex))
	resultSQL := alias
	destination, err := resolveKnowledgeExtractionDestination(operation.Output())
	if err != nil {
		return compiledKnowledgeExtractionObject{}, err
	}
	valueSQL := compiled.valueSQL(resultSQL)
	output := compiledKnowledgeExtractionOutput{
		destination: destination, selector: operation.Selector(), overwrite: operation.Overwrite(),
		producedSQL: compiled.producedSQL(resultSQL), valueSQL: valueSQL,
		storedTypeSQL:  knowledgeDynamicStoredTypeSQL(valueSQL),
		maxStringBytes: uint64(MaximumSpathInputBytes),
	}
	return compiledKnowledgeExtractionObject{
		proof:        compiledKnowledgeExtractionOperation{kind: knowledgeprogram.OperatorConditionalExtractJSON, json: compiled.operation},
		bindingAlias: alias, bindingSQL: compiled.sql, args: append([]any(nil), compiled.args...),
		outputs:            []compiledKnowledgeExtractionOutput{output},
		selectorInputSQL:   compiled.selectorInputBytesSQL(resultSQL),
		selectorQuerySQL:   compiled.selectorQueryUnitsSQL(resultSQL),
		jsonEvaluationWork: compiled.evaluationWorkUnits,
	}, nil
}

func resolveKnowledgeExtractionDestination(name string) (plan.FieldRef, error) {
	destination, err := plan.ResolveField(name, spl.Range{})
	if err != nil || destination.Canonical || destination.Name != name {
		return plan.FieldRef{}, fmt.Errorf(
			"compile ClickHouse knowledge extraction destination %q: invalid exact field",
			name,
		)
	}
	return destination, nil
}

func compileKnowledgeExtractionPrevious(
	name string,
	state compileState,
) (plan.FieldRef, string, []any, uint64, error) {
	destination, err := resolveKnowledgeExtractionDestination(name)
	if err != nil {
		return plan.FieldRef{}, "", nil, 0, err
	}
	field, present, err := resolveCompiledField(destination, state)
	if err != nil {
		return plan.FieldRef{}, "", nil, 0, err
	}
	valueSQL, presentSQL, typeSQL, descendantSQL := "CAST(NULL AS Dynamic)", "toUInt8(0)", "toUInt8(0)", "toUInt8(0)"
	var args []any
	var bound uint64
	if present {
		valueSQL = "CAST(" + field.valueSQL + " AS Dynamic)"
		var presentArgs, typeArgs []any
		presentSQL, presentArgs = knownFieldPresenceSQL(field)
		typeSQL, typeArgs, err = knownFieldStoredTypeSQL(field)
		if err != nil {
			return plan.FieldRef{}, "", nil, 0, fmt.Errorf("compile ClickHouse knowledge extraction prior type for %q: %w", name, err)
		}
		args = append(args, presentArgs...)
		args = append(args, typeArgs...)
		if field.descendantSQL != "" {
			descendantSQL = field.descendantSQL
			args = append(args, field.descendantArgs...)
		}
		bound = fieldStateStringByteBound(field)
	}
	previousSQL := "tuple(" + valueSQL + ", toUInt8(ifNull(" + presentSQL + ", 0)), toUInt8(" + typeSQL + "), toUInt8(ifNull(" + descendantSQL + ", 0)))"
	return destination, previousSQL, args, bound, nil
}

func finalizeKnowledgeExtractionDestination(
	group *compiledKnowledgeExtractionDestination,
	stage, groupIndex int,
) {
	value := knowledgeTupleElement(group.previousAlias, knowledgeExtractionPreviousValueElement)
	previousPresent := knowledgeTupleElementUInt8(group.previousAlias, knowledgeExtractionPreviousPresentElement)
	present := previousPresent
	storedType := knowledgeTupleElementUInt8(group.previousAlias, knowledgeExtractionPreviousStoredTypeElement)
	descendant := knowledgeTupleElementUInt8(group.previousAlias, knowledgeExtractionPreviousDescendantElement)
	for index := len(group.candidates) - 1; index >= 0; index-- {
		candidate := group.candidates[index]
		write := candidate.producedSQL + " != 0"
		if candidate.overwrite == knowledgeprogram.PreserveExisting {
			write += " AND " + previousPresent + " = 0"
		}
		value = "if(" + write + ", " + candidate.valueSQL + ", " + value + ")"
		present = "toUInt8(if(" + write + ", 1, " + present + "))"
		storedType = "toUInt8(if(" + write + ", " + candidate.storedTypeSQL + ", " + storedType + "))"
		descendant = "toUInt8(if(" + write + ", 0, " + descendant + "))"
		group.maxStringBytes = max(group.maxStringBytes, candidate.maxStringBytes)
	}
	group.valueSQL = value
	group.existsAlias = quoteIdentifier(fmt.Sprintf("__os_ko_extract_exists_%d_%d", stage, groupIndex))
	group.typeAlias = quoteIdentifier(fmt.Sprintf("__os_ko_extract_type_%d_%d", stage, groupIndex))
	group.descendantAlias = quoteIdentifier(fmt.Sprintf("__os_ko_extract_descendant_%d_%d", stage, groupIndex))
	group.existsProjection = present + " AS " + group.existsAlias
	group.typeProjection = storedType + " AS " + group.typeAlias
	group.descendantProjection = descendant + " AS " + group.descendantAlias
}
