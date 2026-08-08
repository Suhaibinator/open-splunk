package clickhouse

import (
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/eventfields"
	"github.com/Suhaibinator/open-splunk/internal/knowledgeprogram"
	"github.com/Suhaibinator/open-splunk/internal/plan"
	"github.com/Suhaibinator/open-splunk/internal/spl"
)

const (
	// A field assignment keeps produced presence separate from its Dynamic
	// value. Dynamic null is a present value; only the first element says that
	// the assignment produced nothing and therefore may not erase a prior
	// destination.
	knowledgeFieldAssignmentProducedElement           = 1
	knowledgeFieldAssignmentValueElement              = 2
	knowledgeFieldAssignmentStoredTypeElement         = 3
	knowledgeFieldAssignmentSelectorInputBytesElement = 4
	knowledgeFieldAssignmentSelectorQueryUnitsElement = 5

	knowledgeFieldBindingAssignmentElement         = 1
	knowledgeFieldBindingPreviousValueElement      = 2
	knowledgeFieldBindingPreviousPresentElement    = 3
	knowledgeFieldBindingPreviousStoredTypeElement = 4
	knowledgeFieldBindingPreviousDescendantElement = 5

	maxCompiledKnowledgeFieldAssignmentSQLBytes = 64 << 10
)

// compiledKnowledgeFieldAssignment is one selector-guarded, row-local
// assignment. The tuple is independent of destination overwrite state, so a
// fused stage can compile every assignment against exactly the same frozen
// input and publish all destinations atomically.
type compiledKnowledgeFieldAssignment struct {
	sql            string
	args           []any
	destination    plan.FieldRef
	selector       knowledgeprogram.Selector
	overwrite      knowledgeprogram.OverwriteBehavior
	origin         knowledgeprogram.Origin
	maxStringBytes uint64
	alias          knowledgeprogram.Alias
	calculated     knowledgeprogram.Calculated
}

func (compiled compiledKnowledgeFieldAssignment) producedSQL(resultSQL string) string {
	return knowledgeTupleElementUInt8(resultSQL, knowledgeFieldAssignmentProducedElement)
}

func (compiled compiledKnowledgeFieldAssignment) valueSQL(resultSQL string) string {
	return "tupleElement(" + resultSQL + ", " +
		strconv.Itoa(knowledgeFieldAssignmentValueElement) + ")"
}

func (compiled compiledKnowledgeFieldAssignment) storedTypeSQL(resultSQL string) string {
	return knowledgeTupleElementUInt8(resultSQL, knowledgeFieldAssignmentStoredTypeElement)
}

func (compiled compiledKnowledgeFieldAssignment) selectorInputBytesSQL(resultSQL string) string {
	return knowledgeTupleElementUInt128(resultSQL, knowledgeFieldAssignmentSelectorInputBytesElement)
}

func (compiled compiledKnowledgeFieldAssignment) selectorQueryUnitsSQL(resultSQL string) string {
	return knowledgeTupleElementUInt128(resultSQL, knowledgeFieldAssignmentSelectorQueryUnitsElement)
}

type compiledKnowledgeSelectorChargeColumns struct {
	inputBytes string
	queryUnits string
}

// compiledKnowledgeFusedFieldProjection is a relation-neutral plan for one
// physical alias or calculated projection. The central compiler first emits
// bindingProjection and arrayJoinBindings over the frozen input, then emits
// projection in an outer SELECT. Keeping destination aliases out of the inner
// layer prevents ClickHouse alias substitution from feeding a same-stage
// result back into its own input. selectorCharges are per-row columns and must
// be included in the whole-event and whole-query selector guards before final
// evidence opens.
type compiledKnowledgeFusedFieldProjection struct {
	bindingProjection  []string
	projection         []string
	arrayJoinBindings  []string
	state              compileState
	suffixArgs         []any
	selectorCharges    compiledKnowledgeSelectorChargeColumns
	emittedAssignments uint32
	aliases            []knowledgeprogram.Alias
	calculated         []knowledgeprogram.Calculated
}

// compileKnowledgeAliasAssignment lowers one immutable alias against the
// frozen extraction-stage state. Direct scalar, array, and empty-object values
// remain exact Dynamic values. Non-empty object parents are flattened in the
// source JSON representation; the central nonempty-finalization gate must stay
// closed until its object materializer remaps the retained descendants to the
// destination instead of publishing the parent's Dynamic None value.
func compileKnowledgeAliasAssignment(
	operation knowledgeprogram.Alias,
	state compileState,
) (compiledKnowledgeFieldAssignment, error) {
	if err := validateKnowledgeAliasOperation(operation); err != nil {
		return compiledKnowledgeFieldAssignment{}, err
	}
	source, err := plan.ResolveField(operation.Source(), spl.Range{})
	if err != nil {
		return compiledKnowledgeFieldAssignment{}, fmt.Errorf(
			"compile ClickHouse knowledge alias source: %w",
			err,
		)
	}
	field, present, err := resolveCompiledField(source, state)
	if err != nil {
		return compiledKnowledgeFieldAssignment{}, err
	}
	value := compiledScalar{
		valueSQL:       "CAST(NULL AS Nullable(String))",
		maxStringBytes: 1,
		existsSQL:      "0",
		kind:           fieldKindString,
		alwaysNull:     true,
	}
	if present {
		value = compiledScalarFromField(field)
	}
	compiled, err := compileKnowledgeFieldAssignment(
		operation.Selector(),
		operation.Destination(),
		operation.Overwrite(),
		operation.Origin(),
		value,
	)
	if err != nil {
		return compiledKnowledgeFieldAssignment{}, err
	}
	compiled.alias = operation
	return compiled, nil
}

// compileKnowledgeCalculatedAssignment reconstructs and recompiles the exact
// retained expression against the frozen alias-stage input. It does not extend
// state, so no assignment in the fused stage can observe another assignment's
// output.
func compileKnowledgeCalculatedAssignment(
	operation knowledgeprogram.Calculated,
	state compileState,
) (compiledKnowledgeFieldAssignment, error) {
	if err := validateKnowledgeCalculatedOperation(operation); err != nil {
		return compiledKnowledgeFieldAssignment{}, err
	}
	expression, err := plan.ConvertKnowledgeCalculatedExpression(operation)
	if err != nil {
		return compiledKnowledgeFieldAssignment{}, fmt.Errorf(
			"compile ClickHouse knowledge calculated expression: %w",
			err,
		)
	}
	if err := validateCompiledScalarComplexity(expression); err != nil {
		return compiledKnowledgeFieldAssignment{}, err
	}
	value, err := compileScalarValue(expression, state)
	if err != nil {
		return compiledKnowledgeFieldAssignment{}, err
	}
	compiled, err := compileKnowledgeFieldAssignment(
		operation.Selector(),
		operation.Destination(),
		operation.Overwrite(),
		operation.Origin(),
		value,
	)
	if err != nil {
		return compiledKnowledgeFieldAssignment{}, err
	}
	compiled.calculated = operation
	return compiled, nil
}

func compileKnowledgeFieldAssignment(
	selectorAuthority knowledgeprogram.Selector,
	destinationName string,
	overwrite knowledgeprogram.OverwriteBehavior,
	origin knowledgeprogram.Origin,
	value compiledScalar,
) (compiledKnowledgeFieldAssignment, error) {
	destination, err := plan.ResolveField(destinationName, spl.Range{})
	if err != nil || destination.Canonical || destination.Name != destinationName {
		if err == nil {
			err = errors.New("destination is not an exact dynamic field")
		}
		return compiledKnowledgeFieldAssignment{}, fmt.Errorf(
			"compile ClickHouse knowledge field assignment destination: %w",
			err,
		)
	}
	selector, err := compileKnowledgeSelector(selectorAuthority)
	if err != nil {
		return compiledKnowledgeFieldAssignment{}, fmt.Errorf(
			"compile ClickHouse knowledge field assignment selector: %w",
			err,
		)
	}

	const (
		selectorVariable = "__os_ko_field_selector"
		valueVariable    = "__os_ko_field_value"
		presentVariable  = "__os_ko_field_present"
		typeVariable     = "__os_ko_field_type"
	)
	presenceSQL, presenceArgs := knowledgeScalarPresenceSQL(value)
	typeSQL, typeArgs, err := knowledgeScalarStoredTypeSQL(value, valueVariable)
	if err != nil {
		return compiledKnowledgeFieldAssignment{}, err
	}

	selected := "tuple(" +
		"toUInt8(ifNull(" + presentVariable + ", 0)), " +
		"if(ifNull(" + presentVariable + ", 0), " + valueVariable +
		", CAST(NULL AS Dynamic)), " +
		"toUInt8(if(ifNull(" + presentVariable + ", 0), " + typeVariable + ", 0)), " +
		"toUInt128(tupleElement(" + selectorVariable + ", 2)), " +
		"toUInt128(tupleElement(" + selectorVariable + ", 3)))"
	selected = bindSQLExpressions(
		[]string{presentVariable, typeVariable},
		[]string{presenceSQL, typeSQL},
		selected,
	)
	selected = bindSQLExpressions(
		[]string{valueVariable},
		[]string{"CAST(" + value.valueSQL + " AS Dynamic)"},
		selected,
	)
	result := "if(tupleElement(" + selectorVariable + ", 1) != 0, " + selected +
		", " + knowledgeFieldNoAssignmentTuple(selectorVariable) + ")"
	result = bindSQLExpressions(
		[]string{selectorVariable},
		[]string{selector.sql},
		result,
	)
	if len(result) > maxCompiledKnowledgeFieldAssignmentSQLBytes {
		return compiledKnowledgeFieldAssignment{}, errors.New(
			"compile ClickHouse knowledge field assignment: generated SQL exceeds the per-object limit",
		)
	}

	// Lambda bodies precede bound values. Presence and semantic-type
	// placeholders occur inside the value lambda body, followed by the scalar
	// value itself and finally the selector authority.
	args := make([]any, 0,
		len(presenceArgs)+len(typeArgs)+len(value.valueArgs)+len(selector.args),
	)
	args = append(args, presenceArgs...)
	args = append(args, typeArgs...)
	args = append(args, value.valueArgs...)
	args = append(args, selector.args...)
	return compiledKnowledgeFieldAssignment{
		sql:            result,
		args:           args,
		destination:    destination,
		selector:       selectorAuthority,
		overwrite:      overwrite,
		origin:         origin,
		maxStringBytes: compiledScalarStringByteBound(value),
	}, nil
}

func knowledgeFieldNoAssignmentTuple(selectorVariable string) string {
	return "tuple(toUInt8(0), CAST(NULL AS Dynamic), toUInt8(0), " +
		"toUInt128(tupleElement(" + selectorVariable + ", 2)), " +
		"toUInt128(tupleElement(" + selectorVariable + ", 3)))"
}

func knowledgeScalarPresenceSQL(value compiledScalar) (string, []any) {
	field := knowledgeFieldStateFromScalar(value)
	presence, args := knownFieldPresenceSQL(field)
	return "toUInt8(ifNull(" + presence + ", 0))", args
}

func knowledgeScalarStoredTypeSQL(
	value compiledScalar,
	boundValueSQL string,
) (string, []any, error) {
	switch value.kind {
	case fieldKindInvalid:
		return "toUInt8(" + strconv.Itoa(int(eventfields.StoredValueTypeNull)) + ")", nil, nil
	case fieldKindString:
		textEligible := "isValidUTF8(dynamicElement(" + boundValueSQL + ", 'String'))"
		if value.textEligibleSQL != "" {
			textEligible = "(" + value.textEligibleSQL + ") AND " + textEligible
		}
		return "multiIf(dynamicType(" + boundValueSQL + ") = 'None', toUInt8(" +
			strconv.Itoa(int(eventfields.StoredValueTypeNull)) + "), " + textEligible +
			", toUInt8(" + strconv.Itoa(int(eventfields.StoredValueTypeString)) +
			"), toUInt8(" + strconv.Itoa(int(eventfields.StoredValueTypeBytes)) + "))", nil, nil
	case fieldKindBool:
		return knowledgeNullableStoredTypeSQL(boundValueSQL, eventfields.StoredValueTypeBool), nil, nil
	case fieldKindTime:
		return knowledgeNullableStoredTypeSQL(boundValueSQL, eventfields.StoredValueTypeTimestamp), nil, nil
	case fieldKindStringArray:
		return knowledgeNullableStoredTypeSQL(boundValueSQL, eventfields.StoredValueTypeList), nil, nil
	case fieldKindNumber:
		code := eventfields.StoredValueTypeSint64
		if strings.HasPrefix(value.numberType, "UInt") {
			code = eventfields.StoredValueTypeUint64
		} else if strings.HasPrefix(value.numberType, "Float") ||
			strings.HasPrefix(value.numberType, "Decimal") || value.numberType == "" {
			code = eventfields.StoredValueTypeDouble
		}
		return knowledgeNullableStoredTypeSQL(boundValueSQL, code), nil, nil
	case fieldKindDynamic:
	default:
		return "", nil, fmt.Errorf(
			"compile ClickHouse knowledge field assignment semantic type: unsupported scalar kind %d",
			value.kind,
		)
	}

	field := knowledgeFieldStateFromScalar(value)
	if value.storedTypeSQL != "" || len(value.existsArgs) > 0 || len(value.descendantArgs) > 0 {
		typeSQL, args, err := knownFieldStoredTypeSQL(field)
		if err == nil {
			return typeSQL, args, nil
		}
	}
	return knowledgeDynamicStoredTypeSQL(boundValueSQL), nil, nil
}

func knowledgeFieldStateFromScalar(value compiledScalar) fieldState {
	return fieldState{
		valueSQL:                value.valueSQL,
		maxStringBytes:          value.maxStringBytes,
		textEligibleSQL:         value.textEligibleSQL,
		dynamicDomain:           value.dynamicDomain,
		numericIntegral:         value.numericIntegral,
		mvCountOneOrNull:        value.mvCountOneOrNull,
		dynamicTypeSQL:          value.dynamicTypeSQL,
		storedTypeSQL:           value.storedTypeSQL,
		existsSQL:               value.existsSQL,
		existsArgs:              slices.Clone(value.existsArgs),
		descendantSQL:           value.descendantSQL,
		descendantArgs:          slices.Clone(value.descendantArgs),
		storedPath:              value.storedPath.clone(),
		kind:                    value.kind,
		numberType:              value.numberType,
		alwaysNull:              value.alwaysNull,
		materializeForPredicate: value.materializeForPredicate,
	}
}

func knowledgeNullableStoredTypeSQL(
	valueSQL string,
	typeCode eventfields.StoredValueType,
) string {
	return "if(dynamicType(" + valueSQL + ") = 'None', toUInt8(" +
		strconv.Itoa(int(eventfields.StoredValueTypeNull)) + "), toUInt8(" +
		strconv.Itoa(int(typeCode)) + "))"
}

// knowledgeDynamicStoredTypeSQL classifies compiler-produced Dynamic values
// that no longer have an exact stored metadata path. Direct event fields take
// the authoritative knownFieldStoredTypeSQL path above; this fallback is for
// typed calculated producers only.
func knowledgeDynamicStoredTypeSQL(valueSQL string) string {
	typeSQL := "dynamicType(" + valueSQL + ")"
	dynamic := compiledScalar{valueSQL: valueSQL, dynamicTypeSQL: typeSQL, kind: fieldKindDynamic}
	code := func(value eventfields.StoredValueType) string {
		return "toUInt8(" + strconv.Itoa(int(value)) + ")"
	}
	stringValue := "dynamicElement(" + valueSQL + ", 'String')"
	return "multiIf(" +
		typeSQL + " = 'None', " + code(eventfields.StoredValueTypeNull) + ", " +
		dynamicTaggedEnvelopeCondition(dynamic, "bytes/v1") + ", " + code(eventfields.StoredValueTypeBytes) + ", " +
		dynamicTaggedEnvelopeCondition(dynamic, "timestamp/v1") + ", " + code(eventfields.StoredValueTypeTimestamp) + ", " +
		dynamicTaggedEnvelopeCondition(dynamic, "duration/v1") + ", " + code(eventfields.StoredValueTypeDuration) + ", " +
		dynamicTaggedEnvelopeCondition(dynamic, "decimal/v1") + ", " + code(eventfields.StoredValueTypeDecimal) + ", " +
		typeSQL + " = 'String' AND isValidUTF8(" + stringValue + "), " + code(eventfields.StoredValueTypeString) + ", " +
		typeSQL + " = 'String', " + code(eventfields.StoredValueTypeBytes) + ", " +
		"startsWith(" + typeSQL + ", 'Int'), " + code(eventfields.StoredValueTypeSint64) + ", " +
		"startsWith(" + typeSQL + ", 'UInt'), " + code(eventfields.StoredValueTypeUint64) + ", " +
		"startsWith(" + typeSQL + ", 'Float'), " + code(eventfields.StoredValueTypeDouble) + ", " +
		"startsWith(" + typeSQL + ", 'Decimal'), " + code(eventfields.StoredValueTypeDecimal) + ", " +
		typeSQL + " = 'Bool', " + code(eventfields.StoredValueTypeBool) + ", " +
		"startsWith(" + typeSQL + ", 'Date') OR startsWith(" + typeSQL + ", 'DateTime'), " + code(eventfields.StoredValueTypeTimestamp) + ", " +
		"startsWith(" + typeSQL + ", 'Interval'), " + code(eventfields.StoredValueTypeDuration) + ", " +
		"startsWith(" + typeSQL + ", 'Array'), " + code(eventfields.StoredValueTypeList) + ", " +
		"startsWith(" + typeSQL + ", 'Map') OR startsWith(" + typeSQL + ", 'Tuple'), " + code(eventfields.StoredValueTypeObject) + ", " +
		"toUInt8(0))"
}

// compileKnowledgeAliasStage emits every alias assignment in one projection.
func compileKnowledgeAliasStage(
	state compileState,
	assignments []knowledgeprogram.Alias,
	stage int,
	priorCharges compiledKnowledgeSelectorChargeColumns,
) (compiledKnowledgeFusedFieldProjection, error) {
	if len(assignments) > knowledgeprogram.MaximumObjects {
		return compiledKnowledgeFusedFieldProjection{}, errors.New(
			"compile ClickHouse knowledge alias stage: too many assignments",
		)
	}
	compiled := make([]compiledKnowledgeFieldAssignment, 0, len(assignments))
	for _, assignment := range assignments {
		lowered, err := compileKnowledgeAliasAssignment(assignment, state)
		if err != nil {
			return compiledKnowledgeFusedFieldProjection{}, err
		}
		compiled = append(compiled, lowered)
	}
	result, err := compileKnowledgeFusedFieldProjection(
		state,
		compiled,
		stage,
		"alias",
		priorCharges,
	)
	if err != nil {
		return compiledKnowledgeFusedFieldProjection{}, err
	}
	return result, nil
}

// compileKnowledgeCalculatedStage emits every calculated assignment in one
// projection after compiling all expressions against the same frozen state.
func compileKnowledgeCalculatedStage(
	state compileState,
	assignments []knowledgeprogram.Calculated,
	stage int,
	priorCharges compiledKnowledgeSelectorChargeColumns,
) (compiledKnowledgeFusedFieldProjection, error) {
	if len(assignments) > knowledgeprogram.MaximumScalarExpressions {
		return compiledKnowledgeFusedFieldProjection{}, errors.New(
			"compile ClickHouse knowledge calculated stage: too many assignments",
		)
	}
	compiled := make([]compiledKnowledgeFieldAssignment, 0, len(assignments))
	for _, assignment := range assignments {
		lowered, err := compileKnowledgeCalculatedAssignment(assignment, state)
		if err != nil {
			return compiledKnowledgeFusedFieldProjection{}, err
		}
		compiled = append(compiled, lowered)
	}
	result, err := compileKnowledgeFusedFieldProjection(
		state,
		compiled,
		stage,
		"calculated",
		priorCharges,
	)
	if err != nil {
		return compiledKnowledgeFusedFieldProjection{}, err
	}
	return result, nil
}

type compiledKnowledgeFieldMerge struct {
	assignment            compiledKnowledgeFieldAssignment
	bindingAlias          string
	bindingSQL            string
	args                  []any
	writeSQL              string
	writtenValueSQL       string
	writtenTypeSQL        string
	previousValueSQL      string
	previousPresentSQL    string
	previousTypeSQL       string
	previousDescendantSQL string
	maxStringBytes        uint64
	stage                 int
	index                 int
}

type compiledKnowledgeFieldDestinationMerge struct {
	destination          plan.FieldRef
	candidates           []compiledKnowledgeFieldMerge
	outputValueSQL       string
	existsAlias          string
	existsProjection     string
	typeAlias            string
	typeProjection       string
	descendantAlias      string
	descendantProjection string
	maxStringBytes       uint64
}

func compileKnowledgeFusedFieldProjection(
	state compileState,
	assignments []compiledKnowledgeFieldAssignment,
	stage int,
	label string,
	priorCharges compiledKnowledgeSelectorChargeColumns,
) (compiledKnowledgeFusedFieldProjection, error) {
	if stage < 0 || label == "" {
		return compiledKnowledgeFusedFieldProjection{}, errors.New(
			"compile ClickHouse knowledge fused field stage: invalid stage identity",
		)
	}
	if len(assignments) == 0 {
		return compiledKnowledgeFusedFieldProjection{}, errors.New(
			"compile ClickHouse knowledge fused field stage: no assignments",
		)
	}
	if err := validateKnowledgePriorSelectorCharges(priorCharges); err != nil {
		return compiledKnowledgeFusedFieldProjection{}, err
	}

	next := cloneCompileState(state)
	if exposesRawFieldsPayload(state) {
		dropRawFieldsPayload(&next)
	}
	merges := make([]compiledKnowledgeFieldMerge, 0, len(assignments))
	aliases := make([]knowledgeprogram.Alias, 0, len(assignments))
	calculated := make([]knowledgeprogram.Calculated, 0, len(assignments))
	groups := make([]compiledKnowledgeFieldDestinationMerge, 0, len(assignments))
	groupByDestination := make(map[string]int, len(assignments))
	for index, assignment := range assignments {
		switch label {
		case "alias":
			if !knowledgeAliasLoweringProofMatches(assignment) {
				return compiledKnowledgeFusedFieldProjection{}, errors.New(
					"compile ClickHouse knowledge fused field stage: alias lowering proof is invalid",
				)
			}
			aliases = append(aliases, assignment.alias)
		case "calculated":
			if !knowledgeCalculatedLoweringProofMatches(assignment) {
				return compiledKnowledgeFusedFieldProjection{}, errors.New(
					"compile ClickHouse knowledge fused field stage: calculated lowering proof is invalid",
				)
			}
			calculated = append(calculated, assignment.calculated)
		default:
			return compiledKnowledgeFusedFieldProjection{}, errors.New(
				"compile ClickHouse knowledge fused field stage: lowering proof kind is invalid",
			)
		}
		if assignment.origin.StageOrdinal() != uint32(index) {
			return compiledKnowledgeFusedFieldProjection{}, errors.New(
				"compile ClickHouse knowledge fused field stage: assignment order disagrees with provenance",
			)
		}
		name := assignment.destination.Name
		merge, err := compileKnowledgeFieldMerge(assignment, state, stage, index)
		if err != nil {
			return compiledKnowledgeFusedFieldProjection{}, err
		}
		merges = append(merges, merge)
		groupIndex, grouped := groupByDestination[name]
		if !grouped {
			groupIndex = len(groups)
			groupByDestination[name] = groupIndex
			groups = append(groups, compiledKnowledgeFieldDestinationMerge{
				destination: assignment.destination,
			})
		}
		for _, existing := range groups[groupIndex].candidates {
			if !existing.assignment.selector.ProvablyDisjoint(merge.assignment.selector) {
				return compiledKnowledgeFusedFieldProjection{}, fmt.Errorf(
					"compile ClickHouse knowledge fused field stage: repeated destination %q is not provably disjoint",
					name,
				)
			}
		}
		groups[groupIndex].candidates = append(groups[groupIndex].candidates, merge)
	}
	for index := range groups {
		if err := finalizeKnowledgeFieldDestinationMerge(&groups[index]); err != nil {
			return compiledKnowledgeFusedFieldProjection{}, err
		}

		name := groups[index].destination.Name
		delete(next.blocked, name)
		if !slices.Contains(next.publicOrder, name) {
			next.publicOrder = append(next.publicOrder, name)
		}
		output := quoteIdentifier(name)
		next.visible[name] = fieldState{
			valueSQL:                output,
			maxStringBytes:          groups[index].maxStringBytes,
			textEligibleSQL:         groups[index].typeAlias + " = toUInt8(" + strconv.Itoa(int(eventfields.StoredValueTypeString)) + ")",
			dynamicTypeSQL:          "dynamicType(" + output + ")",
			storedTypeSQL:           groups[index].typeAlias,
			existsSQL:               groups[index].existsAlias,
			descendantSQL:           groups[index].descendantAlias,
			kind:                    fieldKindDynamic,
			caseSensitive:           false,
			materializeForPredicate: true,
		}
	}

	liveOldPrivateColumns := livePrivateColumns(state.privateColumns, next.visible)
	next.privateColumns = append([]string(nil), liveOldPrivateColumns...)
	for _, merge := range groups {
		next.privateColumns = append(
			next.privateColumns,
			merge.existsAlias,
			merge.typeAlias,
			merge.descendantAlias,
		)
	}
	chargeColumns := compiledKnowledgeSelectorChargeColumns{
		inputBytes: quoteIdentifier(fmt.Sprintf("__os_ko_selector_input_bytes_%d", stage)),
		queryUnits: quoteIdentifier(fmt.Sprintf("__os_ko_selector_query_units_%d", stage)),
	}
	bindingProjection := make([]string, 0, len(state.visible)+len(merges)+12)
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
	for _, merge := range merges {
		bindingProjection = append(bindingProjection, merge.bindingAlias)
	}

	projection := make([]string, 0, len(next.visible)+len(groups)*3+12)
	mergeByDestination := make(map[string]compiledKnowledgeFieldDestinationMerge, len(groups))
	for _, merge := range groups {
		mergeByDestination[merge.destination.Name] = merge
	}
	for _, name := range orderedVisibleNames(next) {
		publicName := quoteIdentifier(name)
		if merge, generated := mergeByDestination[name]; generated {
			projection = append(projection, merge.outputValueSQL+" AS "+publicName)
			continue
		}
		if _, ok := state.visible[name]; !ok {
			return compiledKnowledgeFusedFieldProjection{}, fmt.Errorf(
				"compile ClickHouse knowledge fused field stage: field %q has no frozen input value",
				name,
			)
		}
		projection = append(projection, publicName)
	}
	projectionState := next
	projectionState.privateColumns = liveOldPrivateColumns
	projection = appendPrivateEventProjection(projection, projectionState)
	for _, merge := range groups {
		projection = append(
			projection,
			merge.existsProjection,
			merge.typeProjection,
			merge.descendantProjection,
		)
	}
	inputCharges := make([]string, 0, len(merges)+1)
	queryCharges := make([]string, 0, len(merges)+1)
	if priorCharges.inputBytes != "" {
		inputCharges = append(inputCharges, "toUInt128("+priorCharges.inputBytes+")")
		queryCharges = append(queryCharges, "toUInt128("+priorCharges.queryUnits+")")
	}
	bindings := make([]string, 0, len(merges))
	args := make([]any, 0)
	for _, merge := range merges {
		assignmentResult := knowledgeTupleElement(
			merge.bindingAlias,
			knowledgeFieldBindingAssignmentElement,
		)
		inputCharges = append(inputCharges, merge.assignment.selectorInputBytesSQL(assignmentResult))
		queryCharges = append(queryCharges, merge.assignment.selectorQueryUnitsSQL(assignmentResult))
		bindings = append(bindings, "["+merge.bindingSQL+"] AS "+merge.bindingAlias)
		args = append(args, merge.args...)
	}
	projection = append(
		projection,
		knowledgeUInt128Sum(inputCharges)+" AS "+chargeColumns.inputBytes,
		knowledgeUInt128Sum(queryCharges)+" AS "+chargeColumns.queryUnits,
	)
	return compiledKnowledgeFusedFieldProjection{
		bindingProjection:  bindingProjection,
		projection:         projection,
		arrayJoinBindings:  bindings,
		state:              next,
		suffixArgs:         args,
		selectorCharges:    chargeColumns,
		emittedAssignments: uint32(len(assignments)),
		aliases:            aliases,
		calculated:         calculated,
	}, nil
}

func knowledgeAliasLoweringProofMatches(assignment compiledKnowledgeFieldAssignment) bool {
	proof := assignment.alias
	return proof.Origin() != (knowledgeprogram.Origin{}) &&
		assignment.calculated.Origin() == (knowledgeprogram.Origin{}) &&
		proof.Origin() == assignment.origin &&
		proof.Overwrite() == assignment.overwrite &&
		proof.Destination() == assignment.destination.Name &&
		slices.Equal(proof.Selector().CanonicalBytes(), assignment.selector.CanonicalBytes())
}

func knowledgeCalculatedLoweringProofMatches(assignment compiledKnowledgeFieldAssignment) bool {
	proof := assignment.calculated
	return proof.Origin() != (knowledgeprogram.Origin{}) &&
		assignment.alias.Origin() == (knowledgeprogram.Origin{}) &&
		proof.Origin() == assignment.origin &&
		proof.Overwrite() == assignment.overwrite &&
		proof.Destination() == assignment.destination.Name &&
		slices.Equal(proof.Selector().CanonicalBytes(), assignment.selector.CanonicalBytes())
}

func validateKnowledgePriorSelectorCharges(
	charges compiledKnowledgeSelectorChargeColumns,
) error {
	if charges.inputBytes == "" && charges.queryUnits == "" {
		return nil
	}
	if charges.inputBytes == "" || charges.queryUnits == "" ||
		charges.inputBytes == charges.queryUnits {
		return errors.New(
			"compile ClickHouse knowledge fused field stage: prior selector charges are invalid",
		)
	}
	return nil
}

func finalizeKnowledgeFieldDestinationMerge(
	group *compiledKnowledgeFieldDestinationMerge,
) error {
	if group == nil || group.destination.Name == "" || len(group.candidates) == 0 {
		return errors.New(
			"compile ClickHouse knowledge fused field stage: destination group is invalid",
		)
	}
	first := group.candidates[0]
	value := first.previousValueSQL
	present := first.previousPresentSQL
	storedType := first.previousTypeSQL
	descendant := first.previousDescendantSQL
	for index := len(group.candidates) - 1; index >= 0; index-- {
		candidate := group.candidates[index]
		value = "if(" + candidate.writeSQL + ", " + candidate.writtenValueSQL + ", " + value + ")"
		present = "toUInt8(if(" + candidate.writeSQL + ", 1, " + present + "))"
		storedType = "toUInt8(if(" + candidate.writeSQL + ", " + candidate.writtenTypeSQL +
			", " + storedType + "))"
		descendant = "toUInt8(if(" + candidate.writeSQL + ", 0, " + descendant + "))"
		group.maxStringBytes = max(group.maxStringBytes, candidate.maxStringBytes)
	}
	group.outputValueSQL = value
	group.existsAlias = quoteIdentifier(fmt.Sprintf(
		"__os_ko_field_exists_%d_%d",
		first.stage,
		first.index,
	))
	group.typeAlias = quoteIdentifier(fmt.Sprintf(
		"__os_ko_field_type_%d_%d",
		first.stage,
		first.index,
	))
	group.descendantAlias = quoteIdentifier(fmt.Sprintf(
		"__os_ko_field_descendant_%d_%d",
		first.stage,
		first.index,
	))
	group.existsProjection = present + " AS " + group.existsAlias
	group.typeProjection = storedType + " AS " + group.typeAlias
	group.descendantProjection = descendant + " AS " + group.descendantAlias
	return nil
}

func compileKnowledgeFieldMerge(
	assignment compiledKnowledgeFieldAssignment,
	state compileState,
	stage int,
	index int,
) (compiledKnowledgeFieldMerge, error) {
	previous, previousKnown, err := resolveCompiledField(assignment.destination, state)
	if err != nil {
		return compiledKnowledgeFieldMerge{}, err
	}
	previousValue := "CAST(NULL AS Dynamic)"
	previousPresent := "toUInt8(0)"
	previousType := "toUInt8(0)"
	previousDescendant := "toUInt8(0)"
	args := append([]any(nil), assignment.args...)
	maxStringBytes := assignment.maxStringBytes
	if previousKnown {
		previousValue = "CAST(" + previous.valueSQL + " AS Dynamic)"
		var presenceArgs, typeArgs []any
		previousPresent, presenceArgs = knownFieldPresenceSQL(previous)
		previousType, typeArgs, err = knownFieldStoredTypeSQL(previous)
		if err != nil {
			return compiledKnowledgeFieldMerge{}, fmt.Errorf(
				"compile ClickHouse knowledge field destination type for %q: %w",
				assignment.destination.Name,
				err,
			)
		}
		args = append(args, presenceArgs...)
		args = append(args, typeArgs...)
		if previous.descendantSQL != "" {
			previousDescendant = previous.descendantSQL
			args = append(args, previous.descendantArgs...)
		}
		maxStringBytes = max(maxStringBytes, fieldStateStringByteBound(previous))
	}
	assignment.maxStringBytes = maxStringBytes

	bindingAlias := quoteIdentifier(fmt.Sprintf("__os_ko_field_binding_%d_%d", stage, index))
	bindingSQL := "tuple(" + assignment.sql + ", " + previousValue + ", " +
		"toUInt8(ifNull(" + previousPresent + ", 0)), toUInt8(" + previousType + "), " +
		"toUInt8(ifNull(" + previousDescendant + ", 0)))"
	assignmentResult := knowledgeTupleElement(bindingAlias, knowledgeFieldBindingAssignmentElement)
	produced := assignment.producedSQL(assignmentResult) + " != 0"
	previousPresentBound := knowledgeTupleElementUInt8(
		bindingAlias,
		knowledgeFieldBindingPreviousPresentElement,
	)
	write := produced
	if assignment.overwrite == knowledgeprogram.PreserveExisting {
		write += " AND " + previousPresentBound + " = 0"
	}
	return compiledKnowledgeFieldMerge{
		assignment:            assignment,
		bindingAlias:          bindingAlias,
		bindingSQL:            bindingSQL,
		args:                  args,
		writeSQL:              write,
		writtenValueSQL:       assignment.valueSQL(assignmentResult),
		writtenTypeSQL:        assignment.storedTypeSQL(assignmentResult),
		previousValueSQL:      knowledgeTupleElement(bindingAlias, knowledgeFieldBindingPreviousValueElement),
		previousPresentSQL:    previousPresentBound,
		previousTypeSQL:       knowledgeTupleElementUInt8(bindingAlias, knowledgeFieldBindingPreviousStoredTypeElement),
		previousDescendantSQL: knowledgeTupleElementUInt8(bindingAlias, knowledgeFieldBindingPreviousDescendantElement),
		maxStringBytes:        maxStringBytes,
		stage:                 stage,
		index:                 index,
	}, nil
}

func knowledgeTupleElement(tupleSQL string, element int) string {
	return "tupleElement(" + tupleSQL + ", " + strconv.Itoa(element) + ")"
}

func knowledgeTupleElementUInt8(tupleSQL string, element int) string {
	return "toUInt8(" + knowledgeTupleElement(tupleSQL, element) + ")"
}

func knowledgeTupleElementUInt128(tupleSQL string, element int) string {
	return "toUInt128(" + knowledgeTupleElement(tupleSQL, element) + ")"
}

func knowledgeUInt128Sum(expressions []string) string {
	if len(expressions) == 0 {
		return "toUInt128(0)"
	}
	return strings.Join(expressions, " + ")
}

func validateKnowledgeAliasOperation(operation knowledgeprogram.Alias) error {
	if err := validateKnowledgeFieldOrigin(
		operation.Origin(),
		opensplunkv1.KnowledgeObjectType_KNOWLEDGE_OBJECT_TYPE_FIELD_ALIAS,
		opensplunkv1.KnowledgeSearchStage_KNOWLEDGE_SEARCH_STAGE_FIELD_ALIAS,
		"field_alias.destination_field",
	); err != nil {
		return fmt.Errorf("compile ClickHouse knowledge alias: %w", err)
	}
	if operation.Source() == "" || operation.Destination() == "" ||
		operation.Source() == operation.Destination() {
		return errors.New("compile ClickHouse knowledge alias: fields are invalid")
	}
	return validateKnowledgeOverwrite(operation.Overwrite(), "alias")
}

func validateKnowledgeCalculatedOperation(operation knowledgeprogram.Calculated) error {
	if err := validateKnowledgeFieldOrigin(
		operation.Origin(),
		opensplunkv1.KnowledgeObjectType_KNOWLEDGE_OBJECT_TYPE_CALCULATED_FIELD,
		opensplunkv1.KnowledgeSearchStage_KNOWLEDGE_SEARCH_STAGE_CALCULATED_FIELD,
		"calculated_field.expression",
	); err != nil {
		return fmt.Errorf("compile ClickHouse knowledge calculated field: %w", err)
	}
	if operation.Destination() == "" || operation.Expression() == "" || operation.Nodes() == 0 {
		return errors.New("compile ClickHouse knowledge calculated field: operation is invalid")
	}
	return validateKnowledgeOverwrite(operation.Overwrite(), "calculated field")
}

func validateKnowledgeFieldOrigin(
	origin knowledgeprogram.Origin,
	objectType opensplunkv1.KnowledgeObjectType,
	stage opensplunkv1.KnowledgeSearchStage,
	location string,
) error {
	if origin.ObjectType() != objectType || origin.Stage() != stage ||
		origin.Version() == 0 || origin.ObjectID() == "" || origin.Name() == "" ||
		origin.AppID() == "" || origin.OwnerID() == "" ||
		origin.DefinitionLocation() != location {
		return errors.New("object provenance is invalid")
	}
	switch origin.SharingScope() {
	case opensplunkv1.SharingScope_SHARING_SCOPE_PRIVATE,
		opensplunkv1.SharingScope_SHARING_SCOPE_APP,
		opensplunkv1.SharingScope_SHARING_SCOPE_GLOBAL:
		return nil
	default:
		return errors.New("sharing provenance is invalid")
	}
}

func validateKnowledgeOverwrite(
	overwrite knowledgeprogram.OverwriteBehavior,
	label string,
) error {
	switch overwrite {
	case knowledgeprogram.PreserveExisting, knowledgeprogram.ReplaceExisting:
		return nil
	default:
		return fmt.Errorf(
			"compile ClickHouse knowledge %s: overwrite authority is invalid",
			label,
		)
	}
}
