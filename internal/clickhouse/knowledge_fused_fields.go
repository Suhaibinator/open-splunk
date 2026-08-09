package clickhouse

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"hash"
	"reflect"
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
	compiledKnowledgeFieldInputStateDomain = "open-splunk/clickhouse/knowledge-field-input-state/v1"

	// A field candidate keeps the canonical six-element source tuple separate
	// from its selector charges. The nested source tuple distinguishes missing
	// from present Dynamic null without conflating either with overwrite state.
	knowledgeFieldAssignmentSourceElement             = 1
	knowledgeFieldAssignmentSelectorInputBytesElement = 2
	knowledgeFieldAssignmentSelectorQueryUnitsElement = 3

	knowledgeFieldBindingSourceElement             = 1
	knowledgeFieldBindingWroteElement              = 2
	knowledgeFieldBindingSelectorInputBytesElement = 3
	knowledgeFieldBindingSelectorQueryUnitsElement = 4

	maxCompiledKnowledgeFieldAssignmentSQLBytes = 64 << 10
)

func compileKnowledgeFieldInputStateAuthority(
	state compileState,
	inputFields []string,
) ([sha256.Size]byte, error) {
	if len(inputFields) > knowledgeprogram.MaximumGeneratedFields {
		return [sha256.Size]byte{}, errors.New(
			"compile ClickHouse knowledge field input authority: too many inputs",
		)
	}
	digest := sha256.New()
	writeTokenPart(digest, compiledKnowledgeFieldInputStateDomain)
	if state.context == nil {
		writeBool(digest, false)
	} else {
		writeBool(digest, true)
		writeInt64(digest, state.context.searchStartUnix)
		writeTokenPart(digest, state.context.searchTimezone)
	}
	writeUint64(digest, uint64(len(inputFields)))
	for _, name := range inputFields {
		writeTokenPart(digest, name)
		fieldRef, err := plan.ResolveField(name, spl.Range{})
		if err != nil {
			return [sha256.Size]byte{}, fmt.Errorf(
				"compile ClickHouse knowledge field input authority for %q: %w",
				name,
				err,
			)
		}
		field, present, err := resolveCompiledField(fieldRef, state)
		if err != nil {
			return [sha256.Size]byte{}, err
		}
		writeBool(digest, present)
		if present && !writeKnowledgeFieldStateAuthority(digest, field) {
			return [sha256.Size]byte{}, errors.New(
				"compile ClickHouse knowledge field input authority: unsupported field argument",
			)
		}
	}
	var authority [sha256.Size]byte
	digest.Sum(authority[:0])
	return authority, nil
}

func writeKnowledgeFieldStateAuthority(writer hash.Hash, field fieldState) bool {
	writeTokenPart(writer, field.valueSQL)
	writeTokenPart(writer, field.exactNumericKeySQL)
	writeTokenPart(writer, field.dynamicNumericEligibleSQL)
	writeUint64(writer, field.maxStringBytes)
	writeTokenPart(writer, field.textEligibleSQL)
	writeBool(writer, field.rawTextIndexEligible)
	writeUint64(writer, uint64(field.dynamicDomain))
	writeBool(writer, field.numericIntegral)
	writeBool(writer, field.mvCountOneOrNull)
	writeTokenPart(writer, field.dynamicTypeSQL)
	writeTokenPart(writer, field.storedTypeSQL)
	writeTokenPart(writer, field.existsSQL)
	if !writeKnowledgeFieldStateArguments(writer, field.existsArgs) {
		return false
	}
	writeTokenPart(writer, field.descendantSQL)
	if !writeKnowledgeFieldStateArguments(writer, field.descendantArgs) {
		return false
	}
	writeStringSlice(writer, field.storedPath.logicalSegments)
	writeTokenPart(writer, field.storedPath.normalizedExactPath)
	writeTokenPart(writer, field.storedPath.normalizedDescendantPrefix)
	writeStringSlice(writer, field.storedPath.physicalSegments)
	writeTokenPart(writer, field.relativeFieldNamesSQL)
	writeTokenPart(writer, field.relativeFieldTypesSQL)
	writeTokenPart(writer, field.fieldMetadataVersionSQL)
	writeUint64(writer, uint64(field.kind))
	writeBool(writer, field.caseSensitive)
	writeTokenPart(writer, field.numberType)
	writeBool(writer, field.numericSort)
	writeBool(writer, field.canonicalTime)
	writeBool(writer, field.alwaysNull)
	writeBool(writer, field.materializeForPredicate)
	return true
}

func writeKnowledgeFieldStateArguments(writer hash.Hash, arguments []any) bool {
	writeBool(writer, arguments == nil)
	writeUint64(writer, uint64(len(arguments)))
	for _, argument := range arguments {
		if !writeCompiledArgument(writer, argument, 0) {
			return false
		}
	}
	return true
}

// compiledKnowledgeFieldAssignment is one selector-guarded, row-local
// assignment. The tuple is independent of destination overwrite state, so a
// fused stage can compile every assignment against exactly the same frozen
// input and publish all destinations atomically.
type compiledKnowledgeFieldAssignment struct {
	selectorSQL compiledKnowledgeSelector
	destination plan.FieldRef
	selector    knowledgeprogram.Selector
	overwrite   knowledgeprogram.OverwriteBehavior
	origin      knowledgeprogram.Origin
	source      compiledKnowledgeFieldSource
	alias       knowledgeprogram.Alias
	calculated  knowledgeprogram.Calculated
}

func (compiled compiledKnowledgeFieldAssignment) producedSQL(resultSQL string) string {
	return compiled.source.producedSQL(compiled.sourceResultSQL(resultSQL))
}

func (compiled compiledKnowledgeFieldAssignment) sourceResultSQL(resultSQL string) string {
	return knowledgeTupleElement(resultSQL, knowledgeFieldAssignmentSourceElement)
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
// frozen extraction-stage state. Exact leaves retain their Dynamic value;
// flattened object parents retain a lazy materializer plus relative metadata
// sidecars. The central nonempty-finalization gate remains closed until result
// transport and runtime copy accounting consume that authority end to end.
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
	sourceValue, err := compileKnowledgeFieldSourceFromScalar(value, true)
	if err != nil {
		return compiledKnowledgeFieldAssignment{}, err
	}
	inputStateAuthority, err := compileKnowledgeFieldInputStateAuthority(
		state,
		[]string{operation.Source()},
	)
	if err != nil {
		return compiledKnowledgeFieldAssignment{}, err
	}
	maxStringBytes := compiledScalarStringByteBound(value)
	sourceValue, err = authorizeCompiledKnowledgeFieldSource(
		sourceValue,
		"field:"+operation.Source(),
		inputStateAuthority,
		maxStringBytes,
	)
	if err != nil {
		return compiledKnowledgeFieldAssignment{}, err
	}
	compiled, err := compileKnowledgeFieldAssignment(
		operation.Selector(),
		operation.Destination(),
		operation.Overwrite(),
		operation.Origin(),
		sourceValue,
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
	_, directField := expression.(*plan.ScalarFieldExpression)
	sourceValue, err := compileKnowledgeFieldSourceFromScalar(value, directField)
	if err != nil {
		return compiledKnowledgeFieldAssignment{}, err
	}
	inputStateAuthority, err := compileKnowledgeFieldInputStateAuthority(
		state,
		operation.InputFields(),
	)
	if err != nil {
		return compiledKnowledgeFieldAssignment{}, err
	}
	maxStringBytes := compiledScalarStringByteBound(value)
	sourceValue, err = authorizeCompiledKnowledgeFieldSource(
		sourceValue,
		"expression:"+operation.Expression(),
		inputStateAuthority,
		maxStringBytes,
	)
	if err != nil {
		return compiledKnowledgeFieldAssignment{}, err
	}
	compiled, err := compileKnowledgeFieldAssignment(
		operation.Selector(),
		operation.Destination(),
		operation.Overwrite(),
		operation.Origin(),
		sourceValue,
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
	source compiledKnowledgeFieldSource,
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

	return compiledKnowledgeFieldAssignment{
		selectorSQL: selector,
		destination: destination,
		selector:    selectorAuthority,
		overwrite:   overwrite,
		origin:      origin,
		source:      source,
	}, nil
}

type compiledKnowledgeFieldCandidate struct {
	sql  string
	args []any
}

// compileKnowledgeFieldCandidate keeps the expensive source tuple behind both
// selector and overwrite eligibility. The returned expression is always
// evaluated so its selector charges remain authoritative, but selector-false
// and PreserveExisting-blocked rows return the typed missing tuple without
// evaluating a calculated value or stored-container materializer.
func compileKnowledgeFieldCandidate(
	assignment compiledKnowledgeFieldAssignment,
	previous compiledKnowledgeFieldSource,
) (compiledKnowledgeFieldCandidate, error) {
	if assignment.selectorSQL.sql == "" || assignment.source.sql == "" {
		return compiledKnowledgeFieldCandidate{}, errors.New(
			"compile ClickHouse knowledge field candidate: authority is incomplete",
		)
	}

	selectedSource := assignment.source.sql
	args := make([]any, 0,
		len(assignment.source.args)+len(previous.presenceArgs)+
			len(assignment.selectorSQL.args),
	)
	args = append(args, assignment.source.args...)
	if assignment.overwrite == knowledgeprogram.PreserveExisting {
		const previousPresentVariable = "__os_ko_field_previous_present"
		selectedSource = "if(toUInt8(ifNull(" + previousPresentVariable +
			", 0)) = 0, " + selectedSource + ", " +
			knowledgeMissingFieldSourceSQL() + ")"
		selectedSource = bindSQLExpressions(
			[]string{previousPresentVariable},
			[]string{previous.presenceSQL},
			selectedSource,
		)
		args = append(args, previous.presenceArgs...)
	}

	const selectorVariable = "__os_ko_field_selector"
	result := "tuple(if(tupleElement(" + selectorVariable + ", 1) != 0, " +
		selectedSource + ", " + knowledgeMissingFieldSourceSQL() + "), " +
		"toUInt128(tupleElement(" + selectorVariable + ", 2)), " +
		"toUInt128(tupleElement(" + selectorVariable + ", 3)))"
	result = bindSQLExpressions(
		[]string{selectorVariable},
		[]string{assignment.selectorSQL.sql},
		result,
	)
	args = append(args, assignment.selectorSQL.args...)
	if len(result) > maxCompiledKnowledgeFieldAssignmentSQLBytes ||
		strings.Count(result, "?") != len(args) {
		return compiledKnowledgeFieldCandidate{}, errors.New(
			"compile ClickHouse knowledge field candidate: generated SQL or arguments are invalid",
		)
	}
	return compiledKnowledgeFieldCandidate{sql: result, args: args}, nil
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
		relativeFieldNamesSQL:   value.relativeFieldNamesSQL,
		relativeFieldTypesSQL:   value.relativeFieldTypesSQL,
		fieldMetadataVersionSQL: value.fieldMetadataVersionSQL,
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

type compiledKnowledgeFieldDestinationMerge struct {
	destination          plan.FieldRef
	candidates           []compiledKnowledgeFieldAssignment
	bindingAlias         string
	bindingSQL           string
	args                 []any
	outputValueSQL       string
	existsAlias          string
	existsProjection     string
	typeAlias            string
	typeProjection       string
	descendantAlias      string
	descendantProjection string
	namesAlias           string
	namesProjection      string
	typesAlias           string
	typesProjection      string
	metadataAlias        string
	metadataProjection   string
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
	aliases := make([]knowledgeprogram.Alias, 0, len(assignments))
	calculated := make([]knowledgeprogram.Calculated, 0, len(assignments))
	groups := make([]compiledKnowledgeFieldDestinationMerge, 0, len(assignments))
	groupByDestination := make(map[string]int, len(assignments))
	for index, assignment := range assignments {
		switch label {
		case "alias":
			if !knowledgeAliasLoweringProofMatches(assignment, state) {
				return compiledKnowledgeFusedFieldProjection{}, errors.New(
					"compile ClickHouse knowledge fused field stage: alias lowering proof is invalid",
				)
			}
			aliases = append(aliases, assignment.alias)
		case "calculated":
			if !knowledgeCalculatedLoweringProofMatches(assignment, state) {
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
		groupIndex, grouped := groupByDestination[name]
		if !grouped {
			groupIndex = len(groups)
			groupByDestination[name] = groupIndex
			groups = append(groups, compiledKnowledgeFieldDestinationMerge{
				destination: assignment.destination,
			})
		}
		for _, existing := range groups[groupIndex].candidates {
			if !existing.selector.ProvablyDisjoint(assignment.selector) {
				return compiledKnowledgeFusedFieldProjection{}, fmt.Errorf(
					"compile ClickHouse knowledge fused field stage: repeated destination %q is not provably disjoint",
					name,
				)
			}
		}
		groups[groupIndex].candidates = append(groups[groupIndex].candidates, assignment)
	}
	for index := range groups {
		if err := compileKnowledgeFieldDestinationMerge(
			&groups[index],
			state,
			stage,
			index,
		); err != nil {
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
			relativeFieldNamesSQL:   groups[index].namesAlias,
			relativeFieldTypesSQL:   groups[index].typesAlias,
			fieldMetadataVersionSQL: groups[index].metadataAlias,
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
			merge.namesAlias,
			merge.typesAlias,
			merge.metadataAlias,
		)
	}
	chargeColumns := compiledKnowledgeSelectorChargeColumns{
		inputBytes: quoteIdentifier(fmt.Sprintf("__os_ko_selector_input_bytes_%d", stage)),
		queryUnits: quoteIdentifier(fmt.Sprintf("__os_ko_selector_query_units_%d", stage)),
	}
	bindingProjection := make([]string, 0, len(state.visible)+len(groups)+12)
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
	for _, merge := range groups {
		bindingProjection = append(bindingProjection, merge.bindingAlias)
	}

	projection := make([]string, 0, len(next.visible)+len(groups)*6+12)
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
			merge.namesProjection,
			merge.typesProjection,
			merge.metadataProjection,
		)
	}
	inputCharges := make([]string, 0, len(groups)+1)
	queryCharges := make([]string, 0, len(groups)+1)
	if priorCharges.inputBytes != "" {
		inputCharges = append(inputCharges, "toUInt128("+priorCharges.inputBytes+")")
		queryCharges = append(queryCharges, "toUInt128("+priorCharges.queryUnits+")")
	}
	bindings := make([]string, 0, len(groups))
	args := make([]any, 0)
	for _, merge := range groups {
		inputCharges = append(inputCharges, knowledgeTupleElementUInt128(
			merge.bindingAlias,
			knowledgeFieldBindingSelectorInputBytesElement,
		))
		queryCharges = append(queryCharges, knowledgeTupleElementUInt128(
			merge.bindingAlias,
			knowledgeFieldBindingSelectorQueryUnitsElement,
		))
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

func knowledgeAliasLoweringProofMatches(
	assignment compiledKnowledgeFieldAssignment,
	state compileState,
) bool {
	proof := assignment.alias
	inputStateAuthority, err := compileKnowledgeFieldInputStateAuthority(
		state,
		[]string{proof.Source()},
	)
	return proof.Origin() != (knowledgeprogram.Origin{}) &&
		err == nil &&
		assignment.calculated.Origin() == (knowledgeprogram.Origin{}) &&
		proof.Origin() == assignment.origin &&
		proof.Overwrite() == assignment.overwrite &&
		validCompiledKnowledgeFieldSourceAuthority(
			assignment.source,
			"field:"+proof.Source(),
			inputStateAuthority,
		) &&
		proof.Destination() == assignment.destination.Name &&
		slices.Equal(proof.Selector().CanonicalBytes(), assignment.selector.CanonicalBytes()) &&
		knowledgeCompiledSelectorMatches(assignment.selectorSQL, assignment.selector)
}

func knowledgeCalculatedLoweringProofMatches(
	assignment compiledKnowledgeFieldAssignment,
	state compileState,
) bool {
	proof := assignment.calculated
	inputStateAuthority, err := compileKnowledgeFieldInputStateAuthority(
		state,
		proof.InputFields(),
	)
	return proof.Origin() != (knowledgeprogram.Origin{}) &&
		err == nil &&
		assignment.alias.Origin() == (knowledgeprogram.Origin{}) &&
		proof.Origin() == assignment.origin &&
		proof.Overwrite() == assignment.overwrite &&
		validCompiledKnowledgeFieldSourceAuthority(
			assignment.source,
			"expression:"+proof.Expression(),
			inputStateAuthority,
		) &&
		proof.Destination() == assignment.destination.Name &&
		slices.Equal(proof.Selector().CanonicalBytes(), assignment.selector.CanonicalBytes()) &&
		knowledgeCompiledSelectorMatches(assignment.selectorSQL, assignment.selector)
}

func knowledgeCompiledSelectorMatches(
	compiled compiledKnowledgeSelector,
	authority knowledgeprogram.Selector,
) bool {
	expected, err := compileKnowledgeSelector(authority)
	return err == nil && compiled.sql == expected.sql &&
		reflect.DeepEqual(compiled.args, expected.args)
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

func compileKnowledgeFieldDestinationMerge(
	group *compiledKnowledgeFieldDestinationMerge,
	state compileState,
	stage int,
	index int,
) error {
	if group == nil || group.destination.Name == "" ||
		len(group.candidates) == 0 || stage < 0 || index < 0 {
		return errors.New(
			"compile ClickHouse knowledge fused field stage: destination group is invalid",
		)
	}
	previous, previousKnown, err := resolveCompiledField(group.destination, state)
	if err != nil {
		return err
	}
	previousSource, err := compileKnowledgeFieldSourceFromField(previous, previousKnown)
	if err != nil {
		return fmt.Errorf(
			"compile ClickHouse knowledge field destination source for %q: %w",
			group.destination.Name,
			err,
		)
	}

	candidates := make([]compiledKnowledgeFieldCandidate, len(group.candidates))
	candidateSQL := make([]string, len(group.candidates))
	args := make([]any, 0, len(previousSource.args))
	group.maxStringBytes = 0
	for candidateIndex, assignment := range group.candidates {
		candidate, candidateErr := compileKnowledgeFieldCandidate(
			assignment,
			previousSource,
		)
		if candidateErr != nil {
			return candidateErr
		}
		candidates[candidateIndex] = candidate
		candidateSQL[candidateIndex] = candidate.sql
		group.maxStringBytes = max(group.maxStringBytes, assignment.source.maxStringBytes)
	}
	if previousKnown {
		group.maxStringBytes = max(
			group.maxStringBytes,
			fieldStateStringByteBound(previous),
		)
	}

	const (
		candidatesVariable = "__os_ko_field_candidates"
		candidateVariable  = "__os_ko_field_candidate"
	)
	candidateSource := func(candidateSQL string) string {
		return knowledgeTupleElement(
			candidateSQL,
			knowledgeFieldAssignmentSourceElement,
		)
	}
	produced := func(candidateSQL string) string {
		return previousSource.producedSQL(candidateSource(candidateSQL)) + " != 0"
	}
	wroteSQL := "arrayExists(" + candidateVariable + " -> " +
		produced(candidateVariable) + ", " + candidatesVariable + ")"
	winnerSourceSQL := candidateSource(
		"arrayFirst(" + candidateVariable + " -> " +
			produced(candidateVariable) + ", " + candidatesVariable + ")",
	)
	chosenSourceSQL := "if(" + wroteSQL + ", " + winnerSourceSQL + ", " +
		previousSource.sql + ")"
	inputBytesSQL := "arrayFold((__os_ko_sum, " + candidateVariable + ") -> " +
		"__os_ko_sum + " + knowledgeTupleElementUInt128(
		candidateVariable,
		knowledgeFieldAssignmentSelectorInputBytesElement,
	) + ", " + candidatesVariable + ", toUInt128(0))"
	queryUnitsSQL := "arrayFold((__os_ko_sum, " + candidateVariable + ") -> " +
		"__os_ko_sum + " + knowledgeTupleElementUInt128(
		candidateVariable,
		knowledgeFieldAssignmentSelectorQueryUnitsElement,
	) + ", " + candidatesVariable + ", toUInt128(0))"
	body := "tuple(" + chosenSourceSQL + ", toUInt8(" + wroteSQL + "), " +
		inputBytesSQL + ", " + queryUnitsSQL + ")"
	bindingSQL := bindSQLExpressions(
		[]string{candidatesVariable},
		[]string{"[" + strings.Join(candidateSQL, ", ") + "]"},
		body,
	)
	// bindSQLExpressions writes the body before its bound value. The full prior
	// fallback therefore owns the first arguments, followed by candidates in
	// canonical program order.
	args = append(args, previousSource.args...)
	for _, candidate := range candidates {
		args = append(args, candidate.args...)
	}
	if len(bindingSQL) > maxCompiledQueryBytes || strings.Count(bindingSQL, "?") != len(args) {
		return errors.New(
			"compile ClickHouse knowledge fused field stage: destination SQL or arguments are invalid",
		)
	}

	group.bindingAlias = quoteIdentifier(fmt.Sprintf(
		"__os_ko_field_binding_%d_%d",
		stage,
		index,
	))
	group.bindingSQL = bindingSQL
	group.args = args
	resultSource := knowledgeTupleElement(
		group.bindingAlias,
		knowledgeFieldBindingSourceElement,
	)
	group.outputValueSQL = previousSource.valueSQL(resultSource)
	group.existsAlias = quoteIdentifier(fmt.Sprintf(
		"__os_ko_field_exists_%d_%d",
		stage,
		index,
	))
	group.typeAlias = quoteIdentifier(fmt.Sprintf(
		"__os_ko_field_type_%d_%d",
		stage,
		index,
	))
	group.descendantAlias = quoteIdentifier(fmt.Sprintf(
		"__os_ko_field_descendant_%d_%d",
		stage,
		index,
	))
	group.namesAlias = quoteIdentifier(fmt.Sprintf(
		"__os_ko_field_names_%d_%d",
		stage,
		index,
	))
	group.typesAlias = quoteIdentifier(fmt.Sprintf(
		"__os_ko_field_types_%d_%d",
		stage,
		index,
	))
	group.metadataAlias = quoteIdentifier(fmt.Sprintf(
		"__os_ko_field_metadata_version_%d_%d",
		stage,
		index,
	))
	group.existsProjection = previousSource.producedSQL(resultSource) + " AS " +
		group.existsAlias
	group.typeProjection = previousSource.storedTypeSQL(resultSource) + " AS " +
		group.typeAlias
	group.descendantProjection = "toUInt8(notEmpty(" +
		previousSource.namesSQL(resultSource) + ")) AS " + group.descendantAlias
	group.namesProjection = previousSource.namesSQL(resultSource) + " AS " +
		group.namesAlias
	group.typesProjection = previousSource.typesSQL(resultSource) + " AS " +
		group.typesAlias
	group.metadataProjection = previousSource.metadataVersionSQL(resultSource) +
		" AS " + group.metadataAlias
	return nil
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
