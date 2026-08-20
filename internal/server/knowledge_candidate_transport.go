package server

// This file owns the allocation-bounded raw-wire decoder shared by candidate
// validation request envelopes.

import (
	"errors"
	"fmt"
	"unicode/utf8"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/knowledge"
	"github.com/Suhaibinator/open-splunk/internal/knowledgedefinition"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

const (
	maximumValidateRetainedMaskPaths         = len(validateDefinitionProjectionFields) + 1
	maximumValidateRetainedSelectorPatterns  = knowledge.MaximumSelectorPatternsPerDimension + 1
	maximumValidateRetainedExtractionOutputs = knowledgedefinition.MaximumFieldExtractionOutputs + 1
	maximumValidateUnknownGroupDepth         = 32
)

var validateDefinitionProjectionFields = [...]struct {
	path   string
	number protowire.Number
}{
	{path: "app_id", number: 1},
	{path: "calculated_field", number: 12},
	{path: "description", number: 3},
	{path: "field_alias", number: 11},
	{path: "field_extraction", number: 10},
	{path: "name", number: 2},
	{path: "selector", number: 5},
	{path: "sharing_scope", number: 4},
}

type validateDefinitionProjection struct {
	full     bool
	selected [13]bool
}

func (projection validateDefinitionProjection) retains(number protowire.Number) bool {
	return projection.full ||
		number >= 0 && int(number) < len(projection.selected) && projection.selected[number]
}

type knowledgeCandidateRequestWireLayout struct {
	definition      protowire.Number
	objectID        protowire.Number
	expectedVersion protowire.Number
	updateMask      protowire.Number
}

func inspectKnowledgeCandidateProjection(
	data []byte,
	layout knowledgeCandidateRequestWireLayout,
) (validateDefinitionProjection, error) {
	var objectIDPresent bool
	mask := validateMaskWireBuilder{discardUnknown: true}
	err := walkValidateWire(data, func(field validateWireField) error {
		switch {
		case field.number == layout.objectID && field.wireType == protowire.BytesType:
			if _, err := validateWireString(field); err != nil {
				return err
			}
			objectIDPresent = true
		case field.number == layout.updateMask && field.wireType == protowire.BytesType:
			mask.present = true
			if err := mask.consume(field.payload); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return validateDefinitionProjection{}, err
	}
	if !objectIDPresent {
		return validateDefinitionProjection{full: true}, nil
	}
	projection := validateDefinitionProjection{}
	for _, path := range mask.paths {
		for _, field := range validateDefinitionProjectionFields {
			if string(path) == field.path {
				projection.selected[field.number] = true
				break
			}
		}
	}
	return projection, nil
}

type validateWireField struct {
	number    protowire.Number
	wireType  protowire.Type
	valueWire []byte
	payload   []byte
	varint    uint64
}

func walkValidateWire(data []byte, visit func(validateWireField) error) error {
	for len(data) != 0 {
		number, wireType, tagBytes := protowire.ConsumeTag(data)
		if tagBytes < 0 {
			return fmt.Errorf("invalid knowledge validation protobuf tag: %w", protowire.ParseError(tagBytes))
		}
		if number > protowire.MaxValidNumber {
			return errors.New("knowledge validation protobuf field number exceeds the maximum")
		}
		valueBytes, err := consumeValidateWireValue(number, wireType, data[tagBytes:], 0)
		if err != nil {
			return err
		}
		field := validateWireField{
			number:    number,
			wireType:  wireType,
			valueWire: data[tagBytes : tagBytes+valueBytes],
		}
		switch wireType {
		case protowire.BytesType:
			field.payload, _ = protowire.ConsumeBytes(field.valueWire)
		case protowire.VarintType:
			field.varint, _ = protowire.ConsumeVarint(field.valueWire)
		}
		if err := visit(field); err != nil {
			return err
		}
		data = data[tagBytes+valueBytes:]
	}
	return nil
}

func consumeValidateWireValue(
	number protowire.Number,
	wireType protowire.Type,
	data []byte,
	groupDepth int,
) (int, error) {
	var consumed int
	switch wireType {
	case protowire.VarintType:
		_, consumed = protowire.ConsumeVarint(data)
	case protowire.Fixed32Type:
		_, consumed = protowire.ConsumeFixed32(data)
	case protowire.Fixed64Type:
		_, consumed = protowire.ConsumeFixed64(data)
	case protowire.BytesType:
		_, consumed = protowire.ConsumeBytes(data)
	case protowire.StartGroupType:
		if groupDepth >= maximumValidateUnknownGroupDepth {
			return 0, errors.New("knowledge validation protobuf group recursion exceeds 32")
		}
		remaining := data
		for {
			nestedNumber, nestedType, tagBytes := protowire.ConsumeTag(remaining)
			if tagBytes < 0 || nestedNumber > protowire.MaxValidNumber {
				return 0, errors.New("invalid knowledge validation protobuf group")
			}
			remaining = remaining[tagBytes:]
			if nestedType == protowire.EndGroupType {
				if nestedNumber != number {
					return 0, errors.New("mismatched knowledge validation protobuf end group")
				}
				return len(data) - len(remaining), nil
			}
			valueBytes, err := consumeValidateWireValue(
				nestedNumber,
				nestedType,
				remaining,
				groupDepth+1,
			)
			if err != nil {
				return 0, err
			}
			remaining = remaining[valueBytes:]
		}
	case protowire.EndGroupType:
		return 0, errors.New("unexpected knowledge validation protobuf end group")
	default:
		return 0, errors.New("invalid knowledge validation protobuf wire type")
	}
	if consumed < 0 {
		return 0, fmt.Errorf("invalid knowledge validation protobuf value: %w", protowire.ParseError(consumed))
	}
	return consumed, nil
}

func validateWireString(field validateWireField) ([]byte, error) {
	if !utf8.Valid(field.payload) {
		return nil, errors.New("knowledge validation protobuf contains invalid UTF-8")
	}
	return field.payload, nil
}

func appendValidateUnknown(destination []byte, field validateWireField) []byte {
	destination = protowire.AppendTag(destination, field.number, field.wireType)
	return append(destination, field.valueWire...)
}

func setValidateUnknown(message proto.Message, unknown []byte) {
	if len(unknown) != 0 {
		message.ProtoReflect().SetUnknown(append([]byte(nil), unknown...))
	}
}

func validateWireStringValue(value []byte) string {
	return string(value)
}

type knowledgeCandidateRequestWireResult struct {
	candidateDefinition *opensplunk.KnowledgeObjectDefinition
	objectID            *string
	expectedVersion     *uint64
	updateMask          *fieldmaskpb.FieldMask
	unknown             []byte
}

type knowledgeCandidateEnvelopeWireConsumer func(validateWireField) (bool, error)

type knowledgeCandidateRequestWireBuilder struct {
	layout          knowledgeCandidateRequestWireLayout
	projection      validateDefinitionProjection
	candidate       *validateDefinitionWireBuilder
	objectID        []byte
	objectIDPresent bool
	expectedVersion uint64
	expectedPresent bool
	mask            validateMaskWireBuilder
	unknown         []byte
}

func decodeKnowledgeCandidateRequestWire(
	data []byte,
	layout knowledgeCandidateRequestWireLayout,
	consumeEnvelope knowledgeCandidateEnvelopeWireConsumer,
) (knowledgeCandidateRequestWireResult, error) {
	projection, err := inspectKnowledgeCandidateProjection(data, layout)
	if err != nil {
		return knowledgeCandidateRequestWireResult{}, err
	}
	builder := knowledgeCandidateRequestWireBuilder{
		layout:     layout,
		projection: projection,
	}
	if err := builder.consume(data, consumeEnvelope); err != nil {
		return knowledgeCandidateRequestWireResult{}, err
	}
	return builder.finish(), nil
}

func (builder *knowledgeCandidateRequestWireBuilder) consume(
	data []byte,
	consumeEnvelope knowledgeCandidateEnvelopeWireConsumer,
) error {
	return walkValidateWire(data, func(field validateWireField) error {
		switch {
		case field.number == builder.layout.definition && field.wireType == protowire.BytesType:
			if builder.candidate == nil {
				builder.candidate = &validateDefinitionWireBuilder{projection: builder.projection}
			}
			return builder.candidate.consume(field.payload)
		case field.number == builder.layout.objectID && field.wireType == protowire.BytesType:
			value, err := validateWireString(field)
			if err != nil {
				return err
			}
			builder.objectID = value
			builder.objectIDPresent = true
			return nil
		case field.number == builder.layout.expectedVersion && field.wireType == protowire.VarintType:
			builder.expectedVersion = field.varint
			builder.expectedPresent = true
			return nil
		case field.number == builder.layout.updateMask && field.wireType == protowire.BytesType:
			builder.mask.present = true
			return builder.mask.consume(field.payload)
		}
		if consumeEnvelope != nil {
			handled, err := consumeEnvelope(field)
			if err != nil {
				return err
			}
			if handled {
				return nil
			}
		}
		builder.unknown = appendValidateUnknown(builder.unknown, field)
		return nil
	})
}

func (builder *knowledgeCandidateRequestWireBuilder) finish() knowledgeCandidateRequestWireResult {
	result := knowledgeCandidateRequestWireResult{
		// Both adapters immediately install this ephemeral builder-owned buffer
		// with setValidateUnknown, which performs the one required detached copy.
		unknown: builder.unknown,
	}
	if builder.candidate != nil {
		result.candidateDefinition = builder.candidate.finish()
	}
	if builder.objectIDPresent {
		value := validateWireStringValue(builder.objectID)
		result.objectID = &value
	}
	if builder.expectedPresent {
		value := builder.expectedVersion
		result.expectedVersion = &value
	}
	if builder.mask.present {
		result.updateMask = builder.mask.finish()
	}
	return result
}

type validateMaskWireBuilder struct {
	present        bool
	discardUnknown bool
	paths          [][]byte
	unknown        []byte
}

func (builder *validateMaskWireBuilder) consume(data []byte) error {
	return walkValidateWire(data, func(field validateWireField) error {
		if field.number == 1 && field.wireType == protowire.BytesType {
			value, err := validateWireString(field)
			if err != nil {
				return err
			}
			if len(builder.paths) < maximumValidateRetainedMaskPaths {
				builder.paths = append(builder.paths, value)
			}
			return nil
		}
		if !builder.discardUnknown {
			builder.unknown = appendValidateUnknown(builder.unknown, field)
		}
		return nil
	})
}

func (builder *validateMaskWireBuilder) finish() *fieldmaskpb.FieldMask {
	result := &fieldmaskpb.FieldMask{Paths: make([]string, len(builder.paths))}
	for index, path := range builder.paths {
		result.Paths[index] = validateWireStringValue(path)
	}
	setValidateUnknown(result, builder.unknown)
	return result
}

type validateDefinitionWireBuilder struct {
	projection         validateDefinitionProjection
	appID              []byte
	name               []byte
	description        []byte
	descriptionPresent bool
	sharingScope       uint64
	selector           *validateSelectorWireBuilder
	bodyNumber         protowire.Number
	fieldExtraction    validateFieldExtractionWireBuilder
	fieldAlias         validateStringPairOverwriteWireBuilder
	calculatedField    validateStringPairOverwriteWireBuilder
	unknown            []byte
}

func (builder *validateDefinitionWireBuilder) consume(data []byte) error {
	return walkValidateWire(data, func(field validateWireField) error {
		switch {
		case (field.number == 1 || field.number == 2 || field.number == 3) && field.wireType == protowire.BytesType:
			value, err := validateWireString(field)
			if err != nil {
				return err
			}
			if !builder.projection.retains(field.number) {
				return nil
			}
			switch field.number {
			case 1:
				builder.appID = value
			case 2:
				builder.name = value
			case 3:
				builder.description = value
				builder.descriptionPresent = true
			}
		case field.number == 4 && field.wireType == protowire.VarintType:
			if builder.projection.retains(4) {
				builder.sharingScope = field.varint
			}
		case field.number == 5 && field.wireType == protowire.BytesType:
			if builder.projection.retains(5) {
				if builder.selector == nil {
					builder.selector = &validateSelectorWireBuilder{}
				}
				return builder.selector.consume(field.payload)
			}
			return (*validateSelectorWireBuilder)(nil).consume(field.payload)
		case (field.number == 10 || field.number == 11 || field.number == 12) && field.wireType == protowire.BytesType:
			return builder.consumeBody(field)
		default:
			if builder.projection.full {
				builder.unknown = appendValidateUnknown(builder.unknown, field)
			}
		}
		return nil
	})
}

func (builder *validateDefinitionWireBuilder) consumeBody(field validateWireField) error {
	retained := builder.projection.retains(field.number)
	if builder.bodyNumber != field.number {
		builder.bodyNumber = field.number
		switch field.number {
		case 10:
			builder.fieldExtraction.reset()
		case 11:
			builder.fieldAlias.reset()
		case 12:
			builder.calculatedField.reset()
		}
	}
	switch field.number {
	case 10:
		if retained {
			return builder.fieldExtraction.consume(field.payload)
		}
		return (*validateFieldExtractionWireBuilder)(nil).consume(field.payload)
	case 11:
		if retained {
			return builder.fieldAlias.consume(field.payload)
		}
		return (*validateStringPairOverwriteWireBuilder)(nil).consume(field.payload)
	case 12:
		if retained {
			return builder.calculatedField.consume(field.payload)
		}
		return (*validateStringPairOverwriteWireBuilder)(nil).consume(field.payload)
	default:
		return nil
	}
}

func (builder *validateDefinitionWireBuilder) finish() *opensplunk.KnowledgeObjectDefinition {
	result := &opensplunk.KnowledgeObjectDefinition{
		AppId:        validateWireStringValue(builder.appID),
		Name:         validateWireStringValue(builder.name),
		SharingScope: opensplunk.SharingScope(int32(builder.sharingScope)), // #nosec G115 -- protobuf varints intentionally retain generated unmarshal truncation semantics.
	}
	if builder.descriptionPresent {
		value := validateWireStringValue(builder.description)
		result.Description = &value
	}
	if builder.selector != nil {
		result.Selector = builder.selector.finish()
	}
	switch builder.bodyNumber {
	case 10:
		if builder.projection.retains(10) {
			result.Body = &opensplunk.KnowledgeObjectDefinition_FieldExtraction{FieldExtraction: builder.fieldExtraction.finish()}
		}
	case 11:
		if builder.projection.retains(11) {
			result.Body = &opensplunk.KnowledgeObjectDefinition_FieldAlias{FieldAlias: builder.fieldAlias.finishFieldAlias()}
		}
	case 12:
		if builder.projection.retains(12) {
			result.Body = &opensplunk.KnowledgeObjectDefinition_CalculatedField{CalculatedField: builder.calculatedField.finishCalculatedField()}
		}
	}
	setValidateUnknown(result, builder.unknown)
	return result
}

type validateSelectorWireBuilder struct {
	patterns [4][]*opensplunk.KnowledgeSelectorPattern
	unknown  []byte
}

func (builder *validateSelectorWireBuilder) consume(data []byte) error {
	return walkValidateWire(data, func(field validateWireField) error {
		if field.number >= 1 && field.number <= 4 && field.wireType == protowire.BytesType {
			retain := builder != nil && len(builder.patterns[field.number-1]) < maximumValidateRetainedSelectorPatterns
			var patternBuilder *validateSelectorPatternWireBuilder
			if retain {
				patternBuilder = &validateSelectorPatternWireBuilder{}
			}
			if err := patternBuilder.consume(field.payload); err != nil {
				return err
			}
			if retain {
				builder.patterns[field.number-1] = append(builder.patterns[field.number-1], patternBuilder.finish())
			}
			return nil
		}
		if builder != nil {
			builder.unknown = appendValidateUnknown(builder.unknown, field)
		}
		return nil
	})
}

func (builder *validateSelectorWireBuilder) finish() *opensplunk.KnowledgeSelector {
	result := &opensplunk.KnowledgeSelector{
		IndexPatterns:      builder.patterns[0],
		HostPatterns:       builder.patterns[1],
		SourcePatterns:     builder.patterns[2],
		SourcetypePatterns: builder.patterns[3],
	}
	setValidateUnknown(result, builder.unknown)
	return result
}

type validateSelectorPatternWireBuilder struct {
	matchKind uint64
	value     []byte
	unknown   []byte
}

func (builder *validateSelectorPatternWireBuilder) consume(data []byte) error {
	return walkValidateWire(data, func(field validateWireField) error {
		switch {
		case field.number == 1 && field.wireType == protowire.VarintType:
			if builder != nil {
				builder.matchKind = field.varint
			}
		case field.number == 2 && field.wireType == protowire.BytesType:
			value, err := validateWireString(field)
			if err != nil {
				return err
			}
			if builder != nil {
				builder.value = value
			}
		default:
			if builder != nil {
				builder.unknown = appendValidateUnknown(builder.unknown, field)
			}
		}
		return nil
	})
}

func (builder *validateSelectorPatternWireBuilder) finish() *opensplunk.KnowledgeSelectorPattern {
	result := &opensplunk.KnowledgeSelectorPattern{
		MatchKind: opensplunk.KnowledgeSelectorMatchKind(int32(builder.matchKind)), // #nosec G115 -- protobuf varints intentionally retain generated unmarshal truncation semantics.
		Value:     validateWireStringValue(builder.value),
	}
	setValidateUnknown(result, builder.unknown)
	return result
}

type validateFieldExtractionWireBuilder struct {
	inputField       []byte
	overwrite        uint64
	extractionNumber protowire.Number
	regex            validateRegexWireBuilder
	json             validateJSONWireBuilder
	unknown          []byte
}

func (builder *validateFieldExtractionWireBuilder) reset() {
	builder.inputField = nil
	builder.overwrite = 0
	builder.extractionNumber = 0
	builder.regex.reset()
	builder.json.reset()
	builder.unknown = builder.unknown[:0]
}

func (builder *validateFieldExtractionWireBuilder) consume(data []byte) error {
	return walkValidateWire(data, func(field validateWireField) error {
		switch {
		case field.number == 1 && field.wireType == protowire.BytesType:
			value, err := validateWireString(field)
			if err != nil {
				return err
			}
			if builder != nil {
				builder.inputField = value
			}
		case field.number == 4 && field.wireType == protowire.VarintType:
			if builder != nil {
				builder.overwrite = field.varint
			}
		case (field.number == 2 || field.number == 3) && field.wireType == protowire.BytesType:
			if builder != nil && builder.extractionNumber != field.number {
				builder.extractionNumber = field.number
				if field.number == 2 {
					builder.regex.reset()
				} else {
					builder.json.reset()
				}
			}
			if field.number == 2 {
				if builder == nil {
					return (*validateRegexWireBuilder)(nil).consume(field.payload)
				}
				return builder.regex.consume(field.payload)
			}
			if builder == nil {
				return (*validateJSONWireBuilder)(nil).consume(field.payload)
			}
			return builder.json.consume(field.payload)
		default:
			if builder != nil {
				builder.unknown = appendValidateUnknown(builder.unknown, field)
			}
		}
		return nil
	})
}

func (builder *validateFieldExtractionWireBuilder) finish() *opensplunk.FieldExtractionDefinition {
	result := &opensplunk.FieldExtractionDefinition{
		InputField:        validateWireStringValue(builder.inputField),
		OverwriteBehavior: opensplunk.KnowledgeOverwriteBehavior(int32(builder.overwrite)), // #nosec G115 -- protobuf varints intentionally retain generated unmarshal truncation semantics.
	}
	switch builder.extractionNumber {
	case 2:
		result.Extraction = &opensplunk.FieldExtractionDefinition_Regex{Regex: builder.regex.finish()}
	case 3:
		result.Extraction = &opensplunk.FieldExtractionDefinition_Json{Json: builder.json.finish()}
	}
	setValidateUnknown(result, builder.unknown)
	return result
}

type validateRegexWireBuilder struct {
	pattern []byte
	outputs [][]byte
	unknown []byte
}

func (builder *validateRegexWireBuilder) reset() {
	builder.pattern = nil
	builder.outputs = builder.outputs[:0]
	builder.unknown = builder.unknown[:0]
}

func (builder *validateRegexWireBuilder) consume(data []byte) error {
	return walkValidateWire(data, func(field validateWireField) error {
		switch {
		case field.number == 1 && field.wireType == protowire.BytesType:
			value, err := validateWireString(field)
			if err != nil {
				return err
			}
			if builder != nil {
				builder.pattern = value
			}
		case field.number == 2 && field.wireType == protowire.BytesType:
			value, err := validateWireString(field)
			if err != nil {
				return err
			}
			if builder != nil && len(builder.outputs) < maximumValidateRetainedExtractionOutputs {
				builder.outputs = append(builder.outputs, value)
			}
		default:
			if builder != nil {
				builder.unknown = appendValidateUnknown(builder.unknown, field)
			}
		}
		return nil
	})
}

func (builder *validateRegexWireBuilder) finish() *opensplunk.RegexFieldExtractionDefinition {
	result := &opensplunk.RegexFieldExtractionDefinition{Pattern: validateWireStringValue(builder.pattern), OutputFields: make([]string, len(builder.outputs))}
	for index, output := range builder.outputs {
		result.OutputFields[index] = validateWireStringValue(output)
	}
	setValidateUnknown(result, builder.unknown)
	return result
}

type validateJSONWireBuilder struct {
	path    []byte
	output  []byte
	unknown []byte
}

func (builder *validateJSONWireBuilder) reset() {
	builder.path = nil
	builder.output = nil
	builder.unknown = builder.unknown[:0]
}

func (builder *validateJSONWireBuilder) consume(data []byte) error {
	return walkValidateWire(data, func(field validateWireField) error {
		switch {
		case (field.number == 1 || field.number == 2) && field.wireType == protowire.BytesType:
			value, err := validateWireString(field)
			if err != nil {
				return err
			}
			if builder != nil {
				if field.number == 1 {
					builder.path = value
				} else {
					builder.output = value
				}
			}
		default:
			if builder != nil {
				builder.unknown = appendValidateUnknown(builder.unknown, field)
			}
		}
		return nil
	})
}

func (builder *validateJSONWireBuilder) finish() *opensplunk.JsonFieldExtractionDefinition {
	result := &opensplunk.JsonFieldExtractionDefinition{Path: validateWireStringValue(builder.path), OutputField: validateWireStringValue(builder.output)}
	setValidateUnknown(result, builder.unknown)
	return result
}

// validateStringPairOverwriteWireBuilder decodes the wire shape shared by the
// field-alias and calculated-field bodies: two length-delimited strings and one
// overwrite varint. Every method tolerates a nil receiver so the discard path
// can reuse the same parser without retaining anything.
type validateStringPairOverwriteWireBuilder struct {
	first     []byte
	second    []byte
	overwrite uint64
	unknown   []byte
}

func (builder *validateStringPairOverwriteWireBuilder) reset() {
	builder.first = nil
	builder.second = nil
	builder.overwrite = 0
	builder.unknown = builder.unknown[:0]
}

func (builder *validateStringPairOverwriteWireBuilder) consume(data []byte) error {
	return walkValidateWire(data, func(field validateWireField) error {
		switch {
		case (field.number == 1 || field.number == 2) && field.wireType == protowire.BytesType:
			value, err := validateWireString(field)
			if err != nil {
				return err
			}
			if builder != nil {
				if field.number == 1 {
					builder.first = value
				} else {
					builder.second = value
				}
			}
		case field.number == 3 && field.wireType == protowire.VarintType:
			if builder != nil {
				builder.overwrite = field.varint
			}
		default:
			if builder != nil {
				builder.unknown = appendValidateUnknown(builder.unknown, field)
			}
		}
		return nil
	})
}

func (builder *validateStringPairOverwriteWireBuilder) finishFieldAlias() *opensplunk.FieldAliasDefinition {
	result := &opensplunk.FieldAliasDefinition{
		SourceField:       validateWireStringValue(builder.first),
		DestinationField:  validateWireStringValue(builder.second),
		OverwriteBehavior: opensplunk.KnowledgeOverwriteBehavior(int32(builder.overwrite)), // #nosec G115 -- protobuf varints intentionally retain generated unmarshal truncation semantics.
	}
	setValidateUnknown(result, builder.unknown)
	return result
}

func (builder *validateStringPairOverwriteWireBuilder) finishCalculatedField() *opensplunk.CalculatedFieldDefinition {
	result := &opensplunk.CalculatedFieldDefinition{
		DestinationField:  validateWireStringValue(builder.first),
		Expression:        validateWireStringValue(builder.second),
		OverwriteBehavior: opensplunk.KnowledgeOverwriteBehavior(int32(builder.overwrite)), // #nosec G115 -- protobuf varints intentionally retain generated unmarshal truncation semantics.
	}
	setValidateUnknown(result, builder.unknown)
	return result
}
