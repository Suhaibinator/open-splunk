package server

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"unicode/utf8"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/knowledge"
	"github.com/Suhaibinator/open-splunk/internal/knowledgedefinition"
	"github.com/Suhaibinator/open-splunk/internal/knowledgevalidation"
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

// validateKnowledgeObjectCodec owns the Validate-specific allocation boundary
// which the general protobuf codec cannot provide for attacker-controlled
// repetitions.
type validateKnowledgeObjectCodec struct{}

func newValidateKnowledgeObjectCodec() *validateKnowledgeObjectCodec {
	return &validateKnowledgeObjectCodec{}
}

func (*validateKnowledgeObjectCodec) NewRequest() *opensplunkv1.ValidateKnowledgeObjectRequest {
	return &opensplunkv1.ValidateKnowledgeObjectRequest{}
}

func (codec *validateKnowledgeObjectCodec) Decode(
	request *http.Request,
) (*opensplunkv1.ValidateKnowledgeObjectRequest, error) {
	if request == nil || request.Body == nil {
		return nil, errors.New("knowledge validation request body is unavailable")
	}
	defer request.Body.Close()
	data, err := io.ReadAll(io.LimitReader(
		request.Body,
		maximumKnowledgeMutationRequestBytes+1,
	))
	if err != nil {
		return nil, err
	}
	return codec.DecodeBytes(data)
}

func (*validateKnowledgeObjectCodec) DecodeBytes(
	data []byte,
) (*opensplunkv1.ValidateKnowledgeObjectRequest, error) {
	if int64(len(data)) > maximumKnowledgeMutationRequestBytes {
		return nil, &http.MaxBytesError{Limit: maximumKnowledgeMutationRequestBytes}
	}
	projection, err := inspectValidateProjection(data)
	if err != nil {
		return nil, err
	}
	builder := validateRequestWireBuilder{projection: projection}
	if err := builder.consume(data); err != nil {
		return nil, err
	}
	return builder.finish(), nil
}

// sealedValidateKnowledgeObjectResponse is the only transport value which may
// carry a service seal and one serialization permit into Encode. The phantom
// protobuf parameter keeps the registered response contract statically
// discoverable without retaining a second mutable response representation.
type sealedValidateKnowledgeObjectResponse[Message proto.Message] struct {
	sealed  knowledgevalidation.SealedValidateResponse
	ctx     context.Context
	release func()
}

type serializedValidateKnowledgeObjectResponse = sealedValidateKnowledgeObjectResponse[*opensplunkv1.ValidateKnowledgeObjectResponse]

func newSerializedValidateKnowledgeObjectResponse(
	sealed knowledgevalidation.SealedValidateResponse,
	ctx context.Context,
	release func(),
) *serializedValidateKnowledgeObjectResponse {
	return &serializedValidateKnowledgeObjectResponse{
		sealed:  sealed,
		ctx:     ctx,
		release: release,
	}
}

func (*validateKnowledgeObjectCodec) Encode(
	response http.ResponseWriter,
	result *serializedValidateKnowledgeObjectResponse,
) error {
	if result == nil || result.release == nil {
		return errors.New("knowledge validation response serialization state is invalid")
	}
	defer result.release()
	if response == nil {
		return errors.New("knowledge validation response writer is unavailable")
	}
	if err := validateTransportContextError(result.ctx); err != nil {
		return err
	}
	validated, err := result.sealed.Proto(result.ctx)
	if err != nil {
		return err
	}
	if validated == nil {
		return knowledgevalidation.ErrInvariant
	}
	payload := result.sealed.DeterministicBytes()
	if len(payload) == 0 || len(payload) > knowledgevalidation.MaximumResponseBytes {
		return knowledgevalidation.ErrInvariant
	}
	if err := validateTransportContextError(result.ctx); err != nil {
		return err
	}
	response.Header().Set("Content-Type", "application/x-protobuf")
	_, err = response.Write(payload)
	return err
}

func validateTransportContextError(ctx context.Context) error {
	if ctx == nil {
		return errors.New("knowledge validation response context is unavailable")
	}
	return ctx.Err()
}

type validateDefinitionProjection struct {
	full     bool
	selected [13]bool
}

func (projection validateDefinitionProjection) retains(number protowire.Number) bool {
	return projection.full ||
		number >= 0 && int(number) < len(projection.selected) && projection.selected[number]
}

func inspectValidateProjection(data []byte) (validateDefinitionProjection, error) {
	var objectIDPresent bool
	mask := validateMaskWireBuilder{discardUnknown: true}
	err := walkValidateWire(data, func(field validateWireField) error {
		switch {
		case field.number == 2 && field.wireType == protowire.BytesType:
			if _, err := validateWireString(field); err != nil {
				return err
			}
			objectIDPresent = true
		case field.number == 4 && field.wireType == protowire.BytesType:
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

type validateRequestWireBuilder struct {
	projection      validateDefinitionProjection
	candidate       *validateDefinitionWireBuilder
	objectID        []byte
	objectIDPresent bool
	expectedVersion uint64
	expectedPresent bool
	mask            validateMaskWireBuilder
	intent          uint64
	unknown         []byte
}

func (builder *validateRequestWireBuilder) consume(data []byte) error {
	return walkValidateWire(data, func(field validateWireField) error {
		switch {
		case field.number == 1 && field.wireType == protowire.BytesType:
			if builder.candidate == nil {
				builder.candidate = &validateDefinitionWireBuilder{projection: builder.projection}
			}
			return builder.candidate.consume(field.payload)
		case field.number == 2 && field.wireType == protowire.BytesType:
			value, err := validateWireString(field)
			if err != nil {
				return err
			}
			builder.objectID = value
			builder.objectIDPresent = true
		case field.number == 3 && field.wireType == protowire.VarintType:
			builder.expectedVersion = field.varint
			builder.expectedPresent = true
		case field.number == 4 && field.wireType == protowire.BytesType:
			builder.mask.present = true
			return builder.mask.consume(field.payload)
		case field.number == 5 && field.wireType == protowire.VarintType:
			builder.intent = field.varint
		default:
			builder.unknown = appendValidateUnknown(builder.unknown, field)
		}
		return nil
	})
}

func (builder *validateRequestWireBuilder) finish() *opensplunkv1.ValidateKnowledgeObjectRequest {
	result := &opensplunkv1.ValidateKnowledgeObjectRequest{
		Intent: opensplunkv1.KnowledgeValidationIntent(int32(builder.intent)),
	}
	if builder.candidate != nil {
		result.Definition = builder.candidate.finish()
	}
	if builder.objectIDPresent {
		value := validateWireStringValue(builder.objectID)
		result.KnowledgeObjectId = &value
	}
	if builder.expectedPresent {
		value := builder.expectedVersion
		result.ExpectedVersion = &value
	}
	if builder.mask.present {
		result.UpdateMask = builder.mask.finish()
	}
	setValidateUnknown(result, builder.unknown)
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
	fieldAlias         validateFieldAliasWireBuilder
	calculatedField    validateCalculatedFieldWireBuilder
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
		return (*validateFieldAliasWireBuilder)(nil).consume(field.payload)
	case 12:
		if retained {
			return builder.calculatedField.consume(field.payload)
		}
		return (*validateCalculatedFieldWireBuilder)(nil).consume(field.payload)
	default:
		return nil
	}
}

func (builder *validateDefinitionWireBuilder) finish() *opensplunkv1.KnowledgeObjectDefinition {
	result := &opensplunkv1.KnowledgeObjectDefinition{
		AppId:        validateWireStringValue(builder.appID),
		Name:         validateWireStringValue(builder.name),
		SharingScope: opensplunkv1.SharingScope(int32(builder.sharingScope)),
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
			result.Body = &opensplunkv1.KnowledgeObjectDefinition_FieldExtraction{FieldExtraction: builder.fieldExtraction.finish()}
		}
	case 11:
		if builder.projection.retains(11) {
			result.Body = &opensplunkv1.KnowledgeObjectDefinition_FieldAlias{FieldAlias: builder.fieldAlias.finish()}
		}
	case 12:
		if builder.projection.retains(12) {
			result.Body = &opensplunkv1.KnowledgeObjectDefinition_CalculatedField{CalculatedField: builder.calculatedField.finish()}
		}
	}
	setValidateUnknown(result, builder.unknown)
	return result
}

type validateSelectorWireBuilder struct {
	patterns [4][]*opensplunkv1.KnowledgeSelectorPattern
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

func (builder *validateSelectorWireBuilder) finish() *opensplunkv1.KnowledgeSelector {
	result := &opensplunkv1.KnowledgeSelector{
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

func (builder *validateSelectorPatternWireBuilder) finish() *opensplunkv1.KnowledgeSelectorPattern {
	result := &opensplunkv1.KnowledgeSelectorPattern{
		MatchKind: opensplunkv1.KnowledgeSelectorMatchKind(int32(builder.matchKind)),
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

func (builder *validateFieldExtractionWireBuilder) finish() *opensplunkv1.FieldExtractionDefinition {
	result := &opensplunkv1.FieldExtractionDefinition{
		InputField:        validateWireStringValue(builder.inputField),
		OverwriteBehavior: opensplunkv1.KnowledgeOverwriteBehavior(int32(builder.overwrite)),
	}
	switch builder.extractionNumber {
	case 2:
		result.Extraction = &opensplunkv1.FieldExtractionDefinition_Regex{Regex: builder.regex.finish()}
	case 3:
		result.Extraction = &opensplunkv1.FieldExtractionDefinition_Json{Json: builder.json.finish()}
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

func (builder *validateRegexWireBuilder) finish() *opensplunkv1.RegexFieldExtractionDefinition {
	result := &opensplunkv1.RegexFieldExtractionDefinition{Pattern: validateWireStringValue(builder.pattern), OutputFields: make([]string, len(builder.outputs))}
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

func (builder *validateJSONWireBuilder) finish() *opensplunkv1.JsonFieldExtractionDefinition {
	result := &opensplunkv1.JsonFieldExtractionDefinition{Path: validateWireStringValue(builder.path), OutputField: validateWireStringValue(builder.output)}
	setValidateUnknown(result, builder.unknown)
	return result
}

type validateFieldAliasWireBuilder struct {
	source      []byte
	destination []byte
	overwrite   uint64
	unknown     []byte
}

func (builder *validateFieldAliasWireBuilder) reset() {
	builder.source = nil
	builder.destination = nil
	builder.overwrite = 0
	builder.unknown = builder.unknown[:0]
}

func (builder *validateFieldAliasWireBuilder) consume(data []byte) error {
	return walkValidateWire(data, func(field validateWireField) error {
		switch {
		case (field.number == 1 || field.number == 2) && field.wireType == protowire.BytesType:
			value, err := validateWireString(field)
			if err != nil {
				return err
			}
			if builder != nil {
				if field.number == 1 {
					builder.source = value
				} else {
					builder.destination = value
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

func (builder *validateFieldAliasWireBuilder) finish() *opensplunkv1.FieldAliasDefinition {
	result := &opensplunkv1.FieldAliasDefinition{SourceField: validateWireStringValue(builder.source), DestinationField: validateWireStringValue(builder.destination), OverwriteBehavior: opensplunkv1.KnowledgeOverwriteBehavior(int32(builder.overwrite))}
	setValidateUnknown(result, builder.unknown)
	return result
}

type validateCalculatedFieldWireBuilder struct {
	destination []byte
	expression  []byte
	overwrite   uint64
	unknown     []byte
}

func (builder *validateCalculatedFieldWireBuilder) reset() {
	builder.destination = nil
	builder.expression = nil
	builder.overwrite = 0
	builder.unknown = builder.unknown[:0]
}

func (builder *validateCalculatedFieldWireBuilder) consume(data []byte) error {
	return walkValidateWire(data, func(field validateWireField) error {
		switch {
		case (field.number == 1 || field.number == 2) && field.wireType == protowire.BytesType:
			value, err := validateWireString(field)
			if err != nil {
				return err
			}
			if builder != nil {
				if field.number == 1 {
					builder.destination = value
				} else {
					builder.expression = value
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

func (builder *validateCalculatedFieldWireBuilder) finish() *opensplunkv1.CalculatedFieldDefinition {
	result := &opensplunkv1.CalculatedFieldDefinition{DestinationField: validateWireStringValue(builder.destination), Expression: validateWireStringValue(builder.expression), OverwriteBehavior: opensplunkv1.KnowledgeOverwriteBehavior(int32(builder.overwrite))}
	setValidateUnknown(result, builder.unknown)
	return result
}
