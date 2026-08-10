package hec

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/Suhaibinator/open-splunk/internal/eventfields"
)

// JSONKind identifies one exact JSON value representation.
type JSONKind uint8

const (
	JSONNull JSONKind = iota + 1
	JSONString
	JSONNumber
	JSONBoolean
	JSONObject
	JSONArray
)

// JSONMember preserves authored object member order. The streaming decoder
// rejects duplicates before constructing this representation.
type JSONMember struct {
	Name  string
	Value JSONValue
}

// JSONValue is a bounded exact JSON tree. NumberValue retains the source
// lexeme as json.Number and ObjectValue retains authored member order.
type JSONValue struct {
	Kind         JSONKind
	StringValue  string
	NumberValue  json.Number
	BooleanValue bool
	ObjectValue  []JSONMember
	ArrayValue   []JSONValue
	encodedBytes int64
}

// PresentValue distinguishes an absent envelope member from a present JSON
// null or any other value.
type PresentValue struct {
	Present bool
	Value   JSONValue
}

// Envelope is one complete JSON HEC event envelope. Number is zero-based and
// is suitable for invalid-event-number reporting.
type Envelope struct {
	Number       int
	Time         PresentValue
	Host         PresentValue
	Source       PresentValue
	Sourcetype   PresentValue
	Index        PresentValue
	Event        PresentValue
	Fields       PresentValue
	encodedBytes int64
	fieldsError  error
}

// EnvelopeDecoder incrementally decodes a whitespace-separated sequence of
// top-level objects. It retains at most the current and caller-retained
// envelopes rather than materializing the complete request.
type EnvelopeDecoder struct {
	decoder          *json.Decoder
	limits           Limits
	nextNumber       int
	values           int
	members          int
	normalizedBytes  int64
	failed           error
	done             bool
	fieldsContext    bool
	fieldArrayValues int
	currentFieldsErr error
}

// NewEnvelopeDecoder constructs an exact-number, UTF-8-validating streaming
// decoder. Body decompression/byte limiting should normally be applied first
// with NewBodyReader.
func NewEnvelopeDecoder(source io.Reader, limits Limits) (*EnvelopeDecoder, error) {
	if source == nil {
		return nil, NewProtocolError(ErrorInternal, errors.New("HEC JSON reader is nil"))
	}
	if err := limits.Validate(); err != nil {
		return nil, NewProtocolError(ErrorInternal, err)
	}
	decoder := json.NewDecoder(newUTF8ValidatingReader(source))
	decoder.UseNumber()
	return &EnvelopeDecoder{decoder: decoder, limits: limits}, nil
}

// Next returns the next complete envelope or io.EOF after all trailing
// whitespace. Once a deterministic failure occurs, subsequent calls return
// that same failure.
func (decoder *EnvelopeDecoder) Next() (Envelope, error) {
	if decoder == nil {
		return Envelope{}, NewProtocolError(ErrorInternal, errors.New("HEC JSON decoder is nil"))
	}
	if decoder.failed != nil {
		return Envelope{}, decoder.failed
	}
	if decoder.done {
		return Envelope{}, io.EOF
	}

	token, err := decoder.decoder.Token()
	if errors.Is(err, io.EOF) {
		decoder.done = true
		return Envelope{}, io.EOF
	}
	if err != nil {
		return Envelope{}, decoder.failDecode(err, decoder.nextNumber)
	}
	if decoder.nextNumber >= decoder.limits.MaximumEvents {
		return Envelope{}, decoder.fail(NewEventError(ErrorInvalidDataFormat, decoder.nextNumber, errors.New("HEC event count exceeds its limit")))
	}
	delimiter, ok := token.(json.Delim)
	if !ok || delimiter != '{' {
		return Envelope{}, decoder.fail(NewEventError(ErrorInvalidDataFormat, decoder.nextNumber, errors.New("HEC top-level value is not an object")))
	}

	envelope := Envelope{Number: decoder.nextNumber, encodedBytes: 2}
	decoder.nextNumber++
	seen := make(map[string]struct{}, 7)
	memberIndex := 0
	for decoder.decoder.More() {
		nameToken, nameErr := decoder.decoder.Token()
		if nameErr != nil {
			return Envelope{}, decoder.failDecode(nameErr, envelope.Number)
		}
		name, ok := nameToken.(string)
		if !ok || !utf8.ValidString(name) {
			return Envelope{}, decoder.fail(NewEventError(ErrorInvalidDataFormat, envelope.Number, errors.New("HEC envelope member name is invalid")))
		}
		decoder.members++
		if decoder.members > decoder.limits.MaximumObjectMembers {
			return Envelope{}, decoder.fail(NewEventError(ErrorInvalidDataFormat, envelope.Number, errors.New("HEC object member count exceeds its limit")))
		}
		if _, duplicate := seen[name]; duplicate {
			return Envelope{}, decoder.fail(NewEventError(ErrorInvalidDataFormat, envelope.Number, errors.New("HEC envelope member is duplicated")))
		}
		seen[name] = struct{}{}
		member := envelope.member(name)
		if member == nil {
			return Envelope{}, decoder.fail(NewEventError(ErrorInvalidDataFormat, envelope.Number, errors.New("HEC envelope member is unknown")))
		}
		previousFieldsContext := decoder.fieldsContext
		decoder.fieldsContext = name == "fields"
		if decoder.fieldsContext {
			decoder.fieldArrayValues = 0
			decoder.currentFieldsErr = nil
		}
		value, valueErr := decoder.decodeValue(1, envelope.Number)
		if decoder.fieldsContext {
			envelope.fieldsError = decoder.currentFieldsErr
		}
		decoder.fieldsContext = previousFieldsContext
		if valueErr != nil {
			return Envelope{}, decoder.fail(valueErr)
		}
		member.Present, member.Value = true, value
		if memberIndex != 0 {
			envelope.encodedBytes++
		}
		encodedName, marshalErr := json.Marshal(name)
		if marshalErr != nil {
			return Envelope{}, decoder.fail(NewEventError(ErrorInternal, envelope.Number, marshalErr))
		}
		envelope.encodedBytes += int64(len(encodedName)) + 1 + value.encodedBytes
		memberIndex++
	}
	closing, closeErr := decoder.decoder.Token()
	if closeErr != nil {
		return Envelope{}, decoder.failDecode(closeErr, envelope.Number)
	}
	if delimiter, ok := closing.(json.Delim); !ok || delimiter != '}' {
		return Envelope{}, decoder.fail(NewEventError(ErrorInvalidDataFormat, envelope.Number, errors.New("HEC envelope is not closed")))
	}
	if validationErr := validateEnvelopeShape(envelope, decoder.limits); validationErr != nil {
		return Envelope{}, decoder.fail(validationErr)
	}
	if envelope.encodedBytes > decoder.limits.MaximumNormalizedBytes-decoder.normalizedBytes {
		return Envelope{}, decoder.fail(NewProtocolError(ErrorNormalizedBodyTooLarge, errors.New("HEC normalized request exceeds its limit")))
	}
	decoder.normalizedBytes += envelope.encodedBytes
	return envelope, nil
}

// Count returns the number of envelope starts consumed, including the current
// envelope when it failed after its opening object token.
func (decoder *EnvelopeDecoder) Count() int {
	if decoder == nil {
		return 0
	}
	return decoder.nextNumber
}

func (envelope *Envelope) member(name string) *PresentValue {
	switch name {
	case "time":
		return &envelope.Time
	case "host":
		return &envelope.Host
	case "source":
		return &envelope.Source
	case "sourcetype":
		return &envelope.Sourcetype
	case "index":
		return &envelope.Index
	case "event":
		return &envelope.Event
	case "fields":
		return &envelope.Fields
	default:
		return nil
	}
}

func (decoder *EnvelopeDecoder) decodeValue(depth, eventNumber int) (JSONValue, error) {
	decoder.values++
	if decoder.values > decoder.limits.MaximumJSONValues {
		return JSONValue{}, NewEventError(ErrorInvalidDataFormat, eventNumber, errors.New("HEC JSON value count exceeds its limit"))
	}
	token, err := decoder.decoder.Token()
	if err != nil {
		return JSONValue{}, decoder.eventDecodeError(err, eventNumber)
	}
	if decoder.fieldsContext {
		if delimiter, composite := token.(json.Delim); composite {
			allowed := depth == 1 && delimiter == '{' || depth == 2 && delimiter == '['
			if !allowed {
				return JSONValue{}, NewEventError(ErrorIndexedFields, eventNumber, errors.New("HEC field value has an invalid composite shape"))
			}
		} else if depth == 1 {
			return JSONValue{}, NewEventError(ErrorIndexedFields, eventNumber, errors.New("HEC fields member must be an object"))
		}
	}
	switch value := token.(type) {
	case nil:
		return JSONValue{Kind: JSONNull, encodedBytes: 4}, nil
	case string:
		encoded, marshalErr := json.Marshal(value)
		if marshalErr != nil {
			return JSONValue{}, NewEventError(ErrorInternal, eventNumber, marshalErr)
		}
		return JSONValue{Kind: JSONString, StringValue: value, encodedBytes: int64(len(encoded))}, nil
	case json.Number:
		text := value.String()
		if numberErr := validateBoundedNumberLexeme(text); numberErr != nil {
			kind := ErrorInvalidDataFormat
			if decoder.fieldsContext {
				kind = ErrorIndexedFields
			}
			return JSONValue{}, NewEventError(kind, eventNumber, numberErr)
		}
		return JSONValue{Kind: JSONNumber, NumberValue: value, encodedBytes: int64(len(text))}, nil
	case bool:
		if value {
			return JSONValue{Kind: JSONBoolean, BooleanValue: true, encodedBytes: 4}, nil
		}
		return JSONValue{Kind: JSONBoolean, encodedBytes: 5}, nil
	case json.Delim:
		if depth > decoder.limits.MaximumJSONDepth {
			return JSONValue{}, NewEventError(ErrorInvalidDataFormat, eventNumber, errors.New("HEC JSON nesting exceeds its limit"))
		}
		switch value {
		case '{':
			return decoder.decodeObject(depth, eventNumber)
		case '[':
			return decoder.decodeArray(depth, eventNumber)
		default:
			return JSONValue{}, NewEventError(ErrorInvalidDataFormat, eventNumber, errors.New("unexpected HEC JSON delimiter"))
		}
	default:
		return JSONValue{}, NewEventError(ErrorInvalidDataFormat, eventNumber, errors.New("unexpected HEC JSON token"))
	}
}

func (decoder *EnvelopeDecoder) decodeObject(depth, eventNumber int) (JSONValue, error) {
	value := JSONValue{Kind: JSONObject, encodedBytes: 2}
	seen := make(map[string]struct{})
	for decoder.decoder.More() {
		nameToken, err := decoder.decoder.Token()
		if err != nil {
			return JSONValue{}, decoder.eventDecodeError(err, eventNumber)
		}
		name, ok := nameToken.(string)
		if !ok || !utf8.ValidString(name) {
			return JSONValue{}, NewEventError(ErrorInvalidDataFormat, eventNumber, errors.New("HEC JSON object member name is invalid"))
		}
		decoder.members++
		if decoder.members > decoder.limits.MaximumObjectMembers {
			return JSONValue{}, NewEventError(ErrorInvalidDataFormat, eventNumber, errors.New("HEC object member count exceeds its limit"))
		}
		if _, duplicate := seen[name]; duplicate {
			kind := ErrorInvalidDataFormat
			if decoder.fieldsContext {
				if decoder.currentFieldsErr == nil {
					decoder.currentFieldsErr = NewEventError(
						ErrorIndexedFields,
						eventNumber,
						errors.New("HEC JSON object member is duplicated"),
					)
				}
				// Consume the duplicate value so envelope-level precedence can
				// report a missing event before the deferred fields error.
				if _, valueErr := decoder.decodeValue(depth+1, eventNumber); valueErr != nil {
					return JSONValue{}, valueErr
				}
				continue
			}
			return JSONValue{}, NewEventError(kind, eventNumber, errors.New("HEC JSON object member is duplicated"))
		}
		if decoder.fieldsContext && depth == 1 && len(value.ObjectValue) >= eventfields.MaximumStoredFieldsPerEvent {
			return JSONValue{}, NewEventError(ErrorIndexedFields, eventNumber, errors.New("HEC fields member exceeds its field-count limit"))
		}
		seen[name] = struct{}{}
		memberValue, valueErr := decoder.decodeValue(depth+1, eventNumber)
		if valueErr != nil {
			return JSONValue{}, valueErr
		}
		encodedName, marshalErr := json.Marshal(name)
		if marshalErr != nil {
			return JSONValue{}, NewEventError(ErrorInternal, eventNumber, marshalErr)
		}
		if len(value.ObjectValue) != 0 {
			value.encodedBytes++
		}
		value.encodedBytes += int64(len(encodedName)) + 1 + memberValue.encodedBytes
		value.ObjectValue = append(value.ObjectValue, JSONMember{Name: name, Value: memberValue})
	}
	closing, err := decoder.decoder.Token()
	if err != nil {
		return JSONValue{}, decoder.eventDecodeError(err, eventNumber)
	}
	if delimiter, ok := closing.(json.Delim); !ok || delimiter != '}' {
		return JSONValue{}, NewEventError(ErrorInvalidDataFormat, eventNumber, errors.New("HEC JSON object is not closed"))
	}
	return value, nil
}

func (decoder *EnvelopeDecoder) decodeArray(depth, eventNumber int) (JSONValue, error) {
	value := JSONValue{Kind: JSONArray, encodedBytes: 2}
	for decoder.decoder.More() {
		if decoder.fieldsContext && depth == 2 {
			decoder.fieldArrayValues++
			if decoder.fieldArrayValues > MaximumFieldArrayElements {
				return JSONValue{}, NewEventError(ErrorIndexedFields, eventNumber, errors.New("HEC field arrays exceed their scalar limit"))
			}
		}
		item, err := decoder.decodeValue(depth+1, eventNumber)
		if err != nil {
			return JSONValue{}, err
		}
		if len(value.ArrayValue) != 0 {
			value.encodedBytes++
		}
		value.encodedBytes += item.encodedBytes
		value.ArrayValue = append(value.ArrayValue, item)
	}
	closing, err := decoder.decoder.Token()
	if err != nil {
		return JSONValue{}, decoder.eventDecodeError(err, eventNumber)
	}
	if delimiter, ok := closing.(json.Delim); !ok || delimiter != ']' {
		return JSONValue{}, NewEventError(ErrorInvalidDataFormat, eventNumber, errors.New("HEC JSON array is not closed"))
	}
	return value, nil
}

func validateEnvelopeShape(envelope Envelope, limits Limits) error {
	if !envelope.Event.Present {
		return NewEventError(ErrorEventRequired, envelope.Number, nil)
	}
	if envelope.Event.Value.Kind == JSONNull ||
		envelope.Event.Value.Kind == JSONString && envelope.Event.Value.StringValue == "" {
		return NewEventError(ErrorEventBlank, envelope.Number, nil)
	}
	eventBytes := envelope.Event.Value.encodedBytes
	if envelope.Event.Value.Kind == JSONString {
		eventBytes = int64(len(envelope.Event.Value.StringValue))
	}
	if envelope.Event.Value.Kind == JSONString && strings.IndexByte(envelope.Event.Value.StringValue, 0) >= 0 {
		return NewEventError(ErrorInvalidDataFormat, envelope.Number, errors.New("HEC event contains NUL"))
	}
	if eventBytes > limits.MaximumEventBytes {
		return NewEventError(ErrorEventTooLarge, envelope.Number, errors.New("HEC event exceeds its size limit"))
	}
	if err := validateExplicitTime(envelope); err != nil {
		return err
	}
	if _, err := DecodeEnvelopeMetadata(envelope); err != nil {
		return err
	}
	if envelope.fieldsError != nil {
		return envelope.fieldsError
	}
	if err := ValidateEnvelopeFields(envelope); err != nil {
		return err
	}
	return nil
}

func (decoder *EnvelopeDecoder) failDecode(err error, eventNumber int) error {
	return decoder.fail(decoder.eventDecodeError(err, eventNumber))
}

func (decoder *EnvelopeDecoder) eventDecodeError(err error, eventNumber int) error {
	var failure *ProtocolError
	if errors.As(err, &failure) {
		if failure.InvalidEventNumber == nil &&
			failure.Kind != ErrorCompressedBodyTooLarge && failure.Kind != ErrorDecompressedBodyTooLarge &&
			failure.Kind != ErrorNormalizedBodyTooLarge && failure.Kind != ErrorInvalidCompressedBody &&
			failure.Kind != ErrorInvalidUTF8 {
			copy := *failure
			copy.InvalidEventNumber = &eventNumber
			return &copy
		}
		return failure
	}
	return NewEventError(ErrorInvalidDataFormat, eventNumber, err)
}

func (decoder *EnvelopeDecoder) fail(err error) error {
	decoder.failed = err
	return err
}

// CompactJSON serializes a decoded value with no whitespace, preserved object
// member order, and exact source number lexemes.
func (value JSONValue) CompactJSON() ([]byte, error) {
	result := make([]byte, 0, max(0, int(value.encodedBytes)))
	result, err := value.appendCompact(result)
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (value JSONValue) appendCompact(destination []byte) ([]byte, error) {
	switch value.Kind {
	case JSONNull:
		return append(destination, "null"...), nil
	case JSONString:
		encoded, err := json.Marshal(value.StringValue)
		if err != nil {
			return nil, err
		}
		return append(destination, encoded...), nil
	case JSONNumber:
		if value.NumberValue == "" {
			return nil, errors.New("HEC JSON number is empty")
		}
		return append(destination, value.NumberValue.String()...), nil
	case JSONBoolean:
		return strconv.AppendBool(destination, value.BooleanValue), nil
	case JSONObject:
		destination = append(destination, '{')
		for index, member := range value.ObjectValue {
			if index != 0 {
				destination = append(destination, ',')
			}
			encodedName, err := json.Marshal(member.Name)
			if err != nil {
				return nil, err
			}
			destination = append(destination, encodedName...)
			destination = append(destination, ':')
			destination, err = member.Value.appendCompact(destination)
			if err != nil {
				return nil, err
			}
		}
		return append(destination, '}'), nil
	case JSONArray:
		destination = append(destination, '[')
		for index, item := range value.ArrayValue {
			if index != 0 {
				destination = append(destination, ',')
			}
			var err error
			destination, err = item.appendCompact(destination)
			if err != nil {
				return nil, err
			}
		}
		return append(destination, ']'), nil
	default:
		return nil, fmt.Errorf("unknown HEC JSON kind %d", value.Kind)
	}
}

// RawEvent converts the event member to the canonical _raw representation:
// strings become their decoded UTF-8 bytes and every other accepted value is
// compact JSON.
func (envelope Envelope) RawEvent() ([]byte, error) {
	if !envelope.Event.Present {
		return nil, NewEventError(ErrorEventRequired, envelope.Number, nil)
	}
	if envelope.Event.Value.Kind == JSONNull ||
		envelope.Event.Value.Kind == JSONString && envelope.Event.Value.StringValue == "" {
		return nil, NewEventError(ErrorEventBlank, envelope.Number, nil)
	}
	if envelope.Event.Value.Kind == JSONString {
		if strings.IndexByte(envelope.Event.Value.StringValue, 0) >= 0 {
			return nil, NewEventError(ErrorInvalidDataFormat, envelope.Number, errors.New("HEC event contains NUL"))
		}
		return bytes.Clone([]byte(envelope.Event.Value.StringValue)), nil
	}
	return envelope.Event.Value.CompactJSON()
}
