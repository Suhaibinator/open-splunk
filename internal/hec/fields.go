package hec

import (
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/Suhaibinator/open-splunk/internal/eventfields"
	"github.com/Suhaibinator/open-splunk/internal/jsonnumber"
)

const (
	MaximumFieldArrayElements    = 1_024
	MaximumJSONNumberBytes       = 128
	MaximumJSONExponentMagnitude = 1_024
	// Backward-compatible names for the typed-field API. The lexical bound is
	// shared by every number in a HEC envelope, not only indexed fields.
	MaximumFieldNumberBytes       = MaximumJSONNumberBytes
	MaximumFieldExponentMagnitude = MaximumJSONExponentMagnitude
)

// FieldNumberKind is the exact HEC-to-TypedValue numeric classification.
type FieldNumberKind uint8

const (
	FieldNumberSint64 FieldNumberKind = iota + 1
	FieldNumberUint64
	FieldNumberDecimal
)

// FieldNumber contains exactly one value selected by Kind. Decimal is the
// canonical decimal/v1 spelling (lowercase normalized exponent).
type FieldNumber struct {
	Kind    FieldNumberKind
	Sint64  int64
	Uint64  uint64
	Decimal string
}

// ValidateEnvelopeFields enforces the dependency-neutral HEC typed-field
// shape. Conversion into protobuf TypedValue messages remains with the ingest
// adapter so this package does not own the canonical event model.
func ValidateEnvelopeFields(envelope Envelope) error {
	if !envelope.Fields.Present {
		return nil
	}
	fields := envelope.Fields.Value
	if fields.Kind != JSONObject || len(fields.ObjectValue) > eventfields.MaximumStoredFieldsPerEvent {
		return NewEventError(ErrorIndexedFields, envelope.Number, errors.New("HEC fields member has an invalid shape or count"))
	}
	arrayElements := 0
	for _, field := range fields.ObjectValue {
		if !validFieldName(field.Name) {
			return NewEventError(ErrorIndexedFields, envelope.Number, errors.New("HEC field name is invalid"))
		}
		if field.Value.Kind == JSONArray {
			arrayElements += len(field.Value.ArrayValue)
			if arrayElements > MaximumFieldArrayElements {
				return NewEventError(ErrorIndexedFields, envelope.Number, errors.New("HEC field arrays exceed their scalar limit"))
			}
			for _, item := range field.Value.ArrayValue {
				if !validFieldScalar(item) {
					return NewEventError(ErrorIndexedFields, envelope.Number, errors.New("HEC field array contains a composite value"))
				}
			}
			continue
		}
		if !validFieldScalar(field.Value) {
			return NewEventError(ErrorIndexedFields, envelope.Number, errors.New("HEC field value is unsupported"))
		}
	}
	return nil
}

func validFieldName(name string) bool {
	if name == "" || len(name) > eventfields.MaximumDynamicPathSegmentBytes || !utf8.ValidString(name) ||
		eventfields.IsReservedDynamicRoot(name) {
		return false
	}
	return !strings.ContainsFunc(name, unicode.IsControl)
}

func validFieldScalar(value JSONValue) bool {
	switch value.Kind {
	case JSONNull, JSONString, JSONBoolean:
		return true
	case JSONNumber:
		_, err := ClassifyFieldNumber(value.NumberValue)
		return err == nil
	default:
		return false
	}
}

// ClassifyFieldNumber maps plain negative integers to sint64, plain
// nonnegative integers to uint64, and every fraction/exponent form to an exact
// DecimalValue. It applies lexical and exponent bounds before big-number work.
func ClassifyFieldNumber(number json.Number) (FieldNumber, error) {
	text := number.String()
	if err := validateBoundedNumberLexeme(text); err != nil {
		return FieldNumber{}, errors.New("HEC field number is invalid")
	}
	if !strings.ContainsAny(text, ".eE") {
		if strings.HasPrefix(text, "-") {
			value, err := strconv.ParseInt(text, 10, 64)
			if err != nil {
				return FieldNumber{}, errors.New("HEC signed field integer is outside int64")
			}
			return FieldNumber{Kind: FieldNumberSint64, Sint64: value}, nil
		}
		value, err := strconv.ParseUint(text, 10, 64)
		if err != nil {
			return FieldNumber{}, errors.New("HEC unsigned field integer is outside uint64")
		}
		return FieldNumber{Kind: FieldNumberUint64, Uint64: value}, nil
	}
	return FieldNumber{Kind: FieldNumberDecimal, Decimal: jsonnumber.NormalizeDecimalLexeme(text)}, nil
}

// validateBoundedNumberLexeme applies the common HEC v0.1 lexical and
// exponent bounds before any exact big-number conversion. json.Decoder already
// enforces JSON number grammar for numeric tokens; numeric strings used for
// time are checked separately before calling this helper.
func validateBoundedNumberLexeme(text string) error {
	if text == "" || len(text) > MaximumJSONNumberBytes {
		return errors.New("HEC JSON number has invalid length")
	}
	if exponentPosition := strings.IndexAny(text, "eE"); exponentPosition >= 0 {
		exponent := text[exponentPosition+1:]
		if strings.HasPrefix(exponent, "+") || strings.HasPrefix(exponent, "-") {
			exponent = exponent[1:]
		}
		exponent = strings.TrimLeft(exponent, "0")
		if exponent == "" {
			exponent = "0"
		}
		value, err := strconv.ParseUint(exponent, 10, 16)
		if err != nil || value > MaximumJSONExponentMagnitude {
			return errors.New("HEC JSON number exponent exceeds its bound")
		}
	}
	if _, err := jsonnumber.ParseDecimalRat(text); err != nil {
		return errors.New("HEC JSON number is invalid")
	}
	return nil
}
