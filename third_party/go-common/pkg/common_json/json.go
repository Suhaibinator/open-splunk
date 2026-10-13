// Package common_json centralizes the repository's JSON v2 policies that are
// intentionally stronger than a plain encoding/json/v2 call.
package common_json

import (
	"errors"

	"encoding/json/jsontext"
	json "encoding/json/v2"
)

var exactNumbers = json.WithUnmarshalers(
	json.UnmarshalFromFunc(func(decoder *jsontext.Decoder, value *any) error {
		if decoder.PeekKind() == '0' {
			*value = jsontext.Value(nil)
		}
		return errors.ErrUnsupported
	}),
)

// UnmarshalExact decodes JSON using v2 semantics while preserving every
// number reached through an any value as its exact JSON representation.
func UnmarshalExact(data []byte, destination any) error {
	return json.Unmarshal(data, destination, exactNumbers)
}

// MarshalCanonical encodes a value with JSON v2 defaults and canonicalizes
// the resulting JSON for stable hashing and comparisons.
func MarshalCanonical(value any) (jsontext.Value, error) {
	encoded, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	return Canonicalize(encoded)
}

// Canonicalize validates and canonicalizes one raw JSON value.
func Canonicalize(raw []byte) (jsontext.Value, error) {
	value := append(jsontext.Value(nil), raw...)
	// JCS normally rounds integers through IEEE-754. Durable tool arguments can
	// contain identifiers beyond 2^53, so retain their exact decimal value while
	// still canonicalizing floats and object member order.
	if err := value.Canonicalize(jsontext.CanonicalizeRawInts(false)); err != nil {
		return nil, err
	}
	return value, nil
}
