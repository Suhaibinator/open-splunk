package decisions

import (
	"bytes"
	"encoding/json/jsontext"
	json "encoding/json/v2"
	"errors"
)

// Content is text or structured JSON accepted by the Decisions API. Construct
// values with Text, JSON, or Null; its serialized representation is kept
// private and immutable so callers do not need to marshal request fragments.
// The zero value is invalid and is rejected by question constructors and Decide.
type Content struct {
	raw []byte
}

// Text constructs content from a string. If value cannot be encoded as JSON,
// Text returns invalid zero Content, which question constructors and Decide
// reject.
func Text(value string) Content {
	raw, err := json.Marshal(value)
	if err != nil {
		return Content{}
	}
	return Content{raw: raw}
}

// JSON constructs content by JSON-encoding value. The resulting JSON must be a
// string, object, or array; scalar numbers, booleans, and null are rejected.
func JSON(value any) (Content, error) {
	raw, err := json.Marshal(value)
	if err != nil {
		return Content{}, err
	}
	content, err := parseContent(raw, false)
	if err != nil {
		return Content{}, err
	}
	return content, nil
}

// Null constructs a JSON null choice description, indicating that the option
// name needs no extra guidance. It is valid only in [NewChoice]'s criteria map.
func Null() Content {
	return Content{raw: []byte("null")}
}

// MarshalJSON implements JSON serialization for requests built by this package.
// It returns an error for zero or otherwise uninitialized Content. Most callers
// should use Content through Request and the question constructors rather than
// call MarshalJSON directly.
func (c Content) MarshalJSON() ([]byte, error) {
	if len(c.raw) == 0 {
		return nil, errors.New("decisions: content is not initialized")
	}
	return bytes.Clone(c.raw), nil
}

func parseContent(raw []byte, allowNull bool) (Content, error) {
	trimmed := bytes.TrimSpace(raw)
	if !jsontext.Value(trimmed).IsValid() {
		return Content{}, errors.New("decisions: content is not valid JSON")
	}
	if bytes.Equal(trimmed, []byte("null")) {
		if !allowNull {
			return Content{}, errors.New("decisions: null content is only valid for choice descriptions")
		}
		return Null(), nil
	}
	if len(trimmed) == 0 || (trimmed[0] != '"' && trimmed[0] != '{' && trimmed[0] != '[') {
		return Content{}, errors.New("decisions: content must be a string, object, or array")
	}
	return Content{raw: bytes.Clone(trimmed)}, nil
}

func (c Content) validate(allowNull bool) error {
	_, err := parseContent(c.raw, allowNull)
	return err
}
