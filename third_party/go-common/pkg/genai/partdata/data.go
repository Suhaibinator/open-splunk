// Package partdata centralizes the Genkit v1.13 data-part representation used by
// provider adapters and durable transcript conversion.
package partdata

import (
	"fmt"
	"strings"

	"github.com/firebase/genkit/go/ai"
)

// String returns a data part's provider-facing serialized representation.
// Genkit stores string payloads verbatim and JSON-serializes structured values.
// DataString reports serialization failures as an empty string, so this helper
// distinguishes those failures from valid nil and empty-string payloads.
func String(part *ai.Part) (string, error) {
	if part == nil || !part.IsData() {
		return "", nil
	}
	if _, isBytes := part.Data.([]byte); isBytes {
		// Genkit would JSON-encode a byte slice as a quoted base64 string, which no
		// caller intends. Refuse rather than persist a representation the author did not choose.
		return "", fmt.Errorf("genkit data payload of type []byte is ambiguous: pass raw bytes as a string or a structured value")
	}
	value := part.DataString()
	if value != "" || part.Data == nil {
		return value, nil
	}
	if _, ok := part.Data.(string); ok {
		return value, nil
	}
	return "", fmt.Errorf("genkit data payload of type %T is not JSON serializable", part.Data)
}

// ContentType returns the declared content type, or application/json when the
// part contains a non-string value that DataString necessarily JSON-serializes.
// Inferring before durable conversion preserves that representation after the
// structured Go value becomes an opaque byte/string payload.
func ContentType(part *ai.Part) string {
	if part == nil {
		return ""
	}
	if contentType := strings.TrimSpace(part.ContentType); contentType != "" {
		return contentType
	}
	if part.IsData() && part.Data != nil {
		if _, isString := part.Data.(string); !isString {
			return "application/json"
		}
	}
	return ""
}
