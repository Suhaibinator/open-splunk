package inference

import (
	"fmt"
	"strings"
)

// NormalizeProviderModelID preserves opaque upstream identifiers, trimming only surrounding whitespace.
func NormalizeProviderModelID(provider string, id string) (string, error) {
	if strings.TrimSpace(provider) == "" {
		return "", fmt.Errorf("provider is required")
	}
	id = strings.TrimSpace(id)
	if id == "" {
		return "", fmt.Errorf("provider model id is required")
	}
	return id, nil
}
