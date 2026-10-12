package agentkit

import (
	"bytes"
	"encoding/json/jsontext"
	json "encoding/json/v2"
	"fmt"
)

// DecodeStrictArguments unmarshals raw tool arguments into dst, rejecting unknown
// fields, an explicit null, and trailing JSON. Empty input is treated as an empty
// object so a no-argument tool call decodes cleanly. Tool executors should always
// decode their arguments through this (or DecodeToolArgs) so a model that
// hallucinates an extra field fails loudly instead of silently dropping it.
func DecodeStrictArguments(raw jsontext.Value, dst any) error {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 {
		trimmed = []byte(`{}`)
	}
	if bytes.Equal(trimmed, []byte(`null`)) {
		return fmt.Errorf("arguments must be a JSON object")
	}

	if err := json.Unmarshal(trimmed, dst, json.RejectUnknownMembers(true)); err != nil {
		return fmt.Errorf("invalid tool arguments: %w", err)
	}
	return nil
}

// DecodeToolArgs is the generic convenience wrapper over DecodeStrictArguments: it
// allocates a fresh A, strictly decodes raw into it, and returns it. A decode
// failure is a model-visible invalid-arguments condition, not an operational
// error, so callers typically map it to an ErrorResult with
// GenericErrorCodeInvalidArguments.
func DecodeToolArgs[A any](raw jsontext.Value) (A, error) {
	var args A
	if err := DecodeStrictArguments(raw, &args); err != nil {
		return args, err
	}
	return args, nil
}
