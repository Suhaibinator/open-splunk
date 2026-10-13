package modelconfig

import (
	json "encoding/json/v2"
	"fmt"
	"reflect"
	"strings"

	"github.com/openai/openai-go/v3/packages/param"
	"github.com/openai/openai-go/v3/responses"
)

// EffectiveResponsesHostedTools returns the native tools after the same SDK normalization
// used when building a request. Typed and extra-field configuration is projected
// to tools only; a whole-object override must be decoded to obtain its tools.
// Callers can fingerprint and estimate tool costs independently of other settings.
func EffectiveResponsesHostedTools(params responses.ResponseNewParams) ([]responses.ToolUnionParam, error) {
	projection := responses.ResponseNewParams{Tools: params.Tools}
	if override, ok := params.Overrides(); ok {
		// A whole-object SDK override can replace tools too. Its JSON object is
		// the authoritative source, rather than the otherwise ignored typed fields.
		projection = param.Override[responses.ResponseNewParams](override)
	} else if tools, ok := params.ExtraFields()["tools"]; ok {
		projection.SetExtraFields(map[string]any{"tools": tools})
	}
	normalized, err := NormalizeResponsesParams(projection)
	if err != nil {
		return nil, err
	}
	return normalized.Tools, nil
}

// NormalizeResponsesParams materializes SDK escape-hatch overrides before policy
// checks and Genkit-owned fields are applied. Without this round trip, a raw
// ResponseNewParams override (or a tool object override hidden behind a benign
// union field) would ignore the public fields inspected and rewritten below.
// The typed decode drops undeclared fields, so provider extensions such as
// OpenRouter's provider and session_id are re-attached afterwards from the
// override's members or the extra fields; members and extra fields spelling a
// declared name stay materialized and policed.
func NormalizeResponsesParams(params responses.ResponseNewParams) (responses.ResponseNewParams, error) {
	encoded, err := json.Marshal(params)
	if err != nil {
		return responses.ResponseNewParams{}, fmt.Errorf("encode native Responses params: %w", err)
	}
	var normalized responses.ResponseNewParams
	if err := json.Unmarshal(encoded, &normalized); err != nil {
		return responses.ResponseNewParams{}, fmt.Errorf("decode native Responses params: %w", err)
	}
	// ExtraFields is the SDK's map[string]any extension surface. A whole-object
	// override carries its extensions as members instead, and ExtraFields is empty.
	extensions := make(map[string]any)
	members, _, err := OverrideMembers(params)
	if err != nil {
		return responses.ResponseNewParams{}, err
	}
	for key, value := range members {
		if !responsesParamFields[key] {
			extensions[key] = value
		}
	}
	for key, value := range params.ExtraFields() {
		if !responsesParamFields[key] {
			extensions[key] = value
		}
	}
	if len(extensions) != 0 {
		normalized.SetExtraFields(extensions)
	}
	return normalized, nil
}

// responsesParamFields holds the top-level wire names ResponseNewParams decodes.
var responsesParamFields = func() map[string]bool {
	declared := make(map[string]bool)
	for field := range reflect.TypeFor[responses.ResponseNewParams]().Fields() {
		if name, _, _ := strings.Cut(field.Tag.Get("json"), ","); field.IsExported() && name != "" && name != "-" {
			declared[name] = true
		}
	}
	return declared
}()
