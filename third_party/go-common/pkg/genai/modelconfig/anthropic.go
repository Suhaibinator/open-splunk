package modelconfig

import (
	"encoding/json/jsontext"
	json "encoding/json/v2"
	"fmt"

	anthropic "github.com/anthropics/anthropic-sdk-go"
	"github.com/anthropics/anthropic-sdk-go/packages/param"
)

// EffectiveAnthropicHostedTools projects the native tools the SDK will send,
// including escape-hatch overrides. Unlike the Responses adapter, Anthropic's
// adapter preserves undeclared tool fields, so retain their wire representation.
func EffectiveAnthropicHostedTools(params anthropic.MessageNewParams) ([]jsontext.Value, error) {
	projection := anthropic.MessageNewParams{Tools: params.Tools}
	if override, ok := params.Overrides(); ok {
		projection = param.Override[anthropic.MessageNewParams](override)
	} else if tools, ok := params.ExtraFields()["tools"]; ok {
		projection.SetExtraFields(map[string]any{"tools": tools})
	}
	encoded, err := json.Marshal(projection)
	if err != nil {
		return nil, fmt.Errorf("encode native Anthropic tools: %w", err)
	}
	var wire struct {
		Tools []jsontext.Value `json:"tools"`
	}
	if err := json.Unmarshal(encoded, &wire); err != nil {
		return nil, fmt.Errorf("decode native Anthropic tools: %w", err)
	}
	return wire.Tools, nil
}
