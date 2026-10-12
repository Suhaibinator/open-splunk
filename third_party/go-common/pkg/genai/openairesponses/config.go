package openairesponses

import (
	json "encoding/json/v2"
	"fmt"

	"github.com/Suhaibinator/go-common/pkg/genai/modelconfig"

	"github.com/firebase/genkit/go/ai"
	"github.com/firebase/genkit/go/core"
)

// Config is the model configuration accepted by this provider. Params exposes
// the native Responses API request surface. Fields owned by Genkit (model,
// input, tools, tool_choice, text) are derived from ai.ModelRequest instead.
type Config = modelconfig.ResponsesConfig

// ClientToolBinding maps a Genkit tool name to its native Responses definition.
type ClientToolBinding = modelconfig.ClientToolBinding

// ConfigFrom converts a Genkit model config value into Config. It accepts the
// typed values go-common produces: Config from modelconfig.Config.Native and
// ModelRef, and GenerationCommonConfig from configs whose API was unresolved.
// JSON callers such as the Genkit Dev UI deliver map[string]any, which must be
// the Config wrapper; unknown members are rejected. The parameter is Genkit's
// untyped config slot.
func ConfigFrom(v any) (Config, error) {
	switch cfg := v.(type) {
	case nil:
		return Config{}, nil
	case Config:
		return cfg, nil
	case *Config:
		if cfg == nil {
			return Config{}, nil
		}
		return *cfg, nil
	case ai.GenerationCommonConfig:
		return Config{Common: &cfg}, nil
	case *ai.GenerationCommonConfig:
		if cfg == nil {
			return Config{}, nil
		}
		common := *cfg
		return Config{Common: &common}, nil
	case map[string]any:
		b, err := json.Marshal(cfg)
		if err != nil {
			return Config{}, fmt.Errorf("openairesponses: encode config: %w", err)
		}
		var decoded Config
		if err := json.Unmarshal(b, &decoded, json.RejectUnknownMembers(true)); err != nil {
			return Config{}, fmt.Errorf("openairesponses: config must be {common, params, clientTools, requestReasoningTraces}: %w", err)
		}
		return decoded, nil
	}
	return Config{}, fmt.Errorf("openairesponses: unsupported config type %T; use openairesponses.Config", v)
}

// configSchema describes the JSON forms of the configs ConfigFrom accepts.
// Genkit validates typed configs against it too, so it admits the flat
// GenerationCommonConfig alongside the wrapper; maps must still use the
// wrapper. Native params and tools are checked by the SDK decode and
// buildRequest. map[string]any is Genkit's ModelOptions.ConfigSchema type.
func configSchema() map[string]any {
	common := core.InferSchemaMap(ai.GenerationCommonConfig{})
	return map[string]any{
		"anyOf": []any{
			map[string]any{
				"type":                 "object",
				"additionalProperties": false,
				"properties": map[string]any{
					"common": common,
					"params": map[string]any{"type": "object"},
					"clientTools": map[string]any{
						"type": "array",
						"items": map[string]any{
							"type":                 "object",
							"additionalProperties": false,
							"required":             []any{"genkitToolName", "tool"},
							"properties": map[string]any{
								"genkitToolName": map[string]any{"type": "string"},
								"tool":           map[string]any{"type": "object"},
							},
						},
					},
					"requestReasoningTraces": map[string]any{"type": "boolean"},
				},
			},
			common,
		},
	}
}
