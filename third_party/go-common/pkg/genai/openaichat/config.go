package openaichat

import (
	"encoding/json/jsontext"
	json "encoding/json/v2"
	"fmt"
	"reflect"
	"strings"
	"sync"

	openai "github.com/openai/openai-go/v3"
)

// Config is the model configuration accepted by this provider: the native Chat
// Completions request. Fields owned by Genkit (messages, tools, tool_choice,
// response_format and their legacy function forms) are derived from
// ai.ModelRequest and rejected when a config sets them; n is rejected because
// only the first choice is projected. A non-empty Model pins the exact model
// version the request is served by.
type Config = openai.ChatCompletionNewParams

// managedRequestFields are the request fields Genkit owns. The advertised
// schema only admits them as null or empty, and buildRequest rejects any value
// that slips through a typed config.
var managedRequestFields = []string{"messages", "tools", "tool_choice", "functions", "function_call", "response_format", "n"}

// ConfigFrom converts a Genkit model config value into Config. It accepts the
// typed SDK params go-common produces (modelconfig.Config.Native, ModelRef) and,
// for JSON callers such as the Genkit Dev UI, a map[string]any in the request's
// own JSON shape (snake_case wire names). Keys the SDK does not declare are
// retained as extra request fields, because the SDK decoder would otherwise
// drop them silently; the model action's config schema decides which of those
// keys a provider admits before this function runs. The parameter is Genkit's
// untyped config slot.
func ConfigFrom(v any) (Config, error) {
	switch cfg := v.(type) {
	case nil:
		return Config{}, nil
	case Config:
		return normalizeTypedConfig(cfg)
	case *Config:
		if cfg == nil {
			return Config{}, nil
		}
		return normalizeTypedConfig(*cfg)
	case map[string]any:
		b, err := json.Marshal(cfg)
		if err != nil {
			return Config{}, fmt.Errorf("openaichat: encode config: %w", err)
		}
		return configFromJSON(b)
	}
	return Config{}, fmt.Errorf("openaichat: unsupported config type %T; use openaichat.Config", v)
}

// Native SDK extras override declared fields during marshaling, and a
// whole-object override replaces every typed field. Materialize either before
// route middleware and buildRequest update the typed fields, preserving
// unrelated extras and override members as extra fields.
func normalizeTypedConfig(cfg Config) (Config, error) {
	for _, field := range managedRequestFields {
		if _, set := cfg.ExtraFields()[field]; set {
			return Config{}, fmt.Errorf("openaichat: extra field %q is built by Genkit from the request and cannot be set from config", field)
		}
	}
	_, materialize := cfg.Overrides()
	for key := range cfg.ExtraFields() {
		if _, declared := chatCompletionParamFields()[key]; declared {
			materialize = true
		}
	}
	if !materialize {
		return cfg, nil
	}
	encoded, err := json.Marshal(cfg)
	if err != nil {
		return Config{}, fmt.Errorf("openaichat: encode native config: %w", err)
	}
	return configFromJSON(encoded)
}

func configFromJSON(b []byte) (Config, error) {
	var fields map[string]jsontext.Value
	if err := json.Unmarshal(b, &fields); err != nil {
		return Config{}, fmt.Errorf("openaichat: config must be a JSON object: %w", err)
	}
	if fields == nil {
		return Config{}, fmt.Errorf("openaichat: config must be a JSON object")
	}
	var cfg Config
	if err := json.Unmarshal(b, &cfg); err != nil {
		return Config{}, fmt.Errorf("openaichat: decode native Chat Completions params: %w", err)
	}
	declared := chatCompletionParamFields()
	var extra map[string]any // SetExtraFields requires this map; values stay raw JSON.
	for key, value := range fields {
		if _, ok := declared[key]; ok {
			continue
		}
		if extra == nil {
			extra = make(map[string]any)
		}
		extra[key] = value
	}
	if extra != nil {
		cfg.SetExtraFields(extra)
	}
	return cfg, nil
}

// chatCompletionParamFields is the set of wire names the SDK request declares,
// used to tell a provider-specific extra apart from a field the SDK decoded.
var chatCompletionParamFields = sync.OnceValue(func() map[string]struct{} {
	paramsType := reflect.TypeFor[Config]()
	fields := make(map[string]struct{}, paramsType.NumField())
	for field := range paramsType.Fields() {
		name, _, _ := strings.Cut(field.Tag.Get("json"), ",")
		if name != "" && name != "-" {
			fields[name] = struct{}{}
		}
	}
	return fields
})

// rejectManagedConfig reports a config field a Genkit primitive owns, whether
// it arrived typed or as an extra field. Each one would be overwritten while
// the request is built, so failing with the option to use instead beats a
// request that silently ignores half of what it was given.
func rejectManagedConfig(cfg *Config) error {
	switch {
	case len(cfg.Messages) > 0:
		return fmt.Errorf("openaichat: messages must be set using Genkit feature: ai.WithMessages() or ai.WithPrompt()")
	case len(cfg.Tools) > 0 || len(cfg.Functions) > 0:
		return fmt.Errorf("openaichat: tools must be set using Genkit feature: ai.WithTools()")
	case cfg.ToolChoice.OfAuto.Valid() || cfg.ToolChoice.OfAllowedTools != nil || cfg.ToolChoice.OfFunctionToolChoice != nil || cfg.ToolChoice.OfCustomToolChoice != nil ||
		cfg.FunctionCall.OfFunctionCallMode.Valid() || cfg.FunctionCall.OfFunctionCallOption != nil:
		return fmt.Errorf("openaichat: tool choice must be set using Genkit feature: ai.WithToolChoice()")
	case cfg.ResponseFormat.OfText != nil || cfg.ResponseFormat.OfJSONSchema != nil || cfg.ResponseFormat.OfJSONObject != nil:
		return fmt.Errorf("openaichat: output format must be set using Genkit feature: ai.WithOutputType(), ai.WithOutputFormat(), or ai.WithOutputInstructions()")
	case cfg.N.Valid():
		return fmt.Errorf("openaichat: n is not supported: the response carries the first candidate only, so extra candidates would be billed and dropped")
	}
	extra := cfg.ExtraFields()
	for _, field := range managedRequestFields {
		if _, ok := extra[field]; ok {
			return fmt.Errorf("openaichat: extra field %q is built by Genkit from the request and cannot be set from config", field)
		}
	}
	return nil
}
