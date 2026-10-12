package openaichat

import (
	json "encoding/json/v2"
	"maps"
	"reflect"
	"sync"

	"github.com/Suhaibinator/go-common/pkg/genai/modelconfig"
	"github.com/invopop/jsonschema"
	openai "github.com/openai/openai-go/v3"
)

// Genkit advertises and enforces model configuration as a JSON-schema map; the
// map[string]any values in this file are that framework contract.

// sdkConfigSchema reflects the SDK request type once. Reflection is expensive
// and the result is read-only, so every model shares it. Unions inline a value
// that marshals as one of several shapes, which the reflector cannot know, so
// the ones a config may legitimately carry are mapped here; the framework-owned
// unions are replaced by the managed-field schema below instead.
var sdkConfigSchema = sync.OnceValue(func() map[string]any {
	reflector := jsonschema.Reflector{
		DoNotReference:             true,
		AllowAdditionalProperties:  false,
		ExpandedStruct:             true,
		RequiredFromJSONSchemaTags: true,
	}
	stringSchema := &jsonschema.Schema{Type: "string"}
	stringOrList := &jsonschema.Schema{AnyOf: []*jsonschema.Schema{
		stringSchema,
		{Type: "array", Items: stringSchema},
	}}
	reflector.Mapper = func(t reflect.Type) *jsonschema.Schema {
		switch t.Name() {
		case "Opt[float64]":
			return &jsonschema.Schema{Type: "number"}
		case "Opt[int64]":
			return &jsonschema.Schema{Type: "integer"}
		case "Opt[string]":
			return stringSchema
		case "Opt[bool]":
			return &jsonschema.Schema{Type: "boolean"}
		case "ChatCompletionNewParamsStopUnion":
			return stringOrList
		case "ChatCompletionAudioParamVoiceUnion":
			return stringSchema
		case "ChatCompletionPredictionContentContentUnionParam":
			return &jsonschema.Schema{AnyOf: []*jsonschema.Schema{
				stringSchema,
				{Type: "array", Items: &jsonschema.Schema{Type: "object"}},
			}}
		}
		return nil
	}
	encoded, err := json.Marshal(reflector.Reflect(openai.ChatCompletionNewParams{}))
	if err != nil {
		panic("openaichat: encode reflected config schema: " + err.Error())
	}
	var schema map[string]any
	if err := json.Unmarshal(encoded, &schema); err != nil {
		panic("openaichat: decode reflected config schema: " + err.Error())
	}
	stripParamObjectArtifact(schema)
	properties := schema["properties"].(map[string]any)
	properties["model"] = map[string]any{
		"type":        "string",
		"description": "Pins the exact model version the request is served by. When unset, the request is served by the model named in the action.",
	}
	// Genkit builds these from the request; a config naming one of them is
	// rejected at the schema boundary rather than silently dropped. The response
	// path serves the first choice only, so n is refused too.
	for _, field := range []string{"tool_choice", "function_call", "response_format", "n"} {
		properties[field] = map[string]any{"type": "null"}
	}
	for _, field := range []string{"messages", "tools", "functions"} {
		properties[field] = map[string]any{"anyOf": []any{map[string]any{"type": "null"}, map[string]any{"type": "array", "maxItems": 0}}}
	}
	return schema
})

// stripParamObjectArtifact removes the "any" property the SDK's embedded
// param.APIObject (an anonymous any carrying the raw decoded message) reflects
// into every object in the schema. It is machinery rather than a request field.
func stripParamObjectArtifact(schema map[string]any) {
	if properties, ok := schema["properties"].(map[string]any); ok {
		delete(properties, "any")
		for _, sub := range properties {
			if nested, ok := sub.(map[string]any); ok {
				stripParamObjectArtifact(nested)
			}
		}
	}
	for _, key := range []string{"items", "additionalProperties"} {
		if nested, ok := schema[key].(map[string]any); ok {
			stripParamObjectArtifact(nested)
		}
	}
	for _, key := range []string{"anyOf", "oneOf", "allOf"} {
		branches, _ := schema[key].([]any)
		for _, branch := range branches {
			if nested, ok := branch.(map[string]any); ok {
				stripParamObjectArtifact(nested)
			}
		}
	}
}

// configSchema is the SDK schema plus, for OpenRouter routes, the routing
// fields modelconfig.OpenRouterConfig sends. The root stays closed so a typo or
// a routing preference aimed at a provider that lacks the feature fails before
// the request is built.
func configSchema(openRouterRouting bool) map[string]any {
	schema := maps.Clone(sdkConfigSchema())
	if !openRouterRouting {
		return schema
	}
	properties := maps.Clone(schema["properties"].(map[string]any))
	// Untyped configs accept exactly the provider fields modelconfig sends;
	// unknown routing fields stay rejected.
	properties["provider"] = modelconfig.OpenRouterProviderSchema()
	properties["session_id"] = map[string]any{"type": "string"}
	schema["properties"] = properties
	return schema
}
