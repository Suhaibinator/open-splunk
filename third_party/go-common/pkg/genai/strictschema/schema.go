// Package strictschema applies the object policy shared by OpenAI adapters.
package strictschema

import "sort"

// Close recursively closes object schemas. requireAll distinguishes structured
// output (every property required) from tools (the author owns required).
// The caller supplies an independent JSON-safe copy.
func Close(schema map[string]any, requireAll bool) {
	for _, key := range []string{"properties", "patternProperties", "dependentSchemas", "$defs", "definitions"} {
		children, _ := schema[key].(map[string]any)
		for _, child := range children {
			if nested, ok := child.(map[string]any); ok {
				Close(nested, requireAll)
			}
		}
	}
	for _, key := range []string{"items", "additionalProperties", "contains", "not", "if", "then", "else", "propertyNames", "unevaluatedProperties", "contentSchema"} {
		if nested, ok := schema[key].(map[string]any); ok {
			Close(nested, requireAll)
		}
	}
	for _, key := range []string{"items", "prefixItems", "allOf", "anyOf", "oneOf"} {
		children, _ := schema[key].([]any)
		for _, child := range children {
			if nested, ok := child.(map[string]any); ok {
				Close(nested, requireAll)
			}
		}
	}
	properties, hasProperties := schema["properties"].(map[string]any)
	object := schema["type"] == "object"
	if types, ok := schema["type"].([]any); ok {
		for _, typ := range types {
			if typ == "object" {
				object = true
			}
		}
	}
	if !object && !hasProperties {
		return
	}
	schema["additionalProperties"] = false
	if requireAll {
		required := make([]string, 0, len(properties))
		for name := range properties {
			required = append(required, name)
		}
		sort.Strings(required)
		schema["required"] = required
	}
}
