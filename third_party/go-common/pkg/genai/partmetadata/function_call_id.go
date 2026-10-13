package partmetadata

const (
	functionCallIDMetadataKey      = "go-common.vertex.function_call_id"
	functionCallIDKnownMetadataKey = "go-common.vertex.function_call_id_known"
)

// SetFunctionCallID records the exact Vertex FunctionCall.ID on a Genkit part.
func SetFunctionCallID(metadata map[string]any, id string) map[string]any {
	if metadata == nil {
		metadata = make(map[string]any, 2)
	}
	metadata[functionCallIDMetadataKey] = id
	metadata[functionCallIDKnownMetadataKey] = true
	return metadata
}

// FunctionCallID returns the exact provider call ID and whether it is known.
func FunctionCallID(metadata map[string]any) (string, bool) {
	if metadata == nil {
		return "", false
	}
	known, ok := metadata[functionCallIDKnownMetadataKey].(bool)
	if !ok || !known {
		return "", false
	}
	id, ok := metadata[functionCallIDMetadataKey].(string)
	return id, ok
}
