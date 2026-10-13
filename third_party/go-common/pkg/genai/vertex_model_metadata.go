package genai

import (
	"fmt"
	"strings"

	"github.com/firebase/genkit/go/ai"
	genkitgooglegenai "github.com/firebase/genkit/go/plugins/googlegenai"
)

const vertexAIProviderName = "vertexai"

var (
	vertexAIModelSupports = ai.ModelSupports{
		Multiturn:   true,
		Tools:       true,
		ToolChoice:  true,
		SystemRole:  true,
		Media:       true,
		Constrained: ai.ConstrainedSupportAll,
	}
	vertexAIConfigSchema = map[string]any{
		"type": "object",
	}
)

// vertexAIModelIDSupportsGenerateContent reports whether modelID can safely use
// this plugin's conversational GenerateContent adapter. Keep the primary model
// family classification aligned with the pinned Genkit Google GenAI plugin,
// whose other families use different APIs or action types.
func vertexAIModelIDSupportsGenerateContent(modelID string) bool {
	modelID = strings.TrimSpace(strings.TrimPrefix(modelID, vertexAIProviderName+"/"))
	if genkitgooglegenai.ClassifyModel(modelID) != genkitgooglegenai.ModelTypeGemini {
		return false
	}

	// These Gemini-prefixed models require specialized media/live defaults that
	// the custom chat adapter intentionally does not synthesize.
	normalized := strings.ToLower(modelID)
	return !strings.Contains(normalized, "-tts") &&
		!strings.Contains(normalized, "-image") &&
		!strings.Contains(normalized, "-live") &&
		!strings.Contains(normalized, "native-audio")
}

// vertexAIModelOptions performs its package-specific work in the GenAI model resolution and runtime construction flow.
func vertexAIModelOptions(modelID string) ai.ModelOptions {
	supports := vertexAIModelSupports
	upstreamOptions := genkitgooglegenai.GetModelOptions(modelID, vertexAIProviderName)
	return ai.ModelOptions{
		Label:        fmt.Sprintf("Vertex AI - %s", modelID),
		Stage:        upstreamOptions.Stage,
		Versions:     []string{},
		Supports:     &supports,
		ConfigSchema: vertexAIConfigSchema,
	}
}
