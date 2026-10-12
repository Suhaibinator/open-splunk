package ai

import genkitai "github.com/firebase/genkit/go/ai"

// HasOutputContent identifies text or tool output, excluding empty and reasoning-only chunks.
func HasOutputContent(chunk *genkitai.ModelResponseChunk) bool {
	if chunk == nil {
		return false
	}
	for _, part := range chunk.Content {
		if part.IsText() && part.Text != "" || part.IsToolRequest() && part.ToolRequest != nil {
			return true
		}
	}
	return false
}
