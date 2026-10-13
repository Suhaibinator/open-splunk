// Package toolrejection carries non-executable model-output rejections between adapters and agents.
package toolrejection

import (
	"errors"
	"fmt"

	"github.com/Suhaibinator/go-common/pkg/proto_models"
	"github.com/firebase/genkit/go/ai"
)

const metadataKey = "go-common.rejected-tool-arguments"

// Part carries a rejection without exposing an executable ToolRequest to Genkit.
func Part(rejected *proto_models.RejectedToolArguments) *ai.Part {
	p := ai.NewTextPart("")
	p.Metadata = map[string]any{metadataKey: rejected}
	return p
}

// FromPart returns only an in-process rejection, never a JSON-decoded lookalike.
func FromPart(p *ai.Part) *proto_models.RejectedToolArguments {
	if p == nil {
		return nil
	}
	r, _ := p.Metadata[metadataKey].(*proto_models.RejectedToolArguments)
	return r
}

// Error retains the complete validated terminal envelope, including usage and exact replay state.
// Retrying the same request is unsafe: only an agent that persists corrective feedback may recover.
type Error struct{ Response *ai.ModelResponse }

func (e *Error) Error() string {
	if e.Response != nil && e.Response.Message != nil {
		for _, p := range e.Response.Message.Content {
			if r := FromPart(p); r != nil {
				return fmt.Sprintf("decode arguments for function %q: %s", r.ProviderToolName, r.Diagnostic)
			}
		}
	}
	return "invalid tool-call arguments"
}
func (*Error) Retryable() bool { return false }

// ResponseFromError unwraps the explicit adapter failure; ordinary errors are never recovered.
func ResponseFromError(err error) (*ai.ModelResponse, bool) {
	var rejected *Error
	if errors.As(err, &rejected) && rejected.Response != nil {
		return rejected.Response, true
	}
	return nil, false
}
