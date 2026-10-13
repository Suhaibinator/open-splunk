package common_service

import (
	"context"

	"github.com/Suhaibinator/go-common/pkg/common_service/agentkit"
	commonai "github.com/Suhaibinator/go-common/pkg/common_service/ai"
	"github.com/Suhaibinator/go-common/pkg/common_service/common_service_models"
	"github.com/Suhaibinator/go-common/pkg/common_service/ports"
	genkitai "github.com/firebase/genkit/go/ai"
)

type actorlessGenerator[U common_service_models.UserLike] struct {
	commonai.Generator
}

func actorlessInferenceGenerator[U common_service_models.UserLike](generator commonai.Generator) commonai.Generator {
	isolated := actorlessGenerator[U]{Generator: generator}
	if streaming, present := generator.(commonai.StreamingGenerator); present {
		return actorlessStreamingGenerator[U]{actorlessGenerator: isolated, streaming: streaming}
	}
	return isolated
}

func (g actorlessGenerator[U]) Generate(ctx context.Context, step commonai.GenerationStep, history []*genkitai.Message, tools []ports.AgentToolDefinition) (*commonai.GenerationOutcome, error) {
	return g.Generator.Generate(agentkit.ContextWithoutActor[U](ctx), step, history, tools)
}

type actorlessStreamingGenerator[U common_service_models.UserLike] struct {
	actorlessGenerator[U]
	streaming commonai.StreamingGenerator
}

func (g actorlessStreamingGenerator[U]) GenerateStream(ctx context.Context, step commonai.GenerationStep, history []*genkitai.Message, tools []ports.AgentToolDefinition, onTextDelta func(string) error) (*commonai.GenerationOutcome, error) {
	return g.streaming.GenerateStream(agentkit.ContextWithoutActor[U](ctx), step, history, tools, onTextDelta)
}
