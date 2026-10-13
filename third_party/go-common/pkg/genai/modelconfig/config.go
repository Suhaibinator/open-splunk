// Package modelconfig defines the typed, process-local configuration accepted by
// agent model runtimes. SDK types match the versions used by each adapter.
package modelconfig

import (
	"encoding/json/jsontext"
	"fmt"

	"github.com/Suhaibinator/go-common/pkg/genai/apishape"
	pb "github.com/Suhaibinator/go-common/pkg/proto_models"
	anthropic "github.com/anthropics/anthropic-sdk-go"
	"github.com/firebase/genkit/go/ai"
	openai "github.com/openai/openai-go/v3"
	openaiparam "github.com/openai/openai-go/v3/packages/param"
	"github.com/openai/openai-go/v3/responses"
	azure "github.com/xavidop/genkit-azure-foundry-go"
	google "google.golang.org/genai"
)

// Config contains exactly one API's configuration. Nil means no provider
// overrides. Common is only for an unresolved/default API; explicit APIs use
// their native branch. Configurations must not be mutated after publication.
type Config struct {
	ChatCompletions *openai.ChatCompletionNewParams
	Responses       *ResponsesConfig
	Anthropic       *anthropic.MessageNewParams
	Google          *google.GenerateContentConfig
	Azure           *azure.GenerationConfig
	Common          *ai.GenerationCommonConfig
	OpenRouter      *OpenRouterConfig
}

// ResponsesConfig holds the native Responses request and Genkit tool bindings.
// Model, input and Genkit-owned tools are supplied by the adapter.
type ResponsesConfig struct {
	Common                 *ai.GenerationCommonConfig  `json:"common,omitempty"`
	Params                 responses.ResponseNewParams `json:"params"`
	ClientTools            []ClientToolBinding         `json:"clientTools,omitempty"`
	RequestReasoningTraces *bool                       `json:"requestReasoningTraces,omitempty"`
}

// ClientToolBinding preserves the Genkit name for a native function tool.
type ClientToolBinding struct {
	GenkitToolName string                   `json:"genkitToolName"`
	Tool           responses.ToolUnionParam `json:"tool"`
}

// Validate rejects ambiguous configurations and API mismatches before generation.
// An unspecified API defers the match check until the runtime resolves it.
func (c *Config) Validate(api pb.InferenceAPI) error {
	if c == nil {
		return nil
	}
	count := 0
	selected := apishape.Default
	for _, branch := range []struct {
		present bool
		api     pb.InferenceAPI
	}{
		{c.ChatCompletions != nil, apishape.ChatCompletions},
		{c.Responses != nil, apishape.Responses},
		{c.Anthropic != nil, apishape.AnthropicMessages},
		{c.Google != nil, apishape.GoogleNative},
		{c.Azure != nil, apishape.AzureNative},
		{c.Common != nil, apishape.Default},
	} {
		if branch.present {
			count++
			selected = branch.api
		}
	}
	if count != 1 {
		return fmt.Errorf("model config must contain exactly one API branch (got %d)", count)
	}
	if api != apishape.Default && selected != api {
		return fmt.Errorf("model config is for %s, selected API is %s", selected, api)
	}
	if c.OpenRouter != nil && selected != apishape.ChatCompletions && selected != apishape.Responses {
		return fmt.Errorf("OpenRouter options require Chat Completions or Responses config")
	}
	// A whole-object override must be a JSON object, so route policy, provider
	// preferences and Native can patch its members.
	var extra map[string]any
	var members map[string]jsontext.Value
	var err error
	switch {
	case c.ChatCompletions != nil:
		extra = c.ChatCompletions.ExtraFields()
		members, _, err = OverrideMembers(*c.ChatCompletions)
	case c.Responses != nil:
		extra = c.Responses.Params.ExtraFields()
		members, _, err = OverrideMembers(c.Responses.Params)
	case c.Anthropic != nil:
		_, _, err = OverrideMembers(*c.Anthropic)
	}
	if err != nil {
		return fmt.Errorf("model config: %w", err)
	}
	for _, key := range openRouterOwnedFields {
		if _, ok := extra[key]; ok {
			return fmt.Errorf("model config extra field %q is owned by OpenRouterConfig; set it there instead", key)
		}
		if _, ok := members[key]; ok {
			return fmt.Errorf("model config override field %q is owned by OpenRouterConfig; set it there instead", key)
		}
	}
	return c.OpenRouter.validate()
}

// Native unwraps a validated configuration at Genkit's WithConfig boundary.
// The interface and extra-fields map here are required by the third-party SDK;
// callers and agent configuration builders use Config throughout.
func (c *Config) Native() any {
	if c == nil {
		return nil
	}
	switch {
	case c.ChatCompletions != nil:
		cfg := *c.ChatCompletions
		if c.OpenRouter == nil {
			return cfg
		}
		if encoded, overridden := openRouterOverride(c.OpenRouter, cfg); !overridden {
			cfg.SetExtraFields(c.OpenRouter.extraFields(cfg.ExtraFields()))
		} else if encoded != nil {
			openaiparam.SetJSON(encoded, &cfg)
		}
		return cfg
	case c.Responses != nil:
		cfg := *c.Responses
		if c.OpenRouter == nil {
			return cfg
		}
		if encoded, overridden := openRouterOverride(c.OpenRouter, cfg.Params); !overridden {
			cfg.Params.SetExtraFields(c.OpenRouter.extraFields(cfg.Params.ExtraFields()))
		} else if encoded != nil {
			openaiparam.SetJSON(encoded, &cfg.Params)
		}
		return cfg
	case c.Anthropic != nil:
		return c.Anthropic
	case c.Google != nil:
		return c.Google
	case c.Azure != nil:
		return c.Azure
	case c.Common != nil:
		return c.Common
	default:
		return nil
	}
}
