// Package openaichat implements an OpenAI Chat Completions provider for Firebase
// Genkit on openai-go v3. It serves OpenAI and every OpenAI-compatible gateway
// (OpenRouter, Ollama Cloud, Baseten, Bedrock's compatible endpoint, ...) through
// one stateless adapter: Genkit owns messages, tools, tool choice, and the output
// format; the caller's config tunes the remaining request fields.
package openaichat

import (
	"context"
	"fmt"
	"net/http"
	"strings"

	"github.com/Suhaibinator/go-common/pkg/genai/bodylimit"
	"github.com/firebase/genkit/go/ai"
	"github.com/firebase/genkit/go/core/api"
	openai "github.com/openai/openai-go/v3"
	"github.com/openai/openai-go/v3/option"
	"go.uber.org/zap"
)

// DefaultProvider is the Genkit provider namespace used when PluginOptions.Provider is empty.
const DefaultProvider = "chat-completions"

// PluginOptions configures the Chat Completions provider.
type PluginOptions struct {
	// Logger records complete model-facing payloads at DEBUG. Per-call WithDiagnosticLogger takes precedence.
	Logger *zap.Logger
	// Provider is the Genkit namespace prefixed to every model name, for example
	// "openrouter" in "openrouter/openai/gpt-test". Empty uses DefaultProvider.
	Provider string
	APIKey   string
	// BaseURL is the API root. The SDK appends the chat/completions endpoint. For
	// example, use https://api.openai.com/v1/ for the public OpenAI API.
	BaseURL string
	// HTTPClient is primarily useful for custom transports and tests.
	HTTPClient *http.Client
	// OpenRouterRouting admits OpenRouter's provider routing object and session
	// pinning field in model configuration. Both are rejected by the config schema
	// when disabled, so a routing preference can never reach a provider that does
	// not implement it.
	OpenRouterRouting bool
}

// Plugin is a Genkit DynamicPlugin that resolves arbitrary model IDs.
type Plugin struct {
	logger       *zap.Logger
	provider     string
	client       openai.Client
	configSchema map[string]any // Genkit's ModelOptions.ConfigSchema is a JSON-schema map.
}

var _ api.Plugin = (*Plugin)(nil)
var _ api.DynamicPlugin = (*Plugin)(nil)

// New constructs a Chat Completions plugin.
func New(opts PluginOptions) (*Plugin, error) {
	return newWithResponseBodyLimit(opts, bodylimit.MaxResponseBodyBytes)
}

func newWithResponseBodyLimit(opts PluginOptions, responseBodyLimit int64) (*Plugin, error) {
	if strings.TrimSpace(opts.APIKey) == "" {
		return nil, fmt.Errorf("openaichat: APIKey is required")
	}
	if responseBodyLimit <= 0 {
		return nil, fmt.Errorf("openaichat: response body limit must be positive")
	}
	provider := strings.TrimSpace(opts.Provider)
	if provider == "" {
		provider = DefaultProvider
	}

	// The SDK reads OPENAI_API_KEY, OPENAI_ORG_ID, and OPENAI_PROJECT_ID from the
	// environment for every client. Clear them first so a plugin pointed at another
	// vendor never sends OpenAI's identity there; the options below win over the scrub.
	clientOpts := []option.RequestOption{
		option.WithAPIKey(""), option.WithHeaderDel("Authorization"),
		option.WithOrganization(""), option.WithHeaderDel("OpenAI-Organization"),
		option.WithProject(""), option.WithHeaderDel("OpenAI-Project"),
		option.WithAPIKey(opts.APIKey), option.WithMaxRetries(0),
	}
	if baseURL := strings.TrimSpace(opts.BaseURL); baseURL != "" {
		if !strings.HasSuffix(baseURL, "/") {
			baseURL += "/"
		}
		clientOpts = append(clientOpts, option.WithBaseURL(baseURL))
	}
	clientOpts = append(clientOpts, option.WithHTTPClient(bodylimit.Client(opts.HTTPClient, responseBodyLimit)))

	return &Plugin{
		client:       openai.NewClient(clientOpts...),
		logger:       opts.Logger,
		provider:     provider,
		configSchema: configSchema(opts.OpenRouterRouting),
	}, nil
}

// Name implements api.Plugin.
func (p *Plugin) Name() string { return p.provider }

// Init implements api.Plugin. Models are resolved lazily through ResolveAction.
func (*Plugin) Init(context.Context) []api.Action { return nil }

// ListActions implements api.DynamicPlugin. Compatible gateways serve open-ended
// model IDs behind one exact endpoint, so nothing is enumerated; callers use
// ResolveAction for the desired ID.
func (*Plugin) ListActions(context.Context) []api.ActionDesc { return nil }

// ResolveAction implements api.DynamicPlugin and accepts any non-empty model ID.
func (p *Plugin) ResolveAction(atype api.ActionType, id string) api.Action {
	if atype != api.ActionTypeModel || strings.TrimSpace(id) == "" {
		return nil
	}
	model := p.newModel(strings.TrimSpace(id))
	action, _ := model.(api.Action)
	return action
}

func (p *Plugin) newModel(id string) ai.Model {
	return ai.NewModel(api.NewName(p.provider, id), p.modelOptions(id), func(ctx context.Context, req *ai.ModelRequest, cb ai.ModelStreamCallback) (*ai.ModelResponse, error) {
		return p.generate(ctx, id, req, cb)
	})
}

// ModelRef creates a reference to a Chat Completions model under provider. A
// name already carrying the namespace is preserved; every other name is
// qualified, because gateway model IDs such as "deepseek/deepseek-v4" contain
// slashes of their own.
func ModelRef(provider, name string, config *openai.ChatCompletionNewParams) ai.ModelRef {
	if provider == "" {
		provider = DefaultProvider
	}
	if !strings.HasPrefix(name, provider+"/") {
		name = api.NewName(provider, name)
	}
	return ai.NewModelRef(name, config)
}

func (p *Plugin) modelOptions(id string) *ai.ModelOptions {
	return &ai.ModelOptions{
		Label: p.provider + " - " + id,
		Stage: ai.ModelStageStable,
		// Genkit validates every request's config against this schema before the
		// model function runs, so unknown and framework-owned fields fail there.
		ConfigSchema: p.configSchema,
		Supports: &ai.ModelSupports{
			Constrained: ai.ConstrainedSupportAll,
			ContentType: []string{"text/plain", "application/json"},
			Media:       true,
			Multiturn:   true,
			Output:      []string{"text", "json"},
			SystemRole:  true,
			ToolChoice:  true,
			Tools:       true,
		},
	}
}
