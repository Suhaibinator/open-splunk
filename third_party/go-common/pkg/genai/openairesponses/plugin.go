// Package openairesponses implements a native OpenAI Responses API provider for
// Firebase Genkit.
package openairesponses

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

const (
	// ProviderID is the Genkit provider namespace used by this plugin.
	ProviderID = "openai"

	// MetadataOutputItemsJSON is the message metadata key containing the exact JSON
	// representation of a Responses API output array. It enables lossless,
	// stateless replay on the next turn.
	MetadataOutputItemsJSON = "openai.responses.output_items_json"
	// MetadataOutputJSON is retained as a descriptive alias.
	MetadataOutputJSON = MetadataOutputItemsJSON

	// MetadataResponseID is the message metadata key containing the originating
	// Responses API response ID.
	MetadataResponseID = "openai.responses.response_id"

	// MetadataProjectionHash is the message metadata key containing the SHA-256
	// hash of the Genkit content projected from MetadataOutputJSON. Replay is only
	// used while this hash still matches the message content.
	MetadataProjectionHash = "openai.responses.projection_sha256"

	// MetadataReplayVerified is the key under which a durable persistence boundary may place
	// common_service_models.NewOpenAIResponsesReplayAttestation after checking projection,
	// digest, ownership, and tool manifest. Scalar/JSON values under this key are never trusted.
	MetadataReplayVerified = "openai.responses.replay_verified"

	metadataItemID   = "openai.responses.item_id"
	metadataToolName = "openai.responses.tool_name"
)

// The body limit is shared with the Chat Completions adapter; see bodylimit.
const maxResponsesResponseBodyBytes = bodylimit.MaxResponseBodyBytes

var errResponsesResponseBodyTooLarge = bodylimit.ErrResponseBodyTooLarge

// PluginOptions configures the OpenAI Responses provider.
type PluginOptions struct {
	// Logger records complete model-facing payloads at DEBUG. Per-call WithDiagnosticLogger takes precedence.
	Logger *zap.Logger
	APIKey string
	// BaseURL is the API root. The SDK appends the responses endpoint. For
	// example, use https://api.openai.com/v1/ for the public OpenAI API.
	BaseURL string
	// HTTPClient is primarily useful for custom transports and tests.
	HTTPClient *http.Client
	// AllowHostedImageGeneration permits native Responses image_generation
	// tools. It is disabled by default because callers must be able to persist
	// media-only model output before enabling the tool.
	AllowHostedImageGeneration bool
	// AllowAtLeastOnceHostedMCPExecution permits native Responses MCP tools only when callers
	// explicitly accept that provider-side MCP actions are not fenced by go-common's durable tool
	// loop and may execute more than once after a network, process, or higher-level generation
	// retry. It is disabled by default. Even when enabled, require_approval must be "never" because
	// this plugin does not implement MCP approval round-trips.
	AllowAtLeastOnceHostedMCPExecution bool
}

// Plugin is a Genkit DynamicPlugin that resolves arbitrary OpenAI model IDs.
type Plugin struct {
	logger                     *zap.Logger
	client                     openai.Client
	allowHostedImageGeneration bool
	allowAtLeastOnceHostedMCP  bool
}

var _ api.Plugin = (*Plugin)(nil)
var _ api.DynamicPlugin = (*Plugin)(nil)

// New constructs an OpenAI Responses API plugin.
func New(opts PluginOptions) (*Plugin, error) {
	return newWithResponseBodyLimit(opts, maxResponsesResponseBodyBytes)
}

func newWithResponseBodyLimit(opts PluginOptions, responseBodyLimit int64) (*Plugin, error) {
	if strings.TrimSpace(opts.APIKey) == "" {
		return nil, fmt.Errorf("openairesponses: APIKey is required")
	}
	if responseBodyLimit <= 0 {
		return nil, fmt.Errorf("openairesponses: response body limit must be positive")
	}

	clientOpts := []option.RequestOption{option.WithAPIKey(opts.APIKey), option.WithMaxRetries(0)}
	if baseURL := strings.TrimSpace(opts.BaseURL); baseURL != "" {
		if !strings.HasSuffix(baseURL, "/") {
			baseURL += "/"
		}
		clientOpts = append(clientOpts, option.WithBaseURL(baseURL))
	}
	clientOpts = append(clientOpts, option.WithHTTPClient(bodylimit.Client(opts.HTTPClient, responseBodyLimit)))

	return &Plugin{
		client:                     openai.NewClient(clientOpts...),
		logger:                     opts.Logger,
		allowHostedImageGeneration: opts.AllowHostedImageGeneration,
		allowAtLeastOnceHostedMCP:  opts.AllowAtLeastOnceHostedMCPExecution,
	}, nil
}

// Name implements api.Plugin.
func (*Plugin) Name() string { return ProviderID }

// Init implements api.Plugin. Models are resolved lazily through ResolveAction.
func (*Plugin) Init(context.Context) []api.Action { return nil }

// ListActions implements api.DynamicPlugin. OpenAI model IDs are intentionally
// open-ended, so callers should use ResolveAction for the desired ID.
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
	return ai.NewModel(api.NewName(ProviderID, id), modelOptions(id), func(ctx context.Context, req *ai.ModelRequest, cb ai.ModelStreamCallback) (*ai.ModelResponse, error) {
		return p.generate(ctx, id, req, cb)
	})
}

// ModelRef creates a reference to an OpenAI Responses model. A bare model ID is
// automatically qualified with the openai provider namespace; an already
// qualified name is preserved.
func ModelRef(name string, config *Config) ai.ModelRef {
	if provider, _ := api.ParseName(name); provider == "" {
		name = api.NewName(ProviderID, name)
	}
	return ai.NewModelRef(name, config)
}

func modelOptions(id string) *ai.ModelOptions {
	return &ai.ModelOptions{
		Label:        "OpenAI " + id + " (Responses API)",
		Stage:        ai.ModelStageUnstable,
		ConfigSchema: configSchema(),
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
