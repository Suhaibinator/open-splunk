package genai

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"sync"

	"github.com/Suhaibinator/go-common/pkg/common_service/common_service_models"
	"github.com/Suhaibinator/go-common/pkg/config"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
	genkitapi "github.com/firebase/genkit/go/core/api"
	"github.com/firebase/genkit/go/genkit"
	"github.com/firebase/genkit/go/plugins/googlegenai"
	azureaifoundry "github.com/xavidop/genkit-azure-foundry-go"
)

type ResolvedModel struct {
	OpenRouterRouting      bool
	API                    proto_models.InferenceAPI
	DestinationFingerprint string
	Genkit                 *genkit.Genkit
	ProviderID             string
	ModelName              string
	ProviderModelID        string
}

// RuntimeFactoryConfig is one coherent generation of provider definitions and application-owned credentials.
type RuntimeFactoryConfig struct {
	Providers   []config.ProviderConfig
	Credentials []config.CredentialBinding
}

// ProviderTransport returns the HTTP transport for one provider ID, or nil for
// http.DefaultTransport. go-common still targets the configured endpoint, applies
// the provider's authentication and headers, and refuses redirects; the transport
// only carries the request, so it is the place for mTLS, private CAs, proxies,
// custom dialers, or request signing (with the provider's auth set to NONE).
//
// The factory calls it while building a provider's runtime, under the factory's
// lock, and keeps the result for that runtime's life (until the provider's
// configuration changes). It must be fast and must not call back into the
// factory. Rotate certificates inside the transport, for example with
// tls.Config.GetClientCertificate. Only HTTP inference APIs (Chat Completions,
// Responses, Anthropic Messages) accept a transport; returning one for a Google
// or Azure native provider fails that provider's resolution.
type ProviderTransport func(providerID string) http.RoundTripper

// RuntimeFactoryOption configures a ModelRuntimeFactory at construction.
type RuntimeFactoryOption func(*ModelRuntimeFactory)

// WithProviderTransport installs the application's per-provider HTTP transport.
func WithProviderTransport(transport ProviderTransport) RuntimeFactoryOption {
	return func(f *ModelRuntimeFactory) { f.transport = transport }
}

type ModelRuntimeFactory struct {
	provider   func() (RuntimeFactoryConfig, error)
	transport  ProviderTransport
	hotContext context.Context
	hotMu      sync.Mutex
	hotClosed  bool
	apiEntries map[runtimeAPIKey]hotRuntimeEntry
}

type genkitProviderRuntime struct {
	openRouterRouting      bool
	namespace              string
	api                    proto_models.InferenceAPI
	destinationFingerprint string
	unavailableHint        string
	g                      *genkit.Genkit
	ensureModel            func(string) error
}

func NewModelRuntimeFactory(ctx context.Context, cfg RuntimeFactoryConfig, opts ...RuntimeFactoryOption) (*ModelRuntimeFactory, error) {
	cfg = cloneRuntimeConfig(cfg)
	return NewReloadingModelRuntimeFactory(ctx, func() (RuntimeFactoryConfig, error) { return cfg, nil }, opts...)
}

func (f *ModelRuntimeFactory) ResolveModel(model *common_service_models.InferenceModel) (*ResolvedModel, error) {
	return f.ResolveModelForAPI(model, 0)
}

func (f *ModelRuntimeFactory) ResolveModelForAPI(model *common_service_models.InferenceModel, api proto_models.InferenceAPI) (*ResolvedModel, error) {
	if f == nil {
		return nil, fmt.Errorf("model runtime factory is not configured")
	}
	if model == nil {
		return nil, fmt.Errorf("inference model is required")
	}
	modelID, err := NormalizeProviderModelID(model.ProviderID, model.ProviderModelID)
	if err != nil {
		return nil, err
	}
	runtime, err := f.runtimeForAPI(model.ProviderID, api)
	if err != nil {
		return nil, err
	}
	if runtime.g == nil {
		return nil, fmt.Errorf("provider %s: %s", model.ProviderID, runtime.unavailableHint)
	}
	name := runtime.namespace + "/" + modelID
	if runtime.ensureModel != nil {
		if err := runtime.ensureModel(name); err != nil {
			return nil, err
		}
	}
	return &ResolvedModel{OpenRouterRouting: runtime.openRouterRouting, API: runtime.api, DestinationFingerprint: runtime.destinationFingerprint, Genkit: runtime.g, ProviderID: model.ProviderID, ModelName: name, ProviderModelID: modelID}, nil
}

// newGoogleAIStudioRuntime performs its package-specific work in the GenAI model resolution and runtime construction flow.
func newGoogleAIStudioRuntime(
	ctx context.Context,
	apiKey string,
) (*genkitProviderRuntime, error) {
	runtime := &genkitProviderRuntime{
		unavailableHint: "set GEMINI_API_KEY, GOOGLE_API_KEY, or the service GeminiAPIKey",
	}
	if apiKey == "" {
		return runtime, nil
	}
	g, err := initGenkitWithPlugins(ctx, &googlegenai.GoogleAI{APIKey: apiKey})
	if err != nil {
		return nil, fmt.Errorf("failed to initialize google ai studio runtime: %w", err)
	}
	runtime.g = g
	return runtime, nil
}

// newVertexAIRuntimeWithInit performs its package-specific work in the GenAI model resolution and runtime construction flow.
//
// Parameters:
// - ctx (context.Context): Supplies input consumed by newVertexAIRuntimeWithInit during this step of the flow.
// - apiKey (string): Supplies input consumed by newVertexAIRuntimeWithInit during this step of the flow.
// - projectID (string): Supplies input consumed by newVertexAIRuntimeWithInit during this step of the flow.
// - location (string): Supplies input consumed by newVertexAIRuntimeWithInit during this step of the flow.
// - initFn (func(context.Context, ...genkitapi.Plugin) (*genkit.Genkit, error)): Supplies input consumed by newVertexAIRuntimeWithInit during this step of the flow.
func newVertexAIRuntimeWithInit(
	ctx context.Context,
	apiKey string,
	projectID string,
	location string,
	initFn func(context.Context, ...genkitapi.Plugin) (*genkit.Genkit, error),
) (*genkitProviderRuntime, error) {
	runtime := &genkitProviderRuntime{
		unavailableHint: "set VertexAIAPIKey, or set VertexAIProjectID and VertexAILocation for ADC credentials",
	}
	apiKey = strings.TrimSpace(apiKey)
	if apiKey != "" {
		g, err := initFn(ctx, newVertexAIAPIKeyPlugin(apiKey, projectID, location))
		if err != nil {
			return nil, fmt.Errorf("failed to initialize vertex ai api key runtime: %w", err)
		}
		runtime.g = g
		return runtime, nil
	}
	projectID, location = resolveVertexAIProjectAndLocation(projectID, location)
	if projectID == "" || location == "" {
		return runtime, nil
	}
	g, err := initFn(ctx, &googlegenai.VertexAI{
		ProjectID: projectID,
		Location:  location,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to initialize vertex ai runtime: %w", err)
	}
	runtime.g = g
	return runtime, nil
}

// newAzureAIFoundryRuntime performs its package-specific work in the GenAI model resolution and runtime construction flow.
func newAzureAIFoundryRuntime(
	ctx context.Context,
	apiKey string,
	endpoint string,
	apiVersion string,
) (*genkitProviderRuntime, error) {
	runtime := &genkitProviderRuntime{
		unavailableHint: "set AZURE_OPENAI_API_KEY and AZURE_OPENAI_BASE_URL",
	}
	if apiKey == "" || endpoint == "" {
		return runtime, nil
	}

	plugin := &azureaifoundry.AzureAIFoundry{
		Endpoint:   strings.TrimRight(endpoint, "/"),
		APIKey:     apiKey,
		APIVersion: apiVersion,
	}
	g, err := initGenkitWithPlugins(ctx, plugin)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize %s runtime: %w", "azure_foundry", err)
	}
	runtime.g = g
	runtime.ensureModel = ensureAzureAIFoundryModel(g, plugin)
	return runtime, nil
}

// initGenkitWithPlugins performs its package-specific work in the GenAI model resolution and runtime construction flow.
func initGenkitWithPlugins(ctx context.Context, plugins ...genkitapi.Plugin) (_ *genkit.Genkit, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("%v", recovered)
		}
	}()
	return genkit.Init(ctx, genkit.WithPlugins(plugins...)), nil
}

// ensureAzureAIFoundryModel performs its package-specific work in the GenAI model resolution and runtime construction flow.
func ensureAzureAIFoundryModel(g *genkit.Genkit, plugin *azureaifoundry.AzureAIFoundry) func(string) error {
	var mu sync.Mutex
	return func(modelName string) error {
		mu.Lock()
		defer mu.Unlock()
		fullModelName := strings.TrimSpace(modelName)
		if fullModelName == "" {
			return fmt.Errorf("model name is required")
		}
		if genkit.LookupModel(g, fullModelName) != nil {
			return nil
		}

		providerName := "azureaifoundry"
		deploymentName := strings.TrimPrefix(fullModelName, providerName+"/")
		if deploymentName == "" {
			return fmt.Errorf("azure openai deployment name is required")
		}

		plugin.DefineModel(g, azureaifoundry.ModelDefinition{
			Name:          deploymentName,
			Type:          "chat",
			SupportsMedia: true,
		}, nil)
		if genkit.LookupModel(g, fullModelName) == nil {
			return fmt.Errorf("failed to register azure openai deployment %q", deploymentName)
		}
		return nil
	}
}

// resolveVertexAIProjectAndLocation performs its package-specific work in the GenAI model resolution and runtime construction flow.
func resolveVertexAIProjectAndLocation(projectID string, location string) (string, string) {
	return strings.TrimSpace(projectID), strings.TrimSpace(location)
}
