package genai

import (
	"context"
	"crypto/sha256"
	"fmt"
	"net/http"
	"net/url"
	"strings"

	"github.com/Suhaibinator/go-common/pkg/config"

	"github.com/Suhaibinator/go-common/pkg/common_service/common_service_models"
	"github.com/Suhaibinator/go-common/pkg/genai/apishape"
	"github.com/Suhaibinator/go-common/pkg/genai/openaichat"
	"github.com/Suhaibinator/go-common/pkg/genai/openairesponses"
	pb "github.com/Suhaibinator/go-common/pkg/proto_models"
	antoption "github.com/anthropics/anthropic-sdk-go/option"
	genkitapi "github.com/firebase/genkit/go/core/api"
	"github.com/firebase/genkit/go/genkit"
	"github.com/firebase/genkit/go/plugins/anthropic"
)

type runtimeAPIKey struct {
	provider string
	api      pb.InferenceAPI
}

// APIModelResolver is the explicit API-aware extension of ModelResolver.
// A resolver implementing only ModelResolver cannot accept an API override.
type APIModelResolver interface {
	ResolveModelForAPI(*common_service_models.InferenceModel, pb.InferenceAPI) (*ResolvedModel, error)
}

func ResolveModelForAPI(resolver ModelResolver, model *common_service_models.InferenceModel, api pb.InferenceAPI) (*ResolvedModel, error) {
	if resolver == nil {
		return nil, fmt.Errorf("model runtime resolver is required")
	}
	if err := apishape.ValidateSelection(api); err != nil {
		return nil, err
	}
	if r, ok := resolver.(APIModelResolver); ok {
		resolved, err := r.ResolveModelForAPI(model, api)
		if err == nil && api != 0 && (resolved == nil || resolved.API != api) {
			return nil, fmt.Errorf("model resolver did not honor explicit inference API %s", api)
		}
		return resolved, err
	}
	if api != apishape.Default {
		return nil, fmt.Errorf("model resolver does not support explicit inference API selection")
	}
	return resolver.ResolveModel(model)
}

// endpointTransport sends inference to an exact configured endpoint, independent
// of SDK-specific version/path joining. Discovery is never used by this runtime.
type endpointTransport struct {
	endpoint   *url.URL
	headers    http.Header
	auth       pb.InferenceHTTPAuth
	key        string
	authHeader string
	// base carries the rewritten request; nil uses http.DefaultTransport.
	base http.RoundTripper
}

func (t *endpointTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	clone := req.Clone(req.Context())
	target := *t.endpoint
	clone.URL = &target
	clone.Host = target.Host
	clone.Header.Del("Authorization")
	clone.Header.Del("X-Api-Key")
	switch t.auth {
	case pb.InferenceHTTPAuth_INFERENCE_HTTP_AUTH_NONE:
	case pb.InferenceHTTPAuth_INFERENCE_HTTP_AUTH_API_KEY:
		header := t.authHeader
		if header == "" {
			header = "X-Api-Key"
		}
		clone.Header.Set(header, t.key)
	default:
		clone.Header.Set("Authorization", "Bearer "+t.key)
	}
	for key, values := range t.headers {
		clone.Header[key] = append([]string(nil), values...)
	}
	base := t.base
	if base == nil {
		base = http.DefaultTransport
	}
	return base.RoundTrip(clone)
}

func apiDestination(p config.ProviderConfig, credential config.Secret, api pb.InferenceAPI) (string, string, http.Header, error) {
	key := strings.TrimSpace(credential.StringValue())
	headers := make(http.Header)
	if p.CredentialRef != "" && strings.TrimSpace(key) == "" {
		return "", "", nil, fmt.Errorf("provider %s: credential %s is empty", p.ID, p.CredentialRef)
	}
	for _, h := range p.HTTP.Headers {
		headers.Add(h.Name, h.Value)
	}
	if p.CredentialRef == "" && p.HTTP.Auth != pb.InferenceHTTPAuth_INFERENCE_HTTP_AUTH_NONE {
		return "", "", nil, fmt.Errorf("provider %s: HTTP inference requires a credential or explicit anonymous authentication", p.ID)
	}
	endpoint := p.Endpoints.Endpoint(api)
	if endpoint == "" {
		return "", "", nil, fmt.Errorf("provider %s: API %s is unavailable", p.ID, api)
	}
	return key, endpoint, headers, nil
}

// newProviderRuntime builds one provider/API runtime. A non-nil base is the
// application's transport for this provider; only HTTP APIs can honor it.
func newProviderRuntime(ctx context.Context, p config.ProviderConfig, credential config.Secret, api pb.InferenceAPI, base http.RoundTripper) (*genkitProviderRuntime, error) {
	return newProviderRuntimeWithInit(ctx, p, credential, api, base, initGenkitWithPlugins)
}

func newProviderRuntimeWithInit(ctx context.Context, p config.ProviderConfig, credential config.Secret, api pb.InferenceAPI, base http.RoundTripper, initFn func(context.Context, ...genkitapi.Plugin) (*genkit.Genkit, error)) (*genkitProviderRuntime, error) {
	if base != nil && (api == apishape.GoogleNative || api == apishape.AzureNative) {
		// Native SDKs own their clients and authentication; silently bypassing an
		// application transport could skip an mTLS identity or egress policy.
		return nil, fmt.Errorf("provider %s: a provider transport is supported only for HTTP inference APIs, not %s", p.ID, api)
	}
	key := strings.TrimSpace(credential.StringValue())
	nativeADC := api == apishape.GoogleNative && p.Google.Auth == pb.InferenceGoogleAuth_INFERENCE_GOOGLE_AUTH_APPLICATION_DEFAULT
	if nativeADC {
		key = ""
	}
	if !nativeADC && p.CredentialRef != "" && strings.TrimSpace(key) == "" {
		return nil, fmt.Errorf("provider %s: credential %s is empty", p.ID, p.CredentialRef)
	}
	var r *genkitProviderRuntime
	var err error
	switch api {
	case apishape.GoogleNative:
		if p.Google.Backend == pb.InferenceGoogleBackend_INFERENCE_GOOGLE_BACKEND_AI_STUDIO {
			r, err = newGoogleAIStudioRuntime(ctx, key)
			if r != nil {
				r.namespace = "googleai"
			}
		} else {
			r, err = newVertexAIRuntimeWithInit(ctx, key, p.Google.Project, p.Google.Location, initFn)
			if r != nil {
				r.namespace = "vertexai"
			}
		}
	case apishape.AzureNative:
		r, err = newAzureAIFoundryRuntime(ctx, key, p.Azure.Endpoint, p.Azure.APIVersion)
		if r != nil {
			r.namespace = "azureaifoundry"
		}
	default:
		return newAPIRuntime(ctx, p, credential, api, base)
	}
	if err != nil {
		return nil, err
	}
	if r.g == nil {
		return nil, fmt.Errorf("provider %s: native runtime unavailable", p.ID)
	}
	r.destinationFingerprint = fmt.Sprintf("%x", sha256.Sum256(fmt.Appendf(nil, "%s:%v:%v", p.ID, p.Google, p.Azure)))
	return r, nil
}

func newAPIRuntime(ctx context.Context, p config.ProviderConfig, credential config.Secret, api pb.InferenceAPI, base http.RoundTripper) (*genkitProviderRuntime, error) {
	return newAPIRuntimeWithNamespace(ctx, p, credential, api, "", base)
}

func newAPIRuntimeWithNamespace(ctx context.Context, p config.ProviderConfig, credential config.Secret, api pb.InferenceAPI, namespace string, base http.RoundTripper) (*genkitProviderRuntime, error) {
	key, endpoint, headers, err := apiDestination(p, credential, api)
	if err != nil {
		return nil, err
	}
	target, _ := url.Parse(endpoint)
	auth := p.HTTP.Auth
	if auth == pb.InferenceHTTPAuth_INFERENCE_HTTP_AUTH_UNSPECIFIED {
		auth = pb.InferenceHTTPAuth_INFERENCE_HTTP_AUTH_BEARER
		if api == apishape.AnthropicMessages {
			auth = pb.InferenceHTTPAuth_INFERENCE_HTTP_AUTH_API_KEY
		}
	}
	if key == "" {
		key = "unused"
	}
	client := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }, Transport: &endpointTransport{endpoint: target, headers: headers, auth: auth, key: key, authHeader: p.HTTP.AuthHeader, base: base}}
	var plugin genkitapi.Plugin
	switch api {
	case apishape.ChatCompletions:
		if namespace == "" {
			namespace = openaichat.DefaultProvider
		}
		plugin, err = openaichat.New(openaichat.PluginOptions{Provider: namespace, APIKey: key, BaseURL: target.Scheme + "://" + target.Host + "/", HTTPClient: client, OpenRouterRouting: p.HTTP.OpenRouterRouting})
	case apishape.Responses:
		namespace = openairesponses.ProviderID
		plugin, err = openairesponses.New(openairesponses.PluginOptions{APIKey: key, BaseURL: target.Scheme + "://" + target.Host + "/", HTTPClient: client})
	case apishape.AnthropicMessages:
		namespace = "anthropic"
		plugin = &anthropic.Anthropic{APIKey: key, BaseURL: target.Scheme + "://" + target.Host + "/", Opts: []antoption.RequestOption{antoption.WithHTTPClient(client), antoption.WithMaxRetries(0)}}
	default:
		return nil, fmt.Errorf("unsupported inference API %d", api)
	}
	if err != nil {
		return nil, err
	}
	g, err := initGenkitWithPlugins(ctx, plugin)
	if err != nil {
		return nil, err
	}
	return &genkitProviderRuntime{g: g, namespace: namespace, destinationFingerprint: fmt.Sprintf("%x", sha256.Sum256([]byte(endpoint)))}, nil
}
