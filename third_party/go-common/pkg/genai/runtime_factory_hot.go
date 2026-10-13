package genai

import (
	"context"
	"fmt"
	"net/http"
	"slices"
	"strings"

	"github.com/Suhaibinator/go-common/pkg/config"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
)

type hotRuntimeEntry struct {
	definition config.ProviderConfig
	credential config.Secret
	runtime    *genkitProviderRuntime
	cancel     context.CancelFunc
}

func cloneRuntimeConfig(c RuntimeFactoryConfig) RuntimeFactoryConfig {
	c.Providers = config.CloneProviders(c.Providers)
	bindings := make([]config.CredentialBinding, len(c.Credentials))
	for i, b := range c.Credentials {
		bindings[i] = config.CredentialBinding{Name: b.Name, Secret: b.Secret.Clone()}
	}
	c.Credentials = bindings
	return c
}
func validateRuntimeConfig(c RuntimeFactoryConfig) error {
	return config.ValidateCredentialBindings(c.Providers, c.Credentials)
}

// NewReloadingModelRuntimeFactory samples a coherent source for each resolution.
func NewReloadingModelRuntimeFactory(ctx context.Context, source func() (RuntimeFactoryConfig, error), opts ...RuntimeFactoryOption) (*ModelRuntimeFactory, error) {
	if source == nil {
		return nil, fmt.Errorf("inference source is required")
	}
	initial, err := source()
	if err != nil {
		return nil, err
	}
	if err = validateRuntimeConfig(initial); err != nil {
		return nil, err
	}
	if ctx == nil {
		ctx = context.Background()
	}
	factory := &ModelRuntimeFactory{provider: source, hotContext: ctx, apiEntries: make(map[runtimeAPIKey]hotRuntimeEntry)}
	for _, opt := range opts {
		opt(factory)
	}
	return factory, nil
}
func (f *ModelRuntimeFactory) runtimeForAPI(id string, api proto_models.InferenceAPI) (*genkitProviderRuntime, error) {
	f.hotMu.Lock()
	defer f.hotMu.Unlock()
	if f.hotClosed {
		return nil, fmt.Errorf("model runtime factory is closed")
	}
	cfg, err := f.provider()
	if err != nil {
		return nil, err
	}
	cfg = cloneRuntimeConfig(cfg)
	if err = validateRuntimeConfig(cfg); err != nil {
		return nil, err
	}
	// Retire deleted destinations immediately when observing a new generation.
	present := make(map[string]bool, len(cfg.Providers))
	for _, p := range cfg.Providers {
		present[p.ID] = true
	}
	for key, entry := range f.apiEntries {
		if !present[key.provider] {
			entry.cancel()
			delete(f.apiEntries, key)
		}
	}
	var p *config.ProviderConfig
	for i := range cfg.Providers {
		if cfg.Providers[i].ID == id {
			p = &cfg.Providers[i]
			break
		}
	}
	if p == nil {
		return nil, fmt.Errorf("unknown inference provider %q", id)
	}
	if api == 0 {
		api = p.DefaultAPI
	}
	if !p.SupportsAPI(api) {
		return nil, fmt.Errorf("provider %s does not support API %s", id, api)
	}
	var credential config.Secret
	for _, b := range cfg.Credentials {
		if b.Name == p.CredentialRef {
			credential = b.Secret
			break
		}
	}
	key := runtimeAPIKey{id, api}
	if old, ok := f.apiEntries[key]; ok {
		if equalProviderConfig(old.definition, *p) && old.credential.Equal(credential) {
			return old.runtime, nil
		}
		old.cancel()
		delete(f.apiEntries, key)
	}
	ctx, cancel := context.WithCancel(f.hotContext)
	var base http.RoundTripper
	if f.transport != nil {
		base = f.transport(p.ID)
	}
	runtime, err := newProviderRuntime(ctx, *p, credential, api, base)
	if err != nil {
		cancel()
		return nil, err
	}
	runtime.api = api
	runtime.openRouterRouting = p.HTTP.OpenRouterRouting
	f.apiEntries[key] = hotRuntimeEntry{*p, credential, runtime, cancel}
	return runtime, nil
}

// Close cancels clients. In-flight requests have best-effort replacement semantics.
func (f *ModelRuntimeFactory) Close() {
	if f == nil {
		return
	}
	f.hotMu.Lock()
	defer f.hotMu.Unlock()
	f.hotClosed = true
	for k, e := range f.apiEntries {
		e.cancel()
		delete(f.apiEntries, k)
	}
}

type ProviderDescriptor struct {
	ID, Name                 string
	DefaultAPI               proto_models.InferenceAPI
	SupportedAPIs            []proto_models.InferenceAPI
	OpenRouterRouting        bool
	CredentialUnavailable    bool
	GoogleApplicationDefault bool
}

// ListProviders exposes only public registry metadata from the current generation.
func (f *ModelRuntimeFactory) ListProviders() ([]ProviderDescriptor, error) {
	if f == nil {
		return nil, fmt.Errorf("model runtime factory is not configured")
	}
	f.hotMu.Lock()
	defer f.hotMu.Unlock()
	if f.hotClosed {
		return nil, fmt.Errorf("model runtime factory is closed")
	}
	cfg, err := f.provider()
	if err != nil {
		return nil, err
	}
	if err = validateRuntimeConfig(cfg); err != nil {
		return nil, err
	}
	result := make([]ProviderDescriptor, 0, len(cfg.Providers))
	credentials := make(map[string]bool, len(cfg.Credentials))
	for _, binding := range cfg.Credentials {
		credentials[binding.Name] = strings.TrimSpace(binding.Secret.StringValue()) != ""
	}
	for _, p := range cfg.Providers {
		result = append(result, ProviderDescriptor{ID: p.ID, Name: p.Name, DefaultAPI: p.DefaultAPI, SupportedAPIs: p.SupportedAPIs(), OpenRouterRouting: p.HTTP.OpenRouterRouting, CredentialUnavailable: p.CredentialRef != "" && !credentials[p.CredentialRef], GoogleApplicationDefault: p.Google.Auth == proto_models.InferenceGoogleAuth_INFERENCE_GOOGLE_AUTH_APPLICATION_DEFAULT})
	}
	return result, nil
}

// equalProviderConfig preserves header order and distinguishes nil from empty
// headers, matching the cache's existing replacement behavior.
func equalProviderConfig(a, b config.ProviderConfig) bool {
	return a.ID == b.ID && a.Name == b.Name && a.Adapter == b.Adapter &&
		a.DefaultAPI == b.DefaultAPI && a.CredentialRef == b.CredentialRef &&
		a.Endpoints == b.Endpoints && a.Google == b.Google && a.Azure == b.Azure &&
		a.HTTP.Auth == b.HTTP.Auth && a.HTTP.AuthHeader == b.HTTP.AuthHeader &&
		a.HTTP.OpenRouterRouting == b.HTTP.OpenRouterRouting &&
		(a.HTTP.Headers == nil) == (b.HTTP.Headers == nil) &&
		slices.Equal(a.HTTP.Headers, b.HTTP.Headers)
}
