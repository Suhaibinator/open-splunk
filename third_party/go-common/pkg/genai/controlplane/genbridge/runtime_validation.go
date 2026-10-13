package genbridge

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"sync"

	"github.com/Suhaibinator/go-common/pkg/common_service/common_service_models"
	commongenai "github.com/Suhaibinator/go-common/pkg/genai"
	"github.com/Suhaibinator/go-common/pkg/genai/apishape"
	"github.com/Suhaibinator/go-common/pkg/genai/controlplane"
	"github.com/Suhaibinator/go-common/pkg/genai/inference"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
)

// RuntimeModelChecker validates that a provider model can be resolved by the
// same runtime source used to prepare generation requests.
type RuntimeModelChecker struct {
	resolver commongenai.ModelResolver
}

// NewRuntimeModelChecker creates a runtime-backed control-plane model checker.
func NewRuntimeModelChecker(resolver commongenai.ModelResolver) *RuntimeModelChecker {
	return &RuntimeModelChecker{resolver: resolver}
}

// CanResolveModel implements controlplane.RuntimeModelChecker.
func (c *RuntimeModelChecker) CanResolveModel(
	_ context.Context,
	provider string,
	providerModelID string,
) error {
	if c == nil || c.resolver == nil {
		return fmt.Errorf("model runtime resolver is required")
	}
	_, err := c.resolver.ResolveModel(&common_service_models.InferenceModel{
		ProviderID:      provider,
		ProviderModelID: providerModelID,
		Active:          true,
	})
	return err
}

// RuntimeModelRouteValidator validates a control-plane route against the same
// runtime/model source used for generation. Structural temperature validation
// remains the responsibility of the control-plane registry.
type RuntimeModelRouteValidator struct {
	checker *RuntimeModelChecker
}

// NewRuntimeModelRouteValidator creates a model-route validator backed by resolver.
func NewRuntimeModelRouteValidator(resolver commongenai.ModelResolver) *RuntimeModelRouteValidator {
	return &RuntimeModelRouteValidator{checker: NewRuntimeModelChecker(resolver)}
}

// ValidateModelRoute delegates provider/model validation to the runtime checker.
func (v *RuntimeModelRouteValidator) ValidateModelRoute(
	ctx context.Context,
	route controlplane.ModelRouteValidation,
) error {
	if v == nil || v.checker == nil {
		return fmt.Errorf("model route validator is required")
	}
	_, err := commongenai.ResolveModelForAPI(v.checker.resolver, &common_service_models.InferenceModel{ProviderID: route.ProviderID, ProviderModelID: route.ProviderModelID, Active: true}, route.API)
	return err
}

func (c *RuntimeModelChecker) CanResolveModelForAPI(_ context.Context, provider string, modelID string, api proto_models.InferenceAPI) error {
	if c == nil || c.resolver == nil {
		return fmt.Errorf("model runtime resolver is required")
	}
	_, err := commongenai.ResolveModelForAPI(c.resolver, &common_service_models.InferenceModel{ProviderID: provider, ProviderModelID: modelID, Active: true}, api)
	return err
}

// ResolveModelRuntime returns the selected runtime's effective protocol for the route snapshot.
func (c *RuntimeModelChecker) ResolveModelRuntime(ctx context.Context, providerID, modelID string, api proto_models.InferenceAPI) (controlplane.ModelRuntimeInfo, error) {
	if c == nil || c.resolver == nil {
		return controlplane.ModelRuntimeInfo{}, fmt.Errorf("model runtime resolver is required")
	}
	model, err := commongenai.ResolveModelForAPI(c.resolver, &common_service_models.InferenceModel{ProviderID: providerID, ProviderModelID: modelID, Active: true}, api)
	if err != nil {
		return controlplane.ModelRuntimeInfo{}, err
	}
	if model == nil {
		return controlplane.ModelRuntimeInfo{}, fmt.Errorf("resolved runtime is required")
	}
	return controlplane.ModelRuntimeInfo{API: model.API, OpenRouterRouting: model.OpenRouterRouting}, nil
}

// UsesRuntimeModelValidation identifies the shared runtime validator so routing can
// replace it with the same captured source used by its runtime checker.
func (v *RuntimeModelRouteValidator) UsesRuntimeModelValidation() bool { return true }

// SnapshotModelValidation captures public provider capabilities once for one routing
// decision. Custom model resolvers without provider metadata retain their validation
// behavior and memoize each model/API result within the decision.
func (c *RuntimeModelChecker) SnapshotModelValidation(ctx context.Context) (controlplane.ModelValidationSnapshot, error) {
	if c == nil || c.resolver == nil {
		return nil, fmt.Errorf("model runtime resolver is required")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	snapshot := &runtimeValidationSnapshot{checker: c, results: make(map[runtimeValidationKey]runtimeValidationResult)}
	if source, ok := c.resolver.(interface {
		ListProviders() ([]commongenai.ProviderDescriptor, error)
	}); ok {
		providers, err := source.ListProviders()
		if err != nil {
			return nil, err
		}
		snapshot.providers = make(map[string]commongenai.ProviderDescriptor, len(providers))
		for _, provider := range providers {
			provider.SupportedAPIs = slices.Clone(provider.SupportedAPIs)
			snapshot.providers[provider.ID] = provider
		}
	}
	return snapshot, nil
}

type runtimeValidationKey struct {
	provider, model string
	api             proto_models.InferenceAPI
}

type runtimeValidationResult struct {
	info controlplane.ModelRuntimeInfo
	err  error
}

type runtimeValidationSnapshot struct {
	checker   *RuntimeModelChecker
	providers map[string]commongenai.ProviderDescriptor
	mu        sync.Mutex
	results   map[runtimeValidationKey]runtimeValidationResult
}

func (s *runtimeValidationSnapshot) CanResolveModel(ctx context.Context, provider, model string) error {
	return s.CanResolveModelForAPI(ctx, provider, model, 0)
}

func (s *runtimeValidationSnapshot) CanResolveModelForAPI(ctx context.Context, provider, model string, api proto_models.InferenceAPI) error {
	_, err := s.ResolveModelRuntime(ctx, provider, model, api)
	return err
}

func (s *runtimeValidationSnapshot) ValidateModelRoute(ctx context.Context, route controlplane.ModelRouteValidation) error {
	return s.CanResolveModelForAPI(ctx, route.ProviderID, route.ProviderModelID, route.API)
}

func (s *runtimeValidationSnapshot) ResolveModelRuntime(ctx context.Context, provider, model string, api proto_models.InferenceAPI) (controlplane.ModelRuntimeInfo, error) {
	if err := ctx.Err(); err != nil {
		return controlplane.ModelRuntimeInfo{}, err
	}
	if _, err := commongenai.NormalizeProviderModelID(provider, model); err != nil {
		return controlplane.ModelRuntimeInfo{}, err
	}
	if s.providers != nil {
		descriptor, ok := s.providers[provider]
		if !ok {
			return controlplane.ModelRuntimeInfo{}, &inference.BlockError{Reason: proto_models.InferenceBlockReason_INFERENCE_BLOCK_REASON_PROVIDER_UNAVAILABLE, Cause: fmt.Errorf("inference provider %s is not configured", provider)}
		}
		if api == 0 {
			api = descriptor.DefaultAPI
		}
		if descriptor.CredentialUnavailable && !(api == apishape.GoogleNative && descriptor.GoogleApplicationDefault) {
			return controlplane.ModelRuntimeInfo{}, &inference.BlockError{Reason: proto_models.InferenceBlockReason_INFERENCE_BLOCK_REASON_PROVIDER_UNAVAILABLE, Cause: fmt.Errorf("inference provider %s credentials are unavailable", provider)}
		}
		if !slices.Contains(descriptor.SupportedAPIs, api) {
			return controlplane.ModelRuntimeInfo{}, &inference.BlockError{Reason: proto_models.InferenceBlockReason_INFERENCE_BLOCK_REASON_PROVIDER_UNAVAILABLE, Cause: fmt.Errorf("inference provider %s does not support API %s", provider, api)}
		}
		return controlplane.ModelRuntimeInfo{API: api, OpenRouterRouting: descriptor.OpenRouterRouting}, nil
	}
	key := runtimeValidationKey{provider: provider, model: model, api: api}
	s.mu.Lock()
	defer s.mu.Unlock()
	result, ok := s.results[key]
	if !ok {
		result.info, result.err = s.checker.ResolveModelRuntime(ctx, provider, model, api)
		s.results[key] = result
	}
	return result.info, result.err
}

// ConfiguredProviderIDs exposes the same captured provider registry used by validation.
// The boolean distinguishes a known empty registry from a custom resolver without metadata.
func (s *runtimeValidationSnapshot) ConfiguredProviderIDs() ([]string, bool) {
	if s.providers == nil {
		return nil, false
	}
	return slices.Sorted(maps.Keys(s.providers)), true
}
