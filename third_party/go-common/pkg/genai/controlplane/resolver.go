package controlplane

import (
	"context"
	"errors"
	"fmt"

	cpm "github.com/Suhaibinator/go-common/pkg/common_persistence/common_persistence_models"
	"github.com/Suhaibinator/go-common/pkg/genai/inference"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
)

// ModelRuntimeChecker confirms that go-common's existing model runtime factory can construct a
// runtime for a resolved model. The persistence resolver calls it as the last validation step so a
// route that points at a model the runtime cannot build fails resolution rather than at first use.
type ModelRuntimeChecker interface {
	CanResolveModel(ctx context.Context, provider string, providerModelID string) error
}

// PersistenceRouteResolver implements the kernel's RouteResolver against the control-plane store and
// operation registry. It produces an immutable ResolvedRoute for one logical generation.
type PersistenceRouteResolver struct {
	registry       *Registry
	runtimeChecker ModelRuntimeChecker
	metrics        Metrics
}

// ResolverOption configures a PersistenceRouteResolver.
type ResolverOption func(*PersistenceRouteResolver)

// WithModelRuntimeChecker installs the optional runtime-factory resolvability check (step 6). When
// omitted, that step is skipped.
func WithModelRuntimeChecker(checker ModelRuntimeChecker) ResolverOption {
	return func(r *PersistenceRouteResolver) {
		if checker != nil {
			r.runtimeChecker = checker
		}
	}
}

// WithResolverMetrics installs the bounded metrics sink so every resolution failure increments the
// route-resolution-failure counter.
func WithResolverMetrics(metrics Metrics) ResolverOption {
	return func(r *PersistenceRouteResolver) {
		if metrics != nil {
			r.metrics = metrics
		}
	}
}

// NewPersistenceResolver builds a resolver over a populated registry (which carries the store).
func NewPersistenceResolver(registry *Registry, opts ...ResolverOption) *PersistenceRouteResolver {
	r := &PersistenceRouteResolver{registry: registry}
	for _, opt := range opts {
		opt(r)
	}
	return r
}

// Resolve maps a numeric operation reference to an immutable ResolvedRoute. The steps are: validate
// the registered reference; load its route; confirm the model exists and is active; validate the
// retry policy and provider/model-specific route settings; load and validate the model rate-limit
// policy; confirm runtime resolvability; return the immutable route.
func (r *PersistenceRouteResolver) Resolve(ctx context.Context, request inference.GenerationRequest) (*ResolvedRoute, error) {
	route, err := r.resolve(ctx, request)
	if err != nil && r.metrics != nil {
		r.metrics.RecordInferenceRouteResolutionFailure()
	}
	return route, err
}

func (r *PersistenceRouteResolver) resolve(ctx context.Context, request inference.GenerationRequest) (*ResolvedRoute, error) {
	operation := Operation(request.Operation)

	// Step 1: the reference must be registered (closed-registry behavior).
	_, ok := r.registry.lookup(operation)
	if !ok {
		return nil, fmt.Errorf("operation %s is not registered", operation)
	}

	// Step 2: load the persisted route by numeric operation.
	// Without a model selection, one joined read supplies the route, model, and policy.
	if request.ModelSelection == nil {
		route, model, policy, err := r.registry.store.GetRouteSnapshot(ctx, int32(operation))
		if err != nil {
			return nil, err
		}
		return r.resolveLoadedRoute(ctx, operation, route, model, policy)
	}
	route, err := r.registry.store.GetRoute(ctx, int32(operation))
	if err != nil {
		return nil, err
	}
	if err := request.ModelSelection.Validate(); err != nil {
		return nil, err
	}
	if request.ModelSelection != nil && route != nil {
		cloned := *route
		cloned.Temperature = cloneFloat64(route.Temperature)
		cloned.InferenceModelID = request.ModelSelection.InferenceModelID
		if request.ModelSelection.API != nil {
			cloned.API = *request.ModelSelection.API
		}
		route = &cloned
	}
	return r.resolveRoute(ctx, operation, route)
}

// resolveRoute validates and resolves an already-loaded route. Keeping this path separate from the
// route lookup lets startup validate a default seed candidate before it is persisted while sharing
// the exact model and policy checks used for request-time resolution.
func (r *PersistenceRouteResolver) resolveRoute(ctx context.Context, operation Operation, route *cpm.InferenceOperationRoute) (*ResolvedRoute, error) {
	if route == nil {
		return nil, fmt.Errorf("operation %s has a nil route", operation)
	}
	if route.Operation != int32(operation) {
		return nil, fmt.Errorf("route operation %d does not match requested operation %s", route.Operation, operation)
	}

	// Step 3: confirm the selected model exists and is active.
	model, err := r.registry.store.GetModel(ctx, route.InferenceModelID)
	if err != nil {
		return nil, err
	}
	policyRow, err := r.registry.store.GetModelRateLimitPolicy(ctx, model.ID)
	if err != nil {
		return nil, err
	}
	return r.resolveLoadedRoute(ctx, operation, route, model, policyRow)
}

func (r *PersistenceRouteResolver) resolveLoadedRoute(ctx context.Context, operation Operation, route *cpm.InferenceOperationRoute, model *cpm.InferenceModel, policyRow *cpm.InferenceModelRateLimitPolicy) (*ResolvedRoute, error) {
	if route == nil || route.Operation != int32(operation) {
		return nil, fmt.Errorf("invalid route for operation %s", operation)
	}
	if model == nil || model.ID != route.InferenceModelID {
		return nil, fmt.Errorf("route model does not match operation %s", operation)
	}
	if !model.Active {
		return nil, fmt.Errorf("operation %s routes to inactive model %d (%s/%s)", operation, model.ID, model.ProviderID, model.ProviderModelID)
	}

	// Step 4: validate the retry policy.
	retryPolicy := RetryPolicy{
		TotalTimeout:   route.TotalTimeout,
		MaxAttempts:    int(route.MaxAttempts),
		InitialBackoff: route.InitialBackoff,
		MaxBackoff:     route.MaxBackoff,
	}
	if err := ValidateRetryPolicy(retryPolicy); err != nil {
		return nil, fmt.Errorf("invalid retry policy for operation %s: %w", operation, &inference.BlockError{Reason: proto_models.InferenceBlockReason_INFERENCE_BLOCK_REASON_INVALID_MODEL_SETTINGS, Operation: int32(operation), InferenceModelID: model.ID, Cause: err})
	}
	if err := r.registry.ValidateModelRoute(ctx, model.ProviderID, model.ProviderModelID, route.Temperature, route.API); err != nil {
		var blocked *inference.BlockError
		if errors.As(err, &blocked) {
			return nil, fmt.Errorf("invalid model route for operation %s: %w", operation, err)
		}
		return nil, fmt.Errorf("invalid model route for operation %s: %w", operation, &inference.BlockError{Reason: proto_models.InferenceBlockReason_INFERENCE_BLOCK_REASON_INVALID_MODEL_SETTINGS, Operation: int32(operation), InferenceModelID: model.ID, Cause: err})
	}

	// Step 5: validate the model rate-limit policy (loaded with the route or by resolveRoute).
	rateLimitPolicy := modelRateLimitPolicyFromRow(policyRow)
	if err := ValidateModelRateLimitPolicy(rateLimitPolicy); err != nil {
		return nil, fmt.Errorf("invalid rate-limit policy for operation %s: %w", operation, err)
	}

	// Step 6: confirm the runtime factory can resolve the model.
	runtimeInfo := ModelRuntimeInfo{API: route.API}
	if r.runtimeChecker != nil {
		var checkErr error
		if checker, ok := r.runtimeChecker.(interface {
			ResolveModelRuntime(context.Context, string, string, proto_models.InferenceAPI) (ModelRuntimeInfo, error)
		}); ok {
			runtimeInfo, checkErr = checker.ResolveModelRuntime(ctx, model.ProviderID, model.ProviderModelID, route.API)
		} else if route.API != 0 {
			checker, ok := r.runtimeChecker.(interface {
				CanResolveModelForAPI(context.Context, string, string, proto_models.InferenceAPI) error
			})
			if !ok {
				return nil, fmt.Errorf("runtime checker does not support explicit inference API selection")
			}
			checkErr = checker.CanResolveModelForAPI(ctx, model.ProviderID, model.ProviderModelID, route.API)
		} else {
			checkErr = r.runtimeChecker.CanResolveModel(ctx, model.ProviderID, model.ProviderModelID)
		}
		if err := checkErr; err != nil {
			return nil, fmt.Errorf("runtime cannot resolve model %d for operation %s: %w", model.ID, operation, &inference.BlockError{Reason: proto_models.InferenceBlockReason_INFERENCE_BLOCK_REASON_PROVIDER_UNAVAILABLE, Operation: int32(operation), InferenceModelID: model.ID, Cause: err})
		}
	}

	// Step 7: return the immutable resolved route.
	return &ResolvedRoute{
		Operation:         operation,
		InferenceModelID:  model.ID,
		API:               runtimeInfo.API,
		OpenRouterRouting: runtimeInfo.OpenRouterRouting,
		ProviderID:        model.ProviderID,
		ProviderModelID:   model.ProviderModelID,
		RetryPolicy:       retryPolicy,
		RateLimitPolicy:   rateLimitPolicy,
		Temperature:       cloneFloat64(route.Temperature),
	}, nil
}

// modelRateLimitPolicyFromRow maps a persisted model rate-limit policy (possibly nil) into the
// kernel's all-or-none ModelRateLimitPolicy. A nil row means no limits are configured.
func modelRateLimitPolicyFromRow(row *cpm.InferenceModelRateLimitPolicy) ModelRateLimitPolicy {
	if row == nil {
		return ModelRateLimitPolicy{}
	}
	policy := ModelRateLimitPolicy{}
	if row.HasRequestLimit {
		policy.HasRequestLimit = true
		policy.RequestLimit = BucketRateLimitPolicy{
			Capacity:       int(row.RequestCapacity),
			RefillInterval: row.RequestRefillInterval,
			RefillUnits:    int(row.RequestRefillUnits),
			WaitCap:        row.RequestWaitCap,
		}
	}
	if row.HasTokenLimit {
		policy.HasTokenLimit = true
		policy.TokenLimit = BucketRateLimitPolicy{
			Capacity:       int(row.TokenCapacity),
			RefillInterval: row.TokenRefillInterval,
			RefillUnits:    int(row.TokenRefillUnits),
			WaitCap:        row.TokenWaitCap,
		}
	}
	return policy
}

// ModelRuntimeInfo is the selected adapter's protocol and capabilities from one snapshot.
type ModelRuntimeInfo struct {
	API               proto_models.InferenceAPI
	OpenRouterRouting bool
}
