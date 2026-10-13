package controlplane

import (
	"context"
	json "encoding/json/v2"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/Suhaibinator/go-common/pkg/genai/inference"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
)

// Operation is the owning application's generated protobuf enum value. go-common
// carries the numeric identity without depending on the enum's semantics.
type Operation int32

// String renders the numeric operation for logs and diagnostics.
func (o Operation) String() string {
	return fmt.Sprintf("%d", o)
}

// ResolvedRoute is the immutable execution plan for one logical generation.
// Resolvers construct it from static configuration or persisted operation routes.
type ResolvedRoute struct {
	OpenRouterRouting bool
	API               proto_models.InferenceAPI
	// Operation is the numeric reference this route was resolved for.
	Operation Operation
	// InferenceModelID is the resolved model's registry identity, used as the default
	// process-local bucket scope.
	InferenceModelID uint64
	// ProviderID and ProviderModelID snapshot the model identity for attempt accounting.
	ProviderID      string
	ProviderModelID string
	// BucketScopeKey overrides the process-local bucket scope. When empty, buckets are
	// scoped to InferenceModelID.
	BucketScopeKey string
	// RetryPolicy governs total timeout, attempt count, and backoff.
	RetryPolicy RetryPolicy
	// RateLimitPolicy governs the shared request and token buckets for the model.
	RateLimitPolicy ModelRateLimitPolicy
	// Temperature is the optional generation temperature the attempt should apply.
	Temperature                        *float64
	SelectionSource                    proto_models.InferenceSelectionSource
	OperationListVersion               uint64
	OrganizationPolicyVersion          uint64
	OrganizationOperationPolicyVersion uint64
}

// scopeKey returns the normalized process-local bucket scope for this route,
// defaulting to inference-model scope when no explicit key is supplied.
func (r *ResolvedRoute) scopeKey() string {
	if key := normalizeScopeKey(r.BucketScopeKey); key != "" {
		return key
	}
	return fmt.Sprintf("inference-model:%d", r.InferenceModelID)
}

// RouteResolver maps a numeric operation reference to an immutable ResolvedRoute
// exactly once per logical execution call.
type RouteResolver interface {
	Resolve(ctx context.Context, request inference.GenerationRequest) (*ResolvedRoute, error)
}

// RouteResolverFunc adapts a function to the RouteResolver interface.
type RouteResolverFunc func(ctx context.Context, request inference.GenerationRequest) (*ResolvedRoute, error)

// Resolve calls the wrapped function.
func (f RouteResolverFunc) Resolve(ctx context.Context, request inference.GenerationRequest) (*ResolvedRoute, error) {
	return f(ctx, request)
}

// StaticRouteResolver resolves operations from a fixed, in-memory map.
// Applications with static routing and tests can wire routes without persistence.
type StaticRouteResolver struct {
	routes map[Operation]*ResolvedRoute
}

// NewStaticRouteResolver validates route identities and builds a resolver over deep copies of the
// supplied routes, so later caller mutations cannot change live execution behavior.
func NewStaticRouteResolver(routes map[Operation]*ResolvedRoute) (*StaticRouteResolver, error) {
	copied := make(map[Operation]*ResolvedRoute, len(routes))
	for operation, route := range routes {
		if operation == 0 {
			return nil, fmt.Errorf("cannot register unspecified (0) static operation")
		}
		if route == nil {
			return nil, fmt.Errorf("nil static route for operation %s", operation)
		}
		if route.Operation != operation {
			return nil, fmt.Errorf("static route operation %s does not match map key %s", route.Operation, operation)
		}
		cloned := *route
		cloned.Temperature = cloneFloat64(route.Temperature)
		copied[operation] = &cloned
	}
	return &StaticRouteResolver{routes: copied}, nil
}

// Resolve returns the route registered for the operation, or an error if none is.
func (s *StaticRouteResolver) Resolve(_ context.Context, request inference.GenerationRequest) (*ResolvedRoute, error) {
	operation := Operation(request.Operation)

	if request.ModelSelection != nil {
		return nil, fmt.Errorf("static route resolver does not support explicit model selection")
	}
	route, ok := s.routes[operation]
	if !ok || route == nil {
		return nil, fmt.Errorf("no route registered for operation %s", operation)
	}
	cloned := *route
	cloned.Temperature = cloneFloat64(route.Temperature)
	return &cloned, nil
}

// normalizeScopeKey lower-cases, trims, and hyphenates a bucket scope key so
// cosmetically different spellings share one bucket.
func normalizeScopeKey(raw string) string {
	key := strings.ToLower(strings.TrimSpace(raw))
	key = strings.ReplaceAll(key, "_", "-")
	key = strings.ReplaceAll(key, " ", "-")
	return key
}

// executionSettings is the non-secret policy snapshot stored on attempt rows.
type executionSettings struct {
	Temperature                        *float64                              `json:"temperature"`
	RetryPolicy                        RetryPolicy                           `json:"retry_policy"`
	RateLimitPolicy                    ModelRateLimitPolicy                  `json:"rate_limit_policy"`
	SelectionSource                    proto_models.InferenceSelectionSource `json:"selection_source,omitzero"`
	OperationListVersion               uint64                                `json:"operation_list_version,omitzero"`
	OrganizationPolicyVersion          uint64                                `json:"organization_policy_version,omitzero"`
	OrganizationOperationPolicyVersion uint64                                `json:"organization_operation_policy_version,omitzero"`
}

// durationNanos keeps the integer-nanosecond durations already stored in
// attempt rows; encoding/json/v2 has no default time.Duration representation.
var durationNanos = json.WithMarshalers(json.MarshalFunc(func(d time.Duration) ([]byte, error) {
	return strconv.AppendInt(nil, int64(d), 10), nil
}))

// executionSettingsJSON records only non-secret control-plane policies.
func (r *ResolvedRoute) executionSettingsJSON() []byte {
	b, _ := json.Marshal(executionSettings{Temperature: r.Temperature, RetryPolicy: r.RetryPolicy, RateLimitPolicy: r.RateLimitPolicy, SelectionSource: r.SelectionSource, OperationListVersion: r.OperationListVersion, OrganizationPolicyVersion: r.OrganizationPolicyVersion, OrganizationOperationPolicyVersion: r.OrganizationOperationPolicyVersion}, durationNanos)
	return b
}
