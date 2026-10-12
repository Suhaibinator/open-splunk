// Package genbridge adapts the go-common inference control plane

// (pkg/genai/controlplane) to genkit-based model generation. It supplies the
// control-plane-backed implementations that the control-plane kernel and the agent
// runtime deliberately leave as seams, so a consuming application wires them together
// without reimplementing the bridge.
//
// It provides two bridges:
//
//   - NewControlPlanePlanner wires the control plane into the go-common agent runtime's
//     GenerationPlanner seam (pkg/common_service/ai): each PlanGeneration call resolves
//     the operation's route once, and the returned plan carries the model to generate
//     on plus a retrier bound to that same resolved route, so an agent's buffered and
//     streaming model attempts run through the control plane's retry classification,
//     backoff, rate-limit buckets, and per-attempt ledger recording under the exact
//     route the drive resolved.
//   - PrepareGeneration resolves a direct (non-agent) generation's route and runtime
//     once; Generate and GenerateData execute that prepared decision through the control
//     plane, applying the selected model and temperature to every attempt.
//
// Both bridges keep application-specific concerns injectable, so go-common carries no
// knowledge of an application's operation semantics: the sanitized usage context
// attached to attempt records (a GenerationCallMetadataFunc for the agent planner, or a
// caller-built metadata map for Generate) and the provider-specific temperature
// encoding (a TemperatureConfigFunc) are supplied by the consuming application.
package genbridge

import (
	"context"
	"fmt"

	commonai "github.com/Suhaibinator/go-common/pkg/common_service/ai"
	"github.com/Suhaibinator/go-common/pkg/common_service/common_service_models"
	apperrors "github.com/Suhaibinator/go-common/pkg/common_service/errors"
	"github.com/Suhaibinator/go-common/pkg/genai/controlplane"
	"github.com/Suhaibinator/go-common/pkg/genai/inference"
)

// GenerationCallMetadataFunc derives the sanitized usage-context map attached to each
// control-plane attempt record from the run's call context. The control plane's recorder
// further filters this map by the routed operation's metadata allowlist, so returning
// keys the operation does not allow is safe (they are dropped). Returning a nil map
// records an empty context. Applications supply this to scope attempt records (for
// example to channel or session identifiers) without go-common knowing their operation
// semantics; keeping it injectable is also what lets an application enforce its own
// allowlist before the map ever reaches the control plane.
type GenerationCallMetadataFunc func(commonai.GenerationCallContext) map[string]any

// controlPlanePlanner adapts the inference control plane to the go-common agent
// runtime's GenerationPlanner seam. Each PlanGeneration call resolves the operation's
// route through the control plane exactly once and derives the whole drive from that
// single resolution: the model the generator is built from, the catalog row the ledger
// stamps, and a retrier bound to the same route.
type controlPlanePlanner struct {
	controlPlane *controlplane.ControlPlane
	callMetadata GenerationCallMetadataFunc
}

// PlannerOption customizes a control-plane planner at construction.
type PlannerOption func(*controlPlanePlanner)

// WithCallMetadata injects the sanitized usage context attached to each attempt record.
// A nil func (the default) records an empty context, matching the pre-metadata behavior.
func WithCallMetadata(fn GenerationCallMetadataFunc) PlannerOption {
	return func(p *controlPlanePlanner) { p.callMetadata = fn }
}

// NewControlPlanePlanner builds the agent-runtime generation planner over the control
// plane. It returns an untyped nil interface when controlPlane is nil so the caller
// leaves the runtime seam unset, matching go-common's nil-planner contract. The plans
// it produces carry a retrier implementing both commonai.GenerationRetrier and
// commonai.GenerationStreamRetrier, so the agent runtime's streaming path retries too.
// The control plane owns retry classification, jittered backoff, process-local
// rate-limit buckets, and per-attempt ledger recording via its installed observer, so
// no separate usage observer is wired for the agent path (a second recorder would
// double-count the global inference-attempt ledger).
func NewControlPlanePlanner(controlPlane *controlplane.ControlPlane, opts ...PlannerOption) commonai.GenerationPlanner {
	if controlPlane == nil {
		return nil
	}
	p := &controlPlanePlanner{controlPlane: controlPlane}
	for _, opt := range opts {
		if opt != nil {
			opt(p)
		}
	}
	return p
}

// PlanGeneration resolves the operation's route once and maps it onto one drive's
// generation plan, with a retrier bound to that same resolved route. Every resolution
// failure wraps apperrors.ErrAgentOperationRouteUnresolved with multi-%w — the same
// taxonomy the store-backed planner speaks — because the agent runtime settles a run
// FAILED only on that sentinel (an unmatched planner error wedges the run in RUNNING
// forever), and ValidateAgentRoutes callers classify startup failures by the
// persistence cause underneath it, which stays visible to errors.Is.
func (p *controlPlanePlanner) PlanGeneration(ctx context.Context, request inference.GenerationRequest) (*commonai.GenerationPlan, error) {
	operation := int32(request.Operation)

	request.ModelSelection = request.ModelSelection.Clone()
	route, err := p.controlPlane.ResolveRoute(ctx, request)
	if err != nil {
		return nil, fmt.Errorf("%w: operation %d: %w", apperrors.ErrAgentOperationRouteUnresolved, operation, err)
	}
	route = copyGenerationRoute(route)
	plan := &commonai.GenerationPlan{
		Operation:                          request.Operation,
		SelectionSource:                    route.SelectionSource,
		OperationListVersion:               route.OperationListVersion,
		OrganizationPolicyVersion:          route.OrganizationPolicyVersion,
		OrganizationOperationPolicyVersion: route.OrganizationOperationPolicyVersion,
		ProviderID:                         route.ProviderID,
		OpenRouterRouting:                  route.OpenRouterRouting,
		API:                                route.API,
		ProviderModelID:                    route.ProviderModelID,
		InferenceModelID:                   route.InferenceModelID,
		Retrier: &routeBoundRetrier{
			controlPlane: p.controlPlane,
			route:        route,
			callMetadata: p.callMetadata,
			request:      request,
		},
	}
	if route.Temperature != nil {
		// The route stores float64 and the plan carries float32, which is exact for the Google
		// path (natively float32). A non-Google provider is handed the float64 widening of that
		// float32, so a route temperature of 0.7 reaches it as 0.699999988079071 — harmless at
		// generation precision.
		temperature := float32(*route.Temperature)
		plan.Temperature = &temperature
	}
	return plan, nil
}

// routeBoundRetrier adapts the control plane to the go-common agent runtime's
// generation seams for ONE resolved route. It implements commonai.GenerationRetrier and
// commonai.GenerationStreamRetrier, routing every model attempt (buffered and
// streaming) of the drive that planned it through ControlPlane.Execute / ExecuteStream
// against the captured route and trusted organization/actor scope. Security is revalidated
// per attempt without replacing the captured decision. The generator was built from the
// same resolution, so the model that generates,
// the buckets and backoff that govern it, and the ledger rows that record it agree by
// construction, and a route edit takes effect on the next drive's plan rather than
// mid-drive.
type routeBoundRetrier struct {
	controlPlane *controlplane.ControlPlane
	route        *controlplane.ResolvedRoute
	callMetadata GenerationCallMetadataFunc
	request      inference.GenerationRequest
}

// Execute runs one buffered agent generation through the control plane. Each control
// plane attempt invokes the runtime's GenerationFn and accounts for returned usage on
// success or failure. The final outcome/error pair is returned to the runtime.
//
// The invocation ID is minted here and handed to the control plane, so every GenerationFn call
// receives the same identity the attempt ledger records. step is not used by the control plane;
// it is part of the seam for retriers that wrap this one (per-attempt admission).
func (r *routeBoundRetrier) Execute(ctx context.Context, call commonai.GenerationCallContext, _ commonai.GenerationStep, fn commonai.GenerationFn) (*commonai.GenerationOutcome, error) {
	if err := r.validateScope(call); err != nil {
		return nil, err
	}
	// The captured durable actor survives background recovery and later nudges.
	ctx = controlplane.WithAttemptActorUserID(ctx, common_service_models.UserIdType(r.request.ActorUserID))
	invocationID := controlplane.NewInvocationID()
	var outcome *commonai.GenerationOutcome
	var finalAttempt int
	execResult, err := r.controlPlane.Execute(
		ctx,
		controlplane.ExecuteRequest{
			OrganizationID: common_service_models.OrganizationIdType(r.request.OrganizationID),
			RuntimePurpose: call.Purpose,
			Route:          r.route,
			Context:        r.metadata(call),
			InvocationID:   invocationID,
		},
		func(attemptCtx context.Context, _ *controlplane.ResolvedRoute, attempt int) (controlplane.AttemptResult, error) {
			outcome = nil
			finalAttempt = attempt
			if err := r.controlPlane.RevalidateRoute(attemptCtx, r.request, r.route); err != nil {
				return controlplane.AttemptResult{}, &controlplane.ProviderCallGateError{Err: err}
			}
			result, genErr := fn(attemptCtx, commonai.GenerationAttempt{Number: attempt, InvocationID: invocationID})
			outcome = result
			return attemptResultFromOutcome(result), genErr
		},
	)
	stampControlPlaneIdentity(outcome, execResult.InvocationID, finalAttempt)
	return outcome, err
}

// ExecuteStream runs one streaming agent generation through the control plane. The
// runtime's StreamGenerationFn calls commit exactly once before the first chunk is
// delivered downstream; the control plane treats a post-commit error as terminal.
// The invocation ID and step follow the same contract as Execute.
func (r *routeBoundRetrier) ExecuteStream(ctx context.Context, call commonai.GenerationCallContext, _ commonai.GenerationStep, fn commonai.StreamGenerationFn) (*commonai.GenerationOutcome, error) {
	if err := r.validateScope(call); err != nil {
		return nil, err
	}
	// The captured durable actor survives background recovery and later nudges.
	ctx = controlplane.WithAttemptActorUserID(ctx, common_service_models.UserIdType(r.request.ActorUserID))
	invocationID := controlplane.NewInvocationID()
	var outcome *commonai.GenerationOutcome
	var finalAttempt int
	execResult, err := r.controlPlane.ExecuteStream(
		ctx,
		controlplane.ExecuteRequest{
			OrganizationID: common_service_models.OrganizationIdType(r.request.OrganizationID),
			RuntimePurpose: call.Purpose,
			Route:          r.route,
			Context:        r.metadata(call),
			InvocationID:   invocationID,
		},
		func(attemptCtx context.Context, _ *controlplane.ResolvedRoute, attempt int, commit func()) (controlplane.AttemptResult, error) {
			outcome = nil
			finalAttempt = attempt
			if err := r.controlPlane.RevalidateRoute(attemptCtx, r.request, r.route); err != nil {
				return controlplane.AttemptResult{}, &controlplane.ProviderCallGateError{Err: err}
			}
			result, genErr := fn(attemptCtx, commonai.GenerationAttempt{Number: attempt, InvocationID: invocationID}, commit)
			outcome = result
			return attemptResultFromOutcome(result), genErr
		},
	)
	stampControlPlaneIdentity(outcome, execResult.InvocationID, finalAttempt)
	return outcome, err
}

func (r *routeBoundRetrier) validateScope(call commonai.GenerationCallContext) error {
	if call.OrganizationID <= 0 {
		return controlplane.ErrInferenceOrganizationRequired
	}
	if uint64(call.OrganizationID) != r.request.OrganizationID || uint64(call.InferenceActor()) != r.request.ActorUserID {
		return fmt.Errorf("generation scope does not match planned organization and actor: %w", apperrors.ErrInsufficientPermissions)
	}
	return nil
}

// metadata builds the sanitized usage context attached to every attempt record from the
// run's call context, delegating to the injected GenerationCallMetadataFunc. A nil func
// records an empty context, matching the pre-metadata behavior.
func (r *routeBoundRetrier) metadata(call commonai.GenerationCallContext) map[string]any {
	if r.callMetadata == nil {
		return nil
	}
	return r.callMetadata(call)
}

// stampControlPlaneIdentity records the control-plane correlation identity — the
// invocation that produced the generation and the final attempt number — onto the
// outcome so the agent runtime's recorder can key the inference ledger row on
// invocation_id:attempt_number. Failed usage-only outcomes are stamped as well;
// a nil outcome is left untouched.
func stampControlPlaneIdentity(outcome *commonai.GenerationOutcome, invocationID string, attempt int) {
	if outcome == nil {
		return
	}
	outcome.InvocationID = invocationID
	if attempt > 0 {
		outcome.AttemptNumber = uint32(attempt)
	}
}

// attemptResultFromOutcome maps a runtime generation outcome onto the control plane's
// attempt result. The recorder reads the outcome's provider-reported token counts to
// populate the ledger row and charges TokenCount to the token bucket after the attempt.
// RawResponse is left nil: the runtime does not surface the raw provider response through
// the retrier seam, and the recorder persists only token counts.
func attemptResultFromOutcome(outcome *commonai.GenerationOutcome) controlplane.AttemptResult {
	if outcome == nil {
		return controlplane.AttemptResult{}
	}
	return controlplane.AttemptResult{
		Outcome:    outcome,
		TokenCount: int64(outcome.TotalTokens),
	}
}

// Compile-time checks that the adapter satisfies both runtime generation seams.
var (
	_ commonai.GenerationRetrier       = (*routeBoundRetrier)(nil)
	_ commonai.GenerationStreamRetrier = (*routeBoundRetrier)(nil)
)
