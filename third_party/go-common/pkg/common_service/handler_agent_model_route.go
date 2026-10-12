package common_service

import (
	"context"
	"fmt"

	"github.com/Suhaibinator/go-common/pkg/common_service/agentkit"
	commonai "github.com/Suhaibinator/go-common/pkg/common_service/ai"
	"github.com/Suhaibinator/go-common/pkg/common_service/common_service_models"
	apperrors "github.com/Suhaibinator/go-common/pkg/common_service/errors"
	"github.com/Suhaibinator/go-common/pkg/common_service/ports"
	"github.com/Suhaibinator/go-common/pkg/genai/inference"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
)

// AgentOperationRouteStore is the narrow control-plane read surface the store-backed generation
// planner resolves models through. It lives in ports so persistence-side stores can satisfy it
// without importing this package; the alias keeps the name NewStoreGenerationPlanner documents.
type AgentOperationRouteStore = ports.AgentOperationRouteStore

// The packaged wiring path (agentkit.Registrar) drives this handler through agentkit.CommonRuntime,
// so any drift between the two breaks here at compile time rather than at the first drive.
var _ agentkit.CommonRuntime = (*CommonServiceHandler[common_service_models.User])(nil)

// SetAgentGenerationPlanner wires the generation planner every drive plans through: at the start of
// each drive the runtime calls PlanGeneration with the agent's operation and derives the whole
// drive from the returned plan (the model the generator is built from, the catalog row the ledger
// stamps, and the plan's route-bound retrier). It is a PURE setter — no reads, no validation — so
// it can run at any point of startup wiring; call ValidateAgentRoutes afterwards to prove the
// registered agents actually plan. A nil handler is a no-op, matching the other seam setters, and
// a nil planner clears the seam (every later drive then fails with
// ErrAgentOperationRouteUnavailable).
//
// Production wiring is genbridge.NewControlPlanePlanner (route-bound retry, rate limiting, and
// attempt-ledger recording). NewStoreGenerationPlanner resolves catalog rows only; because it does
// not bind a retrier, runtime/startup validation rejects it unless a test adapter adds one.
func (h *CommonServiceHandler[U]) SetAgentGenerationPlanner(planner commonai.GenerationPlanner) {
	if h == nil {
		return
	}
	h.ensureAgentRuntime()
	h.agentRuntime.planner = planner
	if h.chatHandler != nil {
		h.chatHandler.SetInferenceModelSelection(planner, h)
	}
}

// ValidateAgentRoutes proves every already-registered agent's operation currently resolves to a
// usable generation plan, and fails startup when one does not. It is the startup check for
// applications wiring by hand (the agentkit registrar performs the equivalent check in its plan
// phase): call it LAST, after SetAgentGenerationPlanner and after registering every agent type,
// because a missing or broken route must fail startup rather than the first production drive that
// happens to need it. Registry validation itself stays struct-only, so this is the only place the
// route rows are proven to exist. Validation covers exactly the agents already registered when it
// runs: an agent registered afterwards still plans its drive per drive and still fails that drive
// with a typed error, it just loses the startup check.
//
// Both the nil receiver and a missing planner return an error rather than skipping quietly: this
// method's return value IS the startup contract, so a wiring bug must not report a healthy startup
// while every later drive fails.
//
// The per-agent plans are NOT retried here, and go-common does not decide what an unreadable
// control plane means for a boot. Failures wrap their cause with multi-%w, so a caller that wants
// to survive a connection blip during a rolling deploy can treat
// common_persistence.ErrControlPlaneRouteNotFound / ErrControlPlaneModelNotFound as terminal
// misconfiguration and retry anything else with its own bounded backoff. Configuration errors
// the planner decides itself, such as an inactive catalog row, carry no
// persistence sentinel and fall on the retryable side of that split, so a caller's retry budget is
// spent before the same error is reported.
func (h *CommonServiceHandler[U]) ValidateAgentRoutes(ctx context.Context) error {
	if h == nil {
		return fmt.Errorf("%w: nil service handler", apperrors.ErrAgentOperationRouteUnavailable)
	}
	if h.agentRuntime == nil || h.agentRuntime.planner == nil {
		return apperrors.ErrAgentOperationRouteUnavailable
	}
	// No registry means no agents have been (or can be) registered yet, so there is nothing to
	// prove. Every initialized handler has one.
	if h.agentRegistry == nil {
		return nil
	}
	for _, def := range h.agentRegistry.ListAgentTypes() {
		plan, err := h.agentRuntime.planner.PlanGeneration(ctx, inference.GenerationRequest{Operation: int32(def.Operation)})
		if err != nil {
			return fmt.Errorf("agent runtime: agent type %q: %w", def.Type, err)
		}
		if plan == nil {
			return fmt.Errorf("agent runtime: agent type %q: %w: operation %d planned no generation",
				def.Type, apperrors.ErrAgentOperationRouteUnresolved, def.Operation)
		}
		if !commonai.GenerationRetrierConfigured(plan.Retrier) {
			return fmt.Errorf("agent runtime: agent type %q: %w: operation %d: %w",
				def.Type, apperrors.ErrAgentOperationRouteUnavailable, def.Operation, commonai.ErrGenerationRetrierRequired)
		}
	}
	return nil
}

// NewStoreGenerationPlanner builds a catalog-only commonai.GenerationPlanner over a raw
// operation-route store: operation -> InferenceOperationRoute -> the exact InferenceModel row named
// by the route, with the plan's Retrier left nil. It exists for resolution tests and migration
// tooling; the agent runtime deliberately rejects its plans because a direct provider attempt would
// bypass control-plane attribution. Production wiring uses genbridge.NewControlPlanePlanner, whose
// plans carry a retrier bound to the same resolved route.
//
// The runtime resolves a session's durable model/API pin on every drive.
func NewStoreGenerationPlanner(store AgentOperationRouteStore) commonai.GenerationPlanner {
	return &storeGenerationPlanner{store: store}
}

// storeGenerationPlanner is the store-backed GenerationPlanner behind NewStoreGenerationPlanner.
type storeGenerationPlanner struct {
	store AgentOperationRouteStore
}

// PlanGeneration resolves one drive's generation plan from the route store. Every failure wraps
// the ErrAgentOperationRouteUnavailable / ErrAgentOperationRouteUnresolved taxonomy so callers can
// split wiring bugs from unresolvable configuration.
func (p *storeGenerationPlanner) PlanGeneration(ctx context.Context, request inference.GenerationRequest) (*commonai.GenerationPlan, error) {
	operation := int32(request.Operation)

	if p == nil || p.store == nil {
		return nil, fmt.Errorf("%w: operation %d", apperrors.ErrAgentOperationRouteUnavailable, operation)
	}
	if operation == 0 {
		return nil, fmt.Errorf("%w: operation is unset", apperrors.ErrAgentOperationRouteUnresolved)
	}
	route, err := p.store.GetRoute(ctx, operation)
	if err != nil {
		return nil, fmt.Errorf("%w: operation %d: %w", apperrors.ErrAgentOperationRouteUnresolved, operation, err)
	}
	if route == nil || route.InferenceModelID == 0 {
		return nil, fmt.Errorf("%w: operation %d has no inference model", apperrors.ErrAgentOperationRouteUnresolved, operation)
	}
	if err := request.ModelSelection.Validate(); err != nil {
		return nil, fmt.Errorf("%w: %w", apperrors.ErrAgentOperationRouteUnresolved, err)
	}
	if request.ModelSelection != nil {
		cloned := *route
		cloned.InferenceModelID = request.ModelSelection.InferenceModelID
		if request.ModelSelection.API != nil {
			cloned.API = *request.ModelSelection.API
		}
		route = &cloned
	}
	model, err := p.store.GetModel(ctx, route.InferenceModelID)
	if err != nil {
		return nil, fmt.Errorf("%w: operation %d model %d: %w", apperrors.ErrAgentOperationRouteUnresolved, operation, route.InferenceModelID, err)
	}
	if model == nil || model.ID == 0 {
		return nil, fmt.Errorf("%w: operation %d model %d is missing", apperrors.ErrAgentOperationRouteUnresolved, operation, route.InferenceModelID)
	}
	if !model.Active {
		return nil, fmt.Errorf("%w: operation %d model %d is inactive", apperrors.ErrAgentOperationRouteUnresolved, operation, model.ID)
	}
	if model.ProviderModelID == "" || model.ProviderID == "" {
		return nil, fmt.Errorf("%w: operation %d model %d has no provider/model id", apperrors.ErrAgentOperationRouteUnresolved, operation, model.ID)
	}

	plan := &commonai.GenerationPlan{
		Operation:        request.Operation,
		API:              route.API,
		ProviderID:       model.ProviderID,
		ProviderModelID:  model.ProviderModelID,
		InferenceModelID: model.ID,
		SelectionSource:  proto_models.InferenceSelectionSource_INFERENCE_SELECTION_SOURCE_PLATFORM_DEFAULT,
	}
	if request.ModelSelection != nil && request.ModelSelection.SelectionSource != proto_models.InferenceSelectionSource_INFERENCE_SELECTION_SOURCE_UNSPECIFIED {
		plan.SelectionSource = request.ModelSelection.SelectionSource
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
