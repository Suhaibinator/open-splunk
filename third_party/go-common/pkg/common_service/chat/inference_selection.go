package chat

import (
	"context"
	"errors"
	"fmt"

	persistence "github.com/Suhaibinator/go-common/pkg/common_persistence"
	models "github.com/Suhaibinator/go-common/pkg/common_persistence/common_persistence_models"
	"github.com/Suhaibinator/go-common/pkg/common_service/agentkit"
	commonai "github.com/Suhaibinator/go-common/pkg/common_service/ai"
	serviceErrors "github.com/Suhaibinator/go-common/pkg/common_service/errors"
	"github.com/Suhaibinator/go-common/pkg/common_service/internal/inferenceerr"
	"github.com/Suhaibinator/go-common/pkg/common_service/ports"
	"github.com/Suhaibinator/go-common/pkg/genai/inference"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
)

// SetInferenceModelSelection wires trusted authorization and the runtime's existing planner.
func (h *Handler[U]) SetInferenceModelSelection(planner commonai.GenerationPlanner, authorizer ports.InferenceModelOverrideAuthorizer) {
	h.selectionPlanner, h.overrideAuthorizer = planner, authorizer
}

func (h *Handler[U]) prepareInitialModelSelection(ctx context.Context, input *CreateAgentChatSessionInput, actorID uint64) error {
	input.ActorUserID = actorID
	if input.InferenceActorUserID != nil {
		// The initial model is planned for the accountable human, not the (bot) creator.
		user, err := h.persistenceHandler.GetUserByID(ctx, *input.InferenceActorUserID)
		if err != nil && !errors.Is(err, persistence.ErrUserNotFound) {
			return serviceErrors.ErrDatabaseError
		}
		if user == nil || user.IsBot || user.Banned {
			return fmt.Errorf("%w: InferenceActorUserID must name an active human user", serviceErrors.ErrInvalidRequest)
		}
		input.ActorUserID = *input.InferenceActorUserID
	}
	if input.ModelSelection == nil {
		return nil
	}
	if err := input.ModelSelection.Validate(); err != nil {
		return fmt.Errorf("%w: %w", serviceErrors.ErrInvalidRequest, err)
	}
	input.ModelSelection = input.ModelSelection.Clone()
	switch input.ModelSelection.SelectionSource {
	case proto_models.InferenceSelectionSource_INFERENCE_SELECTION_SOURCE_USER_CHOICE:
	case proto_models.InferenceSelectionSource_INFERENCE_SELECTION_SOURCE_SITE_OVERRIDE:
		return h.authorizeSiteOverride(ctx, input.routingAuthority, actorID)
	default:
		return serviceErrors.ErrInvalidRequest
	}
	return nil
}

// authorizeSiteOverride confirms actorID holds the site override permission, through the
// captured routing authority when one is supplied and the wired authorizer otherwise.
func (h *Handler[U]) authorizeSiteOverride(ctx context.Context, authority ports.InferenceRoutingAuthority, actorID uint64) error {
	var authorizer ports.InferenceModelOverrideAuthorizer = h.overrideAuthorizer
	if authority != nil {
		authorizer = authority
	}
	if authorizer == nil {
		return serviceErrors.ErrInsufficientPermissions
	}
	authorizedID, err := authorizer.AuthorizeInferenceModelOverride(ctx)
	if err != nil {
		return err
	}
	if uint64(authorizedID) != actorID {
		return serviceErrors.ErrInsufficientPermissions
	}
	return nil
}

func (h *Handler[U]) resolveInitialModelSelection(ctx context.Context, input *CreateAgentChatSessionInput, operation int32) error {
	if h.selectionPlanner == nil {
		return serviceErrors.ErrAgentOperationRouteUnavailable
	}
	if input.InferenceActorUserID != nil {
		// Routing requires the context user, when present, to be the actor; the creator is not.
		ctx = agentkit.ContextWithoutActor[U](ctx)
	}
	plan, err := h.selectionPlanner.PlanGeneration(ctx, inference.GenerationRequest{
		Operation: operation, ModelSelection: input.ModelSelection,
		OrganizationID: uint64(input.OrganizationID), ActorUserID: input.ActorUserID,
		Authority: input.routingAuthority,
	})
	if err != nil {
		return inferenceerr.Translate(err)
	}
	if plan == nil || plan.InferenceModelID == 0 || plan.ProviderID == "" || plan.ProviderModelID == "" || plan.API == 0 || !commonai.GenerationRetrierConfigured(plan.Retrier) {
		return fmt.Errorf("%w: selected model requires a complete execution plan", serviceErrors.ErrAgentOperationRouteUnresolved)
	}
	if err := (&inference.ModelSelection{InferenceModelID: plan.InferenceModelID, API: &plan.API}).Validate(); err != nil {
		return err
	}
	if input.ModelSelection != nil && (input.ModelSelection.InferenceModelID != plan.InferenceModelID ||
		(input.ModelSelection.API != nil && *input.ModelSelection.API != 0 && *input.ModelSelection.API != plan.API)) {
		return fmt.Errorf("%w: selected API mismatch", serviceErrors.ErrAgentOperationRouteUnresolved)
	}
	input.initialModelPlan = plan
	if input.ModelSelection != nil && plan.SelectionSource == proto_models.InferenceSelectionSource_INFERENCE_SELECTION_SOURCE_UNSPECIFIED {
		plan.SelectionSource = input.ModelSelection.SelectionSource
	}
	return nil
}

func (h *Handler[U]) captureInferenceAuthority() (ports.InferenceRoutingAuthority, error) {
	if capturer, present := h.overrideAuthorizer.(ports.InferenceAuthorityCapturer); present {
		return capturer.CaptureInferenceAuthority()
	}
	return nil, nil
}

func SessionInferencePin(plan *commonai.GenerationPlan, selectingUserID uint64) models.ChatAgentSessionInferencePin {
	source := plan.SelectionSource
	if source == proto_models.InferenceSelectionSource_INFERENCE_SELECTION_SOURCE_UNSPECIFIED {
		source = proto_models.InferenceSelectionSource_INFERENCE_SELECTION_SOURCE_PLATFORM_DEFAULT
	}
	pin := models.ChatAgentSessionInferencePin{
		InferenceOperation:        plan.Operation,
		ExecutionInferenceModelID: new(plan.InferenceModelID), ExecutionInferenceAPI: new(plan.API),
		ExecutionProviderID: new(plan.ProviderID), ExecutionProviderModelID: new(plan.ProviderModelID),
		SelectionSource: source, InferencePinVersion: 1,
		MemberChoiceSelection: source == proto_models.InferenceSelectionSource_INFERENCE_SELECTION_SOURCE_USER_CHOICE,
		OperationListVersion:  plan.OperationListVersion, OrganizationPolicyVersion: plan.OrganizationPolicyVersion,
		OrganizationOperationPolicyVersion: plan.OrganizationOperationPolicyVersion,
	}
	if source == proto_models.InferenceSelectionSource_INFERENCE_SELECTION_SOURCE_USER_CHOICE ||
		source == proto_models.InferenceSelectionSource_INFERENCE_SELECTION_SOURCE_SITE_OVERRIDE ||
		source == proto_models.InferenceSelectionSource_INFERENCE_SELECTION_SOURCE_RETARGET {
		pin.SelectingUserID = new(selectingUserID)
	}
	if source == proto_models.InferenceSelectionSource_INFERENCE_SELECTION_SOURCE_SITE_OVERRIDE {
		pin.SiteOverrideUserID = new(selectingUserID)
	}
	return pin
}
