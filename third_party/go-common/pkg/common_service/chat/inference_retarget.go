package chat

import (
	"context"
	"errors"
	"math"
	"sort"

	persistence "github.com/Suhaibinator/go-common/pkg/common_persistence"
	models "github.com/Suhaibinator/go-common/pkg/common_persistence/common_persistence_models"
	"github.com/Suhaibinator/go-common/pkg/common_service/common_service_models"
	serviceErrors "github.com/Suhaibinator/go-common/pkg/common_service/errors"
	"github.com/Suhaibinator/go-common/pkg/common_service/ports"
	"github.com/Suhaibinator/go-common/pkg/genai/inference"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
)

type RetargetAgentChatSessionInput struct {
	Authority          ports.InferenceRoutingAuthority
	ActorUserID        uint64
	AgentSessionID     uint64
	ExpectedPinVersion uint64
	ModelSelection     *inference.ModelSelection
}

type BulkRetargetAgentChatSessionsInput struct {
	Authority           ports.InferenceRoutingAuthority
	ActorUserID         uint64
	OrganizationID      uint64
	AgentSessionIDs     []uint64
	ExpectedPinVersions map[uint64]uint64
	ModelSelection      *inference.ModelSelection
}

func (h *Handler[U]) RetargetAgentChatSession(ctx context.Context, input RetargetAgentChatSessionInput) (*common_service_models.ChatAgentSession, error) {
	if h.chatDisabled() {
		return nil, serviceErrors.ErrChatDisabled
	}
	if input.AgentSessionID == 0 || input.ModelSelection == nil || input.ModelSelection.Validate() != nil {
		return nil, serviceErrors.ErrInvalidRequest
	}
	if err := h.requireTransaction(ctx); err != nil {
		return nil, err
	}
	authority, err := h.authorityOrCaptured(input.Authority)
	if err != nil {
		return nil, err
	}
	_, actorID, err := h.resolveAgentSessionActor(ctx, input.ActorUserID)
	if err != nil {
		return nil, err
	}
	session, err := h.persistenceHandler.GetChatAgentSessionByID(ctx, input.AgentSessionID)
	if err != nil {
		return nil, mapAgentSessionLocationError(err)
	}
	operation, err := h.retargetSessionOperation(session)
	if err != nil {
		return nil, err
	}
	management, err := h.authorizeInferenceRetarget(ctx, authority, uint64(session.OrganizationID), session.UserID, actorID, operation)
	if err != nil {
		return nil, err
	}
	pin, err := h.resolveRetargetPin(ctx, authority, session, operation, input.ModelSelection, actorID, management)
	if err != nil {
		return nil, err
	}
	retargeted, err := h.persistenceHandler.BatchRetargetChatAgentSessionInferencePins(ctx, session.OrganizationID, []uint64{session.ID}, map[uint64]uint64{session.ID: input.ExpectedPinVersion}, pin, actorID)
	if err != nil {
		return nil, mapInferenceRetargetError(err)
	}
	session = &retargeted[0]
	if err := h.scheduleRetargetedSessions(ctx, []models.ChatAgentSession{*session}); err != nil {
		return nil, err
	}
	return h.converter.PersistenceChatAgentSessionToService(session), nil
}

func (h *Handler[U]) BulkRetargetAgentChatSessions(ctx context.Context, input BulkRetargetAgentChatSessionsInput) ([]*common_service_models.ChatAgentSession, error) {
	if h.chatDisabled() {
		return nil, serviceErrors.ErrChatDisabled
	}
	authority, err := h.authorityOrCaptured(input.Authority)
	if err != nil {
		return nil, err
	}
	limit := 0
	if authority != nil {
		limit = authority.InferenceRetargetBatchSize()
	} else {
		limit = h.policy().InferenceRetargetBatchSize()
	}
	if input.OrganizationID == 0 || input.OrganizationID > math.MaxInt64 || len(input.AgentSessionIDs) == 0 || len(input.AgentSessionIDs) > limit || len(input.ExpectedPinVersions) != len(input.AgentSessionIDs) || input.ModelSelection == nil || input.ModelSelection.Validate() != nil {
		return nil, serviceErrors.ErrInvalidRequest
	}
	seen := make(map[uint64]bool, len(input.AgentSessionIDs))
	for _, sessionID := range input.AgentSessionIDs {
		_, versionPresent := input.ExpectedPinVersions[sessionID]
		if sessionID == 0 || seen[sessionID] || !versionPresent {
			return nil, serviceErrors.ErrInvalidRequest
		}
		seen[sessionID] = true
	}
	if err := h.requireTransaction(ctx); err != nil {
		return nil, err
	}
	_, actorID, err := h.resolveAgentSessionActor(ctx, input.ActorUserID)
	if err != nil {
		return nil, err
	}
	// Classify alone before any read so a non-manager learns nothing about the sessions; with no
	// owner only managers pass.
	management, err := h.authorizeInferenceRetarget(ctx, authority, input.OrganizationID, 0, actorID)
	if err != nil {
		return nil, err
	}
	if !management {
		return nil, serviceErrors.ErrInsufficientPermissions
	}
	sessions, err := h.persistenceHandler.GetChatAgentSessionsByIDs(ctx, int64(input.OrganizationID), input.AgentSessionIDs)
	if err != nil {
		return nil, mapInferenceRetargetError(err)
	}
	if len(sessions) != len(input.AgentSessionIDs) {
		return nil, serviceErrors.ErrChatAgentSessionNotFound
	}
	groups := make(map[int32][]models.ChatAgentSession)
	for _, session := range sessions {
		operation, err := h.retargetSessionOperation(&session)
		if err != nil {
			return nil, err
		}
		groups[operation] = append(groups[operation], session)
	}
	operations := make([]int32, 0, len(groups))
	for operation := range groups {
		operations = append(operations, operation)
	}
	sort.Slice(operations, func(first, second int) bool { return operations[first] < operations[second] })
	// Every operation's gate is checked before any resolution or write.
	if _, err := h.authorizeInferenceRetarget(ctx, authority, input.OrganizationID, 0, actorID, operations...); err != nil {
		return nil, err
	}
	updated := make([]models.ChatAgentSession, 0, len(sessions))
	for _, operation := range operations {
		group := groups[operation]
		pin, err := h.resolveRetargetPin(ctx, authority, &group[0], operation, input.ModelSelection, actorID, true)
		if err != nil {
			return nil, err
		}
		ids := make([]uint64, len(group))
		versions := make(map[uint64]uint64, len(group))
		for index, session := range group {
			ids[index] = session.ID
			versions[session.ID] = input.ExpectedPinVersions[session.ID]
		}
		changed, err := h.persistenceHandler.BatchRetargetChatAgentSessionInferencePins(ctx, int64(input.OrganizationID), ids, versions, pin, actorID)
		if err != nil {
			return nil, mapInferenceRetargetError(err)
		}
		updated = append(updated, changed...)
	}
	if err := h.scheduleRetargetedSessions(ctx, updated); err != nil {
		return nil, err
	}
	sort.Slice(updated, func(first, second int) bool { return updated[first].ID < updated[second].ID })
	result := make([]*common_service_models.ChatAgentSession, len(updated))
	for index := range updated {
		result[index] = h.converter.PersistenceChatAgentSessionToService(&updated[index])
	}
	return result, nil
}

// authorityOrCaptured returns the caller's routing authority, or captures one from the wired
// authorizer so a re-target evaluates every check against one configuration generation.
func (h *Handler[U]) authorityOrCaptured(authority ports.InferenceRoutingAuthority) (ports.InferenceRoutingAuthority, error) {
	if authority != nil {
		return authority, nil
	}
	return h.captureInferenceAuthority()
}

func (h *Handler[U]) retargetAuthorizer(authority ports.InferenceRoutingAuthority) (ports.InferenceSessionRetargetAuthorizer, bool) {
	if authority != nil {
		return authority, true
	}
	authorizer, present := h.overrideAuthorizer.(ports.InferenceSessionRetargetAuthorizer)
	return authorizer, present
}

// authorizeInferenceRetarget classifies the actor and applies each operation's re-target gate
// before any resolution, and reports whether the actor moves as a manager.
func (h *Handler[U]) authorizeInferenceRetarget(ctx context.Context, authority ports.InferenceRoutingAuthority, organizationID, ownerID, actorID uint64, operations ...int32) (bool, error) {
	authorizer, present := h.retargetAuthorizer(authority)
	if !present {
		return false, serviceErrors.ErrInsufficientPermissions
	}
	authorizedActorID, management, err := authorizer.AuthorizeInferenceSessionRetarget(ctx, organizationID, ownerID, operations...)
	if err != nil {
		return false, err
	}
	if uint64(authorizedActorID) != actorID {
		return false, serviceErrors.ErrInsufficientPermissions
	}
	return management, nil
}

func (h *Handler[U]) retargetSessionOperation(session *models.ChatAgentSession) (int32, error) {
	if h.agentRegistry == nil {
		return 0, serviceErrors.ErrAgentOperationRouteUnavailable
	}
	agent, present := h.agentRegistry.GetAgentType(session.AgentType)
	if !present || (session.InferenceOperation != 0 && session.InferenceOperation != agent.Operation) {
		return 0, serviceErrors.ErrAgentOperationRouteUnresolved
	}
	return agent.Operation, nil
}

func (h *Handler[U]) resolveRetargetPin(ctx context.Context, authority ports.InferenceRoutingAuthority, session *models.ChatAgentSession, operation int32, selection *inference.ModelSelection, actorID uint64, management bool) (models.ChatAgentSessionInferencePin, error) {
	selected := selection.Clone()
	if selected.SelectionSource == proto_models.InferenceSelectionSource_INFERENCE_SELECTION_SOURCE_SITE_OVERRIDE {
		if err := h.authorizeSiteOverride(ctx, authority, actorID); err != nil {
			return models.ChatAgentSessionInferencePin{}, err
		}
	} else if management {
		selected.SelectionSource = proto_models.InferenceSelectionSource_INFERENCE_SELECTION_SOURCE_RETARGET
	} else {
		selected.SelectionSource = proto_models.InferenceSelectionSource_INFERENCE_SELECTION_SOURCE_USER_CHOICE
	}
	input := CreateAgentChatSessionInput{ActorUserID: actorID, AgentSessionOptions: AgentSessionOptions{
		OrganizationID: common_service_models.OrganizationIdType(session.OrganizationID), ModelSelection: selected,
	}}
	input.routingAuthority = authority
	if err := h.resolveInitialModelSelection(ctx, &input, operation); err != nil {
		return models.ChatAgentSessionInferencePin{}, err
	}
	pin := SessionInferencePin(input.initialModelPlan, actorID)
	// The plan's source is USER_CHOICE, SITE_OVERRIDE, or RETARGET, so the pin already names actorID
	// as the selecting user.
	pin.SelectionSource = proto_models.InferenceSelectionSource_INFERENCE_SELECTION_SOURCE_RETARGET
	return pin, nil
}

// scheduleRetargetedSessions starts the work retargeted sessions retained while they were blocked:
// it drives a managed session's active runs and drains an idle session's queued input or deferred
// wake into one run. An owner-driven session's runs are never driven here; its drained run is
// announced and left for the owner, whose graph the retarget already advanced.
func (h *Handler[U]) scheduleRetargetedSessions(ctx context.Context, sessions []models.ChatAgentSession) error {
	ids := make([]uint64, len(sessions))
	managed := make(map[uint64]bool, len(sessions))
	registered := make(map[uint64]bool, len(sessions))
	for index, session := range sessions {
		ids[index] = session.ID
		if h.agentRegistry != nil {
			if agent, present := h.agentRegistry.GetAgentType(session.AgentType); present {
				managed[session.ID], registered[session.ID] = !agent.IsWorkflowOwned(), true
			}
		}
	}
	runs, err := h.persistenceHandler.ListActiveChatAgentRunsBySessions(ctx, ids)
	if err != nil {
		return serviceErrors.ErrDatabaseError
	}
	active := make(map[uint64]bool, len(runs))
	for _, run := range runs {
		if run.AgentSessionID == nil {
			continue
		}
		active[*run.AgentSessionID] = true
		if run.Status != proto_models.ChatAgentRunStatus_CHAT_AGENT_RUN_STATUS_RUNNING && run.Status != proto_models.ChatAgentRunStatus_CHAT_AGENT_RUN_STATUS_RETRYABLE {
			continue
		}
		if managed[*run.AgentSessionID] {
			h.scheduleAgentRunDrive(ctx, run.ID)
		}
	}
	idle := make([]uint64, 0, len(sessions))
	for _, session := range sessions {
		if registered[session.ID] && !active[session.ID] && !session.IsSuspended() {
			idle = append(idle, session.ID)
		}
	}
	if len(idle) == 0 {
		return nil
	}
	drained, err := h.persistenceHandler.DrainRetargetedChatAgentSessions(ctx, idle)
	if err != nil {
		return mapInferenceRetargetError(err)
	}
	for _, resumed := range drained {
		if resumed.Session == nil {
			continue
		}
		h.PublishDeliveredAgentInputAfterCommit(ctx, resumed.Session.ID, resumed.Session.ChannelID, resumed.Session.ParentMessageID, resumed.Delivery.MessageIDs())
		if resumed.Run != nil {
			h.startAgentRunAfterCommit(ctx, resumed.Session.AgentType, resumed.Run.ID, resumed.Run.ChannelID, resumed.Run.ParentMessageID)
		}
	}
	return nil
}

func mapInferenceRetargetError(err error) error {
	switch {
	case errors.Is(err, persistence.ErrChatAgentSessionInferencePinVersionConflict):
		return serviceErrors.ErrChatAgentSessionVersionConflict
	case errors.Is(err, persistence.ErrChatAgentSessionRunActive):
		return serviceErrors.ErrChatAgentSessionBusy
	case errors.Is(err, persistence.ErrChatAgentSessionInferencePinInvalid):
		return serviceErrors.ErrInvalidRequest
	default:
		return mapAgentSessionLocationError(err)
	}
}
