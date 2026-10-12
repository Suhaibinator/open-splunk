package chat

import (
	"context"
	"errors"
	"fmt"

	"github.com/Suhaibinator/go-common/pkg/common_json"
	persistence "github.com/Suhaibinator/go-common/pkg/common_persistence"
	pm "github.com/Suhaibinator/go-common/pkg/common_persistence/common_persistence_models"
	serviceErrors "github.com/Suhaibinator/go-common/pkg/common_service/errors"
	"github.com/Suhaibinator/go-common/pkg/common_service/ports"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
)

// CreateAgentChatSessionInLocationInput installs a session in an existing channel.
// ParentMessageID is zero for the main timeline or the ID of a visible thread root.
// Location and organization are immutable. The actor must administer the channel;
// UserID must already participate. A missing registered bot is added through the
// normal participant-management policy. Callers must roll back the ambient
// transaction on error, including any bot addition; execution starts after commit.
// OrganizationID is billing/ownership scope, not a grant of organization access.
type CreateAgentChatSessionInLocationInput struct {
	AgentSessionOptions
	ActorUserID     uint64
	ChannelID       uint64
	ParentMessageID uint64
}

// GetAgentChatSessionInLocationInput selects exactly one location, never a fallback.
type GetAgentChatSessionInLocationInput struct {
	ActorUserID     uint64
	ChannelID       uint64
	ParentMessageID uint64
}

// CreateAgentChatSessionInLocation requires an ambient transaction and performs
// authorization again on retries. Archived sessions continue to occupy their location.
func (h *Handler[U]) CreateAgentChatSessionInLocation(ctx context.Context, input CreateAgentChatSessionInLocationInput) (*CreateAgentChatSessionResult, error) {
	if h.chatDisabled() {
		return nil, serviceErrors.ErrChatDisabled
	}
	if input.ChannelID == 0 || input.OrganizationID <= 0 || input.IdempotencyKey == "" {
		return nil, serviceErrors.ErrInvalidRequest
	}
	if err := h.requireAgentCreateContext(ctx); err != nil {
		return nil, err
	}
	actor, actorID, err := h.resolveAgentSessionActor(ctx, input.ActorUserID)
	if err != nil {
		return nil, err
	}
	sharedInput := CreateAgentChatSessionInput{
		AgentSessionOptions: input.AgentSessionOptions,
		ActorUserID:         actorID,
	}
	prepared, err := h.prepareAgentSessionCreate(sharedInput, actorID)
	if err != nil {
		return nil, err
	}
	if !prepared.agent.CanBeRoot() {
		return nil, fmt.Errorf("%w: agent type %q exists only as a delegation child", serviceErrors.ErrInvalidRequest, prepared.agent.Type)
	}
	sharedInput = prepared.input
	sharedInput.routingAuthority, err = h.captureInferenceAuthority()
	if err != nil {
		return nil, err
	}
	if err := h.prepareInitialModelSelection(ctx, &sharedInput, actorID); err != nil {
		return nil, err
	}
	prepared.input = sharedInput
	channel, err := h.persistenceHandler.LockChatChannelForAgentSession(ctx, input.ChannelID)
	if err != nil {
		return nil, mapAgentSessionLocationError(err)
	}
	// Deliberately use the locked row's freshly loaded roster, not the access cache.
	role, member := agentLocationMember(channel, actorID)
	if !member {
		return nil, serviceErrors.ErrChatNotMember
	}
	if role != proto_models.ChannelRole_CHANNEL_ROLE_ADMIN && role != proto_models.ChannelRole_CHANNEL_ROLE_OWNER {
		return nil, serviceErrors.ErrInsufficientPermissions
	}
	if channel.IsArchived {
		return nil, serviceErrors.ErrChatChannelArchived
	}
	if _, member := agentLocationMember(channel, prepared.effectiveUserID); !member {
		return nil, serviceErrors.ErrChatNotMember
	}
	if channel.OrganizationID != nil && *channel.OrganizationID != int64(input.OrganizationID) {
		return nil, serviceErrors.ErrChatAgentSessionScopeInvalid
	}
	serviceChannel := h.converter.PersistenceChatChannelToService(channel)
	if err := h.validateThreadAndReplyReferences(ctx, serviceChannel, input.ParentMessageID, 0, false); err != nil {
		return nil, err
	}
	if err := h.authorize(ctx, ports.ChatPermissionRequest[U]{
		Operation: ports.ChatPermissionCreateAgentSession, Actor: actor, Channel: serviceChannel,
		ParentMessageID: input.ParentMessageID, ProposedAgentType: input.AgentType,
		ProposedBotUserID: uint64(prepared.agent.BotUserID), ProposedAgentUserID: prepared.effectiveUserID,
		ProposedOrganizationID: input.OrganizationID,
	}); err != nil {
		return nil, err
	}
	// Context is copied into a newly created session, but is not part of request
	// identity: edits to the channel's navigation metadata must not break a retry.
	fingerprint, err := fingerprintAgentSessionLocation(actorID, input.ChannelID, input.ParentMessageID, prepared)
	if err != nil {
		return nil, serviceErrors.ErrInternalError
	}
	existing, err := h.persistenceHandler.GetChatAgentSessionByCreateIdempotencyKey(ctx, int64(input.OrganizationID), actorID, prepared.effectiveUserID, input.AgentType, input.IdempotencyKey)
	if err != nil && !errors.Is(err, persistence.ErrChatAgentSessionNotFound) {
		return nil, mapAgentSessionLocationError(err)
	}
	if existing != nil {
		return h.replayAgentSessionCreate(ctx, actor, existing, fingerprint)
	}
	if err := h.resolveInitialModelSelection(ctx, &sharedInput, prepared.agent.Operation); err != nil {
		return nil, err
	}
	occupant, err := h.persistenceHandler.GetChatAgentSessionInLocation(ctx, input.ChannelID, input.ParentMessageID)
	if err != nil && !errors.Is(err, persistence.ErrChatAgentSessionNotFound) {
		return nil, mapAgentSessionLocationError(err)
	}
	if occupant != nil {
		return nil, serviceErrors.ErrChatAgentSessionLocationConflict
	}
	if _, present := agentLocationMember(channel, uint64(prepared.agent.BotUserID)); !present {
		// Invalidate this actor's cached role first so the existing add path also sees
		// the current roster and archived state checked above.
		h.invalidateChannelAccessCache(ctx, channel.ID, actorID)
		if _, err := h.AddChannelParticipants(ctx, channel.ID, []uint64{uint64(prepared.agent.BotUserID)}); err != nil {
			return nil, err
		}
	}
	session := &pm.ChatAgentSession{
		OrganizationID: int64(input.OrganizationID), ChannelID: input.ChannelID, ParentMessageID: input.ParentMessageID,
		AgentType: input.AgentType, BotUserID: uint64(prepared.agent.BotUserID), UserID: prepared.effectiveUserID,
		CreatedByUserID: actorID, InferenceActorUserID: input.InferenceActorUserID, ActiveConfigVersion: 1, Status: pm.ChatAgentSessionStatusActive,
		AutoRunPolicy: pm.ChatAgentSessionAutoRunPolicy(input.AutoRunPolicy),
		ContextID:     channel.ContextID, ContextEntityID: channel.ContextEntityID, MetadataJSON: prepared.metadataJSON,
		CreateIdempotencyKey: &input.IdempotencyKey, CreateIdempotencyFingerprint: &fingerprint,
	}
	session.ChatAgentSessionInferencePin = SessionInferencePin(sharedInput.initialModelPlan, actorID)
	if err := h.persistenceHandler.CreateChatAgentSession(ctx, session); err != nil {
		return nil, mapAgentSessionLocationError(err)
	}
	return h.finishAgentSessionCreate(ctx, channel, session, actorID, sharedInput, prepared)
}

func agentLocationMember(channel *pm.ChatChannel, userID uint64) (proto_models.ChannelRole, bool) {
	for _, participant := range channel.Participants {
		if participant.UserID == userID && participant.LeftAt == nil {
			return participant.Role, true
		}
	}
	return proto_models.ChannelRole_CHANNEL_ROLE_UNSPECIFIED, false
}

func fingerprintAgentSessionLocation(actorID, channelID, parentMessageID uint64, prepared *preparedAgentSessionCreate) (string, error) {
	base, err := fingerprintAgentSessionCreate(actorID, prepared.effectiveUserID, prepared.input, nil, prepared.tools, prepared.reasoningPolicy, prepared.maxModelSteps)
	if err != nil {
		return "", err
	}
	return canonicalSHA256(common_json.MarshalCanonical(struct {
		Operation       string `json:"operation"`
		ChannelID       uint64 `json:"channel_id"`
		ParentMessageID uint64 `json:"parent_message_id"`
		BotUserID       uint64 `json:"bot_user_id"`
		Request         string `json:"request"`
	}{"create_agent_session_in_location", channelID, parentMessageID, uint64(prepared.agent.BotUserID), base}))
}

func (h *Handler[U]) GetAgentChatSessionInLocation(ctx context.Context, input GetAgentChatSessionInLocationInput) (*GetAgentChatSessionResult, error) {
	if h.chatDisabled() {
		return nil, serviceErrors.ErrChatDisabled
	}
	if input.ChannelID == 0 {
		return nil, serviceErrors.ErrInvalidRequest
	}
	actor, _, err := h.resolveAgentSessionActor(ctx, input.ActorUserID)
	if err != nil {
		return nil, err
	}
	channel, err := h.readChannelAuthorized(ctx, actor, input.ChannelID)
	if err != nil {
		return nil, err
	}
	session, err := h.persistenceHandler.GetChatAgentSessionInLocation(ctx, input.ChannelID, input.ParentMessageID)
	if err != nil {
		return nil, mapAgentSessionLocationError(err)
	}
	if session == nil {
		return nil, serviceErrors.ErrChatAgentSessionNotFound
	}
	return &GetAgentChatSessionResult{Channel: channel, AgentSession: h.converter.PersistenceChatAgentSessionToService(session)}, nil
}

func mapAgentSessionLocationError(err error) error {
	switch {
	case errors.Is(err, persistence.ErrChatAgentSessionLocationConflict):
		return serviceErrors.ErrChatAgentSessionLocationConflict
	case errors.Is(err, persistence.ErrChatAgentSessionScopeInvalid):
		return serviceErrors.ErrChatAgentSessionScopeInvalid
	case errors.Is(err, persistence.ErrChatAgentSessionIdempotencyConflict):
		return serviceErrors.ErrChatAgentSessionIdempotencyConflict
	case errors.Is(err, persistence.ErrChatAgentSessionNotFound):
		return serviceErrors.ErrChatAgentSessionNotFound
	case errors.Is(err, persistence.ErrChatChannelNotFound):
		return serviceErrors.ErrChatChannelNotFound
	case errors.Is(err, persistence.ErrDatabaseError):
		return serviceErrors.ErrDatabaseError
	default:
		return serviceErrors.ErrInternalError
	}
}
