package chat

import (
	"cmp"
	"context"
	"errors"
	"slices"
	"time"

	"github.com/Suhaibinator/SRouter/pkg/logkeys"
	persistence "github.com/Suhaibinator/go-common/pkg/common_persistence"
	common_persistence_models "github.com/Suhaibinator/go-common/pkg/common_persistence/common_persistence_models"
	"github.com/Suhaibinator/go-common/pkg/common_service/common_service_models"
	serviceErrors "github.com/Suhaibinator/go-common/pkg/common_service/errors"
	"github.com/Suhaibinator/go-common/pkg/common_service/permission"
	"github.com/Suhaibinator/go-common/pkg/common_service/ports"
	"github.com/Suhaibinator/go-common/pkg/common_util"
	appLogger "github.com/Suhaibinator/go-common/pkg/logger"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// agentMaxPendingInputs samples the cap on a session's queued input: once per send, and once
// per queue list request as its largest page.
func (h *Handler[U]) agentMaxPendingInputs() int {
	return h.policy().ChatAgentMaxPendingInputs()
}

// AgentSessionInteraction reads the composer state of one agent session for the context actor: the
// input policy in force, whether messages reach the agent as input at all (takesAgentInput), what
// the session is doing, its waiting input, and whether the actor may stop its run or resume it,
// decided exactly as Stop and Resume decide (a check that fails reads as not allowed). It does not
// check that the actor may read the session's channel; the caller has.
func (h *Handler[U]) AgentSessionInteraction(ctx context.Context, sessionID uint64) (*proto_models.AgentSessionInteraction, error) {
	actor, _, err := h.getActor(ctx)
	if err != nil {
		return nil, err
	}
	return h.agentSessionInteraction(ctx, actor, sessionID)
}

// agentSessionInteraction is AgentSessionInteraction for a loaded actor.
func (h *Handler[U]) agentSessionInteraction(ctx context.Context, actor U, sessionID uint64) (*proto_models.AgentSessionInteraction, error) {
	state, err := h.persistenceHandler.GetChatAgentSessionInteractionState(ctx, sessionID)
	if err != nil {
		if errors.Is(err, persistence.ErrChatAgentSessionNotFound) {
			return nil, serviceErrors.ErrChatAgentSessionNotFound
		}
		return nil, serviceErrors.ErrDatabaseError
	}
	return h.agentSessionInteractionFromState(ctx, actor, state), nil
}

// agentSessionInteractionFromState builds the interaction of one read state for actor; the status
// route reads the states of many locations at once and builds each one here.
func (h *Handler[U]) agentSessionInteractionFromState(ctx context.Context, actor U, state *persistence.ChatAgentSessionInteractionState) *proto_models.AgentSessionInteraction {
	session := state.Session
	interaction := &proto_models.AgentSessionInteraction{
		AgentSessionId:    session.ID,
		InputMode:         state.Policy.InputMode,
		AllowUserStop:     state.Policy.AllowUserStop,
		Activity:          agentSessionActivity(state),
		PendingInputCount: state.PendingCount,
		AcceptsInput:      h.takesAgentInput(ctx, session),
	}
	switch {
	case session.IsSuspended():
		interaction.SuspendedByUserId = session.SuspendedByUserID
		interaction.SuspendedAt = timestamppb.New(*session.SuspendedAt)
		// A child stopped with its parent resumes once its run's cancellation commits; an archived
		// session has nothing left to resume.
		interaction.ViewerCanResume = session.IsLive() && state.ActiveRun == nil && h.authorizeAgentSessionResume(ctx, actor, session) == nil
	case state.ActiveRun != nil:
		interaction.ViewerCanStop = h.authorizeAgentRunStop(ctx, actor, session, state.ActiveRun, state.Policy) == nil
	}
	return interaction
}

// agentSessionActivity derives what a session is doing: suspended, else its active run's status,
// else idle.
func agentSessionActivity(state *persistence.ChatAgentSessionInteractionState) proto_models.ChatAgentSessionActivity {
	if state.Session.IsSuspended() {
		return proto_models.ChatAgentSessionActivity_CHAT_AGENT_SESSION_ACTIVITY_SUSPENDED
	}
	if state.ActiveRun == nil {
		return proto_models.ChatAgentSessionActivity_CHAT_AGENT_SESSION_ACTIVITY_IDLE
	}
	switch state.ActiveRun.Status {
	case proto_models.ChatAgentRunStatus_CHAT_AGENT_RUN_STATUS_WAITING_FOR_APPROVAL:
		return proto_models.ChatAgentSessionActivity_CHAT_AGENT_SESSION_ACTIVITY_WAITING_FOR_APPROVAL
	case proto_models.ChatAgentRunStatus_CHAT_AGENT_RUN_STATUS_RETRYABLE:
		return proto_models.ChatAgentSessionActivity_CHAT_AGENT_SESSION_ACTIVITY_RETRYING
	default:
		return proto_models.ChatAgentSessionActivity_CHAT_AGENT_SESSION_ACTIVITY_RUNNING
	}
}

// authorizeAgentRunStop decides whether actor may stop run, an active run of session whose
// captured interaction policy is policy. nil allows it; otherwise it is the error the Stop fails
// with. ErrChatAgentStopNotAllowed: the policy does not allow user Stop, or the agent is
// workflow-owned (its owner drives every run). ErrInsufficientPermissions: the actor may neither
// resolve the run's tool calls (resolvesToolCalls: the triggering user, site tool-approval admins,
// and channel admins when the captured approval policy lets them) nor sent input the run admitted,
// or, not being a site tool-approval admin, no longer participates in the session's channel.
// Otherwise the application's Authorize callback decides (ChatPermissionStopAgentRun).
func (h *Handler[U]) authorizeAgentRunStop(
	ctx context.Context,
	actor U,
	session *common_persistence_models.ChatAgentSession,
	run *common_persistence_models.ChatAgentRun,
	policy common_persistence_models.ChatAgentInteractionPolicy,
) error {
	if !policy.AllowUserStop || h.agentRegistry == nil {
		return serviceErrors.ErrChatAgentStopNotAllowed
	}
	if agent, ok := h.agentRegistry.GetAgentType(session.AgentType); !ok || agent.IsWorkflowOwned() {
		return serviceErrors.ErrChatAgentStopNotAllowed
	}
	actorID := uint64(actor.GetID())
	siteApprover := h.hasSiteToolApproval(ctx, actor.GetUserType())
	channel, role, err := h.agentSessionControlParticipant(ctx, actor, session, siteApprover)
	if err != nil {
		return err
	}
	// The captured approval policy is read only for a channel admin who is neither the triggering
	// user nor a site tool-approval admin.
	allowed, err := h.resolvesToolCalls(ctx, actorID, siteApprover, run, role, func() bool {
		return h.loadRunApprovalPolicy(ctx, run).AllowChannelAdminsToResolve
	})
	if err != nil {
		return err
	}
	if !allowed {
		if e := controlEvidence(ctx); e != nil && e.actorID == actorID && e.channelID == session.ChannelID && e.runs != nil {
			allowed = e.runs[run.ID].HasAdmittedInput
		} else {
			if allowed, err = h.persistenceHandler.HasChatAgentRunAdmittedInputFrom(ctx, run.ID, actorID); err != nil {
				return serviceErrors.ErrDatabaseError
			}
		}
	}
	if !allowed {
		return serviceErrors.ErrInsufficientPermissions
	}
	return h.authorizeAgentSessionControl(ctx, ports.ChatPermissionStopAgentRun, actor, session, channel, run.ID)
}

// authorizeAgentSessionResume decides whether actor may resume session, which a Stop suspended.
// nil allows it; otherwise it is the error the Resume fails with. ErrInsufficientPermissions: the
// actor is not a site tool-approval admin, and is neither a participant of the session's channel
// who stopped it nor an admin of that channel. Otherwise the application's Authorize callback
// decides (ChatPermissionResumeAgentSession).
func (h *Handler[U]) authorizeAgentSessionResume(ctx context.Context, actor U, session *common_persistence_models.ChatAgentSession) error {
	siteApprover := h.hasSiteToolApproval(ctx, actor.GetUserType())
	channel, role, err := h.agentSessionControlParticipant(ctx, actor, session, siteApprover)
	if err != nil {
		return err
	}
	if !siteApprover && common_util.Deref(session.SuspendedByUserID) != uint64(actor.GetID()) && *role < proto_models.ChannelRole_CHANNEL_ROLE_ADMIN {
		return serviceErrors.ErrInsufficientPermissions
	}
	return h.authorizeAgentSessionControl(ctx, ports.ChatPermissionResumeAgentSession, actor, session, channel, common_util.Deref(session.SuspendedRunID))
}

// agentSessionControlParticipant loads actor's access to session's channel for a Stop or a Resume.
// A site tool-approval admin needs none, and gets a nil channel and role. Everyone else must
// participate in the channel (ErrInsufficientPermissions): a person removed from it keeps no
// control over its agents.
func (h *Handler[U]) agentSessionControlParticipant(
	ctx context.Context,
	actor U,
	session *common_persistence_models.ChatAgentSession,
	siteApprover bool,
) (*common_persistence_models.ChatChannel, *proto_models.ChannelRole, error) {
	if e := controlEvidence(ctx); e != nil && e.actorID == uint64(actor.GetID()) && e.channelID == session.ChannelID {
		return e.channel, e.role, e.err
	}
	if siteApprover {
		return nil, nil, nil
	}
	channel, role, isMember, err := h.loadChannelAccess(ctx, session.ChannelID, actor.GetID())
	if err != nil {
		return nil, nil, err
	}
	if !isMember {
		return nil, nil, serviceErrors.ErrInsufficientPermissions
	}
	return channel, &role, nil
}

// hasSiteToolApproval reports whether a user of userType holds the site tool-approval permission,
// which lets them resolve any run's tool calls, and stop and resume any agent session without
// participating in its channel.
func (h *Handler[U]) hasSiteToolApproval(ctx context.Context, userType proto_models.UserType) bool {
	if e := controlEvidence(ctx); e != nil && e.actorUserType == userType {
		return e.siteApprover
	}
	return h.permissionHandler != nil && h.permissionHandler.HasSitePermission(ctx, userType, permission.PermissionChatToolApproval)
}

// authorizeAgentSessionControl asks the application's Authorize callback about a Stop or a Resume
// of session; runID is the run the Stop ends, or the run the resumed Stop ended. channel is the
// session's channel when the caller loaded it, nil otherwise.
func (h *Handler[U]) authorizeAgentSessionControl(
	ctx context.Context,
	operation ports.ChatPermissionOperation,
	actor U,
	session *common_persistence_models.ChatAgentSession,
	channel *common_persistence_models.ChatChannel,
	runID uint64,
) error {
	if !h.hasPermissionCallback() {
		return nil
	}
	if channel == nil {
		var err error
		if e := controlEvidence(ctx); e != nil && e.actorID == uint64(actor.GetID()) && e.channelID == session.ChannelID {
			channel, err = e.channel, e.err
		} else {
			channel, _, _, err = h.loadChannelAccess(ctx, session.ChannelID, actor.GetID())
		}
		if err != nil {
			return err
		}
	}
	return h.authorize(ctx, ports.ChatPermissionRequest[U]{
		Operation:       operation,
		Actor:           actor,
		Channel:         h.converter.PersistenceChatChannelToService(channel),
		ParentMessageID: session.ParentMessageID,
		AgentSessionID:  session.ID,
		AgentRunID:      runID,
	})
}

// PublishAgentSessionInteractionAfterCommit tells readers of an agent session's location, after the
// ambient transaction commits (or now, without one), that the session's interaction state changed
// (CHAT_EVENT_TYPE_AGENT_SESSION_INTERACTION), so they refetch it. Every change of what the status
// route's interaction block reports publishes it: input enqueued, admitted or deleted, a run ending
// (a drain may start the next one), Stop, Resume, and policy changes. A run starting is announced by
// its own RUNNING run-state event. The agent runtime calls it after step admission.
//
// TODO(multi-instance-v2): clients do not poll an idle session's interaction; they rely on this
// event, which the in-process realtime hub delivers to this instance's connections only. With
// several instances it must travel through a shared broker.
func (h *Handler[U]) PublishAgentSessionInteractionAfterCommit(ctx context.Context, sessionID, channelID, parentMessageID uint64) {
	h.publishChatEventAfterCommit(ctx, func(context.Context) *ports.ChatEvent {
		return &ports.ChatEvent{
			Type:       proto_models.ChatEventType_CHAT_EVENT_TYPE_AGENT_SESSION_INTERACTION,
			OccurredAt: time.Now(),
			AgentSessionInteraction: &ports.ChatAgentSessionInteractionEvent{
				ChannelID:       channelID,
				ParentMessageID: parentMessageID,
				AgentSessionID:  sessionID,
			},
		}
	})
}

// PublishDeliveredAgentInputAfterCommit announces, after the ambient transaction commits (or now,
// without one), a change to an agent session's queue and the messages, if any, that a delivery of
// its queued input created at the session's location (messageIDs, in delivery order): the delivery
// that a run's step, the end of a run, Resume or a send made, or the posting of the queue when the
// session was archived or its automatic runs were disabled. Each message is published first, as
// MESSAGE_SENT with its attachments, then the session's AGENT_SESSION_INTERACTION, so a client
// that reads the queue again on that cue already has the delivered input in its transcript. The
// channel's cached last message is dropped. Each message's durable notification was recorded in
// the delivering transaction (SetNotificationSink), so a crash between the delivery's commit and
// this announcement loses the realtime events only: the messages and their notifications are
// committed and appear on the next read.
func (h *Handler[U]) PublishDeliveredAgentInputAfterCommit(ctx context.Context, sessionID, channelID, parentMessageID uint64, messageIDs []uint64) {
	if len(messageIDs) == 0 {
		h.PublishAgentSessionInteractionAfterCommit(ctx, sessionID, channelID, parentMessageID)
		return
	}
	h.invalidateChannelAccessCache(ctx, channelID)
	announce := func(announceCtx context.Context) {
		announceCtx = persistence.WithoutRequestTransaction[U](context.WithoutCancel(announceCtx))
		for _, message := range h.loadDeliveredAgentInput(announceCtx, channelID, messageIDs) {
			h.publishMessageSent(announceCtx, message)
		}
		h.PublishAgentSessionInteractionAfterCommit(announceCtx, sessionID, channelID, parentMessageID)
	}
	if persistence.AddPostCommitHookFromContext(ctx, func(hookCtx context.Context) error {
		announce(hookCtx)
		return nil
	}) {
		return
	}
	announce(ctx)
}

// loadDeliveredAgentInput reads the messages a delivery created in channelID, in delivery order and
// with their attachments, for their MESSAGE_SENT; a failed read is logged and publishes none.
func (h *Handler[U]) loadDeliveredAgentInput(ctx context.Context, channelID uint64, messageIDs []uint64) []*common_service_models.ChatMessage {
	if h.eventPublisher == nil {
		return nil
	}
	rows, _, err := h.persistenceHandler.BatchGetChatMessagesWithOptions(ctx, channelID, messageIDs, persistence.ChatMessageReadOptions{})
	if err != nil {
		appLogger.For(ctx, h.logger, chatLoggerName).Warn("failed to load delivered agent input for announcement",
			zap.String(logkeys.Stage, "announce_delivered_agent_input"),
			zap.String(logkeys.Fallback, "skip_realtime_publication"),
			zap.Uint64(appLogger.KeyChannelID, channelID),
			zap.Int(appLogger.KeyMessageCount, len(messageIDs)),
			zap.Error(err))
		return nil
	}
	slices.SortFunc(rows, func(a, b *common_persistence_models.ChatMessage) int { return cmp.Compare(a.ID, b.ID) })
	messages := common_util.ApplyToEach(rows, h.converter.PersistenceChatMessageToService)
	if err := h.hydrateMessagesAttachments(ctx, messages); err != nil {
		appLogger.For(ctx, h.logger, chatLoggerName).Warn("failed to hydrate delivered agent input attachments",
			zap.String(logkeys.Stage, "announce_delivered_agent_input"),
			zap.Uint64(appLogger.KeyChannelID, channelID),
			zap.Error(err))
	}
	return messages
}
