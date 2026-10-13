package chat

import (
	"context"
	"errors"
	"math"
	"time"

	"github.com/Suhaibinator/SRouter/pkg/logkeys"
	persistence "github.com/Suhaibinator/go-common/pkg/common_persistence"
	common_persistence_models "github.com/Suhaibinator/go-common/pkg/common_persistence/common_persistence_models"
	"github.com/Suhaibinator/go-common/pkg/common_service/common_service_models"
	serviceErrors "github.com/Suhaibinator/go-common/pkg/common_service/errors"
	"github.com/Suhaibinator/go-common/pkg/common_service/ports"
	"github.com/Suhaibinator/go-common/pkg/common_util"
	appLogger "github.com/Suhaibinator/go-common/pkg/logger"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
	"go.uber.org/zap"
)

// AuthorizeAgentRunStop checks that the context actor may stop runID and returns the actor's user
// id. The run must belong to an agent session whose policy, as the run captured it, allows user
// Stop; see authorizeAgentRunStop for who may stop it. Whether the run is still active is decided
// by the Stop's own transaction. The facade's StopChatAgentRun owns the Stop itself, because it
// also cancels the run's local drive.
func (h *Handler[U]) AuthorizeAgentRunStop(ctx context.Context, runID uint64) (uint64, error) {
	if h.chatDisabled() {
		return 0, serviceErrors.ErrChatDisabled
	}
	actor, actorID, err := h.getActor(ctx)
	if err != nil {
		return 0, err
	}
	run, err := h.persistenceHandler.GetChatAgentRunByID(ctx, runID)
	if err != nil {
		if errors.Is(err, persistence.ErrChatAgentRunNotFound) {
			return 0, serviceErrors.ErrChatAgentRunNotFound
		}
		return 0, serviceErrors.ErrDatabaseError
	}
	if isNilOrZero(run.AgentSessionID) {
		return 0, serviceErrors.ErrChatAgentStopNotAllowed
	}
	session, err := h.persistenceHandler.GetChatAgentSessionByID(ctx, *run.AgentSessionID)
	if err != nil {
		if errors.Is(err, persistence.ErrChatAgentSessionNotFound) {
			return 0, serviceErrors.ErrChatAgentRunNotFound
		}
		return 0, serviceErrors.ErrDatabaseError
	}
	version := session.ActiveConfigVersion
	if run.AgentSessionConfigVersion != nil {
		version = *run.AgentSessionConfigVersion
	}
	policy, err := h.persistenceHandler.GetChatAgentInteractionPolicy(ctx, session.ID, version)
	if err != nil {
		return 0, serviceErrors.ErrDatabaseError
	}
	if err := h.authorizeAgentRunStop(ctx, actor, session, run, policy); err != nil {
		if errors.Is(err, serviceErrors.ErrInsufficientPermissions) {
			appLogger.For(ctx, h.logger, chatLoggerName).Warn("agent run stop denied: unauthorized actor",
				zap.String(logkeys.Stage, "authorize_agent_run_stop"),
				zap.String(appLogger.KeyOutcome, "denied"),
				zap.String(logkeys.Reason, "insufficient_permissions"),
				zap.Uint64(appLogger.KeyAgentRunID, run.ID),
				zap.Uint64(appLogger.KeyAgentSessionID, session.ID),
				zap.Uint64(appLogger.KeyUserID, actorID))
		}
		return 0, err
	}
	return actorID, nil
}

// PublishAgentRunStopped publishes the realtime events a person's Stop owes the stopped session's
// viewers, right after the Stop commits and before the Stop returns:
//   - AGENT_SESSION_INTERACTION for the stopped session and for each child the Stop suspended;
//   - the run's CANCELLED (STOPPED_BY_USER) run state;
//   - PublishAgentRunCancellation's stream and tool-call events.
//
// The application's AgentRunObserver is not called here; the facade calls it in the background,
// and the settlement it runs skips the stopped session's interaction (isPersonStop).
func (h *Handler[U]) PublishAgentRunStopped(ctx context.Context, stop *persistence.ChatAgentRunStop) {
	if h == nil || stop == nil || stop.Run == nil || stop.Session == nil {
		return
	}
	ctx = persistence.WithoutRequestTransaction[U](context.WithoutCancel(ctx))
	h.PublishAgentSessionInteractionAfterCommit(ctx, stop.Session.ID, stop.Session.ChannelID, stop.Session.ParentMessageID)
	for _, child := range stop.Children {
		h.PublishAgentSessionInteractionAfterCommit(ctx, child.SessionID, child.ChannelID, child.ParentMessageID)
	}
	PublishAgentRunTransitionAfterCommit[U](ctx, h.logger, chatLoggerName, h.eventPublisher, nil,
		agentRunTransition(stop.Run, stop.Run.Status, stop.Run.StatusReason), nil)
	h.PublishAgentRunCancellation(ctx, &stop.ChatAgentRunCancellation)
}

// isPersonStop reports whether run ended by a person's direct Stop (STOPPED_BY_USER with no code),
// whose session's interaction PublishAgentRunStopped published as the Stop committed. A child the
// parent's Stop cancelled carries a code, so its settlement still publishes.
func isPersonStop(run *common_persistence_models.ChatAgentRun) bool {
	return run.StatusReason == proto_models.ChatAgentRunStatusReason_CHAT_AGENT_RUN_STATUS_REASON_STOPPED_BY_USER &&
		common_util.Deref(run.StatusReasonCode) == ""
}

// PublishAgentRunCancellation publishes, right after a run's cancellation commits, what it ended
// besides the run: a MESSAGE_STREAM_FAILED for each stream, so no bubble keeps streaming, and a
// TOOL_CALL_RESOLUTION for each tool call it expired or failed, so no approval card stays
// actionable. The cancelled run's writers cannot publish these themselves: the cancellation fenced
// them out.
func (h *Handler[U]) PublishAgentRunCancellation(ctx context.Context, cancellation *persistence.ChatAgentRunCancellation) {
	if h == nil || cancellation == nil || cancellation.Run == nil {
		return
	}
	ctx = persistence.WithoutRequestTransaction[U](context.WithoutCancel(ctx))
	run := cancellation.Run
	for _, stream := range cancellation.FailedStreams {
		parentMessageID := common_util.Deref(stream.ParentMessageID)
		h.publishChatEventAfterCommit(ctx, func(context.Context) *ports.ChatEvent {
			return &ports.ChatEvent{
				Type:        proto_models.ChatEventType_CHAT_EVENT_TYPE_MESSAGE_STREAM_FAILED,
				OccurredAt:  time.Now(),
				ActorUserID: common_service_models.UserIdType(run.BotUserID),
				MessageStream: &ports.ChatMessageStreamEvent{
					HiddenFromUsers: common_util.Deref(stream.HiddenFromUsers),
					MessageID:       common_service_models.ChatMessageIdType(stream.ID),
					ChannelID:       common_service_models.ChatChannelIdType(stream.ChannelID),
					// The cancellation, not the stream's writer, ended the stream, so the writer's
					// sequence is unknown here. The largest sequence orders this frame after every
					// delta the writer published; the cancelled run fences any later one.
					Sequence:        math.MaxUint64,
					ParentMessageID: &parentMessageID,
				},
			}
		})
	}
	resolvedAt := common_util.Deref(run.CompletedAt)
	for _, toolCall := range cancellation.SettledToolCalls {
		h.publishToolCallResolution(ctx, toolCall.ID, toolCall.ChannelID, h.converter.PersistenceChatMessageToService(toolCall), nil, nil, resolvedAt)
	}
}

// ResumeAgentSession lifts the suspension a person's Stop left on an agent session and returns the
// session's interaction for the context actor (absent when reading it fails after the Resume
// committed), with the run Resume started. In one transaction the suspension is cleared, the input
// held while suspended is admitted, in order, into one new run, and, when none was held, the
// session's deferred delegation wake starts a run instead; that run is driven after commit. The
// stopped run and the children its Stop cancelled are not restarted.
//
// The person who stopped the session while they still participate in its channel, site
// tool-approval admins, and channel admins may resume it, subject to the application's Authorize
// callback (ChatPermissionResumeAgentSession). A session that is not suspended has nothing to
// resume: any reader of its channel the application admits (ChatPermissionGetChannel) gets its
// current state, so a repeated Resume succeeds. A delegated child stopped with its parent cannot
// resume until its run's cancellation has committed (ErrChatAgentSessionBusy). Resume never joins
// the caller's transaction.
func (h *Handler[U]) ResumeAgentSession(ctx context.Context, sessionID uint64) (*proto_models.ResumeAgentSessionResponse, error) {
	if h.chatDisabled() {
		return nil, serviceErrors.ErrChatDisabled
	}
	actor, actorID, err := h.getActor(ctx)
	if err != nil {
		return nil, err
	}
	ctx = persistence.WithoutRequestTransaction[U](ctx)
	session, err := h.persistenceHandler.GetChatAgentSessionByID(ctx, sessionID)
	if err != nil {
		if errors.Is(err, persistence.ErrChatAgentSessionNotFound) {
			return nil, serviceErrors.ErrChatAgentSessionNotFound
		}
		return nil, serviceErrors.ErrDatabaseError
	}
	response := &proto_models.ResumeAgentSessionResponse{}
	var resumed bool
	if !session.IsSuspended() {
		if _, err := h.readChannelAuthorized(ctx, actor, session.ChannelID); err != nil {
			return nil, err
		}
	} else {
		if err := h.authorizeAgentSessionResume(ctx, actor, session); err != nil {
			if errors.Is(err, serviceErrors.ErrInsufficientPermissions) {
				appLogger.For(ctx, h.logger, chatLoggerName).Warn("agent session resume denied: unauthorized actor",
					zap.String(logkeys.Stage, "authorize_agent_session_resume"),
					zap.String(appLogger.KeyOutcome, "denied"),
					zap.String(logkeys.Reason, "insufficient_permissions"),
					zap.Uint64(appLogger.KeyAgentSessionID, session.ID),
					zap.Uint64(appLogger.KeyUserID, actorID))
			}
			return nil, err
		}
		// The suspended run pins the authorization above to this suspension, which no Stop
		// replaces: a session resumed and stopped again in between is left alone, and the response
		// shows the new suspension.
		resume, err := h.persistenceHandler.ResumeChatAgentSession(ctx, session.ID, common_util.Deref(session.SuspendedRunID))
		if err != nil {
			switch {
			case errors.Is(err, persistence.ErrChatAgentSessionNotFound):
				return nil, serviceErrors.ErrChatAgentSessionNotFound
			case errors.Is(err, persistence.ErrChatAgentSessionRunActive):
				return nil, serviceErrors.ErrChatAgentSessionBusy
			}
			return nil, serviceErrors.ErrDatabaseError
		}
		if resumed = resume.Resumed; resumed {
			appLogger.For(ctx, h.logger, chatLoggerName).Info("agent session resumed",
				zap.String(logkeys.Operation, "resume_agent_session"),
				zap.Uint64(appLogger.KeyAgentSessionID, session.ID),
				zap.Uint64(appLogger.KeyUserID, actorID))
			h.PublishDeliveredAgentInputAfterCommit(ctx, resume.Session.ID, resume.Session.ChannelID, resume.Session.ParentMessageID, resume.Delivery.MessageIDs())
			if resume.Run != nil {
				h.startAgentRunAfterCommit(ctx, session.AgentType, resume.Run.ID, resume.Run.ChannelID, resume.Run.ParentMessageID)
				response.AgentRunId = &resume.Run.ID
			}
		}
	}
	if response.Interaction, err = h.agentSessionInteraction(ctx, actor, session.ID); err != nil {
		if !resumed {
			return nil, err
		}
		// The Resume has committed: a failed read of the session's new state leaves the interaction
		// out rather than reporting the Resume as failed, and the interaction event already sent
		// makes the client read it again.
		appLogger.For(ctx, h.logger, chatLoggerName).Warn("agent session resume: failed to read the resumed session's interaction",
			zap.String(logkeys.Stage, "read_resumed_session_interaction"),
			zap.String(logkeys.Fallback, "omit_interaction"),
			zap.Uint64(appLogger.KeyAgentSessionID, session.ID),
			zap.Error(err))
	}
	return response, nil
}
