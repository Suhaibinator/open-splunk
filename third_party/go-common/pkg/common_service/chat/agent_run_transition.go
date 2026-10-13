package chat

import (
	"context"
	"time"

	"github.com/Suhaibinator/SRouter/pkg/logkeys"
	"github.com/Suhaibinator/SRouter/pkg/scontext"
	common_persistence "github.com/Suhaibinator/go-common/pkg/common_persistence"
	common_persistence_models "github.com/Suhaibinator/go-common/pkg/common_persistence/common_persistence_models"
	"github.com/Suhaibinator/go-common/pkg/common_service/agentkit"
	"github.com/Suhaibinator/go-common/pkg/common_service/common_service_models"
	"github.com/Suhaibinator/go-common/pkg/common_service/ports"
	appLogger "github.com/Suhaibinator/go-common/pkg/logger"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
	"go.uber.org/zap"
)

// agentRunTransitionReportedToObserver keeps the AgentRunObserver contract: RETRYABLE and terminal
// statuses are reported, RUNNING and WAITING_FOR_APPROVAL are not. The realtime event has no such
// filter, because a status reader needs every transition.
func agentRunTransitionReportedToObserver(status proto_models.ChatAgentRunStatus) bool {
	return status == proto_models.ChatAgentRunStatus_CHAT_AGENT_RUN_STATUS_RETRYABLE ||
		common_persistence.IsTerminalChatAgentRunStatus(status)
}

// agentRunTransition describes a committed status transition of a loaded run.
func agentRunTransition(run *common_persistence_models.ChatAgentRun, status proto_models.ChatAgentRunStatus, reason proto_models.ChatAgentRunStatusReason) ports.AgentRunTransition {
	if run == nil {
		return ports.AgentRunTransition{}
	}
	return ports.AgentRunTransition{
		RunID: run.ID, ChannelID: run.ChannelID, ParentMessageID: run.ParentMessageID,
		Status: status, Reason: reason,
	}
}

// agentRunTransitionForRow describes a transition when only a run-tagged transcript row is loaded.
// A run writes every row at its own thread location, so the row's location is the run's.
func agentRunTransitionForRow(row *common_persistence_models.ChatMessage, runID uint64, status proto_models.ChatAgentRunStatus, reason proto_models.ChatAgentRunStatusReason) ports.AgentRunTransition {
	transition := ports.AgentRunTransition{RunID: runID, Status: status, Reason: reason}
	if row != nil {
		transition.ChannelID = row.ChannelID
		if row.ParentMessageID != nil {
			transition.ParentMessageID = *row.ParentMessageID
		}
	}
	return transition
}

// AgentRunSettleHook finishes a terminal run transition once it is durable:
// Handler.SettleAgentRunAfterCommit publishes a delegated child's notice and drives every run the
// ending minted (a drained follow-up, a parent's wake). It runs after the realtime event and before
// the observer.
type AgentRunSettleHook func(ctx context.Context, runID uint64)

// PublishAgentRunTransitionAfterCommit delivers one CAS-winning run-status transition only after its
// transaction is durable: first the CHAT_EVENT_TYPE_AGENT_RUN_STATE realtime hint, then the
// settle hook for a terminal status, then the application's AgentRunObserver. It is the
// single delivery path for the agent runtime and the chat tool/approval lanes, so every transition
// site reports the same way.
//
// Delivery is best-effort. A transaction without post-commit hooks has no trustworthy commit signal,
// so delivery is suppressed rather than announcing a transition its owner can still roll back.
// Observer and settle defects are isolated from the run state machine: a panic is logged and
// recovered.
//
// Parameters:
//   - ctx: carries the ambient transaction, cancellation, and trace metadata.
//   - logger, loggerName: the calling component's logger.
//   - publisher: realtime publisher; nil skips the event.
//   - observer: application callback; nil skips the callback.
//   - transition: the committed transition. A zero ChannelID skips the event, which needs a location.
//   - settle: the settle hook, invoked for terminal statuses only; nil skips it.
func PublishAgentRunTransitionAfterCommit[U common_service_models.UserLike](
	ctx context.Context,
	logger *zap.Logger,
	loggerName string,
	publisher ports.ChatEventPublisher,
	observer ports.AgentRunObserver,
	transition ports.AgentRunTransition,
	settle AgentRunSettleHook,
) {
	if transition.RunID == 0 {
		return
	}
	if !agentRunTransitionReportedToObserver(transition.Status) {
		observer = nil
	}
	if transition.ChannelID == 0 {
		publisher = nil
	}
	if !common_persistence.IsTerminalChatAgentRunStatus(transition.Status) {
		settle = nil
	}
	if publisher == nil && observer == nil && settle == nil {
		return
	}
	deliver := func(callbackCtx context.Context) {
		if publisher != nil {
			// A run-state event is a system event: whoever approved, denied, or cancelled is not its
			// actor and must not lend their authority to the fan-out.
			err := publisher.PublishChatEvent(agentkit.ContextWithoutActor[U](callbackCtx), &ports.ChatEvent{
				Type:       proto_models.ChatEventType_CHAT_EVENT_TYPE_AGENT_RUN_STATE,
				OccurredAt: time.Now(),
				AgentRunState: &ports.ChatAgentRunStateEvent{
					ChannelID:        transition.ChannelID,
					ParentMessageID:  transition.ParentMessageID,
					AgentRunID:       transition.RunID,
					Status:           transition.Status,
					StatusReason:     transition.Reason,
					StatusReasonCode: transition.ReasonCode,
				},
			})
			if err != nil && logger != nil {
				appLogger.For(callbackCtx, logger, loggerName).Error("failed to publish agent run state event",
					zap.String(logkeys.Stage, "publish_agent_run_transition"),
					zap.Uint64(appLogger.KeyAgentRunID, transition.RunID),
					zap.Stringer(logkeys.Status, transition.Status),
					zap.Error(err))
			}
		}
		if settle != nil {
			// Settlement runs before the observer: the observer is application code that may block,
			// and the follow-up or parent wake it would delay is part of this transition's own
			// lifecycle.
			runIsolated(callbackCtx, logger, loggerName, "agent run settle hook panicked; recovered", transition, func() {
				settle(callbackCtx, transition.RunID)
			})
		}
		if observer == nil {
			return
		}
		runIsolated(callbackCtx, logger, loggerName, "agent run observer panicked; recovered", transition, func() {
			observer(callbackCtx, transition)
		})
	}
	if common_persistence.AddPostCommitHookFromContext(ctx, func(hookCtx context.Context) error {
		deliver(common_persistence.WithoutRequestTransaction[U](context.WithoutCancel(hookCtx)))
		return nil
	}) {
		return
	}
	if tx, hasTransaction := scontext.GetTransaction(ctx); hasTransaction && tx != nil {
		// A plain transaction has no trustworthy commit signal. Delivering here would announce a false
		// lifecycle transition if its owner later rolls back, so fail closed. Callers that require
		// lifecycle delivery inside a transaction must use a PostCommitHookRegistrar.
		appLogger.LogInvariantViolation(appLogger.For(ctx, logger, loggerName), "transactional_agent_run_observer_has_post_commit_registrar", nil,
			zap.String(logkeys.Stage, "notify_agent_run_observer"),
			zap.Uint64(appLogger.KeyAgentRunID, transition.RunID),
			zap.Stringer(logkeys.Status, transition.Status),
			zap.String(logkeys.Expected, "post-commit hook registrar"),
			zap.String(logkeys.Actual, "transaction without post-commit hooks"),
			zap.String(logkeys.Fallback, "suppress_observer_callback"))
		return
	}
	deliver(common_persistence.WithoutRequestTransaction[U](context.WithoutCancel(ctx)))
}

// runIsolated runs one post-commit callback with panic isolation, so a defect in application or
// settlement code cannot take the delivery goroutine down with it.
func runIsolated(ctx context.Context, logger *zap.Logger, loggerName, message string, transition ports.AgentRunTransition, fn func()) {
	defer func() {
		if recovered := recover(); recovered != nil && logger != nil {
			appLogger.For(ctx, logger, loggerName).Error(message,
				zap.Uint64(appLogger.KeyAgentRunID, transition.RunID),
				zap.Stringer(logkeys.Status, transition.Status),
				zap.Any(logkeys.Panic, recovered))
		}
	}()
	fn()
}

// publishAgentRunToolActivity reports that a claimed TOOL_CALL started executing (startedAt set) or
// stopped (zero startedAt). It is an ephemeral hint published directly, never after commit: nothing
// durable backs it beyond the tool row itself, which the status read reports on its own. A hidden
// row's id is withheld, matching that read.
func (h *Handler[U]) publishAgentRunToolActivity(ctx context.Context, row *common_persistence_models.ChatMessage, startedAt time.Time) {
	if h == nil || h.eventPublisher == nil || row == nil || isNilOrZero(row.AgentRunID) || row.ChannelID == 0 {
		return
	}
	state := &ports.ChatAgentRunStateEvent{
		ChannelID:  row.ChannelID,
		AgentRunID: *row.AgentRunID,
		Status:     proto_models.ChatAgentRunStatus_CHAT_AGENT_RUN_STATUS_RUNNING,
	}
	if row.ParentMessageID != nil {
		state.ParentMessageID = *row.ParentMessageID
	}
	if !startedAt.IsZero() {
		state.Activity = proto_models.AgentRunActivity_AGENT_RUN_ACTIVITY_EXECUTING_TOOL
		state.ActivityStartedAt = startedAt
		if row.HiddenFromUsers == nil || !*row.HiddenFromUsers {
			state.ActivityMessageID = row.ID
		}
	}
	_ = h.eventPublisher.PublishChatEvent(agentkit.ContextWithoutActor[U](context.WithoutCancel(ctx)), &ports.ChatEvent{
		Type:          proto_models.ChatEventType_CHAT_EVENT_TYPE_AGENT_RUN_STATE,
		OccurredAt:    time.Now(),
		AgentRunState: state,
	})
}
