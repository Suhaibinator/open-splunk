package common_service

import (
	"context"
	"errors"
	"strconv"

	"github.com/Suhaibinator/SRouter/pkg/logkeys"
	"github.com/Suhaibinator/SRouter/pkg/scontext"
	"github.com/Suhaibinator/go-common/pkg/common_context"
	common_persistence "github.com/Suhaibinator/go-common/pkg/common_persistence"
	"github.com/Suhaibinator/go-common/pkg/common_persistence/common_persistence_models"
	chatpkg "github.com/Suhaibinator/go-common/pkg/common_service/chat"
	"github.com/Suhaibinator/go-common/pkg/common_service/common_service_models"
	serviceErrors "github.com/Suhaibinator/go-common/pkg/common_service/errors"
	"github.com/Suhaibinator/go-common/pkg/common_service/ports"
	"github.com/Suhaibinator/go-common/pkg/common_service/rate_limiter"
	"github.com/Suhaibinator/go-common/pkg/common_util"
	appLogger "github.com/Suhaibinator/go-common/pkg/logger"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
	"go.uber.org/zap"
)

// CancelChatAgentRun atomically moves an active run to CANCELLED. It is idempotent: false with a
// nil error means the run exists but was not in a cancellable state. The replica that wins the
// transition delivers the run-state event after commit, before returning; the registered
// AgentRunObserver hears the transition in supervised background work, so it may run after this
// returns and a slow observer never delays the caller.
//
// cancellation is recorded on the run as its status reason and reaches status readers and the
// observer unchanged, so a cancelled run never has to be presented as a user-requested cancel. An
// unknown reason or a malformed code fails with ErrInvalidRequest before anything is written.
func (h *CommonServiceHandler[U]) CancelChatAgentRun(ctx context.Context, runID uint64, cancellation ports.AgentRunCancellation) (bool, error) {
	if h == nil || h.commonPersistenceHandler == nil || runID == 0 {
		return false, serviceErrors.ErrChatAgentRunNotFound
	}
	if !cancellation.Validate() {
		return false, serviceErrors.ErrInvalidRequest
	}
	return h.cancelChatAgentRun(ctx, runID, cancellation)
}

// cancelChatAgentRun is CancelChatAgentRun for a cancellation go-common records itself, whose
// reason an application may not pass (a parent's Stop cancelling its children).
func (h *CommonServiceHandler[U]) cancelChatAgentRun(ctx context.Context, runID uint64, cancellation ports.AgentRunCancellation) (bool, error) {
	// Cancellation is an independent control-plane action. Never inherit a caller's ambient write
	// transaction or another run drive's fencing token: either would delay/rollback cancellation or
	// incorrectly reject it as stale drive work.
	controlCtx := common_persistence.WithoutRequestTransaction[U](ctx)
	cancelled, hooks, err := h.cancelAgentRunWithNotifications(controlCtx, runID, cancellation)
	if err != nil {
		if errors.Is(err, common_persistence.ErrChatAgentRunDriveLeaseLost) {
			return false, err
		}
		return false, serviceErrors.ErrDatabaseError
	}
	if cancelled != nil {
		// Source authority only gates the committed mutation. Detached settlement
		// and application observers must not borrow the originating run's fence.
		postCtx := common_context.ClearChatAgentDelegationOrigin(controlCtx)
		if h.agentRuntime != nil {
			h.agentRuntime.cancelActiveRunAfterCommit(postCtx, runID)
		}
		if h.chatHandler != nil {
			h.chatHandler.CancelAgentRunWork(runID)
			h.chatHandler.PublishAgentRunCancellation(postCtx, cancelled)
		}
		h.runOwnedPostCommitHooks(postCtx, hooks, "cancel_agent_run_post_commit")
		// Notify only after every local execution lane has received cancellation. The event and
		// settlement follow inline; the observer is application code that may block, so it runs in
		// supervised background work and never holds up this caller (the recovery sweep's
		// retirement pass, a parent cancelling several children) or provider/tool shutdown.
		transition := cancelledRunTransition(cancelled.Run)
		observer, settle := h.runTransitionHooks()
		chatpkg.PublishAgentRunTransitionAfterCommit[U](postCtx, h.logger, agentRuntimeLoggerName, h.chatEventPublisher, nil, transition, settle)
		if observer != nil {
			h.runOwnedPostCommitHooks(postCtx, []common_persistence.PostCommitHook{func(hookCtx context.Context) error {
				chatpkg.PublishAgentRunTransitionAfterCommit[U](hookCtx, h.logger, agentRuntimeLoggerName, nil, observer, transition, nil)
				return nil
			}}, "cancel_agent_run_observer")
		}
		return true, nil
	}

	// A zero-row CAS can also mean the id does not exist. Distinguish that from the intentional,
	// idempotent already-terminal case so callers do not need a raw persistence lookup.
	if _, err := h.commonPersistenceHandler.GetChatAgentRunByID(controlCtx, runID); err != nil {
		if errors.Is(err, common_persistence.ErrChatAgentRunNotFound) {
			return false, serviceErrors.ErrChatAgentRunNotFound
		}
		return false, serviceErrors.ErrDatabaseError
	}
	return false, nil
}

// publishCancelledRunTransition delivers a cancelled run's transition: its realtime event through
// publisher (nil when the caller has already published it), then the settle hook and the
// application's AgentRunObserver.
func (h *CommonServiceHandler[U]) publishCancelledRunTransition(ctx context.Context, publisher ports.ChatEventPublisher, run *common_persistence_models.ChatAgentRun) {
	observer, settle := h.runTransitionHooks()
	chatpkg.PublishAgentRunTransitionAfterCommit[U](ctx, h.logger, agentRuntimeLoggerName, publisher, observer, cancelledRunTransition(run), settle)
}

// cancelledRunTransition is the transition a cancelled run reports. Its settlement matters: a
// cancelled run may have drained its session's waiting input into a follow-up, a cancelled
// delegated child still owes its parent a notice publish and, unless the parent asked for the
// cancel, a wake, and a cancelled parent run may have drained a pending wake.
func cancelledRunTransition(run *common_persistence_models.ChatAgentRun) ports.AgentRunTransition {
	return ports.AgentRunTransition{
		RunID:           run.ID,
		ChannelID:       run.ChannelID,
		ParentMessageID: run.ParentMessageID,
		Status:          run.Status,
		Reason:          run.StatusReason,
		ReasonCode:      common_util.Deref(run.StatusReasonCode),
	}
}

// runTransitionHooks returns what follows a durable run transition's realtime event: the
// application's AgentRunObserver and the runtime's settle hook, which publishes a delegated child's
// notice and drives every run an ending minted.
func (h *CommonServiceHandler[U]) runTransitionHooks() (ports.AgentRunObserver, chatpkg.AgentRunSettleHook) {
	if h.agentRuntime == nil {
		return nil, nil
	}
	return h.agentRuntime.runObserver, h.agentRuntime.settleRun
}

// StopChatAgentRun stops an agent session's active run for the context actor and suspends the
// session until ResumeChatAgentSession, returning the session's interaction for the actor (absent
// when reading it fails after the Stop committed). runID is the run the person saw: one that
// already ended is not stopped, and neither is a newer run in its place (ErrChatAgentRunNotActive).
// The run's captured policy must allow user Stop (ErrChatAgentStopNotAllowed), and the actor must
// be able to resolve the run's tool calls or have sent input the run admitted, and, unless a site
// tool-approval admin, still participate in the session's channel (ErrInsufficientPermissions); the
// application's Authorize callback may veto (ChatPermissionStopAgentRun). Stop and Resume share a
// per-person rate limit (ErrRateLimitExceeded).
//
// One transaction cancels the run (CANCELLED, STOPPED_BY_USER), expires its pending approvals,
// fails its outstanding tool work and dangling streams, and records the suspension. After commit,
// in order: the run's local drive and tool work are cancelled, so a provider stream ends at once;
// the session's viewers get the Stop's realtime events; and the delegated children that were
// working when the transaction read them are cancelled, each in its own supervised background
// work. The application's AgentRunObserver hears the transition in the background as well, so
// neither an observer nor a child delays the response. Tokens a provider already processed are
// billed, and a tool that does not honor cancellation finishes, its result fenced out of the
// transcript.
//
// Stop never joins the caller's transaction.
func (h *CommonServiceHandler[U]) StopChatAgentRun(ctx context.Context, runID uint64) (*proto_models.StopAgentRunResponse, error) {
	if h == nil || h.chatHandler == nil || h.commonPersistenceHandler == nil {
		return nil, serviceErrors.ErrChatDisabled
	}
	if err := h.checkAgentSessionControlRateLimit(ctx); err != nil {
		return nil, err
	}
	ctx = common_persistence.WithoutRequestTransaction[U](ctx)
	stoppedBy, err := h.chatHandler.AuthorizeAgentRunStop(ctx, runID)
	if err != nil {
		return nil, err
	}
	var stop *common_persistence.ChatAgentRunStop
	// The pending approvals the Stop expires leave the approvers' notification inboxes in the same
	// transaction.
	hooks, err := h.ownedTransaction(ctx, func(txCtx context.Context) error {
		var err error
		if stop, err = h.commonPersistenceHandler.StopChatAgentRun(txCtx, runID, stoppedBy); err != nil || stop == nil {
			return err
		}
		return h.resolveCancelledRunNotifications(txCtx, &stop.ChatAgentRunCancellation)
	})
	if err != nil {
		return nil, serviceErrors.ErrDatabaseError
	}
	if stop == nil {
		return nil, serviceErrors.ErrChatAgentRunNotActive
	}
	logger := appLogger.For(ctx, h.logger, agentRuntimeLoggerName)
	logger.Info("agent run stopped by user",
		zap.String(logkeys.Operation, "stop_agent_run"),
		zap.Uint64(appLogger.KeyAgentRunID, runID),
		zap.Uint64(appLogger.KeyAgentSessionID, stop.Session.ID),
		zap.Uint64(appLogger.KeyUserID, stoppedBy))

	// TODO(multi-instance-v2): This process owns every drive, so cancelling the run's local drive
	// ends its provider stream now. With several instances, a Stop received by an instance that
	// does not drive the run reaches the drive only at its next heartbeat; that needs a durable
	// cancel check or a broker-backed cancel signal.
	if h.agentRuntime != nil {
		h.agentRuntime.cancelActiveRun(runID)
	}
	h.chatHandler.CancelAgentRunWork(runID)
	h.chatHandler.PublishAgentRunStopped(ctx, stop)

	h.runOwnedPostCommitHooks(ctx, hooks, "stop_agent_run_post_commit")
	for _, child := range stop.Children {
		h.cancelStoppedParentChild(ctx, child.RunID)
	}
	h.runOwnedPostCommitHooks(ctx, []common_persistence.PostCommitHook{func(hookCtx context.Context) error {
		// The run-state event is already out; this delivers the rest of the transition.
		h.publishCancelledRunTransition(hookCtx, nil, stop.Run)
		return nil
	}}, "stop_agent_run_observer")

	// The Stop has committed: a failed read of the session's new state leaves the interaction out
	// rather than reporting the Stop as failed, and the interaction event already sent makes the
	// client read it again.
	interaction, err := h.chatHandler.AgentSessionInteraction(ctx, stop.Session.ID)
	if err != nil {
		logger.Warn("agent run stop: failed to read the stopped session's interaction",
			zap.String(logkeys.Stage, "read_stopped_session_interaction"),
			zap.String(logkeys.Fallback, "omit_interaction"),
			zap.Uint64(appLogger.KeyAgentRunID, runID),
			zap.Uint64(appLogger.KeyAgentSessionID, stop.Session.ID),
			zap.Error(err))
	}
	return &proto_models.StopAgentRunResponse{Interaction: interaction}, nil
}

// ResumeChatAgentSession lifts the suspension a Stop left on an agent session for the context
// actor: the input held while suspended starts one new run, or, when none was held, the session's
// deferred delegation wake does. It shares Stop's rate limit. See chat.Handler.ResumeAgentSession.
func (h *CommonServiceHandler[U]) ResumeChatAgentSession(ctx context.Context, sessionID uint64) (*proto_models.ResumeAgentSessionResponse, error) {
	if h == nil || h.chatHandler == nil {
		return nil, serviceErrors.ErrChatDisabled
	}
	if err := h.checkAgentSessionControlRateLimit(ctx); err != nil {
		return nil, err
	}
	return h.chatHandler.ResumeAgentSession(ctx, sessionID)
}

// checkAgentSessionControlRateLimit applies the per-person limit Stop and Resume share
// (rate_limits.agent_session_control_*, which the reloading limiter samples once per check), so a
// burst of approvals, which have their own limit, never costs a person their Stop. Exceeding it
// returns ErrRateLimitExceeded; a context without a user returns ErrUserNotFound.
func (h *CommonServiceHandler[U]) checkAgentSessionControlRateLimit(ctx context.Context) error {
	actor, found := scontext.GetUser[common_service_models.UserIdType, U](ctx)
	if !found || actor == nil {
		return serviceErrors.ErrUserNotFound
	}
	return h.checkRateLimit(ctx, rate_limiter.ActionAgentSessionControl, strconv.FormatUint(uint64((*actor).GetID()), 10))
}

// cancelStoppedParentChild cancels a delegated child run that was working when its parent was
// stopped, in its own supervised background work, so a slow child observer delays only its own
// child. The cancellation (STOPPED_BY_USER, code delegation_parent_stopped) posts the child's notice
// and never wakes the parent; a child that ended on its own first keeps that ending.
func (h *CommonServiceHandler[U]) cancelStoppedParentChild(ctx context.Context, runID uint64) {
	h.runOwnedPostCommitHooks(ctx, []common_persistence.PostCommitHook{func(hookCtx context.Context) error {
		_, err := h.cancelChatAgentRun(hookCtx, runID, delegationParentStopped)
		return err
	}}, "cancel_stopped_parent_child")
}

// reconcileSuspendedRuns cancels the delegated child runs a parent's Stop left running because a
// crash or a stalled worker cut its after-commit cancellations short. The recovery sweep starts it
// in the background each tick, one pass at a time, with its policy sample, reading one scan page at
// a time, each read bounded like the sweep's own scan.
func (h *CommonServiceHandler[U]) reconcileSuspendedRuns(ctx context.Context, policy agentRecoveryPolicy) {
	h.reconcileRetiringDelegatedChildren(ctx, policy)
	h.sampleActiveDelegatedChildren(ctx, policy)
	pageSize := policy.scanPageSize
	var afterRunID uint64
	for {
		readCtx, cancel := context.WithTimeout(ctx, policy.dbTimeout)
		runIDs, err := h.commonPersistenceHandler.ListSuspendedChatAgentRuns(readCtx, afterRunID, pageSize)
		cancel()
		if err != nil {
			appLogger.For(ctx, h.logger, agentRuntimeLoggerName).Warn("agent runtime: failed to list suspended agent runs",
				zap.String(logkeys.Stage, "list_suspended_chat_agent_runs"),
				zap.String(logkeys.Fallback, "retry_on_next_scan"),
				zap.Error(err))
			return
		}
		for _, runID := range runIDs {
			afterRunID = runID
			h.cancelStoppedParentChild(ctx, runID)
		}
		if len(runIDs) < pageSize {
			return
		}
	}
}

// sampleActiveDelegatedChildren sets the chat_agent_children_active gauge from one count, bounded
// like the scan's reads. Nothing is read while metrics are disabled.
func (h *CommonServiceHandler[U]) sampleActiveDelegatedChildren(ctx context.Context, policy agentRecoveryPolicy) {
	metrics := h.commonPersistenceHandler.GetMetricsHandler()
	if metrics == nil || !metrics.IsEnabled() {
		return
	}
	readCtx, cancel := context.WithTimeout(ctx, policy.dbTimeout)
	interactive, ownerDriven, err := h.commonPersistenceHandler.CountActiveDelegatedChildrenByDriver(readCtx)
	cancel()
	if err != nil {
		appLogger.For(ctx, h.logger, agentRuntimeLoggerName).Warn("agent runtime: failed to count active delegated children",
			zap.String(logkeys.Stage, "count_active_delegated_children_by_driver"),
			zap.String(logkeys.Fallback, "retry_on_next_scan"),
			zap.Error(err))
		return
	}
	metrics.SetChatAgentChildrenActive(interactive, ownerDriven)
}

// reconcileRetiringDelegatedChildren completes the archive/cancellation intent committed by a
// child's archive, its parent's archive, or RetireGraph's archive of an owner-driven root, whose own
// active runs it cancels. No run eligible for this scan can be drive-claimed.
func (h *CommonServiceHandler[U]) reconcileRetiringDelegatedChildren(ctx context.Context, policy agentRecoveryPolicy) {
	pageSize := policy.scanPageSize
	var afterSessionID uint64
	for ctx.Err() == nil {
		readCtx, cancel := context.WithTimeout(ctx, policy.dbTimeout)
		sessions, err := h.commonPersistenceHandler.ListRetiringDelegatedChatAgentSessions(readCtx, afterSessionID, pageSize)
		cancel()
		if err != nil {
			appLogger.For(ctx, h.logger, agentRuntimeLoggerName).Warn("agent runtime: failed to list retiring delegated children", zap.String(logkeys.Fallback, "retry_on_next_scan"), zap.Error(err))
			return
		}
		for _, session := range sessions {
			afterSessionID = session.ID
			if !session.IsDelegatedChild() {
				if err := h.cancelGraphRootRuns(ctx, session.ID); err != nil {
					appLogger.For(ctx, h.logger, agentRuntimeLoggerName).Warn("agent runtime: failed to cancel retired graph root runs", zap.Uint64(appLogger.KeyAgentSessionID, session.ID), zap.String(logkeys.Fallback, "retry_on_next_scan"), zap.Error(err))
				}
				continue
			}
			cancellation := delegationChildArchived
			if session.ParentArchived {
				cancellation = delegationParentArchived
			}
			if _, err := h.retireDelegatedChild(ctx, &session.ChatAgentSession, cancellation); err != nil {
				appLogger.For(ctx, h.logger, agentRuntimeLoggerName).Warn("agent runtime: failed to retire delegated child", zap.Uint64(appLogger.KeyAgentSessionID, session.ID), zap.String(logkeys.Fallback, "retry_on_next_scan"), zap.Error(err))
			}
		}
		if len(sessions) < pageSize {
			return
		}
	}
}
