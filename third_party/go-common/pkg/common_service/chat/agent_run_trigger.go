package chat

import (
	"context"
	"errors"

	"github.com/Suhaibinator/SRouter/pkg/logkeys"
	"github.com/Suhaibinator/SRouter/pkg/scontext"
	"github.com/Suhaibinator/go-common/pkg/common_context"
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

// driveAgentRun invokes the wired run driver in the background. Request cancellation is detached,
// but service-shutdown cancellation remains attached.
func (h *Handler[U]) driveAgentRun(ctx context.Context, runID uint64) {
	if h.agentRunDriver == nil || runID == 0 {
		return
	}
	runCtx, done, ok := h.beginAgentWork(ctx)
	if !ok {
		return
	}
	driver := h.agentRunDriver
	go func() {
		defer done()
		h.runWithPanicRecovery(runCtx, "agent_run_driver", func() {
			driver(runCtx, runID)
		}, zap.Uint64(appLogger.KeyAgentRunID, runID))
	}()
}

func (h *Handler[U]) beginAgentWork(ctx context.Context) (context.Context, func(), bool) {
	h.agentRunsMu.Lock()
	defer h.agentRunsMu.Unlock()
	if h.agentRunsStopping {
		return nil, nil, false
	}
	if h.agentRunsCtx == nil {
		h.agentRunsCtx, h.agentRunsCancel = context.WithCancel(context.Background())
	}
	if ctx == nil {
		ctx = context.Background()
	}
	// A committed child or wake drive is independent work. Its origin's authority
	// fenced admission, not the destination drive's subsequent control operations.
	ctx = common_context.ClearChatAgentDelegationOrigin(ctx)
	runCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	stopLink := context.AfterFunc(h.agentRunsCtx, cancel)
	h.agentRunsWg.Add(1)
	return runCtx, func() {
		stopLink()
		cancel()
		h.agentRunsWg.Done()
	}, true
}

// StopAgentRuns prevents new detached drives, cancels active drives, and waits for them to return.
// Approved tool executions receive the scheduler's bounded shutdown grace; work that violates
// cancellation remains lease-fenced and may outlive the stopped handler.
func (h *Handler[U]) StopAgentRuns() {
	h.agentRunsMu.Lock()
	h.agentRunsStopping = true
	cancel := h.agentRunsCancel
	h.agentRunsMu.Unlock()
	if cancel != nil {
		cancel()
	}
	h.agentRunsWg.Wait()
	// Approved tool work is part of the same agent-run lifecycle and may itself nudge a resumed
	// drive, so shutdown must cancel and join it before returning.
	h.stopToolWorkScheduler()
}

// startAgentRunAfterCommit announces a run the ambient transaction created RUNNING at its location
// (channelID, parentMessageID) and drives it, both after commit: status readers learn of the run
// (CHAT_EVENT_TYPE_AGENT_RUN_STATE) before its drive reports any activity. A run of an owner-driven
// agentType is announced but left undriven for its owner.
func (h *Handler[U]) startAgentRunAfterCommit(ctx context.Context, agentType string, runID, channelID, parentMessageID uint64) {
	h.publishAgentRunStartedAfterCommit(ctx, runID, channelID, parentMessageID)
	if !h.ownerDriven(agentType) {
		h.scheduleAgentRunDrive(ctx, runID)
	}
}

// ownerDriven reports whether agentType is workflow-owned: only its application's owner drive may
// drive its runs, so nothing here drives them after commit, and the runs it mints are left for the
// owner.
func (h *Handler[U]) ownerDriven(agentType string) bool {
	if h.agentRegistry == nil {
		return false
	}
	def, ok := h.agentRegistry.GetAgentType(agentType)
	return ok && def.IsWorkflowOwned()
}

// leaveRunForOwner reports whether nothing here may drive run: its session is owner-driven, or its
// ownership cannot be read. A failed lookup is logged and leaves the run undriven, since only the
// owner may drive a workflow run and durable recovery claims an un-driven interactive one.
func (h *Handler[U]) leaveRunForOwner(ctx context.Context, stage string, run *common_persistence_models.ChatAgentRun) bool {
	if h.agentRegistry == nil || run == nil || isNilOrZero(run.AgentSessionID) {
		return false
	}
	session, err := h.persistenceHandler.GetChatAgentSessionByID(ctx, *run.AgentSessionID)
	if err != nil {
		appLogger.For(ctx, h.logger, chatLoggerName).Warn("agent run drive skipped: ownership lookup failed",
			zap.String(logkeys.Stage, stage),
			zap.String(logkeys.Fallback, "defer_to_durable_recovery"),
			zap.Uint64(appLogger.KeyAgentRunID, run.ID),
			zap.Error(err))
		return true
	}
	return session != nil && h.ownerDriven(session.AgentType)
}

// publishAgentRunStartedAfterCommit is the run-state half of startAgentRunAfterCommit, for a run
// the caller may not drive itself (a workflow-owned session's initial run).
func (h *Handler[U]) publishAgentRunStartedAfterCommit(ctx context.Context, runID, channelID, parentMessageID uint64) {
	PublishAgentRunTransitionAfterCommit[U](ctx, h.logger, chatLoggerName, h.eventPublisher, nil, ports.AgentRunTransition{
		RunID: runID, ChannelID: channelID, ParentMessageID: parentMessageID,
		Status: proto_models.ChatAgentRunStatus_CHAT_AGENT_RUN_STATUS_RUNNING,
	}, nil)
}

// scheduleAgentRunDrive schedules driveAgentRun to fire AFTER the surrounding transaction commits, so
// the run row (and any transcript rows written in the same tx) are durable before the loop reads
// them. It drives immediately only when there is no ambient transaction.
func (h *Handler[U]) scheduleAgentRunDrive(ctx context.Context, runID uint64) {
	if h.agentRunDriver == nil || runID == 0 {
		return
	}
	if persistence.AddPostCommitHookFromContext(ctx, func(hookCtx context.Context) error {
		h.driveAgentRun(hookCtx, runID)
		return nil
	}) {
		return
	}
	if tx, ok := scontext.GetTransaction(ctx); ok && tx != nil {
		appLogger.LogInvariantViolation(appLogger.For(ctx, h.logger, chatLoggerName), "transactional_agent_run_drive_requires_post_commit_registrar", nil,
			zap.String(logkeys.Stage, "schedule_agent_run_drive"),
			zap.Uint64(appLogger.KeyAgentRunID, runID),
			zap.String(logkeys.Fallback, "defer_to_durable_recovery"))
		return
	}
	h.driveAgentRun(ctx, runID)
}

// agentInputTarget is one agent session a message reaches: as the session's human input, or, for a
// main-timeline session, as the broadcast report of one of its delegated children (reportingChild),
// which wakes the session instead.
type agentInputTarget struct {
	session        *common_persistence_models.ChatAgentSession
	reportingChild *common_persistence_models.ChatAgentSession
	// state is an input target's input state, read under its session lock, and waits is its route:
	// false starts a run with the message, true queues the input for the session instead of
	// writing the message.
	state persistence.ChatAgentSessionInputState
	waits bool
}

// agentInputTargets resolves, before the message is written, the agent sessions a message
// reaches. The message's own location is always considered; a thread reply broadcast to the
// channel also reaches the main-timeline session, since the broadcast is the sender's explicit
// request to surface it there (a main-session run writes at parent 0 and cannot broadcast, so this
// cannot ping-pong between the two sessions). A location is skipped when:
//   - the location has no session, or its agent is workflow-owned or unknown,
//   - the sender is that session's bot (the agent's own rows never reach it; this also suppresses
//     the main-timeline session when both share one bot user),
//   - the session is not live, or its auto-run policy is disabled (the application owns run
//     creation).
//
// The caller holds the send's channel lock (lockAgentInputTargets), so a session created in the
// channel concurrently is either visible here or waits for the send.
func (h *Handler[U]) agentInputTargets(ctx context.Context, channelID, parentMessageID uint64, broadcast bool, senderID uint64) ([]*agentInputTarget, error) {
	local, err := h.agentSessionAt(ctx, channelID, parentMessageID)
	if err != nil {
		return nil, err
	}
	var targets []*agentInputTarget
	if h.receivesAgentInput(ctx, local, senderID) {
		targets = append(targets, &agentInputTarget{session: local})
	}
	if parentMessageID == 0 || !broadcast {
		return targets, nil
	}
	main, err := h.agentSessionAt(ctx, channelID, 0)
	if err != nil || !h.receivesAgentInput(ctx, main, senderID) {
		return targets, err
	}
	target := &agentInputTarget{session: main}
	// A broadcast from the bot of one of this session's delegated children is that child's report.
	// It wakes the parent through the persistence coalescer, so a parent that is already running
	// gets one follow-up run for every report that arrived meanwhile instead of concurrent runs.
	if local != nil && local.BotUserID == senderID && common_util.Deref(local.ParentAgentSessionID) == main.ID {
		target.reportingChild = local
	}
	return append(targets, target), nil
}

// agentSessionAt returns the session at one location, or nil when the location has none.
func (h *Handler[U]) agentSessionAt(ctx context.Context, channelID, parentMessageID uint64) (*common_persistence_models.ChatAgentSession, error) {
	session, err := h.persistenceHandler.GetChatAgentSessionInLocation(ctx, channelID, parentMessageID)
	if errors.Is(err, persistence.ErrChatAgentSessionNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, mapChatMessagePersistenceError(err,
			zap.String(logkeys.Stage, "load_agent_session_for_message"),
			zap.Uint64(appLogger.KeyChannelID, channelID),
			zap.Uint64(appLogger.KeyParentMessageID, parentMessageID))
	}
	return session, nil
}

// receivesAgentInput applies the per-session guards of agentInputTargets: the session takes
// input, and the sender is not its own bot.
func (h *Handler[U]) receivesAgentInput(ctx context.Context, session *common_persistence_models.ChatAgentSession, senderID uint64) bool {
	return session != nil && senderID != session.BotUserID && h.takesAgentInput(ctx, session)
}

// takesAgentInput reports whether people's messages reach session as input at all: its agent is
// registered and not workflow-owned, and the session is live with automatic runs. Elsewhere a
// message is ordinary transcript, whatever the session's input policy says.
func (h *Handler[U]) takesAgentInput(ctx context.Context, session *common_persistence_models.ChatAgentSession) bool {
	if h.agentRegistry != nil {
		if def, ok := h.agentRegistry.GetAgentType(session.AgentType); !ok || def.IsWorkflowOwned() {
			return false
		}
	}
	if !session.IsLive() {
		return false
	}
	// Automatic is deliberately zero so sessions persisted before AutoRunPolicy existed retain the
	// historical behavior. Unknown future/corrupt values fail closed instead of creating paid work.
	switch session.AutoRunPolicy {
	case common_persistence_models.ChatAgentSessionAutoRunPolicyAutomatic:
		return true
	case common_persistence_models.ChatAgentSessionAutoRunPolicyDisabled:
		return false
	}
	appLogger.LogInvariantViolation(appLogger.For(ctx, h.logger, chatLoggerName), "chat_agent_session_auto_run_policy_known", nil,
		zap.String(logkeys.Stage, "evaluate_follow_up_auto_run_policy"),
		zap.Uint64(appLogger.KeyChannelID, session.ChannelID),
		zap.Uint64(appLogger.KeyParentMessageID, session.ParentMessageID),
		zap.Uint64(appLogger.KeyAgentSessionID, session.ID),
		zap.String(logkeys.Expected, "automatic or disabled"),
		zap.Uint8(logkeys.Actual, uint8(session.AutoRunPolicy)),
		zap.String(logkeys.Fallback, "skip_paid_follow_up_run"))
	return false
}

// lockAgentInputTargets takes the send's locks before the message is written and returns the
// agent sessions it reaches as input or as a delegated child's report (agentInputTargets), or none
// while the runtime driver is not wired (the feature is inert). It locks the run the message is
// tagged with and the channel before it looks the sessions up, so a session created in the channel
// meanwhile either committed before the lookup or waits for the send; then every session the
// message reaches, thread sessions before the main timeline (a report locks its delegated child
// before the parent it wakes, as settlement does). It reads each input target's state; a target
// whose session stopped taking input before its lock (archived, auto-run disabled) is dropped.
func (h *Handler[U]) lockAgentInputTargets(ctx context.Context, channelID, parentMessageID uint64, broadcast bool, senderID, agentRunID uint64) ([]*agentInputTarget, error) {
	if h.agentRunDriver == nil {
		return nil, nil
	}
	if err := h.persistenceHandler.LockChatChannelForSend(ctx, channelID, agentRunID); err != nil {
		return nil, mapChatMessagePersistenceError(err,
			zap.String(logkeys.Stage, "lock_chat_channel_for_send"),
			zap.Uint64(appLogger.KeyChannelID, channelID))
	}
	targets, err := h.agentInputTargets(ctx, channelID, parentMessageID, broadcast, senderID)
	if err != nil || len(targets) == 0 {
		return nil, err
	}
	locks := make([]persistence.ChatAgentSessionSendLock, 0, len(targets)+1)
	inputLocks := make([]int, len(targets))
	for i, target := range targets {
		if target.reportingChild != nil {
			locks = append(locks, persistence.ChatAgentSessionSendLock{Session: target.reportingChild})
		}
		inputLocks[i] = len(locks)
		locks = append(locks, persistence.ChatAgentSessionSendLock{Session: target.session, Input: target.reportingChild == nil})
	}
	states, err := h.persistenceHandler.LockChatAgentSessionsForSend(ctx, channelID, locks)
	if err != nil {
		return nil, mapChatMessagePersistenceError(err,
			zap.String(logkeys.Stage, "lock_agent_sessions_for_send"),
			zap.Uint64(appLogger.KeyChannelID, channelID))
	}
	kept := targets[:0]
	for i, target := range targets {
		if target.reportingChild != nil {
			// The coalescer applies the wake guards to the locked parent itself.
			kept = append(kept, target)
			continue
		}
		target.state = states[inputLocks[i]]
		if target.session.IsLive() && target.session.AutoRunPolicy == common_persistence_models.ChatAgentSessionAutoRunPolicyAutomatic {
			kept = append(kept, target)
		}
	}
	return kept, nil
}

// routeAgentInput decides, before anything is written, how each input target receives the send,
// and returns the targets that receive it. A session that is idle, not suspended, and has nothing
// queued starts a run with the message. An idle session that still holds queued input (its channel
// was archived when its last run ended) takes the send through its queue, which queueAgentInput
// then delivers whole, this input last, in the send's transaction; a message that is never queued
// (a broadcast reply, agent output) starts the run instead, and the queued input reaches that run
// as input that arrived while it was busy. Otherwise the input waits in the session's queue (held
// while it is suspended). A BLOCKED session that is busy or suspended refuses the whole send
// (ErrChatAgentSessionBusy), and so does a busy or suspended session whose queue already reached
// chat_agent_max_pending_inputs (ErrChatAgentPendingInputLimitExceeded). One send never splits
// into a message for one session and queued input for another, so a thread reply broadcast to the
// channel is refused while either session would make it wait (ErrChatAgentBroadcastWouldWait). A
// message tagged with an agent run (agentAuthored) is that agent's output, never queued input: a
// busy or suspended session drops it and reads it as ordinary transcript context, and it never
// refuses the send.
func (h *Handler[U]) routeAgentInput(targets []*agentInputTarget, agentAuthored, broadcast bool) ([]*agentInputTarget, error) {
	kept := targets[:0]
	limit := 0
	for _, target := range targets {
		if target.reportingChild != nil {
			kept = append(kept, target)
			continue
		}
		// A used-up graph pool admits no run, so its idle session takes input as a busy one does.
		busy, suspended := target.state.ActiveRun != nil || target.state.GraphPoolExhausted, target.session.IsSuspended()
		idle := !busy && !suspended
		if idle && (target.state.PendingCount == 0 || broadcast || agentAuthored) {
			kept = append(kept, target)
			continue
		}
		if agentAuthored {
			continue
		}
		if idle {
			// Delivered with the queue in this transaction, so it never adds to the queue's depth.
			target.waits = true
			kept = append(kept, target)
			continue
		}
		if target.state.Policy.InputMode == proto_models.ChatAgentInputMode_CHAT_AGENT_INPUT_MODE_BLOCKED {
			if target.state.ActiveRun == nil && !suspended {
				// Only the used-up pool holds it: waiting for a run to end would not help.
				return nil, serviceErrors.ErrGraphBudgetExhausted
			}
			return nil, serviceErrors.ErrChatAgentSessionBusy
		}
		if broadcast {
			return nil, serviceErrors.ErrChatAgentBroadcastWouldWait
		}
		if limit == 0 {
			limit = h.agentMaxPendingInputs()
		}
		if int(target.state.PendingCount) >= limit {
			return nil, serviceErrors.ErrChatAgentPendingInputLimitExceeded
		}
		target.waits = true
		kept = append(kept, target)
	}
	return kept, nil
}

// waitingAgentInputTarget returns the target the send must wait for, or nil when it waits for none.
// routeAgentInput leaves at most one: only an unbroadcast send can wait, and it reaches one session.
func waitingAgentInputTarget(targets []*agentInputTarget) *agentInputTarget {
	for _, target := range targets {
		if target.waits {
			return target
		}
	}
	return nil
}

// deliverAgentInput writes each target's route in the transaction that wrote the message, while
// the send still holds its locks, and returns the receipts of the runs the message started. Every
// run it starts is driven after commit.
func (h *Handler[U]) deliverAgentInput(ctx context.Context, senderID uint64, message *common_service_models.ChatMessage, targets []*agentInputTarget) ([]*proto_models.AgentInputReceipt, error) {
	var receipts []*proto_models.AgentInputReceipt
	for _, target := range targets {
		if target.reportingChild != nil {
			h.wakeDelegationParent(ctx, target.reportingChild, target.session, message)
			continue
		}
		runID, err := h.startAgentInputRun(ctx, target.session, target.state.GraphPooled, senderID, message)
		if err != nil {
			return nil, err
		}
		if runID != 0 {
			receipts = append(receipts, agentInputReceipt(target.session.ID, proto_models.ChatAgentInputReceiptState_CHAT_AGENT_INPUT_RECEIPT_STATE_RUN_STARTED, runID))
		}
	}
	return receipts, nil
}

// queueAgentInput queues a send for the session it must wait for, while the send still holds its
// locks, and writes no message: delivery creates it when the agent receives the input. The send's
// attachments are created for the queued input, which owns them until delivery moves them to its
// message. The receipt is PENDING, or HELD while the session is suspended. A session found idle and
// unsuspended still had input queued, so the whole queue, this input last, is delivered into one
// run at once and the send's own message comes back ADMITTED. Delivery discards input whose sender
// can no longer send to the channel; when it discards this send's (the sender left after the
// membership check), the send fails with ErrChatNotMember and its transaction, the drain included,
// rolls back.
func (h *Handler[U]) queueAgentInput(
	ctx context.Context,
	actor U,
	channel *common_service_models.ChatChannel,
	target *agentInputTarget,
	input persistence.NewChatAgentQueuedInput,
	attachments []*proto_models.AttachmentMetadata,
) (*sentMessage, error) {
	session := target.session
	fields := []zap.Field{
		zap.String(logkeys.Stage, "queue_agent_input"),
		zap.Uint64(appLogger.KeyAgentSessionID, session.ID),
		zap.Uint64(appLogger.KeyUserID, input.SenderID),
	}
	input.Session = session
	queuedInput, err := h.persistenceHandler.EnqueueChatAgentInput(ctx, input)
	if err != nil {
		return nil, mapChatMessagePersistenceError(err, fields...)
	}
	sent := &sentMessage{}
	if len(attachments) > 0 {
		var attachmentIDs []uint64
		if attachmentIDs, sent.uploads, err = h.createSendAttachmentUploads(ctx, input.SenderID, queuedInputAttachmentOwner(queuedInput.ID), attachments); err != nil {
			return nil, err
		}
		if len(attachmentIDs) > 0 {
			if err := h.persistenceHandler.SetChatAgentQueuedInputAttachmentIDs(ctx, queuedInput.ID, attachmentIDs); err != nil {
				return nil, mapChatMessagePersistenceError(err, fields...)
			}
		}
	}
	held := session.IsSuspended()
	var run *common_persistence_models.ChatAgentRun
	var delivery persistence.ChatAgentQueuedInputDelivery
	if !held && target.state.ActiveRun == nil && !target.state.GraphPoolExhausted {
		if run, delivery, err = h.persistenceHandler.DrainChatAgentInput(ctx, session.ID); err != nil {
			return nil, mapChatMessagePersistenceError(err, fields...)
		}
	}
	if !delivery.Changed() {
		// The queue length was read under the session lock the send still holds; the depth is
		// recorded once the send commits, and only for input that stays queued.
		metrics, depth := h.persistenceHandler.GetMetricsHandler(), target.state.PendingCount+1
		if !persistence.AddPostCommitHookFromContext(ctx, func(context.Context) error {
			metrics.RecordChatAgentInputEnqueued(depth)
			return nil
		}) {
			metrics.RecordChatAgentInputEnqueued(depth)
		}
		h.PublishAgentSessionInteractionAfterCommit(ctx, session.ID, session.ChannelID, session.ParentMessageID)
		// Re-read with its sender and the attachments recorded since it was queued.
		row, err := h.persistenceHandler.GetChatAgentQueuedInput(ctx, queuedInput.ID)
		if err != nil {
			return nil, mapAgentQueuedInputError(err, fields...)
		}
		views, err := h.agentQueuedInputViews(ctx, senderQueuedInputViewer(actor, channel), row)
		if err != nil {
			return nil, err
		}
		sent.queuedInput = views[0]
		state := proto_models.ChatAgentInputReceiptState_CHAT_AGENT_INPUT_RECEIPT_STATE_PENDING
		if held {
			state = proto_models.ChatAgentInputReceiptState_CHAT_AGENT_INPUT_RECEIPT_STATE_HELD
		}
		sent.receipts = []*proto_models.AgentInputReceipt{agentInputReceipt(session.ID, state, 0)}
		return sent, nil
	}
	messageID := delivery.MessageFor(queuedInput.ID)
	if messageID == 0 {
		return nil, serviceErrors.ErrChatNotMember
	}
	// The delivered input is announced before the run it started.
	h.PublishDeliveredAgentInputAfterCommit(ctx, session.ID, session.ChannelID, session.ParentMessageID, delivery.MessageIDs())
	h.startAgentRunAfterCommit(ctx, session.AgentType, run.ID, run.ChannelID, run.ParentMessageID)
	created, err := h.persistenceHandler.GetChatMessageByID(ctx, messageID)
	if err != nil {
		return nil, mapChatMessagePersistenceError(err, fields...)
	}
	sent.message = h.converter.PersistenceChatMessageToService(created)
	sent.receipts = []*proto_models.AgentInputReceipt{agentInputReceipt(session.ID, proto_models.ChatAgentInputReceiptState_CHAT_AGENT_INPUT_RECEIPT_STATE_ADMITTED, run.ID)}
	return sent, nil
}

// startAgentInputRun creates the run a message starts in an idle session, capturing the session's
// ACTIVE config version (CreateChatAgentRunForSend, under the send's locks) so a follow-up turn
// uses the latest tool/approval/compaction policy, and drives it after commit. A run of a graph with
// a budget pool (pooled) carries no library work ceilings, as a drained follow-up does: the pool
// bounds it. It returns the run id, or zero when the session's location no longer takes runs.
func (h *Handler[U]) startAgentInputRun(ctx context.Context, session *common_persistence_models.ChatAgentSession, pooled bool, senderID uint64, message *common_service_models.ChatMessage) (uint64, error) {
	triggeringUserID := senderID
	triggeringMessageID := uint64(message.ID)
	run := &common_persistence_models.ChatAgentRun{
		OrganizationID:      session.OrganizationID,
		ChannelID:           session.ChannelID,
		ParentMessageID:     session.ParentMessageID,
		AgentSessionID:      &session.ID,
		BotUserID:           session.BotUserID,
		TriggeringUserID:    &triggeringUserID,
		TriggeringMessageID: &triggeringMessageID,
		Status:              proto_models.ChatAgentRunStatus_CHAT_AGENT_RUN_STATUS_RUNNING,
	}
	if !pooled {
		h.AgentRunBudgetCeilings().ApplyTo(run)
	}
	if err := h.persistenceHandler.CreateChatAgentRunForSend(ctx, run); err != nil {
		if errors.Is(err, persistence.ErrChatAgentSessionScopeInvalid) || errors.Is(err, persistence.ErrChatAgentSessionRunActive) ||
			errors.Is(err, persistence.ErrChatAgentSessionSuspended) {
			appLogger.For(ctx, h.logger, chatLoggerName).Warn("agent session cannot start a run for the message",
				zap.String(logkeys.Stage, "create_follow_up_agent_run"),
				zap.String(logkeys.Fallback, "skip_follow_up_run"),
				zap.Uint64(appLogger.KeyChannelID, session.ChannelID),
				zap.Uint64(appLogger.KeyAgentSessionID, session.ID),
				zap.Uint64(appLogger.KeyMessageID, triggeringMessageID),
				zap.Error(err))
			return 0, nil
		}
		return 0, mapChatMessagePersistenceError(err,
			zap.String(logkeys.Stage, "create_follow_up_agent_run"),
			zap.Uint64(appLogger.KeyChannelID, session.ChannelID),
			zap.Uint64(appLogger.KeyAgentSessionID, session.ID),
			zap.Uint64(appLogger.KeyMessageID, triggeringMessageID))
	}
	h.startAgentRunAfterCommit(ctx, session.AgentType, run.ID, run.ChannelID, run.ParentMessageID)
	return run.ID, nil
}

// agentInputReceipt builds one receipt; runID zero leaves agent_run_id absent.
func agentInputReceipt(sessionID uint64, state proto_models.ChatAgentInputReceiptState, runID uint64) *proto_models.AgentInputReceipt {
	receipt := &proto_models.AgentInputReceipt{AgentSessionId: sessionID, State: state}
	if runID != 0 {
		receipt.AgentRunId = &runID
	}
	return receipt
}

// agentInputReceiptsToProto converts stored receipts to their wire form.
func agentInputReceiptsToProto(stored []persistence.ChatAgentInputReceipt) []*proto_models.AgentInputReceipt {
	if len(stored) == 0 {
		return nil
	}
	receipts := make([]*proto_models.AgentInputReceipt, len(stored))
	for i, receipt := range stored {
		receipts[i] = agentInputReceipt(receipt.AgentSessionID, receipt.State, receipt.AgentRunID)
	}
	return receipts
}

// replaySentMessage returns what an earlier send of actor to channel with the same client message
// ID left, or nil when there is none: its input while it is still queued, with its PENDING or HELD receipt,
// or else its message with the receipts of the agent sessions it reached; either with fresh upload
// instructions for the attachments of the replayed request's files (requested) the sender has not
// uploaded yet (a deleted message takes none), matched by content hash, so they reach the
// attachments whatever owns them by then. The lookup locks the key for the rest of the
// transaction, before any other lock of the send, so a concurrent duplicate waits for the first
// send to commit and then replays it. It runs right after the
// membership check, before every other validation and every busy and pending-limit check: a retry
// never fails for a state the original send already got past.
func (h *Handler[U]) replaySentMessage(
	ctx context.Context,
	actor U,
	channel *common_service_models.ChatChannel,
	clientMessageID string,
	requested []*proto_models.AttachmentMetadata,
) (*sentMessage, error) {
	senderID := uint64(actor.GetID())
	owner, err := h.persistenceHandler.LockChatClientMessageID(ctx, senderID, uint64(channel.ID), clientMessageID)
	if err != nil {
		return nil, mapChatMessagePersistenceError(err,
			zap.String(logkeys.Stage, "load_client_message_id_replay"),
			zap.Uint64(appLogger.KeyChannelID, uint64(channel.ID)),
			zap.Uint64(appLogger.KeyUserID, senderID))
	}
	replay := &sentMessage{}
	var attachmentIDs []uint64
	switch {
	case owner.QueuedInput != nil:
		session, err := h.persistenceHandler.GetChatAgentSessionByID(ctx, owner.QueuedInput.AgentSessionID)
		if err != nil {
			return nil, mapChatMessagePersistenceError(err,
				zap.String(logkeys.Stage, "load_client_message_id_replay_session"),
				zap.Uint64(appLogger.KeyAgentSessionID, owner.QueuedInput.AgentSessionID))
		}
		state := proto_models.ChatAgentInputReceiptState_CHAT_AGENT_INPUT_RECEIPT_STATE_PENDING
		if session.IsSuspended() {
			state = proto_models.ChatAgentInputReceiptState_CHAT_AGENT_INPUT_RECEIPT_STATE_HELD
		}
		// The input as the lookup read it: delivery may take it from the queue meanwhile, moving its
		// uploads to its message, since the replay holds none of the session's locks; the queue's
		// re-reads then show it delivered, as after any send.
		views, err := h.agentQueuedInputViews(ctx, senderQueuedInputViewer(actor, channel), owner.QueuedInput)
		if err != nil {
			return nil, err
		}
		replay.queuedInput = views[0]
		replay.receipts = []*proto_models.AgentInputReceipt{agentInputReceipt(session.ID, state, 0)}
		attachmentIDs = replay.queuedInput.AttachmentIDs
	case owner.Message != nil:
		stored, err := h.persistenceHandler.GetChatAgentInputReceipts(ctx, owner.Message.ID)
		if err != nil {
			return nil, mapChatMessagePersistenceError(err,
				zap.String(logkeys.Stage, "load_client_message_id_replay_receipts"),
				zap.Uint64(appLogger.KeyMessageID, owner.Message.ID))
		}
		replay.message = h.converter.PersistenceChatMessageToService(owner.Message)
		replay.receipts = agentInputReceiptsToProto(stored)
		if replay.message.DeletedAt == nil {
			attachmentIDs = replay.message.AttachmentIDs
		}
	default:
		// Removed queued input leaves nothing behind, so a retry after its removal sends it again.
		return nil, nil
	}
	if len(requested) > 0 {
		if replay.uploads, err = h.reissueSentAttachmentUploads(ctx, senderID, attachmentIDs, requested); err != nil {
			return nil, err
		}
	}
	return replay, nil
}
