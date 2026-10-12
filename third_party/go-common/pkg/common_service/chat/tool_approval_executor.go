package chat

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
	"uuid"

	"github.com/Suhaibinator/SRouter/pkg/logkeys"
	"github.com/Suhaibinator/go-common/pkg/common_context"
	common_persistence "github.com/Suhaibinator/go-common/pkg/common_persistence"
	common_persistence_models "github.com/Suhaibinator/go-common/pkg/common_persistence/common_persistence_models"
	commonai "github.com/Suhaibinator/go-common/pkg/common_service/ai"
	attachmentpkg "github.com/Suhaibinator/go-common/pkg/common_service/attachment"
	"github.com/Suhaibinator/go-common/pkg/common_service/common_service_models"
	"github.com/Suhaibinator/go-common/pkg/common_service/ports"
	appLogger "github.com/Suhaibinator/go-common/pkg/logger"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"google.golang.org/protobuf/proto"
)

// toolWorkPolicy is one sample of the tool-work keys, read as-is: the configuration resolved their
// defaults and bounds. maxConcurrency and attemptCapacity are restart-bound; they size the worker
// semaphores once, when the scheduler first creates them.
type toolWorkPolicy struct {
	// scanLimit bounds how many claimable rows one scan pass considers. The global backstop
	// schedules only the slots currently available; unclaimed rows remain durable for the next scan.
	scanLimit int
	// maxConcurrency is handler-wide, not per scan or channel. One tenant's hung tool therefore
	// cannot create an unbounded goroutine fanout across repeated scanner ticks.
	maxConcurrency int
	// A tool implementation can violate the executor contract and ignore context forever. Timed-out
	// wrappers release normal worker slots, while attemptCapacity bounds the abandoned underlying
	// goroutines across scans.
	attemptCapacity int
	// leaseDuration is how long a worker holds a claim before the row becomes reclaimable; a
	// crashed worker's work is reclaimed after this window.
	leaseDuration time.Duration
	// The execution deadline stays inside the lease: a cooperative timeout has settleTimeout to
	// durably release the claim before another worker may reclaim it.
	executionTimeout time.Duration
	settleTimeout    time.Duration
	scanTimeout      time.Duration
	// Shutdown gives already-canceled inner executions one bounded window to unwind and durably
	// settle. Any execution still unfinished after this global grace period is abandoned.
	shutdownGrace time.Duration
}

// toolWorkPolicy samples the tool-work keys once per operation. A handler built without any
// configuration uses the KMS defaults.
func (h *Handler[U]) toolWorkPolicy() toolWorkPolicy {
	cfg := h.policy()
	return toolWorkPolicy{
		scanLimit: cfg.ChatToolWorkScanLimit(), maxConcurrency: cfg.ChatToolWorkMaxConcurrency(), attemptCapacity: cfg.ChatToolWorkAttemptCapacity(),
		leaseDuration: cfg.ChatToolWorkExecutionLeaseDuration(), executionTimeout: cfg.ChatToolWorkExecutionTimeout(),
		settleTimeout: cfg.ChatToolWorkSettleTimeout(), scanTimeout: cfg.ChatToolWorkScanTimeout(), shutdownGrace: cfg.ChatToolWorkShutdownGrace(),
	}
}

func (h *Handler[U]) startToolWorkScheduler() {
	policy := h.toolWorkPolicy()
	h.toolWorkMu.Lock()
	defer h.toolWorkMu.Unlock()
	// Stop is terminal for this handler lifecycle. In particular, a delayed post-commit nudge must
	// not reopen the scheduler after service shutdown has begun.
	if h.toolWorkStopDone != nil {
		return
	}
	h.toolWorkStopping = false
	if h.toolWorkCtx == nil || h.toolWorkCtx.Err() != nil {
		h.toolWorkCtx, h.toolWorkCancel = context.WithCancel(context.Background())
	}
	if h.toolWorkScanSemaphore == nil {
		// Approval nudges are hints for the same durable queue. Coalescing them behind a single
		// nonblocking scan prevents a burst of approvals from creating a burst of DB goroutines.
		h.toolWorkScanSemaphore = make(chan struct{}, 1)
	}
	if h.toolWorkSemaphore == nil {
		h.toolWorkSemaphore = make(chan struct{}, policy.maxConcurrency)
	}
	if h.toolWorkOutstanding == nil {
		h.toolWorkOutstanding = make(chan struct{}, policy.attemptCapacity)
	}
	if h.toolWorkCompletion == nil {
		h.toolWorkCompletion = make(chan struct{}, 1)
	}
}

func (h *Handler[U]) stopToolWorkScheduler() {
	if h == nil {
		return
	}
	h.toolWorkMu.Lock()
	if h.toolWorkStopDone != nil {
		stopDone := h.toolWorkStopDone
		h.toolWorkMu.Unlock()
		<-stopDone
		return
	}
	h.toolWorkStopping = true
	stopDone := make(chan struct{})
	h.toolWorkStopDone = stopDone
	cancel := h.toolWorkCancel
	outstanding := h.toolWorkOutstanding
	completion := h.toolWorkCompletion
	h.toolWorkMu.Unlock()
	defer close(stopDone)
	if cancel != nil {
		cancel()
	}
	h.toolWorkScanWg.Wait()
	h.toolWorkWg.Wait()
	if len(outstanding) == 0 {
		return
	}
	// Sampled only when executions are still unwinding, once per stop.
	shutdownGrace := h.toolWorkPolicy().shutdownGrace
	if !drainOutstandingToolWork(outstanding, completion, shutdownGrace) {
		appLogger.For(context.Background(), h.logger, chatLoggerName).Warn(
			"tool-work shutdown grace expired; abandoning unfinished executions",
			zap.String(logkeys.Stage, "shutdown_tool_workers"),
			zap.String(logkeys.Fallback, "abandon_fenced_executions"),
			zap.Int(appLogger.KeyOutstanding, len(outstanding)),
			appLogger.DurationSeconds(appLogger.KeyShutdownGraceSeconds, shutdownGrace),
		)
	}
}

// drainOutstandingToolWork waits once, with one global deadline, for inner executions whose
// bounded wrappers have already returned. Admission is closed before this begins, so outstanding
// can only shrink. Completion notifications are coalesced because the channel length is the
// authoritative lifetime count.
func drainOutstandingToolWork(outstanding <-chan struct{}, completion <-chan struct{}, grace time.Duration) bool {
	if len(outstanding) == 0 {
		return true
	}
	timer := time.NewTimer(grace)
	defer timer.Stop()
	for len(outstanding) > 0 {
		select {
		case <-completion:
		case <-timer.C:
			return len(outstanding) == 0
		}
	}
	return true
}

func (h *Handler[U]) canRunToolWork() bool {
	return h.toolCallExecutor != nil || h.toolWorkRunner != nil
}

func (h *Handler[U]) listClaimableToolWork(
	ctx context.Context,
	channelID uint64,
) []*common_persistence_models.ChatMessage {
	if !h.canRunToolWork() {
		return nil
	}
	rows, err := h.persistenceHandler.ListClaimableToolCalls(ctx, channelID, time.Now(), h.toolWorkPolicy().scanLimit)
	if err != nil {
		appLogger.For(ctx, h.logger, chatLoggerName).Warn("failed to list claimable tool calls",
			zap.String(logkeys.Stage, "list_claimable_tool_calls"),
			zap.String(logkeys.Fallback, "retry_on_next_backstop_scan"),
			zap.Uint64(appLogger.KeyChannelID, channelID), zap.Error(err))
		return nil
	}
	return rows
}

// runToolWorkItem carries the policy sampled when the item was admitted through claim, execution,
// and settlement: config validation keeps the execution timeout plus the settle timeout inside the
// lease only within one generation, so resampling mid-item could pair a long admitted deadline with
// a shorter reloaded lease and let another worker reclaim a call that is still running.
func (h *Handler[U]) runToolWorkItem(ctx context.Context, row *common_persistence_models.ChatMessage, policy toolWorkPolicy) {
	if h.toolWorkRunner != nil {
		h.toolWorkRunner(ctx, row)
		return
	}
	h.processToolCallWithinRunBudget(ctx, row, policy)
}

func (h *Handler[U]) processToolCallWithinRunBudget(
	ctx context.Context,
	row *common_persistence_models.ChatMessage,
	policy toolWorkPolicy,
) {
	if row == nil || isNilOrZero(row.AgentRunID) {
		return
	}
	release, acquired := h.tryAcquireApprovedRun(*row.AgentRunID)
	if !acquired {
		return
	}
	defer release()
	run, err := h.persistenceHandler.GetChatAgentRunByID(ctx, *row.AgentRunID)
	if err != nil {
		appLogger.For(ctx, h.logger, chatLoggerName).Warn("failed to load run for approved tool work",
			zap.String(logkeys.Stage, "load_run_for_approved_tool_work"),
			zap.String(logkeys.Fallback, "retry_on_next_backstop_scan"),
			zap.Uint64(appLogger.KeyAgentRunID, *row.AgentRunID), zap.Error(err))
		return
	}
	if run == nil {
		appLogger.LogInvariantViolation(appLogger.For(ctx, h.logger, chatLoggerName), "approved_tool_call_owning_run_exists", nil,
			zap.String(logkeys.Stage, "load_run_for_approved_tool_work"),
			zap.Uint64(appLogger.KeyMessageID, row.ID),
			zap.Uint64(appLogger.KeyAgentRunID, *row.AgentRunID),
			zap.String(logkeys.Expected, "agent run row"),
			zap.String(logkeys.Actual, "nil agent run"),
			zap.String(logkeys.Fallback, "skip_tool_execution"))
		return
	}
	var rootSessionID uint64
	if run.AgentSessionID != nil && h.agentRegistry != nil {
		session, err := h.persistenceHandler.GetChatAgentSessionByID(ctx, *run.AgentSessionID)
		if err != nil || session == nil {
			return
		}
		def, ok := h.agentRegistry.GetAgentType(session.AgentType)
		if !ok {
			return
		}
		if def.IsDelegating() || session.IsDelegatedChild() {
			rootSessionID = session.GraphRootID()
		}
		if def.IsWorkflowOwned() {
			// Approved tools of an owner-driven run execute only inside its owner drive, fenced by
			// the graph root's validator.
			if ports.WorkflowAgentRunFromContext(ctx) != run.ID {
				return
			}
			check := ports.NewOwnerCheck(run.ID, session.ID, session.ParentAgentSessionID)
			if err := ValidateWorkflowGraphOwner(ctx, h.persistenceHandler, h.agentRegistry, def, check); err != nil {
				return
			}
		}
	}
	if lease, ok := common_context.GetChatAgentRunDriveLease(ctx); ok && lease.RunID == run.ID {
		// A drive recovering its own tools (RecoverWorkflowAgentTools) owns this interval: it
		// records the active time and enforces the budget, including any result-recovery floor.
		h.claimAndExecute(ctx, row, policy, nil)
		return
	}
	// In a graph with a budget pool the work is also bounded by the pool's remaining active time.
	pool, err := h.persistenceHandler.GetChatAgentGraphBudget(ctx, rootSessionID)
	if err != nil {
		appLogger.For(ctx, h.logger, chatLoggerName).Warn("failed to load graph budget for approved tool work",
			zap.String(logkeys.Stage, "load_graph_budget_for_approved_tool_work"),
			zap.String(logkeys.Fallback, "retry_on_next_backstop_scan"),
			zap.Uint64(appLogger.KeyAgentRunID, run.ID), zap.Error(err))
		return
	}
	maxDurationMs := h.effectiveActiveDurationLimitMs(run, pool != nil)
	limited := maxDurationMs > 0
	remaining := time.Duration(0)
	if run.ActiveDurationMs < maxDurationMs {
		remaining = time.Duration(maxDurationMs-run.ActiveDurationMs) * time.Millisecond
	}
	if pool != nil && (!limited || pool.RemainingActive() < remaining) {
		limited, remaining = true, pool.RemainingActive()
	}
	if !limited {
		h.claimAndExecute(ctx, row, policy, nil)
		return
	}
	if remaining <= 0 {
		h.failRunForBudget(ctx, run)
		return
	}

	workCtx, cancel := context.WithTimeout(ctx, remaining)
	started := time.Now()
	durationRecorded := false
	h.claimAndExecute(workCtx, row, policy, func(elapsed time.Duration) {
		h.recordRunActiveDuration(ctx, run.ID, elapsed)
		durationRecorded = true
	})
	workCtxErr := workCtx.Err()
	cancel()
	if !durationRecorded {
		h.recordRunActiveDuration(ctx, run.ID, time.Since(started))
	}
	if errors.Is(workCtxErr, context.DeadlineExceeded) && ctx.Err() == nil {
		h.failRunForBudget(ctx, run)
	}
}

func (h *Handler[U]) tryAcquireApprovedRun(runID uint64) (func(), bool) {
	h.approvedRunsMu.Lock()
	defer h.approvedRunsMu.Unlock()
	if h.approvedRuns == nil {
		h.approvedRuns = make(map[uint64]struct{})
	}
	if _, busy := h.approvedRuns[runID]; busy {
		return nil, false
	}
	h.approvedRuns[runID] = struct{}{}
	return func() {
		h.approvedRunsMu.Lock()
		defer h.approvedRunsMu.Unlock()
		delete(h.approvedRuns, runID)
	}, true
}

// WorkflowAgentToolsBusy reports whether this process is still executing a tool call of run runID
// through processToolCallWithinRunBudget, as a call an abandoned drive left running is;
// RecoverWorkflowAgentTools skips the run's calls until it returns.
func (h *Handler[U]) WorkflowAgentToolsBusy(runID uint64) bool {
	h.approvedRunsMu.Lock()
	defer h.approvedRunsMu.Unlock()
	_, busy := h.approvedRuns[runID]
	return busy
}

// effectiveActiveDurationLimitMs is the run's active-duration limit, zero for none: its captured
// limit narrowed by the current library ceiling, or, in a graph with a budget pool (pooled), its
// captured limit alone.
func (h *Handler[U]) effectiveActiveDurationLimitMs(run *common_persistence_models.ChatAgentRun, pooled bool) uint64 {
	if pooled && run != nil {
		return run.MaxActiveDurationMs
	}
	operationalConfig := h.policy()

	current := uint64(operationalConfig.ChatAgentRunMaxActiveDuration().Milliseconds())
	if run == nil || run.MaxActiveDurationMs == 0 {
		return current
	}
	if current == 0 || run.MaxActiveDurationMs < current {
		return run.MaxActiveDurationMs
	}
	return current
}

// agentRunSettleTimeout samples chat_agent_run_settle_timeout once per detached write.
func (h *Handler[U]) agentRunSettleTimeout() time.Duration {
	return h.policy().ChatAgentRunSettleTimeout()
}

func (h *Handler[U]) recordRunActiveDuration(ctx context.Context, runID uint64, elapsed time.Duration) {
	recordCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), h.agentRunSettleTimeout())
	defer cancel()
	if err := h.persistenceHandler.AddChatAgentRunActiveDuration(recordCtx, runID, elapsed); err != nil {
		appLogger.For(recordCtx, h.logger, chatLoggerName).Warn("failed to record approved tool active duration",
			zap.String(logkeys.Stage, "record_approved_tool_active_duration"),
			zap.String(logkeys.Fallback, "retain_previous_active_duration"),
			zap.Uint64(appLogger.KeyAgentRunID, runID), appLogger.DurationMS(appLogger.KeyElapsedMS, elapsed), zap.Error(err))
	}
}

// failRunForBudget ends a run whose approved tool work spent the active-duration budget. The
// transition and its visible notice (the generic SYSTEM_EVENT, or a delegated child's broadcast
// notice that also wakes its parent) are one commit, so a crash between them cannot leave a silent
// terminal run, and the delegation settlement runs inside the same transaction.
func (h *Handler[U]) failRunForBudget(ctx context.Context, run *common_persistence_models.ChatAgentRun) {
	if run == nil {
		return
	}
	settleCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), h.agentRunSettleTimeout())
	defer cancel()
	terminalized, noticeID, err := h.persistenceHandler.TerminalizeChatAgentRunWithNotice(
		settleCtx, run.ID,
		common_persistence.DrivableChatAgentRunStatuses(),
		proto_models.ChatAgentRunStatus_CHAT_AGENT_RUN_STATUS_FAILED,
		proto_models.ChatAgentRunStatusReason_CHAT_AGENT_RUN_STATUS_REASON_BUDGET_ACTIVE_DURATION,
	)
	if err != nil {
		appLogger.For(settleCtx, h.logger, chatLoggerName).Error("failed to stop run at active-duration budget",
			zap.String(logkeys.Stage, "settle_agent_run_active_duration_budget"),
			zap.Uint64(appLogger.KeyAgentRunID, run.ID),
			zap.Stringer(logkeys.Expected, proto_models.ChatAgentRunStatus_CHAT_AGENT_RUN_STATUS_FAILED),
			zap.Error(err))
		return
	}
	if !terminalized {
		return
	}
	h.publishAgentRunTransitionAfterCommit(settleCtx, agentRunTransition(run,
		proto_models.ChatAgentRunStatus_CHAT_AGENT_RUN_STATUS_FAILED,
		proto_models.ChatAgentRunStatusReason_CHAT_AGENT_RUN_STATUS_REASON_BUDGET_ACTIVE_DURATION))
	h.PublishAgentRunNoticeAfterCommit(settleCtx, noticeID)
}

// scheduleToolWorkItem reserves one handler-wide slot without waiting. Returning false leaves the
// row unclaimed, so the next durable scan can consider it; the scanner itself never queues behind a
// slow tool. The execution context is bounded even when the scan context has no deadline.
func (h *Handler[U]) scheduleToolWorkItem(
	ctx context.Context,
	row *common_persistence_models.ChatMessage,
) bool {
	if row == nil {
		return true
	}
	policy := h.toolWorkPolicy()
	h.toolWorkMu.Lock()
	if h.toolWorkStopping {
		h.toolWorkMu.Unlock()
		return false
	}
	if h.toolWorkCtx == nil {
		h.toolWorkCtx, h.toolWorkCancel = context.WithCancel(context.Background())
	}
	if h.toolWorkSemaphore == nil {
		h.toolWorkSemaphore = make(chan struct{}, policy.maxConcurrency)
	}
	if h.toolWorkOutstanding == nil {
		h.toolWorkOutstanding = make(chan struct{}, policy.attemptCapacity)
	}
	if h.toolWorkCompletion == nil {
		h.toolWorkCompletion = make(chan struct{}, 1)
	}
	select {
	case h.toolWorkOutstanding <- struct{}{}:
	default:
		h.toolWorkMu.Unlock()
		appLogger.For(ctx, h.logger, chatLoggerName).Warn("tool-work lifetime cap reached; deferring due calls",
			zap.String(logkeys.Stage, "admit_tool_work"),
			zap.String(logkeys.Fallback, "defer_due_calls"),
			zap.Int(appLogger.KeyOutstandingLimit, cap(h.toolWorkOutstanding)))
		return false
	}
	select {
	case h.toolWorkSemaphore <- struct{}{}:
	default:
		<-h.toolWorkOutstanding
		h.toolWorkMu.Unlock()
		return false
	}
	h.toolWorkWg.Add(1)
	semaphore := h.toolWorkSemaphore
	outstanding := h.toolWorkOutstanding
	completion := h.toolWorkCompletion
	schedulerCtx := h.toolWorkCtx
	timeout := policy.executionTimeout
	h.toolWorkMu.Unlock()

	go func() {
		defer func() {
			<-semaphore
			h.toolWorkWg.Done()
		}()
		workCtx, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()
		stopSchedulerCancel := func() bool { return false }
		if schedulerCtx != nil {
			stopSchedulerCancel = context.AfterFunc(schedulerCtx, cancel)
		}
		defer stopSchedulerCancel()
		done := make(chan struct{})
		go func() {
			defer func() {
				<-outstanding
				close(done)
				select {
				case completion <- struct{}{}:
				default:
				}
			}()
			h.runToolWorkItem(workCtx, row, policy)
		}()
		select {
		case <-done:
		case <-workCtx.Done():
			// Go cannot kill unfinished work. Release the normal worker slot so global scans
			// continue; its underlying goroutine remains counted by outstanding until it returns,
			// and every late persistence write remains protected by the per-call lease.
			appLogger.For(ctx, h.logger, chatLoggerName).Warn("tool work outlived its bounded worker context; releasing slot",
				zap.String(logkeys.Stage, "execute_claimed_tool_call"),
				zap.String(logkeys.Fallback, "release_worker_slot_keep_execution_fenced"),
				zap.Uint64(appLogger.KeyMessageID, row.ID),
				zap.Int(appLogger.KeyOutstandingLimit, cap(outstanding)),
				zap.Error(workCtx.Err()))
		}
	}()
	return true
}

// dispatchApprovedWorkBackstop is the global missed-nudge/restart path. It only fills currently
// available worker slots and returns immediately, so one stuck tool cannot stop future scans.
func (h *Handler[U]) dispatchApprovedWorkBackstop(ctx context.Context, channelID uint64) {
	ctx = commonai.WithToolPolicyBatch(ctx)
	// Nudge bursts coalesce: the durable queue makes a missed hint harmless, and the periodic
	// scanner supplies the restart backstop. Track the one active scan so shutdown can cancel/wait it.
	h.toolWorkMu.Lock()
	if h.toolWorkStopping {
		h.toolWorkMu.Unlock()
		return
	}
	if h.toolWorkCtx == nil {
		h.toolWorkCtx, h.toolWorkCancel = context.WithCancel(context.Background())
	}
	if h.toolWorkScanSemaphore == nil {
		h.toolWorkScanSemaphore = make(chan struct{}, 1)
	}
	select {
	case h.toolWorkScanSemaphore <- struct{}{}:
	default:
		h.toolWorkMu.Unlock()
		return
	}
	h.toolWorkScanWg.Add(1)
	scanSemaphore := h.toolWorkScanSemaphore
	schedulerCtx := h.toolWorkCtx
	h.toolWorkMu.Unlock()
	defer func() {
		<-scanSemaphore
		h.toolWorkScanWg.Done()
	}()

	scanCtx, cancel := context.WithTimeout(ctx, h.toolWorkPolicy().scanTimeout)
	stopSchedulerCancel := context.AfterFunc(schedulerCtx, cancel)
	rows := h.listClaimableToolWork(scanCtx, channelID)
	stopSchedulerCancel()
	cancel()
	for _, row := range rows {
		if ctx.Err() != nil || !h.scheduleToolWorkItem(ctx, row) {
			return
		}
	}
}

// processApprovedWorkForChannel scans for claimable TOOL_CALLs in one channel (the nudge-triggered
// fast path) and processes each. channelID==0 scans across all channels (the background backstop).
// It handles approval-required calls and retryable inline calls through the same durable executor.
func (h *Handler[U]) processApprovedWorkForChannel(ctx context.Context, channelID uint64) {
	ctx = commonai.WithToolPolicyBatch(ctx)
	for _, row := range h.listClaimableToolWork(ctx, channelID) {
		if ctx.Err() != nil {
			return
		}
		policy := h.toolWorkPolicy()
		workCtx, cancel := context.WithTimeout(ctx, policy.executionTimeout)
		h.runToolWorkItem(workCtx, row, policy)
		cancel()
	}
}

// isTerminalRunStatus reports whether a run is in a state where no further work may execute for
// it (COMPLETED / FAILED / CANCELLED / MODEL_STEP_LIMIT_EXCEEDED).
func isTerminalRunStatus(s proto_models.ChatAgentRunStatus) bool {
	switch s {
	case proto_models.ChatAgentRunStatus_CHAT_AGENT_RUN_STATUS_COMPLETED,
		proto_models.ChatAgentRunStatus_CHAT_AGENT_RUN_STATUS_FAILED,
		proto_models.ChatAgentRunStatus_CHAT_AGENT_RUN_STATUS_CANCELLED,
		proto_models.ChatAgentRunStatus_CHAT_AGENT_RUN_STATUS_MODEL_STEP_LIMIT_EXCEEDED:
		return true
	default:
		return false
	}
}

// registerAgentRunToolWork roots one claimed-tool attempt in a run-scoped local cancellation
// registry. Registration happens before the durable claim, closing the claim/cancel race on this
// replica: work already registered observes cancellation directly, while work registering after a
// winning cancellation is rejected by the terminal-run predicate in ClaimToolCall.
func (h *Handler[U]) registerAgentRunToolWork(ctx context.Context, runID uint64) (context.Context, func()) {
	if ctx == nil {
		ctx = context.Background()
	}
	workCtx, cancel := context.WithCancel(ctx)
	h.toolRunWorkMu.Lock()
	if h.toolRunWorkCancels == nil {
		h.toolRunWorkCancels = make(map[uint64]map[uint64]context.CancelFunc)
	}
	h.toolRunWorkSeq++
	token := h.toolRunWorkSeq
	registered := h.toolRunWorkCancels[runID]
	if registered == nil {
		registered = make(map[uint64]context.CancelFunc)
		h.toolRunWorkCancels[runID] = registered
	}
	registered[token] = cancel
	h.toolRunWorkMu.Unlock()

	var once sync.Once
	return workCtx, func() {
		once.Do(func() {
			h.toolRunWorkMu.Lock()
			if registered := h.toolRunWorkCancels[runID]; registered != nil {
				delete(registered, token)
				if len(registered) == 0 {
					delete(h.toolRunWorkCancels, runID)
				}
			}
			h.toolRunWorkMu.Unlock()
			cancel()
		})
	}
}

// claimAndExecute atomically claims one TOOL_CALL row and, if this worker won the claim, executes it
// through the pure runtime executor and records the outcome. onExecutionComplete durably accounts
// execution time before result recording can nudge the continuation loop.
func (h *Handler[U]) claimAndExecute(
	ctx context.Context,
	row *common_persistence_models.ChatMessage,
	policy toolWorkPolicy,
	onExecutionComplete func(time.Duration),
) {
	if row == nil || row.ToolCall == nil || isNilOrZero(row.AgentRunID) {
		return
	}
	ctx, unregister := h.registerAgentRunToolWork(ctx, *row.AgentRunID)
	defer unregister()
	now := time.Now()
	leaseExpiry := now.Add(policy.leaseDuration)
	// The persisted lease owner is a per-claim fencing token, not a process identity. A single
	// process can still have an old execution running after its lease expires and a second goroutine
	// reclaims the row; sharing a process-global owner would let the stale execution release or mark
	// the newer claim. A fresh UUID makes those attempts fail the persistence owner predicate.
	leaseToken := uuid.New().String()
	var claimed bool
	var err error
	if ports.WorkflowAgentRunFromContext(ctx) == *row.AgentRunID {
		claimed, err = h.persistenceHandler.ClaimWorkflowToolCall(ctx, *row.AgentRunID, row.ID, leaseToken, leaseExpiry, now)
	} else {
		claimed, err = h.persistenceHandler.ClaimToolCall(ctx, row.ID, leaseToken, leaseExpiry, now)
	}
	if err != nil {
		appLogger.For(ctx, h.logger, chatLoggerName).Warn("failed to claim tool call",
			zap.String(logkeys.Stage, "claim_tool_call"),
			zap.String(logkeys.Fallback, "retry_on_next_backstop_scan"),
			zap.Uint64(appLogger.KeyMessageID, row.ID),
			zap.Uint64(appLogger.KeyAgentRunID, *row.AgentRunID),
			zap.Error(err))
		return
	}
	if !claimed {
		// Another worker holds a live claim; nothing to do.
		return
	}
	// An approved tool runs outside the run drive, so no drive heartbeat repeats this; the claim and
	// its end are the two observable edges. The status read derives the same activity durably.
	h.publishAgentRunToolActivity(ctx, row, now)
	defer h.publishAgentRunToolActivity(ctx, row, time.Time{})
	executionAttempt, stillOwned, readErr := h.readClaimedToolCallExecutionAttempt(ctx, row.ID, leaseToken)
	if readErr != nil {
		fields := []zap.Field{
			zap.String(logkeys.Stage, "read_claimed_tool_execution_attempt"),
			zap.String(logkeys.Fallback, "release_claim_for_retry"),
			zap.Uint64(appLogger.KeyMessageID, row.ID),
			zap.Uint64(appLogger.KeyAgentRunID, *row.AgentRunID),
			zap.Error(readErr),
		}
		fields = append(fields, appLogger.ErrorFields(readErr)...)
		if level, overridden := appLogger.ErrorLevel(readErr); overridden && level >= zapcore.ErrorLevel {
			appLogger.For(ctx, h.logger, chatLoggerName).Error("failed to validate claimed tool execution attempt", fields...)
		} else {
			appLogger.For(ctx, h.logger, chatLoggerName).Warn("failed to read claimed tool execution attempt", fields...)
		}
		settleCtx, settleCancel := h.toolWorkSettlementContext(ctx, policy)
		defer settleCancel()
		h.failClaimedExecution(settleCtx, row, *row.AgentRunID, leaseToken, true,
			capError(fmt.Sprintf("read claimed execution attempt: %v", readErr)))
		return
	}
	if !stillOwned {
		// The lease expired or was otherwise lost before execution began. Its current owner is the
		// only worker allowed to settle or execute this call.
		return
	}

	claim, err := h.buildClaimedToolCall(ctx, row)
	if err != nil {
		// Invalid durable input remains terminal. Descriptive metadata reads can fail operationally;
		// explicitly classified failures release the claim for the existing durable retry/backoff.
		retryable := false
		if classified, ok := errors.AsType[*ports.AgentToolExecutionError](err); ok {
			retryable = classified.Retryable()
		}
		fields := []zap.Field{
			zap.String(logkeys.Stage, "build_claimed_tool_call"),
			zap.Uint64(appLogger.KeyMessageID, row.ID),
			zap.Uint64(appLogger.KeyAgentRunID, *row.AgentRunID),
			zap.Error(err),
		}
		fields = append(fields, appLogger.ErrorFields(err)...)
		if retryable {
			fields = append(fields, zap.String(logkeys.Fallback, "release_claim_for_retry"))
			appLogger.For(ctx, h.logger, chatLoggerName).Warn("failed to build claimed tool execution input", fields...)
		} else {
			appLogger.For(ctx, h.logger, chatLoggerName).Error("failed to build claimed tool execution input", fields...)
		}
		settleCtx, settleCancel := h.toolWorkSettlementContext(ctx, policy)
		defer settleCancel()
		h.failClaimedExecution(settleCtx, row, *row.AgentRunID, leaseToken, retryable, capError(fmt.Sprintf("build claim failed: %v", err)))
		return
	}
	claim.ExecutionAttemptCount = executionAttempt

	executionStarted := time.Now()
	result, retryable, execErr := ports.ExecuteClaimedToolCallSafely(ctx, h.toolCallExecutor, claim)
	if onExecutionComplete != nil {
		onExecutionComplete(time.Since(executionStarted))
	}
	settleCtx, settleCancel := h.toolWorkSettlementContext(ctx, policy)
	defer settleCancel()
	if execErr != nil {
		// Deadline/cancellation is an operational interruption, never a terminal tool-domain
		// verdict. Keep the durable call retryable even if an executor forgot to classify it, unless
		// this was the final allowed attempt (which must end even a repeated timeout loop).
		if ctx.Err() != nil && !errors.Is(execErr, ports.ErrToolExecutionAttemptsExceeded) {
			retryable = true
			execErr = ctx.Err()
		}
		// Operational failure (panic, registry miss, transport). retryable decides reclaim vs terminal.
		h.failClaimedExecution(settleCtx, row, claim.AgentRunID, leaseToken, retryable, capError(execErr.Error()))
		return
	}
	// Success or domain error: record the paired TOOL_RESULT, mark executed, keep run RUNNING, resume.
	h.recordExecutionResult(settleCtx, row, claim, leaseToken, result)
}

// readClaimedToolCallExecutionAttempt returns a lease-token-fenced, post-claim attempt count. The
// optimized production capability avoids hydrating a message. A custom/decorated persistence that
// does not forward it falls back to the required transcript read contract and verifies the fresh
// token itself. Missing ownership metadata or a zero post-claim count fails closed before the app
// executor can produce another side effect.
func (h *Handler[U]) readClaimedToolCallExecutionAttempt(
	ctx context.Context,
	messageID uint64,
	leaseOwner string,
) (uint32, bool, error) {
	if reader, ok := h.persistenceHandler.(common_persistence.ClaimedToolCallAttemptReader); ok {
		attempt, owned, err := reader.GetClaimedToolCallExecutionAttempt(ctx, messageID, leaseOwner)
		if err != nil || !owned {
			return attempt, owned, err
		}
		if attempt == 0 {
			return 0, true, appLogger.InvariantError("claimed_tool_call_attempt_is_positive", errors.New("claimed tool call has zero post-claim execution attempt count"),
				zap.String(logkeys.Stage, "read_claimed_tool_execution_attempt"),
				zap.Uint64(appLogger.KeyMessageID, messageID),
				zap.String(logkeys.Expected, "execution_attempt_count > 0"),
				zap.Uint32(logkeys.Actual, attempt))
		}
		return attempt, true, nil
	}

	claimed, err := h.persistenceHandler.GetChatMessageByIDWithOptions(ctx, messageID, common_persistence.ChatMessageReadOptions{
		VisibilityMode: common_persistence.ChatMessageVisibilityServiceTranscript,
	})
	if err != nil {
		return 0, false, err
	}
	if claimed == nil || claimed.ToolCall == nil {
		return 0, false, appLogger.InvariantError("claimed_tool_call_read_has_tool_metadata", errors.New("claimed tool call read returned no tool metadata"),
			zap.String(logkeys.Stage, "read_claimed_tool_execution_attempt"),
			zap.Uint64(appLogger.KeyMessageID, messageID),
			zap.String(logkeys.Expected, "claimed tool call metadata"),
			zap.String(logkeys.Actual, "missing claimed row or tool metadata"))
	}
	toolCall := claimed.ToolCall
	if toolCall.ExecutionState != proto_models.ToolCallExecutionState_TOOL_CALL_EXECUTION_STATE_EXECUTING ||
		toolCall.ExecutionLeaseOwner == nil || *toolCall.ExecutionLeaseOwner != leaseOwner {
		return 0, false, nil
	}
	if toolCall.ExecutionAttemptCount == 0 {
		return 0, true, appLogger.InvariantError("claimed_tool_call_attempt_is_positive", errors.New("claimed tool call has zero post-claim execution attempt count"),
			zap.String(logkeys.Stage, "read_claimed_tool_execution_attempt"),
			zap.Uint64(appLogger.KeyMessageID, messageID),
			zap.String(logkeys.Expected, "execution_attempt_count > 0"),
			zap.Uint32(logkeys.Actual, toolCall.ExecutionAttemptCount))
	}
	return toolCall.ExecutionAttemptCount, true, nil
}

// toolWorkSettlementContext preserves the ordinary request context while it is usable. Once a
// per-call deadline/cancellation fires, it detaches cancellation (and any completed request
// transaction) for one short, bounded durability window so the claim can be released or its result
// committed instead of sitting EXECUTING until the much longer lease expiry.
func (h *Handler[U]) toolWorkSettlementContext(ctx context.Context, policy toolWorkPolicy) (context.Context, context.CancelFunc) {
	if ctx.Err() == nil {
		return ctx, func() {}
	}
	base := common_persistence.WithoutRequestTransaction[U](context.WithoutCancel(ctx))
	return context.WithTimeout(base, policy.settleTimeout)
}

// buildClaimedToolCall assembles the flat, persistence-free ClaimedToolCall the runtime executor
// consumes, from the claimed persistence row (channel/run/tool-call/args off the row + its decoded
// payload) plus one run lookup (bot user, captured config version, triggering user).
func (h *Handler[U]) buildClaimedToolCall(ctx context.Context, row *common_persistence_models.ChatMessage) (ports.ClaimedToolCall, error) {
	var out ports.ClaimedToolCall
	if row.ToolCall == nil || len(row.ToolCall.ModelPayload) == 0 {
		return out, appLogger.InvariantError("claimed_tool_call_has_model_payload", errors.New("tool call row has no payload"),
			zap.String(logkeys.Stage, "build_claimed_tool_call"),
			zap.Uint64(appLogger.KeyMessageID, row.ID),
			zap.String(logkeys.Expected, "non-empty tool call model payload"),
			zap.String(logkeys.Actual, "missing tool call model payload"))
	}
	payload, err := decodeToolCallPayload(row.ToolCall.ModelPayload)
	if err != nil {
		return out, err
	}
	run, err := h.persistenceHandler.GetChatAgentRunByID(ctx, *row.AgentRunID)
	if err != nil {
		return out, fmt.Errorf("run lookup failed: %w", err)
	}
	if run == nil {
		return out, appLogger.InvariantError("claimed_tool_call_owning_run_exists", errors.New("run lookup returned nil"),
			zap.String(logkeys.Stage, "build_claimed_tool_call"),
			zap.Uint64(appLogger.KeyMessageID, row.ID),
			zap.Uint64(appLogger.KeyAgentRunID, *row.AgentRunID),
			zap.String(logkeys.Expected, "agent run row"),
			zap.String(logkeys.Actual, "nil agent run"))
	}
	if isTerminalRunStatus(run.Status) {
		// The claim predicate excludes terminal runs, but the run can go terminal between the
		// claim and this lookup. The error path fails the row without executing; the run-status
		// CAS in failClaimedExecution is from-guarded, so the terminal run itself is untouched.
		return out, fmt.Errorf("run %d is terminal (%s); skipping execution", run.ID, run.Status)
	}

	toolCallID := ""
	if row.ToolCall.ToolCallID != nil {
		toolCallID = *row.ToolCall.ToolCallID
	}
	out = ports.ClaimedToolCall{
		ChannelID:         run.ChannelID,
		ParentMessageID:   run.ParentMessageID,
		AgentRunID:        run.ID,
		BotUserID:         run.BotUserID,
		OrganizationID:    common_service_models.OrganizationIdType(run.OrganizationID),
		ToolName:          payload.GetToolName(),
		ToolCallID:        toolCallID,
		ToolCallMessageID: row.ID,
		ArgumentsJSON:     []byte(payload.GetArgumentsJson()),
		// Carried verbatim so the executor can refuse an approval-required tool whose row was
		// never approved; being claimable is not evidence of approval.
		ApprovalStatus: row.ToolCall.ApprovalStatus,
	}
	if run.AgentSessionID != nil {
		out.AgentSessionID = *run.AgentSessionID
		// The run does not carry the agent type; the session's is immutable, so this read cannot
		// race a change. AgentType is descriptive for the tool, not authority (the captured config
		// snapshot is), so a missing session leaves it empty rather than failing the call. Operational
		// read errors defer execution through the durable retry path instead of failing approved work.
		session, sessionErr := h.persistenceHandler.GetChatAgentSessionByID(ctx, out.AgentSessionID)
		switch {
		case errors.Is(sessionErr, common_persistence.ErrChatAgentSessionNotFound):
			appLogger.For(ctx, h.logger, chatLoggerName).Warn("claimed tool call session not found; agent type left empty",
				zap.String(logkeys.Stage, "build_claimed_tool_call"),
				zap.Uint64(appLogger.KeyMessageID, row.ID),
				zap.Uint64(appLogger.KeyAgentRunID, run.ID))
		case sessionErr != nil:
			return ports.ClaimedToolCall{}, &ports.AgentToolExecutionError{
				Cause: fmt.Errorf("session lookup failed: %w", sessionErr), CanRetry: true,
			}
		case session != nil:
			out.AgentType = session.AgentType
			metadata, _ := common_service_models.DecodeAgentSessionMetadata(session.MetadataJSON)
			out.Scope = ports.DelegationScope(metadata)
		}
	}
	if run.AgentSessionConfigVersion != nil {
		out.AgentSessionConfigVersion = *run.AgentSessionConfigVersion
	}
	if run.TriggeringUserID != nil {
		out.TriggeringUserID = common_service_models.UserIdType(*run.TriggeringUserID)
	}
	return out, nil
}

// recordExecutionResult writes the paired TOOL_RESULT for a successfully-executed (or domain-errored)
// tool call, marks the TOOL_CALL executed, keeps the run RUNNING, and nudges resume. A domain error
// (result.IsError) is a normal TOOL_RESULT, not a failure — the run continues so the model can react.
func (h *Handler[U]) recordExecutionResult(
	ctx context.Context,
	row *common_persistence_models.ChatMessage,
	claim ports.ClaimedToolCall,
	leaseToken string,
	result *ports.AgentToolResult,
) {
	if result == nil {
		appLogger.LogInvariantViolation(appLogger.For(ctx, h.logger, chatLoggerName), "agent_tool_executor_returns_result_or_error", nil,
			zap.String(logkeys.Stage, "record_tool_execution_result"),
			zap.Uint64(appLogger.KeyAgentRunID, claim.AgentRunID),
			zap.Uint64(appLogger.KeyMessageID, row.ID),
			zap.String(appLogger.KeyToolName, claim.ToolName),
			zap.String(logkeys.Expected, "non-nil result or execution error"),
			zap.String(logkeys.Actual, "nil result and nil error"),
			zap.String(logkeys.Fallback, "fail_claimed_execution_terminally"))
		h.failClaimedExecution(ctx, row, claim.AgentRunID, leaseToken, false, "executor returned nil result and nil error")
		return
	}

	run, err := h.persistenceHandler.GetChatAgentRunByID(ctx, claim.AgentRunID)
	if err != nil {
		appLogger.For(ctx, h.logger, chatLoggerName).Warn("failed to load run before recording tool result",
			zap.String(logkeys.Stage, "load_run_for_tool_result"),
			zap.String(logkeys.Fallback, "release_claim_for_retry"),
			zap.Uint64(appLogger.KeyAgentRunID, claim.AgentRunID),
			zap.Uint64(appLogger.KeyMessageID, row.ID),
			zap.Error(err))
		h.failClaimedExecution(ctx, row, claim.AgentRunID, leaseToken, true, "run lookup failed before recording result")
		return
	}
	if run == nil {
		appLogger.LogInvariantViolation(appLogger.For(ctx, h.logger, chatLoggerName), "tool_result_owning_run_exists", nil,
			zap.String(logkeys.Stage, "load_run_for_tool_result"),
			zap.Uint64(appLogger.KeyAgentRunID, claim.AgentRunID),
			zap.Uint64(appLogger.KeyMessageID, row.ID),
			zap.String(logkeys.Expected, "agent run row"),
			zap.String(logkeys.Actual, "nil agent run"),
			zap.String(logkeys.Fallback, "release_claim_for_retry"))
		h.failClaimedExecution(ctx, row, claim.AgentRunID, leaseToken, true, "run lookup failed before recording result")
		return
	}

	resultMessageID, resultCreated, committed, werr := h.writeExecutionToolResult(
		ctx, run, row.ID, leaseToken, claim.ToolCallID, result,
	)
	if werr != nil {
		// Deterministic validation/binding conflicts cannot become persistable by re-executing an
		// already-completed tool. Operational failures remain retryable under the stable idempotency
		// key supplied to app executors.
		retryableWrite := ports.ToolResultWriteRetryable(werr)
		appLogger.For(ctx, h.logger, chatLoggerName).Warn("failed to record tool result",
			zap.String(logkeys.Stage, "record_claimed_tool_result"),
			zap.String(logkeys.Fallback, "settle_claimed_execution_failure"),
			zap.Uint64(appLogger.KeyMessageID, row.ID),
			zap.Uint64(appLogger.KeyAgentRunID, claim.AgentRunID),
			zap.String(appLogger.KeyToolName, claim.ToolName),
			zap.Error(werr))
		h.failClaimedExecution(ctx, row, claim.AgentRunID, leaseToken, retryableWrite, capError(werr.Error()))
		// The write error may be a lost commit acknowledgement. In that case the token-guarded release
		// above is a no-op because TOOL_RESULT + EXECUTED are already durable. Always run the persisted
		// whole-step gate: a genuine pre-commit failure remains unresolved and safely defers, while an
		// ambiguous committed result opens the gate and cannot strand the run.
		h.finishExecutedToolStep(ctx, run, row.AgentStepIndex)
		return
	}
	if !committed {
		// The fenced transaction verified ownership before any TOOL_RESULT insert. A stale execution
		// cannot bind its payload. The aggregate gate is still safe and useful: if the current claimant
		// already committed but died before its own finalization, the durable state opens the gate;
		// otherwise this call remains unresolved and the gate is a no-op.
		appLogger.For(ctx, h.logger, chatLoggerName).Warn("lost tool call lease before result commit; skipping stale completion",
			zap.String(logkeys.Stage, "commit_claimed_tool_result"),
			zap.String(logkeys.Fallback, "skip_stale_completion"),
			zap.Uint64(appLogger.KeyMessageID, row.ID),
			zap.Uint64(appLogger.KeyAgentRunID, claim.AgentRunID))
		h.finishExecutedToolStep(ctx, run, row.AgentStepIndex)
		return
	}

	executedAt := time.Now()
	// Keep the run RUNNING (it may have been RETRYABLE after a prior reclaim).
	if _, err := h.persistenceHandler.CompareAndSetChatAgentRunStatus(
		ctx, run.ID,
		[]proto_models.ChatAgentRunStatus{
			proto_models.ChatAgentRunStatus_CHAT_AGENT_RUN_STATUS_RUNNING,
			proto_models.ChatAgentRunStatus_CHAT_AGENT_RUN_STATUS_RETRYABLE,
		},
		proto_models.ChatAgentRunStatus_CHAT_AGENT_RUN_STATUS_RUNNING,
		proto_models.ChatAgentRunStatusReason_CHAT_AGENT_RUN_STATUS_REASON_UNSPECIFIED,
	); err != nil {
		appLogger.For(ctx, h.logger, chatLoggerName).Error("failed to keep run RUNNING after result",
			zap.String(logkeys.Stage, "advance_run_after_tool_result"),
			zap.Uint64(appLogger.KeyAgentRunID, run.ID),
			zap.Uint64(appLogger.KeyMessageID, row.ID),
			zap.Stringer(logkeys.Expected, proto_models.ChatAgentRunStatus_CHAT_AGENT_RUN_STATUS_RUNNING),
			zap.Error(err))
	}

	if h.eventPublisher != nil && row.ToolCall != nil &&
		row.ToolCall.ApprovalStatus == proto_models.ToolCallApprovalStatus_TOOL_CALL_APPROVAL_STATUS_APPROVED {
		resolved, reloadErr := h.reloadResolvedToolCall(ctx, row.ID)
		if reloadErr == nil {
			h.publishToolCallResolution(ctx, row.ID, run.ChannelID, resolved, nil, nil, executedAt)
		}
	}

	var resultMsg *common_service_models.ChatMessage
	if resultMessageID != 0 {
		resultMsg = h.reloadServiceTranscriptMessage(ctx, resultMessageID)
	}
	if resultCreated && resultMsg != nil {
		h.publishTranscriptMessageSent(ctx, resultMsg, common_service_models.UserIdType(run.BotUserID))
	}
	// Whole-step completion is derived from durable call state, not from the best-effort result
	// reload used only for websocket fanout. A transient post-commit reload failure must never leave
	// an EXECUTED call on a RUNNING run with no future scanner candidate or resume nudge.
	h.finishExecutedToolStep(ctx, run, row.AgentStepIndex)
}

// failClaimedExecution releases the lease for a failed execution: retryable returns the row to
// PENDING_EXECUTION (reclaimable) and sets the run RETRYABLE; terminal sets EXECUTION_FAILED and the
// run FAILED. The lease-owner predicate ensures only this worker's claim is released. Terminal
// release + run failure are one run-row-locked transaction so a sibling approval cannot observe the
// failed call as settled while the run is still WAITING_FOR_APPROVAL and revive the step.
func (h *Handler[U]) failClaimedExecution(
	ctx context.Context,
	row *common_persistence_models.ChatMessage,
	runID uint64,
	leaseToken string,
	retryable bool,
	lastErr string,
) {
	errCopy := lastErr
	if !retryable && runID != 0 {
		var released, failed bool
		var noticeID uint64
		err := h.runOwnedTransaction(ctx, func(txCtx context.Context) error {
			// Serialize terminal execution failure against sibling approval/denial run-state CASes.
			// ReleaseToolCallLease remains the fencing authority: if this worker lost its per-claim
			// token, the transaction deliberately makes no run-level mutation.
			if err := h.persistenceHandler.LockChatAgentRunForUpdate(txCtx, runID); err != nil {
				return err
			}
			var err error
			released, err = h.persistenceHandler.ReleaseToolCallLease(txCtx, row.ID, leaseToken, false, &errCopy)
			if err != nil || !released {
				return err
			}
			// The failure notice is part of the same outer transaction as the token-fenced lease
			// release and FAILED transition. A crash cannot leave an EXECUTION_FAILED call on a
			// terminal run without a durable user-visible lifecycle event.
			failed, noticeID, err = h.persistenceHandler.FailChatAgentRunWithNotice(txCtx, runID,
				proto_models.ChatAgentRunStatusReason_CHAT_AGENT_RUN_STATUS_REASON_TOOL_EXECUTION_FAILED)
			return err
		})
		if err != nil {
			appLogger.For(ctx, h.logger, chatLoggerName).Error("failed to atomically fail claimed tool execution",
				zap.String(logkeys.Stage, "settle_claimed_tool_execution_failure"),
				zap.Uint64(appLogger.KeyMessageID, row.ID),
				zap.Uint64(appLogger.KeyAgentRunID, runID),
				zap.Error(err))
			return
		}
		if !released {
			// Losing the fencing predicate means this execution is stale. The current lease holder
			// owns both the TOOL_CALL and corresponding run transition.
			appLogger.For(ctx, h.logger, chatLoggerName).Warn("tool call lease was no longer held; skipping run status mutation",
				zap.String(logkeys.Stage, "settle_claimed_tool_execution_failure"),
				zap.String(logkeys.Fallback, "skip_stale_run_status_mutation"),
				zap.Uint64(appLogger.KeyMessageID, row.ID),
				zap.Uint64(appLogger.KeyAgentRunID, runID))
		}
		if failed {
			h.publishAgentRunTransitionAfterCommit(ctx, agentRunTransitionForRow(row, runID,
				proto_models.ChatAgentRunStatus_CHAT_AGENT_RUN_STATUS_FAILED,
				proto_models.ChatAgentRunStatusReason_CHAT_AGENT_RUN_STATUS_REASON_TOOL_EXECUTION_FAILED))
			h.PublishAgentRunNoticeAfterCommit(ctx, noticeID)
		}
		return
	}

	released, err := h.persistenceHandler.ReleaseToolCallLease(ctx, row.ID, leaseToken, retryable, &errCopy)
	if err != nil {
		appLogger.For(ctx, h.logger, chatLoggerName).Warn("failed to release tool call lease",
			zap.String(logkeys.Stage, "release_tool_call_lease"),
			zap.String(logkeys.Fallback, "allow_lease_expiry_recovery"),
			zap.Uint64(appLogger.KeyMessageID, row.ID),
			zap.Uint64(appLogger.KeyAgentRunID, runID),
			zap.Error(err))
		return
	}
	if !released {
		// Losing the fencing predicate means this execution is stale. The current lease holder owns
		// both the TOOL_CALL and the corresponding run transition, so this worker must not mutate the
		// run based on an execution outcome it no longer owns.
		appLogger.For(ctx, h.logger, chatLoggerName).Warn("tool call lease was no longer held; skipping run status mutation",
			zap.String(logkeys.Stage, "release_tool_call_lease"),
			zap.String(logkeys.Fallback, "skip_stale_run_status_mutation"),
			zap.Uint64(appLogger.KeyMessageID, row.ID),
			zap.Uint64(appLogger.KeyAgentRunID, runID))
		return
	}
	if runID == 0 {
		return
	}
	var toStatus proto_models.ChatAgentRunStatus
	var fromStatuses []proto_models.ChatAgentRunStatus
	if retryable {
		toStatus = proto_models.ChatAgentRunStatus_CHAT_AGENT_RUN_STATUS_RETRYABLE
		fromStatuses = []proto_models.ChatAgentRunStatus{proto_models.ChatAgentRunStatus_CHAT_AGENT_RUN_STATUS_RUNNING}
	} else {
		toStatus = proto_models.ChatAgentRunStatus_CHAT_AGENT_RUN_STATUS_FAILED
		fromStatuses = []proto_models.ChatAgentRunStatus{
			proto_models.ChatAgentRunStatus_CHAT_AGENT_RUN_STATUS_RUNNING,
			proto_models.ChatAgentRunStatus_CHAT_AGENT_RUN_STATUS_RETRYABLE,
			proto_models.ChatAgentRunStatus_CHAT_AGENT_RUN_STATUS_WAITING_FOR_APPROVAL,
		}
	}
	// Both outcomes are caused by this tool execution; RETRYABLE keeps the cause until the run resumes.
	reason := proto_models.ChatAgentRunStatusReason_CHAT_AGENT_RUN_STATUS_REASON_TOOL_EXECUTION_FAILED
	swapped, err := h.persistenceHandler.CompareAndSetChatAgentRunStatus(ctx, runID, fromStatuses, toStatus, reason)
	if err != nil {
		appLogger.For(ctx, h.logger, chatLoggerName).Error("failed to set run status after execution failure",
			zap.String(logkeys.Stage, "settle_run_after_tool_execution_failure"),
			zap.Uint64(appLogger.KeyAgentRunID, runID),
			zap.Stringer(logkeys.Expected, toStatus),
			zap.Error(err))
		return
	}
	if swapped {
		h.publishAgentRunTransitionAfterCommit(ctx, agentRunTransitionForRow(row, runID, toStatus, reason))
	}
}

// writeExecutionToolResult persists the real TOOL_RESULT for a completed execution via the shared
// idempotent transcript primitive. Idempotent on (agent_run_id, tool_call_id).
func (h *Handler[U]) writeExecutionToolResult(
	ctx context.Context,
	run *common_persistence_models.ChatAgentRun,
	toolCallMessageID uint64,
	leaseToken string,
	toolCallID string,
	result *ports.AgentToolResult,
) (uint64, bool, bool, error) {
	if err := ports.ValidateToolResultPayload(result.ContentJSON, nil); err != nil {
		return 0, false, false, terminalToolResultWriteError(err)
	}
	previewParts, err := attachmentpkg.PreviewExternalizedToolResultMedia(attachmentpkg.ToolMediaExternalizeInput{
		OwnerUserID: run.BotUserID,
		AgentRunID:  run.ID,
		ToolCallID:  toolCallID,
		LeaseOwner:  leaseToken,
		Parts:       result.ContentParts,
	})
	if err != nil {
		return 0, false, false, terminalToolResultWriteError(err)
	}
	previewPayload, err := marshalExecutionResultPayload(toolCallID, result, previewParts)
	if err != nil {
		return 0, false, false, terminalToolResultWriteError(err)
	}
	if err := ports.ValidateToolResultPayload(result.ContentJSON, previewPayload); err != nil {
		return 0, false, false, terminalToolResultWriteError(err)
	}
	externalized, err := attachmentpkg.ExternalizeToolResultMedia(ctx, h.internalAttachmentStore, attachmentpkg.ToolMediaExternalizeInput{
		OwnerUserID: run.BotUserID,
		AgentRunID:  run.ID,
		ToolCallID:  toolCallID,
		LeaseOwner:  leaseToken,
		Parts:       result.ContentParts,
	})
	if err != nil {
		if errors.Is(err, attachmentpkg.ErrInvalidInternalAttachment) || errors.Is(err, attachmentpkg.ErrInternalAttachmentAccess) {
			return 0, false, false, terminalToolResultWriteError(err)
		}
		return 0, false, false, &ports.ToolResultWriteError{Cause: err, CanRetry: true}
	}
	payload, err := marshalExecutionResultPayload(toolCallID, result, externalized.Parts)
	if err != nil {
		return 0, false, false, terminalToolResultWriteError(err)
	}
	if err := ports.ValidateToolResultPayload(result.ContentJSON, payload); err != nil {
		return 0, false, false, terminalToolResultWriteError(err)
	}
	runID := run.ID
	tcID := toolCallID
	in := buildToolResultInput(run, &runID, &tcID, payload, result, externalized.AttachmentIDs)
	messageID, created, committed, writeErr := h.persistenceHandler.CommitClaimedToolCallResult(
		ctx, toolCallMessageID, leaseToken, time.Now(), in,
	)
	// Do not clean up after persistence has started. A failed call can be
	// commit-ambiguous, and created=false can mean a concurrent writer won with the
	// same deterministic attachment. Cleanup here could dangle the persisted row.
	return messageID, created, committed, classifyApprovedToolResultCommitError(writeErr)
}

func terminalToolResultWriteError(err error) error {
	if err == nil {
		return nil
	}
	return &ports.ToolResultWriteError{Cause: err, CanRetry: false}
}

func classifyApprovedToolResultCommitError(err error) error {
	if err == nil {
		return nil
	}
	retryable := true
	switch {
	case errors.Is(err, common_persistence.ErrChatToolCallConflict),
		errors.Is(err, common_persistence.ErrChatToolResultOrphan),
		errors.Is(err, common_persistence.ErrChatTranscriptRunMismatch),
		errors.Is(err, common_persistence.ErrChatTranscriptPayloadInvalid):
		retryable = false
	}
	return &ports.ToolResultWriteError{Cause: err, CanRetry: retryable}
}

// decodeToolCallPayload extracts the ToolCallPayload from a serialized MessageModelPayload blob.
func decodeToolCallPayload(blob []byte) (*proto_models.ToolCallPayload, error) {
	var mp proto_models.MessageModelPayload
	if err := proto.Unmarshal(blob, &mp); err != nil {
		return nil, err
	}
	tc := mp.GetToolCall()
	if tc == nil {
		return nil, errors.New("model payload is not a tool call")
	}
	return tc, nil
}

// capError truncates a tool/operational error string to the persisted column bound.
func capError(s string) string {
	const maxLastExecutionError = 2048
	return boundedUTF8(s, maxLastExecutionError)
}

// RecoverWorkflowAgentTools executes persisted pending tools only in an explicit
// owner context. The normal scanner never admits workflow-owned calls.
func (h *Handler[U]) RecoverWorkflowAgentTools(ctx context.Context, runID uint64) error {
	ctx = commonai.WithToolPolicyBatch(ctx)
	if ports.WorkflowAgentRunFromContext(ctx) != runID {
		return fmt.Errorf("workflow owner context required")
	}
	run, err := h.persistenceHandler.GetChatAgentRunByID(ctx, runID)
	if err != nil {
		return err
	}
	if run == nil {
		return fmt.Errorf("run missing")
	}
	rows, err := h.persistenceHandler.ListRecoverableWorkflowToolCalls(ctx, runID, time.Now(), h.toolWorkPolicy().scanLimit)
	if err != nil {
		return err
	}
	for _, row := range rows {
		if row.AgentRunID != nil && *row.AgentRunID == runID {
			h.processToolCallWithinRunBudget(ctx, row, h.toolWorkPolicy())
		}
	}
	return ctx.Err()
}
