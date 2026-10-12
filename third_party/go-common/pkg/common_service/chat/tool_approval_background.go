package chat

import (
	"context"
	"math/rand/v2"
	"time"

	"github.com/Suhaibinator/SRouter/pkg/logkeys"
	"github.com/Suhaibinator/go-common/internal/hotpoll"
	persistence "github.com/Suhaibinator/go-common/pkg/common_persistence"
	common_persistence_models "github.com/Suhaibinator/go-common/pkg/common_persistence/common_persistence_models"
	"github.com/Suhaibinator/go-common/pkg/common_service/ports"
	appLogger "github.com/Suhaibinator/go-common/pkg/logger"
	"go.uber.org/zap"
)

// expirySweepBatchSize bounds how many expired pending approvals one sweep pass processes.
const expirySweepBatchSize = 200

// A tick may drain several keyset pages after downtime, but remains bounded so catch-up work cannot
// monopolize the shared database pool. The time budget is further capped to half the configured scan
// interval when that interval is shorter.
const (
	expirySweepMaxPages    = 5
	expirySweepMaxDuration = 30 * time.Second
	approvalTaskJitter     = 0.10
)

// StartApprovalBackgroundTasks starts the agentic tool-approval lane's background workers (Decision
// 6): the pending-approval expiry sweep and the approved-work backstop scanner. Idempotent; each
// worker is gated by its configured interval (<=0 disables that worker). Called by the service
// handler after the chat handler is fully wired (executor, dispatcher, publisher).
func (h *Handler[U]) StartApprovalBackgroundTasks() {
	cfg := h.policy()
	if h.approvalTasksCtx != nil {
		return // already started
	}
	expiryInterval := cfg.ChatApprovalExpiryScanInterval()
	workInterval := cfg.ChatApprovedWorkScanInterval()

	h.approvalTasksCtx, h.approvalTasksCancel = context.WithCancel(context.Background())
	// The scheduler also owns immediate post-approval nudges, so initialize it even when the
	// periodic backstop interval is disabled. Stop must still cancel/wait those bounded workers.
	h.startToolWorkScheduler()

	h.approvalTasksWg.Add(2)
	go h.runApprovalExpiryLoop(expiryInterval)
	go h.runApprovedWorkBackstopLoop(workInterval)
}

// StopApprovalBackgroundTasks cancels the approval loops, prevents new owned post-commit runners,
// and waits for admitted approval work through the scheduler's bounded shutdown drain. Safe to call
// when never started.
func (h *Handler[U]) StopApprovalBackgroundTasks() {
	if h.approvalTasksCancel != nil {
		h.approvalTasksCancel()
	}
	// Set the post-commit stopping gate before either Wait. A sweep already in flight may finish its
	// transaction, but it cannot race a positive Add against the shutdown WaitGroup.Wait.
	h.stopOwnedApprovalPostCommitHooks()
	h.approvalTasksWg.Wait()
	// Nudge scans and the periodic loop synchronize submissions against toolWorkStopping; once it is
	// set, no positive WaitGroup Add can begin while shutdown waits for tracked wrappers.
	h.stopToolWorkScheduler()
}

// runApprovalExpiryLoop sweeps expired pending approvals on each tick (first sweep on the first tick,
// not immediately at startup, matching the repo's background-task convention).
func (h *Handler[U]) runApprovalExpiryLoop(interval time.Duration) {
	defer h.approvalTasksWg.Done()
	defer h.recoverPanic(h.approvalTasksCtx, "tool_approval_expiry_loop")
	logger := h.logger.Named(toolApprovalExpiryLoggerName)
	logger.Info("tool approval expiry sweep started", appLogger.DurationSeconds(appLogger.KeyIntervalSeconds, interval))

	hotpoll.RunWithInitialDelay(h.approvalTasksCtx, interval, jitteredApprovalTaskDelay(interval), func() time.Duration {
		cfg := h.policy()
		if !cfg.EnableChat() {
			return 0
		}
		return cfg.ChatApprovalExpiryScanInterval()
	}, func() {
		h.runWithPanicRecovery(h.approvalTasksCtx, "tool_approval_expiry_tick", func() { h.runApprovalExpirySweep(h.approvalTasksCtx) })
	})
}

func jitteredApprovalTaskDelay(interval time.Duration) time.Duration {
	if interval <= 0 {
		return interval
	}
	factor := (1 - approvalTaskJitter) + rand.Float64()*(2*approvalTaskJitter)
	return time.Duration(float64(interval) * factor)
}

// runApprovedWorkBackstopLoop scans across all channels for approved-but-unexecuted work on each
// tick. This is the restart/missed-nudge backstop; the post-approval nudge handles the fast path.
func (h *Handler[U]) runApprovedWorkBackstopLoop(interval time.Duration) {
	defer h.approvalTasksWg.Done()
	defer h.recoverPanic(h.approvalTasksCtx, "approved_work_backstop_loop")
	logger := h.logger.Named(toolApprovalWorkBackstopLoggerName)
	logger.Info("approved-work backstop scanner started", appLogger.DurationSeconds(appLogger.KeyIntervalSeconds, interval))
	hotpoll.Run(h.approvalTasksCtx, func() time.Duration {
		cfg := h.policy()
		if !cfg.EnableChat() {
			return 0
		}
		return cfg.ChatApprovedWorkScanInterval()
	}, func() {
		h.runWithPanicRecovery(h.approvalTasksCtx, "approved_work_backstop_tick", func() { h.dispatchApprovedWorkBackstop(h.approvalTasksCtx, 0) })
	})
}

// runApprovalExpirySweep lists pending approvals past their stamped approval_expires_at and expires
// each through the shared deny/expiry path (SILENT — synthetic error TOOL_RESULT, no SYSTEM_EVENT).
// Each row is processed with error isolation so one failure does not abort the sweep. Rows with a
// NULL approval_expires_at are never returned by the persistence query, so they never expire.
func (h *Handler[U]) runApprovalExpirySweep(ctx context.Context) {
	h.approvalExpirySweepMu.Lock()
	defer h.approvalExpirySweepMu.Unlock()
	if ctx.Err() != nil {
		return
	}

	now := time.Now()
	startedAt := now
	budget := expirySweepMaxDuration
	cfg := h.policy()
	if cfg.ChatApprovalExpiryScanInterval() > 0 && cfg.ChatApprovalExpiryScanInterval()/2 < budget {
		budget = cfg.ChatApprovalExpiryScanInterval() / 2
	}
	if budget <= 0 {
		budget = expirySweepMaxDuration
	}

	cursor := cloneToolCallExpiryCursor(h.approvalExpiryCursor)
	startedAtHead := cursor == nil
	policyCache := make(map[approvalPolicyCacheKey]ports.AgentApprovalPolicy)
	pages := 0
	scanned := 0
	resolved := 0
	failed := 0
	oldestObserved := false
	oldestOverdue := time.Duration(0)
	backlogPresent := h.approvalExpiryRetryBacklog
	defer func() {
		if metrics := h.persistenceHandler.GetMetricsHandler(); metrics != nil {
			metrics.RecordToolApprovalExpirySweep(
				time.Since(startedAt), scanned, resolved, failed, oldestOverdue, backlogPresent,
			)
		}
	}()
	for pages < expirySweepMaxPages && time.Since(startedAt) < budget {
		batch, err := h.persistenceHandler.ListPendingToolCallExpiryBatch(ctx, now, cursor, expirySweepBatchSize)
		if err != nil {
			appLogger.For(ctx, h.logger, chatLoggerName).Warn("failed to list pending tool calls to expire", zap.Error(err))
			return
		}
		pages++
		if batch == nil || len(batch.Candidates) == 0 {
			h.approvalExpiryCursor = nil
			if startedAtHead {
				// A complete empty pass from the head proves any previously remembered failures have
				// been resolved or removed by another worker.
				h.approvalExpiryRetryBacklog = false
			}
			backlogPresent = h.approvalExpiryRetryBacklog
			break
		}
		// Until every candidate in this page has been handled, overdue work remains.
		backlogPresent = true
		if !oldestObserved {
			first := batch.Candidates[0]
			if first != nil && first.ToolCall != nil && first.ToolCall.ToolCall != nil && first.ToolCall.ToolCall.ApprovalExpiresAt != nil {
				oldestOverdue = max(now.Sub(*first.ToolCall.ToolCall.ApprovalExpiresAt), 0)
				oldestObserved = true
			}
		}

		for _, candidate := range batch.Candidates {
			if ctx.Err() != nil {
				return
			}
			scanned++
			if candidate == nil || candidate.ToolCall == nil || candidate.Run == nil {
				messageID := uint64(0)
				if candidate != nil && candidate.ToolCall != nil {
					messageID = candidate.ToolCall.ID
				}
				appLogger.LogInvariantViolation(appLogger.For(ctx, h.logger, chatLoggerName), "tool_approval_expiry_candidate_has_owning_run", nil,
					zap.String(logkeys.Stage, "expire_pending_tool_calls"),
					zap.Uint64(appLogger.KeyMessageID, messageID),
					zap.String(logkeys.Expected, "candidate, tool call, and owning run"),
					zap.String(logkeys.Actual, "incomplete expiry candidate"),
					zap.String(logkeys.Fallback, "skip_candidate_and_retry_later"))
				failed++
				h.approvalExpiryRetryBacklog = true
				continue
			}
			policy := h.approvalPolicyForExpiryCandidate(ctx, candidate, policyCache, batch.SnapshotLookupErr != nil)
			if h.expirePreparedToolCall(ctx, candidate.ToolCall, candidate.Run, policy) {
				resolved++
			} else {
				failed++
				h.approvalExpiryRetryBacklog = true
			}
		}
		backlogPresent = batch.HasMore || h.approvalExpiryRetryBacklog

		last := batch.Candidates[len(batch.Candidates)-1]
		if last == nil || last.ToolCall == nil || last.ToolCall.ToolCall == nil || last.ToolCall.ToolCall.ApprovalExpiresAt == nil {
			break
		}
		cursor = &persistence.ToolCallExpiryCursor{
			ApprovalExpiresAt: *last.ToolCall.ToolCall.ApprovalExpiresAt,
			MessageID:         last.ToolCall.ID,
		}
		if !batch.HasMore {
			h.approvalExpiryCursor = nil
			if startedAtHead && failed == 0 {
				h.approvalExpiryRetryBacklog = false
				backlogPresent = false
			}
			break
		}
		h.approvalExpiryCursor = cloneToolCallExpiryCursor(cursor)
	}

	if scanned > 0 && appLogger.DebugEnabled(h.logger) {
		appLogger.For(ctx, h.logger, chatLoggerName).Debug("tool approval expiry sweep completed",
			zap.Int(appLogger.KeyPages, pages),
			zap.Int(appLogger.KeyScanned, scanned),
			zap.Int(appLogger.KeyResolved, resolved),
			zap.Int(appLogger.KeyFailed, failed),
			zap.Bool(appLogger.KeyBacklogPresent, backlogPresent),
			appLogger.DurationMS(logkeys.DurationMS, time.Since(startedAt)))
	}
}

func cloneToolCallExpiryCursor(cursor *persistence.ToolCallExpiryCursor) *persistence.ToolCallExpiryCursor {
	if cursor == nil {
		return nil
	}
	copy := *cursor
	return &copy
}

type approvalPolicyCacheKey struct {
	sessionID uint64
	version   uint64
}

func (h *Handler[U]) approvalPolicyForExpiryCandidate(
	ctx context.Context,
	candidate *persistence.PendingToolCallExpiryCandidate,
	cache map[approvalPolicyCacheKey]ports.AgentApprovalPolicy,
	snapshotLookupFailed bool,
) ports.AgentApprovalPolicy {
	def := ports.AgentApprovalPolicy{ResumeOnDenial: true}
	if candidate == nil || candidate.Run == nil || candidate.Run.AgentSessionID == nil || candidate.Run.AgentSessionConfigVersion == nil {
		return def
	}
	key := approvalPolicyCacheKey{sessionID: *candidate.Run.AgentSessionID, version: *candidate.Run.AgentSessionConfigVersion}
	if policy, ok := cache[key]; ok {
		return policy
	}
	if candidate.PolicySnapshot == nil {
		if !snapshotLookupFailed {
			appLogger.LogInvariantViolation(appLogger.For(ctx, h.logger, chatLoggerName), "captured_approval_policy_snapshot_exists", nil,
				zap.String(logkeys.Stage, "load_expiry_approval_policy"),
				zap.Uint64(appLogger.KeyAgentRunID, candidate.Run.ID),
				zap.Uint64(appLogger.KeyAgentSessionID, key.sessionID),
				zap.Uint64(appLogger.KeyConfigVersion, key.version),
				zap.String(logkeys.Expected, "captured config snapshot"),
				zap.String(logkeys.Actual, "nil config snapshot"),
				zap.String(logkeys.Fallback, "use_default_approval_policy"))
		}
		cache[key] = def
		return def
	}
	policy, err := DecodeApprovalPolicy(candidate.PolicySnapshot.ToolConfigPayload)
	if err != nil {
		appLogger.For(ctx, h.logger, chatLoggerName).Warn("failed to decode captured approval policy; using default",
			zap.Uint64(appLogger.KeyAgentRunID, candidate.Run.ID), zap.Error(err))
		cache[key] = def
		return def
	}
	cache[key] = policy
	return policy
}

// expirePreparedToolCall expires one pending TOOL_CALL using run/policy state already bulk-loaded
// for the sweep page. The shared deny/expiry path retains the per-row atomic mutation boundary.
func (h *Handler[U]) expirePreparedToolCall(
	ctx context.Context,
	row *common_persistence_models.ChatMessage,
	run *common_persistence_models.ChatAgentRun,
	policy ports.AgentApprovalPolicy,
) bool {
	if row == nil || row.ToolCall == nil || isNilOrZero(row.AgentRunID) {
		return false
	}
	if _, err := h.applyBackgroundExpiry(ctx, row, run, policy); err != nil {
		appLogger.For(ctx, h.logger, chatLoggerName).Warn("failed to expire pending tool call",
			zap.Uint64(appLogger.KeyMessageID, row.ID), zap.Error(err))
		return false
	}
	return true
}
