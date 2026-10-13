package common_persistence

import (
	"context"
	"errors"
	"slices"
	"strings"
	"time"

	"github.com/Suhaibinator/SRouter/pkg/logkeys"
	"github.com/Suhaibinator/SRouter/pkg/scontext"
	"github.com/Suhaibinator/go-common/pkg/common_context"
	"github.com/Suhaibinator/go-common/pkg/common_persistence/common_persistence_models"
	"github.com/Suhaibinator/go-common/pkg/common_service/common_service_models"
	"github.com/Suhaibinator/go-common/pkg/common_util"
	appLogger "github.com/Suhaibinator/go-common/pkg/logger"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
	"go.uber.org/zap"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// ChatAgentRunFailureNoticeContent is the durable, user-safe lifecycle event written atomically
// whenever an agent run is terminalized as FAILED. Internal causes remain in service logs.
const ChatAgentRunFailureNoticeContent = "The assistant hit an error and stopped."

// ChatAgentRunStepDisposition describes why a completely persisted model step may be advanced.
// Values are durable database data; do not renumber them.
type ChatAgentRunStepDisposition int16

const (
	// The zero value is unspecified and is never written; the blank identifier holds its slot so
	// the durable numbering of the named dispositions below stays stable.
	_ ChatAgentRunStepDisposition = iota
	ChatAgentRunStepDispositionContinue
	ChatAgentRunStepDispositionToolLoop
	ChatAgentRunStepDispositionFinal
	// ChatAgentRunStepDispositionSteered is a final answer sealed while human input waited for the
	// run: the assistant text stays, the run stays RUNNING, and the next step admits the input.
	ChatAgentRunStepDispositionSteered
)

// Requestable reports whether a seal may be requested with d. Steered is never requested: only
// the seal records it, for a final answer it steers under the admission locks.
func (d ChatAgentRunStepDisposition) Requestable() bool {
	switch d {
	case ChatAgentRunStepDispositionContinue, ChatAgentRunStepDispositionToolLoop, ChatAgentRunStepDispositionFinal:
		return true
	}
	return false
}

// DrivableAfterSeal reports whether a run whose latest seal records d takes another model step. A
// final seal completes the run in the same transaction, so a drivable run never legitimately
// carries one.
func (d ChatAgentRunStepDisposition) DrivableAfterSeal() bool {
	switch d {
	case ChatAgentRunStepDispositionContinue, ChatAgentRunStepDispositionToolLoop, ChatAgentRunStepDispositionSteered:
		return true
	}
	return false
}

// ChatAgentRunStepArtifactState summarizes the full durable transcript (not a compacted context
// window) for recovery validation.
type ChatAgentRunStepArtifactState struct {
	HasArtifacts        bool
	MaxStepIndex        uint32
	HasIncompleteStream bool
}

// ChatAgentRunToolStepResolution is the durable aggregate state of the latest sealed tool step.
// It is computed while holding the run-row lock so a denial and a live inline result cannot both
// observe the other as unresolved and leave the run paused forever.
type ChatAgentRunToolStepResolution int

const (
	ChatAgentRunToolStepResolutionUnspecified ChatAgentRunToolStepResolution = iota
	ChatAgentRunToolStepResolutionWaiting
	ChatAgentRunToolStepResolutionContinue
	ChatAgentRunToolStepResolutionCompleted
	ChatAgentRunToolStepResolutionFailed
)

// WithoutRequestTransaction returns a context whose SRouter context is a clone with neither an
// attached transaction nor a run-drive lease, so follow-up work started from it cannot ride a
// request transaction that has already committed (or is about to), and is not mistaken for the
// drive that produced the transition: app-owned lifecycle callbacks may cancel or inspect a
// different run, which a retained lease would reject as stale drive work. The clone also shields
// the caller's context from the mutation.
func WithoutRequestTransaction[U common_service_models.UserLike](ctx context.Context) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	out := scontext.CopySRouterContext[common_service_models.UserIdType, U](ctx, ctx)
	scontext.ClearTransaction[common_service_models.UserIdType, U](out)
	return common_context.ClearChatAgentRunDriveLease(out)
}

// ClaimChatAgentRunDrive atomically acquires the generation lease for a due RUNNING/RETRYABLE run,
// or for a WAITING run selected by the recovery scanner after its sealed tool step fully settled.
// It uses the database clock for both expiry comparison and the new deadline, avoiding replica clock
// skew in the mutual-exclusion decision.
func (h *CommonPersistenceHandler[U]) ClaimChatAgentRunDrive(
	ctx context.Context,
	runID uint64,
	leaseOwner string,
	leaseDuration time.Duration,
) (bool, error) {
	return h.claimChatAgentRunDrive(ctx, runID, leaseOwner, leaseDuration, h.applyRecoverableChatAgentRunPredicate)
}

func (h *CommonPersistenceHandler[U]) claimChatAgentRunDrive(ctx context.Context, runID uint64, leaseOwner string, leaseDuration time.Duration, eligible func(*gorm.DB) *gorm.DB) (bool, error) {
	leaseOwner = strings.TrimSpace(leaseOwner)
	if runID == 0 || leaseOwner == "" || leaseDuration <= 0 {
		return false, nil
	}
	result := h.getDBFromContext(ctx).
		Model(&common_persistence_models.ChatAgentRun{}).
		Where("id = ?", runID).
		Where("agent_session_id IS NOT NULL").
		Where("drive_not_before IS NULL OR drive_not_before <= CURRENT_TIMESTAMP").
		Where("drive_lease_owner IS NULL OR drive_lease_expires_at IS NULL OR drive_lease_expires_at <= CURRENT_TIMESTAMP").
		Scopes(eligible).
		Select("drive_lease_owner", "drive_lease_expires_at", "drive_not_before", "updated_at").
		Updates(struct {
			DriveLeaseOwner     *string     `gorm:"column:drive_lease_owner"`
			DriveLeaseExpiresAt clause.Expr `gorm:"column:drive_lease_expires_at;type:timestamptz"`
			DriveNotBefore      *time.Time  `gorm:"column:drive_not_before"`
			UpdatedAt           clause.Expr `gorm:"column:updated_at;type:timestamptz;autoUpdateTime:false"`
		}{
			DriveLeaseOwner:     &leaseOwner,
			DriveLeaseExpiresAt: databaseLeaseDeadline(leaseDuration),
			DriveNotBefore:      nil,
			UpdatedAt:           gorm.Expr("CURRENT_TIMESTAMP"),
		})
	if result.Error != nil {
		return false, chatPersistenceDatabaseError(result.Error,
			zap.String(logkeys.Stage, "claim_chat_agent_run_drive"),
			zap.Uint64(appLogger.KeyAgentRunID, runID),
			appLogger.DurationSeconds(appLogger.KeyLeaseDurationSeconds, leaseDuration))
	}
	claimed := result.RowsAffected > 0
	if !claimed {
		if appLogger.DebugEnabled(h.logger) {
			appLogger.For(ctx, h.logger, loggerName).Debug("chat agent run drive claim did not match an eligible run",
				zap.Uint64(appLogger.KeyAgentRunID, runID))
		}
	}
	return claimed, nil
}

// RenewChatAgentRunDrive extends a still-live token-owned lease on a nonterminal run. An
// already-expired lease cannot be resurrected, and a terminal run cannot keep a heartbeat alive:
// in either case the caller must stop because its drive authority is gone. A retiring run is not
// renewed either; an owner drive (ctx carries the workflow marker for runID) is also fenced by its
// archived root.
func (h *CommonPersistenceHandler[U]) RenewChatAgentRunDrive(
	ctx context.Context,
	runID uint64,
	leaseOwner string,
	leaseDuration time.Duration,
) (bool, error) {
	leaseOwner = strings.TrimSpace(leaseOwner)
	if runID == 0 || leaseOwner == "" || leaseDuration <= 0 {
		return false, nil
	}
	result := h.getDBFromContext(ctx).
		Model(&common_persistence_models.ChatAgentRun{}).
		Where("id = ? AND drive_lease_owner = ?", runID, leaseOwner).
		Where("drive_lease_expires_at > CURRENT_TIMESTAMP").
		Where("status NOT IN ?", TerminalChatAgentRunStatuses()).
		Where("NOT "+chatAgentRunDriveFenceSQL(ctx, runID)).
		Select("drive_lease_expires_at", "updated_at").
		Updates(struct {
			DriveLeaseExpiresAt clause.Expr `gorm:"column:drive_lease_expires_at;type:timestamptz"`
			UpdatedAt           clause.Expr `gorm:"column:updated_at;type:timestamptz;autoUpdateTime:false"`
		}{
			DriveLeaseExpiresAt: databaseLeaseDeadline(leaseDuration),
			UpdatedAt:           gorm.Expr("CURRENT_TIMESTAMP"),
		})
	if result.Error != nil {
		return false, chatPersistenceDatabaseError(result.Error,
			zap.String(logkeys.Stage, "renew_chat_agent_run_drive"),
			zap.Uint64(appLogger.KeyAgentRunID, runID),
			appLogger.DurationSeconds(appLogger.KeyLeaseDurationSeconds, leaseDuration))
	}
	renewed := result.RowsAffected > 0
	if !renewed {
		if appLogger.DebugEnabled(h.logger) {
			appLogger.For(ctx, h.logger, loggerName).Debug("chat agent run drive renewal lost its live lease",
				zap.Uint64(appLogger.KeyAgentRunID, runID))
		}
	}
	return renewed, nil
}

// ReleaseChatAgentRunDrive clears a token-owned lease. retryAfter durably delays the next recovery
// claim after operational failures; zero makes a still-drivable run immediately eligible.
// retryReason explains that delay to status readers. It is stored only alongside a delay, and every
// release rewrites it, so it never outlives the backoff it describes.
func (h *CommonPersistenceHandler[U]) ReleaseChatAgentRunDrive(
	ctx context.Context,
	runID uint64,
	leaseOwner string,
	retryAfter time.Duration,
	retryReason proto_models.ChatAgentRunStatusReason,
) (bool, error) {
	leaseOwner = strings.TrimSpace(leaseOwner)
	if runID == 0 || leaseOwner == "" {
		return false, nil
	}
	updates := struct {
		DriveLeaseOwner     *string
		DriveLeaseExpiresAt *time.Time
		DriveNotBefore      clause.Expr `gorm:"type:timestamptz"`
		DriveRetryReason    proto_models.ChatAgentRunStatusReason
		UpdatedAt           clause.Expr `gorm:"type:timestamptz;autoUpdateTime:false"`
	}{DriveNotBefore: gorm.Expr("NULL"), UpdatedAt: gorm.Expr("CURRENT_TIMESTAMP")}
	if retryAfter > 0 {
		updates.DriveNotBefore = databaseLeaseDeadline(retryAfter)
		updates.DriveRetryReason = retryReason
	}
	result := h.getDBFromContext(ctx).
		Model(&common_persistence_models.ChatAgentRun{}).
		Where("id = ? AND drive_lease_owner = ?", runID, leaseOwner).
		Where("drive_lease_expires_at > CURRENT_TIMESTAMP").
		Select("drive_lease_owner", "drive_lease_expires_at", "drive_not_before", "drive_retry_reason", "updated_at").
		Updates(updates)
	if result.Error != nil {
		return false, chatPersistenceDatabaseError(result.Error,
			zap.String(logkeys.Stage, "release_chat_agent_run_drive"),
			zap.Uint64(appLogger.KeyAgentRunID, runID),
			appLogger.DurationSeconds(appLogger.KeyRetryAfterSeconds, retryAfter))
	}
	released := result.RowsAffected > 0
	if !released {
		if appLogger.DebugEnabled(h.logger) {
			appLogger.For(ctx, h.logger, loggerName).Debug("chat agent run drive release did not own a live lease",
				zap.Uint64(appLogger.KeyAgentRunID, runID))
		}
	}
	return released, nil
}

// ChatAgentRunDriveAbandonOutcome is what AbandonChatAgentRunDrive committed for the run.
type ChatAgentRunDriveAbandonOutcome int

const (
	// ChatAgentRunDriveAbandonNone: another owner already holds the run, or it was already
	// terminal (the lease is still revoked and the active time recorded).
	ChatAgentRunDriveAbandonNone ChatAgentRunDriveAbandonOutcome = iota
	// ChatAgentRunDriveAbandonRetry: the run is drivable again, at once after a shutdown or after
	// RetryAfter otherwise.
	ChatAgentRunDriveAbandonRetry
	// ChatAgentRunDriveAbandonFailed: the run failed with PROVIDER_TIMEOUT and a visible notice.
	ChatAgentRunDriveAbandonFailed
)

// ChatAgentRunDriveAbandonment describes one drive attempt whose call ignored cancellation.
type ChatAgentRunDriveAbandonment struct {
	// Owner is the abandoned attempt's lease owner.
	Owner string
	// Elapsed is the attempt's active time, which the stuck call will never record.
	Elapsed time.Duration
	// Shutdown marks an abandonment caused by the runtime stopping: it is not counted, and the run
	// is due again at once.
	Shutdown bool
	// RetryAfter delays the next claim after a counted abandonment the run survives.
	RetryAfter time.Duration
	// MaxAbandons is how many counted abandonments a run survives.
	MaxAbandons int
	// AtCap reports that the caller's abandoned-attempt cap is full: the run fails instead of
	// starting another attempt.
	AtCap bool
}

// AbandonChatAgentRunDrive revokes an abandoned drive attempt's claim in one transaction: it adds
// the attempt's active time and clears the lease, so every later token-checked write of the stuck
// call fails. It deliberately does not check expiry: an owner that still matches means nobody has
// taken over. A counted abandonment either delays the next claim (reason PROVIDER_TIMEOUT) or, past
// MaxAbandons or at the cap, fails the run with PROVIDER_TIMEOUT and returns the notice id. No
// matching owner commits nothing.
func (h *CommonPersistenceHandler[U]) AbandonChatAgentRunDrive(ctx context.Context, runID uint64, abandonment ChatAgentRunDriveAbandonment) (ChatAgentRunDriveAbandonOutcome, uint64, error) {
	owner := strings.TrimSpace(abandonment.Owner)
	if runID == 0 || owner == "" {
		return ChatAgentRunDriveAbandonNone, 0, nil
	}
	outcome := ChatAgentRunDriveAbandonNone
	var noticeID uint64
	err := h.transaction(ctx, func(txCtx context.Context, tx *gorm.DB) error {
		var run common_persistence_models.ChatAgentRun
		err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("id = ? AND drive_lease_owner = ?", runID, owner).Take(&run).Error
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil
		}
		if err != nil {
			return err
		}
		updates := struct {
			DriveLeaseOwner     *string                               `gorm:"column:drive_lease_owner"`
			DriveLeaseExpiresAt *time.Time                            `gorm:"column:drive_lease_expires_at"`
			DriveNotBefore      clause.Expr                           `gorm:"column:drive_not_before;type:timestamptz"`
			DriveRetryReason    proto_models.ChatAgentRunStatusReason `gorm:"column:drive_retry_reason"`
			DriveAbandonCount   uint32                                `gorm:"column:drive_abandon_count"`
			ActiveDurationMs    clause.Expr                           `gorm:"column:active_duration_ms;type:bigint"`
			UpdatedAt           clause.Expr                           `gorm:"column:updated_at;type:timestamptz;autoUpdateTime:false"`
		}{
			DriveNotBefore:    gorm.Expr("NULL"),
			DriveAbandonCount: run.DriveAbandonCount,
			ActiveDurationMs:  gorm.Expr("active_duration_ms + ?", activeDurationMillis(abandonment.Elapsed)),
			UpdatedAt:         gorm.Expr("CURRENT_TIMESTAMP"),
		}
		fail := false
		switch {
		case IsTerminalChatAgentRunStatus(run.Status):
		case abandonment.Shutdown:
			outcome = ChatAgentRunDriveAbandonRetry
		default:
			updates.DriveAbandonCount++
			if int64(updates.DriveAbandonCount) > int64(abandonment.MaxAbandons) || abandonment.AtCap {
				fail = true
			} else {
				outcome = ChatAgentRunDriveAbandonRetry
				updates.DriveNotBefore = databaseLeaseDeadline(abandonment.RetryAfter)
				updates.DriveRetryReason = proto_models.ChatAgentRunStatusReason_CHAT_AGENT_RUN_STATUS_REASON_PROVIDER_TIMEOUT
			}
		}
		if err := tx.Model(&common_persistence_models.ChatAgentRun{}).Where("id = ?", runID).
			Select("drive_lease_owner", "drive_lease_expires_at", "drive_not_before", "drive_retry_reason", "drive_abandon_count", "active_duration_ms", "updated_at").
			Updates(updates).Error; err != nil {
			return err
		}
		if err := addChatAgentGraphBudgetActiveTx(tx, runID, activeDurationMillis(abandonment.Elapsed)); err != nil {
			return err
		}
		if !fail {
			return nil
		}
		noticeID, err = h.terminalizeChatAgentRunWithNoticeTx(txCtx, tx, &run, proto_models.ChatAgentRunStatus_CHAT_AGENT_RUN_STATUS_FAILED,
			proto_models.ChatAgentRunStatusReason_CHAT_AGENT_RUN_STATUS_REASON_PROVIDER_TIMEOUT)
		outcome = ChatAgentRunDriveAbandonFailed
		return err
	})
	if err != nil {
		return ChatAgentRunDriveAbandonNone, 0, chatPersistenceDatabaseError(err,
			zap.String(logkeys.Stage, "abandon_chat_agent_run_drive"),
			zap.Uint64(appLogger.KeyAgentRunID, runID),
			zap.Bool("shutdown", abandonment.Shutdown), zap.Bool("at_cap", abandonment.AtCap))
	}
	return outcome, noticeID, nil
}

// activeDurationMillis rounds elapsed up to whole milliseconds, so no recorded interval is lost.
func activeDurationMillis(elapsed time.Duration) uint64 {
	elapsedMs := elapsed.Milliseconds()
	if elapsed%time.Millisecond != 0 {
		elapsedMs++
	}
	return uint64(max(elapsedMs, 0))
}

// ListRecoverableChatAgentRuns returns a bounded set of due runs without a live lease. In addition
// to RUNNING/RETRYABLE it includes WAITING runs only when their exact latest sealed TOOL_LOOP has no
// unresolved calls. That closes the crash window after the final inline result commits but before
// the live owner can reconcile WAITING -> RUNNING/COMPLETED.
func (h *CommonPersistenceHandler[U]) ListRecoverableChatAgentRuns(
	ctx context.Context,
	limit int,
) ([]uint64, error) {
	return h.ListRecoverableChatAgentRunsAfterID(ctx, 0, limit)
}

// ListRecoverableChatAgentRunsAfterID is the keyset-paged recovery read. Ordering solely by the
// immutable primary key lets one sweep advance past any number of rows that are active in this
// process without changing shared eligibility for another replica.
func (h *CommonPersistenceHandler[U]) ListRecoverableChatAgentRunsAfterID(
	ctx context.Context,
	afterID uint64,
	limit int,
) ([]uint64, error) {
	// The recovery sweep samples chat_agent_run_recovery_scan_page_size per tick
	// and passes it here; the store reads it as-is. A non-positive page is empty.
	if limit <= 0 {
		return nil, nil
	}
	var runIDs []uint64
	err := h.getDBFromContext(ctx).
		Model(&common_persistence_models.ChatAgentRun{}).
		Select("id").
		Scopes(h.applyRecoverableChatAgentRunPredicate).
		Where("agent_session_id IS NOT NULL").
		Where("id > ?", afterID).
		Where("drive_not_before IS NULL OR drive_not_before <= CURRENT_TIMESTAMP").
		Where("drive_lease_owner IS NULL OR drive_lease_expires_at IS NULL OR drive_lease_expires_at <= CURRENT_TIMESTAMP").
		Order("id ASC").
		Limit(limit).
		Scan(&runIDs).Error
	if err != nil {
		return nil, chatPersistenceDatabaseError(err,
			zap.String(logkeys.Stage, "list_recoverable_chat_agent_runs"),
			zap.Uint64(appLogger.KeyAfterID, afterID), zap.Int(logkeys.Limit, limit))
	}
	return runIDs, nil
}

// applyRecoverableChatAgentRunPredicate is shared by list and claim so a
// WAITING row is actionable only when its exact latest sealed TOOL_LOOP is fully settled. Keeping
// this as one SQL predicate prevents the scanner from listing a row that either follow-up mutation
// cannot consume, and prevents direct nudges from briefly claiming genuine pending approvals.
// A run its suspended session is stopped on is never drivable: its cancellation is outstanding
// (ListSuspendedChatAgentRuns), so neither the scan nor a nudge such as an approval may drive it.
// Nor is a run of an owner-driven session (its agent type is workflow-owned): only its owner claims
// it, through ClaimWorkflowAgentRunDrive, so the scan never lists it to be turned away every tick.
func (h *CommonPersistenceHandler[U]) applyRecoverableChatAgentRunPredicate(db *gorm.DB) *gorm.DB {
	runs := common_persistence_models.TableNameChatAgentRuns
	if workflowTypes := h.chatAgentWorkflowTypes(); len(workflowTypes) > 0 {
		db = db.Where("NOT EXISTS (SELECT 1 FROM "+common_persistence_models.TableNameChatAgentSessions+
			" recover_sessions WHERE recover_sessions.id = "+runs+".agent_session_id AND recover_sessions.agent_type IN ?)", workflowTypes)
	}
	messages := common_persistence_models.TableNameChatMessages
	toolCalls := common_persistence_models.TableNameChatMessageToolCalls
	settledWaiting := "(" + runs + ".status = ? AND " + runs + ".last_sealed_step_disposition = ? AND " +
		"NOT EXISTS (SELECT 1 FROM " + toolCalls + " recover_calls JOIN " + messages +
		" recover_messages ON recover_messages.id = recover_calls.message_id WHERE recover_calls.agent_run_id = " + runs +
		".id AND recover_calls.kind = ? AND recover_messages.agent_step_index = " + runs +
		".last_sealed_step_index AND recover_calls.execution_state NOT IN (?, ?)))"
	return db.Where("NOT "+chatAgentRunSuspendedOnSQL).Where("NOT "+chatAgentRunInferenceBlockedSQL).Where("NOT "+chatAgentRunRetiringChildSQL).Where("("+runs+".status IN ? OR "+settledWaiting+")",
		DrivableChatAgentRunStatuses(),
		proto_models.ChatAgentRunStatus_CHAT_AGENT_RUN_STATUS_WAITING_FOR_APPROVAL,
		ChatAgentRunStepDispositionToolLoop,
		proto_models.MessageKind_MESSAGE_KIND_TOOL_CALL,
		proto_models.ToolCallExecutionState_TOOL_CALL_EXECUTION_STATE_EXECUTED,
		proto_models.ToolCallExecutionState_TOOL_CALL_EXECUTION_STATE_EXECUTION_FAILED,
	)
}

// ReconcileSealedChatAgentRunToolStep atomically evaluates the exact latest sealed TOOL_LOOP and
// applies its aggregate run transition. A terminal execution failure fails the run; a fully settled
// denial/expiry completes it when ResumeOnDenial is disabled; a fully settled resumable step moves
// WAITING/RETRYABLE back to RUNNING. Unresolved calls leave the run paused. The contextual drive
// token is asserted in the same transaction as the aggregate read and transition.
func (h *CommonPersistenceHandler[U]) ReconcileSealedChatAgentRunToolStep(
	ctx context.Context,
	runID uint64,
	stepIndex uint32,
	resumeOnDenial bool,
) (ChatAgentRunToolStepResolution, error) {
	if runID == 0 {
		return ChatAgentRunToolStepResolutionUnspecified, ErrChatAgentRunNotFound
	}
	resolution := ChatAgentRunToolStepResolutionUnspecified
	err := h.transaction(ctx, func(txCtx context.Context, tx *gorm.DB) error {
		if err := h.assertChatAgentRunDriveLeaseWithDB(ctx, tx, runID); err != nil {
			return err
		}

		var run common_persistence_models.ChatAgentRun
		// The full row is needed: the tool-failure branch writes the visible notice from the run's
		// channel, bot, location, and session, not only from its seal projection.
		if err := tx.Where("id = ?", runID).Take(&run).Error; err != nil {
			return err
		}
		if run.LastSealedStepIndex == nil || *run.LastSealedStepIndex != stepIndex ||
			ChatAgentRunStepDisposition(run.LastSealedStepDisposition) != ChatAgentRunStepDispositionToolLoop ||
			run.LastSealedToolCallCount == 0 {
			return ErrChatAgentRunStepSealInvalid
		}

		type toolStepCounts struct {
			TotalCount      int64
			UnresolvedCount int64
			FailedCount     int64
			DeniedCount     int64
		}
		var counts toolStepCounts
		messages := common_persistence_models.TableNameChatMessages
		toolCalls := common_persistence_models.TableNameChatMessageToolCalls
		if err := tx.Table(toolCalls+" reconcile_calls").
			Select(
				"COUNT(*) AS total_count, "+
					"COUNT(*) FILTER (WHERE reconcile_calls.execution_state NOT IN (?, ?)) AS unresolved_count, "+
					"COUNT(*) FILTER (WHERE reconcile_calls.execution_state = ?) AS failed_count, "+
					"COUNT(*) FILTER (WHERE reconcile_calls.approval_status IN (?, ?)) AS denied_count",
				proto_models.ToolCallExecutionState_TOOL_CALL_EXECUTION_STATE_EXECUTED,
				proto_models.ToolCallExecutionState_TOOL_CALL_EXECUTION_STATE_EXECUTION_FAILED,
				proto_models.ToolCallExecutionState_TOOL_CALL_EXECUTION_STATE_EXECUTION_FAILED,
				proto_models.ToolCallApprovalStatus_TOOL_CALL_APPROVAL_STATUS_DENIED,
				proto_models.ToolCallApprovalStatus_TOOL_CALL_APPROVAL_STATUS_EXPIRED,
			).
			Joins("JOIN "+messages+" reconcile_messages ON reconcile_messages.id = reconcile_calls.message_id").
			Where("reconcile_calls.agent_run_id = ? AND reconcile_calls.kind = ? AND reconcile_messages.agent_step_index = ?",
				runID, proto_models.MessageKind_MESSAGE_KIND_TOOL_CALL, stepIndex).
			Scan(&counts).Error; err != nil {
			return err
		}
		if counts.TotalCount != int64(run.LastSealedToolCallCount) {
			return ErrChatAgentRunStepSealInvalid
		}

		updates := struct {
			Status           proto_models.ChatAgentRunStatus
			StatusReason     proto_models.ChatAgentRunStatusReason
			StatusReasonCode *string
			CompletedAt      clause.Expr `gorm:"type:timestamptz"`
			UpdatedAt        clause.Expr `gorm:"type:timestamptz;autoUpdateTime:false"`
		}{UpdatedAt: gorm.Expr("CURRENT_TIMESTAMP"), CompletedAt: gorm.Expr("NULL")}
		switch {
		case counts.FailedCount > 0:
			resolution = ChatAgentRunToolStepResolutionFailed
			_, err := h.failChatAgentRunWithNoticeTx(txCtx, tx, &run, proto_models.ChatAgentRunStatusReason_CHAT_AGENT_RUN_STATUS_REASON_TOOL_EXECUTION_FAILED)
			return err
		case counts.UnresolvedCount > 0:
			resolution = ChatAgentRunToolStepResolutionWaiting
			return nil
		case counts.DeniedCount > 0 && !resumeOnDenial:
			resolution = ChatAgentRunToolStepResolutionCompleted
			updates.Status = proto_models.ChatAgentRunStatus_CHAT_AGENT_RUN_STATUS_COMPLETED
			updates.StatusReason = proto_models.ChatAgentRunStatusReason_CHAT_AGENT_RUN_STATUS_REASON_APPROVAL_NOT_GRANTED
			updates.StatusReasonCode = nil
			updates.CompletedAt = gorm.Expr("COALESCE(completed_at, CURRENT_TIMESTAMP)")
		default:
			resolution = ChatAgentRunToolStepResolutionContinue
			updates.Status = proto_models.ChatAgentRunStatus_CHAT_AGENT_RUN_STATUS_RUNNING
			updates.StatusReason = proto_models.ChatAgentRunStatusReason_CHAT_AGENT_RUN_STATUS_REASON_UNSPECIFIED
			updates.StatusReasonCode = nil
			updates.CompletedAt = gorm.Expr("NULL")
		}
		result := tx.Model(&common_persistence_models.ChatAgentRun{}).
			Where("id = ? AND status NOT IN ?", runID, TerminalChatAgentRunStatuses()).
			Select("status", "status_reason", "status_reason_code", "completed_at", "updated_at").
			Updates(updates)
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected != 1 {
			return ErrChatAgentRunDriveLeaseLost
		}
		if resolution == ChatAgentRunToolStepResolutionCompleted {
			// A denied approval ends the run without an answer: a delegated child must tell its
			// parent, and a parent must drain any wake it deferred.
			return h.settleAgentRunTerminalTx(txCtx, tx, runID)
		}
		return nil
	})
	if err != nil {
		if errors.Is(err, ErrChatAgentRunDriveLeaseLost) || errors.Is(err, ErrChatAgentRunStepSealInvalid) {
			return ChatAgentRunToolStepResolutionUnspecified, err
		}
		return ChatAgentRunToolStepResolutionUnspecified, chatPersistenceDatabaseError(err,
			zap.String(logkeys.Stage, "reconcile_sealed_chat_agent_run_tool_step"),
			zap.Uint64(appLogger.KeyAgentRunID, runID),
			zap.Uint32(appLogger.KeyAgentStepIndex, stepIndex),
			zap.Bool(appLogger.KeyResumeOnDenial, resumeOnDenial))
	}
	return resolution, nil
}

// FailChatAgentRunWithNotice atomically terminalizes a nonterminal run as FAILED, closes any
// dangling assistant streams, and inserts the generic visible SYSTEM_EVENT. It is safe inside an
// existing transaction-bearing context (for example after a token-fenced tool lease release); the
// run-row lock makes exactly one caller create the notice. reason is the structured cause recorded
// on the run; the visible notice stays generic.
func (h *CommonPersistenceHandler[U]) FailChatAgentRunWithNotice(ctx context.Context, runID uint64, reason proto_models.ChatAgentRunStatusReason) (bool, uint64, error) {
	return h.TerminalizeChatAgentRunWithNotice(ctx, runID, nil, proto_models.ChatAgentRunStatus_CHAT_AGENT_RUN_STATUS_FAILED, reason)
}

// TerminalizeChatAgentRunWithNotice is the general form of FailChatAgentRunWithNotice: it moves a
// nonterminal run to FAILED or MODEL_STEP_LIMIT_EXCEEDED, closes dangling streams, and writes the
// visible notice for that ending in the same transaction, so step-limit and work-budget stops get
// the same exactly-once notice as failures. fromStatuses narrows the source states (empty means any
// nonterminal state). A run that is already terminal, or not in fromStatuses, returns false with no
// writes. When ctx carries a run-drive lease for this run, the transition is fenced by it. The
// returned message id is the generic SYSTEM_EVENT, or the delegation notice for a delegated child
// run (which gets no SYSTEM_EVENT: its notice is a broadcast message from the child bot).
func (h *CommonPersistenceHandler[U]) TerminalizeChatAgentRunWithNotice(
	ctx context.Context,
	runID uint64,
	fromStatuses []proto_models.ChatAgentRunStatus,
	toStatus proto_models.ChatAgentRunStatus,
	reason proto_models.ChatAgentRunStatusReason,
) (bool, uint64, error) {
	if runID == 0 {
		return false, 0, ErrChatAgentRunNotFound
	}
	if toStatus != proto_models.ChatAgentRunStatus_CHAT_AGENT_RUN_STATUS_FAILED &&
		toStatus != proto_models.ChatAgentRunStatus_CHAT_AGENT_RUN_STATUS_MODEL_STEP_LIMIT_EXCEEDED {
		return false, 0, appLogger.InvariantError("chat_agent_run_notice_status_is_failure",
			errors.Join(ErrDatabaseError, errors.New("terminalize with notice requires FAILED or MODEL_STEP_LIMIT_EXCEEDED")),
			zap.String(logkeys.Stage, "terminalize_chat_agent_run_with_notice"),
			zap.Uint64(appLogger.KeyAgentRunID, runID),
			zap.String(logkeys.Expected, "FAILED or MODEL_STEP_LIMIT_EXCEEDED"),
			zap.Stringer(logkeys.Actual, toStatus))
	}
	terminalized := false
	var messageID uint64
	err := h.transaction(ctx, func(txCtx context.Context, tx *gorm.DB) error {
		if lease, ok := common_context.GetChatAgentRunDriveLease(ctx); ok {
			if lease.RunID != runID {
				return ErrChatAgentRunDriveLeaseLost
			}
			if err := h.assertChatAgentRunDriveLeaseWithDB(ctx, tx, runID); err != nil {
				return err
			}
		}
		var run common_persistence_models.ChatAgentRun
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("id = ?", runID).Take(&run).Error; err != nil {
			return err
		}
		if IsTerminalChatAgentRunStatus(run.Status) {
			return nil
		}
		if len(fromStatuses) > 0 && !slicesContainsStatus(fromStatuses, run.Status) {
			return nil
		}
		var err error
		messageID, err = h.terminalizeChatAgentRunWithNoticeTx(txCtx, tx, &run, toStatus, reason)
		if err == nil {
			terminalized = true
		}
		return err
	})
	if err != nil {
		if errors.Is(err, ErrChatAgentRunDriveLeaseLost) {
			return false, 0, err
		}
		return false, 0, chatPersistenceDatabaseError(err,
			zap.String(logkeys.Stage, "terminalize_chat_agent_run_with_notice"),
			zap.Uint64(appLogger.KeyAgentRunID, runID),
			zap.Stringer(appLogger.KeyTargetStatus, toStatus))
	}
	return terminalized, messageID, nil
}

func (h *CommonPersistenceHandler[U]) failChatAgentRunWithNoticeTx(ctx context.Context, tx *gorm.DB, run *common_persistence_models.ChatAgentRun, reason proto_models.ChatAgentRunStatusReason) (uint64, error) {
	return h.terminalizeChatAgentRunWithNoticeTx(ctx, tx, run, proto_models.ChatAgentRunStatus_CHAT_AGENT_RUN_STATUS_FAILED, reason)
}

// lockChatAgentRunChannelTx takes the run's channel lock (lockChatChannelForAgentInputTx) before a
// run's ending writes its messages. A writer that locks one of those messages locks its channel
// first (EditChatMessage), and the ending's settlement takes the channel anyway, so taking it up
// front keeps the order run, then channel, then message. A missing channel locks nothing.
func lockChatAgentRunChannelTx(tx *gorm.DB, channelID uint64) error {
	if _, err := lockChatChannelForAgentInputTx(tx, channelID); err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
		return err
	}
	return nil
}

// failChatAgentRunStreamsTx marks the run's dangling assistant streams FAILED. A run leaving the
// active set can no longer finish them: its drive, if any, loses the lease that writes the ending.
// It returns the rows it failed with the fields a stream-failed event needs: id, channel, thread
// and visibility.
func failChatAgentRunStreamsTx(tx *gorm.DB, runID uint64) ([]*common_persistence_models.ChatMessage, error) {
	var failed []*common_persistence_models.ChatMessage
	err := tx.Model(&failed).
		Clauses(clause.Returning{Columns: []clause.Column{{Name: "id"}, {Name: "channel_id"}, {Name: "parent_message_id"}, {Name: "hidden_from_users"}}}).
		Where("agent_run_id = ? AND kind = ? AND stream_state = ?", runID,
			proto_models.MessageKind_MESSAGE_KIND_STANDARD,
			proto_models.MessageStreamState_MESSAGE_STREAM_STATE_STREAMING).
		Select("stream_state", "updated_at").
		Updates(struct {
			StreamState proto_models.MessageStreamState `gorm:"column:stream_state"`
			UpdatedAt   clause.Expr                     `gorm:"column:updated_at;type:timestamptz;autoUpdateTime:false"`
		}{
			StreamState: proto_models.MessageStreamState_MESSAGE_STREAM_STATE_FAILED,
			UpdatedAt:   gorm.Expr("CURRENT_TIMESTAMP"),
		}).Error
	return failed, err
}

// terminalizeChatAgentRunWithNoticeTx is the shared body of every failure-shaped terminal write:
// the channel is locked, dangling streams are marked FAILED, the visible notice is inserted, the
// run row moves to status, and the delegation settlement runs. The caller holds the run row lock.
// A delegated child run skips the generic SYSTEM_EVENT: its ending is announced by the broadcast
// notice the settlement posts as the child bot, and that notice id is returned instead.
func (h *CommonPersistenceHandler[U]) terminalizeChatAgentRunWithNoticeTx(
	ctx context.Context,
	tx *gorm.DB,
	run *common_persistence_models.ChatAgentRun,
	status proto_models.ChatAgentRunStatus,
	reason proto_models.ChatAgentRunStatusReason,
) (uint64, error) {
	if tx == nil || run == nil || run.ID == 0 {
		return 0, ErrChatAgentRunNotFound
	}
	if err := lockChatAgentRunChannelTx(tx, run.ChannelID); err != nil {
		return 0, err
	}
	if _, err := failChatAgentRunStreamsTx(tx, run.ID); err != nil {
		return 0, err
	}
	var session *common_persistence_models.ChatAgentSession
	if sessionID := common_util.Deref(run.AgentSessionID); sessionID != 0 {
		loaded, err := loadChatAgentSessionTx(tx, sessionID)
		if err != nil {
			return 0, err
		}
		session = loaded
	}
	var noticeID uint64
	if !session.IsDelegatedChild() {
		content := ChatAgentRunNoticeContent(status, reason)
		exclude, hidden := true, false
		runID := run.ID
		var parentMessageID *uint64
		if run.ParentMessageID != 0 {
			parent := run.ParentMessageID
			parentMessageID = &parent
		}
		notice := &common_persistence_models.ChatMessage{
			ChannelID:            run.ChannelID,
			SenderID:             run.BotUserID,
			Content:              content,
			ContentType:          proto_models.MessageContentType_MESSAGE_CONTENT_TYPE_TEXT,
			ParentMessageID:      parentMessageID,
			Kind:                 proto_models.MessageKind_MESSAGE_KIND_SYSTEM_EVENT,
			Role:                 proto_models.MessageRole_MESSAGE_ROLE_SYSTEM,
			ExcludeFromAIContext: &exclude,
			HiddenFromUsers:      &hidden,
			AgentRunID:           &runID,
		}
		if err := insertAgentRunNoticeTx(tx, notice); err != nil {
			return 0, err
		}
		noticeID = notice.ID
	}
	result := tx.Model(run).
		Clauses(clause.Returning{}).
		Where("id = ? AND status NOT IN ?", run.ID, TerminalChatAgentRunStatuses()).
		Select("status", "status_reason", "status_reason_code", "completed_at", "updated_at").
		Updates(struct {
			Status           proto_models.ChatAgentRunStatus       `gorm:"column:status"`
			StatusReason     proto_models.ChatAgentRunStatusReason `gorm:"column:status_reason"`
			StatusReasonCode *string                               `gorm:"column:status_reason_code"`
			CompletedAt      clause.Expr                           `gorm:"column:completed_at;type:timestamptz"`
			UpdatedAt        clause.Expr                           `gorm:"column:updated_at;type:timestamptz;autoUpdateTime:false"`
		}{
			Status:           status,
			StatusReason:     reason,
			StatusReasonCode: nil,
			CompletedAt:      gorm.Expr("COALESCE(completed_at, CURRENT_TIMESTAMP)"),
			UpdatedAt:        gorm.Expr("CURRENT_TIMESTAMP"),
		})
	if result.Error != nil {
		return 0, result.Error
	}
	if result.RowsAffected != 1 {
		return 0, ErrChatAgentRunDriveLeaseLost
	}
	// The settlement reuses the session read above; a delegated child's settlement returns the
	// notice it posted, which is this ending's visible notice.
	childNoticeID, err := h.settleLoadedAgentRunTerminalTx(ctx, tx, run, session)
	if err != nil {
		return 0, err
	}
	if session.IsDelegatedChild() {
		noticeID = childNoticeID
	}
	return noticeID, nil
}

// AssertChatAgentRunDriveLease locks and validates the contextual drive token using the database
// clock. Call it inside the same transaction as a run-generated durable write to fence an owner that
// was canceled late or continued after lease expiry.
func (h *CommonPersistenceHandler[U]) AssertChatAgentRunDriveLease(ctx context.Context, runID uint64) error {
	return h.assertChatAgentRunDriveLeaseWithDB(ctx, h.getDBFromContext(ctx), runID)
}

func (h *CommonPersistenceHandler[U]) assertChatAgentRunDriveLeaseWithDB(ctx context.Context, db *gorm.DB, runID uint64) error {
	_, err := h.lockLeasedChatAgentRunTx(ctx, db, runID, "id")
	return err
}

// lockLeasedChatAgentRunTx is assertChatAgentRunDriveLeaseWithDB returning the locked run, read
// with the given columns, so a caller that needs the run's fields does not read it twice.
func (h *CommonPersistenceHandler[U]) lockLeasedChatAgentRunTx(ctx context.Context, db *gorm.DB, runID uint64, columns ...string) (*common_persistence_models.ChatAgentRun, error) {
	lease, ok := common_context.GetChatAgentRunDriveLease(ctx)
	if !ok || lease.RunID != runID || db == nil {
		if appLogger.DebugEnabled(h.logger) {
			appLogger.For(ctx, h.logger, loggerName).Debug("chat agent run drive lease is unavailable or mismatched",
				zap.Uint64(appLogger.KeyAgentRunID, runID))
		}
		return nil, ErrChatAgentRunDriveLeaseLost
	}
	var run common_persistence_models.ChatAgentRun
	err := db.
		Clauses(clause.Locking{Strength: "UPDATE"}).
		Select(columns).
		Where("id = ? AND drive_lease_owner = ?", runID, lease.Owner).
		Where("drive_lease_expires_at > CURRENT_TIMESTAMP").
		Where("status NOT IN ?", TerminalChatAgentRunStatuses()).
		Where("NOT " + chatAgentRunDriveFenceSQL(ctx, runID)).
		Take(&run).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			if appLogger.DebugEnabled(h.logger) {
				appLogger.For(ctx, h.logger, loggerName).Debug("chat agent run drive lease is no longer live",
					zap.Uint64(appLogger.KeyAgentRunID, runID))
			}
			return nil, ErrChatAgentRunDriveLeaseLost
		}
		return nil, chatPersistenceDatabaseError(err,
			zap.String(logkeys.Stage, "assert_chat_agent_run_drive_lease"),
			zap.Uint64(appLogger.KeyAgentRunID, runID))
	}
	return &run, nil
}

// SealChatAgentRunStep records the recovery boundary for one fully persisted model step. The seal
// is written only after validating the durable projection under the same live run-drive token.
// A final seal atomically completes the run so a crash cannot leave a finished assistant response
// RUNNING and cause it to be generated again. With steer, which the runtime sets for an IMMEDIATE
// run whose step limit leaves another step, a final seal is recorded as Steered instead when input
// waits for the run's unsuspended session and the next step can admit it under maxGenerations and
// the graph pool's generations (see ReserveChatAgentRunStepAndAdmit): the answer stands, the run
// stays RUNNING and unsettled, and its next step admits the input. It returns the disposition it
// recorded, and whether this call wrote the seal (false when the same seal was already recorded).
// consumedPending is the newest pending wake message the sealed generation represented (a child
// instruction or a broadcast result for a main-timeline parent); its watermark only moves forward.
func (h *CommonPersistenceHandler[U]) SealChatAgentRunStep(
	ctx context.Context,
	runID uint64,
	stepIndex uint32,
	disposition ChatAgentRunStepDisposition,
	toolCallCount uint32,
	maxGenerations uint64,
	steer bool,
	consumedPending uint64,
) (ChatAgentRunStepDisposition, bool, error) {
	if runID == 0 || !disposition.Requestable() {
		return 0, false, ErrChatAgentRunStepSealInvalid
	}
	if disposition != ChatAgentRunStepDispositionToolLoop && toolCallCount != 0 {
		return 0, false, ErrChatAgentRunStepSealInvalid
	}
	if disposition == ChatAgentRunStepDispositionToolLoop && toolCallCount == 0 {
		return 0, false, ErrChatAgentRunStepSealInvalid
	}

	sealed := false
	requested := disposition
	err := h.transaction(ctx, func(txCtx context.Context, tx *gorm.DB) error {
		run, err := h.lockLeasedChatAgentRunTx(ctx, tx, runID, "id", "status", "channel_id", "agent_session_id",
			"generation_count", "last_sealed_step_index", "last_sealed_step_disposition", "last_sealed_tool_call_count")
		if err != nil {
			return err
		}
		alreadySealed := false
		if run.LastSealedStepIndex != nil {
			recorded := ChatAgentRunStepDisposition(run.LastSealedStepDisposition)
			if *run.LastSealedStepIndex == stepIndex && run.LastSealedToolCallCount == toolCallCount &&
				(recorded == requested || requested == ChatAgentRunStepDispositionFinal && recorded == ChatAgentRunStepDispositionSteered) {
				alreadySealed = true
				disposition = recorded
			} else if *run.LastSealedStepIndex == ^uint32(0) || stepIndex != *run.LastSealedStepIndex+1 {
				return ErrChatAgentRunStepSealInvalid
			}
		} else if stepIndex != 0 {
			return ErrChatAgentRunStepSealInvalid
		}

		type stepCounts struct {
			StandardCount         int64
			ToolCallCount         int64
			AgentNoteCount        int64
			IncompleteStreamCount int64
			PendingApprovalCount  int64
		}
		var counts stepCounts
		messages := common_persistence_models.TableNameChatMessages
		if err := tx.Table(messages).
			Select(
				"COUNT(*) FILTER (WHERE kind = ?) AS standard_count, "+
					"COUNT(*) FILTER (WHERE kind = ?) AS tool_call_count, "+
					"COUNT(*) FILTER (WHERE kind = ?) AS agent_note_count, "+
					"COUNT(*) FILTER (WHERE kind = ? AND stream_state NOT IN (?, ?)) AS incomplete_stream_count",
				proto_models.MessageKind_MESSAGE_KIND_STANDARD,
				proto_models.MessageKind_MESSAGE_KIND_TOOL_CALL,
				proto_models.MessageKind_MESSAGE_KIND_AGENT_NOTE,
				proto_models.MessageKind_MESSAGE_KIND_STANDARD,
				proto_models.MessageStreamState_MESSAGE_STREAM_STATE_UNSPECIFIED,
				proto_models.MessageStreamState_MESSAGE_STREAM_STATE_COMPLETE,
			).
			Where("agent_run_id = ? AND agent_step_index = ? AND deleted_at IS NULL", runID, stepIndex).
			Scan(&counts).Error; err != nil {
			return err
		}
		if counts.IncompleteStreamCount != 0 {
			return ErrChatAgentRunStepSealInvalid
		}
		switch disposition {
		case ChatAgentRunStepDispositionFinal, ChatAgentRunStepDispositionSteered:
			if counts.StandardCount != 1 || counts.ToolCallCount != 0 {
				return ErrChatAgentRunStepSealInvalid
			}
		case ChatAgentRunStepDispositionContinue:
			if counts.StandardCount != 0 || counts.ToolCallCount != 0 || counts.AgentNoteCount == 0 {
				return ErrChatAgentRunStepSealInvalid
			}
		case ChatAgentRunStepDispositionToolLoop:
			if counts.ToolCallCount != int64(toolCallCount) || counts.StandardCount > 1 {
				return ErrChatAgentRunStepSealInvalid
			}
			toolCalls := common_persistence_models.TableNameChatMessageToolCalls
			if err := tx.Table(toolCalls+" seal_calls").
				Joins("JOIN "+messages+" seal_messages ON seal_messages.id = seal_calls.message_id").
				Where("seal_calls.agent_run_id = ? AND seal_messages.agent_step_index = ? AND seal_calls.kind = ? AND seal_calls.approval_status = ?",
					runID, stepIndex, proto_models.MessageKind_MESSAGE_KIND_TOOL_CALL,
					proto_models.ToolCallApprovalStatus_TOOL_CALL_APPROVAL_STATUS_PENDING).
				Count(&counts.PendingApprovalCount).Error; err != nil {
				return err
			}
		}
		if alreadySealed {
			return nil
		}
		var session *common_persistence_models.ChatAgentSession
		if disposition == ChatAgentRunStepDispositionFinal {
			var steered bool
			if session, steered, err = steerChatAgentRunFinalAnswerTx(tx, run, maxGenerations, steer); err != nil {
				return err
			}
			if steered {
				disposition = ChatAgentRunStepDispositionSteered
			}
		}

		updates := struct {
			StreamRecoveryCount       uint32
			LastSealedStepIndex       *uint32
			LastSealedStepDisposition int16
			LastSealedToolCallCount   uint32
			ConsumedPendingMessageID  clause.Expr `gorm:"type:bigint"`
			Status                    proto_models.ChatAgentRunStatus
			StatusReason              proto_models.ChatAgentRunStatusReason
			StatusReasonCode          *string
			CompletedAt               clause.Expr `gorm:"type:timestamptz"`
			UpdatedAt                 clause.Expr `gorm:"type:timestamptz;autoUpdateTime:false"`
		}{LastSealedStepIndex: &stepIndex, LastSealedStepDisposition: int16(disposition),
			LastSealedToolCallCount: toolCallCount, UpdatedAt: gorm.Expr("CURRENT_TIMESTAMP")}
		columns := []string{"stream_recovery_count", "last_sealed_step_index", "last_sealed_step_disposition", "last_sealed_tool_call_count", "updated_at"}
		if consumedPending != 0 {
			updates.ConsumedPendingMessageID = gorm.Expr("GREATEST(consumed_pending_message_id, ?)", consumedPending)
			columns = append(columns, "consumed_pending_message_id")
		}
		// A seal that moves the status also resets the reason: neither an ordinary completion nor an
		// approval wait inherits the cause of an earlier RETRYABLE episode.
		if disposition == ChatAgentRunStepDispositionFinal {
			updates.Status = proto_models.ChatAgentRunStatus_CHAT_AGENT_RUN_STATUS_COMPLETED
			updates.StatusReason = proto_models.ChatAgentRunStatusReason_CHAT_AGENT_RUN_STATUS_REASON_UNSPECIFIED
			updates.StatusReasonCode = nil
			updates.CompletedAt = gorm.Expr("COALESCE(completed_at, CURRENT_TIMESTAMP)")
			columns = append(columns, "status", "status_reason", "status_reason_code", "completed_at")
		}
		if disposition == ChatAgentRunStepDispositionToolLoop && counts.PendingApprovalCount > 0 {
			updates.Status = proto_models.ChatAgentRunStatus_CHAT_AGENT_RUN_STATUS_WAITING_FOR_APPROVAL
			updates.StatusReason = proto_models.ChatAgentRunStatusReason_CHAT_AGENT_RUN_STATUS_REASON_UNSPECIFIED
			updates.StatusReasonCode = nil
			columns = append(columns, "status", "status_reason", "status_reason_code")
		}
		result := tx.Model(&common_persistence_models.ChatAgentRun{}).
			Where("id = ?", runID).
			Select(columns).
			Updates(updates)
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected != 1 {
			return ErrChatAgentRunStepSealInvalid
		}
		sealed = true
		if disposition == ChatAgentRunStepDispositionFinal {
			// The final seal is the run's normal ending: waiting input drains into a follow-up, a
			// delegated child broadcasts its closing message and wakes its parent, and a parent
			// drains any deferred wake.
			_, err := h.settleAgentRunTerminalWithSessionTx(txCtx, tx, runID, session)
			return err
		}
		return nil
	})
	if err != nil {
		if errors.Is(err, ErrChatAgentRunDriveLeaseLost) || errors.Is(err, ErrChatAgentRunStepSealInvalid) {
			return 0, false, err
		}
		return 0, false, chatPersistenceDatabaseError(err,
			zap.String(logkeys.Stage, "seal_chat_agent_run_step"),
			zap.Uint64(appLogger.KeyAgentRunID, runID),
			zap.Uint32(appLogger.KeyAgentStepIndex, stepIndex),
			zap.Uint8(appLogger.KeyDisposition, uint8(requested)),
			zap.Uint32(appLogger.KeyToolCallCount, toolCallCount))
	}
	return disposition, sealed, nil
}

// GetChatAgentRunStepArtifactState reads the full run-tagged transcript so recovery is not fooled
// by context compaction. The latest durable seal must exactly cover MaxStepIndex before generation
// may advance.
func (h *CommonPersistenceHandler[U]) GetChatAgentRunStepArtifactState(ctx context.Context, runID uint64) (*ChatAgentRunStepArtifactState, error) {
	if runID == 0 {
		return nil, ErrChatAgentRunNotFound
	}
	type aggregate struct {
		ArtifactCount         int64
		MaxStepIndex          *uint32
		IncompleteStreamCount int64
	}
	var row aggregate
	err := h.getDBFromContext(ctx).
		Table(common_persistence_models.TableNameChatMessages).
		Select(
			"COUNT(*) AS artifact_count, MAX(agent_step_index) AS max_step_index, "+
				"COUNT(*) FILTER (WHERE kind = ? AND stream_state NOT IN (?, ?)) AS incomplete_stream_count",
			proto_models.MessageKind_MESSAGE_KIND_STANDARD,
			proto_models.MessageStreamState_MESSAGE_STREAM_STATE_UNSPECIFIED,
			proto_models.MessageStreamState_MESSAGE_STREAM_STATE_COMPLETE,
		).
		Where("agent_run_id = ? AND agent_step_index IS NOT NULL AND deleted_at IS NULL", runID).
		Scan(&row).Error
	if err != nil {
		return nil, chatPersistenceDatabaseError(err,
			zap.String(logkeys.Stage, "inspect_chat_agent_run_step_artifacts"),
			zap.Uint64(appLogger.KeyAgentRunID, runID))
	}
	state := &ChatAgentRunStepArtifactState{
		HasArtifacts:        row.ArtifactCount > 0,
		HasIncompleteStream: row.IncompleteStreamCount > 0,
	}
	if row.MaxStepIndex != nil {
		state.MaxStepIndex = *row.MaxStepIndex
	}
	return state, nil
}

// FailIncompleteChatAgentRunDrive atomically terminalizes a run whose transcript contains model
// artifacts beyond its last durable step seal. Any dangling STREAMING assistant rows are marked
// FAILED in the same transaction, leaving explicit durable evidence instead of silently skipping
// the interrupted step on recovery.
func (h *CommonPersistenceHandler[U]) FailIncompleteChatAgentRunDrive(ctx context.Context, runID uint64) (bool, error) {
	if runID == 0 {
		return false, ErrChatAgentRunNotFound
	}
	failed := false
	err := h.transaction(ctx, func(txCtx context.Context, tx *gorm.DB) error {
		if err := h.assertChatAgentRunDriveLeaseWithDB(ctx, tx, runID); err != nil {
			return err
		}
		var run common_persistence_models.ChatAgentRun
		if err := tx.Where("id = ?", runID).Take(&run).Error; err != nil {
			return err
		}
		if IsTerminalChatAgentRunStatus(run.Status) {
			return nil
		}
		_, err := h.failChatAgentRunWithNoticeTx(txCtx, tx, &run, proto_models.ChatAgentRunStatusReason_CHAT_AGENT_RUN_STATUS_REASON_INCOMPLETE_STEP)
		failed = err == nil
		return err
	})
	if err != nil {
		if errors.Is(err, ErrChatAgentRunDriveLeaseLost) {
			return false, err
		}
		return false, chatPersistenceDatabaseError(err,
			zap.String(logkeys.Stage, "terminalize_incomplete_chat_agent_run_drive"),
			zap.Uint64(appLogger.KeyAgentRunID, runID))
	}
	return failed, nil
}

func slicesContainsStatus(statuses []proto_models.ChatAgentRunStatus, target proto_models.ChatAgentRunStatus) bool {
	return slices.Contains(statuses, target)
}

func databaseLeaseDeadline(duration time.Duration) clause.Expr {
	return gorm.Expr("CURRENT_TIMESTAMP + (? * INTERVAL '1 microsecond')", duration.Microseconds())
}

// DrivableChatAgentRunStatuses are the statuses a drive may advance: RUNNING and RETRYABLE. Each
// call returns a fresh slice.
func DrivableChatAgentRunStatuses() []proto_models.ChatAgentRunStatus {
	return []proto_models.ChatAgentRunStatus{
		proto_models.ChatAgentRunStatus_CHAT_AGENT_RUN_STATUS_RUNNING,
		proto_models.ChatAgentRunStatus_CHAT_AGENT_RUN_STATUS_RETRYABLE,
	}
}

// TerminalChatAgentRunStatuses are the statuses in which no further work may execute for a run:
// COMPLETED, FAILED, CANCELLED, and MODEL_STEP_LIMIT_EXCEEDED. It matches
// IsTerminalChatAgentRunStatus. Each call returns a fresh slice.
func TerminalChatAgentRunStatuses() []proto_models.ChatAgentRunStatus {
	return []proto_models.ChatAgentRunStatus{
		proto_models.ChatAgentRunStatus_CHAT_AGENT_RUN_STATUS_COMPLETED,
		proto_models.ChatAgentRunStatus_CHAT_AGENT_RUN_STATUS_FAILED,
		proto_models.ChatAgentRunStatus_CHAT_AGENT_RUN_STATUS_CANCELLED,
		proto_models.ChatAgentRunStatus_CHAT_AGENT_RUN_STATUS_MODEL_STEP_LIMIT_EXCEEDED,
	}
}
