package common_persistence

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"slices"
	"strconv"
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
	"google.golang.org/protobuf/proto"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// toolCallExecutionUpdate carries concrete execution fields, including SQL expressions for
// atomic attempt increments and database-clock retry deadlines. Call sites select only the
// columns they change; UpdatedAt retains their explicitly supplied timestamp.
type toolCallExecutionUpdate struct {
	ExecutionState          proto_models.ToolCallExecutionState
	ExecutionLeaseOwner     *string
	ExecutionLeaseExpiresAt *time.Time
	ExecutionNotBefore      clause.Expr `gorm:"type:timestamptz"`
	ExecutionStartedAt      *time.Time
	ExecutionAttemptCount   clause.Expr `gorm:"type:integer"`
	ExecutedAt              *time.Time
	LastExecutionError      *string
	UpdatedAt               time.Time `gorm:"autoUpdateTime:false"`
}

type toolCallApprovalUpdate struct {
	ApprovalStatus     proto_models.ToolCallApprovalStatus
	ExecutionState     proto_models.ToolCallExecutionState
	ExecutionNotBefore *time.Time
	ResolvedAt         *time.Time
	ResolvedByUserID   *uint64
	ResolutionReason   *string
	UpdatedAt          time.Time `gorm:"autoUpdateTime:false"`
}

func toolMediaEntityID(runID uint64, toolCallID string, partIndex int) string {
	digest := sha256.Sum256([]byte(toolCallID))
	return fmt.Sprintf("run:%d:call:%s:part:%d", runID, hex.EncodeToString(digest[:]), partIndex)
}

func claimedToolMediaEntityID(agentRunID uint64, toolCallID, leaseOwner string, partIndex int) string {
	callDigest := sha256.Sum256([]byte(toolCallID))
	leaseDigest := sha256.Sum256([]byte(leaseOwner))
	return fmt.Sprintf("run:%d:call:%s:claim:%s:part:%d", agentRunID, hex.EncodeToString(callDigest[:]), hex.EncodeToString(leaseDigest[:]), partIndex)
}

func providerStateEntityIDForDrive(agentRunID uint64, stepIndex uint32, leaseOwner string) string {
	base := fmt.Sprintf("run:%d:step:%d", agentRunID, stepIndex)
	if strings.TrimSpace(leaseOwner) == "" {
		return base
	}
	digest := sha256.Sum256([]byte(leaseOwner))
	return base + ":drive:" + hex.EncodeToString(digest[:])
}

// defaultTranscriptScanBatchSize is the page size used by the unbounded transcript cursor scan.
const defaultTranscriptScanBatchSize = 200

// freshInlineToolCallRecoveryDelay is the durable handoff window for an inline TOOL_CALL that was
// persisted but never reached either its paired TOOL_RESULT or the live loop's explicit failure
// settlement. Normal inline execution owns a fresh row during this window; after it elapses the
// durable scanner may recover the row under the same at-least-once/idempotency contract used for an
// expired execution lease. Keep this at least as long as the scanner's execution lease window.
const freshInlineToolCallRecoveryDelay = 5 * time.Minute

const (
	// toolCallRetryBackoffBase is deliberately longer than a typical scanner tick: a transiently
	// failing call must leave room for other due rows before it is considered again.
	toolCallRetryBackoffBase = time.Minute
	toolCallRetryBackoffMax  = 5 * time.Minute
)

// toolCallRetryBackoff returns the exponential delay after a failed claimed attempt. attempt is
// the durable count after ClaimToolCall incremented it. Saturating before the shift avoids integer
// overflow for a permanently failing call while preserving a stable maximum retry cadence.
func toolCallRetryBackoff(attempt uint32) time.Duration {
	if attempt == 0 {
		attempt = 1
	}
	shift := min(attempt-1, 6)
	delay := toolCallRetryBackoffBase * time.Duration(uint64(1)<<shift)
	if delay > toolCallRetryBackoffMax {
		return toolCallRetryBackoffMax
	}
	return delay
}

// listFairClaimableToolCallRows returns a bounded interleave of never-attempted and retry/reclaim
// rows. Running two bounded index scans avoids a single old failing population monopolizing every
// batch, while also reserving progress for due retries during a sustained influx of fresh calls.
// Each population is ordered oldest-due first; when both are present and limit >= 2, each receives
// at least one slot and at most every other slot until the other population is exhausted.
func listFairClaimableToolCallRows(query *gorm.DB, limit int) ([]common_persistence_models.ChatMessageToolCall, error) {
	dueOrder := "COALESCE(execution_not_before, execution_lease_expires_at, created_at) ASC"
	var fresh []common_persistence_models.ChatMessageToolCall
	if err := query.Session(&gorm.Session{}).
		Where("execution_attempt_count = 0").
		Order(dueOrder).Order("message_id ASC").
		Limit(limit).Find(&fresh).Error; err != nil {
		return nil, err
	}
	var retries []common_persistence_models.ChatMessageToolCall
	if err := query.Session(&gorm.Session{}).
		Where("execution_attempt_count > 0").
		Order(dueOrder).Order("execution_attempt_count ASC").Order("message_id ASC").
		Limit(limit).Find(&retries).Error; err != nil {
		return nil, err
	}

	out := make([]common_persistence_models.ChatMessageToolCall, 0, min(limit, len(fresh)+len(retries)))
	// Start with the population whose oldest row has waited longer, then alternate. This retains
	// age priority without allowing that population to consume the whole batch.
	retryFirst := len(retries) > 0 && (len(fresh) == 0 || toolCallDueTime(retries[0]).Before(toolCallDueTime(fresh[0])))
	fi, ri := 0, 0
	for len(out) < limit && (fi < len(fresh) || ri < len(retries)) {
		appendRetry := func() bool {
			if ri >= len(retries) || len(out) >= limit {
				return false
			}
			out = append(out, retries[ri])
			ri++
			return true
		}
		appendFresh := func() bool {
			if fi >= len(fresh) || len(out) >= limit {
				return false
			}
			out = append(out, fresh[fi])
			fi++
			return true
		}
		if retryFirst {
			if !appendRetry() {
				appendFresh()
			}
			appendFresh()
		} else {
			if !appendFresh() {
				appendRetry()
			}
			appendRetry()
		}
	}
	return out, nil
}

func toolCallDueTime(row common_persistence_models.ChatMessageToolCall) time.Time {
	if row.ExecutionNotBefore != nil {
		return *row.ExecutionNotBefore
	}
	if row.ExecutionLeaseExpiresAt != nil {
		return *row.ExecutionLeaseExpiresAt
	}
	return row.CreatedAt
}

// claimedToolResultReconcileTimeout bounds the detached readback used only after a claimed
// TOOL_RESULT commit has an ambiguous outcome. The caller's context may already be cancelled (the
// common lost-commit-ACK shape), so reconciliation must not inherit its cancellation, but it also
// must not wait indefinitely on an unhealthy database.
const claimedToolResultReconcileTimeout = 5 * time.Second

// inlineApprovalStatuses are the approval statuses an INLINE (non-approval-required) TOOL_CALL can
// carry. UNSPECIFIED is treated as NOT_REQUIRED (Decision 2 enum note), so an inline call written
// without an explicit status still auto-advances to EXECUTED when its result lands. Approval-required
// statuses (PENDING/APPROVED/DENIED/EXPIRED) are deliberately excluded so the approval queue's
// EXECUTING lease and terminal states are never stomped by the inline-result advance (bug 1).
var inlineApprovalStatuses = []proto_models.ToolCallApprovalStatus{
	proto_models.ToolCallApprovalStatus_TOOL_CALL_APPROVAL_STATUS_UNSPECIFIED,
	proto_models.ToolCallApprovalStatus_TOOL_CALL_APPROVAL_STATUS_NOT_REQUIRED,
}

// isInlineApprovalStatus reports whether an approval status marks a call that the recording handler
// executes inline (no approval queue), so its paired result may advance it to EXECUTED. Derived from
// inlineApprovalStatuses so the SQL IN-list and this Go guard can never drift.
func isInlineApprovalStatus(status proto_models.ToolCallApprovalStatus) bool {
	return slices.Contains(inlineApprovalStatuses, status)
}

// errChatAssistantTurnRace signals that a concurrent drive of the same run already inserted this
// step's assistant STANDARD row (idx_chat_messages_run_step_assistant). CreateTranscriptMessage
// resolves it to the winning row as an idempotent no-op, mirroring the (run, tool_call_id) path.
var errChatAssistantTurnRace = errors.New("assistant turn already recorded for step")

// errChatProviderStateRace signals that another writer committed the same run/step provider-state
// row between the idempotency lookup and insert. The transaction is rolled back and the winner is
// compared outside the transaction.
var errChatProviderStateRace = errors.New("provider state already recorded for step")

// isAssistantTurnRow reports whether an input is a run-tagged STANDARD row — the shape guarded by
// the partial unique index idx_chat_messages_run_step_assistant.
func isAssistantTurnRow(in CreateTranscriptMessageInput) bool {
	return in.Kind == proto_models.MessageKind_MESSAGE_KIND_STANDARD &&
		in.AgentRunID != nil && *in.AgentRunID != 0 && in.AgentStepIndex != nil
}

// applyNonTerminalRunPredicate excludes tool-call rows whose owning ChatAgentRun is terminal: an
// approved-but-unexecuted tool call whose owning run was cancelled, failed, or completed must never
// be claimed and run (its side effects would fire for a dead turn).
// NOT EXISTS (rather than a positive join) keeps rows without a run row claimable, preserving
// behavior for legacy/manual writer rows that carry no agent_run_id.
func applyNonTerminalRunPredicate(q *gorm.DB) *gorm.DB {
	return q.Where(
		"NOT EXISTS (SELECT 1 FROM "+common_persistence_models.TableNameChatAgentRuns+
			" runs WHERE runs.id = "+common_persistence_models.TableNameChatMessageToolCalls+".agent_run_id AND (runs.status IN ? OR "+retiringChildRunSQL("runs", false)+"))",
		TerminalChatAgentRunStatuses())
}

// applySealedToolStepPredicate ensures no tool side effect can run from a partially persisted model
// response. The run-drive owner seals the complete TOOL_CALL batch before any live inline claim;
// approval and recovery scanners use the same guard. A crash after only a prefix therefore leaves
// every prefix row inert and recovery terminalizes the run instead of executing incomplete output.
func applySealedToolStepPredicate(q *gorm.DB) *gorm.DB {
	calls := common_persistence_models.TableNameChatMessageToolCalls
	messages := common_persistence_models.TableNameChatMessages
	runs := common_persistence_models.TableNameChatAgentRuns
	return q.Where(
		"EXISTS (SELECT 1 FROM "+runs+" sealed_runs JOIN "+messages+" sealed_messages ON sealed_messages.id = "+calls+".message_id "+
			"WHERE sealed_runs.id = "+calls+".agent_run_id "+
			"AND sealed_runs.last_sealed_step_index IS NOT NULL "+
			"AND sealed_runs.last_sealed_step_disposition = ? "+
			"AND sealed_messages.agent_step_index IS NOT NULL "+
			"AND sealed_runs.last_sealed_step_index = sealed_messages.agent_step_index)",
		ChatAgentRunStepDispositionToolLoop,
	)
}

// applyNoLiveRunDrivePredicate prevents the durable tool scanner from reclaiming a long-running
// inline execution while its owning run-drive heartbeat is still live. Once that drive dies or
// releases its lease, the ordinary per-tool expiry/handoff predicates make the call recoverable.
func applyNoLiveRunDrivePredicate(q *gorm.DB) *gorm.DB {
	calls := common_persistence_models.TableNameChatMessageToolCalls
	runs := common_persistence_models.TableNameChatAgentRuns
	return q.Where(
		"NOT EXISTS (SELECT 1 FROM " + runs + " live_runs WHERE live_runs.id = " + calls + ".agent_run_id " +
			"AND live_runs.drive_lease_owner IS NOT NULL AND live_runs.drive_lease_expires_at > CURRENT_TIMESTAMP)",
	)
}

// applyClaimableApprovedPredicate scopes a ChatMessageToolCall query to claimable approved work:
// an APPROVED TOOL_CALL on a non-terminal run that is either PENDING_EXECUTION or EXECUTING with
// an expired lease (a crashed worker's reclaimable row). It is the single source of the
// claimable-work predicate shared by the list scanner (ListClaimableApprovedToolCalls) and the
// atomic claim (ClaimApprovedToolCall), so the two can never drift. Callers add their own
// narrowing (channel_id / message_id) and action.
func applyClaimableApprovedPredicate(q *gorm.DB, now time.Time) *gorm.DB {
	return applyNoLiveRunDrivePredicate(applySealedToolStepPredicate(applyNonTerminalRunPredicate(q))).
		Where("kind = ? AND approval_status = ?", proto_models.MessageKind_MESSAGE_KIND_TOOL_CALL, proto_models.ToolCallApprovalStatus_TOOL_CALL_APPROVAL_STATUS_APPROVED).
		Where("((execution_state = ? AND (execution_not_before IS NULL OR execution_not_before <= CURRENT_TIMESTAMP)) OR (execution_state = ? AND execution_lease_expires_at IS NOT NULL AND execution_lease_expires_at <= ?))",
			proto_models.ToolCallExecutionState_TOOL_CALL_EXECUTION_STATE_PENDING_EXECUTION,
			proto_models.ToolCallExecutionState_TOOL_CALL_EXECUTION_STATE_EXECUTING,
			now)
}

// applyClaimableToolCallPredicate scopes a ChatMessageToolCall query to durable tool work owned by
// the execution scanner: approved calls; inline calls that already failed once on the live path;
// and fresh inline calls whose live owner did not settle them within the durable handoff window. In
// every case the owning run must remain non-terminal. A fresh inline PENDING_EXECUTION row is hidden
// from the scanner until freshInlineToolCallRecoveryDelay has elapsed, preventing the background
// backstop from racing normal immediate execution. Once a recovered inline row is EXECUTING, an
// expired lease is claimable even though the original fresh row had no last_execution_error.
func applyClaimableToolCallPredicate(q *gorm.DB, now time.Time) *gorm.DB {
	return applyNoLiveRunDrivePredicate(applyRecoverableToolCallPredicate(q, now))
}

// recoverableToolCallAtSQL is when the call row calls (an alias of the tool-call table, on a
// non-terminal run's sealed step) becomes recoverable under applyRecoverableToolCallPredicate, which
// it must match: its retry backoff, a fresh inline call's handoff window, or an EXECUTING call's
// lease expiry. NULL when it never becomes recoverable on its own: it awaits an approval decision,
// is settled, or is EXECUTING without a lease.
func recoverableToolCallAtSQL(calls string) string {
	approved := strconv.Itoa(int(proto_models.ToolCallApprovalStatus_TOOL_CALL_APPROVAL_STATUS_APPROVED))
	recoverable := []string{approved}
	for _, status := range inlineApprovalStatuses {
		recoverable = append(recoverable, strconv.Itoa(int(status)))
	}
	return "CASE WHEN " + calls + ".approval_status NOT IN (" + strings.Join(recoverable, ", ") + ") THEN NULL" +
		" WHEN " + calls + ".execution_state = " + strconv.Itoa(int(proto_models.ToolCallExecutionState_TOOL_CALL_EXECUTION_STATE_EXECUTING)) +
		" THEN " + calls + ".execution_lease_expires_at" +
		" WHEN " + calls + ".execution_state = " + strconv.Itoa(int(proto_models.ToolCallExecutionState_TOOL_CALL_EXECUTION_STATE_PENDING_EXECUTION)) +
		" THEN GREATEST(COALESCE(" + calls + ".execution_not_before, CURRENT_TIMESTAMP), CASE WHEN " + calls + ".approval_status <> " + approved +
		" AND " + calls + ".last_execution_error IS NULL THEN " + calls + ".created_at + INTERVAL '" +
		strconv.FormatInt(freshInlineToolCallRecoveryDelay.Microseconds(), 10) + " microseconds' ELSE CURRENT_TIMESTAMP END) END"
}

// applyRecoverableToolCallPredicate contains tool readiness independent of drive ownership.
// Callers must additionally exclude live drives or validate and lock the owning drive.
func applyRecoverableToolCallPredicate(q *gorm.DB, now time.Time) *gorm.DB {
	pending := proto_models.ToolCallExecutionState_TOOL_CALL_EXECUTION_STATE_PENDING_EXECUTION
	executing := proto_models.ToolCallExecutionState_TOOL_CALL_EXECUTION_STATE_EXECUTING
	claimableState := "((execution_state = ? AND (execution_not_before IS NULL OR execution_not_before <= CURRENT_TIMESTAMP)) OR (execution_state = ? AND execution_lease_expires_at IS NOT NULL AND execution_lease_expires_at <= ?))"
	inlineClaimableState := "((execution_state = ? AND (execution_not_before IS NULL OR execution_not_before <= CURRENT_TIMESTAMP) AND (last_execution_error IS NOT NULL OR created_at <= ?)) OR (execution_state = ? AND execution_lease_expires_at IS NOT NULL AND execution_lease_expires_at <= ?))"
	return applySealedToolStepPredicate(applyNonTerminalRunPredicate(q)).
		Where("kind = ?", proto_models.MessageKind_MESSAGE_KIND_TOOL_CALL).
		Where(
			"((approval_status = ? AND "+claimableState+") OR (approval_status IN ? AND "+inlineClaimableState+"))",
			proto_models.ToolCallApprovalStatus_TOOL_CALL_APPROVAL_STATUS_APPROVED,
			pending, executing, now,
			inlineApprovalStatuses,
			pending, now.Add(-freshInlineToolCallRecoveryDelay), executing, now,
		)
}

// ============================================================================
// CHAT: TRANSCRIPT MESSAGE WRITE (internal writer primitive)
// ============================================================================

// CreateTranscriptMessage inserts one non-STANDARD agent transcript row (and, for tool rows, its
// 1:1 ChatMessageToolCall side row) in a single idempotent transaction. It backs the internal
// transcript writer (RecordToolCall / RecordToolResult / PostSystemEvent); the public STANDARD
// send path stays SendChatMessage.
//
// Idempotency & constraints (Decision 2):
//   - For a tool row with (AgentRunID, ToolCallID) set, a retry with a matching immutable payload
//     returns the existing message id with created=false (no duplicate); a different payload for
//     the same (run, tool_call_id, kind) is a conflict (ErrChatToolCallConflict).
//   - A TOOL_RESULT must reference an existing TOOL_CALL for the same (AgentRunID, ToolCallID);
//     an orphan is rejected (ErrChatToolResultOrphan). The result's AgentStepIndex is copied from
//     the paired TOOL_CALL so it reconstructs in the same step.
//   - last_message_at / last_message_preview are updated only for visible, counting kinds; hidden
//     rows and TOOL_CALL/TOOL_RESULT/SYSTEM_EVENT/COMPACTION_SUMMARY/AGENT_NOTE/PROVIDER_STATE never
//     overwrite the preview (the preview invariant).
//
// Run-context validation & state transitions (Decision 5 / Decision 6):
//   - When the row carries an AgentRunID, the referenced ChatAgentRun must belong to the same
//     channel, thread (parent), and bot-sender scope; a cross-context reference is rejected
//     (ErrChatTranscriptRunMismatch) before any row is written.
//   - Inserting a TOOL_RESULT for an INLINE (NOT_REQUIRED) TOOL_CALL advances that call
//     PENDING_EXECUTION -> EXECUTED in the same transaction, so the whole-step resume gate
//     (CountUnresolvedToolCallsInStep) can reach 0. Approval-required calls are left to the approval
//     queue / executor (the gate excludes anything not NOT_REQUIRED+PENDING_EXECUTION).
//   - Approval-required TOOL_CALL rows do not pause the run one-by-one. The fenced whole-step seal
//     transitions the run only after the complete model-emitted batch is durable, so a crash after
//     a prefix cannot strand an unsealed WAITING_FOR_APPROVAL run.
//
// Parameters:
//   - ctx: Request context used for cancellation, deadlines, and tracing.
//   - in: The transcript message + optional side-row fields.
//
// Returns:
//   - result1: The message id (existing on idempotent retry).
//   - result2: true if a new row was inserted; false on an idempotent no-op retry.
//   - result3: ErrChatToolResultOrphan / ErrChatToolCallConflict / ErrDatabaseError.
func (h *CommonPersistenceHandler[U]) CreateTranscriptMessage(
	ctx context.Context,
	in CreateTranscriptMessageInput,
) (uint64, bool, error) {
	return h.createTranscriptMessage(ctx, in, nil)
}

type claimedToolResultCommit struct {
	toolCallMessageID uint64
	leaseOwner        string
	executedAt        time.Time
}

// CommitClaimedToolCallResult is the fenced completion boundary for every claimed tool execution.
// It locks and verifies the paired TOOL_CALL's current lease, writes/idempotently validates the
// TOOL_RESULT, and marks the call EXECUTED in one database transaction. A stale worker therefore
// cannot bind its payload to the run before discovering that another worker owns the call.
func (h *CommonPersistenceHandler[U]) CommitClaimedToolCallResult(
	ctx context.Context,
	toolCallMessageID uint64,
	leaseOwner string,
	executedAt time.Time,
	in CreateTranscriptMessageInput,
) (messageID uint64, created bool, committed bool, err error) {
	if toolCallMessageID == 0 || leaseOwner == "" {
		return 0, false, false, ErrChatToolCallLeaseLost
	}
	messageID, created, err = h.createTranscriptMessage(ctx, in, &claimedToolResultCommit{
		toolCallMessageID: toolCallMessageID,
		leaseOwner:        leaseOwner,
		executedAt:        executedAt,
	})
	if err == nil {
		return messageID, created, true, nil
	}
	// A transaction commit can become durable while its acknowledgement is lost. It has the same
	// surface shape as an ordinary database error; a retry after the durable EXECUTED transition has
	// also cleared the lease and therefore looks like lease loss. Reconcile both shapes against the
	// primary database on a detached, bounded context. Success is reported only when one joined
	// snapshot proves the exact paired call identity, its terminal EXECUTED marker, and an identical
	// persisted TOOL_RESULT payload/type. A stale worker with a different result remains fenced out.
	if errors.Is(err, ErrDatabaseError) || errors.Is(err, ErrChatToolCallLeaseLost) {
		if durableID, durable, reconcileErr := h.reconcileCommittedClaimedToolCallResult(ctx, toolCallMessageID, in); reconcileErr == nil && durable {
			return durableID, false, true, nil
		}
	}
	if errors.Is(err, ErrChatToolCallLeaseLost) {
		return 0, false, false, nil
	}
	return 0, false, false, err
}

// reconcileCommittedClaimedToolCallResult resolves the only safe success interpretation after an
// ambiguous claimed-result commit. It deliberately bypasses any transaction carried by ctx: that
// transaction may be the poisoned/committed transaction whose acknowledgement was lost. The joined
// query makes the call state and result identity one read snapshot; byte-for-byte payload equality
// prevents a stale claimant from accepting another attempt's different output.
func (h *CommonPersistenceHandler[U]) reconcileCommittedClaimedToolCallResult(
	ctx context.Context,
	toolCallMessageID uint64,
	in CreateTranscriptMessageInput,
) (uint64, bool, error) {
	if h == nil || h.db == nil || toolCallMessageID == 0 || in.AgentRunID == nil || *in.AgentRunID == 0 ||
		in.ToolCallID == nil || *in.ToolCallID == "" {
		return 0, false, nil
	}

	reconcileCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), claimedToolResultReconcileTimeout)
	defer cancel()
	table := common_persistence_models.TableNameChatMessageToolCalls
	type durableResult struct {
		MessageID    uint64
		ModelPayload []byte
		PayloadType  proto_models.MessageModelPayloadType
	}
	var durable durableResult
	err := h.db.WithContext(reconcileCtx).
		Table(table+" AS result").
		Select("result.message_id, result.model_payload, result.payload_type").
		Joins("JOIN "+table+" AS call ON call.message_id = ?", toolCallMessageID).
		Where("call.kind = ? AND call.execution_state = ? AND call.executed_at IS NOT NULL",
			proto_models.MessageKind_MESSAGE_KIND_TOOL_CALL,
			proto_models.ToolCallExecutionState_TOOL_CALL_EXECUTION_STATE_EXECUTED).
		Where("call.agent_run_id = ? AND call.tool_call_id = ?", *in.AgentRunID, *in.ToolCallID).
		Where("result.agent_run_id = ? AND result.tool_call_id = ? AND result.kind = ?",
			*in.AgentRunID, *in.ToolCallID, proto_models.MessageKind_MESSAGE_KIND_TOOL_RESULT).
		Take(&durable).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}
	if durable.MessageID == 0 || durable.PayloadType != in.PayloadType || !bytesEqual(durable.ModelPayload, in.ModelPayload) {
		return 0, false, nil
	}
	return durable.MessageID, true, nil
}

func (h *CommonPersistenceHandler[U]) createTranscriptMessage(
	ctx context.Context,
	in CreateTranscriptMessageInput,
	claim *claimedToolResultCommit,
) (uint64, bool, error) {
	if in.ChannelID == 0 {
		return 0, false, appLogger.InvariantError("transcript_message_has_channel",
			errors.New("channel id is required"),
			zap.String(logkeys.Stage, "create_transcript_message"),
			zap.String(logkeys.Expected, "channel_id > 0"),
			zap.Uint64(logkeys.Actual, in.ChannelID))
	}
	if err := validateTranscriptToolResultEmbeds(in); err != nil {
		return 0, false, err
	}
	if err := validateTranscriptSideRowPayload(in); err != nil {
		return 0, false, err
	}
	if claim != nil && in.Kind != proto_models.MessageKind_MESSAGE_KIND_TOOL_RESULT {
		return 0, false, ErrChatTranscriptPayloadInvalid
	}
	if in.BroadcastToChannel && common_util.Deref(in.ParentMessageID) == 0 {
		return 0, false, ErrChatTranscriptBroadcastInvalid
	}
	db := h.getDBFromContext(ctx)

	isToolCall := in.Kind == proto_models.MessageKind_MESSAGE_KIND_TOOL_CALL
	isToolResult := in.Kind == proto_models.MessageKind_MESSAGE_KIND_TOOL_RESULT
	isProviderState := in.Kind == proto_models.MessageKind_MESSAGE_KIND_PROVIDER_STATE
	hasPairKey := in.AgentRunID != nil && in.ToolCallID != nil && *in.ToolCallID != ""

	var messageID uint64
	created := false
	idempotentExisting := false
	// pairedCall holds the TOOL_CALL side row a TOOL_RESULT answers (loaded in the isToolResult
	// block), so the inline-call execution-state advance (bug 1) can run after the result row is
	// inserted, in the same transaction.
	var pairedCall *common_persistence_models.ChatMessageToolCall

	err := db.Transaction(func(tx *gorm.DB) error {
		if claim != nil {
			if in.AgentRunID == nil || *in.AgentRunID == 0 {
				return ErrChatTranscriptPayloadInvalid
			}
			// Lock the run before the call, matching approval/failure transactions elsewhere in this
			// state machine. Besides avoiding an inverse run<->tool lock order, this gives cancellation
			// and completion one serialization point: whichever locks the run first wins.
			var liveRun common_persistence_models.ChatAgentRun
			if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
				Select("id").
				Where("id = ? AND status NOT IN ?", *in.AgentRunID, TerminalChatAgentRunStatuses()).
				Take(&liveRun).Error; err != nil {
				if errors.Is(err, gorm.ErrRecordNotFound) {
					return ErrChatToolCallLeaseLost
				}
				return err
			}

			// Lock before any attachment validation or result-row lookup/insert. This is both the
			// fencing check and the serialization point against lease reclaim. There is no precheck /
			// later-insert TOCTOU window: the same transaction holds the row lock through result insert
			// and EXECUTED transition.
			var owned common_persistence_models.ChatMessageToolCall
			if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
				Where("message_id = ? AND kind = ? AND execution_state = ? AND execution_lease_owner = ?",
					claim.toolCallMessageID,
					proto_models.MessageKind_MESSAGE_KIND_TOOL_CALL,
					proto_models.ToolCallExecutionState_TOOL_CALL_EXECUTION_STATE_EXECUTING,
					claim.leaseOwner).
				First(&owned).Error; err != nil {
				if errors.Is(err, gorm.ErrRecordNotFound) {
					return ErrChatToolCallLeaseLost
				}
				return err
			}
			if owned.AgentRunID == nil || *in.AgentRunID != *owned.AgentRunID ||
				in.ToolCallID == nil || owned.ToolCallID == nil || *in.ToolCallID != *owned.ToolCallID {
				return ErrChatTranscriptPayloadInvalid
			}
		}
		// Runtime-generated rows carry a durable run-drive token. Verify and row-lock that token in
		// this same transaction before any step row is inserted, so a stale replica that outlived
		// heartbeat cancellation cannot commit after lease expiry/takeover. Approval/scanner writes
		// intentionally carry no drive token and remain governed by their per-tool lease.
		if lease, ok := common_context.GetChatAgentRunDriveLease(ctx); ok {
			if in.AgentRunID == nil || *in.AgentRunID == 0 || lease.RunID != *in.AgentRunID {
				return ErrChatAgentRunDriveLeaseLost
			}
			txCtx := scontext.CopySRouterContext[uint64, U](ctx, ctx)
			txCtx = scontext.WithTransaction[uint64, U](txCtx, &GormTransactionWithHooks{Tx: tx})
			if err := h.AssertChatAgentRunDriveLease(txCtx, *in.AgentRunID); err != nil {
				return err
			}
			if in.AgentStepIndex != nil && isModelStepProjectionKind(in.Kind) {
				var sealed common_persistence_models.ChatAgentRun
				if err := tx.Select("last_sealed_step_index").Where("id = ?", *in.AgentRunID).Take(&sealed).Error; err != nil {
					return err
				}
				if sealed.LastSealedStepIndex != nil && *in.AgentStepIndex <= *sealed.LastSealedStepIndex {
					return ErrChatAgentRunStepSealInvalid
				}
			}
		}
		lockedInternalAttachments := make(map[uint64]common_persistence_models.Attachment, len(in.AttachmentIDs))
		if len(in.AttachmentIDs) > 0 {
			var internalAttachments []common_persistence_models.Attachment
			if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
				Select("id", "user_id", "object_type", "internal_only", "entity_id", "upload_status", "size", "file_hash", "original_file_name", "content_type", "deleted_at").
				Where("id IN ? AND internal_only = ?", in.AttachmentIDs, true).
				Find(&internalAttachments).Error; err != nil {
				return err
			}
			if len(internalAttachments) != len(in.AttachmentIDs) {
				return ErrChatTranscriptPayloadInvalid
			}
			for _, attachment := range internalAttachments {
				lockedInternalAttachments[attachment.ID] = attachment
			}
		}
		// Decision 5 authority: when the row is tagged with a run, the referenced ChatAgentRun must
		// belong to the same channel/thread/bot context. This is the single internal transcript-write
		// validator's run-context check; a cross-channel/cross-thread/cross-bot run reference is
		// rejected before any row is persisted so a transcript row can never be mis-attributed to a run
		// in another channel, thread, or bot-owned run.
		if in.AgentRunID != nil && *in.AgentRunID != 0 {
			var run common_persistence_models.ChatAgentRun
			if err := tx.Select("id", "channel_id", "parent_message_id", "bot_user_id").
				Where("id = ?", *in.AgentRunID).First(&run).Error; err != nil {
				if errors.Is(err, gorm.ErrRecordNotFound) {
					return ErrChatTranscriptRunMismatch
				}
				return err
			}
			if run.ChannelID != in.ChannelID {
				return ErrChatTranscriptRunMismatch
			}
			if run.ParentMessageID != common_util.Deref(in.ParentMessageID) {
				return ErrChatTranscriptRunMismatch
			}
			if run.BotUserID != in.SenderID {
				return ErrChatTranscriptRunMismatch
			}
		}

		// TOOL_RESULT: require a paired TOOL_CALL and adopt its step index.
		if isToolResult {
			if !hasPairKey {
				return ErrChatToolResultOrphan
			}
			var call common_persistence_models.ChatMessageToolCall
			if err := tx.
				Where("agent_run_id = ? AND tool_call_id = ? AND kind = ?",
					*in.AgentRunID, *in.ToolCallID, proto_models.MessageKind_MESSAGE_KIND_TOOL_CALL).
				First(&call).Error; err != nil {
				if errors.Is(err, gorm.ErrRecordNotFound) {
					return ErrChatToolResultOrphan
				}
				return err
			}
			// A TOOL_RESULT strictly mirrors its paired TOOL_CALL's step index and visibility —
			// UNCONDITIONALLY, overriding whatever the caller passed. There is no legitimate case
			// for a result to diverge from its call:
			//   - hidden_from_users: a result must be exactly as hidden as its call, else a hidden
			//     call's result (incl. synthetic denial/expiry results, and the live-loop result
			//     where the bot writer pre-defaults HiddenFromUsers to an explicit false) leaks as
			//     visible — exposing that a hidden tool call existed, reachable via the expiry sweep
			//     alone.
			//   - exclude_from_ai_context: must mirror for PAIRING correctness — an excluded call
			//     with an included result (or vice versa) reconstructs as a dangling result/call in
			//     the model history. Strict mirror keeps call+result well-formed for E4 reconstruction.
			var callMsg common_persistence_models.ChatMessage
			if err := tx.Select("agent_step_index", "hidden_from_users", "exclude_from_ai_context").
				Where("id = ?", call.MessageID).First(&callMsg).Error; err != nil {
				return err
			}
			in.AgentStepIndex = callMsg.AgentStepIndex
			in.HiddenFromUsers = callMsg.HiddenFromUsers
			in.ExcludeFromAIContext = callMsg.ExcludeFromAIContext
			callCopy := call
			pairedCall = &callCopy
		}

		// Idempotency: if a row with this (run, tool_call_id, kind) already exists, return it
		// (created=false) on a matching payload, or conflict on a different payload.
		if (isToolCall || isToolResult) && hasPairKey {
			var existing common_persistence_models.ChatMessageToolCall
			err := tx.
				Where("agent_run_id = ? AND tool_call_id = ? AND kind = ?", *in.AgentRunID, *in.ToolCallID, in.Kind).
				First(&existing).Error
			if err == nil {
				if !bytesEqual(existing.ModelPayload, in.ModelPayload) || existing.PayloadType != in.PayloadType {
					return ErrChatToolCallConflict
				}
				messageID = existing.MessageID
				if claim == nil {
					return nil // idempotent no-op; created stays false
				}
				// A claimed retry may find an identical TOOL_RESULT left by a commit-ambiguous
				// earlier attempt. It still owns the call lease and must finish the token-bound
				// EXECUTED transition below; skipping it would report committed while leaving the
				// call EXECUTING forever.
				idempotentExisting = true
			}
			if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
				return err
			}
		}
		if isToolResult && !idempotentExisting && claim == nil && pairedCall != nil {
			// Unfenced result creation is restricted to the original inline PENDING path (used
			// for schema-invalid, non-executed calls) and denial/expiry synthetic results. Once an
			// inline call is EXECUTING, or for any APPROVED call, only the token-verified atomic
			// primitive may bind a new payload. This rejects a stale live writer after recovery
			// claimed the row, before any TOOL_RESULT insert.
			if pairedCall.ApprovalStatus == proto_models.ToolCallApprovalStatus_TOOL_CALL_APPROVAL_STATUS_APPROVED ||
				(isInlineApprovalStatus(pairedCall.ApprovalStatus) && pairedCall.ExecutionState != proto_models.ToolCallExecutionState_TOOL_CALL_EXECUTION_STATE_PENDING_EXECUTION) {
				return ErrChatToolCallLeaseLost
			}
		}
		// One provider-state commit marker per run + step. A retry with an identical immutable
		// envelope is a no-op; a different reference or hash is a conflict.
		if isProviderState {
			var existingMsg common_persistence_models.ChatMessage
			err := tx.Preload("ToolCall").Where("agent_run_id = ? AND agent_step_index = ? AND kind = ?", *in.AgentRunID, *in.AgentStepIndex, in.Kind).
				First(&existingMsg).Error
			if err == nil {
				if existingMsg.ToolCall == nil || !providerStatePayloadsEquivalent(existingMsg.ToolCall.ModelPayload, in.ModelPayload) || existingMsg.ToolCall.PayloadType != in.PayloadType {
					return ErrChatToolCallConflict
				}
				messageID = existingMsg.ID
				return nil
			}
			if !errors.Is(err, gorm.ErrRecordNotFound) {
				return err
			}
		}
		if isToolResult && !idempotentExisting {
			var envelope proto_models.MessageModelPayload
			if proto.Unmarshal(in.ModelPayload, &envelope) != nil || envelope.GetToolResult() == nil {
				return ErrChatTranscriptPayloadInvalid
			}
			referenced := make(map[uint64]struct{})
			for partIndex, part := range envelope.GetToolResult().GetContentParts() {
				if part == nil || part.GetMedia() == nil || strings.TrimSpace(part.GetMedia().GetAttachmentId()) == "" {
					continue
				}
				media := part.GetMedia()
				attachmentID, err := strconv.ParseUint(strings.TrimSpace(media.GetAttachmentId()), 10, 64)
				if err != nil || attachmentID == 0 || media.GetSizeBytes() == 0 {
					return ErrChatTranscriptPayloadInvalid
				}
				attachment, ok := lockedInternalAttachments[attachmentID]
				expectedEntityID := toolMediaEntityID(*in.AgentRunID, *in.ToolCallID, partIndex)
				if claim != nil {
					expectedEntityID = claimedToolMediaEntityID(*in.AgentRunID, *in.ToolCallID, claim.leaseOwner, partIndex)
				}
				if !ok || !attachment.InternalOnly || attachment.ObjectType != "agent-tool-media" ||
					attachment.EntityID != expectedEntityID || attachment.UserID != in.SenderID ||
					attachment.UploadStatus != common_persistence_models.UploadStatusVetted || attachment.Size <= 0 ||
					uint64(attachment.Size) != media.GetSizeBytes() || attachment.OriginalFileName != media.GetName() ||
					!strings.EqualFold(attachment.ContentType, media.GetContentType()) {
					return ErrChatTranscriptPayloadInvalid
				}
				referenced[attachmentID] = struct{}{}
				if claim != nil {
					// Externalization is deliberately attempt-scoped so a stale worker cannot
					// occupy the durable run/call/part key before its lease is checked. Once
					// this transaction has locked and verified the current lease, promote the
					// winning attachment to the canonical key used by transcript reconstruction.
					// The exact old-key predicate keeps this update fenced as well; a mismatch
					// rolls the whole result commit back.
					promoted := tx.Model(&common_persistence_models.Attachment{}).
						Where("id = ? AND internal_only = ? AND object_type = ? AND entity_id = ?",
							attachmentID, true, "agent-tool-media", expectedEntityID).
						Update("entity_id", toolMediaEntityID(*in.AgentRunID, *in.ToolCallID, partIndex))
					if promoted.Error != nil {
						return promoted.Error
					}
					if promoted.RowsAffected != 1 {
						return ErrChatToolCallLeaseLost
					}
				}
			}
			if len(referenced) != len(lockedInternalAttachments) {
				return ErrChatTranscriptPayloadInvalid
			}
		}

		if isProviderState && !idempotentExisting {
			state := mustDecodeProviderState(in)
			if state == nil {
				return ErrChatTranscriptPayloadInvalid
			}
			attachment, ok := lockedInternalAttachments[state.GetAttachmentId()]
			if !ok {
				return ErrChatTranscriptPayloadInvalid
			}
			leaseOwner := ""
			if lease, ok := common_context.GetChatAgentRunDriveLease(ctx); ok && lease.RunID == *in.AgentRunID {
				leaseOwner = lease.Owner
			}
			expectedEntityID := providerStateEntityIDForDrive(*in.AgentRunID, *in.AgentStepIndex, leaseOwner)
			if !attachment.InternalOnly || attachment.ObjectType != "agent-provider-state" ||
				attachment.EntityID != expectedEntityID || attachment.UserID != in.SenderID ||
				attachment.UploadStatus != common_persistence_models.UploadStatusVetted || attachment.Size < 0 ||
				uint64(attachment.Size) != state.GetSizeBytes() || !strings.EqualFold(attachment.FileHash, state.GetContentSha256()) {
				return ErrChatTranscriptPayloadInvalid
			}
		}

		// Insert the message (hot columns), unless this claimed retry found an identical result.
		var msg *common_persistence_models.ChatMessage
		if !idempotentExisting {
			msg = &common_persistence_models.ChatMessage{
				ChannelID:            in.ChannelID,
				SenderID:             in.SenderID,
				Content:              in.Content,
				ContentType:          in.ContentType,
				ParentMessageID:      in.ParentMessageID,
				BroadcastToChannel:   in.BroadcastToChannel,
				Kind:                 in.Kind,
				Role:                 in.Role,
				StreamState:          in.StreamState,
				ExcludeFromAIContext: in.ExcludeFromAIContext,
				HiddenFromUsers:      in.HiddenFromUsers,
				AgentRunID:           in.AgentRunID,
				AgentStepIndex:       in.AgentStepIndex,
				AgentNoteSubtype:     transcriptAgentNoteSubtype(in),
				AttachmentIDs:        common_util.ConvertUint64SliceToPqInt64Array(in.AttachmentIDs),
			}
		}
		if !idempotentExisting {
			if err := tx.Create(msg).Error; err != nil {
				// A unique violation on a run-tagged STANDARD row means a concurrent drive of the same
				// run won the (agent_run_id, agent_step_index) assistant-turn race; resolve to the
				// winner's row after the transaction rolls back.
				if isUniqueViolation(err) && isAssistantTurnRow(in) {
					return errChatAssistantTurnRace
				}
				if isUniqueViolation(err) && isProviderState {
					return errChatProviderStateRace
				}
				return err
			}
		}

		// Embeds (e.g. a TOOL_RESULT showing an existing embed type).
		if !idempotentExisting && len(in.Embeds) > 0 {
			rows := common_util.ApplyToEach(in.Embeds, func(e CreateChatMessageEmbed) common_persistence_models.ChatMessageEmbed {
				return common_persistence_models.ChatMessageEmbed{
					MessageID:    msg.ID,
					EmbedType:    e.EmbedType,
					EmbedData:    e.EmbedData,
					FallbackText: e.FallbackText,
					Position:     e.Position,
				}
			})
			if err := tx.Create(&rows).Error; err != nil {
				return err
			}
		}

		// Side row for tool/payload rows.
		if !idempotentExisting && in.HasToolCall {
			side := &common_persistence_models.ChatMessageToolCall{
				ArgumentsRejected: in.ArgumentsRejected,
				MessageID:         msg.ID,
				ChannelID:         in.ChannelID,
				AgentRunID:        in.AgentRunID,
				Kind:              in.Kind,
				ToolCallID:        in.ToolCallID,
				ModelPayload:      in.ModelPayload,
				PayloadType:       in.PayloadType,
				ApprovalStatus:    in.ApprovalStatus,
				TriggeringUserID:  in.TriggeringUserID,
				ApprovalExpiresAt: in.ApprovalExpiresAt,
				ExecutionState:    in.ExecutionState,
			}
			if err := tx.Create(side).Error; err != nil {
				// A unique-violation here means a concurrent insert won the (run, tool_call_id)
				// race; surface as a conflict so the caller treats it idempotently.
				if isUniqueViolation(err) {
					return ErrChatToolCallConflict
				}
				return err
			}
		}

		// Bug 1 fix — advance an INLINE (non-approval) TOOL_CALL to EXECUTED when its paired
		// TOOL_RESULT lands. An inline call is recorded NOT_REQUIRED + PENDING_EXECUTION and is
		// executed by the same handler that recorded it; nothing else ever moves it to EXECUTED, so
		// without this it stays PENDING_EXECUTION forever and CountUnresolvedToolCallsInStep never
		// reaches 0 — hanging the whole-step resume gate in a mixed inline+approval step.
		//
		// Gating (so this does NOT stomp the approval queue's EXECUTING lease, spec 1500-1503):
		//   - approval_status = NOT_REQUIRED: only inline calls. APPROVED calls are owned by the
		//     executor, which calls MarkToolCallExecuted after recording their result; DENIED/EXPIRED
		//     calls are already EXECUTED via ResolveToolCallApproval, and their synthetic result must
		//     not re-touch them.
		//   - execution_state = PENDING_EXECUTION: a not-yet-executed inline call. This deliberately
		//     excludes EXECUTING (a claimed lease), EXECUTED (idempotent re-record), and
		//     EXECUTION_FAILED (terminal), so a retried result is a no-op.
		// The update runs in the same transaction as the result insert, so the pairing invariant and
		// the call's execution state move together.
		if !idempotentExisting && claim == nil && isToolResult && pairedCall != nil && isInlineApprovalStatus(pairedCall.ApprovalStatus) {
			now := time.Now()
			// The conditional WHERE keys off the paired call's CURRENT state (NOT the in-memory copy):
			// approval_status in (UNSPECIFIED, NOT_REQUIRED) AND execution_state = PENDING_EXECUTION.
			// This is a safe self-conditional update — if a concurrent claim moved the row to EXECUTING
			// or the executor already EXECUTED it, RowsAffected is 0 and we leave it alone.
			if err := tx.Model(&common_persistence_models.ChatMessageToolCall{}).
				Where("message_id = ? AND kind = ? AND approval_status IN ? AND execution_state = ?",
					pairedCall.MessageID,
					proto_models.MessageKind_MESSAGE_KIND_TOOL_CALL,
					inlineApprovalStatuses,
					proto_models.ToolCallExecutionState_TOOL_CALL_EXECUTION_STATE_PENDING_EXECUTION).
				Select("execution_state", "execution_not_before", "executed_at", "updated_at").Updates(&toolCallExecutionUpdate{
				ExecutionState:     proto_models.ToolCallExecutionState_TOOL_CALL_EXECUTION_STATE_EXECUTED,
				ExecutionNotBefore: gorm.Expr("NULL"),
				ExecutedAt:         &now,
				UpdatedAt:          now,
			}).Error; err != nil {
				return err
			}
		}

		if claim != nil {
			result := tx.Model(&common_persistence_models.ChatMessageToolCall{}).
				Where("message_id = ? AND kind = ? AND execution_state = ? AND execution_lease_owner = ?",
					claim.toolCallMessageID,
					proto_models.MessageKind_MESSAGE_KIND_TOOL_CALL,
					proto_models.ToolCallExecutionState_TOOL_CALL_EXECUTION_STATE_EXECUTING,
					claim.leaseOwner).
				Select("execution_state", "executed_at", "execution_lease_owner", "execution_lease_expires_at", "execution_not_before", "updated_at").Updates(&toolCallExecutionUpdate{
				ExecutionState:          proto_models.ToolCallExecutionState_TOOL_CALL_EXECUTION_STATE_EXECUTED,
				ExecutedAt:              &claim.executedAt,
				ExecutionLeaseOwner:     nil,
				ExecutionLeaseExpiresAt: nil,
				ExecutionNotBefore:      gorm.Expr("NULL"),
				UpdatedAt:               claim.executedAt,
			})
			if result.Error != nil {
				return result.Error
			}
			if result.RowsAffected != 1 {
				return ErrChatToolCallLeaseLost
			}
		}

		// Preview/last-message only for visible, counting kinds.
		if !idempotentExisting && transcriptUpdatesPreview(in.Kind, in.HiddenFromUsers) {
			if err := touchChatChannelLastMessageTx(tx, in.ChannelID, in.SenderID, in.Content); err != nil {
				return err
			}
		}

		if !idempotentExisting {
			messageID = msg.ID
			created = true
		}
		return nil
	})
	if err != nil {
		if errors.Is(err, errChatAssistantTurnRace) {
			// Idempotent no-op: a concurrent drive already recorded this step's assistant turn.
			// Return the winner's row with created=false so the caller neither fans out nor
			// treats the step as un-generated.
			var existing common_persistence_models.ChatMessage
			if lookupErr := db.
				Where("agent_run_id = ? AND agent_step_index = ? AND kind = ?",
					*in.AgentRunID, *in.AgentStepIndex, in.Kind).
				Order("id").First(&existing).Error; lookupErr != nil {
				return 0, false, chatPersistenceDatabaseError(lookupErr,
					zap.String(logkeys.Stage, "load_winning_assistant_turn"),
					zap.Uint64(appLogger.KeyChannelID, in.ChannelID),
					zap.Uint64(appLogger.KeyAgentRunID, *in.AgentRunID),
					zap.Uint32(appLogger.KeyAgentStepIndex, *in.AgentStepIndex))
			}
			if appLogger.DebugEnabled(h.logger) {
				appLogger.For(ctx, h.logger, loggerName).Debug("transcript assistant turn insert lost an idempotent race",
					zap.Uint64(appLogger.KeyChannelID, in.ChannelID),
					zap.Uint64(appLogger.KeyAgentRunID, *in.AgentRunID),
					zap.Uint32(appLogger.KeyAgentStepIndex, *in.AgentStepIndex))
			}
			return existing.ID, false, nil
		}
		if errors.Is(err, errChatProviderStateRace) {
			var existing common_persistence_models.ChatMessage
			if lookupErr := db.Preload("ToolCall").Where("agent_run_id = ? AND agent_step_index = ? AND kind = ?", *in.AgentRunID, *in.AgentStepIndex, in.Kind).
				First(&existing).Error; lookupErr != nil {
				return 0, false, chatPersistenceDatabaseError(lookupErr,
					zap.String(logkeys.Stage, "load_winning_provider_state"),
					zap.Uint64(appLogger.KeyChannelID, in.ChannelID),
					zap.Uint64(appLogger.KeyAgentRunID, *in.AgentRunID),
					zap.Uint32(appLogger.KeyAgentStepIndex, *in.AgentStepIndex))
			}
			if existing.ToolCall == nil || !providerStatePayloadsEquivalent(existing.ToolCall.ModelPayload, in.ModelPayload) || existing.ToolCall.PayloadType != in.PayloadType {
				return 0, false, ErrChatToolCallConflict
			}
			if appLogger.DebugEnabled(h.logger) {
				appLogger.For(ctx, h.logger, loggerName).Debug("transcript provider state insert lost an idempotent race",
					zap.Uint64(appLogger.KeyChannelID, in.ChannelID),
					zap.Uint64(appLogger.KeyAgentRunID, *in.AgentRunID),
					zap.Uint32(appLogger.KeyAgentStepIndex, *in.AgentStepIndex))
			}
			return existing.ID, false, nil
		}
		if errors.Is(err, ErrChatToolResultOrphan) || errors.Is(err, ErrChatToolCallConflict) || errors.Is(err, ErrChatTranscriptRunMismatch) ||
			errors.Is(err, ErrChatTranscriptPayloadInvalid) || errors.Is(err, ErrChatToolCallLeaseLost) ||
			errors.Is(err, ErrChatAgentRunDriveLeaseLost) || errors.Is(err, ErrChatAgentRunStepSealInvalid) {
			return 0, false, err
		}
		return 0, false, chatPersistenceDatabaseError(err,
			zap.String(logkeys.Stage, "create_transcript_message"),
			zap.Uint64(appLogger.KeyChannelID, in.ChannelID),
			zap.Uint8(appLogger.KeyMessageKind, uint8(in.Kind)))
	}
	return messageID, created, nil
}

func validateTranscriptToolResultEmbeds(in CreateTranscriptMessageInput) error {
	if in.Kind != proto_models.MessageKind_MESSAGE_KIND_TOOL_RESULT {
		return nil
	}
	if err := common_service_models.ValidateToolResultText(in.Content); err != nil {
		return fmt.Errorf("%w: %w", ErrChatTranscriptPayloadInvalid, err)
	}
	if err := common_service_models.ValidateToolResultEmbedCount(len(in.Embeds)); err != nil {
		return fmt.Errorf("%w: %w", ErrChatTranscriptPayloadInvalid, err)
	}
	for i, embed := range in.Embeds {
		if err := common_service_models.ValidateToolResultEmbed(len(embed.EmbedData), embed.FallbackText); err != nil {
			return fmt.Errorf("%w: embed %d: %w", ErrChatTranscriptPayloadInvalid, i, err)
		}
	}
	return nil
}

func isModelStepProjectionKind(kind proto_models.MessageKind) bool {
	switch kind {
	case proto_models.MessageKind_MESSAGE_KIND_STANDARD,
		proto_models.MessageKind_MESSAGE_KIND_TOOL_CALL,
		proto_models.MessageKind_MESSAGE_KIND_AGENT_NOTE,
		proto_models.MessageKind_MESSAGE_KIND_PROVIDER_STATE:
		return true
	default:
		return false
	}
}

// validateTranscriptSideRowPayload enforces the invariants that the database cannot express after
// payload fields moved to the 1:1 side table:
//   - TOOL_CALL / TOOL_RESULT rows must have a side row, non-zero run, non-empty tool_call_id, the
//     matching payload_type, and a protobuf payload whose embedded tool_call_id matches the
//     denormalized side-table key.
//   - TOOL_RESULT orphan/step validation still happens in the transaction, after this shape check.
//   - COMPACTION_SUMMARY rows must carry a compaction payload side row.
//   - PROVIDER_STATE rows carry a reference-only, OpenAI-versioned envelope bound to exactly one
//     internal attachment; SYSTEM_EVENT may carry a typed context-event payload, while
//     STANDARD / AGENT_NOTE rows carry no side payload.
func validateTranscriptSideRowPayload(in CreateTranscriptMessageInput) error {
	if in.AgentNoteSubtype != "" && in.Kind != proto_models.MessageKind_MESSAGE_KIND_AGENT_NOTE {
		return ErrChatTranscriptPayloadInvalid
	}
	switch in.Kind {
	case proto_models.MessageKind_MESSAGE_KIND_TOOL_CALL:
		payload, err := decodeTranscriptModelPayload(in, proto_models.MessageModelPayloadType_MESSAGE_MODEL_PAYLOAD_TYPE_TOOL_CALL)
		if err != nil {
			return err
		}
		toolCall := payload.GetToolCall()
		if in.ArgumentsRejected != (toolCall.GetRejectedArguments() != nil) {
			return ErrChatTranscriptPayloadInvalid
		}
		if in.ArgumentsRejected && (toolCall.GetArgumentsJson() != "" || in.ApprovalStatus != proto_models.ToolCallApprovalStatus_TOOL_CALL_APPROVAL_STATUS_NOT_REQUIRED) {
			return ErrChatTranscriptPayloadInvalid
		}
		if toolCall == nil || toolCall.GetToolCallId() == "" || in.ToolCallID == nil || toolCall.GetToolCallId() != *in.ToolCallID {
			return ErrChatTranscriptPayloadInvalid
		}
		if in.AgentRunID == nil || *in.AgentRunID == 0 {
			return ErrChatTranscriptPayloadInvalid
		}
	case proto_models.MessageKind_MESSAGE_KIND_TOOL_RESULT:
		payload, err := decodeTranscriptModelPayload(in, proto_models.MessageModelPayloadType_MESSAGE_MODEL_PAYLOAD_TYPE_TOOL_RESULT)
		if err != nil {
			return err
		}
		toolResult := payload.GetToolResult()
		if toolResult == nil || toolResult.GetToolCallId() == "" || in.ToolCallID == nil || toolResult.GetToolCallId() != *in.ToolCallID {
			return ErrChatTranscriptPayloadInvalid
		}
		if err := common_service_models.ValidateToolResultPayload(toolResult.GetContentJson(), in.ModelPayload); err != nil {
			return ErrChatTranscriptPayloadInvalid
		}
		if err := common_service_models.ValidateToolResultContentPartCount(len(toolResult.GetContentParts())); err != nil {
			return fmt.Errorf("%w: %w", ErrChatTranscriptPayloadInvalid, err)
		}
		for _, part := range toolResult.GetContentParts() {
			if part == nil || part.GetMedia() == nil {
				continue
			}
			media := part.GetMedia()
			if strings.TrimSpace(media.GetAttachmentId()) != "" {
				if media.GetSizeBytes() == 0 {
					return ErrChatTranscriptPayloadInvalid
				}
			} else if media.GetSizeBytes() != 0 {
				return ErrChatTranscriptPayloadInvalid
			}
		}
		if in.AgentRunID == nil || *in.AgentRunID == 0 {
			return ErrChatTranscriptPayloadInvalid
		}
	case proto_models.MessageKind_MESSAGE_KIND_COMPACTION_SUMMARY:
		payload, err := decodeTranscriptModelPayload(in, proto_models.MessageModelPayloadType_MESSAGE_MODEL_PAYLOAD_TYPE_COMPACTION)
		if err != nil {
			return err
		}
		if payload.GetCompaction() == nil {
			return ErrChatTranscriptPayloadInvalid
		}
	case proto_models.MessageKind_MESSAGE_KIND_PROVIDER_STATE:
		payload, err := decodeTranscriptModelPayload(in, proto_models.MessageModelPayloadType_MESSAGE_MODEL_PAYLOAD_TYPE_PROVIDER_STATE)
		if err != nil {
			return err
		}
		state := payload.GetProviderState()
		if state == nil || state.GetProviderId() == "" || state.GetApi() != proto_models.InferenceAPI_INFERENCE_API_RESPONSES ||
			state.GetFormat() != common_service_models.OpenAIResponsesStateFormat || state.GetVersion() != common_service_models.OpenAIResponsesStateVersion || state.GetAttachmentId() == 0 ||
			state.GetContentSha256() == "" || state.GetSizeBytes() == 0 || state.GetVisibleProjectionSha256() == "" ||
			in.AgentRunID == nil || *in.AgentRunID == 0 || in.AgentStepIndex == nil || in.Role != proto_models.MessageRole_MESSAGE_ROLE_ASSISTANT ||
			in.HiddenFromUsers == nil || !*in.HiddenFromUsers || in.ExcludeFromAIContext == nil || !*in.ExcludeFromAIContext || in.ToolCallID != nil ||
			len(in.AttachmentIDs) != 1 || in.AttachmentIDs[0] != state.GetAttachmentId() {
			return ErrChatTranscriptPayloadInvalid
		}
	case proto_models.MessageKind_MESSAGE_KIND_SYSTEM_EVENT:
		// A system event is plain text, or optionally typed as a context event for clients.
		if !in.HasToolCall && len(in.ModelPayload) == 0 && in.PayloadType == proto_models.MessageModelPayloadType_MESSAGE_MODEL_PAYLOAD_TYPE_UNSPECIFIED && in.ToolCallID == nil {
			return nil
		}
		payload, err := decodeTranscriptModelPayload(in, proto_models.MessageModelPayloadType_MESSAGE_MODEL_PAYLOAD_TYPE_CONTEXT_EVENT)
		if err != nil {
			return err
		}
		if in.ToolCallID != nil || payload.GetContextEvent().GetKind() == proto_models.ContextEventKind_CONTEXT_EVENT_KIND_UNSPECIFIED {
			return ErrChatTranscriptPayloadInvalid
		}
	case proto_models.MessageKind_MESSAGE_KIND_AGENT_NOTE:
		if in.AgentRunID == nil || *in.AgentRunID == 0 {
			return ErrChatTranscriptPayloadInvalid
		}
		if in.HasToolCall || len(in.ModelPayload) > 0 || in.PayloadType != proto_models.MessageModelPayloadType_MESSAGE_MODEL_PAYLOAD_TYPE_UNSPECIFIED || in.ToolCallID != nil {
			return ErrChatTranscriptPayloadInvalid
		}
	case proto_models.MessageKind_MESSAGE_KIND_STANDARD:
		if in.HasToolCall || len(in.ModelPayload) > 0 || in.PayloadType != proto_models.MessageModelPayloadType_MESSAGE_MODEL_PAYLOAD_TYPE_UNSPECIFIED || in.ToolCallID != nil {
			return ErrChatTranscriptPayloadInvalid
		}
	default:
		if in.HasToolCall || len(in.ModelPayload) > 0 || in.PayloadType != proto_models.MessageModelPayloadType_MESSAGE_MODEL_PAYLOAD_TYPE_UNSPECIFIED || in.ToolCallID != nil {
			return ErrChatTranscriptPayloadInvalid
		}
	}
	return nil
}

func transcriptAgentNoteSubtype(in CreateTranscriptMessageInput) string {
	if in.Kind == proto_models.MessageKind_MESSAGE_KIND_AGENT_NOTE && in.AgentNoteSubtype == "" {
		return common_service_models.AgentNoteSubtypeNote
	}
	return in.AgentNoteSubtype
}

func decodeTranscriptModelPayload(in CreateTranscriptMessageInput, expected proto_models.MessageModelPayloadType) (*proto_models.MessageModelPayload, error) {
	if !in.HasToolCall || in.PayloadType != expected || len(in.ModelPayload) == 0 {
		return nil, ErrChatTranscriptPayloadInvalid
	}
	var payload proto_models.MessageModelPayload
	if err := proto.Unmarshal(in.ModelPayload, &payload); err != nil {
		return nil, ErrChatTranscriptPayloadInvalid
	}
	return &payload, nil
}

func mustDecodeProviderState(in CreateTranscriptMessageInput) *proto_models.ProviderStatePayload {
	payload, err := decodeTranscriptModelPayload(in, proto_models.MessageModelPayloadType_MESSAGE_MODEL_PAYLOAD_TYPE_PROVIDER_STATE)
	if err != nil {
		return nil
	}
	return payload.GetProviderState()
}

func providerStatePayloadsEquivalent(a, b []byte) bool {
	decode := func(raw []byte) *proto_models.ProviderStatePayload {
		var payload proto_models.MessageModelPayload
		if len(raw) == 0 || proto.Unmarshal(raw, &payload) != nil || payload.GetProviderState() == nil {
			return nil
		}
		state, ok := proto.Clone(payload.GetProviderState()).(*proto_models.ProviderStatePayload)
		if !ok {
			return nil
		}
		// Retried raw bytes may have been externalized to a fresh content-identical attachment.
		// The digest/size and projection guards are the immutable identity. Once persistence starts,
		// commit outcome is ambiguous; unused objects are left for reconciliation rather than risk
		// deleting a concurrently committed winner.
		state.AttachmentId = 0
		return state
	}
	left, right := decode(a), decode(b)
	return left != nil && right != nil && proto.Equal(left, right)
}

// transcriptUpdatesPreview reports whether a transcript row should refresh the channel preview /
// last_message_at. Only visible STANDARD messages do; hidden rows and the agent/system kinds never
// overwrite the human-facing preview (Decision 7).
func transcriptUpdatesPreview(kind proto_models.MessageKind, hidden *bool) bool {
	if hidden != nil && *hidden {
		return false
	}
	return kind == proto_models.MessageKind_MESSAGE_KIND_STANDARD
}

// bytesEqual reports byte-slice equality treating nil and empty as equal.
func bytesEqual(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// ============================================================================
// CHAT: COMPACTION + CONTEXT READS
// ============================================================================

// GetLatestCompaction returns the most recent COMPACTION_SUMMARY checkpoint for a channel/thread
// scope, or nil when none exists. Compaction scope is per (channel_id, parent_message_id); the
// thread scope predicate is shared with the rest of the chat reads so a run's ParentMessageID
// (0 = main timeline) cannot silently miss main-timeline checkpoints stored with NULL.
//
// It reads with ChatMessageVisibilityServiceTranscript implicitly (compaction summaries default
// to hidden_from_users=true), so no hidden filter is applied — this is an internal read.
//
// Parameters:
//   - ctx: Request context used for cancellation, deadlines, and tracing.
//   - channelID: The channel identifier.
//   - parentMessageID: Thread scope (0 = main timeline).
//
// Returns:
//   - result1: The latest compaction message, or nil when none exists.
//   - result2: ErrDatabaseError on failure.
func (h *CommonPersistenceHandler[U]) GetLatestCompaction(
	ctx context.Context,
	channelID uint64,
	parentMessageID uint64,
) (*common_persistence_models.ChatMessage, error) {
	db := h.getDBFromContext(ctx)

	// Attach the ToolCall side row so the caller can read the CompactionPayload (the watermark lives
	// in the side-row ModelPayload, not on the hot message row). The compaction write helper compares
	// the new checkpoint's covers_through against the previous watermark, so it needs the payload.
	query := db.
		Where("channel_id = ?", channelID).
		Where("kind = ?", proto_models.MessageKind_MESSAGE_KIND_COMPACTION_SUMMARY)
	query = applyChatMessageThreadScope(query, parentMessageID)

	var msg common_persistence_models.ChatMessage
	if err := query.Order("id DESC").Limit(1).First(&msg).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		return nil, chatPersistenceDatabaseError(err,
			zap.String(logkeys.Stage, "get_latest_compaction"),
			zap.Uint64(appLogger.KeyChannelID, channelID),
			zap.Uint64(appLogger.KeyParentMessageID, parentMessageID))
	}
	if err := h.attachChatMessageToolCalls(ctx, []*common_persistence_models.ChatMessage{&msg}); err != nil {
		return nil, err
	}
	return &msg, nil
}

// StreamChatMessagesAfter scans every message in a channel/thread scope with id > afterMessageID
// in ascending id order, invoking fn for each batch. Unlike the page-capped public ListChatMessagesWithOptions,
// this loops over an id cursor until the scope is exhausted (or fn returns stop=true), so the
// context builder can read a long uncompacted span without silently truncating at the pagination
// limit (Decision 3). It honors the resolved read options — internal callers pass
// ChatMessageVisibilityServiceTranscript so hidden transcript rows (compaction summaries, agent
// notes) are included; exclude_from_ai_context is applied by the context builder after fetch.
//
// Parameters:
//   - ctx: Request context used for cancellation, deadlines, and tracing.
//   - channelID: The channel identifier.
//   - parentMessageID: Thread scope (0 = main timeline).
//   - afterMessageID: Exclusive lower bound on message id (the compaction watermark; 0 = from start).
//   - opts: Resolved read options (persistence applies the filter only).
//   - batchSize: Rows per batch; <=0 uses the default.
//   - fn: Invoked per batch; returning stop=true ends the scan early.
//
// Returns:
//   - result: ErrDatabaseError on failure, or any error returned by fn.
func (h *CommonPersistenceHandler[U]) StreamChatMessagesAfter(
	ctx context.Context,
	channelID uint64,
	parentMessageID uint64,
	afterMessageID uint64,
	opts ChatMessageReadOptions,
	batchSize int,
	fn func(batch []*common_persistence_models.ChatMessage) (stop bool, err error),
) error {
	if fn == nil {
		return appLogger.InvariantError("chat_message_stream_callback_present",
			errors.New("stream callback is required"),
			zap.String(logkeys.Stage, "stream_chat_messages_after_cursor"),
			zap.Uint64(appLogger.KeyChannelID, channelID),
			zap.String(logkeys.Expected, "non-nil callback"),
			zap.String(logkeys.Actual, "nil callback"))
	}
	if batchSize <= 0 {
		batchSize = defaultTranscriptScanBatchSize
	}
	db := h.getDBFromContext(ctx)
	cursor := afterMessageID

	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		query := db.Model(&common_persistence_models.ChatMessage{}).
			Where("channel_id = ?", channelID).
			Where("id > ?", cursor)
		query = applyChatMessageThreadScope(query, parentMessageID)
		query = applyChatMessageReadOptions(query, opts)

		var rows []common_persistence_models.ChatMessage
		if opts.ModelContextProjection {
			query = preloadChatModelContextAssociations(query)
		} else {
			query = preloadChatMessageAssociations(query)
		}
		if err := query.
			Order("id ASC").
			Limit(batchSize).
			Find(&rows).Error; err != nil {
			return chatPersistenceDatabaseError(err,
				zap.String(logkeys.Stage, "stream_chat_messages_after_cursor"),
				zap.Uint64(appLogger.KeyChannelID, channelID),
				zap.Uint64(appLogger.KeyParentMessageID, parentMessageID),
				zap.Uint64(appLogger.KeyCursor, cursor),
				zap.Int(appLogger.KeyBatchSize, batchSize))
		}
		if len(rows) == 0 {
			return nil
		}

		batch := make([]*common_persistence_models.ChatMessage, 0, len(rows))
		for i := range rows {
			batch = append(batch, &rows[i])
		}
		if err := h.attachChatMessageToolCalls(ctx, batch); err != nil {
			return err
		}
		stop, err := fn(batch)
		if err != nil {
			return err
		}
		cursor = rows[len(rows)-1].ID
		if stop || len(rows) < batchSize {
			return nil
		}
	}
}

// ============================================================================
// CHAT: UNREAD COUNTING (visibility-aware)
// ============================================================================

// CountVisibleCountingMessagesAfter counts messages in a channel/thread scope with id >
// afterMessageID that should contribute to an unread badge. It always excludes hidden rows
// (hidden_from_users = TRUE) and excludes the kinds the service passes in excludeKinds (the
// service owns the config gate and the TOOL_CALL/TOOL_RESULT/SYSTEM_EVENT default-not-counting
// policy — persistence only applies the filter). Messages authored by the reading user are not
// excluded here; callers that exclude self-authored messages add that predicate via senderID.
//
// Parameters:
//   - ctx: Request context used for cancellation, deadlines, and tracing.
//   - channelID: The channel identifier.
//   - parentMessageID: Thread scope (0 = main timeline).
//   - afterMessageID: Exclusive lower bound (the participant's last_read_message_id; 0 = all).
//   - excludeSenderID: When non-zero, excludes messages authored by this user (their own
//     messages never count as unread for them).
//   - excludeKinds: Message kinds that must not count toward unread.
//
// Returns:
//   - result1: The count of qualifying messages.
//   - result2: ErrDatabaseError on failure.
func (h *CommonPersistenceHandler[U]) CountVisibleCountingMessagesAfter(
	ctx context.Context,
	channelID uint64,
	parentMessageID uint64,
	afterMessageID uint64,
	excludeSenderID uint64,
	excludeKinds []proto_models.MessageKind,
) (int64, error) {
	db := h.getDBFromContext(ctx)

	query := db.Model(&common_persistence_models.ChatMessage{}).
		Where("channel_id = ?", channelID).
		Where("id > ?", afterMessageID).
		Where("hidden_from_users IS NOT TRUE")
	query = applyChatMessageThreadScope(query, parentMessageID)
	if excludeSenderID != 0 {
		query = query.Where("sender_id <> ?", excludeSenderID)
	}
	if len(excludeKinds) > 0 {
		query = query.Where("kind NOT IN ?", excludeKinds)
	}

	var count int64
	if err := query.Count(&count).Error; err != nil {
		return 0, chatPersistenceDatabaseError(err,
			zap.String(logkeys.Stage, "count_visible_counting_messages"),
			zap.Uint64(appLogger.KeyChannelID, channelID),
			zap.Uint64(appLogger.KeyParentMessageID, parentMessageID),
			zap.Uint64(appLogger.KeyAfterMessageID, afterMessageID))
	}
	return count, nil
}

// ============================================================================
// CHAT: TOOL-CALL APPROVAL / EXECUTION (durable work queue)
// ============================================================================

// ResolveToolCallApproval conditionally transitions a pending TOOL_CALL's approval status. It
// only updates rows currently at fromStatus (normally PENDING), so a double-resolve or a
// resolve-after-expiry is a no-op (RowsAffected 0) rather than overwriting a final state. APPROVED
// additionally requires approval_expires_at to be NULL or later than resolvedAt, making the durable
// deadline independent of the optional background sweep. The caller (E5/service) owns authority —
// persistence applies the conditional update and records the resolver/audit columns it is given.
//
// On a successful transition this also sets execution_state (e.g. APPROVED -> PENDING_EXECUTION
// to make the row claimable, or DENIED/EXPIRED -> EXECUTED because no side-effect remains).
//
// Parameters:
//   - ctx: Request context used for cancellation, deadlines, and tracing.
//   - messageID: The TOOL_CALL message id.
//   - fromStatus: Required current approval status.
//   - toStatus: The approval status to set.
//   - toExecState: The execution state to set in the same update.
//   - resolvedByUserID: The resolver (audit); nil for system expiry.
//   - reason: Optional resolution reason (audit).
//   - resolvedAt: Resolution timestamp.
//
// Returns:
//   - result1: true if the row transitioned; false if no row matched (idempotent/conflict).
//   - result2: ErrDatabaseError on failure.
func (h *CommonPersistenceHandler[U]) ResolveToolCallApproval(
	ctx context.Context,
	messageID uint64,
	fromStatus proto_models.ToolCallApprovalStatus,
	toStatus proto_models.ToolCallApprovalStatus,
	toExecState proto_models.ToolCallExecutionState,
	resolvedByUserID *uint64,
	reason *string,
	resolvedAt time.Time,
) (bool, error) {
	db := h.getDBFromContext(ctx)
	updates := toolCallApprovalUpdate{
		ApprovalStatus: toStatus,
		ExecutionState: toExecState,
		ResolvedAt:     &resolvedAt,
		UpdatedAt:      time.Now(),
	}
	columns := []string{"approval_status", "execution_state", "execution_not_before", "resolved_at", "updated_at"}
	if resolvedByUserID != nil {
		updates.ResolvedByUserID = resolvedByUserID
		columns = append(columns, "resolved_by_user_id")
	}
	if reason != nil {
		updates.ResolutionReason = reason
		columns = append(columns, "resolution_reason")
	}
	query := applySealedToolStepPredicate(applyNonTerminalRunPredicate(
		db.Model(&common_persistence_models.ChatMessageToolCall{}))).
		Where("message_id = ? AND kind = ? AND approval_status = ?", messageID, proto_models.MessageKind_MESSAGE_KIND_TOOL_CALL, fromStatus)
	if toStatus == proto_models.ToolCallApprovalStatus_TOOL_CALL_APPROVAL_STATUS_APPROVED {
		// Approval expiry remains a correctness boundary even when the optional
		// background expiry sweep is disabled or delayed.
		query = query.Where("approval_expires_at IS NULL OR approval_expires_at > ?", resolvedAt)
	}
	result := query.Select(columns).Updates(&updates)
	if result.Error != nil {
		return false, chatPersistenceDatabaseError(result.Error,
			zap.String(logkeys.Stage, "resolve_tool_call_approval"),
			zap.Uint64(appLogger.KeyMessageID, messageID),
			zap.Uint8(appLogger.KeyFromStatus, uint8(fromStatus)),
			zap.Uint8(appLogger.KeyToStatus, uint8(toStatus)),
			zap.Uint8(appLogger.KeyExecutionState, uint8(toExecState)))
	}
	resolved := result.RowsAffected > 0
	if !resolved {
		if appLogger.DebugEnabled(h.logger) {
			appLogger.For(ctx, h.logger, loggerName).Debug("tool call approval resolution did not match pending state",
				zap.Uint64(appLogger.KeyMessageID, messageID))
		}
	}
	return resolved, nil
}

// ToolCallExpiryCursor is the stable keyset position for the pending-approval expiry scan. The
// matching partial index is ordered by (approval_expires_at, message_id), so a sweep can move past a
// failing oldest row without repeatedly selecting it within the same tick.
type ToolCallExpiryCursor struct {
	ApprovalExpiresAt time.Time
	MessageID         uint64
}

// PendingToolCallExpiryCandidate is the lean service input for one overdue approval. ToolCall holds
// only the hot/side-row fields needed by the atomic expiry path; Run and PolicySnapshot are loaded in
// bulk for the page rather than with per-row queries.
type PendingToolCallExpiryCandidate struct {
	ToolCall       *common_persistence_models.ChatMessage
	Run            *common_persistence_models.ChatAgentRun
	PolicySnapshot *common_persistence_models.ChatAgentSessionConfigSnapshot
}

// PendingToolCallExpiryBatch carries a cursor page, its limit+1 continuation signal, and a non-fatal
// policy-snapshot lookup error. Approval policy reads historically fail safe to the default policy,
// so a snapshot batch failure must not prevent otherwise valid overdue calls from expiring.
type PendingToolCallExpiryBatch struct {
	Candidates        []*PendingToolCallExpiryCandidate
	SnapshotLookupErr error
	HasMore           bool
}

type pendingToolCallExpiryRow struct {
	MessageID         uint64    `gorm:"column:message_id"`
	ChannelID         uint64    `gorm:"column:channel_id"`
	AgentRunID        *uint64   `gorm:"column:agent_run_id"`
	AgentStepIndex    *uint32   `gorm:"column:agent_step_index"`
	HiddenFromUsers   *bool     `gorm:"column:hidden_from_users"`
	ToolCallID        *string   `gorm:"column:tool_call_id"`
	ApprovalExpiresAt time.Time `gorm:"column:approval_expires_at"`
}

type agentSessionConfigKey struct {
	sessionID uint64
	version   uint64
}

// ListPendingToolCallExpiryBatch returns one lean keyset page and bulk-loads every distinct run and
// captured config snapshot referenced by it. The page costs at most three SELECTs regardless of row
// count: candidates, runs, and snapshots. Normalized tool-config rows are deliberately not loaded;
// expiry needs only the approval policy embedded in ToolConfigPayload.
func (h *CommonPersistenceHandler[U]) ListPendingToolCallExpiryBatch(
	ctx context.Context,
	now time.Time,
	after *ToolCallExpiryCursor,
	limit int,
) (*PendingToolCallExpiryBatch, error) {
	if limit <= 0 {
		limit = defaultTranscriptScanBatchSize
	}
	db := h.getDBFromContext(ctx)
	toolCallsTable := common_persistence_models.TableNameChatMessageToolCalls
	messagesTable := common_persistence_models.TableNameChatMessages

	query := db.Table(toolCallsTable+" AS tc").
		Select("tc.message_id, tc.channel_id, tc.agent_run_id, m.agent_step_index, m.hidden_from_users, tc.tool_call_id, tc.approval_expires_at").
		Joins("JOIN "+messagesTable+" AS m ON m.id = tc.message_id AND m.deleted_at IS NULL").
		Where("tc.kind = ? AND tc.approval_status = ?", proto_models.MessageKind_MESSAGE_KIND_TOOL_CALL, proto_models.ToolCallApprovalStatus_TOOL_CALL_APPROVAL_STATUS_PENDING).
		Where("tc.approval_expires_at IS NOT NULL AND tc.approval_expires_at <= ?", now)
	if after != nil {
		query = query.Where(
			"tc.approval_expires_at > ? OR (tc.approval_expires_at = ? AND tc.message_id > ?)",
			after.ApprovalExpiresAt, after.ApprovalExpiresAt, after.MessageID,
		)
	}

	var rows []pendingToolCallExpiryRow
	if err := query.Order("tc.approval_expires_at ASC, tc.message_id ASC").Limit(limit + 1).Scan(&rows).Error; err != nil {
		return nil, chatPersistenceDatabaseError(err,
			zap.String(logkeys.Stage, "list_pending_tool_call_expiry_candidates"),
			zap.Int(logkeys.Limit, limit),
			zap.Bool(appLogger.KeyHasCursor, after != nil))
	}
	batch := &PendingToolCallExpiryBatch{Candidates: make([]*PendingToolCallExpiryCandidate, 0, len(rows))}
	if len(rows) > limit {
		batch.HasMore = true
		rows = rows[:limit]
	}
	if len(rows) == 0 {
		return batch, nil
	}

	runIDs := make([]uint64, 0, len(rows))
	seenRunIDs := make(map[uint64]struct{}, len(rows))
	for i := range rows {
		if rows[i].AgentRunID == nil || *rows[i].AgentRunID == 0 {
			continue
		}
		if _, seen := seenRunIDs[*rows[i].AgentRunID]; seen {
			continue
		}
		seenRunIDs[*rows[i].AgentRunID] = struct{}{}
		runIDs = append(runIDs, *rows[i].AgentRunID)
	}

	runsByID := make(map[uint64]*common_persistence_models.ChatAgentRun, len(runIDs))
	if len(runIDs) > 0 {
		var runs []common_persistence_models.ChatAgentRun
		if err := db.Where("id IN ?", runIDs).Find(&runs).Error; err != nil {
			return nil, chatPersistenceDatabaseError(err,
				zap.String(logkeys.Stage, "bulk_load_runs_for_approval_expiry"),
				zap.Int(appLogger.KeyRunCount, len(runIDs)))
		}
		for i := range runs {
			runsByID[runs[i].ID] = &runs[i]
		}
	}

	requestedSnapshotKeys := make(map[agentSessionConfigKey]struct{}, len(runsByID))
	snapshotKeyValues := make([][]any, 0, len(runsByID))
	for _, run := range runsByID {
		if run.AgentSessionID == nil || *run.AgentSessionID == 0 || run.AgentSessionConfigVersion == nil {
			continue
		}
		key := agentSessionConfigKey{sessionID: *run.AgentSessionID, version: *run.AgentSessionConfigVersion}
		if _, seen := requestedSnapshotKeys[key]; seen {
			continue
		}
		requestedSnapshotKeys[key] = struct{}{}
		snapshotKeyValues = append(snapshotKeyValues, []any{key.sessionID, key.version})
	}

	snapshotsByKey := make(map[agentSessionConfigKey]*common_persistence_models.ChatAgentSessionConfigSnapshot, len(requestedSnapshotKeys))
	if len(requestedSnapshotKeys) > 0 {
		var snapshots []common_persistence_models.ChatAgentSessionConfigSnapshot
		err := db.Select("agent_session_id", "version", "tool_config_payload").
			Where("(agent_session_id, version) IN ?", snapshotKeyValues).
			Find(&snapshots).Error
		if err != nil {
			batch.SnapshotLookupErr = errors.Join(ErrDatabaseError, err)
			appLogger.For(ctx, h.logger, loggerName).Warn("failed to bulk-load approval policy snapshots; using defaults",
				zap.String(logkeys.Stage, "bulk_load_approval_policy_snapshots"),
				zap.String(logkeys.Fallback, "default_approval_policy"),
				zap.Int(appLogger.KeySnapshotCount, len(requestedSnapshotKeys)),
				zap.Error(err))
		} else {
			for i := range snapshots {
				key := agentSessionConfigKey{sessionID: snapshots[i].AgentSessionID, version: snapshots[i].Version}
				if _, requested := requestedSnapshotKeys[key]; requested {
					snapshotsByKey[key] = &snapshots[i]
				}
			}
		}
	}

	for i := range rows {
		row := rows[i]
		expiresAt := row.ApprovalExpiresAt
		candidateMessage := &common_persistence_models.ChatMessage{
			ID:              row.MessageID,
			ChannelID:       row.ChannelID,
			Kind:            proto_models.MessageKind_MESSAGE_KIND_TOOL_CALL,
			AgentRunID:      row.AgentRunID,
			AgentStepIndex:  row.AgentStepIndex,
			HiddenFromUsers: row.HiddenFromUsers,
			ToolCall: &common_persistence_models.ChatMessageToolCall{
				MessageID:         row.MessageID,
				ChannelID:         row.ChannelID,
				AgentRunID:        row.AgentRunID,
				Kind:              proto_models.MessageKind_MESSAGE_KIND_TOOL_CALL,
				ToolCallID:        row.ToolCallID,
				ApprovalStatus:    proto_models.ToolCallApprovalStatus_TOOL_CALL_APPROVAL_STATUS_PENDING,
				ApprovalExpiresAt: &expiresAt,
				ExecutionState:    proto_models.ToolCallExecutionState_TOOL_CALL_EXECUTION_STATE_WAITING_FOR_APPROVAL,
			},
		}
		candidate := &PendingToolCallExpiryCandidate{ToolCall: candidateMessage}
		if row.AgentRunID != nil {
			candidate.Run = runsByID[*row.AgentRunID]
		}
		if candidate.Run != nil && candidate.Run.AgentSessionID != nil && candidate.Run.AgentSessionConfigVersion != nil {
			candidate.PolicySnapshot = snapshotsByKey[agentSessionConfigKey{
				sessionID: *candidate.Run.AgentSessionID,
				version:   *candidate.Run.AgentSessionConfigVersion,
			}]
		}
		batch.Candidates = append(batch.Candidates, candidate)
	}
	return batch, nil
}

// ListPendingToolCallsToExpire returns pending TOOL_CALL rows whose approval has timed out
// (approval_expires_at <= now), for the background expiry sweep. Uses the partial pending-approval
// index.
//
// Parameters:
//   - ctx: Request context used for cancellation, deadlines, and tracing.
//   - now: The current time (rows with approval_expires_at <= now are returned).
//   - limit: Max rows to return per sweep; <=0 uses the transcript batch default.
//
// Returns:
//   - result1: The expired pending TOOL_CALL rows.
//   - result2: ErrDatabaseError on failure.
func (h *CommonPersistenceHandler[U]) ListPendingToolCallsToExpire(
	ctx context.Context,
	now time.Time,
	limit int,
) ([]*common_persistence_models.ChatMessage, error) {
	if limit <= 0 {
		limit = defaultTranscriptScanBatchSize
	}
	db := h.getDBFromContext(ctx)
	// Scan the side table (which owns the pending-approval partial index), then hydrate the owning
	// messages with their ToolCall association so callers keep getting *ChatMessage rows with full
	// channel/run context. Order preserved by the side-table scan order.
	var toolCalls []common_persistence_models.ChatMessageToolCall
	query := applySealedToolStepPredicate(applyNonTerminalRunPredicate(
		db.Model(&common_persistence_models.ChatMessageToolCall{})))
	if err := query.
		Where("kind = ? AND approval_status = ?", proto_models.MessageKind_MESSAGE_KIND_TOOL_CALL, proto_models.ToolCallApprovalStatus_TOOL_CALL_APPROVAL_STATUS_PENDING).
		Where("approval_expires_at IS NOT NULL AND approval_expires_at <= ?", now).
		Order("approval_expires_at ASC").
		Limit(limit).
		Find(&toolCalls).Error; err != nil {
		return nil, chatPersistenceDatabaseError(err,
			zap.String(logkeys.Stage, "list_pending_tool_calls_to_expire"),
			zap.Int(logkeys.Limit, limit))
	}
	return h.hydrateToolCallMessages(ctx, toolCalls)
}

// hydrateToolCallMessages loads the owning ChatMessage rows for a set of tool-call side rows,
// preserving the side rows' order, and attaches each side row as the message's ToolCall
// association. Used by the work-queue list methods so they keep returning *ChatMessage with full
// channel/run context after the tool-call columns moved to the side table.
func (h *CommonPersistenceHandler[U]) hydrateToolCallMessages(
	ctx context.Context,
	toolCalls []common_persistence_models.ChatMessageToolCall,
) ([]*common_persistence_models.ChatMessage, error) {
	if len(toolCalls) == 0 {
		return []*common_persistence_models.ChatMessage{}, nil
	}
	db := h.getDBFromContext(ctx)
	ids := make([]uint64, 0, len(toolCalls))
	for i := range toolCalls {
		ids = append(ids, toolCalls[i].MessageID)
	}
	var msgs []common_persistence_models.ChatMessage
	if err := preloadChatMessageAssociations(db).
		Where("id IN ?", ids).
		Find(&msgs).Error; err != nil {
		return nil, chatPersistenceDatabaseError(err,
			zap.String(logkeys.Stage, "hydrate_tool_call_messages"),
			zap.Int(appLogger.KeyMessageCount, len(ids)))
	}
	byID := make(map[uint64]*common_persistence_models.ChatMessage, len(msgs))
	for i := range msgs {
		byID[msgs[i].ID] = &msgs[i]
	}
	out := make([]*common_persistence_models.ChatMessage, 0, len(toolCalls))
	for i := range toolCalls {
		if msg, ok := byID[toolCalls[i].MessageID]; ok {
			tc := toolCalls[i]
			msg.ToolCall = &tc
			out = append(out, msg)
		}
	}
	return out, nil
}

// ListClaimableApprovedToolCalls returns approved TOOL_CALL rows that are ready to execute:
// either PENDING_EXECUTION, or EXECUTING with an expired lease (a crashed worker's reclaimable
// work). Backed by the claimable-work and execution-lease partial indexes.
//
// Parameters:
//   - ctx: Request context used for cancellation, deadlines, and tracing.
//   - channelID: When non-zero, restricts to one channel; 0 scans across channels.
//   - now: The current time (used for the expired-lease predicate).
//   - limit: Max rows; <=0 uses the transcript batch default.
//
// Returns:
//   - result1: The claimable TOOL_CALL rows.
//   - result2: ErrDatabaseError on failure.
func (h *CommonPersistenceHandler[U]) ListClaimableApprovedToolCalls(
	ctx context.Context,
	channelID uint64,
	now time.Time,
	limit int,
) ([]*common_persistence_models.ChatMessage, error) {
	if limit <= 0 {
		limit = defaultTranscriptScanBatchSize
	}
	db := h.getDBFromContext(ctx)
	query := applyClaimableApprovedPredicate(db.Model(&common_persistence_models.ChatMessageToolCall{}), now)
	if channelID != 0 {
		query = query.Where("channel_id = ?", channelID)
	}
	toolCalls, err := listFairClaimableToolCallRows(query, limit)
	if err != nil {
		return nil, chatPersistenceDatabaseError(err,
			zap.String(logkeys.Stage, "list_claimable_approved_tool_calls"),
			zap.Uint64(appLogger.KeyChannelID, channelID),
			zap.Int(logkeys.Limit, limit))
	}
	return h.hydrateToolCallMessages(ctx, toolCalls)
}

// ListClaimableToolCalls returns TOOL_CALL rows that are ready for durable execution: approved work,
// inline calls that failed operationally on the live path, and stale fresh inline calls whose live
// owner never recorded a result or failure settlement.
func (h *CommonPersistenceHandler[U]) ListClaimableToolCalls(
	ctx context.Context,
	channelID uint64,
	now time.Time,
	limit int,
) ([]*common_persistence_models.ChatMessage, error) {
	if limit <= 0 {
		limit = defaultTranscriptScanBatchSize
	}
	db := h.getDBFromContext(ctx)
	query := applyClaimableToolCallPredicate(db.Model(&common_persistence_models.ChatMessageToolCall{}), now)
	if channelID != 0 {
		query = query.Where("channel_id = ?", channelID)
	}
	toolCalls, err := listFairClaimableToolCallRows(query, limit)
	if err != nil {
		return nil, chatPersistenceDatabaseError(err,
			zap.String(logkeys.Stage, "list_claimable_tool_calls"),
			zap.Uint64(appLogger.KeyChannelID, channelID),
			zap.Int(logkeys.Limit, limit))
	}
	return h.hydrateToolCallMessages(ctx, toolCalls)
}

// ClaimApprovedToolCall atomically claims an approved TOOL_CALL for execution by one worker. It
// conditionally transitions a row that is either PENDING_EXECUTION, or EXECUTING with an expired
// lease, to EXECUTING with a fresh lease, incrementing execution_attempt_count and stamping
// execution_started_at. Only the worker whose update affected the row (RowsAffected==1) owns the
// claim; concurrent claimers see RowsAffected==0.
//
// Parameters:
//   - ctx: Request context used for cancellation, deadlines, and tracing.
//   - messageID: The TOOL_CALL message id.
//   - leaseOwner: The claiming worker's identifier.
//   - leaseExpiresAt: When this lease expires (after which the row is reclaimable).
//   - now: The current time (for the expired-lease predicate and execution_started_at).
//
// Returns:
//   - result1: true if claimed by this caller; false if another worker holds a live claim.
//   - result2: ErrDatabaseError on failure.
func (h *CommonPersistenceHandler[U]) ClaimApprovedToolCall(
	ctx context.Context,
	messageID uint64,
	leaseOwner string,
	leaseExpiresAt time.Time,
	now time.Time,
) (bool, error) {
	if leaseOwner == "" {
		return false, appLogger.InvariantError("tool_call_lease_owner_present",
			errors.New("lease owner is required"),
			zap.String(logkeys.Stage, "claim_approved_tool_call"),
			zap.Uint64(appLogger.KeyMessageID, messageID),
			zap.String(logkeys.Expected, "non-empty lease owner"),
			zap.String(logkeys.Actual, "empty lease owner"))
	}
	db := h.getDBFromContext(ctx)
	// Same claimable-work predicate as the scanner, additionally narrowed to this one message id so a
	// worker claims exactly the row it was handed.
	result := applyClaimableApprovedPredicate(
		db.Model(&common_persistence_models.ChatMessageToolCall{}).Where("message_id = ?", messageID), now).
		Select("execution_state", "execution_lease_owner", "execution_lease_expires_at", "execution_not_before", "execution_started_at", "execution_attempt_count", "updated_at").Updates(&toolCallExecutionUpdate{
		ExecutionState:          proto_models.ToolCallExecutionState_TOOL_CALL_EXECUTION_STATE_EXECUTING,
		ExecutionLeaseOwner:     &leaseOwner,
		ExecutionLeaseExpiresAt: &leaseExpiresAt,
		ExecutionNotBefore:      gorm.Expr("NULL"),
		ExecutionStartedAt:      &now,
		ExecutionAttemptCount:   gorm.Expr(common_persistence_models.TableNameChatMessageToolCalls + ".execution_attempt_count + 1"),
		UpdatedAt:               now,
	})
	if result.Error != nil {
		return false, chatPersistenceDatabaseError(result.Error,
			zap.String(logkeys.Stage, "claim_approved_tool_call"),
			zap.Uint64(appLogger.KeyMessageID, messageID),
			zap.Time(appLogger.KeyLeaseExpiresAt, leaseExpiresAt))
	}
	claimed := result.RowsAffected > 0
	if !claimed {
		if appLogger.DebugEnabled(h.logger) {
			appLogger.For(ctx, h.logger, loggerName).Debug("approved tool call claim did not match claimable state",
				zap.Uint64(appLogger.KeyMessageID, messageID))
		}
	}
	return claimed, nil
}

// ClaimToolCall atomically claims approved work, retryable inline work, or a stale fresh inline row
// whose durable handoff delay elapsed, for execution by one worker.
func (h *CommonPersistenceHandler[U]) ClaimToolCall(
	ctx context.Context,
	messageID uint64,
	leaseOwner string,
	leaseExpiresAt time.Time,
	now time.Time,
) (bool, error) {
	if leaseOwner == "" {
		return false, appLogger.InvariantError("tool_call_lease_owner_present",
			errors.New("lease owner is required"),
			zap.String(logkeys.Stage, "claim_tool_call"),
			zap.Uint64(appLogger.KeyMessageID, messageID),
			zap.String(logkeys.Expected, "non-empty lease owner"),
			zap.String(logkeys.Actual, "empty lease owner"))
	}
	db := h.getDBFromContext(ctx)
	result := applyClaimableToolCallPredicate(
		db.Model(&common_persistence_models.ChatMessageToolCall{}).Where("message_id = ?", messageID), now).
		Select("execution_state", "execution_lease_owner", "execution_lease_expires_at", "execution_not_before", "execution_started_at", "execution_attempt_count", "updated_at").Updates(&toolCallExecutionUpdate{
		ExecutionState:          proto_models.ToolCallExecutionState_TOOL_CALL_EXECUTION_STATE_EXECUTING,
		ExecutionLeaseOwner:     &leaseOwner,
		ExecutionLeaseExpiresAt: &leaseExpiresAt,
		ExecutionNotBefore:      gorm.Expr("NULL"),
		ExecutionStartedAt:      &now,
		ExecutionAttemptCount:   gorm.Expr(common_persistence_models.TableNameChatMessageToolCalls + ".execution_attempt_count + 1"),
		UpdatedAt:               now,
	})
	if result.Error != nil {
		return false, chatPersistenceDatabaseError(result.Error,
			zap.String(logkeys.Stage, "claim_tool_call"),
			zap.Uint64(appLogger.KeyMessageID, messageID),
			zap.Time(appLogger.KeyLeaseExpiresAt, leaseExpiresAt))
	}
	claimed := result.RowsAffected > 0
	if !claimed {
		if appLogger.DebugEnabled(h.logger) {
			appLogger.For(ctx, h.logger, loggerName).Debug("tool call claim did not match claimable state",
				zap.Uint64(appLogger.KeyMessageID, messageID))
		}
	}
	return claimed, nil
}

// GetClaimedToolCallExecutionAttempt returns the exact durable attempt count for a TOOL_CALL lease
// currently owned by leaseOwner. Recovery scanners may hold an old pre-claim row while another
// replica has claimed and released the same call in the meantime, so adding one to the scanned
// value is not a safe retry-budget authority. The lease predicate makes a lost claim a clean
// (0, false, nil) result.
//
// This is an optional capability consumed by the service tool-work lane; it is intentionally not
// part of CommonPersistenceInterface so existing custom persistence implementations remain source
// compatible and fall back to their pre-claim row count.
func (h *CommonPersistenceHandler[U]) GetClaimedToolCallExecutionAttempt(
	ctx context.Context,
	messageID uint64,
	leaseOwner string,
) (uint32, bool, error) {
	if leaseOwner == "" {
		return 0, false, appLogger.InvariantError("tool_call_lease_owner_present",
			errors.New("lease owner is required"),
			zap.String(logkeys.Stage, "read_claimed_tool_call_execution_attempt"),
			zap.Uint64(appLogger.KeyMessageID, messageID),
			zap.String(logkeys.Expected, "non-empty lease owner"),
			zap.String(logkeys.Actual, "empty lease owner"))
	}
	var owned common_persistence_models.ChatMessageToolCall
	result := h.getDBFromContext(ctx).
		Select("execution_attempt_count").
		Where("message_id = ? AND kind = ? AND execution_state = ? AND execution_lease_owner = ?",
			messageID,
			proto_models.MessageKind_MESSAGE_KIND_TOOL_CALL,
			proto_models.ToolCallExecutionState_TOOL_CALL_EXECUTION_STATE_EXECUTING,
			leaseOwner).
		Take(&owned)
	if errors.Is(result.Error, gorm.ErrRecordNotFound) {
		return 0, false, nil
	}
	if result.Error != nil {
		return 0, false, chatPersistenceDatabaseError(result.Error,
			zap.String(logkeys.Stage, "read_claimed_tool_call_execution_attempt"),
			zap.Uint64(appLogger.KeyMessageID, messageID))
	}
	return owned.ExecutionAttemptCount, true, nil
}

// ClaimFreshInlineToolCall establishes ownership for the original live inline execution. Its exact
// predicate is deliberately distinct from scanner recovery: only a fresh, unleased inline row with
// no prior error may be claimed. The live runner calls this immediately after persisting the call,
// before invoking the potentially side-effecting tool.
func (h *CommonPersistenceHandler[U]) ClaimFreshInlineToolCall(
	ctx context.Context,
	messageID uint64,
	leaseOwner string,
	leaseExpiresAt time.Time,
	now time.Time,
) (bool, error) {
	if leaseOwner == "" {
		return false, appLogger.InvariantError("tool_call_lease_owner_present",
			errors.New("lease owner is required"),
			zap.String(logkeys.Stage, "claim_fresh_inline_tool_call"),
			zap.Uint64(appLogger.KeyMessageID, messageID),
			zap.String(logkeys.Expected, "non-empty lease owner"),
			zap.String(logkeys.Actual, "empty lease owner"))
	}
	db := h.getDBFromContext(ctx)
	claimed := false
	err := db.Transaction(func(tx *gorm.DB) error {
		if lease, ok := common_context.GetChatAgentRunDriveLease(ctx); ok {
			if err := h.assertChatAgentRunDriveLeaseWithDB(ctx, tx, lease.RunID); err != nil {
				return err
			}
		}
		query := applySealedToolStepPredicate(applyNonTerminalRunPredicate(
			tx.Model(&common_persistence_models.ChatMessageToolCall{}))).
			Where("message_id = ? AND kind = ? AND approval_status IN ? AND execution_state = ? AND last_execution_error IS NULL AND execution_lease_owner IS NULL",
				messageID,
				proto_models.MessageKind_MESSAGE_KIND_TOOL_CALL,
				inlineApprovalStatuses,
				proto_models.ToolCallExecutionState_TOOL_CALL_EXECUTION_STATE_PENDING_EXECUTION)
		if lease, ok := common_context.GetChatAgentRunDriveLease(ctx); ok {
			query = query.Where("agent_run_id = ?", lease.RunID)
		}
		result := query.Select("execution_state", "execution_lease_owner", "execution_lease_expires_at", "execution_not_before", "execution_started_at", "execution_attempt_count", "updated_at").Updates(&toolCallExecutionUpdate{
			ExecutionState:          proto_models.ToolCallExecutionState_TOOL_CALL_EXECUTION_STATE_EXECUTING,
			ExecutionLeaseOwner:     &leaseOwner,
			ExecutionLeaseExpiresAt: &leaseExpiresAt,
			ExecutionNotBefore:      gorm.Expr("NULL"),
			ExecutionStartedAt:      &now,
			ExecutionAttemptCount:   gorm.Expr(common_persistence_models.TableNameChatMessageToolCalls + ".execution_attempt_count + 1"),
			UpdatedAt:               now,
		})
		if result.Error != nil {
			return result.Error
		}
		claimed = result.RowsAffected == 1
		return nil
	})
	if err != nil {
		if errors.Is(err, ErrChatAgentRunDriveLeaseLost) {
			return false, err
		}
		return false, chatPersistenceDatabaseError(err,
			zap.String(logkeys.Stage, "claim_fresh_inline_tool_call"),
			zap.Uint64(appLogger.KeyMessageID, messageID),
			zap.Time(appLogger.KeyLeaseExpiresAt, leaseExpiresAt))
	}
	if !claimed {
		if appLogger.DebugEnabled(h.logger) {
			appLogger.For(ctx, h.logger, loggerName).Debug("fresh inline tool call claim did not match claimable state",
				zap.Uint64(appLogger.KeyMessageID, messageID))
		}
	}
	return claimed, nil
}

// ReleaseToolCallLease releases a claim held by leaseOwner. For a retryable failure it returns the
// row to PENDING_EXECUTION, clears the lease, and stamps a DB-clock execution_not_before using the
// durable attempt count. For a terminal failure it sets EXECUTION_FAILED and clears both deadlines.
// The lease-owner predicate ensures a worker can only release its own claim.
//
// Parameters:
//   - ctx: Request context used for cancellation, deadlines, and tracing.
//   - messageID: The TOOL_CALL message id.
//   - leaseOwner: The worker releasing the claim (must match the held lease).
//   - retryable: true to make the row reclaimable; false to mark it terminally failed.
//   - lastErr: Optional capped error text to persist.
//
// Returns:
//   - result1: true if this caller's claim was released; false if it did not hold the claim.
//   - result2: ErrDatabaseError on failure.
func (h *CommonPersistenceHandler[U]) ReleaseToolCallLease(
	ctx context.Context,
	messageID uint64,
	leaseOwner string,
	retryable bool,
	lastErr *string,
) (bool, error) {
	if leaseOwner == "" {
		return false, appLogger.InvariantError("tool_call_lease_owner_present",
			errors.New("lease owner is required"),
			zap.String(logkeys.Stage, "release_tool_call_lease"),
			zap.Uint64(appLogger.KeyMessageID, messageID),
			zap.String(logkeys.Expected, "non-empty lease owner"),
			zap.String(logkeys.Actual, "empty lease owner"))
	}
	db := h.getDBFromContext(ctx)
	released := false
	err := db.Transaction(func(tx *gorm.DB) error {
		query := tx.Model(&common_persistence_models.ChatMessageToolCall{})
		if lease, ok := common_context.GetChatAgentRunDriveLease(ctx); ok {
			if err := h.assertChatAgentRunDriveLeaseWithDB(ctx, tx, lease.RunID); err != nil {
				return err
			}
			query = query.Where("agent_run_id = ?", lease.RunID)
		}
		var owned common_persistence_models.ChatMessageToolCall
		result := query.Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("message_id = ? AND kind = ? AND execution_state = ? AND execution_lease_owner = ?",
				messageID, proto_models.MessageKind_MESSAGE_KIND_TOOL_CALL,
				proto_models.ToolCallExecutionState_TOOL_CALL_EXECUTION_STATE_EXECUTING, leaseOwner).
			Take(&owned)
		if result.Error != nil {
			if errors.Is(result.Error, gorm.ErrRecordNotFound) {
				return nil
			}
			return result.Error
		}

		nextState := proto_models.ToolCallExecutionState_TOOL_CALL_EXECUTION_STATE_PENDING_EXECUTION
		updates := toolCallExecutionUpdate{
			ExecutionState:     nextState,
			ExecutionNotBefore: gorm.Expr("NULL"),
			UpdatedAt:          time.Now(),
		}
		columns := []string{"execution_state", "execution_lease_owner", "execution_lease_expires_at", "execution_not_before", "updated_at"}
		if retryable {
			// Use the database clock so replicas with skewed application clocks agree on when the
			// row becomes claimable. The attempt count was locked and read in this transaction.
			delay := toolCallRetryBackoff(owned.ExecutionAttemptCount)
			updates.ExecutionNotBefore = gorm.Expr(
				"CURRENT_TIMESTAMP + (? * INTERVAL '1 microsecond')", delay.Microseconds())
		} else {
			updates.ExecutionState = proto_models.ToolCallExecutionState_TOOL_CALL_EXECUTION_STATE_EXECUTION_FAILED
			updates.ExecutionNotBefore = gorm.Expr("NULL")
		}
		if lastErr != nil {
			updates.LastExecutionError = lastErr
			columns = append(columns, "last_execution_error")
		}
		updateResult := tx.Model(&common_persistence_models.ChatMessageToolCall{}).
			Where("message_id = ? AND kind = ? AND execution_state = ? AND execution_lease_owner = ?",
				messageID, proto_models.MessageKind_MESSAGE_KIND_TOOL_CALL,
				proto_models.ToolCallExecutionState_TOOL_CALL_EXECUTION_STATE_EXECUTING, leaseOwner).
			Select(columns).Updates(&updates)
		if updateResult.Error != nil {
			return updateResult.Error
		}
		released = updateResult.RowsAffected == 1
		return nil
	})
	if err != nil {
		if errors.Is(err, ErrChatAgentRunDriveLeaseLost) {
			return false, err
		}
		return false, chatPersistenceDatabaseError(err,
			zap.String(logkeys.Stage, "release_tool_call_lease"),
			zap.Uint64(appLogger.KeyMessageID, messageID),
			zap.Bool(appLogger.KeyRetryable, retryable))
	}
	if !released {
		if appLogger.DebugEnabled(h.logger) {
			appLogger.For(ctx, h.logger, loggerName).Debug("tool call lease release did not own an executing claim",
				zap.Uint64(appLogger.KeyMessageID, messageID))
		}
	}
	return released, nil
}

// MarkInlineToolCallExecutionFailure records an operational failure for an inline TOOL_CALL that the
// live continuation loop attempted without first claiming a lease. Retryable failures stay
// PENDING_EXECUTION with last_execution_error set, making them visible to the durable scanner.
// Terminal failures become EXECUTION_FAILED.
//
// Only PENDING_EXECUTION is mutable here. Once a scanner has recovered a stale fresh row and moved it
// to EXECUTING, that lease is the exclusive fencing authority: a slower original live execution may
// not clear or terminally overwrite it. Scanner-owned failures use ReleaseToolCallLease instead.
func (h *CommonPersistenceHandler[U]) MarkInlineToolCallExecutionFailure(
	ctx context.Context,
	messageID uint64,
	retryable bool,
	lastErr *string,
) (bool, error) {
	db := h.getDBFromContext(ctx)
	nextState := proto_models.ToolCallExecutionState_TOOL_CALL_EXECUTION_STATE_PENDING_EXECUTION
	if !retryable {
		nextState = proto_models.ToolCallExecutionState_TOOL_CALL_EXECUTION_STATE_EXECUTION_FAILED
	}
	updates := toolCallExecutionUpdate{
		ExecutionState:     nextState,
		ExecutionNotBefore: gorm.Expr("NULL"),
		UpdatedAt:          time.Now(),
	}
	columns := []string{"execution_state", "execution_lease_owner", "execution_lease_expires_at", "execution_not_before", "updated_at"}
	if retryable {
		// This compatibility path has no claimed-attempt row to lock. Stamp the base delay once;
		// repeated failure reporting must not continually push an already scheduled retry away.
		updates.ExecutionNotBefore = gorm.Expr(
			"COALESCE(execution_not_before, CURRENT_TIMESTAMP + (? * INTERVAL '1 microsecond'))",
			toolCallRetryBackoffBase.Microseconds())
	} else {
		updates.ExecutionNotBefore = gorm.Expr("NULL")
	}
	if lastErr != nil {
		updates.LastExecutionError = lastErr
		columns = append(columns, "last_execution_error")
	} else if retryable {
		updates.LastExecutionError = new("inline tool execution failed")
		columns = append(columns, "last_execution_error")
	}
	marked := false
	err := db.Transaction(func(tx *gorm.DB) error {
		query := tx.Model(&common_persistence_models.ChatMessageToolCall{})
		if lease, ok := common_context.GetChatAgentRunDriveLease(ctx); ok {
			if err := h.assertChatAgentRunDriveLeaseWithDB(ctx, tx, lease.RunID); err != nil {
				return err
			}
			query = query.Where("agent_run_id = ?", lease.RunID)
		}
		result := query.
			Where("message_id = ? AND kind = ? AND approval_status IN ?", messageID, proto_models.MessageKind_MESSAGE_KIND_TOOL_CALL, inlineApprovalStatuses).
			Where("execution_state = ?", proto_models.ToolCallExecutionState_TOOL_CALL_EXECUTION_STATE_PENDING_EXECUTION).
			Select(columns).Updates(&updates)
		if result.Error != nil {
			return result.Error
		}
		marked = result.RowsAffected > 0
		return nil
	})
	if err != nil {
		if errors.Is(err, ErrChatAgentRunDriveLeaseLost) {
			return false, err
		}
		return false, chatPersistenceDatabaseError(err,
			zap.String(logkeys.Stage, "mark_inline_tool_call_execution_failure"),
			zap.Uint64(appLogger.KeyMessageID, messageID),
			zap.Bool(appLogger.KeyRetryable, retryable))
	}
	if !marked {
		if appLogger.DebugEnabled(h.logger) {
			appLogger.For(ctx, h.logger, loggerName).Debug("inline tool call failure update did not match pending state",
				zap.Uint64(appLogger.KeyMessageID, messageID))
		}
	}
	return marked, nil
}

// MarkToolCallExecuted marks a claimed TOOL_CALL as executed after its paired TOOL_RESULT was
// persisted: sets EXECUTED, stamps executed_at, and clears the lease. It runs as its own statement
// after the (idempotent) TOOL_RESULT write — tool execution is at-least-once overall; the result
// row, not this flag, is the durable record of the outcome.
//
// The leaseOwner predicate makes the update conditional on the caller still owning the claim: a
// stale worker whose lease expired and was reclaimed by another worker must not mark the row
// executed or null out the reclaimer's live lease (the same guard ReleaseToolCallLease applies).
//
// Parameters:
//   - ctx: Request context used for cancellation, deadlines, and tracing.
//   - messageID: The TOOL_CALL message id.
//   - leaseOwner: The claiming worker's lease owner id; the update only applies while this worker
//     still holds the lease.
//   - executedAt: Execution completion timestamp.
//
// Returns:
//   - result1: true if the row was marked executed; false if it was not in a claimable state or the
//     caller no longer holds the lease.
//   - result2: ErrDatabaseError on failure.
func (h *CommonPersistenceHandler[U]) MarkToolCallExecuted(
	ctx context.Context,
	messageID uint64,
	leaseOwner string,
	executedAt time.Time,
) (bool, error) {
	db := h.getDBFromContext(ctx)
	result := db.Model(&common_persistence_models.ChatMessageToolCall{}).
		Where("message_id = ? AND kind = ?", messageID, proto_models.MessageKind_MESSAGE_KIND_TOOL_CALL).
		Where("execution_state IN ?", []proto_models.ToolCallExecutionState{
			proto_models.ToolCallExecutionState_TOOL_CALL_EXECUTION_STATE_EXECUTING,
			proto_models.ToolCallExecutionState_TOOL_CALL_EXECUTION_STATE_PENDING_EXECUTION,
		}).
		Where("execution_lease_owner = ?", leaseOwner).
		Select("execution_state", "executed_at", "execution_lease_owner", "execution_lease_expires_at", "execution_not_before", "updated_at").Updates(&toolCallExecutionUpdate{
		ExecutionState:          proto_models.ToolCallExecutionState_TOOL_CALL_EXECUTION_STATE_EXECUTED,
		ExecutedAt:              &executedAt,
		ExecutionLeaseOwner:     nil,
		ExecutionLeaseExpiresAt: nil,
		ExecutionNotBefore:      gorm.Expr("NULL"),
		UpdatedAt:               time.Now(),
	})
	if result.Error != nil {
		return false, chatPersistenceDatabaseError(result.Error,
			zap.String(logkeys.Stage, "mark_tool_call_executed"),
			zap.Uint64(appLogger.KeyMessageID, messageID),
			zap.Time(appLogger.KeyExecutedAt, executedAt))
	}
	marked := result.RowsAffected > 0
	if !marked {
		if appLogger.DebugEnabled(h.logger) {
			appLogger.For(ctx, h.logger, loggerName).Debug("mark tool call executed did not own a matching claim",
				zap.Uint64(appLogger.KeyMessageID, messageID))
		}
	}
	return marked, nil
}

// CountUnresolvedToolCallsInStep counts the TOOL_CALL rows in one (agent_run_id, agent_step_index)
// whose execution is not yet finished — i.e. execution_state NOT IN (EXECUTED, EXECUTION_FAILED).
// E5 gates the whole-step resume nudge on this returning 0, so a sibling approval call that is
// still pending when a parallel call resolves does not get its results dropped (an unresolved
// trailing call reconstructs as a dangling tool request).
//
// Only TOOL_CALL rows are counted. TOOL_RESULT rows also have a side row with an execution_state,
// but their resolution is not what gates resume — counting them would keep the gate permanently
// shut. The count therefore filters kind = TOOL_CALL explicitly. Denied/expired calls are treated
// as resolved (their synthetic result sets the call's execution_state to EXECUTED); approved or
// in-flight calls (WAITING_FOR_APPROVAL / PENDING_EXECUTION / EXECUTING) count as unresolved.
//
// agent_step_index lives on chat_messages (not the side table), so the count joins the side table
// to its owning message for the step filter; run/kind/execution_state come from the side table.
//
// Parameters:
//   - ctx: Request context used for cancellation, deadlines, and tracing.
//   - agentRunID: The run whose step is being checked.
//   - agentStepIndex: The model/tool step within the run.
//
// Returns:
//   - result1: The number of unresolved TOOL_CALL rows in the step.
//   - result2: ErrDatabaseError on failure.
func (h *CommonPersistenceHandler[U]) CountUnresolvedToolCallsInStep(
	ctx context.Context,
	agentRunID uint64,
	agentStepIndex uint32,
) (int, error) {
	db := h.getDBFromContext(ctx)
	toolCallsTable := common_persistence_models.TableNameChatMessageToolCalls
	messagesTable := common_persistence_models.TableNameChatMessages

	var count int64
	if err := db.Table(toolCallsTable+" AS tc").
		Joins("JOIN "+messagesTable+" AS m ON m.id = tc.message_id").
		Where("tc.agent_run_id = ? AND tc.kind = ?", agentRunID, proto_models.MessageKind_MESSAGE_KIND_TOOL_CALL).
		Where("m.agent_step_index = ?", agentStepIndex).
		Where("tc.execution_state NOT IN ?", []proto_models.ToolCallExecutionState{
			proto_models.ToolCallExecutionState_TOOL_CALL_EXECUTION_STATE_EXECUTED,
			proto_models.ToolCallExecutionState_TOOL_CALL_EXECUTION_STATE_EXECUTION_FAILED,
		}).
		Count(&count).Error; err != nil {
		return 0, chatPersistenceDatabaseError(err,
			zap.String(logkeys.Stage, "count_unresolved_tool_calls_in_step"),
			zap.Uint64(appLogger.KeyAgentRunID, agentRunID),
			zap.Uint32(appLogger.KeyAgentStepIndex, agentStepIndex))
	}
	return int(count), nil
}

// HasDeniedOrExpiredToolCallsInStep reports whether a tool step contains a durable denial/expiry.
// This lets whichever parallel sibling finishes last honor ResumeOnDenial=false regardless of the
// order in which approvals, denials, and executions completed.
func (h *CommonPersistenceHandler[U]) HasDeniedOrExpiredToolCallsInStep(
	ctx context.Context,
	agentRunID uint64,
	agentStepIndex uint32,
) (bool, error) {
	db := h.getDBFromContext(ctx)
	toolCallsTable := common_persistence_models.TableNameChatMessageToolCalls
	messagesTable := common_persistence_models.TableNameChatMessages

	var count int64
	if err := db.Table(toolCallsTable+" AS tc").
		Joins("JOIN "+messagesTable+" AS m ON m.id = tc.message_id").
		Where("tc.agent_run_id = ? AND tc.kind = ?", agentRunID, proto_models.MessageKind_MESSAGE_KIND_TOOL_CALL).
		Where("m.agent_step_index = ?", agentStepIndex).
		Where("tc.approval_status IN ?", []proto_models.ToolCallApprovalStatus{
			proto_models.ToolCallApprovalStatus_TOOL_CALL_APPROVAL_STATUS_DENIED,
			proto_models.ToolCallApprovalStatus_TOOL_CALL_APPROVAL_STATUS_EXPIRED,
		}).
		Limit(1).
		Count(&count).Error; err != nil {
		return false, chatPersistenceDatabaseError(err,
			zap.String(logkeys.Stage, "check_denied_tool_calls_in_step"),
			zap.Uint64(appLogger.KeyAgentRunID, agentRunID),
			zap.Uint32(appLogger.KeyAgentStepIndex, agentStepIndex))
	}
	return count > 0, nil
}

// CountRejectedArgumentSteps is independent of compaction and uses immutable call markers.
func (h *CommonPersistenceHandler[U]) CountRejectedArgumentSteps(ctx context.Context, runID uint64) (uint32, error) {
	var count int64
	err := h.getDBFromContext(ctx).Table(common_persistence_models.TableNameChatMessageToolCalls+" AS tc").
		Joins("JOIN "+common_persistence_models.TableNameChatMessages+" AS m ON m.id = tc.message_id").
		Where("tc.agent_run_id = ? AND tc.kind = ? AND tc.arguments_rejected = ?", runID, proto_models.MessageKind_MESSAGE_KIND_TOOL_CALL, true).
		Distinct("m.agent_step_index").Count(&count).Error
	if err != nil {
		return 0, chatPersistenceDatabaseError(err, zap.String(logkeys.Stage, "count_rejected_argument_steps"), zap.Uint64(appLogger.KeyAgentRunID, runID))
	}
	return uint32(count), nil
}
