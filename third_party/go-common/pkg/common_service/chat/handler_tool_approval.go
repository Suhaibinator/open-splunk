package chat

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/Suhaibinator/SRouter/pkg/logkeys"
	"github.com/Suhaibinator/SRouter/pkg/scontext"
	persistence "github.com/Suhaibinator/go-common/pkg/common_persistence"
	common_persistence_models "github.com/Suhaibinator/go-common/pkg/common_persistence/common_persistence_models"
	"github.com/Suhaibinator/go-common/pkg/common_service/common_service_models"
	serviceErrors "github.com/Suhaibinator/go-common/pkg/common_service/errors"
	"github.com/Suhaibinator/go-common/pkg/common_service/ports"
	appLogger "github.com/Suhaibinator/go-common/pkg/logger"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
	"go.uber.org/zap"
)

// maxResolutionReasonLen caps the persisted/forwarded resolution reason as a defense-in-depth bound;
// the API sanitizer already trims/caps, but the service must not assume a sanitized caller.
const maxResolutionReasonLen = 1024

// ResolveToolCall approves or denies a pending TOOL_CALL (Decision 6). The transcript is the state
// machine: this is a one-way transition on the persisted TOOL_CALL row (PENDING -> APPROVED on
// approve; PENDING -> DENIED on deny), enforced server-side and surfaced through the
// CHAT_EVENT_TYPE_TOOL_CALL_RESOLUTION websocket event.
//
// Authority (read from persisted state so it survives a restart): the run's triggering user, OR a
// channel admin when the run's captured session policy enables AllowChannelAdminsToResolve, OR a
// site admin meeting the configured SiteMinUserTypeChatToolApproval threshold.
//
// On approve, the row becomes the durable work item (execution_state=PENDING_EXECUTION) and the
// owning run moves WAITING_FOR_APPROVAL -> RUNNING; execution happens later through the durable
// approved-work queue, not inline. On deny, the service writes the paired synthetic error
// TOOL_RESULT and resumes or completes the run per the captured ResumeOnDenial policy.
//
// Re-resolving is idempotent-safe: re-approving an APPROVED call (or re-denying a DENIED one)
// returns the recorded row. A late approval returns the recorded EXPIRED row together with
// ErrToolCallApprovalExpired; the API treats that pair as a committed, explicit expiry outcome.
// Other contradictory resolutions return ErrToolCallResolutionConflict.
func (h *Handler[U]) ResolveToolCall(ctx context.Context, messageID uint64, approve bool, reason *string) (*common_service_models.ChatMessage, error) {
	if h.chatDisabled() {
		return nil, serviceErrors.ErrChatDisabled
	}
	if messageID == 0 {
		return nil, serviceErrors.ErrInvalidRequest
	}

	actor, actorUserID, err := h.getActor(ctx)
	if err != nil {
		return nil, err
	}
	actorUserType := actor.GetUserType()

	reason = clampReason(reason)

	// Load the TOOL_CALL row (service-transcript visibility: the row may be hidden_from_users, but
	// authority — not actor visibility — governs resolution, and the response is the resolver's own
	// view). The owning run carries triggering-user + captured-config authority inputs.
	toolCall, err := h.persistenceHandler.GetChatMessageByIDWithOptions(ctx, messageID, persistence.ChatMessageReadOptions{
		VisibilityMode: persistence.ChatMessageVisibilityServiceTranscript,
	})
	if err != nil {
		if errors.Is(err, persistence.ErrChatMessageNotFound) {
			return nil, serviceErrors.ErrChatMessageNotFound
		}
		return nil, serviceErrors.ErrDatabaseError
	}
	if toolCall == nil {
		return nil, appLogger.InvariantError("chat_message_lookup_returns_non_nil_row", serviceErrors.ErrToolCallNotResolvable,
			zap.String(logkeys.Stage, "load_tool_call_for_resolution"),
			zap.Uint64(appLogger.KeyMessageID, messageID),
			zap.String(logkeys.Expected, "chat message row"),
			zap.String(logkeys.Actual, "nil chat message"))
	}
	if toolCall.Kind != proto_models.MessageKind_MESSAGE_KIND_TOOL_CALL {
		return nil, serviceErrors.ErrToolCallNotResolvable
	}
	if toolCall.ToolCall == nil {
		return nil, appLogger.InvariantError("tool_call_message_has_payload", serviceErrors.ErrToolCallNotResolvable,
			zap.String(logkeys.Stage, "load_tool_call_for_resolution"),
			zap.Uint64(appLogger.KeyMessageID, messageID),
			zap.String(logkeys.Expected, "tool call payload"),
			zap.String(logkeys.Actual, "nil tool call payload"))
	}
	if isNilOrZero(toolCall.AgentRunID) {
		return nil, appLogger.InvariantError("tool_call_message_has_agent_run", serviceErrors.ErrToolCallNotResolvable,
			zap.String(logkeys.Stage, "load_tool_call_for_resolution"),
			zap.Uint64(appLogger.KeyMessageID, messageID),
			zap.String(logkeys.Expected, "agent_run_id > 0"),
			zap.String(logkeys.Actual, "missing agent_run_id"))
	}

	run, err := h.persistenceHandler.GetChatAgentRunByID(ctx, *toolCall.AgentRunID)
	if err != nil {
		if errors.Is(err, persistence.ErrChatAgentRunNotFound) {
			return nil, serviceErrors.ErrChatAgentRunNotFound
		}
		return nil, serviceErrors.ErrDatabaseError
	}
	if run == nil {
		return nil, appLogger.InvariantError("tool_call_owning_run_exists", serviceErrors.ErrChatAgentRunNotFound,
			zap.String(logkeys.Stage, "load_tool_call_owning_run"),
			zap.Uint64(appLogger.KeyMessageID, messageID),
			zap.Uint64(appLogger.KeyAgentRunID, *toolCall.AgentRunID),
			zap.String(logkeys.Expected, "agent run row"),
			zap.String(logkeys.Actual, "nil agent run"))
	}

	policy := h.loadRunApprovalPolicy(ctx, run)

	authorized, err := h.canResolveToolCall(ctx, actorUserID, actorUserType, run, policy)
	if err != nil {
		return nil, err
	}
	if !authorized {
		appLogger.For(ctx, h.logger, chatLoggerName).Warn("tool call resolution denied: unauthorized resolver",
			zap.String(logkeys.Stage, "authorize_tool_call_resolution"),
			zap.String(appLogger.KeyOutcome, "denied"),
			zap.String(logkeys.Reason, "insufficient_permissions"),
			zap.Uint64(appLogger.KeyMessageID, messageID),
			zap.Uint64(appLogger.KeyAgentRunID, run.ID),
			zap.Uint64(appLogger.KeyChannelID, run.ChannelID),
			zap.Uint64(appLogger.KeyUserID, actorUserID),
			zap.Bool(appLogger.KeyApprove, approve),
		)
		return nil, serviceErrors.ErrInsufficientPermissions
	}
	if approve && toolCall.ToolCall.ApprovalStatus == proto_models.ToolCallApprovalStatus_TOOL_CALL_APPROVAL_STATUS_EXPIRED {
		return h.converter.PersistenceChatMessageToService(toolCall), serviceErrors.ErrToolCallApprovalExpired
	}
	if approve && isPendingApprovalExpiredAt(toolCall, time.Now()) {
		// The background sweep is optional and may be delayed. Expire an overdue row inline so the
		// approval deadline remains authoritative and the row does not wait for a later sweep.
		resolved, err := h.applyDenialOrExpiry(
			ctx, toolCall, run, policy,
			proto_models.ToolCallApprovalStatus_TOOL_CALL_APPROVAL_STATUS_EXPIRED,
			nil, nil,
		)
		if err != nil {
			return nil, err
		}
		return resolved, serviceErrors.ErrToolCallApprovalExpired
	}

	// Idempotency / conflict against the recorded final state, before attempting a transition.
	if _, conflict, done := classifyExistingResolution(toolCall.ToolCall.ApprovalStatus, approve); done {
		if conflict {
			return nil, serviceErrors.ErrToolCallResolutionConflict
		}
		// Already in the requested final state: return the recorded row without re-firing events. A
		// denial/expiry retry still runs the idempotent persisted whole-step gate. This is the recovery
		// trigger when the original atomic resolution committed but its ACK (and therefore its nudge)
		// was lost; terminal denied/expired rows are never selected by either background scanner again.
		if !approve && policy.ResumeOnDenial &&
			toolCall.ToolCall.ExecutionState == proto_models.ToolCallExecutionState_TOOL_CALL_EXECUTION_STATE_EXECUTED {
			runCopy := *run
			stepIndex := toolCall.AgentStepIndex
			h.runAfterApprovalCommit(ctx, func(postCtx context.Context) {
				h.maybeNudgeResumeForStep(postCtx, &runCopy, stepIndex)
			})
		}
		return h.reloadResolvedToolCall(ctx, messageID)
	}

	resolvedByUserID := actorUserID
	if approve {
		return h.applyApproval(ctx, toolCall, run, policy, &resolvedByUserID, reason)
	}
	return h.applyDenialOrExpiry(ctx, toolCall, run, policy, proto_models.ToolCallApprovalStatus_TOOL_CALL_APPROVAL_STATUS_DENIED, &resolvedByUserID, reason)
}

// applyApproval performs the approve transition: PENDING -> APPROVED + PENDING_EXECUTION (one
// conditional update), then keeps a non-terminal run RUNNING. The approved row is now the durable
// work item; execution is performed later by the approved-work queue. A best-effort resolution
// event fans out post-commit.
func (h *Handler[U]) applyApproval(
	ctx context.Context,
	toolCall *common_persistence_models.ChatMessage,
	run *common_persistence_models.ChatAgentRun,
	policy ports.AgentApprovalPolicy,
	resolvedByUserID *uint64,
	reason *string,
) (*common_service_models.ChatMessage, error) {
	now := time.Now()
	var transitioned bool
	var runRunnable bool
	var resolved *common_service_models.ChatMessage
	err := h.runOwnedTransaction(ctx, func(txCtx context.Context) error {
		// Serialize resolution against first-class cancellation before touching the TOOL_CALL row.
		// Both paths use run -> tool lock order, so a cancellation cannot commit between approval and
		// the run-state CAS or leave newly-approved work behind on a terminal run.
		if err := h.lockResolvableAgentRun(txCtx, run.ID); err != nil {
			return err
		}
		var err error
		transitioned, err = h.persistenceHandler.ResolveToolCallApproval(
			txCtx,
			toolCall.ID,
			proto_models.ToolCallApprovalStatus_TOOL_CALL_APPROVAL_STATUS_PENDING,
			proto_models.ToolCallApprovalStatus_TOOL_CALL_APPROVAL_STATUS_APPROVED,
			proto_models.ToolCallExecutionState_TOOL_CALL_EXECUTION_STATE_PENDING_EXECUTION,
			resolvedByUserID,
			reason,
			now,
		)
		if err != nil {
			return serviceErrors.ErrDatabaseError
		}
		if !transitioned {
			return nil
		}
		if h.notificationSink != nil {
			if err := h.notificationSink.ChatApprovalResolved(txCtx, toolCall.ID); err != nil {
				return err
			}
		}

		runRunnable, err = h.persistenceHandler.CompareAndSetChatAgentRunStatus(
			txCtx,
			run.ID,
			[]proto_models.ChatAgentRunStatus{
				proto_models.ChatAgentRunStatus_CHAT_AGENT_RUN_STATUS_WAITING_FOR_APPROVAL,
				proto_models.ChatAgentRunStatus_CHAT_AGENT_RUN_STATUS_RUNNING,
			},
			proto_models.ChatAgentRunStatus_CHAT_AGENT_RUN_STATUS_RUNNING,
			proto_models.ChatAgentRunStatusReason_CHAT_AGENT_RUN_STATUS_REASON_UNSPECIFIED,
		)
		if err != nil {
			return chatDatabaseError("advance_run_after_tool_approval", err,
				zap.Uint64(appLogger.KeyAgentRunID, run.ID),
				zap.Uint64(appLogger.KeyMessageID, toolCall.ID),
				zap.Stringer(logkeys.Expected, proto_models.ChatAgentRunStatus_CHAT_AGENT_RUN_STATUS_RUNNING))
		}

		resolved, err = h.reloadResolvedToolCall(txCtx, toolCall.ID)
		if err != nil {
			return err
		}
		runCopy := *run
		if h.eventPublisher != nil {
			resolvedAtCommit := resolved
			h.addApprovalPostCommitHook(txCtx, "approved_tool_resolution_publish", func(postCtx context.Context) {
				h.publishToolCallResolution(postCtx, toolCall.ID, runCopy.ChannelID, resolvedAtCommit, resolvedByUserID, reason, now)
			})
		}
		if runRunnable {
			h.publishAgentRunTransitionAfterCommit(txCtx, agentRunTransition(&runCopy,
				proto_models.ChatAgentRunStatus_CHAT_AGENT_RUN_STATUS_RUNNING,
				proto_models.ChatAgentRunStatusReason_CHAT_AGENT_RUN_STATUS_REASON_UNSPECIFIED))
			h.addApprovalPostCommitHook(txCtx, "approved_tool_execution_nudge", func(postCtx context.Context) {
				// The durable row is authoritative if this low-latency nudge is missed.
				h.nudgeApprovedWork(postCtx, &runCopy)
			})
		}
		return h.advanceResolvedRunGraph(txCtx, run, toolCall.ID)
	})
	if err != nil {
		return nil, err
	}
	if !transitioned {
		// Lost the race, or the durable deadline passed before the conditional approval update. The
		// latter must expire inline rather than surface as an opaque generic conflict.
		return h.resolveApprovalRaceLoser(ctx, toolCall.ID, run, policy, now)
	}

	return resolved, nil
}

// applyDenialOrExpiry is the shared deny/expiry path (Decision 6, M0 #2 SILENT — no SYSTEM_EVENT).
// It conditionally transitions PENDING -> finalStatus (DENIED or EXPIRED), writes the idempotent
// paired synthetic error TOOL_RESULT, and resumes or completes the run per the captured
// ResumeOnDenial policy. resolvedByUserID is nil for system expiry.
func (h *Handler[U]) applyDenialOrExpiry(
	ctx context.Context,
	toolCall *common_persistence_models.ChatMessage,
	run *common_persistence_models.ChatAgentRun,
	policy ports.AgentApprovalPolicy,
	finalStatus proto_models.ToolCallApprovalStatus,
	resolvedByUserID *uint64,
	reason *string,
) (*common_service_models.ChatMessage, error) {
	return h.applyDenialOrExpiryInternal(ctx, toolCall, run, policy, finalStatus, resolvedByUserID, reason, false)
}

// applyBackgroundExpiry uses a lean resolution view for websocket publication because the expiry
// worker has no API response to populate. Interactive late approvals use applyDenialOrExpiry above
// and retain the fully reloaded message contract.
func (h *Handler[U]) applyBackgroundExpiry(
	ctx context.Context,
	toolCall *common_persistence_models.ChatMessage,
	run *common_persistence_models.ChatAgentRun,
	policy ports.AgentApprovalPolicy,
) (*common_service_models.ChatMessage, error) {
	return h.applyDenialOrExpiryInternal(
		ctx, toolCall, run, policy,
		proto_models.ToolCallApprovalStatus_TOOL_CALL_APPROVAL_STATUS_EXPIRED,
		nil, nil, true,
	)
}

func (h *Handler[U]) applyDenialOrExpiryInternal(
	ctx context.Context,
	toolCall *common_persistence_models.ChatMessage,
	run *common_persistence_models.ChatAgentRun,
	policy ports.AgentApprovalPolicy,
	finalStatus proto_models.ToolCallApprovalStatus,
	resolvedByUserID *uint64,
	reason *string,
	leanSystemExpiry bool,
) (*common_service_models.ChatMessage, error) {
	now := time.Now()
	var transitioned bool
	var runTransitioned bool
	var resultMessageID uint64
	var created bool
	var runRunnable bool
	var syntheticContent string
	var syntheticPayload []byte
	var resolved *common_service_models.ChatMessage
	err := h.runOwnedTransaction(ctx, func(txCtx context.Context) error {
		// Serialize denial/expiry against cancellation before changing approval state or appending the
		// synthetic TOOL_RESULT. This shares the run -> tool order with cancellation/result commit.
		if err := h.lockResolvableAgentRun(txCtx, run.ID); err != nil {
			return err
		}
		var err error
		transitioned, err = h.persistenceHandler.ResolveToolCallApproval(
			txCtx,
			toolCall.ID,
			proto_models.ToolCallApprovalStatus_TOOL_CALL_APPROVAL_STATUS_PENDING,
			finalStatus,
			// No side-effect execution remains; the call is terminal for the queue.
			proto_models.ToolCallExecutionState_TOOL_CALL_EXECUTION_STATE_EXECUTED,
			resolvedByUserID,
			reason,
			now,
		)
		if err != nil {
			return serviceErrors.ErrDatabaseError
		}
		if !transitioned {
			return nil
		}
		if h.notificationSink != nil {
			if err := h.notificationSink.ChatApprovalResolved(txCtx, toolCall.ID); err != nil {
				return err
			}
		}

		// Write the paired synthetic error TOOL_RESULT so the model sees the denial/expiry as a normal
		// tool error (keeps the pairing invariant in the data). Idempotent on (agent_run_id, tool_call_id).
		toolCallID := ""
		if toolCall.ToolCall != nil && toolCall.ToolCall.ToolCallID != nil {
			toolCallID = *toolCall.ToolCall.ToolCallID
		}
		syntheticContent = denialResultContent(finalStatus, reason)
		syntheticPayload, err = marshalToolResultPayload(toolCallID, syntheticContent, true)
		if err != nil {
			return serviceErrors.ErrDatabaseError
		}
		resultMessageID, created, err = h.writeSyntheticToolResult(txCtx, run, toolCallID, syntheticContent)
		if err != nil {
			return chatDatabaseError("write_synthetic_tool_resolution_result", err,
				zap.Uint64(appLogger.KeyMessageID, toolCall.ID),
				zap.Uint64(appLogger.KeyAgentRunID, run.ID),
				zap.Stringer(logkeys.Status, finalStatus))
		}

		if policy.ResumeOnDenial {
			runRunnable, err = h.persistenceHandler.CompareAndSetChatAgentRunStatus(
				txCtx, run.ID,
				[]proto_models.ChatAgentRunStatus{
					proto_models.ChatAgentRunStatus_CHAT_AGENT_RUN_STATUS_WAITING_FOR_APPROVAL,
					proto_models.ChatAgentRunStatus_CHAT_AGENT_RUN_STATUS_RUNNING,
				},
				proto_models.ChatAgentRunStatus_CHAT_AGENT_RUN_STATUS_RUNNING,
				proto_models.ChatAgentRunStatusReason_CHAT_AGENT_RUN_STATUS_REASON_UNSPECIFIED,
			)
			if err != nil {
				return chatDatabaseError("advance_run_after_tool_resolution", err,
					zap.Uint64(appLogger.KeyAgentRunID, run.ID),
					zap.Uint64(appLogger.KeyMessageID, toolCall.ID),
					zap.Stringer(logkeys.Status, finalStatus))
			}
		} else {
			// ResumeOnDenial=false terminates the run only after the whole parallel tool step is
			// resolved. Completing on the first denial would make an approved sibling unclaimable;
			// conversely, an earlier approval may already have moved the run to RUNNING, so the final
			// denial must accept the run's actual RUNNING state as well as WAITING_FOR_APPROVAL.
			stepComplete := true
			if toolCall.AgentStepIndex != nil {
				// Serialize aggregate finalization on the run row. Concurrent last-sibling
				// denials otherwise can each see the other's uncommitted row as unresolved,
				// both defer completion, and leave the run stuck forever.
				remaining, countErr := h.persistenceHandler.CountUnresolvedToolCallsInStep(txCtx, run.ID, *toolCall.AgentStepIndex)
				if countErr != nil {
					return chatDatabaseError("count_unresolved_tool_calls_after_denial", countErr,
						zap.Uint64(appLogger.KeyAgentRunID, run.ID),
						zap.Uint64(appLogger.KeyMessageID, toolCall.ID),
						zap.Uint32(appLogger.KeyAgentStepIndex, *toolCall.AgentStepIndex))
				}
				stepComplete = remaining == 0
			}
			if stepComplete {
				runTransitioned, err = h.persistenceHandler.CompareAndSetChatAgentRunStatus(
					txCtx, run.ID,
					[]proto_models.ChatAgentRunStatus{
						proto_models.ChatAgentRunStatus_CHAT_AGENT_RUN_STATUS_WAITING_FOR_APPROVAL,
						proto_models.ChatAgentRunStatus_CHAT_AGENT_RUN_STATUS_RUNNING,
					},
					proto_models.ChatAgentRunStatus_CHAT_AGENT_RUN_STATUS_COMPLETED,
					proto_models.ChatAgentRunStatusReason_CHAT_AGENT_RUN_STATUS_REASON_APPROVAL_NOT_GRANTED,
				)
				if err != nil {
					return chatDatabaseError("complete_run_after_final_denied_tool_call", err,
						zap.Uint64(appLogger.KeyAgentRunID, run.ID),
						zap.Uint64(appLogger.KeyMessageID, toolCall.ID),
						zap.Stringer(logkeys.Expected, proto_models.ChatAgentRunStatus_CHAT_AGENT_RUN_STATUS_COMPLETED))
				}
			}
		}

		if leanSystemExpiry {
			// A system expiry has no API message response. Build the dedicated websocket resolution view
			// from authoritative transition inputs and the candidate's hot-row visibility, avoiding a
			// full message reload with general chat association preloads.
			resolved = systemExpiryResolutionMessage(toolCall, now)
		} else {
			resolved, err = h.reloadResolvedToolCall(txCtx, toolCall.ID)
			if err != nil {
				return err
			}
		}
		runCopy := *run
		stepIndex := toolCall.AgentStepIndex
		if h.eventPublisher != nil {
			resolvedAtCommit := resolved
			h.addApprovalPostCommitHook(txCtx, "denied_tool_resolution_publish", func(postCtx context.Context) {
				h.publishToolCallResolution(postCtx, toolCall.ID, runCopy.ChannelID, resolvedAtCommit, resolvedByUserID, reason, now)
			})
		}
		if policy.ResumeOnDenial && runRunnable && created && resultMessageID != 0 {
			h.addApprovalPostCommitHook(txCtx, "denied_tool_resume_nudge", func(postCtx context.Context) {
				// Resume only after the denial and its synthetic result are durable.
				h.maybeNudgeResumeForStep(postCtx, &runCopy, stepIndex)
			})
		}
		return h.advanceResolvedRunGraph(txCtx, run, toolCall.ID)
	})
	if err != nil {
		// The callback flags are set before Commit. If all durable mutations completed but Commit's
		// acknowledgement was lost, returning without a gate strands an EXECUTED denial/expiry forever.
		// Run the detached aggregate gate optimistically. A real rollback leaves this call unresolved,
		// so CountUnresolvedToolCallsInStep keeps the gate closed; a durable commit opens it.
		if policy.ResumeOnDenial && transitioned && created && runRunnable && resultMessageID != 0 && toolCall.AgentStepIndex != nil {
			runCopy := *run
			stepIndex := toolCall.AgentStepIndex
			h.runAfterApprovalCommit(ctx, func(postCtx context.Context) {
				h.maybeNudgeResumeForStep(postCtx, &runCopy, stepIndex)
			})
		} else if policy.ResumeOnDenial && transitioned && created && runRunnable && resultMessageID != 0 &&
			toolCall.AgentStepIndex == nil && h.durablyCommittedLegacyDenialOrExpiry(
			ctx, toolCall, run, finalStatus, resultMessageID, syntheticContent, syntheticPayload,
		) {
			runCopy := *run
			h.runAfterApprovalCommit(ctx, func(postCtx context.Context) {
				h.nudgeResume(postCtx, &runCopy)
			})
		}
		return nil, err
	}
	if !transitioned {
		resolved, raceErr := h.resolveRaceLoser(ctx, toolCall.ID, false)
		if raceErr == nil && policy.ResumeOnDenial {
			// Another resolver may be the worker that committed and lost its post-commit nudge. The
			// whole-step gate is idempotent and prevents a premature resume when siblings remain.
			runCopy := *run
			stepIndex := toolCall.AgentStepIndex
			h.runAfterApprovalCommit(ctx, func(postCtx context.Context) {
				h.maybeNudgeResumeForStep(postCtx, &runCopy, stepIndex)
			})
		}
		return resolved, raceErr
	}
	if !policy.ResumeOnDenial && runTransitioned {
		h.publishAgentRunTransitionAfterCommit(ctx, agentRunTransition(run,
			proto_models.ChatAgentRunStatus_CHAT_AGENT_RUN_STATUS_COMPLETED,
			proto_models.ChatAgentRunStatusReason_CHAT_AGENT_RUN_STATUS_REASON_APPROVAL_NOT_GRANTED))
	}

	return resolved, nil
}

// advanceResolvedRunGraph advances the resume revision of the resolved run's graph: an approval
// resolution or expiry may make a graph parked on it drivable again. It is the transaction's last
// write, so the root session lock follows the run lock and any channel lock the resolution took.
func (h *Handler[U]) advanceResolvedRunGraph(ctx context.Context, run *common_persistence_models.ChatAgentRun, toolCallMessageID uint64) error {
	if run.AgentSessionID == nil {
		return nil
	}
	if err := h.persistenceHandler.AdvanceChatAgentGraphResumeRevision(ctx, *run.AgentSessionID, proto_models.GraphEventKind_GRAPH_EVENT_KIND_APPROVAL); err != nil {
		return chatDatabaseError("advance_graph_resume_revision_after_tool_resolution", err,
			zap.Uint64(appLogger.KeyAgentRunID, run.ID),
			zap.Uint64(appLogger.KeyMessageID, toolCallMessageID))
	}
	return nil
}

// lockResolvableAgentRun serializes approval resolution with cancellation and then re-reads the
// status under that same transaction. The run loaded for authorization may be stale by the time the
// resolver enters its write transaction; a terminal status must reject before either the TOOL_CALL
// or a synthetic denial result is written.
func (h *Handler[U]) lockResolvableAgentRun(ctx context.Context, runID uint64) error {
	if err := h.persistenceHandler.LockChatAgentRunForUpdate(ctx, runID); err != nil {
		if errors.Is(err, persistence.ErrChatAgentRunNotFound) {
			return serviceErrors.ErrChatAgentRunNotFound
		}
		return serviceErrors.ErrDatabaseError
	}
	run, err := h.persistenceHandler.GetChatAgentRunByID(ctx, runID)
	if err != nil || run == nil {
		if errors.Is(err, persistence.ErrChatAgentRunNotFound) {
			return serviceErrors.ErrChatAgentRunNotFound
		}
		return serviceErrors.ErrDatabaseError
	}
	if isTerminalRunStatus(run.Status) {
		return serviceErrors.ErrToolCallResolutionConflict
	}
	return nil
}

func (h *Handler[U]) runOwnedTransaction(ctx context.Context, fn func(context.Context) error) error {
	if tx, ok := scontext.GetTransaction(ctx); ok && tx != nil {
		return fn(ctx)
	}

	tx, err := h.persistenceHandler.BeginTransaction(ctx)
	if err != nil {
		return serviceErrors.ErrDatabaseError
	}
	gormTx := &persistence.GormTransactionWithHooks{Tx: tx}
	committed := false
	defer func() {
		if !committed {
			_ = gormTx.Rollback()
		}
	}()
	txCtx := scontext.CopySRouterContext[common_service_models.UserIdType, U](ctx, ctx)
	txCtx = scontext.WithTransaction[common_service_models.UserIdType, U](txCtx, gormTx)

	if err := fn(txCtx); err != nil {
		return err
	}
	if err := gormTx.Commit(); err != nil {
		return serviceErrors.ErrDatabaseError
	}
	committed = true

	// No middleware owns this transaction, so this method owns the matching
	// post-commit drain too. Start the best-effort hooks only after Commit succeeds,
	// on a detached context with the same bounded lifetime used by request-owned
	// hooks. Running asynchronously prevents a hook that ignores cancellation from
	// holding the service call forever.
	h.runOwnedApprovalPostCommitHooks(ctx, gormTx.DrainPostCommitHooks())
	return nil
}

// resolveRaceLoser handles the case where the conditional approval transition affected no rows
// because a concurrent resolver already moved the call out of PENDING. It re-reads and applies the
// same idempotency/conflict classification as the pre-check.
func (h *Handler[U]) resolveRaceLoser(ctx context.Context, messageID uint64, approve bool) (*common_service_models.ChatMessage, error) {
	current, err := h.persistenceHandler.GetChatMessageByIDWithOptions(ctx, messageID, persistence.ChatMessageReadOptions{
		VisibilityMode: persistence.ChatMessageVisibilityServiceTranscript,
	})
	if err != nil || current == nil || current.ToolCall == nil {
		return nil, serviceErrors.ErrToolCallResolutionConflict
	}
	if _, conflict, done := classifyExistingResolution(current.ToolCall.ApprovalStatus, approve); done {
		if conflict {
			return nil, serviceErrors.ErrToolCallResolutionConflict
		}
		return h.converter.PersistenceChatMessageToService(current), nil
	}
	// Still PENDING somehow (e.g. transient) — treat as a conflict rather than retrying blindly.
	return nil, serviceErrors.ErrToolCallResolutionConflict
}

func (h *Handler[U]) resolveApprovalRaceLoser(
	ctx context.Context,
	messageID uint64,
	run *common_persistence_models.ChatAgentRun,
	policy ports.AgentApprovalPolicy,
	approvalAttemptedAt time.Time,
) (*common_service_models.ChatMessage, error) {
	current, err := h.persistenceHandler.GetChatMessageByIDWithOptions(ctx, messageID, persistence.ChatMessageReadOptions{
		VisibilityMode: persistence.ChatMessageVisibilityServiceTranscript,
	})
	if err != nil || current == nil || current.ToolCall == nil {
		return nil, serviceErrors.ErrToolCallResolutionConflict
	}
	if current.ToolCall.ApprovalStatus == proto_models.ToolCallApprovalStatus_TOOL_CALL_APPROVAL_STATUS_EXPIRED {
		return h.converter.PersistenceChatMessageToService(current), serviceErrors.ErrToolCallApprovalExpired
	}
	if isPendingApprovalExpiredAt(current, approvalAttemptedAt) {
		resolved, err := h.applyDenialOrExpiry(
			ctx, current, run, policy,
			proto_models.ToolCallApprovalStatus_TOOL_CALL_APPROVAL_STATUS_EXPIRED,
			nil, nil,
		)
		if err != nil {
			return nil, err
		}
		return resolved, serviceErrors.ErrToolCallApprovalExpired
	}
	if _, conflict, done := classifyExistingResolution(current.ToolCall.ApprovalStatus, true); done {
		if conflict {
			return nil, serviceErrors.ErrToolCallResolutionConflict
		}
		return h.converter.PersistenceChatMessageToService(current), nil
	}
	return nil, serviceErrors.ErrToolCallResolutionConflict
}

func isPendingApprovalExpiredAt(toolCall *common_persistence_models.ChatMessage, now time.Time) bool {
	return toolCall != nil && toolCall.ToolCall != nil &&
		toolCall.ToolCall.ApprovalStatus == proto_models.ToolCallApprovalStatus_TOOL_CALL_APPROVAL_STATUS_PENDING &&
		toolCall.ToolCall.ApprovalExpiresAt != nil &&
		!toolCall.ToolCall.ApprovalExpiresAt.After(now)
}

func systemExpiryResolutionMessage(
	toolCall *common_persistence_models.ChatMessage,
	resolvedAt time.Time,
) *common_service_models.ChatMessage {
	if toolCall == nil {
		return nil
	}
	resolvedAtCopy := resolvedAt
	return &common_service_models.ChatMessage{
		ID:              common_service_models.ChatMessageIdType(toolCall.ID),
		ChannelID:       common_service_models.ChatChannelIdType(toolCall.ChannelID),
		HiddenFromUsers: toolCall.HiddenFromUsers,
		ToolCall: &common_service_models.ChatMessageToolCall{
			ApprovalStatus: proto_models.ToolCallApprovalStatus_TOOL_CALL_APPROVAL_STATUS_EXPIRED,
			ExecutionState: proto_models.ToolCallExecutionState_TOOL_CALL_EXECUTION_STATE_EXECUTED,
			ResolvedAt:     &resolvedAtCopy,
		},
	}
}

// classifyExistingResolution decides, given a tool call's current approval status and the requested
// action (approve), whether the request is already satisfied or contradictory.
//
// Returns (final, conflict, done):
//   - done=false: the call is still PENDING and the transition should be attempted.
//   - done=true, conflict=false: already in the requested final state (idempotent success).
//   - done=true, conflict=true: in a contradictory final state (return conflict).
func classifyExistingResolution(status proto_models.ToolCallApprovalStatus, approve bool) (final bool, conflict bool, done bool) {
	switch status {
	case proto_models.ToolCallApprovalStatus_TOOL_CALL_APPROVAL_STATUS_PENDING:
		return false, false, false
	case proto_models.ToolCallApprovalStatus_TOOL_CALL_APPROVAL_STATUS_APPROVED:
		return true, !approve, true
	case proto_models.ToolCallApprovalStatus_TOOL_CALL_APPROVAL_STATUS_DENIED,
		proto_models.ToolCallApprovalStatus_TOOL_CALL_APPROVAL_STATUS_EXPIRED:
		// Denied and expired are equivalent terminal "not approved" outcomes; a deny request against
		// either is idempotent success, an approve request is a conflict.
		return true, approve, true
	case proto_models.ToolCallApprovalStatus_TOOL_CALL_APPROVAL_STATUS_NOT_REQUIRED:
		// Not an approvable call; treat any resolution as a conflict (nothing to resolve).
		return true, true, true
	default:
		return true, true, true
	}
}

// canResolveToolCall enforces Decision 6 resolver authority:
// actor == run.TriggeringUserID OR (policy.AllowChannelAdminsToResolve AND actor is channel
// admin-or-above) OR site-admin meeting the SiteMinUserTypeChatToolApproval threshold.
func (h *Handler[U]) canResolveToolCall(
	ctx context.Context,
	actorUserID uint64,
	actorUserType proto_models.UserType,
	run *common_persistence_models.ChatAgentRun,
	policy ports.AgentApprovalPolicy,
) (bool, error) {
	return h.resolvesToolCalls(ctx, actorUserID, h.hasSiteToolApproval(ctx, actorUserType), run, nil, func() bool {
		return policy.AllowChannelAdminsToResolve
	})
}

// resolvesToolCalls is the resolver decision behind canResolveToolCall, which Stop shares: the
// run's triggering user, a site tool-approval admin, or, when the run's captured approval policy
// lets them (adminsResolve), an admin of the run's channel. This set is aligned with the
// hidden-authorized fanout recipients, so an authorized resolver of a hidden call is also a
// recipient of it. role is the actor's channel role when the caller already loaded it; nil loads
// it only once adminsResolve allows channel admins. adminsResolve is called only when the answer
// depends on it, so a caller may load the policy lazily.
func (h *Handler[U]) resolvesToolCalls(
	ctx context.Context,
	actorUserID uint64,
	siteApprover bool,
	run *common_persistence_models.ChatAgentRun,
	role *proto_models.ChannelRole,
	adminsResolve func() bool,
) (bool, error) {
	if siteApprover || (run.TriggeringUserID != nil && *run.TriggeringUserID == actorUserID) {
		return true, nil
	}
	if role != nil {
		return *role >= proto_models.ChannelRole_CHANNEL_ROLE_ADMIN && adminsResolve(), nil
	}
	if !adminsResolve() {
		return false, nil
	}
	_, loaded, isMember, err := h.loadChannelAccess(ctx, run.ChannelID, common_service_models.UserIdType(actorUserID))
	if err != nil {
		return false, err
	}
	return isMember && loaded >= proto_models.ChannelRole_CHANNEL_ROLE_ADMIN, nil
}

// loadRunApprovalPolicy resolves the run's captured approval policy from its session config snapshot.
// Falls back to a safe default (ResumeOnDenial=true, default expiry) when the run has no first-class
// session or the snapshot cannot be decoded — matching the writer's default behavior.
func (h *Handler[U]) loadRunApprovalPolicy(ctx context.Context, run *common_persistence_models.ChatAgentRun) ports.AgentApprovalPolicy {
	def := ports.AgentApprovalPolicy{ResumeOnDenial: true}
	if run == nil || run.AgentSessionID == nil || run.AgentSessionConfigVersion == nil {
		return def
	}
	if evidence := controlEvidence(ctx); evidence != nil && evidence.runs != nil {
		row := evidence.runs[run.ID]
		return h.decodeRunApprovalPolicy(ctx, run, &common_persistence_models.ChatAgentSessionConfigSnapshot{ToolConfigPayload: row.ToolConfigPayload})
	}
	snapshot, _, err := h.persistenceHandler.GetChatAgentSessionConfigSnapshot(ctx, *run.AgentSessionID, *run.AgentSessionConfigVersion)
	if err != nil {
		appLogger.For(ctx, h.logger, chatLoggerName).Warn("failed to load captured approval policy; using default",
			zap.String(logkeys.Stage, "load_captured_approval_policy"),
			zap.String(logkeys.Fallback, "use_default_approval_policy"),
			zap.Uint64(appLogger.KeyAgentRunID, run.ID), zap.Error(err))
		return def
	}
	return h.decodeRunApprovalPolicy(ctx, run, snapshot)
}

// decodeRunApprovalPolicy preserves identical fallback policy and diagnostics for
// scalar persistence reads and operation-local notification source hydration.
func (h *Handler[U]) decodeRunApprovalPolicy(ctx context.Context, run *common_persistence_models.ChatAgentRun, snapshot *common_persistence_models.ChatAgentSessionConfigSnapshot) ports.AgentApprovalPolicy {
	def := ports.AgentApprovalPolicy{ResumeOnDenial: true}
	if run == nil || run.AgentSessionID == nil || run.AgentSessionConfigVersion == nil {
		return def
	}
	if snapshot == nil {
		appLogger.LogInvariantViolation(appLogger.For(ctx, h.logger, chatLoggerName), "captured_approval_policy_snapshot_exists", nil,
			zap.String(logkeys.Stage, "load_captured_approval_policy"),
			zap.Uint64(appLogger.KeyAgentRunID, run.ID),
			zap.Uint64(appLogger.KeyAgentSessionID, *run.AgentSessionID),
			zap.Uint64(appLogger.KeyConfigVersion, *run.AgentSessionConfigVersion),
			zap.String(logkeys.Expected, "captured config snapshot"),
			zap.String(logkeys.Actual, "nil config snapshot"),
			zap.String(logkeys.Fallback, "use_default_approval_policy"))
		return def
	}
	policy, err := DecodeApprovalPolicy(snapshot.ToolConfigPayload)
	if err != nil {
		appLogger.For(ctx, h.logger, chatLoggerName).Warn("failed to decode captured approval policy; using default",
			zap.Uint64(appLogger.KeyAgentRunID, run.ID), zap.Error(err))
		return def
	}
	return policy
}

// writeSyntheticToolResult inserts the paired error TOOL_RESULT for a denied/expired TOOL_CALL via
// the shared CreateTranscriptMessage primitive (the same idempotent path the bot transcript writer
// uses). Idempotent on (agent_run_id, tool_call_id): a retried denial/expiry does not duplicate the
// result. Returns (messageID, created, error) so callers gate the resolution fanout / resume nudge
// on a real insert.
func (h *Handler[U]) writeSyntheticToolResult(
	ctx context.Context,
	run *common_persistence_models.ChatAgentRun,
	toolCallID string,
	content string,
) (uint64, bool, error) {
	if toolCallID == "" || run == nil || run.ID == 0 {
		return 0, false, serviceErrors.ErrInvalidRequest
	}
	payload, err := marshalToolResultPayload(toolCallID, content, true)
	if err != nil {
		return 0, false, err
	}
	runID := run.ID
	tcID := toolCallID
	in := persistence.CreateTranscriptMessageInput{
		ChannelID:       run.ChannelID,
		ParentMessageID: parentMessageIDPtr(run.ParentMessageID),
		SenderID:        run.BotUserID,
		Content:         content,
		ContentType:     proto_models.MessageContentType_MESSAGE_CONTENT_TYPE_TEXT,
		Kind:            proto_models.MessageKind_MESSAGE_KIND_TOOL_RESULT,
		Role:            proto_models.MessageRole_MESSAGE_ROLE_TOOL,
		AgentRunID:      &runID,
		HasToolCall:     true,
		ToolCallID:      &tcID,
		ModelPayload:    payload,
		PayloadType:     proto_models.MessageModelPayloadType_MESSAGE_MODEL_PAYLOAD_TYPE_TOOL_RESULT,
		ExecutionState:  proto_models.ToolCallExecutionState_TOOL_CALL_EXECUTION_STATE_EXECUTED,
	}
	messageID, created, err := h.persistenceHandler.CreateTranscriptMessage(ctx, in)
	if err != nil {
		return 0, false, err
	}
	return messageID, created, nil
}

// reloadResolvedToolCall re-reads the resolved TOOL_CALL row (service-transcript visibility) and maps
// it to the service model for the API response. Reload failures propagate to the caller.
func (h *Handler[U]) reloadResolvedToolCall(ctx context.Context, messageID uint64) (*common_service_models.ChatMessage, error) {
	row, err := h.persistenceHandler.GetChatMessageByIDWithOptions(ctx, messageID, persistence.ChatMessageReadOptions{VisibilityMode: persistence.ChatMessageVisibilityServiceTranscript})
	if err != nil {
		return nil, chatDatabaseError("reload_resolved_tool_call", err)
	}
	if row == nil {
		return nil, serviceErrors.ErrChatMessageNotFound
	}
	resolved := h.converter.PersistenceChatMessageToService(row)
	if resolved == nil {
		return nil, serviceErrors.ErrInternalError
	}
	return resolved, nil
}

// clampReason trims an over-long resolution reason as a service-side defense-in-depth bound.
func clampReason(reason *string) *string {
	if reason == nil {
		return nil
	}
	r := boundedUTF8(*reason, maxResolutionReasonLen)
	return &r
}

func chatDatabaseError(stage string, cause error, fields ...zap.Field) error {
	contextFields := make([]zap.Field, 0, len(fields)+1)
	contextFields = append(contextFields, zap.String(logkeys.Stage, stage))
	contextFields = append(contextFields, fields...)
	return appLogger.WithErrorFields(
		fmt.Errorf("%s: %w: %w", stage, serviceErrors.ErrDatabaseError, cause),
		contextFields...,
	)
}
