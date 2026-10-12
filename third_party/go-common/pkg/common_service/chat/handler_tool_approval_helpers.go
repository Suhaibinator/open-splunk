package chat

import (
	"bytes"
	"context"
	"fmt"
	"time"

	"github.com/Suhaibinator/SRouter/pkg/logkeys"
	"github.com/Suhaibinator/SRouter/pkg/scontext"
	"github.com/Suhaibinator/go-common/pkg/common_context"
	common_persistence "github.com/Suhaibinator/go-common/pkg/common_persistence"
	common_persistence_models "github.com/Suhaibinator/go-common/pkg/common_persistence/common_persistence_models"
	"github.com/Suhaibinator/go-common/pkg/common_service/agentkit"
	"github.com/Suhaibinator/go-common/pkg/common_service/common_service_models"
	serviceErrors "github.com/Suhaibinator/go-common/pkg/common_service/errors"
	"github.com/Suhaibinator/go-common/pkg/common_service/ports"
	appLogger "github.com/Suhaibinator/go-common/pkg/logger"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

const approvalCommitReconcileTimeout = 5 * time.Second

// runAfterApprovalCommit defers post-resolution work (resume nudges, approved-work claims)
// until the transaction that made the resolution durable has committed. In the
// middleware-owned transaction path the work registers as a post-commit hook — running it
// inline would race the commit: the work's detached goroutines would inherit the open
// transaction and die with "transaction has already been committed or rolled back" mid-turn,
// or claim approved work before the approval is durable. Without a hook-capable transaction
// in ctx, the resolution transaction was owned by runOwnedTransaction and has
// already committed, so the work runs immediately. Both paths hand the work a
// cancellation-detached context with any transaction stripped.
func (h *Handler[U]) runAfterApprovalCommit(ctx context.Context, work func(context.Context)) {
	if work == nil {
		return
	}
	if common_persistence.AddPostCommitHookFromContext(ctx, func(hookCtx context.Context) error {
		work(common_persistence.WithoutRequestTransaction[U](hookCtx))
		return nil
	}) {
		return
	}
	work(common_persistence.WithoutRequestTransaction[U](context.WithoutCancel(ctx)))
}

// publishAgentRunTransitionAfterCommit reports a CAS-winning run-status transition through the
// shared post-commit delivery path (realtime run-state event, then the application observer).
func (h *Handler[U]) publishAgentRunTransitionAfterCommit(ctx context.Context, transition ports.AgentRunTransition) {
	if h == nil {
		return
	}
	PublishAgentRunTransitionAfterCommit[U](ctx, h.logger, chatLoggerName, h.eventPublisher, h.agentRunObserver, transition, h.SettleAgentRunAfterCommit)
}

// marshalToolResultPayload serializes a MessageModelPayload carrying a ToolResultPayload, used for
// the synthetic denial/expiry error result. content_json is left empty (the human-readable text
// lives in the message Content / ToolResultPayload content); is_error marks it as a tool error so the
// model treats it like any other failed tool call.
func marshalToolResultPayload(toolCallID, content string, isError bool) ([]byte, error) {
	return proto.MarshalOptions{Deterministic: true}.Marshal(&proto_models.MessageModelPayload{
		Payload: &proto_models.MessageModelPayload_ToolResult{
			ToolResult: &proto_models.ToolResultPayload{
				ToolCallId: toolCallID,
				IsError:    isError,
				ContentParts: []*proto_models.MessagePart{
					{Part: &proto_models.MessagePart_Text{Text: &proto_models.TextPart{Text: content}}},
				},
			},
		},
	})
}

// marshalExecutionResultPayload serializes the MessageModelPayload for a real (post-execution)
// TOOL_RESULT from the runtime executor's AgentToolResult.
func marshalExecutionResultPayload(toolCallID string, result *ports.AgentToolResult, contentParts []*proto_models.MessagePart) ([]byte, error) {
	tr := &proto_models.ToolResultPayload{
		ToolCallId:   toolCallID,
		ContentJson:  string(result.ContentJSON),
		IsError:      result.IsError,
		ContentParts: contentParts,
	}
	return proto.MarshalOptions{Deterministic: true}.Marshal(&proto_models.MessageModelPayload{
		Payload: &proto_models.MessageModelPayload_ToolResult{ToolResult: tr},
	})
}

// buildToolResultInput assembles the CreateTranscriptMessageInput for a real post-execution
// TOOL_RESULT, carrying the executor's fallback content and any display embeds.
func buildToolResultInput(
	run *common_persistence_models.ChatAgentRun,
	runID *uint64,
	toolCallID *string,
	payload []byte,
	result *ports.AgentToolResult,
	attachmentIDs []uint64,
) common_persistence.CreateTranscriptMessageInput {
	return common_persistence.CreateTranscriptMessageInput{
		ChannelID:       run.ChannelID,
		ParentMessageID: parentMessageIDPtr(run.ParentMessageID),
		SenderID:        run.BotUserID,
		Content:         result.Content,
		ContentType:     proto_models.MessageContentType_MESSAGE_CONTENT_TYPE_TEXT,
		Kind:            proto_models.MessageKind_MESSAGE_KIND_TOOL_RESULT,
		Role:            proto_models.MessageRole_MESSAGE_ROLE_TOOL,
		AgentRunID:      runID,
		AttachmentIDs:   attachmentIDs,
		Embeds:          common_persistence.CreateChatMessageEmbedsFromService(result.Embeds),
		HasToolCall:     true,
		ToolCallID:      toolCallID,
		ModelPayload:    payload,
		PayloadType:     proto_models.MessageModelPayloadType_MESSAGE_MODEL_PAYLOAD_TYPE_TOOL_RESULT,
		ExecutionState:  proto_models.ToolCallExecutionState_TOOL_CALL_EXECUTION_STATE_EXECUTED,
	}
}

// denialResultContent renders the model-/human-visible text for a denied or expired tool call.
func denialResultContent(finalStatus proto_models.ToolCallApprovalStatus, reason *string) string {
	if finalStatus == proto_models.ToolCallApprovalStatus_TOOL_CALL_APPROVAL_STATUS_EXPIRED {
		return "Tool call approval expired before it was resolved."
	}
	if reason != nil && *reason != "" {
		return fmt.Sprintf("Tool call denied by user: %s", *reason)
	}
	return "Tool call denied by user."
}

// publishToolCallResolution fans out the CHAT_EVENT_TYPE_TOOL_CALL_RESOLUTION event post-resolution
// (best-effort). It carries the durable approval/execution state so clients patch the cached card in
// place, and mirrors the row's hidden flag so the recipient resolver applies the same privileged-only
// split (a hidden tool call's resolution must not reach regular users). When caller metadata is nil,
// the payload preserves the resolved_by / reason / resolved_at columns from the reloaded row; this
// lets later execution-state updates avoid erasing the original human approval metadata in clients.
func (h *Handler[U]) publishToolCallResolution(
	ctx context.Context,
	messageID uint64,
	channelID uint64,
	resolved *common_service_models.ChatMessage,
	resolvedByUserID *uint64,
	reason *string,
	resolvedAt time.Time,
) {
	if h.eventPublisher == nil || resolved == nil {
		return
	}

	approvalStatus := proto_models.ToolCallApprovalStatus_TOOL_CALL_APPROVAL_STATUS_UNSPECIFIED
	execState := proto_models.ToolCallExecutionState_TOOL_CALL_EXECUTION_STATE_UNSPECIFIED
	hidden := false
	payloadResolvedByUserID := resolvedByUserID
	payloadReason := reason
	payloadResolvedAt := resolvedAt

	if resolved.HiddenFromUsers != nil {
		hidden = *resolved.HiddenFromUsers
	}
	if resolved.ToolCall != nil {
		approvalStatus = resolved.ToolCall.ApprovalStatus
		execState = resolved.ToolCall.ExecutionState
		if payloadResolvedByUserID == nil {
			payloadResolvedByUserID = resolved.ToolCall.ResolvedByUserID
		}
		if payloadReason == nil {
			payloadReason = resolved.ToolCall.ResolutionReason
		}
		if resolved.ToolCall.ResolvedAt != nil {
			payloadResolvedAt = *resolved.ToolCall.ResolvedAt
		}
	}

	resolvedAtCopy := payloadResolvedAt
	event := ports.NewToolCallResolutionEvent(&ports.ChatToolCallResolutionEvent{
		MessageID:        common_service_models.ChatMessageIdType(messageID),
		ChannelID:        common_service_models.ChatChannelIdType(channelID),
		ApprovalStatus:   approvalStatus,
		ExecutionState:   execState,
		ResolvedByUserID: payloadResolvedByUserID,
		ResolvedAt:       &resolvedAtCopy,
		ResolutionReason: payloadReason,
		HiddenFromUsers:  hidden,
	}, actorUserIDForEvent(resolvedByUserID), resolvedAt)

	publishCtx := context.WithoutCancel(ctx)
	if err := h.eventPublisher.PublishChatEvent(publishCtx, event); err != nil {
		appLogger.For(publishCtx, h.logger, chatLoggerName).Error("failed to publish tool call resolution event",
			zap.String(logkeys.Stage, "publish_tool_call_resolution"),
			zap.Uint64(appLogger.KeyMessageID, messageID),
			zap.Uint64(appLogger.KeyChannelID, channelID),
			zap.Error(err))
	}
}

// publishTranscriptMessageSent fans out MESSAGE_SENT for a freshly inserted internal transcript row.
// The caller passes a service-transcript read so hidden rows still reach the split-recipient resolver.
func (h *Handler[U]) publishTranscriptMessageSent(
	ctx context.Context,
	message *common_service_models.ChatMessage,
	actorUserID common_service_models.UserIdType,
) {
	if h.eventPublisher == nil || message == nil {
		return
	}
	publishCtx := context.WithoutCancel(ctx)
	if err := h.hydrateMessageAttachments(publishCtx, message); err != nil && h.logger != nil {
		appLogger.For(publishCtx, h.logger, chatLoggerName).Warn("Failed to hydrate transcript message media for realtime publication",
			zap.String(logkeys.Stage, "hydrate_realtime_transcript_media"),
			zap.Uint64(appLogger.KeyMessageID, uint64(message.ID)),
			zap.Error(err))
	}
	if err := h.eventPublisher.PublishChatEvent(publishCtx, &ports.ChatEvent{
		Type:        proto_models.ChatEventType_CHAT_EVENT_TYPE_MESSAGE_SENT,
		OccurredAt:  time.Now(),
		ActorUserID: actorUserID,
		Message:     message,
	}); err != nil {
		appLogger.For(publishCtx, h.logger, chatLoggerName).Error("failed to publish transcript MESSAGE_SENT",
			zap.String(logkeys.Stage, "publish_transcript_message_sent"),
			zap.Uint64(appLogger.KeyMessageID, uint64(message.ID)),
			zap.Uint64(appLogger.KeyChannelID, uint64(message.ChannelID)),
			zap.Error(err))
	}
}

func (h *Handler[U]) reloadServiceTranscriptMessage(ctx context.Context, messageID uint64) *common_service_models.ChatMessage {
	msg, err := h.persistenceHandler.GetChatMessageByIDWithOptions(ctx, messageID, common_persistence.ChatMessageReadOptions{
		VisibilityMode: common_persistence.ChatMessageVisibilityServiceTranscript,
	})
	if err != nil {
		appLogger.For(ctx, h.logger, chatLoggerName).Error("failed to reload persisted transcript message",
			zap.String(logkeys.Stage, "reload_service_transcript_message"),
			zap.Uint64(appLogger.KeyMessageID, messageID),
			zap.String(logkeys.Fallback, "return_nil_message"),
			zap.Error(err))
		return nil
	}
	if msg == nil {
		appLogger.LogInvariantViolation(appLogger.For(ctx, h.logger, chatLoggerName), "persisted_transcript_message_reload_returns_row", nil,
			zap.String(logkeys.Stage, "reload_service_transcript_message"),
			zap.Uint64(appLogger.KeyMessageID, messageID),
			zap.String(logkeys.Expected, "transcript message row"),
			zap.String(logkeys.Actual, "nil transcript message"),
			zap.String(logkeys.Fallback, "return_nil_message"))
		return nil
	}
	converted := h.converter.PersistenceChatMessageToService(msg)
	if converted == nil {
		appLogger.LogInvariantViolation(appLogger.For(ctx, h.logger, chatLoggerName), "transcript_message_conversion_returns_service_message", nil,
			zap.String(logkeys.Stage, "reload_service_transcript_message"),
			zap.Uint64(appLogger.KeyMessageID, messageID),
			zap.String(logkeys.Expected, "service chat message"),
			zap.String(logkeys.Actual, "nil converted message"),
			zap.String(logkeys.Fallback, "return_nil_message"))
	}
	return converted
}

// approvalPostCommitContext isolates background approval work from the request's
// mutable SRouter context and strips the authorities that must not cross the
// transaction boundary: the completed transaction, the approving user, and any
// run-drive lease. A resolver running inside another run's drive carries that
// drive's lease; approved-tool writes are fenced by their per-tool lease instead,
// and a foreign drive lease would reject them as stale work of the wrong run. A
// resumed drive claims its own lease. A tool may later attach the durable
// triggering actor to a further isolated context.
func approvalPostCommitContext[U common_service_models.UserLike](ctx context.Context) context.Context {
	postCtx := agentkit.ContextWithoutActor[U](ctx)
	scontext.ClearTransaction[common_service_models.UserIdType, U](postCtx)
	return common_context.ClearChatAgentRunDriveLease(postCtx)
}

// addApprovalPostCommitHook registers a best-effort side effect on the transaction
// that made the resolution durable. There is deliberately no immediate fallback:
// running before commit would violate the approval boundary. Callers tolerate a
// dropped hook; approved execution additionally remains recoverable from its
// durable queue.
func (h *Handler[U]) addApprovalPostCommitHook(
	ctx context.Context,
	operation string,
	hook func(context.Context),
) {
	if hook == nil {
		return
	}
	if common_persistence.AddPostCommitHookFromContext(ctx, func(hookCtx context.Context) error {
		postCtx := approvalPostCommitContext[U](hookCtx)
		h.runWithPanicRecovery(postCtx, operation, func() {
			hook(postCtx)
		})
		return nil
	}) {
		return
	}
	appLogger.LogInvariantViolation(appLogger.For(ctx, h.logger, chatLoggerName), "transactional_approval_side_effect_has_post_commit_registrar", nil,
		zap.String(logkeys.Stage, "schedule_approval_post_commit_hook"),
		zap.String(logkeys.Operation, operation),
		zap.String(logkeys.Expected, "post-commit hook registrar"),
		zap.String(logkeys.Actual, "transaction without post-commit hooks"),
		zap.String(logkeys.Fallback, "drop_best_effort_side_effect"))
}

// runOwnedApprovalPostCommitHooks starts hooks drained from a transaction created
// by the approval service itself. Middleware-owned transactions have their own
// bounded runner; this keeps the direct service path equally bounded without
// importing the HTTP middleware package into the service layer.
func (h *Handler[U]) runOwnedApprovalPostCommitHooks(ctx context.Context, hooks []common_persistence.PostCommitHook) {
	if len(hooks) == 0 {
		return
	}
	if !h.beginOwnedApprovalPostCommitHookRun() {
		return
	}
	postCtx, cancel := context.WithTimeout(approvalPostCommitContext[U](context.WithoutCancel(ctx)), h.policy().PostCommitHookTimeout())
	go func() {
		defer h.approvalPostCommitWg.Done()
		defer cancel()
		for index, hook := range hooks {
			if hook == nil {
				continue
			}
			hook := hook
			h.runWithPanicRecovery(postCtx, "owned_approval_post_commit_hook", func() {
				if hookErr := hook(postCtx); hookErr != nil {
					appLogger.For(postCtx, h.logger, chatLoggerName).Warn("owned approval post-commit hook failed",
						zap.Int(appLogger.KeyHookIndex, index), zap.Error(hookErr))
				}
			}, zap.Int(appLogger.KeyHookIndex, index))
		}
	}()
}

func (h *Handler[U]) beginOwnedApprovalPostCommitHookRun() bool {
	if h == nil {
		return false
	}
	h.approvalPostCommitMu.Lock()
	defer h.approvalPostCommitMu.Unlock()
	if h.approvalPostCommitStopping {
		return false
	}
	h.approvalPostCommitWg.Add(1)
	return true
}

// stopOwnedApprovalPostCommitHooks prevents new owned hook runners and waits for runners already
// admitted. Their contexts retain the timeout captured at admission rather than being shortened by
// service shutdown.
func (h *Handler[U]) stopOwnedApprovalPostCommitHooks() {
	if h == nil {
		return
	}
	h.approvalPostCommitMu.Lock()
	h.approvalPostCommitStopping = true
	h.approvalPostCommitMu.Unlock()
	h.approvalPostCommitWg.Wait()
}

// nudgeApprovedWork triggers an immediate approved-work claim attempt after the
// approval's post-commit hook fires. The durable row is authoritative: if this
// nudge is missed (process death, no executor yet), the background scanner
// reclaims the work. Best-effort, runs in a detached goroutine so it does not
// block the resolve response.
func (h *Handler[U]) nudgeApprovedWork(ctx context.Context, run *common_persistence_models.ChatAgentRun) {
	if run == nil {
		return
	}
	nudgeCtx := context.WithoutCancel(ctx)
	// The hint performs only a coalesced, bounded queue scan; actual tool execution is detached by
	// the handler-wide scheduler. Keeping the scan call joined prevents an untracked goroutine from
	// outliving the handler while preserving non-blocking tool execution.
	h.dispatchApprovedWorkBackstop(nudgeCtx, run.ChannelID)
}

// maybeNudgeResumeForStep is the WHOLE-STEP resume gate (Decision 6 / parallel-approval data-loss
// guard). It nudges the bot to resume only once EVERY TOOL_CALL in (agent_run_id, agent_step_index)
// has finished — execution_state in (EXECUTED, EXECUTION_FAILED). Denied/expired calls count as
// finished (their synthetic result sets EXECUTED); an approved sibling still pending or executing
// keeps the gate shut. This prevents resuming a half-resolved step, which would leave a sibling
// TOOL_CALL unpaired and dropped by context reconstruction.
//
// When stepIndex is nil (a legacy/manual run with no step index) the gate cannot scope a step, so it
// resumes immediately — single-call legacy runs have no parallel-sibling hazard.
func (h *Handler[U]) maybeNudgeResumeForStep(ctx context.Context, run *common_persistence_models.ChatAgentRun, stepIndex *uint32) {
	if run == nil {
		return
	}
	if stepIndex != nil {
		remaining, err := h.persistenceHandler.CountUnresolvedToolCallsInStep(ctx, run.ID, *stepIndex)
		if err != nil {
			// On a count failure, do NOT resume (avoid resuming a possibly-incomplete step); the
			// background backstop / a later resolution will re-trigger once the step completes.
			appLogger.For(ctx, h.logger, chatLoggerName).Warn("resume gate: failed to count unresolved tool calls; deferring resume",
				zap.Uint64(appLogger.KeyAgentRunID, run.ID), zap.Error(err))
			return
		}
		if remaining > 0 {
			// Step not yet complete: a sibling approval-required call is still pending/executing.
			return
		}
	}
	h.nudgeResume(ctx, run)
}

// durablyCommittedLegacyDenialOrExpiry is the safe nil-step counterpart to the aggregate step
// gate. A legacy call has no step index to count, so an optimistic nudge after a transaction error
// could resume work that actually rolled back. Re-read the exact terminal call, its exact synthetic
// result payload, and the owning RUNNING run on a detached bounded context before nudging.
func (h *Handler[U]) durablyCommittedLegacyDenialOrExpiry(
	ctx context.Context,
	toolCall *common_persistence_models.ChatMessage,
	run *common_persistence_models.ChatAgentRun,
	finalStatus proto_models.ToolCallApprovalStatus,
	resultMessageID uint64,
	expectedContent string,
	expectedPayload []byte,
) bool {
	if h == nil || h.persistenceHandler == nil || toolCall == nil || toolCall.ToolCall == nil ||
		toolCall.ToolCall.ToolCallID == nil || *toolCall.ToolCall.ToolCallID == "" || run == nil ||
		run.ID == 0 || resultMessageID == 0 || len(expectedPayload) == 0 {
		return false
	}
	reconcileCtx, cancel := context.WithTimeout(
		common_persistence.WithoutRequestTransaction[U](context.WithoutCancel(ctx)), approvalCommitReconcileTimeout,
	)
	defer cancel()
	opts := common_persistence.ChatMessageReadOptions{VisibilityMode: common_persistence.ChatMessageVisibilityServiceTranscript}
	call, err := h.persistenceHandler.GetChatMessageByIDWithOptions(reconcileCtx, toolCall.ID, opts)
	if err != nil || call == nil || call.Kind != proto_models.MessageKind_MESSAGE_KIND_TOOL_CALL ||
		call.AgentRunID == nil || *call.AgentRunID != run.ID || call.ToolCall == nil ||
		call.ToolCall.ToolCallID == nil || *call.ToolCall.ToolCallID != *toolCall.ToolCall.ToolCallID ||
		call.ToolCall.ApprovalStatus != finalStatus ||
		call.ToolCall.ExecutionState != proto_models.ToolCallExecutionState_TOOL_CALL_EXECUTION_STATE_EXECUTED {
		return false
	}
	result, err := h.persistenceHandler.GetChatMessageByIDWithOptions(reconcileCtx, resultMessageID, opts)
	if err != nil || result == nil || result.Kind != proto_models.MessageKind_MESSAGE_KIND_TOOL_RESULT ||
		result.ChannelID != run.ChannelID || result.SenderID != run.BotUserID || result.Content != expectedContent ||
		result.Role != proto_models.MessageRole_MESSAGE_ROLE_TOOL || result.AgentRunID == nil || *result.AgentRunID != run.ID ||
		result.ToolCall == nil || result.ToolCall.ToolCallID == nil ||
		*result.ToolCall.ToolCallID != *toolCall.ToolCall.ToolCallID ||
		result.ToolCall.Kind != proto_models.MessageKind_MESSAGE_KIND_TOOL_RESULT ||
		result.ToolCall.PayloadType != proto_models.MessageModelPayloadType_MESSAGE_MODEL_PAYLOAD_TYPE_TOOL_RESULT ||
		result.ToolCall.ExecutionState != proto_models.ToolCallExecutionState_TOOL_CALL_EXECUTION_STATE_EXECUTED ||
		!bytes.Equal(result.ToolCall.ModelPayload, expectedPayload) {
		return false
	}
	durableRun, err := h.persistenceHandler.GetChatAgentRunByID(reconcileCtx, run.ID)
	return err == nil && durableRun != nil && durableRun.Status == proto_models.ChatAgentRunStatus_CHAT_AGENT_RUN_STATUS_RUNNING
}

// finishExecutedToolStep applies the same whole-step gate after a real tool execution, with one
// additional persisted-policy check: when ResumeOnDenial=false and an earlier parallel sibling was
// denied/expired, the last executing sibling must complete the run instead of resuming it. Without
// this aggregate check, resolution order (deny first, execute last) could strand or wrongly resume
// the run.
func (h *Handler[U]) finishExecutedToolStep(ctx context.Context, run *common_persistence_models.ChatAgentRun, stepIndex *uint32) {
	if run == nil {
		return
	}
	if stepIndex == nil {
		h.nudgeResume(ctx, run)
		return
	}
	policy := h.loadRunApprovalPolicy(ctx, run)
	readyToResume := false
	completedAfterDenial := false
	err := h.runOwnedTransaction(ctx, func(txCtx context.Context) error {
		// The denial/expiry path uses the same run-row lock before its aggregate read. Serializing
		// both finalizers closes the execution-vs-denial race where each could otherwise observe the
		// other operation as still unresolved and both defer.
		if err := h.persistenceHandler.LockChatAgentRunForUpdate(txCtx, run.ID); err != nil {
			return serviceErrors.ErrDatabaseError
		}
		remaining, err := h.persistenceHandler.CountUnresolvedToolCallsInStep(txCtx, run.ID, *stepIndex)
		if err != nil {
			return serviceErrors.ErrDatabaseError
		}
		if remaining > 0 {
			return nil
		}
		if !policy.ResumeOnDenial {
			hasDenial, err := h.persistenceHandler.HasDeniedOrExpiredToolCallsInStep(txCtx, run.ID, *stepIndex)
			if err != nil {
				return serviceErrors.ErrDatabaseError
			}
			if hasDenial {
				completedAfterDenial, err = h.persistenceHandler.CompareAndSetChatAgentRunStatus(
					txCtx,
					run.ID,
					[]proto_models.ChatAgentRunStatus{
						proto_models.ChatAgentRunStatus_CHAT_AGENT_RUN_STATUS_WAITING_FOR_APPROVAL,
						proto_models.ChatAgentRunStatus_CHAT_AGENT_RUN_STATUS_RUNNING,
						proto_models.ChatAgentRunStatus_CHAT_AGENT_RUN_STATUS_RETRYABLE,
					},
					proto_models.ChatAgentRunStatus_CHAT_AGENT_RUN_STATUS_COMPLETED,
					proto_models.ChatAgentRunStatusReason_CHAT_AGENT_RUN_STATUS_REASON_APPROVAL_NOT_GRANTED,
				)
				if err != nil {
					return serviceErrors.ErrDatabaseError
				}
				return nil
			}
		}
		readyToResume = true
		return nil
	})
	if err != nil {
		appLogger.For(ctx, h.logger, chatLoggerName).Warn("execution completion gate failed; deferring",
			zap.Uint64(appLogger.KeyAgentRunID, run.ID), zap.Error(err))
		return
	}
	if completedAfterDenial {
		h.publishAgentRunTransitionAfterCommit(ctx, agentRunTransition(run,
			proto_models.ChatAgentRunStatus_CHAT_AGENT_RUN_STATUS_COMPLETED,
			proto_models.ChatAgentRunStatusReason_CHAT_AGENT_RUN_STATUS_REASON_APPROVAL_NOT_GRANTED))
		return
	}
	if !readyToResume {
		return
	}
	h.nudgeResume(ctx, run)
}

// nudgeResume re-enters the agent loop after a step's tool work is fully resolved (approved-tool
// execution, denial, or expiry synthetic result). It drives the run directly through the runtime
// lane's agent-run driver (CommonServiceHandler.RunAgentTurn), which re-reads the durable transcript
// and continues at the next step index.
//
// It deliberately does NOT dispatch a MESSAGE_SENT to the bot: that path self-skips (the dispatcher
// skips when the target bot == the actor, and a resume's actor is the bot), so it never re-entered
// the loop. The user-facing MESSAGE_SENT for the (synthetic or real) TOOL_RESULT is already fanned
// out insert-only when the result row is recorded, so dropping the bot-targeted dispatch does not
// affect user fanout.
func (h *Handler[U]) nudgeResume(ctx context.Context, run *common_persistence_models.ChatAgentRun) {
	if run == nil {
		return
	}
	if h.agentRunDriver == nil {
		if appLogger.DebugEnabled(h.logger) {
			appLogger.For(ctx, h.logger, chatLoggerName).Debug("resume nudge skipped: no agent run driver wired",
				zap.Uint64(appLogger.KeyAgentRunID, run.ID))
		}
		return
	}
	// An owner-driven run resumes in its owner's next drive.
	if h.leaveRunForOwner(ctx, "nudge_resume", run) {
		return
	}
	h.driveAgentRun(ctx, run.ID)
}

// actorUserIDForEvent returns the resolving user for event attribution, or 0 for a system (expiry)
// resolution where there is no human resolver.
func actorUserIDForEvent(resolvedByUserID *uint64) common_service_models.UserIdType {
	if resolvedByUserID == nil {
		return 0
	}
	return common_service_models.UserIdType(*resolvedByUserID)
}
