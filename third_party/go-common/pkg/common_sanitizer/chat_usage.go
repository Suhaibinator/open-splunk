package common_sanitizer

import (
	"context"
	"fmt"
	"strings"

	"github.com/Suhaibinator/SRouter/pkg/logkeys"
	"github.com/Suhaibinator/go-common/pkg/common_util"
	appLogger "github.com/Suhaibinator/go-common/pkg/logger"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
	"go.uber.org/zap"
)

// maxResolutionReasonLength bounds the optional human-readable approve/deny reason.
const maxResolutionReasonLength = 1024

// SanitizeGetChatInferenceTotalsRequest validates a channel-totals usage read.
func (h *Handler) SanitizeGetChatInferenceTotalsRequest(ctx context.Context, req *proto_models.GetChatInferenceTotalsRequest) (*proto_models.GetChatInferenceTotalsRequest, error) {
	const stage = "sanitize_get_chat_inference_totals_request"
	sanitized, err := guardRequest(req, requiredID(req.GetChannelId(), ErrChannelIDRequired))
	if err != nil {
		return nil, h.reject(ctx, stage, err)
	}
	return sanitized, nil
}

// SanitizeGetAgentSessionUsageRequest validates an agent-session usage read.
func (h *Handler) SanitizeGetAgentSessionUsageRequest(ctx context.Context, req *proto_models.GetAgentSessionUsageRequest) (*proto_models.GetAgentSessionUsageRequest, error) {
	const stage = "sanitize_get_agent_session_usage_request"
	if _, err := guardRequest(req, requiredID(req.GetAgentSessionId(), ErrAgentSessionIDRequired)); err != nil {
		return nil, h.reject(ctx, stage, err)
	}
	policy := h.requestPolicy()
	var err error
	byRunLimit := req.ByRunPagination.GetLimit()
	req.ByRunPagination, err = sanitizePaginationRequest(req.ByRunPagination, policy.pagination)
	if err != nil {
		return nil, h.reject(ctx, stage, err,
			zap.String("field", "by_run_pagination"),
			zap.Uint32(logkeys.Limit, byRunLimit))
	}
	byModelLimit := req.ByModelPagination.GetLimit()
	req.ByModelPagination, err = sanitizePaginationRequest(req.ByModelPagination, policy.pagination)
	if err != nil {
		return nil, h.reject(ctx, stage, err,
			zap.String("field", "by_model_pagination"),
			zap.Uint32(logkeys.Limit, byModelLimit))
	}
	return req, nil
}

// SanitizeListAgentRunInferenceCallsRequest validates a paginated run-ledger read.
func (h *Handler) SanitizeListAgentRunInferenceCallsRequest(ctx context.Context, req *proto_models.ListAgentRunInferenceCallsRequest) (*proto_models.ListAgentRunInferenceCallsRequest, error) {
	const stage = "sanitize_list_agent_run_inference_calls_request"
	sanitized, err := guardRequest(req, requiredID(req.GetAgentRunId(), ErrAgentRunIDRequired))
	if err != nil {
		return nil, h.reject(ctx, stage, err)
	}
	policy := h.requestPolicy()
	limit := sanitized.Pagination.GetLimit()
	if sanitized.Pagination, err = sanitizePaginationRequest(sanitized.Pagination, policy.pagination); err != nil {
		return nil, h.reject(ctx, stage, err, zap.String("field", "pagination"), zap.Uint32(logkeys.Limit, limit))
	}
	return sanitized, nil
}

// SanitizeGetAgentRunStatusRequest validates a thread-scoped agent-run status read.
func (h *Handler) SanitizeGetAgentRunStatusRequest(ctx context.Context, req *proto_models.GetAgentRunStatusRequest) (*proto_models.GetAgentRunStatusRequest, error) {
	const stage = "sanitize_get_agent_run_status_request"
	sanitized, err := guardRequest(req, requiredID(req.GetChannelId(), ErrChannelIDRequired))
	if err != nil {
		return nil, h.reject(ctx, stage, err)
	}
	// The bounds are the hot chat_agent_run_status_max_locations / _max_history keys, sampled once
	// for this request; the chat service enforces the same keys again for direct callers.
	policy := h.requestPolicy()
	if len(sanitized.ParentMessageIds) > policy.chat.MaxAgentRunStatusLocations {
		return nil, h.reject(ctx, stage, ErrTooManyAgentRunStatusLocations,
			zap.Int("parent_message_id_count", len(sanitized.ParentMessageIds)))
	}
	if sanitized.HistoryLimit > uint32(policy.chat.MaxAgentRunStatusHistory) {
		return nil, h.reject(ctx, stage, ErrAgentRunStatusHistoryLimitTooLarge,
			zap.Uint32(logkeys.Limit, sanitized.HistoryLimit))
	}
	return sanitized, nil
}

// SanitizeListChannelAgentSessionsRequest validates a channel agent-session listing. The service
// caps the result itself, so the channel id is the only input to check.
func (h *Handler) SanitizeListChannelAgentSessionsRequest(ctx context.Context, req *proto_models.ListChannelAgentSessionsRequest) (*proto_models.ListChannelAgentSessionsRequest, error) {
	const stage = "sanitize_list_channel_agent_sessions_request"
	sanitized, err := guardRequest(req, requiredID(req.GetChannelId(), ErrChannelIDRequired))
	if err != nil {
		return nil, h.reject(ctx, stage, err)
	}
	return sanitized, nil
}

// SanitizeGetMessageInferenceSummaryRequest validates a single-message usage summary read.
func (h *Handler) SanitizeGetMessageInferenceSummaryRequest(ctx context.Context, req *proto_models.GetMessageInferenceSummaryRequest) (*proto_models.GetMessageInferenceSummaryRequest, error) {
	const stage = "sanitize_get_message_inference_summary_request"
	sanitized, err := guardRequest(req, requiredID(req.GetMessageId(), ErrChatMessageIDRequired))
	if err != nil {
		return nil, h.reject(ctx, stage, err)
	}
	return sanitized, nil
}

// SanitizeBatchGetMessageInferenceSummariesRequest validates a batched per-message usage read.
func (h *Handler) SanitizeBatchGetMessageInferenceSummariesRequest(ctx context.Context, req *proto_models.BatchGetMessageInferenceSummariesRequest) (*proto_models.BatchGetMessageInferenceSummariesRequest, error) {
	const stage = "sanitize_batch_get_message_inference_summaries_request"
	if req == nil {
		return nil, h.reject(ctx, stage, ErrInvalidRequest)
	}
	if req.ChannelId == 0 {
		return nil, h.reject(ctx, stage, ErrChannelIDRequired)
	}
	req.MessageIds = common_util.DeduplicateNonZero(req.MessageIds)
	if len(req.MessageIds) == 0 {
		return nil, h.reject(ctx, stage, ErrNoMessageIDsProvided,
			zap.Uint64(appLogger.KeyChannelID, req.ChannelId))
	}
	policy := h.requestPolicy()
	if len(req.MessageIds) > policy.chat.MaxBatchGetMessages {
		return nil, h.reject(ctx, stage, ErrTooManyBatchMessages,
			zap.Uint64(appLogger.KeyChannelID, req.ChannelId),
			zap.Int(appLogger.KeyMessageCount, len(req.MessageIds)),
			zap.Int("max_batch_messages", policy.chat.MaxBatchGetMessages))
	}
	return req, nil
}

// SanitizeStopAgentRunRequest validates a Stop of one agent run.
func (h *Handler) SanitizeStopAgentRunRequest(ctx context.Context, req *proto_models.StopAgentRunRequest) (*proto_models.StopAgentRunRequest, error) {
	const stage = "sanitize_stop_agent_run_request"
	sanitized, err := guardRequest(req, requiredID(req.GetAgentRunId(), ErrAgentRunIDRequired))
	if err != nil {
		return nil, h.reject(ctx, stage, err)
	}
	return sanitized, nil
}

// SanitizeResumeAgentSessionRequest validates a Resume of one agent session.
func (h *Handler) SanitizeResumeAgentSessionRequest(ctx context.Context, req *proto_models.ResumeAgentSessionRequest) (*proto_models.ResumeAgentSessionRequest, error) {
	const stage = "sanitize_resume_agent_session_request"
	sanitized, err := guardRequest(req, requiredID(req.GetAgentSessionId(), ErrAgentSessionIDRequired))
	if err != nil {
		return nil, h.reject(ctx, stage, err)
	}
	return sanitized, nil
}

// SanitizeListAgentQueuedInputsRequest validates a queue-tray read of one agent session's queued
// input, which pages by cursor only: a page number past the first is rejected.
func (h *Handler) SanitizeListAgentQueuedInputsRequest(ctx context.Context, req *proto_models.ListAgentQueuedInputsRequest) (*proto_models.ListAgentQueuedInputsRequest, error) {
	const stage = "sanitize_list_agent_queued_inputs_request"
	if _, err := guardRequest(req, requiredID(req.GetAgentSessionId(), ErrAgentSessionIDRequired)); err != nil {
		return nil, h.reject(ctx, stage, err)
	}
	if page := req.Pagination.GetPage(); page > 1 {
		return nil, h.reject(ctx, stage, ErrInvalidRequest, zap.String("field", "pagination.page"), zap.Uint32("page", page))
	}
	policy := h.requestPolicy()
	limit := req.Pagination.GetLimit()
	var err error
	if req.Pagination, err = sanitizePaginationRequest(req.Pagination, policy.pagination); err != nil {
		return nil, h.reject(ctx, stage, err, zap.String("field", "pagination"), zap.Uint32(logkeys.Limit, limit))
	}
	return req, nil
}

// SanitizeEditAgentQueuedInputRequest validates a rewording of a queued agent input. Its content
// is sanitized and bounded like a message edit's. Content may be empty here: the input's kept
// embeds can carry it, which the service checks against the stored input.
func (h *Handler) SanitizeEditAgentQueuedInputRequest(ctx context.Context, req *proto_models.EditAgentQueuedInputRequest) (*proto_models.EditAgentQueuedInputRequest, error) {
	const stage = "sanitize_edit_agent_queued_input_request"
	if _, err := guardRequest(req, requiredID(req.GetQueuedInputId(), ErrAgentQueuedInputIDRequired)); err != nil {
		return nil, h.reject(ctx, stage, err)
	}
	policy := h.requestPolicy()
	req.Content = h.sanitizePlainText(req.Content)
	if len(req.Content) > policy.chat.MaxMessageLength {
		return nil, h.reject(ctx, stage, ErrMessageContentTooLong,
			zap.Uint64(appLogger.KeyQueuedInputID, req.QueuedInputId),
			zap.Int("content_length", len(req.Content)),
			zap.Int("max_message_length", policy.chat.MaxMessageLength))
	}
	if len(req.Attachments) > policy.chat.MaxEmbedsPerMessage {
		return nil, h.reject(ctx, stage, ErrTooManyEmbeds,
			zap.Uint64(appLogger.KeyQueuedInputID, req.QueuedInputId),
			zap.Int(appLogger.KeyAttachmentCount, len(req.Attachments)),
			zap.Int("max_embeds_per_message", policy.chat.MaxEmbedsPerMessage))
	}
	if len(req.Attachments) > 0 {
		if err := h.SanitizeAttachmentMetadata(ctx, req.Attachments); err != nil {
			return nil, fmt.Errorf("attachments: %w", err)
		}
	}
	return req, nil
}

// SanitizeRemoveAgentQueuedInputRequest validates a withdrawal of one queued agent input.
func (h *Handler) SanitizeRemoveAgentQueuedInputRequest(ctx context.Context, req *proto_models.RemoveAgentQueuedInputRequest) (*proto_models.RemoveAgentQueuedInputRequest, error) {
	const stage = "sanitize_remove_agent_queued_input_request"
	sanitized, err := guardRequest(req, requiredID(req.GetQueuedInputId(), ErrAgentQueuedInputIDRequired))
	if err != nil {
		return nil, h.reject(ctx, stage, err)
	}
	return sanitized, nil
}

// SanitizeResolveToolCallRequest validates a tool-call approval resolution. The optional reason is
// trimmed, sanitized, and length-capped.
func (h *Handler) SanitizeResolveToolCallRequest(ctx context.Context, req *proto_models.ResolveToolCallRequest) (*proto_models.ResolveToolCallRequest, error) {
	const stage = "sanitize_resolve_tool_call_request"
	if req == nil {
		return nil, h.reject(ctx, stage, ErrInvalidRequest)
	}
	if req.MessageId == 0 {
		return nil, h.reject(ctx, stage, ErrChatMessageIDRequired)
	}
	if req.Reason != nil {
		reason := strings.TrimSpace(h.sanitizeText(strings.TrimSpace(*req.Reason)))
		if len(reason) > maxResolutionReasonLength {
			return nil, h.reject(ctx, stage, ErrResolutionReasonTooLong,
				zap.Uint64(appLogger.KeyMessageID, req.MessageId),
				zap.Int("reason_length", len(reason)),
				zap.Int("max_reason_length", maxResolutionReasonLength))
		}
		if reason == "" {
			req.Reason = nil
		} else {
			req.Reason = &reason
		}
	}
	return req, nil
}
