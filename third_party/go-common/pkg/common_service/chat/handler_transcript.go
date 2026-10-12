package chat

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/Suhaibinator/go-common/pkg/common_service/ports"

	"github.com/Suhaibinator/SRouter/pkg/logkeys"
	"github.com/Suhaibinator/SRouter/pkg/scontext"
	persistence "github.com/Suhaibinator/go-common/pkg/common_persistence"
	common_persistence_models "github.com/Suhaibinator/go-common/pkg/common_persistence/common_persistence_models"
	"github.com/Suhaibinator/go-common/pkg/common_service/common_service_models"
	serviceErrors "github.com/Suhaibinator/go-common/pkg/common_service/errors"
	appLogger "github.com/Suhaibinator/go-common/pkg/logger"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

// PostSystemEventInput is the service-authority input for writing a SYSTEM_EVENT transcript row.
// Apps call PostSystemEvent from their own backend hooks (join/leave/rename, etc.); it does not go
// through the public message send path (Decision 5).
type PostSystemEventInput struct {
	// ChannelID is the channel the event is posted to (required).
	ChannelID uint64
	// SenderID is the actor/bot the event is attributed to. 0 is allowed for a pure system event.
	SenderID uint64
	// ParentMessageID scopes the event to a thread (0 = main timeline).
	ParentMessageID uint64
	// Content is the human-readable event text (e.g. "Alice renamed the channel to ...").
	Content string
	// AgentRunID optionally ties the event to an agent run; when set, the run must belong to the
	// same channel/thread and SenderID must match the run's bot user (persistence validates this).
	AgentRunID *uint64
	// HiddenFromUsers / ExcludeFromAIContext optionally override the per-kind defaults
	// (SYSTEM_EVENT defaults: exclude_from_ai_context=true, hidden_from_users=false). nil = default.
	HiddenFromUsers      *bool
	ExcludeFromAIContext *bool
}

// CompactionSummaryInput is the input for WriteCompactionSummary. It carries the summary checkpoint
// plus the metadata of the model call that produced it (recorded as a purpose=COMPACTION ledger row).
type CompactionSummaryInput struct {
	// CheckpointPayload optionally carries context measurements and verbatim protected state.
	CheckpointPayload *proto_models.CompactionPayload
	// AtomicLedger requires a transaction and rolls back the checkpoint when accounting fails.
	AtomicLedger     bool
	InferenceModelID uint64

	// ChannelID / ParentMessageID locate the compaction scope (Decision 3: per channel+thread).
	ChannelID       uint64
	ParentMessageID uint64
	// AgentRunID is the run the compaction belongs to (transcript tag + ledger grouping). Required.
	AgentRunID uint64
	// BotUserID is the sender attributed to the summary row. Required.
	BotUserID uint64
	// SummaryContent is the summary text itself (the COMPACTION_SUMMARY content / model-visible block).
	SummaryContent string
	// CoversThroughMessageID is the watermark: the summary covers messages with id <= this. Validated
	// against the previous checkpoint and the summary's own id (Decision 3 integrity).
	CoversThroughMessageID uint64
	// SummarizedMessageCount is the number of messages folded into the summary (stored in the payload).
	SummarizedMessageCount uint32
	// VisibleSummaryDivider mirrors AgentCompactionRules.VisibleSummaryDivider: when true the summary
	// is shown as a Claude-Code-style divider (hidden_from_users=false) instead of the hidden default.
	VisibleSummaryDivider bool

	// Inference-call ledger metadata for the model call that produced the summary (purpose=COMPACTION).
	// InferenceCallID is the idempotency key; when empty a deterministic key is synthesized from the
	// run + watermark so a retry does not double-record the ledger row.
	InferenceCallID     string
	ProviderID          string
	ProviderModelID     string
	FinishReason        string
	LatencyMs           int64
	InputTokens         int64
	InputImages         int64
	OutputTokens        int64
	TotalTokens         int64
	ThoughtsTokens      int64
	CachedContentTokens int64
}

// PostSystemEvent writes a MESSAGE_KIND_SYSTEM_EVENT transcript row through the internal transcript
// writer primitive (NOT the public send path). Per the per-kind defaults table it is, by default,
// excluded from AI context, visible to users, and role=SYSTEM; persistence never bumps unread or the
// channel preview for SYSTEM_EVENT rows. This is the Decision 5 server-side authoring surface apps
// use from their backend (join/leave/rename hooks), not over the public HTTP API.
//
// Parameters:
//   - ctx: Request context used for cancellation, deadlines, and tracing.
//   - input: The system-event content + scope (see PostSystemEventInput).
//
// Returns:
//   - result1: The created SYSTEM_EVENT message in service-model form.
//   - result2: ErrChatDisabled / ErrInvalidRequest / ErrDatabaseError (or run-context rejection).
func (h *Handler[U]) PostSystemEvent(ctx context.Context, input PostSystemEventInput) (*common_service_models.ChatMessage, error) {
	if h.chatDisabled() {
		return nil, serviceErrors.ErrChatDisabled
	}
	if input.ChannelID == 0 {
		return nil, serviceErrors.ErrInvalidRequest
	}

	excludeFromAI, hiddenFromUsers := resolveSystemEventVisibility(input.ExcludeFromAIContext, input.HiddenFromUsers)

	in := persistence.CreateTranscriptMessageInput{
		ChannelID:            input.ChannelID,
		SenderID:             input.SenderID,
		ParentMessageID:      parentMessageIDPtr(input.ParentMessageID),
		Content:              input.Content,
		ContentType:          proto_models.MessageContentType_MESSAGE_CONTENT_TYPE_TEXT,
		Kind:                 proto_models.MessageKind_MESSAGE_KIND_SYSTEM_EVENT,
		Role:                 proto_models.MessageRole_MESSAGE_ROLE_SYSTEM,
		ExcludeFromAIContext: excludeFromAI,
		HiddenFromUsers:      hiddenFromUsers,
		AgentRunID:           input.AgentRunID,
		// A bare SYSTEM_EVENT carries no model payload / side row.
		HasToolCall: false,
	}

	messageID, _, err := h.persistenceHandler.CreateTranscriptMessage(ctx, in)
	if err != nil {
		return nil, mapTranscriptWriteServiceError(err)
	}
	return h.reloadTranscriptMessage(ctx, messageID), nil
}

// WriteCompactionSummary persists a MESSAGE_KIND_COMPACTION_SUMMARY checkpoint and the
// purpose=COMPACTION ChatInferenceCall ledger row for the model call that produced it (Decision 3).
// It validates the watermark on write: covers_through must be >= the previous checkpoint's watermark
// (no un-compaction) and must reference an already-persisted message in scope strictly below the new
// summary's own id (the checkpoint cannot claim to cover itself or future rows). The summary is
// hidden by default; VisibleSummaryDivider makes it a visible divider.
//
// Parameters:
//   - ctx: Request context used for cancellation, deadlines, and tracing.
//   - input: The summary + watermark + producing-call metadata (see CompactionSummaryInput).
//
// Returns:
//   - result1: The created COMPACTION_SUMMARY message in service-model form.
//   - result2: ErrCompactionWatermarkInvalid / ErrInvalidRequest / ErrChatAgentRunNotFound /
//     ErrDatabaseError.
func (h *Handler[U]) WriteCompactionSummary(ctx context.Context, input CompactionSummaryInput) (*common_service_models.ChatMessage, error) {
	if input.AtomicLedger {
		if _, ok := persistence.DatabasePostCommitHookRegistrarFromContext(ctx); !ok {
			return nil, serviceErrors.ErrInvalidRequest
		}
	}
	if h.chatDisabled() {
		return nil, serviceErrors.ErrChatDisabled
	}
	if input.ChannelID == 0 || input.AgentRunID == 0 || input.CoversThroughMessageID == 0 {
		return nil, serviceErrors.ErrInvalidRequest
	}

	// Watermark lower bound: must not regress below the previous checkpoint's watermark (Decision 3
	// stacking — the latest checkpoint's watermark only ever moves forward).
	prev, err := h.persistenceHandler.GetLatestCompaction(ctx, input.ChannelID, input.ParentMessageID)
	if err != nil {
		return nil, serviceErrors.ErrDatabaseError
	}
	if prev != nil {
		prevWatermark, watermarkErr := compactionWatermark(prev)
		if watermarkErr != nil {
			return nil, appLogger.InvariantError("chat_compaction_checkpoint_has_valid_watermark", watermarkErr,
				zap.String(logkeys.Stage, "read_previous_compaction_watermark"),
				zap.Uint64(appLogger.KeyChannelID, input.ChannelID),
				zap.Uint64(appLogger.KeyMessageID, prev.ID),
				zap.String(logkeys.Expected, "decodable compaction payload"),
				zap.String(logkeys.Actual, "missing or invalid compaction payload"))
		}
		if input.CoversThroughMessageID < prevWatermark {
			return nil, serviceErrors.ErrCompactionWatermarkInvalid
		}
	}

	// Watermark upper bound: the covered message must already exist in this channel/thread scope. This
	// rejects a watermark that points past the end of the timeline (claiming to cover not-yet-written
	// rows). Because the summary row is inserted AFTER this check, its own id is necessarily greater
	// than any existing covered id, so covers_through < summary.id holds by construction (asserted
	// post-insert below as defense-in-depth).
	covered, err := h.persistenceHandler.GetChatMessageByIDWithOptions(ctx, input.CoversThroughMessageID, persistence.ChatMessageReadOptions{
		VisibilityMode: persistence.ChatMessageVisibilityServiceTranscript,
	})
	if err != nil {
		if errors.Is(err, persistence.ErrChatMessageNotFound) {
			return nil, serviceErrors.ErrCompactionWatermarkInvalid
		}
		return nil, serviceErrors.ErrDatabaseError
	}
	if covered == nil || covered.ChannelID != input.ChannelID ||
		!messageInScope(covered, input.ParentMessageID) {
		return nil, serviceErrors.ErrCompactionWatermarkInvalid
	}

	payload, err := marshalCompactionPayload(input.CheckpointPayload, input.CoversThroughMessageID, input.SummarizedMessageCount)
	if err != nil {
		return nil, serviceErrors.ErrInvalidRequest
	}

	// COMPACTION_SUMMARY defaults hidden_from_users=true; the visible-divider flag forces false.
	hidden := !input.VisibleSummaryDivider
	runID := input.AgentRunID
	summaryIn := persistence.CreateTranscriptMessageInput{
		ChannelID:       input.ChannelID,
		SenderID:        input.BotUserID,
		ParentMessageID: parentMessageIDPtr(input.ParentMessageID),
		Content:         input.SummaryContent,
		ContentType:     proto_models.MessageContentType_MESSAGE_CONTENT_TYPE_TEXT,
		Kind:            proto_models.MessageKind_MESSAGE_KIND_COMPACTION_SUMMARY,
		// Per the per-kind defaults table the summary text is model-visible context (role USER).
		Role:            proto_models.MessageRole_MESSAGE_ROLE_USER,
		HiddenFromUsers: &hidden,
		AgentRunID:      &runID,
		HasToolCall:     true,
		ModelPayload:    payload,
		PayloadType:     proto_models.MessageModelPayloadType_MESSAGE_MODEL_PAYLOAD_TYPE_COMPACTION,
	}

	summaryID, _, err := h.persistenceHandler.CreateTranscriptMessage(ctx, summaryIn)
	if err != nil {
		return nil, mapTranscriptWriteServiceError(err)
	}

	// The pre-insert scope check guarantees that the watermark precedes this new row.
	if input.CoversThroughMessageID >= summaryID {
		return nil, appLogger.InvariantError(
			"chat_compaction_summary_id_after_watermark",
			serviceErrors.ErrCompactionWatermarkInvalid,
			zap.String(logkeys.Stage, "persist_compaction_summary"),
			zap.Uint64(appLogger.KeyChannelID, input.ChannelID),
			zap.Uint64(appLogger.KeyAgentRunID, input.AgentRunID),
			zap.String(logkeys.Expected, "summary_id > covers_through"),
			zap.String(logkeys.Actual, fmt.Sprintf("summary_id=%d covers_through=%d", summaryID, input.CoversThroughMessageID)),
		)
	}

	// Record the producing model call as a purpose=COMPACTION ledger row linked to the summary message
	// (idempotent on call_id). A ledger failure does not unwind the durable summary; it is logged.
	if err := h.recordCompactionInferenceCall(ctx, input, summaryID); err != nil {
		if input.AtomicLedger {
			return nil, err
		}
		appLogger.For(ctx, h.logger, chatLoggerName).Error("wrote compaction summary but failed to record its inference call",
			zap.String(logkeys.Stage, "record_compaction_inference_call"),
			zap.Uint64(appLogger.KeyChannelID, input.ChannelID),
			zap.Uint64(appLogger.KeyAgentRunID, input.AgentRunID),
			zap.Uint64(appLogger.KeySummaryID, summaryID),
			zap.String(appLogger.KeyInferenceCallID, input.InferenceCallID),
			zap.Error(err))
	}

	h.publishChatEventAfterCommit(ctx, func(publishCtx context.Context) *ports.ChatEvent {
		message := h.reloadTranscriptMessage(publishCtx, summaryID)
		if message == nil {
			return nil
		}
		return &ports.ChatEvent{Type: proto_models.ChatEventType_CHAT_EVENT_TYPE_MESSAGE_SENT, Message: message, Created: true, OccurredAt: time.Now()}
	})
	return h.reloadTranscriptMessage(ctx, summaryID), nil
}

// recordCompactionInferenceCall appends the purpose=COMPACTION ledger row for the model call that
// produced a compaction summary, linking it to the summary message. The call_id is the caller's
// idempotency key, or a deterministic synthesized key when omitted so a retry stays idempotent.
func (h *Handler[U]) recordCompactionInferenceCall(ctx context.Context, input CompactionSummaryInput, summaryID uint64) error {
	return RecordCompactionInferenceCallWithEvent[U](ctx, h.persistenceHandler, h.converter, h.eventPublisher, h.logger, input, summaryID)
}

// RecordCompactionInferenceCallWithEvent shares accounting and durable run attribution between
// checkpoint commits and unsuccessful summary attempts. A zero summaryID leaves the call unlinked.
func RecordCompactionInferenceCallWithEvent[U common_service_models.UserLike](
	ctx context.Context, p persistence.CommonPersistenceInterface,
	converter InferenceEventConverter,
	publisher ports.ChatEventPublisher, logger *zap.Logger,
	input CompactionSummaryInput, summaryID uint64,
) error {
	callID := input.InferenceCallID
	if callID == "" {
		callID = fmt.Sprintf("compaction:%d:%d", input.AgentRunID, input.CoversThroughMessageID)
	}
	runID := input.AgentRunID
	row := &common_persistence_models.ChatInferenceCall{
		ChannelID:           input.ChannelID,
		AgentRunID:          &runID,
		CallID:              callID,
		Purpose:             proto_models.InferenceCallPurpose_INFERENCE_CALL_PURPOSE_COMPACTION,
		ProviderID:          input.ProviderID,
		ProviderModelID:     input.ProviderModelID,
		InferenceModelID:    persistence.StampLedgerInferenceModelID(ctx, p, input.ProviderID, input.ProviderModelID),
		FinishReason:        input.FinishReason,
		LatencyMs:           input.LatencyMs,
		InputTokens:         input.InputTokens,
		InputImages:         input.InputImages,
		OutputTokens:        input.OutputTokens,
		TotalTokens:         input.TotalTokens,
		ThoughtsTokens:      input.ThoughtsTokens,
		CachedContentTokens: input.CachedContentTokens,
	}
	if input.InferenceModelID != 0 {
		row.InferenceModelID = &input.InferenceModelID
	}
	if summaryID != 0 {
		row.MessageID = &summaryID
	}
	enrichCompactionInferenceAttribution[U](ctx, p, logger, row)
	return RecordInferenceCallWithEvent(ctx, p, converter, publisher, logger, row)
}

func enrichCompactionInferenceAttribution[U common_service_models.UserLike](ctx context.Context, p persistence.CommonPersistenceInterface, logger *zap.Logger, row *common_persistence_models.ChatInferenceCall) {
	if p == nil || row == nil || row.AgentRunID == nil || *row.AgentRunID == 0 {
		return
	}
	run, err := p.GetChatAgentRunByID(ctx, *row.AgentRunID)
	if err != nil {
		appLogger.For(ctx, logger, chatLoggerName).Warn("failed to enrich compaction inference attribution: load agent run",
			zap.String(logkeys.Stage, "load_agent_run_attribution"),
			zap.String(logkeys.Fallback, "persist_inference_call_without_run_attribution"),
			zap.Uint64(appLogger.KeyChannelID, row.ChannelID),
			zap.Uint64(appLogger.KeyAgentRunID, *row.AgentRunID),
			zap.Error(err))
		return
	}
	if run == nil {
		appLogger.LogInvariantViolation(appLogger.For(ctx, logger, chatLoggerName), "chat_compaction_attribution_run_exists", nil,
			zap.String(logkeys.Stage, "enrich_compaction_inference_attribution"),
			zap.Uint64(appLogger.KeyChannelID, row.ChannelID),
			zap.Uint64(appLogger.KeyAgentRunID, *row.AgentRunID),
			zap.String(logkeys.Expected, "agent run row"),
			zap.String(logkeys.Actual, "nil agent run"),
			zap.String(logkeys.Fallback, "persist_inference_call_without_run_attribution"))
		return
	}
	if run.OrganizationID <= 0 {
		appLogger.LogInvariantViolation(appLogger.For(ctx, logger, chatLoggerName), "chat_agent_run_has_organization", nil,
			zap.String(logkeys.Stage, "enrich_compaction_inference_attribution"),
			zap.Uint64(appLogger.KeyChannelID, row.ChannelID),
			zap.Uint64(appLogger.KeyAgentRunID, *row.AgentRunID),
			zap.String(logkeys.Expected, "organization_id > 0"),
			zap.Int64(logkeys.Actual, run.OrganizationID),
			zap.String(logkeys.Fallback, "persist_inference_call_without_organization_attribution"))
		return
	}
	// The run is the authoritative ownership snapshot for retries and recovery. Never derive the
	// charged organization from the current request actor or the session's live state.
	row.OrganizationID = run.OrganizationID
	if run.TriggeringUserID != nil && *run.TriggeringUserID != 0 {
		triggeringUserID := *run.TriggeringUserID
		row.TriggeringUserID = &triggeringUserID
		row.ActorUserID = &triggeringUserID
	} else if user, found := scontext.GetUser[common_service_models.UserIdType, U](ctx); found && user != nil && (*user).GetID() != 0 {
		actorUserID := uint64((*user).GetID())
		row.ActorUserID = &actorUserID
	}
	if run.AgentSessionID == nil || *run.AgentSessionID == 0 {
		appLogger.LogInvariantViolation(appLogger.For(ctx, logger, chatLoggerName), "chat_agent_run_has_agent_session", nil,
			zap.String(logkeys.Stage, "enrich_compaction_inference_attribution"),
			zap.Uint64(appLogger.KeyChannelID, row.ChannelID),
			zap.Uint64(appLogger.KeyAgentRunID, *row.AgentRunID),
			zap.String(logkeys.Expected, "agent_session_id > 0"),
			zap.String(logkeys.Actual, "missing agent_session_id"),
			zap.String(logkeys.Fallback, "persist_inference_call_without_session_attribution"))
		return
	}
	sessionID := *run.AgentSessionID
	row.AgentSessionID = &sessionID
	if run.AgentSessionConfigVersion != nil && *run.AgentSessionConfigVersion != 0 {
		configVersion := *run.AgentSessionConfigVersion
		row.AgentSessionConfigVersion = &configVersion
	}
	session, err := p.GetChatAgentSessionByID(ctx, sessionID)
	if err != nil {
		appLogger.For(ctx, logger, chatLoggerName).Warn("failed to enrich compaction inference attribution: load agent session",
			zap.String(logkeys.Stage, "load_agent_session_attribution"),
			zap.String(logkeys.Fallback, "persist_inference_call_without_agent_type"),
			zap.Uint64(appLogger.KeyChannelID, row.ChannelID),
			zap.Uint64(appLogger.KeyAgentRunID, *row.AgentRunID),
			zap.Uint64(appLogger.KeyAgentSessionID, sessionID),
			zap.Error(err))
		return
	}
	if session == nil {
		appLogger.LogInvariantViolation(appLogger.For(ctx, logger, chatLoggerName), "chat_compaction_attribution_session_exists", nil,
			zap.String(logkeys.Stage, "enrich_compaction_inference_attribution"),
			zap.Uint64(appLogger.KeyChannelID, row.ChannelID),
			zap.Uint64(appLogger.KeyAgentRunID, *row.AgentRunID),
			zap.Uint64(appLogger.KeyAgentSessionID, sessionID),
			zap.String(logkeys.Expected, "agent session row"),
			zap.String(logkeys.Actual, "nil agent session"),
			zap.String(logkeys.Fallback, "persist_inference_call_without_agent_type"))
		return
	}
	row.AgentType = session.AgentType
	if session.InferenceActorUserID != nil {
		row.ActorUserID = new(*session.InferenceActorUserID)
	}
}

// reloadTranscriptMessage re-reads a freshly-written transcript row (service-transcript visibility so
// hidden rows like a default compaction summary are returned) and maps it to the service model.
// Returns nil on failure — the caller's response carries a nil message rather than a stale view.
func (h *Handler[U]) reloadTranscriptMessage(ctx context.Context, messageID uint64) *common_service_models.ChatMessage {
	return h.reloadServiceTranscriptMessage(ctx, messageID)
}

// resolveSystemEventVisibility applies the SYSTEM_EVENT per-kind defaults (exclude_from_ai_context
// = true, hidden_from_users = false) unless the caller overrides them, using the single persistence
// authority so the defaults are not duplicated here.
func resolveSystemEventVisibility(excludeOverride, hiddenOverride *bool) (excludeFromAIContext, hiddenFromUsers *bool) {
	excludeDefault, hiddenDefault := persistence.KindVisibilityDefaults(proto_models.MessageKind_MESSAGE_KIND_SYSTEM_EVENT)
	return persistence.ApplyKindVisibilityDefault(excludeOverride, excludeDefault),
		persistence.ApplyKindVisibilityDefault(hiddenOverride, hiddenDefault)
}

// parentMessageIDPtr maps a uint64 thread scope (0 = main timeline) to the nullable column form the
// transcript input expects (nil for main timeline, so it stores NULL like the rest of the chat path).
func parentMessageIDPtr(parentMessageID uint64) *uint64 {
	if parentMessageID == 0 {
		return nil
	}
	return &parentMessageID
}

// parentScopeOf collapses a message's nullable parent_message_id (nil/0 = main timeline) to the
// canonical uint64 thread scope for comparison against an input scope.
func parentScopeOf(msg *common_persistence_models.ChatMessage) uint64 {
	if msg == nil || msg.ParentMessageID == nil {
		return 0
	}
	return *msg.ParentMessageID
}

// messageInScope reports whether msg is part of the transcript read at the given thread scope. It
// mirrors applyChatMessageThreadScope in persistence: the main timeline (0) also includes thread
// replies broadcast to the channel, so a main-timeline window can end on a broadcast row and that
// row must be accepted as its watermark.
func messageInScope(msg *common_persistence_models.ChatMessage, parentMessageID uint64) bool {
	if msg == nil {
		return false
	}
	scope := parentScopeOf(msg)
	if scope == parentMessageID {
		return true
	}
	return parentMessageID == 0 && msg.BroadcastToChannel
}

// compactionWatermark reads the covers_through watermark from a COMPACTION_SUMMARY message's payload
// side row. A previously persisted checkpoint without a valid compaction payload violates the
// transcript contract, so callers must not silently loosen the regression check.
func compactionWatermark(msg *common_persistence_models.ChatMessage) (uint64, error) {
	if msg == nil || msg.ToolCall == nil || len(msg.ToolCall.ModelPayload) == 0 {
		return 0, errors.New("compaction checkpoint payload is missing")
	}
	var mp proto_models.MessageModelPayload
	if err := proto.Unmarshal(msg.ToolCall.ModelPayload, &mp); err != nil {
		return 0, fmt.Errorf("decode compaction checkpoint payload: %w", err)
	}
	if c := mp.GetCompaction(); c != nil {
		return c.GetCoversThroughMessageId(), nil
	}
	return 0, errors.New("compaction checkpoint payload has no compaction value")
}

// marshalCompactionPayload serializes the CompactionPayload (watermark + summarized count) into the
// model-payload blob, deterministically, mirroring the other transcript payload marshalers.
func marshalCompactionPayload(checkpoint *proto_models.CompactionPayload, coversThroughMessageID uint64, summarizedCount uint32) ([]byte, error) {
	// The validated watermark and count are authoritative over whatever a checkpoint carried.
	compaction := &proto_models.CompactionPayload{}
	if checkpoint != nil {
		compaction = proto.Clone(checkpoint).(*proto_models.CompactionPayload)
	}
	compaction.CoversThroughMessageId, compaction.SummarizedMessageCount = coversThroughMessageID, summarizedCount
	return proto.MarshalOptions{Deterministic: true}.Marshal(&proto_models.MessageModelPayload{
		Payload: &proto_models.MessageModelPayload_Compaction{Compaction: compaction},
	})
}

// mapTranscriptWriteServiceError normalizes persistence transcript-write errors into the chat
// service error vocabulary for the PostSystemEvent / compaction facades.
func mapTranscriptWriteServiceError(err error) error {
	switch {
	case errors.Is(err, persistence.ErrChatTranscriptRunMismatch):
		return serviceErrors.ErrChatAgentRunNotFound
	case errors.Is(err, persistence.ErrChatToolResultOrphan),
		errors.Is(err, persistence.ErrChatToolCallConflict),
		errors.Is(err, persistence.ErrChatTranscriptPayloadInvalid):
		return serviceErrors.ErrInvalidRequest
	default:
		return serviceErrors.ErrDatabaseError
	}
}
