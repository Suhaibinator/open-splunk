package ai

import (
	"context"
	json "encoding/json/v2"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/Suhaibinator/go-common/pkg/common_service/common_service_models"
	"github.com/Suhaibinator/go-common/pkg/common_service/ports"
	"github.com/Suhaibinator/go-common/pkg/genai/partdata"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
	genkitai "github.com/firebase/genkit/go/ai"
	"go.uber.org/zap"
)

// errTranscriptReaderRequired is returned when GetContextWindow is called without a reader.
var errTranscriptReaderRequired = errors.New("ai context: transcript reader is required")

// defaultTokenBudget is used when GetContextWindowInput.TokenBudget is unset (<=0).
const defaultTokenBudget = 128000

// TranscriptReader is the internal, hidden-inclusive read seam the context builder uses to
// reconstruct an agent's LLM view. It returns service-model rows already converted from
// persistence (so the ai package never imports persistence), read with the
// ServiceTranscript visibility mode — compaction summaries and agent notes are included,
// not filtered. The chat/service handler supplies the concrete implementation backed by
// GetLatestCompaction + StreamChatMessagesAfter.
type TranscriptReader interface {
	// GetLatestCompaction returns the most recent COMPACTION_SUMMARY checkpoint for the
	// channel/thread scope, or nil when none exists.
	GetLatestCompaction(ctx context.Context, channelID uint64, parentMessageID uint64) (*common_service_models.ChatMessage, error)
	// StreamMessagesAfter returns transcript rows with id > afterMessageID in ascending id
	// order, looping past the public pagination cap until the scope is exhausted or the
	// accumulator signals stop. Rows carry the agentic fields and the ToolCall side row.
	StreamMessagesAfter(ctx context.Context, channelID uint64, parentMessageID uint64, afterMessageID uint64, fn func(batch []*common_service_models.ChatMessage) (stop bool, err error)) error
}

// SystemPromptProvider supplies the system prompt to prepend for an agent run. It is
// loaded from the immutable effective system prompt stored on the session
// at build time. Nil means no system prompt is prepended (legacy/manual bot runs).
type SystemPromptProvider func(ctx context.Context) (string, error)

// GetContextWindowInput parameterizes an internal context-window read. It is not a public
// HTTP/proto API; only server-side bot/service callers construct it.
type GetContextWindowInput struct {
	ChannelID       uint64
	ParentMessageID uint64
	// AgentRunID, when non-zero, is used only for reporting/scope correlation; reconstruction
	// groups by each row's own (agent_run_id, agent_step_index).
	AgentRunID uint64
}

// GetContextWindowResult is the raw result of an internal context-window read: the latest
// compaction checkpoint (if any) and the non-checkpoint transcript rows after its watermark.
type GetContextWindowResult struct {
	Compaction *common_service_models.ChatMessage
	Messages   []*common_service_models.ChatMessage
}

// GetContextWindow reads the hidden-inclusive transcript window for an agent run: the latest
// compaction checkpoint in scope and every message after its watermark, looping past the
// public pagination cap. It performs no reconstruction or budgeting — BuildAgentContext does
// that on top of this read. Splitting the read from the reconstruction keeps each unit
// testable.
func GetContextWindow(ctx context.Context, reader TranscriptReader, in GetContextWindowInput) (*GetContextWindowResult, error) {
	if reader == nil {
		return nil, errTranscriptReaderRequired
	}
	if in.ChannelID == 0 {
		return nil, errors.New("ai context: channel id is required")
	}

	compaction, err := reader.GetLatestCompaction(ctx, in.ChannelID, in.ParentMessageID)
	if err != nil {
		return nil, err
	}

	var afterMessageID uint64
	if compaction != nil {
		afterMessageID = compactionWatermark(compaction)
	}

	messages := make([]*common_service_models.ChatMessage, 0, 64)
	err = reader.StreamMessagesAfter(ctx, in.ChannelID, in.ParentMessageID, afterMessageID, func(batch []*common_service_models.ChatMessage) (bool, error) {
		for _, msg := range batch {
			// Checkpoints are control records, represented only by the selected anchor.
			// Their IDs follow their coverage watermarks, so the stream can include the
			// anchor itself and older stacked checkpoints. Keep the watermark cursor:
			// advancing to the anchor's ID would lose intervening conversation messages.
			if msg != nil && msg.Kind == proto_models.MessageKind_MESSAGE_KIND_COMPACTION_SUMMARY {
				continue
			}
			messages = append(messages, msg)
		}
		return false, nil
	})
	if err != nil {
		return nil, err
	}

	return &GetContextWindowResult{Compaction: compaction, Messages: messages}, nil
}

// NextStepIndex returns the step index a resumed run should generate at: one past the highest
// agent_step_index recorded for agentRunID in the given window. This is the invariant that keeps
// the no-dedup assistant-text property safe — a resume never re-Generates a step whose rows
// (including its assistant text) already exist. With no recorded rows for the run it returns 0
// (a fresh first step).
func NextStepIndex(messages []*common_service_models.ChatMessage, agentRunID uint64) uint32 {
	var maxStep uint32
	found := false
	for _, msg := range messages {
		if msg == nil || msg.AgentRunID == nil || *msg.AgentRunID != agentRunID || msg.AgentStepIndex == nil {
			continue
		}
		if !found || *msg.AgentStepIndex > maxStep {
			maxStep = *msg.AgentStepIndex
			found = true
		}
	}
	if !found {
		return 0
	}
	return maxStep + 1
}

// compactionWatermark returns the covers_through_message_id watermark from a compaction
// summary message's payload, zero included: a fork whose copied covered prefix is gone
// covers nothing. The message's own id is the fallback only when the payload cannot be
// decoded, so the checkpoint row itself is not re-emitted.
func compactionWatermark(msg *common_service_models.ChatMessage) uint64 {
	if msg == nil {
		return 0
	}
	if payload := decodeCompactionPayload(msg); payload != nil {
		return payload.GetCoversThroughMessageId()
	}
	if msg.ID > 0 {
		return uint64(msg.ID)
	}
	return 0
}

// decodeCompactionPayload extracts a CompactionPayload from a COMPACTION_SUMMARY row, or nil.
func decodeCompactionPayload(msg *common_service_models.ChatMessage) *proto_models.CompactionPayload {
	if p := decodeModelPayload(msg, proto_models.MessageModelPayloadType_MESSAGE_MODEL_PAYLOAD_TYPE_COMPACTION); p != nil {
		return p.GetCompaction()
	}
	return nil
}

// AgentContextOptions tunes context reconstruction and budgeting.
type AgentContextOptions struct {
	// RequiredMessageID is the unconsumed trigger of a delegated run. Generation fails unless
	// this row survives selection or is covered by the selected checkpoint. Unlike pending
	// wake intent it remains required after the session wake fields have been drained.
	RequiredMessageID uint64
	// RequiredMessageIDs are unconsumed continue-send receipts in this generation's snapshot.
	// All must survive selection or explicit checkpoint coverage before advancing consumption.
	RequiredMessageIDs []uint64
	// ConsumedPendingMessageID is the last successfully sealed wake-input watermark. A pending
	// message at or below it no longer needs acknowledgement in a later generation. Only delegated
	// child instructions have mandatory retention; main-timeline broadcasts trim normally.
	ConsumedPendingMessageID uint64
	// PendingMessageID is sampled after the window read for this generation; only retained input or
	// checkpoint coverage may confirm it. It is never inferred from later transcript IDs.
	PendingMessageID           uint64
	OrchestratorBotUserID      uint64
	preserveLatestUserAndState bool
	// summaryProjection omits provider-only replay and reasoning before source budgeting.
	summaryProjection bool
	// PreserveHistory rejects overflow instead of silently removing transcript rows.
	PreserveHistory bool
	// AssessOnly measures before hydration; use only for preflight, never for generation.
	AssessOnly bool
	ToolTokens int

	// Logger emits DEBUG context selection diagnostics; nil disables logging.
	Logger                        *zap.Logger
	CurrentAPI                    proto_models.InferenceAPI
	CurrentModelID                string
	CurrentDestinationFingerprint string
	// MaxTokens is the token budget for the reconstructed window. <=0 uses defaultTokenBudget.
	MaxTokens int
	// TokenEstimator estimates the token cost of a set of genkit parts. nil uses a heuristic:
	// chars/4 for text, measured payload bytes for tool request/response, and 2048 per image.
	TokenEstimator TokenEstimator
	// SystemPrompt, when set, is prepended as a system-role message before the transcript.
	SystemPrompt SystemPromptProvider
	// LastKnownInputTokens is the latest available main-call input measurement. A paid failed
	// call can also supply it. 0 means unknown; summary-call usage is never a main-context baseline.
	LastKnownInputTokens        int
	LastMeasuredEstimate        int
	LastInferenceCallID         string
	LastObservationCompactionID uint64
	// CompactionEnabled enables recommendation signals. It does not compact by itself.
	CompactionEnabled bool
	// CompactAtInputTokens is the threshold from the run's captured compaction policy.
	CompactAtInputTokens int
	// TargetSummaryTokens is echoed for callers that choose to run a compaction helper.
	TargetSummaryTokens int
	// ManualCompactionOnly means the threshold signal should be surfaced but not auto-run.
	ManualCompactionOnly bool
	// CurrentAgentRunID scopes step-bound AGENT_NOTE rows (reasoning and recovery instructions).
	// Authoritative USER workflow notes without a step remain visible across runs.
	CurrentAgentRunID uint64
	// CurrentBotUserID is the bot driving the current run. A run-tagged STANDARD row another bot
	// wrote (a delegated child's report, closing message, or notice broadcast into this window)
	// reconstructs as a user-role message prefixed with its sender label instead of joining a
	// model turn. Zero keeps the provider-agnostic grouping used by legacy and custom callers.
	CurrentBotUserID uint64
	// CurrentProviderID identifies the provider selected for the current run. OpenAI Responses
	// provider state is restored only for OpenAI; UNSPECIFIED preserves the provider-agnostic
	// behavior used by legacy and custom callers.
	CurrentProviderID string
	// ProviderStateResolver loads a privileged internal attachment by ID. It is required when
	// OpenAI provider-state rows are present; reconstruction fails closed if state cannot be
	// loaded and verified.
	ProviderStateResolver ProviderStateResolver
	// ToolManifestSHA256 must match provider state captured in the current run. Historical
	// OpenAI output items retain the manifest from the turn that produced them.
	ToolManifestSHA256 string
	// ToolMediaResolver resolves one private attachment-backed multipart tool result. It is required
	// whenever a persisted MediaPart carries attachment_id; reconstruction fails closed otherwise.
	ToolMediaResolver ToolMediaResolver
	// MaxHydratedBytes caps the cumulative persisted bytes represented by provider state and
	// attachment-backed tool media retained in one reconstructed context. Zero uses the secure
	// default. The cap applies even when tool media is represented by a short presigned URL.
	MaxHydratedBytes uint64
}

// ProviderStateResolver resolves bytes for one internal provider-state attachment. ownerUserID is
// the originating transcript row's sender, not necessarily the bot driving the current run.
type ProviderStateResolver func(ctx context.Context, attachmentID, ownerUserID, agentRunID uint64, agentStepIndex uint32, maxBytes uint64) ([]byte, error)

// ToolMediaResolver returns a provider-consumable URL or data URI after validating ownership and
// the deterministic run/call/part binding of an internal tool-media attachment. ownerUserID is the
// originating TOOL_RESULT row's sender, not necessarily the bot driving the current run.
type ToolMediaResolver func(ctx context.Context, attachmentID, ownerUserID, agentRunID uint64, toolCallID string, partIndex int, contentType, filename string, maxBytes uint64) (*ports.ResolvedToolMedia, error)

// TokenEstimator estimates the token cost of a slice of genkit parts.
type TokenEstimator func(parts []*genkitai.Part) int

// AgentContextResult is the reconstructed, budgeted LLM context plus the signals apps need
// to make compaction decisions.
type AgentContextResult struct {
	ConsumedPendingMessageID uint64
	CandidateEstimatedTokens int
	Pressure                 []string
	DroppedMessageIDs        []uint64

	// Messages is the reconstructed genkit history: optional system prompt, optional
	// compaction summary, then the budgeted transcript messages.
	Messages []*genkitai.Message
	// EstimatedTokens is the estimator's total over the emitted messages.
	EstimatedTokens int
	// LastKnownInputTokens is echoed from options (the canonical context-size signal).
	LastKnownInputTokens int
	// DroppedMessages counts whole transcript messages dropped under budget pressure.
	DroppedMessages int
	// CompactionMessageID is the id of the anchoring compaction summary, or 0.
	CompactionMessageID uint64
	// Truncated is true when any message content was truncated or dropped for budget.
	Truncated bool
	// HydratedBytes is the cumulative persisted size of privileged blobs represented in Messages.
	HydratedBytes uint64
	// CompactionRecommendation reports whether the context has crossed the captured compaction
	// threshold. It is advisory: callers still own the summarization prompt and write helper call.
	CompactionRecommendation CompactionRecommendation
}

// CompactionRecommendation is the context builder's policy-neutral compaction signal.
type CompactionRecommendation struct {
	ShouldCompact       bool
	ManualOnly          bool
	ObservedTokens      int
	CompactAtTokens     int
	TargetSummaryTokens int
	SignalSource        string
}

// stepGroup collects the rows for one (agent_run_id, agent_step_index) reconstruction unit:
// carried reasoning notes, assistant text + tool-call rows, and the tool-result rows answering
// that step.
type stepGroup struct {
	runID         uint64
	stepIndex     uint32
	minID         uint64
	reasoning     []*common_service_models.ChatMessage
	assistant     []*common_service_models.ChatMessage
	toolCalls     []*common_service_models.ChatMessage
	results       []*common_service_models.ChatMessage
	providerState []*common_service_models.ChatMessage
}

// defaultTokenEstimator approximates token cost as chars/4 over text parts, measured bytes
// over tool request/response payloads, and fixed costs for images and opaque parts.
// It is a budgeting heuristic, not a billing figure.
func defaultTokenEstimator(parts []*genkitai.Part) int {
	tokens := 0
	for _, part := range parts {
		tokens += estimatePartTokens(part)
	}
	return tokens
}

// estimatePartTokens estimates one part. Tool payloads are measured by their marshaled JSON
// byte length at ~3.5 chars/token (JSON is punctuation-dense and tokenizes worse than prose);
// text and non-image media contents use the chars/4 prose heuristic; parts whose size cannot be
// measured fall back to a flat charge.
func estimatePartTokens(part *genkitai.Part) int {
	const flatOpaquePartTokens = 256
	if part == nil {
		return 0
	}
	switch {
	case part.IsText() || part.IsReasoning():
		return (len(part.Text) + 3) / 4
	case part.IsToolRequest() && part.ToolRequest != nil:
		req := part.ToolRequest
		return jsonTokens(req.Input, flatOpaquePartTokens) + (len(req.Name)+len(req.Ref)+3)/4
	case part.IsToolResponse() && part.ToolResponse != nil:
		resp := part.ToolResponse
		tokens := jsonTokens(resp.Output, flatOpaquePartTokens) + (len(resp.Name)+len(resp.Ref)+3)/4
		for _, content := range resp.Content {
			tokens += estimatePartTokens(content)
		}
		return tokens
	case part.IsMedia():
		if strings.HasPrefix(strings.ToLower(strings.TrimSpace(part.ContentType)), "image/") {
			// Vision cost depends on provider resizing and detail, not PNG compression or
			// the length of a signed URL/base64 representation. Use the same conservative
			// page-sized allowance before and after private media hydration. Byte/security
			// limits are enforced independently by the context builder and media resolver.
			return 2048
		}
		return (len(part.Text) + 3) / 4
	case part.IsData():
		data, err := partdata.String(part)
		if err != nil {
			// This is an estimate, not a validation gate: the conversion paths that send the
			// part to a provider fail closed on the same error, so a flat estimate is enough here.
			return flatOpaquePartTokens
		}
		return (len(data) + 3) / 4
	default:
		return flatOpaquePartTokens
	}
}

// jsonTokens estimates the token cost of a JSON-marshalable payload from its encoded byte
// length at ~3.5 chars/token, or fallback when the payload is nil or fails to marshal.
func jsonTokens(payload any, fallback int) int {
	if payload == nil {
		return 0
	}
	encoded, err := json.Marshal(payload)
	if err != nil {
		return fallback
	}
	return (len(encoded)*2 + 6) / 7
}

// resolveTokenBudget applies the MaxTokens / default precedence.
func resolveTokenBudget(opts AgentContextOptions) int {
	if opts.MaxTokens > 0 {
		return opts.MaxTokens
	}
	return defaultTokenBudget
}

// validateMessageRepresentation rejects rows that cannot be interpreted without guessing.
func validateMessageRepresentation(msg *common_service_models.ChatMessage) error {
	if msg.Kind < proto_models.MessageKind_MESSAGE_KIND_STANDARD || msg.Kind > proto_models.MessageKind_MESSAGE_KIND_PROVIDER_STATE {
		return fmt.Errorf("ai context: message %d has unsupported kind %d; messages require an explicit kind", msg.ID, msg.Kind)
	}
	if roleOf(msg) == "" {
		return fmt.Errorf("ai context: message %d has unsupported role %d; messages require an explicit role", msg.ID, msg.Role)
	}
	return nil
}

// roleOf projects the stored explicit role. An unknown role is not inferred.
func roleOf(msg *common_service_models.ChatMessage) genkitai.Role {
	switch msg.Role {
	case proto_models.MessageRole_MESSAGE_ROLE_ASSISTANT:
		return genkitai.RoleModel
	case proto_models.MessageRole_MESSAGE_ROLE_SYSTEM:
		return genkitai.RoleSystem
	case proto_models.MessageRole_MESSAGE_ROLE_TOOL:
		return genkitai.RoleTool
	case proto_models.MessageRole_MESSAGE_ROLE_USER:
		return genkitai.RoleUser
	default:
		return genkitai.Role("")
	}
}

// kindOf returns the stored explicit kind.
func kindOf(msg *common_service_models.ChatMessage) proto_models.MessageKind {
	return msg.Kind
}

// excludedFromAIContext reports whether a message should be skipped from the LLM context
// unconditionally (exclude_from_ai_context=true).
func excludedFromAIContext(msg *common_service_models.ChatMessage) bool {
	return msg.ExcludeFromAIContext != nil && *msg.ExcludeFromAIContext
}

// sortMessagesByID returns the messages in ascending id order (canonical transcript order).
func sortMessagesByID(messages []*common_service_models.ChatMessage) []*common_service_models.ChatMessage {
	out := make([]*common_service_models.ChatMessage, 0, len(messages))
	for _, m := range messages {
		if m != nil {
			out = append(out, m)
		}
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}
