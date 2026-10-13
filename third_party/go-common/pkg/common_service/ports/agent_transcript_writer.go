package ports

import (
	"context"
	"encoding/json/jsontext"
	"errors"
	"time"

	"github.com/Suhaibinator/go-common/pkg/common_service/common_service_models"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
)

// ToolResultWriteError preserves a transcript/storage failure while telling the execution lanes
// whether repeating an already-completed tool could make the result persistable. Deterministic
// validation, binding, and conflict failures are terminal; transport/database failures remain
// retryable under the tool call's stable idempotency key.
type ToolResultWriteError struct {
	Cause    error
	CanRetry bool
}

func (e *ToolResultWriteError) Error() string {
	if e == nil || e.Cause == nil {
		return "tool result write failed"
	}
	return e.Cause.Error()
}

func (e *ToolResultWriteError) Unwrap() error   { return e.Cause }
func (e *ToolResultWriteError) Retryable() bool { return e != nil && e.CanRetry }

// ToolResultWriteRetryable returns a writer's structural retry classification. Unknown errors
// remain retryable for compatibility with operational writer implementations.
func ToolResultWriteRetryable(err error) bool {
	if err == nil {
		return false
	}
	if classified, ok := errors.AsType[interface {
		error
		Retryable() bool
	}](err); ok {
		return classified.Retryable()
	}
	return true
}

// MaxTranscriptModelPayloadBytes caps the serialized structured descriptor stored beside an agent
// transcript row. Binary media must be externalized before this limit is applied.
const MaxTranscriptModelPayloadBytes = common_service_models.MaxTranscriptModelPayloadBytes

const (
	MaxToolResultEmbeds       = common_service_models.MaxToolResultEmbeds
	MaxToolResultContentParts = common_service_models.MaxToolResultContentParts
)

var (
	// ErrToolResultContentJSONInvalid rejects malformed structured tool output.
	ErrToolResultContentJSONInvalid = common_service_models.ErrToolResultContentJSONInvalid
	// ErrToolResultPayloadTooLarge rejects a descriptor that remains too large after media externalization.
	ErrToolResultPayloadTooLarge     = common_service_models.ErrToolResultPayloadTooLarge
	ErrToolResultTooManyEmbeds       = common_service_models.ErrToolResultTooManyEmbeds
	ErrToolResultTooManyContentParts = common_service_models.ErrToolResultTooManyContentParts
	ErrToolResultTextInvalid         = common_service_models.ErrToolResultTextInvalid
)

// ValidateToolResultPayload applies the shared ingress invariants for inline and approved tool
// results. contentJSON may be empty; non-empty JSON must be valid, and payloadBytes are measured
// after binary media has been externalized.
func ValidateToolResultPayload(contentJSON []byte, payloadBytes []byte) error {
	return common_service_models.ValidateToolResultPayload(string(contentJSON), payloadBytes)
}

// ValidateToolResultEmbeds applies the shared display-embed fan-out and byte ceilings.
func ValidateToolResultEmbeds(embeds []*common_service_models.ChatMessageEmbed) error {
	return common_service_models.ValidateToolResultEmbeds(embeds)
}

// ValidateToolResultContentPartCount bounds callback-controlled multipart fan-out.
func ValidateToolResultContentPartCount(count int) error {
	return common_service_models.ValidateToolResultContentPartCount(count)
}

// ValidateToolResultText rejects strings PostgreSQL text and protobuf strings cannot preserve.
func ValidateToolResultText(text string) error {
	return common_service_models.ValidateToolResultText(text)
}

// BotTranscriptWriter is the server-side surface a bot handler uses to persist
// non-STANDARD agent transcript rows (TOOL_CALL / TOOL_RESULT) as it runs its
// loop. It is the authority-bearing write path described in Decision 5: every
// record goes through the single internal transcript-write validator, which
// applies per-kind defaults, enforces payload/pairing invariants, and confirms
// the run belongs to the same channel/thread/bot context before persistence.
//
// The runtime records an interleaved call->result->call tool loop through these
// methods rather than a single return value, so it exposes RecordToolCall /
// RecordToolResult / RecordAssistantText from one source. Inference-call
// accounting is recorded through a separate recorder, not through this writer.
//
// IMPLEMENTATION INVARIANT — fan out MESSAGE_SENT only on a real INSERT. For
// BOTH RecordToolCall and RecordToolResult, the writer must emit the MESSAGE_SENT
// websocket event only when it actually inserted the row, and suppress fanout on
// an idempotent no-op retry (same (agent_run_id, tool_call_id), matching immutable
// payload). This writer-internal dedup is what makes RecordToolCall safe without a
// created return: a crash-resume that re-records the same TOOL_CALL step must not
// produce a duplicate TOOL_CALL event. RecordToolResult additionally returns
// created so callers can gate their OWN extra side effects (E5's
// TOOL_CALL_RESOLUTION event + resume nudge; E4's skip-re-execute); the writer's
// own MESSAGE_SENT fanout is still insert-only on that path too.
//
// Signed off by E4 (runtime continuation loop) and E5 (approval execution). The
// validator/writer implementation lands later (it needs E2's run/message reads);
// this interface and its record types are the stable cross-lane contract.
type AgentTranscriptWriter interface {
	// RecordToolCall persists a TOOL_CALL message immediately (fanned out via
	// MESSAGE_SENT). With RequireApproval() the call is persisted as PENDING and
	// the current handler is terminal for that call; resolution resumes through
	// the durable queue (Decision 6). Without it, the call is NOT_REQUIRED and may
	// be executed by the same handler. It returns the persisted message ID so the
	// caller can pair the eventual TOOL_RESULT and key idempotent side effects.
	RecordToolCall(call *ToolCallRecord, opts ...ToolCallOption) (uint64, error)

	// RecordAssistantText persists the assistant text turn produced by a model
	// generation as a STANDARD/ASSISTANT transcript row tagged with the run and
	// step, visible to users by default. It is the third agent-row write that goes
	// through this single authority-bearing path alongside RecordToolCall /
	// RecordToolResult so assistant text can interleave with the tool loop.
	// It returns the persisted message ID and a created flag (MESSAGE_SENT fanout is
	// insert-only, mirroring the other records). Inference-call usage is NOT recorded
	// here — the runtime records the ChatInferenceCall ledger row separately, linking
	// it to this message ID.
	RecordAssistantText(rec *AssistantTextRecord) (messageID uint64, created bool, err error)

	// RecordToolResult persists the paired TOOL_RESULT message for an existing
	// TOOL_CALL in the same run. It returns the persisted TOOL_RESULT message ID
	// and a created flag.
	//
	// created reports whether this call inserted the row (true) or found an
	// existing one for the same (agent_run_id, tool_call_id) (false). Callers must
	// branch on it so a retried write does not double-fan-out the MESSAGE_SENT /
	// TOOL_CALL_RESOLUTION websocket event or double-fire a resume nudge: emit
	// side effects only when created is true. This mirrors the created-vs-existing
	// semantics of the chat GetOrCreate flows.
	//
	// The validator REJECTS the write when no TOOL_CALL row exists for
	// (AgentRunID, ToolCallID): an orphan TOOL_RESULT cannot reconstruct into a
	// provider-valid ToolResponse (providers reject responses without their
	// request) and would be silently dropped from context, so it fails at write
	// time instead. The result's AgentStepIndex is copied from the paired
	// TOOL_CALL.
	//
	// It is idempotent on (agent_run_id, tool_call_id), enforced by the partial
	// unique index for TOOL_RESULT rows: a retry with the same immutable result
	// payload returns the existing row's ID with created=false (ON CONFLICT DO
	// NOTHING semantics); a different payload for the same key is a conflict error.
	// The synthetic denial/expiry error result also flows through this same
	// idempotent path.
	RecordToolResult(result *ToolResultRecord) (messageID uint64, created bool, err error)
}

// ClaimedToolResultWriter is the execution-fencing extension used by the live inline loop. The
// durable scanner uses the same persistence primitives directly. A caller must win ClaimInlineTool
// before invoking a side-effecting tool, then commit the result with that exact token.
type ClaimedToolResultWriter interface {
	AgentTranscriptWriter
	ClaimInlineTool(toolCallMessageID uint64, leaseToken string, leaseExpiresAt time.Time) (bool, error)
	ReleaseInlineToolClaim(toolCallMessageID uint64, leaseToken string, retryable bool, lastErr string) (bool, error)
	RecordClaimedToolResult(toolCallMessageID uint64, leaseToken string, result *ToolResultRecord) (messageID uint64, created bool, committed bool, err error)
}

// AssistantStreamWriter is the optional extension used by transcript-native agent runtimes that
// can stream assistant text. It creates the assistant row through the same authority-bearing
// transcript writer path as RecordAssistantText, then fans out MESSAGE_STREAM_* events while the
// model is still generating.
type AssistantStreamWriter interface {
	AgentTranscriptWriter

	StartAssistantStream(start *AssistantStreamStartRecord) (messageID uint64, err error)
	AppendAssistantStreamDelta(delta string) error
	FinishAssistantStream(finish *AssistantStreamFinishRecord) (messageID uint64, err error)
	FailAssistantStream(fail *AssistantStreamFailRecord) (messageID uint64, err error)
}

// AgentNoteWriter is the optional AgentTranscriptWriter extension for AGENT_NOTE rows: LLM-only
// scratchpad / injected-instruction rows tied to a run and step. The runtime uses it for two
// writes: the model's reasoning (carried back into the run's own context as a reasoning part so
// thinking models can continue multi-stage planning, or stored audit-only with an explicit
// exclude_from_ai_context=true override on the run's final turn), and the last-resort
// reasoning-only-turn nudge. AGENT_NOTE context inclusion is run-scoped at reconstruction:
// only the run being driven sees its own notes. Writers that do not implement this extension
// degrade gracefully — reasoning is not persisted and reasoning-only turns fail without
// continuation.
type AgentNoteWriter interface {
	RecordAgentNote(rec *AgentNoteRecord) (messageID uint64, created bool, err error)
}

// ProviderStateWriter is the optional transcript-writer capability for opaque provider
// continuation state. Implementations synchronously externalize State.Data through an internal
// attachment store, then persist a hidden/internal reference row without websocket publication.
type ProviderStateWriter interface {
	RecordProviderState(rec *ProviderStateRecord) (messageID uint64, created bool, err error)
}

// ProviderStateResolver is the optional read side of ProviderStateWriter. It resolves only an
// internal provider-state attachment bound to the expected originating sender, run, and step.
type ProviderStateResolver interface {
	ResolveProviderState(ctx context.Context, attachmentID, ownerUserID, agentRunID uint64, agentStepIndex uint32, maxBytes uint64) ([]byte, error)
}

// ToolMediaResolver is the privileged read side for attachment-backed tool-result media. The
// implementation verifies that the attachment belongs to the originating transcript sender and
// the expected run/call/part before returning a short-lived URL or an integrity-checked data URI.
type ToolMediaResolver interface {
	ResolveToolMedia(ctx context.Context, attachmentID, ownerUserID, agentRunID uint64, toolCallID string, partIndex int, contentType, filename string, maxBytes uint64) (*ResolvedToolMedia, error)
}

// ProviderStateRecord binds one provider-state envelope to the model response produced by a
// particular run and step. State.Data may be set instead of AttachmentID; it never enters the
// serialized MessageModelPayload.
type ProviderStateRecord struct {
	AgentRunID      uint64
	AgentStepIndex  uint32
	ParentMessageID *uint64
	State           *common_service_models.ProviderState
}

// AgentNoteRecord is the input for one AGENT_NOTE transcript row. Kind visibility defaults are
// exclude_from_ai_context=false, hidden_from_users=true; nil override fields apply those defaults.
// The first-class runtime explicitly sets HiddenFromUsers=false for assistant reasoning traces by
// default and leaves internal control notes hidden.
type AgentNoteRecord struct {
	// AgentRunID and AgentStepIndex locate the note within one run and model step. Required.
	AgentRunID     uint64
	AgentStepIndex uint32

	// Content is the note text (reasoning dump or injected instruction).
	Content string

	// Subtype is a structural control marker; empty denotes an ordinary application note. The
	// app_resume subtype is reserved for ResumeParent and rejected.
	Subtype string

	// Role selects how a non-excluded note reconstructs into the owning run's model context:
	// ASSISTANT notes become reasoning parts of their step's model message (carried reasoning);
	// any other role reconstructs as a standalone message with that role (for example the USER
	// nudge). UNSPECIFIED defaults to ASSISTANT.
	Role proto_models.MessageRole

	// ParentMessageID scopes the row to a thread (nil/0 = main timeline).
	ParentMessageID *uint64

	// HiddenFromUsers / ExcludeFromAIContext are optional per-message visibility overrides; nil
	// applies the AGENT_NOTE kind defaults (in AI context, hidden from users).
	HiddenFromUsers      *bool
	ExcludeFromAIContext *bool
}

// ToolCallRecord is the app-facing input for persisting a TOOL_CALL row. The
// writer assembles the immutable ToolCallPayload (tool_call_id, tool_name,
// arguments_json, renderer_key) and sets the authoritative columns
// (agent_run_id, agent_step_index, tool_call_id, kind=TOOL_CALL,
// role=ASSISTANT, approval/execution columns). Approval state is never carried
// in payload bytes.
// AssistantTextRecord is the assistant text turn produced by a model generation, written as a
// STANDARD/ASSISTANT transcript row. The writer sets kind=STANDARD, role=ASSISTANT, detected
// content type, and the run/step tags; it is visible to users by default (the user sees the
// agent's reply).
type AssistantTextRecord struct {
	// AgentRunID and AgentStepIndex locate the assistant turn within one run and model step.
	AgentRunID     uint64
	AgentStepIndex uint32

	// Content is the assistant's text.
	Content string

	// ParentMessageID scopes the row to a thread (nil/0 = main timeline).
	ParentMessageID *uint64

	// HiddenFromUsers / ExcludeFromAIContext are optional per-message visibility overrides. nil
	// applies the kind default (both false: a standard assistant message is visible and in context).
	HiddenFromUsers      *bool
	ExcludeFromAIContext *bool

	// BroadcastToChannel also shows the thread reply on the main timeline. The runtime sets it on
	// a delegated child's closing message so the parent sees the result; it requires a non-zero
	// ParentMessageID.
	BroadcastToChannel bool
}

// AssistantStreamStartRecord creates the STREAMING STANDARD/ASSISTANT shell row for a streamed
// agent turn. The final content is supplied later by AssistantStreamFinishRecord.
type AssistantStreamStartRecord struct {
	AgentRunID     uint64
	AgentStepIndex uint32

	ParentMessageID *uint64

	HiddenFromUsers      *bool
	ExcludeFromAIContext *bool
}

// AssistantStreamFinishRecord finalizes a streamed assistant row with the provider's canonical
// final text.
type AssistantStreamFinishRecord struct {
	Content string
	// BroadcastToChannel flips the streamed thread row onto the main timeline as it is sealed. The
	// start record cannot carry it: whether a streamed turn is the closing one is only known once
	// the provider finishes without tool requests.
	BroadcastToChannel bool
}

// AssistantStreamFailRecord marks a streamed assistant row failed while preserving any text that
// was already visible to clients.
type AssistantStreamFailRecord struct {
	Content string
	Err     error
}

// RejectedToolCallWriter commits a non-executable call and its error result atomically.
// CountRejectedArgumentSteps counts distinct durable steps across the entire run, not the context window.
type RejectedToolCallWriter interface {
	RecordRejectedToolCall(call *ToolCallRecord, result *ToolResultRecord) (uint64, error)
	CountRejectedArgumentSteps(runID uint64) (uint32, error)
}

type ToolCallRecord struct {
	// ParentMessageID scopes the row to a thread (nil/0 = main timeline).
	ParentMessageID *uint64
	// AgentRunID and AgentStepIndex locate the call within one logical run and
	// model/tool step. Required for TOOL_CALL rows.
	AgentRunID     uint64
	AgentStepIndex uint32

	// ToolCallID is the non-empty, step-unique internal call reference (==
	// genkit ToolRequest.Ref) and is stored both in the payload and the
	// authoritative tool_call_id column.
	ToolCallID string
	ToolName   string

	// ArgumentsJSON is the JSON-encoded, provider-portable tool arguments. The
	// validator confirms it is well-formed JSON and within the payload byte cap;
	// it is stored opaque and never executed.
	ArgumentsJSON jsontext.Value
	// RejectedArguments is accepted only by RecordRejectedToolCall; ArgumentsJSON must be empty.
	RejectedArguments *proto_models.RejectedToolArguments
	// ThoughtSignature is opaque Vertex/Gemini continuation state attached to the
	// function-call part. The writer persists it inside ToolCallPayload so the exact
	// bytes survive approval waits and crash resumes.
	ThoughtSignature []byte
	// ProviderCallID is the exact provider FunctionCall.ID. nil means unknown
	// (legacy ref-based replay); a non-nil pointer may contain an empty string.
	// ToolCallID remains the non-empty, unique internal persistence key.
	ProviderCallID *string

	// FallbackContent is the human-readable timeline fallback (for example
	// "Searching the web for ..."). Optional.
	FallbackContent string

	// RendererKey is optional frontend metadata used only for picking a renderer.
	RendererKey string

	// HiddenFromUsers / ExcludeFromAIContext are optional per-message visibility
	// overrides. nil applies the per-kind default at the validator. An explicit
	// false is preserved over a kind default of true.
	HiddenFromUsers      *bool
	ExcludeFromAIContext *bool
}

// ToolResultRecord is the app-facing input for persisting a TOOL_RESULT row. The
// writer assembles the immutable ToolResultPayload and validates it pairs with
// an existing TOOL_CALL in the same run, copying the original call's step index.
type ToolResultRecord struct {
	// ParentMessageID must match the originating run and tool call.
	ParentMessageID *uint64
	// AgentRunID and ToolCallID identify the TOOL_CALL this result answers. The
	// step index is copied from the original TOOL_CALL by the validator.
	AgentRunID uint64
	ToolCallID string

	// ContentJSON is the JSON-encoded result (genkit ToolResponse.Output). Default
	// result path; validated as well-formed JSON within the payload byte cap.
	ContentJSON jsontext.Value
	IsError     bool

	// ContentParts preserves multipart text/data/media results for provider
	// round-trips when ContentJSON alone is insufficient. Optional.
	ContentParts []*proto_models.MessagePart

	// FallbackContent is the human-readable timeline fallback (collapsed in UI).
	FallbackContent string

	// Embeds are optional display-only embeds rendered inside the tool-result card.
	// They never replace ContentJSON as the canonical model-visible result.
	Embeds []*common_service_models.ChatMessageEmbed

	// RendererKey is optional frontend metadata. Optional.
	RendererKey string

	// HiddenFromUsers / ExcludeFromAIContext are optional per-message visibility
	// overrides; nil applies the per-kind default at the validator.
	HiddenFromUsers      *bool
	ExcludeFromAIContext *bool
}

// ToolCallOptions accumulates the effect of ToolCallOption values applied to a
// RecordToolCall call. It is exported so the transcript-writer implementation in
// the service layer can read the resolved option state.
type ToolCallOptions struct {
	// RequireApproval marks the TOOL_CALL PENDING and terminal for the current
	// handler invocation; execution resumes through the durable approval queue.
	RequireApproval bool

	// ApprovalExpiresAfter is the pending-approval lifetime the writer stamps into
	// approval_expires_at at RECORD time (now + this duration). It is only meaningful
	// when RequireApproval is set. Expiry is decided when the row is written — not
	// re-stamped later — so this must carry the correct policy value at record time.
	// Zero means "use the service default" (defaultApprovalExpiry, 1h); the runtime
	// caller passes the run's captured AgentApprovalPolicy.ApprovalExpiresAfter.
	ApprovalExpiresAfter time.Duration
}

// ToolCallOption configures a RecordToolCall invocation.
type ToolCallOption func(*ToolCallOptions)

// RequireApproval marks a TOOL_CALL as approval-required. The call is persisted
// with approval_status=PENDING and execution_state=WAITING_FOR_APPROVAL, the
// owning run transitions to WAITING_FOR_APPROVAL, and the tool is NOT executed
// by the current handler.
//
// The optional expiresAfter carries the pending-approval lifetime, stamped into
// approval_expires_at at record time. Pass the run's captured
// AgentApprovalPolicy.ApprovalExpiresAfter; omit it (or pass <= 0) to use the
// service default (1h). Expiry is fixed when the row is written, so the correct
// duration must be supplied here — it is never re-stamped downstream.
//
// Parameters:
//   - expiresAfter: optional pending-approval lifetime; the first positive value wins.
func RequireApproval(expiresAfter ...time.Duration) ToolCallOption {
	return func(o *ToolCallOptions) {
		o.RequireApproval = true
		for _, d := range expiresAfter {
			if d > 0 {
				o.ApprovalExpiresAfter = d
				break
			}
		}
	}
}

// ApplyToolCallOptions resolves a slice of options into a ToolCallOptions value.
// It is a convenience for transcript-writer implementations.
//
// Parameters:
//   - opts: Variadic ToolCallOption values consumed by ApplyToolCallOptions.
func ApplyToolCallOptions(opts ...ToolCallOption) ToolCallOptions {
	var resolved ToolCallOptions
	for _, opt := range opts {
		if opt != nil {
			opt(&resolved)
		}
	}
	return resolved
}
