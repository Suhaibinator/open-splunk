package common_persistence

import (
	"time"

	"github.com/Suhaibinator/go-common/pkg/common_service/common_service_models"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
)

// CreateChatMessageEmbedsFromService maps service-layer embeds to persistence create-embed
// inputs, skipping nil entries. Returns nil for an empty/nil input. Shared by every code path
// that persists embeds from service models (bot dispatch, transcript writer, approval lane).
func CreateChatMessageEmbedsFromService(embeds []*common_service_models.ChatMessageEmbed) []CreateChatMessageEmbed {
	if len(embeds) == 0 {
		return nil
	}
	out := make([]CreateChatMessageEmbed, 0, len(embeds))
	for _, e := range embeds {
		if e == nil {
			continue
		}
		out = append(out, CreateChatMessageEmbed{
			EmbedType:    e.EmbedType,
			EmbedData:    e.EmbedData,
			FallbackText: e.FallbackText,
			Position:     e.Position,
		})
	}
	return out
}

// CreateTranscriptMessageInput carries everything the internal transcript writer needs to persist
// one non-STANDARD agent transcript row (TOOL_CALL / TOOL_RESULT / SYSTEM_EVENT /
// COMPACTION_SUMMARY / AGENT_NOTE / PROVIDER_STATE) plus, for structured-payload rows, its 1:1
// ChatMessageToolCall side row — in one idempotent transaction. The service layer owns
// authority/validation/defaulting; persistence applies the constraints and idempotency.
type CreateTranscriptMessageInput struct {
	ArgumentsRejected bool
	// Message (hot-table) fields.
	ChannelID       uint64
	SenderID        uint64 // the bot user for agent rows
	ParentMessageID *uint64
	// BroadcastToChannel also shows a thread row on the main timeline. It is only valid with a
	// non-zero ParentMessageID (ErrChatTranscriptBroadcastInvalid otherwise); delegated child runs
	// use it for their closing assistant message.
	BroadcastToChannel   bool
	Content              string
	ContentType          proto_models.MessageContentType
	StreamState          proto_models.MessageStreamState
	Kind                 proto_models.MessageKind
	Role                 proto_models.MessageRole
	ExcludeFromAIContext *bool
	HiddenFromUsers      *bool
	AgentRunID           *uint64
	AgentStepIndex       *uint32
	AgentNoteSubtype     string
	Embeds               []CreateChatMessageEmbed
	// AttachmentIDs links internal, synchronously-vetted blobs referenced by the
	// structured model payload. The transcript row owns these references for
	// retention/lifecycle checks; the bytes never enter ModelPayload.
	AttachmentIDs []uint64

	// ToolCall side-row fields. Set HasToolCall=true for TOOL_CALL / TOOL_RESULT (and any kind that
	// carries a model payload, e.g. COMPACTION_SUMMARY) so the side row is written. When false, no
	// side row is created (e.g. a bare SYSTEM_EVENT or a payload-less AGENT_NOTE).
	HasToolCall  bool
	ToolCallID   *string
	ModelPayload []byte
	PayloadType  proto_models.MessageModelPayloadType

	// Approval/execution columns (TOOL_CALL only). Ignored when HasToolCall is false.
	ApprovalStatus    proto_models.ToolCallApprovalStatus
	TriggeringUserID  *uint64
	ApprovalExpiresAt *time.Time
	ExecutionState    proto_models.ToolCallExecutionState
}

// KindVisibilityDefaults returns the per-kind visibility defaults from the spec's per-kind defaults
// table: the (exclude_from_ai_context, hidden_from_users) pair applied when the caller leaves a
// field unset (nil pointer). It is the single authority for these defaults so writers/facades do not
// scatter literals — an explicit per-message value always wins over the default (see
// ApplyKindVisibilityDefault).
//
//	Kind                | exclude_from_ai_context | hidden_from_users
//	STANDARD            | false                   | false
//	SYSTEM_EVENT        | true                    | false
//	TOOL_CALL           | false                   | false
//	TOOL_RESULT         | false                   | false
//	COMPACTION_SUMMARY  | false                   | true
//	AGENT_NOTE          | false                   | true
//	PROVIDER_STATE      | true                    | true
//
// Unknown kinds fall back to the STANDARD (false, false) defaults.
//
// Parameters:
//   - kind: The message kind whose defaults are requested.
//
// Returns:
//   - result1: The default exclude_from_ai_context for the kind.
//   - result2: The default hidden_from_users for the kind.
func KindVisibilityDefaults(kind proto_models.MessageKind) (excludeFromAIContext bool, hiddenFromUsers bool) {
	switch kind {
	case proto_models.MessageKind_MESSAGE_KIND_SYSTEM_EVENT:
		return true, false
	case proto_models.MessageKind_MESSAGE_KIND_COMPACTION_SUMMARY,
		proto_models.MessageKind_MESSAGE_KIND_AGENT_NOTE:
		return false, true
	case proto_models.MessageKind_MESSAGE_KIND_PROVIDER_STATE:
		return true, true
	default:
		// STANDARD, TOOL_CALL, TOOL_RESULT, and any unknown kind.
		return false, false
	}
}

// ApplyKindVisibilityDefault returns the explicit per-message override when set (field presence via a
// non-nil pointer preserves an explicit false over a kind default of true), else a pointer to the
// supplied per-kind default. It mirrors the writer-side helper so every transcript-write surface
// resolves visibility through one rule.
//
// Parameters:
//   - override: The caller's explicit value, or nil to take the kind default.
//   - kindDefault: The per-kind default (from KindVisibilityDefaults).
//
// Returns:
//   - result1: The resolved pointer value to store.
func ApplyKindVisibilityDefault(override *bool, kindDefault bool) *bool {
	if override != nil {
		return override
	}
	d := kindDefault
	return &d
}

// SendChatMessageInput carries a new chat message and its embeds and mentions for SendChatMessage.
// ParentMessageID and ReplyToMessageID are nil for the main timeline and for "no reply".
// BroadcastToChannel is only meaningful on a thread reply; the service validates it.
// AgentRunID tags a bot-authored STANDARD row (a delegation report) with the run that produced it;
// the service verifies the run belongs to the same channel, location, and bot before sending.
type SendChatMessageInput struct {
	ChannelID          uint64
	SenderID           uint64
	Content            string
	ContentType        proto_models.MessageContentType
	StreamState        proto_models.MessageStreamState
	ParentMessageID    *uint64
	ReplyToMessageID   *uint64
	BroadcastToChannel bool
	AgentRunID         *uint64
	Embeds             []CreateChatMessageEmbed
	Mentions           []CreateChatMessageMention
	Role               proto_models.MessageRole
	// ClientMessageID is the sender's idempotency key for the send; empty stores none. Look the key
	// up with LockChatClientMessageID first: a second message under it violates a unique index.
	ClientMessageID string
}

// CreateChatMessageEmbed defines the data structure used in the persistence flow for transactional database operations and repository access.
// It carries state across layer boundaries handled by this package.
type CreateChatMessageEmbed struct {
	EmbedType    uint32
	EmbedData    []byte
	FallbackText string
	Position     uint32
}

// CreateChatMessageMention defines the data structure used in the persistence flow for transactional database operations and repository access.
// It carries state across layer boundaries handled by this package.
type CreateChatMessageMention struct {
	UserID uint64
}

// EditChatMessageInput defines the data structure used in the persistence flow for transactional database operations and repository access.
// It carries state across layer boundaries handled by this package.
type EditChatMessageInput struct {
	MessageID      uint64
	EditedByUserID uint64
	NewContent     string
	NewContentType proto_models.MessageContentType
	NewStreamState *proto_models.MessageStreamState
	// ExpectedStreamState, when set, makes the edit conditional on the locked row still carrying
	// that state. Assistant stream finalization uses STREAMING so a stale finisher cannot overwrite
	// recovery's FAILED marker.
	ExpectedStreamState *proto_models.MessageStreamState
	Embeds              []CreateChatMessageEmbed
	Mentions            []CreateChatMessageMention
	AttachmentIDs       []uint64
}
