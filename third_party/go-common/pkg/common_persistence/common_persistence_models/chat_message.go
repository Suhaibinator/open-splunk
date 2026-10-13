// Package common_persistence_models contains the database models used by the persistence layer.
// This file defines the ChatMessage model for storing chat messages.
package common_persistence_models

import (
	"time"

	"github.com/Suhaibinator/go-common/pkg/proto_models"
	"github.com/lib/pq"
	"gorm.io/gorm"
)

// ChatMessage defines the data structure used in the persistence model flow for database reads, writes, and ORM mappings.
// It carries state across layer boundaries handled by this package.
type ChatMessage struct {
	// ID is the unique identifier for the message (auto-incrementing primary key)
	ID uint64 `json:"id" gorm:"primaryKey;autoIncrement"`

	// ChannelID references the channel this message belongs to
	ChannelID uint64 `json:"channel_id" gorm:"not null;index:idx_messages_channel_created"`

	// Channel is the associated channel record (when preloaded)
	Channel ChatChannel `json:"channel" gorm:"foreignKey:ChannelID;constraint:OnUpdate:CASCADE,OnDelete:CASCADE;"`

	// SenderID references the user who sent this message
	SenderID uint64 `json:"sender_id" gorm:"not null;index"`

	// Sender is the associated user record (when preloaded)
	Sender User `json:"sender" gorm:"foreignKey:SenderID;constraint:OnUpdate:CASCADE,OnDelete:SET NULL;"`

	// Content is the text content of the message
	Content string `json:"content" gorm:"type:text;not null"`

	// ContentType indicates the format of the content (plain text or markdown)
	ContentType proto_models.MessageContentType `json:"content_type" gorm:"type:int;not null;default:1"` // Default to TEXT

	// StreamState tracks the lifecycle of a streamed bot/LLM response.
	StreamState proto_models.MessageStreamState `json:"stream_state" gorm:"type:int;not null;default:0"`

	// ParentMessageID is the thread root ID (NULL/0 means main timeline, non-zero means thread reply)
	ParentMessageID *uint64 `json:"parent_message_id,omitempty" gorm:"index"`

	// ParentMessage is the thread root message (when preloaded)
	ParentMessage *ChatMessage `json:"parent_message,omitempty" gorm:"foreignKey:ParentMessageID;constraint:OnUpdate:CASCADE,OnDelete:SET NULL;"`

	// ReplyToMessageID is the message being replied to (Discord-style "replying to...")
	// Must refer to a message in the same channel; NULL/0 means no reply reference
	ReplyToMessageID *uint64 `json:"reply_to_message_id,omitempty" gorm:"index"`

	// BroadcastToChannel marks a thread reply that also appears on the main timeline.
	// One row serves both views, so ParentMessageID keeps naming the thread root.
	// No index tag: the main-timeline reads are served by the raw partial index
	// idx_chat_messages_channel_main_timeline_id (ensurePerformanceIndexes), whose
	// predicate must match applyChatMessageThreadScope exactly.
	BroadcastToChannel bool `json:"broadcast_to_channel" gorm:"column:broadcast_to_channel;not null;default:false"`

	// ReplyToMessage is the message being replied to (when preloaded)
	ReplyToMessage *ChatMessage `json:"reply_to_message,omitempty" gorm:"foreignKey:ReplyToMessageID;constraint:OnUpdate:CASCADE,OnDelete:SET NULL;"`

	// IsEdited indicates whether the message has been edited
	IsEdited bool `json:"is_edited" gorm:"not null;default:false"`

	// EditedAt timestamp when the message was last edited
	EditedAt *time.Time `json:"edited_at,omitempty" gorm:"type:timestamptz"`

	// OriginalContent stores the original content before editing (for audit purposes)
	OriginalContent *string `json:"original_content,omitempty" gorm:"type:text"`

	// CreatedAt timestamp when the message was sent
	CreatedAt time.Time `json:"created_at" gorm:"type:timestamptz;index:idx_messages_channel_created,sort:desc"`

	// UpdatedAt timestamp when the message record was last modified
	UpdatedAt time.Time `json:"updated_at" gorm:"type:timestamptz;autoUpdateTime"`

	// DeletedAt enables GORM soft delete behavior
	DeletedAt gorm.DeletedAt `json:"deleted_at" gorm:"index"`

	// Embeds contains the message embeds (when preloaded)
	Embeds []ChatMessageEmbed `json:"embeds,omitempty" gorm:"foreignKey:MessageID"`

	// AttachmentIDs stores attachment IDs requested at send time (may be pending upload).
	AttachmentIDs pq.Int64Array `json:"attachment_ids,omitempty" gorm:"type:bigint[]"`

	// Reactions contains the message reactions (when preloaded)
	Reactions []ChatMessageReaction `json:"reactions,omitempty" gorm:"foreignKey:MessageID"`

	// Mentions contains the message mentions (when preloaded)
	Mentions []ChatMessageMention `json:"mentions,omitempty" gorm:"foreignKey:MessageID"`

	// Kind discriminates standard chat from agentic transcript rows (tool calls,
	// tool results, system events, compaction summaries, agent notes).
	// Stored kinds are explicit; unspecified is rejected by the shared schema.
	//
	// The composite (channel_id, parent_message_id, kind, id) index that serves
	// GetLatestCompaction is created in raw SQL (ensureChatAgenticIndexes) — not via a
	// struct tag here. Tagging an index name on this single field would make AutoMigrate
	// build a single-column index under that name, which would then shadow (and skip) the
	// raw composite.
	Kind proto_models.MessageKind `json:"kind" gorm:"column:kind;type:smallint;not null;default:1"`

	// AgentNoteSubtype identifies control notes independently of their presentation text.
	// Empty means an untyped application note.
	AgentNoteSubtype string `json:"agent_note_subtype,omitempty" gorm:"type:text;not null;default:''"`

	// Role is the explicit LLM role (user/assistant/system/tool).
	// Writers supply the explicit role; unspecified is rejected by the shared schema.
	Role proto_models.MessageRole `json:"role" gorm:"column:role;type:smallint;not null;default:1"`

	// ExcludeFromAIContext, when set, marks the message as never entering LLM context
	// (human-visible noise such as joins/renames). Nullable so an explicit false is
	// distinguishable from "unset" — the service applies per-kind defaults when absent.
	ExcludeFromAIContext *bool `json:"exclude_from_ai_context,omitempty" gorm:"column:exclude_from_ai_context"`

	// HiddenFromUsers, when set, marks the message as never shown to regular users
	// (LLM-only: compaction summaries, agent notes). Filtered server-side. Nullable so
	// an app can force false on a kind that defaults to hidden (e.g. a visible divider).
	HiddenFromUsers *bool `json:"hidden_from_users,omitempty" gorm:"column:hidden_from_users"`

	// AgentRunID references ChatAgentRun, grouping every transcript row produced by one
	// logical agent run. NULL on legacy/human rows (they fall back to sender+ID adjacency).
	AgentRunID *uint64 `json:"agent_run_id,omitempty" gorm:"column:agent_run_id;index:idx_chat_messages_run_step_id,priority:1"`

	// AgentRun is the associated run record (when preloaded)
	AgentRun *ChatAgentRun `json:"agent_run,omitempty" gorm:"foreignKey:AgentRunID;constraint:OnUpdate:CASCADE,OnDelete:SET NULL;"`

	// AgentStepIndex is the reconstruction boundary inside one run: the model/tool step
	// this row belongs to. NULL on non-agent rows.
	AgentStepIndex *uint32 `json:"agent_step_index,omitempty" gorm:"column:agent_step_index;index:idx_chat_messages_run_step_id,priority:2"`

	// ToolCall is the 1:1 side row holding the structured payload + tool-call approval/execution
	// state for TOOL_CALL/TOOL_RESULT/COMPACTION_SUMMARY/PROVIDER_STATE and typed SYSTEM_EVENT
	// messages. STANDARD and AGENT_NOTE have no side row.
	// It is split off this hot table so human-only chat rows do not carry ~16 agent-only columns
	// (the ChatInferenceCall precedent). Preloaded on the read paths where tool rows render, like
	// Embeds/Reactions. nil for STANDARD human messages.
	ToolCall *ChatMessageToolCall `json:"tool_call,omitempty" gorm:"foreignKey:MessageID"`

	// ClientMessageID is the sender-chosen idempotency key of a send. A retry carrying the same key
	// returns this message instead of sending again. The raw partial unique index
	// idx_chat_messages_client_message_id scopes it to (sender_id, channel_id) for the message's
	// whole lifetime, soft deletion included. NULL when the client sent none.
	ClientMessageID *string `json:"client_message_id,omitempty" gorm:"column:client_message_id;type:text"`

	// AdmittedRunID is the agent run that queued input was delivered into when its delivery created
	// this message (ChatAgentQueuedInput). It is NULL on every other message, including queued input
	// an archive or a disabled auto-run posted without a run. The raw partial index
	// idx_chat_messages_admitted_run finds a run's delivered input.
	AdmittedRunID *uint64       `json:"admitted_run_id,omitempty" gorm:"column:admitted_run_id"`
	AdmittedRun   *ChatAgentRun `json:"-" gorm:"foreignKey:AdmittedRunID;constraint:OnUpdate:CASCADE,OnDelete:SET NULL;"`

	// ReplyCount is the number of thread replies to this message.
	// Computed at query time when threads are enabled; not stored in the database.
	ReplyCount uint32 `json:"reply_count" gorm:"-"`
}

// TableName executes this method as part of the persistence model flow for database reads, writes, and ORM mappings.
func (ChatMessage) TableName() string {
	return TableNameChatMessages
}
