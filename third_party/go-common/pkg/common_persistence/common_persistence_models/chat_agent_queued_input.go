package common_persistence_models

import (
	"database/sql/driver"
	json "encoding/json/v2"
	"fmt"
	"time"

	"github.com/Suhaibinator/go-common/pkg/proto_models"
	"github.com/lib/pq"
)

// ChatAgentQueuedInput is a person's input to an agent session that could not reach the agent when
// it was sent, because a run was active or the session was suspended. It is not a chat message: it
// appears in no transcript, model context, unread count, notification or channel preview. Delivery
// turns it into one when the agent receives it: in queue order it writes the message the send
// would have written (with the run it was delivered into, ChatMessage.AdmittedRunID), moves its
// attachments to that message, and deletes this row, so the transcript and the model context both
// see the input at the position it was delivered.
//
// Raw schema setup adds the partial unique index on (SenderID, ChannelID, ClientMessageID).
type ChatAgentQueuedInput struct {
	// ID is the queue order, which delivery follows.
	ID uint64 `json:"id" gorm:"primaryKey;autoIncrement;index:idx_chat_agent_queued_inputs_session_order,priority:2"`

	// AgentSessionID is the session the input waits for. The (AgentSessionID, ID) index serves
	// every read of a session's queue in order.
	AgentSessionID uint64            `json:"agent_session_id" gorm:"not null;index:idx_chat_agent_queued_inputs_session_order,priority:1"`
	AgentSession   *ChatAgentSession `json:"-" gorm:"foreignKey:AgentSessionID;constraint:OnDelete:CASCADE"`

	// ChannelID and ParentMessageID are the session's location (zero parent for the main timeline),
	// where delivery posts the message.
	ChannelID       uint64       `json:"channel_id" gorm:"not null"`
	Channel         *ChatChannel `json:"-" gorm:"foreignKey:ChannelID;constraint:OnDelete:CASCADE"`
	ParentMessageID uint64       `json:"parent_message_id" gorm:"not null;default:0"`

	// SenderID is the person who sent the input; delivery writes the message as them.
	SenderID uint64 `json:"sender_id" gorm:"not null"`
	Sender   *User  `json:"-" gorm:"foreignKey:SenderID;constraint:OnDelete:CASCADE"`

	Content     string                          `json:"content" gorm:"type:text;not null"`
	ContentType proto_models.MessageContentType `json:"content_type" gorm:"type:smallint;not null;default:1"`

	// ReplyToMessageID is the message the input replies to; nil for none. It has no foreign key of
	// its own: the delivered message's reply_to_message_id carries one, and the only hard delete of
	// a message (its channel's) takes the queue with it.
	ReplyToMessageID *uint64 `json:"reply_to_message_id,omitempty"`

	// MentionUserIDs and Embeds were validated as the send's; delivery writes them as the message's
	// mention and embed rows.
	MentionUserIDs pq.Int64Array              `json:"mention_user_ids,omitempty" gorm:"type:bigint[]"`
	Embeds         ChatAgentQueuedInputEmbeds `json:"embeds,omitempty" gorm:"type:jsonb;not null;default:'[]'"`

	// AttachmentIDs are the input's attachments in order. Until delivery their rows belong to the
	// queued input (ObjectTypeChatQueuedInputAttachment, keyed by ID); delivery moves them to the
	// message (ObjectTypeChatAttachment).
	AttachmentIDs pq.Int64Array `json:"attachment_ids,omitempty" gorm:"type:bigint[]"`

	// ClientMessageID is the send's idempotency key. Delivery moves it to the message, so a retry
	// finds the queued input before delivery and the message after.
	ClientMessageID *string `json:"client_message_id,omitempty" gorm:"type:text"`

	CreatedAt time.Time `json:"created_at" gorm:"type:timestamptz;autoCreateTime"`
	UpdatedAt time.Time `json:"updated_at" gorm:"type:timestamptz;autoUpdateTime"`
}

// TableName executes this method as part of the persistence model flow for database reads, writes, and ORM mappings.
func (ChatAgentQueuedInput) TableName() string {
	return TableNameChatAgentQueuedInputs
}

// ChatAgentQueuedInputEmbed is one validated embed of a queued input, in the shape delivery writes
// to the message's ChatMessageEmbed row.
type ChatAgentQueuedInputEmbed struct {
	EmbedType    uint32 `json:"embed_type"`
	EmbedData    []byte `json:"embed_data,omitempty"`
	FallbackText string `json:"fallback_text,omitempty"`
	Position     uint32 `json:"position"`
}

// ChatAgentQueuedInputEmbeds is a queued input's embeds, stored as one jsonb array.
type ChatAgentQueuedInputEmbeds []ChatAgentQueuedInputEmbed

// Value encodes the embeds as a JSON array; none encode as [].
func (e ChatAgentQueuedInputEmbeds) Value() (driver.Value, error) {
	return json.Marshal([]ChatAgentQueuedInputEmbed(e))
}

// Scan decodes the jsonb array Value wrote.
func (e *ChatAgentQueuedInputEmbeds) Scan(src any) error {
	var data []byte
	switch value := src.(type) {
	case nil:
		*e = nil
		return nil
	case []byte:
		data = value
	case string:
		data = []byte(value)
	default:
		return fmt.Errorf("chat agent queued input embeds: unsupported source %T", src)
	}
	return json.Unmarshal(data, (*[]ChatAgentQueuedInputEmbed)(e))
}
