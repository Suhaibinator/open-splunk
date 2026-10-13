package common_persistence_models

import (
	"time"

	"github.com/Suhaibinator/go-common/pkg/proto_models"
	"github.com/lib/pq"
)

// ChatMessageEditHistory defines the data structure used in the persistence model flow for database reads, writes, and ORM mappings.
// It carries state across layer boundaries handled by this package.
type ChatMessageEditHistory struct {
	ID uint64 `json:"id" gorm:"primaryKey;autoIncrement"`

	// MessageID is the live chat message whose prior state is being preserved.
	MessageID uint64 `json:"message_id" gorm:"not null;index;uniqueIndex:idx_chat_message_edit_history_message_version,priority:1"`

	// Message is the associated live message.
	Message *ChatMessage `json:"message,omitempty" gorm:"foreignKey:MessageID;constraint:OnUpdate:CASCADE,OnDelete:CASCADE;"`

	// VersionNumber increments once per superseded version for a message.
	VersionNumber uint32 `json:"version_number" gorm:"not null;uniqueIndex:idx_chat_message_edit_history_message_version,priority:2"`

	// EditedByUserID is the actor that replaced this version.
	EditedByUserID uint64 `json:"edited_by_user_id" gorm:"not null;index"`

	// EditedByUser is the associated actor user.
	EditedByUser *User `json:"edited_by_user,omitempty" gorm:"foreignKey:EditedByUserID;references:ID"`

	// VersionStartedAt is when this preserved version became the active live message state.
	VersionStartedAt time.Time `json:"version_started_at" gorm:"type:timestamptz;not null"`

	// VersionEndedAt is when this preserved version was replaced by a new edit.
	VersionEndedAt time.Time `json:"version_ended_at" gorm:"type:timestamptz;not null;index"`

	// Content is the text content of the preserved version.
	Content string `json:"content" gorm:"type:text;not null"`

	// ContentType indicates the format of the preserved content.
	ContentType proto_models.MessageContentType `json:"content_type" gorm:"type:int;not null;default:1"`

	// EmbedsJSON stores ordered embed snapshots for the preserved version.
	EmbedsJSON []byte `json:"embeds_json" gorm:"type:jsonb;not null;default:'[]'"`

	// AttachmentIDs stores attachment IDs referenced by the preserved version.
	AttachmentIDs pq.Int64Array `json:"attachment_ids" gorm:"type:bigint[]"`

	// MentionUserIDs stores mentioned user IDs referenced by the preserved version.
	MentionUserIDs pq.Int64Array `json:"mention_user_ids" gorm:"type:bigint[]"`

	// CreatedAt is when the history row was created.
	CreatedAt time.Time `json:"created_at" gorm:"type:timestamptz;autoCreateTime"`
}

// TableName executes this method as part of the persistence model flow for database reads, writes, and ORM mappings.
func (ChatMessageEditHistory) TableName() string {
	return TableNameChatMessageEditHistory
}
