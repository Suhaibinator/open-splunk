// Package common_persistence_models contains the database models used by the persistence layer.
// This file defines the ChatMessageEmbed model for storing rich embeds attached to messages.
package common_persistence_models

import (
	"time"
)

// ChatMessageEmbed defines the data structure used in the persistence model flow for database reads, writes, and ORM mappings.
// It carries state across layer boundaries handled by this package.
type ChatMessageEmbed struct {
	// ID is the unique identifier for the embed (auto-incrementing primary key)
	ID uint64 `json:"id" gorm:"primaryKey;autoIncrement"`

	// MessageID references the message this embed belongs to
	MessageID uint64 `json:"message_id" gorm:"not null;index"`

	// Message is the associated message record (when preloaded)
	Message ChatMessage `json:"message" gorm:"foreignKey:MessageID;constraint:OnUpdate:CASCADE,OnDelete:CASCADE;"`

	// EmbedType identifies the type of embed (0-999 = native, 1000+ = custom/app-specific)
	// Native types include: IMAGE(1), VIDEO(2), AUDIO(3), FILE(4), GIF(5),
	// LINK_PREVIEW(100), CODE_BLOCK(101), etc.
	EmbedType uint32 `json:"embed_type" gorm:"not null"`

	// EmbedData contains protobuf-serialized binary data specific to the EmbedType
	EmbedData []byte `json:"embed_data,omitempty" gorm:"type:bytea"`

	// FallbackText provides a human-readable fallback for clients that can't render the embed
	// Used for notifications, search/indexing, and accessibility
	FallbackText string `json:"fallback_text" gorm:"type:text"`

	// Position determines the order of embeds within a message (0-indexed)
	Position uint32 `json:"position" gorm:"not null;default:0"`

	// CreatedAt timestamp when the embed was created
	CreatedAt time.Time `json:"created_at" gorm:"type:timestamptz"`
}

// TableName executes this method as part of the persistence model flow for database reads, writes, and ORM mappings.
func (ChatMessageEmbed) TableName() string {
	return TableNameChatMessageEmbeds
}
