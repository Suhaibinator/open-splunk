// Package common_persistence_models contains the database models used by the persistence layer.
// This file defines the ChatMessageReaction model for storing emoji reactions on messages.
package common_persistence_models

import (
	"time"
)

// ChatMessageReaction defines the data structure used in the persistence model flow for database reads, writes, and ORM mappings.
// It carries state across layer boundaries handled by this package.
type ChatMessageReaction struct {
	// MessageID references the message being reacted to
	MessageID uint64 `json:"message_id" gorm:"primaryKey;index:idx_message_reactions_message"`

	// UserID references the user who added the reaction
	UserID uint64 `json:"user_id" gorm:"primaryKey;index:idx_message_reactions_user"`

	// Emoji is the emoji used for the reaction (unicode character or shortcode)
	Emoji string `json:"emoji" gorm:"primaryKey;type:varchar(32)"`

	// CreatedAt timestamp when the reaction was added
	CreatedAt time.Time `json:"created_at" gorm:"type:timestamptz;autoCreateTime;not null"`

	// Message is the associated message record (when preloaded)
	Message ChatMessage `json:"message" gorm:"foreignKey:MessageID;constraint:OnUpdate:CASCADE,OnDelete:CASCADE;"`

	// User is the associated user record (when preloaded)
	User User `json:"user" gorm:"foreignKey:UserID;constraint:OnUpdate:CASCADE,OnDelete:CASCADE;"`
}

// TableName executes this method as part of the persistence model flow for database reads, writes, and ORM mappings.
func (ChatMessageReaction) TableName() string {
	return TableNameChatMessageReactions
}
