// Package common_persistence_models contains the database models used by the persistence layer.
// This file defines the ChatMessageMention model for storing mentions on messages.
package common_persistence_models

import (
	"time"
)

// ChatMessageMention defines the data structure used in the persistence model flow for database reads, writes, and ORM mappings.
// It carries state across layer boundaries handled by this package.
type ChatMessageMention struct {
	// MessageID references the message containing the mention
	MessageID uint64 `json:"message_id" gorm:"primaryKey;index:idx_message_mentions_message"`

	// UserID references the mentioned user
	UserID uint64 `json:"user_id" gorm:"primaryKey;index:idx_message_mentions_user"`

	// CreatedAt timestamp when the mention was recorded
	CreatedAt time.Time `json:"created_at" gorm:"type:timestamptz;autoCreateTime;not null"`

	// Message is the associated message record (when preloaded)
	Message ChatMessage `json:"message" gorm:"foreignKey:MessageID;constraint:OnUpdate:CASCADE,OnDelete:CASCADE;"`

	// User is the associated user record (when preloaded)
	User User `json:"user" gorm:"foreignKey:UserID;constraint:OnUpdate:CASCADE,OnDelete:CASCADE;"`
}

// TableName executes this method as part of the persistence model flow for database reads, writes, and ORM mappings.
func (ChatMessageMention) TableName() string {
	return TableNameChatMessageMentions
}
