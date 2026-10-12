// Package common_persistence_models contains the database models used by the persistence layer.
// This file defines the ChatChannelParticipant model for tracking channel membership.
package common_persistence_models

import (
	"time"

	"github.com/Suhaibinator/go-common/pkg/proto_models"
	"gorm.io/gorm"
)

// ChatChannelParticipant defines the data structure used in the persistence model flow for database reads, writes, and ORM mappings.
// It carries state across layer boundaries handled by this package.
type ChatChannelParticipant struct {
	// ChannelID references the channel the user is a member of
	ChannelID uint64 `json:"channel_id" gorm:"primaryKey;index:idx_participants_channel"`

	// UserID references the user who is a member of the channel
	UserID uint64 `json:"user_id" gorm:"primaryKey;index:idx_participants_user_left"`

	// Role defines the user's role and permissions within the channel
	Role proto_models.ChannelRole `json:"role" gorm:"type:int;not null;default:1;index"` // Default to CHANNEL_ROLE_MEMBER

	// NotificationMode controls interruptions without hiding inbox activity.
	NotificationMode proto_models.ChannelNotificationMode `json:"notification_mode" gorm:"type:int;not null;default:1"`

	// IsPinned indicates whether the user has pinned this channel
	IsPinned bool `json:"is_pinned" gorm:"not null;default:false"`

	// LastReadMessageID tracks the last message the user has read (for read receipts)
	LastReadMessageID *uint64 `json:"last_read_message_id,omitempty"`

	// JoinedAt timestamp when the user joined the channel
	JoinedAt time.Time `json:"joined_at" gorm:"type:timestamptz;not null"`

	// NotificationJoin is the server-owned membership generation used by queued
	// notifications. A missing or foreign identity uses the unstamped baseline.
	NotificationJoin NotificationTransactionStamp `json:"-" gorm:"embedded;embeddedPrefix:notification_join_"`

	// UpdatedAt timestamp when the participation record was last modified
	UpdatedAt time.Time `json:"updated_at" gorm:"type:timestamptz;autoUpdateTime"`

	// LeftAt timestamp when the user left the channel (soft leave - NULL means active)
	LeftAt *time.Time `json:"left_at,omitempty" gorm:"type:timestamptz;index:idx_participants_user_left"`

	// Channel is the associated channel record (when preloaded)
	Channel ChatChannel `json:"channel" gorm:"foreignKey:ChannelID;constraint:OnUpdate:CASCADE,OnDelete:CASCADE;"`

	// User is the associated user record (when preloaded)
	User User `json:"user" gorm:"foreignKey:UserID;constraint:OnUpdate:CASCADE,OnDelete:CASCADE;"`
}

// ChatChannelParticipantsBatchItem defines the data structure used in the persistence model flow for database reads, writes, and ORM mappings.
// It carries state across layer boundaries handled by this package.
type ChatChannelParticipantsBatchItem struct {
	ChannelID    uint64
	Participants []*ChatChannelParticipant
	Pagination   *proto_models.PaginationResponse
}

// BeforeCreate executes this method as part of the persistence model flow for database reads, writes, and ORM mappings.
func (cp *ChatChannelParticipant) BeforeCreate(tx *gorm.DB) error {
	cp.JoinedAt = time.Now()
	return nil
}

// TableName executes this method as part of the persistence model flow for database reads, writes, and ORM mappings.
func (ChatChannelParticipant) TableName() string {
	return TableNameChatChannelParticipants
}
