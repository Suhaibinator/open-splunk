// Package common_persistence_models contains the database models used by the persistence layer.
// This file defines the ChatChannel model for chat/messaging functionality.
package common_persistence_models

import (
	"time"

	"github.com/Suhaibinator/go-common/pkg/proto_models"
	"gorm.io/gorm"
)

// ChatChannel defines the data structure used in the persistence model flow for database reads, writes, and ORM mappings.
// It carries state across layer boundaries handled by this package.
type ChatChannel struct {
	// ID is the unique identifier for the channel (auto-incrementing primary key)
	ID uint64 `json:"id" gorm:"primaryKey;autoIncrement"`

	// Name is the optional display name of the channel (primarily for group chats)
	Name string `json:"name" gorm:"type:varchar(255)"`

	// Description is an optional description of the channel
	Description *string `json:"description,omitempty" gorm:"type:text"`

	// ContextID is an optional identifier for channel context (e.g., tenancy)
	ContextID *uint64 `json:"context_id,omitempty" gorm:"index"`

	// ContextDisplayName is the optional display name for the channel context
	ContextDisplayName *string `json:"context_display_name,omitempty" gorm:"type:varchar(255)"`

	// ContextEntityID is an optional identifier for the context entity (e.g., org/user/department)
	ContextEntityID *uint64 `json:"context_entity_id,omitempty"`

	// OrganizationID is the organization the channel is associated with for org-derived
	// access. Nil means the channel has no organization and grants no org-derived access,
	// whatever OrgMinRoleRead holds. It is independent of ContextID/ContextEntityID, which
	// are opaque application scoping values. Set through SetChatChannelOrganizationAccess.
	OrganizationID *int64 `json:"organization_id,omitempty" gorm:"index"`

	// OrgMinRoleRead is the minimum EFFECTIVE role (lineage-aware: inherited from ancestor
	// organizations as well as explicit membership) a user must hold in OrganizationID to
	// read the channel without being a participant. ORGANIZATION_ROLE_UNSPECIFIED (0, the
	// default) grants no org-derived read access. OrganizationRole is an ordered enum, so
	// the check is `effectiveRole >= OrgMinRoleRead`. Mutations always require participation.
	// The column follows the OrgMin* threshold convention: one column per permission, so a
	// future write threshold is an additional OrgMinRoleWrite column rather than a reshape.
	OrgMinRoleRead proto_models.OrganizationRole `json:"org_min_role_read" gorm:"type:int;not null;default:0"`

	// PublicRead is who beyond participants may read the channel (see ChatChannelPublicRead); it
	// grants only while enable_public_chat_read is on. Set through SetChatChannelPublicRead.
	PublicRead proto_models.ChatChannelPublicRead `json:"public_read" gorm:"type:int;not null;default:0"`

	// AvatarAttachmentID references the attachment used as channel avatar
	AvatarAttachmentID *uint64 `json:"avatar_attachment_id,omitempty" gorm:"index"`

	// AvatarAttachment is the loaded attachment relation (when preloaded)
	AvatarAttachment *Attachment `json:"avatar_attachment,omitempty" gorm:"foreignKey:AvatarAttachmentID;constraint:OnUpdate:CASCADE,OnDelete:SET NULL;"`

	// ChannelType indicates whether this is a direct (1:1) or group channel
	ChannelType proto_models.ChannelType `json:"channel_type" gorm:"type:int;not null;index"`

	// IsArchived indicates whether the channel has been archived
	IsArchived bool `json:"is_archived" gorm:"not null;default:false"`

	// UnreadNotificationsEnabled controls ordinary unread notifications, not tool approvals.
	UnreadNotificationsEnabled bool `json:"unread_notifications_enabled" gorm:"not null;default:true"`

	// Features is the per-channel feature-flag bitmask (see ChatChannelFeatures).
	Features ChatChannelFeatures `json:"features" gorm:"type:bigint;not null;default:0"`

	// ForkRules are set at creation and never change; a fork copies its source's rules.
	ForkRules ChatChannelForkRules `json:"fork_rules" gorm:"embedded;embeddedPrefix:fork_"`

	// ForkedFromChannelID and ForkPointMessageID record the source channel and the requested fork
	// point of a fork; in-flight agent work at that message is cut, so it may not be copied. NULL on
	// channels that are not forks. No foreign keys: the source may be soft-deleted without
	// affecting the fork.
	ForkedFromChannelID *uint64 `json:"forked_from_channel_id,omitempty" gorm:"index"`
	ForkPointMessageID  *uint64 `json:"fork_point_message_id,omitempty" gorm:"index"`

	// ForkRequestKey is the forker's idempotency key of the fork that created this channel. The
	// partial unique index idx_chat_channels_fork_request_key scopes it to CreatedByUserID for the
	// channel's whole lifetime, soft deletion included. NULL when the fork request carried none.
	ForkRequestKey *string `json:"fork_request_key,omitempty" gorm:"type:text;uniqueIndex:idx_chat_channels_fork_request_key,priority:2"`

	// CreatedByUserID references the user who created the channel
	CreatedByUserID uint64 `json:"created_by_user_id" gorm:"not null;index;uniqueIndex:idx_chat_channels_fork_request_key,priority:1,where:fork_request_key IS NOT NULL"`

	// CreatedByUser is the loaded creator user relation (when preloaded)
	CreatedByUser *User `json:"created_by_user,omitempty" gorm:"foreignKey:CreatedByUserID"`

	// LastMessageAt timestamp of the most recent message (for sorting channels by activity)
	LastMessageAt *time.Time `json:"last_message_at,omitempty" gorm:"type:timestamptz;index"`

	// LastMessagePreview contains a preview of the most recent message content
	LastMessagePreview *string `json:"last_message_preview,omitempty" gorm:"type:varchar(255)"`

	// LastMessageSenderID is the sender of the message LastMessagePreview
	// describes. It is maintained with the preview and is NULL when no visible
	// message remains or the row predates the column.
	LastMessageSenderID *uint64 `json:"last_message_sender_id,omitempty"`

	// CreatedAt timestamp when the channel was created
	CreatedAt time.Time `json:"created_at" gorm:"type:timestamptz"`

	// UpdatedAt timestamp when the channel was last modified
	UpdatedAt time.Time `json:"updated_at" gorm:"type:timestamptz;autoUpdateTime"`

	// DeletedAt enables GORM soft delete behavior
	DeletedAt gorm.DeletedAt `json:"deleted_at" gorm:"index"`

	// Participants contains the channel participants (when preloaded)
	Participants []ChatChannelParticipant `json:"participants,omitempty" gorm:"foreignKey:ChannelID"`

	// Messages contains the channel messages (when preloaded)
	Messages []ChatMessage `json:"messages,omitempty" gorm:"foreignKey:ChannelID"`
}

// ChatChannelForkRules are a channel's fork rules, stored as fork_who_may_fork, fork_member_pool,
// fork_org_access and fork_context. The zero value disables forking.
type ChatChannelForkRules struct {
	WhoMayFork proto_models.ChatForkWhoMayFork `json:"who_may_fork" gorm:"type:int;not null;default:0"`
	MemberPool proto_models.ChatForkMemberPool `json:"member_pool" gorm:"type:int;not null;default:0"`
	OrgAccess  proto_models.ChatForkCarryOver  `json:"org_access" gorm:"type:int;not null;default:0"`
	Context    proto_models.ChatForkCarryOver  `json:"context" gorm:"type:int;not null;default:0"`
}

// Valid reports whether every rule is a defined enum value.
func (r ChatChannelForkRules) Valid() bool {
	_, who := proto_models.ChatForkWhoMayFork_name[int32(r.WhoMayFork)]
	_, pool := proto_models.ChatForkMemberPool_name[int32(r.MemberPool)]
	_, org := proto_models.ChatForkCarryOver_name[int32(r.OrgAccess)]
	_, context := proto_models.ChatForkCarryOver_name[int32(r.Context)]
	return who && pool && org && context
}

// ChatChannelFeatures is a channel's feature-flag bitmask, stored in one bigint column.
// Bit positions are a storage contract: never reuse or
// renumber one, and leave bit 63 unused because PostgreSQL bigint is signed.
type ChatChannelFeatures uint64

const (
	ChatChannelSupportReplies                ChatChannelFeatures = 1 << 0
	ChatChannelSupportThreads                ChatChannelFeatures = 1 << 1
	ChatChannelSupportMessageEdits           ChatChannelFeatures = 1 << 2
	ChatChannelSupportMessageDeletes         ChatChannelFeatures = 1 << 3
	ChatChannelSupportMessageReactions       ChatChannelFeatures = 1 << 4
	ChatChannelAllowMultipleReactionsPerUser ChatChannelFeatures = 1 << 5
	ChatChannelSupportEmbeds                 ChatChannelFeatures = 1 << 6
	ChatChannelSupportAttachments            ChatChannelFeatures = 1 << 7
	ChatChannelAllowNativeEmbeds             ChatChannelFeatures = 1 << 8
	ChatChannelAllowCustomEmbeds             ChatChannelFeatures = 1 << 9
	ChatChannelSupportInferenceUsage         ChatChannelFeatures = 1 << 10
)

// Has reports whether every bit of flag is set.
func (f ChatChannelFeatures) Has(flag ChatChannelFeatures) bool { return f&flag == flag }

// ChatChannelFeatureFlags defines the data structure used in the persistence model flow for database reads, writes, and ORM mappings.
// It carries state across layer boundaries handled by this package.
type ChatChannelFeatureFlags struct {
	// Nil preserves the ordinary channel default of enabled at creation.
	UnreadNotificationsEnabled *bool
	Features                   ChatChannelFeatures
	// ForkRules are stored on the new channel and cannot be changed afterwards.
	ForkRules ChatChannelForkRules
}

// TableName executes this method as part of the persistence model flow for database reads, writes, and ORM mappings.
func (ChatChannel) TableName() string {
	return TableNameChatChannels
}
