package common_service_models

import (
	"time"

	pm "github.com/Suhaibinator/go-common/pkg/common_persistence/common_persistence_models"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
)

// ChatChannelIdType defines the type used for chat channel IDs throughout the service layer.
type ChatChannelIdType = uint64

// ChatMessageIdType defines the type used for chat message IDs throughout the service layer.
type ChatMessageIdType = uint64

// ChatChannelOrganizationAccess is the org-derived access a chat channel owns: the
// organization it is associated with and the minimum effective organization role per
// permission. It maps the persistence columns ChatChannel.OrganizationID and
// ChatChannel.OrgMinRoleRead.
type ChatChannelOrganizationAccess struct {
	// OrganizationID is the organization whose members may receive org-derived access.
	OrganizationID OrganizationIdType `json:"organization_id"`
	// MinRoleRead is the minimum EFFECTIVE (lineage-aware) organization role that may read
	// the channel without participating. ORGANIZATION_ROLE_UNSPECIFIED grants no read access.
	// Mutations always require participation.
	MinRoleRead proto_models.OrganizationRole `json:"min_role_read"`
}

// ChatChannel defines a data model used by the common_service_models backend flow.
// It carries state exchanged between handlers, services, and persistence boundaries.
type ChatChannel struct {
	ID                         ChatChannelIdType        `json:"id"`
	Name                       string                   `json:"name"`
	Description                *string                  `json:"description,omitempty"`
	ContextID                  *uint64                  `json:"context_id,omitempty"`
	ContextDisplayName         *string                  `json:"context_display_name,omitempty"`
	ContextEntityID            *uint64                  `json:"context_entity_id,omitempty"`
	AvatarAttachmentID         *uint64                  `json:"avatar_attachment_id,omitempty"`
	AvatarUrl                  string                   `json:"avatar_url"`
	ChannelType                proto_models.ChannelType `json:"channel_type"`
	IsArchived                 bool                     `json:"is_archived"`
	CreatedByUserID            UserIdType               `json:"created_by_user_id"`
	LastMessageAt              *time.Time               `json:"last_message_at,omitempty"`
	LastMessagePreview         *string                  `json:"last_message_preview,omitempty"`
	CreatedAt                  time.Time                `json:"created_at"`
	UpdatedAt                  time.Time                `json:"updated_at"`
	DeletedAt                  *time.Time               `json:"deleted_at,omitempty"`
	UnreadNotificationsEnabled bool                     `json:"unread_notifications_enabled"`
	Features                   pm.ChatChannelFeatures   `json:"features"`

	// ForkRules are the channel's fork rules, fixed at creation. ForkedFromChannelID and
	// ForkPointMessageID are a fork's provenance and nil on every other channel.
	ForkRules           pm.ChatChannelForkRules `json:"fork_rules"`
	ForkedFromChannelID *uint64                 `json:"forked_from_channel_id,omitempty"`
	ForkPointMessageID  *uint64                 `json:"fork_point_message_id,omitempty"`

	// OrganizationAccess is the org-derived access the channel owns; nil when the channel is
	// not associated with an organization. It is service-layer authorization state and is
	// deliberately absent from ChatChannelLike and the proto Channel.
	OrganizationAccess *ChatChannelOrganizationAccess `json:"organization_access,omitempty"`

	// PublicRead is who beyond participants may read the channel; it grants only while
	// enable_public_chat_read is on. Unlike OrganizationAccess it reaches the proto Channel.
	PublicRead proto_models.ChatChannelPublicRead `json:"public_read"`
}

type ChatChannelLike interface {
	GetID() ChatChannelIdType
	GetName() string
	GetDescription() *string
	GetContextID() *uint64
	GetContextDisplayName() *string
	GetContextEntityID() *uint64
	GetAvatarAttachmentID() *uint64
	GetAvatarUrl() string
	GetChannelType() proto_models.ChannelType
	GetIsArchived() bool
	GetCreatedByUserID() UserIdType
	GetLastMessageAt() *time.Time
	GetLastMessagePreview() *string
	GetCreatedAt() time.Time
	GetUpdatedAt() time.Time
	GetDeletedAt() *time.Time
	GetUnreadNotificationsEnabled() bool
	GetSupportReplies() bool
	GetSupportThreads() bool
	GetSupportMessageEdits() bool
	GetSupportMessageDeletes() bool
	GetSupportMessageReactions() bool
	GetAllowMultipleReactionsPerUser() bool
	GetSupportEmbeds() bool
	GetSupportAttachments() bool
	GetAllowNativeEmbeds() bool
	GetAllowCustomEmbeds() bool
	GetSupportInferenceUsage() bool
	GetPublicRead() proto_models.ChatChannelPublicRead
	GetForkRules() pm.ChatChannelForkRules
	GetForkedFromChannelID() *uint64
	GetForkPointMessageID() *uint64
}

var _ ChatChannelLike = (*ChatChannel)(nil)

// GetID returns the requested value for *ChatChannel in the common_service_models backend flow.
func (c *ChatChannel) GetID() ChatChannelIdType { return c.ID }

// GetName returns the requested value for *ChatChannel in the common_service_models backend flow.
func (c *ChatChannel) GetName() string { return c.Name }

// GetDescription returns the requested value for *ChatChannel in the common_service_models backend flow.
func (c *ChatChannel) GetDescription() *string { return c.Description }

// GetContextID returns the requested value for *ChatChannel in the common_service_models backend flow.
func (c *ChatChannel) GetContextID() *uint64 { return c.ContextID }

// GetContextDisplayName returns the requested value for *ChatChannel in the common_service_models backend flow.
func (c *ChatChannel) GetContextDisplayName() *string { return c.ContextDisplayName }

// GetContextEntityID returns the requested value for *ChatChannel in the common_service_models backend flow.
func (c *ChatChannel) GetContextEntityID() *uint64 { return c.ContextEntityID }

// GetAvatarAttachmentID returns the requested value for *ChatChannel in the common_service_models backend flow.
func (c *ChatChannel) GetAvatarAttachmentID() *uint64 { return c.AvatarAttachmentID }

// GetAvatarUrl returns the requested value for *ChatChannel in the common_service_models backend flow.
func (c *ChatChannel) GetAvatarUrl() string { return c.AvatarUrl }

// GetChannelType returns the requested value for *ChatChannel in the common_service_models backend flow.
func (c *ChatChannel) GetChannelType() proto_models.ChannelType { return c.ChannelType }

// GetIsArchived returns the requested value for *ChatChannel in the common_service_models backend flow.
func (c *ChatChannel) GetIsArchived() bool { return c.IsArchived }

// GetCreatedByUserID returns the requested value for *ChatChannel in the common_service_models backend flow.
func (c *ChatChannel) GetCreatedByUserID() UserIdType { return c.CreatedByUserID }

// GetLastMessageAt returns the requested value for *ChatChannel in the common_service_models backend flow.
func (c *ChatChannel) GetLastMessageAt() *time.Time { return c.LastMessageAt }

// GetLastMessagePreview returns the requested value for *ChatChannel in the common_service_models backend flow.
func (c *ChatChannel) GetLastMessagePreview() *string { return c.LastMessagePreview }

// GetCreatedAt returns the requested value for *ChatChannel in the common_service_models backend flow.
func (c *ChatChannel) GetCreatedAt() time.Time { return c.CreatedAt }

// GetUpdatedAt returns the requested value for *ChatChannel in the common_service_models backend flow.
func (c *ChatChannel) GetUpdatedAt() time.Time { return c.UpdatedAt }

// GetDeletedAt returns the requested value for *ChatChannel in the common_service_models backend flow.
func (c *ChatChannel) GetDeletedAt() *time.Time { return c.DeletedAt }

// GetUnreadNotificationsEnabled reports whether ordinary unread notifications are enabled.
func (c *ChatChannel) GetUnreadNotificationsEnabled() bool { return c.UnreadNotificationsEnabled }

// GetSupportReplies returns the requested value for *ChatChannel in the common_service_models backend flow.
func (c *ChatChannel) GetSupportReplies() bool { return c.Features.Has(pm.ChatChannelSupportReplies) }

// GetSupportThreads returns the requested value for *ChatChannel in the common_service_models backend flow.
func (c *ChatChannel) GetSupportThreads() bool { return c.Features.Has(pm.ChatChannelSupportThreads) }

// GetSupportMessageEdits returns the requested value for *ChatChannel in the common_service_models backend flow.
func (c *ChatChannel) GetSupportMessageEdits() bool {
	return c.Features.Has(pm.ChatChannelSupportMessageEdits)
}

// GetSupportMessageDeletes returns the requested value for *ChatChannel in the common_service_models backend flow.
func (c *ChatChannel) GetSupportMessageDeletes() bool {
	return c.Features.Has(pm.ChatChannelSupportMessageDeletes)
}

// GetSupportMessageReactions returns the requested value for *ChatChannel in the common_service_models backend flow.
func (c *ChatChannel) GetSupportMessageReactions() bool {
	return c.Features.Has(pm.ChatChannelSupportMessageReactions)
}

// GetAllowMultipleReactionsPerUser returns the requested value for *ChatChannel in the common_service_models backend flow.
func (c *ChatChannel) GetAllowMultipleReactionsPerUser() bool {
	return c.Features.Has(pm.ChatChannelAllowMultipleReactionsPerUser)
}

// GetSupportEmbeds returns the requested value for *ChatChannel in the common_service_models backend flow.
func (c *ChatChannel) GetSupportEmbeds() bool { return c.Features.Has(pm.ChatChannelSupportEmbeds) }

// GetSupportAttachments returns the requested value for *ChatChannel in the common_service_models backend flow.
func (c *ChatChannel) GetSupportAttachments() bool {
	return c.Features.Has(pm.ChatChannelSupportAttachments)
}

// GetAllowNativeEmbeds returns the requested value for *ChatChannel in the common_service_models backend flow.
func (c *ChatChannel) GetAllowNativeEmbeds() bool {
	return c.Features.Has(pm.ChatChannelAllowNativeEmbeds)
}

// GetAllowCustomEmbeds returns the requested value for *ChatChannel in the common_service_models backend flow.
func (c *ChatChannel) GetAllowCustomEmbeds() bool {
	return c.Features.Has(pm.ChatChannelAllowCustomEmbeds)
}

// GetSupportInferenceUsage reports whether the channel's readers may see token usage.
func (c *ChatChannel) GetSupportInferenceUsage() bool {
	return c.Features.Has(pm.ChatChannelSupportInferenceUsage)
}

// GetPublicRead returns who beyond participants may read the channel.
func (c *ChatChannel) GetPublicRead() proto_models.ChatChannelPublicRead { return c.PublicRead }

// GetForkRules returns the channel's fork rules.
func (c *ChatChannel) GetForkRules() pm.ChatChannelForkRules {
	return c.ForkRules
}

// GetForkedFromChannelID returns a fork's source channel, or nil.
func (c *ChatChannel) GetForkedFromChannelID() *uint64 { return c.ForkedFromChannelID }

// GetForkPointMessageID returns a fork's requested fork point, or nil.
func (c *ChatChannel) GetForkPointMessageID() *uint64 { return c.ForkPointMessageID }

// ChatChannelParticipant defines a data model used by the common_service_models backend flow.
// It carries state exchanged between handlers, services, and persistence boundaries.
type ChatChannelParticipant struct {
	ChannelID         ChatChannelIdType                    `json:"channel_id"`
	UserID            UserIdType                           `json:"user_id"`
	Role              proto_models.ChannelRole             `json:"role"`
	NotificationMode  proto_models.ChannelNotificationMode `json:"notification_mode"`
	IsPinned          bool                                 `json:"is_pinned"`
	LastReadMessageID *ChatMessageIdType                   `json:"last_read_message_id,omitempty"`
	JoinedAt          time.Time                            `json:"joined_at"`
	UpdatedAt         time.Time                            `json:"updated_at"`
	LeftAt            *time.Time                           `json:"left_at,omitempty"`
	User              *User                                `json:"user,omitempty"`
}

// ChatChannelParticipantsBatchItem defines a data model used by the common_service_models backend flow.
// It carries state exchanged between handlers, services, and persistence boundaries.
type ChatChannelParticipantsBatchItem struct {
	ChannelID    ChatChannelIdType                `json:"channel_id"`
	Participants []*ChatChannelParticipant        `json:"participants"`
	Pagination   *proto_models.PaginationResponse `json:"pagination,omitempty"`
}

type ChatChannelParticipantLike interface {
	GetChannelID() ChatChannelIdType
	GetUserID() UserIdType
	GetRole() proto_models.ChannelRole
	GetNotificationMode() proto_models.ChannelNotificationMode
	GetIsPinned() bool
	GetLastReadMessageID() *ChatMessageIdType
	GetJoinedAt() time.Time
	GetUpdatedAt() time.Time
	GetLeftAt() *time.Time
	GetUser() UserLike
}

var _ ChatChannelParticipantLike = (*ChatChannelParticipant)(nil)

// GetChannelID returns the requested value for *ChatChannelParticipant in the common_service_models backend flow.
func (p *ChatChannelParticipant) GetChannelID() ChatChannelIdType { return p.ChannelID }

// GetUserID returns the requested value for *ChatChannelParticipant in the common_service_models backend flow.
func (p *ChatChannelParticipant) GetUserID() UserIdType { return p.UserID }

// GetRole returns the requested value for *ChatChannelParticipant in the common_service_models backend flow.
func (p *ChatChannelParticipant) GetRole() proto_models.ChannelRole { return p.Role }

// GetNotificationMode returns the requested value for *ChatChannelParticipant in the common_service_models backend flow.
func (p *ChatChannelParticipant) GetNotificationMode() proto_models.ChannelNotificationMode {
	return p.NotificationMode
}

// GetIsPinned returns the requested value for *ChatChannelParticipant in the common_service_models backend flow.
func (p *ChatChannelParticipant) GetIsPinned() bool { return p.IsPinned }

// GetLastReadMessageID returns the requested value for *ChatChannelParticipant in the common_service_models backend flow.
func (p *ChatChannelParticipant) GetLastReadMessageID() *ChatMessageIdType {
	return p.LastReadMessageID
}

// GetJoinedAt returns the requested value for *ChatChannelParticipant in the common_service_models backend flow.
func (p *ChatChannelParticipant) GetJoinedAt() time.Time { return p.JoinedAt }

// GetUpdatedAt returns the requested value for *ChatChannelParticipant in the common_service_models backend flow.
func (p *ChatChannelParticipant) GetUpdatedAt() time.Time { return p.UpdatedAt }

// GetLeftAt returns the requested value for *ChatChannelParticipant in the common_service_models backend flow.
func (p *ChatChannelParticipant) GetLeftAt() *time.Time { return p.LeftAt }

// GetUser returns the requested value for *ChatChannelParticipant in the common_service_models backend flow.
func (p *ChatChannelParticipant) GetUser() UserLike {
	if p.User == nil {
		return nil
	}
	return p.User
}

// ChatMessage defines a data model used by the common_service_models backend flow.
// It carries state exchanged between handlers, services, and persistence boundaries.
type ChatMessage struct {
	ID               ChatMessageIdType               `json:"id"`
	ChannelID        ChatChannelIdType               `json:"channel_id"`
	SenderID         UserIdType                      `json:"sender_id"`
	Content          string                          `json:"content"`
	ContentType      proto_models.MessageContentType `json:"content_type"`
	StreamState      proto_models.MessageStreamState `json:"stream_state"`
	ParentMessageID  *ChatMessageIdType              `json:"parent_message_id,omitempty"`
	ReplyToMessageID *ChatMessageIdType              `json:"reply_to_message_id,omitempty"`
	// BroadcastToChannel marks a thread reply that also appears on the main timeline;
	// ParentMessageID still names its thread root.
	BroadcastToChannel bool                               `json:"broadcast_to_channel"`
	IsEdited           bool                               `json:"is_edited"`
	EditedAt           *time.Time                         `json:"edited_at,omitempty"`
	OriginalContent    *string                            `json:"original_content,omitempty"`
	CreatedAt          time.Time                          `json:"created_at"`
	UpdatedAt          time.Time                          `json:"updated_at"`
	DeletedAt          *time.Time                         `json:"deleted_at,omitempty"`
	Sender             *User                              `json:"sender,omitempty"`
	Embeds             []*ChatMessageEmbed                `json:"embeds,omitempty"`
	AttachmentIDs      []uint64                           `json:"attachment_ids,omitempty"`
	Attachments        []*proto_models.UploadedAttachment `json:"attachments,omitempty"`
	Reactions          []*ChatMessageReaction             `json:"reactions,omitempty"`
	Mentions           []*ChatMessageMention              `json:"mentions,omitempty"`
	ReplyCount         uint32                             `json:"reply_count"`

	// Agentic transcript fields (inline on the message row). Kind/Role discriminate transcript
	// rows; the visibility bools use field presence; AgentRunID/AgentStepIndex group and order a
	// run's rows. ToolCall carries the structured payload + approval/execution state for
	// TOOL_CALL/TOOL_RESULT rows (reassembled from the persistence ChatMessageToolCall side row).
	Kind                 proto_models.MessageKind `json:"kind"`
	Role                 proto_models.MessageRole `json:"role"`
	ExcludeFromAIContext *bool                    `json:"exclude_from_ai_context,omitempty"`
	HiddenFromUsers      *bool                    `json:"hidden_from_users,omitempty"`
	AgentRunID           *uint64                  `json:"agent_run_id,omitempty"`
	AgentStepIndex       *uint32                  `json:"agent_step_index,omitempty"`
	AgentNoteSubtype     string                   `json:"agent_note_subtype,omitempty"`
	ToolCall             *ChatMessageToolCall     `json:"tool_call,omitempty"`
}

type ChatMessageLike interface {
	GetID() ChatMessageIdType
	GetChannelID() ChatChannelIdType
	GetSenderID() UserIdType
	GetContent() string
	GetContentType() proto_models.MessageContentType
	GetStreamState() proto_models.MessageStreamState
	GetParentMessageID() *ChatMessageIdType
	GetReplyToMessageID() *ChatMessageIdType
	GetBroadcastToChannel() bool
	GetIsEdited() bool
	GetEditedAt() *time.Time
	GetOriginalContent() *string
	GetCreatedAt() time.Time
	GetUpdatedAt() time.Time
	GetDeletedAt() *time.Time
	GetSender() UserLike
	GetEmbeds() []*ChatMessageEmbed
	GetAttachmentIDs() []uint64
	GetAttachments() []*proto_models.UploadedAttachment
	GetReactions() []*ChatMessageReaction
	GetMentions() []*ChatMessageMention
	GetReplyCount() uint32
	GetKind() proto_models.MessageKind
	GetRole() proto_models.MessageRole
	GetExcludeFromAIContext() *bool
	GetHiddenFromUsers() *bool
	GetAgentRunID() *uint64
	GetAgentStepIndex() *uint32
	GetAgentNoteSubtype() string
	GetToolCall() *ChatMessageToolCall
}

var _ ChatMessageLike = (*ChatMessage)(nil)

// GetID returns the requested value for *ChatMessage in the common_service_models backend flow.
func (m *ChatMessage) GetID() ChatMessageIdType { return m.ID }

// GetChannelID returns the requested value for *ChatMessage in the common_service_models backend flow.
func (m *ChatMessage) GetChannelID() ChatChannelIdType { return m.ChannelID }

// GetSenderID returns the requested value for *ChatMessage in the common_service_models backend flow.
func (m *ChatMessage) GetSenderID() UserIdType { return m.SenderID }

// GetContent returns the requested value for *ChatMessage in the common_service_models backend flow.
func (m *ChatMessage) GetContent() string { return m.Content }

// GetContentType returns the requested value for *ChatMessage in the common_service_models backend flow.
func (m *ChatMessage) GetContentType() proto_models.MessageContentType { return m.ContentType }

// GetStreamState returns the requested value for *ChatMessage in the common_service_models backend flow.
func (m *ChatMessage) GetStreamState() proto_models.MessageStreamState { return m.StreamState }

// GetParentMessageID returns the requested value for *ChatMessage in the common_service_models backend flow.
func (m *ChatMessage) GetParentMessageID() *ChatMessageIdType { return m.ParentMessageID }

// GetReplyToMessageID returns the requested value for *ChatMessage in the common_service_models backend flow.
func (m *ChatMessage) GetReplyToMessageID() *ChatMessageIdType { return m.ReplyToMessageID }

// GetBroadcastToChannel reports whether this thread reply is also shown on the main timeline.
func (m *ChatMessage) GetBroadcastToChannel() bool { return m.BroadcastToChannel }

// GetIsEdited returns the requested value for *ChatMessage in the common_service_models backend flow.
func (m *ChatMessage) GetIsEdited() bool { return m.IsEdited }

// GetEditedAt returns the requested value for *ChatMessage in the common_service_models backend flow.
func (m *ChatMessage) GetEditedAt() *time.Time { return m.EditedAt }

// GetOriginalContent returns the requested value for *ChatMessage in the common_service_models backend flow.
func (m *ChatMessage) GetOriginalContent() *string { return m.OriginalContent }

// GetCreatedAt returns the requested value for *ChatMessage in the common_service_models backend flow.
func (m *ChatMessage) GetCreatedAt() time.Time { return m.CreatedAt }

// GetUpdatedAt returns the requested value for *ChatMessage in the common_service_models backend flow.
func (m *ChatMessage) GetUpdatedAt() time.Time { return m.UpdatedAt }

// GetDeletedAt returns the requested value for *ChatMessage in the common_service_models backend flow.
func (m *ChatMessage) GetDeletedAt() *time.Time { return m.DeletedAt }

// GetSender returns the requested value for *ChatMessage in the common_service_models backend flow.
func (m *ChatMessage) GetSender() UserLike {
	if m.Sender == nil {
		return nil
	}
	return m.Sender
}

// GetEmbeds returns the requested value for *ChatMessage in the common_service_models backend flow.
func (m *ChatMessage) GetEmbeds() []*ChatMessageEmbed { return m.Embeds }

// GetAttachmentIDs returns the requested value for *ChatMessage in the common_service_models backend flow.
func (m *ChatMessage) GetAttachmentIDs() []uint64 { return m.AttachmentIDs }

// GetAttachments returns the requested value for *ChatMessage in the common_service_models backend flow.
func (m *ChatMessage) GetAttachments() []*proto_models.UploadedAttachment {
	return m.Attachments
}

// GetReactions returns the requested value for *ChatMessage in the common_service_models backend flow.
func (m *ChatMessage) GetReactions() []*ChatMessageReaction { return m.Reactions }

// GetMentions returns the requested value for *ChatMessage in the common_service_models backend flow.
func (m *ChatMessage) GetMentions() []*ChatMessageMention { return m.Mentions }

// GetReplyCount returns the requested value for *ChatMessage in the common_service_models backend flow.
func (m *ChatMessage) GetReplyCount() uint32 { return m.ReplyCount }

// GetKind returns the transcript message kind.
func (m *ChatMessage) GetKind() proto_models.MessageKind { return m.Kind }

// GetRole returns the explicit LLM role.
func (m *ChatMessage) GetRole() proto_models.MessageRole { return m.Role }

// GetExcludeFromAIContext returns the presence-tracked exclude-from-context flag.
func (m *ChatMessage) GetExcludeFromAIContext() *bool { return m.ExcludeFromAIContext }

// GetHiddenFromUsers returns the presence-tracked hidden-from-users flag.
func (m *ChatMessage) GetHiddenFromUsers() *bool { return m.HiddenFromUsers }

// GetAgentRunID returns the run grouping key, when set.
func (m *ChatMessage) GetAgentRunID() *uint64 { return m.AgentRunID }

// GetAgentStepIndex returns the run step index, when set.
func (m *ChatMessage) GetAgentStepIndex() *uint32 { return m.AgentStepIndex }

// GetAgentNoteSubtype returns the control subtype of an AGENT_NOTE row; empty on other kinds.
func (m *ChatMessage) GetAgentNoteSubtype() string { return m.AgentNoteSubtype }

// GetToolCall returns the structured tool-call payload + approval/execution state, when present.
func (m *ChatMessage) GetToolCall() *ChatMessageToolCall { return m.ToolCall }

// ChatMessageEditHistoryEntry defines a data model used by the common_service_models backend flow.
// It carries state exchanged between handlers, services, and persistence boundaries.
type ChatMessageEditHistoryEntry struct {
	ID               uint64                          `json:"id"`
	MessageID        ChatMessageIdType               `json:"message_id"`
	VersionNumber    uint32                          `json:"version_number"`
	EditedByUserID   UserIdType                      `json:"edited_by_user_id"`
	VersionStartedAt time.Time                       `json:"version_started_at"`
	VersionEndedAt   time.Time                       `json:"version_ended_at"`
	Content          string                          `json:"content"`
	ContentType      proto_models.MessageContentType `json:"content_type"`
	Embeds           []*ChatMessageEmbed             `json:"embeds,omitempty"`
	AttachmentIDs    []uint64                        `json:"attachment_ids,omitempty"`
	MentionUserIDs   []uint64                        `json:"mention_user_ids,omitempty"`
	CreatedAt        time.Time                       `json:"created_at"`
	EditedByUser     *User                           `json:"edited_by_user,omitempty"`
}

type ChatMessageEditHistoryEntryLike interface {
	GetID() uint64
	GetMessageID() ChatMessageIdType
	GetVersionNumber() uint32
	GetEditedByUserID() UserIdType
	GetVersionStartedAt() time.Time
	GetVersionEndedAt() time.Time
	GetContent() string
	GetContentType() proto_models.MessageContentType
	GetEmbeds() []*ChatMessageEmbed
	GetAttachmentIDs() []uint64
	GetMentionUserIDs() []uint64
	GetCreatedAt() time.Time
	GetEditedByUser() UserLike
}

var _ ChatMessageEditHistoryEntryLike = (*ChatMessageEditHistoryEntry)(nil)

// GetID returns the requested value for *ChatMessageEditHistoryEntry in the common_service_models backend flow.
func (e *ChatMessageEditHistoryEntry) GetID() uint64 { return e.ID }

// GetMessageID returns the requested value for *ChatMessageEditHistoryEntry in the common_service_models backend flow.
func (e *ChatMessageEditHistoryEntry) GetMessageID() ChatMessageIdType { return e.MessageID }

// GetVersionNumber returns the requested value for *ChatMessageEditHistoryEntry in the common_service_models backend flow.
func (e *ChatMessageEditHistoryEntry) GetVersionNumber() uint32 { return e.VersionNumber }

// GetEditedByUserID returns the requested value for *ChatMessageEditHistoryEntry in the common_service_models backend flow.
func (e *ChatMessageEditHistoryEntry) GetEditedByUserID() UserIdType { return e.EditedByUserID }

// GetVersionStartedAt returns the requested value for *ChatMessageEditHistoryEntry in the common_service_models backend flow.
func (e *ChatMessageEditHistoryEntry) GetVersionStartedAt() time.Time { return e.VersionStartedAt }

// GetVersionEndedAt returns the requested value for *ChatMessageEditHistoryEntry in the common_service_models backend flow.
func (e *ChatMessageEditHistoryEntry) GetVersionEndedAt() time.Time { return e.VersionEndedAt }

// GetContent returns the requested value for *ChatMessageEditHistoryEntry in the common_service_models backend flow.
func (e *ChatMessageEditHistoryEntry) GetContent() string { return e.Content }

// GetContentType returns the requested value for *ChatMessageEditHistoryEntry in the common_service_models backend flow.
func (e *ChatMessageEditHistoryEntry) GetContentType() proto_models.MessageContentType {
	return e.ContentType
}

// GetEmbeds returns the requested value for *ChatMessageEditHistoryEntry in the common_service_models backend flow.
func (e *ChatMessageEditHistoryEntry) GetEmbeds() []*ChatMessageEmbed { return e.Embeds }

// GetAttachmentIDs returns the requested value for *ChatMessageEditHistoryEntry in the common_service_models backend flow.
func (e *ChatMessageEditHistoryEntry) GetAttachmentIDs() []uint64 { return e.AttachmentIDs }

// GetMentionUserIDs returns the requested value for *ChatMessageEditHistoryEntry in the common_service_models backend flow.
func (e *ChatMessageEditHistoryEntry) GetMentionUserIDs() []uint64 { return e.MentionUserIDs }

// GetCreatedAt returns the requested value for *ChatMessageEditHistoryEntry in the common_service_models backend flow.
func (e *ChatMessageEditHistoryEntry) GetCreatedAt() time.Time { return e.CreatedAt }

// GetEditedByUser returns the requested value for *ChatMessageEditHistoryEntry in the common_service_models backend flow.
func (e *ChatMessageEditHistoryEntry) GetEditedByUser() UserLike {
	if e.EditedByUser == nil {
		return nil
	}
	return e.EditedByUser
}

// ChatMessageEmbed defines a data model used by the common_service_models backend flow.
// It carries state exchanged between handlers, services, and persistence boundaries.
type ChatMessageEmbed struct {
	ID           uint64            `json:"id"`
	MessageID    ChatMessageIdType `json:"message_id"`
	EmbedType    uint32            `json:"embed_type"`
	EmbedData    []byte            `json:"embed_data,omitempty"`
	FallbackText string            `json:"fallback_text"`
	Position     uint32            `json:"position"`
	CreatedAt    time.Time         `json:"created_at"`
}

type ChatMessageEmbedLike interface {
	GetID() uint64
	GetMessageID() ChatMessageIdType
	GetEmbedType() uint32
	GetEmbedData() []byte
	GetFallbackText() string
	GetPosition() uint32
	GetCreatedAt() time.Time
}

var _ ChatMessageEmbedLike = (*ChatMessageEmbed)(nil)

// GetID returns the requested value for *ChatMessageEmbed in the common_service_models backend flow.
func (e *ChatMessageEmbed) GetID() uint64 { return e.ID }

// GetMessageID returns the requested value for *ChatMessageEmbed in the common_service_models backend flow.
func (e *ChatMessageEmbed) GetMessageID() ChatMessageIdType { return e.MessageID }

// GetEmbedType returns the requested value for *ChatMessageEmbed in the common_service_models backend flow.
func (e *ChatMessageEmbed) GetEmbedType() uint32 { return e.EmbedType }

// GetEmbedData returns the requested value for *ChatMessageEmbed in the common_service_models backend flow.
func (e *ChatMessageEmbed) GetEmbedData() []byte { return e.EmbedData }

// GetFallbackText returns the requested value for *ChatMessageEmbed in the common_service_models backend flow.
func (e *ChatMessageEmbed) GetFallbackText() string { return e.FallbackText }

// GetPosition returns the requested value for *ChatMessageEmbed in the common_service_models backend flow.
func (e *ChatMessageEmbed) GetPosition() uint32 { return e.Position }

// GetCreatedAt returns the requested value for *ChatMessageEmbed in the common_service_models backend flow.
func (e *ChatMessageEmbed) GetCreatedAt() time.Time { return e.CreatedAt }

// ChatMessageReaction defines a data model used by the common_service_models backend flow.
// It carries state exchanged between handlers, services, and persistence boundaries.
type ChatMessageReaction struct {
	MessageID ChatMessageIdType `json:"message_id"`
	UserID    UserIdType        `json:"user_id"`
	Emoji     string            `json:"emoji"`
	CreatedAt time.Time         `json:"created_at"`
	User      *User             `json:"user,omitempty"`
}

type ChatMessageReactionLike interface {
	GetMessageID() ChatMessageIdType
	GetUserID() UserIdType
	GetEmoji() string
	GetCreatedAt() time.Time
	GetUser() UserLike
}

var _ ChatMessageReactionLike = (*ChatMessageReaction)(nil)

// GetMessageID returns the requested value for *ChatMessageReaction in the common_service_models backend flow.
func (r *ChatMessageReaction) GetMessageID() ChatMessageIdType { return r.MessageID }

// GetUserID returns the requested value for *ChatMessageReaction in the common_service_models backend flow.
func (r *ChatMessageReaction) GetUserID() UserIdType { return r.UserID }

// GetEmoji returns the requested value for *ChatMessageReaction in the common_service_models backend flow.
func (r *ChatMessageReaction) GetEmoji() string { return r.Emoji }

// GetCreatedAt returns the requested value for *ChatMessageReaction in the common_service_models backend flow.
func (r *ChatMessageReaction) GetCreatedAt() time.Time { return r.CreatedAt }

// GetUser returns the requested value for *ChatMessageReaction in the common_service_models backend flow.
func (r *ChatMessageReaction) GetUser() UserLike {
	if r.User == nil {
		return nil
	}
	return r.User
}

// ChatMessageMention defines a data model used by the common_service_models backend flow.
// It carries state exchanged between handlers, services, and persistence boundaries.
type ChatMessageMention struct {
	MessageID ChatMessageIdType `json:"message_id"`
	UserID    UserIdType        `json:"user_id"`
	CreatedAt time.Time         `json:"created_at"`
	User      *User             `json:"user,omitempty"`
}

type ChatMessageMentionLike interface {
	GetMessageID() ChatMessageIdType
	GetUserID() UserIdType
	GetCreatedAt() time.Time
	GetUser() UserLike
}

var _ ChatMessageMentionLike = (*ChatMessageMention)(nil)

// GetMessageID returns the requested value for *ChatMessageMention in the common_service_models backend flow.
func (m *ChatMessageMention) GetMessageID() ChatMessageIdType { return m.MessageID }

// GetUserID returns the requested value for *ChatMessageMention in the common_service_models backend flow.
func (m *ChatMessageMention) GetUserID() UserIdType { return m.UserID }

// GetCreatedAt returns the requested value for *ChatMessageMention in the common_service_models backend flow.
func (m *ChatMessageMention) GetCreatedAt() time.Time { return m.CreatedAt }

// GetUser returns the requested value for *ChatMessageMention in the common_service_models backend flow.
func (m *ChatMessageMention) GetUser() UserLike {
	if m.User == nil {
		return nil
	}
	return m.User
}
