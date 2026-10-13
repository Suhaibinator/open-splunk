package ports

import (
	"context"

	"github.com/Suhaibinator/go-common/pkg/common_persistence/common_persistence_models"
	"github.com/Suhaibinator/go-common/pkg/common_service/common_service_models"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
)

// ChatPermissionOperation identifies a chat permission check performed by the service layer.
type ChatPermissionOperation int

const (
	ChatPermissionCreateChannel ChatPermissionOperation = iota + 1
	ChatPermissionGetOrCreateChannelByMembers
	ChatPermissionGetChannel
	ChatPermissionUpdateChannel
	ChatPermissionListChannels
	ChatPermissionLeaveChannel
	ChatPermissionAddChannelParticipants
	ChatPermissionRemoveChannelParticipant
	ChatPermissionUpdateChannelParticipantRole
	ChatPermissionUpdateChannelParticipantPrefs
	ChatPermissionListChannelParticipants
	ChatPermissionSendMessage
	ChatPermissionEditMessage
	ChatPermissionDeleteMessage
	ChatPermissionGetMessage
	ChatPermissionBatchGetMessages
	ChatPermissionListMessages
	ChatPermissionAddReaction
	ChatPermissionRemoveReaction
	ChatPermissionListReactions
	ChatPermissionListMessageEditHistory
	// ChatPermissionReadHiddenMessages gates an explicit include_hidden=true read (admin/debug/bot
	// tooling). Without it, hidden_from_users rows are filtered server-side and an explicit request
	// is rejected rather than silently downgraded.
	ChatPermissionReadHiddenMessages
	// ChatPermissionCreateAgentSession authorizes installing an agent at an existing location.
	ChatPermissionCreateAgentSession
	// ChatPermissionSpawnDelegatedAgent authorizes a delegating agent spawning a child agent in a
	// new thread. Actor is the parent session's bot, not a human; the default is allow.
	ChatPermissionSpawnDelegatedAgent
	// ChatPermissionStopAgentRun authorizes a person stopping an agent run (AgentRunID) and
	// suspending its session (AgentSessionID). go-common admits only people who could resolve the
	// run's tool calls or whose input the run admitted; the callback may deny. It is also asked,
	// with no Stop following, to compute a viewer's viewer_can_stop on every interaction read, so
	// it must have no side effects for this operation.
	ChatPermissionStopAgentRun
	// ChatPermissionResumeAgentSession authorizes a person resuming a suspended agent session
	// (AgentSessionID). go-common admits only the person who stopped it, site tool-approval admins
	// and channel admins; the callback may deny. It is also asked, with no Resume following, to
	// compute a viewer's viewer_can_resume on every interaction read, so it must have no side
	// effects for this operation.
	ChatPermissionResumeAgentSession
	// ChatPermissionEditAgentQueuedInput authorizes a person rewording input they queued for an
	// agent session (AgentQueuedInputID) before the agent receives it, with the proposed content and
	// mentions. go-common admits only the input's sender; the callback may deny. It is also asked,
	// with no edit following and no proposed content, to compute a viewer's viewer_can_edit
	// whenever queued input is read, so it must have no side effects for this operation.
	ChatPermissionEditAgentQueuedInput
	// ChatPermissionRemoveAgentQueuedInput authorizes a person withdrawing queued input
	// (AgentQueuedInputID; TargetUserID is its sender) before the agent receives it. go-common
	// admits only its sender and the channel's admins; the callback may deny. It is also asked, with
	// no removal following, to compute a viewer's viewer_can_remove whenever queued input is read,
	// so it must have no side effects for this operation.
	ChatPermissionRemoveAgentQueuedInput
	// ChatPermissionForkChannel authorizes forking Channel at Message into a new channel named
	// ProposedChannelName with ParticipantUserIDs besides the actor (bot users of cloned agent
	// sessions are not listed). ProposedOrganizationID is the organization whose read access the
	// fork keeps (zero when dropped); ProposedContext* and ProposedUnreadNotificationsEnabled are
	// the values it carries over. go-common first applies the source's fork rules; the callback may
	// deny.
	ChatPermissionForkChannel
)

var chatPermissionOperationNames = [...]string{
	ChatPermissionCreateChannel:                 "chat.create_channel",
	ChatPermissionGetOrCreateChannelByMembers:   "chat.get_or_create_channel_by_members",
	ChatPermissionGetChannel:                    "chat.get_channel",
	ChatPermissionUpdateChannel:                 "chat.update_channel",
	ChatPermissionListChannels:                  "chat.list_channels",
	ChatPermissionLeaveChannel:                  "chat.leave_channel",
	ChatPermissionAddChannelParticipants:        "chat.add_channel_participants",
	ChatPermissionRemoveChannelParticipant:      "chat.remove_channel_participant",
	ChatPermissionUpdateChannelParticipantRole:  "chat.update_channel_participant_role",
	ChatPermissionUpdateChannelParticipantPrefs: "chat.update_channel_participant_prefs",
	ChatPermissionListChannelParticipants:       "chat.list_channel_participants",
	ChatPermissionSendMessage:                   "chat.send_message",
	ChatPermissionEditMessage:                   "chat.edit_message",
	ChatPermissionDeleteMessage:                 "chat.delete_message",
	ChatPermissionGetMessage:                    "chat.get_message",
	ChatPermissionBatchGetMessages:              "chat.batch_get_messages",
	ChatPermissionListMessages:                  "chat.list_messages",
	ChatPermissionAddReaction:                   "chat.add_reaction",
	ChatPermissionRemoveReaction:                "chat.remove_reaction",
	ChatPermissionListReactions:                 "chat.list_reactions",
	ChatPermissionListMessageEditHistory:        "chat.list_message_edit_history",
	ChatPermissionReadHiddenMessages:            "chat.read_hidden_messages",
	ChatPermissionCreateAgentSession:            "chat.create_agent_session",
	ChatPermissionSpawnDelegatedAgent:           "chat.spawn_delegated_agent",
	ChatPermissionStopAgentRun:                  "chat.stop_agent_run",
	ChatPermissionResumeAgentSession:            "chat.resume_agent_session",
	ChatPermissionEditAgentQueuedInput:          "chat.edit_agent_queued_input",
	ChatPermissionRemoveAgentQueuedInput:        "chat.remove_agent_queued_input",
	ChatPermissionForkChannel:                   "chat.fork_channel",
}

func (op ChatPermissionOperation) String() string {
	if op > 0 && int(op) < len(chatPermissionOperationNames) {
		return chatPermissionOperationNames[op]
	}
	return "chat.unknown"
}

// ChatPermissionRequest defines a data model used by the ports backend flow.
// It carries state exchanged between handlers, services, and persistence boundaries.
type ChatPermissionRequest[U common_service_models.UserLike] struct {
	Operation ChatPermissionOperation
	Actor     U

	Channel *common_service_models.ChatChannel
	Message *common_service_models.ChatMessage

	ChannelType        proto_models.ChannelType
	ParticipantUserIDs []uint64
	TargetUserID       *common_service_models.UserIdType
	NewRole            *proto_models.ChannelRole

	ParentMessageID  uint64
	ReplyToMessageID uint64
	MessageIDs       []uint64

	// BroadcastToChannel is set on ChatPermissionSendMessage when the thread reply asks to
	// also appear on the main timeline, so apps can deny channel-wide visibility separately
	// from thread writes.
	BroadcastToChannel bool

	// Proposed agent identity for ChatPermissionCreateAgentSession. Channel and
	// ParentMessageID identify the destination; no organization access is granted.
	// ProposedOrganizationID is also the organization whose read access a
	// ChatPermissionForkChannel fork keeps, and the one a CreateWorkflowChannel
	// ChatPermissionCreateChannel grants (zero without organization access).
	ProposedAgentType      string
	ProposedBotUserID      uint64
	ProposedAgentUserID    uint64
	ProposedOrganizationID common_service_models.OrganizationIdType
	// ParentAgentSessionID is the delegating session for ChatPermissionSpawnDelegatedAgent (with one
	// level of nesting, its graph's root), and AgentRunID the spawning run. The Proposed* agent
	// fields describe the child.
	ParentAgentSessionID uint64
	// AgentSessionID and AgentRunID name the session and run of ChatPermissionStopAgentRun, and
	// the session of ChatPermissionResumeAgentSession (with the run its Stop ended). For
	// ChatPermissionListMessages, AgentSessionID is the session whose queued inputs are listed,
	// and zero for transcript reads.
	AgentSessionID uint64
	AgentRunID     uint64
	// AgentQueuedInputID names the queued input of ChatPermissionEditAgentQueuedInput and
	// ChatPermissionRemoveAgentQueuedInput; AgentSessionID and ParentMessageID are its session and
	// location.
	AgentQueuedInputID uint64

	Emoji string

	ProposedChannelName                *string
	ProposedChannelDescription         *string
	ProposedAvatarAttachmentID         *uint64
	ProposedUnreadNotificationsEnabled *bool
	ProposedIsArchived                 *bool
	ProposedContextID                  *uint64
	ProposedContextDisplayName         *string
	ProposedContextEntityID            *uint64
	// ProposedPublicRead is the requested level of a SetChannelPublicRead UpdateChannel check.
	ProposedPublicRead *proto_models.ChatChannelPublicRead

	ProposedNotificationMode *proto_models.ChannelNotificationMode
	ProposedIsPinned         *bool

	ProposedMessageContent string
	ProposedEmbedCount     int
	ProposedMentionUserIDs []uint64
	ProposedMentionCount   int

	IncludeDeleted bool
}

// ChatAnonymousReadRequest describes a logged-out read of a CHAT_CHANNEL_PUBLIC_READ_ANYONE
// channel. There is no actor: the reader is anonymous. Operation is ChatPermissionGetChannel or
// ChatPermissionListMessages; for ChatPermissionListMessages, ParentMessageID is the thread root
// being listed and zero for the main timeline.
type ChatAnonymousReadRequest struct {
	Operation ChatPermissionOperation
	Channel   *common_service_models.ChatChannel

	ParentMessageID uint64
}

// ChatChannelCreation describes a channel go-common is about to create, for the ChannelFeatures
// hook. It is built only after Authorize allowed the create.
type ChatChannelCreation[U common_service_models.UserLike] struct {
	// Operation is ChatPermissionCreateChannel for CreateChannel and for the channel of a new agent
	// session, and ChatPermissionGetOrCreateChannelByMembers when no channel with the exact
	// members existed.
	Operation ChatPermissionOperation
	Actor     U

	ChannelType proto_models.ChannelType
	// ParticipantUserIDs are the members besides the actor, who is always the owner.
	ParticipantUserIDs []uint64

	ContextID          *uint64
	ContextDisplayName *string
	ContextEntityID    *uint64

	// AgentType is the agent the channel is created for, and empty for a channel of people.
	AgentType string

	// Configured is the set of chat_support_* and chat_allow_* flags that are on in configuration
	// now. It is the channel's features when the hook is unset, and the most the hook may enable.
	Configured common_persistence_models.ChatChannelFeatures
}

// ChatPermissionCallbacks supplies optional application authorization hooks.
type ChatPermissionCallbacks[U common_service_models.UserLike] struct {
	Authorize func(ctx context.Context, req ChatPermissionRequest[U]) error

	// ChannelFeatures chooses the feature flags of a new channel. go-common stores the returned
	// flags intersected with req.Configured, so configuration stays a ceiling the hook can narrow
	// but never widen, and an error fails the create with that error. Return req.Configured to
	// keep the configured flags. Flags are fixed at creation: a fork copies its source's, and
	// only the admin channel update changes them later. A channel without
	// ChatChannelSupportThreads cannot host threaded agent sessions or delegated agents.
	ChannelFeatures func(ctx context.Context, req ChatChannelCreation[U]) (common_persistence_models.ChatChannelFeatures, error)

	// AuthorizeAnonymous may deny a logged-out read of a public channel; Authorize never sees
	// those reads because they have no actor. It fails closed: when Authorize is set and
	// AuthorizeAnonymous is nil, every anonymous read is denied, so an application's read policy
	// cannot be bypassed by signing out. With neither set, anonymous reads are allowed.
	AuthorizeAnonymous func(ctx context.Context, req ChatAnonymousReadRequest) error
}

// ChatPermissionCallbackConfig defines a data model used by the ports backend flow.
// It carries state exchanged between handlers, services, and persistence boundaries.
type ChatPermissionCallbackConfig[U common_service_models.UserLike] struct {
	Callbacks *ChatPermissionCallbacks[U]
}

// NewChatPermissionCallbackConfig constructs and initializes a new instance in the ports backend flow.
func NewChatPermissionCallbackConfig[U common_service_models.UserLike]() *ChatPermissionCallbackConfig[U] {
	return &ChatPermissionCallbackConfig[U]{}
}

// WithCallbacks executes its part of the service logic for *ChatPermissionCallbackConfig[U] in the ports backend flow.
func (c *ChatPermissionCallbackConfig[U]) WithCallbacks(cb *ChatPermissionCallbacks[U]) *ChatPermissionCallbackConfig[U] {
	c.Callbacks = cb
	return c
}
