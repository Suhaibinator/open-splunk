package ports

import (
	"context"
	"errors"
	"time"

	"github.com/Suhaibinator/go-common/pkg/common_service/common_service_models"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
)

// ChatEvent defines a data model used by the ports backend flow.
// It carries state exchanged between handlers, services, and persistence boundaries.
type ChatEvent struct {
	// HiddenFromUsers and ProviderState carry server-owned visibility for sparse events.
	HiddenFromUsers bool
	ProviderState   bool
	// TargetedRecipients restricts personal events to their owner and permits
	// lifecycle notifications to reach a former member.
	TargetedRecipients bool
	// AccessChange marks an event announcing a committed access change. Grants are suspended
	// before delivery and this event reaches refreshed participants only. Viewers catch up on
	// successful renewal. Set by PublishChatAccessChangeAfterCommit.
	AccessChange bool

	Type        proto_models.ChatEventType
	OccurredAt  time.Time
	ActorUserID common_service_models.UserIdType

	Participant             *common_service_models.ChatChannelParticipant
	Channel                 *common_service_models.ChatChannel
	Message                 *common_service_models.ChatMessage
	Reaction                *common_service_models.ChatMessageReaction
	MessageStream           *ChatMessageStreamEvent
	ToolCallResolution      *ChatToolCallResolutionEvent
	ContextCompaction       *ChatContextCompactionEvent
	AgentRunState           *ChatAgentRunStateEvent
	AgentSessionInteraction *ChatAgentSessionInteractionEvent
	InferenceUsage          *proto_models.ChatInferenceUsageUpdate
	// InferenceUsageHidden limits fanout to the hidden-authorized audience; it is set only when
	// InferenceUsage carries the summary of a hidden message.
	InferenceUsageHidden bool

	// MemberUserIDs is honored only with TargetedRecipients. Ordinary events resolve current
	// membership through the shared realtime recipient cache.
	MemberUserIDs []common_service_models.UserIdType

	// Created indicates whether the underlying object was newly created as part of a get-or-create flow.
	Created bool
}

// ChatContextCompactionEvent is the ephemeral "summarizing context" indicator for one agent run
// (CHAT_EVENT_TYPE_CONTEXT_COMPACTION). It is published directly, never after commit: nothing
// durable backs it, and a client that misses Active=false clears the indicator on its own timeout.
type ChatContextCompactionEvent struct {
	ChannelID       uint64
	ParentMessageID uint64
	AgentRunID      uint64
	Active          bool
}

// ChatAgentRunStateEvent reports one agent run's lifecycle status and observable activity
// (CHAT_EVENT_TYPE_AGENT_RUN_STATE). A status transition is published after commit; an activity
// change is ephemeral and published directly, like ChatContextCompactionEvent. It is a hint: the
// agent-run status read stays authoritative, and clients expire live activity on their own timeout.
// ActivityMessageID must never name a row that is hidden from users.
type ChatAgentRunStateEvent struct {
	ChannelID         uint64
	ParentMessageID   uint64
	AgentRunID        uint64
	Status            proto_models.ChatAgentRunStatus
	StatusReason      proto_models.ChatAgentRunStatusReason
	StatusReasonCode  string
	Activity          proto_models.AgentRunActivity
	ActivityStartedAt time.Time
	ActivityMessageID uint64
}

// ChatAgentSessionInteractionEvent says an agent session's interaction state changed
// (CHAT_EVENT_TYPE_AGENT_SESSION_INTERACTION): it was stopped or resumed, or its waiting input
// changed. It is published after commit and carries only the session and its location; the
// interaction depends on the viewer, so clients re-read it.
type ChatAgentSessionInteractionEvent struct {
	ChannelID       uint64
	ParentMessageID uint64
	AgentSessionID  uint64
}

// ChatToolCallResolutionEvent carries the durable approval/execution-state change for a TOOL_CALL
// row so clients can patch the cached message in place without a refetch
// (CHAT_EVENT_TYPE_TOOL_CALL_RESOLUTION). The approval lane builds this via
// NewToolCallResolutionEvent and publishes it through the chat event publisher; the websocket
// converter maps it to proto ToolCallResolutionEvent, and the recipient resolver applies the same
// hidden/visibility authority split as the row itself.
type ChatToolCallResolutionEvent struct {
	MessageID        common_service_models.ChatMessageIdType
	ChannelID        common_service_models.ChatChannelIdType
	ApprovalStatus   proto_models.ToolCallApprovalStatus
	ExecutionState   proto_models.ToolCallExecutionState
	ResolvedByUserID *uint64
	ResolvedAt       *time.Time
	ResolutionReason *string
	// HiddenFromUsers mirrors the TOOL_CALL row's visibility so the recipient resolver can apply the
	// same privileged-only split to the resolution event (a hidden tool call's resolution must not
	// leak to regular users).
	HiddenFromUsers bool
}

// NewToolCallResolutionEvent builds the CHAT_EVENT_TYPE_TOOL_CALL_RESOLUTION ChatEvent for a resolved
// TOOL_CALL. The approval lane calls this rather than constructing the ChatEvent by hand, so the
// event type, payload shape, and visibility signal stay in one place.
//
// Parameters:
//   - resolution: the resolution payload (message/channel/approval/execution/resolver/visibility).
//   - actorUserID: the actor that performed the resolution (for audit/event ActorUserID).
//   - occurredAt: when the resolution occurred.
//
// Returns:
//   - result1: the ChatEvent ready to publish.
func NewToolCallResolutionEvent(
	resolution *ChatToolCallResolutionEvent,
	actorUserID common_service_models.UserIdType,
	occurredAt time.Time,
) *ChatEvent {
	return &ChatEvent{
		Type:               proto_models.ChatEventType_CHAT_EVENT_TYPE_TOOL_CALL_RESOLUTION,
		OccurredAt:         occurredAt,
		ActorUserID:        actorUserID,
		ToolCallResolution: resolution,
	}
}

// ChatMessageStreamEvent carries ephemeral stream delta metadata for websocket fanout.
type ChatMessageStreamEvent struct {
	// HiddenFromUsers mirrors the persisted assistant row; never accepted from clients.
	HiddenFromUsers bool
	MessageID       common_service_models.ChatMessageIdType
	ChannelID       common_service_models.ChatChannelIdType
	StreamID        string
	Sequence        uint64
	TextDelta       string
	AccumulatedText string
	FinalUsage      *proto_models.InferenceUsage
	// ParentMessageID is present for known placement: zero is the channel root; nil is unknown.
	ParentMessageID *uint64
}

// ChatEventPublisher is implemented by the application to fan out chat events to websocket clients.
// This is intended to be called post-commit (best-effort, async). Implementations must promptly
// honor ctx cancellation/deadlines: assistant stream deltas use a short row-locking handoff fence
// so a publisher that ignores ctx can otherwise delay lease heartbeats and cross-replica takeover.
type ChatEventPublisher interface {
	PublishChatEvent(ctx context.Context, event *ChatEvent) error
}

// PublishChatEventAfterCommit publishes a chat event once the transaction on ctx commits, so clients
// never observe events for uncommitted state. It is the chat-shaped counterpart of PublishAfterCommit
// and shares its post-commit discipline; every chat producer publishes through it.
//
// newEvent is invoked at publish time rather than at registration time, because producers routinely
// build the event from state that is only final after the commit: the message handlers re-read their
// row with the hook context so the event carries the committed content, and OccurredAt stamps the
// moment the publish actually happens. A nil event from newEvent skips the publish.
//
// Behavior change (round-2 decision D8): when ctx carries no transaction the event is published
// immediately instead of being silently dropped. Every chat publish site used to ignore the hook
// registration's false return, so a publish outside a transaction simply vanished.
//
// Mid-stream ephemera are the documented exception and must NOT use this helper: the bot transcript
// writer's stream publishes and the bot dispatcher's typing start/stop publish on a deliberately
// transaction-cleared context because deltas and typing indicators must not wait for a commit.
//
// Parameters:
//   - ctx: Request context used for cancellation, deadlines, tracing, and the ambient transaction.
//   - p: Publisher the event is handed to.
//   - newEvent: Builds the event at publish time; receives the publish context.
//
// Returns:
//   - result: Error encountered while executing this operation; nil on success or when deferred.
func PublishChatEventAfterCommit(
	ctx context.Context,
	p ChatEventPublisher,
	newEvent func(ctx context.Context) *ChatEvent,
) error {
	if newEvent == nil {
		return nil
	}
	return PublishChatEventsAfterCommit(ctx, p, func(publishCtx context.Context) []*ChatEvent {
		return []*ChatEvent{newEvent(publishCtx)}
	})
}

// PublishChatEventsAfterCommit is PublishChatEventAfterCommit for producers that build several
// events from one set of post-commit reads. Nil events are skipped; each publish is attempted.
func PublishChatEventsAfterCommit(
	ctx context.Context,
	p ChatEventPublisher,
	newEvents func(ctx context.Context) []*ChatEvent,
) error {
	if p == nil || newEvents == nil {
		return nil
	}
	return afterCommit(ctx, func(publishCtx context.Context) error {
		var errs []error
		for _, event := range newEvents(publishCtx) {
			if event != nil {
				errs = append(errs, p.PublishChatEvent(publishCtx, event))
			}
		}
		return errors.Join(errs...)
	})
}
