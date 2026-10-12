package common_ws

import (
	"context"
	"strings"

	"github.com/Suhaibinator/go-common/pkg/common_converter"
	"github.com/Suhaibinator/go-common/pkg/common_service/common_service_models"
	"github.com/Suhaibinator/go-common/pkg/common_service/ports"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

// ChatEventTypePrefix namespaces every chat event type on the realtime envelope.
const ChatEventTypePrefix = "chat."

// RecipientResolver defines recipient lookup behavior used in the realtime WebSocket flow for
// connection lifecycle, command routing, and event fan-out.
// Implementations are called before publishing chat events so the hub can target the correct
// connected users.
type RecipientResolver interface {
	ResolveRecipients(ctx context.Context, event *ports.ChatEvent) ([]common_service_models.UserIdType, error)
}

// RecipientResolverFunc adapts a function into a RecipientResolver for the realtime WebSocket flow
// for connection lifecycle, command routing, and event fan-out.
// It is used by lightweight resolver implementations that do not require a dedicated struct type.
type RecipientResolverFunc func(ctx context.Context, event *ports.ChatEvent) ([]common_service_models.UserIdType, error)

// ResolveRecipients executes this method as part of the realtime WebSocket flow for connection lifecycle, command routing, and event fan-out.
func (f RecipientResolverFunc) ResolveRecipients(ctx context.Context, event *ports.ChatEvent) ([]common_service_models.UserIdType, error) {
	return f(ctx, event)
}

// MemberUserIDsResolver supports explicitly targeted lifecycle notifications when no
// persistence is configured. Ordinary and hidden events require a real recipient resolver.
func MemberUserIDsResolver() RecipientResolver {
	return RecipientResolverFunc(func(_ context.Context, event *ports.ChatEvent) ([]common_service_models.UserIdType, error) {
		if event == nil || !event.TargetedRecipients || eventIsHidden(event) || len(event.MemberUserIDs) == 0 {
			return nil, nil
		}
		return event.MemberUserIDs, nil
	})
}

// chatEventTypeNames is the precomputed enum -> wire name table ChatEventTypeName serves from. The
// derivation is a String() lookup plus two allocating string operations, and it sits on the publish
// path of every chat event including each token-stream delta, so it is paid once at init over the
// closed enum rather than per publish.
var chatEventTypeNames = func() map[proto_models.ChatEventType]string {
	names := make(map[proto_models.ChatEventType]string, len(proto_models.ChatEventType_name))
	for value, name := range proto_models.ChatEventType_name {
		names[proto_models.ChatEventType(value)] = deriveChatEventTypeName(name)
	}
	return names
}()

// deriveChatEventTypeName is the single definition of the wire name's shape, used to build the
// lookup table and to name a value that is not in it.
func deriveChatEventTypeName(enumName string) string {
	return ChatEventTypePrefix + strings.ToLower(strings.TrimPrefix(enumName, "CHAT_EVENT_TYPE_"))
}

// ChatEventTypeName maps a chat event enum onto the namespaced event type carried by the realtime
// envelope, e.g. CHAT_EVENT_TYPE_MESSAGE_SENT becomes "chat.message_sent". The enum still travels
// inside the ChatWebSocketEvent payload; this string exists so the generic transport — which never
// enumerates application event types — can name what it is carrying.
//
// A value outside the generated enum (an unknown number from a newer producer) falls back to the
// same derivation over its numeric String(), so it is named rather than dropped.
//
// Parameters:
//   - eventType: The chat event enum being published.
//
// Returns:
//   - result: The namespaced realtime event type.
func ChatEventTypeName(eventType proto_models.ChatEventType) string {
	if name, known := chatEventTypeNames[eventType]; known {
		return name
	}
	return deriveChatEventTypeName(eventType.String())
}

// ChatAudiencePublisher merges participant user topics with authorized open-chat viewers.
// The generation captured before recipient lookup fences revocation at final enqueue.
// PublishChatAccessChange takes the same arguments but publishes only to refreshed participants,
// after the access-change hook has suspended viewer grants (see ports.ChatEvent.AccessChange).
type ChatAudiencePublisher interface {
	ChatAudienceGeneration() uint64
	PublishChatAudience(context.Context, *ports.RealtimeEvent, uint64, ports.ChatRealtimeCategory, []common_service_models.UserIdType, uint64) error
	PublishChatAccessChange(context.Context, *ports.RealtimeEvent, uint64, ports.ChatRealtimeCategory, []common_service_models.UserIdType, uint64) error
}

// ChatRealtimeEventPublisher publishes participant and authorized viewer audiences.
type ChatRealtimeEventPublisher struct {
	publisher ports.RealtimePublisher
	converter common_converter.ConverterInterface
	resolver  RecipientResolver
}

var _ ports.ChatEventPublisher = (*ChatRealtimeEventPublisher)(nil)

// ChatRealtimeEventPublisherOptions selects how a chat publisher decides who receives an event —
// the participant audience authority. The transport independently authorizes open-chat viewers.
//
// The three fields are a precedence chain, not independent settings: an explicit Resolver wins
// outright, Persistence otherwise installs the database-backed resolver (the only one that
// implements the hidden/privileged split, and therefore the only correct choice for an application
// with bots or hidden transcript rows), and supplying neither falls back to MemberUserIDsResolver,
// which delivers only events that already carry a stamped member list.
type ChatRealtimeEventPublisherOptions struct {
	// Resolver overrides the default recipient resolution behavior.
	// If set, Persistence is ignored.
	Resolver RecipientResolver

	// Persistence enables DB-backed recipient resolution when Resolver is nil.
	// This is required for message/reaction fanout (active channel participants).
	Persistence PersistenceForRecipientResolver

	// Logger is used by the DB-backed resolver when enabled. If nil, a no-op logger is used.
	Logger *zap.Logger
}

// NewChatRealtimeEventPublisher creates a chat event publisher used in the realtime WebSocket flow
// for connection lifecycle, command routing, and event fan-out.
//
// Parameters:
//   - publisher: Realtime publisher the chat events are handed to.
//   - converter: Converter that turns a service chat event into its wire payload message.
//   - opts: Recipient resolution options.
func NewChatRealtimeEventPublisher(
	publisher ports.RealtimePublisher,
	converter common_converter.ConverterInterface,
	opts ChatRealtimeEventPublisherOptions,
) *ChatRealtimeEventPublisher {
	resolver := opts.Resolver
	if resolver == nil && opts.Persistence != nil {
		resolver = NewDBRecipientResolver(opts.Persistence, opts.Logger)
	}
	if resolver == nil {
		resolver = MemberUserIDsResolver()
	}
	return &ChatRealtimeEventPublisher{
		publisher: publisher,
		converter: converter,
		resolver:  resolver,
	}
}

// PublishChatEvent executes this method as part of the realtime WebSocket flow for connection lifecycle, command routing, and event fan-out.
//
// Parameters:
//   - ctx: Request context used for cancellation, deadlines, and tracing.
//   - event: The service-layer chat event to fan out.
func (p *ChatRealtimeEventPublisher) PublishChatEvent(ctx context.Context, event *ports.ChatEvent) error {
	if p == nil || p.publisher == nil || event == nil {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	// Provider-state rows are service protocol data, not privileged chat diagnostics. Defense in
	// depth here prevents an accidental future publish site from exposing their attachment/hash
	// envelope even to the hidden-row admin/bot audience.
	if event.ProviderState || (event.Message != nil && event.Message.Kind == proto_models.MessageKind_MESSAGE_KIND_PROVIDER_STATE) {
		return nil
	}

	audience, hasAudience := p.publisher.(ChatAudiencePublisher)
	var generation uint64
	if hasAudience {
		generation = audience.ChatAudienceGeneration()
	}
	recipients, err := p.resolver.ResolveRecipients(ctx, event)
	if err != nil {
		return err
	}

	wsEvent := p.converter.ServiceChatEventToWebSocketProto(ctx, event)
	if wsEvent == nil {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	// The audience transport consumes IDs directly. Only the generic fallback needs
	// per-user topic strings; avoid allocating them for every streamed delta.
	var topics []string
	if !hasAudience {
		topics = make([]string, 0, len(recipients))
		for _, recipient := range recipients {
			if recipient != 0 {
				topics = append(topics, UserTopic(recipient))
			}
		}
		if len(topics) == 0 {
			return nil
		}
	}

	// Ensure payload is stable even if converter returns shared pointers.
	cloned := proto.Clone(wsEvent).(*proto_models.ChatWebSocketEvent)
	if err := ctx.Err(); err != nil {
		return err
	}
	realtimeEvent := &ports.RealtimeEvent{
		Topics:     topics,
		EventType:  ChatEventTypeName(event.Type),
		OccurredAt: event.OccurredAt,
		Payload:    cloned,
	}
	if hasAudience {
		var channelID uint64
		category := chatEventCategory(event.Type)
		if !eventIsHidden(event) && !event.TargetedRecipients && category != 0 {
			channelID, _ = chatEventChannelID(event)
		}
		if event.AccessChange {
			return audience.PublishChatAccessChange(ctx, realtimeEvent, channelID, category, recipients, generation)
		}
		return audience.PublishChatAudience(ctx, realtimeEvent, channelID, category, recipients, generation)
	}
	return p.publisher.Publish(ctx, realtimeEvent)
}

// Unknown event types never acquire a viewer audience implicitly.
func chatEventCategory(eventType proto_models.ChatEventType) ports.ChatRealtimeCategory {
	switch eventType {
	case proto_models.ChatEventType_CHAT_EVENT_TYPE_MESSAGE_SENT,
		proto_models.ChatEventType_CHAT_EVENT_TYPE_MESSAGE_EDITED,
		proto_models.ChatEventType_CHAT_EVENT_TYPE_MESSAGE_DELETED,
		proto_models.ChatEventType_CHAT_EVENT_TYPE_MESSAGE_ATTACHMENTS_READY,
		proto_models.ChatEventType_CHAT_EVENT_TYPE_MESSAGE_STREAM_STARTED,
		proto_models.ChatEventType_CHAT_EVENT_TYPE_MESSAGE_STREAM_DELTA,
		proto_models.ChatEventType_CHAT_EVENT_TYPE_MESSAGE_STREAM_FINISHED,
		proto_models.ChatEventType_CHAT_EVENT_TYPE_MESSAGE_STREAM_FAILED,
		proto_models.ChatEventType_CHAT_EVENT_TYPE_TOOL_CALL_RESOLUTION,
		proto_models.ChatEventType_CHAT_EVENT_TYPE_CONTEXT_COMPACTION,
		proto_models.ChatEventType_CHAT_EVENT_TYPE_AGENT_RUN_STATE,
		proto_models.ChatEventType_CHAT_EVENT_TYPE_AGENT_SESSION_INTERACTION:
		return ports.ChatRealtimeMessages
	case proto_models.ChatEventType_CHAT_EVENT_TYPE_REACTION_ADDED, proto_models.ChatEventType_CHAT_EVENT_TYPE_REACTION_REMOVED:
		return ports.ChatRealtimeReactions
	case proto_models.ChatEventType_CHAT_EVENT_TYPE_INFERENCE_USAGE_UPDATED:
		return ports.ChatRealtimeInference
	case proto_models.ChatEventType_CHAT_EVENT_TYPE_CHANNEL_UPDATED, proto_models.ChatEventType_CHAT_EVENT_TYPE_CHANNEL_DELETED:
		return ports.ChatRealtimeChannel
	default:
		return 0
	}
}
