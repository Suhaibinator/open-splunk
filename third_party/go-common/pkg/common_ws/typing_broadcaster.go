package common_ws

import (
	"context"
	"time"

	"github.com/Suhaibinator/SRouter/pkg/logkeys"
	"github.com/Suhaibinator/go-common/pkg/common_service/common_service_models"
	"github.com/Suhaibinator/go-common/pkg/common_service/ports"
	"github.com/Suhaibinator/go-common/pkg/config"
	appLogger "github.com/Suhaibinator/go-common/pkg/logger"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
	"github.com/jellydator/ttlcache/v3"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// TypingBroadcaster defines the data structure used in the realtime WebSocket flow for connection
// lifecycle, command routing, and event fan-out.
// It carries state across layer boundaries handled by this package.
//
// Typing shares the participant cache with chat events and excludes the typing actor.
type TypingBroadcaster struct {
	publisher   ports.RealtimePublisher
	persistence PersistenceForRecipientResolver
	logger      *zap.Logger
	// refreshWindow samples chat_typing_refresh_window; started holds each participant's
	// (actor, channel) start delivered within that window.
	// TODO(multi-instance-v2): coalescing state is process-local; a stop on another instance does not clear it.
	refreshWindow func() time.Duration
	started       *ttlcache.Cache[chatGrantKey, struct{}]
}

// NewTypingBroadcaster executes this function as part of the realtime WebSocket flow for connection
// lifecycle, command routing, and event fan-out.
//
// Parameters:
//   - publisher: Realtime publisher the typing events are handed to.
//   - persistence: Source of the channel's active participant list.
//   - logger: Logger for typing diagnostics; a no-op logger is used when nil.
//   - refreshWindow: Samples the hot typing refresh window once per command. A nil sampler
//     reads config.DefaultChatTypingRefreshWindow.
func NewTypingBroadcaster(publisher ports.RealtimePublisher, persistence PersistenceForRecipientResolver, logger *zap.Logger, refreshWindow func() time.Duration) *TypingBroadcaster {
	if logger == nil {
		logger = zap.NewNop()
	}
	if refreshWindow == nil {
		refreshWindow = func() time.Duration { return config.DefaultChatTypingRefreshWindow }
	}
	return &TypingBroadcaster{
		publisher:     publisher,
		persistence:   persistence,
		logger:        logger,
		refreshWindow: refreshWindow,
		started:       ttlcache.New[chatGrantKey, struct{}](),
	}
}

// BroadcastTyping executes this method as part of the realtime WebSocket flow for connection lifecycle, command routing, and event fan-out.
//
// Parameters:
//   - ctx: Request context used for cancellation, deadlines, and tracing.
//   - actorUserID: Identifier of the user (or bot) whose typing state changed.
//   - channelID: Channel the typing state applies to.
//   - isTyping: Boolean flag that controls operation behavior.
func (b *TypingBroadcaster) BroadcastTyping(
	ctx context.Context,
	actorUserID common_service_models.UserIdType,
	channelID uint64,
	isTyping bool,
) error {
	if b == nil || b.publisher == nil || b.persistence == nil {
		return nil
	}
	if actorUserID == 0 || channelID == 0 {
		return nil
	}
	// A repeated start within the refresh window is dropped before recipient lookup. A stop always
	// passes and clears the entry, so the next start is delivered.
	key := chatGrantKey{actorUserID, channelID}
	window := b.refreshWindow()
	if !isTyping {
		b.started.Delete(key)
	} else if b.started.Has(key) {
		return nil
	}

	audience, hasAudience := b.publisher.(ChatAudiencePublisher)
	var generation uint64
	if hasAudience {
		generation = audience.ChatAudienceGeneration()
	}
	userIDs, err := b.persistence.GetActiveChannelParticipantUserIDs(ctx, channelID)
	if err != nil || len(userIDs) == 0 {
		if err != nil {
			// Typing delivery is optional and the business request can continue, but a
			// persistence failure drops a realtime signal and is operational degradation.
			appLogger.For(ctx, b.logger, realtimeTypingBroadcasterLoggerName).Warn("failed to resolve typing recipients",
				zap.String(logkeys.Operation, "broadcast_typing"),
				zap.String(logkeys.Stage, "recipient_lookup"),
				zap.Uint64(appLogger.KeyUserID, uint64(actorUserID)),
				zap.Uint64(appLogger.KeyChannelID, channelID),
				zap.String(logkeys.Fallback, "drop_typing_event"),
				zap.Error(err),
			)
		}
		return nil
	}

	// Actor exclusion: the typist is required to be a participant, but never receives their own
	// typing event back.
	recipients := make([]common_service_models.UserIdType, 0, len(userIDs))
	isParticipant := false
	for _, id := range userIDs {
		userID := common_service_models.UserIdType(id)
		if userID == actorUserID {
			isParticipant = true
			continue
		}
		if userID == 0 {
			continue
		}
		recipients = append(recipients, userID)
	}
	if !isParticipant || (len(recipients) == 0 && !hasAudience) {
		return nil
	}
	if isTyping {
		// Only participants leave state, and it expires with the window that admitted it.
		b.started.DeleteExpired()
		b.started.Set(key, struct{}{}, window)
	}

	occurredAt := time.Now()
	event := &proto_models.ChatWebSocketEvent{
		Type:        proto_models.ChatEventType_CHAT_EVENT_TYPE_TYPING,
		OccurredAt:  timestamppb.New(occurredAt),
		ActorUserId: uint64(actorUserID),
		ChannelId:   channelID,
		Payload: &proto_models.ChatWebSocketEvent_Typing{
			Typing: &proto_models.TypingEvent{
				ChannelId: channelID,
				IsTyping:  isTyping,
			},
		},
	}

	realtimeEvent := &ports.RealtimeEvent{
		EventType:  ChatEventTypeName(proto_models.ChatEventType_CHAT_EVENT_TYPE_TYPING),
		OccurredAt: occurredAt,
		Payload:    event,
	}
	if hasAudience {
		return audience.PublishChatAudience(ctx, realtimeEvent, channelID, ports.ChatRealtimeTyping, recipients, generation)
	}
	realtimeEvent.Topics = make([]string, 0, len(recipients))
	for _, recipient := range recipients {
		realtimeEvent.Topics = append(realtimeEvent.Topics, UserTopic(recipient))
	}
	return b.publisher.Publish(ctx, realtimeEvent)
}

// ChatTypingEmitter defines the data structure used in the realtime WebSocket flow for connection
// lifecycle, command routing, and event fan-out.
// It carries state across layer boundaries handled by this package.
type ChatTypingEmitter struct {
	broadcaster *TypingBroadcaster
}

// NewChatTypingEmitter executes this function as part of the realtime WebSocket flow for connection
// lifecycle, command routing, and event fan-out.
//
// Parameters:
//   - broadcaster: Broadcaster the emitted typing events are routed through.
func NewChatTypingEmitter(broadcaster *TypingBroadcaster) *ChatTypingEmitter {
	return &ChatTypingEmitter{broadcaster: broadcaster}
}

// StartTyping executes this method as part of the realtime WebSocket flow for connection lifecycle, command routing, and event fan-out.
//
// Parameters:
//   - ctx: Request context used for cancellation, deadlines, and tracing.
//   - botUserID: Identifier of the bot that started typing.
//   - channelID: Channel the bot is typing in.
func (e *ChatTypingEmitter) StartTyping(
	ctx context.Context,
	botUserID common_service_models.UserIdType,
	channelID common_service_models.ChatChannelIdType,
) error {
	if e == nil || e.broadcaster == nil {
		return nil
	}
	return e.broadcaster.BroadcastTyping(ctx, botUserID, uint64(channelID), true)
}

// StopTyping executes this method as part of the realtime WebSocket flow for connection lifecycle, command routing, and event fan-out.
//
// Parameters:
//   - ctx: Request context used for cancellation, deadlines, and tracing.
//   - botUserID: Identifier of the bot that stopped typing.
//   - channelID: Channel the bot was typing in.
func (e *ChatTypingEmitter) StopTyping(
	ctx context.Context,
	botUserID common_service_models.UserIdType,
	channelID common_service_models.ChatChannelIdType,
) error {
	if e == nil || e.broadcaster == nil {
		return nil
	}
	return e.broadcaster.BroadcastTyping(ctx, botUserID, uint64(channelID), false)
}

var _ ports.AgentTypingEmitter = (*ChatTypingEmitter)(nil)

// ChatTypingCommandType is the RealtimeCommand type carrying a chat TypingCommand payload.
const ChatTypingCommandType = "chat.typing"

// TypingCommandHandler defines the data structure used in the realtime WebSocket flow for
// connection lifecycle, command routing, and event fan-out.
// It carries state across layer boundaries handled by this package.
type TypingCommandHandler struct {
	broadcaster *TypingBroadcaster
}

// NewTypingCommandHandler executes this function as part of the realtime WebSocket flow for
// connection lifecycle, command routing, and event fan-out.
//
// Parameters:
//   - broadcaster: Broadcaster the received typing commands are routed through.
func NewTypingCommandHandler(broadcaster *TypingBroadcaster) *TypingCommandHandler {
	return &TypingCommandHandler{broadcaster: broadcaster}
}

// HandleRealtimeCommand decodes the chat TypingCommand payload of a "chat.typing" RealtimeCommand
// and broadcasts it.
//
// Parameters:
//   - ctx: Request context used for cancellation, deadlines, and tracing.
//   - userID: Identifier of the user that sent the command.
//   - requestID: Client-generated correlation ID; typing commands are not acknowledged.
//   - payload: Serialized proto_models.TypingCommand.
func (h *TypingCommandHandler) HandleRealtimeCommand(
	ctx context.Context,
	userID common_service_models.UserIdType,
	_ string,
	payload []byte,
) error {
	if h == nil || h.broadcaster == nil || len(payload) == 0 {
		return nil
	}
	var typing proto_models.TypingCommand
	if err := proto.Unmarshal(payload, &typing); err != nil {
		return err
	}
	channelID := typing.GetChannelId()
	if channelID == 0 {
		return nil
	}
	return h.broadcaster.BroadcastTyping(ctx, userID, channelID, typing.GetIsTyping())
}

var _ CommandHandler = (*TypingCommandHandler)(nil)
