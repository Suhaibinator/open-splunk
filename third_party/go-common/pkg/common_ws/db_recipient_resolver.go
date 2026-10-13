package common_ws

import (
	"context"
	"errors"
	"fmt"

	"github.com/Suhaibinator/SRouter/pkg/logkeys"
	"github.com/Suhaibinator/go-common/pkg/common_service/common_service_models"
	"github.com/Suhaibinator/go-common/pkg/common_service/ports"
	appLogger "github.com/Suhaibinator/go-common/pkg/logger"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
	"go.uber.org/zap"
)

var errRealtimeEventPayloadMissing = errors.New("realtime event required payload is missing")

// PersistenceForRecipientResolver defines the minimal persistence interface
// needed by the DBRecipientResolver. This avoids coupling to the full
// CommonPersistenceInterface.
type PersistenceForRecipientResolver interface {
	GetActiveChannelParticipantUserIDs(ctx context.Context, channelID uint64) ([]uint64, error)
	// GetHiddenAuthorizedChannelParticipantUserIDs returns the subset of active participants who are
	// authorized to receive hidden_from_users rows: bot users and channel admins/owners. It is used
	// for the split-recipient fanout of hidden transcript rows and tool-call resolution events
	// (Decision 7): regular users must never receive a hidden row's MESSAGE_SENT or its resolution
	// event.
	GetHiddenAuthorizedChannelParticipantUserIDs(ctx context.Context, channelID uint64) ([]uint64, error)
}

// eventIsHidden reports whether a chat event carries a hidden transcript row (or a hidden tool-call
// resolution), so the resolver restricts fanout to hidden-authorized participants only.
func eventIsHidden(event *ports.ChatEvent) bool {
	if event == nil {
		return false
	}
	if event.HiddenFromUsers {
		return true
	}
	if event.MessageStream != nil && event.MessageStream.HiddenFromUsers {
		return true
	}
	if event.Message != nil {
		if h := event.Message.GetHiddenFromUsers(); h != nil && *h {
			return true
		}
	}
	if event.ToolCallResolution != nil && event.ToolCallResolution.HiddenFromUsers {
		return true
	}
	return event.InferenceUsage != nil && event.InferenceUsageHidden
}

// DBRecipientResolver defines the data structure used in the realtime WebSocket flow for connection lifecycle, command routing, and event fan-out.
// It carries state across layer boundaries handled by this package.
type DBRecipientResolver struct {
	persistence PersistenceForRecipientResolver
	logger      *zap.Logger
}

var _ RecipientResolver = (*DBRecipientResolver)(nil)

// NewDBRecipientResolver executes this function as part of the realtime WebSocket flow for connection lifecycle, command routing, and event fan-out.
func NewDBRecipientResolver(
	persistence PersistenceForRecipientResolver,
	logger *zap.Logger,
) *DBRecipientResolver {
	if logger == nil {
		logger = zap.NewNop()
	}
	return &DBRecipientResolver{
		persistence: persistence,
		logger:      logger,
	}
}

// ResolveRecipients executes this method as part of the realtime WebSocket flow for connection lifecycle, command routing, and event fan-out.
func (r *DBRecipientResolver) ResolveRecipients(
	ctx context.Context,
	event *ports.ChatEvent,
) ([]common_service_models.UserIdType, error) {
	if r == nil || event == nil {
		return nil, nil
	}

	// Only deliberately targeted lifecycle notifications may override live membership.
	if event.TargetedRecipients && len(event.MemberUserIDs) > 0 && !eventIsHidden(event) {
		return event.MemberUserIDs, nil
	}

	if r.persistence == nil {
		appLogger.LogInvariantViolation(appLogger.For(ctx, r.logger, chatRecipientResolverLoggerName), "realtime_recipient_persistence_configured", nil,
			zap.String(logkeys.Operation, "resolve_realtime_recipients"),
			zap.String(logkeys.Stage, "recipient_lookup"),
			zap.Stringer(appLogger.KeyEventType, event.Type),
			zap.String(logkeys.Expected, "non-nil persistence"),
			zap.String(logkeys.Actual, "nil persistence"),
			zap.String(logkeys.Fallback, "drop_event"),
		)
		return nil, nil
	}

	// Extract channel ID based on event type.
	channelID, err := r.extractChannelID(event)
	if err != nil {
		if errors.Is(err, errRealtimeEventPayloadMissing) {
			appLogger.LogInvariantViolation(appLogger.For(ctx, r.logger, chatRecipientResolverLoggerName), "realtime_event_payload_present", err,
				zap.String(logkeys.Operation, "resolve_realtime_recipients"),
				zap.String(logkeys.Stage, "extract_channel_id"),
				zap.Stringer(appLogger.KeyEventType, event.Type),
				zap.String(logkeys.Expected, "event payload matching event type"),
				zap.String(logkeys.Actual, "required payload missing"),
				zap.String(logkeys.Fallback, "drop_event"),
			)
			return nil, nil
		}
		if appLogger.DebugEnabled(r.logger) {
			appLogger.For(ctx, r.logger, chatRecipientResolverLoggerName).Debug("could not extract channel ID from event",
				zap.Stringer(appLogger.KeyEventType, event.Type),
				zap.Error(err),
			)
		}
		return nil, nil // Not an error - just no recipients
	}

	if channelID == 0 {
		return nil, nil
	}

	// Hidden transcript rows + hidden tool-call resolutions fan out ONLY to hidden-authorized
	// participants (bot users and channel admins/owners); regular users must never receive them
	// (Decision 7). This is the split-recipient resolution — a single broadcast cannot express
	// "everyone except regular users", so for hidden events we resolve the privileged subset instead
	// of all participants.
	var userIDs []uint64
	if eventIsHidden(event) {
		userIDs, err = r.persistence.GetHiddenAuthorizedChannelParticipantUserIDs(ctx, channelID)
	} else {
		userIDs, err = r.persistence.GetActiveChannelParticipantUserIDs(ctx, channelID)
	}
	if err != nil {
		appLogger.For(ctx, r.logger, chatRecipientResolverLoggerName).Error("failed to resolve recipients from database",
			zap.String(logkeys.Operation, "resolve_realtime_recipients"),
			zap.String(logkeys.Stage, "recipient_lookup"),
			zap.Uint64(appLogger.KeyChannelID, channelID),
			zap.Stringer(appLogger.KeyEventType, event.Type),
			zap.Bool(appLogger.KeyHiddenEvent, eventIsHidden(event)),
			zap.String(logkeys.Fallback, "drop_event"),
			zap.Error(err),
		)
		// Return nil rather than error to allow best-effort delivery.
		// (don't fail the entire publish because of a DB hiccup)
		return nil, nil
	}

	// Convert []uint64 to []UserIdType.
	recipients := make([]common_service_models.UserIdType, len(userIDs))
	for i, id := range userIDs {
		recipients[i] = common_service_models.UserIdType(id)
	}

	return recipients, nil
}

// extractChannelID executes this method as part of the realtime WebSocket flow for connection lifecycle, command routing, and event fan-out.
func (r *DBRecipientResolver) extractChannelID(event *ports.ChatEvent) (uint64, error) {
	return chatEventChannelID(event)
}

func chatEventChannelID(event *ports.ChatEvent) (uint64, error) {
	switch event.Type {
	case proto_models.ChatEventType_CHAT_EVENT_TYPE_MESSAGE_SENT,
		proto_models.ChatEventType_CHAT_EVENT_TYPE_MESSAGE_EDITED,
		proto_models.ChatEventType_CHAT_EVENT_TYPE_MESSAGE_DELETED,
		proto_models.ChatEventType_CHAT_EVENT_TYPE_MESSAGE_ATTACHMENTS_READY:
		if event.Message != nil {
			return event.Message.ChannelID, nil
		}
		return 0, fmt.Errorf("%w: message event missing Message field", errRealtimeEventPayloadMissing)

	case proto_models.ChatEventType_CHAT_EVENT_TYPE_MESSAGE_STREAM_STARTED,
		proto_models.ChatEventType_CHAT_EVENT_TYPE_MESSAGE_STREAM_DELTA,
		proto_models.ChatEventType_CHAT_EVENT_TYPE_MESSAGE_STREAM_FINISHED,
		proto_models.ChatEventType_CHAT_EVENT_TYPE_MESSAGE_STREAM_FAILED:
		if event.MessageStream != nil {
			return uint64(event.MessageStream.ChannelID), nil
		}
		return 0, fmt.Errorf("%w: message stream event missing MessageStream field", errRealtimeEventPayloadMissing)

	case proto_models.ChatEventType_CHAT_EVENT_TYPE_REACTION_ADDED,
		proto_models.ChatEventType_CHAT_EVENT_TYPE_REACTION_REMOVED:
		// Reaction events have Message with ChannelID populated by chat handler.
		if event.Message != nil {
			return event.Message.ChannelID, nil
		}
		return 0, fmt.Errorf("%w: reaction event missing Message field", errRealtimeEventPayloadMissing)

	case proto_models.ChatEventType_CHAT_EVENT_TYPE_INFERENCE_USAGE_UPDATED:
		if event.InferenceUsage != nil {
			return event.InferenceUsage.ChannelId, nil
		}
		return 0, fmt.Errorf("%w: inference event missing InferenceUsage field", errRealtimeEventPayloadMissing)

	case proto_models.ChatEventType_CHAT_EVENT_TYPE_TOOL_CALL_RESOLUTION:
		if event.ToolCallResolution != nil {
			return uint64(event.ToolCallResolution.ChannelID), nil
		}
		return 0, fmt.Errorf("%w: tool call resolution event missing ToolCallResolution field", errRealtimeEventPayloadMissing)

	case proto_models.ChatEventType_CHAT_EVENT_TYPE_CONTEXT_COMPACTION:
		if event.ContextCompaction != nil {
			return event.ContextCompaction.ChannelID, nil
		}
		return 0, fmt.Errorf("%w: context compaction event missing ContextCompaction field", errRealtimeEventPayloadMissing)

	case proto_models.ChatEventType_CHAT_EVENT_TYPE_AGENT_RUN_STATE:
		if event.AgentRunState != nil {
			return event.AgentRunState.ChannelID, nil
		}
		return 0, fmt.Errorf("%w: agent run state event missing AgentRunState field", errRealtimeEventPayloadMissing)

	case proto_models.ChatEventType_CHAT_EVENT_TYPE_AGENT_SESSION_INTERACTION:
		if event.AgentSessionInteraction != nil {
			return event.AgentSessionInteraction.ChannelID, nil
		}
		return 0, fmt.Errorf("%w: agent session interaction event missing AgentSessionInteraction field", errRealtimeEventPayloadMissing)

	case proto_models.ChatEventType_CHAT_EVENT_TYPE_CHANNEL_CREATED,
		proto_models.ChatEventType_CHAT_EVENT_TYPE_CHANNEL_UPDATED,
		proto_models.ChatEventType_CHAT_EVENT_TYPE_CHANNEL_DELETED:
		// Ordinary channel events resolve current membership just like message events.
		if event.Channel != nil {
			return event.Channel.ID, nil
		}
		return 0, fmt.Errorf("%w: channel event missing Channel field", errRealtimeEventPayloadMissing)

	default:
		return 0, errors.New("unrecognized event type")
	}
}
