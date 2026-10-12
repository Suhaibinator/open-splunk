package common_ws

import (
	"context"
	"time"

	"github.com/Suhaibinator/SRouter/pkg/logkeys"
	"github.com/Suhaibinator/go-common/pkg/common_service/common_service_models"
	"github.com/Suhaibinator/go-common/pkg/common_service/ports"
	appLogger "github.com/Suhaibinator/go-common/pkg/logger"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// HubRealtimePublisher is the in-process, websocket-backed ports.RealtimePublisher: it wraps an
// event's payload in the RealtimeEvent envelope and hands the serialized frame to a Hub.
//
// Marshaling is done once per distinct frame, never once per recipient. User-topic fan-out stamps
// the recipient-relative literal topic "user" on the envelope, so a single marshal and a single
// []byte serve every recipient — the naive per-recipient marshal is a hot-path regression, worst on
// token stream deltas. Entity-topic frames are marshaled once per topic and shared by that topic's
// subscribers.
type HubRealtimePublisher[U common_service_models.UserLike] struct {
	hub    *Hub[U]
	logger *zap.Logger
}

var _ ports.RealtimePublisher = (*HubRealtimePublisher[common_service_models.User])(nil)

// NewHubRealtimePublisher creates the websocket-backed realtime publisher.
//
// The publisher holds the hub, not a copy of its settings. A nil hub is accepted and makes every publish a silent no-op, which is what lets an application hold a
// ports.RealtimePublisher whether or not the transport is enabled.
//
// Parameters:
//   - hub: Hub the serialized frames are handed to; nil makes publishing a no-op.
//   - logger: Logger for publish diagnostics; a no-op logger is used when nil.
//
// Returns:
//   - result: The publisher, ready to be handed to application code as a ports.RealtimePublisher.
func NewHubRealtimePublisher[U common_service_models.UserLike](hub *Hub[U], logger *zap.Logger) *HubRealtimePublisher[U] {
	if logger == nil {
		logger = zap.NewNop()
	}
	return &HubRealtimePublisher[U]{
		hub:    hub,
		logger: logger,
	}
}

// Publish serializes the event and fans it out. It is best-effort: a marshal failure is returned
// with Error-level invariant context so the owning caller can record it once without failing its
// business flow.
//
// Parameters:
//   - ctx: Request context used for cancellation, deadlines, and tracing.
//   - event: The event, its topics, and its payload message.
func (p *HubRealtimePublisher[U]) Publish(ctx context.Context, event *ports.RealtimeEvent) error {
	if p == nil || p.hub == nil || event == nil || len(event.Topics) == 0 {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	var payload []byte
	if event.Payload != nil {
		encoded, err := proto.Marshal(event.Payload)
		if err != nil {
			// A payload that will not serialize is a publishing-code invariant violation.
			// Carry its severity and diagnostics to the owning boundary so the failure is
			// recorded once rather than once here and again by the caller.
			p.recordFailure(publishFailureMarshalPayload)
			return appLogger.InvariantError("realtime_payload_serializable", err,
				zap.String(logkeys.Operation, "publish_realtime_event"),
				zap.String(logkeys.Stage, "marshal_payload"),
				zap.String(appLogger.KeyEventType, event.EventType),
				zap.String(logkeys.Expected, "protobuf payload is serializable"),
				zap.String(logkeys.Actual, "protobuf marshal failed"),
			)
		}
		payload = encoded
	}

	occurredAt := event.OccurredAt
	if occurredAt.IsZero() {
		occurredAt = time.Now()
	}
	timestamp := timestamppb.New(occurredAt)

	userIDs := make([]common_service_models.UserIdType, 0, len(event.Topics))
	entityTopics := make([]string, 0, len(event.Topics))
	for _, topic := range event.Topics {
		if err := ctx.Err(); err != nil {
			return err
		}
		if userID, ok := parseUserTopic(topic); ok {
			userIDs = append(userIDs, userID)
			continue
		}
		if topic == "" {
			continue
		}
		entityTopics = append(entityTopics, topic)
	}

	var firstErr error
	if len(userIDs) > 0 {
		if err := ctx.Err(); err != nil {
			return err
		}
		frame, err := p.encodeFrame(TopicUser, event.EventType, timestamp, payload)
		if err != nil {
			firstErr = err
		} else if err := p.hub.BroadcastToUsers(userIDs, frame); err != nil {
			p.reportUndeliveredFrame(ctx, TopicUser, event.EventType, err)
			firstErr = err
		}
	}
	for _, topic := range entityTopics {
		if err := ctx.Err(); err != nil {
			return err
		}
		frame, err := p.encodeFrame(topic, event.EventType, timestamp, payload)
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		if err := p.hub.Publish(topic, frame); err != nil {
			p.reportUndeliveredFrame(ctx, topic, event.EventType, err)
			if firstErr == nil {
				firstErr = err
			}
		}
	}
	return firstErr
}

// reportUndeliveredFrame accounts for a frame the hub refused. Today the hub only refuses while it
// is shutting down, which is expected during a deploy and therefore a Warn rather than an Error —
// but it is counted so a steady rate outside shutdown is visible instead of invisible.
func (p *HubRealtimePublisher[U]) reportUndeliveredFrame(ctx context.Context, topic string, eventType string, err error) {
	p.recordFailure(publishFailureHubClosed)
	appLogger.For(ctx, p.logger, realtimePublisherLoggerName).Warn("realtime frame dropped by the hub",
		zap.String(appLogger.KeyTopic, topic),
		zap.String(appLogger.KeyEventType, eventType),
		zap.Error(err),
	)
}

// recordFailure reports one publish failure through the hub's metrics recorder. The publisher has no
// recorder of its own on purpose: the hub owns the instrumentation surface, so an application that
// supplied one recorder sees both halves of the publish path in it.
func (p *HubRealtimePublisher[U]) recordFailure(cause string) {
	if p == nil || p.hub == nil {
		return
	}
	p.hub.metrics.RecordRealtimePublishFailure(cause)
}

// encodeFrame serializes one RealtimeEvent envelope. The already-serialized inner payload is shared
// across every frame of a publish, so only the envelope is rebuilt per topic.
func (p *HubRealtimePublisher[U]) encodeFrame(
	topic string,
	eventType string,
	occurredAt *timestamppb.Timestamp,
	payload []byte,
) ([]byte, error) {
	frame, err := proto.Marshal(&proto_models.RealtimeEvent{
		Topic:      topic,
		EventType:  eventType,
		OccurredAt: occurredAt,
		Payload:    payload,
	})
	if err != nil {
		// Every field of the envelope is server-owned, so this is an internal
		// invariant violation carried to the owning boundary.
		p.recordFailure(publishFailureMarshalEnvelope)
		return nil, appLogger.InvariantError("realtime_envelope_serializable", err,
			zap.String(logkeys.Operation, "publish_realtime_event"),
			zap.String(logkeys.Stage, "marshal_envelope"),
			zap.String(appLogger.KeyTopic, topic),
			zap.String(appLogger.KeyEventType, eventType),
			zap.String(logkeys.Expected, "server-owned envelope is serializable"),
			zap.String(logkeys.Actual, "protobuf marshal failed"),
		)
	}
	return frame, nil
}
