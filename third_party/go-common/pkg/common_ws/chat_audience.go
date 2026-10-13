package common_ws

import (
	"context"
	"errors"
	"time"

	"github.com/Suhaibinator/go-common/pkg/common_service/common_service_models"
	"github.com/Suhaibinator/go-common/pkg/common_service/ports"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// PublishChatAudience delivers one logical event to background participants and leased, open-chat
// viewers. The audiences are disjoint per user, but viewer delivery remains connection-scoped.
// All authorization and generation checks happen at enqueue under the revocation lock.
func (p *HubRealtimePublisher[U]) PublishChatAudience(ctx context.Context, event *ports.RealtimeEvent, channelID uint64, category ports.ChatRealtimeCategory, recipients []common_service_models.UserIdType, generation uint64) error {
	return p.publishChatAudience(ctx, event, channelID, category, recipients, generation)
}

// PublishChatAccessChange is PublishChatAudience for an event announcing a committed access change.
// Viewer grants have already been suspended by the committed hook. Only refreshed participants
// receive this event; bounded renewal restores viewer delivery and triggers client catch-up.
func (p *HubRealtimePublisher[U]) PublishChatAccessChange(ctx context.Context, event *ports.RealtimeEvent, channelID uint64, category ports.ChatRealtimeCategory, recipients []common_service_models.UserIdType, generation uint64) error {
	if p == nil || p.hub == nil || event == nil {
		return nil
	}
	// Access-change hooks suspend viewers first. Restoration and catch-up are owned by the
	// bounded renewal workers; this publication only reaches the refreshed participant audience.
	return p.publishChatAudience(ctx, event, channelID, 0, recipients, generation)
}

// publishChatAudience enqueues for participants and leased viewers.
func (p *HubRealtimePublisher[U]) publishChatAudience(ctx context.Context, event *ports.RealtimeEvent, channelID uint64, category ports.ChatRealtimeCategory, recipients []common_service_models.UserIdType, generation uint64) error {
	if p == nil || p.hub == nil || event == nil {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	payload, err := proto.Marshal(event.Payload)
	if err != nil {
		p.recordFailure(publishFailureMarshalPayload)
		return err
	}
	at := event.OccurredAt
	if at.IsZero() {
		at = time.Now()
	}
	stamp := timestamppb.New(at)
	participantFrame, err := p.encodeFrame(TopicUser, event.EventType, stamp, payload)
	if err != nil {
		return err
	}
	topic := ChatChannelTopic(channelID)
	var viewerFrame []byte
	if channelID != 0 && category != 0 {
		viewerFrame, err = p.encodeFrame(topic, event.EventType, stamp, payload)
		if err != nil {
			return err
		}
	}
	excluded := common_service_models.UserIdType(0)
	if category == ports.ChatRealtimeTyping {
		if chatEvent, ok := event.Payload.(*proto_models.ChatWebSocketEvent); ok {
			excluded = common_service_models.UserIdType(chatEvent.ActorUserId)
		}
	}
	h := p.hub
	var slow []*connection[U]
	delivered, dropped := 0, 0
	enqueue := func(c *connection[U], frame []byte) {
		if err := c.enqueue(frame); err == nil {
			delivered++
		} else {
			dropped++
			if errors.Is(err, ErrBackpressure) {
				slow = append(slow, c)
			}
		}
	}
	h.mu.RLock()
	if h.closed.Load() {
		h.mu.RUnlock()
		return ErrHubClosed
	}
	if err := ctx.Err(); err != nil {
		h.mu.RUnlock()
		return err
	}
	if h.chat != nil && h.chat.generation != generation {
		h.mu.RUnlock()
		return nil
	}
	seen := make(map[common_service_models.UserIdType]struct{}, len(recipients))
	for _, id := range recipients {
		if id == 0 || id == excluded {
			continue
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		for c := range h.connsByUser[id] {
			enqueue(c, participantFrame)
		}
	}
	if len(viewerFrame) > 0 && h.chat != nil {
		now := time.Now()
		for c := range h.connsByTopic[topic] {
			if c.userID == excluded {
				continue
			}
			if _, participant := seen[c.userID]; participant {
				continue
			}
			grant := h.chat.grants[chatGrantKey{c.userID, channelID}]
			if grant == nil || grant.suspended || !now.Before(grant.expires) || grant.categories&category != category {
				continue
			}
			enqueue(c, viewerFrame)
		}
	}
	h.mu.RUnlock()
	h.closeChatSlowConnections(slow)
	h.metrics.RecordRealtimePublish(delivered, dropped)
	return nil
}
