package common_api

import (
	"context"
	"fmt"

	"github.com/Suhaibinator/go-common/pkg/common_service/common_service_models"
	"github.com/Suhaibinator/go-common/pkg/common_service/ports"
	"github.com/Suhaibinator/go-common/pkg/common_ws"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
)

func (h *CommonAPIHandler[U]) wireNotificationsIntoRealtime() error {
	if h == nil || h.serviceHandler == nil {
		return nil
	}
	n := h.serviceHandler.GetNotificationHandler()
	if n == nil {
		return nil
	}
	if h.realtimeHub != nil {
		activity, err := h.realtimeHub.EnableNotificationActivityWithBackend(h.notificationActivityBackend)
		if err != nil {
			return fmt.Errorf("register notification activity command: %w", err)
		}
		n.SetActivityStore(activity)
	}
	// Like the chat wiring, a publisher the application installed is kept, and a handler without
	// realtime installs nothing.
	if h.realtimePublisher == nil || n.ChangedPublisher() != nil {
		return nil
	}
	n.SetChangedPublisher(func(ctx context.Context, userID, id, revision uint64) error {
		if h.realtimePublisher == nil {
			return nil
		}
		return ports.PublishAfterCommit(ctx, h.realtimePublisher, &ports.RealtimeEvent{
			Topics:    []string{common_ws.UserTopic(common_service_models.UserIdType(userID))},
			EventType: "notification.changed",
			Payload:   &proto_models.NotificationChanged{Id: id, Revision: revision},
		})
	})
	return nil
}
