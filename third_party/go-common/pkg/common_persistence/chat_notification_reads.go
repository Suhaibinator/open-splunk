package common_persistence

import (
	"context"

	"github.com/Suhaibinator/SRouter/pkg/logkeys"
	models "github.com/Suhaibinator/go-common/pkg/common_persistence/common_persistence_models"
	appLogger "github.com/Suhaibinator/go-common/pkg/logger"
	"go.uber.org/zap"
)

// ChatUnreadNotificationsEnabled reads the channel-owned notification policy
// without hydrating participants, avatars, or transcript history.
func (h *CommonPersistenceHandler[U]) ChatUnreadNotificationsEnabled(ctx context.Context, channelID uint64) (bool, error) {
	var channel models.ChatChannel
	err := h.getDBFromContext(ctx).Select("id", "unread_notifications_enabled").First(&channel, channelID).Error
	if err != nil {
		return false, chatPersistenceDatabaseError(err, zap.String(logkeys.Stage, "read_chat_notification_policy"), zap.Uint64(appLogger.KeyChannelID, channelID))
	}
	return channel.UnreadNotificationsEnabled, nil
}
