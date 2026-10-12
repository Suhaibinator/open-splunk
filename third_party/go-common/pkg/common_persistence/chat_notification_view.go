package common_persistence

import (
	"context"
	"slices"

	"github.com/Suhaibinator/SRouter/pkg/logkeys"
	models "github.com/Suhaibinator/go-common/pkg/common_persistence/common_persistence_models"
	pb "github.com/Suhaibinator/go-common/pkg/proto_models"
	"go.uber.org/zap"
)

// ChatNotificationViewUserBatchSize bounds one display-user read: each
// candidate contributes at most a direct-message counterpart and a last sender.
const ChatNotificationViewUserBatchSize = 2 * ChatNotificationCandidateBatchSize

// LoadChatNotificationDirectParticipants returns the active participant user
// IDs of each requested direct channel, keyed by channel and ordered by user
// ID. Group channels contribute nothing, so a misdirected ID never reads a
// group roster. It reads fresh ambient-transaction data.
func (h *CommonPersistenceHandler[U]) LoadChatNotificationDirectParticipants(ctx context.Context, channelIDs []uint64) (map[uint64][]uint64, error) {
	if len(channelIDs) > ChatNotificationCandidateBatchSize {
		return nil, ErrNotificationInput
	}
	out := make(map[uint64][]uint64, len(channelIDs))
	if len(channelIDs) == 0 {
		return out, nil
	}
	var rows []models.ChatChannelParticipant
	err := h.getDBFromContext(ctx).Table(models.TableNameChatChannelParticipants+" p").
		Select("p.channel_id", "p.user_id").
		Joins("JOIN "+models.TableNameChatChannels+" c ON c.id = p.channel_id AND c.deleted_at IS NULL AND c.channel_type = ?", pb.ChannelType_CHANNEL_TYPE_DIRECT).
		Where("p.channel_id IN ? AND p.left_at IS NULL", notificationUniqueIDs(slices.Clone(channelIDs))).
		Order("p.channel_id, p.user_id").
		Find(&rows).Error
	if err != nil {
		return nil, chatPersistenceDatabaseError(err, zap.String(logkeys.Stage, "load_notification_view_direct_participants"), zap.Int("channel_count", len(channelIDs)))
	}
	for _, row := range rows {
		out[row.ChannelID] = append(out[row.ChannelID], row.UserID)
	}
	return out, nil
}

// LoadChatNotificationDisplayUsers loads the user rows whose current display
// names a notification view shows. Like the chat roster's user preload, it
// keeps banned users and bots and omits soft-deleted users.
func (h *CommonPersistenceHandler[U]) LoadChatNotificationDisplayUsers(ctx context.Context, userIDs []uint64) (map[uint64]*models.User, error) {
	if len(userIDs) > ChatNotificationViewUserBatchSize {
		return nil, ErrNotificationInput
	}
	out := make(map[uint64]*models.User, len(userIDs))
	if len(userIDs) == 0 {
		return out, nil
	}
	var rows []*models.User
	if err := h.getDBFromContext(ctx).Where("id IN ?", notificationUniqueIDs(slices.Clone(userIDs))).Find(&rows).Error; err != nil {
		return nil, chatPersistenceDatabaseError(err, zap.String(logkeys.Stage, "load_notification_view_users"), zap.Int("user_count", len(userIDs)))
	}
	for _, row := range rows {
		out[row.ID] = row
	}
	return out, nil
}
