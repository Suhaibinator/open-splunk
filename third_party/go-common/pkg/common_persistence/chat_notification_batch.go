package common_persistence

import (
	"context"
	"errors"
	"slices"

	"github.com/Suhaibinator/SRouter/pkg/logkeys"
	models "github.com/Suhaibinator/go-common/pkg/common_persistence/common_persistence_models"
	"github.com/Suhaibinator/go-common/pkg/config"
	appLogger "github.com/Suhaibinator/go-common/pkg/logger"
	"go.uber.org/zap"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const ChatNotificationCandidateBatchSize = 64
const ChatNotificationSourceBatchSize = 8

// ChatNotificationCandidate identifies one fresh recipient/source evaluation.
type ChatNotificationCandidate struct{ MessageID, RecipientID uint64 }

// ChatNotificationSources is an operation-local snapshot, never an authorization
// grant. Messages retain the same relations as ordinary scalar source reads.
// Map keys for Memberships and Policies are (channel,user) and (session,version).
type ChatNotificationSources struct {
	Messages    map[uint64]*models.ChatMessage
	Channels    map[uint64]*models.ChatChannel
	Users       map[uint64]*models.User
	Memberships map[[2]uint64]*models.ChatChannelParticipant
	Runs        map[uint64]*models.ChatAgentRun
	Policies    map[[2]uint64]*models.ChatAgentSessionConfigSnapshot
}

// Membership returns the active participant row of userID in channelID, or nil
// when the snapshot holds none; a missing row never grants access.
func (s *ChatNotificationSources) Membership(channelID, userID uint64) *models.ChatChannelParticipant {
	if s == nil {
		return nil
	}
	return s.Memberships[[2]uint64{channelID, userID}]
}

// ChatNotificationSourceOptions limits approval-only hydration. Message,
// channel and user callback inputs are unchanged.
type ChatNotificationSourceOptions struct {
	IncludeApproval bool
}

// ChatNotificationPersistence is the required chat-side notification read
// contract of CommonPersistenceInterface. The service has no scalar fallback:
// every read below observes fresh ambient transaction data and never a cache.
type ChatNotificationPersistence interface {
	// LoadChatNotificationSourcesWithOptions hydrates one bounded batch of
	// message/recipient candidates for a single source evaluation.
	LoadChatNotificationSourcesWithOptions(context.Context, []ChatNotificationCandidate, ChatNotificationSourceOptions) (*ChatNotificationSources, error)
	// ListChatNotificationRecipientIDs is a keyset scan of eligible member IDs.
	ListChatNotificationRecipientIDs(ctx context.Context, channelID, after uint64, limit int) ([]uint64, error)
	// GetChatNotificationParticipant reads one active membership without display
	// hydration or bot-roster reads; nil means no active membership.
	GetChatNotificationParticipant(ctx context.Context, channelID, userID uint64) (*models.ChatChannelParticipant, error)
	// LoadChatActivitySources hydrates channel, user and membership rows for
	// channel activity candidates; it never loads messages.
	LoadChatActivitySources(context.Context, []ChatActivityCandidate) (*ChatNotificationSources, error)
	// LockChatActivityEmission fences recipient eligibility against concurrent
	// departure and channel deletion until the source transaction commits.
	LockChatActivityEmission(ctx context.Context, channelID uint64, recipients []uint64) error
	// LoadChatNotificationDirectParticipants returns active participant IDs of
	// direct channels for read-time view counterpart names.
	LoadChatNotificationDirectParticipants(ctx context.Context, channelIDs []uint64) (map[uint64][]uint64, error)
	// LoadChatNotificationDisplayUsers loads users whose current display names
	// a read-time view shows; soft-deleted users are absent.
	LoadChatNotificationDisplayUsers(ctx context.Context, userIDs []uint64) (map[uint64]*models.User, error)
	// ChatUnreadNotificationsEnabled reads the channel-owned unread policy.
	ChatUnreadNotificationsEnabled(ctx context.Context, channelID uint64) (bool, error)
	// Async fan-out reads: fresh, nonlocking, in the ambient materialization transaction.
	ListChatNotificationRecipientCandidates(ctx context.Context, channelID, after uint64, limit int, source models.NotificationTransactionStamp, snapshot models.NotificationVisibilitySnapshot) ([]ChatNotificationRecipient, error)
	FilterChatNotificationRecipientCandidates(ctx context.Context, channelID uint64, ids []uint64, source models.NotificationTransactionStamp, snapshot models.NotificationVisibilitySnapshot) ([]ChatNotificationRecipient, error)
	ValidateChatNotificationRecipient(ctx context.Context, channelID, messageID, recipientID uint64, observed models.NotificationTransactionStamp, source ChatNotificationSourceState) (bool, error)
}

// ListChatNotificationRecipientIDs is a keyset scan of IDs only. Display users,
// avatars and bot rosters are not hydrated. Authorization still runs separately.
func (h *CommonPersistenceHandler[U]) ListChatNotificationRecipientIDs(ctx context.Context, channelID, after uint64, limit int) ([]uint64, error) {
	if limit <= 0 || limit > ChatNotificationCandidateBatchSize {
		return nil, ErrNotificationInput
	}
	var ids []uint64
	err := h.getDBFromContext(ctx).Table(models.TableNameChatChannelParticipants+" p").Joins("JOIN "+models.TableNameUsers+" u ON u.id = p.user_id AND u.deleted_at IS NULL").Where("p.channel_id = ? AND p.user_id > ? AND p.left_at IS NULL AND u.is_bot = false AND u.banned = false", channelID, after).Order("p.user_id ASC").Limit(limit).Pluck("p.user_id", &ids).Error
	if err != nil {
		return nil, chatPersistenceDatabaseError(err, zap.String(logkeys.Stage, "list_notification_recipient_ids"), zap.Uint64(appLogger.KeyChannelID, channelID), zap.Uint64(appLogger.KeyUserID, after))
	}
	return ids, nil
}

func (h *CommonPersistenceHandler[U]) LoadChatNotificationSourcesWithOptions(ctx context.Context, candidates []ChatNotificationCandidate, options ChatNotificationSourceOptions) (*ChatNotificationSources, error) {
	if len(candidates) > ChatNotificationCandidateBatchSize {
		return nil, ErrNotificationInput
	}
	out := &ChatNotificationSources{Messages: map[uint64]*models.ChatMessage{}, Channels: map[uint64]*models.ChatChannel{}, Users: map[uint64]*models.User{}, Memberships: map[[2]uint64]*models.ChatChannelParticipant{}, Runs: map[uint64]*models.ChatAgentRun{}, Policies: map[[2]uint64]*models.ChatAgentSessionConfigSnapshot{}}
	messageIDs, userIDs := []uint64{}, []uint64{}
	for _, c := range candidates {
		messageIDs = append(messageIDs, c.MessageID)
		userIDs = append(userIDs, c.RecipientID)
	}
	messageIDs = notificationUniqueIDs(messageIDs)
	userIDs = notificationUniqueIDs(userIDs)
	if len(messageIDs) > ChatNotificationSourceBatchSize {
		return nil, ErrNotificationInput
	}
	if len(messageIDs) == 0 {
		return out, nil
	}
	db := h.getDBFromContext(ctx)
	loadMessages := func(ids []uint64) error {
		if len(ids) == 0 {
			return nil
		}
		var rows []*models.ChatMessage
		if err := preloadChatMessageAssociations(db).Where("id IN ?", ids).Find(&rows).Error; err != nil {
			return err
		}
		if err := h.attachChatMessageToolCalls(ctx, rows); err != nil {
			return err
		}
		for _, row := range rows {
			out.Messages[row.ID] = row
		}
		return nil
	}
	if err := loadMessages(messageIDs); err != nil {
		return nil, chatPersistenceDatabaseError(err, zap.String(logkeys.Stage, "load_notification_messages"), zap.Int("message_count", len(messageIDs)))
	}
	parents, channels, runs := []uint64{}, []uint64{}, []uint64{}
	for _, id := range messageIDs {
		row := out.Messages[id]
		if row == nil {
			continue
		}
		channels = append(channels, row.ChannelID)
		if row.ParentMessageID != nil && *row.ParentMessageID != 0 && out.Messages[*row.ParentMessageID] == nil {
			parents = append(parents, *row.ParentMessageID)
		}
		if options.IncludeApproval && row.AgentRunID != nil {
			runs = append(runs, *row.AgentRunID)
		}
	}
	if err := loadMessages(notificationUniqueIDs(parents)); err != nil {
		return nil, chatPersistenceDatabaseError(err, zap.String(logkeys.Stage, "load_notification_parents"), zap.Int("message_count", len(parents)))
	}
	channels = notificationUniqueIDs(channels)
	runs = notificationUniqueIDs(runs)
	pairs := [][2]uint64{}
	for _, c := range candidates {
		if row := out.Messages[c.MessageID]; row != nil {
			pairs = append(pairs, [2]uint64{row.ChannelID, c.RecipientID})
		}
	}
	if err := hydrateNotificationAuthorizationSources(db, out, channels, userIDs, pairs, "load_notification_", len(candidates)); err != nil {
		return nil, err
	}
	if len(runs) > 0 {
		var rows []*models.ChatAgentRun
		if err := db.Where("id IN ?", runs).Find(&rows).Error; err != nil {
			return nil, chatPersistenceDatabaseError(err, zap.String(logkeys.Stage, "load_notification_runs"), zap.Int("candidate_count", len(candidates)))
		}
		configs := [][2]uint64{}
		for _, row := range rows {
			out.Runs[row.ID] = row
			if row.AgentSessionID != nil && row.AgentSessionConfigVersion != nil {
				configs = append(configs, [2]uint64{*row.AgentSessionID, *row.AgentSessionConfigVersion})
			}
		}
		if len(configs) > 0 {
			var snapshots []*models.ChatAgentSessionConfigSnapshot
			if err := notificationPairQuery(db, "agent_session_id", "version", configs).Find(&snapshots).Error; err != nil {
				return nil, chatPersistenceDatabaseError(err, zap.String(logkeys.Stage, "load_notification_policies"), zap.Int("candidate_count", len(candidates)))
			}
			for _, row := range snapshots {
				out.Policies[[2]uint64{row.AgentSessionID, row.Version}] = row
			}
		}
	}
	return out, nil
}
func notificationUniqueIDs(ids []uint64) []uint64 { slices.Sort(ids); return slices.Compact(ids) }
func notificationPairQuery(db *gorm.DB, first, second string, pairs [][2]uint64) *gorm.DB {
	seen := map[[2]uint64]bool{}
	q := db.Session(&gorm.Session{NewDB: true}).Where("1 = 0")
	for _, pair := range pairs {
		if !seen[pair] {
			q = q.Or(first+" = ? AND "+second+" = ?", pair[0], pair[1])
			seen[pair] = true
		}
	}
	return db.Where(q)
}

// GetChatNotificationParticipant avoids display hydration and bot-roster reads
// when independently rechecking interruption preferences immediately before send.
func (h *CommonPersistenceHandler[U]) GetChatNotificationParticipant(ctx context.Context, channelID, userID uint64) (*models.ChatChannelParticipant, error) {
	var row models.ChatChannelParticipant
	err := h.getDBFromContext(ctx).Where("channel_id = ? AND user_id = ? AND left_at IS NULL", channelID, userID).Take(&row).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, chatPersistenceDatabaseError(err, zap.String(logkeys.Stage, "load_notification_participant"), zap.Uint64(appLogger.KeyChannelID, channelID), zap.Uint64(appLogger.KeyUserID, userID))
	}
	return &row, nil
}

// ChatActivityCandidate addresses channel activity without retaining message identity.
type ChatActivityCandidate struct{ ChannelID, RecipientID uint64 }

// LoadChatActivitySources performs a bounded set of channel, user and membership
// queries. It deliberately does not hydrate messages or scan message history.
func (h *CommonPersistenceHandler[U]) LoadChatActivitySources(ctx context.Context, candidates []ChatActivityCandidate) (*ChatNotificationSources, error) {
	if len(candidates) > ChatNotificationCandidateBatchSize {
		return nil, ErrNotificationInput
	}
	out := &ChatNotificationSources{Channels: map[uint64]*models.ChatChannel{}, Users: map[uint64]*models.User{}, Memberships: map[[2]uint64]*models.ChatChannelParticipant{}}
	if len(candidates) == 0 {
		return out, nil
	}
	var channels, users []uint64
	var pairs [][2]uint64
	for _, c := range candidates {
		channels = append(channels, c.ChannelID)
		users = append(users, c.RecipientID)
		pairs = append(pairs, [2]uint64{c.ChannelID, c.RecipientID})
	}
	db := h.getDBFromContext(ctx)
	if err := hydrateNotificationAuthorizationSources(db, out, channels, users, pairs, "load_activity_", len(candidates)); err != nil {
		return nil, err
	}
	return out, nil
}

func hydrateNotificationAuthorizationSources(db *gorm.DB, out *ChatNotificationSources, channels, users []uint64, pairs [][2]uint64, stage string, count int) error {
	wrap := func(err error, suffix string) error {
		return chatPersistenceDatabaseError(err, zap.String(logkeys.Stage, stage+suffix), zap.Int("candidate_count", count))
	}
	if len(channels) > 0 {
		var rows []*models.ChatChannel
		if err := preloadPublicChatAvatar(db).Where("id IN ?", notificationUniqueIDs(channels)).Find(&rows).Error; err != nil {
			return wrap(err, "channels")
		}
		for _, row := range rows {
			sanitizeChatChannelAvatar(row)
			out.Channels[row.ID] = row
		}
	}
	if len(users) > 0 {
		var rows []*models.User
		if err := db.Select("id", "banned", "is_bot", "deleted_at").Where("id IN ?", notificationUniqueIDs(users)).Find(&rows).Error; err != nil {
			return wrap(err, "users")
		}
		for _, row := range rows {
			out.Users[row.ID] = row
		}
	}
	if len(pairs) > 0 {
		var rows []*models.ChatChannelParticipant
		if err := notificationPairQuery(db, "channel_id", "user_id", pairs).Where("left_at IS NULL").Find(&rows).Error; err != nil {
			return wrap(err, "memberships")
		}
		for _, row := range rows {
			out.Memberships[[2]uint64{row.ChannelID, row.UserID}] = row
		}
	}
	return nil
}

// LockChatActivityEmission fences recipient eligibility against concurrent
// departure and channel deletion until the source transaction commits.
func (h *CommonPersistenceHandler[U]) LockChatActivityEmission(ctx context.Context, channelID uint64, recipients []uint64) error {
	if len(recipients) > config.MaxNotificationFanoutBatchRecipients {
		return ErrNotificationInput
	}
	db := h.getDBFromContext(ctx)
	var channels []models.ChatChannel
	if err := db.Select("id").Where("id = ?", channelID).Clauses(clause.Locking{Strength: "SHARE"}).Find(&channels).Error; err != nil {
		return chatPersistenceDatabaseError(err, zap.String(logkeys.Stage, "lock_activity_channel"))
	}
	if len(channels) == 0 {
		return nil
	}
	var members []models.ChatChannelParticipant
	if err := db.Select("channel_id", "user_id").Where("channel_id = ? AND user_id IN ? AND left_at IS NULL", channelID, recipients).Order("user_id").Clauses(clause.Locking{Strength: "SHARE"}).Find(&members).Error; err != nil {
		return chatPersistenceDatabaseError(err, zap.String(logkeys.Stage, "lock_activity_recipients"))
	}
	return nil
}
