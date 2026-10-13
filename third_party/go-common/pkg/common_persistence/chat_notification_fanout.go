package common_persistence

import (
	"context"

	models "github.com/Suhaibinator/go-common/pkg/common_persistence/common_persistence_models"
	"github.com/Suhaibinator/go-common/pkg/config"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// ChatNotificationRecipient retains the membership generation observed during
// enumeration. It is rechecked after acquiring the notification advisory lock.
type ChatNotificationRecipient struct {
	RecipientID     uint64
	MembershipStamp models.NotificationTransactionStamp
}

// ChatNotificationSourceState records the source visibility used by authorization.
// A post-lock check suppresses work if those inputs changed while permission
// callbacks ran; hidden messages remain eligible for authorized readers.
type ChatNotificationSourceState struct {
	ParentMessageID uint64
	Hidden          bool
	ParentHidden    bool
	RequireUnread   bool
}

func (h *CommonPersistenceHandler[U]) chatNotificationAudience(ctx context.Context, channelID uint64, source models.NotificationTransactionStamp, snapshot models.NotificationVisibilitySnapshot) *gorm.DB {
	q := h.DB(ctx).Table(models.TableNameChatChannelParticipants+" p").
		Joins("JOIN "+models.TableNameUsers+" u ON u.id = p.user_id AND u.deleted_at IS NULL AND NOT u.banned AND NOT u.is_bot").
		Joins("JOIN "+models.TableNameChatChannels+" c ON c.id = p.channel_id AND c.deleted_at IS NULL").
		Where("p.channel_id = ? AND p.left_at IS NULL", channelID)
	identity := h.NotificationTransactionIdentity()
	if snapshot.Snapshot == "" || !NotificationIdentityMatches(identity, snapshot.NotificationTransactionIdentity) {
		return q
	}
	// Missing/foreign stamps use the unstamped baseline. A join performed in
	// the source transaction is eligible although that XID is not yet visible.
	sourceXID := "0"
	if NotificationIdentityMatches(identity, source.NotificationTransactionIdentity) {
		sourceXID = notificationXIDOrZero(source.XID)
	}
	return q.Where(`p.notification_join_epoch = 0 OR p.notification_join_xid = '0'::xid8
		OR p.notification_join_epoch <> ?
		OR (p.notification_join_system_identifier <> '' AND ? <> '' AND p.notification_join_system_identifier <> ?)
		OR p.notification_join_xid = ?::xid8
		OR pg_visible_in_snapshot(p.notification_join_xid, ?::pg_snapshot)`, identity.Epoch, identity.SystemIdentifier, identity.SystemIdentifier, sourceXID, snapshot.Snapshot)
}

func notificationXIDOrZero(xid string) string {
	if xid == "" {
		return "0"
	}
	return xid
}

func readChatNotificationRecipients(q *gorm.DB) ([]ChatNotificationRecipient, error) {
	var rows []models.ChatChannelParticipant
	if err := q.Select("p.user_id, p.notification_join_epoch, p.notification_join_system_identifier, p.notification_join_xid").Order("p.user_id").Scan(&rows).Error; err != nil {
		return nil, err
	}
	out := make([]ChatNotificationRecipient, len(rows))
	for i, row := range rows {
		out[i] = ChatNotificationRecipient{RecipientID: row.UserID, MembershipStamp: row.NotificationJoin}
	}
	return out, nil
}

func (h *CommonPersistenceHandler[U]) ListChatNotificationRecipientCandidates(ctx context.Context, channelID, after uint64, limit int, source models.NotificationTransactionStamp, snapshot models.NotificationVisibilitySnapshot) ([]ChatNotificationRecipient, error) {
	if channelID == 0 || limit < 1 || limit > config.MaxNotificationFanoutBatchRecipients {
		return nil, ErrNotificationInput
	}
	return readChatNotificationRecipients(h.chatNotificationAudience(ctx, channelID, source, snapshot).Where("p.user_id > ?", after).Limit(limit))
}

func (h *CommonPersistenceHandler[U]) FilterChatNotificationRecipientCandidates(ctx context.Context, channelID uint64, ids []uint64, source models.NotificationTransactionStamp, snapshot models.NotificationVisibilitySnapshot) ([]ChatNotificationRecipient, error) {
	if channelID == 0 || len(ids) > config.MaxNotificationFanoutBatchRecipients {
		return nil, ErrNotificationInput
	}
	if len(ids) == 0 {
		return nil, nil
	}
	return readChatNotificationRecipients(h.chatNotificationAudience(ctx, channelID, source, snapshot).Where("p.user_id IN ?", ids))
}

// ValidateChatNotificationRecipient is deliberately a single fresh statement
// after the recipient/type lock. Application permission callbacks run earlier.
func (h *CommonPersistenceHandler[U]) ValidateChatNotificationRecipient(ctx context.Context, channelID, messageID, recipientID uint64, observed models.NotificationTransactionStamp, source ChatNotificationSourceState) (bool, error) {
	if channelID == 0 || messageID == 0 || recipientID == 0 {
		return false, ErrNotificationInput
	}
	q := h.DB(ctx).Table(models.TableNameChatChannelParticipants+" p").
		Joins("JOIN "+models.TableNameChatChannels+" c ON c.id = p.channel_id AND c.deleted_at IS NULL").
		Joins("JOIN "+models.TableNameChatMessages+" m ON m.channel_id = c.id AND m.id = ? AND m.deleted_at IS NULL", messageID).
		Joins("LEFT JOIN "+models.TableNameChatMessages+" parent ON parent.id = m.parent_message_id AND parent.channel_id = c.id AND parent.deleted_at IS NULL").
		Joins("JOIN "+models.TableNameUsers+" u ON u.id = p.user_id AND u.deleted_at IS NULL AND NOT u.banned AND NOT u.is_bot").
		Where("p.channel_id = ? AND p.user_id = ? AND p.left_at IS NULL", channelID, recipientID).
		Where("COALESCE(m.parent_message_id, 0) = ? AND COALESCE(m.hidden_from_users, false) = ?", source.ParentMessageID, source.Hidden)
	if source.ParentMessageID != 0 {
		q = q.Where("parent.id IS NOT NULL AND COALESCE(parent.hidden_from_users, false) = ?", source.ParentHidden)
	}
	if source.RequireUnread {
		q = q.Where("c.unread_notifications_enabled = true")
	}
	identity := h.NotificationTransactionIdentity()
	if observed.XID != "" && observed.XID != "0" && NotificationIdentityMatches(identity, observed.NotificationTransactionIdentity) {
		q = q.Where("p.notification_join_epoch = ? AND p.notification_join_xid = ?::xid8", observed.Epoch, observed.XID)
		if observed.SystemIdentifier != "" {
			q = q.Where("p.notification_join_system_identifier = '' OR p.notification_join_system_identifier = ?", observed.SystemIdentifier)
		}
	} else if identity.Epoch != 0 {
		// Absence is itself the observed baseline generation. A new stamped
		// rejoin must not make an old unstamped candidate eligible again.
		q = q.Where(`p.notification_join_epoch = 0 OR p.notification_join_xid = '0'::xid8
			OR p.notification_join_epoch <> ?
			OR (p.notification_join_system_identifier <> '' AND ? <> '' AND p.notification_join_system_identifier <> ?)`, identity.Epoch, identity.SystemIdentifier, identity.SystemIdentifier)
	}
	var count int64
	err := q.Count(&count).Error
	return count == 1, err
}

// createStampedChatParticipants captures one top-level XID inside the INSERT's
// transaction. Plain fixture/custom handlers without bootstrap retain baseline
// stamps; production handlers initialize identity before accepting operations.
func (h *CommonPersistenceHandler[U]) createStampedChatParticipants(db *gorm.DB, participants []models.ChatChannelParticipant, conflict *clause.OnConflict) error {
	create := func(tx *gorm.DB) error {
		if conflict != nil {
			tx = tx.Clauses(*conflict)
		}
		return tx.Create(&participants).Error
	}
	identity := h.NotificationTransactionIdentity()
	if identity.Epoch == 0 {
		return create(db)
	}
	return db.Transaction(func(tx *gorm.DB) error {
		stamp := models.NotificationTransactionStamp{NotificationTransactionIdentity: identity}
		if err := tx.Raw("SELECT pg_current_xact_id()::text").Scan(&stamp.XID).Error; err != nil {
			return err
		}
		for i := range participants {
			participants[i].NotificationJoin = stamp
		}
		return create(tx)
	})
}
