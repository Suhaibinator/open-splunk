package common_persistence

import (
	"context"
	"strings"

	models "github.com/Suhaibinator/go-common/pkg/common_persistence/common_persistence_models"
	"github.com/Suhaibinator/go-common/pkg/config"
)

// ChatNotificationRecipientCheck retains the authorization inputs for a fresh
// post-lock statement. Results correspond to checks in input order.
type ChatNotificationRecipientCheck struct {
	RecipientID uint64
	Observed    models.NotificationTransactionStamp
	Source      ChatNotificationSourceState
}

// ChatNotificationRecipientBatchPersistence is an optional bulk replacement for
// ValidateChatNotificationRecipient, implemented by the built-in PostgreSQL store.
type ChatNotificationRecipientBatchPersistence interface {
	ValidateChatNotificationRecipients(context.Context, uint64, uint64, []ChatNotificationRecipientCheck) ([]bool, error)
}

func (h *CommonPersistenceHandler[U]) ValidateChatNotificationRecipients(ctx context.Context, channelID, messageID uint64, checks []ChatNotificationRecipientCheck) ([]bool, error) {
	if channelID == 0 || messageID == 0 || len(checks) > config.MaxNotificationFanoutBatchRecipients {
		return nil, ErrNotificationInput
	}
	result := make([]bool, len(checks))
	if len(checks) == 0 {
		return result, nil
	}
	identity := h.NotificationTransactionIdentity()
	var values strings.Builder
	args := make([]any, 0, len(checks)*9)
	for i, check := range checks {
		if check.RecipientID == 0 {
			return nil, ErrNotificationInput
		}
		if i != 0 {
			values.WriteByte(',')
		}
		values.WriteString("(?::integer,?::bigint,?::bigint,?::text,?::xid8,?::boolean,?::bigint,?::boolean,?::boolean,?::boolean)")
		stamped := check.Observed.XID != "" && check.Observed.XID != "0" && NotificationIdentityMatches(identity, check.Observed.NotificationTransactionIdentity)
		args = append(args, i, check.RecipientID, check.Observed.Epoch, check.Observed.SystemIdentifier, notificationXIDOrZero(check.Observed.XID), stamped, check.Source.ParentMessageID, check.Source.Hidden, check.Source.ParentHidden, check.Source.RequireUnread)
	}
	q := h.DB(ctx).Table(models.TableNameChatChannelParticipants+" p").
		Joins("JOIN (VALUES "+values.String()+") AS checked(position,user_id,epoch,system_identifier,xid,stamped,parent_id,hidden,parent_hidden,require_unread) ON checked.user_id=p.user_id", args...).
		Joins("JOIN "+models.TableNameChatChannels+" c ON c.id=p.channel_id AND c.deleted_at IS NULL").
		Joins("JOIN "+models.TableNameChatMessages+" m ON m.channel_id=c.id AND m.id=? AND m.deleted_at IS NULL", messageID).
		Joins("LEFT JOIN "+models.TableNameChatMessages+" parent ON parent.id=m.parent_message_id AND parent.channel_id=c.id AND parent.deleted_at IS NULL").
		Joins("JOIN "+models.TableNameUsers+" u ON u.id=p.user_id AND u.deleted_at IS NULL AND NOT u.banned AND NOT u.is_bot").
		Where("p.channel_id=? AND p.left_at IS NULL", channelID).
		Where("COALESCE(m.parent_message_id,0)=checked.parent_id AND COALESCE(m.hidden_from_users,false)=checked.hidden").
		Where("checked.parent_id=0 OR (parent.id IS NOT NULL AND COALESCE(parent.hidden_from_users,false)=checked.parent_hidden)").
		Where("NOT checked.require_unread OR c.unread_notifications_enabled=true")
	baseline := "true"
	if identity.Epoch != 0 {
		baseline = "p.notification_join_epoch=0 OR p.notification_join_xid='0'::xid8 OR p.notification_join_epoch<>? OR (p.notification_join_system_identifier<>'' AND ?<>'' AND p.notification_join_system_identifier<>?)"
	}
	predicate := "(checked.stamped AND p.notification_join_epoch=checked.epoch AND p.notification_join_xid=checked.xid AND (checked.system_identifier='' OR p.notification_join_system_identifier='' OR p.notification_join_system_identifier=checked.system_identifier)) OR (NOT checked.stamped AND (" + baseline + "))"
	if identity.Epoch == 0 {
		q = q.Where(predicate)
	} else {
		q = q.Where(predicate, identity.Epoch, identity.SystemIdentifier, identity.SystemIdentifier)
	}
	var positions []int
	if err := q.Pluck("checked.position", &positions).Error; err != nil {
		return nil, err
	}
	for _, i := range positions {
		result[i] = true
	}
	return result, nil
}
