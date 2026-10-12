package common_persistence

import (
	"context"
	"strings"

	models "github.com/Suhaibinator/go-common/pkg/common_persistence/common_persistence_models"
	"github.com/Suhaibinator/go-common/pkg/config"
)

type NotificationOrganizationRecipient struct {
	OrganizationID int64
	UserID         uint64
}

func (s *GormNotificationStore[U]) NotificationOrganizationExists(ctx context.Context, organizationID int64) (bool, error) {
	var exists bool
	err := s.db.DB(ctx).Raw("SELECT EXISTS (SELECT 1 FROM gocommon_organizations WHERE id = ? AND deleted_at IS NULL)", organizationID).Scan(&exists).Error
	return exists, err
}

// NotificationOrganizationMembers uses one fresh query per bounded candidate set.
// Positional results preserve duplicate candidates without application user hydration.
func (s *GormNotificationStore[U]) NotificationOrganizationMembers(ctx context.Context, candidates []NotificationOrganizationRecipient) ([]bool, error) {
	if len(candidates) > config.MaxNotificationSourceBatchRows {
		return nil, ErrNotificationInput
	}
	out := make([]bool, len(candidates))
	if len(candidates) == 0 {
		return out, nil
	}
	var values strings.Builder
	args := make([]any, 0, len(candidates)*3)
	for i, c := range candidates {
		if c.OrganizationID <= 0 || c.UserID == 0 {
			return nil, ErrNotificationInput
		}
		if i > 0 {
			values.WriteByte(',')
		}
		values.WriteString("(?::bigint, ?::bigint, ?::bigint)")
		args = append(args, i, c.OrganizationID, c.UserID)
	}
	var positions []int
	err := s.db.DB(ctx).Raw(`WITH candidates(position, organization_id, user_id) AS (VALUES `+values.String()+`)
SELECT c.position FROM candidates c
JOIN gocommon_user_organizations m ON m.organization_id = c.organization_id AND m.user_id = c.user_id
JOIN gocommon_organizations o ON o.id = m.organization_id AND o.deleted_at IS NULL
JOIN gocommon_users u ON u.id = m.user_id AND u.deleted_at IS NULL AND NOT u.banned AND NOT u.is_bot
WHERE m.role > 0`, args...).Scan(&positions).Error
	if err != nil {
		return nil, err
	}
	for _, i := range positions {
		out[i] = true
	}
	return out, nil
}

func (s *GormNotificationStore[U]) NotificationOrganizationMemberPage(ctx context.Context, org int64, after uint64, limit int) ([]uint64, error) {
	if org <= 0 || limit < 1 || limit > config.MaxNotificationFanoutBatchRecipients {
		return nil, ErrNotificationInput
	}
	var ids []uint64
	err := s.db.DB(ctx).Raw(`SELECT m.user_id FROM gocommon_user_organizations m
 JOIN gocommon_organizations o ON o.id=m.organization_id AND o.deleted_at IS NULL
 JOIN gocommon_users u ON u.id=m.user_id AND u.deleted_at IS NULL AND NOT u.banned AND NOT u.is_bot
 WHERE m.organization_id=? AND m.user_id>? AND m.role>0 ORDER BY m.user_id LIMIT ?`, org, after, limit).Scan(&ids).Error
	return ids, err
}

func (s *GormNotificationStore[U]) ResolveOrganizationSource(ctx context.Context, org int64, typeID uint32, source string, pageSize int) ([]models.Notification, error) {
	if org <= 0 || typeID == 0 || source == "" {
		return nil, ErrNotificationInput
	}
	return s.resolveScopedSource(ctx, org, typeID, source, pageSize)
}
