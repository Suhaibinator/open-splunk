package common_persistence

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/Suhaibinator/SRouter/pkg/scontext"
	models "github.com/Suhaibinator/go-common/pkg/common_persistence/common_persistence_models"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// sourceResolutionPendingSQL holds while a committed resolution job still
// covers the entry's latest live occurrence, matching settlement's own
// latest-source check. Send paths treat it like resolved until settlement.
func sourceResolutionPendingSQL(notificationID string) string {
	return `EXISTS (SELECT 1 FROM ` + models.NotificationSourceResolution{}.TableName() + ` r
	 WHERE ` + notificationID + ` > r.after_id AND ` + notificationID + ` <= r.through_id
	 AND (r.organization_id, r.type, r.source_key) = (SELECT o.organization_id, o.type, o.source_event_key
	  FROM ` + models.NotificationOccurrence{}.TableName() + ` o WHERE o.notification_id = ` + notificationID + `
	  AND o.disposition <> '` + NotificationOccurrenceAcknowledged + `' ORDER BY o.revision DESC LIMIT 1))`
}

type NotificationSourceResolutionAdvance struct {
	Advanced   bool
	Recipients int
	Stage      string
	Attempts   uint32
	Exhausted  bool
}

func (s *GormNotificationStore[U]) AdvanceNotificationSourceResolution(ctx context.Context, policy NotificationChatRetirementPolicy, publish func(context.Context, []models.Notification) error) (NotificationSourceResolutionAdvance, error) {
	var result NotificationSourceResolutionAdvance
	if ambient, ok := scontext.GetTransaction(ctx); ok && ambient != nil {
		return result, ErrNotificationInput
	}
	if !policy.valid() || !validSourceResolutionPageSize(policy.SourceResolutionPageSize) {
		return result, ErrNotificationInput
	}
	var job models.NotificationSourceResolution
	claimed := false
	stage := "claim"
	err := s.WithTransaction(ctx, func(tx context.Context) error {
		db := s.db.DB(tx)
		// Settlement must observe activity that commits while advisory locks
		// are being acquired, regardless of the pool's default isolation.
		if err := db.Exec("SET TRANSACTION ISOLATION LEVEL READ COMMITTED").Error; err != nil {
			return err
		}
		if err := db.Where("due_at <= now()").Order("due_at,id").Clauses(clause.Locking{Strength: "UPDATE", Options: "SKIP LOCKED"}).Take(&job).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return nil
			}
			return err
		}
		claimed = true
		stage = "recipients"
		// One page size bounds both this cursor read and the settlement below,
		// so settlement never truncates IDs the cursor then advances past.
		limit := min(policy.SourceResolutionPageSize, policy.PageSize, policy.MaxRecipients)
		var ids []uint64
		if err := db.Model(&models.NotificationOccurrence{}).Distinct("notification_id").Where("organization_id = ? AND type = ? AND source_event_key = ? AND disposition <> ? AND notification_id > ? AND notification_id <= ?", job.OrganizationID, job.Type, job.SourceKey, NotificationOccurrenceAcknowledged, job.AfterID, job.ThroughID).Order("notification_id").Limit(limit).Pluck("notification_id", &ids).Error; err != nil {
			return err
		}
		result.Recipients = len(ids)
		if len(ids) == 0 {
			return db.Delete(&job).Error
		}
		stage = "resolve"
		last := ids[len(ids)-1]
		rows, err := s.resolveScopedSourcePage(tx, job.OrganizationID, job.Type, job.SourceKey, job.AfterID, last, limit, false)
		if err != nil {
			return err
		}
		stage = "publish"
		if publish != nil && len(rows) > 0 {
			if err := publish(tx, rows); err != nil {
				return err
			}
		}
		stage = "progress"
		if len(ids) < limit {
			return db.Delete(&job).Error
		}
		updates := struct {
			AfterID     uint64
			Attempts    uint32
			LastError   string
			ExhaustedAt *time.Time
			DueAt       clause.Expr `gorm:"type:timestamptz"`
			UpdatedAt   time.Time
		}{AfterID: last, DueAt: gorm.Expr("now()")}
		return db.Model(&job).Select("after_id", "attempts", "last_error", "exhausted_at", "due_at").Updates(&updates).Error
	})
	if err == nil {
		if claimed {
			s.queueChanged(ctx, NotificationQueueDelivery)
		}
		result.Advanced = claimed
		return result, nil
	}
	result.Recipients = 0
	if !claimed || errors.Is(ctx.Err(), context.Canceled) {
		return result, err
	}
	result.Stage = stage
	attempts, exhausted, recordErr := recordRetirementFailure(ctx, s, policy, models.NotificationSourceResolution{}.TableName(), "id", job.ID, job.UpdatedAt, stage)
	if recordErr != nil {
		return result, errors.Join(err, fmt.Errorf("record source resolution failure: %w", recordErr))
	}
	result.Advanced, result.Attempts, result.Exhausted = true, attempts, exhausted
	return result, err
}
