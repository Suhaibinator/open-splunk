package common_persistence

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	models "github.com/Suhaibinator/go-common/pkg/common_persistence/common_persistence_models"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// The process-local cursor holds the last scanned job key so bounded candidate
// batches rotate past busy jobs. Jobs remain authoritative in PostgreSQL;
// resetting it on restart only restarts the scan, and cannot lose work.
type notificationRecipientRetirementClaimCursor struct {
	mu   sync.Mutex
	last models.NotificationRecipientRetirement
}

// recipientRetirementCandidates scans due jobs after the cursor, wrapping to the
// start of the key order, reading at most limit jobs. The zero cursor precedes every job.
func (s *GormNotificationStore[U]) recipientRetirementCandidates(db *gorm.DB, limit int) ([]models.NotificationRecipientRetirement, error) {
	cursor := &s.recipientRetirementClaims
	cursor.mu.Lock()
	defer cursor.mu.Unlock()
	scan := func(op string, limit int) ([]models.NotificationRecipientRetirement, error) {
		var jobs []models.NotificationRecipientRetirement
		last := cursor.last
		err := db.Where("due_at <= now() AND (due_at,activity_scope,user_id,type) "+op+" (?, ?, ?, ?)", last.DueAt, last.ActivityScope, last.UserID, last.Type).
			Order("due_at,activity_scope,user_id,type").Limit(limit).Find(&jobs).Error
		return jobs, err
	}
	candidates, err := scan(">", limit)
	if err == nil && !cursor.last.DueAt.IsZero() && len(candidates) < limit {
		var wrapped []models.NotificationRecipientRetirement
		wrapped, err = scan("<=", limit-len(candidates))
		candidates = append(candidates, wrapped...)
	}
	if err != nil {
		return nil, err
	}
	if len(candidates) > 0 {
		cursor.last = candidates[len(candidates)-1]
	}
	return candidates, nil
}

// Explicit retirement holds recipient locks before writing its continuation.
// Workers use the same order: an unlocked candidate probe precedes the recipient
// lock, then a SKIP LOCKED re-read claims the still-current job. A busy recipient
// is left for a later tick while a rotating bounded scan looks for another runnable job.
func (s *GormNotificationStore[U]) advanceNotificationRecipientRetirement(ctx context.Context, policy NotificationChatRetirementPolicy, publish func(context.Context, []models.Notification) error) (NotificationScopeRetirementAdvance, error) {
	var result NotificationScopeRetirementAdvance
	var job models.NotificationRecipientRetirement
	claimed := false
	stage := "claim"
	err := s.WithTransaction(ctx, func(tx context.Context) error {
		db := s.db.DB(tx)
		if err := db.Exec("SET TRANSACTION ISOLATION LEVEL READ COMMITTED").Error; err != nil {
			return err
		}
		candidates, err := s.recipientRetirementCandidates(db, policy.ClaimCandidates)
		if err != nil {
			return err
		}
		for _, candidate := range candidates {
			// Roll back an unsuccessful probe so its recipient lock cannot
			// obstruct another worker or violate the recipient ordering of the
			// page we eventually claim. PostgreSQL releases locks taken after
			// a savepoint when rolling back to it.
			if err := db.SavePoint("recipient_retirement_candidate").Error; err != nil {
				return err
			}
			pair := NotificationRecipientType{UserID: candidate.UserID, Type: candidate.Type}
			locked, err := tryLockNotificationRecipient(db, pair)
			if err != nil {
				return err
			}
			if locked {
				err = db.Where("activity_scope = ? AND user_id = ? AND type = ? AND due_at <= now()", candidate.ActivityScope, candidate.UserID, candidate.Type).Clauses(clause.Locking{Strength: "UPDATE", Options: "SKIP LOCKED"}).Take(&job).Error
				if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
					return err
				}
				claimed = err == nil
			}
			if !claimed {
				if err := db.RollbackTo("recipient_retirement_candidate").Error; err != nil {
					return err
				}
			}
			if err := db.Exec("RELEASE SAVEPOINT recipient_retirement_candidate").Error; err != nil {
				return err
			}
			if claimed {
				break
			}
		}
		if !claimed {
			return nil
		}
		limit := min(policy.PageSize, policy.MaxRecipients)
		stage = "retire"
		var targets []models.Notification
		if err := db.Select(retirementEntryColumns).Where("activity_scope = ? AND user_id = ? AND type = ? AND reusable_group = true AND id > ?", job.ActivityScope, job.UserID, job.Type, job.AfterNotificationID).Order("id").Limit(limit + 1).Find(&targets).Error; err != nil {
			return err
		}
		more := len(targets) > limit
		if more {
			targets = targets[:limit]
		}
		rows, err := retireNotificationTargets(db, targets, job.Cutoff)
		if err != nil {
			return err
		}
		if publish != nil && len(rows) > 0 {
			stage = "publish"
			if err := publish(tx, rows); err != nil {
				return err
			}
		}
		result.Advanced, result.Recipients = true, 1
		if !more {
			return db.Delete(&job).Error
		}
		updates := struct {
			AfterNotificationID uint64
			Attempts            uint32
			LastError           string
			ExhaustedAt         *time.Time
			DueAt               clause.Expr `gorm:"type:timestamptz"`
			UpdatedAt           time.Time
		}{AfterNotificationID: targets[len(targets)-1].ID, DueAt: gorm.Expr("now()")}
		return db.Model(&job).Select("after_notification_id", "attempts", "last_error", "exhausted_at", "due_at").Updates(&updates).Error
	})
	if err != nil {
		if !claimed || errors.Is(ctx.Err(), context.Canceled) {
			return NotificationScopeRetirementAdvance{}, err
		}
		result = NotificationScopeRetirementAdvance{Stage: stage}
		recordCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), policy.RecordFailureTimeout)
		defer cancel()
		var updated []struct {
			Attempts    uint32
			ExhaustedAt *time.Time
		}
		recordErr := s.WithTransaction(recordCtx, func(tx context.Context) error {
			return s.db.DB(tx).Raw(`UPDATE `+models.NotificationRecipientRetirement{}.TableName()+` SET
			 attempts=attempts+1,last_error=?,updated_at=now(),
			 exhausted_at=CASE WHEN attempts+1>=? THEN now() ELSE NULL END,
			 due_at=now()+CASE WHEN attempts+1>=? THEN make_interval(secs=>?)
			 ELSE make_interval(secs=>least(?::float8,?::float8*power(2,least(attempts,?)))) END
			 WHERE activity_scope=? AND user_id=? AND type=? AND updated_at=? RETURNING attempts,exhausted_at`, stage, policy.MaxAttempts, policy.MaxAttempts, policy.ExhaustedDelay.Seconds(), policy.RetryBackoffMax.Seconds(), policy.RetryBackoffBase.Seconds(), policy.backoffDoublings(), job.ActivityScope, job.UserID, job.Type, job.UpdatedAt).Scan(&updated).Error
		})
		if recordErr != nil {
			return result, errors.Join(err, fmt.Errorf("record recipient retirement failure: %w", recordErr))
		}
		if len(updated) > 0 {
			result.Advanced = true
			result.Attempts = updated[0].Attempts
			result.Exhausted = updated[0].ExhaustedAt != nil
		}
		if result.Exhausted {
			err = fmt.Errorf("%w: %w", ErrNotificationScopeRetirementExhausted, err)
		}
		return result, err
	}
	return result, nil
}
