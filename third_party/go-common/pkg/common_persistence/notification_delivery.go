package common_persistence

import (
	"context"
	"database/sql"
	"errors"
	"slices"
	"time"

	models "github.com/Suhaibinator/go-common/pkg/common_persistence/common_persistence_models"
	"github.com/Suhaibinator/go-common/pkg/config"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// Attempt-first ordering matches idx_notification_delivery_recovery, including
// generic prepared plans. Due-time ordering here would let the planner walk the
// healthy pending backlog to prove that no exhausted attempts exist. Ordinary
// delivery claims below retain due-time order. The array of selected IDs keeps
// the UPDATE on primary-key lookups: a generic plan cannot estimate the LIMIT
// and may otherwise hash-join candidates against the entire delivery table.
const notificationDeliveryRecoverySQL = `WITH candidates AS (
 SELECT id FROM gocommon_notification_deliveries WHERE state = 'pending' AND due_at <= ?
 AND (lease_expires_at IS NULL OR lease_expires_at <= ?) AND attempts >= ?
 ORDER BY attempts,due_at,id LIMIT ? FOR UPDATE SKIP LOCKED)
 UPDATE gocommon_notification_deliveries d SET state = 'exhausted', lease_owner = '', lease_token = '', lease_expires_at = NULL,
 updated_at = ? WHERE d.id = ANY(ARRAY(SELECT id FROM candidates))`

// ClaimDeliveries claims a bounded batch without blocking other workers. Each
// claim increments Attempts, fencing a stale completion even if an owner is reused.
// External sending must happen after this transaction commits.
func (s *GormNotificationStore[U]) ClaimDeliveries(ctx context.Context, owner string, now time.Time, lease time.Duration, limit int, maxAttempts uint32) ([]models.NotificationDelivery, error) {
	if owner == "" || lease <= 0 || limit <= 0 || limit > config.MaxNotificationDeliveryWorkerCount || maxAttempts == 0 {
		return nil, ErrNotificationInput
	}
	var jobs []models.NotificationDelivery
	err := s.WithTransaction(ctx, func(ctx context.Context) error {
		db := s.db.DB(ctx)
		// Crashed final attempts become terminal once their lease expires. SKIP
		// LOCKED also applies here so exhausted rows cannot serialize all workers.
		//
		// TODO(multi-instance-v2): leases use the caller's clock. Workers on hosts
		// with skewed clocks could steal live leases; use the database clock then.
		table := models.NotificationDelivery{}.TableName()
		if err := db.Exec(notificationDeliveryRecoverySQL, now, now, maxAttempts, limit, now).Error; err != nil {
			return err
		}
		if err := db.Raw(`WITH candidates AS (
		 SELECT id FROM `+table+` WHERE state = 'pending' AND due_at <= ?
		 AND (lease_expires_at IS NULL OR lease_expires_at <= ?) AND attempts < ?
		 AND NOT `+sourceResolutionPendingSQL(table+".notification_id")+`
		 ORDER BY due_at,id LIMIT ? FOR UPDATE SKIP LOCKED)
		 UPDATE `+table+` d SET attempts = d.attempts + 1, lease_owner = ?, lease_token = gen_random_uuid()::text,
		 lease_expires_at = ?, updated_at = ?
		 FROM candidates c WHERE d.id = c.id RETURNING d.*`, now, now, maxAttempts, limit, owner, now.Add(lease), now).Scan(&jobs).Error; err != nil {
			return err
		}
		slices.SortFunc(jobs, func(a, b models.NotificationDelivery) int {
			if order := a.DueAt.Compare(b.DueAt); order != 0 {
				return order
			}
			if a.ID < b.ID {
				return -1
			}
			if a.ID > b.ID {
				return 1
			}
			return 0
		})
		return nil
	})
	return jobs, err
}

// PrepareDeliverySend makes the final database decision before an external
// send. The inbox check comes after retirement work so it also observes
// acknowledgments committed while retirement was being checked. Lease expiry is
// the caller's clock check, made immediately afterwards.
func (s *GormNotificationStore[U]) PrepareDeliverySend(ctx context.Context, job models.NotificationDelivery, entry models.Notification, policy NotificationDeliverySendPolicy, publish func(context.Context, []models.Notification) error) (bool, error) {
	retired, err := s.retireScopeFencedDelivery(ctx, job, entry, publish)
	if err != nil || retired || job.ID == 0 {
		return retired, err
	}
	table, notifications := models.NotificationDelivery{}.TableName(), models.Notification{}.TableName()
	captured := "d.id = @id AND d.state = @pending AND d.lease_owner = @owner AND d.lease_token = @token AND d.attempts = @attempts AND d.unread_episode_revision = @episode"
	cancel := "(((@read OR n.reusable_group) AND n.read_revision >= n.revision) OR ((@resolution OR n.reusable_group) AND n.resolved))"
	args := []any{sql.Named("id", job.ID), sql.Named("pending", NotificationDeliveryPending), sql.Named("owner", job.LeaseOwner),
		sql.Named("token", job.LeaseToken), sql.Named("attempts", job.Attempts), sql.Named("episode", job.UnreadEpisodeRevision),
		sql.Named("read", policy.CancelOnRead), sql.Named("resolution", policy.CancelOnResolution), sql.Named("skipped", NotificationDeliverySkipped),
		sql.Named("notification", entry.ID), sql.Named("user", entry.UserID), sql.Named("revision", entry.Revision),
		sql.Named("payload_only", policy.EmailContentUsesPayloadOnly), sql.Named("payload", entry.Payload)}
	// Release the captured attempt, refunded, when the policy cancels on the
	// entry's acknowledgment (skipped) or a source resolution queued after the
	// claim holds the send until its settlement finishes (still pending).
	var released []string
	if err := s.db.DB(ctx).Raw(`UPDATE `+table+` d SET state = CASE WHEN `+cancel+` THEN @skipped ELSE d.state END,
		attempts = d.attempts - 1, lease_owner = '', lease_token = '', lease_expires_at = NULL, updated_at = now()
		FROM `+notifications+` n WHERE `+captured+` AND n.id = d.notification_id AND n.user_id = d.user_id
		AND (`+cancel+` OR `+sourceResolutionPendingSQL("d.notification_id")+`) RETURNING d.state`, args...).Scan(&released).Error; err != nil {
		return false, err
	}
	if len(released) > 0 {
		if released[0] != NotificationDeliverySkipped {
			return false, ErrNotificationLeaseLost
		}
		return true, nil
	}
	// A renderable revision change needs a fresh render. GORM ends a named
	// placeholder only at a space, and a []byte right after "(" becomes an IN-list.
	var current struct{ Live, Changed bool }
	if err := s.db.DB(ctx).Raw(`SELECT EXISTS (SELECT 1 FROM `+table+` d WHERE `+captured+`) AS live,
		EXISTS (SELECT 1 FROM `+notifications+` n WHERE n.id = @notification AND n.user_id = @user AND n.revision <> @revision
			AND (NOT n.reusable_group OR NOT @payload_only
				OR coalesce(n.payload, ''::bytea) <> coalesce(CAST( @payload AS bytea), ''::bytea))) AS changed`, args...).Scan(&current).Error; err != nil {
		return false, err
	}
	if !current.Live {
		return false, ErrNotificationLeaseLost
	}
	if current.Changed {
		return false, ErrNotificationDeliveryChanged
	}
	return false, nil
}

// FinishDelivery requires the exact claimed attempt and an unexpired lease.
func (s *GormNotificationStore[U]) FinishDelivery(ctx context.Context, job models.NotificationDelivery, state, lastError string, nextDue, now time.Time) error {
	switch state {
	case NotificationDeliveryPending, NotificationDeliveryDeferred, NotificationDeliveryWaiting, NotificationDeliverySuppressed, NotificationDeliveryDisabled, NotificationDeliverySent, NotificationDeliverySkipped, NotificationDeliveryExhausted:
	default:
		return ErrNotificationInput
	}
	if job.ID == 0 || job.LeaseOwner == "" || job.Attempts == 0 || job.LeaseToken == "" {
		return ErrNotificationInput
	}
	if state == NotificationDeliveryWaiting || state == NotificationDeliverySuppressed || state == NotificationDeliveryDisabled {
		return s.finishDormantDelivery(ctx, job, state, nextDue.Sub(now), now)
	}
	updates := models.NotificationDelivery{State: state, LastError: lastError, DueAt: nextDue}
	columns := []string{"waiting_tag", "state", "last_error", "due_at", "lease_owner", "lease_token", "lease_expires_at"}
	// Active-scope deferral is not a provider attempt. Preserve the delivery's
	// retry budget; the fresh lease token still fences earlier completions.
	if state == NotificationDeliveryDeferred {
		updates.State = NotificationDeliveryPending
		updates.Attempts = job.Attempts - 1
		columns = append(columns, "attempts")
	}
	res := deliveryCompletionQuery(s.db.DB(ctx), job, now).Select(columns).Updates(&updates)
	if res.Error != nil {
		return res.Error
	}
	if res.RowsAffected != 1 {
		return ErrNotificationLeaseLost
	}
	s.queueChanged(ctx, NotificationQueueDelivery)
	return nil
}

// RetryDelivery only requeues terminal exhausted deliveries owned by this user.
func (s *GormNotificationStore[U]) RetryDelivery(ctx context.Context, userID, notificationID uint64, now time.Time) (bool, error) {
	res := s.db.DB(ctx).Model(&models.NotificationDelivery{}).Where("user_id = ? AND notification_id = ? AND state = ?", userID, notificationID, NotificationDeliveryExhausted).Select("state", "attempts", "due_at", "last_error", "lease_owner", "lease_token", "lease_expires_at").Updates(&models.NotificationDelivery{State: NotificationDeliveryPending, DueAt: now})
	if res.Error == nil && res.RowsAffected == 1 {
		s.queueChanged(ctx, NotificationQueueDelivery)
	}
	return res.RowsAffected == 1, res.Error
}

// GetDeliveryOccurrence uses the partial (notification, tag, revision) index,
// returning at most one row regardless of channel history size. Its fixed
// predicate remains explicit so PostgreSQL can use it with generic query plans.
func (s *GormNotificationStore[U]) GetDeliveryOccurrence(ctx context.Context, userID, notificationID uint64, tag string, afterRevision, throughRevision uint64, latest bool) (*models.NotificationOccurrence, error) {
	if userID == 0 || notificationID == 0 || tag == "" || !validDeliveryTag(tag) {
		return nil, ErrNotificationInput
	}
	order := "revision ASC"
	if latest {
		order = "revision DESC"
	}
	var occurrence models.NotificationOccurrence
	err := s.db.DB(ctx).Where("notification_id = ? AND user_id = ? AND delivery_tag = ? AND delivery_tag <> '' AND disposition = 'materialized' AND revision > ? AND revision <= ?", notificationID, userID, tag, afterRevision, throughRevision).Order(order).Take(&occurrence).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &occurrence, nil
}

// finishDormantDelivery serializes a temporary delivery suppression with recording.
// RecordMany locks the inbox entry before touching its delivery, so qualifying
// activity either appears in this recheck or sees the dormant state and re-arms
// it. No provider call is made while these locks are held.
func (s *GormNotificationStore[U]) finishDormantDelivery(ctx context.Context, job models.NotificationDelivery, state string, delay time.Duration, now time.Time) error {
	if state == NotificationDeliveryWaiting && (job.WaitingTag == "" || !validDeliveryTag(job.WaitingTag) || delay < 0) {
		return ErrNotificationInput
	}
	err := s.WithTransaction(ctx, func(ctx context.Context) error {
		db := s.db.DB(ctx)
		var n models.Notification
		if err := db.Where("id = ? AND user_id = ?", job.NotificationID, job.UserID).Clauses(clause.Locking{Strength: "UPDATE"}).Take(&n).Error; err != nil {
			return err
		}
		if n.UnreadEpisodeRevision != job.UnreadEpisodeRevision {
			return ErrNotificationLeaseLost
		}
		tag, due := "", now
		if state == NotificationDeliveryWaiting {
			tag = job.WaitingTag
		}
		if n.Resolved || n.ReadRevision >= n.Revision {
			state, tag = NotificationDeliverySkipped, ""
		} else {
			after := n.UnreadOccurrenceFloor()
			var occurrence *models.NotificationOccurrence
			if state == NotificationDeliveryWaiting {
				var err error
				occurrence, err = s.GetDeliveryOccurrence(ctx, n.UserID, n.ID, tag, after, n.Revision, false)
				if err != nil {
					return err
				}
			}
			if occurrence != nil {
				state, tag, due = NotificationDeliveryPending, "", occurrence.MaterializedAt.Add(delay)
			} else if n.DeliveryRearmRevision > max(job.WaitingRevision, after) {
				// Only activity that qualified when materialized can request a
				// preference recheck. Ordinary mentions-only traffic must not keep
				// a waiting delivery active while the worker releases its lease.
				state, tag, due = NotificationDeliveryPending, "", now
			}
		}
		updates := models.NotificationDelivery{State: state, WaitingTag: tag, DueAt: due, Attempts: job.Attempts - 1}
		res := deliveryCompletionQuery(db, job, now).Select("state", "waiting_tag", "due_at", "attempts", "last_error", "lease_owner", "lease_token", "lease_expires_at").Updates(&updates)
		if res.Error != nil {
			return res.Error
		}
		if res.RowsAffected != 1 {
			return ErrNotificationLeaseLost
		}
		return nil
	})
	if err == nil {
		s.queueChanged(ctx, NotificationQueueDelivery)
	}
	return err
}

func deliveryCompletionQuery(db *gorm.DB, job models.NotificationDelivery, now time.Time) *gorm.DB {
	return db.Model(&models.NotificationDelivery{}).Where("id = ? AND state = ? AND lease_owner = ? AND lease_token = ? AND attempts = ? AND unread_episode_revision = ? AND lease_expires_at > ?", job.ID, NotificationDeliveryPending, job.LeaseOwner, job.LeaseToken, job.Attempts, job.UnreadEpisodeRevision, now)
}
