package common_persistence

import (
	"context"
	"fmt"

	models "github.com/Suhaibinator/go-common/pkg/common_persistence/common_persistence_models"
	"github.com/Suhaibinator/go-common/pkg/config"
	"gorm.io/gorm"
)

// RetireActivityScope retires an activity scope. A nonzero userID retires that
// recipient's first bounded entry page immediately, capped like
// RetireActivityScopeForRecipients. A zero userID retires every recipient of
// the scope asynchronously and without a size limit: it commits a permanent
// scope fence and a paging job in the caller's transaction (see
// EnqueueNotificationScopeRetirement) and returns no rows; job pages publish
// the entries they retire.
func (s *GormNotificationStore[U]) RetireActivityScope(ctx context.Context, userID uint64, scope string, maxRecipients int) ([]models.Notification, error) {
	if scope == "" || !validRetirementMaxRecipients(maxRecipients) {
		return nil, ErrNotificationInput
	}
	if userID == 0 {
		return nil, s.EnqueueNotificationScopeRetirement(ctx, scope)
	}
	var targets []models.Notification
	// One pair past the cap is enough to reject the call without reading an
	// unbounded set of types.
	if err := s.db.DB(ctx).Where("reusable_group = true AND activity_scope = ? AND user_id = ?", scope, userID).
		Distinct("user_id", "type").Limit(maxRecipients + 1).Find(&targets).Error; err != nil {
		return nil, err
	}
	recipients := make([]NotificationRecipientType, 0, len(targets))
	for _, target := range targets {
		recipients = append(recipients, NotificationRecipientType{UserID: target.UserID, Type: target.Type})
	}
	return s.RetireActivityScopeForRecipients(ctx, scope, recipients, maxRecipients)
}

// RetireActivityScopeForRecipients immediately fences and starts retiring a known audience of
// at most maxRecipients unique recipient/type pairs. It takes the same
// recipient/type locks as materialization, even for an empty inbox, then reads
// entries afresh, and writes per-recipient cutoffs that fence async entries not
// yet materialized. Source lifecycle callers pass the complete affected
// audience, including departed members. Retiring a whole scope whose audience
// may exceed the cap uses RetireActivityScope with a zero userID instead.
// Retirement never locks emission rows.
// Entry work is independently bounded at 1,000 identities; remaining entries
// are committed as durable continuations, and returned rows are first-page
// invalidation identities only (not hydrated payloads).
func (s *GormNotificationStore[U]) RetireActivityScopeForRecipients(ctx context.Context, scope string, recipients []NotificationRecipientType, maxRecipients int) ([]models.Notification, error) {
	return s.retireActivityScopeForRecipients(ctx, scope, recipients, maxRecipients, true)
}

// RetireChatActivityScopeForRecipients relies on native chat's membership
// generations and fresh source checks. Generic retirement cutoffs are neither
// needed nor consulted by native chat materialization.
func (s *GormNotificationStore[U]) RetireChatActivityScopeForRecipients(ctx context.Context, scope string, recipients []NotificationRecipientType, maxRecipients int) ([]models.Notification, error) {
	return s.retireActivityScopeForRecipients(ctx, scope, recipients, maxRecipients, false)
}

func validRetirementMaxRecipients(maxRecipients int) bool {
	return maxRecipients >= config.MinNotificationRetirementMaxRecipients && maxRecipients <= config.MaxNotificationRetirementMaxRecipients
}

func (s *GormNotificationStore[U]) retireActivityScopeForRecipients(ctx context.Context, scope string, recipients []NotificationRecipientType, maxRecipients int, writeCutoffs bool) ([]models.Notification, error) {
	if scope == "" || len(scope) > config.MaxNotificationRetirementScopeBytes || !validRetirementMaxRecipients(maxRecipients) {
		return nil, ErrNotificationInput
	}
	for _, recipient := range recipients {
		if recipient.UserID == 0 || recipient.Type == 0 {
			return nil, ErrNotificationInput
		}
	}
	// Deduplicate once: every statement below binds a constant number of array
	// parameters, so no audience size can reach PostgreSQL's parameter limit and
	// the whole retirement stays one transaction.
	recipients = uniqueNotificationRecipients(recipients)
	if len(recipients) == 0 {
		return nil, nil
	}
	// Each unique pair holds an advisory lock until commit in the lock table
	// every session shares; reject before taking the first one.
	if len(recipients) > maxRecipients {
		return nil, fmt.Errorf("%w: %d unique recipient/type pairs exceed notification_retirement_max_recipients (%d); page the audience across transactions", ErrNotificationInput, len(recipients), maxRecipients)
	}
	var changed []models.Notification
	err := s.WithTransaction(ctx, func(ctx context.Context) error {
		db := s.db.DB(ctx)
		if err := lockNotificationRecipients(db, recipients); err != nil {
			return err
		}
		cutoff, err := s.CaptureNotificationSnapshot(ctx)
		if err != nil {
			return err
		}
		changed, err = retireLockedRecipients(db, scope, recipients, cutoff, writeCutoffs)
		return err
	})
	if err == nil {
		s.queueChanged(ctx, NotificationQueueRetirement, NotificationQueueDelivery)
	}
	return changed, err
}

// retireLockedRecipients retires the reusable entries of recipients (unique and
// sorted, all already locked by the caller) in scope and skips their unsent
// deliveries in a bounded identity-only page. The cutoff is first written as each recipient's retirement
// cutoff unless an equal or newer one exists. Cutoff and group-state writes
// precede notification row locks, as in every retirement path.
func retireLockedRecipients(db *gorm.DB, scope string, recipients []NotificationRecipientType, cutoff models.NotificationVisibilitySnapshot, writeCutoffs bool) ([]models.Notification, error) {
	if len(recipients) == 0 {
		return nil, nil
	}
	userIDs, types := notificationRecipientArrays(recipients)
	if writeCutoffs {
		if err := upsertNotificationRetirementCutoffs(db, scope, recipients, cutoff); err != nil {
			return nil, err
		}
	}
	const limit = config.MaxNotificationChatRetirementPageSize
	var targets []models.Notification
	query := db.Select(retirementEntryColumns).Where("reusable_group = true AND activity_scope = ?", scope)
	if len(recipients) == 1 {
		// Exact leading index keys prevent sorting an arbitrarily large
		// single-recipient history before applying the entry limit.
		query = query.Where("user_id=? AND type=?", recipients[0].UserID, recipients[0].Type)
	} else {
		query = query.Where("(user_id,type) IN (SELECT * FROM unnest(?::bigint[],?::bigint[]))", userIDs, types)
	}
	if err := query.Order("user_id,type,id").Limit(limit + 1).Find(&targets).Error; err != nil {
		return nil, err
	}
	if len(targets) > limit {
		targets = targets[:limit]
		last := targets[len(targets)-1]
		var remaining []NotificationRecipientType
		// One indexed existence probe per capped pair, never DISTINCT over
		// all remaining entries of an arbitrarily large recipient history.
		if err := db.Raw("SELECT r.user_id,r.type FROM unnest(?::bigint[],?::bigint[]) r(user_id,type) WHERE (r.user_id,r.type)>=(?::bigint,?::bigint) AND EXISTS (SELECT 1 FROM "+models.Notification{}.TableName()+" n WHERE n.activity_scope=? AND n.reusable_group=true AND n.user_id=r.user_id AND n.type=r.type AND (n.user_id,n.type,n.id)>(?::bigint,?::bigint,?::bigint)) ORDER BY r.user_id,r.type", userIDs, types, last.UserID, last.Type, scope, last.UserID, last.Type, last.ID).Scan(&remaining).Error; err != nil {
			return nil, err
		}
		// Source fencing is immediate; the continuation is the separate
		// durable proof that settlement has not completed for these pairs.
		if len(remaining) > 0 && remaining[0] == (NotificationRecipientType{UserID: last.UserID, Type: last.Type}) {
			if err := enqueueNotificationRecipientRetirements(db, scope, remaining[:1], cutoff, last.ID); err != nil {
				return nil, err
			}
			remaining = remaining[1:]
		}
		if err := enqueueNotificationRecipientRetirements(db, scope, remaining, cutoff, 0); err != nil {
			return nil, err
		}
		if err := db.Where("activity_scope=? AND (user_id,type)<(?::bigint,?::bigint) AND (user_id,type) IN (SELECT * FROM unnest(?::bigint[],?::bigint[]))", scope, last.UserID, last.Type, userIDs, types).Delete(&models.NotificationRecipientRetirement{}).Error; err != nil {
			return nil, err
		}
	} else {
		if err := db.Where("activity_scope = ? AND (user_id,type) IN (SELECT * FROM unnest(?::bigint[],?::bigint[]))", scope, userIDs, types).Delete(&models.NotificationRecipientRetirement{}).Error; err != nil {
			return nil, err
		}
	}
	return retireNotificationTargets(db, targets, cutoff)
}
