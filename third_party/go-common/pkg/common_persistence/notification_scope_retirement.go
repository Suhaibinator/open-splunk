package common_persistence

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/Suhaibinator/SRouter/pkg/scontext"
	models "github.com/Suhaibinator/go-common/pkg/common_persistence/common_persistence_models"
	"github.com/Suhaibinator/go-common/pkg/common_service/common_service_models"
	"github.com/Suhaibinator/go-common/pkg/common_util/notificationkey"
	"github.com/Suhaibinator/go-common/pkg/config"
	"github.com/lib/pq"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// Scope-wide retirement.
//
// EnqueueNotificationScopeRetirement runs in the source transaction. It takes
// the scope's exclusive advisory lock, captures a snapshot, and writes a
// permanent fence plus a paging job; it takes no recipient lock and has no size
// limit. Materialization of a fenced scope holds the same key in shared mode
// from before its recipient locks until commit, so the enqueue waits for every
// in-flight materialization of the scope to commit (its entries are then
// visible to the job), and every later one waits for the source to commit and
// then sees the fence.
//
// A recipient/type is retired "for" a fence once its per-recipient retirement
// cutoff for the scope is at least as new as the fence. Materialization retires
// such a recipient lazily, under its recipient lock, before recording anything
// for it; job pages retire the rest in (user, type) order and skip recipients
// already retired. Because every materialization after the fence retires its
// recipient first, a recipient without a cutoff as new as the fence has only
// pre-fence entries, and a page never resolves post-fence activity. The
// per-recipient cutoff then carries the fence snapshot, so the ordinary
// retirement fence suppresses pre-fence sources, including async emissions
// that have not materialized yet.

// NotificationScopeRetirementAdvance describes one advance call, like
// NotificationChatRetirementAdvance. Recipients counts the recipient/type pairs
// the page visited; Stage is a bounded label.
type NotificationScopeRetirementAdvance struct {
	Advanced   bool
	Recipients int
	Stage      string
	Attempts   uint32
	Exhausted  bool
}

// ErrNotificationScopeRetirementExhausted wraps a page failure that exhausted its job.
var ErrNotificationScopeRetirementExhausted = errors.New("notification scope retirement exhausted")

func notificationScopeLockKeys(scopes []string) pq.StringArray {
	scopes = slices.Clone(scopes)
	slices.Sort(scopes)
	scopes = slices.Compact(scopes)
	keys := make(pq.StringArray, len(scopes))
	for i, scope := range scopes {
		keys[i] = "notification-scope:" + scope
	}
	return keys
}

// snapshotAtLeastSQL orders visibility snapshots at tied epoch/xmax by their
// in-progress sets. An earlier snapshot can still hide a transaction committed
// in the later one, even when no new transaction ID has been allocated.
func snapshotAtLeastSQL(left, right string) string {
	return "((" + left + "cutoff_epoch," + left + "cutoff_upper_bound) > (" + right + "cutoff_epoch," + right + "cutoff_upper_bound) OR ((" + left + "cutoff_epoch," + left + "cutoff_upper_bound) = (" + right + "cutoff_epoch," + right + "cutoff_upper_bound) AND (" + left + "cutoff_own_xid <> " + right + "cutoff_own_xid OR " + right + "cutoff_own_xid = '0'::xid8 OR " + left + "cutoff_own_ordinal >= " + right + "cutoff_own_ordinal) AND NOT EXISTS (SELECT xid FROM pg_snapshot_xip(" + left + "cutoff_snapshot::pg_snapshot) xid EXCEPT SELECT xid FROM pg_snapshot_xip(" + right + "cutoff_snapshot::pg_snapshot) xid)))"
}

// lockNotificationScopesShared precedes recipient locks in materialization.
// Shared holders never block each other; only a scope-wide enqueue does.
func lockNotificationScopesShared(db *gorm.DB, scopes []string) error {
	if len(scopes) == 0 {
		return nil
	}
	return db.Exec("SELECT pg_advisory_xact_lock_shared(hashtextextended(lock_key, 0)) FROM unnest(?::text[]) WITH ORDINALITY AS scopes(lock_key,position) ORDER BY position", notificationScopeLockKeys(scopes)).Error
}

// EnqueueNotificationScopeRetirement commits a scope's fence and paging job
// with the ambient source transaction (or its own when there is none). A
// pending job for the scope restarts its sweep under the newer fence.
func (s *GormNotificationStore[U]) EnqueueNotificationScopeRetirement(ctx context.Context, scope string) error {
	return s.EnqueueNotificationScopeRetirements(ctx, []string{scope}, 1)
}

// EnqueueNotificationScopeRetirements admits a bounded collection in canonical
// scope order. Call it before ANY notification recipient/type locks in the
// source transaction, even when earlier notifications name a different scope.
// All scopes an operation retires must be supplied in this one admission call.
// PostgreSQL deadlock failures still require retrying the whole source operation.
func (s *GormNotificationStore[U]) EnqueueNotificationScopeRetirements(ctx context.Context, scopes []string, maxScopes int) error {
	if maxScopes < 1 || maxScopes > config.MaxNotificationRetirementMaxRecipients {
		return ErrNotificationInput
	}
	scopes = slices.Clone(scopes)
	for _, scope := range scopes {
		if !notificationkey.ValidActivityScope(scope) {
			return ErrNotificationInput
		}
	}
	slices.Sort(scopes)
	scopes = slices.Compact(scopes)
	if len(scopes) > maxScopes {
		return ErrNotificationInput
	}
	if len(scopes) == 0 {
		return nil
	}
	return s.WithTransaction(ctx, func(tx context.Context) error {
		if err := s.db.DB(tx).Exec("SELECT pg_advisory_xact_lock(hashtextextended(lock_key,0)) FROM unnest(?::text[]) WITH ORDINALITY AS scopes(lock_key,position) ORDER BY position", notificationScopeLockKeys(scopes)).Error; err != nil {
			return err
		}
		for _, scope := range scopes {
			if err := s.enqueueLockedNotificationScopeRetirement(tx, scope); err != nil {
				return err
			}
		}
		return nil
	})
}

func (s *GormNotificationStore[U]) enqueueLockedNotificationScopeRetirement(ctx context.Context, scope string) error {
	if !notificationkey.ValidActivityScope(scope) {
		return ErrNotificationInput
	}
	err := s.WithTransaction(ctx, func(ctx context.Context) error {
		db := s.db.DB(ctx)
		if err := db.Exec("SELECT pg_advisory_xact_lock(hashtextextended(?, 0))", notificationScopeLockKeys([]string{scope})[0]).Error; err != nil {
			return err
		}
		// A separate statement after the lock observes every materialization
		// that held the scope's shared lock.
		snapshot, err := s.CaptureNotificationSnapshot(ctx)
		if err != nil {
			return err
		}
		fences := models.NotificationScopeRetirementFence{}.TableName()
		if err := db.Exec("INSERT INTO "+fences+
			" (activity_scope,cutoff_epoch,cutoff_system_identifier,cutoff_snapshot,cutoff_upper_bound,cutoff_own_xid,cutoff_own_ordinal,created_at,updated_at)"+
			" VALUES (?,?,?,?,?::xid8,?::xid8,?::bigint,now(),now())"+
			" ON CONFLICT (activity_scope) DO UPDATE SET cutoff_epoch = excluded.cutoff_epoch, cutoff_system_identifier = excluded.cutoff_system_identifier,"+
			" cutoff_snapshot = excluded.cutoff_snapshot, cutoff_upper_bound = excluded.cutoff_upper_bound,cutoff_own_xid=excluded.cutoff_own_xid,cutoff_own_ordinal=excluded.cutoff_own_ordinal, updated_at = now()",
			scope, snapshot.Epoch, snapshot.SystemIdentifier, snapshot.Snapshot, snapshot.UpperBound, snapshot.OwnXID, snapshot.OwnOrdinal).Error; err != nil {
			return err
		}
		// A worker holding the job row blocks this upsert only for its current
		// page, whose cursor update is then overwritten by the restarted sweep.
		return db.Exec("INSERT INTO "+models.NotificationScopeRetirement{}.TableName()+
			" (activity_scope,after_user_id,after_type,after_notification_id,attempts,last_error,exhausted_at,due_at,created_at,updated_at)"+
			" VALUES (?,0,0,0,0,'',NULL,now(),now(),now())"+
			" ON CONFLICT (activity_scope) DO UPDATE SET after_user_id = 0, after_type = 0, after_notification_id = 0, attempts = 0, last_error = '',"+
			" exhausted_at = NULL, due_at = now(), updated_at = now()", scope).Error
	})
	if err == nil {
		s.queueChanged(ctx, NotificationQueueRetirement)
	}
	return err
}

func unretiredRecipients(db *gorm.DB, scope string, recipients []NotificationRecipientType, fence models.NotificationVisibilitySnapshot) ([]NotificationRecipientType, error) {
	if len(recipients) == 0 {
		return nil, nil
	}
	userIDs, types := notificationRecipientArrays(recipients)
	var retired []NotificationRecipientType
	if err := db.Raw("SELECT c.user_id, c.type FROM "+models.NotificationRetirementCutoff{}.TableName()+" c CROSS JOIN (SELECT ?::bigint cutoff_epoch, ?::xid8 cutoff_upper_bound, ?::text cutoff_snapshot,?::xid8 cutoff_own_xid,?::bigint cutoff_own_ordinal) f"+
		" WHERE activity_scope = ? AND (user_id,type) IN (SELECT * FROM unnest(?::bigint[],?::bigint[]))"+
		" AND "+snapshotAtLeastSQL("c.", "f.")+
		" AND NOT EXISTS (SELECT 1 FROM "+models.NotificationRecipientRetirement{}.TableName()+" p WHERE p.activity_scope = c.activity_scope AND p.user_id = c.user_id AND p.type = c.type)",
		fence.Epoch, fence.UpperBound, fence.Snapshot, fence.OwnXID, fence.OwnOrdinal, scope, userIDs, types).Scan(&retired).Error; err != nil {
		return nil, err
	}
	if len(retired) == 0 {
		return recipients, nil
	}
	done := make(map[NotificationRecipientType]struct{}, len(retired))
	for _, recipient := range retired {
		done[recipient] = struct{}{}
	}
	pending := make([]NotificationRecipientType, 0, len(recipients)-len(retired))
	for _, recipient := range recipients {
		if _, ok := done[recipient]; !ok {
			pending = append(pending, recipient)
		}
	}
	return pending, nil
}

// fencedScopes lists the distinct activity scopes of inputs the generic
// retirement fence applies to.
func fencedScopes(inputs []NotificationRecordInput) []string {
	scopes := make([]string, 0, 1)
	for _, input := range inputs {
		if input.ActivityScope != "" && !input.SkipRetirementFence {
			scopes = append(scopes, input.ActivityScope)
		}
	}
	slices.Sort(scopes)
	return slices.Compact(scopes)
}

// applyScopeRetirementFences settles only the existing groups named by inputs,
// before anything is recorded, for unfinished scope or recipient fences. The caller
// holds the scopes' shared locks and all input recipient locks. Retired rows
// are returned at the index of the first input for their recipient and scope.
func applyScopeRetirementFences(db *gorm.DB, inputs []NotificationRecordInput, scopes []string) ([][]models.Notification, error) {
	var fences []models.NotificationScopeRetirementFence
	if len(scopes) > 0 {
		if err := db.Where("activity_scope = ANY(?)", pq.StringArray(scopes)).Order("activity_scope").Find(&fences).Error; err != nil {
			return nil, err
		}
	}
	type scopedRecipient struct {
		scope     string
		recipient NotificationRecipientType
	}
	first := make(map[scopedRecipient]int, len(inputs))
	byScope := make(map[string][]NotificationRecipientType, len(fences))
	for i, input := range inputs {
		if input.ActivityScope == "" || input.SkipRetirementFence {
			continue
		}
		key := scopedRecipient{input.ActivityScope, NotificationRecipientType{UserID: input.UserID, Type: input.Type}}
		if _, seen := first[key]; !seen {
			first[key] = i
			byScope[input.ActivityScope] = append(byScope[input.ActivityScope], key.recipient)
		}
	}
	var retired [][]models.Notification
	for _, fence := range fences {
		pending, err := unretiredRecipients(db, fence.ActivityScope, uniqueNotificationRecipients(byScope[fence.ActivityScope]), fence.Cutoff)
		if err != nil {
			return nil, err
		}
		query := db.Where("false")
		for _, input := range inputs {
			if input.ActivityScope == fence.ActivityScope && input.ReusableGroup && !input.SkipRetirementFence && slices.Contains(pending, NotificationRecipientType{UserID: input.UserID, Type: input.Type}) {
				query = query.Or("organization_id = ? AND user_id = ? AND type = ? AND group_key = ? AND reusable_group = true AND activity_scope = ?", input.OrganizationID, input.UserID, input.Type, input.GroupKey, input.ActivityScope)
			}
		}
		var targets []models.Notification
		if err := query.Select(retirementEntryColumns).Order("id").Find(&targets).Error; err != nil {
			return nil, err
		}
		rows, err := retireNotificationTargets(db, targets, fence.Cutoff)
		if err != nil {
			return nil, err
		}
		if len(rows) > 0 && retired == nil {
			retired = make([][]models.Notification, len(inputs))
		}
		for _, row := range rows {
			index := first[scopedRecipient{fence.ActivityScope, NotificationRecipientType{UserID: row.UserID, Type: row.Type}}]
			retired[index] = append(retired[index], row)
		}
	}
	// Explicit retirement can be incomplete without a scope-wide fence. Only
	// the groups these inputs touch need immediate settlement; every remaining
	// group stays in its durable recipient continuation.
	var reusableScopes pq.StringArray
	var recipients []NotificationRecipientType
	for _, input := range inputs {
		if input.ReusableGroup && input.ActivityScope != "" {
			reusableScopes = append(reusableScopes, input.ActivityScope)
			recipients = append(recipients, NotificationRecipientType{UserID: input.UserID, Type: input.Type})
		}
	}
	if len(reusableScopes) == 0 {
		return retired, nil
	}
	userIDs, types := notificationRecipientArrays(recipients)
	var jobs []models.NotificationRecipientRetirement
	if err := db.Where("(activity_scope, user_id, type) IN (SELECT * FROM unnest(?::text[], ?::bigint[], ?::bigint[]))", reusableScopes, userIDs, types).Find(&jobs).Error; err != nil {
		return nil, err
	}
	continuations := make(map[scopedRecipient]models.NotificationVisibilitySnapshot, len(jobs))
	for _, job := range jobs {
		continuations[scopedRecipient{job.ActivityScope, NotificationRecipientType{UserID: job.UserID, Type: job.Type}}] = job.Cutoff
	}
	for index, input := range inputs {
		if !input.ReusableGroup {
			continue
		}
		key := scopedRecipient{input.ActivityScope, NotificationRecipientType{UserID: input.UserID, Type: input.Type}}
		cutoff, ok := continuations[key]
		if !ok {
			continue
		}
		delete(continuations, key) // settle each recipient once, at its first input
		query := db.Where("false")
		for _, candidate := range inputs {
			if candidate.ReusableGroup && candidate.ActivityScope == input.ActivityScope && candidate.UserID == input.UserID && candidate.Type == input.Type {
				query = query.Or("organization_id = ? AND user_id = ? AND type = ? AND group_key = ? AND reusable_group = true AND activity_scope = ?", candidate.OrganizationID, candidate.UserID, candidate.Type, candidate.GroupKey, candidate.ActivityScope)
			}
		}
		var targets []models.Notification
		if err := query.Select(retirementEntryColumns).Order("id").Find(&targets).Error; err != nil {
			return nil, err
		}
		rows, err := retireNotificationTargets(db, targets, cutoff)
		if err != nil {
			return nil, err
		}
		if len(rows) > 0 {
			if retired == nil {
				retired = make([][]models.Notification, len(inputs))
			}
			retired[index] = append(retired[index], rows...)
		}
	}
	return retired, nil
}

// AdvanceNotificationScopeRetirement retires one page of the next due scope
// job in its own transaction, claiming the job row like
// AdvanceChatNotificationRetirement. A page visits at most
// min(policy.PageSize, policy.MaxRecipients) recipient/type pairs.
func (s *GormNotificationStore[U]) AdvanceNotificationScopeRetirement(ctx context.Context, policy NotificationChatRetirementPolicy, publish func(context.Context, []models.Notification) error) (NotificationScopeRetirementAdvance, error) {
	var result NotificationScopeRetirementAdvance
	if ambient, ok := scontext.GetTransaction(ctx); ok && ambient != nil {
		// A caller-owned transaction would retain locks across multiple pages.
		return result, ErrNotificationInput
	}
	if !policy.valid() {
		return result, ErrNotificationInput
	}
	return s.advanceNotificationScopeRetirement(ctx, policy, publish, true)
}

func (s *GormNotificationStore[U]) advanceNotificationScopeRetirement(ctx context.Context, policy NotificationChatRetirementPolicy, publish func(context.Context, []models.Notification) error, preferRecipient bool) (NotificationScopeRetirementAdvance, error) {
	var result NotificationScopeRetirementAdvance
	limit := min(policy.PageSize, policy.MaxRecipients)
	var job models.NotificationScopeRetirement
	claimed := false
	recipientFirst := false
	stage := "claim"
	err := s.WithTransaction(ctx, func(tx context.Context) error {
		db := s.db.DB(tx)
		// Reads after the recipient locks must see rows committed while waiting.
		if err := db.Exec("SET TRANSACTION ISOLATION LEVEL READ COMMITTED").Error; err != nil {
			return err
		}
		if err := db.Where("due_at <= now()").Order("due_at, activity_scope").Clauses(clause.Locking{Strength: "UPDATE", Options: "SKIP LOCKED"}).Take(&job).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return nil
			}
			return err
		}
		// Recipient continuations share this worker: the job that has waited
		// longer runs first when its locks are available.
		if preferRecipient {
			if err := db.Raw("SELECT EXISTS (SELECT 1 FROM ? WHERE due_at < ?)", clause.Table{Name: models.NotificationRecipientRetirement{}.TableName()}, job.DueAt).Scan(&recipientFirst).Error; err != nil || recipientFirst {
				return err
			}
		}
		claimed = true
		stage = "fence"
		var fence models.NotificationScopeRetirementFence
		if err := db.Where("activity_scope = ?", job.ActivityScope).Take(&fence).Error; err != nil {
			return err
		}
		stage = "recipients"
		// Recipients already retired for this fence are skipped here and again
		// under their locks, since materialization may retire them meanwhile.
		var targets []models.Notification
		cutoffs := models.NotificationRetirementCutoff{}.TableName()
		if err := db.Raw("SELECT n.id,n.organization_id,n.user_id,n.type,n.group_key,n.reusable_group,n.activity_scope,n.revision,n.resolved FROM "+models.Notification{}.TableName()+" n CROSS JOIN (SELECT ?::bigint cutoff_epoch, ?::xid8 cutoff_upper_bound, ?::text cutoff_snapshot,?::xid8 cutoff_own_xid,?::bigint cutoff_own_ordinal) f"+
			" WHERE n.reusable_group = true AND n.activity_scope = ? AND (n.user_id, n.type, n.id) > (?::bigint, ?::bigint, ?::bigint)"+
			" AND NOT EXISTS (SELECT 1 FROM "+cutoffs+" c WHERE c.user_id = n.user_id AND c.type = n.type AND c.activity_scope = n.activity_scope"+
			" AND "+snapshotAtLeastSQL("c.", "f.")+" AND NOT EXISTS (SELECT 1 FROM "+models.NotificationRecipientRetirement{}.TableName()+" p WHERE p.activity_scope=c.activity_scope AND p.user_id=c.user_id AND p.type=c.type))"+
			" ORDER BY n.user_id, n.type, n.id LIMIT ?",
			fence.Cutoff.Epoch, fence.Cutoff.UpperBound, fence.Cutoff.Snapshot, fence.Cutoff.OwnXID, fence.Cutoff.OwnOrdinal, job.ActivityScope, job.AfterUserID, job.AfterType, job.AfterNotificationID, limit+1).Scan(&targets).Error; err != nil {
			return err
		}
		var next *models.Notification
		if len(targets) > limit {
			sentinel := targets[limit]
			next = &sentinel
			targets = targets[:limit]
		}
		var candidates []NotificationRecipientType
		for _, row := range targets {
			candidates = append(candidates, NotificationRecipientType{UserID: row.UserID, Type: row.Type})
		}
		candidates = uniqueNotificationRecipients(candidates)
		result.Recipients = len(candidates)
		stage = "retire"
		if err := lockNotificationRecipients(db, candidates); err != nil {
			return err
		}
		rows, err := retireNotificationTargets(db, targets, fence.Cutoff)
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
		completed := candidates
		if next != nil && len(completed) > 0 && completed[len(completed)-1] == (NotificationRecipientType{UserID: next.UserID, Type: next.Type}) {
			completed = completed[:len(completed)-1]
		}
		if err := upsertNotificationRetirementCutoffs(db, job.ActivityScope, completed, fence.Cutoff); err != nil {
			return err
		}
		if next == nil && len(targets) < limit {
			return db.Delete(&job).Error
		}
		last := targets[len(targets)-1]
		// Moving due_at to now() rotates this job behind its peers; a success
		// clears failure state.
		return db.Exec("UPDATE "+job.TableName()+" SET after_user_id = ?, after_type = ?, after_notification_id = ?, attempts = 0, last_error = '', exhausted_at = NULL, due_at = now(), updated_at = now() WHERE activity_scope = ?",
			last.UserID, last.Type, last.ID, job.ActivityScope).Error
	})
	if err == nil {
		result.Advanced = claimed
		if !claimed {
			result, err := s.advanceNotificationRecipientRetirement(ctx, policy, publish)
			if err == nil && !result.Advanced && recipientFirst {
				// The older continuation was busy or disappeared. Retry scope
				// work once without yielding again.
				return s.advanceNotificationScopeRetirement(ctx, policy, publish, false)
			}
			return result, err
		}
		return result, nil
	}
	result.Recipients = 0
	if !claimed {
		return result, err
	}
	result.Stage = stage
	if errors.Is(ctx.Err(), context.Canceled) {
		// Shutdown is not a job failure; the rolled-back claim is retried later.
		return result, err
	}
	attempts, exhausted, recordErr := recordRetirementFailure(ctx, s, policy, models.NotificationScopeRetirement{}.TableName(), "activity_scope", job.ActivityScope, job.UpdatedAt, stage)
	if recordErr != nil {
		return result, errors.Join(err, fmt.Errorf("record scope retirement failure: %w", recordErr))
	}
	result.Advanced, result.Attempts, result.Exhausted = true, attempts, exhausted
	if exhausted {
		err = fmt.Errorf("%w: %w", ErrNotificationScopeRetirementExhausted, err)
	}
	return result, err
}

// recordRetirementFailure backs off a retirement job after its page rolled
// back, for both the chat and scope job tables (fixed identifiers, never
// input). It matches the claimed row version so a concurrent re-enqueue, which
// resets the sweep, is not charged with the superseded job's failure. The
// delay is base * 2^attempts before this failure is counted, so the first
// retry waits exactly the base; exhaustion moves the job out of fast retry.
func recordRetirementFailure[U common_service_models.UserLike, K uint64 | string](ctx context.Context, s *GormNotificationStore[U], policy NotificationChatRetirementPolicy, table, keyColumn string, key K, version time.Time, stage string) (uint32, bool, error) {
	recordCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), policy.RecordFailureTimeout)
	defer cancel()
	var updated []struct {
		Attempts    uint32
		ExhaustedAt *time.Time
	}
	err := s.WithTransaction(recordCtx, func(tx context.Context) error {
		return s.db.DB(tx).Raw(`UPDATE `+table+` SET
			attempts = attempts + 1, last_error = ?, updated_at = now(),
			exhausted_at = CASE WHEN attempts + 1 >= ? THEN now() ELSE NULL END,
			due_at = now() + CASE WHEN attempts + 1 >= ? THEN make_interval(secs => ?)
				ELSE make_interval(secs => least(?::float8, ?::float8 * power(2, least(attempts, ?)))) END
			WHERE `+keyColumn+` = ? AND updated_at = ?
			RETURNING attempts, exhausted_at`, stage, policy.MaxAttempts, policy.MaxAttempts,
			policy.ExhaustedDelay.Seconds(), policy.RetryBackoffMax.Seconds(), policy.RetryBackoffBase.Seconds(),
			policy.backoffDoublings(), key, version).Scan(&updated).Error
	})
	if err != nil || len(updated) == 0 {
		return 0, false, err
	}
	return updated[0].Attempts, updated[0].ExhaustedAt != nil, nil
}

// scopeFencePending reports, in one statement over two primary-key probes,
// whether scope has a retirement fence that recipient has not been retired for.
func scopeFencePending(db *gorm.DB, scope string, recipient NotificationRecipientType) (bool, error) {
	var pending bool
	err := db.Raw("SELECT EXISTS (SELECT 1 FROM "+models.NotificationScopeRetirementFence{}.TableName()+" f WHERE f.activity_scope = ?"+
		" AND NOT EXISTS (SELECT 1 FROM "+models.NotificationRetirementCutoff{}.TableName()+" c WHERE c.user_id = ? AND c.type = ? AND c.activity_scope = f.activity_scope"+
		" AND "+snapshotAtLeastSQL("c.", "f.")+" AND NOT EXISTS (SELECT 1 FROM "+models.NotificationRecipientRetirement{}.TableName()+" p WHERE p.activity_scope=c.activity_scope AND p.user_id=c.user_id AND p.type=c.type)))",
		scope, recipient.UserID, recipient.Type).Scan(&pending).Error
	return pending, err
}

// retireScopeFencedDelivery closes the email side of unfinished scope-wide or
// explicit-audience retirement. Under the recipient lock it compares only the
// job's entry against applicable snapshots; later materializations survive and
// older entries are settled without hydrating siblings. A skipped claim's
// attempt is refunded because no provider was called. The common no-fence case
// takes only indexed probes, and publish runs inside the retirement transaction.
func (s *GormNotificationStore[U]) retireScopeFencedDelivery(ctx context.Context, job models.NotificationDelivery, entry models.Notification, publish func(context.Context, []models.Notification) error) (bool, error) {
	if !entry.ReusableGroup || entry.ActivityScope == "" {
		return false, nil
	}
	recipient := NotificationRecipientType{UserID: entry.UserID, Type: entry.Type}
	pending, err := scopeFencePending(s.db.DB(ctx), entry.ActivityScope, recipient)
	if err == nil && !pending {
		err = s.db.DB(ctx).Model(&models.NotificationRecipientRetirement{}).Select("count(*) > 0").Where("activity_scope = ? AND user_id = ? AND type = ?", entry.ActivityScope, recipient.UserID, recipient.Type).Scan(&pending).Error
	}
	if err != nil || !pending {
		return false, err
	}
	retired := false
	err = s.WithTransaction(ctx, func(tx context.Context) error {
		db := s.db.DB(tx)
		if err := db.Exec("SET TRANSACTION ISOLATION LEVEL READ COMMITTED").Error; err != nil {
			return err
		}
		if err := lockNotificationRecipients(db, []NotificationRecipientType{recipient}); err != nil {
			return err
		}
		var cutoffs []models.NotificationVisibilitySnapshot
		var fences []models.NotificationScopeRetirementFence
		if err := db.Where("activity_scope = ?", entry.ActivityScope).Find(&fences).Error; err != nil {
			return err
		}
		for _, fence := range fences {
			unretired, err := unretiredRecipients(db, entry.ActivityScope, []NotificationRecipientType{recipient}, fence.Cutoff)
			if err != nil {
				return err
			}
			if len(unretired) > 0 {
				cutoffs = append(cutoffs, fence.Cutoff)
			}
		}
		var continuations []models.NotificationRecipientRetirement
		if err := db.Where("activity_scope = ? AND user_id = ? AND type = ?", entry.ActivityScope, recipient.UserID, recipient.Type).Find(&continuations).Error; err != nil {
			return err
		}
		for _, continuation := range continuations {
			cutoffs = append(cutoffs, continuation.Cutoff)
		}
		var targets []models.Notification
		if err := db.Select(retirementEntryColumns).Where("id = ? AND activity_scope = ?", entry.ID, entry.ActivityScope).Find(&targets).Error; err != nil {
			return err
		}
		var rows []models.Notification
		for _, cutoff := range cutoffs {
			changed, err := retireNotificationTargets(db, targets, cutoff)
			if err != nil {
				return err
			}
			rows = append(rows, changed...)
		}
		if err := db.Model(&models.NotificationDelivery{}).Where("id = ? AND attempts = ? AND state = ?", job.ID, job.Attempts, NotificationDeliverySkipped).
			UpdateColumn("attempts", gorm.Expr("attempts - 1")).Error; err != nil {
			return err
		}
		if err := db.Model(&models.NotificationDelivery{}).Select("count(*) > 0").Where("id = ? AND state = ?", job.ID, NotificationDeliverySkipped).Scan(&retired).Error; err != nil {
			return err
		}
		if publish != nil && len(rows) > 0 {
			return publish(tx, rows)
		}
		return nil
	})
	return retired, err
}
