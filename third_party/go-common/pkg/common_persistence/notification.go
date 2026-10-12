package common_persistence

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	models "github.com/Suhaibinator/go-common/pkg/common_persistence/common_persistence_models"
	"github.com/Suhaibinator/go-common/pkg/common_service/common_service_models"
	"github.com/Suhaibinator/go-common/pkg/config"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

var (
	ErrNotificationInput           = errors.New("invalid notification persistence input")
	ErrNotificationLeaseLost       = errors.New("notification delivery lease lost")
	ErrNotificationDeliveryChanged = errors.New("notification delivery content changed")
	ErrNotificationEpisodeConflict = errors.New("notification group already has a newer unread episode")
)

const (
	NotificationDeliveryPending    = "pending"
	NotificationDeliveryDeferred   = "deferred"
	NotificationDeliveryWaiting    = "waiting"
	NotificationDeliverySuppressed = "suppressed"
	NotificationDeliveryDisabled   = "disabled"
	NotificationDeliverySent       = "sent"
	NotificationDeliverySkipped    = "skipped"
	NotificationDeliveryExhausted  = "exhausted"
)

// validDeliveryTag reports whether tag fits the tag columns
// (config.MaxNotificationDeliveryTagBytes). An empty tag is valid and means an
// unfiltered occurrence or preference.
func validDeliveryTag(tag string) bool { return len(tag) <= config.MaxNotificationDeliveryTagBytes }

// NotificationRecordInput contains a validated producer event. Empty GroupKey
// disables grouping. ReusableGroup retains a single row across read episodes.
// EmailDueAt schedules one delivery for the unread episode.
type NotificationRecordInput struct {
	OrganizationID              int64
	UserID                      uint64
	Type                        uint32
	ReusableGroup               bool
	GroupKey, SourceEventKey    string
	Payload                     []byte
	FallbackText, ActivityScope string
	EmailDueAt                  *time.Time
	Now                         time.Time
	SourceTime                  time.Time
	DeliveryTag                 string
	// DeliveryFilter samples the current interruption mode before taking locks.
	// Empty permits any new occurrence. Suppressed occurrences never re-arm.
	DeliveryFilter     string
	DeliverySuppressed bool
	SourceStamp        models.NotificationTransactionStamp
	// Native membership stamps supersede the generic activity retirement fence.
	SkipRetirementFence bool
	// FreshCheck performs only trusted database rechecks after the recipient/type
	// lock. Expensive application permission callbacks must run before RecordMany.
	FreshCheck func(context.Context) (bool, error)
}

// NotificationRecordResult corresponds to the input at the same batch index.
// Changed is false for an already-recorded source occurrence.
type NotificationRecordResult struct {
	Entry   *models.Notification
	Changed bool
	// Suppressed distinguishes a lifecycle fence from a malformed store result.
	Suppressed bool
	// Retired lists entries a pending scope-wide retirement fence resolved for
	// this input's recipient/type before it was recorded. It is set on the first
	// input of that recipient/type and scope; callers publish these rows too.
	Retired []models.Notification
}

// NotificationReadReceipt acknowledges only a revision actually observed by a client.
type NotificationReadReceipt struct{ ID, Revision uint64 }

// NotificationStore is an optional persistence capability, independent of the
// common persistence facade. All request operations honor the ambient
// transaction. It composes the narrow interfaces below, and an alternate store
// implements all of them.
type NotificationStore interface {
	NotificationSchedulingStore
	NotificationInboxStore
	NotificationDeliveryStore
	NotificationRetirementStore
	NotificationFanoutStore
	NotificationOrganizationStore
	NotificationChatRetirementStore
}

// NotificationInboxStore supplies inline emission, inbox and alert operations.
//
// RecordMany records a whole batch with one transaction/savepoint and invokes
// its result callback inside that transaction so post-commit hooks transfer to
// the source commit. MarkReadReturning acknowledges observed revisions and
// returns only entries whose read state advanced.
//
// ClaimAlert fences the qualifying occurrence revision as well as the current
// inbox revision against concurrent reads and successful claims; unfiltered
// claims pass the current revision for both.
type NotificationInboxStore interface {
	WithTransaction(context.Context, func(context.Context) error) error
	RecordMany(context.Context, []NotificationRecordInput, func(context.Context, []NotificationRecordResult) error) ([]NotificationRecordResult, error)
	Get(context.Context, uint64, uint64) (*models.Notification, error)
	List(context.Context, uint64, int, uint64) ([]models.Notification, error)
	ListUnread(context.Context, uint64, int, uint64) ([]models.Notification, error)
	ListByActivityScope(context.Context, uint64, string, int, uint64, bool) ([]models.Notification, error)
	MarkReadReturning(context.Context, uint64, []NotificationReadReceipt) ([]models.Notification, error)
	Resolve(context.Context, uint64, uint64) (*models.Notification, error)
	// ResolveSource takes the notification_source_resolution_page_size sample
	// that decides between settling inline and persisting a resolution job.
	ResolveSource(context.Context, uint32, string, int) ([]models.Notification, error)
	// AdvanceNotificationSourceResolution resolves one page of a source
	// resolution too large for the caller's transaction, in its own
	// transaction; publish runs inside it.
	AdvanceNotificationSourceResolution(context.Context, NotificationChatRetirementPolicy, func(context.Context, []models.Notification) error) (NotificationSourceResolutionAdvance, error)
	Update(context.Context, uint64, uint64, []byte, string) (*models.Notification, error)
	Preferences(context.Context, uint64) ([]models.NotificationPreference, error)
	SetPreference(context.Context, *models.NotificationPreference) error
	ClaimAlert(ctx context.Context, userID, id, revision, qualifyingRevision uint64, now time.Time, cooldown time.Duration) (bool, error)
	// ListUnreadCandidates returns authorization fields, not complete inbox entries.
	ListUnreadCandidates(context.Context, uint64, int, uint64) ([]models.Notification, error)
}

// NotificationDeliverySendPolicy is the sampled type contract at the send
// boundary. Reusable activity groups always cancel on acknowledgment or resolution.
type NotificationDeliverySendPolicy struct {
	CancelOnRead                bool
	CancelOnResolution          bool
	EmailContentUsesPayloadOnly bool
}

// NotificationDeliveryStore supplies delivery operations.
//
// GetDeliveryOccurrence selects a bounded immutable occurrence for
// interruption filters: afterRevision is exclusive, throughRevision inclusive,
// and historical and acknowledged occurrences never match a nonempty tag.
type NotificationDeliveryStore interface {
	GetDeliveryOccurrence(ctx context.Context, userID, notificationID uint64, tag string, afterRevision, throughRevision uint64, latest bool) (*models.NotificationOccurrence, error)
	ClaimDeliveries(context.Context, string, time.Time, time.Duration, int, uint32) ([]models.NotificationDelivery, error)
	FinishDelivery(context.Context, models.NotificationDelivery, string, string, time.Time, time.Time) error
	// PrepareDeliverySend checks the authoritative lease and inbox state after
	// application callbacks. It settles cancellations and unfinished retirement
	// fences without charging an attempt, returning true when the job was skipped.
	// A changed renderable revision returns ErrNotificationDeliveryChanged so the
	// worker can defer the captured attempt and render current content again.
	// publish runs inside the retirement transaction. Network calls follow outside
	// database locks; an external send cannot be recalled after this boundary.
	PrepareDeliverySend(ctx context.Context, job models.NotificationDelivery, entry models.Notification, cancellation NotificationDeliverySendPolicy, publish func(context.Context, []models.Notification) error) (bool, error)
}

// NotificationRetirementStore supplies retirement operations.
type NotificationRetirementStore interface {
	// RetireActivityScope settles reusable entries and deliveries after access
	// removal. With a nonzero userID it is synchronous; every synchronous
	// retirement method takes the caller's sample of
	// notification_retirement_max_recipients and rejects a call with more unique
	// recipient/type pairs with ErrNotificationInput before taking any lock. A
	// zero userID commits a scope fence and paging job in the ambient
	// transaction instead, with no size limit, and returns no rows.
	RetireActivityScope(ctx context.Context, userID uint64, scope string, maxRecipients int) ([]models.Notification, error)
	// AdvanceNotificationScopeRetirement retires one page of a due scope-wide
	// retirement job in its own transaction; publish runs inside it.
	AdvanceNotificationScopeRetirement(context.Context, NotificationChatRetirementPolicy, func(context.Context, []models.Notification) error) (NotificationScopeRetirementAdvance, error)
	// EnqueueNotificationScopeRetirements admits at most maxScopes scope-wide
	// retirements in canonical scope order. Pass every scope an operation
	// retires in one call, before any recipient/type lock in the source transaction.
	EnqueueNotificationScopeRetirements(ctx context.Context, scopes []string, maxScopes int) error
	// Retirement cutoffs fence audiences whose async entries may not exist yet.
	RetireActivityScopeForRecipients(ctx context.Context, scope string, recipients []NotificationRecipientType, maxRecipients int) ([]models.Notification, error)
}

// NotificationFanoutStore supplies fanout operations.
type NotificationFanoutStore interface {
	// Enqueue, Append, and Finalize share one ambient source transaction; every
	// new emission must be finalized before commit. All methods honor its hooks.
	EnqueueNotificationEmission(context.Context, models.NotificationEmission) (*models.NotificationEmission, bool, error)
	AppendNotificationEmissionRecipients(context.Context, uint64, string, []models.NotificationEmissionRecipient) error
	FinalizeNotificationEmission(context.Context, uint64, string, string) (*models.NotificationEmission, error)
	GetNotificationEmission(context.Context, uint64) (*models.NotificationEmission, error)
	RetryNotificationEmission(context.Context, uint64) (bool, error)
	// ClaimNotificationEmissions leases up to limit due emissions for owner. A
	// job whose attempts already reached maxAttempts is marked exhausted and
	// returned unleased instead.
	ClaimNotificationEmissions(ctx context.Context, owner string, lease time.Duration, limit int, maxAttempts uint32) ([]models.NotificationEmission, error)
	AdvanceNotificationEmission(context.Context, models.NotificationEmission, func(context.Context, *models.NotificationEmission) (NotificationEmissionProgress, error)) (*models.NotificationEmission, error)
	ListNotificationEmissionRecipients(context.Context, uint64, int) ([]models.NotificationEmissionRecipient, error)
	// FinishNotificationEmissionAttempt releases a lease after a failed or
	// deferred step; a failed step whose attempts reached maxAttempts exhausts the job.
	FinishNotificationEmissionAttempt(ctx context.Context, job models.NotificationEmission, deferred bool, reason string, retryAfter time.Duration, maxAttempts uint32) error
	NotificationFanoutBacklog(ctx context.Context, now time.Time, limit int) (NotificationFanoutBacklog, error)
	CompactNotificationEmissions(context.Context, time.Time, int) (int64, error)
}

// NotificationOrganizationStore supplies organization operations.
type NotificationOrganizationStore interface {
	NotificationOrganizationExists(context.Context, int64) (bool, error)
	NotificationOrganizationMembers(context.Context, []NotificationOrganizationRecipient) ([]bool, error)
	ResolveOrganizationSource(context.Context, int64, uint32, string, int) ([]models.Notification, error)

	NotificationOrganizationMemberPage(context.Context, int64, uint64, int) ([]uint64, error)
}

// NotificationChatRetirementStore supplies native chat lifecycle retirement.
type NotificationChatRetirementStore interface {
	// RetireChatActivityScopeForRecipients runs in the leave/remove source
	// transaction. Native chat materialization re-checks membership itself, so
	// no generic retirement cutoff is written.
	RetireChatActivityScopeForRecipients(ctx context.Context, scope string, recipients []NotificationRecipientType, maxRecipients int) ([]models.Notification, error)
	// Enqueue runs in the source transaction. Re-enqueueing a pending job for
	// the same channel restarts its sweep, since a new deletion invalidates
	// earlier progress (for example delete, restore, then delete again).
	EnqueueChatNotificationRetirement(context.Context, uint64, string, []uint32) error
	// Advance owns one transaction and calls publish before commit so it can
	// register post-commit hooks. Errors roll back entries and cursor together;
	// the failure is then recorded in a separate short transaction.
	AdvanceChatNotificationRetirement(context.Context, NotificationChatRetirementPolicy, func(context.Context, []models.Notification) error) (NotificationChatRetirementAdvance, error)
}

// NotificationRecipientType identifies the shared serialization lock.
type NotificationRecipientType struct {
	UserID uint64
	Type   uint32
}

// GormNotificationStore uses PostgreSQL row and advisory locks. U must be the
// application's SRouter user type so source transactions and hooks are preserved.
type GormNotificationStore[U common_service_models.UserLike] struct {
	queueWakes                notificationQueueSubscribers
	recipientRetirementClaims notificationRecipientRetirementClaimCursor
	db                        DatabaseProvider
	hookTimeout               func() time.Duration
}

// NewNotificationStore wires the store to db. hookTimeout samples the hot
// post_commit_hook_timeout key once per standalone post-commit drain; the
// service passes its view's PostCommitHookTimeout. A nil sampler (tests, direct
// store callers without a provider) uses the key's default.
func NewNotificationStore[U common_service_models.UserLike](db DatabaseProvider, hookTimeout func() time.Duration) *GormNotificationStore[U] {
	if hookTimeout == nil {
		hookTimeout = func() time.Duration { return defaultPostCommitHookTimeout }
	}
	return &GormNotificationStore[U]{db: db, hookTimeout: hookTimeout}
}

var _ NotificationStore = (*GormNotificationStore[common_service_models.User])(nil)

// WithTransaction uses a savepoint under an ambient transaction and defers its
// hooks to the outer commit. Standalone calls own their transaction and flush
// best-effort hooks only after commit (a failed hook is logged only through a
// request logger in ctx). Callback failures discard their hooks.
func (s *GormNotificationStore[U]) WithTransaction(ctx context.Context, fn func(context.Context) error) error {
	return transactionWithHooks[U](ctx, s.db.DB(ctx), s.hookTimeout, nil, func(txCtx context.Context, _ *gorm.DB) error {
		return fn(txCtx)
	})
}

// RecordMany records a batch in one transaction or savepoint. Results retain
// input order; duplicate sources preserve the first input's semantics. afterRecord
// runs inside that same transaction so callers can register post-commit hooks.
// Any mutation or callback error rolls back the whole batch and discards its hooks.
//
// Existing recipient/type advisory-lock identities are acquired in total order
// before row writes. Callers holding earlier source locks or emitting multiple
// batches must still keep a compatible order and retry their whole transaction
// on a PostgreSQL deadlock; this does not globally eliminate deadlocks.
func (s *GormNotificationStore[U]) RecordMany(ctx context.Context, inputs []NotificationRecordInput, afterRecord func(context.Context, []NotificationRecordResult) error) ([]NotificationRecordResult, error) {
	for _, input := range inputs {
		if input.OrganizationID < 0 || input.UserID == 0 || input.Type == 0 || input.SourceEventKey == "" || len(input.SourceEventKey) > config.MaxNotificationSourceKeyBytes || len(input.GroupKey) > config.MaxNotificationGroupKeyBytes || !validDeliveryTag(input.DeliveryTag) || !validDeliveryTag(input.DeliveryFilter) || (input.ReusableGroup && input.GroupKey == "") {
			return nil, ErrNotificationInput
		}
	}
	// Normalize missing timestamps on a private positional copy.
	inputs = append([]NotificationRecordInput(nil), inputs...)
	results := make([]NotificationRecordResult, len(inputs))
	err := s.WithTransaction(ctx, func(ctx context.Context) error {
		db := s.db.DB(ctx)
		recipients := make([]NotificationRecipientType, 0, len(inputs))
		groupStates := make([]models.NotificationGroupState, 0, len(inputs))
		for _, input := range inputs {
			recipients = append(recipients, NotificationRecipientType{UserID: input.UserID, Type: input.Type})
			if input.GroupKey != "" {
				groupStates = append(groupStates, models.NotificationGroupState{OrganizationID: input.OrganizationID, UserID: input.UserID, Type: input.Type, GroupKey: input.GroupKey, ReusableGroup: input.ReusableGroup})
			}
		}
		// Scope locks precede recipient locks so a scope-wide retirement enqueue
		// (exclusive) never waits on a transaction that waits on it.
		scopes := fencedScopes(inputs)
		if err := lockNotificationScopesShared(db, scopes); err != nil {
			return err
		}
		if err := lockNotificationRecipients(db, recipients); err != nil {
			return err
		}
		scopeRetired, err := applyScopeRetirementFences(db, inputs, scopes)
		if err != nil {
			return err
		}
		lockedGroups, err := lockNotificationGroupStates(db, groupStates)
		if err != nil {
			return err
		}
		eligible := make([]bool, len(inputs))
		for i, input := range inputs {
			eligible[i] = true
			if input.FreshCheck != nil {
				var err error
				eligible[i], err = input.FreshCheck(ctx)
				if err != nil {
					return err
				}
			}
		}
		fences, retired, err := s.notificationFences(ctx, inputs, eligible, lockedGroups)
		if err != nil {
			return err
		}
		for i := range eligible {
			eligible[i] = eligible[i] && !retired[i]
		}
		stamp, _, err := s.CaptureNotificationTransaction(ctx)
		if err != nil {
			return err
		}
		// Match the ID ordering used by acknowledgments before applying inputs in
		// producer order. This prevents an acknowledgment spanning multiple groups
		// from deadlocking against a reverse-ordered fanout.
		groups := db.Where("false")
		groupKeys := make(map[notificationRecordGroup]bool)
		for _, input := range inputs {
			key := recordGroupKey(input)
			if input.GroupKey != "" && !groupKeys[key] {
				groupKeys[key] = true
				groups = groups.Or("(organization_id = ? AND user_id = ? AND type = ? AND group_key = ? AND reusable_group = ? AND (reusable_group OR (revision > read_revision AND resolved = false)))", input.OrganizationID, input.UserID, input.Type, input.GroupKey, input.ReusableGroup)
			}
		}
		var locked []models.Notification
		if len(groupKeys) != 0 {
			if err := groups.Order("id ASC").Clauses(clause.Locking{Strength: "UPDATE"}).Find(&locked).Error; err != nil {
				return err
			}
		}
		state, err := prefetchNotificationRecordState(db, inputs, eligible, fences, locked)
		if err != nil {
			return err
		}
		state.stamp = stamp
		fast, err := recordIndependentNotifications(db, inputs, eligible, fences, state, results)
		if err != nil {
			return err
		}
		for index := range inputs {
			if fast[index] {
				continue
			}
			if !eligible[index] {
				results[index].Suppressed = true
				continue
			}
			var err error
			results[index], err = s.recordInTransaction(ctx, inputs[index], fences[index], state)
			if err != nil {
				return err
			}
		}
		for index := range scopeRetired {
			results[index].Retired = scopeRetired[index]
		}
		if afterRecord != nil {
			return afterRecord(ctx, results)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	for _, input := range inputs {
		if input.EmailDueAt != nil {
			s.queueChanged(ctx, NotificationQueueDelivery)
			break
		}
	}
	return results, nil
}

// recordInTransaction assumes the batch transaction and recipient/type lock are
// already held. It deliberately creates no nested transaction or savepoint.
func (s *GormNotificationStore[U]) recordInTransaction(ctx context.Context, input NotificationRecordInput, fence *models.NotificationGroupState, state *notificationRecordState) (NotificationRecordResult, error) {
	if input.Now.IsZero() {
		input.Now = time.Now().UTC()
	}
	db := s.db.DB(ctx)
	occurrence, exists := state.occurrences[recordOccurrenceKey(input)]
	if exists {
		entry := state.entries[occurrence.NotificationID]
		return NotificationRecordResult{Entry: cloneRecordedNotification(entry)}, nil
	}
	if fence != nil {
		entry := state.entries[fence.NotificationID]
		if entry == nil {
			return NotificationRecordResult{}, fmt.Errorf("acknowledged notification %d missing: %w", fence.NotificationID, ErrNotificationInput)
		}
		occurrence = models.NotificationOccurrence{OrganizationID: input.OrganizationID, UserID: input.UserID, Type: input.Type, SourceEventKey: input.SourceEventKey, NotificationID: fence.NotificationID, Revision: fence.Revision, Disposition: NotificationOccurrenceAcknowledged, DeliveryTag: input.DeliveryTag, SourceTime: input.SourceTime, MaterializedAt: input.Now}
		if err := db.Create(&occurrence).Error; err != nil {
			return NotificationRecordResult{}, err
		}
		state.occurrences[recordOccurrenceKey(input)] = occurrence
		return NotificationRecordResult{Entry: cloneRecordedNotification(entry)}, nil
	}
	entry := models.Notification{}
	if input.GroupKey != "" {
		if cached := state.groups[recordGroupKey(input)]; cached != nil {
			entry = *cached
		}
	}
	entry, newEpisode, qualifiesForDelivery := advanceRecordedNotification(input, entry)
	entry.MaterializationStamp = state.stamp
	if err := db.Save(&entry).Error; err != nil {
		return NotificationRecordResult{}, err
	}
	occurrence = models.NotificationOccurrence{OrganizationID: input.OrganizationID, UserID: input.UserID, Type: input.Type, SourceEventKey: input.SourceEventKey, NotificationID: entry.ID, Revision: entry.Revision, Disposition: NotificationOccurrenceMaterialized, DeliveryTag: input.DeliveryTag, SourceTime: entry.SourceTime, MaterializedAt: input.Now}
	if err := db.Create(&occurrence).Error; err != nil {
		return NotificationRecordResult{}, err
	}
	state.occurrences[recordOccurrenceKey(input)] = occurrence
	state.entries[entry.ID] = &entry
	if input.GroupKey != "" {
		state.groups[recordGroupKey(input)] = &entry
	}
	if newEpisode && input.ReusableGroup && input.EmailDueAt == nil {
		// A disabled email policy must also invalidate the previous episode's lease.
		// Keep the new episode dormant so later eligible activity can schedule it.
		if err := db.Model(&models.NotificationDelivery{}).Where("notification_id = ?", entry.ID).Select("unread_episode_revision", "state", "attempts", "lease_owner", "lease_token", "lease_expires_at", "last_error", "waiting_tag").Updates(&models.NotificationDelivery{UnreadEpisodeRevision: entry.UnreadEpisodeRevision, State: NotificationDeliveryDisabled}).Error; err != nil {
			return NotificationRecordResult{}, err
		}
	}
	if input.EmailDueAt != nil {
		dueAt := *input.EmailDueAt
		if input.ReusableGroup {
			// Enabling email partway through an unread episode must not restart
			// its delay merely because this is the first event to schedule a job.
			dueAt = entry.UnreadEpisodeStartedAt.Add(input.EmailDueAt.Sub(input.Now))
		}
		delivery := models.NotificationDelivery{NotificationID: entry.ID, UnreadEpisodeRevision: entry.UnreadEpisodeRevision, UserID: input.UserID, DueAt: dueAt, State: NotificationDeliveryPending}
		conflict := clause.OnConflict{Columns: []clause.Column{{Name: "notification_id"}}, DoNothing: true}
		if newEpisode && input.ReusableGroup {
			conflict.DoNothing = false
			reset := models.NotificationDelivery{UnreadEpisodeRevision: entry.UnreadEpisodeRevision, DueAt: dueAt, State: NotificationDeliveryPending, UpdatedAt: input.Now}
			conflict.DoUpdates = clause.Set{
				{Column: clause.Column{Name: "unread_episode_revision"}, Value: reset.UnreadEpisodeRevision},
				{Column: clause.Column{Name: "due_at"}, Value: reset.DueAt},
				{Column: clause.Column{Name: "state"}, Value: reset.State},
				{Column: clause.Column{Name: "attempts"}, Value: reset.Attempts},
				{Column: clause.Column{Name: "lease_owner"}, Value: reset.LeaseOwner},
				{Column: clause.Column{Name: "lease_token"}, Value: reset.LeaseToken},
				{Column: clause.Column{Name: "lease_expires_at"}, Value: reset.LeaseExpiresAt},
				{Column: clause.Column{Name: "last_error"}, Value: reset.LastError},
				{Column: clause.Column{Name: "waiting_tag"}, Value: reset.WaitingTag},
				{Column: clause.Column{Name: "updated_at"}, Value: reset.UpdatedAt},
			}
		}
		if !newEpisode && qualifiesForDelivery {
			// Re-arm only dormant unsent work in this episode. Folding this into
			// the insert avoids a delivery UPDATE for every recipient/message and
			// preserves failures already spent in the episode's retry budget.
			rearmDue := *input.EmailDueAt
			if input.DeliveryFilter == "" {
				rearmDue = dueAt
			}
			conflict.DoNothing = false
			rearm := models.NotificationDelivery{State: NotificationDeliveryPending, DueAt: rearmDue, UpdatedAt: input.Now}
			conflict.DoUpdates = clause.Set{
				{Column: clause.Column{Name: "state"}, Value: rearm.State},
				{Column: clause.Column{Name: "due_at"}, Value: rearm.DueAt},
				{Column: clause.Column{Name: "waiting_tag"}, Value: rearm.WaitingTag},
				{Column: clause.Column{Name: "updated_at"}, Value: rearm.UpdatedAt},
			}
			conflict.Where = clause.Where{Exprs: []clause.Expression{clause.Expr{
				SQL:  "gocommon_notification_deliveries.state IN ? AND gocommon_notification_deliveries.unread_episode_revision = excluded.unread_episode_revision",
				Vars: []any{[]string{NotificationDeliveryWaiting, NotificationDeliverySuppressed, NotificationDeliveryDisabled}},
			}}}
		}
		if err := db.Clauses(conflict).Create(&delivery).Error; err != nil {
			return NotificationRecordResult{}, err
		}
	}
	return NotificationRecordResult{Entry: &entry, Changed: true}, nil
}

func (s *GormNotificationStore[U]) Get(ctx context.Context, userID, id uint64) (*models.Notification, error) {
	var entry models.Notification
	err := s.db.DB(ctx).Where("user_id = ? AND id = ?", userID, id).Take(&entry).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil
	}
	return &entry, err
}

// List orders immutable IDs so concurrent regrouping cannot cause pagination to
// skip or duplicate entries. The revision identifies updates to existing rows.
func (s *GormNotificationStore[U]) List(ctx context.Context, userID uint64, limit int, beforeID uint64) ([]models.Notification, error) {
	return s.list(ctx, userID, limit, beforeID, false, nil)
}

// ListUnread bounds authorization work to currently unread, unresolved entries.
func (s *GormNotificationStore[U]) ListUnread(ctx context.Context, userID uint64, limit int, beforeID uint64) ([]models.Notification, error) {
	return s.list(ctx, userID, limit, beforeID, true, nil)
}

// ListUnreadCandidates uses the same unread keyset scan with only the fields
// required by source and organization authorization and resolution evaluation.
func (s *GormNotificationStore[U]) ListUnreadCandidates(ctx context.Context, userID uint64, limit int, beforeID uint64) ([]models.Notification, error) {
	return s.list(ctx, userID, limit, beforeID, true, nil,
		"id", "user_id", "type", "organization_id", "payload", "revision", "read_revision", "resolved")
}

// ListByActivityScope filters exact stored scopes before pagination and authorization.
func (s *GormNotificationStore[U]) ListByActivityScope(ctx context.Context, userID uint64, scope string, limit int, beforeID uint64, unreadOnly bool) ([]models.Notification, error) {
	return s.list(ctx, userID, limit, beforeID, unreadOnly, &scope)
}

func (s *GormNotificationStore[U]) list(ctx context.Context, userID uint64, limit int, beforeID uint64, unread bool, scope *string, columns ...string) ([]models.Notification, error) {
	// The service samples notification_list_* per operation and sends at most
	// one fetch; the store only rejects values outside the keys' ceiling.
	if limit < 1 || limit > config.MaxNotificationListLimit {
		return nil, ErrNotificationInput
	}
	db := s.db.DB(ctx).Where("user_id = ?", userID)
	if len(columns) > 0 {
		db = db.Select(columns)
	}
	if scope != nil {
		db = db.Where("activity_scope = ?", *scope)
	}
	if beforeID > 0 {
		db = db.Where("id < ?", beforeID)
	}
	if unread {
		db = db.Where("revision > read_revision AND resolved = false")
	}
	var entries []models.Notification
	err := db.Order("id DESC").Limit(limit).Find(&entries).Error
	return entries, err
}

// MarkReadReturning serializes with materialization before capturing the read
// committed statement snapshot. Group state is always locked before inbox rows.
func (s *GormNotificationStore[U]) MarkReadReturning(ctx context.Context, userID uint64, receipts []NotificationReadReceipt) ([]models.Notification, error) {
	ids := make([]uint64, 0, len(receipts))
	for _, receipt := range receipts {
		if receipt.ID == 0 || receipt.Revision == 0 {
			return nil, ErrNotificationInput
		}
		ids = append(ids, receipt.ID)
	}
	if len(ids) == 0 {
		return nil, nil
	}
	var changed []models.Notification
	err := s.WithTransaction(ctx, func(ctx context.Context) error {
		db := s.db.DB(ctx)
		var entries []models.Notification
		if err := db.Select("id", "user_id", "type", "organization_id", "group_key", "reusable_group").Where("user_id = ? AND id IN ?", userID, ids).Find(&entries).Error; err != nil {
			return err
		}
		recipients := make([]NotificationRecipientType, 0, len(entries))
		for _, entry := range entries {
			recipients = append(recipients, NotificationRecipientType{UserID: entry.UserID, Type: entry.Type})
		}
		if err := lockNotificationRecipients(db, recipients); err != nil {
			return err
		}
		// A separate statement after advisory lock acquisition is essential.
		snapshot, err := s.CaptureNotificationSnapshot(ctx)
		if err != nil {
			return err
		}
		groups := make([]models.NotificationGroupState, 0, len(entries))
		ids = ids[:0]
		for _, entry := range entries {
			groups = append(groups, notificationGroupIdentity(entry))
			ids = append(ids, entry.ID)
		}
		if err := lockNotificationGroups(db, groups); err != nil {
			return err
		}
		if err := db.Where("user_id = ? AND id IN ?", userID, ids).Order("id").Clauses(clause.Locking{Strength: "UPDATE"}).Find(&entries).Error; err != nil {
			return err
		}
		current := make(map[uint64]uint64, len(entries))
		acknowledged := make(map[uint64]uint64, len(entries))
		for _, entry := range entries {
			current[entry.ID] = entry.Revision
		}
		for _, receipt := range receipts {
			if revision, exists := current[receipt.ID]; exists && receipt.Revision <= revision && receipt.Revision > acknowledged[receipt.ID] {
				acknowledged[receipt.ID] = receipt.Revision
			}
		}
		for _, entry := range entries {
			revision := max(entry.ReadRevision, acknowledged[entry.ID])
			if revision == entry.ReadRevision {
				continue
			}
			entry.ReadRevision = revision
			changed = append(changed, entry)
		}
		if len(changed) == 0 {
			return nil
		}
		if err := saveNotificationAcknowledgments(db, changed, snapshot); err != nil {
			return err
		}
		// The rows are already locked in ID order. One update preserves the old
		// materialization timestamp and avoids a SQL round-trip per receipt.
		var values strings.Builder
		args := make([]any, 0, len(changed)*2+1)
		for i, entry := range changed {
			if i > 0 {
				values.WriteByte(',')
			}
			values.WriteString("(?::bigint,?::bigint)")
			args = append(args, entry.ID, entry.ReadRevision)
		}
		args = append(args, userID)
		return db.Exec("UPDATE gocommon_notifications AS entry SET read_revision = acknowledged.revision FROM (VALUES "+values.String()+") AS acknowledged(id,revision) WHERE entry.id = acknowledged.id AND entry.user_id = ?", args...).Error
	})
	if err != nil {
		return nil, err
	}
	return changed, nil
}

func (s *GormNotificationStore[U]) Resolve(ctx context.Context, userID, id uint64) (*models.Notification, error) {
	var result *models.Notification
	err := s.WithTransaction(ctx, func(ctx context.Context) error {
		db := s.db.DB(ctx)
		var entry models.Notification
		if err := db.Where("user_id = ? AND id = ?", userID, id).Take(&entry).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return nil
			}
			return err
		}
		if err := lockNotificationRecipients(db, []NotificationRecipientType{{UserID: userID, Type: entry.Type}}); err != nil {
			return err
		}
		snapshot, err := s.CaptureNotificationSnapshot(ctx)
		if err != nil {
			return err
		}
		if err := lockNotificationGroups(db, []models.NotificationGroupState{notificationGroupIdentity(entry)}); err != nil {
			return err
		}
		if err := db.Where("user_id = ? AND id = ?", userID, id).Clauses(clause.Locking{Strength: "UPDATE"}).Take(&entry).Error; err != nil {
			return err
		}
		if entry.Resolved {
			result = &entry
			return nil
		}
		// Resolution fences prior committed work without claiming that the user
		// read it. Only the group target uses this revision; ReadRevision is intact.
		acknowledged := entry
		acknowledged.ReadRevision = entry.Revision
		if err := saveNotificationAcknowledgments(db, []models.Notification{acknowledged}, snapshot); err != nil {
			return err
		}
		entry.Resolved = true
		if err := db.Model(&entry).Where("user_id = ? AND id = ?", userID, id).Clauses(clause.Returning{}).Update("resolved", true).Error; err != nil {
			return err
		}
		result = &entry
		return nil
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}
func (s *GormNotificationStore[U]) Preferences(ctx context.Context, userID uint64) ([]models.NotificationPreference, error) {
	var prefs []models.NotificationPreference
	err := s.db.DB(ctx).Where("user_id = ?", userID).Order("type ASC").Find(&prefs).Error
	return prefs, err
}

// SetPreference patches supplied fields; nil leaves the existing value intact.
func (s *GormNotificationStore[U]) SetPreference(ctx context.Context, pref *models.NotificationPreference) error {
	if pref == nil || pref.UserID == 0 {
		return ErrNotificationInput
	}
	var columns []string
	if pref.AlertsEnabled != nil {
		columns = append(columns, "alerts_enabled")
	}
	if pref.EmailEnabled != nil {
		columns = append(columns, "email_enabled")
	}
	if len(columns) == 0 {
		return nil
	}
	return s.db.DB(ctx).Clauses(clause.OnConflict{Columns: []clause.Column{{Name: "user_id"}, {Name: "type"}}, DoUpdates: clause.AssignmentColumns(columns)}).Create(pref).Error
}

// ClaimAlert checks that the qualifying occurrence remains unread and
// unclaimed in the same atomic UPDATE as the current-revision claim.
func (s *GormNotificationStore[U]) ClaimAlert(ctx context.Context, userID, id, revision, qualifyingRevision uint64, now time.Time, cooldown time.Duration) (bool, error) {
	if revision == 0 || qualifyingRevision == 0 || qualifyingRevision > revision || cooldown < 0 {
		return false, ErrNotificationInput
	}
	claimed := false
	err := s.WithTransaction(ctx, func(ctx context.Context) error {
		db := s.db.DB(ctx)
		var entry models.Notification
		if err := db.Select("type", "group_key", "organization_id", "user_id", "reusable_group").Where("user_id = ? AND id = ?", userID, id).Take(&entry).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return nil
			}
			return err
		}
		if entry.GroupKey != "" {
			// Agree with Record's advisory-before-row lock order. Claim history
			// belongs to the logical group and survives reading an older episode.
			if err := lockNotificationRecipients(db, []NotificationRecipientType{{UserID: userID, Type: entry.Type}}); err != nil {
				return err
			}
		}
		if err := lockNotificationGroups(db, []models.NotificationGroupState{notificationGroupIdentity(entry)}); err != nil {
			return err
		}
		cutoff := now.Add(-cooldown)
		claim := db.Model(&models.Notification{}).Where("user_id = ? AND id = ? AND revision = ? AND read_revision < ? AND resolved = false AND alert_claimed_revision < ?", userID, id, revision, qualifyingRevision, qualifyingRevision).Where("alert_claimed_at IS NULL OR alert_claimed_at <= ?", cutoff).Where("NOT " + sourceResolutionPendingSQL(models.Notification{}.TableName()+".id"))
		if entry.GroupKey != "" {
			recent := db.Model(&models.Notification{}).Select("1").Where("organization_id = ? AND user_id = ? AND type = ? AND group_key = ? AND group_key <> '' AND alert_claimed_at > ?", entry.OrganizationID, userID, entry.Type, entry.GroupKey, cutoff)
			claim = claim.Where("NOT EXISTS (?)", recent)
		}
		res := claim.Select("alert_claimed_revision", "alert_claimed_at").Updates(&models.Notification{AlertClaimedRevision: revision, AlertClaimedAt: &now})
		claimed = res.RowsAffected == 1
		return res.Error
	})
	return claimed, err
}

// ResolveSource resolves each recipient's entry only while this source is its
// latest occurrence. Resolving an older grouped source cannot dismiss newer
// activity. Payload-only Update revisions do not change source identity. The
// recipient set may have changed since emission; read state is preserved.
// More than pageSize entries persist a resolution job instead of settling
// inline; the job pages by ID cursor, so its pages may use another size.
func (s *GormNotificationStore[U]) ResolveSource(ctx context.Context, typeID uint32, sourceKey string, pageSize int) ([]models.Notification, error) {
	return s.resolveScopedSource(ctx, 0, typeID, sourceKey, pageSize)
}

func (s *GormNotificationStore[U]) resolveScopedSource(ctx context.Context, organizationID int64, typeID uint32, sourceKey string, pageSize int) ([]models.Notification, error) {
	if !validSourceResolutionPageSize(pageSize) {
		return nil, ErrNotificationInput
	}
	return s.resolveScopedSourcePage(ctx, organizationID, typeID, sourceKey, 0, 0, pageSize, true)
}

func validSourceResolutionPageSize(pageSize int) bool {
	return pageSize >= 1 && pageSize <= config.MaxNotificationSourceResolutionPageSize
}

func (s *GormNotificationStore[U]) resolveScopedSourcePage(ctx context.Context, organizationID int64, typeID uint32, sourceKey string, afterID, throughID uint64, pageSize int, enqueue bool) ([]models.Notification, error) {
	var entries []models.Notification
	err := s.WithTransaction(ctx, func(ctx context.Context) error {
		db := s.db.DB(ctx)
		if err := s.cancelNotificationEmissionsForSource(ctx, organizationID, typeID, sourceKey); err != nil {
			return err
		}
		targets := db.Model(&models.NotificationOccurrence{}).Select("notification_id").Where("organization_id = ? AND type = ? AND source_event_key = ? AND disposition <> ?", organizationID, typeID, sourceKey, NotificationOccurrenceAcknowledged)
		var identities []models.Notification
		q := db.Select("id", "user_id", "type", "organization_id", "group_key", "reusable_group").Where("id IN (?) AND id > ?", targets, afterID).Order("id")
		if throughID != 0 {
			q = q.Where("id <= ?", throughID)
		}
		if err := q.Limit(pageSize + 1).Find(&identities).Error; err != nil {
			return err
		}
		if enqueue && len(identities) > pageSize {
			var upper uint64
			if err := db.Model(&models.Notification{}).Select("COALESCE(MAX(id),0)").Where("id IN (?)", targets).Scan(&upper).Error; err != nil {
				return err
			}
			job := models.NotificationSourceResolution{OrganizationID: organizationID, Type: typeID, SourceKey: sourceKey, ThroughID: upper, DueAt: time.Now()}
			reset := models.NotificationSourceResolution{ThroughID: upper}
			return db.Clauses(clause.OnConflict{
				Columns: []clause.Column{{Name: "organization_id"}, {Name: "type"}, {Name: "source_key"}},
				DoUpdates: clause.Set{
					{Column: clause.Column{Name: "after_id"}, Value: reset.AfterID},
					{Column: clause.Column{Name: "through_id"}, Value: reset.ThroughID},
					{Column: clause.Column{Name: "due_at"}, Value: gorm.Expr("now()")},
					{Column: clause.Column{Name: "attempts"}, Value: reset.Attempts},
					{Column: clause.Column{Name: "last_error"}, Value: reset.LastError},
					{Column: clause.Column{Name: "exhausted_at"}, Value: reset.ExhaustedAt},
					{Column: clause.Column{Name: "updated_at"}, Value: gorm.Expr("now()")},
				},
			}).Create(&job).Error
		}
		if len(identities) > pageSize {
			identities = identities[:pageSize]
		}
		recipients := make([]NotificationRecipientType, 0, len(identities))
		groups := make([]models.NotificationGroupState, 0, len(identities))
		identityIDs := make([]uint64, 0, len(identities))
		for _, entry := range identities {
			recipients = append(recipients, NotificationRecipientType{UserID: entry.UserID, Type: entry.Type})
			groups = append(groups, notificationGroupIdentity(entry))
			identityIDs = append(identityIDs, entry.ID)
		}
		if err := lockNotificationRecipients(db, recipients); err != nil {
			return err
		}
		snapshot, err := s.CaptureNotificationSnapshot(ctx)
		if err != nil {
			return err
		}
		if err := lockNotificationGroups(db, groups); err != nil {
			return err
		}
		var locked []models.Notification
		// Never expand beyond the identities whose advisory and group locks we
		// acquired. An inline emission for another recipient can commit after the
		// discovery read; rerunning targets here would invert the lock order for
		// that new recipient. Keep the latest-source check as a fresh statement
		// after locking entries so concurrent commits cannot dismiss newer work.
		if err := db.Select("id").Clauses(clause.Locking{Strength: "UPDATE"}).Where("id IN ? AND resolved = false", identityIDs).Order("id ASC").Find(&locked).Error; err != nil {
			return err
		}
		if len(locked) == 0 {
			return nil
		}
		ids := make([]uint64, len(locked))
		for i := range locked {
			ids[i] = locked[i].ID
		}
		latest := db.Model(&models.NotificationOccurrence{}).Select("notification_id").Where("organization_id = ? AND type = ? AND source_event_key = ? AND disposition <> ?", organizationID, typeID, sourceKey, NotificationOccurrenceAcknowledged).Where("revision = (SELECT MAX(newest.revision) FROM gocommon_notification_occurrences AS newest WHERE newest.notification_id = gocommon_notification_occurrences.notification_id AND newest.disposition <> ?)", NotificationOccurrenceAcknowledged)
		var advanced []models.Notification
		if err := db.Where("id IN ? AND id IN (?) AND resolved = false", ids, latest).Order("id").Find(&advanced).Error; err != nil {
			return err
		}
		if len(advanced) == 0 {
			return nil
		}
		resolvedIDs := make([]uint64, len(advanced))
		for i := range advanced {
			resolvedIDs[i] = advanced[i].ID
			advanced[i].ReadRevision = advanced[i].Revision
		}
		if err := saveNotificationAcknowledgments(db, advanced, snapshot); err != nil {
			return err
		}
		return db.Model(&entries).Clauses(clause.Returning{}).Where("id IN ? AND resolved = false", resolvedIDs).Update("resolved", true).Error
	})
	if err == nil {
		s.queueChanged(ctx, NotificationQueueRetirement, NotificationQueueDelivery, NotificationQueueFanout)
	}
	return entries, err
}

// Update replaces current content and advances the observed revision without
// creating an occurrence. The owner predicate prevents cross-recipient changes.
// Reopening a previously read grouped row conflicts if a newer unread episode
// already exists. Producers should Record a new source event for that episode.
func (s *GormNotificationStore[U]) Update(ctx context.Context, userID, id uint64, payload []byte, fallback string) (*models.Notification, error) {
	var entry *models.Notification
	err := s.WithTransaction(ctx, func(ctx context.Context) error {
		db := s.db.DB(ctx)
		var identity models.Notification
		if err := db.Select("type", "organization_id", "user_id", "group_key", "reusable_group").Where("user_id = ? AND id = ?", userID, id).Take(&identity).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return nil
			}
			return err
		}
		// Payload updates and source recording must agree on the grouping lock;
		// otherwise a read row can reopen between Record's lookup and insertion.
		if err := lockNotificationRecipients(db, []NotificationRecipientType{{UserID: userID, Type: identity.Type}}); err != nil {
			return err
		}
		if err := lockNotificationGroups(db, []models.NotificationGroupState{notificationGroupIdentity(identity)}); err != nil {
			return err
		}
		if err := db.Clauses(clause.Locking{Strength: "UPDATE"}).Where("user_id = ? AND id = ?", userID, id).Take(&identity).Error; err != nil {
			return err
		}
		if identity.ReusableGroup && (identity.Resolved || identity.ReadRevision >= identity.Revision) {
			// Only a new source occurrence can start an episode and schedule its
			// delivery. A content edit must never reopen a retired/read group.
			return ErrNotificationEpisodeConflict
		}
		var updated models.Notification
		updates := struct {
			Payload      []byte `gorm:"type:bytea"`
			FallbackText string
			Revision     clause.Expr `gorm:"type:bigint"`
			UpdatedAt    time.Time
		}{Payload: payload, FallbackText: fallback, Revision: gorm.Expr("revision + 1")}
		res := db.Model(&updated).Clauses(clause.Returning{}).Where("user_id = ? AND id = ?", userID, id).Select("payload", "fallback_text", "revision").Updates(&updates)
		if res.RowsAffected > 0 {
			entry = &updated
		}
		return res.Error
	})
	if err != nil {
		if isUniqueViolationOn(err, "idx_notification_unread_episode") {
			return nil, ErrNotificationEpisodeConflict
		}
		return nil, err
	}
	return entry, nil
}

// ensureNotificationIndexes installs PostgreSQL predicates that GORM tags cannot
// express cleanly. Schema setup serializes this with the rest of shared migration.
func notificationIndexStatements() []string {
	return []string{
		"CREATE UNIQUE INDEX IF NOT EXISTS idx_notification_unread_episode ON gocommon_notifications (user_id, type, organization_id, group_key) WHERE group_key <> '' AND revision > read_revision AND resolved = false",
		"CREATE UNIQUE INDEX IF NOT EXISTS idx_notification_reusable_group ON gocommon_notifications (user_id, type, organization_id, group_key) WHERE reusable_group = true AND group_key <> ''",
		"CREATE INDEX IF NOT EXISTS idx_notification_owner_page ON gocommon_notifications (user_id, id DESC)",
		"CREATE INDEX IF NOT EXISTS idx_notification_unread_page ON gocommon_notifications (user_id, id DESC) WHERE revision > read_revision AND resolved = false",
		"CREATE INDEX IF NOT EXISTS idx_notification_group_alert ON gocommon_notifications (user_id, type, organization_id, group_key, alert_claimed_at DESC) WHERE group_key <> '' AND alert_claimed_at IS NOT NULL",
		"CREATE INDEX IF NOT EXISTS idx_notification_scope_unread_page ON gocommon_notifications (user_id, activity_scope, id DESC) WHERE revision > read_revision AND resolved = false",
		"CREATE INDEX IF NOT EXISTS idx_notification_source ON gocommon_notification_occurrences (organization_id, type, source_event_key)",
		// The attempt budget is hot policy, so it belongs in the index key,
		// not a fixed partial predicate. Recovery orders by the same keys.
		"CREATE INDEX IF NOT EXISTS idx_notification_delivery_recovery ON gocommon_notification_deliveries (attempts, due_at, id) WHERE state = 'pending'",
		"CREATE INDEX IF NOT EXISTS idx_notification_delivery_deadline ON gocommon_notification_deliveries (GREATEST(due_at, lease_expires_at), id) WHERE state = 'pending'",
		// The composite also serves notification_id-prefix lookups.
		"CREATE INDEX IF NOT EXISTS idx_notification_occurrence_latest ON gocommon_notification_occurrences (notification_id, revision DESC)",
		"CREATE INDEX IF NOT EXISTS idx_notification_ack_highwater ON gocommon_notification_group_states (ack_epoch, ack_upper_bound DESC)",
		"CREATE INDEX IF NOT EXISTS idx_notification_retirement_highwater ON gocommon_notification_retirement_cutoffs (cutoff_epoch, cutoff_upper_bound DESC)",
		"CREATE INDEX IF NOT EXISTS idx_notification_scope_fence_highwater ON gocommon_notification_scope_retirement_fences (cutoff_epoch, cutoff_upper_bound DESC)",
		// Scope-wide retirement pages walk one scope's reusable entries in
		// (user, type, id) order, including continuation inside one pair.
		"CREATE INDEX IF NOT EXISTS idx_notification_scope_retirement_entries ON gocommon_notifications (activity_scope, user_id, type, id) WHERE reusable_group = true",
		"CREATE INDEX IF NOT EXISTS idx_notification_materialization_highwater ON gocommon_notifications (materialization_epoch, materialization_xid DESC)",
	}
}

func ensureNotificationIndexes(db *gorm.DB) error {
	return db.Transaction(func(db *gorm.DB) error {
		for _, statement := range notificationIndexStatements() {
			if err := executeSharedDDL(db, statement); err != nil {
				return err
			}
		}
		return ensureNotificationOwnSnapshotIndexes(db)
	})
}
