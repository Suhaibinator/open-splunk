package common_persistence

import (
	"context"
	"errors"
	"fmt"
	"math/bits"
	"slices"
	"time"

	"github.com/Suhaibinator/SRouter/pkg/scontext"
	models "github.com/Suhaibinator/go-common/pkg/common_persistence/common_persistence_models"
	"github.com/Suhaibinator/go-common/pkg/common_util/notificationkey"
	"github.com/Suhaibinator/go-common/pkg/config"
	"github.com/lib/pq"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// NotificationChatRetirementPolicy is one maintenance pass's sample of the hot
// notification_chat_retirement_* keys. Callers build it per pass and pass it
// explicitly; the store never retains it. MaxAttempts consecutive failed pages
// exhaust a job; an exhausted job is retried after ExhaustedDelay (or at once
// when a new deletion re-enqueues it) and fails straight back to exhausted on
// the next error, so it never re-enters fast retry. Earlier failures back off
// from RetryBackoffBase, doubling per attempt, capped at RetryBackoffMax.
type NotificationChatRetirementPolicy struct {
	PageSize             int
	MaxAttempts          int
	ExhaustedDelay       time.Duration
	RetryBackoffBase     time.Duration
	RetryBackoffMax      time.Duration
	RecordFailureTimeout time.Duration
	// MaxRecipients is the notification_retirement_max_recipients sample; a
	// page shrinks so members x job types never exceeds it.
	MaxRecipients int
	// SourceResolutionPageSize is the notification_source_resolution_page_size
	// sample; it further bounds each source resolution job page.
	SourceResolutionPageSize int
	// ClaimCandidates is the notification_retirement_claim_candidates sample;
	// it bounds the due recipient jobs one claim probes past busy recipients.
	ClaimCandidates int
}

// DefaultNotificationChatRetirementPolicy returns the pkg/config defaults; it
// is for tests and direct store callers without a config provider.
func DefaultNotificationChatRetirementPolicy() NotificationChatRetirementPolicy {
	return NotificationChatRetirementPolicy{
		PageSize:             config.DefaultNotificationChatRetirementPageSize,
		MaxRecipients:        config.DefaultNotificationRetirementMaxRecipients,
		MaxAttempts:          config.DefaultNotificationChatRetirementMaxAttempts,
		ExhaustedDelay:       config.DefaultNotificationChatRetirementExhaustedDelay,
		RetryBackoffBase:     config.DefaultNotificationChatRetirementRetryBackoffBase,
		RetryBackoffMax:      config.DefaultNotificationChatRetirementRetryBackoffMax,
		RecordFailureTimeout: config.DefaultNotificationChatRetirementRecordFailureTimeout,

		SourceResolutionPageSize: config.DefaultNotificationSourceResolutionPageSize,
		ClaimCandidates:          config.DefaultNotificationRetirementClaimCandidates,
	}
}

func (p NotificationChatRetirementPolicy) valid() bool {
	return p.PageSize >= 1 && p.PageSize <= config.MaxNotificationChatRetirementPageSize && validRetirementMaxRecipients(p.MaxRecipients) &&
		p.MaxAttempts >= 1 && p.MaxAttempts <= config.MaxNotificationChatRetirementMaxAttempts &&
		p.ExhaustedDelay > 0 && p.RetryBackoffBase > 0 && p.RetryBackoffMax >= p.RetryBackoffBase &&
		p.RecordFailureTimeout > 0 && p.ClaimCandidates >= 1 && p.ClaimCandidates <= config.MaxNotificationRetirementClaimCandidates
}

// backoffDoublings is the attempt count past which base*2^attempts is at least
// max, so the SQL exponent never grows beyond what the cap can observe.
func (p NotificationChatRetirementPolicy) backoffDoublings() int {
	return bits.Len64(uint64(p.RetryBackoffMax / p.RetryBackoffBase))
}

// ErrNotificationChatRetirementExhausted wraps a page failure that exhausted its job.
var ErrNotificationChatRetirementExhausted = errors.New("notification chat retirement exhausted")

// NotificationChatRetirementAdvance describes one Advance call. Advanced means a
// job was claimed and either progressed or had its failure recorded, so the
// caller may immediately claim the next page. On error ChannelID, Stage, and
// Attempts identify the failed job; Stage is a bounded label.
type NotificationChatRetirementAdvance struct {
	Advanced  bool
	ChannelID uint64
	Members   int
	Stage     string
	Attempts  uint32
	Exhausted bool
}

func (s *GormNotificationStore[U]) EnqueueChatNotificationRetirement(ctx context.Context, channelID uint64, scope string, types []uint32) error {
	ambient, ok := scontext.GetTransaction(ctx)
	if !ok || ambient == nil || channelID == 0 || !notificationkey.ValidActivityScope(scope) || len(types) == 0 || len(types) > 2 {
		return ErrNotificationInput
	}
	types = slices.Clone(types)
	slices.Sort(types)
	types = slices.Compact(types)
	values := make(pq.Int64Array, len(types))
	for i, typ := range types {
		if typ == 0 {
			return ErrNotificationInput
		}
		values[i] = int64(typ)
	}
	// A conflicting row is a job this deletion supersedes: restart its sweep and
	// clear failure state. A worker holding the row blocks this upsert only for
	// its current page, whose cursor update is then overwritten.
	err := s.db.DB(ctx).Exec(`INSERT INTO gocommon_notification_chat_retirements
		(channel_id, activity_scope, types, after_user_id, attempts, last_error, exhausted_at, due_at, created_at, updated_at)
		VALUES (?, ?, ?, 0, 0, '', NULL, now(), now(), now())
		ON CONFLICT (channel_id) DO UPDATE SET activity_scope = excluded.activity_scope, types = excluded.types,
		after_user_id = 0, attempts = 0, last_error = '', exhausted_at = NULL, due_at = now(), updated_at = now()`,
		channelID, scope, values).Error
	if err == nil {
		s.queueChanged(ctx, NotificationQueueRetirement)
	}
	return err
}

// AdvanceChatNotificationRetirement uses the pending row as its claim. A crash
// releases that lock and rolls back progress, so no lease or in-memory backlog
// is needed. A fresh channel read never acquires a source row lock. Jobs whose
// due_at is in the future (backed off after a failure) are skipped.
func (s *GormNotificationStore[U]) AdvanceChatNotificationRetirement(ctx context.Context, policy NotificationChatRetirementPolicy, publish func(context.Context, []models.Notification) error) (NotificationChatRetirementAdvance, error) {
	var result NotificationChatRetirementAdvance
	if ambient, ok := scontext.GetTransaction(ctx); ok && ambient != nil {
		// A caller-owned transaction would retain locks across multiple pages.
		return result, ErrNotificationInput
	}
	if !policy.valid() {
		return result, ErrNotificationInput
	}
	limit := policy.PageSize
	var job models.NotificationChatRetirement
	claimed := false
	stage := "claim"
	err := s.WithTransaction(ctx, func(tx context.Context) error {
		db := s.db.DB(tx)
		// Each statement must see rows committed while it waited on a lock: the
		// member and inbox reads after the claim would otherwise use a snapshot
		// taken before the page began, whatever default_transaction_isolation is.
		if err := db.Exec("SET TRANSACTION ISOLATION LEVEL READ COMMITTED").Error; err != nil {
			return err
		}
		if err := db.Where("due_at <= now()").Order("due_at, channel_id").Clauses(clause.Locking{Strength: "UPDATE", Options: "SKIP LOCKED"}).Take(&job).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return nil
			}
			return err
		}
		claimed = true
		// An explicitly restored channel no longer needs deletion cleanup. If a
		// channel is absent, clean up any source membership that remains.
		stage = "channel"
		var active int64
		if err := db.Model(&models.ChatChannel{}).Where("id = ?", job.ChannelID).Count(&active).Error; err != nil {
			return err
		}
		if active != 0 {
			return db.Delete(&job).Error
		}
		stage = "members"
		// A job has at most two types and the cap is at least 100, so a page
		// keeps at least 50 members.
		limit = min(limit, policy.MaxRecipients/max(len(job.Types), 1))
		var ids []uint64
		if err := db.Model(&models.ChatChannelParticipant{}).Where("channel_id = ? AND user_id > ?", job.ChannelID, job.AfterUserID).Order("user_id").Limit(limit).Pluck("user_id", &ids).Error; err != nil {
			return err
		}
		result.Members = len(ids)
		recipients := make([]NotificationRecipientType, 0, len(ids)*len(job.Types))
		for _, id := range ids {
			for _, typ := range job.Types {
				recipients = append(recipients, NotificationRecipientType{UserID: id, Type: uint32(typ)})
			}
		}
		stage = "retire"
		rows, err := s.RetireChatActivityScopeForRecipients(tx, job.ActivityScope, recipients, policy.MaxRecipients)
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
		// Moving due_at to now() rotates this job behind its peers, so several
		// deleted channels progress fairly; a success clears failure state.
		updates := struct {
			AfterUserID uint64
			Attempts    uint32
			LastError   string
			ExhaustedAt *time.Time
			DueAt       clause.Expr `gorm:"type:timestamptz"`
			UpdatedAt   time.Time
		}{AfterUserID: ids[len(ids)-1], DueAt: gorm.Expr("now()")}
		return db.Model(&job).Select("after_user_id", "attempts", "last_error", "exhausted_at", "due_at").Updates(&updates).Error
	})
	if err == nil {
		result.Advanced, result.ChannelID = claimed, job.ChannelID
		return result, nil
	}
	result.Members = 0
	if !claimed {
		return result, err
	}
	result.ChannelID, result.Stage = job.ChannelID, stage
	if errors.Is(ctx.Err(), context.Canceled) {
		// Shutdown is not a job failure; the rolled-back claim is retried later.
		return result, err
	}
	attempts, exhausted, recordErr := recordRetirementFailure(ctx, s, policy, models.NotificationChatRetirement{}.TableName(), "channel_id", job.ChannelID, job.UpdatedAt, stage)
	if recordErr != nil {
		return result, errors.Join(err, fmt.Errorf("record chat retirement failure: %w", recordErr))
	}
	result.Advanced, result.Attempts, result.Exhausted = true, attempts, exhausted
	if exhausted {
		err = fmt.Errorf("%w: %w", ErrNotificationChatRetirementExhausted, err)
	}
	return result, err
}
