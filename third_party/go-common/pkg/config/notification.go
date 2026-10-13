package config

import (
	"fmt"
	"strconv"
	"time"
)

// NotificationPolicy controls interruptions, independently of inbox history.
type NotificationPolicy struct {
	AlertsEnabled      bool          `json:"alerts_enabled"`
	AlertCooldown      time.Duration `json:"alert_cooldown"`
	EmailMode          string        `json:"email_mode"` // disabled, immediate, delayed
	EmailDelay         time.Duration `json:"email_delay"`
	GroupWhileUnread   bool          `json:"group_while_unread"`
	CancelOnRead       bool          `json:"cancel_on_read"`
	CancelOnResolution bool          `json:"cancel_on_resolution"`
}

func (p NotificationPolicy) Validate() error {
	if p.EmailMode != "disabled" && p.EmailMode != "immediate" && p.EmailMode != "delayed" {
		return fmt.Errorf("invalid notification email mode %q", p.EmailMode)
	}
	if p.AlertCooldown < 0 || p.EmailDelay < 0 {
		return fmt.Errorf("notification delays must be nonnegative")
	}
	if p.EmailMode == "delayed" && p.EmailDelay <= 0 {
		return fmt.Errorf("delayed notification email requires a positive delay")
	}
	return nil
}

func validateNotificationPolicies(policies map[string]NotificationPolicy) error {
	for key, policy := range policies {
		id, err := strconv.ParseUint(key, 10, 32)
		if err != nil || id == 0 || strconv.FormatUint(id, 10) != key {
			return fmt.Errorf("notification policy key %q must be a canonical positive type ID", key)
		}
		if err := policy.Validate(); err != nil {
			return fmt.Errorf("notification policy type %s: %w", key, err)
		}
	}
	return nil
}

const (
	DefaultNotificationFanoutWorkerCount = 1
	MaxNotificationFanoutWorkerCount     = 64
	DefaultNotificationFanoutStepBudget  = 10 * time.Second
	MinNotificationFanoutStepBudget      = time.Second
	MaxNotificationFanoutStepBudget      = time.Minute
	DefaultNotificationFanoutLease       = 2 * time.Minute
	// NotificationFanoutLeaseMargin is the minimum headroom a lease keeps beyond
	// the step budget so a step that spends its whole budget finishes on a live lease.
	NotificationFanoutLeaseMargin                = 30 * time.Second
	MaxNotificationFanoutLease                   = 30 * time.Minute
	DefaultNotificationFanoutBatchRecipients     = 100
	DefaultNotificationFanoutMaintenanceInterval = 5 * time.Minute
	MinNotificationFanoutMaintenanceInterval     = time.Second
	MaxNotificationFanoutMaintenanceInterval     = 5 * time.Minute
	DefaultNotificationFanoutPollInterval        = time.Second
	MinNotificationFanoutPollInterval            = 100 * time.Millisecond
	MaxNotificationFanoutPollInterval            = 30 * time.Second
	DefaultNotificationFanoutMaxAttempts         = 5
	MaxNotificationFanoutMaxAttempts             = 20
	DefaultNotificationChatRetirementPageSize    = 100
	MaxNotificationChatRetirementPageSize        = 1000
	DefaultNotificationSourceResolutionPageSize  = 64
	MaxNotificationSourceResolutionPageSize      = 1000
	DefaultNotificationRetirementClaimCandidates = 32
	MaxNotificationRetirementClaimCandidates     = 1000
	// One advisory lock per unique recipient/type is held for a retirement's
	// whole transaction; the default leaves most of PostgreSQL's default
	// 6,400-slot shared lock table to concurrent sessions. The minimum keeps
	// native chat's two-type single departure and its pages retirable.
	DefaultNotificationRetirementMaxRecipients   = 2000
	MinNotificationRetirementMaxRecipients       = 100
	MaxNotificationRetirementMaxRecipients       = 100000
	DefaultNotificationFanoutCompactionRetention = 30 * 24 * time.Hour
	MinNotificationFanoutCompactionRetention     = 24 * time.Hour
	DefaultNotificationFanoutCompactionBatch     = 100
	MaxNotificationFanoutCompactionBatch         = 1000
	// Each backlog sample counts at most this many pending emissions.
	DefaultNotificationFanoutBacklogCountLimit = 100000
	MinNotificationFanoutBacklogCountLimit     = 1000
	MaxNotificationFanoutBacklogCountLimit     = 10000000
	// Inline event emission enumerates at most this many producer snapshot pages
	// inside the source transaction.
	DefaultNotificationInlineSnapshotMaxPages = 1000
	MaxNotificationInlineSnapshotMaxPages     = 10000
	// MaxNotificationFanoutBatchRecipients is the per-page candidate cap shared
	// with EmitMany and producer snapshot pages; one step never exceeds one page.
	MaxNotificationFanoutBatchRecipients     = 1000
	DefaultNotificationInboxFetchRows        = 16
	DefaultNotificationListCandidateBudget   = 500
	DefaultNotificationCountOperationTimeout = time.Second
	DefaultNotificationCountUnreadLimit      = 100
	DefaultNotificationCountCandidateBudget  = 500
	// MaxNotificationInboxFetchRows bounds hydration to 16 MiB at 1 MiB per payload.
	MaxNotificationInboxFetchRows           = 16
	DefaultNotificationListDefaultLimit     = 30
	DefaultNotificationListMaxLimit         = 100
	DefaultNotificationMaxReadReceipts      = 100
	DefaultNotificationMaxPreferenceUpdates = 100
	// MaxNotificationListLimit bounds one inbox page; pages are also bounded by
	// the list candidate budget and hydrated in fetch-row chunks.
	MaxNotificationListLimit = 1000
	// MaxNotificationReadReceipts bounds the receipts written in one transaction.
	MaxNotificationReadReceipts = 1000
	// MaxNotificationPreferenceUpdates bounds the preference rows written in one transaction.
	MaxNotificationPreferenceUpdates = 1000
)

// requiredCount returns the value of a key every consumer reads as-is: the key
// must be set and lie in [min, max]. A non-positive max leaves the key
// unbounded above.
func requiredCount(name string, value *int, min, max int) (int, error) {
	if value == nil {
		return 0, fmt.Errorf("%s is required", name)
	}
	if *value < min || (max > 0 && *value > max) {
		if max > 0 {
			return 0, fmt.Errorf("%s must be between %d and %d, got: %d", name, min, max, *value)
		}
		return 0, fmt.Errorf("%s must be at least %d, got: %d", name, min, *value)
	}
	return *value, nil
}

// requiredDuration is requiredCount for durations.
func requiredDuration(name string, value *time.Duration, min, max time.Duration) (time.Duration, error) {
	if value == nil {
		return 0, fmt.Errorf("%s is required", name)
	}
	if *value < min || (max > 0 && *value > max) {
		if max > 0 {
			return 0, fmt.Errorf("%s must be between %s and %s, got: %s", name, min, max, *value)
		}
		return 0, fmt.Errorf("%s must be at least %s, got: %s", name, min, *value)
	}
	return *value, nil
}

// requiredCounts checks every entry with requiredCount.
func requiredCounts(entries []struct {
	name     string
	value    *int
	min, max int
}) error {
	for _, e := range entries {
		if _, err := requiredCount(e.name, e.value, e.min, e.max); err != nil {
			return err
		}
	}
	return nil
}

// requiredDurations checks every entry with requiredDuration.
func requiredDurations(entries []struct {
	name     string
	value    *time.Duration
	min, max time.Duration
}) error {
	for _, e := range entries {
		if _, err := requiredDuration(e.name, e.value, e.min, e.max); err != nil {
			return err
		}
	}
	return nil
}

func (c *Config) validateNotificationRequestLimits() error {
	def, err := requiredCount("notification_list_default_limit", c.NotificationListDefaultLimit, 1, MaxNotificationListLimit)
	if err != nil {
		return err
	}
	maxLimit, err := requiredCount("notification_list_max_limit", c.NotificationListMaxLimit, 1, MaxNotificationListLimit)
	if err != nil {
		return err
	}
	if err := requiredCounts([]struct {
		name     string
		value    *int
		min, max int
	}{
		{"notification_max_read_receipts", c.NotificationMaxReadReceipts, 1, MaxNotificationReadReceipts},
		{"notification_max_preference_updates", c.NotificationMaxPreferenceUpdates, 1, MaxNotificationPreferenceUpdates},
	}); err != nil {
		return err
	}
	if def > maxLimit {
		return fmt.Errorf("notification_list_default_limit (%d) must not exceed notification_list_max_limit (%d)", def, maxLimit)
	}
	return nil
}

func (c *Config) validateNotificationFanout() error {
	for _, setting := range []struct{ name, mode string }{
		{"notification_chat_unread_fanout_mode", c.NotificationChatUnreadFanoutMode},
		{"notification_chat_approval_required_fanout_mode", c.NotificationChatApprovalRequiredFanoutMode},
	} {
		if setting.mode != "" && setting.mode != "inline" && setting.mode != "async" {
			return fmt.Errorf("%s must be inline, async, or empty", setting.name)
		}
	}
	if err := requiredCounts([]struct {
		name     string
		value    *int
		min, max int
	}{
		{"notification_fanout_worker_count", c.NotificationFanoutWorkerCount, 1, MaxNotificationFanoutWorkerCount},
		{"notification_fanout_batch_recipients", c.NotificationFanoutBatchRecipients, 1, MaxNotificationFanoutBatchRecipients},
		{"notification_fanout_max_attempts", c.NotificationFanoutMaxAttempts, 1, MaxNotificationFanoutMaxAttempts},
		{"notification_chat_retirement_page_size", c.NotificationChatRetirementPageSize, 1, MaxNotificationChatRetirementPageSize},
		{"notification_source_resolution_page_size", c.NotificationSourceResolutionPageSize, 1, MaxNotificationSourceResolutionPageSize},
		{"notification_retirement_claim_candidates", c.NotificationRetirementClaimCandidates, 1, MaxNotificationRetirementClaimCandidates},
		{"notification_fanout_compaction_batch", c.NotificationFanoutCompactionBatch, 1, MaxNotificationFanoutCompactionBatch},
		{"notification_inline_snapshot_max_pages", c.NotificationInlineSnapshotMaxPages, 1, MaxNotificationInlineSnapshotMaxPages},
		{"notification_retirement_max_recipients", c.NotificationRetirementMaxRecipients, MinNotificationRetirementMaxRecipients, MaxNotificationRetirementMaxRecipients},
		{"notification_fanout_backlog_count_limit", c.NotificationFanoutBacklogCountLimit, MinNotificationFanoutBacklogCountLimit, MaxNotificationFanoutBacklogCountLimit},
	}); err != nil {
		return err
	}
	if err := requiredDurations([]struct {
		name     string
		value    *time.Duration
		min, max time.Duration
	}{
		{"notification_fanout_maintenance_interval", c.NotificationFanoutMaintenanceInterval, MinNotificationFanoutMaintenanceInterval, MaxNotificationFanoutMaintenanceInterval},
		{"notification_fanout_poll_interval", c.NotificationFanoutPollInterval, MinNotificationFanoutPollInterval, MaxNotificationFanoutPollInterval},
		{"notification_fanout_compaction_retention", c.NotificationFanoutCompactionRetention, MinNotificationFanoutCompactionRetention, 0},
	}); err != nil {
		return err
	}
	budget, err := requiredDuration("notification_fanout_step_budget", c.NotificationFanoutStepBudget, MinNotificationFanoutStepBudget, MaxNotificationFanoutStepBudget)
	if err != nil {
		return err
	}
	lease, err := requiredDuration("notification_fanout_lease", c.NotificationFanoutLease, NotificationFanoutLeaseMargin+MinNotificationFanoutStepBudget, MaxNotificationFanoutLease)
	if err != nil {
		return err
	}
	// A lease must outlive the step it protects.
	if lease < budget+NotificationFanoutLeaseMargin {
		return fmt.Errorf("notification_fanout_lease (%s) must be at least notification_fanout_step_budget (%s) plus %s", lease, budget, NotificationFanoutLeaseMargin)
	}
	return nil
}

func (c *Config) validateNotificationInbox() error {
	if err := requiredCounts([]struct {
		name     string
		value    *int
		min, max int
	}{
		{"notification_inbox_fetch_rows", c.NotificationInboxFetchRows, 1, MaxNotificationInboxFetchRows},
		{"notification_list_candidate_budget", c.NotificationListCandidateBudget, 1, 0},
		{"notification_count_unread_limit", c.NotificationCountUnreadLimit, 1, 0},
		{"notification_count_candidate_budget", c.NotificationCountCandidateBudget, 1, 0},
	}); err != nil {
		return err
	}
	_, err := requiredDuration("notification_count_operation_timeout", c.NotificationCountOperationTimeout, time.Nanosecond, 0)
	return err
}

// Delivery worker, retry, emission byte, and chat retirement bounds. Each
// Default* equals the literal the consumer used before the key existed; every
// key is required and must lie inside [Min*, Max*].
const (
	DefaultNotificationDeliveryWorkerCount = 4
	MaxNotificationDeliveryWorkerCount     = 64

	DefaultNotificationDeliveryPollInterval = time.Second
	MinNotificationDeliveryPollInterval     = 100 * time.Millisecond
	MaxNotificationDeliveryPollInterval     = 30 * time.Second
	// The claim error backoff must also be at least the effective poll interval.
	DefaultNotificationDeliveryClaimErrorMaxBackoff = 30 * time.Second
	MinNotificationDeliveryClaimErrorMaxBackoff     = time.Second
	MaxNotificationDeliveryClaimErrorMaxBackoff     = 5 * time.Minute
	DefaultNotificationWorkerShutdownTimeout        = 5 * time.Second
	MinNotificationWorkerShutdownTimeout            = time.Second
	MaxNotificationWorkerShutdownTimeout            = 5 * time.Minute
	DefaultNotificationDeliveryClaimTimeout         = 10 * time.Second
	MinNotificationDeliveryClaimTimeout             = time.Second
	MaxNotificationDeliveryClaimTimeout             = time.Minute
	// The delivery lease must also be at least the effective work budget plus
	// the effective completion margin.
	DefaultNotificationDeliveryLease       = 2 * time.Minute
	MinNotificationDeliveryLease           = time.Second
	MaxNotificationDeliveryLease           = 30 * time.Minute
	DefaultNotificationDeliveryMaxAttempts = 5
	// MaxNotificationDeliveryMaxAttempts shares the fan-out attempt ceiling.
	MaxNotificationDeliveryMaxAttempts               = MaxNotificationFanoutMaxAttempts
	DefaultNotificationDeliveryPollJitterFraction    = 0.2
	MaxNotificationDeliveryPollJitterFraction        = 1.0
	DefaultNotificationDeliveryWorkBudget            = 10 * time.Second
	MinNotificationDeliveryWorkBudget                = time.Second
	MaxNotificationDeliveryWorkBudget                = 10 * time.Minute
	DefaultNotificationDeliveryLeaseCompletionMargin = 5 * time.Second
	MinNotificationDeliveryLeaseCompletionMargin     = time.Second
	MaxNotificationDeliveryLeaseCompletionMargin     = time.Minute
	DefaultNotificationDeliveryRetryBackoffBase      = time.Second
	MinNotificationDeliveryRetryBackoffBase          = 100 * time.Millisecond
	MaxNotificationDeliveryRetryBackoffBase          = time.Minute
	// The retry backoff maximum must also be at least the effective base.
	DefaultNotificationDeliveryRetryBackoffMax      = 64 * time.Second
	MaxNotificationDeliveryRetryBackoffMax          = time.Hour
	DefaultNotificationDeliveryActiveViewDefer      = 30 * time.Second
	MinNotificationDeliveryActiveViewDefer          = time.Second
	MaxNotificationDeliveryActiveViewDefer          = 10 * time.Minute
	DefaultNotificationDeliveryDisabledDefer        = 30 * time.Second
	MinNotificationDeliveryDisabledDefer            = time.Second
	MaxNotificationDeliveryDisabledDefer            = 10 * time.Minute
	DefaultNotificationDeliveryTypeUnavailableDefer = 30 * time.Second
	MinNotificationDeliveryTypeUnavailableDefer     = time.Second
	MaxNotificationDeliveryTypeUnavailableDefer     = 10 * time.Minute
	DefaultNotificationDeliveryRevisionChangedDefer = time.Second
	MinNotificationDeliveryRevisionChangedDefer     = 100 * time.Millisecond
	MaxNotificationDeliveryRevisionChangedDefer     = time.Minute
	DefaultNotificationAlertMinAge                  = time.Second
	MinNotificationAlertMinAge                      = time.Millisecond
	MaxNotificationAlertMinAge                      = 5 * time.Second
	// The alert max age must also exceed the effective min age.
	DefaultNotificationAlertMaxAge                        = 30 * time.Second
	MinNotificationAlertMaxAge                            = time.Second
	MaxNotificationAlertMaxAge                            = 10 * time.Minute
	DefaultNotificationDeliveryCompletionTimeout          = 5 * time.Second
	MinNotificationDeliveryCompletionTimeout              = time.Second
	MaxNotificationDeliveryCompletionTimeout              = time.Minute
	DefaultNotificationDeliveryCompletionRetryBackoffBase = 50 * time.Millisecond
	MinNotificationDeliveryCompletionRetryBackoffBase     = 10 * time.Millisecond
	MaxNotificationDeliveryCompletionRetryBackoffBase     = 5 * time.Second
	// The completion retry maximum must also be at least the effective base.
	DefaultNotificationDeliveryCompletionRetryBackoffMax = 500 * time.Millisecond
	MaxNotificationDeliveryCompletionRetryBackoffMax     = 30 * time.Second

	// The fan-out claim error backoff must also be at least the effective
	// notification_fanout_poll_interval.
	DefaultNotificationFanoutClaimErrorMaxBackoff = 30 * time.Second
	MinNotificationFanoutClaimErrorMaxBackoff     = time.Second
	MaxNotificationFanoutClaimErrorMaxBackoff     = 5 * time.Minute
	DefaultNotificationFanoutCompactionInterval   = 5 * time.Minute
	MinNotificationFanoutCompactionInterval       = time.Second
	MaxNotificationFanoutCompactionInterval       = time.Hour
	DefaultNotificationFanoutRetryBackoffBase     = time.Second
	MinNotificationFanoutRetryBackoffBase         = 100 * time.Millisecond
	MaxNotificationFanoutRetryBackoffBase         = time.Minute
	// The fan-out retry maximum must also be at least the effective base.
	DefaultNotificationFanoutRetryBackoffMax    = 64 * time.Second
	MaxNotificationFanoutRetryBackoffMax        = time.Hour
	DefaultNotificationFanoutDeferredRetryDelay = 30 * time.Second
	MinNotificationFanoutDeferredRetryDelay     = time.Second
	MaxNotificationFanoutDeferredRetryDelay     = 10 * time.Minute
	DefaultNotificationFanoutFinishTimeout      = 5 * time.Second
	MinNotificationFanoutFinishTimeout          = time.Second
	MaxNotificationFanoutFinishTimeout          = time.Minute

	DefaultNotificationSourceBatchRows = 64
	// MaxNotificationSourceBatchRows is also the persistence ceiling for one
	// organization-member check.
	MaxNotificationSourceBatchRows = 1000
	// The source batch byte budget must also hold one effective max payload.
	DefaultNotificationSourceBatchBytes = 4 << 20
	MaxNotificationSourceBatchBytes     = 16 << 20
	DefaultNotificationMaxPayloadBytes  = 1 << 20
	// MaxNotificationPayloadBytes is pinned so MaxNotificationInboxFetchRows
	// rows hydrate within 16 MiB.
	MaxNotificationPayloadBytes             = 1 << 20
	DefaultNotificationMaxFallbackTextBytes = 4096
	MaxNotificationFallbackTextBytes        = 4096
	DefaultNotificationMaxMetadataBytes     = 64 << 10
	MaxNotificationMetadataBytes            = 64 << 10
	// The EmitMany byte budget must also hold one effective maximal input
	// (payload + fallback text + delivery tag).
	DefaultNotificationEmitMaxTotalBytes = 16 << 20
	MaxNotificationEmitTotalBytes        = 16 << 20
	// MaxNotificationProducerCursorBytes caps one opaque producer resume cursor;
	// it is a protocol size shared by the service and the stored cursor, not a key.
	MaxNotificationProducerCursorBytes = 64 << 10
	// MaxNotificationDeliveryTagBytes bounds delivery tags and filters. It is a
	// schema width, not a key: it matches the varchar(64) DeliveryTag and
	// WaitingTag columns on occurrences, deliveries, and fan-out recipients.
	MaxNotificationDeliveryTagBytes = 64
	// MaxNotificationEmissionKeyBytes bounds an event's optional emission key.
	// It is a schema width, not a key: it matches the varchar(64) EmissionKey
	// column on fan-out emissions.
	MaxNotificationEmissionKeyBytes = 64
	// The following are schema widths, not keys: each matches the varchar
	// column its value is stored in, so widening one needs a SQL migration.
	// MaxNotificationProducerBytes matches the varchar(128) Producer column.
	MaxNotificationProducerBytes = 128
	// MaxNotificationSourceKeyBytes matches the varchar(256) SourceKey and
	// SourceEventKey columns.
	MaxNotificationSourceKeyBytes = 256
	// MaxNotificationGroupKeyBytes matches the varchar(256) fence GroupKey column.
	MaxNotificationGroupKeyBytes = 256
	// MaxNotificationRetirementScopeBytes matches the varchar(256) retirement
	// ActivityScope columns; scope grammar is stricter (notificationkey).
	MaxNotificationRetirementScopeBytes = 256
	// MaxNotificationOperationIDBytes matches the varchar(36) UUID OperationID column.
	MaxNotificationOperationIDBytes = 36
	// MaxNotificationTraceIDBytes matches the varchar(128) TraceID column.
	MaxNotificationTraceIDBytes = 128

	DefaultNotificationChatRetirementMaxAttempts      = 5
	MaxNotificationChatRetirementMaxAttempts          = 20
	DefaultNotificationChatRetirementExhaustedDelay   = time.Hour
	MinNotificationChatRetirementExhaustedDelay       = time.Minute
	MaxNotificationChatRetirementExhaustedDelay       = 24 * time.Hour
	DefaultNotificationChatRetirementRetryBackoffBase = time.Second
	MinNotificationChatRetirementRetryBackoffBase     = 100 * time.Millisecond
	MaxNotificationChatRetirementRetryBackoffBase     = time.Minute
	// The retirement retry maximum must also be at least the effective base.
	DefaultNotificationChatRetirementRetryBackoffMax      = 60 * time.Second
	MaxNotificationChatRetirementRetryBackoffMax          = time.Hour
	DefaultNotificationChatRetirementRecordFailureTimeout = 5 * time.Second
	MinNotificationChatRetirementRecordFailureTimeout     = time.Second
	MaxNotificationChatRetirementRecordFailureTimeout     = time.Minute

	// Unread-count route rate limit defaults (rate_limits group).
	DefaultNotificationCountMaxAttempts    = 60
	DefaultNotificationCountWindowDuration = time.Minute
)

func (c *Config) validateNotificationDelivery() error {
	if err := requiredCounts([]struct {
		name     string
		value    *int
		min, max int
	}{
		{"notification_delivery_worker_count", c.NotificationDeliveryWorkerCount, 1, MaxNotificationDeliveryWorkerCount},
		{"notification_delivery_max_attempts", c.NotificationDeliveryMaxAttempts, 1, MaxNotificationDeliveryMaxAttempts},
		{"notification_source_batch_rows", c.NotificationSourceBatchRows, 1, MaxNotificationSourceBatchRows},
		{"notification_max_metadata_bytes", c.NotificationMaxMetadataBytes, 1, MaxNotificationMetadataBytes},
		{"notification_chat_retirement_max_attempts", c.NotificationChatRetirementMaxAttempts, 1, MaxNotificationChatRetirementMaxAttempts},
	}); err != nil {
		return err
	}
	if err := requiredDurations([]struct {
		name     string
		value    *time.Duration
		min, max time.Duration
	}{
		{"notification_worker_shutdown_timeout", c.NotificationWorkerShutdownTimeout, MinNotificationWorkerShutdownTimeout, MaxNotificationWorkerShutdownTimeout},
		{"notification_delivery_claim_timeout", c.NotificationDeliveryClaimTimeout, MinNotificationDeliveryClaimTimeout, MaxNotificationDeliveryClaimTimeout},
		{"notification_delivery_active_view_defer", c.NotificationDeliveryActiveViewDefer, MinNotificationDeliveryActiveViewDefer, MaxNotificationDeliveryActiveViewDefer},
		{"notification_delivery_disabled_defer", c.NotificationDeliveryDisabledDefer, MinNotificationDeliveryDisabledDefer, MaxNotificationDeliveryDisabledDefer},
		{"notification_delivery_type_unavailable_defer", c.NotificationDeliveryTypeUnavailableDefer, MinNotificationDeliveryTypeUnavailableDefer, MaxNotificationDeliveryTypeUnavailableDefer},
		{"notification_delivery_revision_changed_defer", c.NotificationDeliveryRevisionChangedDefer, MinNotificationDeliveryRevisionChangedDefer, MaxNotificationDeliveryRevisionChangedDefer},
		{"notification_delivery_completion_timeout", c.NotificationDeliveryCompletionTimeout, MinNotificationDeliveryCompletionTimeout, MaxNotificationDeliveryCompletionTimeout},
		{"notification_fanout_compaction_interval", c.NotificationFanoutCompactionInterval, MinNotificationFanoutCompactionInterval, MaxNotificationFanoutCompactionInterval},
		{"notification_fanout_deferred_retry_delay", c.NotificationFanoutDeferredRetryDelay, MinNotificationFanoutDeferredRetryDelay, MaxNotificationFanoutDeferredRetryDelay},
		{"notification_fanout_finish_timeout", c.NotificationFanoutFinishTimeout, MinNotificationFanoutFinishTimeout, MaxNotificationFanoutFinishTimeout},
		{"notification_chat_retirement_exhausted_delay", c.NotificationChatRetirementExhaustedDelay, MinNotificationChatRetirementExhaustedDelay, MaxNotificationChatRetirementExhaustedDelay},
		{"notification_chat_retirement_record_failure_timeout", c.NotificationChatRetirementRecordFailureTimeout, MinNotificationChatRetirementRecordFailureTimeout, MaxNotificationChatRetirementRecordFailureTimeout},
	}); err != nil {
		return err
	}
	if v := c.NotificationDeliveryPollJitterFraction; v == nil {
		return fmt.Errorf("notification_delivery_poll_jitter_fraction is required")
	} else if !(*v >= 0 && *v <= MaxNotificationDeliveryPollJitterFraction) { // the negated form also rejects NaN
		return fmt.Errorf("notification_delivery_poll_jitter_fraction must be between 0 and %g, got: %g", MaxNotificationDeliveryPollJitterFraction, *v)
	}

	// Each ordered pair is checked for range first, then the high side must be
	// at least (or, for the alert window, greater than) the low side.
	ordered := []struct {
		lowName, highName string
		low, high         *time.Duration
		lowMin, lowMax    time.Duration
		highMin, highMax  time.Duration
		strict            bool
	}{
		{"notification_delivery_poll_interval", "notification_delivery_claim_error_max_backoff",
			c.NotificationDeliveryPollInterval, c.NotificationDeliveryClaimErrorMaxBackoff,
			MinNotificationDeliveryPollInterval, MaxNotificationDeliveryPollInterval,
			MinNotificationDeliveryClaimErrorMaxBackoff, MaxNotificationDeliveryClaimErrorMaxBackoff, false},
		{"notification_delivery_retry_backoff_base", "notification_delivery_retry_backoff_max",
			c.NotificationDeliveryRetryBackoffBase, c.NotificationDeliveryRetryBackoffMax,
			MinNotificationDeliveryRetryBackoffBase, MaxNotificationDeliveryRetryBackoffBase,
			MinNotificationDeliveryRetryBackoffBase, MaxNotificationDeliveryRetryBackoffMax, false},
		{"notification_alert_min_age", "notification_alert_max_age",
			c.NotificationAlertMinAge, c.NotificationAlertMaxAge,
			MinNotificationAlertMinAge, MaxNotificationAlertMinAge,
			MinNotificationAlertMaxAge, MaxNotificationAlertMaxAge, true},
		{"notification_delivery_completion_retry_backoff_base", "notification_delivery_completion_retry_backoff_max",
			c.NotificationDeliveryCompletionRetryBackoffBase, c.NotificationDeliveryCompletionRetryBackoffMax,
			MinNotificationDeliveryCompletionRetryBackoffBase, MaxNotificationDeliveryCompletionRetryBackoffBase,
			MinNotificationDeliveryCompletionRetryBackoffBase, MaxNotificationDeliveryCompletionRetryBackoffMax, false},
		{"notification_fanout_poll_interval", "notification_fanout_claim_error_max_backoff",
			c.NotificationFanoutPollInterval, c.NotificationFanoutClaimErrorMaxBackoff,
			MinNotificationFanoutPollInterval, MaxNotificationFanoutPollInterval,
			MinNotificationFanoutClaimErrorMaxBackoff, MaxNotificationFanoutClaimErrorMaxBackoff, false},
		{"notification_fanout_retry_backoff_base", "notification_fanout_retry_backoff_max",
			c.NotificationFanoutRetryBackoffBase, c.NotificationFanoutRetryBackoffMax,
			MinNotificationFanoutRetryBackoffBase, MaxNotificationFanoutRetryBackoffBase,
			MinNotificationFanoutRetryBackoffBase, MaxNotificationFanoutRetryBackoffMax, false},
		{"notification_chat_retirement_retry_backoff_base", "notification_chat_retirement_retry_backoff_max",
			c.NotificationChatRetirementRetryBackoffBase, c.NotificationChatRetirementRetryBackoffMax,
			MinNotificationChatRetirementRetryBackoffBase, MaxNotificationChatRetirementRetryBackoffBase,
			MinNotificationChatRetirementRetryBackoffBase, MaxNotificationChatRetirementRetryBackoffMax, false},
	}
	for _, o := range ordered {
		low, err := requiredDuration(o.lowName, o.low, o.lowMin, o.lowMax)
		if err != nil {
			return err
		}
		high, err := requiredDuration(o.highName, o.high, o.highMin, o.highMax)
		if err != nil {
			return err
		}
		if high < low || (o.strict && high == low) {
			relation := "at least"
			if o.strict {
				relation = "greater than"
			}
			return fmt.Errorf("%s (%s) must be %s %s (%s)", o.highName, high, relation, o.lowName, low)
		}
	}
	// A delivery lease must outlive the work it protects plus the completion write.
	budget, err := requiredDuration("notification_delivery_work_budget", c.NotificationDeliveryWorkBudget, MinNotificationDeliveryWorkBudget, MaxNotificationDeliveryWorkBudget)
	if err != nil {
		return err
	}
	margin, err := requiredDuration("notification_delivery_lease_completion_margin", c.NotificationDeliveryLeaseCompletionMargin, MinNotificationDeliveryLeaseCompletionMargin, MaxNotificationDeliveryLeaseCompletionMargin)
	if err != nil {
		return err
	}
	lease, err := requiredDuration("notification_delivery_lease", c.NotificationDeliveryLease, MinNotificationDeliveryLease, MaxNotificationDeliveryLease)
	if err != nil {
		return err
	}
	if lease < budget+margin {
		return fmt.Errorf("notification_delivery_lease (%s) must be at least notification_delivery_work_budget (%s) plus notification_delivery_lease_completion_margin (%s)", lease, budget, margin)
	}
	// Source batches must hold at least one maximal payload.
	payload, err := requiredCount("notification_max_payload_bytes", c.NotificationMaxPayloadBytes, 1, MaxNotificationPayloadBytes)
	if err != nil {
		return err
	}
	sourceBytes, err := requiredCount("notification_source_batch_bytes", c.NotificationSourceBatchBytes, 1, MaxNotificationSourceBatchBytes)
	if err != nil {
		return err
	}
	if sourceBytes < payload {
		return fmt.Errorf("notification_source_batch_bytes (%d) must be at least notification_max_payload_bytes (%d)", sourceBytes, payload)
	}
	// The emission budget charges payload, fallback text, and delivery tag per
	// input, so it must hold one maximal input of each.
	fallback, err := requiredCount("notification_max_fallback_text_bytes", c.NotificationMaxFallbackTextBytes, 1, MaxNotificationFallbackTextBytes)
	if err != nil {
		return err
	}
	total, err := requiredCount("notification_emit_max_total_bytes", c.NotificationEmitMaxTotalBytes, 1, MaxNotificationEmitTotalBytes)
	if err != nil {
		return err
	}
	if minimum := payload + fallback + MaxNotificationDeliveryTagBytes; total < minimum {
		return fmt.Errorf("notification_emit_max_total_bytes (%d) must be at least %d: notification_max_payload_bytes (%d) + notification_max_fallback_text_bytes (%d) + the %d-byte delivery tag maximum", total, minimum, payload, fallback, MaxNotificationDeliveryTagBytes)
	}
	return nil
}
