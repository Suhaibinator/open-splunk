package notification

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/Suhaibinator/SRouter/pkg/logkeys"
	persistence "github.com/Suhaibinator/go-common/pkg/common_persistence"
	models "github.com/Suhaibinator/go-common/pkg/common_persistence/common_persistence_models"
	"github.com/Suhaibinator/go-common/pkg/config"
	appLogger "github.com/Suhaibinator/go-common/pkg/logger"
	pb "github.com/Suhaibinator/go-common/pkg/proto_models"
	"go.uber.org/zap"
)

func (h *Handler) Preferences(ctx context.Context, userID uint64) (*pb.NotificationPreferencesResponse, error) {
	cfg := h.inboxConfig()
	if !cfg.Enabled {
		return nil, ErrDisabled
	}
	rows, err := h.store.Preferences(ctx, userID)
	if err != nil {
		return nil, err
	}
	emailAvailable := h.emailAvailable()
	result := &pb.NotificationPreferencesResponse{InAppAlertsEnabled: true, EmailNotificationsEnabled: emailAvailable, ChatEmailNotificationsEnabled: emailAvailable}
	if h.deps.Preferences != nil {
		global, e := h.deps.Preferences(ctx, userID)
		if e != nil {
			return nil, safeCallbackError("global_preferences", e)
		}
		result.EmailNotificationsEnabled = global.EmailEnabled && emailAvailable
		result.ChatEmailNotificationsEnabled = global.ChatEmailEnabled && emailAvailable
	}
	for _, row := range rows {
		if row.Type == 0 {
			if row.AlertsEnabled != nil {
				result.InAppAlertsEnabled = *row.AlertsEnabled
			}
			continue
		}
		result.Preferences = append(result.Preferences, &pb.NotificationPreference{Type: row.Type, AlertsEnabled: row.AlertsEnabled, EmailEnabled: row.EmailEnabled})
	}
	for _, id := range h.registry.ids() {
		t, _ := h.registry.get(id)
		p, e := h.policyForConfig(ctx, t, cfg)
		if e != nil {
			return nil, e
		}
		alerts := p.AlertsEnabled
		email := p.EmailMode != "disabled" && t.RenderEmail != nil && result.EmailNotificationsEnabled
		if id < 1000 {
			email = email && result.ChatEmailNotificationsEnabled
		}

		result.Types = append(result.Types, &pb.NotificationTypeInfo{Type: id, Name: t.Name, AlertsAvailable: alerts, EmailAvailable: email})
	}
	return result, nil
}
func (h *Handler) UpdatePreferences(ctx context.Context, userID uint64, in *pb.UpdateNotificationPreferencesRequest) error {
	cfg := h.inboxConfig()
	if !cfg.Enabled {
		return ErrDisabled
	}
	if in == nil || len(in.Preferences) > cfg.MaxPreferenceUpdates {
		return ErrInvalid
	}
	for _, p := range in.Preferences {
		if p == nil {
			return ErrInvalid
		}
		if _, ok := h.registry.get(p.Type); !ok {
			return ErrUnknownType
		}
	}
	return h.store.WithTransaction(ctx, func(tx context.Context) error {
		if in.InAppAlertsEnabled != nil {
			if err := h.store.SetPreference(tx, &models.NotificationPreference{UserID: userID, AlertsEnabled: in.InAppAlertsEnabled}); err != nil {
				return err
			}
		}
		for _, p := range in.Preferences {
			if err := h.store.SetPreference(tx, &models.NotificationPreference{UserID: userID, Type: p.Type, AlertsEnabled: p.AlertsEnabled, EmailEnabled: p.EmailEnabled}); err != nil {
				return err
			}
		}
		return nil
	})
}
func (h *Handler) deliverySourceAllowed(ctx context.Context, n *models.Notification, t Type, p config.NotificationPolicy, sampledPreference *DeliveryPreference, limits sourceLimits) (DeliveryPreference, bool, error) {
	cancelOnRead := p.CancelOnRead || n.ReusableGroup
	cancelOnResolution := p.CancelOnResolution || n.ReusableGroup
	candidate := SourceCandidate{OrganizationID: n.OrganizationID, RecipientID: n.UserID, Payload: n.Payload}
	decisions, err := h.evaluateType(ctx, t, []SourceCandidate{candidate}, SourceOptions{IncludeDeliveryPreference: sampledPreference == nil && t.DeliveryPreference != nil, CheckResolution: cancelOnResolution && !n.Resolved && !(cancelOnRead && n.ReadRevision >= n.Revision)}, limits)
	if err != nil {
		return DeliveryPreference{}, false, err
	}
	if !decisions[0].Authorized {
		h.suppressed("unauthorized")
		return DeliveryPreference{}, false, nil
	}
	if cancelOnRead && n.ReadRevision >= n.Revision {
		h.suppressed("read")
		return DeliveryPreference{}, false, nil
	}
	if cancelOnResolution && n.Resolved {
		h.suppressed("resolved")
		return DeliveryPreference{}, false, nil
	}
	if cancelOnResolution && decisions[0].Resolved {
		h.suppressed("resolved")
		return DeliveryPreference{}, false, nil
	}
	if sampledPreference == nil {
		sampledPreference = decisions[0].DeliveryPreference
	}
	preference, err := h.sourceDeliveryPreference(ctx, t, candidate, sampledPreference)
	if err != nil {
		return DeliveryPreference{}, false, err
	}
	if preference.Suppressed {
		h.suppressed("preferences")
		return preference, false, nil
	}
	return preference, true, nil
}

// Source and resource preferences are already sampled. Delay/freshness checks
// run before active-view and user-preference work, which cannot make them due.
// activeViewDefer is the email deferral while the recipient views the source;
// alerts pass zero because an active view suppresses them outright.
func (h *Handler) deliveryInterruptionsAllowed(ctx context.Context, n *models.Notification, email bool, activeViewDefer time.Duration) (bool, error) {
	if h.active(n.UserID, n.ActivityScope) {
		h.suppressed("active")
		if email {
			return false, &deliveryDeferral{reason: "active", until: h.now().Add(activeViewDefer)}
		}
		return false, nil
	}
	prefs, err := h.store.Preferences(ctx, n.UserID)
	if err != nil {
		return false, err
	}
	for _, pref := range prefs {
		if email {
			if pref.Type == n.Type && pref.EmailEnabled != nil && !*pref.EmailEnabled {
				h.suppressed("preferences")
				return false, nil
			}
		} else if (pref.Type == 0 || pref.Type == n.Type) && pref.AlertsEnabled != nil && !*pref.AlertsEnabled {
			h.suppressed("preferences")
			return false, nil
		}
	}
	if email && h.deps.Preferences != nil {
		global, err := h.deps.Preferences(ctx, n.UserID)
		if err != nil {
			return false, safeCallbackError("global_preferences", err)
		}
		if !global.EmailEnabled || (n.Type <= 999 && !global.ChatEmailEnabled) {
			h.suppressed("preferences")
			return false, nil
		}
	}
	return true, nil
}
func (h *Handler) ClaimAlert(ctx context.Context, userID, id, revision uint64) (*pb.ClaimNotificationAlertResponse, error) {
	// One sample per claim: the gate and the freshness window agree.
	cfg := h.inboxConfig()
	if !cfg.Enabled {
		return nil, ErrDisabled
	}
	out := &pb.ClaimNotificationAlertResponse{}
	n, err := h.store.Get(ctx, userID, id)
	if err != nil {
		return nil, err
	}
	if n == nil {
		return nil, ErrNotFound
	}
	if n.Revision != revision || n.ReadRevision >= n.Revision || n.Resolved {
		return out, nil
	}
	t, ok := h.registry.get(n.Type)
	if !ok {
		return out, nil
	}
	p, err := h.policy(ctx, t)
	if err != nil {
		return nil, err
	}
	if !p.AlertsEnabled {
		return out, nil
	}
	// An ordinary message may have advanced the inbox after a mention. Select
	// the latest still-unclaimed qualifying occurrence, retaining its own source
	// timestamp so later ordinary activity cannot refresh a stale mention.
	sourceTime := n.SourceTime
	qualifyingRevision := n.Revision
	if sourceTime.IsZero() {
		sourceTime = n.UpdatedAt
	}
	// Unfiltered types can reject stale claims without any source, resource,
	// or global preference lookup. Filter-capable types must inspect the current
	// preference: an earlier mention can be fresh even when this row's latest
	// materialized ordinary occurrence has an older source timestamp.
	if t.DeliveryPreference == nil && !h.freshAlertSource(sourceTime, cfg.AlertMinAge, cfg.AlertMaxAge) {
		out.RetryAfterMs = h.alertRetryAfter(sourceTime, cfg.AlertMinAge, cfg.AlertMaxAge)
		return out, nil
	}
	// Current resource preferences determine which occurrence could alert. Reject
	// absent/stale occurrences before loading the source for authorization. The
	// email re-arm watermark cannot stand in for this current preference read.
	candidate := SourceCandidate{OrganizationID: n.OrganizationID, RecipientID: n.UserID, Payload: n.Payload}
	preference, err := h.sourceDeliveryPreference(ctx, t, candidate, nil)
	if err != nil {
		return nil, err
	}
	if preference.Suppressed {
		h.suppressed("preferences")
		return out, nil
	}
	tag := preference.Tag
	if tag != "" {
		after := max(n.UnreadOccurrenceFloor(), n.AlertClaimedRevision)
		occurrence, err := h.deliveryOccurrence(ctx, n, tag, after, true)
		if err != nil {
			return nil, err
		}
		if occurrence == nil {
			return out, nil
		}
		sourceTime = occurrence.SourceTime
		qualifyingRevision = occurrence.Revision
	}
	if !h.freshAlertSource(sourceTime, cfg.AlertMinAge, cfg.AlertMaxAge) {
		out.RetryAfterMs = h.alertRetryAfter(sourceTime, cfg.AlertMinAge, cfg.AlertMaxAge)
		return out, nil
	}
	_, allowed, err := h.deliverySourceAllowed(ctx, n, t, p, &preference, sourceLimitsFor(cfg))
	if err != nil {
		return nil, err
	}
	if !allowed {
		return out, nil
	}
	allowed, err = h.deliveryInterruptionsAllowed(ctx, n, false, 0)
	if err != nil {
		return nil, err
	}
	if !allowed {
		return out, nil
	}
	claimed, err := h.store.ClaimAlert(ctx, userID, id, revision, qualifyingRevision, h.now(), p.AlertCooldown)
	if err != nil {
		return nil, err
	}
	if !claimed {
		h.suppressed("cooldown")
		return out, nil
	}
	// Authorization is checked again immediately before exposing source payload after the claim.
	v, err := h.view(ctx, n, sourceLimitsFor(cfg))
	if err != nil {
		return nil, err
	}
	if v.Unavailable || v.Resolved {
		return out, nil
	}
	out.Claimed = true
	out.Notification = v
	return out, nil
}

// processDelivery runs one leased job under the tuning sampled by its claim.
func (h *Handler) processDelivery(ctx context.Context, job models.NotificationDelivery, tuning deliveryTuning) {
	started := h.now()
	if h.deps.Metrics != nil {
		h.deps.Metrics.RecordNotificationClaimLateness(started.Sub(job.DueAt))
	}
	workBudget := tuning.workBudget
	if job.LeaseExpiresAt != nil {
		// Leave time for completion before the lease becomes reclaimable. A
		// callback that ignores this context still cannot bypass the send guard.
		workBudget = min(workBudget, job.LeaseExpiresAt.Sub(started)-tuning.completionMargin)
	}
	workCtx, cancel := context.WithTimeout(ctx, workBudget)
	var err error
	var failureStage string
	if workBudget <= 0 {
		err, failureStage = persistence.ErrNotificationLeaseLost, "lease"
	} else {
		err, failureStage = h.deliverSafely(workCtx, job, tuning)
	}
	workErr := workCtx.Err()
	cancel()
	if errors.Is(err, persistence.ErrNotificationLeaseLost) {
		h.log(ctx).Warn("notification delivery lease expired before send",
			zap.Uint64(appLogger.KeyDeliveryJobID, job.ID), zap.Uint64(appLogger.KeyNotificationID, job.NotificationID),
			zap.Uint64(appLogger.KeyTargetUserID, job.UserID), zap.Uint32(appLogger.KeyAttempt, job.Attempts),
			zap.String(logkeys.Stage, "lease"), zap.String(appLogger.KeyOutcome, "lease_lost"),
			zap.String(logkeys.Expected, "valid delivery lease"), zap.String(logkeys.Actual, "lease expired or absent"))
		if h.deps.Metrics != nil {
			h.deps.Metrics.RecordNotificationDelivery("lease_lost", h.now().Sub(started))
		}
		return
	}
	// The final database guard already settled the job without sending.
	if errors.Is(err, errDeliveryCancelled) {
		if h.deps.Metrics != nil {
			h.deps.Metrics.RecordNotificationDelivery("cancelled", h.now().Sub(started))
		}
		return
	}
	// A cancelled provider call may have been accepted remotely. Retain its
	// lease for recovery instead of recording a known-unsent retry.
	if failureStage == "send" && err != nil && (workErr != nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)) {
		fields := []zap.Field{
			zap.Uint64(appLogger.KeyDeliveryJobID, job.ID), zap.Uint64(appLogger.KeyNotificationID, job.NotificationID),
			zap.Uint64(appLogger.KeyTargetUserID, job.UserID), zap.Uint32(appLogger.KeyAttempt, job.Attempts),
			zap.String(logkeys.Stage, "send"), zap.String(appLogger.KeyOutcome, "unknown"),
			zap.String(appLogger.KeyFailureKind, notificationFailureKind(err)),
			zap.String(logkeys.Expected, "known send result"), zap.String(logkeys.Actual, "unknown after cancellation"),
		}
		if errors.Is(err, errCallbackPanic) {
			h.log(ctx).Error("notification send callback panicked with unknown outcome", fields...)
		} else {
			h.log(ctx).Warn("notification send cancelled with unknown outcome", fields...)
		}
		if h.deps.Metrics != nil {
			h.deps.Metrics.RecordNotificationDelivery("send_unknown", h.now().Sub(started))
		}
		return
	}
	// Cancellation before the external send did not consume provider budget.
	if failureStage != "send" && errors.Is(err, context.Canceled) {
		err = &deliveryDeferral{reason: "shutdown", until: h.now()}
	}
	state := persistence.NotificationDeliverySent
	lastError := ""
	next := h.now()
	wait, waiting := errors.AsType[*deliveryWait](err)
	if waiting {
		state = wait.state
		job.WaitingTag = wait.tag
		job.WaitingRevision = wait.revision
	} else if deferral, deferred := errors.AsType[*deliveryDeferral](err); deferred {
		state = persistence.NotificationDeliveryDeferred
		next = deferral.until
	} else if errors.Is(err, errSkip) {
		state = persistence.NotificationDeliverySkipped
	} else if err != nil {
		state = persistence.NotificationDeliveryPending
		lastError = "delivery failed"
		// The claim already counted this attempt, so the first failure waits the base.
		next = next.Add(exponentialBackoff(tuning.retryBackoffBase, tuning.retryBackoffMax, max(job.Attempts, 1)-1))
		if job.Attempts >= tuning.maxAttempts {
			state = persistence.NotificationDeliveryExhausted
		}
		fields := []zap.Field{
			zap.Uint64(appLogger.KeyDeliveryJobID, job.ID), zap.Uint64(appLogger.KeyNotificationID, job.NotificationID),
			zap.Uint64(appLogger.KeyTargetUserID, job.UserID), zap.Uint32(appLogger.KeyAttempt, job.Attempts),
			zap.String(logkeys.Stage, failureStage), zap.String(appLogger.KeyOutcome, state),
			zap.String(appLogger.KeyFailureKind, notificationFailureKind(err)),
			zap.String(logkeys.Expected, "attempt completed"), zap.String(logkeys.Actual, "attempt failed"),
		}
		// Callback errors can contain rendered content or provider request bodies.
		// The durable row and logs use only a fixed safe failure marker.
		if errors.Is(err, errCallbackPanic) {
			h.log(ctx).Error("notification delivery callback panicked", fields...)
		} else if appLogger.IsInvariantError(err) {
			h.log(ctx).Error("notification delivery invariant failed", append(fields, appLogger.ErrorFields(err)...)...)
		} else if state == persistence.NotificationDeliveryExhausted {
			h.log(ctx).Error("notification email attempts exhausted", fields...)
		} else {
			h.log(ctx).Warn("notification delivery attempt failed", fields...)
		}
	}
	if state == persistence.NotificationDeliverySent && h.deps.Metrics != nil {
		h.deps.Metrics.RecordNotificationEmailSend(h.now().Sub(job.CreatedAt))
	}
	finishedAt := h.now()
	if waiting {
		next = finishedAt.Add(wait.delay)
	}
	finishErr := h.finishDelivery(ctx, job, state, lastError, next, finishedAt, tuning)
	if finishErr != nil {
		h.log(ctx).Error("notification delivery completion failed",
			zap.Uint64(appLogger.KeyDeliveryJobID, job.ID), zap.Uint64(appLogger.KeyNotificationID, job.NotificationID),
			zap.Uint64(appLogger.KeyTargetUserID, job.UserID), zap.Uint32(appLogger.KeyAttempt, job.Attempts),
			zap.String(logkeys.Stage, "finish"), zap.String(appLogger.KeyOutcome, "failed"),
			zap.String(appLogger.KeyDeliveryState, state), zap.Error(finishErr))
	}
	if finishErr == nil && state == persistence.NotificationDeliveryDeferred {
		var deferral *deliveryDeferral
		if metric := h.telemetry.deferral; metric != nil && errors.As(err, &deferral) {
			metric.RecordNotificationDeferral(deferral.reason)
		}
	}
	if h.deps.Metrics != nil {
		outcome := "sent"
		switch state {
		case persistence.NotificationDeliverySkipped:
			outcome = "cancelled"
		case persistence.NotificationDeliveryPending:
			outcome = "retry"
		case persistence.NotificationDeliveryDeferred, persistence.NotificationDeliveryWaiting, persistence.NotificationDeliverySuppressed, persistence.NotificationDeliveryDisabled:
			outcome = "deferred"
		case persistence.NotificationDeliveryExhausted:
			outcome = "failed"
		}
		if finishErr != nil {
			outcome = "finish_failed"
			if state == persistence.NotificationDeliverySent {
				outcome = "finish_failed_after_send"
			}
		}
		h.deps.Metrics.RecordNotificationDelivery(outcome, h.now().Sub(started))
	}
}

var errSkip = fmt.Errorf("notification delivery suppressed")

// errDeliveryCancelled reports that the final database guard skipped the
// delivery and refunded its attempt.
var errDeliveryCancelled = errors.New("notification delivery cancelled before send")

// deliveryWait does not spend the unread episode's email opportunity or retry
// budget. Persistence rechecks occurrences atomically before committing waiting.
type deliveryWait struct {
	// state is waiting (for a tagged occurrence), or suppressed/disabled, which
	// retain an unsent opportunity until NEW eligible activity re-arms it.
	state    string
	tag      string
	delay    time.Duration
	revision uint64
}

func (w *deliveryWait) Error() string {
	return "notification delivery waiting for a qualifying occurrence"
}

// deliveryDeferral carries a scheduling decision without consuming retry budget.
type deliveryDeferral struct {
	reason string
	until  time.Time
}

func (d *deliveryDeferral) Error() string { return "notification delivery deferred" }

var errCallbackPanic = errors.New("notification delivery callback panicked")

func (h *Handler) suppressed(reason string) {
	if h.deps.Metrics != nil {
		h.deps.Metrics.RecordNotificationSuppression(reason)
	}
}

// deliverSafely confines application callback panics to one leased attempt. Never
// include the recovered value: renderers and adapters may panic with source content.
// The normal completion path retains lease fencing and the bounded retry budget.
func (h *Handler) deliverSafely(ctx context.Context, job models.NotificationDelivery, tuning deliveryTuning) (err error, stage string) {
	stage = "load"
	defer func() {
		if recover() != nil {
			err = errCallbackPanic
		}
	}()
	err = h.deliverWithStage(ctx, job, &stage, tuning)
	return err, stage
}

func notificationFailureKind(err error) string {
	switch {
	case errors.Is(err, errCallbackPanic):
		return "panic"
	case errors.Is(err, context.DeadlineExceeded):
		return "deadline_exceeded"
	case errors.Is(err, context.Canceled):
		return "cancelled"
	default:
		return "failed"
	}
}

func (h *Handler) deliverWithStage(ctx context.Context, job models.NotificationDelivery, stage *string, tuning deliveryTuning) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if job.ID != 0 && (job.LeaseExpiresAt == nil || !h.now().Before(*job.LeaseExpiresAt)) {
		*stage = "lease"
		return persistence.ErrNotificationLeaseLost
	}
	if !h.enabled() {
		return &deliveryDeferral{reason: "disabled", until: h.now().Add(tuning.disabledDefer)}
	}
	*stage = "load"
	n, err := h.store.Get(ctx, job.UserID, job.NotificationID)
	if err != nil {
		return err
	}
	if n == nil {
		return errSkip
	}
	if n.ReusableGroup && n.UnreadEpisodeRevision != job.UnreadEpisodeRevision {
		*stage = "lease"
		return persistence.ErrNotificationLeaseLost
	}
	t, ok := h.registry.get(n.Type)
	if !ok {
		// A rolling deployment can share this queue with workers that have not
		// registered a newly introduced type yet. Preserve the job for a capable
		// worker without consuming provider retries or immediately reclaiming it.
		return &deliveryDeferral{reason: "type_unavailable", until: h.now().Add(tuning.typeUnavailableDefer)}
	}
	// A closed email gate is not a provider failure. The job stays dormant
	// until new qualifying activity re-arms it after the gate reopens.
	if t.RenderEmail == nil || !h.emailAvailable() {
		return &deliveryWait{state: persistence.NotificationDeliveryDisabled, revision: n.Revision}
	}
	*stage = "policy"
	p, err := h.policy(ctx, t)
	if err != nil {
		return err
	}
	if p.EmailMode == "disabled" {
		h.suppressed("preferences")
		return &deliveryWait{state: persistence.NotificationDeliveryDisabled, revision: n.Revision}
	}
	*stage = "check"
	if err := h.checkEmailDelivery(ctx, n, t, p, tuning); err != nil {
		return err
	}
	renderedRevision := n.Revision
	renderedPayload := bytes.Clone(n.Payload)
	*stage = "render"
	email, err := t.RenderEmail(ctx, n)
	if err != nil {
		return safeCallbackError("render_email", err)
	}
	// Rendering is application code; reread state and authorization afterwards, outside DB locks.
	*stage = "refresh"
	latest, err := h.store.Get(ctx, job.UserID, job.NotificationID)
	if err != nil {
		return err
	}
	if latest == nil {
		return errSkip
	}
	if latest.ReusableGroup && latest.UnreadEpisodeRevision != job.UnreadEpisodeRevision {
		*stage = "lease"
		return persistence.ErrNotificationLeaseLost
	}
	*stage = "refresh"
	// Generic channel activity can advance within its episode without changing
	// the rendered source. Other content changes still require a fresh render.
	if latest.Revision != renderedRevision && (!latest.ReusableGroup || !t.EmailContentUsesPayloadOnly || !bytes.Equal(latest.Payload, renderedPayload)) {
		return &deliveryDeferral{reason: "revision_changed", until: h.now().Add(tuning.revisionChangedDefer)}
	}
	*stage = "policy"
	p, err = h.policy(ctx, t)
	if err != nil {
		return err
	}
	if p.EmailMode == "disabled" {
		return &deliveryWait{state: persistence.NotificationDeliveryDisabled, revision: latest.Revision}
	}
	*stage = "check"
	if err := h.checkEmailDelivery(ctx, latest, t, p, tuning); err != nil {
		return err
	}
	*stage = "prepare"
	send, err := h.deps.PrepareEmail(ctx, latest.UserID, email)
	if errors.Is(err, ErrNotFound) {
		h.suppressed("unauthorized")
		return errSkip
	}
	if errors.Is(err, ErrEmailDisabled) {
		return &deliveryWait{state: persistence.NotificationDeliveryDisabled, revision: latest.Revision}
	}
	if err == nil && send == nil {
		err = ErrInvalid
	}
	if err != nil {
		return safeCallbackError("prepare_email", err)
	}
	if !h.enabled() {
		return &deliveryDeferral{reason: "disabled", until: h.now().Add(tuning.disabledDefer)}
	}
	if !h.emailAvailable() {
		return &deliveryWait{state: persistence.NotificationDeliveryDisabled, revision: latest.Revision}
	}
	// Application policy, authorization and presence callbacks have all
	// returned. Recheck the authoritative inbox state and retirement immediately
	// at the external send boundary.
	*stage = "retirement"
	retired, err := h.store.PrepareDeliverySend(ctx, job, *latest, persistence.NotificationDeliverySendPolicy{CancelOnRead: p.CancelOnRead, CancelOnResolution: p.CancelOnResolution, EmailContentUsesPayloadOnly: t.EmailContentUsesPayloadOnly}, h.publishAfterCommit)
	if errors.Is(err, persistence.ErrNotificationDeliveryChanged) {
		return &deliveryDeferral{reason: "revision_changed", until: h.now().Add(tuning.revisionChangedDefer)}
	}
	if err != nil {
		return err
	}
	if retired {
		return errDeliveryCancelled
	}
	*stage = "lease"
	if err := ctx.Err(); err != nil {
		return err
	}
	if job.ID != 0 && (job.LeaseExpiresAt == nil || !h.now().Before(*job.LeaseExpiresAt)) {
		return persistence.ErrNotificationLeaseLost
	}
	*stage = "send"
	err = safeCallbackError("send_email", send(ctx))
	if errors.Is(err, ErrNotFound) {
		h.suppressed("unauthorized")
		return errSkip
	}
	// The gate closed after the check above; the provider was never called.
	if errors.Is(err, ErrEmailDisabled) {
		return &deliveryWait{state: persistence.NotificationDeliveryDisabled, revision: latest.Revision}
	}
	return err
}

// Reusable groups schedule each unread episode independently of the row lifetime.
func notificationEmailDeadline(n *models.Notification, delay time.Duration) time.Time {
	if n.ReusableGroup {
		return n.UnreadEpisodeStartedAt.Add(delay)
	}
	return n.CreatedAt.Add(delay)
}

func (h *Handler) deliveryOccurrence(ctx context.Context, n *models.Notification, tag string, after uint64, latest bool) (*models.NotificationOccurrence, error) {
	return h.store.GetDeliveryOccurrence(ctx, n.UserID, n.ID, tag, after, n.Revision, latest)
}

func (h *Handler) deliveryEmailDeadline(ctx context.Context, n *models.Notification, tag string, p config.NotificationPolicy) (time.Time, error) {
	if tag == "" {
		return notificationEmailDeadline(n, p.EmailDelay), nil
	}
	occurrence, err := h.deliveryOccurrence(ctx, n, tag, n.UnreadOccurrenceFloor(), false)
	if err != nil {
		return time.Time{}, err
	}
	delay := time.Duration(0)
	if p.EmailMode == "delayed" {
		delay = p.EmailDelay
	}
	if occurrence == nil {
		return time.Time{}, &deliveryWait{state: persistence.NotificationDeliveryWaiting, tag: tag, delay: delay, revision: n.Revision}
	}
	return occurrence.MaterializedAt.Add(delay), nil
}

// freshAlertSource applies the alert window sampled by the calling claim.
func (h *Handler) freshAlertSource(sourceTime time.Time, minAge, maxAge time.Duration) bool {
	age := h.now().Sub(sourceTime)
	if age < minAge || age > maxAge {
		h.suppressed("stale")
		return false
	}
	return true
}

// alertRetryAfter distinguishes a young source from permanent refusals. Round
// up so the retry cannot precede eligibility, and never advertise a retry that
// would already be outside the sampled freshness window.
func (h *Handler) alertRetryAfter(sourceTime time.Time, minAge, maxAge time.Duration) *uint32 {
	now := h.now()
	delay := sourceTime.Add(minAge).Sub(now)
	if delay <= 0 || minAge >= maxAge {
		return nil
	}
	milliseconds := delay / time.Millisecond
	if delay%time.Millisecond != 0 {
		milliseconds++
	}
	if milliseconds > 1<<32-1 || now.Add(milliseconds*time.Millisecond).After(sourceTime.Add(maxAge)) {
		return nil
	}
	return new(uint32(milliseconds))
}

// checkEmailDelivery is independently called before and after rendering. The
// episode's base deadline is a lower bound even for a later qualifying mention,
// so pending delays short-circuit before authorization, preferences, or activity.
func (h *Handler) checkEmailDelivery(ctx context.Context, n *models.Notification, t Type, p config.NotificationPolicy, tuning deliveryTuning) error {
	if deadline := notificationEmailDeadline(n, p.EmailDelay); p.EmailMode == "delayed" && h.now().Before(deadline) {
		return &deliveryDeferral{reason: "policy_delay", until: deadline}
	}
	preference, allowed, err := h.deliverySourceAllowed(ctx, n, t, p, nil, tuning.source)
	if err != nil {
		return err
	}
	if !allowed {
		if preference.Suppressed {
			return &deliveryWait{state: persistence.NotificationDeliverySuppressed, revision: n.Revision}
		}
		return errSkip
	}
	deadline, err := h.deliveryEmailDeadline(ctx, n, preference.Tag, p)
	if err != nil {
		return err
	}
	if p.EmailMode == "delayed" && h.now().Before(deadline) {
		return &deliveryDeferral{reason: "policy_delay", until: deadline}
	}
	allowed, err = h.deliveryInterruptionsAllowed(ctx, n, true, tuning.activeViewDefer)
	if err != nil {
		return err
	}
	if !allowed {
		return errSkip
	}
	return nil
}
