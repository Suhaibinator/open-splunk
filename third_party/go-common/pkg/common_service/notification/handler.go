package notification

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/Suhaibinator/SRouter/pkg/logkeys"
	persistence "github.com/Suhaibinator/go-common/pkg/common_persistence"
	models "github.com/Suhaibinator/go-common/pkg/common_persistence/common_persistence_models"
	"github.com/Suhaibinator/go-common/pkg/common_service/internal/notificationnative"
	"github.com/Suhaibinator/go-common/pkg/common_util/notificationkey"
	"github.com/Suhaibinator/go-common/pkg/config"
	appLogger "github.com/Suhaibinator/go-common/pkg/logger"
	pb "github.com/Suhaibinator/go-common/pkg/proto_models"
	"go.uber.org/zap"
)

// Config is one immutable sample of the notification keys. Every value is read
// as-is: pkg/config validation guarantees the ranges and cross-key orderings,
// and ConfigFromView copies a validated generation without further resolution.
type Config struct {
	// Inbox budgets are sampled per list/count call.
	InboxFetchRows        int
	ListCandidateBudget   int
	CountOperationTimeout time.Duration
	CountUnreadLimit      int
	CountCandidateBudget  int
	// Request bounds are sampled per list, mark-read, or preference update call.
	ListDefaultLimit     int
	ListMaxLimit         int
	MaxReadReceipts      int
	MaxPreferenceUpdates int

	Enabled              bool
	OrganizationsEnabled bool
	// Policies is an immutable catalog shared by one configuration generation.
	// Config samplers may reuse it across operations; handlers never mutate it.
	Policies          map[string]config.NotificationPolicy
	FanoutAsync       bool
	FanoutWorkerCount int
	FanoutPaused      bool
	// Fan-out tuning is sampled once per claim, maintenance tick, or retirement
	// pass. See fanoutTuningFor.
	FanoutStepBudget          time.Duration
	FanoutLease               time.Duration
	FanoutBatchRecipients     int
	FanoutMaintenanceInterval time.Duration
	FanoutPollInterval        time.Duration
	FanoutMaxAttempts         int
	ChatRetirementPageSize    int
	// Chat retirement failure policy joins the per-pass fanoutTuning sample and
	// is passed to the store as one NotificationChatRetirementPolicy value.
	ChatRetirementMaxAttempts          int
	ChatRetirementExhaustedDelay       time.Duration
	ChatRetirementRetryBackoffBase     time.Duration
	ChatRetirementRetryBackoffMax      time.Duration
	ChatRetirementRecordFailureTimeout time.Duration
	FanoutCompactionRetention          time.Duration
	FanoutCompactionBatch              int
	// FanoutBacklogCountLimit bounds each backlog sample's pending count.
	FanoutBacklogCountLimit int
	// RetirementMaxRecipients caps the unique recipient/type pairs one
	// retirement transaction locks. It is sampled once per retirement call and
	// once per chat retirement pass.
	RetirementMaxRecipients int
	// SourceResolutionPageSize bounds one inline source resolution and each
	// resolution job page. It is sampled once per ResolveSource call and once
	// per retirement pass.
	SourceResolutionPageSize int
	// RetirementClaimCandidates bounds the due recipient retirement jobs one
	// claim probes; sampled once per retirement pass.
	RetirementClaimCandidates int
	// InlineSnapshotMaxPages bounds producer snapshot pages per inline event;
	// sampled once per emission operation.
	InlineSnapshotMaxPages int
	// Fan-out retry and maintenance timing joins the per-claim / per-tick
	// fanoutTuning sample.
	FanoutClaimErrorMaxBackoff time.Duration
	FanoutCompactionInterval   time.Duration
	FanoutRetryBackoffBase     time.Duration
	FanoutRetryBackoffMax      time.Duration
	FanoutDeferredRetryDelay   time.Duration
	FanoutFinishTimeout        time.Duration

	// DeliveryWorkerCount and WorkerShutdownTimeout are captured once by Start;
	// SetWorkerCount overrides the count.
	DeliveryWorkerCount   int
	WorkerShutdownTimeout time.Duration
	// Delivery tuning is sampled once per claim and carried with every job that
	// claim leased (see deliveryTuningFor).
	DeliveryPollInterval               time.Duration
	DeliveryClaimErrorMaxBackoff       time.Duration
	DeliveryPollJitterFraction         float64
	DeliveryClaimTimeout               time.Duration
	DeliveryLease                      time.Duration
	DeliveryMaxAttempts                int
	DeliveryWorkBudget                 time.Duration
	DeliveryLeaseCompletionMargin      time.Duration
	DeliveryRetryBackoffBase           time.Duration
	DeliveryRetryBackoffMax            time.Duration
	DeliveryActiveViewDefer            time.Duration
	DeliveryDisabledDefer              time.Duration
	DeliveryTypeUnavailableDefer       time.Duration
	DeliveryRevisionChangedDefer       time.Duration
	DeliveryCompletionTimeout          time.Duration
	DeliveryCompletionRetryBackoffBase time.Duration
	DeliveryCompletionRetryBackoffMax  time.Duration
	// Alert freshness window, sampled once per ClaimAlert.
	AlertMinAge time.Duration
	AlertMaxAge time.Duration

	// Source evaluation chunking, sampled once per evaluateType call.
	SourceBatchRows  int
	SourceBatchBytes int
	// Emission byte limits, sampled once per operation (see emissionLimitsFor).
	MaxPayloadBytes      int
	MaxFallbackTextBytes int
	MaxMetadataBytes     int
	EmitMaxTotalBytes    int
}

// ConfigView is the configuration a notification Config is sampled from: the
// notifications group plus the organization feature flag.
type ConfigView interface {
	config.NotificationsView
	EnableOrganizations() bool
}

// ConfigFromView copies one validated configuration generation, including its
// policy catalog. Callers memoize the result per generation.
func ConfigFromView(v ConfigView) *Config {
	return &Config{
		Enabled:                   v.EnableNotifications(),
		OrganizationsEnabled:      v.EnableOrganizations(),
		FanoutAsync:               v.NotificationFanoutAsync(),
		FanoutWorkerCount:         v.NotificationFanoutWorkerCount(),
		FanoutPaused:              v.NotificationFanoutPaused(),
		FanoutStepBudget:          v.NotificationFanoutStepBudget(),
		FanoutLease:               v.NotificationFanoutLease(),
		FanoutBatchRecipients:     v.NotificationFanoutBatchRecipients(),
		FanoutMaintenanceInterval: v.NotificationFanoutMaintenanceInterval(),
		FanoutPollInterval:        v.NotificationFanoutPollInterval(),
		FanoutMaxAttempts:         v.NotificationFanoutMaxAttempts(),
		ChatRetirementPageSize:    v.NotificationChatRetirementPageSize(),
		RetirementMaxRecipients:   v.NotificationRetirementMaxRecipients(),
		SourceResolutionPageSize:  v.NotificationSourceResolutionPageSize(),
		RetirementClaimCandidates: v.NotificationRetirementClaimCandidates(),

		ChatRetirementMaxAttempts:          v.NotificationChatRetirementMaxAttempts(),
		ChatRetirementExhaustedDelay:       v.NotificationChatRetirementExhaustedDelay(),
		ChatRetirementRetryBackoffBase:     v.NotificationChatRetirementRetryBackoffBase(),
		ChatRetirementRetryBackoffMax:      v.NotificationChatRetirementRetryBackoffMax(),
		ChatRetirementRecordFailureTimeout: v.NotificationChatRetirementRecordFailureTimeout(),
		FanoutCompactionRetention:          v.NotificationFanoutCompactionRetention(),
		FanoutCompactionBatch:              v.NotificationFanoutCompactionBatch(),
		FanoutBacklogCountLimit:            v.NotificationFanoutBacklogCountLimit(),
		InlineSnapshotMaxPages:             v.NotificationInlineSnapshotMaxPages(),
		Policies:                           v.NotificationPolicies(),
		InboxFetchRows:                     v.NotificationInboxFetchRows(),
		ListCandidateBudget:                v.NotificationListCandidateBudget(),
		CountOperationTimeout:              v.NotificationCountOperationTimeout(),
		CountUnreadLimit:                   v.NotificationCountUnreadLimit(),
		CountCandidateBudget:               v.NotificationCountCandidateBudget(),
		ListDefaultLimit:                   v.NotificationListDefaultLimit(),
		ListMaxLimit:                       v.NotificationListMaxLimit(),
		MaxReadReceipts:                    v.NotificationMaxReadReceipts(),
		MaxPreferenceUpdates:               v.NotificationMaxPreferenceUpdates(),

		FanoutClaimErrorMaxBackoff: v.NotificationFanoutClaimErrorMaxBackoff(),
		FanoutCompactionInterval:   v.NotificationFanoutCompactionInterval(),
		FanoutRetryBackoffBase:     v.NotificationFanoutRetryBackoffBase(),
		FanoutRetryBackoffMax:      v.NotificationFanoutRetryBackoffMax(),
		FanoutDeferredRetryDelay:   v.NotificationFanoutDeferredRetryDelay(),
		FanoutFinishTimeout:        v.NotificationFanoutFinishTimeout(),

		DeliveryWorkerCount:                v.NotificationDeliveryWorkerCount(),
		WorkerShutdownTimeout:              v.NotificationWorkerShutdownTimeout(),
		DeliveryPollInterval:               v.NotificationDeliveryPollInterval(),
		DeliveryClaimErrorMaxBackoff:       v.NotificationDeliveryClaimErrorMaxBackoff(),
		DeliveryPollJitterFraction:         v.NotificationDeliveryPollJitterFraction(),
		DeliveryClaimTimeout:               v.NotificationDeliveryClaimTimeout(),
		DeliveryLease:                      v.NotificationDeliveryLease(),
		DeliveryMaxAttempts:                v.NotificationDeliveryMaxAttempts(),
		DeliveryWorkBudget:                 v.NotificationDeliveryWorkBudget(),
		DeliveryLeaseCompletionMargin:      v.NotificationDeliveryLeaseCompletionMargin(),
		DeliveryRetryBackoffBase:           v.NotificationDeliveryRetryBackoffBase(),
		DeliveryRetryBackoffMax:            v.NotificationDeliveryRetryBackoffMax(),
		DeliveryActiveViewDefer:            v.NotificationDeliveryActiveViewDefer(),
		DeliveryDisabledDefer:              v.NotificationDeliveryDisabledDefer(),
		DeliveryTypeUnavailableDefer:       v.NotificationDeliveryTypeUnavailableDefer(),
		DeliveryRevisionChangedDefer:       v.NotificationDeliveryRevisionChangedDefer(),
		DeliveryCompletionTimeout:          v.NotificationDeliveryCompletionTimeout(),
		DeliveryCompletionRetryBackoffBase: v.NotificationDeliveryCompletionRetryBackoffBase(),
		DeliveryCompletionRetryBackoffMax:  v.NotificationDeliveryCompletionRetryBackoffMax(),
		AlertMinAge:                        v.NotificationAlertMinAge(),
		AlertMaxAge:                        v.NotificationAlertMaxAge(),

		SourceBatchRows:      v.NotificationSourceBatchRows(),
		SourceBatchBytes:     v.NotificationSourceBatchBytes(),
		MaxPayloadBytes:      v.NotificationMaxPayloadBytes(),
		MaxFallbackTextBytes: v.NotificationMaxFallbackTextBytes(),
		MaxMetadataBytes:     v.NotificationMaxMetadataBytes(),
		EmitMaxTotalBytes:    v.NotificationEmitMaxTotalBytes(),
	}
}

// DefaultConfig is the sample of the pkg/config defaults: the module disabled
// with every tuning key at its default. It is for tests and handlers built
// without a configuration provider.
func DefaultConfig() *Config {
	return ConfigFromView(config.NewRootView(config.GetDefaults()))
}

type ActivityStore interface{ IsActive(uint64, string) bool }
type GlobalPreferences struct{ EmailEnabled, ChatEmailEnabled bool }
type Metrics interface {
	RecordNotificationDelivery(string, time.Duration)
	RecordNotificationEmailSend(time.Duration)
	RecordNotificationSuppression(string)
	RecordNotificationClaimLateness(time.Duration)
}
type Dependencies struct {
	// Metrics accepts nil, the delivery minimum Metrics, or independent optional
	// InboxMetrics, CountMetrics, BatchMetrics, DeferralMetrics and
	// common_metrics.NotificationFanoutMetrics. AllMetrics composes the full set.
	Metrics Metrics
	// Config returns an immutable sample; neither the handler nor the caller
	// may mutate a published Config or its policy catalog.
	Config      func() *Config
	Preferences func(context.Context, uint64) (GlobalPreferences, error)
	// PrepareEmail runs recipient lookup and other application callbacks before
	// the final database cancellation guard, and returns the external send.
	PrepareEmail func(context.Context, uint64, Email) (func(context.Context) error, error)
	// EmailEnabled samples the email transport's hot configuration gate at each
	// enqueue, preference read, and delivery check. Nil leaves availability to
	// the configured sender alone. The sender returns ErrEmailDisabled when the gate closes
	// between that check and the send.
	EmailEnabled func() bool
	Logger       *zap.Logger
	// Log derives request correlation and the notification component name at a
	// diagnostic boundary. A nil resolver falls back to Logger with that name.
	Log func(context.Context) *zap.Logger
}
type PolicyProvider func(context.Context, uint32) (*config.NotificationPolicy, error)
type Handler struct {
	store                persistence.NotificationStore
	telemetry            notificationTelemetry
	deps                 Dependencies
	registry             registry
	producers            producerRegistry
	fanoutWorkersEnabled bool
	fanoutWake           chan struct{}
	deliveryWake         chan struct{}
	retirementWake       chan struct{}
	queueSubscribe       sync.Once
	unsubscribeQueues    func()
	mu                   sync.RWMutex
	activity             ActivityStore
	policyProvider       PolicyProvider
	changed              func(context.Context, uint64, uint64, uint64) error
	cancel               context.CancelFunc
	stopped              bool
	workerCount          int
	shutdownTimeout      time.Duration
	workerDone           chan struct{}
	now                  func() time.Time
}

func New(store persistence.NotificationStore, deps Dependencies) *Handler {
	if deps.Logger == nil {
		deps.Logger = zap.NewNop()
	}
	if deps.Config == nil {
		// A handler built without a provider runs on the shared defaults, which
		// keep the module disabled.
		defaults := DefaultConfig()
		deps.Config = func() *Config { return defaults }
	}
	h := &Handler{store: store, deps: deps, now: time.Now, fanoutWorkersEnabled: true, fanoutWake: make(chan struct{}, 1), deliveryWake: make(chan struct{}, 1), retirementWake: make(chan struct{}, 1)}
	h.setMetrics(deps.Metrics)
	h.registerOrganizationProducer()
	return h
}
func (h *Handler) log(ctx context.Context) *zap.Logger {
	if h.deps.Log != nil {
		if log := h.deps.Log(ctx); log != nil {
			return log
		}
	}
	return h.deps.Logger.Named("common_service.notification")
}
func (h *Handler) Register(t Type) error { return h.registry.register(t, false) }

// RegisterNative installs a go-common native type in the reserved id range.
// Importing applications use Register instead.
func (h *Handler) RegisterNative(cap notificationnative.Capability, t Type) error {
	if !cap.Valid() {
		return ErrInvalid
	}
	return h.registry.register(t, true)
}
func (h *Handler) SetActivityStore(a ActivityStore) { h.mu.Lock(); defer h.mu.Unlock(); h.activity = a }
func (h *Handler) SetPolicyProvider(p PolicyProvider) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.policyProvider = p
}

// SetChangedPublisher installs a best-effort thin invalidation publisher.
// Publication waits for commit, or is suppressed for a hook-less transaction.
func (h *Handler) SetChangedPublisher(p func(context.Context, uint64, uint64, uint64) error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.changed = p
}

// ChangedPublisher returns the installed invalidation publisher, or nil when none is set.
func (h *Handler) ChangedPublisher() func(context.Context, uint64, uint64, uint64) error {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.changed
}
func (h *Handler) enabled() bool {
	return h != nil && h.store != nil && h.deps.Config().Enabled
}

// emailGateOpen samples the transport's hot configuration gate. Callers sample
// it once per decision; no job is scheduled or attempted while it is closed.
func (h *Handler) emailGateOpen() bool {
	return h.deps.EmailEnabled == nil || h.deps.EmailEnabled()
}

// emailAvailable reports whether a sender exists and its gate is open.
func (h *Handler) emailAvailable() bool {
	return h.deps.PrepareEmail != nil && h.emailGateOpen()
}
func (h *Handler) policy(ctx context.Context, t Type) (config.NotificationPolicy, error) {
	return h.policyForConfig(ctx, t, h.deps.Config())
}

func (h *Handler) policyForConfig(ctx context.Context, t Type, cfg *Config) (config.NotificationPolicy, error) {
	p := t.DefaultPolicy
	if override, ok := cfg.Policies[strconv.FormatUint(uint64(t.ID), 10)]; ok {
		p = override
	}
	h.mu.RLock()
	provider := h.policyProvider
	h.mu.RUnlock()
	if provider != nil {
		override, err := provider(ctx, t.ID)
		if err != nil {
			return p, safeCallbackError("policy_provider", err)
		}
		if override != nil {
			p = *override
		}
	}
	return p, p.Validate()
}
func (h *Handler) publish(ctx context.Context, n *models.Notification) {
	h.mu.RLock()
	publish := h.changed
	h.mu.RUnlock()
	if publish == nil || n == nil {
		return
	}
	userID, id, revision, typeID := n.UserID, n.ID, n.Revision, n.Type
	run := func(runCtx context.Context) (result error) {
		fields := func(outcome string) []zap.Field {
			return []zap.Field{
				zap.Uint64(appLogger.KeyNotificationID, id), zap.Uint64(appLogger.KeyTargetUserID, userID),
				zap.Uint32(appLogger.KeyNotificationType, typeID), zap.Uint64(appLogger.KeyNotificationRevision, revision),
				zap.String(logkeys.Stage, "publish"), zap.String(appLogger.KeyOutcome, outcome),
			}
		}
		defer func() {
			if recover() != nil {
				h.log(runCtx).Error("notification invalidation callback panicked",
					append(fields("panicked"), zap.String(logkeys.Expected, "publication completes"), zap.String(logkeys.Actual, "callback panicked"), zap.Stack("stack"))...)
				result = nil
			}
		}()
		if err := publish(runCtx, userID, id, revision); err != nil {
			h.log(runCtx).Warn("notification invalidation failed",
				append(fields("failed"), zap.Error(safeCallbackError("publish", err)))...)
		}
		return nil
	}
	if persistence.RegisterNotificationPostCommitOrSuppress(ctx, run) {
		return
	}
	_ = run(ctx)
}
func (h *Handler) active(userID uint64, scope string) bool {
	if scope == "" {
		return false
	}
	h.mu.RLock()
	a := h.activity
	h.mu.RUnlock()
	return a != nil && a.IsActive(userID, scope)
}

type EmitInput struct {
	// OrganizationID adds a current direct-membership gate; zero is unscoped.
	Producer                string
	OrganizationID          int64
	RecipientID             uint64
	Type                    uint32
	Payload                 []byte
	FallbackText, SourceKey string
	SourceTime              time.Time
	// DeliveryTag classifies this immutable occurrence for optional interruption filters.
	DeliveryTag string
}

func (h *Handler) MarkRead(ctx context.Context, userID uint64, receipts []*pb.NotificationReadReceipt) error {
	cfg := h.inboxConfig()
	if !cfg.Enabled {
		return ErrDisabled
	}
	if len(receipts) > cfg.MaxReadReceipts {
		return ErrInvalid
	}
	return h.store.WithTransaction(ctx, func(tx context.Context) error {
		r := make([]persistence.NotificationReadReceipt, 0, len(receipts))
		for _, receipt := range receipts {
			if receipt == nil || receipt.Id == 0 || receipt.Revision == 0 {
				return ErrInvalid
			}
			r = append(r, persistence.NotificationReadReceipt{ID: receipt.Id, Revision: receipt.Revision})
		}
		changed, err := h.store.MarkReadReturning(tx, userID, r)
		if err != nil {
			return err
		}
		for i := range changed {
			h.publish(tx, &changed[i])
		}
		return nil
	})
}
func (h *Handler) Resolve(ctx context.Context, userID, id uint64) error {
	if !h.enabled() {
		return ErrDisabled
	}
	return h.store.WithTransaction(ctx, func(tx context.Context) error {
		n, err := h.store.Resolve(tx, userID, id)
		if err != nil {
			return err
		}
		if n != nil {
			h.publish(tx, n)
		}
		return err
	})
}

// Update changes the payload of one recipient's existing entry, preserving its identity.
func (h *Handler) Update(ctx context.Context, userID, id uint64, payload []byte, fallback string) (*models.Notification, error) {
	// One sample governs the gate, the byte limits, and source evaluation.
	cfg := h.inboxConfig()
	if !cfg.Enabled {
		return nil, ErrDisabled
	}
	n, err := h.store.Get(ctx, userID, id)
	if err != nil {
		return nil, err
	}
	if n == nil {
		return nil, ErrNotFound
	}
	t, ok := h.registry.get(n.Type)
	if !ok {
		return nil, ErrUnknownType
	}
	limits := emissionLimitsFor(cfg)
	if len(payload) > limits.payload || len(fallback) > limits.fallbackText {
		return nil, ErrInvalid
	}
	if err := t.Validate(ctx, payload); err != nil {
		return nil, safeCallbackError("validate", err)
	}
	decisions, err := h.evaluateType(ctx, t, []SourceCandidate{{OrganizationID: n.OrganizationID, RecipientID: userID, Payload: payload}}, SourceOptions{}, limits.source)
	if err != nil {
		return nil, err
	}
	if !decisions[0].Authorized {
		return nil, ErrNotFound
	}
	// Updating an entry cannot move it between source scopes or unread episodes.
	if n.ActivityScope != "" && !notificationkey.ValidActivityScope(n.ActivityScope) {
		return nil, fmt.Errorf("%w: invalid activity scope", ErrInvalid)
	}
	if t.ActivityScope != nil {
		scope, e := t.ActivityScope(ctx, payload)
		if e != nil {
			return nil, safeCallbackError("activity_scope", e)
		}
		if scope != n.ActivityScope {
			return nil, fmt.Errorf("%w: update changes activity scope", ErrInvalid)
		}
	}
	if t.GroupKey != nil && n.GroupKey != "" {
		group, e := t.GroupKey(ctx, payload)
		if e != nil {
			return nil, safeCallbackError("group_key", e)
		}
		if group != n.GroupKey {
			return nil, fmt.Errorf("%w: update changes group", ErrInvalid)
		}
	}
	var result *models.Notification
	err = h.store.WithTransaction(ctx, func(tx context.Context) error {
		var e error
		result, e = h.store.Update(tx, userID, id, payload, fallback)
		if e == nil && result != nil {
			h.publish(tx, result)
		}
		return e
	})
	return result, err
}
func (h *Handler) ResolveSource(ctx context.Context, typeID uint32, sourceKey string) error {
	if h == nil || h.store == nil {
		return nil
	}
	pageSize := h.deps.Config().SourceResolutionPageSize
	return h.store.WithTransaction(ctx, func(tx context.Context) error {
		rows, err := h.store.ResolveSource(tx, typeID, sourceKey, pageSize)
		if err != nil {
			return err
		}
		for i := range rows {
			h.publish(tx, &rows[i])
		}
		return nil
	})
}

// RetireActivityScope settles reusable entries when their source is removed or
// a recipient leaves. This lifecycle cleanup also runs while notification
// production is disabled.
//
// A nonzero userID retires that recipient synchronously, within
// notification_retirement_max_recipients. A zero userID retires the scope for
// every recipient with no size limit: the ambient source transaction commits a
// scope fence, after which no emission from an earlier source materializes for
// anyone, and a durable job that the maintenance worker pages through (see
// docs/backend/external/notifications.md for the reader-visibility window). A
// known audience of at most the cap uses RetireActivityScopeForRecipients.
func (h *Handler) RetireActivityScope(ctx context.Context, userID uint64, scope string) error {
	if h == nil || h.store == nil {
		return nil
	}
	if !notificationkey.ValidActivityScope(scope) {
		return ErrInvalid
	}
	maxRecipients := h.deps.Config().RetirementMaxRecipients
	if userID != 0 {
		// Admission must fence every applicable type, including types whose
		// first async occurrence has not produced an inbox row yet.
		var recipients []persistence.NotificationRecipientType
		for _, id := range h.registry.ids() {
			t, _ := h.registry.get(id)
			if t.ReusableGroup {
				recipients = append(recipients, persistence.NotificationRecipientType{UserID: userID, Type: id})
			}
		}
		if len(recipients) > 0 {
			return h.retireActivityScopeForRecipients(ctx, scope, recipients, h.store.RetireActivityScopeForRecipients)
		}
	}
	return h.store.WithTransaction(ctx, func(tx context.Context) error {
		rows, err := h.store.RetireActivityScope(tx, userID, scope, maxRecipients)
		if err != nil {
			return err
		}
		if userID == 0 {
			h.nudgeRetirementAfterCommit(tx)
		}
		for i := range rows {
			h.publish(tx, &rows[i])
		}
		return nil
	})
}

// WithTransaction binds source lifecycle changes and notification retirement to
// the same transaction, preserving any ambient source transaction and its hooks.
func (h *Handler) WithTransaction(ctx context.Context, fn func(context.Context) error) error {
	if h == nil || h.store == nil {
		return fn(ctx)
	}
	return h.store.WithTransaction(ctx, fn)
}
