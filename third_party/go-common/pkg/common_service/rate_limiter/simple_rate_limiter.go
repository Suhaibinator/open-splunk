package rate_limiter

import (
	"context"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Suhaibinator/go-common/pkg/common_metrics"
	serviceErrors "github.com/Suhaibinator/go-common/pkg/common_service/errors"
	"github.com/Suhaibinator/go-common/pkg/config"
	appLogger "github.com/Suhaibinator/go-common/pkg/logger"

	"github.com/jellydator/ttlcache/v3"
	"go.uber.org/zap"
)

// Compile-time check to ensure SimpleRateLimiter implements RateLimiterInterface
var _ RateLimiterInterface = (*SimpleRateLimiter)(nil)

const (
	rateLimiterLoggerName = "common_service.rate_limiter"

	// maxIdentifierBytesForKey bounds hashing work for malformed, oversized identifiers.
	maxIdentifierBytesForKey = 1024
)

// rateLimitCounter carries state used by the rate_limiter backend flow.
type rateLimitCounter struct {
	mu          sync.Mutex
	windowStart time.Time
	attempts    int
}

// newRateLimitCounter executes this step in the rate_limiter backend flow.
func newRateLimitCounter(now time.Time) *rateLimitCounter {
	return &rateLimitCounter{
		windowStart: now,
	}
}

// check executes this step in the rate_limiter backend flow.
func (counter *rateLimitCounter) check(now time.Time, maxAttempts int, windowDuration time.Duration) (bool, time.Duration) {
	counter.mu.Lock()
	defer counter.mu.Unlock()

	if maxAttempts <= 0 || windowDuration <= 0 {
		return true, 0
	}

	if counter.windowStart.IsZero() || now.Sub(counter.windowStart) >= windowDuration {
		counter.windowStart = now
		counter.attempts = 0
	}

	if counter.attempts < maxAttempts {
		counter.attempts++
		return true, 0
	}

	retryAfter := max(windowDuration-now.Sub(counter.windowStart), 0)

	return false, retryAfter
}

// snapshot executes this step in the rate_limiter backend flow.
func (counter *rateLimitCounter) snapshot(now time.Time, windowDuration time.Duration) (int, time.Time) {
	counter.mu.Lock()
	defer counter.mu.Unlock()

	if counter.windowStart.IsZero() || now.Sub(counter.windowStart) >= windowDuration {
		counter.windowStart = now
		counter.attempts = 0
	}

	return counter.attempts, counter.windowStart
}

// actionLimiterState carries state used by the rate_limiter backend flow.
type actionLimiterState struct {
	config   func() ActionConfig
	limiters *ttlcache.Cache[string, *rateLimitCounter]
}

// SimpleRateLimiter carries state used by the rate_limiter backend flow.
type SimpleRateLimiter struct {
	logger         *zap.Logger
	actions        []actionLimiterState
	cacheCapacity  uint64
	retainHotState bool
	mu             sync.RWMutex
	closeOnce      sync.Once
	metricsHandler common_metrics.MetricsHandlerInterface
	configProvider ConfigProvider
}

// ConfigProvider returns a view captured from one immutable configuration
// generation. It is called once at the boundary of each limiter operation and
// must return a non-nil view.
type ConfigProvider func() config.RateLimitsView

// NewSimpleRateLimiterFromView constructs a limiter from one immutable
// rate-limits view captured at startup.
func NewSimpleRateLimiterFromView(logger *zap.Logger, view config.RateLimitsView, metricsHandler common_metrics.MetricsHandlerInterface) RateLimiterInterface {
	return newSimpleRateLimiter(logger, view, nil, false, metricsHandler)
}

func newSimpleRateLimiter(logger *zap.Logger, view config.RateLimitsView, provider ConfigProvider, retainHotState bool, metricsHandler common_metrics.MetricsHandlerInterface) RateLimiterInterface {
	if logger == nil {
		logger = zap.NewNop()
	}

	if !view.RateLimitEnabled() && provider == nil {
		logger.Named(rateLimiterLoggerName).Info("Rate limiting is disabled")
		return nil
	}

	logger.Named(rateLimiterLoggerName).Info("Rate limiting is enabled")
	rl := &SimpleRateLimiter{
		logger:         logger,
		actions:        make([]actionLimiterState, int(rateLimitActionCount)),
		cacheCapacity:  view.RateLimitCacheCapacity(),
		retainHotState: retainHotState,
		metricsHandler: metricsHandler,
		configProvider: provider,
	}
	for action := ActionUnknown + 1; action < rateLimitActionCount; action++ {
		if fields := viewActionFields[action]; fields.maxAttempts != nil {
			rl.AddActionConfig(action, fields.read(view))
		}
	}

	return rl
}

// NewReloadingSimpleRateLimiterFromView constructs a hot-reloading limiter
// whose restart-bound settings come from one startup view. A nil provider is
// ignored; the limiter then behaves like a non-reloading limiter seeded from
// the startup view, and per-action configs installed via AddActionConfig stay
// authoritative. A hot window may grow beyond the TTL assigned when a counter
// was created, so bounded counter state is retained and counter.check enforces
// the current logical window.
func NewReloadingSimpleRateLimiterFromView(logger *zap.Logger, view config.RateLimitsView, provider ConfigProvider, metricsHandler common_metrics.MetricsHandlerInterface) RateLimiterInterface {
	return newSimpleRateLimiter(logger, view, provider, true, metricsHandler)
}

// AddActionConfig executes this step in the rate_limiter backend flow.
func (rl *SimpleRateLimiter) AddActionConfig(action RateLimitAction, config ActionConfig) {
	if !action.valid() {
		return
	}

	limiters := ttlcache.New(
		ttlcache.WithCapacity[string, *rateLimitCounter](rl.cacheCapacity),
	)
	go limiters.Start()

	rl.mu.Lock()
	defer rl.mu.Unlock()

	previous := rl.actions[int(action)].limiters
	rl.actions[int(action)] = actionLimiterState{
		config:   rl.actionPolicy(action, config),
		limiters: limiters,
	}
	rl.logger.Named(rateLimiterLoggerName).Info("Added rate limit config",
		zap.Stringer(appLogger.KeyAction, action),
		zap.Int(appLogger.KeyMaxAttempts, config.MaxAttempts),
		appLogger.DurationSeconds(appLogger.KeyIntervalSeconds, config.WindowDuration))
	if previous != nil {
		previous.Stop()
	}
}

// CheckRateLimit executes this step in the rate_limiter backend flow.
func (rl *SimpleRateLimiter) CheckRateLimit(ctx context.Context, action RateLimitAction, identifier string) (bool, time.Duration, error) {
	startTime := time.Now().UTC()

	select {
	case <-ctx.Done():
		return false, 0, ctx.Err()
	default:
	}
	if !action.valid() {
		return true, 0, nil
	}

	rl.mu.RLock()
	state := rl.actions[int(action)]
	rl.mu.RUnlock()

	if state.limiters == nil {
		// No rate limiting configured for this action
		return true, 0, nil
	}
	policy := state.config()
	if policy.MaxAttempts <= 0 || policy.WindowDuration <= 0 {
		// Disabled traffic must not allocate entries and evict counters that
		// will become authoritative again when the policy is re-enabled.
		return true, 0, nil
	}

	key := rl.normalizeIdentifierKey(identifier)
	item := state.limiters.Get(key)
	if item == nil {
		// Lookups can borrow the request string. Retained keys own only the
		// bounded normalized bytes, and racing misses share one counter.
		item, _ = state.limiters.GetOrSetFunc(
			strings.Clone(key),
			func() *rateLimitCounter { return newRateLimitCounter(startTime) },
			ttlcache.WithTTL[string, *rateLimitCounter](rl.getEntryTTL(policy)),
		)
	}
	if item == nil {
		return false, 0, serviceErrors.ErrRateLimiterConfigurationError
	}

	allowed, retryAfter := item.Value().check(startTime, policy.MaxAttempts, policy.WindowDuration)
	duration := time.Since(startTime).Seconds()

	if !allowed && appLogger.DebugEnabled(rl.logger) {
		appLogger.For(ctx, rl.logger, rateLimiterLoggerName).Debug("Rate limit exceeded",
			zap.Stringer(appLogger.KeyAction, action),
			zap.String(appLogger.KeyIdentifier, identifier),
			appLogger.DurationSeconds(appLogger.KeyRetryAfterSeconds, retryAfter))
	}

	if rl.metricsHandler != nil {
		rl.metricsHandler.RecordRateLimitCheck(action.String(), string(rl.determineIdentifierType(identifier)), allowed, duration)
	}

	return allowed, retryAfter, nil
}

// getEntryTTL executes this step in the rate_limiter backend flow.
func (rl *SimpleRateLimiter) getEntryTTL(config ActionConfig) time.Duration {
	if rl.retainHotState {
		return ttlcache.NoTTL
	}
	return config.WindowDuration
}

// RecordFailure executes this step in the rate_limiter backend flow.
func (rl *SimpleRateLimiter) RecordFailure(ctx context.Context, action RateLimitAction, identifier string) error {
	if appLogger.DebugEnabled(rl.logger) {
		appLogger.For(ctx, rl.logger, rateLimiterLoggerName).Debug("Failed attempt recorded",
			zap.Stringer(appLogger.KeyAction, action),
			zap.String(appLogger.KeyIdentifier, identifier))
	}
	return nil
}

// RecordSuccess executes this step in the rate_limiter backend flow.
func (rl *SimpleRateLimiter) RecordSuccess(ctx context.Context, action RateLimitAction, identifier string) error {
	if appLogger.DebugEnabled(rl.logger) {
		appLogger.For(ctx, rl.logger, rateLimiterLoggerName).Debug("Successful attempt recorded",
			zap.Stringer(appLogger.KeyAction, action),
			zap.String(appLogger.KeyIdentifier, identifier))
	}
	return nil
}

// ResetLimit executes this step in the rate_limiter backend flow.
func (rl *SimpleRateLimiter) ResetLimit(ctx context.Context, action RateLimitAction, identifier string) error {
	if !action.valid() {
		return nil
	}

	rl.mu.RLock()
	state := rl.actions[int(action)]
	rl.mu.RUnlock()
	if state.limiters != nil {
		state.limiters.Delete(rl.normalizeIdentifierKey(identifier))
	}

	appLogger.For(ctx, rl.logger, rateLimiterLoggerName).Info("Rate limit reset",
		zap.Stringer(appLogger.KeyAction, action),
		zap.String(appLogger.KeyIdentifier, identifier))

	return nil
}

// GetLimitStatus executes this step in the rate_limiter backend flow.
func (rl *SimpleRateLimiter) GetLimitStatus(ctx context.Context, action RateLimitAction, identifier string) (*RateLimitStatus, error) {
	if !action.valid() {
		return &RateLimitStatus{IsBlocked: false}, nil
	}

	rl.mu.RLock()
	state := rl.actions[int(action)]
	rl.mu.RUnlock()

	if state.limiters == nil {
		return &RateLimitStatus{
			IsBlocked: false,
		}, nil
	}
	policy := state.config()
	if policy.MaxAttempts <= 0 || policy.WindowDuration <= 0 {
		return &RateLimitStatus{IsBlocked: false}, nil
	}

	now := time.Now().UTC()
	status := &RateLimitStatus{
		IsBlocked:      false,
		Attempts:       0,
		RemainingTries: policy.MaxAttempts,
		WindowStart:    now,
		NextResetTime:  now.Add(policy.WindowDuration),
	}

	item := state.limiters.Get(rl.normalizeIdentifierKey(identifier))
	if item == nil {
		return status, nil
	}

	attempts, windowStart := item.Value().snapshot(now, policy.WindowDuration)
	status.Attempts = attempts
	status.WindowStart = windowStart
	status.NextResetTime = windowStart.Add(policy.WindowDuration)
	status.RemainingTries = max(policy.MaxAttempts-attempts, 0)
	status.IsBlocked = status.RemainingTries == 0
	if status.IsBlocked {
		status.RetryAfter = max(status.NextResetTime.Sub(now), 0)
	}

	return status, nil
}

// actionPolicy installs one source for each action. A reloading provider owns
// mapped policies; manually registered actions keep their supplied policy while
// observing the provider's enable switch. Static actions use a constant source.
func (rl *SimpleRateLimiter) actionPolicy(action RateLimitAction, policy ActionConfig) func() ActionConfig {
	if rl.configProvider == nil {
		return func() ActionConfig { return policy }
	}
	var read func(config.RateLimitsView) ActionConfig
	if fields := viewActionFields[action]; fields.maxAttempts != nil {
		read = fields.read
	} else {
		read = func(config.RateLimitsView) ActionConfig { return policy }
	}
	provider := rl.configProvider
	return func() ActionConfig {
		view := provider()
		if !view.RateLimitEnabled() {
			return ActionConfig{}
		}
		return read(view)
	}
}

type actionFields struct {
	maxAttempts    func(config.RateLimitsView) int
	windowDuration func(config.RateLimitsView) time.Duration
}

func (fields actionFields) read(view config.RateLimitsView) ActionConfig {
	return ActionConfig{MaxAttempts: fields.maxAttempts(view), WindowDuration: fields.windowDuration(view)}
}

// Unmapped actions are application-owned and configured through AddActionConfig.
var viewActionFields = [rateLimitActionCount]actionFields{
	ActionLogin:                        {config.RateLimitsView.LoginMaxAttempts, config.RateLimitsView.LoginWindowDuration},
	ActionRegister:                     {config.RateLimitsView.RegisterMaxAttempts, config.RateLimitsView.RegisterWindowDuration},
	ActionPasswordReset:                {config.RateLimitsView.PasswordResetMaxAttempts, config.RateLimitsView.PasswordResetWindowDuration},
	ActionVerifyPhoneOTP:               {config.RateLimitsView.VerifyPhoneOTPMaxAttempts, config.RateLimitsView.VerifyPhoneOTPWindowDuration},
	ActionContact:                      {config.RateLimitsView.ContactMaxAttempts, config.RateLimitsView.ContactWindowDuration},
	ActionCareerApplication:            {config.RateLimitsView.CareerApplicationMaxAttempts, config.RateLimitsView.CareerApplicationWindowDuration},
	ActionCareerJobLookup:              {config.RateLimitsView.CareerJobLookupMaxAttempts, config.RateLimitsView.CareerJobLookupWindowDuration},
	ActionOrganizationSlugAvailability: {config.RateLimitsView.OrganizationSlugAvailabilityMaxAttempts, config.RateLimitsView.OrganizationSlugAvailabilityWindowDuration},
	ActionUserSearch:                   {config.RateLimitsView.UserSearchMaxAttempts, config.RateLimitsView.UserSearchWindowDuration},
	ActionNotificationCount:            {config.RateLimitsView.NotificationCountMaxAttempts, config.RateLimitsView.NotificationCountWindowDuration},
	ActionAgentSessionControl:          {config.RateLimitsView.AgentSessionControlMaxAttempts, config.RateLimitsView.AgentSessionControlWindowDuration},
	ActionChatFork:                     {config.RateLimitsView.ChatForkMaxAttempts, config.RateLimitsView.ChatForkWindowDuration},
	ActionOAuthCallback:                {config.RateLimitsView.OAuthCallbackMaxAttempts, config.RateLimitsView.OAuthCallbackWindowDuration},
	ActionPublicChatRead:               {config.RateLimitsView.PublicChatReadMaxAttempts, config.RateLimitsView.PublicChatReadWindowDuration},
}

// Close executes this step in the rate_limiter backend flow.
func (rl *SimpleRateLimiter) Close() error {
	rl.closeOnce.Do(func() {
		for i := 1; i < len(rl.actions); i++ {
			if rl.actions[i].limiters != nil {
				rl.actions[i].limiters.Stop()
			}
		}
		rl.logger.Named(rateLimiterLoggerName).Info("Simple rate limiter closed")
	})

	return nil
}

// normalizeIdentifierKey borrows a bounded substring for lookup. Callers must
// clone it before retention so oversized request strings cannot stay alive.
func (rl *SimpleRateLimiter) normalizeIdentifierKey(identifier string) string {
	normalizedIdentifier := strings.TrimSpace(identifier)
	if len(normalizedIdentifier) > maxIdentifierBytesForKey {
		normalizedIdentifier = normalizedIdentifier[:maxIdentifierBytesForKey]
	}
	return normalizedIdentifier
}

// determineIdentifierType executes this step in the rate_limiter backend flow.
func (rl *SimpleRateLimiter) determineIdentifierType(identifier string) IdentifierType {
	// Simple heuristic: if it contains "@", it's probably an email
	if strings.Contains(identifier, "@") {
		return IdentifierTypeEmail
	}

	// If it looks like an IP address (contains dots or colons for IPv6)
	if strings.Contains(identifier, ".") || strings.Contains(identifier, ":") {
		// Basic check - could be improved with proper IP validation
		return IdentifierTypeIP
	}

	// If it's all digits, might be a user ID
	if _, err := strconv.ParseUint(identifier, 10, 64); err == nil {
		return IdentifierTypeUserID
	}

	// Default to email for backwards compatibility
	return IdentifierTypeEmail
}
