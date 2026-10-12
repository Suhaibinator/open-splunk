// Package rate_limiter provides configurable per-window attempt limits for
// protecting services from abuse and ensuring fair resource usage.
package rate_limiter

import (
	"context"
	"time"
)

// RateLimiterInterface defines the interface for rate limiting operations.
// Implementations can use different backends (in-memory, Redis, etc.) while
// providing consistent rate limiting behavior across the application.
type RateLimiterInterface interface {
	// CheckRateLimit checks if an action is allowed based on rate limits.
	// Parameters:
	//   - action: The action being rate limited (e.g., "login", "register")
	//   - identifier: Unique identifier for the entity (e.g., IP address, user ID)
	// Returns:
	//   - allowed: Whether the action is allowed
	//   - retryAfter: Duration to wait before retrying (if not allowed)
	//   - error: Any error during the check
	CheckRateLimit(ctx context.Context, action RateLimitAction, identifier string) (bool, time.Duration, error)

	// RecordFailure records a failed attempt for the given action and identifier.
	RecordFailure(ctx context.Context, action RateLimitAction, identifier string) error

	// RecordSuccess records a successful attempt for the given action and identifier.
	// Successful attempts count toward the rate limit.
	RecordSuccess(ctx context.Context, action RateLimitAction, identifier string) error

	// ResetLimit resets the rate limit for a specific action and identifier.
	// This clears all attempt history and removes any active blocks.
	ResetLimit(ctx context.Context, action RateLimitAction, identifier string) error

	// GetLimitStatus returns the current status of rate limiting for an identifier.
	// This includes attempt counts, block status, and timing information.
	GetLimitStatus(ctx context.Context, action RateLimitAction, identifier string) (*RateLimitStatus, error)

	// Close gracefully shuts down the rate limiter and releases resources.
	// This may include closing database connections or stopping background workers.
	Close() error
}

// RateLimitStatus carries state used by the rate_limiter backend flow.
type RateLimitStatus struct {
	// Attempts is the total number of attempts made in the current window.
	Attempts int

	// FailedAttempts is the number of failed attempts in the failure tracking window.
	FailedAttempts int

	// WindowStart is the start time of the current rate limit window.
	WindowStart time.Time

	// BlockedUntil indicates when a block expires (nil if not blocked).
	BlockedUntil *time.Time

	// NextResetTime is when the current rate limit window will reset.
	NextResetTime time.Time

	// RemainingTries is the number of attempts remaining in the current window.
	RemainingTries int

	// IsBlocked indicates whether the identifier is currently blocked.
	IsBlocked bool

	// RetryAfter is the duration to wait before the next attempt is allowed.
	RetryAfter time.Duration
}

// ActionConfig carries state used by the rate_limiter backend flow.
type ActionConfig struct {
	// MaxAttempts is the maximum number of attempts allowed within WindowDuration.
	MaxAttempts int

	// WindowDuration is the time window for rate limiting (e.g., 1 hour).
	WindowDuration time.Duration
}
