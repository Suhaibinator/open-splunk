// Package scheduler provides deterministic cron calculation and a small,
// persistence-agnostic scheduling loop for durable control-plane work.
package scheduler

import (
	"context"
	"time"
)

// Clock supplies wall time and cancelable waits. Production code should use
// RealClock; tests can drive Engine without sleeping by calling Step directly.
type Clock interface {
	Now() time.Time
	Wait(context.Context, time.Duration) error
}

// RealClock is the production wall-clock implementation.
type RealClock struct{}

// Now returns the current wall time.
func (RealClock) Now() time.Time { return time.Now() }

// Wait blocks for duration or until ctx is canceled.
func (RealClock) Wait(ctx context.Context, duration time.Duration) error {
	if duration <= 0 {
		return nil
	}
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
