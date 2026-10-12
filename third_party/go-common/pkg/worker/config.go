package worker

import (
	"context"
	"sync/atomic"
	"time"
)

// ConfigProvider stores a typed source of immutable configuration snapshots.
// Installing a provider does not change worker lifecycle or ownership.
type ConfigProvider[T any] struct{ provider atomic.Pointer[func() T] }

func (p *ConfigProvider[T]) Set(provider func() T) { p.provider.Store(&provider) }
func (p *ConfigProvider[T]) Config(fallback T) T {
	if provider := p.provider.Load(); provider != nil && *provider != nil {
		return (*provider)()
	}
	return fallback
}
func (p *ConfigProvider[T]) IsSet() bool {
	provider := p.provider.Load()
	return provider != nil && *provider != nil
}

// Wait reevaluates a live interval at least once per second. Reducing a long
// interval takes effect without restarting the loop or repeating startup work.
// Increases apply on the next wait, so enabling a dormant worker cannot extend
// its already scheduled wake by the newly active long polling interval.
func Wait(ctx context.Context, wake <-chan struct{}, interval func() time.Duration) bool {
	ready, _ := WaitForSignal(ctx, wake, interval)
	return ready
}

// WaitForSignal additionally identifies coalesced wake signals for worker metrics.
func WaitForSignal(ctx context.Context, wake <-chan struct{}, interval func() time.Duration) (ready, signaled bool) {
	start := time.Now()
	var deadline time.Time
	for {
		delay := interval()
		if delay <= 0 {
			delay = time.Second
		}
		candidate := start.Add(delay)
		if deadline.IsZero() || candidate.Before(deadline) {
			deadline = candidate
		}
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return true, false
		}
		if remaining > time.Second {
			remaining = time.Second
		}
		timer := time.NewTimer(remaining)
		select {
		case <-ctx.Done():
			timer.Stop()
			return false, false
		case <-wake:
			timer.Stop()
			return true, true
		case <-timer.C:
		}
	}
}
