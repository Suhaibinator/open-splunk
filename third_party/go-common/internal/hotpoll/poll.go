// Package hotpoll runs a single periodic task with a live interval.
package hotpoll

import (
	"context"
	"time"
)

// Run samples interval at least once per second while idle. A nonpositive
// interval pauses work; changes reschedule the next pass without creating a
// second loop. Work is serialized and cancellation prevents further admission.
func Run(ctx context.Context, interval func() time.Duration, work func()) {
	RunWithInitial(ctx, interval(), interval, work)
}

// RunWithInitial delays the first live sample until reconciliation or the first tick.
func RunWithInitial(ctx context.Context, initial time.Duration, interval func() time.Duration, work func()) {
	RunWithInitialDelay(ctx, initial, initial, interval, work)
}

// RunWithInitialDelay permits a staggered first pass without changing the policy interval.
func RunWithInitialDelay(ctx context.Context, initial, firstDelay time.Duration, interval func() time.Duration, work func()) {
	refresh := time.NewTicker(time.Second)
	defer refresh.Stop()
	current := initial
	timer := time.NewTimer(time.Hour)
	defer timer.Stop()
	var tick <-chan time.Time
	reset := func(d time.Duration) {
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		tick = nil
		if d > 0 {
			timer.Reset(d)
			tick = timer.C
		}
	}
	reset(firstDelay)
	for {
		select {
		case <-ctx.Done():
			return
		case <-refresh.C:
			if next := interval(); next != current {
				current = next
				reset(current)
			}
		case <-tick:
			next := interval()
			if next != current {
				current = next
				reset(current)
				continue
			}
			if ctx.Err() != nil {
				return
			}
			if current > 0 {
				work()
			}
			current = interval()
			reset(current)
		}
	}
}
