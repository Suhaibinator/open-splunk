package controlplane

import (
	"context"
	"fmt"
	"math"
	"sync"
	"time"
)

// tokenBucket is a process-local token bucket that preserves the moved GradeThis
// semantics: the balance may go negative so that post-call token accounting can push
// later callers to wait for refill (negative-balance token debt).
type tokenBucket struct {
	mu             sync.Mutex
	capacity       float64
	tokens         float64
	refillTokens   float64
	refillInterval time.Duration
	lastRefillAt   time.Time
}

// newTokenBucket builds a bucket at full capacity from one validated rate-limit
// policy. A runtime policy change rebuilds the bucket at full capacity.
func newTokenBucket(policy BucketRateLimitPolicy) (*tokenBucket, error) {
	if policy.Capacity <= 0 {
		return nil, fmt.Errorf("bucket capacity must be > 0")
	}
	if policy.RefillInterval <= 0 {
		return nil, fmt.Errorf("bucket refill interval must be > 0")
	}
	if policy.RefillUnits <= 0 {
		return nil, fmt.Errorf("bucket refill tokens must be > 0")
	}
	now := time.Now()
	return &tokenBucket{
		capacity:       float64(policy.Capacity),
		tokens:         float64(policy.Capacity),
		refillTokens:   float64(policy.RefillUnits),
		refillInterval: policy.RefillInterval,
		lastRefillAt:   now,
	}, nil
}

// take blocks until a single unit is available or the wait limit / context deadline
// is exceeded, then consumes it.
func (b *tokenBucket) take(ctx context.Context, waitLimit time.Duration) error {
	return b.acquire(ctx, waitLimit, 1)
}

// acquire blocks until the bucket has at least units available, then consumes them.
func (b *tokenBucket) acquire(ctx context.Context, waitLimit time.Duration, units float64) error {
	return b.wait(ctx, waitLimit, units, true)
}

// waitUntilAvailable blocks until the bucket balance reaches the requested threshold.
// A required threshold of zero waits only until any accumulated token debt is repaid.
func (b *tokenBucket) waitUntilAvailable(ctx context.Context, waitLimit time.Duration, required float64) error {
	return b.wait(ctx, waitLimit, required, false)
}

// wait blocks until the requested balance is available. When reserve is true, it
// subtracts a positive requirement while still holding the availability-check lock.
func (b *tokenBucket) wait(ctx context.Context, waitLimit time.Duration, required float64, reserve bool) error {
	if b == nil {
		return fmt.Errorf("token bucket is nil")
	}
	if waitLimit <= 0 {
		waitLimit = time.Nanosecond
	}
	deadline := time.Now().Add(waitLimit)
	if ctxDeadline, hasDeadline := ctx.Deadline(); hasDeadline && ctxDeadline.Before(deadline) {
		deadline = ctxDeadline
	}

	for {
		waitFor, ok := b.checkNow(required, reserve)
		if ok {
			return nil
		}

		remaining := time.Until(deadline)
		if remaining <= 0 {
			return fmt.Errorf("retry bucket wait exceeded")
		}
		if waitFor > remaining {
			waitFor = remaining
		}
		timer := time.NewTimer(waitFor)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

// consume subtracts units from the bucket, allowing the balance to go negative so
// post-call accounting can push later calls to wait for refill.
func (b *tokenBucket) consume(units float64) {
	if b == nil || units <= 0 {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()

	now := time.Now()
	b.refillLocked(now)
	b.tokens -= units
}

func (b *tokenBucket) checkNow(required float64, reserve bool) (time.Duration, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()

	now := time.Now()
	b.refillLocked(now)
	if b.tokens >= required {
		if reserve && required > 0 {
			b.tokens -= required
		}
		return 0, true
	}
	return b.waitDurationLocked(now, required), false
}

func (b *tokenBucket) waitDurationLocked(now time.Time, required float64) time.Duration {
	if b.refillTokens <= 0 || b.refillInterval <= 0 {
		return time.Second
	}
	missing := required - b.tokens
	if missing <= 0 {
		return 0
	}
	cycles := math.Ceil(missing / b.refillTokens)
	if cycles < 1 {
		cycles = 1
	}
	// Refills land on a fixed schedule anchored at lastRefillAt, so wait until the
	// cycles-th upcoming tick — not cycles whole intervals from now, which would
	// discard progress already made through the current interval. refillLocked(now)
	// has just run, so lastRefillAt is within one interval of now and the result is
	// positive and at most cycles*refillInterval.
	target := b.lastRefillAt.Add(time.Duration(cycles * float64(b.refillInterval)))
	waitFor := target.Sub(now)
	if waitFor <= 0 {
		waitFor = b.refillInterval
	}
	return waitFor
}

// refillLocked adds tokens based on elapsed time since the last refill. Must be
// called with mu held.
func (b *tokenBucket) refillLocked(now time.Time) {
	if now.Before(b.lastRefillAt) {
		b.lastRefillAt = now
		return
	}
	elapsed := now.Sub(b.lastRefillAt)
	if elapsed < b.refillInterval {
		return
	}
	cycles := int(elapsed / b.refillInterval)
	if cycles <= 0 {
		return
	}
	b.tokens += float64(cycles) * b.refillTokens
	if b.tokens > b.capacity {
		b.tokens = b.capacity
	}
	b.lastRefillAt = b.lastRefillAt.Add(time.Duration(cycles) * b.refillInterval)
}
