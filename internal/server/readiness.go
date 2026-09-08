package server

import (
	"context"
	"sync"
)

// runtimeReadinessChecker shares unfinished observations so the public health
// endpoint can occupy at most one slot in the ordinary runtime connection pool.
// Completed observations are not cached: the next probe checks recovery afresh.
type runtimeReadinessChecker struct {
	probe RuntimeReadiness

	mu       sync.Mutex
	inFlight *runtimeReadinessResult
}

type runtimeReadinessResult struct {
	done  chan struct{}
	ready bool
}

func (checker *runtimeReadinessChecker) ready(ctx context.Context) bool {
	if checker.probe == nil || ctx.Err() != nil {
		return false
	}
	checker.mu.Lock()
	result := checker.inFlight
	if result == nil {
		result = &runtimeReadinessResult{done: make(chan struct{})}
		checker.inFlight = result
		go checker.sample(context.WithoutCancel(ctx), result)
	}
	checker.mu.Unlock()

	select {
	case <-ctx.Done():
		return false
	case <-result.done:
		return result.ready && ctx.Err() == nil
	}
}

func (checker *runtimeReadinessChecker) sample(ctx context.Context, result *runtimeReadinessResult) {
	// A disconnected HTTP caller must not cancel other callers' observation.
	ctx, cancel := context.WithTimeout(ctx, runtimeReadinessTimeout)
	defer cancel()
	result.ready = checker.probe.Ping(ctx) == nil && ctx.Err() == nil

	checker.mu.Lock()
	// Retain admission until Ping actually returns. A driver operation that
	// outlives its context must not allow another probe to enter the pool.
	close(result.done)
	checker.inFlight = nil
	checker.mu.Unlock()
}
