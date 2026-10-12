package controlplane

import (
	"context"
	"time"

	"github.com/Suhaibinator/SRouter/pkg/logkeys"
	"go.uber.org/zap"
)

// safeObserve invokes an observer callback on a log-and-continue basis: a panicking
// observer is recovered and logged so a faulty recorder or metrics bridge can never
// affect generation success.
func (c *ControlPlane) safeObserve(event string, fn func()) {
	defer func() {
		if r := recover(); r != nil {
			c.logger.Error("attempt observer panicked",
				zap.String(logkeys.Invariant, "inference_attempt_observer_does_not_panic"),
				zap.String(logkeys.Operation, "observe_inference_attempt"),
				zap.String(logkeys.Stage, event),
				zap.String(logkeys.Expected, "observer returns normally"),
				zap.String(logkeys.Actual, "observer panicked"),
				zap.String(logkeys.Fallback, "continue_generation_without_observer_result"),
				zap.Any(logkeys.Panic, r),
			)
		}
	}()
	fn()
}

func (c *ControlPlane) emitStart(ctx context.Context, event AttemptStartEvent) {
	c.safeObserve("attempt_started", func() { c.observer.AttemptStarted(ctx, event) })
}

func (c *ControlPlane) emitFinish(ctx context.Context, event AttemptFinishEvent) {
	c.safeObserve("attempt_finished", func() { c.observer.AttemptFinished(ctx, event) })
}

func (c *ControlPlane) emitRetryDecision(ctx context.Context, event RetryDecisionEvent) {
	c.safeObserve("retry_decided", func() { c.observer.RetryDecided(ctx, event) })
}

func (c *ControlPlane) emitThrottle(
	ctx context.Context,
	route *ResolvedRoute,
	invocationID string,
	attempt int,
	scope string,
	kind ThrottleKind,
	err error,
) {
	c.safeObserve("throttled", func() {
		c.observer.Throttled(ctx, ThrottleEvent{
			InvocationID:  invocationID,
			Operation:     route.Operation,
			AttemptNumber: attempt,
			ProviderID:    route.ProviderID,
			Scope:         scope,
			Kind:          kind,
			Err:           err,
		})
	})
}

func (c *ControlPlane) emitBackoff(
	ctx context.Context,
	route *ResolvedRoute,
	invocationID string,
	attempt int,
	scope string,
	backoff time.Duration,
) {
	c.safeObserve("backoff_scheduled", func() {
		c.observer.BackoffScheduled(ctx, BackoffEvent{
			InvocationID:  invocationID,
			Operation:     route.Operation,
			AttemptNumber: attempt,
			ProviderID:    route.ProviderID,
			Scope:         scope,
			Backoff:       backoff,
		})
	})
}
