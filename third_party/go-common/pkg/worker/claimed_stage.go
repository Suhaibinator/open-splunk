package worker

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// ErrClaimLost identifies a stage whose persisted ownership was revoked.
var ErrClaimLost = errors.New("worker stage claim lost")

// ClaimedStage composes execution with lease heartbeats. Persistence callbacks
// must compare the captured claim token, including Release and transitions in Run.
// Transient heartbeat errors are observable but do not revoke ownership.
type ClaimedStage struct {
	HeartbeatInterval time.Duration
	Heartbeat         func(context.Context) error
	IsClaimLost       func(error) bool
	OnHeartbeatError  func(error)
	Run               func(context.Context) error
}

// Execute joins its heartbeat goroutine on every exit, including panic. Claim
// loss cancels the stage and wraps a failed Run with ErrClaimLost. A successful
// guarded transition remains successful: its commit can make a concurrent
// heartbeat report claim loss because the row is no longer running. Run must
// never ignore an ownership rejection from its persistence writes.
func (s ClaimedStage) Execute(ctx context.Context) (err error) {
	stageCtx, cancel := context.WithCancelCause(ctx)
	done := make(chan struct{})
	if s.Heartbeat != nil {
		interval := s.HeartbeatInterval
		if interval <= 0 {
			interval = 10 * time.Second
		}
		go func() {
			defer close(done)
			defer func() {
				if recovered := recover(); recovered != nil {
					cancel(fmt.Errorf("%w: heartbeat callback: %v", ErrProcessorPanicked, recovered))
				}
			}()
			ticker := time.NewTicker(interval)
			defer ticker.Stop()
			for {
				select {
				case <-stageCtx.Done():
					return
				case <-ticker.C:
					err := s.Heartbeat(stageCtx)
					if err == nil || errors.Is(err, context.Canceled) {
						continue
					}
					if s.OnHeartbeatError != nil {
						s.OnHeartbeatError(err)
					}
					if s.IsClaimLost != nil && s.IsClaimLost(err) {
						cancel(ErrClaimLost)
						return
					}
				}
			}
		}()
	} else {
		close(done)
	}
	defer func() {
		cancel(context.Canceled)
		<-done
		if cause := context.Cause(stageCtx); (err != nil && errors.Is(cause, ErrClaimLost)) || errors.Is(cause, ErrProcessorPanicked) {
			err = errors.Join(cause, err)
		}
	}()
	return s.Run(stageCtx)
}

// ReleaseOnShutdown releases a captured claim only when the parent worker was
// cancelled and processing failed. Claim loss alone does not release a new owner.
// Release receives a detached, bounded context. Applications normalize already
// completed/changed claims inside their callback.
func ReleaseOnShutdown(ctx context.Context, timeout time.Duration, run func() error, release func(context.Context) error) error {
	err := run()
	if ctx.Err() == nil || err == nil || release == nil {
		return err
	}
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	releaseCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), timeout)
	defer cancel()
	return errors.Join(err, release(releaseCtx))
}

// RecoverClaims walks an application's stale-claim snapshot. The callback owns
// retry policy and a conditional update comparing the snapshot's claim token and
// stale cutoff; a concurrent heartbeat or takeover must leave the row unchanged.
func RecoverClaims[T any](ctx context.Context, list func(context.Context) ([]T, error), recoverClaim func(context.Context, T) error) error {
	rows, err := list(ctx)
	if err != nil {
		return err
	}
	for _, row := range rows {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := recoverClaim(ctx, row); err != nil {
			return err
		}
	}
	return nil
}
