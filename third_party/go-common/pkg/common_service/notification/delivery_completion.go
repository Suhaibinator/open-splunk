package notification

import (
	"context"
	"errors"
	"time"

	persistence "github.com/Suhaibinator/go-common/pkg/common_persistence"
	models "github.com/Suhaibinator/go-common/pkg/common_persistence/common_persistence_models"
)

// finishDelivery retries only the durable completion of a successful send. The
// provider must never be called again within this attempt, including when the
// first completion committed but its response was lost. The same lease token
// fences every retry, and lease loss ends recovery immediately. The write
// timeout and retry spacing come from the tuning sampled by the job's claim.
func (h *Handler) finishDelivery(ctx context.Context, job models.NotificationDelivery, state, lastError string, next, finishedAt time.Time, tuning deliveryTuning) error {
	if state != persistence.NotificationDeliverySent || job.ID == 0 || job.LeaseExpiresAt == nil {
		finishCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), tuning.completionTimeout)
		defer cancel()
		return h.store.FinishDelivery(finishCtx, job, state, lastError, next, finishedAt)
	}

	// The wall-clock deadline bounds retries even if the injected application
	// clock stops advancing. Each write still supplies fresh application time
	// to the store's existing lease fence.
	remaining := job.LeaseExpiresAt.Sub(finishedAt)
	if remaining <= 0 {
		return persistence.ErrNotificationLeaseLost
	}
	deadline := time.Now().Add(remaining)
	finishCtx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()
	cleanup := false
	backoff := tuning.completionRetryBase
	for {
		if ctx.Err() != nil && !cleanup {
			// Shutdown cancels an active database write immediately, then allows
			// one shared cleanup budget for its remaining completion retries.
			cancel()
			cleanupDeadline := time.Now().Add(tuning.completionTimeout)
			if deadline.Before(cleanupDeadline) {
				cleanupDeadline = deadline
			}
			finishCtx, cancel = context.WithDeadline(context.WithoutCancel(ctx), cleanupDeadline)
			defer cancel()
			cleanup = true
		}
		if err := finishCtx.Err(); err != nil {
			return err
		}
		now := h.now()
		if !now.Before(*job.LeaseExpiresAt) {
			return persistence.ErrNotificationLeaseLost
		}
		attemptCtx, cancelAttempt := context.WithTimeout(finishCtx, tuning.completionTimeout)
		err := h.store.FinishDelivery(attemptCtx, job, state, lastError, next, now)
		cancelAttempt()
		if err == nil || errors.Is(err, persistence.ErrNotificationLeaseLost) {
			return err
		}

		timer := time.NewTimer(backoff)
		select {
		case <-timer.C:
		case <-finishCtx.Done():
		}
		timer.Stop()
		backoff = min(2*backoff, tuning.completionRetryMax)
	}
}
