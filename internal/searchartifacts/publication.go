package searchartifacts

import (
	"context"
	"errors"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
)

const publicationPollInterval = 10 * time.Millisecond

// AcquirePublishedBounded waits for an active job's durable publication before
// acquiring the same bounded immutable lease as AcquireBounded. The live search
// manager can expose its completed generation before the journal publishes it.
// Derived consumers supply their bounded operation cancellation; ordinary reads retain
// their existing immediate readiness behavior. Polling covers the window before
// Finalize as well as the artifact write, without retaining a waiter per job.
func (store *Store) AcquirePublishedBounded(
	ctx context.Context,
	access searchjobs.AccessScope,
	jobID string,
	reserve func(uint64) (func(), bool),
) (ResultLease, error) {
	if ctx == nil {
		return nil, ErrInvalid
	}
	for {
		lease, err := store.AcquireBounded(ctx, access, jobID, reserve)
		if !errors.Is(err, ErrNotReady) {
			return lease, err
		}
		record, err := store.Get(ctx, access, jobID, AccessInspect)
		if err != nil {
			return nil, err
		}
		switch record.State {
		case StateQueued, StateParsing, StatePlanning, StateRunning:
			// Queued includes both an admitted job and staged completion.
		case StateCompleted:
			if record.ArtifactPresent {
				// Publication may have completed between Acquire and Get.
				// Retry once, propagating corruption or any later state change.
				return store.AcquireBounded(ctx, access, jobID, reserve)
			}
			return nil, ErrNotReady
		default:
			return nil, ErrNotReady
		}
		timer := time.NewTimer(publicationPollInterval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		case <-store.ctx.Done():
			timer.Stop()
			return nil, ErrClosed
		case <-timer.C:
		}
	}
}
