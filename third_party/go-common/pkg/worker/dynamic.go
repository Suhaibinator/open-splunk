package worker

import (
	"context"
	"time"
)

// Admission is sampled before claiming new work. Reductions never release an
// existing claim: excess processors finish, then admission resumes below the limit.
type Admission struct {
	Enabled          bool
	MaxConcurrent    int
	PollInterval     time.Duration
	IdlePollInterval time.Duration
}

// DrainDynamic runs a single claim coordinator and a variable number of processors.
// Settings changes do not cancel processing, replace ownership state, or replay jobs.
func DrainDynamic[T any](ctx context.Context, settings func() Admission, claim Claimer[T], process Processor[T], options DrainOptions) error {
	if settings == nil || claim == nil || process == nil {
		logMissingCallbacks(options.Logger, claim == nil, process == nil)
		return nil
	}
	workerCtx, cancel := context.WithCancel(ctx)
	type result struct {
		err error
	}
	results := make(chan result)
	active := 0
	var firstErr error
	// Completion is always collected, including after cancellation or claim error.
	defer func() {
		cancel()
		for active > 0 {
			<-results
			active--
		}
	}()
	nextPoll := time.Time{}
	lastLimit := 0
	for {
		cfg := settings()
		limit := max(cfg.MaxConcurrent, 1)
		if !cfg.Enabled {
			limit = 0
		}
		if limit > lastLimit {
			nextPoll = time.Time{}
		}
		lastLimit = limit
		if workerCtx.Err() != nil || firstErr != nil {
			cancel()
			if firstErr != nil {
				return firstErr
			}
			return workerCtx.Err()
		}
		if active < limit && !time.Now().Before(nextPoll) {
			item, claimed, err := runClaimer(workerCtx, claim)
			if err != nil {
				firstErr = err
				continue
			}
			if claimed {
				active++
				go func() {
					outcome := result{}
					defer func() { results <- outcome }()
					outcome.err = runProcessor(workerCtx, process, item)
				}()
				continue
			}
			if active == 0 {
				return nil
			}
			interval := cfg.IdlePollInterval
			if interval <= 0 {
				interval = cfg.PollInterval
			}
			if interval <= 0 {
				interval = defaultIdlePollInterval
			}
			nextPoll = time.Now().Add(interval)
		}
		if active == 0 && limit == 0 {
			return nil
		}
		delay := 250 * time.Millisecond
		if active < limit && !nextPoll.IsZero() {
			if remaining := time.Until(nextPoll); remaining > 0 && remaining < delay {
				delay = remaining
			}
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
		case <-options.WakeCh:
			nextPoll = time.Time{}
		case outcome := <-results:
			active--
			if outcome.err != nil {
				firstErr = outcome.err
			}
			nextPoll = time.Time{}
		case <-timer.C:
			// Poll shortening takes effect while previously claimed jobs remain active.
			interval := cfg.IdlePollInterval
			if interval <= 0 {
				interval = cfg.PollInterval
			}
			if interval > 0 && time.Until(nextPoll) > interval {
				nextPoll = time.Now().Add(interval)
			}
		}
		timer.Stop()
	}
}
