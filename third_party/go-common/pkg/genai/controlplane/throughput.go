package controlplane

import "time"

// AttemptThroughput derives output-only rates. Nil means unavailable. Generation TPS
// is client-observed and approximate: chunk batching and callback delays affect it.
func AttemptThroughput(status AttemptStatus, outputTokens int64, latency time.Duration, startedAt, endedAt, firstOutputAt *time.Time) (request, generation *float64) {
	if status != AttemptStatusSucceeded && status != AttemptStatusFailed || outputTokens <= 0 {
		return
	}
	if latency > 0 {
		rate := float64(outputTokens) / latency.Seconds()
		request = &rate
	}
	if startedAt == nil || endedAt == nil || firstOutputAt == nil || startedAt.IsZero() || endedAt.IsZero() || firstOutputAt.IsZero() || firstOutputAt.Before(*startedAt) || !endedAt.After(*firstOutputAt) {
		return
	}
	rate := float64(outputTokens) / endedAt.Sub(*firstOutputAt).Seconds()
	generation = &rate
	return
}

// Throughput returns the available rates for this finished attempt.
func (e AttemptFinishEvent) Throughput() (*float64, *float64) {
	if e.Result.Outcome == nil {
		return nil, nil
	}
	return AttemptThroughput(e.Status, int64(e.Result.Outcome.OutputTokens), e.Latency(), &e.StartedAt, &e.EndedAt, e.Result.Outcome.FirstOutputAt)
}
