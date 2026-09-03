package clickhouse

import (
	"sync/atomic"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/visibility"
)

// CoalescingDurationHistogramSnapshot uses fixed upper bounds of 1 ms, 10 ms,
// 50 ms, 200 ms, 1 s, 5 s, 30 s, and +Inf. Fixed buckets keep the telemetry
// bounded and free of request-derived labels.
type CoalescingDurationHistogramSnapshot [8]uint64

type coalescingDurationHistogram struct {
	buckets [8]atomic.Uint64
}

func (histogram *coalescingDurationHistogram) observe(duration time.Duration) {
	index := len(histogram.buckets) - 1
	for candidate, bound := range [...]time.Duration{
		time.Millisecond,
		10 * time.Millisecond,
		50 * time.Millisecond,
		200 * time.Millisecond,
		time.Second,
		5 * time.Second,
		30 * time.Second,
	} {
		if duration <= bound {
			index = candidate
			break
		}
	}
	histogram.buckets[index].Add(1)
}

func (histogram *coalescingDurationHistogram) snapshot() CoalescingDurationHistogramSnapshot {
	var result CoalescingDurationHistogramSnapshot
	for index := range histogram.buckets {
		result[index] = histogram.buckets[index].Load()
	}
	return result
}

type writeGroupFillCounters struct {
	rowTarget    atomic.Uint64
	byteTarget   atomic.Uint64
	hardBoundary atomic.Uint64
	linger       atomic.Uint64
	drain        atomic.Uint64
	recovery     atomic.Uint64
}

func (counters *writeGroupFillCounters) add(reason visibility.WriteGroupFillReason) {
	switch reason {
	case visibility.WriteGroupFillRowTarget:
		counters.rowTarget.Add(1)
	case visibility.WriteGroupFillByteTarget:
		counters.byteTarget.Add(1)
	case visibility.WriteGroupFillHardBoundary:
		counters.hardBoundary.Add(1)
	case visibility.WriteGroupFillLinger:
		counters.linger.Add(1)
	case visibility.WriteGroupFillDrain:
		counters.drain.Add(1)
	case visibility.WriteGroupFillRecovery:
		counters.recovery.Add(1)
	}
}

func nonnegativeDuration(later, earlier time.Time) time.Duration {
	if later.IsZero() || earlier.IsZero() || !later.After(earlier) {
		return 0
	}
	return later.Sub(earlier)
}
