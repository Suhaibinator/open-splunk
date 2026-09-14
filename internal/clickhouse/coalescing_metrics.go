package clickhouse

import (
	"math"
	"sync/atomic"
	"time"

	"fortio.org/safecast"

	"github.com/Suhaibinator/open-splunk/internal/visibility"
)

const coalescingShapeBucketCount = 13

var (
	coalescingShapeBounds = [coalescingShapeBucketCount]uint64{
		1, 10, 64, 100, 500, 1_000, 2_500, 5_000, 10_000, 16_384, 32_768, 50_000, 65_536,
	}
	coalescingByteBounds = [coalescingShapeBucketCount]uint64{
		1 << 10, 4 << 10, 16 << 10, 64 << 10, 256 << 10, 1 << 20, 4 << 20,
		8 << 20, 16 << 20, 24 << 20, 32 << 20, 48 << 20, 64 << 20,
	}
	coalescingDurationBounds = [...]time.Duration{
		time.Millisecond,
		10 * time.Millisecond,
		50 * time.Millisecond,
		200 * time.Millisecond,
		time.Second,
		5 * time.Second,
		30 * time.Second,
	}
)

// CoalescingDurationUpperBoundsMicroseconds returns the inclusive bounds for
// the first seven duration buckets. The eighth bucket is overflow.
func CoalescingDurationUpperBoundsMicroseconds() [7]uint64 {
	var result [7]uint64
	for index, bound := range coalescingDurationBounds {
		result[index] = safecast.MustConv[uint64](bound / time.Microsecond)
	}
	return result
}

// CoalescingHistogramSnapshot is a detached fixed-cardinality distribution.
// Counts includes one final overflow bucket after the inclusive upper bounds.
type CoalescingHistogramSnapshot struct {
	UpperBounds  [coalescingShapeBucketCount]uint64
	BucketCounts [coalescingShapeBucketCount + 1]uint64
	Count        uint64
	Sum          uint64
	Max          uint64
}

type coalescingHistogram struct {
	upperBounds  [coalescingShapeBucketCount]uint64
	bucketCounts [coalescingShapeBucketCount + 1]atomic.Uint64
	count        atomic.Uint64
	sum          atomic.Uint64
	max          atomic.Uint64
}

func newCoalescingHistogram(upperBounds [coalescingShapeBucketCount]uint64) coalescingHistogram {
	return coalescingHistogram{upperBounds: upperBounds}
}

func (histogram *coalescingHistogram) observe(value uint64) {
	bucket := len(histogram.upperBounds)
	for index, upperBound := range histogram.upperBounds {
		if value <= upperBound {
			bucket = index
			break
		}
	}
	histogram.bucketCounts[bucket].Add(1)
	histogram.count.Add(1)
	atomicSaturatingAdd(&histogram.sum, value)
	atomicMaximum(&histogram.max, value)
}

func (histogram *coalescingHistogram) snapshot() CoalescingHistogramSnapshot {
	snapshot := CoalescingHistogramSnapshot{
		UpperBounds: histogram.upperBounds,
		Count:       histogram.count.Load(),
		Sum:         histogram.sum.Load(),
		Max:         histogram.max.Load(),
	}
	for index := range histogram.bucketCounts {
		snapshot.BucketCounts[index] = histogram.bucketCounts[index].Load()
	}
	return snapshot
}

func atomicSaturatingAdd(counter *atomic.Uint64, value uint64) {
	for {
		current := counter.Load()
		next := current + value
		if math.MaxUint64-current < value {
			next = math.MaxUint64
		}
		if counter.CompareAndSwap(current, next) {
			return
		}
	}
}

func atomicMaximum(counter *atomic.Uint64, value uint64) {
	for current := counter.Load(); value > current; current = counter.Load() {
		if counter.CompareAndSwap(current, value) {
			return
		}
	}
}

// CoalescingDurationHistogramSnapshot uses fixed upper bounds of 1 ms, 10 ms,
// 50 ms, 200 ms, 1 s, 5 s, 30 s, and +Inf. Fixed buckets keep the telemetry
// bounded and free of request-derived labels.
type CoalescingDurationHistogramSnapshot [8]uint64

type coalescingDurationHistogram struct {
	buckets [8]atomic.Uint64
}

func (histogram *coalescingDurationHistogram) observe(duration time.Duration) {
	index := len(histogram.buckets) - 1
	for candidate, bound := range coalescingDurationBounds {
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
