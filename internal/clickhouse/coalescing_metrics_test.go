package clickhouse

import (
	"math"
	"sync/atomic"
	"testing"
)

func TestCoalescingHistogramHasFixedSelfDescribingShape(t *testing.T) {
	t.Parallel()
	histogram := newCoalescingHistogram(coalescingShapeBounds)
	for _, value := range []uint64{1, 2, 70_000} {
		histogram.observe(value)
	}
	snapshot := histogram.snapshot()
	if snapshot.UpperBounds != coalescingShapeBounds || snapshot.Count != 3 ||
		snapshot.Sum != 70_003 || snapshot.Max != 70_000 ||
		snapshot.BucketCounts[0] != 1 || snapshot.BucketCounts[1] != 1 ||
		snapshot.BucketCounts[len(snapshot.BucketCounts)-1] != 1 {
		t.Fatalf("coalescing histogram = %+v", snapshot)
	}
}

func TestCoalescingDurationBoundsArePublicAndFixed(t *testing.T) {
	t.Parallel()
	if got := CoalescingDurationUpperBoundsMicroseconds(); got != [7]uint64{
		1_000,
		10_000,
		50_000,
		200_000,
		1_000_000,
		5_000_000,
		30_000_000,
	} {
		t.Fatalf("duration bounds = %v", got)
	}
}

func TestAtomicSaturatingAddAndMaximum(t *testing.T) {
	t.Parallel()
	var counter atomic.Uint64
	counter.Store(math.MaxUint64 - 1)
	atomicSaturatingAdd(&counter, 2)
	if got := counter.Load(); got != math.MaxUint64 {
		t.Fatalf("saturated sum = %d", got)
	}
	atomicMaximum(&counter, math.MaxUint64-1)
	if got := counter.Load(); got != math.MaxUint64 {
		t.Fatalf("maximum regressed = %d", got)
	}
}
