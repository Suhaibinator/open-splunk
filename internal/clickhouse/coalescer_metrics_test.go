package clickhouse

import (
	"fmt"
	"math"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestCoalescerMetricsRecordsLogicalPhysicalAndQueueShape(t *testing.T) {
	t.Parallel()
	metrics := NewCoalescerMetrics()
	metrics.ObserveStaged(1)
	metrics.ObserveStaged(9_999)
	metrics.ObserveGroupFormed(
		CoalescerFillRowTarget,
		64,
		10_000,
		16<<20,
		2,
		175*time.Millisecond,
	)
	metrics.ObservePhysicalSend(10_000, 190*time.Millisecond)
	metrics.ObserveAmbiguity()
	metrics.ObserveRetry()
	metrics.ObservePhysicalSend(10_000, 250*time.Millisecond)
	metrics.ObserveGroupSuccess(300 * time.Millisecond)
	metrics.AddNativeWaiter()
	metrics.AddNativeWaiter()
	metrics.ObserveNativeWaiterWakeups(2)
	metrics.ObserveNativeWaiterCancellation()
	metrics.ObserveNativeTerminalLookup()
	metrics.RemoveNativeWaiter()
	metrics.SetQueue(CoalescerQueueSnapshot{
		UngroupedReservations: 3,
		ReadyGroups:           2,
		AmbiguousGroups:       1,
		PendingOutboxBytes:    4 << 20,
		OldestPendingAge:      450 * time.Millisecond,
	})

	snapshot := metrics.Snapshot()
	if snapshot.StagedLogicalBatches != 2 || snapshot.StagedLogicalRows != 10_000 ||
		snapshot.FormedGroups != 1 || snapshot.PhysicalSends != 2 ||
		snapshot.SuccessfulGroups != 1 || snapshot.Retries != 1 || snapshot.Ambiguities != 1 ||
		snapshot.GroupsByFillReason[CoalescerFillRowTarget] != 1 ||
		snapshot.MemberBatchesPerGroup.Count != 1 || snapshot.MemberBatchesPerGroup.Sum != 64 ||
		snapshot.RowsPerGroup.Count != 1 || snapshot.RowsPerGroup.Max != 10_000 ||
		snapshot.DecodedBytesPerGroup.Sum != 16<<20 ||
		snapshot.MonthlyPartitionsPerGroup.Sum != 2 ||
		snapshot.RowsPerPhysicalInsert.Count != 2 ||
		snapshot.RowsPerPhysicalInsert.Sum != 20_000 ||
		snapshot.CreationToSeal.SumMicroseconds != 175_000 ||
		snapshot.CreationToSend.Count != 2 || snapshot.CreationToSend.MaxMicroseconds != 250_000 ||
		snapshot.CreationToCommit.SumMicroseconds != 300_000 ||
		snapshot.NativeWaiters != 1 || snapshot.PeakNativeWaiters != 2 ||
		snapshot.NativeWaiterWakeups != 2 || snapshot.NativeWaiterCancels != 1 ||
		snapshot.NativeTerminalLookups != 1 ||
		snapshot.Queue.UngroupedReservations != 3 || snapshot.Queue.ReadyGroups != 2 ||
		snapshot.Queue.AmbiguousGroups != 1 || snapshot.Queue.PendingOutboxBytes != 4<<20 ||
		snapshot.Queue.OldestPendingAge != 450*time.Millisecond {
		t.Fatalf("coalescer metrics snapshot = %#v", snapshot)
	}
	assertCoalescerHistogram(t, snapshot.RowsPerPhysicalInsert)
	assertCoalescerLatencyHistogram(t, snapshot.CreationToSend)
}

func TestCoalescerMetricsAreFixedCardinalityAndPayloadFree(t *testing.T) {
	t.Parallel()
	metrics := NewCoalescerMetrics()
	metrics.ObserveGroupFormed(
		CoalescerFillReason(math.MaxUint8),
		1,
		1,
		1,
		1,
		time.Microsecond,
	)
	metrics.SetQueue(CoalescerQueueSnapshot{OldestPendingAge: -time.Second})
	snapshot := metrics.Snapshot()
	if snapshot.GroupsByFillReason[CoalescerFillUnknown] != 1 {
		t.Fatalf("unknown fill reason count = %d, want 1", snapshot.GroupsByFillReason[CoalescerFillUnknown])
	}
	if snapshot.Queue.OldestPendingAge != 0 {
		t.Fatalf("negative oldest age = %s, want zero", snapshot.Queue.OldestPendingAge)
	}
	assertCoalescerAggregateShape(t, reflect.TypeFor[CoalescerMetricsSnapshot]())
	for _, private := range []string{
		"tenant-private", "index-private", "batch-private", "channel-private", "payload-private",
	} {
		if strings.Contains(fmt.Sprintf("%#v", snapshot), private) {
			t.Errorf("metrics snapshot contains private value %q", private)
		}
	}
}

func TestCoalescerMetricsConcurrentSnapshotsRemainCoherent(t *testing.T) {
	t.Parallel()
	metrics := NewCoalescerMetrics()
	const workers = 16
	const observations = 250
	var group sync.WaitGroup
	group.Add(workers)
	for range workers {
		go func() {
			defer group.Done()
			for range observations {
				metrics.ObserveStaged(2)
				metrics.ObservePhysicalSend(2, time.Millisecond)
				metrics.ObserveRetry()
				metrics.AddNativeWaiter()
				metrics.RemoveNativeWaiter()
			}
		}()
	}
	group.Wait()
	snapshot := metrics.Snapshot()
	want := uint64(workers * observations)
	if snapshot.StagedLogicalBatches != want || snapshot.StagedLogicalRows != 2*want ||
		snapshot.PhysicalSends != want || snapshot.RowsPerPhysicalInsert.Count != want ||
		snapshot.RowsPerPhysicalInsert.Sum != 2*want || snapshot.Retries != want ||
		snapshot.NativeWaiters != 0 || snapshot.PeakNativeWaiters == 0 {
		t.Fatalf("concurrent snapshot = %#v, want %d observations", snapshot, want)
	}
}

func TestCoalescerMetricsNilOwnerIsSafe(t *testing.T) {
	t.Parallel()
	var metrics *CoalescerMetrics
	metrics.ObserveStaged(1)
	metrics.ObserveGroupFormed(CoalescerFillLinger, 1, 1, 1, 1, time.Millisecond)
	metrics.ObservePhysicalSend(1, time.Millisecond)
	metrics.ObserveGroupSuccess(time.Millisecond)
	metrics.ObserveRetry()
	metrics.ObserveAmbiguity()
	metrics.AddNativeWaiter()
	metrics.RemoveNativeWaiter()
	metrics.ObserveNativeWaiterWakeups(1)
	metrics.ObserveNativeWaiterCancellation()
	metrics.ObserveNativeTerminalLookup()
	metrics.SetQueue(CoalescerQueueSnapshot{})
	if snapshot := metrics.Snapshot(); !reflect.ValueOf(snapshot).IsZero() {
		t.Fatalf("nil metrics snapshot = %#v, want zero", snapshot)
	}
}

func TestCoalescerMetricsSaturateAggregateSums(t *testing.T) {
	t.Parallel()
	metrics := NewCoalescerMetrics()
	metrics.ObserveStaged(math.MaxUint64)
	metrics.ObserveStaged(1)
	metrics.ObserveNativeWaiterWakeups(math.MaxUint64)
	metrics.ObserveNativeWaiterWakeups(1)
	snapshot := metrics.Snapshot()
	if snapshot.StagedLogicalRows != math.MaxUint64 ||
		snapshot.NativeWaiterWakeups != math.MaxUint64 {
		t.Fatalf("saturated counters = rows %d wakeups %d", snapshot.StagedLogicalRows, snapshot.NativeWaiterWakeups)
	}
}

func assertCoalescerHistogram(t *testing.T, histogram CoalescerHistogramSnapshot) {
	t.Helper()
	var count uint64
	for _, bucket := range histogram.Counts {
		count += bucket
	}
	if count != histogram.Count {
		t.Errorf("histogram bucket count = %d, want %d", count, histogram.Count)
	}
	for index := 1; index < len(histogram.Bounds); index++ {
		if histogram.Bounds[index] <= histogram.Bounds[index-1] {
			t.Errorf("histogram bounds are not increasing: %v", histogram.Bounds)
			break
		}
	}
}

func assertCoalescerLatencyHistogram(t *testing.T, histogram CoalescerLatencyHistogramSnapshot) {
	t.Helper()
	var count uint64
	for _, bucket := range histogram.Counts {
		count += bucket
	}
	if count != histogram.Count {
		t.Errorf("latency histogram bucket count = %d, want %d", count, histogram.Count)
	}
	for index := 1; index < len(histogram.BoundsMicroseconds); index++ {
		if histogram.BoundsMicroseconds[index] <= histogram.BoundsMicroseconds[index-1] {
			t.Errorf("latency histogram bounds are not increasing: %v", histogram.BoundsMicroseconds)
			break
		}
	}
}

func assertCoalescerAggregateShape(t *testing.T, metricType reflect.Type) {
	t.Helper()
	for field := range metricType.Fields() {
		assertCoalescerAggregateField(t, field.Name, field.Type)
	}
}

func assertCoalescerAggregateField(t *testing.T, path string, fieldType reflect.Type) {
	t.Helper()
	switch fieldType.Kind() {
	case reflect.Uint64, reflect.Int64:
	case reflect.Array:
		assertCoalescerAggregateField(t, path+"[]", fieldType.Elem())
	case reflect.Struct:
		for field := range fieldType.Fields() {
			assertCoalescerAggregateField(t, path+"."+field.Name, field.Type)
		}
	default:
		t.Errorf("metric field %s has identity-capable type %s", path, fieldType)
	}
}
