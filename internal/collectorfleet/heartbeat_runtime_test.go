package collectorfleet

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/control"
)

func TestHeartbeatRuntimeConfigValidation(t *testing.T) {
	t.Parallel()

	clock := newHeartbeatRuntimeClock(time.Date(2040, 1, 1, 0, 0, 0, 0, time.UTC))
	writer := newHeartbeatRuntimeWriter()
	valid := heartbeatRuntimeConfig(clock)
	tests := []struct {
		name   string
		writer HeartbeatWriter
		mutate func(*HeartbeatRuntimeConfig)
	}{
		{name: "nil writer", writer: nil},
		{
			name:   "zero collector capacity",
			writer: writer,
			mutate: func(config *HeartbeatRuntimeConfig) { config.MaxCollectors = 0 },
		},
		{
			name:   "negative collector capacity",
			writer: writer,
			mutate: func(config *HeartbeatRuntimeConfig) { config.MaxCollectors = -1 },
		},
		{
			name:   "collector capacity above catalog bound",
			writer: writer,
			mutate: func(config *HeartbeatRuntimeConfig) {
				config.MaxCollectors = MaximumActiveCollectors + 1
			},
		},
		{
			name:   "zero heartbeat interval",
			writer: writer,
			mutate: func(config *HeartbeatRuntimeConfig) { config.HeartbeatInterval = 0 },
		},
		{
			name:   "negative heartbeat interval",
			writer: writer,
			mutate: func(config *HeartbeatRuntimeConfig) { config.HeartbeatInterval = -time.Second },
		},
		{
			name:   "negative stale grace",
			writer: writer,
			mutate: func(config *HeartbeatRuntimeConfig) { config.StaleGrace = -time.Nanosecond },
		},
		{
			name:   "zero flush interval",
			writer: writer,
			mutate: func(config *HeartbeatRuntimeConfig) { config.FlushInterval = 0 },
		},
		{
			name:   "negative flush interval",
			writer: writer,
			mutate: func(config *HeartbeatRuntimeConfig) { config.FlushInterval = -time.Second },
		},
		{
			name:   "zero write timeout",
			writer: writer,
			mutate: func(config *HeartbeatRuntimeConfig) { config.WriteTimeout = 0 },
		},
		{
			name:   "negative write timeout",
			writer: writer,
			mutate: func(config *HeartbeatRuntimeConfig) { config.WriteTimeout = -time.Second },
		},
		{
			name:   "nil monotonic clock",
			writer: writer,
			mutate: func(config *HeartbeatRuntimeConfig) { config.MonotonicNow = nil },
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			config := valid
			if test.mutate != nil {
				test.mutate(&config)
			}
			if _, err := NewHeartbeatRuntime(test.writer, config); !errors.Is(
				err,
				control.ErrInvalidArgument,
			) {
				t.Fatalf("NewHeartbeatRuntime() error = %v, want ErrInvalidArgument", err)
			}
		})
	}

	zeroGrace := valid
	zeroGrace.StaleGrace = 0
	zeroGrace.OnError = nil
	runtime, err := NewHeartbeatRuntime(writer, zeroGrace)
	if err != nil {
		t.Fatalf("NewHeartbeatRuntime(zero grace, nil callback): %v", err)
	}
	heartbeatRuntimeClose(t, runtime)
}

func TestHeartbeatRuntimeOfferValidatesAndDetachesSynchronously(t *testing.T) {
	t.Parallel()

	clock := newHeartbeatRuntimeClock(time.Date(2040, 1, 1, 1, 0, 0, 0, time.UTC))
	writer := newHeartbeatRuntimeWriter()
	runtime := newHeartbeatRuntimeForTest(t, writer, heartbeatRuntimeConfig(clock))
	lease := heartbeatRuntimeLease("tenant-a", "collector-a", 1)
	if err := runtime.Activate(lease); err != nil {
		t.Fatal(err)
	}

	invalidHeartbeat := heartbeatRuntimeHeartbeat(0)
	if accepted, err := runtime.Offer(
		context.Background(),
		lease,
		invalidHeartbeat,
	); accepted || !errors.Is(err, control.ErrInvalidArgument) {
		t.Fatalf("Offer(invalid heartbeat) = %t, %v", accepted, err)
	}

	canceledContext, cancel := context.WithCancel(context.Background())
	cancel()
	if accepted, err := runtime.Offer(
		canceledContext,
		lease,
		heartbeatRuntimeHeartbeat(1),
	); accepted || !errors.Is(err, context.Canceled) {
		t.Fatalf("Offer(canceled context) = %t, %v", accepted, err)
	}

	heartbeat := heartbeatRuntimeHeartbeat(7)
	lastErrorAt := heartbeat.ReceivedAt.Add(-2 * time.Second)
	heartbeat.Inputs[0].LastErrorAt = &lastErrorAt
	expected, err := normalizeHeartbeat(heartbeat)
	if err != nil {
		t.Fatalf("normalize expected heartbeat: %v", err)
	}
	if accepted, err := runtime.Offer(
		context.Background(),
		lease,
		heartbeat,
	); err != nil || !accepted {
		t.Fatalf("Offer(valid heartbeat) = %t, %v", accepted, err)
	}

	*heartbeat.Queue.OldestEventAge = 99 * time.Hour
	*heartbeat.LastSentBatchSequence = 999
	*heartbeat.LastAcknowledgedBatchSequence = 998
	heartbeat.Inputs[0].StatusMessage = "mutated"
	*heartbeat.Inputs[0].LastEventAt = heartbeat.ReceivedAt.Add(24 * time.Hour)
	*heartbeat.Inputs[0].LastErrorAt = heartbeat.ReceivedAt.Add(48 * time.Hour)
	heartbeat.Inputs = append(heartbeat.Inputs, InputHealth{
		InputID: "mutated-input",
		State:   1,
	})

	if accepted, err := runtime.Offer(
		context.Background(),
		lease,
		heartbeatRuntimeHeartbeat(7),
	); err != nil || accepted {
		t.Fatalf("Offer(duplicate sequence) = %t, %v", accepted, err)
	}
	if accepted, err := runtime.Offer(
		context.Background(),
		lease,
		heartbeatRuntimeHeartbeat(6),
	); err != nil || accepted {
		t.Fatalf("Offer(older sequence) = %t, %v", accepted, err)
	}
	if err := runtime.Flush(context.Background()); err != nil {
		t.Fatalf("Flush(): %v", err)
	}
	calls := writer.Calls()
	if len(calls) != 1 {
		t.Fatalf("writer calls = %d, want 1", len(calls))
	}
	if !reflect.DeepEqual(calls[0].heartbeat, expected) {
		t.Fatalf("persisted heartbeat aliased caller:\n got %#v\nwant %#v", calls[0].heartbeat, expected)
	}
}

func TestHeartbeatRuntimeCoalescesThousandOffersToOneWrite(t *testing.T) {
	t.Parallel()

	clock := newHeartbeatRuntimeClock(time.Date(2040, 1, 1, 2, 0, 0, 0, time.UTC))
	writer := newHeartbeatRuntimeWriter()
	runtime := newHeartbeatRuntimeForTest(t, writer, heartbeatRuntimeConfig(clock))
	lease := heartbeatRuntimeLease("tenant-a", "collector-coalesced", 1)
	if err := runtime.Activate(lease); err != nil {
		t.Fatal(err)
	}
	for sequence := uint64(1); sequence <= 1_000; sequence++ {
		accepted, err := runtime.Offer(
			context.Background(),
			lease,
			heartbeatRuntimeHeartbeat(sequence),
		)
		if err != nil || !accepted {
			t.Fatalf("Offer(%d) = %t, %v", sequence, accepted, err)
		}
	}
	if err := runtime.Flush(context.Background()); err != nil {
		t.Fatalf("Flush(): %v", err)
	}
	calls := writer.Calls()
	if len(calls) != 1 || calls[0].heartbeat.ObservationSequence != 1_000 {
		t.Fatalf("coalesced writer calls = %#v", calls)
	}
	if err := runtime.Flush(context.Background()); err != nil {
		t.Fatalf("Flush(empty): %v", err)
	}
	if calls := writer.Calls(); len(calls) != 1 {
		t.Fatalf("empty flush repeated persisted heartbeat: %#v", calls)
	}
}

func TestHeartbeatRuntimeIdleFlushDoesNotAllocate(t *testing.T) {
	clock := newHeartbeatRuntimeClock(
		time.Date(2040, 1, 1, 2, 15, 0, 0, time.UTC),
	)
	writer := newHeartbeatRuntimeWriter()
	runtime := newHeartbeatRuntimeForTest(
		t,
		writer,
		heartbeatRuntimeConfig(clock),
	)
	lease := heartbeatRuntimeLease("tenant-a", "collector-idle", 1)
	if err := runtime.Activate(lease); err != nil {
		t.Fatal(err)
	}

	var flushErr error
	allocations := testing.AllocsPerRun(1_000, func() {
		flushErr = runtime.flush(context.Background())
	})
	if flushErr != nil {
		t.Fatalf("idle flush: %v", flushErr)
	}
	if allocations != 0 {
		t.Fatalf("idle flush allocations = %v, want 0", allocations)
	}
	if calls := writer.Calls(); len(calls) != 0 {
		t.Fatalf("idle flush writer calls = %#v", calls)
	}
}

func TestHeartbeatRuntimeConcurrentOffersFlushOnlyMaximumSequence(t *testing.T) {
	t.Parallel()

	clock := newHeartbeatRuntimeClock(time.Date(2040, 1, 1, 2, 30, 0, 0, time.UTC))
	writer := newHeartbeatRuntimeWriter()
	runtime := newHeartbeatRuntimeForTest(t, writer, heartbeatRuntimeConfig(clock))
	lease := heartbeatRuntimeLease("tenant-a", "collector-concurrent", 1)
	if err := runtime.Activate(lease); err != nil {
		t.Fatal(err)
	}

	const offerCount = 256
	type offerResult struct {
		sequence uint64
		accepted bool
		err      error
	}
	start := make(chan struct{})
	results := make(chan offerResult, offerCount)
	var workers sync.WaitGroup
	for sequence := uint64(1); sequence <= offerCount; sequence++ {
		sequence := sequence
		workers.Go(func() {
			<-start
			accepted, err := runtime.Offer(
				context.Background(),
				lease,
				heartbeatRuntimeHeartbeat(sequence),
			)
			results <- offerResult{
				sequence: sequence,
				accepted: accepted,
				err:      err,
			}
		})
	}
	close(start)
	workers.Wait()
	close(results)

	acceptedMaximum := false
	acceptedCount := 0
	for result := range results {
		if result.err != nil {
			t.Fatalf("Offer(%d): %v", result.sequence, result.err)
		}
		if result.accepted {
			acceptedCount++
		}
		if result.sequence == offerCount {
			acceptedMaximum = result.accepted
		}
	}
	if acceptedCount == 0 || !acceptedMaximum {
		t.Fatalf(
			"concurrent offers accepted=%d maximum-accepted=%t",
			acceptedCount,
			acceptedMaximum,
		)
	}
	if err := runtime.Flush(context.Background()); err != nil {
		t.Fatalf("Flush(): %v", err)
	}
	calls := writer.Calls()
	if len(calls) != 1 ||
		calls[0].heartbeat.ObservationSequence != offerCount {
		t.Fatalf("concurrent latest-wins writer calls = %#v", calls)
	}
}

func TestHeartbeatRuntimeBoundsWholeFlushCycleAndRequeuesUnattemptedEntries(
	t *testing.T,
) {
	t.Parallel()

	blocked := make(chan struct{})
	writer := newHeartbeatRuntimeWriter(heartbeatRuntimeWriteResponse{
		release: blocked,
	})
	clock := newHeartbeatRuntimeClock(
		time.Date(2040, 1, 1, 2, 30, 0, 0, time.UTC),
	)
	config := heartbeatRuntimeConfig(clock)
	config.MaxCollectors = 16
	config.WriteTimeout = 25 * time.Millisecond
	reported := make(chan error, 1)
	config.OnError = func(err error) {
		reported <- err
	}
	runtime := newHeartbeatRuntimeForTest(t, writer, config)

	for index := 0; index < config.MaxCollectors; index++ {
		lease := heartbeatRuntimeLease(
			"tenant-a",
			fmt.Sprintf("collector-cycle-%02d", index),
			1,
		)
		if err := runtime.Activate(lease); err != nil {
			t.Fatalf("Activate(%d): %v", index, err)
		}
		if accepted, err := runtime.Offer(
			context.Background(),
			lease,
			heartbeatRuntimeHeartbeat(1),
		); err != nil || !accepted {
			t.Fatalf("Offer(%d) = %t, %v", index, accepted, err)
		}
	}

	if err := runtime.Flush(context.Background()); !errors.Is(
		err,
		context.DeadlineExceeded,
	) {
		t.Fatalf("Flush(blocked cycle) error = %v, want DeadlineExceeded", err)
	}
	if calls := writer.Calls(); len(calls) != 1 {
		t.Fatalf(
			"blocked cycle made %d writer calls, want exactly 1",
			len(calls),
		)
	}
	reportedErr := heartbeatRuntimeAwaitError(
		t,
		reported,
		"owned flush-cycle timeout callback",
	)
	if !errors.Is(reportedErr, errHeartbeatRuntimeWriteTimeout) ||
		!errors.Is(reportedErr, context.DeadlineExceeded) {
		t.Fatalf(
			"OnError timeout = %v, want owned timeout and DeadlineExceeded",
			reportedErr,
		)
	}

	if err := runtime.Flush(context.Background()); err != nil {
		t.Fatalf("Flush(retry all): %v", err)
	}
	calls := writer.Calls()
	if len(calls) != config.MaxCollectors+1 {
		t.Fatalf(
			"writer calls after retry = %d, want %d",
			len(calls),
			config.MaxCollectors+1,
		)
	}
	retried := make(map[string]struct{}, config.MaxCollectors)
	for _, call := range calls[1:] {
		retried[call.lease.CollectorID] = struct{}{}
	}
	if len(retried) != config.MaxCollectors {
		t.Fatalf(
			"retry persisted %d distinct collectors, want %d: %#v",
			len(retried),
			config.MaxCollectors,
			retried,
		)
	}
}

func TestHeartbeatRuntimeCapacityReplacementAndRelease(t *testing.T) {
	t.Parallel()

	clock := newHeartbeatRuntimeClock(time.Date(2040, 1, 1, 3, 0, 0, 0, time.UTC))
	writer := newHeartbeatRuntimeWriter()
	config := heartbeatRuntimeConfig(clock)
	config.MaxCollectors = 1
	runtime := newHeartbeatRuntimeForTest(t, writer, config)
	first := heartbeatRuntimeLease("tenant-a", "collector-a", 1)
	successor := heartbeatRuntimeLease("tenant-a", "collector-a", 2)
	other := heartbeatRuntimeLease("tenant-a", "collector-b", 1)

	if err := runtime.Activate(Lease{}); !errors.Is(err, control.ErrInvalidArgument) {
		t.Fatalf("Activate(invalid lease) error = %v, want ErrInvalidArgument", err)
	}
	if err := runtime.Activate(first); err != nil {
		t.Fatalf("Activate(first): %v", err)
	}
	if err := runtime.Activate(other); !errors.Is(err, control.ErrCapacityExceeded) {
		t.Fatalf("Activate(new key at capacity) error = %v, want ErrCapacityExceeded", err)
	}
	if err := runtime.Activate(successor); err != nil {
		t.Fatalf("Activate(successor replacement): %v", err)
	}
	if err := runtime.Activate(successor); !errors.Is(err, ErrHeartbeatLeaseNotActive) {
		t.Fatalf("Activate(equal generation) error = %v, want ErrHeartbeatLeaseNotActive", err)
	}
	if err := runtime.Activate(first); !errors.Is(err, ErrHeartbeatLeaseNotActive) {
		t.Fatalf("Activate(older generation) error = %v, want ErrHeartbeatLeaseNotActive", err)
	}
	if err := runtime.Release(context.Background(), first); err != nil {
		t.Fatalf("Release(old generation): %v", err)
	}
	if err := runtime.Activate(other); !errors.Is(err, control.ErrCapacityExceeded) {
		t.Fatalf("old release freed successor capacity: %v", err)
	}
	if state, ok := runtime.Liveness(successor); !ok || state != LivenessStateOnline {
		t.Fatalf("successor liveness after old release = %q, %t", state, ok)
	}
	if err := runtime.Release(context.Background(), successor); err != nil {
		t.Fatalf("Release(successor): %v", err)
	}
	if _, ok := runtime.Liveness(successor); ok {
		t.Fatal("released successor still occupies liveness registry")
	}
	if err := runtime.Activate(other); err != nil {
		t.Fatalf("Activate(new key after release): %v", err)
	}
}

func TestHeartbeatRuntimeAppliedFalseDropsPendingWithoutDemotingLiveness(t *testing.T) {
	t.Parallel()

	clock := newHeartbeatRuntimeClock(time.Date(2040, 1, 1, 4, 0, 0, 0, time.UTC))
	writer := newHeartbeatRuntimeWriter(
		heartbeatRuntimeWriteResponse{applied: false},
		heartbeatRuntimeWriteResponse{applied: true},
	)
	runtime := newHeartbeatRuntimeForTest(t, writer, heartbeatRuntimeConfig(clock))
	lease := heartbeatRuntimeLease("tenant-a", "collector-false", 1)
	if err := runtime.Activate(lease); err != nil {
		t.Fatal(err)
	}
	if accepted, err := runtime.Offer(
		context.Background(),
		lease,
		heartbeatRuntimeHeartbeat(1),
	); err != nil || !accepted {
		t.Fatalf("Offer(1) = %t, %v", accepted, err)
	}
	if err := runtime.Flush(context.Background()); err != nil {
		t.Fatalf("Flush(applied=false): %v", err)
	}
	if state, ok := runtime.Liveness(lease); !ok || state != LivenessStateOnline {
		t.Fatalf("liveness after applied=false = %q, %t", state, ok)
	}
	if err := runtime.Flush(context.Background()); err != nil {
		t.Fatalf("Flush(after applied=false): %v", err)
	}
	if calls := writer.Calls(); len(calls) != 1 {
		t.Fatalf("applied=false heartbeat was retried: %#v", calls)
	}

	if accepted, err := runtime.Offer(
		context.Background(),
		lease,
		heartbeatRuntimeHeartbeat(2),
	); err != nil || !accepted {
		t.Fatalf("Offer(2) = %t, %v", accepted, err)
	}
	if err := runtime.Flush(context.Background()); err != nil {
		t.Fatalf("Flush(2): %v", err)
	}
	calls := writer.Calls()
	if len(calls) != 2 ||
		calls[0].heartbeat.ObservationSequence != 1 ||
		calls[1].heartbeat.ObservationSequence != 2 {
		t.Fatalf("writer calls = %#v", calls)
	}
}

func TestHeartbeatRuntimeAcceptedOfferDoesNotRetainCallerContext(t *testing.T) {
	t.Parallel()

	clock := newHeartbeatRuntimeClock(time.Date(2040, 1, 1, 5, 0, 0, 0, time.UTC))
	writer := newHeartbeatRuntimeWriter()
	runtime := newHeartbeatRuntimeForTest(t, writer, heartbeatRuntimeConfig(clock))
	lease := heartbeatRuntimeLease("tenant-a", "collector-context", 1)
	if err := runtime.Activate(lease); err != nil {
		t.Fatal(err)
	}
	offerContext, cancel := context.WithCancel(context.Background())
	if accepted, err := runtime.Offer(
		offerContext,
		lease,
		heartbeatRuntimeHeartbeat(1),
	); err != nil || !accepted {
		t.Fatalf("Offer() = %t, %v", accepted, err)
	}
	cancel()

	if err := runtime.Flush(context.Background()); err != nil {
		t.Fatalf("Flush(): %v", err)
	}
	calls := writer.Calls()
	if len(calls) != 1 {
		t.Fatalf("writer calls = %d, want 1", len(calls))
	}
	if calls[0].contextAtEntry != nil {
		t.Fatalf("writer inherited canceled Offer context: %v", calls[0].contextAtEntry)
	}
}

func TestHeartbeatRuntimeLivenessUsesMonotonicAcceptanceDeadline(t *testing.T) {
	t.Parallel()

	clock := newHeartbeatRuntimeClock(time.Date(2040, 1, 1, 6, 0, 0, 0, time.UTC))
	writer := newHeartbeatRuntimeWriter()
	config := heartbeatRuntimeConfig(clock)
	runtime := newHeartbeatRuntimeForTest(t, writer, config)
	lease := heartbeatRuntimeLease("tenant-a", "collector-liveness", 1)
	if err := runtime.Activate(lease); err != nil {
		t.Fatal(err)
	}
	if state, ok := runtime.Liveness(lease); !ok || state != LivenessStateOnline {
		t.Fatalf("liveness after Activate = %q, %t", state, ok)
	}

	deadline := config.HeartbeatInterval + config.StaleGrace
	clock.Advance(deadline - time.Nanosecond)
	if state, ok := runtime.Liveness(lease); !ok || state != LivenessStateOnline {
		t.Fatalf("liveness before deadline = %q, %t", state, ok)
	}
	clock.Advance(time.Nanosecond)
	if state, ok := runtime.Liveness(lease); !ok || state != LivenessStateStale {
		t.Fatalf("liveness at deadline = %q, %t", state, ok)
	}

	// Persisted wall timestamps are intentionally unrelated to the process
	// monotonic deadline. Acceptance time, not either timestamp, revives it.
	heartbeat := heartbeatRuntimeHeartbeat(1)
	heartbeat.ObservedAt = time.Date(2001, 1, 1, 0, 0, 0, 0, time.UTC)
	heartbeat.ReceivedAt = time.Date(2099, 1, 1, 0, 0, 0, 0, time.UTC)
	if accepted, err := runtime.Offer(
		context.Background(),
		lease,
		heartbeat,
	); err != nil || !accepted {
		t.Fatalf("Offer(wall-clock extremes) = %t, %v", accepted, err)
	}
	if state, ok := runtime.Liveness(lease); !ok || state != LivenessStateOnline {
		t.Fatalf("liveness after accepted heartbeat = %q, %t", state, ok)
	}
	clock.Advance(deadline)
	if state, ok := runtime.Liveness(lease); !ok || state != LivenessStateStale {
		t.Fatalf("liveness at refreshed deadline = %q, %t", state, ok)
	}
}

func TestHeartbeatRuntimeSnapshotLivenessIsTenantScopedSortedAndDetached(
	t *testing.T,
) {
	t.Parallel()

	clock := newHeartbeatRuntimeClock(time.Date(2040, 1, 1, 6, 0, 0, 0, time.UTC))
	writer := newHeartbeatRuntimeWriter()
	config := heartbeatRuntimeConfig(clock)
	runtime := newHeartbeatRuntimeForTest(t, writer, config)

	staleLease := heartbeatRuntimeLease("tenant-a", "collector-z", 7)
	if err := runtime.Activate(staleLease); err != nil {
		t.Fatal(err)
	}
	clock.Advance(config.HeartbeatInterval + config.StaleGrace)

	onlineLease := heartbeatRuntimeLease("tenant-a", "collector-a", 3)
	if err := runtime.Activate(onlineLease); err != nil {
		t.Fatal(err)
	}
	if err := runtime.Activate(
		heartbeatRuntimeLease("tenant-b", "collector-b", 1),
	); err != nil {
		t.Fatal(err)
	}

	snapshot, err := runtime.SnapshotLiveness(Scope{TenantID: "tenant-a"})
	if err != nil {
		t.Fatalf("SnapshotLiveness(): %v", err)
	}
	if len(snapshot) != 2 {
		t.Fatalf("snapshot length = %d, want 2", len(snapshot))
	}
	if snapshot[0].Lease != onlineLease ||
		snapshot[0].State != LivenessStateOnline {
		t.Fatalf("snapshot[0] = %+v, want online lease %+v", snapshot[0], onlineLease)
	}
	if snapshot[1].Lease != staleLease ||
		snapshot[1].State != LivenessStateStale {
		t.Fatalf("snapshot[1] = %+v, want stale lease %+v", snapshot[1], staleLease)
	}

	snapshot[0].Lease.CollectorID = "mutated"
	fresh, err := runtime.SnapshotLiveness(Scope{TenantID: "tenant-a"})
	if err != nil {
		t.Fatalf("SnapshotLiveness() after mutation: %v", err)
	}
	if fresh[0].Lease != onlineLease {
		t.Fatalf("fresh snapshot was aliased: %+v", fresh[0])
	}

	if err := runtime.Release(context.Background(), onlineLease); err != nil {
		t.Fatalf("Release(): %v", err)
	}
	afterRelease, err := runtime.SnapshotLiveness(Scope{TenantID: "tenant-a"})
	if err != nil {
		t.Fatalf("SnapshotLiveness() after Release: %v", err)
	}
	if len(afterRelease) != 1 || afterRelease[0].Lease != staleLease {
		t.Fatalf("snapshot after Release = %+v, want only stale lease", afterRelease)
	}
}

func TestHeartbeatRuntimeSnapshotLivenessRejectsInvalidScopeAndClosedRuntime(
	t *testing.T,
) {
	t.Parallel()

	clock := newHeartbeatRuntimeClock(time.Date(2040, 1, 1, 6, 0, 0, 0, time.UTC))
	runtime := newHeartbeatRuntimeForTest(
		t,
		newHeartbeatRuntimeWriter(),
		heartbeatRuntimeConfig(clock),
	)
	if _, err := runtime.SnapshotLiveness(Scope{TenantID: " tenant-a"}); !errors.Is(
		err,
		control.ErrInvalidArgument,
	) {
		t.Fatalf("SnapshotLiveness(invalid scope) error = %v, want ErrInvalidArgument", err)
	}
	if err := runtime.Close(context.Background()); err != nil {
		t.Fatalf("Close(): %v", err)
	}
	if _, err := runtime.SnapshotLiveness(Scope{TenantID: "tenant-a"}); !errors.Is(
		err,
		ErrHeartbeatRuntimeClosed,
	) {
		t.Fatalf("SnapshotLiveness(closed) error = %v, want ErrHeartbeatRuntimeClosed", err)
	}

	var nilRuntime *HeartbeatRuntime
	if _, err := nilRuntime.SnapshotLiveness(Scope{TenantID: "tenant-a"}); !errors.Is(
		err,
		ErrHeartbeatRuntimeClosed,
	) {
		t.Fatalf("nil SnapshotLiveness() error = %v, want ErrHeartbeatRuntimeClosed", err)
	}
}

func TestHeartbeatRuntimeSnapshotLivenessSamplesClockUnderReadLock(t *testing.T) {
	t.Parallel()

	now := time.Date(2040, 1, 1, 6, 0, 0, 0, time.UTC)
	snapshotClockEntered := make(chan struct{})
	allowSnapshotClock := make(chan struct{})
	var allowSnapshotOnce sync.Once
	allowSnapshot := func() {
		allowSnapshotOnce.Do(func() { close(allowSnapshotClock) })
	}
	activationClockCalled := make(chan struct{})
	var clockMu sync.Mutex
	clockCalls := 0
	monotonicNow := func() time.Time {
		clockMu.Lock()
		clockCalls++
		call := clockCalls
		clockMu.Unlock()
		switch call {
		case 2:
			close(snapshotClockEntered)
			<-allowSnapshotClock
		case 3:
			close(activationClockCalled)
		}
		return now
	}
	config := HeartbeatRuntimeConfig{
		MaxCollectors:     4,
		HeartbeatInterval: 10 * time.Second,
		StaleGrace:        5 * time.Second,
		FlushInterval:     time.Hour,
		WriteTimeout:      5 * time.Second,
		MonotonicNow:      monotonicNow,
	}
	runtime := newHeartbeatRuntimeForTest(
		t,
		newHeartbeatRuntimeWriter(),
		config,
	)
	t.Cleanup(allowSnapshot)
	first := heartbeatRuntimeLease("tenant-a", "collector-a", 1)
	if err := runtime.Activate(first); err != nil {
		t.Fatal(err)
	}

	type snapshotResult struct {
		entries []CollectorLiveness
		err     error
	}
	snapshotDone := make(chan snapshotResult, 1)
	go func() {
		entries, err := runtime.SnapshotLiveness(Scope{TenantID: "tenant-a"})
		snapshotDone <- snapshotResult{entries: entries, err: err}
	}()
	heartbeatRuntimeAwaitSignal(t, snapshotClockEntered, "snapshot clock entry")

	activateDone := make(chan error, 1)
	go func() {
		activateDone <- runtime.Activate(
			heartbeatRuntimeLease("tenant-a", "collector-b", 1),
		)
	}()
	heartbeatRuntimeAwaitSignal(t, activationClockCalled, "activation clock call")
	select {
	case err := <-activateDone:
		t.Fatalf("Activate completed inside snapshot sample: %v", err)
	default:
	}

	allowSnapshot()
	result := <-snapshotDone
	if result.err != nil {
		t.Fatalf("SnapshotLiveness(): %v", result.err)
	}
	if len(result.entries) != 1 || result.entries[0].Lease != first {
		t.Fatalf("snapshot = %+v, want only initial lease", result.entries)
	}
	if err := <-activateDone; err != nil {
		t.Fatalf("Activate() after snapshot: %v", err)
	}
}

func TestHeartbeatRuntimeWriterErrorRetainsOnlyLatestForRetry(t *testing.T) {
	t.Parallel()

	writeErr := errors.New("injected heartbeat write failure")
	entered := make(chan struct{})
	release := make(chan struct{})
	writer := newHeartbeatRuntimeWriter(
		heartbeatRuntimeWriteResponse{
			err:     writeErr,
			entered: entered,
			release: release,
		},
		heartbeatRuntimeWriteResponse{applied: true},
	)
	clock := newHeartbeatRuntimeClock(time.Date(2040, 1, 1, 7, 0, 0, 0, time.UTC))
	runtime := newHeartbeatRuntimeForTest(t, writer, heartbeatRuntimeConfig(clock))
	lease := heartbeatRuntimeLease("tenant-a", "collector-retry", 1)
	if err := runtime.Activate(lease); err != nil {
		t.Fatal(err)
	}
	if accepted, err := runtime.Offer(
		context.Background(),
		lease,
		heartbeatRuntimeHeartbeat(1),
	); err != nil || !accepted {
		t.Fatalf("Offer(1) = %t, %v", accepted, err)
	}

	flushDone := make(chan error, 1)
	go func() {
		flushDone <- runtime.Flush(context.Background())
	}()
	heartbeatRuntimeAwaitSignal(t, entered, "first writer entry")
	if accepted, err := runtime.Offer(
		context.Background(),
		lease,
		heartbeatRuntimeHeartbeat(2),
	); err != nil || !accepted {
		t.Fatalf("Offer(2 while 1 is in flight) = %t, %v", accepted, err)
	}
	close(release)
	if err := heartbeatRuntimeAwaitError(t, flushDone, "failed flush"); !errors.Is(err, writeErr) {
		t.Fatalf("Flush(first) error = %v, want injected error", err)
	}

	if err := runtime.Flush(context.Background()); err != nil {
		t.Fatalf("Flush(retry latest): %v", err)
	}
	if err := runtime.Flush(context.Background()); err != nil {
		t.Fatalf("Flush(empty after retry): %v", err)
	}
	calls := writer.Calls()
	if len(calls) != 2 ||
		calls[0].heartbeat.ObservationSequence != 1 ||
		calls[1].heartbeat.ObservationSequence != 2 {
		t.Fatalf("writer error overwrote newer pending heartbeat: %#v", calls)
	}
}

func TestHeartbeatRuntimeWriteTimeoutRetainsHeartbeatForRetry(t *testing.T) {
	t.Parallel()

	entered := make(chan struct{})
	neverRelease := make(chan struct{})
	writer := newHeartbeatRuntimeWriter(
		heartbeatRuntimeWriteResponse{
			entered: entered,
			release: neverRelease,
		},
		heartbeatRuntimeWriteResponse{applied: true},
	)
	clock := newHeartbeatRuntimeClock(time.Date(2040, 1, 1, 7, 30, 0, 0, time.UTC))
	config := heartbeatRuntimeConfig(clock)
	config.WriteTimeout = 20 * time.Millisecond
	runtime := newHeartbeatRuntimeForTest(t, writer, config)
	lease := heartbeatRuntimeLease("tenant-a", "collector-write-timeout", 1)
	if err := runtime.Activate(lease); err != nil {
		t.Fatal(err)
	}
	if accepted, err := runtime.Offer(
		context.Background(),
		lease,
		heartbeatRuntimeHeartbeat(1),
	); err != nil || !accepted {
		t.Fatalf("Offer() = %t, %v", accepted, err)
	}

	flushDone := make(chan error, 1)
	go func() {
		flushDone <- runtime.Flush(context.Background())
	}()
	heartbeatRuntimeAwaitSignal(t, entered, "write-timeout writer entry")
	if err := heartbeatRuntimeAwaitError(t, flushDone, "write timeout"); !errors.Is(
		err,
		context.DeadlineExceeded,
	) {
		t.Fatalf("Flush(blocked writer) error = %v, want DeadlineExceeded", err)
	}
	if err := runtime.Flush(context.Background()); err != nil {
		t.Fatalf("Flush(retry): %v", err)
	}
	calls := writer.Calls()
	if len(calls) != 2 ||
		calls[0].heartbeat.ObservationSequence != 1 ||
		calls[1].heartbeat.ObservationSequence != 1 ||
		calls[0].contextAtEntry != nil ||
		calls[1].contextAtEntry != nil {
		t.Fatalf("write-timeout retry calls = %#v", calls)
	}
}

func TestHeartbeatRuntimeOnErrorRunsOutsideRuntimeLocks(t *testing.T) {
	t.Parallel()

	writeErr := errors.New("injected callback failure")
	writer := newHeartbeatRuntimeWriter(
		heartbeatRuntimeWriteResponse{err: writeErr},
		heartbeatRuntimeWriteResponse{applied: true},
	)
	clock := newHeartbeatRuntimeClock(time.Date(2040, 1, 1, 8, 0, 0, 0, time.UTC))
	lease := heartbeatRuntimeLease("tenant-a", "collector-callback", 1)
	callbackDone := make(chan error, 1)
	var runtime *HeartbeatRuntime
	config := heartbeatRuntimeConfig(clock)
	config.OnError = func(err error) {
		if !errors.Is(err, writeErr) {
			callbackDone <- fmt.Errorf("callback error: %w", err)
			return
		}
		if state, ok := runtime.Liveness(lease); !ok || state != LivenessStateOnline {
			callbackDone <- fmt.Errorf("callback liveness = %q, %t", state, ok)
			return
		}
		// Re-entering Flush proves the callback owns neither the state mutex nor
		// the global flush mutex. The scripted retry succeeds and cannot recurse.
		callbackDone <- runtime.Flush(context.Background())
	}
	var err error
	runtime, err = NewHeartbeatRuntime(writer, config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { heartbeatRuntimeClose(t, runtime) })
	if err := runtime.Activate(lease); err != nil {
		t.Fatal(err)
	}
	if accepted, err := runtime.Offer(
		context.Background(),
		lease,
		heartbeatRuntimeHeartbeat(1),
	); err != nil || !accepted {
		t.Fatalf("Offer() = %t, %v", accepted, err)
	}
	if err := runtime.Flush(context.Background()); !errors.Is(err, writeErr) {
		t.Fatalf("Flush() error = %v, want injected error", err)
	}
	if callbackErr := heartbeatRuntimeAwaitError(
		t,
		callbackDone,
		"reentrant OnError callback",
	); callbackErr != nil {
		t.Fatalf("OnError callback: %v", callbackErr)
	}
	calls := writer.Calls()
	if len(calls) != 2 ||
		calls[0].heartbeat.ObservationSequence != 1 ||
		calls[1].heartbeat.ObservationSequence != 1 {
		t.Fatalf("reentrant error retry calls = %#v", calls)
	}
}

func TestHeartbeatRuntimeReportsRealFailureInsideJoinedCancellation(t *testing.T) {
	t.Parallel()

	writeErr := errors.New("injected durable heartbeat failure")
	entered := make(chan struct{})
	releaseWriter := make(chan struct{})
	var unblockWriter sync.Once
	unblock := func() {
		unblockWriter.Do(func() { close(releaseWriter) })
	}
	defer unblock()
	writer := newHeartbeatRuntimeWriter(heartbeatRuntimeWriteResponse{
		err:           writeErr,
		entered:       entered,
		release:       releaseWriter,
		ignoreContext: true,
	})
	clock := newHeartbeatRuntimeClock(time.Date(2040, 1, 1, 8, 30, 0, 0, time.UTC))
	reported := make(chan error, 1)
	config := heartbeatRuntimeConfig(clock)
	config.MaxCollectors = 2
	config.OnError = func(err error) {
		reported <- err
	}
	runtime := newHeartbeatRuntimeForTest(t, writer, config)
	for _, lease := range []Lease{
		heartbeatRuntimeLease("tenant-a", "collector-joined-a", 1),
		heartbeatRuntimeLease("tenant-a", "collector-joined-b", 1),
	} {
		if err := runtime.Activate(lease); err != nil {
			t.Fatal(err)
		}
		if accepted, err := runtime.Offer(
			context.Background(),
			lease,
			heartbeatRuntimeHeartbeat(1),
		); err != nil || !accepted {
			t.Fatalf("Offer(%s) = %t, %v", lease.CollectorID, accepted, err)
		}
	}

	flushContext, cancelFlush := context.WithCancel(context.Background())
	defer cancelFlush()
	flushDone := make(chan error, 1)
	go func() {
		flushDone <- runtime.Flush(flushContext)
	}()
	heartbeatRuntimeAwaitSignal(t, entered, "first joined-error writer entry")
	cancelFlush()
	unblock()
	flushErr := heartbeatRuntimeAwaitError(t, flushDone, "joined-error flush")
	if !errors.Is(flushErr, writeErr) || !errors.Is(flushErr, context.Canceled) {
		t.Fatalf(
			"Flush() error = %v, want joined durable failure and context cancellation",
			flushErr,
		)
	}
	reportedErr := heartbeatRuntimeAwaitError(t, reported, "joined real-error callback")
	if !errors.Is(reportedErr, writeErr) {
		t.Fatalf("OnError suppressed real failure inside joined cancellation: %v", reportedErr)
	}
	if calls := writer.Calls(); len(calls) != 1 {
		t.Fatalf("canceled joined flush made %d writer calls, want 1", len(calls))
	}
}

func TestHeartbeatRuntimePeriodicOnErrorCanCloseRuntime(t *testing.T) {
	t.Parallel()

	writeErr := errors.New("periodic heartbeat failure")
	entered := make(chan struct{})
	writer := newHeartbeatRuntimeWriter(
		heartbeatRuntimeWriteResponse{
			err:     writeErr,
			entered: entered,
		},
		heartbeatRuntimeWriteResponse{applied: true},
	)
	clock := newHeartbeatRuntimeClock(time.Date(2040, 1, 1, 8, 40, 0, 0, time.UTC))
	callbackDone := make(chan error, 1)
	var runtime *HeartbeatRuntime
	config := heartbeatRuntimeConfig(clock)
	config.FlushInterval = 2 * time.Millisecond
	config.OnError = func(err error) {
		if !errors.Is(err, writeErr) {
			callbackDone <- fmt.Errorf("periodic callback error: %w", err)
			return
		}
		closeContext, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		callbackDone <- runtime.Close(closeContext)
	}
	var err error
	runtime, err = NewHeartbeatRuntime(writer, config)
	if err != nil {
		t.Fatal(err)
	}
	lease := heartbeatRuntimeLease("tenant-a", "collector-callback-close", 1)
	if err := runtime.Activate(lease); err != nil {
		t.Fatal(err)
	}
	if accepted, err := runtime.Offer(
		context.Background(),
		lease,
		heartbeatRuntimeHeartbeat(1),
	); err != nil || !accepted {
		t.Fatalf("Offer() = %t, %v", accepted, err)
	}
	heartbeatRuntimeAwaitSignal(t, entered, "periodic failed writer entry")
	if callbackErr := heartbeatRuntimeAwaitError(
		t,
		callbackDone,
		"periodic OnError callback Close",
	); callbackErr != nil {
		t.Fatalf("OnError Close(): %v", callbackErr)
	}
	if err := runtime.Activate(heartbeatRuntimeLease("tenant-a", "after-close", 1)); !errors.Is(
		err,
		ErrHeartbeatRuntimeClosed,
	) {
		t.Fatalf("runtime remained open after callback Close: %v", err)
	}
	if writer.Active() != 0 {
		t.Fatalf("active writers after callback Close = %d", writer.Active())
	}
	calls := writer.Calls()
	if len(calls) != 2 ||
		calls[0].heartbeat.ObservationSequence != 1 ||
		calls[1].heartbeat.ObservationSequence != 1 {
		t.Fatalf("callback Close final retry calls = %#v", calls)
	}
}

func TestHeartbeatRuntimeOnErrorDeliveryIsBoundedWhileCallbackBlocks(t *testing.T) {
	t.Parallel()

	writeErr := errors.New("repeated periodic heartbeat failure")
	responses := make([]heartbeatRuntimeWriteResponse, 4)
	for index := range responses {
		responses[index].err = writeErr
	}
	writer := newHeartbeatRuntimeWriter(responses...)
	clock := newHeartbeatRuntimeClock(time.Date(2040, 1, 1, 8, 50, 0, 0, time.UTC))
	firstCallback := make(chan struct{})
	releaseCallbacks := make(chan struct{})
	var callbackSignal sync.Once
	var callbackMu sync.Mutex
	callbackCalls := 0
	activeCallbacks := 0
	maxActiveCallbacks := 0
	config := heartbeatRuntimeConfig(clock)
	config.FlushInterval = 2 * time.Millisecond
	config.OnError = func(err error) {
		if !errors.Is(err, writeErr) {
			return
		}
		callbackMu.Lock()
		callbackCalls++
		activeCallbacks++
		if activeCallbacks > maxActiveCallbacks {
			maxActiveCallbacks = activeCallbacks
		}
		callbackMu.Unlock()
		callbackSignal.Do(func() { close(firstCallback) })
		<-releaseCallbacks
		callbackMu.Lock()
		activeCallbacks--
		callbackMu.Unlock()
	}
	runtime, err := NewHeartbeatRuntime(writer, config)
	if err != nil {
		t.Fatal(err)
	}
	lease := heartbeatRuntimeLease("tenant-a", "collector-callback-bounded", 1)
	if err := runtime.Activate(lease); err != nil {
		t.Fatal(err)
	}
	if accepted, err := runtime.Offer(
		context.Background(),
		lease,
		heartbeatRuntimeHeartbeat(1),
	); err != nil || !accepted {
		t.Fatalf("Offer() = %t, %v", accepted, err)
	}
	heartbeatRuntimeAwaitSignal(t, firstCallback, "first blocked OnError callback")
	heartbeatRuntimeEventually(t, "periodic retries while callback is blocked", func() bool {
		return len(writer.Calls()) >= 3
	})
	callbackMu.Lock()
	blockedCalls := callbackCalls
	blockedActive := activeCallbacks
	blockedMaximum := maxActiveCallbacks
	callbackMu.Unlock()
	if blockedCalls != 1 || blockedActive != 1 || blockedMaximum != 1 {
		t.Fatalf(
			"blocked callback delivery calls=%d active=%d max-active=%d, want 1/1/1",
			blockedCalls,
			blockedActive,
			blockedMaximum,
		)
	}
	close(releaseCallbacks)
	heartbeatRuntimeEventually(t, "failed heartbeat to reach successful retry", func() bool {
		return len(writer.Calls()) >= len(responses)+1
	})
	heartbeatRuntimeEventually(t, "OnError callbacks to drain", func() bool {
		callbackMu.Lock()
		defer callbackMu.Unlock()
		return activeCallbacks == 0
	})
	if err := runtime.Close(context.Background()); err != nil {
		t.Fatalf("Close(): %v", err)
	}
	callbackMu.Lock()
	maximum := maxActiveCallbacks
	callbackMu.Unlock()
	if maximum > 1 {
		t.Fatalf("maximum concurrent OnError callbacks = %d, want at most 1", maximum)
	}
}

func TestHeartbeatRuntimeOlderGenerationCannotAffectSuccessor(t *testing.T) {
	t.Parallel()

	clock := newHeartbeatRuntimeClock(time.Date(2040, 1, 1, 9, 0, 0, 0, time.UTC))
	writer := newHeartbeatRuntimeWriter()
	runtime := newHeartbeatRuntimeForTest(t, writer, heartbeatRuntimeConfig(clock))
	older := heartbeatRuntimeLease("tenant-a", "collector-generation", 1)
	successor := heartbeatRuntimeLease("tenant-a", "collector-generation", 2)
	if err := runtime.Activate(older); err != nil {
		t.Fatal(err)
	}
	if accepted, err := runtime.Offer(
		context.Background(),
		older,
		heartbeatRuntimeHeartbeat(10),
	); err != nil || !accepted {
		t.Fatalf("Offer(old pending) = %t, %v", accepted, err)
	}
	if err := runtime.Activate(successor); err != nil {
		t.Fatalf("Activate(successor): %v", err)
	}
	if accepted, err := runtime.Offer(
		context.Background(),
		successor,
		heartbeatRuntimeHeartbeat(1),
	); err != nil || !accepted {
		t.Fatalf("Offer(successor) = %t, %v", accepted, err)
	}

	if err := runtime.Activate(older); !errors.Is(err, ErrHeartbeatLeaseNotActive) {
		t.Fatalf("Activate(older) error = %v", err)
	}
	if accepted, err := runtime.Offer(
		context.Background(),
		older,
		heartbeatRuntimeHeartbeat(11),
	); accepted || !errors.Is(err, ErrHeartbeatLeaseNotActive) {
		t.Fatalf("Offer(older) = %t, %v", accepted, err)
	}
	impostor := successor
	impostor.StreamID = "different-stream"
	if accepted, err := runtime.Offer(
		context.Background(),
		impostor,
		heartbeatRuntimeHeartbeat(2),
	); accepted || !errors.Is(err, ErrHeartbeatLeaseNotActive) {
		t.Fatalf("Offer(same generation, wrong tuple) = %t, %v", accepted, err)
	}
	if err := runtime.Release(context.Background(), older); err != nil {
		t.Fatalf("Release(older): %v", err)
	}
	if err := runtime.Release(context.Background(), impostor); err != nil {
		t.Fatalf("Release(impostor): %v", err)
	}
	if state, ok := runtime.Liveness(successor); !ok || state != LivenessStateOnline {
		t.Fatalf("successor liveness = %q, %t", state, ok)
	}
	if err := runtime.Flush(context.Background()); err != nil {
		t.Fatalf("Flush(successor): %v", err)
	}
	calls := writer.Calls()
	if len(calls) != 1 ||
		calls[0].lease != successor ||
		calls[0].heartbeat.ObservationSequence != 1 {
		t.Fatalf("old generation affected successor persistence: %#v", calls)
	}
}

func TestHeartbeatRuntimeOldFlushCompletionCannotOverwriteSuccessor(t *testing.T) {
	t.Parallel()

	writeErr := errors.New("generation-one write failed")
	entered := make(chan struct{})
	release := make(chan struct{})
	writer := newHeartbeatRuntimeWriter(
		heartbeatRuntimeWriteResponse{
			err:     writeErr,
			entered: entered,
			release: release,
		},
		heartbeatRuntimeWriteResponse{applied: true},
	)
	clock := newHeartbeatRuntimeClock(time.Date(2040, 1, 1, 10, 0, 0, 0, time.UTC))
	runtime := newHeartbeatRuntimeForTest(t, writer, heartbeatRuntimeConfig(clock))
	older := heartbeatRuntimeLease("tenant-a", "collector-inflight", 1)
	successor := heartbeatRuntimeLease("tenant-a", "collector-inflight", 2)
	if err := runtime.Activate(older); err != nil {
		t.Fatal(err)
	}
	if accepted, err := runtime.Offer(
		context.Background(),
		older,
		heartbeatRuntimeHeartbeat(50),
	); err != nil || !accepted {
		t.Fatalf("Offer(old) = %t, %v", accepted, err)
	}
	flushDone := make(chan error, 1)
	go func() {
		flushDone <- runtime.Flush(context.Background())
	}()
	heartbeatRuntimeAwaitSignal(t, entered, "generation-one writer entry")

	if err := runtime.Activate(successor); err != nil {
		t.Fatalf("Activate(successor): %v", err)
	}
	if accepted, err := runtime.Offer(
		context.Background(),
		successor,
		heartbeatRuntimeHeartbeat(1),
	); err != nil || !accepted {
		t.Fatalf("Offer(successor) = %t, %v", accepted, err)
	}
	close(release)
	if err := heartbeatRuntimeAwaitError(t, flushDone, "old generation flush"); !errors.Is(
		err,
		writeErr,
	) {
		t.Fatalf("Flush(old generation) error = %v", err)
	}
	if state, ok := runtime.Liveness(successor); !ok || state != LivenessStateOnline {
		t.Fatalf("successor after old failure = %q, %t", state, ok)
	}
	if err := runtime.Flush(context.Background()); err != nil {
		t.Fatalf("Flush(successor): %v", err)
	}
	calls := writer.Calls()
	if len(calls) != 2 ||
		calls[0].lease != older ||
		calls[0].heartbeat.ObservationSequence != 50 ||
		calls[1].lease != successor ||
		calls[1].heartbeat.ObservationSequence != 1 {
		t.Fatalf("old failure completion corrupted successor: %#v", calls)
	}
}

func TestHeartbeatRuntimeReleaseMarksOfflineDrainsLatestAndPreservesSuccessor(t *testing.T) {
	t.Parallel()

	entered := make(chan struct{})
	releaseWriter := make(chan struct{})
	var unblockWriter sync.Once
	unblock := func() {
		unblockWriter.Do(func() { close(releaseWriter) })
	}
	defer unblock()
	writer := newHeartbeatRuntimeWriter(
		heartbeatRuntimeWriteResponse{
			applied: true,
			entered: entered,
			release: releaseWriter,
		},
		heartbeatRuntimeWriteResponse{applied: true},
	)
	clock := newHeartbeatRuntimeClock(time.Date(2040, 1, 1, 11, 0, 0, 0, time.UTC))
	runtime := newHeartbeatRuntimeForTest(t, writer, heartbeatRuntimeConfig(clock))
	older := heartbeatRuntimeLease("tenant-a", "collector-release", 1)
	successor := heartbeatRuntimeLease("tenant-a", "collector-release", 2)
	if err := runtime.Activate(older); err != nil {
		t.Fatal(err)
	}
	for sequence := uint64(1); sequence <= 2; sequence++ {
		if accepted, err := runtime.Offer(
			context.Background(),
			older,
			heartbeatRuntimeHeartbeat(sequence),
		); err != nil || !accepted {
			t.Fatalf("Offer(old %d) = %t, %v", sequence, accepted, err)
		}
	}

	releaseDone := make(chan error, 1)
	go func() {
		releaseDone <- runtime.Release(context.Background(), older)
	}()
	heartbeatRuntimeAwaitSignal(t, entered, "release drain writer entry")
	if state, ok := runtime.Liveness(older); ok || state != LivenessStateOffline {
		t.Fatalf("releasing lease liveness = %q, %t, want offline/false", state, ok)
	}
	if accepted, err := runtime.Offer(
		context.Background(),
		older,
		heartbeatRuntimeHeartbeat(3),
	); accepted || !errors.Is(err, ErrHeartbeatLeaseNotActive) {
		t.Fatalf("Offer(releasing lease) = %t, %v", accepted, err)
	}

	activateDone := make(chan error, 1)
	go func() {
		activateDone <- runtime.Activate(successor)
	}()
	if err := heartbeatRuntimeAwaitError(
		t,
		activateDone,
		"successor activation during old release",
	); err != nil {
		t.Fatalf("Activate(successor during release): %v", err)
	}
	if accepted, err := runtime.Offer(
		context.Background(),
		successor,
		heartbeatRuntimeHeartbeat(1),
	); err != nil || !accepted {
		t.Fatalf("Offer(successor) = %t, %v", accepted, err)
	}
	unblock()
	if err := heartbeatRuntimeAwaitError(t, releaseDone, "old release completion"); err != nil {
		t.Fatalf("Release(old): %v", err)
	}
	if err := runtime.Release(context.Background(), older); err != nil {
		t.Fatalf("Release(old after successor): %v", err)
	}
	if state, ok := runtime.Liveness(successor); !ok || state != LivenessStateOnline {
		t.Fatalf("successor after old release completion = %q, %t", state, ok)
	}
	if err := runtime.Flush(context.Background()); err != nil {
		t.Fatalf("Flush(successor): %v", err)
	}
	calls := writer.Calls()
	if len(calls) != 2 ||
		calls[0].lease != older ||
		calls[0].heartbeat.ObservationSequence != 2 ||
		calls[1].lease != successor ||
		calls[1].heartbeat.ObservationSequence != 1 {
		t.Fatalf("release did not drain latest or preserved successor incorrectly: %#v", calls)
	}
}

func TestHeartbeatRuntimeReleaseRetriesWriteFailedDuringConcurrentFlush(
	t *testing.T,
) {
	t.Parallel()

	writeErr := errors.New("transient heartbeat write failure")
	entered := make(chan struct{})
	releaseWriter := make(chan struct{})
	writer := newHeartbeatRuntimeWriter(
		heartbeatRuntimeWriteResponse{
			err:     writeErr,
			entered: entered,
			release: releaseWriter,
		},
		heartbeatRuntimeWriteResponse{applied: true},
	)
	clock := newHeartbeatRuntimeClock(
		time.Date(2040, 1, 1, 11, 15, 0, 0, time.UTC),
	)
	runtime := newHeartbeatRuntimeForTest(
		t,
		writer,
		heartbeatRuntimeConfig(clock),
	)
	lease := heartbeatRuntimeLease(
		"tenant-a",
		"collector-release-retry",
		1,
	)
	heartbeat := heartbeatRuntimeHeartbeat(9)
	if err := runtime.Activate(lease); err != nil {
		t.Fatal(err)
	}
	if accepted, err := runtime.Offer(
		context.Background(),
		lease,
		heartbeat,
	); err != nil || !accepted {
		t.Fatalf("Offer() = %t, %v", accepted, err)
	}

	flushDone := make(chan error, 1)
	go func() {
		flushDone <- runtime.Flush(context.Background())
	}()
	heartbeatRuntimeAwaitSignal(t, entered, "concurrent flush writer entry")

	releaseDone := make(chan error, 1)
	go func() {
		releaseDone <- runtime.Release(context.Background(), lease)
	}()
	heartbeatRuntimeEventually(t, "release to mark exact lease offline", func() bool {
		state, ok := runtime.Liveness(lease)
		return !ok && state == LivenessStateOffline
	})
	close(releaseWriter)

	if err := heartbeatRuntimeAwaitError(
		t,
		flushDone,
		"failed concurrent flush",
	); !errors.Is(err, writeErr) {
		t.Fatalf("Flush() error = %v, want %v", err, writeErr)
	}
	if err := heartbeatRuntimeAwaitError(
		t,
		releaseDone,
		"release retry",
	); err != nil {
		t.Fatalf("Release() retry: %v", err)
	}
	calls := writer.Calls()
	if len(calls) != 2 {
		t.Fatalf("writer calls = %d, want failed flush plus release retry", len(calls))
	}
	for index, call := range calls {
		if call.lease != lease ||
			call.heartbeat.ObservationSequence != heartbeat.ObservationSequence {
			t.Fatalf("writer call %d = %#v, want exact latest snapshot", index, call)
		}
	}
}

func TestHeartbeatRuntimeReleaseReportsFinalWriteFailure(t *testing.T) {
	t.Parallel()

	writeErr := errors.New("final heartbeat write failed")
	writer := newHeartbeatRuntimeWriter(
		heartbeatRuntimeWriteResponse{err: writeErr},
	)
	clock := newHeartbeatRuntimeClock(
		time.Date(2040, 1, 1, 11, 20, 0, 0, time.UTC),
	)
	reported := make(chan error, 1)
	config := heartbeatRuntimeConfig(clock)
	config.OnError = func(err error) {
		reported <- err
	}
	runtime := newHeartbeatRuntimeForTest(t, writer, config)
	lease := heartbeatRuntimeLease(
		"tenant-a",
		"collector-release-report",
		1,
	)
	if err := runtime.Activate(lease); err != nil {
		t.Fatal(err)
	}
	if accepted, err := runtime.Offer(
		context.Background(),
		lease,
		heartbeatRuntimeHeartbeat(4),
	); err != nil || !accepted {
		t.Fatalf("Offer() = %t, %v", accepted, err)
	}

	if err := runtime.Release(context.Background(), lease); !errors.Is(
		err,
		writeErr,
	) {
		t.Fatalf("Release() error = %v, want %v", err, writeErr)
	}
	reportedErr := heartbeatRuntimeAwaitError(
		t,
		reported,
		"failed final heartbeat callback",
	)
	if !errors.Is(reportedErr, writeErr) {
		t.Fatalf("OnError() = %v, want %v", reportedErr, writeErr)
	}
	if err := runtime.Release(context.Background(), lease); err != nil {
		t.Fatalf("second Release(): %v", err)
	}
	if calls := writer.Calls(); len(calls) != 1 {
		t.Fatalf("writer calls after second Release = %#v, want one", calls)
	}
}

func TestHeartbeatRuntimeReleaseReportsOnlyPendingSnapshotDroppedAtDeadline(
	t *testing.T,
) {
	t.Parallel()

	for _, pending := range []bool{false, true} {
		pending := pending
		t.Run(fmt.Sprintf("pending=%t", pending), func(t *testing.T) {
			t.Parallel()

			entered := make(chan struct{})
			releaseWriter := make(chan struct{})
			writer := newHeartbeatRuntimeWriter(
				heartbeatRuntimeWriteResponse{
					applied:       true,
					entered:       entered,
					release:       releaseWriter,
					ignoreContext: true,
				},
			)
			clock := newHeartbeatRuntimeClock(
				time.Date(2040, 1, 1, 11, 25, 0, 0, time.UTC),
			)
			reported := make(chan error, 1)
			config := heartbeatRuntimeConfig(clock)
			config.OnError = func(err error) {
				reported <- err
			}
			runtime := newHeartbeatRuntimeForTest(t, writer, config)
			blocker := heartbeatRuntimeLease(
				"tenant-a",
				fmt.Sprintf("collector-release-deadline-blocker-%t", pending),
				1,
			)
			releasing := heartbeatRuntimeLease(
				"tenant-a",
				fmt.Sprintf("collector-release-deadline-%t", pending),
				1,
			)
			if err := runtime.Activate(blocker); err != nil {
				t.Fatal(err)
			}
			if err := runtime.Activate(releasing); err != nil {
				t.Fatal(err)
			}
			if accepted, err := runtime.Offer(
				context.Background(),
				blocker,
				heartbeatRuntimeHeartbeat(1),
			); err != nil || !accepted {
				t.Fatalf("Offer(blocker) = %t, %v", accepted, err)
			}
			flushDone := make(chan error, 1)
			go func() {
				flushDone <- runtime.Flush(context.Background())
			}()
			heartbeatRuntimeAwaitSignal(t, entered, "gated unrelated flush")
			if pending {
				if accepted, err := runtime.Offer(
					context.Background(),
					releasing,
					heartbeatRuntimeHeartbeat(2),
				); err != nil || !accepted {
					t.Fatalf("Offer(releasing) = %t, %v", accepted, err)
				}
			}

			releaseContext, cancelRelease := context.WithCancel(
				context.Background(),
			)
			releaseDone := make(chan error, 1)
			go func() {
				releaseDone <- runtime.Release(releaseContext, releasing)
			}()
			heartbeatRuntimeEventually(
				t,
				"Release to mark exact lease inactive",
				func() bool {
					state, ok := runtime.Liveness(releasing)
					return !ok && state == LivenessStateOffline
				},
			)
			cancelRelease()
			close(releaseWriter)
			if err := heartbeatRuntimeAwaitError(
				t,
				flushDone,
				"unrelated flush completion",
			); err != nil {
				t.Fatalf("Flush(blocker): %v", err)
			}
			if err := heartbeatRuntimeAwaitError(
				t,
				releaseDone,
				"expired Release",
			); !errors.Is(err, context.Canceled) {
				t.Fatalf("Release() error = %v, want context.Canceled", err)
			}

			if pending {
				reportedErr := heartbeatRuntimeAwaitError(
					t,
					reported,
					"dropped pending heartbeat callback",
				)
				if !errors.Is(reportedErr, context.Canceled) ||
					!strings.Contains(reportedErr.Error(), "observation 2") {
					t.Fatalf(
						"OnError() = %v, want canceled observation 2 drop",
						reportedErr,
					)
				}
			} else {
				select {
				case err := <-reported:
					t.Fatalf("OnError() without pending heartbeat = %v", err)
				default:
				}
			}
		})
	}
}

func TestHeartbeatRuntimeStandaloneDrainsInstallWriteDeadline(t *testing.T) {
	t.Parallel()

	t.Run("release", func(t *testing.T) {
		t.Parallel()
		clock := newHeartbeatRuntimeClock(
			time.Date(2040, 1, 1, 11, 30, 0, 0, time.UTC),
		)
		writer := newHeartbeatRuntimeWriter()
		runtime := newHeartbeatRuntimeForTest(
			t,
			writer,
			heartbeatRuntimeConfig(clock),
		)
		lease := heartbeatRuntimeLease(
			"tenant-a",
			"collector-release-deadline",
			1,
		)
		if err := runtime.Activate(lease); err != nil {
			t.Fatal(err)
		}
		if accepted, err := runtime.Offer(
			context.Background(),
			lease,
			heartbeatRuntimeHeartbeat(1),
		); err != nil || !accepted {
			t.Fatalf("Offer() = %t, %v", accepted, err)
		}
		if err := runtime.Release(context.Background(), lease); err != nil {
			t.Fatalf("Release(): %v", err)
		}
		calls := writer.Calls()
		if len(calls) != 1 || !calls[0].contextHasDeadline {
			t.Fatalf("Release writer context = %#v, want deadline", calls)
		}
	})

	t.Run("close", func(t *testing.T) {
		t.Parallel()
		clock := newHeartbeatRuntimeClock(
			time.Date(2040, 1, 1, 11, 45, 0, 0, time.UTC),
		)
		writer := newHeartbeatRuntimeWriter()
		runtime, err := NewHeartbeatRuntime(
			writer,
			heartbeatRuntimeConfig(clock),
		)
		if err != nil {
			t.Fatal(err)
		}
		lease := heartbeatRuntimeLease(
			"tenant-a",
			"collector-close-deadline",
			1,
		)
		if err := runtime.Activate(lease); err != nil {
			t.Fatal(err)
		}
		if accepted, err := runtime.Offer(
			context.Background(),
			lease,
			heartbeatRuntimeHeartbeat(1),
		); err != nil || !accepted {
			t.Fatalf("Offer() = %t, %v", accepted, err)
		}
		if err := runtime.Close(context.Background()); err != nil {
			t.Fatalf("Close(): %v", err)
		}
		calls := writer.Calls()
		if len(calls) != 1 || !calls[0].contextHasDeadline {
			t.Fatalf("Close writer context = %#v, want deadline", calls)
		}
	})
}

func TestHeartbeatRuntimeCloseFinalFlushRejectsAndIsIdempotent(t *testing.T) {
	t.Parallel()

	clock := newHeartbeatRuntimeClock(time.Date(2040, 1, 1, 12, 0, 0, 0, time.UTC))
	writer := newHeartbeatRuntimeWriter()
	runtime, err := NewHeartbeatRuntime(writer, heartbeatRuntimeConfig(clock))
	if err != nil {
		t.Fatal(err)
	}
	lease := heartbeatRuntimeLease("tenant-a", "collector-close", 1)
	if err := runtime.Activate(lease); err != nil {
		t.Fatal(err)
	}
	if accepted, err := runtime.Offer(
		context.Background(),
		lease,
		heartbeatRuntimeHeartbeat(1),
	); err != nil || !accepted {
		t.Fatalf("Offer() = %t, %v", accepted, err)
	}

	closeContext, cancelClose := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelClose()
	if err := runtime.Close(closeContext); err != nil {
		t.Fatalf("Close(first): %v", err)
	}
	callsAtClose := writer.Calls()
	if len(callsAtClose) != 1 ||
		callsAtClose[0].lease != lease ||
		callsAtClose[0].heartbeat.ObservationSequence != 1 {
		t.Fatalf("Close final writer attempt = %#v", callsAtClose)
	}
	if err := runtime.Close(context.Background()); err != nil {
		t.Fatalf("Close(second): %v", err)
	}
	if err := runtime.Activate(heartbeatRuntimeLease("tenant-a", "other", 1)); !errors.Is(
		err,
		ErrHeartbeatRuntimeClosed,
	) {
		t.Fatalf("Activate(after Close) error = %v", err)
	}
	if accepted, err := runtime.Offer(
		context.Background(),
		lease,
		heartbeatRuntimeHeartbeat(2),
	); accepted || !errors.Is(err, ErrHeartbeatRuntimeClosed) {
		t.Fatalf("Offer(after Close) = %t, %v", accepted, err)
	}
	if err := runtime.Release(context.Background(), lease); !errors.Is(
		err,
		ErrHeartbeatRuntimeClosed,
	) {
		t.Fatalf("Release(after Close) error = %v", err)
	}
	if err := runtime.Flush(context.Background()); !errors.Is(
		err,
		ErrHeartbeatRuntimeClosed,
	) {
		t.Fatalf("Flush(after Close) error = %v", err)
	}
	if state, ok := runtime.Liveness(lease); ok || state != LivenessStateOffline {
		t.Fatalf("Liveness(after Close) = %q, %t", state, ok)
	}
	if calls := writer.Calls(); len(calls) != len(callsAtClose) {
		t.Fatalf("writer called after Close returned: %#v", calls)
	}
}

func TestHeartbeatRuntimeCancelledCloseStillJoinsInflightFlush(t *testing.T) {
	t.Parallel()

	entered := make(chan struct{})
	releaseWriter := make(chan struct{})
	var unblockWriter sync.Once
	unblock := func() {
		unblockWriter.Do(func() { close(releaseWriter) })
	}
	defer unblock()
	writer := newHeartbeatRuntimeWriter(heartbeatRuntimeWriteResponse{
		applied:       true,
		entered:       entered,
		release:       releaseWriter,
		ignoreContext: true,
	})
	clock := newHeartbeatRuntimeClock(time.Date(2040, 1, 1, 13, 0, 0, 0, time.UTC))
	runtime, err := NewHeartbeatRuntime(writer, heartbeatRuntimeConfig(clock))
	if err != nil {
		t.Fatal(err)
	}
	lease := heartbeatRuntimeLease("tenant-a", "collector-close-join", 1)
	if err := runtime.Activate(lease); err != nil {
		t.Fatal(err)
	}
	if accepted, err := runtime.Offer(
		context.Background(),
		lease,
		heartbeatRuntimeHeartbeat(1),
	); err != nil || !accepted {
		t.Fatalf("Offer() = %t, %v", accepted, err)
	}
	flushDone := make(chan error, 1)
	go func() {
		flushDone <- runtime.Flush(context.Background())
	}()
	heartbeatRuntimeAwaitSignal(t, entered, "in-flight public flush")

	canceledContext, cancel := context.WithCancel(context.Background())
	cancel()
	closeDone := make(chan error, 1)
	go func() {
		closeDone <- runtime.Close(canceledContext)
	}()
	heartbeatRuntimeEventually(t, "Close to reject liveness", func() bool {
		_, ok := runtime.Liveness(lease)
		return !ok
	})
	select {
	case closeErr := <-closeDone:
		t.Fatalf("Close returned before in-flight writer exited: %v", closeErr)
	default:
	}
	if writer.Active() != 1 {
		t.Fatalf("active writers before gate release = %d, want 1", writer.Active())
	}
	if accepted, err := runtime.Offer(
		context.Background(),
		lease,
		heartbeatRuntimeHeartbeat(2),
	); accepted || !errors.Is(err, ErrHeartbeatRuntimeClosed) {
		t.Fatalf("Offer(while Close waits) = %t, %v", accepted, err)
	}

	unblock()
	if err := heartbeatRuntimeAwaitError(t, flushDone, "in-flight public flush"); err != nil {
		t.Fatalf("Flush(): %v", err)
	}
	if err := heartbeatRuntimeAwaitError(t, closeDone, "canceled Close join"); !errors.Is(
		err,
		context.Canceled,
	) {
		t.Fatalf("Close(canceled) error = %v, want context.Canceled", err)
	}
	if writer.Active() != 0 {
		t.Fatalf("active writers after Close = %d, want 0", writer.Active())
	}
	callCount := len(writer.Calls())
	if err := runtime.Close(context.Background()); !errors.Is(err, context.Canceled) {
		t.Fatalf("Close(idempotent after canceled first Close) error = %v", err)
	}
	if calls := writer.Calls(); len(calls) != callCount {
		t.Fatalf("writer called after canceled Close returned: %#v", calls)
	}
}

func TestHeartbeatRuntimeReleaseAndCloseCoordinateOneFinalDrain(t *testing.T) {
	t.Parallel()

	entered := make(chan struct{})
	releaseWriter := make(chan struct{})
	var unblockWriter sync.Once
	unblock := func() {
		unblockWriter.Do(func() { close(releaseWriter) })
	}
	defer unblock()
	writer := newHeartbeatRuntimeWriter(heartbeatRuntimeWriteResponse{
		applied:       true,
		entered:       entered,
		release:       releaseWriter,
		ignoreContext: true,
	})
	clock := newHeartbeatRuntimeClock(time.Date(2040, 1, 1, 14, 0, 0, 0, time.UTC))
	runtime, err := NewHeartbeatRuntime(writer, heartbeatRuntimeConfig(clock))
	if err != nil {
		t.Fatal(err)
	}
	lease := heartbeatRuntimeLease("tenant-a", "collector-release-close", 1)
	if err := runtime.Activate(lease); err != nil {
		t.Fatal(err)
	}
	if accepted, err := runtime.Offer(
		context.Background(),
		lease,
		heartbeatRuntimeHeartbeat(1),
	); err != nil || !accepted {
		t.Fatalf("Offer() = %t, %v", accepted, err)
	}
	releaseDone := make(chan error, 1)
	go func() {
		releaseDone <- runtime.Release(context.Background(), lease)
	}()
	heartbeatRuntimeAwaitSignal(t, entered, "release writer before Close")

	closeDone := make(chan error, 1)
	go func() {
		closeDone <- runtime.Close(context.Background())
	}()
	heartbeatRuntimeEventually(t, "Close to reject work behind Release", func() bool {
		accepted, offerErr := runtime.Offer(
			context.Background(),
			lease,
			heartbeatRuntimeHeartbeat(2),
		)
		return !accepted && errors.Is(offerErr, ErrHeartbeatRuntimeClosed)
	})
	select {
	case closeErr := <-closeDone:
		t.Fatalf("Close returned before release writer exited: %v", closeErr)
	default:
	}
	unblock()
	if err := heartbeatRuntimeAwaitError(t, releaseDone, "Release during Close"); err != nil {
		t.Fatalf("Release(): %v", err)
	}
	if err := heartbeatRuntimeAwaitError(t, closeDone, "Close behind Release"); err != nil {
		t.Fatalf("Close(): %v", err)
	}
	if writer.Active() != 0 {
		t.Fatalf("active writers after Release/Close = %d", writer.Active())
	}
	calls := writer.Calls()
	if len(calls) != 1 ||
		calls[0].lease != lease ||
		calls[0].heartbeat.ObservationSequence != 1 {
		t.Fatalf("Release/Close final writes = %#v", calls)
	}
	if err := runtime.Flush(context.Background()); !errors.Is(
		err,
		ErrHeartbeatRuntimeClosed,
	) {
		t.Fatalf("Flush(after Release/Close) error = %v", err)
	}
	if after := writer.Calls(); len(after) != len(calls) {
		t.Fatalf("writer called after Release/Close returned: %#v", after)
	}
}

func TestHeartbeatRuntimeCancelledReleaseLeavesPendingForCloseFinalDrain(t *testing.T) {
	t.Parallel()

	entered := make(chan struct{})
	releaseWriter := make(chan struct{})
	var unblockWriter sync.Once
	unblock := func() {
		unblockWriter.Do(func() { close(releaseWriter) })
	}
	defer unblock()
	writer := newHeartbeatRuntimeWriter(
		heartbeatRuntimeWriteResponse{
			applied:       true,
			entered:       entered,
			release:       releaseWriter,
			ignoreContext: true,
		},
		heartbeatRuntimeWriteResponse{applied: true},
	)
	clock := newHeartbeatRuntimeClock(time.Date(2040, 1, 1, 15, 0, 0, 0, time.UTC))
	config := heartbeatRuntimeConfig(clock)
	config.MaxCollectors = 2
	runtime, err := NewHeartbeatRuntime(writer, config)
	if err != nil {
		t.Fatal(err)
	}
	blocker := heartbeatRuntimeLease("tenant-a", "collector-release-blocker", 1)
	releasing := heartbeatRuntimeLease("tenant-a", "collector-canceled-release", 1)
	if err := runtime.Activate(blocker); err != nil {
		t.Fatal(err)
	}
	if err := runtime.Activate(releasing); err != nil {
		t.Fatal(err)
	}
	if accepted, err := runtime.Offer(
		context.Background(),
		blocker,
		heartbeatRuntimeHeartbeat(1),
	); err != nil || !accepted {
		t.Fatalf("Offer(blocker) = %t, %v", accepted, err)
	}
	flushDone := make(chan error, 1)
	go func() {
		flushDone <- runtime.Flush(context.Background())
	}()
	heartbeatRuntimeAwaitSignal(t, entered, "unrelated gated flush")

	// This offer happens after Flush took its write set, so it remains pending
	// on the entry while Release waits behind the unrelated writer.
	if accepted, err := runtime.Offer(
		context.Background(),
		releasing,
		heartbeatRuntimeHeartbeat(2),
	); err != nil || !accepted {
		t.Fatalf("Offer(releasing) = %t, %v", accepted, err)
	}
	releaseContext, cancelRelease := context.WithCancel(context.Background())
	releaseDone := make(chan error, 1)
	go func() {
		releaseDone <- runtime.Release(releaseContext, releasing)
	}()
	heartbeatRuntimeEventually(t, "Release to mark exact lease inactive", func() bool {
		state, ok := runtime.Liveness(releasing)
		return !ok && state == LivenessStateOffline
	})
	cancelRelease()

	closeDone := make(chan error, 1)
	go func() {
		closeDone <- runtime.Close(context.Background())
	}()
	heartbeatRuntimeEventually(t, "Close to reject work behind canceled Release", func() bool {
		accepted, offerErr := runtime.Offer(
			context.Background(),
			releasing,
			heartbeatRuntimeHeartbeat(3),
		)
		return !accepted && errors.Is(offerErr, ErrHeartbeatRuntimeClosed)
	})
	unblock()
	if err := heartbeatRuntimeAwaitError(t, flushDone, "unrelated flush completion"); err != nil {
		t.Fatalf("Flush(blocker): %v", err)
	}
	if err := heartbeatRuntimeAwaitError(t, releaseDone, "canceled Release"); !errors.Is(
		err,
		context.Canceled,
	) {
		t.Fatalf("Release(canceled) error = %v, want context.Canceled", err)
	}
	if err := heartbeatRuntimeAwaitError(t, closeDone, "Close final drain"); err != nil {
		t.Fatalf("Close(): %v", err)
	}
	calls := writer.Calls()
	if len(calls) != 2 ||
		calls[0].lease != blocker ||
		calls[0].heartbeat.ObservationSequence != 1 ||
		calls[1].lease != releasing ||
		calls[1].heartbeat.ObservationSequence != 2 {
		t.Fatalf("canceled Release lost Close final pending drain: %#v", calls)
	}
	if writer.Active() != 0 {
		t.Fatalf("active writers after canceled Release/Close = %d", writer.Active())
	}
	if err := runtime.Flush(context.Background()); !errors.Is(
		err,
		ErrHeartbeatRuntimeClosed,
	) {
		t.Fatalf("Flush(after Close) error = %v", err)
	}
	if after := writer.Calls(); len(after) != len(calls) {
		t.Fatalf("writer called after canceled Release/Close returned: %#v", after)
	}
}

type heartbeatRuntimeWriteResponse struct {
	applied       bool
	err           error
	entered       chan struct{}
	release       <-chan struct{}
	ignoreContext bool
}

type heartbeatRuntimeWriteCall struct {
	lease              Lease
	heartbeat          Heartbeat
	contextAtEntry     error
	contextHasDeadline bool
}

type heartbeatRuntimeWriter struct {
	mu        sync.Mutex
	responses []heartbeatRuntimeWriteResponse
	calls     []heartbeatRuntimeWriteCall
	active    int
	called    chan int
}

func newHeartbeatRuntimeWriter(
	responses ...heartbeatRuntimeWriteResponse,
) *heartbeatRuntimeWriter {
	return &heartbeatRuntimeWriter{
		responses: responses,
		called:    make(chan int, 4_096),
	}
}

func (writer *heartbeatRuntimeWriter) RecordHeartbeat(
	ctx context.Context,
	lease Lease,
	heartbeat Heartbeat,
) (bool, error) {
	snapshot, err := normalizeHeartbeat(heartbeat)
	if err != nil {
		return false, fmt.Errorf("fake writer received invalid heartbeat: %w", err)
	}
	_, contextHasDeadline := ctx.Deadline()
	writer.mu.Lock()
	callIndex := len(writer.calls)
	writer.calls = append(writer.calls, heartbeatRuntimeWriteCall{
		lease:              lease,
		heartbeat:          snapshot,
		contextAtEntry:     ctx.Err(),
		contextHasDeadline: contextHasDeadline,
	})
	response := heartbeatRuntimeWriteResponse{applied: true}
	if callIndex < len(writer.responses) {
		response = writer.responses[callIndex]
	}
	writer.active++
	writer.mu.Unlock()
	writer.called <- callIndex
	if response.entered != nil {
		close(response.entered)
	}
	defer func() {
		writer.mu.Lock()
		writer.active--
		writer.mu.Unlock()
	}()
	if response.release != nil {
		if response.ignoreContext {
			<-response.release
		} else {
			select {
			case <-response.release:
			case <-ctx.Done():
				return false, ctx.Err()
			}
		}
	}
	return response.applied, response.err
}

func (writer *heartbeatRuntimeWriter) Calls() []heartbeatRuntimeWriteCall {
	writer.mu.Lock()
	defer writer.mu.Unlock()
	result := make([]heartbeatRuntimeWriteCall, len(writer.calls))
	copy(result, writer.calls)
	return result
}

func (writer *heartbeatRuntimeWriter) Active() int {
	writer.mu.Lock()
	defer writer.mu.Unlock()
	return writer.active
}

type heartbeatRuntimeClock struct {
	mu  sync.Mutex
	now time.Time
}

func newHeartbeatRuntimeClock(now time.Time) *heartbeatRuntimeClock {
	return &heartbeatRuntimeClock{now: now}
}

func (clock *heartbeatRuntimeClock) Now() time.Time {
	clock.mu.Lock()
	defer clock.mu.Unlock()
	return clock.now
}

func (clock *heartbeatRuntimeClock) Advance(elapsed time.Duration) {
	clock.mu.Lock()
	clock.now = clock.now.Add(elapsed)
	clock.mu.Unlock()
}

func heartbeatRuntimeConfig(clock *heartbeatRuntimeClock) HeartbeatRuntimeConfig {
	return HeartbeatRuntimeConfig{
		MaxCollectors:     4,
		HeartbeatInterval: 10 * time.Second,
		StaleGrace:        5 * time.Second,
		FlushInterval:     time.Hour,
		WriteTimeout:      5 * time.Second,
		MonotonicNow:      clock.Now,
	}
}

func heartbeatRuntimeLease(tenantID, collectorID string, generation uint64) Lease {
	return Lease{
		Scope:       Scope{TenantID: tenantID},
		CollectorID: collectorID,
		BootEpoch:   "server-boot",
		StreamID:    fmt.Sprintf("stream-%d", generation),
		Generation:  generation,
	}
}

func heartbeatRuntimeHeartbeat(sequence uint64) Heartbeat {
	receivedAt := time.Date(2026, 7, 29, 8, 0, 0, 0, time.UTC).
		Add(time.Duration(sequence) * time.Second)
	return testHeartbeat(receivedAt, sequence)
}

func newHeartbeatRuntimeForTest(
	t *testing.T,
	writer HeartbeatWriter,
	config HeartbeatRuntimeConfig,
) *HeartbeatRuntime {
	t.Helper()
	runtime, err := NewHeartbeatRuntime(writer, config)
	if err != nil {
		t.Fatalf("NewHeartbeatRuntime(): %v", err)
	}
	t.Cleanup(func() {
		heartbeatRuntimeClose(t, runtime)
	})
	return runtime
}

func heartbeatRuntimeClose(t *testing.T, runtime *HeartbeatRuntime) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := runtime.Close(ctx); err != nil {
		t.Errorf("HeartbeatRuntime.Close(): %v", err)
	}
}

func heartbeatRuntimeAwaitSignal(t *testing.T, signal <-chan struct{}, operation string) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for %s", operation)
	}
}

func heartbeatRuntimeAwaitError(
	t *testing.T,
	result <-chan error,
	operation string,
) error {
	t.Helper()
	select {
	case err := <-result:
		return err
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for %s", operation)
		return nil
	}
}

func heartbeatRuntimeEventually(
	t *testing.T,
	operation string,
	condition func() bool,
) {
	t.Helper()
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for {
		if condition() {
			return
		}
		select {
		case <-deadline.C:
			t.Fatalf("timed out waiting for %s", operation)
		case <-ticker.C:
		}
	}
}
