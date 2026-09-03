package clickhouse

import (
	"context"
	"crypto/sha256"
	"errors"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/ingest"
	"github.com/Suhaibinator/open-splunk/internal/visibility"
)

func TestProductionStoreCoalescesAndWakesIndependentNativeResults(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	connection := &fakeStoreConnection{batch: &fakeWriteBatch{}}
	store, sequencer := newProductionCoalescerTestStore(t, ctx, connection)
	store.writeGroupLimits.TargetRows = 2
	store.writeGroupLimits.TargetDecodedBytes = store.writeGroupLimits.MaxDecodedBytes
	store.startReconciler()

	firstBatch := coalescerTestBatch("first", 1)
	secondBatch := coalescerTestBatch("second", 2)
	type outcome struct {
		result ingest.StoreResult
		err    error
	}
	firstDone := make(chan outcome, 1)
	go func() {
		result, err := store.Store(ctx, firstBatch)
		firstDone <- outcome{result: result, err: err}
	}()
	waitForPendingReservations(t, ctx, sequencer)

	retryDone := make(chan outcome, 1)
	go func() {
		result, err := store.Store(ctx, firstBatch)
		retryDone <- outcome{result: result, err: err}
	}()
	secondDone := make(chan outcome, 1)
	go func() {
		result, err := store.Store(ctx, secondBatch)
		secondDone <- outcome{result: result, err: err}
	}()

	first := <-firstDone
	retry := <-retryDone
	second := <-secondDone
	for name, completed := range map[string]outcome{
		"first": first, "exact retry": retry, "second": second,
	} {
		if completed.err != nil {
			t.Fatalf("%s Store: %v", name, completed.err)
		}
	}
	if first.result.Accepted != 1 || first.result.Duplicate != 0 ||
		retry.result.Accepted != 0 || retry.result.Duplicate != 1 ||
		second.result.Accepted != 1 || second.result.Duplicate != 0 {
		t.Fatalf("native outcomes = first %+v retry %+v second %+v", first.result, retry.result, second.result)
	}
	if connection.prepareCalls != 1 || connection.batch.sendCalls != 1 ||
		len(connection.batch.rows) != 2 {
		t.Fatalf(
			"physical insert = prepares %d sends %d rows %d, want 1/1/2",
			connection.prepareCalls,
			connection.batch.sendCalls,
			len(connection.batch.rows),
		)
	}
	if connection.batch.rows[0][0] != "event-first" ||
		connection.batch.rows[1][0] != "event-second" {
		t.Fatalf("physical row order = %#v", connection.batch.rows)
	}
	metrics := store.coalescingMetrics.Snapshot()
	if metrics.NativeWaiters != 0 || metrics.PeakNativeWaiters < 2 ||
		metrics.NativeWaiterWakeups < 2 || metrics.NativeTerminalLookups < 4 {
		t.Fatalf("native waiter metrics = %#v", metrics)
	}
}

func TestProductionStoreCancellationLeavesDurableGroupAndNoWaiter(t *testing.T) {
	ctx := context.Background()
	connection := &gatedStoreConnection{
		entered: make(chan struct{}),
		resume:  make(chan struct{}),
		err:     errors.New("prepare unavailable"),
	}
	store, sequencer := newProductionCoalescerTestStore(t, ctx, connection)
	store.writeGroupLimits.MaxLinger = time.Millisecond
	store.retryAfter = time.Hour
	store.startReconciler()

	requestContext, cancel := context.WithCancel(ctx)
	defer cancel()
	resultDone := make(chan error, 1)
	go func() {
		_, err := store.Store(requestContext, coalescerTestBatch("cancel", 1))
		resultDone <- err
	}()
	select {
	case <-connection.entered:
	case <-time.After(20 * time.Second):
		t.Fatal("coalescer did not reach ClickHouse prepare")
	}
	cancel()
	storeErr := <-resultDone
	if _, ok := errors.AsType[*ingest.TransientStoreError](storeErr); !ok {
		t.Fatalf("canceled Store error = %v, want transient durable-pending result", storeErr)
	}
	if store.commitWaiters.size() != 0 || store.coalescingMetrics.Snapshot().NativeWaiters != 0 {
		t.Fatal("canceled native waiter leaked")
	}
	usage, err := sequencer.PendingUsage(ctx)
	if err != nil || usage.Reservations != 1 || usage.ReadyGroups != 1 {
		t.Fatalf("durable usage after cancellation = %+v, err=%v", usage, err)
	}
	telemetry, err := store.InsertCoalescingTelemetry(ctx)
	if err != nil || telemetry.Queue.PendingReservations != 1 ||
		telemetry.Queue.ReadyGroups != 1 || telemetry.Queue.PendingOutboxBytes == 0 ||
		telemetry.Queue.OldestPendingAge <= 0 {
		t.Fatalf("durable coalescer telemetry = %+v, err=%v", telemetry.Queue, err)
	}
	close(connection.resume)
	deadline := time.Now().Add(2 * time.Second)
	for store.HECReconciliationAvailable() && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if store.HECReconciliationAvailable() {
		t.Fatal("failed prepare did not make reconciliation unavailable")
	}

	recovery := &fakeStoreConnection{batch: &fakeWriteBatch{}}
	store.connection = recovery
	if err := store.ReconcilePending(ctx); err != nil {
		t.Fatalf("recover durable group: %v", err)
	}
	state, result, err := store.LookupBatch(ctx, storeBatchIdentity(coalescerTestBatch("cancel", 1)))
	if err != nil || state != ingest.StoredBatchCommitted || result.Duplicate != 1 {
		t.Fatalf("recovered lookup = state %v result %+v err %v", state, result, err)
	}
}

func TestClickHouseConfigValidatesInsertCoalescingEnvelope(t *testing.T) {
	defaults := DefaultConfig()
	_, normalized, err := (Config{}).clickHouseOptions()
	if err != nil {
		t.Fatalf("normalize default config: %v", err)
	}
	if normalized.InsertTargetRows != 10_000 || normalized.InsertMaxRows != 50_000 ||
		normalized.InsertTargetDecodedBytes != 16<<20 ||
		normalized.InsertMaxDecodedBytes != 64<<20 ||
		normalized.InsertMaxMembers != 10_000 ||
		normalized.InsertMaxLinger != time.Second {
		t.Fatalf("normalized coalescer defaults = %+v", normalized)
	}
	tests := []struct {
		name   string
		mutate func(*Config)
	}{
		{
			name: "row target above max",
			mutate: func(config *Config) {
				config.InsertTargetRows = config.InsertMaxRows + 1
			},
		},
		{
			name: "row hard ceiling",
			mutate: func(config *Config) {
				config.InsertMaxRows = visibility.MaxWriteGroupRows + 1
			},
		},
		{
			name: "byte target above max",
			mutate: func(config *Config) {
				config.InsertTargetDecodedBytes = config.InsertMaxDecodedBytes + 1
			},
		},
		{
			name: "byte hard ceiling",
			mutate: func(config *Config) {
				config.InsertMaxDecodedBytes = visibility.MaxWriteGroupDecodedBytes + 1
			},
		},
		{
			name: "member hard ceiling",
			mutate: func(config *Config) {
				config.InsertMaxMembers = visibility.MaxWriteGroupMembers + 1
			},
		},
		{
			name: "linger hard ceiling",
			mutate: func(config *Config) {
				config.InsertMaxLinger = time.Second + time.Millisecond
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			config := defaults
			test.mutate(&config)
			if _, _, err := config.clickHouseOptions(); err == nil {
				t.Fatal("invalid coalescer config error = nil")
			}
		})
	}
}

func TestPublicStoreConstructionRequiresDurableWriteGroupSequencer(t *testing.T) {
	legacy := &fakeVisibilitySequencer{}
	if _, err := Open(Config{}, fixedRetention(time.Hour), legacy); err == nil ||
		!strings.Contains(err.Error(), "durable write groups") {
		t.Fatalf("Open with legacy sequencer error = %v", err)
	}
	if err := requireWriteGroupSequencer(newFakeWriteGroupSequencer()); err != nil {
		t.Fatalf("write-group sequencer rejected: %v", err)
	}
}

func TestInsertCoalescingTelemetryClampsBackwardClockOldestAge(t *testing.T) {
	now := time.Date(2026, 9, 3, 1, 2, 3, 0, time.UTC)
	sequencer := &fakeVisibilitySequencer{pendingUsage: &visibility.PendingUsage{
		Reservations:    1,
		OutboxBytes:     128,
		OldestPendingAt: now.Add(time.Minute),
	}}
	store := mustTestStoreWithVisibility(
		t,
		&fakeStoreConnection{},
		fixedRetention(time.Hour),
		sequencer,
	)
	store.clock = func() time.Time { return now }
	snapshot, err := store.InsertCoalescingTelemetry(context.Background())
	if err != nil {
		t.Fatalf("InsertCoalescingTelemetry: %v", err)
	}
	if snapshot.Queue.OldestPendingAge != 0 {
		t.Fatalf("backward-clock oldest age = %v, want zero", snapshot.Queue.OldestPendingAge)
	}
}

func TestWaitForReservationClosesCommitBeforeRegisterRace(t *testing.T) {
	connection := &fakeStoreConnection{batch: &fakeWriteBatch{}}
	sequencer := newFakeWriteGroupSequencer()
	store := mustTestStoreWithVisibility(t, connection, fixedRetention(time.Hour), sequencer)
	group := testWriteGroup(t, store, 1)
	committed := group.Members[0]
	committed.AlreadyCommitted = true
	committed.CommittedAt = time.Now().UTC()
	committed.Outbox = nil
	sequencer.reservation = committed
	sequencer.hasReservation = true

	result, err := store.waitForReservation(context.Background(), group.Members[0], false)
	if err != nil || result.Accepted != 1 || result.Duplicate != 0 {
		t.Fatalf("waitForReservation = %+v, %v", result, err)
	}
	if store.commitWaiters.size() != 0 || store.coalescingMetrics.Snapshot().NativeWaiters != 0 {
		t.Fatal("commit-before-register check leaked a waiter")
	}
}

func TestWaitForReservationReregistersAfterSpuriousNonterminalGroupHint(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	connection := &fakeStoreConnection{batch: &fakeWriteBatch{}}
	store, _ := newProductionCoalescerTestStore(t, ctx, connection)
	reservation, duplicate, err := store.reserveStaged(
		ctx,
		coalescerTestBatch("spurious-hint", 1),
		false,
	)
	if err != nil || duplicate {
		t.Fatalf("reserve staged batch = duplicate %v, err %v", duplicate, err)
	}

	type outcome struct {
		result ingest.StoreResult
		err    error
	}
	done := make(chan outcome, 1)
	go func() {
		result, waitErr := store.waitForReservation(ctx, reservation, false)
		done <- outcome{result: result, err: waitErr}
	}()
	waitForNativeWaiterCount(t, ctx, store, 1)
	store.notifyWriteGroupHint(visibility.WriteGroup{
		Members: []visibility.Reservation{{Sequence: reservation.Sequence}},
	})
	for store.coalescingMetrics.Snapshot().NativeTerminalLookups < 2 {
		select {
		case <-ctx.Done():
			t.Fatalf("waiter did not re-read after spurious hint: %v", ctx.Err())
		case <-time.After(time.Millisecond):
		}
	}
	waitForNativeWaiterCount(t, ctx, store, 1)
	select {
	case completed := <-done:
		t.Fatalf("spurious hint completed pending waiter: %+v", completed)
	default:
	}

	if err := store.ReconcilePending(ctx); err != nil {
		t.Fatalf("commit after spurious hint: %v", err)
	}
	completed := <-done
	if completed.err != nil || completed.result.Accepted != 1 ||
		completed.result.Duplicate != 0 {
		t.Fatalf("wait after authoritative commit = %+v, %v", completed.result, completed.err)
	}
	waitForNativeWaiterCount(t, ctx, store, 0)
}

func TestAmbiguousCommitHintNotifiesMaximumGroupWithoutDurableLookupFanout(t *testing.T) {
	sequencer := newFakeWriteGroupSequencer()
	store := mustTestStoreWithVisibility(
		t,
		&fakeStoreConnection{},
		fixedRetention(time.Hour),
		sequencer,
	)
	group := visibility.WriteGroup{
		Members:     make([]visibility.Reservation, visibility.MaxWriteGroupMembers),
		MemberCount: visibility.MaxWriteGroupMembers,
	}
	waiters := make([]*commitWaiter, len(group.Members))
	for index := range group.Members {
		sequence := uint64(index + 1)
		group.Members[index].Sequence = sequence
		waiter, err := store.commitWaiters.register(sequence)
		if err != nil {
			t.Fatalf("register maximum-member waiter %d: %v", index, err)
		}
		store.coalescingMetrics.AddNativeWaiter()
		waiters[index] = waiter
	}
	store.notifyWriteGroupHint(group)
	for index, waiter := range waiters {
		select {
		case <-waiter.done:
		default:
			t.Fatalf("maximum-member waiter %d was not notified", index)
		}
	}
	if sequencer.lookupCalls != 0 || store.commitWaiters.size() != 0 {
		t.Fatalf(
			"group hint durable lookups = %d, remaining waiters = %d",
			sequencer.lookupCalls,
			store.commitWaiters.size(),
		)
	}
	snapshot := store.coalescingMetrics.Snapshot()
	if snapshot.NativeWaiters != 0 ||
		snapshot.NativeWaiterWakeups != uint64(visibility.MaxWriteGroupMembers) {
		t.Fatalf("maximum-member hint metrics = %#v", snapshot)
	}
}

func TestStoreShutdownForceSealsSparseGroupAndUnblocksNativeWaiter(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	connection := &fakeStoreConnection{batch: &fakeWriteBatch{}}
	store, sequencer := newProductionCoalescerTestStore(t, ctx, connection)
	store.writeGroupLimits.MaxLinger = 200 * time.Millisecond

	type outcome struct {
		result ingest.StoreResult
		err    error
	}
	done := make(chan outcome, 1)
	go func() {
		result, err := store.Store(ctx, coalescerTestBatch("shutdown-sparse", 1))
		done <- outcome{result: result, err: err}
	}()
	waitForPendingReservations(t, ctx, sequencer)
	if err := store.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown sparse group: %v", err)
	}
	completed := <-done
	if completed.err != nil || completed.result.Accepted != 1 ||
		completed.result.Duplicate != 0 {
		t.Fatalf("native result during shutdown = %+v, %v", completed.result, completed.err)
	}
	usage, err := sequencer.PendingUsage(ctx)
	if err != nil || usage.Reservations != 0 || usage.OutboxBytes != 0 {
		t.Fatalf("pending usage after shutdown drain = %+v, %v", usage, err)
	}
	if connection.prepareCalls != 1 || connection.batch.sendCalls != 1 ||
		len(connection.batch.rows) != 1 {
		t.Fatalf(
			"sparse shutdown insert = prepare %d send %d rows %d",
			connection.prepareCalls,
			connection.batch.sendCalls,
			len(connection.batch.rows),
		)
	}
	if _, err := store.Store(ctx, coalescerTestBatch("after-shutdown", 2)); !errors.Is(err, ErrStoreClosed) {
		t.Fatalf("Store after Shutdown error = %v, want ErrStoreClosed", err)
	}
}

func TestStoreCloseAloneForceSealsSparseGroupAndUnblocksNativeWaiter(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	connection := &fakeStoreConnection{batch: &fakeWriteBatch{}}
	store, sequencer := newProductionCoalescerTestStore(t, ctx, connection)
	store.writeGroupLimits.MaxLinger = 200 * time.Millisecond

	type outcome struct {
		result ingest.StoreResult
		err    error
	}
	done := make(chan outcome, 1)
	go func() {
		result, err := store.Store(ctx, coalescerTestBatch("close-sparse", 1))
		done <- outcome{result: result, err: err}
	}()
	waitForPendingReservations(t, ctx, sequencer)
	if err := store.Close(); err != nil {
		t.Fatalf("Close sparse group: %v", err)
	}
	completed := <-done
	if completed.err != nil || completed.result.Accepted != 1 ||
		completed.result.Duplicate != 0 {
		t.Fatalf("native result during Close = %+v, %v", completed.result, completed.err)
	}
	usage, err := sequencer.PendingUsage(ctx)
	if err != nil || usage.Reservations != 0 || usage.OutboxBytes != 0 {
		t.Fatalf("pending usage after Close drain = %+v, %v", usage, err)
	}
	if connection.prepareCalls != 1 || connection.batch.sendCalls != 1 ||
		len(connection.batch.rows) != 1 || connection.closeCalls != 1 {
		t.Fatalf(
			"sparse Close insert = prepare %d send %d rows %d closes %d",
			connection.prepareCalls,
			connection.batch.sendCalls,
			len(connection.batch.rows),
			connection.closeCalls,
		)
	}
	if _, err := store.Store(ctx, coalescerTestBatch("after-close", 2)); !errors.Is(err, ErrStoreClosed) {
		t.Fatalf("Store after Close error = %v, want ErrStoreClosed", err)
	}
}

func TestConcurrentCloseDoesNotCancelCloseOwnedGracefulDrain(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	physicalBatch := &fakeWriteBatch{}
	connection := &gatedStoreConnection{
		entered:          make(chan struct{}),
		resume:           make(chan struct{}),
		exited:           make(chan struct{}),
		closed:           make(chan struct{}),
		closedBeforeExit: make(chan struct{}),
		batch:            physicalBatch,
	}
	store, sequencer := newProductionCoalescerTestStore(t, ctx, connection)
	store.writeGroupLimits.MaxLinger = 200 * time.Millisecond

	storeDone := make(chan error, 1)
	go func() {
		_, err := store.Store(ctx, coalescerTestBatch("concurrent-close", 1))
		storeDone <- err
	}()
	waitForPendingReservations(t, ctx, sequencer)
	firstCloseDone := make(chan error, 1)
	go func() { firstCloseDone <- store.Close() }()
	select {
	case <-connection.entered:
	case <-ctx.Done():
		t.Fatalf("Close did not begin graceful sparse drain: %v", ctx.Err())
	}
	secondCloseStarted := make(chan struct{})
	secondCloseDone := make(chan error, 1)
	go func() {
		close(secondCloseStarted)
		secondCloseDone <- store.Close()
	}()
	<-secondCloseStarted
	select {
	case <-connection.exited:
		t.Fatal("concurrent Close canceled a Close-owned graceful drain")
	case err := <-firstCloseDone:
		t.Fatalf("first Close returned before its graceful drain resumed: %v", err)
	case err := <-secondCloseDone:
		t.Fatalf("second Close returned before the graceful drain resumed: %v", err)
	case <-time.After(20 * time.Millisecond):
	}

	close(connection.resume)
	for index, done := range []<-chan error{firstCloseDone, secondCloseDone} {
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("Close %d: %v", index+1, err)
			}
		case <-ctx.Done():
			t.Fatalf("Close %d did not finish: %v", index+1, ctx.Err())
		}
	}
	if err := <-storeDone; err != nil {
		t.Fatalf("native waiter during concurrent Close: %v", err)
	}
	if physicalBatch.sendCalls != 1 || len(physicalBatch.rows) != 1 {
		t.Fatalf("concurrent Close physical insert sends=%d rows=%d", physicalBatch.sendCalls, len(physicalBatch.rows))
	}
	select {
	case <-connection.closedBeforeExit:
		t.Fatal("connection closed before the graceful drain exited")
	default:
	}
}

func TestStoreShutdownTimeoutPreservesSparseDurableGroupAndUnblocksWaiter(t *testing.T) {
	ctx := context.Background()
	connection := &gatedStoreConnection{
		entered:            make(chan struct{}),
		resume:             make(chan struct{}),
		ignoreCancellation: true,
		err:                errors.New("prepare ignored shutdown cancellation"),
	}
	store, sequencer := newProductionCoalescerTestStore(t, ctx, connection)
	store.writeGroupLimits.MaxLinger = 200 * time.Millisecond

	storeDone := make(chan error, 1)
	go func() {
		_, err := store.Store(ctx, coalescerTestBatch("shutdown-timeout", 1))
		storeDone <- err
	}()
	waitForPendingReservations(t, ctx, sequencer)
	shutdownContext, cancel := context.WithTimeout(ctx, 20*time.Millisecond)
	defer cancel()
	shutdownErr := store.Shutdown(shutdownContext)
	if !errors.Is(shutdownErr, context.DeadlineExceeded) {
		t.Fatalf("Shutdown timeout error = %v, want deadline exceeded", shutdownErr)
	}
	select {
	case <-connection.entered:
	case <-time.After(time.Second):
		t.Fatal("shutdown did not attempt to force-seal the sparse group")
	}
	storeErr := <-storeDone
	if _, ok := errors.AsType[*ingest.TransientStoreError](storeErr); !ok {
		t.Fatalf("native waiter after shutdown timeout = %v, want transient", storeErr)
	}
	if errors.Is(storeErr, ErrStoreClosed) {
		t.Fatalf("native waiter after shutdown timeout = %v, want durable-pending transient", storeErr)
	}
	usage, err := sequencer.PendingUsage(ctx)
	if err != nil || usage.Reservations != 1 || usage.LeasedGroups != 1 ||
		usage.OutboxBytes == 0 {
		t.Fatalf("durable usage after shutdown timeout = %+v, %v", usage, err)
	}
	close(connection.resume)
	if err := store.Shutdown(context.Background()); !errors.Is(err, connection.err) {
		t.Fatalf("completed Shutdown error = %v, want driver failure", err)
	}
	usage, err = sequencer.PendingUsage(ctx)
	if err != nil || usage.Reservations != 1 || usage.ReadyGroups != 1 ||
		usage.OutboxBytes == 0 {
		t.Fatalf("durable usage after shutdown join = %+v, %v", usage, err)
	}
}

func TestStoreCloseRacingShutdownDoesNotCloseConnectionBeforeDrainStops(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	connection := &gatedStoreConnection{
		entered:            make(chan struct{}),
		resume:             make(chan struct{}),
		exited:             make(chan struct{}),
		closed:             make(chan struct{}),
		closedBeforeExit:   make(chan struct{}),
		ignoreCancellation: true,
		err:                errors.New("injected shutdown drain failure"),
	}
	store, sequencer := newProductionCoalescerTestStore(t, ctx, connection)
	store.writeGroupLimits.MaxLinger = 200 * time.Millisecond

	storeDone := make(chan error, 1)
	go func() {
		_, err := store.Store(ctx, coalescerTestBatch("close-race", 1))
		storeDone <- err
	}()
	waitForPendingReservations(t, ctx, sequencer)
	shutdownDone := make(chan error, 1)
	go func() { shutdownDone <- store.Shutdown(ctx) }()
	select {
	case <-connection.entered:
	case <-ctx.Done():
		t.Fatalf("shutdown drain did not enter ClickHouse: %v", ctx.Err())
	}
	closeDone := make(chan error, 1)
	go func() { closeDone <- store.Close() }()
	select {
	case <-connection.closedBeforeExit:
		t.Fatal("Close released the connection before the shutdown drain stopped")
	case err := <-closeDone:
		t.Fatalf("Close returned before the shutdown drain stopped: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	close(connection.resume)
	if err := <-shutdownDone; !errors.Is(err, connection.err) {
		t.Fatalf("Shutdown raced by Close error = %v, want drain failure", err)
	}
	if err := <-closeDone; err != nil {
		t.Fatalf("Close after shutdown drain stopped: %v", err)
	}
	select {
	case <-connection.exited:
	default:
		t.Fatal("ClickHouse drain had not exited before connection close")
	}
	select {
	case <-connection.closedBeforeExit:
		t.Fatal("connection reported close before drain exit")
	default:
	}
	storeErr := <-storeDone
	if _, ok := errors.AsType[*ingest.TransientStoreError](storeErr); !ok {
		t.Fatalf("native waiter after Close race = %v, want transient", storeErr)
	}
	if errors.Is(storeErr, ErrStoreClosed) {
		t.Fatalf("native waiter after Close race = %v, want durable-pending transient", storeErr)
	}
}

func TestRealSQLiteReopenReplaysAmbiguousGroupWithIdenticalInsert(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	controlPath := filepath.Join(t.TempDir(), "control.sqlite")

	firstDB, err := control.Open(ctx, controlPath)
	if err != nil {
		t.Fatalf("open first control database: %v", err)
	}
	firstSequencer, err := visibility.NewSQLite(ctx, firstDB)
	if err != nil {
		t.Fatalf("open first sequencer: %v", err)
	}
	firstConnection := &fakeStoreConnection{batch: &fakeWriteBatch{
		sendErr: errors.New("outcome-ambiguous send failure"),
	}}
	firstStore, err := newStore(
		firstConnection,
		"open_splunk",
		"events",
		fixedRetention(time.Hour),
		firstSequencer,
		time.Now,
		10*time.Millisecond,
	)
	if err != nil {
		t.Fatalf("new first Store: %v", err)
	}
	firstStore.groupedProduction = true
	batch := coalescerTestBatch("reopen-ambiguous", 1)
	if _, err := firstStore.Stage(ctx, batch); err != nil {
		t.Fatalf("stage first durable batch: %v", err)
	}
	if err := firstStore.ReconcilePending(ctx); err == nil {
		t.Fatal("ambiguous first send unexpectedly succeeded")
	}
	firstToken := firstConnection.settings["insert_deduplication_token"]
	firstRows := cloneRows(firstConnection.batch.rows)
	if firstToken == nil || len(firstRows) != 1 {
		t.Fatalf("first ambiguous insert = token %#v rows %#v", firstToken, firstRows)
	}
	if err := firstStore.Close(); err != nil {
		t.Fatalf("close first Store: %v", err)
	}
	if err := firstSequencer.Close(); err != nil {
		t.Fatalf("close first sequencer: %v", err)
	}
	if err := firstDB.Close(); err != nil {
		t.Fatalf("close first control database: %v", err)
	}

	secondDB, err := control.Open(ctx, controlPath)
	if err != nil {
		t.Fatalf("reopen control database: %v", err)
	}
	defer func() { _ = secondDB.Close() }()
	secondSequencer, err := visibility.NewSQLite(ctx, secondDB)
	if err != nil {
		t.Fatalf("open second sequencer: %v", err)
	}
	defer func() { _ = secondSequencer.Close() }()
	secondConnection := &fakeStoreConnection{batch: &fakeWriteBatch{}}
	secondStore, err := newStore(
		secondConnection,
		"open_splunk",
		"events",
		fixedRetention(time.Hour),
		secondSequencer,
		time.Now,
		10*time.Millisecond,
	)
	if err != nil {
		t.Fatalf("new second Store: %v", err)
	}
	secondStore.groupedProduction = true
	defer func() { _ = secondStore.Close() }()
	if err := secondStore.ReconcilePending(ctx); err != nil {
		t.Fatalf("replay ambiguous group after reopen: %v", err)
	}
	if secondConnection.settings["insert_deduplication_token"] != firstToken ||
		!reflect.DeepEqual(secondConnection.batch.rows, firstRows) {
		t.Fatalf(
			"reopened replay changed insert: tokens %#v/%#v rows %#v/%#v",
			firstToken,
			secondConnection.settings["insert_deduplication_token"],
			firstRows,
			secondConnection.batch.rows,
		)
	}
	state, result, err := secondStore.LookupBatch(ctx, storeBatchIdentity(batch))
	if err != nil || state != ingest.StoredBatchCommitted || result.Duplicate != 1 {
		t.Fatalf("reopened replay lookup = state %v result %+v err %v", state, result, err)
	}
}

func TestSendWriteGroupCoalescesOrderedLogicalBatches(t *testing.T) {
	connection := &fakeStoreConnection{batch: &fakeWriteBatch{}}
	sequencer := newFakeWriteGroupSequencer()
	store := mustTestStoreWithVisibility(t, connection, fixedRetention(time.Hour), sequencer)
	group := testWriteGroup(t, store, 2)
	sequencer.committedSequences = writeGroupSequences(group)

	if err := store.sendWriteGroup(
		context.Background(),
		group,
		"attempt",
		visibility.WriteGroupFillRowTarget,
	); err != nil {
		t.Fatalf("sendWriteGroup: %v", err)
	}
	if connection.prepareCalls != 1 || connection.batch.sendCalls != 1 ||
		sequencer.markCalls != 1 || sequencer.commitCalls != 1 || sequencer.releaseCalls != 0 {
		t.Fatalf(
			"physical calls = prepare %d append %d mark %d send %d commit %d release %d",
			connection.prepareCalls,
			len(connection.batch.rows),
			sequencer.markCalls,
			connection.batch.sendCalls,
			sequencer.commitCalls,
			sequencer.releaseCalls,
		)
	}
	if got := connection.settings["insert_deduplication_token"]; got != group.ID {
		t.Fatalf("deduplication token = %#v, want %q", got, group.ID)
	}
	if connection.settings["async_insert"] != uint8(0) ||
		connection.settings["wait_for_async_insert"] != uint8(1) {
		t.Fatalf("insert settings = %#v, want synchronous acknowledged insert", connection.settings)
	}
	if len(connection.batch.rows) != 2 ||
		connection.batch.rows[0][0] != "event-1" ||
		connection.batch.rows[1][0] != "event-2" ||
		connection.batch.rows[0][eventVisibilitySequenceColumn] != uint64(1) ||
		connection.batch.rows[1][eventVisibilitySequenceColumn] != uint64(2) {
		t.Fatalf("coalesced rows = %#v", connection.batch.rows)
	}
	wantSequences := []uint64{1, 2}
	if !slices.Equal(sequencer.committedSequences, wantSequences) {
		t.Fatalf("committed sequences = %v, want %v", sequencer.committedSequences, wantSequences)
	}
	snapshot := store.coalescingMetrics.Snapshot()
	if snapshot.FormedGroups != 1 || snapshot.PhysicalSends != 1 ||
		snapshot.SuccessfulGroups != 1 || snapshot.RowsPerPhysicalInsert.Sum != 2 ||
		snapshot.GroupsByFillReason[CoalescerFillRowTarget] != 1 {
		t.Fatalf("coalescer metrics = %#v", snapshot)
	}
}

func TestSendWriteGroupFailureBoundaries(t *testing.T) {
	sentinel := errors.New("injected failure")
	tests := []struct {
		name        string
		configure   func(*fakeStoreConnection, *fakeWriteGroupSequencer)
		wantMark    int
		wantSend    int
		wantCommit  int
		wantRelease int
		wantAbort   int
	}{
		{
			name: "prepare",
			configure: func(connection *fakeStoreConnection, _ *fakeWriteGroupSequencer) {
				connection.prepareErr = sentinel
			},
			wantRelease: 1,
		},
		{
			name: "append",
			configure: func(connection *fakeStoreConnection, _ *fakeWriteGroupSequencer) {
				connection.batch.appendErr = sentinel
			},
			wantRelease: 1,
			wantAbort:   1,
		},
		{
			name: "mark sending",
			configure: func(_ *fakeStoreConnection, sequencer *fakeWriteGroupSequencer) {
				sequencer.markErr = sentinel
			},
			wantMark:    1,
			wantRelease: 1,
			wantAbort:   1,
		},
		{
			name: "send",
			configure: func(connection *fakeStoreConnection, _ *fakeWriteGroupSequencer) {
				connection.batch.sendErr = sentinel
			},
			wantMark:    1,
			wantSend:    1,
			wantRelease: 1,
			wantAbort:   1,
		},
		{
			name: "commit",
			configure: func(_ *fakeStoreConnection, sequencer *fakeWriteGroupSequencer) {
				sequencer.commitErr = sentinel
			},
			wantMark:    1,
			wantSend:    1,
			wantCommit:  1,
			wantRelease: 1,
		},
		{
			name: "close after commit",
			configure: func(connection *fakeStoreConnection, _ *fakeWriteGroupSequencer) {
				connection.batch.closeErr = sentinel
			},
			wantMark:   1,
			wantSend:   1,
			wantCommit: 1,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			connection := &fakeStoreConnection{batch: &fakeWriteBatch{}}
			sequencer := newFakeWriteGroupSequencer()
			store := mustTestStoreWithVisibility(t, connection, fixedRetention(time.Hour), sequencer)
			group := testWriteGroup(t, store, 2)
			sequencer.committedSequences = writeGroupSequences(group)
			test.configure(connection, sequencer)
			err := store.sendWriteGroup(
				context.Background(),
				group,
				"attempt",
				visibility.WriteGroupFillLinger,
			)
			if !errors.Is(err, sentinel) {
				t.Fatalf("sendWriteGroup error = %v, want injected failure", err)
			}
			if sequencer.markCalls != test.wantMark ||
				connection.batch.sendCalls != test.wantSend ||
				sequencer.commitCalls != test.wantCommit ||
				sequencer.releaseCalls != test.wantRelease ||
				connection.batch.abortCalls != test.wantAbort {
				t.Fatalf(
					"calls = mark %d send %d commit %d release %d abort %d",
					sequencer.markCalls,
					connection.batch.sendCalls,
					sequencer.commitCalls,
					sequencer.releaseCalls,
					connection.batch.abortCalls,
				)
			}
			if test.wantSend == 0 && store.coalescingMetrics.Snapshot().PhysicalSends != 0 {
				t.Fatal("pre-send failure was counted as a physical send")
			}
		})
	}
}

func TestSendWriteGroupFailsClosedOnDurableCorruption(t *testing.T) {
	tests := []struct {
		name    string
		corrupt func(*visibility.WriteGroup)
	}{
		{
			name: "outbox checksum",
			corrupt: func(group *visibility.WriteGroup) {
				group.Members[0].Outbox[0] ^= 0xff
			},
		},
		{
			name: "membership digest",
			corrupt: func(group *visibility.WriteGroup) {
				group.MembershipSHA256[0] ^= 0xff
			},
		},
		{
			name: "decoded identity",
			corrupt: func(group *visibility.WriteGroup) {
				group.Members[0].BatchKey = "different-batch-key"
				group.Members[0].OutboxSHA256 = sha256.Sum256(group.Members[0].Outbox)
				group.MembershipSHA256, _ = visibility.WriteGroupMembershipSHA256(group.Members)
			},
		},
		{
			name: "decoded row total",
			corrupt: func(group *visibility.WriteGroup) {
				group.Members[0].StoredRowCount++
				group.RowCount++
				group.MembershipSHA256, _ = visibility.WriteGroupMembershipSHA256(group.Members)
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			connection := &fakeStoreConnection{batch: &fakeWriteBatch{}}
			sequencer := newFakeWriteGroupSequencer()
			store := mustTestStoreWithVisibility(t, connection, fixedRetention(time.Hour), sequencer)
			group := testWriteGroup(t, store, 2)
			test.corrupt(&group)
			if err := store.sendWriteGroup(
				context.Background(), group, "attempt", visibility.WriteGroupFillRecovery,
			); err == nil {
				t.Fatal("sendWriteGroup corruption error = nil")
			}
			if connection.prepareCalls != 0 || connection.batch.sendCalls != 0 ||
				sequencer.markCalls != 0 || sequencer.commitCalls != 0 || sequencer.releaseCalls != 1 {
				t.Fatalf(
					"corrupt group calls = prepare %d mark %d send %d commit %d release %d",
					connection.prepareCalls,
					sequencer.markCalls,
					connection.batch.sendCalls,
					sequencer.commitCalls,
					sequencer.releaseCalls,
				)
			}
		})
	}
}

func TestSendWriteGroupReplayUsesIdenticalTokenAndRows(t *testing.T) {
	sequencer := newFakeWriteGroupSequencer()
	firstConnection := &fakeStoreConnection{batch: &fakeWriteBatch{}}
	store := mustTestStoreWithVisibility(t, firstConnection, fixedRetention(time.Hour), sequencer)
	group := testWriteGroup(t, store, 3)
	sequencer.committedSequences = writeGroupSequences(group)
	if err := store.sendWriteGroup(
		context.Background(), group, "first", visibility.WriteGroupFillRowTarget,
	); err != nil {
		t.Fatalf("first send: %v", err)
	}
	firstRows := cloneRows(firstConnection.batch.rows)
	firstToken := firstConnection.settings["insert_deduplication_token"]

	secondConnection := &fakeStoreConnection{batch: &fakeWriteBatch{}}
	store.connection = secondConnection
	group.State = visibility.WriteGroupAmbiguous
	if err := store.sendWriteGroup(
		context.Background(), group, "second", visibility.WriteGroupFillRecovery,
	); err != nil {
		t.Fatalf("replay send: %v", err)
	}
	if secondConnection.settings["insert_deduplication_token"] != firstToken ||
		!reflect.DeepEqual(firstRows, secondConnection.batch.rows) {
		t.Fatalf(
			"replay changed physical insert: tokens %#v/%#v rows %#v/%#v",
			firstToken,
			secondConnection.settings["insert_deduplication_token"],
			firstRows,
			secondConnection.batch.rows,
		)
	}
}

func testWriteGroup(t *testing.T, store *Store, count int) visibility.WriteGroup {
	t.Helper()
	members := make([]visibility.Reservation, count)
	for index := range count {
		batch := validStoreBatch()
		batch.BatchID = "batch-" + string(rune('a'+index))
		batch.BatchSequence = uint64(index + 1)
		batch.SourceBatchSHA256 = testSourceBatchDigest(batch.BatchID)
		batch.Events[0].BatchID = batch.BatchID
		batch.Events[0].Event.EventId = "event-" + string(rune('1'+index))
		metadata, outbox, indexTime, err := store.freshReservationPayload(context.Background(), batch)
		if err != nil {
			t.Fatalf("freshReservationPayload(%d): %v", index, err)
		}
		rowCount, decodedBytes, err := reservationAccounting(batch)
		if err != nil {
			t.Fatalf("reservationAccounting(%d): %v", index, err)
		}
		members[index] = visibility.Reservation{
			BatchKey:          deduplicationToken(batch),
			SequenceKey:       sequenceIdentityKey(batch),
			Sequence:          uint64(index + 1),
			IndexTime:         indexTime,
			PayloadSHA256:     mustStorePayloadDigest(t, batch),
			Metadata:          metadata,
			Outbox:            outbox,
			StoredRowCount:    rowCount,
			DecodedEventBytes: decodedBytes,
			OutboxSHA256:      sha256.Sum256(outbox),
			CreatedAt:         indexTime.Add(time.Duration(index) * time.Millisecond),
		}
	}
	digest, err := visibility.WriteGroupMembershipSHA256(members)
	if err != nil {
		t.Fatalf("WriteGroupMembershipSHA256: %v", err)
	}
	var rows, decodedBytes uint64
	for _, member := range members {
		rows += uint64(member.StoredRowCount)
		decodedBytes += member.DecodedEventBytes
	}
	return visibility.WriteGroup{
		ID:               "wg-test-stable-token",
		State:            visibility.WriteGroupReady,
		Members:          members,
		MemberCount:      uint32(len(members)),
		RowCount:         rows,
		DecodedBytes:     decodedBytes,
		MembershipSHA256: digest,
		FirstSequence:    members[0].Sequence,
		LastSequence:     members[len(members)-1].Sequence,
		CreatedAt:        members[0].CreatedAt.Add(time.Millisecond),
	}
}

func mustStorePayloadDigest(t *testing.T, batch ingest.StoreBatch) [sha256.Size]byte {
	t.Helper()
	digest, err := storePayloadDigest(batch)
	if err != nil {
		t.Fatalf("storePayloadDigest: %v", err)
	}
	return digest
}

func cloneRows(rows [][]any) [][]any {
	cloned := make([][]any, len(rows))
	for index, row := range rows {
		cloned[index] = slices.Clone(row)
	}
	return cloned
}

func newProductionCoalescerTestStore(
	t *testing.T,
	ctx context.Context,
	connection storeConnection,
) (*Store, *visibility.SQLiteSequencer) {
	t.Helper()
	controlDB, err := control.Open(ctx, filepath.Join(t.TempDir(), "control.sqlite"))
	if err != nil {
		t.Fatalf("open control database: %v", err)
	}
	sequencer, err := visibility.NewSQLite(ctx, controlDB)
	if err != nil {
		_ = controlDB.Close()
		t.Fatalf("open visibility sequencer: %v", err)
	}
	store, err := newStore(
		connection,
		"open_splunk",
		"events",
		fixedRetention(time.Hour),
		sequencer,
		time.Now,
		10*time.Millisecond,
	)
	if err != nil {
		_ = sequencer.Close()
		_ = controlDB.Close()
		t.Fatalf("new production coalescer store: %v", err)
	}
	store.groupedProduction = true
	t.Cleanup(func() {
		_ = store.Close()
		_ = sequencer.Close()
		_ = controlDB.Close()
	})
	return store, sequencer
}

func coalescerTestBatch(label string, sequence uint64) ingest.StoreBatch {
	batch := validStoreBatch()
	batch.BatchID = "batch-" + label
	batch.BatchSequence = sequence
	batch.SourceBatchSHA256 = testSourceBatchDigest(batch.BatchID)
	batch.Events[0].BatchID = batch.BatchID
	batch.Events[0].Event.EventId = "event-" + label
	return batch
}

func storeBatchIdentity(batch ingest.StoreBatch) ingest.StoreBatchIdentity {
	return ingest.StoreBatchIdentity{
		TenantID:          batch.TenantID,
		Source:            batch.Source,
		CollectorID:       batch.CollectorID,
		BatchID:           batch.BatchID,
		BatchSequence:     batch.BatchSequence,
		SourceBatchSHA256: batch.SourceBatchSHA256,
	}
}

func waitForPendingReservations(
	t *testing.T,
	ctx context.Context,
	sequencer visibility.Sequencer,
) {
	t.Helper()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for {
		usage, err := sequencer.PendingUsage(ctx)
		if err != nil {
			t.Fatalf("read pending usage: %v", err)
		}
		if usage.Reservations >= 1 {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("wait for pending reservation: %v", ctx.Err())
		case <-ticker.C:
		}
	}
}

func waitForNativeWaiterCount(
	t *testing.T,
	ctx context.Context,
	store *Store,
	want uint32,
) {
	t.Helper()
	for store.commitWaiters.size() != want {
		select {
		case <-ctx.Done():
			t.Fatalf(
				"native waiter count = %d, want %d: %v",
				store.commitWaiters.size(),
				want,
				ctx.Err(),
			)
		case <-time.After(time.Millisecond):
		}
	}
}

type fakeWriteGroupSequencer struct {
	*fakeVisibilitySequencer
	markErr            error
	commitErr          error
	releaseErr         error
	markCalls          int
	commitCalls        int
	releaseCalls       int
	committedSequences []uint64
}

func newFakeWriteGroupSequencer() *fakeWriteGroupSequencer {
	return &fakeWriteGroupSequencer{fakeVisibilitySequencer: &fakeVisibilitySequencer{}}
}

func (*fakeWriteGroupSequencer) FormOrAcquireWriteGroup(
	context.Context,
	string,
	visibility.WriteGroupLimits,
	bool,
) (visibility.WriteGroupAcquisition, error) {
	return visibility.WriteGroupAcquisition{}, nil
}

func (sequencer *fakeWriteGroupSequencer) MarkWriteGroupSending(
	context.Context,
	string,
	string,
) error {
	sequencer.markCalls++
	return sequencer.markErr
}

func (sequencer *fakeWriteGroupSequencer) CommitWriteGroup(
	_ context.Context,
	_ string,
	_ string,
	_ time.Time,
) ([]uint64, error) {
	sequencer.commitCalls++
	if sequencer.commitErr != nil {
		return nil, sequencer.commitErr
	}
	return slices.Clone(sequencer.committedSequences), nil
}

func (sequencer *fakeWriteGroupSequencer) ReleaseWriteGroup(
	context.Context,
	string,
	string,
) error {
	sequencer.releaseCalls++
	return sequencer.releaseErr
}
