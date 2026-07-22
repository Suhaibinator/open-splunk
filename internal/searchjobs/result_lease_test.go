package searchjobs

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
)

func TestAcquireResultsForScopeAndLifecycle(t *testing.T) {
	t.Parallel()

	releaseActive := make(chan struct{})
	executions := 0
	manager := newTestManager(t, Config{
		Executor: executorFunc(func(ctx context.Context, _ clickhouse.CompiledQuery, sink ResultSink) error {
			executions++
			if executions == 1 {
				if err := sink.SetSchema(messageSchema()); err != nil {
					return err
				}
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-releaseActive:
				}
				return sink.AddRow([]Value{StringValue("ready")})
			}
			return errors.New("executor failed")
		}),
		MaxConcurrent:   1,
		CleanupInterval: -1,
		NewID:           sequenceIDs("lease-lifecycle"),
	})

	active, err := manager.Create(context.Background(), validRequest())
	if err != nil {
		t.Fatal(err)
	}
	waitForState(t, manager, active.ID, StateRunning)
	access := AccessScope{TenantID: "tenant", OwnerID: "owner"}
	if _, err := manager.AcquireResultsFor(context.Background(), access, active.ID); !errors.Is(err, ErrResultsNotReady) {
		t.Fatalf("AcquireResultsFor(active) = %v, want ErrResultsNotReady", err)
	}
	if _, err := manager.AcquireResultsFor(context.Background(), AccessScope{TenantID: "other", OwnerID: "owner"}, active.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("AcquireResultsFor(cross tenant) = %v, want ErrNotFound", err)
	}
	if _, err := manager.AcquireResultsFor(context.Background(), AccessScope{TenantID: "tenant", OwnerID: "other"}, active.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("AcquireResultsFor(cross owner) = %v, want ErrNotFound", err)
	}
	if _, err := manager.AcquireResultsFor(context.Background(), access, "missing"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("AcquireResultsFor(missing) = %v, want ErrNotFound", err)
	}

	close(releaseActive)
	waitForState(t, manager, active.ID, StateCompleted)
	lease, err := manager.AcquireResultsFor(context.Background(), access, active.ID)
	if err != nil {
		t.Fatalf("AcquireResultsFor(completed) error = %v", err)
	}
	if err := lease.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	failedRequest := withSPL(validRequest(), "index=main | table failure")
	failed, err := manager.Create(context.Background(), failedRequest)
	if err != nil {
		t.Fatal(err)
	}
	waitForState(t, manager, failed.ID, StateFailed)
	if _, err := manager.AcquireResultsFor(context.Background(), access, failed.ID); !errors.Is(err, ErrResultsUnavailable) {
		t.Fatalf("AcquireResultsFor(failed) = %v, want ErrResultsUnavailable", err)
	}

	canceledManager := newTestManager(t, Config{
		Executor: executorFunc(func(ctx context.Context, _ clickhouse.CompiledQuery, sink ResultSink) error {
			if err := sink.SetSchema(messageSchema()); err != nil {
				return err
			}
			<-ctx.Done()
			return ctx.Err()
		}),
		CleanupInterval: -1,
		NewID:           sequenceIDs("lease-canceled"),
	})
	canceled, err := canceledManager.Create(context.Background(), validRequest())
	if err != nil {
		t.Fatal(err)
	}
	waitForState(t, canceledManager, canceled.ID, StateRunning)
	if err := canceledManager.CancelFor(access, canceled.ID); err != nil {
		t.Fatal(err)
	}
	waitForState(t, canceledManager, canceled.ID, StateCanceled)
	if _, err := canceledManager.AcquireResultsFor(context.Background(), access, canceled.ID); !errors.Is(err, ErrResultsUnavailable) {
		t.Fatalf("AcquireResultsFor(canceled) = %v, want ErrResultsUnavailable", err)
	}
}

func TestResultLeaseMetadataIterationAndNestedValuesAreDetached(t *testing.T) {
	t.Parallel()

	nested, err := ObjectValue(
		ObjectField{Name: "bytes", Value: BytesValue([]byte{1, 2, 3})},
		ObjectField{Name: "list", Value: ListValue(StringValue("original"), BytesValue([]byte{4, 5}))},
	)
	if err != nil {
		t.Fatal(err)
	}
	manager := newTestManager(t, Config{
		Executor: executorFunc(func(_ context.Context, _ clickhouse.CompiledQuery, sink ResultSink) error {
			schema := Schema{Columns: []Column{
				{Name: "message", Kind: ValueKindString},
				{Name: "payload", Kind: ValueKindObject},
			}}
			if err := sink.SetSchema(schema); err != nil {
				return err
			}
			if err := sink.AddRow([]Value{StringValue("first"), nested}); err != nil {
				return err
			}
			return sink.AddRow([]Value{StringValue("second"), nested})
		}),
		CleanupInterval: -1,
		NewID:           sequenceIDs("lease-values"),
	})
	job, err := manager.Create(
		context.Background(),
		withSPL(validRequest(), "index=main | table message payload"),
	)
	if err != nil {
		t.Fatal(err)
	}
	waitForState(t, manager, job.ID, StateCompleted)
	access := AccessScope{TenantID: "tenant", OwnerID: "owner"}
	lease, err := manager.AcquireResultsFor(context.Background(), access, job.ID)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = lease.Close() }()

	if lease.RowCount() != 2 {
		t.Fatalf("RowCount() = %d, want 2", lease.RowCount())
	}
	if lease.Generation() == 0 {
		t.Fatal("Generation() = 0")
	}
	schema := lease.Schema()
	wantSchema := Schema{Columns: []Column{
		{Name: "message", Kind: ValueKindString},
		{Name: "payload", Kind: ValueKindObject},
	}}
	if !reflect.DeepEqual(schema, wantSchema) {
		t.Fatalf("Schema() = %#v, want %#v", schema, wantSchema)
	}
	schema.Columns[0].Name = "mutated"
	if got := lease.Schema().Columns[0].Name; got != "message" {
		t.Fatalf("Schema() retained caller mutation: %q", got)
	}

	row, ok, err := lease.Next(context.Background())
	if err != nil || !ok {
		t.Fatalf("Next() = (%#v, %v, %v), want first row", row, ok, err)
	}
	if row.Ordinal != 0 || len(row.Values) != 2 {
		t.Fatalf("first row = %#v", row)
	}
	row.Values[0] = StringValue("changed")
	fields, ok := row.Values[1].Object()
	if !ok {
		t.Fatal("payload is not an object")
	}
	bytesValue, ok := fields[0].Value.Bytes()
	if !ok {
		t.Fatal("payload bytes are not bytes")
	}
	bytesValue[0] = 99
	list, ok := fields[1].Value.List()
	if !ok {
		t.Fatal("payload list is not a list")
	}
	list[0] = StringValue("changed")
	listBytes, ok := list[1].Bytes()
	if !ok {
		t.Fatal("nested list bytes are not bytes")
	}
	listBytes[0] = 88

	second, ok, err := lease.Next(context.Background())
	if err != nil || !ok {
		t.Fatalf("second Next() = (%#v, %v, %v)", second, ok, err)
	}
	if text, _ := second.Values[0].String(); text != "second" {
		t.Fatalf("second row message = %q, want second", text)
	}
	assertNestedResultValue(t, second.Values[1])
	if row, ok, err := lease.Next(context.Background()); err != nil || ok || len(row.Values) != 0 {
		t.Fatalf("Next(EOF) = (%#v, %v, %v), want zero, false, nil", row, ok, err)
	}

	page, err := manager.ResultsFor(access, job.ID, PageRequest{})
	if err != nil {
		t.Fatal(err)
	}
	assertNestedResultValue(t, page.Rows[0].Values[1])
}

func TestResultLeaseSerializesConcurrentNext(t *testing.T) {
	t.Parallel()

	const rowCount = 128
	manager := newTestManager(t, Config{
		Executor: executorFunc(func(_ context.Context, _ clickhouse.CompiledQuery, sink ResultSink) error {
			if err := sink.SetSchema(messageSchema()); err != nil {
				return err
			}
			for index := range rowCount {
				if err := sink.AddRow([]Value{StringValue(fmt.Sprintf("row-%d", index))}); err != nil {
					return err
				}
			}
			return nil
		}),
		MaxRows:         rowCount,
		CleanupInterval: -1,
		NewID:           sequenceIDs("lease-concurrent-next"),
	})
	job, err := manager.Create(context.Background(), validRequest())
	if err != nil {
		t.Fatal(err)
	}
	waitForState(t, manager, job.ID, StateCompleted)
	lease, err := manager.AcquireResultsFor(context.Background(), AccessScope{TenantID: "tenant", OwnerID: "owner"}, job.ID)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = lease.Close() }()

	ordinals := make(chan uint64, rowCount)
	errorsSeen := make(chan error, 8)
	var workers sync.WaitGroup
	for range 8 {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for {
				row, ok, err := lease.Next(context.Background())
				if err != nil {
					errorsSeen <- err
					return
				}
				if !ok {
					return
				}
				ordinals <- row.Ordinal
			}
		}()
	}
	workers.Wait()
	close(ordinals)
	close(errorsSeen)
	for err := range errorsSeen {
		t.Errorf("Next() error = %v", err)
	}
	var got []int
	for ordinal := range ordinals {
		got = append(got, int(ordinal))
	}
	sort.Ints(got)
	if len(got) != rowCount {
		t.Fatalf("Next() returned %d rows, want %d", len(got), rowCount)
	}
	for ordinal := range rowCount {
		if got[ordinal] != ordinal {
			t.Fatalf("sorted ordinal %d = %d", ordinal, got[ordinal])
		}
	}
}

func TestResultLeasePinsExpiredStorageUntilFinalClose(t *testing.T) {
	t.Parallel()

	clock := &fakeClock{now: time.Date(2026, time.July, 22, 1, 0, 0, 0, time.UTC)}
	manager := newTestManager(t, Config{
		Executor: executorFunc(func(_ context.Context, _ clickhouse.CompiledQuery, sink ResultSink) error {
			if err := sink.SetSchema(messageSchema()); err != nil {
				return err
			}
			return sink.AddRow([]Value{StringValue("pinned")})
		}),
		RetentionTTL:     10 * time.Second,
		ExpiredRetention: 5 * time.Second,
		CleanupInterval:  -1,
		Now:              clock.Now,
		NewID:            sequenceIDs("lease-expiry"),
	})
	job, err := manager.Create(context.Background(), validRequest())
	if err != nil {
		t.Fatal(err)
	}
	waitForState(t, manager, job.ID, StateCompleted)
	access := AccessScope{TenantID: "tenant", OwnerID: "owner"}
	first, err := manager.AcquireResultsFor(context.Background(), access, job.ID)
	if err != nil {
		t.Fatal(err)
	}
	second, err := manager.AcquireResultsFor(context.Background(), access, job.ID)
	if err != nil {
		t.Fatal(err)
	}
	wantGeneration := first.Generation()
	if wantGeneration == 0 || second.Generation() != wantGeneration {
		t.Fatalf("lease generations = %d, %d", wantGeneration, second.Generation())
	}
	retainedBefore := retainedResultBudget(manager)
	if retainedBefore == 0 {
		t.Fatal("completed result retained no budget")
	}

	clock.Add(10 * time.Second)
	if changed := manager.Cleanup(); changed != 1 {
		t.Fatalf("Cleanup() changed %d jobs, want 1", changed)
	}
	expired, err := manager.Get(job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if expired.State != StateExpired || expired.Schema != nil {
		t.Fatalf("expired job = %#v, want hidden result schema", expired)
	}
	if got := retainedResultBudget(manager); got != retainedBefore {
		t.Fatalf("retained bytes after expiry = %d, want %d", got, retainedBefore)
	}
	assertLeaseCounts(t, manager, job.ID, 2, 2)
	if _, err := manager.AcquireResultsFor(context.Background(), access, job.ID); !errors.Is(err, ErrExpired) {
		t.Fatalf("AcquireResultsFor(expired) = %v, want ErrExpired", err)
	}
	if first.Generation() != wantGeneration || first.RowCount() != 1 || !reflect.DeepEqual(first.Schema(), messageSchema()) {
		t.Fatalf(
			"expired pinned metadata = generation %d rows %d schema %#v",
			first.Generation(),
			first.RowCount(),
			first.Schema(),
		)
	}
	row, ok, err := first.Next(context.Background())
	if err != nil || !ok {
		t.Fatalf("pinned Next() = (%#v, %v, %v)", row, ok, err)
	}
	if text, _ := row.Values[0].String(); text != "pinned" {
		t.Fatalf("pinned row = %q", text)
	}
	secondRow, ok, err := second.Next(context.Background())
	if err != nil || !ok || secondRow.Ordinal != 0 {
		t.Fatalf("independent second lease Next() = (%#v, %v, %v)", secondRow, ok, err)
	}

	firstAlias := first
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	if err := firstAlias.Close(); err != nil {
		t.Fatalf("Close() through copied interface error = %v", err)
	}
	if got := retainedResultBudget(manager); got != retainedBefore {
		t.Fatalf("retained bytes after first close = %d, want %d", got, retainedBefore)
	}
	assertLeaseCounts(t, manager, job.ID, 1, 1)
	if err := second.Close(); err != nil {
		t.Fatal(err)
	}
	if got := retainedResultBudget(manager); got != 0 {
		t.Fatalf("retained bytes after final close = %d, want 0", got)
	}
	assertLeaseCounts(t, manager, job.ID, 0, 0)
	entry := manager.lookup(job.ID)
	entry.mu.RLock()
	rows := len(entry.rows)
	schemaRetained := entry.resultSchema != nil
	entry.mu.RUnlock()
	if rows != 0 || schemaRetained {
		t.Fatalf("released source retains rows=%d schema=%v", rows, schemaRetained)
	}
	if _, ok, err := first.Next(context.Background()); !errors.Is(err, ErrResultLeaseClosed) || ok {
		t.Fatalf("Next(after Close) = (_, %v, %v), want ErrResultLeaseClosed", ok, err)
	}

	clock.Add(5 * time.Second)
	if changed := manager.Cleanup(); changed != 1 {
		t.Fatalf("tombstone Cleanup() changed %d jobs, want 1", changed)
	}
	if _, err := manager.Get(job.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Get(removed tombstone) = %v, want ErrNotFound", err)
	}
}

func TestAcquireResultsForRechecksExpiryAfterWaitingForEntryLock(t *testing.T) {
	t.Parallel()

	clock := &fakeClock{now: time.Date(2026, time.July, 22, 1, 30, 0, 0, time.UTC)}
	manager := newTestManager(t, Config{
		Executor: executorFunc(func(_ context.Context, _ clickhouse.CompiledQuery, sink ResultSink) error {
			if err := sink.SetSchema(messageSchema()); err != nil {
				return err
			}
			return sink.AddRow([]Value{StringValue("boundary")})
		}),
		RetentionTTL:    time.Second,
		CleanupInterval: -1,
		Now:             clock.Now,
		NewID:           sequenceIDs("lease-expiry-lock-boundary"),
	})
	job, err := manager.Create(context.Background(), validRequest())
	if err != nil {
		t.Fatal(err)
	}
	waitForState(t, manager, job.ID, StateCompleted)
	entry := manager.lookup(job.ID)
	entry.mu.Lock()
	entryLocked := true
	defer func() {
		if entryLocked {
			entry.mu.Unlock()
		}
	}()

	result := make(chan error, 1)
	started := make(chan struct{})
	go func() {
		close(started)
		lease, acquireErr := manager.AcquireResultsFor(
			context.Background(),
			AccessScope{TenantID: "tenant", OwnerID: "owner"},
			job.ID,
		)
		if lease != nil {
			_ = lease.Close()
		}
		result <- acquireErr
	}()

	<-started
	// Give the acquiring goroutine an opportunity to reach the deliberately
	// held entry lock, then cross the exact TTL boundary while it waits. The
	// assertion is independent of scheduling: a later start also observes the
	// advanced clock and must return ErrExpired.
	time.Sleep(10 * time.Millisecond)
	clock.Add(time.Second)
	entry.mu.Unlock()
	entryLocked = false
	select {
	case acquireErr := <-result:
		if !errors.Is(acquireErr, ErrExpired) {
			t.Fatalf("AcquireResultsFor(at expiry after wait) = %v, want ErrExpired", acquireErr)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("AcquireResultsFor did not return")
	}
}

func TestPinnedLeasePreventsTombstoneRemovalUntilRelease(t *testing.T) {
	t.Parallel()

	clock := &fakeClock{now: time.Date(2026, time.July, 22, 2, 0, 0, 0, time.UTC)}
	manager := newTestManager(t, Config{
		Executor: executorFunc(func(_ context.Context, _ clickhouse.CompiledQuery, sink ResultSink) error {
			if err := sink.SetSchema(messageSchema()); err != nil {
				return err
			}
			return sink.AddRow([]Value{StringValue("retained tombstone")})
		}),
		RetentionTTL:     time.Second,
		ExpiredRetention: time.Second,
		CleanupInterval:  -1,
		Now:              clock.Now,
		NewID:            sequenceIDs("lease-tombstone"),
	})
	job, err := manager.Create(context.Background(), validRequest())
	if err != nil {
		t.Fatal(err)
	}
	waitForState(t, manager, job.ID, StateCompleted)
	lease, err := manager.AcquireResultsFor(context.Background(), AccessScope{TenantID: "tenant", OwnerID: "owner"}, job.ID)
	if err != nil {
		t.Fatal(err)
	}
	metadataBefore := retainedMetadataBudget(manager)
	if metadataBefore == 0 {
		t.Fatal("created job retained no metadata budget")
	}
	clock.Add(time.Second)
	manager.Cleanup()
	clock.Add(time.Second)
	if changed := manager.Cleanup(); changed != 0 {
		t.Fatalf("Cleanup() changed %d pinned jobs, want 0", changed)
	}
	if got, err := manager.Get(job.ID); err != nil || got.State != StateExpired {
		t.Fatalf("Get(pinned tombstone) = (%#v, %v)", got, err)
	}
	if got := retainedMetadataBudget(manager); got != metadataBefore {
		t.Fatalf("pinned tombstone metadata = %d, want %d", got, metadataBefore)
	}
	if err := lease.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := manager.Get(job.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Get(after final release) = %v, want ErrNotFound", err)
	}
	if got := retainedMetadataBudget(manager); got != 0 {
		t.Fatalf("removed tombstone retained %d metadata bytes", got)
	}
}

func TestResultLeaseContextCancellationDoesNotLeakPin(t *testing.T) {
	t.Parallel()

	clock := &fakeClock{now: time.Date(2026, time.July, 22, 3, 0, 0, 0, time.UTC)}
	manager := newTestManager(t, Config{
		Executor: executorFunc(func(_ context.Context, _ clickhouse.CompiledQuery, sink ResultSink) error {
			if err := sink.SetSchema(messageSchema()); err != nil {
				return err
			}
			return sink.AddRow([]Value{StringValue("context")})
		}),
		RetentionTTL:    time.Second,
		CleanupInterval: -1,
		Now:             clock.Now,
		NewID:           sequenceIDs("lease-context"),
	})
	job, err := manager.Create(context.Background(), validRequest())
	if err != nil {
		t.Fatal(err)
	}
	waitForState(t, manager, job.ID, StateCompleted)
	access := AccessScope{TenantID: "tenant", OwnerID: "owner"}
	canceled, cancelCanceled := context.WithCancel(context.Background())
	cancelCanceled()
	if lease, err := manager.AcquireResultsFor(canceled, access, job.ID); !errors.Is(err, context.Canceled) || lease != nil {
		t.Fatalf("AcquireResultsFor(canceled) = (%v, %v), want nil, context.Canceled", lease, err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	lease, err := manager.AcquireResultsFor(ctx, access, job.ID)
	if err != nil {
		t.Fatal(err)
	}
	callContext, cancelCall := context.WithCancel(context.Background())
	cancelCall()
	if _, ok, err := lease.Next(callContext); !errors.Is(err, context.Canceled) || ok {
		t.Fatalf("Next(canceled) = (_, %v, %v), want context.Canceled", ok, err)
	}
	row, ok, err := lease.Next(context.Background())
	if err != nil || !ok || row.Ordinal != 0 {
		t.Fatalf("Next(after canceled call) = (%#v, %v, %v)", row, ok, err)
	}
	cancel()
	deadline := time.Now().Add(2 * time.Second)
	for {
		entry := manager.lookup(job.ID)
		entry.mu.RLock()
		pins := entry.resultPins
		entry.mu.RUnlock()
		if pins == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("canceled acquisition context retained %d pins", pins)
		}
		time.Sleep(time.Millisecond)
	}
	if _, ok, err := lease.Next(context.Background()); !errors.Is(err, ErrResultLeaseClosed) || ok {
		t.Fatalf("Next(context-closed lease) = (_, %v, %v)", ok, err)
	}
	clock.Add(time.Second)
	manager.Cleanup()
	if got := retainedResultBudget(manager); got != 0 {
		t.Fatalf("expired result retained %d bytes after context release", got)
	}
}

func TestManagerClosePreservesPinnedLeaseUntilRelease(t *testing.T) {
	t.Parallel()

	manager := newTestManager(t, Config{
		Executor: executorFunc(func(_ context.Context, _ clickhouse.CompiledQuery, sink ResultSink) error {
			if err := sink.SetSchema(messageSchema()); err != nil {
				return err
			}
			return sink.AddRow([]Value{StringValue("close")})
		}),
		CleanupInterval: -1,
		NewID:           sequenceIDs("lease-manager-close"),
	})
	job, err := manager.Create(context.Background(), validRequest())
	if err != nil {
		t.Fatal(err)
	}
	waitForState(t, manager, job.ID, StateCompleted)
	access := AccessScope{TenantID: "tenant", OwnerID: "owner"}
	lease, err := manager.AcquireResultsFor(context.Background(), access, job.ID)
	if err != nil {
		t.Fatal(err)
	}
	retainedBefore := retainedResultBudget(manager)
	if err := manager.Close(); err != nil {
		t.Fatal(err)
	}
	if got := retainedResultBudget(manager); got != retainedBefore {
		t.Fatalf("Close retained bytes = %d, want pinned %d", got, retainedBefore)
	}
	assertLeaseCounts(t, manager, job.ID, 1, 1)
	if _, err := manager.AcquireResultsFor(context.Background(), access, job.ID); !errors.Is(err, ErrClosed) {
		t.Fatalf("AcquireResultsFor(after Close) = %v, want ErrClosed", err)
	}
	row, ok, err := lease.Next(context.Background())
	if err != nil || !ok {
		t.Fatalf("Next(after manager Close) = (%#v, %v, %v)", row, ok, err)
	}
	if text, _ := row.Values[0].String(); text != "close" {
		t.Fatalf("row after manager Close = %q", text)
	}
	if err := lease.Close(); err != nil {
		t.Fatal(err)
	}
	if got := retainedResultBudget(manager); got != 0 {
		t.Fatalf("final lease Close retained %d bytes", got)
	}
	assertLeaseCounts(t, manager, job.ID, 0, 0)
}

func TestConcurrentResultPagesLeaseAndCleanup(t *testing.T) {
	t.Parallel()

	clock := &fakeClock{now: time.Date(2026, time.July, 22, 4, 0, 0, 0, time.UTC)}
	manager := newTestManager(t, Config{
		Executor: executorFunc(func(_ context.Context, _ clickhouse.CompiledQuery, sink ResultSink) error {
			if err := sink.SetSchema(messageSchema()); err != nil {
				return err
			}
			for index := range 64 {
				if err := sink.AddRow([]Value{StringValue(fmt.Sprintf("value-%d", index))}); err != nil {
					return err
				}
			}
			return nil
		}),
		MaxRows:          64,
		RetentionTTL:     time.Second,
		ExpiredRetention: time.Second,
		CleanupInterval:  -1,
		Now:              clock.Now,
		NewID:            sequenceIDs("lease-cleanup-race"),
	})
	job, err := manager.Create(context.Background(), validRequest())
	if err != nil {
		t.Fatal(err)
	}
	waitForState(t, manager, job.ID, StateCompleted)
	access := AccessScope{TenantID: "tenant", OwnerID: "owner"}
	lease, err := manager.AcquireResultsFor(context.Background(), access, job.ID)
	if err != nil {
		t.Fatal(err)
	}

	start := make(chan struct{})
	var racers sync.WaitGroup
	racers.Add(3)
	go func() {
		defer racers.Done()
		<-start
		for range 200 {
			_, pageErr := manager.ResultsFor(access, job.ID, PageRequest{Limit: 7})
			if pageErr != nil && !errors.Is(pageErr, ErrExpired) && !errors.Is(pageErr, ErrNotFound) {
				t.Errorf("ResultsFor() race error = %v", pageErr)
				return
			}
		}
	}()
	go func() {
		defer racers.Done()
		<-start
		for {
			_, ok, nextErr := lease.Next(context.Background())
			if nextErr != nil {
				t.Errorf("Next() race error = %v", nextErr)
				return
			}
			if !ok {
				return
			}
		}
	}()
	go func() {
		defer racers.Done()
		<-start
		clock.Add(time.Second)
		for range 50 {
			manager.Cleanup()
		}
	}()
	close(start)
	racers.Wait()
	if got := retainedResultBudget(manager); got == 0 {
		t.Fatal("cleanup reclaimed a pinned result")
	}
	if err := lease.Close(); err != nil {
		t.Fatal(err)
	}
	if got := retainedResultBudget(manager); got != 0 {
		t.Fatalf("lease Close retained %d bytes", got)
	}
}

func TestResultLeasePerJobCapacityAndRecovery(t *testing.T) {
	t.Parallel()

	manager := newTestManager(t, Config{
		Executor: executorFunc(func(_ context.Context, _ clickhouse.CompiledQuery, sink ResultSink) error {
			if err := sink.SetSchema(messageSchema()); err != nil {
				return err
			}
			return sink.AddRow([]Value{StringValue("capacity")})
		}),
		MaxResultLeases:       4,
		MaxResultLeasesPerJob: 2,
		CleanupInterval:       -1,
		NewID:                 sequenceIDs("lease-per-job-capacity"),
	})
	job := createCompletedMessageJob(t, manager)
	access := AccessScope{TenantID: "tenant", OwnerID: "owner"}
	first, err := manager.AcquireResultsFor(context.Background(), access, job.ID)
	if err != nil {
		t.Fatal(err)
	}
	second, err := manager.AcquireResultsFor(context.Background(), access, job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if lease, err := manager.AcquireResultsFor(context.Background(), access, job.ID); !errors.Is(err, ErrCapacity) || lease != nil {
		t.Fatalf("AcquireResultsFor(per-job full) = (%v, %v), want nil, ErrCapacity", lease, err)
	}
	assertLeaseCounts(t, manager, job.ID, 2, 2)

	firstAlias := first
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	if err := firstAlias.Close(); err != nil {
		t.Fatal(err)
	}
	replacement, err := manager.AcquireResultsFor(context.Background(), access, job.ID)
	if err != nil {
		t.Fatalf("AcquireResultsFor(after Close) error = %v", err)
	}
	assertLeaseCounts(t, manager, job.ID, 2, 2)
	for _, lease := range []ResultLease{second, replacement} {
		if err := lease.Close(); err != nil {
			t.Fatal(err)
		}
	}
	assertLeaseCounts(t, manager, job.ID, 0, 0)
}

func TestResultLeaseGlobalCapacityAcrossJobs(t *testing.T) {
	t.Parallel()

	manager := newTestManager(t, Config{
		Executor: executorFunc(func(_ context.Context, _ clickhouse.CompiledQuery, sink ResultSink) error {
			if err := sink.SetSchema(messageSchema()); err != nil {
				return err
			}
			return sink.AddRow([]Value{StringValue("global")})
		}),
		MaxConcurrent:         2,
		MaxResultLeases:       2,
		MaxResultLeasesPerJob: 2,
		CleanupInterval:       -1,
		NewID:                 sequenceIDs("lease-global-capacity"),
	})
	firstJob := createCompletedMessageJob(t, manager)
	secondJob := createCompletedMessageJob(t, manager)
	access := AccessScope{TenantID: "tenant", OwnerID: "owner"}
	first, err := manager.AcquireResultsFor(context.Background(), access, firstJob.ID)
	if err != nil {
		t.Fatal(err)
	}
	second, err := manager.AcquireResultsFor(context.Background(), access, secondJob.ID)
	if err != nil {
		t.Fatal(err)
	}
	if lease, err := manager.AcquireResultsFor(context.Background(), access, firstJob.ID); !errors.Is(err, ErrCapacity) || lease != nil {
		t.Fatalf("AcquireResultsFor(global full) = (%v, %v), want nil, ErrCapacity", lease, err)
	}
	assertLeaseCounts(t, manager, firstJob.ID, 1, 2)
	assertJobPins(t, manager, secondJob.ID, 1)

	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	replacement, err := manager.AcquireResultsFor(context.Background(), access, secondJob.ID)
	if err != nil {
		t.Fatalf("AcquireResultsFor(after global recovery) error = %v", err)
	}
	assertJobPins(t, manager, firstJob.ID, 0)
	assertLeaseCounts(t, manager, secondJob.ID, 2, 2)
	if err := second.Close(); err != nil {
		t.Fatal(err)
	}
	if err := replacement.Close(); err != nil {
		t.Fatal(err)
	}
	assertLeaseCounts(t, manager, secondJob.ID, 0, 0)
}

func TestResultLeaseCancellationAndConcurrentCloseReleaseCapacitySynchronously(t *testing.T) {
	t.Parallel()

	manager := newTestManager(t, Config{
		Executor: executorFunc(func(_ context.Context, _ clickhouse.CompiledQuery, sink ResultSink) error {
			if err := sink.SetSchema(messageSchema()); err != nil {
				return err
			}
			return sink.AddRow([]Value{StringValue("cancel close")})
		}),
		MaxResultLeases:       1,
		MaxResultLeasesPerJob: 1,
		CleanupInterval:       -1,
		NewID:                 sequenceIDs("lease-cancel-close"),
	})
	job := createCompletedMessageJob(t, manager)
	access := AccessScope{TenantID: "tenant", OwnerID: "owner"}
	ctx, cancel := context.WithCancel(context.Background())
	lease, err := manager.AcquireResultsFor(ctx, access, job.ID)
	if err != nil {
		t.Fatal(err)
	}

	start := make(chan struct{})
	var closers sync.WaitGroup
	for range 32 {
		alias := lease
		closers.Add(1)
		go func() {
			defer closers.Done()
			<-start
			if closeErr := alias.Close(); closeErr != nil {
				t.Errorf("Close() error = %v", closeErr)
			}
		}()
	}
	closers.Add(1)
	go func() {
		defer closers.Done()
		<-start
		cancel()
	}()
	close(start)
	closers.Wait()
	assertLeaseCounts(t, manager, job.ID, 0, 0)

	// Every concurrent Close must wait for the winning close operation to
	// release capacity before returning.
	replacement, err := manager.AcquireResultsFor(context.Background(), access, job.ID)
	if err != nil {
		t.Fatalf("AcquireResultsFor(immediately after concurrent Close) = %v", err)
	}
	if err := replacement.Close(); err != nil {
		t.Fatal(err)
	}
	assertLeaseCounts(t, manager, job.ID, 0, 0)
}

func TestFinalLeaseCloseRacingCleanupPreservesOtherJobBudgets(t *testing.T) {
	t.Parallel()

	clock := &fakeClock{now: time.Date(2026, time.July, 22, 5, 0, 0, 0, time.UTC)}
	manager := newTestManager(t, Config{
		Executor: executorFunc(func(_ context.Context, _ clickhouse.CompiledQuery, sink ResultSink) error {
			if err := sink.SetSchema(messageSchema()); err != nil {
				return err
			}
			return sink.AddRow([]Value{StringValue("budget sentinel")})
		}),
		MaxConcurrent:         2,
		MaxResultLeases:       2,
		MaxResultLeasesPerJob: 1,
		RetentionTTL:          10 * time.Second,
		ExpiredRetention:      time.Second,
		CleanupInterval:       -1,
		Now:                   clock.Now,
		NewID:                 sequenceIDs("lease-cleanup-budget-race"),
	})
	firstJob := createCompletedMessageJob(t, manager)
	access := AccessScope{TenantID: "tenant", OwnerID: "owner"}
	lease, err := manager.AcquireResultsFor(context.Background(), access, firstJob.ID)
	if err != nil {
		t.Fatal(err)
	}

	clock.Add(10 * time.Second)
	secondJob := createCompletedMessageJob(t, manager)
	secondLease, err := manager.AcquireResultsFor(context.Background(), access, secondJob.ID)
	if err != nil {
		t.Fatal(err)
	}
	if changed := manager.Cleanup(); changed != 1 {
		t.Fatalf("expiry Cleanup() changed %d jobs, want 1", changed)
	}
	secondEntry := manager.lookup(secondJob.ID)
	secondEntry.mu.RLock()
	wantRetained := secondEntry.retainedBytes
	wantMetadata := secondEntry.metadataBytes
	secondEntry.mu.RUnlock()
	if wantRetained == 0 || wantMetadata == 0 {
		t.Fatalf("sentinel budgets = result %d metadata %d", wantRetained, wantMetadata)
	}

	clock.Add(time.Second)
	start := make(chan struct{})
	var racers sync.WaitGroup
	racers.Add(2)
	go func() {
		defer racers.Done()
		<-start
		if closeErr := lease.Close(); closeErr != nil {
			t.Errorf("Close() error = %v", closeErr)
		}
	}()
	go func() {
		defer racers.Done()
		<-start
		for range 64 {
			manager.Cleanup()
		}
	}()
	close(start)
	racers.Wait()
	manager.Cleanup()
	if _, err := manager.Get(firstJob.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Get(first after release/cleanup race) = %v, want ErrNotFound", err)
	}
	if got, err := manager.Get(secondJob.ID); err != nil || got.State != StateCompleted {
		t.Fatalf("Get(sentinel) = (%#v, %v), want completed", got, err)
	}
	if got := retainedResultBudget(manager); got != wantRetained {
		t.Fatalf("result budget after race = %d, want sentinel %d", got, wantRetained)
	}
	if got := retainedMetadataBudget(manager); got != wantMetadata {
		t.Fatalf("metadata budget after race = %d, want sentinel %d", got, wantMetadata)
	}
	assertLeaseCounts(t, manager, secondJob.ID, 1, 1)
	if err := secondLease.Close(); err != nil {
		t.Fatal(err)
	}
	assertLeaseCounts(t, manager, secondJob.ID, 0, 0)
}

func TestAcquireResultsForRacingManagerClose(t *testing.T) {
	t.Parallel()

	manager := newTestManager(t, Config{
		Executor: executorFunc(func(_ context.Context, _ clickhouse.CompiledQuery, sink ResultSink) error {
			if err := sink.SetSchema(messageSchema()); err != nil {
				return err
			}
			return sink.AddRow([]Value{StringValue("manager close race")})
		}),
		MaxResultLeases:       1,
		MaxResultLeasesPerJob: 1,
		CleanupInterval:       -1,
		NewID:                 sequenceIDs("lease-manager-close-race"),
	})
	job := createCompletedMessageJob(t, manager)
	access := AccessScope{TenantID: "tenant", OwnerID: "owner"}
	start := make(chan struct{})
	acquired := make(chan struct {
		lease ResultLease
		err   error
	}, 1)
	closed := make(chan error, 1)
	go func() {
		<-start
		lease, err := manager.AcquireResultsFor(context.Background(), access, job.ID)
		acquired <- struct {
			lease ResultLease
			err   error
		}{lease: lease, err: err}
	}()
	go func() {
		<-start
		closed <- manager.Close()
	}()
	close(start)
	result := <-acquired
	if closeErr := <-closed; closeErr != nil {
		t.Fatal(closeErr)
	}
	if result.err != nil {
		if !errors.Is(result.err, ErrClosed) || result.lease != nil {
			t.Fatalf("AcquireResultsFor(Close race) = (%v, %v)", result.lease, result.err)
		}
	} else {
		row, ok, nextErr := result.lease.Next(context.Background())
		if nextErr != nil || !ok {
			t.Fatalf("pre-Close lease Next() = (%#v, %v, %v)", row, ok, nextErr)
		}
		if closeErr := result.lease.Close(); closeErr != nil {
			t.Fatal(closeErr)
		}
	}
	assertLeaseCounts(t, manager, job.ID, 0, 0)
}

func TestResultLeaseCloseRacingNextReleasesCapacity(t *testing.T) {
	t.Parallel()

	manager := newTestManager(t, Config{
		Executor: executorFunc(func(_ context.Context, _ clickhouse.CompiledQuery, sink ResultSink) error {
			if err := sink.SetSchema(messageSchema()); err != nil {
				return err
			}
			return sink.AddRow([]Value{StringValue("next close race")})
		}),
		MaxResultLeases:       1,
		MaxResultLeasesPerJob: 1,
		CleanupInterval:       -1,
		NewID:                 sequenceIDs("lease-next-close-race"),
	})
	job := createCompletedMessageJob(t, manager)
	access := AccessScope{TenantID: "tenant", OwnerID: "owner"}
	for iteration := range 64 {
		lease, err := manager.AcquireResultsFor(context.Background(), access, job.ID)
		if err != nil {
			t.Fatalf("iteration %d AcquireResultsFor() = %v", iteration, err)
		}
		start := make(chan struct{})
		nextResult := make(chan error, 1)
		closeResult := make(chan error, 1)
		go func() {
			<-start
			_, ok, nextErr := lease.Next(context.Background())
			if nextErr == nil && !ok {
				nextErr = errors.New("Next returned EOF before reading the only row")
			}
			nextResult <- nextErr
		}()
		go func() {
			<-start
			closeResult <- lease.Close()
		}()
		close(start)
		if nextErr := <-nextResult; nextErr != nil && !errors.Is(nextErr, ErrResultLeaseClosed) {
			t.Fatalf("iteration %d Next() error = %v", iteration, nextErr)
		}
		if closeErr := <-closeResult; closeErr != nil {
			t.Fatalf("iteration %d Close() error = %v", iteration, closeErr)
		}
		// Synchronous Close guarantees capacity can be reused immediately.
		replacement, err := manager.AcquireResultsFor(context.Background(), access, job.ID)
		if err != nil {
			t.Fatalf("iteration %d capacity reuse = %v", iteration, err)
		}
		if err := replacement.Close(); err != nil {
			t.Fatal(err)
		}
	}
	assertLeaseCounts(t, manager, job.ID, 0, 0)
}

func TestNewValidatesResultLeaseLimits(t *testing.T) {
	t.Parallel()

	base := Config{
		Executor: executorFunc(func(context.Context, clickhouse.CompiledQuery, ResultSink) error { return nil }),
		Snapshotter: snapshotterFunc(func(context.Context) (uint64, error) {
			return 0, nil
		}),
		CleanupInterval: -1,
		CursorKey:       testCursorKey,
	}
	tests := []struct {
		name   string
		mutate func(*Config)
	}{
		{name: "negative global", mutate: func(config *Config) { config.MaxResultLeases = -1 }},
		{name: "negative per job", mutate: func(config *Config) { config.MaxResultLeasesPerJob = -1 }},
		{name: "global above hard maximum", mutate: func(config *Config) { config.MaxResultLeases = maximumResultLeases + 1 }},
		{name: "per job above hard maximum", mutate: func(config *Config) { config.MaxResultLeasesPerJob = maximumResultLeasesPerJob + 1 }},
		{name: "per job exceeds global", mutate: func(config *Config) {
			config.MaxResultLeases = 1
			config.MaxResultLeasesPerJob = 2
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			config := base
			test.mutate(&config)
			if manager, err := New(config); err == nil {
				_ = manager.Close()
				t.Fatal("New() succeeded with an invalid result lease limit")
			}
		})
	}

	config := base
	config.MaxResultLeases = 1
	manager, err := New(config)
	if err != nil {
		t.Fatalf("New(global below default per-job limit) = %v", err)
	}
	if manager.maxResultLeases != 1 || manager.maxResultLeasesPerJob != 1 {
		t.Fatalf("normalized lease limits = global %d per-job %d", manager.maxResultLeases, manager.maxResultLeasesPerJob)
	}
	if err := manager.Close(); err != nil {
		t.Fatal(err)
	}
}

func assertNestedResultValue(t *testing.T, value Value) {
	t.Helper()
	fields, ok := value.Object()
	if !ok || len(fields) != 2 {
		t.Fatalf("object = %#v, %v", fields, ok)
	}
	bytesValue, ok := fields[0].Value.Bytes()
	if !ok || !reflect.DeepEqual(bytesValue, []byte{1, 2, 3}) {
		t.Fatalf("object bytes = %v, %v", bytesValue, ok)
	}
	list, ok := fields[1].Value.List()
	if !ok || len(list) != 2 {
		t.Fatalf("object list = %#v, %v", list, ok)
	}
	if text, _ := list[0].String(); text != "original" {
		t.Fatalf("list string = %q", text)
	}
	listBytes, ok := list[1].Bytes()
	if !ok || !reflect.DeepEqual(listBytes, []byte{4, 5}) {
		t.Fatalf("list bytes = %v, %v", listBytes, ok)
	}
}

func retainedResultBudget(manager *Manager) uint64 {
	manager.budgetMu.Lock()
	defer manager.budgetMu.Unlock()
	return manager.retainedBytes
}

func retainedMetadataBudget(manager *Manager) uint64 {
	manager.budgetMu.Lock()
	defer manager.budgetMu.Unlock()
	return manager.metadataBytes
}

func createCompletedMessageJob(t *testing.T, manager *Manager) Job {
	t.Helper()
	job, err := manager.Create(context.Background(), validRequest())
	if err != nil {
		t.Fatal(err)
	}
	return waitForState(t, manager, job.ID, StateCompleted)
}

func assertLeaseCounts(t *testing.T, manager *Manager, jobID string, wantJob, wantGlobal int) {
	t.Helper()
	assertJobPins(t, manager, jobID, wantJob)
	manager.budgetMu.Lock()
	gotGlobal := manager.activeResultLeases
	manager.budgetMu.Unlock()
	if gotGlobal != wantGlobal {
		t.Fatalf("active manager result leases = %d, want %d", gotGlobal, wantGlobal)
	}
}

func assertJobPins(t *testing.T, manager *Manager, jobID string, want int) {
	t.Helper()
	entry := manager.lookup(jobID)
	if entry == nil {
		if want == 0 {
			return
		}
		t.Fatalf("job %q is not retained", jobID)
	}
	entry.mu.RLock()
	got := entry.resultPins
	entry.mu.RUnlock()
	if got != want {
		t.Fatalf("job %q result pins = %d, want %d", jobID, got, want)
	}
}
