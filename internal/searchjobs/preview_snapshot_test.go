package searchjobs

import (
	"context"
	"errors"
	"fmt"
	"math"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
)

func TestPreviewForReturnsVersionConsistentRunningAndCompletedSnapshots(t *testing.T) {
	t.Parallel()

	allowSchema := make(chan struct{})
	schemaReady := make(chan struct{})
	allowFirstRow := make(chan struct{})
	firstRowReady := make(chan struct{})
	allowRemainingRows := make(chan struct{})
	remainingRowsReady := make(chan struct{})
	allowProgress := make(chan struct{})
	progressReady := make(chan struct{})
	allowCompletion := make(chan struct{})
	executor := executorFunc(func(ctx context.Context, _ clickhouse.CompiledQuery, sink ResultSink) error {
		if err := waitForPreviewTestSignal(ctx, allowSchema); err != nil {
			return err
		}
		if err := sink.SetSchema(messageSchema()); err != nil {
			return err
		}
		close(schemaReady)
		if err := waitForPreviewTestSignal(ctx, allowFirstRow); err != nil {
			return err
		}
		if err := sink.AddRow([]Value{StringValue("first")}); err != nil {
			return err
		}
		close(firstRowReady)
		if err := waitForPreviewTestSignal(ctx, allowRemainingRows); err != nil {
			return err
		}
		for _, value := range []string{"second", "third"} {
			if err := sink.AddRow([]Value{StringValue(value)}); err != nil {
				return err
			}
		}
		close(remainingRowsReady)
		if err := waitForPreviewTestSignal(ctx, allowProgress); err != nil {
			return err
		}
		progressSink, ok := sink.(ProgressSink)
		if !ok {
			return errors.New("result sink does not support progress")
		}
		if err := progressSink.ReportProgress(ExecutionProgressDelta{ScannedRows: 7, ScannedBytes: 70}); err != nil {
			return err
		}
		close(progressReady)
		return waitForPreviewTestSignal(ctx, allowCompletion)
	})
	manager := newTestManager(t, Config{
		Executor:        executor,
		MaxConcurrent:   1,
		MaxRows:         10,
		MaxBytes:        1 << 20,
		DefaultPageSize: 2,
		MaxPageSize:     10,
		MaxPageBytes:    1 << 20,
		CleanupInterval: -1,
		NewID:           sequenceIDs("live-preview"),
	})
	access := AccessScope{TenantID: "tenant", OwnerID: "owner"}
	created, err := manager.Create(context.Background(), validRequest())
	if err != nil {
		t.Fatal(err)
	}
	waitForState(t, manager, created.ID, StateRunning)

	if _, err := manager.PreviewFor(access, created.ID, 2); !errors.Is(err, ErrResultsNotReady) {
		t.Fatalf("PreviewFor(before schema) = %v, want ErrResultsNotReady", err)
	}

	close(allowSchema)
	waitForPreviewTestChannel(t, schemaReady)
	schemaOnly, err := manager.PreviewFor(access, created.ID, 2)
	if err != nil {
		t.Fatalf("PreviewFor(schema only) error = %v", err)
	}
	if schemaOnly.Job.State != StateRunning || schemaOnly.Job.Schema == nil || len(schemaOnly.Rows) != 0 ||
		schemaOnly.Truncated || schemaOnly.Revision == 0 {
		t.Fatalf("schema-only preview = %#v", schemaOnly)
	}

	close(allowFirstRow)
	waitForPreviewTestChannel(t, firstRowReady)
	first, err := manager.PreviewFor(access, created.ID, 2)
	if err != nil {
		t.Fatalf("PreviewFor(first row) error = %v", err)
	}
	if first.Revision <= schemaOnly.Revision || first.Truncated {
		t.Fatalf("first-row preview metadata = revision %d truncated %t", first.Revision, first.Truncated)
	}
	if got := resultStrings(t, first.Rows); !reflect.DeepEqual(got, []string{"first"}) {
		t.Fatalf("first-row preview = %v", got)
	}

	close(allowRemainingRows)
	waitForPreviewTestChannel(t, remainingRowsReady)
	running, err := manager.PreviewFor(access, created.ID, 2)
	if err != nil {
		t.Fatalf("PreviewFor(running) error = %v", err)
	}
	if running.Revision <= first.Revision || !running.Truncated {
		t.Fatalf("running preview metadata = revision %d truncated %t", running.Revision, running.Truncated)
	}
	if running.Job.RowCount != 3 {
		t.Fatalf("running Job.RowCount = %d, want 3", running.Job.RowCount)
	}
	if got := resultStrings(t, running.Rows); !reflect.DeepEqual(got, []string{"first", "second"}) {
		t.Fatalf("running preview = %v", got)
	}

	close(allowProgress)
	waitForPreviewTestChannel(t, progressReady)
	afterProgress, err := manager.PreviewFor(access, created.ID, 2)
	if err != nil {
		t.Fatalf("PreviewFor(after progress) error = %v", err)
	}
	if afterProgress.Revision != running.Revision {
		t.Fatalf("progress-only preview revision = %d, want unchanged %d", afterProgress.Revision, running.Revision)
	}
	if afterProgress.Job.Version <= running.Job.Version || afterProgress.Job.ScannedRows != 7 || afterProgress.Job.ScannedBytes != 70 {
		t.Fatalf("progress-only job = version %d rows %d bytes %d, want version > %d and 7/70",
			afterProgress.Job.Version, afterProgress.Job.ScannedRows, afterProgress.Job.ScannedBytes, running.Job.Version)
	}

	// Returned state is fully detached. Mutating it must not change the next
	// snapshot or the manager's retained schema and rows.
	afterProgress.Job.Schema.Columns[0].Name = "mutated"
	afterProgress.Rows[0].Values[0] = StringValue("mutated")
	detached, err := manager.PreviewFor(access, created.ID, 3)
	if err != nil {
		t.Fatalf("PreviewFor(after caller mutation) error = %v", err)
	}
	if detached.Job.Schema.Columns[0].Name != "message" {
		t.Fatalf("retained schema name = %q, want message", detached.Job.Schema.Columns[0].Name)
	}
	if got := resultStrings(t, detached.Rows); !reflect.DeepEqual(got, []string{"first", "second", "third"}) {
		t.Fatalf("retained rows after caller mutation = %v", got)
	}
	if detached.Truncated {
		t.Fatal("preview containing every retained row was marked truncated")
	}

	close(allowCompletion)
	waitForState(t, manager, created.ID, StateCompleted)
	completed, err := manager.PreviewFor(access, created.ID, 2)
	if err != nil {
		t.Fatalf("PreviewFor(completed) error = %v", err)
	}
	if completed.Job.State != StateCompleted || completed.Job.Version <= detached.Job.Version ||
		completed.Revision != detached.Revision || !completed.Truncated {
		t.Fatalf("completed preview = %#v", completed)
	}
}

func TestPreviewForBoundsRowsByPageBytesWithoutFailingSchemaSnapshot(t *testing.T) {
	t.Parallel()

	schema := messageSchema()
	schemaBytes, err := retainedSchemaSize(schema)
	if err != nil {
		t.Fatal(err)
	}
	_, valueBytes, err := measureValue(StringValue("a"), 0)
	if err != nil {
		t.Fatal(err)
	}
	rowBytes, err := checkedAdd(retainedResultRowBase, valueBytes)
	if err != nil {
		t.Fatal(err)
	}
	maxPageBytes, err := checkedAdd(schemaBytes, rowBytes)
	if err != nil {
		t.Fatal(err)
	}

	manager := newTestManager(t, Config{
		Executor: executorFunc(func(_ context.Context, _ clickhouse.CompiledQuery, sink ResultSink) error {
			if err := sink.SetSchema(schema); err != nil {
				return err
			}
			for range 3 {
				if err := sink.AddRow([]Value{StringValue("a")}); err != nil {
					return err
				}
			}
			return nil
		}),
		MaxRows:         3,
		MaxBytes:        1 << 20,
		DefaultPageSize: 3,
		MaxPageSize:     3,
		MaxPageBytes:    maxPageBytes,
		CleanupInterval: -1,
		NewID:           sequenceIDs("byte-bounded-preview"),
	})
	created, err := manager.Create(context.Background(), validRequest())
	if err != nil {
		t.Fatal(err)
	}
	waitForState(t, manager, created.ID, StateCompleted)
	access := AccessScope{TenantID: "tenant", OwnerID: "owner"}

	preview, err := manager.PreviewFor(access, created.ID, 3)
	if err != nil {
		t.Fatalf("PreviewFor(byte bounded) error = %v", err)
	}
	if len(preview.Rows) != 1 || !preview.Truncated || preview.Job.ResultsTruncated {
		t.Fatalf("byte-bounded preview = rows %d truncated %t job truncated %t", len(preview.Rows), preview.Truncated, preview.Job.ResultsTruncated)
	}
	transportBounded, err := manager.PreviewForBytes(access, created.ID, 3, schemaBytes)
	if err != nil {
		t.Fatalf("PreviewForBytes(schema-only transport bound) error = %v", err)
	}
	if transportBounded.Job.Schema == nil || len(transportBounded.Rows) != 0 || !transportBounded.Truncated {
		t.Fatalf("transport byte-bounded preview = %#v", transportBounded)
	}
	if _, err := manager.PreviewForBytes(access, created.ID, 3, 0); !errors.Is(err, ErrByteLimit) {
		t.Fatalf("PreviewForBytes(zero bound) = %v, want ErrByteLimit", err)
	}

	// A schema remains useful even when no row can fit the current preview
	// byte budget. This intentionally differs from paginated result reads.
	manager.maxPageBytes = schemaBytes
	schemaOnly, err := manager.PreviewFor(access, created.ID, 3)
	if err != nil {
		t.Fatalf("PreviewFor(schema-only byte bound) error = %v", err)
	}
	if schemaOnly.Job.Schema == nil || len(schemaOnly.Rows) != 0 || !schemaOnly.Truncated {
		t.Fatalf("schema-only byte-bounded preview = %#v", schemaOnly)
	}
}

func TestPreviewRevisionChangesWhenResultCompletenessChanges(t *testing.T) {
	t.Parallel()

	overflowObserved := make(chan struct{})
	allowCompletion := make(chan struct{})
	manager := newTestManager(t, Config{
		Executor: executorFunc(func(ctx context.Context, _ clickhouse.CompiledQuery, sink ResultSink) error {
			if err := sink.SetSchema(messageSchema()); err != nil {
				return err
			}
			if err := sink.AddRow([]Value{StringValue("retained")}); err != nil {
				return err
			}
			overflowErr := sink.AddRow([]Value{StringValue("discarded")})
			close(overflowObserved)
			if err := waitForPreviewTestSignal(ctx, allowCompletion); err != nil {
				return err
			}
			return fmt.Errorf("stop at retained result boundary: %w", overflowErr)
		}),
		MaxRows:         1,
		MaxBytes:        1 << 20,
		DefaultPageSize: 1,
		MaxPageSize:     1,
		MaxPageBytes:    1 << 20,
		CleanupInterval: -1,
		NewID:           sequenceIDs("truncated-preview-revision"),
	})
	created, err := manager.Create(context.Background(), validRequest())
	if err != nil {
		t.Fatal(err)
	}
	waitForPreviewTestChannel(t, overflowObserved)
	access := AccessScope{TenantID: "tenant", OwnerID: "owner"}
	running, err := manager.PreviewFor(access, created.ID, 1)
	if err != nil {
		t.Fatalf("PreviewFor(before truncation completion) error = %v", err)
	}
	if running.Truncated || running.Job.ResultsTruncated {
		t.Fatalf("running preview was prematurely truncated: %#v", running)
	}

	close(allowCompletion)
	waitForState(t, manager, created.ID, StateCompleted)
	completed, err := manager.PreviewFor(access, created.ID, 1)
	if err != nil {
		t.Fatalf("PreviewFor(after truncation completion) error = %v", err)
	}
	if !completed.Truncated || !completed.Job.ResultsTruncated || completed.Revision <= running.Revision {
		t.Fatalf("completed truncated preview = %#v, want revision greater than %d", completed, running.Revision)
	}
}

func TestResultRevisionOverflowRejectsSchemaAndRowsAtomically(t *testing.T) {
	t.Parallel()

	t.Run("schema", func(t *testing.T) {
		entry := &jobEntry{
			job:            Job{State: StateRunning, Version: 17},
			resultRevision: math.MaxUint64,
		}
		sink := &resultSink{
			manager:        &Manager{maxPageBytes: 1 << 20},
			entry:          entry,
			ctx:            context.Background(),
			expectedFields: []string{"message"},
		}
		if err := sink.SetSchema(messageSchema()); !errors.Is(err, ErrInvalidResult) {
			t.Fatalf("SetSchema() = %v, want ErrInvalidResult", err)
		}
		if entry.resultSchema != nil || entry.job.Schema != nil || entry.job.Version != 17 ||
			entry.resultRevision != math.MaxUint64 {
			t.Fatalf("schema overflow mutated entry = %#v", entry)
		}
	})

	t.Run("row", func(t *testing.T) {
		manager := &Manager{
			maxRows:         1,
			maxBytes:        1 << 20,
			maxTotalBytes:   1 << 20,
			maxPageBytes:    1 << 20,
			maxPageSize:     1,
			defaultPageSize: 1,
		}
		entry := &jobEntry{job: Job{State: StateRunning, Version: 23}}
		sink := &resultSink{
			manager:        manager,
			entry:          entry,
			ctx:            context.Background(),
			expectedFields: []string{"message"},
		}
		if err := sink.SetSchema(messageSchema()); err != nil {
			t.Fatalf("SetSchema() error = %v", err)
		}
		entry.resultRevision = math.MaxUint64
		beforeVersion := entry.job.Version
		if err := sink.AddRow([]Value{StringValue("rejected")}); !errors.Is(err, ErrInvalidResult) {
			t.Fatalf("AddRow() = %v, want ErrInvalidResult", err)
		}
		if len(entry.rows) != 0 || entry.job.RowCount != 0 || entry.job.ResultBytes != 0 ||
			entry.job.Version != beforeVersion || entry.resultRevision != math.MaxUint64 {
			t.Fatalf("row overflow mutated entry = %#v", entry)
		}
	})
}

func TestPreviewForIsCoherentDuringConcurrentResultUpdates(t *testing.T) {
	t.Parallel()

	const (
		rowCount     = 256
		previewLimit = 32
		readers      = 8
	)
	schemaReady := make(chan struct{})
	startRows := make(chan struct{})
	manager := newTestManager(t, Config{
		Executor: executorFunc(func(ctx context.Context, _ clickhouse.CompiledQuery, sink ResultSink) error {
			if err := sink.SetSchema(messageSchema()); err != nil {
				return err
			}
			close(schemaReady)
			if err := waitForPreviewTestSignal(ctx, startRows); err != nil {
				return err
			}
			for ordinal := range rowCount {
				if err := sink.AddRow([]Value{StringValue(fmt.Sprintf("row-%03d", ordinal))}); err != nil {
					return err
				}
			}
			return nil
		}),
		MaxRows:         rowCount,
		MaxBytes:        1 << 20,
		DefaultPageSize: previewLimit,
		MaxPageSize:     previewLimit,
		MaxPageBytes:    1 << 20,
		CleanupInterval: -1,
		NewID:           sequenceIDs("concurrent-preview"),
	})
	created, err := manager.Create(context.Background(), validRequest())
	if err != nil {
		t.Fatal(err)
	}
	waitForPreviewTestChannel(t, schemaReady)
	access := AccessScope{TenantID: "tenant", OwnerID: "owner"}

	ready := make(chan struct{}, readers)
	errs := make(chan error, readers)
	var wait sync.WaitGroup
	wait.Add(readers)
	for range readers {
		go func() {
			defer wait.Done()
			firstRead := true
			for {
				preview, previewErr := manager.PreviewFor(access, created.ID, previewLimit)
				if previewErr != nil {
					errs <- previewErr
					return
				}
				wantRows := int(preview.Job.RowCount)
				if wantRows > previewLimit {
					wantRows = previewLimit
				}
				wantTruncated := preview.Job.ResultsTruncated || preview.Job.RowCount > uint64(wantRows)
				if len(preview.Rows) != wantRows || preview.Truncated != wantTruncated ||
					preview.Revision != preview.Job.RowCount+1 {
					errs <- fmt.Errorf(
						"incoherent preview: state=%s job_rows=%d rows=%d revision=%d truncated=%t",
						preview.Job.State,
						preview.Job.RowCount,
						len(preview.Rows),
						preview.Revision,
						preview.Truncated,
					)
					return
				}
				for ordinal, row := range preview.Rows {
					if row.Ordinal != uint64(ordinal) {
						errs <- fmt.Errorf("preview row %d has ordinal %d", ordinal, row.Ordinal)
						return
					}
				}
				if firstRead {
					ready <- struct{}{}
					firstRead = false
				}
				if preview.Job.State == StateCompleted {
					return
				}
			}
		}()
	}
	for range readers {
		waitForPreviewTestChannel(t, ready)
	}
	close(startRows)
	wait.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("concurrent PreviewFor() error = %v", err)
	}
}

func TestPreviewForValidatesScopeLimitAndLifecycle(t *testing.T) {
	t.Parallel()

	manager := newTestManager(t, Config{
		Executor: executorFunc(func(_ context.Context, _ clickhouse.CompiledQuery, sink ResultSink) error {
			if err := sink.SetSchema(messageSchema()); err != nil {
				return err
			}
			return sink.AddRow([]Value{StringValue("ready")})
		}),
		DefaultPageSize: 2,
		MaxPageSize:     2,
		MaxPageBytes:    1 << 20,
		CleanupInterval: -1,
		NewID:           sequenceIDs("preview-validation"),
	})
	created, err := manager.Create(context.Background(), validRequest())
	if err != nil {
		t.Fatal(err)
	}
	waitForState(t, manager, created.ID, StateCompleted)
	access := AccessScope{TenantID: "tenant", OwnerID: "owner"}

	for _, limit := range []int{-1, 0, 3} {
		if _, err := manager.PreviewFor(access, created.ID, limit); !errors.Is(err, ErrPageSize) {
			t.Fatalf("PreviewFor(limit %d) = %v, want ErrPageSize", limit, err)
		}
	}
	for _, test := range []struct {
		name   string
		access AccessScope
		id     string
	}{
		{name: "invalid scope", access: AccessScope{}, id: created.ID},
		{name: "cross tenant", access: AccessScope{TenantID: "other", OwnerID: "owner"}, id: created.ID},
		{name: "cross owner", access: AccessScope{TenantID: "tenant", OwnerID: "other"}, id: created.ID},
		{name: "missing", access: access, id: "missing"},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := manager.PreviewFor(test.access, test.id, 1); !errors.Is(err, ErrNotFound) {
				t.Fatalf("PreviewFor() = %v, want ErrNotFound", err)
			}
		})
	}
}

func TestPreviewForRejectsFailedCanceledAndExpiredJobs(t *testing.T) {
	t.Parallel()

	access := AccessScope{TenantID: "tenant", OwnerID: "owner"}
	t.Run("failed", func(t *testing.T) {
		manager := newTestManager(t, Config{
			Executor: executorFunc(func(_ context.Context, _ clickhouse.CompiledQuery, sink ResultSink) error {
				if err := sink.SetSchema(messageSchema()); err != nil {
					return err
				}
				return errors.New("executor failed")
			}),
			CleanupInterval: -1,
			NewID:           sequenceIDs("failed-preview"),
		})
		created, err := manager.Create(context.Background(), validRequest())
		if err != nil {
			t.Fatal(err)
		}
		waitForState(t, manager, created.ID, StateFailed)
		if _, err := manager.PreviewFor(access, created.ID, 1); !errors.Is(err, ErrResultsUnavailable) {
			t.Fatalf("PreviewFor(failed) = %v, want ErrResultsUnavailable", err)
		}
	})

	t.Run("canceled", func(t *testing.T) {
		started := make(chan struct{})
		manager := newTestManager(t, Config{
			Executor: executorFunc(func(ctx context.Context, _ clickhouse.CompiledQuery, _ ResultSink) error {
				close(started)
				<-ctx.Done()
				return ctx.Err()
			}),
			CleanupInterval: -1,
			NewID:           sequenceIDs("canceled-preview"),
		})
		created, err := manager.Create(context.Background(), validRequest())
		if err != nil {
			t.Fatal(err)
		}
		waitForPreviewTestChannel(t, started)
		if err := manager.CancelFor(access, created.ID); err != nil {
			t.Fatal(err)
		}
		waitForState(t, manager, created.ID, StateCanceled)
		if _, err := manager.PreviewFor(access, created.ID, 1); !errors.Is(err, ErrResultsUnavailable) {
			t.Fatalf("PreviewFor(canceled) = %v, want ErrResultsUnavailable", err)
		}
	})

	t.Run("expired", func(t *testing.T) {
		clock := &fakeClock{now: time.Date(2026, time.July, 23, 12, 0, 0, 0, time.UTC)}
		manager := newTestManager(t, Config{
			Executor: executorFunc(func(_ context.Context, _ clickhouse.CompiledQuery, sink ResultSink) error {
				return sink.SetSchema(messageSchema())
			}),
			RetentionTTL:    time.Minute,
			CleanupInterval: -1,
			Now:             clock.Now,
			NewID:           sequenceIDs("expired-preview"),
		})
		created, err := manager.Create(context.Background(), validRequest())
		if err != nil {
			t.Fatal(err)
		}
		waitForState(t, manager, created.ID, StateCompleted)
		clock.Add(time.Minute)
		if _, err := manager.PreviewFor(access, created.ID, 1); !errors.Is(err, ErrExpired) {
			t.Fatalf("PreviewFor(expired) = %v, want ErrExpired", err)
		}
	})
}

func TestPreviewForRechecksExpiryAfterWaitingForEntryLock(t *testing.T) {
	t.Parallel()

	clock := &fakeClock{now: time.Date(2026, time.July, 23, 13, 0, 0, 0, time.UTC)}
	manager := newTestManager(t, Config{
		Executor: executorFunc(func(_ context.Context, _ clickhouse.CompiledQuery, sink ResultSink) error {
			if err := sink.SetSchema(messageSchema()); err != nil {
				return err
			}
			return sink.AddRow([]Value{StringValue("boundary")})
		}),
		DefaultPageSize: 1,
		MaxPageSize:     1,
		RetentionTTL:    time.Second,
		CleanupInterval: -1,
		Now:             clock.Now,
		NewID:           sequenceIDs("preview-expiry-lock-boundary"),
	})
	created, err := manager.Create(context.Background(), validRequest())
	if err != nil {
		t.Fatal(err)
	}
	waitForState(t, manager, created.ID, StateCompleted)
	entry := manager.lookup(created.ID)
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
		_, previewErr := manager.PreviewFor(
			AccessScope{TenantID: "tenant", OwnerID: "owner"},
			created.ID,
			1,
		)
		result <- previewErr
	}()
	<-started
	time.Sleep(10 * time.Millisecond)
	clock.Add(time.Second)
	entry.mu.Unlock()
	entryLocked = false

	select {
	case previewErr := <-result:
		if !errors.Is(previewErr, ErrExpired) {
			t.Fatalf("PreviewFor(at expiry after wait) = %v, want ErrExpired", previewErr)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("PreviewFor did not return")
	}
}

func waitForPreviewTestSignal(ctx context.Context, signal <-chan struct{}) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-signal:
		return nil
	}
}

func waitForPreviewTestChannel(t *testing.T, signal <-chan struct{}) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for preview test signal")
	}
}
