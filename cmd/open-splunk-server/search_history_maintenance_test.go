package main

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/searchhistory"
)

type searchHistoryPrunerFunc func(
	context.Context,
	int,
	*searchhistory.MaintenanceCursor,
) (searchhistory.MaintenancePruneResult, error)

func (pruner searchHistoryPrunerFunc) PruneMaintenanceBatch(
	ctx context.Context,
	maximumRows int,
	cursor *searchhistory.MaintenanceCursor,
) (searchhistory.MaintenancePruneResult, error) {
	return pruner(ctx, maximumRows, cursor)
}

func TestSearchHistoryMaintenancePhysicallyPrunesExpiredTerminalScopes(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	database, err := control.Open(ctx, filepath.Join(t.TempDir(), "control.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := database.Close(); err != nil {
			t.Error(err)
		}
	})

	now := time.Date(2026, time.July, 29, 12, 0, 0, 0, time.UTC)
	store, err := searchhistory.New(database, searchhistory.Options{
		Clock:      func() time.Time { return now },
		CursorKey:  []byte("runtime-history-retention-test-key"),
		MaximumAge: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	scope := searchhistory.AccessScope{TenantID: "tenant", OwnerID: "owner"}
	otherScope := searchhistory.AccessScope{TenantID: "tenant", OwnerID: "other"}
	if _, err := store.Record(
		ctx,
		scope,
		runtimeSearchHistoryEntry(
			"expired",
			opensplunkv1.SearchJobState_SEARCH_JOB_STATE_COMPLETED,
			now,
		),
	); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Record(
		ctx,
		otherScope,
		runtimeSearchHistoryEntry(
			"other-expired",
			opensplunkv1.SearchJobState_SEARCH_JOB_STATE_COMPLETED,
			now,
		),
	); err != nil {
		t.Fatal(err)
	}
	if _, err := store.BeginAttempt(
		ctx,
		scope,
		runtimeSearchHistoryEntry(
			"pending-expired",
			opensplunkv1.SearchJobState_SEARCH_JOB_STATE_QUEUED,
			now,
		),
	); err != nil {
		t.Fatal(err)
	}
	now = now.Add(time.Microsecond)
	if _, err := store.Record(
		ctx,
		scope,
		runtimeSearchHistoryEntry(
			"boundary",
			opensplunkv1.SearchJobState_SEARCH_JOB_STATE_COMPLETED,
			now,
		),
	); err != nil {
		t.Fatal(err)
	}
	now = now.Add(time.Hour)

	ticks := make(chan time.Time, 1)
	pruned := make(chan struct{}, 1)
	pruner := searchHistoryPrunerFunc(func(
		ctx context.Context,
		maximumRows int,
		cursor *searchhistory.MaintenanceCursor,
	) (searchhistory.MaintenancePruneResult, error) {
		result, err := store.PruneMaintenanceBatch(ctx, maximumRows, cursor)
		pruned <- struct{}{}
		return result, err
	})
	maintenance, err := newSearchHistoryMaintenance(
		pruner,
		searchHistoryMaintenanceConfig{
			interval:     time.Hour,
			pruneTimeout: time.Second,
			ticks:        ticks,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := maintenance.Close(closeCtx); err != nil {
			t.Error(err)
		}
	})

	ticks <- now
	awaitSearchHistorySignal(t, pruned, "periodic physical prune")

	var currentExpired, boundary, otherExpired, pending int
	row := database.SQLDB().QueryRowContext(ctx, `
		SELECT
			COUNT(*) FILTER (WHERE search_job_id = 'expired'),
			COUNT(*) FILTER (WHERE search_job_id = 'boundary'),
			COUNT(*) FILTER (WHERE search_job_id = 'other-expired')
		FROM search_history
	`)
	if err := row.Scan(&currentExpired, &boundary, &otherExpired); err != nil {
		t.Fatal(err)
	}
	if err := database.SQLDB().QueryRowContext(
		ctx,
		`SELECT COUNT(*) FROM search_history_pending WHERE search_job_id = 'pending-expired'`,
	).Scan(&pending); err != nil {
		t.Fatal(err)
	}
	if currentExpired != 0 || boundary != 1 || otherExpired != 0 || pending != 1 {
		t.Fatalf(
			"retained rows = expired:%d boundary:%d other:%d pending:%d",
			currentExpired,
			boundary,
			otherExpired,
			pending,
		)
	}
}

func TestSearchHistoryMaintenanceRetriesWithoutOverlappingPrunes(t *testing.T) {
	t.Parallel()

	retryErr := errors.New("temporary prune failure")
	var calls atomic.Int64
	var active atomic.Int64
	var maximumActive atomic.Int64
	entered := make(chan int64, 4)
	release := make(chan struct{}, 2)
	pruner := searchHistoryPrunerFunc(func(
		ctx context.Context,
		_ int,
		_ *searchhistory.MaintenanceCursor,
	) (searchhistory.MaintenancePruneResult, error) {
		call := calls.Add(1)
		current := active.Add(1)
		raiseSearchHistoryMaximum(&maximumActive, current)
		defer active.Add(-1)
		entered <- call
		if call == 1 {
			return searchhistory.MaintenancePruneResult{}, retryErr
		}
		select {
		case <-release:
			return searchhistory.MaintenancePruneResult{Deleted: 1}, nil
		case <-ctx.Done():
			return searchhistory.MaintenancePruneResult{}, ctx.Err()
		}
	})
	ticks := make(chan time.Time, 4)
	reported := make(chan error, 1)
	maintenance, err := newSearchHistoryMaintenance(
		pruner,
		searchHistoryMaintenanceConfig{
			interval:     time.Hour,
			pruneTimeout: time.Second,
			ticks:        ticks,
			onError: func(err error) {
				reported <- err
			},
		},
	)
	if err != nil {
		t.Fatal(err)
	}

	ticks <- time.Now()
	if call := awaitSearchHistorySignal(t, entered, "failed prune"); call != 1 {
		t.Fatalf("first call = %d", call)
	}
	if err := awaitSearchHistorySignal(t, reported, "reported prune failure"); !errors.Is(err, retryErr) {
		t.Fatalf("reported error = %v, want %v", err, retryErr)
	}

	ticks <- time.Now()
	if call := awaitSearchHistorySignal(t, entered, "retry prune"); call != 2 {
		t.Fatalf("retry call = %d", call)
	}
	ticks <- time.Now()
	select {
	case call := <-entered:
		t.Fatalf("overlapping call %d entered while call 2 was blocked", call)
	case <-time.After(25 * time.Millisecond):
	}
	release <- struct{}{}
	if call := awaitSearchHistorySignal(t, entered, "coalesced follow-up prune"); call != 3 {
		t.Fatalf("follow-up call = %d", call)
	}
	release <- struct{}{}

	closeCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := maintenance.Close(closeCtx); err != nil {
		t.Fatal(err)
	}
	if maximumActive.Load() != 1 {
		t.Fatalf("maximum concurrent prunes = %d, want 1", maximumActive.Load())
	}
}

func TestPruneSearchHistoryAtStartupDrainsEveryBoundedBatch(t *testing.T) {
	t.Parallel()

	var calls atomic.Int64
	pruner := searchHistoryPrunerFunc(func(
		_ context.Context,
		maximumRows int,
		_ *searchhistory.MaintenanceCursor,
	) (searchhistory.MaintenancePruneResult, error) {
		if maximumRows != searchHistoryMaintenanceBatchSize {
			t.Fatalf(
				"startup maximum rows = %d, want %d",
				maximumRows,
				searchHistoryMaintenanceBatchSize,
			)
		}
		switch calls.Add(1) {
		case 1:
			return searchhistory.MaintenancePruneResult{
				Deleted: 256,
				More:    true,
			}, nil
		case 2:
			return searchhistory.MaintenancePruneResult{Deleted: 7}, nil
		default:
			t.Fatal("startup pruning continued after the final batch")
			return searchhistory.MaintenancePruneResult{}, nil
		}
	})
	result, err := pruneSearchHistoryAtStartup(context.Background(), pruner)
	if err != nil {
		t.Fatal(err)
	}
	if result.deleted != 263 || result.more || result.cursor != nil ||
		calls.Load() != 2 {
		t.Fatalf(
			"startup pruning = deleted:%d more:%t cursor:%p calls:%d, want 263, false, nil, and 2",
			result.deleted,
			result.more,
			result.cursor,
			calls.Load(),
		)
	}
}

func TestStartupPruneBoundsReadinessAndWorkerImmediatelyContinuesCursor(t *testing.T) {
	t.Parallel()

	continuation := new(searchhistory.MaintenanceCursor)
	var calls atomic.Int64
	continued := make(chan struct{}, 1)
	failures := make(chan string, searchHistoryStartupMaxBatches+1)
	pruner := searchHistoryPrunerFunc(func(
		_ context.Context,
		maximumRows int,
		cursor *searchhistory.MaintenanceCursor,
	) (searchhistory.MaintenancePruneResult, error) {
		call := calls.Add(1)
		if call == 1 {
			if cursor != nil {
				failures <- "first startup batch received a cursor"
			}
		} else if cursor != continuation {
			failures <- "continuation cursor was not preserved"
		}
		if call <= searchHistoryStartupMaxBatches {
			if maximumRows != searchHistoryMaintenanceBatchSize {
				failures <- "startup batch size was not bounded"
			}
			return searchhistory.MaintenancePruneResult{
				Deleted: 1,
				More:    true,
				Cursor:  continuation,
			}, nil
		}
		if maximumRows != 1 {
			failures <- "immediate worker batch size was not configured"
		}
		continued <- struct{}{}
		return searchhistory.MaintenancePruneResult{}, nil
	})

	startup, err := pruneSearchHistoryAtStartup(context.Background(), pruner)
	if err != nil {
		t.Fatal(err)
	}
	if startup.deleted != searchHistoryStartupMaxBatches ||
		!startup.more ||
		startup.cursor != continuation ||
		calls.Load() != searchHistoryStartupMaxBatches {
		t.Fatalf(
			"startup result = deleted:%d more:%t cursor:%p calls:%d",
			startup.deleted,
			startup.more,
			startup.cursor,
			calls.Load(),
		)
	}

	ticks := make(chan time.Time)
	maintenance, err := newSearchHistoryMaintenance(
		pruner,
		searchHistoryMaintenanceConfig{
			interval:       time.Hour,
			pruneTimeout:   time.Second,
			batchSize:      1,
			maximumBatches: 1,
			initialCursor:  startup.cursor,
			runImmediately: startup.more,
			ticks:          ticks,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := maintenance.Close(ctx); err != nil {
			t.Error(err)
		}
	})

	awaitSearchHistorySignal(t, continued, "immediate startup continuation")
	select {
	case failure := <-failures:
		t.Fatal(failure)
	default:
	}
	if calls.Load() != searchHistoryStartupMaxBatches+1 {
		t.Fatalf(
			"calls after immediate continuation = %d, want %d",
			calls.Load(),
			searchHistoryStartupMaxBatches+1,
		)
	}
}

func TestSearchHistoryMaintenanceBoundsWorkPerTick(t *testing.T) {
	t.Parallel()

	var calls atomic.Int64
	var observedMaximumRows atomic.Int64
	called := make(chan int64, 3)
	pruner := searchHistoryPrunerFunc(func(
		_ context.Context,
		maximumRows int,
		_ *searchhistory.MaintenanceCursor,
	) (searchhistory.MaintenancePruneResult, error) {
		observedMaximumRows.Store(int64(maximumRows))
		call := calls.Add(1)
		called <- call
		return searchhistory.MaintenancePruneResult{
			Deleted: 3,
			More:    true,
		}, nil
	})
	ticks := make(chan time.Time, 1)
	maintenance, err := newSearchHistoryMaintenance(
		pruner,
		searchHistoryMaintenanceConfig{
			interval:       time.Hour,
			pruneTimeout:   time.Second,
			batchSize:      3,
			maximumBatches: 2,
			backlogDelay:   time.Hour,
			ticks:          ticks,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := maintenance.Close(ctx); err != nil {
			t.Error(err)
		}
	})

	ticks <- time.Now()
	for want := int64(1); want <= 2; want++ {
		if got := awaitSearchHistorySignal(t, called, "bounded periodic prune"); got != want {
			t.Fatalf("periodic call = %d, want %d", got, want)
		}
	}
	select {
	case call := <-called:
		t.Fatalf("periodic work exceeded its two-batch budget at call %d", call)
	case <-time.After(25 * time.Millisecond):
	}
	if calls.Load() != 2 {
		t.Fatalf("periodic calls = %d, want 2", calls.Load())
	}
	if observedMaximumRows.Load() != 3 {
		t.Fatalf(
			"periodic maximum rows = %d, want 3",
			observedMaximumRows.Load(),
		)
	}
}

func TestSearchHistoryMaintenancePromptlyContinuesReportedBacklog(t *testing.T) {
	t.Parallel()

	var calls atomic.Int64
	called := make(chan int64, 4)
	pruner := searchHistoryPrunerFunc(func(
		_ context.Context,
		_ int,
		_ *searchhistory.MaintenanceCursor,
	) (searchhistory.MaintenancePruneResult, error) {
		call := calls.Add(1)
		called <- call
		return searchhistory.MaintenancePruneResult{
			Deleted: 1,
			More:    call < 3,
		}, nil
	})
	ticks := make(chan time.Time, 1)
	maintenance, err := newSearchHistoryMaintenance(
		pruner,
		searchHistoryMaintenanceConfig{
			interval:       time.Hour,
			pruneTimeout:   time.Second,
			batchSize:      1,
			maximumBatches: 1,
			backlogDelay:   time.Millisecond,
			ticks:          ticks,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := maintenance.Close(ctx); err != nil {
			t.Error(err)
		}
	})

	ticks <- time.Now()
	for want := int64(1); want <= 3; want++ {
		if call := awaitSearchHistorySignal(
			t,
			called,
			"prompt backlog continuation",
		); call != want {
			t.Fatalf("backlog call = %d, want %d", call, want)
		}
	}
	select {
	case call := <-called:
		t.Fatalf("maintenance continued after backlog drained at call %d", call)
	case <-time.After(25 * time.Millisecond):
	}
}

func TestSearchHistoryMaintenanceBlockedErrorCallbackDoesNotStallRetryOrClose(t *testing.T) {
	t.Parallel()

	pruneErr := errors.New("blocked-callback prune failure")
	var pruneCalls atomic.Int64
	pruned := make(chan int64, 4)
	pruner := searchHistoryPrunerFunc(func(
		context.Context,
		int,
		*searchhistory.MaintenanceCursor,
	) (searchhistory.MaintenancePruneResult, error) {
		call := pruneCalls.Add(1)
		pruned <- call
		return searchhistory.MaintenancePruneResult{}, pruneErr
	})

	callbackEntered := make(chan struct{}, 1)
	callbackReturned := make(chan struct{}, 1)
	releaseCallback := make(chan struct{})
	var releaseOnce sync.Once
	release := func() {
		releaseOnce.Do(func() { close(releaseCallback) })
	}
	defer release()
	var callbackCalls atomic.Int64
	var activeCallbacks atomic.Int64
	var maximumActiveCallbacks atomic.Int64

	ticks := make(chan time.Time, 4)
	maintenance, err := newSearchHistoryMaintenance(
		pruner,
		searchHistoryMaintenanceConfig{
			interval:     time.Hour,
			pruneTimeout: time.Second,
			ticks:        ticks,
			onError: func(err error) {
				if !errors.Is(err, pruneErr) {
					return
				}
				callbackCalls.Add(1)
				current := activeCallbacks.Add(1)
				raiseSearchHistoryMaximum(&maximumActiveCallbacks, current)
				callbackEntered <- struct{}{}
				<-releaseCallback
				activeCallbacks.Add(-1)
				callbackReturned <- struct{}{}
			},
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		release()
		closeCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := maintenance.Close(closeCtx); err != nil {
			t.Error(err)
		}
	})

	ticks <- time.Now()
	if call := awaitSearchHistorySignal(t, pruned, "first failed prune"); call != 1 {
		t.Fatalf("first prune call = %d, want 1", call)
	}
	awaitSearchHistorySignal(t, callbackEntered, "blocked error callback")

	ticks <- time.Now()
	ticks <- time.Now()
	for want := int64(2); want <= 3; want++ {
		if call := awaitSearchHistorySignal(t, pruned, "retry while callback is blocked"); call != want {
			t.Fatalf("retry prune call = %d, want %d", call, want)
		}
	}

	closeCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := maintenance.Close(closeCtx); err != nil {
		t.Fatalf("Close() while error callback is blocked = %v", err)
	}
	if got := callbackCalls.Load(); got != 1 {
		t.Fatalf("blocked callback calls = %d, want 1", got)
	}
	if got := maximumActiveCallbacks.Load(); got != 1 {
		t.Fatalf("maximum concurrent callbacks = %d, want 1", got)
	}

	release()
	awaitSearchHistorySignal(t, callbackReturned, "blocked callback return")
}

func TestSearchHistoryMaintenanceErrorCallbackPanicIsContainedAndRecovers(t *testing.T) {
	t.Parallel()

	pruneErr := errors.New("panic-callback prune failure")
	var pruneCalls atomic.Int64
	pruner := searchHistoryPrunerFunc(func(
		context.Context,
		int,
		*searchhistory.MaintenanceCursor,
	) (searchhistory.MaintenancePruneResult, error) {
		pruneCalls.Add(1)
		return searchhistory.MaintenancePruneResult{}, pruneErr
	})

	firstCallback := make(chan struct{})
	laterCallback := make(chan int64, 1)
	callbackReturned := make(chan int64, 16)
	var callbackCalls atomic.Int64
	var activeCallbacks atomic.Int64
	var maximumActiveCallbacks atomic.Int64
	ticks := make(chan time.Time, 1)
	maintenance, err := newSearchHistoryMaintenance(
		pruner,
		searchHistoryMaintenanceConfig{
			interval:     time.Hour,
			pruneTimeout: time.Second,
			ticks:        ticks,
			onError: func(err error) {
				if !errors.Is(err, pruneErr) {
					return
				}
				call := callbackCalls.Add(1)
				current := activeCallbacks.Add(1)
				raiseSearchHistoryMaximum(&maximumActiveCallbacks, current)
				defer func() {
					activeCallbacks.Add(-1)
					callbackReturned <- call
				}()
				if call == 1 {
					close(firstCallback)
					panic("search-history maintenance callback panic")
				}
				select {
				case laterCallback <- call:
				default:
				}
			},
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := maintenance.Close(closeCtx); err != nil {
			t.Error(err)
		}
	})

	ticks <- time.Now()
	awaitSearchHistorySignal(t, firstCallback, "panicking error callback")
	if call := awaitSearchHistorySignal(t, callbackReturned, "panicking callback unwind"); call != 1 {
		t.Fatalf("first returned callback = %d, want 1", call)
	}

	retryTicker := time.NewTicker(time.Millisecond)
	defer retryTicker.Stop()
	retryDeadline := time.NewTimer(5 * time.Second)
	defer retryDeadline.Stop()
	var recoveredCall int64
	for recoveredCall == 0 {
		select {
		case recoveredCall = <-laterCallback:
		case <-retryTicker.C:
			select {
			case ticks <- time.Now():
			default:
			}
		case <-retryDeadline.C:
			t.Fatal("maintenance did not recover after error callback panic")
		}
	}
	if recoveredCall < 2 {
		t.Fatalf("recovered callback = %d, want at least 2", recoveredCall)
	}
	awaitSearchHistorySignal(t, callbackReturned, "post-panic callback return")

	closeCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := maintenance.Close(closeCtx); err != nil {
		t.Fatalf("Close() after callback panic = %v", err)
	}
	if got := pruneCalls.Load(); got < 2 {
		t.Fatalf("prune calls after callback panic = %d, want at least 2", got)
	}
	if got := maximumActiveCallbacks.Load(); got != 1 {
		t.Fatalf("maximum concurrent callbacks = %d, want 1", got)
	}
}

func TestSearchHistoryMaintenanceCloseCancelsDrainsAndIsIdempotent(t *testing.T) {
	t.Parallel()

	entered := make(chan struct{}, 1)
	returned := make(chan struct{}, 1)
	var calls atomic.Int64
	pruner := searchHistoryPrunerFunc(func(
		ctx context.Context,
		_ int,
		_ *searchhistory.MaintenanceCursor,
	) (searchhistory.MaintenancePruneResult, error) {
		calls.Add(1)
		entered <- struct{}{}
		<-ctx.Done()
		returned <- struct{}{}
		return searchhistory.MaintenancePruneResult{}, ctx.Err()
	})
	ticks := make(chan time.Time, 2)
	reported := make(chan error, 1)
	maintenance, err := newSearchHistoryMaintenance(
		pruner,
		searchHistoryMaintenanceConfig{
			interval:     time.Hour,
			pruneTimeout: time.Minute,
			ticks:        ticks,
			onError: func(err error) {
				reported <- err
			},
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	ticks <- time.Now()
	awaitSearchHistorySignal(t, entered, "in-flight prune")

	const closers = 8
	closeResults := make(chan error, closers)
	for range closers {
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			closeResults <- maintenance.Close(ctx)
		}()
	}
	for range closers {
		if err := awaitSearchHistorySignal(t, closeResults, "maintenance close"); err != nil {
			t.Fatal(err)
		}
	}
	awaitSearchHistorySignal(t, returned, "canceled prune return")
	select {
	case err := <-reported:
		t.Fatalf("shutdown cancellation was reported as maintenance failure: %v", err)
	default:
	}

	ticks <- time.Now()
	select {
	case <-entered:
		t.Fatal("a prune started after maintenance closed")
	case <-time.After(25 * time.Millisecond):
	}
	if calls.Load() != 1 {
		t.Fatalf("prune calls after close = %d, want 1", calls.Load())
	}
}

func raiseSearchHistoryMaximum(maximum *atomic.Int64, candidate int64) {
	for {
		current := maximum.Load()
		if candidate <= current || maximum.CompareAndSwap(current, candidate) {
			return
		}
	}
}

func awaitSearchHistorySignal[T any](
	t *testing.T,
	channel <-chan T,
	description string,
) T {
	t.Helper()
	select {
	case value := <-channel:
		return value
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for %s", description)
		var zero T
		return zero
	}
}
