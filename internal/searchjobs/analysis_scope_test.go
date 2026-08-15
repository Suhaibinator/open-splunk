package searchjobs

import (
	"context"
	"errors"
	"reflect"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/searchtime"
)

type nilAnalysisSnapshotter struct{}

func (*nilAnalysisSnapshotter) VisibilityCutoff(context.Context) (uint64, error) {
	panic("typed-nil snapshotter must be rejected")
}

type nilAnalysisExecutor struct{}

func (*nilAnalysisExecutor) Execute(context.Context, clickhouse.CompiledQuery, ResultSink) error {
	panic("typed-nil executor must be rejected")
}

func TestNewRejectsTypedNilRequiredAnalysisDependencies(t *testing.T) {
	t.Parallel()

	var snapshotter *nilAnalysisSnapshotter
	if _, err := New(Config{
		Executor:        executorFunc(func(context.Context, clickhouse.CompiledQuery, ResultSink) error { return nil }),
		Snapshotter:     snapshotter,
		CleanupInterval: -1,
		CursorKey:       testCursorKey,
	}); err == nil {
		t.Fatal("New() accepted a typed-nil snapshotter")
	}

	var executor *nilAnalysisExecutor
	if _, err := New(Config{
		Executor:        executor,
		Snapshotter:     snapshotterFunc(func(context.Context) (uint64, error) { return 0, nil }),
		CleanupInterval: -1,
		CursorKey:       testCursorKey,
	}); err == nil {
		t.Fatal("New() accepted a typed-nil executor")
	}
}

func TestSnapshotAnalysisScopeCapturesOneImmutableScopeWithoutJobSideEffects(t *testing.T) {
	t.Parallel()

	var (
		executorCalls   atomic.Int32
		snapshotCalls   atomic.Int32
		journalCalls    atomic.Int32
		idCalls         atomic.Int32
		managerNowCalls atomic.Int32
	)
	anchor := time.Date(2026, time.July, 27, 10, 11, 12, 123_456_789, time.FixedZone("test", -7*60*60))
	manager := newTestManager(t, Config{
		Executor: executorFunc(func(context.Context, clickhouse.CompiledQuery, ResultSink) error {
			executorCalls.Add(1)
			return errors.New("analysis scope must not execute SQL")
		}),
		Snapshotter: snapshotterFunc(func(context.Context) (uint64, error) {
			snapshotCalls.Add(1)
			return 73, nil
		}),
		Journal: jobJournalFunc{
			admit: func(context.Context, Job) error {
				journalCalls.Add(1)
				return errors.New("analysis scope must not admit a job")
			},
			finalize: func(context.Context, Job) error {
				journalCalls.Add(1)
				return errors.New("analysis scope must not finalize a job")
			},
		},
		Now: func() time.Time {
			call := managerNowCalls.Add(1)
			return anchor.Add(time.Duration(call-1) * time.Hour)
		},
		NewID: func() string {
			idCalls.Add(1)
			return "analysis-scope-must-not-create-an-id"
		},
		CleanupInterval: -1,
	})

	timezone := "America/Los_Angeles"
	resolvedRange, err := searchtime.Resolve("-2h", "now", &timezone, anchor)
	if err != nil {
		t.Fatal(err)
	}
	request := AnalysisScopeRequest{
		TenantID:          "tenant-1",
		AuthorizedIndexes: []string{"beta", "alpha"},
		RequestedIndexes:  []string{"alpha", "alpha"},
		TimeRange:         resolvedRange,
	}
	snapshot, err := manager.SnapshotAnalysisScope(context.Background(), request)
	if err != nil {
		t.Fatalf("SnapshotAnalysisScope() error = %v", err)
	}
	if snapshot.TenantID != request.TenantID ||
		!slices.Equal(snapshot.AuthorizedIndexes, request.AuthorizedIndexes) ||
		!slices.Equal(snapshot.RequestedIndexes, request.RequestedIndexes) {
		t.Fatalf("snapshot scope = %#v, want request scope %#v", snapshot, request)
	}
	if !snapshot.TimeRange.Valid() ||
		!snapshot.TimeRange.Earliest().Equal(resolvedRange.Earliest()) ||
		!snapshot.TimeRange.Latest().Equal(resolvedRange.Latest()) ||
		snapshot.TimeRange.Intent() != resolvedRange.Intent() {
		t.Fatalf("snapshot time range = %#v, want %#v", snapshot.TimeRange, resolvedRange)
	}
	wantAnchor := anchor.Round(0).UTC()
	if !snapshot.SearchStart.Equal(wantAnchor) ||
		!snapshot.IndexTimeCutoff.Equal(wantAnchor) ||
		snapshot.VisibilityCutoff != 73 {
		t.Fatalf(
			"snapshot anchors = (%v, %v, %d), want (%v, %v, 73)",
			snapshot.SearchStart,
			snapshot.IndexTimeCutoff,
			snapshot.VisibilityCutoff,
			wantAnchor,
			wantAnchor,
		)
	}

	request.TenantID = "mutated"
	request.AuthorizedIndexes[0] = "mutated"
	request.RequestedIndexes[0] = "mutated"
	request.TimeRange = searchtime.Range{}
	if snapshot.TenantID != "tenant-1" ||
		!slices.Equal(snapshot.AuthorizedIndexes, []string{"beta", "alpha"}) ||
		!slices.Equal(snapshot.RequestedIndexes, []string{"alpha", "alpha"}) ||
		!snapshot.TimeRange.Valid() {
		t.Fatalf("snapshot retained caller-owned request storage: %#v", snapshot)
	}

	if calls := executorCalls.Load(); calls != 0 {
		t.Fatalf("executor calls = %d, want 0", calls)
	}
	if calls := snapshotCalls.Load(); calls != 1 {
		t.Fatalf("snapshotter calls = %d, want 1", calls)
	}
	if calls := journalCalls.Load(); calls != 0 {
		t.Fatalf("journal calls = %d, want 0", calls)
	}
	if calls := idCalls.Load(); calls != 0 {
		t.Fatalf("ID generator calls = %d, want 0", calls)
	}
	if calls := managerNowCalls.Load(); calls != 1 {
		t.Fatalf("manager clock calls = %d, want 1", calls)
	}
	assertAnalysisScopeManagerStateEmpty(t, manager)
}

func TestSnapshotAnalysisScopeAllowsAllAuthorizedIndexesAndZeroVisibility(t *testing.T) {
	t.Parallel()

	manager := newTestManager(t, Config{
		Executor: executorFunc(func(context.Context, clickhouse.CompiledQuery, ResultSink) error {
			return nil
		}),
		Snapshotter:     snapshotterFunc(func(context.Context) (uint64, error) { return 0, nil }),
		CleanupInterval: -1,
	})
	request := validAnalysisScopeRequest()
	request.AuthorizedIndexes = []string{"main", "internal"}
	request.RequestedIndexes = nil
	snapshot, err := manager.SnapshotAnalysisScope(context.Background(), request)
	if err != nil {
		t.Fatalf("SnapshotAnalysisScope() error = %v", err)
	}
	if !slices.Equal(snapshot.AuthorizedIndexes, request.AuthorizedIndexes) ||
		len(snapshot.RequestedIndexes) != 0 ||
		snapshot.VisibilityCutoff != 0 {
		t.Fatalf("SnapshotAnalysisScope() = %#v, want all authorized indexes at empty-table cutoff", snapshot)
	}
	assertAnalysisScopeManagerStateEmpty(t, manager)
}

func TestSnapshotAnalysisScopeRejectsStructuralScopeAndSizeErrorsBeforeAdmission(t *testing.T) {
	t.Parallel()

	var snapshotCalls atomic.Int32
	manager := newTestManager(t, Config{
		Executor: executorFunc(func(context.Context, clickhouse.CompiledQuery, ResultSink) error {
			return nil
		}),
		Snapshotter: snapshotterFunc(func(context.Context) (uint64, error) {
			snapshotCalls.Add(1)
			return 1, nil
		}),
		MaxScopeIndexes: 2,
		CleanupInterval: -1,
	})
	valid := AnalysisScopeRequest{
		TenantID:          "tenant",
		AuthorizedIndexes: []string{"main"},
		RequestedIndexes:  []string{"main"},
		TimeRange: mustAbsoluteTimeRange(
			time.Date(2026, time.July, 26, 0, 0, 0, 0, time.UTC),
			time.Date(2026, time.July, 27, 0, 0, 0, 0, time.UTC),
		),
	}
	invalidUTF8 := string([]byte{0xff})
	tests := []struct {
		name         string
		mutate       func(*AnalysisScopeRequest)
		wantTooLarge bool
	}{
		{name: "missing time range", mutate: func(request *AnalysisScopeRequest) {
			request.TimeRange = searchtime.Range{}
		}},
		{name: "empty tenant", mutate: func(request *AnalysisScopeRequest) {
			request.TenantID = ""
		}},
		{name: "padded tenant", mutate: func(request *AnalysisScopeRequest) {
			request.TenantID = " tenant "
		}},
		{name: "control tenant", mutate: func(request *AnalysisScopeRequest) {
			request.TenantID = "ten\nant"
		}},
		{name: "invalid UTF-8 tenant", mutate: func(request *AnalysisScopeRequest) {
			request.TenantID = invalidUTF8
		}},
		{name: "oversized tenant", mutate: func(request *AnalysisScopeRequest) {
			request.TenantID = strings.Repeat("t", defaultMaxIdentityBytes+1)
		}, wantTooLarge: true},
		{name: "no authorized indexes", mutate: func(request *AnalysisScopeRequest) {
			request.AuthorizedIndexes = nil
		}},
		{name: "requested index outside authorization", mutate: func(request *AnalysisScopeRequest) {
			request.RequestedIndexes = []string{"secret"}
		}},
		{name: "padded authorized index", mutate: func(request *AnalysisScopeRequest) {
			request.AuthorizedIndexes = []string{" main "}
		}},
		{name: "invalid UTF-8 requested index", mutate: func(request *AnalysisScopeRequest) {
			request.RequestedIndexes = []string{invalidUTF8}
		}},
		{name: "oversized index", mutate: func(request *AnalysisScopeRequest) {
			request.AuthorizedIndexes = []string{strings.Repeat("i", defaultMaxIdentityBytes+1)}
			request.RequestedIndexes = nil
		}, wantTooLarge: true},
		{name: "combined scope count", mutate: func(request *AnalysisScopeRequest) {
			request.AuthorizedIndexes = []string{"main", "internal"}
			request.RequestedIndexes = []string{"main"}
		}, wantTooLarge: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			request := valid
			test.mutate(&request)
			got, err := manager.SnapshotAnalysisScope(context.Background(), request)
			if err == nil {
				t.Fatalf("SnapshotAnalysisScope() = %#v, nil; want error", got)
			}
			if !reflect.DeepEqual(got, AnalysisScopeSnapshot{}) {
				t.Fatalf("SnapshotAnalysisScope() partial result = %#v", got)
			}
			if test.wantTooLarge && !errors.Is(err, ErrRequestTooLarge) {
				t.Fatalf("SnapshotAnalysisScope() error = %v, want ErrRequestTooLarge", err)
			}
		})
	}
	if calls := snapshotCalls.Load(); calls != 0 {
		t.Fatalf("snapshotter calls = %d, want 0", calls)
	}
	assertAnalysisScopeManagerStateEmpty(t, manager)
}

func TestSnapshotAnalysisScopeNilManagerAndContextFailAtomically(t *testing.T) {
	t.Parallel()

	request := validAnalysisScopeRequest()
	var manager *Manager
	if got, err := manager.SnapshotAnalysisScope(context.Background(), request); err == nil ||
		!reflect.DeepEqual(got, AnalysisScopeSnapshot{}) {
		t.Fatalf("nil manager SnapshotAnalysisScope() = (%#v, %v), want zero result and error", got, err)
	}

	live := newTestManager(t, Config{
		Executor:        executorFunc(func(context.Context, clickhouse.CompiledQuery, ResultSink) error { return nil }),
		CleanupInterval: -1,
	})
	var nilContext context.Context
	if got, err := live.SnapshotAnalysisScope(nilContext, request); err == nil ||
		!reflect.DeepEqual(got, AnalysisScopeSnapshot{}) {
		t.Fatalf("nil context SnapshotAnalysisScope() = (%#v, %v), want zero result and error", got, err)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if got, err := live.SnapshotAnalysisScope(canceled, request); !errors.Is(err, context.Canceled) ||
		!reflect.DeepEqual(got, AnalysisScopeSnapshot{}) {
		t.Fatalf("canceled SnapshotAnalysisScope() = (%#v, %v), want context.Canceled", got, err)
	}
	assertAnalysisScopeManagerStateEmpty(t, live)
}

func TestSnapshotAnalysisScopeStorageFailureTimeoutAndCancellationAreAtomic(t *testing.T) {
	t.Parallel()

	t.Run("storage failure", func(t *testing.T) {
		t.Parallel()
		var clockCalls atomic.Int32
		manager := newTestManager(t, Config{
			Executor: executorFunc(func(context.Context, clickhouse.CompiledQuery, ResultSink) error {
				return nil
			}),
			Snapshotter: snapshotterFunc(func(context.Context) (uint64, error) {
				return 0, errors.New("private storage failure")
			}),
			Now: func() time.Time {
				clockCalls.Add(1)
				return time.Now()
			},
			CleanupInterval: -1,
		})
		got, err := manager.SnapshotAnalysisScope(context.Background(), validAnalysisScopeRequest())
		if !errors.Is(err, ErrStorageUnavailable) || !reflect.DeepEqual(got, AnalysisScopeSnapshot{}) {
			t.Fatalf("SnapshotAnalysisScope() = (%#v, %v), want zero and ErrStorageUnavailable", got, err)
		}
		if calls := clockCalls.Load(); calls != 0 {
			t.Fatalf("clock calls = %d, want 0 after storage failure", calls)
		}
		assertAnalysisScopeManagerStateEmpty(t, manager)
	})

	t.Run("snapshot timeout", func(t *testing.T) {
		t.Parallel()
		manager := newTestManager(t, Config{
			Executor: executorFunc(func(context.Context, clickhouse.CompiledQuery, ResultSink) error {
				return nil
			}),
			Snapshotter: snapshotterFunc(func(ctx context.Context) (uint64, error) {
				<-ctx.Done()
				return 0, ctx.Err()
			}),
			SnapshotTimeout: 20 * time.Millisecond,
			CleanupInterval: -1,
		})
		got, err := manager.SnapshotAnalysisScope(context.Background(), validAnalysisScopeRequest())
		if !errors.Is(err, ErrStorageUnavailable) || !reflect.DeepEqual(got, AnalysisScopeSnapshot{}) {
			t.Fatalf("SnapshotAnalysisScope() = (%#v, %v), want zero and ErrStorageUnavailable", got, err)
		}
		assertAnalysisScopeManagerStateEmpty(t, manager)
	})

	t.Run("caller cancellation", func(t *testing.T) {
		t.Parallel()
		started := make(chan struct{})
		manager := newTestManager(t, Config{
			Executor: executorFunc(func(context.Context, clickhouse.CompiledQuery, ResultSink) error {
				return nil
			}),
			Snapshotter: snapshotterFunc(func(ctx context.Context) (uint64, error) {
				close(started)
				<-ctx.Done()
				return 0, ctx.Err()
			}),
			CleanupInterval: -1,
		})
		ctx, cancel := context.WithCancel(context.Background())
		result := make(chan struct {
			snapshot AnalysisScopeSnapshot
			err      error
		}, 1)
		go func() {
			snapshot, err := manager.SnapshotAnalysisScope(ctx, validAnalysisScopeRequest())
			result <- struct {
				snapshot AnalysisScopeSnapshot
				err      error
			}{snapshot: snapshot, err: err}
		}()
		<-started
		cancel()
		outcome := <-result
		if !errors.Is(outcome.err, context.Canceled) ||
			!reflect.DeepEqual(outcome.snapshot, AnalysisScopeSnapshot{}) {
			t.Fatalf("SnapshotAnalysisScope() = (%#v, %v), want zero and context.Canceled", outcome.snapshot, outcome.err)
		}
		assertAnalysisScopeManagerStateEmpty(t, manager)
	})
}

func TestSnapshotAnalysisScopeUsesFailFastSynchronousCapacity(t *testing.T) {
	t.Parallel()

	started := make(chan struct{})
	release := make(chan struct{})
	var snapshotCalls atomic.Int32
	manager := newTestManager(t, Config{
		Executor: executorFunc(func(context.Context, clickhouse.CompiledQuery, ResultSink) error {
			return nil
		}),
		Snapshotter: snapshotterFunc(func(ctx context.Context) (uint64, error) {
			snapshotCalls.Add(1)
			close(started)
			select {
			case <-ctx.Done():
				return 0, ctx.Err()
			case <-release:
				return 9, nil
			}
		}),
		MaxConcurrent:   1,
		CleanupInterval: -1,
	})
	first := make(chan error, 1)
	go func() {
		_, err := manager.SnapshotAnalysisScope(context.Background(), validAnalysisScopeRequest())
		first <- err
	}()
	<-started

	second, err := manager.SnapshotAnalysisScope(context.Background(), validAnalysisScopeRequest())
	if !errors.Is(err, ErrCapacity) || !reflect.DeepEqual(second, AnalysisScopeSnapshot{}) {
		t.Fatalf("second SnapshotAnalysisScope() = (%#v, %v), want zero and ErrCapacity", second, err)
	}
	if calls := snapshotCalls.Load(); calls != 1 {
		t.Fatalf("snapshotter calls before release = %d, want 1", calls)
	}
	close(release)
	if err := <-first; err != nil {
		t.Fatalf("first SnapshotAnalysisScope() error = %v", err)
	}
	assertAnalysisScopeManagerStateEmpty(t, manager)
}

func TestSnapshotAnalysisScopeCloseCancelsAndWaitsForActiveSnapshot(t *testing.T) {
	t.Parallel()

	started := make(chan struct{})
	manager, err := New(Config{
		Executor: executorFunc(func(context.Context, clickhouse.CompiledQuery, ResultSink) error {
			return nil
		}),
		Snapshotter: snapshotterFunc(func(ctx context.Context) (uint64, error) {
			close(started)
			<-ctx.Done()
			return 0, ctx.Err()
		}),
		CleanupInterval: -1,
		CursorKey:       testCursorKey,
	})
	if err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	go func() {
		snapshot, snapshotErr := manager.SnapshotAnalysisScope(context.Background(), validAnalysisScopeRequest())
		if !reflect.DeepEqual(snapshot, AnalysisScopeSnapshot{}) {
			result <- errors.New("active scope snapshot returned partial state")
			return
		}
		result <- snapshotErr
	}()
	<-started

	closed := make(chan error, 1)
	go func() { closed <- manager.Close() }()
	select {
	case snapshotErr := <-result:
		if !errors.Is(snapshotErr, ErrClosed) {
			t.Fatalf("SnapshotAnalysisScope() error = %v, want ErrClosed", snapshotErr)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("SnapshotAnalysisScope did not observe manager shutdown")
	}
	select {
	case closeErr := <-closed:
		if closeErr != nil {
			t.Fatalf("Close() error = %v", closeErr)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Close did not wait for and finish active scope snapshot")
	}
	if got, snapshotErr := manager.SnapshotAnalysisScope(context.Background(), validAnalysisScopeRequest()); !errors.Is(snapshotErr, ErrClosed) ||
		!reflect.DeepEqual(got, AnalysisScopeSnapshot{}) {
		t.Fatalf("post-close SnapshotAnalysisScope() = (%#v, %v), want zero and ErrClosed", got, snapshotErr)
	}
	assertAnalysisScopeManagerStateEmpty(t, manager)
}

func validAnalysisScopeRequest() AnalysisScopeRequest {
	return AnalysisScopeRequest{
		TenantID:          "tenant",
		AuthorizedIndexes: []string{"main"},
		RequestedIndexes:  []string{"main"},
		TimeRange: mustAbsoluteTimeRange(
			time.Date(2026, time.July, 26, 0, 0, 0, 0, time.UTC),
			time.Date(2026, time.July, 27, 0, 0, 0, 0, time.UTC),
		),
	}
}

func assertAnalysisScopeManagerStateEmpty(t *testing.T, manager *Manager) {
	t.Helper()
	if jobs := manager.List(); len(jobs) != 0 {
		t.Fatalf("analysis scope retained jobs/history = %+v, want none", jobs)
	}
	manager.mu.RLock()
	retainedJobs, queued, activeOperations, pendingAdmissions :=
		len(manager.jobs), manager.queueCount, manager.activeOperations, manager.pendingAdmissions
	manager.mu.RUnlock()
	manager.budgetMu.Lock()
	metadataBytes, retainedBytes := manager.metadataBytes, manager.retainedBytes
	manager.budgetMu.Unlock()
	if retainedJobs != 0 || queued != 0 || activeOperations != 0 || pendingAdmissions != 0 ||
		metadataBytes != 0 || retainedBytes != 0 {
		t.Fatalf(
			"analysis scope changed manager state: jobs=%d queued=%d active=%d pending=%d metadata=%d retained=%d",
			retainedJobs,
			queued,
			activeOperations,
			pendingAdmissions,
			metadataBytes,
			retainedBytes,
		)
	}
}
