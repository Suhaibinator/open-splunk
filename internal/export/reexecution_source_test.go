package export

import (
	"context"
	"errors"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
)

type reexecutionTestSearches struct {
	mu           sync.Mutex
	job          searchjobs.Job
	pin          *reexecutionTestPin
	acquireErr   error
	getErr       error
	acquireCalls int
	getCalls     int
	access       searchjobs.AccessScope
	id           string
	onGet        func()
}

func (searches *reexecutionTestSearches) AcquireResultsFor(_ context.Context, access searchjobs.AccessScope, id string) (searchjobs.ResultLease, error) {
	searches.mu.Lock()
	defer searches.mu.Unlock()
	searches.acquireCalls++
	searches.access = access
	searches.id = id
	if searches.acquireErr != nil {
		return nil, searches.acquireErr
	}
	return searches.pin, nil
}

func (searches *reexecutionTestSearches) GetFor(access searchjobs.AccessScope, id string) (searchjobs.Job, error) {
	searches.mu.Lock()
	defer searches.mu.Unlock()
	searches.getCalls++
	searches.access = access
	searches.id = id
	if searches.onGet != nil {
		searches.onGet()
	}
	return searches.job, searches.getErr
}

type reexecutionTestPin struct {
	schema     searchjobs.Schema
	generation uint64
	truncated  bool
	closed     atomic.Int32
}

func (pin *reexecutionTestPin) Schema() searchjobs.Schema {
	return cloneResultSchema(pin.schema)
}

func (*reexecutionTestPin) RowCount() uint64 { return 1 }

func (*reexecutionTestPin) RowCountExact() bool { return true }

func (pin *reexecutionTestPin) Generation() uint64 { return pin.generation }

func (pin *reexecutionTestPin) ResultsTruncated() bool { return pin.truncated }

func (*reexecutionTestPin) Next(context.Context) (searchjobs.ResultRow, bool, error) {
	return searchjobs.ResultRow{}, false, nil
}

func (pin *reexecutionTestPin) Close() error {
	pin.closed.Add(1)
	return nil
}

type reexecutionTestExecutor func(context.Context, clickhouse.CompiledQuery, searchjobs.ResultSink) error

func (executor reexecutionTestExecutor) Execute(ctx context.Context, query clickhouse.CompiledQuery, sink searchjobs.ResultSink) error {
	return executor(ctx, query, sink)
}

func TestReexecutionSourceIsLazyScopedAndStreamsBeyondRetainedPreview(t *testing.T) {
	t.Parallel()
	searches, schema, access := newReexecutionTestSearches()
	searches.pin.truncated = true
	var calls atomic.Int32
	var captured clickhouse.CompiledQuery
	executor := reexecutionTestExecutor(func(_ context.Context, query clickhouse.CompiledQuery, sink searchjobs.ResultSink) error {
		calls.Add(1)
		captured = query
		if err := sink.SetSchema(schema); err != nil {
			return err
		}
		for _, value := range []int64{200, 201, 202} {
			if err := sink.AddRow([]searchjobs.Value{searchjobs.SignedValue(value)}); err != nil {
				return err
			}
		}
		return nil
	})
	source := newReexecutionTestSource(t, searches, executor, nil)
	lease, err := source.AcquireResultsFor(context.Background(), access, searches.job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 0 {
		t.Fatal("query executed during admission instead of inside an export worker")
	}
	if searches.pin.closed.Load() != 1 {
		t.Fatalf("source pin closes after descriptor copy = %d, want 1", searches.pin.closed.Load())
	}
	if lease.RowCount() != 0 || lease.Generation() == 0 || lease.ResultsTruncated() {
		t.Fatalf("lease metadata = rows %d generation %d truncated %t", lease.RowCount(), lease.Generation(), lease.ResultsTruncated())
	}
	if !equalResultSchemas(lease.Schema(), schema) {
		t.Fatalf("lease schema = %#v, want %#v", lease.Schema(), schema)
	}

	var values []int64
	for {
		row, ok, nextErr := lease.Next(context.Background())
		if nextErr != nil {
			t.Fatal(nextErr)
		}
		if !ok {
			break
		}
		value, ok := row.Values[0].Signed()
		if !ok {
			t.Fatalf("row value = %#v", row.Values[0])
		}
		values = append(values, value)
	}
	if !slices.Equal(values, []int64{200, 201, 202}) {
		t.Fatalf("streamed values = %v", values)
	}
	if calls.Load() != 1 {
		t.Fatalf("executor calls = %d, want 1", calls.Load())
	}
	if !strings.Contains(captured.SQL, `"visibility_seq" <= ?`) || len(captured.Args) == 0 {
		t.Fatalf("compiled query did not retain snapshot predicates: %s / %#v", captured.SQL, captured.Args)
	}
	if err := lease.Close(); err != nil {
		t.Fatal(err)
	}
	if searches.pin.closed.Load() != 1 {
		t.Fatalf("source pin closes after lease close = %d, want 1", searches.pin.closed.Load())
	}
	searches.mu.Lock()
	defer searches.mu.Unlock()
	if searches.acquireCalls != 1 || searches.getCalls != 1 || searches.access != access || searches.id != searches.job.ID {
		t.Fatalf("snapshot calls = acquire %d get %d scope %+v id %q", searches.acquireCalls, searches.getCalls, searches.access, searches.id)
	}
}

func TestReexecutionSourceUsesDistinctExecutionGenerations(t *testing.T) {
	t.Parallel()
	searches, _, access := newReexecutionTestSearches()
	source := newReexecutionTestSource(t, searches, reexecutionTestExecutor(func(context.Context, clickhouse.CompiledQuery, searchjobs.ResultSink) error {
		return nil
	}), nil)
	first, err := source.AcquireResultsFor(context.Background(), access, searches.job.ID)
	if err != nil {
		t.Fatal(err)
	}
	second, err := source.AcquireResultsFor(context.Background(), access, searches.job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if first.Generation() == 0 || second.Generation() == 0 || first.Generation() == second.Generation() ||
		first.Generation() == searches.pin.generation || second.Generation() == searches.pin.generation {
		t.Fatalf("execution generations = %d, %d; retained generation = %d", first.Generation(), second.Generation(), searches.pin.generation)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	if err := second.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestReexecutionSourceHonorsCancellationDuringSnapshotLookup(t *testing.T) {
	t.Parallel()
	searches, _, access := newReexecutionTestSearches()
	ctx, cancel := context.WithCancel(context.Background())
	searches.onGet = cancel
	source := newReexecutionTestSource(t, searches, reexecutionTestExecutor(func(context.Context, clickhouse.CompiledQuery, searchjobs.ResultSink) error {
		t.Fatal("canceled acquisition must remain lazy")
		return nil
	}), nil)
	lease, err := source.AcquireResultsFor(ctx, access, searches.job.ID)
	if lease != nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("AcquireResultsFor(canceled lookup) = (%v, %v)", lease, err)
	}
	if searches.pin.closed.Load() != 1 {
		t.Fatalf("source pin closes = %d, want 1", searches.pin.closed.Load())
	}
}

func TestReexecutionSourceAdmitsBoundedDynamicTimechartSchema(t *testing.T) {
	t.Parallel()
	searches, _, access := newReexecutionTestSearches()
	searches.job.SPL = `index=main | timechart span=5m count by status`
	schema := searchjobs.Schema{Columns: []searchjobs.Column{
		{Name: "_time", Kind: searchjobs.ValueKindTime},
		{Name: "api", Kind: searchjobs.ValueKindUnsigned},
		{Name: "OTHER", Kind: searchjobs.ValueKindUnsigned},
	}}
	searches.pin.schema = schema
	bucket := searches.job.Earliest.Truncate(5 * time.Minute)
	executor := reexecutionTestExecutor(func(_ context.Context, query clickhouse.CompiledQuery, sink searchjobs.ResultSink) error {
		if query.Timechart == nil {
			t.Fatal("re-executed query lost its timechart contract")
		}
		if err := sink.SetSchema(schema); err != nil {
			return err
		}
		return sink.AddRow([]searchjobs.Value{
			searchjobs.TimeValue(bucket),
			searchjobs.UnsignedValue(3),
			searchjobs.UnsignedValue(1),
		})
	})
	source := newReexecutionTestSource(t, searches, executor, nil)
	lease, err := source.AcquireResultsFor(context.Background(), access, searches.job.ID)
	if err != nil {
		t.Fatal(err)
	}
	row, ok, err := lease.Next(context.Background())
	if err != nil || !ok || len(row.Values) != len(schema.Columns) {
		t.Fatalf("Next(timechart) = (%#v, %t, %v)", row, ok, err)
	}
	if _, ok, err := lease.Next(context.Background()); err != nil || ok {
		t.Fatalf("terminal Next(timechart) = ok %t err %v", ok, err)
	}
	if err := lease.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestReexecutionSourceRejectsDescriptorWideningAndReleasesPin(t *testing.T) {
	t.Parallel()
	searches, _, access := newReexecutionTestSearches()
	searches.job.SPL = `index=main OR index=forbidden | table status`
	source := newReexecutionTestSource(t, searches, reexecutionTestExecutor(func(context.Context, clickhouse.CompiledQuery, searchjobs.ResultSink) error {
		t.Fatal("executor must remain lazy")
		return nil
	}), nil)
	lease, err := source.AcquireResultsFor(context.Background(), access, searches.job.ID)
	if lease != nil || !errors.Is(err, searchjobs.ErrResultsUnavailable) {
		t.Fatalf("AcquireResultsFor(widened descriptor) = (%v, %v)", lease, err)
	}
	if searches.pin.closed.Load() != 1 {
		t.Fatalf("source pin closes = %d, want 1", searches.pin.closed.Load())
	}
}

func TestReexecutionLeaseRejectsSchemaDriftAndMissingSchema(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name     string
		executor reexecutionTestExecutor
	}{
		{
			name: "schema drift",
			executor: func(_ context.Context, _ clickhouse.CompiledQuery, sink searchjobs.ResultSink) error {
				return sink.SetSchema(searchjobs.Schema{Columns: []searchjobs.Column{{Name: "other", Kind: searchjobs.ValueKindString}}})
			},
		},
		{
			name: "missing schema",
			executor: func(context.Context, clickhouse.CompiledQuery, searchjobs.ResultSink) error {
				return nil
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			searches, _, access := newReexecutionTestSearches()
			source := newReexecutionTestSource(t, searches, test.executor, nil)
			lease, err := source.AcquireResultsFor(context.Background(), access, searches.job.ID)
			if err != nil {
				t.Fatal(err)
			}
			if _, ok, nextErr := lease.Next(context.Background()); ok || !errors.Is(nextErr, searchjobs.ErrInvalidResult) {
				t.Fatalf("Next() = ok %t err %v, want invalid result", ok, nextErr)
			}
			if err := lease.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestReexecutionLeaseSurfacesTerminalErrorAfterStreamedRows(t *testing.T) {
	t.Parallel()
	searches, schema, access := newReexecutionTestSearches()
	terminalErr := errors.New("injected terminal query failure")
	executor := reexecutionTestExecutor(func(_ context.Context, _ clickhouse.CompiledQuery, sink searchjobs.ResultSink) error {
		if err := sink.SetSchema(schema); err != nil {
			return err
		}
		if err := sink.AddRow([]searchjobs.Value{searchjobs.SignedValue(200)}); err != nil {
			return err
		}
		return terminalErr
	})
	source := newReexecutionTestSource(t, searches, executor, nil)
	lease, err := source.AcquireResultsFor(context.Background(), access, searches.job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok, nextErr := lease.Next(context.Background()); nextErr != nil || !ok {
		t.Fatalf("first Next() = ok %t err %v", ok, nextErr)
	}
	if _, ok, nextErr := lease.Next(context.Background()); ok || !errors.Is(nextErr, terminalErr) {
		t.Fatalf("terminal Next() = ok %t err %v", ok, nextErr)
	}
	if err := lease.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestReexecutionLeasePreservesExecutorFailureBeforeSchema(t *testing.T) {
	t.Parallel()
	searches, _, access := newReexecutionTestSearches()
	terminalErr := errors.New("injected storage failure before schema")
	source := newReexecutionTestSource(t, searches, reexecutionTestExecutor(func(context.Context, clickhouse.CompiledQuery, searchjobs.ResultSink) error {
		return terminalErr
	}), nil)
	lease, err := source.AcquireResultsFor(context.Background(), access, searches.job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok, nextErr := lease.Next(context.Background()); ok || !errors.Is(nextErr, terminalErr) || errors.Is(nextErr, searchjobs.ErrInvalidResult) {
		t.Fatalf("Next(pre-schema executor failure) = ok %t err %v", ok, nextErr)
	}
	if err := lease.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestReexecutionLeaseCancellationWinsWhenExecutorSwallowsSinkError(t *testing.T) {
	t.Parallel()
	searches, schema, access := newReexecutionTestSearches()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	executor := reexecutionTestExecutor(func(executionContext context.Context, _ clickhouse.CompiledQuery, sink searchjobs.ResultSink) error {
		if err := sink.SetSchema(schema); err != nil {
			return err
		}
		if err := sink.AddRow([]searchjobs.Value{searchjobs.SignedValue(200)}); err != nil {
			return err
		}
		<-executionContext.Done()
		_ = sink.AddRow([]searchjobs.Value{searchjobs.SignedValue(201)})
		return nil
	})
	source := newReexecutionTestSource(t, searches, executor, nil)
	lease, err := source.AcquireResultsFor(ctx, access, searches.job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok, nextErr := lease.Next(context.Background()); nextErr != nil || !ok {
		t.Fatalf("first Next() = ok %t err %v", ok, nextErr)
	}
	cancel()
	if _, ok, nextErr := lease.Next(context.Background()); ok || !errors.Is(nextErr, context.Canceled) {
		t.Fatalf("Next(after swallowed cancellation) = ok %t err %v", ok, nextErr)
	}
	if err := lease.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestReexecutionLeaseDeadlineWinsWhenExecutorReturnsNil(t *testing.T) {
	t.Parallel()
	searches, schema, access := newReexecutionTestSearches()
	release := make(chan struct{})
	executor := reexecutionTestExecutor(func(ctx context.Context, _ clickhouse.CompiledQuery, sink searchjobs.ResultSink) error {
		if err := sink.SetSchema(schema); err != nil {
			return err
		}
		<-ctx.Done()
		<-release
		return nil
	})
	source := newReexecutionTestSource(t, searches, executor, func(config *ReexecutionSourceConfig) {
		config.MaxRuntime = 5 * time.Millisecond
	})
	lease, err := source.AcquireResultsFor(context.Background(), access, searches.job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok, nextErr := lease.Next(context.Background()); ok || !errors.Is(nextErr, context.DeadlineExceeded) {
		t.Fatalf("Next(after swallowed deadline) = ok %t err %v", ok, nextErr)
	}
	close(release)
	if err := lease.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestReexecutionLeaseRejectsCellsThatContradictPinnedSchema(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name  string
		value searchjobs.Value
	}{
		{name: "wrong kind", value: searchjobs.StringValue("200")},
		{name: "null in nonnullable column", value: searchjobs.NullValue()},
		{name: "invalid value", value: searchjobs.Value{}},
	} {
		t.Run(test.name, func(t *testing.T) {
			searches, schema, access := newReexecutionTestSearches()
			executor := reexecutionTestExecutor(func(_ context.Context, _ clickhouse.CompiledQuery, sink searchjobs.ResultSink) error {
				if err := sink.SetSchema(schema); err != nil {
					return err
				}
				_ = sink.AddRow([]searchjobs.Value{test.value})
				return nil
			})
			source := newReexecutionTestSource(t, searches, executor, nil)
			lease, err := source.AcquireResultsFor(context.Background(), access, searches.job.ID)
			if err != nil {
				t.Fatal(err)
			}
			if _, ok, nextErr := lease.Next(context.Background()); ok || !errors.Is(nextErr, searchjobs.ErrInvalidResult) {
				t.Fatalf("Next(invalid cell) = ok %t err %v", ok, nextErr)
			}
			if err := lease.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestReexecutionLeaseCloseBeforeIterationDoesNotStartQuery(t *testing.T) {
	t.Parallel()
	searches, _, access := newReexecutionTestSearches()
	var calls atomic.Int32
	executor := reexecutionTestExecutor(func(context.Context, clickhouse.CompiledQuery, searchjobs.ResultSink) error {
		calls.Add(1)
		return nil
	})
	source := newReexecutionTestSource(t, searches, executor, nil)
	lease, err := source.AcquireResultsFor(context.Background(), access, searches.job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if err := lease.Close(); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 0 {
		t.Fatalf("executor calls = %d, want zero", calls.Load())
	}
	if _, ok, nextErr := lease.Next(context.Background()); ok || !errors.Is(nextErr, searchjobs.ErrResultLeaseClosed) {
		t.Fatalf("Next(after Close) = ok %t err %v", ok, nextErr)
	}
}

func TestReexecutionLeaseConcurrentFirstNextAndClose(t *testing.T) {
	searches, _, access := newReexecutionTestSearches()
	executor := reexecutionTestExecutor(func(ctx context.Context, _ clickhouse.CompiledQuery, _ searchjobs.ResultSink) error {
		<-ctx.Done()
		return ctx.Err()
	})
	source := newReexecutionTestSource(t, searches, executor, nil)
	for iteration := 0; iteration < 200; iteration++ {
		lease, err := source.AcquireResultsFor(context.Background(), access, searches.job.ID)
		if err != nil {
			t.Fatal(err)
		}
		start := make(chan struct{})
		nextResult := make(chan error, 1)
		closeResult := make(chan error, 1)
		go func() {
			<-start
			_, _, nextErr := lease.Next(context.Background())
			nextResult <- nextErr
		}()
		go func() {
			<-start
			closeResult <- lease.Close()
		}()
		close(start)
		if closeErr := <-closeResult; closeErr != nil {
			t.Fatalf("iteration %d Close() = %v", iteration, closeErr)
		}
		nextErr := <-nextResult
		if !errors.Is(nextErr, searchjobs.ErrResultLeaseClosed) && !errors.Is(nextErr, context.Canceled) {
			t.Fatalf("iteration %d Next() = %v", iteration, nextErr)
		}
	}
}

func TestReexecutionLeaseCanceledNextDoesNotConsumeRow(t *testing.T) {
	t.Parallel()
	searches, schema, access := newReexecutionTestSearches()
	entered := make(chan struct{})
	release := make(chan struct{})
	executor := reexecutionTestExecutor(func(ctx context.Context, _ clickhouse.CompiledQuery, sink searchjobs.ResultSink) error {
		if err := sink.SetSchema(schema); err != nil {
			return err
		}
		close(entered)
		select {
		case <-release:
		case <-ctx.Done():
			return ctx.Err()
		}
		return sink.AddRow([]searchjobs.Value{searchjobs.SignedValue(503)})
	})
	source := newReexecutionTestSource(t, searches, executor, func(config *ReexecutionSourceConfig) { config.RowBuffer = 1 })
	lease, err := source.AcquireResultsFor(context.Background(), access, searches.job.ID)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		_, _, nextErr := lease.Next(ctx)
		result <- nextErr
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("executor did not start")
	}
	cancel()
	if nextErr := <-result; !errors.Is(nextErr, context.Canceled) {
		t.Fatalf("canceled Next() = %v", nextErr)
	}
	close(release)
	row, ok, err := lease.Next(context.Background())
	if err != nil || !ok {
		t.Fatalf("Next(after canceled call) = (%#v, %t, %v)", row, ok, err)
	}
	value, _ := row.Values[0].Signed()
	if value != 503 {
		t.Fatalf("retained row value = %d", value)
	}
	if err := lease.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestReexecutionLeaseCloseUnblocksBackpressuredExecutor(t *testing.T) {
	t.Parallel()
	searches, schema, access := newReexecutionTestSearches()
	thirdWrite := make(chan struct{})
	executor := reexecutionTestExecutor(func(_ context.Context, _ clickhouse.CompiledQuery, sink searchjobs.ResultSink) error {
		if err := sink.SetSchema(schema); err != nil {
			return err
		}
		for index := range 3 {
			if index == 2 {
				close(thirdWrite)
			}
			if err := sink.AddRow([]searchjobs.Value{searchjobs.SignedValue(int64(index))}); err != nil {
				return err
			}
		}
		return nil
	})
	source := newReexecutionTestSource(t, searches, executor, func(config *ReexecutionSourceConfig) { config.RowBuffer = 1 })
	lease, err := source.AcquireResultsFor(context.Background(), access, searches.job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok, nextErr := lease.Next(context.Background()); nextErr != nil || !ok {
		t.Fatalf("first Next() = ok %t err %v", ok, nextErr)
	}
	select {
	case <-thirdWrite:
	case <-time.After(5 * time.Second):
		t.Fatal("executor did not reach backpressured row")
	}
	closed := make(chan error, 1)
	go func() { closed <- lease.Close() }()
	select {
	case closeErr := <-closed:
		if closeErr != nil {
			t.Fatal(closeErr)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not unblock the backpressured executor")
	}
	if _, ok, nextErr := lease.Next(context.Background()); ok || !errors.Is(nextErr, searchjobs.ErrResultLeaseClosed) {
		t.Fatalf("Next(after Close) = ok %t err %v", ok, nextErr)
	}
}

func TestNewReexecutionSourceValidatesBoundsAndAcquisitionFailures(t *testing.T) {
	t.Parallel()
	executor := reexecutionTestExecutor(func(context.Context, clickhouse.CompiledQuery, searchjobs.ResultSink) error { return nil })
	if _, err := NewReexecutionSource(ReexecutionSourceConfig{}); err == nil {
		t.Fatal("NewReexecutionSource accepted missing dependencies")
	}
	searches, _, access := newReexecutionTestSearches()
	for _, config := range []ReexecutionSourceConfig{
		{Searches: searches, Executor: executor, MaxRuntime: -1},
		{Searches: searches, Executor: executor, MaxRuntime: maximumReexecutionRuntime + time.Nanosecond},
		{Searches: searches, Executor: executor, RowBuffer: -1},
		{Searches: searches, Executor: executor, RowBuffer: maximumReexecutionRowBuffer + 1},
	} {
		if _, err := NewReexecutionSource(config); err == nil {
			t.Fatalf("NewReexecutionSource(%+v) unexpectedly succeeded", config)
		}
	}
	searches.acquireErr = searchjobs.ErrExpired
	source := newReexecutionTestSource(t, searches, executor, nil)
	if lease, err := source.AcquireResultsFor(context.Background(), access, searches.job.ID); lease != nil || !errors.Is(err, searchjobs.ErrExpired) {
		t.Fatalf("AcquireResultsFor(source failure) = (%v, %v)", lease, err)
	}
}

func newReexecutionTestSearches() (*reexecutionTestSearches, searchjobs.Schema, searchjobs.AccessScope) {
	access := searchjobs.AccessScope{TenantID: "tenant-a", OwnerID: "owner-a"}
	schema := searchjobs.Schema{Columns: []searchjobs.Column{{Name: "status", Kind: searchjobs.ValueKindSigned}}}
	now := time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC)
	return &reexecutionTestSearches{
		job: searchjobs.Job{
			ID:               "search-1",
			OwnerID:          access.OwnerID,
			TenantID:         access.TenantID,
			SPL:              `index=main | table status`,
			RequestedIndexes: []string{"main"},
			EffectiveIndexes: []string{"main"},
			Earliest:         now.Add(-time.Hour),
			Latest:           now,
			IndexTimeCutoff:  now,
			VisibilityCutoff: 42,
			State:            searchjobs.StateCompleted,
		},
		pin: &reexecutionTestPin{schema: schema, generation: 7},
	}, schema, access
}

func newReexecutionTestSource(t *testing.T, searches SearchSnapshotSource, executor searchjobs.Executor, update func(*ReexecutionSourceConfig)) *ReexecutionSource {
	t.Helper()
	config := ReexecutionSourceConfig{
		Searches: searches,
		Executor: executor,
		Compiler: clickhouse.Compiler{Database: "open_splunk", Table: "events"},
	}
	if update != nil {
		update(&config)
	}
	source, err := NewReexecutionSource(config)
	if err != nil {
		t.Fatal(err)
	}
	return source
}
