package export

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/indexread"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
	"github.com/Suhaibinator/open-splunk/internal/searchsnapshot"
	"github.com/Suhaibinator/open-splunk/internal/searchtime"
)

type reexecutionTestSearches struct {
	mu              sync.Mutex
	job             searchjobs.Job
	execution       *searchjobs.ExecutionSnapshot
	pin             *reexecutionTestPin
	manager         *searchjobs.Manager
	resolver        searchjobs.KnowledgeResolver
	appID           string
	compilerVersion string
	lastLease       searchjobs.ResultLease
	acquireErr      error
	acquireCalls    int
	access          searchjobs.AccessScope
	id              string
	onGet           func()
}

func (searches *reexecutionTestSearches) AcquireExecutionFor(
	ctx context.Context,
	access searchjobs.AccessScope,
	id string,
) (searchjobs.ResultLease, searchjobs.ExecutionSnapshot, error) {
	searches.mu.Lock()
	defer searches.mu.Unlock()
	searches.acquireCalls++
	searches.access = access
	searches.id = id
	if searches.acquireErr != nil {
		return nil, searchjobs.ExecutionSnapshot{}, searches.acquireErr
	}
	if searches.onGet != nil {
		searches.onGet()
	}
	if searches.manager == nil {
		if err := searches.startManagerLocked(); err != nil {
			return nil, searchjobs.ExecutionSnapshot{}, err
		}
	}
	pin, execution, err := searches.manager.AcquireExecutionFor(ctx, access, id)
	if err != nil {
		return nil, searchjobs.ExecutionSnapshot{}, err
	}
	if searches.execution != nil {
		execution = *searches.execution
	}
	searches.lastLease = pin
	return pin, execution, nil
}

func (searches *reexecutionTestSearches) lastPinClosed() bool {
	searches.mu.Lock()
	lease := searches.lastLease
	searches.mu.Unlock()
	if lease == nil {
		return false
	}
	_, _, err := lease.Next(context.Background())
	return errors.Is(err, searchjobs.ErrResultLeaseClosed)
}

func (searches *reexecutionTestSearches) startManagerLocked() error {
	if searches.pin == nil {
		return searchjobs.ErrResultsUnavailable
	}
	schema := cloneResultSchema(searches.pin.schema)
	truncated := searches.pin.truncated
	maximumRows := uint64(10)
	if truncated {
		maximumRows = 1
	}
	now := searches.job.CreatedAt
	manager, err := searchjobs.New(searchjobs.Config{
		Executor: integrationSearchExecutor(func(
			_ context.Context,
			_ clickhouse.CompiledQuery,
			sink searchjobs.ResultSink,
		) error {
			if err := sink.SetSchema(schema); err != nil {
				return err
			}
			if !truncated {
				return nil
			}
			if len(schema.Columns) != 1 || schema.Columns[0].Kind != searchjobs.ValueKindSigned {
				return searchjobs.ErrInvalidResult
			}
			if err := sink.AddRow([]searchjobs.Value{searchjobs.SignedValue(1)}); err != nil {
				return err
			}
			return sink.AddRow([]searchjobs.Value{searchjobs.SignedValue(2)})
		}),
		Snapshotter:       integrationSnapshotter(func(context.Context) (uint64, error) { return searches.job.VisibilityCutoff, nil }),
		KnowledgeResolver: searches.resolver,
		CompilerVersion:   searches.compilerVersion,
		MaxConcurrent:     1,
		MaxRows:           maximumRows,
		MaxResultLeases:   16,
		RetentionTTL:      time.Hour,
		CleanupInterval:   -1,
		Now:               func() time.Time { return now },
		NewID:             func() string { return searches.job.ID },
		CursorKey:         []byte("export-reexecution-test-manager-cursor-key-at-least-32-bytes"),
	})
	if err != nil {
		return err
	}
	resolved, err := searchtime.NewAbsoluteRange(searches.job.Earliest, searches.job.Latest)
	if err != nil {
		_ = manager.Close()
		return err
	}
	created, err := manager.Create(context.Background(), searchjobs.CreateRequest{
		SPL:               searches.job.SPL,
		OwnerID:           searches.job.OwnerID,
		TenantID:          searches.job.TenantID,
		AppID:             searches.appID,
		AuthorizedIndexes: slices.Clone(searches.job.EffectiveIndexes),
		RequestedIndexes:  slices.Clone(searches.job.EffectiveIndexes),
		TimeRange:         resolved,
	})
	if err != nil {
		_ = manager.Close()
		return err
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		job, getErr := manager.GetFor(
			searchjobs.AccessScope{TenantID: searches.job.TenantID, OwnerID: searches.job.OwnerID},
			created.ID,
		)
		if getErr != nil {
			_ = manager.Close()
			return getErr
		}
		if job.State.Terminal() {
			if job.State != searchjobs.StateCompleted {
				_ = manager.Close()
				return fmt.Errorf("%w: reexecution fixture search ended in %s", searchjobs.ErrResultsUnavailable, job.State)
			}
			searches.job = job
			searches.manager = manager
			return nil
		}
		time.Sleep(time.Millisecond)
	}
	_ = manager.Close()
	return errors.New("reexecution fixture search did not complete")
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

type nilReexecutionTestExecutor struct{}

func (*nilReexecutionTestExecutor) Execute(context.Context, clickhouse.CompiledQuery, searchjobs.ResultSink) error {
	return nil
}

func (executor reexecutionTestExecutor) Execute(ctx context.Context, query clickhouse.CompiledQuery, sink searchjobs.ResultSink) error {
	return executor(ctx, query, sink)
}

func TestReexecutionSourceRejectsOrdinarySnapshotFromAnotherCompilerVersion(t *testing.T) {
	t.Parallel()
	searches, _, access := newReexecutionTestSearches()
	searches.compilerVersion = "0.1"
	var executions atomic.Int32
	source := newReexecutionTestSource(
		t,
		searches,
		reexecutionTestExecutor(func(
			context.Context,
			clickhouse.CompiledQuery,
			searchjobs.ResultSink,
		) error {
			executions.Add(1)
			return nil
		}),
		nil,
	)

	lease, err := source.AcquireResultsFor(
		context.Background(),
		access,
		searches.job.ID,
	)
	if lease != nil || !errors.Is(err, searchjobs.ErrResultsUnavailable) ||
		!errors.Is(err, searchsnapshot.ErrCompilerVersionMismatch) {
		t.Fatalf(
			"AcquireResultsFor(incompatible ordinary snapshot) = (%#v, %v), want unavailable version mismatch",
			lease,
			err,
		)
	}
	if executions.Load() != 0 {
		t.Fatalf("executor calls = %d, want 0", executions.Load())
	}
	if !searches.lastPinClosed() {
		t.Fatal("failed incompatible-version acquisition retained its result pin")
	}
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
	if searches.lastPinClosed() {
		t.Fatal("source pin closed before re-execution lease")
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
	if !strings.Contains(captured.SQL, `"expires_at" > parseDateTime64BestEffort(?, 3, 'UTC')`) ||
		!strings.Contains(captured.SQL, `"visibility_seq" <= ?`) {
		t.Fatalf("compiled query did not retain snapshot predicates: %s / %#v", captured.SQL, captured.Args)
	}
	cutoff := searches.job.IndexTimeCutoff.UTC().Truncate(time.Millisecond).Format("2006-01-02 15:04:05.000")
	wantScope := []any{
		searches.job.TenantID,
		"main",
		searches.job.Earliest.UTC().Format("2006-01-02 15:04:05.000000000"),
		searches.job.Latest.UTC().Format("2006-01-02 15:04:05.000000000"),
		cutoff,
		cutoff,
		searches.job.VisibilityCutoff,
	}
	if len(captured.Args) < len(wantScope) || !slices.Equal(captured.Args[:len(wantScope)], wantScope) {
		t.Fatalf("compiled export scope = %#v, want prefix %#v", captured.Args, wantScope)
	}
	if err := lease.Close(); err != nil {
		t.Fatal(err)
	}
	if !searches.lastPinClosed() {
		t.Fatal("source pin remained open after lease close")
	}
	searches.mu.Lock()
	defer searches.mu.Unlock()
	if searches.acquireCalls != 1 || searches.access != access || searches.id != searches.job.ID {
		t.Fatalf("snapshot calls = acquire %d scope %+v id %q", searches.acquireCalls, searches.access, searches.id)
	}
}

func TestReexecutionSourceRebuildsStreamStatsCountFieldFromStoredSPL(t *testing.T) {
	t.Parallel()

	searches, _, access := newReexecutionTestSearches()
	searches.job.SPL = `index=main | table status | streamstats current=false count(status) AS prior`
	schema := searchjobs.Schema{Columns: []searchjobs.Column{
		{Name: "status", Kind: searchjobs.ValueKindSigned},
		{Name: "prior", Kind: searchjobs.ValueKindUnsigned},
	}}
	searches.pin.schema = schema
	var captured clickhouse.CompiledQuery
	executor := reexecutionTestExecutor(func(
		_ context.Context,
		query clickhouse.CompiledQuery,
		sink searchjobs.ResultSink,
	) error {
		captured = query
		return sink.SetSchema(schema)
	})
	source := newReexecutionTestSource(t, searches, executor, nil)
	lease, err := source.AcquireResultsFor(
		context.Background(),
		access,
		searches.job.ID,
	)
	if err != nil {
		t.Fatalf("AcquireResultsFor(streamstats count(field)): %v", err)
	}
	if _, ok, nextErr := lease.Next(context.Background()); ok || nextErr != nil {
		t.Fatalf("Next(streamstats count(field)) = ok %t err %v", ok, nextErr)
	}
	for _, required := range []string{
		`sum(toUInt128("__os_streamstats_measure_`,
		`ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING`,
		`AS "prior"`,
		clickhouse.StreamStatsInputLimitMarker,
	} {
		if !strings.Contains(captured.SQL, required) {
			t.Fatalf(
				"re-executed streamstats count(field) SQL missing %q:\n%s",
				required,
				captured.SQL,
			)
		}
	}
	if err := lease.Close(); err != nil {
		t.Fatalf("close streamstats count(field) re-execution: %v", err)
	}
}

func TestReexecutionSourceRebuildsStreamStatsCountEvalFromStoredSPL(t *testing.T) {
	t.Parallel()

	searches, _, access := newReexecutionTestSearches()
	searches.job.SPL = `index=main | table source,service | streamstats current=false window=3 global=false count(eval(source="api")) AS prior_api BY service`
	schema := searchjobs.Schema{Columns: []searchjobs.Column{
		{Name: "source", Kind: searchjobs.ValueKindString},
		{Name: "service", Kind: searchjobs.ValueKindString},
		{Name: "prior_api", Kind: searchjobs.ValueKindUnsigned, Nullable: true},
	}}
	searches.pin.schema = schema
	var captured clickhouse.CompiledQuery
	executor := reexecutionTestExecutor(func(
		_ context.Context,
		query clickhouse.CompiledQuery,
		sink searchjobs.ResultSink,
	) error {
		captured = query
		return sink.SetSchema(schema)
	})
	source := newReexecutionTestSource(t, searches, executor, nil)
	lease, err := source.AcquireResultsFor(
		context.Background(),
		access,
		searches.job.ID,
	)
	if err != nil {
		t.Fatalf("AcquireResultsFor(streamstats count(eval)): %v", err)
	}
	if _, ok, nextErr := lease.Next(context.Background()); ok || nextErr != nil {
		t.Fatalf("Next(streamstats count(eval)) = ok %t err %v", ok, nextErr)
	}
	for _, required := range []string{
		`toString("source") = CAST(? AS String)`,
		`ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING`,
		`AS "prior_api"`,
		clickhouse.StreamStatsInputLimitMarker,
	} {
		if !strings.Contains(captured.SQL, required) {
			t.Fatalf(
				"re-executed streamstats count(eval) SQL missing %q:\n%s",
				required,
				captured.SQL,
			)
		}
	}
	if !slices.Contains(captured.Args, any("api")) {
		t.Fatalf(
			"re-executed streamstats count(eval) args = %#v, want predicate value api",
			captured.Args,
		)
	}
	if err := lease.Close(); err != nil {
		t.Fatalf("close streamstats count(eval) re-execution: %v", err)
	}
}

func TestReexecutionSourceRebuildsStreamStatsSumFromStoredSPL(t *testing.T) {
	t.Parallel()

	searches, _, access := newReexecutionTestSearches()
	searches.job.SPL = `index=main | table status | streamstats current=false sum(status) AS prior_total`
	schema := searchjobs.Schema{Columns: []searchjobs.Column{
		{Name: "status", Kind: searchjobs.ValueKindSigned},
		{Name: "prior_total", Kind: searchjobs.ValueKindDouble, Nullable: true},
	}}
	searches.pin.schema = schema
	var captured clickhouse.CompiledQuery
	executor := reexecutionTestExecutor(func(
		_ context.Context,
		query clickhouse.CompiledQuery,
		sink searchjobs.ResultSink,
	) error {
		captured = query
		return sink.SetSchema(schema)
	})
	source := newReexecutionTestSource(t, searches, executor, nil)
	lease, err := source.AcquireResultsFor(
		context.Background(),
		access,
		searches.job.ID,
	)
	if err != nil {
		t.Fatalf("AcquireResultsFor(streamstats sum(field)): %v", err)
	}
	if _, ok, nextErr := lease.Next(context.Background()); ok || nextErr != nil {
		t.Fatalf("Next(streamstats sum(field)) = ok %t err %v", ok, nextErr)
	}
	for _, required := range []string{
		`sumOrNullArray(`,
		`ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING`,
		`AS "prior_total"`,
		`Nullable(Float64)`,
		clickhouse.StreamStatsInputLimitMarker,
	} {
		if !strings.Contains(captured.SQL, required) {
			t.Fatalf(
				"re-executed streamstats sum(field) SQL missing %q:\n%s",
				required,
				captured.SQL,
			)
		}
	}
	if err := lease.Close(); err != nil {
		t.Fatalf("close streamstats sum(field) re-execution: %v", err)
	}
}

func TestReexecutionSourceRebuildsStreamStatsAverageFromStoredSPL(t *testing.T) {
	t.Parallel()

	searches, _, access := newReexecutionTestSearches()
	searches.job.SPL = `index=main | table status | streamstats current=false avg(status) AS prior_mean`
	schema := searchjobs.Schema{Columns: []searchjobs.Column{
		{Name: "status", Kind: searchjobs.ValueKindSigned},
		{Name: "prior_mean", Kind: searchjobs.ValueKindDouble, Nullable: true},
	}}
	searches.pin.schema = schema
	var captured clickhouse.CompiledQuery
	executor := reexecutionTestExecutor(func(
		_ context.Context,
		query clickhouse.CompiledQuery,
		sink searchjobs.ResultSink,
	) error {
		captured = query
		return sink.SetSchema(schema)
	})
	source := newReexecutionTestSource(t, searches, executor, nil)
	lease, err := source.AcquireResultsFor(
		context.Background(),
		access,
		searches.job.ID,
	)
	if err != nil {
		t.Fatalf("AcquireResultsFor(streamstats avg(field)): %v", err)
	}
	if _, ok, nextErr := lease.Next(context.Background()); ok || nextErr != nil {
		t.Fatalf("Next(streamstats avg(field)) = ok %t err %v", ok, nextErr)
	}
	for _, required := range []string{
		`avgOrNullArray(`,
		`ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING`,
		`AS "prior_mean"`,
		`Nullable(Float64)`,
		clickhouse.StreamStatsInputLimitMarker,
	} {
		if !strings.Contains(captured.SQL, required) {
			t.Fatalf(
				"re-executed streamstats avg(field) SQL missing %q:\n%s",
				required,
				captured.SQL,
			)
		}
	}
	if err := lease.Close(); err != nil {
		t.Fatalf("close streamstats avg(field) re-execution: %v", err)
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

func TestReexecutionSourceAdmitsStaticTimechartSchema(t *testing.T) {
	t.Parallel()
	searches, _, access := newReexecutionTestSearches()
	searches.job.SPL = `index=main | timechart span=5m count`
	schema := searchjobs.Schema{Columns: []searchjobs.Column{
		{Name: "_time", Kind: searchjobs.ValueKindTime},
		{Name: "count", Kind: searchjobs.ValueKindUnsigned},
	}}
	searches.pin.schema = schema
	bucket := searches.job.Earliest.Truncate(5 * time.Minute)
	executor := reexecutionTestExecutor(func(_ context.Context, query clickhouse.CompiledQuery, sink searchjobs.ResultSink) error {
		if query.Timechart == nil ||
			query.Timechart.Mode != clickhouse.TimechartModeFixedCount {
			t.Fatalf("re-executed query lost its static timechart contract: %#v", query.Timechart)
		}
		if err := sink.SetSchema(schema); err != nil {
			return err
		}
		return sink.AddRow([]searchjobs.Value{
			searchjobs.TimeValue(bucket),
			searchjobs.UnsignedValue(3),
		})
	})
	source := newReexecutionTestSource(t, searches, executor, nil)
	lease, err := source.AcquireResultsFor(context.Background(), access, searches.job.ID)
	if err != nil {
		t.Fatal(err)
	}
	row, ok, err := lease.Next(context.Background())
	if err != nil || !ok || len(row.Values) != len(schema.Columns) {
		t.Fatalf("Next(static timechart) = (%#v, %t, %v)", row, ok, err)
	}
	if _, ok, err := lease.Next(context.Background()); err != nil || ok {
		t.Fatalf("terminal Next(static timechart) = ok %t err %v", ok, err)
	}
	if err := lease.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestReexecutionSourceAdmitsFixedPercentileTimechartSchema(t *testing.T) {
	t.Parallel()
	searches, _, access := newReexecutionTestSearches()
	searches.job.SPL = `index=main | timechart span=5m p95(duration) AS p95_duration`
	schema := searchjobs.Schema{Columns: []searchjobs.Column{
		{Name: "_time", Kind: searchjobs.ValueKindTime},
		{Name: "p95_duration", Kind: searchjobs.ValueKindDouble, Nullable: true},
	}}
	searches.pin.schema = schema
	bucket := searches.job.Earliest.Truncate(5 * time.Minute)
	executor := reexecutionTestExecutor(func(
		_ context.Context,
		query clickhouse.CompiledQuery,
		sink searchjobs.ResultSink,
	) error {
		if query.Timechart == nil ||
			query.Timechart.Mode != clickhouse.TimechartModeFixedValue ||
			query.Timechart.ValueKind != clickhouse.TimechartValueKindPercentile ||
			query.Timechart.ValueField != "p95_duration" {
			t.Fatalf(
				"re-executed query lost its fixed percentile contract: %#v",
				query.Timechart,
			)
		}
		if err := sink.SetSchema(schema); err != nil {
			return err
		}
		return sink.AddRow([]searchjobs.Value{
			searchjobs.TimeValue(bucket),
			searchjobs.DoubleValue(125.5),
		})
	})
	source := newReexecutionTestSource(t, searches, executor, nil)
	lease, err := source.AcquireResultsFor(context.Background(), access, searches.job.ID)
	if err != nil {
		t.Fatal(err)
	}
	row, ok, err := lease.Next(context.Background())
	if err != nil || !ok || len(row.Values) != len(schema.Columns) {
		t.Fatalf("Next(fixed percentile timechart) = (%#v, %t, %v)", row, ok, err)
	}
	if value, ok := row.Values[1].Double(); !ok || value != 125.5 {
		t.Fatalf("fixed percentile value = %v, %v", value, ok)
	}
	if _, ok, err := lease.Next(context.Background()); err != nil || ok {
		t.Fatalf("terminal Next(fixed percentile timechart) = ok %t err %v", ok, err)
	}
	if err := lease.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestReexecutionSourceAdmitsFixedSumAndAverageTimechartSchemas(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		spl   string
		field string
		kind  clickhouse.TimechartValueKind
		value float64
	}{
		{
			name: "sum", spl: `index=main | timechart span=5m sum(bytes) AS total_bytes`,
			field: "total_bytes", kind: clickhouse.TimechartValueKindSum, value: 250.5,
		},
		{
			name: "average", spl: `index=main | timechart span=5m avg(latency) AS mean_latency`,
			field: "mean_latency", kind: clickhouse.TimechartValueKindAverage, value: 12.75,
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			searches, _, access := newReexecutionTestSearches()
			searches.job.SPL = test.spl
			schema := searchjobs.Schema{Columns: []searchjobs.Column{
				{Name: "_time", Kind: searchjobs.ValueKindTime},
				{Name: test.field, Kind: searchjobs.ValueKindDouble, Nullable: true},
			}}
			searches.pin.schema = schema
			bucket := searches.job.Earliest.Truncate(5 * time.Minute)
			executor := reexecutionTestExecutor(func(
				_ context.Context,
				query clickhouse.CompiledQuery,
				sink searchjobs.ResultSink,
			) error {
				if query.Timechart == nil ||
					query.Timechart.Mode != clickhouse.TimechartModeFixedValue ||
					query.Timechart.ValueField != test.field ||
					query.Timechart.ValueKind != test.kind {
					t.Fatalf("re-executed query lost its fixed value contract: %#v", query.Timechart)
				}
				if err := sink.SetSchema(schema); err != nil {
					return err
				}
				return sink.AddRow([]searchjobs.Value{
					searchjobs.TimeValue(bucket),
					searchjobs.DoubleValue(test.value),
				})
			})
			source := newReexecutionTestSource(t, searches, executor, nil)
			lease, err := source.AcquireResultsFor(
				context.Background(),
				access,
				searches.job.ID,
			)
			if err != nil {
				t.Fatal(err)
			}
			row, ok, err := lease.Next(context.Background())
			if err != nil || !ok || len(row.Values) != len(schema.Columns) {
				t.Fatalf("Next(fixed value timechart) = (%#v, %t, %v)", row, ok, err)
			}
			if value, ok := row.Values[1].Double(); !ok || value != test.value {
				t.Fatalf("fixed value = %v, %v", value, ok)
			}
			if _, ok, err := lease.Next(context.Background()); err != nil || ok {
				t.Fatalf("terminal Next(fixed value timechart) = ok %t err %v", ok, err)
			}
			if err := lease.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestReexecutionSourceAdmitsBoundedDynamicChartSchema(t *testing.T) {
	t.Parallel()
	searches, _, access := newReexecutionTestSearches()
	searches.job.SPL = `index=main | chart count OVER path BY status`
	schema := searchjobs.Schema{Columns: []searchjobs.Column{
		{Name: "path", Kind: searchjobs.ValueKindString},
		{Name: "api", Kind: searchjobs.ValueKindUnsigned},
		{Name: "OTHER", Kind: searchjobs.ValueKindUnsigned},
	}}
	searches.pin.schema = schema
	executor := reexecutionTestExecutor(func(_ context.Context, query clickhouse.CompiledQuery, sink searchjobs.ResultSink) error {
		if query.Chart == nil || query.Chart.RowField != "path" {
			t.Fatalf("re-executed query lost its chart contract: %#v", query.Chart)
		}
		if err := sink.SetSchema(schema); err != nil {
			return err
		}
		return sink.AddRow([]searchjobs.Value{
			searchjobs.StringValue("/a"),
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
		t.Fatalf("Next(chart) = (%#v, %t, %v)", row, ok, err)
	}
	if _, ok, err := lease.Next(context.Background()); err != nil || ok {
		t.Fatalf("terminal Next(chart) = ok %t err %v", ok, err)
	}
	if err := lease.Close(); err != nil {
		t.Fatal(err)
	}
}

// TestSchemaMatchesCompiledChartRejectsForeignWideSchemas pins the export-side
// duplicate of the wide-schema admission rule for the pivot's runtime-named,
// runtime-typed first column.
func TestSchemaMatchesCompiledChartRejectsForeignWideSchemas(t *testing.T) {
	t.Parallel()

	compiled := clickhouse.CompiledQuery{
		OutputFields: []string{"path"},
		Chart: &clickhouse.ChartOutput{
			RowField: "path", RowKind: clickhouse.ChartRowKindString, RowDatabaseType: "String",
			RowLimit: 10_000, MaxSeries: 2, MaxLabelBytes: 256,
			ValueKind: clickhouse.ChartValueKindCount,
		},
	}
	valid := searchjobs.Schema{Columns: []searchjobs.Column{
		{Name: "path", Kind: searchjobs.ValueKindString},
		{Name: "api", Kind: searchjobs.ValueKindUnsigned},
	}}
	if !schemaMatchesCompiledQuery(valid, compiled) {
		t.Fatal("valid bounded pivot schema was rejected")
	}
	numeric := compiled
	numericChart := *compiled.Chart
	numericChart.ValueKind = clickhouse.ChartValueKindAverage
	numeric.Chart = &numericChart
	numericSchema := searchjobs.Schema{Columns: []searchjobs.Column{
		{Name: "path", Kind: searchjobs.ValueKindString},
		{Name: "api", Kind: searchjobs.ValueKindDouble, Nullable: true},
	}}
	if !schemaMatchesCompiledQuery(numericSchema, numeric) {
		t.Fatal("valid numeric pivot schema was rejected")
	}
	if schemaMatchesCompiledQuery(valid, numeric) || schemaMatchesCompiledQuery(numericSchema, compiled) {
		t.Fatal("chart schema value policy was not bound to its compiled value kind")
	}
	for _, test := range []struct {
		name   string
		schema searchjobs.Schema
		mutate func(*clickhouse.CompiledQuery)
	}{
		{name: "wrong row name", schema: searchjobs.Schema{Columns: []searchjobs.Column{{Name: "_time", Kind: searchjobs.ValueKindTime}}}},
		{name: "wrong row kind", schema: searchjobs.Schema{Columns: []searchjobs.Column{{Name: "path", Kind: searchjobs.ValueKindUnsigned}}}},
		{
			name: "too many series",
			schema: searchjobs.Schema{Columns: []searchjobs.Column{
				{Name: "path", Kind: searchjobs.ValueKindString},
				{Name: "a", Kind: searchjobs.ValueKindUnsigned},
				{Name: "b", Kind: searchjobs.ValueKindUnsigned},
				{Name: "c", Kind: searchjobs.ValueKindUnsigned},
			}},
		},
		{name: "empty schema", schema: searchjobs.Schema{}},
		{
			name: "row kind unset", schema: valid,
			mutate: func(query *clickhouse.CompiledQuery) { query.Chart.RowKind = clickhouse.ChartRowKindInvalid },
		},
		{
			name: "value kind unset", schema: valid,
			mutate: func(query *clickhouse.CompiledQuery) { query.Chart.ValueKind = clickhouse.ChartValueKindInvalid },
		},
		{
			name: "two wide contracts", schema: valid,
			mutate: func(query *clickhouse.CompiledQuery) {
				query.Timechart = &clickhouse.TimechartOutput{MaxSeries: 2, MaxLabelBytes: 256}
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			candidate := compiled
			chart := *compiled.Chart
			candidate.Chart = &chart
			if test.mutate != nil {
				test.mutate(&candidate)
			}
			if schemaMatchesCompiledQuery(test.schema, candidate) {
				t.Fatalf("schemaMatchesCompiledQuery admitted %q", test.name)
			}
		})
	}
}

func TestSchemaMatchesCompiledStaticTimechartRejectsForeignSchemas(t *testing.T) {
	t.Parallel()

	compiled := clickhouse.CompiledQuery{
		OutputFields: []string{"_time", "count"},
		Timechart: &clickhouse.TimechartOutput{
			Mode:      clickhouse.TimechartModeFixedCount,
			MaxSeries: 1,
		},
	}
	valid := searchjobs.Schema{Columns: []searchjobs.Column{
		{Name: "_time", Kind: searchjobs.ValueKindTime},
		{Name: "count", Kind: searchjobs.ValueKindUnsigned},
	}}
	if !schemaMatchesCompiledQuery(valid, compiled) {
		t.Fatal("valid static timechart schema was rejected")
	}
	for _, test := range []struct {
		name   string
		schema searchjobs.Schema
		mutate func(*clickhouse.CompiledQuery)
	}{
		{name: "empty schema", schema: searchjobs.Schema{}},
		{name: "missing count", schema: searchjobs.Schema{Columns: valid.Columns[:1]}},
		{name: "wrong time kind", schema: searchjobs.Schema{Columns: []searchjobs.Column{{Name: "_time", Kind: searchjobs.ValueKindString}, valid.Columns[1]}}},
		{name: "nullable time", schema: searchjobs.Schema{Columns: []searchjobs.Column{{Name: "_time", Kind: searchjobs.ValueKindTime, Nullable: true}, valid.Columns[1]}}},
		{name: "wrong count name", schema: searchjobs.Schema{Columns: []searchjobs.Column{valid.Columns[0], {Name: "events", Kind: searchjobs.ValueKindUnsigned}}}},
		{name: "wrong count kind", schema: searchjobs.Schema{Columns: []searchjobs.Column{valid.Columns[0], {Name: "count", Kind: searchjobs.ValueKindSigned}}}},
		{name: "nullable count", schema: searchjobs.Schema{Columns: []searchjobs.Column{valid.Columns[0], {Name: "count", Kind: searchjobs.ValueKindUnsigned, Nullable: true}}}},
		{
			name: "invalid output mode", schema: valid,
			mutate: func(query *clickhouse.CompiledQuery) {
				query.Timechart.Mode = clickhouse.TimechartMode(255)
			},
		},
		{
			name: "wrong output fields", schema: valid,
			mutate: func(query *clickhouse.CompiledQuery) {
				query.OutputFields = []string{"_time"}
			},
		},
		{
			name: "wrong series bound", schema: valid,
			mutate: func(query *clickhouse.CompiledQuery) {
				query.Timechart.MaxSeries = 2
			},
		},
		{
			name: "dynamic label bound", schema: valid,
			mutate: func(query *clickhouse.CompiledQuery) {
				query.Timechart.MaxLabelBytes = 1
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			candidate := compiled
			timechart := *compiled.Timechart
			candidate.Timechart = &timechart
			if test.mutate != nil {
				test.mutate(&candidate)
			}
			if schemaMatchesCompiledQuery(test.schema, candidate) {
				t.Fatalf("schemaMatchesCompiledQuery admitted %q", test.name)
			}
		})
	}
}

func TestSchemaMatchesCompiledFixedPercentileTimechartRejectsForeignSchemas(t *testing.T) {
	t.Parallel()

	compiled := clickhouse.CompiledQuery{
		OutputFields: []string{"_time", "p95_duration"},
		Timechart: &clickhouse.TimechartOutput{
			Mode:       clickhouse.TimechartModeFixedValue,
			MaxSeries:  1,
			ValueField: "p95_duration",
			ValueKind:  clickhouse.TimechartValueKindPercentile,
		},
	}
	valid := searchjobs.Schema{Columns: []searchjobs.Column{
		{Name: "_time", Kind: searchjobs.ValueKindTime},
		{Name: "p95_duration", Kind: searchjobs.ValueKindDouble, Nullable: true},
	}}
	if !schemaMatchesCompiledQuery(valid, compiled) {
		t.Fatal("valid fixed percentile timechart schema was rejected")
	}
	longValueField := strings.Repeat("a", 200) + "." + strings.Repeat("b", 200)
	longCompiled := compiled
	longTimechart := *compiled.Timechart
	longTimechart.ValueField = longValueField
	longCompiled.Timechart = &longTimechart
	longCompiled.OutputFields = []string{"_time", longValueField}
	longSchema := searchjobs.Schema{Columns: []searchjobs.Column{
		valid.Columns[0],
		{Name: longValueField, Kind: searchjobs.ValueKindDouble, Nullable: true},
	}}
	if !schemaMatchesCompiledQuery(longSchema, longCompiled) {
		t.Fatal("valid bounded dotted fixed percentile schema was rejected")
	}

	for _, test := range []struct {
		name   string
		schema searchjobs.Schema
		mutate func(*clickhouse.CompiledQuery)
	}{
		{name: "empty schema", schema: searchjobs.Schema{}},
		{name: "missing value", schema: searchjobs.Schema{Columns: valid.Columns[:1]}},
		{name: "wrong time kind", schema: searchjobs.Schema{Columns: []searchjobs.Column{{Name: "_time", Kind: searchjobs.ValueKindString}, valid.Columns[1]}}},
		{name: "nullable time", schema: searchjobs.Schema{Columns: []searchjobs.Column{{Name: "_time", Kind: searchjobs.ValueKindTime, Nullable: true}, valid.Columns[1]}}},
		{name: "wrong value name", schema: searchjobs.Schema{Columns: []searchjobs.Column{valid.Columns[0], {Name: "other", Kind: searchjobs.ValueKindDouble, Nullable: true}}}},
		{name: "wrong value kind", schema: searchjobs.Schema{Columns: []searchjobs.Column{valid.Columns[0], {Name: "p95_duration", Kind: searchjobs.ValueKindSigned, Nullable: true}}}},
		{name: "nonnullable value", schema: searchjobs.Schema{Columns: []searchjobs.Column{valid.Columns[0], {Name: "p95_duration", Kind: searchjobs.ValueKindDouble}}}},
		{name: "multivalue value", schema: searchjobs.Schema{Columns: []searchjobs.Column{valid.Columns[0], {Name: "p95_duration", Kind: searchjobs.ValueKindDouble, Nullable: true, Multivalue: true}}}},
		{
			name: "wrong output fields", schema: valid,
			mutate: func(query *clickhouse.CompiledQuery) {
				query.OutputFields = []string{"_time", "other"}
			},
		},
		{
			name: "wrong value field", schema: valid,
			mutate: func(query *clickhouse.CompiledQuery) {
				query.Timechart.ValueField = "other"
			},
		},
		{
			name: "time collision", schema: valid,
			mutate: func(query *clickhouse.CompiledQuery) {
				query.Timechart.ValueField = "_time"
				query.OutputFields = []string{"_time", "_time"}
			},
		},
		{
			name: "private value field", schema: valid,
			mutate: func(query *clickhouse.CompiledQuery) {
				query.Timechart.ValueField = "__os_private"
				query.OutputFields = []string{"_time", "__os_private"}
			},
		},
		{
			name: "wrong series bound", schema: valid,
			mutate: func(query *clickhouse.CompiledQuery) {
				query.Timechart.MaxSeries = 2
			},
		},
		{
			name: "dynamic label bound", schema: valid,
			mutate: func(query *clickhouse.CompiledQuery) {
				query.Timechart.MaxLabelBytes = 1
			},
		},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			candidate := compiled
			timechart := *compiled.Timechart
			candidate.Timechart = &timechart
			if test.mutate != nil {
				test.mutate(&candidate)
			}
			if schemaMatchesCompiledQuery(test.schema, candidate) {
				t.Fatalf("schemaMatchesCompiledQuery admitted %q", test.name)
			}
		})
	}
}

func TestSchemaMatchesCompiledSplitPercentileTimechart(t *testing.T) {
	t.Parallel()

	compiled := clickhouse.CompiledQuery{
		OutputFields: []string{"_time"},
		Timechart: &clickhouse.TimechartOutput{
			Mode:          clickhouse.TimechartModeRuntimeWideValue,
			MaxSeries:     clickhouse.MaximumTimechartSeries,
			MaxLabelBytes: clickhouse.MaximumTimechartLabelBytes,
			ValueKind:     clickhouse.TimechartValueKindPercentile,
		},
	}
	valid := searchjobs.Schema{Columns: []searchjobs.Column{
		{Name: "_time", Kind: searchjobs.ValueKindTime},
		{Name: "api", Kind: searchjobs.ValueKindDouble, Nullable: true},
		{Name: "NULL", Kind: searchjobs.ValueKindDouble, Nullable: true},
	}}
	if !schemaMatchesCompiledQuery(valid, compiled) {
		t.Fatal("valid split percentile timechart schema was rejected")
	}

	for _, kind := range []clickhouse.TimechartValueKind{
		clickhouse.TimechartValueKindInvalid,
		clickhouse.TimechartValueKind(255),
	} {
		invalid := compiled
		timechart := *compiled.Timechart
		timechart.ValueKind = kind
		invalid.Timechart = &timechart
		if schemaMatchesCompiledQuery(valid, invalid) {
			t.Fatalf("invalid split percentile timechart kind %d was admitted", kind)
		}
	}
}

func TestSchemaMatchesCompiledFixedValueKinds(t *testing.T) {
	t.Parallel()

	schema := searchjobs.Schema{Columns: []searchjobs.Column{
		{Name: "_time", Kind: searchjobs.ValueKindTime},
		{Name: "metric", Kind: searchjobs.ValueKindDouble, Nullable: true},
	}}
	for _, kind := range []clickhouse.TimechartValueKind{
		clickhouse.TimechartValueKindSum,
		clickhouse.TimechartValueKindAverage,
	} {
		compiled := clickhouse.CompiledQuery{
			OutputFields: []string{"_time", "metric"},
			Timechart: &clickhouse.TimechartOutput{
				Mode:       clickhouse.TimechartModeFixedValue,
				MaxSeries:  1,
				ValueField: "metric",
				ValueKind:  kind,
			},
		}
		if !schemaMatchesCompiledQuery(schema, compiled) {
			t.Fatalf("valid fixed value kind %v was rejected", kind)
		}
	}

	invalid := clickhouse.CompiledQuery{
		OutputFields: []string{"_time", "metric"},
		Timechart: &clickhouse.TimechartOutput{
			Mode:       clickhouse.TimechartModeFixedValue,
			MaxSeries:  1,
			ValueField: "metric",
			ValueKind:  clickhouse.TimechartValueKind(255),
		},
	}
	if schemaMatchesCompiledQuery(schema, invalid) {
		t.Fatal("invalid fixed value kind was admitted")
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

func TestCompletedSearchExportFailsExplicitlyWhenItsIndexIsRetired(t *testing.T) {
	t.Parallel()

	searches, _, access := newReexecutionTestSearches()
	registry := indexread.NewRegistry()
	executionStarted := make(chan struct{})
	executor := reexecutionTestExecutor(func(
		ctx context.Context,
		compiled clickhouse.CompiledQuery,
		_ searchjobs.ResultSink,
	) error {
		tenantID, indexNames, ok := compiled.ReadScope()
		if !ok {
			return errors.New("compiled export query has no read scope")
		}
		admittedContext, release, err := registry.Acquire(ctx, tenantID, indexNames)
		if err != nil {
			return err
		}
		close(executionStarted)
		<-admittedContext.Done()
		cause := context.Cause(admittedContext)
		release()
		return cause
	})
	source := newReexecutionTestSource(t, searches, executor, nil)
	manager := newExportTestManager(t, source, nil)
	created, err := manager.Create(
		context.Background(),
		access,
		CreateRequest{SearchJobID: searches.job.ID, Format: FormatCSV},
	)
	if err != nil {
		t.Fatalf("Create(): %v", err)
	}
	select {
	case <-executionStarted:
	case <-time.After(time.Second):
		t.Fatal("export re-execution did not start")
	}
	if err := registry.Retire(context.Background(), access.TenantID, "main"); err != nil {
		t.Fatalf("Retire(): %v", err)
	}

	failed := waitExportState(t, manager, access, created.ID, StateFailed)
	if failed.Failure == nil || failed.Failure.Code != FailureSourceUnavailable ||
		failed.Failure.Retryable || failed.Artifact != nil {
		t.Fatalf("retired-index export = %#v, want non-retryable source failure", failed)
	}
}

func TestCompletedSearchExportFailsExplicitlyWhenItsIndexWasAlreadyRetired(t *testing.T) {
	t.Parallel()

	searches, _, access := newReexecutionTestSearches()
	registry := indexread.NewRegistry()
	if err := registry.Retire(context.Background(), access.TenantID, "main"); err != nil {
		t.Fatalf("Retire(): %v", err)
	}

	var (
		executorCalls atomic.Int32
		nativeWork    atomic.Int32
	)
	executor := reexecutionTestExecutor(func(
		ctx context.Context,
		compiled clickhouse.CompiledQuery,
		_ searchjobs.ResultSink,
	) error {
		executorCalls.Add(1)
		tenantID, indexNames, ok := compiled.ReadScope()
		if !ok {
			return errors.New("compiled export query has no read scope")
		}
		_, release, err := registry.Acquire(ctx, tenantID, indexNames)
		if err != nil {
			return err
		}
		defer release()
		nativeWork.Add(1)
		return nil
	})
	source := newReexecutionTestSource(t, searches, executor, nil)
	manager := newExportTestManager(t, source, nil)
	created, err := manager.Create(
		context.Background(),
		access,
		CreateRequest{SearchJobID: searches.job.ID, Format: FormatCSV},
	)
	if err != nil {
		t.Fatalf("Create(): %v", err)
	}

	failed := waitExportState(t, manager, access, created.ID, StateFailed)
	if failed.Failure == nil || failed.Failure.Code != FailureSourceUnavailable ||
		failed.Failure.Retryable || failed.Artifact != nil {
		t.Fatalf("already-retired-index export = %#v, want non-retryable source failure", failed)
	}
	if got := executorCalls.Load(); got != 1 {
		t.Fatalf("executor calls = %d, want 1 admission attempt", got)
	}
	if got := nativeWork.Load(); got != 0 {
		t.Fatalf("native executor work = %d, want 0 after admission rejection", got)
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
	for iteration := range 200 {
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
	var nilSearches *reexecutionTestSearches
	if _, err := NewReexecutionSource(ReexecutionSourceConfig{
		Searches: nilSearches,
		Executor: executor,
	}); err == nil {
		t.Fatal("NewReexecutionSource accepted a typed-nil search service")
	}
	var nilExecutor *nilReexecutionTestExecutor
	if _, err := NewReexecutionSource(ReexecutionSourceConfig{
		Searches: &reexecutionTestSearches{},
		Executor: nilExecutor,
	}); err == nil {
		t.Fatal("NewReexecutionSource accepted a typed-nil query executor")
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
	searches.acquireErr = nil
	searches.pin = (*reexecutionTestPin)(nil)
	if lease, err := source.AcquireResultsFor(context.Background(), access, searches.job.ID); lease != nil || !errors.Is(err, searchjobs.ErrResultsUnavailable) {
		t.Fatalf("AcquireResultsFor(typed-nil pin) = (%v, %v)", lease, err)
	}
	searches.acquireErr = searchjobs.ErrExpired
	if lease, err := source.AcquireResultsFor(context.Background(), access, searches.job.ID); lease != nil || !errors.Is(err, searchjobs.ErrExpired) {
		t.Fatalf("AcquireResultsFor(typed-nil pin with error) = (%v, %v)", lease, err)
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
			TimeRange: searchtime.Intent{
				Earliest: "-1h",
				Latest:   "now",
				Timezone: "UTC",
			},
			Earliest:         now.Add(-time.Hour),
			Latest:           now,
			IndexTimeCutoff:  now,
			VisibilityCutoff: 42,
			State:            searchjobs.StateCompleted,
			CreatedAt:        now.Add(-time.Second),
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
