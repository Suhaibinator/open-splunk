package searchanalysis

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/plan"
	"github.com/Suhaibinator/open-splunk/internal/queryexec"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
)

func TestServiceReturnsClippedContinuousTimelineFromImmutableSnapshot(t *testing.T) {
	snapshot := timelineTestSnapshot()
	searches := &fakeCompletedSearches{snapshot: snapshot}
	compiler := &fakeTimelineCompiler{}
	executor := &fakeTimelineExecutor{execute: func(_ context.Context, query clickhouse.CompiledTimeline) ([]queryexec.TimelineBucket, error) {
		buckets := make([]queryexec.TimelineBucket, query.Spec.BucketCount)
		for index := range buckets {
			buckets[index] = queryexec.TimelineBucket{
				AlignedStart: time.Unix(query.Spec.FirstBucket.Unix()+int64(index)*query.Spec.SpanSeconds, 0).UTC(),
				Count:        uint64(index + 1),
			}
		}
		return buckets, nil
	}}
	service := newTimelineTestService(t, Config{Searches: searches, Compiler: compiler, Executor: executor})
	maxBuckets := uint32(3)
	preferred := int64(60)
	access := searchjobs.AccessScope{TenantID: snapshot.TenantID, OwnerID: snapshot.OwnerID}

	result, err := service.Get(context.Background(), access, Request{
		SearchJobID: snapshot.ID,
		MaxBuckets:  &maxBuckets, PreferredBucketWidthSeconds: &preferred,
	})
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if result.BucketWidthSeconds != 60 || !result.Complete || len(result.Buckets) != 3 {
		t.Fatalf("result = %+v", result)
	}
	want := []Bucket{
		{Earliest: snapshot.Earliest, Latest: time.Date(2026, 7, 21, 8, 1, 0, 0, time.UTC), EventCount: 1, Partial: true},
		{Earliest: time.Date(2026, 7, 21, 8, 1, 0, 0, time.UTC), Latest: time.Date(2026, 7, 21, 8, 2, 0, 0, time.UTC), EventCount: 2},
		{Earliest: time.Date(2026, 7, 21, 8, 2, 0, 0, time.UTC), Latest: snapshot.Latest, EventCount: 3, Partial: true},
	}
	if !reflect.DeepEqual(result.Buckets, want) {
		t.Fatalf("buckets = %#v, want %#v", result.Buckets, want)
	}
	if searches.access != access || searches.id != snapshot.ID {
		t.Fatalf("snapshot lookup = (%+v, %q)", searches.access, searches.id)
	}
	if compiler.query == nil || !reflect.DeepEqual(compiler.spec, executor.query.Spec) {
		t.Fatalf("compiler/executor contract = query:%+v compiler:%+v executor:%+v", compiler.query, compiler.spec, executor.query.Spec)
	}
	scan := compiler.query.Operators[0].(*plan.Scan)
	if !reflect.DeepEqual(scan.Indexes, snapshot.EffectiveIndexes) || scan.VisibilityCutoff != snapshot.VisibilityCutoff {
		t.Fatalf("rebuilt immutable scan = %+v", scan)
	}
}

func TestServiceValidatesRequestsAndTimelineEligibilityBeforeExecution(t *testing.T) {
	snapshot := timelineTestSnapshot()
	tests := []struct {
		name    string
		request Request
		mutate  func(*searchjobs.ExecutionSnapshot)
		want    error
	}{
		{name: "missing ID", request: Request{}, want: ErrInvalidTimelineRequest},
		{name: "zero explicit buckets", request: Request{SearchJobID: snapshot.ID, MaxBuckets: uint32Pointer(0)}, want: ErrInvalidTimelineRequest},
		{name: "above service maximum", request: Request{SearchJobID: snapshot.ID, MaxBuckets: uint32Pointer(1_001)}, want: ErrInvalidTimelineRequest},
		{name: "zero explicit width", request: Request{SearchJobID: snapshot.ID, PreferredBucketWidthSeconds: int64Pointer(0)}, want: ErrInvalidTimelineRequest},
		{name: "substitute time", request: Request{SearchJobID: snapshot.ID}, mutate: func(snapshot *searchjobs.ExecutionSnapshot) {
			snapshot.SPL = `index=main | eval _time=_indextime`
		}, want: ErrTimelineUnsupported},
		{name: "transforming search", request: Request{SearchJobID: snapshot.ID}, mutate: func(snapshot *searchjobs.ExecutionSnapshot) {
			snapshot.SPL = `index=main | stats count`
		}, want: ErrTimelineUnsupported},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			candidate := snapshot
			if test.mutate != nil {
				test.mutate(&candidate)
			}
			compiler := &fakeTimelineCompiler{}
			executor := &fakeTimelineExecutor{}
			service := newTimelineTestService(t, Config{
				Searches: &fakeCompletedSearches{snapshot: candidate}, Compiler: compiler, Executor: executor,
			})
			_, err := service.Get(context.Background(), searchjobs.AccessScope{TenantID: candidate.TenantID, OwnerID: candidate.OwnerID}, test.request)
			if !errors.Is(err, test.want) {
				t.Fatalf("Get() error = %v, want %v", err, test.want)
			}
			if executor.calls != 0 {
				t.Fatalf("executor calls = %d, want 0", executor.calls)
			}
		})
	}
}

func TestServicePropagatesLifecycleStorageAndCancellationErrors(t *testing.T) {
	snapshot := timelineTestSnapshot()
	access := searchjobs.AccessScope{TenantID: snapshot.TenantID, OwnerID: snapshot.OwnerID}
	for _, operationErr := range []error{
		searchjobs.ErrNotFound,
		searchjobs.ErrResultsNotReady,
		searchjobs.ErrResultsUnavailable,
		searchjobs.ErrExpired,
		searchjobs.ErrClosed,
		context.Canceled,
	} {
		service := newTimelineTestService(t, Config{
			Searches: &fakeCompletedSearches{err: operationErr}, Compiler: &fakeTimelineCompiler{}, Executor: &fakeTimelineExecutor{},
		})
		_, err := service.Get(context.Background(), access, Request{SearchJobID: snapshot.ID})
		if !errors.Is(err, operationErr) {
			t.Errorf("Get(snapshot error %v) = %v", operationErr, err)
		}
	}

	storageErr := searchjobs.ErrStorageUnavailable
	service := newTimelineTestService(t, Config{
		Searches: &fakeCompletedSearches{snapshot: snapshot}, Compiler: &fakeTimelineCompiler{},
		Executor: &fakeTimelineExecutor{err: storageErr},
	})
	if _, err := service.Get(context.Background(), access, Request{SearchJobID: snapshot.ID}); !errors.Is(err, storageErr) {
		t.Fatalf("Get(storage error) = %v", err)
	}

	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := service.Get(canceled, access, Request{SearchJobID: snapshot.ID}); !errors.Is(err, context.Canceled) {
		t.Fatalf("Get(canceled) = %v", err)
	}
}

func TestServiceRejectsMalformedExecutorSequence(t *testing.T) {
	snapshot := timelineTestSnapshot()
	access := searchjobs.AccessScope{TenantID: snapshot.TenantID, OwnerID: snapshot.OwnerID}
	service := newTimelineTestService(t, Config{
		Searches: &fakeCompletedSearches{snapshot: snapshot}, Compiler: &fakeTimelineCompiler{},
		Executor: &fakeTimelineExecutor{execute: func(_ context.Context, query clickhouse.CompiledTimeline) ([]queryexec.TimelineBucket, error) {
			return []queryexec.TimelineBucket{{AlignedStart: query.Spec.FirstBucket.Add(time.Second), Count: 1}}, nil
		}},
	})
	if _, err := service.Get(context.Background(), access, Request{SearchJobID: snapshot.ID, MaxBuckets: uint32Pointer(1)}); !errors.Is(err, searchjobs.ErrInvalidResult) {
		t.Fatalf("Get(malformed executor) = %v, want ErrInvalidResult", err)
	}
}

func TestServiceClassifiesCompilerDiagnosticsForTheAPIBoundary(t *testing.T) {
	t.Parallel()

	snapshot := timelineTestSnapshot()
	access := searchjobs.AccessScope{TenantID: snapshot.TenantID, OwnerID: snapshot.OwnerID}
	for _, test := range []struct {
		name string
		code string
		want error
	}{
		{name: "wrapped query limit", code: "SPL_QUERY_TOO_COMPLEX", want: searchjobs.ErrExecutionLimit},
		{name: "timeline compatibility", code: "SPL_UNSUPPORTED_TIMELINE_TIME_FIELD", want: ErrTimelineUnsupported},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			service := newTimelineTestService(t, Config{
				Searches: &fakeCompletedSearches{snapshot: snapshot},
				Compiler: &fakeTimelineCompiler{err: &plan.Diagnostic{Code: test.code, Message: "bounded diagnostic"}},
				Executor: &fakeTimelineExecutor{},
			})
			if _, err := service.Get(context.Background(), access, Request{SearchJobID: snapshot.ID}); !errors.Is(err, test.want) {
				t.Fatalf("Get() error = %v, want %v", err, test.want)
			}
		})
	}
}

func TestServiceBoundsConcurrentExecutionsAndReleasesCapacity(t *testing.T) {
	snapshot := timelineTestSnapshot()
	access := searchjobs.AccessScope{TenantID: snapshot.TenantID, OwnerID: snapshot.OwnerID}
	entered := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	executor := &fakeTimelineExecutor{execute: func(ctx context.Context, query clickhouse.CompiledTimeline) ([]queryexec.TimelineBucket, error) {
		once.Do(func() { close(entered) })
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-release:
		}
		return timelineRows(query.Spec), nil
	}}
	service := newTimelineTestService(t, Config{
		Searches: &fakeCompletedSearches{snapshot: snapshot}, Compiler: &fakeTimelineCompiler{}, Executor: executor, MaxConcurrent: 1,
	})
	first := make(chan error, 1)
	go func() {
		_, err := service.Get(context.Background(), access, Request{SearchJobID: snapshot.ID})
		first <- err
	}()
	<-entered
	if _, err := service.Get(context.Background(), access, Request{SearchJobID: snapshot.ID}); !errors.Is(err, ErrTimelineCapacity) {
		t.Fatalf("second Get() = %v, want ErrTimelineCapacity", err)
	}
	close(release)
	if err := <-first; err != nil {
		t.Fatalf("first Get() error = %v", err)
	}
	if _, err := service.Get(context.Background(), access, Request{SearchJobID: snapshot.ID}); err != nil {
		t.Fatalf("Get(after release) error = %v", err)
	}
}

func TestNewValidatesDependenciesAndExposesEnforcedMaximum(t *testing.T) {
	valid := Config{Searches: &fakeCompletedSearches{}, Compiler: &fakeTimelineCompiler{}, Executor: &fakeTimelineExecutor{}}
	if _, err := New(Config{}); err == nil {
		t.Fatal("New(empty) error = nil")
	}
	var typedNil *fakeTimelineExecutor
	invalid := valid
	invalid.Executor = typedNil
	if _, err := New(invalid); err == nil {
		t.Fatal("New(typed nil executor) error = nil")
	}
	valid.MaximumBuckets = 27
	valid.DefaultMaxBuckets = 13
	service := newTimelineTestService(t, valid)
	if service.MaximumBuckets() != 27 {
		t.Fatalf("MaximumBuckets() = %d, want 27", service.MaximumBuckets())
	}
}

func TestNilServiceGetReturnsError(t *testing.T) {
	t.Parallel()

	var service *Service
	if _, err := service.Get(context.Background(), searchjobs.AccessScope{}, Request{SearchJobID: "search-1"}); err == nil {
		t.Fatal("nil Service.Get() error = nil")
	}
}

type fakeCompletedSearches struct {
	snapshot searchjobs.ExecutionSnapshot
	err      error
	access   searchjobs.AccessScope
	id       string
}

func (searches *fakeCompletedSearches) CompletedExecutionSnapshotFor(_ context.Context, access searchjobs.AccessScope, id string) (searchjobs.ExecutionSnapshot, error) {
	searches.access = access
	searches.id = id
	return searches.snapshot, searches.err
}

type fakeTimelineCompiler struct {
	query *plan.Query
	spec  clickhouse.TimelineSpec
	err   error
}

func (compiler *fakeTimelineCompiler) CompileTimeline(query *plan.Query, spec clickhouse.TimelineSpec) (clickhouse.CompiledTimeline, error) {
	compiler.query = query
	compiler.spec = spec
	return clickhouse.CompiledTimeline{SQL: "SELECT timeline", Spec: spec}, compiler.err
}

type fakeTimelineExecutor struct {
	mu      sync.Mutex
	query   clickhouse.CompiledTimeline
	err     error
	execute func(context.Context, clickhouse.CompiledTimeline) ([]queryexec.TimelineBucket, error)
	calls   int
}

func (executor *fakeTimelineExecutor) ExecuteTimeline(ctx context.Context, query clickhouse.CompiledTimeline) ([]queryexec.TimelineBucket, error) {
	executor.mu.Lock()
	executor.calls++
	executor.query = query
	execute := executor.execute
	err := executor.err
	executor.mu.Unlock()
	if execute != nil {
		return execute(ctx, query)
	}
	if err != nil {
		return nil, err
	}
	return timelineRows(query.Spec), nil
}

func timelineRows(spec clickhouse.TimelineSpec) []queryexec.TimelineBucket {
	result := make([]queryexec.TimelineBucket, spec.BucketCount)
	for index := range result {
		result[index].AlignedStart = time.Unix(spec.FirstBucket.Unix()+int64(index)*spec.SpanSeconds, 0).UTC()
	}
	return result
}

func timelineTestSnapshot() searchjobs.ExecutionSnapshot {
	return searchjobs.ExecutionSnapshot{
		ID: "search-1", OwnerID: "owner-1", TenantID: "tenant-1", SPL: `index=main level=error`,
		EffectiveIndexes: []string{"main"},
		Earliest:         time.Date(2026, 7, 21, 8, 0, 30, 123, time.UTC),
		Latest:           time.Date(2026, 7, 21, 8, 2, 0, 1, time.UTC),
		IndexTimeCutoff:  time.Date(2026, 7, 21, 8, 3, 0, 0, time.UTC),
		VisibilityCutoff: 19,
		FinishedAt:       time.Date(2026, 7, 21, 8, 3, 1, 0, time.UTC),
		ExpiresAt:        time.Date(2026, 7, 21, 8, 18, 1, 0, time.UTC),
	}
}

func newTimelineTestService(t *testing.T, config Config) *Service {
	t.Helper()
	service, err := New(config)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	return service
}

func uint32Pointer(value uint32) *uint32 { return &value }
func int64Pointer(value int64) *int64    { return &value }
