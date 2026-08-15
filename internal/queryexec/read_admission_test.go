package queryexec

import (
	"context"
	"errors"
	"reflect"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/indexread"
	"github.com/Suhaibinator/open-splunk/internal/plan"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
	"github.com/Suhaibinator/open-splunk/internal/searchtime"
	"github.com/Suhaibinator/open-splunk/internal/spl"
)

func TestExecutorReadAdmissionRejectsTamperedScopeBeforeConnection(t *testing.T) {
	t.Parallel()

	compiled := compileReadAdmissionQuery(t, `index=target | stats count`)
	compiled.Args = slices.Clone(compiled.Args)
	mutated := false
	for index, argument := range compiled.Args {
		if value, ok := argument.(string); ok && value == "tenant-read" {
			compiled.Args[index] = "other-tenant"
			mutated = true
			break
		}
	}
	if !mutated {
		t.Fatal("compiled fixture did not expose the tenant bind argument")
	}

	admission := &recordingReadAdmission{}
	connection := &fakeQueryConnection{err: errors.New("connection must not be called")}
	executor := mustExecutor(t, connection)
	executor.readAdmission = admission
	err := executor.Execute(context.Background(), compiled, &fakeSink{})
	if !errors.Is(err, searchjobs.ErrInvalidResult) {
		t.Fatalf("Execute error = %v, want ErrInvalidResult", err)
	}
	if admission.acquireCalls.Load() != 0 || connection.query != "" {
		t.Fatalf("tampered query reached admission/connection: acquire=%d query=%q", admission.acquireCalls.Load(), connection.query)
	}
}

func TestExecutorRejectsTamperedNonReadArgumentBeforeAdmissionOrConnection(t *testing.T) {
	t.Parallel()

	compiled := compileReadAdmissionQuery(t, `index=target status="non-read-literal" | table status`)
	compiled.Args = slices.Clone(compiled.Args)
	mutated := false
	for index, argument := range compiled.Args {
		if value, ok := argument.(string); ok && value == "non-read-literal" {
			compiled.Args[index] = "tampered-filter"
			mutated = true
			break
		}
	}
	if !mutated {
		t.Fatal("compiled fixture did not expose the filter bind argument")
	}
	if _, _, ok := compiled.ReadScope(); !ok {
		t.Fatal("non-read mutation unexpectedly invalidated the older read-scope seal")
	}

	admission := &recordingReadAdmission{}
	connection := &fakeQueryConnection{err: errors.New("connection must not be called")}
	executor := mustExecutor(t, connection)
	executor.readAdmission = admission
	err := executor.Execute(context.Background(), compiled, &fakeSink{})
	if !errors.Is(err, searchjobs.ErrInvalidResult) {
		t.Fatalf("Execute error = %v, want ErrInvalidResult", err)
	}
	if admission.acquireCalls.Load() != 0 || connection.query != "" {
		t.Fatalf("tampered non-read argument reached admission/connection: acquire=%d query=%q", admission.acquireCalls.Load(), connection.query)
	}
}

func TestSearchDerivedExecutorsRejectTamperedNonReadArgumentsBeforeAdmission(t *testing.T) {
	t.Parallel()

	const literal = "derived-non-read-literal"
	compiler := clickhouse.Compiler{}
	build := func() *plan.Query {
		return buildReadAdmissionPlan(t, `index=target host="`+literal+`"`)
	}

	timelinePlan := build()
	timelineScan := timelinePlan.Operators[0].(*plan.Scan)
	timeline, err := compiler.CompileTimeline(timelinePlan, clickhouse.TimelineSpec{
		FirstBucket: timelineScan.Earliest,
		SpanSeconds: 60,
		BucketCount: 60,
		Earliest:    timelineScan.Earliest,
		Latest:      timelineScan.Latest,
	})
	if err != nil {
		t.Fatalf("CompileTimeline: %v", err)
	}
	catalog, err := compiler.CompileFieldCatalog(
		build(),
		clickhouse.FieldCatalogSpec{MaximumFields: 10},
	)
	if err != nil {
		t.Fatalf("CompileFieldCatalog: %v", err)
	}
	summary, err := compiler.CompileFieldSummary(
		build(),
		clickhouse.FieldSummarySpec{
			FieldName:             "host",
			MaximumValues:         10,
			MaximumDistinctValues: 100,
			MaximumValueBytes:     1_024,
		},
	)
	if err != nil {
		t.Fatalf("CompileFieldSummary: %v", err)
	}
	suggestions, err := compiler.CompileFieldSuggestions(
		build(),
		clickhouse.FieldSuggestionSpec{Prefix: "ho", MaximumFields: 10},
	)
	if err != nil {
		t.Fatalf("CompileFieldSuggestions: %v", err)
	}

	mutateArgs := func(args []any) []any {
		t.Helper()
		mutated := slices.Clone(args)
		for index, argument := range mutated {
			if value, ok := argument.(string); ok && value == literal {
				mutated[index] = "tampered-derived-literal"
				return mutated
			}
		}
		t.Fatal("compiled derived fixture did not expose the non-read literal")
		return nil
	}
	timeline.Args = mutateArgs(timeline.Args)
	catalog.Args = mutateArgs(catalog.Args)
	summary.Args = mutateArgs(summary.Args)
	suggestions.Args = mutateArgs(suggestions.Args)

	for name, query := range map[string]interface {
		ReadScope() (string, []string, bool)
	}{
		"timeline":          timeline,
		"field catalog":     catalog,
		"field summary":     summary,
		"field suggestions": suggestions,
	} {
		if _, _, ok := query.ReadScope(); !ok {
			t.Fatalf("%s non-read mutation invalidated the older read-scope seal", name)
		}
	}

	tests := []struct {
		name    string
		execute func(*Executor) error
	}{
		{name: "timeline", execute: func(executor *Executor) error {
			_, executeErr := executor.ExecuteTimeline(context.Background(), timeline)
			return executeErr
		}},
		{name: "field catalog", execute: func(executor *Executor) error {
			_, executeErr := executor.ExecuteFieldCatalog(context.Background(), catalog)
			return executeErr
		}},
		{name: "field summary", execute: func(executor *Executor) error {
			_, executeErr := executor.ExecuteFieldSummary(context.Background(), summary)
			return executeErr
		}},
		{name: "field suggestions", execute: func(executor *Executor) error {
			_, executeErr := executor.ExecuteFieldSuggestions(context.Background(), suggestions)
			return executeErr
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			admission := &recordingReadAdmission{}
			connection := &fakeQueryConnection{err: errors.New("connection must not be called")}
			executor := mustExecutor(t, connection)
			executor.readAdmission = admission
			var queryIDCalls atomic.Int32
			executor.newQueryID = func() (string, error) {
				queryIDCalls.Add(1)
				return "must-not-be-created", nil
			}

			err := test.execute(executor)
			if !errors.Is(err, searchjobs.ErrInvalidResult) {
				t.Fatalf("execution error = %v, want ErrInvalidResult", err)
			}
			if admission.acquireCalls.Load() != 0 || queryIDCalls.Load() != 0 || connection.query != "" {
				t.Fatalf(
					"tampered derived query reached resources: acquire=%d queryIDs=%d query=%q",
					admission.acquireCalls.Load(),
					queryIDCalls.Load(),
					connection.query,
				)
			}
		})
	}
}

func TestExecutorReadAdmissionRejectsRetiredScopeBeforeConnection(t *testing.T) {
	t.Parallel()

	registry := indexread.NewRegistry()
	if err := registry.Retire(context.Background(), "tenant-read", "target"); err != nil {
		t.Fatalf("Retire: %v", err)
	}
	connection := &fakeQueryConnection{err: errors.New("connection must not be called")}
	executor := mustExecutor(t, connection)
	executor.readAdmission = registry
	err := executor.Execute(
		context.Background(),
		compileReadAdmissionQuery(t, `index=target | stats count`),
		&fakeSink{},
	)
	if !errors.Is(err, indexread.ErrUnavailable) {
		t.Fatalf("Execute error = %v, want ErrUnavailable", err)
	}
	if !errors.Is(err, searchjobs.ErrStorageUnavailable) {
		t.Fatalf("Execute error = %v, want storage-unavailable HTTP classification", err)
	}
	if connection.query != "" {
		t.Fatalf("retired query reached connection: %q", connection.query)
	}
}

func TestExecutorReadAdmissionRejectsTypedNilDependency(t *testing.T) {
	t.Parallel()

	var admission *recordingReadAdmission
	connection := &fakeQueryConnection{err: errors.New("connection must not be called")}
	executor := mustExecutor(t, connection)
	executor.readAdmission = admission
	err := executor.Execute(
		context.Background(),
		compileReadAdmissionQuery(t, `index=target | stats count`),
		&fakeSink{},
	)
	if err == nil || !strings.Contains(err.Error(), "read admission is nil") {
		t.Fatalf("Execute error = %v, want typed-nil rejection", err)
	}
	if connection.query != "" {
		t.Fatalf("typed-nil admission reached connection: %q", connection.query)
	}
}

func TestNewRequiresReadAdmission(t *testing.T) {
	t.Parallel()

	var admission *recordingReadAdmission
	for _, test := range []struct {
		name      string
		admission indexread.Admission
	}{
		{name: "nil"},
		{name: "typed nil", admission: admission},
	} {
		t.Run(test.name, func(t *testing.T) {
			executor, err := New(
				&readAdmissionDriverConnection{},
				Config{ReadAdmission: test.admission},
			)
			if err == nil || executor != nil {
				t.Fatalf("New(read admission) = (%v, %v), want nil and error", executor, err)
			}
		})
	}
}

func TestExecutorReadAdmissionPreservesRetirementCancellationCause(t *testing.T) {
	t.Parallel()

	transportFailure := errors.New("driver returned a transport failure after cancellation")
	for _, test := range []struct {
		name      string
		returnErr error
	}{
		{name: "driver context cancellation"},
		{name: "driver transport failure", returnErr: transportFailure},
	} {
		t.Run(test.name, func(t *testing.T) {
			registry := indexread.NewRegistry()
			connection := &blockingReadQueryConnection{
				entered:   make(chan struct{}),
				returnErr: test.returnErr,
			}
			executor := mustExecutor(t, connection)
			executor.readAdmission = registry

			executionDone := make(chan error, 1)
			go func() {
				executionDone <- executor.Execute(
					context.Background(),
					compileReadAdmissionQuery(t, `index=target | stats count`),
					&fakeSink{},
				)
			}()
			select {
			case <-connection.entered:
			case <-time.After(5 * time.Second):
				t.Fatal("query did not enter the connection")
			}

			retirementDone := make(chan error, 1)
			go func() {
				retirementDone <- registry.Retire(context.Background(), "tenant-read", "target")
			}()
			select {
			case err := <-executionDone:
				if !errors.Is(err, indexread.ErrUnavailable) {
					t.Fatalf("Execute error = %v, want retirement cause", err)
				}
				if !errors.Is(err, searchjobs.ErrStorageUnavailable) {
					t.Fatalf("Execute error = %v, want storage-unavailable classification", err)
				}
				if test.returnErr != nil && (errors.Is(err, test.returnErr) ||
					strings.Contains(err.Error(), test.returnErr.Error())) {
					t.Fatalf("Execute error = %v retained unsafe driver detail", err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("retirement did not cancel active query")
			}
			select {
			case err := <-retirementDone:
				if err != nil {
					t.Fatalf("Retire: %v", err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("retirement did not observe query lease release")
			}
		})
	}
}

func TestExecutorReadAdmissionReleasesEveryAcquiredPath(t *testing.T) {
	t.Parallel()

	compiled := compileReadAdmissionQuery(t, `index=target | stats count`)
	tests := []struct {
		name      string
		configure func(*Executor, *fakeQueryConnection)
		wantError bool
	}{
		{
			name: "query ID failure",
			configure: func(executor *Executor, _ *fakeQueryConnection) {
				executor.newQueryID = func() (string, error) { return "", errors.New("entropy unavailable") }
			},
			wantError: true,
		},
		{
			name: "connection failure",
			configure: func(_ *Executor, connection *fakeQueryConnection) {
				connection.err = errors.New("query failed")
			},
			wantError: true,
		},
		{
			name: "success",
			configure: func(_ *Executor, connection *fakeQueryConnection) {
				connection.rows = readAdmissionCountRows(3)
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			admission := &recordingReadAdmission{}
			connection := &fakeQueryConnection{}
			executor := mustExecutor(t, connection)
			executor.readAdmission = admission
			test.configure(executor, connection)
			err := executor.Execute(context.Background(), compiled, &fakeSink{})
			if (err != nil) != test.wantError {
				t.Fatalf("Execute error = %v, wantError=%v", err, test.wantError)
			}
			if admission.acquireCalls.Load() != 1 || admission.releaseCalls.Load() != 1 {
				t.Fatalf("admission calls = acquire %d release %d, want 1/1", admission.acquireCalls.Load(), admission.releaseCalls.Load())
			}
			if admission.tenantID != "tenant-read" || !slices.Equal(admission.indexNames, []string{"target"}) {
				t.Fatalf("admitted scope = %q %v", admission.tenantID, admission.indexNames)
			}
		})
	}
}

func TestSearchDerivedExecutorsShareReadAdmission(t *testing.T) {
	t.Parallel()

	logical := buildReadAdmissionPlan(t, `index=target`)
	compiler := clickhouse.Compiler{}
	scan := logical.Operators[0].(*plan.Scan)
	timeline, err := compiler.CompileTimeline(logical, clickhouse.TimelineSpec{
		FirstBucket: scan.Earliest,
		SpanSeconds: 60,
		BucketCount: 60,
		Earliest:    scan.Earliest,
		Latest:      scan.Latest,
	})
	if err != nil {
		t.Fatalf("CompileTimeline: %v", err)
	}
	catalog, err := compiler.CompileFieldCatalog(
		buildReadAdmissionPlan(t, `index=target`),
		clickhouse.FieldCatalogSpec{MaximumFields: 10},
	)
	if err != nil {
		t.Fatalf("CompileFieldCatalog: %v", err)
	}
	summary, err := compiler.CompileFieldSummary(
		buildReadAdmissionPlan(t, `index=target`),
		clickhouse.FieldSummarySpec{
			FieldName:             "level",
			MaximumValues:         10,
			MaximumDistinctValues: 100,
			MaximumValueBytes:     1_024,
		},
	)
	if err != nil {
		t.Fatalf("CompileFieldSummary: %v", err)
	}
	suggestions, err := compiler.CompileFieldSuggestions(
		buildReadAdmissionPlan(t, `index=target`),
		clickhouse.FieldSuggestionSpec{MaximumFields: 10},
	)
	if err != nil {
		t.Fatalf("CompileFieldSuggestions: %v", err)
	}

	tests := []struct {
		name    string
		execute func(*Executor) error
	}{
		{name: "timeline", execute: func(executor *Executor) error {
			_, executeErr := executor.ExecuteTimeline(context.Background(), timeline)
			return executeErr
		}},
		{name: "field catalog", execute: func(executor *Executor) error {
			_, executeErr := executor.ExecuteFieldCatalog(context.Background(), catalog)
			return executeErr
		}},
		{name: "field summary", execute: func(executor *Executor) error {
			_, executeErr := executor.ExecuteFieldSummary(context.Background(), summary)
			return executeErr
		}},
		{name: "field suggestions", execute: func(executor *Executor) error {
			_, executeErr := executor.ExecuteFieldSuggestions(context.Background(), suggestions)
			return executeErr
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			admission := &recordingReadAdmission{}
			connection := &fakeQueryConnection{err: errors.New("stop after admission")}
			executor := mustExecutor(t, connection)
			executor.readAdmission = admission
			if err := test.execute(executor); err == nil {
				t.Fatal("execution unexpectedly succeeded")
			}
			if admission.acquireCalls.Load() != 1 || admission.releaseCalls.Load() != 1 {
				t.Fatalf("admission calls = acquire %d release %d, want 1/1", admission.acquireCalls.Load(), admission.releaseCalls.Load())
			}
		})
	}

	for _, test := range tests {
		t.Run(test.name+" unavailable classification", func(t *testing.T) {
			registry := indexread.NewRegistry()
			if err := registry.Retire(context.Background(), "tenant-read", "target"); err != nil {
				t.Fatalf("Retire: %v", err)
			}
			connection := &fakeQueryConnection{err: errors.New("connection must not be called")}
			executor := mustExecutor(t, connection)
			executor.readAdmission = registry
			err := test.execute(executor)
			if !errors.Is(err, indexread.ErrUnavailable) ||
				!errors.Is(err, searchjobs.ErrStorageUnavailable) {
				t.Fatalf("execution error = %v, want unavailable and storage classifications", err)
			}
			if connection.query != "" {
				t.Fatalf("retired derived query reached connection: %q", connection.query)
			}
		})
	}
}

func TestQueuedSearchFailsExplicitlyWhenIndexRetiresBeforeExecution(t *testing.T) {
	registry := indexread.NewRegistry()
	connection := &queuedReadQueryConnection{
		firstEntered: make(chan struct{}),
		releaseFirst: make(chan struct{}),
	}
	executor := mustExecutor(t, connection)
	executor.readAdmission = registry

	var nextID atomic.Int32
	manager, err := searchjobs.New(searchjobs.Config{
		Executor:        executor,
		Snapshotter:     constantReadSnapshotter(9),
		Compiler:        clickhouse.Compiler{},
		MaxConcurrent:   1,
		MaxQueued:       2,
		CleanupInterval: -1,
		Now: func() time.Time {
			return time.Date(2026, time.January, 1, 2, 0, 0, 0, time.UTC)
		},
		NewID: func() string {
			if nextID.Add(1) == 1 {
				return "read-fence-job-a"
			}
			return "read-fence-job-b"
		},
		CursorKey: []byte("0123456789abcdef0123456789abcdef"),
	})
	if err != nil {
		t.Fatalf("searchjobs.New: %v", err)
	}
	defer func() {
		connection.releaseOnce.Do(func() { close(connection.releaseFirst) })
		if closeErr := manager.Close(); closeErr != nil {
			t.Errorf("manager.Close: %v", closeErr)
		}
	}()

	earliest := time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)
	resolvedRange, err := searchtime.NewAbsoluteRange(earliest, earliest.Add(time.Hour))
	if err != nil {
		t.Fatalf("NewAbsoluteRange: %v", err)
	}
	request := searchjobs.CreateRequest{
		OwnerID:           "owner",
		TenantID:          "tenant-read",
		AuthorizedIndexes: []string{"other", "target"},
		TimeRange:         resolvedRange,
		Source:            searchjobs.JobSource{Origin: searchjobs.JobOriginAdHoc},
	}
	request.SPL = `index=other | stats count`
	request.RequestedIndexes = []string{"other"}
	jobA, err := manager.Create(context.Background(), request)
	if err != nil {
		t.Fatalf("Create job A: %v", err)
	}
	select {
	case <-connection.firstEntered:
	case <-time.After(5 * time.Second):
		t.Fatal("job A did not block in ClickHouse")
	}

	request.SPL = `index=target | stats count`
	request.RequestedIndexes = []string{"target"}
	jobB, err := manager.Create(context.Background(), request)
	if err != nil {
		t.Fatalf("Create job B: %v", err)
	}
	queuedB, err := manager.Get(jobB.ID)
	if err != nil {
		t.Fatalf("Get queued job B: %v", err)
	}
	if queuedB.State != searchjobs.StateQueued {
		t.Fatalf("job B state = %v, want queued", queuedB.State)
	}

	if err := registry.Retire(context.Background(), "tenant-read", "target"); err != nil {
		t.Fatalf("Retire target: %v", err)
	}
	connection.releaseOnce.Do(func() { close(connection.releaseFirst) })
	terminalA := waitReadAdmissionJobTerminal(t, manager, jobA.ID)
	if terminalA.State != searchjobs.StateCompleted {
		t.Fatalf("job A state = %v, failure=%#v", terminalA.State, terminalA.Failure)
	}
	terminalB := waitReadAdmissionJobTerminal(t, manager, jobB.ID)
	if terminalB.State != searchjobs.StateFailed || terminalB.Failure == nil ||
		terminalB.Failure.Code != searchjobs.FailureExecution {
		t.Fatalf("job B = state %v failure %#v, want explicit unavailable failure", terminalB.State, terminalB.Failure)
	}
	if calls := connection.queryCalls.Load(); calls != 1 {
		t.Fatalf("ClickHouse query calls = %d, want only job A", calls)
	}
}

type recordingReadAdmission struct {
	acquireCalls atomic.Int32
	releaseCalls atomic.Int32

	mu         sync.Mutex
	tenantID   string
	indexNames []string
}

type readAdmissionDriverConnection struct {
	driver.Conn
}

func (admission *recordingReadAdmission) Acquire(
	ctx context.Context,
	tenantID string,
	indexNames []string,
) (context.Context, func(), error) {
	admission.acquireCalls.Add(1)
	admission.mu.Lock()
	admission.tenantID = tenantID
	admission.indexNames = slices.Clone(indexNames)
	admission.mu.Unlock()
	return ctx, func() { admission.releaseCalls.Add(1) }, nil
}

type blockingReadQueryConnection struct {
	entered   chan struct{}
	returnErr error
	once      sync.Once
}

type queuedReadQueryConnection struct {
	queryCalls   atomic.Int32
	firstEntered chan struct{}
	releaseFirst chan struct{}
	enteredOnce  sync.Once
	releaseOnce  sync.Once
}

func (connection *queuedReadQueryConnection) Query(
	ctx context.Context,
	_ string,
	_ ...any,
) (driver.Rows, error) {
	call := connection.queryCalls.Add(1)
	if call == 1 {
		connection.enteredOnce.Do(func() { close(connection.firstEntered) })
		select {
		case <-connection.releaseFirst:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return readAdmissionCountRows(1), nil
}

type constantReadSnapshotter uint64

func (snapshotter constantReadSnapshotter) VisibilityCutoff(context.Context) (uint64, error) {
	return uint64(snapshotter), nil
}

func (connection *blockingReadQueryConnection) Query(
	ctx context.Context,
	_ string,
	_ ...any,
) (driver.Rows, error) {
	connection.once.Do(func() { close(connection.entered) })
	<-ctx.Done()
	if connection.returnErr != nil {
		return nil, connection.returnErr
	}
	return nil, ctx.Err()
}

func readAdmissionCountRows(count uint64) *fakeRows {
	return &fakeRows{
		columns: []string{"count"},
		types: []driver.ColumnType{
			fakeColumnType{name: "count", databaseType: "UInt64", scanType: reflect.TypeFor[uint64]()},
		},
		data: [][]any{{count}},
	}
}

func compileReadAdmissionQuery(t *testing.T, source string) clickhouse.CompiledQuery {
	t.Helper()
	compiled, err := (clickhouse.Compiler{}).Compile(buildReadAdmissionPlan(t, source))
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	return compiled
}

func buildReadAdmissionPlan(t *testing.T, source string) *plan.Query {
	t.Helper()
	parsed, err := spl.Parse(source)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	earliest := time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)
	visibilityCutoff := uint64(7)
	logical, err := plan.Build(parsed, plan.Scope{
		TenantID:          "tenant-read",
		AuthorizedIndexes: []string{"target"},
		Earliest:          earliest,
		Latest:            earliest.Add(time.Hour),
		SearchStart:       earliest.Add(2 * time.Hour),
		SearchTimezone:    "UTC",
		IndexTimeCutoff:   earliest.Add(2 * time.Hour),
		VisibilityCutoff:  &visibilityCutoff,
	})
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	return logical
}

func waitReadAdmissionJobTerminal(t *testing.T, manager *searchjobs.Manager, id string) searchjobs.Job {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		job, err := manager.Get(id)
		if err != nil {
			t.Fatalf("Get job %q: %v", id, err)
		}
		if job.State.Terminal() {
			return job
		}
		if time.Now().After(deadline) {
			t.Fatalf("job %q did not terminate (state %v)", id, job.State)
		}
		time.Sleep(time.Millisecond)
	}
}
