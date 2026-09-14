package queryexec

import (
	"context"
	"errors"
	"reflect"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/indexread"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
)

func TestStagedTimechartUsesOneReadAdmissionLease(t *testing.T) {
	first := time.Date(2026, time.September, 1, 0, 0, 0, 0, time.UTC)
	compiled := compileTerminalTimechartFixture(t, first, first.Add(3*time.Second))
	prefixRows := func() driver.Rows {
		return timechartOrdinalRows([]string{"0:api"}, [][]uint64{{1}})
	}
	terminalRows := func() driver.Rows {
		return fixedTimechartOrdinalRows([]uint64{1, 0, 0})
	}

	t.Run("success", func(t *testing.T) {
		admission := &recordingReadAdmission{}
		connection := &stagedAdmissionQueueConnection{results: []stagedAdmissionQueryResult{
			{rows: prefixRows()},
			{rows: terminalRows()},
		}}
		sink := &terminalTimechartSink{}
		executor := mustExecutor(t, connection)
		executor.readAdmission = admission

		if err := executor.Execute(context.Background(), compiled, sink); err != nil {
			t.Fatal(err)
		}
		assertStagedAdmissionCounts(t, admission, connection.calls.Load(), 2)
		if sink.setCalls != 1 || len(sink.rows) != 3 {
			t.Fatalf("success published schema/rows = %d/%d, want 1/3", sink.setCalls, len(sink.rows))
		}
	})

	t.Run("suffix error", func(t *testing.T) {
		terminalFailure := errors.New("terminal stage failed")
		admission := &recordingReadAdmission{}
		connection := &stagedAdmissionQueueConnection{results: []stagedAdmissionQueryResult{
			{rows: prefixRows()},
			{err: terminalFailure},
		}}
		sink := &terminalTimechartSink{}
		executor := mustExecutor(t, connection)
		executor.readAdmission = admission

		err := executor.Execute(context.Background(), compiled, sink)
		if !errors.Is(err, terminalFailure) {
			t.Fatalf("Execute error = %v, want terminal failure", err)
		}
		assertStagedAdmissionCounts(t, admission, connection.calls.Load(), 2)
		assertStagedAdmissionPublishedNothing(t, sink)
	})

	t.Run("caller cancellation", func(t *testing.T) {
		admission := &recordingReadAdmission{}
		connection := newStagedAdmissionBlockingConnection(prefixRows())
		sink := &terminalTimechartSink{}
		executor := mustExecutor(t, connection)
		executor.readAdmission = admission
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() {
			done <- executor.Execute(ctx, compiled, sink)
		}()

		waitStagedAdmissionSignal(t, connection.suffixEntered, "suffix query")
		cancel()
		waitStagedAdmissionSignal(t, connection.suffixCanceled, "suffix cancellation")
		close(connection.allowSuffixReturn)
		if err := waitStagedAdmissionResult(t, done, "canceled execution"); !errors.Is(err, context.Canceled) {
			t.Fatalf("Execute error = %v, want context.Canceled", err)
		}
		assertStagedAdmissionCounts(t, admission, connection.calls.Load(), 2)
		assertStagedAdmissionPublishedNothing(t, sink)
	})
}

func TestStagedTimechartReadLeaseSurvivesCallerMutation(t *testing.T) {
	first := time.Date(2026, time.September, 1, 0, 0, 0, 0, time.UTC)
	compiled := compileTerminalTimechartFixture(t, first, first.Add(3*time.Second))
	admission := &stagedAdmissionBlockingAcquire{
		entered: make(chan struct{}),
		proceed: make(chan struct{}),
	}
	connection := &stagedAdmissionQueueConnection{results: []stagedAdmissionQueryResult{
		{rows: timechartOrdinalRows([]string{"0:api"}, [][]uint64{{1}})},
		{rows: fixedTimechartOrdinalRows([]uint64{1, 0, 0})},
	}}
	executor := mustExecutor(t, connection)
	executor.readAdmission = admission
	originalArgs := slices.Clone(compiled.Args)
	originalOutputFields := slices.Clone(compiled.OutputFields)
	sink := &terminalTimechartSink{}
	done := make(chan error, 1)
	go func(query clickhouse.CompiledQuery) {
		done <- executor.Execute(context.Background(), query, sink)
	}(compiled)

	waitStagedAdmissionSignal(t, admission.entered, "outer admission")
	for index := range compiled.Args {
		compiled.Args[index] = "caller mutation"
	}
	for index := range compiled.OutputFields {
		compiled.OutputFields[index] = "caller mutation"
	}
	if compiled.HasValidExecutionSeal() {
		t.Fatal("caller mutation did not invalidate the source query")
	}
	close(admission.proceed)
	if err := waitStagedAdmissionResult(t, done, "mutated-caller execution"); err != nil {
		t.Fatalf("detached staged execution observed caller mutation: %v", err)
	}
	if admission.acquireCalls.Load() != 1 || admission.releaseCalls.Load() != 1 {
		t.Fatalf("admission acquire/release = %d/%d, want 1/1", admission.acquireCalls.Load(), admission.releaseCalls.Load())
	}
	if connection.calls.Load() != 2 {
		t.Fatalf("physical query calls = %d, want 2", connection.calls.Load())
	}
	observedArgs := connection.firstQueryArgs()
	if !reflect.DeepEqual(observedArgs, originalArgs) {
		t.Fatalf("first physical query args = %#v, want detached original %#v", observedArgs, originalArgs)
	}
	if reflect.DeepEqual(compiled.OutputFields, originalOutputFields) || sink.setCalls != 1 || len(sink.rows) != 3 {
		t.Fatalf("caller output mutation was not isolated: caller=%#v original=%#v schema=%d rows=%d", compiled.OutputFields, originalOutputFields, sink.setCalls, len(sink.rows))
	}
}

func TestStagedTimechartRegistryLeaseCoversBlockedSuffix(t *testing.T) {
	first := time.Date(2026, time.September, 1, 0, 0, 0, 0, time.UTC)
	compiled := compileTerminalTimechartFixture(t, first, first.Add(3*time.Second))
	connection := newStagedAdmissionBlockingConnection(
		timechartOrdinalRows([]string{"0:api"}, [][]uint64{{1}}),
	)
	registry := indexread.NewRegistry()
	executor := mustExecutor(t, connection)
	executor.readAdmission = registry
	sink := &terminalTimechartSink{}
	executionDone := make(chan error, 1)
	go func() {
		executionDone <- executor.Execute(context.Background(), compiled, sink)
	}()
	waitStagedAdmissionSignal(t, connection.suffixEntered, "suffix query")

	retirementDone := make(chan error, 1)
	go func() {
		retirementDone <- registry.Retire(context.Background(), "tenant", "target")
	}()
	waitStagedAdmissionSignal(t, connection.suffixCanceled, "retirement cancellation")
	select {
	case err := <-retirementDone:
		t.Fatalf("Retire returned before suffix unwind and lease release: %v", err)
	default:
	}
	assertStagedAdmissionPublishedNothing(t, sink)

	close(connection.allowSuffixReturn)
	executeErr := waitStagedAdmissionResult(t, executionDone, "retired execution")
	if !errors.Is(executeErr, indexread.ErrUnavailable) || !errors.Is(executeErr, searchjobs.ErrStorageUnavailable) {
		t.Fatalf("Execute error = %v, want classified retirement cause", executeErr)
	}
	if err := waitStagedAdmissionResult(t, retirementDone, "retirement drain"); err != nil {
		t.Fatalf("Retire error = %v", err)
	}
	assertStagedAdmissionPublishedNothing(t, sink)
}

type stagedAdmissionQueryResult struct {
	rows driver.Rows
	err  error
}

type stagedAdmissionQueueConnection struct {
	results []stagedAdmissionQueryResult
	calls   atomic.Int32
	mu      sync.Mutex
	args    [][]any
}

func (connection *stagedAdmissionQueueConnection) Query(
	_ context.Context,
	_ string,
	args ...any,
) (driver.Rows, error) {
	call := int(connection.calls.Add(1)) - 1
	connection.mu.Lock()
	connection.args = append(connection.args, slices.Clone(args))
	connection.mu.Unlock()
	if call < 0 || call >= len(connection.results) {
		return nil, errors.New("unexpected staged query")
	}
	result := connection.results[call]
	return result.rows, result.err
}

func (connection *stagedAdmissionQueueConnection) firstQueryArgs() []any {
	connection.mu.Lock()
	defer connection.mu.Unlock()
	if len(connection.args) == 0 {
		return nil
	}
	return slices.Clone(connection.args[0])
}

type stagedAdmissionBlockingConnection struct {
	firstRows         driver.Rows
	calls             atomic.Int32
	suffixEntered     chan struct{}
	suffixCanceled    chan struct{}
	allowSuffixReturn chan struct{}
}

func newStagedAdmissionBlockingConnection(firstRows driver.Rows) *stagedAdmissionBlockingConnection {
	return &stagedAdmissionBlockingConnection{
		firstRows:         firstRows,
		suffixEntered:     make(chan struct{}),
		suffixCanceled:    make(chan struct{}),
		allowSuffixReturn: make(chan struct{}),
	}
}

func (connection *stagedAdmissionBlockingConnection) Query(
	ctx context.Context,
	_ string,
	_ ...any,
) (driver.Rows, error) {
	call := connection.calls.Add(1)
	if call == 1 {
		return connection.firstRows, nil
	}
	if call != 2 {
		return nil, errors.New("unexpected staged query")
	}
	close(connection.suffixEntered)
	<-ctx.Done()
	close(connection.suffixCanceled)
	<-connection.allowSuffixReturn
	return nil, ctx.Err()
}

type stagedAdmissionBlockingAcquire struct {
	acquireCalls atomic.Int32
	releaseCalls atomic.Int32
	entered      chan struct{}
	proceed      chan struct{}
	once         sync.Once
}

func (admission *stagedAdmissionBlockingAcquire) Acquire(
	ctx context.Context,
	_ string,
	_ []string,
) (context.Context, func(), error) {
	admission.acquireCalls.Add(1)
	admission.once.Do(func() { close(admission.entered) })
	select {
	case <-admission.proceed:
	case <-ctx.Done():
		return nil, nil, ctx.Err()
	}
	return ctx, func() { admission.releaseCalls.Add(1) }, nil
}

func assertStagedAdmissionCounts(
	t *testing.T,
	admission *recordingReadAdmission,
	queryCalls int32,
	wantQueries int32,
) {
	t.Helper()
	if admission.acquireCalls.Load() != 1 || admission.releaseCalls.Load() != 1 {
		t.Fatalf("admission acquire/release = %d/%d, want 1/1", admission.acquireCalls.Load(), admission.releaseCalls.Load())
	}
	if queryCalls != wantQueries {
		t.Fatalf("physical query calls = %d, want %d", queryCalls, wantQueries)
	}
}

func assertStagedAdmissionPublishedNothing(t *testing.T, sink *terminalTimechartSink) {
	t.Helper()
	if sink.compiledCalls != 0 || sink.setCalls != 0 || len(sink.rows) != 0 || len(sink.bounds) != 0 {
		t.Fatalf("staged failure published descriptor=%d schema=%d rows=%d bounds=%d", sink.compiledCalls, sink.setCalls, len(sink.rows), len(sink.bounds))
	}
}

func waitStagedAdmissionSignal(t *testing.T, signal <-chan struct{}, operation string) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for %s", operation)
	}
}

func waitStagedAdmissionResult(t *testing.T, result <-chan error, operation string) error {
	t.Helper()
	select {
	case err := <-result:
		return err
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for %s", operation)
		return nil
	}
}
