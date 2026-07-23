package queryexec

import (
	"context"
	"errors"
	"math"
	"math/bits"
	"reflect"
	"runtime"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	clickhousedriver "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
)

func TestExecutionProgressIsOptional(t *testing.T) {
	t.Parallel()

	parent := context.Background()
	driverContext, reporter, option := startExecutionProgress(parent, &fakeSink{})
	if driverContext != parent || reporter != nil || option != nil {
		t.Fatal("ordinary result sink unexpectedly enabled ClickHouse progress")
	}

	progressSink := &recordingProgressSink{}
	driverContext, reporter, option = startExecutionProgress(parent, progressSink)
	if driverContext == parent || reporter == nil || option == nil {
		t.Fatal("progress sink did not enable a derived context and ClickHouse progress option")
	}
	reporter.finish(parent, nil)
}

func TestExecutorExecuteAttachesAndForwardsClickHouseProgress(t *testing.T) {
	t.Parallel()

	rows := &fakeRows{
		columns: []string{"message"},
		types: []driver.ColumnType{
			fakeColumnType{name: "message", databaseType: "String", scanType: reflect.TypeOf("")},
		},
		data: [][]any{{"ready"}},
	}
	connection := &progressCallbackConnection{
		rows:     rows,
		progress: clickhousedriver.Progress{Rows: 17, Bytes: 170, TotalRows: 1000},
	}
	executor := mustExecutor(t, connection)
	executor.withProgress = connection.withProgress
	sink := &recordingProgressSink{}

	err := executor.Execute(context.Background(), clickhouse.CompiledQuery{
		SQL:          "SELECT message",
		OutputFields: []string{"message"},
	}, sink)
	if err != nil {
		t.Fatalf("Execute() = %v", err)
	}
	deltas := sink.snapshotDeltas()
	if !slices.Equal(deltas, []searchjobs.ExecutionProgressDelta{{ScannedRows: 17, ScannedBytes: 170}}) {
		t.Fatalf("reported deltas = %#v", deltas)
	}
	if sink.setCalls != 1 || len(sink.rows) != 1 {
		t.Fatalf("result publication = schema calls %d, rows %d", sink.setCalls, len(sink.rows))
	}

	// Even a connection that incorrectly retains the driver callback cannot
	// call the result sink after Execute returns.
	connection.callback(&clickhousedriver.Progress{Rows: 1})
	if got := sink.calls.Load(); got != 1 {
		t.Fatalf("ReportProgress calls after Execute = %d, want 1", got)
	}
}

func TestExecutorExecuteProgressFailureCancelsQueryAndWinsDriverError(t *testing.T) {
	t.Parallel()

	progressErr := errors.New("progress rejected")
	driverErr := errors.New("driver stopped")
	connection := &progressCallbackConnection{
		progress: clickhousedriver.Progress{Rows: 1},
		err:      driverErr,
	}
	executor := mustExecutor(t, connection)
	executor.withProgress = connection.withProgress
	sink := &recordingProgressSink{reportErr: progressErr}

	err := executor.Execute(context.Background(), clickhouse.CompiledQuery{
		SQL:          "SELECT message",
		OutputFields: []string{"message"},
	}, sink)
	if !errors.Is(err, progressErr) || errors.Is(err, driverErr) {
		t.Fatalf("Execute() = %v, want only progress error", err)
	}
	if !errors.Is(connection.contextErr, context.Canceled) {
		t.Fatalf("query context error after progress failure = %v", connection.contextErr)
	}
	if sink.setCalls != 0 || len(sink.rows) != 0 {
		t.Fatalf("progress failure partially published results: schema=%d rows=%d", sink.setCalls, len(sink.rows))
	}
}

func TestExecutorExecuteDoesNotAttachProgressForOrdinarySink(t *testing.T) {
	t.Parallel()

	rows := &fakeRows{
		columns: []string{"message"},
		types: []driver.ColumnType{
			fakeColumnType{name: "message", databaseType: "String", scanType: reflect.TypeOf("")},
		},
	}
	executor := mustExecutor(t, &fakeQueryConnection{rows: rows})
	var progressAttached atomic.Bool
	executor.withProgress = func(callback func(*clickhousedriver.Progress)) clickhousedriver.QueryOption {
		progressAttached.Store(true)
		return clickhousedriver.WithProgress(callback)
	}
	err := executor.Execute(context.Background(), clickhouse.CompiledQuery{
		SQL:          "SELECT message",
		OutputFields: []string{"message"},
	}, &fakeSink{})
	if err != nil {
		t.Fatalf("Execute() = %v", err)
	}
	if progressAttached.Load() {
		t.Fatal("ordinary result sink attached a ClickHouse progress callback")
	}
}

func TestExecutionProgressSerializesConcurrentPackets(t *testing.T) {
	t.Parallel()

	sink := &recordingProgressSink{yield: true}
	parent := context.Background()
	_, reporter, _ := startExecutionProgress(parent, sink)
	if reporter == nil {
		t.Fatal("progress reporter is nil")
	}

	const packets = 64
	start := make(chan struct{})
	var callbacks sync.WaitGroup
	callbacks.Add(packets)
	for packet := uint64(1); packet <= packets; packet++ {
		packet := packet
		go func() {
			defer callbacks.Done()
			<-start
			reporter.report(&clickhousedriver.Progress{Rows: packet, Bytes: packet * 10})
		}()
	}
	close(start)
	callbacks.Wait()
	if err := reporter.finish(parent, nil); err != nil {
		t.Fatalf("finish() = %v", err)
	}

	if got := sink.maximumActive.Load(); got != 1 {
		t.Fatalf("maximum concurrent ReportProgress calls = %d, want 1", got)
	}
	deltas := sink.snapshotDeltas()
	if len(deltas) != packets {
		t.Fatalf("reported packets = %d, want %d", len(deltas), packets)
	}
	slices.SortFunc(deltas, func(left, right searchjobs.ExecutionProgressDelta) int {
		switch {
		case left.ScannedRows < right.ScannedRows:
			return -1
		case left.ScannedRows > right.ScannedRows:
			return 1
		default:
			return 0
		}
	})
	for index, delta := range deltas {
		wantRows := uint64(index + 1)
		if delta.ScannedRows != wantRows || delta.ScannedBytes != wantRows*10 {
			t.Fatalf("packet %d = %#v", index, delta)
		}
	}
}

func TestExecutionProgressIgnoresEmptyPacketsAndCopiesReadDeltas(t *testing.T) {
	t.Parallel()

	sink := &recordingProgressSink{}
	parent := context.Background()
	_, reporter, _ := startExecutionProgress(parent, sink)
	reporter.report(nil)
	reporter.report(&clickhousedriver.Progress{})
	reporter.report(&clickhousedriver.Progress{Rows: 7, TotalRows: 900, WroteRows: 12, WroteBytes: 13})
	reporter.report(&clickhousedriver.Progress{Bytes: 11, Elapsed: time.Hour})
	if err := reporter.finish(parent, nil); err != nil {
		t.Fatalf("finish() = %v", err)
	}

	want := []searchjobs.ExecutionProgressDelta{
		{ScannedRows: 7},
		{ScannedBytes: 11},
	}
	if deltas := sink.snapshotDeltas(); !slices.Equal(deltas, want) {
		t.Fatalf("reported deltas = %#v, want %#v", deltas, want)
	}
}

func TestExecutionProgressFailureCancelsDriverAndWinsDriverError(t *testing.T) {
	t.Parallel()

	progressErr := errors.New("progress rejected")
	sink := &recordingProgressSink{reportErr: progressErr}
	parent := context.Background()
	driverContext, reporter, _ := startExecutionProgress(parent, sink)
	reporter.report(&clickhousedriver.Progress{Rows: 1, Bytes: 2})

	select {
	case <-driverContext.Done():
	default:
		t.Fatal("progress failure did not cancel the derived driver context")
	}
	driverErr := errors.New("driver observed cancellation")
	if got := reporter.finish(parent, driverErr); !errors.Is(got, progressErr) {
		t.Fatalf("finish() = %v, want progress error", got)
	}

	// The first error is sticky and the callback is permanently inert.
	reporter.report(&clickhousedriver.Progress{Rows: 3, Bytes: 4})
	if got := sink.calls.Load(); got != 1 {
		t.Fatalf("ReportProgress calls = %d, want 1", got)
	}
}

func TestExecutionProgressCallerCancellationHasHighestPrecedence(t *testing.T) {
	t.Parallel()

	progressErr := errors.New("progress rejected")
	sink := &recordingProgressSink{reportErr: progressErr}
	parent, cancel := context.WithCancel(context.Background())
	_, reporter, _ := startExecutionProgress(parent, sink)
	reporter.report(&clickhousedriver.Progress{Rows: 1})
	cancel()

	driverErr := errors.New("driver failed")
	if got := reporter.finish(parent, driverErr); !errors.Is(got, context.Canceled) ||
		errors.Is(got, progressErr) || errors.Is(got, driverErr) {
		t.Fatalf("finish() = %v, want only caller context cancellation", got)
	}
}

func TestExecutionProgressPreservesDriverErrorWithoutHigherPriorityFailure(t *testing.T) {
	t.Parallel()

	parent := context.Background()
	driverContext, reporter, _ := startExecutionProgress(parent, &recordingProgressSink{})
	driverErr := errors.New("driver failed")
	if got := reporter.finish(parent, driverErr); !errors.Is(got, driverErr) {
		t.Fatalf("finish() = %v, want driver error", got)
	}
	select {
	case <-driverContext.Done():
	default:
		t.Fatal("finish did not release derived driver context")
	}
}

func TestExecutionProgressRecoversSinkPanicWithoutLeakingValue(t *testing.T) {
	for _, panicValue := range []any{"storage-password", nil} {
		panicValue := panicValue
		name := "value"
		if panicValue == nil {
			name = "nil"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			parent := context.Background()
			driverContext, reporter, _ := startExecutionProgress(parent, &panicProgressSink{value: panicValue})
			reporter.report(&clickhousedriver.Progress{Rows: 1})

			select {
			case <-driverContext.Done():
			default:
				t.Fatal("progress panic did not cancel the derived driver context")
			}
			err := reporter.finish(parent, nil)
			if !errors.Is(err, searchjobs.ErrInvalidResult) {
				t.Fatalf("finish() = %v, want ErrInvalidResult", err)
			}
			if strings.Contains(err.Error(), "storage-password") {
				t.Fatalf("panic value leaked through error: %v", err)
			}
		})
	}
}

func TestExecutionProgressDelegatesOverflowToSink(t *testing.T) {
	t.Parallel()

	overflowErr := errors.New("progress overflow")
	sink := &overflowProgressSink{overflowErr: overflowErr}
	parent := context.Background()
	driverContext, reporter, _ := startExecutionProgress(parent, sink)
	reporter.report(&clickhousedriver.Progress{Rows: math.MaxUint64, Bytes: math.MaxUint64})
	reporter.report(&clickhousedriver.Progress{Rows: 1, Bytes: 1})

	if got := reporter.finish(parent, nil); !errors.Is(got, overflowErr) {
		t.Fatalf("finish() = %v, want sink overflow error", got)
	}
	select {
	case <-driverContext.Done():
	default:
		t.Fatal("sink overflow did not cancel driver context")
	}
	if sink.calls != 2 {
		t.Fatalf("ReportProgress calls = %d, want 2", sink.calls)
	}
	if sink.rows != math.MaxUint64 || sink.bytes != math.MaxUint64 {
		t.Fatalf("sink totals changed after rejected packet: rows=%d bytes=%d", sink.rows, sink.bytes)
	}
}

func TestExecutionProgressFinishWaitsForInFlightCallAndStopsLateCalls(t *testing.T) {
	t.Parallel()

	sink := &blockingProgressSink{
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
	parent := context.Background()
	_, reporter, _ := startExecutionProgress(parent, sink)

	callbackDone := make(chan struct{})
	go func() {
		defer close(callbackDone)
		reporter.report(&clickhousedriver.Progress{Rows: 1})
	}()
	<-sink.entered

	finishDone := make(chan error, 1)
	go func() {
		finishDone <- reporter.finish(parent, nil)
	}()
	select {
	case err := <-finishDone:
		t.Fatalf("finish returned before in-flight callback: %v", err)
	case <-time.After(20 * time.Millisecond):
	}

	close(sink.release)
	<-callbackDone
	if err := <-finishDone; err != nil {
		t.Fatalf("finish() = %v", err)
	}
	reporter.mu.Lock()
	if reporter.sink != nil || reporter.cancel != nil || reporter.err != nil {
		reporter.mu.Unlock()
		t.Fatal("finish retained the result sink, progress error, or derived-context cancellation")
	}
	reporter.mu.Unlock()
	reporter.report(&clickhousedriver.Progress{Rows: 2})
	if got := sink.calls.Load(); got != 1 {
		t.Fatalf("ReportProgress calls after finish = %d, want 1", got)
	}
}

type recordingProgressSink struct {
	fakeSink

	mu            sync.Mutex
	deltas        []searchjobs.ExecutionProgressDelta
	reportErr     error
	yield         bool
	calls         atomic.Uint64
	active        atomic.Int32
	maximumActive atomic.Int32
}

func (sink *recordingProgressSink) ReportProgress(delta searchjobs.ExecutionProgressDelta) error {
	sink.calls.Add(1)
	active := sink.active.Add(1)
	defer sink.active.Add(-1)
	for {
		maximum := sink.maximumActive.Load()
		if active <= maximum || sink.maximumActive.CompareAndSwap(maximum, active) {
			break
		}
	}
	if sink.yield {
		runtime.Gosched()
	}
	sink.mu.Lock()
	sink.deltas = append(sink.deltas, delta)
	sink.mu.Unlock()
	return sink.reportErr
}

func (sink *recordingProgressSink) snapshotDeltas() []searchjobs.ExecutionProgressDelta {
	sink.mu.Lock()
	defer sink.mu.Unlock()
	return slices.Clone(sink.deltas)
}

type panicProgressSink struct {
	fakeSink
	value any
}

func (sink *panicProgressSink) ReportProgress(searchjobs.ExecutionProgressDelta) error {
	panic(sink.value)
}

type overflowProgressSink struct {
	fakeSink

	rows, bytes uint64
	calls       int
	overflowErr error
}

func (sink *overflowProgressSink) ReportProgress(delta searchjobs.ExecutionProgressDelta) error {
	sink.calls++
	rows, rowCarry := bits.Add64(sink.rows, delta.ScannedRows, 0)
	bytes, byteCarry := bits.Add64(sink.bytes, delta.ScannedBytes, 0)
	if rowCarry != 0 || byteCarry != 0 {
		return sink.overflowErr
	}
	sink.rows = rows
	sink.bytes = bytes
	return nil
}

type blockingProgressSink struct {
	fakeSink

	entered chan struct{}
	release chan struct{}
	calls   atomic.Uint64
}

func (sink *blockingProgressSink) ReportProgress(searchjobs.ExecutionProgressDelta) error {
	if sink.calls.Add(1) == 1 {
		close(sink.entered)
	}
	<-sink.release
	return nil
}

type progressCallbackConnection struct {
	rows       driver.Rows
	err        error
	progress   clickhousedriver.Progress
	callback   func(*clickhousedriver.Progress)
	contextErr error
}

func (connection *progressCallbackConnection) withProgress(
	callback func(*clickhousedriver.Progress),
) clickhousedriver.QueryOption {
	connection.callback = callback
	return clickhousedriver.WithProgress(callback)
}

func (connection *progressCallbackConnection) Query(
	ctx context.Context,
	_ string,
	_ ...any,
) (driver.Rows, error) {
	if connection.callback == nil {
		return nil, errors.New("ClickHouse progress callback was not attached")
	}
	connection.callback(&connection.progress)
	connection.contextErr = ctx.Err()
	return connection.rows, connection.err
}
