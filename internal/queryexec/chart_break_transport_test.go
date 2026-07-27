package queryexec

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	clickhousedriver "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
)

// chartBreakTransportStringRows builds a one-series pivot whose row values are
// supplied verbatim, so a test can pin the buffered byte accounting against the
// exact payload the transport scanned.
func chartBreakTransportStringRows(names []string, values ...string) *fakeRows {
	rowValues := make([]any, len(values))
	counts := make([][]uint64, len(values))
	for index, value := range values {
		rowValues[index] = value
		counts[index] = make([]uint64, len(names))
		counts[index][0] = 1
	}
	return chartPivotRows("String", reflect.TypeOf(""), names, rowValues, counts)
}

// TestChartBreakTransportByteCeilingIsExactAtBothSides pins the buffered
// pivot's 48 MiB ceiling to the byte. The guard must accept a result whose
// retained size equals the bound exactly and reject the same result one byte
// larger, publishing nothing in that case.
func TestChartBreakTransportByteCeilingIsExactAtBothSides(t *testing.T) {
	// One shared backing array; every case slices it, so the fixture allocates
	// the ceiling once rather than once per subtest.
	const oneSeriesOverhead = chartRowOverheadBytes + 8
	exact := int(maximumChartResultBytes - oneSeriesOverhead)
	base := strings.Repeat("w", exact+1)

	t.Run("exactly at the ceiling publishes", func(t *testing.T) {
		rows := chartBreakTransportStringRows([]string{"0:INFO"}, base[:exact])
		sink := &fakeSink{}
		if err := executeChartBreakTransport(t, rows, sink); err != nil {
			t.Fatalf("Execute at the exact ceiling: %v", err)
		}
		if sink.setCalls != 1 || len(sink.rows) != 1 {
			t.Fatalf("boundary pivot schema calls=%d rows=%d, want 1 and 1", sink.setCalls, len(sink.rows))
		}
		value, ok := sink.rows[0][0].String()
		if !ok || len(value) != exact {
			t.Fatalf("boundary row value length = %d (%v), want %d", len(value), ok, exact)
		}
	})

	t.Run("one byte over the ceiling publishes nothing", func(t *testing.T) {
		rows := chartBreakTransportStringRows([]string{"0:INFO"}, base)
		sink := &fakeSink{}
		err := executeChartBreakTransport(t, rows, sink)
		if !errors.Is(err, searchjobs.ErrExecutionLimit) {
			t.Fatalf("Execute one byte over the ceiling = %v, want ErrExecutionLimit", err)
		}
		if sink.setCalls != 0 || len(sink.rows) != 0 {
			t.Fatalf("over-ceiling pivot published schema=%d rows=%d, want nothing", sink.setCalls, len(sink.rows))
		}
	})

	t.Run("an extra series consumes the last eight bytes", func(t *testing.T) {
		// The same row value that fits with one series must not fit with two:
		// the guard accounts every retained count, not only the row label.
		rows := chartPivotRows("String", reflect.TypeOf(""), []string{"0:INFO", "1:"},
			[]any{base[:exact]}, [][]uint64{{1, 0}})
		sink := &fakeSink{}
		err := executeChartBreakTransport(t, rows, sink)
		if !errors.Is(err, searchjobs.ErrExecutionLimit) {
			t.Fatalf("Execute with a second series = %v, want ErrExecutionLimit", err)
		}
		if sink.setCalls != 0 || len(sink.rows) != 0 {
			t.Fatalf("two-series pivot published schema=%d rows=%d, want nothing", sink.setCalls, len(sink.rows))
		}
	})

	t.Run("the ceiling accumulates across rows and trips on the offending row", func(t *testing.T) {
		// The first row leaves exactly 1000 bytes of headroom, so the second
		// row decides the outcome by a single byte in either direction.
		const headroom = 1000
		firstLength := int(maximumChartResultBytes - oneSeriesOverhead - headroom)
		fitting := headroom - int(oneSeriesOverhead)
		for _, test := range []struct {
			name         string
			secondLength int
			wantRows     int
			wantErr      error
		}{
			{name: "second row exactly fills the headroom", secondLength: fitting, wantRows: 2},
			{name: "second row exceeds the headroom by one byte", secondLength: fitting + 1, wantErr: searchjobs.ErrExecutionLimit},
		} {
			t.Run(test.name, func(t *testing.T) {
				rows := chartBreakTransportStringRows([]string{"0:INFO"}, base[:firstLength], base[:test.secondLength])
				sink := &fakeSink{}
				err := executeChartBreakTransport(t, rows, sink)
				if test.wantErr != nil {
					if !errors.Is(err, test.wantErr) {
						t.Fatalf("Execute = %v, want %v", err, test.wantErr)
					}
					if sink.setCalls != 0 || len(sink.rows) != 0 {
						t.Fatalf("published schema=%d rows=%d, want nothing", sink.setCalls, len(sink.rows))
					}
					// Every row up to and including the offending one was read,
					// proving the guard trips on the row that crosses it rather
					// than short-circuiting the scan early.
					if rows.nextCalls != 2 {
						t.Fatalf("rows read = %d, want the offending row to be reached", rows.nextCalls)
					}
					return
				}
				if err != nil {
					t.Fatalf("Execute: %v", err)
				}
				if len(sink.rows) != test.wantRows {
					t.Fatalf("published rows = %d, want %d", len(sink.rows), test.wantRows)
				}
			})
		}
	})
}

// TestChartBreakTransportCancellationBeforeAnyRowIsScanned proves a chart
// canceled between the query and the first buffered row publishes nothing and
// never enters the pivot loop.
func TestChartBreakTransportCancellationBeforeAnyRowIsScanned(t *testing.T) {
	t.Parallel()

	rows := chartBreakTransportStringRows([]string{"0:INFO"}, "/a", "/b")
	ctx, cancel := context.WithCancel(context.Background())
	connection := &chartBreakTransportCancelingConnection{rows: rows, cancel: cancel}
	sink := &fakeSink{}

	err := mustExecutor(t, connection).Execute(ctx, chartQuery("path", clickhouse.ChartRowKindString, "String"), sink)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Execute error = %v, want context.Canceled", err)
	}
	if rows.nextCalls != 0 {
		t.Fatalf("canceled chart scanned %d rows, want 0", rows.nextCalls)
	}
	if !rows.closed {
		t.Fatal("canceled chart left the result stream open")
	}
	if sink.setCalls != 0 || len(sink.rows) != 0 {
		t.Fatalf("canceled chart published schema=%d rows=%d", sink.setCalls, len(sink.rows))
	}
}

// TestChartBreakTransportCancellationMidBufferPublishesNothing covers the
// reviewer-flagged buffered path: a cancellation that lands while the pivot is
// still being buffered must abandon the whole result, not a suffix of it.
func TestChartBreakTransportCancellationMidBufferPublishesNothing(t *testing.T) {
	t.Parallel()

	base := chartBreakTransportStringRows([]string{"0:INFO"}, "/a", "/b", "/c", "/d")
	ctx, cancel := context.WithCancel(context.Background())
	rows := &chartBreakTransportCancelOnRowRows{fakeRows: base, cancel: cancel, cancelAfter: 2}
	sink := &fakeSink{}

	err := mustExecutor(t, &fakeQueryConnection{rows: rows}).Execute(
		ctx, chartQuery("path", clickhouse.ChartRowKindString, "String"), sink)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Execute error = %v, want context.Canceled", err)
	}
	if base.nextCalls != 3 {
		t.Fatalf("rows read = %d, want the scan to stop at the cancellation", base.nextCalls)
	}
	if sink.setCalls != 0 || len(sink.rows) != 0 {
		t.Fatalf("mid-buffer cancellation published schema=%d rows=%d, want nothing", sink.setCalls, len(sink.rows))
	}
	if !base.closed {
		t.Fatal("mid-buffer cancellation left the result stream open")
	}
}

// TestChartBreakTransportCancellationBetweenBufferAndPublication pins the
// window a buffered operator uniquely has: the pivot is complete in memory and
// the stream is closed, but the schema has not been published yet.
func TestChartBreakTransportCancellationBetweenBufferAndPublication(t *testing.T) {
	t.Parallel()

	base := chartBreakTransportStringRows([]string{"0:INFO"}, "/a", "/b")
	ctx, cancel := context.WithCancel(context.Background())
	rows := &cancelOnCloseTimechartRows{fakeRows: base, cancel: cancel}
	sink := &fakeSink{}

	err := mustExecutor(t, &fakeQueryConnection{rows: rows}).Execute(
		ctx, chartQuery("path", clickhouse.ChartRowKindString, "String"), sink)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Execute error = %v, want context.Canceled", err)
	}
	if rows.closeCalls != 1 || !base.closed {
		t.Fatalf("close calls = %d, closed = %v; want exactly one successful close", rows.closeCalls, base.closed)
	}
	if sink.setCalls != 0 || len(sink.rows) != 0 {
		t.Fatalf("pre-publication cancellation published schema=%d rows=%d", sink.setCalls, len(sink.rows))
	}
}

// TestChartBreakTransportCancellationDuringPublicationStopsAtTheNextRow proves
// the publication loop rechecks cancellation between rows, so a canceled job
// never keeps writing into a sink whose job is already terminal.
func TestChartBreakTransportCancellationDuringPublicationStopsAtTheNextRow(t *testing.T) {
	t.Parallel()

	rows := chartBreakTransportStringRows([]string{"0:INFO"}, "/a", "/b", "/c")
	ctx, cancel := context.WithCancel(context.Background())
	sink := &cancelOnFirstAddSink{cancel: cancel}

	err := mustExecutor(t, &fakeQueryConnection{rows: rows}).Execute(
		ctx, chartQuery("path", clickhouse.ChartRowKindString, "String"), sink)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Execute error = %v, want context.Canceled", err)
	}
	if sink.setCalls != 1 {
		t.Fatalf("schema calls = %d, want 1", sink.setCalls)
	}
	if sink.addCalls != 1 || len(sink.rows) != 1 {
		t.Fatalf("published rows = %d (recorded %d), want exactly 1", sink.addCalls, len(sink.rows))
	}
}

// TestChartBreakTransportMidStreamIterationErrorsAreClassified covers the
// second reviewer-flagged gap. A failure raised while the pivot is being
// buffered must reach the caller as a classified, redacted sentinel and must
// not publish a partial pivot.
func TestChartBreakTransportMidStreamIterationErrorsAreClassified(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name    string
		failure error
		want    error
	}{
		{
			name: "server row-limit marker",
			failure: &clickhousedriver.Exception{
				Code:    395,
				Name:    "FUNCTION_THROW_IF_VALUE_IS_NON_ZERO",
				Message: clickhouse.ChartRowLimitMarker + "; while executing SELECT secret_column",
			},
			want: searchjobs.ErrExecutionLimit,
		},
		{
			name:    "memory limit",
			failure: &clickhousedriver.Exception{Code: 241, Name: "MEMORY_LIMIT_EXCEEDED", Message: "secret_column blew the budget"},
			want:    searchjobs.ErrExecutionLimit,
		},
		{
			name:    "unsupported split value marker",
			failure: &clickhousedriver.Exception{Code: 395, Name: "THROW_IF", Message: clickhouse.UnsupportedStatsByValueMarker + " secret_column"},
			want:    searchjobs.ErrUnsupportedValue,
		},
		{
			name:    "connection loss",
			failure: clickhousedriver.ErrConnectionClosed,
			want:    searchjobs.ErrStorageUnavailable,
		},
	} {
		t.Run(test.name+" from the iterator", func(t *testing.T) {
			t.Parallel()
			rows := chartBreakTransportStringRows([]string{"0:INFO"}, "/a", "/b")
			rows.err = test.failure
			sink := &fakeSink{}
			err := executeChartBreakTransport(t, rows, sink)
			assertChartBreakTransportClassified(t, err, test.want, sink)
		})
		t.Run(test.name+" from a mid-stream scan", func(t *testing.T) {
			t.Parallel()
			base := chartBreakTransportStringRows([]string{"0:INFO"}, "/a", "/b", "/c")
			rows := &chartBreakTransportFailingScanRows{fakeRows: base, failAt: 2, failure: test.failure}
			sink := &fakeSink{}
			err := executeChartBreakTransport(t, rows, sink)
			assertChartBreakTransportClassified(t, err, test.want, sink)
		})
	}
}

// TestChartBreakTransportCloseFailureAfterBufferingPublishesNothing pins the
// buffered operator's close ordering: the stream is closed before publication
// precisely so a close failure can still suppress the whole result.
func TestChartBreakTransportCloseFailureAfterBufferingPublishesNothing(t *testing.T) {
	t.Parallel()

	rows := chartBreakTransportStringRows([]string{"0:INFO"}, "/a", "/b")
	rows.closeErr = &clickhousedriver.Exception{Code: 210, Name: "NETWORK_ERROR", Message: "secret_column connection reset"}
	sink := &fakeSink{}

	err := executeChartBreakTransport(t, rows, sink)
	if !errors.Is(err, searchjobs.ErrStorageUnavailable) {
		t.Fatalf("Execute error = %v, want ErrStorageUnavailable", err)
	}
	if sink.setCalls != 0 || len(sink.rows) != 0 {
		t.Fatalf("close failure published schema=%d rows=%d, want nothing", sink.setCalls, len(sink.rows))
	}
}

// TestChartBreakTransportSinkFailuresStopPublicationImmediately covers the
// third reviewer-flagged gap: a result-ceiling rejection raised by the sink
// must abort the pivot at the rejected row instead of continuing to push the
// remaining buffered rows at a sink that already failed.
func TestChartBreakTransportSinkFailuresStopPublicationImmediately(t *testing.T) {
	t.Parallel()

	t.Run("schema rejection skips every row", func(t *testing.T) {
		t.Parallel()
		rows := chartBreakTransportStringRows([]string{"0:INFO"}, "/a", "/b", "/c")
		sink := &chartBreakTransportCountingSink{schemaErr: searchjobs.ErrByteLimit}
		err := executeChartBreakTransport(t, rows, sink)
		if !errors.Is(err, searchjobs.ErrByteLimit) {
			t.Fatalf("Execute error = %v, want ErrByteLimit", err)
		}
		if sink.addCalls != 0 {
			t.Fatalf("rejected schema still received %d rows", sink.addCalls)
		}
	})

	for _, test := range []struct {
		name    string
		failure error
	}{
		{name: "retained row limit", failure: searchjobs.ErrRowLimit},
		{name: "retained byte limit", failure: searchjobs.ErrByteLimit},
		{name: "manager capacity", failure: searchjobs.ErrCapacity},
	} {
		t.Run(test.name+" stops at the rejected row", func(t *testing.T) {
			t.Parallel()
			rows := chartBreakTransportStringRows([]string{"0:INFO"}, "/a", "/b", "/c", "/d")
			sink := &chartBreakTransportCountingSink{rowErr: test.failure, failAt: 2}
			err := executeChartBreakTransport(t, rows, sink)
			if !errors.Is(err, test.failure) {
				t.Fatalf("Execute error = %v, want %v", err, test.failure)
			}
			if sink.addCalls != 2 {
				t.Fatalf("sink received %d rows, want the pivot to stop at the rejected row", sink.addCalls)
			}
		})
	}
}

func executeChartBreakTransport(t *testing.T, rows driver.Rows, sink searchjobs.ResultSink) error {
	t.Helper()
	return mustExecutor(t, &fakeQueryConnection{rows: rows}).Execute(
		context.Background(), chartQuery("path", clickhouse.ChartRowKindString, "String"), sink)
}

func assertChartBreakTransportClassified(t *testing.T, err error, want error, sink *fakeSink) {
	t.Helper()
	if !errors.Is(err, want) {
		t.Fatalf("Execute error = %v, want %v", err, want)
	}
	if strings.Contains(err.Error(), "secret_column") ||
		strings.Contains(err.Error(), clickhouse.ChartRowLimitMarker) ||
		strings.Contains(err.Error(), clickhouse.UnsupportedStatsByValueMarker) {
		t.Fatalf("classified chart error leaked backend detail: %v", err)
	}
	if sink.setCalls != 0 || len(sink.rows) != 0 {
		t.Fatalf("failed chart published schema=%d rows=%d, want nothing", sink.setCalls, len(sink.rows))
	}
}

type chartBreakTransportCancelingConnection struct {
	rows   driver.Rows
	cancel context.CancelFunc
	calls  int
}

func (connection *chartBreakTransportCancelingConnection) Query(
	_ context.Context, _ string, _ ...any,
) (driver.Rows, error) {
	connection.calls++
	connection.cancel()
	return connection.rows, nil
}

type chartBreakTransportCancelOnRowRows struct {
	*fakeRows
	cancel      context.CancelFunc
	cancelAfter int
}

func (rows *chartBreakTransportCancelOnRowRows) Next() bool {
	advanced := rows.fakeRows.Next()
	if advanced && rows.nextCalls == rows.cancelAfter+1 {
		rows.cancel()
	}
	return advanced
}

type chartBreakTransportFailingScanRows struct {
	*fakeRows
	failAt  int
	failure error
}

func (rows *chartBreakTransportFailingScanRows) Scan(destinations ...any) error {
	if rows.nextCalls == rows.failAt {
		return rows.failure
	}
	return rows.fakeRows.Scan(destinations...)
}

type chartBreakTransportCountingSink struct {
	schemaErr  error
	rowErr     error
	failAt     int
	setCalls   int
	addCalls   int
	lastSchema searchjobs.Schema
}

func (sink *chartBreakTransportCountingSink) SetSchema(schema searchjobs.Schema) error {
	sink.setCalls++
	if sink.schemaErr != nil {
		return sink.schemaErr
	}
	sink.lastSchema = schema
	return nil
}

func (sink *chartBreakTransportCountingSink) AddRow([]searchjobs.Value) error {
	sink.addCalls++
	if sink.rowErr != nil && sink.addCalls >= sink.failAt {
		return sink.rowErr
	}
	return nil
}
