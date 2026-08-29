package queryexec

import (
	"context"
	sqldriver "database/sql/driver"
	"errors"
	"fmt"
	"io"
	"math"
	"math/big"
	"net"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	clickhousedriver "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/chcol"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/eventfields"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
	"github.com/Suhaibinator/open-splunk/internal/searchlimits"
	"github.com/google/uuid"
	"github.com/shopspring/decimal"
)

func TestExecutorStreamsTypedRowsAndExactSchema(t *testing.T) {
	t.Parallel()
	timestamp := time.Date(2026, time.July, 21, 3, 4, 5, 123000000, time.UTC)
	wideInteger, ok := new(big.Int).SetString("999999999999999999990", 10)
	if !ok {
		t.Fatal("parse wide Dynamic integer fixture")
	}
	rows := &fakeRows{
		columns: []string{"_time", "message", "status", "_raw"},
		types: []driver.ColumnType{
			fakeColumnType{name: "_time", databaseType: "DateTime64(9, 'UTC')", scanType: reflect.TypeFor[time.Time]()},
			fakeColumnType{name: "message", databaseType: "Nullable(String)", scanType: reflect.TypeFor[*string](), nullable: true},
			fakeColumnType{name: "status", databaseType: "Dynamic", scanType: reflect.TypeFor[any]()},
			fakeColumnType{name: "_raw", databaseType: "String", scanType: reflect.TypeFor[string]()},
		},
		data: [][]any{
			{timestamp, "hello", chcol.NewDynamicWithType(int64(500), "Int64"), "valid"},
			{timestamp.Add(time.Second), nil, chcol.NewDynamicWithType("500", "String"), string([]byte{0xff, 0x00})},
			{timestamp.Add(2 * time.Second), "wide", chcol.NewDynamicWithType(wideInteger, "Int256"), "valid"},
		},
	}
	connection := &fakeQueryConnection{rows: rows}
	executor := mustExecutor(t, connection)
	sink := &fakeSink{}
	query := clickhouse.CompiledQuery{
		SQL: "SELECT scoped", Args: []any{"tenant", uint64(7)},
		OutputFields: []string{"_time", "message", "status", "_raw"},
	}
	if err := executor.Execute(context.Background(), query, sink); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if connection.query != query.SQL || !reflect.DeepEqual(connection.args, query.Args) {
		t.Fatalf("query/args = %q %#v", connection.query, connection.args)
	}
	wantKinds := []searchjobs.ValueKind{
		searchjobs.ValueKindTime, searchjobs.ValueKindString, searchjobs.ValueKindMixed, searchjobs.ValueKindMixed,
	}
	for index, want := range wantKinds {
		if sink.schema.Columns[index].Kind != want {
			t.Errorf("schema column %d kind = %v, want %v", index, sink.schema.Columns[index].Kind, want)
		}
	}
	if !sink.schema.Columns[1].Nullable || !sink.schema.Columns[2].Nullable || !sink.schema.Columns[3].Nullable {
		t.Fatalf("nullable schema = %+v", sink.schema.Columns)
	}
	if len(sink.rows) != 3 || !rows.closed {
		t.Fatalf("rows=%d closed=%v", len(sink.rows), rows.closed)
	}
	if value, ok := sink.rows[0][1].String(); !ok || value != "hello" {
		t.Fatalf("message = %#v", sink.rows[0][1])
	}
	if !sink.rows[1][1].IsNull() {
		t.Fatalf("nullable message = %#v", sink.rows[1][1])
	}
	if value, ok := sink.rows[0][2].Signed(); !ok || value != 500 {
		t.Fatalf("integer Dynamic = %#v", sink.rows[0][2])
	}
	if value, ok := sink.rows[1][2].String(); !ok || value != "500" {
		t.Fatalf("string Dynamic = %#v", sink.rows[1][2])
	}
	if value, ok := sink.rows[2][2].Decimal(); !ok || value != wideInteger.String() {
		t.Fatalf("Int256 Dynamic = %#v", sink.rows[2][2])
	}
	if value, ok := sink.rows[1][3].Bytes(); !ok || !slices.Equal(value, []byte{0xff, 0}) {
		t.Fatalf("binary raw = %#v", sink.rows[1][3])
	}
}

func TestExecutorPublishesOrdinarySchemaWithFirstRowOrSuccessfulEmpty(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		data       [][]any
		iteration  error
		wantErr    error
		wantEvents []string
		wantSchema int
		wantRows   int
	}{
		{
			name:       "nonempty success",
			data:       [][]any{{"one"}},
			wantEvents: []string{"schema", "row"},
			wantSchema: 1,
			wantRows:   1,
		},
		{
			name:       "empty success",
			wantEvents: []string{"schema"},
			wantSchema: 1,
		},
		{
			name:      "pre-row iteration failure",
			iteration: io.ErrUnexpectedEOF,
			wantErr:   searchjobs.ErrStorageUnavailable,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			rows := &fakeRows{
				columns: []string{"message"},
				types: []driver.ColumnType{
					fakeColumnType{name: "message", databaseType: "String", scanType: reflect.TypeFor[string]()},
				},
				data: test.data,
				err:  test.iteration,
			}
			sink := &fakeSink{}
			err := mustExecutor(t, &fakeQueryConnection{rows: rows}).Execute(
				context.Background(),
				clickhouse.CompiledQuery{SQL: "SELECT message", OutputFields: []string{"message"}},
				sink,
			)
			if !errors.Is(err, test.wantErr) {
				t.Fatalf("Execute error = %v, want %v", err, test.wantErr)
			}
			if !reflect.DeepEqual(sink.events, test.wantEvents) {
				t.Fatalf("sink events = %#v, want %#v", sink.events, test.wantEvents)
			}
			if sink.setCalls != test.wantSchema {
				t.Fatalf("schema calls = %d, want %d", sink.setCalls, test.wantSchema)
			}
			if len(sink.rows) != test.wantRows {
				t.Fatalf("rows = %d, want %d", len(sink.rows), test.wantRows)
			}
			if !rows.closed {
				t.Fatal("result rows were not closed")
			}
			if test.wantErr == nil {
				wantSchema := []searchjobs.Column{{Name: "message", Kind: searchjobs.ValueKindString}}
				if !reflect.DeepEqual(sink.schema.Columns, wantSchema) {
					t.Fatalf("schema = %#v, want %#v", sink.schema.Columns, wantSchema)
				}
			}
		})
	}
}

func TestScanDestinationsOverridesGenericDynamicScanType(t *testing.T) {
	t.Parallel()

	destinations, err := scanDestinations([]driver.ColumnType{
		fakeColumnType{
			name:         "band",
			databaseType: "LowCardinality(Nullable(Dynamic(max_types=32)))",
			scanType:     reflect.TypeFor[any](),
		},
	})
	if err != nil {
		t.Fatalf("scanDestinations: %v", err)
	}
	if len(destinations) != 1 {
		t.Fatalf("destinations = %d, want 1", len(destinations))
	}
	if _, ok := destinations[0].(*chcol.Dynamic); !ok {
		t.Fatalf("Dynamic destination = %T, want *chcol.Dynamic", destinations[0])
	}
}

func TestExecutorBuffersAndPublishesRuntimeWideTimechart(t *testing.T) {
	t.Parallel()

	// The public bucket timestamp is reconstructed from trusted metadata, so
	// valid ranges are not constrained by ClickHouse's DateTime64 transport
	// epoch.
	first := time.Date(1899, time.December, 31, 23, 45, 0, 0, time.UTC)
	names := []string{"0:_audit", "0:Z", "1:", "2:"}
	rows := timechartOrdinalRows(names, [][]uint64{
		{2, 1, 0, 3},
		{0, 4, 1, 0},
		{5, 0, 0, 2},
	})
	sink := &fakeSink{}
	executor := mustExecutor(t, &fakeQueryConnection{rows: rows})
	if err := executor.Execute(context.Background(), timechartQuery(first, 3), sink); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if sink.setCalls != 1 || !rows.closed {
		t.Fatalf("schema calls=%d rows closed=%v", sink.setCalls, rows.closed)
	}
	wantColumns := []searchjobs.Column{
		{Name: "_time", Kind: searchjobs.ValueKindTime},
		{Name: "VALUE_audit", Kind: searchjobs.ValueKindUnsigned},
		{Name: "Z", Kind: searchjobs.ValueKindUnsigned},
		{Name: "NULL", Kind: searchjobs.ValueKindUnsigned},
		{Name: "OTHER", Kind: searchjobs.ValueKindUnsigned},
	}
	if !reflect.DeepEqual(sink.schema.Columns, wantColumns) {
		t.Fatalf("schema = %#v, want %#v", sink.schema.Columns, wantColumns)
	}
	if len(sink.rows) != 3 {
		t.Fatalf("published rows = %d, want 3", len(sink.rows))
	}
	for index, row := range sink.rows {
		bucket, ok := row[0].Time()
		if !ok || !bucket.Equal(first.Add(time.Duration(index)*5*time.Minute)) {
			t.Fatalf("row %d bucket = %v, %v", index, bucket, ok)
		}
		for seriesIndex, want := range rows.data[index][2].([]uint64) {
			got, ok := row[seriesIndex+1].Unsigned()
			if !ok || got != want {
				t.Fatalf("row %d series %d = %d, %v, want %d", index, seriesIndex, got, ok, want)
			}
		}
	}
}

func TestExecutorBuffersAndPublishesFixedTimechart(t *testing.T) {
	t.Parallel()

	first := time.Date(1899, time.December, 31, 19, 0, 0, 0, time.UTC)
	rows := fixedTimechartOrdinalRows([]uint64{2, 0, 1, 0})
	sink := &fakeSink{}
	executor := mustExecutor(t, &fakeQueryConnection{rows: rows})
	if err := executor.Execute(context.Background(), fixedTimechartQuery(first, 4), sink); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	wantSchema := []searchjobs.Column{
		{Name: "_time", Kind: searchjobs.ValueKindTime},
		{Name: "count", Kind: searchjobs.ValueKindUnsigned},
	}
	if sink.setCalls != 1 || !rows.closed || !reflect.DeepEqual(sink.schema.Columns, wantSchema) {
		t.Fatalf("schema=%#v calls=%d rows closed=%v", sink.schema, sink.setCalls, rows.closed)
	}
	if len(sink.rows) != 4 {
		t.Fatalf("published rows = %d, want 4", len(sink.rows))
	}
	for index, row := range sink.rows {
		bucket, bucketOK := row[0].Time()
		count, countOK := row[1].Unsigned()
		if !bucketOK || !bucket.Equal(first.Add(time.Duration(index)*5*time.Minute)) ||
			!countOK || count != rows.data[index][1].(uint64) {
			t.Fatalf("row %d = %#v", index, row)
		}
	}
}

func TestExecutorSuppressesWhollyEmptyFixedTimechart(t *testing.T) {
	t.Parallel()

	first := time.Date(2026, time.July, 22, 12, 0, 0, 0, time.UTC)
	rows := fixedTimechartOrdinalRows([]uint64{0, 0, 0})
	sink := &fakeSink{}
	executor := mustExecutor(t, &fakeQueryConnection{rows: rows})
	if err := executor.Execute(context.Background(), fixedTimechartQuery(first, 3), sink); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if sink.setCalls != 1 || len(sink.schema.Columns) != 2 ||
		sink.schema.Columns[1].Name != "count" || len(sink.rows) != 0 {
		t.Fatalf("empty fixed timechart schema=%#v rows=%d calls=%d", sink.schema, len(sink.rows), sink.setCalls)
	}
}

func TestExecutorRejectsMalformedFixedTimechartAtomically(t *testing.T) {
	first := time.Date(2026, time.July, 22, 12, 0, 0, 0, time.UTC)
	tests := []struct {
		name        string
		mutate      func(*fakeRows, *clickhouse.CompiledQuery)
		queryIssued bool
	}{
		{
			name: "wrong count column",
			mutate: func(rows *fakeRows, _ *clickhouse.CompiledQuery) {
				rows.columns[1] = "wrong"
			},
			queryIssued: true,
		},
		{
			name: "nullable count",
			mutate: func(rows *fakeRows, _ *clickhouse.CompiledQuery) {
				rows.types[1] = fakeColumnType{
					name: clickhouse.TimechartCountColumn, databaseType: "UInt64",
					scanType: reflect.TypeFor[uint64](), nullable: true,
				}
			},
			queryIssued: true,
		},
		{
			name: "wrong count width",
			mutate: func(rows *fakeRows, _ *clickhouse.CompiledQuery) {
				rows.types[1] = fakeColumnType{
					name: clickhouse.TimechartCountColumn, databaseType: "UInt32",
					scanType: reflect.TypeFor[uint32](),
				}
				rows.data[0][1] = uint32(1)
			},
			queryIssued: true,
		},
		{
			name: "incomplete ordinals",
			mutate: func(rows *fakeRows, _ *clickhouse.CompiledQuery) {
				rows.data = rows.data[:1]
			},
			queryIssued: true,
		},
		{
			name: "ordinal gap",
			mutate: func(rows *fakeRows, _ *clickhouse.CompiledQuery) {
				rows.data[1][0] = uint64(2)
			},
			queryIssued: true,
		},
		{
			name: "extra ordinal",
			mutate: func(rows *fakeRows, _ *clickhouse.CompiledQuery) {
				rows.data = append(rows.data, []any{uint64(2), uint64(0)})
			},
			queryIssued: true,
		},
		{
			name: "invalid output mode",
			mutate: func(_ *fakeRows, query *clickhouse.CompiledQuery) {
				query.Timechart.Mode = clickhouse.TimechartMode(255)
			},
		},
		{
			name: "dynamic label bound on fixed output",
			mutate: func(_ *fakeRows, query *clickhouse.CompiledQuery) {
				query.Timechart.MaxLabelBytes = 1
			},
		},
		{
			name: "wrong public fields",
			mutate: func(_ *fakeRows, query *clickhouse.CompiledQuery) {
				query.OutputFields = []string{"_time"}
			},
		},
		{
			name: "zero bucket origin",
			mutate: func(_ *fakeRows, query *clickhouse.CompiledQuery) {
				query.Timechart.FirstBucket = time.Time{}
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			rows := fixedTimechartOrdinalRows([]uint64{1, 0})
			query := fixedTimechartQuery(first, 2)
			test.mutate(rows, &query)
			connection := &fakeQueryConnection{rows: rows}
			sink := &fakeSink{}
			err := mustExecutor(t, connection).Execute(context.Background(), query, sink)
			if !errors.Is(err, searchjobs.ErrInvalidResult) {
				t.Fatalf("Execute error = %v, want ErrInvalidResult", err)
			}
			if got := connection.query != ""; got != test.queryIssued {
				t.Fatalf("query issued = %v, want %v", got, test.queryIssued)
			}
			if sink.setCalls != 0 || len(sink.rows) != 0 {
				t.Fatalf("malformed fixed timechart published schema=%d rows=%d", sink.setCalls, len(sink.rows))
			}
		})
	}
}

func TestExecutorRejectsFixedTimechartStreamFailuresAtomically(t *testing.T) {
	t.Parallel()

	first := time.Date(2026, time.July, 22, 12, 0, 0, 0, time.UTC)
	for _, test := range []struct {
		name   string
		mutate func(*fakeRows)
	}{
		{
			name:   "iteration failure",
			mutate: func(rows *fakeRows) { rows.err = io.ErrUnexpectedEOF },
		},
		{
			name:   "close failure",
			mutate: func(rows *fakeRows) { rows.closeErr = io.ErrUnexpectedEOF },
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			rows := fixedTimechartOrdinalRows([]uint64{1, 0})
			test.mutate(rows)
			sink := &fakeSink{}
			err := mustExecutor(t, &fakeQueryConnection{rows: rows}).Execute(
				context.Background(),
				fixedTimechartQuery(first, 2),
				sink,
			)
			if !errors.Is(err, searchjobs.ErrStorageUnavailable) {
				t.Fatalf("Execute error = %v, want ErrStorageUnavailable", err)
			}
			if sink.setCalls != 0 || len(sink.rows) != 0 {
				t.Fatalf("failed fixed timechart published schema=%d rows=%d", sink.setCalls, len(sink.rows))
			}
		})
	}
}

func TestExecutorCancelsFixedTimechartBeforePublication(t *testing.T) {
	t.Parallel()

	first := time.Date(2026, time.July, 22, 12, 0, 0, 0, time.UTC)
	rows := fixedTimechartOrdinalRows([]uint64{1, 0})
	ctx, cancel := context.WithCancel(context.Background())
	rows.afterScan = cancel
	sink := &fakeSink{}
	err := mustExecutor(t, &fakeQueryConnection{rows: rows}).Execute(
		ctx,
		fixedTimechartQuery(first, 2),
		sink,
	)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Execute error = %v, want context.Canceled", err)
	}
	if !rows.closed || rows.nextCalls != 1 {
		t.Fatalf("canceled stream closed=%v next calls=%d", rows.closed, rows.nextCalls)
	}
	if sink.setCalls != 0 || len(sink.rows) != 0 {
		t.Fatalf("canceled fixed timechart published schema=%d rows=%d", sink.setCalls, len(sink.rows))
	}
}

func TestExecutorBuffersAndPublishesBoundedChartPivot(t *testing.T) {
	t.Parallel()

	// The row axis is runtime data, so the first public column is named and
	// typed from the row field rather than from canonical time.
	names := []string{"0:_audit", "0:Z", "1:", "2:"}
	rows := chartPivotRows("String", reflect.TypeFor[string](), names,
		[]any{"/a", "/b", "/c"},
		[][]uint64{{2, 1, 0, 3}, {0, 4, 1, 0}, {5, 0, 0, 2}})
	sink := &fakeSink{}
	executor := mustExecutor(t, &fakeQueryConnection{rows: rows})
	if err := executor.Execute(context.Background(), chartQuery("path", clickhouse.ChartRowKindString, "String"), sink); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if sink.setCalls != 1 || !rows.closed {
		t.Fatalf("schema calls=%d rows closed=%v", sink.setCalls, rows.closed)
	}
	wantColumns := []searchjobs.Column{
		{Name: "path", Kind: searchjobs.ValueKindString},
		{Name: "VALUE_audit", Kind: searchjobs.ValueKindUnsigned},
		{Name: "Z", Kind: searchjobs.ValueKindUnsigned},
		{Name: "NULL", Kind: searchjobs.ValueKindUnsigned},
		{Name: "OTHER", Kind: searchjobs.ValueKindUnsigned},
	}
	if !reflect.DeepEqual(sink.schema.Columns, wantColumns) {
		t.Fatalf("schema = %#v, want %#v", sink.schema.Columns, wantColumns)
	}
	if len(sink.rows) != 3 {
		t.Fatalf("published rows = %d, want 3", len(sink.rows))
	}
	for index, row := range sink.rows {
		label, ok := row[0].String()
		if !ok || label != rows.data[index][1].(string) {
			t.Fatalf("row %d label = %q, %v", index, label, ok)
		}
		for seriesIndex, want := range rows.data[index][3].([]uint64) {
			got, ok := row[seriesIndex+1].Unsigned()
			if !ok || got != want {
				t.Fatalf("row %d series %d = %d, %v, want %d", index, seriesIndex, got, ok, want)
			}
		}
	}
}

// TestExecutorPublishesEveryChartRowKind proves the pivot's first column keeps
// the exact scalar kind the compiler declared, including a binned timestamp.
func TestExecutorPublishesEveryChartRowKind(t *testing.T) {
	t.Parallel()

	bucket := time.Date(2026, time.July, 22, 12, 5, 0, 0, time.UTC)
	for _, test := range []struct {
		name         string
		rowKind      clickhouse.ChartRowKind
		databaseType string
		scanType     reflect.Type
		value        any
		want         searchjobs.ValueKind
	}{
		{"string", clickhouse.ChartRowKindString, "String", reflect.TypeFor[string](), "/a", searchjobs.ValueKindString},
		{"unsigned8", clickhouse.ChartRowKindUnsigned, "UInt8", reflect.TypeFor[uint8](), uint8(10), searchjobs.ValueKindUnsigned},
		{"unsigned64", clickhouse.ChartRowKindUnsigned, "UInt64", reflect.TypeFor[uint64](), uint64(10), searchjobs.ValueKindUnsigned},
		{"signed", clickhouse.ChartRowKindSigned, "Int64", reflect.TypeFor[int64](), int64(-20), searchjobs.ValueKindSigned},
		{"double", clickhouse.ChartRowKindDouble, "Float64", reflect.TypeFor[float64](), 1.5, searchjobs.ValueKindDouble},
		{"bool", clickhouse.ChartRowKindBool, "Bool", reflect.TypeFor[bool](), true, searchjobs.ValueKindBool},
		{"time", clickhouse.ChartRowKindTime, "DateTime64(9, 'UTC')", reflect.TypeFor[time.Time](), bucket, searchjobs.ValueKindTime},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			rows := chartPivotRows(test.databaseType, test.scanType, []string{"0:INFO"}, []any{test.value}, [][]uint64{{4}})
			sink := &fakeSink{}
			executor := mustExecutor(t, &fakeQueryConnection{rows: rows})
			query := chartQuery("band", test.rowKind, test.databaseType)
			if err := executor.Execute(context.Background(), query, sink); err != nil {
				t.Fatalf("Execute: %v", err)
			}
			if sink.schema.Columns[0].Kind != test.want || sink.schema.Columns[0].Nullable {
				t.Fatalf("row column = %#v, want kind %v", sink.schema.Columns[0], test.want)
			}
			if len(sink.rows) != 1 || sink.rows[0][0].Kind() != test.want {
				t.Fatalf("published row value = %#v", sink.rows)
			}
		})
	}
}

// TestExecutorPublishesRawChartRowLikeStatsGroupColumn pins the row-axis
// parity clause for _raw, the one field whose ordinary group column is Mixed
// and nullable because ingest deliberately accepts non-UTF-8 raw bytes. The
// chart row column must publish the same schema and the same cell values, not
// fail the job with an internal invalid-result error.
func TestExecutorPublishesRawChartRowLikeStatsGroupColumn(t *testing.T) {
	t.Parallel()

	binary := string([]byte{0x61, 0xff, 0xfe, 0x62})
	rows := chartPivotRows("String", reflect.TypeFor[string](), []string{"0:INFO"},
		[]any{"plain", binary}, [][]uint64{{2}, {3}})
	sink := &fakeSink{}
	executor := mustExecutor(t, &fakeQueryConnection{rows: rows})
	query := chartQuery("_raw", clickhouse.ChartRowKindMixed, "String")
	if err := executor.Execute(context.Background(), query, sink); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	want := searchjobs.Column{Name: "_raw", Kind: searchjobs.ValueKindMixed, Nullable: true}
	if sink.setCalls != 1 || sink.schema.Columns[0] != want {
		t.Fatalf("row column = %#v, want %#v", sink.schema.Columns[0], want)
	}
	if len(sink.rows) != 2 {
		t.Fatalf("published rows = %d, want 2", len(sink.rows))
	}
	if text, ok := sink.rows[0][0].String(); !ok || text != "plain" {
		t.Fatalf("utf-8 row value = %q, %v", text, ok)
	}
	raw, ok := sink.rows[1][0].Bytes()
	if !ok || string(raw) != binary {
		t.Fatalf("binary row value = %v, %v", raw, ok)
	}
}

// TestExecutorRejectsOversizedChartResultAtomically pins the buffered pivot's
// byte ceiling. A chart row value has no length ceiling and the whole pivot is
// materialized before the first sink call, so the executor must fail with a
// deterministic execution-limit error and publish nothing rather than letting
// the sink's incremental byte back-pressure arrive too late.
func TestExecutorRejectsOversizedChartResultAtomically(t *testing.T) {
	t.Parallel()

	// One shared 1 MiB backing string keeps the fixture cheap; the guard
	// accounts each row's own length regardless.
	wide := strings.Repeat("w", 1<<20)
	const rowCount = 64
	rowValues := make([]any, rowCount)
	counts := make([][]uint64, rowCount)
	for index := range rowValues {
		rowValues[index] = wide
		counts[index] = []uint64{1}
	}
	rows := chartPivotRows("String", reflect.TypeFor[string](), []string{"0:INFO"}, rowValues, counts)
	sink := &fakeSink{}
	executor := mustExecutor(t, &fakeQueryConnection{rows: rows})
	err := executor.Execute(context.Background(), chartQuery("_raw", clickhouse.ChartRowKindString, "String"), sink)
	if !errors.Is(err, searchjobs.ErrExecutionLimit) {
		t.Fatalf("Execute error = %v, want an execution limit", err)
	}
	if sink.setCalls != 0 || len(sink.rows) != 0 {
		t.Fatalf("partial publication: schema calls=%d rows=%d", sink.setCalls, len(sink.rows))
	}

	// The same shape well inside the ceiling still publishes normally.
	narrow := chartPivotRows("String", reflect.TypeFor[string](), []string{"0:INFO"},
		[]any{wide, wide}, [][]uint64{{1}, {1}})
	narrowSink := &fakeSink{}
	narrowExecutor := mustExecutor(t, &fakeQueryConnection{rows: narrow})
	if err := narrowExecutor.Execute(context.Background(),
		chartQuery("_raw", clickhouse.ChartRowKindString, "String"), narrowSink); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if narrowSink.setCalls != 1 || len(narrowSink.rows) != 2 {
		t.Fatalf("bounded pivot schema calls=%d rows=%d", narrowSink.setCalls, len(narrowSink.rows))
	}
}

func TestExecutorSuppressesEmptyChartPivot(t *testing.T) {
	t.Parallel()

	rows := chartPivotRows("String", reflect.TypeFor[string](), nil, nil, nil)
	sink := &fakeSink{}
	executor := mustExecutor(t, &fakeQueryConnection{rows: rows})
	if err := executor.Execute(context.Background(), chartQuery("path", clickhouse.ChartRowKindString, "String"), sink); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if sink.setCalls != 1 || len(sink.schema.Columns) != 1 || sink.schema.Columns[0].Name != "path" || len(sink.rows) != 0 {
		t.Fatalf("empty chart schema=%#v rows=%d calls=%d", sink.schema, len(sink.rows), sink.setCalls)
	}
}

func TestExecutorRejectsMalformedChartAtomically(t *testing.T) {
	tests := []struct {
		name        string
		mutate      func(*fakeRows, *clickhouse.CompiledQuery)
		want        error
		queryIssued bool
	}{
		{
			name:   "wrong physical columns",
			mutate: func(rows *fakeRows, _ *clickhouse.CompiledQuery) { rows.columns[1] = "wrong" },
			want:   searchjobs.ErrInvalidResult, queryIssued: true,
		},
		{
			name: "row column type drift",
			mutate: func(rows *fakeRows, _ *clickhouse.CompiledQuery) {
				rows.types[1] = fakeColumnType{name: clickhouse.ChartRowColumn, databaseType: "UInt64", scanType: reflect.TypeFor[uint64]()}
			},
			want: searchjobs.ErrInvalidResult, queryIssued: true,
		},
		{
			name: "row column scan type drift",
			mutate: func(rows *fakeRows, _ *clickhouse.CompiledQuery) {
				rows.types[1] = fakeColumnType{name: clickhouse.ChartRowColumn, databaseType: "String", scanType: reflect.TypeFor[[]byte]()}
			},
			want: searchjobs.ErrInvalidResult, queryIssued: true,
		},
		{
			name: "nullable row column",
			mutate: func(rows *fakeRows, _ *clickhouse.CompiledQuery) {
				rows.types[1] = fakeColumnType{name: clickhouse.ChartRowColumn, databaseType: "String", scanType: reflect.TypeFor[string](), nullable: true}
			},
			want: searchjobs.ErrInvalidResult, queryIssued: true,
		},
		{
			// The declared public kind is authoritative: a String transport
			// column can never publish an unsigned row label.
			name: "row value kind disagrees with the compiled contract",
			mutate: func(_ *fakeRows, query *clickhouse.CompiledQuery) {
				query.Chart.RowKind = clickhouse.ChartRowKindUnsigned
			},
			want: searchjobs.ErrInvalidResult,
		},
		{
			// The Mixed row column admits exactly the two kinds a String
			// transport can produce and nothing else, so it never becomes an
			// escape hatch for a drifting transport type.
			name: "mixed row column over a non-string transport",
			mutate: func(rows *fakeRows, query *clickhouse.CompiledQuery) {
				query.Chart.RowKind = clickhouse.ChartRowKindMixed
				query.Chart.RowDatabaseType = "UInt64"
				rows.types[1] = fakeColumnType{name: clickhouse.ChartRowColumn, databaseType: "UInt64", scanType: reflect.TypeFor[uint64]()}
				for index, row := range rows.data {
					row[1] = uint64(index)
				}
			},
			want: searchjobs.ErrInvalidResult,
		},
		{
			name:   "sparse ordinal sequence",
			mutate: func(rows *fakeRows, _ *clickhouse.CompiledQuery) { rows.data[1][0] = uint64(5) },
			want:   searchjobs.ErrInvalidResult, queryIssued: true,
		},
		{
			name: "series change between rows",
			mutate: func(rows *fakeRows, _ *clickhouse.CompiledQuery) {
				rows.data[1][2] = []string{"0:OTHERWISE"}
				rows.data[1][3] = []uint64{1}
			},
			want: searchjobs.ErrInvalidResult, queryIssued: true,
		},
		{
			name: "nonempty result with empty series domain",
			mutate: func(rows *fakeRows, _ *clickhouse.CompiledQuery) {
				for _, row := range rows.data {
					row[2] = []string{}
					row[3] = []uint64{}
				}
			},
			want: searchjobs.ErrInvalidResult, queryIssued: true,
		},
		{
			name:   "series exceed the declared bound",
			mutate: func(_ *fakeRows, query *clickhouse.CompiledQuery) { query.Chart.MaxSeries = 1 },
			want:   searchjobs.ErrInvalidResult, queryIssued: true,
		},
		{
			// A runtime value must never take the row column's public name.
			name: "series collides with the row column",
			mutate: func(rows *fakeRows, _ *clickhouse.CompiledQuery) {
				for _, row := range rows.data {
					row[2] = []string{"0:path"}
					row[3] = []uint64{1}
				}
			},
			want: searchjobs.ErrUnsupportedValue, queryIssued: true,
		},
		{
			name:   "unsupported value flag",
			mutate: func(rows *fakeRows, _ *clickhouse.CompiledQuery) { rows.data[1][4] = uint8(1) },
			want:   searchjobs.ErrUnsupportedValue, queryIssued: true,
		},
		{
			name:   "more rows than the declared ceiling",
			mutate: func(_ *fakeRows, query *clickhouse.CompiledQuery) { query.Chart.RowLimit = 2 },
			want:   searchjobs.ErrExecutionLimit, queryIssued: true,
		},
		{
			name:   "row ceiling above the executor bound",
			mutate: func(_ *fakeRows, query *clickhouse.CompiledQuery) { query.Chart.RowLimit = maximumChartRows + 1 },
			want:   searchjobs.ErrInvalidResult,
		},
		{
			name:   "declared prefix does not name the row column",
			mutate: func(_ *fakeRows, query *clickhouse.CompiledQuery) { query.OutputFields = []string{"_time"} },
			want:   searchjobs.ErrInvalidResult,
		},
		{
			name: "row kind is unset",
			mutate: func(_ *fakeRows, query *clickhouse.CompiledQuery) {
				query.Chart.RowKind = clickhouse.ChartRowKindInvalid
			},
			want: searchjobs.ErrInvalidResult,
		},
		{
			name: "two wide contracts",
			mutate: func(_ *fakeRows, query *clickhouse.CompiledQuery) {
				query.Timechart = &clickhouse.TimechartOutput{
					FirstBucket: time.Unix(0, 0).UTC(), Span: time.Minute, BucketCount: 1, MaxSeries: 12, MaxLabelBytes: 256,
				}
			},
			want: searchjobs.ErrInvalidResult,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			rows := chartPivotRows("String", reflect.TypeFor[string](), []string{"0:INFO", "1:"},
				[]any{"/a", "/b", "/c"},
				[][]uint64{{1, 0}, {0, 2}, {3, 0}})
			query := chartQuery("path", clickhouse.ChartRowKindString, "String")
			test.mutate(rows, &query)
			connection := &fakeQueryConnection{rows: rows}
			sink := &fakeSink{}
			err := mustExecutor(t, connection).Execute(context.Background(), query, sink)
			if !errors.Is(err, test.want) {
				t.Fatalf("Execute() error = %v, want %v", err, test.want)
			}
			if got := connection.query != ""; got != test.queryIssued {
				t.Fatalf("query issued = %v, want %v", got, test.queryIssued)
			}
			if sink.setCalls != 0 || len(sink.rows) != 0 {
				t.Fatalf("malformed chart published schema=%d rows=%d", sink.setCalls, len(sink.rows))
			}
		})
	}
}

func TestExecutorSuppressesEmptyTimechartGrid(t *testing.T) {
	t.Parallel()

	first := time.Date(2026, time.July, 22, 12, 0, 0, 0, time.UTC)
	rows := timechartOrdinalRows(nil, [][]uint64{{}, {}, {}})
	sink := &fakeSink{}
	executor := mustExecutor(t, &fakeQueryConnection{rows: rows})
	if err := executor.Execute(context.Background(), timechartQuery(first, 3), sink); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if sink.setCalls != 1 || len(sink.schema.Columns) != 1 || sink.schema.Columns[0].Name != "_time" || len(sink.rows) != 0 {
		t.Fatalf("empty timechart schema=%#v rows=%d calls=%d", sink.schema, len(sink.rows), sink.setCalls)
	}
}

func TestExecutorRejectsMalformedTimechartAtomically(t *testing.T) {
	first := time.Date(2026, time.July, 22, 12, 0, 0, 0, time.UTC)
	tests := []struct {
		name        string
		mutate      func(*fakeRows, *clickhouse.CompiledQuery)
		want        error
		queryIssued bool
	}{
		{name: "wrong physical columns", mutate: func(rows *fakeRows, _ *clickhouse.CompiledQuery) { rows.columns[1] = "wrong" }, want: searchjobs.ErrInvalidResult, queryIssued: true},
		{name: "nullable physical column", mutate: func(rows *fakeRows, _ *clickhouse.CompiledQuery) {
			rows.types[1] = fakeColumnType{name: clickhouse.TimechartNamesColumn, databaseType: "Array(String)", scanType: reflect.TypeFor[[]string](), nullable: true}
		}, want: searchjobs.ErrInvalidResult, queryIssued: true},
		{name: "column type name drift", mutate: func(rows *fakeRows, _ *clickhouse.CompiledQuery) {
			rows.types[1] = fakeColumnType{name: "wrong", databaseType: "Array(String)", scanType: reflect.TypeFor[[]string]()}
		}, want: searchjobs.ErrInvalidResult, queryIssued: true},
		{name: "typed nil ordinal type", mutate: func(rows *fakeRows, _ *clickhouse.CompiledQuery) {
			var columnType *fakeColumnType
			rows.types[0] = columnType
		}, want: searchjobs.ErrInvalidResult, queryIssued: true},
		{name: "wrapped ordinal type", mutate: func(rows *fakeRows, _ *clickhouse.CompiledQuery) {
			rows.types[0] = fakeColumnType{name: clickhouse.TimechartOrdinalColumn, databaseType: "Nullable(UInt64)", scanType: reflect.TypeFor[uint64]()}
		}, want: searchjobs.ErrInvalidResult, queryIssued: true},
		{name: "wrong ordinal width", mutate: func(rows *fakeRows, _ *clickhouse.CompiledQuery) {
			rows.types[0] = fakeColumnType{name: clickhouse.TimechartOrdinalColumn, databaseType: "UInt32", scanType: reflect.TypeFor[uint32]()}
		}, want: searchjobs.ErrInvalidResult, queryIssued: true},
		{name: "wrong native ordinal", mutate: func(rows *fakeRows, _ *clickhouse.CompiledQuery) {
			rows.types[0] = fakeColumnType{name: clickhouse.TimechartOrdinalColumn, databaseType: "UInt64", scanType: reflect.TypeFor[int64]()}
			rows.data[0][0] = int64(0)
		}, want: searchjobs.ErrInvalidResult, queryIssued: true},
		{name: "wrong array type", mutate: func(rows *fakeRows, _ *clickhouse.CompiledQuery) {
			rows.types[2] = fakeColumnType{name: clickhouse.TimechartCountsColumn, databaseType: "Array(UInt32)", scanType: reflect.TypeFor[[]uint64]()}
		}, want: searchjobs.ErrInvalidResult, queryIssued: true},
		{name: "too few buckets", mutate: func(rows *fakeRows, _ *clickhouse.CompiledQuery) { rows.data = rows.data[:1] }, want: searchjobs.ErrInvalidResult, queryIssued: true},
		{name: "too many buckets", mutate: func(rows *fakeRows, _ *clickhouse.CompiledQuery) {
			rows.data = append(rows.data, []any{uint64(2), []string{"0:a"}, []uint64{3}, uint8(0)})
		}, want: searchjobs.ErrInvalidResult, queryIssued: true},
		{name: "wrong first ordinal", mutate: func(rows *fakeRows, _ *clickhouse.CompiledQuery) { rows.data[0][0] = uint64(1) }, want: searchjobs.ErrInvalidResult, queryIssued: true},
		{name: "ordinal gap", mutate: func(rows *fakeRows, _ *clickhouse.CompiledQuery) { rows.data[1][0] = uint64(2) }, want: searchjobs.ErrInvalidResult, queryIssued: true},
		{name: "duplicate ordinal", mutate: func(rows *fakeRows, _ *clickhouse.CompiledQuery) { rows.data[1][0] = uint64(0) }, want: searchjobs.ErrInvalidResult, queryIssued: true},
		{name: "out of range ordinal", mutate: func(rows *fakeRows, _ *clickhouse.CompiledQuery) { rows.data[1][0] = uint64(math.MaxUint64) }, want: searchjobs.ErrInvalidResult, queryIssued: true},
		{name: "count length mismatch", mutate: func(rows *fakeRows, _ *clickhouse.CompiledQuery) { rows.data[0][2] = []uint64{} }, want: searchjobs.ErrInvalidResult, queryIssued: true},
		{name: "series changed", mutate: func(rows *fakeRows, _ *clickhouse.CompiledQuery) { rows.data[1][1] = []string{"0:b"} }, want: searchjobs.ErrInvalidResult, queryIssued: true},
		{name: "series out of order", mutate: func(rows *fakeRows, _ *clickhouse.CompiledQuery) { setTimechartNames(rows, []string{"0:b", "0:a"}) }, want: searchjobs.ErrInvalidResult, queryIssued: true},
		{name: "ordinary after null", mutate: func(rows *fakeRows, _ *clickhouse.CompiledQuery) { setTimechartNames(rows, []string{"1:", "0:a"}) }, want: searchjobs.ErrInvalidResult, queryIssued: true},
		{name: "null after other", mutate: func(rows *fakeRows, _ *clickhouse.CompiledQuery) { setTimechartNames(rows, []string{"2:", "1:"}) }, want: searchjobs.ErrInvalidResult, queryIssued: true},
		{name: "duplicate null", mutate: func(rows *fakeRows, _ *clickhouse.CompiledQuery) { setTimechartNames(rows, []string{"1:", "1:"}) }, want: searchjobs.ErrInvalidResult, queryIssued: true},
		{name: "duplicate other", mutate: func(rows *fakeRows, _ *clickhouse.CompiledQuery) { setTimechartNames(rows, []string{"2:", "2:"}) }, want: searchjobs.ErrInvalidResult, queryIssued: true},
		{name: "duplicate encoded series", mutate: func(rows *fakeRows, _ *clickhouse.CompiledQuery) { setTimechartNames(rows, []string{"0:a", "0:a"}) }, want: searchjobs.ErrInvalidResult, queryIssued: true},
		{name: "empty ordinary label", mutate: func(rows *fakeRows, _ *clickhouse.CompiledQuery) { setTimechartNames(rows, []string{"0:"}) }, want: searchjobs.ErrInvalidResult, queryIssued: true},
		{name: "reserved ordinary label", mutate: func(rows *fakeRows, _ *clickhouse.CompiledQuery) { setTimechartNames(rows, []string{"0:NULL"}) }, want: searchjobs.ErrInvalidResult, queryIssued: true},
		{name: "malformed special label", mutate: func(rows *fakeRows, _ *clickhouse.CompiledQuery) { setTimechartNames(rows, []string{"1:value"}) }, want: searchjobs.ErrInvalidResult, queryIssued: true},
		{name: "unknown encoding", mutate: func(rows *fakeRows, _ *clickhouse.CompiledQuery) { setTimechartNames(rows, []string{"3:value"}) }, want: searchjobs.ErrInvalidResult, queryIssued: true},
		{name: "invalid UTF-8", mutate: func(rows *fakeRows, _ *clickhouse.CompiledQuery) {
			setTimechartNames(rows, []string{"0:" + string([]byte{0xff})})
		}, want: searchjobs.ErrInvalidResult, queryIssued: true},
		{name: "normalized collision", mutate: func(rows *fakeRows, _ *clickhouse.CompiledQuery) {
			setTimechartNames(rows, []string{"0:VALUE_x", "0:_x"})
		}, want: searchjobs.ErrUnsupportedValue, queryIssued: true},
		{name: "too many series", mutate: func(rows *fakeRows, query *clickhouse.CompiledQuery) {
			query.Timechart.MaxSeries = 1
			setTimechartNames(rows, []string{"0:a", "0:b"})
		}, want: searchjobs.ErrInvalidResult, queryIssued: true},
		{name: "oversized label", mutate: func(rows *fakeRows, query *clickhouse.CompiledQuery) {
			query.Timechart.MaxLabelBytes = 1
			setTimechartNames(rows, []string{"0:ab"})
		}, want: searchjobs.ErrInvalidResult, queryIssued: true},
		{name: "unsupported runtime value", mutate: func(rows *fakeRows, _ *clickhouse.CompiledQuery) { rows.data[1][3] = uint8(1) }, want: searchjobs.ErrUnsupportedValue, queryIssued: true},
		{name: "iteration failure", mutate: func(rows *fakeRows, _ *clickhouse.CompiledQuery) { rows.err = io.ErrUnexpectedEOF }, want: searchjobs.ErrStorageUnavailable, queryIssued: true},
		{name: "close failure", mutate: func(rows *fakeRows, _ *clickhouse.CompiledQuery) { rows.closeErr = io.ErrUnexpectedEOF }, want: searchjobs.ErrStorageUnavailable, queryIssued: true},
		{name: "invalid output prefix", mutate: func(_ *fakeRows, query *clickhouse.CompiledQuery) { query.OutputFields = []string{"wrong"} }, want: searchjobs.ErrInvalidResult},
		{name: "zero bucket count", mutate: func(_ *fakeRows, query *clickhouse.CompiledQuery) { query.Timechart.BucketCount = 0 }, want: searchjobs.ErrInvalidResult},
		{name: "zero bucket origin", mutate: func(_ *fakeRows, query *clickhouse.CompiledQuery) {
			query.Timechart.FirstBucket = time.Time{}
		}, want: searchjobs.ErrInvalidResult},
		{name: "unaligned origin", mutate: func(_ *fakeRows, query *clickhouse.CompiledQuery) {
			query.Timechart.FirstBucket = first.Add(time.Minute)
		}, want: searchjobs.ErrInvalidResult},
		{name: "bucket timestamp overflow", mutate: func(_ *fakeRows, query *clickhouse.CompiledQuery) {
			const spanSeconds = int64((5 * time.Minute) / time.Second)
			query.Timechart.FirstBucket = time.Unix(math.MaxInt64-math.MaxInt64%spanSeconds, 0).UTC()
		}, want: searchjobs.ErrInvalidResult},
		{name: "excessive bucket count", mutate: func(_ *fakeRows, query *clickhouse.CompiledQuery) {
			query.Timechart.BucketCount = maximumTimechartBuckets + 1
		}, want: searchjobs.ErrInvalidResult},
		{name: "excessive metadata series", mutate: func(_ *fakeRows, query *clickhouse.CompiledQuery) {
			query.Timechart.MaxSeries = clickhouse.MaximumTimechartSeries + 1
		}, want: searchjobs.ErrInvalidResult},
		{name: "excessive metadata label", mutate: func(_ *fakeRows, query *clickhouse.CompiledQuery) {
			query.Timechart.MaxLabelBytes = clickhouse.MaximumTimechartLabelBytes + 1
		}, want: searchjobs.ErrInvalidResult},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			rows := timechartOrdinalRows([]string{"0:a"}, [][]uint64{{1}, {2}})
			query := timechartQuery(first, 2)
			test.mutate(rows, &query)
			connection := &fakeQueryConnection{rows: rows}
			sink := &fakeSink{}
			err := mustExecutor(t, connection).Execute(context.Background(), query, sink)
			if !errors.Is(err, test.want) {
				t.Fatalf("Execute error = %v, want %v", err, test.want)
			}
			if sink.setCalls != 0 || len(sink.rows) != 0 {
				t.Fatalf("invalid result was partially published: schema calls=%d rows=%d", sink.setCalls, len(sink.rows))
			}
			if got := connection.query != ""; got != test.queryIssued {
				t.Fatalf("query issued = %v, want %v", got, test.queryIssued)
			}
		})
	}
}

func timechartQuery(first time.Time, bucketCount uint64) clickhouse.CompiledQuery {
	return clickhouse.CompiledQuery{
		SQL:          "SELECT bounded_timechart",
		OutputFields: []string{"_time"},
		Timechart: &clickhouse.TimechartOutput{
			FirstBucket: first, Span: 5 * time.Minute, BucketCount: bucketCount,
			MaxSeries: 12, MaxLabelBytes: 256,
		},
	}
}

func timechartOrdinalRows(names []string, counts [][]uint64) *fakeRows {
	rows := &fakeRows{
		columns: []string{
			clickhouse.TimechartOrdinalColumn,
			clickhouse.TimechartNamesColumn,
			clickhouse.TimechartCountsColumn,
			clickhouse.TimechartInvalidColumn,
		},
		types: []driver.ColumnType{
			fakeColumnType{name: clickhouse.TimechartOrdinalColumn, databaseType: "UInt64", scanType: reflect.TypeFor[uint64]()},
			fakeColumnType{name: clickhouse.TimechartNamesColumn, databaseType: "Array(String)", scanType: reflect.TypeFor[[]string]()},
			fakeColumnType{name: clickhouse.TimechartCountsColumn, databaseType: "Array(UInt64)", scanType: reflect.TypeFor[[]uint64]()},
			fakeColumnType{name: clickhouse.TimechartInvalidColumn, databaseType: "UInt8", scanType: reflect.TypeFor[uint8]()},
		},
		data: make([][]any, len(counts)),
	}
	for index, values := range counts {
		rowNames := []string(nil)
		if index == 0 {
			rowNames = slices.Clone(names)
		}
		rows.data[index] = []any{uint64(index), rowNames, slices.Clone(values), uint8(0)}
	}
	return rows
}

func fixedTimechartQuery(first time.Time, bucketCount uint64) clickhouse.CompiledQuery {
	return clickhouse.CompiledQuery{
		SQL:          "SELECT bounded_fixed_timechart",
		OutputFields: []string{"_time", "count"},
		Timechart: &clickhouse.TimechartOutput{
			Mode:        clickhouse.TimechartModeFixedCount,
			FirstBucket: first, Span: 5 * time.Minute, BucketCount: bucketCount,
			MaxSeries: 1,
		},
	}
}

func fixedTimechartOrdinalRows(counts []uint64) *fakeRows {
	rows := &fakeRows{
		columns: []string{
			clickhouse.TimechartOrdinalColumn,
			clickhouse.TimechartCountColumn,
		},
		types: []driver.ColumnType{
			fakeColumnType{name: clickhouse.TimechartOrdinalColumn, databaseType: "UInt64", scanType: reflect.TypeFor[uint64]()},
			fakeColumnType{name: clickhouse.TimechartCountColumn, databaseType: "UInt64", scanType: reflect.TypeFor[uint64]()},
		},
		data: make([][]any, len(counts)),
	}
	for index, count := range counts {
		rows.data[index] = []any{uint64(index), count}
	}
	return rows
}

func chartQuery(rowField string, rowKind clickhouse.ChartRowKind, rowDatabaseType string) clickhouse.CompiledQuery {
	return clickhouse.CompiledQuery{
		SQL:          "SELECT bounded_chart",
		OutputFields: []string{rowField},
		Chart: &clickhouse.ChartOutput{
			RowField: rowField, RowKind: rowKind, RowDatabaseType: rowDatabaseType,
			RowLimit: 10_000, MaxSeries: 12, MaxLabelBytes: 256,
			ValueKind: clickhouse.ChartValueKindCount,
		},
	}
}

func chartPivotRows(rowDatabaseType string, rowScanType reflect.Type, names []string, rowValues []any, counts [][]uint64) *fakeRows {
	rows := &fakeRows{
		columns: []string{
			clickhouse.ChartOrdinalColumn,
			clickhouse.ChartRowColumn,
			clickhouse.ChartNamesColumn,
			clickhouse.ChartCountsColumn,
			clickhouse.ChartInvalidColumn,
		},
		types: []driver.ColumnType{
			fakeColumnType{name: clickhouse.ChartOrdinalColumn, databaseType: "UInt64", scanType: reflect.TypeFor[uint64]()},
			fakeColumnType{name: clickhouse.ChartRowColumn, databaseType: rowDatabaseType, scanType: rowScanType},
			fakeColumnType{name: clickhouse.ChartNamesColumn, databaseType: "Array(String)", scanType: reflect.TypeFor[[]string]()},
			fakeColumnType{name: clickhouse.ChartCountsColumn, databaseType: "Array(UInt64)", scanType: reflect.TypeFor[[]uint64]()},
			fakeColumnType{name: clickhouse.ChartInvalidColumn, databaseType: "UInt8", scanType: reflect.TypeFor[uint8]()},
		},
		data: make([][]any, len(counts)),
	}
	for index, values := range counts {
		rows.data[index] = []any{uint64(index), rowValues[index], slices.Clone(names), slices.Clone(values), uint8(0)}
	}
	return rows
}

func setTimechartNames(rows *fakeRows, names []string) {
	for index, row := range rows.data {
		row[1] = []string(nil)
		if index == 0 {
			row[1] = slices.Clone(names)
		}
		row[2] = make([]uint64, len(names))
	}
}

func TestConvertJSONRestoresLogicalPathsAndNestedTypes(t *testing.T) {
	t.Parallel()
	document := chcol.NewJSON()
	document.SetValueAtPath("literal%2Edot", chcol.NewDynamicWithType("value", "String"))
	document.SetValueAtPath("percent%252Ekey", chcol.NewDynamicWithType(uint64(9), "UInt64"))
	document.SetValueAtPath("nested.ok", chcol.NewDynamicWithType(true, "Bool"))
	document.SetValueAtPath("nested.missing", chcol.NewDynamicWithType(nil, ""))
	value, err := convertValue(document)
	if err != nil {
		t.Fatalf("convertValue(JSON): %v", err)
	}
	fields, ok := value.Object()
	if !ok || len(fields) != 3 {
		t.Fatalf("root object = %#v", value)
	}
	if fields[0].Name != "literal.dot" || fields[1].Name != "nested" || fields[2].Name != "percent%2Ekey" {
		t.Fatalf("logical fields = %#v", fields)
	}
	nested, ok := fields[1].Value.Object()
	if !ok || len(nested) != 2 || nested[0].Name != "missing" || !nested[0].Value.IsNull() || nested[1].Name != "ok" {
		t.Fatalf("nested object = %#v", fields[1].Value)
	}
}

func TestExecutorReconstructsSparseEventFieldsFromPresenceMetadata(t *testing.T) {
	t.Parallel()

	document := chcol.NewJSON()
	document.SetValueAtPath("literal%2Edot", chcol.NewDynamicWithType("literal", "String"))
	document.SetValueAtPath("nested.value", chcol.NewDynamicWithType(true, "Bool"))
	document.SetValueAtPath("percent%252Ekey", chcol.NewDynamicWithType(uint64(9), "UInt64"))
	document.SetValueAtPath(`slash\key`, chcol.NewDynamicWithType("slash", "String"))
	document.SetValueAtPath("present", chcol.NewDynamicWithType("kept", "String"))
	// ClickHouse exposes part-wide JSON paths on rows where they are absent.
	document.SetValueAtPath("phantom", chcol.NewDynamicWithType(nil, ""))

	names := []string{
		"explicit_null",
		eventfields.NormalizeDynamicPath([]string{"literal.dot"}),
		eventfields.NormalizeDynamicPath([]string{"nested", "value"}),
		eventfields.NormalizeDynamicPath([]string{"percent%2Ekey"}),
		"present",
		eventfields.NormalizeDynamicPath([]string{`slash\key`}),
	}
	slices.Sort(names)
	rows := &fakeRows{
		columns: []string{"fields", clickhouse.SparseEventFieldNamesColumn},
		types: []driver.ColumnType{
			fakeColumnType{
				name: "fields", databaseType: "JSON(max_dynamic_paths=256)",
				scanType: reflect.TypeFor[*chcol.JSON](),
			},
			fakeColumnType{
				name:         clickhouse.SparseEventFieldNamesColumn,
				databaseType: "Array(String)", scanType: reflect.TypeFor[[]string](),
			},
		},
		data: [][]any{{document, names}},
	}
	sink := &fakeSink{}
	err := mustExecutor(t, &fakeQueryConnection{rows: rows}).Execute(
		context.Background(),
		clickhouse.CompiledQuery{
			SQL: "SELECT fields, field_names", OutputFields: []string{"fields"},
			SparseFields: true,
		},
		sink,
	)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if len(sink.schema.Columns) != 1 || sink.schema.Columns[0].Name != "fields" ||
		sink.schema.Columns[0].Kind != searchjobs.ValueKindObject ||
		len(sink.rows) != 1 || len(sink.rows[0]) != 1 {
		t.Fatalf("public sparse result = schema %#v rows %#v", sink.schema, sink.rows)
	}
	fields, ok := sink.rows[0][0].Object()
	if !ok {
		t.Fatalf("fields value = %#v, want object", sink.rows[0][0])
	}
	byName := make(map[string]searchjobs.Value, len(fields))
	for _, field := range fields {
		byName[field.Name] = field.Value
	}
	if _, exists := byName["phantom"]; exists {
		t.Fatalf("missing union path leaked into fields: %#v", fields)
	}
	if explicit, exists := byName["explicit_null"]; !exists || !explicit.IsNull() {
		t.Fatalf("explicit null = %#v, exists %v", explicit, exists)
	}
	if literal, exists := byName["literal.dot"]; !exists {
		t.Fatal("literal-dot field is missing")
	} else if value, stringOK := literal.String(); !stringOK || value != "literal" {
		t.Fatalf("literal-dot field = %#v", literal)
	}
	if slash, exists := byName[`slash\key`]; !exists {
		t.Fatal("backslash field is missing")
	} else if value, stringOK := slash.String(); !stringOK || value != "slash" {
		t.Fatalf("backslash field = %#v", slash)
	}
	nested, exists := byName["nested"]
	if !exists {
		t.Fatal("nested field is missing")
	}
	nestedFields, nestedOK := nested.Object()
	if !nestedOK || len(nestedFields) != 1 || nestedFields[0].Name != "value" {
		t.Fatalf("nested field = %#v", nested)
	}
}

func TestConvertSparseEventFieldsRejectsInvalidPresenceMetadata(t *testing.T) {
	t.Parallel()

	nonempty := chcol.NewJSON()
	nonempty.SetValueAtPath("stored", chcol.NewDynamicWithType("value", "String"))
	empty := chcol.NewJSON()
	tooMany := make([]string, eventfields.MaximumStoredFieldsPerEvent+1)
	for index := range tooMany {
		tooMany[index] = fmt.Sprintf("field_%04d", index)
	}
	tests := []struct {
		name     string
		document *chcol.JSON
		fields   []string
	}{
		{name: "unsorted", document: empty, fields: []string{"b", "a"}},
		{name: "duplicate", document: empty, fields: []string{"a", "a"}},
		{name: "reserved root", document: empty, fields: []string{"host"}},
		{name: "leaf ancestor collision", document: empty, fields: []string{"a", "a.b"}},
		{name: "invalid escape", document: empty, fields: []string{`bad\q`}},
		{name: "too many", document: empty, fields: tooMany},
		{name: "nonnull path missing metadata", document: nonempty},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if _, err := convertSparseEventFields(test.document, test.fields); err == nil {
				t.Fatal("invalid sparse metadata unexpectedly succeeded")
			}
		})
	}
}

func TestValidateSparseFieldsOutputRejectsForgedContracts(t *testing.T) {
	t.Parallel()

	tests := []clickhouse.CompiledQuery{
		{OutputFields: []string{"message"}, SparseFields: true},
		{OutputFields: []string{"fields", "fields"}, SparseFields: true},
		{
			OutputFields: []string{"fields", clickhouse.SparseEventFieldNamesColumn},
			SparseFields: true,
		},
		{
			OutputFields: []string{"fields"},
			SparseFields: true,
			Chart:        &clickhouse.ChartOutput{},
		},
	}
	for index, query := range tests {
		if _, err := validateSparseFieldsOutput(query); !errors.Is(err, searchjobs.ErrInvalidResult) {
			t.Errorf("forged contract %d error = %v, want ErrInvalidResult", index, err)
		}
	}
}

func TestConvertExtendedDynamicValuesRestoresExactTypes(t *testing.T) {
	t.Parallel()

	timestamp := time.Date(2026, time.July, 22, 1, 2, 3, 456789000, time.UTC)
	tests := []struct {
		name    string
		kind    string
		encoded string
		check   func(*testing.T, searchjobs.Value)
	}{
		{
			name: "bytes", kind: "bytes/v1", encoded: "AP8",
			check: func(t *testing.T, value searchjobs.Value) {
				t.Helper()
				if got, ok := value.Bytes(); !ok || !slices.Equal(got, []byte{0, 0xff}) {
					t.Fatalf("bytes = %v, %v", got, ok)
				}
			},
		},
		{
			name: "timestamp", kind: "timestamp/v1", encoded: timestamp.Format(time.RFC3339Nano),
			check: func(t *testing.T, value searchjobs.Value) {
				t.Helper()
				if got, ok := value.Time(); !ok || !got.Equal(timestamp) || got.Location() != time.UTC {
					t.Fatalf("timestamp = %v, %v", got, ok)
				}
			},
		},
		{
			name: "minimum timestamp", kind: "timestamp/v1", encoded: "0001-01-01T00:00:00Z",
			check: func(t *testing.T, value searchjobs.Value) {
				t.Helper()
				if got, ok := value.Time(); !ok || !got.IsZero() {
					t.Fatalf("minimum timestamp = %v, %v", got, ok)
				}
			},
		},
		{
			name: "duration", kind: "duration/v1", encoded: "-12:-345000000",
			check: func(t *testing.T, value searchjobs.Value) {
				t.Helper()
				if got, ok := value.Duration(); !ok || got != -(12*time.Second+345*time.Millisecond) {
					t.Fatalf("duration = %v, %v", got, ok)
				}
			},
		},
		{
			name: "decimal", kind: "decimal/v1", encoded: "-123.4500e+2",
			check: func(t *testing.T, value searchjobs.Value) {
				t.Helper()
				if got, ok := value.Decimal(); !ok || got != "-123.4500e+2" {
					t.Fatalf("decimal = %q, %v", got, ok)
				}
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			envelope := map[string]string{extendedTypeKey: test.kind, extendedValueKey: test.encoded}
			value, err := convertValue(chcol.NewDynamicWithType(envelope, "Map(String, String)"))
			if err != nil {
				t.Fatal(err)
			}
			test.check(t, value)
		})
	}
}

func TestConvertExtendedDynamicValuesRejectsMalformedEnvelopes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		envelope any
	}{
		{"padded bytes", map[string]string{extendedTypeKey: "bytes/v1", extendedValueKey: "AP8="}},
		{"noncanonical timestamp", map[string]string{extendedTypeKey: "timestamp/v1", extendedValueKey: "2026-07-22T01:02:03+00:00"}},
		{"timestamp before protobuf range", map[string]string{extendedTypeKey: "timestamp/v1", extendedValueKey: "0000-01-01T00:00:00Z"}},
		{"duration missing nanoseconds", map[string]string{extendedTypeKey: "duration/v1", extendedValueKey: "12"}},
		{"duration leading zero", map[string]string{extendedTypeKey: "duration/v1", extendedValueKey: "01:0"}},
		{"duration inconsistent signs", map[string]string{extendedTypeKey: "duration/v1", extendedValueKey: "1:-1"}},
		{"duration out of range", map[string]string{extendedTypeKey: "duration/v1", extendedValueKey: "9223372037:0"}},
		{"decimal leading zero", map[string]string{extendedTypeKey: "decimal/v1", extendedValueKey: "01.5"}},
		{"unknown tag", map[string]string{extendedTypeKey: "future/v1", extendedValueKey: "value"}},
		{"nonstring payload", map[string]any{extendedTypeKey: "bytes/v1", extendedValueKey: uint64(1)}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if _, err := convertValue(test.envelope); err == nil {
				t.Fatal("malformed reserved envelope was accepted")
			}
		})
	}
}

func TestConvertReservedLookingOrdinaryMapRemainsObject(t *testing.T) {
	t.Parallel()

	value, err := convertValue(map[string]string{
		extendedTypeKey:  "bytes/v1",
		extendedValueKey: "AP8",
		"ordinary":       "keeps this a regular map",
	})
	if err != nil {
		t.Fatal(err)
	}
	fields, ok := value.Object()
	if !ok || len(fields) != 3 {
		t.Fatalf("ordinary map = %#v", value)
	}
}

func TestExecutorConvertsDriverDecimalAndDurationScanTypes(t *testing.T) {
	t.Parallel()

	amount := decimal.RequireFromString("-12345678901234567890.00100")
	elapsed := 3*time.Hour + 4*time.Minute + 5*time.Second + 600*time.Microsecond
	rows := &fakeRows{
		columns: []string{"amount", "elapsed"},
		types: []driver.ColumnType{
			fakeColumnType{name: "amount", databaseType: "Decimal(38, 5)", scanType: reflect.TypeFor[decimal.Decimal]()},
			fakeColumnType{name: "elapsed", databaseType: "Time64(6)", scanType: reflect.TypeFor[time.Duration]()},
		},
		data: [][]any{{amount, elapsed}},
	}
	executor := mustExecutor(t, &fakeQueryConnection{rows: rows})
	sink := &fakeSink{}
	err := executor.Execute(context.Background(), clickhouse.CompiledQuery{
		SQL:          "SELECT amount, elapsed",
		OutputFields: []string{"amount", "elapsed"},
	}, sink)
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if got := []searchjobs.ValueKind{sink.schema.Columns[0].Kind, sink.schema.Columns[1].Kind}; !reflect.DeepEqual(got, []searchjobs.ValueKind{searchjobs.ValueKindDecimal, searchjobs.ValueKindDuration}) {
		t.Fatalf("schema kinds = %v", got)
	}
	if got, ok := sink.rows[0][0].Decimal(); !ok || got != "-12345678901234567890.001" {
		t.Fatalf("decimal = %q, %v", got, ok)
	}
	if got, ok := sink.rows[0][1].Duration(); !ok || got != elapsed {
		t.Fatalf("duration = %v, %v", got, ok)
	}
}

func TestConvertAdditionalClickHouseDriverScanTypes(t *testing.T) {
	t.Parallel()

	variant, err := convertValue(chcol.NewVariantWithType(uint64(17), "UInt64"))
	if err != nil {
		t.Fatal(err)
	}
	if got, ok := variant.Unsigned(); !ok || got != 17 {
		t.Fatalf("Variant = %#v", variant)
	}

	wide := new(big.Int)
	wide.SetString("340282366920938463463374607431768211455", 10)
	wideValue, err := convertValue(wide)
	if err != nil {
		t.Fatal(err)
	}
	if got, ok := wideValue.Decimal(); !ok || got != wide.String() {
		t.Fatalf("big.Int = %q, %v", got, ok)
	}

	id := uuid.MustParse("123e4567-e89b-12d3-a456-426614174000")
	idValue, err := convertValue(id)
	if err != nil {
		t.Fatal(err)
	}
	if got, ok := idValue.String(); !ok || got != id.String() {
		t.Fatalf("UUID = %q, %v", got, ok)
	}

	ip := net.ParseIP("192.0.2.1").To4()
	ipValue, err := convertValue(ip)
	if err != nil {
		t.Fatal(err)
	}
	if got, ok := ipValue.String(); !ok || got != "192.0.2.1" {
		t.Fatalf("IP = %q, %v", got, ok)
	}
}

func TestSchemaKindUnwrapsClickHouseWrappers(t *testing.T) {
	t.Parallel()
	tests := []struct {
		field    string
		database string
		kind     searchjobs.ValueKind
		multi    bool
	}{
		{"message", "LowCardinality(Nullable(String))", searchjobs.ValueKindString, false},
		{"count", "UInt64", searchjobs.ValueKindUnsigned, false},
		{"ratio", "Float64", searchjobs.ValueKindDouble, false},
		{"amount", "Decimal(38, 9)", searchjobs.ValueKindDecimal, false},
		{"clock", "Time", searchjobs.ValueKindDuration, false},
		{"precise_clock", "Time64(9)", searchjobs.ValueKindDuration, false},
		{"wide_signed", "Int256", searchjobs.ValueKindDecimal, false},
		{"wide_unsigned", "UInt128", searchjobs.ValueKindDecimal, false},
		{"id", "UUID", searchjobs.ValueKindString, false},
		{"ip", "IPv6", searchjobs.ValueKindString, false},
		{"values", "Array(Dynamic)", searchjobs.ValueKindList, true},
		{"named_tuple", "Tuple(name String, count UInt64)", searchjobs.ValueKindMixed, false},
		{"plain_tuple", "Tuple(String, UInt64)", searchjobs.ValueKindMixed, false},
		{"fields", "JSON(max_dynamic_paths=256)", searchjobs.ValueKindObject, false},
		{"anything", "Dynamic", searchjobs.ValueKindMixed, false},
		{"choice", "Variant(String, UInt64)", searchjobs.ValueKindMixed, false},
		{"_raw", "String", searchjobs.ValueKindMixed, false},
		{"_raw", "UInt64", searchjobs.ValueKindUnsigned, false},
	}
	for _, test := range tests {
		kind, multi := schemaKind(test.field, test.database)
		if kind != test.kind || multi != test.multi {
			t.Errorf("schemaKind(%q, %q) = %v/%v, want %v/%v", test.field, test.database, kind, multi, test.kind, test.multi)
		}
	}
}

func TestDatabaseTypeNullableRecognizesWholeColumnWrappers(t *testing.T) {
	t.Parallel()
	tests := []struct {
		databaseType string
		want         bool
	}{
		{"Nullable(String)", true},
		{"LowCardinality(Nullable(String))", true},
		{" LowCardinality ( Nullable ( String ) ) ", true},
		{"String", false},
		{"LowCardinality(String)", false},
		{"Array(Nullable(String))", false},
		{"Tuple(value Nullable(String))", false},
		{"Nullable(String) trailing", false},
	}
	for _, test := range tests {
		t.Run(test.databaseType, func(t *testing.T) {
			t.Parallel()
			if got := databaseTypeNullable(test.databaseType); got != test.want {
				t.Fatalf("databaseTypeNullable(%q) = %v, want %v", test.databaseType, got, test.want)
			}
		})
	}
}

func TestExecutorMarksLowCardinalityNullableSchema(t *testing.T) {
	t.Parallel()
	rows := &fakeRows{
		columns: []string{"service"},
		types: []driver.ColumnType{
			fakeColumnType{
				name:         "service",
				databaseType: "LowCardinality(Nullable(String))",
				scanType:     reflect.TypeFor[*string](),
				nullable:     false,
			},
		},
		data: [][]any{{nil}},
	}
	sink := &fakeSink{}
	executor := mustExecutor(t, &fakeQueryConnection{rows: rows})
	query := clickhouse.CompiledQuery{SQL: "SELECT service", OutputFields: []string{"service"}}
	if err := executor.Execute(context.Background(), query, sink); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if len(sink.schema.Columns) != 1 || !sink.schema.Columns[0].Nullable {
		t.Fatalf("schema = %#v, want nullable service", sink.schema)
	}
	if len(sink.rows) != 1 || len(sink.rows[0]) != 1 || !sink.rows[0][0].IsNull() {
		t.Fatalf("rows = %#v, want one null service value", sink.rows)
	}
}

func TestExecutorRejectsColumnDriftAndPropagatesSinkLimit(t *testing.T) {
	t.Parallel()
	rows := &fakeRows{
		columns: []string{"wrong"},
		types:   []driver.ColumnType{fakeColumnType{name: "wrong", databaseType: "String", scanType: reflect.TypeFor[string]()}},
	}
	executor := mustExecutor(t, &fakeQueryConnection{rows: rows})
	err := executor.Execute(context.Background(), clickhouse.CompiledQuery{SQL: "SELECT 1", OutputFields: []string{"expected"}}, &fakeSink{})
	if !errors.Is(err, searchjobs.ErrInvalidResult) {
		t.Fatalf("column drift error = %v", err)
	}

	rows = &fakeRows{
		columns: []string{"message"},
		types:   []driver.ColumnType{fakeColumnType{name: "message", databaseType: "String", scanType: reflect.TypeFor[string]()}},
		data:    [][]any{{"one"}, {"two"}},
	}
	sink := &fakeSink{addErr: searchjobs.ErrRowLimit}
	executor = mustExecutor(t, &fakeQueryConnection{rows: rows})
	err = executor.Execute(context.Background(), clickhouse.CompiledQuery{SQL: "SELECT message", OutputFields: []string{"message"}}, sink)
	if !errors.Is(err, searchjobs.ErrRowLimit) || rows.nextCalls != 1 || !rows.closed {
		t.Fatalf("sink error=%v next=%d closed=%v", err, rows.nextCalls, rows.closed)
	}
}

func TestQueryIntegrationDockerArgsRedactCredentials(t *testing.T) {
	t.Parallel()

	const firstSecret = "first-secret"
	const secondSecret = "second-secret"
	got := queryIntegrationRedactedDockerArgs([]string{
		"run", "--env", "CLICKHOUSE_PASSWORD=" + firstSecret,
		"clickhouse-client", "--password", secondSecret, "--multiquery",
	})
	if strings.Contains(got, firstSecret) || strings.Contains(got, secondSecret) ||
		!strings.Contains(got, "CLICKHOUSE_PASSWORD=[REDACTED]") ||
		!strings.Contains(got, "--password [REDACTED]") {
		t.Fatalf("redacted Docker args = %q", got)
	}
}

func TestExecutorBuildsStatsCountSchemaFromNativeTypes(t *testing.T) {
	t.Parallel()

	rows := &fakeRows{
		columns: []string{"host", "count"},
		types: []driver.ColumnType{
			fakeColumnType{name: "host", databaseType: "String", scanType: reflect.TypeFor[string]()},
			fakeColumnType{name: "count", databaseType: "UInt64", scanType: reflect.TypeFor[uint64]()},
		},
		data: [][]any{{"api", uint64(3)}},
	}
	sink := &fakeSink{}
	executor := mustExecutor(t, &fakeQueryConnection{rows: rows})
	if err := executor.Execute(context.Background(), clickhouse.CompiledQuery{
		SQL: "SELECT host, count() AS count FROM events GROUP BY host", OutputFields: []string{"host", "count"},
	}, sink); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if len(sink.schema.Columns) != 2 || sink.schema.Columns[0].Kind != searchjobs.ValueKindString ||
		sink.schema.Columns[1].Kind != searchjobs.ValueKindUnsigned {
		t.Fatalf("schema = %#v", sink.schema)
	}
	if got, ok := sink.rows[0][1].Unsigned(); !ok || got != 3 {
		t.Fatalf("count cell = %d, %v", got, ok)
	}
}

func TestExecutorPublishesStatsValuesAsTypedMultivalueList(t *testing.T) {
	t.Parallel()

	rows := &fakeRows{
		columns: []string{"service", "users"},
		types: []driver.ColumnType{
			fakeColumnType{name: "service", databaseType: "String", scanType: reflect.TypeFor[string]()},
			fakeColumnType{name: "users", databaseType: "Array(String)", scanType: reflect.TypeFor[[]string]()},
		},
		data: [][]any{{"api", []string{"10", "2", "Alice", "alice"}}},
	}
	sink := &fakeSink{}
	executor := mustExecutor(t, &fakeQueryConnection{rows: rows})
	if err := executor.Execute(context.Background(), clickhouse.CompiledQuery{
		SQL:          "SELECT service, values FROM events",
		OutputFields: []string{"service", "users"},
	}, sink); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if len(sink.schema.Columns) != 2 ||
		sink.schema.Columns[1] != (searchjobs.Column{
			Name: "users", Kind: searchjobs.ValueKindList, Multivalue: true,
		}) {
		t.Fatalf("schema = %#v", sink.schema)
	}
	items, ok := sink.rows[0][1].List()
	if !ok || len(items) != 4 {
		t.Fatalf("values cell = %#v", sink.rows[0][1])
	}
	for index, want := range []string{"10", "2", "Alice", "alice"} {
		if got, stringOK := items[index].String(); !stringOK || got != want {
			t.Fatalf("values item %d = %q/%v, want %q", index, got, stringOK, want)
		}
	}
}

func TestExecutorPublishesStatsListInOrderWithDuplicatesAndBinaryMembers(t *testing.T) {
	t.Parallel()

	invalidUTF8 := string([]byte{0xff, 0x00})
	rows := &fakeRows{
		columns: []string{"ordered"},
		types: []driver.ColumnType{
			fakeColumnType{
				name:         "ordered",
				databaseType: "Array(String)",
				scanType:     reflect.TypeFor[[]string](),
			},
		},
		data: [][]any{{[]string{"duplicate", invalidUTF8, "duplicate"}}},
	}
	sink := &fakeSink{}
	executor := mustExecutor(t, &fakeQueryConnection{rows: rows})
	if err := executor.Execute(context.Background(), clickhouse.CompiledQuery{
		SQL:          "SELECT ordered FROM events",
		OutputFields: []string{"ordered"},
	}, sink); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if sink.schema.Columns[0] != (searchjobs.Column{
		Name: "ordered", Kind: searchjobs.ValueKindList, Multivalue: true,
	}) {
		t.Fatalf("schema = %#v", sink.schema)
	}
	items, ok := sink.rows[0][0].List()
	if !ok || len(items) != 3 {
		t.Fatalf("list cell = %#v", sink.rows[0][0])
	}
	for _, index := range []int{0, 2} {
		if got, stringOK := items[index].String(); !stringOK || got != "duplicate" {
			t.Fatalf("list item %d = %#v, want duplicate String", index, items[index])
		}
	}
	if got, bytesOK := items[1].Bytes(); !bytesOK || !slices.Equal(got, []byte{0xff, 0x00}) {
		t.Fatalf("binary list item = %x/%v, want ff00 Bytes", got, bytesOK)
	}
}

func TestExecutorPublishesStatsChronologicalValuesAsCanonicalStrings(t *testing.T) {
	t.Parallel()

	invalidUTF8 := string([]byte{0xff, 0x00})
	rows := &fakeRows{
		columns: []string{"earliest_number", "latest_bool", "absent", "earliest_raw"},
		types: []driver.ColumnType{
			fakeColumnType{
				name:         "earliest_number",
				databaseType: "Dynamic",
				scanType:     reflect.TypeFor[any](),
			},
			fakeColumnType{
				name:         "latest_bool",
				databaseType: "Dynamic",
				scanType:     reflect.TypeFor[any](),
			},
			fakeColumnType{
				name:         "absent",
				databaseType: "Dynamic",
				scanType:     reflect.TypeFor[any](),
			},
			fakeColumnType{
				name:         "earliest_raw",
				databaseType: "Dynamic",
				scanType:     reflect.TypeFor[any](),
			},
		},
		data: [][]any{{
			chcol.NewDynamicWithType("503", "String"),
			chcol.NewDynamicWithType("false", "String"),
			chcol.NewDynamicWithType(nil, ""),
			chcol.NewDynamicWithType(invalidUTF8, "String"),
		}},
	}
	sink := &fakeSink{}
	executor := mustExecutor(t, &fakeQueryConnection{rows: rows})
	if err := executor.Execute(context.Background(), clickhouse.CompiledQuery{
		SQL: "SELECT earliest_number, latest_bool, absent, earliest_raw",
		OutputFields: []string{
			"earliest_number",
			"latest_bool",
			"absent",
			"earliest_raw",
		},
	}, sink); err != nil {
		t.Fatalf("Execute: %v", err)
	}

	for index, name := range rows.columns {
		if sink.schema.Columns[index] != (searchjobs.Column{
			Name: name, Kind: searchjobs.ValueKindMixed, Nullable: true,
		}) {
			t.Fatalf("chronological schema column %d = %#v", index, sink.schema.Columns[index])
		}
	}
	if got, ok := sink.rows[0][0].String(); !ok || got != "503" {
		t.Fatalf("earliest numeric spelling = %q/%v, want String(503)", got, ok)
	}
	if got, ok := sink.rows[0][1].String(); !ok || got != "false" {
		t.Fatalf("latest boolean spelling = %q/%v, want String(false)", got, ok)
	}
	if !sink.rows[0][2].IsNull() {
		t.Fatalf("absent chronological value = %#v, want null", sink.rows[0][2])
	}
	if got, ok := sink.rows[0][3].Bytes(); !ok || !slices.Equal(got, []byte{0xff, 0x00}) {
		t.Fatalf("earliest binary raw = %x/%v, want ff00 Bytes", got, ok)
	}
}

func TestConfigFromPolicyDerivesCheckedResultGuards(t *testing.T) {
	t.Parallel()
	policy := searchlimits.Default()
	config, err := ConfigFromPolicy(policy)
	if err != nil {
		t.Fatal(err)
	}
	if config.MaxExecutionTime != policy.MaxRuntime ||
		config.MaxResultRows != policy.MaxResultRows+1 ||
		config.MaxResultBytes != policy.MaxResultBytes*2 ||
		config.MaxRowsToGroupBy != policy.MaxGroupedRows ||
		config.MaxThreads != policy.MaxThreads {
		t.Fatalf("ConfigFromPolicy() = %+v", config)
	}
	invalid := policy
	invalid.MaxResultRows = math.MaxUint64
	if _, err := ConfigFromPolicy(invalid); err == nil {
		t.Fatal("ConfigFromPolicy() accepted an invalid overflowing policy")
	}
}

func TestBoundLimitSourceUpdatesNewOperationsAndPreservesAdmissionSnapshots(t *testing.T) {
	t.Parallel()
	initial := searchlimits.Default()
	source, err := searchlimits.NewSource(initial)
	if err != nil {
		t.Fatal(err)
	}
	executor := mustExecutor(t, &fakeQueryConnection{})
	if err := executor.BindLimitSource(source); err != nil {
		t.Fatal(err)
	}
	before, err := executor.settingsForContext(context.Background(), clickhouse.CompiledQuery{})
	if err != nil {
		t.Fatal(err)
	}
	updated := initial
	updated.MaxRuntime = 7 * time.Minute
	updated.MaxResultRows = 25_000
	updated.MaxThreads = 9
	if err := source.Store(updated); err != nil {
		t.Fatal(err)
	}
	after, err := executor.settingsForContext(context.Background(), clickhouse.CompiledQuery{})
	if err != nil {
		t.Fatal(err)
	}
	captured, err := executor.settingsForContext(
		searchlimits.WithPolicy(context.Background(), initial),
		clickhouse.CompiledQuery{},
	)
	if err != nil {
		t.Fatal(err)
	}
	if before["max_execution_time"] != uint64(initial.MaxRuntime/time.Second) ||
		after["max_execution_time"] != uint64(updated.MaxRuntime/time.Second) ||
		after["max_result_rows"] != updated.MaxResultRows+1 ||
		after["max_threads"] != updated.MaxThreads {
		t.Fatalf("live settings before/after = %#v / %#v", before, after)
	}
	if captured["max_execution_time"] != uint64(initial.MaxRuntime/time.Second) ||
		captured["max_result_rows"] != initial.MaxResultRows+1 ||
		captured["max_threads"] != initial.MaxThreads {
		t.Fatalf("captured settings changed after publication: %#v", captured)
	}
}

func TestQuerySettingsAreReadOnlyAndBounded(t *testing.T) {
	t.Parallel()
	settings, err := querySettings(Config{})
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{
		"readonly", "max_execution_time", "max_memory_usage", "max_rows_to_read", "max_bytes_to_read",
		"max_result_rows", "max_result_bytes", "max_rows_to_group_by", "max_threads", "max_query_size",
		"max_ast_elements", "max_expanded_ast_elements", "max_subquery_depth", "enable_materialized_cte",
		"short_circuit_function_evaluation",
	} {
		if _, exists := settings[name]; !exists {
			t.Errorf("missing query setting %q", name)
		}
	}
	if settings["readonly"] != uint8(2) || settings["result_overflow_mode"] != "throw" ||
		settings["read_overflow_mode"] != "throw" || settings["group_by_overflow_mode"] != "throw" {
		t.Fatalf("unsafe settings = %#v", settings)
	}
	if settings["max_rows_to_group_by"] != defaultMaxResultRows {
		t.Fatalf("default group cap = %v, want %d", settings["max_rows_to_group_by"], defaultMaxResultRows)
	}
	if settings["max_query_size"] != defaultMaxQueryBytes {
		t.Fatalf("default query cap = %v, want %d", settings["max_query_size"], defaultMaxQueryBytes)
	}
	if settings["max_ast_elements"] != defaultMaxASTElements ||
		settings["max_expanded_ast_elements"] != defaultMaxExpandedASTElements {
		t.Fatalf(
			"AST settings = (%v, %v), want (%d, %d)",
			settings["max_ast_elements"],
			settings["max_expanded_ast_elements"],
			defaultMaxASTElements,
			defaultMaxExpandedASTElements,
		)
	}
	if settings["max_subquery_depth"] != defaultMaxSubqueryDepth {
		t.Fatalf(
			"subquery depth setting = %v, want %d",
			settings["max_subquery_depth"],
			defaultMaxSubqueryDepth,
		)
	}
	if settings["enable_materialized_cte"] != uint8(1) {
		t.Fatalf("materialized CTE setting = %v, want 1", settings["enable_materialized_cte"])
	}
	wantTextIndexSettingNames := []string{
		"enable_full_text_index",
		"query_plan_direct_read_from_text_index",
		"use_skip_indexes_on_data_read",
		"use_skip_indexes",
	}
	if !slices.Equal(requiredTextIndexSettingNames[:], wantTextIndexSettingNames) {
		t.Fatalf(
			"required text-index settings = %q, want %q",
			requiredTextIndexSettingNames,
			wantTextIndexSettingNames,
		)
	}
	for _, name := range requiredTextIndexSettingNames {
		if settings[name] != uint8(1) {
			t.Errorf("text-index setting %s = %v, want 1", name, settings[name])
		}
	}
	if settings["short_circuit_function_evaluation"] != "enable" {
		t.Fatalf("short-circuit setting = %v, want enable", settings["short_circuit_function_evaluation"])
	}
	custom, err := querySettings(Config{MaxResultRows: 77, MaxRowsToGroupBy: 33})
	if err != nil {
		t.Fatal(err)
	}
	if custom["max_result_rows"] != uint64(77) || custom["max_rows_to_group_by"] != uint64(33) {
		t.Fatalf("custom result/group caps = %#v", custom)
	}
	aligned, err := querySettings(Config{MaxResultRows: 77})
	if err != nil {
		t.Fatal(err)
	}
	if aligned["max_rows_to_group_by"] != uint64(77) {
		t.Fatalf("implicit group cap = %v, want result cap 77", aligned["max_rows_to_group_by"])
	}
	if _, err := querySettings(Config{MaxExecutionTime: -time.Second}); err == nil {
		t.Fatal("negative execution time accepted")
	}
}

func TestExecutorExpandsOnlyOptedInTimechartGroupBudget(t *testing.T) {
	t.Parallel()

	settings, err := querySettings(Config{})
	if err != nil {
		t.Fatal(err)
	}
	executor := &Executor{settings: mustValidatedSettings(t, settings), expandTimechartGroupLimit: true}
	dense := timechartQuery(time.Unix(0, 0).UTC(), 5_001)
	dense.Timechart.MaxSeries = 2
	denseSettings := executor.settingsFor(dense)
	if got, want := denseSettings["max_rows_to_group_by"], maximumRuntimeWideTimechartGroups; got != want {
		t.Fatalf("dense timechart group cap = %v, want %d", got, want)
	}
	if got := settings["max_rows_to_group_by"]; got != defaultMaxResultRows {
		t.Fatalf("base settings were mutated: cap=%v", got)
	}
	maximum := timechartQuery(time.Unix(0, 0).UTC(), maximumTimechartBuckets)
	if got, want := executor.settingsFor(maximum)["max_rows_to_group_by"], uint64(130_000); got != want {
		t.Fatalf("maximum timechart group cap = %v, want %d", got, want)
	}
	fixed := fixedTimechartQuery(time.Unix(0, 0).UTC(), maximumTimechartBuckets)
	if got, want := executor.settingsFor(fixed)["max_rows_to_group_by"], defaultMaxResultRows; got != want {
		t.Fatalf("fixed timechart group cap = %v, want unchanged base cap %d", got, want)
	}
	if got := executor.settingsFor(clickhouse.CompiledQuery{SQL: "SELECT 1"})["max_rows_to_group_by"]; got != defaultMaxResultRows {
		t.Fatalf("ordinary query group cap = %v, want %d", got, defaultMaxResultRows)
	}

	customSettings, err := querySettings(Config{MaxRowsToGroupBy: 7})
	if err != nil {
		t.Fatal(err)
	}
	custom := &Executor{settings: mustValidatedSettings(t, customSettings)}
	if got := custom.settingsFor(dense)["max_rows_to_group_by"]; got != uint64(7) {
		t.Fatalf("explicit group cap = %v, want 7", got)
	}
	customExpanded := &Executor{settings: mustValidatedSettings(t, customSettings), expandTimechartGroupLimit: true}
	if got, want := customExpanded.settingsFor(dense)["max_rows_to_group_by"], maximumRuntimeWideTimechartGroups; got != want {
		t.Fatalf("opted-in timechart group cap = %v, want %d", got, want)
	}
	fixed.Timechart.BucketCount = 5_001
	if got, want := customExpanded.settingsFor(fixed)["max_rows_to_group_by"], uint64(5_001); got != want {
		t.Fatalf("opted-in fixed timechart group cap = %v, want %d", got, want)
	}
	if got := customExpanded.settingsFor(clickhouse.CompiledQuery{SQL: "SELECT ordinary"})["max_rows_to_group_by"]; got != uint64(7) {
		t.Fatalf("opted-in ordinary group cap = %v, want 7", got)
	}

	// A chart's first aggregation retains exact raw (row, label) pairs before
	// reducing to the published width, so every non-percentile chart receives
	// the fixed 130k raw-work allowance.
	pivot := chartQuery("path", clickhouse.ChartRowKindString, "String")
	if got, want := executor.settingsFor(pivot)["max_rows_to_group_by"], uint64(130_000); got != want {
		t.Fatalf("chart group cap = %v, want %d", got, want)
	}
	// A narrow public shape still needs the same independent raw-pair allowance.
	narrow := chartQuery("path", clickhouse.ChartRowKindString, "String")
	narrow.Chart.RowLimit = 500
	narrow.Chart.MaxSeries = 2
	if got, want := executor.settingsFor(narrow)["max_rows_to_group_by"], maximumRuntimeWideTimechartGroups; got != want {
		t.Fatalf("narrow chart group cap = %v, want fixed raw-work cap %d", got, want)
	}
	if got := custom.settingsFor(pivot)["max_rows_to_group_by"]; got != uint64(7) {
		t.Fatalf("explicit chart group cap = %v, want 7", got)
	}
	if got := settings["max_rows_to_group_by"]; got != defaultMaxResultRows {
		t.Fatalf("base settings were mutated by chart: cap=%v", got)
	}
}

func TestClassifyQueryErrorsRedactsIntoStableCategories(t *testing.T) {
	t.Parallel()
	if err := classifyQueryError(context.Background(), io.ErrUnexpectedEOF); !errors.Is(err, searchjobs.ErrStorageUnavailable) {
		t.Fatalf("network error = %v", err)
	}
	if err := classifyQueryError(context.Background(), clickhousedriver.ErrAcquireConnTimeout); !errors.Is(err, searchjobs.ErrStorageUnavailable) {
		t.Fatalf("connection-pool timeout = %v", err)
	}
	if err := classifyQueryError(context.Background(), sqldriver.ErrBadConn); !errors.Is(err, searchjobs.ErrStorageUnavailable) {
		t.Fatalf("bad connection = %v", err)
	}
	resource := &clickhousedriver.Exception{Code: 241, Name: "MEMORY_LIMIT_EXCEEDED"}
	if err := classifyQueryError(context.Background(), resource); !errors.Is(err, searchjobs.ErrExecutionLimit) {
		t.Fatalf("resource error = %v", err)
	}
	tooLarge := &clickhousedriver.Exception{Code: 229, Name: "QUERY_IS_TOO_LARGE"}
	if err := classifyQueryError(context.Background(), tooLarge); !errors.Is(err, searchjobs.ErrExecutionLimit) {
		t.Fatalf("query-size error = %v", err)
	}
	tooManyGroups := &clickhousedriver.Exception{Code: 158, Name: "TOO_MANY_ROWS", Message: "secret query detail"}
	if err := classifyQueryError(context.Background(), tooManyGroups); !errors.Is(err, searchjobs.ErrExecutionLimit) || strings.Contains(err.Error(), "secret") {
		t.Fatalf("group cap error = %v", err)
	}
	tooDeep := &clickhousedriver.Exception{Code: 162, Name: "TOO_DEEP_SUBQUERIES", Message: "secret generated SQL"}
	if err := classifyQueryError(context.Background(), tooDeep); !errors.Is(err, searchjobs.ErrExecutionLimit) ||
		strings.Contains(err.Error(), "secret") {
		t.Fatalf("subquery-depth error = %v", err)
	}
	for _, marker := range []string{
		clickhouse.UnsupportedStatsByValueMarker,
		clickhouse.UnsupportedStatsMeasureValueMarker,
		clickhouse.UnsupportedDedupValueMarker,
		clickhouse.UnsupportedNumericBinValueMarker,
		clickhouse.UnsupportedSpathValueMarker,
		clickhouse.KnowledgeSelectorInvalidUTF8Marker,
	} {
		unsupported := &clickhousedriver.Exception{
			Code:    395,
			Name:    "FUNCTION_THROW_IF_VALUE_IS_NON_ZERO",
			Message: marker + "; generated SQL contained secret",
		}
		if err := classifyQueryError(context.Background(), unsupported); !errors.Is(err, searchjobs.ErrUnsupportedValue) || strings.Contains(err.Error(), "secret") {
			t.Fatalf("unsupported dynamic value marker %q error = %v", marker, err)
		}
	}
	for _, classified := range executionLimitMarkers {
		limit := &clickhousedriver.Exception{
			Code:    395,
			Name:    "FUNCTION_THROW_IF_VALUE_IS_NON_ZERO",
			Message: classified.marker + "; generated SQL contained secret",
		}
		if err := classifyQueryError(context.Background(), limit); !errors.Is(err, searchjobs.ErrExecutionLimit) ||
			strings.Contains(err.Error(), "secret") || strings.Contains(err.Error(), classified.marker) ||
			!strings.Contains(err.Error(), classified.message) {
			t.Fatalf("execution-limit marker %q classification = %v", classified.marker, err)
		}
	}
	for _, marker := range []string{
		clickhouse.UnsupportedStatsByValueMarker,
		clickhouse.UnsupportedStatsMeasureValueMarker,
		clickhouse.UnsupportedDedupValueMarker,
		clickhouse.UnsupportedNumericBinValueMarker,
		clickhouse.UnsupportedSpathValueMarker,
		clickhouse.KnowledgeSelectorInvalidUTF8Marker,
	} {
		wrongCode := &clickhousedriver.Exception{Code: 241, Message: marker}
		if err := classifyQueryError(context.Background(), wrongCode); !errors.Is(err, searchjobs.ErrExecutionLimit) || errors.Is(err, searchjobs.ErrUnsupportedValue) {
			t.Fatalf("marker %q on an unrelated exception = %v", marker, err)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := classifyQueryError(ctx, errors.New("contains secret")); !errors.Is(err, context.Canceled) || strings.Contains(err.Error(), "secret") {
		t.Fatalf("canceled error = %v", err)
	}
}

func mustExecutor(t *testing.T, connection queryConnection) *Executor {
	t.Helper()
	settings, err := querySettings(Config{})
	if err != nil {
		t.Fatal(err)
	}
	return &Executor{
		connection:                connection,
		settings:                  mustValidatedSettings(t, settings),
		expandTimechartGroupLimit: true,
		newQueryID:                func() (string, error) { return "open-splunk-search-test", nil },
	}
}

type fakeQueryConnection struct {
	rows  driver.Rows
	err   error
	query string
	args  []any
}

func (connection *fakeQueryConnection) Query(_ context.Context, query string, args ...any) (driver.Rows, error) {
	connection.query = query
	connection.args = slices.Clone(args)
	return connection.rows, connection.err
}

type fakeColumnType struct {
	name, databaseType string
	scanType           reflect.Type
	nullable           bool
}

func (column fakeColumnType) Name() string             { return column.name }
func (column fakeColumnType) Nullable() bool           { return column.nullable }
func (column fakeColumnType) ScanType() reflect.Type   { return column.scanType }
func (column fakeColumnType) DatabaseTypeName() string { return column.databaseType }

type fakeRows struct {
	columns     []string
	types       []driver.ColumnType
	data        [][]any
	index       int
	nextCalls   int
	err         error
	closeErr    error
	closed      bool
	afterScan   func()
	observeScan func([]any)
}

func (rows *fakeRows) Next() bool {
	if rows.index >= len(rows.data) {
		return false
	}
	rows.nextCalls++
	rows.index++
	return true
}
func (rows *fakeRows) Scan(destinations ...any) error {
	if rows.index == 0 || rows.index > len(rows.data) {
		return errors.New("scan outside active row")
	}
	values := rows.data[rows.index-1]
	if len(values) != len(destinations) {
		return errors.New("destination count mismatch")
	}
	if rows.observeScan != nil {
		rows.observeScan(destinations)
	}
	for index, destination := range destinations {
		if err := assignFakeScan(destination, values[index]); err != nil {
			return err
		}
	}
	if rows.afterScan != nil {
		rows.afterScan()
	}
	return nil
}
func (rows *fakeRows) ScanStruct(any) error             { return errors.New("unused") }
func (rows *fakeRows) ColumnTypes() []driver.ColumnType { return rows.types }
func (rows *fakeRows) Totals(...any) error              { return nil }
func (rows *fakeRows) Columns() []string                { return rows.columns }
func (rows *fakeRows) Close() error                     { rows.closed = true; return rows.closeErr }
func (rows *fakeRows) Err() error                       { return rows.err }
func (rows *fakeRows) HasData() bool                    { return len(rows.data) != 0 }

func assignFakeScan(destination, source any) error {
	target := reflect.ValueOf(destination)
	if target.Kind() != reflect.Pointer || target.IsNil() {
		return errors.New("fake scan destination is not a pointer")
	}
	return assignFakeValue(target.Elem(), source)
}

func assignFakeValue(target reflect.Value, source any) error {
	if source == nil {
		target.SetZero()
		return nil
	}
	value := reflect.ValueOf(source)
	if value.Type().AssignableTo(target.Type()) {
		target.Set(value)
		return nil
	}
	if target.Kind() == reflect.Pointer {
		pointer := reflect.New(target.Type().Elem())
		if err := assignFakeValue(pointer.Elem(), source); err != nil {
			return err
		}
		target.Set(pointer)
		return nil
	}
	return errors.New("fake scan type mismatch")
}

type fakeSink struct {
	schema   searchjobs.Schema
	rows     [][]searchjobs.Value
	events   []string
	setErr   error
	addErr   error
	setCalls int
}

func (sink *fakeSink) SetSchema(schema searchjobs.Schema) error {
	sink.setCalls++
	if sink.setErr != nil {
		return sink.setErr
	}
	sink.events = append(sink.events, "schema")
	sink.schema = schema
	return nil
}
func (sink *fakeSink) AddRow(values []searchjobs.Value) error {
	if sink.addErr != nil {
		return sink.addErr
	}
	sink.events = append(sink.events, "row")
	sink.rows = append(sink.rows, slices.Clone(values))
	return nil
}
