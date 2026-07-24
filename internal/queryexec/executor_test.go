package queryexec

import (
	"context"
	"errors"
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
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
	"github.com/google/uuid"
	"github.com/shopspring/decimal"
)

func TestExecutorStreamsTypedRowsAndExactSchema(t *testing.T) {
	t.Parallel()
	timestamp := time.Date(2026, time.July, 21, 3, 4, 5, 123000000, time.UTC)
	rows := &fakeRows{
		columns: []string{"_time", "message", "status", "_raw"},
		types: []driver.ColumnType{
			fakeColumnType{name: "_time", databaseType: "DateTime64(9, 'UTC')", scanType: reflect.TypeOf(time.Time{})},
			fakeColumnType{name: "message", databaseType: "Nullable(String)", scanType: reflect.TypeOf((*string)(nil)), nullable: true},
			fakeColumnType{name: "status", databaseType: "Dynamic", scanType: reflect.TypeOf(chcol.Dynamic{})},
			fakeColumnType{name: "_raw", databaseType: "String", scanType: reflect.TypeOf("")},
		},
		data: [][]any{
			{timestamp, "hello", chcol.NewDynamicWithType(int64(500), "Int64"), "valid"},
			{timestamp.Add(time.Second), nil, chcol.NewDynamicWithType("500", "String"), string([]byte{0xff, 0x00})},
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
	if len(sink.rows) != 2 || !rows.closed {
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
	if value, ok := sink.rows[1][3].Bytes(); !ok || !slices.Equal(value, []byte{0xff, 0}) {
		t.Fatalf("binary raw = %#v", sink.rows[1][3])
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
			rows.types[1] = fakeColumnType{name: clickhouse.TimechartNamesColumn, databaseType: "Array(String)", scanType: reflect.TypeOf([]string{}), nullable: true}
		}, want: searchjobs.ErrInvalidResult, queryIssued: true},
		{name: "column type name drift", mutate: func(rows *fakeRows, _ *clickhouse.CompiledQuery) {
			rows.types[1] = fakeColumnType{name: "wrong", databaseType: "Array(String)", scanType: reflect.TypeOf([]string{})}
		}, want: searchjobs.ErrInvalidResult, queryIssued: true},
		{name: "typed nil ordinal type", mutate: func(rows *fakeRows, _ *clickhouse.CompiledQuery) {
			var columnType *fakeColumnType
			rows.types[0] = columnType
		}, want: searchjobs.ErrInvalidResult, queryIssued: true},
		{name: "wrapped ordinal type", mutate: func(rows *fakeRows, _ *clickhouse.CompiledQuery) {
			rows.types[0] = fakeColumnType{name: clickhouse.TimechartOrdinalColumn, databaseType: "Nullable(UInt64)", scanType: reflect.TypeOf(uint64(0))}
		}, want: searchjobs.ErrInvalidResult, queryIssued: true},
		{name: "wrong ordinal width", mutate: func(rows *fakeRows, _ *clickhouse.CompiledQuery) {
			rows.types[0] = fakeColumnType{name: clickhouse.TimechartOrdinalColumn, databaseType: "UInt32", scanType: reflect.TypeOf(uint32(0))}
		}, want: searchjobs.ErrInvalidResult, queryIssued: true},
		{name: "wrong native ordinal", mutate: func(rows *fakeRows, _ *clickhouse.CompiledQuery) {
			rows.types[0] = fakeColumnType{name: clickhouse.TimechartOrdinalColumn, databaseType: "UInt64", scanType: reflect.TypeOf(int64(0))}
			rows.data[0][0] = int64(0)
		}, want: searchjobs.ErrInvalidResult, queryIssued: true},
		{name: "wrong array type", mutate: func(rows *fakeRows, _ *clickhouse.CompiledQuery) {
			rows.types[2] = fakeColumnType{name: clickhouse.TimechartCountsColumn, databaseType: "Array(UInt32)", scanType: reflect.TypeOf([]uint64{})}
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
			query.Timechart.MaxSeries = maximumTimechartSeries + 1
		}, want: searchjobs.ErrInvalidResult},
		{name: "excessive metadata label", mutate: func(_ *fakeRows, query *clickhouse.CompiledQuery) {
			query.Timechart.MaxLabelBytes = maximumTimechartLabel + 1
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
			fakeColumnType{name: clickhouse.TimechartOrdinalColumn, databaseType: "UInt64", scanType: reflect.TypeOf(uint64(0))},
			fakeColumnType{name: clickhouse.TimechartNamesColumn, databaseType: "Array(String)", scanType: reflect.TypeOf([]string{})},
			fakeColumnType{name: clickhouse.TimechartCountsColumn, databaseType: "Array(UInt64)", scanType: reflect.TypeOf([]uint64{})},
			fakeColumnType{name: clickhouse.TimechartInvalidColumn, databaseType: "UInt8", scanType: reflect.TypeOf(uint8(0))},
		},
		data: make([][]any, len(counts)),
	}
	for index, values := range counts {
		rows.data[index] = []any{uint64(index), slices.Clone(names), slices.Clone(values), uint8(0)}
	}
	return rows
}

func setTimechartNames(rows *fakeRows, names []string) {
	for _, row := range rows.data {
		row[1] = slices.Clone(names)
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
			fakeColumnType{name: "amount", databaseType: "Decimal(38, 5)", scanType: reflect.TypeOf(decimal.Decimal{})},
			fakeColumnType{name: "elapsed", databaseType: "Time64(6)", scanType: reflect.TypeOf(time.Duration(0))},
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
		test := test
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
				scanType:     reflect.TypeOf((*string)(nil)),
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
		types:   []driver.ColumnType{fakeColumnType{name: "wrong", databaseType: "String", scanType: reflect.TypeOf("")}},
	}
	executor := mustExecutor(t, &fakeQueryConnection{rows: rows})
	err := executor.Execute(context.Background(), clickhouse.CompiledQuery{SQL: "SELECT 1", OutputFields: []string{"expected"}}, &fakeSink{})
	if !errors.Is(err, searchjobs.ErrInvalidResult) {
		t.Fatalf("column drift error = %v", err)
	}

	rows = &fakeRows{
		columns: []string{"message"},
		types:   []driver.ColumnType{fakeColumnType{name: "message", databaseType: "String", scanType: reflect.TypeOf("")}},
		data:    [][]any{{"one"}, {"two"}},
	}
	sink := &fakeSink{addErr: searchjobs.ErrRowLimit}
	executor = mustExecutor(t, &fakeQueryConnection{rows: rows})
	err = executor.Execute(context.Background(), clickhouse.CompiledQuery{SQL: "SELECT message", OutputFields: []string{"message"}}, sink)
	if !errors.Is(err, searchjobs.ErrRowLimit) || rows.nextCalls != 1 || !rows.closed {
		t.Fatalf("sink error=%v next=%d closed=%v", err, rows.nextCalls, rows.closed)
	}
}

func TestExecutorBuildsStatsCountSchemaFromNativeTypes(t *testing.T) {
	t.Parallel()

	rows := &fakeRows{
		columns: []string{"host", "count"},
		types: []driver.ColumnType{
			fakeColumnType{name: "host", databaseType: "String", scanType: reflect.TypeOf("")},
			fakeColumnType{name: "count", databaseType: "UInt64", scanType: reflect.TypeOf(uint64(0))},
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

func TestQuerySettingsAreReadOnlyAndBounded(t *testing.T) {
	t.Parallel()
	settings, err := querySettings(Config{})
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{
		"readonly", "max_execution_time", "max_memory_usage", "max_rows_to_read", "max_bytes_to_read",
		"max_result_rows", "max_result_bytes", "max_rows_to_group_by", "max_threads", "max_query_size", "enable_materialized_cte",
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
	if settings["enable_materialized_cte"] != uint8(1) {
		t.Fatalf("materialized CTE setting = %v, want 1", settings["enable_materialized_cte"])
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
	executor := &Executor{settings: settings, expandTimechartGroupLimit: true}
	dense := timechartQuery(time.Unix(0, 0).UTC(), 5_001)
	dense.Timechart.MaxSeries = 2
	denseSettings := executor.settingsFor(dense)
	if got, want := denseSettings["max_rows_to_group_by"], uint64(15_003); got != want {
		t.Fatalf("dense timechart group cap = %v, want %d", got, want)
	}
	if got := settings["max_rows_to_group_by"]; got != defaultMaxResultRows {
		t.Fatalf("base settings were mutated: cap=%v", got)
	}
	maximum := timechartQuery(time.Unix(0, 0).UTC(), maximumTimechartBuckets)
	if got, want := executor.settingsFor(maximum)["max_rows_to_group_by"], uint64(130_000); got != want {
		t.Fatalf("maximum timechart group cap = %v, want %d", got, want)
	}
	if got := executor.settingsFor(clickhouse.CompiledQuery{SQL: "SELECT 1"})["max_rows_to_group_by"]; got != defaultMaxResultRows {
		t.Fatalf("ordinary query group cap = %v, want %d", got, defaultMaxResultRows)
	}

	customSettings, err := querySettings(Config{MaxRowsToGroupBy: 7})
	if err != nil {
		t.Fatal(err)
	}
	custom := &Executor{settings: customSettings}
	if got := custom.settingsFor(dense)["max_rows_to_group_by"]; got != uint64(7) {
		t.Fatalf("explicit group cap = %v, want 7", got)
	}
	customExpanded := &Executor{settings: customSettings, expandTimechartGroupLimit: true}
	if got, want := customExpanded.settingsFor(dense)["max_rows_to_group_by"], uint64(15_003); got != want {
		t.Fatalf("opted-in timechart group cap = %v, want %d", got, want)
	}
	if got := customExpanded.settingsFor(clickhouse.CompiledQuery{SQL: "SELECT ordinary"})["max_rows_to_group_by"]; got != uint64(7) {
		t.Fatalf("opted-in ordinary group cap = %v, want 7", got)
	}
}

func TestClassifyQueryErrorsRedactsIntoStableCategories(t *testing.T) {
	t.Parallel()
	if err := classifyQueryError(context.Background(), io.ErrUnexpectedEOF); !errors.Is(err, searchjobs.ErrStorageUnavailable) {
		t.Fatalf("network error = %v", err)
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
	for _, marker := range []string{clickhouse.UnsupportedStatsByValueMarker, clickhouse.UnsupportedDedupValueMarker} {
		unsupported := &clickhousedriver.Exception{
			Code:    395,
			Name:    "FUNCTION_THROW_IF_VALUE_IS_NON_ZERO",
			Message: marker + "; generated SQL contained secret",
		}
		if err := classifyQueryError(context.Background(), unsupported); !errors.Is(err, searchjobs.ErrUnsupportedValue) || strings.Contains(err.Error(), "secret") {
			t.Fatalf("unsupported dynamic value marker %q error = %v", marker, err)
		}
	}
	rexLimit := &clickhousedriver.Exception{
		Code:    395,
		Name:    "FUNCTION_THROW_IF_VALUE_IS_NON_ZERO",
		Message: clickhouse.RexCaptureLimitMarker + "; generated SQL contained secret",
	}
	if err := classifyQueryError(context.Background(), rexLimit); !errors.Is(err, searchjobs.ErrExecutionLimit) ||
		strings.Contains(err.Error(), "secret") || strings.Contains(err.Error(), clickhouse.RexCaptureLimitMarker) {
		t.Fatalf("rex capture limit classification = %v", err)
	}
	wrongCode := &clickhousedriver.Exception{Code: 241, Message: clickhouse.UnsupportedStatsByValueMarker}
	if err := classifyQueryError(context.Background(), wrongCode); !errors.Is(err, searchjobs.ErrExecutionLimit) || errors.Is(err, searchjobs.ErrUnsupportedValue) {
		t.Fatalf("marker on an unrelated exception = %v", err)
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
		settings:                  settings,
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
	columns   []string
	types     []driver.ColumnType
	data      [][]any
	index     int
	nextCalls int
	err       error
	closeErr  error
	closed    bool
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
	for index, destination := range destinations {
		if err := assignFakeScan(destination, values[index]); err != nil {
			return err
		}
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
	setErr   error
	addErr   error
	setCalls int
}

func (sink *fakeSink) SetSchema(schema searchjobs.Schema) error {
	sink.setCalls++
	if sink.setErr != nil {
		return sink.setErr
	}
	sink.schema = schema
	return nil
}
func (sink *fakeSink) AddRow(values []searchjobs.Value) error {
	if sink.addErr != nil {
		return sink.addErr
	}
	sink.rows = append(sink.rows, slices.Clone(values))
	return nil
}
