package queryexec

import (
	"context"
	"errors"
	"io"
	"math"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
)

func TestExecutorBuffersAndPublishesFixedPercentileTimechart(t *testing.T) {
	t.Parallel()

	first := time.Date(2026, time.July, 22, 12, 0, 0, 0, time.UTC)
	rows := fixedValueTimechartRows(
		[]any{float64(100), nil, float64(350.5)},
		[]uint8{1, 1, 1},
	)
	sink := &fakeSink{}
	executor := mustExecutor(t, &fakeQueryConnection{rows: rows})
	if err := executor.Execute(
		context.Background(),
		fixedValueTimechartQuery(first, 3, "p95_ms", clickhouse.TimechartValueKindPercentile),
		sink,
	); err != nil {
		t.Fatalf("Execute: %v", err)
	}

	wantSchema := searchjobs.Schema{Columns: []searchjobs.Column{
		{Name: "_time", Kind: searchjobs.ValueKindTime},
		{Name: "p95_ms", Kind: searchjobs.ValueKindDouble, Nullable: true},
	}}
	if sink.setCalls != 1 || !reflect.DeepEqual(sink.schema, wantSchema) || !rows.closed {
		t.Fatalf("schema=%#v calls=%d rows closed=%v", sink.schema, sink.setCalls, rows.closed)
	}
	if len(sink.rows) != 3 {
		t.Fatalf("published rows = %d, want 3", len(sink.rows))
	}
	for index, row := range sink.rows {
		bucket, ok := row[0].Time()
		if !ok || !bucket.Equal(first.Add(time.Duration(index)*5*time.Minute)) {
			t.Fatalf("row %d bucket = %#v", index, row[0])
		}
	}
	if value, ok := sink.rows[0][1].Double(); !ok || value != 100 {
		t.Fatalf("first percentile = %v, %v", value, ok)
	}
	if !sink.rows[1][1].IsNull() {
		t.Fatalf("gap percentile = %#v, want null", sink.rows[1][1])
	}
	if value, ok := sink.rows[2][1].Double(); !ok || value != 350.5 {
		t.Fatalf("last percentile = %v, %v", value, ok)
	}
}

func TestExecutorDistinguishesEmptyAndAllIneligibleFixedPercentileInput(t *testing.T) {
	t.Parallel()

	first := time.Date(2026, time.July, 22, 12, 0, 0, 0, time.UTC)
	for _, test := range []struct {
		name       string
		presence   uint8
		wantRows   int
		wantAllNil bool
	}{
		{name: "empty upstream", presence: 0, wantRows: 0},
		{name: "rows with no eligible measures", presence: 1, wantRows: 3, wantAllNil: true},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			rows := fixedValueTimechartRows(
				[]any{nil, nil, nil},
				[]uint8{test.presence, test.presence, test.presence},
			)
			sink := &fakeSink{}
			executor := mustExecutor(t, &fakeQueryConnection{rows: rows})
			if err := executor.Execute(
				context.Background(),
				fixedValueTimechartQuery(first, 3, "latency_p95", clickhouse.TimechartValueKindPercentile),
				sink,
			); err != nil {
				t.Fatalf("Execute: %v", err)
			}
			if sink.setCalls != 1 || len(sink.schema.Columns) != 2 ||
				sink.schema.Columns[1] != (searchjobs.Column{
					Name: "latency_p95", Kind: searchjobs.ValueKindDouble, Nullable: true,
				}) || len(sink.rows) != test.wantRows {
				t.Fatalf("schema=%#v calls=%d rows=%d", sink.schema, sink.setCalls, len(sink.rows))
			}
			if test.wantAllNil {
				for index, row := range sink.rows {
					if !row[1].IsNull() {
						t.Fatalf("row %d percentile = %#v, want null", index, row[1])
					}
				}
			}
		})
	}
}

func TestExecutorAcceptsBoundedDottedFixedPercentileOutput(t *testing.T) {
	t.Parallel()

	valueField := strings.Repeat("a", 200) + "." + strings.Repeat("b", 200)
	first := time.Date(2026, time.July, 22, 12, 0, 0, 0, time.UTC)
	rows := fixedValueTimechartRows([]any{float64(10)}, []uint8{1})
	sink := &fakeSink{}
	if err := mustExecutor(t, &fakeQueryConnection{rows: rows}).Execute(
		context.Background(),
		fixedValueTimechartQuery(first, 1, valueField, clickhouse.TimechartValueKindPercentile),
		sink,
	); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if sink.setCalls != 1 || len(sink.schema.Columns) != 2 ||
		sink.schema.Columns[1].Name != valueField || len(sink.rows) != 1 {
		t.Fatalf("schema=%#v calls=%d rows=%d", sink.schema, sink.setCalls, len(sink.rows))
	}
}

func TestExecutorRejectsMalformedFixedPercentileTimechartAtomically(t *testing.T) {
	first := time.Date(2026, time.July, 22, 12, 0, 0, 0, time.UTC)
	tests := []struct {
		name        string
		mutate      func(*fakeRows, *clickhouse.CompiledQuery)
		queryIssued bool
	}{
		{
			name: "wrong percentile column",
			mutate: func(rows *fakeRows, _ *clickhouse.CompiledQuery) {
				rows.columns[1] = "wrong"
			},
			queryIssued: true,
		},
		{
			name: "nonnullable percentile",
			mutate: func(rows *fakeRows, _ *clickhouse.CompiledQuery) {
				rows.types[1] = fakeColumnType{
					name: clickhouse.TimechartValueColumn, databaseType: "Float64",
					scanType: reflect.TypeOf(float64(0)),
				}
			},
			queryIssued: true,
		},
		{
			name: "nullable wrapper with scalar scan type",
			mutate: func(rows *fakeRows, _ *clickhouse.CompiledQuery) {
				rows.types[1] = fakeColumnType{
					name: clickhouse.TimechartValueColumn, databaseType: "Nullable(Float64)",
					scanType: reflect.TypeOf(float64(0)), nullable: true,
				}
			},
			queryIssued: true,
		},
		{
			name: "nullable presence proof",
			mutate: func(rows *fakeRows, _ *clickhouse.CompiledQuery) {
				rows.types[2] = fakeColumnType{
					name: clickhouse.TimechartInputPresentColumn, databaseType: "Nullable(UInt8)",
					scanType: reflect.TypeOf((*uint8)(nil)), nullable: true,
				}
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
				rows.data = append(rows.data, []any{uint64(2), nil, uint8(1)})
			},
			queryIssued: true,
		},
		{
			name: "infinite percentile",
			mutate: func(rows *fakeRows, _ *clickhouse.CompiledQuery) {
				rows.data[0][1] = math.Inf(1)
			},
			queryIssued: true,
		},
		{
			name: "NaN percentile",
			mutate: func(rows *fakeRows, _ *clickhouse.CompiledQuery) {
				rows.data[0][1] = math.NaN()
			},
			queryIssued: true,
		},
		{
			name: "invalid presence value",
			mutate: func(rows *fakeRows, _ *clickhouse.CompiledQuery) {
				rows.data[0][2] = uint8(2)
			},
			queryIssued: true,
		},
		{
			name: "presence changes between buckets",
			mutate: func(rows *fakeRows, _ *clickhouse.CompiledQuery) {
				rows.data[1][2] = uint8(0)
			},
			queryIssued: true,
		},
		{
			name: "empty input carries a value",
			mutate: func(rows *fakeRows, _ *clickhouse.CompiledQuery) {
				for _, row := range rows.data {
					row[2] = uint8(0)
				}
			},
			queryIssued: true,
		},
		{
			name: "wrong public fields",
			mutate: func(_ *fakeRows, query *clickhouse.CompiledQuery) {
				query.OutputFields[1] = "other"
			},
		},
		{
			name: "wrong declared value field",
			mutate: func(_ *fakeRows, query *clickhouse.CompiledQuery) {
				query.Timechart.ValueField = "other"
			},
		},
		{
			name: "dynamic label bound",
			mutate: func(_ *fakeRows, query *clickhouse.CompiledQuery) {
				query.Timechart.MaxLabelBytes = 1
			},
		},
		{
			name: "invalid utf8 value field",
			mutate: func(_ *fakeRows, query *clickhouse.CompiledQuery) {
				query.Timechart.ValueField = string([]byte{0xff})
				query.OutputFields[1] = query.Timechart.ValueField
			},
		},
		{
			name: "private value field",
			mutate: func(_ *fakeRows, query *clickhouse.CompiledQuery) {
				query.Timechart.ValueField = "__OS_private"
				query.OutputFields[1] = query.Timechart.ValueField
			},
		},
		{
			name: "oversized value field",
			mutate: func(_ *fakeRows, query *clickhouse.CompiledQuery) {
				query.Timechart.ValueField = strings.Repeat("x", int(maximumTimechartLabel)+1)
				query.OutputFields[1] = query.Timechart.ValueField
			},
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			rows := fixedValueTimechartRows(
				[]any{float64(10), nil},
				[]uint8{1, 1},
			)
			query := fixedValueTimechartQuery(first, 2, "p95_ms", clickhouse.TimechartValueKindPercentile)
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
				t.Fatalf("malformed percentile timechart published schema=%d rows=%d", sink.setCalls, len(sink.rows))
			}
		})
	}
}

func TestExecutorFixedPercentileTimechartStreamFailuresAreAtomic(t *testing.T) {
	t.Parallel()

	first := time.Date(2026, time.July, 22, 12, 0, 0, 0, time.UTC)
	for _, test := range []struct {
		name   string
		mutate func(*fakeRows)
	}{
		{name: "iteration", mutate: func(rows *fakeRows) { rows.err = io.ErrUnexpectedEOF }},
		{name: "close", mutate: func(rows *fakeRows) { rows.closeErr = io.ErrUnexpectedEOF }},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			rows := fixedValueTimechartRows([]any{float64(10), nil}, []uint8{1, 1})
			test.mutate(rows)
			sink := &fakeSink{}
			err := mustExecutor(t, &fakeQueryConnection{rows: rows}).Execute(
				context.Background(),
				fixedValueTimechartQuery(first, 2, "p95_ms", clickhouse.TimechartValueKindPercentile),
				sink,
			)
			if !errors.Is(err, searchjobs.ErrStorageUnavailable) {
				t.Fatalf("Execute error = %v, want ErrStorageUnavailable", err)
			}
			if sink.setCalls != 0 || len(sink.rows) != 0 {
				t.Fatalf("failed percentile timechart published schema=%d rows=%d", sink.setCalls, len(sink.rows))
			}
		})
	}
}

func TestExecutorCancelsFixedPercentileTimechartBeforePublication(t *testing.T) {
	t.Parallel()

	first := time.Date(2026, time.July, 22, 12, 0, 0, 0, time.UTC)
	rows := fixedValueTimechartRows([]any{float64(10), nil}, []uint8{1, 1})
	ctx, cancel := context.WithCancel(context.Background())
	rows.afterScan = cancel
	sink := &fakeSink{}
	err := mustExecutor(t, &fakeQueryConnection{rows: rows}).Execute(
		ctx,
		fixedValueTimechartQuery(first, 2, "p95_ms", clickhouse.TimechartValueKindPercentile),
		sink,
	)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Execute error = %v, want context.Canceled", err)
	}
	if !rows.closed || rows.nextCalls != 1 {
		t.Fatalf("canceled stream closed=%v next calls=%d", rows.closed, rows.nextCalls)
	}
	if sink.setCalls != 0 || len(sink.rows) != 0 {
		t.Fatalf("canceled percentile timechart published schema=%d rows=%d", sink.setCalls, len(sink.rows))
	}
}

func fixedValueTimechartQuery(
	first time.Time,
	bucketCount uint64,
	valueField string,
	valueKind clickhouse.TimechartValueKind,
) clickhouse.CompiledQuery {
	return clickhouse.CompiledQuery{
		SQL:          "SELECT bounded_fixed_value_timechart",
		OutputFields: []string{"_time", valueField},
		Timechart: &clickhouse.TimechartOutput{
			Mode:          clickhouse.TimechartModeFixedValue,
			FirstBucket:   first,
			Span:          5 * time.Minute,
			BucketCount:   bucketCount,
			MaxSeries:     1,
			MaxLabelBytes: 0,
			ValueField:    valueField,
			ValueKind:     valueKind,
		},
	}
}

func fixedValueTimechartRows(values []any, presence []uint8) *fakeRows {
	if len(values) != len(presence) {
		panic("fixed value fixture width mismatch")
	}
	rows := &fakeRows{
		columns: []string{
			clickhouse.TimechartOrdinalColumn,
			clickhouse.TimechartValueColumn,
			clickhouse.TimechartInputPresentColumn,
		},
		types: []driver.ColumnType{
			fakeColumnType{
				name: clickhouse.TimechartOrdinalColumn, databaseType: "UInt64",
				scanType: reflect.TypeOf(uint64(0)),
			},
			fakeColumnType{
				name: clickhouse.TimechartValueColumn, databaseType: "Nullable(Float64)",
				scanType: reflect.TypeOf((*float64)(nil)), nullable: true,
			},
			fakeColumnType{
				name: clickhouse.TimechartInputPresentColumn, databaseType: "UInt8",
				scanType: reflect.TypeOf(uint8(0)),
			},
		},
		data: make([][]any, len(values)),
	}
	for index := range values {
		rows.data[index] = []any{uint64(index), values[index], presence[index]}
	}
	return rows
}
