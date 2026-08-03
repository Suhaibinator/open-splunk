package queryexec

import (
	"context"
	"errors"
	"io"
	"math"
	"reflect"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
)

func TestExecutorPublishesSplitSumAndAverageAsNullableWideValues(t *testing.T) {
	t.Parallel()

	first := time.Date(2026, time.July, 22, 12, 0, 0, 0, time.UTC)
	for _, test := range []struct {
		name      string
		kind      clickhouse.TimechartValueKind
		nonfinite float64
		check     func(float64) bool
	}{
		{name: "sum", kind: clickhouse.TimechartValueKindSum, nonfinite: math.Inf(1), check: func(value float64) bool { return math.IsInf(value, 1) }},
		{name: "average", kind: clickhouse.TimechartValueKindAverage, nonfinite: math.NaN(), check: math.IsNaN},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			rows := splitValueTimechartRows(
				[]string{"0:api", "1:", "2:"},
				[][]float64{{12.5, 0, test.nonfinite}, {0, -2.25, 0}},
				[][]uint8{{1, 0, 1}, {0, 1, 0}},
			)
			query := splitValueTimechartQuery(first, 2, test.kind)
			sink := &fakeSink{}
			if err := mustExecutor(t, &fakeQueryConnection{rows: rows}).Execute(context.Background(), query, sink); err != nil {
				t.Fatalf("Execute: %v", err)
			}
			wantSchema := searchjobs.Schema{Columns: []searchjobs.Column{
				{Name: "_time", Kind: searchjobs.ValueKindTime},
				{Name: "api", Kind: searchjobs.ValueKindDouble, Nullable: true},
				{Name: "NULL", Kind: searchjobs.ValueKindDouble, Nullable: true},
				{Name: "OTHER", Kind: searchjobs.ValueKindDouble, Nullable: true},
			}}
			if sink.setCalls != 1 || !reflect.DeepEqual(sink.schema, wantSchema) || len(sink.rows) != 2 || !rows.closed {
				t.Fatalf("schema=%#v calls=%d rows=%d closed=%v", sink.schema, sink.setCalls, len(sink.rows), rows.closed)
			}
			if value, ok := sink.rows[0][1].Double(); !ok || value != 12.5 {
				t.Fatalf("api value = %v, %v", value, ok)
			}
			if !sink.rows[0][2].IsNull() {
				t.Fatalf("NULL gap = %#v, want null", sink.rows[0][2])
			}
			if value, ok := sink.rows[0][3].Double(); !ok || !test.check(value) {
				t.Fatalf("nonfinite OTHER value = %v, %v", value, ok)
			}
			if !sink.rows[1][1].IsNull() || !sink.rows[1][3].IsNull() {
				t.Fatalf("second-row gaps = %#v", sink.rows[1])
			}
			if value, ok := sink.rows[1][2].Double(); !ok || value != -2.25 {
				t.Fatalf("second-row NULL value = %v, %v", value, ok)
			}
		})
	}
}

func TestExecutorPublishesSplitPercentileAsNullableWideValues(t *testing.T) {
	t.Parallel()

	first := time.Date(2026, time.July, 22, 12, 0, 0, 0, time.UTC)
	rows := splitValueTimechartRows(
		[]string{"0:api", "1:", "2:"},
		[][]float64{{12.5, 0, 98.25}, {0, -2.25, 0}},
		[][]uint8{{1, 0, 1}, {0, 1, 0}},
	)
	sink := &fakeSink{}
	if err := mustExecutor(t, &fakeQueryConnection{rows: rows}).Execute(
		context.Background(),
		splitValueTimechartQuery(first, 2, clickhouse.TimechartValueKindPercentile),
		sink,
	); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	wantSchema := searchjobs.Schema{Columns: []searchjobs.Column{
		{Name: "_time", Kind: searchjobs.ValueKindTime},
		{Name: "api", Kind: searchjobs.ValueKindDouble, Nullable: true},
		{Name: "NULL", Kind: searchjobs.ValueKindDouble, Nullable: true},
		{Name: "OTHER", Kind: searchjobs.ValueKindDouble, Nullable: true},
	}}
	if sink.setCalls != 1 || !reflect.DeepEqual(sink.schema, wantSchema) ||
		len(sink.rows) != 2 || !rows.closed {
		t.Fatalf("schema=%#v calls=%d rows=%d closed=%v", sink.schema, sink.setCalls, len(sink.rows), rows.closed)
	}
	if value, ok := sink.rows[0][1].Double(); !ok || value != 12.5 {
		t.Fatalf("api percentile = %v, %v", value, ok)
	}
	if !sink.rows[0][2].IsNull() {
		t.Fatalf("NULL percentile gap = %#v, want null", sink.rows[0][2])
	}
	if value, ok := sink.rows[0][3].Double(); !ok || value != 98.25 {
		t.Fatalf("OTHER percentile = %v, %v", value, ok)
	}
	if !sink.rows[1][1].IsNull() || !sink.rows[1][3].IsNull() {
		t.Fatalf("second-row percentile gaps = %#v", sink.rows[1])
	}
}

func TestExecutorSuppressesEmptySplitNumericTimechartRows(t *testing.T) {
	t.Parallel()

	rows := splitValueTimechartRows(nil, [][]float64{{}, {}, {}}, [][]uint8{{}, {}, {}})
	sink := &fakeSink{}
	if err := mustExecutor(t, &fakeQueryConnection{rows: rows}).Execute(
		context.Background(),
		splitValueTimechartQuery(time.Unix(0, 0).UTC(), 3, clickhouse.TimechartValueKindSum),
		sink,
	); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if sink.setCalls != 1 || len(sink.schema.Columns) != 1 || sink.schema.Columns[0].Name != "_time" || len(sink.rows) != 0 {
		t.Fatalf("empty split numeric timechart schema=%#v rows=%d calls=%d", sink.schema, len(sink.rows), sink.setCalls)
	}
}

func TestExecutorReusesSplitNumericScanDestinations(t *testing.T) {
	t.Parallel()

	rows := splitValueTimechartRows(
		[]string{"0:a"},
		[][]float64{{1}, {2}},
		[][]uint8{{1}, {1}},
	)
	var first []uintptr
	scanCalls := 0
	rows.observeScan = func(destinations []any) {
		got := make([]uintptr, len(destinations))
		for index, destination := range destinations {
			got[index] = reflect.ValueOf(destination).Pointer()
		}
		if scanCalls == 0 {
			first = got
		} else if !reflect.DeepEqual(got, first) {
			t.Fatalf("scan destination pointers changed: got %v, want %v", got, first)
		}
		scanCalls++
	}
	sink := &fakeSink{}
	if err := mustExecutor(t, &fakeQueryConnection{rows: rows}).Execute(
		context.Background(),
		splitValueTimechartQuery(
			time.Unix(0, 0).UTC(),
			2,
			clickhouse.TimechartValueKindSum,
		),
		sink,
	); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if scanCalls != 2 || len(sink.rows) != 2 {
		t.Fatalf("scan calls=%d published rows=%d, want two", scanCalls, len(sink.rows))
	}
}

func TestExecutorExpandsSplitValueTimechartGroupBudget(t *testing.T) {
	t.Parallel()

	settings, err := querySettings(Config{})
	if err != nil {
		t.Fatal(err)
	}
	executor := &Executor{settings: settings, expandTimechartGroupLimit: true}
	query := splitValueTimechartQuery(
		time.Unix(0, 0).UTC(),
		1,
		clickhouse.TimechartValueKindSum,
	)
	query.Timechart.MaxSeries = 2
	if got, want := executor.settingsFor(query)["max_rows_to_group_by"], maximumRuntimeWideTimechartGroups; got != want {
		t.Fatalf("split numeric timechart group cap = %v, want %d", got, want)
	}
	if got := settings["max_rows_to_group_by"]; got != defaultMaxResultRows {
		t.Fatalf("base settings were mutated: cap=%v", got)
	}
	query.Timechart.ValueKind = clickhouse.TimechartValueKindPercentile
	if got, want := executor.settingsFor(query)["max_rows_to_group_by"], maximumRuntimeWidePercentileGroups; got != want {
		t.Fatalf("split percentile timechart group cap = %v, want %d", got, want)
	}

	highSettings, err := querySettings(Config{MaxRowsToGroupBy: 50_000})
	if err != nil {
		t.Fatal(err)
	}
	high := &Executor{settings: highSettings}
	if got, want := high.settingsFor(query)["max_rows_to_group_by"], maximumRuntimeWidePercentileGroups; got != want {
		t.Fatalf("explicit high split percentile group cap = %v, want clamped %d", got, want)
	}
	lowSettings, err := querySettings(Config{MaxRowsToGroupBy: 7})
	if err != nil {
		t.Fatal(err)
	}
	low := &Executor{settings: lowSettings}
	if got := low.settingsFor(query)["max_rows_to_group_by"]; got != uint64(7) {
		t.Fatalf("explicit low split percentile group cap = %v, want 7", got)
	}
}

func TestExecutorRejectsInvalidSplitNumericMetadataBeforeQuery(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name   string
		mutate func(*clickhouse.CompiledQuery)
	}{
		{name: "unset kind", mutate: func(query *clickhouse.CompiledQuery) {
			query.Timechart.ValueKind = clickhouse.TimechartValueKindInvalid
		}},
		{name: "unknown kind", mutate: func(query *clickhouse.CompiledQuery) {
			query.Timechart.ValueKind = clickhouse.TimechartValueKind(255)
		}},
		{name: "declared value field", mutate: func(query *clickhouse.CompiledQuery) {
			query.Timechart.ValueField = "hidden"
		}},
		{name: "static output field", mutate: func(query *clickhouse.CompiledQuery) {
			query.OutputFields = []string{"_time", "hidden"}
		}},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			query := splitValueTimechartQuery(
				time.Unix(0, 0).UTC(),
				1,
				clickhouse.TimechartValueKindSum,
			)
			test.mutate(&query)
			connection := &fakeQueryConnection{rows: splitValueTimechartRows(
				[]string{"0:a"},
				[][]float64{{1}},
				[][]uint8{{1}},
			)}
			sink := &fakeSink{}
			err := mustExecutor(t, connection).Execute(context.Background(), query, sink)
			if !errors.Is(err, searchjobs.ErrInvalidResult) {
				t.Fatalf("Execute error = %v, want ErrInvalidResult", err)
			}
			if connection.query != "" || sink.setCalls != 0 || len(sink.rows) != 0 {
				t.Fatalf("invalid metadata reached query/publication: query=%q schema=%d rows=%d", connection.query, sink.setCalls, len(sink.rows))
			}
		})
	}
}

func TestExecutorRejectsNonFiniteSplitPercentileAtomically(t *testing.T) {
	t.Parallel()

	for _, nonfinite := range []float64{math.NaN(), math.Inf(1), math.Inf(-1)} {
		rows := splitValueTimechartRows(
			[]string{"0:a"},
			[][]float64{{1}, {nonfinite}},
			[][]uint8{{1}, {1}},
		)
		sink := &fakeSink{}
		err := mustExecutor(t, &fakeQueryConnection{rows: rows}).Execute(
			context.Background(),
			splitValueTimechartQuery(
				time.Unix(0, 0).UTC(),
				2,
				clickhouse.TimechartValueKindPercentile,
			),
			sink,
		)
		if !errors.Is(err, searchjobs.ErrInvalidResult) {
			t.Fatalf("Execute(%v) error = %v, want ErrInvalidResult", nonfinite, err)
		}
		if sink.setCalls != 0 || len(sink.rows) != 0 || !rows.closed {
			t.Fatalf("nonfinite percentile published partial output: closed=%v schema=%d rows=%d", rows.closed, sink.setCalls, len(sink.rows))
		}
	}
}

func TestExecutorRejectsSplitNumericCloseFailureAtomically(t *testing.T) {
	t.Parallel()

	rows := splitValueTimechartRows(
		[]string{"0:a"},
		[][]float64{{1}},
		[][]uint8{{1}},
	)
	rows.closeErr = io.ErrUnexpectedEOF
	sink := &fakeSink{}
	err := mustExecutor(t, &fakeQueryConnection{rows: rows}).Execute(
		context.Background(),
		splitValueTimechartQuery(
			time.Unix(0, 0).UTC(),
			1,
			clickhouse.TimechartValueKindSum,
		),
		sink,
	)
	if !errors.Is(err, searchjobs.ErrStorageUnavailable) {
		t.Fatalf("Execute error = %v, want ErrStorageUnavailable", err)
	}
	if !rows.closed || sink.setCalls != 0 || len(sink.rows) != 0 {
		t.Fatalf("close failure closed=%v published schema=%d rows=%d", rows.closed, sink.setCalls, len(sink.rows))
	}
}

func TestExecutorRejectsMalformedSplitNumericTransportAtomically(t *testing.T) {
	t.Parallel()

	for _, malformed := range []struct {
		name   string
		mutate func(*fakeRows)
	}{
		{name: "wrong value type", mutate: func(rows *fakeRows) {
			rows.types[2] = fakeColumnType{name: clickhouse.TimechartValuesColumn, databaseType: "Array(UInt64)", scanType: reflect.TypeOf([]uint64{})}
		}},
		{name: "wrong presence type", mutate: func(rows *fakeRows) {
			rows.types[3] = fakeColumnType{name: clickhouse.TimechartValuePresentColumn, databaseType: "Array(UInt64)", scanType: reflect.TypeOf([]uint64{})}
		}},
		{name: "nullable value array", mutate: func(rows *fakeRows) {
			rows.types[2] = fakeColumnType{name: clickhouse.TimechartValuesColumn, databaseType: "Array(Float64)", scanType: reflect.TypeOf([]float64{}), nullable: true}
		}},
		{name: "wrong value column", mutate: func(rows *fakeRows) { rows.columns[2] = "wrong" }},
		{name: "value width", mutate: func(rows *fakeRows) { rows.data[0][2] = []float64{1} }},
		{name: "presence width", mutate: func(rows *fakeRows) { rows.data[0][3] = []uint8{1} }},
		{name: "invalid presence", mutate: func(rows *fakeRows) { rows.data[0][3] = []uint8{2, 1} }},
		{name: "absent nonzero value", mutate: func(rows *fakeRows) {
			rows.data[0][2] = []float64{1, 9}
			rows.data[0][3] = []uint8{1, 0}
		}},
		{name: "domain changes", mutate: func(rows *fakeRows) { rows.data[1][1] = []string{"0:b", "1:"} }},
		{name: "domain out of order", mutate: func(rows *fakeRows) {
			for _, row := range rows.data {
				row[1] = []string{"0:b", "0:a"}
			}
		}},
		{name: "ordinal gap", mutate: func(rows *fakeRows) { rows.data[1][0] = uint64(2) }},
		{name: "incomplete grid", mutate: func(rows *fakeRows) { rows.data = rows.data[:1] }},
		{name: "invalid split marker", mutate: func(rows *fakeRows) { rows.data[0][4] = uint8(1) }},
	} {
		malformed := malformed
		t.Run(malformed.name, func(t *testing.T) {
			t.Parallel()

			rows := splitValueTimechartRows(
				[]string{"0:a", "1:"},
				[][]float64{{1, 0}, {2, 0}},
				[][]uint8{{1, 0}, {1, 0}},
			)
			malformed.mutate(rows)
			connection := &fakeQueryConnection{rows: rows}
			sink := &fakeSink{}
			err := mustExecutor(t, connection).Execute(
				context.Background(),
				splitValueTimechartQuery(time.Unix(0, 0).UTC(), 2, clickhouse.TimechartValueKindAverage),
				sink,
			)
			if malformed.name == "invalid split marker" {
				if !errors.Is(err, searchjobs.ErrUnsupportedValue) {
					t.Fatalf("Execute error = %v, want ErrUnsupportedValue", err)
				}
			} else if !errors.Is(err, searchjobs.ErrInvalidResult) {
				t.Fatalf("Execute error = %v, want ErrInvalidResult", err)
			}
			if sink.setCalls != 0 || len(sink.rows) != 0 || !rows.closed || connection.query == "" {
				t.Fatalf(
					"malformed transport query=%q closed=%v published schema=%d rows=%d",
					connection.query,
					rows.closed,
					sink.setCalls,
					len(sink.rows),
				)
			}
		})
	}
}

func splitValueTimechartQuery(first time.Time, bucketCount uint64, kind clickhouse.TimechartValueKind) clickhouse.CompiledQuery {
	return clickhouse.CompiledQuery{
		SQL:          "SELECT bounded_split_value_timechart",
		OutputFields: []string{"_time"},
		Timechart: &clickhouse.TimechartOutput{
			Mode:          clickhouse.TimechartModeRuntimeWideValue,
			FirstBucket:   first,
			Span:          time.Minute,
			BucketCount:   bucketCount,
			MaxSeries:     12,
			MaxLabelBytes: 256,
			ValueKind:     kind,
		},
	}
}

func splitValueTimechartRows(names []string, values [][]float64, present [][]uint8) *fakeRows {
	rows := &fakeRows{
		columns: []string{
			clickhouse.TimechartOrdinalColumn,
			clickhouse.TimechartNamesColumn,
			clickhouse.TimechartValuesColumn,
			clickhouse.TimechartValuePresentColumn,
			clickhouse.TimechartInvalidColumn,
		},
		types: []driver.ColumnType{
			fakeColumnType{name: clickhouse.TimechartOrdinalColumn, databaseType: "UInt64", scanType: reflect.TypeOf(uint64(0))},
			fakeColumnType{name: clickhouse.TimechartNamesColumn, databaseType: "Array(String)", scanType: reflect.TypeOf([]string{})},
			fakeColumnType{name: clickhouse.TimechartValuesColumn, databaseType: "Array(Float64)", scanType: reflect.TypeOf([]float64{})},
			fakeColumnType{name: clickhouse.TimechartValuePresentColumn, databaseType: "Array(UInt8)", scanType: reflect.TypeOf([]uint8{})},
			fakeColumnType{name: clickhouse.TimechartInvalidColumn, databaseType: "UInt8", scanType: reflect.TypeOf(uint8(0))},
		},
	}
	for ordinal := range values {
		var rowNames []string
		if ordinal == 0 {
			rowNames = append(rowNames, names...)
		}
		rows.data = append(rows.data, []any{uint64(ordinal), rowNames, values[ordinal], present[ordinal], uint8(0)})
	}
	return rows
}
