package queryexec

import (
	"context"
	"errors"
	"io"
	"math"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
)

func TestExecutorPublishesNumericChartAsNullableWideValues(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name      string
		kind      clickhouse.ChartValueKind
		nonfinite float64
		check     func(float64) bool
	}{
		{
			name: "sum", kind: clickhouse.ChartValueKindSum, nonfinite: math.Inf(1),
			check: func(value float64) bool { return math.IsInf(value, 1) },
		},
		{
			name: "average", kind: clickhouse.ChartValueKindAverage, nonfinite: math.NaN(),
			check: math.IsNaN,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			rows := numericChartRows(
				reflect.TypeFor[string](),
				[]string{"0:api", "1:", "2:"},
				[]any{"/a", "/b"},
				[][]float64{{0, 0, test.nonfinite}, {0, -2.25, 0}},
				[][]uint8{{1, 0, 1}, {0, 1, 0}},
			)
			sink := &fakeSink{}
			query := numericChartQuery(t, test.kind)
			if err := mustExecutor(t, &fakeQueryConnection{rows: rows}).Execute(
				context.Background(), query, sink,
			); err != nil {
				t.Fatalf("Execute: %v", err)
			}

			wantSchema := searchjobs.Schema{Columns: []searchjobs.Column{
				{Name: "path", Kind: searchjobs.ValueKindString},
				{Name: "api", Kind: searchjobs.ValueKindDouble, Nullable: true},
				{Name: "NULL", Kind: searchjobs.ValueKindDouble, Nullable: true},
				{Name: "OTHER", Kind: searchjobs.ValueKindDouble, Nullable: true},
			}}
			if sink.setCalls != 1 || !reflect.DeepEqual(sink.schema, wantSchema) ||
				len(sink.rows) != 2 || !rows.closed {
				t.Fatalf(
					"schema=%#v calls=%d rows=%d closed=%v",
					sink.schema,
					sink.setCalls,
					len(sink.rows),
					rows.closed,
				)
			}
			if row, ok := sink.rows[0][0].String(); !ok || row != "/a" {
				t.Fatalf("first row axis = %q, %v", row, ok)
			}
			if value, ok := sink.rows[0][1].Double(); !ok || value != 0 {
				t.Fatalf("present real zero = %v, %v, want Double(0)", value, ok)
			}
			if !sink.rows[0][2].IsNull() {
				t.Fatalf("absent NULL cell = %#v, want null", sink.rows[0][2])
			}
			if value, ok := sink.rows[0][3].Double(); !ok || !test.check(value) {
				t.Fatalf("nonfinite OTHER value = %v, %v", value, ok)
			}
			if !sink.rows[1][1].IsNull() || !sink.rows[1][3].IsNull() {
				t.Fatalf("second-row absent cells = %#v, want nulls", sink.rows[1])
			}
			if value, ok := sink.rows[1][2].Double(); !ok || value != -2.25 {
				t.Fatalf("second-row NULL value = %v, %v", value, ok)
			}
		})
	}
}

// A numeric measure's eligibility cannot decide whether an otherwise valid
// row-axis value exists. ClickHouse therefore emits the row even when every
// immediate member was ineligible, and the executor publishes a null cell.
func TestExecutorRetainsNumericChartRowsWithNoEligibleMeasureMembers(t *testing.T) {
	t.Parallel()

	rows := numericChartRows(
		reflect.TypeFor[string](),
		[]string{"0:api"},
		[]any{"/measureless"},
		[][]float64{{0}},
		[][]uint8{{0}},
	)
	sink := &fakeSink{}
	if err := mustExecutor(t, &fakeQueryConnection{rows: rows}).Execute(
		context.Background(),
		numericChartQuery(t, clickhouse.ChartValueKindAverage),
		sink,
	); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if sink.setCalls != 1 || len(sink.rows) != 1 || len(sink.schema.Columns) != 2 {
		t.Fatalf("measureless numeric chart schema=%#v rows=%#v", sink.schema, sink.rows)
	}
	if row, ok := sink.rows[0][0].String(); !ok || row != "/measureless" {
		t.Fatalf("measureless row axis = %q, %v", row, ok)
	}
	if !sink.rows[0][1].IsNull() {
		t.Fatalf("measureless numeric cell = %#v, want null", sink.rows[0][1])
	}
}

func TestExecutorNumericChartByteCeilingIsExactAtBothSides(t *testing.T) {
	names := []string{"0:api"}
	public := []string{"api"}
	oneSeriesInitialBytes := chartBufferedBaseBytes +
		chartDomainRetainedBytes(names, public) +
		chartValueRowOverheadBytes + chartValueCellBytes
	exact := int(maximumChartResultBytes - oneSeriesInitialBytes)
	base := strings.Repeat("n", exact+1)

	for _, test := range []struct {
		name    string
		row     string
		wantErr error
	}{
		{name: "exactly at the ceiling", row: base[:exact]},
		{name: "one byte over the ceiling", row: base, wantErr: searchjobs.ErrExecutionLimit},
	} {
		t.Run(test.name, func(t *testing.T) {
			rows := numericChartRows(
				reflect.TypeFor[string](),
				names,
				[]any{test.row},
				[][]float64{{1}},
				[][]uint8{{1}},
			)
			sink := &fakeSink{}
			err := mustExecutor(t, &fakeQueryConnection{rows: rows}).Execute(
				context.Background(),
				numericChartQuery(t, clickhouse.ChartValueKindSum),
				sink,
			)
			if test.wantErr != nil {
				if !errors.Is(err, test.wantErr) {
					t.Fatalf("Execute = %v, want %v", err, test.wantErr)
				}
				if sink.setCalls != 0 || len(sink.rows) != 0 {
					t.Fatalf("over-ceiling numeric chart published schema=%d rows=%d", sink.setCalls, len(sink.rows))
				}
				return
			}
			if err != nil {
				t.Fatalf("Execute: %v", err)
			}
			if sink.setCalls != 1 || len(sink.rows) != 1 {
				t.Fatalf("boundary numeric chart schema=%d rows=%d", sink.setCalls, len(sink.rows))
			}
			row, ok := sink.rows[0][0].String()
			if !ok || len(row) != exact {
				t.Fatalf("boundary row length=%d (%v), want %d", len(row), ok, exact)
			}
		})
	}
}

func TestExecutorReusesNumericChartScanDestinations(t *testing.T) {
	t.Parallel()

	rows := numericChartRows(
		reflect.TypeFor[string](),
		[]string{"0:api"},
		[]any{"/a", "/b"},
		[][]float64{{1}, {2}},
		[][]uint8{{1}, {1}},
	)
	var first []uintptr
	scanCalls := 0
	rows.observeScan = func(destinations []any) {
		got := make([]uintptr, len(destinations))
		for index, destination := range destinations {
			if scanCalls > 0 && !reflect.ValueOf(destination).Elem().IsZero() {
				t.Fatalf("numeric chart scan destination %d retained the prior row", index)
			}
			got[index] = reflect.ValueOf(destination).Pointer()
		}
		if scanCalls == 0 {
			first = got
		} else if !slices.Equal(got, first) {
			t.Fatalf("numeric chart scan destinations changed: got %v, want %v", got, first)
		}
		scanCalls++
	}
	sink := &fakeSink{}
	if err := mustExecutor(t, &fakeQueryConnection{rows: rows}).Execute(
		context.Background(),
		numericChartQuery(t, clickhouse.ChartValueKindSum),
		sink,
	); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if scanCalls != 2 || len(sink.rows) != 2 {
		t.Fatalf("scan calls=%d published rows=%d, want two", scanCalls, len(sink.rows))
	}
}

func TestExecutorBuffersNumericChartBeforePublishing(t *testing.T) {
	t.Parallel()

	rows := numericChartRows(
		reflect.TypeFor[string](),
		[]string{"0:api"},
		[]any{"/a", "/b"},
		[][]float64{{1}, {2}},
		[][]uint8{{1}, {1}},
	)
	rows.err = io.ErrUnexpectedEOF
	sink := &fakeSink{}
	err := mustExecutor(t, &fakeQueryConnection{rows: rows}).Execute(
		context.Background(),
		numericChartQuery(t, clickhouse.ChartValueKindSum),
		sink,
	)
	if !errors.Is(err, searchjobs.ErrStorageUnavailable) {
		t.Fatalf("Execute error = %v, want ErrStorageUnavailable", err)
	}
	if sink.setCalls != 0 || len(sink.rows) != 0 || !rows.closed {
		t.Fatalf(
			"failed buffered numeric chart closed=%v published schema=%d rows=%d",
			rows.closed,
			sink.setCalls,
			len(sink.rows),
		)
	}
}

func TestExecutorKeepsNumericChartRawGroupBudgetIndependentOfOutputWidth(t *testing.T) {
	t.Parallel()

	settings, err := querySettings(Config{})
	if err != nil {
		t.Fatal(err)
	}
	executor := &Executor{settings: mustValidatedSettings(t, settings), expandTimechartGroupLimit: true}
	query := numericChartQuery(
		t,
		clickhouse.ChartValueKindSum,
	)
	if query.Chart.RowLimit != maximumChartRows || query.Chart.MaxSeries != maximumChartSeries {
		t.Fatalf("numeric chart bounds = %#v", query.Chart)
	}
	if got, want := executor.settingsFor(query)["max_rows_to_group_by"], maximumRuntimeWideTimechartGroups; got != want {
		t.Fatalf("numeric chart raw group cap = %v, want %d", got, want)
	}
	if got := settings["max_rows_to_group_by"]; got != defaultMaxResultRows {
		t.Fatalf("numeric chart group expansion mutated base cap: %v", got)
	}

	lowSettings, err := querySettings(Config{MaxRowsToGroupBy: 7})
	if err != nil {
		t.Fatal(err)
	}
	low := &Executor{settings: mustValidatedSettings(t, lowSettings)}
	if got := low.settingsFor(query)["max_rows_to_group_by"]; got != uint64(7) {
		t.Fatalf("explicit lower numeric chart group cap = %v, want 7", got)
	}
}

func TestExecutorRejectsNumericChartCloseFailureAtomically(t *testing.T) {
	t.Parallel()

	rows := numericChartRows(
		reflect.TypeFor[string](),
		[]string{"0:api"},
		[]any{"/a"},
		[][]float64{{1}},
		[][]uint8{{1}},
	)
	rows.closeErr = io.ErrUnexpectedEOF
	sink := &fakeSink{}
	err := mustExecutor(t, &fakeQueryConnection{rows: rows}).Execute(
		context.Background(),
		numericChartQuery(t, clickhouse.ChartValueKindAverage),
		sink,
	)
	if !errors.Is(err, searchjobs.ErrStorageUnavailable) {
		t.Fatalf("Execute error = %v, want ErrStorageUnavailable", err)
	}
	if sink.setCalls != 0 || len(sink.rows) != 0 || !rows.closed {
		t.Fatalf(
			"numeric chart close failure closed=%v published schema=%d rows=%d",
			rows.closed,
			sink.setCalls,
			len(sink.rows),
		)
	}
}

func TestExecutorRejectsInvalidNumericChartValueKindBeforeQuery(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name string
		kind clickhouse.ChartValueKind
	}{
		{name: "unset", kind: clickhouse.ChartValueKindInvalid},
		{name: "unknown", kind: clickhouse.ChartValueKind(255)},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			rows := numericChartRows(
				reflect.TypeFor[string](),
				[]string{"0:api"},
				[]any{"/a"},
				[][]float64{{1}},
				[][]uint8{{1}},
			)
			query := numericChartQuery(t, test.kind)
			connection := &fakeQueryConnection{rows: rows}
			sink := &fakeSink{}
			err := mustExecutor(t, connection).Execute(context.Background(), query, sink)
			if !errors.Is(err, searchjobs.ErrInvalidResult) {
				t.Fatalf("Execute with chart value kind %d = %v, want ErrInvalidResult", test.kind, err)
			}
			if connection.query != "" || sink.setCalls != 0 || len(sink.rows) != 0 {
				t.Fatalf(
					"invalid numeric chart metadata reached query/publication: query=%q schema=%d rows=%d",
					connection.query,
					sink.setCalls,
					len(sink.rows),
				)
			}
		})
	}
}

func TestExecutorRejectsMalformedNumericChartTransportAtomically(t *testing.T) {
	t.Parallel()

	for _, malformed := range []struct {
		name   string
		mutate func(*fakeRows)
		want   error
	}{
		{name: "wrong values column", mutate: func(rows *fakeRows) { rows.columns[3] = "wrong" }, want: searchjobs.ErrInvalidResult},
		{name: "wrong presence column", mutate: func(rows *fakeRows) { rows.columns[4] = "wrong" }, want: searchjobs.ErrInvalidResult},
		{name: "wrong row scan type", mutate: func(rows *fakeRows) {
			rows.types[1] = fakeColumnType{name: clickhouse.ChartRowColumn, databaseType: "String", scanType: reflect.TypeFor[[]byte]()}
		}, want: searchjobs.ErrInvalidResult},
		{name: "wrong value type", mutate: func(rows *fakeRows) {
			rows.types[3] = fakeColumnType{name: clickhouse.ChartValuesColumn, databaseType: "Array(UInt64)", scanType: reflect.TypeFor[[]uint64]()}
		}, want: searchjobs.ErrInvalidResult},
		{name: "wrong presence type", mutate: func(rows *fakeRows) {
			rows.types[4] = fakeColumnType{name: clickhouse.ChartValuePresentColumn, databaseType: "Array(UInt64)", scanType: reflect.TypeFor[[]uint64]()}
		}, want: searchjobs.ErrInvalidResult},
		{name: "nullable value array", mutate: func(rows *fakeRows) {
			rows.types[3] = fakeColumnType{name: clickhouse.ChartValuesColumn, databaseType: "Array(Float64)", scanType: reflect.TypeFor[[]float64](), nullable: true}
		}, want: searchjobs.ErrInvalidResult},
		{name: "nullable presence array", mutate: func(rows *fakeRows) {
			rows.types[4] = fakeColumnType{name: clickhouse.ChartValuePresentColumn, databaseType: "Array(UInt8)", scanType: reflect.TypeFor[[]uint8](), nullable: true}
		}, want: searchjobs.ErrInvalidResult},
		{name: "value width", mutate: func(rows *fakeRows) { rows.data[0][3] = []float64{1} }, want: searchjobs.ErrInvalidResult},
		{name: "presence width", mutate: func(rows *fakeRows) { rows.data[0][4] = []uint8{1} }, want: searchjobs.ErrInvalidResult},
		{name: "invalid presence", mutate: func(rows *fakeRows) { rows.data[0][4] = []uint8{2, 1} }, want: searchjobs.ErrInvalidResult},
		{name: "absent nonzero value", mutate: func(rows *fakeRows) {
			rows.data[0][3] = []float64{1, 9}
			rows.data[0][4] = []uint8{1, 0}
		}, want: searchjobs.ErrInvalidResult},
		{name: "domain changes", mutate: func(rows *fakeRows) {
			rows.data[1][2] = []string{"0:other", "1:"}
		}, want: searchjobs.ErrInvalidResult},
		{name: "nonempty result with empty series domain", mutate: func(rows *fakeRows) {
			for _, row := range rows.data {
				row[2] = []string{}
				row[3] = []float64{}
				row[4] = []uint8{}
			}
		}, want: searchjobs.ErrInvalidResult},
		{name: "domain out of order", mutate: func(rows *fakeRows) {
			for _, row := range rows.data {
				row[2] = []string{"0:b", "0:a"}
			}
		}, want: searchjobs.ErrInvalidResult},
		{name: "ordinal gap", mutate: func(rows *fakeRows) { rows.data[1][0] = uint64(2) }, want: searchjobs.ErrInvalidResult},
		{name: "invalid split marker", mutate: func(rows *fakeRows) { rows.data[0][5] = uint8(1) }, want: searchjobs.ErrUnsupportedValue},
	} {
		t.Run(malformed.name, func(t *testing.T) {
			t.Parallel()

			rows := numericChartRows(
				reflect.TypeFor[string](),
				[]string{"0:api", "1:"},
				[]any{"/a", "/b"},
				[][]float64{{1, 0}, {2, 0}},
				[][]uint8{{1, 0}, {1, 0}},
			)
			malformed.mutate(rows)
			connection := &fakeQueryConnection{rows: rows}
			sink := &fakeSink{}
			err := mustExecutor(t, connection).Execute(
				context.Background(),
				numericChartQuery(t, clickhouse.ChartValueKindAverage),
				sink,
			)
			if !errors.Is(err, malformed.want) {
				t.Fatalf("Execute error = %v, want %v", err, malformed.want)
			}
			if sink.setCalls != 0 || len(sink.rows) != 0 || !rows.closed || connection.query == "" {
				t.Fatalf(
					"malformed numeric chart query=%q closed=%v published schema=%d rows=%d",
					connection.query,
					rows.closed,
					sink.setCalls,
					len(sink.rows),
				)
			}
		})
	}
}

func numericChartQuery(
	t *testing.T,
	valueKind clickhouse.ChartValueKind,
) clickhouse.CompiledQuery {
	t.Helper()
	query := chartQuery("path", clickhouse.ChartRowKindString, "String")
	query.Chart.ValueKind = valueKind
	return query
}

func numericChartRows(
	rowScanType reflect.Type,
	names []string,
	rowValues []any,
	values [][]float64,
	present [][]uint8,
) *fakeRows {
	if len(rowValues) != len(values) || len(values) != len(present) {
		panic("numeric chart fixture row dimensions differ")
	}
	rows := &fakeRows{
		columns: []string{
			clickhouse.ChartOrdinalColumn,
			clickhouse.ChartRowColumn,
			clickhouse.ChartNamesColumn,
			clickhouse.ChartValuesColumn,
			clickhouse.ChartValuePresentColumn,
			clickhouse.ChartInvalidColumn,
		},
		types: []driver.ColumnType{
			fakeColumnType{name: clickhouse.ChartOrdinalColumn, databaseType: "UInt64", scanType: reflect.TypeFor[uint64]()},
			fakeColumnType{name: clickhouse.ChartRowColumn, databaseType: "String", scanType: rowScanType},
			fakeColumnType{name: clickhouse.ChartNamesColumn, databaseType: "Array(String)", scanType: reflect.TypeFor[[]string]()},
			fakeColumnType{name: clickhouse.ChartValuesColumn, databaseType: "Array(Float64)", scanType: reflect.TypeFor[[]float64]()},
			fakeColumnType{name: clickhouse.ChartValuePresentColumn, databaseType: "Array(UInt8)", scanType: reflect.TypeFor[[]uint8]()},
			fakeColumnType{name: clickhouse.ChartInvalidColumn, databaseType: "UInt8", scanType: reflect.TypeFor[uint8]()},
		},
		data: make([][]any, len(values)),
	}
	for index := range values {
		rows.data[index] = []any{
			uint64(index),
			rowValues[index],
			slices.Clone(names),
			slices.Clone(values[index]),
			slices.Clone(present[index]),
			uint8(0),
		}
	}
	return rows
}
