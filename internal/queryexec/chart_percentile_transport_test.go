package queryexec

import (
	"context"
	"errors"
	"math"
	"reflect"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
)

func TestExecutorPublishesChartPercentileAsNullableFiniteValues(t *testing.T) {
	t.Parallel()

	rows := numericChartRows(
		"String",
		reflect.TypeOf(""),
		[]string{"0:api", "1:", "2:"},
		[]any{"/a", "/b"},
		[][]float64{{0, 0, 97.5}, {12.25, 0, 0}},
		[][]uint8{{1, 0, 1}, {1, 0, 0}},
	)
	sink := &fakeSink{}
	query := numericChartQuery(
		t,
		"path",
		clickhouse.ChartRowKindString,
		"String",
		clickhouse.ChartValueKindPercentile,
	)
	if err := mustExecutor(t, &fakeQueryConnection{rows: rows}).Execute(
		context.Background(),
		query,
		sink,
	); err != nil {
		t.Fatalf("Execute percentile chart: %v", err)
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
			"percentile chart schema=%#v calls=%d rows=%d closed=%v",
			sink.schema,
			sink.setCalls,
			len(sink.rows),
			rows.closed,
		)
	}
	if value, ok := sink.rows[0][1].Double(); !ok || value != 0 {
		t.Fatalf("present percentile zero = %v, %v, want Double(0)", value, ok)
	}
	if !sink.rows[0][2].IsNull() {
		t.Fatalf("absent percentile NULL cell = %#v, want null", sink.rows[0][2])
	}
	if value, ok := sink.rows[0][3].Double(); !ok || value != 97.5 {
		t.Fatalf("percentile OTHER cell = %v, %v, want 97.5", value, ok)
	}
	if value, ok := sink.rows[1][1].Double(); !ok || value != 12.25 {
		t.Fatalf("second percentile cell = %v, %v, want 12.25", value, ok)
	}
	if !sink.rows[1][2].IsNull() || !sink.rows[1][3].IsNull() {
		t.Fatalf("second-row absent percentile cells = %#v, want nulls", sink.rows[1])
	}
}

func TestExecutorRejectsNonFiniteChartPercentileAtomically(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name  string
		value float64
	}{
		{name: "nan", value: math.NaN()},
		{name: "positive infinity", value: math.Inf(1)},
		{name: "negative infinity", value: math.Inf(-1)},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			rows := numericChartRows(
				"String",
				reflect.TypeOf(""),
				[]string{"0:api"},
				[]any{"/finite", "/invalid"},
				[][]float64{{25}, {test.value}},
				[][]uint8{{1}, {1}},
			)
			connection := &fakeQueryConnection{rows: rows}
			sink := &fakeSink{}
			err := mustExecutor(t, connection).Execute(
				context.Background(),
				numericChartQuery(
					t,
					"path",
					clickhouse.ChartRowKindString,
					"String",
					clickhouse.ChartValueKindPercentile,
				),
				sink,
			)
			if !errors.Is(err, searchjobs.ErrInvalidResult) {
				t.Fatalf("Execute percentile %s = %v, want ErrInvalidResult", test.name, err)
			}
			if connection.query == "" || !rows.closed || sink.setCalls != 0 || len(sink.rows) != 0 {
				t.Fatalf(
					"nonfinite percentile query=%q closed=%v published schema=%d rows=%d",
					connection.query,
					rows.closed,
					sink.setCalls,
					len(sink.rows),
				)
			}
		})
	}
}

func TestExecutorCapsChartPercentileRawSketchGroupsAtTwentyThousand(t *testing.T) {
	t.Parallel()

	query := numericChartQuery(
		t,
		"path",
		clickhouse.ChartRowKindString,
		"String",
		clickhouse.ChartValueKindPercentile,
	)
	if query.Chart.RowLimit != maximumChartRows ||
		query.Chart.MaxSeries != maximumChartSeries {
		t.Fatalf("percentile chart bounds = %#v", query.Chart)
	}

	settings, err := querySettings(Config{})
	if err != nil {
		t.Fatal(err)
	}
	executor := &Executor{settings: settings, expandTimechartGroupLimit: true}
	if got := executor.settingsFor(query)["max_rows_to_group_by"]; got != uint64(20_000) {
		t.Fatalf("percentile chart raw sketch cap = %v, want 20000", got)
	}
	if got := settings["max_rows_to_group_by"]; got != defaultMaxResultRows {
		t.Fatalf("percentile chart mutated base group cap: %v", got)
	}

	highSettings, err := querySettings(Config{MaxRowsToGroupBy: 50_000})
	if err != nil {
		t.Fatal(err)
	}
	high := &Executor{settings: highSettings}
	if got := high.settingsFor(query)["max_rows_to_group_by"]; got != uint64(20_000) {
		t.Fatalf("explicit high percentile chart group cap = %v, want clamped 20000", got)
	}

	lowSettings, err := querySettings(Config{MaxRowsToGroupBy: 7})
	if err != nil {
		t.Fatal(err)
	}
	low := &Executor{settings: lowSettings}
	if got := low.settingsFor(query)["max_rows_to_group_by"]; got != uint64(7) {
		t.Fatalf("explicit low percentile chart group cap = %v, want 7", got)
	}
	expandedLow := &Executor{
		settings:                  lowSettings,
		expandTimechartGroupLimit: true,
	}
	if got := expandedLow.settingsFor(query)["max_rows_to_group_by"]; got != uint64(20_000) {
		t.Fatalf("expanded low percentile chart group cap = %v, want 20000", got)
	}
	if got := lowSettings["max_rows_to_group_by"]; got != uint64(7) {
		t.Fatalf("expanded percentile chart mutated explicit low group cap: %v", got)
	}
}
