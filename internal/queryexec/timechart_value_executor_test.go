package queryexec

import (
	"context"
	"errors"
	"math"
	"reflect"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
)

func TestExecutorPublishesFixedSumAndAverageValues(t *testing.T) {
	t.Parallel()

	first := time.Date(2026, time.July, 22, 12, 0, 0, 0, time.UTC)
	tests := []struct {
		name        string
		kind        clickhouse.TimechartValueKind
		field       string
		finite      float64
		nonfinite   float64
		isNonfinite func(float64) bool
	}{
		{
			name: "sum", kind: clickhouse.TimechartValueKindSum,
			field: "total_bytes", finite: 12.5, nonfinite: math.Inf(1),
			isNonfinite: func(value float64) bool { return math.IsInf(value, 1) },
		},
		{
			name: "average", kind: clickhouse.TimechartValueKindAverage,
			field: "mean_latency", finite: -2.25, nonfinite: math.NaN(),
			isNonfinite: math.IsNaN,
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			rows := fixedValueTimechartRows(
				[]any{test.finite, test.nonfinite, nil},
				[]uint8{1, 1, 1},
			)
			query := fixedValueTimechartQuery(first, 3, test.field, test.kind)
			sink := &fakeSink{}
			if err := mustExecutor(t, &fakeQueryConnection{rows: rows}).Execute(
				context.Background(),
				query,
				sink,
			); err != nil {
				t.Fatalf("Execute: %v", err)
			}

			wantSchema := searchjobs.Schema{Columns: []searchjobs.Column{
				{Name: "_time", Kind: searchjobs.ValueKindTime},
				{Name: test.field, Kind: searchjobs.ValueKindDouble, Nullable: true},
			}}
			if sink.setCalls != 1 || !reflect.DeepEqual(sink.schema, wantSchema) ||
				!rows.closed || len(sink.rows) != 3 {
				t.Fatalf(
					"schema=%#v calls=%d rows closed=%v rows=%d",
					sink.schema,
					sink.setCalls,
					rows.closed,
					len(sink.rows),
				)
			}
			if value, ok := sink.rows[0][1].Double(); !ok || value != test.finite {
				t.Fatalf("finite value = %v, %v", value, ok)
			}
			if value, ok := sink.rows[1][1].Double(); !ok || !test.isNonfinite(value) {
				t.Fatalf("nonfinite value = %v, %v", value, ok)
			}
			if !sink.rows[2][1].IsNull() {
				t.Fatalf("missing value = %#v, want null", sink.rows[2][1])
			}
		})
	}
}

func TestExecutorDistinguishesEmptyAndAllIneligibleFixedSumAndAverageInput(t *testing.T) {
	t.Parallel()

	first := time.Date(2026, time.July, 22, 12, 0, 0, 0, time.UTC)
	for _, aggregate := range []struct {
		name string
		kind clickhouse.TimechartValueKind
	}{
		{name: "sum", kind: clickhouse.TimechartValueKindSum},
		{name: "average", kind: clickhouse.TimechartValueKindAverage},
	} {
		aggregate := aggregate
		for _, input := range []struct {
			name     string
			presence uint8
			wantRows int
		}{
			{name: "empty upstream", presence: 0, wantRows: 0},
			{name: "all ineligible", presence: 1, wantRows: 3},
		} {
			input := input
			t.Run(aggregate.name+"/"+input.name, func(t *testing.T) {
				t.Parallel()

				rows := fixedValueTimechartRows(
					[]any{nil, nil, nil},
					[]uint8{input.presence, input.presence, input.presence},
				)
				query := fixedValueTimechartQuery(first, 3, aggregate.name, aggregate.kind)
				sink := &fakeSink{}
				if err := mustExecutor(t, &fakeQueryConnection{rows: rows}).Execute(
					context.Background(),
					query,
					sink,
				); err != nil {
					t.Fatalf("Execute: %v", err)
				}
				if sink.setCalls != 1 || len(sink.rows) != input.wantRows {
					t.Fatalf("schema calls=%d rows=%d", sink.setCalls, len(sink.rows))
				}
				for index, row := range sink.rows {
					if !row[1].IsNull() {
						t.Fatalf("row %d value = %#v, want null", index, row[1])
					}
				}
			})
		}
	}
}

func TestExecutorRejectsInvalidFixedValueKindBeforeQuery(t *testing.T) {
	t.Parallel()

	first := time.Date(2026, time.July, 22, 12, 0, 0, 0, time.UTC)
	rows := fixedValueTimechartRows([]any{float64(10)}, []uint8{1})
	query := fixedValueTimechartQuery(
		first,
		1,
		"value",
		clickhouse.TimechartValueKind(255),
	)
	connection := &fakeQueryConnection{rows: rows}
	sink := &fakeSink{}
	err := mustExecutor(t, connection).Execute(context.Background(), query, sink)
	if !errors.Is(err, searchjobs.ErrInvalidResult) {
		t.Fatalf("Execute error = %v, want ErrInvalidResult", err)
	}
	if connection.query != "" || sink.setCalls != 0 || len(sink.rows) != 0 {
		t.Fatalf(
			"invalid kind issued query=%q or published schema=%d rows=%d",
			connection.query,
			sink.setCalls,
			len(sink.rows),
		)
	}
}

func TestExecutorRejectsMalformedFixedSumAndAverageTransportAtomically(t *testing.T) {
	t.Parallel()

	first := time.Date(2026, time.July, 22, 12, 0, 0, 0, time.UTC)
	for _, aggregate := range []struct {
		name string
		kind clickhouse.TimechartValueKind
	}{
		{name: "sum", kind: clickhouse.TimechartValueKindSum},
		{name: "average", kind: clickhouse.TimechartValueKindAverage},
	} {
		aggregate := aggregate
		for _, malformed := range []struct {
			name   string
			mutate func(*fakeRows)
		}{
			{
				name: "wrong value column",
				mutate: func(rows *fakeRows) {
					rows.columns[1] = "attacker_value"
				},
			},
			{
				name: "wrong value type",
				mutate: func(rows *fakeRows) {
					rows.types[1] = fakeColumnType{
						name: clickhouse.TimechartValueColumn, databaseType: "Float64",
						scanType: reflect.TypeOf(float64(0)),
					}
				},
			},
			{
				name: "ordinal gap",
				mutate: func(rows *fakeRows) {
					rows.data[1][0] = uint64(2)
				},
			},
			{
				name: "presence changes",
				mutate: func(rows *fakeRows) {
					rows.data[1][2] = uint8(0)
				},
			},
			{
				name: "empty input carries value",
				mutate: func(rows *fakeRows) {
					rows.data[0][2] = uint8(0)
					rows.data[1][2] = uint8(0)
				},
			},
		} {
			malformed := malformed
			t.Run(aggregate.name+"/"+malformed.name, func(t *testing.T) {
				t.Parallel()

				rows := fixedValueTimechartRows(
					[]any{float64(10), nil},
					[]uint8{1, 1},
				)
				malformed.mutate(rows)
				query := fixedValueTimechartQuery(first, 2, aggregate.name, aggregate.kind)
				connection := &fakeQueryConnection{rows: rows}
				sink := &fakeSink{}
				err := mustExecutor(t, connection).Execute(context.Background(), query, sink)
				if !errors.Is(err, searchjobs.ErrInvalidResult) {
					t.Fatalf("Execute error = %v, want ErrInvalidResult", err)
				}
				if connection.query == "" || sink.setCalls != 0 || len(sink.rows) != 0 {
					t.Fatalf(
						"malformed transport query=%q schema=%d rows=%d",
						connection.query,
						sink.setCalls,
						len(sink.rows),
					)
				}
			})
		}
	}
}
