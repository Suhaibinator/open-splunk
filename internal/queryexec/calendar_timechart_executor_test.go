package queryexec

import (
	"context"
	"errors"
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
)

func TestExecutorPublishesCalendarBoundariesAcrossEveryTimechartMode(t *testing.T) {
	t.Parallel()

	// These are consecutive New York civil midnights across the spring DST
	// transition. Their UTC spacing proves that the executor publishes the
	// private boundaries instead of reconstructing a fixed-duration grid.
	buckets := []time.Time{
		time.Date(2026, time.March, 7, 5, 0, 0, 0, time.UTC),
		time.Date(2026, time.March, 8, 5, 0, 0, 0, time.UTC),
		time.Date(2026, time.March, 9, 4, 0, 0, 0, time.UTC),
	}
	tests := []struct {
		name  string
		query clickhouse.CompiledQuery
		rows  *fakeRows
	}{
		{
			name:  "fixed count",
			query: fixedTimechartQuery(buckets[0], uint64(len(buckets))),
			rows:  fixedTimechartOrdinalRows([]uint64{2, 0, 3}),
		},
		{
			name: "fixed field count",
			query: fixedCountFieldTimechartQuery(
				buckets[0],
				uint64(len(buckets)),
				"eligible_values",
			),
			rows: fixedCountFieldTimechartRows(
				[]uint64{1, 0, 2},
				[]uint8{1, 1, 1},
			),
		},
		{
			name: "fixed value",
			query: fixedValueTimechartQuery(
				buckets[0],
				uint64(len(buckets)),
				"average_latency",
				clickhouse.TimechartValueKindAverage,
			),
			rows: fixedValueTimechartRows(
				[]any{float64(1.5), nil, float64(3.5)},
				[]uint8{1, 1, 1},
			),
		},
		{
			name:  "runtime wide count",
			query: timechartQuery(buckets[0], uint64(len(buckets))),
			rows: timechartOrdinalRows(
				[]string{"0:api"},
				[][]uint64{{1}, {0}, {2}},
			),
		},
		{
			name: "runtime wide value",
			query: splitValueTimechartQuery(
				buckets[0],
				uint64(len(buckets)),
				clickhouse.TimechartValueKindAverage,
			),
			rows: splitValueTimechartRows(
				[]string{"0:api"},
				[][]float64{{1.5}, {0}, {3.5}},
				[][]uint8{{1}, {0}, {1}},
			),
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			test.query.Timechart.Span = 0
			test.query.Timechart.Calendar = true
			calendarTimechartRows(test.rows, buckets)
			sink := &fakeSink{}
			if err := mustExecutor(t, &fakeQueryConnection{rows: test.rows}).Execute(
				context.Background(),
				test.query,
				sink,
			); err != nil {
				t.Fatalf("Execute: %v", err)
			}
			if sink.setCalls != 1 || len(sink.rows) != len(buckets) || !test.rows.closed {
				t.Fatalf(
					"schema calls=%d rows=%d closed=%v",
					sink.setCalls,
					len(sink.rows),
					test.rows.closed,
				)
			}
			if len(sink.schema.Columns) == 0 ||
				sink.schema.Columns[0] != (searchjobs.Column{
					Name: "_time", Kind: searchjobs.ValueKindTime,
				}) {
				t.Fatalf("public schema = %#v", sink.schema)
			}
			for index, row := range sink.rows {
				bucket, ok := row[0].Time()
				if !ok || !bucket.Equal(buckets[index]) || bucket.Location() != time.UTC {
					t.Fatalf("row %d bucket = (%v, %v), want %v", index, bucket, ok, buckets[index])
				}
			}
		})
	}
}

func TestExecutorRejectsMalformedCalendarTimechartTransportAtomically(t *testing.T) {
	t.Parallel()

	first := time.Date(2026, time.March, 7, 5, 0, 0, 0, time.UTC)
	buckets := []time.Time{first, first.Add(24 * time.Hour)}
	tests := []struct {
		name   string
		mutate func(*fakeRows)
	}{
		{
			name: "missing private boundary",
			mutate: func(rows *fakeRows) {
				rows.columns = slices.Delete(rows.columns, 1, 2)
				rows.types = slices.Delete(rows.types, 1, 2)
				for index := range rows.data {
					rows.data[index] = slices.Delete(rows.data[index], 1, 2)
				}
			},
		},
		{
			name: "wrong private boundary name",
			mutate: func(rows *fakeRows) {
				rows.columns[1] = "attacker_bucket"
			},
		},
		{
			name: "wrong private boundary database type",
			mutate: func(rows *fakeRows) {
				rows.types[1] = fakeColumnType{
					name:         clickhouse.TimechartBucketColumn,
					databaseType: "DateTime('UTC')",
					scanType:     reflect.TypeFor[time.Time](),
				}
			},
		},
		{
			name: "wrong private boundary scan type",
			mutate: func(rows *fakeRows) {
				rows.types[1] = fakeColumnType{
					name:         clickhouse.TimechartBucketColumn,
					databaseType: "DateTime64(9, 'UTC')",
					scanType:     reflect.TypeFor[string](),
				}
			},
		},
		{
			name: "first boundary differs from sealed origin",
			mutate: func(rows *fakeRows) {
				rows.data[0][1] = first.Add(time.Hour)
			},
		},
		{
			name: "boundary repeats",
			mutate: func(rows *fakeRows) {
				rows.data[1][1] = first
			},
		},
		{
			name: "boundary decreases",
			mutate: func(rows *fakeRows) {
				rows.data[1][1] = first.Add(-time.Second)
			},
		},
		{
			name: "subsecond boundary",
			mutate: func(rows *fakeRows) {
				rows.data[1][1] = buckets[1].Add(time.Nanosecond)
			},
		},
		{
			name: "non-UTC boundary",
			mutate: func(rows *fakeRows) {
				rows.data[1][1] = buckets[1].In(time.FixedZone("UTC-like", 0))
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			rows := timechartOrdinalRows([]string{"0:api"}, [][]uint64{{1}, {2}})
			calendarTimechartRows(rows, buckets)
			test.mutate(rows)
			query := timechartQuery(first, uint64(len(buckets)))
			query.Timechart.Span = 0
			query.Timechart.Calendar = true
			connection := &fakeQueryConnection{rows: rows}
			sink := &fakeSink{}
			err := mustExecutor(t, connection).Execute(context.Background(), query, sink)
			if !errors.Is(err, searchjobs.ErrInvalidResult) {
				t.Fatalf("Execute error = %v, want %v", err, searchjobs.ErrInvalidResult)
			}
			if connection.query == "" || sink.setCalls != 0 || len(sink.rows) != 0 {
				t.Fatalf(
					"malformed transport query=%q schema calls=%d rows=%d",
					connection.query,
					sink.setCalls,
					len(sink.rows),
				)
			}
		})
	}
}

func TestExecutorRejectsTimechartCalendarSpanDiscriminatorMismatch(t *testing.T) {
	t.Parallel()

	first := time.Date(2026, time.March, 7, 5, 0, 0, 0, time.UTC)
	for _, test := range []struct {
		name   string
		mutate func(*clickhouse.TimechartOutput)
	}{
		{
			name: "zero span without calendar discriminator",
			mutate: func(output *clickhouse.TimechartOutput) {
				output.Span = 0
			},
		},
		{
			name: "calendar discriminator with fixed span",
			mutate: func(output *clickhouse.TimechartOutput) {
				output.Calendar = true
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			query := timechartQuery(first, 2)
			test.mutate(query.Timechart)
			connection := &fakeQueryConnection{
				rows: timechartOrdinalRows([]string{"0:api"}, [][]uint64{{1}, {2}}),
			}
			sink := &fakeSink{}
			err := mustExecutor(t, connection).Execute(context.Background(), query, sink)
			if !errors.Is(err, searchjobs.ErrInvalidResult) || connection.query != "" ||
				sink.setCalls != 0 || len(sink.rows) != 0 {
				t.Fatalf(
					"mismatched discriminator error=%v query=%q schema calls=%d rows=%d",
					err,
					connection.query,
					sink.setCalls,
					len(sink.rows),
				)
			}
		})
	}
}

func TestExecutorRejectsCalendarBoundaryOnFixedTimechart(t *testing.T) {
	t.Parallel()

	first := time.Date(2026, time.March, 7, 5, 0, 0, 0, time.UTC)
	rows := fixedTimechartOrdinalRows([]uint64{1, 2})
	calendarTimechartRows(rows, []time.Time{first, first.Add(5 * time.Minute)})
	connection := &fakeQueryConnection{rows: rows}
	sink := &fakeSink{}
	err := mustExecutor(t, connection).Execute(
		context.Background(),
		fixedTimechartQuery(first, 2),
		sink,
	)
	if !errors.Is(err, searchjobs.ErrInvalidResult) || connection.query == "" ||
		sink.setCalls != 0 || len(sink.rows) != 0 {
		t.Fatalf(
			"fixed transport error=%v query=%q schema calls=%d rows=%d",
			err,
			connection.query,
			sink.setCalls,
			len(sink.rows),
		)
	}
}

func calendarTimechartRows(rows *fakeRows, buckets []time.Time) {
	if len(rows.data) != len(buckets) {
		panic("calendar timechart fixture length mismatch")
	}
	rows.columns = slices.Insert(
		rows.columns,
		1,
		clickhouse.TimechartBucketColumn,
	)
	rows.types = slices.Insert(
		rows.types,
		1,
		driver.ColumnType(fakeColumnType{
			name:         clickhouse.TimechartBucketColumn,
			databaseType: "DateTime64(9, 'UTC')",
			scanType:     reflect.TypeFor[time.Time](),
		}),
	)
	for index := range rows.data {
		rows.data[index] = slices.Insert(rows.data[index], 1, any(buckets[index]))
	}
}
