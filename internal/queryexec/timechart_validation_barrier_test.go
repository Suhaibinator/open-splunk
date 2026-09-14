package queryexec

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
)

func TestTimechartDirectExecutionValidatesCompleteGridBeforePublishing(t *testing.T) {
	first := time.Date(2026, time.September, 1, 0, 0, 0, 0, time.UTC)
	boundaries := []time.Time{
		first,
		first.Add(250 * time.Millisecond),
		first.Add(500 * time.Millisecond),
	}
	testCases := []struct {
		name           string
		continuous     bool
		includePartial bool
		searchLatest   time.Time
	}{
		{
			name:           "continuous",
			continuous:     true,
			includePartial: true,
			searchLatest:   boundaries[2],
		},
		{
			name:           "sparse output cannot hide invalid absent bucket",
			continuous:     false,
			includePartial: true,
			searchLatest:   boundaries[2],
		},
		{
			name:           "partial filtering cannot hide invalid edge bucket",
			continuous:     true,
			includePartial: false,
			searchLatest:   boundaries[1],
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			query := fixedTimechartQuery(first, 2)
			query.Timechart.Calendar = true
			query.Timechart.ExactGrid = true
			query.Timechart.Span = 250 * time.Millisecond
			query.Timechart.Boundaries = boundaries
			query.Timechart.Continuous = testCase.continuous
			query.Timechart.IncludePartial = testCase.includePartial
			query.Timechart.SearchEarliest = first
			query.Timechart.SearchLatest = testCase.searchLatest
			rows := lateInvalidTimechartOccupancyRows(first)
			sink := &fakeSink{}

			err := mustExecutor(t, &fakeQueryConnection{rows: rows}).Execute(
				context.Background(),
				query,
				sink,
			)
			if !errors.Is(err, searchjobs.ErrInvalidResult) {
				t.Fatalf("late invalid occupancy error = %v", err)
			}
			if rows.nextCalls != 2 || !rows.closed {
				t.Fatalf("late invalid occupancy was not fully consumed: next=%d closed=%t", rows.nextCalls, rows.closed)
			}
			if sink.setCalls != 0 || len(sink.schema.Columns) != 0 || len(sink.rows) != 0 {
				t.Fatalf(
					"late invalid occupancy published a prefix: schema calls=%d schema=%#v rows=%d",
					sink.setCalls,
					sink.schema,
					len(sink.rows),
				)
			}
		})
	}
}

func TestTimechartStaticSuffixValidatesAllBoundsBeforePublishing(t *testing.T) {
	first := time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)
	query := compileReadAdmissionQuery(
		t,
		`index=target | timechart span=1m count | where count > 0 | head 2`,
	)
	if query.TimeBucket == nil || !query.RequiresAtomicResult() {
		t.Fatalf("static timechart suffix lacks atomic bucket provenance: %#v", query)
	}
	rows := &fakeRows{
		columns: []string{"_time", "count", clickhouse.ResultTimeBucketEndColumn},
		types: []driver.ColumnType{
			fakeColumnType{name: "_time", databaseType: "DateTime64(9, 'UTC')", scanType: reflect.TypeFor[time.Time]()},
			fakeColumnType{name: "count", databaseType: "UInt64", scanType: reflect.TypeFor[uint64]()},
			fakeColumnType{name: clickhouse.ResultTimeBucketEndColumn, databaseType: "DateTime64(9, 'UTC')", scanType: reflect.TypeFor[time.Time]()},
		},
		data: [][]any{
			{first, uint64(1), first.Add(time.Minute)},
			{first.Add(time.Minute), uint64(1), first},
		},
	}
	sink := &fakeSink{}

	err := mustExecutor(t, &fakeQueryConnection{rows: rows}).Execute(
		context.Background(),
		query,
		sink,
	)
	if !errors.Is(err, searchjobs.ErrInvalidResult) {
		t.Fatalf("late invalid bucket interval error = %v", err)
	}
	if rows.nextCalls != 2 || !rows.closed {
		t.Fatalf("late invalid bucket interval was not fully consumed: next=%d closed=%t", rows.nextCalls, rows.closed)
	}
	if sink.setCalls != 0 || len(sink.schema.Columns) != 0 || len(sink.rows) != 0 {
		t.Fatalf(
			"late invalid bucket interval published a prefix: schema calls=%d schema=%#v rows=%d",
			sink.setCalls,
			sink.schema,
			len(sink.rows),
		)
	}
}

func lateInvalidTimechartOccupancyRows(first time.Time) *fakeRows {
	return &fakeRows{
		columns: []string{
			clickhouse.TimechartOrdinalColumn,
			clickhouse.TimechartBucketColumn,
			clickhouse.TimechartBucketPresentColumn,
			clickhouse.TimechartCountColumn,
		},
		types: []driver.ColumnType{
			fakeColumnType{name: clickhouse.TimechartOrdinalColumn, databaseType: "UInt64", scanType: reflect.TypeFor[uint64]()},
			fakeColumnType{name: clickhouse.TimechartBucketColumn, databaseType: "DateTime64(9, 'UTC')", scanType: reflect.TypeFor[time.Time]()},
			fakeColumnType{name: clickhouse.TimechartBucketPresentColumn, databaseType: "UInt8", scanType: reflect.TypeFor[uint8]()},
			fakeColumnType{name: clickhouse.TimechartCountColumn, databaseType: "UInt64", scanType: reflect.TypeFor[uint64]()},
		},
		data: [][]any{
			{uint64(0), first, uint8(1), uint64(1)},
			{uint64(1), first.Add(250 * time.Millisecond), uint8(0), uint64(1)},
		},
	}
}
