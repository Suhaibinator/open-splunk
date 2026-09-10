package queryexec

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
)

type exactGridTestSink struct {
	rows   [][]searchjobs.Value
	bounds []searchjobs.TimeBucketBounds
}

func (*exactGridTestSink) SetSchema(searchjobs.Schema) error { return nil }
func (sink *exactGridTestSink) AddRow(values []searchjobs.Value) error {
	sink.rows = append(sink.rows, values)
	return nil
}
func (sink *exactGridTestSink) AddRowWithTimeBucket(values []searchjobs.Value, bounds searchjobs.TimeBucketBounds) error {
	sink.bounds = append(sink.bounds, bounds)
	return sink.AddRow(values)
}

func TestTimechartGridSinkPreservesPresenceAndExactBounds(t *testing.T) {
	first := time.Unix(0, -250_000_000).UTC()
	boundaries := []time.Time{first, first.Add(250 * time.Millisecond), first.Add(500 * time.Millisecond), first.Add(750 * time.Millisecond)}
	target := &exactGridTestSink{}
	sink := &timechartGridSink{ResultSink: target, output: clickhouse.TimechartOutput{ExactGrid: true, Boundaries: boundaries, BucketCount: 3, Continuous: false, IncludePartial: true}, occupancy: &timechartGridRows{present: []uint8{1, 0, 1}}}
	for _, bucket := range boundaries[:3] {
		if err := sink.AddRow([]searchjobs.Value{searchjobs.TimeValue(bucket), searchjobs.UnsignedValue(0)}); err != nil {
			t.Fatal(err)
		}
	}
	if len(target.rows) != 2 || len(target.bounds) != 2 || target.bounds[0].Earliest != "1969-12-31T23:59:59.75Z" || target.bounds[0].Latest != "1970-01-01T00:00:00Z" {
		t.Fatalf("target=%+v", target)
	}
	sink.output.IncludePartial = false
	sink.output.SearchEarliest = first.Add(time.Nanosecond)
	sink.output.SearchLatest = boundaries[3]
	if err := sink.AddRow([]searchjobs.Value{searchjobs.TimeValue(first), searchjobs.UnsignedValue(0)}); err != nil {
		t.Fatal(err)
	}
	if len(target.rows) != 2 {
		t.Fatal("partial leading bucket published")
	}
	sink.occupancy.present = sink.occupancy.present[:2]
	if err := sink.AddRow([]searchjobs.Value{searchjobs.TimeValue(first)}); err == nil {
		t.Fatal("incomplete presence accepted")
	}
}

func TestTimechartExactBoundaryRejectsInteriorMutation(t *testing.T) {
	first := time.Date(2026, 3, 8, 8, 0, 0, 0, time.UTC)
	output := clickhouse.TimechartOutput{Calendar: true, ExactGrid: true, FirstBucket: first, BucketCount: 2, Boundaries: []time.Time{first, first.Add(23 * time.Hour), first.Add(47 * time.Hour)}}
	forged := first.Add(24 * time.Hour)
	if _, err := validateTimechartRowBucket(output, 1, &forged, first); err == nil {
		t.Fatal("monotonic but incorrect boundary accepted")
	}
	exact := output.Boundaries[1]
	if _, err := validateTimechartRowBucket(output, 1, &exact, first); err != nil {
		t.Fatal(err)
	}
}

func TestTimechartGridScanReusesStorageWithoutRetainingCallerDestinations(t *testing.T) {
	first := time.Unix(0, 0).UTC()
	source := &fakeRows{data: [][]any{{uint64(0), first, uint8(1), uint64(4)}, {uint64(1), first.Add(time.Millisecond), uint8(0), uint64(0)}}}
	wrapped := &timechartGridRows{Rows: source, present: make([]uint8, 0, 2)}
	var ordinal0, count0, ordinal1, count1 uint64
	var bucket0, bucket1 time.Time
	if !wrapped.Next() {
		t.Fatal("first row missing")
	}
	if err := wrapped.Scan(&ordinal0, &bucket0, &count0); err != nil {
		t.Fatal(err)
	}
	if !wrapped.Next() {
		t.Fatal("second row missing")
	}
	if err := wrapped.Scan(&ordinal1, &bucket1, &count1); err != nil {
		t.Fatal(err)
	}
	if ordinal0 != 0 || count0 != 4 || !bucket0.Equal(first) || ordinal1 != 1 || count1 != 0 || !bucket1.Equal(first.Add(time.Millisecond)) {
		t.Fatal("scan destinations from different rows alias")
	}
	if len(wrapped.present) != 2 || wrapped.present[0] != 1 || wrapped.present[1] != 0 {
		t.Fatalf("presence=%v", wrapped.present)
	}
	for _, destination := range wrapped.destinations {
		if destination != nil {
			t.Fatal("scan retains a caller destination")
		}
	}
}

func TestTimechartGridScanRejectsMalformedPresenceAndWidth(t *testing.T) {
	source := &fakeRows{data: [][]any{{uint64(0), time.Unix(0, 0).UTC(), uint8(2), uint64(0)}}}
	wrapped := &timechartGridRows{Rows: source, present: make([]uint8, 0, 1)}
	if !wrapped.Next() {
		t.Fatal("fixture row missing")
	}
	var ordinal, count uint64
	var bucket time.Time
	if err := wrapped.Scan(&ordinal, &bucket, &count); err == nil {
		t.Fatal("invalid occupancy accepted")
	}
	if len(wrapped.present) != 0 {
		t.Fatal("invalid occupancy was retained")
	}
	for _, destinations := range [][]any{nil, {&ordinal}, make([]any, 7)} {
		if err := wrapped.Scan(destinations...); err == nil {
			t.Fatal("invalid scan width accepted")
		}
	}
}

func TestTimechartGridScanAcceptsWidestNumericTransport(t *testing.T) {
	first := time.Unix(0, 0).UTC()
	source := &fakeRows{data: [][]any{{uint64(0), first, uint8(1), []string{"0:api"}, []float64{3.5}, []uint8{1}, uint8(0)}}}
	wrapped := &timechartGridRows{Rows: source, present: make([]uint8, 0, 1)}
	if !wrapped.Next() {
		t.Fatal("fixture row missing")
	}
	var ordinal uint64
	var bucket time.Time
	var names []string
	var values []float64
	var present []uint8
	var invalid uint8
	if err := wrapped.Scan(&ordinal, &bucket, &names, &values, &present, &invalid); err != nil {
		t.Fatal(err)
	}
	if ordinal != 0 || !bucket.Equal(first) || len(names) != 1 || names[0] != "0:api" || len(values) != 1 || values[0] != 3.5 || len(present) != 1 || present[0] != 1 || invalid != 0 || len(wrapped.present) != 1 || wrapped.present[0] != 1 {
		t.Fatal("numeric transport fields changed while extracting occupancy")
	}
}

func TestExactGridAggregateValidationPrecedesEveryPresentationMode(t *testing.T) {
	first := time.Unix(0, 0).UTC()
	type fixture struct {
		name  string
		query clickhouse.CompiledQuery
		rows  func(bool) *fakeRows
	}
	countRows := func(empty bool) []uint64 {
		if empty {
			return []uint64{1, 0}
		}
		return []uint64{1, 1}
	}
	cases := []fixture{
		{"count", fixedTimechartQuery(first, 2), func(empty bool) *fakeRows { return fixedTimechartOrdinalRows(countRows(empty)) }},
		{"field count", fixedCountFieldTimechartQuery(first, 2, "count(value)"), func(empty bool) *fakeRows { return fixedCountFieldTimechartRows(countRows(empty), []uint8{1, 1}) }},
		{"split count", timechartQuery(first, 2), func(empty bool) *fakeRows {
			counts := countRows(empty)
			return timechartOrdinalRows([]string{"0:api"}, [][]uint64{{counts[0]}, {counts[1]}})
		}},
	}
	for _, kind := range []clickhouse.TimechartValueKind{clickhouse.TimechartValueKindSum, clickhouse.TimechartValueKindAverage, clickhouse.TimechartValueKindPercentile} {
		cases = append(cases, fixture{fmt.Sprintf("fixed value %d", kind), fixedValueTimechartQuery(first, 2, "value", kind), func(empty bool) *fakeRows {
			var second any = float64(0)
			if empty {
				second = nil
			}
			return fixedValueTimechartRows([]any{float64(1), second}, []uint8{1, 1})
		}}, fixture{fmt.Sprintf("split value %d", kind), splitValueTimechartQuery(first, 2, kind), func(empty bool) *fakeRows {
			presence := uint8(1)
			if empty {
				presence = 0
			}
			return splitValueTimechartRows([]string{"0:api"}, [][]float64{{1}, {0}}, [][]uint8{{1}, {presence}})
		}})
	}
	for _, testCase := range cases {
		for _, continuous := range []bool{false, true} {
			for _, partial := range []bool{false, true} {
				for _, valid := range []bool{false, true} {
					t.Run(fmt.Sprintf("%s/cont=%t/partial=%t/valid=%t", testCase.name, continuous, partial, valid), func(t *testing.T) {
						query := testCase.query
						output := *query.Timechart
						query.Timechart = &output
						output.Calendar, output.ExactGrid = true, true
						output.Span = time.Millisecond
						output.Boundaries = []time.Time{first, first.Add(time.Millisecond), first.Add(2 * time.Millisecond)}
						output.Continuous, output.IncludePartial = continuous, partial
						output.SearchEarliest, output.SearchLatest = first, output.Boundaries[1]
						rows := testCase.rows(valid)
						calendarTimechartRows(rows, output.Boundaries[:2])
						rows.columns = slices.Insert(rows.columns, 2, clickhouse.TimechartBucketPresentColumn)
						rows.types = slices.Insert(rows.types, 2, driver.ColumnType(fakeColumnType{name: clickhouse.TimechartBucketPresentColumn, databaseType: "UInt8", scanType: reflect.TypeFor[uint8]()}))
						rows.data[0] = slices.Insert(rows.data[0], 2, any(uint8(1)))
						rows.data[1] = slices.Insert(rows.data[1], 2, any(uint8(0)))
						sink := &fakeSink{}
						err := mustExecutor(t, &fakeQueryConnection{rows: rows}).Execute(context.Background(), query, sink)
						if valid {
							if err != nil || sink.setCalls != 1 {
								t.Fatalf("valid empty bucket: err=%v schema=%d", err, sink.setCalls)
							}
						} else if !errors.Is(err, searchjobs.ErrInvalidResult) || sink.setCalls != 0 || len(sink.rows) != 0 {
							t.Fatalf("invalid occupancy leaked: err=%v schema=%d rows=%d", err, sink.setCalls, len(sink.rows))
						}
						if !rows.closed {
							t.Fatal("transport remained open")
						}
					})
				}
			}
		}
	}
}

func TestTimechartGridRejectsOccupancyWithoutInput(t *testing.T) {
	rows := &timechartGridRows{rowPresent: 1}
	if err := validateTimechartGridAggregate(rows, false, false); !errors.Is(err, searchjobs.ErrInvalidResult) {
		t.Fatalf("occupancy contradicting empty input accepted: %v", err)
	}
	if err := validateTimechartGridAggregate(rows, false, true); err != nil {
		t.Fatalf("input with missing measures rejected: %v", err)
	}
}
