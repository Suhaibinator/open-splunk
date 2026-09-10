package queryexec

import (
	"testing"
	"time"

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
