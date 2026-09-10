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
