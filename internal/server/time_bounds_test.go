package server

import (
	"testing"
	"time"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
)

func TestAbsoluteTimeRangeUsesPracticalClickHouseNanosecondBounds(t *testing.T) {
	t.Parallel()

	minimum := clickhouse.MinimumSearchTime()
	maximum := clickhouse.MaximumSearchTime()
	exact := &opensplunk.TimeRangeSpec{
		Earliest: new(minimum.Format(time.RFC3339Nano)),
		Latest:   new(maximum.Format(time.RFC3339Nano)),
	}
	resolved, err := resolveSearchTimeRange(exact, time.Time{})
	if err != nil {
		t.Fatalf("resolveSearchTimeRange(exact bounds) error = %v", err)
	}
	if !resolved.Earliest().Equal(minimum) || !resolved.Latest().Equal(maximum) {
		t.Fatalf("resolveSearchTimeRange(exact bounds) = [%v, %v), want [%v, %v)", resolved.Earliest(), resolved.Latest(), minimum, maximum)
	}

	for _, test := range []struct {
		name     string
		earliest time.Time
		latest   time.Time
	}{
		{name: "one nanosecond before minimum", earliest: minimum.Add(-time.Nanosecond), latest: maximum},
		{name: "one nanosecond after maximum", earliest: minimum, latest: maximum.Add(time.Nanosecond)},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			spec := &opensplunk.TimeRangeSpec{
				Earliest: new(test.earliest.Format(time.RFC3339Nano)),
				Latest:   new(test.latest.Format(time.RFC3339Nano)),
			}
			if _, err := resolveSearchTimeRange(spec, time.Time{}); err == nil {
				t.Fatal("resolveSearchTimeRange() error = nil")
			}
		})
	}
}

func TestPublishedTimePresetsResolveThroughProtobufAdapter(t *testing.T) {
	t.Parallel()

	anchor := time.Date(2026, time.March, 9, 19, 0, 0, 123, time.UTC)
	timezone := "America/Los_Angeles"
	for _, test := range []struct {
		name         string
		earliest     string
		latest       string
		wantEarliest time.Time
		wantLatest   time.Time
	}{
		{
			name:         "Today",
			earliest:     "@d",
			latest:       "now",
			wantEarliest: time.Date(2026, time.March, 9, 7, 0, 0, 0, time.UTC),
			wantLatest:   anchor,
		},
		{
			name:         "Yesterday",
			earliest:     "-1d@d",
			latest:       "@d",
			wantEarliest: time.Date(2026, time.March, 8, 8, 0, 0, 0, time.UTC),
			wantLatest:   time.Date(2026, time.March, 9, 7, 0, 0, 0, time.UTC),
		},
		{
			name:         "All time",
			earliest:     "0",
			latest:       "now",
			wantEarliest: clickhouse.MinimumSearchTime(),
			wantLatest:   anchor,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			spec := &opensplunk.TimeRangeSpec{
				Earliest: new(test.earliest),
				Latest:   new(test.latest),
				Timezone: &timezone,
			}
			resolved, err := resolveSearchTimeRange(spec, anchor)
			if err != nil {
				t.Fatal(err)
			}
			if !resolved.Earliest().Equal(test.wantEarliest) ||
				!resolved.Latest().Equal(test.wantLatest) {
				t.Fatalf(
					"resolved range = [%s, %s), want [%s, %s)",
					resolved.Earliest(),
					resolved.Latest(),
					test.wantEarliest,
					test.wantLatest,
				)
			}
			intent := resolved.Intent()
			if intent.Earliest != test.earliest ||
				intent.Latest != test.latest ||
				intent.Timezone != timezone ||
				!intent.TimezoneSpecified {
				t.Fatalf("resolved intent = %+v", intent)
			}
		})
	}

	invalid := &opensplunk.TimeRangeSpec{
		Earliest: new("-1h"),
		Latest:   new("0"),
		Timezone: &timezone,
	}
	if _, err := resolveSearchTimeRange(invalid, anchor); err == nil {
		t.Fatal("latest=0 unexpectedly passed the protobuf adapter")
	}
}
