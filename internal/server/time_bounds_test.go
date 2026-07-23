package server

import (
	"testing"
	"time"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
)

func TestAbsoluteTimeRangeUsesPracticalClickHouseNanosecondBounds(t *testing.T) {
	t.Parallel()

	minimum := clickhouse.MinimumSearchTime()
	maximum := clickhouse.MaximumSearchTime()
	exact := &opensplunkv1.TimeRangeSpec{
		Earliest: stringPointer(minimum.Format(time.RFC3339Nano)),
		Latest:   stringPointer(maximum.Format(time.RFC3339Nano)),
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
			spec := &opensplunkv1.TimeRangeSpec{
				Earliest: stringPointer(test.earliest.Format(time.RFC3339Nano)),
				Latest:   stringPointer(test.latest.Format(time.RFC3339Nano)),
			}
			if _, err := resolveSearchTimeRange(spec, time.Time{}); err == nil {
				t.Fatal("resolveSearchTimeRange() error = nil")
			}
		})
	}
}
