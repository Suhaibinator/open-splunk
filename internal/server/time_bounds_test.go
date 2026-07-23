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
	earliest, latest, err := absoluteTimeRange(exact)
	if err != nil {
		t.Fatalf("absoluteTimeRange(exact bounds) error = %v", err)
	}
	if !earliest.Equal(minimum) || !latest.Equal(maximum) {
		t.Fatalf("absoluteTimeRange(exact bounds) = [%v, %v), want [%v, %v)", earliest, latest, minimum, maximum)
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
			if _, _, err := absoluteTimeRange(spec); err == nil {
				t.Fatal("absoluteTimeRange() error = nil")
			}
		})
	}
}
