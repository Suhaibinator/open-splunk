package clickhouse

import (
	"testing"
	"time"
)

func TestSupportedSearchTimeRangeUsesPracticalDateTime64NanosecondBounds(t *testing.T) {
	t.Parallel()

	minimum := time.Date(1900, 1, 1, 0, 0, 0, 0, time.UTC)
	maximum := time.Date(2262, 1, 1, 0, 0, 0, 0, time.UTC)
	if !MinimumSearchTime().Equal(minimum) || MinimumSearchTime().Location() != time.UTC {
		t.Fatalf("MinimumSearchTime() = %v, want %v", MinimumSearchTime(), minimum)
	}
	if !MaximumSearchTime().Equal(maximum) || MaximumSearchTime().Location() != time.UTC {
		t.Fatalf("MaximumSearchTime() = %v, want %v", MaximumSearchTime(), maximum)
	}
	for _, test := range []struct {
		name     string
		earliest time.Time
		latest   time.Time
		want     bool
	}{
		{name: "exact bounds", earliest: minimum, latest: maximum, want: true},
		{name: "fractional values inside bounds", earliest: minimum.Add(time.Nanosecond), latest: maximum.Add(-time.Nanosecond), want: true},
		{name: "before minimum", earliest: minimum.Add(-time.Nanosecond), latest: maximum},
		{name: "after maximum", earliest: minimum, latest: maximum.Add(time.Nanosecond)},
		{name: "zero earliest", latest: maximum},
		{name: "zero latest", earliest: minimum},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if got := SupportsSearchTimeRange(test.earliest, test.latest); got != test.want {
				t.Fatalf("SupportsSearchTimeRange(%v, %v) = %v, want %v", test.earliest, test.latest, got, test.want)
			}
		})
	}
}
