package searchtimebounds

import (
	"testing"
	"time"
)

func TestBoundsPinBackendTimestampDomainAndExactSpans(t *testing.T) {
	t.Parallel()

	minimum := time.Date(1900, 1, 1, 0, 0, 0, 0, time.UTC)
	maximum := time.Date(2262, 1, 1, 0, 0, 0, 0, time.UTC)
	if !MinimumTime().Equal(minimum) || MinimumTime().Location() != time.UTC {
		t.Fatalf("MinimumTime() = %v, want %v", MinimumTime(), minimum)
	}
	if !MaximumTime().Equal(maximum) || MaximumTime().Location() != time.UTC {
		t.Fatalf("MaximumTime() = %v, want %v", MaximumTime(), maximum)
	}
	if MaximumSpanSeconds != 11_423_635_200 ||
		MaximumSpanMinutes != 190_393_920 ||
		MaximumSpanHours != 3_173_232 ||
		MaximumSpanDays != 132_218 ||
		MaximumSpanWeeks != 18_888 ||
		MaximumSpanMonths != 4_344 ||
		MaximumSpanQuarters != 1_448 ||
		MaximumSpanYears != 362 ||
		MinimumYear != 1900 ||
		MaximumYear != 2262 ||
		YearRangeDescription != "1900-to-2262" {
		t.Fatalf(
			"span limits = %d seconds/%d minutes/%d hours/%d days/%d weeks/%d months/%d quarters/%d years; domain %d..%d (%q)",
			MaximumSpanSeconds,
			MaximumSpanMinutes,
			MaximumSpanHours,
			MaximumSpanDays,
			MaximumSpanWeeks,
			MaximumSpanMonths,
			MaximumSpanQuarters,
			MaximumSpanYears,
			MinimumYear,
			MaximumYear,
			YearRangeDescription,
		)
	}
}

func TestSupportsRejectsZeroAndOutOfRangeBoundsWithoutOwningOrdering(t *testing.T) {
	t.Parallel()

	minimum := MinimumTime()
	maximum := MaximumTime()
	for _, test := range []struct {
		name     string
		earliest time.Time
		latest   time.Time
		want     bool
	}{
		{"full domain", minimum, maximum, true},
		{"single point", minimum, minimum, true},
		{"zero earliest", time.Time{}, maximum, false},
		{"zero latest", minimum, time.Time{}, false},
		{"below minimum", minimum.Add(-time.Nanosecond), maximum, false},
		{"above maximum", minimum, maximum.Add(time.Nanosecond), false},
		{"reversed ordering belongs to caller", maximum, minimum, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if got := Supports(test.earliest, test.latest); got != test.want {
				t.Fatalf(
					"Supports(%v, %v) = %t, want %t",
					test.earliest,
					test.latest,
					got,
					test.want,
				)
			}
		})
	}
}
