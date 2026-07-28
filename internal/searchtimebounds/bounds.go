// Package searchtimebounds owns the backend-wide representable timestamp
// domain independently of any storage or SPL consumer.
package searchtimebounds

import "time"

const (
	MinimumUnixSeconds = int64(-2_208_988_800) // 1900-01-01T00:00:00Z
	MaximumUnixSeconds = int64(9_214_646_400)  // 2262-01-01T00:00:00Z

	MaximumSpanSeconds  = uint64(MaximumUnixSeconds - MinimumUnixSeconds)
	MaximumSpanMinutes  = MaximumSpanSeconds / 60
	MaximumSpanHours    = MaximumSpanMinutes / 60
	MaximumSpanDays     = MaximumSpanHours / 24
	MaximumSpanWeeks    = MaximumSpanDays / 7
	MaximumSpanYears    = uint64(362)
	MaximumSpanMonths   = MaximumSpanYears * 12
	MaximumSpanQuarters = MaximumSpanYears * 4
)

// MinimumTime returns the earliest backend-representable search timestamp.
func MinimumTime() time.Time {
	return time.Unix(MinimumUnixSeconds, 0).UTC()
}

// MaximumTime returns the conservative inclusive backend timestamp ceiling.
func MaximumTime() time.Time {
	return time.Unix(MaximumUnixSeconds, 0).UTC()
}

// Supports reports whether both boundaries are nonzero and representable.
// Range ordering deliberately belongs to the caller.
func Supports(earliest, latest time.Time) bool {
	return !earliest.IsZero() && !latest.IsZero() &&
		!earliest.Before(MinimumTime()) && !latest.After(MaximumTime())
}
