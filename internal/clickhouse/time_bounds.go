package clickhouse

import "time"

const (
	minimumSearchUnixSeconds = int64(-2_208_988_800) // 1900-01-01T00:00:00Z
	maximumSearchUnixSeconds = int64(9_214_646_400)  // 2262-01-01T00:00:00Z
)

// MinimumSearchTime is the earliest timestamp that this backend admits into a
// DateTime64(9) search boundary. ClickHouse's nanosecond representation is
// documented and observed to begin at 1900 rather than at Go's wider
// time.Time or signed-nanosecond limits.
func MinimumSearchTime() time.Time {
	return time.Unix(minimumSearchUnixSeconds, 0).UTC()
}

// MaximumSearchTime is a conservative inclusive search-boundary ceiling for
// ClickHouse DateTime64(9). It stays below the precision-specific upper edge so
// every accepted nanosecond value has deterministic parsing behavior.
func MaximumSearchTime() time.Time {
	return time.Unix(maximumSearchUnixSeconds, 0).UTC()
}

// SupportsSearchTimeRange reports whether both boundaries can be represented
// by the event store. Range ordering is deliberately left to the caller.
func SupportsSearchTimeRange(earliest, latest time.Time) bool {
	return !earliest.IsZero() && !latest.IsZero() &&
		!earliest.Before(MinimumSearchTime()) && !latest.After(MaximumSearchTime())
}
