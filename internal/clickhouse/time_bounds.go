package clickhouse

import (
	"time"

	"github.com/Suhaibinator/open-splunk/internal/searchtimebounds"
)

// MinimumSearchTime is the earliest timestamp that this backend admits into a
// DateTime64(9) search boundary. ClickHouse's nanosecond representation is
// documented and observed to begin at 1900 rather than at Go's wider
// time.Time or signed-nanosecond limits.
func MinimumSearchTime() time.Time {
	return searchtimebounds.MinimumTime()
}

// MaximumSearchTime is a conservative inclusive search-boundary ceiling for
// ClickHouse DateTime64(9). It stays below the precision-specific upper edge so
// every accepted nanosecond value has deterministic parsing behavior.
func MaximumSearchTime() time.Time {
	return searchtimebounds.MaximumTime()
}

// SupportsSearchTimeRange reports whether both boundaries can be represented
// by the event store. Range ordering is deliberately left to the caller.
func SupportsSearchTimeRange(earliest, latest time.Time) bool {
	return searchtimebounds.Supports(earliest, latest)
}
