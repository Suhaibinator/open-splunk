package loggen

import (
	"math"
	"time"

	"fortio.org/safecast"
)

type ordinalSchedule struct {
	intervalNanos  uint64
	maximumOrdinal uint64
}

func newOrdinalSchedule(interval time.Duration) (ordinalSchedule, bool) {
	if interval < 0 {
		return ordinalSchedule{}, false
	}
	if interval == 0 {
		return ordinalSchedule{maximumOrdinal: math.MaxUint64}, true
	}

	intervalNanos := uint64(interval)
	return ordinalSchedule{
		intervalNanos:  intervalNanos,
		maximumOrdinal: uint64(math.MaxInt64) / intervalNanos,
	}, true
}

func (schedule ordinalSchedule) offset(ordinal uint64) (time.Duration, bool) {
	if ordinal > schedule.maximumOrdinal {
		return 0, false
	}
	if schedule.intervalNanos == 0 {
		return 0, true
	}

	return time.Duration(safecast.MustConv[int64](ordinal * schedule.intervalNanos)), true
}
