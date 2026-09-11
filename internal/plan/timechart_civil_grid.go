package plan

import (
	"fmt"
	"time"

	"fortio.org/safecast"
)

// Civil coordinates live in UTC only to make AddDate independent of timezone
// normalization. Every boundary is derived from the same origin and index.
func timechartCivilTime(value time.Time) time.Time {
	return time.Date(value.Year(), value.Month(), value.Day(), value.Hour(), value.Minute(), value.Second(), value.Nanosecond(), time.UTC)
}

// A gap resolves to the first valid instant after the missing civil time. A
// fold resolves to its earlier occurrence. ZoneBounds supplies the transition
// itself, including whole skipped dates, without assuming a one-hour change.
func timechartResolveCivil(civil time.Time, location *time.Location) time.Time {
	value := time.Date(civil.Year(), civil.Month(), civil.Day(), civil.Hour(), civil.Minute(), civil.Second(), civil.Nanosecond(), location)
	roundtrip := timechartCivilTime(value)
	start, end := value.ZoneBounds()
	if roundtrip.Before(civil) {
		return end.UTC()
	}
	if roundtrip.After(civil) {
		return start.UTC()
	}
	if !start.IsZero() {
		_, previousOffset := start.Add(-time.Nanosecond).Zone()
		previous := civil.Add(-time.Duration(previousOffset) * time.Second)
		if previous.Before(value) && timechartCivilTime(previous.In(location)).Equal(civil) {
			return previous.UTC()
		}
	}
	return value.UTC()
}

func timechartCivilBoundaries(earliest, latest time.Time, calendar CalendarUnit, magnitude uint64, alignment time.Time, location *time.Location, bucketLimit uint64) ([]time.Time, error) {
	local := timechartCivilTime(earliest.In(location))
	origin := time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)
	step := safecast.MustConv[int64](magnitude)
	if calendar == CalendarWeek {
		origin = time.Date(1969, 12, 28, 0, 0, 0, 0, time.UTC)
		step *= 7
		if !alignment.IsZero() {
			origin = timechartCivilTime(alignment.In(location))
		}
	}
	civilIndex := func(value time.Time) int64 {
		if calendar == CalendarMonth {
			return floorInt64(int64(value.Year()-origin.Year())*12+int64(value.Month()-origin.Month()), step)
		}
		civilDay := func(value time.Time) int64 {
			return time.Date(value.Year(), value.Month(), value.Day(), 0, 0, 0, 0, time.UTC).Unix() / 86400
		}
		return floorInt64(civilDay(value)-civilDay(origin), step)
	}
	index := civilIndex(local)
	boundary := func(index int64) time.Time {
		// An authored weekly alignment is an instant, even in the second
		// occurrence of a fold. All derived coordinates use the civil rule.
		if calendar == CalendarWeek && index == 0 && !alignment.IsZero() {
			return alignment.UTC()
		}
		if calendar == CalendarMonth {
			return timechartResolveCivil(origin.AddDate(0, int(index*step), 0), location)
		}
		return timechartResolveCivil(origin.AddDate(0, 0, int(index*step)), location)
	}
	first := boundary(index)
	for first.After(earliest) {
		index--
		first = boundary(index)
	}
	if !time.Unix(0, first.UnixNano()).Equal(first) {
		return nil, fmt.Errorf("timechart boundary exceeds timestamp range")
	}
	// This is only a capacity estimate: skipped dates can reduce the actual
	// count, so admission below counts resolved intervals, not nominal dates.
	capacity := max(int64(1), min(safecast.MustConv[int64](bucketLimit)+1, civilIndex(timechartCivilTime(latest.In(location)))-index+2))
	boundaries := make([]time.Time, 1, int(capacity))
	boundaries[0] = first
	for boundaries[len(boundaries)-1].Before(latest) {
		index++
		next := boundary(index)
		current := boundaries[len(boundaries)-1]
		if next.Before(current) || !time.Unix(0, next.UnixNano()).Equal(next) {
			return nil, fmt.Errorf("timechart boundary exceeds timestamp range")
		}
		// Two nominal dates may resolve to one instant when a date is skipped.
		// Compact them rather than emitting an empty interval or losing a day.
		if next.Equal(current) {
			continue
		}
		if uint64(len(boundaries)) > bucketLimit {
			return nil, fmt.Errorf("timechart produces more than %d buckets", bucketLimit)
		}
		boundaries = append(boundaries, next)
	}
	return boundaries, nil
}
