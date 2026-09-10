package clickhouse

import (
	"errors"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/ianatimezone"
	"github.com/Suhaibinator/open-splunk/internal/plan"
)

// exactTimechartGridSpec validates the planner's sealed boundary descriptor.
func exactTimechartGridSpec(operator *plan.Timechart, scan *plan.Scan, timezone string) (timechartGridSpec, error) {
	if uint64(len(operator.GridBoundaries)) != operator.BucketCount+1 || operator.BucketCount == 0 || operator.BucketCount > 10000 {
		return timechartGridSpec{}, errors.New("compile timechart: exact boundary count is invalid")
	}
	for index, boundary := range operator.GridBoundaries {
		if boundary.Location() != time.UTC || !time.Unix(0, boundary.UnixNano()).Equal(boundary) || (index > 0 && !boundary.After(operator.GridBoundaries[index-1])) {
			return timechartGridSpec{}, errors.New("compile timechart: exact boundaries are invalid")
		}
	}
	if !operator.FirstBucket.Equal(operator.GridBoundaries[0]) {
		return timechartGridSpec{}, errors.New("compile timechart: exact origin is invalid")
	}
	// Rebuild from immutable search bounds, or the materialized input extent.
	copyOperator := *operator
	copyOperator.AuthoredSpan = operator.AuthoredSpan
	earliest, latest := operator.GridBoundaries[0], operator.GridBoundaries[len(operator.GridBoundaries)-1]
	if operator.FixedRange {
		earliest, latest = scan.Earliest, scan.Latest
	}
	if err := plan.ResolveTimechartGrid(&copyOperator, earliest, latest, timezone); err != nil {
		return timechartGridSpec{}, err
	}
	if !slices.Equal(copyOperator.GridBoundaries, operator.GridBoundaries) || copyOperator.Span != operator.Span || copyOperator.Calendar != operator.Calendar || copyOperator.CalendarMagnitude != operator.CalendarMagnitude {
		return timechartGridSpec{}, errors.New("compile timechart: boundary descriptor disagrees with span")
	}
	return timechartGridSpec{exact: true, calendar: operator.Calendar, spanNanoseconds: int64(operator.Span), firstBucket: operator.FirstBucket, bucketCount: operator.BucketCount, searchTimezone: timezone, magnitude: operator.CalendarMagnitude, boundaries: slices.Clone(operator.GridBoundaries)}, nil
}

func (spec timechartGridSpec) exactBucketKeySQL(eventTime string) string {
	origin := strconv.FormatInt(spec.firstBucket.UnixNano(), 10)
	if spec.calendar == plan.CalendarNone {
		span := strconv.FormatInt(spec.spanNanoseconds, 10)
		delta := "(toInt128(toUnixTimestamp64Nano(" + eventTime + ")) - toInt128(" + origin + "))"
		return "toInt64((intDiv(" + delta + ", " + span + ") - if(" + delta + " < 0 AND modulo(" + delta + ", " + span + ") != 0, 1, 0)) * " + span + " + toInt128(" + origin + "))"
	}

	magnitude := spec.magnitude
	if spec.calendar == plan.CalendarWeek {
		magnitude *= 7
	}
	unit, add := "day", "addDays"
	if spec.calendar == plan.CalendarMonth {
		unit, add = "month", "addMonths"
	}
	timezone := "'" + strings.ReplaceAll(spec.searchTimezone, "'", "''") + "'"
	localOrigin := "toTimeZone(fromUnixTimestamp64Nano(" + origin + ", 'UTC'), " + timezone + ")"
	localEvent := "toTimeZone(" + eventTime + ", " + timezone + ")"
	difference := "dateDiff('" + unit + "', " + localOrigin + ", " + localEvent + ", " + timezone + ")"
	step := strconv.FormatUint(magnitude, 10)
	offset := "((intDiv(" + difference + ", " + step + ") - if(" + difference + " < 0 AND modulo(" + difference + ", " + step + ") != 0, 1, 0)) * " + step + ")"
	candidate := add + "(" + localOrigin + ", " + offset + ")"
	return "toUnixTimestamp64Nano(if(" + candidate + " > " + localEvent + ", " + add + "(" + candidate + ", -" + step + "), " + candidate + "))"

}

func (spec timechartGridSpec) exactGridSQL(ordinal, bucketKey string) string {
	return "SELECT toUInt64(number) AS " + ordinal + ", arrayElement(?, number + 1) AS " + bucketKey + " FROM numbers(?)"
}

func (spec timechartGridSpec) exactGridArgs(args []any) []any {
	ticks := make([]int64, len(spec.boundaries)-1)
	for index := range ticks {
		ticks[index] = spec.boundaries[index].UnixNano()
	}
	return append(args, ticks, spec.bucketCount)
}

// The legacy compiler has already checked origin, span, count, and search
// coverage. Validate the attached descriptor in place without replanning or
// allocating a second full grid for each compilation.
func validateLegacyTimechartBoundaries(operator *plan.Timechart, timezone string) error {
	if len(operator.GridBoundaries) == 0 {
		return nil
	}
	if uint64(len(operator.GridBoundaries)) != operator.BucketCount+1 {
		return errors.New("compile timechart: boundary count is invalid")
	}
	location := time.UTC
	if operator.Calendar != plan.CalendarNone {
		var err error
		location, err = ianatimezone.Load(timezone)
		if err != nil {
			return err
		}
	}
	expected := operator.FirstBucket
	for _, boundary := range operator.GridBoundaries {
		if boundary.Location() != time.UTC || !boundary.Equal(expected) {
			return errors.New("compile timechart: boundary descriptor disagrees with span")
		}
		if operator.Calendar == plan.CalendarNone {
			expected = expected.Add(operator.Span)
		} else {
			expected = addCalendarUnit(expected.In(location), operator.Calendar).UTC()
		}
	}
	return nil
}
