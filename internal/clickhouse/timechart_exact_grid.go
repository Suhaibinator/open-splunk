package clickhouse

import (
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/plan"
)

// exactTimechartGridSpec validates the planner's sealed boundary descriptor.
func exactTimechartGridSpec(operator *plan.Timechart, timezone string) (timechartGridSpec, error) {
	if len(operator.GridBoundaries) != int(operator.BucketCount)+1 || operator.BucketCount == 0 || operator.BucketCount > 10000 {
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
	// Rebuild from the same descriptor extent, preserving authored span and origin.
	copyOperator := *operator
	copyOperator.AuthoredSpan = operator.AuthoredSpan
	if err := plan.ResolveTimechartGrid(&copyOperator, operator.GridBoundaries[0], operator.GridBoundaries[len(operator.GridBoundaries)-1], timezone); err != nil {
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
	unit := "DAY"
	magnitude := spec.magnitude
	if spec.calendar == plan.CalendarWeek {
		magnitude *= 7
	}
	if spec.calendar == plan.CalendarMonth {
		unit = "MONTH"
	}
	timezone := "'" + strings.ReplaceAll(spec.searchTimezone, "'", "''") + "'"
	return fmt.Sprintf("toUnixTimestamp64Nano(toStartOfInterval(%s, INTERVAL %d %s, fromUnixTimestamp64Nano(%s, 'UTC'), %s))", eventTime, magnitude, unit, origin, timezone)
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
