package clickhouse

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/ianatimezone"
	"github.com/Suhaibinator/open-splunk/internal/plan"
)

// timechartGridSpec keeps the existing fixed ordinal grid byte-for-byte while
// carrying the civil-time inputs needed by the calendar-only branch.
type timechartGridSpec struct {
	calendar          plan.CalendarUnit
	spanNanoseconds   int64
	firstBucketNumber int64
	firstBucket       time.Time
	bucketCount       uint64
	searchTimezone    string
}

func (spec timechartGridSpec) isCalendar() bool {
	return spec.calendar != plan.CalendarNone
}

func (spec timechartGridSpec) relationalDepth() int {
	depth := relationalNodeDepth()
	if spec.isCalendar() {
		// The calendar arrayJoin lives in an inner SELECT so its tuple alias can
		// feed both the ordinal and boundary projections of the outer grid.
		depth = relationalNodeDepth(depth)
	}
	return depth
}

func (spec timechartGridSpec) bucketKeySQL(eventTime, ticks string) string {
	if !spec.isCalendar() {
		return epochFloorBucketNumberSQL(ticks)
	}
	return calendarBucketKeySQL(eventTime, spec.calendar)
}

// calendarBucketKeySQL returns one UTC DateTime64 expression for a local
// civil boundary. toStartOfWeek returns Date under the pinned ClickHouse
// settings, so the weekly branch must explicitly reattach the effective
// timezone before converting that local midnight to UTC.
func calendarBucketKeySQL(eventTime string, calendar plan.CalendarUnit) string {
	switch calendar {
	case plan.CalendarDay:
		boundary := "toStartOfDay(toTimeZone(" + eventTime + ", ?))"
		return "toDateTime64(toTimeZone(" + boundary + ", 'UTC'), 9, 'UTC')"
	case plan.CalendarWeek:
		boundary := "toStartOfWeek(toTimeZone(" + eventTime + ", ?), 0)"
		localMidnight := "toDateTime64(" + boundary + ", 9, ?)"
		return "toDateTime64(toTimeZone(" + localMidnight + ", 'UTC'), 9, 'UTC')"
	default:
		panic("invalid calendar bucket unit")
	}
}

func appendCalendarBucketKeyArgs(
	args []any,
	calendar plan.CalendarUnit,
	searchTimezone string,
) []any {
	args = append(args, searchTimezone)
	if calendar == plan.CalendarWeek {
		args = append(args, searchTimezone)
	}
	return args
}

func (spec timechartGridSpec) gridSQL(ordinal, bucketKey string) string {
	if !spec.isCalendar() {
		return ordinalGridSQL(ordinal, bucketKey)
	}
	add := "addDays"
	if spec.calendar == plan.CalendarWeek {
		add = "addWeeks"
	}
	item := quoteIdentifier("__os_calendar_grid_item")
	return "SELECT toUInt64(tupleElement(" + item + ", 1)) AS " + ordinal +
		", toTimeZone(tupleElement(" + item + ", 2), 'UTC') AS " + bucketKey +
		" FROM (SELECT arrayJoin(arrayMap(i -> (toUInt64(i), " + add +
		"(toTimeZone(toDateTime64(?, 9, 'UTC'), ?), i)), range(?))) AS " +
		item + ")"
}

func (spec timechartGridSpec) appendArgs(args []any) []any {
	if !spec.isCalendar() {
		return appendOrdinalGridArgs(
			args,
			spec.spanNanoseconds,
			spec.firstBucketNumber,
			spec.bucketCount,
		)
	}
	args = appendCalendarBucketKeyArgs(args, spec.calendar, spec.searchTimezone)
	return append(
		args,
		formatDateTime64Nanoseconds(spec.firstBucket),
		spec.searchTimezone,
		spec.bucketCount,
	)
}

func (spec timechartGridSpec) writeBucketProjection(
	sql *strings.Builder,
	grid string,
	bucketKey string,
) {
	if !spec.isCalendar() {
		return
	}
	sql.WriteString(", ")
	sql.WriteString(grid)
	sql.WriteString(".")
	sql.WriteString(bucketKey)
	sql.WriteString(" AS ")
	sql.WriteString(quoteIdentifier(TimechartBucketColumn))
}

func (spec timechartGridSpec) writeCalendarBucketCountGridSQL(
	sql *strings.Builder,
	g bucketCountGrid,
	eventTime string,
) {
	sql.WriteString(g.counts)
	sql.WriteString(" AS (SELECT ")
	sql.WriteString(spec.bucketKeySQL(eventTime, g.ticks))
	sql.WriteString(" AS ")
	sql.WriteString(g.bucketNumber)
	sql.WriteString(", count() AS ")
	sql.WriteString(g.count)
	sql.WriteString(" FROM ")
	sql.WriteString(g.countsSource)
	sql.WriteString(" GROUP BY ")
	sql.WriteString(g.bucketNumber)
	sql.WriteString("), ")

	sql.WriteString(g.grid)
	sql.WriteString(" AS (")
	sql.WriteString(spec.gridSQL(g.ordinal, g.bucketNumber))
	sql.WriteString(") SELECT ")
	sql.WriteString(g.grid)
	sql.WriteString(".")
	sql.WriteString(g.ordinal)
	sql.WriteString(" AS ")
	sql.WriteString(g.ordinal)
	spec.writeBucketProjection(sql, g.grid, g.bucketNumber)
	sql.WriteString(", ifNull(")
	sql.WriteString(g.counts)
	sql.WriteString(".")
	sql.WriteString(g.count)
	sql.WriteString(", toUInt64(0)) AS ")
	sql.WriteString(g.count)
	sql.WriteString(" FROM ")
	sql.WriteString(g.grid)
	sql.WriteString(" LEFT JOIN ")
	sql.WriteString(g.counts)
	sql.WriteString(" ON ")
	sql.WriteString(g.counts)
	sql.WriteString(".")
	sql.WriteString(g.bucketNumber)
	sql.WriteString(" = ")
	sql.WriteString(g.grid)
	sql.WriteString(".")
	sql.WriteString(g.bucketNumber)
	sql.WriteString(" ORDER BY ")
	sql.WriteString(g.grid)
	sql.WriteString(".")
	sql.WriteString(g.ordinal)
	sql.WriteString(" ASC")
}

func fixedTimechartGridSpec(
	operator *plan.Timechart,
	scan *plan.Scan,
) (timechartGridSpec, error) {
	spanSeconds := int64(operator.Span / time.Second)
	spanNanoseconds, err := validateFixedTimeGridSpec(TimelineSpec{
		FirstBucket: operator.FirstBucket,
		SpanSeconds: spanSeconds,
		BucketCount: operator.BucketCount,
		Earliest:    scan.Earliest,
		Latest:      scan.Latest,
	}, "timechart")
	if err != nil {
		return timechartGridSpec{}, err
	}
	firstBucketNumber, ok := ordinalGridFirstBucketNumber(
		operator.FirstBucket.Unix(),
		spanSeconds,
		operator.BucketCount,
	)
	if !ok {
		return timechartGridSpec{}, errors.New(
			"compile ClickHouse timechart: bucket grid overflows",
		)
	}
	return timechartGridSpec{
		spanNanoseconds:   spanNanoseconds,
		firstBucketNumber: firstBucketNumber,
		firstBucket:       operator.FirstBucket,
		bucketCount:       operator.BucketCount,
	}, nil
}

func calendarTimechartGridSpec(
	operator *plan.Timechart,
	scan *plan.Scan,
	searchTimezone string,
) (timechartGridSpec, error) {
	location, err := ianatimezone.Load(searchTimezone)
	if err != nil {
		return timechartGridSpec{}, errors.New(
			"compile ClickHouse timechart: search timezone is invalid",
		)
	}
	if operator.FirstBucket.IsZero() || operator.FirstBucket.Location() != time.UTC ||
		operator.FirstBucket.Nanosecond() != 0 || operator.BucketCount == 0 ||
		operator.BucketCount > 10_000 || !scan.Earliest.Before(scan.Latest) ||
		!SupportsSearchTimeRange(scan.Earliest, scan.Latest) {
		return timechartGridSpec{}, errors.New(
			"compile ClickHouse timechart: calendar grid is invalid",
		)
	}
	if operator.FirstBucket.Before(MinimumSearchTime()) {
		return timechartGridSpec{}, &plan.Diagnostic{
			Code:    "SPL_UNSUPPORTED_TIMECHART_TIME_RANGE",
			Message: "the first calendar bucket falls before the supported timestamp range",
			Range:   operator.Range,
			Suggestions: []string{
				"move the search earliest time forward",
				"use a fixed span shorter than 24 hours",
			},
		}
	}
	first := calendarBoundary(scan.Earliest, operator.Calendar, location)
	if first.IsZero() || !first.UTC().Equal(operator.FirstBucket) {
		return timechartGridSpec{}, errors.New(
			"compile ClickHouse timechart: first calendar bucket is invalid",
		)
	}
	cursor := first
	var count uint64
	for cursor.Before(scan.Latest) {
		count++
		if count > 10_000 {
			return timechartGridSpec{}, errors.New(
				"compile ClickHouse timechart: calendar grid exceeds the bucket limit",
			)
		}
		cursor = addCalendarUnit(cursor, operator.Calendar)
	}
	if count != operator.BucketCount {
		return timechartGridSpec{}, fmt.Errorf(
			"compile ClickHouse timechart: calendar bucket count is %d, want %d",
			operator.BucketCount,
			count,
		)
	}
	return timechartGridSpec{
		calendar:       operator.Calendar,
		firstBucket:    operator.FirstBucket,
		bucketCount:    operator.BucketCount,
		searchTimezone: searchTimezone,
	}, nil
}

func calendarBoundary(
	value time.Time,
	unit plan.CalendarUnit,
	location *time.Location,
) time.Time {
	local := value.In(location)
	boundary := time.Date(
		local.Year(), local.Month(), local.Day(), 0, 0, 0, 0, location,
	)
	if unit == plan.CalendarWeek {
		boundary = boundary.AddDate(0, 0, -int(boundary.Weekday()))
	}
	return boundary
}

func addCalendarUnit(value time.Time, unit plan.CalendarUnit) time.Time {
	switch unit {
	case plan.CalendarDay:
		return value.AddDate(0, 0, 1)
	case plan.CalendarWeek:
		return value.AddDate(0, 0, 7)
	default:
		return time.Time{}
	}
}
