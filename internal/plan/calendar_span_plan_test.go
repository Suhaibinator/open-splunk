package plan

import (
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/spl"
)

func TestBuildCalendarBinSpans(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name     string
		span     string
		calendar CalendarUnit
	}{
		{name: "day", span: "1d", calendar: CalendarDay},
		{name: "week", span: "1w", calendar: CalendarWeek},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			logical, err := Build(
				mustParse(t, `index=gradethis | bin _time span=`+test.span),
				testScope([]string{"gradethis"}, nil),
			)
			if err != nil {
				t.Fatalf("Build: %v", err)
			}
			operator, ok := logical.Operators[len(logical.Operators)-1].(*TimeBucket)
			if !ok || operator.Span != 0 || operator.Calendar != test.calendar {
				t.Fatalf("last operator = %#v, want calendar time bucket", logical.Operators[len(logical.Operators)-1])
			}
		})
	}
}

func TestBuildCalendarBinRequiresCanonicalTime(t *testing.T) {
	t.Parallel()

	_, err := Build(
		mustParse(t, `index=gradethis | bin duration span=1d`),
		testScope([]string{"gradethis"}, nil),
	)
	assertDiagnosticCode(t, err, "SPL_UNSUPPORTED_BIN_TIME_FIELD")
}

func TestBuildCalendarTimechartBuckets(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name        string
		timezone    string
		span        string
		earliest    time.Time
		latest      time.Time
		calendar    CalendarUnit
		firstBucket time.Time
		bucketCount uint64
	}{
		{
			name:        "New York spring-forward day is 23 elapsed hours",
			timezone:    "America/New_York",
			span:        "1d",
			earliest:    time.Date(2026, 3, 8, 5, 0, 0, 0, time.UTC),
			latest:      time.Date(2026, 3, 9, 4, 0, 0, 0, time.UTC),
			calendar:    CalendarDay,
			firstBucket: time.Date(2026, 3, 8, 5, 0, 0, 0, time.UTC),
			bucketCount: 1,
		},
		{
			name:        "New York fall-back day is 25 elapsed hours",
			timezone:    "America/New_York",
			span:        "1d",
			earliest:    time.Date(2026, 11, 1, 4, 0, 0, 0, time.UTC),
			latest:      time.Date(2026, 11, 2, 5, 0, 0, 0, time.UTC),
			calendar:    CalendarDay,
			firstBucket: time.Date(2026, 11, 1, 4, 0, 0, 0, time.UTC),
			bucketCount: 1,
		},
		{
			name:        "week starts on previous Sunday across year boundary",
			timezone:    "UTC",
			span:        "1w",
			earliest:    time.Date(2025, 12, 31, 12, 0, 0, 0, time.UTC),
			latest:      time.Date(2026, 1, 2, 12, 0, 0, 0, time.UTC),
			calendar:    CalendarWeek,
			firstBucket: time.Date(2025, 12, 28, 0, 0, 0, 0, time.UTC),
			bucketCount: 1,
		},
		{
			name:        "exact UTC latest boundary remains half open",
			timezone:    "UTC",
			span:        "1d",
			earliest:    time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC),
			latest:      time.Date(2026, 1, 3, 0, 0, 0, 0, time.UTC),
			calendar:    CalendarDay,
			firstBucket: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
			bucketCount: 2,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			scope := testScope([]string{"gradethis"}, nil)
			scope.SearchTimezone = test.timezone
			scope.Earliest = test.earliest
			scope.Latest = test.latest
			logical, err := Build(
				mustParse(t, `index=gradethis | timechart span=`+test.span+` count`),
				scope,
			)
			if err != nil {
				t.Fatalf("Build: %v", err)
			}
			operator := logical.Operators[len(logical.Operators)-1].(*Timechart)
			if operator.Span != 0 || operator.Calendar != test.calendar ||
				!operator.FirstBucket.Equal(test.firstBucket) ||
				operator.FirstBucket.Location() != time.UTC ||
				operator.BucketCount != test.bucketCount {
				t.Fatalf("timechart = %#v, want first=%v count=%d calendar=%v", operator, test.firstBucket, test.bucketCount, test.calendar)
			}
		})
	}
}

func TestBuildCalendarTimechartBoundsBucketCount(t *testing.T) {
	t.Parallel()

	start := time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)
	scope := testScope([]string{"gradethis"}, nil)
	scope.Earliest = start
	scope.Latest = start.AddDate(0, 0, maxTimechartBuckets)
	logical, err := Build(
		mustParse(t, `index=gradethis | timechart span=1d count`),
		scope,
	)
	if err != nil {
		t.Fatalf("Build(exact bucket limit): %v", err)
	}
	if got := logical.Operators[len(logical.Operators)-1].(*Timechart).BucketCount; got != maxTimechartBuckets {
		t.Fatalf("bucket count = %d, want %d", got, maxTimechartBuckets)
	}

	scope.Latest = scope.Latest.Add(time.Nanosecond)
	_, err = Build(
		mustParse(t, `index=gradethis | timechart span=1d count`),
		scope,
	)
	assertDiagnosticCode(t, err, "SPL_QUERY_TOO_COMPLEX")
}

func TestBuildRejectsForgedMultiUnitCalendarSpan(t *testing.T) {
	t.Parallel()

	for _, source := range []string{
		`index=gradethis | bin _time span=1d`,
		`index=gradethis | timechart span=1w count`,
	} {
		t.Run(source, func(t *testing.T) {
			t.Parallel()

			parsed := mustParse(t, source)
			switch command := parsed.Commands[0].(type) {
			case *spl.BinCommand:
				command.Span.Magnitude = 2
			case *spl.TimechartCommand:
				command.Span.Magnitude = 2
			default:
				t.Fatalf("command = %T", command)
			}
			_, err := Build(parsed, testScope([]string{"gradethis"}, nil))
			var diagnostic *Diagnostic
			if !errors.As(err, &diagnostic) ||
				diagnostic.Code != "SPL_UNSUPPORTED_CALENDAR_SPAN" ||
				!slices.Equal(diagnostic.Suggestions, []string{"span=1d"}) {
				t.Fatalf("Build error = %#v, want calendar-span diagnostic", err)
			}
		})
	}
}

func TestAnalyzeCalendarSpanContracts(t *testing.T) {
	t.Parallel()

	input := analysisField("time_input")
	output := analysisField("time_output")
	timeField := mustResolveEventAggregateField(t, "_time")
	measure := AggregateMeasure{Function: AggregateFunctionCountRows, Output: "count"}

	validQueries := []*Query{
		{Operators: []Operator{&TimeBucket{Field: input, Output: output, Span: time.Second}}},
		{SearchTimezone: "UTC", Operators: []Operator{&TimeBucket{Field: input, Output: output, Calendar: CalendarDay}}},
		{Operators: []Operator{&Timechart{Time: timeField, Measure: measure, Span: time.Minute}}},
		{SearchTimezone: "America/New_York", Operators: []Operator{&Timechart{Time: timeField, Measure: measure, Calendar: CalendarWeek}}},
	}
	for index, query := range validQueries {
		if _, err := Analyze(query); err != nil {
			t.Fatalf("Analyze(valid %d): %v", index, err)
		}
	}

	invalidQueries := []*Query{
		{Operators: []Operator{&TimeBucket{Field: input, Output: output}}},
		{SearchTimezone: "UTC", Operators: []Operator{&TimeBucket{Field: input, Output: output, Span: time.Second, Calendar: CalendarDay}}},
		{SearchTimezone: "UTC", Operators: []Operator{&TimeBucket{Field: input, Output: output, Calendar: CalendarUnit(255)}}},
		{Operators: []Operator{&TimeBucket{Field: input, Output: output, Calendar: CalendarDay}}},
		{Operators: []Operator{&Timechart{Time: timeField, Measure: measure}}},
		{SearchTimezone: "UTC", Operators: []Operator{&Timechart{Time: timeField, Measure: measure, Span: time.Minute, Calendar: CalendarWeek}}},
		{SearchTimezone: "Local", Operators: []Operator{&Timechart{Time: timeField, Measure: measure, Calendar: CalendarWeek}}},
	}
	for index, query := range invalidQueries {
		if _, err := Analyze(query); err == nil {
			t.Fatalf("Analyze(invalid %d) succeeded", index)
		}
	}
}
