package plan

import (
	"errors"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/spl"
)

func TestBuildTimechartAutomaticSpanDocumentedRanges(t *testing.T) {
	t.Parallel()
	start := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	tests := []struct {
		name        string
		latest      time.Time
		span        time.Duration
		calendar    CalendarUnit
		bucketCount uint64
	}{
		{name: "15 minutes", latest: start.Add(15 * time.Minute), span: 10 * time.Second, bucketCount: 90},
		{name: "60 minutes", latest: start.Add(time.Hour), span: time.Minute, bucketCount: 60},
		{name: "4 hours", latest: start.Add(4 * time.Hour), span: 5 * time.Minute, bucketCount: 48},
		{name: "24 hours", latest: start.Add(24 * time.Hour), span: 30 * time.Minute, bucketCount: 48},
		{name: "7 days", latest: start.AddDate(0, 0, 7), calendar: CalendarDay, bucketCount: 7},
		{name: "30 days", latest: start.AddDate(0, 0, 30), calendar: CalendarDay, bucketCount: 30},
		{name: "previous year", latest: start.AddDate(1, 0, 0), calendar: CalendarMonth, bucketCount: 12},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			scope := testScope([]string{"gradethis"}, nil)
			scope.Earliest = start
			scope.Latest = test.latest
			logical, err := Build(mustParse(t, `index=gradethis | timechart count BY message`), scope)
			if err != nil {
				t.Fatalf("Build: %v", err)
			}
			operator := logical.Operators[len(logical.Operators)-1].(*Timechart)
			if operator.Span != test.span || operator.Calendar != test.calendar || operator.BucketCount != test.bucketCount {
				t.Fatalf("timechart = span %v calendar %v buckets %d", operator.Span, operator.Calendar, operator.BucketCount)
			}
		})
	}
}

func TestBuildTimechartAutomaticOptionsUseCeilingAndMinimum(t *testing.T) {
	t.Parallel()
	start := time.Date(2025, 1, 1, 0, 0, 1, 0, time.UTC)
	tests := []struct {
		name        string
		source      string
		latest      time.Time
		span        time.Duration
		calendar    CalendarUnit
		bucketCount uint64
	}{
		{name: "unaligned bins ceiling admits exact aligned count", source: `index=gradethis | timechart bins=20 count`, latest: time.Date(2025, 1, 1, 0, 1, 40, 0, time.UTC), span: 5 * time.Second, bucketCount: 20},
		{name: "one nanosecond over ceiling chooses next threshold", source: `index=gradethis | timechart bins=20 count`, latest: time.Date(2025, 1, 1, 0, 1, 40, 1, time.UTC), span: 10 * time.Second, bucketCount: 11},
		{name: "minimum rounds up to ladder threshold", source: `index=gradethis | timechart minspan=15m count`, latest: start.Add(time.Hour), span: 30 * time.Minute, bucketCount: 3},
		{name: "multi-day minimum rounds up to month", source: `index=gradethis | timechart minspan=2d count`, latest: start.AddDate(0, 0, 7), calendar: CalendarMonth, bucketCount: 1},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			scope := testScope([]string{"gradethis"}, nil)
			scope.Earliest = start
			scope.Latest = test.latest
			logical, err := Build(mustParse(t, test.source), scope)
			if err != nil {
				t.Fatalf("Build: %v", err)
			}
			operator := logical.Operators[len(logical.Operators)-1].(*Timechart)
			if operator.Span != test.span || operator.Calendar != test.calendar || operator.BucketCount != test.bucketCount {
				t.Fatalf("timechart = span %v calendar %v buckets %d", operator.Span, operator.Calendar, operator.BucketCount)
			}
		})
	}
}

func TestBuildTimechartExplicitSpanPrecedesAutomaticOptions(t *testing.T) {
	t.Parallel()
	scope := testScope([]string{"gradethis"}, nil)
	scope.Earliest = time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	scope.Latest = scope.Earliest.Add(time.Hour)
	logical, err := Build(mustParse(t, `index=gradethis | timechart bins=1 minspan=2d span=1m sum(bytes)`), scope)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	operator := logical.Operators[len(logical.Operators)-1].(*Timechart)
	if operator.Span != time.Minute || operator.Calendar != CalendarNone || operator.BucketCount != 60 {
		t.Fatalf("timechart = %#v", operator)
	}
}

func TestBuildTimechartRejectsForgedAutomaticOptions(t *testing.T) {
	t.Parallel()
	mutations := []func(*spl.TimechartCommand){
		func(command *spl.TimechartCommand) { command.Axis.Bins = 10 },
		func(command *spl.TimechartCommand) { command.Axis.BinsSpecified = true },
		func(command *spl.TimechartCommand) {
			command.Axis.MinSpan = spl.TimeSpan{Magnitude: 1, Unit: spl.TimeSpanUnitMinute}
		},
		func(command *spl.TimechartCommand) { command.Axis.MinSpanSpecified = true },
	}
	for _, mutate := range mutations {
		query := mustParse(t, `index=gradethis | timechart span=1m count`)
		mutate(query.Commands[0].(*spl.TimechartCommand))
		_, err := Build(query, testScope([]string{"gradethis"}, nil))
		if _, ok := errors.AsType[*Diagnostic](err); !ok {
			t.Fatalf("Build error = %#v, want diagnostic", err)
		}
	}
}

func TestBuildBinStillRejectsForgedMonthSpan(t *testing.T) {
	t.Parallel()
	query := mustParse(t, `index=gradethis | bin _time span=1d`)
	query.Commands[0].(*spl.BinCommand).Span.Unit = spl.TimeSpanUnitMonth
	_, err := Build(query, testScope([]string{"gradethis"}, nil))
	var diagnostic *Diagnostic
	if !errors.As(err, &diagnostic) || diagnostic.Code != "SPL_UNSUPPORTED_BIN_SYNTAX" {
		t.Fatalf("Build error = %#v, want bin syntax diagnostic", err)
	}
}
