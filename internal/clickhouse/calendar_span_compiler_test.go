package clickhouse

import (
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/plan"
)

func TestCompileCalendarTimechartUsesCivilKeyGridAndPrivateBoundary(t *testing.T) {
	t.Parallel()

	const timezone = "America/New_York"
	scope := testChartScope()
	scope.SearchTimezone = timezone
	scope.Earliest = time.Date(2026, time.March, 7, 5, 0, 0, 0, time.UTC)
	scope.Latest = time.Date(2026, time.March, 10, 4, 0, 0, 0, time.UTC)
	scope.SearchStart = scope.Latest.Add(time.Second)
	scope.IndexTimeCutoff = scope.SearchStart

	compiled := compileSPLWithScope(
		t,
		`index=gradethis | timechart span=1d count`,
		scope,
	)
	if compiled.Timechart == nil || !compiled.Timechart.Calendar ||
		compiled.Timechart.Span != 0 ||
		compiled.Timechart.FirstBucket != time.Date(2026, time.March, 7, 5, 0, 0, 0, time.UTC) ||
		compiled.Timechart.BucketCount != 3 {
		t.Fatalf("calendar timechart metadata = %#v", compiled.Timechart)
	}
	for _, fragment := range []string{
		`toStartOfDay(toTimeZone("__os_tc_event_time", ?))`,
		`arrayJoin(arrayMap(i -> (toUInt64(i), addDays(toTimeZone(toDateTime64(?, 9, 'UTC'), ?), i)), range(?)))`,
		`AS "` + TimechartBucketColumn + `"`,
	} {
		if !strings.Contains(compiled.SQL, fragment) {
			t.Fatalf("calendar timechart SQL missing %q:\n%s", fragment, compiled.SQL)
		}
	}
	if strings.Contains(compiled.SQL, `FROM numbers(?)`) {
		t.Fatalf("calendar timechart used the fixed ordinal grid:\n%s", compiled.SQL)
	}
	wantTail := []any{
		timezone,
		"2026-03-07 05:00:00.000000000",
		timezone,
		uint64(3),
	}
	if got := compiled.Args[len(compiled.Args)-len(wantTail):]; !reflect.DeepEqual(got, wantTail) {
		t.Fatalf("calendar grid arguments = %#v, want %#v", got, wantTail)
	}
	if got, want := strings.Count(compiled.SQL, "?"), len(compiled.Args); got != want {
		t.Fatalf("placeholder count = %d, args = %d", got, want)
	}
}

func TestCompileCalendarWeekTimechartAlignsToSunday(t *testing.T) {
	t.Parallel()

	scope := testChartScope()
	scope.SearchTimezone = "UTC"
	scope.Earliest = time.Date(2026, time.December, 31, 12, 0, 0, 0, time.UTC)
	scope.Latest = time.Date(2027, time.January, 11, 0, 0, 0, 0, time.UTC)
	scope.SearchStart = scope.Latest.Add(time.Second)
	scope.IndexTimeCutoff = scope.SearchStart
	compiled := compileSPLWithScope(
		t,
		`index=gradethis | timechart span=1w count BY level`,
		scope,
	)
	if compiled.Timechart == nil || !compiled.Timechart.Calendar ||
		compiled.Timechart.FirstBucket != time.Date(2026, time.December, 27, 0, 0, 0, 0, time.UTC) ||
		compiled.Timechart.BucketCount != 3 {
		t.Fatalf("calendar week metadata = %#v", compiled.Timechart)
	}
	for _, fragment := range []string{
		`toStartOfWeek(toTimeZone("__os_tc_event_time", ?), 0)`,
		`toDateTime64(toStartOfWeek(toTimeZone("__os_tc_event_time", ?), 0), 9, ?)`,
		`addWeeks(toTimeZone(toDateTime64(?, 9, 'UTC'), ?), i)`,
	} {
		if !strings.Contains(compiled.SQL, fragment) {
			t.Fatalf("calendar week SQL missing %q:\n%s", fragment, compiled.SQL)
		}
	}
}

func TestCompileCalendarTimechartCoversEveryTransportMode(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		spl  string
		mode TimechartMode
	}{
		{name: "fixed count", spl: `index=gradethis | timechart span=1d count`, mode: TimechartModeFixedCount},
		{name: "fixed field count", spl: `index=gradethis | timechart span=1d count(status)`, mode: TimechartModeFixedFieldCount},
		{name: "fixed value", spl: `index=gradethis | timechart span=1d sum(status)`, mode: TimechartModeFixedValue},
		{name: "wide count", spl: `index=gradethis | timechart span=1d count BY level`, mode: TimechartModeRuntimeWide},
		{name: "wide value", spl: `index=gradethis | timechart span=1d avg(status) BY level`, mode: TimechartModeRuntimeWideValue},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			compiled := compileSPL(t, test.spl)
			if compiled.Timechart == nil || !compiled.Timechart.Calendar ||
				compiled.Timechart.Mode != test.mode ||
				compiled.Timechart.Span != 0 {
				t.Fatalf("calendar transport = %#v, want mode %v", compiled.Timechart, test.mode)
			}
			if got := strings.Count(compiled.SQL, `AS "`+TimechartBucketColumn+`"`); got != 1 {
				t.Fatalf("private calendar boundary projections = %d, want 1:\n%s", got, compiled.SQL)
			}
			if got, want := strings.Count(compiled.SQL, "?"), len(compiled.Args); got != want {
				t.Fatalf("placeholder count = %d, args = %d", got, want)
			}
		})
	}
}

func TestCompileCalendarBinUsesTimezoneAwareBoundary(t *testing.T) {
	t.Parallel()

	scope := testChartScope()
	scope.SearchTimezone = "America/New_York"
	compiled := compileSPLWithScope(
		t,
		`index=gradethis | bin _time span=1w`,
		scope,
	)
	for _, fragment := range []string{
		`toStartOfWeek(toTimeZone("_time", ?), 0)`,
		`toDateTime64(toStartOfWeek(toTimeZone("_time", ?), 0), 9, ?)`,
	} {
		if !strings.Contains(compiled.SQL, fragment) {
			t.Fatalf("calendar bin SQL missing %q:\n%s", fragment, compiled.SQL)
		}
	}
	if countArgument(compiled.Args, scope.SearchTimezone) != 2 {
		t.Fatalf("calendar bin timezone args = %#v", compiled.Args)
	}
}

func TestCompileFixed24HourBinRemainsDistinctFromCalendarDay(t *testing.T) {
	t.Parallel()

	scope := testChartScope()
	scope.SearchTimezone = "America/New_York"
	scope.Earliest = time.Date(2026, time.March, 8, 5, 0, 0, 0, time.UTC)
	scope.Latest = time.Date(2026, time.March, 10, 4, 0, 0, 0, time.UTC)
	scope.SearchStart = scope.Latest.Add(time.Second)
	scope.IndexTimeCutoff = scope.SearchStart

	fixed := compileSPLWithScope(t, `index=gradethis | bin _time span=24h`, scope)
	calendar := compileSPLWithScope(t, `index=gradethis | bin _time span=1d`, scope)
	if !strings.Contains(fixed.SQL, `fromUnixTimestamp64Nano(`) ||
		strings.Contains(fixed.SQL, `toStartOfDay(`) ||
		countArgument(fixed.Args, int64(24*time.Hour)) != 3 {
		t.Fatalf("24h bin did not retain the fixed epoch path:\n%s\nargs: %#v", fixed.SQL, fixed.Args)
	}
	if !strings.Contains(calendar.SQL, `toStartOfDay(toTimeZone("_time", ?))`) ||
		strings.Contains(calendar.SQL, `fromUnixTimestamp64Nano(`) {
		t.Fatalf("1d bin did not retain the civil-time path:\n%s", calendar.SQL)
	}

	fixedTimechart := compileSPLWithScope(
		t,
		`index=gradethis | timechart span=24h count`,
		scope,
	)
	calendarTimechart := compileSPLWithScope(
		t,
		`index=gradethis | timechart span=1d count`,
		scope,
	)
	if fixedTimechart.Timechart == nil || fixedTimechart.Timechart.Calendar ||
		fixedTimechart.Timechart.Span != 24*time.Hour ||
		strings.Contains(fixedTimechart.SQL, TimechartBucketColumn) ||
		!strings.Contains(fixedTimechart.SQL, `intDiv("__os_tc_ticks", ?)`) {
		t.Fatalf("24h timechart did not retain fixed metadata/SQL: %#v\n%s", fixedTimechart.Timechart, fixedTimechart.SQL)
	}
	if calendarTimechart.Timechart == nil || !calendarTimechart.Timechart.Calendar ||
		calendarTimechart.Timechart.Span != 0 ||
		!strings.Contains(calendarTimechart.SQL, TimechartBucketColumn) ||
		!strings.Contains(calendarTimechart.SQL, `toStartOfDay(`) {
		t.Fatalf("1d timechart did not retain calendar metadata/SQL: %#v\n%s", calendarTimechart.Timechart, calendarTimechart.SQL)
	}
}

func TestCompileCalendarGridsRejectUnsupportedLowerBoundary(t *testing.T) {
	t.Parallel()

	scope := testChartScope()
	scope.SearchTimezone = "America/New_York"
	scope.Earliest = MinimumSearchTime()
	scope.Latest = scope.Earliest.Add(time.Hour)
	scope.SearchStart = scope.Latest.Add(time.Second)
	scope.IndexTimeCutoff = scope.SearchStart

	for _, source := range []string{
		`index=gradethis | bin _time span=1d`,
		`index=gradethis | timechart span=1d count`,
	} {
		logical := buildPlanWithScope(t, source, scope)
		_, err := (Compiler{}).Compile(logical)
		var diagnostic *plan.Diagnostic
		if !errors.As(err, &diagnostic) ||
			(diagnostic.Code != "SPL_UNSUPPORTED_BIN_TIME_RANGE" &&
				diagnostic.Code != "SPL_UNSUPPORTED_TIMECHART_TIME_RANGE") {
			t.Fatalf("Compile(%q) error = %#v", source, err)
		}
	}
}

func TestFixedTimechartDoesNotCarryCalendarTransport(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(t, `index=gradethis | timechart span=5m count`)
	if compiled.Timechart == nil || compiled.Timechart.Calendar ||
		strings.Contains(compiled.SQL, TimechartBucketColumn) ||
		strings.Contains(compiled.SQL, "arrayJoin(arrayMap(i ->") {
		t.Fatalf("fixed timechart gained calendar transport:\n%s", compiled.SQL)
	}
	clone, ok := compiled.CloneForExecution()
	if !ok || clone.Timechart == nil {
		t.Fatal("fixed timechart execution contract did not clone")
	}
	clone.Timechart.Calendar = true
	if clone.HasValidExecutionSeal() || !compiled.HasValidExecutionSeal() {
		t.Fatal("fixed-to-calendar discriminator tamper did not invalidate only the clone")
	}
}

func TestCompiledCalendarTimechartIsSealedClonedAndRetained(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(
		t,
		`index=gradethis | timechart span=1d avg(status) BY level`,
	)
	clone, ok := compiled.CloneForExecution()
	retained, retainedOK := compiled.RetainedBytes()
	cloneRetained, cloneRetainedOK := clone.RetainedBytes()
	if !ok || !retainedOK || retained <= uint64(len(compiled.SQL)) ||
		!cloneRetainedOK || cloneRetained != retained ||
		!compiled.HasValidExecutionSeal() || !clone.HasValidExecutionSeal() ||
		!compiled.EqualForExecution(clone) || clone.Timechart == compiled.Timechart ||
		clone.Timechart == nil || !clone.Timechart.Calendar || clone.Timechart.Span != 0 {
		t.Fatalf(
			"calendar execution contract clone=%t retained=(%d,%t)/(%d,%t) original=%#v clone=%#v",
			ok,
			retained,
			retainedOK,
			cloneRetained,
			cloneRetainedOK,
			compiled.Timechart,
			clone.Timechart,
		)
	}
	clone.Timechart.Calendar = false
	if clone.HasValidExecutionSeal() || compiled.EqualForExecution(clone) ||
		!compiled.HasValidExecutionSeal() {
		t.Fatal("calendar discriminator tamper did not invalidate only the detached clone")
	}
}
