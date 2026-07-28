package clickhouse

import (
	"context"
	"math"
	"strconv"
	"strings"
	"testing"
	"time"

	clickhousedriver "github.com/ClickHouse/clickhouse-go/v2"
)

func testRelativeTimeAgainstClickHouse(
	ctx context.Context,
	t *testing.T,
	store *Store,
	connection clickhousedriver.Conn,
	indexTime time.Time,
) {
	t.Helper()

	location := mustLocation(t, "America/Los_Angeles")
	baseTime := time.Date(2024, 7, 17, 12, 34, 56, 125_000_000, location)
	event := testStoredEvent("relative-time-scalars", "relative_time", indexTime)
	eventTime := event.Event.EventTime.AsTime()
	event.Event.Fields = typedObjectValue(
		typedField("dynamic_double", typedDouble(relativeTimeUnixSeconds(baseTime))),
		typedField("dynamic_signed", typedSint(baseTime.Unix())),
		typedField("dynamic_unsigned", typedUint(uint64(baseTime.Unix()))),
		typedField("dynamic_maximum", typedSint(MaximumSearchTime().Unix())),
		typedField("dynamic_decimal", typedDecimal(relativeTimeLiteral(baseTime))),
		typedField(
			"dynamic_oversized_decimal",
			typedDecimal(
				strings.Repeat(
					"9",
					MaximumUnixTimestampDynamicDecimalBytes+1,
				),
			),
		),
		typedField("dynamic_text", typedString(relativeTimeLiteral(baseTime))),
		typedField("dynamic_flag", typedBool(true)),
		typedField("dynamic_null", typedNull()),
		typedField(
			"dynamic_list",
			typedList(typedDouble(relativeTimeUnixSeconds(baseTime))),
		),
		typedField(
			"dynamic_object",
			typedObject(typedField("value", typedDouble(relativeTimeUnixSeconds(baseTime)))),
		),
		typedField("dynamic_timestamp", typedTimestamp(baseTime)),
	)
	compileUTC, queryContext := storeScalarFunctionIntegrationFixtures(
		ctx,
		t,
		store,
		indexTime,
		"relative_time",
		"relative-time-batch",
		117,
		event,
	)
	visibilityCutoff := mustVisibilityCutoff(t, ctx, store)
	compile := func(source, timezone string) CompiledQuery {
		t.Helper()
		logical := buildIntegrationPlanForIndex(
			t,
			source,
			indexTime.Add(10*time.Second),
			visibilityCutoff,
			"relative_time",
		)
		logical.SearchTimezone = timezone
		compiled, err := (Compiler{}).Compile(logical)
		if err != nil {
			t.Fatalf("compile relative_time integration SPL %q: %v", source, err)
		}
		return compiled
	}
	baseSearch := `index=relative_time event_id="relative-time-scalars"`

	t.Run("fractional offsets and explicit snap", func(t *testing.T) {
		query := compile(
			baseSearch+
				` | eval unchanged=relative_time(_time, "+0s"),`+
				` shifted=relative_time(_time, "+1s"),`+
				` snapped=relative_time(_time, "@s"),`+
				` elapsed=relative_time(_time, "-1h"),`+
				` elapsed_snapped=relative_time(_time, "-1h@h")`+
				` | table unchanged,shifted,snapped,elapsed,elapsed_snapped`,
			"America/Los_Angeles",
		)
		got := queryRelativeTimeFloats(t, queryContext, connection, query, 5)
		elapsed := eventTime.Add(-time.Hour)
		elapsedLocal := elapsed.In(location)
		want := []time.Time{
			eventTime,
			eventTime.Add(time.Second),
			eventTime.Truncate(time.Second),
			elapsed,
			time.Date(
				elapsedLocal.Year(),
				elapsedLocal.Month(),
				elapsedLocal.Day(),
				elapsedLocal.Hour(),
				0,
				0,
				0,
				location,
			),
		}
		for index, instant := range want {
			assertRelativeTimeEpochPointer(t, "fractional result "+strconv.Itoa(index), got[index], instant)
		}
	})

	t.Run("all snap units and weekdays", func(t *testing.T) {
		type snapCase struct {
			name      string
			specifier string
			want      time.Time
		}
		year, month, day := baseTime.Date()
		hour, minute, second := baseTime.Clock()
		quarterMonth := time.Month((int(month)-1)/3*3 + 1)
		cases := []snapCase{
			{
				name:      "snap_second",
				specifier: "@s",
				want: time.Date(
					year, month, day, hour, minute, second, 0, location,
				),
			},
			{
				name:      "snap_minute",
				specifier: "@m",
				want:      time.Date(year, month, day, hour, minute, 0, 0, location),
			},
			{
				name:      "snap_hour",
				specifier: "@h",
				want:      time.Date(year, month, day, hour, 0, 0, 0, location),
			},
			{
				name:      "snap_day",
				specifier: "@d",
				want:      time.Date(year, month, day, 0, 0, 0, 0, location),
			},
			{
				name:      "snap_week",
				specifier: "@w",
				want:      relativeTimeWeekBoundary(baseTime, time.Sunday),
			},
			{
				name:      "snap_week_alias",
				specifier: "@week",
				want:      relativeTimeWeekBoundary(baseTime, time.Sunday),
			},
		}
		for weekday := 0; weekday <= 7; weekday++ {
			normalized := time.Weekday(weekday % 7)
			cases = append(cases, snapCase{
				name:      "snap_w" + strconv.Itoa(weekday),
				specifier: "@w" + strconv.Itoa(weekday),
				want:      relativeTimeWeekBoundary(baseTime, normalized),
			})
		}
		cases = append(
			cases,
			snapCase{
				name:      "snap_month",
				specifier: "@mon",
				want:      time.Date(year, month, 1, 0, 0, 0, 0, location),
			},
			snapCase{
				name:      "snap_quarter",
				specifier: "@q",
				want:      time.Date(year, quarterMonth, 1, 0, 0, 0, 0, location),
			},
			snapCase{
				name:      "snap_year",
				specifier: "@y",
				want:      time.Date(year, time.January, 1, 0, 0, 0, 0, location),
			},
		)

		assignments := make([]string, 0, len(cases))
		fields := make([]string, 0, len(cases))
		for _, test := range cases {
			assignments = append(
				assignments,
				test.name+`=relative_time(`+relativeTimeLiteral(baseTime)+`, "`+
					test.specifier+`")`,
			)
			fields = append(fields, test.name)
		}
		query := compile(
			baseSearch+` | eval `+strings.Join(assignments, ",")+
				` | table `+strings.Join(fields, ","),
			"America/Los_Angeles",
		)
		got := queryRelativeTimeFloats(
			t,
			queryContext,
			connection,
			query,
			len(cases),
		)
		for index, test := range cases {
			assertRelativeTimeEpochPointer(t, test.name, got[index], test.want)
		}
	})

	t.Run("ordinary offsets cover every unit", func(t *testing.T) {
		type offsetCase struct {
			name      string
			specifier string
			want      time.Time
		}
		cases := []offsetCase{
			{name: "second", specifier: "+1s", want: baseTime.Add(time.Second)},
			{name: "minute", specifier: "+1m", want: baseTime.Add(time.Minute)},
			{name: "hour", specifier: "+1h", want: baseTime.Add(time.Hour)},
			{name: "day", specifier: "+1d", want: baseTime.AddDate(0, 0, 1)},
			{name: "week", specifier: "+1w", want: baseTime.AddDate(0, 0, 7)},
			{name: "month", specifier: "+1mon", want: baseTime.AddDate(0, 1, 0)},
			{name: "quarter", specifier: "+1q", want: baseTime.AddDate(0, 3, 0)},
			{name: "year", specifier: "+1y", want: baseTime.AddDate(1, 0, 0)},
		}
		assignments := make([]string, 0, len(cases))
		fields := make([]string, 0, len(cases))
		for _, test := range cases {
			assignments = append(
				assignments,
				test.name+`=relative_time(`+relativeTimeLiteral(baseTime)+
					`, "`+test.specifier+`")`,
			)
			fields = append(fields, test.name)
		}
		query := compile(
			baseSearch+` | eval `+strings.Join(assignments, ",")+
				` | table `+strings.Join(fields, ","),
			"America/Los_Angeles",
		)
		results := queryRelativeTimeFloats(
			t,
			queryContext,
			connection,
			query,
			len(cases),
		)
		for index, test := range cases {
			assertRelativeTimeEpochPointer(
				t,
				"ordinary "+test.name+" offset",
				results[index],
				test.want,
			)
		}
	})

	t.Run("subday snaps honor historical and folded timezone offsets", func(t *testing.T) {
		historical := time.Date(
			1900,
			time.January,
			1,
			0,
			34,
			56,
			987_654_321,
			time.UTC,
		)
		amsterdam := compile(
			baseSearch+
				` | eval second=relative_time(`+
				relativeTimeLiteral(historical)+`, "@s"),`+
				` minute=relative_time(`+
				relativeTimeLiteral(historical)+`, "@m"),`+
				` hour=relative_time(`+
				relativeTimeLiteral(historical)+`, "@h")`+
				` | table second,minute,hour`,
			"Europe/Amsterdam",
		)
		amsterdamResults := queryRelativeTimeFloats(
			t,
			queryContext,
			connection,
			amsterdam,
			3,
		)
		assertRelativeTimeEpochPointer(
			t,
			"historical Amsterdam second",
			amsterdamResults[0],
			time.Date(1900, time.January, 1, 0, 34, 56, 0, time.UTC),
		)
		assertRelativeTimeEpochPointer(
			t,
			"historical Amsterdam minute",
			amsterdamResults[1],
			time.Date(1900, time.January, 1, 0, 34, 28, 0, time.UTC),
		)
		if amsterdamResults[2] != nil {
			t.Fatalf(
				"historical Amsterdam hour = %.9f, want policy null",
				*amsterdamResults[2],
			)
		}

		firstFold := time.Date(
			2024,
			time.November,
			3,
			8,
			30,
			0,
			0,
			time.UTC,
		)
		secondFold := firstFold.Add(time.Hour)
		folds := compile(
			baseSearch+
				` | eval first=relative_time(`+
				relativeTimeLiteral(firstFold)+`, "@h"),`+
				` second=relative_time(`+
				relativeTimeLiteral(secondFold)+`, "@h")`+
				` | table first,second`,
			"America/Los_Angeles",
		)
		foldResults := queryRelativeTimeFloats(
			t,
			queryContext,
			connection,
			folds,
			2,
		)
		assertRelativeTimeEpochPointer(
			t,
			"first fold hour",
			foldResults[0],
			time.Date(2024, time.November, 3, 8, 0, 0, 0, time.UTC),
		)
		assertRelativeTimeEpochPointer(
			t,
			"second fold hour",
			foldResults[1],
			time.Date(2024, time.November, 3, 9, 0, 0, 0, time.UTC),
		)

		kathmanduLocation := mustLocation(t, "Asia/Kathmandu")
		kathmanduInput := time.Date(
			2024,
			time.July,
			17,
			12,
			34,
			56,
			125_000_000,
			time.UTC,
		)
		kathmanduLocal := kathmanduInput.In(kathmanduLocation)
		kathmandu := compile(
			baseSearch+
				` | eval hour=relative_time(`+
				relativeTimeLiteral(kathmanduInput)+`, "@h")`+
				` | table hour`,
			"Asia/Kathmandu",
		)
		kathmanduResult := queryRelativeTimeFloats(
			t,
			queryContext,
			connection,
			kathmandu,
			1,
		)
		assertRelativeTimeEpochPointer(
			t,
			"Kathmandu hour",
			kathmanduResult[0],
			time.Date(
				kathmanduLocal.Year(),
				kathmanduLocal.Month(),
				kathmanduLocal.Day(),
				kathmanduLocal.Hour(),
				0,
				0,
				0,
				kathmanduLocation,
			),
		)
	})

	t.Run("calendar arithmetic operation order and DST", func(t *testing.T) {
		spring := time.Date(2024, 3, 10, 12, 30, 15, 125_000_000, location)
		fall := time.Date(2024, 11, 3, 12, 30, 15, 125_000_000, location)
		januaryEnd := time.Date(2024, 1, 31, 12, 30, 15, 125_000_000, location)
		quarterEnd := time.Date(2023, 11, 30, 12, 30, 15, 125_000_000, location)
		leapDay := time.Date(2024, 2, 29, 12, 30, 15, 125_000_000, location)
		gapSource := time.Date(2024, 3, 9, 2, 30, 0, 125_000_000, location)
		foldSource := time.Date(2024, 11, 2, 1, 30, 0, 125_000_000, location)

		query := compile(
			baseSearch+
				` | eval spring_day=relative_time(`+relativeTimeLiteral(spring)+`, "-1d"),`+
				` spring_hours=relative_time(`+relativeTimeLiteral(spring)+`, "-24h"),`+
				` fall_day=relative_time(`+relativeTimeLiteral(fall)+`, "-1d"),`+
				` fall_hours=relative_time(`+relativeTimeLiteral(fall)+`, "-24h"),`+
				` month_end=relative_time(`+relativeTimeLiteral(januaryEnd)+`, "+1mon"),`+
				` quarter_end=relative_time(`+relativeTimeLiteral(quarterEnd)+`, "+1q"),`+
				` leap_year=relative_time(`+relativeTimeLiteral(leapDay)+`, "+1y"),`+
				` ordered=relative_time(`+relativeTimeLiteral(baseTime)+`, "-1mon@mon+7d"),`+
				` gap_day=relative_time(`+relativeTimeLiteral(gapSource)+`, "+1d"),`+
				` fold_day=relative_time(`+relativeTimeLiteral(foldSource)+`, "+1d"),`+
				` snap_post=relative_time(`+relativeTimeLiteral(spring)+`, "@d+2h")`+
				` | table spring_day,spring_hours,fall_day,fall_hours,month_end,quarter_end,leap_year,ordered,gap_day,fold_day,snap_post`,
			"America/Los_Angeles",
		)
		got := queryRelativeTimeFloats(t, queryContext, connection, query, 11)
		want := []time.Time{
			time.Date(2024, 3, 9, 12, 30, 15, 125_000_000, location),
			spring.Add(-24 * time.Hour),
			time.Date(2024, 11, 2, 12, 30, 15, 125_000_000, location),
			fall.Add(-24 * time.Hour),
			time.Date(2024, 2, 29, 12, 30, 15, 125_000_000, location),
			time.Date(2024, 2, 29, 12, 30, 15, 125_000_000, location),
			time.Date(2025, 2, 28, 12, 30, 15, 125_000_000, location),
			time.Date(2024, 6, 8, 0, 0, 0, 0, location),
			time.Date(2024, 3, 10, 1, 30, 0, 125_000_000, location),
			time.Date(2024, 11, 3, 8, 30, 0, 125_000_000, time.UTC),
			time.Date(2024, 3, 10, 10, 0, 0, 0, time.UTC),
		}
		for index, instant := range want {
			assertRelativeTimeEpochPointer(t, "calendar result "+strconv.Itoa(index), got[index], instant)
		}
		if got[0] == nil || got[1] == nil || math.Abs((*got[0]-*got[1])-3600) > 0.000001 {
			t.Fatalf("spring calendar day and 24 hours did not differ by one hour: %#v/%#v", got[0], got[1])
		}
		if got[2] == nil || got[3] == nil || math.Abs((*got[3]-*got[2])-3600) > 0.000001 {
			t.Fatalf("fall calendar day and 24 hours did not differ by one hour: %#v/%#v", got[2], got[3])
		}
	})

	t.Run("Dynamic numeric and unsupported runtime types", func(t *testing.T) {
		query := compileUTC(
			baseSearch +
				` | eval double_result=relative_time(dynamic_double, "+1s"),` +
				` signed_result=relative_time(dynamic_signed, "+1s"),` +
				` unsigned_result=relative_time(dynamic_unsigned, "+1s"),` +
				` maximum_result=relative_time(dynamic_maximum, "+0s"),` +
				` decimal_result=relative_time(dynamic_decimal, "+1s"),` +
				` oversized_decimal_result=relative_time(dynamic_oversized_decimal, "+1s"),` +
				` text_result=relative_time(dynamic_text, "+1s"),` +
				` flag_result=relative_time(dynamic_flag, "+1s"),` +
				` null_result=relative_time(dynamic_null, "+1s"),` +
				` missing_result=relative_time(absent, "+1s"),` +
				` list_result=relative_time(dynamic_list, "+1s"),` +
				` object_result=relative_time(dynamic_object, "+1s"),` +
				` timestamp_result=relative_time(dynamic_timestamp, "+1s")` +
				` | table double_result,signed_result,unsigned_result,maximum_result,decimal_result,oversized_decimal_result,text_result,flag_result,null_result,missing_result,list_result,object_result,timestamp_result`,
		)
		got := queryRelativeTimeFloats(t, queryContext, connection, query, 13)
		assertRelativeTimeEpochPointer(
			t,
			"Dynamic Float64",
			got[0],
			baseTime.Add(time.Second),
		)
		assertRelativeTimeEpochPointer(
			t,
			"Dynamic Int64",
			got[1],
			time.Unix(baseTime.Unix()+1, 0),
		)
		assertRelativeTimeEpochPointer(
			t,
			"Dynamic UInt64",
			got[2],
			time.Unix(baseTime.Unix()+1, 0),
		)
		assertRelativeTimeEpochPointer(
			t,
			"Dynamic Int64 maximum",
			got[3],
			MaximumSearchTime(),
		)
		assertRelativeTimeEpochPointer(
			t,
			"Dynamic tagged decimal",
			got[4],
			baseTime.Add(time.Second),
		)
		for index, result := range got[5:] {
			if result != nil {
				t.Fatalf(
					"unsupported Dynamic relative_time result %d = %.9f, want null",
					index,
					*result,
				)
			}
		}
	})

	t.Run("policy boundaries", func(t *testing.T) {
		minimum := MinimumSearchTime()
		maximum := MaximumSearchTime()
		lowerFraction := time.Unix(
			minimum.Unix()+34*60+56,
			987_654_321,
		).UTC()
		minimumSeconds := strconv.FormatInt(minimum.Unix(), 10)
		maximumSeconds := strconv.FormatInt(maximum.Unix(), 10)
		query := compile(
			baseSearch+
				` | eval minimum_ok=relative_time(`+minimumSeconds+`, "+0s"),`+
				` maximum_ok=relative_time(`+maximumSeconds+`, "+0s"),`+
				` below_minimum=relative_time(`+
				strconv.FormatInt(minimum.Unix()-1, 10)+`, "+0s"),`+
				` above_maximum=relative_time(`+
				strconv.FormatInt(maximum.Unix()+1, 10)+`, "+0s"),`+
				` minimum_backward=relative_time(`+minimumSeconds+`, "-1s"),`+
				` maximum_forward=relative_time(`+maximumSeconds+`, "+1s"),`+
				` minimum_local_hour=relative_time(`+minimumSeconds+`, "@h"),`+
				` minimum_local_day=relative_time(`+minimumSeconds+`, "@d"),`+
				` minimum_day_forward=relative_time(`+minimumSeconds+`, "+1d"),`+
				` lower_fraction_second=relative_time(`+
				relativeTimeLiteral(lowerFraction)+`, "@s"),`+
				` lower_fraction_minute=relative_time(`+
				relativeTimeLiteral(lowerFraction)+`, "@m"),`+
				` lower_fraction_hour=relative_time(`+
				relativeTimeLiteral(lowerFraction)+`, "@h"),`+
				` below_minimum_fraction=relative_time(`+
				relativeTimeLiteral(minimum.Add(-500*time.Millisecond))+`, "+0s"),`+
				` above_maximum_fraction=relative_time(`+
				relativeTimeLiteral(maximum.Add(500*time.Millisecond))+`, "+0s"),`+
				` inside_minimum_fraction=relative_time(`+
				relativeTimeLiteral(minimum.Add(500*time.Millisecond))+`, "+0s"),`+
				` inside_maximum_fraction=relative_time(`+
				relativeTimeLiteral(maximum.Add(-500*time.Millisecond))+`, "+0s")`+
				` | table minimum_ok,maximum_ok,below_minimum,above_maximum,minimum_backward,maximum_forward,minimum_local_hour,minimum_local_day,minimum_day_forward,lower_fraction_second,lower_fraction_minute,lower_fraction_hour,below_minimum_fraction,above_maximum_fraction,inside_minimum_fraction,inside_maximum_fraction`,
			"America/Los_Angeles",
		)
		got := queryRelativeTimeFloats(t, queryContext, connection, query, 16)
		assertRelativeTimeEpochPointer(t, "minimum boundary", got[0], minimum)
		assertRelativeTimeEpochPointer(t, "maximum boundary", got[1], maximum)
		assertRelativeTimeEpochPointer(t, "minimum local hour", got[6], minimum)
		for _, index := range []int{2, 3, 4, 5, 7, 8, 12, 13} {
			result := got[index]
			if result != nil {
				t.Fatalf(
					"out-of-policy relative_time result %d = %.9f, want null",
					index,
					*result,
				)
			}
		}
		assertRelativeTimeEpochPointer(
			t,
			"lower fractional second",
			got[9],
			time.Unix(lowerFraction.Unix(), 0),
		)
		assertRelativeTimeEpochPointer(
			t,
			"lower fractional minute",
			got[10],
			time.Date(1900, time.January, 1, 0, 34, 0, 0, time.UTC),
		)
		assertRelativeTimeEpochPointer(
			t,
			"lower fractional hour",
			got[11],
			minimum,
		)
		assertRelativeTimeEpochPointer(
			t,
			"inside minimum fraction",
			got[14],
			minimum.Add(500*time.Millisecond),
		)
		assertRelativeTimeEpochPointer(
			t,
			"inside maximum fraction",
			got[15],
			maximum.Add(-500*time.Millisecond),
		)
	})

	t.Run("calendar lower bound and intermediate excursions fail closed", func(t *testing.T) {
		minimumSeconds := strconv.FormatInt(MinimumSearchTime().Unix(), 10)
		maximumSeconds := strconv.FormatInt(MaximumSearchTime().Unix(), 10)
		query := compile(
			baseSearch+
				` | eval snap_week=relative_time(`+minimumSeconds+`, "@w"),`+
				` snap_month=relative_time(`+minimumSeconds+`, "@mon"),`+
				` snap_quarter=relative_time(`+minimumSeconds+`, "@q"),`+
				` snap_year=relative_time(`+minimumSeconds+`, "@y"),`+
				` offset_week=relative_time(`+minimumSeconds+`, "+1w"),`+
				` offset_month=relative_time(`+minimumSeconds+`, "+1mon"),`+
				` offset_quarter=relative_time(`+minimumSeconds+`, "+1q"),`+
				` offset_year=relative_time(`+minimumSeconds+`, "+1y"),`+
				` lower_excursion=relative_time(`+
				minimumSeconds+`, "-1s@s+1s"),`+
				` upper_excursion=relative_time(`+
				maximumSeconds+`, "+1s@s-1s")`+
				` | table snap_week,snap_month,snap_quarter,snap_year,offset_week,offset_month,offset_quarter,offset_year,lower_excursion,upper_excursion`,
			"America/Los_Angeles",
		)
		results := queryRelativeTimeFloats(
			t,
			queryContext,
			connection,
			query,
			10,
		)
		for index, result := range results {
			if result != nil {
				t.Fatalf(
					"lower-bound calendar guard result %d = %.9f, want null",
					index,
					*result,
				)
			}
		}
	})

	t.Run("historical IANA local boundary agrees across runtimes", func(t *testing.T) {
		dublin := mustLocation(t, "Europe/Dublin")
		localBoundary := time.Date(
			1900,
			time.January,
			1,
			0,
			0,
			0,
			0,
			dublin,
		)
		query := compile(
			baseSearch+
				` | eval at_boundary=relative_time(`+
				relativeTimeLiteral(localBoundary)+`, "+1d"),`+
				` before_boundary=relative_time(`+
				relativeTimeLiteral(localBoundary.Add(-time.Second))+`, "+1d"),`+
				` snapped=relative_time(`+
				relativeTimeLiteral(localBoundary)+`, "@d")`+
				` | table at_boundary,before_boundary,snapped`,
			"Europe/Dublin",
		)
		results := queryRelativeTimeFloats(
			t,
			queryContext,
			connection,
			query,
			3,
		)
		assertRelativeTimeEpochPointer(
			t,
			"Dublin local-boundary day",
			results[0],
			localBoundary.AddDate(0, 0, 1),
		)
		if results[1] != nil {
			t.Fatalf(
				"Dublin pre-local-boundary day = %.9f, want null",
				*results[1],
			)
		}
		assertRelativeTimeEpochPointer(
			t,
			"Dublin local-boundary snap",
			results[2],
			localBoundary,
		)
	})

	t.Run("skipped civil date fails closed", func(t *testing.T) {
		apia := mustLocation(t, "Pacific/Apia")
		beforeSkippedDate := time.Date(2011, 12, 29, 12, 0, 0, 0, apia)
		query := compile(
			baseSearch+
				` | eval skipped=relative_time(`+
				relativeTimeLiteral(beforeSkippedDate)+`, "+1d")`+
				` | table skipped`,
			"Pacific/Apia",
		)
		got := queryRelativeTimeFloats(t, queryContext, connection, query, 1)
		if got[0] != nil {
			t.Fatalf(
				"Pacific/Apia skipped-date relative_time = %.9f, want null",
				*got[0],
			)
		}
	})

	t.Run("where conditionals nested time functions stats and later eval", func(t *testing.T) {
		baseWholeSeconds := strconv.FormatInt(baseTime.Unix(), 10)
		query := compile(
			baseSearch+
				` | where relative_time(dynamic_double, "@s")=`+baseWholeSeconds+
				` | eval conditional=if(relative_time(dynamic_double, "+1s")>dynamic_double, relative_time(dynamic_double, "+1s"), null),`+
				` branched=case(relative_time(dynamic_double, "@m")<=dynamic_double, relative_time(dynamic_double, "@m"), 1=1, null),`+
				` parsed=relative_time(strptime("2024-07-17 12:34:56.125", "%F %T.%Q"), "+1d"),`+
				` rendered=strftime(relative_time(dynamic_double, "@d"), "%F %T.%3N"),`+
				` anchored=relative_time(now(), "@h")`+
				` | table conditional,branched,parsed,rendered,anchored`+
				` | stats count BY conditional,branched,parsed,rendered,anchored`+
				` | eval later=relative_time(conditional, "+1s")`+
				` | where later>conditional`+
				` | table conditional,branched,parsed,rendered,anchored,count,later`,
			"America/Los_Angeles",
		)
		var conditional, branched, parsed, anchored, later float64
		var rendered string
		var count uint64
		if err := connection.QueryRow(
			queryContext,
			query.SQL,
			query.Args...,
		).Scan(
			&conditional,
			&branched,
			&parsed,
			&rendered,
			&anchored,
			&count,
			&later,
		); err != nil {
			t.Fatalf(
				"execute composed relative_time: %v\nSQL: %s\nargs: %#v",
				err,
				query.SQL,
				query.Args,
			)
		}
		searchStart := indexTime.Add(9 * time.Second).In(location)
		wantAnchored := time.Date(
			searchStart.Year(),
			searchStart.Month(),
			searchStart.Day(),
			searchStart.Hour(),
			0,
			0,
			0,
			location,
		)
		assertRelativeTimeEpoch(t, "conditional", conditional, baseTime.Add(time.Second))
		assertRelativeTimeEpoch(
			t,
			"branched",
			branched,
			time.Date(2024, 7, 17, 12, 34, 0, 0, location),
		)
		assertRelativeTimeEpoch(
			t,
			"parsed",
			parsed,
			time.Date(2024, 7, 18, 12, 34, 56, 125_000_000, location),
		)
		if rendered != "2024-07-17 00:00:00.000" {
			t.Fatalf("nested strftime(relative_time) = %q", rendered)
		}
		assertRelativeTimeEpoch(t, "anchored now", anchored, wantAnchored)
		if count != 1 {
			t.Fatalf("grouped relative_time count = %d, want 1", count)
		}
		assertRelativeTimeEpoch(t, "later eval", later, baseTime.Add(2*time.Second))
	})
}

func queryRelativeTimeFloats(
	t *testing.T,
	ctx context.Context,
	connection clickhousedriver.Conn,
	query CompiledQuery,
	count int,
) []*float64 {
	t.Helper()
	values := make([]*float64, count)
	destinations := make([]any, count)
	for index := range values {
		destinations[index] = &values[index]
	}
	if err := connection.QueryRow(ctx, query.SQL, query.Args...).Scan(destinations...); err != nil {
		t.Fatalf(
			"execute relative_time matrix: %v\nSQL: %s\nargs: %#v",
			err,
			query.SQL,
			query.Args,
		)
	}
	return values
}

func assertRelativeTimeEpochPointer(
	t *testing.T,
	name string,
	got *float64,
	want time.Time,
) {
	t.Helper()
	if got == nil {
		t.Fatalf("%s relative_time returned null, want %s", name, want)
	}
	assertRelativeTimeEpoch(t, name, *got, want)
}

func assertRelativeTimeEpoch(
	t *testing.T,
	name string,
	got float64,
	want time.Time,
) {
	t.Helper()
	wantEpoch := relativeTimeUnixSeconds(want)
	if math.Abs(got-wantEpoch) > 0.000002 {
		t.Fatalf("%s relative_time = %.9f, want %.9f", name, got, wantEpoch)
	}
}

func relativeTimeLiteral(value time.Time) string {
	return strconv.FormatFloat(relativeTimeUnixSeconds(value), 'f', 9, 64)
}

func relativeTimeUnixSeconds(value time.Time) float64 {
	return float64(value.Unix()) + float64(value.Nanosecond())/1_000_000_000
}

func relativeTimeWeekBoundary(value time.Time, weekday time.Weekday) time.Time {
	local := value.In(value.Location())
	daysBack := (int(local.Weekday()) - int(weekday) + 7) % 7
	return time.Date(
		local.Year(),
		local.Month(),
		local.Day()-daysBack,
		0,
		0,
		0,
		0,
		local.Location(),
	)
}
