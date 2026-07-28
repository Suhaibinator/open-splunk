package clickhouse

import (
	"context"
	"math"
	"strings"
	"testing"
	"time"

	clickhousedriver "github.com/ClickHouse/clickhouse-go/v2"
)

func testStrptimeAgainstClickHouse(
	ctx context.Context,
	t *testing.T,
	store *Store,
	connection clickhousedriver.Conn,
	indexTime time.Time,
) {
	t.Helper()

	event := testStoredEvent("strptime-scalars", "strptime", indexTime)
	event.Event.Fields = typedObjectValue(
		typedField("valid", typedString("2026-07-27 19:20:21.123456")),
		typedField("invalid_date", typedString("2025-02-29 19:20:21")),
		typedField("trailing", typedString("2026-07-27 19:20:21 trailing")),
		typedField("before_minimum", typedString("1970-12-31 23:59:59.999999")),
		typedField(
			"oversized",
			typedString("2026-07-27"+strings.Repeat(" ", int(MaximumStrptimeInputBytes))),
		),
		typedField("numeric", typedSint(20260727)),
		typedField("flag", typedBool(true)),
		typedField("nothing", typedNull()),
		typedField(
			"multi",
			typedList(typedString("2026-07-27"), typedString("2026-07-28")),
		),
		typedField(
			"object_value",
			typedObject(typedField("child", typedString("2026-07-27"))),
		),
		typedField("bytes_value", typedBytes([]byte("2026-07-27"))),
		typedField("timestamp_value", typedTimestamp(indexTime)),
	)
	compileUTC, queryContext := storeScalarFunctionIntegrationFixtures(
		ctx,
		t,
		store,
		indexTime,
		"strptime",
		"strptime-batch",
		116,
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
			"strptime",
		)
		logical.SearchTimezone = timezone
		compiled, err := (Compiler{}).Compile(logical)
		if err != nil {
			t.Fatalf("compile strptime integration SPL %q: %v", source, err)
		}
		return compiled
	}

	fixed := compileUTC(
		`index=strptime event_id="strptime-scalars"` +
			` | eval full=strptime("2026-07-27 19:20:21.123456", "%F %T.%6N"),` +
			` millis=strptime("2026-07-27 19:20:21.125", "%F %T.%Q"),` +
			` midnight=strptime("1971-01-01", "%F"),` +
			` twelve=strptime("2026-07-27 07:20:21 PM", "%F %I:%M:%S %p"),` +
			` twelve_lower=strptime("2026-07-27 07:20:21 pm", "%F %I:%M:%S %p"),` +
			` twelve_mixed=strptime("2026-07-27 07:20:21 pM", "%F %I:%M:%S %p"),` +
			` offset=strptime("2026-07-27 19:20:21.654321-0730", "%F %T.%f%z"),` +
			` unpadded=strptime("2026-7-2 3:4:5", "%Y-%m-%d %H:%M:%S"),` +
			` literal_percent=strptime("2026%07%27", "%Y%%%m%%%d"),` +
			` optional_fraction=strptime("2026-07-27 19:20:21", "%F %T.%Q")` +
			` | table full,millis,midnight,twelve,twelve_lower,twelve_mixed,offset,unpadded,literal_percent,optional_fraction`,
	)
	var full, millis, midnight, twelve, twelveLower, twelveMixed float64
	var offset, unpadded, literalPercent float64
	var optionalFraction float64
	if err := connection.QueryRow(
		queryContext,
		fixed.SQL,
		fixed.Args...,
	).Scan(
		&full,
		&millis,
		&midnight,
		&twelve,
		&twelveLower,
		&twelveMixed,
		&offset,
		&unpadded,
		&literalPercent,
		&optionalFraction,
	); err != nil {
		t.Fatalf(
			"execute fixed strptime: %v\nSQL: %s\nargs: %#v",
			err,
			fixed.SQL,
			fixed.Args,
		)
	}
	assertStrptimeEpoch(
		t,
		"full",
		full,
		time.Date(2026, 7, 27, 19, 20, 21, 123456000, time.UTC),
	)
	assertStrptimeEpoch(
		t,
		"millis",
		millis,
		time.Date(2026, 7, 27, 19, 20, 21, 125000000, time.UTC),
	)
	assertStrptimeEpoch(
		t,
		"midnight",
		midnight,
		time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC),
	)
	assertStrptimeEpoch(
		t,
		"twelve",
		twelve,
		time.Date(2026, 7, 27, 19, 20, 21, 0, time.UTC),
	)
	assertStrptimeEpoch(
		t,
		"twelve lowercase",
		twelveLower,
		time.Date(2026, 7, 27, 19, 20, 21, 0, time.UTC),
	)
	assertStrptimeEpoch(
		t,
		"twelve mixed case",
		twelveMixed,
		time.Date(2026, 7, 27, 19, 20, 21, 0, time.UTC),
	)
	assertStrptimeEpoch(
		t,
		"offset",
		offset,
		time.Date(2026, 7, 28, 2, 50, 21, 654321000, time.UTC),
	)
	assertStrptimeEpoch(
		t,
		"unpadded",
		unpadded,
		time.Date(2026, 7, 2, 3, 4, 5, 0, time.UTC),
	)
	assertStrptimeEpoch(
		t,
		"literal percent",
		literalPercent,
		time.Date(2026, 7, 27, 0, 0, 0, 0, time.UTC),
	)
	assertStrptimeEpoch(
		t,
		"optional fraction",
		optionalFraction,
		time.Date(2026, 7, 27, 19, 20, 21, 0, time.UTC),
	)

	dynamic := compileUTC(
		`index=strptime event_id="strptime-scalars"` +
			` | eval valid_result=strptime(valid, "%F %T.%6N"),` +
			` invalid_result=strptime(invalid_date, "%F %T"),` +
			` trailing_result=strptime(trailing, "%F %T"),` +
			` before_result=strptime(before_minimum, "%F %T.%6N"),` +
			` oversized_result=strptime(oversized, "%F"),` +
			` numeric_result=strptime(numeric, "%F"),` +
			` flag_result=strptime(flag, "%F"),` +
			` null_result=strptime(nothing, "%F"),` +
			` missing_result=strptime(absent, "%F"),` +
			` multi_result=strptime(multi, "%F"),` +
			` object_result=strptime(object_value, "%F"),` +
			` bytes_result=strptime(bytes_value, "%F"),` +
			` timestamp_result=strptime(timestamp_value, "%F")` +
			` | table valid_result,invalid_result,trailing_result,before_result,oversized_result,numeric_result,flag_result,null_result,missing_result,multi_result,object_result,bytes_result,timestamp_result`,
	)
	var validResult *float64
	nullResults := make([]*float64, 12)
	if err := connection.QueryRow(
		queryContext,
		dynamic.SQL,
		dynamic.Args...,
	).Scan(
		&validResult,
		&nullResults[0],
		&nullResults[1],
		&nullResults[2],
		&nullResults[3],
		&nullResults[4],
		&nullResults[5],
		&nullResults[6],
		&nullResults[7],
		&nullResults[8],
		&nullResults[9],
		&nullResults[10],
		&nullResults[11],
	); err != nil {
		t.Fatalf(
			"execute Dynamic strptime: %v\nSQL: %s\nargs: %#v",
			err,
			dynamic.SQL,
			dynamic.Args,
		)
	}
	if validResult == nil {
		t.Fatal("Dynamic String strptime returned null")
	}
	assertStrptimeEpoch(
		t,
		"Dynamic String",
		*validResult,
		time.Date(2026, 7, 27, 19, 20, 21, 123456000, time.UTC),
	)
	for index, result := range nullResults {
		if result != nil {
			t.Fatalf("Dynamic invalid result %d = %v, want null", index, *result)
		}
	}

	timezones := compile(
		`index=strptime event_id="strptime-scalars"`+
			` | eval summer=strptime("2026-07-27 12:00:00", "%F %T"),`+
			` winter=strptime("2026-01-01 12:00:00", "%F %T"),`+
			` explicit_offset=strptime("2026-07-27 12:00:00+0200", "%F %T%z"),`+
			` civil_min=strptime("1971-01-01 00:00:00+1400", "%F %T%z"),`+
			` civil_before=strptime("1970-12-31 23:30:00-1200", "%F %T%z"),`+
			` gap=strptime("2024-03-10 02:30:00", "%F %T"),`+
			` fold=strptime("2024-11-03 01:30:00", "%F %T")`+
			` | table summer,winter,explicit_offset,civil_min,civil_before,gap,fold`,
		"America/Los_Angeles",
	)
	var summer, winter, explicitOffset, civilMinimum, gap, fold float64
	var civilBefore *float64
	if err := connection.QueryRow(
		queryContext,
		timezones.SQL,
		timezones.Args...,
	).Scan(
		&summer,
		&winter,
		&explicitOffset,
		&civilMinimum,
		&civilBefore,
		&gap,
		&fold,
	); err != nil {
		t.Fatalf(
			"execute timezone strptime: %v\nSQL: %s\nargs: %#v",
			err,
			timezones.SQL,
			timezones.Args,
		)
	}
	assertStrptimeEpoch(
		t,
		"summer",
		summer,
		time.Date(2026, 7, 27, 19, 0, 0, 0, time.UTC),
	)
	assertStrptimeEpoch(
		t,
		"winter",
		winter,
		time.Date(2026, 1, 1, 20, 0, 0, 0, time.UTC),
	)
	assertStrptimeEpoch(
		t,
		"explicit offset",
		explicitOffset,
		time.Date(2026, 7, 27, 10, 0, 0, 0, time.UTC),
	)
	assertStrptimeEpoch(
		t,
		"civil minimum",
		civilMinimum,
		time.Date(1970, 12, 31, 10, 0, 0, 0, time.UTC),
	)
	if civilBefore != nil {
		t.Fatalf("pre-1971 civil date = %v, want null", *civilBefore)
	}
	// The pinned ClickHouse parser normalizes spring-forward gaps and chooses
	// the earlier occurrence of a fall-back fold. These deterministic choices
	// remain part of the compatibility contract until a live Splunk oracle
	// establishes different behavior.
	assertStrptimeEpoch(
		t,
		"DST gap",
		gap,
		time.Date(2024, 3, 10, 10, 30, 0, 0, time.UTC),
	)
	assertStrptimeEpoch(
		t,
		"DST fold",
		fold,
		time.Date(2024, 11, 3, 8, 30, 0, 0, time.UTC),
	)

	rangeBounds := compileUTC(
		`index=strptime event_id="strptime-scalars"` +
			` | eval maximum=strptime("2299-12-31 00:00:00", "%F %T"),` +
			` after_maximum=strptime("2300-01-01 00:00:00", "%F %T")` +
			` | table maximum,after_maximum`,
	)
	var maximum float64
	var afterMaximum *float64
	if err := connection.QueryRow(
		queryContext,
		rangeBounds.SQL,
		rangeBounds.Args...,
	).Scan(&maximum, &afterMaximum); err != nil {
		t.Fatalf(
			"execute strptime range bounds: %v\nSQL: %s\nargs: %#v",
			err,
			rangeBounds.SQL,
			rangeBounds.Args,
		)
	}
	assertStrptimeEpoch(
		t,
		"maximum",
		maximum,
		time.Date(2299, 12, 31, 0, 0, 0, 0, time.UTC),
	)
	if afterMaximum != nil {
		t.Fatalf("post-2299 date = %v, want null", *afterMaximum)
	}

	transformed := compileUTC(
		`index=strptime event_id="strptime-scalars"` +
			` | where strptime(valid, "%F %T.%6N")>=0` +
			` | eval first=strptime(valid, "%F %T.%6N")` +
			` | table first` +
			` | stats count BY first` +
			` | eval second=strptime("2026-07-27 19:20:21.123456", "%F %T.%6N")` +
			` | where first=second` +
			` | table first,second,count`,
	)
	var first, second float64
	var count uint64
	if err := connection.QueryRow(
		queryContext,
		transformed.SQL,
		transformed.Args...,
	).Scan(&first, &second, &count); err != nil {
		t.Fatalf(
			"execute transformed strptime: %v\nSQL: %s\nargs: %#v",
			err,
			transformed.SQL,
			transformed.Args,
		)
	}
	want := time.Date(2026, 7, 27, 19, 20, 21, 123456000, time.UTC)
	assertStrptimeEpoch(t, "transformed first", first, want)
	assertStrptimeEpoch(t, "transformed second", second, want)
	if count != 1 {
		t.Fatalf("transformed strptime count = %d, want 1", count)
	}
}

func assertStrptimeEpoch(
	t *testing.T,
	name string,
	got float64,
	want time.Time,
) {
	t.Helper()
	wantEpoch := float64(want.Unix()) +
		float64(want.Nanosecond()/int(time.Microsecond))/1_000_000
	if math.Abs(got-wantEpoch) > 0.000001 {
		t.Fatalf("%s strptime = %.9f, want %.9f", name, got, wantEpoch)
	}
}
