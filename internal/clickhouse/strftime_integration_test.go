package clickhouse

import (
	"context"
	"fmt"
	"testing"
	"time"

	clickhousedriver "github.com/ClickHouse/clickhouse-go/v2"
)

func testStrftimeAgainstClickHouse(
	ctx context.Context,
	t *testing.T,
	store *Store,
	connection clickhousedriver.Conn,
	indexTime time.Time,
) {
	t.Helper()

	event := testStoredEvent("strftime-scalars", "strftime", indexTime)
	eventTime := event.Event.EventTime.AsTime()
	event.Event.Fields = typedObjectValue(
		typedField("whole", typedSint(0)),
		typedField("signed", typedSint(1_700_000_001)),
		typedField("unsigned", typedUint(1_700_000_001)),
		typedField("fraction", typedDouble(0.125)),
		typedField("huge", typedUint(^uint64(0))),
		typedField("text", typedString("0")),
		typedField("flag", typedBool(false)),
		typedField("nothing", typedNull()),
		typedField("multi", typedList(typedSint(0), typedSint(1))),
	)
	compileUTC, queryContext := storeScalarFunctionIntegrationFixtures(
		ctx,
		t,
		store,
		indexTime,
		"strftime",
		"strftime-batch",
		115,
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
			"strftime",
		)
		logical.SearchTimezone = timezone
		compiled, err := (Compiler{}).Compile(logical)
		if err != nil {
			t.Fatalf("compile strftime integration SPL %q: %v", source, err)
		}
		return compiled
	}

	const allFormat = "%Y|%y|%G|%g|%m|%b|%B|%d|%e|%j|%V|%w|%a|%A|" +
		"%H|%I|%M|%S|%p|%T|%F|%s|%z|%:z|%Q|%3N|%6N|%9N|%f|%%"
	all := compile(
		`index=strftime event_id="strftime-scalars" | eval rendered=strftime(_time, "`+
			allFormat+`") | table rendered`,
		"America/Los_Angeles",
	)
	var rendered string
	if err := connection.QueryRow(
		queryContext,
		all.SQL,
		all.Args...,
	).Scan(&rendered); err != nil {
		t.Fatalf(
			"execute all strftime directives: %v\nSQL: %s\nargs: %#v",
			err,
			all.SQL,
			all.Args,
		)
	}
	if want := expectedStrftimeAll(t, eventTime, "America/Los_Angeles"); rendered != want {
		t.Fatalf("all strftime directives = %q, want %q", rendered, want)
	}

	fixed := compileUTC(
		`index=strftime event_id="strftime-scalars"` +
			` | eval epoch=strftime(0, "%F %T.%9N %s %z"),` +
			` negative=strftime(-0.5, "%F %T.%9N %s"),` +
			` padded=strftime(1782864000, "%e"),` +
			` week_boundary=strftime(1609459200, "%Y %G %g %V %w %I %p"),` +
			` admitted=strftime(now(), "%F %T.%Q"),` +
			` literal=strftime(_time, "東京 %H時%M分 O'clock %%"),` +
			` empty=strftime(_time, "")` +
			` | table epoch,negative,padded,week_boundary,admitted,literal,empty`,
	)
	var epoch, negative, padded, weekBoundary, admitted, literal, empty string
	if err := connection.QueryRow(
		queryContext,
		fixed.SQL,
		fixed.Args...,
	).Scan(
		&epoch,
		&negative,
		&padded,
		&weekBoundary,
		&admitted,
		&literal,
		&empty,
	); err != nil {
		t.Fatalf(
			"execute fixed strftime: %v\nSQL: %s\nargs: %#v",
			err,
			fixed.SQL,
			fixed.Args,
		)
	}
	wantAdmission := indexTime.Add(9*time.Second).UTC().
		Format("2006-01-02 15:04:05") + ".000"
	if epoch != "1970-01-01 00:00:00.000000000 0 +0000" ||
		negative != "1969-12-31 23:59:59.500000000 -1" ||
		padded != " 1" || weekBoundary != "2021 2020 20 53 5 12 AM" ||
		admitted != wantAdmission ||
		literal != "東京 22時04分 O'clock %" || empty != "" {
		t.Fatalf(
			"fixed strftime = %q/%q/%q/%q/%q/%q/%q, want epoch/negative/padded/week/%q/literal/empty",
			epoch,
			negative,
			padded,
			weekBoundary,
			admitted,
			literal,
			empty,
			wantAdmission,
		)
	}

	dynamic := compileUTC(
		`index=strftime event_id="strftime-scalars"` +
			` | eval whole_result=strftime(whole, "%s.%9N"),` +
			` signed_result=strftime(signed, "%s.%9N"),` +
			` unsigned_result=strftime(unsigned, "%s.%9N"),` +
			` fraction_result=strftime(fraction, "%s.%9N"),` +
			` huge_result=strftime(huge, "%F"),` +
			` text_result=strftime(text, "%F"),` +
			` flag_result=strftime(flag, "%F"),` +
			` null_result=strftime(nothing, "%F"),` +
			` missing_result=strftime(absent, "%F"),` +
			` multi_result=strftime(multi, "%F")` +
			` | table whole_result,signed_result,unsigned_result,fraction_result,huge_result,text_result,flag_result,null_result,missing_result,multi_result`,
	)
	var whole, signed, unsigned, fraction *string
	var hugeResult, textResult, flagResult, nullResult, missingResult, multiResult *string
	if err := connection.QueryRow(
		queryContext,
		dynamic.SQL,
		dynamic.Args...,
	).Scan(
		&whole,
		&signed,
		&unsigned,
		&fraction,
		&hugeResult,
		&textResult,
		&flagResult,
		&nullResult,
		&missingResult,
		&multiResult,
	); err != nil {
		t.Fatalf(
			"execute Dynamic strftime: %v\nSQL: %s\nargs: %#v",
			err,
			dynamic.SQL,
			dynamic.Args,
		)
	}
	if whole == nil || *whole != "0.000000000" ||
		signed == nil || *signed != "1700000001.000000000" ||
		unsigned == nil || *unsigned != "1700000001.000000000" ||
		fraction == nil || *fraction != "0.125000000" ||
		hugeResult != nil || textResult != nil || flagResult != nil || nullResult != nil ||
		missingResult != nil || multiResult != nil {
		t.Fatalf(
			"Dynamic strftime = %#v/%#v/%#v/%#v/%#v/%#v/%#v/%#v/%#v/%#v",
			whole,
			signed,
			unsigned,
			fraction,
			hugeResult,
			textResult,
			flagResult,
			nullResult,
			missingResult,
			multiResult,
		)
	}

	dst := compile(
		`index=strftime event_id="strftime-scalars"`+
			` | eval summer=strftime(_time, "%F %T %z %:z"),`+
			` winter=strftime(1767225600, "%F %T %z %:z")`+
			` | table summer,winter`,
		"America/Los_Angeles",
	)
	var summer, winter string
	if err := connection.QueryRow(
		queryContext,
		dst.SQL,
		dst.Args...,
	).Scan(&summer, &winter); err != nil {
		t.Fatalf(
			"execute timezone strftime: %v\nSQL: %s\nargs: %#v",
			err,
			dst.SQL,
			dst.Args,
		)
	}
	if summer != eventTime.In(mustLocation(t, "America/Los_Angeles")).
		Format("2006-01-02 15:04:05 -0700")+" -07:00" ||
		winter != "2025-12-31 16:00:00 -0800 -08:00" {
		t.Fatalf("timezone strftime = %q/%q", summer, winter)
	}

	transformed := compile(
		`index=strftime event_id="strftime-scalars"`+
			` | where strftime(_time, "%F")="2026-07-20"`+
			` | eval first=strftime(_time, "%F %T")`+
			` | table first`+
			` | stats count BY first`+
			` | eval second=strftime(now(), "%F %T")`+
			` | table first,second,count`,
		"America/Los_Angeles",
	)
	var first, second string
	var count uint64
	if err := connection.QueryRow(
		queryContext,
		transformed.SQL,
		transformed.Args...,
	).Scan(&first, &second, &count); err != nil {
		t.Fatalf(
			"execute transformed strftime: %v\nSQL: %s\nargs: %#v",
			err,
			transformed.SQL,
			transformed.Args,
		)
	}
	if first != eventTime.In(mustLocation(t, "America/Los_Angeles")).
		Format("2006-01-02 15:04:05") ||
		second != indexTime.Add(9*time.Second).
			In(mustLocation(t, "America/Los_Angeles")).
			Format("2006-01-02 15:04:05") ||
		count != 1 {
		t.Fatalf("transformed strftime = %q/%q/%d", first, second, count)
	}
}

func expectedStrftimeAll(t *testing.T, instant time.Time, timezone string) string {
	t.Helper()
	local := instant.In(mustLocation(t, timezone))
	isoYear, isoWeek := local.ISOWeek()
	_, offset := local.Zone()
	offsetSign := "+"
	if offset < 0 {
		offsetSign = "-"
		offset = -offset
	}
	offsetHours := offset / int(time.Hour/time.Second)
	offsetMinutes := offset / int(time.Minute/time.Second) % 60
	offsetCompact := fmt.Sprintf("%s%02d%02d", offsetSign, offsetHours, offsetMinutes)
	offsetColon := fmt.Sprintf("%s%02d:%02d", offsetSign, offsetHours, offsetMinutes)
	nanoseconds := fmt.Sprintf("%09d", local.Nanosecond())
	return fmt.Sprintf(
		"%04d|%02d|%04d|%02d|%02d|%s|%s|%02d|%2d|%03d|%02d|%d|%s|%s|"+
			"%02d|%02d|%02d|%02d|%s|%s|%s|%d|%s|%s|%s|%s|%s|%s|%s|%%",
		local.Year(),
		local.Year()%100,
		isoYear,
		isoYear%100,
		int(local.Month()),
		local.Format("Jan"),
		local.Format("January"),
		local.Day(),
		local.Day(),
		local.YearDay(),
		isoWeek,
		int(local.Weekday()),
		local.Format("Mon"),
		local.Format("Monday"),
		local.Hour(),
		hour12(local.Hour()),
		local.Minute(),
		local.Second(),
		local.Format("PM"),
		local.Format("15:04:05"),
		local.Format("2006-01-02"),
		local.Unix(),
		offsetCompact,
		offsetColon,
		nanoseconds[:3],
		nanoseconds[:3],
		nanoseconds[:6],
		nanoseconds,
		nanoseconds[:6],
	)
}

func hour12(hour int) int {
	hour %= 12
	if hour == 0 {
		return 12
	}
	return hour
}

func mustLocation(t *testing.T, name string) *time.Location {
	t.Helper()
	location, err := time.LoadLocation(name)
	if err != nil {
		t.Fatal(err)
	}
	return location
}

func mustVisibilityCutoff(
	t *testing.T,
	ctx context.Context,
	store *Store,
) uint64 {
	t.Helper()
	cutoff, err := store.VisibilityCutoff(ctx)
	if err != nil {
		t.Fatal(err)
	}
	return cutoff
}
