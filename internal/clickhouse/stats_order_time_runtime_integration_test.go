package clickhouse

import (
	"context"
	"math"
	"os"
	"os/exec"
	"reflect"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/chcol"
	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/ingest"
	"github.com/Suhaibinator/open-splunk/internal/testsupport"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// TestStatsOrderAndTimeAgainstClickHouse is opt-in because it starts the
// digest-pinned production ClickHouse image and applies the real schema.
func TestStatsOrderAndTimeAgainstClickHouse(t *testing.T) {
	if os.Getenv("OPEN_SPLUNK_CLICKHOUSE_INTEGRATION") != "1" {
		t.Skip("set OPEN_SPLUNK_CLICKHOUSE_INTEGRATION=1 to run the Docker integration test")
	}
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skipf("docker CLI is unavailable: %v", err)
	}
	image, err := testsupport.ResolvePinnedClickHouseImage(
		os.Getenv("OPEN_SPLUNK_CLICKHOUSE_TEST_IMAGE"),
	)
	if err != nil {
		t.Fatalf("resolve pinned ClickHouse image: %v", err)
	}
	t.Logf("ClickHouse image: %s", image)

	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()
	connection, store := chartEdgeStartClickHouse(t, ctx)
	indexTime := time.Date(2026, time.August, 11, 18, 0, 0, 0, time.UTC)
	// The shared integration planner intentionally searches July 20-22 while
	// the later index time exercises the independent ingestion cutoff.
	eventBase := time.Date(2026, time.July, 21, 3, 0, 0, 0, time.UTC)

	newEvent := func(
		id string,
		cohort string,
		eventTime time.Time,
		processOrder uint64,
		label *string,
		counter *opensplunk.TypedValue,
	) *ingest.StoredEvent {
		fields := []*opensplunk.TypedObjectField{
			typedField("cohort", typedString(cohort)),
			typedField("process_order", typedUint(processOrder)),
		}
		if label != nil {
			fields = append(fields, typedField("label", typedString(*label)))
		}
		if counter != nil {
			fields = append(fields, typedField("counter", counter))
		}
		event := testStoredEvent(id, "stats-order-time", indexTime)
		event.Event.Source = "stats-order-time"
		event.Event.EventTime = timestamppb.New(eventTime)
		event.Event.Fields = typedObjectValue(fields...)
		return event
	}
	fixtureStringPointer := func(value string) *string { return &value }
	events := []*ingest.StoredEvent{
		// Pipeline order and chronology intentionally disagree. The chronological
		// endpoint values are 10 at t=0 and 50 at t=20, hence rate=2.
		newEvent("order-a", "order", eventBase.Add(20*time.Second), 1,
			fixtureStringPointer("PIPELINE-FIRST"), typedSint(50)),
		newEvent("order-b", "order", eventBase, 2,
			fixtureStringPointer("CHRONOLOGICAL-FIRST"), typedSint(10)),
		newEvent("order-c", "order", eventBase.Add(10*time.Second), 3,
			fixtureStringPointer("PIPELINE-LAST"), typedSint(30)),
		newEvent("singleton", "singleton", eventBase.Add(5*time.Second), 1,
			fixtureStringPointer("ONLY"), typedSint(7)),
		newEvent("equal-a", "equal-time", eventBase.Add(5*time.Second), 1,
			fixtureStringPointer("EQUAL-FIRST"), typedSint(3)),
		newEvent("equal-b", "equal-time", eventBase.Add(5*time.Second), 2,
			fixtureStringPointer("EQUAL-LAST"), typedSint(9)),
		newEvent("ineligible", "ineligible", eventBase, 1, nil, nil),
		newEvent("reset-a", "counter-reset", eventBase, 1,
			fixtureStringPointer("RESET-FIRST"), typedSint(100)),
		newEvent("reset-b", "counter-reset", eventBase.Add(10*time.Second), 2,
			fixtureStringPointer("RESET-LAST"), typedSint(20)),
	}
	compile, queryContext := storeScalarFunctionIntegrationFixtures(
		ctx,
		t,
		store,
		indexTime,
		"stats-order-time",
		"stats-order-time-batch",
		453,
		events...,
	)

	type orderTimeResult struct {
		first        any
		last         any
		earliestTime *float64
		latestTime   *float64
		rate         *float64
	}
	execute := func(source string) orderTimeResult {
		t.Helper()
		compiled := compile(source)
		var first, last chcol.Dynamic
		var result orderTimeResult
		if queryErr := connection.QueryRow(
			queryContext,
			compiled.SQL,
			compiled.Args...,
		).Scan(
			&first,
			&last,
			&result.earliestTime,
			&result.latestTime,
			&result.rate,
		); queryErr != nil {
			t.Fatalf(
				"execute order/time stats: %v\nSPL: %s\nSQL: %s\nargs: %#v",
				queryErr,
				source,
				compiled.SQL,
				compiled.Args,
			)
		}
		result.first = first.Any()
		result.last = last.Any()
		return result
	}
	assertResult := func(
		name string,
		got orderTimeResult,
		wantFirst any,
		wantLast any,
		wantEarliest *float64,
		wantLatest *float64,
		wantRate *float64,
	) {
		t.Helper()
		if !reflect.DeepEqual(got.first, wantFirst) ||
			!reflect.DeepEqual(got.last, wantLast) {
			t.Fatalf(
				"%s first/last = %#v/%#v, want %#v/%#v (earliest=%v latest=%v rate=%v)",
				name,
				got.first,
				got.last,
				wantFirst,
				wantLast,
				got.earliestTime,
				got.latestTime,
				got.rate,
			)
		}
		assertStatsOrderTimeFloat(t, name+" earliest_time", got.earliestTime, wantEarliest)
		assertStatsOrderTimeFloat(t, name+" latest_time", got.latestTime, wantLatest)
		assertStatsOrderTimeFloat(t, name+" rate", got.rate, wantRate)
	}
	epoch := func(value time.Time) *float64 {
		seconds := float64(value.UnixNano()) / float64(time.Second)
		return &seconds
	}
	value := func(number float64) *float64 { return &number }

	const measures = `first(label) AS first_value last(label) AS last_value ` +
		`earliest_time(label) AS earliest_value_time ` +
		`latest_time(label) AS latest_value_time rate(counter) AS per_second`
	ordered := execute(
		`index=stats-order-time source="stats-order-time" cohort="order" ` +
			`| sort 0 +process_order | stats ` + measures,
	)
	assertResult(
		"pipeline order differs from chronology",
		ordered,
		"PIPELINE-FIRST",
		"PIPELINE-LAST",
		epoch(eventBase),
		epoch(eventBase.Add(20*time.Second)),
		value(2),
	)

	evaluated := execute(
		`index=stats-order-time source="stats-order-time" cohort="order" ` +
			`| sort 0 +process_order | stats ` +
			`first(eval(lower(label))) AS first_value ` +
			`last(eval(lower(label))) AS last_value ` +
			`earliest_time(eval(lower(label))) AS earliest_value_time ` +
			`latest_time(eval(lower(label))) AS latest_value_time ` +
			`rate(eval(counter+5)) AS per_second`,
	)
	assertResult(
		"eval inputs",
		evaluated,
		"pipeline-first",
		"pipeline-last",
		epoch(eventBase),
		epoch(eventBase.Add(20*time.Second)),
		value(2),
	)

	singleton := execute(
		`index=stats-order-time source="stats-order-time" cohort="singleton" ` +
			`| sort 0 +process_order | stats ` + measures,
	)
	assertResult(
		"singleton",
		singleton,
		"ONLY",
		"ONLY",
		epoch(eventBase.Add(5*time.Second)),
		epoch(eventBase.Add(5*time.Second)),
		nil,
	)

	equalTime := execute(
		`index=stats-order-time source="stats-order-time" cohort="equal-time" ` +
			`| sort 0 +process_order | stats ` + measures,
	)
	assertResult(
		"equal event times",
		equalTime,
		"EQUAL-FIRST",
		"EQUAL-LAST",
		epoch(eventBase.Add(5*time.Second)),
		epoch(eventBase.Add(5*time.Second)),
		nil,
	)

	empty := execute(
		`index=stats-order-time source="absent" | stats ` + measures,
	)
	assertResult("empty input", empty, nil, nil, nil, nil, nil)

	allIneligible := execute(
		`index=stats-order-time source="stats-order-time" cohort="ineligible" ` +
			`| stats first(missing_value) AS first_value ` +
			`last(missing_value) AS last_value ` +
			`earliest_time(missing_value) AS earliest_value_time ` +
			`latest_time(missing_value) AS latest_value_time ` +
			`rate(missing_value) AS per_second`,
	)
	assertResult("all-ineligible input", allIneligible, nil, nil, nil, nil, nil)

	// OracleRequired: this pins only the deliberately provisional no-reset
	// endpoint formula implemented locally. It is not an assertion of Splunk's
	// undocumented counter-reset behavior. A differential oracle must decide
	// whether a reset should instead incorporate the counter's largest value.
	reset := compile(
		`index=stats-order-time source="stats-order-time" cohort="counter-reset" ` +
			`| stats rate(counter) AS direct rate(eval(counter+1)) AS evaluated`,
	)
	var directResetRate, evaluatedResetRate *float64
	if queryErr := connection.QueryRow(
		queryContext,
		reset.SQL,
		reset.Args...,
	).Scan(&directResetRate, &evaluatedResetRate); queryErr != nil {
		t.Fatalf("execute provisional reset rates: %v\nSQL: %s", queryErr, reset.SQL)
	}
	assertStatsOrderTimeFloat(t, "provisional direct reset rate", directResetRate, value(-8))
	assertStatsOrderTimeFloat(t, "provisional evaluated reset rate", evaluatedResetRate, value(-8))
}

func assertStatsOrderTimeFloat(t *testing.T, name string, got, want *float64) {
	t.Helper()
	if got == nil || want == nil {
		if got != nil || want != nil {
			t.Fatalf("%s = %v, want %v", name, got, want)
		}
		return
	}
	if math.Abs(*got-*want) > 1e-12 {
		t.Fatalf("%s = %.17g, want %.17g", name, *got, *want)
	}
}
