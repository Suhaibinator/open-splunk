package clickhouse

import (
	"context"
	"strings"
	"testing"
	"time"

	clickhousedriver "github.com/ClickHouse/clickhouse-go/v2"
	"google.golang.org/protobuf/types/known/timestamppb"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/ingest"
)

// optionPackEvent stores one command-option fixture row. app is omitted
// when empty so dedup and timechart see a missing split key; region and status
// feed the top/rare BY groups; bytes feeds the value timechart.
func optionPackEvent(
	id string,
	indexTime time.Time,
	offset time.Duration,
	app, region string,
	status, bytes int64,
) *ingest.StoredEvent {
	event := testStoredEvent(id, "optionpack", indexTime)
	eventTime := time.Date(2026, 7, 20, 22, 0, 0, 0, time.UTC).Add(offset)
	event.Event.EventTime = timestamppb.New(eventTime)
	event.Event.CollectedAt = timestamppb.New(eventTime)
	fields := []*opensplunk.TypedObjectField{
		typedField("region", typedString(region)),
		typedField("status", typedSint(status)),
		typedField("bytes", typedSint(bytes)),
	}
	if app != "" {
		fields = append(fields, typedField("app", typedString(app)))
	}
	event.Event.Fields = typedObjectValue(fields...)
	return event
}

// optionPackRows executes a compiled query wrapped in a string projection so
// exact row expectations can be compared as one delimited string per row.
func optionPackRows(
	t *testing.T,
	ctx context.Context,
	connection clickhousedriver.Conn,
	label string,
	projection string,
	compiled CompiledQuery,
) []string {
	t.Helper()
	query := "SELECT " + projection + " FROM (" + compiled.SQL + ")"
	rows, err := connection.Query(ctx, query, compiled.Args...)
	if err != nil {
		t.Fatalf("execute %s: %v\nSQL: %s\nargs: %#v", label, err, query, compiled.Args)
	}
	defer func() {
		if closeErr := rows.Close(); closeErr != nil && !t.Failed() {
			t.Errorf("close %s rows: %v", label, closeErr)
		}
	}()
	var collected []string
	for rows.Next() {
		var value string
		if err := rows.Scan(&value); err != nil {
			t.Fatalf("scan %s row: %v\nSQL: %s", label, err, query)
		}
		collected = append(collected, value)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate %s rows: %v\nSQL: %s", label, err, query)
	}
	return collected
}

func requireOptionPackRows(t *testing.T, label string, got, want []string) {
	t.Helper()
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Fatalf("%s rows = %q, want %q", label, got, want)
	}
}

// optionPackTimechart reduces a split timechart to its ordered series names
// and element-wise totals so limit/useother/usenull can be asserted exactly.
func optionPackTimechart(
	t *testing.T,
	ctx context.Context,
	connection clickhousedriver.Conn,
	label string,
	compiled CompiledQuery,
) (string, string) {
	t.Helper()
	if compiled.Timechart == nil {
		t.Fatalf("%s is not a timechart: %#v", label, compiled)
	}
	totals := quoteIdentifier(TimechartCountsColumn)
	if compiled.Timechart.Mode == TimechartModeRuntimeWideValue {
		totals = quoteIdentifier(TimechartValuesColumn)
	}
	query := "SELECT arrayStringConcat(argMin(" + quoteIdentifier(TimechartNamesColumn) + ", " +
		quoteIdentifier(TimechartOrdinalColumn) + "), '|'), " +
		"arrayStringConcat(arrayMap(value -> toString(value), sumForEach(" + totals + ")), '|'), " +
		"max(" + quoteIdentifier(TimechartInvalidColumn) + ") FROM (" + compiled.SQL + ")"
	var names, counts string
	var invalid uint8
	if err := connection.QueryRow(ctx, query, compiled.Args...).Scan(&names, &counts, &invalid); err != nil {
		t.Fatalf("execute %s: %v\nSQL: %s\nargs: %#v", label, err, query, compiled.Args)
	}
	if invalid != 0 {
		t.Fatalf("%s reported invalid split rows", label)
	}
	return names, counts
}

// testCommandOptionPackAgainstClickHouse exercises the runtime shape of the
// command-option lowerings that unit tests can only pin as SQL text: dedup
// sortby/consecutive runs, top/rare BY per-group percentages and retention,
// and the timechart limit/useother/usenull series selection in both the count
// and value transports.
func testCommandOptionPackAgainstClickHouse(
	ctx context.Context,
	t *testing.T,
	store *Store,
	connection clickhousedriver.Conn,
	indexTime time.Time,
) {
	t.Helper()

	// Newest first (the default relation order): a a b a b b <missing> b c.
	compile, queryContext := storeScalarFunctionIntegrationFixtures(
		ctx,
		t,
		store,
		indexTime,
		"optionpack",
		"option-pack-batch",
		140,
		optionPackEvent("op-10", indexTime, 10*time.Minute, "a", "east", 200, 10),
		optionPackEvent("op-09", indexTime, 9*time.Minute, "a", "east", 200, 20),
		optionPackEvent("op-08", indexTime, 8*time.Minute, "b", "east", 200, 30),
		optionPackEvent("op-07", indexTime, 7*time.Minute, "a", "east", 500, 40),
		optionPackEvent("op-06", indexTime, 6*time.Minute, "b", "west", 404, 50),
		optionPackEvent("op-05", indexTime, 5*time.Minute, "b", "west", 404, 60),
		optionPackEvent("op-04", indexTime, 4*time.Minute, "", "west", 200, 70),
		optionPackEvent("op-03", indexTime, 3*time.Minute, "b", "west", 503, 80),
		optionPackEvent("op-02", indexTime, 2*time.Minute, "c", "west", 503, 90),
	)

	t.Run("dedup consecutive keeps the first row of every adjacent run", func(t *testing.T) {
		requireOptionPackRows(t, "dedup app",
			optionPackRows(t, queryContext, connection, "dedup app", "event_id",
				compile(`index=optionpack | dedup app | table event_id`)),
			[]string{"op-10", "op-08", "op-02"})
		// The missing-app row neither starts nor breaks the b run, so op-03
		// repeats the run that began at op-06.
		requireOptionPackRows(t, "dedup app consecutive",
			optionPackRows(t, queryContext, connection, "dedup app consecutive", "event_id",
				compile(`index=optionpack | dedup app consecutive=true | table event_id`)),
			[]string{"op-10", "op-08", "op-07", "op-06", "op-02"})
		requireOptionPackRows(t, "dedup 2 app consecutive",
			optionPackRows(t, queryContext, connection, "dedup 2 app consecutive", "event_id",
				compile(`index=optionpack | dedup 2 app consecutive=true | table event_id`)),
			[]string{"op-10", "op-09", "op-08", "op-07", "op-06", "op-05", "op-02"})
	})

	t.Run("dedup sortby orders the input before retention", func(t *testing.T) {
		requireOptionPackRows(t, "dedup app sortby bytes",
			optionPackRows(t, queryContext, connection, "dedup app sortby bytes", "event_id",
				compile(`index=optionpack | dedup app sortby +bytes | table event_id`)),
			[]string{"op-10", "op-08", "op-02"})
		requireOptionPackRows(t, "dedup app sortby -bytes",
			optionPackRows(t, queryContext, connection, "dedup app sortby -bytes", "event_id",
				compile(`index=optionpack | dedup app sortby -bytes | table event_id`)),
			[]string{"op-02", "op-03", "op-07"})
		// Descending bytes reverses the relation (c b _ b b a b a a), so the
		// runs start at different rows than the newest-first order's.
		requireOptionPackRows(t, "dedup app sortby -bytes consecutive",
			optionPackRows(t, queryContext, connection, "dedup app sortby -bytes consecutive", "event_id",
				compile(`index=optionpack | dedup app consecutive=true sortby -bytes | table event_id`)),
			[]string{"op-02", "op-03", "op-07", "op-08", "op-09"})
	})

	t.Run("top and rare BY scope percent and limit to each group", func(t *testing.T) {
		projection := `concat(region, ' ', toString(status), ' ', toString(count), ' ', toString(round(percent, 2)))`
		requireOptionPackRows(t, "top status BY region",
			optionPackRows(t, queryContext, connection, "top status BY region", projection,
				compile(`index=optionpack | top status BY region`)),
			[]string{"east 200 3 75", "east 500 1 25", "west 503 2 40", "west 404 2 40", "west 200 1 20"})
		requireOptionPackRows(t, "top limit=1 status BY region",
			optionPackRows(t, queryContext, connection, "top limit=1 status BY region", projection,
				compile(`index=optionpack | top limit=1 status BY region`)),
			[]string{"east 200 3 75", "west 503 2 40"})
		requireOptionPackRows(t, "rare limit=1 status BY region",
			optionPackRows(t, queryContext, connection, "rare limit=1 status BY region", projection,
				compile(`index=optionpack | rare limit=1 status BY region`)),
			[]string{"east 500 1 25", "west 200 1 20"})
		requireOptionPackRows(t, "top showperc=false countfield=total status BY region",
			optionPackRows(t, queryContext, connection, "top showperc=false countfield=total status BY region",
				`concat(region, ' ', toString(status), ' ', toString(total))`,
				compile(`index=optionpack | top limit=1 showperc=false countfield=total status BY region`)),
			[]string{"east 200 3", "west 503 2"})
	})

	t.Run("timechart limit useother and usenull select the published series", func(t *testing.T) {
		for _, test := range []struct {
			source string
			names  string
			totals string
		}{
			{`timechart span=1h count BY app`, "0:a|0:b|0:c|1:", "3|4|1|1"},
			{`timechart span=1h limit=2 count BY app`, "0:a|0:b|1:|2:", "3|4|1|1"},
			{`timechart span=1h count BY app limit=2 useother=false`, "0:a|0:b|1:", "3|4|1"},
			{`timechart span=1h usenull=false count BY app limit=2`, "0:a|0:b|2:", "3|4|1"},
			{`timechart span=1h limit=1 usenull=false useother=false count BY app`, "0:b", "4"},
			{`timechart span=1h count(status) BY app limit=1 useother=false`, "0:b|1:", "4|1"},
			{`timechart span=1h sum(bytes) BY app`, "0:a|0:b|0:c|1:", "70|220|90|70"},
			{`timechart span=1h limit=1 sum(bytes) BY app`, "0:b|1:|2:", "220|70|160"},
			{`timechart span=1h sum(bytes) BY app limit=1 useother=false`, "0:b|1:", "220|70"},
			// Value series rank by their aggregate, so avg and p50 pick c (90).
			{`timechart span=1h avg(bytes) BY app limit=1 usenull=false`, "0:c|2:", "90|41.42857142857143"},
			{`timechart span=1h limit=1 usenull=false useother=false p50(bytes) BY app`, "0:c", "90"},
		} {
			names, totals := optionPackTimechart(t, queryContext, connection, test.source,
				compile(`index=optionpack | `+test.source))
			if names != test.names || totals != test.totals {
				t.Fatalf("%s series = %q/%q, want %q/%q", test.source, names, totals, test.names, test.totals)
			}
		}
	})
}
