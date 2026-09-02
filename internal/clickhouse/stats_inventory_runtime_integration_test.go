package clickhouse

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	clickhousedriver "github.com/ClickHouse/clickhouse-go/v2"
	"google.golang.org/protobuf/types/known/timestamppb"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/ingest"
	"github.com/Suhaibinator/open-splunk/internal/plan"
	"github.com/Suhaibinator/open-splunk/internal/spl"
	"github.com/Suhaibinator/open-splunk/internal/testsupport"
)

// TestStatsInventoryPinnedRuntimeAgainstClickHouse executes the stats
// inventory surfaces whose compatibility ledger entries claim a pinned
// runtime (internal/spl/testdata/compatibility.json → stats_inventory) against
// the pinned ClickHouse image. Each subtest is named after the ledger id it
// pins; internal/spl/stats_inventory_evidence_test.go refuses a ledger flip
// that is not backed by one of these subtests.
func TestStatsInventoryPinnedRuntimeAgainstClickHouse(t *testing.T) {
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

	const (
		index  = "stats-inventory"
		source = "stats-inventory-main"
	)
	indexTime := time.Date(2026, time.August, 11, 20, 0, 0, 0, time.UTC)
	// The search window is 2026-07-20 → 2026-07-22, so every event sits in
	// the first 1d sparkline bin and the second bin is deliberately empty.
	eventBase := time.Date(2026, time.July, 20, 22, 0, 0, 0, time.UTC)
	newEvent := func(
		id string,
		offset time.Duration,
		fields ...*opensplunk.TypedObjectField,
	) *ingest.StoredEvent {
		event := testStoredEvent(id, index, indexTime)
		event.Event.Source = source
		event.Event.EventTime = timestamppb.New(eventBase.Add(offset))
		event.Event.CollectedAt = timestamppb.New(eventBase.Add(offset))
		event.Event.Fields = typedObjectValue(fields...)
		return event
	}
	compile, queryContext := storeScalarFunctionIntegrationFixtures(
		ctx, t, store, indexTime, index, "stats-inventory-batch", 131,
		newEvent("inv-1", 0,
			typedField("value", typedSint(1)),
			typedField("label", typedString("alpha")),
			typedField("tag", typedList(typedString("x"), typedString("y"))),
			typedField("Product Name", typedDouble(10)),
			typedField("request-bytes", typedUint(100)),
			typedField("HTTP Status", typedString("200")),
			typedField("http_request_bytes", typedUint(100)),
			typedField("http_response_bytes", typedUint(1000)),
			typedField("latency", typedUint(5)),
			typedField("bytes", typedUint(10)),
			typedField("delay", typedUint(1)),
			typedField("xdelay", typedUint(2)),
		),
		newEvent("inv-2", 10*time.Second,
			typedField("value", typedSint(2)),
			typedField("label", typedString("alpha")),
			typedField("tag", typedList(typedString("y"))),
			typedField("Product Name", typedDouble(20)),
			typedField("request-bytes", typedUint(200)),
			typedField("HTTP Status", typedString("200")),
			typedField("http_request_bytes", typedUint(200)),
			typedField("http_response_bytes", typedUint(2000)),
			typedField("latency", typedUint(10)),
			typedField("bytes", typedUint(20)),
			typedField("delay", typedUint(2)),
			typedField("xdelay", typedUint(4)),
		),
		newEvent("inv-3", 20*time.Second,
			typedField("value", typedSint(3)),
			typedField("label", typedString("beta")),
			typedField("tag", typedList(typedString("z"))),
			typedField("Product Name", typedDouble(30)),
			typedField("request-bytes", typedUint(300)),
			typedField("HTTP Status", typedString("500")),
			typedField("http_request_bytes", typedUint(300)),
			typedField("http_response_bytes", typedUint(3000)),
			typedField("latency", typedUint(15)),
			typedField("bytes", typedUint(30)),
			typedField("delay", typedUint(3)),
			typedField("xdelay", typedUint(6)),
		),
		// inv-4 carries no numeric fields so every numeric aggregate has to
		// ignore a missing input rather than coerce it.
		newEvent("inv-4", 30*time.Second,
			typedField("label", typedString("beta")),
			typedField("tag", typedString("w")),
			typedField("HTTP Status", typedString("500")),
		),
	)
	base := `index=` + index + ` source="` + source + `"`
	sparkline := func(bins ...string) string {
		return pipelineJSONText(append([]string{statsSparklineMarker}, bins...))
	}

	t.Run("partitions", func(t *testing.T) {
		// Every stats stage contributes min(partitions,4); stages take the
		// minimum; absent and 0 resolve to 1; a query without stats carries
		// no hint at all.
		for _, tc := range []struct {
			source string
			hint   uint8
			ok     bool
		}{
			{source: base + ` | table label`, hint: 0, ok: false},
			{source: base + ` | stats count AS events`, hint: 1, ok: true},
			{source: base + ` | stats partitions=0 count AS events`, hint: 1, ok: true},
			{source: base + ` | stats partitions=2 count AS events`, hint: 2, ok: true},
			{source: base + ` | stats partitions=100 count AS events`, hint: 4, ok: true},
			{
				source: base + ` | stats partitions=3 count AS events BY label` +
					` | stats partitions=2 sum(events) AS events`,
				hint: 2,
				ok:   true,
			},
		} {
			compiled := compile(tc.source)
			hint, ok := compiled.StatsPartitionsMaxThreadsHint()
			if hint != tc.hint || ok != tc.ok {
				t.Fatalf("%q max_threads hint = %d/%t, want %d/%t", tc.source, hint, ok, tc.hint, tc.ok)
			}
			if !ok {
				continue
			}
			// Execute with the hint applied exactly as the executor would and
			// prove through system.query_log that ClickHouse ran the query
			// under that max_threads value while still returning the row count.
			queryID := fmt.Sprintf("open-splunk-stats-partitions-%d-%d", time.Now().UnixNano(), hint)
			partitionContext := clickhousedriver.Context(
				queryContext,
				clickhousedriver.WithQueryID(queryID),
				clickhousedriver.WithSettings(clickhousedriver.Settings{
					"log_queries": uint64(1),
					"max_threads": uint64(hint),
				}),
			)
			rows := statsInventoryRows(t, partitionContext, connection, compiled)
			if !reflect.DeepEqual(rows, [][]string{{"4"}}) {
				t.Fatalf("%q rows = %#v, want one row counting 4 events", tc.source, rows)
			}
			if err := connection.Exec(ctx, "SYSTEM FLUSH LOGS"); err != nil {
				t.Fatalf("flush query log: %v", err)
			}
			var effective uint64
			if err := connection.QueryRow(
				ctx,
				`SELECT if(
					mapContains(Settings, 'max_threads'),
					toUInt64OrZero(Settings['max_threads']),
					toUInt64(getSetting('max_threads'))
				)
				FROM system.query_log
				WHERE type = 'QueryFinish' AND query_id = ?`,
				queryID,
			).Scan(&effective); err != nil {
				t.Fatalf("read query_log max_threads for %q: %v", tc.source, err)
			}
			if effective != uint64(hint) {
				t.Fatalf("%q ran with max_threads=%d, want %d", tc.source, effective, hint)
			}
		}
	})

	t.Run("delim", func(t *testing.T) {
		compiled := compile(base +
			` | stats delim="::" values(tag) AS tags list(tag) AS ordered count AS events BY label`)
		if !slices.Equal(compiled.OutputFields, []string{"label", "tags", "ordered", "events"}) {
			t.Fatalf("delim output fields = %#v", compiled.OutputFields)
		}
		statsInventoryRequireDelimiters(t, compiled, map[string]string{"tags": "::", "ordered": "::"})
		// values() is sorted and de-duplicated; list() keeps arrival order,
		// which is newest event first.
		statsInventoryRequireRows(t, queryContext, connection, compiled, [][]string{
			{"alpha", "[x,y]", "[y,x,y]", "2"},
			{"beta", "[w,z]", "[w,z]", "2"},
		})

		// The delimiter is ordinal presentation metadata: it follows an exact
		// rename/table and the typed list cell itself is unchanged.
		renamed := compile(base +
			` | stats delim="::" values(tag) AS tags list(tag) AS ordered count AS events BY label` +
			` | rename tags AS renamed | table label events renamed`)
		if !slices.Equal(renamed.OutputFields, []string{"label", "events", "renamed"}) {
			t.Fatalf("renamed output fields = %#v", renamed.OutputFields)
		}
		statsInventoryRequireDelimiters(t, renamed, map[string]string{"renamed": "::"})
		statsInventoryRequireRows(t, queryContext, connection, renamed, [][]string{
			{"alpha", "2", "[x,y]"},
			{"beta", "2", "[w,z]"},
		})

		// Overwriting the field with eval drops the presentation with the list.
		overwritten := compile(base +
			` | stats delim="::" values(tag) AS tags count AS events BY label | eval tags="replacement"`)
		if !slices.Equal(overwritten.OutputFields, []string{"label", "tags", "events"}) {
			t.Fatalf("overwritten output fields = %#v", overwritten.OutputFields)
		}
		statsInventoryRequireDelimiters(t, overwritten, map[string]string{})
		statsInventoryRequireRows(t, queryContext, connection, overwritten, [][]string{
			{"alpha", "replacement", "2"},
			{"beta", "replacement", "2"},
		})

		// Absent delim keeps the documented single-space default, and a
		// downstream stats stage over the list resets the presentation while
		// still consuming the typed members (count sees four tags, not two
		// joined strings).
		defaulted := compile(base + ` | stats values(tag) AS tags BY label | stats count(tags) AS members`)
		if !slices.Equal(defaulted.OutputFields, []string{"members"}) {
			t.Fatalf("defaulted output fields = %#v", defaulted.OutputFields)
		}
		statsInventoryRequireDelimiters(t, defaulted, map[string]string{})
		statsInventoryRequireRows(t, queryContext, connection, defaulted, [][]string{{"4"}})
		upstream := compile(base + ` | stats values(tag) AS tags BY label`)
		statsInventoryRequireDelimiters(t, upstream, map[string]string{"tags": spl.DefaultStatsDelimiter})
	})

	t.Run("eval-count-default-alias", func(t *testing.T) {
		compiled := compile(base +
			` | stats count(eval(isnull(value))) count(eval(value > 1)) CoUnT(eval( value=2 ))`)
		want := []string{
			"count(eval(isnull(value)))",
			"count(eval(value > 1))",
			"CoUnT(eval( value=2 ))",
		}
		if !slices.Equal(compiled.OutputFields, want) {
			t.Fatalf("eval count output fields = %#v, want %#v", compiled.OutputFields, want)
		}
		statsInventoryRequireAliasedSQL(t, compiled)
		statsInventoryRequireRows(t, queryContext, connection, compiled, [][]string{{"1", "2", "1"}})
	})

	t.Run("eval-numeric-default-alias", func(t *testing.T) {
		compiled := compile(base +
			` | stats p50(eval(value)) perc50(eval(value)) exactperc50(eval(value))` +
			` upperperc50(eval(value)) median(eval(value)) sum(eval(value*2)) AvG(eval( value + 1 ))` +
			` mean(eval(value)) range(eval(value)) sumsq(eval(value)) stdev(eval(value))` +
			` stdevp(eval(value)) var(eval(value)) varp(eval(value)) rate(eval(value))`)
		want := []string{
			"p50(eval(value))",
			"perc50(eval(value))",
			"exactperc50(eval(value))",
			"upperperc50(eval(value))",
			"median(eval(value))",
			"sum(eval(value*2))",
			"AvG(eval( value + 1 ))",
			"mean(eval(value))",
			"range(eval(value))",
			"sumsq(eval(value))",
			"stdev(eval(value))",
			"stdevp(eval(value))",
			"var(eval(value))",
			"varp(eval(value))",
			"rate(eval(value))",
		}
		if !slices.Equal(compiled.OutputFields, want) {
			t.Fatalf("eval numeric output fields = %#v, want %#v", compiled.OutputFields, want)
		}
		statsInventoryRequireAliasedSQL(t, compiled)
		// value = 1, 2, 3 at t, t+10s, t+20s; the fourth event has no value.
		statsInventoryRequireRows(t, queryContext, connection, compiled, [][]string{{
			"2",                  // p50
			"2",                  // perc50
			"2",                  // exactperc50
			"2",                  // upperperc50
			"2",                  // median
			"12",                 // sum(value*2)
			"3",                  // avg(value+1)
			"2",                  // mean
			"2",                  // range
			"14",                 // sumsq
			"1",                  // stdev (sample)
			"0.816496580927726",  // stdevp (population)
			"1",                  // var (sample)
			"0.6666666666666666", // varp (population)
			"0.1",                // rate: (3 - 1) / 20s
		}})
	})

	t.Run("wildcard-implicit", func(t *testing.T) {
		compiled := compile(base + ` | table bytes,latency | stats sum avg count`)
		want := []string{"sum(bytes)", "sum(latency)", "avg(bytes)", "avg(latency)", "count"}
		if !slices.Equal(compiled.OutputFields, want) {
			t.Fatalf("implicit wildcard output fields = %#v, want %#v", compiled.OutputFields, want)
		}
		// Bare count is the row count (4), while the implicit-* functions
		// only see the three events that carry the numeric fields.
		statsInventoryRequireRows(t, queryContext, connection, compiled, [][]string{{"60", "30", "20", "10", "4"}})

		explicit := compile(base +
			` | table http_request_bytes,http_response_bytes,latency` +
			` | stats avg(http_*_*) AS mean_*_* sum AS total_*`)
		wantExplicit := []string{
			"mean_request_bytes",
			"mean_response_bytes",
			"total_http_request_bytes",
			"total_http_response_bytes",
			"total_latency",
		}
		if !slices.Equal(explicit.OutputFields, wantExplicit) {
			t.Fatalf("explicit wildcard output fields = %#v, want %#v", explicit.OutputFields, wantExplicit)
		}
		statsInventoryRequireRows(t, queryContext, connection, explicit, [][]string{{"200", "2000", "600", "6000", "30"}})
	})

	t.Run("input-quoted-exact", func(t *testing.T) {
		compiled := compile(base +
			` | stats avg('Product Name') AS revenue sparkline(sum('request-bytes'),1d) AS trend` +
			` count('HTTP Status') AS statuses BY label`)
		if !slices.Equal(compiled.OutputFields, []string{"label", "revenue", "trend", "statuses"}) {
			t.Fatalf("quoted input output fields = %#v", compiled.OutputFields)
		}
		for _, literal := range []string{"Product Name", "request-bytes", "HTTP Status"} {
			if !slices.Contains(compiled.Args, any(literal)) {
				t.Fatalf("quoted input %q is not bound as a literal argument: %#v", literal, compiled.Args)
			}
		}
		statsInventoryRequireRows(t, queryContext, connection, compiled, [][]string{
			{"alpha", "15", sparkline("300", ""), "2"},
			{"beta", "30", sparkline("300", ""), "2"},
		})
	})

	t.Run("alias-quoted", func(t *testing.T) {
		compiled := compile(base +
			` | stats count AS "Event Count" sum(bytes) AS "total.bytes" sparkline(count,1d) AS ".com"` +
			` | eval doubled='Event Count' * 2 | table 'Event Count', 'total.bytes', '.com', doubled`)
		want := []string{"Event Count", "total.bytes", ".com", "doubled"}
		if !slices.Equal(compiled.OutputFields, want) {
			t.Fatalf("quoted alias output fields = %#v, want %#v", compiled.OutputFields, want)
		}
		statsInventoryRequireRows(t, queryContext, connection, compiled, [][]string{
			{"4", "60", sparkline("4", "0"), "8"},
		})
	})

	t.Run("by-quoted-field", func(t *testing.T) {
		compiled := compile(base + ` | stats count AS events sum(bytes) AS total BY 'HTTP Status'`)
		if !slices.Equal(compiled.OutputFields, []string{"HTTP Status", "events", "total"}) {
			t.Fatalf("quoted BY output fields = %#v", compiled.OutputFields)
		}
		if !slices.Contains(compiled.Args, any("HTTP Status")) {
			t.Fatalf("quoted BY field is not bound as a literal argument: %#v", compiled.Args)
		}
		statsInventoryRequireRows(t, queryContext, connection, compiled, [][]string{
			{"200", "2", "30"},
			{"500", "2", "30"},
		})
	})

	t.Run("sparkline-wildcard-alias", func(t *testing.T) {
		compiled := compile(base +
			` | table _time,delay,xdelay,latency` +
			` | stats sparkline(avg(*lay),1d) AS trend_* sparkline(count,1d) AS rows`)
		want := []string{"trend_de", "trend_xde", "rows"}
		if !slices.Equal(compiled.OutputFields, want) {
			t.Fatalf("sparkline wildcard output fields = %#v, want %#v", compiled.OutputFields, want)
		}
		// Missing bins render "" for avg and "0" for count.
		statsInventoryRequireRows(t, queryContext, connection, compiled, [][]string{
			{sparkline("2", ""), sparkline("4", ""), sparkline("4", "0")},
		})
	})

	t.Run("alias-same-source-twice-rejection", func(t *testing.T) {
		// The runtime scope (the same one every executed query above uses)
		// refuses the canonical duplicate-source forms before any SQL exists,
		// while the distinct-function form over the same input still executes.
		visibilityCutoff, err := store.VisibilityCutoff(ctx)
		if err != nil {
			t.Fatalf("capture visibility cutoff: %v", err)
		}
		scope := plan.Scope{
			TenantID: "tenant", AuthorizedIndexes: []string{index},
			Earliest:         time.Date(2026, time.July, 20, 0, 0, 0, 0, time.UTC),
			Latest:           time.Date(2026, time.July, 22, 0, 0, 0, 0, time.UTC),
			SearchStart:      indexTime.Add(9 * time.Second),
			SearchTimezone:   "UTC",
			IndexTimeCutoff:  indexTime.Add(10 * time.Second),
			VisibilityCutoff: new(visibilityCutoff),
		}
		for _, source := range []string{
			base + ` | stats c(label) AS a count(label) AS b`,
			base + ` | stats dc(label) AS a distinct_count(label) AS b`,
			base + ` | stats p95(value) AS a perc95(value) AS b`,
			base + ` | stats avg(value) AS a mean(value) AS b`,
			base + ` | stats first(label) AS a first('label') AS b`,
			base + ` | stats sparkline(avg(value),1d) AS a sparkline(mean(value),1d) AS b`,
		} {
			parsed, parseErr := spl.Parse(source)
			if parseErr != nil {
				t.Fatalf("parse %q: %v", source, parseErr)
			}
			_, buildErr := plan.Build(parsed, scope)
			var diagnostic *plan.Diagnostic
			if !errors.As(buildErr, &diagnostic) || diagnostic.Code != "SPL_DUPLICATE_STATS_AGGREGATE" {
				t.Fatalf("Build(%q) error = %v, want SPL_DUPLICATE_STATS_AGGREGATE", source, buildErr)
			}
		}
		distinct := compile(base +
			` | stats count(value) AS a dc(value) AS b avg(value) AS c` +
			` sum(eval(value+1)) AS d sum(eval(value+1)) AS e` +
			` sparkline(avg(value),1d) AS f sparkline(avg(value),1h) AS g`)
		if !slices.Equal(distinct.OutputFields, []string{"a", "b", "c", "d", "e", "f", "g"}) {
			t.Fatalf("distinct output fields = %#v", distinct.OutputFields)
		}
		rows := statsInventoryRows(t, queryContext, connection, distinct)
		if len(rows) != 1 || !slices.Equal(rows[0][:6], []string{"3", "3", "2", "9", "9", sparkline("2", "")}) {
			t.Fatalf("distinct rows = %#v", rows)
		}
		hourly := strings.Split(strings.Trim(rows[0][6], "[]"), ",")
		if len(hourly) != 49 || hourly[0] != statsSparklineMarker || hourly[23] != "2" ||
			slices.Contains(hourly[1:23], "2") || slices.Contains(hourly[24:], "2") {
			t.Fatalf("hourly sparkline = %#v", rows[0][6])
		}
	})
}

// statsInventoryRows executes the compiled query and renders every output
// field of every row as text, sorted lexicographically so grouped results
// without an ORDER BY compare deterministically.
func statsInventoryRows(
	t *testing.T,
	ctx context.Context,
	connection clickhousedriver.Conn,
	compiled CompiledQuery,
) [][]string {
	t.Helper()
	decoded := pipelineJSONRows(t, ctx, connection, compiled, compiled.OutputFields)
	rows := make([][]string, len(decoded))
	for rowIndex, row := range decoded {
		rows[rowIndex] = make([]string, len(row))
		for fieldIndex, value := range row {
			rows[rowIndex][fieldIndex] = pipelineJSONText(value)
		}
	}
	slices.SortFunc(rows, func(left, right []string) int {
		return slices.Compare(left, right)
	})
	return rows
}

func statsInventoryRequireRows(
	t *testing.T,
	ctx context.Context,
	connection clickhousedriver.Conn,
	compiled CompiledQuery,
	want [][]string,
) {
	t.Helper()
	got := statsInventoryRows(t, ctx, connection, compiled)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("rows = %#v, want %#v\nSQL: %s\nargs: %#v", got, want, compiled.SQL, compiled.Args)
	}
}

// statsInventoryRequireDelimiters asserts that exactly the named outputs carry
// a flat multivalue delimiter and that the presentation stays ordinal with
// OutputFields. A query with no presentation at all may omit the slice.
func statsInventoryRequireDelimiters(t *testing.T, compiled CompiledQuery, want map[string]string) {
	t.Helper()
	if compiled.OutputPresentations == nil && len(want) == 0 {
		return
	}
	if len(compiled.OutputPresentations) != len(compiled.OutputFields) {
		t.Fatalf(
			"presentations = %d, want one per output field (%d)",
			len(compiled.OutputPresentations),
			len(compiled.OutputFields),
		)
	}
	got := make(map[string]string, len(want))
	for index, presentation := range compiled.OutputPresentations {
		if presentation.HasFlatMultivalueDelimiter {
			got[compiled.OutputFields[index]] = presentation.FlatMultivalueDelimiter
		} else if presentation.FlatMultivalueDelimiter != "" {
			t.Fatalf("output %q has a delimiter without the flag", compiled.OutputFields[index])
		}
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("delimiters = %#v, want %#v", got, want)
	}
}

// statsInventoryRequireAliasedSQL asserts that every default alias is bound
// as the SQL column name, so the authored invocation text is what ClickHouse
// returns rather than a compiler-normalised spelling.
func statsInventoryRequireAliasedSQL(t *testing.T, compiled CompiledQuery) {
	t.Helper()
	for _, output := range compiled.OutputFields {
		if !strings.Contains(compiled.SQL, ` AS "`+output+`"`) {
			t.Fatalf("SQL does not bind output %q by its authored name\nSQL: %s", output, compiled.SQL)
		}
	}
}
