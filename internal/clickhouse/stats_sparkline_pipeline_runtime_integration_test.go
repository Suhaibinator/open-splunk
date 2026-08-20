package clickhouse

import (
	"context"
	"fmt"
	"math"
	"os"
	"os/exec"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	clickhousedriver "github.com/ClickHouse/clickhouse-go/v2"
	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/ingest"
	"github.com/Suhaibinator/open-splunk/internal/plan"
	"github.com/Suhaibinator/open-splunk/internal/spl"
	"github.com/Suhaibinator/open-splunk/internal/testsupport"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestStatsSparklinePipelineAgainstClickHouse(t *testing.T) {
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

	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	connection, store := chartEdgeStartClickHouse(t, ctx)

	const index = "stats-sparkline"
	earliest := time.Date(2026, time.July, 21, 0, 0, 0, 0, time.UTC)
	latest := earliest.Add(4 * time.Hour)
	indexTime := time.Date(2026, time.July, 21, 5, 0, 0, 0, time.UTC)
	newEvent := func(
		id string,
		at time.Time,
		fields ...*opensplunk.TypedObjectField,
	) *ingest.StoredEvent {
		t.Helper()
		event := testStoredEvent(id, index, indexTime)
		event.Event.Source = "stats-sparkline-fixture"
		event.Event.EventTime = timestamppb.New(at)
		event.Event.CollectedAt = timestamppb.New(at)
		event.Event.Fields = typedObjectValue(fields...)
		return event
	}
	events := []*ingest.StoredEvent{
		newEvent("spark-1", earliest.Add(10*time.Minute),
			typedField("metric", typedSint(1)),
			typedField("label", typedString("b")),
			typedField("tags", typedList(typedString("a"), typedString("b"), typedString("b"))),
		),
		newEvent("spark-2", earliest.Add(20*time.Minute),
			typedField("metric", typedSint(2)),
			typedField("label", typedString("a")),
			typedField("tags", typedList(typedString("a"))),
		),
		newEvent("spark-3", earliest.Add(2*time.Hour+10*time.Minute),
			typedField("metric", typedSint(3)),
			typedField("label", typedString("b")),
			typedField("tags", typedList(typedString("b"))),
		),
		newEvent("spark-4", earliest.Add(2*time.Hour+20*time.Minute),
			typedField("metric", typedSint(4)),
			typedField("label", typedString("c")),
			typedField("tags", typedList(typedString("a"), typedString("b"))),
		),
	}
	for _, event := range events {
		event.BatchID = "stats-sparkline-batch"
	}
	if _, err := store.Store(ctx, ingest.StoreBatch{
		TenantID:           "tenant",
		CollectorID:        "collector",
		BatchID:            "stats-sparkline-batch",
		BatchSequence:      701,
		OriginalEventCount: uint32(len(events)),
		SourceBatchSHA256:  testSourceBatchDigest("stats-sparkline-batch"),
		ReceivedAt:         indexTime,
		Events:             events,
	}); err != nil {
		t.Fatalf("store sparkline fixtures: %v", err)
	}
	visibilityCutoff, err := store.VisibilityCutoff(ctx)
	if err != nil {
		t.Fatalf("capture sparkline visibility cutoff: %v", err)
	}
	compile := func(t *testing.T, source string) CompiledQuery {
		t.Helper()
		parsed, parseErr := spl.Parse(source)
		if parseErr != nil {
			t.Fatalf("parse sparkline SPL %q: %v", source, parseErr)
		}
		logical, buildErr := plan.Build(parsed, plan.Scope{
			TenantID:          "tenant",
			AuthorizedIndexes: []string{index},
			Earliest:          earliest,
			Latest:            latest,
			SearchStart:       indexTime.Add(2 * time.Minute),
			SearchTimezone:    "UTC",
			IndexTimeCutoff:   indexTime.Add(time.Minute),
			VisibilityCutoff:  new(visibilityCutoff),
		})
		if buildErr != nil {
			t.Fatalf("build sparkline SPL %q: %v", source, buildErr)
		}
		compiled, compileErr := (Compiler{}).Compile(logical)
		if compileErr != nil {
			t.Fatalf("compile sparkline SPL %q: %v", source, compileErr)
		}
		if got, want := strings.Count(compiled.SQL, "?"), len(compiled.Args); got != want {
			t.Fatalf("placeholder count = %d, args = %d\nSQL: %s\nargs: %#v", got, want, compiled.SQL, compiled.Args)
		}
		return compiled
	}
	queryContext := clickhousedriver.Context(
		ctx,
		clickhousedriver.WithSettings(clickhousedriver.Settings{
			"use_variant_as_common_type":        uint8(0),
			"short_circuit_function_evaluation": "enable",
		}),
	)
	base := `index=stats-sparkline source="stats-sparkline-fixture"`

	t.Run("mixed ordinary explicit BY multivalue and missing bins", func(t *testing.T) {
		compiled := compile(t,
			base+` | stats count AS events sparkline(count,1h) AS trend `+
				`BY tags dedup_splitvals=true | sort 0 +tags`,
		)
		if !slices.Equal(compiled.OutputFields, []string{"tags", "events", "trend"}) {
			t.Fatalf("mixed output fields = %#v", compiled.OutputFields)
		}
		rows, queryErr := connection.Query(queryContext, compiled.SQL, compiled.Args...)
		if queryErr != nil {
			t.Fatalf("execute mixed sparkline: %v\nSQL: %s\nargs: %#v", queryErr, compiled.SQL, compiled.Args)
		}
		defer rows.Close()
		got := make(map[string]struct {
			events uint64
			trend  []string
		})
		for rows.Next() {
			var tag string
			var events uint64
			var trend []string
			if scanErr := rows.Scan(&tag, &events, &trend); scanErr != nil {
				t.Fatalf("scan mixed sparkline: %v", scanErr)
			}
			got[tag] = struct {
				events uint64
				trend  []string
			}{events: events, trend: trend}
		}
		if rowsErr := rows.Err(); rowsErr != nil {
			t.Fatalf("iterate mixed sparkline: %v", rowsErr)
		}
		want := map[string]struct {
			events uint64
			trend  []string
		}{
			"a": {events: 3, trend: []string{statsSparklineMarker, "2", "0", "1", "0"}},
			"b": {events: 3, trend: []string{statsSparklineMarker, "1", "0", "2", "0"}},
		}
		if fmt.Sprint(got) != fmt.Sprint(want) {
			t.Fatalf("mixed sparkline rows = %#v, want %#v", got, want)
		}
	})

	t.Run("all documented inner function lowerings", func(t *testing.T) {
		compiled := compile(t,
			base+` | stats `+
				`sparkline(count,1h) AS row_count `+
				`sparkline(count(metric),1h) AS field_count `+
				`sparkline(dc(label),1h) AS distinct_count `+
				`sparkline(avg(metric),1h) AS average `+
				`sparkline(stdev(metric),1h) AS sample_stdev `+
				`sparkline(stdevp(metric),1h) AS population_stdev `+
				`sparkline(var(metric),1h) AS sample_variance `+
				`sparkline(varp(metric),1h) AS population_variance `+
				`sparkline(sum(metric),1h) AS total `+
				`sparkline(sumsq(metric),1h) AS squares `+
				`sparkline(min(label),1h) AS minimum `+
				`sparkline(max(label),1h) AS maximum `+
				`sparkline(range(metric),1h) AS value_range`,
		)
		arrays := make([][]string, 13)
		targets := make([]any, len(arrays))
		for index := range arrays {
			targets[index] = &arrays[index]
		}
		if queryErr := connection.QueryRow(
			queryContext,
			compiled.SQL,
			compiled.Args...,
		).Scan(targets...); queryErr != nil {
			t.Fatalf("execute all sparkline functions: %v\nSQL: %s\nargs: %#v", queryErr, compiled.SQL, compiled.Args)
		}
		for index, series := range arrays {
			if len(series) != 5 || series[0] != statsSparklineMarker {
				t.Fatalf("function series %d = %#v", index, series)
			}
		}
		mean := compile(t, base+` | stats sparkline(mean(metric),1h) AS mean`)
		var meanSeries []string
		if queryErr := connection.QueryRow(
			queryContext,
			mean.SQL,
			mean.Args...,
		).Scan(&meanSeries); queryErr != nil {
			t.Fatalf("execute mean sparkline alias: %v\nSQL: %s", queryErr, mean.SQL)
		}
		if fmt.Sprint(meanSeries) != fmt.Sprint(arrays[3]) {
			t.Fatalf("avg/mean differ: %#v / %#v", arrays[3], meanSeries)
		}
		if fmt.Sprint(arrays[0]) != fmt.Sprint([]string{statsSparklineMarker, "2", "0", "2", "0"}) ||
			fmt.Sprint(arrays[1]) != fmt.Sprint(arrays[0]) ||
			fmt.Sprint(arrays[2]) != fmt.Sprint([]string{statsSparklineMarker, "2", "0", "2", "0"}) ||
			fmt.Sprint(arrays[8]) != fmt.Sprint([]string{statsSparklineMarker, "3", "", "7", ""}) ||
			fmt.Sprint(arrays[9]) != fmt.Sprint([]string{statsSparklineMarker, "5", "", "25", ""}) ||
			fmt.Sprint(arrays[10]) != fmt.Sprint([]string{statsSparklineMarker, "a", "", "b", ""}) ||
			fmt.Sprint(arrays[11]) != fmt.Sprint([]string{statsSparklineMarker, "b", "", "c", ""}) ||
			fmt.Sprint(arrays[12]) != fmt.Sprint([]string{statsSparklineMarker, "1", "", "1", ""}) {
			t.Fatalf("unexpected exact series = %#v", arrays)
		}
		assertNumericSeries := func(name string, series []string, first, third float64) {
			t.Helper()
			for _, probe := range []struct {
				index int
				want  float64
			}{{1, first}, {3, third}} {
				got, parseErr := strconv.ParseFloat(series[probe.index], 64)
				if parseErr != nil || math.Abs(got-probe.want) > 1e-12 {
					t.Fatalf("%s[%d] = %q (%v), want %.15g", name, probe.index, series[probe.index], parseErr, probe.want)
				}
			}
		}
		assertNumericSeries("average", arrays[3], 1.5, 3.5)
		assertNumericSeries("sample stdev", arrays[4], math.Sqrt(0.5), math.Sqrt(0.5))
		assertNumericSeries("population stdev", arrays[5], 0.5, 0.5)
		assertNumericSeries("sample variance", arrays[6], 0.5, 0.5)
		assertNumericSeries("population variance", arrays[7], 0.25, 0.25)
	})

	t.Run("automatic span empty global and downstream visibility", func(t *testing.T) {
		automatic := compile(t, base+` | stats sparkline(count) AS trend`)
		var automaticTrend []string
		if queryErr := connection.QueryRow(
			queryContext,
			automatic.SQL,
			automatic.Args...,
		).Scan(&automaticTrend); queryErr != nil {
			t.Fatalf("execute automatic sparkline: %v\nSQL: %s", queryErr, automatic.SQL)
		}
		// Four hours selects the first official step under 100 bins: five minutes.
		if len(automaticTrend) != 49 || automaticTrend[0] != statsSparklineMarker {
			t.Fatalf("automatic sparkline = %#v", automaticTrend)
		}

		downstream := compile(t,
			base+` | stats count AS events sparkline(count,1h) AS trend `+
				`| eval points=mvcount(trend) | where points=5 | table events points trend`,
		)
		var eventsCount, points uint64
		var trend []string
		if queryErr := connection.QueryRow(
			queryContext,
			downstream.SQL,
			downstream.Args...,
		).Scan(&eventsCount, &points, &trend); queryErr != nil {
			t.Fatalf("execute downstream sparkline: %v\nSQL: %s", queryErr, downstream.SQL)
		}
		if eventsCount != 4 || points != 5 || len(trend) != 5 {
			t.Fatalf("downstream sparkline = events=%d points=%d trend=%#v", eventsCount, points, trend)
		}

		empty := compile(t,
			base+` event_id="not-present" | stats `+
				`sparkline(count,1h) AS count_trend sparkline(avg(metric),1h) AS avg_trend`,
		)
		var countTrend, averageTrend []string
		if queryErr := connection.QueryRow(
			queryContext,
			empty.SQL,
			empty.Args...,
		).Scan(&countTrend, &averageTrend); queryErr != nil {
			t.Fatalf("execute empty global sparkline: %v\nSQL: %s", queryErr, empty.SQL)
		}
		if fmt.Sprint(countTrend) != fmt.Sprint([]string{statsSparklineMarker, "0", "0", "0", "0"}) ||
			fmt.Sprint(averageTrend) != fmt.Sprint([]string{statsSparklineMarker, "", "", "", ""}) {
			t.Fatalf("empty global sparklines = %#v / %#v", countTrend, averageTrend)
		}
	})
}
