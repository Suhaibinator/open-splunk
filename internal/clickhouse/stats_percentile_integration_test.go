package clickhouse

import (
	"context"
	"fmt"
	"math"
	"strings"
	"testing"
	"time"

	clickhousedriver "github.com/ClickHouse/clickhouse-go/v2"
	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/ingest"
)

func testStatsPercentilesAgainstClickHouse(
	ctx context.Context,
	t *testing.T,
	store *Store,
	connection clickhousedriver.Conn,
	indexTime time.Time,
) {
	t.Helper()

	const (
		batchID     = "stats-percentile-batch"
		collectorID = "stats-percentile-collector"
	)
	newEvent := func(id, group string, fields ...*opensplunkv1.TypedObjectField) *ingest.StoredEvent {
		fields = append(
			[]*opensplunkv1.TypedObjectField{
				typedField("percentile_group", typedString(group)),
			},
			fields...,
		)
		event := compilerIntegrationEvent(id, "percentile-host", "percentile fixture", indexTime, fields...)
		event.CollectorID = collectorID
		event.BatchID = batchID
		event.Event.Source = "stats-percentile"
		return event
	}

	events := make([]*ingest.StoredEvent, 0, 103)
	for value := 1; value <= 100; value++ {
		events = append(events, newEvent(
			fmt.Sprintf("percentile-rank-%03d", value),
			"rank",
			typedField("metric", typedSint(int64(value))),
		))
	}
	events = append(
		events,
		newEvent(
			"percentile-multivalue",
			"multivalue",
			typedField("metric", typedList(
				typedSint(1),
				typedSint(2),
				typedSint(2),
				typedSint(100),
				typedString("bad"),
				typedNull(),
				typedList(typedSint(999)),
				typedObject(typedField("nested", typedSint(1000))),
			)),
		),
		newEvent(
			"percentile-invalid",
			"invalid",
			typedField("metric", typedList(
				typedString("bad"),
				typedNull(),
				typedList(typedSint(999)),
				typedObject(typedField("nested", typedSint(1000))),
			)),
		),
		newEvent(
			"percentile-fixed-multivalue",
			"fixed",
			typedField("fixed_metric", typedList(
				typedSint(1),
				typedSint(2),
				typedSint(100),
			)),
		),
	)
	result, storeErr := store.Store(ctx, ingest.StoreBatch{
		TenantID:          "tenant",
		CollectorID:       collectorID,
		BatchID:           batchID,
		BatchSequence:     1,
		SourceBatchSHA256: testSourceBatchDigest(batchID),
		ReceivedAt:        indexTime,
		Events:            events,
	})
	if storeErr != nil {
		t.Fatalf("store percentile fixtures: %v", storeErr)
	}
	const expectedStoredEvents = 103
	if len(events) != expectedStoredEvents {
		t.Fatalf("percentile fixture count = %d, want %d", len(events), expectedStoredEvents)
	}
	if result.Accepted != expectedStoredEvents || result.Duplicate != 0 {
		t.Fatalf("store percentile fixtures result = %+v, want %d accepted", result, len(events))
	}
	visibilityCutoff, cutoffErr := store.VisibilityCutoff(ctx)
	if cutoffErr != nil {
		t.Fatalf("capture percentile visibility cutoff: %v", cutoffErr)
	}
	compile := func(source string) CompiledQuery {
		return compileIntegrationSPL(t, source, indexTime.Add(10*time.Second), visibilityCutoff)
	}
	base := `index=compiler source="stats-percentile"`
	assertOnePhysicalState := func(name string, compiled CompiledQuery) {
		t.Helper()
		actions := explainCompiledQuery(t, ctx, connection, explainActionsPrefix, compiled)
		physicalStates := 0
		for line := range strings.SplitSeq(actions, "\n") {
			if strings.Contains(line, "Function:") && strings.Contains(line, "quantilesGK") {
				physicalStates++
			}
		}
		if physicalStates != 1 {
			t.Fatalf("%s has %d physical GK states, want one:\n%s", name, physicalStates, actions)
		}
		if strings.Contains(actions, "ArrayJoin") {
			t.Fatalf("%s physical plan expands event rows:\n%s", name, actions)
		}
	}

	family := compile(
		base + ` percentile_group="rank" | stats ` +
			`p1(metric) AS q1 p50(metric) AS q50 p90(metric) AS q90 ` +
			`p95(metric) AS q95 p99(metric) AS q99 perc75(metric) AS q75`,
	)
	familyRows, queryErr := connection.Query(ctx, family.SQL, family.Args...)
	if queryErr != nil {
		t.Fatalf("execute percentile family: %v\nSQL: %s\nargs: %#v", queryErr, family.SQL, family.Args)
	}
	if types := familyRows.ColumnTypes(); len(types) != 6 {
		_ = familyRows.Close()
		t.Fatalf("percentile family column types = %#v", types)
	} else {
		for _, columnType := range types {
			if columnType.DatabaseTypeName() != "Nullable(Float64)" {
				_ = familyRows.Close()
				t.Fatalf("percentile family column types = %#v", types)
			}
		}
	}
	if !familyRows.Next() {
		_ = familyRows.Close()
		t.Fatalf("percentile family returned no row: %v", familyRows.Err())
	}
	var q1, q50, q90, q95, q99, q75 *float64
	if err := familyRows.Scan(&q1, &q50, &q90, &q95, &q99, &q75); err != nil {
		_ = familyRows.Close()
		t.Fatalf("scan percentile family: %v", err)
	}
	if familyRows.Next() {
		_ = familyRows.Close()
		t.Fatal("percentile family returned more than one row")
	}
	if err := familyRows.Err(); err != nil {
		_ = familyRows.Close()
		t.Fatalf("iterate percentile family: %v", err)
	}
	if err := familyRows.Close(); err != nil {
		t.Fatalf("close percentile family: %v", err)
	}
	assertApproximatePercentile(t, "p1", q1, 1, 2)
	assertApproximatePercentile(t, "p50", q50, 49, 51)
	assertApproximatePercentile(t, "p90", q90, 89, 91)
	assertApproximatePercentile(t, "p95", q95, 94, 96)
	assertApproximatePercentile(t, "p99", q99, 98, 100)
	assertApproximatePercentile(t, "p75", q75, 74, 76)
	if !(*q1 <= *q50 && *q50 <= *q75 && *q75 <= *q90 && *q90 <= *q95 && *q95 <= *q99) {
		t.Fatalf("percentile family is not monotonic: %v/%v/%v/%v/%v/%v", q1, q50, q75, q90, q95, q99)
	}
	assertOnePhysicalState("Dynamic percentile family", family)
	scalarFamily := compile(
		base + ` | eval scalar_metric=42 | stats ` +
			`p50(scalar_metric) AS q50 p90(scalar_metric) AS q90 p99(scalar_metric) AS q99`,
	)
	assertOnePhysicalState("scalar percentile family", scalarFamily)

	grouped := compile(base + ` | stats p75(metric) AS q75 BY percentile_group | sort percentile_group`)
	groupedRows, queryErr := connection.Query(ctx, grouped.SQL, grouped.Args...)
	if queryErr != nil {
		t.Fatalf("execute grouped percentiles: %v\nSQL: %s\nargs: %#v", queryErr, grouped.SQL, grouped.Args)
	}
	got := make(map[string]*float64, 4)
	for groupedRows.Next() {
		var group string
		var percentile *float64
		if err := groupedRows.Scan(&group, &percentile); err != nil {
			_ = groupedRows.Close()
			t.Fatalf("scan grouped percentile: %v", err)
		}
		got[group] = percentile
	}
	if err := groupedRows.Err(); err != nil {
		_ = groupedRows.Close()
		t.Fatalf("iterate grouped percentiles: %v", err)
	}
	if err := groupedRows.Close(); err != nil {
		t.Fatalf("close grouped percentiles: %v", err)
	}
	if len(got) != 4 || got["invalid"] != nil || got["fixed"] != nil {
		t.Fatalf("grouped percentile nulls = %#v, want invalid/fixed null", got)
	}
	assertApproximatePercentile(t, "grouped rank p75", got["rank"], 74, 76)
	assertApproximatePercentile(t, "multivalue duplicate p75", got["multivalue"], 2, 2)

	fixed := compile(
		base + ` percentile_group="fixed"` +
			` | stats values(fixed_metric) AS values | stats p75(values) AS q75`,
	)
	var fixedP75 *float64
	if err := connection.QueryRow(ctx, fixed.SQL, fixed.Args...).Scan(&fixedP75); err != nil {
		t.Fatalf("execute fixed multivalue percentile: %v\nSQL: %s\nargs: %#v", err, fixed.SQL, fixed.Args)
	}
	assertApproximatePercentile(t, "fixed multivalue p75", fixedP75, 100, 100)

	empty := compile(`index=compiler source="stats-percentile-absent" | stats p50(metric) AS q50`)
	var emptyP50 *float64
	if err := connection.QueryRow(ctx, empty.SQL, empty.Args...).Scan(&emptyP50); err != nil {
		t.Fatalf("execute empty global percentile: %v\nSQL: %s\nargs: %#v", err, empty.SQL, empty.Args)
	}
	if emptyP50 != nil {
		t.Fatalf("empty global percentile = %v, want null", emptyP50)
	}
}

func assertApproximatePercentile(t *testing.T, name string, got *float64, minimum, maximum float64) {
	t.Helper()
	if got == nil || math.IsNaN(*got) || math.IsInf(*got, 0) ||
		*got < minimum || *got > maximum {
		t.Fatalf("%s = %v, want finite value in [%g, %g]", name, got, minimum, maximum)
	}
}
