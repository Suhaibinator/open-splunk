package clickhouse

import (
	"context"
	"reflect"
	"strings"
	"testing"
	"time"

	clickhousedriver "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/chcol"
	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/ingest"
)

// testStatsExtremaAgainstClickHouse pins the runtime min/max contract against
// the same ClickHouse version production uses. It deliberately lives outside
// the already-large aggregate fixture so failures remain attributable.
func testStatsExtremaAgainstClickHouse(
	t *testing.T,
	ctx context.Context,
	store *Store,
	connection clickhousedriver.Conn,
	indexTime time.Time,
) {
	t.Helper()
	newEvent := func(id, group string, fields ...*opensplunkv1.TypedObjectField) *ingest.StoredEvent {
		fields = append([]*opensplunkv1.TypedObjectField{
			typedField("extrema_group", typedString(group)),
		}, fields...)
		event := compilerIntegrationEvent(id, "stats-extrema-host", "stats extrema fixture", indexTime, fields...)
		event.BatchID = "stats-extrema-batch"
		event.Event.Source = "stats-extrema"
		return event
	}
	events := []*ingest.StoredEvent{
		newEvent("extrema-numeric-a", "numeric", typedField("extrema_value", typedString("10"))),
		newEvent("extrema-numeric-b", "numeric", typedField("extrema_value", typedList(
			typedString("2"), typedString("01"), typedString("1.0"), typedNull(),
		))),
		newEvent("extrema-lexical-a", "lexical", typedField("extrema_value", typedString(""))),
		newEvent("extrema-lexical-b", "lexical", typedField("extrema_value", typedList(
			typedString("A"), typedString("a"), typedString("é"),
		))),
		newEvent("extrema-mixed-a", "mixed", typedField("extrema_value", typedList(
			typedString("20"), typedString("5"), typedNull(),
		))),
		newEvent("extrema-mixed-b", "mixed", typedField("extrema_value", typedString("z"))),
		newEvent("extrema-fallback", "fallback", typedField("extrema_value", typedList(
			typedString("3"), typedString("NaN"), typedString("1e9999"), typedString(" 2"),
		))),
		newEvent("extrema-zero", "zero", typedField("extrema_value", typedList(
			typedString("-0"), typedString("+0"), typedString("0.0"),
		))),
		newEvent("extrema-empty-missing", "empty"),
		newEvent("extrema-empty-null", "empty", typedField("extrema_value", typedNull())),
		newEvent("extrema-empty-list", "empty", typedField("extrema_value", typedList(typedNull()))),
		newEvent("extrema-poison-object", "poison", typedField(
			"extrema_poison", typedObject(typedField("child", typedString("secret"))),
		)),
		newEvent("extrema-poison-nested", "poison", typedField(
			"extrema_nested", typedList(typedString("safe"), typedList(typedString("secret"))),
		)),
		newEvent("extrema-incomplete-by", "ignored", typedField(
			"extrema_incomplete", typedObject(typedField("child", typedString("secret"))),
		)),
		newEvent("extrema-complete-by", "complete",
			typedField("required_group", typedString("present")),
			typedField("extrema_incomplete", typedString("7")),
		),
	}
	batch := ingest.StoreBatch{
		TenantID:          "tenant",
		CollectorID:       "collector",
		BatchID:           "stats-extrema-batch",
		BatchSequence:     11,
		SourceBatchSHA256: testSourceBatchDigest("stats-extrema-batch"),
		ReceivedAt:        indexTime,
		Events:            events,
	}
	if _, err := store.Store(ctx, batch); err != nil {
		t.Fatalf("store min/max fixtures: %v", err)
	}

	foreign := newEvent(
		"extrema-foreign-poison",
		"foreign",
		typedField("extrema_scope", typedObject(typedField("secret", typedString("hidden")))),
	)
	foreign.TenantID = "other-tenant"
	foreign.BatchID = "stats-extrema-foreign-batch"
	foreignBatch := ingest.StoreBatch{
		TenantID:          "other-tenant",
		CollectorID:       "collector",
		BatchID:           "stats-extrema-foreign-batch",
		BatchSequence:     12,
		SourceBatchSHA256: testSourceBatchDigest("stats-extrema-foreign-batch"),
		ReceivedAt:        indexTime,
		Events:            []*ingest.StoredEvent{foreign},
	}
	if _, err := store.Store(ctx, foreignBatch); err != nil {
		t.Fatalf("store cross-tenant min/max poison: %v", err)
	}

	visibilityCutoff, err := store.VisibilityCutoff(ctx)
	if err != nil {
		t.Fatalf("capture min/max visibility cutoff: %v", err)
	}
	compile := func(source string) CompiledQuery {
		return compileIntegrationSPL(t, source, indexTime.Add(10*time.Second), visibilityCutoff)
	}
	base := `index=compiler source="stats-extrema"`

	grouped := compile(base + ` | stats min(extrema_value) AS low max(extrema_value) AS high BY extrema_group | sort extrema_group`)
	rows, err := connection.Query(ctx, grouped.SQL, grouped.Args...)
	if err != nil {
		t.Fatalf("execute grouped min/max: %v\nSQL: %s\nargs: %#v", err, grouped.SQL, grouped.Args)
	}
	type extremaRow struct {
		group string
		low   any
		high  any
	}
	var got []extremaRow
	for rows.Next() {
		var group string
		var low, high chcol.Dynamic
		if err := rows.Scan(&group, &low, &high); err != nil {
			_ = rows.Close()
			t.Fatalf("scan grouped min/max: %v", err)
		}
		got = append(got, extremaRow{group: group, low: low.Any(), high: high.Any()})
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		t.Fatalf("iterate grouped min/max: %v", err)
	}
	if err := rows.Close(); err != nil {
		t.Fatalf("close grouped min/max rows: %v", err)
	}
	want := []extremaRow{
		{group: "complete", low: nil, high: nil},
		{group: "empty", low: nil, high: nil},
		{group: "fallback", low: float64(3), high: "NaN"},
		{group: "ignored", low: nil, high: nil},
		{group: "lexical", low: "", high: "é"},
		{group: "mixed", low: float64(5), high: "z"},
		{group: "numeric", low: float64(1), high: float64(10)},
		{group: "poison", low: nil, high: nil},
		{group: "zero", low: float64(0), high: float64(0)},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("grouped min/max = %#v, want %#v", got, want)
	}

	for name, source := range map[string]string{
		"flattened object": base + ` event_id=extrema-poison-object | stats min(extrema_poison) AS low`,
		"nested list":      base + ` event_id=extrema-poison-nested | stats max(extrema_nested) AS high`,
	} {
		query := compile(source)
		queryErr := connection.QueryRow(ctx, query.SQL, query.Args...).Scan(new(chcol.Dynamic))
		if queryErr == nil || !strings.Contains(queryErr.Error(), UnsupportedStatsMeasureValueMarker) {
			t.Fatalf("%s error = %v, want guarded unsupported-value marker", name, queryErr)
		}
	}

	eligible := compile(base + ` (event_id=extrema-incomplete-by OR event_id=extrema-complete-by) | stats min(extrema_incomplete) AS low BY required_group`)
	var requiredGroup string
	var eligibleLow chcol.Dynamic
	if err := connection.QueryRow(ctx, eligible.SQL, eligible.Args...).Scan(&requiredGroup, &eligibleLow); err != nil {
		t.Fatalf("execute incomplete-BY min/max guard: %v\nSQL: %s", err, eligible.SQL)
	}
	if requiredGroup != "present" || eligibleLow.Any() != float64(7) {
		t.Fatalf("incomplete-BY min = %q/%#v, want present/7", requiredGroup, eligibleLow.Any())
	}

	scoped := compile(base + ` | stats min(extrema_scope) AS low`)
	var scopedLow chcol.Dynamic
	if err := connection.QueryRow(ctx, scoped.SQL, scoped.Args...).Scan(&scopedLow); err != nil {
		t.Fatalf("cross-tenant poison affected min/max: %v\nSQL: %s", err, scoped.SQL)
	}
	if scopedLow.Any() != nil {
		t.Fatalf("cross-tenant scoped min = %#v, want null", scopedLow.Any())
	}

	projected := compile(base + ` | fields extrema_group | stats min(extrema_value) AS low`)
	var projectedLow chcol.Dynamic
	if err := connection.QueryRow(ctx, projected.SQL, projected.Args...).Scan(&projectedLow); err != nil {
		t.Fatalf("execute projected-away min: %v\nSQL: %s", err, projected.SQL)
	}
	if projectedLow.Any() != nil {
		t.Fatalf("projected-away min = %#v, want null", projectedLow.Any())
	}

	globalEmpty := compile(base + ` event_id=does-not-exist | stats min(extrema_value) AS low`)
	var emptyLow chcol.Dynamic
	if err := connection.QueryRow(ctx, globalEmpty.SQL, globalEmpty.Args...).Scan(&emptyLow); err != nil {
		t.Fatalf("execute global empty min: %v\nSQL: %s", err, globalEmpty.SQL)
	}
	if emptyLow.Any() != nil {
		t.Fatalf("global empty min = %#v, want null", emptyLow.Any())
	}
	groupedEmpty := compile(base + ` event_id=does-not-exist | stats min(extrema_value) AS low BY extrema_group`)
	var groupedCount uint64
	if err := connection.QueryRow(ctx, "SELECT count() FROM ("+groupedEmpty.SQL+")", groupedEmpty.Args...).Scan(&groupedCount); err != nil {
		t.Fatalf("execute grouped empty min: %v\nSQL: %s", err, groupedEmpty.SQL)
	}
	if groupedCount != 0 {
		t.Fatalf("grouped empty min rows = %d, want 0", groupedCount)
	}

	binned := compile(base + ` extrema_group=mixed | stats min(extrema_value) AS low | bin low span=2 | table low`)
	var binnedLow chcol.Dynamic
	if err := connection.QueryRow(ctx, binned.SQL, binned.Args...).Scan(&binnedLow); err != nil {
		t.Fatalf("execute downstream min bin: %v\nSQL: %s", err, binned.SQL)
	}
	if binnedLow.Any() != float64(4) {
		t.Fatalf("downstream binned min = %#v, want 4", binnedLow.Any())
	}
	reaggregated := compile(base + ` | stats min(extrema_value) AS low BY extrema_group | stats max(low) AS largest`)
	var largest chcol.Dynamic
	if err := connection.QueryRow(ctx, reaggregated.SQL, reaggregated.Args...).Scan(&largest); err != nil {
		t.Fatalf("execute downstream min/max reaggregation: %v\nSQL: %s", err, reaggregated.SQL)
	}
	if largest.Any() != "" {
		t.Fatalf("reaggregated max = %#v, want empty lexical String", largest.Any())
	}

	nativeTime := compile(base + ` | stats min(_time) AS earliest max(_time) AS latest`)
	var earliest, latest time.Time
	if err := connection.QueryRow(ctx, nativeTime.SQL, nativeTime.Args...).Scan(&earliest, &latest); err != nil {
		t.Fatalf("execute native time extrema: %v\nSQL: %s", err, nativeTime.SQL)
	}
	wantEventTime := events[0].Event.EventTime.AsTime()
	if !earliest.Equal(wantEventTime) || !latest.Equal(wantEventTime) {
		t.Fatalf("native time extrema = %s/%s, want %s", earliest, latest, wantEventTime)
	}

	shared := compile(base + ` | stats min(extrema_value) AS low max(extrema_value) AS high min(extrema_value) AS low_again`)
	actions := explainCompiledQuery(t, ctx, connection, "EXPLAIN actions=1 ", shared)
	if got := strings.Count(actions, "Function: argMinArray("); got != 1 {
		t.Fatalf("repeated min has %d physical aggregate states, want one:\n%s", got, actions)
	}
	if got := strings.Count(actions, "Function: argMaxArray("); got != 1 {
		t.Fatalf("min/max has %d physical max states, want one:\n%s", got, actions)
	}
	if strings.Contains(actions, "ArrayJoin") {
		t.Fatalf("min/max physical plan expands event rows:\n%s", actions)
	}
	if strings.Count(shared.SQL, `AS "__os_measure_extrema_0"`) != 1 ||
		strings.Contains(shared.SQL, `__os_measure_extrema_1`) {
		t.Fatalf("min/max normalization is not shared:\n%s", shared.SQL)
	}
}
