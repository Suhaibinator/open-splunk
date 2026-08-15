package clickhouse

import (
	"context"
	"math"
	"reflect"
	"strings"
	"testing"
	"time"

	clickhousedriver "github.com/ClickHouse/clickhouse-go/v2"
)

// testEventStatsPercentilesAgainstClickHouse reuses the numeric eventstats
// corpus and bounded-row corpus installed earlier in TestStoreAgainstClickHouse.
// That keeps the production-image proof compact while exercising Dynamic and
// fixed multivalue normalization, grouping, composition, physical-state shape,
// and the atomic 10,001-row fence.
func testEventStatsPercentilesAgainstClickHouse(
	t *testing.T,
	ctx context.Context,
	store *Store,
	connection clickhousedriver.Conn,
	indexTime time.Time,
) {
	t.Helper()

	visibilityCutoff, err := store.VisibilityCutoff(ctx)
	if err != nil {
		t.Fatalf("capture eventstats percentile visibility cutoff: %v", err)
	}
	compile := func(source string) CompiledQuery {
		t.Helper()
		return compileIntegrationSPL(
			t,
			source,
			indexTime.Add(10*time.Second),
			visibilityCutoff,
		)
	}

	const base = `index=compiler source="eventstats-sum-fixture"`
	global := compile(
		base + ` | eventstats p50(eventstats_sum_value) AS median` +
			` | sort event_id | table event_id median`,
	)
	alias := compile(
		base + ` | eventstats perc50(eventstats_sum_value) AS median` +
			` | sort event_id | table event_id median`,
	)
	grouped := compile(
		base + ` | eventstats p75(eventstats_sum_value) AS q75` +
			` BY eventstats_sum_group | sort event_id | table event_id q75`,
	)
	fixed := compile(
		base + ` | stats values(eventstats_sum_value) AS fixed_values` +
			` | eventstats p75(fixed_values) AS q75 | table q75`,
	)
	statsOracle := compile(
		base + ` | stats p50(eventstats_sum_value) AS median | table median`,
	)
	projectedAway := compile(
		base + ` | fields - eventstats_sum_value` +
			` | eventstats p95(eventstats_sum_value) AS p95_value` +
			` | head 1 | table p95_value`,
	)
	canonicalTime := compile(
		base + ` | eventstats p50(_time) AS median_time` +
			` | head 1 | table median_time`,
	)
	stacked := compile(
		base + ` | eventstats p50(eventstats_sum_value) AS median` +
			` | eventstats perc90(eventstats_sum_value) AS q90` +
			` | head 1 | table median q90`,
	)
	overflow := compileIntegrationSPLForIndex(
		t,
		`index=eventstats-boundary source="eventstats-boundary"`+
			` | eventstats p50(eventstats_percentile_missing) AS discarded`+
			` | search definitely_missing=value | table discarded`,
		indexTime.Add(10*time.Second),
		visibilityCutoff,
		"eventstats-boundary",
	)

	assertSQLShape := func(
		name string,
		query CompiledQuery,
		levels []string,
	) {
		t.Helper()
		states := len(levels)
		for _, level := range levels {
			if !strings.Contains(
				query.SQL,
				`quantilesGKOrNullArray(100, `+level+`)(`,
			) {
				t.Fatalf("%s SQL is missing exact GK level %s:\n%s", name, level, query.SQL)
			}
		}
		if got := strings.Count(query.SQL, `quantilesGKOrNullArray(`); got != states {
			t.Fatalf("%s aggregate states = %d, want %d:\n%s", name, got, states, query.SQL)
		}
		if got := strings.Count(query.SQL, ` AS "__os_eventstats_measure_`); got != states {
			t.Fatalf("%s numeric normalizations = %d, want %d:\n%s", name, got, states, query.SQL)
		}
		if got := strings.Count(query.SQL, `FROM "open_splunk"."events"`); got != 1 {
			t.Fatalf("%s source scans = %d, want 1:\n%s", name, got, query.SQL)
		}
		if got, want := strings.Count(query.SQL, "?"), len(query.Args); got != want {
			t.Fatalf("%s placeholder count = %d, args = %d", name, got, want)
		}
		for _, forbidden := range []string{"ARRAY JOIN", "arrayJoin(", "groupArray("} {
			if strings.Contains(query.SQL, forbidden) {
				t.Fatalf("%s SQL contains row-expanding %q:\n%s", name, forbidden, query.SQL)
			}
		}
	}
	assertSQLShape("global p50", global, []string{"0.5"})
	assertSQLShape("grouped p75", grouped, []string{"0.75"})
	assertSQLShape("stacked p50/perc90", stacked, []string{"0.5", "0.9"})
	if alias.SQL != global.SQL ||
		!reflect.DeepEqual(alias.Args, global.Args) ||
		!reflect.DeepEqual(alias.OutputFields, global.OutputFields) ||
		alias.SparseFields != global.SparseFields {
		t.Fatalf(
			"perc50 execution contract differs from p50:\n"+
				"p50: %s %#v %#v sparse=%t\n"+
				"perc50: %s %#v %#v sparse=%t",
			global.SQL,
			global.Args,
			global.OutputFields,
			global.SparseFields,
			alias.SQL,
			alias.Args,
			alias.OutputFields,
			alias.SparseFields,
		)
	}
	if got := strings.Count(stacked.SQL, ` AS MATERIALIZED (`); got != 1 {
		t.Fatalf("stacked percentile materialized inputs = %d, want first leaf only:\n%s", got, stacked.SQL)
	}

	collectRows := func(
		name string,
		query CompiledQuery,
	) []eventStatsNullableFloatRow {
		t.Helper()
		return collectEventStatsNullableFloatRows(
			t,
			ctx,
			connection,
			name,
			query,
		)
	}
	collectOne := func(name string, query CompiledQuery) *float64 {
		t.Helper()
		return collectEventStatsNullableFloat(
			t,
			ctx,
			connection,
			name,
			query,
		)
	}
	assertPresence := func(
		name string,
		source string,
		field string,
		want eventStatsFieldPresence,
	) {
		t.Helper()
		logical := buildIntegrationPlan(
			t,
			source,
			indexTime.Add(10*time.Second),
			visibilityCutoff,
		)
		got := collectEventStatsFieldPresence(
			t,
			ctx,
			connection,
			logical,
			field,
		)
		if got != want {
			t.Fatalf("%s presence = %#v, want %#v", name, got, want)
		}
	}

	ids := []string{
		"eventstats-sum-01-int",
		"eventstats-sum-02-float",
		"eventstats-sum-03-string",
		"eventstats-sum-04-multivalue",
		"eventstats-sum-05-missing",
		"eventstats-sum-06-nonnumeric",
		"eventstats-sum-07-null",
		"eventstats-sum-08-missing-group",
		"eventstats-sum-09-null-group",
	}
	globalRows := collectRows("global p50", global)
	if len(globalRows) != len(ids) {
		t.Fatalf("global p50 rows = %d, want %d", len(globalRows), len(ids))
	}
	assertApproximatePercentile(t, "global p50", globalRows[0].value, 2.5, 8)
	for index, row := range globalRows {
		if row.id != ids[index] || row.value == nil ||
			*row.value != *globalRows[0].value {
			t.Fatalf("global p50 row %d = %#v, want id %q and shared %v", index, row, ids[index], globalRows[0].value)
		}
	}
	statsMedian := collectOne("stats p50 oracle", statsOracle)
	if statsMedian == nil || globalRows[0].value == nil ||
		*statsMedian != *globalRows[0].value {
		t.Fatalf(
			"eventstats p50 = %v, stats p50 over the same scope = %v",
			globalRows[0].value,
			statsMedian,
		)
	}
	if projected := collectOne("projected-away percentile", projectedAway); projected != nil {
		t.Fatalf("projected-away percentile = %v, want present null", projected)
	}
	assertPresence(
		"projected-away percentile",
		base+` | fields - eventstats_sum_value`+
			` | eventstats p95(eventstats_sum_value) AS p95_value`,
		"p95_value",
		eventStatsFieldPresence{present: 9, nulls: 9, total: 9},
	)

	groupedRows := collectRows("grouped p75", grouped)
	if len(groupedRows) != len(ids) {
		t.Fatalf("grouped p75 rows = %d, want %d", len(groupedRows), len(ids))
	}
	assertApproximatePercentile(t, "group A p75", groupedRows[0].value, 8, 20.5)
	for index, row := range groupedRows {
		if row.id != ids[index] {
			t.Fatalf("grouped p75 row %d id = %q, want %q", index, row.id, ids[index])
		}
		if index < 5 {
			if row.value == nil || *row.value != *groupedRows[0].value {
				t.Fatalf("group A p75 row %d = %v, want shared %v", index, row.value, groupedRows[0].value)
			}
		} else if row.value != nil {
			t.Fatalf("ineligible or all-nonnumeric group row %d p75 = %v, want null", index, row.value)
		}
	}
	assertPresence(
		"grouped p75",
		base+` | eventstats p75(eventstats_sum_value) AS q75`+
			` BY eventstats_sum_group`,
		"q75",
		eventStatsFieldPresence{present: 7, nulls: 2, missing: 2, total: 9},
	)

	fixedP75 := collectOne("fixed multivalue p75", fixed)
	assertApproximatePercentile(t, "fixed multivalue p75", fixedP75, 8, 20.5)
	medianTime := collectOne("canonical time p50", canonicalTime)
	wantTime := float64(time.Date(
		2026,
		time.July,
		21,
		3,
		4,
		5,
		123456789,
		time.FixedZone("event-offset", 5*60*60),
	).UnixNano()) / 1e9
	if medianTime == nil || math.Abs(*medianTime-wantTime) > 1e-6 {
		t.Fatalf("canonical time p50 = %v, want %g", medianTime, wantTime)
	}

	stackedRows, queryErr := connection.Query(ctx, stacked.SQL, stacked.Args...)
	if queryErr != nil {
		t.Fatalf("execute stacked p50/perc90: %v\nSQL: %s\nargs: %#v", queryErr, stacked.SQL, stacked.Args)
	}
	stackedTypes := stackedRows.ColumnTypes()
	if len(stackedTypes) != 2 ||
		stackedTypes[0].DatabaseTypeName() != "Nullable(Float64)" ||
		stackedTypes[1].DatabaseTypeName() != "Nullable(Float64)" {
		_ = stackedRows.Close()
		t.Fatalf("stacked percentile column types = %#v", stackedTypes)
	}
	if !stackedRows.Next() {
		rowsErr := stackedRows.Err()
		_ = stackedRows.Close()
		t.Fatalf("stacked percentile returned no row: %v", rowsErr)
	}
	var median, q90 *float64
	if scanErr := stackedRows.Scan(&median, &q90); scanErr != nil {
		_ = stackedRows.Close()
		t.Fatalf("scan stacked percentile: %v", scanErr)
	}
	if stackedRows.Next() {
		_ = stackedRows.Close()
		t.Fatal("stacked percentile returned more than one row")
	}
	if rowsErr := stackedRows.Err(); rowsErr != nil {
		_ = stackedRows.Close()
		t.Fatalf("iterate stacked percentile: %v", rowsErr)
	}
	if closeErr := stackedRows.Close(); closeErr != nil {
		t.Fatalf("close stacked percentile: %v", closeErr)
	}
	assertApproximatePercentile(t, "stacked p50", median, 2.5, 8)
	assertApproximatePercentile(t, "stacked perc90", q90, 10, 20.5)
	if *median > *q90 {
		t.Fatalf("stacked percentiles are not monotonic: p50=%g p90=%g", *median, *q90)
	}

	assertPhysicalStates := func(name string, query CompiledQuery, want int) {
		t.Helper()
		actions := explainCompiledQuery(t, ctx, connection, explainActionsPrefix, query)
		states := 0
		for line := range strings.SplitSeq(actions, "\n") {
			if strings.Contains(line, "Function:") && strings.Contains(line, "quantilesGK") {
				states++
			}
		}
		if states != want {
			t.Fatalf("%s physical GK states = %d, want %d:\n%s", name, states, want, actions)
		}
		if strings.Contains(actions, "ArrayJoin") {
			t.Fatalf("%s physical plan expands event rows:\n%s", name, actions)
		}
	}
	assertPhysicalStates("global p50", global, 1)
	assertPhysicalStates("grouped p75", grouped, 1)
	// The later global stage has two CTE consumers, so it evaluates the earlier
	// p50 state twice and its own q90 state once. This is the exact bounded
	// fanout charged by validateChronologicalGraphAmplification; the SQL still
	// contains only two aggregate definitions and one physical event scan.
	assertPhysicalStates("stacked p50/perc90", stacked, 3)

	overflowErr := executeCompiledExpectingNoRows(ctx, connection, overflow)
	if overflowErr == nil || !strings.Contains(overflowErr.Error(), EventStatsInputLimitMarker) {
		t.Fatalf("hidden 10,001-row percentile error = %v, want atomic limit marker", overflowErr)
	}
}
