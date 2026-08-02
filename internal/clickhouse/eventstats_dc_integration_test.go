package clickhouse

import (
	"context"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	clickhousedriver "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/Suhaibinator/open-splunk/internal/eventfields"
)

// testEventStatsDistinctCountAgainstClickHouse reuses the transforming-stats
// fixture so the two commands are proved against exactly the same canonical
// scalar, multivalue, missing, null, and unsupported-container inputs. The
// eventstats row-boundary fixture is also reused to keep this production-image
// test from ingesting another large corpus.
func testEventStatsDistinctCountAgainstClickHouse(
	t *testing.T,
	ctx context.Context,
	store *Store,
	connection clickhousedriver.Conn,
	indexTime time.Time,
) {
	t.Helper()

	visibilityCutoff, err := store.VisibilityCutoff(ctx)
	if err != nil {
		t.Fatalf("capture eventstats dc visibility cutoff: %v", err)
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
	compileBoundary := func(source string) CompiledQuery {
		t.Helper()
		return compileIntegrationSPLForIndex(
			t,
			source,
			indexTime.Add(10*time.Second),
			visibilityCutoff,
			"eventstats-boundary",
		)
	}
	base := `index=compiler source="stats-sum-avg"`

	// Generate the exact-set boundary with the same native JSON/Dynamic values
	// as production ingestion, but bypass the independent per-event request
	// ceiling. Parsing a JSON string here would infer Array(String), whereas the
	// collector's typed-list contract deliberately stores Array(Dynamic).
	members := make(
		[]clickhousedriver.Dynamic,
		MaximumStatsDistinctValuesPerGroup+1,
	)
	members[0] = clickhousedriver.NewDynamicWithType(true, "Bool")
	for index := 1; index < len(members); index++ {
		members[index] = clickhousedriver.NewDynamicWithType(
			strconv.Itoa(index-1),
			"String",
		)
	}
	batch, prepareErr := connection.PrepareBatch(ctx, `
		INSERT INTO open_splunk.events
		(
			event_id, tenant_id, index_name, event_time, index_time,
			host, source, sourcetype, severity, raw, raw_encoding,
			fields, field_names, field_types, field_metadata_version,
			collector_id, batch_id, batch_sequence, expires_at,
			visibility_seq
		)`)
	if prepareErr != nil {
		t.Fatalf("prepare eventstats dc boundaries: %v", prepareErr)
	}
	appendDistinctBoundary := func(
		id string,
		boundaryMembers []clickhousedriver.Dynamic,
	) {
		t.Helper()
		document := clickhousedriver.NewJSON()
		document.SetValueAtPath(
			"eventstats_dc_boundary",
			clickhousedriver.NewDynamicWithType(
				boundaryMembers,
				"Array(Dynamic)",
			),
		)
		appendErr := batch.Append(
			id,
			"tenant",
			"compiler",
			indexTime,
			indexTime,
			"eventstats-dc-boundary-host",
			id,
			"eventstats-dc-boundary",
			uint8(0),
			"eventstats dc boundary",
			uint8(1),
			document,
			[]string{"eventstats_dc_boundary"},
			[]uint8{uint8(eventfields.StoredValueTypeList)},
			eventfields.CurrentFieldMetadataVersion,
			"eventstats-dc-boundary",
			id,
			uint64(len(boundaryMembers)),
			MaximumSearchTime(),
			visibilityCutoff,
		)
		if appendErr != nil {
			_ = batch.Abort()
			t.Fatalf(
				"append eventstats dc boundary %d: %v",
				len(boundaryMembers),
				appendErr,
			)
		}
	}
	appendDistinctBoundary(
		"eventstats-dc-distinct-exact",
		members[:MaximumStatsDistinctValuesPerGroup],
	)
	appendDistinctBoundary(
		"eventstats-dc-distinct-over",
		members,
	)
	if sendErr := batch.Send(); sendErr != nil {
		t.Fatalf("send eventstats dc boundaries: %v", sendErr)
	}
	var inserted uint64
	if countErr := connection.QueryRow(
		ctx,
		`SELECT count() FROM open_splunk.events WHERE event_id IN (?, ?)`,
		"eventstats-dc-distinct-exact",
		"eventstats-dc-distinct-over",
	).Scan(&inserted); countErr != nil || inserted != 2 {
		t.Fatalf(
			"verify eventstats dc boundaries: count=%d err=%v",
			inserted,
			countErr,
		)
	}
	global := compile(
		base + ` | eventstats dc(distinct_value) AS distinct_values` +
			` | head 1 | table distinct_values`,
	)
	var globalCount uint64
	if queryErr := connection.QueryRow(
		ctx,
		global.SQL,
		global.Args...,
	).Scan(&globalCount); queryErr != nil {
		t.Fatalf(
			"execute global eventstats dc: %v\nSQL: %s\nargs: %#v",
			queryErr,
			global.SQL,
			global.Args,
		)
	}
	if globalCount != 7 {
		t.Fatalf("global eventstats dc = %d, want 7", globalCount)
	}

	exactDistinct := compile(
		`index=compiler source="eventstats-dc-distinct-exact"` +
			` | eventstats dc(eventstats_dc_boundary) AS distinct_values` +
			` | head 1 | table distinct_values`,
	)
	if queryErr := connection.QueryRow(
		ctx,
		exactDistinct.SQL,
		exactDistinct.Args...,
	).Scan(&globalCount); queryErr != nil {
		t.Fatalf(
			"execute exact-distinct-boundary eventstats dc: %v\nSQL: %s\nargs: %#v",
			queryErr,
			exactDistinct.SQL,
			exactDistinct.Args,
		)
	}
	if globalCount != MaximumStatsDistinctValuesPerGroup {
		t.Fatalf(
			"exact-distinct-boundary eventstats dc = %d, want %d",
			globalCount,
			MaximumStatsDistinctValuesPerGroup,
		)
	}

	overDistinct := compile(
		`index=compiler source="eventstats-dc-distinct-over"` +
			` | eventstats dc(eventstats_dc_boundary) AS distinct_values` +
			` | head 1 | table distinct_values`,
	)
	overflowErr := connection.QueryRow(
		ctx,
		overDistinct.SQL,
		overDistinct.Args...,
	).Scan(&globalCount)
	if overflowErr == nil ||
		!strings.Contains(overflowErr.Error(), ExactDistinctLimitMarker) {
		t.Fatalf(
			"eventstats dc distinct overflow error = %v, want stable marker\nSQL: %s",
			overflowErr,
			overDistinct.SQL,
		)
	}

	hiddenGroupedOverflow := compile(
		`index=compiler source="eventstats-dc-distinct-over"` +
			` | eventstats dc(eventstats_dc_boundary) AS discarded BY host` +
			` | where host="definitely-absent" | table event_id | head 1`,
	)
	var ignoredBoundary string
	hiddenOverflowErr := connection.QueryRow(
		ctx,
		hiddenGroupedOverflow.SQL,
		hiddenGroupedOverflow.Args...,
	).Scan(&ignoredBoundary)
	if hiddenOverflowErr == nil ||
		!strings.Contains(hiddenOverflowErr.Error(), ExactDistinctLimitMarker) {
		t.Fatalf(
			"hidden grouped eventstats dc overflow error = %v, want stable marker\nSQL: %s",
			hiddenOverflowErr,
			hiddenGroupedOverflow.SQL,
		)
	}

	type groupedRow struct {
		id    string
		count *uint64
	}
	collect := func(name string, query CompiledQuery) []groupedRow {
		t.Helper()
		rows, executeErr := connection.Query(ctx, query.SQL, query.Args...)
		if executeErr != nil {
			t.Fatalf(
				"execute %s: %v\nSQL: %s\nargs: %#v",
				name,
				executeErr,
				query.SQL,
				query.Args,
			)
		}
		defer func() {
			if closeErr := rows.Close(); closeErr != nil {
				t.Fatalf("close %s rows: %v", name, closeErr)
			}
		}()
		var result []groupedRow
		for rows.Next() {
			var row groupedRow
			if scanErr := rows.Scan(&row.id, &row.count); scanErr != nil {
				t.Fatalf("scan %s: %v", name, scanErr)
			}
			result = append(result, row)
		}
		if rowsErr := rows.Err(); rowsErr != nil {
			t.Fatalf("iterate %s: %v", name, rowsErr)
		}
		return result
	}
	grouped := compile(
		base + ` | eventstats dc(distinct_value) AS distinct_values BY aggregate_group` +
			` | sort event_id | table event_id distinct_values`,
	)
	if got, want := collect("grouped eventstats dc", grouped), []groupedRow{
		{id: "sum-avg-a-array", count: uint64PointerForIntegration(6)},
		{id: "sum-avg-a-int", count: uint64PointerForIntegration(6)},
		{id: "sum-avg-a-missing", count: uint64PointerForIntegration(6)},
		{id: "sum-avg-a-string", count: uint64PointerForIntegration(6)},
		{id: "sum-avg-b-bad", count: uint64PointerForIntegration(1)},
		{id: "sum-avg-b-null", count: uint64PointerForIntegration(1)},
	}; !reflect.DeepEqual(got, want) {
		t.Fatalf("grouped eventstats dc = %#v, want %#v", got, want)
	}

	// The only dc_object container is on a row whose official_group is
	// incomplete. It is outside the grouped measure's eligible scope: the row
	// stays visible without an annotation, while complete groups publish zero.
	incompleteGroup := compile(
		base + ` | eventstats dc(dc_object) AS object_kinds BY official_group` +
			` | sort event_id | table event_id object_kinds`,
	)
	if got, want := collect("incomplete eventstats dc group", incompleteGroup), []groupedRow{
		{id: "sum-avg-a-array"},
		{id: "sum-avg-a-int", count: uint64PointerForIntegration(0)},
		{id: "sum-avg-a-missing"},
		{id: "sum-avg-a-string", count: uint64PointerForIntegration(0)},
		{id: "sum-avg-b-bad", count: uint64PointerForIntegration(0)},
		{id: "sum-avg-b-null"},
	}; !reflect.DeepEqual(got, want) {
		t.Fatalf("incomplete eventstats dc group = %#v, want %#v", got, want)
	}

	projected := compile(
		base + ` | fields event_id | eventstats dc(distinct_value) AS distinct_values` +
			` | head 1 | table distinct_values`,
	)
	if queryErr := connection.QueryRow(
		ctx,
		projected.SQL,
		projected.Args...,
	).Scan(&globalCount); queryErr != nil {
		t.Fatalf(
			"execute projected-away eventstats dc: %v\nSQL: %s\nargs: %#v",
			queryErr,
			projected.SQL,
			projected.Args,
		)
	}
	if globalCount != 0 {
		t.Fatalf("projected-away eventstats dc = %d, want 0", globalCount)
	}

	stacked := compile(
		base + ` | eventstats dc(distinct_value) AS group_distinct BY aggregate_group` +
			` | eventstats dc(group_distinct) AS distinct_group_counts` +
			` | head 1 | table distinct_group_counts`,
	)
	if queryErr := connection.QueryRow(
		ctx,
		stacked.SQL,
		stacked.Args...,
	).Scan(&globalCount); queryErr != nil {
		t.Fatalf(
			"execute stacked eventstats dc: %v\nSQL: %s\nargs: %#v",
			queryErr,
			stacked.SQL,
			stacked.Args,
		)
	}
	if globalCount != 2 {
		t.Fatalf("stacked eventstats dc = %d, want 2", globalCount)
	}

	for _, field := range []string{"dc_object", "dc_empty_object", "dc_nested"} {
		hiddenPoison := compile(
			base + ` | eventstats dc(` + field + `) AS discarded` +
				` | table event_id | search event_id="definitely-absent"`,
		)
		var ignored string
		poisonErr := connection.QueryRow(
			ctx,
			hiddenPoison.SQL,
			hiddenPoison.Args...,
		).Scan(&ignored)
		if poisonErr == nil ||
			!strings.Contains(poisonErr.Error(), UnsupportedStatsMeasureValueMarker) {
			t.Fatalf(
				"hidden eventstats dc poison %s error = %v, want stable marker\nSQL: %s",
				field,
				poisonErr,
				hiddenPoison.SQL,
			)
		}
	}

	// Exactly 10,000 rows remain valid for this measure; the existing sentinel
	// fixture proves the next row fails before a downstream head can hide it.
	exactRows := compileBoundary(
		`index=eventstats-boundary host="in"` +
			` | eventstats dc(event_id) AS distinct_events` +
			` | head 1 | table distinct_events`,
	)
	if queryErr := connection.QueryRow(
		ctx,
		exactRows.SQL,
		exactRows.Args...,
	).Scan(&globalCount); queryErr != nil {
		t.Fatalf(
			"execute exact-row-boundary eventstats dc: %v\nSQL: %s\nargs: %#v",
			queryErr,
			exactRows.SQL,
			exactRows.Args,
		)
	}
	if globalCount != MaximumEventStatsInputRows {
		t.Fatalf(
			"exact-row-boundary eventstats dc = %d, want %d",
			globalCount,
			MaximumEventStatsInputRows,
		)
	}

	overRows := compileBoundary(
		`index=eventstats-boundary` +
			` | eventstats dc(event_id) AS distinct_events` +
			` | head 1 | table distinct_events`,
	)
	rowOverflowErr := connection.QueryRow(
		ctx,
		overRows.SQL,
		overRows.Args...,
	).Scan(&globalCount)
	if rowOverflowErr == nil ||
		!strings.Contains(rowOverflowErr.Error(), EventStatsInputLimitMarker) {
		t.Fatalf(
			"eventstats dc row overflow error = %v, want stable marker\nSQL: %s",
			rowOverflowErr,
			overRows.SQL,
		)
	}
}
