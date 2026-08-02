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
)

// testEventStatsMaximumAgainstClickHouse reuses the deliberately adversarial
// minimum fixture installed immediately before it. That keeps the production-
// image suite small while making the direction-specific winner, exact Decimal,
// native type, grouped presence, validation, and input-fence contracts visible.
func testEventStatsMaximumAgainstClickHouse(
	t *testing.T,
	ctx context.Context,
	store *Store,
	connection clickhousedriver.Conn,
	indexTime time.Time,
) {
	t.Helper()

	visibilityCutoff, err := store.VisibilityCutoff(ctx)
	if err != nil {
		t.Fatalf("capture eventstats max visibility cutoff: %v", err)
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
	base := `index=compiler source="eventstats-min-fixture"`

	// The winning "z" is inside a multivalue row whose minimum is numeric 1.
	// This therefore distinguishes a genuine maximum fold from merely swapping
	// the outer aggregate around the minimum row fold. The same query proves a
	// grouped min/max stack and a downstream predicate over the maximum.
	stacked := compile(
		base +
			` | eventstats min(eventstats_min_value) AS low BY eventstats_min_group` +
			` | eventstats max(eventstats_min_value) AS high BY eventstats_min_group` +
			` | where high="z" | sort event_id | table event_id low high`,
	)
	type extremaRow struct {
		id   string
		low  any
		high any
	}
	rows, err := connection.Query(ctx, stacked.SQL, stacked.Args...)
	if err != nil {
		t.Fatalf(
			"execute stacked eventstats min/max: %v\nSQL: %s\nargs: %#v",
			err,
			stacked.SQL,
			stacked.Args,
		)
	}
	var got []extremaRow
	for rows.Next() {
		var id string
		var low, high chcol.Dynamic
		if scanErr := rows.Scan(&id, &low, &high); scanErr != nil {
			_ = rows.Close()
			t.Fatalf("scan stacked eventstats min/max: %v", scanErr)
		}
		got = append(got, extremaRow{id: id, low: low.Any(), high: high.Any()})
	}
	if rowsErr := rows.Err(); rowsErr != nil {
		_ = rows.Close()
		t.Fatalf("iterate stacked eventstats min/max: %v", rowsErr)
	}
	if closeErr := rows.Close(); closeErr != nil {
		t.Fatalf("close stacked eventstats min/max rows: %v", closeErr)
	}
	want := []extremaRow{
		{id: "eventstats-min-01-int", low: float64(1), high: "z"},
		{id: "eventstats-min-02-multivalue", low: float64(1), high: "z"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("stacked eventstats min/max = %#v, want %#v", got, want)
	}

	// Reverse the stack while using canonical event_id as the fixed String
	// input. The maximum must use scalar extrema ordering, and the following
	// minimum must preserve that published winner on every row.
	reverse := compile(
		base + ` | eventstats max(event_id) AS highest_id` +
			` | eventstats min(highest_id) AS repeated` +
			` | head 1 | table highest_id repeated`,
	)
	var highestID, repeated chcol.Dynamic
	if queryErr := connection.QueryRow(
		ctx,
		reverse.SQL,
		reverse.Args...,
	).Scan(&highestID, &repeated); queryErr != nil {
		t.Fatalf(
			"execute fixed String eventstats max/min: %v\nSQL: %s\nargs: %#v",
			queryErr,
			reverse.SQL,
			reverse.Args,
		)
	}
	const wantHighestID = "eventstats-min-11-complete-group"
	if highestID.Any() != wantHighestID || repeated.Any() != wantHighestID {
		t.Fatalf(
			"fixed String eventstats max/min = %#v/%#v, want %q/%q",
			highestID.Any(),
			repeated.Any(),
			wantHighestID,
			wantHighestID,
		)
	}

	fixedMultivalue := compile(
		base + ` | stats values(event_id) AS event_ids` +
			` | eventstats max(event_ids) AS highest_id | table highest_id`,
	)
	var highestFixedMember chcol.Dynamic
	if queryErr := connection.QueryRow(
		ctx,
		fixedMultivalue.SQL,
		fixedMultivalue.Args...,
	).Scan(&highestFixedMember); queryErr != nil {
		t.Fatalf(
			"execute fixed multivalue eventstats max: %v\nSQL: %s\nargs: %#v",
			queryErr,
			fixedMultivalue.SQL,
			fixedMultivalue.Args,
		)
	}
	if highestFixedMember.Any() != wantHighestID {
		t.Fatalf(
			"fixed multivalue eventstats max = %#v, want %q",
			highestFixedMember.Any(),
			wantHighestID,
		)
	}

	groupedSource := base +
		` | eventstats max(eventstats_min_value) AS high BY eventstats_min_group`
	groupedPlan := buildIntegrationPlan(
		t,
		groupedSource,
		indexTime.Add(10*time.Second),
		visibilityCutoff,
	)
	summary, compileErr := (Compiler{}).CompileFieldSummary(
		groupedPlan,
		FieldSummarySpec{
			FieldName:             "high",
			MaximumValues:         10,
			MaximumDistinctValues: 10,
			MaximumValueBytes:     64,
		},
	)
	if compileErr != nil {
		t.Fatalf("compile grouped eventstats max field summary: %v", compileErr)
	}
	var rowKind uint8
	var fieldName string
	var observedTypes []uint8
	var present, nulls, missing, total uint64
	var valueType uint8
	var encodedValue string
	var valueCount uint64
	var metadataInvalid, unsupported, oversized uint8
	if queryErr := connection.QueryRow(
		ctx,
		summary.SQL,
		summary.Args...,
	).Scan(
		&rowKind,
		&fieldName,
		&observedTypes,
		&present,
		&nulls,
		&missing,
		&total,
		&valueType,
		&encodedValue,
		&valueCount,
		&metadataInvalid,
		&unsupported,
		&oversized,
	); queryErr != nil {
		t.Fatalf(
			"execute grouped eventstats max field summary: %v\nSQL: %s\nargs: %#v",
			queryErr,
			summary.SQL,
			summary.Args,
		)
	}
	if rowKind != 0 || fieldName != "high" || metadataInvalid != 0 ||
		unsupported != 0 || oversized != 0 {
		t.Fatalf(
			"grouped eventstats max control = kind %d field %q invalid %d/%d/%d",
			rowKind,
			fieldName,
			metadataInvalid,
			unsupported,
			oversized,
		)
	}
	if present != 9 || nulls != 3 || missing != 2 || total != 11 {
		t.Fatalf(
			"grouped eventstats max presence = %d/%d/%d/%d, want 9/3/2/11",
			present,
			nulls,
			missing,
			total,
		)
	}

	const exactMaximum = "9007199254740993"
	exact := compile(
		base + ` | eventstats max(eventstats_min_exact) AS high | head 1 | table high`,
	)
	exactControl := `SELECT
		dynamicType(high),
		if(dynamicType(high) = 'Map(String, String)',
			dynamicElement(high, 'Map(String, String)')[concat(char(0), 'open_splunk_type')], ''),
		if(dynamicType(high) = 'Map(String, String)',
			dynamicElement(high, 'Map(String, String)')[concat(char(0), 'open_splunk_value')], toString(high))
		FROM (` + exact.SQL + `)`
	var exactPhysical, exactTag, exactValue string
	if queryErr := connection.QueryRow(
		ctx,
		exactControl,
		exact.Args...,
	).Scan(&exactPhysical, &exactTag, &exactValue); queryErr != nil {
		t.Fatalf("execute exact Decimal eventstats max: %v\nSQL: %s", queryErr, exact.SQL)
	}
	if exactPhysical != "Map(String, String)" ||
		exactTag != "decimal/v1" || exactValue != exactMaximum {
		t.Fatalf(
			"exact Decimal eventstats max = %q/%q/%q, want Map/decimal/v1/%s",
			exactPhysical,
			exactTag,
			exactValue,
			exactMaximum,
		)
	}

	severity := compile(
		base + ` | eventstats max(severity) AS high | head 1 | table high`,
	)
	var highSeverity *uint8
	if queryErr := connection.QueryRow(
		ctx,
		severity.SQL,
		severity.Args...,
	).Scan(&highSeverity); queryErr != nil {
		t.Fatalf("execute native UInt8 eventstats max: %v\nSQL: %s", queryErr, severity.SQL)
	}
	wantSeverity := uint8(opensplunkv1.LogSeverity_LOG_SEVERITY_WARN)
	if highSeverity == nil || *highSeverity != wantSeverity {
		t.Fatalf("native UInt8 eventstats max = %v, want %d", highSeverity, wantSeverity)
	}

	latest := compile(
		base + ` | eventstats max(_time) AS latest | head 1 | table latest`,
	)
	var latestTime *time.Time
	if queryErr := connection.QueryRow(
		ctx,
		latest.SQL,
		latest.Args...,
	).Scan(&latestTime); queryErr != nil {
		t.Fatalf("execute native time eventstats max: %v\nSQL: %s", queryErr, latest.SQL)
	}
	wantLatest := indexTime.Add(10 * time.Nanosecond)
	if latestTime == nil || !latestTime.Equal(wantLatest) {
		t.Fatalf("native time eventstats max = %v, want %s", latestTime, wantLatest)
	}

	boolean := compile(
		base + ` | eval selected=if(event_id="eventstats-min-11-complete-group", true, false)` +
			` | eventstats max(selected) AS any_selected | head 1 | table any_selected`,
	)
	var anySelected *bool
	if queryErr := connection.QueryRow(
		ctx,
		boolean.SQL,
		boolean.Args...,
	).Scan(&anySelected); queryErr != nil {
		t.Fatalf("execute native Bool eventstats max: %v\nSQL: %s", queryErr, boolean.SQL)
	}
	if anySelected == nil || !*anySelected {
		t.Fatalf("native Bool eventstats max = %v, want true", anySelected)
	}

	poison := compile(
		base + ` | eventstats max(eventstats_min_object) AS high` +
			` | search event_id="not-present" | table high`,
	)
	poisonErr := executeCompiledExpectingNoRows(ctx, connection, poison)
	if poisonErr == nil ||
		!strings.Contains(poisonErr.Error(), UnsupportedStatsMeasureValueMarker) {
		t.Fatalf(
			"unsupported-container eventstats max error = %v, want atomic marker",
			poisonErr,
		)
	}

	overflow := compileIntegrationSPLForIndex(
		t,
		`index=eventstats-boundary source="eventstats-boundary"`+
			` | eventstats max(eventstats_max_missing) AS high`+
			` | search event_id="not-present" | table high`,
		indexTime.Add(10*time.Second),
		visibilityCutoff,
		"eventstats-boundary",
	)
	overflowErr := executeCompiledExpectingNoRows(ctx, connection, overflow)
	if overflowErr == nil ||
		!strings.Contains(overflowErr.Error(), EventStatsInputLimitMarker) {
		t.Fatalf(
			"10,001-row eventstats max error = %v, want atomic limit failure",
			overflowErr,
		)
	}
}
