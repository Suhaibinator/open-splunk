package clickhouse

import (
	"context"
	"reflect"
	"strings"
	"testing"
	"time"

	clickhousedriver "github.com/ClickHouse/clickhouse-go/v2"
	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/ingest"
)

// testEventStatsSumAgainstClickHouse pins numeric normalization, nullable
// sums, row preservation, grouping, downstream composition, and the existing
// 10,000-row eventstats fence to the production ClickHouse image.
func testEventStatsSumAgainstClickHouse(
	t *testing.T,
	ctx context.Context,
	store *Store,
	connection clickhousedriver.Conn,
	indexTime time.Time,
) {
	t.Helper()

	newEvent := func(
		id, source string,
		fields ...*opensplunkv1.TypedObjectField,
	) *ingest.StoredEvent {
		event := compilerIntegrationEvent(
			id,
			"eventstats-sum-host",
			"eventstats sum fixture",
			indexTime,
			fields...,
		)
		event.BatchID = "eventstats-sum-batch"
		event.Event.Source = source
		return event
	}
	const fixtureSource = "eventstats-sum-fixture"
	events := []*ingest.StoredEvent{
		newEvent(
			"eventstats-sum-01-int",
			fixtureSource,
			typedField("eventstats_sum_group", typedString("A")),
			typedField("eventstats_sum_value", typedSint(10)),
			typedField("eventstats_sum_ineligible", typedBool(true)),
		),
		newEvent(
			"eventstats-sum-02-float",
			fixtureSource,
			typedField("eventstats_sum_group", typedString("A")),
			typedField("eventstats_sum_value", typedDouble(0.5)),
			typedField("eventstats_sum_ineligible", typedString("not-a-number")),
		),
		newEvent(
			"eventstats-sum-03-string",
			fixtureSource,
			typedField("eventstats_sum_group", typedString("A")),
			typedField("eventstats_sum_value", typedString("20.5")),
			typedField("eventstats_sum_ineligible", typedNull()),
		),
		newEvent(
			"eventstats-sum-04-multivalue",
			fixtureSource,
			typedField("eventstats_sum_group", typedString("A")),
			typedField(
				"eventstats_sum_value",
				typedList(
					typedSint(1),
					typedString("2.5"),
					typedString("bad"),
					typedNull(),
				),
			),
			typedField(
				"eventstats_sum_ineligible",
				typedObject(typedField("child", typedSint(99))),
			),
		),
		newEvent(
			"eventstats-sum-05-missing",
			fixtureSource,
			typedField("eventstats_sum_group", typedString("A")),
			typedField("eventstats_sum_decimal", typedDecimal("3.25")),
		),
		newEvent(
			"eventstats-sum-06-nonnumeric",
			fixtureSource,
			typedField("eventstats_sum_group", typedString("B")),
			typedField("eventstats_sum_value", typedString("bad")),
		),
		newEvent(
			"eventstats-sum-07-null",
			fixtureSource,
			typedField("eventstats_sum_group", typedString("B")),
			typedField("eventstats_sum_value", typedNull()),
		),
		newEvent(
			"eventstats-sum-08-missing-group",
			fixtureSource,
			typedField("eventstats_sum_value", typedSint(7)),
		),
		newEvent(
			"eventstats-sum-09-null-group",
			fixtureSource,
			typedField("eventstats_sum_group", typedNull()),
			typedField("eventstats_sum_value", typedSint(8)),
		),
		newEvent(
			"eventstats-sum-same-tenant-poison",
			"eventstats-sum-poison",
			typedField("eventstats_sum_group", typedString("A")),
			typedField("eventstats_sum_value", typedSint(2_000)),
		),
	}
	if _, err := store.Store(ctx, ingest.StoreBatch{
		TenantID:           "tenant",
		CollectorID:        "collector",
		BatchID:            "eventstats-sum-batch",
		BatchSequence:      82,
		OriginalEventCount: uint32(len(events)),
		SourceBatchSHA256:  testSourceBatchDigest("eventstats-sum-batch"),
		ReceivedAt:         indexTime,
		Events:             events,
	}); err != nil {
		t.Fatalf("store eventstats sum fixtures: %v", err)
	}

	foreign := newEvent(
		"eventstats-sum-foreign-poison",
		fixtureSource,
		typedField("eventstats_sum_group", typedString("A")),
		typedField("eventstats_sum_value", typedSint(1_000)),
	)
	foreign.TenantID = "other-tenant"
	foreign.BatchID = "eventstats-sum-foreign-batch"
	if _, err := store.Store(ctx, ingest.StoreBatch{
		TenantID:           "other-tenant",
		CollectorID:        "collector",
		BatchID:            "eventstats-sum-foreign-batch",
		BatchSequence:      82,
		OriginalEventCount: 1,
		SourceBatchSHA256: testSourceBatchDigest(
			"eventstats-sum-foreign-batch",
		),
		ReceivedAt: indexTime,
		Events:     []*ingest.StoredEvent{foreign},
	}); err != nil {
		t.Fatalf("store foreign eventstats sum poison: %v", err)
	}

	visibilityCutoff, err := store.VisibilityCutoff(ctx)
	if err != nil {
		t.Fatalf("capture eventstats sum visibility cutoff: %v", err)
	}
	compile := func(source string) CompiledQuery {
		return compileIntegrationSPL(
			t,
			source,
			indexTime.Add(10*time.Second),
			visibilityCutoff,
		)
	}
	base := `index=compiler source="` + fixtureSource + `"`

	type sumRow struct {
		id    string
		total *float64
	}
	collect := func(name string, query CompiledQuery) []sumRow {
		t.Helper()
		rows, queryErr := connection.Query(ctx, query.SQL, query.Args...)
		if queryErr != nil {
			t.Fatalf(
				"execute %s: %v\nSQL: %s\nargs: %#v",
				name,
				queryErr,
				query.SQL,
				query.Args,
			)
		}
		types := rows.ColumnTypes()
		if len(types) != 2 ||
			types[0].DatabaseTypeName() != "String" ||
			types[1].DatabaseTypeName() != "Nullable(Float64)" {
			typeNames := make([]string, len(types))
			for index, columnType := range types {
				typeNames[index] = columnType.DatabaseTypeName()
			}
			_ = rows.Close()
			t.Fatalf("%s column types = %#v", name, typeNames)
		}
		var got []sumRow
		for rows.Next() {
			var row sumRow
			if scanErr := rows.Scan(&row.id, &row.total); scanErr != nil {
				_ = rows.Close()
				t.Fatalf("scan %s: %v", name, scanErr)
			}
			got = append(got, row)
		}
		if rowsErr := rows.Err(); rowsErr != nil {
			_ = rows.Close()
			t.Fatalf("iterate %s: %v", name, rowsErr)
		}
		if closeErr := rows.Close(); closeErr != nil {
			t.Fatalf("close %s rows: %v", name, closeErr)
		}
		return got
	}
	collectSingleTotal := func(name string, query CompiledQuery) *float64 {
		t.Helper()
		rows, queryErr := connection.Query(ctx, query.SQL, query.Args...)
		if queryErr != nil {
			t.Fatalf(
				"execute %s: %v\nSQL: %s\nargs: %#v",
				name,
				queryErr,
				query.SQL,
				query.Args,
			)
		}
		types := rows.ColumnTypes()
		if len(types) != 1 || types[0].DatabaseTypeName() != "Nullable(Float64)" {
			typeNames := make([]string, len(types))
			for index, columnType := range types {
				typeNames[index] = columnType.DatabaseTypeName()
			}
			_ = rows.Close()
			t.Fatalf("%s column types = %#v", name, typeNames)
		}
		if !rows.Next() {
			rowsErr := rows.Err()
			_ = rows.Close()
			t.Fatalf("%s returned no row: %v", name, rowsErr)
		}
		var total *float64
		if scanErr := rows.Scan(&total); scanErr != nil {
			_ = rows.Close()
			t.Fatalf("scan %s: %v", name, scanErr)
		}
		if rows.Next() {
			_ = rows.Close()
			t.Fatalf("%s returned multiple rows", name)
		}
		if rowsErr := rows.Err(); rowsErr != nil {
			_ = rows.Close()
			t.Fatalf("iterate %s: %v", name, rowsErr)
		}
		if closeErr := rows.Close(); closeErr != nil {
			t.Fatalf("close %s rows: %v", name, closeErr)
		}
		return total
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
	globalTotal := 49.5
	globalWant := make([]sumRow, 0, len(ids))
	for _, id := range ids {
		globalWant = append(globalWant, sumRow{id: id, total: &globalTotal})
	}
	globalGot := collect(
		"scoped global eventstats sum",
		compile(
			base+` | eventstats sum(eventstats_sum_value) AS total | sort event_id | table event_id total`,
		),
	)
	if !reflect.DeepEqual(globalGot, globalWant) {
		t.Fatalf(
			"scoped global eventstats sum = %#v, want %#v",
			globalGot,
			globalWant,
		)
	}

	decimalTotal := 3.25
	decimalWant := make([]sumRow, 0, len(ids))
	for _, id := range ids {
		decimalWant = append(decimalWant, sumRow{id: id, total: &decimalTotal})
	}
	decimalGot := collect(
		"tagged Decimal eventstats sum",
		compile(
			base+` | eventstats sum(eventstats_sum_decimal) AS total | sort event_id | table event_id total`,
		),
	)
	if !reflect.DeepEqual(decimalGot, decimalWant) {
		t.Fatalf(
			"tagged Decimal eventstats sum = %#v, want %#v",
			decimalGot,
			decimalWant,
		)
	}

	fixedTotal := collectSingleTotal(
		"fixed multivalue eventstats sum",
		compile(
			base+` | stats values(eventstats_sum_value) AS fixed_values | eventstats sum(fixed_values) AS total | table total`,
		),
	)
	if fixedTotal == nil || *fixedTotal != globalTotal {
		t.Fatalf(
			"fixed multivalue eventstats sum = %v, want %v",
			fixedTotal,
			globalTotal,
		)
	}

	groupATotal := 34.5
	groupedWant := []sumRow{
		{id: ids[0], total: &groupATotal},
		{id: ids[1], total: &groupATotal},
		{id: ids[2], total: &groupATotal},
		{id: ids[3], total: &groupATotal},
		{id: ids[4], total: &groupATotal},
		{id: ids[5]},
		{id: ids[6]},
		{id: ids[7]},
		{id: ids[8]},
	}
	groupedGot := collect(
		"grouped eventstats sum",
		compile(
			base+` | eventstats sum(eventstats_sum_value) AS total BY eventstats_sum_group | sort event_id | table event_id total`,
		),
	)
	if !reflect.DeepEqual(groupedGot, groupedWant) {
		t.Fatalf(
			"grouped eventstats sum = %#v, want %#v; missing/null BY rows must retain no aggregate value",
			groupedGot,
			groupedWant,
		)
	}

	groupedSummaryPlan := buildIntegrationPlan(
		t,
		base+` | eventstats sum(eventstats_sum_value) AS total BY eventstats_sum_group`,
		indexTime.Add(10*time.Second),
		visibilityCutoff,
	)
	groupedSummary, err := (Compiler{}).CompileFieldSummary(
		groupedSummaryPlan,
		FieldSummarySpec{
			FieldName:             "total",
			MaximumValues:         10,
			MaximumDistinctValues: 10,
			MaximumValueBytes:     64,
		},
	)
	if err != nil {
		t.Fatalf("compile grouped eventstats sum field summary: %v", err)
	}
	groupedSummaryControl := `SELECT ` +
		quoteIdentifier(FieldSummaryEventCountColumn) + `, ` +
		quoteIdentifier(FieldSummaryNullCountColumn) + `, ` +
		quoteIdentifier(FieldSummaryMissingCountColumn) + `, ` +
		quoteIdentifier(FieldSummaryTotalEventCountColumn) +
		` FROM (` + groupedSummary.SQL + `) WHERE ` +
		quoteIdentifier(FieldSummaryRowKindColumn) + ` = 0`
	var present, nulls, missing, total uint64
	if err := connection.QueryRow(
		ctx,
		groupedSummaryControl,
		groupedSummary.Args...,
	).Scan(&present, &nulls, &missing, &total); err != nil {
		t.Fatalf(
			"execute grouped eventstats sum field summary: %v\nSQL: %s\nargs: %#v",
			err,
			groupedSummary.SQL,
			groupedSummary.Args,
		)
	}
	if present != 7 || nulls != 2 || missing != 2 || total != 9 {
		t.Fatalf(
			"grouped eventstats sum presence = present:%d null:%d missing:%d total:%d, want 7/2/2/9",
			present,
			nulls,
			missing,
			total,
		)
	}

	allIneligibleGot := collect(
		"all-ineligible eventstats sum",
		compile(
			base+` | eventstats sum(eventstats_sum_ineligible) AS total | sort event_id | table event_id total`,
		),
	)
	if len(allIneligibleGot) != len(ids) {
		t.Fatalf(
			"all-ineligible eventstats sum rows = %d, want %d",
			len(allIneligibleGot),
			len(ids),
		)
	}
	for index, row := range allIneligibleGot {
		if row.id != ids[index] || row.total != nil {
			t.Fatalf(
				"all-ineligible eventstats sum row %d = %#v, want id %q with null total",
				index,
				row,
				ids[index],
			)
		}
	}

	downstreamWant := []sumRow{
		{id: ids[0], total: &globalTotal},
		{id: ids[3], total: &globalTotal},
	}
	downstreamGot := collect(
		"downstream-filtered eventstats sum",
		compile(
			base+` | eventstats sum(eventstats_sum_value) AS total | where event_id="`+ids[3]+`" OR event_id="`+ids[0]+`" | sort event_id | table event_id total`,
		),
	)
	if !reflect.DeepEqual(downstreamGot, downstreamWant) {
		t.Fatalf(
			"downstream-filtered eventstats sum = %#v, want %#v",
			downstreamGot,
			downstreamWant,
		)
	}

	projectedGot := collect(
		"projected-away eventstats sum input",
		compile(
			base+` | fields event_id | eventstats sum(eventstats_sum_value) AS total | sort event_id | head 1 | table event_id total`,
		),
	)
	if want := []sumRow{{id: ids[0]}}; !reflect.DeepEqual(projectedGot, want) {
		t.Fatalf(
			"projected-away eventstats sum = %#v, want %#v",
			projectedGot,
			want,
		)
	}

	overflow := compileIntegrationSPLForIndex(
		t,
		`index=eventstats-boundary source="eventstats-boundary" | eventstats sum(eventstats_sum_missing) AS total | search event_id="not-present" | table total`,
		indexTime.Add(10*time.Second),
		visibilityCutoff,
		"eventstats-boundary",
	)
	overflowErr := executeCompiledExpectingNoRows(ctx, connection, overflow)
	if overflowErr == nil ||
		!strings.Contains(overflowErr.Error(), EventStatsInputLimitMarker) {
		t.Fatalf(
			"10,001-row eventstats sum error = %v, want atomic input-limit failure",
			overflowErr,
		)
	}
}
