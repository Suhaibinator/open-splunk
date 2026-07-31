package clickhouse

import (
	"context"
	"errors"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	clickhousedriver "github.com/ClickHouse/clickhouse-go/v2"
	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/ingest"
)

// testEventStatsAgainstClickHouse pins the row-preserving count contract,
// sparse-group presence, lexical Dynamic grouping, scoping, composition, and
// runtime guards against the exact ClickHouse image used in production.
func testEventStatsAgainstClickHouse(
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
			"eventstats-host",
			"eventstats fixture",
			indexTime,
			fields...,
		)
		event.BatchID = "eventstats-batch"
		event.Event.Source = source
		return event
	}
	events := []*ingest.StoredEvent{
		newEvent(
			"eventstats-1",
			"eventstats-fixture",
			typedField("eventstats_group", typedString("500")),
			typedField("eventstats_existing", typedString("shadowed")),
			typedField("eventstats_value", typedString("scalar")),
			typedField(
				"eventstats_letters",
				typedList(typedString("ALPHA"), typedString("BETA")),
			),
		),
		newEvent(
			"eventstats-2",
			"eventstats-fixture",
			typedField("eventstats_group", typedSint(500)),
			typedField(
				"eventstats_value",
				typedList(
					typedString("immediate"),
					typedNull(),
					typedObject(typedField("child", typedString("container"))),
				),
			),
			typedField("eventstats_letters", typedList()),
		),
		newEvent(
			"eventstats-3",
			"eventstats-fixture",
			typedField("eventstats_group", typedBool(true)),
			typedField(
				"eventstats_value",
				typedObject(typedField("child", typedString("flattened"))),
			),
		),
		newEvent(
			"eventstats-4",
			"eventstats-fixture",
			typedField("eventstats_value", typedList()),
		),
		newEvent(
			"eventstats-5",
			"eventstats-fixture",
			typedField("eventstats_group", typedNull()),
			typedField("eventstats_value", typedNull()),
		),
		newEvent(
			"eventstats-poison",
			"eventstats-poison",
			typedField(
				"eventstats_group",
				typedObject(typedField("child", typedString("hidden"))),
			),
		),
	}
	if _, err := store.Store(ctx, ingest.StoreBatch{
		TenantID:           "tenant",
		CollectorID:        "collector",
		BatchID:            "eventstats-batch",
		BatchSequence:      80,
		OriginalEventCount: uint32(len(events)),
		SourceBatchSHA256:  testSourceBatchDigest("eventstats-batch"),
		ReceivedAt:         indexTime,
		Events:             events,
	}); err != nil {
		t.Fatalf("store eventstats fixtures: %v", err)
	}

	foreign := newEvent(
		"eventstats-foreign-poison",
		"eventstats-fixture",
		typedField(
			"eventstats_group",
			typedObject(typedField("child", typedString("foreign"))),
		),
	)
	foreign.TenantID = "other-tenant"
	foreign.BatchID = "eventstats-foreign-batch"
	if _, err := store.Store(ctx, ingest.StoreBatch{
		TenantID:           "other-tenant",
		CollectorID:        "collector",
		BatchID:            "eventstats-foreign-batch",
		BatchSequence:      81,
		OriginalEventCount: 1,
		SourceBatchSHA256:  testSourceBatchDigest("eventstats-foreign-batch"),
		ReceivedAt:         indexTime,
		Events:             []*ingest.StoredEvent{foreign},
	}); err != nil {
		t.Fatalf("store foreign eventstats poison: %v", err)
	}

	const (
		boundaryChunk     = 1_000
		boundaryCollector = "eventstats-boundary-collector"
		boundaryIndex     = "eventstats-boundary"
	)
	boundaryRows := int(MaximumEventStatsInputRows + 1)
	for start := 0; start < boundaryRows; start += boundaryChunk {
		end := min(start+boundaryChunk, boundaryRows)
		batchID := "eventstats-boundary-batch-" +
			strconv.Itoa(start/boundaryChunk)
		batchEvents := make(
			[]*ingest.StoredEvent,
			0,
			end-start,
		)
		for index := start; index < end; index++ {
			event := newEvent(
				"eventstats-boundary-"+strconv.Itoa(index),
				"eventstats-boundary",
			)
			event.CollectorID = boundaryCollector
			event.BatchID = batchID
			event.Event.IndexName = boundaryIndex
			event.Event.Host = "in"
			if index == boundaryRows-1 {
				event.Event.Host = "extra"
			}
			batchEvents = append(batchEvents, event)
		}
		if _, err := store.Store(ctx, ingest.StoreBatch{
			TenantID:           "tenant",
			CollectorID:        boundaryCollector,
			BatchID:            batchID,
			BatchSequence:      uint64(start/boundaryChunk) + 1,
			OriginalEventCount: uint32(len(batchEvents)),
			SourceBatchSHA256:  testSourceBatchDigest(batchID),
			ReceivedAt:         indexTime,
			Events:             batchEvents,
		}); err != nil {
			t.Fatalf("store eventstats boundary batch %s: %v", batchID, err)
		}
	}

	visibilityCutoff, err := store.VisibilityCutoff(ctx)
	if err != nil {
		t.Fatalf("capture eventstats visibility cutoff: %v", err)
	}
	compile := func(source string) CompiledQuery {
		return compileIntegrationSPL(
			t,
			source,
			indexTime.Add(10*time.Second),
			visibilityCutoff,
		)
	}
	compileBoundary := func(source string) CompiledQuery {
		return compileIntegrationSPLForIndex(
			t,
			source,
			indexTime.Add(10*time.Second),
			visibilityCutoff,
			boundaryIndex,
		)
	}
	base := `index=compiler source="eventstats-fixture"`

	eventstatsIDs := []string{
		"eventstats-1",
		"eventstats-2",
		"eventstats-3",
		"eventstats-4",
		"eventstats-5",
	}
	assertIDTotals := func(
		name string,
		query CompiledQuery,
		wantIDs []string,
		wantTotal uint64,
	) {
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
			types[1].DatabaseTypeName() != "UInt64" {
			typeNames := make([]string, len(types))
			for index, columnType := range types {
				typeNames[index] = columnType.DatabaseTypeName()
			}
			_ = rows.Close()
			t.Fatalf("%s column types = %#v", name, typeNames)
		}
		var gotIDs []string
		for rows.Next() {
			var id string
			var total uint64
			if scanErr := rows.Scan(&id, &total); scanErr != nil {
				_ = rows.Close()
				t.Fatalf("scan %s: %v", name, scanErr)
			}
			gotIDs = append(gotIDs, id)
			if total != wantTotal {
				_ = rows.Close()
				t.Fatalf(
					"%s %s total = %d, want %d",
					name,
					id,
					total,
					wantTotal,
				)
			}
		}
		if rowsErr := rows.Err(); rowsErr != nil {
			_ = rows.Close()
			t.Fatalf("iterate %s: %v", name, rowsErr)
		}
		if closeErr := rows.Close(); closeErr != nil {
			t.Fatalf("close %s rows: %v", name, closeErr)
		}
		if !reflect.DeepEqual(gotIDs, wantIDs) {
			t.Fatalf("%s IDs = %v, want %v", name, gotIDs, wantIDs)
		}
	}
	assertIDTotals(
		"global eventstats",
		compile(base+` | eventstats count AS total | sort event_id | table event_id total`),
		eventstatsIDs,
		5,
	)
	assertIDTotals(
		"global eventstats count(field)",
		compile(
			base+` | eventstats count(eventstats_value) AS occurrences | sort event_id | table event_id occurrences`,
		),
		eventstatsIDs,
		4,
	)
	assertIDTotals(
		"global eventstats count(nonempty calculated homogeneous array)",
		compile(
			base+` event_id="eventstats-1" | eval lowered=lower(eventstats_letters) | eventstats count(lowered) AS occurrences | table event_id occurrences`,
		),
		[]string{"eventstats-1"},
		2,
	)
	assertIDTotals(
		"global eventstats count(empty calculated homogeneous array)",
		compile(
			base+` event_id="eventstats-2" | eval lowered=lower(eventstats_letters) | eventstats count(lowered) AS occurrences | table event_id occurrences`,
		),
		[]string{"eventstats-2"},
		0,
	)

	grouped := compile(
		base + ` | eventstats count AS peers BY eventstats_group | eventstats count AS total | sort event_id | table event_id peers total`,
	)
	groupedRows, err := connection.Query(ctx, grouped.SQL, grouped.Args...)
	if err != nil {
		t.Fatalf("execute grouped eventstats: %v\nSQL: %s\nargs: %#v", err, grouped.SQL, grouped.Args)
	}
	type groupedResult struct {
		id    string
		peers *uint64
		total uint64
	}
	var groupedGot []groupedResult
	for groupedRows.Next() {
		var row groupedResult
		if scanErr := groupedRows.Scan(
			&row.id,
			&row.peers,
			&row.total,
		); scanErr != nil {
			_ = groupedRows.Close()
			t.Fatalf("scan grouped eventstats: %v", scanErr)
		}
		groupedGot = append(groupedGot, row)
	}
	if rowsErr := groupedRows.Err(); rowsErr != nil {
		_ = groupedRows.Close()
		t.Fatalf("iterate grouped eventstats: %v", rowsErr)
	}
	if closeErr := groupedRows.Close(); closeErr != nil {
		t.Fatalf("close grouped eventstats rows: %v", closeErr)
	}
	groupTwo := uint64(2)
	groupOne := uint64(1)
	groupedWant := []groupedResult{
		{id: "eventstats-1", peers: &groupTwo, total: 5},
		{id: "eventstats-2", peers: &groupTwo, total: 5},
		{id: "eventstats-3", peers: &groupOne, total: 5},
		{id: "eventstats-4", total: 5},
		{id: "eventstats-5", total: 5},
	}
	if !reflect.DeepEqual(groupedGot, groupedWant) {
		t.Fatalf("grouped eventstats = %#v, want %#v", groupedGot, groupedWant)
	}

	type groupedValueResult struct {
		id          string
		occurrences *uint64
	}
	collectGroupedValues := func(
		name string,
		query CompiledQuery,
	) []groupedValueResult {
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
		var got []groupedValueResult
		for rows.Next() {
			var row groupedValueResult
			if scanErr := rows.Scan(&row.id, &row.occurrences); scanErr != nil {
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
	groupThree := uint64(3)
	groupedValueWant := []groupedValueResult{
		{id: "eventstats-1", occurrences: &groupThree},
		{id: "eventstats-2", occurrences: &groupThree},
		{id: "eventstats-3", occurrences: &groupOne},
		{id: "eventstats-4"},
		{id: "eventstats-5"},
	}
	groupedValueGot := collectGroupedValues(
		"grouped eventstats count(field)",
		compile(
			base+` | eventstats count(eventstats_value) AS occurrences BY eventstats_group | sort event_id | table event_id occurrences`,
		),
	)
	if !reflect.DeepEqual(groupedValueGot, groupedValueWant) {
		t.Fatalf(
			"grouped eventstats count(field) = %#v, want %#v",
			groupedValueGot,
			groupedValueWant,
		)
	}

	groupZero := uint64(0)
	groupedZeroWant := []groupedValueResult{
		{id: "eventstats-1", occurrences: &groupZero},
		{id: "eventstats-2", occurrences: &groupZero},
		{id: "eventstats-3", occurrences: &groupZero},
		{id: "eventstats-4"},
		{id: "eventstats-5"},
	}
	groupedZeroGot := collectGroupedValues(
		"grouped zero eventstats count(field)",
		compile(
			base+` | eventstats count(eventstats_zero) AS occurrences BY eventstats_group | sort event_id | table event_id occurrences`,
		),
	)
	if !reflect.DeepEqual(groupedZeroGot, groupedZeroWant) {
		t.Fatalf(
			"grouped zero eventstats count(field) = %#v, want %#v",
			groupedZeroGot,
			groupedZeroWant,
		)
	}

	calculatedGroupedWant := []groupedValueResult{
		{id: "eventstats-1", occurrences: &groupTwo},
		{id: "eventstats-2", occurrences: &groupZero},
		{id: "eventstats-3", occurrences: &groupZero},
		{id: "eventstats-4", occurrences: &groupZero},
		{id: "eventstats-5", occurrences: &groupZero},
	}
	calculatedGroupedGot := collectGroupedValues(
		"grouped eventstats count(calculated homogeneous array)",
		compile(
			base+` | eval lowered=lower(eventstats_letters) | eventstats count(lowered) AS occurrences BY event_id | sort event_id | table event_id occurrences`,
		),
	)
	if !reflect.DeepEqual(calculatedGroupedGot, calculatedGroupedWant) {
		t.Fatalf(
			"grouped calculated-array eventstats = %#v, want %#v",
			calculatedGroupedGot,
			calculatedGroupedWant,
		)
	}

	assertTotals := func(
		name string,
		query CompiledQuery,
		wantRows int,
		wantTotal uint64,
	) {
		t.Helper()
		rows, queryErr := connection.Query(ctx, query.SQL, query.Args...)
		if queryErr != nil {
			t.Fatalf("execute %s: %v\nSQL: %s\nargs: %#v", name, queryErr, query.SQL, query.Args)
		}
		count := 0
		for rows.Next() {
			var total uint64
			if scanErr := rows.Scan(&total); scanErr != nil {
				_ = rows.Close()
				t.Fatalf("scan %s: %v", name, scanErr)
			}
			count++
			if total != wantTotal {
				_ = rows.Close()
				t.Fatalf("%s row %d total = %d, want %d", name, count, total, wantTotal)
			}
		}
		if rowsErr := rows.Err(); rowsErr != nil {
			_ = rows.Close()
			t.Fatalf("iterate %s: %v", name, rowsErr)
		}
		if closeErr := rows.Close(); closeErr != nil {
			t.Fatalf("close %s rows: %v", name, closeErr)
		}
		if count != wantRows {
			t.Fatalf("%s rows = %d, want %d", name, count, wantRows)
		}
	}
	assertTotals(
		"upstream head",
		compile(base+` | sort event_id | head 3 | eventstats count AS total | table total`),
		3,
		3,
	)
	assertTotals(
		"empty input",
		compile(base+` event_id="not-present" | eventstats count AS total | table total`),
		0,
		0,
	)
	assertTotals(
		"downstream head",
		compile(base+` | eventstats count AS total | sort event_id | head 2 | table total`),
		2,
		5,
	)
	assertTotals(
		"alias replacement",
		compile(base+` | eventstats count AS eventstats_existing | sort event_id | head 1 | table eventstats_existing`),
		1,
		5,
	)
	assertTotals(
		"count(field) projected input",
		compile(
			base+` | fields event_id | eventstats count(eventstats_value) AS total | head 1 | table total`,
		),
		1,
		0,
	)
	assertTotals(
		"count(field) alias replacement",
		compile(
			base+` | eventstats count(eventstats_value) AS eventstats_value | head 1 | table eventstats_value`,
		),
		1,
		4,
	)
	assertTotals(
		"generated exact row boundary",
		compileBoundary(
			`index=eventstats-boundary source="eventstats-boundary" host="in" | eventstats count AS total | head 1 | table total`,
		),
		1,
		MaximumEventStatsInputRows,
	)
	assertTotals(
		"count(field) generated exact row boundary",
		compileBoundary(
			`index=eventstats-boundary source="eventstats-boundary" host="in" | eventstats count(eventstats_missing) AS total | head 1 | table total`,
		),
		1,
		0,
	)

	for name, source := range map[string]string{
		"visible generated overflow":           `index=eventstats-boundary source="eventstats-boundary" | eventstats count AS total | table event_id total`,
		"downstream-pruned generated overflow": `index=eventstats-boundary source="eventstats-boundary" | eventstats count AS total | search event_id="not-present" | table total`,
		"zero-occurrence generated overflow":   `index=eventstats-boundary source="eventstats-boundary" | eventstats count(eventstats_missing) AS total | table total`,
	} {
		queryErr := executeCompiledExpectingNoRows(
			ctx,
			connection,
			compileBoundary(source),
		)
		if queryErr == nil ||
			!strings.Contains(queryErr.Error(), EventStatsInputLimitMarker) {
			t.Fatalf(
				"%s error = %v, want generated eventstats limit marker and no rows",
				name,
				queryErr,
			)
		}
	}

	postStats := compile(
		base + ` | stats count BY host | eventstats count AS groups | table host count groups`,
	)
	var host string
	var count, groups uint64
	if err := connection.QueryRow(ctx, postStats.SQL, postStats.Args...).Scan(
		&host,
		&count,
		&groups,
	); err != nil {
		t.Fatalf("execute eventstats after stats: %v\nSQL: %s\nargs: %#v", err, postStats.SQL, postStats.Args)
	}
	if host != "eventstats-host" || count != 5 || groups != 1 {
		t.Fatalf("eventstats after stats = host %q count %d groups %d", host, count, groups)
	}

	postEventStats := compile(
		base + ` | eventstats count AS total | stats count AS rows BY total | table total rows`,
	)
	var total, rows uint64
	if err := connection.QueryRow(
		ctx,
		postEventStats.SQL,
		postEventStats.Args...,
	).Scan(&total, &rows); err != nil {
		t.Fatalf(
			"execute stats after eventstats: %v\nSQL: %s\nargs: %#v",
			err,
			postEventStats.SQL,
			postEventStats.Args,
		)
	}
	if total != 5 || rows != 5 {
		t.Fatalf(
			"stats after eventstats = total %d rows %d, want 5/5",
			total,
			rows,
		)
	}

	postValues := compile(
		base + ` | stats values(eventstats_group) AS groups | eventstats count(groups) AS occurrences | table occurrences`,
	)
	var fixedOccurrences uint64
	if err := connection.QueryRow(
		ctx,
		postValues.SQL,
		postValues.Args...,
	).Scan(&fixedOccurrences); err != nil {
		t.Fatalf(
			"execute fixed multivalue eventstats count(field): %v\nSQL: %s\nargs: %#v",
			err,
			postValues.SQL,
			postValues.Args,
		)
	}
	if fixedOccurrences != 2 {
		t.Fatalf(
			"fixed multivalue eventstats count(field) = %d, want 2",
			fixedOccurrences,
		)
	}

	missingGroups := compile(
		base + ` | eventstats count AS peers BY eventstats_group | where isnull(peers) | sort event_id | table event_id`,
	)
	missingRows, err := connection.Query(
		ctx,
		missingGroups.SQL,
		missingGroups.Args...,
	)
	if err != nil {
		t.Fatalf(
			"execute missing eventstats groups: %v\nSQL: %s\nargs: %#v",
			err,
			missingGroups.SQL,
			missingGroups.Args,
		)
	}
	var missingIDs []string
	for missingRows.Next() {
		var id string
		if scanErr := missingRows.Scan(&id); scanErr != nil {
			_ = missingRows.Close()
			t.Fatalf("scan missing eventstats groups: %v", scanErr)
		}
		missingIDs = append(missingIDs, id)
	}
	if rowsErr := missingRows.Err(); rowsErr != nil {
		_ = missingRows.Close()
		t.Fatalf("iterate missing eventstats groups: %v", rowsErr)
	}
	if closeErr := missingRows.Close(); closeErr != nil {
		t.Fatalf("close missing eventstats groups: %v", closeErr)
	}
	if want := []string{"eventstats-4", "eventstats-5"}; !reflect.DeepEqual(
		missingIDs,
		want,
	) {
		t.Fatalf("missing eventstats group IDs = %v, want %v", missingIDs, want)
	}

	for name, source := range map[string]string{
		"visible poison":           `index=compiler source="eventstats-poison" | eventstats count AS peers BY eventstats_group`,
		"downstream hidden poison": `index=compiler source="eventstats-poison" | eventstats count AS peers BY eventstats_group | fields - peers | search event_id="not-present"`,
	} {
		queryErr := executeCompiledExpectingNoRows(ctx, connection, compile(source))
		var exception *clickhousedriver.Exception
		if !errors.As(queryErr, &exception) ||
			exception.Code != 395 ||
			!strings.Contains(exception.Message, UnsupportedStatsByValueMarker) {
			t.Fatalf("%s error = %v, want guarded scalar-group failure", name, queryErr)
		}
	}
}
