package clickhouse

import (
	"context"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	clickhousedriver "github.com/ClickHouse/clickhouse-go/v2"
	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/ingest"
	"github.com/Suhaibinator/open-splunk/internal/spl"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// testStatsListAgainstClickHouse pins list() to the production ClickHouse
// image. Unit tests prove the generated shape; these cases prove ordering,
// truncation, typed multivalue behavior, and atomic runtime guards.
func testStatsListAgainstClickHouse(
	t *testing.T,
	ctx context.Context,
	store *Store,
	connection clickhousedriver.Conn,
	indexTime time.Time,
) {
	t.Helper()

	newEvent := func(
		id string,
		source string,
		eventTime time.Time,
		fields ...*opensplunkv1.TypedObjectField,
	) *ingest.StoredEvent {
		event := compilerIntegrationEvent(id, "stats-list-host", "stats list fixture", indexTime, fields...)
		event.BatchID = "stats-list-main"
		event.Event.Source = source
		event.Event.EventTime = timestamppb.New(eventTime)
		return event
	}
	tiedTime := indexTime.Add(-time.Second)
	events := []*ingest.StoredEvent{
		newEvent(
			"list-order-null",
			"stats-list-order",
			tiedTime,
			typedField("list_group", typedString("ordered")),
			typedField("list_sequence", typedUint(0)),
			typedField("list_value", typedNull()),
		),
		newEvent(
			"list-order-first",
			"stats-list-order",
			tiedTime,
			typedField("list_group", typedString("ordered")),
			typedField("list_sequence", typedUint(1)),
			typedField("list_dedup", typedString("A")),
			typedField("list_value", typedList(
				typedString("first"),
				typedString("duplicate"),
				typedNull(),
				typedString(""),
			)),
		),
		newEvent(
			"list-order-second",
			"stats-list-order",
			tiedTime,
			typedField("list_group", typedString("ordered")),
			typedField("list_sequence", typedUint(2)),
			typedField("list_dedup", typedString("A")),
			typedField("list_value", typedString("second")),
		),
		newEvent(
			"list-order-missing",
			"stats-list-order",
			tiedTime,
			typedField("list_group", typedString("ordered")),
			typedField("list_sequence", typedUint(3)),
		),
		newEvent(
			"list-order-fourth",
			"stats-list-order",
			tiedTime,
			typedField("list_group", typedString("ordered")),
			typedField("list_sequence", typedUint(4)),
			typedField("list_dedup", typedString("B")),
			typedField("list_value", typedString("fourth")),
		),
		newEvent(
			"list-order-fifth",
			"stats-list-order",
			tiedTime,
			typedField("list_group", typedString("ordered")),
			typedField("list_sequence", typedUint(5)),
			typedField("list_dedup", typedString("B")),
			typedField("list_value", typedList(
				typedString("duplicate"),
				typedUint(7),
				typedBool(true),
			)),
		),
	}

	for index := 0; index <= int(MaximumStatsListValuesPerGroup); index++ {
		value := fmt.Sprintf("value-%03d", index)
		if index == int(MaximumStatsListValuesPerGroup) {
			// A valid scalar after occurrence 100 is truncated and therefore
			// does not count toward the selected-prefix byte ceiling.
			value = strings.Repeat("z", int(MaximumStatsListBytesPerGroup)+1)
		}
		events = append(events, newEvent(
			fmt.Sprintf("list-truncate-%03d", index),
			"stats-list-truncate",
			tiedTime,
			typedField("list_sequence", typedUint(uint64(index))),
			typedField("list_value", typedString(value)),
		))
	}

	for index := 0; index <= int(MaximumStatsListValuesPerGroup); index++ {
		value := typedString(fmt.Sprintf("safe-%03d", index))
		if index == int(MaximumStatsListValuesPerGroup) {
			value = typedObject(typedField("child", typedString("poison")))
		}
		events = append(events, newEvent(
			fmt.Sprintf("list-poison-%03d", index),
			"stats-list-poison",
			tiedTime,
			typedField("list_sequence", typedUint(uint64(index))),
			typedField("list_value", value),
		))
	}

	events = append(events,
		newEvent(
			"list-by-complete",
			"stats-list-incomplete-by",
			tiedTime,
			typedField("list_group", typedString("kept")),
			typedField("list_value", typedString("visible")),
		),
		newEvent(
			"list-by-incomplete-poison",
			"stats-list-incomplete-by",
			tiedTime,
			typedField("list_value", typedObject(
				typedField("child", typedString("must remain ineligible")),
			)),
		),
		newEvent(
			"list-bytes-exact",
			"stats-list-bytes-exact",
			tiedTime,
			typedField("list_value", typedString(
				strings.Repeat("x", int(MaximumStatsListBytesPerGroup)),
			)),
		),
		newEvent(
			"list-bytes-over",
			"stats-list-bytes-over",
			tiedTime,
			typedField("list_value", typedString(
				strings.Repeat("x", int(MaximumStatsListBytesPerGroup)+1),
			)),
		),
	)
	storeStatsListBatch(t, ctx, store, "stats-list-main", 60, events)

	older := newEvent(
		"list-visibility-tie",
		"stats-list-visibility",
		tiedTime,
		typedField("list_value", typedString("older")),
	)
	storeStatsListBatch(t, ctx, store, "stats-list-older", 61, []*ingest.StoredEvent{older})
	newer := newEvent(
		"list-visibility-tie",
		"stats-list-visibility",
		tiedTime,
		typedField("list_value", typedString("newer")),
	)
	storeStatsListBatch(t, ctx, store, "stats-list-newer", 62, []*ingest.StoredEvent{newer})

	// Give two otherwise tied rows the same visibility sequence. This is the
	// shape of migrated pre-visibility data (where every sequence is zero);
	// the positive value keeps the production constraint enabled while proving
	// that immutable batch identity closes the remaining ordering tie.
	if err := connection.Exec(ctx, `
		INSERT INTO open_splunk.events
		SELECT * REPLACE (
			'stats-list-source-identity' AS source,
			toUInt64(1) AS visibility_seq
		)
		FROM open_splunk.events
		WHERE batch_id IN ('stats-list-older', 'stats-list-newer')`); err != nil {
		t.Fatalf("insert source-identity tie fixtures: %v", err)
	}

	visibilityCutoff, err := store.VisibilityCutoff(ctx)
	if err != nil {
		t.Fatalf("capture stats list visibility cutoff: %v", err)
	}
	compile := func(source string) CompiledQuery {
		return compileIntegrationSPL(t, source, indexTime.Add(10*time.Second), visibilityCutoff)
	}
	oneList := func(query CompiledQuery) []string {
		t.Helper()
		var values []string
		if err := connection.QueryRow(ctx, query.SQL, query.Args...).Scan(&values); err != nil {
			t.Fatalf("execute stats list: %v\nSQL: %s\nargs: %#v", err, query.SQL, query.Args)
		}
		return values
	}

	const orderBase = `index=compiler source="stats-list-order"`
	ordered := oneList(compile(
		orderBase + ` | sort 0 +list_sequence | stats list(list_value) AS ordered`,
	))
	wantOrdered := []string{
		"first", "duplicate", "", "second", "fourth", "duplicate", "7", "true",
	}
	if !reflect.DeepEqual(ordered, wantOrdered) {
		t.Fatalf("ordered list = %#v, want %#v", ordered, wantOrdered)
	}

	tail := oneList(compile(
		orderBase + ` | sort 0 +list_sequence | tail 2 | stats list(list_value) AS ordered`,
	))
	if want := []string{"duplicate", "7", "true", "fourth"}; !reflect.DeepEqual(tail, want) {
		t.Fatalf("tail-fed list = %#v, want %#v", tail, want)
	}
	deduplicated := oneList(compile(
		orderBase + ` | sort 0 +list_sequence | dedup list_dedup | stats list(list_value) AS ordered`,
	))
	if want := []string{"first", "duplicate", "", "fourth"}; !reflect.DeepEqual(deduplicated, want) {
		t.Fatalf("dedup-fed list = %#v, want %#v", deduplicated, want)
	}

	truncated := oneList(compile(
		`index=compiler source="stats-list-truncate" | sort 0 +list_sequence | stats list(list_value) AS ordered`,
	))
	if len(truncated) != int(MaximumStatsListValuesPerGroup) ||
		truncated[0] != "value-000" ||
		truncated[len(truncated)-1] != "value-099" {
		t.Fatalf("truncated list boundary = len:%d first:%q last:%q", len(truncated), truncated[0], truncated[len(truncated)-1])
	}

	for _, threads := range []uint64{1, 4} {
		threadContext := clickhousedriver.Context(
			ctx,
			clickhousedriver.WithSettings(clickhousedriver.Settings{"max_threads": threads}),
		)
		query := compile(`index=compiler source="stats-list-visibility" | stats list(list_value) AS ordered`)
		var values []string
		if err := connection.QueryRow(threadContext, query.SQL, query.Args...).Scan(&values); err != nil {
			t.Fatalf("execute visibility-tied list with %d threads: %v", threads, err)
		}
		if want := []string{"newer", "older"}; !reflect.DeepEqual(values, want) {
			t.Fatalf("visibility-tied list with %d threads = %#v, want %#v", threads, values, want)
		}

		query = compile(`index=compiler source="stats-list-source-identity" | stats list(list_value) AS ordered`)
		values = nil
		if err := connection.QueryRow(threadContext, query.SQL, query.Args...).Scan(&values); err != nil {
			t.Fatalf("execute source-identity-tied list with %d threads: %v", threads, err)
		}
		if want := []string{"newer", "older"}; !reflect.DeepEqual(values, want) {
			t.Fatalf("source-identity-tied list with %d threads = %#v, want %#v", threads, values, want)
		}
	}

	grouped := compile(
		`index=compiler source="stats-list-incomplete-by" | stats list(list_value) AS ordered BY list_group`,
	)
	var group string
	var groupedValues []string
	if err := connection.QueryRow(ctx, grouped.SQL, grouped.Args...).Scan(&group, &groupedValues); err != nil {
		t.Fatalf("execute incomplete-BY list: %v\nSQL: %s", err, grouped.SQL)
	}
	if group != "kept" || !reflect.DeepEqual(groupedValues, []string{"visible"}) {
		t.Fatalf("incomplete-BY list = %q/%#v, want kept/[visible]", group, groupedValues)
	}

	exactBytes := oneList(compile(
		`index=compiler source="stats-list-bytes-exact" | stats list(list_value) AS ordered`,
	))
	if len(exactBytes) != 1 || len(exactBytes[0]) != int(MaximumStatsListBytesPerGroup) {
		t.Fatalf("exact-byte list = %d values / %d bytes", len(exactBytes), len(exactBytes[0]))
	}
	for _, test := range []struct {
		name   string
		source string
		marker string
	}{
		{
			name:   "selected byte overflow",
			source: `index=compiler source="stats-list-bytes-over" | stats list(list_value) AS ordered`,
			marker: StatsListBytesLimitMarker,
		},
		{
			name:   "poison after occurrence 100",
			source: `index=compiler source="stats-list-poison" | sort 0 +list_sequence | stats list(list_value) AS ordered`,
			marker: UnsupportedStatsMeasureValueMarker,
		},
	} {
		query := compile(test.source)
		err := executeCompiledExpectingNoRows(ctx, connection, query)
		if err == nil || !strings.Contains(err.Error(), test.marker) {
			t.Fatalf("%s error = %v, want marker %q", test.name, err, test.marker)
		}
	}

	downstream := compile(
		orderBase + ` | sort 0 +list_sequence | stats list(list_value) AS ordered` +
			` | stats count(ordered) AS occurrences values(ordered) AS distinct_values list(ordered) AS repeated`,
	)
	var occurrences uint64
	var distinct, repeated []string
	if err := connection.QueryRow(ctx, downstream.SQL, downstream.Args...).Scan(
		&occurrences,
		&distinct,
		&repeated,
	); err != nil {
		t.Fatalf("execute downstream list aggregates: %v\nSQL: %s", err, downstream.SQL)
	}
	wantDistinct := []string{"", "7", "duplicate", "first", "fourth", "second", "true"}
	if occurrences != uint64(len(wantOrdered)) ||
		!reflect.DeepEqual(distinct, wantDistinct) ||
		!reflect.DeepEqual(repeated, wantOrdered) {
		t.Fatalf(
			"downstream list aggregates = %d/%#v/%#v, want %d/%#v/%#v",
			occurrences,
			distinct,
			repeated,
			len(wantOrdered),
			wantDistinct,
			wantOrdered,
		)
	}

	shared := compile(
		orderBase + ` | stats count list(list_value) AS first list(list_value) AS second`,
	)
	actions := explainCompiledQuery(t, ctx, connection, "EXPLAIN actions=1 ", shared)
	if got := strings.Count(actions, "Function: groupArraySortedArray("); got != 1 {
		t.Fatalf("repeated list has %d physical ordered states, want one:\n%s", got, actions)
	}
	if strings.Contains(actions, "ArrayJoin") {
		t.Fatalf("list physical plan expands event rows:\n%s", actions)
	}

	boundedListResults := func(
		source string,
		outputCount int,
		existingValues []string,
	) compiledRelation {
		t.Helper()
		listColumn := quoteIdentifier("__os_ordered_strings_0")
		overflowColumn := quoteIdentifier("__os_ordered_strings_bytes_overflow_0")
		measures := make([]compiledOrderedStringMeasure, outputCount)
		for index := range measures {
			measures[index] = compiledOrderedStringMeasure{
				listColumn:     listColumn,
				overflowColumn: overflowColumn,
				outputColumn:   quoteIdentifier(fmt.Sprintf("list_%d", index)),
			}
		}
		bounded, _ := compileBoundedOrderedStringResults(
			newScanRelation(source, spl.Range{}),
			measures,
			existingValues,
			spl.Range{},
			0,
		)
		return bounded
	}
	expectBoundedListError := func(name, sql, marker string) {
		t.Helper()
		err := executeCompiledExpectingNoRows(ctx, connection, CompiledQuery{SQL: sql})
		if err == nil || !strings.Contains(err.Error(), marker) {
			t.Fatalf("%s error = %v, want marker %q", name, err, marker)
		}
	}
	orderedTuples := func(valueCount uint64, valueSQL string) string {
		return "arrayMap(element -> tuple(toUInt64(element + 1), toUInt64(1), " +
			valueSQL + "), range(toUInt64(" + strconv.FormatUint(valueCount, 10) + ")))"
	}

	// Repeated public aliases count independently even though they share one
	// physical list state. Each individual cell is below the byte ceiling.
	duplicateValueBytes := uint64(
		MaximumStatsListBytesPerGroup / MaximumStatsListValuesPerGroup,
	)
	duplicateAliases := boundedListResults(
		`SELECT `+
			orderedTuples(
				MaximumStatsListValuesPerGroup,
				`repeat('x', `+strconv.FormatUint(duplicateValueBytes, 10)+`)`,
			)+` AS "__os_ordered_strings_0", `+
			`toUInt8(0) AS "__os_ordered_strings_bytes_overflow_0"`,
		2,
		nil,
	)
	expectBoundedListError(
		"duplicate list alias bytes",
		duplicateAliases.sql,
		StatsListBytesLimitMarker,
	)

	// A nearly full values() result and one valid 100-member list cross the
	// shared row element budget even though each cell is independently valid.
	existingValueCount := uint64(
		MaximumStatsValuesPerGroup - MaximumStatsListValuesPerGroup/2,
	)
	combinedValues := boundedListResults(
		`SELECT `+
			orderedTuples(MaximumStatsListValuesPerGroup, `toString(element)`)+
			` AS "__os_ordered_strings_0", `+
			`toUInt8(0) AS "__os_ordered_strings_bytes_overflow_0", `+
			`arrayMap(element -> toString(element), range(toUInt64(`+
			strconv.FormatUint(existingValueCount, 10)+
			`))) AS "__os_existing_values"`,
		1,
		[]string{quoteIdentifier("__os_existing_values")},
	)
	expectBoundedListError(
		"combined values and list elements",
		combinedValues.sql,
		StatsListLimitMarker,
	)

	// Whole-result barriers run before a later LIMIT can hide overflowing
	// groups. Keep every cell within both per-row limits.
	elementRows := uint64(
		MaximumStatsListValuesPerResult/MaximumStatsListValuesPerGroup + 1,
	)
	wholeResultElements := boundedListResults(
		`SELECT toString(number) AS "group", `+
			orderedTuples(MaximumStatsListValuesPerGroup, `toString(element)`)+
			` AS "__os_ordered_strings_0", `+
			`toUInt8(0) AS "__os_ordered_strings_bytes_overflow_0" `+
			`FROM numbers(`+strconv.FormatUint(elementRows, 10)+`)`,
		1,
		nil,
	)
	expectBoundedListError(
		"whole-result list elements hidden by limit",
		`SELECT "list_0" FROM (`+wholeResultElements.sql+
			`) ORDER BY "group" LIMIT 1`,
		StatsListLimitMarker,
	)

	const wholeResultValueBytes uint64 = 1000
	bytesPerRow := MaximumStatsListValuesPerGroup * wholeResultValueBytes
	byteRows := MaximumStatsListBytesPerResult/bytesPerRow + 1
	wholeResultBytes := boundedListResults(
		`SELECT toString(number) AS "group", `+
			orderedTuples(
				MaximumStatsListValuesPerGroup,
				`repeat('x', `+strconv.FormatUint(wholeResultValueBytes, 10)+`)`,
			)+` AS "__os_ordered_strings_0", `+
			`toUInt8(0) AS "__os_ordered_strings_bytes_overflow_0" `+
			`FROM numbers(`+strconv.FormatUint(byteRows, 10)+`)`,
		1,
		nil,
	)
	expectBoundedListError(
		"whole-result list bytes hidden by limit",
		`SELECT "list_0" FROM (`+wholeResultBytes.sql+
			`) ORDER BY "group" LIMIT 1`,
		StatsListBytesLimitMarker,
	)
}

func storeStatsListBatch(
	t *testing.T,
	ctx context.Context,
	store *Store,
	batchID string,
	sequence uint64,
	events []*ingest.StoredEvent,
) {
	t.Helper()
	for _, event := range events {
		event.BatchID = batchID
	}
	if _, err := store.Store(ctx, ingest.StoreBatch{
		TenantID:          "tenant",
		CollectorID:       "collector",
		BatchID:           batchID,
		BatchSequence:     sequence,
		SourceBatchSHA256: testSourceBatchDigest(batchID + "-" + strconv.FormatUint(sequence, 10)),
		ReceivedAt:        events[0].IndexTime,
		Events:            events,
	}); err != nil {
		t.Fatalf("store stats list batch %q: %v", batchID, err)
	}
}
