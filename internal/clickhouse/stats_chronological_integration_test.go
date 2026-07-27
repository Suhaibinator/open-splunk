package clickhouse

import (
	"context"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	clickhousedriver "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/chcol"
	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/ingest"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// testStatsChronologicalAgainstClickHouse pins earliest()/latest() to the
// production ClickHouse image. These cases deliberately separate event
// chronology from value and pipeline order, exercise every immutable row-key
// tie-breaker, and prove unsupported values cannot be optimized away.
func testStatsChronologicalAgainstClickHouse(
	ctx context.Context,
	t *testing.T,
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
		event := compilerIntegrationEvent(
			id,
			"stats-chronological-host",
			"stats chronological fixture",
			indexTime,
			fields...,
		)
		event.Event.Source = source
		event.Event.EventTime = timestamppb.New(eventTime)
		return event
	}

	events := []*ingest.StoredEvent{
		newEvent(
			"chronological-order-oldest",
			"stats-chronological-order",
			indexTime.Add(-3*time.Second),
			typedField("chronological_group", typedString("ordered")),
			typedField("chronological_sequence", typedUint(3)),
			typedField("chronological_value", typedString("z")),
		),
		newEvent(
			"chronological-order-middle",
			"stats-chronological-order",
			indexTime.Add(-2*time.Second),
			typedField("chronological_group", typedString("ordered")),
			typedField("chronological_sequence", typedUint(2)),
			typedField("chronological_value", typedString("middle")),
		),
		newEvent(
			"chronological-order-newest",
			"stats-chronological-order",
			indexTime.Add(-time.Second),
			typedField("chronological_group", typedString("ordered")),
			typedField("chronological_sequence", typedUint(1)),
			typedField("chronological_value", typedString("a")),
		),
		newEvent(
			"chronological-event-id-a",
			"stats-chronological-event-id",
			indexTime.Add(-5*time.Second),
			typedField("chronological_value", typedString("id-a")),
		),
		newEvent(
			"chronological-event-id-b",
			"stats-chronological-event-id",
			indexTime.Add(-5*time.Second),
			typedField("chronological_value", typedString("id-b")),
		),
		newEvent(
			"chronological-multivalue",
			"stats-chronological-multivalue",
			indexTime.Add(-4*time.Second),
			typedField("chronological_value", typedList(
				typedNull(),
				typedString(""),
				typedString("middle"),
				typedString("last"),
				typedNull(),
			)),
		),
		newEvent(
			"chronological-null-missing",
			"stats-chronological-null",
			indexTime.Add(-4*time.Second),
			typedField("chronological_group", typedString("retained")),
		),
		newEvent(
			"chronological-null-explicit",
			"stats-chronological-null",
			indexTime.Add(-3*time.Second),
			typedField("chronological_group", typedString("retained")),
			typedField("chronological_value", typedNull()),
		),
		newEvent(
			"chronological-type-string",
			"stats-chronological-types",
			indexTime.Add(-4*time.Second),
			typedField("chronological_group", typedString("string")),
			typedField("chronological_value", typedString("word")),
		),
		newEvent(
			"chronological-type-signed",
			"stats-chronological-types",
			indexTime.Add(-4*time.Second),
			typedField("chronological_group", typedString("signed")),
			typedField("chronological_value", typedSint(-7)),
		),
		newEvent(
			"chronological-type-unsigned",
			"stats-chronological-types",
			indexTime.Add(-4*time.Second),
			typedField("chronological_group", typedString("unsigned")),
			typedField("chronological_value", typedUint(^uint64(0))),
		),
		newEvent(
			"chronological-type-floating",
			"stats-chronological-types",
			indexTime.Add(-4*time.Second),
			typedField("chronological_group", typedString("floating")),
			typedField("chronological_value", typedDouble(1.25)),
		),
		newEvent(
			"chronological-type-bool",
			"stats-chronological-types",
			indexTime.Add(-4*time.Second),
			typedField("chronological_group", typedString("bool")),
			typedField("chronological_value", typedBool(true)),
		),
		newEvent(
			"chronological-type-bytes",
			"stats-chronological-types",
			indexTime.Add(-4*time.Second),
			typedField("chronological_group", typedString("bytes")),
			typedField("chronological_value", typedBytes([]byte{0, 0xff, 0x10})),
		),
		newEvent(
			"chronological-type-timestamp",
			"stats-chronological-types",
			indexTime.Add(-4*time.Second),
			typedField("chronological_group", typedString("timestamp")),
			typedField(
				"chronological_value",
				typedTimestamp(time.Date(2026, 7, 21, 3, 4, 5, 123456789, time.UTC)),
			),
		),
		newEvent(
			"chronological-type-duration",
			"stats-chronological-types",
			indexTime.Add(-4*time.Second),
			typedField("chronological_group", typedString("duration")),
			typedField("chronological_value", typedDuration(3*time.Second+4*time.Nanosecond)),
		),
		newEvent(
			"chronological-type-decimal",
			"stats-chronological-types",
			indexTime.Add(-4*time.Second),
			typedField("chronological_group", typedString("decimal")),
			typedField("chronological_value", typedDecimal("-1234567890.00100e+12")),
		),
		newEvent(
			"chronological-poison-valid-winner",
			"stats-chronological-poison",
			indexTime.Add(-4*time.Second),
			typedField("chronological_group", typedString("poison")),
			typedField("chronological_value", typedString("valid-earliest")),
		),
		newEvent(
			"chronological-poison-object-nonwinner",
			"stats-chronological-poison",
			indexTime.Add(-3*time.Second),
			typedField("chronological_group", typedString("poison")),
			typedField("chronological_value", typedObject(
				typedField("child", typedString("must fail atomically")),
			)),
		),
		newEvent(
			"chronological-poison-safe-group",
			"stats-chronological-poison",
			indexTime.Add(-2*time.Second),
			typedField("chronological_group", typedString("safe")),
			typedField("chronological_value", typedString("safe-value")),
		),
		newEvent(
			"chronological-by-complete",
			"stats-chronological-incomplete-by",
			indexTime.Add(-4*time.Second),
			typedField("chronological_group", typedString("kept")),
			typedField("chronological_value", typedString("visible")),
		),
		newEvent(
			"chronological-by-incomplete-poison",
			"stats-chronological-incomplete-by",
			indexTime.Add(-3*time.Second),
			typedField("chronological_value", typedObject(
				typedField("child", typedString("ineligible")),
			)),
		),
	}
	storeStatsChronologicalBatch(ctx, t, store, "stats-chronological-main", 70, events)

	tiedTime := indexTime.Add(-5 * time.Second)
	older := newEvent(
		"chronological-tied-event",
		"stats-chronological-visibility",
		tiedTime,
		typedField("chronological_value", typedString("older")),
	)
	storeStatsChronologicalBatch(
		ctx,
		t,
		store,
		"stats-chronological-older",
		71,
		[]*ingest.StoredEvent{older},
	)
	newer := newEvent(
		"chronological-tied-event",
		"stats-chronological-visibility",
		tiedTime,
		typedField("chronological_value", typedString("newer")),
	)
	storeStatsChronologicalBatch(
		ctx,
		t,
		store,
		"stats-chronological-newer",
		72,
		[]*ingest.StoredEvent{newer},
	)

	// Simulate migrated rows whose visibility sequence is tied. The immutable
	// source identity must close the remaining event-time/event-id tie.
	if err := connection.Exec(ctx, `
		INSERT INTO open_splunk.events
		SELECT * REPLACE (
			'stats-chronological-source-identity' AS source,
			toUInt64(1) AS visibility_seq
		)
		FROM open_splunk.events
		WHERE batch_id IN (
			'stats-chronological-older',
			'stats-chronological-newer'
		)`); err != nil {
		t.Fatalf("insert chronological source-identity tie fixtures: %v", err)
	}

	visibilityCutoff, err := store.VisibilityCutoff(ctx)
	if err != nil {
		t.Fatalf("capture chronological visibility cutoff: %v", err)
	}
	compile := func(source string) CompiledQuery {
		return compileIntegrationSPL(
			t,
			source,
			indexTime.Add(10*time.Second),
			visibilityCutoff,
		)
	}
	onePair := func(query CompiledQuery) (any, any) {
		t.Helper()
		var earliest, latest chcol.Dynamic
		if queryErr := connection.QueryRow(
			ctx,
			query.SQL,
			query.Args...,
		).Scan(&earliest, &latest); queryErr != nil {
			t.Fatalf(
				"execute chronological pair: %v\nSQL: %s\nargs: %#v",
				queryErr,
				query.SQL,
				query.Args,
			)
		}
		return earliest.Any(), latest.Any()
	}
	expectPair := func(name string, query CompiledQuery, wantEarliest, wantLatest any) {
		t.Helper()
		gotEarliest, gotLatest := onePair(query)
		if !reflect.DeepEqual(gotEarliest, wantEarliest) ||
			!reflect.DeepEqual(gotLatest, wantLatest) {
			t.Fatalf(
				"%s earliest/latest = %#v/%#v, want %#v/%#v",
				name,
				gotEarliest,
				gotLatest,
				wantEarliest,
				wantLatest,
			)
		}
	}

	const orderBase = `index=compiler source="stats-chronological-order"`
	expectPair(
		"chronology instead of lexical value order",
		compile(orderBase+` | stats earliest(chronological_value) AS first latest(chronological_value) AS last`),
		"z",
		"a",
	)
	expectPair(
		"reversed pipeline sort",
		compile(orderBase+` | sort 0 +chronological_sequence`+
			` | stats earliest(chronological_value) AS first latest(chronological_value) AS last`),
		"z",
		"a",
	)
	expectPair(
		"head survivor set",
		compile(orderBase+` | sort 0 -chronological_sequence | head 2`+
			` | stats earliest(chronological_value) AS first latest(chronological_value) AS last`),
		"z",
		"middle",
	)
	expectPair(
		"multivalue member ordinal and empty string",
		compile(`index=compiler source="stats-chronological-multivalue"`+
			` | stats earliest(chronological_value) AS first latest(chronological_value) AS last`),
		"",
		"last",
	)
	expectPair(
		"event ID closes an event-time tie",
		compile(`index=compiler source="stats-chronological-event-id"`+
			` | stats earliest(chronological_value) AS first latest(chronological_value) AS last`),
		"id-a",
		"id-b",
	)

	for _, test := range []struct {
		name   string
		source string
	}{
		{
			name:   "visibility sequence closes an event-id tie",
			source: `stats-chronological-visibility`,
		},
		{
			name:   "source identity closes a visibility tie",
			source: `stats-chronological-source-identity`,
		},
	} {
		for _, threads := range []uint64{1, 4} {
			threadContext := clickhousedriver.Context(
				ctx,
				clickhousedriver.WithSettings(clickhousedriver.Settings{
					"max_threads": threads,
				}),
			)
			query := compile(
				`index=compiler source="` + test.source + `"` +
					` | stats earliest(chronological_value) AS first` +
					` latest(chronological_value) AS last`,
			)
			var first, last chcol.Dynamic
			if queryErr := connection.QueryRow(
				threadContext,
				query.SQL,
				query.Args...,
			).Scan(&first, &last); queryErr != nil {
				t.Fatalf(
					"%s with %d threads: %v\nSQL: %s",
					test.name,
					threads,
					queryErr,
					query.SQL,
				)
			}
			if first.Any() != "older" || last.Any() != "newer" {
				t.Fatalf(
					"%s with %d threads = %#v/%#v, want older/newer",
					test.name,
					threads,
					first.Any(),
					last.Any(),
				)
			}
		}
	}

	typeQuery := compile(
		`index=compiler source="stats-chronological-types"` +
			` | stats earliest(chronological_value) AS first` +
			` latest(chronological_value) AS last BY chronological_group`,
	)
	typeRows, err := connection.Query(ctx, typeQuery.SQL, typeQuery.Args...)
	if err != nil {
		t.Fatalf(
			"execute chronological scalar types: %v\nSQL: %s\nargs: %#v",
			err,
			typeQuery.SQL,
			typeQuery.Args,
		)
	}
	gotTypes := make(map[string][2]any)
	for typeRows.Next() {
		var group string
		var first, last chcol.Dynamic
		if scanErr := typeRows.Scan(&group, &first, &last); scanErr != nil {
			_ = typeRows.Close()
			t.Fatalf("scan chronological scalar types: %v", scanErr)
		}
		gotTypes[group] = [2]any{first.Any(), last.Any()}
	}
	if err := typeRows.Err(); err != nil {
		_ = typeRows.Close()
		t.Fatalf("iterate chronological scalar types: %v", err)
	}
	if err := typeRows.Close(); err != nil {
		t.Fatalf("close chronological scalar type rows: %v", err)
	}
	wantTypes := map[string][2]any{
		"bool":      {"true", "true"},
		"bytes":     {"AP8Q", "AP8Q"},
		"decimal":   {"-1234567890.00100e+12", "-1234567890.00100e+12"},
		"duration":  {"3:4", "3:4"},
		"floating":  {"1.25", "1.25"},
		"signed":    {"-7", "-7"},
		"string":    {"word", "word"},
		"timestamp": {"2026-07-21T03:04:05.123456789Z", "2026-07-21T03:04:05.123456789Z"},
		"unsigned":  {"18446744073709551615", "18446744073709551615"},
	}
	if !reflect.DeepEqual(gotTypes, wantTypes) {
		t.Fatalf("chronological scalar types = %#v, want %#v", gotTypes, wantTypes)
	}

	nullGrouped := compile(
		`index=compiler source="stats-chronological-null"` +
			` | stats earliest(chronological_value) AS first` +
			` latest(chronological_value) AS last BY chronological_group`,
	)
	var nullGroup string
	var nullFirst, nullLast chcol.Dynamic
	if err := connection.QueryRow(
		ctx,
		nullGrouped.SQL,
		nullGrouped.Args...,
	).Scan(&nullGroup, &nullFirst, &nullLast); err != nil {
		t.Fatalf("execute retained all-null chronological group: %v\nSQL: %s", err, nullGrouped.SQL)
	}
	if nullGroup != "retained" || nullFirst.Any() != nil || nullLast.Any() != nil {
		t.Fatalf(
			"retained all-null chronological group = %q/%#v/%#v, want retained/null/null",
			nullGroup,
			nullFirst.Any(),
			nullLast.Any(),
		)
	}

	globalEmpty := compile(
		orderBase + ` event_id=does-not-exist` +
			` | stats earliest(chronological_value) AS first latest(chronological_value) AS last`,
	)
	expectPair("global empty input", globalEmpty, nil, nil)

	groupedEmpty := compile(
		orderBase + ` event_id=does-not-exist` +
			` | stats earliest(chronological_value) AS first BY chronological_group`,
	)
	var groupedEmptyCount uint64
	if err := connection.QueryRow(
		ctx,
		`SELECT count() FROM (`+groupedEmpty.SQL+`)`,
		groupedEmpty.Args...,
	).Scan(&groupedEmptyCount); err != nil {
		t.Fatalf("execute grouped empty chronological stats: %v\nSQL: %s", err, groupedEmpty.SQL)
	}
	if groupedEmptyCount != 0 {
		t.Fatalf("grouped empty chronological rows = %d, want 0", groupedEmptyCount)
	}

	projected := compile(
		orderBase + ` | fields - chronological_value` +
			` | stats earliest(chronological_value) AS first latest(chronological_value) AS last`,
	)
	expectPair("projected-away input", projected, nil, nil)

	incompleteBy := compile(
		`index=compiler source="stats-chronological-incomplete-by"` +
			` | stats earliest(chronological_value) AS first BY chronological_group`,
	)
	var incompleteGroup string
	var incompleteFirst chcol.Dynamic
	if err := connection.QueryRow(
		ctx,
		incompleteBy.SQL,
		incompleteBy.Args...,
	).Scan(&incompleteGroup, &incompleteFirst); err != nil {
		t.Fatalf("execute incomplete-BY chronological stats: %v\nSQL: %s", err, incompleteBy.SQL)
	}
	if incompleteGroup != "kept" || incompleteFirst.Any() != "visible" {
		t.Fatalf(
			"incomplete-BY chronological result = %q/%#v, want kept/visible",
			incompleteGroup,
			incompleteFirst.Any(),
		)
	}

	nonwinnerPoison := compile(
		`index=compiler source="stats-chronological-poison"` +
			` | stats earliest(chronological_value) AS first`,
	)
	nonwinnerErr := connection.QueryRow(
		ctx,
		nonwinnerPoison.SQL,
		nonwinnerPoison.Args...,
	).Scan(new(chcol.Dynamic))
	if nonwinnerErr == nil ||
		!strings.Contains(nonwinnerErr.Error(), UnsupportedStatsMeasureValueMarker) {
		t.Fatalf(
			"invalid chronological nonwinner error = %v, want marker %q",
			nonwinnerErr,
			UnsupportedStatsMeasureValueMarker,
		)
	}

	prunedPoison := compile(
		`index=compiler source="stats-chronological-poison"` +
			` | stats earliest(chronological_value) AS discarded BY chronological_group` +
			` | table chronological_group`,
	)
	prunedErr := executeCompiledExpectingNoRows(ctx, connection, prunedPoison)
	if prunedErr == nil ||
		!strings.Contains(prunedErr.Error(), UnsupportedStatsMeasureValueMarker) {
		t.Fatalf(
			"output-pruned chronological validation error = %v, want marker %q",
			prunedErr,
			UnsupportedStatsMeasureValueMarker,
		)
	}

	for _, test := range []struct {
		name       string
		downstream string
	}{
		{
			name:       "retained group filter",
			downstream: ` | search chronological_group=safe | table chronological_group`,
		},
		{
			name:       "always-false missing-field filter",
			downstream: ` | search absent=value | table chronological_group`,
		},
	} {
		filteredPoison := compile(
			`index=compiler source="stats-chronological-poison"` +
				` | stats earliest(chronological_value) AS discarded BY chronological_group` +
				test.downstream,
		)
		filteredErr := executeCompiledExpectingNoRows(ctx, connection, filteredPoison)
		if filteredErr == nil ||
			!strings.Contains(filteredErr.Error(), UnsupportedStatsMeasureValueMarker) {
			t.Fatalf(
				"%s chronological validation error = %v, want marker %q",
				test.name,
				filteredErr,
				UnsupportedStatsMeasureValueMarker,
			)
		}
	}

	shared := compile(
		orderBase + ` | stats earliest(chronological_value) AS first` +
			` latest(chronological_value) AS last` +
			` earliest(chronological_value) AS first_again`,
	)
	if got := strings.Count(shared.SQL, "argMinOrNullIf("); got != 1 {
		t.Fatalf(
			"repeated earliest compiled %d aggregate states, want one:\n%s",
			got,
			shared.SQL,
		)
	}
	if got := strings.Count(shared.SQL, "argMaxOrNullIf("); got != 1 {
		t.Fatalf(
			"earliest/latest compiled %d latest states, want one:\n%s",
			got,
			shared.SQL,
		)
	}
	actions := explainCompiledQuery(t, ctx, connection, "EXPLAIN actions=1 ", shared)
	if !strings.Contains(actions, "Function: argMinOrNullIf(") {
		t.Fatalf("physical plan is missing earliest aggregation:\n%s", actions)
	}
	if !strings.Contains(actions, "Function: argMaxOrNullIf(") {
		t.Fatalf("physical plan is missing latest aggregation:\n%s", actions)
	}
	upperActions := strings.ToUpper(actions)
	for _, forbidden := range []string{"ARRAYJOIN", "GROUPARRAY", "WINDOW"} {
		if strings.Contains(upperActions, forbidden) {
			t.Fatalf(
				"chronological physical plan retained forbidden %q work:\n%s",
				forbidden,
				actions,
			)
		}
	}
}

func storeStatsChronologicalBatch(
	ctx context.Context,
	t *testing.T,
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
		t.Fatalf("store stats chronological batch %q: %v", batchID, err)
	}
}
