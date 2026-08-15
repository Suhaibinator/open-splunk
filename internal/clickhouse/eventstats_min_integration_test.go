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
	"github.com/Suhaibinator/open-splunk/internal/eventfields"
	"github.com/Suhaibinator/open-splunk/internal/ingest"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// testEventStatsMinimumAgainstClickHouse pins the row-preserving eventstats
// minimum contract to the production ClickHouse image. The fixture stays
// deliberately small and reuses the eventstats count boundary corpus for the
// 10,001-row execution fence.
func testEventStatsMinimumAgainstClickHouse(
	t *testing.T,
	ctx context.Context,
	store *Store,
	connection clickhousedriver.Conn,
	indexTime time.Time,
) {
	t.Helper()

	newEvent := func(
		id string,
		fields ...*opensplunkv1.TypedObjectField,
	) *ingest.StoredEvent {
		event := compilerIntegrationEvent(
			id,
			"eventstats-min-host",
			"eventstats min fixture",
			indexTime,
			fields...,
		)
		event.BatchID = "eventstats-min-batch"
		event.Event.Source = "eventstats-min-fixture"
		return event
	}
	withGroup := func(
		event *ingest.StoredEvent,
		group string,
	) *ingest.StoredEvent {
		event.Event.Fields.Fields = append(
			[]*opensplunkv1.TypedObjectField{
				typedField("eventstats_min_group", typedString(group)),
			},
			event.Event.Fields.Fields...,
		)
		return event
	}

	const exactMinimum = "9007199254740992.75"
	events := []*ingest.StoredEvent{
		withGroup(newEvent(
			"eventstats-min-01-int",
			typedField("eventstats_min_value", typedSint(10)),
			typedField("eventstats_min_exact", typedDecimal(exactMinimum)),
		), "A"),
		withGroup(newEvent(
			"eventstats-min-02-multivalue",
			typedField("eventstats_min_value", typedList(
				typedSint(2),
				typedString("01"),
				typedString("1.0"),
				typedString("z"),
				typedNull(),
			)),
			typedField("eventstats_min_exact", typedUint(9_007_199_254_740_993)),
		), "A"),
		withGroup(newEvent(
			"eventstats-min-03-numeric-text",
			typedField("eventstats_min_value", typedString("20")),
		), "B"),
		withGroup(newEvent(
			"eventstats-min-04-lexical-text",
			typedField("eventstats_min_value", typedString("alpha")),
		), "B"),
		withGroup(newEvent("eventstats-min-05-missing"), "B"),
		withGroup(newEvent(
			"eventstats-min-06-null",
			typedField("eventstats_min_value", typedNull()),
		), "B"),
		newEvent(
			"eventstats-min-07-missing-group",
			typedField("eventstats_min_incomplete", typedObject(
				typedField("child", typedString("must-not-poison")),
			)),
		),
		newEvent(
			"eventstats-min-08-null-group",
			typedField("eventstats_min_group", typedNull()),
		),
		withGroup(newEvent(
			"eventstats-min-09-object-poison",
			typedField("eventstats_min_object", typedObject(
				typedField("child", typedString("secret")),
			)),
		), "C"),
		withGroup(newEvent(
			"eventstats-min-10-nested-poison",
			typedField("eventstats_min_nested", typedList(
				typedString("safe"),
				typedList(typedString("secret")),
			)),
		), "C"),
		withGroup(newEvent(
			"eventstats-min-11-complete-group",
			typedField("required_group", typedString("present")),
			typedField("eventstats_min_incomplete", typedString("7")),
		), "D"),
	}
	for eventIndex, event := range events {
		event.Event.EventTime = timestamppb.New(
			indexTime.Add(time.Duration(eventIndex) * time.Nanosecond),
		)
		event.Event.Severity = opensplunkv1.LogSeverity_LOG_SEVERITY_INFO
		event.Event.Raw = []byte(`{"value":"other"}`)
	}
	events[0].Event.Raw = []byte(`{"value":"wanted"}`)
	events[0].Event.Severity = opensplunkv1.LogSeverity_LOG_SEVERITY_WARN
	events[1].Event.Severity = opensplunkv1.LogSeverity_LOG_SEVERITY_TRACE

	if _, err := store.Store(ctx, ingest.StoreBatch{
		TenantID:           "tenant",
		CollectorID:        "collector",
		BatchID:            "eventstats-min-batch",
		BatchSequence:      83,
		OriginalEventCount: uint32(len(events)),
		SourceBatchSHA256:  testSourceBatchDigest("eventstats-min-batch"),
		ReceivedAt:         indexTime,
		Events:             events,
	}); err != nil {
		t.Fatalf("store eventstats min fixtures: %v", err)
	}

	sameTenantPoison := newEvent(
		"eventstats-min-same-tenant-poison",
		typedField("eventstats_min_scope", typedObject(
			typedField("secret", typedString("outside-source")),
		)),
	)
	sameTenantPoison.BatchID = "eventstats-min-scope-batch"
	sameTenantPoison.Event.Source = "eventstats-min-poison"
	if _, err := store.Store(ctx, ingest.StoreBatch{
		TenantID:           "tenant",
		CollectorID:        "collector",
		BatchID:            "eventstats-min-scope-batch",
		BatchSequence:      84,
		OriginalEventCount: 1,
		SourceBatchSHA256:  testSourceBatchDigest("eventstats-min-scope-batch"),
		ReceivedAt:         indexTime,
		Events:             []*ingest.StoredEvent{sameTenantPoison},
	}); err != nil {
		t.Fatalf("store same-tenant eventstats min poison: %v", err)
	}

	foreignPoison := newEvent(
		"eventstats-min-foreign-poison",
		typedField("eventstats_min_scope", typedObject(
			typedField("secret", typedString("outside-tenant")),
		)),
	)
	foreignPoison.TenantID = "other-tenant"
	foreignPoison.BatchID = "eventstats-min-foreign-batch"
	if _, err := store.Store(ctx, ingest.StoreBatch{
		TenantID:           "other-tenant",
		CollectorID:        "collector",
		BatchID:            "eventstats-min-foreign-batch",
		BatchSequence:      85,
		OriginalEventCount: 1,
		SourceBatchSHA256:  testSourceBatchDigest("eventstats-min-foreign-batch"),
		ReceivedAt:         indexTime,
		Events:             []*ingest.StoredEvent{foreignPoison},
	}); err != nil {
		t.Fatalf("store cross-tenant eventstats min poison: %v", err)
	}

	visibilityCutoff, err := store.VisibilityCutoff(ctx)
	if err != nil {
		t.Fatalf("capture eventstats min visibility cutoff: %v", err)
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

	type dynamicRow struct {
		id    string
		value any
	}
	collectDynamicRows := func(name string, query CompiledQuery) []dynamicRow {
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
		var got []dynamicRow
		for rows.Next() {
			var id string
			var value chcol.Dynamic
			if scanErr := rows.Scan(&id, &value); scanErr != nil {
				_ = rows.Close()
				t.Fatalf("scan %s: %v", name, scanErr)
			}
			got = append(got, dynamicRow{id: id, value: value.Any()})
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
	allIDs := []string{
		"eventstats-min-01-int",
		"eventstats-min-02-multivalue",
		"eventstats-min-03-numeric-text",
		"eventstats-min-04-lexical-text",
		"eventstats-min-05-missing",
		"eventstats-min-06-null",
		"eventstats-min-07-missing-group",
		"eventstats-min-08-null-group",
		"eventstats-min-09-object-poison",
		"eventstats-min-10-nested-poison",
		"eventstats-min-11-complete-group",
	}
	dynamicRows := func(ids ...string) []dynamicRow {
		rows := make([]dynamicRow, 0, len(ids))
		for _, id := range ids {
			rows = append(rows, dynamicRow{id: id, value: float64(1)})
		}
		return rows
	}

	global := collectDynamicRows(
		"global eventstats min",
		compile(
			base+` | eventstats min(eventstats_min_value) AS low`+
				` | sort event_id | table event_id low`,
		),
	)
	if want := dynamicRows(allIDs...); !reflect.DeepEqual(global, want) {
		t.Fatalf("global eventstats min = %#v, want %#v", global, want)
	}

	grouped := collectDynamicRows(
		"grouped eventstats min",
		compile(
			base+` | eventstats min(eventstats_min_value) AS low BY eventstats_min_group`+
				` | sort event_id | table event_id low`,
		),
	)
	groupedWant := []dynamicRow{
		{id: allIDs[0], value: float64(1)},
		{id: allIDs[1], value: float64(1)},
		{id: allIDs[2], value: float64(20)},
		{id: allIDs[3], value: float64(20)},
		{id: allIDs[4], value: float64(20)},
		{id: allIDs[5], value: float64(20)},
		{id: allIDs[6]},
		{id: allIDs[7]},
		{id: allIDs[8]},
		{id: allIDs[9]},
		{id: allIDs[10]},
	}
	if !reflect.DeepEqual(grouped, groupedWant) {
		t.Fatalf("grouped eventstats min = %#v, want %#v", grouped, groupedWant)
	}

	assertPresence := func(
		name string,
		source string,
		wantPresent uint64,
		wantNull uint64,
		wantMissing uint64,
	) {
		t.Helper()
		logical := buildIntegrationPlan(
			t,
			source,
			indexTime.Add(10*time.Second),
			visibilityCutoff,
		)
		summary, compileErr := (Compiler{}).CompileFieldSummary(
			logical,
			FieldSummarySpec{
				FieldName:             "low",
				MaximumValues:         10,
				MaximumDistinctValues: 10,
				MaximumValueBytes:     64,
			},
		)
		if compileErr != nil {
			t.Fatalf("compile %s field summary: %v", name, compileErr)
		}
		// The summary contract orders its one control row first. Execute the
		// compiler product directly, as the production executor does: ClickHouse
		// does not support safely nesting a query that owns MATERIALIZED CTEs.
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
				"execute %s field summary: %v\nSQL: %s\nargs: %#v",
				name,
				queryErr,
				summary.SQL,
				summary.Args,
			)
		}
		if rowKind != 0 || fieldName != "low" || metadataInvalid != 0 ||
			unsupported != 0 || oversized != 0 {
			t.Fatalf(
				"%s control row = kind %d field %q invalid %d/%d/%d",
				name,
				rowKind,
				fieldName,
				metadataInvalid,
				unsupported,
				oversized,
			)
		}
		if present != wantPresent || nulls != wantNull ||
			missing != wantMissing || total != uint64(len(events)) {
			t.Fatalf(
				"%s presence = %d/%d/%d/%d, want %d/%d/%d/%d",
				name,
				present,
				nulls,
				missing,
				total,
				wantPresent,
				wantNull,
				wantMissing,
				len(events),
			)
		}
	}
	assertPresence(
		"grouped eventstats min",
		base+` | eventstats min(eventstats_min_value) AS low BY eventstats_min_group`,
		9,
		3,
		2,
	)

	type catalogRow struct {
		kind          uint8
		name          string
		observedTypes []uint8
		events        uint64
		nulls         uint64
		missing       uint64
		total         uint64
		invalid       uint8
	}
	collectCatalog := func(name, source string) []catalogRow {
		t.Helper()
		logical := buildIntegrationPlan(
			t,
			source,
			indexTime.Add(10*time.Second),
			visibilityCutoff,
		)
		catalog, compileErr := (Compiler{}).CompileFieldCatalog(
			logical,
			FieldCatalogSpec{MaximumFields: 64},
		)
		if compileErr != nil {
			t.Fatalf("compile %s field catalog: %v", name, compileErr)
		}
		rows, queryErr := connection.Query(ctx, catalog.SQL, catalog.Args...)
		if queryErr != nil {
			t.Fatalf(
				"execute %s field catalog: %v\nSQL: %s\nargs: %#v",
				name,
				queryErr,
				catalog.SQL,
				catalog.Args,
			)
		}
		var got []catalogRow
		for rows.Next() {
			var row catalogRow
			if scanErr := rows.Scan(
				&row.kind,
				&row.name,
				&row.observedTypes,
				&row.events,
				&row.nulls,
				&row.missing,
				&row.total,
				&row.invalid,
			); scanErr != nil {
				_ = rows.Close()
				t.Fatalf("scan %s field catalog: %v", name, scanErr)
			}
			got = append(got, row)
		}
		if rowsErr := rows.Err(); rowsErr != nil {
			_ = rows.Close()
			t.Fatalf("iterate %s field catalog: %v", name, rowsErr)
		}
		if closeErr := rows.Close(); closeErr != nil {
			t.Fatalf("close %s field catalog: %v", name, closeErr)
		}
		return got
	}
	assertCatalogProfile := func(
		name string,
		source string,
		want catalogRow,
	) {
		t.Helper()
		rows := collectCatalog(name, source)
		if len(rows) < 2 {
			t.Fatalf("%s field catalog = %#v, want header and profiles", name, rows)
		}
		header := rows[0]
		if header.kind != 0 || header.name != "" ||
			len(header.observedTypes) != 0 || header.events != 0 ||
			header.nulls != 0 || header.missing != 0 ||
			header.total != want.total || header.invalid != 0 {
			t.Fatalf("%s field catalog header = %#v", name, header)
		}
		found := false
		previous := ""
		for _, row := range rows[1:] {
			if row.kind != 1 || row.invalid != 0 ||
				(previous != "" && row.name <= previous) {
				t.Fatalf("%s field catalog is not deterministically ordered: %#v", name, rows)
			}
			previous = row.name
			if row.name != want.name {
				continue
			}
			if found {
				t.Fatalf("%s field catalog contains duplicate %q profiles", name, want.name)
			}
			found = true
			if !reflect.DeepEqual(row, want) {
				t.Fatalf("%s field catalog %q = %#v, want %#v", name, want.name, row, want)
			}
		}
		if !found {
			t.Fatalf("%s field catalog = %#v, missing %q", name, rows, want.name)
		}
	}

	type suggestionRow struct {
		kind    uint8
		name    string
		invalid uint8
	}
	collectSuggestions := func(name, source, prefix string) []suggestionRow {
		t.Helper()
		logical := buildIntegrationPlan(
			t,
			source,
			indexTime.Add(10*time.Second),
			visibilityCutoff,
		)
		suggestions, compileErr := (Compiler{}).CompileFieldSuggestions(
			logical,
			FieldSuggestionSpec{Prefix: prefix, MaximumFields: 10},
		)
		if compileErr != nil {
			t.Fatalf("compile %s field suggestions: %v", name, compileErr)
		}
		rows, queryErr := connection.Query(ctx, suggestions.SQL, suggestions.Args...)
		if queryErr != nil {
			t.Fatalf(
				"execute %s field suggestions: %v\nSQL: %s\nargs: %#v",
				name,
				queryErr,
				suggestions.SQL,
				suggestions.Args,
			)
		}
		var got []suggestionRow
		for rows.Next() {
			var row suggestionRow
			if scanErr := rows.Scan(&row.kind, &row.name, &row.invalid); scanErr != nil {
				_ = rows.Close()
				t.Fatalf("scan %s field suggestions: %v", name, scanErr)
			}
			got = append(got, row)
		}
		if rowsErr := rows.Err(); rowsErr != nil {
			_ = rows.Close()
			t.Fatalf("iterate %s field suggestions: %v", name, rowsErr)
		}
		if closeErr := rows.Close(); closeErr != nil {
			t.Fatalf("close %s field suggestions: %v", name, closeErr)
		}
		return got
	}

	groupedAnalysisSource := base +
		` | eventstats min(eventstats_min_value) AS low BY eventstats_min_group`
	if got, want := collectSuggestions(
		"grouped eventstats min",
		groupedAnalysisSource,
		"lo",
	), []suggestionRow{
		{kind: 0},
		{kind: 1, name: "low"},
	}; !reflect.DeepEqual(got, want) {
		t.Fatalf("grouped eventstats min field suggestions = %#v, want %#v", got, want)
	}
	assertCatalogProfile(
		"grouped eventstats min",
		groupedAnalysisSource,
		catalogRow{
			kind: 1,
			name: "low",
			observedTypes: []uint8{
				uint8(eventfields.StoredValueTypeNull),
				uint8(eventfields.StoredValueTypeDouble),
			},
			events:  9,
			nulls:   3,
			missing: 2,
			total:   uint64(len(events)),
		},
	)

	stackedAnalysisSource := groupedAnalysisSource +
		` | eventstats min(low) AS lower`
	stacked := collectDynamicRows(
		"stacked eventstats min",
		compile(stackedAnalysisSource+` | sort event_id | table event_id lower`),
	)
	if want := dynamicRows(allIDs...); !reflect.DeepEqual(stacked, want) {
		t.Fatalf("stacked eventstats min = %#v, want %#v", stacked, want)
	}
	if got, want := collectSuggestions(
		"stacked eventstats min",
		stackedAnalysisSource,
		"low",
	), []suggestionRow{
		{kind: 0},
		{kind: 1, name: "low"},
		{kind: 1, name: "lower"},
	}; !reflect.DeepEqual(got, want) {
		t.Fatalf("stacked eventstats min field suggestions = %#v, want %#v", got, want)
	}

	fencedCountThenMinimumSource := base +
		` | spath input=_raw output=selected path=value` +
		` | eventstats count(eval(selected="wanted")) AS hits` +
		` | eventstats min(eventstats_min_value) AS low` +
		` | where hits=1`
	for _, test := range []struct {
		name   string
		source string
	}{
		{
			name:   "fenced conditional count then minimum",
			source: fencedCountThenMinimumSource,
		},
		{
			name: "minimum then fenced conditional count",
			source: base + ` | eventstats min(eventstats_min_value) AS low` +
				` | spath input=_raw output=selected path=value` +
				` | eventstats count(eval(selected="wanted")) AS hits` +
				` | where hits=1`,
		},
	} {
		mixed := collectDynamicRows(
			test.name,
			compile(test.source+` | sort event_id | table event_id low`),
		)
		if want := dynamicRows(allIDs...); !reflect.DeepEqual(mixed, want) {
			t.Fatalf("%s = %#v, want %#v", test.name, mixed, want)
		}
	}
	// The compiler suite pins all four count/minimum orderings. Keep the live
	// analysis execution on the first-input, non-materialized spath branch; the
	// reverse branch is executed above as a search and would otherwise duplicate
	// a costly ClickHouse optimizer proof.
	if got, want := collectSuggestions(
		"fenced conditional count then minimum",
		fencedCountThenMinimumSource,
		"lo",
	), []suggestionRow{
		{kind: 0},
		{kind: 1, name: "low"},
	}; !reflect.DeepEqual(got, want) {
		t.Fatalf("fenced conditional count then minimum field suggestions = %#v, want %#v", got, want)
	}

	// The unsupported object is deliberately attached to the row whose BY key
	// is absent. Such a row cannot join an eventstats group, so it must neither
	// poison validation nor acquire the group's published minimum.
	incompleteAnalysisSource := base +
		` (event_id="eventstats-min-07-missing-group"` +
		` OR event_id="eventstats-min-11-complete-group")` +
		` | eventstats min(eventstats_min_incomplete) AS low BY required_group`
	assertCatalogProfile(
		"incomplete grouped eventstats min",
		incompleteAnalysisSource,
		catalogRow{
			kind:          1,
			name:          "low",
			observedTypes: []uint8{uint8(eventfields.StoredValueTypeDouble)},
			events:        1,
			missing:       1,
			total:         2,
		},
	)
	if got, want := collectSuggestions(
		"incomplete grouped eventstats min",
		incompleteAnalysisSource,
		"lo",
	), []suggestionRow{
		{kind: 0},
		{kind: 1, name: "low"},
	}; !reflect.DeepEqual(got, want) {
		t.Fatalf(
			"incomplete grouped eventstats min field suggestions = %#v, want %#v",
			got,
			want,
		)
	}

	// A missing key may suppress measure traversal, but it must not hide an
	// independently unsupported BY key on the same row. Exercise both source
	// orders so classifier argument order cannot accidentally choose the result.
	for _, groupOrder := range []string{
		"required_group eventstats_min_object",
		"eventstats_min_object required_group",
	} {
		poison := compile(
			base + ` | eventstats min(eventstats_min_value) AS discarded BY ` +
				groupOrder + ` | fields event_id | search event_id="not-present"`,
		)
		poisonErr := executeCompiledExpectingNoRows(ctx, connection, poison)
		if poisonErr == nil ||
			!strings.Contains(poisonErr.Error(), UnsupportedStatsByValueMarker) {
			t.Fatalf(
				"multi-key eventstats min BY %q error = %v, want atomic unsupported-BY marker",
				groupOrder,
				poisonErr,
			)
		}
	}

	// Projection hides both the unsupported input and the eventstats output
	// from each analysis product. The chronological validation barrier must
	// still inspect every eligible source row and fail atomically.
	hiddenPoisonSource := base +
		` | eventstats min(eventstats_min_object) AS low BY eventstats_min_group` +
		` | fields event_id`
	hiddenPoisonPlan := buildIntegrationPlan(
		t,
		hiddenPoisonSource,
		indexTime.Add(10*time.Second),
		visibilityCutoff,
	)
	hiddenCatalog, compileErr := (Compiler{}).CompileFieldCatalog(
		hiddenPoisonPlan,
		FieldCatalogSpec{MaximumFields: 64},
	)
	if compileErr != nil {
		t.Fatalf("compile hidden-poison field catalog: %v", compileErr)
	}
	hiddenCatalogErr := executeCompiledExpectingNoRows(
		ctx,
		connection,
		CompiledQuery{SQL: hiddenCatalog.SQL, Args: hiddenCatalog.Args},
	)
	if hiddenCatalogErr == nil ||
		!strings.Contains(hiddenCatalogErr.Error(), UnsupportedStatsMeasureValueMarker) {
		t.Fatalf(
			"hidden-poison field catalog error = %v, want atomic unsupported-value marker",
			hiddenCatalogErr,
		)
	}
	hiddenSuggestions, compileErr := (Compiler{}).CompileFieldSuggestions(
		hiddenPoisonPlan,
		FieldSuggestionSpec{Prefix: "event", MaximumFields: 10},
	)
	if compileErr != nil {
		t.Fatalf("compile hidden-poison field suggestions: %v", compileErr)
	}
	hiddenSuggestionsErr := executeCompiledExpectingNoRows(
		ctx,
		connection,
		CompiledQuery{SQL: hiddenSuggestions.SQL, Args: hiddenSuggestions.Args},
	)
	if hiddenSuggestionsErr == nil ||
		!strings.Contains(hiddenSuggestionsErr.Error(), UnsupportedStatsMeasureValueMarker) {
		t.Fatalf(
			"hidden-poison field suggestions error = %v, want atomic unsupported-value marker",
			hiddenSuggestionsErr,
		)
	}

	// Only the first extrema result remains materialized in a stacked graph.
	// The ordinary second result must still be consumed by whole-result
	// validation when all of its public columns and rows are pruned downstream.
	stackedHiddenPoison := compile(
		base + ` | eventstats min(eventstats_min_value) AS safe` +
			` | eventstats min(eventstats_min_object) AS discarded` +
			` | fields event_id | search event_id="not-present"`,
	)
	stackedHiddenPoisonErr := executeCompiledExpectingNoRows(
		ctx,
		connection,
		stackedHiddenPoison,
	)
	if stackedHiddenPoisonErr == nil ||
		!strings.Contains(
			stackedHiddenPoisonErr.Error(),
			UnsupportedStatsMeasureValueMarker,
		) {
		t.Fatalf(
			"stacked hidden eventstats min error = %v, want atomic unsupported-measure marker",
			stackedHiddenPoisonErr,
		)
	}

	exact := compile(
		base + ` | eventstats min(eventstats_min_exact) AS low | head 1 | table low`,
	)
	exactControl := `SELECT
		dynamicType(low),
		if(dynamicType(low) = 'Map(String, String)',
			dynamicElement(low, 'Map(String, String)')[concat(char(0), 'open_splunk_type')], ''),
		if(dynamicType(low) = 'Map(String, String)',
			dynamicElement(low, 'Map(String, String)')[concat(char(0), 'open_splunk_value')], toString(low))
		FROM (` + exact.SQL + `)`
	var exactPhysical, exactTag, exactValue string
	if queryErr := connection.QueryRow(
		ctx,
		exactControl,
		exact.Args...,
	).Scan(&exactPhysical, &exactTag, &exactValue); queryErr != nil {
		t.Fatalf("execute exact Decimal eventstats min: %v\nSQL: %s", queryErr, exact.SQL)
	}
	if exactPhysical != "Map(String, String)" ||
		exactTag != "decimal/v1" || exactValue != exactMinimum {
		t.Fatalf(
			"exact Decimal eventstats min = %q/%q/%q, want Map/decimal/v1/%s",
			exactPhysical,
			exactTag,
			exactValue,
			exactMinimum,
		)
	}

	incomplete := collectDynamicRows(
		"incomplete grouped eventstats min",
		compile(
			base+` (event_id="eventstats-min-07-missing-group"`+
				` OR event_id="eventstats-min-11-complete-group")`+
				` | eventstats min(eventstats_min_incomplete) AS low BY required_group`+
				` | sort event_id | table event_id low`,
		),
	)
	if want := []dynamicRow{
		{id: allIDs[6]},
		{id: allIDs[10], value: float64(7)},
	}; !reflect.DeepEqual(incomplete, want) {
		t.Fatalf("incomplete grouped eventstats min = %#v, want %#v", incomplete, want)
	}

	projected := collectDynamicRows(
		"projected-away eventstats min",
		compile(
			base+` | fields event_id | eventstats min(eventstats_min_value) AS low`+
				` | sort event_id | head 1 | table event_id low`,
		),
	)
	if want := []dynamicRow{{id: allIDs[0]}}; !reflect.DeepEqual(projected, want) {
		t.Fatalf("projected-away eventstats min = %#v, want %#v", projected, want)
	}
	assertPresence(
		"projected-away eventstats min",
		base+` | fields event_id | eventstats min(eventstats_min_value) AS low`,
		uint64(len(events)),
		uint64(len(events)),
		0,
	)

	aliased := collectDynamicRows(
		"aliased eventstats min",
		compile(
			base+` | eventstats min(eventstats_min_value) AS eventstats_min_value`+
				` | sort event_id | table event_id eventstats_min_value`,
		),
	)
	if want := dynamicRows(allIDs...); !reflect.DeepEqual(aliased, want) {
		t.Fatalf("aliased eventstats min = %#v, want %#v", aliased, want)
	}

	downstream := collectDynamicRows(
		"downstream eventstats min",
		compile(
			base+` | eventstats min(eventstats_min_value) AS low`+
				` | where event_id="eventstats-min-04-lexical-text"`+
				` | table event_id low`,
		),
	)
	if want := []dynamicRow{{id: allIDs[3], value: float64(1)}}; !reflect.DeepEqual(downstream, want) {
		t.Fatalf("downstream eventstats min = %#v, want %#v", downstream, want)
	}

	reaggregated := compile(
		base + ` | eventstats min(eventstats_min_value) AS low` +
			` | stats max(low) AS repeated | table repeated`,
	)
	var repeated chcol.Dynamic
	if queryErr := connection.QueryRow(
		ctx,
		reaggregated.SQL,
		reaggregated.Args...,
	).Scan(&repeated); queryErr != nil {
		t.Fatalf("execute reaggregated eventstats min: %v\nSQL: %s", queryErr, reaggregated.SQL)
	}
	if repeated.Any() != float64(1) {
		t.Fatalf("reaggregated eventstats min = %#v, want 1", repeated.Any())
	}

	for _, test := range []struct {
		name   string
		source string
		want   any
	}{
		{
			name: "chronological aggregate before eventstats minimum",
			source: base +
				` | stats earliest(eventstats_min_value) AS first` +
				` | eventstats min(first) AS low | table low`,
			want: float64(10),
		},
		{
			name: "eventstats minimum before chronological aggregate",
			source: base +
				` | eventstats min(eventstats_min_value) AS low` +
				` | stats earliest(low) AS first | table first`,
			want: "1",
		},
	} {
		query := compile(test.source)
		var got chcol.Dynamic
		if queryErr := connection.QueryRow(
			ctx,
			query.SQL,
			query.Args...,
		).Scan(&got); queryErr != nil {
			t.Fatalf("execute %s: %v\nSQL: %s", test.name, queryErr, query.SQL)
		}
		if !reflect.DeepEqual(got.Any(), test.want) {
			t.Fatalf("%s = %#v, want %v", test.name, got.Any(), test.want)
		}
	}

	severity := compile(
		base + ` | eventstats min(severity) AS low | head 1 | table low`,
	)
	var lowSeverity *uint8
	if queryErr := connection.QueryRow(
		ctx,
		severity.SQL,
		severity.Args...,
	).Scan(&lowSeverity); queryErr != nil {
		t.Fatalf("execute native UInt8 eventstats min: %v\nSQL: %s", queryErr, severity.SQL)
	}
	wantSeverity := uint8(opensplunkv1.LogSeverity_LOG_SEVERITY_TRACE)
	if lowSeverity == nil || *lowSeverity != wantSeverity {
		t.Fatalf("native UInt8 eventstats min = %v, want %d", lowSeverity, wantSeverity)
	}

	earliest := compile(
		base + ` | eventstats min(_time) AS earliest | head 1 | table earliest`,
	)
	var earliestTime *time.Time
	if queryErr := connection.QueryRow(
		ctx,
		earliest.SQL,
		earliest.Args...,
	).Scan(&earliestTime); queryErr != nil {
		t.Fatalf("execute native time eventstats min: %v\nSQL: %s", queryErr, earliest.SQL)
	}
	wantEarliest := events[0].Event.EventTime.AsTime()
	if earliestTime == nil || !earliestTime.Equal(wantEarliest) {
		t.Fatalf("native time eventstats min = %v, want %s", earliestTime, wantEarliest)
	}

	boolean := compile(
		base + ` | eval selected=false` +
			` | eventstats min(selected) AS lowest | head 1 | table lowest`,
	)
	var lowest *bool
	if queryErr := connection.QueryRow(
		ctx,
		boolean.SQL,
		boolean.Args...,
	).Scan(&lowest); queryErr != nil {
		t.Fatalf("execute native Bool eventstats min: %v\nSQL: %s", queryErr, boolean.SQL)
	}
	if lowest == nil || *lowest {
		t.Fatalf("native Bool eventstats min = %v, want false", lowest)
	}

	scoped := collectDynamicRows(
		"scoped eventstats min poison",
		compile(
			base+` | eventstats min(eventstats_min_scope) AS low`+
				` | sort event_id | head 1 | table event_id low`,
		),
	)
	if want := []dynamicRow{{id: allIDs[0]}}; !reflect.DeepEqual(scoped, want) {
		t.Fatalf("scoped eventstats min poison = %#v, want %#v", scoped, want)
	}

	for name, field := range map[string]string{
		"flattened object": "eventstats_min_object",
		"nested list":      "eventstats_min_nested",
	} {
		query := compile(
			base + ` | eventstats min(` + field + `) AS low` +
				` | search event_id="not-present" | table low`,
		)
		queryErr := executeCompiledExpectingNoRows(ctx, connection, query)
		if queryErr == nil ||
			!strings.Contains(queryErr.Error(), UnsupportedStatsMeasureValueMarker) {
			t.Fatalf(
				"%s eventstats min error = %v, want atomic unsupported-value marker",
				name,
				queryErr,
			)
		}
	}

	overflow := compileIntegrationSPLForIndex(
		t,
		`index=eventstats-boundary source="eventstats-boundary"`+
			` | eventstats min(eventstats_min_missing) AS low`+
			` | search event_id="not-present" | table low`,
		indexTime.Add(10*time.Second),
		visibilityCutoff,
		"eventstats-boundary",
	)
	overflowErr := executeCompiledExpectingNoRows(ctx, connection, overflow)
	if overflowErr == nil ||
		!strings.Contains(overflowErr.Error(), EventStatsInputLimitMarker) {
		t.Fatalf(
			"10,001-row eventstats min error = %v, want atomic limit failure",
			overflowErr,
		)
	}
}
