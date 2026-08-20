package clickhouse

import (
	"context"
	"reflect"
	"strings"
	"testing"
	"time"

	clickhousedriver "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/chcol"
	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/ingest"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const streamStatsMinimumIntegrationCollectorID = "streamstats-min-integration-collector"

// testStreamStatsMinimumAgainstClickHouse pins the mixed extrema contract to
// the production ClickHouse image. It deliberately reuses the preceding
// eventstats boundary corpus rather than ingesting another 10,001 events.
func testStreamStatsMinimumAgainstClickHouse(
	t *testing.T,
	ctx context.Context,
	store *Store,
	connection clickhousedriver.Conn,
	indexTime time.Time,
) {
	t.Helper()

	newEvent := func(
		id string,
		fields ...*opensplunk.TypedObjectField,
	) *ingest.StoredEvent {
		event := compilerIntegrationEvent(
			id,
			"streamstats-min-host",
			"streamstats min fixture",
			indexTime,
			fields...,
		)
		event.BatchID = "streamstats-min-batch"
		event.CollectorID = streamStatsMinimumIntegrationCollectorID
		event.Event.Source = "streamstats-min-fixture"
		return event
	}
	withGroup := func(event *ingest.StoredEvent, group string) *ingest.StoredEvent {
		event.Event.Fields.Fields = append(
			[]*opensplunk.TypedObjectField{
				typedField("streamstats_min_group", typedString(group)),
			},
			event.Event.Fields.Fields...,
		)
		return event
	}

	const exactMinimum = "9007199254740992.75"
	events := []*ingest.StoredEvent{
		withGroup(newEvent(
			"streamstats-min-01-int",
			typedField("streamstats_min_value", typedSint(10)),
			typedField("streamstats_min_exact", typedDecimal(exactMinimum)),
		), "A"),
		withGroup(newEvent(
			"streamstats-min-02-multivalue",
			typedField("streamstats_min_value", typedList(
				typedSint(2),
				typedString("01"),
				typedString("1.0"),
				typedString("z"),
				typedNull(),
			)),
			typedField("streamstats_min_exact", typedUint(9_007_199_254_740_993)),
		), "A"),
		withGroup(newEvent(
			"streamstats-min-03-numeric-text",
			typedField("streamstats_min_value", typedString("20")),
		), "B"),
		withGroup(newEvent(
			"streamstats-min-04-lexical-text",
			typedField("streamstats_min_value", typedString("alpha")),
		), "B"),
		withGroup(newEvent("streamstats-min-05-missing"), "B"),
		withGroup(newEvent(
			"streamstats-min-06-null",
			typedField("streamstats_min_value", typedNull()),
		), "B"),
		newEvent(
			"streamstats-min-07-missing-group",
			typedField("streamstats_min_incomplete", typedObject(
				typedField("child", typedString("must-not-poison")),
			)),
		),
		newEvent(
			"streamstats-min-08-null-group",
			typedField("streamstats_min_group", typedNull()),
		),
		withGroup(newEvent(
			"streamstats-min-09-object-poison",
			typedField("streamstats_min_object", typedObject(
				typedField("child", typedString("secret")),
			)),
		), "C"),
		withGroup(newEvent(
			"streamstats-min-10-nested-poison",
			typedField("streamstats_min_nested", typedList(
				typedString("safe"),
				typedList(typedString("secret")),
			)),
		), "C"),
		withGroup(newEvent(
			"streamstats-min-11-complete-group",
			typedField("required_group", typedString("present")),
			typedField("streamstats_min_incomplete", typedString("7")),
			typedField("streamstats_min_final_poison", typedObject(
				typedField("child", typedString("must-still-validate")),
			)),
		), "D"),
	}
	for eventIndex, event := range events {
		event.Event.EventTime = timestamppb.New(
			indexTime.Add(time.Duration(eventIndex) * time.Nanosecond),
		)
	}

	if _, err := store.Store(ctx, ingest.StoreBatch{
		TenantID:           "tenant",
		CollectorID:        streamStatsMinimumIntegrationCollectorID,
		BatchID:            "streamstats-min-batch",
		BatchSequence:      92,
		OriginalEventCount: uint32(len(events)),
		SourceBatchSHA256:  testSourceBatchDigest("streamstats-min-batch"),
		ReceivedAt:         indexTime,
		Events:             events,
	}); err != nil {
		t.Fatalf("store streamstats min fixtures: %v", err)
	}

	visibilityCutoff, err := store.VisibilityCutoff(ctx)
	if err != nil {
		t.Fatalf("capture streamstats min visibility cutoff: %v", err)
	}
	compile := func(source string) CompiledQuery {
		t.Helper()
		return compileIntegrationSPL(
			t,
			source,
			indexTime.Add(20*time.Second),
			visibilityCutoff,
		)
	}
	compileBoundary := func(source string) CompiledQuery {
		t.Helper()
		return compileIntegrationSPLForIndex(
			t,
			source,
			indexTime.Add(20*time.Second),
			visibilityCutoff,
			"eventstats-boundary",
		)
	}
	base := `index=compiler source="streamstats-min-fixture"`
	ordered := base + ` | sort 0 +event_id`

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
		"streamstats-min-01-int",
		"streamstats-min-02-multivalue",
		"streamstats-min-03-numeric-text",
		"streamstats-min-04-lexical-text",
		"streamstats-min-05-missing",
		"streamstats-min-06-null",
		"streamstats-min-07-missing-group",
		"streamstats-min-08-null-group",
		"streamstats-min-09-object-poison",
		"streamstats-min-10-nested-poison",
		"streamstats-min-11-complete-group",
	}
	rowsWithValues := func(values ...any) []dynamicRow {
		t.Helper()
		if len(values) != len(allIDs) {
			t.Fatalf("streamstats min expectation has %d values, want %d", len(values), len(allIDs))
		}
		rows := make([]dynamicRow, len(allIDs))
		for index, id := range allIDs {
			rows[index] = dynamicRow{id: id, value: values[index]}
		}
		return rows
	}

	for _, test := range []struct {
		name    string
		options string
		want    []dynamicRow
	}{
		{
			name: "complete current prefix",
			want: rowsWithValues(
				float64(10), float64(1), float64(1), float64(1),
				float64(1), float64(1), float64(1), float64(1),
				float64(1), float64(1), float64(1),
			),
		},
		{
			name:    "complete prior prefix",
			options: "current=false",
			want: rowsWithValues(
				nil, float64(10), float64(1), float64(1),
				float64(1), float64(1), float64(1), float64(1),
				float64(1), float64(1), float64(1),
			),
		},
		{
			name:    "one-row current window",
			options: "window=1",
			want: rowsWithValues(
				float64(10), float64(1), float64(20), "alpha",
				nil, nil, nil, nil, nil, nil, nil,
			),
		},
		{
			name:    "one-row prior window",
			options: "current=false window=1",
			want: rowsWithValues(
				nil, float64(10), float64(1), float64(20),
				"alpha", nil, nil, nil, nil, nil, nil,
			),
		},
		{
			name:    "two-row current window",
			options: "window=2",
			want: rowsWithValues(
				float64(10), float64(1), float64(1), float64(20),
				"alpha", nil, nil, nil, nil, nil, nil,
			),
		},
		{
			name:    "two-row prior window",
			options: "current=false window=2",
			want: rowsWithValues(
				nil, float64(10), float64(1), float64(1),
				float64(20), "alpha", nil, nil, nil, nil, nil,
			),
		},
	} {
		command := ` | streamstats`
		if test.options != "" {
			command += " " + test.options
		}
		query := compile(
			ordered + command +
				` min(streamstats_min_value) AS low | table event_id low`,
		)
		if test.name == "complete current prefix" {
			assertBoundedStreamStatsSQL(t, query)
		}
		if got := collectDynamicRows(test.name, query); !reflect.DeepEqual(got, test.want) {
			t.Fatalf("%s = %#v, want %#v", test.name, got, test.want)
		}
	}

	grouped := collectDynamicRows(
		"grouped two-row window",
		compile(
			ordered+` | streamstats window=2 global=false`+
				` min(streamstats_min_value) AS low BY streamstats_min_group`+
				` | table event_id low`,
		),
	)
	groupedWant := rowsWithValues(
		float64(10), float64(1), float64(20), float64(20),
		"alpha", nil, nil, nil, nil, nil, nil,
	)
	if !reflect.DeepEqual(grouped, groupedWant) {
		t.Fatalf("grouped two-row streamstats min = %#v, want %#v", grouped, groupedWant)
	}

	// The object lives on the row whose BY key is absent. Group eligibility must
	// be established before traversing the measure, while the complete group
	// must still receive its ordinary numeric minimum.
	incomplete := collectDynamicRows(
		"incomplete grouped minimum",
		compile(
			base+` (event_id="streamstats-min-07-missing-group"`+
				` OR event_id="streamstats-min-11-complete-group")`+
				` | sort 0 +event_id`+
				` | streamstats window=2 global=false`+
				` min(streamstats_min_incomplete) AS low BY required_group`+
				` | table event_id low`,
		),
	)
	if want := []dynamicRow{
		{id: allIDs[6]},
		{id: allIDs[10], value: float64(7)},
	}; !reflect.DeepEqual(incomplete, want) {
		t.Fatalf("incomplete grouped streamstats min = %#v, want %#v", incomplete, want)
	}

	projected := collectDynamicRows(
		"projected-away minimum input",
		compile(
			ordered+` | fields event_id`+
				` | streamstats min(streamstats_min_value) AS low`+
				` | table event_id low`,
		),
	)
	if want := rowsWithValues(nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil); !reflect.DeepEqual(projected, want) {
		t.Fatalf("projected-away streamstats min = %#v, want %#v", projected, want)
	}

	aliased := collectDynamicRows(
		"minimum alias replacement",
		compile(
			ordered+` | streamstats min(streamstats_min_value) AS streamstats_min_value`+
				` | table event_id streamstats_min_value`,
		),
	)
	if want := rowsWithValues(
		float64(10), float64(1), float64(1), float64(1),
		float64(1), float64(1), float64(1), float64(1),
		float64(1), float64(1), float64(1),
	); !reflect.DeepEqual(aliased, want) {
		t.Fatalf("aliased streamstats min = %#v, want %#v", aliased, want)
	}

	exact := compile(
		ordered + ` | streamstats min(streamstats_min_exact) AS low` +
			` | where event_id="streamstats-min-02-multivalue" | table low`,
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
		t.Fatalf("execute exact Decimal streamstats min: %v\nSQL: %s", queryErr, exact.SQL)
	}
	if exactPhysical != "Map(String, String)" ||
		exactTag != "decimal/v1" || exactValue != exactMinimum {
		t.Fatalf(
			"exact Decimal streamstats min = %q/%q/%q, want Map/decimal/v1/%s",
			exactPhysical,
			exactTag,
			exactValue,
			exactMinimum,
		)
	}

	// Fixed scalar inputs stay on their native ClickHouse types instead of
	// round-tripping through the mixed publication tuple.
	firstTime := compile(
		ordered + ` | streamstats min(_time) AS first_time | tail 1 | table first_time`,
	)
	var gotFirstTime time.Time
	if queryErr := connection.QueryRow(
		ctx,
		firstTime.SQL,
		firstTime.Args...,
	).Scan(&gotFirstTime); queryErr != nil {
		t.Fatalf("execute native DateTime64 streamstats min: %v", queryErr)
	}
	if !gotFirstTime.Equal(indexTime) {
		t.Fatalf("native DateTime64 streamstats min = %s, want %s", gotFirstTime, indexTime)
	}

	minimumBool := compile(
		ordered +
			` | eval selected=if(event_id="streamstats-min-01-int", true, false)` +
			` | streamstats min(selected) AS all_selected | tail 1 | table all_selected`,
	)
	var gotMinimumBool bool
	if queryErr := connection.QueryRow(
		ctx,
		minimumBool.SQL,
		minimumBool.Args...,
	).Scan(&gotMinimumBool); queryErr != nil {
		t.Fatalf("execute native Bool streamstats min: %v", queryErr)
	}
	if gotMinimumBool {
		t.Fatal("native Bool streamstats min = true, want false")
	}

	minimumHost := compile(
		ordered + ` | streamstats min(host) AS first_host | tail 1 | table first_host`,
	)
	var gotMinimumHost chcol.Dynamic
	if queryErr := connection.QueryRow(
		ctx,
		minimumHost.SQL,
		minimumHost.Args...,
	).Scan(&gotMinimumHost); queryErr != nil {
		t.Fatalf("execute fixed String streamstats min: %v", queryErr)
	}
	if got, want := gotMinimumHost.Any(), any("streamstats-min-host"); got != want {
		t.Fatalf("fixed String streamstats min = %#v, want %#v", got, want)
	}

	minimumStringArray := compile(
		base + ` | stats values(streamstats_min_group) AS groups` +
			` | streamstats min(groups) AS first_group | table first_group`,
	)
	var gotMinimumStringArray chcol.Dynamic
	if queryErr := connection.QueryRow(
		ctx,
		minimumStringArray.SQL,
		minimumStringArray.Args...,
	).Scan(&gotMinimumStringArray); queryErr != nil {
		t.Fatalf("execute fixed Array(String) streamstats min: %v", queryErr)
	}
	if got, want := gotMinimumStringArray.Any(), any("A"); got != want {
		t.Fatalf("fixed Array(String) streamstats min = %#v, want %#v", got, want)
	}

	// Downstream projection and an empty result must not prune poison
	// validation. Exercise both a direct object and a nested container.
	for _, test := range []struct {
		name    string
		options string
		field   string
	}{
		{name: "flattened object", field: "streamstats_min_object"},
		{name: "nested list", field: "streamstats_min_nested"},
		{
			name:    "final-row object excluded from prior frame",
			options: " current=false",
			field:   "streamstats_min_final_poison",
		},
	} {
		query := compile(
			ordered + ` | streamstats` + test.options + ` min(` + test.field + `) AS low` +
				` | fields event_id | search event_id="not-present"`,
		)
		queryErr := executeCompiledExpectingNoRows(ctx, connection, query)
		if queryErr == nil ||
			!strings.Contains(queryErr.Error(), UnsupportedStatsMeasureValueMarker) {
			t.Fatalf(
				"%s streamstats min error = %v, want atomic unsupported-value marker",
				test.name,
				queryErr,
			)
		}
	}

	exactBoundary := compileBoundary(
		`index=eventstats-boundary source="eventstats-boundary" host="in"` +
			` | streamstats window=10000 min(streamstats_min_missing) AS low` +
			` | head 1 | table low`,
	)
	assertBoundedStreamStatsSQL(t, exactBoundary)
	var exactEmpty chcol.Dynamic
	if queryErr := connection.QueryRow(
		ctx,
		exactBoundary.SQL,
		exactBoundary.Args...,
	).Scan(&exactEmpty); queryErr != nil {
		t.Fatalf(
			"execute exact streamstats min boundary: %v\nSQL: %s",
			queryErr,
			exactBoundary.SQL,
		)
	}
	if exactEmpty.Any() != nil {
		t.Fatalf("exact streamstats min boundary = %#v, want null", exactEmpty.Any())
	}

	hiddenOverflow := compileBoundary(
		`index=eventstats-boundary source="eventstats-boundary"` +
			` | streamstats min(streamstats_min_missing) AS low` +
			` | fields event_id | search event_id="not-present"`,
	)
	assertBoundedStreamStatsSQL(t, hiddenOverflow)
	overflowErr := executeCompiledExpectingNoRows(ctx, connection, hiddenOverflow)
	if overflowErr == nil ||
		!strings.Contains(overflowErr.Error(), StreamStatsInputLimitMarker) {
		t.Fatalf(
			"downstream-hidden streamstats min overflow error = %v, want %q",
			overflowErr,
			StreamStatsInputLimitMarker,
		)
	}
}
