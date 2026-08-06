package clickhouse

import (
	"context"
	"encoding/base64"
	"reflect"
	"strings"
	"testing"
	"time"

	clickhousedriver "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/chcol"
	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/eventfields"
	"github.com/Suhaibinator/open-splunk/internal/ingest"
)

const streamStatsMaximumIntegrationCollectorID = "streamstats-max-integration-collector"

// testStreamStatsMaximumAgainstClickHouse reuses the preceding minimum and
// eventstats-boundary fixtures. Direction-specific expectations ensure the
// compiler cannot merely swap the outer window aggregate around a minimum row
// fold. Two typed-Bytes events invert raw-byte and base64-text order so every
// shared extrema path must preserve semantic ordering and publication metadata.
func testStreamStatsMaximumAgainstClickHouse(
	t *testing.T,
	ctx context.Context,
	store *Store,
	connection clickhousedriver.Conn,
	indexTime time.Time,
) {
	t.Helper()

	rawMinimum := []byte{0xc2, 0xa2}
	rawMaximum := []byte{0xd0, 0x90}
	bytesEvents := make([]*ingest.StoredEvent, 0, 4)
	for _, fixture := range []struct {
		id    string
		value []byte
	}{
		{id: "streamstats-max-bytes-01", value: rawMinimum},
		{id: "streamstats-max-bytes-02", value: rawMaximum},
	} {
		tieValue := typedBytes(rawMinimum)
		emptyTieValue := typedBytes(nil)
		if fixture.id == "streamstats-max-bytes-01" {
			tieValue = typedString(string(rawMinimum))
			emptyTieValue = typedString("")
		}
		event := compilerIntegrationEvent(
			fixture.id,
			"streamstats-max-host",
			string(fixture.value),
			indexTime,
			typedField("streamstats_max_bytes", typedBytes(fixture.value)),
			typedField("streamstats_max_tie", tieValue),
			typedField("streamstats_max_empty_tie", emptyTieValue),
		)
		event.BatchID = "streamstats-max-bytes-batch"
		event.CollectorID = streamStatsMaximumIntegrationCollectorID
		event.Event.Source = "streamstats-max-bytes-fixture"
		event.Event.RawEncoding = opensplunkv1.RawEncoding_RAW_ENCODING_BINARY
		bytesEvents = append(bytesEvents, event)
	}
	for _, fixture := range []struct {
		id       string
		encoding opensplunkv1.RawEncoding
	}{
		{id: "streamstats-max-raw-tie-01-string", encoding: opensplunkv1.RawEncoding_RAW_ENCODING_UTF8},
		{id: "streamstats-max-raw-tie-02-bytes", encoding: opensplunkv1.RawEncoding_RAW_ENCODING_BINARY},
	} {
		event := compilerIntegrationEvent(
			fixture.id,
			"streamstats-max-host",
			string(rawMinimum),
			indexTime,
		)
		event.BatchID = "streamstats-max-bytes-batch"
		event.CollectorID = streamStatsMaximumIntegrationCollectorID
		event.Event.Source = "streamstats-max-raw-tie-fixture"
		event.Event.RawEncoding = fixture.encoding
		bytesEvents = append(bytesEvents, event)
	}
	if _, err := store.Store(ctx, ingest.StoreBatch{
		TenantID:           "tenant",
		CollectorID:        streamStatsMaximumIntegrationCollectorID,
		BatchID:            "streamstats-max-bytes-batch",
		BatchSequence:      1,
		OriginalEventCount: 4,
		SourceBatchSHA256:  testSourceBatchDigest("streamstats-max-bytes-batch"),
		ReceivedAt:         indexTime,
		Events:             bytesEvents,
	}); err != nil {
		t.Fatalf("store streamstats max byte fixture: %v", err)
	}

	visibilityCutoff, err := store.VisibilityCutoff(ctx)
	if err != nil {
		t.Fatalf("capture streamstats max visibility cutoff: %v", err)
	}
	binEdgeInsertRawDecimalEnvelopes(
		t,
		ctx,
		connection,
		"streamstats-max-malformed-bytes",
		[]binEdgeRawDecimalEnvelope{
			{
				eventID:       "streamstats-max-malformed-bytes",
				tenantID:      "tenant",
				indexName:     "compiler",
				eventTime:     indexTime,
				indexTime:     indexTime,
				visibilitySeq: visibilityCutoff,
				fieldName:     "streamstats_max_malformed",
				fieldType:     eventfields.StoredValueTypeBytes,
				envelope: map[string]string{
					"\x00open_splunk_type":  "bytes/v1",
					"\x00open_splunk_value": "AB",
				},
			},
		},
	)
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

	type maximumDynamicRow struct {
		id    string
		value any
	}
	collectDynamicRows := func(name string, query CompiledQuery) []maximumDynamicRow {
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
		var got []maximumDynamicRow
		for rows.Next() {
			var id string
			var value chcol.Dynamic
			if scanErr := rows.Scan(&id, &value); scanErr != nil {
				_ = rows.Close()
				t.Fatalf("scan %s: %v", name, scanErr)
			}
			got = append(got, maximumDynamicRow{id: id, value: value.Any()})
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
	rowsWithValues := func(values ...any) []maximumDynamicRow {
		t.Helper()
		if len(values) != len(allIDs) {
			t.Fatalf("streamstats max expectation has %d values, want %d", len(values), len(allIDs))
		}
		rows := make([]maximumDynamicRow, len(allIDs))
		for index, id := range allIDs {
			rows[index] = maximumDynamicRow{id: id, value: values[index]}
		}
		return rows
	}

	// These six queries pin every supported frame. The second source row's
	// multivalue maximum is lexical "z", whereas its minimum is numeric 1.
	for _, test := range []struct {
		name    string
		options string
		want    []maximumDynamicRow
	}{
		{
			name: "complete current prefix",
			want: rowsWithValues(
				float64(10), "z", "z", "z", "z", "z",
				"z", "z", "z", "z", "z",
			),
		},
		{
			name:    "complete prior prefix",
			options: "current=false",
			want: rowsWithValues(
				nil, float64(10), "z", "z", "z", "z",
				"z", "z", "z", "z", "z",
			),
		},
		{
			name:    "one-row current window",
			options: "window=1",
			want: rowsWithValues(
				float64(10), "z", float64(20), "alpha", nil, nil,
				nil, nil, nil, nil, nil,
			),
		},
		{
			name:    "one-row prior window",
			options: "current=false window=1",
			want: rowsWithValues(
				nil, float64(10), "z", float64(20), "alpha", nil,
				nil, nil, nil, nil, nil,
			),
		},
		{
			name:    "two-row current window",
			options: "window=2",
			want: rowsWithValues(
				float64(10), "z", "z", "alpha", "alpha", nil,
				nil, nil, nil, nil, nil,
			),
		},
		{
			name:    "two-row prior window",
			options: "current=false window=2",
			want: rowsWithValues(
				nil, float64(10), "z", "z", "alpha", "alpha",
				nil, nil, nil, nil, nil,
			),
		},
	} {
		command := ` | streamstats`
		if test.options != "" {
			command += " " + test.options
		}
		query := compile(
			ordered + command +
				` max(streamstats_min_value) AS high | table event_id high`,
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
				` max(streamstats_min_value) AS high BY streamstats_min_group`+
				` | table event_id high`,
		),
	)
	groupedWant := rowsWithValues(
		float64(10), "z", float64(20), "alpha", "alpha", nil,
		nil, nil, nil, nil, nil,
	)
	if !reflect.DeepEqual(grouped, groupedWant) {
		t.Fatalf("grouped two-row streamstats max = %#v, want %#v", grouped, groupedWant)
	}

	// The object is on a row with no complete BY key and must be neither walked
	// nor published. The complete group still receives its ordinary winner.
	incomplete := collectDynamicRows(
		"incomplete grouped maximum",
		compile(
			base+` (event_id="streamstats-min-07-missing-group"`+
				` OR event_id="streamstats-min-11-complete-group")`+
				` | sort 0 +event_id`+
				` | streamstats window=2 global=false`+
				` max(streamstats_min_incomplete) AS high BY required_group`+
				` | table event_id high`,
		),
	)
	if want := []maximumDynamicRow{
		{id: allIDs[6]},
		{id: allIDs[10], value: float64(7)},
	}; !reflect.DeepEqual(incomplete, want) {
		t.Fatalf("incomplete grouped streamstats max = %#v, want %#v", incomplete, want)
	}

	projected := collectDynamicRows(
		"projected-away maximum input",
		compile(
			ordered+` | fields event_id`+
				` | streamstats max(streamstats_min_value) AS high`+
				` | table event_id high`,
		),
	)
	if want := rowsWithValues(nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil); !reflect.DeepEqual(projected, want) {
		t.Fatalf("projected-away streamstats max = %#v, want %#v", projected, want)
	}

	stacked := compile(
		ordered + ` | streamstats max(streamstats_min_value) AS high` +
			` | streamstats max(high) AS higher` +
			` | tail 1 | table event_id high higher`,
	)
	var stackedID string
	var high, higher chcol.Dynamic
	if scanErr := connection.QueryRow(
		ctx,
		stacked.SQL,
		stacked.Args...,
	).Scan(&stackedID, &high, &higher); scanErr != nil {
		t.Fatalf("execute stacked streamstats max: %v", scanErr)
	}
	if stackedID != allIDs[10] || high.Any() != "z" || higher.Any() != "z" {
		t.Fatalf(
			"stacked streamstats max = %q/%#v/%#v, want %q/z/z",
			stackedID,
			high.Any(),
			higher.Any(),
			allIDs[10],
		)
	}

	const exactMaximum = "9007199254740993"
	exact := compile(
		ordered + ` | streamstats max(streamstats_min_exact) AS high` +
			` | where event_id="streamstats-min-02-multivalue" | table high`,
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
		t.Fatalf("execute exact Decimal streamstats max: %v\nSQL: %s", queryErr, exact.SQL)
	}
	if exactPhysical != "Map(String, String)" ||
		exactTag != "decimal/v1" || exactValue != exactMaximum {
		t.Fatalf(
			"exact Decimal streamstats max = %q/%q/%q, want Map/decimal/v1/%s",
			exactPhysical,
			exactTag,
			exactValue,
			exactMaximum,
		)
	}

	latest := compile(
		ordered + ` | streamstats max(_time) AS last_time | tail 1 | table last_time`,
	)
	var gotLatest time.Time
	if queryErr := connection.QueryRow(ctx, latest.SQL, latest.Args...).Scan(&gotLatest); queryErr != nil {
		t.Fatalf("execute native DateTime64 streamstats max: %v", queryErr)
	}
	wantLatest := indexTime.Add(10 * time.Nanosecond)
	if !gotLatest.Equal(wantLatest) {
		t.Fatalf("native DateTime64 streamstats max = %s, want %s", gotLatest, wantLatest)
	}

	maximumBool := compile(
		ordered +
			` | eval selected=if(event_id="streamstats-min-01-int", true, false)` +
			` | streamstats max(selected) AS any_selected | tail 1 | table any_selected`,
	)
	var gotMaximumBool bool
	if queryErr := connection.QueryRow(ctx, maximumBool.SQL, maximumBool.Args...).Scan(&gotMaximumBool); queryErr != nil {
		t.Fatalf("execute native Bool streamstats max: %v", queryErr)
	}
	if !gotMaximumBool {
		t.Fatal("native Bool streamstats max = false, want true")
	}

	maximumHost := compile(
		ordered + ` | streamstats max(host) AS last_host | tail 1 | table last_host`,
	)
	var gotMaximumHost chcol.Dynamic
	if queryErr := connection.QueryRow(ctx, maximumHost.SQL, maximumHost.Args...).Scan(&gotMaximumHost); queryErr != nil {
		t.Fatalf("execute fixed String streamstats max: %v", queryErr)
	}
	if got, want := gotMaximumHost.Any(), any("streamstats-min-host"); got != want {
		t.Fatalf("fixed String streamstats max = %#v, want %#v", got, want)
	}

	maximumStringArray := compile(
		base + ` | stats values(streamstats_min_group) AS groups` +
			` | streamstats max(groups) AS last_group | table last_group`,
	)
	var gotMaximumStringArray chcol.Dynamic
	if queryErr := connection.QueryRow(
		ctx,
		maximumStringArray.SQL,
		maximumStringArray.Args...,
	).Scan(&gotMaximumStringArray); queryErr != nil {
		t.Fatalf("execute fixed Array(String) streamstats max: %v", queryErr)
	}
	if got, want := gotMaximumStringArray.Any(), any("D"); got != want {
		t.Fatalf("fixed Array(String) streamstats max = %#v, want %#v", got, want)
	}

	wantEncodedMinimum := base64.RawStdEncoding.EncodeToString(rawMinimum)
	wantEncodedMaximum := base64.RawStdEncoding.EncodeToString(rawMaximum)
	type physicalBytes struct {
		physical string
		tag      string
		payload  string
	}
	assertPhysicalBytes := func(
		name string,
		query CompiledQuery,
		fields []string,
		wantPayloads []string,
	) {
		t.Helper()
		if len(fields) != len(wantPayloads) {
			t.Fatalf("%s fields/payloads = %d/%d", name, len(fields), len(wantPayloads))
		}
		projection := make([]string, 0, len(fields)*3)
		got := make([]physicalBytes, len(fields))
		destinations := make([]any, 0, len(fields)*3)
		for index, field := range fields {
			column := quoteIdentifier(field)
			physical := "dynamicType(" + column + ")"
			mapping := "dynamicElement(" + column + ", 'Map(String, String)')"
			projection = append(
				projection,
				physical,
				"if("+physical+" = 'Map(String, String)', "+mapping+
					"[concat(char(0), 'open_splunk_type')], CAST('' AS String))",
				"if("+physical+" = 'Map(String, String)', "+mapping+
					"[concat(char(0), 'open_splunk_value')], CAST('' AS String))",
			)
			destinations = append(
				destinations,
				&got[index].physical,
				&got[index].tag,
				&got[index].payload,
			)
		}
		control := "SELECT " + strings.Join(projection, ", ") + " FROM (" +
			query.SQL + ")"
		if queryErr := connection.QueryRow(
			ctx,
			control,
			query.Args...,
		).Scan(destinations...); queryErr != nil {
			t.Fatalf("execute %s: %v\nSQL: %s", name, queryErr, query.SQL)
		}
		for index := range fields {
			if got[index].physical != "Map(String, String)" ||
				got[index].tag != "bytes/v1" ||
				got[index].payload != wantPayloads[index] {
				t.Fatalf(
					"%s %s = %#v, want Map(String, String)/bytes/v1/%q",
					name,
					fields[index],
					got[index],
					wantPayloads[index],
				)
			}
		}
	}
	assertPhysicalString := func(
		name string,
		query CompiledQuery,
		field string,
		want string,
	) {
		t.Helper()
		column := quoteIdentifier(field)
		physical := "dynamicType(" + column + ")"
		control := "SELECT " + physical + ", if(" + physical +
			" = 'String', dynamicElement(" + column +
			", 'String'), CAST('' AS String)) FROM (" + query.SQL + ")"
		var gotPhysical, got string
		if queryErr := connection.QueryRow(
			ctx,
			control,
			query.Args...,
		).Scan(&gotPhysical, &got); queryErr != nil {
			t.Fatalf("execute %s: %v\nSQL: %s", name, queryErr, query.SQL)
		}
		if gotPhysical != "String" || got != want {
			t.Fatalf("%s %s = %q/%q, want String/%q", name, field, gotPhysical, got, want)
		}
	}

	bytesBase := `index=compiler source="streamstats-max-bytes-fixture"`
	bytesSource := bytesBase + ` | sort 0 +event_id` +
		` | streamstats max(streamstats_max_bytes) AS high` +
		` | streamstats min(streamstats_max_bytes) AS low` +
		` | streamstats max(high) AS higher | tail 1`
	assertPhysicalBytes(
		"stacked streamstats typed Bytes extrema",
		compile(bytesSource+` | table high low higher`),
		[]string{"high", "low", "higher"},
		[]string{wantEncodedMaximum, wantEncodedMinimum, wantEncodedMaximum},
	)
	assertPhysicalBytes(
		"transforming stats typed Bytes extrema",
		compile(bytesBase+` | stats max(streamstats_max_bytes) AS high`+
			` min(streamstats_max_bytes) AS low | table high low`),
		[]string{"high", "low"},
		[]string{wantEncodedMaximum, wantEncodedMinimum},
	)
	assertPhysicalBytes(
		"eventstats typed Bytes extrema",
		compile(bytesBase+` | eventstats max(streamstats_max_bytes) AS high`+
			` | eventstats min(streamstats_max_bytes) AS low`+
			` | tail 1 | table high low`),
		[]string{"high", "low"},
		[]string{wantEncodedMaximum, wantEncodedMinimum},
	)
	rawSource := bytesBase + ` | sort 0 +event_id` +
		` | streamstats max(_raw) AS high` +
		` | streamstats min(_raw) AS low` +
		` | streamstats max(high) AS higher | tail 1`
	assertPhysicalBytes(
		"binary raw streamstats extrema",
		compile(rawSource+` | table high low higher`),
		[]string{"high", "low", "higher"},
		[]string{wantEncodedMaximum, wantEncodedMinimum, wantEncodedMaximum},
	)
	assertPhysicalBytes(
		"binary raw transforming extrema",
		compile(bytesBase+` | stats max(_raw) AS high min(_raw) AS low | table high low`),
		[]string{"high", "low"},
		[]string{wantEncodedMaximum, wantEncodedMinimum},
	)
	assertPhysicalBytes(
		"binary raw eventstats extrema",
		compile(bytesBase+` | eventstats max(_raw) AS high`+
			` | eventstats min(_raw) AS low | tail 1 | table high low`),
		[]string{"high", "low"},
		[]string{wantEncodedMaximum, wantEncodedMinimum},
	)
	tieStreamSource := bytesBase + ` | sort 0 +event_id` +
		` | streamstats max(streamstats_max_tie) AS high` +
		` | streamstats min(streamstats_max_tie) AS low | tail 1`
	assertPhysicalBytes(
		"equal lexical streamstats maximum prefers Bytes",
		compile(tieStreamSource+` | table high`),
		[]string{"high"},
		[]string{wantEncodedMinimum},
	)
	assertPhysicalString(
		"equal lexical streamstats minimum prefers String",
		compile(tieStreamSource+` | table low`),
		"low",
		string(rawMinimum),
	)
	tieStatsSource := bytesBase +
		` | stats max(streamstats_max_tie) AS high min(streamstats_max_tie) AS low`
	assertPhysicalBytes(
		"equal lexical transforming maximum prefers Bytes",
		compile(tieStatsSource+` | table high`),
		[]string{"high"},
		[]string{wantEncodedMinimum},
	)
	assertPhysicalString(
		"equal lexical transforming minimum prefers String",
		compile(tieStatsSource+` | table low`),
		"low",
		string(rawMinimum),
	)
	emptyTieSource := bytesBase + ` | sort 0 +event_id` +
		` | streamstats max(streamstats_max_empty_tie) AS high` +
		` | streamstats min(streamstats_max_empty_tie) AS low | tail 1`
	assertPhysicalBytes(
		"empty lexical streamstats maximum prefers Bytes",
		compile(emptyTieSource+` | table high`),
		[]string{"high"},
		[]string{""},
	)
	assertPhysicalString(
		"empty lexical streamstats minimum prefers String",
		compile(emptyTieSource+` | table low`),
		"low",
		"",
	)
	rawTieBase := `index=compiler source="streamstats-max-raw-tie-fixture"`
	rawTieStreamSource := rawTieBase + ` | sort 0 +event_id` +
		` | streamstats max(_raw) AS high` +
		` | streamstats min(_raw) AS low | tail 1`
	assertPhysicalBytes(
		"equal raw streamstats maximum prefers binary provenance",
		compile(rawTieStreamSource+` | table high`),
		[]string{"high"},
		[]string{wantEncodedMinimum},
	)
	assertPhysicalString(
		"equal raw streamstats minimum prefers text provenance",
		compile(rawTieStreamSource+` | table low`),
		"low",
		string(rawMinimum),
	)
	rawTieStatsSource := rawTieBase + ` | stats max(_raw) AS high min(_raw) AS low`
	assertPhysicalBytes(
		"equal raw transforming maximum prefers binary provenance",
		compile(rawTieStatsSource+` | table high`),
		[]string{"high"},
		[]string{wantEncodedMinimum},
	)
	assertPhysicalString(
		"equal raw transforming minimum prefers text provenance",
		compile(rawTieStatsSource+` | table low`),
		"low",
		string(rawMinimum),
	)
	rawTieEventSource := rawTieBase + ` | eventstats max(_raw) AS high` +
		` | eventstats min(_raw) AS low | tail 1`
	assertPhysicalBytes(
		"equal raw eventstats maximum prefers binary provenance",
		compile(rawTieEventSource+` | table high`),
		[]string{"high"},
		[]string{wantEncodedMinimum},
	)
	assertPhysicalString(
		"equal raw eventstats minimum prefers text provenance",
		compile(rawTieEventSource+` | table low`),
		"low",
		string(rawMinimum),
	)
	for _, test := range []struct {
		payload string
		valid   uint8
	}{
		{payload: "", valid: 1},
		{payload: "AA", valid: 1},
		{payload: "AAA", valid: 1},
		{payload: "AP8", valid: 1},
		{payload: "0JA", valid: 1},
		{payload: "A"},
		{payload: "AB"},
		{payload: "AAB"},
		{payload: "AA="},
		{payload: "AA-"},
	} {
		validity := newDynamicEnvelopePayloadValiditySQL("?")
		var got uint8
		if queryErr := connection.QueryRow(
			ctx,
			"SELECT toUInt8("+validity.bytesValid+")",
			test.payload,
		).Scan(&got); queryErr != nil {
			t.Fatalf("validate RawStd payload %q: %v", test.payload, queryErr)
		}
		if got != test.valid {
			t.Fatalf("RawStd payload %q validity = %d, want %d", test.payload, got, test.valid)
		}
	}
	malformed := compile(
		`index=compiler source="poison-source" event_id="streamstats-max-malformed-bytes"` +
			` | streamstats max(streamstats_max_malformed) AS high` +
			` | fields event_id | search event_id="not-present"`,
	)
	malformedErr := executeCompiledExpectingNoRows(ctx, connection, malformed)
	if malformedErr == nil ||
		!strings.Contains(malformedErr.Error(), UnsupportedStatsMeasureValueMarker) {
		t.Fatalf(
			"noncanonical bytes/v1 streamstats max error = %v, want %q",
			malformedErr,
			UnsupportedStatsMeasureValueMarker,
		)
	}
	bytesPlan := buildIntegrationPlan(
		t,
		bytesSource,
		indexTime.Add(20*time.Second),
		visibilityCutoff,
	)
	bytesSummary, compileErr := (Compiler{}).CompileFieldSummary(
		bytesPlan,
		FieldSummarySpec{
			FieldName:             "higher",
			MaximumValues:         10,
			MaximumDistinctValues: 10,
			MaximumValueBytes:     64,
		},
	)
	if compileErr != nil {
		t.Fatalf("compile typed Bytes streamstats max summary: %v", compileErr)
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
		bytesSummary.SQL,
		bytesSummary.Args...,
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
		t.Fatalf("execute typed Bytes streamstats max summary: %v", queryErr)
	}
	if rowKind != 0 || fieldName != "higher" ||
		!reflect.DeepEqual(observedTypes, []uint8{uint8(eventfields.StoredValueTypeBytes)}) ||
		present != 1 || nulls != 0 || missing != 0 || total != 1 ||
		metadataInvalid != 0 || unsupported != 0 || oversized != 0 {
		t.Fatalf(
			"typed Bytes streamstats max summary = kind=%d field=%q types=%v counts=%d/%d/%d/%d value=%d/%q/%d invalid=%d/%d/%d",
			rowKind,
			fieldName,
			observedTypes,
			present,
			nulls,
			missing,
			total,
			valueType,
			encodedValue,
			valueCount,
			metadataInvalid,
			unsupported,
			oversized,
		)
	}

	// A downstream empty result cannot prune unsupported Dynamic members. The
	// final-row prior-frame case also proves validation is independent from the
	// selected window winner.
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
			ordered + ` | streamstats` + test.options + ` max(` + test.field + `) AS high` +
				` | fields event_id | search event_id="not-present"`,
		)
		queryErr := executeCompiledExpectingNoRows(ctx, connection, query)
		if queryErr == nil ||
			!strings.Contains(queryErr.Error(), UnsupportedStatsMeasureValueMarker) {
			t.Fatalf(
				"%s streamstats max error = %v, want atomic unsupported-value marker",
				test.name,
				queryErr,
			)
		}
	}

	exactBoundary := compileBoundary(
		`index=eventstats-boundary source="eventstats-boundary" host="in"` +
			` | streamstats window=10000 max(streamstats_max_missing) AS high` +
			` | head 1 | table high`,
	)
	assertBoundedStreamStatsSQL(t, exactBoundary)
	var exactEmpty chcol.Dynamic
	if queryErr := connection.QueryRow(
		ctx,
		exactBoundary.SQL,
		exactBoundary.Args...,
	).Scan(&exactEmpty); queryErr != nil {
		t.Fatalf("execute exact streamstats max boundary: %v\nSQL: %s", queryErr, exactBoundary.SQL)
	}
	if exactEmpty.Any() != nil {
		t.Fatalf("exact streamstats max boundary = %#v, want null", exactEmpty.Any())
	}

	hiddenOverflow := compileBoundary(
		`index=eventstats-boundary source="eventstats-boundary"` +
			` | streamstats max(streamstats_max_missing) AS high` +
			` | fields event_id | search event_id="not-present"`,
	)
	assertBoundedStreamStatsSQL(t, hiddenOverflow)
	overflowErr := executeCompiledExpectingNoRows(ctx, connection, hiddenOverflow)
	if overflowErr == nil ||
		!strings.Contains(overflowErr.Error(), StreamStatsInputLimitMarker) {
		t.Fatalf(
			"downstream-hidden streamstats max overflow error = %v, want %q",
			overflowErr,
			StreamStatsInputLimitMarker,
		)
	}
}
