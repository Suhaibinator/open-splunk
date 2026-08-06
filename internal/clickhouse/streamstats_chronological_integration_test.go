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
)

// testStreamStatsChronologicalAgainstClickHouse reuses the preceding
// stats/eventstats chronological fixtures so all three commands are pinned to
// one immutable event-order contract. Pipeline order controls only ROWS-frame
// membership; original event chronology and member ordinal choose the winner.
func testStreamStatsChronologicalAgainstClickHouse(
	ctx context.Context,
	t *testing.T,
	store *Store,
	connection clickhousedriver.Conn,
	indexTime time.Time,
) {
	t.Helper()

	visibilityCutoff, err := store.VisibilityCutoff(ctx)
	if err != nil {
		t.Fatalf("capture streamstats chronological visibility cutoff: %v", err)
	}
	compile := func(source string) CompiledQuery {
		t.Helper()
		return compileIntegrationSPL(
			t,
			source,
			indexTime.Add(30*time.Second),
			visibilityCutoff,
		)
	}
	compileBoundary := func(source string) CompiledQuery {
		t.Helper()
		return compileIntegrationSPLForIndex(
			t,
			source,
			indexTime.Add(30*time.Second),
			visibilityCutoff,
			"eventstats-boundary",
		)
	}

	collect := func(name string, query CompiledQuery) []dynamicPairRow {
		t.Helper()
		return collectDynamicPairRows(t, ctx, connection, name, query)
	}

	const orderBase = `index=compiler source="stats-chronological-order"`
	ordered := orderBase + ` | sort 0 +chronological_sequence`
	pairPipeline := func(options string) string {
		if options != "" {
			options += " "
		}
		return ` | streamstats ` + options +
			`earliest(chronological_value) AS first_seen` +
			` | streamstats ` + options +
			`latest(chronological_value) AS last_seen` +
			` | table event_id first_seen last_seen`
	}
	for _, test := range []struct {
		name    string
		options string
		want    []dynamicPairRow
	}{
		{
			name: "complete current prefix",
			want: []dynamicPairRow{
				{id: "chronological-order-newest", first: "a", second: "a"},
				{id: "chronological-order-middle", first: "middle", second: "a"},
				{id: "chronological-order-oldest", first: "z", second: "a"},
			},
		},
		{
			name:    "complete prior prefix",
			options: "current=false",
			want: []dynamicPairRow{
				{id: "chronological-order-newest"},
				{id: "chronological-order-middle", first: "a", second: "a"},
				{id: "chronological-order-oldest", first: "middle", second: "a"},
			},
		},
		{
			name:    "two-row current frame",
			options: "window=2",
			want: []dynamicPairRow{
				{id: "chronological-order-newest", first: "a", second: "a"},
				{id: "chronological-order-middle", first: "middle", second: "a"},
				{id: "chronological-order-oldest", first: "z", second: "middle"},
			},
		},
	} {
		query := compile(ordered + pairPipeline(test.options))
		if test.name == "complete current prefix" {
			assertBoundedStreamStatsSQL(t, query)
		}
		if got := collect(test.name, query); !reflect.DeepEqual(got, test.want) {
			t.Fatalf("%s = %#v, want %#v", test.name, got, test.want)
		}
	}

	for _, test := range []struct {
		name       string
		source     string
		wantFirst  any
		wantSecond any
	}{
		{
			name:       "event ID tie",
			source:     "stats-chronological-event-id",
			wantFirst:  "id-a",
			wantSecond: "id-b",
		},
		{
			name:       "visibility sequence tie",
			source:     "stats-chronological-visibility",
			wantFirst:  "older",
			wantSecond: "newer",
		},
		{
			name:       "source identity tie",
			source:     "stats-chronological-source-identity",
			wantFirst:  "older",
			wantSecond: "newer",
		},
	} {
		query := compile(
			`index=compiler source="` + test.source + `"` +
				` | sort 0 -event_id` +
				` | streamstats earliest(chronological_value) AS first_seen` +
				` | streamstats latest(chronological_value) AS last_seen` +
				` | tail 1 | table first_seen last_seen`,
		)
		for _, threads := range []uint64{1, 4} {
			threadContext := clickhousedriver.Context(
				ctx,
				clickhousedriver.WithSettings(clickhousedriver.Settings{
					"max_threads": threads,
				}),
			)
			var first, last chcol.Dynamic
			if queryErr := connection.QueryRow(
				threadContext,
				query.SQL,
				query.Args...,
			).Scan(&first, &last); queryErr != nil {
				t.Fatalf("%s with %d threads: %v\nSQL: %s", test.name, threads, queryErr, query.SQL)
			}
			if first.Any() != test.wantFirst || last.Any() != test.wantSecond {
				t.Fatalf(
					"%s with %d threads = %#v/%#v, want %#v/%#v",
					test.name,
					threads,
					first.Any(),
					last.Any(),
					test.wantFirst,
					test.wantSecond,
				)
			}
		}
	}

	multivalue := compile(
		`index=compiler source="stats-chronological-multivalue"` +
			` | streamstats earliest(chronological_value) AS first_seen` +
			` | streamstats latest(chronological_value) AS last_seen` +
			` | table first_seen last_seen`,
	)
	var firstMember, lastMember chcol.Dynamic
	if queryErr := connection.QueryRow(
		ctx,
		multivalue.SQL,
		multivalue.Args...,
	).Scan(&firstMember, &lastMember); queryErr != nil {
		t.Fatalf("execute streamstats multivalue chronology: %v\nSQL: %s", queryErr, multivalue.SQL)
	}
	if firstMember.Any() != "" || lastMember.Any() != "last" {
		t.Fatalf(
			"streamstats multivalue chronology = %#v/%#v, want empty/last",
			firstMember.Any(),
			lastMember.Any(),
		)
	}

	typeQuery := compile(
		`index=compiler source="stats-chronological-types"` +
			` | streamstats earliest(chronological_value) AS first_seen BY chronological_group` +
			` | streamstats latest(chronological_value) AS last_seen BY chronological_group` +
			` | table chronological_group first_seen last_seen`,
	)
	typeRows, err := connection.Query(ctx, typeQuery.SQL, typeQuery.Args...)
	if err != nil {
		t.Fatalf("execute streamstats chronological scalar types: %v\nSQL: %s", err, typeQuery.SQL)
	}
	gotTypes := make(map[string][2]any)
	for typeRows.Next() {
		var group string
		var first, last chcol.Dynamic
		if scanErr := typeRows.Scan(&group, &first, &last); scanErr != nil {
			_ = typeRows.Close()
			t.Fatalf("scan streamstats chronological scalar types: %v", scanErr)
		}
		gotTypes[group] = [2]any{first.Any(), last.Any()}
	}
	if rowsErr := typeRows.Err(); rowsErr != nil {
		_ = typeRows.Close()
		t.Fatalf("iterate streamstats chronological scalar types: %v", rowsErr)
	}
	if closeErr := typeRows.Close(); closeErr != nil {
		t.Fatalf("close streamstats chronological scalar types: %v", closeErr)
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
		t.Fatalf("streamstats chronological scalar types = %#v, want %#v", gotTypes, wantTypes)
	}

	invalidFixed := []byte{0xff, 0, 'b', 'y', 't', 'e', 's'}
	invalidQuery := compile(
		`index=compiler source="eventstats-values-invalid-fixed"` +
			` | streamstats earliest(host) AS first_seen` +
			` | streamstats latest(host) AS last_seen` +
			` | table first_seen last_seen`,
	)
	physical := func(field string) []string {
		column := quoteIdentifier(field)
		typeSQL := "dynamicType(" + column + ")"
		mapping := "dynamicElement(" + column + ", 'Map(String, String)')"
		return []string{
			typeSQL,
			"if(" + typeSQL + " = 'Map(String, String)', " + mapping +
				"[concat(char(0), 'open_splunk_type')], CAST('' AS String))",
			"if(" + typeSQL + " = 'Map(String, String)', " + mapping +
				"[concat(char(0), 'open_splunk_value')], CAST('' AS String))",
		}
	}
	controlProjection := append(physical("first_seen"), physical("last_seen")...)
	control := "SELECT " + strings.Join(controlProjection, ", ") +
		" FROM (" + invalidQuery.SQL + ")"
	var firstPhysical, firstTag, firstPayload string
	var lastPhysical, lastTag, lastPayload string
	if queryErr := connection.QueryRow(
		ctx,
		control,
		invalidQuery.Args...,
	).Scan(
		&firstPhysical,
		&firstTag,
		&firstPayload,
		&lastPhysical,
		&lastTag,
		&lastPayload,
	); queryErr != nil {
		t.Fatalf("execute invalid UTF-8 streamstats chronology: %v\nSQL: %s", queryErr, invalidQuery.SQL)
	}
	wantPayload := base64.RawStdEncoding.EncodeToString(invalidFixed)
	if firstPhysical != "Map(String, String)" || firstTag != "bytes/v1" ||
		firstPayload != wantPayload || lastPhysical != "Map(String, String)" ||
		lastTag != "bytes/v1" || lastPayload != wantPayload {
		t.Fatalf(
			"invalid UTF-8 streamstats chronology = %q/%q/%q and %q/%q/%q, want Map/bytes/v1/%q",
			firstPhysical,
			firstTag,
			firstPayload,
			lastPhysical,
			lastTag,
			lastPayload,
			wantPayload,
		)
	}

	nullRows := collect(
		"null and missing values",
		compile(
			`index=compiler source="stats-chronological-null" | sort 0 +event_id`+
				` | streamstats earliest(chronological_value) AS first_seen BY chronological_group`+
				` | streamstats latest(chronological_value) AS last_seen BY chronological_group`+
				` | table event_id first_seen last_seen`,
		),
	)
	if want := []dynamicPairRow{
		{id: "chronological-null-explicit"},
		{id: "chronological-null-missing"},
	}; !reflect.DeepEqual(nullRows, want) {
		t.Fatalf("null/missing streamstats chronology = %#v, want %#v", nullRows, want)
	}

	incomplete := collect(
		"incomplete BY tuple",
		compile(
			`index=compiler source="stats-chronological-incomplete-by" | sort 0 +event_id`+
				` | streamstats earliest(chronological_value) AS first_seen BY chronological_group`+
				` | streamstats latest(chronological_value) AS last_seen BY chronological_group`+
				` | table event_id first_seen last_seen`,
		),
	)
	if want := []dynamicPairRow{
		{id: "chronological-by-complete", first: "visible", second: "visible"},
		{id: "chronological-by-incomplete-poison"},
	}; !reflect.DeepEqual(incomplete, want) {
		t.Fatalf("incomplete grouped streamstats chronology = %#v, want %#v", incomplete, want)
	}

	projected := collect(
		"projected-away input",
		compile(
			ordered+` | table _time,event_id`+
				` | streamstats earliest(chronological_value) AS first_seen`+
				` | streamstats latest(chronological_value) AS last_seen`+
				` | table event_id first_seen last_seen`,
		),
	)
	if want := []dynamicPairRow{
		{id: "chronological-order-newest"},
		{id: "chronological-order-middle"},
		{id: "chronological-order-oldest"},
	}; !reflect.DeepEqual(projected, want) {
		t.Fatalf("projected-away streamstats chronology = %#v, want %#v", projected, want)
	}

	stacked := compile(
		ordered +
			` | streamstats earliest(chronological_value) AS first_seen` +
			` | streamstats latest(first_seen) AS last_of_first` +
			` | tail 1 | table first_seen last_of_first`,
	)
	var stackedFirst, stackedLast chcol.Dynamic
	if queryErr := connection.QueryRow(
		ctx,
		stacked.SQL,
		stacked.Args...,
	).Scan(&stackedFirst, &stackedLast); queryErr != nil {
		t.Fatalf("execute stacked streamstats chronology: %v\nSQL: %s", queryErr, stacked.SQL)
	}
	if stackedFirst.Any() != "z" || stackedLast.Any() != "a" {
		t.Fatalf(
			"stacked streamstats chronology = %#v/%#v, want z/a",
			stackedFirst.Any(),
			stackedLast.Any(),
		)
	}

	for _, test := range []struct {
		name    string
		source  string
		options string
	}{
		{name: "object nonwinner", source: "stats-chronological-poison"},
		{
			name:    "prior-only frame still validates current poison",
			source:  "stats-chronological-multivalue-poison",
			options: "current=false ",
		},
	} {
		query := compile(
			`index=compiler source="` + test.source + `"` +
				` | streamstats ` + test.options +
				`earliest(chronological_value) AS discarded` +
				` | fields event_id | search event_id="not-present"`,
		)
		queryErr := executeCompiledExpectingNoRows(ctx, connection, query)
		if queryErr == nil ||
			!strings.Contains(queryErr.Error(), UnsupportedStatsMeasureValueMarker) {
			t.Fatalf(
				"%s streamstats chronology error = %v, want %q",
				test.name,
				queryErr,
				UnsupportedStatsMeasureValueMarker,
			)
		}
	}

	hiddenOverflow := compileBoundary(
		`index=eventstats-boundary source="eventstats-boundary"` +
			` | streamstats latest(streamstats_chronological_missing) AS last_seen` +
			` | fields event_id | search event_id="not-present"`,
	)
	assertBoundedStreamStatsSQL(t, hiddenOverflow)
	overflowErr := executeCompiledExpectingNoRows(ctx, connection, hiddenOverflow)
	if overflowErr == nil ||
		!strings.Contains(overflowErr.Error(), StreamStatsInputLimitMarker) {
		t.Fatalf(
			"downstream-hidden streamstats chronology overflow error = %v, want %q",
			overflowErr,
			StreamStatsInputLimitMarker,
		)
	}
}
