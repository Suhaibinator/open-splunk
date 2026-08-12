package clickhouse

import (
	"context"
	"reflect"
	"strings"
	"testing"
	"time"

	clickhousedriver "github.com/ClickHouse/clickhouse-go/v2"
)

// testEventStatsChronologicalAgainstClickHouse reuses the transforming-stats
// chronology corpus to pin row-preserving earliest/latest selection, logical
// presence, hidden validation, and the shared 10,000-row eventstats fence.
func testEventStatsChronologicalAgainstClickHouse(
	ctx context.Context,
	t *testing.T,
	store *Store,
	connection clickhousedriver.Conn,
	indexTime time.Time,
) {
	t.Helper()

	visibilityCutoff, err := store.VisibilityCutoff(ctx)
	if err != nil {
		t.Fatalf("capture eventstats chronological visibility cutoff: %v", err)
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

	collect := func(name string, query CompiledQuery) []dynamicPairRow {
		t.Helper()
		return collectDynamicPairRows(t, ctx, connection, name, query)
	}

	const orderBase = `index=compiler source="stats-chronological-order"`
	global := collect(
		"global eventstats chronology",
		compile(
			orderBase+` | sort 0 +chronological_sequence`+
				` | eventstats earliest(chronological_value) AS first`+
				` | eventstats latest(chronological_value) AS last`+
				` | sort 0 +event_id | table event_id first last`,
		),
	)
	wantGlobal := []dynamicPairRow{
		{id: "chronological-order-middle", first: "z", second: "a"},
		{id: "chronological-order-newest", first: "z", second: "a"},
		{id: "chronological-order-oldest", first: "z", second: "a"},
	}
	if !reflect.DeepEqual(global, wantGlobal) {
		t.Fatalf("global eventstats chronology = %#v, want %#v", global, wantGlobal)
	}

	head := collect(
		"head survivor eventstats chronology",
		compile(
			orderBase+` | sort 0 -chronological_sequence | head 2`+
				` | eventstats earliest(chronological_value) AS first`+
				` | eventstats latest(chronological_value) AS last`+
				` | sort 0 +event_id | table event_id first last`,
		),
	)
	wantHead := []dynamicPairRow{
		{id: "chronological-order-middle", first: "z", second: "middle"},
		{id: "chronological-order-oldest", first: "z", second: "middle"},
	}
	if !reflect.DeepEqual(head, wantHead) {
		t.Fatalf("head eventstats chronology = %#v, want %#v", head, wantHead)
	}

	multivalue := collect(
		"multivalue eventstats chronology",
		compile(
			`index=compiler source="stats-chronological-multivalue"`+
				` | eventstats earliest(chronological_value) AS first`+
				` | eventstats latest(chronological_value) AS last`+
				` | table event_id first last`,
		),
	)
	wantMultivalue := []dynamicPairRow{{
		id: "chronological-multivalue", first: "", second: "last",
	}}
	if !reflect.DeepEqual(multivalue, wantMultivalue) {
		t.Fatalf("multivalue eventstats chronology = %#v, want %#v", multivalue, wantMultivalue)
	}

	groupedTypes := collect(
		"grouped scalar-type eventstats chronology",
		compile(
			`index=compiler source="stats-chronological-types"`+
				` | eventstats earliest(chronological_value) AS first BY chronological_group`+
				` | eventstats latest(chronological_value) AS last BY chronological_group`+
				` | sort 0 +event_id | table event_id first last`,
		),
	)
	gotTypes := make(map[string][2]any, len(groupedTypes))
	for _, row := range groupedTypes {
		gotTypes[row.id] = [2]any{row.first, row.second}
	}
	wantTypes := map[string][2]any{
		"chronological-type-bool":      {"true", "true"},
		"chronological-type-bytes":     {"AP8Q", "AP8Q"},
		"chronological-type-decimal":   {"-1234567890.00100e+12", "-1234567890.00100e+12"},
		"chronological-type-duration":  {"3:4", "3:4"},
		"chronological-type-floating":  {"1.25", "1.25"},
		"chronological-type-signed":    {"-7", "-7"},
		"chronological-type-string":    {"word", "word"},
		"chronological-type-timestamp": {"2026-07-21T03:04:05.123456789Z", "2026-07-21T03:04:05.123456789Z"},
		"chronological-type-unsigned":  {"18446744073709551615", "18446744073709551615"},
	}
	if !reflect.DeepEqual(gotTypes, wantTypes) {
		t.Fatalf("grouped eventstats scalar types = %#v, want %#v", gotTypes, wantTypes)
	}

	allNull := collect(
		"grouped all-null eventstats chronology",
		compile(
			`index=compiler source="stats-chronological-null"`+
				` | eventstats earliest(chronological_value) AS first BY chronological_group`+
				` | eventstats latest(chronological_value) AS last BY chronological_group`+
				` | sort 0 +event_id | table event_id first last`,
		),
	)
	wantAllNull := []dynamicPairRow{
		{id: "chronological-null-explicit", first: nil, second: nil},
		{id: "chronological-null-missing", first: nil, second: nil},
	}
	if !reflect.DeepEqual(allNull, wantAllNull) {
		t.Fatalf("grouped all-null eventstats chronology = %#v, want %#v", allNull, wantAllNull)
	}

	for _, tie := range []struct {
		name   string
		source string
		first  any
		last   any
	}{
		{
			name: "event ID closes an event-time tie", source: "stats-chronological-event-id",
			first: "id-a", last: "id-b",
		},
		{
			name: "visibility sequence closes an event-id tie", source: "stats-chronological-visibility",
			first: "older", last: "newer",
		},
		{
			name: "source identity closes a visibility tie", source: "stats-chronological-source-identity",
			first: "older", last: "newer",
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
				`index=compiler source="` + tie.source + `"` +
					` | eventstats earliest(chronological_value) AS first` +
					` | eventstats latest(chronological_value) AS last` +
					` | head 1 | table event_id first last`,
			)
			got := collectDynamicPairRows(
				t,
				threadContext,
				connection,
				tie.name,
				query,
			)
			if len(got) != 1 || got[0].first != tie.first || got[0].second != tie.last {
				t.Fatalf(
					"%s with %d threads = %#v, want %v/%v",
					tie.name,
					threads,
					got,
					tie.first,
					tie.last,
				)
			}
		}
	}

	incompleteLogical := buildIntegrationPlan(
		t,
		`index=compiler source="stats-chronological-incomplete-by"`+
			` | eventstats earliest(chronological_value) AS first BY chronological_group`,
		indexTime.Add(10*time.Second),
		visibilityCutoff,
	)
	if got, want := collectEventStatsFieldPresence(
		t,
		ctx,
		connection,
		incompleteLogical,
		"first",
	), (eventStatsFieldPresence{present: 1, missing: 1, total: 2}); got != want {
		t.Fatalf("incomplete-BY eventstats chronological presence = %#v, want %#v", got, want)
	}

	for _, source := range []string{
		`index=compiler source="stats-chronological-poison"` +
			` | eventstats earliest(chronological_value) AS discarded BY chronological_group` +
			` | table event_id`,
		`index=compiler source="stats-chronological-multivalue-poison"` +
			` | eventstats latest(chronological_value) AS discarded` +
			` | search definitely_missing=value | table event_id`,
		`index=compiler source="stats-chronological-multivalue-poison"` +
			` | eventstats earliest(chronological_value) AS event_first` +
			` | eventstats latest(chronological_value) AS event_last` +
			` | sort 0 +event_id` +
			` | streamstats earliest(chronological_value) AS stream_first` +
			` | streamstats latest(chronological_value) AS stream_last` +
			` | search definitely_missing=value | table event_id`,
	} {
		queryErr := executeCompiledExpectingNoRows(ctx, connection, compile(source))
		if queryErr == nil || !strings.Contains(
			queryErr.Error(),
			UnsupportedStatsMeasureValueMarker,
		) {
			t.Fatalf(
				"hidden eventstats chronological poison error = %v, want marker %q",
				queryErr,
				UnsupportedStatsMeasureValueMarker,
			)
		}
	}

	overflow := compileBoundary(
		`index=eventstats-boundary source="eventstats-boundary"` +
			` | eventstats earliest(host) AS discarded` +
			` | search definitely_missing=value | table event_id`,
	)
	overflowErr := executeCompiledExpectingNoRows(ctx, connection, overflow)
	if overflowErr == nil || !strings.Contains(
		overflowErr.Error(),
		EventStatsInputLimitMarker,
	) {
		t.Fatalf(
			"hidden eventstats chronological overflow = %v, want marker %q",
			overflowErr,
			EventStatsInputLimitMarker,
		)
	}
}
