package clickhouse

import (
	"context"
	"os"
	"os/exec"
	"testing"
	"time"

	clickhousedriver "github.com/ClickHouse/clickhouse-go/v2"
	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/ingest"
	"github.com/Suhaibinator/open-splunk/internal/testsupport"
)

type dynamicCrossFeatureCase struct {
	id                string
	value             *opensplunk.TypedValue
	kind              string
	equalLiteral      string
	unequalLiteral    string
	arithmeticPresent bool
	arithmeticValue   float64
	ordered           bool
}

// TestDynamicCrossFeatureMatrixAgainstClickHouse keeps runtime type
// classification and every accepted comparison surface on one semantic axis.
// In particular, once bounded Dynamic text is classified as Number and is
// admitted by arithmetic, its spelling must not change numeric predicate
// results. Wide integers deliberately include the first value Float64 cannot
// represent exactly so comparisons remain exact across that boundary.
func TestDynamicCrossFeatureMatrixAgainstClickHouse(t *testing.T) {
	if os.Getenv("OPEN_SPLUNK_CLICKHOUSE_INTEGRATION") != "1" {
		t.Skip("set OPEN_SPLUNK_CLICKHOUSE_INTEGRATION=1 to run the Docker integration test")
	}
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skipf("docker CLI is unavailable: %v", err)
	}
	image, err := testsupport.ResolvePinnedClickHouseImage(
		os.Getenv("OPEN_SPLUNK_CLICKHOUSE_TEST_IMAGE"),
	)
	if err != nil {
		t.Fatalf("resolve pinned ClickHouse image: %v", err)
	}
	t.Logf("ClickHouse image: %s", image)

	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	connection, store := chartEdgeStartClickHouse(t, ctx)
	indexTime := time.Date(2026, time.September, 2, 12, 0, 0, 0, time.UTC)
	const index = "dynamic-cross-feature"

	cases := []dynamicCrossFeatureCase{
		{id: "sint", value: typedSint(-100), kind: "Number", equalLiteral: "-100", unequalLiteral: "-99", arithmeticPresent: true, arithmeticValue: -100, ordered: true},
		{id: "uint-wide", value: typedUint(9_007_199_254_740_993), kind: "Number", equalLiteral: "9007199254740993", unequalLiteral: "9007199254740992", ordered: true},
		{id: "float", value: typedDouble(100.5), kind: "Number", equalLiteral: "100.5", unequalLiteral: "101.5", arithmeticPresent: true, arithmeticValue: 100.5, ordered: true},
		{id: "decimal", value: typedDecimal("100.500"), kind: "Number", equalLiteral: "100.5", unequalLiteral: "101.5", arithmeticPresent: true, arithmeticValue: 100.5, ordered: true},
		{id: "decimal-wide", value: typedDecimal("9007199254740993"), kind: "Number", equalLiteral: "9007199254740993", unequalLiteral: "9007199254740992", arithmeticPresent: true, arithmeticValue: 9_007_199_254_740_992, ordered: true},
		{id: "text-integer", value: typedString("100"), kind: "Number", equalLiteral: "100", unequalLiteral: "101", arithmeticPresent: true, arithmeticValue: 100, ordered: true},
		{id: "text-leading-plus", value: typedString("+100"), kind: "Number", equalLiteral: "100", unequalLiteral: "101", arithmeticPresent: true, arithmeticValue: 100, ordered: true},
		{id: "text-leading-zero", value: typedString("00100"), kind: "Number", equalLiteral: "100", unequalLiteral: "101", arithmeticPresent: true, arithmeticValue: 100, ordered: true},
		{id: "text-decimal", value: typedString("100.0"), kind: "Number", equalLiteral: "100", unequalLiteral: "101", arithmeticPresent: true, arithmeticValue: 100, ordered: true},
		{id: "text-exponent", value: typedString("1e2"), kind: "Number", equalLiteral: "100", unequalLiteral: "101", arithmeticPresent: true, arithmeticValue: 100, ordered: true},
		{id: "text-negative", value: typedString("-100"), kind: "Number", equalLiteral: "-100", unequalLiteral: "-99", arithmeticPresent: true, arithmeticValue: -100, ordered: true},
		{id: "text-wide", value: typedString("9007199254740993"), kind: "Number", equalLiteral: "9007199254740993", unequalLiteral: "9007199254740992", arithmeticPresent: true, arithmeticValue: 9_007_199_254_740_992, ordered: true},
		{id: "text", value: typedString("abc"), kind: "String", equalLiteral: `"abc"`, unequalLiteral: `"xyz"`, ordered: true},
		{id: "text-empty", value: typedString(""), kind: "String", equalLiteral: `""`, unequalLiteral: `"xyz"`, ordered: true},
		{id: "bool", value: typedBool(true), kind: "Boolean", equalLiteral: "true", unequalLiteral: "false"},
		{id: "null", value: typedNull(), kind: "Invalid"},
		{id: "missing", kind: "Invalid"},
	}

	events := make([]*ingest.StoredEvent, 0, len(cases))
	for _, test := range cases {
		event := testStoredEvent(test.id, index, indexTime)
		if test.value != nil {
			event.Event.Fields = typedObjectValue(typedField("value", test.value))
		}
		events = append(events, event)
	}
	_, queryContext := storeScalarFunctionIntegrationFixtures(
		ctx,
		t,
		store,
		indexTime,
		index,
		"dynamic-cross-feature-batch",
		1,
		events...,
	)
	visibilityCutoff, err := store.VisibilityCutoff(ctx)
	if err != nil {
		t.Fatalf("capture Dynamic cross-feature visibility cutoff: %v", err)
	}
	compile := func(t *testing.T, source string) CompiledQuery {
		t.Helper()
		return compileIntegrationSPLForIndex(
			t,
			source,
			indexTime.Add(10*time.Second),
			visibilityCutoff,
			index,
		)
	}

	t.Run("typeof and arithmetic", func(t *testing.T) {
		compiled := compile(t,
			`index=dynamic-cross-feature`+
				` | sort 0 +event_id`+
				` | eval kind=typeof(value), numeric=value+0`+
				` | table event_id kind numeric`,
		)
		rows, queryErr := connection.Query(queryContext, compiled.SQL, compiled.Args...)
		if queryErr != nil {
			t.Fatalf("execute Dynamic classification matrix: %v", queryErr)
		}
		defer func() { _ = rows.Close() }()
		got := make(map[string]struct {
			kind           string
			numericPresent bool
			numericValue   float64
		}, len(cases))
		for rows.Next() {
			var eventID, kind string
			var numeric *float64
			if scanErr := rows.Scan(&eventID, &kind, &numeric); scanErr != nil {
				t.Fatalf("scan Dynamic classification matrix: %v", scanErr)
			}
			got[eventID] = struct {
				kind           string
				numericPresent bool
				numericValue   float64
			}{kind: kind, numericPresent: numeric != nil}
			if numeric != nil {
				actual := got[eventID]
				actual.numericValue = *numeric
				got[eventID] = actual
			}
		}
		if rowsErr := rows.Err(); rowsErr != nil {
			t.Fatalf("iterate Dynamic classification matrix: %v", rowsErr)
		}
		for _, test := range cases {
			t.Run(test.id, func(t *testing.T) {
				actual, ok := got[test.id]
				if !ok {
					t.Fatalf("%s is missing from classification output", test.id)
				}
				if actual.kind != test.kind {
					t.Errorf("typeof(%s) = %q, want %q", test.id, actual.kind, test.kind)
				}
				if actual.numericPresent != test.arithmeticPresent {
					t.Errorf("%s arithmetic present = %t, want %t", test.id, actual.numericPresent, test.arithmeticPresent)
				} else if actual.numericPresent && actual.numericValue != test.arithmeticValue {
					t.Errorf("%s arithmetic value = %v, want %v", test.id, actual.numericValue, test.arithmeticValue)
				}
			})
		}
	})

	for _, test := range cases {
		if test.equalLiteral == "" {
			continue
		}
		t.Run(test.id, func(t *testing.T) {
			base := `index=dynamic-cross-feature event_id="` + test.id + `"`
			checks := []struct {
				name   string
				source string
				want   uint64
			}{
				{name: "base search equality", source: base + ` value=` + test.equalLiteral + ` | stats count`, want: 1},
				{name: "pipeline search equality", source: base + ` | search value=` + test.equalLiteral + ` | stats count`, want: 1},
				{name: "where equality", source: base + ` | where value=` + test.equalLiteral + ` | stats count`, want: 1},
				{name: "eval equality", source: base + ` | eval matched=if(value=` + test.equalLiteral + `, 1, 0) | where matched=1 | stats count`, want: 1},
				{name: "where inequality", source: base + ` | where value!=` + test.unequalLiteral + ` | stats count`, want: 1},
				{name: "membership", source: base + ` | where value IN (` + test.equalLiteral + `) | stats count`, want: 1},
			}
			if test.ordered {
				checks = append(checks, struct {
					name   string
					source string
					want   uint64
				}{
					name: "ordering identity",
					source: base + ` | where value>=` + test.equalLiteral +
						` AND value<=` + test.equalLiteral + ` | stats count`,
					want: 1,
				})
			}
			for _, check := range checks {
				t.Run(check.name, func(t *testing.T) {
					if got := dynamicCrossFeatureCount(t, queryContext, connection, compile(t, check.source)); got != check.want {
						t.Errorf("%s count = %d, want %d", check.source, got, check.want)
					}
				})
			}
		})
	}

	// A field can cross several relational boundaries without changing its SPL
	// value. Keep each boundary paired with type/arithmetic controls so a
	// comparison failure cannot be mistaken for lost data or a changed scalar.
	t.Run("numeric text representation transitions", func(t *testing.T) {
		transitions := []struct {
			name   string
			prefix string
			field  string
		}{
			{name: "direct", field: "value"},
			{name: "eval copy", prefix: ` | eval copied=value`, field: "copied"},
			{name: "rename", prefix: ` | rename value AS copied`, field: "copied"},
			{name: "fields projection", prefix: ` | fields event_id value`, field: "value"},
			{name: "eventstats earliest", prefix: ` | eventstats earliest(value) AS copied`, field: "copied"},
			{name: "stats first", prefix: ` | stats first(value) AS copied`, field: "copied"},
		}
		for _, transition := range transitions {
			t.Run(transition.name, func(t *testing.T) {
				base := `index=dynamic-cross-feature event_id="text-integer"` + transition.prefix
				checks := []struct {
					name      string
					predicate string
				}{
					{name: "type control", predicate: `typeof(` + transition.field + `)="Number"`},
					{name: "arithmetic control", predicate: transition.field + `+0=100`},
					{name: "equality", predicate: transition.field + `=100`},
					{name: "inequality", predicate: transition.field + `!=101`},
					{name: "membership", predicate: transition.field + ` IN (100)`},
					{name: "ordering", predicate: transition.field + `>=100 AND ` + transition.field + `<=100`},
				}
				for _, check := range checks {
					t.Run(check.name, func(t *testing.T) {
						source := base + ` | where ` + check.predicate + ` | stats count`
						if got := dynamicCrossFeatureCount(t, queryContext, connection, compile(t, source)); got != 1 {
							t.Errorf("%s count = %d, want 1", source, got)
						}
					})
				}
			})
		}
	})

	t.Run("null and missing", func(t *testing.T) {
		checks := []struct {
			name   string
			source string
			want   uint64
		}{
			{name: "explicit null base search", source: `index=dynamic-cross-feature event_id="null" value=null | stats count`, want: 1},
			{name: "missing base search", source: `index=dynamic-cross-feature event_id="missing" value=null | stats count`, want: 0},
			{name: "explicit null isnull", source: `index=dynamic-cross-feature event_id="null" | where isnull(value) | stats count`, want: 1},
			{name: "missing isnull", source: `index=dynamic-cross-feature event_id="missing" | where isnull(value) | stats count`, want: 1},
			{name: "explicit null equality is unknown", source: `index=dynamic-cross-feature event_id="null" | where value=null | stats count`, want: 0},
			{name: "missing equality is unknown", source: `index=dynamic-cross-feature event_id="missing" | where value=null | stats count`, want: 0},
			{name: "explicit null membership is unknown", source: `index=dynamic-cross-feature event_id="null" | where value IN (null) | stats count`, want: 0},
			{name: "missing membership is unknown", source: `index=dynamic-cross-feature event_id="missing" | where value IN (null) | stats count`, want: 0},
		}
		for _, check := range checks {
			t.Run(check.name, func(t *testing.T) {
				if got := dynamicCrossFeatureCount(t, queryContext, connection, compile(t, check.source)); got != check.want {
					t.Errorf("%s count = %d, want %d", check.source, got, check.want)
				}
			})
		}
	})
}

func dynamicCrossFeatureCount(
	t *testing.T,
	ctx context.Context,
	connection clickhousedriver.Conn,
	compiled CompiledQuery,
) uint64 {
	t.Helper()
	var count uint64
	if err := connection.QueryRow(ctx, compiled.SQL, compiled.Args...).Scan(&count); err != nil {
		t.Fatalf("execute Dynamic cross-feature query: %v\nSQL: %s\nargs: %#v", err, compiled.SQL, compiled.Args)
	}
	return count
}
