package clickhouse

import (
	"context"
	"maps"
	"slices"
	"strings"
	"testing"
	"time"

	clickhousedriver "github.com/ClickHouse/clickhouse-go/v2"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
)

func testIfAgainstClickHouse(
	ctx context.Context,
	t *testing.T,
	store *Store,
	connection clickhousedriver.Conn,
	indexTime time.Time,
) {
	t.Helper()

	visibilityCutoff, err := store.VisibilityCutoff(ctx)
	if err != nil {
		t.Fatalf("capture if visibility cutoff: %v", err)
	}
	compile := func(source string) CompiledQuery {
		t.Helper()
		return compileIntegrationSPL(t, source, indexTime.Add(10*time.Second), visibilityCutoff)
	}
	// Prove the compiler does not depend on ClickHouse's Variant common-type
	// inference even though the pinned server currently enables it by default.
	queryContext := clickhousedriver.Context(
		ctx,
		clickhousedriver.WithSettings(clickhousedriver.Settings{
			"use_variant_as_common_type":        uint8(0),
			"short_circuit_function_evaluation": "enable",
		}),
	)
	base := `index=compiler source="null-predicate"`
	queryLabels := func(source string) map[string]string {
		t.Helper()
		compiled := compile(source + ` | table event_id, label`)
		rows, queryErr := connection.Query(queryContext, compiled.SQL, compiled.Args...)
		if queryErr != nil {
			t.Fatalf("execute if query %q: %v\nSQL: %s\nargs: %#v", source, queryErr, compiled.SQL, compiled.Args)
		}
		defer func() {
			if closeErr := rows.Close(); closeErr != nil {
				t.Errorf("close if rows: %v", closeErr)
			}
		}()
		labels := make(map[string]string)
		for rows.Next() {
			var eventID, label string
			if scanErr := rows.Scan(&eventID, &label); scanErr != nil {
				t.Fatalf("scan if row: %v", scanErr)
			}
			labels[eventID] = label
		}
		if rowsErr := rows.Err(); rowsErr != nil {
			t.Fatalf("iterate if rows: %v", rowsErr)
		}
		return labels
	}
	wantNullLabels := map[string]string{
		"null-missing":    "missing",
		"null-explicit":   "missing",
		"null-empty-text": "present",
		"null-zero":       "present",
		"null-false":      "present",
		"null-empty-list": "present",
		"null-list-null":  "present",
		"null-list":       "present",
		"null-object":     "present",
	}
	if got := queryLabels(base + ` | eval label=if(isnull(probe), "missing", "present")`); !maps.Equal(got, wantNullLabels) {
		t.Fatalf("if(isnull) labels = %#v, want %#v", got, wantNullLabels)
	}
	if got := queryLabels(base + ` | eval label=if(isnotnull(probe), "present", "missing")`); !maps.Equal(got, wantNullLabels) {
		t.Fatalf("if(isnotnull) labels = %#v, want %#v", got, wantNullLabels)
	}

	queryEventIDs := func(source string) []string {
		t.Helper()
		compiled := compile(source + ` | table event_id`)
		rows, queryErr := connection.Query(queryContext, compiled.SQL, compiled.Args...)
		if queryErr != nil {
			t.Fatalf("execute if event query %q: %v\nSQL: %s\nargs: %#v", source, queryErr, compiled.SQL, compiled.Args)
		}
		defer func() {
			if closeErr := rows.Close(); closeErr != nil {
				t.Errorf("close if event rows: %v", closeErr)
			}
		}()
		var eventIDs []string
		for rows.Next() {
			var eventID string
			if scanErr := rows.Scan(&eventID); scanErr != nil {
				t.Fatalf("scan if event ID: %v", scanErr)
			}
			eventIDs = append(eventIDs, eventID)
		}
		if rowsErr := rows.Err(); rowsErr != nil {
			t.Fatalf("iterate if event rows: %v", rowsErr)
		}
		slices.Sort(eventIDs)
		return eventIDs
	}
	nullIDs := []string{"null-explicit", "null-missing"}
	slices.Sort(nullIDs)
	if got := queryEventIDs(base + ` | where if(isnull(probe), true, false)`); !slices.Equal(got, nullIDs) {
		t.Fatalf("Boolean if event IDs = %#v, want %#v", got, nullIDs)
	}
	if got := queryEventIDs(base + ` | eval value=if(isnull(probe), null, "present") | where isnull(value)`); !slices.Equal(got, nullIDs) {
		t.Fatalf("nullable if event IDs = %#v, want %#v", got, nullIDs)
	}
	if got := queryEventIDs(
		base + ` | eval value=if(isnull(probe), if(isnull(absent), null, null), 1)` +
			` | where isnull(value)`,
	); !slices.Equal(got, nullIDs) {
		t.Fatalf("nested all-null numeric if event IDs = %#v, want %#v", got, nullIDs)
	}
	if got := queryEventIDs(
		base + ` | eval value=if(isnull(probe), if(isnull(absent), null, null), true)` +
			` | where isnull(value)`,
	); !slices.Equal(got, nullIDs) {
		t.Fatalf("nested all-null Boolean if event IDs = %#v, want %#v", got, nullIDs)
	}
	if got := queryEventIDs(
		base + ` | eval selected=if(isnull(absent), null, null)` +
			` | fields event_id probe selected` +
			` | rename selected AS renamed` +
			` | eval value=if(isnull(probe), renamed, 1)` +
			` | where isnull(value)`,
	); !slices.Equal(got, nullIDs) {
		t.Fatalf("projected all-null numeric if event IDs = %#v, want %#v", got, nullIDs)
	}
	if got := queryEventIDs(
		base + ` | fields event_id probe` +
			` | table event_id probe removed` +
			` | eval value=if(isnull(probe), removed, 1)` +
			` | where isnull(value)`,
	); !slices.Equal(got, nullIDs) {
		t.Fatalf("synthetic missing numeric if event IDs = %#v, want %#v", got, nullIDs)
	}

	projected := queryLabels(base + ` | fields event_id | eval label=if(isnull(probe), "missing", "present")`)
	for eventID, label := range projected {
		if label != "missing" {
			t.Fatalf("projected-away if label for %q = %q, want missing", eventID, label)
		}
	}
	if len(projected) != len(wantNullLabels) {
		t.Fatalf("projected-away if row count = %d, want %d", len(projected), len(wantNullLabels))
	}

	falseOnNullCondition := queryLabels(base + ` | eval label=if(absent=1, "true", "false")`)
	if len(falseOnNullCondition) != len(wantNullLabels) {
		t.Fatalf(
			"NULL condition returned %d rows, want %d",
			len(falseOnNullCondition),
			len(wantNullLabels),
		)
	}
	for eventID, label := range falseOnNullCondition {
		if label != "false" {
			t.Fatalf("NULL condition label for %q = %q, want false", eventID, label)
		}
	}

	sequential := compile(
		base + ` event_id="null-missing"` +
			` | eval first=if(isnull(probe), "M", "P"), label=if(first="M", "missing", "present")` +
			` | table first, label`,
	)
	var first, label string
	if queryErr := connection.QueryRow(queryContext, sequential.SQL, sequential.Args...).Scan(&first, &label); queryErr != nil {
		t.Fatalf("execute sequential if: %v\nSQL: %s\nargs: %#v", queryErr, sequential.SQL, sequential.Args)
	}
	if first != "M" || label != "missing" {
		t.Fatalf("sequential if = %q/%q, want M/missing", first, label)
	}

	numeric := compile(base + ` event_id="null-missing" | eval value=if(isnull(probe), 1, 2) | table value`)
	var numericValue int64
	if queryErr := connection.QueryRow(queryContext, numeric.SQL, numeric.Args...).Scan(&numericValue); queryErr != nil {
		t.Fatalf("execute numeric if: %v\nSQL: %s\nargs: %#v", queryErr, numeric.SQL, numeric.Args)
	}
	if numericValue != 1 {
		t.Fatalf("numeric if = %d, want 1", numericValue)
	}

	floating := compile(
		base + ` event_id="null-missing"` +
			` | eval value=if(isnull(probe), tonumber("1.5"), tonumber("2.5"))` +
			` | table value`,
	)
	var floatingValue float64
	if queryErr := connection.QueryRow(queryContext, floating.SQL, floating.Args...).Scan(&floatingValue); queryErr != nil {
		t.Fatalf("execute Float64 if: %v\nSQL: %s\nargs: %#v", queryErr, floating.SQL, floating.Args)
	}
	if floatingValue != 1.5 {
		t.Fatalf("Float64 if = %g, want 1.5", floatingValue)
	}

	unsigned := compile(
		base + ` event_id="null-missing"` +
			` | eval value=if(isnull(probe), severity, severity)` +
			` | table value`,
	)
	var unsignedValue uint8
	if queryErr := connection.QueryRow(queryContext, unsigned.SQL, unsigned.Args...).Scan(&unsignedValue); queryErr != nil {
		t.Fatalf("execute UInt8 if: %v\nSQL: %s\nargs: %#v", queryErr, unsigned.SQL, unsigned.Args)
	}
	if want := uint8(opensplunkv1.LogSeverity_LOG_SEVERITY_INFO); unsignedValue != want {
		t.Fatalf("UInt8 if = %d, want %d", unsignedValue, want)
	}

	wideUnsigned := compile(
		base + ` event_id="null-missing"` +
			` | eval value=if(isnull(probe), 18446744073709551615, 18446744073709551614)` +
			` | table value`,
	)
	var wideUnsignedValue uint64
	if queryErr := connection.QueryRow(
		queryContext,
		wideUnsigned.SQL,
		wideUnsigned.Args...,
	).Scan(&wideUnsignedValue); queryErr != nil {
		t.Fatalf("execute UInt64 if: %v\nSQL: %s\nargs: %#v", queryErr, wideUnsigned.SQL, wideUnsigned.Args)
	}
	if wideUnsignedValue != uint64(18446744073709551615) {
		t.Fatalf("UInt64 if = %d, want %d", wideUnsignedValue, uint64(18446744073709551615))
	}

	wantNonNullIDs := []string{
		"null-empty-list",
		"null-empty-text",
		"null-false",
		"null-list",
		"null-list-null",
		"null-object",
		"null-zero",
	}
	if got := queryEventIDs(
		base + ` | eval value=if(isnull(probe), null, true) | where value=true`,
	); !slices.Equal(got, wantNonNullIDs) {
		t.Fatalf("nullable Boolean if event IDs = %#v, want %#v", got, wantNonNullIDs)
	}

	wantAllIDs := append(append([]string(nil), nullIDs...), wantNonNullIDs...)
	slices.Sort(wantAllIDs)
	if got := queryEventIDs(
		base + ` | spath input=_raw output=selected path=value` +
			` | where if(isnull(selected), true, false)`,
	); !slices.Equal(got, wantAllIDs) {
		t.Fatalf("materialized if predicate event IDs = %#v, want %#v", got, wantAllIDs)
	}

	physical := compile(base + ` | eval label=if(isnull(probe), "missing", "present") | table event_id, label`)
	actions := explainCompiledQuery(t, queryContext, connection, "EXPLAIN actions=1 ", physical)
	if strings.Contains(actions, "ArrayJoin") {
		t.Fatalf("if expands event rows:\n%s", actions)
	}
}
