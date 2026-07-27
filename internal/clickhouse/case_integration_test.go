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

func testCaseAgainstClickHouse(
	ctx context.Context,
	t *testing.T,
	store *Store,
	connection clickhousedriver.Conn,
	indexTime time.Time,
) {
	t.Helper()

	visibilityCutoff, err := store.VisibilityCutoff(ctx)
	if err != nil {
		t.Fatalf("capture case visibility cutoff: %v", err)
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
		rows, queryErr := connection.Query(
			queryContext,
			compiled.SQL,
			compiled.Args...,
		)
		if queryErr != nil {
			t.Fatalf(
				"execute case query %q: %v\nSQL: %s\nargs: %#v",
				source,
				queryErr,
				compiled.SQL,
				compiled.Args,
			)
		}
		defer func() {
			if closeErr := rows.Close(); closeErr != nil {
				t.Errorf("close case rows: %v", closeErr)
			}
		}()
		labels := make(map[string]string)
		for rows.Next() {
			var eventID, label string
			if scanErr := rows.Scan(&eventID, &label); scanErr != nil {
				t.Fatalf("scan case row: %v", scanErr)
			}
			labels[eventID] = label
		}
		if rowsErr := rows.Err(); rowsErr != nil {
			t.Fatalf("iterate case rows: %v", rowsErr)
		}
		return labels
	}

	wantLabels := map[string]string{
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
	gotLabels := queryLabels(
		base + ` | eval label=case(isnull(probe), "missing", 1=1, "present")`,
	)
	if !maps.Equal(gotLabels, wantLabels) {
		t.Fatalf("case labels = %#v, want %#v", gotLabels, wantLabels)
	}

	firstMatch := queryLabels(
		base +
			` | eval label=case(isnotnull(probe), "first", isnotnull(probe), "second", 1=1, "none")`,
	)
	for eventID, label := range firstMatch {
		want := "first"
		if eventID == "null-missing" || eventID == "null-explicit" {
			want = "none"
		}
		if label != want {
			t.Fatalf("first-match case label for %q = %q, want %q", eventID, label, want)
		}
	}

	nullCondition := queryLabels(
		base + ` | eval label=case(absent=1, "bad", 1=1, "fallback")`,
	)
	for eventID, label := range nullCondition {
		if label != "fallback" {
			t.Fatalf("null-condition case label for %q = %q, want fallback", eventID, label)
		}
	}

	queryEventIDs := func(source string) []string {
		t.Helper()

		compiled := compile(source + ` | table event_id`)
		rows, queryErr := connection.Query(
			queryContext,
			compiled.SQL,
			compiled.Args...,
		)
		if queryErr != nil {
			t.Fatalf(
				"execute case event query %q: %v\nSQL: %s\nargs: %#v",
				source,
				queryErr,
				compiled.SQL,
				compiled.Args,
			)
		}
		defer func() {
			if closeErr := rows.Close(); closeErr != nil {
				t.Errorf("close case event rows: %v", closeErr)
			}
		}()
		var eventIDs []string
		for rows.Next() {
			var eventID string
			if scanErr := rows.Scan(&eventID); scanErr != nil {
				t.Fatalf("scan case event ID: %v", scanErr)
			}
			eventIDs = append(eventIDs, eventID)
		}
		if rowsErr := rows.Err(); rowsErr != nil {
			t.Fatalf("iterate case event rows: %v", rowsErr)
		}
		slices.Sort(eventIDs)
		return eventIDs
	}
	allIDs := make([]string, 0, len(wantLabels))
	for eventID := range wantLabels {
		allIDs = append(allIDs, eventID)
	}
	slices.Sort(allIDs)
	if got := queryEventIDs(
		base + ` | eval value=case(absent=1, "bad") | where isnull(value)`,
	); !slices.Equal(got, allIDs) {
		t.Fatalf("no-match nullable case event IDs = %#v, want %#v", got, allIDs)
	}
	nullIDs := []string{"null-explicit", "null-missing"}
	if got := queryEventIDs(
		base + ` | where case(isnull(probe), true, isnotnull(probe), false)`,
	); !slices.Equal(got, nullIDs) {
		t.Fatalf("Boolean case event IDs = %#v, want %#v", got, nullIDs)
	}

	scalar := func(source string, destination any) {
		t.Helper()

		compiled := compile(
			base + ` event_id="null-missing" | ` + source,
		)
		if queryErr := connection.QueryRow(
			queryContext,
			compiled.SQL,
			compiled.Args...,
		).Scan(destination); queryErr != nil {
			t.Fatalf(
				"execute case scalar %q: %v\nSQL: %s\nargs: %#v",
				source,
				queryErr,
				compiled.SQL,
				compiled.Args,
			)
		}
	}

	var integer int64
	scalar(`eval value=case(1=1, 1) | table value`, &integer)
	if integer != 1 {
		t.Fatalf("case Int64 = %d, want 1", integer)
	}
	var wideUnsigned uint64
	scalar(
		`eval value=case(1=1, 18446744073709551615) | table value`,
		&wideUnsigned,
	)
	if wideUnsigned != uint64(18446744073709551615) {
		t.Fatalf(
			"case UInt64 = %d, want %d",
			wideUnsigned,
			uint64(18446744073709551615),
		)
	}
	var floating float64
	scalar(
		`eval value=case(1=1, tonumber("1.5")) | table value`,
		&floating,
	)
	if floating != 1.5 {
		t.Fatalf("case Float64 = %g, want 1.5", floating)
	}
	var severity uint8
	scalar(`eval value=case(1=1, severity) | table value`, &severity)
	if want := uint8(opensplunkv1.LogSeverity_LOG_SEVERITY_INFO); severity != want {
		t.Fatalf("case UInt8 = %d, want %d", severity, want)
	}
	var noMatch *string
	scalar(`eval value=case(1=0, "never") | table value`, &noMatch)
	if noMatch != nil {
		t.Fatalf("no-match case = %q, want null", *noMatch)
	}
	var first, second string
	sequential := compile(
		base + ` event_id="null-missing"` +
			` | eval first=case(isnull(probe), "M", 1=1, "P"), second=case(first="M", "missing", 1=1, "present")` +
			` | table first, second`,
	)
	if queryErr := connection.QueryRow(
		queryContext,
		sequential.SQL,
		sequential.Args...,
	).Scan(&first, &second); queryErr != nil {
		t.Fatalf(
			"execute sequential case: %v\nSQL: %s\nargs: %#v",
			queryErr,
			sequential.SQL,
			sequential.Args,
		)
	}
	if first != "M" || second != "missing" {
		t.Fatalf("sequential case = %q/%q, want M/missing", first, second)
	}

	materialized := compile(
		base +
			` | spath input=_raw output=selected path=value` +
			` | eval label=case(isnull(selected), "missing", selected="x", "matched")` +
			` | where label="missing"`,
	)
	if !strings.Contains(materialized.SQL, " AS MATERIALIZED (") ||
		!strings.Contains(materialized.SQL, `"__os_filter_bound_`) {
		t.Fatalf("executed case lost calculated-field materialization:\n%s", materialized.SQL)
	}
	rows, queryErr := connection.Query(
		queryContext,
		materialized.SQL,
		materialized.Args...,
	)
	if queryErr != nil {
		t.Fatalf(
			"execute materialized case: %v\nSQL: %s\nargs: %#v",
			queryErr,
			materialized.SQL,
			materialized.Args,
		)
	}
	for rows.Next() {
	}
	if rowsErr := rows.Err(); rowsErr != nil {
		_ = rows.Close()
		t.Fatalf("iterate materialized case: %v", rowsErr)
	}
	if closeErr := rows.Close(); closeErr != nil {
		t.Fatalf("close materialized case rows: %v", closeErr)
	}

	physical := compile(
		base +
			` | eval label=case(isnull(probe), "missing", 1=1, "present")` +
			` | table event_id, label`,
	)
	actions := explainCompiledQuery(
		t,
		queryContext,
		connection,
		"EXPLAIN actions=1 ",
		physical,
	)
	if strings.Contains(actions, "ArrayJoin") {
		t.Fatalf("case expands event rows:\n%s", actions)
	}
}
