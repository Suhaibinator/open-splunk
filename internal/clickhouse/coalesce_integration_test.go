package clickhouse

import (
	"context"
	"maps"
	"slices"
	"strings"
	"testing"
	"time"

	clickhousedriver "github.com/ClickHouse/clickhouse-go/v2"
	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
)

func testCoalesceAgainstClickHouse(
	ctx context.Context,
	t *testing.T,
	store *Store,
	connection clickhousedriver.Conn,
	indexTime time.Time,
) {
	t.Helper()

	visibilityCutoff, err := store.VisibilityCutoff(ctx)
	if err != nil {
		t.Fatalf("capture coalesce visibility cutoff: %v", err)
	}
	compile := func(source string) CompiledQuery {
		t.Helper()
		return compileIntegrationSPL(t, source, indexTime.Add(10*time.Second), visibilityCutoff)
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
		compiled := compile(source + ` | table event_id, selected`)
		rows, queryErr := connection.Query(queryContext, compiled.SQL, compiled.Args...)
		if queryErr != nil {
			t.Fatalf(
				"execute coalesce query %q: %v\nSQL: %s\nargs: %#v",
				source,
				queryErr,
				compiled.SQL,
				compiled.Args,
			)
		}
		defer func() {
			if closeErr := rows.Close(); closeErr != nil {
				t.Errorf("close coalesce rows: %v", closeErr)
			}
		}()
		labels := make(map[string]string)
		for rows.Next() {
			var eventID, selected string
			if scanErr := rows.Scan(&eventID, &selected); scanErr != nil {
				t.Fatalf("scan coalesce row: %v", scanErr)
			}
			labels[eventID] = selected
		}
		if rowsErr := rows.Err(); rowsErr != nil {
			t.Fatalf("iterate coalesce rows: %v", rowsErr)
		}
		return labels
	}

	wantLabels := map[string]string{
		"null-missing":    "fallback",
		"null-explicit":   "fallback",
		"null-empty-text": "present",
		"null-zero":       "present",
		"null-false":      "present",
		"null-empty-list": "present",
		"null-list-null":  "present",
		"null-list":       "present",
		"null-object":     "present",
	}
	gotLabels := queryLabels(
		base +
			` | eval candidate=if(isnull(probe), null, "present")` +
			` | eval selected=coalesce(candidate, "fallback")`,
	)
	if !maps.Equal(gotLabels, wantLabels) {
		t.Fatalf("coalesce labels = %#v, want %#v", gotLabels, wantLabels)
	}

	projected := queryLabels(
		base +
			` | fields event_id` +
			` | eval selected=coalesce(removed, "fallback")`,
	)
	if len(projected) != len(wantLabels) {
		t.Fatalf("projected coalesce row count = %d, want %d", len(projected), len(wantLabels))
	}
	for eventID, selected := range projected {
		if selected != "fallback" {
			t.Fatalf("projected coalesce value for %q = %q, want fallback", eventID, selected)
		}
	}

	queryEventIDs := func(source string) []string {
		t.Helper()
		compiled := compile(source + ` | table event_id`)
		rows, queryErr := connection.Query(queryContext, compiled.SQL, compiled.Args...)
		if queryErr != nil {
			t.Fatalf(
				"execute coalesce event query %q: %v\nSQL: %s\nargs: %#v",
				source,
				queryErr,
				compiled.SQL,
				compiled.Args,
			)
		}
		defer func() {
			if closeErr := rows.Close(); closeErr != nil {
				t.Errorf("close coalesce event rows: %v", closeErr)
			}
		}()
		var eventIDs []string
		for rows.Next() {
			var eventID string
			if scanErr := rows.Scan(&eventID); scanErr != nil {
				t.Fatalf("scan coalesce event ID: %v", scanErr)
			}
			eventIDs = append(eventIDs, eventID)
		}
		if rowsErr := rows.Err(); rowsErr != nil {
			t.Fatalf("iterate coalesce event rows: %v", rowsErr)
		}
		slices.Sort(eventIDs)
		return eventIDs
	}
	wantPresentIDs := []string{
		"null-empty-list",
		"null-empty-text",
		"null-false",
		"null-list",
		"null-list-null",
		"null-object",
		"null-zero",
	}
	if got := queryEventIDs(
		base +
			` | where coalesce(null, isnotnull(probe))`,
	); !slices.Equal(got, wantPresentIDs) {
		t.Fatalf("Boolean coalesce event IDs = %#v, want %#v", got, wantPresentIDs)
	}

	scalar := func(source string, destination any) {
		t.Helper()
		compiled := compile(base + ` event_id="null-missing" | ` + source)
		if queryErr := connection.QueryRow(
			queryContext,
			compiled.SQL,
			compiled.Args...,
		).Scan(destination); queryErr != nil {
			t.Fatalf(
				"execute coalesce scalar %q: %v\nSQL: %s\nargs: %#v",
				source,
				queryErr,
				compiled.SQL,
				compiled.Args,
			)
		}
	}

	var empty string
	scalar(`eval selected=coalesce("", "fallback") | table selected`, &empty)
	if empty != "" {
		t.Fatalf("coalesce empty String = %q, want empty", empty)
	}
	var zero int64
	scalar(`eval selected=coalesce(0, 1) | table selected`, &zero)
	if zero != 0 {
		t.Fatalf("coalesce zero = %d, want 0", zero)
	}
	var falseValue bool
	scalar(`eval selected=coalesce(false, true) | table selected`, &falseValue)
	if falseValue {
		t.Fatal("coalesce false = true, want false")
	}
	var floating float64
	scalar(
		`eval selected=coalesce(tonumber("not-a-number"), tonumber("1.5")) | table selected`,
		&floating,
	)
	if floating != 1.5 {
		t.Fatalf("coalesce Float64 = %g, want 1.5", floating)
	}
	var wideUnsigned uint64
	scalar(
		`eval selected=coalesce(null, 18446744073709551615) | table selected`,
		&wideUnsigned,
	)
	if wideUnsigned != uint64(18446744073709551615) {
		t.Fatalf(
			"coalesce UInt64 = %d, want %d",
			wideUnsigned,
			uint64(18446744073709551615),
		)
	}
	var severity uint8
	scalar(`eval selected=coalesce(severity, severity) | table selected`, &severity)
	if want := uint8(opensplunk.LogSeverity_LOG_SEVERITY_INFO); severity != want {
		t.Fatalf("coalesce UInt8 = %d, want %d", severity, want)
	}
	var allNull *string
	scalar(`eval selected=coalesce(null, null) | table selected`, &allNull)
	if allNull != nil {
		t.Fatalf("all-null coalesce = %q, want null", *allNull)
	}
	var sequential string
	scalar(
		`eval first=coalesce(null, "selected"), selected=coalesce(first, "fallback") | table selected`,
		&sequential,
	)
	if sequential != "selected" {
		t.Fatalf("sequential coalesce = %q, want selected", sequential)
	}
	var identity string
	scalar(`eval selected=coalesce("identity") | table selected`, &identity)
	if identity != "identity" {
		t.Fatalf("single-argument coalesce = %q, want identity", identity)
	}

	materialized := compile(
		base +
			` | spath input=_raw output=selected path=value` +
			` | eval numeric=coalesce(tonumber(selected), tonumber("1"))` +
			` | where numeric>0`,
	)
	if !strings.Contains(materialized.SQL, " AS MATERIALIZED (") ||
		!strings.Contains(materialized.SQL, `"__os_filter_bound_`) {
		t.Fatalf("executed coalesce lost calculated-field materialization:\n%s", materialized.SQL)
	}
	rows, queryErr := connection.Query(queryContext, materialized.SQL, materialized.Args...)
	if queryErr != nil {
		t.Fatalf(
			"execute materialized coalesce: %v\nSQL: %s\nargs: %#v",
			queryErr,
			materialized.SQL,
			materialized.Args,
		)
	}
	for rows.Next() {
	}
	if rowsErr := rows.Err(); rowsErr != nil {
		_ = rows.Close()
		t.Fatalf("iterate materialized coalesce: %v", rowsErr)
	}
	if closeErr := rows.Close(); closeErr != nil {
		t.Fatalf("close materialized coalesce rows: %v", closeErr)
	}

	physical := compile(
		base +
			` | eval selected=coalesce(if(isnull(probe), null, "present"), "fallback")` +
			` | table event_id, selected`,
	)
	actions := explainCompiledQuery(t, queryContext, connection, explainActionsPrefix, physical)
	if strings.Contains(actions, "ArrayJoin") {
		t.Fatalf("coalesce expands event rows:\n%s", actions)
	}
}
