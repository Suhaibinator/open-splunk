package clickhouse

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/ingest"
	"github.com/Suhaibinator/open-splunk/internal/testsupport"
)

func TestStatsMultivalueByAgainstClickHouse(t *testing.T) {
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

	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()
	connection, store := chartEdgeStartClickHouse(t, ctx)
	indexTime := time.Date(2026, time.August, 11, 20, 0, 0, 0, time.UTC)
	newEvent := func(
		id string,
		source string,
		fields ...*opensplunk.TypedObjectField,
	) *ingest.StoredEvent {
		event := testStoredEvent(id, "stats-mv-by", indexTime)
		event.Event.Source = source
		event.Event.Fields = typedObjectValue(fields...)
		return event
	}
	events := []*ingest.StoredEvent{
		newEvent("main-1", "stats-mv-by-main",
			typedField("value", typedSint(1)),
			typedField("tags", typedList(typedString("a"), typedString("b"), typedString("b"))),
		),
		newEvent("main-2", "stats-mv-by-main",
			typedField("value", typedSint(2)),
			typedField("tags", typedList(typedString("b"), typedString("c"))),
		),
		newEvent("main-3", "stats-mv-by-main",
			typedField("value", typedSint(3)),
			typedField("tags", typedString("a")),
		),
		newEvent("main-missing", "stats-mv-by-main",
			typedField("value", typedSint(4)),
		),
		newEvent("cartesian", "stats-mv-by-cart",
			typedField("value", typedSint(5)),
			typedField("tags", typedList(typedString("a"), typedString("b"))),
			typedField("zones", typedList(typedString("1"), typedString("2"), typedString("2"))),
		),
		newEvent("invalid", "stats-mv-by-invalid",
			typedField("tags", typedList(typedObject(typedField("nested", typedString("x"))))),
		),
	}
	compile, queryContext := storeScalarFunctionIntegrationFixtures(
		ctx,
		t,
		store,
		indexTime,
		"stats-mv-by",
		"stats-mv-by-batch",
		361,
		events...,
	)

	type groupedValue struct {
		count uint64
		total float64
	}
	readSingleGroup := func(source string, deduplicate bool) map[string]groupedValue {
		t.Helper()
		option := ""
		if deduplicate {
			option = " dedup_splitvals=true"
		}
		compiled := compile(
			`index=stats-mv-by source="` + source + `" | stats count ` +
				`sum(value) AS total BY tags` + option + ` | sort tags`,
		)
		rows, queryErr := connection.Query(
			queryContext,
			compiled.SQL,
			compiled.Args...,
		)
		if queryErr != nil {
			t.Fatalf("execute multivalue BY: %v\nSQL: %s\nargs: %#v", queryErr, compiled.SQL, compiled.Args)
		}
		result := make(map[string]groupedValue)
		for rows.Next() {
			var tag string
			var count uint64
			var total *float64
			if scanErr := rows.Scan(&tag, &count, &total); scanErr != nil {
				_ = rows.Close()
				t.Fatalf("scan multivalue BY: %v", scanErr)
			}
			if total == nil {
				_ = rows.Close()
				t.Fatalf("multivalue BY total for %q is null", tag)
			}
			result[tag] = groupedValue{count: count, total: *total}
		}
		if rowsErr := rows.Err(); rowsErr != nil {
			_ = rows.Close()
			t.Fatalf("iterate multivalue BY: %v", rowsErr)
		}
		if closeErr := rows.Close(); closeErr != nil {
			t.Fatalf("close multivalue BY: %v", closeErr)
		}
		return result
	}

	assertGroups := func(name string, got, want map[string]groupedValue) {
		t.Helper()
		if fmt.Sprint(got) != fmt.Sprint(want) {
			t.Fatalf("%s groups = %#v, want %#v", name, got, want)
		}
	}
	assertGroups("duplicate-preserving", readSingleGroup("stats-mv-by-main", false), map[string]groupedValue{
		"a": {count: 2, total: 4},
		"b": {count: 3, total: 4},
		"c": {count: 1, total: 2},
	})
	assertGroups("deduplicated", readSingleGroup("stats-mv-by-main", true), map[string]groupedValue{
		"a": {count: 2, total: 4},
		"b": {count: 2, total: 3},
		"c": {count: 1, total: 2},
	})

	type cartesianValue struct {
		count uint64
		total float64
	}
	readCartesian := func(deduplicate bool) map[string]cartesianValue {
		t.Helper()
		option := ""
		if deduplicate {
			option = " dedup_splitvals=true"
		}
		compiled := compile(
			`index=stats-mv-by source="stats-mv-by-cart" | stats count ` +
				`sum(value) AS total BY tags zones` + option + ` | sort tags zones`,
		)
		rows, queryErr := connection.Query(queryContext, compiled.SQL, compiled.Args...)
		if queryErr != nil {
			t.Fatalf("execute Cartesian multivalue BY: %v\nSQL: %s\nargs: %#v", queryErr, compiled.SQL, compiled.Args)
		}
		result := make(map[string]cartesianValue)
		for rows.Next() {
			var tag, zone string
			var count uint64
			var total *float64
			if scanErr := rows.Scan(&tag, &zone, &count, &total); scanErr != nil {
				_ = rows.Close()
				t.Fatalf("scan Cartesian multivalue BY: %v", scanErr)
			}
			if total == nil {
				_ = rows.Close()
				t.Fatalf("Cartesian total for %s/%s is null", tag, zone)
			}
			result[tag+"/"+zone] = cartesianValue{count: count, total: *total}
		}
		if rowsErr := rows.Err(); rowsErr != nil {
			_ = rows.Close()
			t.Fatalf("iterate Cartesian multivalue BY: %v", rowsErr)
		}
		if closeErr := rows.Close(); closeErr != nil {
			t.Fatalf("close Cartesian multivalue BY: %v", closeErr)
		}
		return result
	}
	assertCartesian := func(name string, got map[string]cartesianValue, duplicateCount uint64, duplicateTotal float64) {
		t.Helper()
		want := map[string]cartesianValue{
			"a/1": {count: 1, total: 5},
			"a/2": {count: duplicateCount, total: duplicateTotal},
			"b/1": {count: 1, total: 5},
			"b/2": {count: duplicateCount, total: duplicateTotal},
		}
		if fmt.Sprint(got) != fmt.Sprint(want) {
			t.Fatalf("%s Cartesian groups = %#v, want %#v", name, got, want)
		}
	}
	assertCartesian("duplicate-preserving", readCartesian(false), 2, 10)
	assertCartesian("deduplicated", readCartesian(true), 1, 5)

	invalid := compile(
		`index=stats-mv-by source="stats-mv-by-invalid" | stats count BY tags`,
	)
	invalidRows, queryErr := connection.Query(
		queryContext,
		invalid.SQL,
		invalid.Args...,
	)
	if queryErr == nil {
		for invalidRows.Next() {
		}
		queryErr = invalidRows.Err()
		_ = invalidRows.Close()
	}
	if queryErr == nil || !strings.Contains(queryErr.Error(), UnsupportedStatsByValueMarker) {
		t.Fatalf("nested multivalue BY error = %v, want %q", queryErr, UnsupportedStatsByValueMarker)
	}
}
