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

func TestStatsMultivalueByExpansionLimitAgainstClickHouse(t *testing.T) {
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
	indexTime := time.Date(2026, time.August, 11, 21, 0, 0, 0, time.UTC)

	uniqueList := func(prefix string, count int) *opensplunk.TypedValue {
		members := make([]*opensplunk.TypedValue, count)
		for index := range members {
			members[index] = typedString(fmt.Sprintf("%s-%03d", prefix, index))
		}
		return typedList(members...)
	}
	repeatedList := func(value string, count int) *opensplunk.TypedValue {
		members := make([]*opensplunk.TypedValue, count)
		for index := range members {
			members[index] = typedString(value)
		}
		return typedList(members...)
	}
	newEvent := func(
		id string,
		source string,
		fields ...*opensplunk.TypedObjectField,
	) *ingest.StoredEvent {
		event := testStoredEvent(id, "stats-mv-by-limit", indexTime)
		event.Event.Source = source
		event.Event.Fields = typedObjectValue(fields...)
		return event
	}

	highDimensionFields := make([]*opensplunk.TypedObjectField, 16)
	highDimensionNames := make([]string, len(highDimensionFields))
	for index := range highDimensionFields {
		name := fmt.Sprintf("dimension_%02d", index)
		highDimensionNames[index] = name
		// Ten members in every supported BY dimension would otherwise turn
		// this single source event into 10^16 intermediate rows.
		highDimensionFields[index] = typedField(name, uniqueList(name, 10))
	}
	events := []*ingest.StoredEvent{
		newEvent("boundary", "stats-mv-boundary",
			typedField("tags", uniqueList("tag", 100)),
			typedField("zones", uniqueList("zone", 100)),
		),
		newEvent("overflow", "stats-mv-overflow",
			typedField("tags", uniqueList("tag", 101)),
			typedField("zones", uniqueList("zone", 100)),
		),
		newEvent("dedup", "stats-mv-dedup",
			typedField("tags", repeatedList("same", 101)),
			typedField("zones", uniqueList("zone", 100)),
		),
		newEvent("zero", "stats-mv-zero",
			typedField("tags", uniqueList("tag", 101)),
			typedField("zones", uniqueList("zone", 100)),
			typedField("empty", typedList()),
		),
		newEvent("missing", "stats-mv-missing",
			typedField("tags", uniqueList("tag", 101)),
			typedField("zones", uniqueList("zone", 100)),
		),
		newEvent("high-dimensional", "stats-mv-high-dimensional", highDimensionFields...),
		newEvent("atomic-safe", "stats-mv-atomic",
			typedField("tags", uniqueList("safe-tag", 2)),
			typedField("zones", uniqueList("safe-zone", 2)),
		),
		newEvent("atomic-overflow", "stats-mv-atomic",
			typedField("tags", uniqueList("tag", 101)),
			typedField("zones", uniqueList("zone", 100)),
		),
	}
	compile, queryContext := storeScalarFunctionIntegrationFixtures(
		ctx,
		t,
		store,
		indexTime,
		"stats-mv-by-limit",
		"stats-mv-by-limit-batch",
		362,
		events...,
	)

	readOneCount := func(source string, suffix string) uint64 {
		t.Helper()
		compiled := compile(
			`index=stats-mv-by-limit source="` + source + `" | ` + suffix,
		)
		rows, queryErr := connection.Query(queryContext, compiled.SQL, compiled.Args...)
		if queryErr != nil {
			t.Fatalf("execute %s: %v\nSQL: %s", source, queryErr, compiled.SQL)
		}
		defer rows.Close()
		if !rows.Next() {
			t.Fatalf("%s returned no count row: %v", source, rows.Err())
		}
		var count uint64
		if scanErr := rows.Scan(&count); scanErr != nil {
			t.Fatalf("scan %s count: %v", source, scanErr)
		}
		if rows.Next() {
			t.Fatalf("%s returned more than one count row", source)
		}
		if rowsErr := rows.Err(); rowsErr != nil {
			t.Fatalf("iterate %s count: %v", source, rowsErr)
		}
		return count
	}

	if got := readOneCount(
		"stats-mv-boundary",
		`stats count BY tags zones | stats count AS groups`,
	); got != MaximumStatsMultivalueByCombinationsPerEvent {
		t.Fatalf("exact boundary groups = %d, want %d", got, MaximumStatsMultivalueByCombinationsPerEvent)
	}

	executeError := func(source string, suffix string) (int, error) {
		t.Helper()
		compiled := compile(
			`index=stats-mv-by-limit source="` + source + `" | ` + suffix,
		)
		rows, queryErr := connection.Query(queryContext, compiled.SQL, compiled.Args...)
		if queryErr != nil {
			return 0, queryErr
		}
		rowCount := 0
		for rows.Next() {
			rowCount++
		}
		queryErr = rows.Err()
		if closeErr := rows.Close(); queryErr == nil {
			queryErr = closeErr
		}
		return rowCount, queryErr
	}
	assertExpansionLimit := func(source string, suffix string) {
		t.Helper()
		rowCount, queryErr := executeError(source, suffix)
		if queryErr == nil || !strings.Contains(
			queryErr.Error(),
			StatsMultivalueByExpansionLimitMarker,
		) {
			t.Fatalf(
				"%s expansion error = %v, want %q",
				source,
				queryErr,
				StatsMultivalueByExpansionLimitMarker,
			)
		}
		if rowCount != 0 {
			t.Fatalf("%s returned %d partial rows before its limit error", source, rowCount)
		}
	}
	assertExpansionLimit(
		"stats-mv-overflow",
		`stats count BY tags zones | stats count AS groups`,
	)
	assertExpansionLimit(
		"stats-mv-dedup",
		`stats count BY tags zones | stats count AS groups`,
	)
	if got := readOneCount(
		"stats-mv-dedup",
		`stats count BY tags zones dedup_splitvals=true | stats count AS groups`,
	); got != 100 {
		t.Fatalf("deduplicated groups = %d, want 100", got)
	}
	if got := readOneCount(
		"stats-mv-zero",
		`stats count BY tags zones empty | stats count AS groups`,
	); got != 0 {
		t.Fatalf("empty final Cartesian dimension groups = %d, want 0", got)
	}
	if got := readOneCount(
		"stats-mv-missing",
		`stats count BY tags zones absent_dimension | stats count AS groups`,
	); got != 0 {
		t.Fatalf("missing final Cartesian dimension groups = %d, want 0", got)
	}
	assertExpansionLimit(
		"stats-mv-high-dimensional",
		`stats count BY `+strings.Join(highDimensionNames, " ")+` | stats count AS groups`,
	)
	assertExpansionLimit(
		"stats-mv-atomic",
		`stats count BY tags zones | head 1`,
	)
}
