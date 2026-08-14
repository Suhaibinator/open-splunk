package clickhouse

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	clickhousedriver "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/Suhaibinator/open-splunk/internal/testsupport"
)

// TestStatsByUnsupportedCannotHideBehindMissingKeyAgainstClickHouse proves
// that one zero-cardinality BY dimension cannot mask validation of a sibling
// poisoned dimension. The complete operation must fail before a supported
// prefix escapes, and the backend marker must not disclose the poisoned value.
func TestStatsByUnsupportedCannotHideBehindMissingKeyAgainstClickHouse(t *testing.T) {
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

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	connection, store := chartEdgeStartClickHouse(t, ctx)
	indexTime := time.Date(2026, time.August, 12, 22, 0, 0, 0, time.UTC)
	const secret = "sdet-poison-must-not-leak"
	event := testStoredEvent("sdet-invalid-missing", "sdet-stats-by-missing", indexTime)
	event.Event.Source = "sdet-invalid-missing"
	event.Event.Fields = typedObjectValue(
		typedField("grouping", typedObject(
			typedField("secret", typedString(secret)),
		)),
	)
	compile, queryContext := storeScalarFunctionIntegrationFixtures(
		ctx,
		t,
		store,
		indexTime,
		"sdet-stats-by-missing",
		"sdet-stats-by-missing-batch",
		977,
		event,
	)
	compiled := compile(
		`index=sdet-stats-by-missing source="sdet-invalid-missing"` +
			` | stats count BY grouping absent_dimension | head 1`,
	)

	rowsSeen := 0
	rows, queryErr := connection.Query(queryContext, compiled.SQL, compiled.Args...)
	if queryErr == nil {
		for rows.Next() {
			rowsSeen++
		}
		queryErr = rows.Err()
		if closeErr := rows.Close(); queryErr == nil {
			queryErr = closeErr
		}
	}
	if rowsSeen != 0 {
		t.Fatalf("unsupported stats BY published %d rows before failing", rowsSeen)
	}
	var exception *clickhousedriver.Exception
	if !errors.As(queryErr, &exception) || exception.Code != 395 {
		t.Fatalf("unsupported stats BY error type/code = %T, want ClickHouse code 395", queryErr)
	}
	if !strings.Contains(exception.Message, UnsupportedStatsByValueMarker) ||
		strings.Contains(exception.Message, UnsupportedStatsMeasureValueMarker) {
		t.Fatal("unsupported stats BY used the wrong sanitized marker")
	}
	if strings.Contains(exception.Message, secret) {
		t.Fatal("unsupported stats BY exception disclosed the poisoned event value")
	}
}
