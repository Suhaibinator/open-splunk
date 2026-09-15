package clickhouse

import (
	"context"
	"os"
	"os/exec"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/ingest"
	"github.com/Suhaibinator/open-splunk/internal/testsupport"
	"github.com/Suhaibinator/open-splunk/internal/testsupport/officialspl"
)

// TestOfficialCoreRuntimeConformance executes row-level result oracles for
// documented search and eval cases. Golden SQL remains useful lowering
// evidence; these fixtures independently pin identities, values, and schemas.
func TestOfficialCoreRuntimeConformance(t *testing.T) {
	if os.Getenv("OPEN_SPLUNK_CLICKHOUSE_INTEGRATION") != "1" {
		t.Skip("set OPEN_SPLUNK_CLICKHOUSE_INTEGRATION=1 to run the Docker integration test")
	}
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skipf("docker CLI is unavailable: %v", err)
	}
	if _, err := testsupport.ResolvePinnedClickHouseImage(os.Getenv("OPEN_SPLUNK_CLICKHOUSE_TEST_IMAGE")); err != nil {
		t.Fatalf("resolve pinned ClickHouse integration image: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()
	connection, store := chartEdgeStartClickHouse(t, ctx)
	indexTime := time.Date(2026, time.September, 15, 12, 0, 0, 0, time.UTC)
	events := []*ingest.StoredEvent{
		authoredExpressionStoredEvent("accepted-400", indexTime, typedField("status", typedSint(400)), typedField("price", typedSint(3)), typedField("quantity", typedSint(2))),
		authoredExpressionStoredEvent("accepted-500", indexTime, typedField("status", typedSint(500)), typedField("price", typedSint(4)), typedField("quantity", typedSint(5))),
		authoredExpressionStoredEvent("rejected-200", indexTime, typedField("status", typedSint(200)), typedField("price", typedSint(7)), typedField("quantity", typedSint(11))),
	}
	for _, event := range events {
		event.Event.IndexName = "official-core-v01"
	}
	compile, queryContext := storeScalarFunctionIntegrationFixtures(
		ctx, t, store, indexTime, "official-core-v01", "official-core-v01-batch", 251, events...,
	)

	corpus := loadOfficialGoldenCorpus(t)
	caseByID := make(map[string]officialspl.Case, len(corpus.Cases))
	for _, testCase := range corpus.Cases {
		caseByID[testCase.ID] = testCase
	}
	requireCase := func(id, sourceURL string) officialspl.Case {
		t.Helper()
		testCase, ok := caseByID[id]
		if !ok {
			t.Fatalf("official corpus case %q is missing", id)
		}
		if testCase.Source.URL != sourceURL {
			t.Fatalf("official corpus case %q source = %q, want %q", id, testCase.Source.URL, sourceURL)
		}
		return testCase
	}
	rewriteFixtureIndex := func(testCase officialspl.Case) string {
		t.Helper()
		remainder, ok := strings.CutPrefix(testCase.Query, "index=main")
		if !ok {
			t.Fatalf("official corpus case %q query does not begin with index=main: %q", testCase.ID, testCase.Query)
		}
		return "index=official-core-v01" + remainder
	}

	for _, testCase := range []struct {
		id      string
		source  string
		wantIDs []string
	}{
		{
			id:      "search.base-membership",
			source:  "https://help.splunk.com/en/splunk-enterprise/spl-search-reference/9.1/search-commands/search",
			wantIDs: []string{"accepted-400", "accepted-500"},
		},
		{
			id:      "search.pipeline-filter",
			source:  "https://help.splunk.com/en/splunk-enterprise/search/spl-search-reference/10.0/search-commands/search",
			wantIDs: []string{"rejected-200"},
		},
	} {
		t.Run(testCase.id, func(t *testing.T) {
			officialCase := requireCase(testCase.id, testCase.source)
			query := rewriteFixtureIndex(officialCase)
			compiled := compile(query + ` | sort 0 +event_id | table event_id status`)
			if !slices.Equal(compiled.OutputFields, []string{"event_id", "status"}) {
				t.Fatalf("output schema = %v, want [event_id status]", compiled.OutputFields)
			}
			rows, err := connection.Query(queryContext, compiled.SQL, compiled.Args...)
			if err != nil {
				t.Fatalf("execute official case %s: %v\nSQL: %s", testCase.id, err, compiled.SQL)
			}
			defer func() { _ = rows.Close() }()
			got := make(map[string]int64, len(testCase.wantIDs))
			for rows.Next() {
				var eventID string
				var status int64
				if err := rows.Scan(&eventID, &status); err != nil {
					t.Fatalf("scan official case %s: %v", testCase.id, err)
				}
				if _, duplicate := got[eventID]; duplicate {
					t.Fatalf("official case %s returned duplicate event_id %q", testCase.id, eventID)
				}
				got[eventID] = status
			}
			if err := rows.Err(); err != nil {
				t.Fatalf("iterate official case %s: %v", testCase.id, err)
			}
			want := make(map[string]int64, len(testCase.wantIDs))
			for _, eventID := range testCase.wantIDs {
				switch eventID {
				case "accepted-400":
					want[eventID] = 400
				case "accepted-500":
					want[eventID] = 500
				case "rejected-200":
					want[eventID] = 200
				}
			}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("rows = %#v, want %#v", got, want)
			}
		})
	}

	t.Run("eval.arithmetic-assignment", func(t *testing.T) {
		officialCase := requireCase(
			"eval.arithmetic-assignment",
			"https://help.splunk.com/en/splunk-enterprise/search/spl-search-reference/10.0/search-commands/eval",
		)
		query := rewriteFixtureIndex(officialCase)
		compiled := compile(query + ` | sort 0 +event_id | table event_id total`)
		if !slices.Equal(compiled.OutputFields, []string{"event_id", "total"}) {
			t.Fatalf("output schema = %v, want [event_id total]", compiled.OutputFields)
		}
		rows, err := connection.Query(queryContext, compiled.SQL, compiled.Args...)
		if err != nil {
			t.Fatalf("execute official eval case: %v\nSQL: %s", err, compiled.SQL)
		}
		defer func() { _ = rows.Close() }()
		got := make(map[string]float64, 3)
		for rows.Next() {
			var eventID string
			var total float64
			if err := rows.Scan(&eventID, &total); err != nil {
				t.Fatalf("scan official eval case: %v", err)
			}
			if _, duplicate := got[eventID]; duplicate {
				t.Fatalf("official eval case returned duplicate event_id %q", eventID)
			}
			got[eventID] = total
		}
		if err := rows.Err(); err != nil {
			t.Fatalf("iterate official eval case: %v", err)
		}
		want := map[string]float64{"accepted-400": 6, "accepted-500": 20, "rejected-200": 77}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("rows = %#v, want %#v", got, want)
		}
	})
}
