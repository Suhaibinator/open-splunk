package clickhouse

import (
	"context"
	"slices"
	"strings"
	"testing"
	"time"

	clickhousedriver "github.com/ClickHouse/clickhouse-go/v2"
	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/ingest"
)

func testNullPredicatesAgainstClickHouse(
	ctx context.Context,
	t *testing.T,
	store *Store,
	connection clickhousedriver.Conn,
	indexTime time.Time,
) {
	t.Helper()

	const (
		batchID            = "null-predicate-batch"
		collectorID        = "null-predicate-collector"
		expectedEventCount = 9
	)
	newEvent := func(id string, fields ...*opensplunk.TypedObjectField) *ingest.StoredEvent {
		event := compilerIntegrationEvent(id, "null-predicate-host", "null predicate fixture", indexTime, fields...)
		event.CollectorID = collectorID
		event.BatchID = batchID
		event.Event.Source = "null-predicate"
		return event
	}
	events := []*ingest.StoredEvent{
		newEvent("null-missing"),
		newEvent("null-explicit", typedField("probe", typedNull())),
		newEvent(
			"null-empty-text",
			typedField("probe", typedString("")),
			typedField("fixed_candidate", typedString("present")),
		),
		newEvent("null-zero", typedField("probe", typedSint(0))),
		newEvent("null-false", typedField("probe", typedBool(false))),
		newEvent("null-empty-list", typedField("probe", typedList())),
		newEvent("null-list-null", typedField("probe", typedList(typedNull()))),
		newEvent("null-list", typedField("probe", typedList(typedString("value")))),
		newEvent(
			"null-object",
			typedField("probe", typedObject(typedField("child", typedString("value")))),
		),
	}
	result, err := store.Store(ctx, ingest.StoreBatch{
		TenantID:          "tenant",
		CollectorID:       collectorID,
		BatchID:           batchID,
		BatchSequence:     1,
		SourceBatchSHA256: testSourceBatchDigest(batchID),
		ReceivedAt:        indexTime,
		Events:            events,
	})
	if err != nil {
		t.Fatalf("store null-predicate fixtures: %v", err)
	}
	if len(events) != expectedEventCount {
		t.Fatalf("null-predicate fixture count = %d, want %d", len(events), expectedEventCount)
	}
	if result.Accepted != expectedEventCount || result.Duplicate != 0 {
		t.Fatalf("store null-predicate fixtures result = %+v, want %d accepted", result, len(events))
	}
	visibilityCutoff, err := store.VisibilityCutoff(ctx)
	if err != nil {
		t.Fatalf("capture null-predicate visibility cutoff: %v", err)
	}
	compile := func(source string) CompiledQuery {
		t.Helper()
		return compileIntegrationSPL(t, source, indexTime.Add(10*time.Second), visibilityCutoff)
	}
	base := `index=compiler source="null-predicate"`
	queryEventIDs := func(source string) []string {
		t.Helper()
		compiled := compile(source + ` | table event_id`)
		rows, queryErr := connection.Query(ctx, compiled.SQL, compiled.Args...)
		if queryErr != nil {
			t.Fatalf("execute null-predicate query %q: %v\nSQL: %s\nargs: %#v", source, queryErr, compiled.SQL, compiled.Args)
		}
		defer func() {
			if closeErr := rows.Close(); closeErr != nil {
				t.Errorf("close null-predicate rows: %v", closeErr)
			}
		}()
		var eventIDs []string
		for rows.Next() {
			var eventID string
			if scanErr := rows.Scan(&eventID); scanErr != nil {
				t.Fatalf("scan null-predicate event ID: %v", scanErr)
			}
			eventIDs = append(eventIDs, eventID)
		}
		if rowsErr := rows.Err(); rowsErr != nil {
			t.Fatalf("iterate null-predicate rows: %v", rowsErr)
		}
		slices.Sort(eventIDs)
		return eventIDs
	}
	assertEventIDs := func(name string, got, want []string) {
		t.Helper()
		slices.Sort(want)
		if !slices.Equal(got, want) {
			t.Fatalf("%s event IDs = %#v, want %#v", name, got, want)
		}
	}

	nullIDs := []string{"null-explicit", "null-missing"}
	nonNullIDs := []string{
		"null-empty-list",
		"null-empty-text",
		"null-false",
		"null-list",
		"null-list-null",
		"null-object",
		"null-zero",
	}
	allIDs := append(append([]string(nil), nullIDs...), nonNullIDs...)
	assertEventIDs("isnull", queryEventIDs(base+` | where isnull(probe)`), nullIDs)
	assertEventIDs("isnotnull", queryEventIDs(base+` | where isnotnull(probe)`), nonNullIDs)
	assertEventIDs("NOT isnull", queryEventIDs(base+` | where NOT isnull(probe)`), nonNullIDs)
	assertEventIDs(
		"explicit Boolean comparison",
		queryEventIDs(base+` | where isnull(probe)=true`),
		nullIDs,
	)
	assertEventIDs(
		"complement union",
		queryEventIDs(base+` | where isnull(probe) OR isnotnull(probe)`),
		allIDs,
	)
	assertEventIDs(
		"complement intersection",
		queryEventIDs(base+` | where isnull(probe) AND isnotnull(probe)`),
		nil,
	)
	assertEventIDs(
		"flattened object child",
		queryEventIDs(base+` | where isnotnull(probe.child)`),
		[]string{"null-object"},
	)
	assertEventIDs(
		"projected-away field",
		queryEventIDs(base+` | fields event_id | where isnull(probe)`),
		allIDs,
	)
	assertEventIDs(
		"nullable scalar result",
		queryEventIDs(base+` | eval bad=tonumber("bad") | where isnull(bad)`),
		allIDs,
	)

	for _, test := range []struct {
		name   string
		source string
	}{
		{
			name: "empty fixed multivalue",
			source: base + ` event_id="null-missing"` +
				` | stats values(fixed_candidate) AS values | where isnull(values) | stats count AS matches`,
		},
		{
			name: "nonempty fixed multivalue",
			source: base + ` event_id="null-empty-text"` +
				` | stats values(fixed_candidate) AS values | where isnotnull(values) | stats count AS matches`,
		},
	} {
		compiled := compile(test.source)
		var matches uint64
		if queryErr := connection.QueryRow(ctx, compiled.SQL, compiled.Args...).Scan(&matches); queryErr != nil {
			t.Fatalf("execute %s: %v\nSQL: %s\nargs: %#v", test.name, queryErr, compiled.SQL, compiled.Args)
		}
		if matches != 1 {
			t.Fatalf("%s matched %d rows, want 1", test.name, matches)
		}
	}

	physical := compile(base + ` | where isnull(probe) OR isnotnull(probe) | table event_id`)
	actions := explainCompiledQuery(t, ctx, connection, explainActionsPrefix, physical)
	if strings.Contains(actions, "ArrayJoin") {
		t.Fatalf("null predicates expand event rows:\n%s", actions)
	}
}
