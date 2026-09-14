package clickhouse

import (
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"

	clickhousedriver "github.com/ClickHouse/clickhouse-go/v2"
)

// This full-schema fixture spans 64 granules, so EXPLAIN demonstrates physical
// pruning independently of machine-specific wall-clock timing. Every result
// is also executed with skip indexes disabled.
func TestNormalizedIDIndexesAgainstClickHouse(t *testing.T) {
	if os.Getenv("OPEN_SPLUNK_CLICKHOUSE_INTEGRATION") != "1" {
		t.Skip("set OPEN_SPLUNK_CLICKHOUSE_INTEGRATION=1 to run the Docker integration test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	connection, _ := startClickHouseStoreFixture(t, ctx)
	const fixtureRows = 524_288
	const fixtureIndex = "normalized-id-index"
	const insert = `INSERT INTO open_splunk.events
		(event_id, tenant_id, index_name, event_time, index_time, raw, raw_encoding,
		 trace_id, span_id, field_metadata_version, collector_id, ingest_source_kind,
		 ingest_source_id, batch_id, batch_sequence, visibility_seq, expires_at)
		SELECT ifNull(id, concat('event-', toString(number))), 'tenant', ?,
			toDateTime64('2026-07-21 03:00:00', 9) + toIntervalNanosecond(number),
			toDateTime64('2026-07-21 04:00:00', 3), 'quiet filler', 1, id, id,
			1, 'id-index-collector', 1, 'id-index-collector', 'id-index-batch', 1, 1,
			toDateTime64('2100-01-01 00:00:00', 3)
		FROM (SELECT number, multiIf(
			number = 32768, 'NeEdLe', number = 32769, 'needle',
			number = 32770, '', number = 32771, NULL,
			number = 32772, 'CAFÉ', number = 32773, unhex('ff41'),
			number = 32774, '00123', number = 32775, '123',
			concat('other-', toString(number))) AS id FROM numbers(?))`
	if err := connection.Exec(ctx, insert, fixtureIndex, fixtureRows); err != nil {
		t.Fatalf("insert normalized ID fixture: %v", err)
	}
	cutoff := time.Date(2026, time.July, 21, 4, 1, 0, 0, time.UTC)
	compile := func(source string) CompiledQuery {
		t.Helper()
		return compileIntegrationSPLForIndex(t, source+" | stats count", cutoff, 1, fixtureIndex)
	}
	for _, field := range []string{"event_id", "trace_id", "span_id"} {
		t.Run(field, func(t *testing.T) {
			for _, test := range []struct {
				term string
				want uint64
			}{
				{`="NEEDLE"`, 2},
				{`=""`, 1},
				{`="café"`, 1},
				{`="00123"`, 1},
				{`="123"`, 1},
				{`="absent-id"`, 0},
				{`="need*"`, 2},
				{`!="needle"`, fixtureRows - 2},
			} {
				source := "index=" + fixtureIndex + " " + field + test.term
				assertNormalizedIDCount(t, ctx, connection, compile(source), test.want)
			}
			query := compile("index=" + fixtureIndex + " " + field + `="NEEDLE"`)
			assertNormalizedIDIndexPrunes(t, ctx, connection, query, "idx_"+field+"_ci")
		})
	}
	for _, source := range []string{
		`trace_id=null`,
		`span_id=null`,
	} {
		assertNormalizedIDCount(t, ctx, connection, compile("index="+fixtureIndex+" "+source), 1)
	}
	for _, test := range []struct {
		pipeline string
		want     uint64
	}{
		{`trace_id="needle" OR span_id="CAFÉ"`, 3},
		{`NOT trace_id="needle"`, fixtureRows - 2},
		{`| eval trace_id="needle" | search trace_id="needle"`, fixtureRows},
		{`| rename trace_id AS id | search id="needle"`, 2},
		{`| table trace_id | search trace_id="needle"`, 2},
		{`| eval trace_id="" | search trace_id=""`, fixtureRows},
	} {
		assertNormalizedIDCount(t, ctx, connection, compile("index="+fixtureIndex+" "+test.pipeline), test.want)
	}
}

func assertNormalizedIDCount(t *testing.T, ctx context.Context, connection clickhousedriver.Conn, query CompiledQuery, want uint64) {
	t.Helper()
	for _, indexes := range []uint8{0, 1} {
		queryContext := clickhousedriver.Context(ctx, clickhousedriver.WithSettings(clickhousedriver.Settings{
			"use_skip_indexes":          indexes,
			"use_query_condition_cache": uint8(0),
			"use_query_cache":           uint8(0),
		}))
		var count uint64
		if err := connection.QueryRow(queryContext, query.SQL, query.Args...).Scan(&count); err != nil {
			t.Fatalf("execute normalized ID query (indexes=%d): %v\n%s", indexes, err, query.SQL)
		}
		if count != want {
			t.Fatalf("normalized ID count (indexes=%d) = %d, want %d\n%s", indexes, count, want, query.SQL)
		}
	}
}

func assertNormalizedIDIndexPrunes(t *testing.T, ctx context.Context, connection clickhousedriver.Conn, query CompiledQuery, name string) {
	t.Helper()
	explainContext := clickhousedriver.Context(ctx, clickhousedriver.WithSettings(clickhousedriver.Settings{
		"use_skip_indexes":              uint8(1),
		"use_skip_indexes_on_data_read": uint8(0),
		"use_query_condition_cache":     uint8(0),
	}))
	var planText string
	if err := connection.QueryRow(explainContext, "EXPLAIN json=1, indexes=1 "+query.SQL, query.Args...).Scan(&planText); err != nil {
		t.Fatalf("explain normalized ID index: %v", err)
	}
	var envelopes []rawTextExplainEnvelope
	if err := json.Unmarshal([]byte(planText), &envelopes); err != nil || len(envelopes) != 1 {
		t.Fatalf("decode normalized ID EXPLAIN: %v\n%s", err, planText)
	}
	var found []rawTextExplainIndex
	var visit func(rawTextExplainNode)
	visit = func(node rawTextExplainNode) {
		for _, index := range node.Indexes {
			if index.Type == "Skip" && index.Name == name {
				found = append(found, index)
			}
		}
		for _, child := range node.Plans {
			visit(child)
		}
	}
	visit(envelopes[0].Plan)
	if len(found) != 1 || found[0].InitialGranules < 32 || found[0].SelectedGranules == 0 ||
		found[0].SelectedGranules >= found[0].InitialGranules/2 {
		t.Fatalf("normalized ID index did not prune most granules: %+v\n%s", found, planText)
	}
	t.Logf("%s selected %d/%d granules", name, found[0].SelectedGranules, found[0].InitialGranules)
}
