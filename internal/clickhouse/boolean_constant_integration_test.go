package clickhouse

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/ingest"
)

func TestBooleanConstantsAgainstClickHouse(t *testing.T) {
	requireClickHouseIntegration(t)
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()
	connection, store := chartEdgeStartClickHouse(t, ctx)
	indexTime := time.Date(2026, time.November, 5, 12, 0, 0, 0, time.UTC)
	fallback := strings.Repeat("unclassified ", 8)
	fixtures := []struct {
		sql     string
		label   string
		elapsed int64
	}{
		{`SELECT count(*) FROM "crawl_jobs" FOR UPDATE`, "job row lock (FOR UPDATE)", 100},
		{`SELECT * FROM "crawl_jobs" FOR UPDATE`, "job row lock (FOR UPDATE)", 100},
		{`SELECT * FROM crawl_jobs FOR SHARE`, "claim job lock (FOR SHARE)", 60},
		{`UPDATE crawl_jobs SET parent_count=(SELECT count(*) FROM nodes)`, "parent_count recount", 50},
		{`INSERT INTO scrape_nodes VALUES (1)`, "discovery node upsert", 40},
		{`UPDATE crawl_jobs SET seed_expanded=1`, "seed expansion", 30},
		{`SELECT count(*) FROM nodes`, "progress counting", 20},
		{fallback, fallback[:80], 10},
	}
	events := make([]*ingest.StoredEvent, 0, len(fixtures))
	for index, fixture := range fixtures {
		event := testStoredEvent(fmt.Sprintf("slow-query-%d", index), "shubuhat", indexTime)
		event.Event.Raw = []byte("GORM slow query " + fixture.sql)
		event.Event.Fields = typedObjectValue(
			typedField("sql", typedString(fixture.sql)),
			typedField("elapsed_ms", typedSint(fixture.elapsed)),
		)
		events = append(events, event)
	}
	compile, queryContext := storeScalarFunctionIntegrationFixtures(
		ctx, t, store, indexTime, "shubuhat", "boolean-constants", 1, events...,
	)

	t.Run("slow query classification and aggregates", func(t *testing.T) {
		compiled := compile(slowQueryClassificationSPL)
		rows, err := connection.Query(queryContext, compiled.SQL, compiled.Args...)
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := rows.Close(); err != nil {
				t.Errorf("close classification rows: %v", err)
			}
		}()
		row := 0
		for rows.Next() {
			var label string
			var count uint64
			var average, percentile, maximum, total float64
			if err := rows.Scan(&label, &count, &average, &percentile, &maximum, &total); err != nil {
				t.Fatal(err)
			}
			fixtureIndex := row
			wantCount := uint64(2)
			if row > 0 {
				fixtureIndex++
				wantCount = 1
			}
			if fixtureIndex >= len(fixtures) {
				t.Fatal("unexpected extra classification row")
			}
			fixture := fixtures[fixtureIndex]
			elapsed := float64(fixture.elapsed)
			if label != fixture.label || count != wantCount || average != elapsed ||
				percentile != elapsed || maximum != elapsed || total != elapsed*float64(wantCount) {
				t.Fatalf("row %d = %q %d %g %g %g %g; want %q count %d elapsed %g",
					row, label, count, average, percentile, maximum, total, fixture.label, wantCount, elapsed)
			}
			row++
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		if row != 7 {
			t.Fatalf("classification rows = %d, want 7", row)
		}
	})

	for _, test := range []struct {
		pipeline string
		want     uint64
	}{
		{`where true() | stats count`, 8},
		{`where false() | stats count`, 0},
		{`where NOT false() AND true() | stats count`, 8},
		{`eval label=case(false(), "never", true(), "first", true(), "later") | where label="first" | stats count`, 8},
		{`eval label=case(missing=1, "never", true(), "fallback") | where label="fallback" | stats count`, 8},
		{`eval label=case(false(), "never") | where isnull(label) | stats count`, 8},
		{`eval label=if(false(), "never", "fallback") | where label="fallback" | stats count`, 8},
		{`eval text=tostring(true()), kind=typeof(false()) | where text="True" AND kind="Boolean" | stats count`, 8},
		{`stats count(eval(false())) AS count`, 0},
		{`stats count(eval(true())) AS count`, 8},
		{`eventstats count(eval(true())) AS all | eventstats count(eval(false())) AS none | where all=8 AND none=0 | stats count`, 8},
		{`streamstats count(eval(true())) AS all | streamstats count(eval(false())) AS none | where all>0 AND none=0 | stats count`, 8},
	} {
		t.Run(test.pipeline, func(t *testing.T) {
			compiled := compile("index=shubuhat | " + test.pipeline)
			var count uint64
			if err := connection.QueryRow(queryContext, compiled.SQL, compiled.Args...).Scan(&count); err != nil {
				t.Fatal(err)
			}
			if count != test.want {
				t.Fatalf("count = %d, want %d", count, test.want)
			}
		})
	}
}
