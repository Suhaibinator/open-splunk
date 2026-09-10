package clickhouse

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"testing"
	"time"

	clickhousedriver "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/Suhaibinator/open-splunk/internal/testsupport"
)

// TestEventResultLimitAgainstClickHouse compares complete canonical results
// with the bounded execution prefix, including authored order/limit barriers
// and the private tie-breakers for otherwise identical events.
func TestEventResultLimitAgainstClickHouse(t *testing.T) {
	if os.Getenv("OPEN_SPLUNK_CLICKHOUSE_INTEGRATION") != "1" {
		t.Skip("set OPEN_SPLUNK_CLICKHOUSE_INTEGRATION=1 to run the Docker integration test")
	}
	ctx, connection := eventResultLimitDatabaseFixture(t, 12_050)
	for _, source := range []string{
		`index=gradethis | table event_id host`,
		`index=gradethis | sort 0 host | table event_id host`,
		`index=gradethis | sort 0 severity | fields - severity | tail 41 | table event_id host`,
		`index=gradethis | head 71 | search level=error | table event_id host`,
		`index=gradethis | tail 71 | search level=error | table event_id host`,
		`index=gradethis level=missing | table event_id host`,
	} {
		t.Run(source, func(t *testing.T) {
			canonical := eventResultLimitFixtureCompile(t, source, 12_050)
			unlimited := readEventResultLimitFixture(t, ctx, connection, canonical.SQL, canonical.Args, 0)
			if source == `index=gradethis | table event_id host` && len(unlimited) != 12_050 {
				t.Fatalf("canonical fixture returned %d rows, want 12050", len(unlimited))
			}
			for _, ceiling := range []uint64{1, 17, 10_001, 12_050, 12_051} {
				limited, eligible, err := CompileEventResultLimitContext(ctx, canonical, ceiling)
				if err != nil || !eligible {
					t.Fatalf("derive limit %d: eligible %t, err %v", ceiling, eligible, err)
				}
				actual := readEventResultLimitFixture(t, ctx, connection, limited.SQL, limited.Args, 0)
				want := unlimited[:min(uint64(len(unlimited)), ceiling)]
				if !slices.Equal(actual, want) {
					t.Fatalf("limit %d changed ordered rows: got %d, want %d", ceiling, len(actual), len(want))
				}
			}
			// Reusing the retained source after bounded searches must still return
			// every event, including export rows beyond the retained search cap.
			after := readEventResultLimitFixture(t, ctx, connection, canonical.SQL, canonical.Args, 0)
			if !slices.Equal(after, unlimited) {
				t.Fatal("bounded execution changed canonical reexecution")
			}
		})
	}
}

// BenchmarkEventResultLimit measures the same first 10,001 ordered events with
// production-style result settings, comparing the unlimited canonical SQL with
// its compiler-owned LIMIT specialization. The default fixture has 1,048,576
// events; set OPEN_SPLUNK_EVENT_RESULT_BENCH_ROWS to change its size.
//
//	OPEN_SPLUNK_CLICKHOUSE_BENCHMARK=1 go test ./internal/clickhouse -run '^$' \
//	  -bench '^BenchmarkEventResultLimit$' -benchtime=7x -count=3 -v
func BenchmarkEventResultLimit(b *testing.B) {
	if os.Getenv("OPEN_SPLUNK_CLICKHOUSE_BENCHMARK") != "1" {
		b.Skip("set OPEN_SPLUNK_CLICKHOUSE_BENCHMARK=1 to run the Docker benchmark")
	}
	fixtureRows := uint64(1 << 20)
	if value := os.Getenv("OPEN_SPLUNK_EVENT_RESULT_BENCH_ROWS"); value != "" {
		parsed, err := strconv.ParseUint(value, 10, 64)
		if err != nil || parsed < 10_001 || parsed > 10_000_000 {
			b.Fatal("OPEN_SPLUNK_EVENT_RESULT_BENCH_ROWS must be between 10001 and 10000000")
		}
		fixtureRows = parsed
	}
	ctx, connection := eventResultLimitDatabaseFixture(b, fixtureRows)
	const ceiling = uint64(10_001)
	for _, workload := range []struct {
		name   string
		source string
	}{
		{name: "all_events", source: `index=gradethis | table event_id host`},
		{name: "filtered_events", source: `index=gradethis level=error | table event_id host`},
	} {
		canonical := eventResultLimitFixtureCompile(b, workload.source, fixtureRows)
		limited, eligible, err := CompileEventResultLimitContext(ctx, canonical, ceiling)
		if err != nil || !eligible {
			b.Fatalf("derive benchmark limit: eligible %t, err %v", eligible, err)
		}
		queryContext := clickhousedriver.Context(ctx, clickhousedriver.WithSettings(clickhousedriver.Settings{
			"max_threads": uint64(1), "use_query_cache": uint64(0),
			"max_result_rows": ceiling, "result_overflow_mode": "break",
			"max_result_bytes": uint64(128 << 20), "log_queries": uint64(1),
		}))
		want := readEventResultLimitFixture(b, queryContext, connection, canonical.SQL, canonical.Args, ceiling)
		wantRows := min(ceiling, fixtureRows)
		if workload.name == "filtered_events" {
			wantRows = min(ceiling, (fixtureRows+7)/8)
		}
		if uint64(len(want)) != wantRows {
			b.Fatalf("benchmark fixture returned %d rows, want %d", len(want), wantRows)
		}
		actual := readEventResultLimitFixture(b, queryContext, connection, limited.SQL, limited.Args, ceiling)
		if !slices.Equal(want, actual) {
			b.Fatal("benchmark specialization changed ordered output")
		}
		for _, variant := range []struct{ name, sql string }{
			{name: "canonical", sql: canonical.SQL},
			{name: "bounded", sql: limited.SQL},
		} {
			b.Run(workload.name+"/"+variant.name, func(b *testing.B) {
				prefix := fmt.Sprintf("event-result-limit-%d-", time.Now().UnixNano())
				b.ResetTimer()
				for iteration := range b.N {
					iterationContext := clickhousedriver.Context(queryContext, clickhousedriver.WithQueryID(prefix+strconv.Itoa(iteration)))
					got := readEventResultLimitFixture(b, iterationContext, connection, variant.sql, canonical.Args, ceiling)
					if !slices.Equal(got, want) {
						b.Fatal("timed result changed ordered output")
					}
				}
				b.StopTimer()
				if err := connection.Exec(ctx, "SYSTEM FLUSH LOGS"); err != nil {
					b.Fatalf("flush benchmark query log: %v", err)
				}
				var samples, medianMS, peakMemory uint64
				var readRows, readBytes float64
				if err := connection.QueryRow(ctx, `SELECT count(), quantileExact(0.5)(query_duration_ms), max(memory_usage), avg(read_rows), avg(read_bytes)
					FROM system.query_log WHERE type = 'QueryFinish' AND startsWith(query_id, ?)`, prefix).
					Scan(&samples, &medianMS, &peakMemory, &readRows, &readBytes); err != nil {
					b.Fatalf("read benchmark evidence: %v", err)
				}
				if samples != uint64(b.N) {
					b.Fatalf("completed benchmark samples = %d, want %d", samples, b.N)
				}
				b.Logf("samples=%d median_ms=%d peak_memory_bytes=%d average_read_rows=%.0f average_read_bytes=%.0f", samples, medianMS, peakMemory, readRows, readBytes)
				b.ReportMetric(float64(medianMS), "server-ms")
				b.ReportMetric(float64(peakMemory), "peak-memory-B")
				b.ReportMetric(readRows, "read-rows")
			})
		}
	}
}

type eventResultLimitFixtureRow struct{ id, host string }

func readEventResultLimitFixture(
	t testing.TB, ctx context.Context, connection clickhousedriver.Conn,
	sql string, args []any, ceiling uint64,
) []eventResultLimitFixtureRow {
	t.Helper()
	rows, err := connection.Query(ctx, sql, args...)
	if err != nil {
		t.Fatalf("query event result fixture: %v", err)
	}
	var result []eventResultLimitFixtureRow
	for rows.Next() {
		var row eventResultLimitFixtureRow
		if err := rows.Scan(&row.id, &row.host); err != nil {
			_ = rows.Close()
			t.Fatalf("scan event result fixture: %v", err)
		}
		result = append(result, row)
		if ceiling != 0 && uint64(len(result)) >= ceiling {
			break
		}
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		t.Fatalf("iterate event result fixture: %v", err)
	}
	if err := rows.Close(); err != nil {
		t.Fatalf("close event result fixture: %v", err)
	}
	return result
}

func eventResultLimitFixtureCompile(t testing.TB, source string, rows uint64) CompiledQuery {
	t.Helper()
	scope := testChartScope()
	scope.VisibilityCutoff = new(rows + 1)
	return compileSPLWithScope(t, source, scope)
}

func eventResultLimitDatabaseFixture(t testing.TB, rows uint64) (context.Context, clickhousedriver.Conn) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	t.Cleanup(cancel)
	container, err := testsupport.StartClickHouse(ctx, os.Getenv("OPEN_SPLUNK_CLICKHOUSE_TEST_IMAGE"))
	if err != nil {
		t.Fatalf("start event result fixture: %v", err)
	}
	t.Cleanup(func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		if err := container.Close(cleanup); err != nil {
			t.Errorf("close event result container: %v", err)
		}
	})
	paths, err := filepath.Glob(filepath.Join("..", "..", "migrations", "clickhouse", "[0-9][0-9][0-9][0-9]_*.sql"))
	if err != nil || len(paths) == 0 {
		t.Fatalf("discover event result schema: %v", err)
	}
	var ddl bytes.Buffer
	for _, path := range paths {
		content, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read event result schema: %v", err)
		}
		ddl.Write(content)
		ddl.WriteByte('\n')
	}
	command := exec.CommandContext(ctx, "docker", "exec", "--interactive", container.Name,
		"clickhouse-client", "--user", container.Username, "--password", container.Password, "--multiquery")
	command.Stdin = &ddl
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("apply event result schema: %v: %s", err, output)
	}
	config := DefaultConfig()
	config.Addresses = []string{container.Address}
	config.Username, config.Password = container.Username, container.Password
	options, _, err := config.clickHouseOptions()
	if err != nil {
		t.Fatalf("configure event result fixture: %v", err)
	}
	connection, err := clickhousedriver.Open(options)
	if err != nil {
		t.Fatalf("open event result fixture: %v", err)
	}
	t.Cleanup(func() { _ = connection.Close() })
	const insert = `INSERT INTO open_splunk.events
		(event_id, tenant_id, index_name, event_time, index_time, host, source, sourcetype, level, severity, raw, raw_encoding,
		 collector_id, ingest_source_kind, ingest_source_id, batch_id, batch_sequence, visibility_seq, field_metadata_version, expires_at)
		SELECT concat('event-', leftPad(toString(intDiv(number, 4)), 12, '0')), 'tenant-1', 'gradethis',
		 fromUnixTimestamp64Nano(toInt64(1784592000000000000 + intDiv(number, 16) * 1000), 'UTC'),
		 toDateTime64('2026-07-21 01:00:00', 3, 'UTC'), concat('host-', toString(number % 32)), 'benchmark', 'benchmark',
		 if(number % 8 = 0, 'error', 'info'), toUInt8(number % 8), repeat('payload ', 64), toUInt8(1),
		 'benchmark', toUInt8(1), 'benchmark', 'batch', number, intDiv(number, 2) + 1, toUInt8(1),
		 toDateTime64('2099-01-01 00:00:00', 3, 'UTC') FROM numbers(?)`
	if err := connection.Exec(ctx, insert, rows); err != nil {
		t.Fatalf("populate event result fixture: %v", err)
	}
	if err := connection.Exec(ctx, "OPTIMIZE TABLE open_splunk.events FINAL"); err != nil {
		t.Fatalf("merge event result fixture: %v", err)
	}
	var version string
	if err := connection.QueryRow(ctx, "SELECT version()").Scan(&version); err != nil {
		t.Fatalf("read fixture server version: %v", err)
	}
	t.Logf("image=%s clickhouse=%s fixture_rows=%d parts=1 max_threads=1", container.Image, version, rows)
	return ctx, connection
}
