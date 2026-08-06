package clickhouse

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"

	clickhousedriver "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/chcol"
	"github.com/Suhaibinator/open-splunk/internal/eventfields"
	"github.com/Suhaibinator/open-splunk/internal/plan"
	"github.com/Suhaibinator/open-splunk/internal/testsupport"
)

const defaultStatsExtremaBenchmarkRows = 2_000_000

// BenchmarkStatsExtremaLowering compares the guarded Array lowering retained
// for runtime multivalue fields with the scalar tuple lowering used for
// statically scalar String fields. Both queries are assembled from the
// production SQL helpers so classifier or ordering changes cannot silently
// leave this benchmark measuring an obsolete implementation.
//
// Run a fixed sample rather than relying on the benchmark calibrator:
//
//	OPEN_SPLUNK_CLICKHOUSE_BENCHMARK=1 \
//	  go test ./internal/clickhouse -run '^$' \
//	  -bench '^BenchmarkStatsExtremaLowering$' -benchtime=7x -count=1 -v
//
// OPEN_SPLUNK_STATS_EXTREMA_BENCH_ROWS changes the default two-million-row
// corpus. OPEN_SPLUNK_CLICKHOUSE_TEST_IMAGE deliberately overrides the pinned
// ClickHouse image shared by the integration suite.
func BenchmarkStatsExtremaLowering(b *testing.B) {
	if os.Getenv("OPEN_SPLUNK_CLICKHOUSE_BENCHMARK") != "1" {
		b.Skip("set OPEN_SPLUNK_CLICKHOUSE_BENCHMARK=1 to run the Docker benchmark")
	}
	if _, err := exec.LookPath("docker"); err != nil {
		b.Skipf("docker CLI is unavailable: %v", err)
	}
	rows := statsExtremaBenchmarkRows(b)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	b.Cleanup(cancel)

	container, err := testsupport.StartClickHouse(
		ctx,
		os.Getenv("OPEN_SPLUNK_CLICKHOUSE_TEST_IMAGE"),
	)
	if err != nil {
		b.Fatalf("start ClickHouse benchmark container: %v", err)
	}
	b.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cleanupCancel()
		if err := container.Close(cleanupCtx); err != nil {
			b.Errorf("close ClickHouse benchmark container: %v", err)
		}
	})

	config := DefaultConfig()
	config.Addresses = []string{container.Address}
	config.Database = container.Database
	config.Username = container.Username
	config.Password = container.Password
	options, _, err := config.clickHouseOptions()
	if err != nil {
		b.Fatalf("create ClickHouse benchmark options: %v", err)
	}
	connection, err := clickhousedriver.Open(options)
	if err != nil {
		b.Fatalf("open ClickHouse benchmark connection: %v", err)
	}
	b.Cleanup(func() {
		if err := connection.Close(); err != nil {
			b.Errorf("close ClickHouse benchmark connection: %v", err)
		}
	})
	if err := connection.Ping(ctx); err != nil {
		b.Fatalf("ping ClickHouse benchmark container: %v", err)
	}
	var version string
	if err := connection.QueryRow(ctx, "SELECT version()").Scan(&version); err != nil {
		b.Fatalf("read ClickHouse benchmark version: %v", err)
	}
	b.Logf(
		"image=%s version=%s rows=%d max_threads=1 use_query_cache=0",
		container.Image,
		version,
		rows,
	)

	for _, benchmark := range []struct {
		name string
		sql  string
	}{
		{name: "guarded_array", sql: statsExtremaArrayBenchmarkSQL(rows)},
		{name: "scalar_tuple", sql: statsExtremaScalarBenchmarkSQL(rows)},
	} {
		b.Run(benchmark.name, func(b *testing.B) {
			benchmarkStatsExtremaQuery(b, ctx, connection, benchmark.name, benchmark.sql, rows)
		})
	}
}

func statsExtremaBenchmarkRows(b *testing.B) uint64 {
	b.Helper()
	raw := strings.TrimSpace(os.Getenv("OPEN_SPLUNK_STATS_EXTREMA_BENCH_ROWS"))
	if raw == "" {
		return defaultStatsExtremaBenchmarkRows
	}
	rows, err := strconv.ParseUint(raw, 10, 64)
	if err != nil || rows == 0 || rows > 100_000_000 {
		b.Fatalf(
			"OPEN_SPLUNK_STATS_EXTREMA_BENCH_ROWS must be an integer in [1, 100000000], got %q",
			raw,
		)
	}
	return rows
}

func benchmarkStatsExtremaQuery(
	b *testing.B,
	ctx context.Context,
	connection clickhousedriver.Conn,
	name string,
	query string,
	rows uint64,
) {
	b.Helper()
	prefix := fmt.Sprintf(
		"open-splunk-stats-extrema-%d-%s-",
		time.Now().UnixNano(),
		name,
	)
	execute := func(queryID string) {
		queryCtx := clickhousedriver.Context(
			ctx,
			clickhousedriver.WithQueryID(queryID),
			clickhousedriver.WithSettings(clickhousedriver.Settings{
				"log_queries":     uint64(1),
				"max_threads":     uint64(1),
				"use_query_cache": uint64(0),
			}),
		)
		var low, high chcol.Dynamic
		var lowType, highType uint8
		if err := connection.QueryRow(queryCtx, query).Scan(
			&low,
			&lowType,
			&high,
			&highType,
		); err != nil {
			b.Fatalf("execute %s benchmark query: %v\nSQL: %s", name, err, query)
		}
		if low.Any() != float64(0) ||
			lowType != uint8(eventfields.StoredValueTypeDouble) ||
			high.Any() != "z" ||
			highType != uint8(eventfields.StoredValueTypeString) {
			b.Fatalf(
				"%s benchmark result = %#v/%d %#v/%d, want 0/%d z/%d",
				name,
				low.Any(),
				lowType,
				high.Any(),
				highType,
				eventfields.StoredValueTypeDouble,
				eventfields.StoredValueTypeString,
			)
		}
	}

	execute(prefix + "warmup")
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		execute(prefix + "run-" + strconv.Itoa(iteration))
	}
	b.StopTimer()

	if err := connection.Exec(ctx, "SYSTEM FLUSH LOGS"); err != nil {
		b.Fatalf("flush ClickHouse benchmark query log: %v", err)
	}
	var samples, medianDurationMS, peakMemory uint64
	var averageMemory, averageReadRows float64
	if err := connection.QueryRow(
		ctx,
		`SELECT
			count(),
			quantileExact(0.5)(query_duration_ms),
			avg(memory_usage),
			max(memory_usage),
			avg(read_rows)
		FROM system.query_log
		WHERE type = 'QueryFinish' AND startsWith(query_id, ?)`,
		prefix+"run-",
	).Scan(
		&samples,
		&medianDurationMS,
		&averageMemory,
		&peakMemory,
		&averageReadRows,
	); err != nil {
		b.Fatalf("read %s benchmark query log: %v", name, err)
	}
	if samples != uint64(b.N) {
		b.Fatalf("%s benchmark query-log samples = %d, want %d", name, samples, b.N)
	}
	b.ReportMetric(float64(medianDurationMS), "server_ms/op")
	b.ReportMetric(averageMemory, "avg_B/op")
	b.ReportMetric(float64(peakMemory), "peak_B/op")
	b.ReportMetric(averageReadRows, "read_rows/op")
	if medianDurationMS != 0 {
		b.ReportMetric(
			float64(rows)/(float64(medianDurationMS)/1_000),
			"rows/s",
		)
	}
}

func statsExtremaBenchmarkCorpusSQL(rows uint64) string {
	return fmt.Sprintf(
		`SELECT if(
			number %% 5 = 0,
			CAST(NULL AS Nullable(String)),
			arrayElement(['10', 'z', '-0', '1e3'], toUInt8(number %% 4) + 1)
		) AS value
		FROM numbers(%d)`,
		rows,
	)
}

func statsExtremaScalarBenchmarkSQL(rows uint64) string {
	number := statsExtremaScalarNumberSQL("value")
	candidate := statsExtremaScalarCandidateSQL("value", "parsed_number", "0")
	minimum := statsExtremaScalarAggregateWinnerSQL(plan.AggregateFunctionMinimum, "candidate")
	maximum := statsExtremaScalarAggregateWinnerSQL(plan.AggregateFunctionMaximum, "candidate")
	return `SELECT
		` + statsExtremaScalarValueSQL("minimum_key") + `,
		` + statsExtremaScalarStoredTypeSQL("minimum_key") + `,
		` + statsExtremaScalarValueSQL("maximum_key") + `,
		` + statsExtremaScalarStoredTypeSQL("maximum_key") + `
	FROM (
		SELECT
			` + minimum + ` AS minimum_key,
			` + maximum + ` AS maximum_key
		FROM (
			SELECT ` + candidate + ` AS candidate
			FROM (
				SELECT value, ` + number + ` AS parsed_number
				FROM (` + statsExtremaBenchmarkCorpusSQL(rows) + `)
			)
		)
	)`
}

func statsExtremaArrayBenchmarkSQL(rows uint64) string {
	values := compactNullableArraySQL("[value]")
	candidates := statsExtremaCandidatesSQL("values")
	minimum := statsExtremaAggregateSQL(plan.AggregateFunctionMinimum, "candidates")
	maximum := statsExtremaAggregateSQL(plan.AggregateFunctionMaximum, "candidates")
	return `SELECT
		minimum_value,
		` + statsExtremaStoredTypeSQL("minimum_value") + `,
		maximum_value,
		` + statsExtremaStoredTypeSQL("maximum_value") + `
	FROM (
		SELECT
			` + minimum + ` AS minimum_value,
			` + maximum + ` AS maximum_value
		FROM (
			SELECT ` + candidates + ` AS candidates
			FROM (
				SELECT ` + values + ` AS values
				FROM (` + statsExtremaBenchmarkCorpusSQL(rows) + `)
			)
		)
	)`
}
