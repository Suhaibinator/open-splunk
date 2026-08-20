package queryexec

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	clickhousedriver "github.com/ClickHouse/clickhouse-go/v2"
	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/indexread"
	"github.com/Suhaibinator/open-splunk/internal/ingest"
	"github.com/Suhaibinator/open-splunk/internal/plan"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
	"github.com/Suhaibinator/open-splunk/internal/spl"
	"github.com/Suhaibinator/open-splunk/internal/testsupport"
	"github.com/Suhaibinator/open-splunk/internal/visibility"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	defaultAuthoredExpressionBenchmarkRows = uint64(100_000)
	authoredExpressionBenchmarkIndex       = "expression-v02-benchmark"
	authoredExpressionBenchmarkSource      = "expression-v02-benchmark"
	authoredExpressionBenchmarkTenant      = "tenant"
	authoredExpressionBenchmarkBatchRows   = uint64(1_000)
)

// BenchmarkAuthoredExpressionExecution is the opt-in production-shaped execution
// baseline for authored-expression behavior. The fixture is admitted through
// the production Store and read through the parser, planner, compiler, and
// resource-bounded Executor. Each production case is paired with parameterized
// hand-authored ClickHouse SQL over the same immutable rows. The controls are
// fixture-equivalent, not alternate compatibility implementations: in
// particular the Dynamic control knows that this corpus contains only Int64.
//
// Run fixed samples so benchmark variance is meaningful and reproducible:
//
//	OPEN_SPLUNK_CLICKHOUSE_BENCHMARK=1 \
//	  go test ./internal/queryexec -run '^$' \
//	  -bench '^BenchmarkAuthoredExpressionExecution$' \
//	  -benchtime=7x -count=3 -benchmem -v
//
// OPEN_SPLUNK_AUTHORED_EXPRESSION_BENCH_ROWS changes the default 100,000-row
// corpus, within [32, 2,000,000]. OPEN_SPLUNK_CLICKHOUSE_TEST_IMAGE may
// override the repository image only with another canonical sha256 digest.
// Setup and one verified warmup per case are excluded from timed iterations.
func BenchmarkAuthoredExpressionExecution(b *testing.B) {
	if os.Getenv("OPEN_SPLUNK_CLICKHOUSE_BENCHMARK") != "1" {
		b.Skip("set OPEN_SPLUNK_CLICKHOUSE_BENCHMARK=1 to run the Docker benchmark")
	}
	if _, err := exec.LookPath("docker"); err != nil {
		b.Skipf("docker CLI is unavailable: %v", err)
	}
	rows := authoredExpressionBenchmarkRows(b)
	image, err := testsupport.ResolvePinnedClickHouseImage(
		os.Getenv("OPEN_SPLUNK_CLICKHOUSE_TEST_IMAGE"),
	)
	if err != nil {
		b.Fatalf("resolve pinned ClickHouse image: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	b.Cleanup(cancel)
	container, err := testsupport.StartClickHouse(ctx, image)
	if err != nil {
		b.Fatalf("start ClickHouse authored expression benchmark container: %v", err)
	}
	b.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cleanupCancel()
		if closeErr := container.Close(cleanupCtx); closeErr != nil {
			b.Errorf("close ClickHouse authored expression benchmark container: %v", closeErr)
		}
	})
	authoredExpressionBenchmarkMigrate(b, ctx, container.Name, container.Password)

	options := &clickhousedriver.Options{
		Addr: []string{container.Address},
		Auth: clickhousedriver.Auth{
			Database: container.Database,
			Username: container.Username,
			Password: container.Password,
		},
		DialTimeout: 5 * time.Second,
	}
	queryConnection, err := clickhousedriver.Open(options)
	if err != nil {
		b.Fatalf("open ClickHouse authored expression benchmark query connection: %v", err)
	}
	b.Cleanup(func() {
		if closeErr := queryConnection.Close(); closeErr != nil {
			b.Errorf("close ClickHouse authored expression benchmark query connection: %v", closeErr)
		}
	})
	if err := queryConnection.Ping(ctx); err != nil {
		b.Fatalf("ping ClickHouse authored expression benchmark connection: %v", err)
	}
	var clickHouseVersion string
	if err := queryConnection.QueryRow(ctx, "SELECT version()").Scan(&clickHouseVersion); err != nil {
		b.Fatalf("read ClickHouse authored expression benchmark version: %v", err)
	}

	executor, err := New(queryConnection, Config{
		MaxThreads:    1,
		ReadAdmission: indexread.UnfencedAdmission{},
	})
	if err != nil {
		b.Fatalf("create authored expression benchmark executor: %v", err)
	}
	storeConnection, err := clickhousedriver.Open(options)
	if err != nil {
		b.Fatalf("open ClickHouse authored expression benchmark store connection: %v", err)
	}
	controlDB, err := control.Open(ctx, filepath.Join(b.TempDir(), "visibility.sqlite"))
	if err != nil {
		_ = storeConnection.Close()
		b.Fatalf("open authored expression benchmark visibility database: %v", err)
	}
	b.Cleanup(func() {
		if closeErr := controlDB.Close(); closeErr != nil {
			b.Errorf("close authored expression benchmark visibility database: %v", closeErr)
		}
	})
	sequencer, err := visibility.NewSQLite(ctx, controlDB)
	if err != nil {
		_ = storeConnection.Close()
		b.Fatalf("create authored expression benchmark visibility sequencer: %v", err)
	}
	b.Cleanup(func() {
		if closeErr := sequencer.Close(); closeErr != nil {
			b.Errorf("close authored expression benchmark visibility sequencer: %v", closeErr)
		}
	})
	store, err := clickhouse.NewStore(
		storeConnection,
		clickhouse.RetentionProviderFunc(func(context.Context, string, string) (time.Duration, error) {
			return 24 * time.Hour, nil
		}),
		sequencer,
	)
	if err != nil {
		_ = storeConnection.Close()
		b.Fatalf("create authored expression benchmark store: %v", err)
	}
	b.Cleanup(func() {
		if closeErr := store.Close(); closeErr != nil {
			b.Errorf("close authored expression benchmark store: %v", closeErr)
		}
	})

	// The production events table enforces expires_at with a physical TTL. The
	// benchmark's data and assertions are all relative to one captured clock,
	// so keep that clock current instead of letting a historical fixture turn
	// into a benchmark of an empty table.
	indexTime := time.Now().UTC().Truncate(time.Second)
	wantDynamicSum := authoredExpressionStoreBenchmarkFixture(b, ctx, store, indexTime, rows)
	visibilityCutoff, err := store.VisibilityCutoff(ctx)
	if err != nil {
		b.Fatalf("resolve authored expression benchmark visibility cutoff: %v", err)
	}
	wantBatches := (rows + authoredExpressionBenchmarkBatchRows - 1) / authoredExpressionBenchmarkBatchRows
	if visibilityCutoff != wantBatches {
		b.Fatalf("authored expression benchmark visibility cutoff = %d, want %d", visibilityCutoff, wantBatches)
	}
	// Store deliberately creates one immutable visibility sequence per bounded
	// ingestion batch. Compact those setup parts before timing so asynchronous
	// background merges do not become uncontrolled benchmark variance.
	if err := queryConnection.Exec(ctx, `OPTIMIZE TABLE "open_splunk"."events" FINAL`); err != nil {
		b.Fatalf("compact authored expression benchmark fixture: %v", err)
	}
	var activeParts uint64
	if err := queryConnection.QueryRow(
		ctx,
		`SELECT count() FROM system.parts WHERE active AND database = ? AND table = ?`,
		"open_splunk",
		"events",
	).Scan(&activeParts); err != nil {
		b.Fatalf("count authored expression benchmark parts: %v", err)
	}
	if activeParts != 1 {
		b.Fatalf("authored expression benchmark active parts = %d, want 1", activeParts)
	}
	var storedRows uint64
	if err := queryConnection.QueryRow(
		ctx,
		`SELECT count() FROM "open_splunk"."events" WHERE tenant_id = ? AND index_name = ?`,
		authoredExpressionBenchmarkTenant,
		authoredExpressionBenchmarkIndex,
	).Scan(&storedRows); err != nil {
		b.Fatalf("count authored expression benchmark fixture: %v", err)
	}
	if storedRows != rows {
		b.Fatalf("authored expression benchmark stored rows = %d, want %d", storedRows, rows)
	}

	wantFixedSum := float64(rows) * (float64(opensplunk.LogSeverity_LOG_SEVERITY_INFO) + 1)
	wantLowMembership := float64((rows/8)*2 + min(rows%8, 2))
	highCandidates := authoredExpressionBenchmarkHighCandidates(rows)
	wantHighMembership := float64(len(highCandidates))
	commonArgs := []any{
		authoredExpressionBenchmarkTenant,
		authoredExpressionBenchmarkIndex,
		authoredExpressionBenchmarkSource,
		indexTime.Add(-time.Hour),
		indexTime.Add(time.Hour),
		indexTime.Add(time.Minute),
		visibilityCutoff,
		indexTime.Add(time.Minute),
	}
	commonPredicate := ` FROM "open_splunk"."events"
		WHERE tenant_id = ? AND index_name = ? AND source = ?
			AND event_time >= ? AND event_time < ? AND index_time <= ?
			AND visibility_seq <= ? AND expires_at > ?`

	workloads := []authoredExpressionExecutionWorkload{
		{
			name: "fixed_arithmetic",
			source: `index=` + authoredExpressionBenchmarkIndex + ` source="` + authoredExpressionBenchmarkSource + `"` +
				` | eval benchmark_value=severity+1 | stats sum(benchmark_value) AS result`,
			want:        wantFixedSum,
			baselineSQL: `SELECT toFloat64(sum(toFloat64(severity) + toFloat64(?)))` + commonPredicate,
			baselineArgs: append(
				[]any{float64(1)},
				commonArgs...,
			),
		},
		{
			name: "dynamic_arithmetic",
			source: `index=` + authoredExpressionBenchmarkIndex + ` source="` + authoredExpressionBenchmarkSource + `"` +
				` | eval benchmark_value=metric*1.5+2 | stats sum(benchmark_value) AS result`,
			want: wantDynamicSum,
			baselineSQL: `SELECT toFloat64(sum(
				toFloat64(dynamicElement("fields"."metric", 'Int64')) * toFloat64(?) + toFloat64(?)
			))` + commonPredicate,
			baselineArgs: append(
				[]any{float64(1.5), float64(2)},
				commonArgs...,
			),
		},
		{
			name: "membership_low_cardinality",
			source: `index=` + authoredExpressionBenchmarkIndex + ` source="` + authoredExpressionBenchmarkSource + `"` +
				` | where host IN ("bench-host-0", "bench-host-1") | stats count AS result`,
			want:        wantLowMembership,
			baselineSQL: `SELECT toFloat64(count())` + commonPredicate + ` AND host IN (?, ?)`,
			baselineArgs: append(
				slices.Clone(commonArgs),
				"bench-host-0",
				"bench-host-1",
			),
		},
		{
			name: "membership_high_cardinality",
			source: `index=` + authoredExpressionBenchmarkIndex + ` source="` + authoredExpressionBenchmarkSource + `"` +
				` | where event_id IN (` + authoredExpressionBenchmarkQuotedCandidates(highCandidates) + `)` +
				` | stats count AS result`,
			want: wantHighMembership,
			baselineSQL: `SELECT toFloat64(count())` + commonPredicate +
				` AND event_id IN (` + strings.TrimSuffix(strings.Repeat("?,", len(highCandidates)), ",") + `)`,
			baselineArgs: append(
				slices.Clone(commonArgs),
				authoredExpressionBenchmarkAnyStrings(highCandidates)...,
			),
		},
	}

	b.Logf(
		"image=%s clickhouse=%s rows=%d batches=%d parts=1 warmups=1 max_threads=1 go=%s os=%s arch=%s cpus=%d",
		container.Image,
		clickHouseVersion,
		rows,
		wantBatches,
		runtime.Version(),
		runtime.GOOS,
		runtime.GOARCH,
		runtime.NumCPU(),
	)
	for _, workload := range workloads {
		compiled := authoredExpressionBenchmarkCompile(
			b,
			workload.source,
			indexTime,
			visibilityCutoff,
		)
		b.Logf("workload=%s generated_sql_bytes=%d", workload.name, len(compiled.SQL))
		b.Run(workload.name+"/production_executor", func(b *testing.B) {
			authoredExpressionBenchmarkProduction(b, ctx, executor, compiled, workload.want, rows)
		})
		b.Run(workload.name+"/hand_sql_driver", func(b *testing.B) {
			authoredExpressionBenchmarkHandSQL(
				b,
				ctx,
				queryConnection,
				workload.baselineSQL,
				workload.baselineArgs,
				workload.want,
				rows,
			)
		})
	}
}

type authoredExpressionExecutionWorkload struct {
	name         string
	source       string
	want         float64
	baselineSQL  string
	baselineArgs []any
}

func authoredExpressionBenchmarkRows(b *testing.B) uint64 {
	b.Helper()
	raw := strings.TrimSpace(os.Getenv("OPEN_SPLUNK_AUTHORED_EXPRESSION_BENCH_ROWS"))
	if raw == "" {
		return defaultAuthoredExpressionBenchmarkRows
	}
	rows, err := strconv.ParseUint(raw, 10, 64)
	if err != nil || rows < 32 || rows > 2_000_000 {
		b.Fatalf(
			"OPEN_SPLUNK_AUTHORED_EXPRESSION_BENCH_ROWS must be an integer in [32, 2000000], got %q",
			raw,
		)
	}
	return rows
}

func authoredExpressionBenchmarkMigrate(
	b *testing.B,
	ctx context.Context,
	container string,
	password string,
) {
	b.Helper()
	paths, err := filepath.Glob(filepath.Join("..", "..", "migrations", "clickhouse", "[0-9][0-9][0-9][0-9]_*.sql"))
	if err != nil || len(paths) == 0 {
		b.Fatalf("discover authored expression benchmark migrations: paths=%v err=%v", paths, err)
	}
	var combined bytes.Buffer
	for _, path := range paths {
		migration, readErr := os.ReadFile(path)
		if readErr != nil {
			b.Fatalf("read authored expression benchmark migration: %v", readErr)
		}
		combined.Write(migration)
		combined.WriteByte('\n')
	}
	command := exec.CommandContext(
		ctx,
		"docker",
		"exec",
		"--interactive",
		container,
		"clickhouse-client",
		"--user",
		"open_splunk",
		"--password",
		password,
		"--multiquery",
	)
	command.Stdin = bytes.NewReader(combined.Bytes())
	if output, err := command.CombinedOutput(); err != nil {
		b.Fatalf("apply authored expression benchmark migrations: %v: %s", err, output)
	}
}

func authoredExpressionStoreBenchmarkFixture(
	b *testing.B,
	ctx context.Context,
	store *clickhouse.Store,
	indexTime time.Time,
	rows uint64,
) float64 {
	b.Helper()
	var dynamicSum float64
	for first := uint64(0); first < rows; first += authoredExpressionBenchmarkBatchRows {
		last := min(first+authoredExpressionBenchmarkBatchRows, rows)
		batchNumber := first/authoredExpressionBenchmarkBatchRows + 1
		batchID := fmt.Sprintf("expression-v02-benchmark-%08d", batchNumber)
		events := make([]*ingest.StoredEvent, 0, last-first)
		for row := first; row < last; row++ {
			metric := int64(row % 1_000)
			dynamicSum += float64(metric)*1.5 + 2
			events = append(events, authoredExpressionBenchmarkEvent(indexTime, batchID, row, metric))
		}
		digest := sha256.Sum256([]byte(batchID))
		result, err := store.Store(ctx, ingest.StoreBatch{
			TenantID:           authoredExpressionBenchmarkTenant,
			CollectorID:        "expression-v02-benchmark-collector",
			BatchID:            batchID,
			BatchSequence:      batchNumber,
			OriginalEventCount: uint32(len(events)), // #nosec G115 -- each batch is bounded at 1,000.
			SourceBatchSHA256:  digest,
			ReceivedAt:         indexTime,
			Events:             events,
			RetentionByIndex:   map[string]time.Duration{authoredExpressionBenchmarkIndex: 24 * time.Hour},
		})
		if err != nil {
			b.Fatalf("store authored expression benchmark batch %d: %v", batchNumber, err)
		}
		if result.Accepted != uint32(len(events)) || result.Duplicate != 0 {
			b.Fatalf("authored expression benchmark batch %d result = %#v", batchNumber, result)
		}
	}
	return dynamicSum
}

func authoredExpressionBenchmarkEvent(
	indexTime time.Time,
	batchID string,
	row uint64,
	metric int64,
) *ingest.StoredEvent {
	eventTime := indexTime.Add(-time.Minute).Add(time.Duration(row%60_000) * time.Microsecond)
	message := "expression v0.2 production-shaped benchmark event"
	service := fmt.Sprintf("benchmark-service-%02d", row%32)
	return &ingest.StoredEvent{
		TenantID:    authoredExpressionBenchmarkTenant,
		CollectorID: "expression-v02-benchmark-collector",
		BatchID:     batchID,
		IndexTime:   indexTime,
		Event: &opensplunk.LogEvent{
			EventId:         fmt.Sprintf("bench-%08d", row),
			IndexName:       authoredExpressionBenchmarkIndex,
			EventTime:       timestamppb.New(eventTime),
			CollectedAt:     timestamppb.New(eventTime.Add(time.Second)),
			EventTimeSource: opensplunk.EventTimeSource_EVENT_TIME_SOURCE_PARSED,
			Host:            fmt.Sprintf("bench-host-%d", row%8),
			Source:          authoredExpressionBenchmarkSource,
			Sourcetype:      "open-splunk:expression-benchmark",
			Service:         &service,
			Severity:        opensplunk.LogSeverity_LOG_SEVERITY_INFO,
			Message:         &message,
			Raw:             []byte(message),
			RawEncoding:     opensplunk.RawEncoding_RAW_ENCODING_UTF8,
			Fields: &opensplunk.TypedObject{Fields: []*opensplunk.TypedObjectField{
				{
					Name: "metric",
					Value: &opensplunk.TypedValue{Kind: &opensplunk.TypedValue_Sint64Value{
						Sint64Value: metric,
					}},
				},
			}},
		},
	}
}

func authoredExpressionBenchmarkCompile(
	b *testing.B,
	source string,
	indexTime time.Time,
	visibilityCutoff uint64,
) clickhouse.CompiledQuery {
	b.Helper()
	parsed, err := spl.Parse(source)
	if err != nil {
		b.Fatalf("parse authored expression benchmark SPL %q: %v", source, err)
	}
	logical, err := plan.Build(parsed, plan.Scope{
		TenantID:          authoredExpressionBenchmarkTenant,
		AuthorizedIndexes: []string{authoredExpressionBenchmarkIndex},
		RequestedIndexes:  []string{authoredExpressionBenchmarkIndex},
		Earliest:          indexTime.Add(-time.Hour),
		Latest:            indexTime.Add(time.Hour),
		SearchStart:       indexTime.Add(time.Minute),
		SearchTimezone:    "UTC",
		IndexTimeCutoff:   indexTime.Add(time.Minute),
		VisibilityCutoff:  &visibilityCutoff,
	})
	if err != nil {
		b.Fatalf("build authored expression benchmark SPL %q: %v", source, err)
	}
	compiled, err := (clickhouse.Compiler{}).Compile(logical)
	if err != nil {
		b.Fatalf("compile authored expression benchmark SPL %q: %v", source, err)
	}
	if len(compiled.OutputFields) != 1 || compiled.OutputFields[0] != "result" {
		b.Fatalf("authored expression benchmark output fields = %#v, want [result]", compiled.OutputFields)
	}
	return compiled
}

type authoredExpressionBenchmarkSink struct {
	schemaCalls int
	rows        [][]searchjobs.Value
}

func (sink *authoredExpressionBenchmarkSink) SetSchema(schema searchjobs.Schema) error {
	if sink.schemaCalls != 0 || len(schema.Columns) != 1 || schema.Columns[0].Name != "result" {
		return fmt.Errorf("unexpected authored expression benchmark schema: %#v", schema)
	}
	sink.schemaCalls++
	return nil
}

func (sink *authoredExpressionBenchmarkSink) AddRow(values []searchjobs.Value) error {
	if sink.schemaCalls != 1 || len(sink.rows) != 0 || len(values) != 1 {
		return fmt.Errorf("unexpected authored expression benchmark result row")
	}
	sink.rows = append(sink.rows, slices.Clone(values))
	return nil
}

func (sink *authoredExpressionBenchmarkSink) result() (float64, error) {
	if sink.schemaCalls != 1 || len(sink.rows) != 1 || len(sink.rows[0]) != 1 {
		return 0, fmt.Errorf(
			"authored expression benchmark publication = schema calls %d rows %d",
			sink.schemaCalls,
			len(sink.rows),
		)
	}
	value := sink.rows[0][0]
	if result, ok := value.Double(); ok {
		return result, nil
	}
	if result, ok := value.Unsigned(); ok {
		return float64(result), nil
	}
	if result, ok := value.Signed(); ok {
		return float64(result), nil
	}
	if result, ok := value.Decimal(); ok {
		parsed, err := strconv.ParseFloat(result, 64)
		if err == nil {
			return parsed, nil
		}
	}
	return 0, fmt.Errorf("authored expression benchmark result has unexpected kind %v", value.Kind())
}

func authoredExpressionBenchmarkProduction(
	b *testing.B,
	ctx context.Context,
	executor *Executor,
	compiled clickhouse.CompiledQuery,
	want float64,
	rows uint64,
) {
	b.Helper()
	execute := func() error {
		sink := &authoredExpressionBenchmarkSink{}
		if err := executor.Execute(ctx, compiled, sink); err != nil {
			return err
		}
		got, err := sink.result()
		if err != nil {
			return err
		}
		return authoredExpressionBenchmarkResultError(got, want)
	}
	authoredExpressionRunTimedBenchmark(b, rows, execute)
}

func authoredExpressionBenchmarkHandSQL(
	b *testing.B,
	ctx context.Context,
	connection clickhousedriver.Conn,
	query string,
	args []any,
	want float64,
	rows uint64,
) {
	b.Helper()
	queryCtx := clickhousedriver.Context(
		ctx,
		clickhousedriver.WithSettings(clickhousedriver.Settings{
			"max_threads":     uint64(1),
			"use_query_cache": uint64(0),
		}),
	)
	execute := func() error {
		var got float64
		if err := connection.QueryRow(queryCtx, query, args...).Scan(&got); err != nil {
			return err
		}
		return authoredExpressionBenchmarkResultError(got, want)
	}
	authoredExpressionRunTimedBenchmark(b, rows, execute)
}

func authoredExpressionRunTimedBenchmark(
	b *testing.B,
	rows uint64,
	execute func() error,
) {
	b.Helper()
	b.ReportAllocs()
	b.StopTimer()
	if err := execute(); err != nil {
		b.Fatalf("authored expression benchmark warmup: %v", err)
	}
	durations := make([]time.Duration, 0, b.N)
	b.ResetTimer()
	b.StartTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		started := time.Now()
		if err := execute(); err != nil {
			b.Fatalf("authored expression benchmark iteration %d: %v", iteration, err)
		}
		durations = append(durations, time.Since(started))
	}
	b.StopTimer()
	median, coefficientOfVariation := authoredExpressionBenchmarkDistribution(durations)
	b.ReportMetric(float64(rows), "rows/op")
	b.ReportMetric(float64(median)/float64(time.Millisecond), "median_ms/op")
	b.ReportMetric(coefficientOfVariation*100, "cv_pct")
	if median > 0 {
		b.ReportMetric(float64(rows)/median.Seconds(), "rows/s")
	}
}

func authoredExpressionBenchmarkDistribution(samples []time.Duration) (time.Duration, float64) {
	ordered := slices.Clone(samples)
	slices.Sort(ordered)
	median := ordered[len(ordered)/2]
	if len(ordered)%2 == 0 {
		median = (ordered[len(ordered)/2-1] + median) / 2
	}
	var total float64
	for _, sample := range samples {
		total += float64(sample)
	}
	mean := total / float64(len(samples))
	if mean == 0 {
		return median, 0
	}
	var squared float64
	for _, sample := range samples {
		delta := float64(sample) - mean
		squared += delta * delta
	}
	return median, math.Sqrt(squared/float64(len(samples))) / mean
}

func authoredExpressionBenchmarkResultError(got, want float64) error {
	tolerance := math.Max(1, math.Abs(want)) * 1e-12
	if math.IsNaN(got) || math.Abs(got-want) > tolerance {
		return fmt.Errorf("result = %.17g, want %.17g", got, want)
	}
	return nil
}

func authoredExpressionBenchmarkHighCandidates(rows uint64) []string {
	count := min(rows, uint64(16))
	result := make([]string, 0, count)
	for index := range count {
		row := index * rows / count
		result = append(result, fmt.Sprintf("bench-%08d", row))
	}
	return result
}

func authoredExpressionBenchmarkQuotedCandidates(candidates []string) string {
	quoted := make([]string, len(candidates))
	for index, candidate := range candidates {
		quoted[index] = strconv.Quote(candidate)
	}
	return strings.Join(quoted, ", ")
}

func authoredExpressionBenchmarkAnyStrings(values []string) []any {
	result := make([]any, len(values))
	for index, value := range values {
		result[index] = value
	}
	return result
}
