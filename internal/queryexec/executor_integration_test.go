package queryexec

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"maps"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	clickhousedriver "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/eventfields"
	"github.com/Suhaibinator/open-splunk/internal/indexread"
	"github.com/Suhaibinator/open-splunk/internal/plan"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
	"github.com/Suhaibinator/open-splunk/internal/searchtime"
	"github.com/Suhaibinator/open-splunk/internal/spl"
	"github.com/Suhaibinator/open-splunk/internal/testsupport"
	"github.com/Suhaibinator/open-splunk/internal/testsupport/gradethiscorpus"
)

const queryExecutorIntegrationImage = testsupport.DefaultClickHouseImage

// TestExecutorAndManagerAgainstClickHouse is opt-in because it starts an
// ephemeral Docker container and may pull the pinned ClickHouse image.
func TestExecutorAndManagerAgainstClickHouse(t *testing.T) {
	if os.Getenv("OPEN_SPLUNK_CLICKHOUSE_INTEGRATION") != "1" {
		t.Skip("set OPEN_SPLUNK_CLICKHOUSE_INTEGRATION=1 to run the Docker integration test")
	}
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skipf("docker CLI is unavailable: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	container := "open-splunk-queryexec-" + queryIntegrationRandomHex(t, 6)
	password := queryIntegrationRandomHex(t, 24)
	image := os.Getenv("OPEN_SPLUNK_CLICKHOUSE_TEST_IMAGE")
	if image == "" {
		image = queryExecutorIntegrationImage
	}
	queryIntegrationDocker(t, ctx, nil,
		"run", "--detach", "--rm", "--name", container,
		"--publish", "127.0.0.1::9000",
		"--env", "CLICKHOUSE_DB=open_splunk",
		"--env", "CLICKHOUSE_USER=open_splunk",
		"--env", "CLICKHOUSE_PASSWORD="+password,
		"--env", "CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT=1",
		image,
	)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cleanupCancel()
		_ = exec.CommandContext(cleanupCtx, "docker", "rm", "--force", "--volumes", container).Run()
	})
	queryIntegrationWait(t, ctx, container, password)
	queryIntegrationMigrate(t, ctx, container, password)

	connectionOptions := &clickhousedriver.Options{
		Addr: []string{queryIntegrationNativeAddress(t, ctx, container)},
		Auth: clickhousedriver.Auth{
			Database: "open_splunk",
			Username: "open_splunk",
			Password: password,
		},
		DialTimeout: 5 * time.Second,
	}
	connection, err := clickhousedriver.Open(connectionOptions)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = connection.Close() })
	if err := connection.Ping(ctx); err != nil {
		t.Fatal(err)
	}

	executor, err := New(connection, Config{
		ReadAdmission: indexread.UnfencedAdmission{},
	})
	if err != nil {
		t.Fatal(err)
	}
	diagnosticExecutor := queryIntegrationDiagnosticExecutor(t, connection, Config{})
	explainer, err := NewExplainer(connectionOptions, Config{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := explainer.Close(); err != nil {
			t.Errorf("close EXPLAIN transport: %v", err)
		}
	})
	t.Run("structured EXPLAIN accepts empty MergeTree ReadNothing", func(t *testing.T) {
		anchor := time.Date(
			2026,
			time.July,
			27,
			12,
			0,
			0,
			0,
			time.UTC,
		)
		compiled := queryIntegrationCompileSearchRange(
			t,
			`index=main`,
			anchor,
			anchor.Add(-time.Hour),
			anchor.Add(time.Hour),
		)
		explained, err := explainer.Explain(ctx, compiled)
		if err != nil {
			t.Fatalf("Explain() empty MergeTree query: %v", err)
		}
		physical, err := ParseExplainPlan(explained)
		if err != nil {
			t.Fatalf("parse empty MergeTree plan: %v", err)
		}
		if !slices.Contains(physical.NodeTypes, "ReadNothing") ||
			len(physical.Reads) != 0 {
			t.Fatalf("empty MergeTree plan = %#v", physical)
		}
	})
	queryIntegrationTestFieldCatalog(t, ctx, connection, executor)
	queryIntegrationTestLogicalRetention(t, ctx, connection, executor)
	t.Run("native progress reports exact generated scan", func(t *testing.T) {
		const generatedRows = uint64(262_144)
		sink := &recordingProgressSink{}
		err := diagnosticExecutor.Execute(ctx, clickhouse.CompiledQuery{
			SQL: "SELECT sum(cityHash64(number)) AS checksum FROM numbers(262144)",
			OutputFields: []string{
				"checksum",
			},
		}, sink)
		if err != nil {
			t.Fatal(err)
		}

		deltas := sink.snapshotDeltas()
		var scannedRows, scannedBytes uint64
		for _, delta := range deltas {
			if ^scannedRows < delta.ScannedRows || ^scannedBytes < delta.ScannedBytes {
				t.Fatalf("native progress overflowed: %#v", deltas)
			}
			scannedRows += delta.ScannedRows
			scannedBytes += delta.ScannedBytes
		}
		if scannedRows != generatedRows {
			t.Fatalf("scanned rows = %d, want exactly %d; packets=%#v", scannedRows, generatedRows, deltas)
		}
		if scannedBytes != generatedRows*8 {
			t.Fatalf("scanned bytes = %d, want exactly %d; packets=%#v", scannedBytes, generatedRows*8, deltas)
		}
		if sink.setCalls != 1 || len(sink.rows) != 1 {
			t.Fatalf("result publication = schema calls %d, rows %d", sink.setCalls, len(sink.rows))
		}
	})
	t.Run("native typed scan types", func(t *testing.T) {
		sink := &fakeSink{}
		err := diagnosticExecutor.Execute(ctx, clickhouse.CompiledQuery{
			SQL: "SELECT toDecimal128('123.4500', 4) AS amount, " +
				"CAST('12:34:56.123456' AS Time64(6)) AS elapsed, " +
				"CAST(42 AS Dynamic) AS choice, " +
				"CAST(toInt256('999999999999999999990') AS Dynamic) AS wide_choice, " +
				"CAST('{\"name\":\"alice\",\"count\":2}' AS JSON) AS payload",
			OutputFields: []string{"amount", "elapsed", "choice", "wide_choice", "payload"},
		}, sink)
		if err != nil {
			t.Fatal(err)
		}
		wantKinds := []searchjobs.ValueKind{
			searchjobs.ValueKindDecimal, searchjobs.ValueKindDuration,
			searchjobs.ValueKindMixed, searchjobs.ValueKindMixed, searchjobs.ValueKindObject,
		}
		for index, want := range wantKinds {
			if sink.schema.Columns[index].Kind != want {
				t.Fatalf("column %d kind = %v, want %v", index, sink.schema.Columns[index].Kind, want)
			}
		}
		if got, ok := sink.rows[0][0].Decimal(); !ok || got != "123.45" {
			t.Fatalf("decimal = %q, %v", got, ok)
		}
		if got, ok := sink.rows[0][1].Duration(); !ok || got != 12*time.Hour+34*time.Minute+56*time.Second+123456*time.Microsecond {
			t.Fatalf("duration = %v, %v", got, ok)
		}
		if got, ok := sink.rows[0][2].Unsigned(); !ok || got != 42 {
			t.Fatalf("dynamic = %d, %v", got, ok)
		}
		if got, ok := sink.rows[0][3].Decimal(); !ok || got != "999999999999999999990" {
			t.Fatalf("Dynamic(Int256) = %q, %v", got, ok)
		}
		if fields, ok := sink.rows[0][4].Object(); !ok || len(fields) != 2 {
			t.Fatalf("JSON = %#v", sink.rows[0][4])
		}
	})

	eventIndexTime := queryIntegrationInsertEvent(t, ctx, connection)
	binaryIndexTime := queryIntegrationInsertBinaryEvent(t, ctx, connection)
	timechartBase, timechartIndexTime := queryIntegrationInsertTimechartEvents(t, ctx, connection)
	countFieldBase, countFieldIndexTime := queryIntegrationInsertTimechartCountFieldEvents(t, ctx, connection)
	t.Run("eventstats production resource envelope", func(t *testing.T) {
		queryIntegrationTestEventStatsProductionEnvelope(
			t,
			ctx,
			connection,
			eventIndexTime,
		)
	})
	t.Run("streamstats executor and manager transport", func(t *testing.T) {
		queryIntegrationTestStreamStatsTransport(
			t,
			ctx,
			executor,
			eventIndexTime,
		)
	})
	t.Run("timechart field occurrence count", func(t *testing.T) {
		queryIntegrationTestTimechartCountField(
			t,
			ctx,
			connection,
			executor,
			explainer,
			countFieldBase,
			countFieldIndexTime,
		)
	})
	t.Run("fixed percentile timechart", func(t *testing.T) {
		queryIntegrationTestFixedPercentileTimechart(
			t,
			ctx,
			connection,
			executor,
			timechartBase,
			timechartIndexTime,
		)
	})
	t.Run("fixed numeric timechart", func(t *testing.T) {
		queryIntegrationTestFixedNumericTimechart(
			t,
			ctx,
			connection,
			executor,
			explainer,
			timechartBase,
			timechartIndexTime,
		)
	})
	splitNumericBase, splitNumericIndexTime := queryIntegrationInsertSplitNumericTimechartEvents(t, ctx, connection)
	t.Run("split numeric timechart", func(t *testing.T) {
		queryIntegrationTestSplitNumericTimechart(
			t,
			ctx,
			connection,
			executor,
			explainer,
			splitNumericBase,
			splitNumericIndexTime,
		)
	})
	t.Run("split percentile timechart", func(t *testing.T) {
		queryIntegrationTestSplitPercentileTimechart(
			t,
			ctx,
			connection,
			explainer,
			splitNumericBase,
			splitNumericIndexTime,
		)
	})
	gradeThisBase, gradeThisIndexTime, gradeThisTraceID := queryIntegrationInsertGradeThisEvents(t, ctx, connection)
	chartBase, chartIndexTime := queryIntegrationInsertChartEvents(t, ctx, connection)
	t.Run("chart field occurrence count", func(t *testing.T) {
		queryIntegrationTestChartCountField(
			t,
			ctx,
			connection,
			executor,
			explainer,
			countFieldBase,
			countFieldIndexTime,
		)
	})
	t.Run("structured EXPLAIN accepts an index-free MergeTree read", func(t *testing.T) {
		compiled := queryIntegrationCompileSearchRange(
			t,
			`index=main | where true=false`,
			eventIndexTime,
			eventIndexTime.Add(-time.Hour),
			eventIndexTime.Add(time.Hour),
		)
		explained, err := explainer.Explain(ctx, compiled)
		if err != nil {
			t.Fatalf("Explain() constant-false query: %v", err)
		}
		physical, err := ParseExplainPlan(explained)
		if err != nil {
			t.Fatalf("parse constant-false plan: %v", err)
		}
		indexlessRead := false
		for _, read := range physical.Reads {
			if len(read.Columns) != 0 && len(read.Indexes) == 0 {
				indexlessRead = true
				break
			}
		}
		if !slices.Contains(physical.NodeTypes, "ReadFromMergeTree") ||
			!indexlessRead {
			t.Fatalf("constant-false MergeTree plan = %#v", physical)
		}
	})
	t.Run("structured EXPLAIN accepts a legal deep sort pipeline", func(t *testing.T) {
		var source strings.Builder
		source.WriteString("| table host")
		for range 21 {
			source.WriteString(" | sort host")
		}
		compiled := queryIntegrationCompileSearchRange(
			t,
			source.String(),
			eventIndexTime,
			eventIndexTime.Add(-time.Hour),
			eventIndexTime.Add(time.Hour),
		)
		explained, err := explainer.Explain(ctx, compiled)
		if err != nil {
			t.Fatalf("Explain() 21-sort query: %v", err)
		}
		physical, err := ParseExplainPlan(explained)
		if err != nil {
			t.Fatalf("parse 21-sort plan: %v", err)
		}
		if len(physical.NodeTypes) <= 64 ||
			!slices.Contains(physical.NodeTypes, "ReadFromMergeTree") {
			t.Fatalf(
				"21-sort physical plan has %d nodes: %#v",
				len(physical.NodeTypes),
				physical,
			)
		}
	})
	t.Run("bounded EXPLAIN accepts compiler SQL and ordered parameters", func(t *testing.T) {
		// clickhouse-go v2.46 overwrites a protocol max_execution_time from a
		// visible deadline by adding five seconds. Prove on the pinned server
		// that our fixed outer SQL clause wins over that looser value, including
		// inside the wrapped subquery that EXPLAIN plans.
		probeContext, probeCancel := context.WithTimeout(ctx, 30*time.Second)
		defer probeCancel()
		probeContext = clickhousedriver.Context(
			probeContext,
			clickhousedriver.WithSettings(clickhousedriver.Settings{
				"max_execution_time": uint64(1),
			}),
		)
		var protocolTimeout uint64
		if err := connection.QueryRow(
			probeContext,
			"SELECT toUInt64(getSetting('max_execution_time'))",
		).Scan(&protocolTimeout); err != nil {
			t.Fatalf("read deadline-derived protocol timeout: %v", err)
		}
		if protocolTimeout <=
			uint64(maximumExplainExecutionTime/time.Second) {
			t.Fatalf(
				"deadline-derived protocol timeout = %d, want above EXPLAIN cap",
				protocolTimeout,
			)
		}
		var clauseTimeout uint64
		if err := connection.QueryRow(
			probeContext,
			"SELECT effective_timeout FROM ("+
				"SELECT toUInt64(getSetting('max_execution_time')) AS effective_timeout"+
				") AS __os_explain_input SETTINGS max_execution_time = "+
				strconv.FormatUint(
					uint64(maximumExplainExecutionTime/time.Second),
					10,
				),
		).Scan(&clauseTimeout); err != nil {
			t.Fatalf("read SQL-clause timeout: %v", err)
		}
		if clauseTimeout !=
			uint64(maximumExplainExecutionTime/time.Second) {
			t.Fatalf(
				"SQL-clause timeout = %d, want %d (protocol was %d)",
				clauseTimeout,
				uint64(maximumExplainExecutionTime/time.Second),
				protocolTimeout,
			)
		}

		compiled := queryIntegrationCompileSearchRange(
			t,
			`index=main "hello"`,
			eventIndexTime,
			eventIndexTime.Add(-time.Hour),
			eventIndexTime.Add(time.Hour),
		)
		if len(compiled.Args) == 0 {
			t.Fatal("compiler produced no ordered parameters for EXPLAIN coverage")
		}
		if !slices.ContainsFunc(compiled.Args, func(argument any) bool {
			literal, ok := argument.(string)
			return ok && literal == "hello"
		}) {
			t.Fatalf("compiler arguments have no search literal: %#v", compiled.Args)
		}
		if !compiled.HasValidSQLSeal() || !compiled.HasValidExecutionSeal() {
			t.Fatal("compiled SQL or execution authority seal is invalid")
		}
		explained, err := explainer.Explain(ctx, compiled)
		if err != nil {
			t.Fatalf("Explain() error = %v", err)
		}
		if !validExplainQueryID(explained.QueryID) ||
			!strings.HasPrefix(explained.QueryID, "open-splunk-explain-") {
			t.Fatalf("EXPLAIN query ID = %q", explained.QueryID)
		}
		queryIntegrationAssertStructuredExplain(t, explained)
	})
	t.Run("bounded EXPLAIN wraps chronological compiler settings", func(t *testing.T) {
		compiled := queryIntegrationCompileSearchRange(
			t,
			`index=main | stats earliest(path) AS first latest(path) AS last`,
			eventIndexTime,
			eventIndexTime.Add(-time.Hour),
			eventIndexTime.Add(time.Hour),
		)
		if !compiled.HasValidSQLSeal() ||
			!strings.Contains(compiled.SQL, " UNION ALL ") ||
			!strings.Contains(compiled.SQL, "__os_chronological_") ||
			!strings.HasSuffix(
				compiled.SQL,
				" SETTINGS enable_materialized_cte = 1",
			) {
			t.Fatalf(
				"Compiler did not produce sealed chronological UNION with inner settings:\n%s",
				compiled.SQL,
			)
		}
		explained, err := explainer.Explain(ctx, compiled)
		if err != nil {
			t.Fatalf("Explain() chronological compiler query: %v", err)
		}
		queryIntegrationAssertStructuredExplain(t, explained)
	})
	t.Run("timeline compiler and executor preserve exact event selection", func(t *testing.T) {
		earliest := gradeThisBase.Add(2 * time.Minute)
		latest := gradeThisBase.Add(12 * time.Minute)
		spec := clickhouse.TimelineSpec{
			FirstBucket: gradeThisBase,
			SpanSeconds: int64((5 * time.Minute) / time.Second),
			BucketCount: 3,
			Earliest:    earliest,
			Latest:      latest,
		}
		tests := []struct {
			name       string
			source     string
			wantCounts []uint64
		}{
			{
				name:       "half-open search boundaries",
				source:     `index=gradethis`,
				wantCounts: []uint64{2, 3, 1},
			},
			{
				name:       "sort and head pipeline with zero fill",
				source:     `index=gradethis | sort 0 -_time | head 4`,
				wantCounts: []uint64{0, 3, 1},
			},
		}
		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				compiled := queryIntegrationCompileTimeline(
					t, test.source, "gradethis", gradeThisIndexTime, spec,
				)
				buckets, err := executor.ExecuteTimeline(ctx, compiled)
				if err != nil {
					t.Fatalf("ExecuteTimeline() error = %v", err)
				}
				if len(buckets) != len(test.wantCounts) {
					t.Fatalf("timeline buckets = %#v, want %d buckets", buckets, len(test.wantCounts))
				}
				for index, wantCount := range test.wantCounts {
					wantStart := gradeThisBase.Add(time.Duration(index) * 5 * time.Minute)
					if !buckets[index].AlignedStart.Equal(wantStart) || buckets[index].Count != wantCount {
						t.Fatalf(
							"timeline bucket %d = {%v, %d}, want {%v, %d}",
							index, buckets[index].AlignedStart, buckets[index].Count, wantStart, wantCount,
						)
					}
				}
			})
		}
	})
	t.Run("GradeThis compatibility smoke and distinct-count extension", func(t *testing.T) {
		sourceFor := func(id gradethiscorpus.SearchID) string {
			t.Helper()
			for _, search := range gradethiscorpus.Searches() {
				if search.ID != id {
					continue
				}
				source, err := search.Render(gradeThisTraceID)
				if err != nil {
					t.Fatal(err)
				}
				return source
			}
			t.Fatalf("GradeThis corpus search %q is missing", id)
			return ""
		}
		tests := []struct {
			name        string
			source      string
			wantColumns []string
			wantRows    int
			assert      func(*testing.T, searchjobs.ResultPage)
		}{
			{
				name:        "follow one request",
				source:      sourceFor(gradethiscorpus.SearchFollowTrace),
				wantColumns: []string{"_time", "level", "layer", "logger", "message"},
				wantRows:    2,
				assert: func(t *testing.T, page searchjobs.ResultPage) {
					first, firstOK := page.Rows[0].Values[0].Time()
					second, secondOK := page.Rows[1].Values[0].Time()
					if !firstOK || !secondOK || !first.Equal(gradeThisBase.Add(time.Minute)) || !second.Equal(gradeThisBase.Add(2*time.Minute)) {
						t.Fatalf("trace order = %v, %v (valid %v, %v)", first, second, firstOK, secondOK)
					}
				},
			},
			{
				name:     "errors and warnings",
				source:   sourceFor(gradethiscorpus.SearchErrorsAndWarnings),
				wantRows: 5,
				assert: func(t *testing.T, page searchjobs.ResultPage) {
					timeColumn := queryIntegrationColumnIndex(t, page, "_time")
					serviceColumn := queryIntegrationColumnIndex(t, page, "service")
					if !page.Schema.Columns[serviceColumn].Nullable {
						t.Fatalf("service schema = %#v, want nullable", page.Schema.Columns[serviceColumn])
					}
					nullServices := 0
					for index := 1; index < len(page.Rows); index++ {
						previous, previousOK := page.Rows[index-1].Values[timeColumn].Time()
						current, currentOK := page.Rows[index].Values[timeColumn].Time()
						if !previousOK || !currentOK || previous.Before(current) {
							t.Fatalf("descending _time rows %d and %d = %v, %v", index-1, index, previous, current)
						}
					}
					for _, row := range page.Rows {
						if row.Values[serviceColumn].IsNull() {
							nullServices++
						}
					}
					if nullServices != 1 {
						t.Fatalf("null service rows = %d, want 1", nullServices)
					}
				},
			},
			{
				name:        "known raw error fragment",
				source:      sourceFor(gradethiscorpus.SearchRawErrorFragment),
				wantColumns: []string{"_time", "level", "logger", "message", "trace_id"},
				wantRows:    1,
				assert: func(t *testing.T, page searchjobs.ResultPage) {
					message, messageOK := page.Rows[0].Values[3].String()
					traceID, traceOK := page.Rows[0].Values[4].String()
					if !messageOK || message != "connection refused" || !traceOK || traceID != gradeThisTraceID {
						t.Fatalf("raw match message=%q (%v), trace_id=%q (%v)", message, messageOK, traceID, traceOK)
					}
				},
			},
			{
				name:        "severity counts",
				source:      sourceFor(gradethiscorpus.SearchSeverityCounts),
				wantColumns: []string{"level", "count"},
				wantRows:    3,
				assert: func(t *testing.T, page searchjobs.ResultPage) {
					counts := make(map[string]uint64, len(page.Rows))
					for _, row := range page.Rows {
						level, levelOK := row.Values[0].String()
						count, countOK := row.Values[1].Unsigned()
						if !levelOK || !countOK {
							t.Fatalf("severity row = %#v", row)
						}
						counts[level] = count
					}
					if counts["ERROR"] != 4 || counts["INFO"] != 4 || counts["WARN"] != 1 {
						t.Fatalf("severity counts = %#v", counts)
					}
				},
			},
			{
				name:        "distinct loggers",
				source:      `index=gradethis | stats distinct_count(logger) AS unique_loggers`,
				wantColumns: []string{"unique_loggers"},
				wantRows:    1,
				assert: func(t *testing.T, page searchjobs.ResultPage) {
					distinct, ok := page.Rows[0].Values[0].Unsigned()
					if !ok || distinct != 5 {
						t.Fatalf("distinct loggers = %d (%v), want unsigned 5", distinct, ok)
					}
				},
			},
			{
				name:        "frequent errors",
				source:      sourceFor(gradethiscorpus.SearchFrequentErrors),
				wantColumns: []string{"logger", "message", "count"},
				wantRows:    2,
				assert: func(t *testing.T, page searchjobs.ResultPage) {
					logger, loggerOK := page.Rows[0].Values[0].String()
					message, messageOK := page.Rows[0].Values[1].String()
					count, countOK := page.Rows[0].Values[2].Unsigned()
					if !loggerOK || logger != "http" || !messageOK || message != "Request metrics" || !countOK || count != 3 {
						t.Fatalf("most frequent error = %#v", page.Rows[0])
					}
				},
			},
			{
				name:        "volume by severity",
				source:      sourceFor(gradethiscorpus.SearchVolumeBySeverity),
				wantColumns: []string{"_time", "ERROR", "INFO", "WARN"},
				wantRows:    3,
				assert: func(t *testing.T, page searchjobs.ResultPage) {
					queryIntegrationAssertTimechartMatrix(t, page, gradeThisBase, 5*time.Minute, map[string][]uint64{
						"ERROR": {1, 2, 1},
						"INFO":  {1, 1, 2},
						"WARN":  {1, 0, 0},
					})
				},
			},
			{
				name:        "server errors by route",
				source:      sourceFor(gradethiscorpus.SearchServerErrors),
				wantColumns: []string{"_time", "/fast", "/slow"},
				wantRows:    3,
				assert: func(t *testing.T, page searchjobs.ResultPage) {
					queryIntegrationAssertTimechartMatrix(t, page, gradeThisBase, 5*time.Minute, map[string][]uint64{
						"/fast": {0, 0, 1},
						"/slow": {0, 2, 0},
					})
				},
			},
			{
				name:        "responses by route and status",
				source:      sourceFor(gradethiscorpus.SearchResponses),
				wantColumns: []string{"path", "status", "count"},
				wantRows:    4,
				assert: func(t *testing.T, page searchjobs.ResultPage) {
					counts := make(map[string]uint64, len(page.Rows))
					for _, row := range page.Rows {
						path, pathOK := row.Values[0].String()
						status, statusOK := row.Values[1].String()
						count, countOK := row.Values[2].Unsigned()
						if !pathOK || !statusOK || !countOK {
							t.Fatalf("route/status row = %#v", row)
						}
						counts[path+"\x00"+status] = count
					}
					want := map[string]uint64{
						"/slow\x00503": 1,
						"/slow\x00500": 1,
						"/fast\x00200": 1,
						"/fast\x00502": 1,
					}
					if len(counts) != len(want) {
						t.Fatalf("route/status counts = %#v, want %#v", counts, want)
					}
					for key, wantCount := range want {
						if counts[key] != wantCount {
							t.Fatalf("route/status counts = %#v, want %#v", counts, want)
						}
					}
				},
			},
			{
				name:        "slow routes",
				source:      sourceFor(gradethiscorpus.SearchSlowRoutes),
				wantColumns: []string{"path", "count", "p95_ms"},
				wantRows:    1,
				assert: func(t *testing.T, page searchjobs.ResultPage) {
					path, pathOK := page.Rows[0].Values[0].String()
					count, countOK := page.Rows[0].Values[1].Unsigned()
					p95, p95OK := page.Rows[0].Values[2].Double()
					if !pathOK || path != "/slow" || !countOK || count != 2 || !p95OK || p95 <= 500 {
						t.Fatalf("slow route row = %#v", page.Rows[0])
					}
				},
			},
			{
				name:        "common messages",
				source:      sourceFor(gradethiscorpus.SearchTopMessages),
				wantColumns: []string{"message", "count", "percent"},
				wantRows:    5,
				assert: func(t *testing.T, page searchjobs.ResultPage) {
					message, messageOK := page.Rows[0].Values[0].String()
					count, countOK := page.Rows[0].Values[1].Unsigned()
					if !messageOK || message != "Request metrics" || !countOK || count != 4 {
						t.Fatalf("top message row = %#v", page.Rows[0])
					}
				},
			},
		}

		for index, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				job, page := queryIntegrationRunGradeThisSearchRange(
					t, ctx, executor, gradeThisIndexTime,
					fmt.Sprintf("queryexec-gradethis-%02d", index), test.source,
					gradeThisBase, gradeThisBase.Add(15*time.Minute),
				)
				if job.State != searchjobs.StateCompleted {
					t.Fatalf("state = %v, failure=%#v", job.State, job.Failure)
				}
				if test.wantColumns != nil {
					queryIntegrationAssertColumns(t, page, test.wantColumns)
				}
				if len(page.Rows) != test.wantRows || page.TotalRows != uint64(test.wantRows) || !page.Complete {
					t.Fatalf("rows=%d total=%d complete=%v, want %d complete rows", len(page.Rows), page.TotalRows, page.Complete, test.wantRows)
				}
				if test.assert != nil {
					test.assert(t, page)
				}
			})
		}
	})
	t.Run("dedup pipeline through manager preserves deterministic winner", func(t *testing.T) {
		job, page := queryIntegrationRunGradeThisSearchRange(
			t, ctx, executor, gradeThisIndexTime, "queryexec-dedup-pipeline",
			`index=gradethis message=heartbeat | dedup logger | table event_id, logger`,
			gradeThisBase, gradeThisBase.Add(15*time.Minute),
		)
		if job.State != searchjobs.StateCompleted {
			t.Fatalf("dedup state = %v, failure=%#v", job.State, job.Failure)
		}
		queryIntegrationAssertColumns(t, page, []string{"event_id", "logger"})
		if len(page.Rows) != 1 || page.TotalRows != 1 || !page.Complete {
			t.Fatalf("dedup page = %#v, want one complete row", page)
		}
		if id, ok := page.Rows[0].Values[0].String(); !ok || id != "queryexec-gradethis-heartbeat-b" {
			t.Fatalf("dedup winner = %q, %v", id, ok)
		}
		if logger, ok := page.Rows[0].Values[1].String(); !ok || logger != "health" {
			t.Fatalf("dedup logger = %q, %v", logger, ok)
		}
	})
	t.Run("extended event fields retain their types", func(t *testing.T) {
		job, page := queryIntegrationRunSearch(t, ctx, executor, eventIndexTime,
			"queryexec-extended-values",
			`index=main | table typed_bytes, typed_timestamp, typed_duration, typed_decimal`,
		)
		if job.State != searchjobs.StateCompleted {
			t.Fatalf("extended-value state = %v, failure=%#v", job.State, job.Failure)
		}
		if len(page.Rows) != 1 || len(page.Rows[0].Values) != 4 {
			t.Fatalf("extended-value page = %#v", page)
		}
		if got, ok := page.Rows[0].Values[0].Bytes(); !ok || !bytes.Equal(got, []byte{0, 0xff}) {
			t.Fatalf("bytes = %v, %v", got, ok)
		}
		wantTimestamp := eventIndexTime.Add(42 * time.Second)
		if got, ok := page.Rows[0].Values[1].Time(); !ok || !got.Equal(wantTimestamp) || got.Location() != time.UTC {
			t.Fatalf("timestamp = %v, %v", got, ok)
		}
		if got, ok := page.Rows[0].Values[2].Duration(); !ok || got != -(12*time.Second+345*time.Millisecond) {
			t.Fatalf("duration = %v, %v", got, ok)
		}
		if got, ok := page.Rows[0].Values[3].Decimal(); !ok || got != "-123.4500e+2" {
			t.Fatalf("decimal = %q, %v", got, ok)
		}
	})
	t.Run("manager pipeline and stable page", func(t *testing.T) {
		manager, err := searchjobs.New(searchjobs.Config{
			Executor:        executor,
			Snapshotter:     queryIntegrationSnapshotter(1),
			Compiler:        clickhouse.Compiler{},
			MaxConcurrent:   1,
			MaxQueued:       1,
			CleanupInterval: -1,
			NewID:           func() string { return "queryexec-integration-job" },
			CursorKey:       []byte("0123456789abcdef0123456789abcdef"),
		})
		if err != nil {
			t.Fatal(err)
		}
		defer manager.Close()
		now := time.Now().UTC()
		job, err := manager.Create(ctx, searchjobs.CreateRequest{
			SPL:               "index=main | table message",
			OwnerID:           "owner",
			TenantID:          "tenant",
			AuthorizedIndexes: []string{"main"},
			RequestedIndexes:  []string{"main"},
			TimeRange:         queryIntegrationTimeRange(t, now.Add(-time.Hour), now.Add(time.Hour)),
		})
		if err != nil {
			t.Fatal(err)
		}
		completed := queryIntegrationWaitForTerminal(t, manager, job.ID)
		if completed.State != searchjobs.StateCompleted {
			t.Fatalf("job state = %v, failure=%#v", completed.State, completed.Failure)
		}
		if completed.ScannedRows == 0 || completed.ScannedBytes == 0 {
			t.Fatalf(
				"completed job progress = %d rows, %d bytes; want nonzero native scan counters",
				completed.ScannedRows,
				completed.ScannedBytes,
			)
		}
		page, err := manager.Results(job.ID, searchjobs.PageRequest{Limit: 10})
		if err != nil {
			t.Fatal(err)
		}
		if len(page.Rows) != 1 {
			t.Fatalf("rows = %d, want 1", len(page.Rows))
		}
		if got, ok := page.Rows[0].Values[0].String(); !ok || got != "manager integration" {
			t.Fatalf("message = %q, %v", got, ok)
		}
	})

	t.Run("stats manager pipeline and typed schema", func(t *testing.T) {
		manager, err := searchjobs.New(searchjobs.Config{
			Executor:        executor,
			Snapshotter:     queryIntegrationSnapshotter(1),
			Compiler:        clickhouse.Compiler{},
			MaxConcurrent:   1,
			MaxQueued:       1,
			CleanupInterval: -1,
			// The event and cutoff deliberately share a wall-clock second. A bare
			// time.Time bind is inferred as second-precision DateTime and used to
			// exclude this already-committed DateTime64(3) event.
			Now:       func() time.Time { return eventIndexTime.Add(500 * time.Microsecond) },
			NewID:     func() string { return "queryexec-stats-integration-job" },
			CursorKey: []byte("0123456789abcdef0123456789abcdef"),
		})
		if err != nil {
			t.Fatal(err)
		}
		defer manager.Close()
		now := eventIndexTime.Add(500 * time.Microsecond)
		job, err := manager.Create(ctx, searchjobs.CreateRequest{
			SPL:               "index=main | stats count AS events by host",
			OwnerID:           "owner",
			TenantID:          "tenant",
			AuthorizedIndexes: []string{"main"},
			RequestedIndexes:  []string{"main"},
			TimeRange:         queryIntegrationTimeRange(t, now.Add(-time.Hour), now.Add(time.Hour)),
		})
		if err != nil {
			t.Fatal(err)
		}
		completed := queryIntegrationWaitForTerminal(t, manager, job.ID)
		if completed.State != searchjobs.StateCompleted {
			t.Fatalf("job state = %v, failure=%#v", completed.State, completed.Failure)
		}
		page, err := manager.Results(job.ID, searchjobs.PageRequest{Limit: 10})
		if err != nil {
			t.Fatal(err)
		}
		if len(page.Schema.Columns) != 2 || page.Schema.Columns[0].Name != "host" ||
			page.Schema.Columns[0].Kind != searchjobs.ValueKindString || page.Schema.Columns[1].Name != "events" ||
			page.Schema.Columns[1].Kind != searchjobs.ValueKindUnsigned {
			t.Fatalf("stats schema = %#v", page.Schema)
		}
		if len(page.Rows) != 1 {
			t.Fatalf("stats rows = %d, want 1", len(page.Rows))
		}
		if host, ok := page.Rows[0].Values[0].String(); !ok || host != "host" {
			t.Fatalf("stats host = %q, %v", host, ok)
		}
		if count, ok := page.Rows[0].Values[1].Unsigned(); !ok || count != 1 {
			t.Fatalf("stats count = %d, %v", count, ok)
		}
	})

	t.Run("post-stats pipeline through manager", func(t *testing.T) {
		job, page := queryIntegrationRunSearch(t, ctx, executor, eventIndexTime,
			"queryexec-post-stats-pipeline",
			`index=main | stats count AS events by status | search events>0 | sort -events | head 1 | table status, events`,
		)
		if job.State != searchjobs.StateCompleted {
			t.Fatalf("post-stats state = %v, failure=%#v", job.State, job.Failure)
		}
		if len(page.Schema.Columns) != 2 || page.Schema.Columns[0].Name != "status" ||
			page.Schema.Columns[0].Kind != searchjobs.ValueKindString ||
			page.Schema.Columns[1].Name != "events" || page.Schema.Columns[1].Kind != searchjobs.ValueKindUnsigned {
			t.Fatalf("post-stats schema = %#v", page.Schema)
		}
		if len(page.Rows) != 1 {
			t.Fatalf("post-stats rows = %d, want 1", len(page.Rows))
		}
		if status, ok := page.Rows[0].Values[0].String(); !ok || status != "200" {
			t.Fatalf("post-stats status = %q, %v", status, ok)
		}
		if count, ok := page.Rows[0].Values[1].Unsigned(); !ok || count != 1 {
			t.Fatalf("post-stats count = %d, %v", count, ok)
		}
	})

	t.Run("spath typed output through manager", func(t *testing.T) {
		job, page := queryIntegrationRunSearch(t, ctx, executor, eventIndexTime,
			"queryexec-spath-pipeline",
			`index=main | spath input=payload output=first_sku path=items{0}.sku | where first_sku="sku-1" | table first_sku`,
		)
		if job.State != searchjobs.StateCompleted {
			t.Fatalf("spath state = %v, failure=%#v", job.State, job.Failure)
		}
		if len(page.Schema.Columns) != 1 || page.Schema.Columns[0].Name != "first_sku" ||
			page.Schema.Columns[0].Kind != searchjobs.ValueKindMixed ||
			!page.Schema.Columns[0].Nullable || page.Schema.Columns[0].Multivalue {
			t.Fatalf("spath schema = %#v", page.Schema)
		}
		if len(page.Rows) != 1 {
			t.Fatalf("spath rows = %d, want 1", len(page.Rows))
		}
		if sku, ok := page.Rows[0].Values[0].String(); !ok || sku != "sku-1" {
			t.Fatalf("spath first_sku = %q, %v", sku, ok)
		}
	})

	t.Run("stats sum and average nullable doubles through manager", func(t *testing.T) {
		job, page := queryIntegrationRunSearch(t, ctx, executor, eventIndexTime,
			"queryexec-stats-sum-average",
			`index=main | stats count sum(status) AS total avg(status) AS mean`,
		)
		if job.State != searchjobs.StateCompleted {
			t.Fatalf("sum/avg state = %v, failure=%#v", job.State, job.Failure)
		}
		if len(page.Schema.Columns) != 3 || page.Schema.Columns[0].Name != "count" ||
			page.Schema.Columns[0].Kind != searchjobs.ValueKindUnsigned ||
			page.Schema.Columns[1].Name != "total" || page.Schema.Columns[1].Kind != searchjobs.ValueKindDouble || !page.Schema.Columns[1].Nullable ||
			page.Schema.Columns[2].Name != "mean" || page.Schema.Columns[2].Kind != searchjobs.ValueKindDouble || !page.Schema.Columns[2].Nullable {
			t.Fatalf("sum/avg schema = %#v", page.Schema)
		}
		if len(page.Rows) != 1 {
			t.Fatalf("sum/avg rows = %d, want 1", len(page.Rows))
		}
		count, countOK := page.Rows[0].Values[0].Unsigned()
		total, totalOK := page.Rows[0].Values[1].Double()
		mean, meanOK := page.Rows[0].Values[2].Double()
		if !countOK || count != 1 || !totalOK || total != 200 || !meanOK || mean != 200 {
			t.Fatalf("sum/avg row = %#v", page.Rows[0])
		}
	})

	t.Run("stats min and max mixed values through manager", func(t *testing.T) {
		job, page := queryIntegrationRunSearch(t, ctx, executor, eventIndexTime,
			"queryexec-stats-min-max",
			`index=main | stats min(status) AS low max(path) AS high min(absent) AS absent`,
		)
		if job.State != searchjobs.StateCompleted {
			t.Fatalf("min/max state = %v, failure=%#v", job.State, job.Failure)
		}
		if len(page.Schema.Columns) != 3 {
			t.Fatalf("min/max schema = %#v", page.Schema)
		}
		for index, name := range []string{"low", "high", "absent"} {
			column := page.Schema.Columns[index]
			if column.Name != name || column.Kind != searchjobs.ValueKindMixed ||
				!column.Nullable || column.Multivalue {
				t.Fatalf("min/max column %d = %#v, want nullable Mixed %q", index, column, name)
			}
		}
		if len(page.Rows) != 1 || len(page.Rows[0].Values) != 3 {
			t.Fatalf("min/max rows = %#v, want one three-cell row", page.Rows)
		}
		low, lowOK := page.Rows[0].Values[0].Double()
		high, highOK := page.Rows[0].Values[1].String()
		if !lowOK || low != 200 || !highOK || high != "/manager" ||
			!page.Rows[0].Values[2].IsNull() {
			t.Fatalf("min/max row = %#v, want Double(200)/String(/manager)/null", page.Rows[0])
		}
	})

	t.Run("stats earliest and latest canonical strings through manager", func(t *testing.T) {
		job, page := queryIntegrationRunGradeThisSearchRange(
			t,
			ctx,
			executor,
			gradeThisIndexTime,
			"queryexec-stats-earliest-latest",
			`index=gradethis | stats earliest(status) AS earliest_status latest(status) AS latest_status earliest(absent) AS absent`,
			gradeThisBase,
			gradeThisBase.Add(20*time.Minute),
		)
		if job.State != searchjobs.StateCompleted {
			t.Fatalf("earliest/latest state = %v, failure=%#v", job.State, job.Failure)
		}
		if len(page.Schema.Columns) != 3 {
			t.Fatalf("earliest/latest schema = %#v", page.Schema)
		}
		for index, name := range []string{"earliest_status", "latest_status", "absent"} {
			column := page.Schema.Columns[index]
			if column.Name != name || column.Kind != searchjobs.ValueKindMixed ||
				!column.Nullable || column.Multivalue {
				t.Fatalf(
					"earliest/latest column %d = %#v, want nullable Mixed %q",
					index,
					column,
					name,
				)
			}
		}
		if len(page.Rows) != 1 || len(page.Rows[0].Values) != 3 {
			t.Fatalf("earliest/latest rows = %#v, want one three-cell row", page.Rows)
		}
		earliestStatus, earliestOK := page.Rows[0].Values[0].String()
		latestStatus, latestOK := page.Rows[0].Values[1].String()
		if !earliestOK || earliestStatus != "503" ||
			!latestOK || latestStatus != "502" ||
			!page.Rows[0].Values[2].IsNull() {
			t.Fatalf(
				"earliest/latest row = %#v, want String(503)/String(502)/null",
				page.Rows[0],
			)
		}
	})

	t.Run("stats earliest and latest preserve binary raw through manager", func(t *testing.T) {
		job, page := queryIntegrationRunSearchRangeForIndex(
			t,
			ctx,
			executor,
			binaryIndexTime,
			"queryexec-stats-earliest-latest-binary",
			`index=binary | stats earliest(_raw) AS earliest_raw latest(_raw) AS latest_raw earliest(absent) AS absent`,
			binaryIndexTime.Add(-time.Hour),
			binaryIndexTime.Add(time.Hour),
			"binary",
		)
		if job.State != searchjobs.StateCompleted ||
			len(page.Schema.Columns) != 3 ||
			len(page.Rows) != 1 {
			t.Fatalf(
				"binary earliest/latest transport = state %v failure %#v schema %#v rows %#v",
				job.State,
				job.Failure,
				page.Schema,
				page.Rows,
			)
		}
		for index, name := range []string{"earliest_raw", "latest_raw", "absent"} {
			if page.Schema.Columns[index] != (searchjobs.Column{
				Name: name, Kind: searchjobs.ValueKindMixed, Nullable: true,
			}) {
				t.Fatalf("binary earliest/latest column %d = %#v", index, page.Schema.Columns[index])
			}
		}
		for _, index := range []int{0, 1} {
			raw, ok := page.Rows[0].Values[index].Bytes()
			if !ok || !bytes.Equal(raw, []byte{0xff, 0x00}) {
				t.Fatalf("binary earliest/latest cell %d = %#v (%x/%v), want ff00 Bytes", index, page.Rows[0].Values[index], raw, ok)
			}
		}
		if !page.Rows[0].Values[2].IsNull() {
			t.Fatalf("binary absent chronological value = %#v, want null", page.Rows[0].Values[2])
		}
	})

	t.Run("stats count field unsigned transport through manager", func(t *testing.T) {
		job, page := queryIntegrationRunSearch(t, ctx, executor, eventIndexTime,
			"queryexec-stats-count-field",
			`index=main | stats count count(status) AS statuses count(absent) AS absent`,
		)
		if job.State != searchjobs.StateCompleted {
			t.Fatalf("count(field) state = %v, failure=%#v", job.State, job.Failure)
		}
		if len(page.Schema.Columns) != 3 {
			t.Fatalf("count(field) schema = %#v", page.Schema)
		}
		for index, name := range []string{"count", "statuses", "absent"} {
			column := page.Schema.Columns[index]
			if column.Name != name || column.Kind != searchjobs.ValueKindUnsigned ||
				column.Nullable || column.Multivalue {
				t.Fatalf("count(field) column %d = %#v, want non-null unsigned %q", index, column, name)
			}
		}
		if len(page.Rows) != 1 {
			t.Fatalf("count(field) rows = %d, want 1", len(page.Rows))
		}
		for index, want := range []uint64{1, 1, 0} {
			got, ok := page.Rows[0].Values[index].Unsigned()
			if !ok || got != want {
				t.Fatalf("count(field) value %d = %d, %v, want %d", index, got, ok, want)
			}
		}
	})

	t.Run("rename dynamic field through manager", func(t *testing.T) {
		defaultJob, defaultPage := queryIntegrationRunSearch(t, ctx, executor, eventIndexTime,
			"queryexec-rename-default-output", `index=main | rename path AS route`,
		)
		if defaultJob.State != searchjobs.StateCompleted {
			t.Fatalf("rename default state = %v, failure=%#v", defaultJob.State, defaultJob.Failure)
		}
		for _, column := range defaultPage.Schema.Columns {
			if column.Name == "fields" {
				t.Fatalf("rename default schema leaked stale fields payload: %#v", defaultPage.Schema)
			}
		}
		routeColumn := queryIntegrationColumnIndex(t, defaultPage, "route")
		if len(defaultPage.Rows) != 1 {
			t.Fatalf("rename default rows = %d, want 1", len(defaultPage.Rows))
		}
		if route, ok := defaultPage.Rows[0].Values[routeColumn].String(); !ok || route != "/manager" {
			t.Fatalf("renamed default route = %q, %v", route, ok)
		}

		job, page := queryIntegrationRunSearch(t, ctx, executor, eventIndexTime,
			"queryexec-rename-dynamic",
			`index=main | rename path AS route, route AS endpoint | where endpoint="/manager" | table endpoint, status`,
		)
		if job.State != searchjobs.StateCompleted {
			t.Fatalf("rename state = %v, failure=%#v", job.State, job.Failure)
		}
		queryIntegrationAssertColumns(t, page, []string{"endpoint", "status"})
		if len(page.Rows) != 1 {
			t.Fatalf("rename rows = %d, want 1", len(page.Rows))
		}
		if endpoint, ok := page.Rows[0].Values[0].String(); !ok || endpoint != "/manager" {
			t.Fatalf("renamed endpoint = %q, %v", endpoint, ok)
		}
		if status, ok := page.Rows[0].Values[1].String(); !ok || status != "200" {
			t.Fatalf("status = %q, %v", status, ok)
		}
	})

	t.Run("rename overwrite and missing source through manager", func(t *testing.T) {
		overwriteJob, overwritePage := queryIntegrationRunSearch(t, ctx, executor, eventIndexTime,
			"queryexec-rename-overwrite",
			`index=main | stats count by status | rename status AS count`,
		)
		if overwriteJob.State != searchjobs.StateCompleted {
			t.Fatalf("rename overwrite state = %v, failure=%#v", overwriteJob.State, overwriteJob.Failure)
		}
		queryIntegrationAssertColumns(t, overwritePage, []string{"count"})
		if len(overwritePage.Rows) != 1 {
			t.Fatalf("rename overwrite rows = %d, want 1", len(overwritePage.Rows))
		}
		if count, ok := overwritePage.Rows[0].Values[0].String(); !ok || count != "200" {
			t.Fatalf("overwritten count = %q, %v", count, ok)
		}

		missingJob, missingPage := queryIntegrationRunSearch(t, ctx, executor, eventIndexTime,
			"queryexec-rename-missing",
			`index=main | stats count AS events by status | rename absent AS events`,
		)
		if missingJob.State != searchjobs.StateCompleted {
			t.Fatalf("rename missing state = %v, failure=%#v", missingJob.State, missingJob.Failure)
		}
		queryIntegrationAssertColumns(t, missingPage, []string{"status", "events"})
		if len(missingPage.Rows) != 1 || !missingPage.Rows[0].Values[1].IsNull() {
			t.Fatalf("rename missing page = %#v, want one row with null events", missingPage)
		}

		for _, test := range []struct {
			id     string
			source string
		}{
			{
				id:     "queryexec-rename-projected-away-source",
				source: `index=main | fields - logger | rename logger AS path | table path`,
			},
			{
				id:     "queryexec-rename-blocked-source",
				source: `index=main | rename logger AS component | rename logger AS path | table path`,
			},
		} {
			job, page := queryIntegrationRunSearch(t, ctx, executor, eventIndexTime, test.id, test.source)
			if job.State != searchjobs.StateCompleted {
				t.Fatalf("dynamic missing-source rename state = %v, failure=%#v", job.State, job.Failure)
			}
			queryIntegrationAssertColumns(t, page, []string{"path"})
			if len(page.Rows) != 1 || !page.Rows[0].Values[0].IsNull() {
				t.Fatalf("dynamic missing source resurrected stored destination: %#v", page)
			}
		}
	})

	t.Run("rename tombstones and canonical scan scope through manager", func(t *testing.T) {
		tombstoneJob, tombstonePage := queryIntegrationRunSearch(t, ctx, executor, eventIndexTime,
			"queryexec-rename-tombstones",
			`index=main | rename logger AS component | eval component="replacement" | table logger.child, component.child`,
		)
		if tombstoneJob.State != searchjobs.StateCompleted {
			t.Fatalf("rename tombstone state = %v, failure=%#v", tombstoneJob.State, tombstoneJob.Failure)
		}
		queryIntegrationAssertColumns(t, tombstonePage, []string{"logger.child", "component.child"})
		if len(tombstonePage.Rows) != 1 || !tombstonePage.Rows[0].Values[0].IsNull() || !tombstonePage.Rows[0].Values[1].IsNull() {
			t.Fatalf("rename tombstones exposed stored descendants: %#v", tombstonePage)
		}

		indexJob, indexPage := queryIntegrationRunSearch(t, ctx, executor, eventIndexTime,
			"queryexec-rename-calculated-index",
			`index=main | table path | rename path AS index | search index="/manager" | table index`,
		)
		if indexJob.State != searchjobs.StateCompleted {
			t.Fatalf("rename calculated index state = %v, failure=%#v", indexJob.State, indexJob.Failure)
		}
		queryIntegrationAssertColumns(t, indexPage, []string{"index"})
		if len(indexPage.Rows) != 1 {
			t.Fatalf("rename calculated index rows = %d, want 1", len(indexPage.Rows))
		}
		if value, ok := indexPage.Rows[0].Values[0].String(); !ok || value != "/manager" {
			t.Fatalf("calculated index = %q, %v", value, ok)
		}

		timeJob, timePage := queryIntegrationRunSearch(t, ctx, executor, eventIndexTime,
			"queryexec-rename-canonical-time",
			`index=main | table _time, path | rename _time AS observed_at | table observed_at, path`,
		)
		if timeJob.State != searchjobs.StateCompleted {
			t.Fatalf("rename canonical time state = %v, failure=%#v", timeJob.State, timeJob.Failure)
		}
		queryIntegrationAssertColumns(t, timePage, []string{"observed_at", "path"})
		if len(timePage.Rows) != 1 {
			t.Fatalf("rename canonical time rows = %d, want 1", len(timePage.Rows))
		}
		if observedAt, ok := timePage.Rows[0].Values[0].Time(); !ok || !observedAt.Equal(eventIndexTime) {
			t.Fatalf("renamed canonical time = %v, %v, want %v", observedAt, ok, eventIndexTime)
		}
	})

	t.Run("multi-field top pipeline through manager", func(t *testing.T) {
		job, page := queryIntegrationRunSearch(t, ctx, executor, eventIndexTime,
			"queryexec-top-pipeline", `index=main | top limit=20 path,status`,
		)
		if job.State != searchjobs.StateCompleted {
			t.Fatalf("top state = %v, failure=%#v", job.State, job.Failure)
		}
		queryIntegrationAssertColumns(
			t,
			page,
			[]string{"path", "status", "count", "percent"},
		)
		if page.Schema.Columns[0].Kind != searchjobs.ValueKindString ||
			page.Schema.Columns[1].Kind != searchjobs.ValueKindString ||
			page.Schema.Columns[2].Kind != searchjobs.ValueKindUnsigned ||
			page.Schema.Columns[3].Kind != searchjobs.ValueKindDouble {
			t.Fatalf("top schema = %#v", page.Schema)
		}
		if len(page.Rows) != 1 {
			t.Fatalf("top rows = %d, want 1", len(page.Rows))
		}
		if path, ok := page.Rows[0].Values[0].String(); !ok || path != "/manager" {
			t.Fatalf("top path = %q, %v", path, ok)
		}
		if statusValue, ok := page.Rows[0].Values[1].String(); !ok || statusValue != "200" {
			t.Fatalf("top status = %q, %v", statusValue, ok)
		}
		if count, ok := page.Rows[0].Values[2].Unsigned(); !ok || count != 1 {
			t.Fatalf("top count = %d, %v", count, ok)
		}
		if percent, ok := page.Rows[0].Values[3].Double(); !ok || percent != 100 {
			t.Fatalf("top percent = %v, %v", percent, ok)
		}
	})

	t.Run("eval percentile where pipeline through manager", func(t *testing.T) {
		job, page := queryIntegrationRunSearch(t, ctx, executor, eventIndexTime,
			"queryexec-slow-route-pipeline",
			`index=main | eval duration_ms=tonumber(replace(duration, "ms$", "")) | stats count p95(duration_ms) AS p95_ms BY path | where p95_ms>500`,
		)
		if job.State != searchjobs.StateCompleted {
			t.Fatalf("slow-route state = %v, failure=%#v", job.State, job.Failure)
		}
		if len(page.Schema.Columns) != 3 || page.Schema.Columns[0].Name != "path" ||
			page.Schema.Columns[0].Kind != searchjobs.ValueKindString ||
			page.Schema.Columns[1].Name != "count" || page.Schema.Columns[1].Kind != searchjobs.ValueKindUnsigned ||
			page.Schema.Columns[2].Name != "p95_ms" || page.Schema.Columns[2].Kind != searchjobs.ValueKindDouble ||
			!page.Schema.Columns[2].Nullable {
			t.Fatalf("slow-route schema = %#v", page.Schema)
		}
		if len(page.Rows) != 1 {
			t.Fatalf("slow-route rows = %d, want 1", len(page.Rows))
		}
		if path, ok := page.Rows[0].Values[0].String(); !ok || path != "/manager" {
			t.Fatalf("slow-route path = %q, %v", path, ok)
		}
		if count, ok := page.Rows[0].Values[1].Unsigned(); !ok || count != 1 {
			t.Fatalf("slow-route count = %d, %v", count, ok)
		}
		if percentile, ok := page.Rows[0].Values[2].Double(); !ok || percentile != 650 {
			t.Fatalf("slow-route p95 = %v, %v", percentile, ok)
		}
	})

	t.Run("percentile family through manager", func(t *testing.T) {
		job, page := queryIntegrationRunSearch(t, ctx, executor, eventIndexTime,
			"queryexec-percentile-family",
			`index=main | eval duration_ms=tonumber(replace(duration, "ms$", "")) | stats `+
				`p50(duration_ms) AS q50 p90(duration_ms) AS q90 `+
				`p95(duration_ms) AS q95 p99(duration_ms) AS q99 `+
				`perc75(duration_ms) AS q75`,
		)
		if job.State != searchjobs.StateCompleted {
			t.Fatalf("percentile family state = %v, failure=%#v", job.State, job.Failure)
		}
		queryIntegrationAssertColumns(t, page, []string{"q50", "q90", "q95", "q99", "q75"})
		if len(page.Rows) != 1 || len(page.Schema.Columns) != 5 {
			t.Fatalf("percentile family page = %#v", page)
		}
		for index, column := range page.Schema.Columns {
			if column.Kind != searchjobs.ValueKindDouble || !column.Nullable {
				t.Fatalf("percentile family column[%d] = %#v", index, column)
			}
			value, ok := page.Rows[0].Values[index].Double()
			if !ok || value != 650 {
				t.Fatalf("percentile family value[%d] = %v, %v, want 650", index, value, ok)
			}
		}
	})

	t.Run("timechart fixed range gaps and null series through manager", func(t *testing.T) {
		source := `index=main source="timechart-level" | timechart span=5m count by level`
		parsed, err := spl.Parse(source)
		if err != nil {
			t.Fatal(err)
		}
		visibility := uint64(1)
		logical, err := plan.Build(parsed, plan.Scope{
			TenantID:          "tenant",
			AuthorizedIndexes: []string{"main"},
			RequestedIndexes:  []string{"main"},
			Earliest:          timechartBase.Add(2 * time.Minute),
			Latest:            timechartBase.Add(18 * time.Minute),
			SearchStart:       timechartIndexTime,
			SearchTimezone:    "UTC",
			IndexTimeCutoff:   timechartIndexTime.Add(500 * time.Microsecond),
			VisibilityCutoff:  &visibility,
		})
		if err != nil {
			t.Fatal(err)
		}
		compiled, err := (clickhouse.Compiler{}).Compile(logical)
		if err != nil {
			t.Fatal(err)
		}
		if err := executor.Execute(ctx, compiled, &fakeSink{}); err != nil {
			t.Fatalf("execute compiled timechart: %v", err)
		}
		job, page := queryIntegrationRunSearchRange(
			t, ctx, executor, timechartIndexTime, "queryexec-timechart-fixed-range",
			source,
			timechartBase.Add(2*time.Minute), timechartBase.Add(18*time.Minute),
		)
		if job.State != searchjobs.StateCompleted {
			t.Fatalf("timechart state = %v, failure=%#v", job.State, job.Failure)
		}
		wantNames := []string{"_time", "ERROR", "WARN", "NULL"}
		if len(page.Schema.Columns) != len(wantNames) {
			t.Fatalf("timechart schema = %#v", page.Schema)
		}
		for index, name := range wantNames {
			column := page.Schema.Columns[index]
			wantKind := searchjobs.ValueKindUnsigned
			if index == 0 {
				wantKind = searchjobs.ValueKindTime
			}
			if column.Name != name || column.Kind != wantKind || column.Nullable || column.Multivalue {
				t.Fatalf("timechart column %d = %#v", index, column)
			}
		}
		wantCounts := [][]uint64{{2, 0, 0}, {0, 1, 0}, {0, 0, 1}, {0, 0, 0}}
		if len(page.Rows) != len(wantCounts) {
			t.Fatalf("timechart rows = %d, want %d", len(page.Rows), len(wantCounts))
		}
		for rowIndex, want := range wantCounts {
			bucket, ok := page.Rows[rowIndex].Values[0].Time()
			if !ok || !bucket.Equal(timechartBase.Add(time.Duration(rowIndex)*5*time.Minute)) {
				t.Fatalf("timechart row %d bucket = %v, %v", rowIndex, bucket, ok)
			}
			for columnIndex, count := range want {
				got, ok := page.Rows[rowIndex].Values[columnIndex+1].Unsigned()
				if !ok || got != count {
					t.Fatalf("timechart row %d column %d = %d, %v, want %d", rowIndex, columnIndex+1, got, ok, count)
				}
			}
		}
	})

	t.Run("timechart fixed range count without split through manager", func(t *testing.T) {
		job, page := queryIntegrationRunSearchRange(
			t, ctx, executor, timechartIndexTime, "queryexec-timechart-unsplit-count",
			`index=main source="timechart-level" level=* | timechart span=5m count`,
			timechartBase.Add(2*time.Minute), timechartBase.Add(18*time.Minute),
		)
		if job.State != searchjobs.StateCompleted {
			t.Fatalf("unsplit timechart state = %v, failure=%#v", job.State, job.Failure)
		}
		if len(page.Schema.Columns) != 2 ||
			page.Schema.Columns[0] != (searchjobs.Column{Name: "_time", Kind: searchjobs.ValueKindTime}) ||
			page.Schema.Columns[1] != (searchjobs.Column{Name: "count", Kind: searchjobs.ValueKindUnsigned}) {
			t.Fatalf("unsplit timechart schema = %#v", page.Schema)
		}
		queryIntegrationAssertTimechartMatrix(
			t,
			page,
			timechartBase,
			5*time.Minute,
			map[string][]uint64{"count": {2, 1, 0, 0}},
		)
	})

	t.Run("timechart count without split keeps static schema on empty input", func(t *testing.T) {
		job, page := queryIntegrationRunSearchRange(
			t, ctx, executor, timechartIndexTime, "queryexec-timechart-unsplit-empty",
			`index=main source="timechart-empty" | timechart span=5m count`,
			timechartBase, timechartBase.Add(11*time.Minute),
		)
		if job.State != searchjobs.StateCompleted {
			t.Fatalf("empty unsplit timechart state = %v, failure=%#v", job.State, job.Failure)
		}
		if len(page.Schema.Columns) != 2 ||
			page.Schema.Columns[0] != (searchjobs.Column{Name: "_time", Kind: searchjobs.ValueKindTime}) ||
			page.Schema.Columns[1] != (searchjobs.Column{Name: "count", Kind: searchjobs.ValueKindUnsigned}) ||
			len(page.Rows) != 0 {
			t.Fatalf("empty unsplit timechart page = %#v", page)
		}
	})

	t.Run("timechart reconstructs a bucket before the DateTime64 storage minimum", func(t *testing.T) {
		earliest := time.Date(1900, time.January, 1, 0, 0, 0, 0, time.UTC)
		job, page := queryIntegrationRunSearchRange(
			t, ctx, executor, timechartIndexTime, "queryexec-timechart-storage-floor",
			`index=main source="timechart-storage-floor" | timechart span=7h count by level`,
			earliest, earliest.Add(time.Hour),
		)
		if job.State != searchjobs.StateCompleted {
			t.Fatalf("lower-bound timechart state = %v, failure=%#v", job.State, job.Failure)
		}
		queryIntegrationAssertTimechartMatrix(t, page,
			time.Date(1899, time.December, 31, 19, 0, 0, 0, time.UTC),
			7*time.Hour,
			map[string][]uint64{"ERROR": {1}},
		)
		fixedJob, fixedPage := queryIntegrationRunSearchRange(
			t, ctx, executor, timechartIndexTime, "queryexec-timechart-storage-floor-fixed",
			`index=main source="timechart-storage-floor" | timechart span=7h count`,
			earliest, earliest.Add(time.Hour),
		)
		if fixedJob.State != searchjobs.StateCompleted {
			t.Fatalf("lower-bound fixed timechart state = %v, failure=%#v", fixedJob.State, fixedJob.Failure)
		}
		queryIntegrationAssertTimechartMatrix(t, fixedPage,
			time.Date(1899, time.December, 31, 19, 0, 0, 0, time.UTC),
			7*time.Hour,
			map[string][]uint64{"count": {1}},
		)
	})

	t.Run("timechart dynamic top ten null other and lexical ties", func(t *testing.T) {
		job, page := queryIntegrationRunSearchRange(
			t, ctx, executor, timechartIndexTime, "queryexec-timechart-top",
			`index=main source="timechart-top" | timechart span=5m count by path`,
			timechartBase, timechartBase.Add(5*time.Minute),
		)
		if job.State != searchjobs.StateCompleted {
			t.Fatalf("timechart top state = %v, failure=%#v", job.State, job.Failure)
		}
		wantNames := []string{"_time", "a", "b", "c", "d", "e", "f", "g", "h", "hot", "i", "NULL", "OTHER"}
		if len(page.Schema.Columns) != len(wantNames) {
			t.Fatalf("timechart top schema = %#v", page.Schema)
		}
		for index, name := range wantNames {
			if page.Schema.Columns[index].Name != name {
				t.Fatalf("timechart top column %d = %q, want %q", index, page.Schema.Columns[index].Name, name)
			}
		}
		if len(page.Rows) != 1 {
			t.Fatalf("timechart top rows = %d, want 1", len(page.Rows))
		}
		wantCounts := []uint64{1, 1, 1, 1, 1, 1, 1, 1, 3, 1, 2, 2}
		for index, want := range wantCounts {
			got, ok := page.Rows[0].Values[index+1].Unsigned()
			if !ok || got != want {
				t.Fatalf("timechart top count %q = %d, %v, want %d", wantNames[index+1], got, ok, want)
			}
		}
	})

	t.Run("timechart leading underscore series uses VALUE prefix and public lexical order", func(t *testing.T) {
		compiled := queryIntegrationCompileSearchRange(
			t,
			`index=main source="timechart-underscore" | timechart span=5m count by path`,
			timechartIndexTime,
			timechartBase,
			timechartBase.Add(5*time.Minute),
		)
		if err := executor.Execute(ctx, compiled, &fakeSink{}); err != nil {
			t.Fatalf("execute underscore timechart directly: %v", err)
		}
		job, page := queryIntegrationRunSearchRange(
			t, ctx, executor, timechartIndexTime, "queryexec-timechart-underscore",
			`index=main source="timechart-underscore" | timechart span=5m count by path`,
			timechartBase, timechartBase.Add(5*time.Minute),
		)
		wantNames := []string{"_time", "VALUE_audit", "Z"}
		if job.State != searchjobs.StateCompleted || len(page.Schema.Columns) != len(wantNames) || len(page.Rows) != 1 {
			t.Fatalf("underscore timechart state=%v failure=%+v page=%#v", job.State, job.Failure, page)
		}
		for index, want := range wantNames {
			if page.Schema.Columns[index].Name != want {
				t.Fatalf("underscore column %d = %q, want %q", index, page.Schema.Columns[index].Name, want)
			}
		}
		for index := 1; index < len(wantNames); index++ {
			if count, ok := page.Rows[0].Values[index].Unsigned(); !ok || count != 1 {
				t.Fatalf("underscore count %q = %d, %v", wantNames[index], count, ok)
			}
		}
	})

	t.Run("timechart normalization collision outside top ten fails atomically", func(t *testing.T) {
		compiled := queryIntegrationCompileSearchRange(
			t,
			`index=main source="timechart-collision-outside-top" | timechart span=5m count by path`,
			timechartIndexTime,
			timechartBase,
			timechartBase.Add(5*time.Minute),
		)
		sink := &fakeSink{}
		err := executor.Execute(ctx, compiled, sink)
		if !errors.Is(err, searchjobs.ErrUnsupportedValue) || len(sink.schema.Columns) != 0 || len(sink.rows) != 0 {
			t.Fatalf("execute outside-top normalization collision directly: err=%v schema=%#v rows=%d", err, sink.schema, len(sink.rows))
		}
		job, _ := queryIntegrationRunSearchRange(
			t, ctx, executor, timechartIndexTime, "queryexec-timechart-collision-outside-top",
			`index=main source="timechart-collision-outside-top" | timechart span=5m count by path`,
			timechartBase, timechartBase.Add(5*time.Minute),
		)
		if job.State != searchjobs.StateFailed || job.Failure == nil ||
			job.Failure.Code != searchjobs.FailureUnsupportedSPL || job.RowCount != 0 || job.Schema != nil {
			t.Fatalf("outside-top normalization collision state=%v failure=%+v rows=%d schema=%#v", job.State, job.Failure, job.RowCount, job.Schema)
		}
	})

	t.Run("timechart projected split field becomes null series", func(t *testing.T) {
		job, page := queryIntegrationRunSearchRange(
			t, ctx, executor, timechartIndexTime, "queryexec-timechart-projected",
			`index=main source="timechart-level" | fields host | timechart span=5m count by path`,
			timechartBase.Add(2*time.Minute), timechartBase.Add(18*time.Minute),
		)
		if job.State != searchjobs.StateCompleted || len(page.Schema.Columns) != 2 || page.Schema.Columns[1].Name != "NULL" || len(page.Rows) != 4 {
			t.Fatalf("projected timechart job=%#v page=%#v", job, page)
		}
		for index, want := range []uint64{2, 1, 1, 0} {
			got, ok := page.Rows[index].Values[1].Unsigned()
			if !ok || got != want {
				t.Fatalf("projected timechart row %d = %d, %v, want %d", index, got, ok, want)
			}
		}
	})

	t.Run("timechart empty input publishes only schema", func(t *testing.T) {
		job, page := queryIntegrationRunSearchRange(
			t, ctx, executor, timechartIndexTime, "queryexec-timechart-empty",
			`index=main source="timechart-empty" | timechart span=5m count by path`,
			timechartBase, timechartBase.Add(11*time.Minute),
		)
		if job.State != searchjobs.StateCompleted || len(page.Schema.Columns) != 1 || page.Schema.Columns[0].Name != "_time" || len(page.Rows) != 0 {
			t.Fatalf("empty timechart job=%#v page=%#v", job, page)
		}
	})

	t.Run("timechart unsupported domains fail atomically", func(t *testing.T) {
		for _, fixture := range []string{"timechart-invalid", "timechart-list", "timechart-object", "timechart-collision"} {
			job, _ := queryIntegrationRunSearchRange(
				t, ctx, executor, timechartIndexTime, "queryexec-"+fixture,
				`index=main source="`+fixture+`" | timechart span=5m count by path`,
				timechartBase, timechartBase.Add(5*time.Minute),
			)
			if job.State != searchjobs.StateFailed || job.Failure == nil || job.Failure.Code != searchjobs.FailureUnsupportedSPL || job.RowCount != 0 || job.Schema != nil {
				t.Fatalf("unsupported %q job = %#v", fixture, job)
			}
		}
	})

	t.Run("timechart supports two dense series over five thousand buckets", func(t *testing.T) {
		const bucketCount = uint64(5_001)
		const names = "CAST(['0:a', '0:b'], 'Array(String)')"
		query := clickhouse.CompiledQuery{
			SQL: `WITH
"__os_dense_groups" AS MATERIALIZED
(
    SELECT
        number % 5001 AS bucket_number,
        if(number < 5001, '0:a', '0:b') AS series,
        count() AS total
    FROM numbers(10002)
    GROUP BY bucket_number, series
),
"__os_dense_maps" AS
(
    SELECT
        bucket_number,
        mapFromArrays(groupArray(series), groupArray(total)) AS series_counts
    FROM "__os_dense_groups"
    GROUP BY bucket_number
)
SELECT
    toUInt64(grid.number) AS "__os_timechart_ordinal",
    if(grid.number = 0, ` + names + `, CAST([], 'Array(String)')) AS "__os_timechart_names",
    arrayMap(series -> ifNull("__os_dense_maps".series_counts[series], toUInt64(0)), ` + names + `) AS "__os_timechart_counts",
    toUInt8(0) AS "__os_timechart_invalid"
FROM numbers(5001) AS grid
LEFT JOIN "__os_dense_maps" ON "__os_dense_maps".bucket_number = grid.number
ORDER BY grid.number`,
			OutputFields: []string{"_time"},
			Timechart: &clickhouse.TimechartOutput{
				FirstBucket:   time.Unix(0, 0).UTC(),
				Span:          time.Second,
				BucketCount:   bucketCount,
				MaxSeries:     2,
				MaxLabelBytes: 256,
			},
		}
		sink := &fakeSink{}
		if err := diagnosticExecutor.Execute(ctx, query, sink); err != nil {
			t.Fatalf("execute dense timechart: %v", err)
		}
		wantNames := []string{"_time", "a", "b"}
		if len(sink.schema.Columns) != len(wantNames) || len(sink.rows) != int(bucketCount) {
			t.Fatalf("dense timechart schema=%#v rows=%d", sink.schema, len(sink.rows))
		}
		for index, want := range wantNames {
			if sink.schema.Columns[index].Name != want {
				t.Fatalf("dense timechart column %d = %q, want %q", index, sink.schema.Columns[index].Name, want)
			}
		}
		for _, rowIndex := range []int{0, int(bucketCount / 2), int(bucketCount - 1)} {
			bucket, ok := sink.rows[rowIndex][0].Time()
			if !ok || !bucket.Equal(time.Unix(int64(rowIndex), 0).UTC()) {
				t.Fatalf("dense timechart row %d bucket = %v, %v", rowIndex, bucket, ok)
			}
			for column := 1; column <= 2; column++ {
				count, countOK := sink.rows[rowIndex][column].Unsigned()
				if !countOK || count != 1 {
					t.Fatalf("dense timechart row %d column %d = %d, %v", rowIndex, column, count, countOK)
				}
			}
		}

		explicitlyBounded := queryIntegrationDiagnosticExecutor(
			t,
			connection,
			Config{MaxRowsToGroupBy: 10_001},
		)
		boundedSink := &fakeSink{}
		err = explicitlyBounded.Execute(ctx, query, boundedSink)
		if !errors.Is(err, searchjobs.ErrExecutionLimit) || boundedSink.setCalls != 0 || len(boundedSink.rows) != 0 {
			t.Fatalf("explicit dense timechart cap: err=%v schema calls=%d rows=%d", err, boundedSink.setCalls, len(boundedSink.rows))
		}
	})

	// chart is the bounded runtime-wide pivot. Unlike timechart both of its
	// axes are runtime data, so the executor decodes the public schema from
	// the result itself; these cases pin the decoded schema, not the wire.
	chartRange := func(t *testing.T, id, source string) (searchjobs.Job, searchjobs.ResultPage) {
		t.Helper()
		return queryIntegrationRunSearchRange(
			t, ctx, executor, chartIndexTime, id, source, chartBase, chartBase.Add(30*time.Minute),
		)
	}

	t.Run("chart publishes the anchor pivot for both accepted spellings", func(t *testing.T) {
		wantNames := []string{"path", "ERROR", "INFO", "NULL"}
		wantCounts := [][]uint64{{1, 2, 0}, {1, 0, 1}}
		wantRows := []string{"/a", "/b"}
		for index, source := range []string{
			`index=main source="chart-r5" | chart count OVER path BY level`,
			`index=main source="chart-r5" | chart count BY path, level`,
		} {
			job, page := chartRange(t, fmt.Sprintf("queryexec-chart-r5-%d", index), source)
			if job.State != searchjobs.StateCompleted {
				t.Fatalf("%q state = %v, failure=%#v", source, job.State, job.Failure)
			}
			queryIntegrationAssertColumns(t, page, wantNames)
			for column, name := range wantNames {
				got := page.Schema.Columns[column]
				want := searchjobs.ValueKindUnsigned
				if column == 0 {
					want = searchjobs.ValueKindString
				}
				if got.Kind != want || got.Nullable || got.Multivalue {
					t.Fatalf("%q column %q = %#v", source, name, got)
				}
			}
			if len(page.Rows) != len(wantCounts) {
				t.Fatalf("%q rows = %d, want %d", source, len(page.Rows), len(wantCounts))
			}
			for rowIndex, counts := range wantCounts {
				label, ok := page.Rows[rowIndex].Values[0].String()
				if !ok || label != wantRows[rowIndex] {
					t.Fatalf("%q row %d label = %q, %v", source, rowIndex, label, ok)
				}
				for columnIndex, want := range counts {
					got, ok := page.Rows[rowIndex].Values[columnIndex+1].Unsigned()
					if !ok || got != want {
						t.Fatalf("%q row %d column %q = %d, %v, want %d",
							source, rowIndex, wantNames[columnIndex+1], got, ok, want)
					}
				}
			}
		}
	})

	t.Run("chart bounds the column axis with null and other", func(t *testing.T) {
		job, page := chartRange(t, "queryexec-chart-c8",
			`index=main source="chart-c8" | chart count OVER path BY series`)
		if job.State != searchjobs.StateCompleted {
			t.Fatalf("bounded chart state = %v, failure=%#v", job.State, job.Failure)
		}
		// Exactly thirteen public columns: the row column, the ten retained
		// ordinary labels, then NULL and OTHER in that order.
		queryIntegrationAssertColumns(t, page, []string{
			"path", "c01", "c02", "c03", "c04", "c05", "c06", "c07", "c08", "c09", "c12", "NULL", "OTHER",
		})
		wantCounts := [][]uint64{
			{3, 2, 1, 1, 1, 1, 1, 1, 1, 0, 2, 2},
			{1, 0, 0, 0, 0, 0, 0, 0, 0, 4, 1, 0},
			{0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0},
		}
		for rowIndex, want := range wantCounts {
			for columnIndex, count := range want {
				got, ok := page.Rows[rowIndex].Values[columnIndex+1].Unsigned()
				if !ok || got != count {
					t.Fatalf("bounded chart row %d column %d = %d, %v, want %d", rowIndex, columnIndex+1, got, ok, count)
				}
			}
		}
	})

	t.Run("numeric chart publishes nullable values through manager", func(t *testing.T) {
		wantColumns := []string{
			"path", "s01", "s02", "s03", "s04", "s05", "s06",
			"s07", "s08", "s09", "s10", "NULL", "OTHER",
		}
		for _, test := range []struct {
			name       string
			aggregate  string
			otherValue float64
		}{
			{name: "sum", aggregate: "sum", otherValue: 50},
			{name: "member weighted average", aggregate: "avg", otherValue: 50.0 / 21.0},
			{name: "p95 pooled raw state", aggregate: "p95", otherValue: 1},
			{name: "perc50 pooled raw state", aggregate: "perc50", otherValue: 1},
		} {
			t.Run(test.name, func(t *testing.T) {
				job, page := chartRange(
					t,
					"queryexec-chart-numeric-"+strings.ReplaceAll(test.name, " ", "-"),
					`index=main source="chart-numeric" | chart `+test.aggregate+`(metric) OVER path BY series`,
				)
				if job.State != searchjobs.StateCompleted || job.RowCount != 3 {
					t.Fatalf("numeric %s chart job = %#v, failure=%#v", test.aggregate, job, job.Failure)
				}
				queryIntegrationAssertColumns(t, page, wantColumns)
				if page.Schema.Columns[0] != (searchjobs.Column{Name: "path", Kind: searchjobs.ValueKindString}) {
					t.Fatalf("numeric %s chart row column = %#v", test.aggregate, page.Schema.Columns[0])
				}
				for columnIndex, column := range page.Schema.Columns[1:] {
					if column.Kind != searchjobs.ValueKindDouble || !column.Nullable || column.Multivalue {
						t.Fatalf(
							"numeric %s chart value column %d = %#v, want nullable scalar Double",
							test.aggregate,
							columnIndex+1,
							column,
						)
					}
				}
				if len(page.Rows) != 3 {
					t.Fatalf("numeric %s chart rows = %d, want 3", test.aggregate, len(page.Rows))
				}
				for rowIndex, wantRow := range []string{"/empty", "/weighted", "/zero"} {
					gotRow, ok := page.Rows[rowIndex].Values[0].String()
					if !ok || gotRow != wantRow {
						t.Fatalf(
							"numeric %s chart row %d = %q, %v, want %q",
							test.aggregate,
							rowIndex,
							gotRow,
							ok,
							wantRow,
						)
					}
				}

				// A valid row domain is independent of numeric measure eligibility.
				// The row survives, while every cell publishes as an explicit null.
				for columnIndex, value := range page.Rows[0].Values[1:] {
					if !value.IsNull() {
						t.Fatalf(
							"numeric %s all-ineligible cell %q = %#v, want null",
							test.aggregate,
							wantColumns[columnIndex+1],
							value,
						)
					}
				}

				weighted := page.Rows[1].Values
				for index := 1; index <= 10; index++ {
					queryIntegrationAssertDouble(
						t,
						weighted[index],
						float64(101-index),
						fmt.Sprintf("numeric %s s%02d", test.aggregate, index),
					)
				}
				queryIntegrationAssertDouble(t, weighted[11], 7, "numeric "+test.aggregate+" NULL")
				queryIntegrationAssertDouble(
					t,
					weighted[12],
					test.otherValue,
					"numeric "+test.aggregate+" weighted OTHER",
				)

				// The value-presence bitmap must keep a real zero distinct from
				// absent or wholly ineligible cells in the same public schema.
				zero := page.Rows[2].Values
				queryIntegrationAssertDouble(t, zero[1], 0, "numeric "+test.aggregate+" real zero")
				for columnIndex, value := range zero[2:] {
					if !value.IsNull() {
						t.Fatalf(
							"numeric %s zero-row cell %q = %#v, want null",
							test.aggregate,
							wantColumns[columnIndex+2],
							value,
						)
					}
				}
			})
		}

		// Numeric aggregation cannot make a malformed split domain safe. The
		// executor buffers the complete pivot and rejects it before publishing.
		const invalidSource = `index=main source="chart-bad-rowname" | chart sum(metric) OVER path BY series`
		compiled := queryIntegrationCompileSearchRange(
			t,
			invalidSource,
			chartIndexTime,
			chartBase,
			chartBase.Add(30*time.Minute),
		)
		sink := &fakeSink{}
		err := executor.Execute(ctx, compiled, sink)
		if !errors.Is(err, searchjobs.ErrUnsupportedValue) || sink.setCalls != 0 || len(sink.rows) != 0 {
			t.Fatalf(
				"invalid numeric chart: err=%v schema calls=%d rows=%d",
				err,
				sink.setCalls,
				len(sink.rows),
			)
		}
		job, _ := chartRange(t, "queryexec-chart-numeric-atomic", invalidSource)
		if job.State != searchjobs.StateFailed || job.Failure == nil ||
			job.Failure.Code != searchjobs.FailureUnsupportedSPL || job.RowCount != 0 || job.Schema != nil {
			t.Fatalf("invalid numeric chart job = %#v, failure=%+v", job, job.Failure)
		}
	})

	t.Run("chart row totals equal stats count by the row field", func(t *testing.T) {
		// The primary differential: usenull and useother are always on, so the
		// column bound can never drop an eligible input row.
		_, pivot := chartRange(t, "queryexec-chart-differential-pivot",
			`index=main source="chart-c8" | chart count OVER path BY series`)
		_, grouped := chartRange(t, "queryexec-chart-differential-stats",
			`index=main source="chart-c8" | stats count BY path | sort 0 +path`)
		if len(pivot.Rows) != len(grouped.Rows) || len(pivot.Rows) == 0 {
			t.Fatalf("pivot published %d rows, stats published %d", len(pivot.Rows), len(grouped.Rows))
		}
		if pivot.Schema.Columns[0].Name != grouped.Schema.Columns[0].Name ||
			pivot.Schema.Columns[0].Kind != grouped.Schema.Columns[0].Kind {
			t.Fatalf("pivot row column = %#v, stats group column = %#v",
				pivot.Schema.Columns[0], grouped.Schema.Columns[0])
		}
		for index, row := range pivot.Rows {
			pivotLabel, pivotOK := row.Values[0].String()
			statsLabel, statsOK := grouped.Rows[index].Values[0].String()
			if !pivotOK || !statsOK || pivotLabel != statsLabel {
				t.Fatalf("row %d label = %q (%v), stats reports %q (%v)", index, pivotLabel, pivotOK, statsLabel, statsOK)
			}
			var total uint64
			for _, value := range row.Values[1:] {
				count, ok := value.Unsigned()
				if !ok {
					t.Fatalf("row %q published a non-unsigned cell: %#v", pivotLabel, value)
				}
				total += count
			}
			want, ok := grouped.Rows[index].Values[1].Unsigned()
			if !ok || total != want {
				t.Fatalf("row %q cells sum to %d, stats count BY path reports %d (%v)", pivotLabel, total, want, ok)
			}
		}
	})

	t.Run("chart leading underscore column uses the VALUE prefix", func(t *testing.T) {
		job, page := chartRange(t, "queryexec-chart-underscore",
			`index=main source="chart-underscore" | chart count OVER path BY series`)
		if job.State != searchjobs.StateCompleted {
			t.Fatalf("underscore chart state = %v, failure=%#v", job.State, job.Failure)
		}
		// Publication order uses the normalized name, so VALUE_audit follows Z.
		queryIntegrationAssertColumns(t, page, []string{"path", "VALUE_audit", "Z"})
	})

	t.Run("chart row column equals the stats group column", func(t *testing.T) {
		for _, test := range []struct {
			name     string
			field    string
			prefix   string
			wantKind searchjobs.ValueKind
		}{
			{name: "fixed string", field: "host", wantKind: searchjobs.ValueKindString},
			{name: "runtime typed", field: "path", wantKind: searchjobs.ValueKindString},
			{name: "fixed numeric", field: "severity", wantKind: searchjobs.ValueKindUnsigned},
			{
				name: "binned timestamp", field: "bucket_time",
				prefix: `| bin _time span=5m AS bucket_time `, wantKind: searchjobs.ValueKindTime,
			},
		} {
			t.Run(test.name, func(t *testing.T) {
				scope := `index=main source="chart-kinds" ` + test.prefix
				_, pivot := chartRange(t, "queryexec-chart-kind-"+test.field,
					scope+`| chart count OVER `+test.field+` BY level`)
				_, grouped := chartRange(t, "queryexec-chart-kind-stats-"+test.field,
					scope+`| stats count BY `+test.field+` | sort 0 +`+test.field)
				if len(pivot.Schema.Columns) == 0 || len(grouped.Schema.Columns) == 0 {
					t.Fatalf("pivot=%#v stats=%#v", pivot.Schema, grouped.Schema)
				}
				column := pivot.Schema.Columns[0]
				if column.Name != test.field || column.Kind != test.wantKind || column.Nullable || column.Multivalue {
					t.Fatalf("pivot row column = %#v, want %q/%v", column, test.field, test.wantKind)
				}
				if column.Kind != grouped.Schema.Columns[0].Kind || column.Name != grouped.Schema.Columns[0].Name {
					t.Fatalf("pivot row column %#v diverged from the stats group column %#v",
						column, grouped.Schema.Columns[0])
				}
				if len(pivot.Rows) != len(grouped.Rows) || len(pivot.Rows) == 0 {
					t.Fatalf("pivot rows = %d, stats rows = %d", len(pivot.Rows), len(grouped.Rows))
				}
				for index, row := range pivot.Rows {
					if !queryIntegrationSameValue(row.Values[0], grouped.Rows[index].Values[0]) {
						t.Fatalf("row %d = %#v, stats reports %#v", index, row.Values[0], grouped.Rows[index].Values[0])
					}
				}
			})
		}
	})

	t.Run("chart unsupported axis values fail atomically", func(t *testing.T) {
		for _, test := range []struct{ name, source string }{
			{"numeric column value", `index=main source="chart-bad-number" | chart count OVER path BY series`},
			{"container row value", `index=main source="chart-bad-row-list" | chart count OVER series BY level`},
			{"label equal to the row column name", `index=main source="chart-bad-rowname" | chart count OVER path BY series`},
			{"normalization collision", `index=main source="chart-bad-collision" | chart count OVER path BY series`},
		} {
			t.Run(test.name, func(t *testing.T) {
				compiled := queryIntegrationCompileSearchRange(
					t, test.source, chartIndexTime, chartBase, chartBase.Add(30*time.Minute))
				sink := &fakeSink{}
				err := executor.Execute(ctx, compiled, sink)
				// No schema and no row may be published: the pivot is buffered
				// and validated before publication precisely so this holds.
				if !errors.Is(err, searchjobs.ErrUnsupportedValue) || sink.setCalls != 0 || len(sink.rows) != 0 {
					t.Fatalf("execute %q: err=%v schema calls=%d rows=%d", test.source, err, sink.setCalls, len(sink.rows))
				}
				job, _ := chartRange(t, "queryexec-chart-atomic-"+strings.ReplaceAll(test.name, " ", "-"), test.source)
				if job.State != searchjobs.StateFailed || job.Failure == nil ||
					job.Failure.Code != searchjobs.FailureUnsupportedSPL || job.RowCount != 0 || job.Schema != nil {
					t.Fatalf("%q job = %#v failure=%+v", test.source, job, job.Failure)
				}
				if strings.Contains(job.Failure.Message, "series") || strings.Contains(job.Failure.Message, "path") {
					t.Fatalf("%q failure message leaked a field value: %q", test.source, job.Failure.Message)
				}
			})
		}
	})

	t.Run("chart projected and empty relations", func(t *testing.T) {
		// I2: the column field is gone for every row, so one NULL column
		// carries each row's whole total.
		job, page := chartRange(t, "queryexec-chart-projected-column",
			`index=main source="chart-r5" | fields path | chart count OVER path BY level`)
		if job.State != searchjobs.StateCompleted {
			t.Fatalf("projected-column chart state = %v, failure=%#v", job.State, job.Failure)
		}
		queryIntegrationAssertColumns(t, page, []string{"path", "NULL"})
		for index, want := range []uint64{3, 2} {
			got, ok := page.Rows[index].Values[1].Unsigned()
			if !ok || got != want {
				t.Fatalf("projected-column row %d = %d, %v, want %d", index, got, ok, want)
			}
		}

		// I3 and I4: without a row value there is no group, so the pivot
		// publishes its declared one-column schema and no rows at all — a
		// grouped chart never emits the global row stats count would emit.
		for _, source := range []string{
			`index=main source="chart-r5" | fields host | chart count OVER path BY level`,
			`index=main source="chart-nothing" | chart count OVER path BY level`,
		} {
			emptyJob, emptyPage := chartRange(t, "queryexec-chart-empty-"+strconv.Itoa(len(source)), source)
			if emptyJob.State != searchjobs.StateCompleted {
				t.Fatalf("%q state = %v, failure=%#v", source, emptyJob.State, emptyJob.Failure)
			}
			queryIntegrationAssertColumns(t, emptyPage, []string{"path"})
			if len(emptyPage.Rows) != 0 || emptyJob.RowCount != 0 {
				t.Fatalf("%q published %d rows (job reports %d)", source, len(emptyPage.Rows), emptyJob.RowCount)
			}
		}
	})

	t.Run("chart transport is bounded at ten thousand rows", func(t *testing.T) {
		// The row axis is runtime data, so the executor cannot re-derive it
		// from the plan: it must accept a dense bounded pivot and reject one
		// row more without publishing anything.
		denseChart := func(rows uint64) clickhouse.CompiledQuery {
			const names = "CAST(['0:a', '1:'], 'Array(String)')"
			return clickhouse.CompiledQuery{
				SQL: `SELECT toUInt64(number) AS "__os_chart_ordinal", ` +
					`CAST(leftPad(toString(number), 8, '0') AS String) AS "__os_chart_row", ` +
					names + ` AS "__os_chart_names", ` +
					`CAST([toUInt64(1), toUInt64(0)], 'Array(UInt64)') AS "__os_chart_counts", ` +
					`toUInt8(0) AS "__os_chart_invalid" FROM numbers(` +
					strconv.FormatUint(rows, 10) + `) ORDER BY number`,
				OutputFields: []string{"row_value"},
				Chart: &clickhouse.ChartOutput{
					RowField:        "row_value",
					RowKind:         clickhouse.ChartRowKindString,
					RowDatabaseType: "String",
					RowLimit:        10_000,
					MaxSeries:       12,
					MaxLabelBytes:   256,
					ValueKind:       clickhouse.ChartValueKindCount,
				},
			}
		}
		sink := &fakeSink{}
		if err := diagnosticExecutor.Execute(ctx, denseChart(10_000), sink); err != nil {
			t.Fatalf("execute dense chart: %v", err)
		}
		if len(sink.rows) != 10_000 || len(sink.schema.Columns) != 3 ||
			sink.schema.Columns[0].Name != "row_value" || sink.schema.Columns[0].Kind != searchjobs.ValueKindString ||
			sink.schema.Columns[1].Name != "a" || sink.schema.Columns[2].Name != "NULL" {
			t.Fatalf("dense chart schema=%#v rows=%d", sink.schema, len(sink.rows))
		}
		overflowSink := &fakeSink{}
		err := diagnosticExecutor.Execute(ctx, denseChart(10_001), overflowSink)
		if !errors.Is(err, searchjobs.ErrExecutionLimit) || overflowSink.setCalls != 0 || len(overflowSink.rows) != 0 {
			t.Fatalf("dense chart overflow: err=%v schema calls=%d rows=%d",
				err, overflowSink.setCalls, len(overflowSink.rows))
		}
	})

	t.Run("stats aliases retain aggregate types", func(t *testing.T) {
		for _, alias := range []string{"fields", "_raw"} {
			job, page := queryIntegrationRunSearch(t, ctx, executor, eventIndexTime, "queryexec-stats-alias-"+alias, `index=main | stats count AS `+alias)
			if job.State != searchjobs.StateCompleted {
				t.Fatalf("alias %q state = %v, failure=%#v", alias, job.State, job.Failure)
			}
			if len(page.Schema.Columns) != 1 || page.Schema.Columns[0].Name != alias || page.Schema.Columns[0].Kind != searchjobs.ValueKindUnsigned {
				t.Fatalf("alias %q schema = %#v", alias, page.Schema)
			}
			if len(page.Rows) != 1 {
				t.Fatalf("alias %q rows = %d, want 1", alias, len(page.Rows))
			}
			if count, ok := page.Rows[0].Values[0].Unsigned(); !ok || count != 1 {
				t.Fatalf("alias %q count = %d, %v", alias, count, ok)
			}
		}
	})

	t.Run("stats values retains typed multivalue transport", func(t *testing.T) {
		job, page := queryIntegrationRunSearch(
			t,
			ctx,
			executor,
			eventIndexTime,
			"queryexec-stats-values",
			`index=main | stats values(status) AS statuses`,
		)
		if job.State != searchjobs.StateCompleted {
			t.Fatalf("values state = %v, failure=%#v", job.State, job.Failure)
		}
		wantValuesColumn := searchjobs.Column{
			Name:                       "statuses",
			Kind:                       searchjobs.ValueKindList,
			Multivalue:                 true,
			FlatMultivalueDelimiter:    spl.DefaultStatsDelimiter,
			HasFlatMultivalueDelimiter: true,
		}
		if len(page.Schema.Columns) != 1 || page.Schema.Columns[0] != wantValuesColumn {
			t.Fatalf("values schema = %#v", page.Schema)
		}
		if len(page.Rows) != 1 {
			t.Fatalf("values rows = %d, want 1", len(page.Rows))
		}
		values, ok := page.Rows[0].Values[0].List()
		if !ok || len(values) != 1 {
			t.Fatalf("values cell = %#v", page.Rows[0].Values[0])
		}
		if status, stringOK := values[0].String(); !stringOK || status != "200" {
			t.Fatalf("values status = %q/%v, want 200", status, stringOK)
		}

		binaryJob, binaryPage := queryIntegrationRunSearchRangeForIndex(
			t,
			ctx,
			executor,
			binaryIndexTime,
			"queryexec-stats-values-binary",
			`index=binary | stats values(_raw) AS binary_values`,
			binaryIndexTime.Add(-time.Hour),
			binaryIndexTime.Add(time.Hour),
			"binary",
		)
		if binaryJob.State != searchjobs.StateCompleted {
			t.Fatalf("binary values state = %v, failure=%#v", binaryJob.State, binaryJob.Failure)
		}
		wantBinaryValuesColumn := wantValuesColumn
		wantBinaryValuesColumn.Name = "binary_values"
		if len(binaryPage.Schema.Columns) != 1 ||
			binaryPage.Schema.Columns[0] != wantBinaryValuesColumn ||
			len(binaryPage.Rows) != 1 {
			t.Fatalf("binary values transport = schema %#v rows %#v", binaryPage.Schema, binaryPage.Rows)
		}
		binaryValues, listOK := binaryPage.Rows[0].Values[0].List()
		if !listOK || len(binaryValues) != 0 {
			t.Fatalf("binary values cell = %#v, want empty text-eligible multivalue", binaryPage.Rows[0].Values[0])
		}
		binaryMaxJob, binaryMaxPage := queryIntegrationRunSearchRangeForIndex(
			t,
			ctx,
			executor,
			binaryIndexTime,
			"queryexec-stats-max-binary",
			`index=binary | stats max(_raw) AS binary_max`,
			binaryIndexTime.Add(-time.Hour),
			binaryIndexTime.Add(time.Hour),
			"binary",
		)
		if binaryMaxJob.State != searchjobs.StateCompleted ||
			len(binaryMaxPage.Schema.Columns) != 1 ||
			binaryMaxPage.Schema.Columns[0] != (searchjobs.Column{
				Name: "binary_max", Kind: searchjobs.ValueKindMixed, Nullable: true,
			}) ||
			len(binaryMaxPage.Rows) != 1 {
			t.Fatalf(
				"binary max transport = state %v failure %#v schema %#v rows %#v",
				binaryMaxJob.State,
				binaryMaxJob.Failure,
				binaryMaxPage.Schema,
				binaryMaxPage.Rows,
			)
		}
		if raw, bytesOK := binaryMaxPage.Rows[0].Values[0].Bytes(); !bytesOK || !bytes.Equal(raw, []byte{0xff, 0x00}) {
			t.Fatalf("binary max cell = %x/%v", raw, bytesOK)
		}
		for _, test := range []struct {
			name   string
			filter string
			rows   int
		}{
			{name: "equality", filter: `search binary_values=missing`, rows: 0},
			{name: "wildcard", filter: `search binary_values="m*"`, rows: 0},
			{name: "inequality", filter: `search binary_values!=missing`, rows: 0},
			{name: "presence", filter: `search binary_values=*`, rows: 0},
		} {
			t.Run("binary "+test.name, func(t *testing.T) {
				job, page := queryIntegrationRunSearchRangeForIndex(
					t,
					ctx,
					executor,
					binaryIndexTime,
					"queryexec-stats-values-binary-"+test.name,
					`index=binary | stats values(_raw) AS binary_values | `+test.filter,
					binaryIndexTime.Add(-time.Hour),
					binaryIndexTime.Add(time.Hour),
					"binary",
				)
				if job.State != searchjobs.StateCompleted || len(page.Rows) != test.rows {
					t.Fatalf("%s state = %v, failure=%#v, rows=%d, want %d", test.name, job.State, job.Failure, len(page.Rows), test.rows)
				}
			})
		}
	})

	t.Run("stats projection boundary retains no hidden dynamic field", func(t *testing.T) {
		job, page := queryIntegrationRunSearch(t, ctx, executor, eventIndexTime, "queryexec-stats-projected", `index=main | fields host | stats count by status`)
		if job.State != searchjobs.StateCompleted {
			t.Fatalf("projected stats state = %v, failure=%#v", job.State, job.Failure)
		}
		if len(page.Schema.Columns) != 2 || page.Schema.Columns[0].Name != "status" || page.Schema.Columns[1].Name != "count" {
			t.Fatalf("projected stats schema = %#v", page.Schema)
		}
		if len(page.Rows) != 0 {
			t.Fatalf("projected-away status emitted %d rows", len(page.Rows))
		}

		retainedJob, retainedPage := queryIntegrationRunSearch(t, ctx, executor, eventIndexTime, "queryexec-stats-retained", `index=main | fields status | stats count by status`)
		if retainedJob.State != searchjobs.StateCompleted || len(retainedPage.Rows) != 1 {
			t.Fatalf("retained stats job=%#v page=%#v", retainedJob, retainedPage)
		}
		if status, ok := retainedPage.Rows[0].Values[0].String(); !ok || status != "200" {
			t.Fatalf("retained status = %q, %v", status, ok)
		}
	})

	t.Run("non-scalar markers are safely classified", func(t *testing.T) {
		for _, marker := range []string{
			clickhouse.UnsupportedStatsByValueMarker,
			clickhouse.UnsupportedStatsMeasureValueMarker,
			clickhouse.UnsupportedDedupValueMarker,
			clickhouse.UnsupportedNumericBinValueMarker,
			clickhouse.UnsupportedSpathValueMarker,
		} {
			err := diagnosticExecutor.Execute(ctx, clickhouse.CompiledQuery{
				SQL:          `SELECT throwIf(toUInt8(1), '` + marker + `') AS impossible`,
				OutputFields: []string{"impossible"},
			}, &fakeSink{})
			if !errors.Is(err, searchjobs.ErrUnsupportedValue) || strings.Contains(err.Error(), marker) {
				t.Fatalf("non-scalar marker %q classification = %v", marker, err)
			}
		}
	})

	t.Run("runtime resource limits are safely classified", func(t *testing.T) {
		for _, marker := range []string{
			clickhouse.RexCaptureLimitMarker,
			clickhouse.SpathInputLimitMarker,
			clickhouse.SpathJSONTokenLimitMarker,
			clickhouse.EventStatsInputLimitMarker,
			clickhouse.StreamStatsInputLimitMarker,
			clickhouse.ExactDistinctLimitMarker,
			clickhouse.StatsValuesBytesLimitMarker,
			clickhouse.StatsValuesLimitMarker,
			clickhouse.EventStatsValuesBytesLimitMarker,
			clickhouse.EventStatsValuesLimitMarker,
			clickhouse.EventStatsListBytesLimitMarker,
			clickhouse.EventStatsListLimitMarker,
			clickhouse.StatsListBytesLimitMarker,
			clickhouse.StatsListLimitMarker,
			clickhouse.KnowledgeAliasCopyEventLimitMarker,
			clickhouse.KnowledgeAliasCopyQueryLimitMarker,
		} {
			err := diagnosticExecutor.Execute(ctx, clickhouse.CompiledQuery{
				SQL: `SELECT throwIf(toUInt8(1), '` + marker +
					`') AS impossible`,
				OutputFields: []string{"impossible"},
			}, &fakeSink{})
			if !errors.Is(err, searchjobs.ErrExecutionLimit) ||
				strings.Contains(err.Error(), marker) {
				t.Fatalf("runtime resource limit %q classification = %v", marker, err)
			}
		}
	})

	t.Run("group cardinality is bounded before result streaming", func(t *testing.T) {
		bounded := queryIntegrationDiagnosticExecutor(
			t,
			connection,
			Config{MaxRowsToGroupBy: 1},
		)
		for _, query := range []clickhouse.CompiledQuery{
			{
				SQL:          "SELECT count() AS total FROM numbers(2)",
				OutputFields: []string{"total"},
			},
			{
				SQL:          "SELECT number % 1 AS bucket FROM numbers(2) GROUP BY bucket",
				OutputFields: []string{"bucket"},
			},
		} {
			if err := bounded.Execute(ctx, query, &fakeSink{}); err != nil {
				t.Fatalf("query at group boundary: %v", err)
			}
		}
		sink := &fakeSink{}
		err = bounded.Execute(ctx, clickhouse.CompiledQuery{
			SQL:          "SELECT number FROM numbers(2) GROUP BY number ORDER BY number",
			OutputFields: []string{"number"},
		}, sink)
		if !errors.Is(err, searchjobs.ErrExecutionLimit) {
			t.Fatalf("group cardinality error = %v", err)
		}
		if len(sink.rows) != 0 {
			t.Fatalf("group cardinality limit streamed %d rows", len(sink.rows))
		}
	})

	t.Run("cancellation reaches native query", func(t *testing.T) {
		cancelCtx, cancelQuery := context.WithTimeout(ctx, 150*time.Millisecond)
		defer cancelQuery()
		started := time.Now()
		err := diagnosticExecutor.Execute(cancelCtx, clickhouse.CompiledQuery{
			SQL:          "SELECT sleep(2) AS waited",
			OutputFields: []string{"waited"},
		}, &fakeSink{})
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("canceled query error = %v", err)
		}
		if elapsed := time.Since(started); elapsed > 2*time.Second {
			t.Fatalf("canceled query returned after %v", elapsed)
		}
	})
}

func queryIntegrationDiagnosticExecutor(
	t *testing.T,
	connection queryConnection,
	config Config,
) *Executor {
	t.Helper()
	settings, err := querySettings(config)
	if err != nil {
		t.Fatal(err)
	}
	return &Executor{
		connection:                connection,
		settings:                  mustValidatedSettings(t, settings),
		expandTimechartGroupLimit: config.MaxRowsToGroupBy == 0 || config.ExpandTimechartGroupLimit,
		newQueryID:                randomQueryID,
		withProgress:              clickhousedriver.WithProgress,
	}
}

func queryIntegrationAssertStructuredExplain(
	t *testing.T,
	result ExplainResult,
) ExplainPlan {
	t.Helper()
	physical, err := ParseExplainPlan(result)
	if err != nil {
		t.Fatalf("parse structured EXPLAIN: %v", err)
	}
	if !slices.Contains(physical.NodeTypes, "ReadFromMergeTree") ||
		len(physical.Reads) == 0 {
		t.Fatalf("structured EXPLAIN has no MergeTree read: %#v", physical)
	}
	for readIndex, read := range physical.Reads {
		if len(read.Columns) == 0 || len(read.Indexes) == 0 {
			t.Fatalf(
				"structured EXPLAIN read %d lacks headers or indexes: %#v",
				readIndex,
				read,
			)
		}
		types := make([]string, len(read.Indexes))
		for index, evidence := range read.Indexes {
			types[index] = evidence.Type
		}
		if !slices.Contains(types, "MinMax") ||
			!slices.Contains(types, "PrimaryKey") {
			t.Fatalf(
				"structured EXPLAIN read %d index types = %v",
				readIndex,
				types,
			)
		}
	}
	return physical
}

type queryIntegrationSnapshotter uint64

func (snapshotter queryIntegrationSnapshotter) VisibilityCutoff(ctx context.Context) (uint64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	return uint64(snapshotter), nil
}

func queryIntegrationInsertEvent(t *testing.T, ctx context.Context, connection clickhousedriver.Conn) time.Time {
	t.Helper()
	query := "INSERT INTO open_splunk.events (event_id, tenant_id, index_name, event_time, index_time, " +
		"collected_at, event_time_source, host, source, sourcetype, service, severity, level, body, raw, " +
		"raw_encoding, trace_id, span_id, fields, field_names, field_types, field_metadata_version, " +
		"collector_id, batch_id, batch_sequence, " +
		"expires_at, visibility_seq)"
	batch, err := connection.PrepareBatch(ctx, query)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Add(-time.Minute).Truncate(time.Second).Add(987 * time.Millisecond)
	message := "manager integration"
	fields := []struct {
		name       string
		value      any
		storedType eventfields.StoredValueType
	}{
		{"component.child", clickhousedriver.NewDynamic("stored-target-child"), eventfields.StoredValueTypeString},
		{"duration", clickhousedriver.NewDynamic("650ms"), eventfields.StoredValueTypeString},
		{"logger.child", clickhousedriver.NewDynamic("stored-source-child"), eventfields.StoredValueTypeString},
		{"path", clickhousedriver.NewDynamic("/manager"), eventfields.StoredValueTypeString},
		{"payload", clickhousedriver.NewDynamic(`{"items":[{"sku":"sku-1"}]}`), eventfields.StoredValueTypeString},
		{"status", clickhousedriver.NewDynamic("200"), eventfields.StoredValueTypeString},
		{"typed_bytes", queryIntegrationExtendedValue("bytes/v1", "AP8"), eventfields.StoredValueTypeBytes},
		{"typed_decimal", queryIntegrationExtendedValue("decimal/v1", "-123.4500e+2"), eventfields.StoredValueTypeDecimal},
		{"typed_duration", queryIntegrationExtendedValue("duration/v1", "-12:-345000000"), eventfields.StoredValueTypeDuration},
		{"typed_timestamp", queryIntegrationExtendedValue("timestamp/v1", now.Add(42*time.Second).Format(time.RFC3339Nano)), eventfields.StoredValueTypeTimestamp},
	}
	document := clickhousedriver.NewJSON()
	fieldNames := make([]string, len(fields))
	fieldTypes := make([]uint8, len(fields))
	for index, field := range fields {
		document.SetValueAtPath(field.name, field.value)
		fieldNames[index] = field.name
		fieldTypes[index] = uint8(field.storedType)
	}
	if err := batch.Append(
		"queryexec-event", "tenant", "main", now, now,
		nil, uint8(1), "host", "source", "test", nil, uint8(1), nil, &message, []byte(message),
		uint8(1), nil, nil, document,
		fieldNames,
		fieldTypes,
		eventfields.CurrentFieldMetadataVersion,
		"collector", "batch", uint64(1),
		now.Add(24*time.Hour), uint64(1),
	); err != nil {
		t.Fatal(err)
	}
	if err := batch.Send(); err != nil {
		t.Fatal(err)
	}
	return now
}

func queryIntegrationInsertBinaryEvent(t *testing.T, ctx context.Context, connection clickhousedriver.Conn) time.Time {
	t.Helper()
	query := "INSERT INTO open_splunk.events (event_id, tenant_id, index_name, event_time, index_time, " +
		"collected_at, event_time_source, host, source, sourcetype, service, severity, level, body, raw, " +
		"raw_encoding, trace_id, span_id, fields, field_names, collector_id, batch_id, batch_sequence, " +
		"expires_at, visibility_seq)"
	batch, err := connection.PrepareBatch(ctx, query)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Add(-time.Minute).Truncate(time.Second).Add(987 * time.Millisecond)
	if err := batch.Append(
		"queryexec-binary-event", "tenant", "binary", now, now,
		nil, uint8(1), "host", "binary", "test", nil, uint8(1), nil, nil, []byte{0xff, 0x00},
		uint8(2), nil, nil, clickhousedriver.NewJSON(), []string{},
		"collector", "binary-batch", uint64(1),
		now.Add(24*time.Hour), uint64(1),
	); err != nil {
		t.Fatal(err)
	}
	if err := batch.Send(); err != nil {
		t.Fatal(err)
	}
	return now
}

func queryIntegrationInsertGradeThisEvents(
	t *testing.T,
	ctx context.Context,
	connection clickhousedriver.Conn,
) (time.Time, time.Time, string) {
	t.Helper()
	traceID := gradethiscorpus.Fixture().TraceID
	query := "INSERT INTO open_splunk.events (event_id, tenant_id, index_name, event_time, index_time, " +
		"collected_at, event_time_source, host, source, sourcetype, service, severity, level, body, raw, " +
		"raw_encoding, trace_id, span_id, fields, field_names, collector_id, batch_id, batch_sequence, " +
		"expires_at, visibility_seq)"
	batch, err := connection.PrepareBatch(ctx, query)
	if err != nil {
		t.Fatal(err)
	}
	base := time.Now().UTC().Add(-3 * time.Hour).Truncate(5 * time.Minute)
	indexTime := time.Now().UTC().Truncate(time.Millisecond)
	type fixtureEvent struct {
		id       string
		offset   time.Duration
		level    string
		layer    string
		logger   string
		message  string
		traceID  string
		path     string
		status   int64
		duration string
	}
	events := []fixtureEvent{
		{
			id: "trace-start", offset: time.Minute, level: "INFO", layer: "api", logger: "request",
			message: "request started", traceID: traceID,
		},
		{
			id: "trace-error", offset: 2 * time.Minute, level: "ERROR", layer: "storage", logger: "database",
			message: "connection refused", traceID: traceID,
		},
		{
			id: "warning", offset: 4 * time.Minute, level: "WARN", layer: "worker", logger: "dependency",
			message: "dependency warning",
		},
		{
			id: "slow-503", offset: 6 * time.Minute, level: "ERROR", layer: "api", logger: "http",
			message: "Request metrics", path: "/slow", status: 503, duration: "800ms",
		},
		{
			id: "slow-500", offset: 7 * time.Minute, level: "ERROR", layer: "api", logger: "http",
			message: "Request metrics", path: "/slow", status: 500, duration: "700ms",
		},
		{
			id: "fast-200", offset: 8 * time.Minute, level: "INFO", layer: "api", logger: "http",
			message: "Request metrics", path: "/fast", status: 200, duration: "120ms",
		},
		{
			id: "fast-502", offset: 11 * time.Minute, level: "ERROR", layer: "api", logger: "http",
			message: "Request metrics", path: "/fast", status: 502, duration: "150ms",
		},
		{
			id: "heartbeat-a", offset: 12 * time.Minute, level: "INFO", layer: "worker", logger: "health",
			message: "heartbeat",
		},
		{
			id: "heartbeat-b", offset: 13 * time.Minute, level: "INFO", layer: "worker", logger: "health",
			message: "heartbeat",
		},
	}
	for index, event := range events {
		document := clickhousedriver.NewJSON()
		document.SetValueAtPath("layer", clickhousedriver.NewDynamic(event.layer))
		document.SetValueAtPath("logger", clickhousedriver.NewDynamic(event.logger))
		fieldNames := []string{"layer", "logger"}
		if event.path != "" {
			document.SetValueAtPath("duration", clickhousedriver.NewDynamic(event.duration))
			document.SetValueAtPath("path", clickhousedriver.NewDynamic(event.path))
			document.SetValueAtPath("status", clickhousedriver.NewDynamic(event.status))
			fieldNames = []string{"duration", "layer", "logger", "path", "status"}
		}
		var eventTraceID *string
		if event.traceID != "" {
			value := event.traceID
			eventTraceID = &value
		}
		eventTime := base.Add(event.offset)
		message := event.message
		level := event.level
		var service *string
		if event.id != "trace-error" {
			value := "gradethis"
			service = &value
		}
		raw := fmt.Sprintf(
			`{"level":%q,"layer":%q,"logger":%q,"message":%q}`,
			event.level, event.layer, event.logger, event.message,
		)
		if err := batch.Append(
			"queryexec-gradethis-"+event.id, "tenant", "gradethis", eventTime, indexTime,
			nil, uint8(1), "gradethis-host", "app.log", "zap:json", service, uint8(1), &level, &message, []byte(raw),
			uint8(1), eventTraceID, nil, document, fieldNames, "collector", "gradethis-batch", uint64(index+1),
			indexTime.Add(24*time.Hour), uint64(1),
		); err != nil {
			t.Fatal(err)
		}
	}
	if err := batch.Send(); err != nil {
		t.Fatal(err)
	}
	return base, indexTime, traceID
}

func queryIntegrationTestFixedPercentileTimechart(
	t *testing.T,
	ctx context.Context,
	connection clickhousedriver.Conn,
	executor *Executor,
	base time.Time,
	indexTime time.Time,
) {
	t.Helper()

	earliest := base.Add(2 * time.Minute)
	latest := base.Add(18 * time.Minute)
	const percentileSource = `index=main source="timechart-percentile" | timechart span=5m p95(metric) AS p95_metric`
	compiled := queryIntegrationCompileSearchRange(
		t,
		percentileSource,
		indexTime,
		earliest,
		latest,
	)
	if got := strings.Count(compiled.SQL, "quantilesGKOrNullArray("); got != 1 ||
		strings.Contains(strings.ToUpper(compiled.SQL), "ARRAY JOIN") ||
		strings.Count(compiled.SQL, `FROM "open_splunk"."events"`) != 1 {
		t.Fatalf("percentile timechart compiler shape is not one bounded scoped state:\n%s", compiled.SQL)
	}

	actions := queryIntegrationExplainActions(t, ctx, connection, compiled)
	physicalStates := 0
	for line := range strings.SplitSeq(actions, "\n") {
		if strings.Contains(line, "Function:") && strings.Contains(line, "quantilesGK") {
			physicalStates++
		}
	}
	if physicalStates != 1 || strings.Contains(actions, "ArrayJoin") {
		t.Fatalf(
			"percentile timechart physical plan has %d GK states or expands rows:\n%s",
			physicalStates,
			actions,
		)
	}
	t.Run("fixed percentile normalization gaps and scope through manager", func(t *testing.T) {
		job, page := queryIntegrationRunSearchRange(
			t,
			ctx,
			executor,
			indexTime,
			"queryexec-timechart-percentile",
			percentileSource,
			earliest,
			latest,
		)
		if job.State != searchjobs.StateCompleted {
			t.Fatalf("percentile timechart state = %v, failure=%#v", job.State, job.Failure)
		}
		if len(page.Schema.Columns) != 2 ||
			page.Schema.Columns[0] != (searchjobs.Column{Name: "_time", Kind: searchjobs.ValueKindTime}) ||
			page.Schema.Columns[1] != (searchjobs.Column{
				Name: "p95_metric", Kind: searchjobs.ValueKindDouble, Nullable: true,
			}) {
			t.Fatalf("percentile timechart schema = %#v", page.Schema)
		}
		if len(page.Rows) != 4 {
			t.Fatalf("percentile timechart rows = %d, want 4", len(page.Rows))
		}
		wantValues := map[int]float64{0: 100, 2: 300, 3: 400}
		for rowIndex, row := range page.Rows {
			bucket, ok := row.Values[0].Time()
			wantBucket := base.Add(time.Duration(rowIndex) * 5 * time.Minute)
			if !ok || !bucket.Equal(wantBucket) {
				t.Fatalf("percentile timechart row %d bucket = %v, %v, want %v", rowIndex, bucket, ok, wantBucket)
			}
			wantValue, populated := wantValues[rowIndex]
			if !populated {
				if !row.Values[1].IsNull() {
					t.Fatalf("percentile timechart gap = %#v, want null", row.Values[1])
				}
				continue
			}
			value, ok := row.Values[1].Double()
			if !ok || value != wantValue {
				t.Fatalf("percentile timechart row %d value = %v, %v, want %v", rowIndex, value, ok, wantValue)
			}
		}

		// The populated buckets must agree exactly with stats over the same
		// fixed bins. This is the durable normalization oracle for native,
		// lexical, decimal, and multivalue metric representations.
		statsJob, statsPage := queryIntegrationRunSearchRange(
			t,
			ctx,
			executor,
			indexTime,
			"queryexec-timechart-percentile-stats-oracle",
			`index=main source="timechart-percentile" | bin _time span=5m AS bucket | stats p95(metric) AS p95_metric BY bucket | sort bucket`,
			earliest,
			latest,
		)
		if statsJob.State != searchjobs.StateCompleted {
			t.Fatalf("stats oracle state = %v, failure=%#v", statsJob.State, statsJob.Failure)
		}
		if len(statsPage.Rows) != len(wantValues) {
			t.Fatalf("stats oracle rows = %d, want %d", len(statsPage.Rows), len(wantValues))
		}
		statsByBucket := make(map[int64]float64, len(statsPage.Rows))
		for _, row := range statsPage.Rows {
			bucket, bucketOK := row.Values[0].Time()
			value, valueOK := row.Values[1].Double()
			if !bucketOK || !valueOK {
				t.Fatalf("stats oracle row = %#v", row)
			}
			statsByBucket[bucket.Unix()] = value
		}
		for rowIndex, wantValue := range wantValues {
			bucket := base.Add(time.Duration(rowIndex) * 5 * time.Minute)
			if got, ok := statsByBucket[bucket.Unix()]; !ok || got != wantValue {
				t.Fatalf("stats oracle bucket %v = %v, %v, want %v", bucket, got, ok, wantValue)
			}
		}
	})

	t.Run("fixed percentile nonempty all-ineligible input keeps null grid", func(t *testing.T) {
		job, page := queryIntegrationRunSearchRange(
			t,
			ctx,
			executor,
			indexTime,
			"queryexec-timechart-percentile-ineligible",
			`index=main source="timechart-percentile-ineligible" | timechart span=5m p95(metric) AS p95_metric`,
			earliest,
			latest,
		)
		if job.State != searchjobs.StateCompleted || len(page.Rows) != 4 {
			t.Fatalf("all-ineligible percentile job=%#v page=%#v", job, page)
		}
		for index, row := range page.Rows {
			if !row.Values[1].IsNull() {
				t.Fatalf("all-ineligible percentile row %d = %#v, want null", index, row.Values[1])
			}
		}
	})

	t.Run("fixed percentile empty input publishes static schema only", func(t *testing.T) {
		job, page := queryIntegrationRunSearchRange(
			t,
			ctx,
			executor,
			indexTime,
			"queryexec-timechart-percentile-empty",
			`index=main source="timechart-percentile-empty" | timechart span=5m p95(metric) AS p95_metric`,
			earliest,
			latest,
		)
		if job.State != searchjobs.StateCompleted || len(page.Rows) != 0 ||
			len(page.Schema.Columns) != 2 || page.Schema.Columns[1] != (searchjobs.Column{
			Name: "p95_metric", Kind: searchjobs.ValueKindDouble, Nullable: true,
		}) {
			t.Fatalf("empty percentile job=%#v page=%#v", job, page)
		}
	})
}

func queryIntegrationTestFixedNumericTimechart(
	t *testing.T,
	ctx context.Context,
	connection clickhousedriver.Conn,
	executor *Executor,
	explainer *Explainer,
	base time.Time,
	indexTime time.Time,
) {
	t.Helper()

	earliest := base.Add(2 * time.Minute)
	latest := base.Add(18 * time.Minute)
	type measureCase struct {
		name              string
		function          string
		alias             string
		physicalAggregate string
		valueKind         clickhouse.TimechartValueKind
		first             float64
	}
	measures := []measureCase{
		{
			name: "sum", function: "sum", alias: "total_metric",
			physicalAggregate: "sumOrNullArray",
			valueKind:         clickhouse.TimechartValueKindSum, first: 23.5,
		},
		{
			name: "average", function: "avg", alias: "mean_metric",
			physicalAggregate: "avgOrNullArray",
			valueKind:         clickhouse.TimechartValueKindAverage, first: 23.5 / 7,
		},
	}

	for _, test := range measures {
		t.Run(test.name+" normalization gaps scope and physical shape", func(t *testing.T) {
			source := `index=main source="timechart-numeric" | timechart span=5m ` +
				test.function + `(metric) AS ` + test.alias
			compiled := queryIntegrationCompileSearchRange(
				t,
				source,
				indexTime,
				earliest,
				latest,
			)
			if compiled.Timechart == nil ||
				compiled.Timechart.Mode != clickhouse.TimechartModeFixedValue ||
				compiled.Timechart.ValueKind != test.valueKind ||
				compiled.Timechart.ValueField != test.alias ||
				compiled.Timechart.BucketCount != 4 ||
				compiled.Timechart.BucketCount > 10_000 {
				t.Fatalf("compiled %s timechart contract = %#v", test.name, compiled.Timechart)
			}
			upperSQL := strings.ToUpper(compiled.SQL)
			if strings.Count(compiled.SQL, `FROM "open_splunk"."events"`) != 1 ||
				!strings.Contains(compiled.SQL, `"__os_timechart_value_groups" AS MATERIALIZED (SELECT `) ||
				strings.Count(compiled.SQL, ` GROUP BY `) != 1 ||
				strings.Count(compiled.SQL, test.physicalAggregate+"(") != 1 ||
				strings.Contains(upperSQL, "ARRAY JOIN") {
				t.Fatalf(
					"%s timechart is not one bounded materialized bucket aggregation:\n%s",
					test.name,
					compiled.SQL,
				)
			}

			explained, err := explainer.Explain(ctx, compiled)
			if err != nil {
				t.Fatalf("EXPLAIN %s timechart: %v\nSQL: %s\nargs: %#v", test.name, err, compiled.SQL, compiled.Args)
			}
			physical := queryIntegrationAssertStructuredExplain(t, explained)
			if len(physical.Reads) != 1 || queryIntegrationPlanContainsArrayJoin(physical) {
				t.Fatalf("%s timechart physical plan rescans or expands rows: %#v", test.name, physical)
			}
			actions := queryIntegrationExplainActions(t, ctx, connection, compiled)
			physicalStates := 0
			for line := range strings.SplitSeq(actions, "\n") {
				if strings.Contains(line, "Function:") &&
					strings.Contains(line, test.physicalAggregate) {
					physicalStates++
				}
			}
			if physicalStates != 1 || strings.Contains(actions, "ArrayJoin") {
				t.Fatalf("%s timechart EXPLAIN actions violate the aggregate contract:\n%s", test.name, actions)
			}

			job, page := queryIntegrationRunSearchRange(
				t,
				ctx,
				executor,
				indexTime,
				"queryexec-timechart-numeric-"+test.name,
				source,
				earliest,
				latest,
			)
			queryIntegrationAssertFixedValueSchema(t, job, page, test.alias)
			if len(page.Rows) != 4 {
				t.Fatalf("%s timechart rows = %d, want 4", test.name, len(page.Rows))
			}
			for rowIndex, row := range page.Rows {
				bucket, ok := row.Values[0].Time()
				wantBucket := base.Add(time.Duration(rowIndex) * 5 * time.Minute)
				if !ok || !bucket.Equal(wantBucket) {
					t.Fatalf("%s row %d bucket = %v, %v, want %v", test.name, rowIndex, bucket, ok, wantBucket)
				}
				switch rowIndex {
				case 0:
					queryIntegrationAssertDouble(t, row.Values[1], test.first, test.name+" normalized first bucket")
				case 1:
					queryIntegrationAssertDouble(t, row.Values[1], 0, test.name+" zero-sum bucket")
				case 2, 3:
					if !row.Values[1].IsNull() {
						t.Fatalf("%s row %d = %#v, want null", test.name, rowIndex, row.Values[1])
					}
				}
			}

			statsJob, statsPage := queryIntegrationRunSearchRange(
				t,
				ctx,
				executor,
				indexTime,
				"queryexec-timechart-numeric-stats-"+test.name,
				`index=main source="timechart-numeric" | bin _time span=5m AS bucket | stats `+
					test.function+`(metric) AS `+test.alias+` BY bucket | sort bucket`,
				earliest,
				latest,
			)
			if statsJob.State != searchjobs.StateCompleted {
				t.Fatalf("%s stats oracle state = %v, failure=%#v", test.name, statsJob.State, statsJob.Failure)
			}
			queryIntegrationAssertFixedValueStatsParity(t, page, statsPage, test.alias)
		})
	}

	for _, test := range measures {
		t.Run(test.name+" all-ineligible empty and projected-away inputs", func(t *testing.T) {
			for _, input := range []struct {
				name     string
				source   string
				wantRows int
			}{
				{
					name: "all ineligible",
					source: `index=main source="timechart-percentile-ineligible" | timechart span=5m ` +
						test.function + `(metric) AS ` + test.alias,
					wantRows: 4,
				},
				{
					name: "empty",
					source: `index=main source="timechart-numeric-empty" | timechart span=5m ` +
						test.function + `(metric) AS ` + test.alias,
					wantRows: 0,
				},
				{
					name: "projected away",
					source: `index=main source="timechart-numeric" | fields - metric | timechart span=5m ` +
						test.function + `(metric) AS ` + test.alias,
					wantRows: 4,
				},
			} {
				t.Run(input.name, func(t *testing.T) {
					job, page := queryIntegrationRunSearchRange(
						t,
						ctx,
						executor,
						indexTime,
						"queryexec-timechart-numeric-"+test.name+"-"+strings.ReplaceAll(input.name, " ", "-"),
						input.source,
						earliest,
						latest,
					)
					queryIntegrationAssertFixedValueSchema(t, job, page, test.alias)
					if len(page.Rows) != input.wantRows {
						t.Fatalf("%s %s rows = %d, want %d", test.name, input.name, len(page.Rows), input.wantRows)
					}
					for rowIndex, row := range page.Rows {
						if !row.Values[1].IsNull() {
							t.Fatalf("%s %s row %d = %#v, want null", test.name, input.name, rowIndex, row.Values[1])
						}
					}
				})
			}
		})
	}

	for _, test := range measures {
		t.Run(test.name+" canonical timestamp matches stats", func(t *testing.T) {
			alias := "epoch_" + test.name
			job, page := queryIntegrationRunSearchRange(
				t,
				ctx,
				executor,
				indexTime,
				"queryexec-timechart-time-"+test.name,
				`index=main source="timechart-numeric" | timechart span=5m `+
					test.function+`(_time) AS `+alias,
				earliest,
				latest,
			)
			queryIntegrationAssertFixedValueSchema(t, job, page, alias)
			statsJob, statsPage := queryIntegrationRunSearchRange(
				t,
				ctx,
				executor,
				indexTime,
				"queryexec-timechart-time-stats-"+test.name,
				`index=main source="timechart-numeric" | bin _time span=5m AS bucket | stats `+
					test.function+`(_time) AS `+alias+` BY bucket | sort bucket`,
				earliest,
				latest,
			)
			if statsJob.State != searchjobs.StateCompleted {
				t.Fatalf("%s timestamp stats oracle state = %v, failure=%#v", test.name, statsJob.State, statsJob.Failure)
			}
			queryIntegrationAssertFixedValueStatsParity(t, page, statsPage, alias)
		})
	}

	for _, test := range measures {
		t.Run(test.name+" preserves computed nonfinite result", func(t *testing.T) {
			alias := "overflow_" + test.name
			job, page := queryIntegrationRunSearchRange(
				t,
				ctx,
				executor,
				indexTime,
				"queryexec-timechart-overflow-"+test.name,
				`index=main source="timechart-numeric-overflow" | timechart span=5m `+
					test.function+`(metric) AS `+alias,
				earliest,
				latest,
			)
			queryIntegrationAssertFixedValueSchema(t, job, page, alias)
			if len(page.Rows) != 4 {
				t.Fatalf("%s overflow rows = %d, want 4", test.name, len(page.Rows))
			}
			value, ok := page.Rows[0].Values[1].Double()
			if !ok || !math.IsInf(value, 1) {
				t.Fatalf("%s computed overflow = %v, %v, want +Inf", test.name, value, ok)
			}
			for rowIndex := 1; rowIndex < len(page.Rows); rowIndex++ {
				if !page.Rows[rowIndex].Values[1].IsNull() {
					t.Fatalf("%s overflow gap %d = %#v, want null", test.name, rowIndex, page.Rows[rowIndex].Values[1])
				}
			}
		})
	}
}

func queryIntegrationAssertFixedValueSchema(
	t *testing.T,
	job searchjobs.Job,
	page searchjobs.ResultPage,
	valueField string,
) {
	t.Helper()
	if job.State != searchjobs.StateCompleted {
		t.Fatalf("fixed numeric timechart state = %v, failure=%#v", job.State, job.Failure)
	}
	if len(page.Schema.Columns) != 2 ||
		page.Schema.Columns[0] != (searchjobs.Column{Name: "_time", Kind: searchjobs.ValueKindTime}) ||
		page.Schema.Columns[1] != (searchjobs.Column{
			Name: valueField, Kind: searchjobs.ValueKindDouble, Nullable: true,
		}) {
		t.Fatalf("fixed numeric timechart schema = %#v", page.Schema)
	}
}

func queryIntegrationAssertFixedValueStatsParity(
	t *testing.T,
	timechart searchjobs.ResultPage,
	stats searchjobs.ResultPage,
	valueField string,
) {
	t.Helper()
	statsBucket := queryIntegrationColumnIndex(t, stats, "bucket")
	statsValue := queryIntegrationColumnIndex(t, stats, valueField)
	byBucket := make(map[int64]searchjobs.Value, len(stats.Rows))
	for _, row := range stats.Rows {
		bucket, ok := row.Values[statsBucket].Time()
		if !ok {
			t.Fatalf("stats oracle bucket = %#v", row.Values[statsBucket])
		}
		byBucket[bucket.UnixNano()] = row.Values[statsValue]
	}
	for rowIndex, row := range timechart.Rows {
		bucket, ok := row.Values[0].Time()
		if !ok {
			t.Fatalf("timechart row %d bucket = %#v", rowIndex, row.Values[0])
		}
		want, observed := byBucket[bucket.UnixNano()]
		if !observed {
			if !row.Values[1].IsNull() {
				t.Fatalf("timechart gap %v = %#v, want null", bucket, row.Values[1])
			}
			continue
		}
		delete(byBucket, bucket.UnixNano())
		if want.IsNull() {
			if !row.Values[1].IsNull() {
				t.Fatalf("timechart all-ineligible bucket %v = %#v, want null", bucket, row.Values[1])
			}
			continue
		}
		wantDouble, wantOK := want.Double()
		if !wantOK {
			t.Fatalf("stats oracle bucket %v value = %#v", bucket, want)
		}
		queryIntegrationAssertDouble(t, row.Values[1], wantDouble, "timechart/stats parity")
	}
	if len(byBucket) != 0 {
		t.Fatalf("stats oracle contains buckets outside the timechart grid: %#v", byBucket)
	}
}

func queryIntegrationAssertDouble(t *testing.T, value searchjobs.Value, want float64, label string) {
	t.Helper()
	got, ok := value.Double()
	if !ok || math.IsNaN(got) || math.IsInf(got, 0) ||
		math.Abs(got-want) > 1e-12*math.Max(1, math.Abs(want)) {
		t.Fatalf("%s = %v, %v, want %v", label, got, ok, want)
	}
}

func queryIntegrationPlanContainsArrayJoin(plan ExplainPlan) bool {
	for _, nodeType := range plan.NodeTypes {
		if strings.Contains(strings.ToLower(nodeType), "arrayjoin") {
			return true
		}
	}
	return false
}

func queryIntegrationExplainActions(
	t *testing.T,
	ctx context.Context,
	connection clickhousedriver.Conn,
	compiled clickhouse.CompiledQuery,
) string {
	t.Helper()
	// ClickHouse 26.7 made the compact "pretty" plan the EXPLAIN PLAN default
	// (explain_query_plan_default), which summarizes each step's expressions
	// instead of listing the physical actions this helper's callers count.
	// Naming actions, compact, and pretty explicitly restores that rendering.
	const explainActionsPrefix = "EXPLAIN actions = 1, compact = 0, pretty = 0 "
	rows, err := connection.Query(ctx, explainActionsPrefix+compiled.SQL, compiled.Args...)
	if err != nil {
		t.Fatalf("EXPLAIN actions: %v\nSQL: %s\nargs: %#v", err, compiled.SQL, compiled.Args)
	}
	defer func() {
		if err := rows.Close(); err != nil {
			t.Errorf("close EXPLAIN actions: %v", err)
		}
	}()
	var lines []string
	for rows.Next() {
		var line string
		if err := rows.Scan(&line); err != nil {
			t.Fatalf("scan EXPLAIN actions: %v", err)
		}
		lines = append(lines, line)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate EXPLAIN actions: %v", err)
	}
	return strings.Join(lines, "\n")
}

func queryIntegrationInsertTimechartEvents(t *testing.T, ctx context.Context, connection clickhousedriver.Conn) (time.Time, time.Time) {
	t.Helper()
	query := "INSERT INTO open_splunk.events (event_id, tenant_id, index_name, event_time, index_time, " +
		"collected_at, event_time_source, host, source, sourcetype, service, severity, level, body, raw, " +
		"raw_encoding, trace_id, span_id, fields, field_names, collector_id, batch_id, batch_sequence, " +
		"expires_at, visibility_seq)"
	batch, err := connection.PrepareBatch(ctx, query)
	if err != nil {
		t.Fatal(err)
	}
	base := time.Now().UTC().Add(-2 * time.Hour).Truncate(5 * time.Minute)
	indexTime := time.Now().UTC().Truncate(time.Millisecond)
	errorLevel := "ERROR"
	warnLevel := "WARN"
	outsideLevel := "OUTSIDE"
	type fixtureEvent struct {
		id         string
		tenant     string
		indexName  string
		source     string
		at         time.Time
		level      *string
		pathSet    bool
		pathName   string
		path       any
		metricSet  bool
		metric     clickhousedriver.Dynamic
		visibility uint64
	}
	events := []fixtureEvent{
		{
			id:     "storage-floor",
			source: "timechart-storage-floor",
			at:     time.Date(1900, time.January, 1, 0, 30, 0, 0, time.UTC),
			level:  &errorLevel,
		},
		{id: "before", source: "timechart-level", at: base.Add(2*time.Minute - time.Nanosecond), level: &outsideLevel},
		{id: "error-start", source: "timechart-level", at: base.Add(2 * time.Minute), level: &errorLevel},
		{id: "error-end", source: "timechart-level", at: base.Add(5*time.Minute - time.Nanosecond), level: &errorLevel},
		{id: "warn", source: "timechart-level", at: base.Add(5 * time.Minute), level: &warnLevel},
		{id: "missing", source: "timechart-level", at: base.Add(10 * time.Minute)},
		{id: "latest", source: "timechart-level", at: base.Add(18 * time.Minute), level: &outsideLevel},
	}
	for index := range 3 {
		events = append(events, fixtureEvent{id: fmt.Sprintf("top-hot-%d", index), source: "timechart-top", at: base.Add(3 * time.Minute), pathSet: true, path: "hot"})
	}
	for label := 'a'; label <= 'k'; label++ {
		events = append(events, fixtureEvent{id: "top-" + string(label), source: "timechart-top", at: base.Add(3 * time.Minute), pathSet: true, path: string(label)})
	}
	// The ten two-count labels fill the top-series domain. VALUE_x has one
	// event, so it collapses into OTHER while still colliding with the public
	// normalization of the selected _x series.
	for _, label := range []string{"_x", "a", "b", "c", "d", "e", "f", "g", "h", "i"} {
		for repeat := range 2 {
			events = append(events, fixtureEvent{
				id:      fmt.Sprintf("collision-outside-%s-%d", label, repeat),
				source:  "timechart-collision-outside-top",
				at:      base.Add(3 * time.Minute),
				pathSet: true,
				path:    label,
			})
		}
	}
	events = append(events,
		fixtureEvent{id: "top-missing", source: "timechart-top", at: base.Add(3 * time.Minute)},
		fixtureEvent{id: "top-null", source: "timechart-top", at: base.Add(3 * time.Minute), pathSet: true, path: nil},
		fixtureEvent{id: "underscore", source: "timechart-underscore", at: base.Add(3 * time.Minute), pathSet: true, path: "_audit"},
		fixtureEvent{id: "underscore-z", source: "timechart-underscore", at: base.Add(3 * time.Minute), pathSet: true, path: "Z"},
		fixtureEvent{id: "collision-outside-literal", source: "timechart-collision-outside-top", at: base.Add(3 * time.Minute), pathSet: true, path: "VALUE_x"},
		fixtureEvent{id: "invalid-number", source: "timechart-invalid", at: base.Add(3 * time.Minute), pathSet: true, path: int64(7)},
		fixtureEvent{id: "invalid-list", source: "timechart-list", at: base.Add(3 * time.Minute), pathSet: true, path: []string{"a", "b"}},
		fixtureEvent{id: "invalid-object", source: "timechart-object", at: base.Add(3 * time.Minute), pathSet: true, pathName: "path.child", path: "leaf"},
		fixtureEvent{id: "collision-prefixed", source: "timechart-collision", at: base.Add(3 * time.Minute), pathSet: true, path: "_x"},
		fixtureEvent{id: "collision-literal", source: "timechart-collision", at: base.Add(3 * time.Minute), pathSet: true, path: "VALUE_x"},
		fixtureEvent{id: "other-tenant", tenant: "other", source: "timechart-level", at: base.Add(3 * time.Minute), level: &outsideLevel},
		fixtureEvent{id: "other-index", indexName: "other", source: "timechart-level", at: base.Add(3 * time.Minute), level: &outsideLevel},
		fixtureEvent{id: "future-visibility", source: "timechart-level", at: base.Add(3 * time.Minute), level: &outsideLevel, visibility: 2},
	)
	numericList := func(values ...any) clickhousedriver.Dynamic {
		items := make([]clickhousedriver.Dynamic, len(values))
		for index, value := range values {
			items[index] = clickhousedriver.NewDynamic(value)
		}
		return clickhousedriver.NewDynamicWithType(items, "Array(Dynamic)")
	}
	events = append(events,
		// Visible values within each populated bucket are intentionally equal,
		// making the approximate GK result exact while still exercising every
		// normalization path shared with stats: native integers/floats, numeric
		// strings, tagged decimals, and multivalue arrays containing null and
		// nonnumeric elements.
		fixtureEvent{id: "percentile-before", source: "timechart-percentile", at: base.Add(2*time.Minute - time.Nanosecond), metricSet: true, metric: clickhousedriver.NewDynamic(int64(9_999))},
		fixtureEvent{id: "percentile-int", source: "timechart-percentile", at: base.Add(2 * time.Minute), metricSet: true, metric: clickhousedriver.NewDynamic(int64(100))},
		fixtureEvent{id: "percentile-string", source: "timechart-percentile", at: base.Add(3 * time.Minute), metricSet: true, metric: clickhousedriver.NewDynamic("100")},
		fixtureEvent{id: "percentile-double", source: "timechart-percentile", at: base.Add(4 * time.Minute), metricSet: true, metric: clickhousedriver.NewDynamic(float64(100))},
		fixtureEvent{id: "percentile-list", source: "timechart-percentile", at: base.Add(4*time.Minute + time.Second), metricSet: true, metric: numericList(int64(100), "100", "not-numeric", nil)},
		fixtureEvent{id: "percentile-second-int", source: "timechart-percentile", at: base.Add(10 * time.Minute), metricSet: true, metric: clickhousedriver.NewDynamic(uint64(300))},
		fixtureEvent{id: "percentile-second-decimal", source: "timechart-percentile", at: base.Add(11 * time.Minute), metricSet: true, metric: queryIntegrationExtendedValue("decimal/v1", "300.000")},
		fixtureEvent{id: "percentile-third-list", source: "timechart-percentile", at: base.Add(15 * time.Minute), metricSet: true, metric: numericList(int64(400), "400", nil)},
		fixtureEvent{id: "percentile-latest", source: "timechart-percentile", at: base.Add(18 * time.Minute), metricSet: true, metric: clickhousedriver.NewDynamic(int64(9_999))},
		fixtureEvent{id: "percentile-other-tenant", tenant: "other", source: "timechart-percentile", at: base.Add(3 * time.Minute), metricSet: true, metric: clickhousedriver.NewDynamic(int64(8_888))},
		fixtureEvent{id: "percentile-other-index", indexName: "other", source: "timechart-percentile", at: base.Add(3 * time.Minute), metricSet: true, metric: clickhousedriver.NewDynamic(int64(7_777))},
		fixtureEvent{id: "percentile-future-visibility", source: "timechart-percentile", at: base.Add(3 * time.Minute), metricSet: true, metric: clickhousedriver.NewDynamic(int64(6_666)), visibility: 2},
		// A nonempty scoped relation with no numeric candidates must retain the
		// complete grid and publish nulls rather than looking like empty input.
		fixtureEvent{id: "percentile-ineligible-missing", source: "timechart-percentile-ineligible", at: base.Add(2 * time.Minute)},
		fixtureEvent{id: "percentile-ineligible-text", source: "timechart-percentile-ineligible", at: base.Add(6 * time.Minute), metricSet: true, metric: clickhousedriver.NewDynamic("not-numeric")},
		fixtureEvent{id: "percentile-ineligible-bool", source: "timechart-percentile-ineligible", at: base.Add(11 * time.Minute), metricSet: true, metric: clickhousedriver.NewDynamic(true)},
		fixtureEvent{id: "percentile-ineligible-null", source: "timechart-percentile-ineligible", at: base.Add(16 * time.Minute), metricSet: true, metric: clickhousedriver.NewDynamic(nil)},
	)
	events = append(events,
		// The first visible fixed-value bucket contains seven eligible immediate
		// members: native integer/float, numeric String, tagged Decimal, and a
		// multivalue whose duplicated 1 contributes twice. Its total is 23.5 and
		// its member-weighted average is 23.5/7. Large poison values prove that
		// tenant, index, half-open event time, and visibility scoping happen before
		// the bounded bucket aggregation.
		fixtureEvent{id: "numeric-before", source: "timechart-numeric", at: base.Add(2*time.Minute - time.Nanosecond), metricSet: true, metric: clickhousedriver.NewDynamic(float64(9_999))},
		fixtureEvent{id: "numeric-int", source: "timechart-numeric", at: base.Add(2 * time.Minute), metricSet: true, metric: clickhousedriver.NewDynamic(int64(10))},
		fixtureEvent{id: "numeric-float", source: "timechart-numeric", at: base.Add(2*time.Minute + 30*time.Second), metricSet: true, metric: clickhousedriver.NewDynamicWithType(float64(2.5), "Float64")},
		fixtureEvent{id: "numeric-string", source: "timechart-numeric", at: base.Add(3 * time.Minute), metricSet: true, metric: clickhousedriver.NewDynamic("3.5")},
		fixtureEvent{id: "numeric-decimal", source: "timechart-numeric", at: base.Add(3*time.Minute + 30*time.Second), metricSet: true, metric: queryIntegrationExtendedValue("decimal/v1", "4.000")},
		fixtureEvent{id: "numeric-list", source: "timechart-numeric", at: base.Add(4 * time.Minute), metricSet: true, metric: numericList(int64(1), int64(1), "1.5", "not-numeric", nil)},
		// A real zero must remain distinguishable from the null gap and the
		// present-but-all-ineligible bucket that follow it.
		fixtureEvent{id: "numeric-zero-positive", source: "timechart-numeric", at: base.Add(6 * time.Minute), metricSet: true, metric: clickhousedriver.NewDynamic(int64(7))},
		fixtureEvent{id: "numeric-zero-negative", source: "timechart-numeric", at: base.Add(7 * time.Minute), metricSet: true, metric: clickhousedriver.NewDynamic(int64(-7))},
		fixtureEvent{id: "numeric-ineligible-missing", source: "timechart-numeric", at: base.Add(15 * time.Minute)},
		fixtureEvent{id: "numeric-ineligible-text", source: "timechart-numeric", at: base.Add(16 * time.Minute), metricSet: true, metric: clickhousedriver.NewDynamic("not-numeric")},
		fixtureEvent{id: "numeric-latest", source: "timechart-numeric", at: base.Add(18 * time.Minute), metricSet: true, metric: clickhousedriver.NewDynamic(float64(9_999))},
		fixtureEvent{id: "numeric-other-tenant", tenant: "other", source: "timechart-numeric", at: base.Add(3 * time.Minute), metricSet: true, metric: clickhousedriver.NewDynamic(float64(8_888))},
		fixtureEvent{id: "numeric-other-index", indexName: "other", source: "timechart-numeric", at: base.Add(3 * time.Minute), metricSet: true, metric: clickhousedriver.NewDynamic(float64(7_777))},
		fixtureEvent{id: "numeric-future-visibility", source: "timechart-numeric", at: base.Add(3 * time.Minute), metricSet: true, metric: clickhousedriver.NewDynamic(float64(6_666)), visibility: 2},
		// Every input is finite; only the aggregate arithmetic overflows. Both
		// sum and avg intentionally preserve ClickHouse's computed +Inf.
		fixtureEvent{id: "numeric-overflow-a", source: "timechart-numeric-overflow", at: base.Add(2 * time.Minute), metricSet: true, metric: clickhousedriver.NewDynamic("1.7976931348623157e308")},
		fixtureEvent{id: "numeric-overflow-b", source: "timechart-numeric-overflow", at: base.Add(3 * time.Minute), metricSet: true, metric: clickhousedriver.NewDynamic("1.7976931348623157e308")},
	)
	for index, event := range events {
		message := "timechart " + event.id
		document := clickhousedriver.NewJSON()
		var fieldNames []string
		if event.pathSet {
			pathName := event.pathName
			if pathName == "" {
				pathName = "path"
			}
			document.SetValueAtPath(pathName, clickhousedriver.NewDynamic(event.path))
			fieldNames = []string{pathName}
		}
		if event.metricSet {
			document.SetValueAtPath("metric", event.metric)
			fieldNames = append(fieldNames, "metric")
			slices.Sort(fieldNames)
		}
		tenant := event.tenant
		if tenant == "" {
			tenant = "tenant"
		}
		indexName := event.indexName
		if indexName == "" {
			indexName = "main"
		}
		visibility := event.visibility
		if visibility == 0 {
			visibility = 1
		}
		if err := batch.Append(
			"queryexec-timechart-"+event.id, tenant, indexName, event.at, indexTime,
			nil, uint8(1), "host", event.source, "test", nil, uint8(1), event.level, &message, []byte(message),
			uint8(1), nil, nil, document, fieldNames, "collector", "timechart-batch", uint64(index+1),
			indexTime.Add(24*time.Hour), visibility,
		); err != nil {
			t.Fatal(err)
		}
	}
	if err := batch.Send(); err != nil {
		t.Fatal(err)
	}
	return base, indexTime
}

// queryIntegrationChartEvent is one chart fixture row. Every chart scenario is
// separated by source so each case scopes to exactly the rows it reasons about.
type queryIntegrationChartEvent struct {
	id       string
	source   string
	at       time.Time
	host     string
	severity uint8
	level    *string
	fields   map[string]any
}

// queryIntegrationInsertChartEvents writes the pivot fixture. The column axis
// deliberately carries real values — a fixture whose split field is null on
// every row cannot prove any column-axis behavior.
func queryIntegrationInsertChartEvents(t *testing.T, ctx context.Context, connection clickhousedriver.Conn) (time.Time, time.Time) {
	t.Helper()
	query := "INSERT INTO open_splunk.events (event_id, tenant_id, index_name, event_time, index_time, " +
		"collected_at, event_time_source, host, source, sourcetype, service, severity, level, body, raw, " +
		"raw_encoding, trace_id, span_id, fields, field_names, collector_id, batch_id, batch_sequence, " +
		"expires_at, visibility_seq)"
	batch, err := connection.PrepareBatch(ctx, query)
	if err != nil {
		t.Fatal(err)
	}
	base := time.Now().UTC().Add(-3 * time.Hour).Truncate(5 * time.Minute)
	indexTime := time.Now().UTC().Truncate(time.Millisecond)
	info, failure := "INFO", "ERROR"

	var events []queryIntegrationChartEvent
	add := func(id, source string, at time.Time, level *string, fields map[string]any) {
		events = append(events, queryIntegrationChartEvent{
			id: id, source: source, at: at, level: level, fields: fields,
		})
	}
	path := func(value string) map[string]any { return map[string]any{"path": value} }
	pathSeries := func(pathValue string, series any) map[string]any {
		return map[string]any{"path": pathValue, "series": series}
	}

	// The contract's anchor example.
	add("r5-a1", "chart-r5", base, &info, path("/a"))
	add("r5-a2", "chart-r5", base, &info, path("/a"))
	add("r5-a3", "chart-r5", base, &failure, path("/a"))
	add("r5-b1", "chart-r5", base, &failure, path("/b"))
	add("r5-b2", "chart-r5", base, nil, path("/b"))

	// Three rows, twelve column values, an excluded tail, and missing column
	// values, so the per-row totals must survive the column bound.
	columns := []struct {
		row    string
		series string
		repeat int
	}{
		{"/x", "c01", 3}, {"/x", "c02", 2}, {"/x", "c03", 1}, {"/x", "c04", 1},
		{"/x", "c05", 1}, {"/x", "c06", 1}, {"/x", "c07", 1}, {"/x", "c08", 1},
		{"/x", "c09", 1}, {"/x", "c10", 1}, {"/x", "c11", 1}, {"/x", "", 2},
		{"/y", "c01", 1}, {"/y", "c12", 4}, {"/y", "", 1},
		{"/z", "c05", 1},
	}
	counter := 0
	for _, column := range columns {
		for range column.repeat {
			counter++
			fields := path(column.row)
			if column.series != "" {
				fields = pathSeries(column.row, column.series)
			}
			add(fmt.Sprintf("chart-c8-%d", counter), "chart-c8", base, &info, fields)
		}
	}

	// Numeric chart needs a result-level fixture because the CI vertical runs
	// the production executor and manager, not only the compiler transport.
	// Ten labels dominate numeric ranking; the remaining two are folded into
	// OTHER. Average must merge 21 underlying members, while percentile must
	// merge both raw GK states before finalization rather than combine the two
	// already-finalized cells.
	numericLabel := func(value string) *string { return &value }
	addNumeric := func(id, row string, series *string, metricSet bool, metric any) {
		fields := path(row)
		if series != nil {
			fields["series"] = *series
		}
		if metricSet {
			fields["metric"] = metric
		}
		add(id, "chart-numeric", base, &info, fields)
	}
	for index := 1; index <= 10; index++ {
		addNumeric(
			fmt.Sprintf("numeric-top-%02d", index),
			"/weighted",
			numericLabel(fmt.Sprintf("s%02d", index)),
			true,
			float64(101-index),
		)
	}
	addNumeric("numeric-overflow-eleven", "/weighted", numericLabel("s11"), true, float64(30))
	for index := range 20 {
		addNumeric(
			fmt.Sprintf("numeric-overflow-twelve-%02d", index),
			"/weighted",
			numericLabel("s12"),
			true,
			float64(1),
		)
	}
	addNumeric("numeric-null-series", "/weighted", nil, true, float64(7))

	// One eligible zero plus poison remains present. The adjacent s02 cell and
	// the entire /empty row have no eligible member and must publish as null.
	addNumeric("numeric-zero", "/zero", numericLabel("s01"), true, float64(0))
	addNumeric("numeric-zero-text", "/zero", numericLabel("s01"), true, "not-numeric")
	addNumeric("numeric-zero-bool", "/zero", numericLabel("s01"), true, true)
	addNumeric("numeric-zero-null", "/zero", numericLabel("s01"), true, nil)
	addNumeric("numeric-zero-missing", "/zero", numericLabel("s01"), false, nil)
	addNumeric("numeric-null-text", "/zero", numericLabel("s02"), true, "still-not-numeric")
	addNumeric("numeric-null-bool", "/zero", numericLabel("s02"), true, false)
	addNumeric("numeric-null-explicit", "/zero", numericLabel("s02"), true, nil)
	addNumeric("numeric-null-missing", "/zero", numericLabel("s02"), false, nil)
	addNumeric("numeric-empty-text", "/empty", numericLabel("s03"), true, "never-numeric")
	addNumeric("numeric-empty-bool", "/empty", numericLabel("s03"), true, true)
	addNumeric("numeric-empty-null", "/empty", numericLabel("s03"), true, nil)
	addNumeric("numeric-empty-missing", "/empty", numericLabel("s03"), false, nil)

	add("underscore-audit", "chart-underscore", base, &info, pathSeries("/u", "_audit"))
	add("underscore-z", "chart-underscore", base, &info, pathSeries("/u", "Z"))

	// R1 across every supported row kind: a fixed String column, a runtime
	// typed event field, a fixed numeric column, and a binned timestamp.
	events = append(events,
		queryIntegrationChartEvent{id: "kinds-1", source: "chart-kinds", at: base, host: "alpha", severity: 3, level: &info, fields: path("/a")},
		queryIntegrationChartEvent{id: "kinds-2", source: "chart-kinds", at: base.Add(2 * time.Minute), host: "beta", severity: 3, level: &failure, fields: path("/b")},
		queryIntegrationChartEvent{id: "kinds-3", source: "chart-kinds", at: base.Add(7 * time.Minute), host: "alpha", severity: 9, level: &info, fields: path("/a")},
	)

	// Every atomic unsupported-value boundary the executor must classify.
	add("bad-number", "chart-bad-number", base, &info, pathSeries("/p", int64(7)))
	add("bad-row-list", "chart-bad-row-list", base, &info, pathSeries("/p", []string{"a", "b"}))
	add("bad-rowname", "chart-bad-rowname", base, &info, pathSeries("/p", "path"))
	add("bad-collision-prefixed", "chart-bad-collision", base, &info, pathSeries("/p", "_x"))
	add("bad-collision-literal", "chart-bad-collision", base, &info, pathSeries("/p", "VALUE_x"))

	for index, event := range events {
		message := "chart " + event.id
		document := clickhousedriver.NewJSON()
		fieldNames := make([]string, 0, len(event.fields))
		for _, name := range slices.Sorted(maps.Keys(event.fields)) {
			document.SetValueAtPath(name, clickhousedriver.NewDynamic(event.fields[name]))
			fieldNames = append(fieldNames, name)
		}
		host := event.host
		if host == "" {
			host = "host"
		}
		severity := event.severity
		if severity == 0 {
			severity = 1
		}
		if err := batch.Append(
			"queryexec-chart-"+event.id, "tenant", "main", event.at, indexTime,
			nil, uint8(1), host, event.source, "test", nil, severity, event.level, &message, []byte(message),
			uint8(1), nil, nil, document, fieldNames, "collector", "chart-batch", uint64(index+1),
			indexTime.Add(24*time.Hour), uint64(1),
		); err != nil {
			t.Fatal(err)
		}
	}
	if err := batch.Send(); err != nil {
		t.Fatal(err)
	}
	return base, indexTime
}

// queryIntegrationSameValue compares two published scalars without assuming a
// kind, so a chart's row column can be checked against the stats group column
// it must reproduce exactly.
func queryIntegrationSameValue(left, right searchjobs.Value) bool {
	if left.Kind() != right.Kind() {
		return false
	}
	switch left.Kind() {
	case searchjobs.ValueKindString:
		leftValue, leftOK := left.String()
		rightValue, rightOK := right.String()
		return leftOK && rightOK && leftValue == rightValue
	case searchjobs.ValueKindUnsigned:
		leftValue, leftOK := left.Unsigned()
		rightValue, rightOK := right.Unsigned()
		return leftOK && rightOK && leftValue == rightValue
	case searchjobs.ValueKindSigned:
		leftValue, leftOK := left.Signed()
		rightValue, rightOK := right.Signed()
		return leftOK && rightOK && leftValue == rightValue
	case searchjobs.ValueKindDouble:
		leftValue, leftOK := left.Double()
		rightValue, rightOK := right.Double()
		return leftOK && rightOK && leftValue == rightValue
	case searchjobs.ValueKindBool:
		leftValue, leftOK := left.Bool()
		rightValue, rightOK := right.Bool()
		return leftOK && rightOK && leftValue == rightValue
	case searchjobs.ValueKindTime:
		leftValue, leftOK := left.Time()
		rightValue, rightOK := right.Time()
		return leftOK && rightOK && leftValue.Equal(rightValue)
	default:
		return false
	}
}

func queryIntegrationExtendedValue(kind, value string) clickhousedriver.Dynamic {
	return clickhousedriver.NewDynamicWithType(map[string]string{
		extendedTypeKey:  kind,
		extendedValueKey: value,
	}, "Map(String, String)")
}

func queryIntegrationRunSearch(
	t *testing.T,
	ctx context.Context,
	executor *Executor,
	eventIndexTime time.Time,
	id, source string,
) (searchjobs.Job, searchjobs.ResultPage) {
	t.Helper()
	return queryIntegrationRunSearchRange(
		t, ctx, executor, eventIndexTime, id, source,
		eventIndexTime.Add(-time.Hour), eventIndexTime.Add(time.Hour),
	)
}

func queryIntegrationRunSearchRange(
	t *testing.T,
	ctx context.Context,
	executor *Executor,
	indexTime time.Time,
	id, source string,
	earliest, latest time.Time,
) (searchjobs.Job, searchjobs.ResultPage) {
	t.Helper()
	return queryIntegrationRunSearchRangeForIndex(
		t, ctx, executor, indexTime, id, source, earliest, latest, "main",
	)
}

func queryIntegrationRunGradeThisSearchRange(
	t *testing.T,
	ctx context.Context,
	executor *Executor,
	indexTime time.Time,
	id, source string,
	earliest, latest time.Time,
) (searchjobs.Job, searchjobs.ResultPage) {
	t.Helper()
	return queryIntegrationRunSearchRangeForIndex(
		t, ctx, executor, indexTime, id, source, earliest, latest, "gradethis",
	)
}

func queryIntegrationRunSearchRangeForIndex(
	t *testing.T,
	ctx context.Context,
	executor *Executor,
	indexTime time.Time,
	id, source string,
	earliest, latest time.Time,
	indexName string,
) (searchjobs.Job, searchjobs.ResultPage) {
	t.Helper()
	manager, err := searchjobs.New(searchjobs.Config{
		Executor:        executor,
		Snapshotter:     queryIntegrationSnapshotter(1),
		Compiler:        clickhouse.Compiler{},
		MaxConcurrent:   1,
		MaxQueued:       1,
		CleanupInterval: -1,
		Now:             func() time.Time { return indexTime.Add(500 * time.Microsecond) },
		NewID:           func() string { return id },
		CursorKey:       []byte("0123456789abcdef0123456789abcdef"),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Close()
	job, err := manager.Create(ctx, searchjobs.CreateRequest{
		SPL:               source,
		OwnerID:           "owner",
		TenantID:          "tenant",
		AuthorizedIndexes: []string{indexName},
		RequestedIndexes:  []string{indexName},
		TimeRange:         queryIntegrationTimeRange(t, earliest, latest),
	})
	if err != nil {
		t.Fatal(err)
	}
	terminal := queryIntegrationWaitForTerminal(t, manager, job.ID)
	if terminal.State != searchjobs.StateCompleted {
		return terminal, searchjobs.ResultPage{}
	}
	page, err := manager.Results(job.ID, searchjobs.PageRequest{Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	return terminal, page
}

func queryIntegrationTimeRange(t *testing.T, earliest, latest time.Time) searchtime.Range {
	t.Helper()
	resolved, err := searchtime.NewAbsoluteRange(earliest, latest)
	if err != nil {
		t.Fatal(err)
	}
	return resolved
}

func queryIntegrationAssertColumns(t *testing.T, page searchjobs.ResultPage, want []string) {
	t.Helper()
	if len(page.Schema.Columns) != len(want) {
		t.Fatalf("schema = %#v, want columns %v", page.Schema, want)
	}
	for index, name := range want {
		if page.Schema.Columns[index].Name != name {
			t.Fatalf("column %d = %q, want %q (schema %#v)", index, page.Schema.Columns[index].Name, name, page.Schema)
		}
	}
}

func queryIntegrationColumnIndex(t *testing.T, page searchjobs.ResultPage, name string) int {
	t.Helper()
	for index, column := range page.Schema.Columns {
		if column.Name == name {
			return index
		}
	}
	t.Fatalf("schema %#v has no column %q", page.Schema, name)
	return -1
}

func queryIntegrationAssertTimechartMatrix(
	t *testing.T,
	page searchjobs.ResultPage,
	first time.Time,
	span time.Duration,
	want map[string][]uint64,
) {
	t.Helper()
	timeColumn := queryIntegrationColumnIndex(t, page, "_time")
	wantRows := -1
	for series, counts := range want {
		if wantRows < 0 {
			wantRows = len(counts)
		} else if len(counts) != wantRows {
			t.Fatalf("invalid expected matrix: series %q has %d rows, want %d", series, len(counts), wantRows)
		}
	}
	if len(page.Rows) != wantRows {
		t.Fatalf("timechart rows = %d, want %d", len(page.Rows), wantRows)
	}
	for rowIndex, row := range page.Rows {
		bucket, ok := row.Values[timeColumn].Time()
		wantBucket := first.Add(time.Duration(rowIndex) * span)
		if !ok || !bucket.Equal(wantBucket) {
			t.Fatalf("timechart row %d bucket = %v (%v), want %v", rowIndex, bucket, ok, wantBucket)
		}
	}
	for series, counts := range want {
		column := queryIntegrationColumnIndex(t, page, series)
		for rowIndex, wantCount := range counts {
			value, ok := page.Rows[rowIndex].Values[column].Unsigned()
			if !ok {
				t.Fatalf("timechart row %d series %q = %#v, want unsigned", rowIndex, series, page.Rows[rowIndex].Values[column])
			}
			if value != wantCount {
				t.Fatalf("timechart row %d series %q = %d, want %d", rowIndex, series, value, wantCount)
			}
		}
	}
}

func queryIntegrationCompileSearchRange(
	t *testing.T,
	source string,
	indexTime, earliest, latest time.Time,
) clickhouse.CompiledQuery {
	t.Helper()
	parsed, err := spl.Parse(source)
	if err != nil {
		t.Fatal(err)
	}
	visibility := uint64(1)
	logical, err := plan.Build(parsed, plan.Scope{
		TenantID:          "tenant",
		AuthorizedIndexes: []string{"main"},
		RequestedIndexes:  []string{"main"},
		Earliest:          earliest,
		Latest:            latest,
		SearchStart:       indexTime,
		SearchTimezone:    "UTC",
		IndexTimeCutoff:   indexTime.Add(500 * time.Microsecond),
		VisibilityCutoff:  &visibility,
	})
	if err != nil {
		t.Fatal(err)
	}
	compiled, err := (clickhouse.Compiler{}).Compile(logical)
	if err != nil {
		t.Fatal(err)
	}
	return compiled
}

func queryIntegrationCompileTimeline(
	t *testing.T,
	source, indexName string,
	indexTime time.Time,
	spec clickhouse.TimelineSpec,
) clickhouse.CompiledTimeline {
	t.Helper()
	parsed, err := spl.Parse(source)
	if err != nil {
		t.Fatal(err)
	}
	visibility := uint64(1)
	logical, err := plan.Build(parsed, plan.Scope{
		TenantID:          "tenant",
		AuthorizedIndexes: []string{indexName},
		RequestedIndexes:  []string{indexName},
		Earliest:          spec.Earliest,
		Latest:            spec.Latest,
		SearchStart:       indexTime,
		SearchTimezone:    "UTC",
		IndexTimeCutoff:   indexTime.Add(500 * time.Microsecond),
		VisibilityCutoff:  &visibility,
	})
	if err != nil {
		t.Fatal(err)
	}
	compiled, err := (clickhouse.Compiler{}).CompileTimeline(logical, spec)
	if err != nil {
		t.Fatal(err)
	}
	return compiled
}

func queryIntegrationWaitForTerminal(t *testing.T, manager *searchjobs.Manager, id string) searchjobs.Job {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		job, err := manager.Get(id)
		if err != nil {
			t.Fatal(err)
		}
		if job.State.Terminal() {
			return job
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("search job did not reach a terminal state")
	return searchjobs.Job{}
}

func queryIntegrationMigrate(t *testing.T, ctx context.Context, container, password string) {
	t.Helper()
	paths, err := filepath.Glob(filepath.Join("..", "..", "migrations", "clickhouse", "[0-9][0-9][0-9][0-9]_*.sql"))
	if err != nil || len(paths) == 0 {
		t.Fatalf("discover migrations: paths=%v err=%v", paths, err)
	}
	var migrations bytes.Buffer
	for _, path := range paths {
		migration, readErr := os.ReadFile(path)
		if readErr != nil {
			t.Fatal(readErr)
		}
		migrations.Write(migration)
		migrations.WriteByte('\n')
	}
	queryIntegrationDocker(t, ctx, bytes.NewReader(migrations.Bytes()),
		"exec", "--interactive", container, "clickhouse-client",
		"--user", "open_splunk", "--password", password, "--multiquery",
	)
}

func queryIntegrationRandomHex(t *testing.T, size int) string {
	t.Helper()
	value := make([]byte, size)
	if _, err := rand.Read(value); err != nil {
		t.Fatal(err)
	}
	return hex.EncodeToString(value)
}

func queryIntegrationDocker(t *testing.T, ctx context.Context, stdin *bytes.Reader, args ...string) {
	t.Helper()
	command := exec.CommandContext(ctx, "docker", args...)
	if stdin != nil {
		command.Stdin = stdin
	}
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("docker %s: %v\n%s", queryIntegrationRedactedDockerArgs(args), err, output)
	}
}

func queryIntegrationRedactedDockerArgs(args []string) string {
	redacted := slices.Clone(args)
	for index := range redacted {
		if index > 0 && redacted[index-1] == "--password" {
			redacted[index] = "[REDACTED]"
			continue
		}
		if strings.HasPrefix(redacted[index], "CLICKHOUSE_PASSWORD=") {
			redacted[index] = "CLICKHOUSE_PASSWORD=[REDACTED]"
		}
	}
	return strings.Join(redacted, " ")
}

func queryIntegrationWait(t *testing.T, ctx context.Context, container, password string) {
	t.Helper()
	deadline := time.Now().Add(90 * time.Second)
	stable := 0
	var last string
	for time.Now().Before(deadline) {
		command := exec.CommandContext(ctx, "docker", "exec", container, "clickhouse-client",
			"--user", "open_splunk", "--password", password, "--query", "SELECT 1")
		output, err := command.CombinedOutput()
		last = fmt.Sprintf("%v: %s", err, output)
		if err == nil && strings.TrimSpace(string(output)) == "1" {
			stable++
			if stable == 4 {
				return
			}
		} else {
			stable = 0
		}
		select {
		case <-ctx.Done():
			t.Fatalf("wait for ClickHouse: %v", ctx.Err())
		case <-time.After(500 * time.Millisecond):
		}
	}
	t.Fatalf("ClickHouse did not become ready: %s", last)
}

func queryIntegrationNativeAddress(t *testing.T, ctx context.Context, container string) string {
	t.Helper()
	output, err := exec.CommandContext(ctx, "docker", "port", container, "9000/tcp").CombinedOutput()
	if err != nil {
		t.Fatalf("resolve native port: %v: %s", err, output)
	}
	for line := range strings.SplitSeq(strings.TrimSpace(string(output)), "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "127.0.0.1:") {
			return line
		}
	}
	t.Fatalf("Docker returned no loopback native address: %s", output)
	return ""
}
