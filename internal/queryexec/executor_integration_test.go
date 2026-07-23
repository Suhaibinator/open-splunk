package queryexec

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	clickhousedriver "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/plan"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
	"github.com/Suhaibinator/open-splunk/internal/searchtime"
	"github.com/Suhaibinator/open-splunk/internal/spl"
)

const queryExecutorIntegrationImage = "clickhouse/clickhouse-server:26.3.17.4"

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
		_ = exec.CommandContext(cleanupCtx, "docker", "rm", "--force", container).Run()
	})
	queryIntegrationWait(t, ctx, container, password)
	queryIntegrationMigrate(t, ctx, container, password)

	connection, err := clickhousedriver.Open(&clickhousedriver.Options{
		Addr: []string{queryIntegrationNativeAddress(t, ctx, container)},
		Auth: clickhousedriver.Auth{
			Database: "open_splunk",
			Username: "open_splunk",
			Password: password,
		},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = connection.Close() })
	if err := connection.Ping(ctx); err != nil {
		t.Fatal(err)
	}

	executor, err := New(connection, Config{})
	if err != nil {
		t.Fatal(err)
	}
	queryIntegrationTestFieldCatalog(t, ctx, connection, executor)
	t.Run("native typed scan types", func(t *testing.T) {
		sink := &fakeSink{}
		err := executor.Execute(ctx, clickhouse.CompiledQuery{
			SQL: "SELECT toDecimal128('123.4500', 4) AS amount, " +
				"CAST('12:34:56.123456' AS Time64(6)) AS elapsed, " +
				"CAST(42 AS Dynamic) AS choice, CAST('{\"name\":\"alice\",\"count\":2}' AS JSON) AS payload",
			OutputFields: []string{"amount", "elapsed", "choice", "payload"},
		}, sink)
		if err != nil {
			t.Fatal(err)
		}
		wantKinds := []searchjobs.ValueKind{
			searchjobs.ValueKindDecimal, searchjobs.ValueKindDuration,
			searchjobs.ValueKindMixed, searchjobs.ValueKindObject,
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
		if fields, ok := sink.rows[0][3].Object(); !ok || len(fields) != 2 {
			t.Fatalf("JSON = %#v", sink.rows[0][3])
		}
	})

	eventIndexTime := queryIntegrationInsertEvent(t, ctx, connection)
	timechartBase, timechartIndexTime := queryIntegrationInsertTimechartEvents(t, ctx, connection)
	gradeThisBase, gradeThisIndexTime, gradeThisTraceID := queryIntegrationInsertGradeThisEvents(t, ctx, connection)
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
	t.Run("GradeThis product plan compatibility corpus", func(t *testing.T) {
		tests := []struct {
			name        string
			source      string
			wantColumns []string
			wantRows    int
			assert      func(*testing.T, searchjobs.ResultPage)
		}{
			{
				name: "follow one request",
				source: `index=gradethis trace_id="` + gradeThisTraceID + `"
| sort _time
| table _time level layer logger message`,
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
				name: "errors and warnings",
				source: `index=gradethis (level=ERROR OR level=WARN)
| sort -_time`,
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
				name: "known raw error fragment",
				source: `index=gradethis "connection refused"
| table _time level logger message trace_id`,
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
				name: "severity counts",
				source: `index=gradethis
| stats count by level
| sort -count`,
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
				name: "frequent errors",
				source: `index=gradethis level=ERROR
| stats count by logger, message
| sort -count
| head 20`,
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
				name: "volume by severity",
				source: `index=gradethis
| timechart span=5m count by level`,
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
				name: "server errors by route",
				source: `index=gradethis message="Request metrics" status>=500
| timechart span=5m count by path`,
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
				name: "responses by route and status",
				source: `index=gradethis message="Request metrics"
| stats count by path, status
| sort -count`,
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
				name: "slow routes",
				source: `index=gradethis message="Request metrics"
| eval duration_ms=tonumber(replace(duration, "ms$", ""))
| stats count p95(duration_ms) as p95_ms by path
| where p95_ms > 500`,
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
				name: "common messages",
				source: `index=gradethis
| top limit=20 message`,
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

	t.Run("top pipeline through manager", func(t *testing.T) {
		job, page := queryIntegrationRunSearch(t, ctx, executor, eventIndexTime,
			"queryexec-top-pipeline", `index=main | top limit=20 message`,
		)
		if job.State != searchjobs.StateCompleted {
			t.Fatalf("top state = %v, failure=%#v", job.State, job.Failure)
		}
		if len(page.Schema.Columns) != 3 || page.Schema.Columns[0].Name != "message" ||
			page.Schema.Columns[0].Kind != searchjobs.ValueKindString ||
			page.Schema.Columns[1].Name != "count" || page.Schema.Columns[1].Kind != searchjobs.ValueKindUnsigned ||
			page.Schema.Columns[2].Name != "percent" || page.Schema.Columns[2].Kind != searchjobs.ValueKindDouble {
			t.Fatalf("top schema = %#v", page.Schema)
		}
		if len(page.Rows) != 1 {
			t.Fatalf("top rows = %d, want 1", len(page.Rows))
		}
		if message, ok := page.Rows[0].Values[0].String(); !ok || message != "manager integration" {
			t.Fatalf("top message = %q, %v", message, ok)
		}
		if count, ok := page.Rows[0].Values[1].Unsigned(); !ok || count != 1 {
			t.Fatalf("top count = %d, %v", count, ok)
		}
		if percent, ok := page.Rows[0].Values[2].Double(); !ok || percent != 100 {
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
    fromUnixTimestamp64Nano(toInt64(grid.number) * 1000000000, 'UTC') AS "__os_timechart_bucket",
    ` + names + ` AS "__os_timechart_names",
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
		if err := executor.Execute(ctx, query, sink); err != nil {
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

		explicitlyBounded, err := New(connection, Config{MaxRowsToGroupBy: 10_001})
		if err != nil {
			t.Fatal(err)
		}
		boundedSink := &fakeSink{}
		err = explicitlyBounded.Execute(ctx, query, boundedSink)
		if !errors.Is(err, searchjobs.ErrExecutionLimit) || boundedSink.setCalls != 0 || len(boundedSink.rows) != 0 {
			t.Fatalf("explicit dense timechart cap: err=%v schema calls=%d rows=%d", err, boundedSink.setCalls, len(boundedSink.rows))
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
		for _, marker := range []string{clickhouse.UnsupportedStatsByValueMarker, clickhouse.UnsupportedDedupValueMarker} {
			err := executor.Execute(ctx, clickhouse.CompiledQuery{
				SQL:          `SELECT throwIf(toUInt8(1), '` + marker + `') AS impossible`,
				OutputFields: []string{"impossible"},
			}, &fakeSink{})
			if !errors.Is(err, searchjobs.ErrUnsupportedValue) || strings.Contains(err.Error(), marker) {
				t.Fatalf("non-scalar marker %q classification = %v", marker, err)
			}
		}
	})

	t.Run("group cardinality is bounded before result streaming", func(t *testing.T) {
		bounded, err := New(connection, Config{MaxRowsToGroupBy: 1})
		if err != nil {
			t.Fatal(err)
		}
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
		err := executor.Execute(cancelCtx, clickhouse.CompiledQuery{
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
		"raw_encoding, trace_id, span_id, fields, field_names, collector_id, batch_id, batch_sequence, " +
		"expires_at, visibility_seq)"
	batch, err := connection.PrepareBatch(ctx, query)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Add(-time.Minute).Truncate(time.Second).Add(987 * time.Millisecond)
	message := "manager integration"
	document := clickhousedriver.NewJSON()
	document.SetValueAtPath("status", clickhousedriver.NewDynamic("200"))
	document.SetValueAtPath("path", clickhousedriver.NewDynamic("/manager"))
	document.SetValueAtPath("duration", clickhousedriver.NewDynamic("650ms"))
	document.SetValueAtPath("logger.child", clickhousedriver.NewDynamic("stored-source-child"))
	document.SetValueAtPath("typed_bytes", queryIntegrationExtendedValue("bytes/v1", "AP8"))
	document.SetValueAtPath("typed_timestamp", queryIntegrationExtendedValue("timestamp/v1", now.Add(42*time.Second).Format(time.RFC3339Nano)))
	document.SetValueAtPath("typed_duration", queryIntegrationExtendedValue("duration/v1", "-12:-345000000"))
	document.SetValueAtPath("typed_decimal", queryIntegrationExtendedValue("decimal/v1", "-123.4500e+2"))
	document.SetValueAtPath("component.child", clickhousedriver.NewDynamic("stored-target-child"))
	if err := batch.Append(
		"queryexec-event", "tenant", "main", now, now,
		nil, uint8(1), "host", "source", "test", nil, uint8(1), nil, &message, []byte(message),
		uint8(1), nil, nil, document,
		[]string{"component.child", "duration", "logger.child", "path", "status", "typed_bytes", "typed_decimal", "typed_duration", "typed_timestamp"},
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

func queryIntegrationInsertGradeThisEvents(
	t *testing.T,
	ctx context.Context,
	connection clickhousedriver.Conn,
) (time.Time, time.Time, string) {
	t.Helper()
	const traceID = "trace-gradethis-001"
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
		visibility uint64
	}
	events := []fixtureEvent{
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
		t.Fatalf("docker %s: %v\n%s", strings.Join(args, " "), err, output)
	}
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
	for _, line := range strings.Split(strings.TrimSpace(string(output)), "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "127.0.0.1:") {
			return line
		}
	}
	t.Fatalf("Docker returned no loopback native address: %s", output)
	return ""
}
