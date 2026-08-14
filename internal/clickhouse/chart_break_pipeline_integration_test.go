package clickhouse

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	clickhousedriver "github.com/ClickHouse/clickhouse-go/v2"
	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/ingest"
	"github.com/Suhaibinator/open-splunk/internal/plan"
	"github.com/Suhaibinator/open-splunk/internal/spl"
	"github.com/Suhaibinator/open-splunk/internal/visibility"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	chartBreakPipelineIndex  = "chartpipe"
	chartBreakPipelineTenant = "tenant"
)

var chartBreakPipelineBase = time.Date(2026, time.July, 21, 3, 0, 0, 0, time.UTC)

// chartBreakPipelineRow is the physical transport one pivot row occupies.
type chartBreakPipelineRow struct {
	ordinal uint64
	row     string
	names   string
	counts  string
}

// TestChartBreakPipelineAgainstClickHouse executes the pivot behind every
// supported upstream command family against the pinned server. The compile-time
// suite proves the SQL text; only a real server proves that each distinct
// physical shape the upstreams manufacture — a Bool row cast, an Int64 row
// cast, a Float64 row cast, a DateTime64 bucket row, an unsigned aggregate row,
// the Mixed _raw row, the flattened-object descendant probes, and quoted
// identifiers full of driver metacharacters — is executable at all.
func TestChartBreakPipelineAgainstClickHouse(t *testing.T) {
	if os.Getenv("OPEN_SPLUNK_CLICKHOUSE_INTEGRATION") != "1" {
		t.Skip("set OPEN_SPLUNK_CLICKHOUSE_INTEGRATION=1 to run the Docker integration test")
	}
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skipf("docker CLI is unavailable: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Minute)
	defer cancel()

	connection, store := chartBreakPipelineStartClickHouse(t, ctx)
	indexTime := time.Date(2026, time.July, 21, 4, 0, 0, 0, time.UTC)
	cutoff := chartBreakPipelineStoreFixture(t, ctx, store, indexTime)
	compile := func(source string) CompiledQuery {
		t.Helper()
		return chartBreakPipelineIntegrationCompile(t, source, indexTime.Add(time.Minute), cutoff)
	}

	// Every upstream family, executed end to end. The `want` column is the
	// complete published transport, so a wrong cast, a wrong ordering, or a
	// dropped row all fail here rather than being masked by a smoke check.
	for _, test := range []struct {
		name   string
		source string
		want   []chartBreakPipelineRow
	}{
		{
			name:   "bare pivot",
			source: `index=chartpipe source="pipe" | chart count OVER path BY level`,
			want: []chartBreakPipelineRow{
				{0, "/a", "0:ERROR|0:INFO|1:", "1|2|0"},
				{1, "/b", "0:ERROR|0:INFO|1:", "0|1|1"},
			},
		},
		{
			name:   "search and where upstream",
			source: `index=chartpipe source="pipe" level=INFO | where severity>0 | chart count OVER path BY level`,
			want: []chartBreakPipelineRow{
				{0, "/a", "0:INFO", "2"},
				{1, "/b", "0:INFO", "1"},
			},
		},
		{
			name:   "rex capture is the column axis",
			source: `index=chartpipe source="pipe" | rex field=path "^/(?<area>[a-z]+)" | chart count OVER path BY area`,
			want: []chartBreakPipelineRow{
				{0, "/a", "0:a|0:b", "3|0"},
				{1, "/b", "0:a|0:b", "0|2"},
			},
		},
		{
			name:   "rex capture is the row axis",
			source: `index=chartpipe source="pipe" | rex field=path "^/(?<area>[a-z]+)" | chart count OVER area BY level`,
			want: []chartBreakPipelineRow{
				{0, "a", "0:ERROR|0:INFO|1:", "1|2|0"},
				{1, "b", "0:ERROR|0:INFO|1:", "0|1|1"},
			},
		},
		{
			name:   "eval replace is the column axis",
			source: `index=chartpipe source="pipe" | eval trimmed=replace(duration, "ms$", "") | chart count OVER path BY trimmed`,
			// /a carries 10ms twice and 30ms once; /b carries 20ms twice.
			want: []chartBreakPipelineRow{
				{0, "/a", "0:10|0:20|0:30", "2|0|1"},
				{1, "/b", "0:10|0:20|0:30", "0|2|0"},
			},
		},
		{
			name: "eval tonumber then numeric bin is the row axis",
			source: `index=chartpipe source="pipe" | eval ms=tonumber(replace(duration, "ms$", "")) ` +
				`| bin ms span=20 AS band | chart count OVER band BY level`,
			want: []chartBreakPipelineRow{
				{0, "0", "0:ERROR|0:INFO|1:", "0|2|0"},
				{1, "20", "0:ERROR|0:INFO|1:", "1|1|1"},
			},
		},
		{
			name:   "eval bool literal row axis",
			source: `index=chartpipe source="pipe" | eval flag=true | chart count OVER flag BY level`,
			want:   []chartBreakPipelineRow{{0, "true", "0:ERROR|0:INFO|1:", "1|3|1"}},
		},
		{
			name:   "eval signed literal row axis",
			source: `index=chartpipe source="pipe" | eval offset=-3 | chart count OVER offset BY level`,
			want:   []chartBreakPipelineRow{{0, "-3", "0:ERROR|0:INFO|1:", "1|3|1"}},
		},
		{
			name:   "eval float literal row axis",
			source: `index=chartpipe source="pipe" | eval ratio=1.25 | chart count OVER ratio BY level`,
			want:   []chartBreakPipelineRow{{0, "1.25", "0:ERROR|0:INFO|1:", "1|3|1"}},
		},
		{
			name:   "eval unsigned literal row axis",
			source: `index=chartpipe source="pipe" | eval big=18446744073709551615 | chart count OVER big BY level`,
			want:   []chartBreakPipelineRow{{0, "18446744073709551615", "0:ERROR|0:INFO|1:", "1|3|1"}},
		},
		{
			name:   "static null literal row axis names no rows",
			source: `index=chartpipe source="pipe" | eval n=null | chart count OVER n BY level`,
			want:   nil,
		},
		{
			name:   "in-place time bin row axis",
			source: `index=chartpipe source="pipe" | bin _time span=10m | chart count OVER _time BY level`,
			want: []chartBreakPipelineRow{
				{0, "2026-07-21 03:00:00.000000000", "0:ERROR|0:INFO|1:", "0|2|0"},
				{1, "2026-07-21 03:20:00.000000000", "0:ERROR|0:INFO|1:", "1|1|1"},
			},
		},
		{
			name:   "renamed time bin row axis",
			source: `index=chartpipe source="pipe" | bin _time span=10m AS bt | chart count OVER bt BY level`,
			want: []chartBreakPipelineRow{
				{0, "2026-07-21 03:00:00.000000000", "0:ERROR|0:INFO|1:", "0|2|0"},
				{1, "2026-07-21 03:20:00.000000000", "0:ERROR|0:INFO|1:", "1|1|1"},
			},
		},
		{
			name:   "canonical severity bin row axis keeps its unsigned type",
			source: `index=chartpipe source="pipe" | bin severity span=4 | chart count OVER severity BY level`,
			want:   []chartBreakPipelineRow{{0, "0", "0:ERROR|0:INFO|1:", "1|3|1"}},
		},
		{
			name:   "stats group columns count input rows",
			source: `index=chartpipe source="pipe" | stats count BY path, level | chart count OVER path BY level`,
			want: []chartBreakPipelineRow{
				{0, "/a", "0:ERROR|0:INFO", "1|1"},
				{1, "/b", "0:ERROR|0:INFO", "0|1"},
			},
		},
		{
			name:   "renamed stats aggregate is an unsigned row axis",
			source: `index=chartpipe source="pipe" | stats count AS hits BY path, level | chart count OVER hits BY level`,
			want: []chartBreakPipelineRow{
				{0, "1", "0:ERROR|0:INFO", "1|1"},
				{1, "2", "0:ERROR|0:INFO", "0|1"},
			},
		},
		{
			name:   "top generated count column is the row axis",
			source: `index=chartpipe source="pipe" | top path | chart count OVER count BY path`,
			// The column domain is global, not per row: both retained labels
			// name a column on every published row, and the absent pair is 0.
			want: []chartBreakPipelineRow{
				{0, "2", "0:/a|0:/b", "0|1"},
				{1, "3", "0:/a|0:/b", "1|0"},
			},
		},
		{
			name:   "rare generated percent column is a double row axis",
			source: `index=chartpipe source="pipe" | rare path | chart count OVER percent BY path`,
			want: []chartBreakPipelineRow{
				{0, "40", "0:/a|0:/b", "0|1"},
				{1, "60", "0:/a|0:/b", "1|0"},
			},
		},
		{
			name:   "sort and head upstream",
			source: `index=chartpipe source="pipe" | sort 0 +path | head 3 | chart count OVER path BY level`,
			want:   []chartBreakPipelineRow{{0, "/a", "0:ERROR|0:INFO", "1|2"}},
		},
		{
			name:   "dedup upstream",
			source: `index=chartpipe source="pipe" | where level="INFO" | dedup path | chart count OVER path BY level`,
			want: []chartBreakPipelineRow{
				{0, "/a", "0:INFO", "1"},
				{1, "/b", "0:INFO", "1"},
			},
		},
		{
			name:   "tail upstream",
			source: `index=chartpipe source="pipe" | sort 0 +path | tail 2 | chart count OVER path BY level`,
			want:   []chartBreakPipelineRow{{0, "/b", "0:INFO|1:", "1|1"}},
		},
		{
			name:   "rename onto both axes",
			source: `index=chartpipe source="pipe" | rename path AS route, level AS lvl | chart count OVER route BY lvl`,
			want: []chartBreakPipelineRow{
				{0, "/a", "0:ERROR|0:INFO|1:", "1|2|0"},
				{1, "/b", "0:ERROR|0:INFO|1:", "0|1|1"},
			},
		},
		{
			name:   "rename away from the column axis is the NULL series",
			source: `index=chartpipe source="pipe" | rename level AS lvl | chart count OVER path BY level`,
			want: []chartBreakPipelineRow{
				{0, "/a", "1:", "3"},
				{1, "/b", "1:", "2"},
			},
		},
		{
			name:   "projection removes the column axis",
			source: `index=chartpipe source="pipe" | table path level | fields - level | chart count OVER path BY level`,
			want: []chartBreakPipelineRow{
				{0, "/a", "1:", "3"},
				{1, "/b", "1:", "2"},
			},
		},
		{
			name:   "projection removes the row axis",
			source: `index=chartpipe source="pipe" | table host level | chart count OVER path BY level`,
			want:   nil,
		},
		{
			name:   "canonical raw row axis",
			source: `index=chartpipe source="pipe-raw" | chart count OVER _raw BY level`,
			want: []chartBreakPipelineRow{
				{0, "raw-one", "0:INFO", "1"},
				{1, "raw-two", "0:INFO", "1"},
			},
		},
		{
			name:   "dotted paths on both axes",
			source: `index=chartpipe source="pipe-dotted" | chart count OVER obj.route BY obj.status`,
			// obj.status exists only on the /x event, so /y is the NULL series.
			want: []chartBreakPipelineRow{
				{0, "/x", "0:ok|1:", "1|0"},
				{1, "/y", "0:ok|1:", "0|1"},
			},
		},
		{
			name:   "driver metacharacter field names on both axes",
			source: `index=chartpipe source="pipe-meta" | chart count OVER foo?bar BY brace{x:y}`,
			want: []chartBreakPipelineRow{
				{0, "m1", "0:s1", "1"},
				{1, "m2", "0:s1", "1"},
			},
		},
		{
			name:   "chart with no base search predicate",
			source: `index=chartpipe | chart count OVER source BY level`,
			want: []chartBreakPipelineRow{
				{0, "pipe", "0:ERROR|0:INFO|1:", "1|3|1"},
				{1, "pipe-dotted", "0:ERROR|0:INFO|1:", "0|0|2"},
				{2, "pipe-meta", "0:ERROR|0:INFO|1:", "0|0|2"},
				{3, "pipe-object", "0:ERROR|0:INFO|1:", "0|0|1"},
				{4, "pipe-raw", "0:ERROR|0:INFO|1:", "0|2|0"},
			},
		},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			compiled := compile(test.source)
			got := chartBreakPipelineTransport(t, ctx, connection, compiled)
			if !reflect.DeepEqual(got, test.want) {
				t.Fatalf("%q pivot = %#v, want %#v", test.source, got, test.want)
			}
			// The comma-separated BY spelling is the same pivot and must agree
			// on real data as well as in compiled text.
			byForm := chartBreakPipelineSpellAsBy(t, test.source)
			if byGot := chartBreakPipelineTransport(t, ctx, connection, compile(byForm)); !reflect.DeepEqual(byGot, test.want) {
				t.Fatalf("%q pivot = %#v, want %#v", byForm, byGot, test.want)
			}
		})
	}

	// A flattened non-empty object parent on the row axis is the documented
	// stats BY boundary and must raise the atomic invalid flag rather than
	// silently naming a row, even though the compiler accepted the plan. Pin
	// both validation lowerings: bare count filters before grouping, while every
	// field measure retains ineligible rows for independent column validation.
	t.Run("flattened object parent row axis fails atomically", func(t *testing.T) {
		for _, aggregate := range []string{"count", "count(path)", "sum(path)"} {
			aggregate := aggregate
			t.Run(aggregate, func(t *testing.T) {
				source := fmt.Sprintf(
					`index=chartpipe source="pipe-object" | chart %s OVER obj BY level`,
					aggregate,
				)
				if got := chartBreakPipelineInvalid(
					t,
					ctx,
					connection,
					compile(source),
				); got != 1 {
					t.Fatalf("%s object-parent row invalid flag = %d, want 1", aggregate, got)
				}
			})
		}
		if got := chartBreakPipelineInvalid(t, ctx, connection,
			compile(`index=chartpipe source="pipe-object" | chart count OVER path BY obj`)); got != 1 {
			t.Fatalf("object-parent column axis invalid flag = %d, want 1", got)
		}
	})
}

func chartBreakPipelineSpellAsBy(t *testing.T, source string) string {
	t.Helper()
	marker := " | chart count OVER "
	index := strings.LastIndex(source, marker)
	if index < 0 {
		t.Fatalf("source %q has no OVER-spelled chart", source)
	}
	clause := source[index+len(marker):]
	axes := strings.SplitN(clause, " BY ", 2)
	if len(axes) != 2 {
		t.Fatalf("chart clause %q has no BY column split", clause)
	}
	return source[:index] + " | chart count BY " + axes[0] + ", " + axes[1]
}

func chartBreakPipelineTransport(
	t *testing.T,
	ctx context.Context,
	connection clickhousedriver.Conn,
	compiled CompiledQuery,
) []chartBreakPipelineRow {
	t.Helper()
	if compiled.Chart == nil {
		t.Fatalf("compiled query is not a chart: %#v", compiled)
	}
	query := "SELECT toString(" + quoteIdentifier(ChartOrdinalColumn) + "), " +
		"toString(" + quoteIdentifier(ChartRowColumn) + "), " +
		"arrayStringConcat(" + quoteIdentifier(ChartNamesColumn) + ", '|'), " +
		"arrayStringConcat(arrayMap(value -> toString(value), " + quoteIdentifier(ChartCountsColumn) + "), '|'), " +
		"toString(" + quoteIdentifier(ChartInvalidColumn) + ") FROM (" + compiled.SQL + ")"
	rows, err := connection.Query(ctx, query, compiled.Args...)
	if err != nil {
		t.Fatalf("execute chart pipeline query: %v\nSQL: %s", err, query)
	}
	defer func() {
		if closeErr := rows.Close(); closeErr != nil && !t.Failed() {
			t.Errorf("close chart pipeline rows: %v", closeErr)
		}
	}()
	var collected []chartBreakPipelineRow
	for rows.Next() {
		values := make([]string, 5)
		targets := make([]any, 5)
		for index := range values {
			targets[index] = &values[index]
		}
		if scanErr := rows.Scan(targets...); scanErr != nil {
			t.Fatalf("scan chart pipeline row: %v\nSQL: %s", scanErr, query)
		}
		if values[4] != "0" {
			t.Fatalf("chart row raised the atomic invalid flag: %#v", values)
		}
		var ordinal uint64
		if _, scanErr := fmt.Sscanf(values[0], "%d", &ordinal); scanErr != nil {
			t.Fatalf("chart ordinal %q is not a number: %v", values[0], scanErr)
		}
		collected = append(collected, chartBreakPipelineRow{
			ordinal: ordinal, row: values[1], names: values[2], counts: values[3],
		})
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate chart pipeline rows: %v\nSQL: %s", err, query)
	}
	return collected
}

func chartBreakPipelineInvalid(
	t *testing.T,
	ctx context.Context,
	connection clickhousedriver.Conn,
	compiled CompiledQuery,
) uint8 {
	t.Helper()
	query := "SELECT max(" + quoteIdentifier(ChartInvalidColumn) + ") FROM (" + compiled.SQL + ")"
	var invalid uint8
	if err := connection.QueryRow(ctx, query, compiled.Args...).Scan(&invalid); err != nil {
		t.Fatalf("execute chart validity fold: %v\nSQL: %s", err, query)
	}
	return invalid
}

func chartBreakPipelineIntegrationCompile(t *testing.T, source string, cutoff time.Time, visibilityCutoff uint64) CompiledQuery {
	t.Helper()
	parsed, err := spl.Parse(source)
	if err != nil {
		t.Fatalf("parse chart SPL %q: %v", source, err)
	}
	logical, err := plan.Build(parsed, plan.Scope{
		TenantID: chartBreakPipelineTenant, AuthorizedIndexes: []string{chartBreakPipelineIndex},
		Earliest:         time.Date(2026, time.July, 21, 0, 0, 0, 0, time.UTC),
		Latest:           time.Date(2026, time.July, 22, 0, 0, 0, 0, time.UTC),
		SearchStart:      cutoff.Add(-time.Second),
		SearchTimezone:   "UTC",
		IndexTimeCutoff:  cutoff,
		VisibilityCutoff: uint64PointerForIntegration(visibilityCutoff),
	})
	if err != nil {
		t.Fatalf("build chart SPL %q: %v", source, err)
	}
	compiled, err := (Compiler{}).Compile(logical)
	if err != nil {
		t.Fatalf("compile chart SPL %q: %v", source, err)
	}
	if got, want := strings.Count(compiled.SQL, "?"), len(compiled.Args); got != want {
		t.Fatalf("placeholder count = %d, args = %d for %q", got, want, source)
	}
	return compiled
}

// chartBreakPipelineStoreFixture writes the pipeline fixture through the store
// writer. Scenarios are separated by source so each pivot scopes to the rows it
// reasons about.
func chartBreakPipelineStoreFixture(t *testing.T, ctx context.Context, store *Store, indexTime time.Time) uint64 {
	t.Helper()
	info, failure := "INFO", "ERROR"
	var events []*ingest.StoredEvent
	add := func(event *ingest.StoredEvent) { events = append(events, event) }

	// The primary scenario: two routes, three levels of presence, and two
	// distinct ten-minute time buckets so a time bin has something to observe.
	add(chartBreakPipelineEvent("p1", "pipe", chartBreakPipelineBase, &info,
		typedField("path", typedString("/a")), typedField("duration", typedString("10ms"))))
	add(chartBreakPipelineEvent("p2", "pipe", chartBreakPipelineBase, &info,
		typedField("path", typedString("/a")), typedField("duration", typedString("10ms"))))
	add(chartBreakPipelineEvent("p3", "pipe", chartBreakPipelineBase.Add(20*time.Minute), &failure,
		typedField("path", typedString("/a")), typedField("duration", typedString("30ms"))))
	add(chartBreakPipelineEvent("p4", "pipe", chartBreakPipelineBase.Add(20*time.Minute), &info,
		typedField("path", typedString("/b")), typedField("duration", typedString("20ms"))))
	add(chartBreakPipelineEvent("p5", "pipe", chartBreakPipelineBase.Add(20*time.Minute), nil,
		typedField("path", typedString("/b")), typedField("duration", typedString("20ms"))))

	// _raw parity: the Mixed row column over ordinary UTF-8 bytes.
	rawOne := chartBreakPipelineEvent("r1", "pipe-raw", chartBreakPipelineBase, &info)
	rawOne.Event.Raw = []byte("raw-one")
	add(rawOne)
	rawTwo := chartBreakPipelineEvent("r2", "pipe-raw", chartBreakPipelineBase, &info)
	rawTwo.Event.Raw = []byte("raw-two")
	add(rawTwo)

	// Dotted leaf paths on both axes, one of them absent on the second event.
	add(chartBreakPipelineEvent("d1", "pipe-dotted", chartBreakPipelineBase, nil,
		typedField("obj", typedObject(
			typedField("route", typedString("/x")),
			typedField("status", typedString("ok")),
		))))
	add(chartBreakPipelineEvent("d2", "pipe-dotted", chartBreakPipelineBase, nil,
		typedField("obj", typedObject(typedField("route", typedString("/y"))))))

	// A flattened non-empty object parent, which is the documented stats BY
	// boundary on either axis.
	add(chartBreakPipelineEvent("o1", "pipe-object", chartBreakPipelineBase, nil,
		typedField("path", typedString("/o")),
		typedField("obj", typedObject(typedField("leaf", typedString("v"))))))

	// Field names full of ClickHouse and driver metacharacters.
	add(chartBreakPipelineEvent("m1", "pipe-meta", chartBreakPipelineBase, nil,
		typedField("foo?bar", typedString("m1")), typedField("brace{x:y}", typedString("s1"))))
	add(chartBreakPipelineEvent("m2", "pipe-meta", chartBreakPipelineBase, nil,
		typedField("foo?bar", typedString("m2")), typedField("brace{x:y}", typedString("s1"))))

	if _, err := store.Store(ctx, ingest.StoreBatch{
		TenantID: chartBreakPipelineTenant, CollectorID: "collector", BatchID: "chart-pipeline-batch",
		BatchSequence:     1,
		SourceBatchSHA256: testSourceBatchDigest("chart-pipeline-batch"),
		ReceivedAt:        indexTime,
		Events:            events,
	}); err != nil {
		t.Fatalf("store chart pipeline fixture: %v", err)
	}
	cutoff, err := store.VisibilityCutoff(ctx)
	if err != nil {
		t.Fatalf("capture chart pipeline visibility cutoff: %v", err)
	}
	return cutoff
}

func chartBreakPipelineEvent(
	id, source string,
	at time.Time,
	level *string,
	fields ...*opensplunkv1.TypedObjectField,
) *ingest.StoredEvent {
	return &ingest.StoredEvent{
		TenantID:    chartBreakPipelineTenant,
		CollectorID: "collector",
		BatchID:     "chart-pipeline-batch",
		IndexTime:   time.Date(2026, time.July, 21, 4, 0, 0, 0, time.UTC),
		Event: &opensplunkv1.LogEvent{
			EventId:         id,
			IndexName:       chartBreakPipelineIndex,
			EventTime:       timestamppb.New(at),
			CollectedAt:     timestamppb.New(at),
			EventTimeSource: opensplunkv1.EventTimeSource_EVENT_TIME_SOURCE_PARSED,
			Host:            "api",
			Source:          source,
			Sourcetype:      "go:zap:json",
			Severity:        opensplunkv1.LogSeverity_LOG_SEVERITY_INFO,
			Level:           level,
			Raw:             []byte(id),
			RawEncoding:     opensplunkv1.RawEncoding_RAW_ENCODING_UTF8,
			Message:         stringPointer("Request metrics"),
			Fields:          typedObjectValue(fields...),
		},
	}
}

// chartBreakPipelineStartClickHouse starts an isolated pinned server, applies
// the repository migrations, and returns a query connection plus a store writer.
func chartBreakPipelineStartClickHouse(t *testing.T, ctx context.Context) (clickhousedriver.Conn, *Store) {
	t.Helper()
	container := "open-splunk-chart-pipeline-" + integrationRandomHex(t, 6)
	password := integrationRandomHex(t, 24)
	image := os.Getenv("OPEN_SPLUNK_CLICKHOUSE_TEST_IMAGE")
	if image == "" {
		image = storeIntegrationImage
	}
	integrationDocker(t, ctx, nil,
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
	integrationWaitForClickHouse(t, ctx, container, password)

	migrationPaths, err := filepath.Glob(filepath.Join("..", "..", "migrations", "clickhouse", "[0-9][0-9][0-9][0-9]_*.sql"))
	if err != nil || len(migrationPaths) == 0 {
		t.Fatalf("discover migrations: paths=%v err=%v", migrationPaths, err)
	}
	var migrations bytes.Buffer
	for _, migrationPath := range migrationPaths {
		migration, readErr := os.ReadFile(migrationPath)
		if readErr != nil {
			t.Fatalf("read migration %s: %v", migrationPath, readErr)
		}
		migrations.Write(migration)
		migrations.WriteByte('\n')
	}
	integrationDocker(t, ctx, bytes.NewReader(migrations.Bytes()),
		"exec", "--interactive", container, "clickhouse-client",
		"--user", "open_splunk", "--password", password, "--multiquery",
	)

	config := DefaultConfig()
	config.Addresses = []string{integrationNativeAddress(t, ctx, container)}
	config.Username = "open_splunk"
	config.Password = password
	controlDB, err := control.Open(ctx, filepath.Join(t.TempDir(), "control.sqlite"))
	if err != nil {
		t.Fatalf("open chart pipeline visibility control database: %v", err)
	}
	t.Cleanup(func() { _ = controlDB.Close() })
	sequencer, err := visibility.NewSQLite(ctx, controlDB)
	if err != nil {
		t.Fatalf("create chart pipeline visibility sequencer: %v", err)
	}
	t.Cleanup(func() { _ = sequencer.Close() })
	// Preserve the fixed logical fixture clock without letting ClickHouse's
	// physical TTL make this integration matrix expire as wall time advances.
	store, err := Open(config, fixedRetention(100*365*24*time.Hour), sequencer)
	if err != nil {
		t.Fatalf("open chart pipeline store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	if err := store.Ping(ctx); err != nil {
		t.Fatalf("ping chart pipeline store: %v", err)
	}
	options, _, err := config.clickHouseOptions()
	if err != nil {
		t.Fatalf("resolve chart pipeline ClickHouse options: %v", err)
	}
	connection, err := clickhousedriver.Open(options)
	if err != nil {
		t.Fatalf("open chart pipeline query connection: %v", err)
	}
	t.Cleanup(func() { _ = connection.Close() })
	return connection, store
}
