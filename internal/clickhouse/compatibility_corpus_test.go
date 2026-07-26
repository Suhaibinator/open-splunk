package clickhouse

import (
	"slices"
	"strings"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/testsupport/gradethiscorpus"
)

// TestGradeThisCompatibilityCorpus keeps the product plan's ten initial SPL
// searches progressing through the complete parse, plan, and compile contract.
// Pinned ClickHouse integration tests separately exercise the emitted query
// primitives and result transport.
func TestGradeThisCompatibilityCorpus(t *testing.T) {
	t.Parallel()

	fixture := gradethiscorpus.Fixture()
	for _, search := range gradethiscorpus.Searches() {
		search := search
		t.Run(search.Name, func(t *testing.T) {
			t.Parallel()
			source, err := search.Render(fixture.TraceID)
			if err != nil {
				t.Fatal(err)
			}
			compiled := compileSPL(t, source)
			if compiled.SQL == "" || len(compiled.OutputFields) == 0 {
				t.Fatalf("compiled query is incomplete: %#v", compiled)
			}
		})
	}
}

// TestGradeThisCurrentMigrationCorpus keeps the representative current
// GradeThis/go-common investigations compiling independently from the exact
// product-plan v0.1 corpus. In particular it pins the current request-summary
// message and µs/ms/s duration extraction contract.
func TestGradeThisCurrentMigrationCorpus(t *testing.T) {
	t.Parallel()

	fixture := gradethiscorpus.MigrationFixture()
	for _, search := range gradethiscorpus.MigrationSearches() {
		search := search
		t.Run(search.Name, func(t *testing.T) {
			t.Parallel()
			source, err := search.Render(fixture.TraceID)
			if err != nil {
				t.Fatal(err)
			}
			compiled := compileSPL(t, source)
			if compiled.SQL == "" || len(compiled.OutputFields) == 0 {
				t.Fatalf("compiled current-source query is incomplete: %#v", compiled)
			}
		})
	}
}

func TestRexCompatibilityCorpus(t *testing.T) {
	t.Parallel()

	queries := []string{
		`index=gradethis | rex "method=(?<raw_method>[A-Z]+)\s+path=(?<raw_path>\S+)\s+status=(?<raw_status>\d+)" | table raw_method raw_path raw_status`,
		`index=gradethis message="Request summary statistics" | rex field=duration "^(?<duration_value>\d+(?:\.\d+)?)(?<duration_unit>µs|ms)$" | stats count BY duration_unit`,
		`index=gradethis | rex field=path "^/api/v1/(?<area>[^/?]+)(?:/(?<resource>[^/?]+))?" | stats count BY area, resource | sort -count`,
		`index=gradethis message="GORM slow query" | rex field=sql "^\s*(?<sql_verb>[A-Za-z]+)\b" | stats count BY sql_verb`,
	}
	for _, source := range queries {
		compiled := compileSPL(t, source)
		if compiled.SQL == "" || len(compiled.OutputFields) == 0 ||
			strings.Count(compiled.SQL, "extractGroups(") != 1 {
			t.Fatalf("rex corpus query is incomplete for %q: %#v", source, compiled)
		}
	}
}

func TestSpathCompatibilityCorpus(t *testing.T) {
	t.Parallel()

	queries := []string{
		`index=gradethis | spath path=request.context.trace_id | table request.context.trace_id`,
		`index=gradethis | spath input=payload output=status path=response.status | stats count BY status`,
		`index=gradethis | spath input=payload output=first_sku path=items{0}.sku | search first_sku=*`,
		`index=gradethis | spath output=server_name server.name | sort server_name`,
	}
	for _, source := range queries {
		compiled := compileSPL(t, source)
		if compiled.SQL == "" || len(compiled.OutputFields) == 0 ||
			strings.Count(compiled.SQL, "JSONExtractRaw(") != 1 ||
			strings.Count(compiled.SQL, "JSONExtract(") != 1 {
			t.Fatalf("spath corpus query is incomplete for %q: %#v", source, compiled)
		}
	}
}

func TestStatsDistinctCountCompatibilityCorpus(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(
		t,
		`index=gradethis | stats count dc(trace_id) AS unique_traces distinct_count(logger) AS unique_loggers BY level`,
	)
	if compiled.SQL == "" ||
		!slices.Equal(compiled.OutputFields, []string{"level", "count", "unique_traces", "unique_loggers"}) ||
		!strings.Contains(compiled.SQL, "groupUniqArrayArray(") ||
		strings.Contains(strings.ToUpper(compiled.SQL), "ARRAY JOIN") {
		t.Fatalf("dc corpus query is incomplete or row-expanding: %#v", compiled)
	}
}

func TestBinCompatibilityCorpus(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		source   string
		required string
	}{
		{`index=gradethis | bin _time span=5m | stats count BY _time`, "fromUnixTimestamp64Nano("},
		{`index=gradethis | bucket span=1h _time AS hour | table _time hour message`, "fromUnixTimestamp64Nano("},
		{`index=gradethis | bin severity span=10 | stats count BY severity`, `toUInt64("severity")`},
		{`index=gradethis | bin duration_ms span=10 AS band | stats count BY band`, "arrayFirstIndex("},
		{`index=gradethis | eval latency=-11.5 | bucket span=10 latency AS band | table latency band`, UnsupportedNumericBinValueMarker},
		{`index=gradethis | stats count | bin count span=10`, `toUInt64("count")`},
	} {
		compiled := compileSPL(t, test.source)
		if compiled.SQL == "" || len(compiled.OutputFields) == 0 ||
			!strings.Contains(compiled.SQL, test.required) {
			t.Fatalf("bin corpus query is incomplete for %q: %#v", test.source, compiled)
		}
	}
}

// TestChartCompatibilityCorpus keeps the bounded runtime-wide pivot progressing
// through the complete parse, plan, and compile contract, including the
// discretization contract it reuses from bin and the stats BY row axis that
// makes bin ... | chart ... work. Pinned ClickHouse integration tests separately
// prove the emitted counts.
func TestChartCompatibilityCorpus(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name     string
		source   string
		rowField string
		rowKind  ChartRowKind
		required string
	}{
		{
			// The product plan's "HTTP responses by route and status" search
			// expressed as a real pivot rather than stats count by path, status.
			name:     "responses by route and status",
			source:   `index=gradethis message="Request metrics" | chart count OVER path BY status_class`,
			rowField: "path",
			rowKind:  ChartRowKindString,
			required: `AS "` + ChartRowColumn + `"`,
		},
		{
			name:     "two-field BY spelling",
			source:   `index=gradethis message="Request metrics" | chart count BY path, status_class`,
			rowField: "path",
			rowKind:  ChartRowKindString,
			required: `AS "` + ChartRowColumn + `"`,
		},
		{
			name:     "discretized numeric row axis",
			source:   `index=gradethis | bin severity span=10 | chart count OVER severity BY level`,
			rowField: "severity",
			rowKind:  ChartRowKindUnsigned,
			required: `toUInt64("severity")`,
		},
		{
			name:     "discretized runtime-typed row axis",
			source:   `index=gradethis | bin duration_ms span=10 AS band | chart count OVER band BY level`,
			rowField: "band",
			rowKind:  ChartRowKindString,
			required: "arrayFirstIndex(",
		},
		{
			name:     "observed time buckets only",
			source:   `index=gradethis | bin _time span=5m AS bucket_time | chart count OVER bucket_time BY level`,
			rowField: "bucket_time",
			rowKind:  ChartRowKindTime,
			required: "fromUnixTimestamp64Nano(",
		},
		{
			// NULL and OTHER are always available, so a chart never drops an
			// eligible input row: the per-row total equals stats count BY path.
			name:     "null and other series",
			source:   `index=gradethis | chart count OVER path BY level`,
			rowField: "path",
			rowKind:  ChartRowKindString,
			required: `CAST('2:' AS String)`,
		},
		{
			// chart does not require event rows or canonical _time.
			name:     "pivot over a transformed relation",
			source:   `index=gradethis | stats count BY path, level | chart count OVER path BY level`,
			rowField: "path",
			rowKind:  ChartRowKindString,
			required: `AS "` + ChartCountsColumn + `"`,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			compiled := compileSPL(t, test.source)
			if compiled.SQL == "" || !slices.Equal(compiled.OutputFields, []string{test.rowField}) {
				t.Fatalf("chart corpus query is incomplete for %q: %#v", test.source, compiled)
			}
			if compiled.Chart == nil || compiled.Chart.RowField != test.rowField || compiled.Chart.RowKind != test.rowKind ||
				compiled.Chart.RowLimit != 10_000 || compiled.Chart.MaxSeries != 12 {
				t.Fatalf("chart corpus contract for %q = %#v", test.source, compiled.Chart)
			}
			if !strings.Contains(compiled.SQL, test.required) {
				t.Fatalf("chart corpus query for %q is missing %q", test.source, test.required)
			}
			if got, want := strings.Count(compiled.SQL, "?"), len(compiled.Args); got != want {
				t.Fatalf("chart corpus placeholders = %d, args = %d for %q", got, want, test.source)
			}
		})
	}
}

// TestChartTotalsMatchStatsCountByRowField pins the C8 differential at the
// compiled-contract level: the pivot's row axis is the same group column stats
// BY produces, and usenull/useother are always on, so no eligible input row can
// be dropped by the column bound.
func TestChartTotalsMatchStatsCountByRowField(t *testing.T) {
	t.Parallel()

	chart := compileSPL(t, `index=gradethis | chart count OVER path BY level`)
	stats := compileSPL(t, `index=gradethis | stats count BY path | sort 0 +path`)
	if chart.Chart == nil || chart.Chart.RowField != "path" {
		t.Fatalf("chart row contract = %#v", chart.Chart)
	}
	if !slices.Contains(stats.OutputFields, "path") {
		t.Fatalf("stats output fields = %v", stats.OutputFields)
	}
	// Both derive the row key from the identical stats BY lexical scalar pair.
	lexical := `dynamicElement("__os_fields"."path", 'Map(String, String)')[concat(char(0), 'open_splunk_value')]`
	if !strings.Contains(stats.SQL, lexical) {
		t.Fatalf("stats BY no longer uses the lexical scalar pair:\n%s", stats.SQL)
	}
	if !strings.Contains(chart.SQL, `dynamicElement("__os_ch_row_value", 'Map(String, String)')[concat(char(0), 'open_splunk_value')]`) {
		t.Fatalf("chart row axis diverged from the stats BY scalar pair:\n%s", chart.SQL)
	}
	// Both eliminate exactly the same ineligible rows: missing or explicit null.
	if !strings.Contains(chart.SQL, `"__os_ch_row_present" AS "__os_ch_row_eligible"`) ||
		!strings.Contains(chart.SQL, `WHERE "__os_ch_row_eligible" != 0 GROUP BY "__os_ch_row"`) {
		t.Fatalf("chart row eligibility diverged from stats BY:\n%s", chart.SQL)
	}
	// usenull and useother are unconditional, so the column bound cannot drop a row.
	for _, required := range []string{`CAST('1:' AS String)`, `CAST('2:' AS String)`} {
		if !strings.Contains(chart.SQL, required) {
			t.Fatalf("chart omitted the %q series:\n%s", required, chart.SQL)
		}
	}
}
