package clickhouse

import (
	"errors"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/plan"
	"github.com/Suhaibinator/open-splunk/internal/spl"
)

// chartBreakPipelineUpstream is one hostile pipeline prefix plus the two axes
// charted from it. Every supported command family that can precede the
// terminal pivot appears at least once, including the producers whose output
// column is runtime-typed, statically null, renamed, or projected away.
type chartBreakPipelineUpstream struct {
	name     string
	upstream string
	row      string
	column   string
}

func chartBreakPipelineUpstreams() []chartBreakPipelineUpstream {
	return []chartBreakPipelineUpstream{
		{"bare scan", `index=gradethis`, "path", "level"},
		{"base search predicate", `index=gradethis level=ERROR status>=500`, "path", "level"},
		{"search command", `index=gradethis | search status>=500`, "path", "level"},
		{"where command", `index=gradethis | where severity > 3`, "path", "level"},

		{"eval string literal", `index=gradethis | eval area="fixed"`, "path", "area"},
		{"eval null literal as row", `index=gradethis | eval n=null`, "n", "level"},
		{"eval bool literal", `index=gradethis | eval flag=true`, "flag", "level"},
		{"eval signed literal", `index=gradethis | eval offset=-3`, "offset", "level"},
		{"eval unsigned literal", `index=gradethis | eval big=18446744073709551615`, "big", "level"},
		{"eval float literal", `index=gradethis | eval ratio=1.25`, "ratio", "level"},
		{"eval field copy", `index=gradethis | eval copied=path`, "copied", "level"},
		{"eval replace produces a string column", `index=gradethis | eval trimmed=replace(duration, "ms$", "")`, "path", "trimmed"},
		{"eval tonumber produces a numeric row", `index=gradethis | eval duration_ms=tonumber(replace(duration, "ms$", ""))`, "duration_ms", "level"},
		{"eval chained assignments", `index=gradethis | eval first=replace(duration, "ms$", ""), second=tonumber(first)`, "second", "level"},
		{"eval overwrites a canonical column", `index=gradethis | eval message=replace(message, "old", "new")`, "path", "message"},

		{"rex capture as column", `index=gradethis | rex field=path "^/api/v1/(?<area>[^/?]+)"`, "path", "area"},
		{"rex capture as row", `index=gradethis | rex field=path "^/api/v1/(?<area>[^/?]+)"`, "area", "level"},
		{"rex captures on both axes", `index=gradethis | rex "method=(?<method>[A-Z]+)\s+path=(?<route>\S+)"`, "route", "method"},
		{"rex overwriting its own input", `index=gradethis | rex field=status "^(?<status>\d+)-(?<tail>.*)$"`, "status", "tail"},

		{"rename onto the column axis", `index=gradethis | rename level AS lvl`, "path", "lvl"},
		{"rename away from the column axis", `index=gradethis | rename level AS lvl`, "path", "level"},
		{"rename onto the row axis", `index=gradethis | rename path AS route`, "route", "level"},
		{"rename away from the row axis", `index=gradethis | rename path AS route`, "path", "level"},
		{"rename swaps both axes", `index=gradethis | rename path AS level, level AS path`, "path", "level"},

		{"table projection keeps both axes", `index=gradethis | table path level`, "path", "level"},
		{"table projection drops the column axis", `index=gradethis | table path level`, "path", "host"},
		{"table projection drops the row axis", `index=gradethis | table host level`, "path", "level"},
		{"fields projection", `index=gradethis | fields path level`, "path", "level"},
		{"fields minus removes the column axis", `index=gradethis | table path level | fields - level`, "path", "level"},
		{"fields minus removes the row axis", `index=gradethis | table path level | fields - path`, "path", "level"},

		{"sort descending", `index=gradethis | sort -_time`, "path", "level"},
		{"sort unlimited ascending", `index=gradethis | sort 0 +path`, "path", "level"},
		{"head", `index=gradethis | head 100`, "path", "level"},
		{"tail", `index=gradethis | tail 100`, "path", "level"},
		{"dedup one field", `index=gradethis | dedup path`, "path", "level"},
		{"dedup counted multi-field", `index=gradethis | dedup 2 status, host`, "path", "level"},

		{"stats group columns", `index=gradethis | stats count BY path, level`, "path", "level"},
		{"stats implicit count column as the row axis", `index=gradethis | stats count BY path, level`, "count", "level"},
		{"stats renamed aggregate", `index=gradethis | stats count AS hits BY path, level`, "path", "level"},
		{"stats renamed aggregate as the row axis", `index=gradethis | stats count AS hits BY path, level`, "hits", "level"},
		{"stats percentile alias as the row axis", `index=gradethis | stats p95(severity) AS p95_severity BY path, level`, "p95_severity", "level"},
		{"stats sum alias as the row axis", `index=gradethis | stats sum(severity) AS total BY path, level`, "total", "level"},

		{"top generated count column", `index=gradethis | top message`, "count", "message"},
		{"top generated percent column", `index=gradethis | top message`, "percent", "message"},
		{"rare generated count column", `index=gradethis | rare message`, "count", "message"},

		{"numeric bin in place", `index=gradethis | bin severity span=10`, "severity", "level"},
		{"numeric bin with AS", `index=gradethis | bin severity span=10 AS band`, "band", "level"},
		{"bucket alias", `index=gradethis | bucket severity span=10`, "severity", "level"},
		{"time bin in place", `index=gradethis | bin _time span=5m`, "_time", "level"},
		{"time bin with AS", `index=gradethis | bin _time span=5m AS bucket_time`, "bucket_time", "level"},
		{"runtime numeric bin", `index=gradethis | bin metric span=10 AS band`, "band", "level"},

		{"canonical raw row axis", `index=gradethis`, "_raw", "level"},
		{"canonical time row axis", `index=gradethis`, "_time", "level"},
		{"canonical indextime row axis", `index=gradethis`, "_indextime", "level"},

		{
			name: "deep mixed pipeline",
			upstream: `index=gradethis message="Request metrics" status>=500 ` +
				`| rex field=path "^/api/v1/(?<area>[^/?]+)" ` +
				`| eval duration_ms=tonumber(replace(duration, "ms$", "")) ` +
				`| bin duration_ms span=100 AS latency_band ` +
				`| where severity>1 ` +
				`| sort 0 +path ` +
				`| head 5000 ` +
				`| dedup path`,
			row:    "latency_band",
			column: "area",
		},
		{
			name:     "chart over a transformed relation",
			upstream: `index=gradethis | stats count BY path, level | sort 0 +path | head 100`,
			row:      "path",
			column:   "level",
		},
	}
}

// TestChartBreakPipelineSpellingsAgreeAfterEverySupportedUpstream pins that the
// two documented spellings really are one pivot: after every supported command
// family the OVER form, the comma-separated BY form, and the whitespace-only BY
// form must compile to byte-identical SQL, identical arguments, and identical
// public contracts. A divergence here would make the accepted surface depend on
// how the user spelled a command that carries no semantics of its own.
func TestChartBreakPipelineSpellingsAgreeAfterEverySupportedUpstream(t *testing.T) {
	t.Parallel()

	for _, test := range chartBreakPipelineUpstreams() {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			over := test.upstream + " | chart count OVER " + test.row + " BY " + test.column
			comma := test.upstream + " | chart count BY " + test.row + ", " + test.column
			space := test.upstream + " | chart count BY " + test.row + " " + test.column

			reference := chartBreakPipelineCompile(t, over)
			if reference.Chart == nil {
				t.Fatalf("%q lost its chart contract", over)
			}
			if !slices.Equal(reference.OutputFields, []string{test.row}) {
				t.Fatalf("%q public fixed fields = %v, want the row field only", over, reference.OutputFields)
			}
			if reference.Timechart != nil {
				t.Fatalf("%q declared a timechart contract", over)
			}
			for _, spelling := range []string{comma, space} {
				other := chartBreakPipelineCompile(t, spelling)
				if other.SQL != reference.SQL {
					t.Fatalf("spelling %q diverged from %q:\n%s\n\n%s", spelling, over, reference.SQL, other.SQL)
				}
				if !reflect.DeepEqual(other.Args, reference.Args) {
					t.Fatalf("spelling %q arguments = %#v, want %#v", spelling, other.Args, reference.Args)
				}
				if !reflect.DeepEqual(other.Chart, reference.Chart) {
					t.Fatalf("spelling %q chart contract = %#v, want %#v", spelling, other.Chart, reference.Chart)
				}
				if !slices.Equal(other.OutputFields, reference.OutputFields) {
					t.Fatalf("spelling %q output fields = %v, want %v", spelling, other.OutputFields, reference.OutputFields)
				}
			}

			chartBreakPipelineAssertPlaceholders(t, over, reference)
			// The pivot is a single scoped scan no matter how tall the
			// upstream is: re-reading the scan would double every predicate's
			// cost and could disagree with itself between the two aggregates.
			if got := strings.Count(reference.SQL, `FROM "open_splunk"."events"`); got != 1 {
				t.Fatalf("%q scanned the event table %d times:\n%s", over, got, reference.SQL)
			}
			if len(reference.SQL) > maxCompiledQueryBytes {
				t.Fatalf("%q compiled to %d bytes, ceiling is %d", over, len(reference.SQL), maxCompiledQueryBytes)
			}
		})
	}
}

// TestChartBreakPipelineKeywordAndReservedFieldNames charts fields whose names
// collide with chart's own grammar keywords, with the pivot's generated series
// names, and with ClickHouse/driver metacharacters. The row field name crosses
// into the SQL both as a quoted identifier and as a bound argument, so a
// mistake here is either an injection or a placeholder-count desynchronization.
func TestChartBreakPipelineKeywordAndReservedFieldNames(t *testing.T) {
	t.Parallel()

	seventeen := strings.Repeat("seg.", 16) + "leaf"
	eighteen := strings.Repeat("seg.", 17) + "leaf"

	for _, test := range []struct {
		name   string
		source string
		row    string
	}{
		{"row field named over", `index=gradethis | chart count OVER over BY level`, "over"},
		{"column field named over", `index=gradethis | chart count OVER level BY over`, "level"},
		{"row field named by", `index=gradethis | chart count OVER by BY level`, "by"},
		{"column field named by", `index=gradethis | chart count OVER path BY by`, "path"},
		{"row field named count", `index=gradethis | chart count OVER count BY level`, "count"},
		{"column field named count", `index=gradethis | chart count OVER path BY count`, "path"},
		{"row field named chart", `index=gradethis | chart count OVER chart BY level`, "chart"},
		{"row field named where", `index=gradethis | chart count OVER where BY level`, "where"},
		{"row field named as", `index=gradethis | chart count OVER as BY level`, "as"},
		{"row field named span", `index=gradethis | chart count OVER span BY level`, "span"},
		{"row field named limit", `index=gradethis | chart count OVER limit BY level`, "limit"},
		{"canonical raw row", `index=gradethis | chart count OVER _raw BY level`, "_raw"},
		{"canonical time row", `index=gradethis | chart count OVER _time BY level`, "_time"},
		{"underscore-prefixed dynamic row", `index=gradethis | chart count OVER _custom BY level`, "_custom"},
		{"driver metacharacters on both axes", `index=gradethis | chart count OVER foo?bar BY brace{x:y}`, "foo?bar"},
		{"dollar and backslash metacharacters", `index=gradethis | chart count OVER total$1 BY quote\.d`, "total$1"},
		{"dotted paths at the segment ceiling", `index=gradethis | chart count OVER ` + seventeen + ` BY level`, seventeen},
		{"dotted column path at the segment ceiling", `index=gradethis | chart count OVER path BY ` + seventeen, "path"},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			compiled := chartBreakPipelineCompile(t, test.source)
			if compiled.Chart == nil || compiled.Chart.RowField != test.row {
				t.Fatalf("chart contract = %#v, want row field %q", compiled.Chart, test.row)
			}
			if !slices.Equal(compiled.OutputFields, []string{test.row}) {
				t.Fatalf("public fixed fields = %v, want %q", compiled.OutputFields, test.row)
			}
			chartBreakPipelineAssertPlaceholders(t, test.source, compiled)
			// Every field name reaches SQL only through quoteIdentifier or a
			// bound argument; the raw spelling must never survive.
			for _, marker := range []string{`"foo?bar"`, `"brace{x:y}"`, `"total$1"`, `"quote\.d"`} {
				if strings.Contains(compiled.SQL, marker) {
					t.Fatalf("compiled SQL retained the unsafe identifier %s:\n%s", marker, compiled.SQL)
				}
			}
		})
	}

	// One segment past the documented ceiling is a planning rejection, not a
	// silently truncated path.
	parsed, err := spl.Parse(`index=gradethis | chart count OVER ` + eighteen + ` BY level`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if _, err = plan.Build(parsed, testChartScope()); err == nil {
		t.Fatal("an 18-segment chart row path was accepted")
	} else {
		diagnostic := &plan.Diagnostic{}
		if !errors.As(err, &diagnostic) || diagnostic.Code != "SPL_QUERY_TOO_COMPLEX" {
			t.Fatalf("Build error = %#v, want SPL_QUERY_TOO_COMPLEX", err)
		}
	}
}

// TestChartBreakPipelineRejectsCompilerPrivateAxisNames proves the pivot cannot
// be pointed at the compiler's own private namespace, whose identifiers name
// the physical transport columns the executor decodes.
func TestChartBreakPipelineRejectsCompilerPrivateAxisNames(t *testing.T) {
	t.Parallel()

	for _, source := range []string{
		`index=gradethis | chart count OVER __os_chart_row BY level`,
		`index=gradethis | chart count OVER path BY __os_ch_label`,
		`index=gradethis | chart count OVER __OS_CHART_ORDINAL BY level`,
	} {
		parsed, err := spl.Parse(source)
		if err != nil {
			t.Fatalf("Parse(%q): %v", source, err)
		}
		_, err = plan.Build(parsed, testChartScope())
		diagnostic := &plan.Diagnostic{}
		ok := errors.As(err, &diagnostic)
		if !ok || diagnostic.Code != "SPL_RESERVED_FIELD" {
			t.Fatalf("Build(%q) error = %#v, want SPL_RESERVED_FIELD", source, err)
		}
	}
}

// TestChartBreakPipelineColumnAxisTypeRejectionSurvivesEveryProducer pins the
// documented asymmetry against each upstream that can manufacture a non-string
// column: the same value is a legal row label and a fatal column label, and the
// rejection is a compile-time diagnostic located on the column field.
func TestChartBreakPipelineColumnAxisTypeRejectionSurvivesEveryProducer(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name   string
		source string
		column string
	}{
		{"canonical numeric column", `index=gradethis | chart count OVER path BY severity`, "severity"},
		{"canonical timestamp column", `index=gradethis | chart count OVER path BY _time`, "_time"},
		{"canonical index-time column", `index=gradethis | chart count OVER path BY _indextime`, "_indextime"},
		{"binned numeric column", `index=gradethis | bin severity span=10 | chart count OVER path BY severity`, "severity"},
		{"binned time column", `index=gradethis | bin _time span=5m AS bt | chart count OVER path BY bt`, "bt"},
		{"eval bool column", `index=gradethis | eval flag=true | chart count OVER path BY flag`, "flag"},
		{"eval signed column", `index=gradethis | eval offset=-3 | chart count OVER path BY offset`, "offset"},
		{"eval float column", `index=gradethis | eval ratio=1.25 | chart count OVER path BY ratio`, "ratio"},
		{"eval tonumber column", `index=gradethis | eval n=tonumber(duration) | chart count OVER path BY n`, "n"},
		{"stats count column", `index=gradethis | stats count BY path, level | chart count OVER path BY count`, "count"},
		{"stats renamed aggregate column", `index=gradethis | stats count AS hits BY path, level | chart count OVER path BY hits`, "hits"},
		{"top percent column", `index=gradethis | top message | chart count OVER message BY percent`, "percent"},
		{"rare count column", `index=gradethis | rare message | chart count OVER message BY count`, "count"},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			logical := chartBreakPipelineBuild(t, test.source)
			_, err := (Compiler{}).Compile(logical)
			diagnostic := &plan.Diagnostic{}
			ok := errors.As(err, &diagnostic)
			if !ok || diagnostic.Code != "SPL_UNSUPPORTED_CHART_FIELD_TYPE" {
				t.Fatalf("Compile(%q) error = %#v, want SPL_UNSUPPORTED_CHART_FIELD_TYPE", test.source, err)
			}
			chart := logical.Operators[len(logical.Operators)-1].(*plan.Chart)
			if diagnostic.Range != chart.SplitBy.Range {
				t.Fatalf("diagnostic range = %#v, want the column field range %#v", diagnostic.Range, chart.SplitBy.Range)
			}
			// The mirrored pivot keeps the same value as a legal row label.
			mirrored := strings.Replace(test.source, "chart count OVER ", "chart count OVER "+test.column+" BY ", 1)
			mirrored = mirrored[:strings.LastIndex(mirrored, " BY "+test.column)]
			if compiled := chartBreakPipelineCompile(t, mirrored); compiled.Chart == nil ||
				compiled.Chart.RowField != test.column {
				t.Fatalf("mirrored pivot %q lost the row axis: %#v", mirrored, compiled.Chart)
			}
		})
	}
}

// TestChartBreakPipelineRejectsReservedSeriesNamesFromEveryProducer pins S8: a
// field literally spelled NULL or OTHER can never name an axis, no matter which
// upstream created it, because usenull and useother always publish those series.
func TestChartBreakPipelineRejectsReservedSeriesNamesFromEveryProducer(t *testing.T) {
	t.Parallel()

	for _, source := range []string{
		`index=gradethis | chart count OVER NULL BY level`,
		`index=gradethis | chart count OVER path BY NULL`,
		`index=gradethis | chart count OVER OTHER BY level`,
		`index=gradethis | chart count OVER path BY OTHER`,
		`index=gradethis | chart count BY NULL, OTHER`,
		`index=gradethis | rename level AS OTHER | chart count OVER path BY OTHER`,
		`index=gradethis | rex field=path "(?<OTHER>[a-z]+)" | chart count OVER OTHER BY level`,
		`index=gradethis | stats count BY path, level | rename level AS NULL | chart count OVER path BY NULL`,
		`index=gradethis | bin severity span=10 AS NULL | chart count OVER NULL BY level`,
	} {
		parsed, err := spl.Parse(source)
		if err != nil {
			t.Fatalf("Parse(%q): %v", source, err)
		}
		_, err = plan.Build(parsed, testChartScope())
		diagnostic := &plan.Diagnostic{}
		ok := errors.As(err, &diagnostic)
		if !ok || diagnostic.Code != "SPL_UNSUPPORTED_CHART_FIELD_TYPE" {
			t.Fatalf("Build(%q) error = %#v, want SPL_UNSUPPORTED_CHART_FIELD_TYPE", source, err)
		}
	}

	// Case matters: only the exact reserved spellings collide with the
	// generated series, so a lowercase field of the same word still charts.
	for _, source := range []string{
		`index=gradethis | chart count OVER null BY level`,
		`index=gradethis | chart count OVER other BY level`,
		`index=gradethis | chart count OVER path BY other`,
	} {
		if compiled := chartBreakPipelineCompile(t, source); compiled.Chart == nil {
			t.Fatalf("%q lost its chart contract", source)
		}
	}
}

// TestChartBreakPipelineReservedFieldsPayloadDependsOnUpstreamSchema pins that
// the reserved convenience column is rejected exactly while the upstream schema
// stays open, and becomes an ordinary axis the moment a projection or an
// aggregation closes it.
func TestChartBreakPipelineReservedFieldsPayloadDependsOnUpstreamSchema(t *testing.T) {
	t.Parallel()

	for _, source := range []string{
		`index=gradethis | chart count OVER fields BY level`,
		`index=gradethis | chart count OVER path BY fields`,
		`index=gradethis | where severity>1 | sort 0 +path | head 10 | chart count OVER fields BY level`,
		`index=gradethis | rename level AS fields | chart count OVER fields BY path`,
		`index=gradethis | eval fields="x" | chart count OVER path BY fields`,
	} {
		parsed, err := spl.Parse(source)
		if err != nil {
			t.Fatalf("Parse(%q): %v", source, err)
		}
		if _, err = plan.Build(parsed, testChartScope()); err == nil {
			t.Fatalf("Build(%q) accepted the reserved payload", source)
		}
	}

	for _, source := range []string{
		`index=gradethis | table fields path | chart count OVER fields BY path`,
		`index=gradethis | stats count BY level, path | rename level AS fields | chart count OVER fields BY path`,
	} {
		compiled := chartBreakPipelineCompile(t, source)
		if compiled.Chart == nil || compiled.Chart.RowField != "fields" {
			t.Fatalf("%q rejected a closed-schema fields column: %#v", source, compiled.Chart)
		}
		chartBreakPipelineAssertPlaceholders(t, source, compiled)
	}
}

// TestChartBreakPipelineProjectionsThatDropAnAxis pins I2/I3 through every
// projection form: a chart field a projection removed becomes the documented
// missing-value behavior rather than resurrecting the private event document.
func TestChartBreakPipelineProjectionsThatDropAnAxis(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name       string
		source     string
		rowMissing bool
	}{
		{"table drops the column axis", `index=gradethis | table path host | chart count OVER path BY level`, false},
		{"table drops the row axis", `index=gradethis | table host level | chart count OVER path BY level`, true},
		{"fields drops the column axis", `index=gradethis | fields path host | chart count OVER path BY level`, false},
		{"fields minus drops the column axis", `index=gradethis | table path level | fields - level | chart count OVER path BY level`, false},
		{"fields minus drops the row axis", `index=gradethis | table path level | fields - path | chart count OVER path BY level`, true},
		{"rename tombstones the column axis", `index=gradethis | table path level | rename level AS lvl | chart count OVER path BY level`, false},
		{"rename tombstones the row axis", `index=gradethis | table path level | rename path AS route | chart count OVER path BY level`, true},
		{"stats closes the schema without the column axis", `index=gradethis | stats count BY path | chart count OVER path BY level`, false},
		{"stats closes the schema without the row axis", `index=gradethis | stats count BY level | chart count OVER path BY level`, true},
		{"top closes the schema without the row axis", `index=gradethis | top message | chart count OVER path BY message`, true},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			compiled := chartBreakPipelineCompile(t, test.source)
			if compiled.Chart == nil {
				t.Fatalf("%q lost its chart contract", test.source)
			}
			missingRow := strings.Contains(compiled.SQL, `CAST(NULL AS Nullable(String)) AS "__os_ch_row_value"`)
			missingColumn := strings.Contains(compiled.SQL, `CAST(NULL AS Nullable(String)) AS "__os_ch_value"`)
			if missingRow != test.rowMissing {
				t.Fatalf("%q missing-row transport = %t, want %t:\n%s", test.source, missingRow, test.rowMissing, compiled.SQL)
			}
			if test.rowMissing && !slices.Equal(compiled.OutputFields, []string{"path"}) {
				t.Fatalf("%q changed the declared schema: %v", test.source, compiled.OutputFields)
			}
			if !test.rowMissing && !missingColumn {
				t.Fatalf("%q did not turn the removed column field into a NULL series:\n%s", test.source, compiled.SQL)
			}
			chartBreakPipelineAssertPlaceholders(t, test.source, compiled)
		})
	}
}

// TestChartBreakPipelineRejectsTransformingConsumers pins that a completed
// chart job is not an event relation: field analysis, the timeline, and the
// field summary all refuse it exactly as they refuse timechart.
func TestChartBreakPipelineRejectsTransformingConsumers(t *testing.T) {
	t.Parallel()

	for _, source := range []string{
		`index=gradethis | chart count OVER path BY level`,
		`index=gradethis | chart count BY path, level`,
		`index=gradethis | bin _time span=5m | chart count OVER _time BY level`,
		`index=gradethis | stats count BY path, level | chart count OVER path BY level`,
	} {
		logical := chartBreakPipelineBuild(t, source)
		if err := plan.ValidateFieldAnalysisEligibility(logical); err == nil {
			t.Fatalf("field analysis accepted %q", source)
		}
		if err := plan.ValidateTimelineEligibility(logical); err == nil {
			t.Fatalf("timeline accepted %q", source)
		}
		if _, err := (Compiler{}).CompileTimeline(logical, TimelineSpec{
			FirstBucket: testChartScope().Earliest,
			SpanSeconds: 300,
			BucketCount: 12,
			Earliest:    testChartScope().Earliest,
			Latest:      testChartScope().Latest,
		}); err == nil {
			t.Fatalf("timeline compilation accepted %q", source)
		}
		if _, err := (Compiler{}).CompileFieldSummary(logical, FieldSummarySpec{
			FieldName:             "level",
			MaximumValues:         10,
			MaximumDistinctValues: 1_000,
			MaximumValueBytes:     4_096,
		}); err == nil {
			t.Fatalf("field summary accepted %q", source)
		}
	}
}

// TestChartBreakPipelineAtTheCommandBudgetStillCompiles pins the interaction
// between the parser's 64-command budget and the pivot's extra CTE stages: a
// pipeline that fills the command budget and then charts must still land inside
// the compiled-SQL ceiling with exact placeholder accounting.
func TestChartBreakPipelineAtTheCommandBudgetStillCompiles(t *testing.T) {
	t.Parallel()

	var source strings.Builder
	source.WriteString("index=gradethis")
	for index := 0; index < 63; index++ {
		source.WriteString(" | eval f")
		source.WriteString(strconv.Itoa(index))
		source.WriteString(`=replace(duration, "ms$", "")`)
	}
	source.WriteString(" | chart count OVER f62 BY level")

	parsed, err := spl.Parse(source.String())
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if len(parsed.Commands) != 64 {
		t.Fatalf("command count = %d, want the full 64-command budget", len(parsed.Commands))
	}
	compiled := chartBreakPipelineCompile(t, source.String())
	if compiled.Chart == nil || compiled.Chart.RowField != "f62" {
		t.Fatalf("budget-filling pipeline lost its chart contract: %#v", compiled.Chart)
	}
	if len(compiled.SQL) > maxCompiledQueryBytes {
		t.Fatalf("compiled to %d bytes, ceiling is %d", len(compiled.SQL), maxCompiledQueryBytes)
	}
	chartBreakPipelineAssertPlaceholders(t, "budget-filling pipeline", compiled)
}

// TestChartBreakPipelineFlattenedObjectAxesStayParameterized charts the
// flattened object parent on each axis. Those axes need an extra descendant
// probe whose bind argument is appended after the nested scan's arguments, so
// the pivot is where an off-by-one in placeholder ordering would surface.
func TestChartBreakPipelineFlattenedObjectAxesStayParameterized(t *testing.T) {
	t.Parallel()

	for _, source := range []string{
		`index=gradethis | chart count OVER object_parent BY level`,
		`index=gradethis | chart count OVER path BY object_parent`,
		`index=gradethis | chart count OVER object_parent BY object_other`,
		`index=gradethis | eval copied=object_parent | chart count OVER copied BY level`,
		`index=gradethis | eval copied=object_parent | chart count OVER path BY copied`,
	} {
		compiled := chartBreakPipelineCompile(t, source)
		if compiled.Chart == nil {
			t.Fatalf("%q lost its chart contract", source)
		}
		chartBreakPipelineAssertPlaceholders(t, source, compiled)
		if !strings.Contains(compiled.SQL, "arrayExists(") {
			t.Fatalf("%q emitted no descendant probe:\n%s", source, compiled.SQL)
		}
	}
}

// TestChartBreakPipelineIsRejectedBeforeEveryDownstreamCommand pins the
// terminal rule with the exact documented code and source range: the
// diagnostic is located on the following command, whichever it is.
func TestChartBreakPipelineIsRejectedBeforeEveryDownstreamCommand(t *testing.T) {
	t.Parallel()

	prefix := `index=gradethis | chart count OVER path BY level | `
	for _, downstream := range []string{
		`search path="x"`,
		`where path="x"`,
		`eval x=1`,
		`rex field=path "(?<a>[a-z]+)"`,
		`rename path AS route`,
		`fields path`,
		`table path`,
		`sort 0 +path`,
		`dedup path`,
		`head 1`,
		`tail 1`,
		`stats count BY path`,
		`top path`,
		`rare path`,
		`bin severity span=10`,
		`timechart span=5m count BY level`,
		`chart count OVER path BY level`,
	} {
		source := prefix + downstream
		parsed, err := spl.Parse(source)
		if err != nil {
			t.Fatalf("Parse(%q): %v", source, err)
		}
		_, err = plan.Build(parsed, testChartScope())
		diagnostic := &plan.Diagnostic{}
		ok := errors.As(err, &diagnostic)
		if !ok || diagnostic.Code != "SPL_UNSUPPORTED_CHART_PIPELINE" {
			t.Fatalf("Build(%q) error = %#v, want SPL_UNSUPPORTED_CHART_PIPELINE", source, err)
		}
		located := source[diagnostic.Range.Start.Offset:diagnostic.Range.End.Offset]
		if !strings.HasPrefix(located, strings.SplitN(downstream, " ", 2)[0]) {
			t.Fatalf("Build(%q) located the rejection at %q, want the following command", source, located)
		}
	}
}

// TestChartBreakPipelineNarrowsIndexScopeAndStopsAtTheChart pins that the
// terminal pivot does not widen or forget the resolved index scope, and that a
// forbidden index behind a chart is still a scope rejection.
func TestChartBreakPipelineNarrowsIndexScopeAndStopsAtTheChart(t *testing.T) {
	t.Parallel()

	scope := plan.Scope{
		TenantID:          "tenant-1",
		AuthorizedIndexes: []string{"gradethis", "internal"},
		Earliest:          testChartScope().Earliest,
		Latest:            testChartScope().Latest,
		SearchStart:       testChartScope().SearchStart,
		SearchTimezone:    testChartScope().SearchTimezone,
		IndexTimeCutoff:   testChartScope().IndexTimeCutoff,
		VisibilityCutoff:  testChartScope().VisibilityCutoff,
	}

	requested := scope
	requested.RequestedIndexes = []string{"gradethis"}
	narrowed, err := plan.Build(mustParseChartBreakPipeline(t, `index=gradethis | chart count OVER path BY level`), requested)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if !slices.Equal(narrowed.EffectiveIndexes, []string{"gradethis"}) {
		t.Fatalf("effective indexes = %v, want the requested scope", narrowed.EffectiveIndexes)
	}
	if scan := narrowed.Operators[0].(*plan.Scan); !slices.Equal(scan.Indexes, []string{"gradethis"}) {
		t.Fatalf("scan indexes = %v, want the requested scope", scan.Indexes)
	}

	// A field that only some scopes populate is still an ordinary runtime axis:
	// the pivot never requires the field to exist in every scanned index.
	both, err := plan.Build(
		mustParseChartBreakPipeline(t, `index=gradethis OR index=internal | chart count OVER only_in_internal BY level`), scope)
	if err != nil {
		t.Fatalf("Build(multi-index): %v", err)
	}
	if !slices.Equal(both.EffectiveIndexes, []string{"gradethis", "internal"}) {
		t.Fatalf("multi-index effective indexes = %v", both.EffectiveIndexes)
	}
	compiled, err := (Compiler{}).Compile(both)
	if err != nil {
		t.Fatalf("Compile(multi-index): %v", err)
	}
	if compiled.Chart == nil || compiled.Chart.RowField != "only_in_internal" {
		t.Fatalf("multi-index chart contract = %#v", compiled.Chart)
	}
	if got := strings.Count(compiled.SQL, "?"); got != len(compiled.Args) {
		t.Fatalf("placeholder count = %d, args = %d", got, len(compiled.Args))
	}

	// An unauthorized index in the base search is refused before the terminal
	// rule ever runs, so a chart cannot be used to smuggle a scope past
	// resolution.
	_, err = plan.Build(mustParseChartBreakPipeline(t, `index=forbidden | chart count OVER path BY level`), scope)
	diagnostic := &plan.Diagnostic{}
	if !errors.As(err, &diagnostic) || diagnostic.Code != "SPL_INDEX_FORBIDDEN" {
		t.Fatalf("Build(forbidden index) error = %#v, want SPL_INDEX_FORBIDDEN", err)
	}

	// Index-reference collection stops at chart: a search behind the pivot is
	// never consulted for scope, so the terminal-pipeline rule is what rejects
	// it. Otherwise the diagnostic a user sees would depend on whether the
	// smuggled index happened to be authorized.
	for _, source := range []string{
		`index=gradethis | chart count OVER path BY level | search index=forbidden`,
		`index=gradethis | chart count OVER path BY level | search index=internal`,
	} {
		_, err := plan.Build(mustParseChartBreakPipeline(t, source), requested)
		diagnostic := &plan.Diagnostic{}
		ok := errors.As(err, &diagnostic)
		if !ok || diagnostic.Code != "SPL_UNSUPPORTED_CHART_PIPELINE" {
			t.Fatalf("Build(%q) error = %#v, want SPL_UNSUPPORTED_CHART_PIPELINE", source, err)
		}
	}
}

// TestChartBreakPipelineStaticNullColumnAxisIsTheDocumentedNullSeries pins the
// documented column-axis domain: "This version supports string column values
// plus missing/explicit-null", and the fatal set is enumerated as "Numeric,
// Boolean, timestamp, extended, list, and object column values" — null is not
// in it. The contract also fixes the type of a statically null field in the
// row-axis section: `| eval n=null | chart count OVER n BY level` "is the
// String group column stats count BY n publishes". A field whose compile-time
// type is that same String column, carrying only explicit nulls, must therefore
// produce the documented usenull=true NULL series, exactly as a column field a
// projection removed already does.
//
// It does not: the compiler treats the null literal's field kind as an
// unsupported column type and fails the whole search. The result is that the
// clearer of the two "explicit-null" spellings is rejected while the vaguer one
// (a missing field) is accepted.
func TestChartBreakPipelineStaticNullColumnAxisIsTheDocumentedNullSeries(t *testing.T) {
	t.Parallel()

	// Baseline: a column field a projection removed is the documented NULL
	// series, so "no present, non-null column value on any row" is already a
	// supported shape.
	removed := chartBreakPipelineCompile(t, `index=gradethis | table path level | fields - level | chart count OVER path BY level`)
	if !strings.Contains(removed.SQL, `CAST(NULL AS Nullable(String)) AS "__os_ch_value"`) {
		t.Fatalf("removed column field is not the typed NULL series:\n%s", removed.SQL)
	}

	// The same shape written as an explicit null literal must behave the same.
	for _, source := range []string{
		`index=gradethis | eval n=null | chart count OVER path BY n`,
		`index=gradethis | eval n=null | table path n | chart count OVER path BY n`,
		`index=gradethis | eval n=null, m=n | chart count OVER path BY m`,
		`index=gradethis | eval n=null | rename n AS m | chart count OVER path BY m`,
	} {
		logical := chartBreakPipelineBuild(t, source)
		compiled, err := (Compiler{}).Compile(logical)
		if err != nil {
			t.Fatalf("Compile(%q) error = %#v, want the documented NULL series", source, err)
		}
		if compiled.Chart == nil {
			t.Fatalf("%q lost its chart contract", source)
		}
		if !strings.Contains(compiled.SQL, `CAST(NULL AS Nullable(String)) AS "__os_ch_value"`) {
			t.Fatalf("%q did not publish the explicit-null column as the NULL series:\n%s", source, compiled.SQL)
		}
		chartBreakPipelineAssertPlaceholders(t, source, compiled)
	}

	// The mirrored pivot proves the type really is the String group column: the
	// same field is accepted without complaint on the row axis.
	if compiled := chartBreakPipelineCompile(t, `index=gradethis | eval n=null | chart count OVER n BY level`); compiled.Chart == nil ||
		compiled.Chart.RowKind != ChartRowKindString || compiled.Chart.RowDatabaseType != "String" {
		t.Fatalf("static null row axis contract = %#v", compiled.Chart)
	}
}

func chartBreakPipelineAssertPlaceholders(t *testing.T, source string, compiled CompiledQuery) {
	t.Helper()
	if got, want := strings.Count(compiled.SQL, "?"), len(compiled.Args); got != want {
		t.Fatalf("%q placeholder count = %d, arguments = %d (%#v):\n%s", source, got, want, compiled.Args, compiled.SQL)
	}
}

func chartBreakPipelineCompile(t *testing.T, source string) CompiledQuery {
	t.Helper()
	compiled, err := (Compiler{}).Compile(chartBreakPipelineBuild(t, source))
	if err != nil {
		t.Fatalf("Compile(%q): %v", source, err)
	}
	return compiled
}

func chartBreakPipelineBuild(t *testing.T, source string) *plan.Query {
	t.Helper()
	logical, err := plan.Build(mustParseChartBreakPipeline(t, source), testChartScope())
	if err != nil {
		t.Fatalf("Build(%q): %v", source, err)
	}
	return logical
}

func mustParseChartBreakPipeline(t *testing.T, source string) *spl.Query {
	t.Helper()
	parsed, err := spl.Parse(source)
	if err != nil {
		t.Fatalf("Parse(%q): %v", source, err)
	}
	return parsed
}
