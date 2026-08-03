package plan

import (
	"errors"
	"slices"
	"strings"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/spl"
)

// chartBreakPipelineScope is the plan-level scope these tests build against.
// Two authorized indexes make the index-narrowing behavior observable.
func chartBreakPipelineScope() Scope {
	return testScope([]string{"gradethis", "internal"}, nil)
}

func chartBreakPipelineBuild(t *testing.T, source string) *Query {
	t.Helper()
	logical, err := Build(mustParse(t, source), chartBreakPipelineScope())
	if err != nil {
		t.Fatalf("Build(%q): %v", source, err)
	}
	return logical
}

func chartBreakPipelineBuildError(t *testing.T, source string) *Diagnostic {
	t.Helper()
	_, err := Build(mustParse(t, source), chartBreakPipelineScope())
	if err == nil {
		t.Fatalf("Build(%q) succeeded", source)
	}
	diagnostic := &Diagnostic{}
	ok := errors.As(err, &diagnostic)
	if !ok {
		t.Fatalf("Build(%q) error = %T, want *Diagnostic", source, err)
	}
	return diagnostic
}

// TestChartBreakPipelineOperatorIsTerminalAndWideAfterEveryUpstream pins the
// logical shape of the pivot behind every supported upstream: the Chart
// operator is always last, the fixed public schema is exactly the row field,
// and the dynamic series contract carries Splunk's documented defaults.
func TestChartBreakPipelineOperatorIsTerminalAndWideAfterEveryUpstream(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name   string
		source string
		row    string
		column string
	}{
		{"bare scan", `index=gradethis | chart count OVER path BY level`, "path", "level"},
		{"base search", `index=gradethis level=ERROR | chart count BY path, level`, "path", "level"},
		{"where", `index=gradethis | where severity>3 | chart count OVER path BY level`, "path", "level"},
		{"eval", `index=gradethis | eval area="x" | chart count OVER path BY area`, "path", "area"},
		{"eval null literal", `index=gradethis | eval n=null | chart count OVER n BY level`, "n", "level"},
		{"rex capture row", `index=gradethis | rex field=path "^/(?<area>[a-z]+)" | chart count OVER area BY level`, "area", "level"},
		{"rex capture column", `index=gradethis | rex field=path "^/(?<area>[a-z]+)" | chart count OVER path BY area`, "path", "area"},
		{"rename onto the row axis", `index=gradethis | rename path AS route | chart count OVER route BY level`, "route", "level"},
		{"rename away from the row axis", `index=gradethis | rename path AS route | chart count OVER path BY level`, "path", "level"},
		{"table", `index=gradethis | table path level | chart count OVER path BY level`, "path", "level"},
		{"fields", `index=gradethis | fields path level | chart count OVER path BY level`, "path", "level"},
		{"fields minus", `index=gradethis | table path level | fields - level | chart count OVER path BY level`, "path", "level"},
		{"sort", `index=gradethis | sort 0 +path | chart count OVER path BY level`, "path", "level"},
		{"dedup", `index=gradethis | dedup 2 path, level | chart count OVER path BY level`, "path", "level"},
		{"head", `index=gradethis | head 10 | chart count OVER path BY level`, "path", "level"},
		{"tail", `index=gradethis | tail 10 | chart count OVER path BY level`, "path", "level"},
		{"stats", `index=gradethis | stats count BY path, level | chart count OVER path BY level`, "path", "level"},
		{"stats renamed aggregate", `index=gradethis | stats count AS hits BY path, level | chart count OVER hits BY level`, "hits", "level"},
		{"top", `index=gradethis | top message | chart count OVER count BY message`, "count", "message"},
		{"rare", `index=gradethis | rare message | chart count OVER percent BY message`, "percent", "message"},
		{"numeric bin", `index=gradethis | bin severity span=10 | chart count OVER severity BY level`, "severity", "level"},
		{"time bin", `index=gradethis | bin _time span=5m AS bt | chart count OVER bt BY level`, "bt", "level"},
		{"in-place time bin", `index=gradethis | bin _time span=5m | chart count OVER _time BY level`, "_time", "level"},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			logical := chartBreakPipelineBuild(t, test.source)
			chart, ok := logical.Operators[len(logical.Operators)-1].(*Chart)
			if !ok {
				t.Fatalf("last operator = %T, want *Chart", logical.Operators[len(logical.Operators)-1])
			}
			for index, operator := range logical.Operators[:len(logical.Operators)-1] {
				if _, isChart := operator.(*Chart); isChart {
					t.Fatalf("operator %d is a second Chart", index)
				}
			}
			if chart.Over.Name != test.row || chart.SplitBy.Name != test.column {
				t.Fatalf("chart axes = %q/%q, want %q/%q", chart.Over.Name, chart.SplitBy.Name, test.row, test.column)
			}
			if chart.Measure.Function != AggregateFunctionCountRows || chart.Measure.Output != "count" ||
				chart.RowLimit != 10_000 || chart.SeriesLimit != 10 ||
				!chart.IncludeNull || !chart.IncludeOther || chart.NullLabel != "NULL" || chart.OtherLabel != "OTHER" {
				t.Fatalf("chart bounds = %#v", chart)
			}
			if logical.OutputFields != nil {
				t.Fatalf("chart declared a fixed output schema: %v", logical.OutputFields)
			}
			if logical.DynamicOutput == nil || !slices.Equal(logical.DynamicOutput.FixedFields, []string{test.row}) ||
				logical.DynamicOutput.MaxSeries != 12 {
				t.Fatalf("dynamic output = %#v", logical.DynamicOutput)
			}
			if chart.LogicalName() != "Chart" || chart.SourceRange() != chart.Range {
				t.Fatalf("chart identity = %q / %#v", chart.LogicalName(), chart.SourceRange())
			}
			// The pivot's own source range must cover the whole chart clause so
			// backend diagnostics can point at it.
			text := test.source[chart.Range.Start.Offset:chart.Range.End.Offset]
			if !strings.HasPrefix(text, "chart ") || !strings.HasSuffix(text, test.column) {
				t.Fatalf("chart source range covers %q", text)
			}
		})
	}
}

// TestChartBreakPipelineRejectsEveryTransformingConsumer pins that a chart plan
// is refused by the completed-job analyses with their own documented codes, for
// every upstream that can precede the pivot. A chart relation is not events, so
// the refusal must be structural rather than dependent on which field is named.
func TestChartBreakPipelineRejectsEveryTransformingConsumer(t *testing.T) {
	t.Parallel()

	for _, source := range []string{
		`index=gradethis | chart count OVER path BY level`,
		`index=gradethis | chart count BY path, level`,
		`index=gradethis | eval n=null | chart count OVER n BY level`,
		`index=gradethis | bin _time span=5m | chart count OVER _time BY level`,
		`index=gradethis | table path level | chart count OVER path BY level`,
		`index=gradethis | stats count BY path, level | chart count OVER path BY level`,
		`index=gradethis | top message | chart count OVER count BY message`,
	} {
		logical := chartBreakPipelineBuild(t, source)

		err := ValidateFieldAnalysisEligibility(logical)
		diagnostic := &Diagnostic{}
		ok := errors.As(err, &diagnostic)
		if !ok || diagnostic.Code != "SPL_UNSUPPORTED_FIELD_ANALYSIS_PIPELINE" {
			t.Fatalf("field analysis for %q = %#v, want SPL_UNSUPPORTED_FIELD_ANALYSIS_PIPELINE", source, err)
		}
		err = ValidateTimelineEligibility(logical)
		diagnostic = &Diagnostic{}
		ok = errors.As(err, &diagnostic)
		if !ok || diagnostic.Code != "SPL_UNSUPPORTED_TIMELINE_PIPELINE" {
			t.Fatalf("timeline for %q = %#v, want SPL_UNSUPPORTED_TIMELINE_PIPELINE", source, err)
		}
	}

	// The refusal survives an operator list whose Chart is not last, so it can
	// never be reached only through the builder's terminal rule.
	logical := chartBreakPipelineBuild(t, `index=gradethis | chart count OVER path BY level`)
	logical.Operators = append(logical.Operators, &Limit{Count: 5})
	if err := ValidateFieldAnalysisEligibility(logical); err == nil {
		t.Fatal("field analysis accepted a non-terminal chart plan")
	}
	if err := ValidateTimelineEligibility(logical); err == nil {
		t.Fatal("timeline accepted a non-terminal chart plan")
	}
}

// TestChartBreakPipelineTerminalRuleIsLocatedAtTheFollowingCommand pins
// SPL_UNSUPPORTED_CHART_PIPELINE and its source range for every command family
// that can follow, including a second chart and a timechart.
func TestChartBreakPipelineTerminalRuleIsLocatedAtTheFollowingCommand(t *testing.T) {
	t.Parallel()

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
		`bucket severity span=10`,
		`timechart span=5m count BY level`,
		`chart count OVER path BY level`,
	} {
		source := `index=gradethis | chart count OVER path BY level | ` + downstream
		diagnostic := chartBreakPipelineBuildError(t, source)
		if diagnostic.Code != "SPL_UNSUPPORTED_CHART_PIPELINE" {
			t.Fatalf("Build(%q) code = %q, want SPL_UNSUPPORTED_CHART_PIPELINE", source, diagnostic.Code)
		}
		located := source[diagnostic.Range.Start.Offset:diagnostic.Range.End.Offset]
		if !strings.HasPrefix(located, strings.SplitN(downstream, " ", 2)[0]) {
			t.Fatalf("Build(%q) located the rejection at %q", source, located)
		}
	}

	// A chart three stages from the end still reports the very next command,
	// never the last one, so the fix a user is told to make is unambiguous.
	source := `index=gradethis | chart count OVER path BY level | head 1 | sort 0 +path | table path`
	diagnostic := chartBreakPipelineBuildError(t, source)
	if located := source[diagnostic.Range.Start.Offset:diagnostic.Range.End.Offset]; located != "head 1" {
		t.Fatalf("Build(%q) located the rejection at %q, want the immediately following command", source, located)
	}
}

// TestChartBreakPipelineTerminalRuleOutranksLaterCommandErrors pins the
// diagnostic precedence: the terminal rule is evaluated when the pivot is
// reached, so a later command's own rejection never masks it.
func TestChartBreakPipelineTerminalRuleOutranksLaterCommandErrors(t *testing.T) {
	t.Parallel()

	for _, source := range []string{
		`index=gradethis | chart count OVER path BY level | stats count BY NULL`,
		`index=gradethis | chart count OVER path BY level | timechart span=5m count BY level`,
		`index=gradethis | chart count OVER path BY level | eval __os_x=1`,
	} {
		if diagnostic := chartBreakPipelineBuildError(t, source); diagnostic.Code != "SPL_UNSUPPORTED_CHART_PIPELINE" {
			t.Fatalf("Build(%q) code = %q, want SPL_UNSUPPORTED_CHART_PIPELINE", source, diagnostic.Code)
		}
	}

	// A timechart before a chart is rejected by timechart's own terminal rule,
	// so neither wide operator can hide behind the other.
	source := `index=gradethis | timechart span=5m count BY level | chart count OVER _time BY level`
	if diagnostic := chartBreakPipelineBuildError(t, source); diagnostic.Code != "SPL_UNSUPPORTED_TIMECHART_PIPELINE" {
		t.Fatalf("Build(%q) code = %q, want SPL_UNSUPPORTED_TIMECHART_PIPELINE", source, diagnostic.Code)
	}
}

// TestChartBreakPipelineFieldResolutionRejections pins the plan-layer field
// rules the pivot inherits: reserved series names, the reserved event payload,
// the compiler-private namespace, the dotted-path ceiling, and duplicates that
// only become visible after resolution.
func TestChartBreakPipelineFieldResolutionRejections(t *testing.T) {
	t.Parallel()

	seventeen := strings.Repeat("seg.", 16) + "leaf"
	eighteen := strings.Repeat("seg.", 17) + "leaf"

	for _, test := range []struct {
		name   string
		source string
		code   string
	}{
		{"reserved NULL row", `index=gradethis | chart count OVER NULL BY level`, "SPL_UNSUPPORTED_CHART_FIELD_TYPE"},
		{"reserved OTHER row", `index=gradethis | chart count OVER OTHER BY level`, "SPL_UNSUPPORTED_CHART_FIELD_TYPE"},
		{"reserved NULL column", `index=gradethis | chart count OVER path BY NULL`, "SPL_UNSUPPORTED_CHART_FIELD_TYPE"},
		{"reserved OTHER column", `index=gradethis | chart count OVER path BY OTHER`, "SPL_UNSUPPORTED_CHART_FIELD_TYPE"},
		{"open-schema fields row", `index=gradethis | chart count OVER fields BY level`, "SPL_UNSUPPORTED_CHART_FIELD_TYPE"},
		{"open-schema fields column", `index=gradethis | chart count OVER path BY fields`, "SPL_UNSUPPORTED_CHART_FIELD_TYPE"},
		{"private namespace row", `index=gradethis | chart count OVER __os_row BY level`, "SPL_RESERVED_FIELD"},
		{"private namespace column", `index=gradethis | chart count OVER path BY __os_col`, "SPL_RESERVED_FIELD"},
		{"row path past the segment ceiling", `index=gradethis | chart count OVER ` + eighteen + ` BY level`, "SPL_QUERY_TOO_COMPLEX"},
		{"column path past the segment ceiling", `index=gradethis | chart count OVER path BY ` + eighteen, "SPL_QUERY_TOO_COMPLEX"},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if diagnostic := chartBreakPipelineBuildError(t, test.source); diagnostic.Code != test.code {
				t.Fatalf("Build(%q) code = %q, want %q", test.source, diagnostic.Code, test.code)
			}
		})
	}

	// Exactly at the ceiling both axes resolve to full 17-segment paths.
	logical := chartBreakPipelineBuild(t, `index=gradethis | chart count OVER `+seventeen+` BY `+seventeen+`2`)
	chart := logical.Operators[len(logical.Operators)-1].(*Chart)
	if len(chart.Over.Path) != 17 || len(chart.SplitBy.Path) != 17 {
		t.Fatalf("resolved path lengths = %d/%d, want 17/17", len(chart.Over.Path), len(chart.SplitBy.Path))
	}
}

// TestChartBreakPipelineIndexReferenceCollectionStopsAtChart pins that the
// pivot terminates index-scope narrowing exactly as the other transforming
// commands do: a search behind a chart never contributes an index reference, so
// the user always sees the terminal-pipeline rule rather than a scope error
// whose presence would depend on the smuggled index's authorization.
func TestChartBreakPipelineIndexReferenceCollectionStopsAtChart(t *testing.T) {
	t.Parallel()

	for _, source := range []string{
		`index=gradethis | chart count OVER path BY level | search index=forbidden`,
		`index=gradethis | chart count OVER path BY level | search index=internal`,
		`index=gradethis | chart count BY path, level | search index=forbidden`,
	} {
		if diagnostic := chartBreakPipelineBuildError(t, source); diagnostic.Code != "SPL_UNSUPPORTED_CHART_PIPELINE" {
			t.Fatalf("Build(%q) code = %q, want SPL_UNSUPPORTED_CHART_PIPELINE", source, diagnostic.Code)
		}
	}

	// The same smuggled search in front of the pivot is still a scope error, so
	// the stopping rule is what changed the outcome, not the scope itself.
	if diagnostic := chartBreakPipelineBuildError(t,
		`index=gradethis | search index=forbidden | chart count OVER path BY level`); diagnostic.Code != "SPL_INDEX_FORBIDDEN" {
		t.Fatalf("Build(search before chart) code = %q, want SPL_INDEX_FORBIDDEN", diagnostic.Code)
	}

	// A chart never widens or narrows the resolved scan scope on its own.
	both := chartBreakPipelineBuild(t, `index=gradethis OR index=internal | chart count OVER path BY level`)
	if !slices.Equal(both.EffectiveIndexes, []string{"gradethis", "internal"}) {
		t.Fatalf("effective indexes = %v", both.EffectiveIndexes)
	}
	requested, err := Build(
		mustParse(t, `index=gradethis | chart count OVER path BY level`),
		testScope([]string{"gradethis", "internal"}, []string{"gradethis"}),
	)
	if err != nil {
		t.Fatalf("Build(requested scope): %v", err)
	}
	if !slices.Equal(requested.EffectiveIndexes, []string{"gradethis"}) ||
		!slices.Equal(requested.Operators[0].(*Scan).Indexes, []string{"gradethis"}) {
		t.Fatalf("requested scope was not honored: %v", requested.EffectiveIndexes)
	}
}

// TestChartBreakPipelineDropsCanonicalTimeEligibility pins that the pivot
// invalidates the canonical time provenance the timeline and timechart depend
// on, even when the row axis is _time itself.
func TestChartBreakPipelineDropsCanonicalTimeEligibility(t *testing.T) {
	t.Parallel()

	for _, source := range []string{
		`index=gradethis | chart count OVER _time BY level`,
		`index=gradethis | bin _time span=5m | chart count OVER _time BY level`,
	} {
		logical := chartBreakPipelineBuild(t, source)
		if err := ValidateTimelineEligibility(logical); err == nil {
			t.Fatalf("timeline accepted %q even though chart transformed the relation", source)
		}
		if logical.OutputFields != nil {
			t.Fatalf("%q kept a fixed output schema: %v", source, logical.OutputFields)
		}
	}
}

// TestChartBreakPipelineDuplicateAxesAreRejectedAtTheColumnField pins that a
// duplicated axis is refused before planning, with the diagnostic located on
// the second (column) field in both spellings. Reaching the planner with two
// identical axes would publish a public column whose name is the row column's.
func TestChartBreakPipelineDuplicateAxesAreRejectedAtTheColumnField(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		source string
		field  string
	}{
		{`index=gradethis | chart count OVER path BY path`, "path"},
		{`index=gradethis | chart count BY path, path`, "path"},
		{`index=gradethis | chart count BY path path`, "path"},
		{`index=gradethis | chart count OVER _time BY _time`, "_time"},
		{`index=gradethis | chart count OVER a.b BY a.b`, "a.b"},
		{`index=gradethis | chart count OVER NULL BY NULL`, "NULL"},
		{`index=gradethis | chart count OVER fields BY fields`, "fields"},
	} {
		_, err := spl.Parse(test.source)
		diagnostic := &spl.Diagnostic{}
		ok := errors.As(err, &diagnostic)
		if !ok || diagnostic.Code != "SPL_UNSUPPORTED_CHART_SYNTAX" {
			t.Fatalf("Parse(%q) diagnostic = %#v, want SPL_UNSUPPORTED_CHART_SYNTAX", test.source, err)
		}
		located := test.source[diagnostic.Range.Start.Offset:diagnostic.Range.End.Offset]
		if located != test.field {
			t.Fatalf("Parse(%q) located the rejection at %q, want the repeated column field", test.source, located)
		}
	}
}
