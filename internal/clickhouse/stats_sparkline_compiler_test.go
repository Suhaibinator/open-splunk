package clickhouse

import (
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/plan"
	"github.com/Suhaibinator/open-splunk/internal/spl"
)

func TestCompileStatsSparklineParsePlanCompilePipeline(t *testing.T) {
	t.Parallel()

	source := `index=gradethis | stats count AS events ` +
		`sparkline(count,30m) AS volume ` +
		`sparkline(sum(latency),30m) AS latency ` +
		`BY host | where mvcount(volume)>0 | table host events volume latency`
	parsed, err := spl.Parse(source)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	logical, err := plan.Build(parsed, testChartScope())
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	var aggregate *plan.Aggregate
	for _, operator := range logical.Operators {
		if candidate, candidateOK := operator.(*plan.Aggregate); candidateOK {
			aggregate = candidate
			break
		}
	}
	ok := aggregate != nil
	if !ok || len(aggregate.Measures) != 3 || aggregate.Measures[1].Sparkline == nil ||
		aggregate.Measures[2].Sparkline == nil {
		t.Fatalf("aggregate plan = %#v", logical.Operators)
	}
	compiled, err := (Compiler{}).Compile(logical)
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	if !slices.Equal(compiled.OutputFields, []string{"host", "events", "volume", "latency"}) {
		t.Fatalf("output fields = %#v", compiled.OutputFields)
	}
	for _, required := range []string{
		`AS "__os_sparkline_bucket_0"`,
		`OVER (PARTITION BY "host", "__os_sparkline_bucket_0")`,
		`groupUniqArray(101)(tuple(toInt64("__os_sparkline_bucket_0")`,
		statsSparklineMarker,
		StatsSparklineLimitMarker,
		StatsSparklineBytesLimitMarker,
		`length("volume")`,
	} {
		if !strings.Contains(compiled.SQL, required) {
			t.Fatalf("compiled sparkline SQL missing %q:\n%s", required, compiled.SQL)
		}
	}
	if scans := strings.Count(compiled.SQL, `FROM "open_splunk"."events"`); scans != 1 {
		t.Fatalf("mixed stats/sparkline uses %d scans, want one:\n%s", scans, compiled.SQL)
	}
	if got, want := strings.Count(compiled.SQL, "?"), len(compiled.Args); got != want {
		t.Fatalf("placeholder count = %d, args = %d\nSQL: %s\nargs: %#v", got, want, compiled.SQL, compiled.Args)
	}
}

func TestCompileStatsSparklineAllDocumentedFunctionsAndSpans(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		call     string
		required string
	}{
		{"count rows automatic", `count`, `toUInt64(count() OVER`},
		{"count field", `count(value)`, `sum(toUInt128(`},
		{"distinct count", `dc(value)`, `groupUniqArrayArray(100001)`},
		{"average", `avg(value)`, `avgOrNullArray(`},
		{"sample stdev", `stdev(value)`, `stddevSampStableOrNullArray(`},
		{"population stdev", `stdevp(value)`, `stddevPopStableOrNullArray(`},
		{"sample variance", `var(value)`, `varSampStableOrNullArray(`},
		{"population variance", `varp(value)`, `varPopStableOrNullArray(`},
		{"sum", `sum(value)`, `sumOrNullArray(`},
		{"sum squares", `sumsq(value)`, `value -> value * value`},
		{"minimum", `min(value)`, `minOrNullArray(`},
		{"maximum", `max(value)`, `maxOrNullArray(`},
		{"range", `range(value)`, `maxOrNullArray(`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			span := ",30m"
			if strings.Contains(test.name, "automatic") {
				span = ""
			}
			compiled := compileSPL(
				t,
				`index=gradethis | stats sparkline(`+test.call+span+`) AS trend`,
			)
			if !slices.Equal(compiled.OutputFields, []string{"trend"}) ||
				!strings.Contains(compiled.SQL, test.required) ||
				!strings.Contains(compiled.SQL, statsSparklineMarker) {
				t.Fatalf("compiled %s sparkline = %#v\n%s", test.name, compiled.OutputFields, compiled.SQL)
			}
		})
	}
}

func TestCompileStatsSparklineMultivalueBYAndDownstreamVisibility(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(
		t,
		`index=gradethis | stats sparkline(sum(value),30m) AS trend BY tags zones `+
			`dedup_splitvals=true | eval points=mvcount(trend) | where points>1 | table tags zones points trend`,
	)
	// SQL is rendered outermost-first, so the window text precedes the two
	// nested ARRAY JOIN clauses even though relational execution expands both
	// dimensions before evaluating the window stage.
	window := strings.Index(compiled.SQL, `OVER (PARTITION BY "__os_group_value_0", "__os_group_value_1", "__os_sparkline_bucket_0")`)
	secondExpansion := strings.Index(compiled.SQL, `ARRAY JOIN "__os_group_values_1"`)
	firstExpansion := strings.Index(compiled.SQL, `ARRAY JOIN "__os_group_values_0"`)
	if window < 0 || firstExpansion <= window || secondExpansion <= firstExpansion {
		t.Fatalf("MV expansion/window order is invalid:\n%s", compiled.SQL)
	}
	if !slices.Equal(
		compiled.OutputFields,
		[]string{"tags", "zones", "points", "trend"},
	) {
		t.Fatalf("downstream output fields = %#v", compiled.OutputFields)
	}
}

func TestCompileStatsSparklineClosedSchemaWildcardExpansion(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(
		t,
		`index=gradethis | table _time,delay,xdelay,latency | `+
			`stats sparkline(avg(*lay),30m) AS trend_*`,
	)
	if !slices.Equal(compiled.OutputFields, []string{"trend_de", "trend_xde"}) {
		t.Fatalf("output fields = %#v", compiled.OutputFields)
	}
	if got := strings.Count(compiled.SQL, `avgOrNullArray(`); got != 2 {
		t.Fatalf("average sparkline windows = %d, want 2:\n%s", got, compiled.SQL)
	}
	if strings.Contains(compiled.SQL, "*lay") || strings.Contains(compiled.SQL, "trend_*") {
		t.Fatalf("wildcard metadata crossed into compiled SQL:\n%s", compiled.SQL)
	}
}

func TestCompileStatsSparklineRejectsUnsupportedAndOversizedRanges(t *testing.T) {
	t.Parallel()

	for _, source := range []string{
		`index=gradethis | stats sparkline(median(value)) AS trend`,
		`index=gradethis | stats sparkline(avg(eval(value+1))) AS trend`,
	} {
		if _, err := spl.Parse(source); err == nil {
			t.Fatalf("Parse(%q) accepted unsupported sparkline", source)
		}
	}
	wildcard, err := spl.Parse(`index=gradethis | stats sparkline(avg(*)) AS trend_*`)
	if err != nil {
		t.Fatalf("Parse wildcard sparkline: %v", err)
	}
	_, err = plan.Build(wildcard, testChartScope())
	var wildcardDiagnostic *plan.Diagnostic
	if !errors.As(err, &wildcardDiagnostic) ||
		wildcardDiagnostic.Code != "SPL_UNSUPPORTED_STATS_WILDCARD" {
		t.Fatalf("open-schema wildcard error = %#v", err)
	}

	parsed, err := spl.Parse(`index=gradethis | stats sparkline(count,1s) AS trend`)
	if err != nil {
		t.Fatalf("Parse oversized span: %v", err)
	}
	scope := testChartScope()
	scope.Earliest = time.Date(2026, time.July, 21, 0, 0, 0, 0, time.UTC)
	scope.Latest = scope.Earliest.Add(101 * time.Second)
	logical, err := plan.Build(parsed, scope)
	if err != nil {
		t.Fatalf("Build oversized span: %v", err)
	}
	_, err = (Compiler{}).Compile(logical)
	if err == nil || !strings.Contains(err.Error(), "maximum is 100") {
		t.Fatalf("oversized explicit sparkline error = %v", err)
	}
}

func TestCompileStatsSparklineRejectsForgedBackendMetadata(t *testing.T) {
	t.Parallel()

	logical := buildPlan(t, `index=gradethis | stats sparkline(count) AS trend`)
	aggregate := logical.Operators[len(logical.Operators)-1].(*plan.Aggregate)
	aggregate.Measures[0].Sparkline.Function = plan.AggregateFunctionMedian
	_, err := (Compiler{}).Compile(logical)
	if err == nil {
		t.Fatal("compiler accepted forged unsupported sparkline function")
	}
	var diagnostic *plan.Diagnostic
	if errors.As(err, &diagnostic) {
		t.Fatalf("forged backend metadata unexpectedly reached a user diagnostic: %#v", diagnostic)
	}
}
