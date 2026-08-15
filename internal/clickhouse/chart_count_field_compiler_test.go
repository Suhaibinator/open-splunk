package clickhouse

import (
	"slices"
	"strings"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/plan"
)

const (
	chartFieldCountMeasureAlias    = `"__os_ch_measure_count"`
	chartFieldCountRowAlias        = `"__os_ch_row_count"`
	chartFieldOccurrenceCountAlias = `"__os_ch_occurrence_count"`
	chartFieldOccurrenceScoreAlias = `"__os_ch_occurrence_score"`
)

func TestCompileChartCountFieldUsesOneGroupedOccurrencePath(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(
		t,
		`index=gradethis message="Request metrics" | chart count(counted_value) OVER path BY level`,
	)

	if compiled.Chart == nil || compiled.Chart.ValueKind != ChartValueKindCount ||
		!compiled.Chart.ValueKind.Valid() {
		t.Fatalf("compiled chart contract = %#v", compiled.Chart)
	}
	if compiled.sourceFanout != eventStatsOrdinarySourceFanout {
		t.Fatalf("chart count(field) source fanout = %d, want one", compiled.sourceFanout)
	}
	for _, required := range []string{
		`AS ` + chartFieldCountMeasureAlias,
		`"__os_chart_group_counts" AS MATERIALIZED (`,
		`count() AS ` + chartFieldCountRowAlias,
		`sum(toUInt128(` + chartFieldCountMeasureAlias + `)) AS ` + chartFieldOccurrenceCountAlias,
		`sumIf(` + chartFieldOccurrenceCountAlias + `, "__os_ch_row_eligible" != 0) AS ` + chartFieldOccurrenceScoreAlias,
		`ORDER BY ` + chartFieldOccurrenceScoreAlias + ` DESC, "__os_ch_label" ASC LIMIT 10`,
		`toUInt64(sum(toUInt128(` + chartFieldOccurrenceCountAlias + `))) AS "__os_ch_count"`,
		`"__os_chart_validation" AS (SELECT toUInt8(maxOrDefault("__os_ch_row_invalid") > 0) AS "__os_ch_invalid" FROM "__os_chart_group_counts" WHERE "__os_ch_row_eligible" != 0)`,
		`mapFromArrays(`,
		`AS "` + ChartCountsColumn + `"`,
	} {
		if !strings.Contains(compiled.SQL, required) {
			t.Fatalf("chart count(field) SQL missing %q:\n%s", required, compiled.SQL)
		}
	}
	if strings.Contains(compiled.SQL, `WHERE `+chartFieldCountMeasureAlias+` > 0`) ||
		strings.Contains(compiled.SQL, `WHERE `+chartFieldCountMeasureAlias+` != 0`) {
		t.Fatalf("zero-occurrence rows were removed from the chart domain:\n%s", compiled.SQL)
	}
	for _, bareOnly := range []string{
		`"__os_chart_scored"`,
		`"__os_chart_ranked"`,
		`"__os_chart_checks"`,
		`"__os_ch_group_row"`,
	} {
		if strings.Contains(compiled.SQL, bareOnly) {
			t.Fatalf("chart count(field) entered bare-count graph %q:\n%s", bareOnly, compiled.SQL)
		}
	}
	assertChartCountFieldShape(t, compiled)
}

func TestCompileChartBareCountUsesOneConsumerEnvelope(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(t, `index=gradethis | chart count OVER path BY level`)
	if compiled.sourceFanout != eventStatsOrdinarySourceFanout {
		t.Fatalf("bare chart count source fanout = %d, want one", compiled.sourceFanout)
	}
}

func TestCompileChartCountFieldAfterEventStatsPreservesOneConsumerEnvelope(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(
		t,
		`index=gradethis | eventstats count AS peers | chart count(peers) OVER path BY level`,
	)
	if compiled.Chart == nil || compiled.Timechart != nil ||
		compiled.Chart.ValueKind != ChartValueKindCount ||
		compiled.sourceFanout != eventStatsOrdinarySourceFanout {
		t.Fatalf("chronological chart count(field) contract = %#v", compiled)
	}
	wantProjection := `SELECT "__os_chart_ordinal", "__os_chart_row", "__os_chart_names", ` +
		`"__os_chart_counts", "__os_chart_invalid" FROM "__os_chronological_final_input_`
	if !strings.Contains(compiled.SQL, wantProjection) {
		t.Fatalf("chart count(field) chronological envelope is missing %q:\n%s", wantProjection, compiled.SQL)
	}
	assertChartCountFieldShape(t, compiled)
}

func TestCompileChartCountFieldTreatsProjectedInputAsZero(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(
		t,
		`index=gradethis | fields path level | chart count(projected_value) OVER path BY level`,
	)
	if !strings.Contains(compiled.SQL, `toUInt64(0) AS `+chartFieldCountMeasureAlias) {
		t.Fatalf("projected chart count(field) input was not normalized to zero:\n%s", compiled.SQL)
	}
	if slices.Contains(compiled.Args, any("projected_value")) ||
		slices.Contains(compiled.Args, any("projected_value.")) {
		t.Fatalf("projected chart count(field) input was rebound from storage: %#v", compiled.Args)
	}
	for _, required := range []string{
		`count() AS ` + chartFieldCountRowAlias,
		`WHERE "__os_ch_kind" = 0 AND ` + chartFieldCountRowAlias + ` > 0`,
		`WHERE "__os_ch_kind" = 1 AND ` + chartFieldCountRowAlias + ` > 0`,
	} {
		if !strings.Contains(compiled.SQL, required) {
			t.Fatalf("projected chart count(field) lost row-derived domain %q:\n%s", required, compiled.SQL)
		}
	}
	assertChartCountFieldShape(t, compiled)
}

func TestCompileChartCountFieldAllowsColumnAsMeasure(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(
		t,
		`index=gradethis | chart count(level) OVER path BY level`,
	)
	if compiled.Chart == nil || compiled.Chart.ValueKind != ChartValueKindCount {
		t.Fatalf("column-measured chart contract = %#v", compiled.Chart)
	}
	if !strings.Contains(compiled.SQL, `AS `+chartFieldCountMeasureAlias) {
		t.Fatalf("column-measured chart omitted occurrence normalization:\n%s", compiled.SQL)
	}
	assertChartCountFieldShape(t, compiled)
}

func TestCompileChartCountFieldRevalidatesForgedPlan(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name   string
		mutate func(*plan.Query, *plan.Chart)
	}{
		{
			name: "missing input",
			mutate: func(_ *plan.Query, operator *plan.Chart) {
				operator.Measure.Input = plan.FieldRef{}
			},
		},
		{
			name: "noncanonical input",
			mutate: func(_ *plan.Query, operator *plan.Chart) {
				operator.Measure.Input.Canonical = true
			},
		},
		{
			name: "forged input path",
			mutate: func(_ *plan.Query, operator *plan.Chart) {
				operator.Measure.Input.Path = []string{"attacker"}
			},
		},
		{
			name: "predicate metadata",
			mutate: func(_ *plan.Query, operator *plan.Chart) {
				operator.Measure.Predicate = &plan.BooleanExpression{}
			},
		},
		{
			name: "percentile metadata",
			mutate: func(_ *plan.Query, operator *plan.Chart) {
				operator.Measure.Percentile = 50
			},
		},
		{
			name: "wrong output",
			mutate: func(_ *plan.Query, operator *plan.Chart) {
				operator.Measure.Output = "attacker"
			},
		},
		{
			name: "input equals row axis",
			mutate: func(_ *plan.Query, operator *plan.Chart) {
				operator.Measure.Input = operator.Over
				operator.Measure.Output = "count(" + operator.Over.Name + ")"
			},
		},
		{
			name: "input contains whitespace",
			mutate: func(_ *plan.Query, operator *plan.Chart) {
				operator.Measure.Input.Name = "bad field"
				operator.Measure.Output = "count(bad field)"
			},
		},
		{
			name: "input contains quote syntax",
			mutate: func(_ *plan.Query, operator *plan.Chart) {
				operator.Measure.Input.Name = `"quoted"`
				operator.Measure.Output = `count("quoted")`
			},
		},
		{
			name: "input contains wildcard syntax",
			mutate: func(_ *plan.Query, operator *plan.Chart) {
				operator.Measure.Input.Name = "prefix*"
				operator.Measure.Output = "count(prefix*)"
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			logical, operator := parsedChartCountFieldPlan(t)
			test.mutate(logical, operator)
			if _, err := (Compiler{}).Compile(logical); err == nil {
				t.Fatal("Compile accepted forged chart count(field) metadata")
			}
		})
	}
}

func assertChartCountFieldShape(t *testing.T, compiled CompiledQuery) {
	t.Helper()
	assertCountFieldPhysicalShape(t, compiled, chartFieldCountMeasureAlias, "chart")
}

func parsedChartCountFieldPlan(t *testing.T) (*plan.Query, *plan.Chart) {
	t.Helper()

	logical := buildPlan(t, `index=gradethis | chart count(counted_value) OVER path BY level`)
	operator, ok := logical.Operators[len(logical.Operators)-1].(*plan.Chart)
	if !ok {
		t.Fatalf("last logical operator = %T, want *plan.Chart", logical.Operators[len(logical.Operators)-1])
	}
	if operator.Measure.Function != plan.AggregateFunctionCountValues {
		t.Fatalf("parsed chart measure = %d, want count(field)", operator.Measure.Function)
	}
	return logical, operator
}
