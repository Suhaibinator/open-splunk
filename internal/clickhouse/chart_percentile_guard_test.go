package clickhouse

import (
	"strings"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/plan"
)

func TestCompileChartPercentilePreservesTerminalRelationalDepth(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(
		t,
		`index=gradethis | chart p95(metric) OVER path BY service`,
	)
	if compiled.relationalDepth != 16 {
		t.Fatalf("percentile chart relational depth = %d, want 16", compiled.relationalDepth)
	}
}

func TestCompileChartPercentileCanonicalBoundaryLevels(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		source string
		level  string
	}{
		{source: `index=gradethis | chart p1(metric) OVER path BY service`, level: "0.01"},
		{source: `index=gradethis | chart perc99(metric) OVER path BY service`, level: "0.99"},
	} {
		test := test
		t.Run(test.level, func(t *testing.T) {
			t.Parallel()

			compiled := compileSPL(t, test.source)
			for _, function := range []string{
				"quantilesGKOrNullArrayState(100, " + test.level + ")",
				"quantilesGKOrNullArrayMerge(100, " + test.level + ")",
			} {
				if !strings.Contains(compiled.SQL, function) {
					t.Fatalf("canonical percentile function %s is missing:\n%s", function, compiled.SQL)
				}
			}
		})
	}
}

func TestCompileChartPercentileRevalidatesForgedMeasureMetadata(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name   string
		mutate func(*plan.Chart)
	}{
		{name: "zero percentile", mutate: func(operator *plan.Chart) { operator.Measure.Percentile = 0 }},
		{name: "percentile above range", mutate: func(operator *plan.Chart) { operator.Measure.Percentile = 100 }},
		{name: "percentile on sum", mutate: func(operator *plan.Chart) { operator.Measure.Function = plan.AggregateFunctionSum }},
		{name: "missing input", mutate: func(operator *plan.Chart) { operator.Measure.Input = plan.FieldRef{} }},
		{name: "predicate metadata", mutate: func(operator *plan.Chart) { operator.Measure.Predicate = &plan.ComparisonExpression{} }},
		{name: "wrong canonical output", mutate: func(operator *plan.Chart) { operator.Measure.Output = "p95_metric" }},
		{name: "multiple-token input", mutate: func(operator *plan.Chart) {
			operator.Measure.Input.Name = "metric other"
			operator.Measure.Input.Path = []string{"metric other"}
			operator.Measure.Output = "perc95(metric other)"
		}},
		{name: "quoted input", mutate: func(operator *plan.Chart) {
			operator.Measure.Input.Name = "\"metric\""
			operator.Measure.Input.Path = []string{"\"metric\""}
			operator.Measure.Output = "perc95(\"metric\")"
		}},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			logical := buildPlan(
				t,
				`index=gradethis | chart p95(metric) OVER path BY service`,
			)
			operator := logical.Operators[len(logical.Operators)-1].(*plan.Chart)
			test.mutate(operator)
			if _, err := (Compiler{}).Compile(logical); err == nil {
				t.Fatal("Compile() accepted forged percentile chart metadata")
			}
		})
	}
}
