package clickhouse

import (
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/plan"
	"github.com/Suhaibinator/open-splunk/internal/spl"
)

func TestCompileStatsPercentileFamilySharesOneBoundedStatePerInput(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(
		t,
		`index=gradethis | stats p50(duration) AS median p90(duration) AS p90 `+
			`p95(duration) AS p95 p99(duration) AS p99 `+
			`perc50(duration) AS median_again sum(duration) AS total avg(duration) AS mean`,
	)
	if !slices.Equal(
		compiled.OutputFields,
		[]string{"median", "p90", "p95", "p99", "median_again", "total", "mean"},
	) {
		t.Fatalf("output fields = %v", compiled.OutputFields)
	}
	const state = `"__os_stats_percentiles_0"`
	for _, required := range []string{
		`quantilesGKOrNullArray(100, 0.5, 0.9, 0.95, 0.99)("__os_measure_values_0") AS ` + state,
		`arrayElementOrNull(` + state + `, 1) AS "median"`,
		`arrayElementOrNull(` + state + `, 2) AS "p90"`,
		`arrayElementOrNull(` + state + `, 3) AS "p95"`,
		`arrayElementOrNull(` + state + `, 4) AS "p99"`,
		`arrayElementOrNull(` + state + `, 1) AS "median_again"`,
		`sum(arraySum("__os_measure_values_0"))`,
	} {
		if !strings.Contains(compiled.SQL, required) {
			t.Fatalf("percentile SQL missing %q:\n%s", required, compiled.SQL)
		}
	}
	if strings.Count(compiled.SQL, "quantilesGKOrNullArray(") != 1 ||
		strings.Count(compiled.SQL, ` AS "__os_measure_values_0"`) != 1 {
		t.Fatalf("percentile input/state was duplicated:\n%s", compiled.SQL)
	}
	if strings.Contains(compiled.SQL, "quantileGKOrNull(") ||
		strings.Contains(strings.ToUpper(compiled.SQL), "ARRAY JOIN") {
		t.Fatalf("percentile lowering retained a scalar state or expanded rows:\n%s", compiled.SQL)
	}
}

func TestCompileStatsPercentileFamilyKeepsScalarOnlyInputScalar(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(
		t,
		`index=gradethis | eval metric=42 | stats `+
			`p50(metric) AS q50 p90(metric) AS q90 p99(metric) AS q99`,
	)
	for _, required := range []string{
		`ifNotFinite(toFloat64("metric"), CAST(NULL AS Nullable(Float64))) AS "__os_measure_percentile_value_0"`,
		`quantilesGKOrNull(100, 0.5, 0.9, 0.99)("__os_measure_percentile_value_0") AS "__os_stats_percentiles_0"`,
		`arrayElementOrNull("__os_stats_percentiles_0", 3) AS "q99"`,
	} {
		if !strings.Contains(compiled.SQL, required) {
			t.Fatalf("scalar percentile SQL missing %q:\n%s", required, compiled.SQL)
		}
	}
	if strings.Contains(compiled.SQL, "__os_measure_values_") ||
		strings.Contains(compiled.SQL, "arrayFilter(") ||
		strings.Contains(compiled.SQL, "quantilesGKOrNullArray(") {
		t.Fatalf("scalar percentile family paid the multivalue array cost:\n%s", compiled.SQL)
	}
}

func TestCompileStatsPercentileFamilyFlattensFixedMultivalue(t *testing.T) {
	t.Parallel()

	fixed := compileSPL(
		t,
		`index=gradethis | stats values(user) AS users | stats p50(users) AS median`,
	)
	if !strings.Contains(fixed.SQL, "quantilesGKOrNullArray(100, 0.5)") ||
		!strings.Contains(fixed.SQL, `arrayMap(element ->`) ||
		!strings.Contains(fixed.SQL, `arrayElementOrNull("__os_stats_percentiles_0", 1) AS "median"`) {
		t.Fatalf("fixed multivalue percentile is not flattened safely:\n%s", fixed.SQL)
	}
}

func TestCompileStatsPercentileMaximumMeasuresKeepOneAggregateState(t *testing.T) {
	t.Parallel()

	var source strings.Builder
	source.WriteString("index=gradethis | stats ")
	wantOutputs := make([]string, 0, spl.MaximumStatsMeasures)
	for percentile := 1; percentile <= spl.MaximumStatsMeasures; percentile++ {
		if percentile > 1 {
			source.WriteByte(' ')
		}
		output := fmt.Sprintf("q%d", percentile)
		fmt.Fprintf(&source, "perc%d(metric) AS %s", percentile, output)
		wantOutputs = append(wantOutputs, output)
	}
	compiled := compileSPL(t, source.String())
	if !slices.Equal(compiled.OutputFields, wantOutputs) {
		t.Fatalf("output fields = %v, want %v", compiled.OutputFields, wantOutputs)
	}
	if got := strings.Count(compiled.SQL, "quantilesGKOrNullArray("); got != 1 {
		t.Fatalf("maximum percentile aliases compiled %d aggregate states, want one:\n%s", got, compiled.SQL)
	}
	for percentile := 1; percentile <= spl.MaximumStatsMeasures; percentile++ {
		required := fmt.Sprintf("arrayElementOrNull(\"__os_stats_percentiles_0\", %d) AS \"q%d\"", percentile, percentile)
		if !strings.Contains(compiled.SQL, required) {
			t.Fatalf("maximum percentile SQL missing %q:\n%s", required, compiled.SQL)
		}
	}
	if len(compiled.SQL) > maxCompiledQueryBytes {
		t.Fatalf("compiled query bytes = %d, limit %d", len(compiled.SQL), maxCompiledQueryBytes)
	}
}

func TestCompileStatsPercentileFamilyUsesSeparateStatesForSeparateInputs(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(
		t,
		`index=gradethis | stats p50(left) AS left50 p90(right) AS right90 `+
			`p95(left) AS left95 p99(right) AS right99`,
	)
	for _, required := range []string{
		`quantilesGKOrNullArray(100, 0.5, 0.95)("__os_measure_values_0") AS "__os_stats_percentiles_0"`,
		`quantilesGKOrNullArray(100, 0.9, 0.99)("__os_measure_values_1") AS "__os_stats_percentiles_1"`,
		`arrayElementOrNull("__os_stats_percentiles_0", 2) AS "left95"`,
		`arrayElementOrNull("__os_stats_percentiles_1", 2) AS "right99"`,
	} {
		if !strings.Contains(compiled.SQL, required) {
			t.Fatalf("percentile SQL missing %q:\n%s", required, compiled.SQL)
		}
	}
	if strings.Count(compiled.SQL, "quantilesGKOrNullArray(") != 2 {
		t.Fatalf("separate percentile inputs did not compile to exactly two states:\n%s", compiled.SQL)
	}
}

func TestCompileStatsPercentileAndNumericArrayConsumersShareInputInEitherOrder(t *testing.T) {
	t.Parallel()

	for _, source := range []string{
		`index=gradethis | stats p50(request.amount) AS median sum(request.amount) AS total`,
		`index=gradethis | stats sum(request.amount) AS total p50(request.amount) AS median`,
	} {
		compiled := compileSPL(t, source)
		if strings.Count(compiled.SQL, ` AS "__os_measure_values_0"`) != 1 ||
			strings.Count(compiled.SQL, "quantilesGKOrNullArray(") != 1 ||
			strings.Contains(compiled.SQL, "__os_measure_percentile_value_") {
			t.Fatalf("percentile/sum did not share one array normalization for %q:\n%s", source, compiled.SQL)
		}
		if got, want := strings.Count(compiled.SQL, "?"), len(compiled.Args); got != want {
			t.Fatalf("placeholder count = %d, args = %d for %q", got, want, source)
		}
	}
}

func TestCompileRejectsForgedStatsPercentileLevels(t *testing.T) {
	t.Parallel()

	base := buildPlan(t, `index=gradethis`)
	input, err := plan.ResolveField("metric", spl.Range{})
	if err != nil {
		t.Fatal(err)
	}
	for _, percentile := range []uint8{0, 100, 255} {
		percentile := percentile
		t.Run(fmt.Sprintf("%d", percentile), func(t *testing.T) {
			t.Parallel()

			candidate := *base
			candidate.Operators = append(
				append([]plan.Operator(nil), base.Operators...),
				&plan.Aggregate{Measures: []plan.AggregateMeasure{{
					Function:   plan.AggregateFunctionPercentile,
					Input:      input,
					Percentile: percentile,
					Output:     "result",
				}}},
			)
			if _, err := (Compiler{}).Compile(&candidate); err == nil ||
				!strings.Contains(err.Error(), "unsupported percentile") {
				t.Fatalf("Compile percentile %d error = %v", percentile, err)
			}
		})
	}
}

func TestStatsPercentileLevelSQLUsesTrustedCanonicalLiterals(t *testing.T) {
	t.Parallel()

	for percentile, want := range map[uint8]string{
		1: "0.01", 9: "0.09", 10: "0.1", 42: "0.42", 50: "0.5", 90: "0.9", 99: "0.99",
	} {
		if got := statsPercentileLevelSQL(percentile); got != want {
			t.Errorf("statsPercentileLevelSQL(%d) = %q, want %q", percentile, got, want)
		}
	}
}
