package clickhouse

import (
	"slices"
	"strings"
	"testing"
)

func TestCompileStatsNumericMomentFamilySharesOneNormalizedInput(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(
		t,
		`index=gradethis | stats `+
			`range(metric) AS span sumsq(metric) AS squared `+
			`stdev(metric) AS sample_sd stdevp(metric) AS population_sd `+
			`var(metric) AS sample_variance varp(metric) AS population_variance`,
	)
	wantFields := []string{
		"span",
		"squared",
		"sample_sd",
		"population_sd",
		"sample_variance",
		"population_variance",
	}
	if !slices.Equal(compiled.OutputFields, wantFields) {
		t.Fatalf("output fields = %v, want %v", compiled.OutputFields, wantFields)
	}

	const input = `"__os_measure_values_0"`
	for _, required := range []string{
		`maxOrNullArray(` + input + `) - minOrNullArray(` + input + `) AS "span"`,
		`sumOrNullArray(arrayMap(value -> value * value, ` + input + `)) AS "squared"`,
		`if(countArray(` + input + `) = 1, CAST(0 AS Nullable(Float64)), stddevSampStableOrNullArray(` + input + `)) AS "sample_sd"`,
		`stddevPopStableOrNullArray(` + input + `) AS "population_sd"`,
		`if(countArray(` + input + `) = 1, CAST(0 AS Nullable(Float64)), varSampStableOrNullArray(` + input + `)) AS "sample_variance"`,
		`varPopStableOrNullArray(` + input + `) AS "population_variance"`,
	} {
		if !strings.Contains(compiled.SQL, required) {
			t.Fatalf("numeric stats SQL missing %q:\n%s", required, compiled.SQL)
		}
	}
	if got := strings.Count(compiled.SQL, ` AS `+input); got != 1 {
		t.Fatalf("normalized numeric input materializations = %d, want 1:\n%s", got, compiled.SQL)
	}
	if got := strings.Count(compiled.SQL, `countArray(`+input+`)`); got != 2 {
		t.Fatalf("sample singleton guards = %d, want 2:\n%s", got, compiled.SQL)
	}
	if strings.Contains(compiled.SQL, "stddevSampOrNullArray(") ||
		strings.Contains(compiled.SQL, "varSampOrNullArray(") ||
		strings.Contains(strings.ToUpper(compiled.SQL), "ARRAY JOIN") {
		t.Fatalf("numeric stats lost stable states or expanded rows:\n%s", compiled.SQL)
	}
	if got, want := strings.Count(compiled.SQL, "?"), len(compiled.Args); got != want {
		t.Fatalf("placeholder count = %d, args = %d", got, want)
	}
}

func TestCompileStatsNumericEvalInputsShareOneScalarFence(t *testing.T) {
	t.Parallel()

	const expression = `if(status>=500, bytes*price, tonumber(fallback))`
	compiled := compileSPL(
		t,
		`index=gradethis | stats `+
			`range(eval(`+expression+`)) AS span `+
			`sumsq(eval(`+expression+`)) AS squared`,
	)
	if !slices.Equal(compiled.OutputFields, []string{"span", "squared"}) {
		t.Fatalf("output fields = %v", compiled.OutputFields)
	}
	for _, required := range []string{
		` AS "__os_measure_numeric_expression_value_0"`,
		` AS "__os_measure_numeric_expression_0"`,
		`maxOrNullArray("__os_measure_numeric_expression_0") - minOrNullArray("__os_measure_numeric_expression_0") AS "span"`,
		`sumOrNullArray(arrayMap(value -> value * value, "__os_measure_numeric_expression_0")) AS "squared"`,
	} {
		if !strings.Contains(compiled.SQL, required) {
			t.Fatalf("numeric eval SQL missing %q:\n%s", required, compiled.SQL)
		}
	}
	if strings.Contains(compiled.SQL, "__os_measure_numeric_expression_value_1") ||
		strings.Contains(compiled.SQL, "__os_measure_numeric_expression_1") {
		t.Fatalf("identical numeric eval input was materialized twice:\n%s", compiled.SQL)
	}
	if got, want := strings.Count(compiled.SQL, "?"), len(compiled.Args); got != want {
		t.Fatalf("placeholder count = %d, args = %d", got, want)
	}
}

func TestCompileStatsNumericMomentsKeepProjectedInputEmpty(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(
		t,
		`index=gradethis | fields service | stats `+
			`range(metric) AS span sumsq(metric) AS squared `+
			`stdev(metric) AS sample_sd var(metric) AS sample_variance BY service`,
	)
	if got := strings.Count(
		compiled.SQL,
		`CAST([], 'Array(Float64)') AS "__os_measure_values_0"`,
	); got != 1 {
		t.Fatalf("projected numeric input empty materializations = %d, want 1:\n%s", got, compiled.SQL)
	}
	if strings.Contains(compiled.SQL, `"__os_fields"."metric"`) ||
		strings.Contains(compiled.SQL, `fields.metric`) {
		t.Fatalf("projected numeric input was resurrected:\n%s", compiled.SQL)
	}
	for _, required := range []string{
		`maxOrNullArray("__os_measure_values_0")`,
		`sumOrNullArray(arrayMap(value -> value * value, "__os_measure_values_0"))`,
		`stddevSampStableOrNullArray("__os_measure_values_0")`,
		`varSampStableOrNullArray("__os_measure_values_0")`,
	} {
		if !strings.Contains(compiled.SQL, required) {
			t.Fatalf("projected numeric stats SQL missing %q:\n%s", required, compiled.SQL)
		}
	}
}
