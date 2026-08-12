package clickhouse

import (
	"slices"
	"strings"
	"testing"
)

func TestCompileStatsDistributionFamiliesShareNormalizedInputs(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(
		t,
		`index=gradethis | stats `+
			`exactperc50(metric) AS exact_median `+
			`upperperc95(metric) AS upper_p95 median(metric) AS approximate_median `+
			`estdc(label) AS estimated_labels estdc_error(label) AS label_error `+
			`mode(label) AS common_label`,
	)
	wantFields := []string{
		"exact_median",
		"upper_p95",
		"approximate_median",
		"estimated_labels",
		"label_error",
		"common_label",
	}
	if !slices.Equal(compiled.OutputFields, wantFields) {
		t.Fatalf("output fields = %v, want %v", compiled.OutputFields, wantFields)
	}

	const (
		numericInput = `"__os_measure_values_0"`
		stringInput  = `"__os_measure_strings_0"`
	)
	for _, required := range []string{
		`arrayElementOrNull(arraySort(groupArrayArray(` + numericInput + `)), toUInt64(ceil(toFloat64(countArray(` + numericInput + `)) * 0.5))) AS "exact_median"`,
		`arrayElementOrNull(quantilesGKOrNullArray(100, 0.95, 0.96)(` + numericInput + `), if(uniqCombined64Array(17)(` + numericInput + `) <= 1000, 1, 2)) AS "upper_p95"`,
		`arrayElementOrNull(quantilesGKOrNullArray(100, 0.5)(` + numericInput + `), 1) AS "approximate_median"`,
		`uniqCombined64Array(17)(` + stringInput + `) AS "estimated_labels"`,
		`if(uniqCombined64Array(17)(` + stringInput + `) < 1000, toFloat64(0), toFloat64(0.002872621298570349)) AS "label_error"`,
		`arrayElementOrNull(tupleElement(sumMap(` + stringInput + `, arrayMap(_ -> toUInt64(1), ` + stringInput + `)), 1)`,
		`AS "common_label"`,
	} {
		if !strings.Contains(compiled.SQL, required) {
			t.Fatalf("distribution SQL missing %q:\n%s", required, compiled.SQL)
		}
	}
	if got := strings.Count(compiled.SQL, ` AS `+numericInput); got != 1 {
		t.Fatalf("numeric distribution materializations = %d, want 1:\n%s", got, compiled.SQL)
	}
	if got := strings.Count(compiled.SQL, ` AS `+stringInput); got != 1 {
		t.Fatalf("string distribution materializations = %d, want 1:\n%s", got, compiled.SQL)
	}
	if strings.Contains(strings.ToUpper(compiled.SQL), "ARRAY JOIN") {
		t.Fatalf("distribution aggregates expanded event rows:\n%s", compiled.SQL)
	}
	if got, want := strings.Count(compiled.SQL, "?"), len(compiled.Args); got != want {
		t.Fatalf("placeholder count = %d, args = %d", got, want)
	}
}

func TestCompileStatsDistributionEvalInputSharesScalarFenceAcrossDomains(t *testing.T) {
	t.Parallel()

	const expression = `bytes+1`
	compiled := compileSPL(
		t,
		`index=gradethis | stats `+
			`exactperc50(eval(`+expression+`)) AS exact_value `+
			`mode(eval(`+expression+`)) AS common_text `+
			`estdc(eval(`+expression+`)) AS estimated_texts`,
	)
	if !slices.Equal(
		compiled.OutputFields,
		[]string{"exact_value", "common_text", "estimated_texts"},
	) {
		t.Fatalf("output fields = %v", compiled.OutputFields)
	}
	for _, required := range []string{
		` AS "__os_measure_numeric_expression_value_0"`,
		` AS "__os_measure_numeric_expression_0"`,
		` AS "__os_measure_string_expression_0"`,
		`groupArrayArray("__os_measure_numeric_expression_0")`,
		`sumMap("__os_measure_string_expression_0"`,
		`uniqCombined64Array(17)("__os_measure_string_expression_0")`,
	} {
		if !strings.Contains(compiled.SQL, required) {
			t.Fatalf("distribution eval SQL missing %q:\n%s", required, compiled.SQL)
		}
	}
	for _, duplicate := range []string{
		"__os_measure_numeric_expression_value_1",
		"__os_measure_numeric_expression_1",
		"__os_measure_string_expression_1",
	} {
		if strings.Contains(compiled.SQL, duplicate) {
			t.Fatalf("shared distribution eval input duplicated %q:\n%s", duplicate, compiled.SQL)
		}
	}
	if got, want := strings.Count(compiled.SQL, "?"), len(compiled.Args); got != want {
		t.Fatalf("placeholder count = %d, args = %d", got, want)
	}
}

func TestCompileStatsDistributionProjectedInputsStayEmpty(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(
		t,
		`index=gradethis | fields service | stats `+
			`exactperc50(metric) AS exact_value upperperc95(metric) AS upper_value `+
			`median(metric) AS median_value estdc(label) AS estimated_labels `+
			`estdc_error(label) AS label_error mode(label) AS common_label BY service`,
	)
	if got := strings.Count(
		compiled.SQL,
		`CAST([], 'Array(Float64)') AS "__os_measure_values_0"`,
	); got != 1 {
		t.Fatalf("projected numeric distribution inputs = %d, want 1:\n%s", got, compiled.SQL)
	}
	if !strings.Contains(compiled.SQL, `CAST([], 'Array(String)')`) {
		t.Fatalf("projected string distribution input is not empty:\n%s", compiled.SQL)
	}
	for _, forbidden := range []string{
		`"__os_fields"."metric"`,
		`"__os_fields"."label"`,
		"fields.metric",
		"fields.label",
	} {
		if strings.Contains(compiled.SQL, forbidden) {
			t.Fatalf("projected distribution input was resurrected through %q:\n%s", forbidden, compiled.SQL)
		}
	}
}
