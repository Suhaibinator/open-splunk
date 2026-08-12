package clickhouse

import (
	"slices"
	"strings"
	"testing"
)

func TestCompileStatsDistributionFamilies(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(
		t,
		`index=gradethis | stats exactperc95(metric) upperperc95(metric) `+
			`median(metric) estdc(host) estdc_error(host) mode(host)`,
	)
	wantFields := []string{
		"exactperc95(metric)",
		"upperperc95(metric)",
		"median(metric)",
		"estdc(host)",
		"estdc_error(host)",
		"mode(host)",
	}
	if !slices.Equal(compiled.OutputFields, wantFields) {
		t.Fatalf("output fields = %v, want %v", compiled.OutputFields, wantFields)
	}
	for _, required := range []string{
		`arrayElementOrNull(arraySort(groupArrayArray("__os_measure_values_0"))`,
		`quantilesGKOrNullArray(100, 0.95, 0.96)("__os_measure_values_0")`,
		`quantilesGKOrNullArray(100, 0.5)("__os_measure_values_0")`,
		`uniqCombined64Array(17)("__os_measure_strings_0")`,
		`sumMap("__os_measure_strings_0", arrayMap(_ -> toUInt64(1), "__os_measure_strings_0"))`,
	} {
		if !strings.Contains(compiled.SQL, required) {
			t.Fatalf("distribution SQL missing %q:\n%s", required, compiled.SQL)
		}
	}
	if got := strings.Count(compiled.SQL, ` AS "__os_measure_values_0"`); got != 1 {
		t.Fatalf("numeric distribution materializations = %d, want 1:\n%s", got, compiled.SQL)
	}
	if got := strings.Count(compiled.SQL, ` AS "__os_measure_strings_0"`); got != 1 {
		t.Fatalf("string distribution materializations = %d, want 1:\n%s", got, compiled.SQL)
	}
}

func TestCompileStatsOrderAndTimeFamilies(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(
		t,
		`index=gradethis | stats first(host) AS first_host last(host) AS last_host `+
			`earliest_time(host) AS first_seen latest_time(host) AS last_seen `+
			`rate(duration) AS per_second`,
	)
	if !slices.Equal(compiled.OutputFields, []string{
		"first_host", "last_host", "first_seen", "last_seen", "per_second",
	}) {
		t.Fatalf("output fields = %v", compiled.OutputFields)
	}
	for _, required := range []string{
		`row_number() OVER (ORDER BY`,
		`argMinOrNullIf(`,
		`argMaxOrNullIf(`,
		`toFloat64(toUnixTimestamp64Nano(`,
		`countIf(isNotNull(arrayElementOrNull("__os_measure_values_0", 1))`,
		`ifNotFinite((argMaxOrNullIf(`,
	} {
		if !strings.Contains(compiled.SQL, required) {
			t.Fatalf("order/time SQL missing %q:\n%s", required, compiled.SQL)
		}
	}
	if strings.Contains(strings.ToUpper(compiled.SQL), "ARRAY JOIN") {
		t.Fatalf("order/time stats unexpectedly expanded rows:\n%s", compiled.SQL)
	}
}

func TestCompileStatsGeneralEvalInputSharesScalarMaterialization(t *testing.T) {
	t.Parallel()

	const expression = `lower(host)`
	compiled := compileSPL(
		t,
		`index=gradethis | stats `+
			`dc(eval(`+expression+`)) AS distinct_status `+
			`values(eval(`+expression+`)) AS statuses `+
			`list(eval(`+expression+`)) AS ordered_statuses `+
			`min(eval(`+expression+`)) AS minimum_status `+
			`max(eval(`+expression+`)) AS maximum_status `+
			`mode(eval(`+expression+`)) AS modal_status `+
			`first(eval(`+expression+`)) AS first_status `+
			`last(eval(`+expression+`)) AS last_status `+
			`earliest(eval(`+expression+`)) AS earliest_status `+
			`latest(eval(`+expression+`)) AS latest_status `+
			`earliest_time(eval(`+expression+`)) AS earliest_status_time `+
			`latest_time(eval(`+expression+`)) AS latest_status_time`,
	)
	if len(compiled.OutputFields) != 12 {
		t.Fatalf("output fields = %v", compiled.OutputFields)
	}
	if got := strings.Count(
		compiled.SQL,
		` AS "__os_measure_numeric_expression_value_0"`,
	); got != 1 {
		t.Fatalf("shared eval value materializations = %d, want 1:\n%s", got, compiled.SQL)
	}
	if strings.Contains(compiled.SQL, "__os_measure_numeric_expression_value_1") {
		t.Fatalf("identical eval expression was compiled more than once:\n%s", compiled.SQL)
	}
	for _, required := range []string{
		` AS "__os_measure_string_expression_0"`,
		`groupUniqArrayArray(`,
		`sumMap("__os_measure_string_expression_0"`,
		`row_number() OVER (ORDER BY`,
		`"earliest_status_time"`,
		`"latest_status_time"`,
	} {
		if !strings.Contains(compiled.SQL, required) {
			t.Fatalf("general eval stats SQL missing %q:\n%s", required, compiled.SQL)
		}
	}
}
