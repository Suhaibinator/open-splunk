package clickhouse

import (
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/plan"
)

func TestCompileStreamStatsAverageUsesOneWeightedNullableArrayWindow(t *testing.T) {
	t.Parallel()

	logical, _ := buildStreamStatsAveragePlan(
		t,
		`index=gradethis | streamstats count(streamstats_value) AS running_mean`,
	)
	compiled, err := (Compiler{}).Compile(logical)
	if err != nil {
		t.Fatalf("Compile(resolved streamstats average plan): %v", err)
	}
	measure := streamStatsSumPrivateAlias(t, compiled.SQL)
	window := `avgOrNullArray(` + measure + `) OVER (`
	for _, required := range []string{
		`arrayMap(value -> assumeNotNull(value), arrayFilter(value -> isNotNull(value)`,
		`Array(Float64)`,
		` AS ` + measure,
		window,
		`AS Nullable(Float64)`,
		`count() OVER () AS "__os_streamstats_input_count_`,
		` AS MATERIALIZED (`,
		`LIMIT ` + strconv.FormatUint(MaximumStreamStatsInputRows+1, 10),
		`AS "running_mean"`,
		StreamStatsInputLimitMarker,
	} {
		if !strings.Contains(compiled.SQL, required) {
			t.Fatalf("streamstats average SQL missing %q:\n%s", required, compiled.SQL)
		}
	}
	if got := strings.Count(compiled.SQL, measure); got != 2 {
		t.Fatalf("streamstats average measure alias occurs %d times, want definition and one window use:\n%s", got, compiled.SQL)
	}
	if got := strings.Count(compiled.SQL, window); got != 1 {
		t.Fatalf("streamstats average windows = %d, want one:\n%s", got, compiled.SQL)
	}
	if got := strings.Count(compiled.SQL, ` AS MATERIALIZED (`); got != 1 {
		t.Fatalf("streamstats average materialized inputs = %d, want one:\n%s", got, compiled.SQL)
	}
	for _, forbidden := range []string{
		"arrayAvg(", "sumOrNullArray(", "ARRAY JOIN", "arrayJoin(", "groupArray(",
	} {
		if strings.Contains(compiled.SQL, forbidden) {
			t.Fatalf("streamstats average contains forbidden %q:\n%s", forbidden, compiled.SQL)
		}
	}
	if got := strings.Count(compiled.SQL, `FROM "open_splunk"."events"`); got != 1 {
		t.Fatalf("streamstats average physical scans = %d, want one:\n%s", got, compiled.SQL)
	}
	wantPrefix := []any{"streamstats_value"}
	if len(compiled.Args) <= len(wantPrefix) ||
		!slices.Equal(compiled.Args[:len(wantPrefix)], wantPrefix) ||
		compiled.Args[len(wantPrefix)] != "tenant-1" {
		t.Fatalf("streamstats average argument prefix = %#v, want %#v then tenant", compiled.Args, wantPrefix)
	}
	assertBoundedStreamStatsSQL(t, compiled)
}

func TestCompileStreamStatsAveragePinsFramesAndGroupedFloatState(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name, options, frame string
	}{
		{name: "unbounded current", frame: `ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW`},
		{name: "unbounded prior", options: `current=false`, frame: `ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING`},
		{name: "one current row", options: `window=1`, frame: `ROWS BETWEEN CURRENT ROW AND CURRENT ROW`},
		{name: "one prior row", options: `current=false window=1`, frame: `ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING`},
		{name: "three current rows", options: `window=3`, frame: `ROWS BETWEEN 2 PRECEDING AND CURRENT ROW`},
		{name: "three prior rows", options: `current=false window=3`, frame: `ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING`},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			logical, _ := buildStreamStatsAveragePlan(
				t,
				`index=gradethis | streamstats `+test.options+
					` count(streamstats_value) AS running_mean`,
			)
			compiled, err := (Compiler{}).Compile(logical)
			if err != nil {
				t.Fatalf("Compile(streamstats average frame): %v", err)
			}
			measure := streamStatsSumPrivateAlias(t, compiled.SQL)
			window := `avgOrNullArray(` + measure + `) OVER (`
			if strings.Count(compiled.SQL, window) != 1 ||
				strings.Count(compiled.SQL, test.frame) != 1 ||
				!strings.Contains(compiled.SQL, `CAST(`+window) ||
				strings.Contains(compiled.SQL, `ifNull(`+window) {
				t.Fatalf("streamstats average frame is not one nullable exact row window:\n%s", compiled.SQL)
			}
			assertBoundedStreamStatsSQL(t, compiled)
		})
	}

	grouped, _ := buildStreamStatsAveragePlan(
		t,
		`index=gradethis | streamstats window=2 global=false count(streamstats_value) AS running_mean BY user`+
			` | where running_mean>1.5 | table event_id running_mean`,
	)
	compiled, err := (Compiler{}).Compile(grouped)
	if err != nil {
		t.Fatalf("Compile(grouped streamstats average): %v", err)
	}
	measure := streamStatsSumPrivateAlias(t, compiled.SQL)
	for _, required := range []string{
		`PARTITION BY "__os_streamstats_eligible_`,
		`avgOrNullArray(` + measure + `) OVER (`,
		`CAST(NULL AS Nullable(Float64))`,
		`"__os_streamstats_exists_`,
		UnsupportedStatsByValueMarker,
	} {
		if !strings.Contains(compiled.SQL, required) {
			t.Fatalf("grouped streamstats average SQL missing %q:\n%s", required, compiled.SQL)
		}
	}
	assertBoundedStreamStatsSQL(t, compiled)
}

func TestCompileStreamStatsAverageCanonicalDefaultAndDefensiveValidation(t *testing.T) {
	t.Parallel()

	logical, operator := buildStreamStatsAveragePlan(
		t,
		`index=gradethis | streamstats count(Payload.Items)`,
	)
	operator.Measure.Output = "avg(Payload.Items)"
	compiled, err := (Compiler{}).Compile(logical)
	if err != nil {
		t.Fatalf("Compile(default-output streamstats average): %v", err)
	}
	if !slices.Contains(compiled.OutputFields, "avg(Payload.Items)") ||
		!strings.Contains(compiled.SQL, `AS "avg(Payload.Items)"`) {
		t.Fatalf("streamstats average canonical output missing: fields=%#v\n%s", compiled.OutputFields, compiled.SQL)
	}

	valid := func() *plan.StreamAggregate {
		return &plan.StreamAggregate{
			Measure: plan.AggregateMeasure{
				Function: plan.AggregateFunctionAverage,
				Input:    mustResolveStreamStatsField(t, "status"),
				Output:   "mean",
			},
			IncludeCurrent: true,
			Global:         true,
		}
	}
	for _, test := range []struct {
		name   string
		mutate func(*plan.StreamAggregate)
	}{
		{"missing input", func(op *plan.StreamAggregate) { op.Measure.Input = plan.FieldRef{} }},
		{"forged canonical input", func(op *plan.StreamAggregate) { op.Measure.Input.Canonical = true }},
		{"forged input path", func(op *plan.StreamAggregate) { op.Measure.Input.Path = []string{"attacker"} }},
		{"mismatched average default", func(op *plan.StreamAggregate) { op.Measure.Output = "avg(other)" }},
		{"sum default on average", func(op *plan.StreamAggregate) { op.Measure.Output = "sum(status)" }},
		{"predicate", func(op *plan.StreamAggregate) { op.Measure.Predicate = &plan.BooleanExpression{} }},
		{"percentile", func(op *plan.StreamAggregate) { op.Measure.Percentile = 50 }},
		{"unsupported maximum", func(op *plan.StreamAggregate) { op.Measure.Function = plan.AggregateFunctionMaximum }},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			op := valid()
			test.mutate(op)
			if _, err := (Compiler{}).Compile(appendStreamStatsOperator(
				buildPlan(t, `index=gradethis`),
				op,
			)); err == nil {
				t.Fatal("Compile accepted forged streamstats average metadata")
			}
		})
	}

	canonical := valid()
	canonical.Measure.Output = "avg(status)"
	if _, err := (Compiler{}).Compile(appendStreamStatsOperator(
		buildPlan(t, `index=gradethis`),
		canonical,
	)); err != nil {
		t.Fatalf("Compile rejected canonical streamstats average default: %v", err)
	}
}

func buildStreamStatsAveragePlan(
	t *testing.T,
	source string,
) (*plan.Query, *plan.StreamAggregate) {
	t.Helper()
	logical := buildPlan(t, source)
	for index := len(logical.Operators) - 1; index >= 0; index-- {
		operator, ok := logical.Operators[index].(*plan.StreamAggregate)
		if !ok {
			continue
		}
		operator.Measure.Function = plan.AggregateFunctionAverage
		return logical, operator
	}
	t.Fatalf("Build(%q) produced no stream aggregate", source)
	return nil, nil
}
