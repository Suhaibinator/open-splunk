package clickhouse

import (
	"errors"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/plan"
)

func TestCompileStreamStatsSumAcceptsResolvedPlanWithoutParser(t *testing.T) {
	t.Parallel()

	logical, _ := buildStreamStatsSumPlan(
		t,
		`index=gradethis | streamstats count(streamstats_value) AS total`,
	)
	compiled, err := (Compiler{}).Compile(logical)
	if err != nil {
		t.Fatalf("Compile(resolved streamstats sum plan): %v", err)
	}
	if !strings.Contains(compiled.SQL, `sumOrNullArray(`) ||
		!strings.Contains(compiled.SQL, ` OVER (`) ||
		!strings.Contains(compiled.SQL, `AS "total"`) ||
		!strings.Contains(compiled.SQL, `Nullable(Float64)`) {
		t.Fatalf("resolved streamstats sum plan was not lowered as a nullable running sum:\n%s", compiled.SQL)
	}
	assertBoundedStreamStatsSQL(t, compiled)
}

func TestCompileStreamStatsSumMaterializesOneNormalizedNumericInput(t *testing.T) {
	t.Parallel()

	logical, _ := buildStreamStatsSumPlan(
		t,
		`index=gradethis | streamstats count(streamstats_value) AS total`,
	)
	compiled, err := (Compiler{}).Compile(logical)
	if err != nil {
		t.Fatalf("Compile(streamstats sum): %v", err)
	}
	measure := streamStatsSumPrivateAlias(t, compiled.SQL)
	window := `sumOrNullArray(` + measure + `) OVER (`
	sentinel := `LIMIT ` + strconv.FormatUint(MaximumStreamStatsInputRows+1, 10)
	for _, required := range []string{
		`arrayMap(value -> assumeNotNull(value), arrayFilter(value -> isNotNull(value)`,
		`Array(Float64)`,
		` AS ` + measure,
		window,
		`AS Nullable(Float64)`,
		`count() OVER () AS "__os_streamstats_input_count_`,
		` AS MATERIALIZED (`,
		sentinel,
		`AS "total"`,
		StreamStatsInputLimitMarker,
	} {
		if !strings.Contains(compiled.SQL, required) {
			t.Fatalf("streamstats sum SQL missing %q:\n%s", required, compiled.SQL)
		}
	}
	if got := strings.Count(compiled.SQL, measure); got != 2 {
		t.Fatalf("streamstats sum measure alias occurs %d times, want definition and one window use:\n%s", got, compiled.SQL)
	}
	if got := strings.Count(compiled.SQL, window); got != 1 {
		t.Fatalf("streamstats sum window occurs %d times, want one:\n%s", got, compiled.SQL)
	}
	if got := strings.Count(compiled.SQL, ` AS MATERIALIZED (`); got != 1 {
		t.Fatalf("streamstats sum materialized inputs = %d, want one:\n%s", got, compiled.SQL)
	}
	wantPrefix := []any{"streamstats_value"}
	if len(compiled.Args) <= len(wantPrefix) ||
		!slices.Equal(compiled.Args[:len(wantPrefix)], wantPrefix) ||
		compiled.Args[len(wantPrefix)] != "tenant-1" {
		t.Fatalf("streamstats sum argument prefix = %#v, want %#v then tenant", compiled.Args, wantPrefix)
	}
	assertBoundedStreamStatsSQL(t, compiled)
}

func TestCompileStreamStatsSumPinsEveryRowFrameAndKeepsEmptyFramesNull(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name, options, frame string
	}{
		{
			name:  "unbounded current",
			frame: `ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW`,
		},
		{
			name:    "unbounded prior",
			options: `current=false`,
			frame:   `ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING`,
		},
		{
			name:    "one current row",
			options: `window=1`,
			frame:   `ROWS BETWEEN CURRENT ROW AND CURRENT ROW`,
		},
		{
			name:    "one prior row",
			options: `current=false window=1`,
			frame:   `ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING`,
		},
		{
			name:    "three current rows",
			options: `window=3`,
			frame:   `ROWS BETWEEN 2 PRECEDING AND CURRENT ROW`,
		},
		{
			name:    "three prior rows",
			options: `current=false window=3`,
			frame:   `ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING`,
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			logical, _ := buildStreamStatsSumPlan(
				t,
				`index=gradethis | streamstats `+test.options+
					` count(streamstats_value) AS total`,
			)
			compiled, err := (Compiler{}).Compile(logical)
			if err != nil {
				t.Fatalf("Compile(streamstats sum frame): %v", err)
			}
			measure := streamStatsSumPrivateAlias(t, compiled.SQL)
			window := `sumOrNullArray(` + measure + `) OVER (`
			if strings.Count(compiled.SQL, window) != 1 ||
				strings.Count(compiled.SQL, test.frame) != 1 ||
				!strings.Contains(compiled.SQL, `CAST(`+window) ||
				strings.Contains(compiled.SQL, `ifNull(`+window) {
				t.Fatalf("streamstats sum frame is not one nullable exact row window:\n%s", compiled.SQL)
			}
			assertBoundedStreamStatsSQL(t, compiled)
		})
	}
}

func TestCompileStreamStatsSumTreatsProjectedInputAsEmptyNumericArray(t *testing.T) {
	t.Parallel()

	logical, _ := buildStreamStatsSumPlan(
		t,
		`index=gradethis | fields event_id | streamstats count(streamstats_value) AS total`,
	)
	compiled, err := (Compiler{}).Compile(logical)
	if err != nil {
		t.Fatalf("Compile(projected streamstats sum): %v", err)
	}
	measure := streamStatsSumPrivateAlias(t, compiled.SQL)
	if !strings.Contains(compiled.SQL, `CAST([], 'Array(Float64)') AS `+measure) {
		t.Fatalf("projected streamstats sum input was not an empty numeric array:\n%s", compiled.SQL)
	}
	if slices.Contains(compiled.Args, any("streamstats_value")) ||
		slices.Contains(compiled.Args, any("streamstats_value.")) {
		t.Fatalf("projected streamstats sum rebound hidden input: %#v", compiled.Args)
	}
	assertBoundedStreamStatsSQL(t, compiled)
}

func TestCompileStreamStatsSumNormalizesFixedMultivalueOnceWithoutExpansion(t *testing.T) {
	t.Parallel()

	logical, _ := buildStreamStatsSumPlan(
		t,
		`index=gradethis | stats values(host) AS hosts | streamstats count(hosts) AS total`,
	)
	compiled, err := (Compiler{}).Compile(logical)
	if err != nil {
		t.Fatalf("Compile(fixed-multivalue streamstats sum): %v", err)
	}
	measure := streamStatsSumPrivateAlias(t, compiled.SQL)
	for _, required := range []string{
		`arrayMap(element -> `,
		`"hosts"`,
		` AS ` + measure,
		`sumOrNullArray(` + measure + `) OVER (`,
	} {
		if !strings.Contains(compiled.SQL, required) {
			t.Fatalf("fixed-multivalue streamstats sum SQL missing %q:\n%s", required, compiled.SQL)
		}
	}
	// The one groupUniqArray belongs to upstream stats values(host). The
	// streamstats stage consumes that fixed array directly and adds no collector.
	if got := strings.Count(compiled.SQL, "groupUniqArray"); got != 1 {
		t.Fatalf("fixed-multivalue pipeline collectors = %d, want the one upstream values collector:\n%s", got, compiled.SQL)
	}
	for _, forbidden := range []string{"ARRAY JOIN", "arrayJoin(", "groupArray("} {
		if strings.Contains(compiled.SQL, forbidden) {
			t.Fatalf("fixed-multivalue streamstats sum contains %q:\n%s", forbidden, compiled.SQL)
		}
	}
	if got := strings.Count(compiled.SQL, `FROM "open_splunk"."events"`); got != 1 {
		t.Fatalf("fixed-multivalue streamstats physical scans = %d, want one:\n%s", got, compiled.SQL)
	}
	if got, want := strings.Count(compiled.SQL, "?"), len(compiled.Args); got != want {
		t.Fatalf("fixed-multivalue placeholders = %d, args = %d:\nSQL: %s\nargs: %#v", got, want, compiled.SQL, compiled.Args)
	}
}

func TestCompileStreamStatsSumGroupedOutputSeparatesPresenceFromNullableValue(t *testing.T) {
	t.Parallel()

	logical, _ := buildStreamStatsSumPlan(
		t,
		`index=gradethis | streamstats window=2 global=false count(streamstats_value) AS total BY user`,
	)
	compiled, err := (Compiler{}).Compile(logical)
	if err != nil {
		t.Fatalf("Compile(grouped streamstats sum): %v", err)
	}
	measure := streamStatsSumPrivateAlias(t, compiled.SQL)
	for _, required := range []string{
		`PARTITION BY "__os_streamstats_eligible_`,
		`sumOrNullArray(` + measure + `) OVER (`,
		`CAST(NULL AS Nullable(Float64))`,
		`"__os_streamstats_exists_`,
		UnsupportedStatsByValueMarker,
	} {
		if !strings.Contains(compiled.SQL, required) {
			t.Fatalf("grouped streamstats sum SQL missing %q:\n%s", required, compiled.SQL)
		}
	}
	wantPrefix := []any{"user", "user.", "streamstats_value"}
	if len(compiled.Args) <= len(wantPrefix) ||
		!slices.Equal(compiled.Args[:len(wantPrefix)], wantPrefix) ||
		compiled.Args[len(wantPrefix)] != "tenant-1" {
		t.Fatalf("grouped streamstats sum argument prefix = %#v, want %#v then tenant", compiled.Args, wantPrefix)
	}
	assertBoundedStreamStatsSQL(t, compiled)
}

func TestCompileStreamStatsSumResolvesInputBeforeAliasReplacement(t *testing.T) {
	t.Parallel()

	logical, _ := buildStreamStatsSumPlan(
		t,
		`index=gradethis | sort 0 +streamstats_value`+
			` | streamstats count(streamstats_value) AS streamstats_value`+
			` | where streamstats_value>1.5 | table event_id streamstats_value`,
	)
	compiled, err := (Compiler{}).Compile(logical)
	if err != nil {
		t.Fatalf("Compile(streamstats sum alias replacement): %v", err)
	}
	measure := streamStatsSumPrivateAlias(t, compiled.SQL)
	if !strings.Contains(compiled.SQL, ` AS `+measure) ||
		!strings.Contains(compiled.SQL, ` AS "streamstats_value"`) ||
		!regexp.MustCompile(`AS "__os_streamstats_order_[0-9]+_0"`).MatchString(compiled.SQL) {
		t.Fatalf("streamstats sum alias replacement lost its input or order snapshot:\n%s", compiled.SQL)
	}
	if len(compiled.Args) < 1 || compiled.Args[0] != "streamstats_value" {
		t.Fatalf("streamstats sum alias replacement args = %#v", compiled.Args)
	}
	assertBoundedStreamStatsSQL(t, compiled)
}

func TestCompileStreamStatsSumPublishesAndValidatesCanonicalDefaultOutput(t *testing.T) {
	t.Parallel()

	logical, operator := buildStreamStatsSumPlan(
		t,
		`index=gradethis | streamstats count(Payload.Items)`,
	)
	operator.Measure.Output = "sum(Payload.Items)"
	compiled, err := (Compiler{}).Compile(logical)
	if err != nil {
		t.Fatalf("Compile(default-output streamstats sum): %v", err)
	}
	if !slices.Contains(compiled.OutputFields, "sum(Payload.Items)") ||
		!strings.Contains(compiled.SQL, `AS "sum(Payload.Items)"`) {
		t.Fatalf("streamstats sum did not publish its canonical default: fields=%#v\n%s", compiled.OutputFields, compiled.SQL)
	}
	if len(compiled.Args) < 1 || compiled.Args[0] != "Payload.Items" {
		t.Fatalf("streamstats sum default-output args = %#v", compiled.Args)
	}
	assertBoundedStreamStatsSQL(t, compiled)
}

func TestCompileStreamStatsSumRejectsForgedCanonicalPlans(t *testing.T) {
	t.Parallel()

	valid := func() *plan.StreamAggregate {
		return &plan.StreamAggregate{
			Measure: plan.AggregateMeasure{
				Function: plan.AggregateFunctionSum,
				Input:    mustResolveStreamStatsField(t, "status"),
				Output:   "total",
			},
			IncludeCurrent: true,
			Global:         true,
		}
	}
	for _, test := range []struct {
		name   string
		mutate func(*plan.StreamAggregate)
	}{
		{"missing input", func(operator *plan.StreamAggregate) { operator.Measure.Input = plan.FieldRef{} }},
		{"forged canonical input", func(operator *plan.StreamAggregate) { operator.Measure.Input.Canonical = true }},
		{"forged input path", func(operator *plan.StreamAggregate) { operator.Measure.Input.Path = []string{"attacker"} }},
		{"quoted input", func(operator *plan.StreamAggregate) {
			operator.Measure.Input = mustResolveStreamStatsField(t, "'status'")
		}},
		{"comma input", func(operator *plan.StreamAggregate) {
			operator.Measure.Input = mustResolveStreamStatsField(t, "status,host")
		}},
		{"whitespace input", func(operator *plan.StreamAggregate) {
			operator.Measure.Input = mustResolveStreamStatsField(t, "status host")
		}},
		{"mismatched sum default", func(operator *plan.StreamAggregate) {
			operator.Measure.Output = "sum(other)"
		}},
		{"count default on sum", func(operator *plan.StreamAggregate) {
			operator.Measure.Output = "count(status)"
		}},
		{"whitespace output", func(operator *plan.StreamAggregate) {
			operator.Measure.Output = "running total"
		}},
		{"comma group", func(operator *plan.StreamAggregate) {
			operator.GroupBy = []plan.FieldRef{mustResolveStreamStatsField(t, "host,status")}
		}},
		{"predicate", func(operator *plan.StreamAggregate) {
			operator.Measure.Predicate = &plan.BooleanExpression{}
		}},
		{"percentile", func(operator *plan.StreamAggregate) { operator.Measure.Percentile = 50 }},
		{"unsupported average", func(operator *plan.StreamAggregate) {
			operator.Measure.Function = plan.AggregateFunctionAverage
		}},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			operator := valid()
			test.mutate(operator)
			if _, err := (Compiler{}).Compile(appendStreamStatsOperator(
				buildPlan(t, `index=gradethis`),
				operator,
			)); err == nil {
				t.Fatal("Compile accepted forged streamstats sum metadata")
			}
		})
	}

	canonical := valid()
	canonical.Measure.Output = "sum(status)"
	if _, err := (Compiler{}).Compile(appendStreamStatsOperator(
		buildPlan(t, `index=gradethis`),
		canonical,
	)); err != nil {
		t.Fatalf("Compile rejected canonical streamstats sum default: %v", err)
	}
}

func TestCompileStreamStatsSumProtectsOpenFieldsPayload(t *testing.T) {
	t.Parallel()

	logical, operator := buildStreamStatsSumPlan(
		t,
		`index=gradethis | streamstats count(status) AS total`,
	)
	operator.Measure.Input = mustResolveStreamStatsField(t, "fields")
	_, err := (Compiler{}).Compile(logical)
	var diagnostic *plan.Diagnostic
	if !errors.As(err, &diagnostic) ||
		diagnostic.Code != "SPL_AMBIGUOUS_STREAMSTATS_FIELD" {
		t.Fatalf("open fields streamstats sum input error = %#v", err)
	}
}

func buildStreamStatsSumPlan(
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
		operator.Measure.Function = plan.AggregateFunctionSum
		return logical, operator
	}
	t.Fatalf("Build(%q) produced no stream aggregate", source)
	return nil, nil
}

func streamStatsSumPrivateAlias(t *testing.T, sql string) string {
	t.Helper()
	alias := regexp.MustCompile(`"__os_streamstats_measure_[0-9]+"`).FindString(sql)
	if alias == "" {
		t.Fatalf("streamstats sum SQL has no private measure alias:\n%s", sql)
	}
	return alias
}
