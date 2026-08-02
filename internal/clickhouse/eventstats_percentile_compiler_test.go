package clickhouse

import (
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/plan"
)

func TestCompileEventStatsPercentileAcceptsResolvedPlanWithoutParser(t *testing.T) {
	t.Parallel()

	logical, eventAggregate := cloneEventAggregatePlan(t, buildPlan(
		t,
		`index=gradethis | eventstats count(duration_ms) AS p95_ms`,
	))
	eventAggregate.Measure.Function = plan.AggregateFunctionPercentile
	eventAggregate.Measure.Percentile = 95

	compiled, err := (Compiler{}).Compile(logical)
	if err != nil {
		t.Fatalf("Compile(resolved eventstats p95 plan): %v", err)
	}
	for _, required := range []string{
		`quantilesGKOrNullArray(100, 0.95)(`,
		`arrayElementOrNull(`,
		`CAST(NULL AS Nullable(Float64))`,
		`AS "p95_ms"`,
	} {
		if !strings.Contains(compiled.SQL, required) {
			t.Fatalf("resolved eventstats p95 plan SQL missing %q:\n%s", required, compiled.SQL)
		}
	}
}

func TestCompileEventStatsPercentileUsesBoundedSharedNumericPath(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(
		t,
		`index=gradethis source="eventstats-percentile-fixture"`+
			` | eventstats p95(eventstats_value) AS p95_value`+
			` | where p95_value>6 | table event_id p95_value`,
	)
	if !slices.Equal(compiled.OutputFields, []string{"event_id", "p95_value"}) {
		t.Fatalf("eventstats p95 output fields = %#v", compiled.OutputFields)
	}

	inputAlias := eventStatsPrivateAlias(t, compiled.SQL, "__os_eventstats_input_")
	measureAlias := eventStatsPrivateAlias(t, compiled.SQL, "__os_eventstats_measure_")
	sentinel := `LIMIT ` + strconv.FormatUint(MaximumEventStatsInputRows+1, 10)
	for _, required := range []string{
		inputAlias + ` AS MATERIALIZED (`,
		sentinel,
		`dynamicElement("__os_fields"."eventstats_value", 'Array(Dynamic)')`,
		`CAST(NULL AS Nullable(Float64))`,
		` AS ` + measureAlias,
		`arrayElementOrNull(quantilesGKOrNullArray(100, 0.95)(` + measureAlias + `), 1)`,
		`AS "p95_value"`,
		EventStatsInputLimitMarker,
		materializedCTESettingsSQL,
	} {
		if !strings.Contains(compiled.SQL, required) {
			t.Fatalf("eventstats p95 SQL missing %q:\n%s", required, compiled.SQL)
		}
	}
	if strings.Count(compiled.SQL, inputAlias+` AS MATERIALIZED (`) != 1 ||
		strings.Count(compiled.SQL, sentinel) != 1 ||
		strings.Count(compiled.SQL, ` AS `+measureAlias) != 1 ||
		strings.Count(compiled.SQL, `quantilesGKOrNullArray(`) != 1 ||
		strings.Count(compiled.SQL, `FROM "open_splunk"."events"`) != 1 {
		t.Fatalf("eventstats p95 did not retain one input/normalization/state/scan:\n%s", compiled.SQL)
	}
	for _, forbidden := range []string{
		"ARRAY JOIN",
		"arrayJoin(",
		"groupArray(",
		"quantileExact",
		"ORDER BY p95_value",
	} {
		if strings.Contains(compiled.SQL, forbidden) {
			t.Fatalf("eventstats p95 SQL contains forbidden %q:\n%s", forbidden, compiled.SQL)
		}
	}
	if got, want := strings.Count(compiled.SQL, "?"), len(compiled.Args); got != want {
		t.Fatalf("placeholder count = %d, args = %d", got, want)
	}
}

func TestCompileEventStatsPercentileUsesGroupedLeftJoinAndCanonicalLevels(
	testingT *testing.T,
) {
	testingT.Parallel()

	for _, test := range []struct {
		name     string
		function string
		literal  string
	}{
		{name: "minimum", function: "p1", literal: "0.01"},
		{name: "median", function: "perc50", literal: "0.5"},
		{name: "maximum", function: "p99", literal: "0.99"},
	} {
		test := test
		testingT.Run(test.name, func(t *testing.T) {
			t.Parallel()

			compiled := compileSPL(
				t,
				`index=gradethis | eventstats `+test.function+
					`(eventstats_value) AS percentile BY eventstats_group`+
					` | table event_id percentile`,
			)
			for _, required := range []string{
				`quantilesGKOrNullArray(100, ` + test.literal + `)(`,
				` GROUP BY "__os_eventstats_group_0"`,
				` LEFT JOIN "__os_eventstats_counts_`,
				`CAST(NULL AS Nullable(Float64))`,
				`AS "percentile"`,
			} {
				if !strings.Contains(compiled.SQL, required) {
					t.Fatalf("grouped eventstats %s SQL missing %q:\n%s", test.function, required, compiled.SQL)
				}
			}
			if strings.Count(compiled.SQL, `quantilesGKOrNullArray(`) != 1 ||
				strings.Count(compiled.SQL, ` LEFT JOIN `) != 1 ||
				strings.Count(compiled.SQL, `FROM "open_splunk"."events"`) != 1 {
				t.Fatalf("grouped eventstats %s shape is not one state/join/scan:\n%s", test.function, compiled.SQL)
			}
		})
	}
}

func TestCompileEventStatsPercentileNormalizesTimeAndFixedMultivalue(t *testing.T) {
	t.Parallel()

	timePercentile := compileSPL(
		t,
		`index=gradethis | eventstats p95(_time) AS p95_time | table p95_time`,
	)
	if !strings.Contains(timePercentile.SQL, `toUnixTimestamp64Nano(`) ||
		!strings.Contains(timePercentile.SQL, `quantilesGKOrNullArray(100, 0.95)(`) {
		t.Fatalf("eventstats time percentile is not epoch based:\n%s", timePercentile.SQL)
	}

	fixedMultivalue := compileSPL(
		t,
		`index=gradethis | stats values(eventstats_value) AS values`+
			` | eventstats p50(values) AS median | table values median`,
	)
	if !strings.Contains(fixedMultivalue.SQL, `arrayMap(element ->`) ||
		!strings.Contains(fixedMultivalue.SQL, `quantilesGKOrNullArray(100, 0.5)(`) ||
		strings.Contains(fixedMultivalue.SQL, `ARRAY JOIN`) {
		t.Fatalf("eventstats fixed-multivalue percentile lost bounded array normalization:\n%s", fixedMultivalue.SQL)
	}
}

func TestCompileEventStatsPercentileRejectsForgedMeasureMetadata(t *testing.T) {
	t.Parallel()

	base := buildPlan(
		t,
		`index=gradethis | eventstats count(duration_ms) AS p95_ms`,
	)
	for _, test := range []struct {
		name   string
		mutate func(*plan.EventAggregate)
	}{
		{"zero level", func(operator *plan.EventAggregate) {
			operator.Measure.Function = plan.AggregateFunctionPercentile
			operator.Measure.Percentile = 0
		}},
		{"level 100", func(operator *plan.EventAggregate) {
			operator.Measure.Function = plan.AggregateFunctionPercentile
			operator.Measure.Percentile = 100
		}},
		{"predicate", func(operator *plan.EventAggregate) {
			operator.Measure.Function = plan.AggregateFunctionPercentile
			operator.Measure.Percentile = 95
			operator.Measure.Predicate = &plan.ComparisonExpression{}
		}},
		{"missing input", func(operator *plan.EventAggregate) {
			operator.Measure.Function = plan.AggregateFunctionPercentile
			operator.Measure.Percentile = 95
			operator.Measure.Input = plan.FieldRef{}
		}},
		{"percentile metadata on average", func(operator *plan.EventAggregate) {
			operator.Measure.Function = plan.AggregateFunctionAverage
			operator.Measure.Percentile = 95
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			logical, operator := cloneEventAggregatePlan(t, base)
			test.mutate(operator)
			if _, err := (Compiler{}).Compile(logical); err == nil {
				t.Fatal("Compile accepted forged eventstats percentile measure")
			}
		})
	}
}
