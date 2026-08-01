package clickhouse

import (
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/plan"
)

func TestCompileEventStatsAverageAcceptsResolvedPlanWithoutParser(t *testing.T) {
	t.Parallel()

	logical := buildPlan(
		t,
		`index=gradethis | eventstats count(eventstats_value) AS mean`,
	)
	var eventAggregate *plan.EventAggregate
	for _, operator := range logical.Operators {
		if candidate, ok := operator.(*plan.EventAggregate); ok {
			eventAggregate = candidate
			break
		}
	}
	if eventAggregate == nil {
		t.Fatal("count(field) plan has no EventAggregate")
	}
	eventAggregate.Measure.Function = plan.AggregateFunctionAverage

	compiled, err := (Compiler{}).Compile(logical)
	if err != nil {
		t.Fatalf("Compile(resolved eventstats avg plan): %v", err)
	}
	if !strings.Contains(compiled.SQL, `avgOrNullArray(`) ||
		!strings.Contains(compiled.SQL, `CAST(NULL AS Nullable(Float64))`) ||
		!strings.Contains(compiled.SQL, `AS "mean"`) {
		t.Fatalf("resolved eventstats avg plan was not lowered as nullable Float64:\n%s", compiled.SQL)
	}
}

func TestCompileEventStatsAverageRejectsForgedMeasureMetadata(t *testing.T) {
	t.Parallel()

	base := buildPlan(
		t,
		`index=gradethis | eventstats count(eventstats_value) AS mean`,
	)
	for _, test := range []struct {
		name   string
		mutate func(*plan.EventAggregate)
	}{
		{"predicate", func(operator *plan.EventAggregate) {
			operator.Measure.Predicate = &plan.ComparisonExpression{}
		}},
		{"missing input", func(operator *plan.EventAggregate) {
			operator.Measure.Input = plan.FieldRef{}
		}},
		{"private output", func(operator *plan.EventAggregate) {
			operator.Measure.Output = "__os_eventstats_avg_private"
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			logical, operator := cloneEventAggregatePlan(t, base)
			operator.Measure.Function = plan.AggregateFunctionAverage
			test.mutate(operator)
			if _, err := (Compiler{}).Compile(logical); err == nil {
				t.Fatal("Compile accepted forged eventstats avg measure")
			}
		})
	}
}

func TestCompileEventStatsAverageUsesBoundedSharedNumericPath(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(
		t,
		`index=gradethis source="eventstats-sum-fixture" | eventstats avg(eventstats_value) AS mean | where mean>6 | table event_id mean`,
	)
	if !slices.Equal(compiled.OutputFields, []string{"event_id", "mean"}) {
		t.Fatalf("eventstats avg output fields = %#v", compiled.OutputFields)
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
		`avgOrNullArray(` + measureAlias + `)`,
		`AS "mean"`,
		EventStatsInputLimitMarker,
		materializedCTESettingsSQL,
	} {
		if !strings.Contains(compiled.SQL, required) {
			t.Fatalf("eventstats avg SQL missing %q:\n%s", required, compiled.SQL)
		}
	}
	if strings.Count(compiled.SQL, inputAlias+` AS MATERIALIZED (`) != 1 ||
		strings.Count(compiled.SQL, sentinel) != 1 ||
		strings.Count(compiled.SQL, ` AS `+measureAlias) != 1 ||
		strings.Count(compiled.SQL, `avgOrNullArray(`) != 1 ||
		strings.Count(compiled.SQL, `FROM "open_splunk"."events"`) != 1 {
		t.Fatalf("eventstats avg did not retain one input/normalization/state/scan:\n%s", compiled.SQL)
	}
	for _, forbidden := range []string{"ARRAY JOIN", "arrayJoin(", "groupArray(", "arrayAvg("} {
		if strings.Contains(compiled.SQL, forbidden) {
			t.Fatalf("eventstats avg SQL contains row-expanding %q:\n%s", forbidden, compiled.SQL)
		}
	}
	if got, want := strings.Count(compiled.SQL, "?"), len(compiled.Args); got != want {
		t.Fatalf("placeholder count = %d, args = %d", got, want)
	}
}

func TestCompileEventStatsAveragePreservesComputedNonFiniteAndNormalizesTime(
	t *testing.T,
) {
	t.Parallel()

	compiled := compileSPL(
		t,
		`index=gradethis | eventstats avg(_time) AS mean_time | table mean_time`,
	)
	if !strings.Contains(compiled.SQL, `toUnixTimestamp64Nano(`) ||
		!strings.Contains(compiled.SQL, `avgOrNullArray(`) {
		t.Fatalf("eventstats avg canonical-time normalization is missing:\n%s", compiled.SQL)
	}
	if strings.Contains(compiled.SQL, `ifNotFinite(avgOrNullArray(`) {
		t.Fatalf("computed non-finite eventstats avg was converted to null:\n%s", compiled.SQL)
	}
}
