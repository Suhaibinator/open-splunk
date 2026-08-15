package clickhouse

import (
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/plan"
)

func TestCompileEventStatsSumAcceptsResolvedPlanWithoutParser(t *testing.T) {
	t.Parallel()

	logical := buildPlan(
		t,
		`index=gradethis | eventstats count(eventstats_value) AS total`,
	)
	var eventAggregate *plan.EventAggregate
	for _, operator := range logical.Operators {
		candidate, ok := operator.(*plan.EventAggregate)
		if ok {
			eventAggregate = candidate
			break
		}
	}
	if eventAggregate == nil {
		t.Fatal("count(field) plan has no EventAggregate")
	}
	eventAggregate.Measure.Function = plan.AggregateFunctionSum

	compiled, err := (Compiler{}).Compile(logical)
	if err != nil {
		t.Fatalf("Compile(resolved eventstats sum plan): %v", err)
	}
	if !strings.Contains(compiled.SQL, `sumOrNullArray(`) ||
		!strings.Contains(compiled.SQL, `AS "total"`) {
		t.Fatalf("resolved eventstats sum plan was not lowered as a nullable sum:\n%s", compiled.SQL)
	}
}

func TestCompileEventStatsSumRejectsForgedMeasureMetadata(t *testing.T) {
	t.Parallel()

	base := buildPlan(
		t,
		`index=gradethis | eventstats count(eventstats_value) AS total`,
	)
	tests := []struct {
		name   string
		mutate func(*plan.EventAggregate)
	}{
		{
			name: "predicate",
			mutate: func(operator *plan.EventAggregate) {
				operator.Measure.Predicate = &plan.ComparisonExpression{
					Field: plan.FieldRef{Name: "host", Canonical: true},
					Op:    plan.ComparisonOpEqual,
					Value: plan.Value{Kind: plan.ValueKindString, String: "web"},
				}
			},
		},
		{
			name: "percentile",
			mutate: func(operator *plan.EventAggregate) {
				operator.Measure.Percentile = 95
			},
		},
		{
			name: "missing input",
			mutate: func(operator *plan.EventAggregate) {
				operator.Measure.Input = plan.FieldRef{}
			},
		},
		{
			name: "malformed input",
			mutate: func(operator *plan.EventAggregate) {
				operator.Measure.Input = plan.FieldRef{Name: "eventstats_value"}
			},
		},
		{
			name: "private output",
			mutate: func(operator *plan.EventAggregate) {
				operator.Measure.Output = "__os_eventstats_sum_private"
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			logical, operator := cloneEventAggregatePlan(t, base)
			operator.Measure.Function = plan.AggregateFunctionSum
			test.mutate(operator)
			if _, err := (Compiler{}).Compile(logical); err == nil {
				t.Fatal("Compile() accepted forged eventstats sum measure")
			}
		})
	}
}

func TestCompileEventStatsSumUsesOneBoundedMaterializedNumericInput(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(
		t,
		`index=gradethis source="eventstats-sum-fixture" | eventstats sum(eventstats_value) AS total | where total>30 | table event_id total`,
	)
	if !slices.Equal(compiled.OutputFields, []string{"event_id", "total"}) {
		t.Fatalf("eventstats sum output fields = %#v", compiled.OutputFields)
	}

	inputAlias := eventStatsPrivateAlias(
		t,
		compiled.SQL,
		"__os_eventstats_input_",
	)
	sentinel := `LIMIT ` + strconv.FormatUint(MaximumEventStatsInputRows+1, 10)
	for _, required := range []string{
		inputAlias + ` AS MATERIALIZED (`,
		sentinel,
		`dynamicElement("__os_fields"."eventstats_value", 'Array(Dynamic)')`,
		`arrayMap(value -> assumeNotNull(value), arrayFilter(value -> isNotNull(value),`,
		`ifNotFinite(`,
		`CAST(NULL AS Nullable(Float64))`,
		`sumOrNullArray(`,
		`AS "total"`,
		`toFloat64("total") > toFloat64(CAST(? AS Int64))`,
		EventStatsInputLimitMarker,
		materializedCTESettingsSQL,
	} {
		if !strings.Contains(compiled.SQL, required) {
			t.Fatalf("eventstats sum SQL missing %q:\n%s", required, compiled.SQL)
		}
	}
	if got := strings.Count(compiled.SQL, inputAlias+` AS MATERIALIZED (`); got != 1 {
		t.Fatalf("bounded eventstats input definitions = %d, want 1:\n%s", got, compiled.SQL)
	}
	if got := strings.Count(compiled.SQL, sentinel); got != 1 {
		t.Fatalf("eventstats sentinel limits = %d, want 1:\n%s", got, compiled.SQL)
	}
	if got := strings.Count(compiled.SQL, `sumOrNullArray(`); got != 1 {
		t.Fatalf("eventstats sum aggregate count = %d, want 1:\n%s", got, compiled.SQL)
	}
	if got := strings.Count(compiled.SQL, `FROM "open_splunk"."events"`); got != 1 {
		t.Fatalf("eventstats sum physical scan count = %d, want 1:\n%s", got, compiled.SQL)
	}
	for _, forbidden := range []string{
		"ARRAY JOIN",
		"arrayJoin(",
		"groupArray(",
		"arraySum(",
	} {
		if strings.Contains(compiled.SQL, forbidden) {
			t.Fatalf("eventstats sum SQL contains row-multiplying %q:\n%s", forbidden, compiled.SQL)
		}
	}
	if got, want := strings.Count(compiled.SQL, "?"), len(compiled.Args); got != want {
		t.Fatalf(
			"eventstats sum placeholder count = %d, args = %d\nSQL: %s\nargs: %#v",
			got,
			want,
			compiled.SQL,
			compiled.Args,
		)
	}
}

func TestCompileEventStatsSumKeepsScopePredicatesBelowInputFence(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(
		t,
		`index=gradethis source="eventstats-sum-fixture" host="web" | eventstats sum(eventstats_value) AS total | table event_id total`,
	)
	inputAlias := eventStatsPrivateAlias(
		t,
		compiled.SQL,
		"__os_eventstats_input_",
	)
	inputStart := strings.Index(
		compiled.SQL,
		inputAlias+` AS MATERIALIZED (`,
	)
	if inputStart < 0 {
		t.Fatalf("eventstats sum materialized input is missing:\n%s", compiled.SQL)
	}
	sentinel := `LIMIT ` + strconv.FormatUint(MaximumEventStatsInputRows+1, 10)
	limitOffset := strings.Index(compiled.SQL[inputStart:], sentinel)
	if limitOffset < 0 {
		t.Fatalf("eventstats sum input fence is missing:\n%s", compiled.SQL)
	}
	boundedInput := compiled.SQL[inputStart : inputStart+limitOffset]
	for _, predicate := range []string{
		`WHERE "tenant_id" = ? AND "index_name" IN (?)`,
		`"event_time" >= parseDateTime64BestEffort(?, 9, 'UTC')`,
		`"event_time" < parseDateTime64BestEffort(?, 9, 'UTC')`,
		`"index_time" <= parseDateTime64BestEffort(?, 3, 'UTC')`,
		`"expires_at" > parseDateTime64BestEffort(?, 3, 'UTC')`,
		`"visibility_seq" <= ?`,
		`lowerUTF8(toString("source")) = lowerUTF8(?)`,
		`lowerUTF8(toString("host")) = lowerUTF8(?)`,
	} {
		if !strings.Contains(boundedInput, predicate) {
			t.Fatalf(
				"eventstats sum scope predicate %q escaped the bounded input:\n%s",
				predicate,
				compiled.SQL,
			)
		}
	}
	limitAt := inputStart + limitOffset
	sumAt := strings.Index(compiled.SQL, `sumOrNullArray(`)
	if sumAt < limitAt {
		t.Fatalf("eventstats sum aggregate ran before its input fence:\n%s", compiled.SQL)
	}
}

func TestCompileEventStatsSumUsesOneGroupedAggregateAndLeftJoin(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(
		t,
		`index=gradethis | eventstats sum(eventstats_value) AS total BY eventstats_group | where total>30 | table event_id eventstats_group total`,
	)
	for _, required := range []string{
		`sumOrNullArray(`,
		`GROUP BY "__os_eventstats_group_0"`,
		`LEFT JOIN`,
		`CAST(NULL AS Nullable(Float64))`,
		`AS "total"`,
		`"__os_eventstats_exists_`,
		`toFloat64("total") > toFloat64(CAST(? AS Int64))`,
	} {
		if !strings.Contains(compiled.SQL, required) {
			t.Fatalf("grouped eventstats sum SQL missing %q:\n%s", required, compiled.SQL)
		}
	}
	if got := strings.Count(compiled.SQL, `sumOrNullArray(`); got != 1 {
		t.Fatalf("grouped eventstats sum aggregates = %d, want 1:\n%s", got, compiled.SQL)
	}
	if got := strings.Count(compiled.SQL, ` GROUP BY `); got != 1 {
		t.Fatalf("grouped eventstats aggregate passes = %d, want 1:\n%s", got, compiled.SQL)
	}
	if got := strings.Count(compiled.SQL, ` LEFT JOIN `); got != 1 {
		t.Fatalf("grouped eventstats left joins = %d, want 1:\n%s", got, compiled.SQL)
	}
	if strings.Contains(strings.ToUpper(compiled.SQL), "ARRAY JOIN") {
		t.Fatalf("grouped eventstats sum expanded multivalue rows:\n%s", compiled.SQL)
	}
	if got := strings.Count(compiled.SQL, `FROM "open_splunk"."events"`); got != 1 {
		t.Fatalf("grouped eventstats physical scan count = %d, want 1:\n%s", got, compiled.SQL)
	}
}

func TestCompileEventStatsSumTreatsProjectedInputAsEmptyNumericArray(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(
		t,
		`index=gradethis | fields event_id | eventstats sum(eventstats_value) AS total | table event_id total`,
	)
	if got := strings.Count(
		compiled.SQL,
		`CAST([], 'Array(Float64)')`,
	); got != 1 {
		t.Fatalf(
			"projected eventstats sum empty numeric inputs = %d, want 1:\n%s",
			got,
			compiled.SQL,
		)
	}
	if !strings.Contains(compiled.SQL, `sumOrNullArray(`) {
		t.Fatalf("projected eventstats sum lost its nullable aggregate:\n%s", compiled.SQL)
	}
	if strings.Contains(compiled.SQL, `"__os_fields"."eventstats_value"`) ||
		slices.Contains(compiled.Args, any("eventstats_value")) ||
		slices.Contains(compiled.Args, any("eventstats_value.")) {
		t.Fatalf(
			"projected-away eventstats sum input was rebound from storage:\nSQL: %s\nargs: %#v",
			compiled.SQL,
			compiled.Args,
		)
	}
}
