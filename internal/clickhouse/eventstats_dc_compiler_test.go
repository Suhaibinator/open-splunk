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

func TestCompileEventStatsDistinctCountAcceptsResolvedPlanWithoutParser(
	t *testing.T,
) {
	t.Parallel()

	logical, operator := cloneEventAggregatePlan(
		t,
		buildPlan(
			t,
			`index=gradethis | eventstats count(user) AS users`,
		),
	)
	operator.Measure.Function = plan.AggregateFunctionDistinctCount

	compiled, err := (Compiler{}).Compile(logical)
	if err != nil {
		t.Fatalf("Compile(resolved eventstats dc plan): %v", err)
	}
	sentinel := strconv.FormatUint(MaximumStatsDistinctValuesPerGroup+1, 10)
	if !strings.Contains(
		compiled.SQL,
		`groupUniqArrayArray(`+sentinel+`)(tupleElement(`,
	) || !strings.Contains(compiled.SQL, `AS "users"`) {
		t.Fatalf("resolved eventstats dc plan was not lowered exactly:\n%s", compiled.SQL)
	}
}

func TestCompileEventStatsDistinctCountRejectsForgedMeasureMetadata(t *testing.T) {
	t.Parallel()

	base := buildPlan(
		t,
		`index=gradethis | eventstats count(user) AS users`,
	)
	for _, test := range []struct {
		name   string
		mutate func(*plan.EventAggregate)
	}{
		{
			name: "predicate",
			mutate: func(operator *plan.EventAggregate) {
				operator.Measure.Predicate = &plan.ComparisonExpression{}
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
				operator.Measure.Input = plan.FieldRef{Name: "user"}
			},
		},
		{
			name: "private output",
			mutate: func(operator *plan.EventAggregate) {
				operator.Measure.Output = "__os_eventstats_dc_private"
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			logical, operator := cloneEventAggregatePlan(t, base)
			operator.Measure.Function = plan.AggregateFunctionDistinctCount
			test.mutate(operator)
			if _, err := (Compiler{}).Compile(logical); err == nil {
				t.Fatal("Compile() accepted forged eventstats dc measure")
			}
		})
	}
}

func TestCompileEventStatsDistinctCountUsesOneExactBoundedDynamicSet(
	t *testing.T,
) {
	t.Parallel()

	compiled := compileSPL(
		t,
		`index=gradethis source="eventstats-dc-fixture" | eventstats dc(user) AS users | where users>1 | table event_id users`,
	)
	if !slices.Equal(compiled.OutputFields, []string{"event_id", "users"}) {
		t.Fatalf("eventstats dc output fields = %#v", compiled.OutputFields)
	}

	inputAlias := eventStatsPrivateAlias(
		t,
		compiled.SQL,
		"__os_eventstats_input_",
	)
	valueAlias := eventStatsPrivateAlias(
		t,
		compiled.SQL,
		"__os_eventstats_value_dc_",
	)
	validationAlias := eventStatsPrivateAlias(
		t,
		compiled.SQL,
		"__os_eventstats_validation_",
	)
	sentinelRows := `LIMIT ` + strconv.FormatUint(MaximumEventStatsInputRows+1, 10)
	distinctSentinel := strconv.FormatUint(
		MaximumStatsDistinctValuesPerGroup+1,
		10,
	)
	maximumDistinct := strconv.FormatUint(
		MaximumStatsDistinctValuesPerGroup,
		10,
	)
	for _, required := range []string{
		inputAlias + ` AS MATERIALIZED (`,
		sentinelRows,
		`dynamicElement("__os_fields"."user", 'Array(Dynamic)')`,
		`arrayMap(element ->`,
		`dynamicType(element) != 'None' AND NOT (`,
		`arrayMap((row_eligible, field_present, descendant_present) ->`,
		`groupUniqArrayArray(` + distinctSentinel + `)(tupleElement(`,
		valueAlias + ` > toUInt64(` + maximumDistinct + `)`,
		` AS ` + validationAlias,
		validationAlias + ` != 0`,
		UnsupportedStatsMeasureValueMarker,
		ExactDistinctLimitMarker,
		`toInt256("users") > accurateCastOrNull(CAST(? AS Int64), 'Int256')`,
		EventStatsInputLimitMarker,
		`UNION ALL`,
		materializedCTESettingsSQL,
	} {
		if !strings.Contains(compiled.SQL, required) {
			t.Fatalf("eventstats dc SQL missing %q:\n%s", required, compiled.SQL)
		}
	}
	if got := strings.Count(
		compiled.SQL,
		`groupUniqArrayArray(`+distinctSentinel+`)`,
	); got != 1 {
		t.Fatalf("eventstats dc exact aggregate count = %d, want 1:\n%s", got, compiled.SQL)
	}
	if got := strings.Count(compiled.SQL, `FROM "open_splunk"."events"`); got != 1 {
		t.Fatalf("eventstats dc physical scan count = %d, want 1:\n%s", got, compiled.SQL)
	}
	for _, forbidden := range []string{
		"ARRAY JOIN",
		"arrayJoin(",
		"groupArray(",
		"uniq(",
		"uniqExact(",
		"uniqCombined(",
	} {
		if strings.Contains(compiled.SQL, forbidden) {
			t.Fatalf("eventstats dc contains row-multiplying %q:\n%s", forbidden, compiled.SQL)
		}
	}
	if got, want := strings.Count(compiled.SQL, "?"), len(compiled.Args); got != want {
		t.Fatalf(
			"eventstats dc placeholder count = %d, args = %d\nSQL: %s\nargs: %#v",
			got,
			want,
			compiled.SQL,
			compiled.Args,
		)
	}
}

func TestCompileGroupedEventStatsDistinctCountScopesValueValidationToCompleteKeys(
	t *testing.T,
) {
	t.Parallel()

	compiled := compileSPL(
		t,
		`index=gradethis | eventstats dc(user) AS users BY service region | search users=* | table event_id users`,
	)
	eligibleAlias := eventStatsPrivateAlias(
		t,
		compiled.SQL,
		"__os_eventstats_eligible_",
	)
	validationAlias := eventStatsPrivateAlias(
		t,
		compiled.SQL,
		"__os_eventstats_validation_",
	)
	for _, required := range []string{
		`toUInt8(`,
		`AS ` + eligibleAlias,
		`[toUInt8(` + eligibleAlias + ` != 0)]`,
		`row_eligible = 0, tuple(CAST([], 'Array(String)'), toUInt8(0))`,
		`descendant_present != 0, tuple(CAST([], 'Array(String)'), toUInt8(1))`,
		`arrayExists(member -> tupleElement(member, 2) != 0`,
		eligibleAlias + ` != 0) GROUP BY`,
		`GROUP BY "__os_eventstats_group_0", "__os_eventstats_group_1"`,
		` LEFT JOIN `,
		`if("__os_eventstats_rows_`,
		`CAST(NULL AS Nullable(UInt64))`,
		` AS ` + validationAlias,
		UnsupportedStatsMeasureValueMarker,
		ExactDistinctLimitMarker,
	} {
		if !strings.Contains(compiled.SQL, required) {
			t.Fatalf("grouped eventstats dc SQL missing %q:\n%s", required, compiled.SQL)
		}
	}
	inputDefinition := strings.Index(
		compiled.SQL,
		eventStatsPrivateAlias(t, compiled.SQL, "__os_eventstats_input_")+` AS MATERIALIZED (`,
	)
	aggregate := strings.Index(compiled.SQL, `groupUniqArrayArray(`)
	if inputDefinition < 0 || aggregate < inputDefinition {
		t.Fatalf("grouped eventstats dc aggregate escaped its bounded input:\n%s", compiled.SQL)
	}
	boundedInput := compiled.SQL[inputDefinition:aggregate]
	if strings.Contains(boundedInput, UnsupportedStatsMeasureValueMarker) ||
		strings.Contains(boundedInput, ExactDistinctLimitMarker) {
		t.Fatalf("grouped dc throws during row normalization instead of validation:\n%s", compiled.SQL)
	}
}

func TestCompileEventStatsDistinctCountProjectedInputPublishesUInt64Zero(
	t *testing.T,
) {
	t.Parallel()

	compiled := compileSPL(
		t,
		`index=gradethis | fields event_id | eventstats dc(user) AS users | where users=0 | table event_id users`,
	)
	distinctSentinel := strconv.FormatUint(
		MaximumStatsDistinctValuesPerGroup+1,
		10,
	)
	for _, required := range []string{
		`tuple(CAST([], 'Array(String)'), toUInt8(0))`,
		`groupUniqArrayArray(` + distinctSentinel + `)(tupleElement(`,
		`toInt256("users") = accurateCastOrNull(CAST(? AS Int64), 'Int256')`,
	} {
		if !strings.Contains(compiled.SQL, required) {
			t.Fatalf("projected eventstats dc SQL missing %q:\n%s", required, compiled.SQL)
		}
	}
	if slices.Contains(compiled.Args, any("user")) ||
		slices.Contains(compiled.Args, any("user.")) {
		t.Fatalf(
			"projected eventstats dc rebound private storage: %#v\n%s",
			compiled.Args,
			compiled.SQL,
		)
	}
}

func TestCompileEventStatsDistinctCountValidationCannotBeHiddenDownstream(
	t *testing.T,
) {
	t.Parallel()

	compiled := compileSPL(
		t,
		`index=gradethis | eventstats dc(payload) AS discarded | head 1 | table event_id | search definitely_missing=value`,
	)
	if !slices.Equal(compiled.OutputFields, []string{"event_id"}) {
		t.Fatalf("discarded eventstats dc output fields = %#v", compiled.OutputFields)
	}
	for _, required := range []string{
		UnsupportedStatsMeasureValueMarker,
		ExactDistinctLimitMarker,
		EventStatsInputLimitMarker,
		`WHERE 0`,
		`UNION ALL`,
		materializedCTESettingsSQL,
	} {
		if !strings.Contains(compiled.SQL, required) {
			t.Fatalf("eventstats dc validation envelope missing %q:\n%s", required, compiled.SQL)
		}
	}
	if strings.LastIndex(compiled.SQL, `UNION ALL`) <
		strings.LastIndex(compiled.SQL, `WHERE 0`) {
		t.Fatalf("downstream filter escaped final dc validation:\n%s", compiled.SQL)
	}
	if got := strings.Count(compiled.SQL, `FROM "open_splunk"."events"`); got != 1 {
		t.Fatalf("dc validation rescans events %d times, want 1:\n%s", got, compiled.SQL)
	}
}

func TestCompileStackedEventStatsDistinctCountKeepsFlatDeferredGraph(
	t *testing.T,
) {
	t.Parallel()

	compiled := compileSPL(
		t,
		`index=gradethis | eventstats dc(first_value) AS distinct_first | eventstats min(second_value) AS lowest_second | table event_id distinct_first lowest_second`,
	)
	distinctCountDefinitions := regexp.MustCompile(
		`"__os_eventstats_input_[0-9]+" AS (?:MATERIALIZED )?\(`,
	).FindAllString(compiled.SQL, -1)
	extremaDefinitions := regexp.MustCompile(
		`"__os_eventstats_result_input_[0-9]+" AS (?:MATERIALIZED )?\(`,
	).FindAllString(compiled.SQL, -1)
	if len(distinctCountDefinitions) != 1 ||
		!strings.Contains(distinctCountDefinitions[0], " AS MATERIALIZED (") ||
		len(extremaDefinitions) != 1 ||
		strings.Contains(extremaDefinitions[0], " AS MATERIALIZED (") {
		t.Fatalf(
			"stacked eventstats definitions = dc:%#v extrema:%#v\n%s",
			distinctCountDefinitions,
			extremaDefinitions,
			compiled.SQL,
		)
	}
	if got := strings.Count(compiled.SQL, ` AS MATERIALIZED (`); got != 1 {
		t.Fatalf("stacked eventstats materialized CTEs = %d, want 1:\n%s", got, compiled.SQL)
	}
	validationSource := ` AS "__os_chronological_invalid" FROM "__os_eventstats_result_`
	if got := strings.Count(compiled.SQL, validationSource); got != 2 ||
		!strings.Contains(compiled.SQL, ` UNION ALL SELECT `) {
		t.Fatalf(
			"stacked eventstats deferred validation sources = %d, want two flat UNION branches:\n%s",
			got,
			compiled.SQL,
		)
	}
	if got := strings.Count(compiled.SQL, `FROM "open_splunk"."events"`); got != 1 {
		t.Fatalf("stacked eventstats physical scans = %d, want 1:\n%s", got, compiled.SQL)
	}
	firstAt := slices.Index(compiled.Args, any("first_value"))
	secondAt := slices.Index(compiled.Args, any("second_value"))
	if firstAt < 0 || secondAt <= firstAt {
		t.Fatalf("stacked eventstats args are out of order: %#v", compiled.Args)
	}
	if got, want := strings.Count(compiled.SQL, "?"), len(compiled.Args); got != want {
		t.Fatalf("placeholder count = %d, args = %d", got, want)
	}
}

func TestCompileEventStatsDistinctCountPreservesReservedFieldDiagnostic(
	t *testing.T,
) {
	t.Parallel()

	logical, operator := cloneEventAggregatePlan(
		t,
		buildPlan(
			t,
			`index=gradethis | eventstats count(user) AS users`,
		),
	)
	operator.Measure.Function = plan.AggregateFunctionDistinctCount
	operator.Measure.Input.Name = "fields"
	operator.Measure.Input.Path = []string{"fields"}
	_, err := (Compiler{}).Compile(logical)
	var diagnostic *plan.Diagnostic
	if !errors.As(err, &diagnostic) ||
		diagnostic.Code != "SPL_AMBIGUOUS_EVENTSTATS_FIELD" {
		t.Fatalf(
			"reserved eventstats dc error = %#v, want SPL_AMBIGUOUS_EVENTSTATS_FIELD",
			err,
		)
	}
}
