package clickhouse

import (
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/plan"
)

func TestCompileEventStatsMaximumFoldsOneBoundedDynamicInputToScalarWinner(
	t *testing.T,
) {
	t.Parallel()

	compiled := compileSPL(
		t,
		`index=gradethis source="eventstats-min-fixture" | sort 0 +event_id | eventstats max(eventstats_min_value) AS high | where high="z" | table event_id high`,
	)
	if !slices.Equal(compiled.OutputFields, []string{"event_id", "high"}) {
		t.Fatalf("eventstats max output fields = %#v", compiled.OutputFields)
	}

	inputAlias := eventStatsPrivateAlias(t, compiled.SQL, "__os_eventstats_input_")
	measureAlias := eventStatsPrivateAlias(t, compiled.SQL, "__os_eventstats_measure_")
	publishedAlias := eventStatsPrivateAlias(
		t,
		compiled.SQL,
		"__os_eventstats_published_value_",
	)
	typeAlias := eventStatsPrivateAlias(t, compiled.SQL, "__os_eventstats_extrema_type_")
	sentinel := `LIMIT ` + strconv.FormatUint(MaximumEventStatsInputRows+1, 10)
	maximumFold := `tupleElement(__os_eventstats_extrema_candidate, 1) > ` +
		`tupleElement(__os_eventstats_extrema_state, 1)`
	for _, required := range []string{
		inputAlias + ` AS MATERIALIZED (`,
		sentinel,
		`dynamicElement("__os_fields"."eventstats_min_value", 'Array(Dynamic)')`,
		`arrayFold((__os_eventstats_extrema_state, element) ->`,
		maximumFold,
		`'decimal/v1'`,
		`argMaxOrNullIf(tuple(tupleElement(` + measureAlias + `, 2), tupleElement(` +
			measureAlias + `, 3), tupleElement(` + measureAlias + `, 4)), tupleElement(` +
			measureAlias + `, 1), tupleElement(` + measureAlias + `, 5) != 0)`,
		`maxOrDefault(toUInt8(tupleElement(` + measureAlias + `, 6)))`,
		` AS ` + publishedAlias,
		`.` + publishedAlias + ` AS "high"`,
		` AS ` + typeAlias,
		UnsupportedStatsMeasureValueMarker,
		EventStatsInputLimitMarker,
		materializedCTESettingsSQL,
	} {
		if !strings.Contains(compiled.SQL, required) {
			t.Fatalf("eventstats max SQL missing %q:\n%s", required, compiled.SQL)
		}
	}
	if got := strings.Count(compiled.SQL, inputAlias+` AS MATERIALIZED (`); got != 1 {
		t.Fatalf("bounded eventstats max input definitions = %d, want 1:\n%s", got, compiled.SQL)
	}
	if got := strings.Count(compiled.SQL, sentinel); got != 1 {
		t.Fatalf("eventstats max sentinel limits = %d, want 1:\n%s", got, compiled.SQL)
	}
	if got := strings.Count(compiled.SQL, `arrayFold(`); got != 1 {
		t.Fatalf("eventstats max Dynamic member folds = %d, want 1:\n%s", got, compiled.SQL)
	}
	if got := strings.Count(compiled.SQL, `argMaxOrNullIf(`); got != 1 {
		t.Fatalf("eventstats max aggregate count = %d, want 1:\n%s", got, compiled.SQL)
	}
	if got := strings.Count(compiled.SQL, `FROM "open_splunk"."events"`); got != 1 {
		t.Fatalf("eventstats max physical scan count = %d, want 1:\n%s", got, compiled.SQL)
	}
	for _, forbidden := range []string{
		"ARRAY JOIN",
		"arrayJoin(",
		"groupArray(",
		"groupUniqArray(",
		"argMaxArray(",
		"Array(Tuple(String, String))",
		"arrayFilter(element ->",
		"arrayExists(element ->",
	} {
		if strings.Contains(compiled.SQL, forbidden) {
			t.Fatalf("eventstats max SQL contains row-multiplying %q:\n%s", forbidden, compiled.SQL)
		}
	}
	if got, want := strings.Count(compiled.SQL, "?"), len(compiled.Args); got != want {
		t.Fatalf(
			"eventstats max placeholder count = %d, args = %d\nSQL: %s\nargs: %#v",
			got,
			want,
			compiled.SQL,
			compiled.Args,
		)
	}
}

func TestCompileEventStatsMaximumRejectsForgedPlanMetadata(t *testing.T) {
	t.Parallel()

	base := buildPlan(
		t,
		`index=gradethis | eventstats max(eventstats_value) AS high`,
	)
	tests := []struct {
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
				operator.Measure.Percentile = 99
			},
		},
		{
			name: "missing input",
			mutate: func(operator *plan.EventAggregate) {
				operator.Measure.Input = plan.FieldRef{}
			},
		},
		{
			name: "private output",
			mutate: func(operator *plan.EventAggregate) {
				operator.Measure.Output = "__os_eventstats_max_private"
			},
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			logical, operator := cloneEventAggregatePlan(t, base)
			test.mutate(operator)
			if _, err := (Compiler{}).Compile(logical); err == nil {
				t.Fatal("Compile() accepted forged eventstats max measure")
			}
		})
	}
}

func TestCompileEventStatsMaximumKeepsNativeFixedNumericAndTimeTypes(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name   string
		field  string
		output string
	}{
		{name: "UInt8 severity", field: "severity", output: "highest_severity"},
		{name: "DateTime64 time", field: "_time", output: "latest_time"},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			compiled := compileSPL(
				t,
				`index=gradethis | eventstats max(`+test.field+`) AS `+test.output+
					` | table event_id `+test.output,
			)
			if !strings.Contains(compiled.SQL, `maxIfOrNull(`) ||
				!strings.Contains(compiled.SQL, `"`+test.field+`"`) ||
				!strings.Contains(compiled.SQL, `AS "`+test.output+`"`) {
				t.Fatalf("fixed eventstats max lost native %s lowering:\n%s", test.field, compiled.SQL)
			}
			for _, forbidden := range []string{
				`toFloat64("` + test.field + `")`,
				`argMaxArray(`,
				`argMaxOrNullIf(`,
			} {
				if strings.Contains(compiled.SQL, forbidden) {
					t.Fatalf("fixed eventstats max contains %q:\n%s", forbidden, compiled.SQL)
				}
			}
			if strings.Count(compiled.SQL, `maxIfOrNull(`) != 1 {
				t.Fatalf("fixed eventstats max aggregate count != 1:\n%s", compiled.SQL)
			}
		})
	}
}

func TestCompileEventStatsMaximumKeepsComputedBooleanType(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(
		t,
		`index=gradethis | eval selected=if(event_id="selected", true, false)`+
			` | eventstats max(selected) AS any_selected | table any_selected`,
	)
	for _, required := range []string{
		`maxIfOrNull(`,
		`AS "any_selected"`,
	} {
		if !strings.Contains(compiled.SQL, required) {
			t.Fatalf("fixed Bool eventstats max SQL missing %q:\n%s", required, compiled.SQL)
		}
	}
	for _, forbidden := range []string{
		`argMaxArray(`,
		`argMaxOrNullIf(`,
		`toFloat64("selected")`,
	} {
		if strings.Contains(compiled.SQL, forbidden) {
			t.Fatalf("fixed Bool eventstats max contains %q:\n%s", forbidden, compiled.SQL)
		}
	}
}

func TestCompileEventStatsMaximumKeepsFixedStringDirectionThroughMinimumStack(
	t *testing.T,
) {
	t.Parallel()

	compiled := compileSPL(
		t,
		`index=gradethis | eventstats max(event_id) AS highest_id | eventstats min(highest_id) AS repeated | table event_id highest_id repeated`,
	)
	if !slices.Equal(
		compiled.OutputFields,
		[]string{"event_id", "highest_id", "repeated"},
	) {
		t.Fatalf("fixed String max/min output fields = %#v", compiled.OutputFields)
	}
	for _, required := range []string{
		`CAST(toString("event_id") AS Nullable(String))`,
		`argMaxOrNullIf(`,
		`argMinOrNullIf(`,
		`AS "highest_id"`,
		`AS "repeated"`,
	} {
		if !strings.Contains(compiled.SQL, required) {
			t.Fatalf("fixed String eventstats max/min SQL missing %q:\n%s", required, compiled.SQL)
		}
	}
	for _, forbidden := range []string{
		`argMaxArray(`,
		`Array(Tuple(String, String))`,
		`toFloat64("event_id")`,
	} {
		if strings.Contains(compiled.SQL, forbidden) {
			t.Fatalf("fixed String eventstats max retained %q:\n%s", forbidden, compiled.SQL)
		}
	}
}

func TestCompileEventStatsMaximumFoldsFixedMultivalueWithoutRowExpansion(
	t *testing.T,
) {
	t.Parallel()

	compiled := compileSPL(
		t,
		`index=gradethis | stats values(event_id) AS event_ids`+
			` | eventstats max(event_ids) AS highest_id | table highest_id`,
	)
	if !slices.Equal(compiled.OutputFields, []string{"highest_id"}) {
		t.Fatalf("fixed multivalue eventstats max output fields = %#v", compiled.OutputFields)
	}
	if !strings.Contains(compiled.SQL, `argMaxArray(`) {
		t.Fatalf("fixed multivalue eventstats max lost maximum direction:\n%s", compiled.SQL)
	}
	if got := strings.Count(compiled.SQL, `argMaxArray(`); got != 1 {
		t.Fatalf("fixed multivalue eventstats max aggregates = %d, want 1:\n%s", got, compiled.SQL)
	}
	for _, forbidden := range []string{
		`argMinArray(`,
		`ARRAY JOIN`,
		`arrayJoin(`,
		`groupArray(`,
	} {
		if strings.Contains(compiled.SQL, forbidden) {
			t.Fatalf("fixed multivalue eventstats max contains %q:\n%s", forbidden, compiled.SQL)
		}
	}
}

func TestCompileEventStatsMaximumComposesAfterMinimumWithGroupedPresence(
	t *testing.T,
) {
	t.Parallel()

	compiled := compileSPL(
		t,
		`index=gradethis | eventstats min(eventstats_value) AS low BY eventstats_group | eventstats max(eventstats_value) AS high BY eventstats_group | where high="z" | table event_id low high`,
	)
	if !slices.Equal(compiled.OutputFields, []string{"event_id", "low", "high"}) {
		t.Fatalf("stacked min/max output fields = %#v", compiled.OutputFields)
	}
	for _, required := range []string{
		`argMinOrNullIf(`,
		`argMaxOrNullIf(`,
		`"__os_eventstats_exists_`,
		`LEFT JOIN`,
		`AS "low"`,
		`AS "high"`,
		UnsupportedStatsMeasureValueMarker,
		EventStatsInputLimitMarker,
		materializedCTESettingsSQL,
	} {
		if !strings.Contains(compiled.SQL, required) {
			t.Fatalf("stacked eventstats min/max SQL missing %q:\n%s", required, compiled.SQL)
		}
	}
	if got := strings.Count(compiled.SQL, ` LEFT JOIN `); got != 2 {
		t.Fatalf("stacked grouped eventstats joins = %d, want 2:\n%s", got, compiled.SQL)
	}
	if !slices.Contains(compiled.Args, any("z")) {
		t.Fatalf("stacked eventstats max lost downstream predicate args: %#v", compiled.Args)
	}
}

func TestCompileEventStatsMaximumCannotPruneUnsupportedContainerValidation(
	t *testing.T,
) {
	t.Parallel()

	compiled := compileSPL(
		t,
		`index=gradethis | eventstats max(payload) AS discarded | table event_id | search definitely_missing=value`,
	)
	if !slices.Equal(compiled.OutputFields, []string{"event_id"}) {
		t.Fatalf("discarded eventstats max output fields = %#v", compiled.OutputFields)
	}
	for _, required := range []string{
		UnsupportedStatsMeasureValueMarker,
		EventStatsInputLimitMarker,
		`WHERE 0`,
		`UNION ALL`,
		materializedCTESettingsSQL,
	} {
		if !strings.Contains(compiled.SQL, required) {
			t.Fatalf("eventstats max validation envelope missing %q:\n%s", required, compiled.SQL)
		}
	}
	if strings.LastIndex(compiled.SQL, `UNION ALL`) < strings.LastIndex(compiled.SQL, `WHERE 0`) {
		t.Fatalf("always-false downstream filter escaped final max validation:\n%s", compiled.SQL)
	}
	if got := strings.Count(compiled.SQL, `FROM "open_splunk"."events"`); got != 1 {
		t.Fatalf("eventstats max validation rescans events %d times, want 1:\n%s", got, compiled.SQL)
	}
}
