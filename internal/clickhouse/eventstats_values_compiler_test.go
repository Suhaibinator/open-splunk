package clickhouse

import (
	"errors"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/plan"
)

func TestCompileEventStatsValuesAcceptsResolvedPlanWithoutParser(t *testing.T) {
	t.Parallel()

	logical, operator := cloneEventAggregatePlan(
		t,
		buildPlan(t, `index=gradethis | eventstats count(user) AS users`),
	)
	operator.Measure.Function = plan.AggregateFunctionValues

	compiled, err := (Compiler{}).Compile(logical)
	if err != nil {
		t.Fatalf("Compile(resolved eventstats values plan): %v", err)
	}
	sentinel := strconv.FormatUint(MaximumStatsValuesPerGroup+1, 10)
	if !strings.Contains(compiled.SQL, `groupUniqArrayArray(`+sentinel+`)(`) ||
		!strings.Contains(compiled.SQL, `arraySort(`) ||
		!strings.Contains(compiled.SQL, `AS "users"`) {
		t.Fatalf("resolved eventstats values plan was not lowered exactly:\n%s", compiled.SQL)
	}
}

func TestCompileEventStatsValuesUsesOneBoundedSortedExactSet(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name    string
		source  string
		grouped bool
	}{
		{
			name: "global",
			source: `index=gradethis source="eventstats-values-fixture"` +
				` | eventstats values(user) AS users | search users=* | table event_id users`,
		},
		{
			name: "grouped",
			source: `index=gradethis source="eventstats-values-fixture"` +
				` | eventstats values(user) AS users BY service region` +
				` | search users=* | table event_id users`,
			grouped: true,
		},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			compiled := compileSPL(t, test.source)
			if !slices.Equal(compiled.OutputFields, []string{"event_id", "users"}) {
				t.Fatalf("eventstats values output fields = %#v", compiled.OutputFields)
			}
			inputAlias := eventStatsPrivateAlias(
				t,
				compiled.SQL,
				"__os_eventstats_input_",
			)
			sentinel := strconv.FormatUint(MaximumStatsValuesPerGroup+1, 10)
			maximumValues := strconv.FormatUint(MaximumStatsValuesPerGroup, 10)
			maximumBytes := strconv.FormatUint(MaximumStatsValuesBytesPerGroup, 10)
			maximumResultValues := strconv.FormatUint(MaximumStatsValuesPerResult, 10)
			maximumResultBytes := strconv.FormatUint(MaximumStatsValuesBytesPerResult, 10)
			for _, required := range []string{
				inputAlias + ` AS MATERIALIZED (`,
				`LIMIT ` + strconv.FormatUint(MaximumEventStatsInputRows+1, 10),
				`dynamicElement("__os_fields"."user", 'Array(Dynamic)')`,
				`arrayMap(element ->`,
				`groupUniqArrayArray(` + sentinel + `)(`,
				`length(`,
				`> toUInt64(` + maximumValues + `)`,
				`arrayFold((bytes, value) -> bytes + toUInt128(length(value))`,
				`> toUInt128(` + maximumBytes + `)`,
				`arraySort(`,
				`sum(`,
				`OVER ()`,
				`> toUInt128(` + maximumResultValues + `)`,
				`> toUInt128(` + maximumResultBytes + `)`,
				`notEmpty("users")`,
				UnsupportedStatsMeasureValueMarker,
				EventStatsValuesLimitMarker,
				EventStatsValuesBytesLimitMarker,
				EventStatsInputLimitMarker,
				materializedCTESettingsSQL,
			} {
				if !strings.Contains(compiled.SQL, required) {
					t.Fatalf("eventstats values SQL missing %q:\n%s", required, compiled.SQL)
				}
			}
			if test.grouped {
				for _, required := range []string{
					`[toUInt8("__os_eventstats_eligible_`,
					`GROUP BY "__os_eventstats_group_0", "__os_eventstats_group_1"`,
					` LEFT JOIN `,
					`CAST([], 'Array(String)')`,
					`"__os_eventstats_exists_`,
				} {
					if !strings.Contains(compiled.SQL, required) {
						t.Fatalf("grouped eventstats values SQL missing %q:\n%s", required, compiled.SQL)
					}
				}
			}
			if got := strings.Count(
				compiled.SQL,
				`groupUniqArrayArray(`+sentinel+`)(`,
			); got != 1 {
				t.Fatalf("eventstats values exact states = %d, want 1:\n%s", got, compiled.SQL)
			}
			if got := strings.Count(compiled.SQL, `arraySort(`); got != 1 {
				t.Fatalf("eventstats values lexical sorts = %d, want 1:\n%s", got, compiled.SQL)
			}
			if got := strings.Count(
				compiled.SQL,
				`arrayFold((bytes, value) -> bytes + toUInt128(length(value))`,
			); got != 1 {
				t.Fatalf("eventstats values payload folds = %d, want 1:\n%s", got, compiled.SQL)
			}
			if validation, sort := strings.Index(
				compiled.SQL,
				EventStatsValuesLimitMarker,
			), strings.Index(compiled.SQL, `arraySort(`); validation < 0 || sort < validation {
				t.Fatalf("eventstats values sorted before validation:\n%s", compiled.SQL)
			}
			if got := strings.Count(compiled.SQL, `FROM "open_splunk"."events"`); got != 1 {
				t.Fatalf("eventstats values physical scans = %d, want 1:\n%s", got, compiled.SQL)
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
					t.Fatalf("eventstats values contains unbounded or row-multiplying %q:\n%s", forbidden, compiled.SQL)
				}
			}
			if got, want := strings.Count(compiled.SQL, "?"), len(compiled.Args); got != want {
				t.Fatalf(
					"eventstats values placeholders = %d, args = %d\nSQL: %s\nargs: %#v",
					got,
					want,
					compiled.SQL,
					compiled.Args,
				)
			}
		})
	}
}

func TestCompileEventStatsValuesValidationCannotBeHiddenDownstream(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(
		t,
		`index=gradethis | eventstats values(payload) AS discarded BY service`+
			` | head 1 | table event_id | search definitely_missing=value`,
	)
	if !slices.Equal(compiled.OutputFields, []string{"event_id"}) {
		t.Fatalf("discarded eventstats values output fields = %#v", compiled.OutputFields)
	}
	for _, required := range []string{
		UnsupportedStatsMeasureValueMarker,
		EventStatsValuesLimitMarker,
		EventStatsValuesBytesLimitMarker,
		EventStatsInputLimitMarker,
		`WHERE 0`,
		`UNION ALL`,
		`OVER ()`,
		materializedCTESettingsSQL,
	} {
		if !strings.Contains(compiled.SQL, required) {
			t.Fatalf("eventstats values validation envelope missing %q:\n%s", required, compiled.SQL)
		}
	}
	if strings.LastIndex(compiled.SQL, `UNION ALL`) <
		strings.LastIndex(compiled.SQL, `WHERE 0`) {
		t.Fatalf("downstream filter escaped final values validation:\n%s", compiled.SQL)
	}
	if got := strings.Count(compiled.SQL, `FROM "open_splunk"."events"`); got != 1 {
		t.Fatalf("values validation rescans events %d times, want 1:\n%s", got, compiled.SQL)
	}
}

func TestCompileEventStatsValuesProjectedReplacementAndArrayComposition(t *testing.T) {
	t.Parallel()

	projected := compileSPL(
		t,
		`index=gradethis | fields event_id | eventstats values(user) AS users`+
			` | search users=* | table event_id users`,
	)
	for _, required := range []string{
		`CAST([], 'Array(String)')`,
		`notEmpty("users")`,
		EventStatsValuesLimitMarker,
		EventStatsValuesBytesLimitMarker,
	} {
		if !strings.Contains(projected.SQL, required) {
			t.Fatalf("projected eventstats values SQL missing %q:\n%s", required, projected.SQL)
		}
	}
	if slices.Contains(projected.Args, any("user")) ||
		slices.Contains(projected.Args, any("user.")) {
		t.Fatalf(
			"projected eventstats values rebound private storage: %#v\n%s",
			projected.Args,
			projected.SQL,
		)
	}

	base := compileSPL(
		t,
		`index=gradethis | eventstats values(user) AS user | table event_id user`,
	)
	composed := compileSPL(
		t,
		`index=gradethis | eventstats values(user) AS user`+
			` | stats count(user) AS occurrences values(user) AS repeated`,
	)
	if !slices.Equal(composed.OutputFields, []string{"occurrences", "repeated"}) {
		t.Fatalf("composed eventstats values output = %#v", composed.OutputFields)
	}
	if strings.Count(composed.SQL, `groupUniqArrayArray(`) != 2 ||
		!strings.Contains(composed.SQL, `notEmpty("user")`) ||
		!strings.Contains(composed.SQL, `AS "repeated"`) {
		t.Fatalf("eventstats values result lost fixed multivalue composition:\n%s", composed.SQL)
	}
	for _, target := range []string{"user", "user."} {
		if got, want := countArgument(composed.Args, target), countArgument(base.Args, target); got != want {
			t.Fatalf(
				"downstream fixed array rebound dynamic %q: composed=%d base=%d\nargs: %#v",
				target,
				got,
				want,
				composed.Args,
			)
		}
	}
	if strings.Contains(composed.SQL, "ARRAY JOIN") ||
		strings.Contains(composed.SQL, "arrayJoin(") {
		t.Fatalf("composed eventstats values expanded rows:\n%s", composed.SQL)
	}
}

func TestCompileGroupedEventStatsValuesScopesPoisonToCompleteKeys(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(
		t,
		`index=gradethis | eventstats values(payload) AS payloads BY service region`+
			` | table event_id payloads`,
	)
	eligibleAlias := eventStatsPrivateAlias(
		t,
		compiled.SQL,
		"__os_eventstats_eligible_",
	)
	for _, required := range []string{
		`[toUInt8(` + eligibleAlias + ` != 0)]`,
		`row_eligible = 0, tuple(CAST([], 'Array(String)'), toUInt8(0))`,
		`descendant_present != 0, tuple(CAST([], 'Array(String)'), toUInt8(1))`,
		`arrayExists(member -> tupleElement(member, 2) != 0`,
		eligibleAlias + ` != 0) GROUP BY`,
		`if(` + `"__os_eventstats_rows_`,
		`CAST([], 'Array(String)')`,
		UnsupportedStatsMeasureValueMarker,
	} {
		if !strings.Contains(compiled.SQL, required) {
			t.Fatalf("grouped eventstats values SQL missing %q:\n%s", required, compiled.SQL)
		}
	}
}

func TestCompileEventStatsValuesRejectsForgedMeasureMetadata(t *testing.T) {
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
				operator.Measure.Output = "__os_eventstats_values_private"
			},
		},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			logical, operator := cloneEventAggregatePlan(t, base)
			operator.Measure.Function = plan.AggregateFunctionValues
			test.mutate(operator)
			if _, err := (Compiler{}).Compile(logical); err == nil {
				t.Fatal("Compile() accepted forged eventstats values measure")
			}
		})
	}
}

func TestCompileEventStatsValuesPreservesReservedFieldDiagnostic(t *testing.T) {
	t.Parallel()

	logical, operator := cloneEventAggregatePlan(
		t,
		buildPlan(t, `index=gradethis | eventstats count(user) AS users`),
	)
	operator.Measure.Function = plan.AggregateFunctionValues
	operator.Measure.Input.Name = "fields"
	operator.Measure.Input.Path = []string{"fields"}
	_, err := (Compiler{}).Compile(logical)
	var diagnostic *plan.Diagnostic
	if !errors.As(err, &diagnostic) ||
		diagnostic.Code != "SPL_AMBIGUOUS_EVENTSTATS_FIELD" {
		t.Fatalf(
			"reserved eventstats values error = %#v, want SPL_AMBIGUOUS_EVENTSTATS_FIELD",
			err,
		)
	}
}
