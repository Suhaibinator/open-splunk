package clickhouse

import (
	"errors"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/plan"
)

func TestEventStatsListLimitMarkersRemainStable(t *testing.T) {
	t.Parallel()

	if EventStatsListLimitMarker !=
		"open-splunk: eventstats list exceeds the supported limit" ||
		EventStatsListBytesLimitMarker !=
			"open-splunk: eventstats list bytes exceed the supported limit" {
		t.Fatalf(
			"eventstats list limit markers = %q/%q",
			EventStatsListLimitMarker,
			EventStatsListBytesLimitMarker,
		)
	}
}

func TestCompileEventStatsListAcceptsResolvedPlanWithoutParser(t *testing.T) {
	t.Parallel()

	logical, operator := cloneEventAggregatePlan(
		t,
		buildPlan(t, `index=gradethis | eventstats count(user) AS users`),
	)
	operator.Measure.Function = plan.AggregateFunctionList

	compiled, err := (Compiler{}).Compile(logical)
	if err != nil {
		t.Fatalf("Compile(resolved eventstats list plan): %v", err)
	}
	maximum := strconv.FormatUint(MaximumStatsListValuesPerGroup, 10)
	for _, required := range []string{
		`groupArraySortedArray(` + maximum + `)(`,
		`arrayEnumerate(`,
		`arrayCumSum(`,
		`AS "users"`,
		`notEmpty("users")`,
		EventStatsListLimitMarker,
		EventStatsListBytesLimitMarker,
	} {
		if !strings.Contains(compiled.SQL, required) {
			t.Fatalf("resolved eventstats list SQL missing %q:\n%s", required, compiled.SQL)
		}
	}
}

func TestCompileEventStatsListRejectsInputWithoutDeterministicRowIdentity(t *testing.T) {
	t.Parallel()

	_, operator := cloneEventAggregatePlan(
		t,
		buildPlan(t, `index=gradethis | eventstats count(user) AS users`),
	)
	operator.Measure.Function = plan.AggregateFunctionList
	state := compileState{
		visible: map[string]fieldState{
			"user": {
				valueSQL:  `"user"`,
				existsSQL: "1",
				kind:      fieldKindString,
			},
		},
		blocked:         make(map[string]struct{}),
		blockedPrefixes: make(map[string]struct{}),
	}
	_, _, _, _, err := compileEventAggregate(
		compiledRelation{sql: `SELECT "user"`, depth: 1},
		operator,
		state,
		1,
	)
	if err == nil || !strings.Contains(err.Error(), "no deterministic row identity") {
		t.Fatalf(
			"compileEventAggregate error = %v, want deterministic-order rejection",
			err,
		)
	}
}

func TestCompileEventStatsListUsesOneBoundedOrderedState(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name      string
		source    string
		partition string
	}{
		{
			name: "global",
			source: `index=gradethis | sort 0 +sequence` +
				` | eventstats list(user) AS users | table event_id users`,
		},
		{
			name: "grouped",
			source: `index=gradethis | sort 0 +sequence` +
				` | eventstats list(user) AS users BY service region` +
				` | table event_id users`,
			partition: `PARTITION BY "__os_eventstats_eligible_3", ` +
				`"__os_eventstats_group_0", ` +
				`"__os_eventstats_group_1" `,
		},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			compiled := compileSPL(t, test.source)
			if !slices.Equal(compiled.OutputFields, []string{"event_id", "users"}) {
				t.Fatalf("eventstats list output fields = %#v", compiled.OutputFields)
			}
			measureAlias := eventStatsPrivateAlias(
				t,
				compiled.SQL,
				"__os_eventstats_measure_",
			)
			inputAlias := eventStatsPrivateAlias(
				t,
				compiled.SQL,
				"__os_eventstats_input_",
			)
			windowAlias := eventStatsPrivateAlias(
				t,
				compiled.SQL,
				"__os_eventstats_list_window_",
			)
			candidateAlias := eventStatsPrivateAlias(
				t,
				compiled.SQL,
				"__os_eventstats_list_candidates_",
			)
			maximum := strconv.FormatUint(MaximumStatsListValuesPerGroup, 10)
			maximumBytes := strconv.FormatUint(MaximumStatsListBytesPerGroup, 10)
			maximumResultValues := strconv.FormatUint(MaximumStatsListValuesPerResult, 10)
			maximumResultBytes := strconv.FormatUint(MaximumStatsListBytesPerResult, 10)
			order := `row_number() OVER (` + test.partition +
				`ORDER BY "__os_order_2_0" ASC NULLS LAST, ` +
				`"__os_order_2_tie_0" DESC NULLS LAST, ` +
				`"__os_order_2_tie_1" DESC NULLS LAST, ` +
				`"__os_order_2_tie_2" DESC NULLS LAST)`
			for _, required := range []string{
				` AS MATERIALIZED (`,
				`LIMIT ` + strconv.FormatUint(MaximumEventStatsInputRows+1, 10),
				`<= ` + strconv.FormatUint(MaximumEventStatsInputRows, 10),
				order,
				`length(tupleElement(` + measureAlias + `, 1))`,
				`arraySlice(tupleElement(` + measureAlias + `, 1), 1, ` + maximum + `)`,
				`ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING`,
				`arraySlice(`,
				`toUInt128(` + maximum + `)`,
				`arrayEnumerate(__os_list_remaining_values)`,
				`arrayCumSum(`,
				`groupArraySortedArray(` + maximum + `)(`,
				`arrayMap(item -> tupleElement(item, 3),`,
				`> toUInt128(` + maximumBytes + `)`,
				`sum(toUInt128(`,
				`OVER ()`,
				`> toUInt128(` + maximumResultValues + `)`,
				`> toUInt128(` + maximumResultBytes + `)`,
				`notEmpty("users")`,
				UnsupportedStatsMeasureValueMarker,
				EventStatsListLimitMarker,
				EventStatsListBytesLimitMarker,
				EventStatsInputLimitMarker,
				materializedCTESettingsSQL,
			} {
				if !strings.Contains(compiled.SQL, required) {
					t.Fatalf("eventstats list SQL missing %q:\n%s", required, compiled.SQL)
				}
			}
			if strings.Contains(compiled.SQL, inputAlias+` AS MATERIALIZED (`) ||
				!strings.Contains(compiled.SQL, inputAlias+` AS (`) {
				t.Fatalf(
					"eventstats list raw input is not an ordinary CTE:\n%s",
					compiled.SQL,
				)
			}
			if strings.Contains(compiled.SQL, windowAlias+` AS MATERIALIZED (`) ||
				!strings.Contains(compiled.SQL, windowAlias+` AS (`) {
				t.Fatalf(
					"eventstats list window is not an ordinary CTE:\n%s",
					compiled.SQL,
				)
			}
			if got := strings.Count(
				compiled.SQL,
				candidateAlias+` AS MATERIALIZED (`,
			); got != 1 {
				t.Fatalf(
					"eventstats list candidate materializations = %d, want 1:\n%s",
					got,
					compiled.SQL,
				)
			}
			if got := strings.Count(compiled.SQL, ` AS MATERIALIZED (`); got != 1 {
				t.Fatalf(
					"standalone eventstats list materialized fences = %d, want 1:\n%s",
					got,
					compiled.SQL,
				)
			}
			if got := strings.Count(compiled.SQL, `row_number() OVER (`); got != 1 {
				t.Fatalf("eventstats list row-order windows = %d, want 1:\n%s", got, compiled.SQL)
			}
			if got := strings.Count(
				compiled.SQL,
				`groupArraySortedArray(`+maximum+`)(`,
			); got != 1 {
				t.Fatalf("eventstats list ordered states = %d, want 1:\n%s", got, compiled.SQL)
			}
			for _, forbidden := range []string{
				"ARRAY JOIN",
				"arrayJoin(",
				"groupArray(",
				"groupUniqArray",
				"uniqExact(",
			} {
				if strings.Contains(compiled.SQL, forbidden) {
					t.Fatalf("eventstats list contains row-expanding or set operation %q:\n%s", forbidden, compiled.SQL)
				}
			}
			if got := strings.Count(compiled.SQL, `FROM "open_splunk"."events"`); got != 1 {
				t.Fatalf("eventstats list physical scans = %d, want 1:\n%s", got, compiled.SQL)
			}
		})
	}
}

func TestCompileEventStatsListStackKeepsOneEarliestMaterializedFence(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name                      string
		source                    string
		wantCandidateMaterialized bool
	}{
		{
			name: "list first",
			source: `index=gradethis | eventstats list(user) AS users` +
				` | eventstats count(users) AS occurrences | table users occurrences`,
			wantCandidateMaterialized: true,
		},
		{
			name: "list after count",
			source: `index=gradethis | eventstats count(user) AS occurrences` +
				` | eventstats list(occurrences) AS counts | table occurrences counts`,
		},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			compiled := compileSPL(t, test.source)
			candidateAlias := eventStatsPrivateAlias(
				t,
				compiled.SQL,
				"__os_eventstats_list_candidates_",
			)
			if got := strings.Count(compiled.SQL, ` AS MATERIALIZED (`); got != 1 {
				t.Fatalf(
					"stacked eventstats materialized fences = %d, want 1:\n%s",
					got,
					compiled.SQL,
				)
			}
			gotCandidateMaterialized := strings.Contains(
				compiled.SQL,
				candidateAlias+` AS MATERIALIZED (`,
			)
			if gotCandidateMaterialized != test.wantCandidateMaterialized {
				t.Fatalf(
					"stacked eventstats list candidate materialized = %t, want %t:\n%s",
					gotCandidateMaterialized,
					test.wantCandidateMaterialized,
					compiled.SQL,
				)
			}
		})
	}
}

func TestCompileEventStatsListUsesStableDefaultOrderAndAtomicValidation(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(
		t,
		`index=gradethis | eventstats list(payload) AS discarded BY service`+
			` | head 1 | table event_id | search definitely_missing=value`,
	)
	measureAlias := eventStatsPrivateAlias(
		t,
		compiled.SQL,
		"__os_eventstats_measure_",
	)
	for _, required := range []string{
		`row_number() OVER (PARTITION BY "__os_eventstats_eligible_2", ` +
			`"__os_eventstats_group_0" ORDER BY ` +
			`"__os_sort_time" DESC NULLS LAST, "__os_sort_event_id" DESC NULLS LAST, ` +
			`"__os_sort_visibility_seq" DESC NULLS LAST, ` +
			`"__os_sort_source_identity" DESC NULLS LAST)`,
		`maxOrDefault(toUInt8(tupleElement(` + measureAlias + `, 2)))`,
		UnsupportedStatsMeasureValueMarker,
		EventStatsListLimitMarker,
		EventStatsListBytesLimitMarker,
		EventStatsInputLimitMarker,
		`WHERE 0`,
		`UNION ALL`,
		`OVER ()`,
		materializedCTESettingsSQL,
	} {
		if !strings.Contains(compiled.SQL, required) {
			t.Fatalf("eventstats list validation SQL missing %q:\n%s", required, compiled.SQL)
		}
	}
	if strings.LastIndex(compiled.SQL, `UNION ALL`) <
		strings.LastIndex(compiled.SQL, `WHERE 0`) {
		t.Fatalf("downstream filter escaped final eventstats list validation:\n%s", compiled.SQL)
	}
	if strings.Contains(compiled.SQL, StatsListLimitMarker) ||
		strings.Contains(compiled.SQL, StatsListBytesLimitMarker) {
		t.Fatalf("eventstats list reused transforming-list markers:\n%s", compiled.SQL)
	}
	if got := strings.Count(compiled.SQL, `FROM "open_splunk"."events"`); got != 1 {
		t.Fatalf("eventstats list validation rescans events %d times, want 1:\n%s", got, compiled.SQL)
	}
}

func TestCompileEventStatsListProjectedInputIsAbsentAndComposesAsArray(t *testing.T) {
	t.Parallel()

	projected := compileSPL(
		t,
		`index=gradethis | fields event_id | eventstats list(user) AS users`+
			` | search users=* | table event_id users`,
	)
	for _, required := range []string{
		`CAST([], 'Array(String)')`,
		`AS "users"`,
		`notEmpty("users")`,
		EventStatsInputLimitMarker,
	} {
		if !strings.Contains(projected.SQL, required) {
			t.Fatalf("projected eventstats list SQL missing %q:\n%s", required, projected.SQL)
		}
	}
	for _, wasteful := range []string{
		`row_number() OVER`,
		`arrayCumSum(`,
		`groupArraySortedArray(`,
	} {
		if strings.Contains(projected.SQL, wasteful) {
			t.Fatalf("projected eventstats list retained ordered work %q:\n%s", wasteful, projected.SQL)
		}
	}
	if slices.Contains(projected.Args, any("user")) ||
		slices.Contains(projected.Args, any("user.")) {
		t.Fatalf("projected eventstats list rebound dynamic storage: %#v", projected.Args)
	}

	base := compileSPL(
		t,
		`index=gradethis | eventstats list(user) AS user | table event_id user`,
	)
	composed := compileSPL(
		t,
		`index=gradethis | eventstats list(user) AS user`+
			` | stats count(user) AS occurrences values(user) AS distinct_values`+
			` list(user) AS repeated`,
	)
	if !slices.Equal(
		composed.OutputFields,
		[]string{"occurrences", "distinct_values", "repeated"},
	) {
		t.Fatalf("composed eventstats list output = %#v", composed.OutputFields)
	}
	for _, required := range []string{
		`notEmpty("user")`,
		`AS "distinct_values"`,
		`AS "repeated"`,
	} {
		if !strings.Contains(composed.SQL, required) {
			t.Fatalf("eventstats list result lost array composition %q:\n%s", required, composed.SQL)
		}
	}
	for _, target := range []string{"user", "user."} {
		if got, want := countArgument(composed.Args, target), countArgument(base.Args, target); got != want {
			t.Fatalf(
				"downstream fixed list rebound dynamic %q: composed=%d base=%d\nargs: %#v",
				target,
				got,
				want,
				composed.Args,
			)
		}
	}
}

func TestCompileEventStatsListRejectsForgedMeasureMetadata(t *testing.T) {
	t.Parallel()

	base := buildPlan(t, `index=gradethis | eventstats count(user) AS users`)
	for _, test := range []struct {
		name   string
		mutate func(*plan.EventAggregate)
	}{
		{"predicate", func(operator *plan.EventAggregate) {
			operator.Measure.Predicate = &plan.ComparisonExpression{}
		}},
		{"percentile", func(operator *plan.EventAggregate) {
			operator.Measure.Percentile = 95
		}},
		{"missing input", func(operator *plan.EventAggregate) {
			operator.Measure.Input = plan.FieldRef{}
		}},
		{"malformed input", func(operator *plan.EventAggregate) {
			operator.Measure.Input = plan.FieldRef{Name: "user"}
		}},
		{"private output", func(operator *plan.EventAggregate) {
			operator.Measure.Output = "__os_eventstats_list_private"
		}},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			logical, operator := cloneEventAggregatePlan(t, base)
			operator.Measure.Function = plan.AggregateFunctionList
			test.mutate(operator)
			if _, err := (Compiler{}).Compile(logical); err == nil {
				t.Fatal("Compile() accepted forged eventstats list measure")
			}
		})
	}

	logical, operator := cloneEventAggregatePlan(t, base)
	operator.Measure.Function = plan.AggregateFunctionList
	operator.Measure.Input.Name = "fields"
	operator.Measure.Input.Path = []string{"fields"}
	_, err := (Compiler{}).Compile(logical)
	var diagnostic *plan.Diagnostic
	if !errors.As(err, &diagnostic) ||
		diagnostic.Code != "SPL_AMBIGUOUS_EVENTSTATS_FIELD" {
		t.Fatalf(
			"reserved eventstats list error = %#v, want SPL_AMBIGUOUS_EVENTSTATS_FIELD",
			err,
		)
	}
}
