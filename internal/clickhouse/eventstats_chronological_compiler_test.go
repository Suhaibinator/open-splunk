package clickhouse

import (
	"errors"
	"slices"
	"strings"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/plan"
)

func TestCompileEventStatsChronologicalUsesImmutableOrderAndBoundedCandidates(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(
		t,
		`index=gradethis | sort 0 +event_id`+
			` | eventstats earliest(payload) AS first_seen BY service`+
			` | eventstats latest(payload) AS last_seen`+
			` | table event_id first_seen last_seen`,
	)
	if !slices.Equal(compiled.OutputFields, []string{"event_id", "first_seen", "last_seen"}) {
		t.Fatalf("eventstats chronological output fields = %#v", compiled.OutputFields)
	}
	for _, function := range []string{"argMinOrNullIf(", "argMaxOrNullIf("} {
		call := chronologicalAggregateCall(t, compiled.SQL, function)
		if !strings.Contains(call, `tupleElement("__os_eventstats_measure_`) {
			t.Fatalf("%s does not consume its materialized bounded measure:\n%s", function, call)
		}
		if strings.Contains(call, `"__os_order_`) {
			t.Fatalf("%s depends on user-visible pipeline order:\n%s", function, call)
		}
	}
	for _, identity := range []string{
		`"__os_sort_time"`,
		`"__os_sort_event_id"`,
		`"__os_sort_visibility_seq"`,
		`"__os_sort_source_identity"`,
	} {
		if !strings.Contains(compiled.SQL, identity) {
			t.Fatalf("eventstats chronology is missing immutable identity %s:\n%s", identity, compiled.SQL)
		}
	}
	for _, selector := range []string{
		"arrayFirstIndex(", "arrayLastIndex(", "arrayExists(",
	} {
		if !strings.Contains(compiled.SQL, selector) {
			t.Fatalf("eventstats chronological Dynamic input is missing %q:\n%s", selector, compiled.SQL)
		}
	}
	upperSQL := strings.ToUpper(compiled.SQL)
	for _, forbidden := range []string{
		"ARRAY JOIN",
		"ARRAYJOIN(",
		"GROUPARRAY",
		"ARRAYFOLD(",
		" OVER (",
	} {
		if strings.Contains(upperSQL, forbidden) {
			t.Fatalf("eventstats chronological lowering retained %q:\n%s", forbidden, compiled.SQL)
		}
	}
	if got := strings.Count(compiled.SQL, `FROM "open_splunk"."events"`); got != 1 {
		t.Fatalf("eventstats chronological physical scans = %d, want 1:\n%s", got, compiled.SQL)
	}
	if got, want := strings.Count(compiled.SQL, "?"), len(compiled.Args); got != want {
		t.Fatalf("placeholder count = %d, args = %d\nSQL: %s\nargs: %#v", got, want, compiled.SQL, compiled.Args)
	}
}

func TestCompileEventStatsChronologicalOnlySelectsRequestedDirection(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name      string
		function  string
		required  []string
		forbidden []string
	}{
		{
			name: "earliest", function: "earliest",
			required: []string{"arrayFirstIndex(", "argMinOrNullIf("},
			forbidden: []string{
				"arrayFirst(", "arrayLast(", "arrayLastIndex(", "arrayCount(", "argMaxOrNullIf(",
			},
		},
		{
			name: "latest", function: "latest",
			required: []string{"arrayLastIndex(", "argMaxOrNullIf("},
			forbidden: []string{
				"arrayLast(", "arrayFirst(", "arrayFirstIndex(", "arrayCount(", "argMinOrNullIf(",
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			compiled := compileSPL(
				t,
				`index=gradethis | eventstats `+test.function+
					`(payload) AS selected | table selected`,
			)
			for _, required := range test.required {
				if !strings.Contains(compiled.SQL, required) {
					t.Fatalf("%s lowering is missing %q:\n%s", test.name, required, compiled.SQL)
				}
			}
			for _, forbidden := range test.forbidden {
				if strings.Contains(compiled.SQL, forbidden) {
					t.Fatalf("%s lowering retained unused %q:\n%s", test.name, forbidden, compiled.SQL)
				}
			}
		})
	}
}

func TestCompileEventStatsChronologicalRetainsAtomicValidation(t *testing.T) {
	t.Parallel()

	for _, source := range []string{
		`index=gradethis | eventstats earliest(payload) AS discarded | table event_id`,
		`index=gradethis | eventstats latest(payload) AS discarded BY service` +
			` | search definitely_missing=value | table event_id`,
	} {
		compiled := compileSPL(t, source)
		for _, required := range []string{
			"AS MATERIALIZED",
			"UNION ALL",
			UnsupportedStatsMeasureValueMarker,
			`"__os_eventstats_validation_`,
		} {
			if !strings.Contains(compiled.SQL, required) {
				t.Fatalf("eventstats chronological validation is missing %q:\n%s", required, compiled.SQL)
			}
		}
		if strings.Contains(source, "definitely_missing") &&
			strings.LastIndex(compiled.SQL, "UNION ALL") < strings.LastIndex(compiled.SQL, "WHERE 0") {
			t.Fatalf("downstream empty result can prune chronological validation:\n%s", compiled.SQL)
		}
	}
}

func TestEventStatsChronologicalAggregateUsesMemberOrdinalTieBreak(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name      string
		function  plan.AggregateFunction
		aggregate string
	}{
		{
			name:      "earliest",
			function:  plan.AggregateFunctionEarliest,
			aggregate: "argMinOrNullIf(",
		},
		{
			name:      "latest",
			function:  plan.AggregateFunctionLatest,
			aggregate: "argMaxOrNullIf(",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			got, err := singleChronologicalAggregateSQL(test.function, `"input"`)
			if err != nil {
				t.Fatalf("singleChronologicalAggregateSQL: %v", err)
			}
			for _, required := range []string{
				test.aggregate,
				`tupleElement(tupleElement("input", 1), 1)`,
				`tupleElement("input", 2)`,
				`tupleElement(tupleElement("input", 1), 2)`,
				`tupleElement(tupleElement("input", 1), 3) != 0`,
			} {
				if !strings.Contains(got, required) {
					t.Fatalf("%s aggregate is missing %q: %s", test.name, required, got)
				}
			}
		})
	}

	if _, err := singleChronologicalAggregateSQL(
		plan.AggregateFunctionValues,
		`"input"`,
	); err == nil {
		t.Fatal("singleChronologicalAggregateSQL accepted a non-chronological function")
	}
}

func TestCompileEventStatsChronologicalRejectsForgedContracts(t *testing.T) {
	t.Parallel()

	base := buildPlan(t, `index=gradethis | eventstats earliest(payload) AS first_seen`)
	for _, test := range []struct {
		name   string
		mutate func(*plan.EventAggregate)
	}{
		{
			name: "predicate metadata",
			mutate: func(operator *plan.EventAggregate) {
				operator.Measure.Predicate = &plan.ComparisonExpression{}
			},
		},
		{
			name: "percentile metadata",
			mutate: func(operator *plan.EventAggregate) {
				operator.Measure.Percentile = 50
			},
		},
		{
			name: "unresolved input",
			mutate: func(operator *plan.EventAggregate) {
				operator.Measure.Input.Path = nil
			},
		},
		{
			name: "missing canonical time",
			mutate: func(operator *plan.EventAggregate) {
				operator.Measure.Function = plan.AggregateFunctionLatest
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			logical, operator := cloneEventAggregatePlan(t, base)
			test.mutate(operator)
			if test.name == "missing canonical time" {
				logical.Operators = append(
					[]plan.Operator{logical.Operators[0], &plan.Project{
						Mode:   plan.ProjectModeTable,
						Fields: []plan.FieldRef{operator.Measure.Input},
					}},
					logical.Operators[1:]...,
				)
			}
			_, err := (Compiler{}).Compile(logical)
			if err == nil {
				t.Fatal("Compile() accepted forged chronological eventstats plan")
			}
			if test.name == "missing canonical time" {
				var diagnostic *plan.Diagnostic
				if !errors.As(err, &diagnostic) || diagnostic.Code != "SPL_UNSUPPORTED_EVENTSTATS_TIME_FIELD" {
					t.Fatalf("missing canonical time error = %#v", err)
				}
			}
		})
	}
}
