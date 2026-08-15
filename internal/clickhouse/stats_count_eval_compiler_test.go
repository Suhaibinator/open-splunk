package clickhouse

import (
	"errors"
	"slices"
	"strings"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/plan"
	"github.com/Suhaibinator/open-splunk/internal/spl"
)

func TestCompileStatsCountEvalUsesSharedTrueOnlyContributions(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(
		t,
		`index=gradethis | stats count AS rows count(eval(source="first")) AS first count(eval(source="second")) AS second count(eval(source="first")) AS again`,
	)
	for _, required := range []string{
		`count() AS "rows"`,
		`toUInt64(ifNull(`,
		`AS "__os_measure_conditional_count_0"`,
		`AS "__os_measure_conditional_count_1"`,
		`toUInt64(sum(toUInt128("__os_measure_conditional_count_0"))) AS "first"`,
		`toUInt64(sum(toUInt128("__os_measure_conditional_count_1"))) AS "second"`,
		`toUInt64(sum(toUInt128("__os_measure_conditional_count_0"))) AS "again"`,
	} {
		if !strings.Contains(compiled.SQL, required) {
			t.Fatalf("conditional count SQL missing %q:\n%s", required, compiled.SQL)
		}
	}
	if got := strings.Count(compiled.SQL, ` AS "__os_measure_conditional_count_`); got != 2 {
		t.Fatalf("physical conditional contributions = %d, want 2:\n%s", got, compiled.SQL)
	}
	if got := strings.Count(compiled.SQL, " WHERE "); got != 2 {
		t.Fatalf("conditional measures introduced an aggregate prefilter; WHERE count = %d, want scan plus base-search filter:\n%s", got, compiled.SQL)
	}
	if strings.Contains(strings.ToUpper(compiled.SQL), "ARRAY JOIN") {
		t.Fatalf("ordinary conditional count introduced row expansion:\n%s", compiled.SQL)
	}
	wantPrefix := []any{"first", "second"}
	if len(compiled.Args) < len(wantPrefix) ||
		!slices.Equal(compiled.Args[:len(wantPrefix)], wantPrefix) {
		t.Fatalf("conditional bind prefix = %#v, want %#v\nSQL: %s", compiled.Args, wantPrefix, compiled.SQL)
	}
	if got, want := strings.Count(compiled.SQL, "?"), len(compiled.Args); got != want {
		t.Fatalf("placeholder count = %d, args = %d\nSQL: %s\nargs: %#v", got, want, compiled.SQL, compiled.Args)
	}
}

func TestCompileStatsCountEvalPreservesPredicateBindOrder(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(
		t,
		`index=gradethis | stats count(eval(source="first" AND sourcetype="second")) AS both count(eval(source="first" AND sourcetype="second")) AS again count(eval(source="third")) AS other`,
	)
	wantPrefix := []any{"first", "second", "third"}
	if len(compiled.Args) < len(wantPrefix) ||
		!slices.Equal(compiled.Args[:len(wantPrefix)], wantPrefix) {
		t.Fatalf("conditional bind prefix = %#v, want %#v\nSQL: %s", compiled.Args, wantPrefix, compiled.SQL)
	}
	if got := strings.Count(compiled.SQL, ` AS "__os_measure_conditional_count_`); got != 2 {
		t.Fatalf("physical conditional contributions = %d, want 2:\n%s", got, compiled.SQL)
	}
}

func TestCompileStatsCountEvalFencesCalculatedPredicateFieldsOnce(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(
		t,
		`index=gradethis | spath input=_raw output=selected path=value | stats count AS rows count(eval(selected="missing")) AS missing count(eval(isnotnull(selected))) AS selected`,
	)
	for _, required := range []string{
		`AS MATERIALIZED (`,
		`ARRAY JOIN`,
		`__os_stats_predicate_bound_`,
		`count() AS "rows"`,
		`AS "missing"`,
		`AS "selected"`,
	} {
		if !strings.Contains(compiled.SQL, required) {
			t.Fatalf("calculated conditional count SQL missing %q:\n%s", required, compiled.SQL)
		}
	}
	if got := strings.Count(strings.ToUpper(compiled.SQL), "ARRAY JOIN"); got != 1 {
		t.Fatalf("calculated predicate fences = %d, want 1:\n%s", got, compiled.SQL)
	}
	if got, want := strings.Count(compiled.SQL, "?"), len(compiled.Args); got != want {
		t.Fatalf("placeholder count = %d, args = %d\nSQL: %s\nargs: %#v", got, want, compiled.SQL, compiled.Args)
	}
}

func TestCompileStatsCountEvalTreatsProjectedFieldAsMissing(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(
		t,
		`index=gradethis | fields event_id | stats count AS rows count(eval(isnull(probe))) AS missing count(eval(isnotnull(probe))) AS present`,
	)
	if strings.Contains(compiled.SQL, `dynamicElement("fields", 'probe')`) {
		t.Fatalf("projected-away predicate resurrected probe from storage:\n%s", compiled.SQL)
	}
	for _, required := range []string{
		`count() AS "rows"`,
		`AS "missing"`,
		`AS "present"`,
	} {
		if !strings.Contains(compiled.SQL, required) {
			t.Fatalf("projected conditional count SQL missing %q:\n%s", required, compiled.SQL)
		}
	}
}

func TestCompileStatsCountEvalRejectsForgedPlans(t *testing.T) {
	t.Parallel()

	validPredicate := func() plan.Expression {
		return &plan.EvalComparisonExpression{
			Left: &plan.ScalarFieldExpression{
				Field: plan.FieldRef{Name: "source", Canonical: true},
			},
			Op: plan.ComparisonOpEqual,
			Right: &plan.ScalarLiteralExpression{
				Value: plan.Value{Kind: plan.ValueKindString, String: "api"},
			},
		}
	}
	field := plan.FieldRef{Name: "source", Canonical: true}
	var typedNil *plan.EvalComparisonExpression
	tests := []struct {
		name    string
		measure plan.AggregateMeasure
	}{
		{
			name: "missing predicate",
			measure: plan.AggregateMeasure{
				Function: plan.AggregateFunctionCountPredicate,
				Output:   "matches",
			},
		},
		{
			name: "typed nil predicate",
			measure: plan.AggregateMeasure{
				Function:  plan.AggregateFunctionCountPredicate,
				Predicate: typedNil,
				Output:    "matches",
			},
		},
		{
			name: "input and predicate",
			measure: plan.AggregateMeasure{
				Function:  plan.AggregateFunctionCountPredicate,
				Input:     field,
				Predicate: validPredicate(),
				Output:    "matches",
			},
		},
		{
			name: "percentile metadata",
			measure: plan.AggregateMeasure{
				Function:   plan.AggregateFunctionCountPredicate,
				Predicate:  validPredicate(),
				Percentile: 95,
				Output:     "matches",
			},
		},
		{
			name: "predicate on row count",
			measure: plan.AggregateMeasure{
				Function:  plan.AggregateFunctionCountRows,
				Predicate: validPredicate(),
				Output:    "matches",
			},
		},
		{
			name: "base search predicate",
			measure: plan.AggregateMeasure{
				Function:  plan.AggregateFunctionCountPredicate,
				Predicate: &plan.TextExpression{Value: "unsafe"},
				Output:    "matches",
			},
		},
		{
			name: "invalid Boolean operator",
			measure: plan.AggregateMeasure{
				Function: plan.AggregateFunctionCountPredicate,
				Predicate: &plan.BooleanExpression{
					Op:    plan.BooleanOpInvalid,
					Left:  validPredicate(),
					Right: validPredicate(),
				},
				Output: "matches",
			},
		},
		{
			name: "invalid comparison operator",
			measure: plan.AggregateMeasure{
				Function: plan.AggregateFunctionCountPredicate,
				Predicate: &plan.EvalComparisonExpression{
					Left: &plan.ScalarFieldExpression{Field: field},
					Op:   plan.ComparisonOpInvalid,
					Right: &plan.ScalarLiteralExpression{
						Value: plan.Value{Kind: plan.ValueKindString, String: "api"},
					},
				},
				Output: "matches",
			},
		},
		{
			name: "non Boolean scalar predicate",
			measure: plan.AggregateMeasure{
				Function: plan.AggregateFunctionCountPredicate,
				Predicate: &plan.ScalarPredicateExpression{
					Value: &plan.ScalarFieldExpression{Field: field},
				},
				Output: "matches",
			},
		},
		{
			name: "forged scalar field metadata",
			measure: plan.AggregateMeasure{
				Function: plan.AggregateFunctionCountPredicate,
				Predicate: &plan.EvalComparisonExpression{
					Left: &plan.ScalarFieldExpression{Field: plan.FieldRef{
						Name:      "source",
						Canonical: true,
						Path:      []string{"forged"},
					}},
					Op: plan.ComparisonOpEqual,
					Right: &plan.ScalarLiteralExpression{
						Value: plan.Value{Kind: plan.ValueKindString, String: "api"},
					},
				},
				Output: "matches",
			},
		},
		{
			name: "forged scalar function arity",
			measure: plan.AggregateMeasure{
				Function: plan.AggregateFunctionCountPredicate,
				Predicate: &plan.ScalarPredicateExpression{
					Value: &plan.ScalarCallExpression{
						Function: plan.ScalarFunctionIsNull,
					},
				},
				Output: "matches",
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			logical := buildPlan(t, `index=gradethis | stats count`)
			aggregate := logical.Operators[len(logical.Operators)-1].(*plan.Aggregate)
			aggregate.Measures = []plan.AggregateMeasure{test.measure}
			_, err := (Compiler{}).Compile(logical)
			if err == nil {
				t.Fatalf("Compile succeeded for forged measure %#v", test.measure)
			}
			if errors.Is(err, nil) {
				t.Fatalf("Compile returned a nil-equivalent error")
			}
		})
	}
}

func TestCompileStatsCountEvalRejectsForgedPredicateComplexity(t *testing.T) {
	t.Parallel()

	validLeaf := func() plan.Expression {
		return &plan.EvalComparisonExpression{
			Left: &plan.ScalarFieldExpression{
				Field: plan.FieldRef{Name: "source", Canonical: true},
			},
			Op: plan.ComparisonOpEqual,
			Right: &plan.ScalarLiteralExpression{
				Value: plan.Value{Kind: plan.ValueKindString, String: "api"},
			},
		}
	}
	cyclic := &plan.NotExpression{}
	cyclic.Operand = cyclic

	deep := validLeaf()
	for range maxCompiledPredicateDepth {
		deep = &plan.NotExpression{Operand: deep}
	}

	sharedDAG := validLeaf()
	for range 35 {
		sharedDAG = &plan.BooleanExpression{
			Op:    plan.BooleanOpAnd,
			Left:  sharedDAG,
			Right: sharedDAG,
		}
	}

	for _, test := range []struct {
		name      string
		predicate plan.Expression
	}{
		{name: "cycle", predicate: cyclic},
		{name: "depth", predicate: deep},
		{name: "shared DAG expansion", predicate: sharedDAG},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			logical := buildPlan(t, `index=gradethis | stats count`)
			aggregate := logical.Operators[len(logical.Operators)-1].(*plan.Aggregate)
			aggregate.Measures = []plan.AggregateMeasure{{
				Function:  plan.AggregateFunctionCountPredicate,
				Predicate: test.predicate,
				Output:    "matches",
			}}
			_, err := (Compiler{}).Compile(logical)
			var diagnostic *plan.Diagnostic
			if !errors.As(err, &diagnostic) ||
				diagnostic.Code != "SPL_QUERY_TOO_COMPLEX" {
				t.Fatalf("Compile complexity error = %#v, want SPL_QUERY_TOO_COMPLEX", err)
			}
		})
	}
}

func TestCompileStatsCountEvalChecksMeasureLimitBeforePredicateTraversal(t *testing.T) {
	t.Parallel()

	cyclic := &plan.NotExpression{}
	cyclic.Operand = cyclic
	measures := make([]plan.AggregateMeasure, spl.MaximumStatsMeasures+1)
	for index := range measures {
		measures[index] = plan.AggregateMeasure{
			Function:  plan.AggregateFunctionCountPredicate,
			Predicate: cyclic,
			Output:    "matches",
		}
	}

	logical := buildPlan(t, `index=gradethis | stats count`)
	aggregate := logical.Operators[len(logical.Operators)-1].(*plan.Aggregate)
	aggregate.Measures = measures
	_, err := (Compiler{}).Compile(logical)
	if err == nil ||
		!strings.Contains(err.Error(), "more than 16 measures") {
		t.Fatalf("Compile oversized conditional aggregate error = %#v, want early measure-limit rejection", err)
	}
}

func TestCompileStatsCountEvalPreservesReservedFieldsBoundary(t *testing.T) {
	t.Parallel()

	logical := buildPlan(t, `index=gradethis | stats count`)
	aggregate := logical.Operators[len(logical.Operators)-1].(*plan.Aggregate)
	fields, err := plan.ResolveField("fields", spl.Range{})
	if err != nil {
		t.Fatal(err)
	}
	aggregate.Measures = []plan.AggregateMeasure{{
		Function: plan.AggregateFunctionCountPredicate,
		Predicate: &plan.EvalComparisonExpression{
			Left: &plan.ScalarFieldExpression{Field: fields},
			Op:   plan.ComparisonOpEqual,
			Right: &plan.ScalarLiteralExpression{
				Value: plan.Value{Kind: plan.ValueKindString, String: "value"},
			},
		},
		Output: "matches",
	}}
	_, err = (Compiler{}).Compile(logical)
	var diagnostic *plan.Diagnostic
	if !errors.As(err, &diagnostic) ||
		diagnostic.Code != "SPL_AMBIGUOUS_STATS_FIELD" {
		t.Fatalf("open-schema fields error = %#v, want SPL_AMBIGUOUS_STATS_FIELD", err)
	}

	closed := compileSPL(
		t,
		`index=gradethis | stats count AS fields | stats count(eval(fields=1)) AS matches`,
	)
	if !slices.Equal(closed.OutputFields, []string{"matches"}) {
		t.Fatalf("closed-schema output fields = %#v, want matches", closed.OutputFields)
	}
}
