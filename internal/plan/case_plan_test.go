package plan

import (
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/spl"
)

func TestBuildEvalCasePreservesOrderedPredicateAndValueIR(t *testing.T) {
	t.Parallel()

	logical, err := Build(
		mustParse(
			t,
			`index=gradethis | eval label=case(status=200, replace(source, "api", "API"), status=404 OR isnull(status), coalesce(message, "missing"))`,
		),
		testScope([]string{"gradethis"}, nil),
	)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	extend := logical.Operators[len(logical.Operators)-1].(*Extend)
	conditional, ok := extend.Assignments[0].Expression.(*ScalarCaseExpression)
	if !ok || len(conditional.Branches) != 2 {
		t.Fatalf("case IR = %#v, want two ordered branches", extend.Assignments[0].Expression)
	}
	firstCondition := conditional.Branches[0].Condition.(*EvalComparisonExpression)
	if firstCondition.Left.(*ScalarFieldExpression).Field.Name != "status" ||
		firstCondition.Op != ComparisonOpEqual ||
		firstCondition.Right.(*ScalarLiteralExpression).Value.Int64 != 200 {
		t.Fatalf("first condition = %#v", firstCondition)
	}
	firstValue := conditional.Branches[0].Value.(*ScalarCallExpression)
	if firstValue.Function != ScalarFunctionReplace ||
		firstValue.Arguments[0].(*ScalarFieldExpression).Field.Name != "source" {
		t.Fatalf("first value = %#v", firstValue)
	}
	secondCondition := conditional.Branches[1].Condition.(*BooleanExpression)
	if secondCondition.Op != BooleanOpOr {
		t.Fatalf("second condition = %#v, want OR", secondCondition)
	}
	if secondCondition.Left.(*EvalComparisonExpression).
		Right.(*ScalarLiteralExpression).Value.Int64 != 404 {
		t.Fatalf("second condition left = %#v", secondCondition.Left)
	}
	secondPredicate := secondCondition.Right.(*ScalarPredicateExpression)
	if secondPredicate.Value.(*ScalarCallExpression).Function != ScalarFunctionIsNull {
		t.Fatalf("second condition right = %#v", secondCondition.Right)
	}
	secondValue := conditional.Branches[1].Value.(*ScalarCallExpression)
	if secondValue.Function != ScalarFunctionCoalesce {
		t.Fatalf("second value = %#v", secondValue)
	}
}

func TestBuildCaseBooleanResultCanBeConsumedAsPredicate(t *testing.T) {
	t.Parallel()

	logical, err := Build(
		mustParse(
			t,
			`index=gradethis | where case(isnull(first), false, isnotnull(second), true)`,
		),
		testScope([]string{"gradethis"}, nil),
	)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	filter := logical.Operators[len(logical.Operators)-1].(*Filter)
	predicate, ok := filter.Expression.(*ScalarPredicateExpression)
	if !ok {
		t.Fatalf("predicate = %T, want *ScalarPredicateExpression", filter.Expression)
	}
	conditional, ok := predicate.Value.(*ScalarCaseExpression)
	if !ok || len(conditional.Branches) != 2 {
		t.Fatalf("predicate scalar = %#v, want two-branch case", predicate.Value)
	}
}

func TestBuildEvalCaseRejectsForgedBranchesAndCycles(t *testing.T) {
	t.Parallel()

	sourceRange := spl.Range{
		Start: spl.Position{Line: 1, Column: 1},
		End:   spl.Position{Line: 1, Column: 5},
	}
	condition := func() spl.WhereExpr {
		return &spl.WhereComparisonExpr{
			Left: &spl.ScalarFieldExpr{
				Field: "status",
				Range: sourceRange,
			},
			Op: spl.CompareOpEqual,
			Right: &spl.ScalarLiteralExpr{
				Value: spl.Literal{
					Kind:  spl.LiteralKindInteger,
					Text:  "200",
					Range: sourceRange,
				},
				Range: sourceRange,
			},
			Range: sourceRange,
		}
	}
	value := func() spl.ScalarExpr {
		return &spl.ScalarLiteralExpr{
			Value: spl.Literal{
				Kind:   spl.LiteralKindString,
				Text:   "ok",
				Quoted: true,
				Range:  sourceRange,
			},
			Range: sourceRange,
		}
	}
	var typedNilCondition *spl.WhereComparisonExpr
	var typedNilValue *spl.ScalarLiteralExpr
	tooMany := make([]spl.ScalarCaseBranch, spl.MaximumCaseBranches+1)
	for index := range tooMany {
		tooMany[index] = spl.ScalarCaseBranch{
			Condition: condition(),
			Value:     value(),
			Range:     sourceRange,
		}
	}
	cyclic := &spl.ScalarCaseExpr{Range: sourceRange}
	cyclic.Branches = []spl.ScalarCaseBranch{{
		Condition: condition(),
		Value:     cyclic,
		Range:     sourceRange,
	}}

	for _, test := range []struct {
		name       string
		expression spl.ScalarExpr
		code       string
	}{
		{
			name:       "zero branches",
			expression: &spl.ScalarCaseExpr{Range: sourceRange},
			code:       "SPL_INVALID_EVAL_ARITY",
		},
		{
			name: "too many branches",
			expression: &spl.ScalarCaseExpr{
				Branches: tooMany,
				Range:    sourceRange,
			},
			code: "SPL_QUERY_TOO_COMPLEX",
		},
		{
			name: "nil condition",
			expression: &spl.ScalarCaseExpr{
				Branches: []spl.ScalarCaseBranch{{
					Value: value(),
					Range: sourceRange,
				}},
				Range: sourceRange,
			},
			code: "SPL_UNSUPPORTED_WHERE_EXPRESSION",
		},
		{
			name: "typed nil condition",
			expression: &spl.ScalarCaseExpr{
				Branches: []spl.ScalarCaseBranch{{
					Condition: typedNilCondition,
					Value:     value(),
					Range:     sourceRange,
				}},
				Range: sourceRange,
			},
			code: "SPL_UNSUPPORTED_WHERE_EXPRESSION",
		},
		{
			name: "nil value",
			expression: &spl.ScalarCaseExpr{
				Branches: []spl.ScalarCaseBranch{{
					Condition: condition(),
					Range:     sourceRange,
				}},
				Range: sourceRange,
			},
			code: "SPL_UNSUPPORTED_EVAL_EXPRESSION",
		},
		{
			name: "typed nil value",
			expression: &spl.ScalarCaseExpr{
				Branches: []spl.ScalarCaseBranch{{
					Condition: condition(),
					Value:     typedNilValue,
					Range:     sourceRange,
				}},
				Range: sourceRange,
			},
			code: "SPL_UNSUPPORTED_EVAL_EXPRESSION",
		},
		{
			name:       "cycle",
			expression: cyclic,
			code:       "SPL_QUERY_TOO_COMPLEX",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			query := mustParse(t, `index=gradethis | eval selected="ok"`)
			query.Commands[0].(*spl.EvalCommand).
				Assignments[0].Expression = test.expression
			_, err := Build(query, testScope([]string{"gradethis"}, nil))
			assertDiagnosticCode(t, err, test.code)
		})
	}
}
