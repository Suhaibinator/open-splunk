package plan

import (
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/spl"
)

func TestBuildEvalCoalescePreservesOrderedScalarIR(t *testing.T) {
	t.Parallel()

	logical, err := Build(
		mustParse(
			t,
			`index=gradethis | eval selected=coalesce(null, source, "fallback")`,
		),
		testScope([]string{"gradethis"}, nil),
	)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	extend := logical.Operators[len(logical.Operators)-1].(*Extend)
	call, ok := extend.Assignments[0].Expression.(*ScalarCallExpression)
	if !ok || call.Function != ScalarFunctionCoalesce ||
		len(call.Arguments) != 3 {
		t.Fatalf("coalesce IR = %#v", extend.Assignments[0].Expression)
	}
	if literal, ok := call.Arguments[0].(*ScalarLiteralExpression); !ok ||
		literal.Value.Kind != ValueKindNull {
		t.Fatalf("first argument = %#v, want null", call.Arguments[0])
	}
	if field, ok := call.Arguments[1].(*ScalarFieldExpression); !ok ||
		field.Field.Name != "source" || !field.Field.Canonical {
		t.Fatalf("second argument = %#v, want canonical source", call.Arguments[1])
	}
	if literal, ok := call.Arguments[2].(*ScalarLiteralExpression); !ok ||
		literal.Value.Kind != ValueKindString ||
		literal.Value.String != "fallback" {
		t.Fatalf("third argument = %#v, want fallback", call.Arguments[2])
	}
}

func TestBuildCoalesceBooleanResultCanBeConsumedAsPredicate(t *testing.T) {
	t.Parallel()

	logical, err := Build(
		mustParse(
			t,
			`index=gradethis | where coalesce(isnull(first), false)`,
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
	call, ok := predicate.Value.(*ScalarCallExpression)
	if !ok || call.Function != ScalarFunctionCoalesce {
		t.Fatalf("predicate scalar = %#v, want coalesce", predicate.Value)
	}
}

func TestBuildEvalCoalesceRejectsForgedArityEnumAndArguments(t *testing.T) {
	t.Parallel()

	base := mustParse(t, `index=gradethis`)
	sourceRange := spl.Range{
		Start: spl.Position{Line: 1, Column: 1},
		End:   spl.Position{Line: 1, Column: 5},
	}
	stringArgument := func() spl.ScalarExpr {
		return &spl.ScalarLiteralExpr{
			Value: spl.Literal{
				Kind:  spl.LiteralKindString,
				Text:  "value",
				Range: sourceRange,
			},
			Range: sourceRange,
		}
	}
	var typedNil *spl.ScalarLiteralExpr
	tooMany := make([]spl.ScalarExpr, spl.MaximumCoalesceArguments+1)
	for index := range tooMany {
		tooMany[index] = stringArgument()
	}
	for _, test := range []struct {
		name       string
		expression spl.ScalarExpr
		code       string
	}{
		{
			name: "zero arguments",
			expression: &spl.ScalarCallExpr{
				Function: spl.ScalarFunctionCoalesce,
				Range:    sourceRange,
			},
			code: "SPL_INVALID_EVAL_ARITY",
		},
		{
			name: "too many arguments",
			expression: &spl.ScalarCallExpr{
				Function:  spl.ScalarFunctionCoalesce,
				Arguments: tooMany,
				Range:     sourceRange,
			},
			code: "SPL_QUERY_TOO_COMPLEX",
		},
		{
			name: "typed nil argument",
			expression: &spl.ScalarCallExpr{
				Function: spl.ScalarFunctionCoalesce,
				Arguments: []spl.ScalarExpr{
					stringArgument(),
					typedNil,
				},
				Range: sourceRange,
			},
			code: "SPL_UNSUPPORTED_EVAL_EXPRESSION",
		},
		{
			name: "invalid function enum",
			expression: &spl.ScalarCallExpr{
				Function:  spl.ScalarFunctionInvalid,
				Arguments: []spl.ScalarExpr{stringArgument()},
				Range:     sourceRange,
			},
			code: "SPL_UNSUPPORTED_EVAL_FUNCTION",
		},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			query := &spl.Query{
				Search: base.Search,
				Commands: []spl.Command{&spl.EvalCommand{
					Assignments: []spl.EvalAssignment{{
						Field:      "selected",
						FieldRange: sourceRange,
						Expression: test.expression,
						Range:      sourceRange,
					}},
					Range: sourceRange,
				}},
				Range: base.Range,
			}
			_, err := Build(query, testScope([]string{"gradethis"}, nil))
			if err == nil {
				t.Fatalf("Build succeeded for forged coalesce %#v", test.expression)
			}
			assertDiagnosticCode(t, err, test.code)
		})
	}
}
