package plan

import (
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/spl"
)

func TestBuildEvalLowerUpperPreservesTypedScalarIR(t *testing.T) {
	t.Parallel()

	logical, err := Build(
		mustParse(
			t,
			`index=gradethis | eval folded=lower("MÜNCHEN"), shouted=upper(source)`,
		),
		testScope([]string{"gradethis"}, nil),
	)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	extend := logical.Operators[len(logical.Operators)-1].(*Extend)
	if len(extend.Assignments) != 2 {
		t.Fatalf("assignments = %#v", extend.Assignments)
	}

	lower, ok := extend.Assignments[0].Expression.(*ScalarCallExpression)
	if !ok || lower.Function != ScalarFunctionLower ||
		len(lower.Arguments) != 1 {
		t.Fatalf("lower IR = %#v", extend.Assignments[0].Expression)
	}
	literal, ok := lower.Arguments[0].(*ScalarLiteralExpression)
	if !ok || literal.Value.Kind != ValueKindString ||
		literal.Value.String != "MÜNCHEN" {
		t.Fatalf("lower argument = %#v", lower.Arguments[0])
	}

	upper, ok := extend.Assignments[1].Expression.(*ScalarCallExpression)
	if !ok || upper.Function != ScalarFunctionUpper ||
		len(upper.Arguments) != 1 {
		t.Fatalf("upper IR = %#v", extend.Assignments[1].Expression)
	}
	field, ok := upper.Arguments[0].(*ScalarFieldExpression)
	if !ok || field.Field.Name != "source" || !field.Field.Canonical {
		t.Fatalf("upper argument = %#v, want canonical source", upper.Arguments[0])
	}
}

func TestBuildEvalLowerUpperRejectForgedArityEnumAndTypedNil(t *testing.T) {
	t.Parallel()

	base := mustParse(t, `index=gradethis`)
	sourceRange := spl.Range{
		Start: spl.Position{Line: 1, Column: 1},
		End:   spl.Position{Line: 1, Column: 5},
	}
	argument := func() spl.ScalarExpr {
		return &spl.ScalarFieldExpr{Field: "source", Range: sourceRange}
	}
	var typedNil *spl.ScalarFieldExpr
	for _, test := range []struct {
		name       string
		expression spl.ScalarExpr
		code       string
	}{
		{
			name: "lower zero arguments",
			expression: &spl.ScalarCallExpr{
				Function: spl.ScalarFunctionLower,
				Range:    sourceRange,
			},
			code: "SPL_INVALID_EVAL_ARITY",
		},
		{
			name: "upper two arguments",
			expression: &spl.ScalarCallExpr{
				Function:  spl.ScalarFunctionUpper,
				Arguments: []spl.ScalarExpr{argument(), argument()},
				Range:     sourceRange,
			},
			code: "SPL_INVALID_EVAL_ARITY",
		},
		{
			name: "typed nil argument",
			expression: &spl.ScalarCallExpr{
				Function:  spl.ScalarFunctionLower,
				Arguments: []spl.ScalarExpr{typedNil},
				Range:     sourceRange,
			},
			code: "SPL_UNSUPPORTED_EVAL_EXPRESSION",
		},
		{
			name: "invalid function enum",
			expression: &spl.ScalarCallExpr{
				Function:  spl.ScalarFunctionInvalid,
				Arguments: []spl.ScalarExpr{argument()},
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
						Field:      "result",
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
				t.Fatalf("Build succeeded for forged expression %#v", test.expression)
			}
			assertDiagnosticCode(t, err, test.code)
		})
	}
}
