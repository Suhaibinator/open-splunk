package plan

import (
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/spl"
)

func TestBuildEvalLenLengthPreservesTypedScalarIR(t *testing.T) {
	t.Parallel()

	logical, err := Build(
		mustParse(
			t,
			`index=gradethis | eval short=len("München"), long=length(source)`,
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

	for index := range extend.Assignments {
		call, ok := extend.Assignments[index].Expression.(*ScalarCallExpression)
		if !ok || call.Function != ScalarFunctionLength || len(call.Arguments) != 1 {
			t.Fatalf("assignment %d IR = %#v", index, extend.Assignments[index].Expression)
		}
	}

	literal, ok := extend.Assignments[0].Expression.(*ScalarCallExpression).Arguments[0].(*ScalarLiteralExpression)
	if !ok || literal.Value.Kind != ValueKindString || literal.Value.String != "München" {
		t.Fatalf("len argument = %#v", extend.Assignments[0].Expression)
	}
	field, ok := extend.Assignments[1].Expression.(*ScalarCallExpression).Arguments[0].(*ScalarFieldExpression)
	if !ok || field.Field.Name != "source" || !field.Field.Canonical {
		t.Fatalf("length argument = %#v, want canonical source", extend.Assignments[1].Expression)
	}
}

func TestBuildEvalLenLengthRejectForgedArityEnumAndTypedNil(t *testing.T) {
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
			name: "zero arguments",
			expression: &spl.ScalarCallExpr{
				Function: spl.ScalarFunctionLength,
				Range:    sourceRange,
			},
			code: "SPL_INVALID_EVAL_ARITY",
		},
		{
			name: "two arguments",
			expression: &spl.ScalarCallExpr{
				Function:  spl.ScalarFunctionLength,
				Arguments: []spl.ScalarExpr{argument(), argument()},
				Range:     sourceRange,
			},
			code: "SPL_INVALID_EVAL_ARITY",
		},
		{
			name: "typed nil argument",
			expression: &spl.ScalarCallExpr{
				Function:  spl.ScalarFunctionLength,
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
