package plan

import (
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/spl"
)

func TestBuildEvalMVCountPreservesTypedScalarIR(t *testing.T) {
	t.Parallel()

	logical, err := Build(
		mustParse(
			t,
			`index=gradethis | eval values_count=mvcount(values), predicate_count=mvcount(isnull(optional))`,
		),
		testScope([]string{"gradethis"}, nil),
	)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	assignments := logical.Operators[len(logical.Operators)-1].(*Extend).Assignments
	if len(assignments) != 2 {
		t.Fatalf("assignments = %#v", assignments)
	}
	for index, assignment := range assignments {
		call, ok := assignment.Expression.(*ScalarCallExpression)
		if !ok || call.Function != ScalarFunctionMVCount || len(call.Arguments) != 1 {
			t.Fatalf("assignment %d expression = %#v", index, assignment.Expression)
		}
	}
	predicate, ok := assignments[1].Expression.(*ScalarCallExpression).
		Arguments[0].(*ScalarCallExpression)
	if !ok || predicate.Function != ScalarFunctionIsNull {
		t.Fatalf("predicate input = %#v", assignments[1].Expression)
	}
}

func TestBuildEvalMVCountRejectsForgedArityEnumAndTypedNil(t *testing.T) {
	t.Parallel()

	base := mustParse(t, `index=gradethis`)
	sourceRange := spl.Range{
		Start: spl.Position{Line: 1, Column: 1},
		End:   spl.Position{Line: 1, Column: 5},
	}
	value := func() spl.ScalarExpr {
		return &spl.ScalarFieldExpr{Field: "values", Range: sourceRange}
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
				Function: spl.ScalarFunctionMVCount,
				Range:    sourceRange,
			},
			code: "SPL_INVALID_EVAL_ARITY",
		},
		{
			name: "two arguments",
			expression: &spl.ScalarCallExpr{
				Function:  spl.ScalarFunctionMVCount,
				Arguments: []spl.ScalarExpr{value(), value()},
				Range:     sourceRange,
			},
			code: "SPL_INVALID_EVAL_ARITY",
		},
		{
			name: "typed nil value",
			expression: &spl.ScalarCallExpr{
				Function:  spl.ScalarFunctionMVCount,
				Arguments: []spl.ScalarExpr{typedNil},
				Range:     sourceRange,
			},
			code: "SPL_UNSUPPORTED_EVAL_EXPRESSION",
		},
		{
			name: "invalid function enum",
			expression: &spl.ScalarCallExpr{
				Function:  spl.ScalarFunctionInvalid,
				Arguments: []spl.ScalarExpr{value()},
				Range:     sourceRange,
			},
			code: "SPL_UNSUPPORTED_EVAL_FUNCTION",
		},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			assertForgedEvalBuildDiagnostic(
				t,
				base,
				sourceRange,
				test.expression,
				test.code,
			)
		})
	}
}
