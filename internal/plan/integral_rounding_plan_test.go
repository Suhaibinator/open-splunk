package plan

import (
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/spl"
)

func TestBuildEvalIntegralRoundingPreservesTypedScalarIR(t *testing.T) {
	t.Parallel()

	logical, err := Build(
		mustParse(
			t,
			`index=gradethis | eval up=ceil(1.2), alias=ceiling(amount), down=floor(-1.2)`,
		),
		testScope([]string{"gradethis"}, nil),
	)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	assignments := logical.Operators[len(logical.Operators)-1].(*Extend).Assignments
	if len(assignments) != 3 {
		t.Fatalf("assignments = %#v", assignments)
	}
	for index, function := range []ScalarFunction{
		ScalarFunctionCeil,
		ScalarFunctionCeil,
		ScalarFunctionFloor,
	} {
		call, ok := assignments[index].Expression.(*ScalarCallExpression)
		if !ok || call.Function != function || len(call.Arguments) != 1 {
			t.Fatalf("assignment %d expression = %#v", index, assignments[index].Expression)
		}
	}
}

func TestBuildEvalIntegralRoundingRejectsForgedArityBooleanEnumAndTypedNil(t *testing.T) {
	t.Parallel()

	base := mustParse(t, `index=gradethis`)
	sourceRange := spl.Range{
		Start: spl.Position{Line: 1, Column: 1},
		End:   spl.Position{Line: 1, Column: 5},
	}
	value := func() spl.ScalarExpr {
		return &spl.ScalarFieldExpr{Field: "amount", Range: sourceRange}
	}
	boolean := func() spl.ScalarExpr {
		return &spl.ScalarCallExpr{
			Function:  spl.ScalarFunctionIsNull,
			Arguments: []spl.ScalarExpr{value()},
			Range:     sourceRange,
		}
	}
	var typedNil *spl.ScalarFieldExpr
	for _, test := range []struct {
		name       string
		expression spl.ScalarExpr
		code       string
	}{
		{
			name: "ceil zero arguments",
			expression: &spl.ScalarCallExpr{
				Function: spl.ScalarFunctionCeil,
				Range:    sourceRange,
			},
			code: "SPL_INVALID_EVAL_ARITY",
		},
		{
			name: "floor two arguments",
			expression: &spl.ScalarCallExpr{
				Function:  spl.ScalarFunctionFloor,
				Arguments: []spl.ScalarExpr{value(), value()},
				Range:     sourceRange,
			},
			code: "SPL_INVALID_EVAL_ARITY",
		},
		{
			name: "ceil Boolean input",
			expression: &spl.ScalarCallExpr{
				Function:  spl.ScalarFunctionCeil,
				Arguments: []spl.ScalarExpr{boolean()},
				Range:     sourceRange,
			},
			code: "SPL_UNSUPPORTED_EVAL_EXPRESSION",
		},
		{
			name: "floor typed nil input",
			expression: &spl.ScalarCallExpr{
				Function:  spl.ScalarFunctionFloor,
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
