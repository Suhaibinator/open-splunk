package plan

import (
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/spl"
)

func TestBuildEvalToStringPreservesTypedScalarIR(t *testing.T) {
	t.Parallel()

	logical, err := Build(
		mustParse(
			t,
			`index=gradethis | eval number=tostring(123), flag=tostring(isnull(optional))`,
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
	for index := range assignments {
		call, ok := assignments[index].Expression.(*ScalarCallExpression)
		if !ok || call.Function != ScalarFunctionToString ||
			len(call.Arguments) != 1 {
			t.Fatalf("assignment %d IR = %#v", index, assignments[index].Expression)
		}
	}
	number := assignments[0].Expression.(*ScalarCallExpression)
	literal, ok := number.Arguments[0].(*ScalarLiteralExpression)
	if !ok || literal.Value.Kind != ValueKindInt64 || literal.Value.Int64 != 123 {
		t.Fatalf("numeric argument = %#v", number.Arguments[0])
	}
	flag := assignments[1].Expression.(*ScalarCallExpression)
	predicate, ok := flag.Arguments[0].(*ScalarCallExpression)
	if !ok || predicate.Function != ScalarFunctionIsNull {
		t.Fatalf("Boolean argument = %#v", flag.Arguments[0])
	}
}

func TestBuildEvalToStringRejectsForgedArityEnumAndTypedNil(t *testing.T) {
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
				Function: spl.ScalarFunctionToString,
				Range:    sourceRange,
			},
			code: "SPL_INVALID_EVAL_ARITY",
		},
		{
			name: "three arguments",
			expression: &spl.ScalarCallExpr{
				Function:  spl.ScalarFunctionToString,
				Arguments: []spl.ScalarExpr{argument(), argument(), argument()},
				Range:     sourceRange,
			},
			code: "SPL_INVALID_EVAL_ARITY",
		},
		{
			name: "non-literal format",
			expression: &spl.ScalarCallExpr{
				Function:  spl.ScalarFunctionToString,
				Arguments: []spl.ScalarExpr{argument(), argument()},
				Range:     sourceRange,
			},
			code: "SPL_UNSUPPORTED_TOSTRING_FORMAT",
		},
		{
			name: "typed nil argument",
			expression: &spl.ScalarCallExpr{
				Function:  spl.ScalarFunctionToString,
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
