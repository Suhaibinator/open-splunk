package plan

import (
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/spl"
)

func TestBuildEvalRoundPreservesTypedScalarIR(t *testing.T) {
	t.Parallel()

	logical, err := Build(
		mustParse(
			t,
			`index=gradethis | eval whole=round(3.5), cents=round(amount, 2)`,
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
	first, ok := assignments[0].Expression.(*ScalarCallExpression)
	if !ok || first.Function != ScalarFunctionRound || len(first.Arguments) != 1 {
		t.Fatalf("first round IR = %#v", assignments[0].Expression)
	}
	value, ok := first.Arguments[0].(*ScalarLiteralExpression)
	if !ok || value.Value.Kind != ValueKindFloat64 || value.Value.Float64 != 3.5 {
		t.Fatalf("first value IR = %#v", first.Arguments[0])
	}
	second, ok := assignments[1].Expression.(*ScalarCallExpression)
	if !ok || second.Function != ScalarFunctionRound || len(second.Arguments) != 2 {
		t.Fatalf("second round IR = %#v", assignments[1].Expression)
	}
	precision, ok := second.Arguments[1].(*ScalarLiteralExpression)
	if !ok || precision.Value.Kind != ValueKindInt64 || precision.Value.Int64 != 2 {
		t.Fatalf("precision IR = %#v", second.Arguments[1])
	}
}

func TestBuildEvalRoundRejectsForgedArityPrecisionEnumAndTypedNil(t *testing.T) {
	t.Parallel()

	base := mustParse(t, `index=gradethis`)
	sourceRange := spl.Range{
		Start: spl.Position{Line: 1, Column: 1},
		End:   spl.Position{Line: 1, Column: 5},
	}
	value := func() spl.ScalarExpr {
		return &spl.ScalarFieldExpr{Field: "amount", Range: sourceRange}
	}
	integer := func(text string) spl.ScalarExpr {
		return &spl.ScalarLiteralExpr{
			Value: spl.Literal{
				Kind:  spl.LiteralKindInteger,
				Text:  text,
				Range: sourceRange,
			},
			Range: sourceRange,
		}
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
				Function: spl.ScalarFunctionRound,
				Range:    sourceRange,
			},
			code: "SPL_INVALID_EVAL_ARITY",
		},
		{
			name: "three arguments",
			expression: &spl.ScalarCallExpr{
				Function: spl.ScalarFunctionRound,
				Arguments: []spl.ScalarExpr{
					value(), integer("1"), integer("2"),
				},
				Range: sourceRange,
			},
			code: "SPL_INVALID_EVAL_ARITY",
		},
		{
			name: "typed nil value",
			expression: &spl.ScalarCallExpr{
				Function:  spl.ScalarFunctionRound,
				Arguments: []spl.ScalarExpr{typedNil},
				Range:     sourceRange,
			},
			code: "SPL_UNSUPPORTED_EVAL_EXPRESSION",
		},
		{
			name: "field precision",
			expression: &spl.ScalarCallExpr{
				Function:  spl.ScalarFunctionRound,
				Arguments: []spl.ScalarExpr{value(), value()},
				Range:     sourceRange,
			},
			code: "SPL_UNSUPPORTED_ROUND_PRECISION",
		},
		{
			name: "negative precision",
			expression: &spl.ScalarCallExpr{
				Function:  spl.ScalarFunctionRound,
				Arguments: []spl.ScalarExpr{value(), integer("-1")},
				Range:     sourceRange,
			},
			code: "SPL_UNSUPPORTED_ROUND_PRECISION",
		},
		{
			name: "excessive precision",
			expression: &spl.ScalarCallExpr{
				Function:  spl.ScalarFunctionRound,
				Arguments: []spl.ScalarExpr{value(), integer("19")},
				Range:     sourceRange,
			},
			code: "SPL_UNSUPPORTED_ROUND_PRECISION",
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
