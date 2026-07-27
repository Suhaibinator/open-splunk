package plan

import (
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/spl"
)

func TestBuildNowPreservesTypedScalarIR(t *testing.T) {
	t.Parallel()

	logical, err := Build(
		mustParse(
			t,
			`index=gradethis | eval started=now(), rendered=tostring(now()) | where now()=started`,
		),
		testScope([]string{"gradethis"}, nil),
	)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	extend := logical.Operators[len(logical.Operators)-2].(*Extend)
	started := extend.Assignments[0].Expression.(*ScalarCallExpression)
	if started.Function != ScalarFunctionNow || len(started.Arguments) != 0 {
		t.Fatalf("started = %#v", started)
	}
	rendered := extend.Assignments[1].Expression.(*ScalarCallExpression)
	if rendered.Function != ScalarFunctionToString ||
		rendered.Arguments[0].(*ScalarCallExpression).Function != ScalarFunctionNow {
		t.Fatalf("rendered = %#v", rendered)
	}
	filter := logical.Operators[len(logical.Operators)-1].(*Filter)
	comparison := filter.Expression.(*EvalComparisonExpression)
	if comparison.Left.(*ScalarCallExpression).Function != ScalarFunctionNow {
		t.Fatalf("filter = %#v", filter.Expression)
	}
}

func TestBuildNowRejectsForgedArgumentsAndInvalidEnum(t *testing.T) {
	t.Parallel()

	base := mustParse(t, `index=gradethis`)
	sourceRange := spl.Range{
		Start: spl.Position{Line: 1, Column: 1},
		End:   spl.Position{Line: 1, Column: 6},
	}
	for _, test := range []struct {
		name       string
		expression *spl.ScalarCallExpr
		code       string
	}{
		{
			name: "one argument",
			expression: &spl.ScalarCallExpr{
				Function: spl.ScalarFunctionNow,
				Arguments: []spl.ScalarExpr{&spl.ScalarLiteralExpr{
					Value: spl.Literal{Kind: spl.LiteralKindInteger, Text: "1"},
					Range: sourceRange,
				}},
				Range: sourceRange,
			},
			code: "SPL_INVALID_EVAL_ARITY",
		},
		{
			name:       "invalid function enum",
			expression: &spl.ScalarCallExpr{Function: spl.ScalarFunctionInvalid, Range: sourceRange},
			code:       "SPL_UNSUPPORTED_EVAL_FUNCTION",
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
