package plan

import (
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/spl"
)

func TestBuildNowPreservesTypedScalarIR(t *testing.T) {
	t.Parallel()

	scope := testScope([]string{"gradethis"}, nil)
	scope.SearchStart = time.Date(2026, time.July, 27, 19, 20, 21, 987654321, time.FixedZone("fixture", -7*60*60))
	logical, err := Build(
		mustParse(
			t,
			`index=gradethis | eval started=now(), rendered=tostring(now()) | where now()=started`,
		),
		scope,
	)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if !logical.SearchStart.Equal(scope.SearchStart.UTC()) {
		t.Fatalf("SearchStart = %v, want %v", logical.SearchStart, scope.SearchStart.UTC())
	}
	if logical.SearchStart.Equal(scope.IndexTimeCutoff) {
		t.Fatal("SearchStart is coupled to IndexTimeCutoff")
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

func TestBuildNowRequiresExplicitSearchStart(t *testing.T) {
	t.Parallel()

	scope := testScope([]string{"gradethis"}, nil)
	scope.SearchStart = time.Time{}
	_, err := Build(mustParse(t, `index=gradethis | eval started=now()`), scope)
	assertDiagnosticCode(t, err, "SPL_INVALID_SEARCH_START")
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
