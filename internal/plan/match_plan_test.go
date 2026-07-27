package plan

import (
	"strings"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/spl"
	"github.com/Suhaibinator/open-splunk/internal/splregex"
)

func TestBuildMatchPreservesTypedPredicateIR(t *testing.T) {
	t.Parallel()

	logical, err := Build(
		mustParse(
			t,
			`index=gradethis | where match(message, "(?i)^error") OR NOT match(lower(service), "api")`,
		),
		testScope([]string{"gradethis"}, nil),
	)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	filter := logical.Operators[len(logical.Operators)-1].(*Filter)
	root := filter.Expression.(*BooleanExpression)
	left := root.Left.(*ScalarPredicateExpression).Value.(*ScalarCallExpression)
	if left.Function != ScalarFunctionMatch || len(left.Arguments) != 2 {
		t.Fatalf("left predicate = %#v", root.Left)
	}
	pattern := left.Arguments[1].(*ScalarLiteralExpression)
	if pattern.Value.Kind != ValueKindString || pattern.Value.String != "(?i)^error" {
		t.Fatalf("pattern = %#v", pattern)
	}
	right := root.Right.(*NotExpression).Operand.(*ScalarPredicateExpression).
		Value.(*ScalarCallExpression)
	if right.Function != ScalarFunctionMatch ||
		right.Arguments[0].(*ScalarCallExpression).Function != ScalarFunctionLower {
		t.Fatalf("right predicate = %#v", root.Right)
	}
}

func TestBuildMatchRejectsForgedArityPatternBooleanEnumAndTypedNil(t *testing.T) {
	t.Parallel()

	base := mustParse(t, `index=gradethis`)
	sourceRange := spl.Range{
		Start: spl.Position{Line: 1, Column: 1},
		End:   spl.Position{Line: 1, Column: 5},
	}
	value := func() spl.ScalarExpr {
		return &spl.ScalarFieldExpr{Field: "message", Range: sourceRange}
	}
	pattern := func(text string) spl.ScalarExpr {
		return &spl.ScalarLiteralExpr{
			Value: spl.Literal{Kind: spl.LiteralKindString, Text: text, Quoted: true},
			Range: sourceRange,
		}
	}
	var typedNil *spl.ScalarFieldExpr
	var typedNilPattern *spl.ScalarLiteralExpr
	for _, test := range []struct {
		name       string
		expression *spl.ScalarCallExpr
		code       string
	}{
		{
			name:       "zero arguments",
			expression: &spl.ScalarCallExpr{Function: spl.ScalarFunctionMatch, Range: sourceRange},
			code:       "SPL_INVALID_EVAL_ARITY",
		},
		{
			name: "one argument",
			expression: &spl.ScalarCallExpr{
				Function: spl.ScalarFunctionMatch, Arguments: []spl.ScalarExpr{value()}, Range: sourceRange,
			},
			code: "SPL_INVALID_EVAL_ARITY",
		},
		{
			name: "three arguments",
			expression: &spl.ScalarCallExpr{
				Function:  spl.ScalarFunctionMatch,
				Arguments: []spl.ScalarExpr{value(), pattern("x"), pattern("y")},
				Range:     sourceRange,
			},
			code: "SPL_INVALID_EVAL_ARITY",
		},
		{
			name: "typed nil value",
			expression: &spl.ScalarCallExpr{
				Function:  spl.ScalarFunctionMatch,
				Arguments: []spl.ScalarExpr{typedNil, pattern("x")},
				Range:     sourceRange,
			},
			code: "SPL_UNSUPPORTED_EVAL_EXPRESSION",
		},
		{
			name: "nonliteral pattern",
			expression: &spl.ScalarCallExpr{
				Function:  spl.ScalarFunctionMatch,
				Arguments: []spl.ScalarExpr{value(), value()},
				Range:     sourceRange,
			},
			code: "SPL_UNSUPPORTED_EVAL_EXPRESSION",
		},
		{
			name: "typed nil pattern",
			expression: &spl.ScalarCallExpr{
				Function:  spl.ScalarFunctionMatch,
				Arguments: []spl.ScalarExpr{value(), typedNilPattern},
				Range:     sourceRange,
			},
			code: "SPL_UNSUPPORTED_EVAL_EXPRESSION",
		},
		{
			name: "Boolean value",
			expression: &spl.ScalarCallExpr{
				Function: spl.ScalarFunctionMatch,
				Arguments: []spl.ScalarExpr{&spl.ScalarCallExpr{
					Function:  spl.ScalarFunctionIsNull,
					Arguments: []spl.ScalarExpr{value()},
					Range:     sourceRange,
				}, pattern("true")},
				Range: sourceRange,
			},
			code: "SPL_UNSUPPORTED_EVAL_EXPRESSION",
		},
		{
			name: "unsupported regex",
			expression: &spl.ScalarCallExpr{
				Function:  spl.ScalarFunctionMatch,
				Arguments: []spl.ScalarExpr{value(), pattern(`(?=secret)`)},
				Range:     sourceRange,
			},
			code: "SPL_UNSUPPORTED_REGEX",
		},
		{
			name: "oversized regex",
			expression: &spl.ScalarCallExpr{
				Function:  spl.ScalarFunctionMatch,
				Arguments: []spl.ScalarExpr{value(), pattern(strings.Repeat("x", splregex.MaximumMatchPatternBytes+1))},
				Range:     sourceRange,
			},
			code: "SPL_QUERY_TOO_COMPLEX",
		},
		{
			name: "oversized regex program",
			expression: &spl.ScalarCallExpr{
				Function: spl.ScalarFunctionMatch,
				Arguments: []spl.ScalarExpr{
					value(),
					pattern(strings.Repeat("a{1000}", 5)),
				},
				Range: sourceRange,
			},
			code: "SPL_QUERY_TOO_COMPLEX",
		},
		{
			name: "invalid function enum",
			expression: &spl.ScalarCallExpr{
				Function:  spl.ScalarFunctionInvalid,
				Arguments: []spl.ScalarExpr{value(), pattern("x")},
				Range:     sourceRange,
			},
			code: "SPL_UNSUPPORTED_EVAL_FUNCTION",
		},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			wrapped := &spl.ScalarCallExpr{
				Function:  spl.ScalarFunctionMVCount,
				Arguments: []spl.ScalarExpr{test.expression},
				Range:     sourceRange,
			}
			assertForgedEvalBuildDiagnostic(
				t,
				base,
				sourceRange,
				wrapped,
				test.code,
			)
		})
	}
}

func TestScalarFunctionBooleanTraitsRemainInParity(t *testing.T) {
	t.Parallel()

	functions := []struct {
		spl  spl.ScalarFunction
		plan ScalarFunction
		want bool
	}{
		{spl.ScalarFunctionInvalid, ScalarFunctionInvalid, false},
		{spl.ScalarFunctionToNumber, ScalarFunctionToNumber, false},
		{spl.ScalarFunctionReplace, ScalarFunctionReplace, false},
		{spl.ScalarFunctionIsNull, ScalarFunctionIsNull, true},
		{spl.ScalarFunctionIsNotNull, ScalarFunctionIsNotNull, true},
		{spl.ScalarFunctionCoalesce, ScalarFunctionCoalesce, false},
		{spl.ScalarFunctionLower, ScalarFunctionLower, false},
		{spl.ScalarFunctionUpper, ScalarFunctionUpper, false},
		{spl.ScalarFunctionLength, ScalarFunctionLength, false},
		{spl.ScalarFunctionSubstring, ScalarFunctionSubstring, false},
		{spl.ScalarFunctionToString, ScalarFunctionToString, false},
		{spl.ScalarFunctionRound, ScalarFunctionRound, false},
		{spl.ScalarFunctionCeil, ScalarFunctionCeil, false},
		{spl.ScalarFunctionFloor, ScalarFunctionFloor, false},
		{spl.ScalarFunctionMVCount, ScalarFunctionMVCount, false},
		{spl.ScalarFunctionMatch, ScalarFunctionMatch, true},
		{spl.ScalarFunctionLike, ScalarFunctionLike, true},
		{spl.ScalarFunctionNow, ScalarFunctionNow, false},
		{spl.ScalarFunctionStrftime, ScalarFunctionStrftime, false},
	}
	for index, function := range functions {
		if int(function.spl) != index || int(function.plan) != index {
			t.Fatalf(
				"function %d enum mapping = SPL %d / plan %d",
				index,
				function.spl,
				function.plan,
			)
		}
		if got := function.spl.ReturnsBoolean(); got != function.want {
			t.Errorf("SPL function %d ReturnsBoolean = %t, want %t", index, got, function.want)
		}
		if got := function.plan.ReturnsBoolean(); got != function.want {
			t.Errorf("plan function %d ReturnsBoolean = %t, want %t", index, got, function.want)
		}
	}
}
