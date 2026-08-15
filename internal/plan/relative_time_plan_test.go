package plan

import (
	"strings"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/spl"
	"github.com/Suhaibinator/open-splunk/internal/splrelativetime"
)

func TestBuildRelativeTimePreservesTypedIRAndSearchTimezone(t *testing.T) {
	t.Parallel()

	scope := testScope([]string{"gradethis"}, nil)
	scope.SearchTimezone = "America/Los_Angeles"
	logical, err := Build(
		mustParse(
			t,
			`index=gradethis | eval shifted=relative_time(strptime(timestamp, "%F"), "-1d@d+2h")`,
		),
		scope,
	)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if logical.SearchTimezone != scope.SearchTimezone {
		t.Fatalf(
			"SearchTimezone = %q, want %q",
			logical.SearchTimezone,
			scope.SearchTimezone,
		)
	}
	call := logical.Operators[len(logical.Operators)-1].(*Extend).
		Assignments[0].Expression.(*ScalarCallExpression)
	if call.Function != ScalarFunctionRelativeTime || len(call.Arguments) != 2 {
		t.Fatalf("expression = %#v", call)
	}
	parsed, ok := call.Arguments[0].(*ScalarCallExpression)
	if !ok || parsed.Function != ScalarFunctionStrptime {
		t.Fatalf("time argument = %#v", call.Arguments[0])
	}
	specifier := call.Arguments[1].(*ScalarLiteralExpression)
	if specifier.Value.Kind != ValueKindString ||
		specifier.Value.String != "-1d@d+2h" {
		t.Fatalf("specifier = %#v", specifier)
	}
}

func TestBuildRelativeTimeRejectsForgedAritySpecifierBooleanEnumAndTypedNil(t *testing.T) {
	t.Parallel()

	base := mustParse(t, `index=gradethis`)
	sourceRange := spl.Range{
		Start: spl.Position{Line: 1, Column: 1},
		End:   spl.Position{Line: 1, Column: 9},
	}
	value := func() spl.ScalarExpr {
		return &spl.ScalarFieldExpr{Field: "_time", Range: sourceRange}
	}
	specifier := func(text string, quoted bool) spl.ScalarExpr {
		return &spl.ScalarLiteralExpr{
			Value: spl.Literal{
				Kind: spl.LiteralKindString, Text: text, Quoted: quoted,
			},
			Range: sourceRange,
		}
	}
	var typedNil *spl.ScalarFieldExpr
	var typedNilSpecifier *spl.ScalarLiteralExpr
	for _, test := range []struct {
		name       string
		expression *spl.ScalarCallExpr
		code       string
	}{
		{
			name: "zero arguments",
			expression: &spl.ScalarCallExpr{
				Function: spl.ScalarFunctionRelativeTime,
				Range:    sourceRange,
			},
			code: "SPL_INVALID_EVAL_ARITY",
		},
		{
			name: "one argument",
			expression: &spl.ScalarCallExpr{
				Function:  spl.ScalarFunctionRelativeTime,
				Arguments: []spl.ScalarExpr{value()},
				Range:     sourceRange,
			},
			code: "SPL_INVALID_EVAL_ARITY",
		},
		{
			name: "three arguments",
			expression: &spl.ScalarCallExpr{
				Function: spl.ScalarFunctionRelativeTime,
				Arguments: []spl.ScalarExpr{
					value(), specifier("@d", true), specifier("+1h", true),
				},
				Range: sourceRange,
			},
			code: "SPL_INVALID_EVAL_ARITY",
		},
		{
			name: "typed nil time",
			expression: &spl.ScalarCallExpr{
				Function: spl.ScalarFunctionRelativeTime,
				Arguments: []spl.ScalarExpr{
					typedNil, specifier("@d", true),
				},
				Range: sourceRange,
			},
			code: "SPL_UNSUPPORTED_EVAL_EXPRESSION",
		},
		{
			name: "nonliteral specifier",
			expression: &spl.ScalarCallExpr{
				Function: spl.ScalarFunctionRelativeTime,
				Arguments: []spl.ScalarExpr{
					value(), value(),
				},
				Range: sourceRange,
			},
			code: "SPL_UNSUPPORTED_EVAL_EXPRESSION",
		},
		{
			name: "typed nil specifier",
			expression: &spl.ScalarCallExpr{
				Function: spl.ScalarFunctionRelativeTime,
				Arguments: []spl.ScalarExpr{
					value(), typedNilSpecifier,
				},
				Range: sourceRange,
			},
			code: "SPL_UNSUPPORTED_EVAL_EXPRESSION",
		},
		{
			name: "unquoted specifier",
			expression: &spl.ScalarCallExpr{
				Function: spl.ScalarFunctionRelativeTime,
				Arguments: []spl.ScalarExpr{
					value(), specifier("@d", false),
				},
				Range: sourceRange,
			},
			code: "SPL_UNSUPPORTED_EVAL_EXPRESSION",
		},
		{
			name: "Boolean time",
			expression: &spl.ScalarCallExpr{
				Function: spl.ScalarFunctionRelativeTime,
				Arguments: []spl.ScalarExpr{&spl.ScalarCallExpr{
					Function:  spl.ScalarFunctionIsNull,
					Arguments: []spl.ScalarExpr{value()},
					Range:     sourceRange,
				}, specifier("@d", true)},
				Range: sourceRange,
			},
			code: "SPL_UNSUPPORTED_EVAL_EXPRESSION",
		},
		{
			name: "invalid specifier",
			expression: &spl.ScalarCallExpr{
				Function: spl.ScalarFunctionRelativeTime,
				Arguments: []spl.ScalarExpr{
					value(), specifier("-1d@d+2h+1s", true),
				},
				Range: sourceRange,
			},
			code: "SPL_UNSUPPORTED_RELATIVE_TIME_SPECIFIER",
		},
		{
			name: "oversized specifier",
			expression: &spl.ScalarCallExpr{
				Function: spl.ScalarFunctionRelativeTime,
				Arguments: []spl.ScalarExpr{
					value(),
					specifier(
						"+1"+strings.Repeat(
							"s",
							splrelativetime.MaximumSpecifierBytes+1,
						),
						true,
					),
				},
				Range: sourceRange,
			},
			code: "SPL_QUERY_TOO_COMPLEX",
		},
		{
			name: "magnitude out of range",
			expression: &spl.ScalarCallExpr{
				Function: spl.ScalarFunctionRelativeTime,
				Arguments: []spl.ScalarExpr{
					value(), specifier("+363y", true),
				},
				Range: sourceRange,
			},
			code: "SPL_NUMBER_OUT_OF_RANGE",
		},
		{
			name: "count sentinel",
			expression: &spl.ScalarCallExpr{
				Function: spl.ScalarFunctionCount,
				Arguments: []spl.ScalarExpr{
					value(), specifier("@d", true),
				},
				Range: sourceRange,
			},
			code: "SPL_UNSUPPORTED_EVAL_FUNCTION",
		},
		{
			name: "invalid function enum",
			expression: &spl.ScalarCallExpr{
				Function: spl.ScalarFunctionInvalid,
				Arguments: []spl.ScalarExpr{
					value(), specifier("@d", true),
				},
				Range: sourceRange,
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
