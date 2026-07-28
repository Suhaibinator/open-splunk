package plan

import (
	"strings"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/spl"
	"github.com/Suhaibinator/open-splunk/internal/spltimeformat"
)

func TestBuildStrptimePreservesTypedIRAndSearchTimezone(t *testing.T) {
	t.Parallel()

	scope := testScope([]string{"gradethis"}, nil)
	scope.SearchTimezone = "America/Los_Angeles"
	logical, err := Build(
		mustParse(
			t,
			`index=gradethis | eval epoch=strptime(strftime(now(), "%F %T"), "%F %T")`,
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
	if call.Function != ScalarFunctionStrptime || len(call.Arguments) != 2 {
		t.Fatalf("expression = %#v", call)
	}
	formatted, ok := call.Arguments[0].(*ScalarCallExpression)
	if !ok || formatted.Function != ScalarFunctionStrftime {
		t.Fatalf("text argument = %#v", call.Arguments[0])
	}
	format := call.Arguments[1].(*ScalarLiteralExpression)
	if format.Value.Kind != ValueKindString || format.Value.String != "%F %T" {
		t.Fatalf("format = %#v", format)
	}
}

func TestBuildStrptimeRejectsForgedArityFormatBooleanEnumAndTypedNil(t *testing.T) {
	t.Parallel()

	base := mustParse(t, `index=gradethis`)
	sourceRange := spl.Range{
		Start: spl.Position{Line: 1, Column: 1},
		End:   spl.Position{Line: 1, Column: 9},
	}
	value := func() spl.ScalarExpr {
		return &spl.ScalarFieldExpr{Field: "timestamp", Range: sourceRange}
	}
	format := func(text string, quoted bool) spl.ScalarExpr {
		return &spl.ScalarLiteralExpr{
			Value: spl.Literal{
				Kind: spl.LiteralKindString, Text: text, Quoted: quoted,
			},
			Range: sourceRange,
		}
	}
	var typedNil *spl.ScalarFieldExpr
	var typedNilFormat *spl.ScalarLiteralExpr
	for _, test := range []struct {
		name       string
		expression *spl.ScalarCallExpr
		code       string
	}{
		{
			name: "zero arguments",
			expression: &spl.ScalarCallExpr{
				Function: spl.ScalarFunctionStrptime,
				Range:    sourceRange,
			},
			code: "SPL_INVALID_EVAL_ARITY",
		},
		{
			name: "one argument",
			expression: &spl.ScalarCallExpr{
				Function:  spl.ScalarFunctionStrptime,
				Arguments: []spl.ScalarExpr{value()},
				Range:     sourceRange,
			},
			code: "SPL_INVALID_EVAL_ARITY",
		},
		{
			name: "three arguments",
			expression: &spl.ScalarCallExpr{
				Function: spl.ScalarFunctionStrptime,
				Arguments: []spl.ScalarExpr{
					value(), format("%F", true), format("%T", true),
				},
				Range: sourceRange,
			},
			code: "SPL_INVALID_EVAL_ARITY",
		},
		{
			name: "typed nil text",
			expression: &spl.ScalarCallExpr{
				Function: spl.ScalarFunctionStrptime,
				Arguments: []spl.ScalarExpr{
					typedNil, format("%F", true),
				},
				Range: sourceRange,
			},
			code: "SPL_UNSUPPORTED_EVAL_EXPRESSION",
		},
		{
			name: "nonliteral format",
			expression: &spl.ScalarCallExpr{
				Function: spl.ScalarFunctionStrptime,
				Arguments: []spl.ScalarExpr{
					value(), value(),
				},
				Range: sourceRange,
			},
			code: "SPL_UNSUPPORTED_EVAL_EXPRESSION",
		},
		{
			name: "typed nil format",
			expression: &spl.ScalarCallExpr{
				Function: spl.ScalarFunctionStrptime,
				Arguments: []spl.ScalarExpr{
					value(), typedNilFormat,
				},
				Range: sourceRange,
			},
			code: "SPL_UNSUPPORTED_EVAL_EXPRESSION",
		},
		{
			name: "unquoted format",
			expression: &spl.ScalarCallExpr{
				Function: spl.ScalarFunctionStrptime,
				Arguments: []spl.ScalarExpr{
					value(), format("%F", false),
				},
				Range: sourceRange,
			},
			code: "SPL_UNSUPPORTED_EVAL_EXPRESSION",
		},
		{
			name: "Boolean text",
			expression: &spl.ScalarCallExpr{
				Function: spl.ScalarFunctionStrptime,
				Arguments: []spl.ScalarExpr{&spl.ScalarCallExpr{
					Function:  spl.ScalarFunctionIsNull,
					Arguments: []spl.ScalarExpr{value()},
					Range:     sourceRange,
				}, format("%F", true)},
				Range: sourceRange,
			},
			code: "SPL_UNSUPPORTED_EVAL_EXPRESSION",
		},
		{
			name: "invalid format",
			expression: &spl.ScalarCallExpr{
				Function: spl.ScalarFunctionStrptime,
				Arguments: []spl.ScalarExpr{
					value(), format("%Y-%m", true),
				},
				Range: sourceRange,
			},
			code: "SPL_UNSUPPORTED_TIME_FORMAT",
		},
		{
			name: "formatter output amplification is unsupported",
			expression: &spl.ScalarCallExpr{
				Function: spl.ScalarFunctionStrptime,
				Arguments: []spl.ScalarExpr{
					value(), format(strings.Repeat("%F", 1700), true),
				},
				Range: sourceRange,
			},
			code: "SPL_UNSUPPORTED_TIME_FORMAT",
		},
		{
			name: "oversized format",
			expression: &spl.ScalarCallExpr{
				Function: spl.ScalarFunctionStrptime,
				Arguments: []spl.ScalarExpr{
					value(),
					format(
						strings.Repeat(
							"x",
							spltimeformat.MaximumStrptimeFormatBytes+1,
						),
						true,
					),
				},
				Range: sourceRange,
			},
			code: "SPL_QUERY_TOO_COMPLEX",
		},
		{
			name: "invalid function enum",
			expression: &spl.ScalarCallExpr{
				Function: spl.ScalarFunctionInvalid,
				Arguments: []spl.ScalarExpr{
					value(), format("%F", true),
				},
				Range: sourceRange,
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
