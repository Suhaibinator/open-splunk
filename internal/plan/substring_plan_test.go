package plan

import (
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/spl"
)

func TestBuildEvalSubstringPreservesTypedScalarIR(t *testing.T) {
	t.Parallel()

	logical, err := Build(
		mustParse(
			t,
			`index=gradethis | eval middle=substr("😀abcdef", -4, 3), suffix=substr(source, 2)`,
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

	first, firstOK := assignments[0].Expression.(*ScalarCallExpression)
	if !firstOK || first.Function != ScalarFunctionSubstring || len(first.Arguments) != 3 {
		t.Fatalf("first substring IR = %#v", assignments[0].Expression)
	}
	if value, valueOK := first.Arguments[0].(*ScalarLiteralExpression); !valueOK ||
		value.Value.Kind != ValueKindString || value.Value.String != "😀abcdef" {
		t.Fatalf("first value IR = %#v", first.Arguments[0])
	}
	if start, startOK := first.Arguments[1].(*ScalarLiteralExpression); !startOK ||
		start.Value.Kind != ValueKindInt64 || start.Value.Int64 != -4 {
		t.Fatalf("start IR = %#v", first.Arguments[1])
	}
	if length, lengthOK := first.Arguments[2].(*ScalarLiteralExpression); !lengthOK ||
		length.Value.Kind != ValueKindInt64 || length.Value.Int64 != 3 {
		t.Fatalf("length IR = %#v", first.Arguments[2])
	}

	second, secondOK := assignments[1].Expression.(*ScalarCallExpression)
	if !secondOK || second.Function != ScalarFunctionSubstring || len(second.Arguments) != 2 {
		t.Fatalf("second substring IR = %#v", assignments[1].Expression)
	}
	field, fieldOK := second.Arguments[0].(*ScalarFieldExpression)
	if !fieldOK || field.Field.Name != "source" || !field.Field.Canonical {
		t.Fatalf("second input IR = %#v, want canonical source", second.Arguments[0])
	}
}

func TestBuildEvalSubstringRejectsForgedArityIndexesEnumAndTypedNil(t *testing.T) {
	t.Parallel()

	base := mustParse(t, `index=gradethis`)
	sourceRange := spl.Range{
		Start: spl.Position{Line: 1, Column: 1},
		End:   spl.Position{Line: 1, Column: 5},
	}
	value := func() spl.ScalarExpr {
		return &spl.ScalarFieldExpr{Field: "source", Range: sourceRange}
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
			name: "one argument",
			expression: &spl.ScalarCallExpr{
				Function:  spl.ScalarFunctionSubstring,
				Arguments: []spl.ScalarExpr{value()},
				Range:     sourceRange,
			},
			code: "SPL_INVALID_EVAL_ARITY",
		},
		{
			name: "four arguments",
			expression: &spl.ScalarCallExpr{
				Function: spl.ScalarFunctionSubstring,
				Arguments: []spl.ScalarExpr{
					value(), integer("1"), integer("2"), integer("3"),
				},
				Range: sourceRange,
			},
			code: "SPL_INVALID_EVAL_ARITY",
		},
		{
			name: "typed nil value",
			expression: &spl.ScalarCallExpr{
				Function:  spl.ScalarFunctionSubstring,
				Arguments: []spl.ScalarExpr{typedNil, integer("1")},
				Range:     sourceRange,
			},
			code: "SPL_UNSUPPORTED_EVAL_EXPRESSION",
		},
		{
			name: "field start",
			expression: &spl.ScalarCallExpr{
				Function:  spl.ScalarFunctionSubstring,
				Arguments: []spl.ScalarExpr{value(), value()},
				Range:     sourceRange,
			},
			code: "SPL_UNSUPPORTED_SUBSTRING_INDEX",
		},
		{
			name: "float length",
			expression: &spl.ScalarCallExpr{
				Function: spl.ScalarFunctionSubstring,
				Arguments: []spl.ScalarExpr{
					value(),
					integer("1"),
					&spl.ScalarLiteralExpr{
						Value: spl.Literal{
							Kind:  spl.LiteralKindFloat,
							Text:  "1.5",
							Range: sourceRange,
						},
						Range: sourceRange,
					},
				},
				Range: sourceRange,
			},
			code: "SPL_UNSUPPORTED_SUBSTRING_INDEX",
		},
		{
			name: "invalid function enum",
			expression: &spl.ScalarCallExpr{
				Function:  spl.ScalarFunctionInvalid,
				Arguments: []spl.ScalarExpr{value(), integer("1")},
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
