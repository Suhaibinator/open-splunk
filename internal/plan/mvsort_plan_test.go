package plan

import (
	"slices"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/spl"
)

func TestAnalyzeEventAggregateAllowsMVSortAndTracksItsInput(t *testing.T) {
	t.Parallel()

	logical, err := Build(
		mustParse(
			t,
			`index=gradethis | eventstats count(eval(mvcount(mvsort(values))>1)) AS matches BY host`,
		),
		testScope([]string{"gradethis"}, nil),
	)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	analysis, err := Analyze(logical)
	if err != nil {
		t.Fatalf("Analyze: %v", err)
	}
	if !slices.Equal(
		analysis.ReferencedFields,
		[]string{"host", "index", "values"},
	) {
		t.Fatalf(
			"referenced fields = %v, want [host index values]",
			analysis.ReferencedFields,
		)
	}
}

func TestConvertKnowledgeMVSortExpressionPreservesTypedIR(t *testing.T) {
	t.Parallel()

	expression, err := convertKnowledgeCalculatedExpression(
		`mvsort(recipients)`,
		[]string{"recipients"},
		2,
		0,
	)
	if err != nil {
		t.Fatalf("convertKnowledgeCalculatedExpression: %v", err)
	}
	call, ok := expression.(*ScalarCallExpression)
	if !ok || call.Function != ScalarFunctionMVSort || len(call.Arguments) != 1 {
		t.Fatalf("expression = %#v", expression)
	}
	input, ok := call.Arguments[0].(*ScalarFieldExpression)
	if !ok || input.Field.Name != "recipients" {
		t.Fatalf("input = %#v, want recipients field", call.Arguments[0])
	}
}

func TestBuildEvalMVSortPreservesTypedScalarIR(t *testing.T) {
	t.Parallel()

	logical, err := Build(
		mustParse(
			t,
			`index=gradethis | eval sorted=mvsort(values), count=mvcount(mvsort(values))`,
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
	if !ok || first.Function != ScalarFunctionMVSort || len(first.Arguments) != 1 {
		t.Fatalf("first expression = %#v", assignments[0].Expression)
	}
	count, ok := assignments[1].Expression.(*ScalarCallExpression)
	if !ok || count.Function != ScalarFunctionMVCount || len(count.Arguments) != 1 {
		t.Fatalf("count expression = %#v", assignments[1].Expression)
	}
	nested, ok := count.Arguments[0].(*ScalarCallExpression)
	if !ok || nested.Function != ScalarFunctionMVSort || len(nested.Arguments) != 1 {
		t.Fatalf("nested expression = %#v", count.Arguments[0])
	}
}

func TestBuildEvalMVSortRejectsForgedArityEnumBooleanAndTypedNil(t *testing.T) {
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
				Function: spl.ScalarFunctionMVSort,
				Range:    sourceRange,
			},
			code: "SPL_INVALID_EVAL_ARITY",
		},
		{
			name: "two arguments",
			expression: &spl.ScalarCallExpr{
				Function:  spl.ScalarFunctionMVSort,
				Arguments: []spl.ScalarExpr{value(), value()},
				Range:     sourceRange,
			},
			code: "SPL_INVALID_EVAL_ARITY",
		},
		{
			name: "typed nil value",
			expression: &spl.ScalarCallExpr{
				Function:  spl.ScalarFunctionMVSort,
				Arguments: []spl.ScalarExpr{typedNil},
				Range:     sourceRange,
			},
			code: "SPL_UNSUPPORTED_EVAL_EXPRESSION",
		},
		{
			name: "Boolean function value",
			expression: &spl.ScalarCallExpr{
				Function: spl.ScalarFunctionMVSort,
				Arguments: []spl.ScalarExpr{&spl.ScalarCallExpr{
					Function:  spl.ScalarFunctionIsNull,
					Arguments: []spl.ScalarExpr{value()},
					Range:     sourceRange,
				}},
				Range: sourceRange,
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

func TestBuildArithmeticRejectsMVSortResult(t *testing.T) {
	t.Parallel()

	_, err := Build(
		mustParse(t, `index=gradethis | eval invalid=mvsort(values)+1`),
		testScope([]string{"gradethis"}, nil),
	)
	assertDiagnosticCode(t, err, "SPL_UNSUPPORTED_ARITHMETIC_VALUE_TYPE")
}
