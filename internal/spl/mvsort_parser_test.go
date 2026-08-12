package spl

import (
	"slices"
	"testing"
)

func TestParseEvalMVSortPreservesRangeAndCase(t *testing.T) {
	t.Parallel()

	const source = `index=main | eval sorted=MvSoRt(values)`
	query, err := Parse(source)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	expression := query.Commands[0].(*EvalCommand).Assignments[0].Expression
	call, ok := expression.(*ScalarCallExpr)
	if !ok || call.Function != ScalarFunctionMVSort || len(call.Arguments) != 1 {
		t.Fatalf("expression = %#v", expression)
	}
	if got := source[call.Range.Start.Offset:call.Range.End.Offset]; got != `MvSoRt(values)` {
		t.Fatalf("range = %q, want %q", got, `MvSoRt(values)`)
	}
}

func TestParseMVSortSupportsNestingAndPredicateComposition(t *testing.T) {
	t.Parallel()

	for _, source := range []string{
		`index=main | eval sorted=mvsort(values)`,
		`index=main | eval sorted=mvsort(lower(values))`,
		`index=main | eval sorted=mvsort(mvsort(values))`,
		`index=main | where mvcount(mvsort(values))>2`,
		`index=main | eval sorted=mvsort(null)`,
	} {
		if _, err := Parse(source); err != nil {
			t.Fatalf("Parse(%q): %v", source, err)
		}
	}
}

func TestParseMVSortEnforcesExactlyOneArgument(t *testing.T) {
	t.Parallel()

	for _, source := range []string{
		`index=main | eval sorted=mvsort()`,
		`index=main | eval sorted=mvsort(first, second)`,
	} {
		assertParseDiagnosticCode(t, source, "SPL_INVALID_EVAL_ARITY")
	}
}

func TestParseMVSortRejectsBooleanFunctionInput(t *testing.T) {
	t.Parallel()

	assertParseDiagnosticCode(
		t,
		`index=main | eval sorted=mvsort(isnull(optional))`,
		"SPL_UNSUPPORTED_EVAL_EXPRESSION",
	)
}

func TestParseBareMVSortNameRemainsField(t *testing.T) {
	t.Parallel()

	query, err := Parse(`index=main | eval copied=mvsort`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	field, ok := query.Commands[0].(*EvalCommand).Assignments[0].Expression.(*ScalarFieldExpr)
	if !ok || field.Field != "mvsort" {
		t.Fatalf("expression = %#v, want field mvsort", query.Commands[0])
	}
}

func TestParseKnowledgeMVSortExpressionTracksItsInput(t *testing.T) {
	t.Parallel()

	expression, err := ParseScalarExpression(`mvsort(recipients)`)
	if err != nil {
		t.Fatalf("ParseScalarExpression: %v", err)
	}
	call, ok := expression.(*ScalarCallExpr)
	if !ok || call.Function != ScalarFunctionMVSort || len(call.Arguments) != 1 {
		t.Fatalf("expression = %#v", expression)
	}
	analysis, err := AnalyzeScalarExpression(expression)
	if err != nil {
		t.Fatalf("AnalyzeScalarExpression: %v", err)
	}
	if analysis.Nodes != 2 || analysis.Predicates != 0 ||
		!slices.Equal(analysis.InputFields, []string{"recipients"}) {
		t.Fatalf("analysis = %#v", analysis)
	}
}
