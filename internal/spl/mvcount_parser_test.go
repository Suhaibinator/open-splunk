package spl

import "testing"

func TestParseEvalMVCountPreservesRangeAndCase(t *testing.T) {
	t.Parallel()

	const source = `index=main | eval count=MvCoUnT(values)`
	query, err := Parse(source)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	expression := query.Commands[0].(*EvalCommand).Assignments[0].Expression
	call, ok := expression.(*ScalarCallExpr)
	if !ok || call.Function != ScalarFunctionMVCount || len(call.Arguments) != 1 {
		t.Fatalf("expression = %#v", expression)
	}
	if got := source[call.Range.Start.Offset:call.Range.End.Offset]; got != `MvCoUnT(values)` {
		t.Fatalf("range = %q, want %q", got, `MvCoUnT(values)`)
	}
}

func TestParseMVCountSupportsScalarsBooleanNestingPredicatesAndNull(t *testing.T) {
	t.Parallel()

	for _, source := range []string{
		`index=main | eval count=mvcount(values)`,
		`index=main | eval count=mvcount("one")`,
		`index=main | eval count=mvcount(42)`,
		`index=main | eval count=mvcount(true)`,
		`index=main | eval count=mvcount(isnull(optional))`,
		`index=main | eval count=mvcount(mvcount(values))`,
		`index=main | where mvcount(values)>2`,
		`index=main | eval count=mvcount(null)`,
	} {
		if _, err := Parse(source); err != nil {
			t.Fatalf("Parse(%q): %v", source, err)
		}
	}
}

func TestParseMVCountEnforcesExactlyOneArgument(t *testing.T) {
	t.Parallel()

	for _, source := range []string{
		`index=main | eval count=mvcount()`,
		`index=main | eval count=mvcount(first, second)`,
	} {
		assertParseDiagnosticCode(t, source, "SPL_INVALID_EVAL_ARITY")
	}
}

func TestParseBareMVCountNameRemainsField(t *testing.T) {
	t.Parallel()

	query, err := Parse(`index=main | eval copied=mvcount`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	field, ok := query.Commands[0].(*EvalCommand).Assignments[0].Expression.(*ScalarFieldExpr)
	if !ok || field.Field != "mvcount" {
		t.Fatalf("expression = %#v, want field mvcount", query.Commands[0])
	}
}
