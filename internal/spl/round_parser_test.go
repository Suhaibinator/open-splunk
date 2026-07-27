package spl

import "testing"

func TestParseEvalRoundPreservesRangeAndOptionalPrecision(t *testing.T) {
	t.Parallel()

	const source = `index=main | eval whole=RoUnD(3.5), cents=round(2.555, 2)`
	query, err := Parse(source)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	assignments := query.Commands[0].(*EvalCommand).Assignments
	if len(assignments) != 2 {
		t.Fatalf("assignments = %#v", assignments)
	}
	for index, test := range []struct {
		source    string
		arguments int
	}{
		{source: `RoUnD(3.5)`, arguments: 1},
		{source: `round(2.555, 2)`, arguments: 2},
	} {
		call, ok := assignments[index].Expression.(*ScalarCallExpr)
		if !ok || call.Function != ScalarFunctionRound ||
			len(call.Arguments) != test.arguments {
			t.Fatalf("assignment %d expression = %#v", index, assignments[index].Expression)
		}
		if got := source[call.Range.Start.Offset:call.Range.End.Offset]; got != test.source {
			t.Fatalf("assignment %d range = %q, want %q", index, got, test.source)
		}
	}
}

func TestParseRoundSupportsNestingPredicatesAndBoundedPrecision(t *testing.T) {
	t.Parallel()

	for _, source := range []string{
		`index=main | eval value=round(3.5)`,
		`index=main | eval value=round(-2.555, 2)`,
		`index=main | eval value=round(tonumber(amount), 18)`,
		`index=main | where round(ratio, 2)=1.25`,
		`index=main | eval value=round(null)`,
		`index=main | eval value=round(1.25, +2)`,
	} {
		if _, err := Parse(source); err != nil {
			t.Fatalf("Parse(%q): %v", source, err)
		}
	}
}

func TestParseRoundEnforcesArityNumericInputAndLiteralPrecision(t *testing.T) {
	t.Parallel()

	for _, source := range []string{
		`index=main | eval value=round()`,
		`index=main | eval value=round(amount, 2, 3)`,
	} {
		assertParseDiagnosticCode(t, source, "SPL_INVALID_EVAL_ARITY")
	}
	assertParseDiagnosticCode(
		t,
		`index=main | eval value=round(isnull(amount))`,
		"SPL_UNSUPPORTED_EVAL_EXPRESSION",
	)
	for _, source := range []string{
		`index=main | eval value=round(amount, precision)`,
		`index=main | eval value=round(amount, 1.5)`,
		`index=main | eval value=round(amount, -1)`,
		`index=main | eval value=round(amount, 19)`,
		`index=main | eval value=round(amount, 18446744073709551615)`,
		`index=main | eval value=round(amount, true)`,
		`index=main | eval value=round(amount, null)`,
	} {
		assertParseDiagnosticCode(t, source, "SPL_UNSUPPORTED_ROUND_PRECISION")
	}
}

func TestParseBareRoundNameRemainsField(t *testing.T) {
	t.Parallel()

	query, err := Parse(`index=main | eval copied=round`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	field, ok := query.Commands[0].(*EvalCommand).Assignments[0].Expression.(*ScalarFieldExpr)
	if !ok || field.Field != "round" {
		t.Fatalf("expression = %#v, want field round", query.Commands[0])
	}
}
