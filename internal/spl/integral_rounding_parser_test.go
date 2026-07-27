package spl

import "testing"

func TestParseEvalIntegralRoundingPreservesAliasesAndRanges(t *testing.T) {
	t.Parallel()

	const source = `index=main | eval up=CeIl(1.2), alias=CEILING(1.2), down=FlOoR(-1.2)`
	query, err := Parse(source)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	assignments := query.Commands[0].(*EvalCommand).Assignments
	if len(assignments) != 3 {
		t.Fatalf("assignments = %#v", assignments)
	}
	for index, test := range []struct {
		source   string
		function ScalarFunction
	}{
		{source: `CeIl(1.2)`, function: ScalarFunctionCeil},
		{source: `CEILING(1.2)`, function: ScalarFunctionCeil},
		{source: `FlOoR(-1.2)`, function: ScalarFunctionFloor},
	} {
		call, ok := assignments[index].Expression.(*ScalarCallExpr)
		if !ok || call.Function != test.function || len(call.Arguments) != 1 {
			t.Fatalf("assignment %d expression = %#v", index, assignments[index].Expression)
		}
		if got := source[call.Range.Start.Offset:call.Range.End.Offset]; got != test.source {
			t.Fatalf("assignment %d range = %q, want %q", index, got, test.source)
		}
	}
}

func TestParseIntegralRoundingSupportsNestingPredicatesAndNull(t *testing.T) {
	t.Parallel()

	for _, source := range []string{
		`index=main | eval value=ceil(1.2)`,
		`index=main | eval value=ceiling(-1.2)`,
		`index=main | eval value=floor(tonumber(amount))`,
		`index=main | eval value=ceil(floor(amount))`,
		`index=main | where ceiling(ratio)=2`,
		`index=main | eval value=floor(null)`,
	} {
		if _, err := Parse(source); err != nil {
			t.Fatalf("Parse(%q): %v", source, err)
		}
	}
}

func TestParseIntegralRoundingEnforcesArityAndNonBooleanInput(t *testing.T) {
	t.Parallel()

	for _, source := range []string{
		`index=main | eval value=ceil()`,
		`index=main | eval value=ceiling(amount, 2)`,
		`index=main | eval value=floor()`,
		`index=main | eval value=floor(amount, 2)`,
	} {
		assertParseDiagnosticCode(t, source, "SPL_INVALID_EVAL_ARITY")
	}
	for _, source := range []string{
		`index=main | eval value=ceil(isnull(amount))`,
		`index=main | eval value=ceiling(isnull(amount))`,
		`index=main | eval value=floor(isnull(amount))`,
	} {
		assertParseDiagnosticCode(t, source, "SPL_UNSUPPORTED_EVAL_EXPRESSION")
	}
}

func TestParseBareIntegralRoundingNamesRemainFields(t *testing.T) {
	t.Parallel()

	query, err := Parse(`index=main | eval a=ceil, b=ceiling, c=floor`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	assignments := query.Commands[0].(*EvalCommand).Assignments
	for index, name := range []string{"ceil", "ceiling", "floor"} {
		field, ok := assignments[index].Expression.(*ScalarFieldExpr)
		if !ok || field.Field != name {
			t.Fatalf("assignment %d expression = %#v, want field %q", index, assignments[index].Expression, name)
		}
	}
}
