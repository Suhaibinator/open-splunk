package spl

import "testing"

func TestParseEvalLowerUpperPreservesFunctionRangeAndArgument(t *testing.T) {
	t.Parallel()

	const source = `index=main | eval folded=LOWER("MÜNCHEN"), shouted=upper(source)`
	query, err := Parse(source)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	command := query.Commands[0].(*EvalCommand)
	if len(command.Assignments) != 2 {
		t.Fatalf("assignments = %#v", command.Assignments)
	}

	lower, ok := command.Assignments[0].Expression.(*ScalarCallExpr)
	if !ok || lower.Function != ScalarFunctionLower || len(lower.Arguments) != 1 {
		t.Fatalf("lower expression = %#v", command.Assignments[0].Expression)
	}
	literal, ok := lower.Arguments[0].(*ScalarLiteralExpr)
	if !ok || literal.Value.Kind != LiteralKindString ||
		literal.Value.Text != "MÜNCHEN" {
		t.Fatalf("lower argument = %#v", lower.Arguments[0])
	}
	if got := source[lower.Range.Start.Offset:lower.Range.End.Offset]; got !=
		`LOWER("MÜNCHEN")` {
		t.Fatalf("lower range = %q", got)
	}

	upper, ok := command.Assignments[1].Expression.(*ScalarCallExpr)
	if !ok || upper.Function != ScalarFunctionUpper || len(upper.Arguments) != 1 {
		t.Fatalf("upper expression = %#v", command.Assignments[1].Expression)
	}
	field, ok := upper.Arguments[0].(*ScalarFieldExpr)
	if !ok || field.Field != "source" {
		t.Fatalf("upper argument = %#v", upper.Arguments[0])
	}
}

func TestParseLowerUpperSupportNestingAndRejectBooleanFunctionResults(t *testing.T) {
	t.Parallel()

	for _, source := range []string{
		`index=main | eval folded=lower(upper(message))`,
		`index=main | where lower(source)="api"`,
		`index=main | eval selected=if(status=200, lower(source), upper(source))`,
		`index=main | eval text=lower(true)`,
	} {
		if _, err := Parse(source); err != nil {
			t.Fatalf("Parse(%q): %v", source, err)
		}
	}

	for _, source := range []string{
		`index=main | eval text=lower(isnull(optional))`,
		`index=main | eval text=upper(coalesce(isnotnull(optional), false))`,
	} {
		assertParseDiagnosticCode(t, source, "SPL_UNSUPPORTED_EVAL_EXPRESSION")
	}
}

func TestParseLowerUpperEnforceExactArity(t *testing.T) {
	t.Parallel()

	for _, source := range []string{
		`index=main | eval text=lower()`,
		`index=main | eval text=lower(first, second)`,
		`index=main | eval text=upper()`,
		`index=main | eval text=upper(first, second)`,
	} {
		assertParseDiagnosticCode(t, source, "SPL_INVALID_EVAL_ARITY")
	}
}

func TestParseBareLowerUpperNamesRemainFields(t *testing.T) {
	t.Parallel()

	query, err := Parse(`index=main | eval copied_lower=lower, copied_upper=upper`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	assignments := query.Commands[0].(*EvalCommand).Assignments
	for index, want := range []string{"lower", "upper"} {
		field, ok := assignments[index].Expression.(*ScalarFieldExpr)
		if !ok || field.Field != want {
			t.Fatalf("assignment %d expression = %#v, want field %q", index, assignments[index].Expression, want)
		}
	}
}
