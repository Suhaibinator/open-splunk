package spl

import "testing"

func TestParseEvalLenLengthPreservesAliasRangeAndArgument(t *testing.T) {
	t.Parallel()

	const source = `index=main | eval short=LEN("München"), long=LeNgTh(source)`
	query, err := Parse(source)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	command := query.Commands[0].(*EvalCommand)
	if len(command.Assignments) != 2 {
		t.Fatalf("assignments = %#v", command.Assignments)
	}

	for index, wantSource := range []string{`LEN("München")`, `LeNgTh(source)`} {
		call, ok := command.Assignments[index].Expression.(*ScalarCallExpr)
		if !ok || call.Function != ScalarFunctionLength || len(call.Arguments) != 1 {
			t.Fatalf("assignment %d expression = %#v", index, command.Assignments[index].Expression)
		}
		if got := source[call.Range.Start.Offset:call.Range.End.Offset]; got != wantSource {
			t.Fatalf("assignment %d range = %q, want %q", index, got, wantSource)
		}
	}

	literal, ok := command.Assignments[0].Expression.(*ScalarCallExpr).Arguments[0].(*ScalarLiteralExpr)
	if !ok || literal.Value.Kind != LiteralKindString || literal.Value.Text != "München" {
		t.Fatalf("len literal argument = %#v", command.Assignments[0].Expression)
	}
	field, ok := command.Assignments[1].Expression.(*ScalarCallExpr).Arguments[0].(*ScalarFieldExpr)
	if !ok || field.Field != "source" {
		t.Fatalf("length field argument = %#v", command.Assignments[1].Expression)
	}
}

func TestParseLenLengthSupportNestingAndRejectBooleanFunctionResults(t *testing.T) {
	t.Parallel()

	for _, source := range []string{
		`index=main | eval size=len(lower(message))`,
		`index=main | where length(source)>3`,
		`index=main | eval selected=if(length(source)>3, len(source), 0)`,
		`index=main | eval size=len(null)`,
	} {
		if _, err := Parse(source); err != nil {
			t.Fatalf("Parse(%q): %v", source, err)
		}
	}

	for _, source := range []string{
		`index=main | eval size=len(isnull(optional))`,
		`index=main | eval size=length(coalesce(isnotnull(optional), false))`,
	} {
		assertParseDiagnosticCode(t, source, "SPL_UNSUPPORTED_EVAL_EXPRESSION")
	}
}

func TestParseLenLengthEnforceExactArity(t *testing.T) {
	t.Parallel()

	for _, source := range []string{
		`index=main | eval size=len()`,
		`index=main | eval size=len(first, second)`,
		`index=main | eval size=length()`,
		`index=main | eval size=length(first, second)`,
	} {
		assertParseDiagnosticCode(t, source, "SPL_INVALID_EVAL_ARITY")
	}
}

func TestParseBareLenLengthNamesRemainFields(t *testing.T) {
	t.Parallel()

	query, err := Parse(`index=main | eval copied_len=len, copied_length=length`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	assignments := query.Commands[0].(*EvalCommand).Assignments
	for index, want := range []string{"len", "length"} {
		field, ok := assignments[index].Expression.(*ScalarFieldExpr)
		if !ok || field.Field != want {
			t.Fatalf("assignment %d expression = %#v, want field %q", index, assignments[index].Expression, want)
		}
	}
}
