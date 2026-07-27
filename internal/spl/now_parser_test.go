package spl

import "testing"

func TestParseNowPreservesRangeCaseAndZeroArity(t *testing.T) {
	t.Parallel()

	const source = `index=main | eval started=NoW(), rendered=tostring(now())`
	query, err := Parse(source)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	eval := query.Commands[0].(*EvalCommand)
	call, ok := eval.Assignments[0].Expression.(*ScalarCallExpr)
	if !ok || call.Function != ScalarFunctionNow || len(call.Arguments) != 0 {
		t.Fatalf("expression = %#v", eval.Assignments[0].Expression)
	}
	if got := source[call.Range.Start.Offset:call.Range.End.Offset]; got != `NoW()` {
		t.Fatalf("range = %q", got)
	}
	nested := eval.Assignments[1].Expression.(*ScalarCallExpr)
	if nested.Function != ScalarFunctionToString ||
		nested.Arguments[0].(*ScalarCallExpr).Function != ScalarFunctionNow {
		t.Fatalf("nested expression = %#v", nested)
	}
}

func TestParseNowSupportsPredicatesAndConditionals(t *testing.T) {
	t.Parallel()

	for _, source := range []string{
		`index=main | where now()>0`,
		`index=main | eval started=now() | where now()=started`,
		`index=main | eval state=if(now()>0, "started", "invalid")`,
		`index=main | eval rounded=floor(now())`,
	} {
		if _, err := Parse(source); err != nil {
			t.Fatalf("Parse(%q): %v", source, err)
		}
	}
}

func TestParseNowRejectsArguments(t *testing.T) {
	t.Parallel()

	for _, source := range []string{
		`index=main | eval started=now(1)`,
		`index=main | where now(message)>0`,
		`index=main | eval started=now(1, 2)`,
	} {
		assertParseDiagnosticCode(t, source, "SPL_INVALID_EVAL_ARITY")
	}
}

func TestParseBareNowNameRemainsField(t *testing.T) {
	t.Parallel()

	query, err := Parse(`index=main | eval copied=now`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	field, ok := query.Commands[0].(*EvalCommand).Assignments[0].Expression.(*ScalarFieldExpr)
	if !ok || field.Field != "now" {
		t.Fatalf("expression = %#v, want field now", query.Commands[0])
	}
}
