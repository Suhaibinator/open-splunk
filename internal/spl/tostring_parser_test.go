package spl

import "testing"

func TestParseEvalToStringPreservesRangeAndArgument(t *testing.T) {
	t.Parallel()

	const source = `index=main | eval number=ToStRiNg(123), flag=tostring(isnull(optional))`
	query, err := Parse(source)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	assignments := query.Commands[0].(*EvalCommand).Assignments
	if len(assignments) != 2 {
		t.Fatalf("assignments = %#v", assignments)
	}
	for index, wantSource := range []string{
		`ToStRiNg(123)`,
		`tostring(isnull(optional))`,
	} {
		call, ok := assignments[index].Expression.(*ScalarCallExpr)
		if !ok || call.Function != ScalarFunctionToString ||
			len(call.Arguments) != 1 {
			t.Fatalf("assignment %d expression = %#v", index, assignments[index].Expression)
		}
		if got := source[call.Range.Start.Offset:call.Range.End.Offset]; got != wantSource {
			t.Fatalf("assignment %d range = %q, want %q", index, got, wantSource)
		}
	}
}

func TestParseToStringSupportsNestingPredicatesAndScalarKinds(t *testing.T) {
	t.Parallel()

	for _, source := range []string{
		`index=main | eval value=tostring("München")`,
		`index=main | eval value=tostring(123.5)`,
		`index=main | eval value=tostring(true)`,
		`index=main | eval value=lower(tostring(status))`,
		`index=main | where tostring(status)="500"`,
		`index=main | eval value=tostring(null)`,
	} {
		if _, err := Parse(source); err != nil {
			t.Fatalf("Parse(%q): %v", source, err)
		}
	}
}

func TestParseToStringEnforcesDefaultOnlyArity(t *testing.T) {
	t.Parallel()

	for _, source := range []string{
		`index=main | eval value=tostring()`,
		`index=main | eval value=tostring(status, "hex", "extra")`,
	} {
		assertParseDiagnosticCode(t, source, "SPL_INVALID_EVAL_ARITY")
	}
	for _, source := range []string{
		`index=main | eval value=tostring(status, "hex")`,
		`index=main | eval value=tostring(status, format)`,
	} {
		assertParseDiagnosticCode(t, source, "SPL_UNSUPPORTED_TOSTRING_FORMAT")
	}
}

func TestParseBareToStringNameRemainsField(t *testing.T) {
	t.Parallel()

	query, err := Parse(`index=main | eval copied=tostring`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	field, ok := query.Commands[0].(*EvalCommand).Assignments[0].Expression.(*ScalarFieldExpr)
	if !ok || field.Field != "tostring" {
		t.Fatalf("expression = %#v, want field tostring", query.Commands[0])
	}
}
