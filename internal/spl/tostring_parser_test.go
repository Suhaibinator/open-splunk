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

func TestParseToStringEnforcesArityAndFormats(t *testing.T) {
	t.Parallel()

	for _, source := range []string{
		`index=main | eval value=tostring()`,
		`index=main | eval value=tostring(status, "hex", "extra")`,
		`index=main | eval value=tostring(status, "commas", "extra")`,
	} {
		assertParseDiagnosticCode(t, source, "SPL_INVALID_EVAL_ARITY")
	}
	for _, source := range []string{
		`index=main | eval value=tostring(status, "hex")`,
		`index=main | eval value=tostring(status, format)`,
		`index=main | eval value=tostring(status, commas)`,
		`index=main | eval value=tostring(status, "Commas")`,
		`index=main | eval value=tostring(status, "")`,
	} {
		assertParseDiagnosticCode(t, source, "SPL_UNSUPPORTED_TOSTRING_FORMAT")
	}
	for _, source := range []string{
		`index=main | eval value=tostring(bytes, "commas")`,
		`index=main | eval value=tostring(duration, "duration")`,
	} {
		query, err := Parse(source)
		if err != nil {
			t.Fatalf("Parse(%q): %v", source, err)
		}
		call, ok := query.Commands[0].(*EvalCommand).Assignments[0].Expression.(*ScalarCallExpr)
		if !ok || call.Function != ScalarFunctionToString || len(call.Arguments) != 2 {
			t.Fatalf("Parse(%q) expression = %#v, want a two-argument tostring call", source, query.Commands[0])
		}
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
