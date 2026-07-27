package spl

import "testing"

func TestParseEvalSubstringPreservesRangeAndTypedArguments(t *testing.T) {
	t.Parallel()

	const source = `index=main | eval middle=SuBsTr("😀abcdef", -4, 3), suffix=substr(source, 2)`
	query, err := Parse(source)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	assignments := query.Commands[0].(*EvalCommand).Assignments
	if len(assignments) != 2 {
		t.Fatalf("assignments = %#v", assignments)
	}

	first, ok := assignments[0].Expression.(*ScalarCallExpr)
	if !ok || first.Function != ScalarFunctionSubstring || len(first.Arguments) != 3 {
		t.Fatalf("first substring = %#v", assignments[0].Expression)
	}
	if got, want := source[first.Range.Start.Offset:first.Range.End.Offset], `SuBsTr("😀abcdef", -4, 3)`; got != want {
		t.Fatalf("first range = %q, want %q", got, want)
	}
	if literal, ok := first.Arguments[0].(*ScalarLiteralExpr); !ok ||
		literal.Value.Kind != LiteralKindString || literal.Value.Text != "😀abcdef" {
		t.Fatalf("first value = %#v", first.Arguments[0])
	}
	for index, want := range []string{"-4", "3"} {
		literal, ok := first.Arguments[index+1].(*ScalarLiteralExpr)
		if !ok || literal.Value.Kind != LiteralKindInteger || literal.Value.Text != want {
			t.Fatalf("first index argument %d = %#v, want integer %q", index, first.Arguments[index+1], want)
		}
	}

	second, ok := assignments[1].Expression.(*ScalarCallExpr)
	if !ok || second.Function != ScalarFunctionSubstring || len(second.Arguments) != 2 {
		t.Fatalf("second substring = %#v", assignments[1].Expression)
	}
	if got, want := source[second.Range.Start.Offset:second.Range.End.Offset], `substr(source, 2)`; got != want {
		t.Fatalf("second range = %q, want %q", got, want)
	}
}

func TestParseSubstringSupportsNestingPredicateAndSignedIntegerEdges(t *testing.T) {
	t.Parallel()

	for _, source := range []string{
		`index=main | eval value=substr(lower(message), 0, 3)`,
		`index=main | eval value=upper(substr(message, -3))`,
		`index=main | where substr(source, 1, 3)="api"`,
		`index=main | eval value=substr(message, -9223372036854775808, 18446744073709551615)`,
		`index=main | eval value=substr(null, +1, 0)`,
	} {
		if _, err := Parse(source); err != nil {
			t.Fatalf("Parse(%q): %v", source, err)
		}
	}

	for _, source := range []string{
		`index=main | eval value=substr(isnull(optional), 1)`,
		`index=main | eval value=substr(coalesce(isnotnull(optional), false), 1)`,
	} {
		assertParseDiagnosticCode(t, source, "SPL_UNSUPPORTED_EVAL_EXPRESSION")
	}
}

func TestParseSubstringEnforcesArityAndLiteralIntegerIndexes(t *testing.T) {
	t.Parallel()

	for _, source := range []string{
		`index=main | eval value=substr()`,
		`index=main | eval value=substr(message)`,
		`index=main | eval value=substr(message, 1, 2, 3)`,
	} {
		assertParseDiagnosticCode(t, source, "SPL_INVALID_EVAL_ARITY")
	}

	for _, source := range []string{
		`index=main | eval value=substr(message, offset)`,
		`index=main | eval value=substr(message, 1.5)`,
		`index=main | eval value=substr(message, 1, span)`,
		`index=main | eval value=substr(message, 1, tonumber(span))`,
		`index=main | eval value=substr(message, true, 2)`,
	} {
		assertParseDiagnosticCode(t, source, "SPL_UNSUPPORTED_SUBSTRING_INDEX")
	}
}

func TestParseBareSubstringNameRemainsField(t *testing.T) {
	t.Parallel()

	query, err := Parse(`index=main | eval copied=substr`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	field, ok := query.Commands[0].(*EvalCommand).Assignments[0].Expression.(*ScalarFieldExpr)
	if !ok || field.Field != "substr" {
		t.Fatalf("expression = %#v, want field substr", query.Commands[0])
	}
}
