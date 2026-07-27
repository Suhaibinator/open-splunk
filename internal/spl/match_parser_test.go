package spl

import (
	"strings"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/splregex"
)

func TestParseMatchPreservesRangeCaseAndLiteralPattern(t *testing.T) {
	t.Parallel()

	const source = `index=main | where MaTcH(message, "^error|warn")`
	query, err := Parse(source)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	predicate := query.Commands[0].(*WhereCommand).Expression.(*WhereScalarPredicateExpr)
	call, ok := predicate.Value.(*ScalarCallExpr)
	if !ok || call.Function != ScalarFunctionMatch || len(call.Arguments) != 2 {
		t.Fatalf("predicate = %#v", predicate)
	}
	pattern, ok := call.Arguments[1].(*ScalarLiteralExpr)
	if !ok || pattern.Value.Kind != LiteralKindString ||
		pattern.Value.Text != "^error|warn" || !pattern.Value.Quoted {
		t.Fatalf("pattern = %#v", call.Arguments[1])
	}
	if got := source[call.Range.Start.Offset:call.Range.End.Offset]; got != `MaTcH(message, "^error|warn")` {
		t.Fatalf("range = %q", got)
	}
}

func TestParseMatchSupportsPredicatesConditionalsNestingAndZeroWidth(t *testing.T) {
	t.Parallel()

	for _, source := range []string{
		`index=main | where match(message, "error")`,
		`index=main | where NOT match(lower(service), "^api")`,
		`index=main | where match(message, "error")=true`,
		`index=main | eval label=if(match(message, "(?i)error|warn"), "problem", "ok")`,
		`index=main | eval rendered=tostring(match(message, "^$"))`,
		`index=main | where match(message, "")`,
		`index=main | where match(message, "a*")`,
	} {
		if _, err := Parse(source); err != nil {
			t.Fatalf("Parse(%q): %v", source, err)
		}
	}
}

func TestParseMatchRejectsArityNonliteralBooleanAndUnsupportedRegex(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		source string
		code   string
	}{
		{`index=main | where match()`, "SPL_INVALID_EVAL_ARITY"},
		{`index=main | where match(message)`, "SPL_INVALID_EVAL_ARITY"},
		{`index=main | where match(message, "x", "y")`, "SPL_INVALID_EVAL_ARITY"},
		{`index=main | where match(message, pattern)`, "SPL_UNSUPPORTED_EVAL_EXPRESSION"},
		{`index=main | where match(isnull(message), "true")`, "SPL_UNSUPPORTED_EVAL_EXPRESSION"},
		{`index=main | where match(message, "(?=secret)")`, "SPL_UNSUPPORTED_REGEX"},
		{`index=main | where match(message, "(.)\1")`, "SPL_UNSUPPORTED_REGEX"},
	} {
		assertParseDiagnosticCode(t, test.source, test.code)
	}
}

func TestParseMatchBoundsOriginalAndNormalizedPattern(t *testing.T) {
	t.Parallel()

	tooLong := strings.Repeat("x", splregex.MaximumMatchPatternBytes+1)
	assertParseDiagnosticCode(
		t,
		`index=main | where match(message, "`+tooLong+`")`,
		"SPL_QUERY_TOO_COMPLEX",
	)
	assertParseDiagnosticCode(
		t,
		`index=main | where match(message, "`+strings.Repeat("a{1000}", 5)+`")`,
		"SPL_QUERY_TOO_COMPLEX",
	)
}

func TestParseBareMatchNameRemainsField(t *testing.T) {
	t.Parallel()

	query, err := Parse(`index=main | eval copied=match`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	field, ok := query.Commands[0].(*EvalCommand).Assignments[0].Expression.(*ScalarFieldExpr)
	if !ok || field.Field != "match" {
		t.Fatalf("expression = %#v, want field match", query.Commands[0])
	}
}
