package spl

import (
	"strings"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/splwildcard"
)

func TestParseLikePreservesRangeCaseAndLiteralPattern(t *testing.T) {
	t.Parallel()

	const source = `index=main | where LiKe(message, "error%_")`
	query, err := Parse(source)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	predicate := query.Commands[0].(*WhereCommand).Expression.(*WhereScalarPredicateExpr)
	call, ok := predicate.Value.(*ScalarCallExpr)
	if !ok || call.Function != ScalarFunctionLike || len(call.Arguments) != 2 {
		t.Fatalf("predicate = %#v", predicate)
	}
	pattern, ok := call.Arguments[1].(*ScalarLiteralExpr)
	if !ok || pattern.Value.Kind != LiteralKindString ||
		pattern.Value.Text != "error%_" || !pattern.Value.Quoted {
		t.Fatalf("pattern = %#v", call.Arguments[1])
	}
	if got := source[call.Range.Start.Offset:call.Range.End.Offset]; got != `LiKe(message, "error%_")` {
		t.Fatalf("range = %q", got)
	}
}

func TestParseLikeSupportsPredicatesConditionalsNestingAndEscapes(t *testing.T) {
	t.Parallel()

	for _, source := range []string{
		`index=main | where like(message, "%error%")`,
		`index=main | where NOT like(lower(service), "api%")`,
		`index=main | where like(message, "error%")=true`,
		`index=main | eval label=if(like(message, "%error%"), "problem", "ok")`,
		`index=main | eval rendered=tostring(like(message, ""))`,
		`index=main | where like(message, "\%\_\\\\")`,
	} {
		if _, err := Parse(source); err != nil {
			t.Fatalf("Parse(%q): %v", source, err)
		}
	}
}

func TestParseLikeRejectsArityNonliteralBooleanAndInvalidPattern(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		source string
		code   string
	}{
		{`index=main | where like()`, "SPL_INVALID_EVAL_ARITY"},
		{`index=main | where like(message)`, "SPL_INVALID_EVAL_ARITY"},
		{`index=main | where like(message, "x", "y")`, "SPL_INVALID_EVAL_ARITY"},
		{`index=main | where like(message, pattern)`, "SPL_UNSUPPORTED_EVAL_EXPRESSION"},
		{`index=main | where like(isnull(message), "true")`, "SPL_UNSUPPORTED_EVAL_EXPRESSION"},
		{`index=main | where like(message, "bad\\")`, "SPL_UNSUPPORTED_LIKE_PATTERN"},
		{"index=main | where like(message, \"bad\x00pattern\")", "SPL_UNSUPPORTED_LIKE_PATTERN"},
	} {
		assertParseDiagnosticCode(t, test.source, test.code)
	}
}

func TestParseLikeBoundsPatternBytes(t *testing.T) {
	t.Parallel()

	tooLong := strings.Repeat("x", splwildcard.MaximumLikePatternBytes+1)
	assertParseDiagnosticCode(
		t,
		`index=main | where like(message, "`+tooLong+`")`,
		"SPL_QUERY_TOO_COMPLEX",
	)
}

func TestParseBareLikeNameRemainsField(t *testing.T) {
	t.Parallel()

	query, err := Parse(`index=main | eval copied=like`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	field, ok := query.Commands[0].(*EvalCommand).Assignments[0].Expression.(*ScalarFieldExpr)
	if !ok || field.Field != "like" {
		t.Fatalf("expression = %#v, want field like", query.Commands[0])
	}
}
