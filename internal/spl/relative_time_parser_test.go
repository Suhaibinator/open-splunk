package spl

import (
	"strings"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/splrelativetime"
)

func TestParseRelativeTimePreservesRangeCaseAndLiteralSpecifier(t *testing.T) {
	t.Parallel()

	const source = `index=main | eval shifted=ReLaTiVe_TiMe(strptime("2026-07-27", "%F"), "-1d@d+2h")`
	query, err := Parse(source)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	call, ok := query.Commands[0].(*EvalCommand).Assignments[0].Expression.(*ScalarCallExpr)
	if !ok || call.Function != ScalarFunctionRelativeTime || len(call.Arguments) != 2 {
		t.Fatalf("expression = %#v", query.Commands[0])
	}
	parsed, ok := call.Arguments[0].(*ScalarCallExpr)
	if !ok || parsed.Function != ScalarFunctionStrptime {
		t.Fatalf("time argument = %#v", call.Arguments[0])
	}
	specifier, ok := call.Arguments[1].(*ScalarLiteralExpr)
	if !ok || specifier.Value.Kind != LiteralKindString ||
		specifier.Value.Text != "-1d@d+2h" || !specifier.Value.Quoted {
		t.Fatalf("specifier = %#v", call.Arguments[1])
	}
	if got := source[call.Range.Start.Offset:call.Range.End.Offset]; got !=
		`ReLaTiVe_TiMe(strptime("2026-07-27", "%F"), "-1d@d+2h")` {
		t.Fatalf("range = %q", got)
	}
}

func TestParseRelativeTimeSupportsNumericScalarContexts(t *testing.T) {
	t.Parallel()

	for _, source := range []string{
		`index=main | eval shifted=relative_time(_time, "-1d@d")`,
		`index=main | eval shifted=relative_time(now(), "-2h@h")`,
		`index=main | where relative_time(_time, "@h")>=0`,
		`index=main | eval rendered=tostring(relative_time(_time, "@s"))`,
		`index=main | eval rendered=strftime(relative_time(_time, "+1mon@mon"), "%F %T")`,
		`index=main | eval shifted=relative_time(strptime(timestamp, "%F"), "-1w@w1+2d")`,
		`index=main | eval shifted=relative_time(_time, "+0s")`,
	} {
		if _, err := Parse(source); err != nil {
			t.Fatalf("Parse(%q): %v", source, err)
		}
	}
}

func TestParseRelativeTimeRejectsArityNonliteralBooleanAndInvalidSpecifiers(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		source string
		code   string
	}{
		{`index=main | eval shifted=relative_time()`, "SPL_INVALID_EVAL_ARITY"},
		{`index=main | eval shifted=relative_time(_time)`, "SPL_INVALID_EVAL_ARITY"},
		{`index=main | eval shifted=relative_time(_time, "@d", "+1h")`, "SPL_INVALID_EVAL_ARITY"},
		{`index=main | eval shifted=relative_time(_time, specifier)`, "SPL_UNSUPPORTED_EVAL_EXPRESSION"},
		{`index=main | eval shifted=relative_time(_time, 1)`, "SPL_UNSUPPORTED_EVAL_EXPRESSION"},
		{`index=main | eval shifted=relative_time(isnull(_time), "@d")`, "SPL_UNSUPPORTED_EVAL_EXPRESSION"},
		{`index=main | eval shifted=relative_time(_time, "")`, "SPL_UNSUPPORTED_RELATIVE_TIME_SPECIFIER"},
		{`index=main | eval shifted=relative_time(_time, "1d")`, "SPL_UNSUPPORTED_RELATIVE_TIME_SPECIFIER"},
		{`index=main | eval shifted=relative_time(_time, "@w8")`, "SPL_UNSUPPORTED_RELATIVE_TIME_SPECIFIER"},
		{`index=main | eval shifted=relative_time(_time, "-1d@d+2h+1s")`, "SPL_UNSUPPORTED_RELATIVE_TIME_SPECIFIER"},
		{`index=main | eval shifted=relative_time(_time, "-1D@D")`, "SPL_UNSUPPORTED_RELATIVE_TIME_SPECIFIER"},
		{"index=main | eval shifted=relative_time(_time, \"@d\x00\")", "SPL_UNSUPPORTED_RELATIVE_TIME_SPECIFIER"},
		{`index=main | eval shifted=relative_time(_time, "+363y")`, "SPL_NUMBER_OUT_OF_RANGE"},
		{`index=main | eval shifted=relative_time(_time, "+18446744073709551616s")`, "SPL_NUMBER_OUT_OF_RANGE"},
	} {
		assertParseDiagnosticCode(t, test.source, test.code)
	}
}

func TestParseRelativeTimeBoundsSpecifierResources(t *testing.T) {
	t.Parallel()

	tooLong := strings.Repeat("s", splrelativetime.MaximumSpecifierBytes+1)
	assertParseDiagnosticCode(
		t,
		`index=main | eval shifted=relative_time(_time, "+1`+tooLong+`")`,
		"SPL_QUERY_TOO_COMPLEX",
	)
}

func TestParseBareRelativeTimeNameRemainsField(t *testing.T) {
	t.Parallel()

	query, err := Parse(`index=main | eval copied=relative_time`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	field, ok := query.Commands[0].(*EvalCommand).Assignments[0].Expression.(*ScalarFieldExpr)
	if !ok || field.Field != "relative_time" {
		t.Fatalf("expression = %#v, want field relative_time", query.Commands[0])
	}
}
