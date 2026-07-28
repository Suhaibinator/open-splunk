package spl

import (
	"strings"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/spltimeformat"
)

func TestParseStrftimePreservesRangeCaseAndLiteralFormat(t *testing.T) {
	t.Parallel()

	const source = `index=main | eval rendered=StRfTiMe(now(), "%Y-%m-%dT%H:%M:%S.%Q")`
	query, err := Parse(source)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	call, ok := query.Commands[0].(*EvalCommand).Assignments[0].Expression.(*ScalarCallExpr)
	if !ok || call.Function != ScalarFunctionStrftime || len(call.Arguments) != 2 {
		t.Fatalf("expression = %#v", query.Commands[0])
	}
	if call.Arguments[0].(*ScalarCallExpr).Function != ScalarFunctionNow {
		t.Fatalf("time argument = %#v", call.Arguments[0])
	}
	format, ok := call.Arguments[1].(*ScalarLiteralExpr)
	if !ok || format.Value.Kind != LiteralKindString ||
		format.Value.Text != "%Y-%m-%dT%H:%M:%S.%Q" || !format.Value.Quoted {
		t.Fatalf("format = %#v", call.Arguments[1])
	}
	if got := source[call.Range.Start.Offset:call.Range.End.Offset]; got !=
		`StRfTiMe(now(), "%Y-%m-%dT%H:%M:%S.%Q")` {
		t.Fatalf("range = %q", got)
	}
}

func TestParseStrftimeSupportsScalarContexts(t *testing.T) {
	t.Parallel()

	for _, source := range []string{
		`index=main | eval rendered=strftime(_time, "%F %T %z")`,
		`index=main | eval rendered=strftime(0, "%s")`,
		`index=main | eval rendered=tostring(strftime(now(), "%9N"))`,
		`index=main | where strftime(_time, "%Y")="2026"`,
		`index=main | eval rendered=if(isnull(_time), "unknown", strftime(_time, "%F"))`,
	} {
		if _, err := Parse(source); err != nil {
			t.Fatalf("Parse(%q): %v", source, err)
		}
	}
}

func TestParseStrftimeRejectsArityNonliteralBooleanAndInvalidFormats(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		source string
		code   string
	}{
		{`index=main | eval rendered=strftime()`, "SPL_INVALID_EVAL_ARITY"},
		{`index=main | eval rendered=strftime(_time)`, "SPL_INVALID_EVAL_ARITY"},
		{`index=main | eval rendered=strftime(_time, "%F", "%T")`, "SPL_INVALID_EVAL_ARITY"},
		{`index=main | eval rendered=strftime(_time, format)`, "SPL_UNSUPPORTED_EVAL_EXPRESSION"},
		{`index=main | eval rendered=strftime(_time, 1)`, "SPL_UNSUPPORTED_EVAL_EXPRESSION"},
		{`index=main | eval rendered=strftime(isnull(_time), "%F")`, "SPL_UNSUPPORTED_EVAL_EXPRESSION"},
		{`index=main | eval rendered=strftime(_time, "%")`, "SPL_UNSUPPORTED_TIME_FORMAT"},
		{`index=main | eval rendered=strftime(_time, "%c")`, "SPL_UNSUPPORTED_TIME_FORMAT"},
		{`index=main | eval rendered=strftime(_time, "%Z")`, "SPL_UNSUPPORTED_TIME_FORMAT"},
		{"index=main | eval rendered=strftime(_time, \"bad\x00format\")", "SPL_UNSUPPORTED_TIME_FORMAT"},
	} {
		assertParseDiagnosticCode(t, test.source, test.code)
	}
}

func TestParseStrftimeBoundsFormatResources(t *testing.T) {
	t.Parallel()

	tooLong := strings.Repeat("x", spltimeformat.MaximumStrftimeFormatBytes+1)
	assertParseDiagnosticCode(
		t,
		`index=main | eval rendered=strftime(_time, "`+tooLong+`")`,
		"SPL_QUERY_TOO_COMPLEX",
	)
	amplifying := strings.Repeat(
		"%s",
		spltimeformat.MaximumStrftimeOutputBytes/20+1,
	)
	assertParseDiagnosticCode(
		t,
		`index=main | eval rendered=strftime(_time, "`+amplifying+`")`,
		"SPL_QUERY_TOO_COMPLEX",
	)
}

func TestParseBareStrftimeNameRemainsField(t *testing.T) {
	t.Parallel()

	query, err := Parse(`index=main | eval copied=strftime`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	field, ok := query.Commands[0].(*EvalCommand).Assignments[0].Expression.(*ScalarFieldExpr)
	if !ok || field.Field != "strftime" {
		t.Fatalf("expression = %#v, want field strftime", query.Commands[0])
	}
}
