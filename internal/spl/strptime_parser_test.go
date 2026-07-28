package spl

import (
	"strings"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/spltimeformat"
)

func TestParseStrptimePreservesRangeCaseAndLiteralFormat(t *testing.T) {
	t.Parallel()

	const source = `index=main | eval epoch=StRpTiMe(strftime(now(), "%F %T"), "%F %T")`
	query, err := Parse(source)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	call, ok := query.Commands[0].(*EvalCommand).Assignments[0].Expression.(*ScalarCallExpr)
	if !ok || call.Function != ScalarFunctionStrptime || len(call.Arguments) != 2 {
		t.Fatalf("expression = %#v", query.Commands[0])
	}
	formatted, ok := call.Arguments[0].(*ScalarCallExpr)
	if !ok || formatted.Function != ScalarFunctionStrftime {
		t.Fatalf("text argument = %#v", call.Arguments[0])
	}
	format, ok := call.Arguments[1].(*ScalarLiteralExpr)
	if !ok || format.Value.Kind != LiteralKindString ||
		format.Value.Text != "%F %T" || !format.Value.Quoted {
		t.Fatalf("format = %#v", call.Arguments[1])
	}
	if got := source[call.Range.Start.Offset:call.Range.End.Offset]; got !=
		`StRpTiMe(strftime(now(), "%F %T"), "%F %T")` {
		t.Fatalf("range = %q", got)
	}
}

func TestParseStrptimeSupportsStringScalarContexts(t *testing.T) {
	t.Parallel()

	for _, source := range []string{
		`index=main | eval epoch=strptime("2026-07-27", "%F")`,
		`index=main | eval epoch=strptime(timestamp, "%Y-%m-%dT%T")`,
		`index=main | eval rendered=tostring(strptime(timestamp, "%F"))`,
		`index=main | where strptime(timestamp, "%F")>=0`,
		`index=main | eval epoch=if(isnull(timestamp), null, strptime(timestamp, "%F"))`,
	} {
		if _, err := Parse(source); err != nil {
			t.Fatalf("Parse(%q): %v", source, err)
		}
	}
}

func TestParseStrptimeRejectsArityNonliteralBooleanAndInvalidFormats(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		source string
		code   string
	}{
		{`index=main | eval epoch=strptime()`, "SPL_INVALID_EVAL_ARITY"},
		{`index=main | eval epoch=strptime(timestamp)`, "SPL_INVALID_EVAL_ARITY"},
		{`index=main | eval epoch=strptime(timestamp, "%F", "%T")`, "SPL_INVALID_EVAL_ARITY"},
		{`index=main | eval epoch=strptime(timestamp, format)`, "SPL_UNSUPPORTED_EVAL_EXPRESSION"},
		{`index=main | eval epoch=strptime(timestamp, 1)`, "SPL_UNSUPPORTED_EVAL_EXPRESSION"},
		{`index=main | eval epoch=strptime(isnull(timestamp), "%F")`, "SPL_UNSUPPORTED_EVAL_EXPRESSION"},
		{`index=main | eval epoch=strptime(timestamp, "%Y-%m")`, "SPL_UNSUPPORTED_TIME_FORMAT"},
		{`index=main | eval epoch=strptime(timestamp, "%Y-%m-%d %H:%M:%S.%N")`, "SPL_UNSUPPORTED_TIME_FORMAT"},
		{`index=main | eval epoch=strptime(timestamp, "%Y-%m-%d %:z")`, "SPL_UNSUPPORTED_TIME_FORMAT"},
		{
			`index=main | eval epoch=strptime(timestamp, "` +
				strings.Repeat("%F", 1700) + `")`,
			"SPL_UNSUPPORTED_TIME_FORMAT",
		},
		{"index=main | eval epoch=strptime(timestamp, \"bad\x00format\")", "SPL_UNSUPPORTED_TIME_FORMAT"},
	} {
		assertParseDiagnosticCode(t, test.source, test.code)
	}
}

func TestParseStrptimeBoundsFormatResources(t *testing.T) {
	t.Parallel()

	tooLong := strings.Repeat("x", spltimeformat.MaximumStrptimeFormatBytes+1)
	assertParseDiagnosticCode(
		t,
		`index=main | eval epoch=strptime(timestamp, "`+tooLong+`")`,
		"SPL_QUERY_TOO_COMPLEX",
	)
}

func TestParseBareStrptimeNameRemainsField(t *testing.T) {
	t.Parallel()

	query, err := Parse(`index=main | eval copied=strptime`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	field, ok := query.Commands[0].(*EvalCommand).Assignments[0].Expression.(*ScalarFieldExpr)
	if !ok || field.Field != "strptime" {
		t.Fatalf("expression = %#v, want field strptime", query.Commands[0])
	}
}
