package spl

import (
	"strings"
	"testing"
)

func TestParseConvertPreservesClausesAliasesFormatAndRanges(t *testing.T) {
	t.Parallel()
	source := `index=main | CoNvErT timeformat="%m/%d/%Y %H:%M:%S" CtImE(first_seen) AS "First Seen", mktime(last_seen) dur2sec(delay) num(bytes) auto(cost*) rmunit(duration) none(cost_ignore)`
	parsed, err := Parse(source)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	command := parsed.Commands[0].(*ConvertCommand)
	if command.TimeFormat != DefaultConvertTimeFormat || len(command.Clauses) != 7 {
		t.Fatalf("command = %#v", command)
	}
	for index, function := range []ConvertFunction{ConvertFunctionCTime, ConvertFunctionMktime, ConvertFunctionDur2Sec, ConvertFunctionNum, ConvertFunctionAuto, ConvertFunctionRmUnit, ConvertFunctionNone} {
		if command.Clauses[index].Function != function {
			t.Fatalf("clause %d = %#v", index, command.Clauses[index])
		}
	}
	first := command.Clauses[0]
	if first.Input != "first_seen" || first.Output != "First Seen" || !first.ExplicitOutput {
		t.Fatalf("first = %#v", first)
	}
	if got := source[first.Range.Start.Offset:first.Range.End.Offset]; got != `CtImE(first_seen) AS "First Seen"` {
		t.Fatalf("clause range = %q", got)
	}
	if got := source[command.Range.Start.Offset:command.Range.End.Offset]; got != strings.TrimPrefix(source, "index=main | ") {
		t.Fatalf("command range = %q", got)
	}
	if shape := ClassifyResultShape(parsed); shape.Kind != ResultKindEvents {
		t.Fatalf("shape = %#v", shape)
	}
}

func TestParseConvertDefaultAndInvalidSyntax(t *testing.T) {
	t.Parallel()
	parsed, err := Parse(`index=main | convert ctime(_time) ctime(other) | head 20`)
	if err != nil {
		t.Fatal(err)
	}
	if got := parsed.Commands[0].(*ConvertCommand).TimeFormat; got != DefaultConvertTimeFormat {
		t.Fatalf("format = %q", got)
	}
	for _, source := range []string{
		`convert`, `convert timeformat="%F"`, `convert ctime()`, `convert ctime(a,b)`, `convert ctime(a`,
		`convert ctime(a) AS`, `convert ctime(a) AS *`, `convert ctime(*) AS output`, `convert none(a) AS output`,
		`convert ctime("a")`, `convert ctime('a')`, `convert eval(a)`, `convert memk(a)`, `convert ctime(a),`,
		`convert timeformat=format ctime(a)`, `convert ctime(a) timeformat="%F"`, `convert ctime(now())`,
	} {
		if _, err := Parse("index=main | " + source); err == nil {
			t.Errorf("accepted %q", source)
		}
	}
	assertParseDiagnosticCode(t, `index=main | convert ctime(__os_private)`, "SPL_RESERVED_FIELD")
	assertParseDiagnosticCode(t, `index=main | convert ctime(a) AS __os_private`, "SPL_RESERVED_FIELD")
	assertParseDiagnosticCode(t, `index=main | convert timeformat="%c" ctime(a)`, "SPL_UNSUPPORTED_TIME_FORMAT")
	assertParseDiagnosticCode(t, `index=main | convert timeformat="%H:%M:%S" mktime(a)`, "SPL_UNSUPPORTED_TIME_FORMAT")
	assertParseDiagnosticCode(t, "index=main | convert "+strings.Repeat("num(a) ", MaximumConvertClauses+1), "SPL_QUERY_TOO_COMPLEX")
	if _, err := Parse("index=main | convert " + strings.Repeat("num(a) ", MaximumConvertClauses)); err != nil {
		t.Fatalf("64 clauses: %v", err)
	}
}
