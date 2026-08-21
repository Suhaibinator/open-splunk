package spl

import (
	"strings"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/splregex"
)

func TestParseNativeMultivalueFunctionsPreservesTypedCalls(t *testing.T) {
	t.Parallel()

	const source = `index=main | eval ` +
		`parts=SpLiT(message, ""), ` +
		`all=mvappend(parts, 7, true, null), ` +
		`unique=mvdedup(all), ` +
		`one=mvindex(unique, -1), ` +
		`many=mvindex(unique, 0, 2), ` +
		`joined=mvjoin(unique, "|"), ` +
		`zipped=mvzip(parts, unique), ` +
		`found=mvfind(unique, "(?i)^api")`
	query, err := Parse(source)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	assignments := query.Commands[0].(*EvalCommand).Assignments
	want := []struct {
		function ScalarFunction
		arity    int
	}{
		{ScalarFunctionSplit, 2},
		{ScalarFunctionMVAppend, 4},
		{ScalarFunctionMVDedup, 1},
		{ScalarFunctionMVIndex, 2},
		{ScalarFunctionMVIndex, 3},
		{ScalarFunctionMVJoin, 2},
		{ScalarFunctionMVZip, 2},
		{ScalarFunctionMVFind, 2},
	}
	if len(assignments) != len(want) {
		t.Fatalf("assignments = %#v", assignments)
	}
	for index, expected := range want {
		call, ok := assignments[index].Expression.(*ScalarCallExpr)
		if !ok || call.Function != expected.function ||
			len(call.Arguments) != expected.arity {
			t.Fatalf("assignment %d = %#v, want function %d/%d arguments",
				index, assignments[index].Expression, expected.function, expected.arity)
		}
	}
	first := assignments[0].Expression.(*ScalarCallExpr)
	if got := source[first.Range.Start.Offset:first.Range.End.Offset]; got != `SpLiT(message, "")` {
		t.Fatalf("split range = %q", got)
	}
	for argumentIndex, text := range []string{"-1", "0", "2"} {
		assignmentIndex := 3
		callArgumentIndex := 1
		if argumentIndex > 0 {
			assignmentIndex = 4
			callArgumentIndex = argumentIndex
		}
		literal, ok := assignments[assignmentIndex].Expression.(*ScalarCallExpr).
			Arguments[callArgumentIndex].(*ScalarLiteralExpr)
		if !ok || literal.Value.Kind != LiteralKindInteger || literal.Value.Text != text {
			t.Fatalf("mvindex literal %d = %#v, want %q", argumentIndex, literal, text)
		}
	}
}

func TestParseNativeMultivalueFunctionsComposeInSuppliedPipeline(t *testing.T) {
	t.Parallel()

	_, err := Parse(`index=main
| spath path=users{} output=users
| eval users=mvdedup(users), first_user=mvindex(users, 0), user_list=mvjoin(users, ",")
| mvexpand users
| stats count BY users`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
}

func TestParseNativeMultivalueDelimitersAreQuotedAndBounded(t *testing.T) {
	t.Parallel()

	for _, source := range []string{
		`index=main | eval value=split(text, "")`,
		`index=main | eval value=split(text, "😀")`,
		`index=main | eval value=mvjoin(values, "\n")`,
		`index=main | eval value=mvzip(left, right)`,
		`index=main | eval value=mvzip(left, right, "::")`,
	} {
		if _, err := Parse(source); err != nil {
			t.Fatalf("Parse(%q): %v", source, err)
		}
	}
	maximum := strings.Repeat("x", MaximumMVDelimiterBytes)
	if _, err := Parse(`index=main | eval value=mvjoin(values, "` + maximum + `")`); err != nil {
		t.Fatalf("Parse maximum delimiter: %v", err)
	}

	for _, source := range []string{
		`index=main | eval value=split(text, delimiter)`,
		`index=main | eval value=split(text, 1)`,
		`index=main | eval value=mvjoin(values, delimiter)`,
		`index=main | eval value=mvzip(left, right, delimiter)`,
	} {
		assertParseDiagnosticCode(t, source, "SPL_UNSUPPORTED_EVAL_EXPRESSION")
	}

	tooLong := strings.Repeat("x", MaximumMVDelimiterBytes+1)
	for _, call := range []string{
		`split(text, "` + tooLong + `")`,
		`mvjoin(values, "` + tooLong + `")`,
		`mvzip(left, right, "` + tooLong + `")`,
	} {
		assertParseDiagnosticCode(t, `index=main | eval value=`+call, "SPL_QUERY_TOO_COMPLEX")
	}
}

func TestParseMVAppendBoundsArgumentCount(t *testing.T) {
	t.Parallel()

	maximum := strings.TrimSuffix(strings.Repeat("value,", MaximumMVAppendArguments), ",")
	if _, err := Parse(`index=main | eval values=mvappend(` + maximum + `)`); err != nil {
		t.Fatalf("Parse maximum mvappend: %v", err)
	}
	tooMany := maximum + ",value"
	assertParseDiagnosticCode(
		t,
		`index=main | eval values=mvappend(`+tooMany+`)`,
		"SPL_QUERY_TOO_COMPLEX",
	)
}

func TestParseMVIndexRequiresBoundedSignedIntegerLiterals(t *testing.T) {
	t.Parallel()

	for _, source := range []string{
		`index=main | eval value=mvindex(values, -2147483648)`,
		`index=main | eval value=mvindex(values, +2147483647)`,
		`index=main | eval value=mvindex(values, -2, -1)`,
	} {
		if _, err := Parse(source); err != nil {
			t.Fatalf("Parse(%q): %v", source, err)
		}
	}
	for _, source := range []string{
		`index=main | eval value=mvindex(values, offset)`,
		`index=main | eval value=mvindex(values, 1.5)`,
		`index=main | eval value=mvindex(values, 0, tonumber(span))`,
	} {
		assertParseDiagnosticCode(t, source, "SPL_UNSUPPORTED_MV_INDEX")
	}
	for _, source := range []string{
		`index=main | eval value=mvindex(values, -2147483649)`,
		`index=main | eval value=mvindex(values, 2147483648)`,
	} {
		assertParseDiagnosticCode(t, source, "SPL_NUMBER_OUT_OF_RANGE")
	}
}

func TestParseMVFindRequiresBoundedLiteralRE2Pattern(t *testing.T) {
	t.Parallel()

	for _, source := range []string{
		`index=main | eval value=mvfind(values, pattern)`,
		`index=main | eval value=mvfind(values, 1)`,
	} {
		assertParseDiagnosticCode(t, source, "SPL_UNSUPPORTED_EVAL_EXPRESSION")
	}
	assertParseDiagnosticCode(
		t,
		`index=main | eval value=mvfind(values, "(?=secret)")`,
		"SPL_UNSUPPORTED_REGEX",
	)
	tooLong := strings.Repeat("x", splregex.MaximumMatchPatternBytes+1)
	assertParseDiagnosticCode(
		t,
		`index=main | eval value=mvfind(values, "`+tooLong+`")`,
		"SPL_QUERY_TOO_COMPLEX",
	)
}
