package plan

import (
	"strings"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/spl"
)

func TestBuildScalarPackMapsEveryFunctionIntoTypedIR(t *testing.T) {
	t.Parallel()

	logical, err := Build(
		mustParse(
			t,
			`index=gradethis | eval a=abs(bytes), s=sqrt(bytes), e=exp(bytes), l=ln(bytes), l10=log(bytes), lb=log(bytes, 2), p=pow(bytes, 2), c=pi(), t=trim(host), lt=ltrim(host, "-"), rt=rtrim(host, "-"), u=urldecode(host), m=md5(host), s1=sha1(host), s2=sha256(host), s5=sha512(host), k=typeof(host), f=tostring(bytes, "commas"), lab=if(cidrmatch("10.1.2.3/8", host), "lab", "other")`,
		),
		testScope([]string{"gradethis"}, nil),
	)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	assignments := logical.Operators[len(logical.Operators)-1].(*Extend).Assignments
	want := []struct {
		field    string
		function ScalarFunction
		arity    int
	}{
		{"a", ScalarFunctionAbs, 1},
		{"s", ScalarFunctionSqrt, 1},
		{"e", ScalarFunctionExp, 1},
		{"l", ScalarFunctionLn, 1},
		{"l10", ScalarFunctionLog, 1},
		{"lb", ScalarFunctionLog, 2},
		{"p", ScalarFunctionPow, 2},
		{"c", ScalarFunctionPi, 0},
		{"t", ScalarFunctionTrim, 1},
		{"lt", ScalarFunctionLTrim, 2},
		{"rt", ScalarFunctionRTrim, 2},
		{"u", ScalarFunctionURLDecode, 1},
		{"m", ScalarFunctionMD5, 1},
		{"s1", ScalarFunctionSHA1, 1},
		{"s2", ScalarFunctionSHA256, 1},
		{"s5", ScalarFunctionSHA512, 1},
		{"k", ScalarFunctionTypeOf, 1},
		{"f", ScalarFunctionToString, 2},
	}
	if len(assignments) != len(want)+1 {
		t.Fatalf("assignments = %d, want %d", len(assignments), len(want)+1)
	}
	for index, expected := range want {
		call, ok := assignments[index].Expression.(*ScalarCallExpression)
		if assignments[index].Output.Name != expected.field || !ok ||
			call.Function != expected.function || len(call.Arguments) != expected.arity {
			t.Fatalf("assignment %d = %#v, want %s=%v/%d", index, assignments[index], expected.field, expected.function, expected.arity)
		}
	}
	conditional, ok := assignments[len(want)].Expression.(*ScalarIfExpression)
	if !ok {
		t.Fatalf("lab = %#v, want a conditional", assignments[len(want)].Expression)
	}
	predicate, ok := conditional.Condition.(*ScalarPredicateExpression)
	if !ok {
		t.Fatalf("lab condition = %#v", conditional.Condition)
	}
	call, ok := predicate.Value.(*ScalarCallExpression)
	if !ok || call.Function != ScalarFunctionCIDRMatch || len(call.Arguments) != 2 {
		t.Fatalf("lab predicate = %#v", predicate.Value)
	}
	if !ScalarFunctionCIDRMatch.ReturnsBoolean() {
		t.Fatalf("cidrmatch must keep its Boolean contract in the plan IR")
	}
	prefix, ok := call.Arguments[0].(*ScalarLiteralExpression)
	if !ok || prefix.Value.Kind != ValueKindString || !prefix.Value.Quoted || prefix.Value.String != "10.1.2.3/8" {
		t.Fatalf("cidrmatch prefix = %#v, want the authored literal (masking happens at lowering)", call.Arguments[0])
	}
}

func TestBuildScalarPackRejectsStaticallyInvalidArguments(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		source string
		code   string
	}{
		{source: `index=gradethis | eval v=sqrt("16")`, code: "SPL_UNSUPPORTED_ARITHMETIC_VALUE_TYPE"},
		{source: `index=gradethis | eval v=pow(lower(host), 2)`, code: "SPL_UNSUPPORTED_ARITHMETIC_VALUE_TYPE"},
		{source: `index=gradethis | eval v=abs(split(host, "."))`, code: "SPL_UNSUPPORTED_ARITHMETIC_VALUE_TYPE"},
	} {
		_, err := Build(mustParse(t, test.source), testScope([]string{"gradethis"}, nil))
		if err == nil {
			t.Fatalf("Build(%q) succeeded, want %s", test.source, test.code)
		}
		assertDiagnosticCode(t, err, test.code)
	}
}

func TestBuildScalarPackRejectsForgedArityBooleansAndLiterals(t *testing.T) {
	t.Parallel()

	base := mustParse(t, `index=gradethis`)
	sourceRange := spl.Range{
		Start: spl.Position{Line: 1, Column: 1},
		End:   spl.Position{Line: 1, Column: 5},
	}
	field := func() spl.ScalarExpr {
		return &spl.ScalarFieldExpr{Field: "source", Range: sourceRange}
	}
	quoted := func(text string) spl.ScalarExpr {
		return &spl.ScalarLiteralExpr{
			Value: spl.Literal{Kind: spl.LiteralKindString, Text: text, Quoted: true, Range: sourceRange},
			Range: sourceRange,
		}
	}
	boolean := func() spl.ScalarExpr {
		return &spl.ScalarCallExpr{
			Function:  spl.ScalarFunctionIsNull,
			Arguments: []spl.ScalarExpr{field()},
			Range:     sourceRange,
		}
	}
	call := func(function spl.ScalarFunction, arguments ...spl.ScalarExpr) spl.ScalarExpr {
		return &spl.ScalarCallExpr{Function: function, Arguments: arguments, Range: sourceRange}
	}
	// cidrmatch is Boolean, so a forged eval must consume it through if(...)
	// to get past the search-mode Boolean-assignment guard.
	predicate := func(arguments ...spl.ScalarExpr) spl.ScalarExpr {
		return &spl.ScalarIfExpr{
			Condition: &spl.WhereScalarPredicateExpr{
				Value: call(spl.ScalarFunctionCIDRMatch, arguments...),
				Range: sourceRange,
			},
			True:  quoted("lab"),
			False: quoted("other"),
			Range: sourceRange,
		}
	}
	var typedNil *spl.ScalarFieldExpr
	for _, test := range []struct {
		name       string
		expression spl.ScalarExpr
		code       string
	}{
		{name: "abs without arguments", expression: call(spl.ScalarFunctionAbs), code: "SPL_INVALID_EVAL_ARITY"},
		{name: "sqrt with two arguments", expression: call(spl.ScalarFunctionSqrt, field(), field()), code: "SPL_INVALID_EVAL_ARITY"},
		{name: "log with three arguments", expression: call(spl.ScalarFunctionLog, field(), field(), field()), code: "SPL_INVALID_EVAL_ARITY"},
		{name: "pow with one argument", expression: call(spl.ScalarFunctionPow, field()), code: "SPL_INVALID_EVAL_ARITY"},
		{name: "pi with an argument", expression: call(spl.ScalarFunctionPi, field()), code: "SPL_INVALID_EVAL_ARITY"},
		{name: "trim with three arguments", expression: call(spl.ScalarFunctionTrim, field(), quoted("-"), quoted("-")), code: "SPL_INVALID_EVAL_ARITY"},
		{name: "ltrim without arguments", expression: call(spl.ScalarFunctionLTrim), code: "SPL_INVALID_EVAL_ARITY"},
		{name: "urldecode with two arguments", expression: call(spl.ScalarFunctionURLDecode, field(), field()), code: "SPL_INVALID_EVAL_ARITY"},
		{name: "sha256 without arguments", expression: call(spl.ScalarFunctionSHA256), code: "SPL_INVALID_EVAL_ARITY"},
		{name: "typeof with two arguments", expression: call(spl.ScalarFunctionTypeOf, field(), field()), code: "SPL_INVALID_EVAL_ARITY"},
		{name: "cidrmatch with one argument", expression: predicate(quoted("10.0.0.0/8")), code: "SPL_INVALID_EVAL_ARITY"},
		{name: "math Boolean operand", expression: call(spl.ScalarFunctionLn, boolean()), code: "SPL_UNSUPPORTED_EVAL_EXPRESSION"},
		{name: "math fixed String operand", expression: call(spl.ScalarFunctionExp, quoted("1")), code: "SPL_UNSUPPORTED_ARITHMETIC_VALUE_TYPE"},
		{name: "text Boolean operand", expression: call(spl.ScalarFunctionMD5, boolean()), code: "SPL_UNSUPPORTED_EVAL_EXPRESSION"},
		{name: "unquoted trim characters", expression: call(spl.ScalarFunctionRTrim, field(), field()), code: "SPL_UNSUPPORTED_TRIM_CHARACTERS"},
		{name: "empty trim characters", expression: call(spl.ScalarFunctionTrim, field(), quoted("")), code: "SPL_UNSUPPORTED_TRIM_CHARACTERS"},
		{name: "invalid UTF-8 trim characters", expression: call(spl.ScalarFunctionTrim, field(), quoted("\xff")), code: "SPL_UNSUPPORTED_TRIM_CHARACTERS"},
		{name: "oversized trim characters", expression: call(spl.ScalarFunctionTrim, field(), quoted(strings.Repeat("x", spl.MaximumTrimCharactersBytes+1))), code: "SPL_QUERY_TOO_COMPLEX"},
		{name: "cidrmatch unquoted prefix", expression: predicate(field(), field()), code: "SPL_UNSUPPORTED_CIDR_PREFIX"},
		{name: "cidrmatch malformed prefix", expression: predicate(quoted("10.0.0.0/33"), field()), code: "SPL_UNSUPPORTED_CIDR_PREFIX"},
		{name: "cidrmatch Boolean address", expression: predicate(quoted("10.0.0.0/8"), boolean()), code: "SPL_UNSUPPORTED_EVAL_EXPRESSION"},
		{name: "tostring unsupported format", expression: call(spl.ScalarFunctionToString, field(), quoted("hex")), code: "SPL_UNSUPPORTED_TOSTRING_FORMAT"},
		{name: "typed nil math operand", expression: call(spl.ScalarFunctionAbs, typedNil), code: "SPL_UNSUPPORTED_EVAL_EXPRESSION"},
		{name: "typed nil text operand", expression: call(spl.ScalarFunctionTrim, typedNil), code: "SPL_UNSUPPORTED_EVAL_EXPRESSION"},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			assertForgedEvalBuildDiagnostic(t, base, sourceRange, test.expression, test.code)
		})
	}
}
