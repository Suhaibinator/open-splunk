package clickhouse

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/plan"
	"github.com/Suhaibinator/open-splunk/internal/spl"
)

func requireScalarPackSQL(t *testing.T, compiled CompiledQuery, required ...string) {
	t.Helper()
	for _, fragment := range required {
		if !strings.Contains(compiled.SQL, fragment) {
			t.Fatalf("SQL missing %q:\n%s", fragment, compiled.SQL)
		}
	}
	if got, want := strings.Count(compiled.SQL, "?"), len(compiled.Args); got != want {
		t.Fatalf("placeholder count = %d, args = %d\nSQL: %s", got, want, compiled.SQL)
	}
}

// requireScalarPackDiagnostic accepts the diagnostic from whichever phase owns
// the rejection: the planner rejects statically known operand types while the
// compiler owns everything that depends on the lowered value kinds.
func requireScalarPackDiagnostic(t *testing.T, source, code string) {
	t.Helper()
	_ = scalarPackDiagnostic(t, source, code)
}

// scalarPackDiagnostic is requireScalarPackDiagnostic for callers that also
// inspect the diagnostic message.
func scalarPackDiagnostic(t *testing.T, source, code string) *plan.Diagnostic {
	t.Helper()
	parsed, err := spl.Parse(source)
	if err != nil {
		t.Fatalf("Parse(%q): %v", source, err)
	}
	logical, err := plan.Build(parsed, testChartScope())
	if err == nil {
		_, err = (Compiler{}).Compile(logical)
	}
	var diagnostic *plan.Diagnostic
	if !errors.As(err, &diagnostic) || diagnostic.Code != code {
		t.Fatalf("Compile(%q) error = %#v, want %s", source, err, code)
	}
	if diagnostic.Range.Start.Offset >= diagnostic.Range.End.Offset ||
		diagnostic.Range.End.Offset > len(source) {
		t.Fatalf("Compile(%q) diagnostic range = %#v", source, diagnostic.Range)
	}
	return diagnostic
}

func TestCompileEvalMathFunctionsLowerThroughArithmeticOperands(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(
		t,
		`index=gradethis | eval a=abs(bytes), s=sqrt(bytes), e=exp(1), l=ln(bytes), l10=log(bytes), lb=log(bytes, 2), p=pow(bytes, 2), circle=pi() | table a,s,e,l,l10,lb,p,circle`,
	)
	requireScalarPackSQL(
		t,
		compiled,
		`plus(abs(__os_math_operand_1), toFloat64(0))`,
		`plus(if(__os_math_operand_1 < 0, CAST(NULL AS Nullable(Float64)), sqrt(__os_math_operand_1)), toFloat64(0))`,
		`plus(exp(__os_math_operand_1), toFloat64(0)), [toFloat64(CAST(? AS Int64))]`,
		`plus(if(__os_math_operand_1 <= 0, CAST(NULL AS Nullable(Float64)), log(__os_math_operand_1)), toFloat64(0))`,
		`plus(if(__os_math_operand_1 <= 0, CAST(NULL AS Nullable(Float64)), log10(__os_math_operand_1)), toFloat64(0))`,
		`plus(if(__os_math_operand_1 <= 0 OR __os_math_operand_2 <= 0 OR __os_math_operand_2 = 1, CAST(NULL AS Nullable(Float64)), divide(log(__os_math_operand_1), log(__os_math_operand_2))), toFloat64(0))`,
		`plus(if(isNaN(pow(__os_math_operand_1, __os_math_operand_2)), CAST(NULL AS Nullable(Float64)), pow(__os_math_operand_1, __os_math_operand_2)), toFloat64(0))`,
		`pi() AS "circle"`,
	)
	// Dynamic operands go through the same numeric-text and tagged-decimal
	// normalisation as `+`, which binds the field name once per operand.
	if got := strings.Count(compiled.SQL, `has("__os_field_names", ?)`); got != 6 {
		t.Fatalf("dynamic operand bindings = %d, want 6\n%s", got, compiled.SQL)
	}
	if !compiled.RequiresAtomicResult() {
		t.Fatalf("math over Dynamic operands must require an atomic result")
	}
}

func TestCompileEvalMathFunctionsRejectNonNumericOperands(t *testing.T) {
	t.Parallel()

	for _, source := range []string{
		`index=gradethis | eval value=sqrt("16")`,
		`index=gradethis | eval value=abs(_time)`,
		`index=gradethis | eval value=pow(host, 2)`,
		`index=gradethis | eval value=ln(split(host, "."))`,
	} {
		requireScalarPackDiagnostic(t, source, "SPL_UNSUPPORTED_ARITHMETIC_VALUE_TYPE")
	}
}

func TestCompileEvalMathFunctionsChargeTheArithmeticOperatorCeiling(t *testing.T) {
	t.Parallel()

	chain := func(calls int) string {
		terms := make([]string, calls)
		for index := range terms {
			terms[index] = "abs(1)"
		}
		return `index=gradethis | eval value=` + strings.Join(terms, "+") + ` | table value`
	}
	// calls function applications plus calls-1 additions.
	atLimit := (spl.MaximumArithmeticOperatorsPerQuery + 1) / 2
	compileSPL(t, chain(atLimit))

	logical := buildPlan(t, chain(atLimit+1))
	_, err := (Compiler{}).Compile(logical)
	var diagnostic *plan.Diagnostic
	if !errors.As(err, &diagnostic) || diagnostic.Code != "SPL_QUERY_TOO_COMPLEX" ||
		!strings.Contains(diagnostic.Message, fmt.Sprintf("%d arithmetic operators", spl.MaximumArithmeticOperatorsPerQuery)) {
		t.Fatalf("Compile error = %#v, want SPL_QUERY_TOO_COMPLEX over the operator ceiling", err)
	}
}

func TestCompileEvalMathFunctionsRejectForgedPlans(t *testing.T) {
	t.Parallel()

	base := buildPlan(t, `index=gradethis`)
	number := func() plan.ScalarExpression {
		return &plan.ScalarLiteralExpression{Value: plan.Value{Kind: plan.ValueKindInt64, Int64: 4}}
	}
	var typedNil *plan.ScalarLiteralExpression
	cyclic := &plan.ScalarCallExpression{Function: plan.ScalarFunctionAbs}
	cyclic.Arguments = []plan.ScalarExpression{cyclic}
	for _, test := range []struct {
		name       string
		expression plan.ScalarExpression
		want       string
	}{
		{name: "abs without arguments", expression: &plan.ScalarCallExpression{Function: plan.ScalarFunctionAbs}, want: "expected"},
		{name: "log with three arguments", expression: &plan.ScalarCallExpression{Function: plan.ScalarFunctionLog, Arguments: []plan.ScalarExpression{number(), number(), number()}}, want: "log: expected 1 to 2 arguments"},
		{name: "pow with one argument", expression: &plan.ScalarCallExpression{Function: plan.ScalarFunctionPow, Arguments: []plan.ScalarExpression{number()}}, want: "expected"},
		{name: "pi with an argument", expression: &plan.ScalarCallExpression{Function: plan.ScalarFunctionPi, Arguments: []plan.ScalarExpression{number()}}, want: "expected"},
		{name: "typed nil argument", expression: &plan.ScalarCallExpression{Function: plan.ScalarFunctionSqrt, Arguments: []plan.ScalarExpression{typedNil}}, want: "missing scalar expression"},
		{name: "cyclic expression", expression: cyclic, want: "contains a cycle"},
		{
			name: "Boolean operand",
			expression: &plan.ScalarCallExpression{
				Function: plan.ScalarFunctionExp,
				Arguments: []plan.ScalarExpression{&plan.ScalarCallExpression{
					Function:  plan.ScalarFunctionIsNull,
					Arguments: []plan.ScalarExpression{number()},
				}},
			},
			want: "cannot consume a Boolean",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			err := compileForgedScalarAssignment(t, base, test.expression)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("Compile error = %v, want %q", err, test.want)
			}
		})
	}
}

func TestCompileEvalTextTransformsInlineConstantTrimCharacters(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(
		t,
		`index=gradethis | eval t=trim(host), lt=ltrim(host, "x"), rt=rtrim(_raw), u=urldecode(host), m=md5(host), s1=sha1(host), s2=sha256(host), s5=sha512(host) | table t,lt,rt,u,m,s1,s2,s5`,
	)
	requireScalarPackSQL(
		t,
		compiled,
		`trimBoth("host", ' \x09') AS "t"`,
		`trimLeft("host", 'x') AS "lt"`,
		`trimRight(if(ifNull("__os_raw_encoding" = 1, 0), "_raw", CAST(NULL AS Nullable(String))), ' \x09')`,
		`arrayElement(arrayMap((__os_urldecode_value) -> if(isValidUTF8(decodeURLComponent(__os_urldecode_value)), decodeURLComponent(__os_urldecode_value), __os_urldecode_value), ["host"]), 1) AS "u"`,
		`lower(hex(MD5("host"))) AS "m"`,
		`lower(hex(SHA1("host"))) AS "s1"`,
		`lower(hex(SHA256("host"))) AS "s2"`,
		`lower(hex(SHA512("host"))) AS "s5"`,
	)
	for _, arg := range compiled.Args {
		if text, ok := arg.(string); ok && (text == " \t" || text == "x") {
			t.Fatalf("trim characters were bound instead of inlined: %#v", compiled.Args)
		}
	}
}

// ClickHouse only accepts a constant trim character set, so the literal is
// inlined; every byte that could close the literal, read as a driver bind
// marker, or hide inside a formatter is hexadecimal-escaped.
func TestCompileEvalTrimCharactersEscapeQuotesBindMarkersAndControlBytes(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(
		t,
		`index=gradethis | eval t=trim(host, "'\\?${}"), n=ltrim(host, "é") | table t,n`,
	)
	requireScalarPackSQL(
		t,
		compiled,
		`trimBoth("host", '\x27\x5C\x3F\x24\x7B\x7D') AS "t"`,
		`trimLeft("host", 'é') AS "n"`,
	)
	for _, literal := range []struct {
		text string
		want string
	}{
		{text: "abc", want: `'abc'`},
		{text: " \t\n\r\x00\x7f", want: `' \x09\x0A\x0D\x00\x7F'`},
		{text: `'\`, want: `'\x27\x5C'`},
		{text: "?$", want: `'\x3F\x24'`},
		{text: "{name:String}", want: `'\x7Bname:String\x7D'`},
		{text: "ünïcödé", want: `'ünïcödé'`},
	} {
		if got := quoteStringLiteral(literal.text); got != literal.want {
			t.Fatalf("quoteStringLiteral(%q) = %s, want %s", literal.text, got, literal.want)
		}
	}
}

func TestCompileEvalTextTransformsMapMultivalueMembers(t *testing.T) {
	t.Parallel()

	native := compileSPL(
		t,
		`index=gradethis | eval parts=split(host, "."), trimmed=trim(parts, "-"), digests=sha256(parts) | table trimmed,digests`,
	)
	requireScalarPackSQL(
		t,
		native,
		`arrayMap(element -> trimBoth(element, '-')`,
		`arrayMap(element -> lower(hex(SHA256(element)))`,
	)
	if strings.Contains(strings.ToUpper(native.SQL), "ARRAY JOIN") {
		t.Fatalf("text transform introduced row expansion:\n%s", native.SQL)
	}

	dynamic := compileSPL(t, `index=gradethis | eval decoded=urldecode(category) | table decoded`)
	requireScalarPackSQL(
		t,
		dynamic,
		`dynamicType(value) = 'String'`,
		`dynamicType(value) = 'Array(String)'`,
		`arrayMap(element -> arrayElement(arrayMap((__os_urldecode_value) ->`,
	)

	fixed := compileSPL(
		t,
		`index=gradethis | stats values(user) AS users | eval digests=md5(users) | table digests`,
	)
	requireScalarPackSQL(
		t,
		fixed,
		`arrayAll(element -> isValidUTF8(element), values)`,
		`arrayMap(element -> lower(hex(MD5(element))), values)`,
	)
}

func TestCompileEvalTextTransformsRejectNonStringInputs(t *testing.T) {
	t.Parallel()

	for _, source := range []string{
		`index=gradethis | eval value=trim(123)`,
		`index=gradethis | eval value=urldecode(true)`,
		`index=gradethis | eval value=sha1(_time)`,
	} {
		diagnostic := scalarPackDiagnostic(t, source, "SPL_UNSUPPORTED_TEXT_TRANSFORM_VALUE_TYPE")
		if !strings.Contains(diagnostic.Message, "requires a String or multivalue String input") {
			t.Fatalf("Compile(%q) message = %q", source, diagnostic.Message)
		}
	}
}

func TestCompileEvalTextTransformsRejectForgedPlans(t *testing.T) {
	t.Parallel()

	base := buildPlan(t, `index=gradethis`)
	text := func(value string) plan.ScalarExpression {
		return &plan.ScalarLiteralExpression{Value: plan.Value{Kind: plan.ValueKindString, String: value, Quoted: true}}
	}
	for _, test := range []struct {
		name       string
		expression plan.ScalarExpression
		want       string
	}{
		{name: "trim with three arguments", expression: &plan.ScalarCallExpression{Function: plan.ScalarFunctionTrim, Arguments: []plan.ScalarExpression{text("a"), text("b"), text("c")}}, want: "trim: expected one or two arguments"},
		{name: "unquoted trim characters", expression: &plan.ScalarCallExpression{Function: plan.ScalarFunctionLTrim, Arguments: []plan.ScalarExpression{text("a"), &plan.ScalarFieldExpression{Field: plan.FieldRef{Name: "host"}}}}, want: "ltrim: characters must be a quoted string literal"},
		{name: "empty trim characters", expression: &plan.ScalarCallExpression{Function: plan.ScalarFunctionRTrim, Arguments: []plan.ScalarExpression{text("a"), text("")}}, want: "rtrim: characters must be a non-empty valid UTF-8 string"},
		{name: "oversized trim characters", expression: &plan.ScalarCallExpression{Function: plan.ScalarFunctionTrim, Arguments: []plan.ScalarExpression{text("a"), text(strings.Repeat("x", spl.MaximumTrimCharactersBytes+1))}}, want: "trim: characters exceed the 256-byte limit"},
		{name: "md5 with two arguments", expression: &plan.ScalarCallExpression{Function: plan.ScalarFunctionMD5, Arguments: []plan.ScalarExpression{text("a"), text("b")}}, want: "expected"},
		{name: "urldecode without arguments", expression: &plan.ScalarCallExpression{Function: plan.ScalarFunctionURLDecode}, want: "expected"},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			err := compileForgedScalarAssignment(t, base, test.expression)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("Compile error = %v, want %q", err, test.want)
			}
		})
	}
}

func TestCompileEvalTypeOfResolvesFixedKindsAndClassifiesDynamicValues(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(
		t,
		`index=gradethis | eval s=typeof(host), n=typeof(_time), b=typeof(isnull(host)), invalid=typeof(null), literal=typeof(5), dynamic=typeof(bytes) | table s,n,b,invalid,literal,dynamic`,
	)
	requireScalarPackSQL(
		t,
		compiled,
		`if(ifNull(((1) AND isNotNull("host")), 0), 'String', 'Invalid') AS "s"`,
		`if(ifNull(((1) AND isNotNull("_time")), 0), 'Number', 'Invalid') AS "n"`,
		`'Boolean', 'Invalid') AS "b"`,
		`CAST('Invalid' AS String) AS "invalid"`,
		`if(ifNull(((1) AND isNotNull(CAST(? AS Int64))), 0), 'Number', 'Invalid') AS "literal"`,
		`arrayElement(arrayMap((__os_typeof_value, __os_typeof_exists) -> multiIf(__os_typeof_exists = 0 OR isNull(__os_typeof_value), 'Invalid', dynamicType(__os_typeof_value) = 'Bool', 'Boolean'`,
		`dynamicType(__os_typeof_value) = 'String', 'String', CAST(NULL AS Nullable(String)))`,
	)
	if !strings.Contains(compiled.SQL, `'Number', dynamicType(__os_typeof_value) = 'String'`) {
		t.Fatalf("dynamic typeof must classify numeric values before Strings:\n%s", compiled.SQL)
	}
}

func TestCompileEvalTypeOfRejectsMultivalueAndForgedPlans(t *testing.T) {
	t.Parallel()

	requireScalarPackDiagnostic(
		t,
		`index=gradethis | eval kind=typeof(split(host, "."))`,
		"SPL_UNSUPPORTED_MULTIVALUE_USAGE",
	)

	base := buildPlan(t, `index=gradethis`)
	text := &plan.ScalarLiteralExpression{Value: plan.Value{Kind: plan.ValueKindString, String: "a", Quoted: true}}
	for _, test := range []struct {
		name       string
		expression plan.ScalarExpression
		want       string
	}{
		{name: "no arguments", expression: &plan.ScalarCallExpression{Function: plan.ScalarFunctionTypeOf}, want: "expected"},
		{name: "two arguments", expression: &plan.ScalarCallExpression{Function: plan.ScalarFunctionTypeOf, Arguments: []plan.ScalarExpression{text, text}}, want: "expected"},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			err := compileForgedScalarAssignment(t, base, test.expression)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("Compile error = %v, want %q", err, test.want)
			}
		})
	}
}

func TestCompileWhereCIDRMatchGuardsAddressesAndBindsTheMaskedPrefix(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(
		t,
		`index=gradethis | where cidrmatch("10.1.2.3/8", host) | eval lab=if(cidrmatch("2001:db8::1/32", "2001:db8::1"), "yes", "no"), absent=if(cidrmatch("10.0.0.0/8", null), "yes", "no") | table lab,absent`,
	)
	requireScalarPackSQL(
		t,
		compiled,
		`arrayMap((__os_cidrmatch_address, __os_cidrmatch_prefix) -> if(isNull(__os_cidrmatch_address), CAST(NULL AS Nullable(Bool)), CAST((isIPv4String(assumeNotNull(__os_cidrmatch_address)) OR isIPv6String(assumeNotNull(__os_cidrmatch_address))) AND isIPAddressInRange(if((isIPv4String(assumeNotNull(__os_cidrmatch_address)) OR isIPv6String(assumeNotNull(__os_cidrmatch_address))), assumeNotNull(__os_cidrmatch_address), '0.0.0.0'), __os_cidrmatch_prefix) AS Nullable(Bool))), ["host"], [CAST(? AS String)])`,
		`CAST(NULL AS Nullable(Bool))`,
	)
	var prefixes []any
	for _, arg := range compiled.Args {
		if text, ok := arg.(string); ok && strings.Contains(text, "/") {
			prefixes = append(prefixes, text)
		}
	}
	// The address literal binds before its prefix, and the where clause is
	// rendered after the eval stage's projections.
	if got, want := fmt.Sprint(prefixes), fmt.Sprint([]any{"2001:db8::/32", "10.0.0.0/8"}); got != want {
		t.Fatalf("bound prefixes = %s, want masked %s\nargs: %#v", got, want, compiled.Args)
	}
}

func TestCompileWhereCIDRMatchRejectsMultivalueAndForgedPlans(t *testing.T) {
	t.Parallel()

	requireScalarPackDiagnostic(
		t,
		`index=gradethis | where cidrmatch("10.0.0.0/8", split(host, ","))`,
		"SPL_UNSUPPORTED_MULTIVALUE_USAGE",
	)

	base := buildPlan(t, `index=gradethis`)
	text := func(value string) plan.ScalarExpression {
		return &plan.ScalarLiteralExpression{Value: plan.Value{Kind: plan.ValueKindString, String: value, Quoted: true}}
	}
	for _, test := range []struct {
		name       string
		expression plan.ScalarExpression
		want       string
	}{
		{name: "one argument", expression: &plan.ScalarCallExpression{Function: plan.ScalarFunctionCIDRMatch, Arguments: []plan.ScalarExpression{text("10.0.0.0/8")}}, want: "cidrmatch: expected two arguments"},
		{name: "unquoted prefix", expression: &plan.ScalarCallExpression{Function: plan.ScalarFunctionCIDRMatch, Arguments: []plan.ScalarExpression{&plan.ScalarFieldExpression{Field: plan.FieldRef{Name: "host"}}, text("10.0.0.1")}}, want: "cidrmatch: prefix must be a quoted string literal"},
		{name: "malformed prefix", expression: &plan.ScalarCallExpression{Function: plan.ScalarFunctionCIDRMatch, Arguments: []plan.ScalarExpression{text("10.0.0.0/33"), text("10.0.0.1")}}, want: "cidrmatch: invalid prefix"},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			err := compileForgedScalarAssignment(t, base, &plan.ScalarCallExpression{
				Function:  plan.ScalarFunctionToString,
				Arguments: []plan.ScalarExpression{test.expression},
			})
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("Compile error = %v, want %q", err, test.want)
			}
		})
	}
}

func TestCompileEvalToStringFormatsRenderCommasAndDurations(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(
		t,
		`index=gradethis | eval pretty=tostring(bytes, "commas"), elapsed=tostring(1234567.891, "duration"), flag=tostring(isnull(host), "commas") | table pretty,elapsed,flag`,
	)
	requireScalarPackSQL(
		t,
		compiled,
		`multiIf(isNull(__os_tostring_value), CAST(NULL AS Nullable(String)), NOT (isFinite(__os_tostring_value) AND abs(__os_tostring_value) < toFloat64(9223372036854775808)), CAST(NULL AS Nullable(String)), `,
		`round(abs(__os_tostring_safe), 2)`,
		`reverse(arrayStringConcat(arrayMap(k -> substring(__os_tostring_reversed, k * 3 + 1, 3), range(intDiv(length(__os_tostring_reversed) + 2, 3))), ','))`,
		`leftPad(toString(toInt64(round((__os_tostring_rounded - floor(__os_tostring_rounded)) * 100))), 2, '0')`,
		`concat(if(__os_tostring_value < 0, '-', ''), `,
		`NOT (isFinite(__os_tostring_value) AND __os_tostring_value >= 0 AND __os_tostring_value < toFloat64(9223372036854775808))`,
		`toInt64(floor(if((isFinite(__os_tostring_value) AND __os_tostring_value >= 0 AND __os_tostring_value < toFloat64(9223372036854775808)), __os_tostring_value, toFloat64(0))))`,
		`concat(if(intDiv(__os_tostring_total, 86400) > 0, concat(toString(intDiv(__os_tostring_total, 86400)), '+'), CAST('' AS String)), leftPad(toString(modulo(intDiv(__os_tostring_total, 3600), 24)), 2, '0'), ':', leftPad(toString(modulo(intDiv(__os_tostring_total, 60), 60)), 2, '0'), ':', leftPad(toString(modulo(__os_tostring_total, 60)), 2, '0'))`,
		`'True', 'False'`,
	)
	if !compiled.RequiresAtomicResult() {
		t.Fatalf("tostring formats over Dynamic operands must require an atomic result")
	}
}

func TestCompileEvalToStringFormatsRejectNonNumericInputs(t *testing.T) {
	t.Parallel()

	for _, source := range []string{
		`index=gradethis | eval value=tostring(host, "commas")`,
		`index=gradethis | eval value=tostring(_time, "duration")`,
		`index=gradethis | eval value=tostring("12", "commas")`,
	} {
		diagnostic := scalarPackDiagnostic(t, source, "SPL_UNSUPPORTED_TOSTRING_VALUE_TYPE")
		if !strings.Contains(diagnostic.Message, "requires a numeric value") {
			t.Fatalf("Compile(%q) message = %q", source, diagnostic.Message)
		}
	}
	requireScalarPackDiagnostic(
		t,
		`index=gradethis | eval value=tostring(split(host, "."), "commas")`,
		"SPL_UNSUPPORTED_MULTIVALUE_USAGE",
	)
}

func TestCompileEvalNullIfDesugarsToAConditional(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(t, `index=gradethis | eval cleaned=nullif(host, "-") | table cleaned`)
	requireScalarPackSQL(t, compiled, `AS "cleaned"`, `CAST(NULL AS Nullable(String))`)
	if !strings.Contains(compiled.SQL, `toString("host") = CAST(? AS String), CAST(NULL AS Nullable(Bool))), 0), CAST(NULL AS Nullable(String)), "host") AS "cleaned"`) {
		t.Fatalf("nullif must compare the operands with the if lowering:\n%s", compiled.SQL)
	}
	if compiled.Args[0] != "-" {
		t.Fatalf("args = %#v, want the comparison literal first", compiled.Args)
	}
	// The desugared if keeps its branch-kind rule: a Dynamic event field has no
	// stable branch type, so the query is rejected before execution.
	diagnostic := scalarPackDiagnostic(
		t,
		`index=gradethis | eval cleaned=nullif(status, "-")`,
		"SPL_UNSUPPORTED_IF_BRANCH_TYPE",
	)
	if !strings.Contains(diagnostic.Message, "Null and Dynamic") {
		t.Fatalf("nullif Dynamic diagnostic = %q", diagnostic.Message)
	}
	compileSPL(t, `index=gradethis | eval cleaned=nullif(tostring(status), "-") | table cleaned`)
}
