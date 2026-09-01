package spl

import (
	"errors"
	"strings"
	"testing"
)

func parseEvalScalarCall(t *testing.T, source string) *ScalarCallExpr {
	t.Helper()
	query, err := Parse(source)
	if err != nil {
		t.Fatalf("Parse(%q): %v", source, err)
	}
	call, ok := query.Commands[0].(*EvalCommand).Assignments[0].Expression.(*ScalarCallExpr)
	if !ok {
		t.Fatalf("Parse(%q) expression = %#v, want a scalar call", source, query.Commands[0])
	}
	return call
}

func TestParseScalarPackFunctionsResolveNamesCaseInsensitively(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		source   string
		function ScalarFunction
		arity    int
		boolean  bool
	}{
		{source: `index=main | eval v=ABS(bytes)`, function: ScalarFunctionAbs, arity: 1},
		{source: `index=main | eval v=sqrt(bytes)`, function: ScalarFunctionSqrt, arity: 1},
		{source: `index=main | eval v=exp(1)`, function: ScalarFunctionExp, arity: 1},
		{source: `index=main | eval v=ln(bytes)`, function: ScalarFunctionLn, arity: 1},
		{source: `index=main | eval v=log(bytes)`, function: ScalarFunctionLog, arity: 1},
		{source: `index=main | eval v=Log(bytes, 2)`, function: ScalarFunctionLog, arity: 2},
		{source: `index=main | eval v=pow(bytes, 2)`, function: ScalarFunctionPow, arity: 2},
		{source: `index=main | eval v=PI()`, function: ScalarFunctionPi, arity: 0},
		{source: `index=main | eval v=trim(host)`, function: ScalarFunctionTrim, arity: 1},
		{source: `index=main | eval v=ltrim(host, "-_")`, function: ScalarFunctionLTrim, arity: 2},
		{source: `index=main | eval v=RTRIM(host, "\t")`, function: ScalarFunctionRTrim, arity: 2},
		{source: `index=main | eval v=urldecode(host)`, function: ScalarFunctionURLDecode, arity: 1},
		{source: `index=main | eval v=md5(host)`, function: ScalarFunctionMD5, arity: 1},
		{source: `index=main | eval v=sha1(host)`, function: ScalarFunctionSHA1, arity: 1},
		{source: `index=main | eval v=SHA256(host)`, function: ScalarFunctionSHA256, arity: 1},
		{source: `index=main | eval v=sha512(host)`, function: ScalarFunctionSHA512, arity: 1},
		{source: `index=main | eval v=typeof(host)`, function: ScalarFunctionTypeOf, arity: 1},
		{source: `index=main | eval v=typeof(null)`, function: ScalarFunctionTypeOf, arity: 1},
		{source: `index=main | eval v=typeof(isnull(host))`, function: ScalarFunctionTypeOf, arity: 1},
		{source: `index=main | eval v=tostring(bytes, "commas")`, function: ScalarFunctionToString, arity: 2},
		{source: `index=main | eval v=tostring(bytes, "duration")`, function: ScalarFunctionToString, arity: 2},
	} {
		call := parseEvalScalarCall(t, test.source)
		if call.Function != test.function || len(call.Arguments) != test.arity {
			t.Fatalf("Parse(%q) = %v/%d, want %v/%d", test.source, call.Function, len(call.Arguments), test.function, test.arity)
		}
		if got := test.source[call.Range.Start.Offset:call.Range.End.Offset]; !strings.HasSuffix(got, ")") ||
			!strings.EqualFold(got[:strings.Index(got, "(")], strings.SplitN(strings.TrimPrefix(test.source, "index=main | eval v="), "(", 2)[0]) {
			t.Fatalf("Parse(%q) range = %q", test.source, got)
		}
	}
}

func TestParseCIDRMatchIsBooleanAndConfinedToPredicates(t *testing.T) {
	t.Parallel()

	if !ScalarFunctionCIDRMatch.ReturnsBoolean() {
		t.Fatalf("cidrmatch must be Boolean-returning")
	}
	query, err := Parse(`index=main | where cidrmatch("2001:db8::/32", client_ip)`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	where, ok := query.Commands[0].(*WhereCommand)
	if !ok {
		t.Fatalf("command = %#v", query.Commands[0])
	}
	predicate, ok := where.Expression.(*WhereScalarPredicateExpr)
	if !ok {
		t.Fatalf("where expression = %#v", where.Expression)
	}
	call, ok := predicate.Value.(*ScalarCallExpr)
	if !ok || call.Function != ScalarFunctionCIDRMatch || len(call.Arguments) != 2 {
		t.Fatalf("where predicate = %#v", predicate.Value)
	}
	query, err = Parse(`index=main | eval v=if(cidrmatch("10.0.0.0/8", host), 1, 0)`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	conditional, ok := query.Commands[0].(*EvalCommand).Assignments[0].Expression.(*ScalarIfExpr)
	if !ok {
		t.Fatalf("if expression = %#v", query.Commands[0])
	}
	if predicate, ok := conditional.Condition.(*WhereScalarPredicateExpr); !ok {
		t.Fatalf("if condition = %#v", conditional.Condition)
	} else if call, ok := predicate.Value.(*ScalarCallExpr); !ok || call.Function != ScalarFunctionCIDRMatch {
		t.Fatalf("if predicate = %#v", predicate.Value)
	}
	// Search-mode eval cannot assign a Boolean directly; the predicate must be
	// consumed by if/where like every other Boolean function.
	assertParseDiagnosticCode(
		t,
		`index=main | eval lab=cidrmatch("10.0.0.0/8", client_ip)`,
		"SPL_UNSUPPORTED_EVAL_EXPRESSION",
	)
}

func TestParseCIDRMatchValidatesTheLiteralPrefix(t *testing.T) {
	t.Parallel()

	for _, source := range []string{
		`index=main | where cidrmatch("10.0.0.0/8", client_ip)`,
		`index=main | where cidrmatch("10.1.2.3/8", client_ip)`,
		`index=main | where cidrmatch("192.168.1.10/32", client_ip)`,
		`index=main | where cidrmatch("0.0.0.0/0", client_ip)`,
		`index=main | where cidrmatch("2001:db8::/32", client_ip)`,
		`index=main | where cidrmatch("::1/128", client_ip)`,
		`index=main | where cidrmatch("::/0", client_ip)`,
		`index=main | where cidrmatch("::ffff:10.0.0.0/104", client_ip)`,
	} {
		if _, err := Parse(source); err != nil {
			t.Fatalf("Parse(%q): %v", source, err)
		}
	}
	for _, source := range []string{
		`index=main | where cidrmatch(network, client_ip)`,
		`index=main | where cidrmatch(10.0.0.0/8, client_ip)`,
		`index=main | where cidrmatch("10.0.0.0", client_ip)`,
		`index=main | where cidrmatch("10.0.0.0/33", client_ip)`,
		`index=main | where cidrmatch("10.0.0/8", client_ip)`,
		`index=main | where cidrmatch("2001:db8::/129", client_ip)`,
		`index=main | where cidrmatch("", client_ip)`,
		`index=main | where cidrmatch(" 10.0.0.0/8", client_ip)`,
		`index=main | where cidrmatch("fe80::%eth0/64", client_ip)`,
	} {
		assertParseDiagnosticCode(t, source, "SPL_UNSUPPORTED_CIDR_PREFIX")
	}
	for _, source := range []string{
		`index=main | where cidrmatch("10.0.0.0/8")`,
		`index=main | where cidrmatch("10.0.0.0/8", client_ip, extra)`,
		`index=main | where cidrmatch()`,
	} {
		assertParseDiagnosticCode(t, source, "SPL_INVALID_EVAL_ARITY")
	}
	assertParseDiagnosticCode(
		t,
		`index=main | where cidrmatch("10.0.0.0/8", isnull(client_ip))`,
		"SPL_UNSUPPORTED_EVAL_EXPRESSION",
	)
}

func TestParseScalarPackEnforcesArity(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		source  string
		message string
	}{
		{source: `index=main | eval v=abs()`, message: "abs requires exactly one argument"},
		{source: `index=main | eval v=abs(bytes, 2)`, message: "abs requires exactly one argument"},
		{source: `index=main | eval v=sqrt(bytes, 2)`, message: "sqrt requires exactly one argument"},
		{source: `index=main | eval v=exp()`, message: "exp requires exactly one argument"},
		{source: `index=main | eval v=ln(bytes, 2)`, message: "ln requires exactly one argument"},
		{source: `index=main | eval v=log()`, message: "log requires one or two arguments"},
		{source: `index=main | eval v=log(bytes, 2, 3)`, message: "log requires one or two arguments"},
		{source: `index=main | eval v=pow(bytes)`, message: "pow requires exactly two arguments"},
		{source: `index=main | eval v=pi(1)`, message: "pi requires no arguments"},
		{source: `index=main | eval v=trim()`, message: "trim requires one or two arguments"},
		{source: `index=main | eval v=ltrim(host, "a", "b")`, message: "ltrim requires one or two arguments"},
		{source: `index=main | eval v=rtrim()`, message: "rtrim requires one or two arguments"},
		{source: `index=main | eval v=urldecode(host, host)`, message: "urldecode requires exactly one argument"},
		{source: `index=main | eval v=md5()`, message: "md5 requires exactly one argument"},
		{source: `index=main | eval v=sha1(host, host)`, message: "sha1 requires exactly one argument"},
		{source: `index=main | eval v=sha256()`, message: "sha256 requires exactly one argument"},
		{source: `index=main | eval v=sha512(host, host)`, message: "sha512 requires exactly one argument"},
		{source: `index=main | eval v=typeof()`, message: "typeof requires exactly one argument"},
		{source: `index=main | eval v=typeof(host, bytes)`, message: "typeof requires exactly one argument"},
		{source: `index=main | eval v=nullif(host)`, message: "nullif requires exactly two arguments"},
		{source: `index=main | eval v=nullif(host, "-", "x")`, message: "nullif requires exactly two arguments"},
		{source: `index=main | eval v=tostring(bytes, "commas", "x")`, message: "tostring requires one or two arguments"},
	} {
		_, err := Parse(test.source)
		diagnostic, ok := parseDiagnostic(err)
		if !ok || diagnostic.Code != "SPL_INVALID_EVAL_ARITY" || diagnostic.Message != test.message {
			t.Fatalf("Parse(%q) error = %#v, want SPL_INVALID_EVAL_ARITY %q", test.source, err, test.message)
		}
		if diagnostic.Range.Start.Offset >= diagnostic.Range.End.Offset ||
			diagnostic.Range.End.Offset > len(test.source) {
			t.Fatalf("Parse(%q) range = %#v", test.source, diagnostic.Range)
		}
	}
}

func parseDiagnostic(err error) (*Diagnostic, bool) {
	var diagnostic *Diagnostic
	if !errors.As(err, &diagnostic) || diagnostic == nil {
		return nil, false
	}
	return diagnostic, true
}

func TestParseScalarPackRejectsBooleanArguments(t *testing.T) {
	t.Parallel()

	for _, source := range []string{
		`index=main | eval v=abs(isnull(bytes))`,
		`index=main | eval v=sqrt(isnotnull(bytes))`,
		`index=main | eval v=log(bytes, isnull(bytes))`,
		`index=main | eval v=pow(isnull(bytes), 2)`,
		`index=main | eval v=trim(isnull(host))`,
		`index=main | eval v=urldecode(isnull(host))`,
		`index=main | eval v=md5(isnull(host))`,
		`index=main | eval v=sha512(match(host, "a"))`,
		`index=main | eval v=nullif(isnull(host), "x")`,
		`index=main | eval v=nullif(host, isnull(host))`,
	} {
		assertParseDiagnosticCode(t, source, "SPL_UNSUPPORTED_EVAL_EXPRESSION")
	}
	// typeof names the Boolean kind, and tostring keeps its Boolean rendering
	// under either format, so both consume Booleans on purpose.
	for _, source := range []string{
		`index=main | eval v=typeof(isnull(host))`,
		`index=main | eval v=tostring(isnull(host), "commas")`,
		`index=main | eval v=tostring(like(host, "a%"), "duration")`,
	} {
		if _, err := Parse(source); err != nil {
			t.Fatalf("Parse(%q): %v", source, err)
		}
	}
}

func TestParseTrimCharactersMustBeABoundedQuotedLiteral(t *testing.T) {
	t.Parallel()

	for _, source := range []string{
		`index=main | eval v=trim(host, " ")`,
		`index=main | eval v=ltrim(host, "-_")`,
		`index=main | eval v=rtrim(host, "é")`,
		`index=main | eval v=trim(host, "` + strings.Repeat("x", MaximumTrimCharactersBytes) + `")`,
	} {
		if _, err := Parse(source); err != nil {
			t.Fatalf("Parse(%q): %v", source, err)
		}
	}
	for _, test := range []struct {
		source  string
		message string
	}{
		{source: `index=main | eval v=trim(host, characters)`, message: "trim characters must be a quoted string literal"},
		{source: `index=main | eval v=ltrim(host, 5)`, message: "ltrim characters must be a quoted string literal"},
		{source: `index=main | eval v=rtrim(host, lower("x"))`, message: "rtrim characters must be a quoted string literal"},
		{source: `index=main | eval v=trim(host, "")`, message: "trim characters must be a non-empty valid UTF-8 string"},
	} {
		_, err := Parse(test.source)
		diagnostic, ok := parseDiagnostic(err)
		if !ok || diagnostic.Code != "SPL_UNSUPPORTED_TRIM_CHARACTERS" || diagnostic.Message != test.message {
			t.Fatalf("Parse(%q) error = %#v, want SPL_UNSUPPORTED_TRIM_CHARACTERS %q", test.source, err, test.message)
		}
	}
	oversized := `index=main | eval v=trim(host, "` + strings.Repeat("x", MaximumTrimCharactersBytes+1) + `")`
	assertParseDiagnosticCode(t, oversized, "SPL_QUERY_TOO_COMPLEX")
	// The budget is measured in bytes, so multibyte characters count fully.
	multibyte := `index=main | eval v=trim(host, "` + strings.Repeat("é", MaximumTrimCharactersBytes/2+1) + `")`
	assertParseDiagnosticCode(t, multibyte, "SPL_QUERY_TOO_COMPLEX")
}

func TestParseNullIfDesugarsToConditional(t *testing.T) {
	t.Parallel()

	const source = `index=main | eval cleaned=NullIf(host, "-")`
	query, err := Parse(source)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	expression := query.Commands[0].(*EvalCommand).Assignments[0].Expression
	conditional, ok := expression.(*ScalarIfExpr)
	if !ok {
		t.Fatalf("expression = %#v, want ScalarIfExpr", expression)
	}
	comparison, ok := conditional.Condition.(*WhereComparisonExpr)
	if !ok || comparison.Op != CompareOpEqual {
		t.Fatalf("condition = %#v, want an equality comparison", conditional.Condition)
	}
	left, ok := comparison.Left.(*ScalarFieldExpr)
	if !ok || left.Field != "host" {
		t.Fatalf("left = %#v, want the host field", comparison.Left)
	}
	right, ok := comparison.Right.(*ScalarLiteralExpr)
	if !ok || right.Value.Kind != LiteralKindString || right.Value.Text != "-" {
		t.Fatalf("right = %#v, want the sentinel literal", comparison.Right)
	}
	null, ok := conditional.True.(*ScalarLiteralExpr)
	if !ok || null.Value.Kind != LiteralKindNull {
		t.Fatalf("true branch = %#v, want the null literal", conditional.True)
	}
	if conditional.False != comparison.Left {
		t.Fatalf("false branch = %#v, want the first argument", conditional.False)
	}
	if got := source[conditional.Range.Start.Offset:conditional.Range.End.Offset]; got != `NullIf(host, "-")` {
		t.Fatalf("range = %q", got)
	}
	// nullif charges the shared predicate budget like the if it expands to.
	nested := "host"
	for range MaximumEvalPredicates + 1 {
		nested = `nullif(` + nested + `, "-")`
	}
	assertParseDiagnosticCode(t, `index=main | eval v=`+nested, "SPL_QUERY_TOO_COMPLEX")
}

// The unknown-function diagnostic carries the longest suggestion list the
// parser emits. Search history, knowledge validation, and the suggestion API
// reject diagnostics with more than MaximumDiagnosticSuggestions entries, so
// growing the eval function surface must not grow this list past the bound.
func TestParseUnknownEvalFunctionSuggestionsStayWithinTheDiagnosticBound(t *testing.T) {
	t.Parallel()

	_, err := Parse(`index=main | eval value=mystery(host)`)
	diagnostic, ok := parseDiagnostic(err)
	if !ok || diagnostic.Code != "SPL_UNSUPPORTED_EVAL_FUNCTION" {
		t.Fatalf("Parse error = %#v, want SPL_UNSUPPORTED_EVAL_FUNCTION", err)
	}
	if len(diagnostic.Suggestions) == 0 || len(diagnostic.Suggestions) > MaximumDiagnosticSuggestions {
		t.Fatalf(
			"unknown function diagnostic carries %d suggestions, want 1 to %d",
			len(diagnostic.Suggestions),
			MaximumDiagnosticSuggestions,
		)
	}
	seen := make(map[string]struct{}, len(diagnostic.Suggestions))
	for _, suggestion := range diagnostic.Suggestions {
		if _, duplicate := seen[suggestion]; duplicate {
			t.Fatalf("duplicate suggestion %q", suggestion)
		}
		seen[suggestion] = struct{}{}
	}
	for _, want := range []string{
		"abs(value)",
		"trim(value)",
		"typeof(value)",
		"nullif(value, sentinel)",
		`cidrmatch("10.0.0.0/8", ip)`,
		`tostring(value, "commas")`,
	} {
		if _, present := seen[want]; !present {
			t.Fatalf("suggestions %q omit %q", diagnostic.Suggestions, want)
		}
	}
}
