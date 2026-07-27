package clickhouse

import (
	"errors"
	"strings"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/plan"
	"github.com/Suhaibinator/open-splunk/internal/splregex"
)

func TestCompileWhereMatchBindsDynamicTextAndNormalizedPatternOnce(t *testing.T) {
	t.Parallel()

	const pattern = `(?i)^api`
	normalized, err := splregex.CompileMatchPattern(pattern)
	if err != nil {
		t.Fatal(err)
	}
	compiled := compileSPL(
		t,
		`index=gradethis | where match(category, "(?i)^api")`,
	)
	for _, required := range []string{
		`CAST(match(`,
		`dynamicElement("__os_fields"."category", 'String')`,
		`CAST(NULL AS Nullable(String))`,
		`AS Nullable(Bool)`,
	} {
		if !strings.Contains(compiled.SQL, required) {
			t.Fatalf("match SQL missing %q:\n%s", required, compiled.SQL)
		}
	}
	if got := strings.Count(compiled.SQL, `"__os_fields"."category"`); got != 1 {
		t.Fatalf("Dynamic match source references = %d, want 1:\n%s", got, compiled.SQL)
	}
	if strings.Contains(compiled.SQL, pattern) || !containsArgument(compiled.Args, normalized.Pattern) {
		t.Fatalf("match pattern was not normalized and bound: args=%#v\nSQL: %s", compiled.Args, compiled.SQL)
	}
}

func TestCompileMatchSupportsFixedScalarConversionAndTextProvenance(t *testing.T) {
	t.Parallel()

	fixed := compileSPL(
		t,
		`index=gradethis | where match(42, "^42$") AND match(false, "^false$")`,
	)
	for _, required := range []string{
		`toString(CAST(? AS Int64))`,
		`toString(CAST(? AS Bool))`,
	} {
		if !strings.Contains(fixed.SQL, required) {
			t.Fatalf("fixed match SQL missing %q:\n%s", required, fixed.SQL)
		}
	}

	raw := compileSPL(
		t,
		`index=gradethis | where match(_raw, "error")`,
	)
	if !strings.Contains(raw.SQL, `"__os_raw_encoding" = 1`) {
		t.Fatalf("_raw match omitted text-eligibility guard:\n%s", raw.SQL)
	}
}

func TestCompileMatchWorksInConditionalsComparisonsAndConversion(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(
		t,
		`index=gradethis | eval label=if(match(message, "(?i)error"), "bad", "ok"), rendered=tostring(match(source, "^/api")) | where match(label, "^b")=true | table label,rendered`,
	)
	for _, required := range []string{
		`CAST(match(`,
		`transform(`,
		`'True'`,
		`'False'`,
		`AS "label"`,
		`AS "rendered"`,
	} {
		if !strings.Contains(compiled.SQL, required) {
			t.Fatalf("composed match SQL missing %q:\n%s", required, compiled.SQL)
		}
	}
}

func TestCompileMatchRejectsFixedMultivalue(t *testing.T) {
	t.Parallel()

	const source = `index=gradethis | stats values(status) AS values | where match(values, "ok")`
	_, err := (Compiler{}).Compile(buildPlan(t, source))
	var diagnostic *plan.Diagnostic
	if !errors.As(err, &diagnostic) ||
		diagnostic.Code != "SPL_UNSUPPORTED_MULTIVALUE_USAGE" {
		t.Fatalf("Compile error = %#v, want SPL_UNSUPPORTED_MULTIVALUE_USAGE", err)
	}
	if got := source[diagnostic.Range.Start.Offset:diagnostic.Range.End.Offset]; got != `match(values, "ok")` {
		t.Fatalf("diagnostic range = %q", got)
	}
}

func TestCompileMatchNestedSQLGrowsLinearly(t *testing.T) {
	t.Parallel()

	expression := "category"
	for range 12 {
		expression = `tostring(match(` + expression + `, "^x"))`
	}
	compiled := compileSPL(
		t,
		`index=gradethis | eval rendered=`+expression+` | table rendered`,
	)
	if len(compiled.SQL) > maxCompiledMatchScalarSQLBytes {
		t.Fatalf("nested match SQL = %d bytes, want at most %d", len(compiled.SQL), maxCompiledMatchScalarSQLBytes)
	}
	if got := strings.Count(compiled.SQL, `"__os_fields"."category"`); got != 1 {
		t.Fatalf("nested match source references = %d, want 1:\n%s", got, compiled.SQL)
	}
}

func TestCompileMatchRejectsForgedPlans(t *testing.T) {
	t.Parallel()

	base := buildPlan(t, `index=gradethis`)
	value := func() plan.ScalarExpression {
		return &plan.ScalarFieldExpression{
			Field: plan.FieldRef{Name: "message", Path: []string{"message"}},
		}
	}
	pattern := func(text string) plan.ScalarExpression {
		return &plan.ScalarLiteralExpression{
			Value: plan.Value{Kind: plan.ValueKindString, String: text},
		}
	}
	var typedNil *plan.ScalarLiteralExpression
	boolean := &plan.ScalarCallExpression{
		Function: plan.ScalarFunctionIsNull, Arguments: []plan.ScalarExpression{value()},
	}
	cyclic := &plan.ScalarCallExpression{Function: plan.ScalarFunctionMatch}
	cyclic.Arguments = []plan.ScalarExpression{cyclic, pattern("x")}
	for _, test := range []struct {
		name       string
		expression plan.ScalarExpression
		want       string
		wantCode   string
	}{
		{
			name: "zero arguments",
			expression: &plan.ScalarCallExpression{
				Function: plan.ScalarFunctionMatch,
			},
			want: "expected two arguments",
		},
		{
			name: "one argument",
			expression: &plan.ScalarCallExpression{
				Function: plan.ScalarFunctionMatch, Arguments: []plan.ScalarExpression{value()},
			},
			want: "expected two arguments",
		},
		{
			name: "three arguments",
			expression: &plan.ScalarCallExpression{
				Function:  plan.ScalarFunctionMatch,
				Arguments: []plan.ScalarExpression{value(), pattern("x"), pattern("y")},
			},
			want: "expected two arguments",
		},
		{
			name: "typed nil pattern",
			expression: &plan.ScalarCallExpression{
				Function:  plan.ScalarFunctionMatch,
				Arguments: []plan.ScalarExpression{value(), typedNil},
			},
			want: "regular expression must be a string literal",
		},
		{
			name: "nonliteral pattern",
			expression: &plan.ScalarCallExpression{
				Function:  plan.ScalarFunctionMatch,
				Arguments: []plan.ScalarExpression{value(), value()},
			},
			want: "regular expression must be a string literal",
		},
		{
			name: "Boolean input",
			expression: &plan.ScalarCallExpression{
				Function:  plan.ScalarFunctionMatch,
				Arguments: []plan.ScalarExpression{boolean, pattern("true")},
			},
			want: "cannot consume a Boolean",
		},
		{
			name: "invalid regex",
			expression: &plan.ScalarCallExpression{
				Function:  plan.ScalarFunctionMatch,
				Arguments: []plan.ScalarExpression{value(), pattern(`(?=secret)`)},
			},
			want: "outside the supported RE2 subset",
		},
		{
			name: "oversized regex text",
			expression: &plan.ScalarCallExpression{
				Function: plan.ScalarFunctionMatch,
				Arguments: []plan.ScalarExpression{
					value(),
					pattern(strings.Repeat("x", splregex.MaximumMatchPatternBytes+1)),
				},
			},
			want:     "byte or",
			wantCode: "SPL_QUERY_TOO_COMPLEX",
		},
		{
			name: "oversized regex program",
			expression: &plan.ScalarCallExpression{
				Function: plan.ScalarFunctionMatch,
				Arguments: []plan.ScalarExpression{
					value(),
					pattern(strings.Repeat("a{1000}", 5)),
				},
			},
			want:     "work-unit limit",
			wantCode: "SPL_QUERY_TOO_COMPLEX",
		},
		{
			name:       "cyclic expression",
			expression: cyclic,
			want:       "contains a cycle",
		},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			err := compileForgedScalarAssignment(
				t,
				base,
				&plan.ScalarCallExpression{
					Function:  plan.ScalarFunctionMVCount,
					Arguments: []plan.ScalarExpression{test.expression},
				},
			)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("Compile error = %v, want %q", err, test.want)
			}
			if test.wantCode != "" {
				var diagnostic *plan.Diagnostic
				if !errors.As(err, &diagnostic) || diagnostic.Code != test.wantCode {
					t.Fatalf("Compile error = %#v, want code %s", err, test.wantCode)
				}
			}
		})
	}
}

func TestCompileMatchBoundsAggregateRegexWorkAndCalculatedInputBytes(t *testing.T) {
	t.Parallel()

	pattern := strings.Repeat("a{1000}", 4)
	predicates := make([]string, 5)
	for index := range predicates {
		predicates[index] = `match(message, "` + pattern + `")`
	}
	_, err := (Compiler{}).Compile(buildPlan(
		t,
		`index=gradethis | where `+strings.Join(predicates, " OR "),
	))
	var diagnostic *plan.Diagnostic
	if !errors.As(err, &diagnostic) ||
		diagnostic.Code != "SPL_QUERY_TOO_COMPLEX" ||
		!strings.Contains(diagnostic.Message, "match programs") {
		t.Fatalf("aggregate regex error = %#v, want match program budget", err)
	}

	for _, source := range []string{
		`index=gradethis | where match(replace(message, "x", "12345"), "ok")`,
		`index=gradethis | eval amplified=replace(message, "x", "12345") | where match(amplified, "ok")`,
	} {
		_, err = (Compiler{}).Compile(buildPlan(t, source))
		diagnostic = nil
		if !errors.As(err, &diagnostic) ||
			diagnostic.Code != "SPL_QUERY_TOO_COMPLEX" ||
			!strings.Contains(diagnostic.Message, "match input") {
			t.Fatalf("calculated input error = %#v, want match input byte budget", err)
		}
	}
}
