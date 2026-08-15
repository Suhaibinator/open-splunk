package clickhouse

import (
	"errors"
	"strings"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/plan"
	"github.com/Suhaibinator/open-splunk/internal/splwildcard"
)

func TestCompileWhereLikeBindsDynamicTextAndNormalizedPatternOnce(t *testing.T) {
	t.Parallel()

	const authored = `%%%api%%`
	normalized, err := splwildcard.CompileLikePattern(authored)
	if err != nil {
		t.Fatal(err)
	}
	compiled := compileSPL(
		t,
		`index=gradethis | where like(category, "%%%api%%")`,
	)
	for _, required := range []string{
		`CAST(like(`,
		`dynamicElement("__os_fields"."category", 'String')`,
		`CAST(NULL AS Nullable(String))`,
		`AS Nullable(Bool)`,
	} {
		if !strings.Contains(compiled.SQL, required) {
			t.Fatalf("like SQL missing %q:\n%s", required, compiled.SQL)
		}
	}
	if got := strings.Count(compiled.SQL, `"__os_fields"."category"`); got != 1 {
		t.Fatalf("Dynamic like source references = %d, want 1:\n%s", got, compiled.SQL)
	}
	if strings.Contains(compiled.SQL, authored) ||
		!containsArgument(compiled.Args, normalized.Pattern) {
		t.Fatalf(
			"like pattern was not normalized and bound: args=%#v\nSQL: %s",
			compiled.Args,
			compiled.SQL,
		)
	}
}

func TestCompileLikeSupportsFixedScalarConversionAndTextProvenance(t *testing.T) {
	t.Parallel()

	fixed := compileSPL(
		t,
		`index=gradethis | where like(42, "4_") AND like(false, "false")`,
	)
	for _, required := range []string{
		`toString(CAST(? AS Int64))`,
		`toString(CAST(? AS Bool))`,
	} {
		if !strings.Contains(fixed.SQL, required) {
			t.Fatalf("fixed like SQL missing %q:\n%s", required, fixed.SQL)
		}
	}

	raw := compileSPL(
		t,
		`index=gradethis | where like(_raw, "%error%")`,
	)
	if !strings.Contains(raw.SQL, `"__os_raw_encoding" = 1`) {
		t.Fatalf("_raw like omitted text-eligibility guard:\n%s", raw.SQL)
	}
}

func TestCompileLikeWorksInConditionalsComparisonsAndConversion(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(
		t,
		`index=gradethis | eval label=if(like(message, "%error%"), "bad", "ok"), rendered=tostring(like(source, "/api%")) | where like(label, "b%")=true | table label,rendered`,
	)
	for _, required := range []string{
		`CAST(like(`,
		`transform(`,
		`'True'`,
		`'False'`,
		`AS "label"`,
		`AS "rendered"`,
	} {
		if !strings.Contains(compiled.SQL, required) {
			t.Fatalf("composed like SQL missing %q:\n%s", required, compiled.SQL)
		}
	}
}

func TestCompileLikeRejectsFixedMultivalue(t *testing.T) {
	t.Parallel()

	const source = `index=gradethis | stats values(status) AS values | where like(values, "%ok%")`
	_, err := (Compiler{}).Compile(buildPlan(t, source))
	var diagnostic *plan.Diagnostic
	if !errors.As(err, &diagnostic) ||
		diagnostic.Code != "SPL_UNSUPPORTED_MULTIVALUE_USAGE" {
		t.Fatalf("Compile error = %#v, want SPL_UNSUPPORTED_MULTIVALUE_USAGE", err)
	}
	if got := source[diagnostic.Range.Start.Offset:diagnostic.Range.End.Offset]; got != `like(values, "%ok%")` {
		t.Fatalf("diagnostic range = %q", got)
	}
}

func TestCompileLikeNestedSQLGrowsLinearly(t *testing.T) {
	t.Parallel()

	expression := "category"
	for range 12 {
		expression = `tostring(like(` + expression + `, "x%"))`
	}
	compiled := compileSPL(
		t,
		`index=gradethis | eval rendered=`+expression+` | table rendered`,
	)
	if len(compiled.SQL) > maxCompiledLikeScalarSQLBytes {
		t.Fatalf(
			"nested like SQL = %d bytes, want at most %d",
			len(compiled.SQL),
			maxCompiledLikeScalarSQLBytes,
		)
	}
	if got := strings.Count(compiled.SQL, `"__os_fields"."category"`); got != 1 {
		t.Fatalf("nested like source references = %d, want 1:\n%s", got, compiled.SQL)
	}
}

func TestCompileLikeRejectsForgedPlans(t *testing.T) {
	t.Parallel()

	base := buildPlan(t, `index=gradethis`)
	value := func() plan.ScalarExpression {
		return &plan.ScalarFieldExpression{
			Field: plan.FieldRef{Name: "message", Path: []string{"message"}},
		}
	}
	pattern := func(text string) plan.ScalarExpression {
		return &plan.ScalarLiteralExpression{
			Value: plan.Value{Kind: plan.ValueKindString, String: text, Quoted: true},
		}
	}
	var typedNil *plan.ScalarLiteralExpression
	boolean := &plan.ScalarCallExpression{
		Function: plan.ScalarFunctionIsNull, Arguments: []plan.ScalarExpression{value()},
	}
	cyclic := &plan.ScalarCallExpression{Function: plan.ScalarFunctionLike}
	cyclic.Arguments = []plan.ScalarExpression{cyclic, pattern("x%")}
	for _, test := range []struct {
		name       string
		expression plan.ScalarExpression
		want       string
		wantCode   string
	}{
		{
			name: "zero arguments",
			expression: &plan.ScalarCallExpression{
				Function: plan.ScalarFunctionLike,
			},
			want: "expected two arguments",
		},
		{
			name: "one argument",
			expression: &plan.ScalarCallExpression{
				Function: plan.ScalarFunctionLike, Arguments: []plan.ScalarExpression{value()},
			},
			want: "expected two arguments",
		},
		{
			name: "three arguments",
			expression: &plan.ScalarCallExpression{
				Function:  plan.ScalarFunctionLike,
				Arguments: []plan.ScalarExpression{value(), pattern("x"), pattern("y")},
			},
			want: "expected two arguments",
		},
		{
			name: "typed nil pattern",
			expression: &plan.ScalarCallExpression{
				Function:  plan.ScalarFunctionLike,
				Arguments: []plan.ScalarExpression{value(), typedNil},
			},
			want: "pattern must be a string literal",
		},
		{
			name: "nonliteral pattern",
			expression: &plan.ScalarCallExpression{
				Function:  plan.ScalarFunctionLike,
				Arguments: []plan.ScalarExpression{value(), value()},
			},
			want: "pattern must be a string literal",
		},
		{
			name: "unquoted pattern",
			expression: &plan.ScalarCallExpression{
				Function: plan.ScalarFunctionLike,
				Arguments: []plan.ScalarExpression{
					value(),
					&plan.ScalarLiteralExpression{
						Value: plan.Value{Kind: plan.ValueKindString, String: "%"},
					},
				},
			},
			want: "pattern must be a string literal",
		},
		{
			name: "Boolean input",
			expression: &plan.ScalarCallExpression{
				Function:  plan.ScalarFunctionLike,
				Arguments: []plan.ScalarExpression{boolean, pattern("true")},
			},
			want: "cannot consume a Boolean",
		},
		{
			name: "invalid pattern",
			expression: &plan.ScalarCallExpression{
				Function:  plan.ScalarFunctionLike,
				Arguments: []plan.ScalarExpression{value(), pattern("bad\x00pattern")},
			},
			want:     "valid UTF-8 without NUL",
			wantCode: "SPL_UNSUPPORTED_LIKE_PATTERN",
		},
		{
			name: "trailing escape",
			expression: &plan.ScalarCallExpression{
				Function:  plan.ScalarFunctionLike,
				Arguments: []plan.ScalarExpression{value(), pattern("bad\\")},
			},
			want:     "valid UTF-8 without NUL",
			wantCode: "SPL_UNSUPPORTED_LIKE_PATTERN",
		},
		{
			name: "oversized pattern",
			expression: &plan.ScalarCallExpression{
				Function: plan.ScalarFunctionLike,
				Arguments: []plan.ScalarExpression{
					value(),
					pattern(strings.Repeat("x", splwildcard.MaximumLikePatternBytes+1)),
				},
			},
			want:     "byte or",
			wantCode: "SPL_QUERY_TOO_COMPLEX",
		},
		{
			name:       "cyclic expression",
			expression: cyclic,
			want:       "contains a cycle",
		},
	} {
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

func TestCompileLikeBoundsAggregatePatternWorkAndCalculatedInputBytes(t *testing.T) {
	t.Parallel()

	patternText := strings.Repeat("x", splwildcard.MaximumLikePatternWorkUnits)
	expression := plan.ScalarExpression(&plan.ScalarFieldExpression{
		Field: plan.FieldRef{Name: "message", Path: []string{"message"}},
	})
	for range splwildcard.MaximumLikeQueryPatternWorkUnits/
		splwildcard.MaximumLikePatternWorkUnits + 1 {
		expression = &plan.ScalarCallExpression{
			Function: plan.ScalarFunctionToString,
			Arguments: []plan.ScalarExpression{&plan.ScalarCallExpression{
				Function: plan.ScalarFunctionLike,
				Arguments: []plan.ScalarExpression{
					expression,
					&plan.ScalarLiteralExpression{
						Value: plan.Value{
							Kind:   plan.ValueKindString,
							String: patternText,
							Quoted: true,
						},
					},
				},
			}},
		}
	}
	err := compileForgedScalarAssignment(
		t,
		buildPlan(t, `index=gradethis`),
		expression,
	)
	var diagnostic *plan.Diagnostic
	if !errors.As(err, &diagnostic) ||
		diagnostic.Code != "SPL_QUERY_TOO_COMPLEX" ||
		!strings.Contains(diagnostic.Message, "like patterns") {
		t.Fatalf("aggregate pattern error = %#v, want like pattern budget", err)
	}

	for _, source := range []string{
		`index=gradethis | where like(replace(message, "x", "12345"), "%ok%")`,
		`index=gradethis | eval amplified=replace(message, "x", "12345") | where like(amplified, "%ok%")`,
	} {
		_, err = (Compiler{}).Compile(buildPlan(t, source))
		diagnostic = nil
		if !errors.As(err, &diagnostic) ||
			diagnostic.Code != "SPL_QUERY_TOO_COMPLEX" ||
			!strings.Contains(diagnostic.Message, "like input") {
			t.Fatalf("calculated input error = %#v, want like input byte budget", err)
		}
	}
}

func TestCompileLikePreservesCalculatedInputBoundsAcrossRetainingOperators(t *testing.T) {
	t.Parallel()

	for _, source := range []string{
		`index=gradethis | eval amplified=replace(message, "x", "12345") | rex field=message "(?<amplified>never)" | where like(amplified, "%")`,
		`index=gradethis | eval amplified=replace(message, "x", "12345") | spath input=message output=amplified path=never | where like(amplified, "%")`,
		`index=gradethis | eval amplified=replace(message, "x", "12345") | stats count BY amplified | where like(amplified, "%")`,
		`index=gradethis | eval amplified=replace(message, "x", "12345") | stats min(amplified) AS retained | where like(retained, "%")`,
		`index=gradethis | eval amplified=replace(message, "x", "12345") | stats max(amplified) AS retained | where like(retained, "%")`,
		`index=gradethis | eval amplified=replace(message, "x", "12345") | stats earliest(amplified) AS retained | where like(retained, "%")`,
		`index=gradethis | eval amplified=replace(message, "x", "12345") | stats latest(amplified) AS retained | where like(retained, "%")`,
	} {
		_, err := (Compiler{}).Compile(buildPlan(t, source))
		var diagnostic *plan.Diagnostic
		if !errors.As(err, &diagnostic) ||
			diagnostic.Code != "SPL_QUERY_TOO_COMPLEX" ||
			!strings.Contains(diagnostic.Message, "like input") {
			t.Fatalf("%q error = %#v, want like input byte budget", source, err)
		}
	}
}

func TestCompileLikeBoundsAggregateInputScanning(t *testing.T) {
	t.Parallel()

	sourceWithCalls := func(calls int) string {
		predicates := make([]string, calls)
		for index := range predicates {
			predicates[index] = `like(message, "%z")`
		}
		return `index=gradethis | where ` + strings.Join(predicates, " OR ")
	}

	if _, err := (Compiler{}).Compile(buildPlan(
		t,
		sourceWithCalls(int(MaximumLikeQueryInputBytes/MaximumStoredScalarBytes)),
	)); err != nil {
		t.Fatalf("Compile aggregate input boundary: %v", err)
	}

	_, err := (Compiler{}).Compile(buildPlan(
		t,
		sourceWithCalls(int(MaximumLikeQueryInputBytes/MaximumStoredScalarBytes)+1),
	))
	var diagnostic *plan.Diagnostic
	if !errors.As(err, &diagnostic) ||
		diagnostic.Code != "SPL_QUERY_TOO_COMPLEX" ||
		!strings.Contains(diagnostic.Message, "wildcard scanning") {
		t.Fatalf("aggregate input error = %#v, want like scan byte budget", err)
	}
}

func TestCompileLikeUsesFixedNumericTextBounds(t *testing.T) {
	t.Parallel()

	compileSPL(
		t,
		`index=gradethis | stats count AS n | eval rendered=replace(tostring(n), "0", "12345") | where like(rendered, "%")`,
	)
}
