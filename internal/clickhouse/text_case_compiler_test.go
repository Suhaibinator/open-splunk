package clickhouse

import (
	"errors"
	"strings"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/plan"
)

func TestCompileEvalLowerUpperFixedStringsAreTypedAndParameterized(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(
		t,
		`index=gradethis | eval folded=lower("MÜNCHEN"), shouted=upper(folded), absent=lower(null) | table folded,shouted,absent`,
	)
	for _, required := range []string{
		`lowerUTF8(CAST(? AS String)) AS "folded"`,
		`upperUTF8("folded") AS "shouted"`,
		`lowerUTF8(toString(CAST(NULL AS Nullable(String)))) AS "absent"`,
	} {
		if !strings.Contains(compiled.SQL, required) {
			t.Fatalf("text-case SQL missing %q:\n%s", required, compiled.SQL)
		}
	}
	wantPrefix := []any{"MÜNCHEN"}
	if len(compiled.Args) < len(wantPrefix) {
		t.Fatalf("args = %#v, want prefix %#v", compiled.Args, wantPrefix)
	}
	for index, want := range wantPrefix {
		if compiled.Args[index] != want {
			t.Fatalf("arg %d = %#v, want %#v\nall args: %#v", index, compiled.Args[index], want, compiled.Args)
		}
	}
	if got, want := strings.Count(compiled.SQL, "?"), len(compiled.Args); got != want {
		t.Fatalf("placeholder count = %d, args = %d\nSQL: %s", got, want, compiled.SQL)
	}
}

func TestCompileEvalLowerUpperRejectsFixedNonStringInputs(t *testing.T) {
	t.Parallel()

	for _, source := range []string{
		`index=gradethis | eval value=lower(123)`,
		`index=gradethis | eval value=upper(true)`,
		`index=gradethis | eval value=lower(_time)`,
	} {
		logical := buildPlan(t, source)
		_, err := (Compiler{}).Compile(logical)
		var diagnostic *plan.Diagnostic
		if !errors.As(err, &diagnostic) ||
			diagnostic.Code != "SPL_UNSUPPORTED_TEXT_CASE_VALUE_TYPE" {
			t.Fatalf(
				"Compile(%q) error = %#v, want SPL_UNSUPPORTED_TEXT_CASE_VALUE_TYPE",
				source,
				err,
			)
		}
		if diagnostic.Range.Start.Offset >= diagnostic.Range.End.Offset ||
			!strings.Contains(
				source[diagnostic.Range.Start.Offset:diagnostic.Range.End.Offset],
				"(",
			) {
			t.Fatalf("Compile(%q) diagnostic range = %#v", source, diagnostic.Range)
		}
	}
}

func TestCompileEvalLowerUpperFailClosedForBinaryRaw(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(
		t,
		`index=gradethis | eval normalized=lower(_raw), copied=upper(normalized) | table normalized,copied`,
	)
	for _, required := range []string{
		`lowerUTF8(if(ifNull("__os_raw_encoding" = 1, 0), "_raw", CAST(NULL AS Nullable(String)))) AS "normalized"`,
		`upperUTF8("normalized") AS "copied"`,
	} {
		if !strings.Contains(compiled.SQL, required) {
			t.Fatalf("raw text-case SQL missing %q:\n%s", required, compiled.SQL)
		}
	}

	replaced := compileSPL(
		t,
		`index=gradethis | eval normalized=lower(replace(_raw, "x", "y")) | table normalized`,
	)
	if !strings.Contains(
		replaced.SQL,
		`lowerUTF8(if(ifNull("__os_raw_encoding" = 1, 0), replaceRegexpAll("_raw", ?, ?), CAST(NULL AS Nullable(String))))`,
	) {
		t.Fatalf("replace did not preserve raw text provenance:\n%s", replaced.SQL)
	}
}

func TestCompileEvalLowerUpperSupportsDynamicStringAndStringArrayWithoutExpansion(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(
		t,
		`index=gradethis | eval normalized=lower(category), shouted=upper(normalized) | table normalized,shouted`,
	)
	for _, required := range []string{
		`arrayElement(arrayMap(value -> multiIf(`,
		`dynamicType(value) = 'String'`,
		`CAST(lowerUTF8(dynamicElement(value, 'String')) AS Dynamic)`,
		`dynamicType(value) = 'Array(String)'`,
		`CAST(arrayMap(element -> lowerUTF8(element), dynamicElement(value, 'Array(String)')) AS Dynamic)`,
		`dynamicType(value) = 'Array(Dynamic)' AND arrayAll(element -> dynamicType(element) = 'String'`,
		`lowerUTF8(assumeNotNull(dynamicElement(element, 'String')))`,
		`CAST(NULL AS Dynamic)`,
		`CAST(upperUTF8(dynamicElement(value, 'String')) AS Dynamic)`,
		`CAST(arrayMap(element -> upperUTF8(element), dynamicElement(value, 'Array(String)')) AS Dynamic)`,
	} {
		if !strings.Contains(compiled.SQL, required) {
			t.Fatalf("dynamic text-case SQL missing %q:\n%s", required, compiled.SQL)
		}
	}
	if strings.Contains(strings.ToUpper(compiled.SQL), "ARRAY JOIN") {
		t.Fatalf("dynamic text case introduced row expansion:\n%s", compiled.SQL)
	}
}

func TestCompileEvalLowerUpperMapsFixedMultivalueStrings(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(
		t,
		`index=gradethis | stats values(user) AS users | eval normalized=upper(users) | table normalized`,
	)
	for _, required := range []string{
		`arrayAll(element -> isValidUTF8(element), values)`,
		`arrayMap(element -> upperUTF8(element), values)`,
		`CAST([], 'Array(String)')`,
	} {
		if !strings.Contains(compiled.SQL, required) {
			t.Fatalf("fixed multivalue text-case SQL missing %q:\n%s", required, compiled.SQL)
		}
	}
	if strings.Count(compiled.SQL, `["users"]`) != 1 {
		t.Fatalf("fixed multivalue text case duplicated its input:\n%s", compiled.SQL)
	}
	if strings.Contains(strings.ToUpper(compiled.SQL), "ARRAY JOIN") {
		t.Fatalf("fixed multivalue text case introduced row expansion:\n%s", compiled.SQL)
	}

	presence := compileSPL(
		t,
		`index=gradethis | stats values(user) AS users | eval normalized=lower(users) | where isnull(normalized)`,
	)
	if !strings.Contains(
		presence.SQL,
		`notEmpty("normalized")`,
	) {
		t.Fatalf("fixed multivalue logical presence was not preserved:\n%s", presence.SQL)
	}
}

func TestCompileEvalLowerUpperPreservesRawTextEligibilityThroughStats(t *testing.T) {
	t.Parallel()

	for _, aggregate := range []string{"values", "list"} {
		compiled := compileSPL(
			t,
			`index=gradethis | stats `+aggregate+`(_raw) AS raws | eval normalized=lower(raws) | table normalized`,
		)
		if !strings.Contains(compiled.SQL, `"__os_raw_encoding" = 1`) {
			t.Fatalf("%s(_raw) discarded the raw text eligibility guard:\n%s", aggregate, compiled.SQL)
		}
		if !strings.Contains(compiled.SQL, `arrayAll(element -> isValidUTF8(element), values)`) {
			t.Fatalf("%s(_raw) text case omitted its UTF-8 array guard:\n%s", aggregate, compiled.SQL)
		}
	}
}

func TestCompileLowerUpperCanFeedWhereWithoutChangingCaseSensitivity(t *testing.T) {
	t.Parallel()

	fixed := compileSPL(
		t,
		`index=gradethis | where lower(source)="api" AND upper(host)="EDGE"`,
	)
	for _, required := range []string{
		`toString(lowerUTF8("source")) = CAST(? AS String)`,
		`toString(upperUTF8("host")) = CAST(? AS String)`,
	} {
		if !strings.Contains(fixed.SQL, required) {
			t.Fatalf("fixed where text-case SQL missing %q:\n%s", required, fixed.SQL)
		}
	}
	if strings.Contains(fixed.SQL, `lowerUTF8(CAST(? AS String))`) {
		t.Fatalf("where changed eval's case-sensitive literal comparison:\n%s", fixed.SQL)
	}

	dynamic := compileSPL(
		t,
		`index=gradethis | where upper(category)="API"`,
	)
	if !strings.Contains(dynamic.SQL, `dynamicType(value) = 'Array(String)'`) ||
		!strings.Contains(dynamic.SQL, `dynamicElement(`) {
		t.Fatalf("dynamic where text-case SQL lost runtime typing:\n%s", dynamic.SQL)
	}
	if got := strings.Count(dynamic.SQL, `"__os_fields"."category"`); got != 1 {
		t.Fatalf("dynamic where text case references its source %d times, want 1:\n%s", got, dynamic.SQL)
	}
	for _, forbidden := range []string{"'decimal/v1'", "startsWith(dynamicType("} {
		if strings.Contains(dynamic.SQL, forbidden) {
			t.Fatalf("dynamic text-only comparison retained generic branch %q:\n%s", forbidden, dynamic.SQL)
		}
	}

	twoDynamic := compileSPL(
		t,
		`index=gradethis | where lower(category)=upper(other)`,
	)
	for _, field := range []string{"category", "other"} {
		path := `"__os_fields"."` + field + `"`
		if got := strings.Count(twoDynamic.SQL, path); got != 1 {
			t.Fatalf("dynamic text comparison references %s %d times, want 1:\n%s", field, got, twoDynamic.SQL)
		}
	}
}

func TestCompileEvalLowerUpperNestedDynamicSQLGrowsLinearly(t *testing.T) {
	t.Parallel()

	expression := "category"
	for range 31 {
		expression = "lower(" + expression + ")"
	}
	compiled := compileSPL(
		t,
		`index=gradethis | eval normalized=`+expression+` | table normalized`,
	)
	if len(compiled.SQL) > maxCompiledTextCaseScalarSQLBytes {
		t.Fatalf("nested text-case SQL = %d bytes, want at most %d", len(compiled.SQL), maxCompiledTextCaseScalarSQLBytes)
	}
	if strings.Count(compiled.SQL, `"__os_fields"."category"`) != 1 {
		t.Fatalf("nested dynamic input was duplicated:\n%s", compiled.SQL)
	}
}

func TestCompileEvalLowerUpperRejectsForgedPlans(t *testing.T) {
	t.Parallel()

	base := buildPlan(t, `index=gradethis`)
	literal := func() plan.ScalarExpression {
		return &plan.ScalarLiteralExpression{
			Value: plan.Value{Kind: plan.ValueKindString, String: "value"},
		}
	}
	compileAssignment := func(expression plan.ScalarExpression) error {
		t.Helper()
		candidate := *base
		candidate.Operators = append(
			append([]plan.Operator(nil), base.Operators...),
			&plan.Extend{Assignments: []plan.ExtendAssignment{{
				Output:     plan.FieldRef{Name: "value"},
				Expression: expression,
			}}},
		)
		_, err := (Compiler{}).Compile(&candidate)
		return err
	}

	var typedNil *plan.ScalarLiteralExpression
	boolean := &plan.ScalarCallExpression{
		Function: plan.ScalarFunctionIsNull,
		Arguments: []plan.ScalarExpression{
			literal(),
		},
	}
	cyclic := &plan.ScalarCallExpression{Function: plan.ScalarFunctionLower}
	cyclic.Arguments = []plan.ScalarExpression{cyclic}
	for _, test := range []struct {
		name       string
		expression plan.ScalarExpression
		want       string
	}{
		{
			name: "zero arguments",
			expression: &plan.ScalarCallExpression{
				Function: plan.ScalarFunctionLower,
			},
			want: "expected one argument",
		},
		{
			name: "two arguments",
			expression: &plan.ScalarCallExpression{
				Function:  plan.ScalarFunctionUpper,
				Arguments: []plan.ScalarExpression{literal(), literal()},
			},
			want: "expected one argument",
		},
		{
			name: "typed nil argument",
			expression: &plan.ScalarCallExpression{
				Function:  plan.ScalarFunctionLower,
				Arguments: []plan.ScalarExpression{typedNil},
			},
			want: "missing scalar expression",
		},
		{
			name: "Boolean null predicate",
			expression: &plan.ScalarCallExpression{
				Function:  plan.ScalarFunctionUpper,
				Arguments: []plan.ScalarExpression{boolean},
			},
			want: "cannot consume a Boolean",
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
			err := compileAssignment(test.expression)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("Compile error = %v, want %q", err, test.want)
			}
		})
	}
}
