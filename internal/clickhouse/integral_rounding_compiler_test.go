package clickhouse

import (
	"errors"
	"math"
	"strings"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/plan"
)

func TestCompileEvalIntegralRoundingFixedNumbersAreTypedAndAliasesNormalize(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(
		t,
		`index=gradethis | eval up=ceil(1.2), alias=ceiling(-1.2), down=floor(-1.2), signed=ceil(-42), unsigned=floor(18446744073709551615), absent=ceil(null) | table up,alias,down,signed,unsigned,absent`,
	)
	for _, required := range []string{
		`ceil(CAST(? AS Float64)) AS "up"`,
		`ceil(CAST(? AS Float64)) AS "alias"`,
		`floor(CAST(? AS Float64)) AS "down"`,
		`CAST(? AS Int64) AS "signed"`,
		`CAST(? AS UInt64) AS "unsigned"`,
		`CAST(NULL AS Nullable(Float64)) AS "absent"`,
	} {
		if !strings.Contains(compiled.SQL, required) {
			t.Fatalf("integral-rounding SQL missing %q:\n%s", required, compiled.SQL)
		}
	}
	for _, argument := range []any{
		float64(1.2),
		float64(-1.2),
		int64(-42),
		uint64(math.MaxUint64),
	} {
		if !containsArgument(compiled.Args, argument) {
			t.Fatalf("args = %#v, missing %#v", compiled.Args, argument)
		}
	}
	if got, want := strings.Count(compiled.SQL, "?"), len(compiled.Args); got != want {
		t.Fatalf("placeholder count = %d, args = %d\nSQL: %s", got, want, compiled.SQL)
	}
}

func TestCompileEvalIntegralRoundingSupportsDynamicNumbersOnce(t *testing.T) {
	t.Parallel()

	for _, function := range []string{"ceil", "ceiling", "floor"} {
		compiled := compileSPL(
			t,
			`index=gradethis | where `+function+`(category)=3`,
		)
		clickHouseFunction := function
		if clickHouseFunction == "ceiling" {
			clickHouseFunction = "ceil"
		}
		for _, required := range []string{
			`arrayElement(arrayMap(value ->`,
			`arrayElement(arrayMap(exact_value -> multiIf(`,
			`dynamicType(value) IN ('Int8', 'Int16'`,
			`'decimal/v1'`,
			`open_splunk_value`,
			`accurateCastOrNull(value, 'Float64')`,
			clickHouseFunction + `(`,
			`CAST(NULL AS Nullable(Float64))`,
		} {
			if !strings.Contains(compiled.SQL, required) {
				t.Fatalf("%s Dynamic SQL missing %q:\n%s", function, required, compiled.SQL)
			}
		}
		if got := strings.Count(compiled.SQL, `"__os_fields"."category"`); got != 1 {
			t.Fatalf("%s Dynamic source references = %d, want 1:\n%s", function, got, compiled.SQL)
		}
		for _, forbidden := range []string{
			`dynamicType(value) = 'String'`,
			`dynamicType(value) = 'Bool'`,
			`Array(String)`,
			`ARRAY JOIN`,
			`toFloat64OrNull(toString(value))`,
			`dynamicElement(left_value, 'Map(String, String)')`,
			`dynamicElement(left_value, 'String')`,
			`dynamicElement(left_value, 'Bool')`,
		} {
			if strings.Contains(compiled.SQL, forbidden) {
				t.Fatalf("%s Dynamic SQL retained unsupported branch %q:\n%s", function, forbidden, compiled.SQL)
			}
		}
	}
}

func TestCompileEvalIntegralRoundingTextOnlyDynamicInputIsStaticallyNull(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(
		t,
		`index=gradethis | eval up=ceil(lower(category)), down=floor(upper(category)) | table up,down`,
	)
	if got := strings.Count(
		compiled.SQL,
		`CAST(NULL AS Nullable(Float64))`,
	); got < 2 {
		t.Fatalf("text-only integral rounding did not become static null:\n%s", compiled.SQL)
	}
	for _, forbidden := range []string{
		`ceil(multiIf(`,
		`floor(multiIf(`,
		`'decimal/v1'`,
	} {
		if strings.Contains(compiled.SQL, forbidden) {
			t.Fatalf("text-only integral rounding retained unused branch %q:\n%s", forbidden, compiled.SQL)
		}
	}
}

func TestCompileEvalIntegralRoundingClosedSchemaMissingInputIsNull(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(
		t,
		`index=gradethis | stats count | eval up=ceil(absent), down=floor(absent) | table up,down`,
	)
	if got := strings.Count(
		compiled.SQL,
		`CAST(NULL AS Nullable(Float64))`,
	); got < 2 {
		t.Fatalf("closed-schema missing integral rounding did not become null:\n%s", compiled.SQL)
	}
	for _, forbidden := range []string{`ceil(CAST(NULL`, `floor(CAST(NULL`} {
		if strings.Contains(compiled.SQL, forbidden) {
			t.Fatalf("closed-schema missing input retained %q:\n%s", forbidden, compiled.SQL)
		}
	}
}

func TestCompileEvalIntegralRoundingRejectsFixedNonNumbersAndMultivalue(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		source string
		code   string
	}{
		{
			source: `index=gradethis | eval rounded=ceil("2.5")`,
			code:   "SPL_UNSUPPORTED_CEIL_VALUE_TYPE",
		},
		{
			source: `index=gradethis | eval rounded=ceiling(true)`,
			code:   "SPL_UNSUPPORTED_CEIL_VALUE_TYPE",
		},
		{
			source: `index=gradethis | eval rounded=floor(_time)`,
			code:   "SPL_UNSUPPORTED_FLOOR_VALUE_TYPE",
		},
		{
			source: `index=gradethis | stats values(status) AS values | eval rounded=ceil(values)`,
			code:   "SPL_UNSUPPORTED_MULTIVALUE_USAGE",
		},
	} {
		logical := buildPlan(t, test.source)
		_, err := (Compiler{}).Compile(logical)
		var diagnostic *plan.Diagnostic
		if !errors.As(err, &diagnostic) || diagnostic.Code != test.code {
			t.Fatalf("Compile(%q) error = %#v, want %s", test.source, err, test.code)
		}
		if diagnostic.Range.Start.Offset >= diagnostic.Range.End.Offset {
			t.Fatalf("Compile(%q) diagnostic range = %#v", test.source, diagnostic.Range)
		}
	}
}

func TestCompileEvalIntegralRoundingNestedDynamicIdentitiesCollapse(t *testing.T) {
	t.Parallel()

	expression := "category"
	for index := range 24 {
		if index%2 == 0 {
			expression = "ceil(" + expression + ")"
		} else {
			expression = "floor(" + expression + ")"
		}
	}
	compiled := compileSPL(
		t,
		`index=gradethis | eval rounded=`+expression+` | table rounded`,
	)
	if len(compiled.SQL) > maxCompiledNumericRoundingScalarSQLBytes {
		t.Fatalf(
			"nested integral-rounding SQL = %d bytes, want at most %d",
			len(compiled.SQL),
			maxCompiledNumericRoundingScalarSQLBytes,
		)
	}
	if strings.Count(compiled.SQL, `"__os_fields"."category"`) != 1 {
		t.Fatalf("nested Dynamic input was duplicated:\n%s", compiled.SQL)
	}
	if got := strings.Count(compiled.SQL, "ceil(") +
		strings.Count(compiled.SQL, "floor("); got != 1 {
		t.Fatalf("nested integral-rounding operations = %d, want one:\n%s", got, compiled.SQL)
	}
}

func TestCompileEvalIntegralRoundingIdentitySurvivesAssignmentAndRename(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(
		t,
		`index=gradethis | eval integral=ceil(category) | rename integral AS renamed | eval collapsed=floor(renamed) | table collapsed`,
	)
	if got := strings.Count(compiled.SQL, "ceil("); got != 1 {
		t.Fatalf("ceil operations = %d, want one:\n%s", got, compiled.SQL)
	}
	if strings.Contains(compiled.SQL, "floor(") {
		t.Fatalf("outer floor did not collapse after assignment and rename:\n%s", compiled.SQL)
	}
	if got := strings.Count(compiled.SQL, `"__os_fields"."category"`); got != 1 {
		t.Fatalf("Dynamic source references = %d, want one:\n%s", got, compiled.SQL)
	}
}

func TestCompileEvalIntegralRoundingRejectsForgedPlans(t *testing.T) {
	t.Parallel()

	base := buildPlan(t, `index=gradethis`)
	number := func() plan.ScalarExpression {
		return &plan.ScalarLiteralExpression{
			Value: plan.Value{Kind: plan.ValueKindFloat64, Float64: 1.25},
		}
	}
	var typedNil *plan.ScalarLiteralExpression
	cyclic := &plan.ScalarCallExpression{Function: plan.ScalarFunctionCeil}
	cyclic.Arguments = []plan.ScalarExpression{cyclic}
	for _, test := range []struct {
		name       string
		expression plan.ScalarExpression
		want       string
	}{
		{
			name:       "ceil zero arguments",
			expression: &plan.ScalarCallExpression{Function: plan.ScalarFunctionCeil},
			want:       "expected one argument",
		},
		{
			name: "floor two arguments",
			expression: &plan.ScalarCallExpression{
				Function:  plan.ScalarFunctionFloor,
				Arguments: []plan.ScalarExpression{number(), number()},
			},
			want: "expected one argument",
		},
		{
			name: "typed nil value",
			expression: &plan.ScalarCallExpression{
				Function:  plan.ScalarFunctionFloor,
				Arguments: []plan.ScalarExpression{typedNil},
			},
			want: "missing scalar expression",
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
			err := compileForgedScalarAssignment(t, base, test.expression)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("Compile error = %v, want %q", err, test.want)
			}
		})
	}
}
