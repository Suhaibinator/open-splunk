package clickhouse

import (
	"errors"
	"math"
	"slices"
	"strings"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/plan"
)

func TestCompileEvalRoundFixedNumbersAreTypedAndParameterized(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(
		t,
		`index=gradethis | eval whole=round(3.5), cents=round(2.555, 2), negative=round(-2.5), signed=round(-42, 18), unsigned=round(18446744073709551615, 18), absent=round(null) | table whole,cents,negative,signed,unsigned,absent`,
	)
	for _, required := range []string{
		`round(CAST(? AS Float64)) AS "whole"`,
		`round(CAST(? AS Float64), CAST(? AS UInt8)) AS "cents"`,
		`[toFloat64(CAST(? AS Float64))]), 1)) AS "negative"`,
		`[toFloat64(CAST(? AS Int64))]), 1), CAST(? AS UInt8)) AS "signed"`,
		`negate(__os_arithmetic_operand)`,
		`CAST(? AS UInt64) AS "unsigned"`,
		`CAST(NULL AS Nullable(Float64)) AS "absent"`,
	} {
		if !strings.Contains(compiled.SQL, required) {
			t.Fatalf("round SQL missing %q:\n%s", required, compiled.SQL)
		}
	}
	for _, argument := range []any{
		float64(3.5),
		float64(2.555),
		uint8(2),
		float64(2.5),
		int64(42),
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

func TestCompileEvalRoundSupportsDynamicNumbersOnceWithoutStringCoercion(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(
		t,
		`index=gradethis | eval rounded=round(category, 2) | where rounded=2.56`,
	)
	for _, required := range []string{
		`arrayElement(arrayMap((value, precision) ->`,
		`arrayElement(arrayMap(exact_value -> multiIf(`,
		`dynamicType(value) IN ('Int8', 'Int16'`,
		`'decimal/v1'`,
		`open_splunk_value`,
		`accurateCastOrNull(value, 'Float64')`,
		`["__os_fields"."category"], [CAST(? AS UInt8)]`,
		`CAST(NULL AS Nullable(Float64))`,
		`accurateCastOrNull("rounded", 'Float64')`,
	} {
		if !strings.Contains(compiled.SQL, required) {
			t.Fatalf("Dynamic round SQL missing %q:\n%s", required, compiled.SQL)
		}
	}
	if got := strings.Count(compiled.SQL, `"__os_fields"."category"`); got != 1 {
		t.Fatalf("Dynamic source references = %d, want 1:\n%s", got, compiled.SQL)
	}
	for _, forbidden := range []string{
		`dynamicType(value) = 'String'`,
		`dynamicType(value) = 'Bool'`,
		`Array(String)`,
		`ARRAY JOIN`,
		`toFloat64OrNull(toString(value))`,
	} {
		if strings.Contains(compiled.SQL, forbidden) {
			t.Fatalf("Dynamic round retained unsupported branch %q:\n%s", forbidden, compiled.SQL)
		}
	}
	for _, forbidden := range []string{
		`dynamicElement(left_value, 'Map(String, String)')`,
		`dynamicElement(left_value, 'String')`,
		`dynamicElement(left_value, 'Bool')`,
	} {
		if strings.Contains(compiled.SQL, forbidden) {
			t.Fatalf("numeric-only round predicate retained %q:\n%s", forbidden, compiled.SQL)
		}
	}
}

func TestCompileEvalRoundBindsDistinctNestedPrecisionsInSourceOrder(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(
		t,
		`index=gradethis | eval rounded=round(round(category, 1), 2) | table rounded`,
	)
	if got := countArgument(compiled.Args, uint8(1)); got != 1 {
		t.Fatalf("inner precision argument count = %d, want 1: %#v", got, compiled.Args)
	}
	if got := countArgument(compiled.Args, uint8(2)); got != 1 {
		t.Fatalf("outer precision argument count = %d, want 1: %#v", got, compiled.Args)
	}
	inner := slices.Index(compiled.Args, any(uint8(1)))
	outer := slices.Index(compiled.Args, any(uint8(2)))
	if inner < 0 || outer != inner+1 {
		t.Fatalf("nested precision arguments = %#v, want inner then outer", compiled.Args)
	}
	if got, want := strings.Count(compiled.SQL, "?"), len(compiled.Args); got != want {
		t.Fatalf("placeholder count = %d, args = %d\nSQL: %s", got, want, compiled.SQL)
	}
}

func TestCompileEvalRoundTextOnlyDynamicInputIsStaticallyNull(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(
		t,
		`index=gradethis | eval rounded=round(lower(category), 2) | table rounded`,
	)
	if !strings.Contains(
		compiled.SQL,
		`CAST(NULL AS Nullable(Float64)) AS "rounded"`,
	) {
		t.Fatalf("text-only round was not statically null:\n%s", compiled.SQL)
	}
	for _, forbidden := range []string{
		`round(multiIf(`,
		`'decimal/v1'`,
		`CAST(? AS UInt8)`,
	} {
		if strings.Contains(compiled.SQL, forbidden) {
			t.Fatalf("text-only round retained unused branch %q:\n%s", forbidden, compiled.SQL)
		}
	}
}

func TestCompileEvalRoundClosedSchemaMissingInputIsNull(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(
		t,
		`index=gradethis | stats count | eval rounded=round(absent, 2) | table rounded`,
	)
	if !strings.Contains(
		compiled.SQL,
		`CAST(NULL AS Nullable(Float64)) AS "rounded"`,
	) {
		t.Fatalf("closed-schema missing round did not become null:\n%s", compiled.SQL)
	}
	if strings.Contains(compiled.SQL, `CAST(? AS UInt8)`) {
		t.Fatalf("closed-schema missing round retained unused precision:\n%s", compiled.SQL)
	}
}

func TestCompileEvalRoundRejectsFixedNonNumbersAndMultivalue(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		source string
		code   string
	}{
		{
			source: `index=gradethis | eval rounded=round("2.5")`,
			code:   "SPL_UNSUPPORTED_ROUND_VALUE_TYPE",
		},
		{
			source: `index=gradethis | eval rounded=round(true)`,
			code:   "SPL_UNSUPPORTED_ROUND_VALUE_TYPE",
		},
		{
			source: `index=gradethis | eval rounded=round(_time)`,
			code:   "SPL_UNSUPPORTED_ROUND_VALUE_TYPE",
		},
		{
			source: `index=gradethis | stats values(status) AS values | eval rounded=round(values)`,
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

func TestCompileEvalRoundNestedDynamicSQLGrowsLinearly(t *testing.T) {
	t.Parallel()

	expression := "category"
	for range 24 {
		expression = "round(" + expression + ", 2)"
	}
	compiled := compileSPL(
		t,
		`index=gradethis | eval rounded=`+expression+` | table rounded`,
	)
	if len(compiled.SQL) > maxCompiledNumericRoundingScalarSQLBytes {
		t.Fatalf(
			"nested round SQL = %d bytes, want at most %d",
			len(compiled.SQL),
			maxCompiledNumericRoundingScalarSQLBytes,
		)
	}
	if strings.Count(compiled.SQL, `"__os_fields"."category"`) != 1 {
		t.Fatalf("nested Dynamic input was duplicated:\n%s", compiled.SQL)
	}
}

func TestCompileEvalRoundRejectsForgedPlans(t *testing.T) {
	t.Parallel()

	base := buildPlan(t, `index=gradethis`)
	number := func(value plan.Value) plan.ScalarExpression {
		return &plan.ScalarLiteralExpression{Value: value}
	}
	field := func() plan.ScalarExpression {
		return &plan.ScalarFieldExpression{
			Field: plan.FieldRef{Name: "status", Path: []string{"status"}},
		}
	}
	var typedNil *plan.ScalarLiteralExpression
	cyclic := &plan.ScalarCallExpression{Function: plan.ScalarFunctionRound}
	cyclic.Arguments = []plan.ScalarExpression{cyclic}
	for _, test := range []struct {
		name       string
		expression plan.ScalarExpression
		want       string
	}{
		{
			name:       "zero arguments",
			expression: &plan.ScalarCallExpression{Function: plan.ScalarFunctionRound},
			want:       "requires one or two arguments",
		},
		{
			name: "three arguments",
			expression: &plan.ScalarCallExpression{
				Function: plan.ScalarFunctionRound,
				Arguments: []plan.ScalarExpression{
					number(plan.Value{Kind: plan.ValueKindFloat64, Float64: 1.25}),
					number(plan.Value{Kind: plan.ValueKindInt64, Int64: 2}),
					number(plan.Value{Kind: plan.ValueKindInt64, Int64: 3}),
				},
			},
			want: "requires one or two arguments",
		},
		{
			name: "typed nil value",
			expression: &plan.ScalarCallExpression{
				Function:  plan.ScalarFunctionRound,
				Arguments: []plan.ScalarExpression{typedNil},
			},
			want: "missing scalar expression",
		},
		{
			name: "field precision",
			expression: &plan.ScalarCallExpression{
				Function: plan.ScalarFunctionRound,
				Arguments: []plan.ScalarExpression{
					number(plan.Value{Kind: plan.ValueKindFloat64, Float64: 1.25}),
					field(),
				},
			},
			want: "precision must be a literal integer",
		},
		{
			name: "negative precision",
			expression: &plan.ScalarCallExpression{
				Function: plan.ScalarFunctionRound,
				Arguments: []plan.ScalarExpression{
					number(plan.Value{Kind: plan.ValueKindFloat64, Float64: 1.25}),
					number(plan.Value{Kind: plan.ValueKindInt64, Int64: -1}),
				},
			},
			want: "precision must be from 0 through 18",
		},
		{
			name: "excessive precision",
			expression: &plan.ScalarCallExpression{
				Function: plan.ScalarFunctionRound,
				Arguments: []plan.ScalarExpression{
					number(plan.Value{Kind: plan.ValueKindFloat64, Float64: 1.25}),
					number(plan.Value{Kind: plan.ValueKindUint64, Uint64: 19}),
				},
			},
			want: "precision must be from 0 through 18",
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
