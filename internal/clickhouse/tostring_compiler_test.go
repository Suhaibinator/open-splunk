package clickhouse

import (
	"errors"
	"math"
	"strings"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/plan"
)

func TestCompileEvalToStringFixedScalarsAreTypedAndParameterized(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(
		t,
		`index=gradethis | eval text=tostring("München"), signed=tostring(-42), unsigned=tostring(18446744073709551615), decimal=tostring(12.5), yes=tostring(true), no=tostring(false), absent=tostring(null), predicate=tostring(isnull(optional)) | table text,signed,unsigned,decimal,yes,no,absent,predicate`,
	)
	for _, required := range []string{
		`CAST(? AS String) AS "text"`,
		`toString(CAST(? AS Int64)) AS "signed"`,
		`toString(CAST(? AS UInt64)) AS "unsigned"`,
		`toString(CAST(? AS Float64)) AS "decimal"`,
		`'True'`,
		`'False'`,
		`CAST(NULL AS Nullable(String)) AS "absent"`,
		`) AS "predicate"`,
	} {
		if !strings.Contains(compiled.SQL, required) {
			t.Fatalf("tostring SQL missing %q:\n%s", required, compiled.SQL)
		}
	}
	for _, argument := range []any{
		"München",
		int64(-42),
		uint64(math.MaxUint64),
		float64(12.5),
		true,
		false,
	} {
		if !containsArgument(compiled.Args, argument) {
			t.Fatalf("args = %#v, missing %#v", compiled.Args, argument)
		}
	}
	if got, want := strings.Count(compiled.SQL, "?"), len(compiled.Args); got != want {
		t.Fatalf("placeholder count = %d, args = %d\nSQL: %s", got, want, compiled.SQL)
	}
}

func TestCompileEvalToStringSupportsDynamicScalarsOnceWithoutContainerConversion(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(
		t,
		`index=gradethis | eval rendered=tostring(category) | where rendered="42"`,
	)
	for _, required := range []string{
		`arrayElement(arrayMap(value -> multiIf(`,
		`dynamicType(value) = 'String'`,
		`dynamicElement(value, 'String')`,
		`dynamicType(value) = 'Bool'`,
		`dynamicElement(value, 'Bool')`,
		`toString(value)`,
		`CAST(NULL AS Nullable(String))`,
	} {
		if !strings.Contains(compiled.SQL, required) {
			t.Fatalf("Dynamic tostring SQL missing %q:\n%s", required, compiled.SQL)
		}
	}
	if got := strings.Count(compiled.SQL, `"__os_fields"."category"`); got != 1 {
		t.Fatalf("Dynamic source references = %d, want 1:\n%s", got, compiled.SQL)
	}
	for _, forbidden := range []string{
		`Array(String)`,
		`Map(String`,
		`ARRAY JOIN`,
		`'decimal/v1'`,
	} {
		if strings.Contains(compiled.SQL, forbidden) {
			t.Fatalf("Dynamic tostring retained container branch %q:\n%s", forbidden, compiled.SQL)
		}
	}
}

func TestCompileEvalToStringPreservesStringRawProvenance(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(
		t,
		`index=gradethis | eval copied=tostring(_raw), normalized=lower(copied) | table copied,normalized`,
	)
	if !strings.Contains(compiled.SQL, `"_raw" AS "copied"`) {
		t.Fatalf("tostring changed String identity:\n%s", compiled.SQL)
	}
	if !strings.Contains(
		compiled.SQL,
		`lowerUTF8(if(ifNull("__os_raw_encoding" = 1, 0), "copied", CAST(NULL AS Nullable(String))))`,
	) {
		t.Fatalf("tostring lost _raw UTF-8 provenance:\n%s", compiled.SQL)
	}
}

func TestCompileEvalToStringRejectsTimeAndFixedMultivalue(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		source string
		code   string
	}{
		{
			source: `index=gradethis | eval rendered=tostring(_time)`,
			code:   "SPL_UNSUPPORTED_TOSTRING_VALUE_TYPE",
		},
		{
			source: `index=gradethis | stats values(user) AS users | eval rendered=tostring(users)`,
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

func TestCompileEvalToStringNestedDynamicSQLGrowsLinearly(t *testing.T) {
	t.Parallel()

	expression := "category"
	for range 24 {
		expression = "tostring(" + expression + ")"
	}
	compiled := compileSPL(
		t,
		`index=gradethis | eval rendered=`+expression+` | table rendered`,
	)
	if len(compiled.SQL) > maxCompiledToStringScalarSQLBytes {
		t.Fatalf(
			"nested tostring SQL = %d bytes, want at most %d",
			len(compiled.SQL),
			maxCompiledToStringScalarSQLBytes,
		)
	}
	if strings.Count(compiled.SQL, `"__os_fields"."category"`) != 1 {
		t.Fatalf("nested Dynamic input was duplicated:\n%s", compiled.SQL)
	}
}

func TestCompileEvalToStringRejectsForgedPlansButAcceptsBoolean(t *testing.T) {
	t.Parallel()

	base := buildPlan(t, `index=gradethis`)
	literal := func() plan.ScalarExpression {
		return &plan.ScalarLiteralExpression{
			Value: plan.Value{Kind: plan.ValueKindString, String: "value"},
		}
	}
	var typedNil *plan.ScalarLiteralExpression
	cyclic := &plan.ScalarCallExpression{Function: plan.ScalarFunctionToString}
	cyclic.Arguments = []plan.ScalarExpression{cyclic}
	for _, test := range []struct {
		name       string
		expression plan.ScalarExpression
		want       string
	}{
		{
			name:       "zero arguments",
			expression: &plan.ScalarCallExpression{Function: plan.ScalarFunctionToString},
			want:       "expected one argument",
		},
		{
			name: "two arguments",
			expression: &plan.ScalarCallExpression{
				Function:  plan.ScalarFunctionToString,
				Arguments: []plan.ScalarExpression{literal(), literal()},
			},
			want: "expected one argument",
		},
		{
			name: "typed nil argument",
			expression: &plan.ScalarCallExpression{
				Function:  plan.ScalarFunctionToString,
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

	boolean := &plan.ScalarCallExpression{
		Function:  plan.ScalarFunctionIsNull,
		Arguments: []plan.ScalarExpression{literal()},
	}
	err := compileForgedScalarAssignment(
		t,
		base,
		&plan.ScalarCallExpression{
			Function:  plan.ScalarFunctionToString,
			Arguments: []plan.ScalarExpression{boolean},
		},
	)
	if err != nil {
		t.Fatalf("Compile Boolean tostring: %v", err)
	}
}
