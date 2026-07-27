package clickhouse

import (
	"errors"
	"math"
	"strings"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/plan"
)

func TestCompileEvalSubstringUsesSQLiteUTF8IntervalSemantics(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(
		t,
		`index=gradethis | eval value=substr("😀abcdef", -4, -2) | table value`,
	)
	for _, required := range []string{
		`arrayMap((value, start, span) ->`,
		`CAST(lengthUTF8(value) AS Int128)`,
		`if(start < 0, n + start + 1, start)`,
		`clamp(if(span < 0, (if(start < 0, n + start + 1, start)) + span`,
		`clamp(if(span < 0, if(start < 0, n + start + 1, start)`,
		`substringUTF8(value, toInt64(`,
		`toUInt64(`,
		`) AS "value"`,
	} {
		if !strings.Contains(compiled.SQL, required) {
			t.Fatalf("substring SQL missing %q:\n%s", required, compiled.SQL)
		}
	}
	if strings.Contains(compiled.SQL, `substringUTF8(value, start, span)`) {
		t.Fatalf("substring passed SQLite-incompatible span through directly:\n%s", compiled.SQL)
	}
	if got := strings.Count(compiled.SQL, "arrayMap("); got != 2 {
		t.Fatalf("generic substring arrayMap calls = %d, want 2:\n%s", got, compiled.SQL)
	}
	if len(compiled.Args) < 3 ||
		compiled.Args[0] != "😀abcdef" ||
		compiled.Args[1] != int64(-4) ||
		compiled.Args[2] != int64(-2) {
		t.Fatalf("args = %#v, want parameterized input/start/length", compiled.Args)
	}
	if got, want := strings.Count(compiled.SQL, "?"), len(compiled.Args); got != want {
		t.Fatalf("placeholder count = %d, args = %d\nSQL: %s", got, want, compiled.SQL)
	}
}

func TestCompileEvalSubstringOmittedLengthAndExtremeIndexesStayOverflowSafe(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(
		t,
		`index=gradethis | eval whole=substr(source, -9223372036854775808), empty=substr(source, 18446744073709551615) | table whole,empty`,
	)
	for _, argument := range []any{int64(math.MinInt64), uint64(math.MaxUint64)} {
		found := false
		for _, got := range compiled.Args {
			if got == argument {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("args = %#v, missing extreme index %#v", compiled.Args, argument)
		}
	}
	for _, forbidden := range []string{
		`lengthUTF8("source")`,
		`arrayMap(`,
		`abs(`,
	} {
		if strings.Contains(compiled.SQL, forbidden) {
			t.Fatalf("omitted-length substring retained slow/unsafe operation %q:\n%s", forbidden, compiled.SQL)
		}
	}
	for _, required := range []string{
		`substringUTF8("source", CAST(? AS Int64)) AS "whole"`,
		`substringUTF8("source", CAST(? AS UInt64)) AS "empty"`,
	} {
		if !strings.Contains(compiled.SQL, required) {
			t.Fatalf("omitted-length substring SQL missing %q:\n%s", required, compiled.SQL)
		}
	}

	generic := compileSPL(
		t,
		`index=gradethis | eval value=substr(source, -9223372036854775808, 18446744073709551615) | table value`,
	)
	if got := strings.Count(generic.SQL, `CAST(? AS Int128)`); got != 2 {
		t.Fatalf("generic Int128 index casts = %d, want 2:\n%s", got, generic.SQL)
	}
	if !containsArgument(generic.Args, int64(math.MinInt64)) ||
		!containsArgument(generic.Args, uint64(math.MaxUint64)) {
		t.Fatalf("generic args = %#v, want full-width indexes", generic.Args)
	}
	if strings.Contains(generic.SQL, "abs(") {
		t.Fatalf("generic substring negates an extreme signed index:\n%s", generic.SQL)
	}
}

func TestCompileEvalSubstringSpecializesLiteralIntervalsWithoutUTF8LengthScan(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(
		t,
		`index=gradethis | eval positive=substr(source, 1, 3), zero_start=substr(source, 0, 3), preceding=substr(source, 4, -2), empty=substr(source, -4, 0), whole=substr(source, 0) | table positive,zero_start,preceding,empty,whole`,
	)
	for _, forbidden := range []string{"lengthUTF8(", "arrayMap(", "Int128"} {
		if strings.Contains(compiled.SQL, forbidden) {
			t.Fatalf("specialized substring retained %q:\n%s", forbidden, compiled.SQL)
		}
	}
	for _, required := range []string{
		`substringUTF8("source", CAST(? AS Int64), CAST(? AS Int64)) AS "positive"`,
		`substringUTF8("source", CAST(? AS UInt64), CAST(? AS UInt64)) AS "zero_start"`,
		`substringUTF8("source", CAST(? AS UInt64), CAST(? AS UInt64)) AS "preceding"`,
		`substringUTF8("source", CAST(? AS UInt64), CAST(? AS UInt64)) AS "empty"`,
		`"source" AS "whole"`,
	} {
		if !strings.Contains(compiled.SQL, required) {
			t.Fatalf("specialized substring SQL missing %q:\n%s", required, compiled.SQL)
		}
	}
}

func TestCompileEvalSubstringSupportsDynamicStringOnceWithoutGenericBranches(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(
		t,
		`index=gradethis | where substr(category, -3, 2)="xy"`,
	)
	for _, required := range []string{
		`dynamicElement("__os_fields"."category", 'String')`,
		`substringUTF8(value`,
	} {
		if !strings.Contains(compiled.SQL, required) {
			t.Fatalf("dynamic substring SQL missing %q:\n%s", required, compiled.SQL)
		}
	}
	if got := strings.Count(compiled.SQL, `"__os_fields"."category"`); got != 1 {
		t.Fatalf("Dynamic source references = %d, want 1:\n%s", got, compiled.SQL)
	}
	for _, forbidden := range []string{
		`'decimal/v1'`,
		`startsWith(dynamicType(`,
		`Array(String)`,
		`ARRAY JOIN`,
	} {
		if strings.Contains(compiled.SQL, forbidden) {
			t.Fatalf("substring retained unsupported branch %q:\n%s", forbidden, compiled.SQL)
		}
	}
}

func TestCompileEvalSubstringFailClosedForBinaryRawAndPreservesReplaceProvenance(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(
		t,
		`index=gradethis | eval raw_part=substr(_raw, 1, 3), replaced_part=substr(replace(_raw, "x", "y"), -2) | table raw_part,replaced_part`,
	)
	for _, required := range []string{
		`if(ifNull("__os_raw_encoding" = 1, 0), "_raw", CAST(NULL AS Nullable(String)))`,
		`if(ifNull("__os_raw_encoding" = 1, 0), replaceRegexpAll("_raw", ?, ?), CAST(NULL AS Nullable(String)))`,
	} {
		if !strings.Contains(compiled.SQL, required) {
			t.Fatalf("raw substring SQL missing %q:\n%s", required, compiled.SQL)
		}
	}
}

func TestCompileEvalSubstringRejectsNonStringAndMultivalueInputs(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		source string
		code   string
	}{
		{
			source: `index=gradethis | eval part=substr(123, 1)`,
			code:   "SPL_UNSUPPORTED_SUBSTRING_VALUE_TYPE",
		},
		{
			source: `index=gradethis | eval part=substr(true, 1)`,
			code:   "SPL_UNSUPPORTED_SUBSTRING_VALUE_TYPE",
		},
		{
			source: `index=gradethis | eval part=substr(_time, 1)`,
			code:   "SPL_UNSUPPORTED_SUBSTRING_VALUE_TYPE",
		},
		{
			source: `index=gradethis | stats values(user) AS users | eval part=substr(users, 1)`,
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

func TestCompileEvalSubstringNestedDynamicSQLGrowsLinearly(t *testing.T) {
	t.Parallel()

	expression := "category"
	for range 24 {
		expression = "substr(" + expression + ", 1)"
	}
	compiled := compileSPL(
		t,
		`index=gradethis | eval part=`+expression+` | table part`,
	)
	if len(compiled.SQL) > maxCompiledSubstringScalarSQLBytes {
		t.Fatalf(
			"nested substring SQL = %d bytes, want at most %d",
			len(compiled.SQL),
			maxCompiledSubstringScalarSQLBytes,
		)
	}
	if strings.Count(compiled.SQL, `"__os_fields"."category"`) != 1 {
		t.Fatalf("nested Dynamic input was duplicated:\n%s", compiled.SQL)
	}
}

func TestCompileEvalSubstringRejectsForgedPlans(t *testing.T) {
	t.Parallel()

	base := buildPlan(t, `index=gradethis`)
	stringLiteral := func() plan.ScalarExpression {
		return &plan.ScalarLiteralExpression{
			Value: plan.Value{Kind: plan.ValueKindString, String: "abcdef"},
		}
	}
	integerLiteral := func(value int64) plan.ScalarExpression {
		return &plan.ScalarLiteralExpression{
			Value: plan.Value{Kind: plan.ValueKindInt64, Int64: value},
		}
	}

	var typedNil *plan.ScalarLiteralExpression
	boolean := &plan.ScalarCallExpression{
		Function:  plan.ScalarFunctionIsNull,
		Arguments: []plan.ScalarExpression{stringLiteral()},
	}
	cyclic := &plan.ScalarCallExpression{Function: plan.ScalarFunctionSubstring}
	cyclic.Arguments = []plan.ScalarExpression{cyclic, integerLiteral(1)}
	for _, test := range []struct {
		name       string
		expression plan.ScalarExpression
		want       string
	}{
		{
			name: "one argument",
			expression: &plan.ScalarCallExpression{
				Function:  plan.ScalarFunctionSubstring,
				Arguments: []plan.ScalarExpression{stringLiteral()},
			},
			want: "expected two or three arguments",
		},
		{
			name: "four arguments",
			expression: &plan.ScalarCallExpression{
				Function: plan.ScalarFunctionSubstring,
				Arguments: []plan.ScalarExpression{
					stringLiteral(), integerLiteral(1), integerLiteral(2), integerLiteral(3),
				},
			},
			want: "expected two or three arguments",
		},
		{
			name: "typed nil input",
			expression: &plan.ScalarCallExpression{
				Function: plan.ScalarFunctionSubstring,
				Arguments: []plan.ScalarExpression{
					typedNil, integerLiteral(1),
				},
			},
			want: "missing scalar expression",
		},
		{
			name: "typed nil index",
			expression: &plan.ScalarCallExpression{
				Function: plan.ScalarFunctionSubstring,
				Arguments: []plan.ScalarExpression{
					stringLiteral(), typedNil,
				},
			},
			want: "missing index",
		},
		{
			name: "nonliteral index",
			expression: &plan.ScalarCallExpression{
				Function: plan.ScalarFunctionSubstring,
				Arguments: []plan.ScalarExpression{
					stringLiteral(),
					&plan.ScalarFieldExpression{
						Field: plan.FieldRef{Name: "source", Canonical: true},
					},
				},
			},
			want: "literal integer",
		},
		{
			name: "float index",
			expression: &plan.ScalarCallExpression{
				Function: plan.ScalarFunctionSubstring,
				Arguments: []plan.ScalarExpression{
					stringLiteral(),
					&plan.ScalarLiteralExpression{
						Value: plan.Value{Kind: plan.ValueKindFloat64, Float64: 1.5},
					},
				},
			},
			want: "literal integer",
		},
		{
			name: "Boolean null predicate",
			expression: &plan.ScalarCallExpression{
				Function: plan.ScalarFunctionSubstring,
				Arguments: []plan.ScalarExpression{
					boolean, integerLiteral(1),
				},
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
			err := compileForgedScalarAssignment(t, base, test.expression)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("Compile error = %v, want %q", err, test.want)
			}
		})
	}
}
