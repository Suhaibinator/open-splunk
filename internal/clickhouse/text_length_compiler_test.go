package clickhouse

import (
	"errors"
	"strings"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/plan"
)

func TestCompileEvalLenLengthFixedStringsAreTypedAndParameterized(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(
		t,
		`index=gradethis | eval short=len("München"), long=length(source), absent=len(null) | table short,long,absent`,
	)
	for _, required := range []string{
		`lengthUTF8(CAST(? AS String)) AS "short"`,
		`lengthUTF8("source") AS "long"`,
		`lengthUTF8(toString(CAST(NULL AS Nullable(String)))) AS "absent"`,
	} {
		if !strings.Contains(compiled.SQL, required) {
			t.Fatalf("text-length SQL missing %q:\n%s", required, compiled.SQL)
		}
	}
	if len(compiled.Args) == 0 || compiled.Args[0] != "München" {
		t.Fatalf("args = %#v, want leading string literal", compiled.Args)
	}
	if got, want := strings.Count(compiled.SQL, "?"), len(compiled.Args); got != want {
		t.Fatalf("placeholder count = %d, args = %d\nSQL: %s", got, want, compiled.SQL)
	}
}

func TestCompileEvalLenLengthSupportsDynamicStringWithoutGenericComparisonBranches(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(
		t,
		`index=gradethis | where len(category)>0 AND length(other)=3`,
	)
	for _, required := range []string{
		`lengthUTF8(dynamicElement("__os_fields"."category", 'String'))`,
		`lengthUTF8(dynamicElement("__os_fields"."other", 'String'))`,
	} {
		if !strings.Contains(compiled.SQL, required) {
			t.Fatalf("dynamic text-length SQL missing %q:\n%s", required, compiled.SQL)
		}
	}
	for _, field := range []string{"category", "other"} {
		path := `"__os_fields"."` + field + `"`
		if got := strings.Count(compiled.SQL, path); got != 1 {
			t.Fatalf("dynamic text length references %s %d times, want 1:\n%s", field, got, compiled.SQL)
		}
	}
	for _, forbidden := range []string{
		"'decimal/v1'",
		"startsWith(dynamicType(",
		"Array(String)",
		"arrayElement(",
		"arrayMap(",
	} {
		if strings.Contains(compiled.SQL, forbidden) {
			t.Fatalf("text length retained unsupported branch %q:\n%s", forbidden, compiled.SQL)
		}
	}
}

func TestCompileEvalLenLengthFailClosedForBinaryRawAndPreserveReplaceProvenance(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(
		t,
		`index=gradethis | eval raw_size=len(_raw), replaced_size=length(replace(_raw, "x", "y")) | table raw_size,replaced_size`,
	)
	for _, required := range []string{
		`lengthUTF8(if(ifNull("__os_raw_encoding" = 1, 0), "_raw", CAST(NULL AS Nullable(String)))) AS "raw_size"`,
		`lengthUTF8(if(ifNull("__os_raw_encoding" = 1, 0), replaceRegexpAll("_raw", ?, ?), CAST(NULL AS Nullable(String)))) AS "replaced_size"`,
	} {
		if !strings.Contains(compiled.SQL, required) {
			t.Fatalf("raw text-length SQL missing %q:\n%s", required, compiled.SQL)
		}
	}
}

func TestCompileEvalLenLengthRejectsNonStringAndMultivalueInputs(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		source string
		code   string
	}{
		{
			source: `index=gradethis | eval size=len(123)`,
			code:   "SPL_UNSUPPORTED_TEXT_LENGTH_VALUE_TYPE",
		},
		{
			source: `index=gradethis | eval size=length(true)`,
			code:   "SPL_UNSUPPORTED_TEXT_LENGTH_VALUE_TYPE",
		},
		{
			source: `index=gradethis | eval size=len(_time)`,
			code:   "SPL_UNSUPPORTED_TEXT_LENGTH_VALUE_TYPE",
		},
		{
			source: `index=gradethis | stats values(user) AS users | eval size=len(users)`,
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

func TestCompileEvalLenLengthNestedDynamicSQLGrowsLinearly(t *testing.T) {
	t.Parallel()

	expression := "category"
	for range 30 {
		expression = "lower(" + expression + ")"
	}
	compiled := compileSPL(
		t,
		`index=gradethis | eval size=len(`+expression+`) | table size`,
	)
	if len(compiled.SQL) > maxCompiledTextLengthScalarSQLBytes {
		t.Fatalf(
			"nested text-length SQL = %d bytes, want at most %d",
			len(compiled.SQL),
			maxCompiledTextLengthScalarSQLBytes,
		)
	}
	if strings.Count(compiled.SQL, `"__os_fields"."category"`) != 1 {
		t.Fatalf("nested Dynamic input was duplicated:\n%s", compiled.SQL)
	}
}

func TestCompileEvalLenLengthRejectsForgedPlans(t *testing.T) {
	t.Parallel()
	testUnaryScalarCompilerTrustBoundary(
		t,
		plan.ScalarFunctionLength,
		plan.ScalarFunctionLength,
	)
}
