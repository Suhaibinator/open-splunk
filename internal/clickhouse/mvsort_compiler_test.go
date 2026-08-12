package clickhouse

import (
	"errors"
	"strconv"
	"strings"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/plan"
)

func TestCompileEvalMVSortFixedListUsesBoundedLexicalSort(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(
		t,
		`index=gradethis | stats list(status) AS collected | eval sorted=mvsort(collected) | table sorted`,
	)
	for _, required := range []string{
		`arraySort(values)`,
		`length(values) <= toUInt64(` + strconv.FormatUint(uint64(MaximumMVSortValues), 10) + `)`,
		`toUInt128(` + strconv.FormatUint(uint64(MaximumMVSortBytes), 10) + `)`,
		`arrayAll(element -> isValidUTF8(element), values)`,
		`CAST([], 'Array(String)')`,
		`AS "sorted"`,
	} {
		if !strings.Contains(compiled.SQL, required) {
			t.Fatalf("fixed mvsort SQL missing %q:\n%s", required, compiled.SQL)
		}
	}
	if got := strings.Count(compiled.SQL, `arraySort(`); got != 1 {
		t.Fatalf("fixed mvsort sort calls = %d, want 1:\n%s", got, compiled.SQL)
	}
	if strings.Contains(compiled.SQL, `ARRAY JOIN`) {
		t.Fatalf("fixed mvsort expands rows:\n%s", compiled.SQL)
	}
}

func TestCompileEvalMVSortValuesResultIsAlreadySorted(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(
		t,
		`index=gradethis | stats values(status) AS collected | eval sorted=mvsort(collected) | table sorted`,
	)
	if got := strings.Count(compiled.SQL, `arraySort(`); got != 1 {
		t.Fatalf("values plus mvsort sort calls = %d, want values publication only:\n%s", got, compiled.SQL)
	}
	if strings.Contains(compiled.SQL, `length(values) <= toUInt64(`) {
		t.Fatalf("mvsort did not reuse the sorted values invariant:\n%s", compiled.SQL)
	}
}

func TestCompileEvalMVSortSortedInvariantSurvivesEventStatsProjectionAndRename(t *testing.T) {
	t.Parallel()

	guard := `length(values) <= toUInt64(` +
		strconv.FormatUint(uint64(MaximumMVSortValues), 10) + `)`
	for _, source := range []string{
		`index=gradethis | eventstats values(status) AS collected | eval sorted=mvsort(collected) | table sorted`,
		`index=gradethis | stats values(status) AS collected | table collected | rename collected AS renamed | eval sorted=mvsort(renamed) | table sorted`,
	} {
		compiled := compileSPL(t, source)
		if strings.Contains(compiled.SQL, guard) {
			t.Fatalf("sorted invariant was lost for %q:\n%s", source, compiled.SQL)
		}
	}
}

func TestCompileEvalMVSortTextCaseClearsSortedInvariant(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(
		t,
		`index=gradethis | stats values(status) AS collected | eval sorted=mvsort(lower(collected)) | table sorted`,
	)
	guard := `length(values) <= toUInt64(` +
		strconv.FormatUint(uint64(MaximumMVSortValues), 10) + `)`
	if !strings.Contains(compiled.SQL, guard) {
		t.Fatalf("lower incorrectly retained the sorted invariant:\n%s", compiled.SQL)
	}
}

func TestCompileEvalMVSortDynamicInputIsBoundOnceAndNormalizesStringArrays(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(
		t,
		`index=gradethis | eval sorted=mvsort(category) | table sorted`,
	)
	for _, required := range []string{
		`arrayElement(arrayMap(value -> multiIf(`,
		`dynamicType(value) = 'Array(String)'`,
		`dynamicType(value) = 'Array(Dynamic)'`,
		`arrayAll(element -> dynamicType(element) = 'String'`,
		`arrayFold((bytes, element) -> bytes + toUInt128(ifNull(length(dynamicElement(element, 'String'))`,
		`arraySort(values)`,
		`arraySort(arrayMap(element -> assumeNotNull(dynamicElement(element, 'String'))`,
		`notEmpty(values)`,
		`length(values) <= toUInt64(` + strconv.FormatUint(uint64(MaximumMVSortValues), 10) + `)`,
		`toUInt128(` + strconv.FormatUint(uint64(MaximumMVSortBytes), 10) + `)`,
		`CAST(NULL AS Dynamic)`,
	} {
		if !strings.Contains(compiled.SQL, required) {
			t.Fatalf("Dynamic mvsort SQL missing %q:\n%s", required, compiled.SQL)
		}
	}
	if got := strings.Count(compiled.SQL, `"__os_fields"."category"`); got != 1 {
		t.Fatalf("Dynamic source references = %d, want 1:\n%s", got, compiled.SQL)
	}
	if got, want := strings.Count(compiled.SQL, "?"), len(compiled.Args); got != want {
		t.Fatalf(
			"Dynamic mvsort placeholder count = %d, args = %d:\n%s\n%#v",
			got,
			want,
			compiled.SQL,
			compiled.Args,
		)
	}
	for _, forbidden := range []string{
		`ARRAY JOIN`,
		`arrayJoin(`,
		`toString(element)`,
		`startsWith(dynamicType(value), 'Array(')`,
		`arrayFold((bytes, value) -> bytes + toUInt128(length(value)), arrayMap(element ->`,
	} {
		if strings.Contains(compiled.SQL, forbidden) {
			t.Fatalf("Dynamic mvsort retained forbidden branch %q:\n%s", forbidden, compiled.SQL)
		}
	}
}

func TestCompileEvalMVSortComposesWithTextCaseAndMVCount(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(
		t,
		`index=gradethis | eval count=mvcount(mvsort(lower(category))) | table count`,
	)
	for _, required := range []string{
		`lowerUTF8`,
		`arraySort(`,
		`dynamicType(value) = 'Array(String)'`,
		`nullIf(toUInt64(length(dynamicElement(value, 'Array(String)'))), toUInt64(0))`,
	} {
		if !strings.Contains(compiled.SQL, required) {
			t.Fatalf("composed mvsort SQL missing %q:\n%s", required, compiled.SQL)
		}
	}
	if got := strings.Count(compiled.SQL, `"__os_fields"."category"`); got != 1 {
		t.Fatalf("composed source references = %d, want 1:\n%s", got, compiled.SQL)
	}
}

func TestCompileEvalMVSortNestedAndSequentialCallsCollapse(t *testing.T) {
	t.Parallel()

	single := compileSPL(
		t,
		`index=gradethis | eval sorted=mvsort(category) | table sorted`,
	)
	expression := "category"
	for range 24 {
		expression = "mvsort(" + expression + ")"
	}
	nested := compileSPL(
		t,
		`index=gradethis | eval sorted=`+expression+` | table sorted`,
	)
	if nested.SQL != single.SQL {
		t.Fatalf("nested mvsort did not collapse:\nsingle:\n%s\nnested:\n%s", single.SQL, nested.SQL)
	}

	sequential := compileSPL(
		t,
		`index=gradethis | eval first=mvsort(category), second=mvsort(first) | table second`,
	)
	if got := strings.Count(sequential.SQL, `arraySort(`); got != 2 {
		// The Dynamic lowering has one branch for Array(String) and one for the
		// normalized Array(Dynamic) representation. A second authored mvsort
		// must not add either branch again.
		t.Fatalf("sequential mvsort sort branches = %d, want 2:\n%s", got, sequential.SQL)
	}
}

func TestCompileEvalMVSortClosedSchemaMissingInputIsAbsent(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(
		t,
		`index=gradethis | stats count | eval sorted=mvsort(absent) | table sorted`,
	)
	if !strings.Contains(compiled.SQL, `CAST([], 'Array(String)') AS "sorted"`) {
		t.Fatalf("closed-schema missing mvsort did not become empty MV:\n%s", compiled.SQL)
	}
	if strings.Contains(compiled.SQL, `arraySort(`) {
		t.Fatalf("closed-schema missing mvsort retained sort work:\n%s", compiled.SQL)
	}
}

func TestCompileEvalMVSortFixedEmptyResultsAreLogicallyAbsent(t *testing.T) {
	t.Parallel()

	literalNull := compileSPL(
		t,
		`index=gradethis | eval missing=if(isnull(mvsort(null)), 1, 0), present=if(isnotnull(mvsort(null)), 1, 0), count=mvcount(mvsort(null)) | table missing,present,count`,
	)
	for _, required := range []string{
		`CAST(NOT ifNull(0, 0) AS Bool)`,
		`CAST(ifNull(0, 0) AS Bool)`,
		`CAST(NULL AS Nullable(UInt64)) AS "count"`,
	} {
		if !strings.Contains(literalNull.SQL, required) {
			t.Fatalf("literal-null mvsort SQL missing %q:\n%s", required, literalNull.SQL)
		}
	}

	fixed := compileSPL(
		t,
		`index=gradethis | stats list(status) AS collected | eval missing=if(isnull(mvsort(collected)), 1, 0) | table missing`,
	)
	if !strings.Contains(fixed.SQL, `notEmpty(arrayElement(arrayMap(values -> if(`) {
		t.Fatalf("fixed mvsort presence does not use logical array presence:\n%s", fixed.SQL)
	}
	if strings.Contains(fixed.SQL, `isNotNull(arrayElement(arrayMap(values -> if(`) {
		t.Fatalf("fixed mvsort presence uses physical non-nullness:\n%s", fixed.SQL)
	}
}

func TestCompileEvalMVSortRejectsFixedScalarInputs(t *testing.T) {
	t.Parallel()

	for _, source := range []string{
		`index=gradethis | eval sorted=mvsort("value")`,
		`index=gradethis | eval sorted=mvsort(42)`,
		`index=gradethis | eval sorted=mvsort(true)`,
		`index=gradethis | eval sorted=mvsort(_time)`,
	} {
		logical := buildPlan(t, source)
		_, err := (Compiler{}).Compile(logical)
		var diagnostic *plan.Diagnostic
		if !errors.As(err, &diagnostic) ||
			diagnostic.Code != "SPL_UNSUPPORTED_MVSORT_VALUE_TYPE" {
			t.Fatalf(
				"Compile(%q) error = %#v, want SPL_UNSUPPORTED_MVSORT_VALUE_TYPE",
				source,
				err,
			)
		}
		if diagnostic.Range.Start.Offset >= diagnostic.Range.End.Offset {
			t.Fatalf("Compile(%q) diagnostic range = %#v", source, diagnostic.Range)
		}
	}
}

func TestCompileEvalMVSortRejectsForgedPlans(t *testing.T) {
	t.Parallel()
	testUnaryScalarCompilerTrustBoundary(
		t,
		plan.ScalarFunctionMVSort,
		plan.ScalarFunctionMVSort,
	)
}
