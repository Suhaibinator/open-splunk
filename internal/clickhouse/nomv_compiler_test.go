package clickhouse

import (
	"slices"
	"strings"
	"testing"
)

func TestCompileNoMVOnSealedNativeStringArrayIsPresentationOnly(t *testing.T) {
	t.Parallel()

	for _, source := range []string{
		`index=gradethis | eval values=split("a,b", ",") | nomv values | table values`,
		`index=gradethis | eval values=mvzip(split("a,b", ","), split("1,2", ","), "=") | nomv values | table values`,
	} {
		compiled := compileSPL(t, source)

		for _, redundant := range []string{`__os_nomv_string_state`, `__os_nomv_output_state_`} {
			if strings.Contains(compiled.SQL, redundant) {
				t.Fatalf("sealed native String nomv retained redundant validation %q:\n%s", redundant, compiled.SQL)
			}
		}
		assertNoMVNewlinePresentation(t, compiled, "values")
		if got := len(compiled.OptionalMultivalueOutputs); got != 1 ||
			compiled.OptionalMultivalueOutputs[0].OutputIndex != 0 {
			t.Fatalf("sealed native String nomv changed typed MV transport: %#v", compiled.OptionalMultivalueOutputs)
		}
		for _, required := range []string{
			`splitByString(`,
			`__os_eval_mv_state_`,
			`AS "__os_result_multivalue_present_0"`,
		} {
			if !strings.Contains(compiled.SQL, required) {
				t.Fatalf("sealed native String nomv SQL is missing %q:\n%s", required, compiled.SQL)
			}
		}
		if !compiled.RequiresAtomicResult() || !compiled.HasValidExecutionSeal() {
			t.Fatalf(
				"sealed native String nomv = atomic %t sealed %t",
				compiled.RequiresAtomicResult(),
				compiled.HasValidExecutionSeal(),
			)
		}
	}
}

func TestCompileNoMVStillValidatesLegacyStringArrays(t *testing.T) {
	t.Parallel()

	for _, source := range []string{
		`index=gradethis | makemv delim="," tags | nomv tags | table tags`,
		`index=gradethis | makemv delim="," tags | eval copied=tags | nomv copied | table copied`,
		`index=gradethis | stats values(user) AS values | nomv values | table values`,
	} {
		compiled := compileSPL(t, source)
		for _, required := range []string{
			`__os_nomv_string_state`,
			`__os_nomv_output_state_`,
			NativeMVMembersLimitMarker,
			NativeMVPayloadLimitMarker,
		} {
			if !strings.Contains(compiled.SQL, required) {
				t.Fatalf("legacy String array nomv lost validation %q for %q:\n%s", required, source, compiled.SQL)
			}
		}
		if !compiled.RequiresAtomicResult() || !compiled.HasValidExecutionSeal() {
			t.Fatalf(
				"legacy String array nomv = atomic %t sealed %t for %q",
				compiled.RequiresAtomicResult(),
				compiled.HasValidExecutionSeal(),
				source,
			)
		}
	}
}

func TestCompileNoMVNormalizesRuntimeDynamicListsAtomically(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(t, `index=gradethis | nomv user | table user`)

	for _, required := range []string{
		`__os_nomv_state`,
		`__os_nomv_output_state_`,
		`dynamicType(value) = 'Array(String)'`,
		`dynamicType(value) = 'Array(Dynamic)'`,
		`arrayMap(member -> CAST(member AS Dynamic)`,
		emptyNativeMVSQL(),
		UnsupportedNativeMVValueMarker,
		NativeMVMembersLimitMarker,
		NativeMVPayloadLimitMarker,
		`AS "__os_result_multivalue_present_0"`,
	} {
		if !strings.Contains(compiled.SQL, required) {
			t.Fatalf("runtime-Dynamic nomv SQL is missing %q:\n%s", required, compiled.SQL)
		}
	}
	assertNoMVNewlinePresentation(t, compiled, "user")
	if got := len(compiled.OptionalMultivalueOutputs); got != 1 {
		t.Fatalf("runtime-Dynamic nomv transport = %#v", compiled.OptionalMultivalueOutputs)
	}
	if !compiled.RequiresAtomicResult() || !compiled.HasValidExecutionSeal() {
		t.Fatalf(
			"runtime-Dynamic nomv = atomic %t sealed %t",
			compiled.RequiresAtomicResult(),
			compiled.HasValidExecutionSeal(),
		)
	}
	if got, want := strings.Count(compiled.SQL, "?"), len(compiled.Args); got != want {
		t.Fatalf("runtime-Dynamic nomv placeholders = %d, args = %d", got, want)
	}
}

func TestCompileNoMVPresentationSurvivesProjectionAndRename(t *testing.T) {
	t.Parallel()

	for _, source := range []string{
		`index=gradethis | eval values=mvappend("a", 7, true) | nomv values | fields values`,
		`index=gradethis | eval values=mvappend("a", 7, true) | nomv values | rename values AS members | table members`,
		`index=gradethis | eval values=mvappend("a", 7, true) | nomv values | table values`,
	} {
		compiled := compileSPL(t, source)
		want := "values"
		if slices.Contains(compiled.OutputFields, "members") {
			want = "members"
		}
		assertNoMVNewlinePresentation(t, compiled, want)
		if got := len(compiled.OptionalMultivalueOutputs); got != 1 {
			t.Fatalf("Compile(%q) optional MV transport = %#v", source, compiled.OptionalMultivalueOutputs)
		}
		if !compiled.HasValidExecutionSeal() {
			t.Fatalf("Compile(%q) has an invalid execution seal", source)
		}
	}
}

func TestCompileNoMVPresentationClearsOnOverwriteAndCanBeReapplied(t *testing.T) {
	t.Parallel()

	cleared := compileSPL(t,
		`index=gradethis `+
			`| eval values=split("a,b", ",") `+
			`| nomv values `+
			`| eval values=mvappend("new", 7) `+
			`| table values`,
	)
	if len(cleared.OutputPresentations) != 0 {
		t.Fatalf("overwritten nomv presentation survived: %#v", cleared.OutputPresentations)
	}
	if got := len(cleared.OptionalMultivalueOutputs); got != 1 {
		t.Fatalf("overwrite changed authoritative typed MV transport: %#v", cleared.OptionalMultivalueOutputs)
	}

	reapplied := compileSPL(t,
		`index=gradethis `+
			`| eval values=split("a,b", ",") `+
			`| nomv values `+
			`| eval values=mvappend("new", 7) `+
			`| nomv values `+
			`| table values`,
	)
	assertNoMVNewlinePresentation(t, reapplied, "values")
}

func TestCompileNoMVDynamicNormalizationMarkerSurvivesDownstreamProjection(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(t,
		`index=gradethis | nomv user | fields event_id`,
	)
	if slices.Contains(compiled.OutputFields, "user") {
		t.Fatalf("projected-away nomv field leaked into output: %v", compiled.OutputFields)
	}
	for _, required := range []string{
		`__os_nomv_state`,
		UnsupportedNativeMVValueMarker,
		`WITH "__os_nomv_validation_`,
		` AS MATERIALIZED (`,
		`WHERE ignore("user") = 0`,
	} {
		if !strings.Contains(compiled.SQL, required) {
			t.Fatalf("projected-away Dynamic nomv lost %q:\n%s", required, compiled.SQL)
		}
	}
	if !compiled.RequiresAtomicResult() || !compiled.HasValidExecutionSeal() {
		t.Fatalf(
			"projected-away Dynamic nomv = atomic %t sealed %t",
			compiled.RequiresAtomicResult(),
			compiled.HasValidExecutionSeal(),
		)
	}
}

func assertNoMVNewlinePresentation(t *testing.T, compiled CompiledQuery, field string) {
	t.Helper()

	index := slices.Index(compiled.OutputFields, field)
	if index < 0 {
		t.Fatalf("nomv output fields = %v, missing %q", compiled.OutputFields, field)
	}
	if len(compiled.OutputPresentations) != len(compiled.OutputFields) {
		t.Fatalf(
			"nomv presentation alignment = %d presentations for %d fields: %#v",
			len(compiled.OutputPresentations),
			len(compiled.OutputFields),
			compiled.OutputPresentations,
		)
	}
	presentation := compiled.OutputPresentations[index]
	if !presentation.HasFlatMultivalueDelimiter ||
		presentation.FlatMultivalueDelimiter != "\n" ||
		presentation.StatsSparkline {
		t.Fatalf("nomv presentation for %q = %#v", field, presentation)
	}
}
