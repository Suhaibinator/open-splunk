package clickhouse

import (
	"slices"
	"strings"
	"testing"
)

func TestCompileWildcardSpathPublishesBoundedNativeDynamicArray(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(t,
		`index=gradethis | spath input=_raw output=users path=users{} | table users`,
	)

	for _, required := range []string{
		`JSONExtractArrayRaw(`,
		`arrayFlatten(`,
		`JSONExtract(raw, 'Dynamic')`,
		emptyNativeMVSQL(),
		`"__os_spath_mv_output_state_`,
		`AS "__os_result_multivalue_present_0"`,
		SpathInputLimitMarker,
		SpathJSONLexemeLimitMarker,
		UnsupportedNativeMVValueMarker,
		NativeMVMembersLimitMarker,
		NativeMVPayloadLimitMarker,
	} {
		if !strings.Contains(compiled.SQL, required) {
			t.Fatalf("terminal wildcard spath SQL is missing %q:\n%s", required, compiled.SQL)
		}
	}
	if strings.Contains(compiled.SQL, `users{}`) {
		t.Fatalf("terminal wildcard path was interpolated into SQL:\n%s", compiled.SQL)
	}
	if !slices.Contains(compiled.Args, any("users")) ||
		!slices.Contains(compiled.Args, any(spathJSONNumberPattern)) {
		t.Fatalf("terminal wildcard path/number pattern are not bound: %#v", compiled.Args)
	}
	if got := len(compiled.OptionalMultivalueOutputs); got != 1 ||
		compiled.OptionalMultivalueOutputs[0].OutputIndex != 0 {
		t.Fatalf("terminal wildcard optional-MV transport = %#v", compiled.OptionalMultivalueOutputs)
	}
	if len(compiled.OutputPresentations) != 0 {
		t.Fatalf("spath attached presentation metadata: %#v", compiled.OutputPresentations)
	}
	if !compiled.RequiresAtomicResult() || !compiled.HasValidExecutionSeal() {
		t.Fatalf(
			"terminal wildcard spath = atomic %t sealed %t",
			compiled.RequiresAtomicResult(),
			compiled.HasValidExecutionSeal(),
		)
	}
	if got, want := strings.Count(compiled.SQL, "?"), len(compiled.Args); got != want {
		t.Fatalf("terminal wildcard placeholders = %d, args = %d", got, want)
	}
}

func TestCompileWildcardSpathSkipsTokenRegexForProvablySmallInputs(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(t,
		`index=gradethis | spath input=_raw output=users path=users{} | table users`,
	)

	for _, required := range []string{
		`AND length("__os_spath_mv_input_`,
		`> 16384, countMatches(if(`,
		`> 16384, "__os_spath_mv_input_`,
		`CAST('' AS String)), ?), toUInt64(0)) AS "__os_spath_mv_token_count_`,
	} {
		if !strings.Contains(compiled.SQL, required) {
			t.Fatalf("wildcard spath token preflight is missing %q:\n%s", required, compiled.SQL)
		}
	}
	if got, want := strings.Count(compiled.SQL, "?"), len(compiled.Args); got != want {
		t.Fatalf("wildcard token preflight placeholders = %d, args = %d", got, want)
	}
}

func TestCompileWildcardSpathFlattensNestedAndMultipleSelectorsInSourceOrder(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(t,
		`index=gradethis | spath input=_raw output=names path=groups{}.users{}.name | table names`,
	)

	if got := strings.Count(compiled.SQL, `JSONExtractArrayRaw(`); got != 2 {
		t.Fatalf("nested wildcard array extraction count = %d, want 2:\n%s", got, compiled.SQL)
	}
	if got := strings.Count(compiled.SQL, `arrayFlatten(`); got != 2 {
		t.Fatalf("nested wildcard flatten count = %d, want 2:\n%s", got, compiled.SQL)
	}
	for _, required := range []string{
		`__os_spath_mv_state_`,
		`JSONExtractRaw(node, __os_spath_wildcard_key)`,
		`arrayFilter(raw -> notEmpty(raw)`,
		`arrayMap(raw -> if(match(raw, number_pattern)`,
		`toUInt8(if(tupleElement(traversal_state, 2) != 0, 1,`,
	} {
		if !strings.Contains(compiled.SQL, required) {
			t.Fatalf("nested wildcard spath SQL is missing %q:\n%s", required, compiled.SQL)
		}
	}
	for _, key := range []string{"groups", "users", "name"} {
		if !slices.Contains(compiled.Args, any(key)) {
			t.Fatalf("nested wildcard key %q is not bound: %#v", key, compiled.Args)
		}
		if strings.Contains(compiled.SQL, key+`{}`) {
			t.Fatalf("nested wildcard component %q was interpolated into SQL", key)
		}
	}
	if !compiled.RequiresAtomicResult() || !compiled.HasValidExecutionSeal() {
		t.Fatalf(
			"nested wildcard spath = atomic %t sealed %t",
			compiled.RequiresAtomicResult(),
			compiled.HasValidExecutionSeal(),
		)
	}
}

func TestCompileTerminalWildcardRetainsPresentEmptyMatch(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(t,
		`index=gradethis | spath input=_raw output=members path=payload.members{} | table members`,
	)

	for _, required := range []string{
		`arrayExists(node -> toString(JSONType(node, __os_spath_wildcard_key)) = 'Array'`,
		`CAST([], 'Array(String)')), toUInt8(0)) AS "__os_spath_mv_state_`,
		`toUInt8(if(tupleElement(traversal_state, 2) != 0, 1,`,
		`toUInt8(tupleElement("__os_spath_mv_final_`,
	} {
		if !strings.Contains(compiled.SQL, required) {
			t.Fatalf("present-empty wildcard spath SQL is missing %q:\n%s", required, compiled.SQL)
		}
	}
	if got := len(compiled.OptionalMultivalueOutputs); got != 1 {
		t.Fatalf("present-empty wildcard lost its presence sidecar: %#v", compiled.OptionalMultivalueOutputs)
	}
}

func TestCompileWildcardSpathPreservesOnlyPriorMultivalueOnNoMatch(t *testing.T) {
	t.Parallel()

	for _, producer := range []string{
		`split("prior,value", ",")`,
		`mvappend("prior", 7, true, null)`,
	} {
		multivalue := compileSPL(t,
			`index=gradethis `+
				`| eval users=`+producer+` `+
				`| spath input=_raw output=users path=missing{} `+
				`| table users`,
		)
		for _, required := range []string{
			`if(tupleElement(traversal_state, 2) != 0, arrayMap(raw ->`,
			`tupleElement(prior_state, 1)`,
			`tupleElement(prior_state, 2) != 0`,
			`tupleElement(prior_state, 3) != 0`,
			emptyNativeMVSQL(),
		} {
			if !strings.Contains(multivalue.SQL, required) {
				t.Fatalf("no-match prior-MV %s SQL is missing %q:\n%s", producer, required, multivalue.SQL)
			}
		}
		if got := len(multivalue.OptionalMultivalueOutputs); got != 1 {
			t.Fatalf("preserved prior MV %s transport = %#v", producer, multivalue.OptionalMultivalueOutputs)
		}
	}

	scalar := compileSPL(t,
		`index=gradethis `+
			`| eval users="present scalar" `+
			`| spath input=_raw output=users path=missing{} `+
			`| table users`,
	)
	for _, required := range []string{
		`field_present != 0 AND isNotNull(value)`,
		UnsupportedNativeMVValueMarker,
		`throwIf(toUInt8(`,
	} {
		if !strings.Contains(scalar.SQL, required) {
			t.Fatalf("no-match prior-scalar SQL is missing %q:\n%s", required, scalar.SQL)
		}
	}
	if !scalar.RequiresAtomicResult() || !scalar.HasValidExecutionSeal() {
		t.Fatalf(
			"prior-scalar wildcard spath = atomic %t sealed %t",
			scalar.RequiresAtomicResult(),
			scalar.HasValidExecutionSeal(),
		)
	}
}

func TestCompileWildcardSpathPreflightsMembersAndPayloadBeforeNativeConstruction(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(t,
		`index=gradethis | spath input=_raw output=values path=groups{}.values{} | table values`,
	)

	constructor := strings.Index(compiled.SQL, `arrayMap(raw -> if(match(raw, number_pattern)`)
	memberGuard := strings.Index(compiled.SQL, NativeMVMembersLimitMarker)
	payloadGuard := strings.Index(compiled.SQL, NativeMVPayloadLimitMarker)
	if constructor < 0 || memberGuard < 0 || payloadGuard < 0 ||
		memberGuard > constructor || payloadGuard > constructor {
		t.Fatalf(
			"wildcard spath did not preflight member/payload limits before Array(Dynamic) construction:\n%s",
			compiled.SQL,
		)
	}
	for _, required := range []string{
		`arrayFold((bytes, raw) -> bytes + arrayElement(arrayMap((member) -> toUInt128(length(`,
		`arrayExists(raw -> NOT (`,
		`length(tupleElement(traversal_state, 1))`,
		UnsupportedNativeMVValueMarker,
	} {
		if !strings.Contains(compiled.SQL, required) {
			t.Fatalf("wildcard spath preflight is missing %q:\n%s", required, compiled.SQL)
		}
	}
}
