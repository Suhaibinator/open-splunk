package clickhouse

import (
	"errors"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/plan"
	"github.com/Suhaibinator/open-splunk/internal/splpath"
)

func TestCompileSpathBindsPathAndChecksContainersBeforeOneRawExtraction(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(
		t,
		`index=gradethis | spath input=_raw output=extracted path="private'key;--{0}.leaf"`,
	)
	if got := strings.Count(compiled.SQL, "JSONExtractRaw("); got != 1 {
		t.Fatalf("JSONExtractRaw occurrences = %d, want one:\n%s", got, compiled.SQL)
	}
	if got := strings.Count(compiled.SQL, "JSONType("); got != 1 {
		t.Fatalf("JSONType occurrences = %d, want one array-prefix check:\n%s", got, compiled.SQL)
	}
	if got := strings.Count(compiled.SQL, "JSONExtract("); got != 1 {
		t.Fatalf("typed JSONExtract occurrences = %d, want one:\n%s", got, compiled.SQL)
	}
	for _, forbidden := range []string{"private'key;--", "leaf"} {
		if strings.Contains(compiled.SQL, forbidden) {
			t.Fatalf("generated SQL interpolates path component %q:\n%s", forbidden, compiled.SQL)
		}
	}
	for _, required := range []string{
		`"__os_spath_input_`,
		`"__os_spath_path_eligible_`,
		`"__os_spath_raw_`,
		`"__os_spath_matched_`,
		`"__os_spath_exists_`,
		`"__os_spath_type_`,
		`dynamicType("__os_spath_value_`,
		UnsupportedSpathValueMarker,
		SpathInputLimitMarker,
	} {
		if !strings.Contains(compiled.SQL, required) {
			t.Fatalf("generated SQL is missing %q:\n%s", required, compiled.SQL)
		}
	}
	if !containsArgumentSubsequence(compiled.Args, []any{"private'key;--", int64(1), "leaf"}) {
		t.Fatalf("compiled arguments do not contain translated bound path: %#v", compiled.Args)
	}
	if got, want := strings.Count(compiled.SQL, "?"), len(compiled.Args); got != want {
		t.Fatalf("spath placeholders = %d, args = %d:\n%s\n%#v", got, want, compiled.SQL, compiled.Args)
	}
	if slices.Contains(compiled.OutputFields, "fields") {
		t.Fatalf("spath retained stale fields payload: %v", compiled.OutputFields)
	}
	if !slices.Contains(compiled.OutputFields, "extracted") {
		t.Fatalf("spath output fields = %v, want extracted", compiled.OutputFields)
	}
}

func TestCompileSpathClassifiesNumbersFromOneBoundedLexicalTokenization(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(
		t,
		`index=gradethis | spath output=selected path=payload.value | table selected`,
	)
	for fragment, want := range map[string]int{
		"extractAll(":          1,
		"JSONExtractString(":   1,
		"JSONExtractRaw(":      1,
		"reinterpretAsUInt64(": 1,
		"toDecimalString(":     1,
	} {
		if got := strings.Count(compiled.SQL, fragment); got != want {
			t.Fatalf("spath SQL contains %q %d times, want %d:\n%s", fragment, got, want, compiled.SQL)
		}
	}
	for _, required := range []string{
		"countMatches(",
		"countMatches(if(",
		"extractAll(if(",
		"arrayEnumerate(",
		"arrayStringConcat(",
		"accurateCastOrNull(",
		"'decimal/v1'",
		SpathJSONTokenLimitMarker,
	} {
		if !strings.Contains(compiled.SQL, required) {
			t.Fatalf("numeric spath SQL is missing %q:\n%s", required, compiled.SQL)
		}
	}
	for _, forbidden := range []string{"arrayCumSum(", "arrayFilter("} {
		if strings.Contains(compiled.SQL, forbidden) {
			t.Fatalf("numeric spath SQL retains avoidable %q work:\n%s", forbidden, compiled.SQL)
		}
	}
	if !slices.Contains(compiled.Args, spathJSONTokenPattern) ||
		!slices.Contains(compiled.Args, spathJSONNumberPattern) {
		t.Fatalf("numeric spath regexes are not bound arguments: %#v", compiled.Args)
	}
	if got := countArgument(compiled.Args, spathJSONTokenPattern); got != 2 {
		t.Fatalf("numeric spath token-pattern arguments = %d, want preflight plus extraction", got)
	}
	if got, want := strings.Count(compiled.SQL, "?"), len(compiled.Args); got != want {
		t.Fatalf("numeric spath placeholders = %d, args = %d:\n%s\n%#v", got, want, compiled.SQL, compiled.Args)
	}
	if got, wantMax := len(compiled.SQL), 64<<10; got > wantMax {
		t.Fatalf("numeric spath compiled SQL bytes = %d, want at most %d", got, wantMax)
	}
}

func TestCompileSpathUsesConsumerLocalPredicateFence(t *testing.T) {
	t.Parallel()

	affected := compileSPL(
		t,
		`index=gradethis | spath output=selected path=payload.value | where selected>=7`,
	)
	for _, required := range []string{" AS MATERIALIZED (", "ARRAY JOIN", materializedCTESettingsSQL} {
		if !strings.Contains(affected.SQL, required) {
			t.Fatalf("calculated spath predicate is missing %q:\n%s", required, affected.SQL)
		}
	}
	if got := strings.Count(affected.SQL, "JSONExtractRaw("); got != 1 {
		t.Fatalf("fenced spath parses full source %d times, want once:\n%s", got, affected.SQL)
	}

	unrelated := compileSPL(
		t,
		`index=gradethis | spath output=selected path=payload.value | where host="api"`,
	)
	if strings.Contains(unrelated.SQL, " AS MATERIALIZED (") {
		t.Fatalf("unrelated predicate unnecessarily materialized spath relation:\n%s", unrelated.SQL)
	}

	impossible := compileSPL(
		t,
		`index=gradethis | table severity | spath input=severity output=selected path=value | where selected=7`,
	)
	if strings.Contains(impossible.SQL, " AS MATERIALIZED (") {
		t.Fatalf("statically non-string spath input unnecessarily materialized relation:\n%s", impossible.SQL)
	}

	openSchemaImpossible := compileSPL(
		t,
		`index=gradethis | spath input=severity output=selected path=value`,
	)
	if !slices.Contains(openSchemaImpossible.OutputFields, "fields") {
		t.Fatalf(
			"statically non-string spath input removed exact fields payload: %v",
			openSchemaImpossible.OutputFields,
		)
	}
}

func TestCompileSpathSupportsCurrentPipelineInputAndSequentialStages(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(
		t,
		`index=gradethis | rex field=_raw "(?<payload>\{.*\})" | spath input=payload output=nested path=body | spath input=nested output=value path=value`,
	)
	if got := strings.Count(compiled.SQL, "JSONExtractRaw("); got != 2 {
		t.Fatalf("JSONExtractRaw occurrences = %d, want two stages:\n%s", got, compiled.SQL)
	}
	for _, name := range []string{`"payload"`, `"nested"`, `"value"`} {
		if !strings.Contains(compiled.SQL, name) {
			t.Fatalf("sequential spath SQL is missing %s:\n%s", name, compiled.SQL)
		}
	}
	if !slices.Contains(compiled.OutputFields, "value") {
		t.Fatalf("sequential spath output fields = %v", compiled.OutputFields)
	}
	if got, want := strings.Count(compiled.SQL, "?"), len(compiled.Args); got != want {
		t.Fatalf("sequential spath placeholders = %d, args = %d:\n%s\n%#v", got, want, compiled.SQL, compiled.Args)
	}
}

func TestCompileSpathReadsPreCommandValueWhenInputEqualsOutput(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(
		t,
		`index=gradethis | spath input=message output=message path=value`,
	)
	if got := strings.Count(compiled.SQL, `extractAll(if("__os_spath_token_guard_`); got != 1 {
		t.Fatalf("same-field spath does not read its bound pre-command input once:\n%s", compiled.SQL)
	}
	if !slices.Contains(compiled.OutputFields, "message") {
		t.Fatalf("same-field output fields = %v", compiled.OutputFields)
	}
}

func TestCompileSpathCarriesRawEncodingEligibilityThroughCopiesAndMisses(t *testing.T) {
	t.Parallel()

	for _, source := range []string{
		`index=gradethis | spath output=value path=payload.value`,
		`index=gradethis | eval copied=_raw | spath input=copied output=value path=payload.value`,
		`index=gradethis | spath output=_raw path=missing | spath output=value path=payload.value`,
		`index=gradethis | rex field=_raw "(?<_raw>never)" | spath output=value path=payload.value`,
	} {
		compiled := compileSPL(t, source)
		if !strings.Contains(compiled.SQL, `"raw_encoding" AS "__os_raw_encoding"`) ||
			!strings.Contains(compiled.SQL, `"__os_raw_encoding" = 1`) {
			t.Fatalf("%q does not retain the raw UTF-8 eligibility gate:\n%s", source, compiled.SQL)
		}
		if got, want := strings.Count(compiled.SQL, "?"), len(compiled.Args); got != want {
			t.Fatalf("%q placeholders = %d, args = %d:\n%s", source, got, want, compiled.SQL)
		}
	}
}

func TestCompilerRejectsForgedSpathOperatorMetadata(t *testing.T) {
	t.Parallel()

	t.Run("path mismatch", func(t *testing.T) {
		t.Parallel()
		logical := buildPlan(t, `index=gradethis | spath output=value path=payload.value`)
		extract := logical.Operators[2].(*plan.ExtractJSON)
		extract.Steps = []splpath.Step{{Key: "forged"}}
		if _, err := (Compiler{}).Compile(logical); err == nil || !strings.Contains(err.Error(), "spath") {
			t.Fatalf("Compile(forged path) error = %v, want rejection", err)
		}
	})

	t.Run("nil operator", func(t *testing.T) {
		t.Parallel()
		logical := buildPlan(t, `index=gradethis | spath output=value path=payload.value`)
		logical.Operators[2] = (*plan.ExtractJSON)(nil)
		if _, err := (Compiler{}).Compile(logical); err == nil {
			t.Fatal("Compile accepted nil ExtractJSON")
		}
	})

	t.Run("cumulative JSON work budget", func(t *testing.T) {
		t.Parallel()
		logical := buildPlan(
			t,
			`index=gradethis | spath output=value path=a{0}.b{0}.c{0}.d{0}`,
		)
		extract := logical.Operators[2].(*plan.ExtractJSON)
		for splpath.EvaluationWorkUnits(extract.Steps)*countExtractJSONOperators(logical) <=
			splpath.MaximumEvaluationWorkUnits {
			cloned := *extract
			logical.Operators = append(logical.Operators, &cloned)
		}
		_, err := (Compiler{}).Compile(logical)
		var diagnostic *plan.Diagnostic
		if !errors.As(err, &diagnostic) || diagnostic.Code != "SPL_QUERY_TOO_COMPLEX" {
			t.Fatalf("Compile(over spath work budget) error = %v, want SPL_QUERY_TOO_COMPLEX", err)
		}
	})

	t.Run("combined extraction output budget", func(t *testing.T) {
		t.Parallel()
		logical := buildPlan(t, `index=gradethis | rex "(?<value>x)"`)
		extract := logical.Operators[2].(*plan.Extract)
		for index := 1; index < maxCompiledExtractionOutputs+1; index++ {
			cloned := *extract
			logical.Operators = append(logical.Operators, &cloned)
		}
		_, err := (Compiler{}).Compile(logical)
		var diagnostic *plan.Diagnostic
		if !errors.As(err, &diagnostic) || diagnostic.Code != "SPL_QUERY_TOO_COMPLEX" {
			t.Fatalf("Compile(over extraction output budget) error = %v, want SPL_QUERY_TOO_COMPLEX", err)
		}
	})
}

func countExtractJSONOperators(logical *plan.Query) int {
	count := 0
	for _, operator := range logical.Operators {
		if _, ok := operator.(*plan.ExtractJSON); ok {
			count++
		}
	}
	return count
}

func containsArgumentSubsequence(arguments, want []any) bool {
	for start := 0; start+len(want) <= len(arguments); start++ {
		if reflect.DeepEqual(arguments[start:start+len(want)], want) {
			return true
		}
	}
	return false
}
