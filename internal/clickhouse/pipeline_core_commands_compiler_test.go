package clickhouse

import (
	"errors"
	"slices"
	"strings"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/plan"
	"github.com/Suhaibinator/open-splunk/internal/splregex"
)

func TestPipelineRegexCompilationAccountsRetainedAndEveryAuthoredProgram(t *testing.T) {
	t.Parallel()

	program := testKnowledgeEvidenceProgram(t, "pipeline-regex-accounting")
	logical, err := plan.InjectKnowledgePrelude(
		buildPlan(t, `index=gradethis | rex field=_raw "(?<word>a+)" | regex message="b+" | where match(source, "c+")`),
		program,
	)
	if err != nil {
		t.Fatalf("InjectKnowledgePrelude: %v", err)
	}
	compiled, err := (Compiler{}).Compile(logical)
	if err != nil {
		t.Fatalf("Compile(composed regex programs): %v", err)
	}
	evidence, ok := compiled.KnowledgeSnapshotEvidenceFor(program)
	if !ok {
		t.Fatal("compiled query omitted its exact retained knowledge evidence")
	}
	extraction, err := splregex.CompileExtractionPattern(`(?<word>a+)`)
	if err != nil {
		t.Fatal(err)
	}
	regexCommand, err := splregex.CompileMatchPattern(`b+`)
	if err != nil {
		t.Fatal(err)
	}
	matchCall, err := splregex.CompileMatchPattern(`c+`)
	if err != nil {
		t.Fatal(err)
	}
	wantAuthoredWork := uint64(
		extraction.ProgramWorkUnits + regexCommand.ProgramWorkUnits + matchCall.ProgramWorkUnits,
	)
	charges := program.Charges()
	if evidence.AuthoredRegexPrograms() != 3 ||
		evidence.AuthoredRegexWorkUnits() != wantAuthoredWork ||
		evidence.RegexPrograms() != charges.RegexPrograms+3 ||
		evidence.RegexWorkUnits() != charges.RegexWorkUnits+wantAuthoredWork {
		t.Fatalf("composed regex evidence = %#v, retained charges = %#v", evidence, charges)
	}
}

func TestPipelineRegexCompilerUsesExactPatternRangeForForgedResourceFailure(t *testing.T) {
	t.Parallel()

	logical := buildPlan(t, `index=gradethis | regex message="ok"`)
	operator, ok := logical.Operators[len(logical.Operators)-1].(*plan.RegexFilter)
	if !ok || operator == nil {
		t.Fatalf("regex logical operator = %T", logical.Operators[len(logical.Operators)-1])
	}
	wantRange := operator.PatternRange
	operator.Pattern = strings.Repeat("x", splregex.MaximumMatchPatternBytes+1)
	_, err := (Compiler{}).Compile(logical)
	var diagnostic *plan.Diagnostic
	if !errors.As(err, &diagnostic) || diagnostic.Code != "SPL_QUERY_TOO_COMPLEX" ||
		diagnostic.Range != wantRange {
		t.Fatalf("forged regex resource error = %#v, want exact pattern range %#v", err, wantRange)
	}
}

func TestPipelineReverseAndAccumRetainPrivateCompleteOrder(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(
		t,
		`index=gradethis | sort 0 +event_id | reverse | accum bytes AS running | reverse | table event_id running`,
	)
	if !compiled.HasValidExecutionSeal() ||
		!slices.Equal(compiled.OutputFields, []string{"event_id", "running"}) {
		t.Fatalf("reverse/accum compiled result = %#v", compiled)
	}
	for _, required := range []string{
		`AS "__os_reverse_order_`,
		`AS "__os_streamstats_order_`,
		`sumOrNullArray(`,
	} {
		if !strings.Contains(compiled.SQL, required) {
			t.Fatalf("reverse/accum SQL missing %q:\n%s", required, compiled.SQL)
		}
	}
	for _, output := range compiled.OutputFields {
		if strings.HasPrefix(strings.ToLower(output), "__os_") {
			t.Fatalf("compiler-private order leaked as public output %q", output)
		}
	}
}

func TestPipelineStrcatAllRequiredPreservesPriorDestination(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(
		t,
		`index=gradethis | eval joined="preexisting" | strcat allrequired=true missing ":" route joined | table joined`,
	)
	for _, required := range []string{
		`arrayMap((__os_strcat_value, __os_strcat_text, __os_strcat_previous) -> if(isNotNull(__os_strcat_value)`,
		`tuple(toUInt8(1), CAST(__os_strcat_value AS Dynamic)`,
		`toUInt8(if(ifNull(__os_strcat_text, 0)`,
		`AS "__os_strcat_exists_`,
		`AS "__os_strcat_text_eligible_`,
		`concat(`,
	} {
		if !strings.Contains(compiled.SQL, required) {
			t.Fatalf("allrequired strcat SQL missing %q:\n%s", required, compiled.SQL)
		}
	}
	if strings.Contains(compiled.SQL, "preexisting") {
		t.Fatal("strcat prior destination literal was interpolated into SQL")
	}
	if got, want := strings.Count(compiled.SQL, "?"), len(compiled.Args); got != want {
		t.Fatalf("strcat placeholders = %d, args = %d: %#v", got, want, compiled.Args)
	}
}

func TestPipelineStrcatAllRequiredPreservesOptionalMultivalueNullness(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(
		t,
		`index=gradethis event_id="v03-05" | makemv delim="," tags_csv | strcat allrequired=true missing ":" tags_csv | table tags_csv`,
	)
	if !compiled.HasValidExecutionSeal() ||
		!slices.Equal(compiled.OutputFields, []string{"tags_csv"}) {
		t.Fatalf("optional multivalue strcat result = %#v", compiled)
	}
	// A successful row writes a scalar String while a failed row retains the
	// prior List, so the conditional column is necessarily Dynamic. The prior
	// sealed presence bit must nevertheless enter the lossless tuple: presence
	// zero converts the physical [] sentinel to Dynamic null instead of leaking
	// it as a present empty List.
	for _, required := range []string{
		`"__os_makemv_value_present_`,
		`if(ifNull(__os_ko_source_present, 0), __os_ko_source_value, CAST(NULL AS Dynamic))`,
		`if(ifNull("__os_makemv_value_present_`,
		`CAST(NULL AS Dynamic)`,
		`AS "__os_strcat_exists_`,
		`toUInt8(ifNull("__os_makemv_value_present_`,
	} {
		if !strings.Contains(compiled.SQL, required) {
			t.Fatalf("optional multivalue preservation SQL missing %q:\n%s", required, compiled.SQL)
		}
	}
	if len(compiled.OptionalMultivalueOutputs) != 0 {
		t.Fatalf(
			"mixed String/List strcat forged an Array(String)-only descriptor: %#v",
			compiled.OptionalMultivalueOutputs,
		)
	}
	if got, want := strings.Count(compiled.SQL, "?"), len(compiled.Args); got != want {
		t.Fatalf("strcat placeholders = %d, args = %d: %#v", got, want, compiled.Args)
	}
}

func TestPipelineStrcatAllRequiredPreservesFlattenedContainerAuthority(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(
		t,
		`index=gradethis | strcat allrequired=true missing ":" parent parent | table parent`,
	)
	want := []ResultContainerOutput{canonicalResultContainerOutput(0)}
	got, valid := compiled.ValidatedResultContainerOutputs()
	if !compiled.HasValidExecutionSeal() || !valid || !slices.Equal(got, want) {
		t.Fatalf("container strcat authority = %#v / valid %t, want %#v", got, valid, want)
	}
	for _, required := range []string{
		`AS "__os_strcat_names_`,
		`AS "__os_strcat_types_`,
		`AS "__os_strcat_metadata_version_`,
		`AS "__os_result_container_names_0"`,
		`AS "__os_result_container_types_0"`,
		`AS "__os_result_container_metadata_version_0"`,
	} {
		if !strings.Contains(compiled.SQL, required) {
			t.Fatalf("container preservation SQL missing %q:\n%s", required, compiled.SQL)
		}
	}
	for _, authority := range []string{"parent", "parent."} {
		if !pipelineArgumentContainsString(compiled.Args, authority) {
			t.Fatalf("container preservation args omit %q: %#v", authority, compiled.Args)
		}
	}
	if got, want := strings.Count(compiled.SQL, "?"), len(compiled.Args); got != want {
		t.Fatalf("strcat placeholders = %d, args = %d: %#v", got, want, compiled.Args)
	}
}
