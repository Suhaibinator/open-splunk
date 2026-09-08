package clickhouse

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/plan"
	"github.com/Suhaibinator/open-splunk/internal/splregex"
)

func TestCompileReplacePreservesParametersAndAccountsKnowledgeWork(t *testing.T) {
	t.Parallel()

	for _, pattern := range []string{`ms$`, `(?ims)^(word).+$`, `^(\d{1,2})/(\d{1,2})/`, `\Q(a){12}\E`} {
		const replacement = `\2/\1/`
		program := testKnowledgeEvidenceProgram(t, "replace-accounting")
		logical, err := plan.InjectKnowledgePrelude(buildPlan(t,
			`index=gradethis | eval value=replace(message, "`+pattern+`", "`+replacement+`") | table value`,
		), program)
		if err != nil {
			t.Fatal(err)
		}
		compiled, err := (Compiler{}).Compile(logical)
		if err != nil {
			t.Fatal(err)
		}
		if !containsArgument(compiled.Args, pattern) || !containsArgument(compiled.Args, replacement) ||
			!strings.Contains(compiled.SQL, "replaceRegexpAll(") || strings.Contains(compiled.SQL, pattern) {
			t.Fatalf("replacement parameters changed: %#v", compiled.Args)
		}
		validated, err := splregex.CompileReplacePattern(pattern)
		if err != nil {
			t.Fatal(err)
		}
		evidence, ok := compiled.KnowledgeSnapshotEvidenceFor(program)
		if !ok || evidence.AuthoredRegexPrograms() != 1 ||
			evidence.AuthoredRegexWorkUnits() != uint64(validated.ProgramWorkUnits) ||
			evidence.RegexPrograms() != program.Charges().RegexPrograms+1 ||
			evidence.RegexWorkUnits() != program.Charges().RegexWorkUnits+uint64(validated.ProgramWorkUnits) {
			t.Fatalf("replacement regex accounting = %#v", evidence)
		}
	}
}

func TestCompileReplaceRejectsDirectPlanPatternResourceLimits(t *testing.T) {
	t.Parallel()

	for _, quoted := range []bool{false, true} {
		for _, pattern := range []string{`(?:ab|cd){900}`, `(abc){900}`, strings.Repeat("x", splregex.MaximumMatchPatternBytes+1)} {
			logical := buildPlan(t, `index=gradethis | eval value=replace(message, "ok", "")`)
			call := logical.Operators[len(logical.Operators)-1].(*plan.Extend).Assignments[0].Expression.(*plan.ScalarCallExpression)
			literal := call.Arguments[1].(*plan.ScalarLiteralExpression)
			literal.Value.String = pattern
			literal.Value.Quoted = quoted
			_, err := (Compiler{}).Compile(logical)
			var diagnostic *plan.Diagnostic
			if !errors.As(err, &diagnostic) || diagnostic.Code != "SPL_QUERY_TOO_COMPLEX" || diagnostic.Range != literal.Range {
				t.Fatalf("Compile(quoted=%v) error = %v, want pattern complexity diagnostic", quoted, err)
			}
		}
	}
}

func TestCompileReplaceChargesEveryOccurrenceAndMixedRegexPrograms(t *testing.T) {
	t.Parallel()

	pattern := strings.Repeat("a{1000}", 4)
	replace := `replace(message, "` + pattern + `", "")`
	assignments := make([]string, 5)
	for index := range assignments {
		assignments[index] = fmt.Sprintf("v%d=%s", index, replace)
	}
	for _, source := range []string{
		`index=gradethis | eval ` + strings.Join(assignments, ", "),
		`index=gradethis | eval ` + strings.Join(assignments, " | eval "),
		`index=gradethis | where ` + strings.Repeat(replace+`="x" OR `, 4) + replace + `="x"`,
		`index=gradethis | eval v=if(message="x", ` + replace + `, ` + replace + `), w=case(message="x", ` + replace + `, message="y", ` + replace + `, message="z", ` + replace + `)`,
		`index=gradethis | eval ` + strings.Join(assignments[:4], ", ") + ` | regex message="` + pattern + `"`,
		`index=gradethis | eval ` + strings.Join(assignments[:4], ", ") + ` | where match(message, "` + pattern + `")`,
		`index=gradethis | eval ` + strings.Join(assignments[:4], ", ") + `, found=mvfind(split(message, ","), "` + pattern + `")`,
		`index=gradethis | regex message="` + pattern + `" | eval ` + strings.Join(assignments[:3], ", ") + `, found=mvfind(split(message, ","), "` + pattern + `")`,
	} {
		_, err := (Compiler{}).Compile(buildPlan(t, source))
		assertReplaceQueryBudgetError(t, err)
	}
	// The same pointer and unquoted string values are supported plan forms.
	// Every occurrence still incurs its own budget charge.
	for _, quoted := range []bool{false, true} {
		logical := buildPlan(t, `index=gradethis | eval value=`+replace)
		extend := logical.Operators[len(logical.Operators)-1].(*plan.Extend)
		assignment := extend.Assignments[0]
		call := assignment.Expression.(*plan.ScalarCallExpression)
		call.Arguments[1].(*plan.ScalarLiteralExpression).Value.Quoted = quoted
		if _, err := (Compiler{}).Compile(logical); err != nil {
			t.Fatalf("Compile one occurrence with quoted=%v: %v", quoted, err)
		}
		for index := 1; index < 5; index++ {
			repeated := assignment
			repeated.Output = plan.FieldRef{Name: fmt.Sprintf("v%d", index)}
			extend.Assignments = append(extend.Assignments, repeated)
		}
		_, err := (Compiler{}).Compile(logical)
		assertReplaceQueryBudgetError(t, err)
	}
}

func TestCompileReplaceChargesNestedCallsAndAcceptsBoundedQueries(t *testing.T) {
	t.Parallel()

	pattern := strings.Repeat("a{1000}", 4)
	expression := "message"
	for range 4 {
		expression = `replace(` + expression + `, "` + pattern + `", "")`
	}
	compileSPL(t, `index=gradethis | eval value=`+expression)
	expression = `replace(` + expression + `, "` + pattern + `", "")`
	_, err := (Compiler{}).Compile(buildPlan(t, `index=gradethis | eval value=`+expression))
	assertReplaceQueryBudgetError(t, err)
}

func TestCompileReplaceScalarChargesWithoutAuthoredPreflight(t *testing.T) {
	t.Parallel()

	logical := buildPlan(t, `index=gradethis | eval value=replace("text", "a{1000}", "")`)
	call := logical.Operators[len(logical.Operators)-1].(*plan.Extend).Assignments[0].Expression.(*plan.ScalarCallExpression)
	state := compileState{context: &compileContext{}}
	state.context.patternBudgets.match.programWorkUnits = splregex.MaximumMatchQueryProgramWorkUnits
	_, err := compileReplaceScalar(call, state)
	assertReplaceQueryBudgetError(t, err)
}

func assertReplaceQueryBudgetError(t *testing.T, err error) {
	t.Helper()
	var diagnostic *plan.Diagnostic
	if !errors.As(err, &diagnostic) || diagnostic.Code != "SPL_QUERY_TOO_COMPLEX" ||
		!strings.Contains(diagnostic.Message, "match programs") {
		t.Fatalf("Compile error = %v, want cumulative regex program diagnostic", err)
	}
}
