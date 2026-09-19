package clickhouse

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"math"
	"strings"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/plan"
	"github.com/Suhaibinator/open-splunk/internal/spl"
	"github.com/Suhaibinator/open-splunk/internal/splregex"
)

func TestCompileReplacePathBindingsAndComposition(t *testing.T) {
	t.Parallel()
	const pattern = `/(\d+)(?=/|$)`
	for _, source := range []string{
		`index=gradethis | eval route=replace(path, "/(\d+)(?=/|$)", "/:id") | table route`,
		`index=gradethis | eval route=replace(replace("/123/456", "/(\d+)(?=/|$)", "/123"), "/(\d+)(?=/|$)", "/:id") | table route`,
		`index=gradethis | where replace(path, "/(\d+)(?=/|$)", "/:id")="/:id"`,
		`index=gradethis | eval route=replace(_raw, "/(\d+)(?=/|$)", "/:id") | stats count BY route`,
	} {
		compiled := compileSPL(t, source)
		if strings.Contains(compiled.SQL, pattern) || !strings.Contains(compiled.SQL, "splitByChar") || strings.Count(compiled.SQL, "?") != len(compiled.Args) {
			t.Fatalf("binding contract: %s", source)
		}
	}
	state := replaceOutputTestState(100)
	compiled, err := compileScalarValue(replaceOutputTestCall(replaceOutputTestField(), pattern, "/:id"), state)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Count(compiled.valueSQL, "input_value") != 1 || len(compiled.valueArgs) != 2 || compiled.valueArgs[1] != "/:id" {
		t.Fatalf("input/args: %s %#v", compiled.valueSQL, compiled.valueArgs)
	}
	if compiled.maxStringBytes != 500 || state.context.replaceOutputBytes != 500 {
		t.Fatal("output bound not retained")
	}
}

func TestCompileReplacePathBoundsAndContinuation(t *testing.T) {
	t.Parallel()
	pattern, err := splregex.CompileReplacePattern(`/(a)(?=/|$)`)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name    string
		input   uint64
		sql     int
		initial compiledReplacePathBudget
		reject  bool
	}{
		{name: "exact input", input: MaximumReplacePathInputBytes},
		{name: "over input", input: MaximumReplacePathInputBytes + 1, reject: true},
		{name: "overflow input", input: math.MaxUint64, reject: true},
		{name: "exact query input", input: 1, initial: compiledReplacePathBudget{inputBytes: MaximumReplacePathQueryInputBytes - 1}},
		{name: "over query input", input: 2, initial: compiledReplacePathBudget{inputBytes: MaximumReplacePathQueryInputBytes - 1}, reject: true},
		{name: "overflow query input", input: 1, initial: compiledReplacePathBudget{inputBytes: math.MaxUint64}, reject: true},
		{name: "exact query program", initial: compiledReplacePathBudget{programWorkUnits: splregex.MaximumReplacePathQueryProgramWorkUnits - pattern.ProgramWorkUnits}},
		{name: "over query program", initial: compiledReplacePathBudget{programWorkUnits: splregex.MaximumReplacePathQueryProgramWorkUnits}, reject: true},
		{name: "exact SQL", sql: maxCompiledReplacePathSQLBytes},
		{name: "over SQL", sql: maxCompiledReplacePathSQLBytes + 1, reject: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			context := replaceOutputTestState(1).context
			context.replacePathBudget = tc.initial
			err := reserveReplacePath(context, pattern, tc.input, tc.sql, spl.Range{})
			if tc.reject {
				requireReplaceOutputDiagnostic(t, err, "replace path evaluation")
				if context.replacePathBudget != tc.initial {
					t.Fatal("rejection charged budget")
				}
			} else if err != nil {
				t.Fatal(err)
			}
		})
	}
	context := replaceOutputTestState(1).context
	context.replacePathBudget = compiledReplacePathBudget{inputBytes: MaximumReplacePathQueryInputBytes, programWorkUnits: 200}
	budget := timechartContinuationBudget(context)
	restored := replaceOutputTestState(1).context
	budget.apply(restored)
	if restored.replacePathBudget != context.replacePathBudget {
		t.Fatal("continuation lost budget")
	}
	requireReplaceOutputDiagnostic(t, reserveReplacePath(restored, pattern, 1, 1, spl.Range{}), "replace path evaluation")
	before, after := sha256.New(), sha256.New()
	budget.write(before)
	budget.replacePath.inputBytes--
	budget.write(after)
	if bytes.Equal(before.Sum(nil), after.Sum(nil)) {
		t.Fatal("continuation fingerprint lost budget")
	}
}

func TestCompileReplacePathForgedPlansAndOutputBudget(t *testing.T) {
	t.Parallel()
	for pattern, code := range map[string]string{
		`/(.*)(?=/|$)`: "SPL_UNSUPPORTED_PCRE", `a*`: "SPL_UNSUPPORTED_REGEX",
		"/" + strings.Repeat("a", splregex.MaximumReplacePathPatternBytes) + "(?=/|$)": "SPL_QUERY_TOO_COMPLEX",
	} {
		state := replaceOutputTestState(1)
		_, err := compileScalarValue(replaceOutputTestCall(replaceOutputTestField(), pattern, "x"), state)
		var diagnostic *plan.Diagnostic
		if !errors.As(err, &diagnostic) || diagnostic.Code != code {
			t.Fatalf("%q: %v", pattern, err)
		}
	}
	for _, input := range []uint64{MaximumReplaceOutputBytes/5 + 1, math.MaxUint64} {
		_, err := compileScalarValue(replaceOutputTestCall(replaceOutputTestField(), `/(a)(?=/|$)`, "/:id"), replaceOutputTestState(input))
		requireReplaceOutputDiagnostic(t, err, "replace output may exceed")
	}
	stateTooLarge := replaceOutputTestState(MaximumReplacePathInputBytes + 1)
	_, inputErr := compileScalarValue(replaceOutputTestCall(replaceOutputTestField(), `/(a)(?=/|$)`, ""), stateTooLarge)
	requireReplaceOutputDiagnostic(t, inputErr, "replace path evaluation")
	if stateTooLarge.context.replaceOutputBytes != 0 || stateTooLarge.context.replacePathBudget != (compiledReplacePathBudget{}) {
		t.Fatal("rejected input charged replacement budgets")
	}
	state := replaceOutputTestState(MaximumStoredScalarBytes)
	for range 4 {
		if _, err := compileScalarValue(replaceOutputTestCall(replaceOutputTestField(), `/(a)(?=/|$)`, strings.Repeat("x", 15)), state); err != nil {
			t.Fatal(err)
		}
	}
	_, err := compileScalarValue(replaceOutputTestCall(replaceOutputTestField(), `/(a)(?=/|$)`, strings.Repeat("x", 15)), state)
	requireReplaceOutputDiagnostic(t, err, "replace outputs may exceed")
}
