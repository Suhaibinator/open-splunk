package clickhouse

import (
	"errors"
	"fmt"
	"math"
	"slices"
	"strings"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/plan"
	"github.com/Suhaibinator/open-splunk/internal/spl"
)

func TestCompileReplaceOutputBoundaryAndOverflow(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name       string
		inputBytes uint64
		wantError  bool
	}{
		{"exact", MaximumReplaceOutputBytes / 2, false},
		{"one input byte over", MaximumReplaceOutputBytes/2 + 1, true},
		{"saturating product", math.MaxUint64, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			state := replaceOutputTestState(test.inputBytes)
			expression := replaceOutputTestCall(replaceOutputTestField(), ".", "x")
			expression.Range = spl.Range{Start: spl.Position{Offset: 10}, End: spl.Position{Offset: 30}}
			compiled, err := compileScalarValue(expression, state)
			if test.wantError {
				requireReplaceOutputDiagnostic(t, err, "replace output may exceed")
				var diagnostic *plan.Diagnostic
				if !errors.As(err, &diagnostic) || diagnostic.Range != expression.Range || state.context.replaceOutputBytes != 0 {
					t.Fatalf("rejection lost range or charged output: %#v, %d", diagnostic, state.context.replaceOutputBytes)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if compiled.maxStringBytes != MaximumReplaceOutputBytes ||
				state.context.replaceOutputBytes != MaximumReplaceOutputBytes {
				t.Fatalf("output/reservation = %d/%d", compiled.maxStringBytes, state.context.replaceOutputBytes)
			}
			if compiled.valueSQL != `replaceRegexpAll(input_value, ?, ?)` ||
				!slices.Equal(compiled.valueArgs, []any{".", "x"}) {
				t.Fatalf("accepted replacement changed: %s, %#v", compiled.valueSQL, compiled.valueArgs)
			}
		})
	}
}

func TestCompileReplaceRejectsOversizedProducersBeforeConsumers(t *testing.T) {
	t.Parallel()

	for _, source := range []string{
		`index=gradethis | eval value=replace(_raw,".","abcdefghijklmnop") | table value`,
		`index=gradethis | eval value=replace(category,".","abcdefghijklmnop") | table value`,
		`index=gradethis | where replace(message,".","abcdefghijklmnop")="value"`,
		`index=gradethis | eval value=len(replace(message,".","abcdefghijklmnop"))`,
		`index=gradethis | eval value=substr(replace(message,".","abcdefghijklmnop"),1,1)`,
		`index=gradethis | eval value=if(isnull(category),replace(message,".","abcdefghijklmnop"),"fallback")`,
		`index=gradethis | eval first=replace(message,".","1234567") | eval value=replace(first,".","ab")`,
		`index=gradethis | eval value=replace(replace(message,".","1234567"),".","ab")`,
		`index=gradethis | eval value=replace(message,"(.)","\1\1\1\1\1\1\1\1")`,
		`index=gradethis | eval value=replace(message,".","😀😀😀😀")`,
		`index=gradethis | eval value=replace(message,".","abcdefghijklmnop") | table event_id`,
	} {
		t.Run(source, func(t *testing.T) {
			compiled, err := (Compiler{}).Compile(buildPlan(t, source))
			requireReplaceOutputDiagnostic(t, err, "replace output may exceed")
			if compiled.SQL != "" {
				t.Fatal("rejected replacement published SQL")
			}
		})
	}

	err := compileForgedScalarAssignment(t, buildPlan(t, `index=gradethis`),
		replaceOutputTestCall(&plan.ScalarFieldExpression{
			Field: plan.FieldRef{Name: "message", Path: []string{"message"}},
		}, ".", "abcdefghijklmnop"))
	requireReplaceOutputDiagnostic(t, err, "replace output may exceed")
}

func TestCompileReplaceSharesOutputBudgetAcrossOccurrencesAndStages(t *testing.T) {
	t.Parallel()

	state := replaceOutputTestState(MaximumStoredScalarBytes)
	expression := replaceOutputTestCall(replaceOutputTestField(), ".", "abcdefghijklmno")
	for range MaximumReplaceQueryOutputBytes / MaximumReplaceOutputBytes {
		if _, err := compileScalarValue(expression, cloneCompileState(state)); err != nil {
			t.Fatal(err)
		}
	}
	if state.context.replaceOutputBytes != MaximumReplaceQueryOutputBytes {
		t.Fatalf("shared pointer reservation = %d", state.context.replaceOutputBytes)
	}
	_, err := compileScalarValue(expression, state)
	requireReplaceOutputDiagnostic(t, err, "replace outputs may exceed")
	if state.context.replaceOutputBytes != MaximumReplaceQueryOutputBytes {
		t.Fatal("rejection changed the reservation")
	}
	state.context.replaceOutputBytes = math.MaxUint64
	_, err = compileScalarValue(expression, state)
	requireReplaceOutputDiagnostic(t, err, "replace outputs may exceed")
	state.context = nil
	if _, err = compileScalarValue(expression, state); err == nil || !strings.Contains(err.Error(), "query context is required") {
		t.Fatalf("missing shared context error = %v", err)
	}

	source := `index=gradethis`
	for index := range 5 {
		source += fmt.Sprintf(` | eval value%d=replace(message,".","abcdefghijklmno")`, index)
		compiled, compileErr := (Compiler{}).Compile(buildPlan(t, source))
		if index == 4 {
			requireReplaceOutputDiagnostic(t, compileErr, "replace outputs may exceed")
		} else if compileErr != nil || compiled.SQL == "" {
			t.Fatalf("compile %d replacements: %v", index+1, compileErr)
		}
	}
}

func TestCompileReplacePreservesOrdinaryAndBoundedInputs(t *testing.T) {
	t.Parallel()

	for _, source := range []string{
		`index=gradethis | eval value=replace(duration,"ms$","") | table value`,
		`index=gradethis | eval value=replace(date,"^(\d{1,2})/(\d{1,2})/","\2/\1/") | table value`,
		`index=gradethis | eval message=replace(message,"Request","Updated") | table message`,
		`index=gradethis | eval value=replace(null,".","replacement") | table value`,
		`index=gradethis | stats count AS n | eval value=replace(tostring(n),"0","replacement") | table value`,
		`index=gradethis | eval bounded=if(isnull(category),"` + strings.Repeat("x", 1024) + `","` + strings.Repeat("x", 1024) + `y") | eval value=replace(bounded,"x","` + strings.Repeat("a", 1024) + `") | spath input=value output=selected path=value | table selected`,
	} {
		compiled := compileSPL(t, source)
		if strings.Count(compiled.SQL, "?") != len(compiled.Args) ||
			!strings.Contains(compiled.SQL, "replaceRegexpAll(") {
			t.Fatalf("replacement binding contract changed for %q", source)
		}
	}
}

func replaceOutputTestState(inputBytes uint64) compileState {
	scope := testChartScope()
	return compileState{
		visible: map[string]fieldState{"value": {
			valueSQL: "input_value", maxStringBytes: inputBytes, existsSQL: "1", kind: fieldKindString,
		}},
		context: newCompileContext(scope.SearchStart, scope.SearchTimezone),
	}
}

func replaceOutputTestField() *plan.ScalarFieldExpression {
	return &plan.ScalarFieldExpression{Field: plan.FieldRef{Name: "value", Path: []string{"value"}}}
}

func replaceOutputTestCall(input plan.ScalarExpression, pattern, replacement string) *plan.ScalarCallExpression {
	return &plan.ScalarCallExpression{
		Function: plan.ScalarFunctionReplace,
		Arguments: []plan.ScalarExpression{input,
			&plan.ScalarLiteralExpression{Value: plan.Value{Kind: plan.ValueKindString, String: pattern, Quoted: true}},
			&plan.ScalarLiteralExpression{Value: plan.Value{Kind: plan.ValueKindString, String: replacement, Quoted: true}},
		},
	}
}

func requireReplaceOutputDiagnostic(t *testing.T, err error, message string) {
	t.Helper()
	var diagnostic *plan.Diagnostic
	if !errors.As(err, &diagnostic) || diagnostic.Code != "SPL_QUERY_TOO_COMPLEX" || !strings.Contains(diagnostic.Message, message) {
		t.Fatalf("replace error = %#v, want SPL_QUERY_TOO_COMPLEX containing %q", err, message)
	}
}
