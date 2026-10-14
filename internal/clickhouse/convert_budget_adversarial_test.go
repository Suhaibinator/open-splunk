package clickhouse

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/plan"
	"github.com/Suhaibinator/open-splunk/internal/spl"
)

func TestConvertBudgetAcceptsBoundedNativeTimeAndNumericWrites(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		function string
		count    int
	}{{"ctime(_time)", spl.MaximumConvertWrites}, {"num(value)", 16}, {"rmunit(value)", 16}} {
		function := test.function
		clauses := make([]string, test.count)
		for index := range clauses {
			clauses[index] = fmt.Sprintf("%s AS output_%d", function, index)
		}
		logical := buildPlan(t, "index=gradethis | convert "+strings.Join(clauses, " "))
		compiled, err := (Compiler{}).Compile(logical)
		if err != nil {
			t.Fatalf("%d %s writes: %v", test.count, function, err)
		}
		if !compiled.HasValidExecutionSeal() {
			t.Fatalf("%d %s writes lack executable authority", test.count, function)
		}
		if strings.Count(compiled.SQL, "?") != len(compiled.Args) {
			t.Fatalf("%d %s writes changed bind contract", test.count, function)
		}
		t.Logf("%d %s writes: %d SQL bytes", test.count, function, len(compiled.SQL))
	}
}

func TestConvertBudgetSharesParserSQLAndDepthLimits(t *testing.T) {
	t.Parallel()
	for _, function := range []string{"ctime(value)", "mktime(value)", "auto(value)", "num(value)", "rmunit(value)"} {
		clauses := make([]string, spl.MaximumConvertWrites)
		for index := range clauses {
			clauses[index] = fmt.Sprintf("%s AS output_%d", function, index)
		}
		logical := buildPlan(t, "index=gradethis | convert "+strings.Join(clauses, " "))
		_, err := (Compiler{}).Compile(logical)
		diagnostic, ok := errors.AsType[*plan.Diagnostic](err)
		if !ok || diagnostic.Code != "SPL_QUERY_TOO_COMPLEX" {
			t.Fatalf("64 %s writes error = %v, want resource diagnostic", function, err)
		}
	}
}

func TestConvertBudgetRejectsForgedWriteCountAndFieldMetadata(t *testing.T) {
	t.Parallel()
	logical := buildPlan(t, `index=gradethis | convert num(value)`)
	conversion := logical.Operators[len(logical.Operators)-1].(*plan.Convert)
	original := conversion.Assignments[0]
	conversion.Assignments = make([]plan.ConvertAssignment, spl.MaximumConvertWrites+1)
	for index := range conversion.Assignments {
		conversion.Assignments[index] = original
	}
	if _, err := (Compiler{}).Compile(logical); err == nil {
		t.Fatal("forged 65 writes compiled")
	}
	logical = buildPlan(t, `index=gradethis | convert num(value)`)
	conversion = logical.Operators[len(logical.Operators)-1].(*plan.Convert)
	conversion.Assignments[0].Output.Canonical = true
	if _, err := (Compiler{}).Compile(logical); err == nil {
		t.Fatal("forged canonical output metadata compiled")
	}
}

func TestCompileConvertPreservesLiteralStatisticsFields(t *testing.T) {
	t.Parallel()
	for _, source := range []string{
		`index=gradethis | stats count AS ".com" | convert num(*)`,
		`index=gradethis | convert num(value) AS ".com" num(.com) | table '.com'`,
	} {
		compiled := compileSPL(t, source)
		if !compiled.HasValidExecutionSeal() || !strings.Contains(compiled.SQL, `".com"`) {
			t.Fatalf("literal field compilation lacks authority/output: %s", source)
		}
	}
}
