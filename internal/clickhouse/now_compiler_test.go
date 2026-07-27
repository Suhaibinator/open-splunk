package clickhouse

import (
	"strings"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/plan"
)

func TestCompileNowBindsImmutableSearchStartAtWholeSecondPrecision(t *testing.T) {
	t.Parallel()

	const source = `index=gradethis | eval started=now(), rendered=tostring(now()) | table started,rendered`
	started := time.Date(2026, time.July, 27, 19, 20, 21, 987654321, time.FixedZone("fixture", -7*60*60))
	visibility := uint64(73)
	scope := testChartScope()
	scope.SearchStart = started
	scope.IndexTimeCutoff = started.Add(17 * time.Second)
	scope.VisibilityCutoff = &visibility
	logical := buildPlanWithScope(t, source, scope)
	compiled, err := (Compiler{}).Compile(logical)
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	for _, required := range []string{
		`CAST(? AS Int64) AS "started"`,
		`toString(CAST(? AS Int64)) AS "rendered"`,
	} {
		if !strings.Contains(compiled.SQL, required) {
			t.Fatalf("now SQL missing %q:\n%s", required, compiled.SQL)
		}
	}
	if strings.Contains(compiled.SQL, "now(") || strings.Contains(compiled.SQL, "now64(") {
		t.Fatalf("now lowering depends on ClickHouse wall clock:\n%s", compiled.SQL)
	}
	if got := countArgument(compiled.Args, started.Unix()); got != 2 {
		t.Fatalf("search-start argument count = %d, want 2: %#v", got, compiled.Args)
	}
}

func TestCompileNowPreservesAnchorAcrossProjectionAndAggregation(t *testing.T) {
	t.Parallel()

	const source = `index=gradethis | eval first=now() | table first | stats count BY first | eval second=now() | where first=second | table first,second`
	logical := buildPlan(t, source)
	compiled, err := (Compiler{}).Compile(logical)
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	if got := countArgument(compiled.Args, logical.SearchStart.Unix()); got != 2 {
		t.Fatalf("stable search-start argument count = %d, want 2: %#v", got, compiled.Args)
	}
	if countArgument(compiled.Args, int64(0)) != 0 {
		t.Fatalf("search-start anchor was reset across a transforming stage: %#v", compiled.Args)
	}
}

func TestCompileNowRejectsMissingSearchStartAnchor(t *testing.T) {
	t.Parallel()

	logical := buildPlan(t, `index=gradethis | eval started=now()`)
	logical.SearchStart = time.Time{}
	_, err := (Compiler{}).Compile(logical)
	if err == nil || !strings.Contains(err.Error(), "search-start anchor is required") {
		t.Fatalf("Compile missing search start error = %v, want explicit anchor rejection", err)
	}
}

func TestCompileNowRejectsForgedArguments(t *testing.T) {
	t.Parallel()

	base := buildPlan(t, `index=gradethis`)
	valid := &plan.ScalarCallExpression{Function: plan.ScalarFunctionNow}
	if err := compileForgedScalarAssignment(t, base, valid); err != nil {
		t.Fatalf("Compile valid forged now: %v", err)
	}

	invalid := &plan.ScalarCallExpression{
		Function: plan.ScalarFunctionNow,
		Arguments: []plan.ScalarExpression{&plan.ScalarLiteralExpression{
			Value: plan.Value{Kind: plan.ValueKindInt64, Int64: 1},
		}},
	}
	err := compileForgedScalarAssignment(t, base, invalid)
	if err == nil || !strings.Contains(err.Error(), "now requires no arguments") {
		t.Fatalf("Compile forged now error = %v, want zero-arity rejection", err)
	}
}
