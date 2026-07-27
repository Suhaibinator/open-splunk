package clickhouse

import (
	"strings"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/plan"
	"github.com/Suhaibinator/open-splunk/internal/spl"
)

func TestCompileNowBindsImmutableSearchStartAtWholeSecondPrecision(t *testing.T) {
	t.Parallel()

	const source = `index=gradethis | eval started=now(), rendered=tostring(now()) | table started,rendered`
	parsed, err := spl.Parse(source)
	if err != nil {
		t.Fatal(err)
	}
	started := time.Date(2026, time.July, 27, 19, 20, 21, 987654321, time.FixedZone("fixture", -7*60*60))
	visibility := uint64(73)
	scope := testChartScope()
	scope.IndexTimeCutoff = started
	scope.VisibilityCutoff = &visibility
	logical, err := plan.Build(parsed, scope)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
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
	scan := logical.Operators[0].(*plan.Scan)
	compiled, err := (Compiler{}).Compile(logical)
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	if got := countArgument(compiled.Args, scan.IndexTimeCutoff.Unix()); got != 2 {
		t.Fatalf("stable search-start argument count = %d, want 2: %#v", got, compiled.Args)
	}
	if countArgument(compiled.Args, int64(0)) != 0 {
		t.Fatalf("search-start anchor was reset across a transforming stage: %#v", compiled.Args)
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
