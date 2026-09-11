package clickhouse

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/plan"
	"github.com/Suhaibinator/open-splunk/internal/spl"
)

func continueBudgetTestChart(compiled CompiledQuery) (CompiledQuery, error) {
	return compiled.ContinueContext(context.Background(), []RelationColumn{{"_time", "DateTime64(9, 'UTC')"}, {"api", "UInt64"}}, [][]any{{time.Date(2026, 7, 21, 0, 0, 0, 0, time.UTC), uint64(1)}})
}

func requireCumulativeComplexity(t *testing.T, err error, fragment string) {
	t.Helper()
	var diagnostic *plan.Diagnostic
	if !errors.As(err, &diagnostic) || diagnostic.Code != "SPL_QUERY_TOO_COMPLEX" || !strings.Contains(diagnostic.Message, fragment) {
		t.Fatalf("error=%v, want cumulative %s complexity diagnostic", err, fragment)
	}
}

func TestTimechartCumulativeJSONWork(t *testing.T) {
	commands := ` | eval payload="{\"value\":1}"` + strings.Repeat(` | spath input=payload output=v path=value | where v=1`, 6)
	for _, chart := range []string{` | timechart span=1h count BY host`, ` | timechart span=1h count`} {
		source := `index=gradethis` + commands + chart + commands
		parsed, err := spl.Parse(source)
		if err != nil {
			t.Fatal(err)
		}
		logical, err := plan.Build(parsed, testChartScope())
		if strings.Contains(chart, "BY") {
			if err != nil {
				t.Fatal(err)
			}
			compiled, err := (Compiler{}).Compile(logical)
			if err != nil {
				t.Fatal(err)
			}
			_, continuationErr := continueBudgetTestChart(compiled)
			requireCumulativeComplexity(t, continuationErr, "JSON")
			continue
		}
		requireCumulativeComplexity(t, err, "JSON")
	}
}

func TestTimechartCumulativeExtractionOutputs(t *testing.T) {
	pattern := ""
	for i := range 16 {
		pattern += fmt.Sprintf("(?<capture%d>x)", i)
	}
	prefix := `index=gradethis | eval payload="xxxxxxxxxxxxxxxx"` + strings.Repeat(` | rex field=payload "`+pattern+`"`, 4)
	compiled := compileSPL(t, prefix+` | timechart span=1h count BY host | eval payload="x" | rex field=payload "(?<overflow>x)"`)
	_, err := continueBudgetTestChart(compiled)
	requireCumulativeComplexity(t, err, "extraction")
}

func TestTimechartRepeatedStagesRetainExtractionBudget(t *testing.T) {
	commands := ` | eval payload="{\"value\":1}"` + strings.Repeat(` | spath input=payload output=v path=value | where v=1`, 4)
	compiled := compileSPL(t, `index=gradethis`+commands+` | timechart span=1h count BY host`+commands+` | eval host="api" | timechart span=1h count BY host`+commands)
	next, err := continueBudgetTestChart(compiled)
	if err != nil {
		t.Fatal(err)
	}
	if next.logicalExtractionBudget.jsonEvaluationWork != 24 || !next.HasValidExecutionSeal() {
		t.Fatal("second stage lost cumulative JSON authority")
	}
	_, err = continueBudgetTestChart(next)
	requireCumulativeComplexity(t, err, "JSON")
	next.continuation.compiler.continuationBudget.extraction.jsonEvaluationWork = 0
	if next.HasValidExecutionSeal() {
		t.Fatal("tampered continuation extraction budget retained authority")
	}
}

func TestTimechartCompilerCumulativeRegexPrograms(t *testing.T) {
	// Regex command count is independent from extraction output count. The
	// parser source remains within its own token and predicate limits.
	commands := strings.Repeat(` | regex payload="x"`, 20)
	matches := func(count int) string {
		return strings.TrimSuffix(strings.Repeat(`match(host,"x") OR `, count), " OR ")
	}
	prefix := ` | eval payload=if(` + matches(13) + `,"x","x")` + commands
	suffix := ` | eval payload=if(` + matches(12) + `,"x","x")` + commands
	compiled := compileSPL(t, `index=gradethis`+prefix+` | timechart span=1h count BY host`+suffix)
	_, err := continueBudgetTestChart(compiled)
	requireCumulativeComplexity(t, err, "regular-expression programs")
}

func TestTimechartCompilerExtractionPreflightUsesPriorEvidence(t *testing.T) {
	// Independently enforce the backend trust boundary for hand-built plans.
	previous := authoredKnowledgeCompilation{extractionOutputs: 64, jsonEvaluationWork: 30}
	query, err := spl.Parse(`index=gradethis | spath input=payload output=v path=value`)
	if err != nil {
		t.Fatal(err)
	}
	logical, err := plan.Build(query, testChartScope())
	if err != nil {
		t.Fatal(err)
	}
	_, err = validateCompiledExtractionBudgetsWithPrior(logical.Operators, previous)
	requireCumulativeComplexity(t, err, "JSON")
}
