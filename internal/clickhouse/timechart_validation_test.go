package clickhouse

import (
	"regexp"
	"strings"
	"testing"
	"time"
)

func TestTimechartChronologicalValidationDoesNotReinferTransport(t *testing.T) {
	finalInputPattern := regexp.MustCompile(`"(__os_chronological_final_input_[0-9]+)" AS (?:MATERIALIZED )?\(`)
	for _, span := range []string{"1m", "250ms", "1d"} {
		for _, measure := range []string{"count", "count(value)", "sum(value)", "avg(value)", "p95(value)", "count BY host", "count(value) BY host", "sum(value) BY host", "avg(value) BY host", "p95(value) BY host"} {
			t.Run(span+"/"+measure, func(t *testing.T) {
				scope := testChartScope()
				scope.Latest = scope.Earliest.Add(time.Minute)
				compiled := compileSPLWithScope(t, `index=gradethis | eventstats min(payload) AS ignored | timechart span=`+span+` `+measure, scope)
				finalInput := finalInputPattern.FindStringSubmatch(compiled.SQL)
				if len(finalInput) != 2 {
					t.Fatal("chronological input is missing")
				}
				if got := strings.Count(compiled.SQL, `FROM "`+finalInput[1]+`" AS `); got != 1 {
					t.Fatalf("timechart input consumers = %d, want one", got)
				}
				if !strings.Contains(compiled.SQL, `UNION ALL SELECT`) || !strings.Contains(compiled.SQL, UnsupportedStatsMeasureValueMarker) {
					t.Fatal("complete-source validation branch is missing")
				}
				if !compiled.HasValidExecutionSeal() || strings.Count(compiled.SQL, `FROM "open_splunk"."events"`) != 1 {
					t.Fatal("timechart source is unsealed or repeated")
				}
				if compiled.validationDummyProjection != nil {
					t.Fatal("compiled query retained temporary validation projection")
				}
			})
		}
	}
}
