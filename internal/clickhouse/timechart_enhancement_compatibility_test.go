package clickhouse

import (
	"strings"
	"testing"
	"time"
)

// TestTimechartEnhancementCrossFeatureCompileMatrix keeps combinations that
// cross the grid, split-series, and continuation implementations progressing
// through one parser/planner/compiler boundary. Focused package tests own each
// option in isolation; these cases protect the seams and the one-read contract.
func TestTimechartEnhancementCrossFeatureCompileMatrix(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name   string
		source string
	}{
		{
			name: "subsecond sparse static suffix",
			source: `index=gradethis
| timechart span=250ms cont=false partial=false fixedrange=false count
| where count > 0
| sort 0 +_time`,
		},
		{
			name: "calendar split suffix with unlimited series",
			source: `index=gradethis
| timechart span=2d cont=false count BY service limit=0 useother=false
| head 1`,
		},
		{
			name: "relative alignment and literal runtime label",
			source: `index=gradethis
| timechart span=2h aligntime=-30m count BY host limit=0
| where 'west coast' > 0
| stats sum(*)`,
		},
		{
			name: "sealed wildcard runtime schema",
			source: `index=gradethis
| timechart span=1h count BY service limit=0
| fields _time api*`,
		},
		{
			name: "repeated chart after dynamic projection",
			source: `index=gradethis
| timechart span=1h count BY host limit=0
| fields _time east
| timechart span=1h sum(east) AS total
| head 1`,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			scope := testChartScope()
			scope.Earliest = time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)
			scope.Latest = scope.Earliest.Add(48 * time.Hour)
			if test.name == "subsecond sparse static suffix" {
				scope.Latest = scope.Earliest.Add(time.Second)
			}
			compiled := compileSPLWithScope(t, test.source, scope)
			if compiled.SQL == "" || !compiled.HasValidExecutionSeal() {
				t.Fatalf("compiled query is incomplete or unsealed: %#v", compiled)
			}
			if !compiled.RequiresAtomicResult() {
				t.Fatal("timechart continuation lost complete-result validation")
			}
			if scans := strings.Count(compiled.SQL, `FROM "open_splunk"."events"`); scans != 1 {
				t.Fatalf("events source references = %d, want 1\nSQL: %s", scans, compiled.SQL)
			}
			for _, field := range compiled.OutputFields {
				if strings.HasPrefix(strings.ToLower(field), "__os_") {
					t.Fatalf("compiler-private output field leaked: %q", field)
				}
			}
		})
	}
}
