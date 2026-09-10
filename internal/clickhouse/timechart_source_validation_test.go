package clickhouse

import (
	"strings"
	"testing"
)

func TestTimechartSplitValidationDoesNotDependOnVisibleBuckets(t *testing.T) {
	for _, span := range []string{"1m", "1d"} {
		for _, measure := range []string{"count", "count(missing)", "sum(value)", "avg(value)", "p95(value)", "sum(missing)", "avg(missing)", "p95(missing)"} {
			source := `index=gradethis | table _time host | bin _time span=1w | timechart span=` + span + ` cont=false partial=false ` + measure + ` BY host limit=0`
			t.Run(span+"/"+measure, func(t *testing.T) {
				compiled := compileSPL(t, source)
				if strings.Count(compiled.SQL, `FROM "open_splunk"."events"`) != 1 || !strings.Contains(compiled.SQL, `CROSS JOIN "__os_timechart_validation"`) {
					t.Fatalf("validation is not source-wide: %s", compiled.SQL)
				}
				if strings.HasPrefix(measure, "count") {
					witness := `"__os_timechart_validation" AS (SELECT toUInt8(maxOrDefault("__os_tc_invalid" != 0 OR "__os_tc_collision" != 0)) AS "__os_tc_invalid" FROM "__os_timechart_collapsed")`
					if !strings.Contains(compiled.SQL, witness) || strings.Contains(compiled.SQL, `"__os_tc_count_map"['']`) {
						t.Fatalf("count validation depends on bucket map: %s", compiled.SQL)
					}
				} else if !strings.Contains(compiled.SQL, `CROSS JOIN "__os_timechart_normalization_collisions"`) {
					t.Fatalf("numeric collision witness missing: %s", compiled.SQL)
				}
				if !compiled.HasValidExecutionSeal() || !compiled.RequiresAtomicResult() {
					t.Fatal("complete validation contract is unsealed")
				}
			})
		}
	}
}
