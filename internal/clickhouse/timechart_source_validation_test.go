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
				if strings.Count(compiled.SQL, `FROM "open_splunk"."events"`) != 1 || !strings.Contains(compiled.SQL, `CROSS JOIN "__os_timechart_domain"`) {
					t.Fatalf("validation is not source-wide: %s", compiled.SQL)
				}
				witnessSource := "__os_timechart_finalized"
				if strings.HasPrefix(measure, "count") {
					witnessSource = "__os_timechart_collapsed"
				}
				witness := `toUInt8(maxOrDefault("__os_tc_invalid" != 0 OR "__os_tc_collision" != 0)) AS "__os_tc_invalid" FROM ` + quoteIdentifier(witnessSource)
				if !strings.Contains(compiled.SQL, witness) || strings.Contains(compiled.SQL, `"__os_tc_count_map"['']`) ||
					!strings.Contains(compiled.SQL, `maxIf("__os_tc_collision_cardinality", "__os_tc_kind" = 0) > 1`) {
					t.Fatalf("validation depends on clipped bucket maps: %s", compiled.SQL)
				}
				if !compiled.HasValidExecutionSeal() || !compiled.RequiresAtomicResult() {
					t.Fatal("complete validation contract is unsealed")
				}
			})
		}
	}
}
