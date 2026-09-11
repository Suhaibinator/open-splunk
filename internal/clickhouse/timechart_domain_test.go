package clickhouse

import (
	"strings"
	"testing"
)

func TestTimechartDomainGuardsPrecedeLabelArrays(t *testing.T) {
	for _, measure := range []string{"count", "count(value)", "sum(value)", "avg(value)", "p95(value)"} {
		t.Run(measure, func(t *testing.T) {
			compiled := compileSPL(t, `index=gradethis | timechart span=5m `+measure+` BY host limit=0`)
			usage := timechartCTESection(t, compiled.SQL, "__os_timechart_resource_usage", "__os_timechart_guarded_domain_rows")
			for _, expression := range []string{
				`countIf("__os_tc_domain_member" != 0) OVER ()`,
				`sumIf(toUInt64(length("__os_tc_encoded")), "__os_tc_domain_member" != 0) OVER ()`,
				`FROM "__os_timechart_domain_rows"`,
			} {
				if !strings.Contains(usage, expression) {
					t.Fatalf("domain preflight omits %s: %s", expression, usage)
				}
			}
			guard := timechartCTESection(t, compiled.SQL, "__os_timechart_guarded_domain_rows", "__os_timechart_domain")
			if !strings.Contains(guard, `AS MATERIALIZED (SELECT * FROM "__os_timechart_resource_usage" WHERE `) {
				t.Fatalf("domain preflight is not materialized: %s", guard)
			}
			for _, marker := range []string{TimechartDomainLimitMarker, TimechartCellLimitMarker, TimechartRetainedBytesLimitMarker} {
				if strings.Count(guard, marker) != 1 {
					t.Fatalf("domain preflight omits resource guard %s: %s", marker, guard)
				}
			}
			domain := timechartCTESection(t, compiled.SQL, "__os_timechart_domain", "__os_timechart_bucket_maps")
			if !strings.Contains(domain, `groupArrayIf(`) ||
				!strings.Contains(domain, `FROM "__os_timechart_guarded_domain_rows"`) ||
				strings.Contains(usage+guard, "groupArray") {
				t.Fatalf("label array precedes scalar preflight: %s", domain)
			}
			if !strings.Contains(domain, `maxOrDefault("__os_tc_invalid") AS "__os_tc_invalid"`) {
				t.Fatalf("domain dropped complete-source validation: %s", domain)
			}
		})
	}
}
