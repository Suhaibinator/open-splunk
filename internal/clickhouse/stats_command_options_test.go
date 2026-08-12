package clickhouse

import (
	"strings"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/plan"
)

func TestCompileStatsAllNumericGuardsEachGroup(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(
		t,
		`index=gradethis | stats allnum=true avg(status) AS average `+
			`p95(status) AS percentile range(status) AS span BY host`,
	)
	if len(compiled.OutputFields) != 4 {
		t.Fatalf("output fields = %v", compiled.OutputFields)
	}
	if got := strings.Count(compiled.SQL, ` AS "__os_measure_all_numeric_invalid_0"`); got != 1 {
		t.Fatalf("allnum input guards = %d, want one shared guard:\n%s", got, compiled.SQL)
	}
	if got := strings.Count(compiled.SQL, `max("__os_measure_all_numeric_invalid_0")`); got != 3 {
		t.Fatalf("allnum result guards = %d, want one per numerical result:\n%s", got, compiled.SQL)
	}
	if !strings.Contains(compiled.SQL, `GROUP BY "host"`) {
		t.Fatalf("allnum grouped query lost its BY key:\n%s", compiled.SQL)
	}
}

func TestCompileStatsOptionsDefaultsAndScalarNoOps(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(
		t,
		`index=gradethis | stats partitions=2 delim="," count `+
			`dedup_splitvals=true`,
	)
	if len(compiled.OutputFields) != 1 || compiled.OutputFields[0] != "count" {
		t.Fatalf("output fields = %v", compiled.OutputFields)
	}
	if strings.Contains(compiled.SQL, "__os_measure_all_numeric_invalid_") {
		t.Fatalf("non-numerical stats unexpectedly acquired an allnum guard:\n%s", compiled.SQL)
	}
}

func TestCompileStatsAllNumericFalseKeepsPartialNumericValues(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(
		t,
		`index=gradethis | stats allnum=false avg(status) AS average BY host`,
	)
	if strings.Contains(compiled.SQL, "__os_measure_all_numeric_invalid_") ||
		strings.Contains(compiled.SQL, "all_numeric") {
		t.Fatalf("allnum=false unexpectedly changed numeric lowering:\n%s", compiled.SQL)
	}
}

func TestCompileStatsRejectsInvalidDelimiterForEveryMeasureFamily(t *testing.T) {
	t.Parallel()

	for _, source := range []string{
		`index=gradethis | stats count`,
		`index=gradethis | stats list(host) AS hosts`,
	} {
		logical := buildPlan(t, source)
		aggregate := logical.Operators[len(logical.Operators)-1].(*plan.Aggregate)
		aggregate.StatsOptions.Delimiter = string([]byte{0xff})
		if _, err := (Compiler{}).Compile(logical); err == nil ||
			!strings.Contains(err.Error(), "valid UTF-8") {
			t.Fatalf("Compile(%q) error = %v, want invalid delimiter rejection", source, err)
		}
	}
}
