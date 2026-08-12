package clickhouse

import (
	"strings"
	"testing"
)

func TestCompileStatsMultivalueByExpandsAndDeduplicates(t *testing.T) {
	t.Parallel()

	withoutDedup := compileSPL(
		t,
		`index=gradethis | stats count sum(score) AS total BY tags`,
	)
	for _, required := range []string{
		` AS "__os_group_values_0"`,
		`ARRAY JOIN "__os_group_values_0" AS "__os_group_value_0"`,
		`GROUP BY "__os_group_value_0"`,
	} {
		if !strings.Contains(withoutDedup.SQL, required) {
			t.Fatalf("multivalue BY SQL missing %q:\n%s", required, withoutDedup.SQL)
		}
	}
	if strings.Contains(withoutDedup.SQL, `arrayDistinct(`) {
		t.Fatalf("default multivalue BY unexpectedly deduplicates values:\n%s", withoutDedup.SQL)
	}

	withDedup := compileSPL(
		t,
		`index=gradethis | stats count sum(score) AS total BY tags `+
			`dedup_splitvals=true`,
	)
	if !strings.Contains(withDedup.SQL, `arrayDistinct(`) {
		t.Fatalf("dedup_splitvals=true did not deduplicate before expansion:\n%s", withDedup.SQL)
	}
}

func TestCompileStatsMultipleMultivalueByUsesCartesianStages(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(
		t,
		`index=gradethis | stats count BY tags zones dedup_splitvals=true`,
	)
	if got := strings.Count(compiled.SQL, ` ARRAY JOIN `); got != 2 {
		t.Fatalf("multivalue BY expansion stages = %d, want 2:\n%s", got, compiled.SQL)
	}
	first := `ARRAY JOIN "__os_group_values_0" AS "__os_group_value_0"`
	second := `ARRAY JOIN "__os_group_values_1" AS "__os_group_value_1"`
	firstAt := strings.Index(compiled.SQL, first)
	secondAt := strings.Index(compiled.SQL, second)
	if firstAt < 0 || secondAt < 0 || firstAt >= secondAt {
		t.Fatalf("multivalue BY expansions are not staged Cartesian joins:\n%s", compiled.SQL)
	}
	if strings.Contains(compiled.SQL, `ARRAY JOIN "__os_group_values_0" AS "__os_group_value_0",`) {
		t.Fatalf("multivalue BY arrays were positionally zipped:\n%s", compiled.SQL)
	}
}

func TestCompileStatsFixedScalarByDoesNotExpandRows(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(t, `index=gradethis | stats count BY host`)
	if strings.Contains(compiled.SQL, "ARRAY JOIN") {
		t.Fatalf("fixed scalar BY unexpectedly expanded rows:\n%s", compiled.SQL)
	}
}

func TestCompileStatsFixedMultivalueResultCanFeedBy(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(
		t,
		`index=gradethis | stats values(user) AS users | stats count BY users`,
	)
	if !strings.Contains(
		compiled.SQL,
		`ARRAY JOIN "__os_group_values_0" AS "__os_group_value_0"`,
	) {
		t.Fatalf("fixed multivalue stats result was not expanded by downstream BY:\n%s", compiled.SQL)
	}
	if strings.Contains(compiled.SQL, UnsupportedStatsByValueMarker) {
		t.Fatalf("fixed multivalue BY retained a dynamic-container rejection:\n%s", compiled.SQL)
	}
}
