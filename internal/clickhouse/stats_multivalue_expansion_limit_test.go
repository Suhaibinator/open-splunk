package clickhouse

import (
	"strconv"
	"strings"
	"testing"
)

func TestCompileStatsMultivalueByGuardsCartesianExpansion(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(
		t,
		`index=gradethis | stats count BY tags zones dedup_splitvals=true`,
	)
	if got := strings.Count(compiled.SQL, StatsMultivalueByExpansionLimitMarker); got != 1 {
		t.Fatalf("multivalue BY expansion guards = %d, want 1:\n%s", got, compiled.SQL)
	}
	guardAt := strings.Index(compiled.SQL, StatsMultivalueByExpansionLimitMarker)
	firstExpansionAt := strings.Index(
		compiled.SQL,
		`ARRAY JOIN "__os_group_values_0" AS "__os_group_value_0"`,
	)
	if guardAt < 0 || firstExpansionAt < 0 || guardAt >= firstExpansionAt {
		t.Fatalf("multivalue BY guard does not precede the first expansion:\n%s", compiled.SQL)
	}
	for _, required := range []string{
		`arrayFold((product, cardinality) -> multiIf(`,
		`product = toUInt64(0) OR cardinality = toUInt64(0), toUInt64(0)`,
		`cardinality > intDiv(toUInt64(10000), greatest(product, toUInt64(1)))`,
		`toUInt64(length("__os_group_values_0"))`,
		`toUInt64(length("__os_group_values_1"))`,
		`toUInt64(10001)`,
		`max(toUInt8((arrayFold(`,
		`"__os_stats_mv_by_any_over_limit" != 0`,
		`"__os_stats_mv_by_combinations" != toUInt64(0)`,
		`* EXCEPT ("__os_stats_mv_by_combinations", "__os_stats_mv_by_any_over_limit")`,
	} {
		if !strings.Contains(compiled.SQL, required) {
			t.Fatalf("multivalue BY guard SQL missing %q:\n%s", required, compiled.SQL)
		}
	}
	if dedupAt := strings.Index(compiled.SQL, `arrayDistinct(`); dedupAt < 0 || dedupAt >= guardAt {
		t.Fatalf("dedup_splitvals does not materialize before the expansion guard:\n%s", compiled.SQL)
	}
}

func TestCompileStatsMultivalueByGuardUsesEffectiveMemberArrays(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(
		t,
		`index=gradethis | stats count BY tags zones dedup_splitvals=false`,
	)
	if !strings.Contains(compiled.SQL, StatsMultivalueByExpansionLimitMarker) {
		t.Fatalf("multivalue BY SQL is missing its expansion guard:\n%s", compiled.SQL)
	}
	if strings.Contains(compiled.SQL, `arrayDistinct(`) {
		t.Fatalf("default multivalue BY guard unexpectedly deduplicates members:\n%s", compiled.SQL)
	}
	// The guard consumes the same empty-array-preserving aliases as ARRAY JOIN.
	// It must not substitute one for missing dimensions, because one missing BY
	// value makes the event produce zero Cartesian rows.
	for _, alias := range []string{
		`length("__os_group_values_0")`,
		`length("__os_group_values_1")`,
	} {
		if !strings.Contains(compiled.SQL, alias) {
			t.Fatalf("multivalue BY guard does not use %s:\n%s", alias, compiled.SQL)
		}
	}
}

func TestCompileStatsMultivalueByGuardTreatsScalarDimensionsAsOne(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(
		t,
		`index=gradethis | stats count BY host tags zones`,
	)
	guardAt := strings.Index(compiled.SQL, StatsMultivalueByExpansionLimitMarker)
	if guardAt < 0 {
		t.Fatalf("mixed scalar/multivalue BY is missing its expansion guard:\n%s", compiled.SQL)
	}
	guardPrefix := compiled.SQL[:guardAt]
	for _, multivalueAlias := range []string{
		`length("__os_group_values_1")`,
		`length("__os_group_values_2")`,
	} {
		if !strings.Contains(guardPrefix, multivalueAlias) {
			t.Fatalf("mixed BY guard is missing %s:\n%s", multivalueAlias, compiled.SQL)
		}
	}
	if strings.Contains(guardPrefix, `length("__os_group_values_0")`) {
		t.Fatalf("mixed BY guard assigned a cardinality to scalar host:\n%s", compiled.SQL)
	}
}

func TestCompileScalarStatsByDoesNotEmitMultivalueExpansionGuard(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(t, `index=gradethis | stats count BY host service`)
	if strings.Contains(compiled.SQL, StatsMultivalueByExpansionLimitMarker) ||
		strings.Contains(compiled.SQL, `arrayFold((product, cardinality)`) {
		t.Fatalf("scalar stats BY unexpectedly contains an expansion guard:\n%s", compiled.SQL)
	}
}

func TestStatsMultivalueByExpansionGuardRejectsInvalidContract(t *testing.T) {
	t.Parallel()

	if product, err := statsMultivalueByExpansionProductSQL(nil); err == nil || product != "" {
		t.Fatalf("empty expansion product = %q, %v, want defensive failure", product, err)
	}
	for _, expansion := range []compiledStatsGroupExpansion{
		{valueAlias: `"value"`},
		{valuesAlias: `"values"`},
	} {
		if product, err := statsMultivalueByExpansionProductSQL(
			[]compiledStatsGroupExpansion{expansion},
		); err == nil || product != "" {
			t.Fatalf("invalid expansion product = %q, %v, want defensive failure", product, err)
		}
	}
	for _, aliases := range [][2]string{
		{"", `"overflow"`},
		{`"product"`, ""},
	} {
		if guard, err := statsMultivalueByExpansionGuardSQL(
			aliases[0],
			aliases[1],
		); err == nil || guard != "" {
			t.Fatalf("invalid expansion guard = %q, %v, want defensive failure", guard, err)
		}
	}
}

func TestStatsMultivalueByExpansionLimitConstantsRemainStable(t *testing.T) {
	t.Parallel()

	if MaximumStatsMultivalueByCombinationsPerEvent != 10_000 {
		t.Fatalf(
			"stats multivalue BY per-event limit = %d, want 10000",
			MaximumStatsMultivalueByCombinationsPerEvent,
		)
	}
	if StatsMultivalueByExpansionLimitMarker !=
		"open-splunk: stats multivalue BY expansion exceeds the per-event limit" {
		t.Fatalf(
			"stats multivalue BY expansion marker = %q",
			StatsMultivalueByExpansionLimitMarker,
		)
	}
	if strconv.FormatUint(MaximumStatsMultivalueByCombinationsPerEvent+1, 10) != "10001" {
		t.Fatal("stats multivalue BY saturation sentinel changed")
	}
}
