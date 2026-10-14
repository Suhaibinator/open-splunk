package plan

import (
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/spl"
)

func convertRuntimeQuery(t *testing.T, source string) *spl.Query {
	t.Helper()
	query, err := spl.Parse(source)
	if err != nil {
		t.Fatal(err)
	}
	return query
}

func TestConvertRuntimeDiscoveryReplaysMultipleStages(t *testing.T) {
	query := convertRuntimeQuery(t, `index=gradethis | convert num(value*) | convert ctime(time*) | stats sum(value*) AS total*`)
	scope := testScope([]string{"gradethis"}, nil)
	preparation, err := PrepareStatsWildcard(query, scope)
	if err != nil {
		t.Fatal(err)
	}
	first := preparation.Request()
	if !first.IsConvert() || !first.ValidForPrefix(preparation.Prefix()) {
		t.Fatal("first conversion authority is invalid")
	}
	expansion, err := ValidateStatsWildcardInventory(first, []StatsWildcardInventoryMatch{{Ordinal: 0, Field: "value_ms"}})
	if err != nil {
		t.Fatal(err)
	}
	preparation, err = PrepareNextStatsWildcard(query, scope, expansion)
	if err != nil {
		t.Fatal(err)
	}
	second := preparation.Request()
	if !second.IsConvert() || second.Equal(first) {
		t.Fatal("second stage did not advance")
	}
	expansion, err = ValidateStatsWildcardInventory(second, []StatsWildcardInventoryMatch{{Ordinal: 0, Field: "time_seen"}})
	if err != nil {
		t.Fatal(err)
	}
	preparation, err = PrepareNextStatsWildcard(query, scope, expansion)
	if err != nil {
		t.Fatal(err)
	}
	third := preparation.Request()
	if third.IsConvert() || third.IsZero() {
		t.Fatal("stats stage did not preserve its command identity")
	}
	expansion, err = ValidateStatsWildcardInventory(third, []StatsWildcardInventoryMatch{{Ordinal: 0, Field: "value_ms"}})
	if err != nil {
		t.Fatal(err)
	}
	preparation, err = PrepareNextStatsWildcard(query, scope, expansion)
	if err != nil {
		t.Fatal(err)
	}
	if preparation.FullPlan() == nil {
		t.Fatal("completed evidence still requires discovery")
	}
	replay, err := BuildWithStatsWildcardExpansion(query, scope, expansion.Clone())
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(replay.OutputFields, []string{"total_ms"}) {
		t.Fatalf("outputs=%v", replay.OutputFields)
	}
	if charge, ok := expansion.RetainedBytes(); !ok || charge == 0 {
		t.Fatal("ordered evidence not charged")
	}
}

func TestConvertRuntimeEmptyExpansionIsSealedAndCanAdvance(t *testing.T) {
	query := convertRuntimeQuery(t, `index=gradethis | convert num(absent*) | convert num(value*)`)
	scope := testScope([]string{"gradethis"}, nil)
	preparation, err := PrepareStatsWildcard(query, scope)
	if err != nil {
		t.Fatal(err)
	}
	empty, err := ValidateStatsWildcardInventory(preparation.Request(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if empty.IsZero() {
		t.Fatal("empty inventory must retain authority")
	}
	preparation, err = PrepareNextStatsWildcard(query, scope, empty)
	if err != nil {
		t.Fatal(err)
	}
	expansion, err := ValidateStatsWildcardInventory(preparation.Request(), []StatsWildcardInventoryMatch{{Ordinal: 0, Field: "value"}})
	if err != nil {
		t.Fatal(err)
	}
	logical, err := BuildWithStatsWildcardExpansion(query, scope, expansion)
	if err != nil {
		t.Fatal(err)
	}
	conversions := 0
	for _, operator := range logical.Operators {
		if converted, ok := operator.(*Convert); ok {
			conversions++
			if conversions == 1 && len(converted.Assignments) != 0 {
				t.Fatal("empty conversion gained assignments")
			}
		}
	}
	if conversions != 2 {
		t.Fatalf("conversions=%d", conversions)
	}
	stats := convertRuntimeQuery(t, `index=gradethis | stats sum(value*)`)
	preparation, err = PrepareStatsWildcard(stats, scope)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = ValidateStatsWildcardInventory(preparation.Request(), nil); err == nil {
		t.Fatal("stats empty matching policy changed")
	}
}

func TestConvertRuntimeExclusionsAliasesAndExactWritesShareBudget(t *testing.T) {
	query := convertRuntimeQuery(t, `index=gradethis | convert num(source) AS copied num(cop*) num(*) none(secret*)`)
	scope := testScope([]string{"gradethis"}, nil)
	preparation, err := PrepareStatsWildcard(query, scope)
	if err != nil {
		t.Fatal(err)
	}
	request := preparation.Request()
	if request.MaximumPairs() != 64 {
		t.Fatalf("remaining cap=%d", request.MaximumPairs())
	}
	if !slices.Equal(request.Exclusions(), []string{"secret*"}) || !slices.Contains(request.AdditionalMatches(), StatsWildcardInventoryMatch{Ordinal: 1, Field: "copied"}) {
		t.Fatal("missing exclusions or preceding aliases")
	}
	if _, err = ValidateStatsWildcardInventory(request, []StatsWildcardInventoryMatch{{Ordinal: 2, Field: "secret_key"}}); err == nil {
		t.Fatal("excluded field admitted")
	}
	if _, err = ValidateStatsWildcardInventory(request, []StatsWildcardInventoryMatch{{Ordinal: 2, Field: "_time"}}); err == nil {
		t.Fatal("broad pattern admitted internal name")
	}
	source := `index=gradethis | convert ` + strings.Repeat(`num(source) `, 63) + `num(value*)`
	preparation, err = PrepareStatsWildcard(convertRuntimeQuery(t, source), scope)
	if err != nil {
		t.Fatal(err)
	}
	if preparation.Request().MaximumPairs() != 2 {
		t.Fatal("exact writes not debited")
	}
	if _, err = ValidateStatsWildcardInventory(preparation.Request(), []StatsWildcardInventoryMatch{{Ordinal: 63, Field: "value_a"}, {Ordinal: 63, Field: "value_b"}}); err == nil {
		t.Fatal("65-write command admitted")
	}
}

func TestConvertRuntimeRejectsCrossSourceScopeAndReorderedEvidence(t *testing.T) {
	query := convertRuntimeQuery(t, `index=gradethis | convert num(a*) | convert num(b*)`)
	scope := testScope([]string{"gradethis"}, nil)
	preparation, err := PrepareStatsWildcard(query, scope)
	if err != nil {
		t.Fatal(err)
	}
	first, err := ValidateStatsWildcardInventory(preparation.Request(), []StatsWildcardInventoryMatch{{Ordinal: 0, Field: "a"}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = PrepareNextStatsWildcard(convertRuntimeQuery(t, `index=gradethis | convert ctime(a*) | convert num(b*)`), scope, first); err == nil {
		t.Fatal("cross-source evidence admitted")
	}
	changedScope := scope
	changedScope.TenantID = "another"
	if _, err = PrepareNextStatsWildcard(query, changedScope, first); err == nil {
		t.Fatal("cross-tenant evidence admitted")
	}
	preparation, err = PrepareNextStatsWildcard(query, scope, first)
	if err != nil {
		t.Fatal(err)
	}
	second, err := ValidateStatsWildcardInventory(preparation.Request(), []StatsWildcardInventoryMatch{{Ordinal: 0, Field: "b"}})
	if err != nil {
		t.Fatal(err)
	}
	changed := second.Clone()
	changed.request.previous = &second
	if _, ok := changed.AuthorityDigest(); ok {
		t.Fatal("reordered evidence admitted")
	}
	changed = second.Clone()
	changed.request.conversion = false
	if _, ok := changed.AuthorityDigest(); ok {
		t.Fatal("conversion evidence substituted for stats")
	}
	for _, name := range []string{"__os_private", "fields"} {
		if _, err := ValidateStatsWildcardInventory(preparation.Request(), []StatsWildcardInventoryMatch{{Ordinal: 0, Field: name}}); err == nil {
			t.Fatalf("reserved field admitted: %s", name)
		}
	}
}

func TestConvertRuntimeDiscoveryStageLimit(t *testing.T) {
	for _, count := range []int{MaximumRuntimeWildcardStages, MaximumRuntimeWildcardStages + 1} {
		query := convertRuntimeQuery(t, `index=gradethis`+strings.Repeat(` | convert num(value*)`, count))
		scope := testScope([]string{"gradethis"}, nil)
		preparation, err := PrepareStatsWildcard(query, scope)
		if err != nil {
			t.Fatal(err)
		}
		for round := range MaximumRuntimeWildcardStages {
			evidence, err := ValidateStatsWildcardInventory(preparation.Request(), []StatsWildcardInventoryMatch{{Ordinal: 0, Field: "value"}})
			if err != nil {
				t.Fatal(err)
			}
			preparation, err = PrepareNextStatsWildcard(query, scope, evidence)
			if count > MaximumRuntimeWildcardStages && round == MaximumRuntimeWildcardStages-1 {
				if err == nil {
					t.Fatal("ninth discovery round admitted")
				}
				break
			}
			if err != nil {
				t.Fatal(err)
			}
		}
		if count == MaximumRuntimeWildcardStages && preparation.FullPlan() == nil {
			t.Fatal("eight stages did not complete")
		}
	}
}

func TestConvertRuntimeRetainedBytesChargeArrayCapacity(t *testing.T) {
	query := convertRuntimeQuery(t, `index=gradethis | convert num(source) AS copied num(cop*) none(secret*)`)
	preparation, err := PrepareStatsWildcard(query, testScope([]string{"gradethis"}, nil))
	if err != nil {
		t.Fatal(err)
	}
	request := preparation.Request()
	before, ok := request.RetainedBytes()
	if !ok {
		t.Fatal("invalid original request")
	}
	larger := request
	larger.exclusions = make([]string, len(request.exclusions), cap(request.exclusions)+64)
	copy(larger.exclusions, request.exclusions)
	larger.additions = make([]StatsWildcardInventoryMatch, len(request.additions), cap(request.additions)+64)
	copy(larger.additions, request.additions)
	after, ok := larger.RetainedBytes()
	if !ok || after <= before+64*16 {
		t.Fatalf("array capacity undercounted: before=%d after=%d valid=%t", before, after, ok)
	}
}

func TestConvertRuntimeAliasExpansionFailsBeforeRetainingUnboundedPairs(t *testing.T) {
	source := "index=gradethis | convert "
	for index := range 32 {
		source += "num(source) AS field_" + strconv.Itoa(index) + " "
	}
	source += strings.Repeat("num(field*) ", 32)
	_, err := PrepareStatsWildcard(convertRuntimeQuery(t, source), testScope([]string{"gradethis"}, nil))
	if err == nil {
		t.Fatal("known alias expansions exceeded command write budget")
	}
}
