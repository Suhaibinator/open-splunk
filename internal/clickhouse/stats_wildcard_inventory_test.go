package clickhouse

import (
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/plan"
	"github.com/Suhaibinator/open-splunk/internal/spl"
)

func TestCompileStatsWildcardInventoryIsSealedScopedBoundedAndNameOnly(t *testing.T) {
	t.Parallel()

	parsed, err := spl.Parse(`index=gradethis | where isnotnull(payload) | stats avg(met.a*tail*) AS value_**`)
	if err != nil {
		t.Fatal(err)
	}
	preparation, err := plan.PrepareStatsWildcard(parsed, testChartScope())
	if err != nil {
		t.Fatal(err)
	}
	prefix := preparation.Prefix()
	request := preparation.Request()
	compiled, err := (Compiler{}).CompileStatsWildcardInventory(prefix, request)
	if err != nil {
		t.Fatalf("CompileStatsWildcardInventory: %v", err)
	}
	if got, want := strings.Count(compiled.SQL, "?"), len(compiled.Args); got != want {
		t.Fatalf("placeholder count = %d, args = %d\n%s", got, want, compiled.SQL)
	}
	for _, required := range []string{
		`FROM "open_splunk"."events"`,
		`"tenant_id" = ?`,
		`arrayExists(wildcard_re -> match(`,
		`ORDER BY "__os_stats_wildcard_candidate_ordinal" ASC, "__os_stats_wildcard_candidate_field" ASC LIMIT 17`,
		`(?s:\A(?:met\.a.*tail.*)\z)`,
	} {
		if required == `(?s:\A(?:met\.a.*tail.*)\z)` {
			if !statsWildcardContainsStringArgument(compiled.Args, required) {
				t.Fatalf("compiled arguments omit anchored literal-safe RE2 %q: %#v", required, compiled.Args)
			}
			continue
		}
		if !strings.Contains(compiled.SQL, required) {
			t.Fatalf("compiled inventory SQL misses %q:\n%s", required, compiled.SQL)
		}
	}
	if strings.Count(compiled.SQL, `FROM "open_splunk"."events"`) != 1 {
		t.Fatalf("inventory physical source count != 1:\n%s", compiled.SQL)
	}
	known := statsWildcardKnownArgument(t, compiled.Args)
	if slices.Contains(known, "fields") {
		t.Fatalf("raw Dynamic payload container entered known inventory: %#v", known)
	}
	if !slices.Contains(known, "metadata") && slices.Contains(known, "met.a") {
		// This branch merely keeps the assertion tied to the actual pattern: no
		// unrelated known name should have survived compile-time prefiltering.
		t.Fatalf("unexpected known-field wildcard candidates: %#v", known)
	}
	clone, ok := compiled.CloneForExecution()
	if !ok || !compiled.EqualForExecution(clone) || !request.Equal(clone.Request()) {
		t.Fatal("compiler-produced wildcard inventory did not preserve sealed detached authority")
	}
	retained, retainedOK := compiled.RetainedBytes()
	cloneRetained, cloneRetainedOK := clone.RetainedBytes()
	if !retainedOK || !cloneRetainedOK || retained == 0 || cloneRetained == 0 ||
		cloneRetained > retained {
		t.Fatalf("retained bytes = (%d/%t, clone %d/%t)", retained, retainedOK, cloneRetained, cloneRetainedOK)
	}
	tenant, indexes, ok := clone.ReadScope()
	if !ok || tenant != "tenant-1" || !slices.Equal(indexes, []string{"gradethis"}) {
		t.Fatalf("inventory read scope = (%q, %#v, %t)", tenant, indexes, ok)
	}
	mutated := clone
	mutated.Args = slices.Clone(clone.Args)
	mutated.Args[len(mutated.Args)-1] = []string{`(?s:\A(?:forged)\z)`}
	if _, ok := mutated.CloneForExecution(); ok || compiled.EqualForExecution(mutated) {
		t.Fatal("mutated wildcard pattern arguments retained compiler authority")
	}
	if _, ok := mutated.RetainedBytes(); ok {
		t.Fatal("tampered wildcard inventory retained metadata accounting authority")
	}
}

func statsWildcardContainsStringArgument(arguments []any, expected string) bool {
	for _, argument := range arguments {
		switch value := argument.(type) {
		case string:
			if value == expected {
				return true
			}
		case []string:
			if slices.Contains(value, expected) {
				return true
			}
		}
	}
	return false
}

func TestCompileStatsWildcardInventoryChronologicalPrerequisiteRetainsCalculatedSidecars(t *testing.T) {
	t.Parallel()

	parsed, err := spl.Parse(`index=gradethis | eventstats earliest(payload) AS first_seen BY host | stats avg(first_*)`)
	if err != nil {
		t.Fatal(err)
	}
	preparation, err := plan.PrepareStatsWildcard(parsed, testChartScope())
	if err != nil {
		t.Fatal(err)
	}
	compiled, err := (Compiler{}).CompileStatsWildcardInventory(
		preparation.Prefix(), preparation.Request(),
	)
	if err != nil {
		t.Fatal(err)
	}
	for _, required := range []string{
		fieldSuggestionSidecarsCTE,
		fieldSuggestionRowsCTE,
		fieldSuggestionObservationsCTE,
		fieldSuggestionGroupsCTE,
		materializedCTESettingsSQL,
	} {
		if !strings.Contains(compiled.SQL, required) {
			t.Fatalf("prerequisite inventory omits %q:\n%s", required, compiled.SQL)
		}
	}
	if got := strings.Count(compiled.SQL, `FROM "open_splunk"."events"`); got != 1 {
		t.Fatalf("prerequisite inventory physical source count = %d:\n%s", got, compiled.SQL)
	}
}

func TestCompileStatsWildcardInventoryRejectsRequestForDifferentPrefix(t *testing.T) {
	t.Parallel()

	parsed, err := spl.Parse(`index=gradethis | stats sum(pay*)`)
	if err != nil {
		t.Fatal(err)
	}
	preparation, err := plan.PrepareStatsWildcard(parsed, testChartScope())
	if err != nil {
		t.Fatal(err)
	}
	other := buildPlan(t, `index=gradethis | where host="other"`)
	if _, err := (Compiler{}).CompileStatsWildcardInventory(
		other, preparation.Request(),
	); err == nil {
		t.Fatal("CompileStatsWildcardInventory accepted a request for a different prefix")
	}
}

func statsWildcardKnownArgument(t *testing.T, arguments []any) []string {
	t.Helper()
	for index, argument := range arguments {
		ordinals, ok := argument.([]uint8)
		if !ok || !reflect.DeepEqual(ordinals, []uint8{0}) || index == 0 {
			continue
		}
		known, ok := arguments[index-1].([]string)
		if ok {
			return known
		}
	}
	t.Fatalf("known-name argument was not found: %#v", arguments)
	return nil
}
