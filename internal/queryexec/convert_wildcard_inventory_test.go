package queryexec

import (
	"context"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/plan"
	"github.com/Suhaibinator/open-splunk/internal/spl"
)

func TestExecutorConvertWildcardEmptyAndLiteralAliasInventories(t *testing.T) {
	for _, source := range []string{`index=gradethis | convert num(absent*)`, `index=gradethis | convert num(bytes) AS "First Seen" num(First*)`} {
		compiled := validCompiledStatsWildcardInventory(t, source)
		matches := []plan.StatsWildcardInventoryMatch{}
		if len(compiled.Request().AdditionalMatches()) > 0 {
			matches = compiled.Request().AdditionalMatches()
		}
		rows := statsWildcardFakeRows(0, matches...)
		evidence, err := mustExecutor(t, &fakeQueryConnection{rows: rows}).ExecuteStatsWildcardInventory(context.Background(), compiled)
		if err != nil || evidence.IsZero() {
			t.Fatalf("inventory zero=%t err=%v", evidence.IsZero(), err)
		}
		parsed, err := spl.Parse(source)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := plan.BuildWithStatsWildcardExpansion(parsed, statsWildcardTestScope(), evidence); err != nil {
			t.Fatal(err)
		}
	}
}
