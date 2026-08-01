package clickhouse

import (
	"slices"
	"strings"
	"testing"
)

func TestCompileEventStatsSumResolvesInputBeforeAliasReplacement(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(
		t,
		`index=gradethis | eventstats sum(eventstats_value) AS eventstats_value | table event_id eventstats_value`,
	)
	if !slices.Equal(
		compiled.OutputFields,
		[]string{"event_id", "eventstats_value"},
	) {
		t.Fatalf("eventstats sum output fields = %#v", compiled.OutputFields)
	}
	measureAlias := eventStatsPrivateAlias(
		t,
		compiled.SQL,
		"__os_eventstats_measure_",
	)
	if !strings.Contains(compiled.SQL, `AS `+measureAlias) ||
		!strings.Contains(compiled.SQL, `sumOrNullArray(`+measureAlias+`)`) ||
		!strings.Contains(compiled.SQL, `AS "eventstats_value"`) {
		t.Fatalf("sum(field) alias replacement lost its upstream input:\n%s", compiled.SQL)
	}
	wantPrefix := []any{"eventstats_value"}
	if len(compiled.Args) < len(wantPrefix) ||
		!slices.Equal(compiled.Args[:len(wantPrefix)], wantPrefix) {
		t.Fatalf(
			"alias replacement input args = %#v, want prefix %#v",
			compiled.Args,
			wantPrefix,
		)
	}
}
