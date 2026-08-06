package export

import (
	"context"
	"slices"
	"strings"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
)

func TestReexecutionSourceRebuildsReplacingAndStackedStreamStatsMaximumFromStoredSPL(t *testing.T) {
	t.Parallel()

	searches, _, access := newReexecutionTestSearches()
	searches.job.SPL = `index=main | table status | streamstats current=false max(status) AS status | streamstats max(status) AS max_so_far | table status,max_so_far`
	schema := searchjobs.Schema{Columns: []searchjobs.Column{
		{Name: "status", Kind: searchjobs.ValueKindMixed, Nullable: true},
		{Name: "max_so_far", Kind: searchjobs.ValueKindMixed, Nullable: true},
	}}
	searches.pin.schema = schema
	var captured clickhouse.CompiledQuery
	executor := reexecutionTestExecutor(func(
		_ context.Context,
		query clickhouse.CompiledQuery,
		sink searchjobs.ResultSink,
	) error {
		captured = query
		return sink.SetSchema(schema)
	})
	source := newReexecutionTestSource(t, searches, executor, nil)
	lease, err := source.AcquireResultsFor(
		context.Background(),
		access,
		searches.job.ID,
	)
	if err != nil {
		t.Fatalf("AcquireResultsFor(stacked streamstats max(field)): %v", err)
	}
	if _, ok, nextErr := lease.Next(context.Background()); ok || nextErr != nil {
		t.Fatalf("Next(stacked streamstats max(field)) = ok %t err %v", ok, nextErr)
	}
	if !slices.Equal(captured.OutputFields, []string{"status", "max_so_far"}) {
		t.Fatalf("re-executed streamstats maximum fields = %v", captured.OutputFields)
	}
	for _, required := range []string{
		`argMaxOrNullIf(`,
		`ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING`,
		`AS "status"`,
		`AS "max_so_far"`,
		`CAST(NULL AS Dynamic)`,
		clickhouse.StreamStatsInputLimitMarker,
		clickhouse.UnsupportedStatsMeasureValueMarker,
	} {
		if !strings.Contains(captured.SQL, required) {
			t.Fatalf(
				"re-executed stacked streamstats max(field) SQL missing %q:\n%s",
				required,
				captured.SQL,
			)
		}
	}
	if got := strings.Count(captured.SQL, `argMaxOrNullIf(`); got != 2 {
		t.Fatalf("stacked streamstats maximum aggregate count = %d, want 2\n%s", got, captured.SQL)
	}
	if err := lease.Close(); err != nil {
		t.Fatalf("close stacked streamstats max(field) re-execution: %v", err)
	}
}
