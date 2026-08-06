package export

import (
	"context"
	"slices"
	"strings"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
)

func TestReexecutionSourceRebuildsReplacingAndStackedStreamStatsChronologicalFromStoredSPL(t *testing.T) {
	t.Parallel()

	searches, _, access := newReexecutionTestSearches()
	searches.job.SPL = `index=main | table _time,status | streamstats current=false earliest(status) AS status | streamstats latest(status) AS last_seen | table status,last_seen`
	schema := searchjobs.Schema{Columns: []searchjobs.Column{
		{Name: "status", Kind: searchjobs.ValueKindMixed, Nullable: true},
		{Name: "last_seen", Kind: searchjobs.ValueKindMixed, Nullable: true},
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
		t.Fatalf("AcquireResultsFor(stacked streamstats earliest/latest): %v", err)
	}
	if _, ok, nextErr := lease.Next(context.Background()); ok || nextErr != nil {
		t.Fatalf("Next(stacked streamstats earliest/latest) = ok %t err %v", ok, nextErr)
	}
	if !slices.Equal(captured.OutputFields, []string{"status", "last_seen"}) {
		t.Fatalf("re-executed streamstats chronological fields = %v", captured.OutputFields)
	}
	for _, required := range []string{
		`argMinOrNullIf(`,
		`argMaxOrNullIf(`,
		`ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING`,
		`AS "status"`,
		`AS "last_seen"`,
		`CAST(NULL AS Dynamic)`,
		clickhouse.StreamStatsInputLimitMarker,
		clickhouse.UnsupportedStatsMeasureValueMarker,
	} {
		if !strings.Contains(captured.SQL, required) {
			t.Fatalf(
				"re-executed stacked streamstats earliest/latest SQL missing %q:\n%s",
				required,
				captured.SQL,
			)
		}
	}
	for _, aggregate := range []string{`argMinOrNullIf(`, `argMaxOrNullIf(`} {
		if got := strings.Count(captured.SQL, aggregate); got != 1 {
			t.Fatalf(
				"stacked streamstats chronological %s count = %d, want 1\n%s",
				aggregate,
				got,
				captured.SQL,
			)
		}
	}
	if err := lease.Close(); err != nil {
		t.Fatalf("close stacked streamstats earliest/latest re-execution: %v", err)
	}
}
