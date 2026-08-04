package export

import (
	"context"
	"slices"
	"strings"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
)

func TestReexecutionSourceRebuildsStreamStatsMinimumFromStoredSPL(t *testing.T) {
	t.Parallel()

	searches, _, access := newReexecutionTestSearches()
	searches.job.SPL = `index=main | table status | streamstats current=false min(status) AS prior_min`
	schema := searchjobs.Schema{Columns: []searchjobs.Column{
		{Name: "status", Kind: searchjobs.ValueKindMixed, Nullable: true},
		{Name: "prior_min", Kind: searchjobs.ValueKindMixed, Nullable: true},
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
		t.Fatalf("AcquireResultsFor(streamstats min(field)): %v", err)
	}
	if _, ok, nextErr := lease.Next(context.Background()); ok || nextErr != nil {
		t.Fatalf("Next(streamstats min(field)) = ok %t err %v", ok, nextErr)
	}
	if !slices.Equal(captured.OutputFields, []string{"status", "prior_min"}) {
		t.Fatalf("re-executed streamstats minimum fields = %v", captured.OutputFields)
	}
	for _, required := range []string{
		`argMinOrNullIf(`,
		`ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING`,
		`AS "prior_min"`,
		`CAST(NULL AS Dynamic)`,
		clickhouse.StreamStatsInputLimitMarker,
		clickhouse.UnsupportedStatsMeasureValueMarker,
	} {
		if !strings.Contains(captured.SQL, required) {
			t.Fatalf(
				"re-executed streamstats min(field) SQL missing %q:\n%s",
				required,
				captured.SQL,
			)
		}
	}
	if err := lease.Close(); err != nil {
		t.Fatalf("close streamstats min(field) re-execution: %v", err)
	}
}
