package queryexec

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/plan"
	"github.com/Suhaibinator/open-splunk/internal/spl"
)

// queryIntegrationTestStatsWildcardInventory runs inside the digest-pinned
// executor integration container. It proves ClickHouse's anchored RE2 syntax,
// bytewise transport order, Dynamic metadata inventory, overflow sentinel,
// and atomic poison handling.
func queryIntegrationTestStatsWildcardInventory(
	t *testing.T,
	ctx context.Context,
	executor *Executor,
	indexTime time.Time,
) {
	t.Helper()
	t.Run("stats wildcard runtime inventory", func(t *testing.T) {
		t.Run("matches are returned in exact bytewise field order", func(t *testing.T) {
			parsed, _, compiled := queryIntegrationCompileStatsWildcardInventory(
				t,
				"field-catalog-v1",
				indexTime,
				`index=field-catalog-v1 | stats count(*_id) AS id_*`,
			)
			expansion, err := executor.ExecuteStatsWildcardInventory(ctx, compiled)
			if err != nil {
				t.Fatalf("ExecuteStatsWildcardInventory: %v\nSQL: %s\nargs: %#v", err, compiled.SQL, compiled.Args)
			}
			logical, err := plan.BuildWithStatsWildcardExpansion(
				parsed,
				queryIntegrationStatsWildcardScope("field-catalog-v1", indexTime),
				expansion,
			)
			if err != nil {
				t.Fatal(err)
			}
			want := []string{"id_batch", "id_collector", "id_event", "id_span", "id_trace"}
			if !slices.Equal(logical.OutputFields, want) {
				t.Fatalf("expanded outputs = %#v, want exact bytewise order %#v", logical.OutputFields, want)
			}
		})

		t.Run("anchored multistar dynamic field and alias captures", func(t *testing.T) {
			parsed, preparation, compiled := queryIntegrationCompileStatsWildcardInventory(
				t,
				"field-catalog-v1",
				indexTime,
				`index=field-catalog-v1 | stats values(p*.c*) AS output_*_*`,
			)
			expansion, err := executor.ExecuteStatsWildcardInventory(ctx, compiled)
			if err != nil {
				t.Fatalf("ExecuteStatsWildcardInventory: %v\nSQL: %s\nargs: %#v", err, compiled.SQL, compiled.Args)
			}
			logical, err := plan.BuildWithStatsWildcardExpansion(
				parsed,
				queryIntegrationStatsWildcardScope("field-catalog-v1", indexTime),
				expansion,
			)
			if err != nil {
				t.Fatal(err)
			}
			if preparation.Request().MaximumPairs() != 17 ||
				!slices.Equal(logical.OutputFields, []string{"output_arent_hild"}) {
				t.Fatalf("expanded outputs = %#v", logical.OutputFields)
			}
		})

		t.Run("case-sensitive anchored match does not admit folded name", func(t *testing.T) {
			_, _, compiled := queryIntegrationCompileStatsWildcardInventory(
				t,
				"field-catalog-v1",
				indexTime,
				`index=field-catalog-v1 | stats count(STAT*)`,
			)
			if _, err := executor.ExecuteStatsWildcardInventory(ctx, compiled); err == nil {
				t.Fatal("uppercase pattern unexpectedly matched lowercase status")
			}
		})

		t.Run("overflow sentinel rejects instead of truncating", func(t *testing.T) {
			_, _, compiled := queryIntegrationCompileStatsWildcardInventory(
				t,
				"field-catalog-v1",
				indexTime,
				`index=field-catalog-v1 | stats avg(*)`,
			)
			if _, err := executor.ExecuteStatsWildcardInventory(ctx, compiled); err == nil {
				t.Fatal("over-width avg(*) inventory unexpectedly succeeded")
			}
		})

		t.Run("nonmatching poisoned metadata still rejects atomically", func(t *testing.T) {
			_, _, compiled := queryIntegrationCompileStatsWildcardInventory(
				t,
				"field-catalog-invalid",
				indexTime,
				`index=field-catalog-invalid | stats count(never*)`,
			)
			expansion, err := executor.ExecuteStatsWildcardInventory(ctx, compiled)
			if !errors.Is(err, ErrFieldMetadataUnavailable) || !expansion.IsZero() {
				t.Fatalf("poisoned inventory = (%#v, %v), want atomic metadata error", expansion, err)
			}
		})
	})
}

func queryIntegrationCompileStatsWildcardInventory(
	t *testing.T,
	indexName string,
	indexTime time.Time,
	source string,
) (*spl.Query, *plan.StatsWildcardPreparation, clickhouse.CompiledStatsWildcardInventory) {
	t.Helper()
	parsed, err := spl.Parse(source)
	if err != nil {
		t.Fatal(err)
	}
	preparation, err := plan.PrepareStatsWildcard(
		parsed,
		queryIntegrationStatsWildcardScope(indexName, indexTime),
	)
	if err != nil {
		t.Fatal(err)
	}
	compiled, err := (clickhouse.Compiler{}).CompileStatsWildcardInventory(
		preparation.Prefix(), preparation.Request(),
	)
	if err != nil {
		t.Fatal(err)
	}
	return parsed, preparation, compiled
}

func queryIntegrationStatsWildcardScope(indexName string, indexTime time.Time) plan.Scope {
	visibility := uint64(1)
	return plan.Scope{
		TenantID:          "tenant",
		AuthorizedIndexes: []string{indexName},
		RequestedIndexes:  []string{indexName},
		Earliest:          indexTime.Add(-time.Minute),
		Latest:            indexTime.Add(time.Minute),
		SearchStart:       indexTime,
		SearchTimezone:    "UTC",
		IndexTimeCutoff:   indexTime.Add(time.Second),
		VisibilityCutoff:  &visibility,
	}
}
