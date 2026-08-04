package export

import (
	"context"
	"slices"
	"strings"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
)

// TestReexecutionSourceRoundTripsChartCountField proves an export rebuilds the
// occurrence-count pivot from stored SPL instead of accidentally falling back
// to the row-count chart protocol. Both forms intentionally share the public
// unsigned wide transport; the parsed aggregate is what must select the SQL.
func TestReexecutionSourceRoundTripsChartCountField(t *testing.T) {
	t.Parallel()

	searches, _, access := newReexecutionTestSearches()
	searches.job.SPL = `index=main | chart count(metric) OVER path BY series`
	schema := searchjobs.Schema{Columns: []searchjobs.Column{
		{Name: "path", Kind: searchjobs.ValueKindString},
		{Name: "api", Kind: searchjobs.ValueKindUnsigned},
		{Name: "NULL", Kind: searchjobs.ValueKindUnsigned},
	}}
	searches.pin.schema = schema
	executor := reexecutionTestExecutor(func(
		_ context.Context,
		query clickhouse.CompiledQuery,
		sink searchjobs.ResultSink,
	) error {
		if query.Chart == nil ||
			query.Chart.RowField != "path" ||
			query.Chart.ValueKind != clickhouse.ChartValueKindCount ||
			query.Chart.RowLimit != 10_000 ||
			query.Chart.MaxSeries != 12 ||
			!slices.Equal(query.OutputFields, []string{"path"}) {
			t.Fatalf(
				"re-executed chart count(field) metadata = %#v / %#v",
				query.Chart,
				query.OutputFields,
			)
		}
		for _, marker := range []string{
			`AS "__os_ch_measure_count"`,
			`sum(toUInt128("__os_ch_measure_count")) AS "__os_ch_occurrence_count"`,
			`toUInt64(sum(toUInt128("__os_ch_occurrence_count"))) AS "__os_ch_count"`,
		} {
			if !strings.Contains(query.SQL, marker) {
				t.Fatalf("re-executed chart count(field) SQL lost %q:\n%s", marker, query.SQL)
			}
		}
		if err := sink.SetSchema(schema); err != nil {
			return err
		}
		return sink.AddRow([]searchjobs.Value{
			searchjobs.StringValue("/api"),
			searchjobs.UnsignedValue(3),
			searchjobs.UnsignedValue(0),
		})
	})

	source := newReexecutionTestSource(t, searches, executor, nil)
	lease, err := source.AcquireResultsFor(context.Background(), access, searches.job.ID)
	if err != nil {
		t.Fatalf("AcquireResultsFor(chart count(field)): %v", err)
	}
	row, ok, err := lease.Next(context.Background())
	if err != nil || !ok || len(row.Values) != len(schema.Columns) {
		t.Fatalf("Next(chart count(field)) = (%#v, %t, %v)", row, ok, err)
	}
	if _, ok, err := lease.Next(context.Background()); err != nil || ok {
		t.Fatalf("terminal Next(chart count(field)) = ok %t err %v", ok, err)
	}
	if err := lease.Close(); err != nil {
		t.Fatalf("close chart count(field) re-execution: %v", err)
	}
}
