package export

import (
	"context"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
)

func TestReexecutionSourceRoundTripsFixedTimechartCountFieldMetadata(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name       string
		spl        string
		valueField string
	}{
		{
			name:       "canonical output",
			spl:        `index=main | timechart span=5m count(status)`,
			valueField: "count(status)",
		},
		{
			name:       "aliased output",
			spl:        `index=main | timechart span=5m count(status) AS populated`,
			valueField: "populated",
		},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			searches, _, access := newReexecutionTestSearches()
			searches.job.SPL = test.spl
			schema := searchjobs.Schema{Columns: []searchjobs.Column{
				{Name: "_time", Kind: searchjobs.ValueKindTime},
				{Name: test.valueField, Kind: searchjobs.ValueKindUnsigned},
			}}
			searches.pin.schema = schema
			bucket := searches.job.Earliest.Truncate(5 * time.Minute)
			executor := reexecutionTestExecutor(func(
				_ context.Context,
				query clickhouse.CompiledQuery,
				sink searchjobs.ResultSink,
			) error {
				if query.Timechart == nil ||
					query.Timechart.Mode != clickhouse.TimechartModeFixedFieldCount ||
					query.Timechart.ValueField != test.valueField ||
					query.Timechart.ValueKind != clickhouse.TimechartValueKindInvalid ||
					query.Timechart.MaxSeries != 1 || query.Timechart.MaxLabelBytes != 0 ||
					!slices.Equal(query.OutputFields, []string{"_time", test.valueField}) {
					t.Fatalf(
						"re-executed count(field) metadata = %#v / %#v",
						query.Timechart,
						query.OutputFields,
					)
				}
				for _, physicalField := range []string{
					clickhouse.TimechartOrdinalColumn,
					clickhouse.TimechartCountColumn,
					clickhouse.TimechartInputPresentColumn,
				} {
					if !strings.Contains(query.SQL, `"`+physicalField+`"`) {
						t.Fatalf(
							"re-executed count(field) SQL lost physical field %q:\n%s",
							physicalField,
							query.SQL,
						)
					}
				}
				if err := sink.SetSchema(schema); err != nil {
					return err
				}
				return sink.AddRow([]searchjobs.Value{
					searchjobs.TimeValue(bucket),
					searchjobs.UnsignedValue(3),
				})
			})

			source := newReexecutionTestSource(t, searches, executor, nil)
			lease, err := source.AcquireResultsFor(
				context.Background(),
				access,
				searches.job.ID,
			)
			if err != nil {
				t.Fatalf("AcquireResultsFor(count(field)): %v", err)
			}
			row, ok, err := lease.Next(context.Background())
			if err != nil || !ok || len(row.Values) != len(schema.Columns) {
				t.Fatalf("Next(count(field)) = (%#v, %t, %v)", row, ok, err)
			}
			if _, ok, err := lease.Next(context.Background()); err != nil || ok {
				t.Fatalf("terminal Next(count(field)) = ok %t err %v", ok, err)
			}
			if err := lease.Close(); err != nil {
				t.Fatalf("close count(field) re-execution: %v", err)
			}
		})
	}
}
