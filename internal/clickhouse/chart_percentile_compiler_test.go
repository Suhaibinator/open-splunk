package clickhouse

import (
	"reflect"
	"slices"
	"strings"
	"testing"
)

func TestCompileChartPercentileUsesOneScopedScanAndMergeableGKStates(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name   string
		source string
		level  string
	}{
		{
			name:   "p95",
			source: `index=gradethis message="Request metrics" | chart p95(metric) OVER path BY service`,
			level:  "0.95",
		},
		{
			name:   "perc50",
			source: `index=gradethis message="Request metrics" | chart perc50(metric) BY path, service`,
			level:  "0.5",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			compiled := compileSPL(t, test.source)
			if !slices.Equal(compiled.OutputFields, []string{"path"}) ||
				compiled.Timechart != nil || compiled.Chart == nil {
				t.Fatalf(
					"percentile chart contract = fields %v chart %#v timechart %#v",
					compiled.OutputFields,
					compiled.Chart,
					compiled.Timechart,
				)
			}
			if compiled.Chart.RowField != "path" ||
				compiled.Chart.RowKind != ChartRowKindString ||
				compiled.Chart.RowDatabaseType != "String" ||
				compiled.Chart.RowLimit != 10_000 ||
				compiled.Chart.MaxSeries != 12 ||
				compiled.Chart.MaxLabelBytes != 256 ||
				compiled.Chart.ValueKind != ChartValueKindPercentile ||
				!compiled.Chart.ValueKind.Valid() {
				t.Fatalf("percentile chart bounds = %#v", compiled.Chart)
			}

			state := "quantilesGKOrNullArrayState(100, " + test.level + ")"
			merge := "quantilesGKOrNullArrayMerge(100, " + test.level + ")"
			for _, required := range []string{
				`"__os_chart_source" AS (`,
				`AS "__os_ch_measure_values"`,
				`"__os_chart_numeric_groups" AS MATERIALIZED`,
				state,
				`GROUP BY "__os_ch_row", "__os_ch_row_eligible", "__os_ch_kind", "__os_ch_label"`,
				`finalizeAggregation(`,
				`sum(ifNull(arrayElementOrNull(`,
				merge,
				`arrayElementOrNull(`,
				`AS "` + ChartValuesColumn + `"`,
				`AS "` + ChartValuePresentColumn + `"`,
				`AS "` + ChartInvalidColumn + `"`,
				`CAST([], 'Array(Float64)') AS "` + ChartValuesColumn + `"`,
				`CAST([], 'Array(UInt8)') AS "` + ChartValuePresentColumn + `"`,
				`ORDER BY "` + ChartInvalidColumn + `" DESC, "` + ChartOrdinalColumn + `" ASC`,
				materializedCTESettingsSQL,
			} {
				if !strings.Contains(compiled.SQL, required) {
					t.Fatalf("percentile chart SQL missing %q:\n%s", required, compiled.SQL)
				}
			}
			if got := strings.Count(compiled.SQL, state); got != 1 {
				t.Fatalf("percentile chart state constructors = %d, want one:\n%s", got, compiled.SQL)
			}
			if got := strings.Count(compiled.SQL, merge); got != 1 {
				t.Fatalf("percentile chart state merges = %d, want one:\n%s", got, compiled.SQL)
			}
			if got := strings.Count(compiled.SQL, `FROM "open_splunk"."events"`); got != 1 {
				t.Fatalf("percentile chart scoped storage scans = %d, want one:\n%s", got, compiled.SQL)
			}
			if got := strings.Count(compiled.SQL, `FROM "__os_chart_canonicalized"`); got != 1 {
				t.Fatalf("percentile chart scoped relation consumers = %d, want one:\n%s", got, compiled.SQL)
			}
			upperSQL := strings.ToUpper(compiled.SQL)
			for _, forbidden := range []string{
				"ARRAY JOIN",
				"SUMCOUNTARRAY(",
				`AVG("__OS_CH_MEASURE_VALUE")`,
				`AVG(ARRAYELEMENTORNULL(`,
			} {
				if strings.Contains(upperSQL, forbidden) {
					t.Fatalf("percentile chart retained forbidden %q:\n%s", forbidden, compiled.SQL)
				}
			}
			if got, want := strings.Count(compiled.SQL, "?"), len(compiled.Args); got != want {
				t.Fatalf("placeholder count = %d, args = %d\nSQL: %s\nargs: %#v", got, want, compiled.SQL, compiled.Args)
			}
			if len(compiled.Args) < 2 ||
				!reflect.DeepEqual(compiled.Args[:2], []any{"path", "metric"}) {
				t.Fatalf(
					"dynamic row/measure exact-presence arguments = %#v, want path/metric first",
					compiled.Args,
				)
			}
		})
	}
}
