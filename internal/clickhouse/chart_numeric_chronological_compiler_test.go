package clickhouse

import (
	"strings"
	"testing"
)

// A deferred eventstats stage causes the terminal chart to be wrapped in the
// chronological validation envelope. That outer projection must preserve the
// numeric chart transport rather than falling back to count-chart columns.
func TestCompileNumericChartAfterEventStatsPreservesValueTransportEnvelope(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(
		t,
		`index=gradethis | eventstats count AS peers | chart avg(peers) OVER path BY level`,
	)
	if compiled.Chart == nil || compiled.Timechart != nil ||
		compiled.Chart.ValueKind != ChartValueKindAverage {
		t.Fatalf("numeric chart output contract = %#v", compiled)
	}
	wantProjection := `SELECT "__os_chart_ordinal", "__os_chart_row", "__os_chart_names", ` +
		`"__os_chart_values", "__os_chart_value_present", "__os_chart_invalid" FROM ` +
		`"__os_chronological_final_input_`
	if !strings.Contains(compiled.SQL, wantProjection) {
		t.Fatalf("numeric chart chronological envelope is missing %q:\n%s", wantProjection, compiled.SQL)
	}
	countProjection := `SELECT "__os_chart_ordinal", "__os_chart_row", "__os_chart_names", ` +
		`"__os_chart_counts", "__os_chart_invalid" FROM "__os_chronological_final_input_`
	if strings.Contains(compiled.SQL, countProjection) {
		t.Fatalf("numeric chart chronological envelope used count columns:\n%s", compiled.SQL)
	}
	if !strings.Contains(
		compiled.SQL,
		`ORDER BY "`+ChartInvalidColumn+`" DESC, "`+ChartOrdinalColumn+`" ASC`,
	) {
		t.Fatalf("numeric chart chronological envelope can reorder its invalid sentinel:\n%s", compiled.SQL)
	}
	if got := strings.Count(compiled.SQL, `FROM "open_splunk"."events"`); got != 1 {
		t.Fatalf("numeric chart after eventstats physical scans = %d, want 1:\n%s", got, compiled.SQL)
	}
	if got, want := strings.Count(compiled.SQL, "?"), len(compiled.Args); got != want {
		t.Fatalf("placeholder count = %d, args = %d\nSQL: %s\nargs: %#v", got, want, compiled.SQL, compiled.Args)
	}
}
