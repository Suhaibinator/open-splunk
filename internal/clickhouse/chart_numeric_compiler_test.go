package clickhouse

import (
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/plan"
)

const (
	testChartValueKindSum     ChartValueKind = 2
	testChartValueKindAverage ChartValueKind = 3
)

// TestCompileNumericChartUsesOneScopedScanAndMergeablePerCellStates pins the
// physical shape of sum/average chart before the transport is allowed to rely
// on it. In particular, averaging already-finalized series would make OTHER an
// average of averages; the numerator/count tuple has to survive until after
// omitted raw labels have been collapsed for each row.
func TestCompileNumericChartUsesOneScopedScanAndMergeablePerCellStates(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name      string
		source    string
		valueKind ChartValueKind
		score     string
		publish   string
		forbidden string
	}{
		{
			name:      "sum",
			source:    `index=gradethis message="Request metrics" | chart sum(duration) OVER path BY service`,
			valueKind: testChartValueKindSum,
			score:     `sum(if("__os_ch_denominator" = 0, toFloat64(0), "__os_ch_numerator"))`,
			publish:   `if("__os_ch_denominator" = 0, CAST(NULL AS Nullable(Float64)), "__os_ch_numerator")`,
			forbidden: `sum(if("__os_ch_denominator" = 0, toFloat64(0), "__os_ch_numerator" / toFloat64("__os_ch_denominator")))`,
		},
		{
			name:      "weighted average",
			source:    `index=gradethis message="Request metrics" | chart avg(duration) BY path, service`,
			valueKind: testChartValueKindAverage,
			score:     `sum(if("__os_ch_denominator" = 0, toFloat64(0), "__os_ch_numerator" / toFloat64("__os_ch_denominator")))`,
			publish:   `if("__os_ch_denominator" = 0, CAST(NULL AS Nullable(Float64)), "__os_ch_numerator" / toFloat64("__os_ch_denominator"))`,
			forbidden: `avg("__os_ch_measure_value")`,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			compiled := compileSPL(t, test.source)
			if !slices.Equal(compiled.OutputFields, []string{"path"}) ||
				compiled.Timechart != nil || compiled.Chart == nil {
				t.Fatalf(
					"numeric chart contract = fields %v chart %#v timechart %#v",
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
				compiled.Chart.MaxLabelBytes != 256 {
				t.Fatalf("numeric chart bounds = %#v", compiled.Chart)
			}
			requireCompiledChartValueKind(t, compiled.Chart, test.valueKind)

			for _, required := range []string{
				`"__os_chart_source" AS (`,
				`AS "__os_ch_measure_values"`,
				`"__os_chart_label_totals" AS MATERIALIZED`,
				`"__os_chart_numeric_groups" AS MATERIALIZED`,
				`sumCountArray("__os_ch_measure_values") AS "__os_ch_numeric_state"`,
				`GROUP BY "__os_ch_row", "__os_ch_row_eligible", "__os_ch_kind", "__os_ch_label"`,
				`FROM "__os_chart_numeric_groups" GROUP BY "__os_ch_kind", "__os_ch_label"`,
				`"__os_chart_numeric_scores" AS MATERIALIZED`,
				test.score,
				`multiIf(isNaN("__os_ch_score"), toUInt8(0), isInfinite("__os_ch_score") AND "__os_ch_score" < 0, toUInt8(1), isInfinite("__os_ch_score"), toUInt8(3), toUInt8(2)) DESC`,
				`if(isFinite("__os_ch_score"), "__os_ch_score", toFloat64(0)) DESC, "__os_ch_label" ASC LIMIT 10`,
				`"__os_chart_collapsed" AS (`,
				`sum("__os_ch_numerator") AS "__os_ch_numerator", sum("__os_ch_denominator") AS "__os_ch_denominator"`,
				`"__os_chart_finalized" AS (`,
				test.publish,
				`"__os_chart_row_domain" AS MATERIALIZED`,
				`AS "` + ChartOrdinalColumn + `"`,
				`AS "` + ChartRowColumn + `"`,
				`AS "` + ChartNamesColumn + `"`,
				`AS "__os_chart_values"`,
				`AS "__os_chart_value_present"`,
				`AS "` + ChartInvalidColumn + `"`,
				`CAST([], 'Array(Float64)') AS "` + ChartValuesColumn + `"`,
				`CAST([], 'Array(UInt8)') AS "` + ChartValuePresentColumn + `"`,
				`ORDER BY "` + ChartInvalidColumn + `" DESC, "` + ChartOrdinalColumn + `" ASC`,
				materializedCTESettingsSQL,
			} {
				if !strings.Contains(compiled.SQL, required) {
					t.Fatalf("numeric chart SQL missing %q:\n%s", required, compiled.SQL)
				}
			}
			if strings.Contains(compiled.SQL, test.forbidden) {
				t.Fatalf("numeric chart used invalid aggregate shape %q:\n%s", test.forbidden, compiled.SQL)
			}
			if got := strings.Count(compiled.SQL, `FROM "open_splunk"."events"`); got != 1 {
				t.Fatalf("numeric chart scoped storage scan occurs %d times, want once:\n%s", got, compiled.SQL)
			}
			if got := strings.Count(compiled.SQL, `FROM "__os_chart_canonicalized"`); got != 1 {
				t.Fatalf("numeric chart scoped relation has %d aggregate consumers, want one:\n%s", got, compiled.SQL)
			}
			if strings.Contains(strings.ToUpper(compiled.SQL), "ARRAY JOIN") {
				t.Fatalf("numeric chart expanded immediate members into rows:\n%s", compiled.SQL)
			}
			if strings.Contains(compiled.SQL, `WHERE length("__os_ch_measure_values")`) ||
				strings.Contains(compiled.SQL, `WHERE notEmpty("__os_ch_measure_values")`) {
				t.Fatalf("numeric chart dropped row/split groups with no eligible measure members:\n%s", compiled.SQL)
			}
			if got, want := strings.Count(compiled.SQL, "?"), len(compiled.Args); got != want {
				t.Fatalf("placeholder count = %d, args = %d\nSQL: %s\nargs: %#v", got, want, compiled.SQL, compiled.Args)
			}
			// service is a fixed physical column and therefore needs no sparse
			// presence marker. The dynamic row and measure paths do, and their
			// source-select placeholders must precede the nested relation's args.
			if len(compiled.Args) < 2 ||
				!reflect.DeepEqual(compiled.Args[:2], []any{"path", "duration"}) {
				t.Fatalf(
					"dynamic row/measure exact-presence arguments = %#v, want path/duration first",
					compiled.Args,
				)
			}
		})
	}
}

// TestCompileNumericChartKeepsSplitValidationAndSentinelsIndependent pins two
// easy-to-miss semantic boundaries: bad split labels remain fatal even when a
// row or measure is ineligible, and NULL never competes for the ten ordinary
// score-ranked slots. OTHER is formed by merging the omitted member states,
// not by adding or averaging already-finalized cells.
func TestCompileNumericChartKeepsSplitValidationAndSentinelsIndependent(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(t, `index=gradethis | chart avg(metric) OVER path BY service`)
	for _, required := range []string{
		`"__os_chart_kinded" AS (SELECT *, multiIf(`,
		`FROM "__os_chart_prepared")`,
		`"__os_chart_column_check" AS (`,
		`maxOrDefault("__os_ch_kind" = 3)`,
		`CROSS JOIN "__os_chart_column_check"`,
		`FROM "__os_chart_numeric_scores"`,
		`WHERE "__os_ch_row_eligible" != 0 AND "__os_ch_kind" = 1`,
		`"__os_ch_label" NOT IN (SELECT "__os_ch_label" FROM "__os_chart_numeric_scores")`,
		`multiIf("__os_ch_kind" = 1, '1:', "__os_ch_label" IN (SELECT "__os_ch_label" FROM "__os_chart_numeric_scores"), concat('0:', "__os_ch_label"), '2:')`,
	} {
		if !strings.Contains(compiled.SQL, required) {
			t.Fatalf("numeric chart validation/domain SQL missing %q:\n%s", required, compiled.SQL)
		}
	}
	if strings.Contains(compiled.SQL, `FROM "__os_chart_prepared" WHERE`) {
		t.Fatalf("numeric chart filtered split classification by another field's eligibility:\n%s", compiled.SQL)
	}
}

func TestCompileNumericChartRejectsLoweredRowLimit(t *testing.T) {
	t.Parallel()

	logical := buildPlan(t, `index=gradethis | chart sum(metric) OVER path BY service`)
	chart := logical.Operators[len(logical.Operators)-1].(*plan.Chart)
	chart.RowLimit = 9_999
	if _, err := (Compiler{}).Compile(logical); err == nil ||
		!strings.Contains(err.Error(), "bounded defaults are invalid") {
		t.Fatalf("Compile() error = %v, want exact row-bound rejection", err)
	}
}

func requireCompiledChartValueKind(t *testing.T, output *ChartOutput, want ChartValueKind) {
	t.Helper()
	if output == nil {
		t.Fatal("compiled chart output is nil")
	}
	if got := output.ValueKind; got != want {
		t.Fatalf("compiled chart value kind = %d, want %d", got, want)
	}
	if !output.ValueKind.Valid() {
		t.Fatalf("ChartValueKind(%d).Valid() = false, want true", want)
	}
}
