package clickhouse

import (
	"fmt"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/plan"
)

func TestCompileTimechartPercentileUsesOneScopedScanAndBoundedMaterializedState(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(
		t,
		`index=gradethis message="Request metrics" | timechart span=5m p95(duration) AS p95_duration`,
	)
	if !slices.Equal(compiled.OutputFields, []string{"_time", "p95_duration"}) {
		t.Fatalf("public fields = %v, want _time/p95_duration", compiled.OutputFields)
	}
	if compiled.Timechart == nil ||
		compiled.Timechart.Mode != TimechartModeFixedValue ||
		compiled.Timechart.ValueKind != TimechartValueKindPercentile ||
		compiled.Timechart.ValueField != "p95_duration" ||
		compiled.Timechart.FirstBucket != time.Date(2026, 7, 21, 0, 0, 0, 0, time.UTC) ||
		compiled.Timechart.Span != 5*time.Minute ||
		compiled.Timechart.BucketCount != 288 ||
		compiled.Timechart.MaxSeries != 1 ||
		compiled.Timechart.MaxLabelBytes != 0 {
		t.Fatalf("compiled percentile timechart metadata = %#v", compiled.Timechart)
	}

	for _, required := range []string{
		`"__os_timechart_source" AS (`,
		`"__os_timechart_value_groups" AS MATERIALIZED (`,
		`AS "__os_tc_measure_values"`,
		`quantilesGKOrNullArray(100, 0.95)("__os_tc_measure_values")`,
		`arrayElementOrNull(`,
		`"__os_timechart_input_presence" AS (SELECT toUInt8(count() > 0)`,
		`FROM "__os_timechart_value_groups"`,
		`FROM numbers(?)`,
		`AS "` + TimechartOrdinalColumn + `"`,
		`AS "` + TimechartValueColumn + `"`,
		`AS "` + TimechartInputPresentColumn + `"`,
		materializedCTESettingsSQL,
	} {
		if !strings.Contains(compiled.SQL, required) {
			t.Fatalf("percentile timechart SQL missing %q:\n%s", required, compiled.SQL)
		}
	}
	if strings.Contains(compiled.SQL, `"__os_timechart_source" AS MATERIALIZED (`) {
		t.Fatalf("single-consumer event source is unnecessarily materialized:\n%s", compiled.SQL)
	}
	if got := strings.Count(compiled.SQL, `FROM "open_splunk"."events"`); got != 1 {
		t.Fatalf("scoped storage scan occurs %d times, want once:\n%s", got, compiled.SQL)
	}
	if got := strings.Count(compiled.SQL, `FROM "__os_timechart_source"`); got != 1 {
		t.Fatalf("materialized event source is traversed %d times, want once:\n%s", got, compiled.SQL)
	}
	if got := strings.Count(compiled.SQL, "quantilesGKOrNullArray("); got != 1 {
		t.Fatalf("physical GK states = %d, want one per grouped query:\n%s", got, compiled.SQL)
	}
	if strings.Contains(strings.ToUpper(compiled.SQL), "ARRAY JOIN") {
		t.Fatalf("percentile timechart expanded multivalue rows:\n%s", compiled.SQL)
	}
	if got, want := strings.Count(compiled.SQL, "?"), len(compiled.Args); got != want {
		t.Fatalf("placeholder count = %d, args = %d\nSQL: %s\nargs: %#v", got, want, compiled.SQL, compiled.Args)
	}
	if got := compiled.Args[0]; got != "duration" {
		t.Fatalf("dynamic exact-presence argument = %#v, want duration before nested scan", got)
	}
	spanNanoseconds := int64(5 * time.Minute)
	wantTail := []any{spanNanoseconds, spanNanoseconds, int64(5_948_640), uint64(288)}
	if got := compiled.Args[len(compiled.Args)-4:]; !reflect.DeepEqual(got, wantTail) {
		t.Fatalf("grid arguments = %#v, want %#v", got, wantTail)
	}
}

func TestCompileSplitTimechartPercentileUsesMergeableGKStates(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(
		t,
		`index=gradethis message="Request metrics" | timechart span=5m p95(duration) BY service`,
	)
	if len(compiled.OutputFields) != 1 || compiled.OutputFields[0] != "_time" ||
		compiled.Timechart == nil ||
		compiled.Timechart.Mode != TimechartModeRuntimeWideValue ||
		compiled.Timechart.ValueKind != TimechartValueKindPercentile ||
		compiled.Timechart.ValueField != "" ||
		compiled.Timechart.MaxSeries != 12 ||
		compiled.Timechart.MaxLabelBytes != MaximumTimechartLabelBytes {
		t.Fatalf("compiled split percentile timechart = fields %v metadata %#v", compiled.OutputFields, compiled.Timechart)
	}

	for _, required := range []string{
		`"__os_timechart_source" AS (`,
		`AS "__os_tc_measure_values"`,
		`"__os_timechart_numeric_groups" AS MATERIALIZED (`,
		`quantilesGKOrNullArrayState(100, 0.95)(if("__os_tc_kind" IN (0, 1), "__os_tc_measure_values", CAST([], 'Array(Float64)'))) AS "__os_tc_percentile_state"`,
		`sum(ifNull(arrayElementOrNull(finalizeAggregation("__os_tc_percentile_state"), 1), toFloat64(0))) AS "__os_tc_score"`,
		`multiIf(isNaN("__os_tc_score"), toUInt8(0), isInfinite("__os_tc_score") AND "__os_tc_score" < 0, toUInt8(1), isInfinite("__os_tc_score"), toUInt8(3), toUInt8(2)) DESC`,
		`if(isFinite("__os_tc_score"), "__os_tc_score", toFloat64(0)) DESC, "__os_tc_label" ASC`,
		`quantilesGKOrNullArrayMerge(100, 0.95)("__os_tc_percentile_state") AS "__os_tc_percentile_values"`,
		`arrayElementOrNull("__os_tc_percentile_values", 1) AS "__os_tc_measure_value"`,
		`CAST('1:' AS String)`,
		`CAST('2:' AS String)`,
		`"__os_timechart_validation" AS (SELECT toUInt8(sumIf("__os_tc_count", "__os_tc_kind" = 3) > 0)`,
		`if("__os_timechart_grid"."__os_timechart_ordinal" = 0, "__os_timechart_domain".names, CAST([], 'Array(String)')) AS "` + TimechartNamesColumn + `"`,
		`AS "` + TimechartValuesColumn + `"`,
		`AS "` + TimechartValuePresentColumn + `"`,
		`AS "` + TimechartInvalidColumn + `"`,
		materializedCTESettingsSQL,
	} {
		if !strings.Contains(compiled.SQL, required) {
			t.Fatalf("split percentile timechart SQL missing %q:\n%s", required, compiled.SQL)
		}
	}
	if got := strings.Count(compiled.SQL, `FROM "open_splunk"."events"`); got != 1 {
		t.Fatalf("scoped storage scan occurs %d times, want once:\n%s", got, compiled.SQL)
	}
	if got := strings.Count(compiled.SQL, `quantilesGKOrNullArrayState(`); got != 1 {
		t.Fatalf("physical GK state constructors = %d, want one:\n%s", got, compiled.SQL)
	}
	if got := strings.Count(compiled.SQL, `quantilesGKOrNullArrayMerge(`); got != 1 {
		t.Fatalf("GK state merges = %d, want one shared OTHER/finalization path:\n%s", got, compiled.SQL)
	}
	for _, forbidden := range []string{
		"ARRAY JOIN",
		"sumCountArray(",
		`avg("__os_tc_measure_value")`,
		`avg(arrayElementOrNull(`,
	} {
		if strings.Contains(strings.ToUpper(compiled.SQL), strings.ToUpper(forbidden)) {
			t.Fatalf("split percentile timechart retained forbidden %q:\n%s", forbidden, compiled.SQL)
		}
	}
	if strings.Contains(
		compiled.SQL,
		`, "__os_timechart_domain".names AS "`+TimechartNamesColumn+`"`,
	) {
		t.Fatalf("split percentile timechart repeats runtime names on every bucket:\n%s", compiled.SQL)
	}
	if got, want := strings.Count(compiled.SQL, "?"), len(compiled.Args); got != want {
		t.Fatalf("placeholder count = %d, args = %d\nSQL: %s\nargs: %#v", got, want, compiled.SQL, compiled.Args)
	}
}

func TestCompileSplitTimechartPercentileCanonicalLevels(t *testing.T) {
	t.Parallel()

	for percentile, literal := range map[uint8]string{
		1: "0.01", 9: "0.09", 10: "0.1", 50: "0.5", 95: "0.95", 99: "0.99",
	} {
		percentile, literal := percentile, literal
		t.Run(fmt.Sprintf("p%d", percentile), func(t *testing.T) {
			t.Parallel()

			compiled := compileSPL(
				t,
				fmt.Sprintf(`index=gradethis | timechart span=5m perc%d(duration) BY service`, percentile),
			)
			for _, function := range []string{
				"quantilesGKOrNullArrayState(100, " + literal + ")",
				"quantilesGKOrNullArrayMerge(100, " + literal + ")",
			} {
				if !strings.Contains(compiled.SQL, function) {
					t.Fatalf("canonical split percentile function %s is missing:\n%s", function, compiled.SQL)
				}
			}
		})
	}
}

func TestCompileTimechartPercentileRevalidatesForgedMeasureAndOutput(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name    string
		corrupt func(*plan.Query, *plan.Timechart)
		want    string
	}{
		{
			name: "unsupported aggregate",
			corrupt: func(_ *plan.Query, operator *plan.Timechart) {
				operator.Measure.Function = plan.AggregateFunctionMinimum
			},
			want: "aggregate function is unsupported",
		},
		{
			name: "zero percentile",
			corrupt: func(_ *plan.Query, operator *plan.Timechart) {
				operator.Measure.Percentile = 0
			},
			want: "percentile must be from 1 through 99",
		},
		{
			name: "percentile above range",
			corrupt: func(_ *plan.Query, operator *plan.Timechart) {
				operator.Measure.Percentile = 100
			},
			want: "percentile must be from 1 through 99",
		},
		{
			name: "missing input",
			corrupt: func(_ *plan.Query, operator *plan.Timechart) {
				operator.Measure.Input = plan.FieldRef{}
			},
			want: "input field",
		},
		{
			name: "noncanonical input path",
			corrupt: func(_ *plan.Query, operator *plan.Timechart) {
				operator.Measure.Input.Path = nil
			},
			want: "input",
		},
		{
			name: "aggregate input and split field agree",
			corrupt: func(query *plan.Query, operator *plan.Timechart) {
				operator.Split = &plan.TimechartSplit{
					Field:        operator.Measure.Input,
					SeriesLimit:  10,
					IncludeNull:  true,
					IncludeOther: true,
					NullLabel:    "NULL",
					OtherLabel:   "OTHER",
				}
				query.OutputFields = nil
				query.DynamicOutput = &plan.DynamicSeriesOutput{
					FixedFields: []string{"_time"},
					MaxSeries:   12,
				}
			},
			want: "aggregate input and split field must differ",
		},
		{
			name: "static output disagrees with measure",
			corrupt: func(query *plan.Query, _ *plan.Timechart) {
				query.OutputFields[1] = "other"
			},
			want: "fixed value output contract is invalid",
		},
		{
			name: "dynamic descriptor",
			corrupt: func(query *plan.Query, _ *plan.Timechart) {
				query.DynamicOutput = &plan.DynamicSeriesOutput{
					FixedFields: []string{"_time"},
					MaxSeries:   12,
				}
			},
			want: "fixed value output contract is invalid",
		},
		{
			name: "time output collision",
			corrupt: func(query *plan.Query, operator *plan.Timechart) {
				operator.Measure.Output = "_time"
				query.OutputFields = []string{"_time", "_time"}
			},
			want: "field aggregate output contract is invalid",
		},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			logical := buildPlan(
				t,
				`index=gradethis | timechart span=5m p95(duration) AS p95_duration`,
			)
			operator := logical.Operators[len(logical.Operators)-1].(*plan.Timechart)
			test.corrupt(logical, operator)
			if _, err := (Compiler{}).Compile(logical); err == nil ||
				!strings.Contains(err.Error(), test.want) {
				t.Fatalf("Compile() error = %v, want %q", err, test.want)
			}
		})
	}
}

func TestCompileTimechartCountMeasureContractRemainsStrict(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name    string
		corrupt func(*plan.Timechart)
	}{
		{
			name: "input metadata",
			corrupt: func(operator *plan.Timechart) {
				operator.Measure.Input = plan.FieldRef{Name: "duration", Path: []string{"duration"}}
			},
		},
		{
			name: "percentile metadata",
			corrupt: func(operator *plan.Timechart) {
				operator.Measure.Percentile = 95
			},
		},
		{
			name: "renamed measure",
			corrupt: func(operator *plan.Timechart) {
				operator.Measure.Output = "events"
			},
		},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			logical := buildPlan(t, `index=gradethis | timechart span=5m count`)
			operator := logical.Operators[len(logical.Operators)-1].(*plan.Timechart)
			test.corrupt(operator)
			if _, err := (Compiler{}).Compile(logical); err == nil ||
				!strings.Contains(err.Error(), "count measure contract is invalid") {
				t.Fatalf("Compile() error = %v, want count measure rejection", err)
			}
		})
	}
}

func TestCompileTimechartPercentileCanonicalLevels(t *testing.T) {
	t.Parallel()

	for percentile, literal := range map[uint8]string{
		1: "0.01", 9: "0.09", 10: "0.1", 50: "0.5", 95: "0.95", 99: "0.99",
	} {
		percentile, literal := percentile, literal
		t.Run(fmt.Sprintf("p%d", percentile), func(t *testing.T) {
			t.Parallel()

			source := fmt.Sprintf(
				`index=gradethis | timechart span=5m perc%d(duration) AS result`,
				percentile,
			)
			compiled := compileSPL(t, source)
			if !strings.Contains(
				compiled.SQL,
				"quantilesGKOrNullArray(100, "+literal+")",
			) {
				t.Fatalf("canonical percentile level %s is missing:\n%s", literal, compiled.SQL)
			}
		})
	}
}
