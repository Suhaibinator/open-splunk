package clickhouse

import (
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/plan"
)

func TestCompileTimechartSumAndAverageUseOneScopedScanAndSharedNumericNormalization(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		source        string
		output        string
		kind          TimechartValueKind
		valueFragment string
	}{
		{
			name:          "sum default output",
			source:        `index=gradethis message="Request metrics" | timechart span=5m sum(duration)`,
			output:        "sum(duration)",
			kind:          TimechartValueKindSum,
			valueFragment: `sumOrNullArray("__os_tc_measure_values")`,
		},
		{
			name:          "average explicit output",
			source:        `index=gradethis message="Request metrics" | timechart span=5m avg(duration) AS mean_duration`,
			output:        "mean_duration",
			kind:          TimechartValueKindAverage,
			valueFragment: `avgOrNullArray("__os_tc_measure_values")`,
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			compiled := compileSPL(t, test.source)
			if !slices.Equal(compiled.OutputFields, []string{"_time", test.output}) {
				t.Fatalf("public fields = %v, want _time/%s", compiled.OutputFields, test.output)
			}
			if compiled.Timechart == nil ||
				compiled.Timechart.Mode != TimechartModeFixedValue ||
				compiled.Timechart.ValueKind != test.kind ||
				compiled.Timechart.ValueField != test.output ||
				compiled.Timechart.FirstBucket != time.Date(2026, 7, 21, 0, 0, 0, 0, time.UTC) ||
				compiled.Timechart.Span != 5*time.Minute ||
				compiled.Timechart.BucketCount != 288 ||
				compiled.Timechart.MaxSeries != 1 ||
				compiled.Timechart.MaxLabelBytes != 0 {
				t.Fatalf("compiled numeric timechart metadata = %#v", compiled.Timechart)
			}

			for _, required := range []string{
				`"__os_timechart_source" AS (`,
				`"__os_timechart_value_groups" AS MATERIALIZED (`,
				`AS "__os_tc_measure_values"`,
				test.valueFragment,
				`AS "` + TimechartValueColumn + `"`,
				`"__os_timechart_input_presence" AS (SELECT toUInt8(count() > 0)`,
				`FROM "__os_timechart_value_groups"`,
				`FROM numbers(?)`,
				materializedCTESettingsSQL,
			} {
				if !strings.Contains(compiled.SQL, required) {
					t.Fatalf("numeric timechart SQL missing %q:\n%s", required, compiled.SQL)
				}
			}
			if strings.Contains(compiled.SQL, `"__os_timechart_source" AS MATERIALIZED (`) {
				t.Fatalf("single-consumer event source is unnecessarily materialized:\n%s", compiled.SQL)
			}
			if got := strings.Count(compiled.SQL, `FROM "open_splunk"."events"`); got != 1 {
				t.Fatalf("scoped storage scan occurs %d times, want once:\n%s", got, compiled.SQL)
			}
			if got := strings.Count(compiled.SQL, `FROM "__os_timechart_source"`); got != 1 {
				t.Fatalf("event source is traversed %d times, want once:\n%s", got, compiled.SQL)
			}
			if strings.Contains(strings.ToUpper(compiled.SQL), "ARRAY JOIN") {
				t.Fatalf("numeric timechart expanded multivalue rows:\n%s", compiled.SQL)
			}
			if strings.Contains(compiled.SQL, "arraySum(") ||
				strings.Contains(compiled.SQL, "sum(length(") {
				t.Fatalf("numeric timechart retained redundant array/count passes:\n%s", compiled.SQL)
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
		})
	}
}

func TestCompileTimechartNumericAggregateRevalidatesForgedMeasure(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name    string
		corrupt func(*plan.Query, *plan.Timechart)
		want    string
	}{
		{
			name: "percentile metadata",
			corrupt: func(_ *plan.Query, operator *plan.Timechart) {
				operator.Measure.Percentile = 95
			},
			want: "contains percentile metadata",
		},
		{
			name: "missing input",
			corrupt: func(_ *plan.Query, operator *plan.Timechart) {
				operator.Measure.Input = plan.FieldRef{}
			},
			want: "input field",
		},
		{
			name: "noncanonical input",
			corrupt: func(_ *plan.Query, operator *plan.Timechart) {
				operator.Measure.Input.Path = nil
			},
			want: "input",
		},
		{
			name: "split numeric aggregate",
			corrupt: func(_ *plan.Query, operator *plan.Timechart) {
				operator.Split = &plan.TimechartSplit{
					Field: plan.FieldRef{Name: "level", Path: []string{"level"}},
				}
			},
			want: "cannot split by a field",
		},
		{
			name: "static output disagreement",
			corrupt: func(query *plan.Query, _ *plan.Timechart) {
				query.OutputFields[1] = "other"
			},
			want: "fixed value output contract is invalid",
		},
		{
			name: "time output collision",
			corrupt: func(query *plan.Query, operator *plan.Timechart) {
				operator.Measure.Output = "_time"
				query.OutputFields = []string{"_time", "_time"}
			},
			want: "fixed value output contract is invalid",
		},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			logical := buildPlan(t, `index=gradethis | timechart span=5m sum(duration) AS total`)
			operator := logical.Operators[len(logical.Operators)-1].(*plan.Timechart)
			test.corrupt(logical, operator)
			if _, err := (Compiler{}).Compile(logical); err == nil ||
				!strings.Contains(err.Error(), test.want) {
				t.Fatalf("Compile() error = %v, want %q", err, test.want)
			}
		})
	}
}
