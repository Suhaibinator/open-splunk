package clickhouse

import (
	"reflect"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/plan"
)

func TestStatsDistributionArrayAggregateSQL(t *testing.T) {
	t.Parallel()

	const input = `"__os_distribution_values"`
	tests := []struct {
		name       string
		function   plan.AggregateFunction
		percentile uint8
		want       statsDistributionAggregateLowering
	}{
		{
			name:       "exact percentile",
			function:   plan.AggregateFunctionExactPercentile,
			percentile: 95,
			want: statsDistributionAggregateLowering{
				SQL:           `arrayElementOrNull(arraySort(groupArrayArray("__os_distribution_values")), toUInt64(ceil(toFloat64(countArray("__os_distribution_values")) * 0.95)))`,
				Result:        statsDistributionResultNullableFloat64,
				StateBound:    statsDistributionStateBoundLinearValues,
				Exact:         true,
				Deterministic: true,
			},
		},
		{
			name:       "upper percentile",
			function:   plan.AggregateFunctionUpperPercentile,
			percentile: 95,
			want: statsDistributionAggregateLowering{
				SQL:           `arrayElementOrNull(quantilesGKOrNullArray(100, 0.95, 0.96)("__os_distribution_values"), if(uniqCombined64Array(17)("__os_distribution_values") <= 1000, 1, 2))`,
				Result:        statsDistributionResultNullableFloat64,
				StateBound:    statsDistributionStateBoundConstant,
				Exact:         false,
				Deterministic: false,
			},
		},
		{
			name:     "median",
			function: plan.AggregateFunctionMedian,
			want: statsDistributionAggregateLowering{
				SQL:           `arrayElementOrNull(quantilesGKOrNullArray(100, 0.5)("__os_distribution_values"), 1)`,
				Result:        statsDistributionResultNullableFloat64,
				StateBound:    statsDistributionStateBoundConstant,
				Exact:         false,
				Deterministic: false,
			},
		},
		{
			name:     "estimated distinct count",
			function: plan.AggregateFunctionEstimatedDistinctCount,
			want: statsDistributionAggregateLowering{
				SQL:           `uniqCombined64Array(17)("__os_distribution_values")`,
				Result:        statsDistributionResultUInt64,
				StateBound:    statsDistributionStateBoundConstant,
				Exact:         false,
				Deterministic: true,
			},
		},
		{
			name:     "estimated distinct count error",
			function: plan.AggregateFunctionEstimatedDistinctCountError,
			want: statsDistributionAggregateLowering{
				SQL:           `if(uniqCombined64Array(17)("__os_distribution_values") < 1000, toFloat64(0), toFloat64(0.002872621298570349))`,
				Result:        statsDistributionResultFloat64,
				StateBound:    statsDistributionStateBoundConstant,
				Exact:         false,
				Deterministic: true,
			},
		},
		{
			name:     "mode",
			function: plan.AggregateFunctionMode,
			want: statsDistributionAggregateLowering{
				SQL:           `arrayElementOrNull(tupleElement(sumMap("__os_distribution_values", arrayMap(_ -> toUInt64(1), "__os_distribution_values")), 1), indexOf(tupleElement(sumMap("__os_distribution_values", arrayMap(_ -> toUInt64(1), "__os_distribution_values")), 2), arrayMax(tupleElement(sumMap("__os_distribution_values", arrayMap(_ -> toUInt64(1), "__os_distribution_values")), 2))))`,
				Result:        statsDistributionResultNullableString,
				StateBound:    statsDistributionStateBoundLinearDistinct,
				Exact:         true,
				Deterministic: true,
			},
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			got, ok := statsDistributionArrayAggregateSQL(
				test.function,
				test.percentile,
				input,
			)
			if !ok {
				t.Fatalf("statsDistributionArrayAggregateSQL(%v) rejected a supported function", test.function)
			}
			if !reflect.DeepEqual(got, test.want) {
				t.Fatalf("statsDistributionArrayAggregateSQL(%v) =\n%#v\nwant\n%#v", test.function, got, test.want)
			}
		})
	}
}

func TestStatsDistributionArrayAggregateSQLUpperPercentileBoundary(t *testing.T) {
	t.Parallel()

	got, ok := statsDistributionArrayAggregateSQL(
		plan.AggregateFunctionUpperPercentile,
		99,
		`"values"`,
	)
	if !ok {
		t.Fatal("upperperc99 was rejected")
	}
	want := `arrayElementOrNull(quantilesGKOrNullArray(100, 0.99, 1)("values"), if(uniqCombined64Array(17)("values") <= 1000, 1, 2))`
	if got.SQL != want {
		t.Fatalf("upperperc99 SQL = %q, want %q", got.SQL, want)
	}
}

func TestStatsDistributionArrayAggregateSQLRejectsInvalidMetadata(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		function   plan.AggregateFunction
		percentile uint8
		input      string
	}{
		{"invalid function", plan.AggregateFunctionInvalid, 0, `"values"`},
		{"unknown function", plan.AggregateFunction(255), 0, `"values"`},
		{"unrelated function", plan.AggregateFunctionSum, 0, `"values"`},
		{"exact percentile zero", plan.AggregateFunctionExactPercentile, 0, `"values"`},
		{"exact percentile 100", plan.AggregateFunctionExactPercentile, 100, `"values"`},
		{"upper percentile zero", plan.AggregateFunctionUpperPercentile, 0, `"values"`},
		{"upper percentile 100", plan.AggregateFunctionUpperPercentile, 100, `"values"`},
		{"median percentile metadata", plan.AggregateFunctionMedian, 50, `"values"`},
		{"estdc percentile metadata", plan.AggregateFunctionEstimatedDistinctCount, 50, `"values"`},
		{"estdc error percentile metadata", plan.AggregateFunctionEstimatedDistinctCountError, 50, `"values"`},
		{"mode percentile metadata", plan.AggregateFunctionMode, 50, `"values"`},
		{"empty SQL", plan.AggregateFunctionMedian, 0, ""},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			got, ok := statsDistributionArrayAggregateSQL(
				test.function,
				test.percentile,
				test.input,
			)
			if ok || got != (statsDistributionAggregateLowering{}) {
				t.Fatalf("statsDistributionArrayAggregateSQL(%v, %d, %q) = %#v, %v; want zero, false", test.function, test.percentile, test.input, got, ok)
			}
		})
	}
}
