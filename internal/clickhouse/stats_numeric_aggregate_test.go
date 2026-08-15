package clickhouse

import (
	"strings"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/plan"
)

func TestStatsNumericArrayAggregateSQL(t *testing.T) {
	t.Parallel()

	const inputSQL = `"__os_numeric_values"`
	tests := []struct {
		name     string
		function plan.AggregateFunction
		want     string
	}{
		{"sum", plan.AggregateFunctionSum, `sumOrNullArray("__os_numeric_values")`},
		{"average", plan.AggregateFunctionAverage, `avgOrNullArray("__os_numeric_values")`},
		{"range", plan.AggregateFunctionRange, `maxOrNullArray("__os_numeric_values") - minOrNullArray("__os_numeric_values")`},
		{"sum squares", plan.AggregateFunctionSumSquares, `sumOrNullArray(arrayMap(value -> value * value, "__os_numeric_values"))`},
		{"sample standard deviation", plan.AggregateFunctionStandardDeviationSample, `if(countArray("__os_numeric_values") = 1, CAST(0 AS Nullable(Float64)), stddevSampStableOrNullArray("__os_numeric_values"))`},
		{"population standard deviation", plan.AggregateFunctionStandardDeviationPopulation, `stddevPopStableOrNullArray("__os_numeric_values")`},
		{"sample variance", plan.AggregateFunctionVarianceSample, `if(countArray("__os_numeric_values") = 1, CAST(0 AS Nullable(Float64)), varSampStableOrNullArray("__os_numeric_values"))`},
		{"population variance", plan.AggregateFunctionVariancePopulation, `varPopStableOrNullArray("__os_numeric_values")`},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			got, ok := statsNumericArrayAggregateSQL(test.function, inputSQL)
			if !ok {
				t.Fatalf("statsNumericArrayAggregateSQL(%v) rejected a supported function", test.function)
			}
			if got != test.want {
				t.Fatalf("statsNumericArrayAggregateSQL(%v) = %q, want %q", test.function, got, test.want)
			}
		})
	}
}

func TestStatsNumericArrayAggregateSQLUsesNullableAggregates(t *testing.T) {
	t.Parallel()

	functions := []plan.AggregateFunction{
		plan.AggregateFunctionSum,
		plan.AggregateFunctionAverage,
		plan.AggregateFunctionRange,
		plan.AggregateFunctionSumSquares,
		plan.AggregateFunctionStandardDeviationSample,
		plan.AggregateFunctionStandardDeviationPopulation,
		plan.AggregateFunctionVarianceSample,
		plan.AggregateFunctionVariancePopulation,
	}
	for _, function := range functions {
		got, ok := statsNumericArrayAggregateSQL(function, `CAST([], 'Array(Float64)')`)
		if !ok {
			t.Fatalf("statsNumericArrayAggregateSQL(%v) rejected a supported function", function)
		}
		if !strings.Contains(got, "OrNullArray(") {
			t.Errorf("statsNumericArrayAggregateSQL(%v) = %q; empty/all-ineligible input would not be nullable", function, got)
		}
	}
}

func TestStatsNumericArrayAggregateSQLSingletonContracts(t *testing.T) {
	t.Parallel()

	const singleton = `CAST([3.0], 'Array(Float64)')`
	tests := []struct {
		name     string
		function plan.AggregateFunction
		fragment string
	}{
		// max(x) - min(x) is zero for a singleton.
		{"range is zero", plan.AggregateFunctionRange, "maxOrNullArray("},
		// Squaring precedes aggregation, so a singleton produces x*x.
		{"sum squares squares its value", plan.AggregateFunctionSumSquares, "value * value"},
		// ClickHouse's population moments are zero for a singleton.
		{"population standard deviation is zero", plan.AggregateFunctionStandardDeviationPopulation, "stddevPopStableOrNullArray("},
		{"population variance is zero", plan.AggregateFunctionVariancePopulation, "varPopStableOrNullArray("},
		// Splunk returns zero for sample moments of a singleton. The count guard
		// overrides ClickHouse's native NaN without changing the empty-input NULL.
		{"sample standard deviation is zero", plan.AggregateFunctionStandardDeviationSample, "if(countArray("},
		{"sample variance is zero", plan.AggregateFunctionVarianceSample, "if(countArray("},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			got, ok := statsNumericArrayAggregateSQL(test.function, singleton)
			if !ok || !strings.Contains(got, test.fragment) {
				t.Fatalf("statsNumericArrayAggregateSQL(%v) = %q, %v; want fragment %q", test.function, got, ok, test.fragment)
			}
		})
	}
}

func TestStatsNumericArrayAggregateSQLRejectsUnsupportedInputs(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		function plan.AggregateFunction
		inputSQL string
	}{
		{"invalid function", plan.AggregateFunctionInvalid, `"__os_numeric_values"`},
		{"non-numeric aggregate", plan.AggregateFunctionCountRows, `"__os_numeric_values"`},
		{"unknown function", plan.AggregateFunction(255), `"__os_numeric_values"`},
		{"empty SQL", plan.AggregateFunctionSum, ""},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			if got, ok := statsNumericArrayAggregateSQL(test.function, test.inputSQL); ok || got != "" {
				t.Fatalf("statsNumericArrayAggregateSQL(%v, %q) = %q, %v; want empty, false", test.function, test.inputSQL, got, ok)
			}
		})
	}
}
