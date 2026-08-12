package clickhouse

import "github.com/Suhaibinator/open-splunk/internal/plan"

// statsNumericArrayAggregateSQL lowers a numeric stats aggregate over an
// Array(Float64) expression. Every supported lowering has a Nullable(Float64)
// result: the OrNull combinator keeps an empty array (including a group where
// every source value was ineligible) distinct from a numeric zero.
//
// The variance and standard-deviation functions deliberately use ClickHouse's
// mergeable, numerically stable aggregate states instead of deriving moments
// from independent sums. inputSQL is compiler-owned SQL, not user input.
func statsNumericArrayAggregateSQL(
	function plan.AggregateFunction,
	inputSQL string,
) (string, bool) {
	if inputSQL == "" {
		return "", false
	}

	switch function {
	case plan.AggregateFunctionSum:
		return "sumOrNullArray(" + inputSQL + ")", true
	case plan.AggregateFunctionAverage:
		return "avgOrNullArray(" + inputSQL + ")", true
	case plan.AggregateFunctionRange:
		return "maxOrNullArray(" + inputSQL + ") - minOrNullArray(" + inputSQL + ")", true
	case plan.AggregateFunctionSumSquares:
		return "sumOrNullArray(arrayMap(value -> value * value, " + inputSQL + "))", true
	case plan.AggregateFunctionStandardDeviationSample:
		return sampleNumericArrayAggregateSQL("stddevSampStableOrNullArray", inputSQL), true
	case plan.AggregateFunctionStandardDeviationPopulation:
		return "stddevPopStableOrNullArray(" + inputSQL + ")", true
	case plan.AggregateFunctionVarianceSample:
		return sampleNumericArrayAggregateSQL("varSampStableOrNullArray", inputSQL), true
	case plan.AggregateFunctionVariancePopulation:
		return "varPopStableOrNullArray(" + inputSQL + ")", true
	default:
		return "", false
	}
}

// Splunk defines sample variance and standard deviation as zero for a
// singleton. ClickHouse returns NaN in that case, so retain its stable,
// mergeable moment state and use a mergeable count state to select zero only
// when exactly one eligible element was aggregated. For zero elements the
// stable OrNull aggregate remains NULL.
func sampleNumericArrayAggregateSQL(functionName, inputSQL string) string {
	return "if(countArray(" + inputSQL + ") = 1, " +
		"CAST(0 AS Nullable(Float64)), " + functionName + "(" + inputSQL + "))"
}
