package clickhouse

import "github.com/Suhaibinator/open-splunk/internal/plan"

// numericArrayAggregateOverSQL lowers a numeric aggregate over an
// Array(Float64) expression, appending over to every aggregate call so the same
// lowering serves both the plain stats form (over == "") and the sparkline
// window form. Every supported lowering has a Nullable(Float64) result: the
// OrNull combinator keeps an empty array (including a group where every source
// value was ineligible) distinct from a numeric zero.
//
// The variance and standard-deviation functions deliberately use ClickHouse's
// mergeable, numerically stable aggregate states instead of deriving moments
// from independent sums. inputSQL is compiler-owned SQL, not user input.
func numericArrayAggregateOverSQL(
	function plan.AggregateFunction,
	inputSQL string,
	over string,
) (string, bool) {
	if inputSQL == "" {
		return "", false
	}

	switch function {
	case plan.AggregateFunctionSum:
		return "sumOrNullArray(" + inputSQL + ")" + over, true
	case plan.AggregateFunctionAverage:
		return "avgOrNullArray(" + inputSQL + ")" + over, true
	case plan.AggregateFunctionRange:
		return "maxOrNullArray(" + inputSQL + ")" + over + " - minOrNullArray(" + inputSQL + ")" + over, true
	case plan.AggregateFunctionSumSquares:
		return "sumOrNullArray(arrayMap(value -> value * value, " + inputSQL + "))" + over, true
	case plan.AggregateFunctionStandardDeviationSample:
		return sampleNumericArrayAggregateSQL("stddevSampStableOrNullArray", inputSQL, over), true
	case plan.AggregateFunctionStandardDeviationPopulation:
		return "stddevPopStableOrNullArray(" + inputSQL + ")" + over, true
	case plan.AggregateFunctionVarianceSample:
		return sampleNumericArrayAggregateSQL("varSampStableOrNullArray", inputSQL, over), true
	case plan.AggregateFunctionVariancePopulation:
		return "varPopStableOrNullArray(" + inputSQL + ")" + over, true
	default:
		return "", false
	}
}

// statsNumericArrayAggregateSQL lowers a numeric stats aggregate over an
// Array(Float64) expression without a window suffix.
func statsNumericArrayAggregateSQL(
	function plan.AggregateFunction,
	inputSQL string,
) (string, bool) {
	return numericArrayAggregateOverSQL(function, inputSQL, "")
}

// Splunk defines sample variance and standard deviation as zero for a
// singleton. ClickHouse returns NaN in that case, so retain its stable,
// mergeable moment state and use a mergeable count state to select zero only
// when exactly one eligible element was aggregated. For zero elements the
// stable OrNull aggregate remains NULL.
func sampleNumericArrayAggregateSQL(functionName, inputSQL, over string) string {
	return "if(countArray(" + inputSQL + ")" + over + " = 1, " +
		"CAST(0 AS Nullable(Float64)), " + functionName + "(" + inputSQL + ")" + over + ")"
}
