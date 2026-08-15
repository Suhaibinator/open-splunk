package plan

import (
	"strconv"

	"github.com/Suhaibinator/open-splunk/internal/spl"
)

// validChartContract recognizes the deliberately bounded two-axis pivot.
// Build constructs this shape, while analysis and backend compilation repeat
// the validation so forged logical plans cannot widen the result or change
// the aggregate policy.
func validChartContract(operator *Chart) bool {
	if operator == nil ||
		!validResolvedEventAggregateField(operator.Over) ||
		!validResolvedEventAggregateField(operator.SplitBy) ||
		operator.Over.Name == operator.SplitBy.Name ||
		operator.RowLimit != maxChartRows ||
		operator.SeriesLimit != chartSeriesLimit ||
		!operator.IncludeNull || !operator.IncludeOther ||
		operator.NullLabel != "NULL" || operator.OtherLabel != "OTHER" ||
		operator.Measure.Sparkline != nil ||
		operator.Measure.OutputLiteral ||
		operator.Measure.Predicate != nil ||
		operator.Measure.InputExpression != nil {
		return false
	}

	switch operator.Measure.Function {
	case AggregateFunctionCountRows:
		return operator.Measure.Percentile == 0 &&
			emptyAggregateField(operator.Measure.Input) &&
			operator.Measure.Output == "count"
	case AggregateFunctionCountValues:
		return spl.IsExactUnquotedFieldName(operator.Measure.Input.Name) &&
			validResolvedEventAggregateField(operator.Measure.Input) &&
			operator.Measure.Input.Name != operator.Over.Name &&
			operator.Measure.Percentile == 0 &&
			operator.Measure.Output == "count("+operator.Measure.Input.Name+")"
	case AggregateFunctionSum, AggregateFunctionAverage:
		if !spl.IsExactUnquotedFieldName(operator.Measure.Input.Name) ||
			!validResolvedEventAggregateField(operator.Measure.Input) ||
			operator.Measure.Input.Name == operator.Over.Name ||
			operator.Measure.Percentile != 0 {
			return false
		}
		canonicalName, _ := canonicalAggregateName(operator.Measure.Function, 0)
		return operator.Measure.Output == canonicalName+"("+operator.Measure.Input.Name+")"
	case AggregateFunctionPercentile:
		return spl.IsExactUnquotedFieldName(operator.Measure.Input.Name) &&
			validResolvedEventAggregateField(operator.Measure.Input) &&
			operator.Measure.Input.Name != operator.Over.Name &&
			operator.Measure.Percentile >= 1 && operator.Measure.Percentile <= 99 &&
			operator.Measure.Output == "perc"+
				strconv.Itoa(int(operator.Measure.Percentile))+"("+
				operator.Measure.Input.Name+")"
	default:
		return false
	}
}
