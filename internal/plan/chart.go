package plan

import "github.com/Suhaibinator/open-splunk/internal/spl"

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
		operator.Measure.Predicate != nil || operator.Measure.Percentile != 0 {
		return false
	}

	switch operator.Measure.Function {
	case AggregateFunctionCountRows:
		return operator.Measure.Input.Name == "" &&
			!operator.Measure.Input.Canonical &&
			operator.Measure.Input.Path == nil &&
			operator.Measure.Input.Range == (spl.Range{}) &&
			operator.Measure.Output == "count"
	case AggregateFunctionSum, AggregateFunctionAverage:
		if !validResolvedEventAggregateField(operator.Measure.Input) ||
			operator.Measure.Input.Name == operator.Over.Name {
			return false
		}
		canonicalName := "sum"
		if operator.Measure.Function == AggregateFunctionAverage {
			canonicalName = "avg"
		}
		return operator.Measure.Output == canonicalName+"("+operator.Measure.Input.Name+")"
	default:
		return false
	}
}
