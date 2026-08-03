package plan

import "github.com/Suhaibinator/open-splunk/internal/spl"

// validTimechartMeasureContract validates the aggregate-specific logical
// shape needed by metadata consumers. Backend compilation independently
// revalidates the complete time-grid and resolved-field contract before SQL is
// emitted, because a logical plan can be forged without passing through Build.
func validTimechartMeasureContract(operator *Timechart) bool {
	if operator == nil ||
		operator.Measure.Output == "" ||
		operator.Measure.Predicate != nil ||
		operator.Time.Name != "_time" ||
		!operator.Time.Canonical ||
		!validResolvedEventAggregateField(operator.Time) {
		return false
	}
	if _, err := ResolveField(operator.Measure.Output, spl.Range{}); err != nil {
		return false
	}
	if operator.Split != nil && !validTimechartSplitContract(operator.Split) {
		return false
	}
	switch operator.Measure.Function {
	case AggregateFunctionCountRows:
		return operator.Measure.Input.Name == "" &&
			!operator.Measure.Input.Canonical &&
			operator.Measure.Input.Path == nil &&
			operator.Measure.Input.Range == (spl.Range{}) &&
			operator.Measure.Percentile == 0 &&
			operator.Measure.Output == "count"
	case AggregateFunctionPercentile:
		return validResolvedEventAggregateField(operator.Measure.Input) &&
			operator.Measure.Percentile >= 1 &&
			operator.Measure.Percentile <= 99 &&
			operator.Measure.Output != "_time" &&
			(operator.Split == nil ||
				operator.Split.Field.Name != operator.Measure.Input.Name)
	case AggregateFunctionSum, AggregateFunctionAverage:
		return validResolvedEventAggregateField(operator.Measure.Input) &&
			operator.Measure.Percentile == 0 &&
			operator.Measure.Output != "_time" &&
			(operator.Split == nil ||
				operator.Split.Field.Name != operator.Measure.Input.Name)
	default:
		return false
	}
}

func validTimechartSplitContract(split *TimechartSplit) bool {
	return split != nil &&
		validResolvedEventAggregateField(split.Field) &&
		split.SeriesLimit == timechartSeriesLimit &&
		split.IncludeNull &&
		split.IncludeOther &&
		split.NullLabel == "NULL" &&
		split.OtherLabel == "OTHER"
}
