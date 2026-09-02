package plan

import (
	"time"

	"github.com/Suhaibinator/open-splunk/internal/spl"
)

func validBucketSpanContract(span time.Duration, calendar CalendarUnit) bool {
	switch calendar {
	case CalendarNone:
		return span > 0
	case CalendarDay, CalendarWeek:
		return span == 0
	default:
		return false
	}
}

// validTimechartMeasureContract validates the aggregate-specific logical
// shape needed by metadata consumers. Backend compilation independently
// revalidates the complete time-grid and resolved-field contract before SQL is
// emitted, because a logical plan can be forged without passing through Build.
func validTimechartMeasureContract(operator *Timechart) bool {
	if operator == nil ||
		operator.Measure.Output == "" ||
		operator.Measure.OutputLiteral ||
		operator.Measure.Sparkline != nil ||
		operator.Measure.Predicate != nil ||
		operator.Measure.InputExpression != nil ||
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
		return emptyAggregateField(operator.Measure.Input) &&
			operator.Measure.Percentile == 0 &&
			operator.Measure.Output == "count"
	case AggregateFunctionPercentile:
		return validResolvedEventAggregateField(operator.Measure.Input) &&
			operator.Measure.Percentile >= 1 &&
			operator.Measure.Percentile <= 99 &&
			operator.Measure.Output != "_time" &&
			(operator.Split == nil ||
				operator.Split.Field.Name != operator.Measure.Input.Name)
	case AggregateFunctionCountValues, AggregateFunctionSum,
		AggregateFunctionAverage:
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
		split.SeriesLimit >= 1 &&
		split.SeriesLimit <= timechartSeriesLimit &&
		split.NullLabel == "NULL" &&
		split.OtherLabel == "OTHER"
}
