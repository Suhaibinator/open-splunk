package plan

import "github.com/Suhaibinator/open-splunk/internal/spl"

func validStreamAggregateFieldName(name string) bool {
	return spl.IsExactUnquotedFieldName(name)
}

func validStreamAggregateOutputName(measure AggregateMeasure) bool {
	return validStreamAggregateFieldName(measure.Output) ||
		((measure.Function == AggregateFunctionCountValues ||
			measure.Function == AggregateFunctionSum ||
			measure.Function == AggregateFunctionAverage) &&
			measure.Input.Name != "" &&
			measure.Output == streamAggregateDefaultOutput(measure))
}

func streamAggregateDefaultOutput(measure AggregateMeasure) string {
	switch measure.Function {
	case AggregateFunctionCountValues:
		return "count(" + measure.Input.Name + ")"
	case AggregateFunctionSum:
		return "sum(" + measure.Input.Name + ")"
	case AggregateFunctionAverage:
		return "avg(" + measure.Input.Name + ")"
	default:
		return ""
	}
}

// validStreamAggregateContract is shared by every analysis that relies on
// streamstats preserving event identity. Keep it deliberately strict so a
// forged logical operator cannot smuggle a broader aggregate or an
// unsupported global grouped window into a provenance-sensitive path.
func validStreamAggregateContract(operator *StreamAggregate) bool {
	if operator == nil ||
		len(operator.GroupBy) > spl.MaximumStatsGroupFields ||
		operator.WindowRows > spl.MaximumStreamStatsWindow ||
		!validStreamAggregateOutputName(operator.Measure) ||
		operator.Measure.Predicate != nil ||
		operator.Measure.Percentile != 0 ||
		(len(operator.GroupBy) > 0 && operator.WindowRows > 0 && operator.Global) {
		return false
	}
	switch operator.Measure.Function {
	case AggregateFunctionCountRows:
		if operator.Measure.Input.Name != "" ||
			operator.Measure.Input.Canonical ||
			operator.Measure.Input.Path != nil ||
			operator.Measure.Input.Range != (spl.Range{}) {
			return false
		}
	case AggregateFunctionCountValues, AggregateFunctionSum,
		AggregateFunctionAverage:
		if !validStreamAggregateFieldName(operator.Measure.Input.Name) ||
			!validResolvedEventAggregateField(operator.Measure.Input) {
			return false
		}
	default:
		return false
	}
	if _, err := ResolveField(operator.Measure.Output, spl.Range{}); err != nil {
		return false
	}
	seen := make(map[string]struct{}, len(operator.GroupBy))
	for _, group := range operator.GroupBy {
		if !validStreamAggregateFieldName(group.Name) ||
			!validResolvedEventAggregateField(group) {
			return false
		}
		if _, duplicate := seen[group.Name]; duplicate {
			return false
		}
		seen[group.Name] = struct{}{}
	}
	return true
}
