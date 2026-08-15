package plan

import (
	"strconv"

	"github.com/Suhaibinator/open-splunk/internal/spl"
)

// canonicalAggregateName returns the documented short name for one plan
// aggregate. percentile is used only for AggregateFunctionPercentile.
func canonicalAggregateName(function AggregateFunction, percentile uint8) (string, bool) {
	switch function {
	case AggregateFunctionCountValues:
		return "count", true
	case AggregateFunctionSum:
		return "sum", true
	case AggregateFunctionAverage:
		return "avg", true
	case AggregateFunctionMinimum:
		return "min", true
	case AggregateFunctionMaximum:
		return "max", true
	case AggregateFunctionEarliest:
		return "earliest", true
	case AggregateFunctionLatest:
		return "latest", true
	case AggregateFunctionDistinctCount:
		return "dc", true
	case AggregateFunctionValues:
		return "values", true
	case AggregateFunctionList:
		return "list", true
	case AggregateFunctionPercentile:
		return "perc" + strconv.Itoa(int(percentile)), true
	default:
		return "", false
	}
}

func validStreamAggregateFieldName(name string) bool {
	return spl.IsExactUnquotedFieldName(name)
}

func validStreamAggregateOutputName(measure AggregateMeasure) bool {
	defaultOutput := streamAggregateDefaultOutput(measure)
	return validStreamAggregateFieldName(measure.Output) ||
		(defaultOutput != "" && measure.Output == defaultOutput)
}

func streamAggregateDefaultOutput(measure AggregateMeasure) string {
	// streamstats supports a strict subset of the named aggregates; the
	// allowlist keeps canonicalAggregateName from widening it.
	switch measure.Function {
	case AggregateFunctionCountValues, AggregateFunctionSum,
		AggregateFunctionAverage, AggregateFunctionMinimum,
		AggregateFunctionMaximum, AggregateFunctionEarliest,
		AggregateFunctionLatest:
	default:
		return ""
	}
	name, ok := canonicalAggregateName(measure.Function, 0)
	if !ok {
		return ""
	}
	return name + "(" + measure.Input.Name + ")"
}

// validStreamAggregateContract is shared by every analysis that relies on
// streamstats preserving event identity. Keep it deliberately strict so a
// forged logical operator cannot smuggle a broader aggregate or an
// unsupported global grouped window into a provenance-sensitive path.
func validStreamAggregateContract(operator *StreamAggregate) bool {
	if operator == nil ||
		len(operator.GroupBy) > spl.MaximumStatsGroupFields ||
		operator.WindowRows > spl.MaximumStreamStatsWindow ||
		operator.Measure.OutputLiteral ||
		!validStreamAggregateOutputName(operator.Measure) ||
		operator.Measure.Sparkline != nil ||
		operator.Measure.InputExpression != nil ||
		operator.Measure.Percentile != 0 ||
		(len(operator.GroupBy) > 0 && operator.WindowRows > 0 && operator.Global) {
		return false
	}
	switch operator.Measure.Function {
	case AggregateFunctionCountRows:
		if operator.Measure.Predicate != nil ||
			!emptyAggregateField(operator.Measure.Input) {
			return false
		}
	case AggregateFunctionCountPredicate:
		if !emptyAggregateField(operator.Measure.Input) ||
			!validEventAggregatePredicate(operator.Measure.Predicate) {
			return false
		}
	case AggregateFunctionCountValues, AggregateFunctionSum,
		AggregateFunctionAverage, AggregateFunctionMinimum,
		AggregateFunctionMaximum, AggregateFunctionEarliest,
		AggregateFunctionLatest:
		if operator.Measure.Predicate != nil ||
			!validStreamAggregateFieldName(operator.Measure.Input.Name) ||
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
