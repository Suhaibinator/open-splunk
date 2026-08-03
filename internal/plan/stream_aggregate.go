package plan

import (
	"strings"

	"github.com/Suhaibinator/open-splunk/internal/spl"
)

func validStreamAggregateFieldName(name string) bool {
	return name != "" && !strings.ContainsAny(name, "'\"`")
}

// validStreamAggregateContract is shared by every analysis that relies on
// streamstats preserving event identity. Keep it deliberately strict so a
// forged logical operator cannot smuggle a broader aggregate or an
// unsupported global grouped window into a provenance-sensitive path.
func validStreamAggregateContract(operator *StreamAggregate) bool {
	if operator == nil ||
		len(operator.GroupBy) > spl.MaximumStatsGroupFields ||
		operator.WindowRows > spl.MaximumStreamStatsWindow ||
		operator.Measure.Function != AggregateFunctionCountRows ||
		!validStreamAggregateFieldName(operator.Measure.Output) ||
		operator.Measure.Input.Name != "" ||
		operator.Measure.Input.Canonical ||
		operator.Measure.Input.Path != nil ||
		operator.Measure.Input.Range != (spl.Range{}) ||
		operator.Measure.Predicate != nil ||
		operator.Measure.Percentile != 0 ||
		(len(operator.GroupBy) > 0 && operator.WindowRows > 0 && operator.Global) {
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
