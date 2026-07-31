package plan

import (
	"slices"

	"github.com/Suhaibinator/open-splunk/internal/spl"
)

// validEventAggregateContract recognizes the deliberately narrow,
// row-preserving eventstats count plan contract. Consumers that use event
// provenance metadata must fail closed when handed forged logical operators.
func validEventAggregateContract(operator *EventAggregate) bool {
	if operator == nil ||
		len(operator.GroupBy) > spl.MaximumStatsGroupFields ||
		operator.Measure.Function != AggregateFunctionCountRows ||
		operator.Measure.Input.Name != "" ||
		operator.Measure.Input.Canonical ||
		operator.Measure.Input.Path != nil ||
		operator.Measure.Input.Range != (spl.Range{}) ||
		operator.Measure.Predicate != nil ||
		operator.Measure.Percentile != 0 ||
		operator.Measure.Output == "" {
		return false
	}
	if _, err := ResolveField(
		operator.Measure.Output,
		spl.Range{},
	); err != nil {
		return false
	}

	seen := make(map[string]struct{}, len(operator.GroupBy))
	for _, group := range operator.GroupBy {
		if !validResolvedEventAggregateField(group) {
			return false
		}
		if _, duplicate := seen[group.Name]; duplicate {
			return false
		}
		seen[group.Name] = struct{}{}
	}
	return true
}

func validResolvedEventAggregateField(field FieldRef) bool {
	resolved, err := ResolveField(field.Name, field.Range)
	if err != nil {
		return false
	}
	return resolved.Name == field.Name &&
		resolved.Canonical == field.Canonical &&
		(resolved.Path == nil) == (field.Path == nil) &&
		slices.Equal(resolved.Path, field.Path) &&
		resolved.Range == field.Range
}
