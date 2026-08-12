package plan

import "errors"

// ValidateStatsAggregateSourceUniqueness rejects a repeated conservative
// stats source identity. It is exported for backend trust-boundary
// revalidation; callers must apply it only to genuine stats Aggregate nodes,
// not the internal Aggregate representation shared by top and rare.
func ValidateStatsAggregateSourceUniqueness(measures []AggregateMeasure) error {
	seen := make([]AggregateMeasure, 0, len(measures))
	for _, measure := range measures {
		for _, source := range seen {
			if sameStatsAggregateSource(source, measure) {
				return errors.New("stats aggregate source is renamed more than once")
			}
		}
		seen = append(seen, measure)
	}
	return nil
}

// sameStatsAggregateSource implements Splunk's documented prohibition on
// renaming one source aggregate to multiple output names. Exact-field inputs
// are compared after lowering, which intentionally normalizes documented
// syntactic aliases such as c/count(field), dc/distinct_count, p/perc, and
// avg/mean. Different aggregate functions over the same field remain valid.
//
// Eval and predicate expressions are deliberately excluded. Official sources
// do not define whether whitespace, case, literal spelling, or other
// semantically equivalent expression forms identify the same source; that
// question remains governed by O-alias-schema rather than an inferred rule.
func sameStatsAggregateSource(left, right AggregateMeasure) bool {
	if left.Sparkline != nil || right.Sparkline != nil {
		if left.Sparkline == nil || right.Sparkline == nil {
			return false
		}
		return sameStatsSparklineSource(left.Sparkline, right.Sparkline)
	}
	if left.InputExpression != nil || right.InputExpression != nil ||
		left.Predicate != nil || right.Predicate != nil {
		return false
	}
	return left.Function == right.Function &&
		left.Percentile == right.Percentile &&
		left.Input.Name == right.Input.Name
}

func sameStatsSparklineSource(left, right *SparklineMeasure) bool {
	if left == nil || right == nil {
		return false
	}
	return left.Function == right.Function &&
		left.Input.Name == right.Input.Name &&
		left.Time.Name == right.Time.Name &&
		left.Span == right.Span &&
		left.MaximumPoints == right.MaximumPoints
}
