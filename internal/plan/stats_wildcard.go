package plan

import (
	"fmt"

	"github.com/Suhaibinator/open-splunk/internal/spl"
)

// expandStatsWildcardAggregates consumes parser-owned ordinary and sparkline
// wc-field metadata and returns only exact-field aggregates. Expansion follows
// the proven upstream OutputFields order so neither map iteration nor backend
// discovery can influence measure or result-column order.
func expandStatsWildcardAggregates(
	aggregates []spl.StatsAggregate,
	outputSchemaKnown bool,
	upstreamFields []string,
) ([]spl.StatsAggregate, *Diagnostic) {
	hasWildcard := false
	for _, aggregate := range aggregates {
		if aggregate.InputGlob != nil ||
			(aggregate.Sparkline != nil && aggregate.Sparkline.InputGlob != nil) {
			hasWildcard = true
			break
		}
	}
	if !hasWildcard {
		return aggregates, nil
	}

	expanded := make([]spl.StatsAggregate, 0, len(aggregates))
	for _, aggregate := range aggregates {
		if aggregate.InputGlob == nil &&
			(aggregate.Sparkline == nil || aggregate.Sparkline.InputGlob == nil) {
			if len(expanded) >= spl.MaximumStatsMeasures {
				return nil, statsWildcardMeasureLimitDiagnostic(aggregate.Range)
			}
			expanded = append(expanded, aggregate)
			continue
		}
		glob := aggregate.InputGlob
		sparklineGlob := aggregate.Sparkline != nil && aggregate.Sparkline.InputGlob != nil
		if sparklineGlob {
			glob = aggregate.Sparkline.InputGlob
		}
		valid := spl.ValidStatsFieldGlobAggregate(aggregate)
		if sparklineGlob {
			valid = spl.ValidStatsSparklineFieldGlobAggregate(aggregate)
		}
		if !valid {
			return nil, &Diagnostic{
				Code:    "SPL_UNSUPPORTED_STATS_AGGREGATE",
				Message: "stats wc-field aggregate metadata is invalid",
				Range:   glob.Range,
			}
		}
		if !outputSchemaKnown {
			return nil, &Diagnostic{
				Code: "SPL_UNSUPPORTED_STATS_WILDCARD",
				Message: "stats wc-field inputs require an exact upstream output " +
					"schema so expansion is deterministic",
				Range: glob.Range,
				Suggestions: []string{
					"select an exact schema with table before the wildcard stats aggregate",
				},
			}
		}

		matches := 0
		for _, field := range upstreamFields {
			if !spl.MatchStatsFieldGlob(glob.Pattern, field) {
				continue
			}
			concrete, ok := spl.ExpandStatsFieldGlob(aggregate, field)
			if sparklineGlob {
				concrete, ok = spl.ExpandStatsSparklineFieldGlob(aggregate, field)
			}
			if !ok {
				return nil, &Diagnostic{
					Code: "SPL_UNSUPPORTED_STATS_WILDCARD",
					Message: fmt.Sprintf(
						"stats wc-field %q matched unsupported upstream field %q",
						glob.Pattern,
						field,
					),
					Range: glob.Range,
				}
			}
			if len(expanded) >= spl.MaximumStatsMeasures {
				return nil, statsWildcardMeasureLimitDiagnostic(glob.Range)
			}
			expanded = append(expanded, concrete)
			matches++
		}
		if matches == 0 {
			return nil, &Diagnostic{
				Code:    "SPL_NO_MATCHING_STATS_FIELDS",
				Message: fmt.Sprintf("stats wc-field %q matched no upstream output fields", glob.Pattern),
				Range:   glob.Range,
			}
		}
	}
	return expanded, nil
}

func statsWildcardMeasureLimitDiagnostic(sourceRange spl.Range) *Diagnostic {
	return &Diagnostic{
		Code: "SPL_QUERY_TOO_COMPLEX",
		Message: fmt.Sprintf(
			"stats wc-field expansion contains more than %d concrete aggregate measures",
			spl.MaximumStatsMeasures,
		),
		Range: sourceRange,
	}
}
