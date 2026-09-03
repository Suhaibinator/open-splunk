package plan

import (
	"fmt"

	"github.com/Suhaibinator/open-splunk/internal/spl"
)

func buildEventStatsCommand(
	result *Query,
	command *spl.EventStatsCommand,
	outputSchemaKnown bool,
	canonicalTimeAvailable bool,
	expressionBudget *splExpressionResourceBudget,
	publishOutputFieldAndTrackTime func(string),
) error {
	if command == nil {
		return &Diagnostic{
			Code:    "SPL_INVALID_QUERY",
			Message: "eventstats command is nil",
		}
	}
	if len(command.GroupBy) > spl.MaximumStatsGroupFields {
		return &Diagnostic{
			Code: "SPL_QUERY_TOO_COMPLEX",
			Message: fmt.Sprintf(
				"eventstats BY contains more than %d grouping fields",
				spl.MaximumStatsGroupFields,
			),
			Range: command.Range,
		}
	}
	aggregate := command.Aggregate
	if aggregate.Sparkline != nil {
		return &Diagnostic{
			Code:    "SPL_UNSUPPORTED_EVENTSTATS_AGGREGATE",
			Message: "sparkline is supported only by stats",
			Range:   aggregate.Range,
		}
	}
	if aggregate.InputGlob != nil || aggregate.AliasGlob != nil ||
		aggregate.InputQuoted || aggregate.AliasQuoted ||
		aggregate.AliasSourceDerived || aggregate.AliasWildcardDerived {
		return &Diagnostic{
			Code:    "SPL_UNSUPPORTED_EVENTSTATS_AGGREGATE",
			Message: "quoted stats-only field provenance is not supported by eventstats",
			Range:   aggregate.Range,
		}
	}
	if aggregate.Alias == "" {
		return &Diagnostic{
			Code:    "SPL_UNSUPPORTED_EVENTSTATS_AGGREGATE",
			Message: eventStatsSupportedAggregateMessage,
			Range:   aggregate.Range,
		}
	}
	measure := AggregateMeasure{Output: aggregate.Alias}
	switch aggregate.Function {
	case spl.AggregateFunctionCount:
		if aggregate.Input != "" ||
			aggregate.InputRange != (spl.Range{}) ||
			aggregate.InputExpression != nil ||
			aggregate.Predicate != nil ||
			aggregate.Percentile != 0 ||
			(!aggregate.ExplicitAlias && aggregate.Alias != "count") {
			return &Diagnostic{
				Code: "SPL_UNSUPPORTED_EVENTSTATS_AGGREGATE",
				Message: "eventstats row count requires no input " +
					"metadata and uses either its count default or an explicit alias",
				Range: aggregate.Range,
			}
		}
		measure.Function = AggregateFunctionCountRows
	case spl.AggregateFunctionCountValues:
		var inputErr error
		measure, inputErr = buildEventStatsFieldMeasure(
			aggregate,
			AggregateFunctionCountValues,
			"count(field)",
		)
		if inputErr != nil {
			return inputErr
		}
	case spl.AggregateFunctionPercentile:
		var inputErr error
		measure, inputErr = buildEventStatsFieldMeasure(
			aggregate,
			AggregateFunctionPercentile,
			"pN(field) or percN(field)",
		)
		if inputErr != nil {
			return inputErr
		}
	case spl.AggregateFunctionMinimum, spl.AggregateFunctionMaximum,
		spl.AggregateFunctionEarliest,
		spl.AggregateFunctionLatest,
		spl.AggregateFunctionSum,
		spl.AggregateFunctionAverage,
		spl.AggregateFunctionDistinctCount,
		spl.AggregateFunctionValues,
		spl.AggregateFunctionList:
		function := convertNamedAggregateFunction(aggregate.Function)
		name, _ := canonicalAggregateName(function, aggregate.Percentile)
		form := name + "(field)"
		var inputErr error
		measure, inputErr = buildEventStatsFieldMeasure(
			aggregate,
			function,
			form,
		)
		if inputErr != nil {
			return inputErr
		}
	case spl.AggregateFunctionCountPredicate:
		predicateMeasure, predicateErr := buildCountPredicateMeasure(
			aggregate,
			outputSchemaKnown,
			expressionBudget,
			countPredicateMeasureDiagnostics{
				unsupportedCode: "SPL_UNSUPPORTED_EVENTSTATS_AGGREGATE",
				invalidMessage: "eventstats count(eval(...)) requires one " +
					"predicate, an explicit alias, and no field or percentile metadata",
				ambiguousCode: "SPL_AMBIGUOUS_EVENTSTATS_FIELD",
				reservedMessage: "eventstats cannot read the event result's " +
					"reserved fields payload without an exact upstream schema",
			},
		)
		if predicateErr != nil {
			return predicateErr
		}
		measure = predicateMeasure
	default:
		return &Diagnostic{
			Code:    "SPL_UNSUPPORTED_EVENTSTATS_AGGREGATE",
			Message: eventStatsSupportedAggregateMessage,
			Range:   aggregate.Range,
		}
	}
	if (aggregate.Function == spl.AggregateFunctionEarliest ||
		aggregate.Function == spl.AggregateFunctionLatest) &&
		!canonicalTimeAvailable {
		return &Diagnostic{
			Code: "SPL_UNSUPPORTED_EVENTSTATS_TIME_FIELD",
			Message: "eventstats earliest and latest require the " +
				"unmodified canonical _time field",
			Range: aggregate.Range,
			Suggestions: []string{
				"run eventstats earliest or latest before removing, replacing, or transforming _time",
			},
		}
	}
	if !outputSchemaKnown {
		if aggregate.Alias == "fields" {
			return &Diagnostic{
				Code: "SPL_AMBIGUOUS_EVENTSTATS_FIELD",
				Message: "eventstats cannot replace the event result's " +
					"reserved fields payload without an exact upstream schema",
				Range: aggregate.AliasRange,
			}
		}
		if aggregate.Input == "fields" {
			return &Diagnostic{
				Code: "SPL_AMBIGUOUS_EVENTSTATS_FIELD",
				Message: "eventstats cannot read the event result's " +
					"reserved fields payload without an exact upstream schema",
				Range: aggregate.InputRange,
			}
		}
		for _, group := range command.GroupBy {
			if group.Name == "fields" {
				return &Diagnostic{
					Code: "SPL_AMBIGUOUS_EVENTSTATS_FIELD",
					Message: "eventstats cannot group by the event result's " +
						"reserved fields payload without an exact upstream schema",
					Range: group.Range,
				}
			}
		}
	}
	if _, aliasErr := ResolveField(
		aggregate.Alias,
		aggregate.AliasRange,
	); aliasErr != nil {
		return aliasErr
	}
	groupBy, groupErr := convertStatsGroupFields(
		"eventstats",
		command.GroupBy,
	)
	if groupErr != nil {
		return groupErr
	}
	result.Operators = append(
		result.Operators,
		&EventAggregate{
			GroupBy: groupBy,
			Measure: measure,
			Range:   command.Range,
		},
	)
	publishOutputFieldAndTrackTime(aggregate.Alias)
	return nil
}
