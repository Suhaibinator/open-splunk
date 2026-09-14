package plan

import (
	"fmt"

	"github.com/Suhaibinator/open-splunk/internal/spl"
)

func buildStreamStatsCommand(
	result *Query,
	command *spl.StreamStatsCommand,
	outputSchemaKnown bool,
	canonicalTimeAvailable bool,
	expressionBudget *splExpressionResourceBudget,
	publishOutputFieldAndTrackTime func(string),
) error {
	if command == nil {
		return &Diagnostic{
			Code:    "SPL_INVALID_QUERY",
			Message: "streamstats command is nil",
		}
	}
	if len(command.GroupBy) > spl.MaximumStatsGroupFields {
		return &Diagnostic{
			Code: "SPL_QUERY_TOO_COMPLEX",
			Message: fmt.Sprintf(
				"streamstats BY contains more than %d grouping fields",
				spl.MaximumStatsGroupFields,
			),
			Range: command.Range,
		}
	}
	if command.Window > spl.MaximumStreamStatsWindow {
		return &Diagnostic{
			Code: "SPL_UNSUPPORTED_STREAMSTATS_SYNTAX",
			Message: fmt.Sprintf(
				"streamstats window must be between 0 and %d rows",
				spl.MaximumStreamStatsWindow,
			),
			Range: command.Range,
		}
	}
	if (!command.CurrentSpecified && !command.Current) ||
		(!command.WindowSpecified && command.Window != 0) ||
		(!command.GlobalSpecified && !command.Global) {
		return &Diagnostic{
			Code:    "SPL_UNSUPPORTED_STREAMSTATS_SYNTAX",
			Message: "streamstats option values are inconsistent with their default metadata",
			Range:   command.Range,
		}
	}
	if len(command.GroupBy) > 0 && command.Window > 0 &&
		(!command.GlobalSpecified || command.Global) {
		return &Diagnostic{
			Code:    "SPL_UNSUPPORTED_STREAMSTATS_SYNTAX",
			Message: "streamstats with BY and a positive window requires explicit global=false",
			Range:   command.Range,
		}
	}

	aggregate := command.Aggregate
	if aggregate.Sparkline != nil {
		return &Diagnostic{
			Code:    "SPL_UNSUPPORTED_STREAMSTATS_AGGREGATE",
			Message: "sparkline is supported only by stats",
			Range:   aggregate.Range,
		}
	}
	if aggregate.InputGlob != nil || aggregate.AliasGlob != nil ||
		aggregate.InputQuoted || aggregate.AliasQuoted ||
		aggregate.AliasSourceDerived || aggregate.AliasWildcardDerived {
		return &Diagnostic{
			Code:    "SPL_UNSUPPORTED_STREAMSTATS_AGGREGATE",
			Message: "quoted stats-only field provenance is not supported by streamstats",
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
			aggregate.Alias == "" ||
			(!aggregate.ExplicitAlias && aggregate.Alias != "count") {
			return &Diagnostic{
				Code: "SPL_UNSUPPORTED_STREAMSTATS_AGGREGATE",
				Message: "streamstats row count requires no input metadata " +
					"and uses its count default or one explicit alias",
				Range: aggregate.Range,
			}
		}
		measure.Function = AggregateFunctionCountRows
	case spl.AggregateFunctionCountPredicate:
		predicateMeasure, predicateErr := buildCountPredicateMeasure(
			aggregate,
			outputSchemaKnown,
			expressionBudget,
			countPredicateMeasureDiagnostics{
				unsupportedCode: "SPL_UNSUPPORTED_STREAMSTATS_AGGREGATE",
				invalidMessage: "streamstats count(eval(...)) requires one " +
					"predicate, an explicit alias, and no field or percentile metadata",
				ambiguousCode: "SPL_AMBIGUOUS_STREAMSTATS_FIELD",
				reservedMessage: "streamstats cannot read the event result's " +
					"reserved fields payload without an exact upstream schema",
			},
		)
		if predicateErr != nil {
			return predicateErr
		}
		measure = predicateMeasure
	case spl.AggregateFunctionCountValues, spl.AggregateFunctionSum,
		spl.AggregateFunctionAverage, spl.AggregateFunctionMinimum,
		spl.AggregateFunctionMaximum, spl.AggregateFunctionEarliest,
		spl.AggregateFunctionLatest:
		function := convertNamedAggregateFunction(aggregate.Function)
		form, _ := canonicalAggregateName(function, aggregate.Percentile)
		if aggregate.Input == "" ||
			aggregate.InputRange == (spl.Range{}) ||
			aggregate.InputExpression != nil ||
			aggregate.Predicate != nil ||
			aggregate.Percentile != 0 ||
			aggregate.Alias == "" ||
			(!aggregate.ExplicitAlias &&
				aggregate.Alias != form+"("+aggregate.Input+")") {
			return &Diagnostic{
				Code: "SPL_UNSUPPORTED_STREAMSTATS_AGGREGATE",
				Message: fmt.Sprintf(
					"streamstats %s(field) requires one exact input with its canonical default or one explicit alias",
					form,
				),
				Range: aggregate.Range,
			}
		}
		if !validStreamAggregateFieldName(aggregate.Input) {
			return &Diagnostic{
				Code:    "SPL_UNSUPPORTED_STREAMSTATS_SYNTAX",
				Message: fmt.Sprintf("streamstats %s(field) requires one exact unquoted input field", form),
				Range:   aggregate.InputRange,
			}
		}
		input, inputErr := ResolveField(
			aggregate.Input,
			aggregate.InputRange,
		)
		if inputErr != nil {
			return inputErr
		}
		measure.Function = function
		measure.Input = input
	default:
		return &Diagnostic{
			Code: "SPL_UNSUPPORTED_STREAMSTATS_AGGREGATE",
			Message: "streamstats currently supports exactly one count, " +
				"count(field), count(eval(predicate)), sum(field), avg(field), min(field), max(field), " +
				"earliest(field), or latest(field) aggregate",
			Range: aggregate.Range,
		}
	}
	if (aggregate.Function == spl.AggregateFunctionEarliest ||
		aggregate.Function == spl.AggregateFunctionLatest) &&
		!canonicalTimeAvailable {
		return &Diagnostic{
			Code: "SPL_UNSUPPORTED_STREAMSTATS_TIME_FIELD",
			Message: "streamstats earliest and latest require the " +
				"unmodified canonical _time field",
			Range: aggregate.Range,
			Suggestions: []string{
				"run streamstats earliest or latest before removing, replacing, or transforming _time",
			},
		}
	}
	validOutput := validStreamAggregateFieldName(aggregate.Alias)
	if !aggregate.ExplicitAlias && validStreamAggregateOutputName(measure) {
		validOutput = true
	}
	if !validOutput {
		return &Diagnostic{
			Code:    "SPL_UNSUPPORTED_STREAMSTATS_SYNTAX",
			Message: "streamstats AS requires one exact unquoted output field",
			Range:   aggregate.AliasRange,
		}
	}
	if !outputSchemaKnown && aggregate.Alias == "fields" {
		return &Diagnostic{
			Code: "SPL_AMBIGUOUS_STREAMSTATS_FIELD",
			Message: "streamstats cannot replace the event result's reserved " +
				"fields payload without an exact upstream schema",
			Range: aggregate.AliasRange,
		}
	}
	if !outputSchemaKnown && aggregate.Input == "fields" {
		return &Diagnostic{
			Code: "SPL_AMBIGUOUS_STREAMSTATS_FIELD",
			Message: "streamstats cannot read the event result's reserved " +
				"fields payload without an exact upstream schema",
			Range: aggregate.InputRange,
		}
	}
	if _, aliasErr := ResolveField(
		aggregate.Alias,
		aggregate.AliasRange,
	); aliasErr != nil {
		return aliasErr
	}
	for _, group := range command.GroupBy {
		if group.Quoted || !validStreamAggregateFieldName(group.Name) {
			return &Diagnostic{
				Code:    "SPL_UNSUPPORTED_STREAMSTATS_SYNTAX",
				Message: "streamstats BY requires exact unquoted grouping fields",
				Range:   group.Range,
			}
		}
		if !outputSchemaKnown {
			if group.Name == "fields" {
				return &Diagnostic{
					Code: "SPL_AMBIGUOUS_STREAMSTATS_FIELD",
					Message: "streamstats cannot group by the event result's " +
						"reserved fields payload without an exact upstream schema",
					Range: group.Range,
				}
			}
		}
	}
	groupBy, groupErr := convertStatsGroupFields(
		"streamstats",
		command.GroupBy,
	)
	if groupErr != nil {
		return groupErr
	}
	result.Operators = append(
		result.Operators,
		&StreamAggregate{
			GroupBy:        groupBy,
			Measure:        measure,
			IncludeCurrent: command.Current,
			WindowRows:     command.Window,
			Global:         command.Global,
			Range:          command.Range,
		},
	)
	publishOutputFieldAndTrackTime(aggregate.Alias)
	return nil
}
