package plan

import (
	"fmt"

	"github.com/Suhaibinator/open-splunk/internal/nilcheck"
	"github.com/Suhaibinator/open-splunk/internal/spl"
)

func buildStatsCommand(
	result *Query,
	command *spl.StatsCommand,
	outputSchemaKnown bool,
	canonicalTimeAvailable bool,
	expressionBudget *splExpressionResourceBudget,
) error {
	if len(command.Aggregates) == 0 {
		return &Diagnostic{
			Code:    "SPL_EXPECTED_AGGREGATE",
			Message: "stats requires an aggregate function",
			Range:   command.Range,
		}
	}
	if len(command.Aggregates) > spl.MaximumStatsMeasures {
		return &Diagnostic{
			Code:    "SPL_QUERY_TOO_COMPLEX",
			Message: fmt.Sprintf("stats contains more than %d aggregate measures", spl.MaximumStatsMeasures),
			Range:   command.Range,
		}
	}
	aggregates, wildcardErr := expandStatsWildcardAggregates(
		command.Aggregates,
		outputSchemaKnown,
		result.OutputFields,
	)
	if wildcardErr != nil {
		return wildcardErr
	}
	if len(command.GroupBy) > spl.MaximumStatsGroupFields {
		return &Diagnostic{
			Code:    "SPL_QUERY_TOO_COMPLEX",
			Message: fmt.Sprintf("stats BY contains more than %d grouping fields", spl.MaximumStatsGroupFields),
			Range:   command.Range,
		}
	}
	statsOptions, optionsErr := buildStatsOptions(
		command.Options,
		command.Range,
	)
	if optionsErr != nil {
		return optionsErr
	}
	if !outputSchemaKnown {
		for _, aggregate := range aggregates {
			if aggregate.Input == "fields" ||
				(aggregate.Sparkline != nil && aggregate.Sparkline.Input == "fields") {
				inputRange := aggregate.InputRange
				if aggregate.Sparkline != nil {
					inputRange = aggregate.Sparkline.InputRange
				}
				return &Diagnostic{
					Code:    "SPL_AMBIGUOUS_STATS_FIELD",
					Message: "stats cannot read the event result's reserved fields payload without an exact upstream schema",
					Range:   inputRange,
				}
			}
		}
		for _, group := range command.GroupBy {
			if group.Name == "fields" {
				return &Diagnostic{
					Code:    "SPL_AMBIGUOUS_STATS_FIELD",
					Message: "stats cannot group by the event result's reserved fields payload without an exact upstream schema",
					Range:   group.Range,
				}
			}
		}
	}
	groupBy, groupErr := convertStatsGroupFields(
		"stats",
		command.GroupBy,
	)
	if groupErr != nil {
		return groupErr
	}
	seenOutputs := make(map[string]struct{}, len(groupBy)+len(aggregates))
	outputFields := make([]string, 0, len(groupBy)+len(aggregates))
	for _, group := range groupBy {
		seenOutputs[group.Name] = struct{}{}
		outputFields = append(outputFields, group.Name)
	}
	measures := make([]AggregateMeasure, 0, len(aggregates))
	seenSources := make([]AggregateMeasure, 0, len(aggregates))
	for _, aggregate := range aggregates {
		if aggregate.InputGlob != nil || aggregate.AliasGlob != nil {
			return &Diagnostic{
				Code:    "SPL_UNSUPPORTED_STATS_AGGREGATE",
				Message: "unexpanded stats wc-field metadata is invalid",
				Range:   aggregate.Range,
			}
		}
		if aggregate.Sparkline != nil &&
			(aggregate.Function != spl.AggregateFunctionInvalid ||
				aggregate.Input != "" || aggregate.InputRange != (spl.Range{}) ||
				aggregate.InputQuoted ||
				aggregate.InputExpression != nil || aggregate.Predicate != nil ||
				aggregate.Percentile != 0) {
			return &Diagnostic{
				Code:    "SPL_UNSUPPORTED_STATS_AGGREGATE",
				Message: "sparkline metadata is mutually exclusive with ordinary aggregate metadata",
				Range:   aggregate.Range,
			}
		}
		if aggregate.InputExpression != nil &&
			nilcheck.IsNil(aggregate.InputExpression) {
			return &Diagnostic{
				Code:    "SPL_UNSUPPORTED_STATS_AGGREGATE",
				Message: "stats aggregate contains a missing scalar input expression",
				Range:   aggregate.Range,
			}
		}
		if aggregate.InputExpression != nil &&
			(aggregate.Input != "" || aggregate.InputRange != (spl.Range{}) ||
				aggregate.InputQuoted || aggregate.Predicate != nil) {
			return &Diagnostic{
				Code:    "SPL_UNSUPPORTED_STATS_AGGREGATE",
				Message: "stats aggregate scalar input is mutually exclusive with exact-field and predicate metadata",
				Range:   aggregate.Range,
			}
		}
		if aggregate.Function != spl.AggregateFunctionCountPredicate &&
			aggregate.Predicate != nil {
			return &Diagnostic{
				Code:    "SPL_UNSUPPORTED_STATS_AGGREGATE",
				Message: "only count(eval(...)) can contain predicate metadata",
				Range:   aggregate.Range,
			}
		}
		if aggregate.Input == "" && aggregate.InputQuoted {
			return &Diagnostic{
				Code:    "SPL_UNSUPPORTED_STATS_AGGREGATE",
				Message: "stats aggregate contains quoted-input provenance without an exact input field",
				Range:   aggregate.Range,
			}
		}
		if statsAggregateUsesPercentileSuffix(aggregate.Function) {
			if aggregate.Percentile < 1 || aggregate.Percentile > 99 {
				return &Diagnostic{
					Code:    "SPL_UNSUPPORTED_STATS_AGGREGATE",
					Message: "percentile suffix must be an integer from 1 through 99",
					Range:   aggregate.Range,
				}
			}
		} else if aggregate.Percentile != 0 {
			return &Diagnostic{
				Code:    "SPL_UNSUPPORTED_STATS_AGGREGATE",
				Message: "stats aggregate without a percentile suffix contains percentile metadata",
				Range:   aggregate.Range,
			}
		}
		if aggregate.Sparkline != nil {
			validAliasMetadata := aggregate.Alias != "" &&
				aggregate.AliasRange != (spl.Range{}) &&
				aggregate.Range != (spl.Range{}) &&
				aggregate.Range.Start == aggregate.Sparkline.Range.Start
			if aggregate.ExplicitAlias {
				validAliasMetadata = validAliasMetadata &&
					aggregate.Range.End == aggregate.AliasRange.End
			} else {
				validAliasMetadata = validAliasMetadata &&
					aggregate.Alias == "sparkline" &&
					aggregate.Range == aggregate.Sparkline.Range &&
					aggregate.AliasRange == aggregate.Sparkline.Range
			}
			if !validAliasMetadata {
				return &Diagnostic{
					Code:    "SPL_UNSUPPORTED_STATS_AGGREGATE",
					Message: "stats sparkline alias metadata is invalid",
					Range:   aggregate.Range,
				}
			}
		}
		if aggregate.AliasQuoted &&
			(!aggregate.ExplicitAlias || aggregate.AliasSourceDerived ||
				aggregate.AliasWildcardDerived) {
			return &Diagnostic{
				Code:    "SPL_UNSUPPORTED_STATS_AGGREGATE",
				Message: "quoted stats output provenance requires an explicit AS alias",
				Range:   aggregate.AliasRange,
			}
		}
		if aggregate.AliasSourceDerived &&
			(aggregate.ExplicitAlias || aggregate.AliasQuoted ||
				aggregate.AliasWildcardDerived ||
				(aggregate.InputExpression == nil &&
					aggregate.Function != spl.AggregateFunctionCountPredicate) ||
				aggregate.Range == (spl.Range{}) ||
				aggregate.AliasRange != aggregate.Range) {
			return &Diagnostic{
				Code:    "SPL_UNSUPPORTED_STATS_AGGREGATE",
				Message: "source-derived stats output provenance is invalid",
				Range:   aggregate.AliasRange,
			}
		}
		if aggregate.AliasWildcardDerived &&
			!spl.ValidStatsWildcardDerivedAlias(aggregate) {
			return &Diagnostic{
				Code:    "SPL_UNSUPPORTED_STATS_AGGREGATE",
				Message: "wc-field-derived stats output provenance is invalid",
				Range:   aggregate.AliasRange,
			}
		}
		hasEvalSource := aggregate.InputExpression != nil ||
			aggregate.Function == spl.AggregateFunctionCountPredicate
		if hasEvalSource && !aggregate.ExplicitAlias &&
			!aggregate.AliasSourceDerived && !aggregate.AliasWildcardDerived {
			return &Diagnostic{
				Code:    "SPL_UNSUPPORTED_STATS_AGGREGATE",
				Message: "implicit stats eval output requires source-derived alias provenance",
				Range:   aggregate.AliasRange,
			}
		}
		literalOutput := aggregate.AliasQuoted || aggregate.AliasSourceDerived ||
			aggregate.AliasWildcardDerived
		if literalOutput {
			if aggregate.AliasRange == (spl.Range{}) ||
				!spl.IsStatsLiteralOutputName(aggregate.Alias) {
				return &Diagnostic{
					Code:    "SPL_INVALID_FIELD",
					Message: "stats literal output field is empty, invalid, private, reserved, or too long",
					Range:   aggregate.AliasRange,
				}
			}
		} else if _, aliasErr := ResolveField(aggregate.Alias, aggregate.AliasRange); aliasErr != nil {
			return aliasErr
		}
		if _, duplicate := seenOutputs[aggregate.Alias]; duplicate {
			return &Diagnostic{
				Code:    "SPL_DUPLICATE_FIELD",
				Message: fmt.Sprintf("aggregate output field %q is duplicated", aggregate.Alias),
				Range:   aggregate.AliasRange,
			}
		}
		seenOutputs[aggregate.Alias] = struct{}{}
		measure := AggregateMeasure{
			Output:        aggregate.Alias,
			OutputLiteral: literalOutput,
		}
		if aggregate.Sparkline != nil {
			if !canonicalTimeAvailable {
				return &Diagnostic{
					Code:    "SPL_UNSUPPORTED_STATS_TIME_FIELD",
					Message: "stats sparkline requires the unmodified canonical _time field",
					Range:   aggregate.Sparkline.Range,
					Suggestions: []string{
						"run stats sparkline before removing, replacing, or transforming _time",
					},
				}
			}
			sparkline, sparklineErr := buildStatsSparklineMeasure(
				aggregate.Sparkline,
			)
			if sparklineErr != nil {
				return sparklineErr
			}
			measure.Sparkline = sparkline
		} else {
			switch aggregate.Function {
			case spl.AggregateFunctionCount:
				if aggregate.Input != "" || aggregate.InputRange != (spl.Range{}) ||
					aggregate.InputQuoted || aggregate.InputExpression != nil {
					return &Diagnostic{
						Code:    "SPL_UNSUPPORTED_STATS_AGGREGATE",
						Message: "argument-free count cannot contain input metadata",
						Range:   aggregate.Range,
					}
				}
				measure.Function = AggregateFunctionCountRows
			case spl.AggregateFunctionCountPredicate:
				predicateMeasure, predicateErr := buildCountPredicateMeasure(
					aggregate,
					outputSchemaKnown,
					expressionBudget,
					countPredicateMeasureDiagnostics{
						unsupportedCode:    "SPL_UNSUPPORTED_STATS_AGGREGATE",
						invalidMessage:     "count(eval(...)) requires one predicate, a valid explicit or source-derived output, and no field or percentile metadata",
						ambiguousCode:      "SPL_AMBIGUOUS_STATS_FIELD",
						reservedMessage:    "stats cannot read the event result's reserved fields payload without an exact upstream schema",
						allowImplicitAlias: true,
					},
				)
				if predicateErr != nil {
					return predicateErr
				}
				measure = predicateMeasure
			case spl.AggregateFunctionCountValues:
				if aggregate.Input == "" || aggregate.InputRange == (spl.Range{}) ||
					aggregate.InputExpression != nil {
					return &Diagnostic{
						Code:    "SPL_UNSUPPORTED_STATS_AGGREGATE",
						Message: "count(field) requires one exact input field",
						Range:   aggregate.Range,
					}
				}
				if aggregate.InputQuoted {
					if !spl.IsStatsLiteralFieldReference(aggregate.Input) {
						return &Diagnostic{
							Code:    "SPL_INVALID_FIELD",
							Message: "quoted stats aggregate input is invalid",
							Range:   aggregate.InputRange,
						}
					}
				} else if !spl.IsExactUnquotedFieldName(aggregate.Input) {
					return &Diagnostic{
						Code:    "SPL_UNSUPPORTED_STATS_AGGREGATE",
						Message: "stats aggregate input is not an exact unquoted field",
						Range:   aggregate.InputRange,
					}
				}
				input, inputErr := resolveStatsInputField(
					aggregate.Input,
					aggregate.InputRange,
					aggregate.InputQuoted,
				)
				if inputErr != nil {
					return inputErr
				}
				measure.Function = AggregateFunctionCountValues
				measure.Input = input
			default:
				function, supported := convertStatsFieldAggregateFunction(
					aggregate.Function,
				)
				if !supported {
					return &Diagnostic{
						Code:    "SPL_UNSUPPORTED_STATS_AGGREGATE",
						Message: "unsupported stats aggregate",
						Range:   aggregate.Range,
					}
				}
				hasExactInput := aggregate.Input != "" ||
					aggregate.InputRange != (spl.Range{})
				hasExpressionInput := aggregate.InputExpression != nil
				if hasExactInput == hasExpressionInput ||
					(hasExactInput && (aggregate.Input == "" ||
						aggregate.InputRange == (spl.Range{}))) {
					return &Diagnostic{
						Code:    "SPL_UNSUPPORTED_STATS_AGGREGATE",
						Message: "stats aggregate requires exactly one exact-field or eval scalar input",
						Range:   aggregate.Range,
					}
				}
				if statsAggregateRequiresCanonicalTime(aggregate.Function) &&
					!canonicalTimeAvailable {
					return &Diagnostic{
						Code: "SPL_UNSUPPORTED_STATS_TIME_FIELD",
						Message: "time-sensitive stats aggregates require the " +
							"unmodified canonical _time field",
						Range: aggregate.Range,
						Suggestions: []string{
							"run stats earliest or latest before removing, replacing, or transforming _time",
							"run the time-sensitive stats aggregate before removing, replacing, or transforming _time",
						},
					}
				}
				measure.Function = function
				if hasExpressionInput {
					if complexityErr := validateSPLScalarExpressionComplexity(
						aggregate.InputExpression,
						expressionBudget,
					); complexityErr != nil {
						return complexityErr
					}
					expression, expressionErr := convertScalarExpressionUnchecked(
						aggregate.InputExpression,
					)
					if expressionErr != nil {
						return expressionErr
					}
					if !outputSchemaKnown {
						fieldRange, referencesReserved, inventoryErr := predicateFieldRange(
							&ScalarPredicateExpression{
								Value: expression,
								Range: expression.SourceRange(),
							},
						)
						if inventoryErr != nil {
							return inventoryErr
						}
						if referencesReserved {
							return &Diagnostic{
								Code:    "SPL_AMBIGUOUS_STATS_FIELD",
								Message: "stats cannot read the event result's reserved fields payload without an exact upstream schema",
								Range:   fieldRange,
							}
						}
					}
					measure.InputExpression = expression
				} else {
					if aggregate.InputQuoted {
						if !spl.IsStatsLiteralFieldReference(aggregate.Input) {
							return &Diagnostic{
								Code:    "SPL_INVALID_FIELD",
								Message: "quoted stats aggregate input is invalid",
								Range:   aggregate.InputRange,
							}
						}
					} else if !spl.IsExactUnquotedFieldName(aggregate.Input) {
						return &Diagnostic{
							Code:    "SPL_UNSUPPORTED_STATS_AGGREGATE",
							Message: "stats aggregate input is not an exact unquoted field",
							Range:   aggregate.InputRange,
						}
					}
					input, inputErr := resolveStatsInputField(
						aggregate.Input,
						aggregate.InputRange,
						aggregate.InputQuoted,
					)
					if inputErr != nil {
						return inputErr
					}
					measure.Input = input
				}
				if statsAggregateUsesPercentileSuffix(aggregate.Function) {
					measure.Percentile = aggregate.Percentile
				}
			}
		}
		for _, source := range seenSources {
			if sameStatsAggregateSource(source, measure) {
				return &Diagnostic{
					Code:    "SPL_DUPLICATE_STATS_AGGREGATE",
					Message: "the same stats aggregate source cannot be renamed to multiple output fields",
					Range:   aggregate.Range,
				}
			}
		}
		seenSources = append(seenSources, measure)
		measures = append(measures, measure)
		outputFields = append(outputFields, aggregate.Alias)
	}
	result.OutputFields = outputFields
	result.Operators = append(result.Operators, &Aggregate{
		GroupBy:      groupBy,
		Measures:     measures,
		StatsOptions: statsOptions,
		Range:        command.Range,
	})
	return nil
}
