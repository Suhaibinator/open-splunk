package plan

import (
	"errors"
	"fmt"
	"unicode/utf8"

	"github.com/Suhaibinator/open-splunk/internal/nilcheck"
	"github.com/Suhaibinator/open-splunk/internal/spl"
)

func statsAggregateUsesPercentileSuffix(function spl.AggregateFunction) bool {
	switch function {
	case spl.AggregateFunctionPercentile,
		spl.AggregateFunctionExactPercentile,
		spl.AggregateFunctionUpperPercentile:
		return true
	default:
		return false
	}
}

func buildStatsSparklineMeasure(
	spec *spl.StatsSparkline,
) (*SparklineMeasure, error) {
	if spec == nil || spec.Range == (spl.Range{}) {
		return nil, &Diagnostic{
			Code:    "SPL_UNSUPPORTED_STATS_AGGREGATE",
			Message: "stats sparkline metadata is missing",
		}
	}
	span, spanErr := convertStatsSparklineSpan(spec.Span, spec.Range)
	if spanErr != nil {
		return nil, spanErr
	}
	function, requiresInput, supported := convertStatsSparklineFunction(spec.Function)
	if !supported {
		return nil, &Diagnostic{
			Code:    "SPL_UNSUPPORTED_STATS_AGGREGATE",
			Message: "unsupported aggregate inside stats sparkline",
			Range:   spec.Range,
		}
	}
	hasInput := spec.Input != "" || spec.InputQuoted ||
		spec.InputRange != (spl.Range{}) || spec.InputGlob != nil
	if spec.InputGlob != nil {
		return nil, &Diagnostic{
			Code:    "SPL_UNSUPPORTED_STATS_AGGREGATE",
			Message: "stats sparkline contains unexpanded wc-field metadata",
			Range:   spec.InputGlob.Range,
		}
	}
	if hasInput != requiresInput ||
		(hasInput && (spec.Input == "" || spec.InputRange == (spl.Range{}))) {
		return nil, &Diagnostic{
			Code:    "SPL_UNSUPPORTED_STATS_AGGREGATE",
			Message: "stats sparkline inner aggregate contains invalid input metadata",
			Range:   spec.Range,
		}
	}
	var input FieldRef
	if hasInput {
		if spec.InputQuoted {
			if !spl.IsStatsLiteralFieldReference(spec.Input) {
				return nil, &Diagnostic{
					Code:    "SPL_INVALID_FIELD",
					Message: "quoted stats sparkline input is invalid",
					Range:   spec.InputRange,
				}
			}
		} else if !spl.IsExactUnquotedFieldName(spec.Input) {
			return nil, &Diagnostic{
				Code:    "SPL_UNSUPPORTED_STATS_AGGREGATE",
				Message: "stats sparkline input is not an exact unquoted field",
				Range:   spec.InputRange,
			}
		}
		var inputErr error
		input, inputErr = resolveStatsInputField(
			spec.Input,
			spec.InputRange,
			spec.InputQuoted,
		)
		if inputErr != nil {
			return nil, inputErr
		}
	}
	timeField, timeErr := ResolveField("_time", spec.Range)
	if timeErr != nil {
		return nil, timeErr
	}
	return &SparklineMeasure{
		Function:      function,
		Input:         input,
		Time:          timeField,
		Span:          span,
		MaximumPoints: spl.MaximumStatsSparklinePoints,
	}, nil
}

func convertStatsSparklineFunction(
	function spl.AggregateFunction,
) (AggregateFunction, bool, bool) {
	switch function {
	case spl.AggregateFunctionCount:
		return AggregateFunctionCountRows, false, true
	case spl.AggregateFunctionCountValues:
		return AggregateFunctionCountValues, true, true
	case spl.AggregateFunctionDistinctCount:
		return AggregateFunctionDistinctCount, true, true
	case spl.AggregateFunctionAverage:
		return AggregateFunctionAverage, true, true
	case spl.AggregateFunctionStandardDeviationSample:
		return AggregateFunctionStandardDeviationSample, true, true
	case spl.AggregateFunctionStandardDeviationPopulation:
		return AggregateFunctionStandardDeviationPopulation, true, true
	case spl.AggregateFunctionVarianceSample:
		return AggregateFunctionVarianceSample, true, true
	case spl.AggregateFunctionVariancePopulation:
		return AggregateFunctionVariancePopulation, true, true
	case spl.AggregateFunctionSum:
		return AggregateFunctionSum, true, true
	case spl.AggregateFunctionSumSquares:
		return AggregateFunctionSumSquares, true, true
	case spl.AggregateFunctionMinimum:
		return AggregateFunctionMinimum, true, true
	case spl.AggregateFunctionMaximum:
		return AggregateFunctionMaximum, true, true
	case spl.AggregateFunctionRange:
		return AggregateFunctionRange, true, true
	default:
		return AggregateFunctionInvalid, false, false
	}
}

func convertStatsSparklineSpan(
	span spl.SparklineSpan,
	sourceRange spl.Range,
) (SparklineSpan, error) {
	invalid := func(message string) (SparklineSpan, error) {
		return SparklineSpan{}, &Diagnostic{
			Code:    "SPL_UNSUPPORTED_STATS_AGGREGATE",
			Message: message,
			Range:   sourceRange,
		}
	}
	switch span.Kind {
	case spl.SparklineSpanKindAutomatic:
		if span.Magnitude != 0 || span.Unit != spl.SparklineSpanUnitInvalid ||
			span.Range != (spl.Range{}) {
			return invalid("automatic stats sparkline span contains explicit metadata")
		}
		return SparklineSpan{Kind: SparklineSpanKindAutomatic}, nil
	case spl.SparklineSpanKindExplicit:
		if span.Magnitude == 0 || span.Range == (spl.Range{}) {
			return invalid("explicit stats sparkline span metadata is incomplete")
		}
		unit, unitsPerSecond, supported := convertStatsSparklineSpanUnit(span.Unit)
		if !supported {
			return invalid("explicit stats sparkline span unit is invalid")
		}
		if unitsPerSecond != 0 &&
			(span.Magnitude >= unitsPerSecond || unitsPerSecond%span.Magnitude != 0) {
			return invalid("stats sparkline subsecond span must divide one second evenly")
		}
		return SparklineSpan{
			Kind:      SparklineSpanKindExplicit,
			Magnitude: span.Magnitude,
			Unit:      unit,
		}, nil
	default:
		return invalid("stats sparkline span kind is invalid")
	}
}

func convertStatsSparklineSpanUnit(
	unit spl.SparklineSpanUnit,
) (SparklineSpanUnit, uint64, bool) {
	switch unit {
	case spl.SparklineSpanUnitMicrosecond:
		return SparklineSpanUnitMicrosecond, 1_000_000, true
	case spl.SparklineSpanUnitMillisecond:
		return SparklineSpanUnitMillisecond, 1_000, true
	case spl.SparklineSpanUnitCentisecond:
		return SparklineSpanUnitCentisecond, 100, true
	case spl.SparklineSpanUnitDecisecond:
		return SparklineSpanUnitDecisecond, 10, true
	case spl.SparklineSpanUnitSecond:
		return SparklineSpanUnitSecond, 0, true
	case spl.SparklineSpanUnitMinute:
		return SparklineSpanUnitMinute, 0, true
	case spl.SparklineSpanUnitHour:
		return SparklineSpanUnitHour, 0, true
	case spl.SparklineSpanUnitDay:
		return SparklineSpanUnitDay, 0, true
	case spl.SparklineSpanUnitMonth:
		return SparklineSpanUnitMonth, 0, true
	default:
		return SparklineSpanUnitInvalid, 0, false
	}
}

func buildStatsOptions(
	options spl.StatsOptions,
	commandRange spl.Range,
) (*StatsOptions, error) {
	invalid := func(message string, sourceRange spl.Range) (*StatsOptions, error) {
		if sourceRange == (spl.Range{}) {
			sourceRange = commandRange
		}
		return nil, &Diagnostic{
			Code:    "SPL_UNSUPPORTED_STATS_SYNTAX",
			Message: message,
			Range:   sourceRange,
		}
	}

	if options.PartitionsSpecified {
		if options.PartitionsRange == (spl.Range{}) {
			return invalid("stats partitions metadata is invalid", options.PartitionsRange)
		}
	} else if options.Partitions != 0 || options.PartitionsRange != (spl.Range{}) {
		return invalid("unspecified stats partitions contains authored metadata", options.PartitionsRange)
	}
	if options.AllNumericSpecified {
		if options.AllNumericRange == (spl.Range{}) {
			return invalid("stats allnum metadata is invalid", options.AllNumericRange)
		}
	} else if options.AllNumeric || options.AllNumericRange != (spl.Range{}) {
		return invalid("unspecified stats allnum contains authored metadata", options.AllNumericRange)
	}
	if options.DelimiterSpecified {
		if options.DelimiterRange == (spl.Range{}) ||
			!utf8.ValidString(options.Delimiter) {
			return invalid("stats delim metadata is invalid", options.DelimiterRange)
		}
	} else if options.Delimiter != "" || options.DelimiterRange != (spl.Range{}) {
		return invalid("unspecified stats delim contains authored metadata", options.DelimiterRange)
	}
	if options.DeduplicateSplitValuesSpecified {
		if options.DeduplicateSplitValuesRange == (spl.Range{}) {
			return invalid(
				"stats dedup_splitvals metadata is invalid",
				options.DeduplicateSplitValuesRange,
			)
		}
	} else if options.DeduplicateSplitValues ||
		options.DeduplicateSplitValuesRange != (spl.Range{}) {
		return invalid(
			"unspecified stats dedup_splitvals contains authored metadata",
			options.DeduplicateSplitValuesRange,
		)
	}

	partitions := spl.DefaultStatsPartitions
	if options.Partitions > uint64(spl.MaximumStatsPartitions) {
		partitions = spl.MaximumStatsPartitions
	} else if options.Partitions > 0 {
		partitions = uint8(options.Partitions)
	}
	delimiter := spl.DefaultStatsDelimiter
	if options.DelimiterSpecified {
		delimiter = options.Delimiter
	}
	return &StatsOptions{
		Partitions:             partitions,
		AllNumeric:             options.AllNumeric,
		Delimiter:              delimiter,
		DeduplicateSplitValues: options.DeduplicateSplitValues,
	}, nil
}

func statsAggregateRequiresCanonicalTime(function spl.AggregateFunction) bool {
	switch function {
	case spl.AggregateFunctionEarliest,
		spl.AggregateFunctionLatest,
		spl.AggregateFunctionEarliestTime,
		spl.AggregateFunctionLatestTime,
		spl.AggregateFunctionRate:
		return true
	default:
		return false
	}
}

// convertNamedAggregateFunction maps one spl aggregate onto its plan
// counterpart for the command paths that also accept the bare count(field)
// form. stats itself does not, so convertStatsFieldAggregateFunction stays
// narrower. Callers restrict the input through their own outer case lists and
// receive AggregateFunctionInvalid for anything outside that set.
func convertNamedAggregateFunction(function spl.AggregateFunction) AggregateFunction {
	if function == spl.AggregateFunctionCountValues {
		return AggregateFunctionCountValues
	}
	converted, _ := convertStatsFieldAggregateFunction(function)
	return converted
}

func convertStatsFieldAggregateFunction(
	function spl.AggregateFunction,
) (AggregateFunction, bool) {
	switch function {
	case spl.AggregateFunctionPercentile:
		return AggregateFunctionPercentile, true
	case spl.AggregateFunctionExactPercentile:
		return AggregateFunctionExactPercentile, true
	case spl.AggregateFunctionUpperPercentile:
		return AggregateFunctionUpperPercentile, true
	case spl.AggregateFunctionMedian:
		return AggregateFunctionMedian, true
	case spl.AggregateFunctionSum:
		return AggregateFunctionSum, true
	case spl.AggregateFunctionAverage:
		return AggregateFunctionAverage, true
	case spl.AggregateFunctionRange:
		return AggregateFunctionRange, true
	case spl.AggregateFunctionSumSquares:
		return AggregateFunctionSumSquares, true
	case spl.AggregateFunctionStandardDeviationSample:
		return AggregateFunctionStandardDeviationSample, true
	case spl.AggregateFunctionStandardDeviationPopulation:
		return AggregateFunctionStandardDeviationPopulation, true
	case spl.AggregateFunctionVarianceSample:
		return AggregateFunctionVarianceSample, true
	case spl.AggregateFunctionVariancePopulation:
		return AggregateFunctionVariancePopulation, true
	case spl.AggregateFunctionDistinctCount:
		return AggregateFunctionDistinctCount, true
	case spl.AggregateFunctionEstimatedDistinctCount:
		return AggregateFunctionEstimatedDistinctCount, true
	case spl.AggregateFunctionEstimatedDistinctCountError:
		return AggregateFunctionEstimatedDistinctCountError, true
	case spl.AggregateFunctionValues:
		return AggregateFunctionValues, true
	case spl.AggregateFunctionList:
		return AggregateFunctionList, true
	case spl.AggregateFunctionMinimum:
		return AggregateFunctionMinimum, true
	case spl.AggregateFunctionMaximum:
		return AggregateFunctionMaximum, true
	case spl.AggregateFunctionMode:
		return AggregateFunctionMode, true
	case spl.AggregateFunctionFirst:
		return AggregateFunctionFirst, true
	case spl.AggregateFunctionLast:
		return AggregateFunctionLast, true
	case spl.AggregateFunctionEarliest:
		return AggregateFunctionEarliest, true
	case spl.AggregateFunctionLatest:
		return AggregateFunctionLatest, true
	case spl.AggregateFunctionEarliestTime:
		return AggregateFunctionEarliestTime, true
	case spl.AggregateFunctionLatestTime:
		return AggregateFunctionLatestTime, true
	case spl.AggregateFunctionRate:
		return AggregateFunctionRate, true
	default:
		return AggregateFunctionInvalid, false
	}
}

func buildEventStatsFieldMeasure(
	aggregate spl.StatsAggregate,
	function AggregateFunction,
	form string,
) (AggregateMeasure, error) {
	percentile := uint8(0)
	if function == AggregateFunctionPercentile {
		if aggregate.Percentile < 1 || aggregate.Percentile > 99 {
			return AggregateMeasure{}, &Diagnostic{
				Code: "SPL_UNSUPPORTED_EVENTSTATS_AGGREGATE",
				Message: "eventstats percentile suffix must be an integer " +
					"from 1 through 99",
				Range: aggregate.Range,
			}
		}
		percentile = aggregate.Percentile
	} else if aggregate.Percentile != 0 {
		return AggregateMeasure{}, &Diagnostic{
			Code: "SPL_UNSUPPORTED_EVENTSTATS_AGGREGATE",
			Message: "non-percentile eventstats aggregate contains " +
				"percentile metadata",
			Range: aggregate.Range,
		}
	}
	if aggregate.Input == "" ||
		aggregate.InputRange == (spl.Range{}) ||
		aggregate.InputGlob != nil || aggregate.AliasGlob != nil ||
		aggregate.InputExpression != nil ||
		aggregate.Predicate != nil ||
		!aggregate.ExplicitAlias {
		return AggregateMeasure{}, &Diagnostic{
			Code: "SPL_UNSUPPORTED_EVENTSTATS_AGGREGATE",
			Message: "eventstats " + form + " requires one exact " +
				"input field and an explicit alias",
			Range: aggregate.Range,
		}
	}
	input, err := ResolveField(aggregate.Input, aggregate.InputRange)
	if err != nil {
		return AggregateMeasure{}, err
	}
	return AggregateMeasure{
		Function:   function,
		Input:      input,
		Output:     aggregate.Alias,
		Percentile: percentile,
	}, nil
}

type countPredicateMeasureDiagnostics struct {
	unsupportedCode    string
	invalidMessage     string
	ambiguousCode      string
	reservedMessage    string
	allowImplicitAlias bool
}

// buildCountPredicateMeasure converts the common count(eval(...)) contract
// while leaving command-specific diagnostics at the aggregate-command callers.
func buildCountPredicateMeasure(
	aggregate spl.StatsAggregate,
	outputSchemaKnown bool,
	budget *splExpressionResourceBudget,
	diagnostics countPredicateMeasureDiagnostics,
) (AggregateMeasure, error) {
	if aggregate.Input != "" ||
		aggregate.InputGlob != nil || aggregate.AliasGlob != nil ||
		aggregate.InputQuoted ||
		aggregate.InputRange != (spl.Range{}) ||
		aggregate.InputExpression != nil ||
		aggregate.Percentile != 0 ||
		(!aggregate.ExplicitAlias && !diagnostics.allowImplicitAlias) ||
		nilcheck.IsNil(aggregate.Predicate) {
		return AggregateMeasure{}, &Diagnostic{
			Code:    diagnostics.unsupportedCode,
			Message: diagnostics.invalidMessage,
			Range:   aggregate.Range,
		}
	}
	predicate, err := convertWhereExpression(aggregate.Predicate, budget)
	if err != nil {
		return AggregateMeasure{}, err
	}
	if !outputSchemaKnown {
		fieldRange, referencesReserved, inventoryErr := predicateFieldRange(predicate)
		if inventoryErr != nil {
			return AggregateMeasure{}, inventoryErr
		}
		if referencesReserved {
			return AggregateMeasure{}, &Diagnostic{
				Code:    diagnostics.ambiguousCode,
				Message: diagnostics.reservedMessage,
				Range:   fieldRange,
			}
		}
	}
	return AggregateMeasure{
		Function:      AggregateFunctionCountPredicate,
		Predicate:     predicate,
		Output:        aggregate.Alias,
		OutputLiteral: aggregate.AliasQuoted || aggregate.AliasSourceDerived,
	}, nil
}

// reservedFieldsPayloadName is the open-event reserved payload field that
// conditional aggregate planning must never let a predicate read.
const reservedFieldsPayloadName = "fields"

// predicateFieldRange finds the first reference to the reserved fields payload
// in deterministic expression order. Conditional aggregate planning uses it to
// preserve the reserved open-event "fields" payload boundary just as it does
// for exact inputs.
func predicateFieldRange(
	expression Expression,
) (spl.Range, bool, error) {
	var visitExpression func(Expression) (spl.Range, bool, error)
	var visitScalar func(ScalarExpression) (spl.Range, bool, error)
	visitScalar = func(expression ScalarExpression) (spl.Range, bool, error) {
		switch expression := expression.(type) {
		case *ScalarFieldExpression:
			if expression == nil {
				return spl.Range{}, false, errors.New("inspect predicate fields: scalar field expression is nil")
			}
			if expression.Field.Name == reservedFieldsPayloadName {
				return expression.Field.Range, true, nil
			}
			return spl.Range{}, false, nil
		case *ScalarLiteralExpression:
			if expression == nil {
				return spl.Range{}, false, errors.New("inspect predicate fields: scalar literal expression is nil")
			}
			return spl.Range{}, false, nil
		case *ScalarUnaryExpression:
			if expression == nil {
				return spl.Range{}, false, errors.New("inspect predicate fields: scalar unary expression is nil")
			}
			return visitScalar(expression.Operand)
		case *ScalarBinaryExpression:
			if expression == nil {
				return spl.Range{}, false, errors.New("inspect predicate fields: scalar binary expression is nil")
			}
			if sourceRange, ok, err := visitScalar(expression.Left); ok || err != nil {
				return sourceRange, ok, err
			}
			return visitScalar(expression.Right)
		case *ScalarCallExpression:
			if expression == nil {
				return spl.Range{}, false, errors.New("inspect predicate fields: scalar call expression is nil")
			}
			for _, argument := range expression.Arguments {
				if sourceRange, ok, err := visitScalar(argument); ok || err != nil {
					return sourceRange, ok, err
				}
			}
			return spl.Range{}, false, nil
		case *ScalarIfExpression:
			if expression == nil {
				return spl.Range{}, false, errors.New("inspect predicate fields: scalar if expression is nil")
			}
			if sourceRange, ok, err := visitExpression(expression.Condition); ok || err != nil {
				return sourceRange, ok, err
			}
			if sourceRange, ok, err := visitScalar(expression.True); ok || err != nil {
				return sourceRange, ok, err
			}
			return visitScalar(expression.False)
		case *ScalarCaseExpression:
			if expression == nil {
				return spl.Range{}, false, errors.New("inspect predicate fields: scalar case expression is nil")
			}
			for _, branch := range expression.Branches {
				if sourceRange, ok, err := visitExpression(branch.Condition); ok || err != nil {
					return sourceRange, ok, err
				}
				if sourceRange, ok, err := visitScalar(branch.Value); ok || err != nil {
					return sourceRange, ok, err
				}
			}
			return spl.Range{}, false, nil
		default:
			return spl.Range{}, false, fmt.Errorf(
				"inspect predicate fields: unsupported scalar expression %T",
				expression,
			)
		}
	}
	visitExpression = func(expression Expression) (spl.Range, bool, error) {
		switch expression := expression.(type) {
		case *BooleanExpression:
			if expression == nil {
				return spl.Range{}, false, errors.New("inspect predicate fields: Boolean expression is nil")
			}
			if sourceRange, ok, err := visitExpression(expression.Left); ok || err != nil {
				return sourceRange, ok, err
			}
			return visitExpression(expression.Right)
		case *NotExpression:
			if expression == nil {
				return spl.Range{}, false, errors.New("inspect predicate fields: not expression is nil")
			}
			return visitExpression(expression.Operand)
		case *TextExpression:
			if expression == nil {
				return spl.Range{}, false, errors.New("inspect predicate fields: text expression is nil")
			}
			return spl.Range{}, false, nil
		case *ComparisonExpression:
			if expression == nil {
				return spl.Range{}, false, errors.New("inspect predicate fields: comparison expression is nil")
			}
			if expression.Field.Name == reservedFieldsPayloadName {
				return expression.Field.Range, true, nil
			}
			return spl.Range{}, false, nil
		case *EvalComparisonExpression:
			if expression == nil {
				return spl.Range{}, false, errors.New("inspect predicate fields: eval comparison expression is nil")
			}
			if sourceRange, ok, err := visitScalar(expression.Left); ok || err != nil {
				return sourceRange, ok, err
			}
			return visitScalar(expression.Right)
		case *ScalarPredicateExpression:
			if expression == nil {
				return spl.Range{}, false, errors.New("inspect predicate fields: scalar predicate expression is nil")
			}
			return visitScalar(expression.Value)
		case *MembershipExpression:
			if expression == nil {
				return spl.Range{}, false, errors.New("inspect predicate fields: membership expression is nil")
			}
			if sourceRange, ok, err := visitScalar(expression.Value); ok || err != nil {
				return sourceRange, ok, err
			}
			for _, candidate := range expression.Candidates {
				if sourceRange, ok, err := visitScalar(candidate); ok || err != nil {
					return sourceRange, ok, err
				}
			}
			return spl.Range{}, false, nil
		default:
			return spl.Range{}, false, fmt.Errorf(
				"inspect predicate fields: unsupported expression %T",
				expression,
			)
		}
	}
	return visitExpression(expression)
}
