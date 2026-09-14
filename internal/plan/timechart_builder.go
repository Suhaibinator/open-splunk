package plan

import (
	"fmt"
	"math"
	"math/big"
	"strconv"
	"time"

	"fortio.org/safecast"

	"github.com/Suhaibinator/open-splunk/internal/spl"
)

// buildTimechartSplit applies Splunk's series defaults (limit=10,
// useother=true, usenull=true) over the authored options. The parser owns the
// limit bound, but a forged command can carry any value, so the planner
// revalidates it before the compiler trusts SeriesLimit.
func buildTimechartSplit(
	field FieldRef,
	options spl.TimechartOptions,
	commandRange spl.Range,
) (*TimechartSplit, error) {
	invalid := func(message string, sourceRange spl.Range) (*TimechartSplit, error) {
		if sourceRange == (spl.Range{}) {
			sourceRange = commandRange
		}
		return nil, &Diagnostic{
			Code:    "SPL_UNSUPPORTED_TIMECHART_SYNTAX",
			Message: message,
			Range:   sourceRange,
		}
	}
	split := &TimechartSplit{
		Field:        field,
		SeriesLimit:  timechartSeriesLimit,
		IncludeNull:  true,
		IncludeOther: true,
		NullLabel:    "NULL",
		OtherLabel:   "OTHER",
	}
	if options.LimitSpecified {
		if options.LimitRange == (spl.Range{}) {
			return invalid("timechart limit metadata is invalid", options.LimitRange)
		}
		split.SeriesLimit = options.Limit
	} else if options.Limit != 0 || options.LimitRange != (spl.Range{}) {
		return invalid("unspecified timechart limit contains authored metadata", options.LimitRange)
	}
	if options.UseOtherSpecified {
		if options.UseOtherRange == (spl.Range{}) {
			return invalid("timechart useother metadata is invalid", options.UseOtherRange)
		}
		split.IncludeOther = options.UseOther
	} else if options.UseOther || options.UseOtherRange != (spl.Range{}) {
		return invalid("unspecified timechart useother contains authored metadata", options.UseOtherRange)
	}
	if options.UseNullSpecified {
		if options.UseNullRange == (spl.Range{}) {
			return invalid("timechart usenull metadata is invalid", options.UseNullRange)
		}
		split.IncludeNull = options.UseNull
	} else if options.UseNull || options.UseNullRange != (spl.Range{}) {
		return invalid("unspecified timechart usenull contains authored metadata", options.UseNullRange)
	}
	return split, nil
}

// timechartMaxSeries is the runtime series allowance a split publishes: the
// ordinary series limit plus each enabled NULL and OTHER sentinel series.
func timechartMaxSeries(split *TimechartSplit) uint64 {
	if split.SeriesLimit == 0 {
		return 0
	}
	series := split.SeriesLimit
	if split.IncludeNull && series != math.MaxUint64 {
		series++
	}
	if split.IncludeOther && series != math.MaxUint64 {
		series++
	}
	return series
}

func buildChartMeasure(
	command *spl.ChartCommand,
	outputSchemaKnown bool,
) (AggregateMeasure, error) {
	if command == nil {
		return AggregateMeasure{}, &Diagnostic{
			Code:    "SPL_INVALID_QUERY",
			Message: "chart command is nil",
		}
	}
	aggregate := command.Aggregate
	invalid := func(message string) (AggregateMeasure, error) {
		return AggregateMeasure{}, &Diagnostic{
			Code:    "SPL_UNSUPPORTED_CHART_AGGREGATE",
			Message: message,
			Range:   aggregate.Range,
		}
	}
	if aggregate.Range == (spl.Range{}) || aggregate.AliasRange == (spl.Range{}) ||
		aggregate.Sparkline != nil ||
		aggregate.InputGlob != nil || aggregate.AliasGlob != nil ||
		aggregate.Predicate != nil || aggregate.InputExpression != nil ||
		aggregate.InputQuoted || aggregate.AliasQuoted ||
		aggregate.AliasSourceDerived || aggregate.AliasWildcardDerived {
		return invalid("chart aggregate metadata is invalid")
	}

	switch aggregate.Function {
	case spl.AggregateFunctionCount:
		if aggregate.Input != "" || aggregate.InputRange != (spl.Range{}) ||
			aggregate.Percentile != 0 || aggregate.Alias != "count" ||
			aggregate.ExplicitAlias {
			return invalid("chart count must be argument-free and unaliased")
		}
		return AggregateMeasure{
			Function: AggregateFunctionCountRows,
			Output:   "count",
		}, nil
	case spl.AggregateFunctionCountValues,
		spl.AggregateFunctionPercentile,
		spl.AggregateFunctionSum,
		spl.AggregateFunctionAverage:
		if aggregate.Function == spl.AggregateFunctionPercentile &&
			(aggregate.Percentile < 1 || aggregate.Percentile > 99) {
			return invalid("chart percentile must be from 1 through 99")
		}
		function := convertNamedAggregateFunction(aggregate.Function)
		canonicalName, _ := canonicalAggregateName(function, aggregate.Percentile)
		canonicalOutput := canonicalName + "(" + aggregate.Input + ")"
		if aggregate.Input == "" || aggregate.InputRange == (spl.Range{}) ||
			(function != AggregateFunctionPercentile && aggregate.Percentile != 0) ||
			aggregate.ExplicitAlias ||
			aggregate.Alias != canonicalOutput {
			return invalid("chart field aggregates require one exact input field and no alias")
		}
		if !spl.IsExactUnquotedFieldName(aggregate.Input) {
			return invalid("chart field aggregates require one exact unquoted input field")
		}
		input, err := ResolveField(aggregate.Input, aggregate.InputRange)
		if err != nil {
			return AggregateMeasure{}, err
		}
		if !outputSchemaKnown && input.Name == "fields" {
			return AggregateMeasure{}, &Diagnostic{
				Code:    "SPL_AMBIGUOUS_CHART_FIELD",
				Message: "chart cannot read the event result's reserved fields payload without an exact upstream schema",
				Range:   aggregate.InputRange,
				Suggestions: []string{
					"select an exact ordinary field with table before chart",
					"produce a closed stats schema before charting fields",
				},
			}
		}
		return AggregateMeasure{
			Function:   function,
			Input:      input,
			Percentile: aggregate.Percentile,
			Output:     canonicalOutput,
		}, nil
	default:
		return invalid("unsupported chart aggregate")
	}
}

func buildTimechartMeasure(
	command *spl.TimechartCommand,
	outputSchemaKnown bool,
) (AggregateMeasure, error) {
	if command == nil {
		return AggregateMeasure{}, &Diagnostic{
			Code:    "SPL_INVALID_QUERY",
			Message: "timechart command is nil",
		}
	}
	aggregate := command.Aggregate
	if aggregate.Sparkline != nil ||
		aggregate.InputGlob != nil || aggregate.AliasGlob != nil ||
		aggregate.Predicate != nil || aggregate.InputExpression != nil ||
		(aggregate.InputQuoted && !outputSchemaKnown) || aggregate.AliasQuoted ||
		aggregate.AliasSourceDerived || aggregate.AliasWildcardDerived {
		return AggregateMeasure{}, &Diagnostic{
			Code:    "SPL_UNSUPPORTED_TIMECHART_AGGREGATE",
			Message: "timechart aggregate cannot contain predicate or scalar-expression metadata",
			Range:   aggregate.Range,
		}
	}
	switch aggregate.Function {
	case spl.AggregateFunctionCount:
		if aggregate.Input != "" ||
			aggregate.InputRange != (spl.Range{}) ||
			aggregate.Percentile != 0 ||
			aggregate.Alias != "count" ||
			aggregate.ExplicitAlias {
			return AggregateMeasure{}, &Diagnostic{
				Code: "SPL_UNSUPPORTED_TIMECHART_AGGREGATE",
				Message: "timechart count must be argument-free and use its " +
					"unaliased count output",
				Range: aggregate.Range,
			}
		}
		return AggregateMeasure{
			Function: AggregateFunctionCountRows,
			Output:   "count",
		}, nil
	case spl.AggregateFunctionPercentile:
		if aggregate.Input == "" ||
			aggregate.InputRange == (spl.Range{}) ||
			aggregate.Percentile < 1 ||
			aggregate.Percentile > 99 ||
			aggregate.Alias == "" {
			return AggregateMeasure{}, &Diagnostic{
				Code: "SPL_UNSUPPORTED_TIMECHART_AGGREGATE",
				Message: "timechart percentile requires one exact input field, " +
					"an integer level from 1 through 99, and one output",
				Range: aggregate.Range,
			}
		}
		canonicalOutput := "perc" + strconv.Itoa(int(aggregate.Percentile)) +
			"(" + aggregate.Input + ")"
		return buildTimechartFieldMeasure(
			command,
			AggregateFunctionPercentile,
			canonicalOutput,
			aggregate.Percentile,
			outputSchemaKnown,
		)
	case spl.AggregateFunctionCountValues, spl.AggregateFunctionSum,
		spl.AggregateFunctionAverage:
		if aggregate.Input == "" ||
			aggregate.InputRange == (spl.Range{}) ||
			aggregate.Percentile != 0 ||
			aggregate.Alias == "" {
			return AggregateMeasure{}, &Diagnostic{
				Code: "SPL_UNSUPPORTED_TIMECHART_AGGREGATE",
				Message: "timechart field aggregate requires one exact input " +
					"field, no percentile metadata, and one output",
				Range: aggregate.Range,
			}
		}
		function := convertNamedAggregateFunction(aggregate.Function)
		canonicalName, _ := canonicalAggregateName(function, aggregate.Percentile)
		return buildTimechartFieldMeasure(
			command,
			function,
			canonicalName+"("+aggregate.Input+")",
			0,
			outputSchemaKnown,
		)
	default:
		return AggregateMeasure{}, &Diagnostic{
			Code:    "SPL_UNSUPPORTED_TIMECHART_AGGREGATE",
			Message: "unsupported timechart aggregate",
			Range:   aggregate.Range,
		}
	}
}

func buildTimechartFieldMeasure(
	command *spl.TimechartCommand,
	function AggregateFunction,
	canonicalOutput string,
	percentile uint8,
	outputSchemaKnown bool,
) (AggregateMeasure, error) {
	aggregate := command.Aggregate
	if !aggregate.ExplicitAlias && aggregate.Alias != canonicalOutput {
		return AggregateMeasure{}, &Diagnostic{
			Code:    "SPL_UNSUPPORTED_TIMECHART_AGGREGATE",
			Message: "unaliased timechart aggregate output must use its canonical name",
			Range:   aggregate.Range,
		}
	}
	if !outputSchemaKnown && aggregate.Input == "fields" {
		return AggregateMeasure{}, &Diagnostic{
			Code:    "SPL_AMBIGUOUS_TIMECHART_FIELD",
			Message: "timechart cannot read the event result's reserved fields payload without an exact upstream schema",
			Range:   aggregate.InputRange,
		}
	}
	input, inputErr := resolveStatsInputField(aggregate.Input, aggregate.InputRange, aggregate.InputQuoted)
	if inputErr != nil {
		return AggregateMeasure{}, inputErr
	}
	if _, outputErr := ResolveField(aggregate.Alias, aggregate.AliasRange); outputErr != nil {
		return AggregateMeasure{}, outputErr
	}
	if aggregate.Alias == "_time" {
		return AggregateMeasure{}, &Diagnostic{
			Code:    "SPL_DUPLICATE_FIELD",
			Message: "timechart aggregate output collides with the _time axis",
			Range:   aggregate.AliasRange,
		}
	}
	return AggregateMeasure{
		Function:   function,
		Input:      input,
		Percentile: percentile,
		Output:     aggregate.Alias,
	}, nil
}

func validateTimechartAxisOptions(
	axis spl.TimechartAxisOptions,
	sourceRange spl.Range,
) (uint64, error) {
	for _, option := range []struct {
		name             string
		value, specified bool
		location         spl.Range
	}{
		{"cont", axis.Cont, axis.ContSpecified, axis.ContRange},
		{"partial", axis.Partial, axis.PartialSpecified, axis.PartialRange},
		{"fixedrange", axis.FixedRange, axis.FixedRangeSpecified, axis.FixedRangeRange},
	} {
		if (option.specified && option.location == (spl.Range{})) || (!option.specified && (option.value || option.location != (spl.Range{}))) {
			return 0, &Diagnostic{Code: "SPL_UNSUPPORTED_TIMECHART_SYNTAX", Message: "timechart " + option.name + " option metadata is invalid", Range: sourceRange}
		}
	}
	bins := uint64(spl.DefaultTimechartBins)
	if axis.BinsSpecified {
		if axis.Bins == 0 || axis.Bins > spl.MaximumTimechartBins ||
			axis.BinsRange == (spl.Range{}) {
			return 0, &Diagnostic{
				Code:    "SPL_UNSUPPORTED_TIMECHART_BINS",
				Message: fmt.Sprintf("timechart bins must be from 1 through %d", spl.MaximumTimechartBins),
				Range:   axis.BinsRange,
			}
		}
		bins = axis.Bins
	} else if axis.Bins != 0 || axis.BinsRange != (spl.Range{}) {
		return 0, &Diagnostic{
			Code:    "SPL_UNSUPPORTED_TIMECHART_SYNTAX",
			Message: "unspecified timechart bins contains authored metadata",
			Range:   sourceRange,
		}
	}
	if axis.MinSpanSpecified {
		if axis.MinSpan == (spl.TimeSpan{}) || axis.MinSpan.Range == (spl.Range{}) {
			return 0, &Diagnostic{
				Code:    "SPL_UNSUPPORTED_TIMECHART_SYNTAX",
				Message: "timechart minspan metadata is invalid",
				Range:   sourceRange,
			}
		}
		if _, ok := nominalTimeSpanNanoseconds(axis.MinSpan); !ok {
			return 0, &Diagnostic{
				Code:    "SPL_UNSUPPORTED_TIMECHART_SYNTAX",
				Message: "timechart minspan is outside the automatic span range",
				Range:   axis.MinSpan.Range,
			}
		}
	} else if axis.MinSpan != (spl.TimeSpan{}) {
		return 0, &Diagnostic{
			Code:    "SPL_UNSUPPORTED_TIMECHART_SYNTAX",
			Message: "unspecified timechart minspan contains authored metadata",
			Range:   sourceRange,
		}
	}

	return bins, nil
}

func automaticTimeSpanAsSPL(span AutomaticTimeSpan, sourceRange spl.Range) spl.TimeSpan {
	unit := spl.TimeSpanUnitInvalid
	switch span.Unit {
	case AutomaticTimeSpanUnitSecond:
		unit = spl.TimeSpanUnitSecond
	case AutomaticTimeSpanUnitMinute:
		unit = spl.TimeSpanUnitMinute
	case AutomaticTimeSpanUnitHour:
		unit = spl.TimeSpanUnitHour
	case AutomaticTimeSpanUnitDay:
		unit = spl.TimeSpanUnitDay
	case AutomaticTimeSpanUnitMonth:
		unit = spl.TimeSpanUnitMonth
	}
	return spl.TimeSpan{Magnitude: span.Magnitude, Unit: unit, Range: sourceRange}
}

func automaticTimeSpanAtLeast(candidate, minimum spl.TimeSpan) bool {
	candidateUnits, candidateOK := nominalTimeSpanNanoseconds(candidate)
	minimumUnits, minimumOK := nominalTimeSpanNanoseconds(minimum)
	return candidateOK && minimumOK && candidateUnits.Cmp(minimumUnits) >= 0
}

func nominalTimeSpanNanoseconds(span spl.TimeSpan) (*big.Int, bool) {
	magnitude := new(big.Int).SetUint64(span.Magnitude)
	normalizedUnit := span.Unit
	// Calendar aliases describe the same month grid. Normalize before the
	// nominal duration comparison so a year cannot round up past twelve months.
	switch normalizedUnit {
	case spl.TimeSpanUnitQuarter:
		magnitude.Mul(magnitude, big.NewInt(3))
		normalizedUnit = spl.TimeSpanUnitMonth
	case spl.TimeSpanUnitYear:
		magnitude.Mul(magnitude, big.NewInt(12))
		normalizedUnit = spl.TimeSpanUnitMonth
	}
	var unit uint64
	switch normalizedUnit {
	case spl.TimeSpanUnitMicrosecond:
		unit = 1000
	case spl.TimeSpanUnitMillisecond:
		unit = 1_000_000
	case spl.TimeSpanUnitCentisecond:
		unit = 10_000_000
	case spl.TimeSpanUnitDecisecond:
		unit = 100_000_000
	default:
		seconds, ok := nominalTimeSpanSeconds(spl.TimeSpan{Magnitude: 1, Unit: normalizedUnit})
		if !ok {
			return nil, false
		}
		unit = seconds * 1_000_000_000
	}
	return magnitude.Mul(magnitude, new(big.Int).SetUint64(unit)), span.Magnitude > 0
}

func nominalTimeSpanSeconds(span spl.TimeSpan) (uint64, bool) {
	var seconds uint64
	switch span.Unit {
	case spl.TimeSpanUnitSecond:
		seconds = 1
	case spl.TimeSpanUnitMinute:
		seconds = 60
	case spl.TimeSpanUnitHour:
		seconds = 60 * 60
	case spl.TimeSpanUnitDay:
		seconds = 24 * 60 * 60
	case spl.TimeSpanUnitWeek:
		seconds = 7 * 24 * 60 * 60
	case spl.TimeSpanUnitMonth:
		seconds = 30 * 24 * 60 * 60
	default:
		return 0, false
	}
	if span.Magnitude == 0 || span.Magnitude > math.MaxUint64/seconds {
		return 0, false
	}
	return span.Magnitude * seconds, true
}

func fixedNumericBinSpan(span spl.BinSpan) (uint64, error) {
	if span.Kind != spl.BinSpanKindNumeric || span.Unit != spl.TimeSpanUnitInvalid {
		return 0, &Diagnostic{
			Code:    "SPL_UNSUPPORTED_BIN_SYNTAX",
			Message: "numeric bin spans must be unitless",
			Range:   span.Range,
		}
	}
	if span.Magnitude == 0 || span.Magnitude > MaximumNumericBinSpan {
		return 0, &Diagnostic{
			Code:    "SPL_NUMBER_OUT_OF_RANGE",
			Message: fmt.Sprintf("numeric bin span must be between 1 and %d", MaximumNumericBinSpan),
			Range:   span.Range,
		}
	}
	return span.Magnitude, nil
}

func fixedBinSpan(span spl.BinSpan) (time.Duration, error) {
	var unit spl.TimeSpanUnit
	switch span.Kind {
	case spl.BinSpanKindNumeric:
		if _, err := fixedNumericBinSpan(span); err != nil {
			return 0, err
		}
		unit = spl.TimeSpanUnitSecond
	case spl.BinSpanKindTime:
		unit = span.Unit
	default:
		return 0, &Diagnostic{
			Code:    "SPL_UNSUPPORTED_BIN_SYNTAX",
			Message: "bin span kind is invalid",
			Range:   span.Range,
		}
	}
	duration, err := fixedDurationSpan(
		spl.TimeSpan{
			Magnitude: span.Magnitude,
			Unit:      unit,
			Range:     span.Range,
		},
		"SPL_UNSUPPORTED_BIN_SYNTAX",
		"bin",
	)
	if err != nil {
		return 0, err
	}
	if duration > 24*time.Hour {
		return 0, &Diagnostic{
			Code:    "SPL_UNSUPPORTED_BIN_SYNTAX",
			Message: "fixed bin spans greater than 24 hours are not supported",
			Range:   span.Range,
			Suggestions: []string{
				"use a fixed span from 1s through 24h",
			},
		}
	}
	return duration, nil
}

func timeBucketSpan(span spl.BinSpan) (time.Duration, CalendarUnit, error) {
	if span.Kind == spl.BinSpanKindTime {
		var calendar CalendarUnit
		switch span.Unit {
		case spl.TimeSpanUnitDay:
			calendar = CalendarDay
		case spl.TimeSpanUnitWeek:
			calendar = CalendarWeek
		}
		if calendar != CalendarNone {
			if err := validateCalendarMagnitude(span.Magnitude, "bin", span.Range); err != nil {
				return 0, CalendarNone, err
			}
			return 0, calendar, nil
		}
	}
	duration, err := fixedBinSpan(span)
	if err != nil {
		return 0, CalendarNone, err
	}
	return duration, CalendarNone, nil
}

func calendarUnit(unit spl.TimeSpanUnit) (CalendarUnit, bool) {
	switch unit {
	case spl.TimeSpanUnitDay:
		return CalendarDay, true
	case spl.TimeSpanUnitWeek:
		return CalendarWeek, true
	case spl.TimeSpanUnitMonth:
		return CalendarMonth, true
	default:
		return CalendarNone, false
	}
}

func validateCalendarMagnitude(magnitude uint64, commandName string, sourceRange spl.Range) error {
	if magnitude == 1 {
		return nil
	}
	return &Diagnostic{
		Code:        "SPL_UNSUPPORTED_CALENDAR_SPAN",
		Message:     commandName + " calendar spans currently require a magnitude of exactly 1",
		Range:       sourceRange,
		Suggestions: []string{"span=1d"},
	}
}

func unsupportedBinTimeField(sourceRange spl.Range) *Diagnostic {
	return &Diagnostic{
		Code:        "SPL_UNSUPPORTED_BIN_TIME_FIELD",
		Message:     "bin requires the unmodified canonical _time field",
		Range:       sourceRange,
		Suggestions: []string{"run bin before removing, replacing, transforming, or previously binning _time"},
	}
}

func fixedDurationSpan(span spl.TimeSpan, syntaxCode, commandName string) (time.Duration, error) {
	var unit time.Duration
	switch span.Unit {
	case spl.TimeSpanUnitSecond:
		unit = time.Second
	case spl.TimeSpanUnitMinute:
		unit = time.Minute
	case spl.TimeSpanUnitHour:
		unit = time.Hour
	default:
		return 0, &Diagnostic{
			Code:    syntaxCode,
			Message: "unsupported " + commandName + " span unit",
			Range:   span.Range,
		}
	}

	maximumMagnitude := safecast.MustConv[uint64](math.MaxInt64) /
		safecast.MustConv[uint64](unit)
	if span.Magnitude == 0 || span.Magnitude > maximumMagnitude {
		return 0, &Diagnostic{
			Code:    "SPL_NUMBER_OUT_OF_RANGE",
			Message: commandName + " span is outside the supported duration range",
			Range:   span.Range,
		}
	}

	return time.Duration(safecast.MustConv[int64](span.Magnitude)) * unit, nil
}

func floorInt64(value, divisor int64) int64 {
	quotient := value / divisor
	if value%divisor < 0 {
		quotient--
	}
	return quotient
}
