package plan

import (
	"fmt"
	"math"
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
		if options.Limit == 0 || options.Limit > timechartSeriesLimit {
			return nil, &Diagnostic{
				Code:        "SPL_UNSUPPORTED_TIMECHART_LIMIT",
				Message:     fmt.Sprintf("timechart limit must be from 1 through %d", timechartSeriesLimit),
				Range:       options.LimitRange,
				Suggestions: []string{fmt.Sprintf("limit=%d", timechartSeriesLimit)},
			}
		}
		split.SeriesLimit = uint16(options.Limit)
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
func timechartMaxSeries(split *TimechartSplit) uint16 {
	series := split.SeriesLimit
	if split.IncludeNull {
		series++
	}
	if split.IncludeOther {
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
		aggregate.InputQuoted || aggregate.AliasQuoted ||
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
	input, inputErr := ResolveField(aggregate.Input, aggregate.InputRange)
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

func fixedTimechartSpan(span spl.TimeSpan) (time.Duration, error) {
	duration, err := fixedDurationSpan(
		span,
		"SPL_UNSUPPORTED_TIMECHART_SYNTAX",
		"timechart",
	)
	if err != nil {
		return 0, err
	}
	if duration > maxTimechartSpan {
		return 0, &Diagnostic{
			Code:        "SPL_UNSUPPORTED_TIMECHART_SYNTAX",
			Message:     "timechart spans greater than 24 hours are not supported",
			Range:       span.Range,
			Suggestions: []string{"use a fixed span from 1s through 24h"},
		}
	}
	return duration, nil
}

func timechartSpan(span spl.TimeSpan) (time.Duration, CalendarUnit, error) {
	if calendar, ok := calendarUnit(span.Unit); ok {
		if err := validateCalendarMagnitude(span.Magnitude, "timechart", span.Range); err != nil {
			return 0, CalendarNone, err
		}
		return 0, calendar, nil
	}
	duration, err := fixedTimechartSpan(span)
	if err != nil {
		return 0, CalendarNone, err
	}
	return duration, CalendarNone, nil
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
		if calendar, ok := calendarUnit(span.Unit); ok {
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

func fixedTimechartBuckets(earliest, latest time.Time, span time.Duration, sourceRange spl.Range) (time.Time, uint64, error) {
	spanSeconds := int64(span / time.Second)
	if spanSeconds <= 0 {
		return time.Time{}, 0, &Diagnostic{
			Code:    "SPL_INVALID_ARGUMENT",
			Message: "timechart span must be at least one second",
			Range:   sourceRange,
		}
	}
	firstSeconds := floorInt64(earliest.Unix(), spanSeconds) * spanSeconds
	deltaSeconds := latest.Unix() - firstSeconds
	if deltaSeconds < 0 {
		return time.Time{}, 0, &Diagnostic{
			Code:    "SPL_INVALID_TIME_RANGE",
			Message: "timechart range cannot be represented",
			Range:   sourceRange,
		}
	}

	bucketCount := safecast.MustConv[uint64](deltaSeconds / spanSeconds)
	if deltaSeconds%spanSeconds != 0 || latest.Nanosecond() != 0 {
		bucketCount++
	}
	if bucketCount == 0 {
		// Build has already established a non-empty search interval; retain a
		// defensive check so malformed plans cannot generate numbers(0).
		return time.Time{}, 0, &Diagnostic{
			Code:    "SPL_INVALID_TIME_RANGE",
			Message: "timechart requires a non-empty bucket range",
			Range:   sourceRange,
		}
	}
	if bucketCount > maxTimechartBuckets {
		return time.Time{}, 0, &Diagnostic{
			Code:    "SPL_QUERY_TOO_COMPLEX",
			Message: fmt.Sprintf("timechart produces more than %d fixed-range buckets", maxTimechartBuckets),
			Range:   sourceRange,
		}
	}
	return time.Unix(firstSeconds, 0).UTC(), bucketCount, nil
}

func timechartBuckets(
	earliest, latest time.Time,
	span time.Duration,
	calendar CalendarUnit,
	location *time.Location,
	sourceRange spl.Range,
) (time.Time, uint64, error) {
	if calendar == CalendarNone {
		return fixedTimechartBuckets(earliest, latest, span, sourceRange)
	}
	if span != 0 || (calendar != CalendarDay && calendar != CalendarWeek) || location == nil {
		return time.Time{}, 0, &Diagnostic{
			Code:    "SPL_INVALID_ARGUMENT",
			Message: "timechart calendar span metadata is invalid",
			Range:   sourceRange,
		}
	}

	localEarliest := earliest.In(location)
	firstBucket := time.Date(
		localEarliest.Year(),
		localEarliest.Month(),
		localEarliest.Day(),
		0,
		0,
		0,
		0,
		location,
	)
	daysPerBucket := 1
	if calendar == CalendarWeek {
		firstBucket = firstBucket.AddDate(0, 0, -int(firstBucket.Weekday()))
		daysPerBucket = 7
	}

	var bucketCount uint64
	for bucket := firstBucket; bucket.Before(latest); bucket = bucket.AddDate(0, 0, daysPerBucket) {
		bucketCount++
		if bucketCount > maxTimechartBuckets {
			return time.Time{}, 0, &Diagnostic{
				Code:    "SPL_QUERY_TOO_COMPLEX",
				Message: fmt.Sprintf("timechart produces more than %d fixed-range buckets", maxTimechartBuckets),
				Range:   sourceRange,
			}
		}
	}
	if bucketCount == 0 {
		return time.Time{}, 0, &Diagnostic{
			Code:    "SPL_INVALID_TIME_RANGE",
			Message: "timechart requires a non-empty bucket range",
			Range:   sourceRange,
		}
	}
	return firstBucket.UTC(), bucketCount, nil
}

func floorInt64(value, divisor int64) int64 {
	quotient := value / divisor
	if value%divisor < 0 {
		quotient--
	}
	return quotient
}
