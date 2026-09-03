package spl

import (
	"fmt"
	"strconv"
	"strings"
)

func (p *parser) parseBinCommand(name token) (Command, error) {
	commandName := strings.ToLower(name.text)
	var (
		fieldSeen bool
		field     token
		output    token
		spanSeen  bool
		span      BinSpan
		spanValue token
		end       = name.sourceRange.End
	)

	for !p.atCommandEnd() {
		current := p.current()
		followedByEqual := current.kind == tokenWord &&
			p.index+1 < len(p.tokens) &&
			p.tokens[p.index+1].kind == tokenEqual
		if fieldSeen && current.kind == tokenWord &&
			strings.EqualFold(current.text, "span") && !followedByEqual {
			return nil, &Diagnostic{
				Code:        "SPL_EXPECTED_EQUAL",
				Message:     "bin span must be followed by '='",
				Range:       current.sourceRange,
				Suggestions: []string{"bin _time span=5m"},
			}
		}
		if followedByEqual && strings.EqualFold(current.text, "span") {
			if spanSeen {
				return nil, p.unsupportedBinSyntax(current, "bin span may be specified only once")
			}
			option := current
			p.advance()
			p.advance() // '=' was established by lookahead.
			value := p.current()
			if value.kind != tokenWord {
				if value.kind == tokenEOF || value.kind == tokenPipe {
					value = option
				}
				return nil, invalidBinSpan(value)
			}
			parsed, err := parseBinSpan(value)
			if err != nil {
				return nil, err
			}
			spanSeen = true
			span = parsed
			spanValue = value
			end = value.sourceRange.End
			p.advance()
			continue
		}

		if followedByEqual {
			switch strings.ToLower(current.text) {
			case "bins", "minspan", "start", "end", "aligntime":
				return nil, p.unsupportedBinSyntax(
					current,
					fmt.Sprintf("bin option %q is not supported; use one explicit fixed span", current.text),
				)
			default:
				return nil, p.unsupportedBinSyntax(
					current,
					fmt.Sprintf("bin option %q is not supported", current.text),
				)
			}
		}
		if current.kind == tokenWord && strings.EqualFold(current.text, "as") {
			if !fieldSeen {
				return nil, p.unsupportedBinSyntax(current, "bin AS requires a source field first")
			}
			asToken := current
			p.advance()
			destination := p.current()
			if destination.kind == tokenEOF || destination.kind == tokenPipe {
				return nil, p.unsupportedBinSyntax(asToken, "bin AS requires one exact unquoted output field")
			}
			if destination.kind != tokenWord {
				return nil, p.unsupportedBinSyntax(destination, "bin AS requires one exact unquoted output field")
			}
			if strings.Contains(destination.text, "*") {
				return nil, p.unsupportedBinSyntax(destination, "wildcard bin output fields are not supported")
			}
			output = destination
			end = destination.sourceRange.End
			p.advance()
			if !p.atCommandEnd() {
				return nil, p.unsupportedBinSyntax(p.current(), "bin AS output must be the final command argument")
			}
			continue
		}
		if current.kind == tokenComma {
			located := current
			if p.index+1 < len(p.tokens) {
				next := p.tokens[p.index+1]
				if next.kind != tokenEOF && next.kind != tokenPipe {
					located = next
				}
			}
			return nil, p.unsupportedBinSyntax(located, "bin supports exactly one field")
		}
		if current.kind != tokenWord {
			return nil, p.unsupportedBinSyntax(current, "bin requires one exact unquoted source field")
		}
		if fieldSeen {
			return nil, p.unsupportedBinSyntax(current, "bin supports exactly one field")
		}
		if strings.Contains(current.text, "*") {
			return nil, p.unsupportedBinSyntax(current, "wildcard bin fields are not supported")
		}
		fieldSeen = true
		field = current
		output = current
		end = current.sourceRange.End
		p.advance()
	}

	if !fieldSeen {
		located := name
		if spanSeen {
			located = spanValue
		}
		return nil, p.unsupportedBinSyntax(located, "bin requires exactly one unquoted source field")
	}
	if !spanSeen {
		return nil, p.unsupportedBinSyntax(field, "bin requires one explicit positive span")
	}
	return &BinCommand{
		CommandName: commandName,
		Field:       field.text,
		FieldRange:  field.sourceRange,
		Output:      output.text,
		OutputRange: output.sourceRange,
		Span:        span,
		Range:       Range{Start: name.sourceRange.Start, End: end},
	}, nil
}

func (p *parser) unsupportedBinSyntax(tok token, message string) *Diagnostic {
	return &Diagnostic{
		Code:        "SPL_UNSUPPORTED_BIN_SYNTAX",
		Message:     message,
		Range:       tok.sourceRange,
		Suggestions: []string{"bin _time span=5m"},
	}
}

func (p *parser) parseTimechartCommand(name token) (Command, error) {
	var options TimechartOptions
	span, spanSpecified, err := p.parseTimechartOptions(&options, true)
	if err != nil {
		return nil, err
	}
	if !spanSpecified {
		return nil, p.unsupportedTimechartSyntax(p.current(), "timechart requires span=<positive integer><s|m|h> before its aggregate")
	}

	aggregate, aggregateEnd, err := p.parseTimechartAggregate()
	if err != nil {
		return nil, err
	}
	if p.atCommandEnd() {
		if diagnostic := p.timechartOptionsRequireSplit(options); diagnostic != nil {
			return nil, diagnostic
		}
		return &TimechartCommand{
			Span:      span,
			Aggregate: aggregate,
			Options:   options,
			Range: Range{
				Start: name.sourceRange.Start,
				End:   aggregateEnd,
			},
		}, nil
	}
	if !p.isKeyword("BY") {
		message := "this timechart form accepts exactly one aggregate and an optional AS output"
		if aggregate.Function == AggregateFunctionCount {
			message = "timechart count accepts only an optional BY followed by one split field"
		}
		return nil, p.unsupportedTimechartSyntax(
			p.current(),
			message,
		)
	}
	if !timechartAggregateSupportsSplit(aggregate.Function) {
		return nil, p.unsupportedTimechartSyntax(
			p.current(),
			"this timechart aggregate does not support a BY split field",
		)
	}
	p.advance()

	field := p.current()
	if field.kind != tokenWord {
		return nil, p.errorAtCurrent("SPL_EXPECTED_FIELD", "timechart BY requires one split field")
	}
	if strings.Contains(field.text, "*") {
		return nil, p.unsupportedTimechartSyntax(field, "wildcard timechart split fields are not supported")
	}
	p.advance()
	end := field.sourceRange.End
	if !p.atCommandEnd() {
		if _, _, optionsErr := p.parseTimechartOptions(&options, false); optionsErr != nil {
			return nil, optionsErr
		}
		end = p.previous().sourceRange.End
	}
	if !p.atCommandEnd() {
		return nil, p.unsupportedTimechartSyntax(p.current(), "only one timechart split field is currently supported")
	}
	return &TimechartCommand{
		Span:      span,
		Aggregate: aggregate,
		SplitBy:   &StatsGroupField{Name: field.text, Range: field.sourceRange},
		Options:   options,
		Range:     Range{Start: name.sourceRange.Start, End: end},
	}, nil
}

// parseTimechartOptions consumes the span=, limit=, useother=, and usenull=
// options at the current position: before the aggregate (where span is
// required) or after the split field, where Splunk places the series options.
// Each option may be authored once across both positions.
func (p *parser) parseTimechartOptions(options *TimechartOptions, allowSpan bool) (TimeSpan, bool, error) {
	var span TimeSpan
	spanSpecified := false
	for {
		option := p.current()
		if option.kind != tokenWord {
			return span, spanSpecified, nil
		}
		lower := strings.ToLower(option.text)
		if !p.nextIs(tokenEqual) {
			if lower == "span" && allowSpan && !spanSpecified {
				return span, spanSpecified, &Diagnostic{
					Code:        "SPL_EXPECTED_EQUAL",
					Message:     "timechart span must be followed by '='",
					Range:       option.sourceRange,
					Suggestions: []string{timechartSyntaxSuggestion},
				}
			}
			return span, spanSpecified, nil
		}
		switch lower {
		case "span":
			if !allowSpan {
				return span, spanSpecified, p.unsupportedTimechartSyntax(option, "timechart span must precede the aggregate")
			}
			if spanSpecified {
				return span, spanSpecified, p.unsupportedTimechartSyntax(option, "timechart option \"span\" is repeated")
			}
			p.advance()
			p.advance()
			spanToken := p.current()
			if spanToken.kind != tokenWord {
				return span, spanSpecified, &Diagnostic{
					Code:        "SPL_INVALID_ARGUMENT",
					Message:     "timechart span must be a positive integer followed by s, m, or h",
					Range:       spanToken.sourceRange,
					Suggestions: []string{timechartSyntaxSuggestion},
				}
			}
			parsed, err := parseTimechartSpan(spanToken)
			if err != nil {
				return span, spanSpecified, err
			}
			span, spanSpecified = parsed, true
			p.advance()
		case "limit":
			if options.LimitSpecified {
				return span, spanSpecified, p.unsupportedTimechartSyntax(option, "timechart option \"limit\" is repeated")
			}
			p.advance()
			p.advance()
			value := p.current()
			if value.kind != tokenWord || !unsignedIntegerSyntax(value.text) {
				if p.atCommandEnd() {
					value = option
				}
				return span, spanSpecified, &Diagnostic{
					Code:        "SPL_INVALID_ARGUMENT",
					Message:     "timechart limit must be a non-negative integer",
					Range:       value.sourceRange,
					Suggestions: []string{"limit=10"},
				}
			}
			limit, limitErr := strconv.ParseUint(value.text, 10, 64)
			if limitErr != nil || limit == 0 || limit > MaximumTimechartSeriesLimit {
				message := fmt.Sprintf("timechart limit must be from 1 through %d", MaximumTimechartSeriesLimit)
				if limitErr == nil && limit == 0 {
					message = "timechart limit=0 (unlimited series) is not supported"
				}
				return span, spanSpecified, &Diagnostic{
					Code:        "SPL_UNSUPPORTED_TIMECHART_LIMIT",
					Message:     message,
					Range:       Range{Start: option.sourceRange.Start, End: value.sourceRange.End},
					Suggestions: []string{fmt.Sprintf("limit=%d", MaximumTimechartSeriesLimit)},
				}
			}
			options.Limit = limit
			options.LimitSpecified = true
			options.LimitRange = Range{Start: option.sourceRange.Start, End: value.sourceRange.End}
			p.advance()
		case "useother", "usenull":
			specified := options.UseOtherSpecified
			if lower == "usenull" {
				specified = options.UseNullSpecified
			}
			if specified {
				return span, spanSpecified, p.unsupportedTimechartSyntax(option, fmt.Sprintf("timechart option %q is repeated", lower))
			}
			p.advance()
			p.advance()
			value := p.current()
			parsed, ok := parseStrictBool(value.text)
			if value.kind != tokenWord || !ok {
				if p.atCommandEnd() {
					value = option
				}
				return span, spanSpecified, p.unsupportedTimechartSyntax(value, fmt.Sprintf("timechart %s must be true or false", lower))
			}
			optionRange := Range{Start: option.sourceRange.Start, End: value.sourceRange.End}
			if lower == "useother" {
				options.UseOther, options.UseOtherSpecified, options.UseOtherRange = parsed, true, optionRange
			} else {
				options.UseNull, options.UseNullSpecified, options.UseNullRange = parsed, true, optionRange
			}
			p.advance()
		default:
			return span, spanSpecified, p.unsupportedTimechartSyntax(option, fmt.Sprintf("timechart option %q is not supported", option.text))
		}
	}
}

// timechartOptionsRequireSplit rejects series options on a timechart without
// a BY split field: there are no series to limit or to collect into NULL and
// OTHER, so accepting them silently would hide an authoring mistake.
func (p *parser) timechartOptionsRequireSplit(options TimechartOptions) *Diagnostic {
	var optionRange Range
	switch {
	case options.LimitSpecified:
		optionRange = options.LimitRange
	case options.UseOtherSpecified:
		optionRange = options.UseOtherRange
	case options.UseNullSpecified:
		optionRange = options.UseNullRange
	default:
		return nil
	}
	return &Diagnostic{
		Code:        "SPL_UNSUPPORTED_TIMECHART_SYNTAX",
		Message:     "timechart limit, useother, and usenull require a BY split field",
		Range:       optionRange,
		Suggestions: []string{"timechart span=5m count BY host limit=5 useother=false"},
	}
}

func (p *parser) parseTimechartAggregate() (StatsAggregate, Position, error) {
	function := p.current()
	if function.kind != tokenWord {
		return StatsAggregate{}, function.sourceRange.End,
			p.unsupportedTimechartAggregate(
				function,
				"timechart requires argument-free count or one count(field), pN/percN(field), sum(field), or avg(field) aggregate",
			)
	}
	functionName := strings.ToLower(function.text)
	if functionName == "count" &&
		(p.index+1 >= len(p.tokens) || p.tokens[p.index+1].kind != tokenLeftParen) {
		p.advance()
		return StatsAggregate{
			Function:   AggregateFunctionCount,
			Alias:      "count",
			Range:      function.sourceRange,
			AliasRange: function.sourceRange,
		}, function.sourceRange.End, nil
	}
	spec, supported := pivotFieldAggregateSpecForName(functionName)
	if !supported {
		return StatsAggregate{}, function.sourceRange.End,
			p.unsupportedTimechartAggregate(
				function,
				fmt.Sprintf("timechart aggregate %q is not supported; use argument-free count or one count(field), pN/percN(field), sum(field), or avg(field) aggregate", function.text),
			)
	}
	p.advance()
	leftParenthesis := p.current()
	if !p.match(tokenLeftParen) {
		return StatsAggregate{}, function.sourceRange.End,
			p.unsupportedTimechartSyntax(
				function,
				"timechart field aggregate requires one exact unquoted field in parentheses",
			)
	}
	input := p.current()
	if input.kind != tokenWord || strings.Contains(input.text, "*") ||
		(strings.EqualFold(input.text, "eval") &&
			p.index+1 < len(p.tokens) &&
			p.tokens[p.index+1].kind == tokenLeftParen) {
		located := input
		// Keep the established count() diagnostic on the opening parenthesis,
		// which previously marked the first token forbidden after bare count.
		if functionName == "count" && input.kind == tokenRightParen {
			located = leftParenthesis
		}
		return StatsAggregate{}, input.sourceRange.End,
			p.unsupportedTimechartSyntax(
				located,
				"timechart field aggregate requires one exact unquoted field",
			)
	}
	p.advance()
	if !p.match(tokenRightParen) {
		return StatsAggregate{}, input.sourceRange.End,
			p.unsupportedTimechartSyntax(
				p.current(),
				"timechart field aggregate accepts exactly one field argument",
			)
	}
	end := p.previous().sourceRange.End
	aggregate := StatsAggregate{
		Function:   spec.function,
		Input:      input.text,
		InputRange: input.sourceRange,
		Percentile: spec.percentile,
		Alias:      spec.canonicalName + "(" + input.text + ")",
		Range:      Range{Start: function.sourceRange.Start, End: end},
		AliasRange: Range{Start: function.sourceRange.Start, End: end},
	}
	if !p.isKeyword("AS") {
		return aggregate, end, nil
	}
	p.advance()
	alias := p.current()
	if alias.kind != tokenWord || p.isKeyword("BY") || strings.Contains(alias.text, "*") {
		return StatsAggregate{}, end, p.unsupportedTimechartSyntax(
			alias,
			"timechart field aggregate AS requires one exact unquoted output field",
		)
	}
	aggregate.Alias = alias.text
	aggregate.ExplicitAlias = true
	aggregate.AliasRange = alias.sourceRange
	aggregate.Range.End = alias.sourceRange.End
	p.advance()
	return aggregate, aggregate.Range.End, nil
}

func pivotFieldAggregateSpecForName(name string) (statsAggregateSpec, bool) {
	if strings.EqualFold(name, "count") {
		return statsAggregateSpec{
			function:      AggregateFunctionCountValues,
			canonicalName: "count",
			requiresInput: true,
		}, true
	}
	spec, supported := statsAggregateSpecForName(name)
	if !supported || !spec.requiresInput {
		return statsAggregateSpec{}, false
	}
	switch spec.function {
	case AggregateFunctionPercentile, AggregateFunctionSum, AggregateFunctionAverage:
		return spec, true
	default:
		return statsAggregateSpec{}, false
	}
}

func timechartAggregateSupportsSplit(function AggregateFunction) bool {
	switch function {
	case AggregateFunctionCount, AggregateFunctionCountValues,
		AggregateFunctionPercentile,
		AggregateFunctionSum, AggregateFunctionAverage:
		return true
	default:
		return false
	}
}

func parseTimechartSpan(tok token) (TimeSpan, error) {
	return parseFixedTimeSpan(tok, timechartTimeSpanConfig)
}

func parseBinSpan(tok token) (BinSpan, error) {
	if unsignedIntegerSyntax(tok.text) {
		magnitude, err := strconv.ParseUint(tok.text, 10, 64)
		if err != nil {
			return BinSpan{}, &Diagnostic{
				Code:    "SPL_NUMBER_OUT_OF_RANGE",
				Message: "bin span is outside the supported 64-bit range",
				Range:   tok.sourceRange,
			}
		}
		if magnitude == 0 {
			return BinSpan{}, invalidBinSpan(tok)
		}
		return BinSpan{
			Kind:      BinSpanKindNumeric,
			Magnitude: magnitude,
			Range:     tok.sourceRange,
		}, nil
	}

	span, err := parseFixedTimeSpan(tok, binTimeSpanConfig)
	if err != nil {
		return BinSpan{}, err
	}
	return BinSpan{
		Kind:      BinSpanKindTime,
		Magnitude: span.Magnitude,
		Unit:      span.Unit,
		Range:     span.Range,
	}, nil
}

func invalidBinSpan(tok token) *Diagnostic {
	return &Diagnostic{
		Code:        "SPL_INVALID_ARGUMENT",
		Message:     "bin span must be a positive integer, optionally followed by s, m, or h",
		Range:       tok.sourceRange,
		Suggestions: []string{"bin _time span=5m"},
	}
}

type fixedTimeSpanParserConfig struct {
	commandName        string
	syntaxCode         string
	suggestion         string
	logSpanUnsupported bool
	calendarAllowed    bool
}

const timechartSyntaxSuggestion = "timechart span=5m count"

var (
	binTimeSpanConfig = fixedTimeSpanParserConfig{
		commandName:        "bin",
		syntaxCode:         "SPL_UNSUPPORTED_BIN_SYNTAX",
		suggestion:         "bin _time span=5m",
		logSpanUnsupported: true,
		calendarAllowed:    true,
	}
	timechartTimeSpanConfig = fixedTimeSpanParserConfig{
		commandName:     "timechart",
		syntaxCode:      "SPL_UNSUPPORTED_TIMECHART_SYNTAX",
		suggestion:      timechartSyntaxSuggestion,
		calendarAllowed: true,
	}
)

func parseFixedTimeSpan(tok token, config fixedTimeSpanParserConfig) (TimeSpan, error) {
	digitEnd := 0
	for digitEnd < len(tok.text) && tok.text[digitEnd] >= '0' && tok.text[digitEnd] <= '9' {
		digitEnd++
	}
	if digitEnd == 0 || digitEnd == len(tok.text) {
		return TimeSpan{}, invalidFixedTimeSpan(tok, config)
	}
	unitText := tok.text[digitEnd:]
	if config.logSpanUnsupported && strings.Contains(strings.ToLower(unitText), "log") {
		return TimeSpan{}, unsupportedFixedTimeSpanUnit(tok, config)
	}
	for index := range len(unitText) {
		if unitText[index] >= '0' && unitText[index] <= '9' {
			return TimeSpan{}, invalidFixedTimeSpan(tok, config)
		}
	}
	var unit TimeSpanUnit
	var unitNanoseconds uint64
	calendar := false
	switch strings.ToLower(unitText) {
	case "s":
		unit = TimeSpanUnitSecond
		unitNanoseconds = 1_000_000_000
	case "m":
		unit = TimeSpanUnitMinute
		unitNanoseconds = 60 * 1_000_000_000
	case "h":
		unit = TimeSpanUnitHour
		unitNanoseconds = 60 * 60 * 1_000_000_000
	case "d":
		if !config.calendarAllowed {
			return TimeSpan{}, unsupportedFixedTimeSpanUnit(tok, config)
		}
		unit = TimeSpanUnitDay
		calendar = true
	case "w":
		if !config.calendarAllowed {
			return TimeSpan{}, unsupportedFixedTimeSpanUnit(tok, config)
		}
		unit = TimeSpanUnitWeek
		calendar = true
	default:
		return TimeSpan{}, unsupportedFixedTimeSpanUnit(tok, config)
	}
	magnitude, err := strconv.ParseUint(tok.text[:digitEnd], 10, 64)
	if err != nil {
		return TimeSpan{}, &Diagnostic{
			Code:    "SPL_NUMBER_OUT_OF_RANGE",
			Message: config.commandName + " span is outside the supported 64-bit range",
			Range:   tok.sourceRange,
		}
	}
	if magnitude == 0 {
		return TimeSpan{}, invalidFixedTimeSpan(tok, config)
	}
	if calendar {
		if magnitude != 1 {
			return TimeSpan{}, unsupportedCalendarSpan(tok, config)
		}
		return TimeSpan{Magnitude: magnitude, Unit: unit, Range: tok.sourceRange}, nil
	}
	const maxDurationNanoseconds = uint64(1<<63 - 1)
	if magnitude > maxDurationNanoseconds/unitNanoseconds {
		return TimeSpan{}, &Diagnostic{
			Code:    "SPL_NUMBER_OUT_OF_RANGE",
			Message: config.commandName + " span is outside the supported duration range",
			Range:   tok.sourceRange,
		}
	}
	return TimeSpan{Magnitude: magnitude, Unit: unit, Range: tok.sourceRange}, nil
}

func invalidFixedTimeSpan(tok token, config fixedTimeSpanParserConfig) *Diagnostic {
	return &Diagnostic{
		Code:        "SPL_INVALID_ARGUMENT",
		Message:     config.commandName + " span must be a positive integer followed by s, m, or h",
		Range:       tok.sourceRange,
		Suggestions: []string{config.suggestion},
	}
}

func unsupportedCalendarSpan(tok token, config fixedTimeSpanParserConfig) *Diagnostic {
	return &Diagnostic{
		Code:        "SPL_UNSUPPORTED_CALENDAR_SPAN",
		Message:     config.commandName + " calendar spans currently require a magnitude of exactly 1",
		Range:       tok.sourceRange,
		Suggestions: []string{"span=1d"},
	}
}

func unsupportedFixedTimeSpanUnit(tok token, config fixedTimeSpanParserConfig) *Diagnostic {
	return &Diagnostic{
		Code:        config.syntaxCode,
		Message:     fmt.Sprintf("%s span unit in %q is unsupported; use fixed seconds, minutes, or hours", config.commandName, tok.text),
		Range:       tok.sourceRange,
		Suggestions: []string{config.suggestion},
	}
}

func (p *parser) unsupportedTimechartSyntax(tok token, message string) *Diagnostic {
	return &Diagnostic{
		Code:        "SPL_UNSUPPORTED_TIMECHART_SYNTAX",
		Message:     message,
		Range:       tok.sourceRange,
		Suggestions: timechartSyntaxSuggestions(),
	}
}

func (p *parser) unsupportedTimechartAggregate(tok token, message string) *Diagnostic {
	return &Diagnostic{
		Code:        "SPL_UNSUPPORTED_TIMECHART_AGGREGATE",
		Message:     message,
		Range:       tok.sourceRange,
		Suggestions: timechartSyntaxSuggestions(),
	}
}

func timechartSyntaxSuggestions() []string {
	return []string{
		timechartSyntaxSuggestion,
		"timechart span=5m p95(field) AS p95_field",
	}
}

// parseChartCommand parses the bounded two-field pivot
// "chart <aggregate> OVER <row> BY <column>", its equivalent spelling
// "chart <aggregate> BY <row>, <column>", and the single-split forms
// "chart <aggregate> OVER <row>" / "chart <aggregate> BY <row>" that plan as
// the stats BY table. Chart never discretizes, so no option is accepted;
// bin/bucket remains the only discretizer.
func (p *parser) parseChartCommand(name token) (Command, error) {
	if diagnostic := p.chartOptionDiagnostic(); diagnostic != nil {
		return nil, diagnostic
	}
	aggregate, err := p.parseChartAggregate()
	if err != nil {
		return nil, err
	}
	if p.isKeyword("AS") {
		return nil, p.unsupportedChartAggregate(
			p.current(),
			"chart aggregates cannot be renamed with AS because no pivot column can carry the alias",
		)
	}
	if diagnostic := p.chartOptionDiagnostic(); diagnostic != nil {
		return nil, diagnostic
	}

	var over, splitBy StatsGroupField
	overSpelledOver := false
	switch {
	case p.isKeyword("OVER"):
		overSpelledOver = true
		p.advance()
		row, rowErr := p.parseChartField("OVER")
		if rowErr != nil {
			return nil, rowErr
		}
		over = row
		if p.atCommandEnd() {
			break
		}
		if !p.isKeyword("BY") {
			return nil, p.chartClauseDiagnostic("chart OVER requires BY followed by one column split field")
		}
		p.advance()
		column, columnErr := p.parseChartField("BY")
		if columnErr != nil {
			return nil, columnErr
		}
		splitBy = column
		if !p.atCommandEnd() {
			return nil, p.chartClauseDiagnostic("chart supports exactly one row split field and one column split field")
		}
	case p.isKeyword("BY"):
		p.advance()
		row, column, fieldsErr := p.parseChartSplitFields()
		if fieldsErr != nil {
			return nil, fieldsErr
		}
		over, splitBy = row, column
	default:
		if p.atCommandEnd() {
			return nil, p.chartMissingSplitSyntax()
		}
		current := p.current()
		if current.kind == tokenComma || (current.kind == tokenWord && (supportedStatsAggregateName(current.text) ||
			(p.index+1 < len(p.tokens) && p.tokens[p.index+1].kind == tokenLeftParen))) {
			return nil, p.unsupportedChartAggregate(current, "only one chart aggregate is supported")
		}
		return nil, p.chartClauseDiagnostic("chart requires OVER <row> BY <column> or BY <row>, <column>")
	}

	end := over.Range.End
	if splitBy != (StatsGroupField{}) {
		if over.Name == splitBy.Name {
			return nil, p.unsupportedChartSyntaxAt(splitBy.Range, "chart row and column fields must be different")
		}
		end = splitBy.Range.End
	}
	return &ChartCommand{
		Aggregate:       aggregate,
		Over:            over,
		SplitBy:         splitBy,
		OverSpelledOver: overSpelledOver,
		Range:           Range{Start: name.sourceRange.Start, End: end},
	}, nil
}

func (p *parser) parseChartAggregate() (StatsAggregate, error) {
	function := p.current()
	if function.kind != tokenWord {
		return StatsAggregate{}, p.unsupportedChartAggregate(
			function,
			"chart requires argument-free count or one count(field), pN(field), percN(field), sum(field), or avg(field) aggregate",
		)
	}
	functionName := strings.ToLower(function.text)
	if functionName == "count" &&
		(p.index+1 >= len(p.tokens) || p.tokens[p.index+1].kind != tokenLeftParen) {
		p.advance()
		return StatsAggregate{
			Function:   AggregateFunctionCount,
			Alias:      "count",
			Range:      function.sourceRange,
			AliasRange: function.sourceRange,
		}, nil
	}

	spec, supported := pivotFieldAggregateSpecForName(functionName)
	if !supported {
		return StatsAggregate{}, p.unsupportedChartAggregate(
			function,
			fmt.Sprintf("chart aggregate %q is not supported; use count, count(field), pN(field), percN(field), sum(field), or avg(field)", function.text),
		)
	}
	p.advance()
	if !p.match(tokenLeftParen) {
		return StatsAggregate{}, p.unsupportedChartSyntax(
			function,
			"chart field aggregates require one exact unquoted field in parentheses",
		)
	}
	input := p.current()
	if input.kind != tokenWord || !IsExactUnquotedFieldName(input.text) ||
		(strings.EqualFold(input.text, "eval") &&
			p.index+1 < len(p.tokens) && p.tokens[p.index+1].kind == tokenLeftParen) {
		return StatsAggregate{}, p.unsupportedChartSyntax(
			input,
			"chart field aggregates require one exact unquoted field",
		)
	}
	p.advance()
	if !p.match(tokenRightParen) {
		return StatsAggregate{}, p.unsupportedChartSyntax(
			p.current(),
			"chart field aggregates accept exactly one field argument",
		)
	}
	end := p.previous().sourceRange.End
	aggregate := StatsAggregate{
		Function:   spec.function,
		Input:      input.text,
		InputRange: input.sourceRange,
		Percentile: spec.percentile,
		Alias:      spec.canonicalName + "(" + input.text + ")",
		Range:      Range{Start: function.sourceRange.Start, End: end},
		AliasRange: Range{Start: function.sourceRange.Start, End: end},
	}
	return aggregate, nil
}

// parseChartSplitFields parses exactly two comma-, whitespace-, or
// comma-and-whitespace-separated split fields, using the stats BY separator
// rule. A trailing comma and a third field are both rejected.
func (p *parser) parseChartSplitFields() (StatsGroupField, StatsGroupField, error) {
	var fields []StatsGroupField
	wantField := true
	for !p.atCommandEnd() {
		tok := p.current()
		if tok.kind == tokenComma {
			if wantField {
				return StatsGroupField{}, StatsGroupField{}, p.errorAtCurrent("SPL_EXPECTED_FIELD", "chart BY requires a split field")
			}
			if len(fields) == 2 {
				return StatsGroupField{}, StatsGroupField{}, p.unsupportedChartSyntax(
					tok, "chart supports exactly one row split field and one column split field",
				)
			}
			wantField = true
			p.advance()
			continue
		}
		// OVER is only a misplaced keyword where a field cannot begin: after an
		// already-parsed field with no separating comma. At the start of the
		// list or just after a comma it is an ordinary field name, and the two
		// documented spellings must agree about it.
		if p.isKeyword("OVER") && !wantField {
			return StatsGroupField{}, StatsGroupField{}, p.unsupportedChartSyntax(
				tok, "chart requires OVER before the BY column split field",
			)
		}
		if len(fields) == 2 {
			return StatsGroupField{}, StatsGroupField{}, p.chartClauseDiagnostic(
				"chart supports exactly one row split field and one column split field",
			)
		}
		field, err := p.parseChartField("BY")
		if err != nil {
			return StatsGroupField{}, StatsGroupField{}, err
		}
		fields = append(fields, field)
		wantField = false
	}
	if wantField {
		return StatsGroupField{}, StatsGroupField{}, p.errorAtCurrent("SPL_EXPECTED_FIELD", "chart BY requires a split field")
	}
	if len(fields) == 1 {
		return fields[0], StatsGroupField{}, nil
	}
	return fields[0], fields[1], nil
}

func (p *parser) parseChartField(clause string) (StatsGroupField, error) {
	if p.atCommandEnd() {
		return StatsGroupField{}, p.errorAtCurrent("SPL_EXPECTED_FIELD", "chart "+clause+" requires one split field")
	}
	if diagnostic := p.chartOptionDiagnostic(); diagnostic != nil {
		return StatsGroupField{}, diagnostic
	}
	field := p.current()
	if field.kind == tokenString {
		return StatsGroupField{}, p.unsupportedChartSyntax(field, "quoted chart field names are not supported")
	}
	if field.kind != tokenWord {
		return StatsGroupField{}, p.errorAtCurrent("SPL_EXPECTED_FIELD", "chart "+clause+" requires one split field")
	}
	if strings.Contains(field.text, "*") {
		return StatsGroupField{}, p.unsupportedChartSyntax(field, "wildcard chart fields are not supported")
	}
	p.advance()
	return StatsGroupField{Name: field.text, Range: field.sourceRange}, nil
}

// chartOptionDiagnostic rejects every <name>=<value> token, including
// spellings equal to Splunk's documented defaults, which chart implements
// without letting them be restated. agg= names the aggregate rather than a
// rendering option and keeps the aggregate classification.
func (p *parser) chartOptionDiagnostic() *Diagnostic {
	option := p.current()
	if option.kind != tokenWord || p.index+1 >= len(p.tokens) || p.tokens[p.index+1].kind != tokenEqual {
		return nil
	}
	if strings.EqualFold(option.text, "agg") {
		return p.unsupportedChartAggregate(option, "chart agg= is not supported; write count, pN(field), percN(field), sum(field), or avg(field) directly")
	}
	return &Diagnostic{
		Code:        "SPL_UNSUPPORTED_CHART_OPTION",
		Message:     fmt.Sprintf("chart option %q is not supported", option.text),
		Range:       option.sourceRange,
		Suggestions: []string{"chart count OVER row BY column", "chart p95(field) OVER row BY column", "chart sum(field) OVER row BY column"},
	}
}

// chartClauseDiagnostic classifies an unexpected token at a clause boundary:
// the trailing series filter and every option keep their own codes so the
// rejected surface stays distinguishable.
func (p *parser) chartClauseDiagnostic(message string) *Diagnostic {
	current := p.current()
	if current.kind == tokenWord && strings.EqualFold(current.text, "WHERE") {
		return p.unsupportedChartSyntax(current, "chart WHERE series filters are not supported")
	}
	if diagnostic := p.chartOptionDiagnostic(); diagnostic != nil {
		return diagnostic
	}
	return p.unsupportedChartSyntax(current, message)
}

func (p *parser) chartMissingSplitSyntax() *Diagnostic {
	return &Diagnostic{
		Code:        "SPL_UNSUPPORTED_CHART_SYNTAX",
		Message:     "chart requires a row split field: OVER <row> or BY <row>, optionally followed by one column split field",
		Range:       p.current().sourceRange,
		Suggestions: []string{"stats count BY <field>", "chart count BY row", "chart count OVER row BY column", "chart p95(field) OVER row BY column", "chart sum(field) OVER row BY column"},
	}
}

func (p *parser) unsupportedChartAggregate(tok token, message string) *Diagnostic {
	return &Diagnostic{
		Code:        "SPL_UNSUPPORTED_CHART_AGGREGATE",
		Message:     message,
		Range:       tok.sourceRange,
		Suggestions: []string{"chart count OVER row BY column", "chart p95(field) OVER row BY column", "chart sum(field) OVER row BY column"},
	}
}

func (p *parser) unsupportedChartSyntax(tok token, message string) *Diagnostic {
	return p.unsupportedChartSyntaxAt(tok.sourceRange, message)
}

func (p *parser) unsupportedChartSyntaxAt(sourceRange Range, message string) *Diagnostic {
	return &Diagnostic{
		Code:        "SPL_UNSUPPORTED_CHART_SYNTAX",
		Message:     message,
		Range:       sourceRange,
		Suggestions: []string{"chart count OVER row BY column", "chart p95(field) OVER row BY column"},
	}
}
