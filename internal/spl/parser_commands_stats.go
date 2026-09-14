package spl

import (
	"fmt"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"
)

func (p *parser) parseStatsCommand(name token) (Command, error) {
	if p.atCommandEnd() {
		return nil, p.errorAtCurrent("SPL_EXPECTED_AGGREGATE", "stats requires an aggregate function")
	}

	options := StatsOptions{}
	for !p.atCommandEnd() {
		option := p.current()
		if option.kind != tokenWord || !isLeadingStatsOptionName(option.text) {
			if statsOptionFollowedByEqual(p.tokens, p.index) {
				return nil, p.unsupportedStatsOption(
					option,
					fmt.Sprintf("stats option %q is not supported", option.text),
				)
			}
			break
		}
		if !statsOptionFollowedByEqual(p.tokens, p.index) {
			return nil, &Diagnostic{
				Code:        "SPL_EXPECTED_EQUAL",
				Message:     fmt.Sprintf("stats option %q must be followed by '='", option.text),
				Range:       option.sourceRange,
				Suggestions: []string{strings.ToLower(option.text) + "="},
			}
		}
		if _, err := p.parseLeadingStatsOption(&options); err != nil {
			return nil, err
		}
	}
	if p.atCommandEnd() {
		return nil, p.errorAtCurrent("SPL_EXPECTED_AGGREGATE", "stats options must be followed by an aggregate function")
	}

	aggregates := make([]StatsAggregate, 0, 4)
	var end Position
	for {
		if len(aggregates) >= MaximumStatsMeasures {
			return nil, &Diagnostic{
				Code:    "SPL_QUERY_TOO_COMPLEX",
				Message: fmt.Sprintf("stats contains more than %d aggregate measures", MaximumStatsMeasures),
				Range:   p.current().sourceRange,
			}
		}
		aggregate, aggregateEnd, err := p.parseStatsAggregate()
		if err != nil {
			return nil, err
		}
		aggregates = append(aggregates, aggregate)
		end = aggregateEnd
		if p.isKeyword("BY") || p.atCommandEnd() ||
			statsOptionFollowedByEqual(p.tokens, p.index) {
			break
		}
		if p.match(tokenComma) {
			if p.atCommandEnd() || p.isKeyword("BY") {
				return nil, p.errorAtCurrent("SPL_EXPECTED_AGGREGATE", "expected a stats aggregate after comma")
			}
			continue
		}
		if p.current().kind == tokenWord && (supportedStatsAggregateName(p.current().text) ||
			(p.index+1 < len(p.tokens) && p.tokens[p.index+1].kind == tokenLeftParen)) {
			continue
		}
		current := p.current()
		return nil, &Diagnostic{
			Code:        "SPL_UNSUPPORTED_STATS_SYNTAX",
			Message:     fmt.Sprintf("unsupported stats syntax at %q; expected another supported aggregate, AS, or BY", current.text),
			Range:       current.sourceRange,
			Suggestions: []string{"stats count", "stats dc(field) BY group", "stats earliest(field) latest(field) BY group", "stats min(field) max(field) BY group", "stats sum(field) avg(field) BY group", "stats p50(field) p95(field) BY group"},
		}
	}

	var groupBy []StatsGroupField
	if p.isKeyword("BY") {
		p.advance()
		var err error
		groupBy, end, err = p.parseBoundedAggregateGroupFields(
			"stats",
			"a",
			false,
			true,
			p.unsupportedStatsGroupSyntax,
		)
		if err != nil {
			return nil, err
		}
	}
	if !p.atCommandEnd() {
		option := p.current()
		if !statsOptionFollowedByEqual(p.tokens, p.index) ||
			!strings.EqualFold(option.text, "dedup_splitvals") {
			message := fmt.Sprintf("unsupported stats syntax at %q", option.text)
			if statsOptionFollowedByEqual(p.tokens, p.index) {
				message = fmt.Sprintf(
					"stats option %q must precede aggregates or is not supported in this position",
					option.text,
				)
			}
			return nil, p.unsupportedStatsOption(option, message)
		}
		optionEnd, err := p.parseStatsDeduplicateSplitValues(&options)
		if err != nil {
			return nil, err
		}
		end = optionEnd
		if !p.atCommandEnd() {
			return nil, p.unsupportedStatsOption(
				p.current(),
				"dedup_splitvals must be the final stats command option",
			)
		}
	}
	return &StatsCommand{
		Aggregates: aggregates,
		GroupBy:    groupBy,
		Options:    options,
		Range:      Range{Start: name.sourceRange.Start, End: end},
	}, nil
}

func isLeadingStatsOptionName(name string) bool {
	switch strings.ToLower(name) {
	case "partitions", "allnum", "delim":
		return true
	default:
		return false
	}
}

func statsOptionFollowedByEqual(tokens []token, index int) bool {
	return index >= 0 && index+1 < len(tokens) &&
		tokens[index].kind == tokenWord && tokens[index+1].kind == tokenEqual
}

func (p *parser) parseLeadingStatsOption(options *StatsOptions) (Position, error) {
	option := p.current()
	optionName := strings.ToLower(option.text)
	p.advance()
	p.advance() // parseStatsCommand established the '=' token by lookahead.
	value := p.current()

	switch optionName {
	case "partitions":
		if options.PartitionsSpecified {
			return option.sourceRange.End, p.unsupportedStatsOption(
				option,
				"stats partitions may be specified only once",
			)
		}
		if value.kind != tokenWord || !unsignedIntegerSyntax(value.text) {
			return option.sourceRange.End, p.unsupportedStatsOption(
				value,
				"stats partitions must be an unsigned base-10 64-bit integer; values above the configured limit clamp",
			)
		}
		parsed, err := strconv.ParseUint(value.text, 10, 64)
		if err != nil {
			return option.sourceRange.End, p.unsupportedStatsOption(
				value,
				"stats partitions is outside the supported unsigned 64-bit range",
			)
		}
		options.Partitions = parsed
		options.PartitionsSpecified = true
		options.PartitionsRange = Range{Start: option.sourceRange.Start, End: value.sourceRange.End}
	case "allnum":
		if options.AllNumericSpecified {
			return option.sourceRange.End, p.unsupportedStatsOption(
				option,
				"stats allnum may be specified only once",
			)
		}
		if value.kind != tokenWord {
			return option.sourceRange.End, p.unsupportedStatsOption(
				value,
				"stats allnum must be t, true, f, or false",
			)
		}
		parsed, ok := parseStreamStatsBool(value.text)
		if !ok {
			return option.sourceRange.End, p.unsupportedStatsOption(
				value,
				"stats allnum must be t, true, f, or false",
			)
		}
		options.AllNumeric = parsed
		options.AllNumericSpecified = true
		options.AllNumericRange = Range{Start: option.sourceRange.Start, End: value.sourceRange.End}
	case "delim":
		if options.DelimiterSpecified {
			return option.sourceRange.End, p.unsupportedStatsOption(
				option,
				"stats delim may be specified only once",
			)
		}
		if value.kind != tokenWord && value.kind != tokenString {
			return option.sourceRange.End, p.unsupportedStatsOption(
				value,
				"stats delim requires one quoted string or bare token",
			)
		}
		if value.kind == tokenWord && value.text == "" {
			return option.sourceRange.End, p.unsupportedStatsOption(
				value,
				"an empty stats delimiter must be written as a quoted string",
			)
		}
		if !utf8.ValidString(value.text) {
			return option.sourceRange.End, p.unsupportedStatsOption(
				value,
				"stats delimiter must contain valid UTF-8",
			)
		}
		options.Delimiter = value.text
		options.DelimiterSpecified = true
		options.DelimiterRange = Range{Start: option.sourceRange.Start, End: value.sourceRange.End}
	default:
		return option.sourceRange.End, p.unsupportedStatsOption(
			option,
			fmt.Sprintf("stats option %q is not supported", option.text),
		)
	}

	end := value.sourceRange.End
	p.advance()
	return end, nil
}

func (p *parser) parseStatsDeduplicateSplitValues(
	options *StatsOptions,
) (Position, error) {
	option := p.current()
	if options.DeduplicateSplitValuesSpecified {
		return option.sourceRange.End, p.unsupportedStatsOption(
			option,
			"stats dedup_splitvals may be specified only once",
		)
	}
	p.advance()
	p.advance() // The caller established the '=' token by lookahead.
	value := p.current()
	if value.kind != tokenWord {
		if value.kind == tokenEOF || value.kind == tokenPipe {
			value = option
		}
		return option.sourceRange.End, p.unsupportedStatsOption(
			value,
			"stats dedup_splitvals must be t, true, f, or false",
		)
	}
	parsed, ok := parseStreamStatsBool(value.text)
	if !ok {
		return option.sourceRange.End, p.unsupportedStatsOption(
			value,
			"stats dedup_splitvals must be t, true, f, or false",
		)
	}
	options.DeduplicateSplitValues = parsed
	options.DeduplicateSplitValuesSpecified = true
	options.DeduplicateSplitValuesRange = Range{
		Start: option.sourceRange.Start,
		End:   value.sourceRange.End,
	}
	end := value.sourceRange.End
	p.advance()
	return end, nil
}

func (p *parser) parseEventStatsCommand(name token) (Command, error) {
	acceptedForms := eventStatsAcceptedAggregateForms()
	if p.atCommandEnd() {
		return nil, p.errorAtCurrent(
			"SPL_EXPECTED_AGGREGATE",
			"eventstats requires "+acceptedForms,
		)
	}

	functionToken := p.current()
	if functionToken.kind != tokenWord {
		return nil, p.errorAtCurrent(
			"SPL_EXPECTED_AGGREGATE",
			"eventstats requires "+acceptedForms,
		)
	}
	functionName := strings.ToLower(functionToken.text)
	fieldSpec, fieldAggregate := eventStatsFieldAggregateSpecForName(
		functionName,
	)
	if functionName != "count" && !fieldAggregate {
		if p.index+1 < len(p.tokens) && p.tokens[p.index+1].kind == tokenEqual {
			return nil, p.unsupportedEventStatsSyntax(
				functionToken,
				fmt.Sprintf("eventstats option %q is not supported", functionToken.text),
			)
		}
		return nil, p.unsupportedEventStatsAggregate(
			functionToken,
			fmt.Sprintf(
				"eventstats aggregate %q is not supported; supported aggregates are %s",
				functionToken.text,
				acceptedForms,
			),
		)
	}
	p.advance()

	aggregate := StatsAggregate{
		Function:   AggregateFunctionCount,
		Alias:      "count",
		Range:      functionToken.sourceRange,
		AliasRange: functionToken.sourceRange,
	}
	end := functionToken.sourceRange.End
	if fieldAggregate {
		var aggregateErr error
		aggregate, end, aggregateErr = p.parseEventStatsFieldAggregate(
			functionToken,
			fieldSpec,
		)
		if aggregateErr != nil {
			return nil, aggregateErr
		}
	} else if p.startsCountPredicate() {
		predicate, predicateEnd, predicateErr := p.parseCountPredicate()
		if predicateErr != nil {
			return nil, predicateErr
		}
		aggregate.Function = AggregateFunctionCountPredicate
		aggregate.Predicate = predicate
		end = predicateEnd
		aggregate.Range.End = end
		aggregate.Alias = ""
		aggregate.AliasRange = Range{
			Start: functionToken.sourceRange.Start,
			End:   end,
		}
	} else if p.current().kind == tokenLeftParen {
		var aggregateErr error
		aggregate, end, aggregateErr = p.parseEventStatsFieldAggregate(
			functionToken,
			statsAggregateSpec{
				function:      AggregateFunctionCountValues,
				canonicalName: "count",
				requiresInput: true,
			},
		)
		if aggregateErr != nil {
			return nil, aggregateErr
		}
	}

	if p.isKeyword("AS") {
		p.advance()
		alias := p.current()
		if alias.kind != tokenWord || p.isKeyword("BY") {
			return nil, p.errorAtCurrent("SPL_EXPECTED_FIELD", "expected an eventstats output field name after AS")
		}
		if strings.Contains(alias.text, "*") {
			return nil, p.unsupportedEventStatsSyntax(alias, "wildcard eventstats output fields are not supported")
		}
		aggregate.Alias = alias.text
		aggregate.ExplicitAlias = true
		aggregate.AliasRange = alias.sourceRange
		aggregate.Range.End = alias.sourceRange.End
		end = alias.sourceRange.End
		p.advance()
	}
	if (aggregate.Function == AggregateFunctionCountValues ||
		aggregate.Function == AggregateFunctionCountPredicate ||
		fieldAggregate) &&
		!aggregate.ExplicitAlias {
		form := "count(field)"
		suggestion := "eventstats count(field) AS occurrences"
		switch aggregate.Function {
		case AggregateFunctionCountPredicate:
			form = "count(eval(...))"
			suggestion = "eventstats count(eval(field=value)) AS matches"
		case AggregateFunctionCountValues:
			// The defaults describe count(field). Unlike the exact-field
			// extrema and arithmetic functions, count also owns the row and
			// predicate forms, so it intentionally remains outside the shared
			// exact-field descriptor table.
		default:
			var ok bool
			form, suggestion, ok = eventStatsFieldAggregatePresentation(fieldSpec)
			if !ok {
				return nil, p.unsupportedEventStatsAggregate(
					functionToken,
					"eventstats aggregate metadata is invalid",
				)
			}
		}
		return nil, &Diagnostic{
			Code:        "SPL_UNSUPPORTED_EVENTSTATS_SYNTAX",
			Message:     "eventstats " + form + " requires AS followed by an output field name",
			Range:       aggregate.Range,
			Suggestions: []string{suggestion},
		}
	}

	var groupBy []StatsGroupField
	if p.isKeyword("BY") {
		p.advance()
		var err error
		groupBy, end, err = p.parseBoundedAggregateGroupFields(
			"eventstats",
			"an",
			true,
			false,
			p.unsupportedEventStatsSyntax,
		)
		if err != nil {
			return nil, err
		}
	}
	if !p.atCommandEnd() {
		return nil, p.unsupportedEventStatsSyntax(
			p.current(),
			fmt.Sprintf(
				"unsupported eventstats syntax at %q; accepted syntax is %s with optional BY",
				p.current().text,
				acceptedForms,
			),
		)
	}
	return &EventStatsCommand{
		Aggregate: aggregate,
		GroupBy:   groupBy,
		Range:     Range{Start: name.sourceRange.Start, End: end},
	}, nil
}

const (
	streamStatsSyntaxSuggestion         = "streamstats count AS running_count"
	streamStatsCountPredicateSuggestion = "streamstats count(eval(field=value)) AS matches"
)

type streamStatsAggregateDescriptor struct {
	name            string
	function        AggregateFunction
	allowsBare      bool
	allowsPredicate bool
}

var streamStatsAggregateDescriptors = []streamStatsAggregateDescriptor{
	{
		name:            "count",
		function:        AggregateFunctionCountValues,
		allowsBare:      true,
		allowsPredicate: true,
	},
	{name: "sum", function: AggregateFunctionSum},
	{name: "avg", function: AggregateFunctionAverage},
	{name: "min", function: AggregateFunctionMinimum},
	{name: "max", function: AggregateFunctionMaximum},
	{name: "earliest", function: AggregateFunctionEarliest},
	{name: "latest", function: AggregateFunctionLatest},
}

func (p *parser) parseEventStatsFieldAggregate(
	functionToken token,
	spec statsAggregateSpec,
) (StatsAggregate, Position, error) {
	functionName := strings.ToLower(functionToken.text)
	end := functionToken.sourceRange.End
	aggregate := StatsAggregate{
		Function:   spec.function,
		Percentile: spec.percentile,
		Range:      functionToken.sourceRange,
		AliasRange: functionToken.sourceRange,
	}
	openParen := p.current()
	if !p.match(tokenLeftParen) {
		return StatsAggregate{}, end, p.unsupportedEventStatsSyntax(
			functionToken,
			"eventstats "+functionName+
				" requires one exact field argument in parentheses",
		)
	}
	if spec.function == AggregateFunctionCountValues &&
		p.current().kind == tokenRightParen {
		return StatsAggregate{}, end, p.unsupportedEventStatsSyntax(
			openParen,
			"eventstats count() is not supported; omit the parentheses for a row count",
		)
	}
	input := p.current()
	if input.kind != tokenWord {
		return StatsAggregate{}, end, p.errorAtCurrent(
			"SPL_EXPECTED_FIELD",
			"eventstats "+functionName+
				"(field) requires one exact unquoted input field",
		)
	}
	if strings.Contains(input.text, "*") {
		return StatsAggregate{}, end, p.unsupportedEventStatsSyntax(
			input,
			"wildcard eventstats "+functionName+" fields are not supported",
		)
	}
	aggregate.Input = input.text
	aggregate.InputRange = input.sourceRange
	p.advance()
	if !p.match(tokenRightParen) {
		return StatsAggregate{}, end, p.errorAtCurrent(
			"SPL_EXPECTED_RIGHT_PAREN",
			"expected ')' after the eventstats "+functionName+" input field",
		)
	}
	end = p.previous().sourceRange.End
	aggregate.Range.End = end
	aggregate.AliasRange = Range{
		Start: functionToken.sourceRange.Start,
		End:   end,
	}
	return aggregate, end, nil
}

func (p *parser) unsupportedEventStatsAggregate(tok token, message string) *Diagnostic {
	return &Diagnostic{
		Code:        "SPL_UNSUPPORTED_EVENTSTATS_AGGREGATE",
		Message:     message,
		Range:       tok.sourceRange,
		Suggestions: eventStatsDiagnosticSuggestions(),
	}
}

func (p *parser) unsupportedEventStatsSyntax(tok token, message string) *Diagnostic {
	return &Diagnostic{
		Code:        "SPL_UNSUPPORTED_EVENTSTATS_SYNTAX",
		Message:     message,
		Range:       tok.sourceRange,
		Suggestions: eventStatsDiagnosticSuggestions(),
	}
}

func eventStatsDiagnosticSuggestions() []string {
	suggestions := []string{
		"eventstats count",
		"eventstats count AS event_count BY group",
		"eventstats count(field) AS occurrences BY group",
		"eventstats count(eval(field=value)) AS matches BY group",
		"eventstats p50(field) AS p50_value BY group",
		"eventstats p95(field) AS p95_value BY group",
	}
	for _, descriptor := range eventStatsFieldAggregateDescriptors {
		suggestions = append(suggestions, descriptor.suggestion+" BY group")
	}
	return suggestions
}

func (p *parser) parseStatsAggregate() (StatsAggregate, Position, error) {
	functionToken := p.current()
	if functionToken.kind != tokenWord {
		return StatsAggregate{}, functionToken.sourceRange.End, p.errorAtCurrent("SPL_EXPECTED_AGGREGATE", "stats requires an aggregate function")
	}
	p.advance()
	if strings.EqualFold(functionToken.text, "sparkline") {
		return p.parseStatsSparkline(functionToken)
	}
	aggregate := StatsAggregate{Range: functionToken.sourceRange, AliasRange: functionToken.sourceRange}
	end := functionToken.sourceRange.End
	functionName := strings.ToLower(functionToken.text)
	spec, supported := statsAggregateSpecForName(functionName)
	if !supported {
		return StatsAggregate{}, end, p.unsupportedStatsAggregate(
			functionToken,
			fmt.Sprintf("stats aggregate %q is not supported by the documented bounded function surface", functionToken.text),
		)
	}
	aggregate.Function = spec.function
	aggregate.Percentile = spec.percentile
	aggregate.Alias = spec.canonicalName
	if functionName == "count" && p.startsCountPredicate() {
		predicate, predicateEnd, predicateErr := p.parseCountPredicate()
		if predicateErr != nil {
			return StatsAggregate{}, end, predicateErr
		}
		aggregate.Function = AggregateFunctionCountPredicate
		aggregate.Predicate = predicate
		end = predicateEnd
		aggregate.Range.End = end
		aggregate.Alias = p.statsAggregateInvocationText(
			functionToken.sourceRange.Start,
			end,
		)
		aggregate.AliasSourceDerived = true
		aggregate.AliasRange = Range{Start: functionToken.sourceRange.Start, End: end}
	}
	parseInput := spec.requiresInput ||
		(spec.inputFunction != AggregateFunctionInvalid && p.current().kind == tokenLeftParen)
	if aggregate.Function != AggregateFunctionCountPredicate && parseInput {
		if spec.supportsExpressionInput && startsEvalPredicateArgument(p.tokens, p.index) {
			expression, expressionEnd, expressionErr := p.parseStatsScalarInput(functionName)
			if expressionErr != nil {
				return StatsAggregate{}, end, expressionErr
			}
			aggregate.InputExpression = expression
			end = expressionEnd
			aggregate.Range.End = end
			aggregate.Alias = p.statsAggregateInvocationText(
				functionToken.sourceRange.Start,
				end,
			)
			aggregate.AliasSourceDerived = true
			aggregate.AliasRange = Range{Start: functionToken.sourceRange.Start, End: end}
		} else if spec.requiresInput && p.current().kind != tokenLeftParen {
			// Deprecated bare input-requiring functions are implicit wc-field
			// aggregates over "*". Planning expands them only when the upstream
			// schema is closed.
			aggregate.InputGlob = &StatsFieldGlob{
				Pattern:  "*",
				Range:    functionToken.sourceRange,
				Implicit: true,
			}
			aggregate.Alias = spec.canonicalName + "(*)"
			if spec.inputFunction != AggregateFunctionInvalid {
				aggregate.Function = spec.inputFunction
			}
		} else {
			if !p.match(tokenLeftParen) {
				return StatsAggregate{}, end, &Diagnostic{
					Code:        "SPL_UNSUPPORTED_STATS_SYNTAX",
					Message:     functionName + " requires one field argument in parentheses",
					Range:       functionToken.sourceRange,
					Suggestions: []string{functionName + "(field)"},
				}
			}
			input := p.current()
			if input.kind == tokenScalarComposite {
				if err := p.prepareSearchToken(); err != nil {
					return StatsAggregate{}, end, err
				}
				input = p.current()
			}
			quotedInput := input.kind == tokenQuotedField
			if quotedInput {
				if input.scalarDiagnostic != nil {
					return StatsAggregate{}, end, input.scalarDiagnostic
				}
				if err := validateQuotedFieldReference(input); err != nil {
					return StatsAggregate{}, end, err
				}
			}
			if input.kind != tokenWord && !quotedInput {
				return StatsAggregate{}, end, p.errorAtCurrent("SPL_EXPECTED_FIELD", functionName+" requires one input field")
			}
			wildcardInput := !quotedInput && IsStatsFieldGlob(input.text)
			if !quotedInput && !wildcardInput && !IsExactUnquotedFieldName(input.text) {
				return StatsAggregate{}, end, p.unsupportedStatsAggregate(
					input,
					"stats aggregate input must be one exact field or wc-field pattern",
				)
			}
			if wildcardInput {
				aggregate.InputGlob = &StatsFieldGlob{
					Pattern: input.text,
					Range:   input.sourceRange,
				}
			} else {
				aggregate.Input = input.text
				aggregate.InputQuoted = quotedInput
				aggregate.InputRange = input.sourceRange
			}
			p.advance()
			if !p.match(tokenRightParen) {
				return StatsAggregate{}, end, p.errorAtCurrent("SPL_EXPECTED_RIGHT_PAREN", "expected ')' after the "+functionName+" input field")
			}
			if spec.inputFunction != AggregateFunctionInvalid {
				aggregate.Function = spec.inputFunction
			}
			end = p.previous().sourceRange.End
			aggregate.Range.End = end
			aggregate.Alias = spec.canonicalName + "(" + input.text + ")"
			aggregate.AliasRange = Range{Start: functionToken.sourceRange.Start, End: end}
		}
	}
	if p.isKeyword("AS") {
		if aggregate.InputGlob != nil {
			p.advance()
			alias := p.current()
			if alias.kind != tokenWord || !IsStatsFieldGlob(alias.text) {
				return StatsAggregate{}, end, p.unsupportedStatsAggregate(
					alias,
					"stats wc-field AS requires one unquoted wc-field output pattern",
				)
			}
			if strings.Count(alias.text, "*") !=
				strings.Count(aggregate.InputGlob.Pattern, "*") {
				return StatsAggregate{}, end, p.unsupportedStatsAggregate(
					alias,
					"stats wc-field AS output must contain exactly one '*' for each input capture",
				)
			}
			aggregate.AliasGlob = &StatsFieldGlob{
				Pattern: alias.text,
				Range:   alias.sourceRange,
			}
			aggregate.ExplicitAlias = true
			aggregate.AliasRange = alias.sourceRange
			aggregate.Range.End = alias.sourceRange.End
			end = alias.sourceRange.End
			p.advance()
			return aggregate, end, nil
		}
		p.advance()
		alias := p.current()
		quotedAlias := alias.kind == tokenString
		if alias.kind != tokenWord && !quotedAlias ||
			(!quotedAlias && p.isKeyword("BY")) {
			return StatsAggregate{}, end, p.errorAtCurrent("SPL_EXPECTED_FIELD", "expected an output field name after AS")
		}
		if quotedAlias {
			if !IsStatsLiteralOutputName(alias.text) {
				return StatsAggregate{}, end, p.unsupportedStatsAggregate(
					alias,
					"quoted stats output field is empty, invalid, private, reserved, or too long",
				)
			}
		} else if !IsExactUnquotedFieldName(alias.text) {
			return StatsAggregate{}, end, p.unsupportedStatsAggregate(
				alias,
				"stats AS requires one exact output field; wildcard aliases are not supported in this slice",
			)
		}
		aggregate.Alias = alias.text
		aggregate.ExplicitAlias = true
		aggregate.AliasQuoted = quotedAlias
		aggregate.AliasSourceDerived = false
		aggregate.AliasRange = alias.sourceRange
		aggregate.Range.End = alias.sourceRange.End
		end = alias.sourceRange.End
		p.advance()
	}
	return aggregate, end, nil
}

func (p *parser) statsAggregateInvocationText(start, end Position) string {
	if start.Offset < 0 || end.Offset <= start.Offset || end.Offset > len(p.source) {
		return ""
	}
	return normalizeStatsSourceDerivedAlias(p.source[start.Offset:end.Offset])
}

func normalizeStatsSourceDerivedAlias(value string) string {
	return strings.Map(func(character rune) rune {
		if unicode.IsControl(character) && unicode.IsSpace(character) {
			return ' '
		}
		return character
	}, value)
}

func statsSparklineAggregateFunctionForName(name string) (AggregateFunction, bool) {
	switch strings.ToLower(name) {
	case "count":
		return AggregateFunctionCount, true
	case "c":
		return AggregateFunctionCountValues, true
	case "dc":
		return AggregateFunctionDistinctCount, true
	case "mean", "avg":
		return AggregateFunctionAverage, true
	case "stdev":
		return AggregateFunctionStandardDeviationSample, true
	case "stdevp":
		return AggregateFunctionStandardDeviationPopulation, true
	case "var":
		return AggregateFunctionVarianceSample, true
	case "varp":
		return AggregateFunctionVariancePopulation, true
	case "sum":
		return AggregateFunctionSum, true
	case "sumsq":
		return AggregateFunctionSumSquares, true
	case "min":
		return AggregateFunctionMinimum, true
	case "max":
		return AggregateFunctionMaximum, true
	case "range":
		return AggregateFunctionRange, true
	default:
		return AggregateFunctionInvalid, false
	}
}

func (p *parser) parseStatsSparkline(
	functionToken token,
) (StatsAggregate, Position, error) {
	aggregate := StatsAggregate{
		Alias:      "sparkline",
		Range:      functionToken.sourceRange,
		AliasRange: functionToken.sourceRange,
	}
	sparkline := &StatsSparkline{
		Span: SparklineSpan{Kind: SparklineSpanKindAutomatic},
	}
	aggregate.Sparkline = sparkline

	// Splunk's documented legacy shorthand is equivalent to an automatic,
	// unscoped count sparkline. Other shorthand functions remain invalid; they
	// require the canonical sparkline(function(field)) wrapper.
	if p.current().kind != tokenLeftParen {
		inner := p.current()
		if inner.kind != tokenWord || !strings.EqualFold(inner.text, "count") {
			return StatsAggregate{}, functionToken.sourceRange.End, &Diagnostic{
				Code:    "SPL_UNSUPPORTED_STATS_SYNTAX",
				Message: "sparkline must contain a supported aggregate or use the legacy 'sparkline count' form",
				Range:   inner.sourceRange,
				Suggestions: []string{
					"sparkline(count)",
					"sparkline(avg(field), 5m) AS trend",
					"sparkline count",
				},
			}
		}
		p.advance()
		sparkline.Function = AggregateFunctionCount
		sparkline.Range = Range{
			Start: functionToken.sourceRange.Start,
			End:   inner.sourceRange.End,
		}
		aggregate.Range = sparkline.Range
		aggregate.AliasRange = sparkline.Range
		return p.parseStatsSparklineAlias(aggregate)
	}

	p.advance()
	innerToken := p.current()
	if innerToken.kind != tokenWord {
		return StatsAggregate{}, functionToken.sourceRange.End, p.errorAtCurrent(
			"SPL_EXPECTED_AGGREGATE",
			"sparkline requires a supported aggregate function",
		)
	}
	p.advance()
	function, supported := statsSparklineAggregateFunctionForName(innerToken.text)
	if !supported {
		return StatsAggregate{}, innerToken.sourceRange.End, p.unsupportedStatsAggregate(
			innerToken,
			fmt.Sprintf("aggregate %q is not supported inside sparkline", innerToken.text),
		)
	}
	sparkline.Function = function

	if strings.EqualFold(innerToken.text, "count") && p.current().kind != tokenLeftParen {
		// Bare count is the only unscoped inner aggregate.
		if p.current().kind != tokenComma && p.current().kind != tokenRightParen {
			return StatsAggregate{}, innerToken.sourceRange.End, p.errorAtCurrent(
				"SPL_EXPECTED_RIGHT_PAREN",
				"expected ')' or a span after unscoped sparkline count",
			)
		}
	} else {
		if !p.match(tokenLeftParen) {
			return StatsAggregate{}, innerToken.sourceRange.End, &Diagnostic{
				Code:        "SPL_UNSUPPORTED_STATS_SYNTAX",
				Message:     strings.ToLower(innerToken.text) + " requires one exact field inside sparkline",
				Range:       innerToken.sourceRange,
				Suggestions: []string{"sparkline(" + strings.ToLower(innerToken.text) + "(field))"},
			}
		}
		if strings.EqualFold(innerToken.text, "count") && p.match(tokenRightParen) {
			// count() is the parenthesized spelling of unscoped row count.
			sparkline.Function = AggregateFunctionCount
		} else {
			input := p.current()
			if input.kind == tokenScalarComposite {
				if err := p.prepareSearchToken(); err != nil {
					return StatsAggregate{}, innerToken.sourceRange.End, err
				}
				input = p.current()
			}
			quotedInput := input.kind == tokenQuotedField
			if quotedInput {
				if input.scalarDiagnostic != nil {
					return StatsAggregate{}, input.sourceRange.End, input.scalarDiagnostic
				}
				if err := validateQuotedFieldReference(input); err != nil {
					return StatsAggregate{}, input.sourceRange.End, err
				}
			}
			if input.kind != tokenWord && !quotedInput {
				return StatsAggregate{}, input.sourceRange.End, p.errorAtCurrent(
					"SPL_EXPECTED_FIELD",
					strings.ToLower(innerToken.text)+" requires one exact sparkline input field",
				)
			}
			wildcardInput := !quotedInput && IsStatsFieldGlob(input.text)
			if !quotedInput && !wildcardInput && !IsExactUnquotedFieldName(input.text) {
				return StatsAggregate{}, input.sourceRange.End, p.unsupportedStatsAggregate(
					input,
					"sparkline input must be one exact field or wc-field pattern",
				)
			}
			if wildcardInput {
				sparkline.InputGlob = &StatsFieldGlob{
					Pattern: input.text,
					Range:   input.sourceRange,
				}
			} else {
				sparkline.Input = input.text
				sparkline.InputQuoted = quotedInput
				sparkline.InputRange = input.sourceRange
			}
			p.advance()
			if !p.match(tokenRightParen) {
				return StatsAggregate{}, input.sourceRange.End, p.errorAtCurrent(
					"SPL_EXPECTED_RIGHT_PAREN",
					"expected ')' after the sparkline input field",
				)
			}
			if strings.EqualFold(innerToken.text, "count") {
				sparkline.Function = AggregateFunctionCountValues
			}
		}
	}

	if p.match(tokenComma) {
		spanToken := p.current()
		span, err := parseSparklineSpan(spanToken)
		if err != nil {
			return StatsAggregate{}, spanToken.sourceRange.End, err
		}
		sparkline.Span = span
		p.advance()
	}
	if !p.match(tokenRightParen) {
		return StatsAggregate{}, p.current().sourceRange.End, p.errorAtCurrent(
			"SPL_EXPECTED_RIGHT_PAREN",
			"expected ')' to close sparkline",
		)
	}
	end := p.previous().sourceRange.End
	sparkline.Range = Range{Start: functionToken.sourceRange.Start, End: end}
	aggregate.Range = sparkline.Range
	aggregate.AliasRange = sparkline.Range
	return p.parseStatsSparklineAlias(aggregate)
}

func (p *parser) parseStatsSparklineAlias(
	aggregate StatsAggregate,
) (StatsAggregate, Position, error) {
	end := aggregate.Range.End
	if !p.isKeyword("AS") {
		return aggregate, end, nil
	}
	p.advance()
	alias := p.current()
	if aggregate.Sparkline.InputGlob != nil {
		if alias.kind != tokenWord || !IsStatsFieldGlob(alias.text) {
			return StatsAggregate{}, end, p.unsupportedStatsAggregate(
				alias,
				"sparkline wc-field AS requires one unquoted wc-field output pattern",
			)
		}
		if strings.Count(alias.text, "*") !=
			strings.Count(aggregate.Sparkline.InputGlob.Pattern, "*") {
			return StatsAggregate{}, end, p.unsupportedStatsAggregate(
				alias,
				"sparkline wc-field AS output must contain exactly one '*' for each input capture",
			)
		}
		aggregate.AliasGlob = &StatsFieldGlob{
			Pattern: alias.text,
			Range:   alias.sourceRange,
		}
		aggregate.ExplicitAlias = true
		aggregate.AliasRange = alias.sourceRange
		aggregate.Range.End = alias.sourceRange.End
		end = alias.sourceRange.End
		p.advance()
		return aggregate, end, nil
	}
	quotedAlias := alias.kind == tokenString
	if alias.kind != tokenWord && !quotedAlias ||
		(!quotedAlias && p.isKeyword("BY")) {
		return StatsAggregate{}, end, p.errorAtCurrent(
			"SPL_EXPECTED_FIELD",
			"expected an output field name after sparkline AS",
		)
	}
	if quotedAlias {
		if !IsStatsLiteralOutputName(alias.text) {
			return StatsAggregate{}, end, p.unsupportedStatsAggregate(
				alias,
				"quoted sparkline output field is empty, invalid, private, reserved, or too long",
			)
		}
	} else if !IsExactUnquotedFieldName(alias.text) {
		return StatsAggregate{}, end, p.unsupportedStatsAggregate(
			alias,
			"sparkline AS requires one exact output field; wildcard aliases are not supported in this slice",
		)
	}
	aggregate.Alias = alias.text
	aggregate.ExplicitAlias = true
	aggregate.AliasQuoted = quotedAlias
	aggregate.AliasRange = alias.sourceRange
	aggregate.Range.End = alias.sourceRange.End
	p.advance()
	return aggregate, alias.sourceRange.End, nil
}

func parseSparklineSpan(tok token) (SparklineSpan, error) {
	if tok.kind != tokenWord {
		return SparklineSpan{}, invalidSparklineSpan(tok)
	}
	digitEnd := 0
	for digitEnd < len(tok.text) && tok.text[digitEnd] >= '0' && tok.text[digitEnd] <= '9' {
		digitEnd++
	}
	if digitEnd == 0 || digitEnd == len(tok.text) {
		return SparklineSpan{}, invalidSparklineSpan(tok)
	}
	magnitude, err := strconv.ParseUint(tok.text[:digitEnd], 10, 64)
	if err != nil {
		return SparklineSpan{}, &Diagnostic{
			Code:    "SPL_NUMBER_OUT_OF_RANGE",
			Message: "sparkline span magnitude is outside the supported 64-bit range",
			Range:   tok.sourceRange,
		}
	}
	if magnitude == 0 {
		return SparklineSpan{}, invalidSparklineSpan(tok)
	}

	var unit SparklineSpanUnit
	var unitsPerSecond uint64
	switch strings.ToLower(tok.text[digitEnd:]) {
	case "us":
		unit, unitsPerSecond = SparklineSpanUnitMicrosecond, 1_000_000
	case "ms":
		unit, unitsPerSecond = SparklineSpanUnitMillisecond, 1_000
	case "cs":
		unit, unitsPerSecond = SparklineSpanUnitCentisecond, 100
	case "ds":
		unit, unitsPerSecond = SparklineSpanUnitDecisecond, 10
	case "s", "sec", "secs", "second", "seconds":
		unit = SparklineSpanUnitSecond
	case "m", "min", "mins", "minute", "minutes":
		unit = SparklineSpanUnitMinute
	case "h", "hr", "hrs", "hour", "hours":
		unit = SparklineSpanUnitHour
	case "d", "day", "days":
		unit = SparklineSpanUnitDay
	case "mon", "month", "months":
		unit = SparklineSpanUnitMonth
	default:
		return SparklineSpan{}, &Diagnostic{
			Code:    "SPL_UNSUPPORTED_STATS_SYNTAX",
			Message: fmt.Sprintf("sparkline span unit in %q is unsupported", tok.text),
			Range:   tok.sourceRange,
			Suggestions: []string{
				"sparkline(count, 30m)",
				"sparkline(avg(field), 6h)",
			},
		}
	}
	if unitsPerSecond != 0 &&
		(magnitude >= unitsPerSecond || unitsPerSecond%magnitude != 0) {
		return SparklineSpan{}, &Diagnostic{
			Code: "SPL_INVALID_ARGUMENT",
			Message: fmt.Sprintf(
				"sparkline subsecond span %q must divide one second evenly and be less than one second",
				tok.text,
			),
			Range:       tok.sourceRange,
			Suggestions: []string{"use an exact divisor of one second, or use 1s"},
		}
	}
	return SparklineSpan{
		Kind:      SparklineSpanKindExplicit,
		Magnitude: magnitude,
		Unit:      unit,
		Range:     tok.sourceRange,
	}, nil
}

func invalidSparklineSpan(tok token) *Diagnostic {
	return &Diagnostic{
		Code:    "SPL_INVALID_ARGUMENT",
		Message: "sparkline span must be a positive integer followed by a documented time unit",
		Range:   tok.sourceRange,
		Suggestions: []string{
			"sparkline(count, 30m)",
			"sparkline(avg(field), 6h)",
		},
	}
}

// parseStatsScalarInput parses the general field-taking aggregate wrapper
// function(eval(<authored scalar expression>)). count(eval(...)) is parsed first as
// its distinct predicate form; every other field-taking stats function retains
// the scalar result kind, including Boolean values.
func (p *parser) parseStatsScalarInput(functionName string) (ScalarExpr, Position, error) {
	if !p.match(tokenLeftParen) {
		return nil, p.current().sourceRange.End, p.errorAtCurrent(
			"SPL_EXPECTED_LEFT_PAREN",
			"expected '(' after "+functionName,
		)
	}
	p.advance() // startsEvalPredicateArgument proved this token is eval.
	if !p.match(tokenLeftParen) {
		return nil, p.current().sourceRange.End, p.errorAtCurrent(
			"SPL_EXPECTED_LEFT_PAREN",
			"expected '(' after eval",
		)
	}
	if p.current().kind == tokenRightParen {
		return nil, p.current().sourceRange.End, p.errorAtCurrent(
			"SPL_EXPECTED_SCALAR_EXPRESSION",
			functionName+"(eval(...)) requires a scalar expression",
		)
	}
	expression, err := p.parseScalarExpression()
	if err != nil {
		return nil, p.current().sourceRange.End, err
	}
	if !p.match(tokenRightParen) {
		return nil, expression.SourceRange().End, p.errorAtCurrent(
			"SPL_EXPECTED_RIGHT_PAREN",
			"expected ')' to close the eval scalar expression",
		)
	}
	if !p.match(tokenRightParen) {
		return nil, expression.SourceRange().End, p.errorAtCurrent(
			"SPL_EXPECTED_RIGHT_PAREN",
			"expected ')' to close "+functionName+"(eval(...))",
		)
	}
	return expression, p.previous().sourceRange.End, nil
}

func (p *parser) startsCountPredicate() bool {
	return startsEvalPredicateArgument(p.tokens, p.index)
}

func startsEvalPredicateArgument(tokens []token, leftParenthesis int) bool {
	return leftParenthesis >= 0 && leftParenthesis+2 < len(tokens) &&
		tokens[leftParenthesis].kind == tokenLeftParen &&
		tokens[leftParenthesis+1].kind == tokenWord &&
		strings.EqualFold(tokens[leftParenthesis+1].text, "eval") &&
		tokens[leftParenthesis+2].kind == tokenLeftParen
}

func startsCountEvalCall(tokens []token, function int) bool {
	return function >= 0 && function < len(tokens) &&
		tokenWordEqual(tokens[function], "count") &&
		startsEvalPredicateArgument(tokens, function+1)
}

// parseCountPredicate parses the shared count(eval(<Boolean predicate>))
// grammar. Stats, eventstats, and streamstats call this helper so predicate
// precedence, source ranges, scalar validation, and the query-wide predicate
// budget cannot drift between commands.
func (p *parser) parseCountPredicate() (WhereExpr, Position, error) {
	if !p.match(tokenLeftParen) {
		return nil, p.current().sourceRange.End, p.errorAtCurrent(
			"SPL_EXPECTED_LEFT_PAREN",
			"expected '(' after count",
		)
	}
	p.advance() // startsCountPredicate proved this token is eval.
	if !p.match(tokenLeftParen) {
		return nil, p.current().sourceRange.End, p.errorAtCurrent(
			"SPL_EXPECTED_LEFT_PAREN",
			"expected '(' after eval",
		)
	}
	if p.current().kind == tokenRightParen {
		return nil, p.current().sourceRange.End, p.errorAtCurrent(
			"SPL_EXPECTED_EXPRESSION",
			"count(eval(...)) requires a Boolean predicate",
		)
	}
	predicate, predicateErr := p.parseWhereExpression()
	if predicateErr != nil {
		return nil, p.current().sourceRange.End, predicateErr
	}
	if !p.match(tokenRightParen) {
		if p.canStartWhereOperand() {
			return nil, p.current().sourceRange.End, &Diagnostic{
				Code:    "SPL_UNSUPPORTED_WHERE_EXPRESSION",
				Message: "count(eval(...)) requires explicit AND or OR between predicates",
				Range:   p.current().sourceRange,
				Suggestions: []string{
					"count(eval(field=value AND other_field=value)) AS matches",
				},
			}
		}
		return nil, p.current().sourceRange.End, p.errorAtCurrent(
			"SPL_EXPECTED_RIGHT_PAREN",
			"expected ')' to close the eval predicate",
		)
	}
	if !p.match(tokenRightParen) {
		return nil, p.current().sourceRange.End, p.errorAtCurrent(
			"SPL_EXPECTED_RIGHT_PAREN",
			"expected ')' to close count(eval(...))",
		)
	}
	return predicate, p.previous().sourceRange.End, nil
}

type statsAggregateSpec struct {
	function                AggregateFunction
	inputFunction           AggregateFunction
	canonicalName           string
	requiresInput           bool
	supportsExpressionInput bool
	percentile              uint8
}

type eventStatsFieldAggregateDescriptor struct {
	name       string
	function   AggregateFunction
	form       string
	suggestion string
}

// eventStatsFieldAggregateDescriptors is the ordered authority for the
// exact-field eventstats surface shared by parsing, diagnostics, and editor
// suggestions. Row count remains separate because only its parenthesized
// forms require an alias.
var eventStatsFieldAggregateDescriptors = [...]eventStatsFieldAggregateDescriptor{
	{
		name:       "min",
		function:   AggregateFunctionMinimum,
		form:       "min(field)",
		suggestion: "eventstats min(field) AS minimum",
	},
	{
		name:       "max",
		function:   AggregateFunctionMaximum,
		form:       "max(field)",
		suggestion: "eventstats max(field) AS maximum",
	},
	{
		name:       "earliest",
		function:   AggregateFunctionEarliest,
		form:       "earliest(field)",
		suggestion: "eventstats earliest(field) AS first_value",
	},
	{
		name:       "latest",
		function:   AggregateFunctionLatest,
		form:       "latest(field)",
		suggestion: "eventstats latest(field) AS last_value",
	},
	{
		name:       "sum",
		function:   AggregateFunctionSum,
		form:       "sum(field)",
		suggestion: "eventstats sum(field) AS total",
	},
	{
		name:       "avg",
		function:   AggregateFunctionAverage,
		form:       "avg(field)",
		suggestion: "eventstats avg(field) AS mean",
	},
	{
		name:       "dc",
		function:   AggregateFunctionDistinctCount,
		form:       "dc(field)",
		suggestion: "eventstats dc(field) AS distinct_values",
	},
	{
		name:       "values",
		function:   AggregateFunctionValues,
		form:       "values(field)",
		suggestion: "eventstats values(field) AS distinct_values",
	},
	{
		name:       "list",
		function:   AggregateFunctionList,
		form:       "list(field)",
		suggestion: "eventstats list(field) AS ordered_values",
	},
}

func eventStatsAcceptedAggregateForms() string {
	forms := []string{
		"count",
		"count(field) AS output",
		"count(eval(predicate)) AS output",
		"pN/percN(field) AS output for N from 1 through 99",
	}
	for _, descriptor := range eventStatsFieldAggregateDescriptors {
		forms = append(forms, descriptor.form+" AS output")
	}
	return strings.Join(forms, ", ")
}

func eventStatsFunctionNames() []string {
	names := make([]string, 0, len(eventStatsFieldAggregateDescriptors)+3)
	names = append(names, "count", "p50", "p95")
	for _, descriptor := range eventStatsFieldAggregateDescriptors {
		names = append(names, descriptor.name)
	}
	return names
}

func eventStatsFieldAggregateDescriptorForName(
	name string,
) (eventStatsFieldAggregateDescriptor, bool) {
	name = strings.ToLower(name)
	for _, descriptor := range eventStatsFieldAggregateDescriptors {
		if descriptor.name == name {
			return descriptor, true
		}
	}
	return eventStatsFieldAggregateDescriptor{}, false
}

func eventStatsFieldAggregateDescriptorForFunction(
	function AggregateFunction,
) (eventStatsFieldAggregateDescriptor, bool) {
	for _, descriptor := range eventStatsFieldAggregateDescriptors {
		if descriptor.function == function {
			return descriptor, true
		}
	}
	return eventStatsFieldAggregateDescriptor{}, false
}

func statsAggregateSpecForName(name string) (statsAggregateSpec, bool) {
	name = strings.ToLower(name)
	if percentile, ok := parseStatsPercentileSuffix(name); ok {
		return statsAggregateSpec{
			function:                AggregateFunctionPercentile,
			canonicalName:           "perc" + strconv.Itoa(int(percentile)),
			requiresInput:           true,
			supportsExpressionInput: true,
			percentile:              percentile,
		}, true
	}
	if percentile, ok := parseBoundedStatsFunctionSuffix(name, "exactperc"); ok {
		return statsAggregateSpec{
			function:                AggregateFunctionExactPercentile,
			canonicalName:           "exactperc" + strconv.Itoa(int(percentile)),
			requiresInput:           true,
			supportsExpressionInput: true,
			percentile:              percentile,
		}, true
	}
	if percentile, ok := parseBoundedStatsFunctionSuffix(name, "upperperc"); ok {
		return statsAggregateSpec{
			function:                AggregateFunctionUpperPercentile,
			canonicalName:           "upperperc" + strconv.Itoa(int(percentile)),
			requiresInput:           true,
			supportsExpressionInput: true,
			percentile:              percentile,
		}, true
	}
	switch name {
	case "count":
		return statsAggregateSpec{
			function:      AggregateFunctionCount,
			inputFunction: AggregateFunctionCountValues,
			canonicalName: "count",
		}, true
	case "c":
		return statsAggregateSpec{
			function:      AggregateFunctionCountValues,
			canonicalName: "count",
			requiresInput: true,
		}, true
	case "sum":
		return statsAggregateSpec{function: AggregateFunctionSum, canonicalName: "sum", requiresInput: true, supportsExpressionInput: true}, true
	case "avg":
		return statsAggregateSpec{function: AggregateFunctionAverage, canonicalName: "avg", requiresInput: true, supportsExpressionInput: true}, true
	case "mean":
		return statsAggregateSpec{function: AggregateFunctionAverage, canonicalName: "mean", requiresInput: true, supportsExpressionInput: true}, true
	case "range":
		return statsAggregateSpec{function: AggregateFunctionRange, canonicalName: "range", requiresInput: true, supportsExpressionInput: true}, true
	case "sumsq":
		return statsAggregateSpec{function: AggregateFunctionSumSquares, canonicalName: "sumsq", requiresInput: true, supportsExpressionInput: true}, true
	case "stdev":
		return statsAggregateSpec{function: AggregateFunctionStandardDeviationSample, canonicalName: "stdev", requiresInput: true, supportsExpressionInput: true}, true
	case "stdevp":
		return statsAggregateSpec{function: AggregateFunctionStandardDeviationPopulation, canonicalName: "stdevp", requiresInput: true, supportsExpressionInput: true}, true
	case "var":
		return statsAggregateSpec{function: AggregateFunctionVarianceSample, canonicalName: "var", requiresInput: true, supportsExpressionInput: true}, true
	case "varp":
		return statsAggregateSpec{function: AggregateFunctionVariancePopulation, canonicalName: "varp", requiresInput: true, supportsExpressionInput: true}, true
	case "dc", "distinct_count":
		return statsAggregateSpec{function: AggregateFunctionDistinctCount, canonicalName: "dc", requiresInput: true, supportsExpressionInput: true}, true
	case "estdc":
		return statsAggregateSpec{function: AggregateFunctionEstimatedDistinctCount, canonicalName: "estdc", requiresInput: true, supportsExpressionInput: true}, true
	case "estdc_error":
		return statsAggregateSpec{function: AggregateFunctionEstimatedDistinctCountError, canonicalName: "estdc_error", requiresInput: true, supportsExpressionInput: true}, true
	case "median":
		return statsAggregateSpec{function: AggregateFunctionMedian, canonicalName: "median", requiresInput: true, supportsExpressionInput: true}, true
	case "mode":
		return statsAggregateSpec{function: AggregateFunctionMode, canonicalName: "mode", requiresInput: true, supportsExpressionInput: true}, true
	case "values":
		return statsAggregateSpec{function: AggregateFunctionValues, canonicalName: "values", requiresInput: true, supportsExpressionInput: true}, true
	case "list":
		return statsAggregateSpec{function: AggregateFunctionList, canonicalName: "list", requiresInput: true, supportsExpressionInput: true}, true
	case "min":
		return statsAggregateSpec{function: AggregateFunctionMinimum, canonicalName: "min", requiresInput: true, supportsExpressionInput: true}, true
	case "max":
		return statsAggregateSpec{function: AggregateFunctionMaximum, canonicalName: "max", requiresInput: true, supportsExpressionInput: true}, true
	case "first":
		return statsAggregateSpec{function: AggregateFunctionFirst, canonicalName: "first", requiresInput: true, supportsExpressionInput: true}, true
	case "last":
		return statsAggregateSpec{function: AggregateFunctionLast, canonicalName: "last", requiresInput: true, supportsExpressionInput: true}, true
	case "earliest":
		return statsAggregateSpec{function: AggregateFunctionEarliest, canonicalName: "earliest", requiresInput: true, supportsExpressionInput: true}, true
	case "latest":
		return statsAggregateSpec{function: AggregateFunctionLatest, canonicalName: "latest", requiresInput: true, supportsExpressionInput: true}, true
	case "earliest_time":
		return statsAggregateSpec{function: AggregateFunctionEarliestTime, canonicalName: "earliest_time", requiresInput: true, supportsExpressionInput: true}, true
	case "latest_time":
		return statsAggregateSpec{function: AggregateFunctionLatestTime, canonicalName: "latest_time", requiresInput: true, supportsExpressionInput: true}, true
	case "rate":
		return statsAggregateSpec{function: AggregateFunctionRate, canonicalName: "rate", requiresInput: true, supportsExpressionInput: true}, true
	default:
		return statsAggregateSpec{}, false
	}
}

func eventStatsFieldAggregateSpecForName(
	name string,
) (statsAggregateSpec, bool) {
	name = strings.ToLower(name)
	if percentile, supported := parseStatsPercentileSuffix(name); supported {
		prefix := "p"
		if strings.HasPrefix(name, "perc") {
			prefix = "perc"
		}
		return statsAggregateSpec{
			function:      AggregateFunctionPercentile,
			canonicalName: prefix + strconv.Itoa(int(percentile)),
			requiresInput: true,
			percentile:    percentile,
		}, true
	}
	descriptor, supported := eventStatsFieldAggregateDescriptorForName(name)
	if !supported {
		return statsAggregateSpec{}, false
	}
	return statsAggregateSpec{
		function:      descriptor.function,
		canonicalName: descriptor.name,
		requiresInput: true,
	}, true
}

func eventStatsFieldAggregatePresentation(
	spec statsAggregateSpec,
) (string, string, bool) {
	if spec.function == AggregateFunctionPercentile &&
		spec.percentile >= 1 && spec.percentile <= 99 &&
		(spec.canonicalName == "p"+strconv.Itoa(int(spec.percentile)) ||
			spec.canonicalName == "perc"+strconv.Itoa(int(spec.percentile))) {
		form := spec.canonicalName + "(field)"
		return form, "eventstats " + form + " AS " + spec.canonicalName + "_value", true
	}
	descriptor, supported := eventStatsFieldAggregateDescriptorForFunction(
		spec.function,
	)
	if !supported {
		return "", "", false
	}
	return descriptor.form, descriptor.suggestion, true
}

func parseStatsPercentileSuffix(name string) (uint8, bool) {
	switch {
	case strings.HasPrefix(name, "perc"):
		return parseBoundedStatsFunctionSuffix(name, "perc")
	case strings.HasPrefix(name, "p"):
		return parseBoundedStatsFunctionSuffix(name, "p")
	default:
		return 0, false
	}
}

// parseBoundedStatsFunctionSuffix intentionally retains the current integer
// 1..99 percentile surface. The pinned SPL1 pages disagree on broader suffix
// ranges and percentile algorithms, so widening either requires oracle-backed
// evidence rather than parser inference.
func parseBoundedStatsFunctionSuffix(name, prefix string) (uint8, bool) {
	if !strings.HasPrefix(name, prefix) {
		return 0, false
	}
	suffix := strings.TrimPrefix(name, prefix)
	if suffix == "" {
		return 0, false
	}
	value, err := strconv.ParseUint(suffix, 10, 8)
	if err != nil || value < 1 || value > 99 {
		return 0, false
	}
	return uint8(value), true
}

func supportedStatsAggregateName(name string) bool {
	if strings.EqualFold(name, "sparkline") {
		return true
	}
	_, supported := statsAggregateSpecForName(name)
	return supported
}

func (p *parser) parseBoundedAggregateGroupFields(
	commandName string,
	article string,
	requireExactDistinctFields bool,
	stopBeforeOption bool,
	unsupportedSyntax func(token, string) *Diagnostic,
) ([]StatsGroupField, Position, error) {
	fields := make([]StatsGroupField, 0, 4)
	var seen map[string]struct{}
	if requireExactDistinctFields {
		seen = make(map[string]struct{}, 4)
	}
	end := p.current().sourceRange.Start
	wantField := true
	for !p.atCommandEnd() {
		if !wantField && stopBeforeOption &&
			statsOptionFollowedByEqual(p.tokens, p.index) {
			break
		}
		if p.current().kind == tokenScalarComposite {
			if err := p.prepareSearchToken(); err != nil {
				return nil, end, err
			}
		}
		tok := p.current()
		if tok.kind == tokenComma {
			if wantField {
				return nil, end, p.errorAtCurrent(
					"SPL_EXPECTED_FIELD",
					fmt.Sprintf("expected %s %s grouping field", article, commandName),
				)
			}
			wantField = true
			p.advance()
			continue
		}
		quotedField := commandName == "stats" && tok.kind == tokenQuotedField
		if quotedField {
			if tok.scalarDiagnostic != nil {
				return nil, end, tok.scalarDiagnostic
			}
			if err := validateQuotedFieldReference(tok); err != nil {
				return nil, end, err
			}
		}
		if tok.kind != tokenWord && !quotedField {
			return nil, end, p.errorAtCurrent(
				"SPL_EXPECTED_FIELD",
				fmt.Sprintf("expected %s %s grouping field", article, commandName),
			)
		}
		if !quotedField && strings.EqualFold(tok.text, "AS") {
			return nil, end, unsupportedSyntax(
				tok,
				fmt.Sprintf("%s %s aggregate alias must appear before the BY clause", article, commandName),
			)
		}
		if strings.Contains(tok.text, "*") {
			return nil, end, unsupportedSyntax(
				tok,
				fmt.Sprintf("wildcard %s grouping fields are not supported", commandName),
			)
		}
		if _, duplicate := seen[tok.text]; requireExactDistinctFields && duplicate {
			return nil, end, unsupportedSyntax(
				tok,
				fmt.Sprintf("%s grouping field %q is repeated", commandName, tok.text),
			)
		}
		if len(fields) >= MaximumStatsGroupFields {
			return nil, end, &Diagnostic{
				Code:    "SPL_QUERY_TOO_COMPLEX",
				Message: fmt.Sprintf("%s BY contains more than %d grouping fields", commandName, MaximumStatsGroupFields),
				Range:   tok.sourceRange,
			}
		}
		if requireExactDistinctFields {
			seen[tok.text] = struct{}{}
		}
		fields = append(fields, StatsGroupField{
			Name:   tok.text,
			Quoted: quotedField,
			Range:  tok.sourceRange,
		})
		end = tok.sourceRange.End
		wantField = false
		p.advance()
	}
	if len(fields) == 0 || wantField {
		return nil, end, p.errorAtCurrent(
			"SPL_EXPECTED_FIELD",
			fmt.Sprintf("%s BY requires at least one field", commandName),
		)
	}
	return fields, end, nil
}

func (p *parser) unsupportedStatsGroupSyntax(tok token, message string) *Diagnostic {
	return &Diagnostic{
		Code:        "SPL_UNSUPPORTED_STATS_SYNTAX",
		Message:     message,
		Range:       tok.sourceRange,
		Suggestions: []string{"stats count AS total BY field"},
	}
}

func (p *parser) unsupportedStatsOption(tok token, message string) *Diagnostic {
	return &Diagnostic{
		Code:    "SPL_UNSUPPORTED_STATS_SYNTAX",
		Message: message,
		Range:   tok.sourceRange,
		Suggestions: []string{
			"stats partitions=1 allnum=false delim=\",\" count BY field dedup_splitvals=false",
		},
	}
}

func (p *parser) unsupportedStatsAggregate(tok token, message string) *Diagnostic {
	return &Diagnostic{
		Code:        "SPL_UNSUPPORTED_STATS_AGGREGATE",
		Message:     message,
		Range:       tok.sourceRange,
		Suggestions: []string{"stats count", "stats dc(field) BY group", "stats earliest(field) latest(field) BY group", "stats min(field) max(field) BY group", "stats sum(field) avg(field) BY group", "stats p50(field) p95(field) BY group"},
	}
}
