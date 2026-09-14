package spl

import (
	"fmt"
	"strconv"
	"strings"
)

func streamStatsAggregateDescriptorForName(
	name string,
) (streamStatsAggregateDescriptor, bool) {
	for _, descriptor := range streamStatsAggregateDescriptors {
		if strings.EqualFold(descriptor.name, name) {
			return descriptor, true
		}
	}
	return streamStatsAggregateDescriptor{}, false
}

func streamStatsFunctionNames() []string {
	names := make([]string, 0, len(streamStatsAggregateDescriptors))
	for _, descriptor := range streamStatsAggregateDescriptors {
		names = append(names, descriptor.name)
	}
	return names
}

func streamStatsAcceptedAggregateForms() string {
	forms := make([]string, 0, len(streamStatsAggregateDescriptors)+2)
	for _, descriptor := range streamStatsAggregateDescriptors {
		if descriptor.allowsBare {
			forms = append(forms, descriptor.name)
		}
		forms = append(forms, descriptor.name+"(field)")
		if descriptor.allowsPredicate {
			forms = append(forms, "count(eval(predicate)) AS output")
		}
	}
	if len(forms) == 1 {
		return forms[0]
	}
	return strings.Join(forms[:len(forms)-1], ", ") + ", or " + forms[len(forms)-1]
}

// parseStreamStatsCommand accepts one deliberately bounded running count,
// conditional count, exact-field numeric sum or average, exact-field mixed
// extremum, or exact-field chronological value. Splunk commonly places options
// both before and after the aggregate (and examples also place them after BY),
// so this parser treats supported name=value options as position-independent
// while keeping the aggregate, alias, and grouping tuple exact.
func (p *parser) parseStreamStatsCommand(name token) (Command, error) {
	command := &StreamStatsCommand{
		Current: true,
		Global:  true,
	}
	acceptedForms := streamStatsAcceptedAggregateForms()
	var (
		aggregateSeen bool
		aliasSeen     bool
		bySeen        bool
		byClosed      bool
		currentSeen   bool
		windowSeen    bool
		globalSeen    bool
		end           = name.sourceRange.End
	)

	for !p.atCommandEnd() {
		current := p.current()
		followedByEqual := current.kind == tokenWord &&
			p.index+1 < len(p.tokens) && p.tokens[p.index+1].kind == tokenEqual
		if followedByEqual {
			optionName := strings.ToLower(current.text)
			option := current
			p.advance()
			p.advance() // '=' was established by lookahead.
			value := p.current()
			if value.kind != tokenWord {
				if value.kind == tokenEOF || value.kind == tokenPipe {
					value = option
				}
				return nil, p.unsupportedStreamStatsSyntax(
					value,
					fmt.Sprintf("streamstats option %q requires an unquoted value", option.text),
				)
			}

			switch optionName {
			case "current":
				if currentSeen {
					return nil, p.unsupportedStreamStatsSyntax(option, "streamstats current may be specified only once")
				}
				parsed, ok := parseStreamStatsBool(value.text)
				if !ok {
					return nil, p.unsupportedStreamStatsSyntax(value, "streamstats current must be t, true, f, or false")
				}
				currentSeen = true
				command.Current = parsed
				command.CurrentSpecified = true
				command.CurrentRange = Range{Start: option.sourceRange.Start, End: value.sourceRange.End}
			case "window":
				if windowSeen {
					return nil, p.unsupportedStreamStatsSyntax(option, "streamstats window may be specified only once")
				}
				if !unsignedIntegerSyntax(value.text) {
					return nil, p.unsupportedStreamStatsSyntax(
						value,
						fmt.Sprintf("streamstats window must be an unsigned base-10 integer from 0 through %d", MaximumStreamStatsWindow),
					)
				}
				parsed, err := strconv.ParseUint(value.text, 10, 64)
				if err != nil || parsed > MaximumStreamStatsWindow {
					return nil, p.unsupportedStreamStatsSyntax(
						value,
						fmt.Sprintf("streamstats window must be an unsigned base-10 integer from 0 through %d", MaximumStreamStatsWindow),
					)
				}
				windowSeen = true
				command.Window = parsed
				command.WindowSpecified = true
				command.WindowRange = Range{Start: option.sourceRange.Start, End: value.sourceRange.End}
			case "global":
				if globalSeen {
					return nil, p.unsupportedStreamStatsSyntax(option, "streamstats global may be specified only once")
				}
				parsed, ok := parseStreamStatsBool(value.text)
				if !ok {
					return nil, p.unsupportedStreamStatsSyntax(value, "streamstats global must be t, true, f, or false")
				}
				globalSeen = true
				command.Global = parsed
				command.GlobalSpecified = true
				command.GlobalRange = Range{Start: option.sourceRange.Start, End: value.sourceRange.End}
			case "time_window", "allnum", "reset_before", "reset_after", "reset_on_change":
				return nil, p.unsupportedStreamStatsSyntax(
					option,
					fmt.Sprintf("streamstats option %q is not supported", option.text),
				)
			default:
				return nil, p.unsupportedStreamStatsSyntax(
					option,
					fmt.Sprintf("streamstats option %q is not supported", option.text),
				)
			}
			end = value.sourceRange.End
			p.advance()
			if bySeen {
				byClosed = true
			}
			continue
		}

		if current.kind == tokenWord {
			optionName := strings.ToLower(current.text)
			if optionName == "current" || optionName == "window" || optionName == "global" ||
				optionName == "time_window" || optionName == "allnum" ||
				optionName == "reset_before" || optionName == "reset_after" ||
				optionName == "reset_on_change" {
				return nil, &Diagnostic{
					Code:        "SPL_EXPECTED_EQUAL",
					Message:     fmt.Sprintf("streamstats option %q must be followed by '='", current.text),
					Range:       current.sourceRange,
					Suggestions: []string{streamStatsSyntaxSuggestion},
				}
			}
		}

		descriptor, supportedAggregate := streamStatsAggregateDescriptorForName(
			current.text,
		)
		if current.kind == tokenWord && supportedAggregate {
			if aggregateSeen || bySeen {
				return nil, p.unsupportedStreamStatsAggregate(current, "streamstats supports exactly one aggregate")
			}
			hasArguments := p.index+1 < len(p.tokens) &&
				p.tokens[p.index+1].kind == tokenLeftParen
			if hasArguments {
				evalArgument := startsEvalPredicateArgument(
					p.tokens,
					p.index+1,
				)
				if evalArgument && descriptor.allowsPredicate {
					functionToken := current
					p.advance()
					predicate, predicateEnd, predicateErr := p.parseCountPredicate()
					if predicateErr != nil {
						return nil, predicateErr
					}
					aggregateSeen = true
					command.Aggregate = StatsAggregate{
						Function:  AggregateFunctionCountPredicate,
						Predicate: predicate,
						Range: Range{
							Start: functionToken.sourceRange.Start,
							End:   predicateEnd,
						},
						AliasRange: Range{
							Start: functionToken.sourceRange.Start,
							End:   predicateEnd,
						},
					}
					end = predicateEnd
					continue
				}
				if evalArgument ||
					p.index+2 < len(p.tokens) && p.tokens[p.index+2].kind == tokenRightParen {
					return nil, p.unsupportedStreamStatsAggregate(
						current,
						fmt.Sprintf("streamstats %s(field) requires one exact field, not %s() or %s(eval(...))", descriptor.name, descriptor.name, descriptor.name),
					)
				}
				aggregate, aggregateEnd, aggregateErr := p.parseStreamStatsFieldAggregate(
					current,
					descriptor,
				)
				if aggregateErr != nil {
					return nil, aggregateErr
				}
				aggregateSeen = true
				command.Aggregate = aggregate
				end = aggregateEnd
				continue
			}
			if !descriptor.allowsBare {
				return nil, p.unsupportedStreamStatsAggregate(
					current,
					fmt.Sprintf("streamstats %s requires one exact field in parentheses", descriptor.name),
				)
			}
			aggregateSeen = true
			command.Aggregate = StatsAggregate{
				Function:   AggregateFunctionCount,
				Alias:      "count",
				Range:      current.sourceRange,
				AliasRange: current.sourceRange,
			}
			end = current.sourceRange.End
			p.advance()
			continue
		}

		if current.kind == tokenWord && strings.EqualFold(current.text, "AS") {
			if !aggregateSeen || aliasSeen || bySeen {
				return nil, p.unsupportedStreamStatsSyntax(current, "streamstats AS must follow its aggregate and may appear only once before BY")
			}
			asToken := current
			p.advance()
			alias := p.current()
			if alias.kind == tokenWord && strings.Contains(alias.text, "*") {
				return nil, p.unsupportedStreamStatsSyntax(alias, "wildcard streamstats output fields are not supported")
			}
			if !isExactUnquotedStreamStatsField(alias) || strings.EqualFold(alias.text, "BY") {
				located := alias
				if command.Aggregate.Function == AggregateFunctionCountPredicate &&
					(alias.kind == tokenEOF || alias.kind == tokenPipe) {
					located = asToken
				}
				return nil, p.unsupportedStreamStatsSyntax(located, "streamstats AS requires one exact unquoted output field")
			}
			aliasSeen = true
			command.Aggregate.Alias = alias.text
			command.Aggregate.ExplicitAlias = true
			command.Aggregate.AliasRange = alias.sourceRange
			command.Aggregate.Range.End = alias.sourceRange.End
			end = alias.sourceRange.End
			p.advance()
			continue
		}

		if current.kind == tokenWord && strings.EqualFold(current.text, "BY") {
			if !aggregateSeen || bySeen {
				return nil, p.unsupportedStreamStatsSyntax(current, "streamstats accepts one BY clause after its aggregate")
			}
			bySeen = true
			p.advance()
			groups, groupEnd, err := p.parseStreamStatsGroupFields()
			if err != nil {
				return nil, err
			}
			command.GroupBy = groups
			end = groupEnd
			if !p.atCommandEnd() {
				byClosed = true
			}
			continue
		}

		if byClosed {
			return nil, p.unsupportedStreamStatsSyntax(current, "streamstats BY fields must precede any trailing options")
		}
		if current.kind == tokenComma {
			return nil, p.unsupportedStreamStatsAggregate(current, "streamstats supports exactly one aggregate")
		}
		if current.kind == tokenWord {
			return nil, p.unsupportedStreamStatsAggregate(
				current,
				fmt.Sprintf("streamstats aggregate %q is not supported; use %s", current.text, acceptedForms),
			)
		}
		return nil, p.unsupportedStreamStatsSyntax(current, "streamstats requires one "+acceptedForms+" aggregate")
	}

	if !aggregateSeen {
		return nil, p.errorAtCurrent("SPL_EXPECTED_AGGREGATE", "streamstats requires one "+acceptedForms+" aggregate")
	}
	if command.Aggregate.Function == AggregateFunctionCountPredicate &&
		!command.Aggregate.ExplicitAlias {
		return nil, &Diagnostic{
			Code: "SPL_UNSUPPORTED_STREAMSTATS_SYNTAX",
			Message: "streamstats count(eval(...)) requires AS followed by " +
				"an output field name",
			Range:       command.Aggregate.Range,
			Suggestions: []string{streamStatsCountPredicateSuggestion},
		}
	}
	if len(command.GroupBy) > 0 && command.Window > 0 &&
		(!command.GlobalSpecified || command.Global) {
		located := command.WindowRange
		if command.GlobalSpecified {
			located = command.GlobalRange
		}
		return nil, &Diagnostic{
			Code: "SPL_UNSUPPORTED_STREAMSTATS_SYNTAX",
			Message: "streamstats with BY and a positive window currently requires " +
				"explicit global=false so each group owns an independent row window",
			Range:       located,
			Suggestions: []string{"streamstats window=5 global=false count BY group"},
		}
	}
	command.Range = Range{Start: name.sourceRange.Start, End: end}
	return command, nil
}

// parseStreamStatsFieldAggregate consumes one supported exact long-form field
// aggregate. It deliberately does not share eventstats parsing because the two
// commands have different alias requirements and diagnostic namespaces.
func (p *parser) parseStreamStatsFieldAggregate(
	functionToken token,
	descriptor streamStatsAggregateDescriptor,
) (StatsAggregate, Position, error) {
	form := descriptor.name
	aggregate := StatsAggregate{
		Function: descriptor.function,
		Range:    functionToken.sourceRange,
	}
	end := functionToken.sourceRange.End
	p.advance()
	if !p.match(tokenLeftParen) {
		return StatsAggregate{}, end, p.unsupportedStreamStatsAggregate(
			functionToken,
			fmt.Sprintf("streamstats %s(field) requires one exact field in parentheses", form),
		)
	}

	input := p.current()
	if input.kind == tokenWord && strings.Contains(input.text, "*") {
		return StatsAggregate{}, end, p.unsupportedStreamStatsSyntax(
			input,
			fmt.Sprintf("wildcard streamstats %s fields are not supported", form),
		)
	}
	if !isExactUnquotedStreamStatsField(input) {
		return StatsAggregate{}, end, p.unsupportedStreamStatsSyntax(
			input,
			fmt.Sprintf("streamstats %s(field) requires one exact unquoted input field", form),
		)
	}
	aggregate.Input = input.text
	aggregate.InputRange = input.sourceRange
	p.advance()
	if !p.match(tokenRightParen) {
		return StatsAggregate{}, end, p.unsupportedStreamStatsAggregate(
			p.current(),
			fmt.Sprintf("streamstats %s(field) requires exactly one field and a closing ')'", form),
		)
	}
	end = p.previous().sourceRange.End
	aggregate.Range.End = end
	aggregate.Alias = form + "(" + input.text + ")"
	aggregate.AliasRange = Range{
		Start: functionToken.sourceRange.Start,
		End:   end,
	}
	return aggregate, end, nil
}

func (p *parser) parseStreamStatsGroupFields() ([]StatsGroupField, Position, error) {
	fields := make([]StatsGroupField, 0, 4)
	seen := make(map[string]struct{}, 4)
	end := p.current().sourceRange.Start
	wantField := true
	for !p.atCommandEnd() {
		if p.current().kind == tokenScalarComposite {
			if err := p.prepareSearchToken(); err != nil {
				return nil, end, err
			}
		}
		tok := p.current()
		followedByEqual := tok.kind == tokenWord && p.index+1 < len(p.tokens) &&
			p.tokens[p.index+1].kind == tokenEqual
		if followedByEqual && !wantField {
			break
		}
		if tok.kind == tokenComma {
			if wantField {
				return nil, end, p.errorAtCurrent("SPL_EXPECTED_FIELD", "streamstats BY requires an exact grouping field")
			}
			wantField = true
			p.advance()
			continue
		}
		if tok.kind == tokenQuotedField {
			return nil, end, p.unsupportedStreamStatsSyntax(tok, "quoted streamstats grouping fields are not supported")
		}
		if tok.kind != tokenWord || followedByEqual {
			return nil, end, p.errorAtCurrent("SPL_EXPECTED_FIELD", "streamstats BY requires an exact unquoted grouping field")
		}
		if strings.EqualFold(tok.text, "AS") || strings.EqualFold(tok.text, "BY") {
			return nil, end, p.unsupportedStreamStatsSyntax(tok, "streamstats accepts one BY clause and its alias must appear before BY")
		}
		if strings.Contains(tok.text, "*") {
			return nil, end, p.unsupportedStreamStatsSyntax(tok, "wildcard streamstats grouping fields are not supported")
		}
		if !isExactUnquotedStreamStatsField(tok) {
			return nil, end, p.unsupportedStreamStatsSyntax(tok, "quoted streamstats grouping fields are not supported")
		}
		if _, duplicate := seen[tok.text]; duplicate {
			return nil, end, p.unsupportedStreamStatsSyntax(tok, fmt.Sprintf("streamstats grouping field %q is repeated", tok.text))
		}
		if len(fields) >= MaximumStatsGroupFields {
			return nil, end, &Diagnostic{
				Code:    "SPL_QUERY_TOO_COMPLEX",
				Message: fmt.Sprintf("streamstats BY contains more than %d grouping fields", MaximumStatsGroupFields),
				Range:   tok.sourceRange,
			}
		}
		seen[tok.text] = struct{}{}
		fields = append(fields, StatsGroupField{Name: tok.text, Range: tok.sourceRange})
		end = tok.sourceRange.End
		wantField = false
		p.advance()
	}
	if len(fields) == 0 || wantField {
		return nil, end, p.errorAtCurrent("SPL_EXPECTED_FIELD", "streamstats BY requires at least one exact grouping field")
	}
	return fields, end, nil
}

func isExactUnquotedStreamStatsField(tok token) bool {
	return tok.kind == tokenWord && IsExactUnquotedFieldName(tok.text)
}

func parseStreamStatsBool(value string) (bool, bool) {
	switch strings.ToLower(value) {
	case "t", "true":
		return true, true
	case "f", "false":
		return false, true
	default:
		return false, false
	}
}

func parseStrictBool(value string) (bool, bool) {
	switch strings.ToLower(value) {
	case "true":
		return true, true
	case "false":
		return false, true
	default:
		return false, false
	}
}

func (p *parser) unsupportedStreamStatsAggregate(tok token, message string) *Diagnostic {
	return &Diagnostic{
		Code:        "SPL_UNSUPPORTED_STREAMSTATS_AGGREGATE",
		Message:     message,
		Range:       tok.sourceRange,
		Suggestions: []string{streamStatsSyntaxSuggestion},
	}
}

func (p *parser) unsupportedStreamStatsSyntax(tok token, message string) *Diagnostic {
	return &Diagnostic{
		Code:        "SPL_UNSUPPORTED_STREAMSTATS_SYNTAX",
		Message:     message,
		Range:       tok.sourceRange,
		Suggestions: []string{streamStatsSyntaxSuggestion},
	}
}
