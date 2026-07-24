package spl

import (
	"fmt"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/Suhaibinator/open-splunk/internal/splregex"
)

const (
	maxSPLSourceBytes     = 16 << 10
	maxSPLTokens          = 1024
	maxPipelineCommands   = 64
	maxEvalAssignments    = 64
	maxRenameAssignments  = 64
	maxStatsAggregates    = 16
	maxStatsGroupFields   = 16
	maxDedupFields        = 16
	maxWhereComparisons   = 32
	maxScalarNestingDepth = 32
)

// Parse parses the supported SPL compatibility tier. Unsupported commands and
// syntax are rejected; a valid prefix is never returned as a partial query.
func Parse(source string) (*Query, error) {
	if len(source) > maxSPLSourceBytes {
		start := sourcePositionAtOffset(source, maxSPLSourceBytes)
		end := sourcePositionAtOffset(source, maxSPLSourceBytes+1)
		return nil, &Diagnostic{
			Code:    "SPL_QUERY_TOO_COMPLEX",
			Message: fmt.Sprintf("search source exceeds %d UTF-8 bytes", maxSPLSourceBytes),
			Range:   Range{Start: start, End: end},
		}
	}
	tokens, err := lex(source)
	if err != nil {
		return nil, err
	}
	// Bound syntax before constructing recursive ASTs or nested SQL. The server
	// also caps source bytes, but a short token stream can still create deeply
	// nested expressions and quadratic compiler work.
	if len(tokens)-1 > maxSPLTokens { // exclude EOF
		return nil, &Diagnostic{
			Code:    "SPL_QUERY_TOO_COMPLEX",
			Message: fmt.Sprintf("search contains more than %d syntax tokens", maxSPLTokens),
			Range:   tokens[maxSPLTokens].range_,
		}
	}
	p := parser{tokens: tokens}
	return p.parseQuery()
}

func sourcePositionAtOffset(source string, offset int) Position {
	if offset > len(source) {
		offset = len(source)
	}
	position := Position{Line: 1, Column: 1}
	for position.Offset < offset {
		r, width := utf8.DecodeRuneInString(source[position.Offset:])
		if r == utf8.RuneError && width == 1 {
			width = 1
		}
		if position.Offset+width > offset {
			position.Offset = offset
			return position
		}
		position.Offset += width
		if r == '\n' {
			position.Line++
			position.Column = 1
		} else {
			position.Column++
		}
	}
	return position
}

type parser struct {
	tokens           []token
	index            int
	scalarDepth      int
	whereComparisons int
}

func (p *parser) parseQuery() (*Query, error) {
	start := p.current().range_.Start
	query := &Query{}

	if p.current().kind != tokenPipe && p.current().kind != tokenEOF {
		expression, err := p.parseSearchExpression()
		if err != nil {
			return nil, err
		}
		query.Search = expression
	}

	stage := 0
	for p.match(tokenPipe) {
		stage++
		if stage > maxPipelineCommands {
			return nil, &Diagnostic{
				Code:    "SPL_QUERY_TOO_COMPLEX",
				Message: fmt.Sprintf("search contains more than %d pipeline commands", maxPipelineCommands),
				Range:   p.current().range_,
			}
		}
		command, err := p.parseCommand(stage)
		if err != nil {
			return nil, err
		}
		query.Commands = append(query.Commands, command)
	}
	if p.current().kind != tokenEOF {
		return nil, p.errorAtCurrent("SPL_UNEXPECTED_TOKEN", fmt.Sprintf("unexpected token %q", p.current().text))
	}
	if query.Search == nil && len(query.Commands) == 0 {
		return nil, p.errorAtCurrent("SPL_EMPTY_QUERY", "search query is empty")
	}
	query.Range = Range{Start: start, End: p.current().range_.End}
	return query, nil
}

func (p *parser) parseCommand(stage int) (Command, error) {
	nameToken := p.current()
	if nameToken.kind == tokenEOF || nameToken.kind == tokenPipe {
		return nil, p.errorAtCurrent("SPL_EXPECTED_COMMAND", "expected a command after '|'")
	}
	if nameToken.kind != tokenWord {
		return nil, p.errorAtCurrent("SPL_EXPECTED_COMMAND", "expected a command name after '|'")
	}
	p.advance()
	name := strings.ToLower(nameToken.text)
	switch name {
	case "search":
		return p.parseSearchCommand(nameToken)
	case "where":
		return p.parseWhereCommand(nameToken)
	case "eval":
		return p.parseEvalCommand(nameToken)
	case "rex":
		return p.parseRexCommand(nameToken)
	case "rename":
		return p.parseRenameCommand(nameToken)
	case "fields":
		return p.parseFieldsCommand(nameToken)
	case "table":
		return p.parseTableCommand(nameToken)
	case "sort":
		return p.parseSortCommand(nameToken)
	case "dedup":
		return p.parseDedupCommand(nameToken)
	case "head", "tail":
		return p.parseLimitCommand(name, nameToken)
	case "stats":
		return p.parseStatsCommand(nameToken)
	case "top":
		return p.parseTopCommand(nameToken)
	case "rare":
		return p.parseRareCommand(nameToken)
	case "bin", "bucket":
		return p.parseBinCommand(nameToken)
	case "timechart":
		return p.parseTimechartCommand(nameToken)
	default:
		return nil, &Diagnostic{
			Code:    "SPL_UNSUPPORTED_COMMAND",
			Message: fmt.Sprintf("unsupported command %q at pipeline stage %d", nameToken.text, stage),
			Range:   nameToken.range_,
		}
	}
}

func (p *parser) parseRexCommand(name token) (Command, error) {
	command := &RexCommand{
		Field:      "_raw",
		FieldRange: name.range_,
		MaxMatch:   1,
	}
	if p.atCommandEnd() {
		return nil, p.errorAtCurrent("SPL_EXPECTED_REX_PATTERN", "rex requires a quoted extraction regular expression")
	}

	fieldSeen := false
	maxMatchSeen := false
	parseField := func() error {
		option := p.current()
		if fieldSeen {
			return &Diagnostic{
				Code:    "SPL_UNSUPPORTED_REX_SYNTAX",
				Message: "rex field may be specified only once before the pattern",
				Range:   option.range_,
			}
		}
		fieldSeen = true
		p.advance()
		if !p.match(tokenEqual) {
			return &Diagnostic{
				Code:        "SPL_UNSUPPORTED_REX_SYNTAX",
				Message:     "rex field must be written as field=<field>",
				Range:       option.range_,
				Suggestions: []string{`rex field=message "(?<value>pattern)"`},
			}
		}
		field := p.current()
		if field.kind != tokenWord {
			return p.errorAtCurrent("SPL_EXPECTED_FIELD", "rex field= requires an exact unquoted field")
		}
		command.Field = field.text
		command.FieldRange = field.range_
		p.advance()
		return nil
	}
	parseMaxMatch := func() (Position, error) {
		option := p.current()
		if maxMatchSeen {
			return Position{}, &Diagnostic{
				Code:    "SPL_UNSUPPORTED_REX_SYNTAX",
				Message: "rex max_match may be specified only once",
				Range:   option.range_,
			}
		}
		maxMatchSeen = true
		p.advance()
		if !p.match(tokenEqual) || p.current().kind != tokenWord {
			return Position{}, &Diagnostic{
				Code:    "SPL_UNSUPPORTED_REX_SYNTAX",
				Message: "rex max_match must be written as max_match=1",
				Range:   option.range_,
			}
		}
		value := p.current()
		maxMatch, err := strconv.ParseUint(value.text, 10, 64)
		if err != nil || maxMatch != 1 {
			return Position{}, &Diagnostic{
				Code:        "SPL_UNSUPPORTED_REX_SYNTAX",
				Message:     "rex currently supports only the first match (max_match=1)",
				Range:       value.range_,
				Suggestions: []string{"omit max_match or use max_match=1"},
			}
		}
		command.MaxMatch = maxMatch
		p.advance()
		return value.range_.End, nil
	}
	unsupportedOption := func(message string) error {
		return &Diagnostic{
			Code:        "SPL_UNSUPPORTED_REX_SYNTAX",
			Message:     message,
			Range:       p.current().range_,
			Suggestions: []string{`rex field=message max_match=1 "(?<value>pattern)"`},
		}
	}

options:
	for !p.atCommandEnd() && p.current().kind != tokenString {
		switch {
		case p.isKeyword("field"):
			if err := parseField(); err != nil {
				return nil, err
			}
		case p.isKeyword("max_match"):
			if _, err := parseMaxMatch(); err != nil {
				return nil, err
			}
		case p.isKeyword("mode"):
			return nil, unsupportedOption("rex sed mode is not supported in compatibility version 0.1")
		case p.isKeyword("offset_field"):
			return nil, unsupportedOption("rex offset_field is not supported in compatibility version 0.1")
		default:
			break options
		}
	}

	pattern := p.current()
	if pattern.kind != tokenString || !pattern.quoted {
		return nil, p.errorAtCurrent("SPL_EXPECTED_REX_PATTERN", "rex requires a quoted extraction regular expression")
	}
	command.Pattern = pattern.text
	command.PatternRange = pattern.range_
	end := pattern.range_.End
	p.advance()

	if !p.atCommandEnd() {
		if !p.isKeyword("max_match") {
			return nil, &Diagnostic{
				Code:        "SPL_UNSUPPORTED_REX_SYNTAX",
				Message:     fmt.Sprintf("unsupported rex option or syntax at %q", p.current().text),
				Range:       p.current().range_,
				Suggestions: []string{`rex field=message max_match=1 "(?<value>pattern)"`},
			}
		}
		var err error
		end, err = parseMaxMatch()
		if err != nil {
			return nil, err
		}
		if !p.atCommandEnd() {
			return nil, &Diagnostic{
				Code:    "SPL_UNSUPPORTED_REX_SYNTAX",
				Message: fmt.Sprintf("unsupported rex option or syntax at %q", p.current().text),
				Range:   p.current().range_,
			}
		}
	}

	_, err := splregex.CompileExtractionPattern(command.Pattern)
	if err != nil {
		code := "SPL_UNSUPPORTED_REGEX"
		message := "rex regular expression is outside the supported named-capture RE2-compatible subset"
		if splregex.IsExtractionComplexityError(err) {
			code = "SPL_QUERY_TOO_COMPLEX"
			message = "rex regular expression exceeds the supported pattern or capture-group limit"
		}
		return nil, &Diagnostic{
			Code:        code,
			Message:     message,
			Range:       command.PatternRange,
			Suggestions: []string{`use a bounded RE2 pattern with one or more unique named captures such as "(?<value>...)"`},
		}
	}
	command.Range = Range{Start: name.range_.Start, End: end}
	return command, nil
}

func (p *parser) parseRenameCommand(name token) (Command, error) {
	command := &RenameCommand{}
	seenSources := make(map[string]struct{})
	seenDestinations := make(map[string]struct{})
	end := name.range_.End

	if p.atCommandEnd() {
		return nil, p.errorAtCurrent("SPL_EXPECTED_FIELD", "rename requires a source field")
	}
	for {
		source := p.current()
		if source.kind != tokenWord {
			return nil, p.errorAtCurrent("SPL_EXPECTED_FIELD", "rename requires an exact source field")
		}
		if strings.Contains(source.text, "*") {
			return nil, &Diagnostic{
				Code:        "SPL_UNSUPPORTED_RENAME_PATTERN",
				Message:     "wildcard rename patterns are not supported in compatibility version 0.1",
				Range:       source.range_,
				Suggestions: []string{"rename old_field AS new_field"},
			}
		}
		if _, duplicate := seenSources[source.text]; duplicate {
			return nil, &Diagnostic{
				Code:    "SPL_DUPLICATE_RENAME_SOURCE",
				Message: fmt.Sprintf("rename source field %q is repeated", source.text),
				Range:   source.range_,
			}
		}
		p.advance()
		if !p.isKeyword("AS") {
			return nil, &Diagnostic{
				Code:        "SPL_EXPECTED_AS",
				Message:     "rename source field must be followed by AS",
				Range:       p.current().range_,
				Suggestions: []string{"rename old_field AS new_field"},
			}
		}
		p.advance()

		destination := p.current()
		if destination.kind != tokenWord {
			return nil, p.errorAtCurrent("SPL_EXPECTED_FIELD", "rename AS requires an exact destination field")
		}
		if strings.Contains(destination.text, "*") {
			return nil, &Diagnostic{
				Code:        "SPL_UNSUPPORTED_RENAME_PATTERN",
				Message:     "wildcard rename patterns are not supported in compatibility version 0.1",
				Range:       destination.range_,
				Suggestions: []string{"rename old_field AS new_field"},
			}
		}
		if source.text == destination.text {
			return nil, &Diagnostic{
				Code:    "SPL_INVALID_RENAME",
				Message: fmt.Sprintf("rename source and destination are both %q", source.text),
				Range:   Range{Start: source.range_.Start, End: destination.range_.End},
			}
		}
		if _, duplicate := seenDestinations[destination.text]; duplicate {
			return nil, &Diagnostic{
				Code:    "SPL_DUPLICATE_RENAME_TARGET",
				Message: fmt.Sprintf("rename destination field %q is repeated", destination.text),
				Range:   destination.range_,
			}
		}
		seenSources[source.text] = struct{}{}
		seenDestinations[destination.text] = struct{}{}
		command.Assignments = append(command.Assignments, RenameAssignment{
			Source:           source.text,
			SourceRange:      source.range_,
			Destination:      destination.text,
			DestinationRange: destination.range_,
			Range:            Range{Start: source.range_.Start, End: destination.range_.End},
		})
		if len(command.Assignments) > maxRenameAssignments {
			return nil, &Diagnostic{
				Code:    "SPL_QUERY_TOO_COMPLEX",
				Message: fmt.Sprintf("rename contains more than %d assignments", maxRenameAssignments),
				Range:   destination.range_,
			}
		}
		end = destination.range_.End
		p.advance()
		if p.atCommandEnd() {
			break
		}
		if !p.match(tokenComma) {
			return nil, &Diagnostic{
				Code:        "SPL_EXPECTED_COMMA",
				Message:     "rename pairs must be separated by a comma",
				Range:       p.current().range_,
				Suggestions: []string{"rename first AS one, second AS two"},
			}
		}
		if p.atCommandEnd() {
			return nil, p.errorAtCurrent("SPL_EXPECTED_FIELD", "expected another rename source field after comma")
		}
	}
	command.Range = Range{Start: name.range_.Start, End: end}
	return command, nil
}

func (p *parser) parseBinCommand(name token) (Command, error) {
	commandName := strings.ToLower(name.text)
	var (
		fieldSeen bool
		field     token
		spanSeen  bool
		span      TimeSpan
		spanValue token
		end       = name.range_.End
	)

	for !p.atCommandEnd() {
		current := p.current()
		if current.kind == tokenWord && strings.EqualFold(current.text, "span") {
			if spanSeen {
				return nil, p.unsupportedBinSyntax(current, "bin span may be specified only once")
			}
			option := current
			p.advance()
			if !p.match(tokenEqual) {
				return nil, &Diagnostic{
					Code:        "SPL_EXPECTED_EQUAL",
					Message:     "bin span must be followed by '='",
					Range:       option.range_,
					Suggestions: []string{"bin _time span=5m"},
				}
			}
			value := p.current()
			if value.kind != tokenWord {
				if value.kind == tokenEOF || value.kind == tokenPipe {
					value = option
				}
				return nil, invalidFixedTimeSpan(value, binTimeSpanConfig)
			}
			parsed, err := parseFixedTimeSpan(value, binTimeSpanConfig)
			if err != nil {
				return nil, err
			}
			spanSeen = true
			span = parsed
			spanValue = value
			end = value.range_.End
			p.advance()
			continue
		}

		if current.kind == tokenWord {
			switch strings.ToLower(current.text) {
			case "bins", "minspan", "start", "end", "aligntime":
				return nil, p.unsupportedBinSyntax(
					current,
					fmt.Sprintf("bin option %q is not supported; use one explicit fixed span", current.text),
				)
			case "as":
				return nil, p.unsupportedBinSyntax(current, "bin AS output fields are not supported")
			}
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
			return nil, p.unsupportedBinSyntax(current, "bin requires the exact unquoted _time field")
		}
		if fieldSeen {
			return nil, p.unsupportedBinSyntax(current, "bin supports exactly one field")
		}
		if current.text != "_time" {
			if strings.Contains(current.text, "*") {
				return nil, p.unsupportedBinSyntax(current, "wildcard bin fields are not supported")
			}
			return nil, p.unsupportedBinSyntax(current, "bin currently supports only the exact _time field")
		}
		fieldSeen = true
		field = current
		end = current.range_.End
		p.advance()
	}

	if !fieldSeen {
		located := name
		if spanSeen {
			located = spanValue
		}
		return nil, p.unsupportedBinSyntax(located, "bin requires exactly one unquoted _time field")
	}
	if !spanSeen {
		return nil, p.unsupportedBinSyntax(field, "bin requires one explicit span=<positive integer><s|m|h>")
	}
	return &BinCommand{
		CommandName: commandName,
		Field:       field.text,
		FieldRange:  field.range_,
		Span:        span,
		Range:       Range{Start: name.range_.Start, End: end},
	}, nil
}

func (p *parser) unsupportedBinSyntax(tok token, message string) *Diagnostic {
	return &Diagnostic{
		Code:        "SPL_UNSUPPORTED_BIN_SYNTAX",
		Message:     message,
		Range:       tok.range_,
		Suggestions: []string{"bin _time span=5m"},
	}
}

func (p *parser) parseTimechartCommand(name token) (Command, error) {
	if !p.isKeyword("SPAN") {
		return nil, p.unsupportedTimechartSyntax(p.current(), "timechart requires span=<positive integer><s|m|h> before count")
	}
	spanOption := p.current()
	p.advance()
	if !p.match(tokenEqual) {
		return nil, &Diagnostic{
			Code:        "SPL_EXPECTED_EQUAL",
			Message:     "timechart span must be followed by '='",
			Range:       spanOption.range_,
			Suggestions: []string{"timechart span=5m count by field"},
		}
	}
	spanToken := p.current()
	if spanToken.kind != tokenWord {
		return nil, &Diagnostic{
			Code:        "SPL_INVALID_ARGUMENT",
			Message:     "timechart span must be a positive integer followed by s, m, or h",
			Range:       spanToken.range_,
			Suggestions: []string{"timechart span=5m count by field"},
		}
	}
	span, err := parseTimechartSpan(spanToken)
	if err != nil {
		return nil, err
	}
	p.advance()

	aggregate := p.current()
	if aggregate.kind != tokenWord || !strings.EqualFold(aggregate.text, "count") {
		return nil, &Diagnostic{
			Code:        "SPL_UNSUPPORTED_TIMECHART_AGGREGATE",
			Message:     "only argument-free count is supported by timechart",
			Range:       aggregate.range_,
			Suggestions: []string{"timechart span=5m count by field"},
		}
	}
	p.advance()
	if !p.isKeyword("BY") {
		return nil, p.unsupportedTimechartSyntax(p.current(), "timechart count requires BY followed by one split field")
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
	if !p.atCommandEnd() {
		return nil, p.unsupportedTimechartSyntax(p.current(), "only one timechart split field is currently supported")
	}
	return &TimechartCommand{
		Span:           span,
		Function:       AggregateFunctionCount,
		AggregateRange: aggregate.range_,
		SplitBy:        StatsGroupField{Name: field.text, Range: field.range_},
		Range:          Range{Start: name.range_.Start, End: field.range_.End},
	}, nil
}

func parseTimechartSpan(tok token) (TimeSpan, error) {
	return parseFixedTimeSpan(tok, timechartTimeSpanConfig)
}

type fixedTimeSpanParserConfig struct {
	commandName        string
	syntaxCode         string
	suggestion         string
	logSpanUnsupported bool
}

var (
	binTimeSpanConfig = fixedTimeSpanParserConfig{
		commandName:        "bin",
		syntaxCode:         "SPL_UNSUPPORTED_BIN_SYNTAX",
		suggestion:         "bin _time span=5m",
		logSpanUnsupported: true,
	}
	timechartTimeSpanConfig = fixedTimeSpanParserConfig{
		commandName: "timechart",
		syntaxCode:  "SPL_UNSUPPORTED_TIMECHART_SYNTAX",
		suggestion:  "timechart span=5m count by field",
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
	default:
		return TimeSpan{}, unsupportedFixedTimeSpanUnit(tok, config)
	}
	magnitude, err := strconv.ParseUint(tok.text[:digitEnd], 10, 64)
	if err != nil {
		return TimeSpan{}, &Diagnostic{
			Code:    "SPL_NUMBER_OUT_OF_RANGE",
			Message: config.commandName + " span is outside the supported 64-bit range",
			Range:   tok.range_,
		}
	}
	if magnitude == 0 {
		return TimeSpan{}, invalidFixedTimeSpan(tok, config)
	}
	const maxDurationNanoseconds = uint64(1<<63 - 1)
	if magnitude > maxDurationNanoseconds/unitNanoseconds {
		return TimeSpan{}, &Diagnostic{
			Code:    "SPL_NUMBER_OUT_OF_RANGE",
			Message: config.commandName + " span is outside the supported duration range",
			Range:   tok.range_,
		}
	}
	return TimeSpan{Magnitude: magnitude, Unit: unit, Range: tok.range_}, nil
}

func invalidFixedTimeSpan(tok token, config fixedTimeSpanParserConfig) *Diagnostic {
	return &Diagnostic{
		Code:        "SPL_INVALID_ARGUMENT",
		Message:     config.commandName + " span must be a positive integer followed by s, m, or h",
		Range:       tok.range_,
		Suggestions: []string{config.suggestion},
	}
}

func unsupportedFixedTimeSpanUnit(tok token, config fixedTimeSpanParserConfig) *Diagnostic {
	return &Diagnostic{
		Code:        config.syntaxCode,
		Message:     fmt.Sprintf("%s span unit in %q is unsupported; use fixed seconds, minutes, or hours", config.commandName, tok.text),
		Range:       tok.range_,
		Suggestions: []string{config.suggestion},
	}
}

func (p *parser) unsupportedTimechartSyntax(tok token, message string) *Diagnostic {
	return &Diagnostic{
		Code:        "SPL_UNSUPPORTED_TIMECHART_SYNTAX",
		Message:     message,
		Range:       tok.range_,
		Suggestions: []string{"timechart span=5m count by field"},
	}
}

type parsedFrequencyCommand struct {
	field      string
	fieldRange Range
	limit      uint64
	range_     Range
}

func (p *parser) parseTopCommand(name token) (Command, error) {
	parsed, err := p.parseFrequencyCommand(name, "top")
	if err != nil {
		return nil, err
	}
	return &TopCommand{
		Field:      parsed.field,
		FieldRange: parsed.fieldRange,
		Limit:      parsed.limit,
		Range:      parsed.range_,
	}, nil
}

func (p *parser) parseRareCommand(name token) (Command, error) {
	parsed, err := p.parseFrequencyCommand(name, "rare")
	if err != nil {
		return nil, err
	}
	return &RareCommand{
		Field:      parsed.field,
		FieldRange: parsed.fieldRange,
		Limit:      parsed.limit,
		Range:      parsed.range_,
	}, nil
}

func (p *parser) parseFrequencyCommand(name token, commandName string) (parsedFrequencyCommand, error) {
	command := parsedFrequencyCommand{limit: 10}
	if p.atCommandEnd() {
		return parsedFrequencyCommand{}, p.errorAtCurrent("SPL_EXPECTED_FIELD", commandName+" requires one field")
	}

	var limitToken token
	hasLimit := false
	if p.current().kind == tokenWord && unsignedIntegerSyntax(p.current().text) {
		limitToken = p.current()
		hasLimit = true
		p.advance()
	} else if p.current().kind == tokenWord && strings.HasPrefix(p.current().text, "-") && integerSyntax(p.current().text) {
		return parsedFrequencyCommand{}, p.errorAtCurrent("SPL_INVALID_ARGUMENT", commandName+" limit must be a non-negative integer")
	} else if p.isKeyword("LIMIT") && p.index+1 < len(p.tokens) && p.tokens[p.index+1].kind == tokenEqual {
		p.advance()
		p.advance()
		limitToken = p.current()
		if limitToken.kind != tokenWord || !unsignedIntegerSyntax(limitToken.text) {
			return parsedFrequencyCommand{}, p.errorAtCurrent("SPL_INVALID_ARGUMENT", commandName+" limit must be a non-negative integer")
		}
		hasLimit = true
		p.advance()
	} else if p.current().kind == tokenWord && p.index+1 < len(p.tokens) && p.tokens[p.index+1].kind == tokenEqual {
		return parsedFrequencyCommand{}, p.unsupportedFrequencySyntax(
			p.current(), commandName, fmt.Sprintf("%s option %q is not supported", commandName, p.current().text),
		)
	}
	if hasLimit {
		limit, err := strconv.ParseUint(limitToken.text, 10, 64)
		if err != nil {
			return parsedFrequencyCommand{}, &Diagnostic{
				Code:    "SPL_NUMBER_OUT_OF_RANGE",
				Message: commandName + " result count is outside the supported 64-bit range",
				Range:   limitToken.range_,
			}
		}
		command.limit = limit
	}

	if p.atCommandEnd() {
		return parsedFrequencyCommand{}, p.errorAtCurrent("SPL_EXPECTED_FIELD", commandName+" requires one field")
	}
	if p.current().kind == tokenWord && p.index+1 < len(p.tokens) && p.tokens[p.index+1].kind == tokenEqual {
		return parsedFrequencyCommand{}, p.unsupportedFrequencySyntax(
			p.current(), commandName, fmt.Sprintf("%s option %q is not supported", commandName, p.current().text),
		)
	}
	field := p.current()
	if field.kind != tokenWord {
		return parsedFrequencyCommand{}, p.errorAtCurrent("SPL_EXPECTED_FIELD", commandName+" requires one field")
	}
	if strings.Contains(field.text, "*") {
		return parsedFrequencyCommand{}, p.unsupportedFrequencySyntax(field, commandName, "wildcard "+commandName+" fields are not supported")
	}
	command.field = field.text
	command.fieldRange = field.range_
	command.range_ = Range{Start: name.range_.Start, End: field.range_.End}
	p.advance()
	if !p.atCommandEnd() {
		return parsedFrequencyCommand{}, p.unsupportedFrequencySyntax(p.current(), commandName, "only one "+commandName+" field is currently supported")
	}
	return command, nil
}

func (p *parser) unsupportedFrequencySyntax(tok token, commandName, message string) *Diagnostic {
	return &Diagnostic{
		Code:        "SPL_UNSUPPORTED_" + strings.ToUpper(commandName) + "_SYNTAX",
		Message:     message,
		Range:       tok.range_,
		Suggestions: []string{commandName + " field", commandName + " limit=20 field"},
	}
}

func (p *parser) parseStatsCommand(name token) (Command, error) {
	if p.atCommandEnd() {
		return nil, p.errorAtCurrent("SPL_EXPECTED_AGGREGATE", "stats requires an aggregate function")
	}

	aggregates := make([]StatsAggregate, 0, 4)
	end := name.range_.End
	for {
		if len(aggregates) >= maxStatsAggregates {
			return nil, &Diagnostic{
				Code:    "SPL_QUERY_TOO_COMPLEX",
				Message: fmt.Sprintf("stats contains more than %d aggregate measures", maxStatsAggregates),
				Range:   p.current().range_,
			}
		}
		aggregate, aggregateEnd, err := p.parseStatsAggregate()
		if err != nil {
			return nil, err
		}
		aggregates = append(aggregates, aggregate)
		end = aggregateEnd
		if p.isKeyword("BY") || p.atCommandEnd() {
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
			Range:       current.range_,
			Suggestions: []string{"stats count", "stats sum(field) avg(field) BY group", "stats p95(field) AS p95_value BY group"},
		}
	}

	var groupBy []StatsGroupField
	if p.isKeyword("BY") {
		p.advance()
		var err error
		groupBy, end, err = p.parseStatsGroupFields()
		if err != nil {
			return nil, err
		}
	}
	return &StatsCommand{
		Aggregates: aggregates,
		GroupBy:    groupBy,
		Range:      Range{Start: name.range_.Start, End: end},
	}, nil
}

func (p *parser) parseStatsAggregate() (StatsAggregate, Position, error) {
	functionToken := p.current()
	if functionToken.kind != tokenWord {
		return StatsAggregate{}, functionToken.range_.End, p.errorAtCurrent("SPL_EXPECTED_AGGREGATE", "stats requires an aggregate function")
	}
	p.advance()
	aggregate := StatsAggregate{Range: functionToken.range_, AliasRange: functionToken.range_}
	end := functionToken.range_.End
	switch strings.ToLower(functionToken.text) {
	case "count":
		aggregate.Function = AggregateFunctionCount
		aggregate.Alias = "count"
		if p.current().kind == tokenLeftParen {
			return StatsAggregate{}, end, p.unsupportedStatsAggregate(p.current(), "count arguments are not supported; use argument-free count")
		}
	case "p95", "sum", "avg":
		functionName := strings.ToLower(functionToken.text)
		switch functionName {
		case "p95":
			aggregate.Function = AggregateFunctionP95
		case "sum":
			aggregate.Function = AggregateFunctionSum
		case "avg":
			aggregate.Function = AggregateFunctionAverage
		}
		if !p.match(tokenLeftParen) {
			return StatsAggregate{}, end, &Diagnostic{
				Code:        "SPL_UNSUPPORTED_STATS_SYNTAX",
				Message:     functionName + " requires one field argument in parentheses",
				Range:       functionToken.range_,
				Suggestions: []string{functionName + "(field)"},
			}
		}
		input := p.current()
		if input.kind != tokenWord {
			return StatsAggregate{}, end, p.errorAtCurrent("SPL_EXPECTED_FIELD", functionName+" requires one input field")
		}
		aggregate.Input = input.text
		aggregate.InputRange = input.range_
		p.advance()
		if !p.match(tokenRightParen) {
			return StatsAggregate{}, end, p.errorAtCurrent("SPL_EXPECTED_RIGHT_PAREN", "expected ')' after the "+functionName+" input field")
		}
		end = p.previous().range_.End
		aggregate.Range.End = end
		aggregate.Alias = functionName + "(" + input.text + ")"
		aggregate.AliasRange = Range{Start: functionToken.range_.Start, End: end}
	default:
		return StatsAggregate{}, end, p.unsupportedStatsAggregate(
			functionToken,
			fmt.Sprintf("stats aggregate %q is not supported; count, p95, sum, and avg are available", functionToken.text),
		)
	}

	if p.isKeyword("AS") {
		p.advance()
		alias := p.current()
		if alias.kind != tokenWord || p.isKeyword("BY") {
			return StatsAggregate{}, end, p.errorAtCurrent("SPL_EXPECTED_FIELD", "expected an output field name after AS")
		}
		aggregate.Alias = alias.text
		aggregate.AliasRange = alias.range_
		aggregate.Range.End = alias.range_.End
		end = alias.range_.End
		p.advance()
	}
	return aggregate, end, nil
}

func supportedStatsAggregateName(name string) bool {
	return strings.EqualFold(name, "count") || strings.EqualFold(name, "p95") ||
		strings.EqualFold(name, "sum") || strings.EqualFold(name, "avg")
}

func (p *parser) parseStatsGroupFields() ([]StatsGroupField, Position, error) {
	fields := make([]StatsGroupField, 0, 4)
	end := p.current().range_.Start
	wantField := true
	for !p.atCommandEnd() {
		tok := p.current()
		if tok.kind == tokenComma {
			if wantField {
				return nil, end, p.errorAtCurrent("SPL_EXPECTED_FIELD", "expected a stats grouping field")
			}
			wantField = true
			p.advance()
			continue
		}
		if tok.kind != tokenWord {
			return nil, end, p.errorAtCurrent("SPL_EXPECTED_FIELD", "expected a stats grouping field")
		}
		if strings.EqualFold(tok.text, "AS") {
			return nil, end, &Diagnostic{
				Code:        "SPL_UNSUPPORTED_STATS_SYNTAX",
				Message:     "a stats aggregate alias must appear before the BY clause",
				Range:       tok.range_,
				Suggestions: []string{"stats count AS total BY field"},
			}
		}
		if len(fields) >= maxStatsGroupFields {
			return nil, end, &Diagnostic{
				Code:    "SPL_QUERY_TOO_COMPLEX",
				Message: fmt.Sprintf("stats BY contains more than %d grouping fields", maxStatsGroupFields),
				Range:   tok.range_,
			}
		}
		fields = append(fields, StatsGroupField{Name: tok.text, Range: tok.range_})
		end = tok.range_.End
		wantField = false
		p.advance()
	}
	if len(fields) == 0 || wantField {
		return nil, end, p.errorAtCurrent("SPL_EXPECTED_FIELD", "stats BY requires at least one field")
	}
	return fields, end, nil
}

func (p *parser) unsupportedStatsAggregate(tok token, message string) *Diagnostic {
	return &Diagnostic{
		Code:        "SPL_UNSUPPORTED_STATS_AGGREGATE",
		Message:     message,
		Range:       tok.range_,
		Suggestions: []string{"stats count", "stats sum(field) avg(field) BY group", "stats p95(field) AS p95_value BY group"},
	}
}

func (p *parser) parseSearchCommand(name token) (Command, error) {
	if p.atCommandEnd() {
		return nil, p.errorAtCurrent("SPL_EXPECTED_EXPRESSION", "search requires an expression")
	}
	expression, err := p.parseSearchExpression()
	if err != nil {
		return nil, err
	}
	return &SearchCommand{Expression: expression, Range: Range{Start: name.range_.Start, End: expression.SourceRange().End}}, nil
}

func (p *parser) parseWhereCommand(name token) (Command, error) {
	if p.atCommandEnd() {
		return nil, p.errorAtCurrent("SPL_EXPECTED_EXPRESSION", "where requires a boolean expression")
	}
	expression, err := p.parseWhereExpression()
	if err != nil {
		return nil, err
	}
	if !p.atCommandEnd() {
		return nil, &Diagnostic{
			Code:        "SPL_UNSUPPORTED_WHERE_EXPRESSION",
			Message:     fmt.Sprintf("unsupported where syntax at %q; explicit AND or OR is required between comparisons", p.current().text),
			Range:       p.current().range_,
			Suggestions: []string{"where field=value AND other_field>0"},
		}
	}
	return &WhereCommand{
		Expression: expression,
		Range:      Range{Start: name.range_.Start, End: expression.SourceRange().End},
	}, nil
}

func (p *parser) parseEvalCommand(name token) (Command, error) {
	command := &EvalCommand{}
	end := name.range_.End
	for {
		if len(command.Assignments) >= maxEvalAssignments {
			return nil, &Diagnostic{
				Code:    "SPL_QUERY_TOO_COMPLEX",
				Message: fmt.Sprintf("eval contains more than %d assignments", maxEvalAssignments),
				Range:   p.current().range_,
			}
		}
		field := p.current()
		if field.kind != tokenWord {
			return nil, p.errorAtCurrent("SPL_EXPECTED_FIELD", "eval requires a destination field")
		}
		if classifyLiteral(field.text, false) != LiteralKindString || unsupportedScalarIdentifier(field.text) {
			return nil, &Diagnostic{
				Code:        "SPL_UNSUPPORTED_EVAL_EXPRESSION",
				Message:     fmt.Sprintf("unsupported eval destination %q", field.text),
				Range:       field.range_,
				Suggestions: []string{"use an unquoted field name without arithmetic operators"},
			}
		}
		p.advance()
		if !p.match(tokenEqual) {
			return nil, &Diagnostic{
				Code:        "SPL_EXPECTED_EQUAL",
				Message:     fmt.Sprintf("eval destination field %q must be followed by '='", field.text),
				Range:       field.range_,
				Suggestions: []string{"eval field=expression"},
			}
		}
		expression, err := p.parseScalarExpression()
		if err != nil {
			return nil, err
		}
		assignment := EvalAssignment{
			Field:      field.text,
			FieldRange: field.range_,
			Expression: expression,
			Range:      Range{Start: field.range_.Start, End: expression.SourceRange().End},
		}
		command.Assignments = append(command.Assignments, assignment)
		end = expression.SourceRange().End
		if !p.match(tokenComma) {
			break
		}
		if p.atCommandEnd() {
			return nil, p.errorAtCurrent("SPL_EXPECTED_FIELD", "expected another eval destination field after comma")
		}
	}
	if !p.atCommandEnd() {
		return nil, &Diagnostic{
			Code:        "SPL_UNSUPPORTED_EVAL_EXPRESSION",
			Message:     fmt.Sprintf("unsupported eval syntax at %q", p.current().text),
			Range:       p.current().range_,
			Suggestions: []string{"eval field=expression"},
		}
	}
	command.Range = Range{Start: name.range_.Start, End: end}
	return command, nil
}

func (p *parser) parseFieldsCommand(name token) (Command, error) {
	exclude := false
	if p.current().kind == tokenWord && p.current().text == "-" {
		exclude = true
		p.advance()
	}
	fields, end, err := p.parseFieldList()
	if err != nil {
		return nil, err
	}
	return &FieldsCommand{Fields: fields, Exclude: exclude, Range: Range{Start: name.range_.Start, End: end}}, nil
}

func (p *parser) parseTableCommand(name token) (Command, error) {
	fields, end, err := p.parseFieldList()
	if err != nil {
		return nil, err
	}
	return &TableCommand{Fields: fields, Range: Range{Start: name.range_.Start, End: end}}, nil
}

func (p *parser) parseFieldList() ([]string, Position, error) {
	fields := make([]string, 0, 8)
	end := p.current().range_.Start
	wantField := true
	for !p.atCommandEnd() {
		tok := p.current()
		if tok.kind == tokenComma {
			if wantField {
				return nil, end, p.errorAtCurrent("SPL_EXPECTED_FIELD", "expected a field name")
			}
			wantField = true
			p.advance()
			continue
		}
		if tok.kind != tokenWord {
			return nil, end, p.errorAtCurrent("SPL_EXPECTED_FIELD", "expected a field name")
		}
		fields = append(fields, tok.text)
		end = tok.range_.End
		wantField = false
		p.advance()
	}
	if len(fields) == 0 || wantField {
		return nil, end, p.errorAtCurrent("SPL_EXPECTED_FIELD", "expected at least one field name")
	}
	return fields, end, nil
}

func (p *parser) parseSortCommand(name token) (Command, error) {
	command := &SortCommand{}
	if p.atCommandEnd() {
		return nil, p.errorAtCurrent("SPL_EXPECTED_FIELD", "sort requires at least one field")
	}
	if p.current().kind == tokenWord {
		if unsignedIntegerSyntax(p.current().text) {
			limit, err := strconv.ParseUint(p.current().text, 10, 64)
			if err != nil {
				return nil, p.errorAtCurrent("SPL_NUMBER_OUT_OF_RANGE", "sort result count is outside the supported 64-bit range")
			}
			command.Limit = limit
			command.LimitSpecified = true
			p.advance()
		}
	}
	if p.current().kind == tokenWord && (strings.EqualFold(p.current().text, "asc") || strings.EqualFold(p.current().text, "desc")) {
		return nil, p.errorAtCurrent("SPL_UNSUPPORTED_SORT_SYNTAX", "use a + or - prefix on each sort field")
	}

	end := name.range_.End
	lastWasComma := false
	for !p.atCommandEnd() {
		if p.match(tokenComma) {
			if len(command.Fields) == 0 || lastWasComma {
				return nil, p.errorAtCurrent("SPL_EXPECTED_FIELD", "expected a sort field before comma")
			}
			lastWasComma = true
			continue
		}
		tok := p.current()
		if tok.kind != tokenWord {
			return nil, p.errorAtCurrent("SPL_EXPECTED_FIELD", "expected a sort field")
		}
		field := tok.text
		descending := false
		if strings.HasPrefix(field, "-") {
			descending = true
			field = strings.TrimPrefix(field, "-")
		} else if strings.HasPrefix(field, "+") {
			field = strings.TrimPrefix(field, "+")
		}
		if field == "" {
			return nil, p.errorAtCurrent("SPL_EXPECTED_FIELD", "expected a sort field after direction prefix")
		}
		command.Fields = append(command.Fields, SortField{Field: field, Descending: descending, Range: tok.range_})
		end = tok.range_.End
		lastWasComma = false
		p.advance()
	}
	if len(command.Fields) == 0 || lastWasComma {
		return nil, p.errorAtCurrent("SPL_EXPECTED_FIELD", "sort requires at least one field")
	}
	command.Range = Range{Start: name.range_.Start, End: end}
	return command, nil
}

func (p *parser) parseDedupCommand(name token) (Command, error) {
	unsupported := func(tok token, message string) (Command, error) {
		return nil, &Diagnostic{
			Code:        "SPL_UNSUPPORTED_DEDUP_SYNTAX",
			Message:     message,
			Range:       tok.range_,
			Suggestions: []string{"dedup field", "dedup 2 field1, field2"},
		}
	}
	if p.atCommandEnd() {
		return unsupported(p.current(), "dedup requires at least one exact field")
	}

	command := &DedupCommand{Count: 1}
	first := p.current()
	if first.kind == tokenWord && integerSyntax(first.text) {
		count, err := strconv.ParseUint(first.text, 10, 64)
		if err != nil || count == 0 {
			return unsupported(first, "dedup count must be a positive unsigned 64-bit integer")
		}
		command.Count = count
		p.advance()
	}

	end := name.range_.End
	wantField := true
	seen := make(map[string]struct{})
	for !p.atCommandEnd() {
		tok := p.current()
		if tok.kind == tokenComma {
			if wantField {
				return unsupported(tok, "dedup requires an exact field before each comma")
			}
			wantField = true
			p.advance()
			continue
		}
		if tok.kind != tokenWord {
			return unsupported(tok, "dedup supports unquoted exact field names only")
		}
		lower := strings.ToLower(tok.text)
		if lower == "keepempty" || lower == "consecutive" || lower == "keepevents" || lower == "sortby" {
			return unsupported(tok, fmt.Sprintf("dedup option %q is not supported", tok.text))
		}
		if strings.Contains(tok.text, "*") {
			return unsupported(tok, "dedup wildcard fields are not supported")
		}
		if _, duplicate := seen[tok.text]; duplicate {
			return unsupported(tok, fmt.Sprintf("dedup field %q is duplicated", tok.text))
		}
		if len(command.Fields) >= maxDedupFields {
			return unsupported(tok, fmt.Sprintf("dedup supports at most %d fields", maxDedupFields))
		}
		seen[tok.text] = struct{}{}
		command.Fields = append(command.Fields, DedupField{Name: tok.text, Range: tok.range_})
		end = tok.range_.End
		wantField = false
		p.advance()
	}
	if len(command.Fields) == 0 || wantField {
		return unsupported(p.current(), "dedup requires at least one exact field and cannot end with a comma")
	}
	command.Range = Range{Start: name.range_.Start, End: end}
	return command, nil
}

func (p *parser) parseLimitCommand(name string, nameToken token) (Command, error) {
	count := uint64(10)
	end := nameToken.range_.End
	if !p.atCommandEnd() {
		tok := p.current()
		if tok.kind != tokenWord {
			return nil, p.errorAtCurrent("SPL_INVALID_ARGUMENT", fmt.Sprintf("%s count must be a positive integer", name))
		}
		parsed, err := strconv.ParseUint(tok.text, 10, 64)
		if err != nil || parsed == 0 {
			return nil, p.errorAtCurrent("SPL_INVALID_ARGUMENT", fmt.Sprintf("%s count must be a positive integer", name))
		}
		count = parsed
		end = tok.range_.End
		p.advance()
	}
	if !p.atCommandEnd() {
		return nil, p.errorAtCurrent("SPL_UNSUPPORTED_ARGUMENT", fmt.Sprintf("unsupported %s argument %q", name, p.current().text))
	}
	return &LimitCommand{CommandName: name, Count: count, Range: Range{Start: nameToken.range_.Start, End: end}}, nil
}

// parseSearchExpression implements Splunk base-search precedence:
// parentheses, NOT, OR, AND. Adjacent operands imply AND.
func (p *parser) parseSearchExpression() (Expr, error) {
	return p.parseSearchAnd()
}

// parseWhereExpression implements expression-language precedence:
// parentheses, NOT, AND, OR. Unlike search, adjacent operands do not imply
// AND and a primary must be a scalar-to-scalar comparison.
func (p *parser) parseWhereExpression() (WhereExpr, error) {
	return p.parseWhereOr()
}

func (p *parser) parseWhereOr() (WhereExpr, error) {
	left, err := p.parseWhereAnd()
	if err != nil {
		return nil, err
	}
	for p.isKeyword("OR") {
		p.advance()
		if !p.canStartWhereOperand() {
			return nil, p.errorAtCurrent("SPL_EXPECTED_EXPRESSION", "expected an expression after OR")
		}
		right, parseErr := p.parseWhereAnd()
		if parseErr != nil {
			return nil, parseErr
		}
		left = &WhereBoolExpr{Op: BoolOpOr, Left: left, Right: right, Range: Range{Start: left.SourceRange().Start, End: right.SourceRange().End}}
	}
	return left, nil
}

func (p *parser) parseWhereAnd() (WhereExpr, error) {
	left, err := p.parseWhereUnary()
	if err != nil {
		return nil, err
	}
	for p.isKeyword("AND") {
		p.advance()
		if !p.canStartWhereOperand() {
			return nil, p.errorAtCurrent("SPL_EXPECTED_EXPRESSION", "expected an expression after AND")
		}
		right, parseErr := p.parseWhereUnary()
		if parseErr != nil {
			return nil, parseErr
		}
		left = &WhereBoolExpr{Op: BoolOpAnd, Left: left, Right: right, Range: Range{Start: left.SourceRange().Start, End: right.SourceRange().End}}
	}
	return left, nil
}

func (p *parser) parseWhereUnary() (WhereExpr, error) {
	if p.isKeyword("NOT") {
		start := p.current().range_.Start
		p.advance()
		if !p.canStartWhereOperand() {
			return nil, p.errorAtCurrent("SPL_EXPECTED_EXPRESSION", "expected an expression after NOT")
		}
		operand, err := p.parseWhereUnary()
		if err != nil {
			return nil, err
		}
		return &WhereNotExpr{Operand: operand, Range: Range{Start: start, End: operand.SourceRange().End}}, nil
	}
	return p.parseWherePrimary()
}

func (p *parser) parseWherePrimary() (WhereExpr, error) {
	if p.match(tokenLeftParen) {
		start := p.previous().range_.Start
		if p.current().kind == tokenRightParen {
			return nil, p.errorAtCurrent("SPL_EXPECTED_EXPRESSION", "empty parenthesized where expression")
		}
		expression, err := p.parseWhereExpression()
		if err != nil {
			return nil, err
		}
		if !p.match(tokenRightParen) {
			return nil, p.errorAtCurrent("SPL_EXPECTED_RIGHT_PAREN", "expected ')' to close where expression")
		}
		setWhereExpressionRange(expression, Range{Start: start, End: p.previous().range_.End})
		return expression, nil
	}

	left, err := p.parseScalarExpression()
	if err != nil {
		return nil, err
	}
	op, ok := comparisonOperator(p.current().kind)
	if !ok {
		if p.current().kind == tokenWord && unsupportedScalarIdentifier(p.current().text) {
			return nil, &Diagnostic{
				Code:        "SPL_UNSUPPORTED_EVAL_EXPRESSION",
				Message:     fmt.Sprintf("unsupported where scalar operator %q", p.current().text),
				Range:       p.current().range_,
				Suggestions: []string{"use a supported comparison operator"},
			}
		}
		return nil, &Diagnostic{
			Code:        "SPL_EXPECTED_COMPARISON",
			Message:     "where scalar expression must be followed by a comparison operator",
			Range:       left.SourceRange(),
			Suggestions: []string{"where field=value"},
		}
	}
	p.advance()
	right, err := p.parseScalarExpression()
	if err != nil {
		return nil, err
	}
	if p.whereComparisons >= maxWhereComparisons {
		return nil, &Diagnostic{
			Code:    "SPL_QUERY_TOO_COMPLEX",
			Message: fmt.Sprintf("search contains more than %d where comparisons", maxWhereComparisons),
			Range:   left.SourceRange(),
		}
	}
	p.whereComparisons++
	return &WhereComparisonExpr{
		Left:  left,
		Op:    op,
		Right: right,
		Range: Range{Start: left.SourceRange().Start, End: right.SourceRange().End},
	}, nil
}

func (p *parser) parseScalarExpression() (ScalarExpr, error) {
	if p.scalarDepth >= maxScalarNestingDepth {
		return nil, &Diagnostic{
			Code:    "SPL_QUERY_TOO_COMPLEX",
			Message: fmt.Sprintf("scalar expression nesting exceeds %d levels", maxScalarNestingDepth),
			Range:   p.current().range_,
		}
	}
	p.scalarDepth++
	defer func() { p.scalarDepth-- }()

	tok := p.current()
	if tok.kind == tokenString {
		p.advance()
		literal := Literal{Kind: LiteralKindString, Text: tok.text, Quoted: true, Range: tok.range_}
		return &ScalarLiteralExpr{Value: literal, Range: tok.range_}, nil
	}
	if tok.kind != tokenWord || p.isKeyword("AND") || p.isKeyword("OR") || p.isKeyword("NOT") {
		return nil, p.errorAtCurrent("SPL_EXPECTED_SCALAR_EXPRESSION", "expected a field, literal, or supported function call")
	}
	p.advance()
	if p.match(tokenLeftParen) {
		return p.parseScalarCall(tok)
	}
	kind := classifyLiteral(tok.text, false)
	if kind != LiteralKindString {
		literal := Literal{Kind: kind, Text: tok.text, Range: tok.range_}
		return &ScalarLiteralExpr{Value: literal, Range: tok.range_}, nil
	}
	if unsupportedScalarIdentifier(tok.text) {
		return nil, &Diagnostic{
			Code:        "SPL_UNSUPPORTED_EVAL_EXPRESSION",
			Message:     fmt.Sprintf("unsupported unquoted scalar expression %q", tok.text),
			Range:       tok.range_,
			Suggestions: []string{"use a supported field, literal, or function call"},
		}
	}
	return &ScalarFieldExpr{Field: tok.text, Range: tok.range_}, nil
}

func unsupportedScalarIdentifier(value string) bool {
	return strings.ContainsAny(value, "+-*/%'")
}

func (p *parser) parseScalarCall(name token) (ScalarExpr, error) {
	arguments := make([]ScalarExpr, 0, 3)
	if p.current().kind != tokenRightParen {
		for {
			argument, err := p.parseScalarExpression()
			if err != nil {
				return nil, err
			}
			arguments = append(arguments, argument)
			if !p.match(tokenComma) {
				break
			}
			if p.current().kind == tokenRightParen {
				return nil, p.errorAtCurrent("SPL_EXPECTED_SCALAR_EXPRESSION", "expected a function argument after comma")
			}
		}
	}
	if !p.match(tokenRightParen) {
		return nil, p.errorAtCurrent("SPL_EXPECTED_RIGHT_PAREN", "expected ')' to close function call")
	}
	function := ScalarFunctionInvalid
	switch strings.ToLower(name.text) {
	case "tonumber":
		function = ScalarFunctionToNumber
		if len(arguments) != 1 {
			return nil, &Diagnostic{Code: "SPL_INVALID_EVAL_ARITY", Message: "tonumber requires exactly one argument in compatibility version 0.1", Range: name.range_}
		}
	case "replace":
		function = ScalarFunctionReplace
		if len(arguments) != 3 {
			return nil, &Diagnostic{Code: "SPL_INVALID_EVAL_ARITY", Message: "replace requires exactly three arguments", Range: name.range_}
		}
		for index := 1; index < 3; index++ {
			literal, ok := arguments[index].(*ScalarLiteralExpr)
			if !ok || literal.Value.Kind != LiteralKindString || !literal.Value.Quoted {
				return nil, &Diagnostic{
					Code:        "SPL_UNSUPPORTED_EVAL_EXPRESSION",
					Message:     "replace regex and replacement arguments must be quoted string literals",
					Range:       arguments[index].SourceRange(),
					Suggestions: []string{`replace(field, "pattern", "replacement")`},
				}
			}
		}
		pattern := arguments[1].(*ScalarLiteralExpr)
		if pattern.Value.Text == "" {
			return nil, &Diagnostic{
				Code:        "SPL_UNSUPPORTED_REGEX",
				Message:     "replace does not support an empty regular expression in compatibility version 0.1",
				Range:       pattern.Range,
				Suggestions: []string{"use a non-empty RE2-compatible regular expression"},
			}
		}
		if err := splregex.ValidateReplacePattern(pattern.Value.Text); err != nil {
			return nil, &Diagnostic{
				Code:        "SPL_UNSUPPORTED_REGEX",
				Message:     "replace regular expression is outside the supported always-consuming RE2-compatible subset",
				Range:       pattern.Range,
				Suggestions: []string{"use an RE2-compatible regular expression"},
			}
		}
	default:
		return nil, &Diagnostic{
			Code:        "SPL_UNSUPPORTED_EVAL_FUNCTION",
			Message:     fmt.Sprintf("eval function %q is not supported", name.text),
			Range:       name.range_,
			Suggestions: []string{"tonumber(value)", `replace(value, "pattern", "replacement")`},
		}
	}
	return &ScalarCallExpr{
		Function:  function,
		Arguments: arguments,
		Range:     Range{Start: name.range_.Start, End: p.previous().range_.End},
	}, nil
}

func (p *parser) parseSearchAnd() (Expr, error) {
	left, err := p.parseSearchOr()
	if err != nil {
		return nil, err
	}
	for {
		explicit := p.isKeyword("AND")
		if explicit {
			p.advance()
		}
		if !explicit && !p.canStartSearchOperand() {
			break
		}
		if explicit && !p.canStartSearchOperand() {
			return nil, p.errorAtCurrent("SPL_EXPECTED_EXPRESSION", "expected an expression after AND")
		}
		right, parseErr := p.parseSearchOr()
		if parseErr != nil {
			return nil, parseErr
		}
		left = &BinaryExpr{Op: BoolOpAnd, Left: left, Right: right, Range: Range{Start: left.SourceRange().Start, End: right.SourceRange().End}}
	}
	return left, nil
}

func (p *parser) parseSearchOr() (Expr, error) {
	left, err := p.parseSearchUnary()
	if err != nil {
		return nil, err
	}
	for p.isKeyword("OR") {
		p.advance()
		if !p.canStartSearchOperand() {
			return nil, p.errorAtCurrent("SPL_EXPECTED_EXPRESSION", "expected an expression after OR")
		}
		right, parseErr := p.parseSearchUnary()
		if parseErr != nil {
			return nil, parseErr
		}
		left = &BinaryExpr{Op: BoolOpOr, Left: left, Right: right, Range: Range{Start: left.SourceRange().Start, End: right.SourceRange().End}}
	}
	return left, nil
}

func (p *parser) parseSearchUnary() (Expr, error) {
	if p.isKeyword("NOT") {
		start := p.current().range_.Start
		p.advance()
		if !p.canStartSearchOperand() {
			return nil, p.errorAtCurrent("SPL_EXPECTED_EXPRESSION", "expected an expression after NOT")
		}
		operand, err := p.parseSearchUnary()
		if err != nil {
			return nil, err
		}
		return &NotExpr{Operand: operand, Range: Range{Start: start, End: operand.SourceRange().End}}, nil
	}
	return p.parseSearchPrimary()
}

func (p *parser) parseSearchPrimary() (Expr, error) {
	if p.match(tokenLeftParen) {
		start := p.previous().range_.Start
		if p.current().kind == tokenRightParen {
			return nil, p.errorAtCurrent("SPL_EXPECTED_EXPRESSION", "empty parenthesized expression")
		}
		expression, err := p.parseSearchExpression()
		if err != nil {
			return nil, err
		}
		if !p.match(tokenRightParen) {
			return nil, p.errorAtCurrent("SPL_EXPECTED_RIGHT_PAREN", "expected ')' to close search expression")
		}
		setExpressionRange(expression, Range{Start: start, End: p.previous().range_.End})
		return expression, nil
	}

	tok := p.current()
	if tok.kind == tokenString {
		p.advance()
		return &TermExpr{Value: tok.text, Quoted: true, Range: tok.range_}, nil
	}
	if tok.kind != tokenWord || p.isKeyword("AND") || p.isKeyword("OR") {
		return nil, p.errorAtCurrent("SPL_EXPECTED_EXPRESSION", "expected a search term or field comparison")
	}
	p.advance()
	if op, ok := comparisonOperator(p.current().kind); ok {
		p.advance()
		literal, err := p.parseLiteral()
		if err != nil {
			return nil, err
		}
		return &ComparisonExpr{
			Field: tok.text,
			Op:    op,
			Value: literal,
			Range: Range{Start: tok.range_.Start, End: literal.Range.End},
		}, nil
	}
	return &TermExpr{Value: tok.text, Range: tok.range_}, nil
}

func (p *parser) parseLiteral() (Literal, error) {
	tok := p.current()
	if tok.kind != tokenWord && tok.kind != tokenString {
		return Literal{}, p.errorAtCurrent("SPL_EXPECTED_LITERAL", "expected a comparison value")
	}
	p.advance()
	kind := classifyLiteral(tok.text, tok.quoted)
	return Literal{Kind: kind, Text: tok.text, Quoted: tok.quoted, Range: tok.range_}, nil
}

func classifyLiteral(text string, quoted bool) LiteralKind {
	if quoted {
		return LiteralKindString
	}
	switch strings.ToLower(text) {
	case "true", "false":
		return LiteralKindBool
	case "null":
		return LiteralKindNull
	}
	if integerSyntax(text) {
		return LiteralKindInteger
	}
	if floatSyntax(text) {
		return LiteralKindFloat
	}
	return LiteralKindString
}

func unsignedIntegerSyntax(text string) bool {
	if text == "" {
		return false
	}
	for i := range len(text) {
		if text[i] < '0' || text[i] > '9' {
			return false
		}
	}
	return true
}

func floatSyntax(text string) bool {
	if text == "" {
		return false
	}
	i := 0
	if text[i] == '-' || text[i] == '+' {
		i++
	}
	digits := 0
	for i < len(text) && text[i] >= '0' && text[i] <= '9' {
		i++
		digits++
	}
	hasDecimalPoint := false
	if i < len(text) && text[i] == '.' {
		hasDecimalPoint = true
		i++
		for i < len(text) && text[i] >= '0' && text[i] <= '9' {
			i++
			digits++
		}
	}
	if digits == 0 {
		return false
	}
	hasExponent := false
	if i < len(text) && (text[i] == 'e' || text[i] == 'E') {
		hasExponent = true
		i++
		if i < len(text) && (text[i] == '-' || text[i] == '+') {
			i++
		}
		exponentStart := i
		for i < len(text) && text[i] >= '0' && text[i] <= '9' {
			i++
		}
		if exponentStart == i {
			return false
		}
	}
	return i == len(text) && (hasDecimalPoint || hasExponent)
}

func integerSyntax(text string) bool {
	if text == "" {
		return false
	}
	start := 0
	if text[0] == '-' || text[0] == '+' {
		start = 1
	}
	if start == len(text) {
		return false
	}
	for i := start; i < len(text); i++ {
		if text[i] < '0' || text[i] > '9' {
			return false
		}
	}
	return true
}

func comparisonOperator(kind tokenKind) (CompareOp, bool) {
	switch kind {
	case tokenEqual:
		return CompareOpEqual, true
	case tokenNotEqual:
		return CompareOpNotEqual, true
	case tokenLess:
		return CompareOpLess, true
	case tokenLessEqual:
		return CompareOpLessEqual, true
	case tokenGreater:
		return CompareOpGreater, true
	case tokenGreaterEqual:
		return CompareOpGreaterEqual, true
	default:
		return CompareOpInvalid, false
	}
}

func setExpressionRange(expression Expr, sourceRange Range) {
	switch e := expression.(type) {
	case *BinaryExpr:
		e.Range = sourceRange
	case *NotExpr:
		e.Range = sourceRange
	case *TermExpr:
		e.Range = sourceRange
	case *ComparisonExpr:
		e.Range = sourceRange
	}
}

func setWhereExpressionRange(expression WhereExpr, sourceRange Range) {
	switch expression := expression.(type) {
	case *WhereBoolExpr:
		expression.Range = sourceRange
	case *WhereNotExpr:
		expression.Range = sourceRange
	case *WhereComparisonExpr:
		expression.Range = sourceRange
	}
}

func (p *parser) canStartSearchOperand() bool {
	tok := p.current()
	if tok.kind == tokenString || tok.kind == tokenLeftParen {
		return true
	}
	if tok.kind != tokenWord {
		return false
	}
	return !p.isKeyword("AND") && !p.isKeyword("OR")
}

func (p *parser) canStartWhereOperand() bool {
	tok := p.current()
	if tok.kind == tokenLeftParen || tok.kind == tokenString {
		return true
	}
	return tok.kind == tokenWord && !p.isKeyword("AND") && !p.isKeyword("OR")
}

func (p *parser) atCommandEnd() bool {
	return p.current().kind == tokenPipe || p.current().kind == tokenEOF
}

func (p *parser) isKeyword(keyword string) bool {
	return p.current().kind == tokenWord && strings.EqualFold(p.current().text, keyword)
}

func (p *parser) match(kind tokenKind) bool {
	if p.current().kind != kind {
		return false
	}
	p.advance()
	return true
}

func (p *parser) advance() {
	if p.index < len(p.tokens)-1 {
		p.index++
	}
}

func (p *parser) current() token {
	return p.tokens[p.index]
}

func (p *parser) previous() token {
	return p.tokens[p.index-1]
}

func (p *parser) errorAtCurrent(code, message string) *Diagnostic {
	return &Diagnostic{Code: code, Message: message, Range: p.current().range_}
}
