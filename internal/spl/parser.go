package spl

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/Suhaibinator/open-splunk/internal/eventfields"
	"github.com/Suhaibinator/open-splunk/internal/searchtimebounds"
	"github.com/Suhaibinator/open-splunk/internal/splpath"
	"github.com/Suhaibinator/open-splunk/internal/splregex"
	"github.com/Suhaibinator/open-splunk/internal/splrelativetime"
	"github.com/Suhaibinator/open-splunk/internal/spltimeformat"
	"github.com/Suhaibinator/open-splunk/internal/splwildcard"
)

const (
	// MaximumEvalPredicates is the shared authored eval/where predicate-leaf
	// ceiling retained as sealed compiler evidence for knowledge admission.
	MaximumEvalPredicates = 32
	// MaximumSearchSourceBytes is the exact authored-search ceiling. Logical
	// knowledge contracts that promise an authored spelling use this same
	// authority rather than mirroring a numeric limit in another package.
	MaximumSearchSourceBytes = 16 << 10

	maxSPLSourceBytes     = MaximumSearchSourceBytes
	maxSPLTokens          = 1024
	maxPipelineCommands   = 64
	maxEvalAssignments    = 64
	maxRenameAssignments  = 64
	maxDedupFields        = 16
	maxEvalPredicates     = MaximumEvalPredicates
	maxScalarNestingDepth = 32
)

// expressionProfile is deliberately closed and internal. Production-authored
// SPL selects v0.2 while reusable knowledge expressions remain pinned to v0.1;
// callers cannot accidentally construct a hybrid grammar.
type expressionProfile uint8

const (
	expressionProfileInvalid expressionProfile = iota
	expressionProfileV01
	expressionProfileV02
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
			Range:   tokens[maxSPLTokens].sourceRange,
		}
	}
	p := parser{source: source, tokens: tokens, profile: expressionProfileV02}
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
	source                string
	tokens                []token
	index                 int
	profile               expressionProfile
	scalarDepth           int
	unaryDepth            int
	evalPredicates        int
	concatenationOperands int
	arithmeticOperators   int
	membershipCandidates  int
	matchProgramWorkUnits int
	preserveSignedLiteral int
}

func (p *parser) parseQuery() (*Query, error) {
	start := p.current().sourceRange.Start
	query := &Query{}

	if p.current().kind != tokenPipe && p.current().kind != tokenEOF {
		expression, err := p.parseSearchExpression()
		if err != nil {
			return nil, err
		}
		query.Search = expression
	}
	query.parsedEvalPredicatePrefixes = append(
		query.parsedEvalPredicatePrefixes,
		uint32(p.evalPredicates), // #nosec G115 -- parser predicate count is bounded.
	)

	stage := 0
	for p.match(tokenPipe) {
		stage++
		if stage > maxPipelineCommands {
			return nil, &Diagnostic{
				Code:    "SPL_QUERY_TOO_COMPLEX",
				Message: fmt.Sprintf("search contains more than %d pipeline commands", maxPipelineCommands),
				Range:   p.current().sourceRange,
			}
		}
		command, err := p.parseCommand(stage)
		if err != nil {
			return nil, err
		}
		query.Commands = append(query.Commands, command)
		query.parsedEvalPredicatePrefixes = append(
			query.parsedEvalPredicatePrefixes,
			uint32(p.evalPredicates), // #nosec G115 -- parser predicate count is bounded.
		)
	}
	if p.current().kind != tokenEOF {
		return nil, p.errorAtCurrent("SPL_UNEXPECTED_TOKEN", fmt.Sprintf("unexpected token %q", p.current().text))
	}
	if query.Search == nil && len(query.Commands) == 0 {
		return nil, p.errorAtCurrent("SPL_EMPTY_QUERY", "search query is empty")
	}
	query.Range = Range{Start: start, End: p.current().sourceRange.End}
	// #nosec G115 -- countEvalPredicate admits at most MaximumEvalPredicates.
	query.parsedEvalPredicates = uint32(p.evalPredicates)
	query.sourceDigest = sha256.Sum256([]byte(p.source))
	query.parsedSource = strings.Clone(p.source)
	query.parsed = true
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
	if name != "search" && name != "where" && name != "eval" &&
		name != "stats" && name != "eventstats" && name != "streamstats" {
		if err := p.expandLegacyScalarCompositesUntilCommandEnd(); err != nil {
			return nil, err
		}
	}
	switch name {
	case "search":
		return p.parseSearchCommand(nameToken)
	case "where":
		return p.parseWhereCommand(nameToken)
	case "eval":
		return p.parseEvalCommand(nameToken)
	case "rex":
		return p.parseRexCommand(nameToken)
	case "regex":
		return p.parseRegexCommand(nameToken)
	case "reverse":
		return p.parseArgumentFreeCommand(nameToken, "reverse", func(sourceRange Range) Command {
			return &ReverseCommand{Range: sourceRange}
		})
	case "accum":
		return p.parseAccumCommand(nameToken)
	case "strcat":
		return p.parseStrcatCommand(nameToken)
	case "addinfo":
		return p.parseArgumentFreeCommand(nameToken, "addinfo", func(sourceRange Range) Command {
			return &AddInfoCommand{Range: sourceRange}
		})
	case "fillnull":
		return p.parseFillNullCommand(nameToken)
	case "addtotals":
		return p.parseAddTotalsCommand(nameToken)
	case "delta":
		return p.parseDeltaCommand(nameToken)
	case "makemv":
		return p.parseMakeMVCommand(nameToken)
	case "mvexpand":
		return p.parseMVExpandCommand(nameToken)
	case "spath":
		return p.parseSpathCommand(nameToken)
	case "lookup":
		return p.parseLookupCommand(nameToken)
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
	case "eventstats":
		return p.parseEventStatsCommand(nameToken)
	case "streamstats":
		return p.parseStreamStatsCommand(nameToken)
	case "top":
		return p.parseTopCommand(nameToken)
	case "rare":
		return p.parseRareCommand(nameToken)
	case "bin", "bucket":
		return p.parseBinCommand(nameToken)
	case "timechart":
		return p.parseTimechartCommand(nameToken)
	case "chart":
		return p.parseChartCommand(nameToken)
	default:
		return nil, &Diagnostic{
			Code:    "SPL_UNSUPPORTED_COMMAND",
			Message: fmt.Sprintf("unsupported command %q at pipeline stage %d", nameToken.text, stage),
			Range:   nameToken.sourceRange,
		}
	}
}

func (p *parser) parseArgumentFreeCommand(
	name token,
	commandName string,
	build func(Range) Command,
) (Command, error) {
	if !p.atCommandEnd() {
		return nil, &Diagnostic{
			Code:    "SPL_UNSUPPORTED_" + strings.ToUpper(commandName) + "_SYNTAX",
			Message: commandName + " does not accept arguments or options",
			Range:   p.current().sourceRange,
		}
	}
	sourceRange := Range{Start: name.sourceRange.Start, End: name.sourceRange.End}
	return build(sourceRange), nil
}

func (p *parser) parseRegexCommand(name token) (Command, error) {
	command := &RegexCommand{
		Field:      "_raw",
		FieldRange: name.sourceRange,
	}
	if p.atCommandEnd() {
		return nil, &Diagnostic{
			Code:    "SPL_EXPECTED_REGEX_PATTERN",
			Message: "regex requires one quoted regular expression",
			Range:   p.current().sourceRange,
		}
	}

	first := p.current()
	if first.kind == tokenString {
		command.Pattern = first.text
		command.PatternRange = first.sourceRange
		p.advance()
	} else {
		if first.kind != tokenWord || !IsExactUnquotedFieldName(first.text) {
			return nil, &Diagnostic{
				Code:    "SPL_UNSUPPORTED_REGEX_SYNTAX",
				Message: "regex requires a quoted pattern or one exact unquoted field followed by = or !=",
				Range:   first.sourceRange,
			}
		}
		if err := rejectV03CompilerPrivateField("regex", first); err != nil {
			return nil, err
		}
		command.Field = first.text
		command.FieldRange = first.sourceRange
		p.advance()
		switch p.current().kind {
		case tokenEqual:
		case tokenNotEqual:
			command.Negated = true
		default:
			return nil, &Diagnostic{
				Code:    "SPL_UNSUPPORTED_REGEX_SYNTAX",
				Message: "regex field form requires = or != followed by one quoted pattern",
				Range:   p.current().sourceRange,
			}
		}
		p.advance()
		pattern := p.current()
		if pattern.kind != tokenString {
			return nil, &Diagnostic{
				Code:    "SPL_EXPECTED_REGEX_PATTERN",
				Message: "regex pattern must be a quoted String literal",
				Range:   pattern.sourceRange,
			}
		}
		command.Pattern = pattern.text
		command.PatternRange = pattern.sourceRange
		p.advance()
	}
	if !p.atCommandEnd() {
		return nil, &Diagnostic{
			Code:    "SPL_UNSUPPORTED_REGEX_SYNTAX",
			Message: "regex accepts exactly one field and one quoted pattern",
			Range:   p.current().sourceRange,
		}
	}
	if err := p.validateRegexCommandPattern(command.Pattern, command.PatternRange); err != nil {
		return nil, err
	}
	command.Range = Range{Start: name.sourceRange.Start, End: command.PatternRange.End}
	return command, nil
}

func (p *parser) validateRegexCommandPattern(pattern string, sourceRange Range) error {
	compiled, err := splregex.CompileMatchPattern(pattern)
	if err == nil {
		return p.chargeMatchProgram(compiled, sourceRange)
	}
	if splregex.IsMatchComplexityError(err) {
		return &Diagnostic{
			Code: "SPL_QUERY_TOO_COMPLEX",
			Message: fmt.Sprintf(
				"regex regular expression exceeds the %d-byte or %d-work-unit limit",
				splregex.MaximumMatchPatternBytes,
				splregex.MaximumMatchProgramWorkUnits,
			),
			Range: sourceRange,
		}
	}
	return &Diagnostic{
		Code:    "SPL_UNSUPPORTED_REGEX",
		Message: "regex regular expression is outside the supported RE2-compatible subset",
		Range:   sourceRange,
	}
}

func (p *parser) chargeMatchProgram(
	compiled splregex.MatchPattern,
	sourceRange Range,
) error {
	if compiled.ProgramWorkUnits <= 0 ||
		p.matchProgramWorkUnits > splregex.MaximumMatchQueryProgramWorkUnits-
			compiled.ProgramWorkUnits {
		return &Diagnostic{
			Code: "SPL_QUERY_TOO_COMPLEX",
			Message: fmt.Sprintf(
				"search match and regex programs require more than %d work units",
				splregex.MaximumMatchQueryProgramWorkUnits,
			),
			Range: sourceRange,
		}
	}
	p.matchProgramWorkUnits += compiled.ProgramWorkUnits
	return nil
}

func (p *parser) parseAccumCommand(name token) (Command, error) {
	if p.atCommandEnd() {
		return nil, &Diagnostic{
			Code:    "SPL_EXPECTED_FIELD",
			Message: "accum requires one exact unquoted field",
			Range:   p.current().sourceRange,
		}
	}
	field := p.current()
	if field.kind != tokenWord || !IsExactUnquotedFieldName(field.text) {
		return nil, &Diagnostic{
			Code:    "SPL_UNSUPPORTED_ACCUM_SYNTAX",
			Message: "accum requires one exact unquoted field",
			Range:   field.sourceRange,
		}
	}
	if err := rejectV03CompilerPrivateField("accum", field); err != nil {
		return nil, err
	}
	command := &AccumCommand{
		Field:       field.text,
		FieldRange:  field.sourceRange,
		Output:      field.text,
		OutputRange: field.sourceRange,
	}
	end := field.sourceRange.End
	p.advance()
	if p.isKeyword("AS") {
		command.ExplicitOutput = true
		as := p.current()
		p.advance()
		if p.atCommandEnd() {
			return nil, &Diagnostic{
				Code:    "SPL_EXPECTED_FIELD",
				Message: "accum AS requires one exact unquoted output field",
				Range:   as.sourceRange,
			}
		}
		output := p.current()
		if output.kind != tokenWord || !IsExactUnquotedFieldName(output.text) {
			return nil, &Diagnostic{
				Code:    "SPL_UNSUPPORTED_ACCUM_SYNTAX",
				Message: "accum AS requires one exact unquoted output field",
				Range:   output.sourceRange,
			}
		}
		if err := rejectV03CompilerPrivateField("accum", output); err != nil {
			return nil, err
		}
		command.Output = output.text
		command.OutputRange = output.sourceRange
		end = output.sourceRange.End
		p.advance()
	}
	if !p.atCommandEnd() {
		return nil, &Diagnostic{
			Code:    "SPL_UNSUPPORTED_ACCUM_SYNTAX",
			Message: "accum accepts one field and an optional AS output",
			Range:   p.current().sourceRange,
		}
	}
	command.Range = Range{Start: name.sourceRange.Start, End: end}
	return command, nil
}

func (p *parser) parseStrcatCommand(name token) (Command, error) {
	command := &StrcatCommand{}
	end := name.sourceRange.End
	if p.atCommandEnd() {
		return nil, &Diagnostic{
			Code:    "SPL_EXPECTED_FIELD",
			Message: "strcat requires two through 32 source operands and one destination",
			Range:   p.current().sourceRange,
		}
	}

	if p.current().kind == tokenWord && strings.EqualFold(p.current().text, "allrequired") &&
		p.nextIs(tokenEqual) {
		option := p.current()
		p.advance()
		if !p.match(tokenEqual) {
			return nil, &Diagnostic{
				Code:    "SPL_UNSUPPORTED_STRCAT_SYNTAX",
				Message: "strcat allrequired must be written as allrequired=true or allrequired=false",
				Range:   option.sourceRange,
			}
		}
		value := p.current()
		if value.kind != tokenWord {
			return nil, &Diagnostic{
				Code:    "SPL_UNSUPPORTED_STRCAT_SYNTAX",
				Message: "strcat allrequired requires a Boolean value",
				Range:   value.sourceRange,
			}
		}
		parsed, ok := parseV03Bool(value.text)
		if !ok {
			return nil, &Diagnostic{
				Code:    "SPL_UNSUPPORTED_STRCAT_SYNTAX",
				Message: "strcat allrequired must be true or false",
				Range:   value.sourceRange,
			}
		}
		command.AllRequired = parsed
		command.AllRequiredSpecified = true
		command.AllRequiredRange = Range{Start: option.sourceRange.Start, End: value.sourceRange.End}
		end = value.sourceRange.End
		p.advance()
	}

	var values []token
	for !p.atCommandEnd() {
		current := p.current()
		if current.kind != tokenWord && current.kind != tokenString {
			return nil, &Diagnostic{
				Code:    "SPL_UNSUPPORTED_STRCAT_SYNTAX",
				Message: "strcat sources must be exact unquoted fields or quoted String literals",
				Range:   current.sourceRange,
			}
		}
		if current.kind == tokenWord && !IsExactUnquotedFieldName(current.text) {
			return nil, &Diagnostic{
				Code:    "SPL_UNSUPPORTED_STRCAT_SYNTAX",
				Message: "strcat fields must be exact and unquoted",
				Range:   current.sourceRange,
			}
		}
		if current.kind == tokenWord {
			if err := rejectV03CompilerPrivateField("strcat", current); err != nil {
				return nil, err
			}
		}
		values = append(values, current)
		end = current.sourceRange.End
		p.advance()
	}
	if len(values) < 3 {
		return nil, &Diagnostic{
			Code: "SPL_UNSUPPORTED_STRCAT_SYNTAX",
			Message: fmt.Sprintf(
				"strcat requires two through %d source operands and one destination",
				MaximumConcatenationOperands,
			),
			Range: Range{Start: name.sourceRange.Start, End: end},
		}
	}
	if len(values) > MaximumConcatenationOperands+1 {
		return nil, &Diagnostic{
			Code: "SPL_QUERY_TOO_COMPLEX",
			Message: fmt.Sprintf(
				"strcat contains more than %d source operands",
				MaximumConcatenationOperands,
			),
			Range: values[MaximumConcatenationOperands].sourceRange,
		}
	}
	destination := values[len(values)-1]
	if destination.kind != tokenWord {
		return nil, &Diagnostic{
			Code:    "SPL_UNSUPPORTED_STRCAT_SYNTAX",
			Message: "strcat destination must be one exact unquoted field",
			Range:   destination.sourceRange,
		}
	}
	command.Destination = destination.text
	command.DestinationRange = destination.sourceRange
	for _, value := range values[:len(values)-1] {
		operand := StrcatOperand{Range: value.sourceRange}
		if value.kind == tokenString {
			literal := value.text
			operand.Literal = &literal
		} else {
			operand.Field = value.text
		}
		command.Operands = append(command.Operands, operand)
	}
	if p.concatenationOperands > MaximumConcatenationOperandsPerQuery-len(command.Operands) {
		return nil, &Diagnostic{
			Code: "SPL_QUERY_TOO_COMPLEX",
			Message: fmt.Sprintf(
				"concatenation contains more than %d operand occurrences per query",
				MaximumConcatenationOperandsPerQuery,
			),
			Range: Range{Start: name.sourceRange.Start, End: end},
		}
	}
	p.concatenationOperands += len(command.Operands)
	command.Range = Range{Start: name.sourceRange.Start, End: end}
	return command, nil
}

func rejectV03CompilerPrivateField(commandName string, field token) error {
	if !strings.HasPrefix(strings.ToLower(field.text), "__os_") {
		return nil
	}
	return &Diagnostic{
		Code:    "SPL_RESERVED_FIELD",
		Message: commandName + " field uses the compiler-private __os_ namespace",
		Range:   field.sourceRange,
	}
}

func (p *parser) parseSpathCommand(name token) (Command, error) {
	command := &SpathCommand{
		Input:      "_raw",
		InputRange: name.sourceRange,
	}
	var inputSeen, outputSeen, pathSeen bool
	end := name.sourceRange.End

	if p.atCommandEnd() {
		return nil, &Diagnostic{
			Code:        "SPL_UNSUPPORTED_SPATH_SYNTAX",
			Message:     "spath auto-extraction is not supported; provide one explicit JSON path",
			Range:       name.sourceRange,
			Suggestions: []string{"spath path=server.name", "spath output=value path=server.name"},
		}
	}

	parseFieldOption := func(optionName string, seen *bool, destination *string, sourceRange *Range) error {
		option := p.current()
		if *seen {
			return &Diagnostic{
				Code:    "SPL_UNSUPPORTED_SPATH_SYNTAX",
				Message: fmt.Sprintf("spath %s may be specified only once", optionName),
				Range:   option.sourceRange,
			}
		}
		*seen = true
		p.advance()
		if !p.match(tokenEqual) {
			return &Diagnostic{
				Code:    "SPL_UNSUPPORTED_SPATH_SYNTAX",
				Message: fmt.Sprintf("spath %s must be written as %s=<field>", optionName, optionName),
				Range:   option.sourceRange,
			}
		}
		value := p.current()
		if (value.kind != tokenWord && value.kind != tokenString) || value.text == "" {
			return p.errorAtCurrent("SPL_EXPECTED_FIELD", fmt.Sprintf("spath %s requires one exact field", optionName))
		}
		*destination = value.text
		*sourceRange = value.sourceRange
		end = value.sourceRange.End
		p.advance()
		return nil
	}

	parsePath := func(labeled bool) error {
		option := p.current()
		if pathSeen {
			return &Diagnostic{
				Code:    "SPL_UNSUPPORTED_SPATH_SYNTAX",
				Message: "spath path may be specified only once",
				Range:   option.sourceRange,
			}
		}
		pathSeen = true
		if labeled {
			p.advance()
			if !p.match(tokenEqual) {
				return &Diagnostic{
					Code:    "SPL_UNSUPPORTED_SPATH_SYNTAX",
					Message: "spath path must be written as path=<datapath>",
					Range:   option.sourceRange,
				}
			}
		}
		value := p.current()
		if (value.kind != tokenWord && value.kind != tokenString) || value.text == "" {
			return p.errorAtCurrent("SPL_EXPECTED_SPATH_PATH", "spath requires one explicit JSON path")
		}
		steps, err := splpath.ParseJSON(value.text)
		if err != nil {
			var pathErr *splpath.Error
			if !errors.As(err, &pathErr) {
				return err
			}
			var code string
			switch pathErr.Kind {
			case splpath.ErrorKindUnsupported:
				code = "SPL_UNSUPPORTED_SPATH_PATH"
			case splpath.ErrorKindTooComplex:
				code = "SPL_QUERY_TOO_COMPLEX"
			default:
				code = "SPL_INVALID_SPATH_PATH"
			}
			return &Diagnostic{
				Code:        code,
				Message:     fmt.Sprintf("%s at decoded path byte %d", pathErr.Message, pathErr.Offset),
				Range:       value.sourceRange,
				Suggestions: []string{"use a bounded case-sensitive JSON path such as items{0}.name"},
			}
		}
		command.Path = value.text
		command.PathRange = value.sourceRange
		command.Steps = append([]splpath.Step(nil), steps...)
		end = value.sourceRange.End
		p.advance()
		return nil
	}

	for !p.atCommandEnd() {
		switch {
		case p.isKeyword("input") && p.nextIs(tokenEqual):
			if err := parseFieldOption("input", &inputSeen, &command.Input, &command.InputRange); err != nil {
				return nil, err
			}
		case p.isKeyword("output") && p.nextIs(tokenEqual):
			if err := parseFieldOption("output", &outputSeen, &command.Output, &command.OutputRange); err != nil {
				return nil, err
			}
		case p.isKeyword("path") && p.nextIs(tokenEqual):
			if err := parsePath(true); err != nil {
				return nil, err
			}
		case !pathSeen && !p.nextIs(tokenEqual) &&
			(p.current().kind == tokenWord || p.current().kind == tokenString):
			if err := parsePath(false); err != nil {
				return nil, err
			}
		default:
			return nil, &Diagnostic{
				Code:        "SPL_UNSUPPORTED_SPATH_SYNTAX",
				Message:     fmt.Sprintf("unsupported spath option or syntax at %q", p.current().text),
				Range:       p.current().sourceRange,
				Suggestions: []string{"spath input=payload output=value path=server.name"},
			}
		}
	}

	if !pathSeen {
		code := "SPL_UNSUPPORTED_SPATH_SYNTAX"
		message := "spath auto-extraction is not supported; provide one explicit JSON path"
		if outputSeen {
			code = "SPL_EXPECTED_SPATH_PATH"
			message = "spath output requires one explicit JSON path"
		}
		return nil, &Diagnostic{
			Code:        code,
			Message:     message,
			Range:       name.sourceRange,
			Suggestions: []string{"spath path=server.name"},
		}
	}
	if !outputSeen {
		command.Output = command.Path
		command.OutputRange = command.PathRange
	}
	command.Range = Range{Start: name.sourceRange.Start, End: end}
	return command, nil
}

func (p *parser) parseRexCommand(name token) (Command, error) {
	command := &RexCommand{
		Field:      "_raw",
		FieldRange: name.sourceRange,
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
				Range:   option.sourceRange,
			}
		}
		fieldSeen = true
		p.advance()
		if !p.match(tokenEqual) {
			return &Diagnostic{
				Code:        "SPL_UNSUPPORTED_REX_SYNTAX",
				Message:     "rex field must be written as field=<field>",
				Range:       option.sourceRange,
				Suggestions: []string{`rex field=message "(?<value>pattern)"`},
			}
		}
		field := p.current()
		if field.kind != tokenWord {
			return p.errorAtCurrent("SPL_EXPECTED_FIELD", "rex field= requires an exact unquoted field")
		}
		command.Field = field.text
		command.FieldRange = field.sourceRange
		p.advance()
		return nil
	}
	parseMaxMatch := func() (Position, error) {
		option := p.current()
		if maxMatchSeen {
			return Position{}, &Diagnostic{
				Code:    "SPL_UNSUPPORTED_REX_SYNTAX",
				Message: "rex max_match may be specified only once",
				Range:   option.sourceRange,
			}
		}
		maxMatchSeen = true
		p.advance()
		if !p.match(tokenEqual) || p.current().kind != tokenWord {
			return Position{}, &Diagnostic{
				Code:    "SPL_UNSUPPORTED_REX_SYNTAX",
				Message: "rex max_match must be written as max_match=1",
				Range:   option.sourceRange,
			}
		}
		value := p.current()
		maxMatch, err := strconv.ParseUint(value.text, 10, 64)
		if err != nil || maxMatch != 1 {
			return Position{}, &Diagnostic{
				Code:        "SPL_UNSUPPORTED_REX_SYNTAX",
				Message:     "rex currently supports only the first match (max_match=1)",
				Range:       value.sourceRange,
				Suggestions: []string{"omit max_match or use max_match=1"},
			}
		}
		command.MaxMatch = maxMatch
		p.advance()
		return value.sourceRange.End, nil
	}
	unsupportedOption := func(message string) error {
		return &Diagnostic{
			Code:        "SPL_UNSUPPORTED_REX_SYNTAX",
			Message:     message,
			Range:       p.current().sourceRange,
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
	command.PatternRange = pattern.sourceRange
	end := pattern.sourceRange.End
	p.advance()

	if !p.atCommandEnd() {
		if !p.isKeyword("max_match") {
			return nil, &Diagnostic{
				Code:        "SPL_UNSUPPORTED_REX_SYNTAX",
				Message:     fmt.Sprintf("unsupported rex option or syntax at %q", p.current().text),
				Range:       p.current().sourceRange,
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
				Range:   p.current().sourceRange,
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
	command.Range = Range{Start: name.sourceRange.Start, End: end}
	return command, nil
}

func (p *parser) parseRenameCommand(name token) (Command, error) {
	command := &RenameCommand{}
	seenSources := make(map[string]struct{})
	seenDestinations := make(map[string]struct{})
	var end Position

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
				Range:       source.sourceRange,
				Suggestions: []string{"rename old_field AS new_field"},
			}
		}
		if _, duplicate := seenSources[source.text]; duplicate {
			return nil, &Diagnostic{
				Code:    "SPL_DUPLICATE_RENAME_SOURCE",
				Message: fmt.Sprintf("rename source field %q is repeated", source.text),
				Range:   source.sourceRange,
			}
		}
		p.advance()
		if !p.isKeyword("AS") {
			return nil, &Diagnostic{
				Code:        "SPL_EXPECTED_AS",
				Message:     "rename source field must be followed by AS",
				Range:       p.current().sourceRange,
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
				Range:       destination.sourceRange,
				Suggestions: []string{"rename old_field AS new_field"},
			}
		}
		if source.text == destination.text {
			return nil, &Diagnostic{
				Code:    "SPL_INVALID_RENAME",
				Message: fmt.Sprintf("rename source and destination are both %q", source.text),
				Range:   Range{Start: source.sourceRange.Start, End: destination.sourceRange.End},
			}
		}
		if _, duplicate := seenDestinations[destination.text]; duplicate {
			return nil, &Diagnostic{
				Code:    "SPL_DUPLICATE_RENAME_TARGET",
				Message: fmt.Sprintf("rename destination field %q is repeated", destination.text),
				Range:   destination.sourceRange,
			}
		}
		seenSources[source.text] = struct{}{}
		seenDestinations[destination.text] = struct{}{}
		command.Assignments = append(command.Assignments, RenameAssignment{
			Source:           source.text,
			SourceRange:      source.sourceRange,
			Destination:      destination.text,
			DestinationRange: destination.sourceRange,
			Range:            Range{Start: source.sourceRange.Start, End: destination.sourceRange.End},
		})
		if len(command.Assignments) > maxRenameAssignments {
			return nil, &Diagnostic{
				Code:    "SPL_QUERY_TOO_COMPLEX",
				Message: fmt.Sprintf("rename contains more than %d assignments", maxRenameAssignments),
				Range:   destination.sourceRange,
			}
		}
		end = destination.sourceRange.End
		p.advance()
		if p.atCommandEnd() {
			break
		}
		if !p.match(tokenComma) {
			return nil, &Diagnostic{
				Code:        "SPL_EXPECTED_COMMA",
				Message:     "rename pairs must be separated by a comma",
				Range:       p.current().sourceRange,
				Suggestions: []string{"rename first AS one, second AS two"},
			}
		}
		if p.atCommandEnd() {
			return nil, p.errorAtCurrent("SPL_EXPECTED_FIELD", "expected another rename source field after comma")
		}
	}
	command.Range = Range{Start: name.sourceRange.Start, End: end}
	return command, nil
}

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
	if !p.isKeyword("SPAN") {
		return nil, p.unsupportedTimechartSyntax(p.current(), "timechart requires span=<positive integer><s|m|h> before its aggregate")
	}
	spanOption := p.current()
	p.advance()
	if !p.match(tokenEqual) {
		return nil, &Diagnostic{
			Code:        "SPL_EXPECTED_EQUAL",
			Message:     "timechart span must be followed by '='",
			Range:       spanOption.sourceRange,
			Suggestions: []string{timechartSyntaxSuggestion},
		}
	}
	spanToken := p.current()
	if spanToken.kind != tokenWord {
		return nil, &Diagnostic{
			Code:        "SPL_INVALID_ARGUMENT",
			Message:     "timechart span must be a positive integer followed by s, m, or h",
			Range:       spanToken.sourceRange,
			Suggestions: []string{timechartSyntaxSuggestion},
		}
	}
	span, err := parseTimechartSpan(spanToken)
	if err != nil {
		return nil, err
	}
	p.advance()

	aggregate, aggregateEnd, err := p.parseTimechartAggregate()
	if err != nil {
		return nil, err
	}
	if p.atCommandEnd() {
		return &TimechartCommand{
			Span:      span,
			Aggregate: aggregate,
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
	if !p.atCommandEnd() {
		return nil, p.unsupportedTimechartSyntax(p.current(), "only one timechart split field is currently supported")
	}
	return &TimechartCommand{
		Span:      span,
		Aggregate: aggregate,
		SplitBy:   &StatsGroupField{Name: field.text, Range: field.sourceRange},
		Range:     Range{Start: name.sourceRange.Start, End: field.sourceRange.End},
	}, nil
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
}

const timechartSyntaxSuggestion = "timechart span=5m count"

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
		suggestion:  timechartSyntaxSuggestion,
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
			Range:   tok.sourceRange,
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
// "chart <aggregate> OVER <row> BY <column>" and its equivalent spelling
// "chart <aggregate> BY <row>, <column>". Chart never discretizes, so no
// option is accepted; bin/bucket remains the only discretizer.
func (p *parser) parseChartCommand(name token) (Command, error) {
	if diagnostic := p.chartOptionDiagnostic(); diagnostic != nil {
		return nil, diagnostic
	}
	aggregate, _, err := p.parseChartAggregate()
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
			return nil, p.chartSingleSplitSyntax()
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
			return nil, p.chartSingleSplitSyntax()
		}
		current := p.current()
		if current.kind == tokenComma || (current.kind == tokenWord && (supportedStatsAggregateName(current.text) ||
			(p.index+1 < len(p.tokens) && p.tokens[p.index+1].kind == tokenLeftParen))) {
			return nil, p.unsupportedChartAggregate(current, "only one chart aggregate is supported")
		}
		return nil, p.chartClauseDiagnostic("chart requires OVER <row> BY <column> or BY <row>, <column>")
	}

	if over.Name == splitBy.Name {
		return nil, p.unsupportedChartSyntaxAt(splitBy.Range, "chart row and column fields must be different")
	}
	return &ChartCommand{
		Aggregate:       aggregate,
		Over:            over,
		SplitBy:         splitBy,
		OverSpelledOver: overSpelledOver,
		Range:           Range{Start: name.sourceRange.Start, End: splitBy.Range.End},
	}, nil
}

func (p *parser) parseChartAggregate() (StatsAggregate, Position, error) {
	function := p.current()
	if function.kind != tokenWord {
		return StatsAggregate{}, function.sourceRange.End,
			p.unsupportedChartAggregate(
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
		}, function.sourceRange.End, nil
	}

	spec, supported := pivotFieldAggregateSpecForName(functionName)
	if !supported {
		return StatsAggregate{}, function.sourceRange.End,
			p.unsupportedChartAggregate(
				function,
				fmt.Sprintf("chart aggregate %q is not supported; use count, count(field), pN(field), percN(field), sum(field), or avg(field)", function.text),
			)
	}
	p.advance()
	if !p.match(tokenLeftParen) {
		return StatsAggregate{}, function.sourceRange.End,
			p.unsupportedChartSyntax(
				function,
				"chart field aggregates require one exact unquoted field in parentheses",
			)
	}
	input := p.current()
	if input.kind != tokenWord || !IsExactUnquotedFieldName(input.text) ||
		(strings.EqualFold(input.text, "eval") &&
			p.index+1 < len(p.tokens) && p.tokens[p.index+1].kind == tokenLeftParen) {
		return StatsAggregate{}, input.sourceRange.End,
			p.unsupportedChartSyntax(
				input,
				"chart field aggregates require one exact unquoted field",
			)
	}
	p.advance()
	if !p.match(tokenRightParen) {
		return StatsAggregate{}, input.sourceRange.End,
			p.unsupportedChartSyntax(
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
	return aggregate, end, nil
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
	if len(fields) != 2 {
		return StatsGroupField{}, StatsGroupField{}, p.chartSingleSplitSyntax()
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

func (p *parser) chartSingleSplitSyntax() *Diagnostic {
	return &Diagnostic{
		Code:        "SPL_UNSUPPORTED_CHART_SYNTAX",
		Message:     "chart requires one row split field and one column split field",
		Range:       p.current().sourceRange,
		Suggestions: []string{"stats count BY <field>", "chart count OVER row BY column", "chart p95(field) OVER row BY column", "chart sum(field) OVER row BY column"},
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

type parsedFrequencyCommand struct {
	fields      []FrequencyField
	limit       uint64
	sourceRange Range
}

func (p *parser) parseTopCommand(name token) (Command, error) {
	parsed, err := p.parseFrequencyCommand(name, "top")
	if err != nil {
		return nil, err
	}
	return &TopCommand{
		Fields: parsed.fields,
		Limit:  parsed.limit,
		Range:  parsed.sourceRange,
	}, nil
}

func (p *parser) parseRareCommand(name token) (Command, error) {
	parsed, err := p.parseFrequencyCommand(name, "rare")
	if err != nil {
		return nil, err
	}
	return &RareCommand{
		Fields: parsed.fields,
		Limit:  parsed.limit,
		Range:  parsed.sourceRange,
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
				Range:   limitToken.sourceRange,
			}
		}
		command.limit = limit
	}

	if p.atCommandEnd() {
		return parsedFrequencyCommand{}, p.errorAtCurrent("SPL_EXPECTED_FIELD", commandName+" requires one field")
	}
	command.fields = make([]FrequencyField, 0, 1)
	wantField := true
	end := name.sourceRange.End
	for !p.atCommandEnd() {
		tok := p.current()
		if wantField {
			if tok.kind == tokenComma {
				return parsedFrequencyCommand{}, p.errorAtCurrent(
					"SPL_EXPECTED_FIELD",
					commandName+" requires an exact field after each comma",
				)
			}
			if tok.kind == tokenWord && p.index+1 < len(p.tokens) && p.tokens[p.index+1].kind == tokenEqual {
				return parsedFrequencyCommand{}, p.unsupportedFrequencySyntax(
					tok,
					commandName,
					fmt.Sprintf("%s option %q is not supported", commandName, tok.text),
				)
			}
			if tok.kind != tokenWord {
				return parsedFrequencyCommand{}, p.unsupportedFrequencySyntax(
					tok,
					commandName,
					commandName+" supports unquoted exact fields only",
				)
			}
			if strings.EqualFold(tok.text, "BY") {
				return parsedFrequencyCommand{}, p.unsupportedFrequencySyntax(
					tok,
					commandName,
					commandName+" BY clauses are not supported",
				)
			}
			if strings.Contains(tok.text, "*") {
				return parsedFrequencyCommand{}, p.unsupportedFrequencySyntax(
					tok,
					commandName,
					"wildcard "+commandName+" fields are not supported",
				)
			}
			duplicate := false
			for _, field := range command.fields {
				if field.Name == tok.text {
					duplicate = true
					break
				}
			}
			if duplicate {
				return parsedFrequencyCommand{}, p.unsupportedFrequencySyntax(
					tok,
					commandName,
					fmt.Sprintf("%s field %q is repeated", commandName, tok.text),
				)
			}
			if len(command.fields) >= MaximumFrequencyFields {
				return parsedFrequencyCommand{}, &Diagnostic{
					Code:    "SPL_QUERY_TOO_COMPLEX",
					Message: fmt.Sprintf("%s contains more than %d fields", commandName, MaximumFrequencyFields),
					Range:   tok.sourceRange,
				}
			}
			command.fields = append(command.fields, FrequencyField{
				Name:  tok.text,
				Range: tok.sourceRange,
			})
			end = tok.sourceRange.End
			wantField = false
			p.advance()
			continue
		}

		if tok.kind == tokenComma {
			wantField = true
			p.advance()
			continue
		}
		message := commandName + " fields must be separated by commas"
		if tok.kind == tokenWord && p.index+1 < len(p.tokens) && p.tokens[p.index+1].kind == tokenEqual {
			message = fmt.Sprintf("%s option %q is not supported", commandName, tok.text)
		} else if strings.EqualFold(tok.text, "BY") {
			message = commandName + " BY clauses are not supported"
		}
		return parsedFrequencyCommand{}, p.unsupportedFrequencySyntax(tok, commandName, message)
	}
	if len(command.fields) == 0 || wantField {
		return parsedFrequencyCommand{}, p.errorAtCurrent(
			"SPL_EXPECTED_FIELD",
			commandName+" requires at least one exact field and cannot end with a comma",
		)
	}
	command.sourceRange = Range{Start: name.sourceRange.Start, End: end}
	return command, nil
}

func (p *parser) unsupportedFrequencySyntax(tok token, commandName, message string) *Diagnostic {
	return &Diagnostic{
		Code:        "SPL_UNSUPPORTED_" + strings.ToUpper(commandName) + "_SYNTAX",
		Message:     message,
		Range:       tok.sourceRange,
		Suggestions: []string{commandName + " field1, field2", commandName + " limit=20 field1, field2"},
	}
}

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
				"eventstats aggregate %q is not supported; this compatibility slice accepts %s",
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
				"unsupported eventstats syntax at %q; this compatibility slice accepts %s and optional BY",
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
					fmt.Sprintf("streamstats option %q is not supported by the bounded compatibility slice", option.text),
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

func parseV03Bool(value string) (bool, bool) {
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

type statsSparklineAggregateSpec struct {
	function AggregateFunction
}

func statsSparklineAggregateSpecForName(
	name string,
) (statsSparklineAggregateSpec, bool) {
	switch strings.ToLower(name) {
	case "count":
		return statsSparklineAggregateSpec{function: AggregateFunctionCount}, true
	case "c":
		return statsSparklineAggregateSpec{
			function: AggregateFunctionCountValues,
		}, true
	case "dc":
		return statsSparklineAggregateSpec{
			function: AggregateFunctionDistinctCount,
		}, true
	case "mean", "avg":
		return statsSparklineAggregateSpec{
			function: AggregateFunctionAverage,
		}, true
	case "stdev":
		return statsSparklineAggregateSpec{
			function: AggregateFunctionStandardDeviationSample,
		}, true
	case "stdevp":
		return statsSparklineAggregateSpec{
			function: AggregateFunctionStandardDeviationPopulation,
		}, true
	case "var":
		return statsSparklineAggregateSpec{
			function: AggregateFunctionVarianceSample,
		}, true
	case "varp":
		return statsSparklineAggregateSpec{
			function: AggregateFunctionVariancePopulation,
		}, true
	case "sum":
		return statsSparklineAggregateSpec{
			function: AggregateFunctionSum,
		}, true
	case "sumsq":
		return statsSparklineAggregateSpec{
			function: AggregateFunctionSumSquares,
		}, true
	case "min":
		return statsSparklineAggregateSpec{
			function: AggregateFunctionMinimum,
		}, true
	case "max":
		return statsSparklineAggregateSpec{
			function: AggregateFunctionMaximum,
		}, true
	case "range":
		return statsSparklineAggregateSpec{
			function: AggregateFunctionRange,
		}, true
	default:
		return statsSparklineAggregateSpec{}, false
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
	spec, supported := statsSparklineAggregateSpecForName(innerToken.text)
	if !supported {
		return StatsAggregate{}, innerToken.sourceRange.End, p.unsupportedStatsAggregate(
			innerToken,
			fmt.Sprintf("aggregate %q is not supported inside sparkline", innerToken.text),
		)
	}
	sparkline.Function = spec.function

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
// function(eval(<v0.2 scalar expression>)). count(eval(...)) is parsed first as
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

func (p *parser) parseSearchCommand(name token) (Command, error) {
	if p.atCommandEnd() {
		return nil, p.errorAtCurrent("SPL_EXPECTED_EXPRESSION", "search requires an expression")
	}
	expression, err := p.parseSearchExpression()
	if err != nil {
		return nil, err
	}
	return &SearchCommand{Expression: expression, Range: Range{Start: name.sourceRange.Start, End: expression.SourceRange().End}}, nil
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
			Range:       p.current().sourceRange,
			Suggestions: []string{"where field=value AND other_field>0"},
		}
	}
	return &WhereCommand{
		Expression: expression,
		Range:      Range{Start: name.sourceRange.Start, End: expression.SourceRange().End},
	}, nil
}

func (p *parser) parseEvalCommand(name token) (Command, error) {
	command := &EvalCommand{}
	var end Position
	for {
		if len(command.Assignments) >= maxEvalAssignments {
			return nil, &Diagnostic{
				Code:    "SPL_QUERY_TOO_COMPLEX",
				Message: fmt.Sprintf("eval contains more than %d assignments", maxEvalAssignments),
				Range:   p.current().sourceRange,
			}
		}
		field := p.current()
		if p.profile == expressionProfileV02 {
			if field.scalarDiagnostic != nil {
				return nil, field.scalarDiagnostic
			}
			if field.kind == tokenWord && strings.HasPrefix(field.text, "'") {
				return nil, unterminatedQuotedScalarField(field)
			}
		}
		quotedDestination := p.profile == expressionProfileV02 && field.kind == tokenQuotedField
		if field.kind != tokenWord && !quotedDestination {
			return nil, p.errorAtCurrent("SPL_EXPECTED_FIELD", "eval requires a destination field")
		}
		if quotedDestination {
			if err := validateQuotedScalarField(field); err != nil {
				return nil, err
			}
		} else if classifyLiteral(field.text, false) != LiteralKindString || unsupportedScalarIdentifier(field.text) {
			return nil, &Diagnostic{
				Code:        "SPL_UNSUPPORTED_EVAL_EXPRESSION",
				Message:     fmt.Sprintf("unsupported eval destination %q", field.text),
				Range:       field.sourceRange,
				Suggestions: []string{"single-quote an exact destination containing expression punctuation"},
			}
		}
		p.advance()
		if !p.match(tokenEqual) {
			return nil, &Diagnostic{
				Code:        "SPL_EXPECTED_EQUAL",
				Message:     fmt.Sprintf("eval destination field %q must be followed by '='", field.text),
				Range:       field.sourceRange,
				Suggestions: []string{"eval field=expression"},
			}
		}
		expression, err := p.parseScalarExpression()
		if err != nil {
			return nil, err
		}
		if scalarExpressionMayReturnBooleanFunction(expression) {
			return nil, &Diagnostic{
				Code:    "SPL_UNSUPPORTED_EVAL_EXPRESSION",
				Message: "search-mode eval cannot directly assign a Boolean result",
				Range:   expression.SourceRange(),
				Suggestions: []string{
					"use isnull or isnotnull directly with where",
					"consume the Boolean with a supported conditional or conversion function",
				},
			}
		}
		assignment := EvalAssignment{
			Field:      field.text,
			FieldRange: field.sourceRange,
			Expression: expression,
			Range:      Range{Start: field.sourceRange.Start, End: expression.SourceRange().End},
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
			Range:       p.current().sourceRange,
			Suggestions: []string{"eval field=expression"},
		}
	}
	command.Range = Range{Start: name.sourceRange.Start, End: end}
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
	return &FieldsCommand{Fields: fields, Exclude: exclude, Range: Range{Start: name.sourceRange.Start, End: end}}, nil
}

func (p *parser) parseTableCommand(name token) (Command, error) {
	fields, quoted, ranges, end, err := p.parseTableFieldList()
	if err != nil {
		return nil, err
	}
	return &TableCommand{
		Fields:       fields,
		QuotedFields: quoted,
		FieldRanges:  ranges,
		Range:        Range{Start: name.sourceRange.Start, End: end},
	}, nil
}

func (p *parser) parseTableFieldList() ([]string, []bool, []Range, Position, error) {
	fields := make([]string, 0, 8)
	quoted := make([]bool, 0, 8)
	ranges := make([]Range, 0, 8)
	end := p.current().sourceRange.Start
	wantField := true
	for !p.atCommandEnd() {
		tok := p.current()
		if tok.kind == tokenComma {
			if wantField {
				return nil, nil, nil, end, p.errorAtCurrent("SPL_EXPECTED_FIELD", "expected a field name")
			}
			wantField = true
			p.advance()
			continue
		}
		isQuoted := tok.kind == tokenQuotedField
		if isQuoted {
			if tok.scalarDiagnostic != nil {
				return nil, nil, nil, end, tok.scalarDiagnostic
			}
			if err := validateQuotedFieldReference(tok); err != nil {
				return nil, nil, nil, end, err
			}
		}
		if tok.kind != tokenWord && !isQuoted {
			return nil, nil, nil, end, p.errorAtCurrent("SPL_EXPECTED_FIELD", "expected a field name")
		}
		fields = append(fields, tok.text)
		quoted = append(quoted, isQuoted)
		ranges = append(ranges, tok.sourceRange)
		end = tok.sourceRange.End
		wantField = false
		p.advance()
	}
	if len(fields) == 0 || wantField {
		return nil, nil, nil, end, p.errorAtCurrent("SPL_EXPECTED_FIELD", "expected a field name")
	}
	return fields, quoted, ranges, end, nil
}

func (p *parser) parseFieldList() ([]string, Position, error) {
	fields := make([]string, 0, 8)
	end := p.current().sourceRange.Start
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
		end = tok.sourceRange.End
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

	end := name.sourceRange.End
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
		command.Fields = append(command.Fields, SortField{Field: field, Descending: descending, Range: tok.sourceRange})
		end = tok.sourceRange.End
		lastWasComma = false
		p.advance()
	}
	if len(command.Fields) == 0 || lastWasComma {
		return nil, p.errorAtCurrent("SPL_EXPECTED_FIELD", "sort requires at least one field")
	}
	command.Range = Range{Start: name.sourceRange.Start, End: end}
	return command, nil
}

func (p *parser) parseDedupCommand(name token) (Command, error) {
	unsupported := func(tok token, message string) (Command, error) {
		return nil, &Diagnostic{
			Code:        "SPL_UNSUPPORTED_DEDUP_SYNTAX",
			Message:     message,
			Range:       tok.sourceRange,
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

	end := name.sourceRange.End
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
		command.Fields = append(command.Fields, DedupField{Name: tok.text, Range: tok.sourceRange})
		end = tok.sourceRange.End
		wantField = false
		p.advance()
	}
	if len(command.Fields) == 0 || wantField {
		return unsupported(p.current(), "dedup requires at least one exact field and cannot end with a comma")
	}
	command.Range = Range{Start: name.sourceRange.Start, End: end}
	return command, nil
}

func (p *parser) parseLimitCommand(name string, nameToken token) (Command, error) {
	count := uint64(10)
	end := nameToken.sourceRange.End
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
		end = tok.sourceRange.End
		p.advance()
	}
	if !p.atCommandEnd() {
		return nil, p.errorAtCurrent("SPL_UNSUPPORTED_ARGUMENT", fmt.Sprintf("unsupported %s argument %q", name, p.current().text))
	}
	return &LimitCommand{CommandName: name, Count: count, Range: Range{Start: nameToken.sourceRange.Start, End: end}}, nil
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
	expression, err := p.parseWhereOr()
	if err != nil {
		return nil, err
	}
	if p.profile == expressionProfileV02 && whereExpressionContainsMembership(expression) {
		if _, comparison := evalComparisonOperator(p.current().kind, p.profile); comparison {
			return nil, p.membershipSyntaxError(
				p.current().sourceRange,
				"membership is already a Boolean predicate and cannot be compared explicitly",
			)
		}
	}
	return expression, nil
}

func whereExpressionContainsMembership(expression WhereExpr) bool {
	switch expression := expression.(type) {
	case *WhereMembershipExpr:
		return expression != nil
	case *WhereBoolExpr:
		return expression != nil &&
			(whereExpressionContainsMembership(expression.Left) ||
				whereExpressionContainsMembership(expression.Right))
	case *WhereNotExpr:
		return expression != nil && whereExpressionContainsMembership(expression.Operand)
	default:
		return false
	}
}

func (p *parser) parseWhereOr() (WhereExpr, error) {
	left, err := p.parseWhereAnd()
	if err != nil {
		return nil, err
	}
	for {
		if err := p.prepareScalarToken(); err != nil {
			return nil, err
		}
		if !p.isKeyword("OR") {
			return left, nil
		}
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
}

func (p *parser) parseWhereAnd() (WhereExpr, error) {
	left, err := p.parseWhereUnary()
	if err != nil {
		return nil, err
	}
	for {
		if err := p.prepareScalarToken(); err != nil {
			return nil, err
		}
		if !p.isKeyword("AND") {
			return left, nil
		}
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
}

func (p *parser) parseWhereUnary() (WhereExpr, error) {
	if err := p.prepareScalarToken(); err != nil {
		return nil, err
	}
	if p.isKeyword("NOT") {
		start := p.current().sourceRange.Start
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
	if p.profile == expressionProfileV02 && p.isKeyword("IN") && p.nextIs(tokenLeftParen) {
		return p.parseWhereMembershipFunction()
	}

	if p.current().kind == tokenLeftParen && p.profile == expressionProfileV02 {
		// Parse the scalar interpretation on an isolated parser snapshot. This
		// resolves grouped scalar/Boolean syntax from grammar alone while
		// preserving lazy token splits and all complexity counters atomically.
		trial := *p
		trial.tokens = append([]token(nil), p.tokens...)
		left, scalarErr := trial.parseScalarExpression()
		if scalarErr == nil {
			*p = trial
			return p.parseWherePredicateAfterScalar(left)
		}
	}

	if p.match(tokenLeftParen) {
		start := p.previous().sourceRange.Start
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
		setWhereExpressionRange(expression, Range{Start: start, End: p.previous().sourceRange.End})
		return expression, nil
	}

	left, err := p.parseScalarExpression()
	if err != nil {
		return nil, err
	}
	return p.parseWherePredicateAfterScalar(left)
}

func (p *parser) parseWherePredicateAfterScalar(left ScalarExpr) (WhereExpr, error) {
	if p.profile == expressionProfileV02 {
		negated := false
		membership := false
		if p.isKeyword("IN") {
			membership = true
			p.advance()
		} else if p.isKeyword("NOT") && p.index+1 < len(p.tokens) &&
			p.tokens[p.index+1].kind == tokenWord && strings.EqualFold(p.tokens[p.index+1].text, "IN") {
			membership = true
			negated = true
			p.advance()
			p.advance()
		}
		if membership {
			candidates, end, err := p.parseMembershipList()
			if err != nil {
				return nil, err
			}
			if countErr := p.countEvalPredicate(left.SourceRange()); countErr != nil {
				return nil, countErr
			}
			return &WhereMembershipExpr{
				Value:      left,
				Candidates: candidates,
				Negated:    negated,
				Range:      Range{Start: left.SourceRange().Start, End: end},
			}, nil
		}
	}

	op, ok := evalComparisonOperator(p.current().kind, p.profile)
	if !ok {
		if scalarExpressionCanBeDirectPredicate(left) {
			if countErr := p.countEvalPredicate(left.SourceRange()); countErr != nil {
				return nil, countErr
			}
			return &WhereScalarPredicateExpr{
				Value: left,
				Range: left.SourceRange(),
			}, nil
		}
		if p.current().kind == tokenWord && unsupportedScalarIdentifier(p.current().text) {
			return nil, &Diagnostic{
				Code:        "SPL_UNSUPPORTED_EVAL_EXPRESSION",
				Message:     fmt.Sprintf("unsupported where scalar operator %q", p.current().text),
				Range:       p.current().sourceRange,
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
	if countErr := p.countEvalPredicate(left.SourceRange()); countErr != nil {
		return nil, countErr
	}
	return &WhereComparisonExpr{
		Left:  left,
		Op:    op,
		Right: right,
		Range: Range{Start: left.SourceRange().Start, End: right.SourceRange().End},
	}, nil
}

func (p *parser) parseWhereMembershipFunction() (WhereExpr, error) {
	name := p.current()
	p.advance()
	if !p.match(tokenLeftParen) {
		return nil, p.membershipSyntaxError(name.sourceRange, "in requires a parenthesized value and candidate list")
	}
	if p.current().kind == tokenRightParen || p.current().kind == tokenComma {
		return nil, p.membershipSyntaxError(name.sourceRange, "in requires a value followed by at least one candidate")
	}
	value, err := p.parseScalarExpression()
	if err != nil {
		return nil, err
	}
	if !p.match(tokenComma) {
		return nil, p.membershipSyntaxError(name.sourceRange, "in requires at least one candidate after its value")
	}
	if p.current().kind == tokenRightParen || p.current().kind == tokenComma {
		return nil, p.membershipSyntaxError(p.current().sourceRange, "in requires a candidate after comma")
	}
	candidates, end, err := p.parseMembershipCandidates(name.sourceRange)
	if err != nil {
		return nil, err
	}
	if countErr := p.countEvalPredicate(name.sourceRange); countErr != nil {
		return nil, countErr
	}
	return &WhereMembershipExpr{
		Value:      value,
		Candidates: candidates,
		Range:      Range{Start: name.sourceRange.Start, End: end},
	}, nil
}

func (p *parser) parseMembershipList() ([]ScalarExpr, Position, error) {
	start := p.current().sourceRange
	if !p.match(tokenLeftParen) {
		return nil, start.End, p.membershipSyntaxError(start, "membership requires a parenthesized candidate list")
	}
	if p.current().kind == tokenRightParen || p.current().kind == tokenComma {
		return nil, start.End, p.membershipSyntaxError(p.current().sourceRange, "membership requires at least one candidate")
	}
	return p.parseMembershipCandidates(start)
}

func (p *parser) parseMembershipCandidates(listRange Range) ([]ScalarExpr, Position, error) {
	candidates := make([]ScalarExpr, 0, 4)
	for {
		candidate, err := p.parseScalarExpression()
		if err != nil {
			return nil, listRange.End, err
		}
		candidates = append(candidates, candidate)
		if len(candidates) > MaximumMembershipCandidates {
			return nil, candidate.SourceRange().End, &Diagnostic{
				Code:    "SPL_QUERY_TOO_COMPLEX",
				Message: fmt.Sprintf("membership contains more than %d candidates", MaximumMembershipCandidates),
				Range:   Range{Start: listRange.Start, End: candidate.SourceRange().End},
			}
		}
		if p.match(tokenRightParen) {
			end := p.previous().sourceRange.End
			if err := p.chargeMembershipCandidates(len(candidates), Range{Start: listRange.Start, End: end}); err != nil {
				return nil, end, err
			}
			return candidates, end, nil
		}
		if !p.match(tokenComma) {
			return nil, candidate.SourceRange().End, p.membershipSyntaxError(
				p.current().sourceRange,
				"expected ',' or ')' after membership candidate",
			)
		}
		if p.current().kind == tokenRightParen || p.current().kind == tokenComma {
			return nil, candidate.SourceRange().End, p.membershipSyntaxError(
				p.current().sourceRange,
				"membership candidate list cannot contain an empty or trailing candidate",
			)
		}
	}
}

func (p *parser) chargeMembershipCandidates(count int, sourceRange Range) error {
	if p.membershipCandidates > MaximumMembershipCandidatesPerQuery-count {
		return &Diagnostic{
			Code:    "SPL_QUERY_TOO_COMPLEX",
			Message: fmt.Sprintf("search contains more than %d membership candidates", MaximumMembershipCandidatesPerQuery),
			Range:   sourceRange,
		}
	}
	p.membershipCandidates += count
	return nil
}

func (p *parser) membershipSyntaxError(sourceRange Range, message string) *Diagnostic {
	return &Diagnostic{
		Code:        "SPL_UNSUPPORTED_MEMBERSHIP_SYNTAX",
		Message:     message,
		Range:       sourceRange,
		Suggestions: []string{"where field IN (value)", "where in(field, value)"},
	}
}

func (p *parser) parseScalarExpression() (ScalarExpr, error) {
	if p.scalarDepth >= maxScalarNestingDepth {
		return nil, &Diagnostic{
			Code:    "SPL_QUERY_TOO_COMPLEX",
			Message: fmt.Sprintf("scalar expression nesting exceeds %d levels", maxScalarNestingDepth),
			Range:   p.current().sourceRange,
		}
	}
	p.scalarDepth++
	defer func() { p.scalarDepth-- }()
	if p.profile == expressionProfileV01 {
		return p.parseScalarConcatenation(p.parseScalarPrimaryV01)
	}
	if p.profile != expressionProfileV02 {
		return nil, p.errorAtCurrent("SPL_UNSUPPORTED_EVAL_EXPRESSION", "unsupported scalar expression profile")
	}
	return p.parseScalarConcatenation(p.parseScalarAdditive)
}

func (p *parser) parseScalarConcatenation(parseOperand func() (ScalarExpr, error)) (ScalarExpr, error) {
	first, err := parseOperand()
	if err != nil {
		return nil, err
	}
	if !p.match(tokenConcat) {
		return first, nil
	}

	arguments := make([]ScalarExpr, 0, 4)
	arguments = append(arguments, first)
	for {
		argument, argumentErr := parseOperand()
		if argumentErr != nil {
			return nil, argumentErr
		}
		arguments = append(arguments, argument)
		if len(arguments) > MaximumConcatenationOperands {
			return nil, &Diagnostic{
				Code: "SPL_QUERY_TOO_COMPLEX",
				Message: fmt.Sprintf(
					"concatenation contains more than %d operands",
					MaximumConcatenationOperands,
				),
				Range: Range{
					Start: first.SourceRange().Start,
					End:   argument.SourceRange().End,
				},
			}
		}
		if !p.match(tokenConcat) {
			break
		}
	}
	for _, argument := range arguments {
		if scalarExpressionMayReturnBooleanValue(argument) {
			return nil, &Diagnostic{
				Code:    "SPL_UNSUPPORTED_EVAL_EXPRESSION",
				Message: "concatenation cannot consume a Boolean result",
				Range:   argument.SourceRange(),
				Suggestions: []string{
					"convert the Boolean explicitly with tostring",
				},
			}
		}
	}
	if p.concatenationOperands >
		MaximumConcatenationOperandsPerQuery-len(arguments) {
		return nil, &Diagnostic{
			Code: "SPL_QUERY_TOO_COMPLEX",
			Message: fmt.Sprintf(
				"concatenation contains more than %d operand occurrences per query",
				MaximumConcatenationOperandsPerQuery,
			),
			Range: Range{
				Start: first.SourceRange().Start,
				End:   arguments[len(arguments)-1].SourceRange().End,
			},
		}
	}
	p.concatenationOperands += len(arguments)
	return &ScalarCallExpr{
		Function:  ScalarFunctionConcat,
		Arguments: arguments,
		Range: Range{
			Start: first.SourceRange().Start,
			End:   arguments[len(arguments)-1].SourceRange().End,
		},
	}, nil
}

func (p *parser) parseScalarAdditive() (ScalarExpr, error) {
	left, err := p.parseScalarMultiplicative()
	if err != nil {
		return nil, err
	}
	for {
		if err := p.prepareScalarToken(); err != nil {
			return nil, err
		}
		var op ScalarBinaryOp
		switch p.current().kind {
		case tokenPlus:
			op = ScalarBinaryOpAdd
		case tokenMinus:
			op = ScalarBinaryOpSubtract
		default:
			return left, nil
		}
		operatorRange := p.current().sourceRange
		p.advance()
		right, parseErr := p.parseScalarMultiplicative()
		if parseErr != nil {
			return nil, arithmeticOperandError(parseErr, operatorRange)
		}
		if countErr := p.countArithmeticOperator(operatorRange); countErr != nil {
			return nil, countErr
		}
		left = &ScalarBinaryExpr{
			Op:    op,
			Left:  left,
			Right: right,
			Range: Range{Start: left.SourceRange().Start, End: right.SourceRange().End},
		}
	}
}

func (p *parser) parseScalarMultiplicative() (ScalarExpr, error) {
	left, err := p.parseScalarUnary()
	if err != nil {
		return nil, err
	}
	for {
		if err := p.prepareScalarToken(); err != nil {
			return nil, err
		}
		var op ScalarBinaryOp
		switch p.current().kind {
		case tokenMultiply:
			op = ScalarBinaryOpMultiply
		case tokenDivide:
			op = ScalarBinaryOpDivide
		case tokenRemainder:
			op = ScalarBinaryOpRemainder
		default:
			return left, nil
		}
		operatorRange := p.current().sourceRange
		p.advance()
		right, parseErr := p.parseScalarUnary()
		if parseErr != nil {
			return nil, arithmeticOperandError(parseErr, operatorRange)
		}
		if countErr := p.countArithmeticOperator(operatorRange); countErr != nil {
			return nil, countErr
		}
		left = &ScalarBinaryExpr{
			Op:    op,
			Left:  left,
			Right: right,
			Range: Range{Start: left.SourceRange().Start, End: right.SourceRange().End},
		}
	}
}

func (p *parser) parseScalarUnary() (ScalarExpr, error) {
	if err := p.prepareScalarToken(); err != nil {
		return nil, err
	}
	var op ScalarUnaryOp
	switch p.current().kind {
	case tokenPlus:
		op = ScalarUnaryOpPositive
	case tokenMinus:
		op = ScalarUnaryOpNegative
	default:
		return p.parseScalarPrimaryV02()
	}
	operator := p.current()
	if p.unaryDepth >= MaximumUnaryOperatorChain {
		return nil, &Diagnostic{
			Code:    "SPL_QUERY_TOO_COMPLEX",
			Message: fmt.Sprintf("unary operator chain exceeds %d operators", MaximumUnaryOperatorChain),
			Range:   operator.sourceRange,
		}
	}
	p.unaryDepth++
	defer func() { p.unaryDepth-- }()
	p.advance()
	operand, err := p.parseScalarUnary()
	if err != nil {
		return nil, arithmeticOperandError(err, operator.sourceRange)
	}
	if countErr := p.countArithmeticOperator(operator.sourceRange); countErr != nil {
		return nil, countErr
	}
	return &ScalarUnaryExpr{
		Op:      op,
		Operand: operand,
		Range:   Range{Start: operator.sourceRange.Start, End: operand.SourceRange().End},
	}, nil
}

func (p *parser) countArithmeticOperator(sourceRange Range) error {
	if p.arithmeticOperators >= MaximumArithmeticOperatorsPerQuery {
		return &Diagnostic{
			Code:    "SPL_QUERY_TOO_COMPLEX",
			Message: fmt.Sprintf("search contains more than %d arithmetic operators", MaximumArithmeticOperatorsPerQuery),
			Range:   sourceRange,
		}
	}
	p.arithmeticOperators++
	return nil
}

func arithmeticOperandError(err error, operatorRange Range) error {
	var diagnostic *Diagnostic
	if errors.As(err, &diagnostic) && diagnostic.Code == "SPL_EXPECTED_SCALAR_EXPRESSION" {
		return &Diagnostic{
			Code:        "SPL_UNSUPPORTED_ARITHMETIC_SYNTAX",
			Message:     "arithmetic operator must be followed by a scalar operand",
			Range:       operatorRange,
			Suggestions: []string{"provide a numeric scalar operand after the operator"},
		}
	}
	return err
}

func (p *parser) parseScalarPrimaryV01() (ScalarExpr, error) {
	tok := p.current()
	if tok.kind == tokenString {
		p.advance()
		literal := Literal{Kind: LiteralKindString, Text: tok.text, Quoted: true, Range: tok.sourceRange}
		return &ScalarLiteralExpr{Value: literal, Range: tok.sourceRange}, nil
	}
	if tok.kind != tokenWord || p.isKeyword("AND") || p.isKeyword("OR") || p.isKeyword("NOT") {
		return nil, p.errorAtCurrent("SPL_EXPECTED_SCALAR_EXPRESSION", "expected a field, literal, or supported function call")
	}
	p.advance()
	if p.match(tokenLeftParen) {
		if strings.EqualFold(tok.text, "if") {
			return p.parseScalarIf(tok)
		}
		if strings.EqualFold(tok.text, "case") {
			return p.parseScalarCase(tok)
		}
		return p.parseScalarCall(tok)
	}
	kind := classifyLiteral(tok.text, false)
	if kind != LiteralKindString {
		literal := Literal{Kind: kind, Text: tok.text, Range: tok.sourceRange}
		return &ScalarLiteralExpr{Value: literal, Range: tok.sourceRange}, nil
	}
	if unsupportedScalarIdentifier(tok.text) {
		return nil, &Diagnostic{
			Code:        "SPL_UNSUPPORTED_EVAL_EXPRESSION",
			Message:     fmt.Sprintf("unsupported unquoted scalar expression %q", tok.text),
			Range:       tok.sourceRange,
			Suggestions: []string{"use a supported field, literal, or function call"},
		}
	}
	return &ScalarFieldExpr{Field: tok.text, Range: tok.sourceRange}, nil
}

func (p *parser) parseScalarPrimaryV02() (ScalarExpr, error) {
	if err := p.prepareScalarToken(); err != nil {
		return nil, err
	}
	tok := p.current()
	if tok.scalarDiagnostic != nil {
		return nil, tok.scalarDiagnostic
	}
	if tok.kind == tokenWord && strings.HasPrefix(tok.text, "'") {
		return nil, unterminatedQuotedScalarField(tok)
	}
	if tok.kind == tokenMultiply || tok.kind == tokenDivide || tok.kind == tokenRemainder {
		return nil, &Diagnostic{
			Code:        "SPL_UNSUPPORTED_ARITHMETIC_SYNTAX",
			Message:     "binary arithmetic operator is missing its left operand",
			Range:       tok.sourceRange,
			Suggestions: []string{"provide a numeric scalar operand before the operator"},
		}
	}
	if tok.kind == tokenLeftParen {
		p.advance()
		if p.current().kind == tokenRightParen {
			return nil, p.errorAtCurrent("SPL_EXPECTED_SCALAR_EXPRESSION", "empty parenthesized scalar expression")
		}
		expression, err := p.parseScalarExpression()
		if err != nil {
			return nil, err
		}
		if !p.match(tokenRightParen) {
			if _, comparison := evalComparisonOperator(p.current().kind, p.profile); comparison ||
				p.isKeyword("IN") || p.isKeyword("NOT") || p.isKeyword("AND") || p.isKeyword("OR") {
				return nil, &Diagnostic{
					Code:    "SPL_UNSUPPORTED_EVAL_EXPRESSION",
					Message: "a Boolean expression cannot be used as a grouped scalar value",
					Range:   Range{Start: tok.sourceRange.Start, End: p.current().sourceRange.End},
				}
			}
			return nil, p.errorAtCurrent("SPL_EXPECTED_RIGHT_PAREN", "expected ')' to close scalar expression")
		}
		setScalarExpressionRange(expression, Range{Start: tok.sourceRange.Start, End: p.previous().sourceRange.End})
		return expression, nil
	}
	if tok.kind == tokenString {
		p.advance()
		literal := Literal{Kind: LiteralKindString, Text: tok.text, Quoted: true, Range: tok.sourceRange}
		return &ScalarLiteralExpr{Value: literal, Range: tok.sourceRange}, nil
	}
	if tok.kind == tokenQuotedField {
		if err := validateQuotedFieldReference(tok); err != nil {
			return nil, err
		}
		p.advance()
		return &ScalarFieldExpr{Field: tok.text, Quoted: true, Range: tok.sourceRange}, nil
	}
	if tok.kind != tokenWord || p.isKeyword("AND") || p.isKeyword("OR") || p.isKeyword("NOT") {
		return nil, p.errorAtCurrent("SPL_EXPECTED_SCALAR_EXPRESSION", "expected a field, literal, supported function call, or grouped scalar expression")
	}
	p.advance()
	if p.match(tokenLeftParen) {
		if strings.EqualFold(tok.text, "in") {
			return nil, &Diagnostic{
				Code:    "SPL_UNSUPPORTED_EVAL_EXPRESSION",
				Message: "membership is a Boolean predicate and cannot be used as a scalar value",
				Range:   tok.sourceRange,
				Suggestions: []string{
					"use in(value, candidate) inside where, if, case, or count(eval(...))",
				},
			}
		}
		if strings.EqualFold(tok.text, "if") {
			return p.parseScalarIf(tok)
		}
		if strings.EqualFold(tok.text, "case") {
			return p.parseScalarCase(tok)
		}
		return p.parseScalarCall(tok)
	}
	kind := classifyLiteral(tok.text, false)
	if kind != LiteralKindString {
		literal := Literal{Kind: kind, Text: tok.text, Range: tok.sourceRange}
		return &ScalarLiteralExpr{Value: literal, Range: tok.sourceRange}, nil
	}
	if unsupportedScalarIdentifier(tok.text) {
		return nil, &Diagnostic{
			Code:    "SPL_UNSUPPORTED_EVAL_EXPRESSION",
			Message: fmt.Sprintf("unquoted scalar field %q contains reserved expression punctuation", tok.text),
			Range:   tok.sourceRange,
			Suggestions: []string{
				"single-quote the exact scalar field name",
			},
		}
	}
	return &ScalarFieldExpr{Field: tok.text, Range: tok.sourceRange}, nil
}

func validateQuotedScalarField(tok token) error {
	field := tok.text
	if field == "" {
		return &Diagnostic{
			Code:    "SPL_EXPECTED_FIELD",
			Message: "single-quoted scalar field cannot be empty",
			Range:   tok.sourceRange,
		}
	}
	if !utf8.ValidString(field) || strings.ContainsAny(field, "*?") ||
		strings.TrimFunc(field, unicode.IsSpace) != field {
		return &Diagnostic{
			Code:    "SPL_INVALID_FIELD",
			Message: "single-quoted scalar field is empty or contains unsupported whitespace or wildcard syntax",
			Range:   tok.sourceRange,
		}
	}
	for _, value := range field {
		if unicode.IsControl(value) {
			return &Diagnostic{
				Code:    "SPL_INVALID_FIELD",
				Message: "single-quoted scalar field contains a control character",
				Range:   tok.sourceRange,
			}
		}
	}
	segments, err := eventfields.ParseNormalizedSearchFieldPath(field)
	if err != nil || len(segments) == 0 {
		return &Diagnostic{
			Code:    "SPL_INVALID_FIELD",
			Message: "single-quoted scalar field is not a canonical bounded field path",
			Range:   tok.sourceRange,
		}
	}
	if !eventfields.IsCanonicalSPLField(field) && eventfields.IsReservedDynamicRoot(segments[0]) {
		return &Diagnostic{
			Code:    "SPL_INVALID_FIELD",
			Message: "single-quoted scalar field uses a reserved or compiler-private root",
			Range:   tok.sourceRange,
		}
	}
	return nil
}

func validateQuotedFieldReference(tok token) error {
	if IsExactQuotedFieldName(tok.text) ||
		IsStatsLiteralFieldReference(tok.text) {
		return nil
	}
	return validateQuotedScalarField(tok)
}

func unterminatedQuotedScalarField(tok token) *Diagnostic {
	end := advanceSourcePosition(tok.sourceRange.Start, "'")
	return &Diagnostic{
		Code:    "SPL_UNTERMINATED_FIELD_QUOTE",
		Message: "unterminated single-quoted field reference",
		Range:   Range{Start: tok.sourceRange.Start, End: end},
	}
}

func (p *parser) parseScalarIf(name token) (ScalarExpr, error) {
	invalidArity := func(sourceRange Range) error {
		return &Diagnostic{
			Code:    "SPL_INVALID_EVAL_ARITY",
			Message: "if requires exactly three arguments",
			Range:   sourceRange,
		}
	}
	if p.current().kind == tokenRightParen || p.current().kind == tokenComma {
		return nil, invalidArity(name.sourceRange)
	}
	condition, err := p.parseWhereExpression()
	if err != nil {
		return nil, err
	}
	if !p.match(tokenComma) {
		if p.current().kind == tokenRightParen || p.atCommandEnd() {
			return nil, invalidArity(name.sourceRange)
		}
		if p.current().kind == tokenWord {
			return nil, &Diagnostic{
				Code: "SPL_UNSUPPORTED_WHERE_EXPRESSION",
				Message: fmt.Sprintf(
					"unsupported if predicate syntax at %q; explicit AND or OR is required between predicates",
					p.current().text,
				),
				Range:       p.current().sourceRange,
				Suggestions: []string{`if(first=value AND second=value, "yes", "no")`},
			}
		}
		return nil, p.errorAtCurrent("SPL_EXPECTED_COMMA", "expected ',' after if predicate")
	}
	if p.current().kind == tokenRightParen || p.current().kind == tokenComma {
		return nil, invalidArity(name.sourceRange)
	}
	trueValue, err := p.parseScalarExpression()
	if err != nil {
		return nil, err
	}
	if !p.match(tokenComma) {
		return nil, invalidArity(name.sourceRange)
	}
	if p.current().kind == tokenRightParen || p.current().kind == tokenComma {
		return nil, invalidArity(name.sourceRange)
	}
	falseValue, err := p.parseScalarExpression()
	if err != nil {
		return nil, err
	}
	if p.current().kind == tokenComma {
		return nil, invalidArity(name.sourceRange)
	}
	if !p.match(tokenRightParen) {
		return nil, p.errorAtCurrent("SPL_EXPECTED_RIGHT_PAREN", "expected ')' to close if function")
	}
	return &ScalarIfExpr{
		Condition: condition,
		True:      trueValue,
		False:     falseValue,
		Range:     Range{Start: name.sourceRange.Start, End: p.previous().sourceRange.End},
	}, nil
}

func (p *parser) parseScalarCase(name token) (ScalarExpr, error) {
	invalidArity := func(sourceRange Range) error {
		return &Diagnostic{
			Code:    "SPL_INVALID_EVAL_ARITY",
			Message: "case requires one or more condition/value pairs",
			Range:   sourceRange,
		}
	}
	if p.current().kind == tokenRightParen || p.current().kind == tokenComma {
		return nil, invalidArity(name.sourceRange)
	}

	branches := make([]ScalarCaseBranch, 0, 2)
	for {
		condition, err := p.parseWhereExpression()
		if err != nil {
			return nil, err
		}
		if !p.match(tokenComma) {
			if p.current().kind == tokenRightParen || p.atCommandEnd() {
				return nil, invalidArity(name.sourceRange)
			}
			if p.current().kind == tokenWord {
				return nil, &Diagnostic{
					Code: "SPL_UNSUPPORTED_WHERE_EXPRESSION",
					Message: fmt.Sprintf(
						"unsupported case predicate syntax at %q; explicit AND or OR is required between predicates",
						p.current().text,
					),
					Range:       p.current().sourceRange,
					Suggestions: []string{`case(first=value AND second=value, "match")`},
				}
			}
			return nil, p.errorAtCurrent(
				"SPL_EXPECTED_COMMA",
				"expected ',' after case predicate",
			)
		}
		if p.current().kind == tokenRightParen || p.current().kind == tokenComma {
			return nil, invalidArity(name.sourceRange)
		}
		value, err := p.parseScalarExpression()
		if err != nil {
			return nil, err
		}
		branches = append(branches, ScalarCaseBranch{
			Condition: condition,
			Value:     value,
			Range: Range{
				Start: condition.SourceRange().Start,
				End:   value.SourceRange().End,
			},
		})
		if len(branches) > MaximumCaseBranches {
			return nil, &Diagnostic{
				Code: "SPL_QUERY_TOO_COMPLEX",
				Message: fmt.Sprintf(
					"case contains more than %d condition/value pairs",
					MaximumCaseBranches,
				),
				Range: name.sourceRange,
			}
		}
		if !p.match(tokenComma) {
			break
		}
		if p.current().kind == tokenRightParen || p.current().kind == tokenComma {
			return nil, invalidArity(name.sourceRange)
		}
	}
	if !p.match(tokenRightParen) {
		return nil, p.errorAtCurrent(
			"SPL_EXPECTED_RIGHT_PAREN",
			"expected ')' to close case function",
		)
	}
	return &ScalarCaseExpr{
		Branches: branches,
		Range:    Range{Start: name.sourceRange.Start, End: p.previous().sourceRange.End},
	}, nil
}

func unsupportedScalarIdentifier(value string) bool {
	return strings.ContainsAny(value, "+-*/%'`")
}

func (p *parser) parseScalarCall(name token) (ScalarExpr, error) {
	arguments := make([]ScalarExpr, 0, 3)
	functionName := strings.ToLower(name.text)
	if p.current().kind != tokenRightParen {
		for {
			argumentIndex := len(arguments)
			preserveSignedLiteral :=
				(functionName == "substr" && argumentIndex >= 1) ||
					(functionName == "round" && argumentIndex == 1)
			if preserveSignedLiteral {
				p.preserveSignedLiteral++
			}
			argument, err := p.parseScalarExpression()
			if preserveSignedLiteral {
				p.preserveSignedLiteral--
			}
			if err != nil {
				return nil, err
			}
			arguments = append(arguments, argument)
			if functionName == "coalesce" && len(arguments) > MaximumCoalesceArguments {
				return nil, &Diagnostic{
					Code: "SPL_QUERY_TOO_COMPLEX",
					Message: fmt.Sprintf(
						"coalesce contains more than %d arguments",
						MaximumCoalesceArguments,
					),
					Range: name.sourceRange,
				}
			}
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
	var function ScalarFunction
	switch functionName {
	case "now":
		function = ScalarFunctionNow
		if len(arguments) != 0 {
			return nil, &Diagnostic{
				Code:    "SPL_INVALID_EVAL_ARITY",
				Message: "now requires no arguments",
				Range:   name.sourceRange,
			}
		}
	case "strftime":
		function = ScalarFunctionStrftime
		if len(arguments) != 2 {
			return nil, &Diagnostic{
				Code:    "SPL_INVALID_EVAL_ARITY",
				Message: "strftime requires exactly two arguments",
				Range:   name.sourceRange,
			}
		}
		if scalarExpressionMayReturnBooleanFunction(arguments[0]) {
			return nil, &Diagnostic{
				Code:    "SPL_UNSUPPORTED_EVAL_EXPRESSION",
				Message: "strftime cannot consume a Boolean time value",
				Range:   arguments[0].SourceRange(),
				Suggestions: []string{
					`strftime(_time, "%Y-%m-%dT%H:%M:%S.%Q")`,
				},
			}
		}
		format, ok := arguments[1].(*ScalarLiteralExpr)
		if !ok || format == nil || format.Value.Kind != LiteralKindString ||
			!format.Value.Quoted {
			return nil, &Diagnostic{
				Code:    "SPL_UNSUPPORTED_EVAL_EXPRESSION",
				Message: "strftime format must be a quoted string literal",
				Range:   arguments[1].SourceRange(),
				Suggestions: []string{
					`strftime(_time, "%Y-%m-%dT%H:%M:%S.%Q")`,
				},
			}
		}
		if _, err := spltimeformat.CompileStrftimeFormat(format.Value.Text); err != nil {
			if errors.Is(err, spltimeformat.ErrStrftimeFormatTooLarge) {
				return nil, &Diagnostic{
					Code: "SPL_QUERY_TOO_COMPLEX",
					Message: fmt.Sprintf(
						"strftime format exceeds the %d-byte, %d-work-unit, or %d-output-byte limit",
						spltimeformat.MaximumStrftimeFormatBytes,
						spltimeformat.MaximumStrftimeWorkUnits,
						spltimeformat.MaximumStrftimeOutputBytes,
					),
					Range: format.Range,
				}
			}
			return nil, &Diagnostic{
				Code: "SPL_UNSUPPORTED_TIME_FORMAT",
				Message: "strftime format is outside the supported locale-stable " +
					"date/time variable subset",
				Range: format.Range,
				Suggestions: []string{
					`strftime(_time, "%Y-%m-%dT%H:%M:%S.%Q")`,
				},
			}
		}
	case "strptime":
		function = ScalarFunctionStrptime
		if len(arguments) != 2 {
			return nil, &Diagnostic{
				Code:    "SPL_INVALID_EVAL_ARITY",
				Message: "strptime requires exactly two arguments",
				Range:   name.sourceRange,
			}
		}
		if scalarExpressionMayReturnBooleanFunction(arguments[0]) {
			return nil, &Diagnostic{
				Code:    "SPL_UNSUPPORTED_EVAL_EXPRESSION",
				Message: "strptime cannot consume a Boolean text value",
				Range:   arguments[0].SourceRange(),
				Suggestions: []string{
					`strptime(timestamp, "%Y-%m-%dT%H:%M:%S.%6N")`,
				},
			}
		}
		format, ok := arguments[1].(*ScalarLiteralExpr)
		if !ok || format == nil || format.Value.Kind != LiteralKindString ||
			!format.Value.Quoted {
			return nil, &Diagnostic{
				Code:    "SPL_UNSUPPORTED_EVAL_EXPRESSION",
				Message: "strptime format must be a quoted string literal",
				Range:   arguments[1].SourceRange(),
				Suggestions: []string{
					`strptime(timestamp, "%Y-%m-%dT%H:%M:%S.%6N")`,
				},
			}
		}
		if _, err := spltimeformat.CompileStrptimeFormat(format.Value.Text); err != nil {
			if errors.Is(err, spltimeformat.ErrStrptimeFormatTooLarge) {
				return nil, &Diagnostic{
					Code: "SPL_QUERY_TOO_COMPLEX",
					Message: fmt.Sprintf(
						"strptime format exceeds the %d-byte or %d-work-unit limit",
						spltimeformat.MaximumStrptimeFormatBytes,
						spltimeformat.MaximumStrptimeWorkUnits,
					),
					Range: format.Range,
				}
			}
			return nil, &Diagnostic{
				Code: "SPL_UNSUPPORTED_TIME_FORMAT",
				Message: "strptime format is outside the supported deterministic " +
					"full-date parsing subset",
				Range: format.Range,
				Suggestions: []string{
					`strptime(timestamp, "%Y-%m-%dT%H:%M:%S.%6N")`,
				},
			}
		}
	case "relative_time":
		function = ScalarFunctionRelativeTime
		if len(arguments) != 2 {
			return nil, &Diagnostic{
				Code:    "SPL_INVALID_EVAL_ARITY",
				Message: "relative_time requires exactly two arguments",
				Range:   name.sourceRange,
			}
		}
		if scalarExpressionMayReturnBooleanFunction(arguments[0]) {
			return nil, &Diagnostic{
				Code:    "SPL_UNSUPPORTED_EVAL_EXPRESSION",
				Message: "relative_time cannot consume a Boolean time value",
				Range:   arguments[0].SourceRange(),
				Suggestions: []string{
					`relative_time(_time, "-1d@d")`,
				},
			}
		}
		specifier, ok := arguments[1].(*ScalarLiteralExpr)
		if !ok || specifier == nil ||
			specifier.Value.Kind != LiteralKindString ||
			!specifier.Value.Quoted {
			return nil, &Diagnostic{
				Code:    "SPL_UNSUPPORTED_EVAL_EXPRESSION",
				Message: "relative_time specifier must be a quoted string literal",
				Range:   arguments[1].SourceRange(),
				Suggestions: []string{
					`relative_time(_time, "-1d@d")`,
				},
			}
		}
		if _, err := splrelativetime.CompileSpecifier(
			specifier.Value.Text,
		); err != nil {
			if errors.Is(err, splrelativetime.ErrSpecifierTooLarge) {
				return nil, &Diagnostic{
					Code: "SPL_QUERY_TOO_COMPLEX",
					Message: fmt.Sprintf(
						"relative_time specifier exceeds the %d-byte or %d-work-unit limit",
						splrelativetime.MaximumSpecifierBytes,
						splrelativetime.MaximumSpecifierWorkUnits,
					),
					Range: specifier.Range,
				}
			}
			if errors.Is(err, splrelativetime.ErrMagnitudeOutOfRange) {
				return nil, &Diagnostic{
					Code: "SPL_NUMBER_OUT_OF_RANGE",
					Message: "relative_time magnitude exceeds the supported " +
						searchtimebounds.YearRangeDescription + " timestamp span",
					Range: specifier.Range,
				}
			}
			return nil, &Diagnostic{
				Code: "SPL_UNSUPPORTED_RELATIVE_TIME_SPECIFIER",
				Message: "relative_time specifier is outside the supported " +
					"bounded offset-and-snap subset",
				Range: specifier.Range,
				Suggestions: []string{
					`relative_time(_time, "-1d@d")`,
				},
			}
		}
	case "tonumber":
		function = ScalarFunctionToNumber
		if len(arguments) != 1 {
			return nil, &Diagnostic{Code: "SPL_INVALID_EVAL_ARITY", Message: "tonumber requires exactly one argument in compatibility version 0.1", Range: name.sourceRange}
		}
		if scalarExpressionMayReturnBooleanFunction(arguments[0]) {
			return nil, &Diagnostic{
				Code:    "SPL_UNSUPPORTED_EVAL_EXPRESSION",
				Message: "tonumber cannot consume a Boolean result in search-mode expressions",
				Range:   arguments[0].SourceRange(),
				Suggestions: []string{
					"use isnull or isnotnull directly with where",
					"consume the Boolean with a supported conditional or conversion function",
				},
			}
		}
	case "tostring":
		function = ScalarFunctionToString
		if len(arguments) == 2 {
			return nil, &Diagnostic{
				Code: "SPL_UNSUPPORTED_TOSTRING_FORMAT",
				Message: "tostring formats are outside compatibility version 0.1; " +
					"only tostring(value) is supported",
				Range: arguments[1].SourceRange(),
				Suggestions: []string{
					"tostring(value)",
				},
			}
		}
		if len(arguments) != 1 {
			return nil, &Diagnostic{
				Code:    "SPL_INVALID_EVAL_ARITY",
				Message: "tostring requires exactly one argument in compatibility version 0.1",
				Range:   name.sourceRange,
			}
		}
	case "replace":
		function = ScalarFunctionReplace
		if len(arguments) != 3 {
			return nil, &Diagnostic{Code: "SPL_INVALID_EVAL_ARITY", Message: "replace requires exactly three arguments", Range: name.sourceRange}
		}
		if scalarExpressionMayReturnBooleanFunction(arguments[0]) {
			return nil, &Diagnostic{
				Code:    "SPL_UNSUPPORTED_EVAL_EXPRESSION",
				Message: "replace cannot consume a Boolean result in search-mode expressions",
				Range:   arguments[0].SourceRange(),
				Suggestions: []string{
					"use isnull or isnotnull directly with where",
					"consume the Boolean with a supported conditional or conversion function",
				},
			}
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
	case "isnull":
		function = ScalarFunctionIsNull
		if len(arguments) != 1 {
			return nil, &Diagnostic{
				Code:    "SPL_INVALID_EVAL_ARITY",
				Message: "isnull requires exactly one argument",
				Range:   name.sourceRange,
			}
		}
	case "isnotnull":
		function = ScalarFunctionIsNotNull
		if len(arguments) != 1 {
			return nil, &Diagnostic{
				Code:    "SPL_INVALID_EVAL_ARITY",
				Message: "isnotnull requires exactly one argument",
				Range:   name.sourceRange,
			}
		}
	case "coalesce":
		function = ScalarFunctionCoalesce
		if len(arguments) == 0 {
			return nil, &Diagnostic{
				Code:    "SPL_INVALID_EVAL_ARITY",
				Message: "coalesce requires at least one argument",
				Range:   name.sourceRange,
			}
		}
	case "lower", "upper":
		if functionName == "lower" {
			function = ScalarFunctionLower
		} else {
			function = ScalarFunctionUpper
		}
		if len(arguments) != 1 {
			return nil, &Diagnostic{
				Code:    "SPL_INVALID_EVAL_ARITY",
				Message: functionName + " requires exactly one argument",
				Range:   name.sourceRange,
			}
		}
		if scalarExpressionMayReturnBooleanFunction(arguments[0]) {
			return nil, &Diagnostic{
				Code: "SPL_UNSUPPORTED_EVAL_EXPRESSION",
				Message: functionName +
					" cannot consume a Boolean result in search-mode expressions",
				Range: arguments[0].SourceRange(),
				Suggestions: []string{
					"use isnull or isnotnull directly with where",
					"consume the Boolean with a supported conditional or conversion function",
				},
			}
		}
	case "len", "length":
		function = ScalarFunctionLength
		if len(arguments) != 1 {
			return nil, &Diagnostic{
				Code:    "SPL_INVALID_EVAL_ARITY",
				Message: functionName + " requires exactly one argument",
				Range:   name.sourceRange,
			}
		}
		if scalarExpressionMayReturnBooleanFunction(arguments[0]) {
			return nil, &Diagnostic{
				Code: "SPL_UNSUPPORTED_EVAL_EXPRESSION",
				Message: functionName +
					" cannot consume a Boolean result in search-mode expressions",
				Range: arguments[0].SourceRange(),
				Suggestions: []string{
					"use isnull or isnotnull directly with where",
					"consume the Boolean with a supported conditional or conversion function",
				},
			}
		}
	case "round":
		function = ScalarFunctionRound
		if len(arguments) < 1 || len(arguments) > 2 {
			return nil, &Diagnostic{
				Code:    "SPL_INVALID_EVAL_ARITY",
				Message: "round requires one or two arguments",
				Range:   name.sourceRange,
			}
		}
		if scalarExpressionMayReturnBooleanFunction(arguments[0]) {
			return nil, &Diagnostic{
				Code:    "SPL_UNSUPPORTED_EVAL_EXPRESSION",
				Message: "round cannot consume a Boolean result in search-mode expressions",
				Range:   arguments[0].SourceRange(),
				Suggestions: []string{
					"use isnull or isnotnull directly with where",
					"convert a numeric value before rounding it",
				},
			}
		}
		if len(arguments) == 2 && !SupportedRoundPrecision(arguments[1]) {
			return nil, &Diagnostic{
				Code: "SPL_UNSUPPORTED_ROUND_PRECISION",
				Message: fmt.Sprintf(
					"round precision must be a literal integer from 0 through %d",
					MaximumRoundPrecision,
				),
				Range: arguments[1].SourceRange(),
				Suggestions: []string{
					"round(value)",
					"round(value, 2)",
				},
			}
		}
	case "ceil", "ceiling", "floor":
		if functionName == "floor" {
			function = ScalarFunctionFloor
		} else {
			function = ScalarFunctionCeil
		}
		if len(arguments) != 1 {
			return nil, &Diagnostic{
				Code:    "SPL_INVALID_EVAL_ARITY",
				Message: functionName + " requires exactly one argument",
				Range:   name.sourceRange,
			}
		}
		if scalarExpressionMayReturnBooleanFunction(arguments[0]) {
			return nil, &Diagnostic{
				Code: "SPL_UNSUPPORTED_EVAL_EXPRESSION",
				Message: functionName +
					" cannot consume a Boolean result in search-mode expressions",
				Range: arguments[0].SourceRange(),
				Suggestions: []string{
					"use isnull or isnotnull directly with where",
					"convert a numeric value before rounding it",
				},
			}
		}
	case "mvcount":
		function = ScalarFunctionMVCount
		if len(arguments) != 1 {
			return nil, &Diagnostic{
				Code:    "SPL_INVALID_EVAL_ARITY",
				Message: "mvcount requires exactly one argument",
				Range:   name.sourceRange,
			}
		}
	case "mvsort":
		function = ScalarFunctionMVSort
		if len(arguments) != 1 {
			return nil, &Diagnostic{
				Code:    "SPL_INVALID_EVAL_ARITY",
				Message: "mvsort requires exactly one argument",
				Range:   name.sourceRange,
			}
		}
		if scalarExpressionMayReturnBooleanFunction(arguments[0]) {
			return nil, &Diagnostic{
				Code:    "SPL_UNSUPPORTED_EVAL_EXPRESSION",
				Message: "mvsort cannot consume a Boolean result in search-mode expressions",
				Range:   arguments[0].SourceRange(),
				Suggestions: []string{
					"mvsort(multivalue_field)",
				},
			}
		}
	case "match":
		function = ScalarFunctionMatch
		if len(arguments) != 2 {
			return nil, &Diagnostic{
				Code:    "SPL_INVALID_EVAL_ARITY",
				Message: "match requires exactly two arguments",
				Range:   name.sourceRange,
			}
		}
		if scalarExpressionMayReturnBooleanFunction(arguments[0]) {
			return nil, &Diagnostic{
				Code:    "SPL_UNSUPPORTED_EVAL_EXPRESSION",
				Message: "match cannot consume a Boolean result in search-mode expressions",
				Range:   arguments[0].SourceRange(),
				Suggestions: []string{
					"use the Boolean directly with where",
					`match(value, "pattern")`,
				},
			}
		}
		pattern, ok := arguments[1].(*ScalarLiteralExpr)
		if !ok || pattern == nil || pattern.Value.Kind != LiteralKindString ||
			!pattern.Value.Quoted {
			return nil, &Diagnostic{
				Code:    "SPL_UNSUPPORTED_EVAL_EXPRESSION",
				Message: "match regular expression must be a quoted string literal",
				Range:   arguments[1].SourceRange(),
				Suggestions: []string{
					`match(value, "pattern")`,
				},
			}
		}
		_, err := splregex.CompileMatchPattern(pattern.Value.Text)
		if err != nil {
			if splregex.IsMatchComplexityError(err) {
				return nil, &Diagnostic{
					Code: "SPL_QUERY_TOO_COMPLEX",
					Message: fmt.Sprintf(
						"match regular expression exceeds the %d-byte or %d-work-unit limit",
						splregex.MaximumMatchPatternBytes,
						splregex.MaximumMatchProgramWorkUnits,
					),
					Range: pattern.Range,
				}
			}
			return nil, &Diagnostic{
				Code:    "SPL_UNSUPPORTED_REGEX",
				Message: "match regular expression is outside the supported RE2-compatible subset",
				Range:   pattern.Range,
				Suggestions: []string{
					"use an RE2-compatible regular expression",
				},
			}
		}
	case "like":
		function = ScalarFunctionLike
		if len(arguments) != 2 {
			return nil, &Diagnostic{
				Code:    "SPL_INVALID_EVAL_ARITY",
				Message: "like requires exactly two arguments",
				Range:   name.sourceRange,
			}
		}
		if scalarExpressionMayReturnBooleanFunction(arguments[0]) {
			return nil, &Diagnostic{
				Code:    "SPL_UNSUPPORTED_EVAL_EXPRESSION",
				Message: "like cannot consume a Boolean result in search-mode expressions",
				Range:   arguments[0].SourceRange(),
				Suggestions: []string{
					"use the Boolean directly with where",
					`like(value, "pattern")`,
				},
			}
		}
		pattern, ok := arguments[1].(*ScalarLiteralExpr)
		if !ok || pattern == nil || pattern.Value.Kind != LiteralKindString ||
			!pattern.Value.Quoted {
			return nil, &Diagnostic{
				Code:    "SPL_UNSUPPORTED_EVAL_EXPRESSION",
				Message: "like pattern must be a quoted string literal",
				Range:   arguments[1].SourceRange(),
				Suggestions: []string{
					`like(value, "pattern%")`,
				},
			}
		}
		if _, err := splwildcard.CompileLikePattern(pattern.Value.Text); err != nil {
			if splwildcard.IsLikeComplexityError(err) {
				return nil, &Diagnostic{
					Code: "SPL_QUERY_TOO_COMPLEX",
					Message: fmt.Sprintf(
						"like pattern exceeds the %d-byte or %d-work-unit limit",
						splwildcard.MaximumLikePatternBytes,
						splwildcard.MaximumLikePatternWorkUnits,
					),
					Range: pattern.Range,
				}
			}
			return nil, &Diagnostic{
				Code:    "SPL_UNSUPPORTED_LIKE_PATTERN",
				Message: "like pattern must be valid UTF-8 without NUL bytes or an unpaired terminal backslash",
				Range:   pattern.Range,
				Suggestions: []string{
					`like(value, "prefix%")`,
				},
			}
		}
	case "substr":
		function = ScalarFunctionSubstring
		if len(arguments) < 2 || len(arguments) > 3 {
			return nil, &Diagnostic{
				Code:    "SPL_INVALID_EVAL_ARITY",
				Message: "substr requires two or three arguments",
				Range:   name.sourceRange,
			}
		}
		if scalarExpressionMayReturnBooleanFunction(arguments[0]) {
			return nil, &Diagnostic{
				Code:    "SPL_UNSUPPORTED_EVAL_EXPRESSION",
				Message: "substr cannot consume a Boolean result in search-mode expressions",
				Range:   arguments[0].SourceRange(),
				Suggestions: []string{
					"use isnull or isnotnull directly with where",
					"consume the Boolean with a supported conditional or conversion function",
				},
			}
		}
		for index := 1; index < len(arguments); index++ {
			literal, ok := arguments[index].(*ScalarLiteralExpr)
			if !ok || literal == nil || literal.Value.Kind != LiteralKindInteger {
				return nil, &Diagnostic{
					Code: "SPL_UNSUPPORTED_SUBSTRING_INDEX",
					Message: "substr start and length must be literal integers " +
						"in compatibility version 0.1",
					Range: arguments[index].SourceRange(),
					Suggestions: []string{
						`substr(value, 1)`,
						`substr(value, -3, 2)`,
					},
				}
			}
		}
	default:
		return nil, &Diagnostic{
			Code:    "SPL_UNSUPPORTED_EVAL_FUNCTION",
			Message: fmt.Sprintf("eval function %q is not supported", name.text),
			Range:   name.sourceRange,
			Suggestions: []string{
				"tonumber(value)",
				"tostring(value)",
				`replace(value, "pattern", "replacement")`,
				"isnull(value)",
				"isnotnull(value)",
				"coalesce(value, fallback)",
				"lower(value)",
				"upper(value)",
				"len(value)",
				"substr(value, start, length)",
				"round(value, precision)",
				"ceil(value)",
				"floor(value)",
				"mvcount(value)",
				"mvsort(multivalue_field)",
				`match(value, "pattern")`,
				`like(value, "pattern%")`,
				"now()",
				`strftime(time, "%Y-%m-%dT%H:%M:%S.%Q")`,
				`if(predicate, true_value, false_value)`,
			},
		}
	}
	return &ScalarCallExpr{
		Function:  function,
		Arguments: arguments,
		Range:     Range{Start: name.sourceRange.Start, End: p.previous().sourceRange.End},
	}, nil
}

// SupportedRoundPrecision reports whether an SPL scalar expression is the
// bounded non-negative integer literal accepted by round in compatibility
// version 0.1. Parser and planner trust boundaries share this pure check.
func SupportedRoundPrecision(expression ScalarExpr) bool {
	literal, ok := expression.(*ScalarLiteralExpr)
	if !ok || literal == nil ||
		literal.Value.Kind != LiteralKindInteger {
		return false
	}
	text := strings.TrimPrefix(literal.Value.Text, "+")
	precision, err := strconv.ParseUint(text, 10, 8)
	return err == nil && precision <= MaximumRoundPrecision
}

func scalarExpressionReturnsBoolean(expression ScalarExpr) bool {
	switch expression := expression.(type) {
	case *ScalarCallExpr:
		if expression == nil {
			return false
		}
		if expression.Function.ReturnsBoolean() {
			return true
		}
		if expression.Function == ScalarFunctionCoalesce {
			return coalesceScalarExpressionReturnsBoolean(expression.Arguments)
		}
		return false
	case *ScalarLiteralExpr:
		return expression != nil && expression.Value.Kind == LiteralKindBool
	case *ScalarIfExpr:
		return expression != nil &&
			scalarExpressionReturnsBoolean(expression.True) &&
			scalarExpressionReturnsBoolean(expression.False)
	case *ScalarCaseExpr:
		return expression != nil &&
			caseScalarExpressionReturnsBoolean(expression.Branches)
	default:
		return false
	}
}

func scalarExpressionCanBeDirectPredicate(expression ScalarExpr) bool {
	switch expression := expression.(type) {
	case *ScalarCallExpr:
		if expression == nil {
			return false
		}
		if expression.Function.ReturnsBoolean() {
			return true
		}
		if expression.Function == ScalarFunctionCoalesce {
			return coalesceScalarExpressionReturnsBoolean(expression.Arguments)
		}
		return false
	case *ScalarIfExpr:
		return expression != nil && scalarExpressionReturnsBoolean(expression)
	case *ScalarCaseExpr:
		return expression != nil && scalarExpressionReturnsBoolean(expression)
	default:
		return false
	}
}

// scalarExpressionMayReturnBooleanFunction is consumer-aware: predicates
// nested in an if condition are consumed there, while a Boolean function in a
// result branch can still escape to an eval assignment or non-Boolean
// function. Plain Bool literals retain the compatibility tier's pre-existing
// scalar behavior.
func scalarExpressionMayReturnBooleanFunction(expression ScalarExpr) bool {
	switch expression := expression.(type) {
	case *ScalarCallExpr:
		if expression == nil {
			return false
		}
		if expression.Function.ReturnsBoolean() {
			return true
		}
		if expression.Function == ScalarFunctionCoalesce {
			if slices.ContainsFunc(expression.Arguments, scalarExpressionMayReturnBooleanFunction) {
				return true
			}
		}
		return false
	case *ScalarIfExpr:
		return expression != nil &&
			(scalarExpressionMayReturnBooleanFunction(expression.True) ||
				scalarExpressionMayReturnBooleanFunction(expression.False))
	case *ScalarCaseExpr:
		if expression == nil {
			return false
		}
		for _, branch := range expression.Branches {
			if scalarExpressionMayReturnBooleanFunction(branch.Value) {
				return true
			}
		}
		return false
	default:
		return false
	}
}

// scalarExpressionMayReturnBooleanValue is intentionally stricter than the
// general function-only guard above. Search-mode eval retains its historical
// support for assigning Boolean literals, but concatenation has no implicit
// Boolean spelling; callers must use tostring explicitly.
func scalarExpressionMayReturnBooleanValue(expression ScalarExpr) bool {
	switch expression := expression.(type) {
	case *ScalarLiteralExpr:
		return expression != nil && expression.Value.Kind == LiteralKindBool
	case *ScalarCallExpr:
		if expression == nil {
			return false
		}
		if expression.Function.ReturnsBoolean() {
			return true
		}
		if expression.Function == ScalarFunctionCoalesce {
			if slices.ContainsFunc(expression.Arguments, scalarExpressionMayReturnBooleanValue) {
				return true
			}
		}
		return false
	case *ScalarIfExpr:
		return expression != nil &&
			(scalarExpressionMayReturnBooleanValue(expression.True) ||
				scalarExpressionMayReturnBooleanValue(expression.False))
	case *ScalarCaseExpr:
		if expression == nil {
			return false
		}
		for _, branch := range expression.Branches {
			if scalarExpressionMayReturnBooleanValue(branch.Value) {
				return true
			}
		}
		return false
	default:
		return false
	}
}

func coalesceScalarExpressionReturnsBoolean(arguments []ScalarExpr) bool {
	foundBoolean := false
	for _, argument := range arguments {
		if literal, ok := argument.(*ScalarLiteralExpr); ok &&
			literal != nil &&
			literal.Value.Kind == LiteralKindNull {
			continue
		}
		if !scalarExpressionReturnsBoolean(argument) {
			return false
		}
		foundBoolean = true
	}
	return foundBoolean
}

func caseScalarExpressionReturnsBoolean(branches []ScalarCaseBranch) bool {
	foundBoolean := false
	for _, branch := range branches {
		if literal, ok := branch.Value.(*ScalarLiteralExpr); ok &&
			literal != nil &&
			literal.Value.Kind == LiteralKindNull {
			continue
		}
		if !scalarExpressionReturnsBoolean(branch.Value) {
			return false
		}
		foundBoolean = true
	}
	return foundBoolean
}

func (p *parser) countEvalPredicate(sourceRange Range) error {
	if p.evalPredicates >= maxEvalPredicates {
		return &Diagnostic{
			Code:    "SPL_QUERY_TOO_COMPLEX",
			Message: fmt.Sprintf("search contains more than %d eval/where predicates", maxEvalPredicates),
			Range:   sourceRange,
		}
	}
	p.evalPredicates++
	return nil
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
		start := p.current().sourceRange.Start
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
	if err := p.prepareSearchToken(); err != nil {
		return nil, err
	}
	if p.match(tokenLeftParen) {
		start := p.previous().sourceRange.Start
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
		setExpressionRange(expression, Range{Start: start, End: p.previous().sourceRange.End})
		return expression, nil
	}

	tok := p.current()
	if tok.kind == tokenString {
		p.advance()
		return &TermExpr{Value: tok.text, Quoted: true, Range: tok.sourceRange}, nil
	}
	if (tok.kind != tokenWord && tok.kind != tokenConcat) ||
		p.isKeyword("AND") || p.isKeyword("OR") {
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
			Range: Range{Start: tok.sourceRange.Start, End: literal.Range.End},
		}, nil
	}
	if (strings.EqualFold(tok.text, "IN") && p.current().kind == tokenLeftParen) ||
		(p.isKeyword("IN") && p.nextIs(tokenLeftParen)) ||
		(p.isKeyword("NOT") && p.index+2 < len(p.tokens) &&
			p.tokens[p.index+1].kind == tokenWord && strings.EqualFold(p.tokens[p.index+1].text, "IN") &&
			p.tokens[p.index+2].kind == tokenLeftParen) {
		return nil, &Diagnostic{
			Code:        "SPL_UNSUPPORTED_EXPRESSION",
			Message:     "membership is supported only in eval-language predicate positions, not base search",
			Range:       Range{Start: tok.sourceRange.Start, End: p.current().sourceRange.End},
			Suggestions: []string{"use a where command for exact eval-language membership"},
		}
	}
	return &TermExpr{Value: tok.text, Range: tok.sourceRange}, nil
}

func (p *parser) parseLiteral() (Literal, error) {
	if err := p.prepareSearchToken(); err != nil {
		return Literal{}, err
	}
	tok := p.current()
	if tok.kind != tokenWord && tok.kind != tokenString &&
		tok.kind != tokenConcat {
		return Literal{}, p.errorAtCurrent("SPL_EXPECTED_LITERAL", "expected a comparison value")
	}
	p.advance()
	kind := classifyLiteral(tok.text, tok.quoted)
	return Literal{Kind: kind, Text: tok.text, Quoted: tok.quoted, Range: tok.sourceRange}, nil
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

func evalComparisonOperator(kind tokenKind, profile expressionProfile) (CompareOp, bool) {
	if kind == tokenEqualEqual && profile == expressionProfileV02 {
		return CompareOpEqual, true
	}
	return comparisonOperator(kind)
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
	case *WhereMembershipExpr:
		expression.Range = sourceRange
	case *WhereScalarPredicateExpr:
		expression.Range = sourceRange
	}
}

func setScalarExpressionRange(expression ScalarExpr, sourceRange Range) {
	switch expression := expression.(type) {
	case *ScalarFieldExpr:
		expression.Range = sourceRange
	case *ScalarLiteralExpr:
		expression.Range = sourceRange
		expression.Value.Range = sourceRange
	case *ScalarUnaryExpr:
		expression.Range = sourceRange
	case *ScalarBinaryExpr:
		expression.Range = sourceRange
	case *ScalarCallExpr:
		expression.Range = sourceRange
	case *ScalarIfExpr:
		expression.Range = sourceRange
	case *ScalarCaseExpr:
		expression.Range = sourceRange
	}
}

func (p *parser) canStartSearchOperand() bool {
	tok := p.current()
	if tok.kind == tokenString || tok.kind == tokenLeftParen ||
		tok.kind == tokenConcat || tok.kind == tokenQuotedField ||
		tok.kind == tokenScalarComposite {
		return true
	}
	if tok.kind != tokenWord {
		return false
	}
	return !p.isKeyword("AND") && !p.isKeyword("OR")
}

func (p *parser) canStartWhereOperand() bool {
	tok := p.current()
	if tok.kind == tokenLeftParen || tok.kind == tokenString ||
		tok.kind == tokenQuotedField || tok.kind == tokenScalarComposite ||
		tok.kind == tokenPlus || tok.kind == tokenMinus {
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

func (p *parser) nextIs(kind tokenKind) bool {
	return p.index+1 < len(p.tokens) && p.tokens[p.index+1].kind == kind
}

func (p *parser) previous() token {
	return p.tokens[p.index-1]
}

func (p *parser) errorAtCurrent(code, message string) *Diagnostic {
	return &Diagnostic{Code: code, Message: message, Range: p.current().sourceRange}
}

// prepareSearchToken expands a single-quoted scalar-field token back into the
// exact legacy token stream when it occurs in base search. This keeps quoted
// fields confined to scalar grammar without changing base-search apostrophes,
// whitespace, or punctuation boundaries.
func (p *parser) prepareSearchToken() error {
	tok := p.current()
	if tok.kind != tokenQuotedField && tok.kind != tokenScalarComposite {
		return nil
	}
	legacy, err := lexWithQuotedFields(tok.raw, false)
	if err != nil {
		var diagnostic *Diagnostic
		if errors.As(err, &diagnostic) && diagnostic != nil {
			diagnosticCopy := *diagnostic
			diagnosticCopy.Range = translateFragmentRange(tok.sourceRange.Start, diagnostic.Range)
			return &diagnosticCopy
		}
		return err
	}
	legacy = legacy[:len(legacy)-1] // enclosing stream already owns EOF
	if len(p.tokens)-1+len(legacy)-1 > maxSPLTokens {
		return &Diagnostic{
			Code:    "SPL_QUERY_TOO_COMPLEX",
			Message: fmt.Sprintf("search contains more than %d syntax tokens", maxSPLTokens),
			Range:   tok.sourceRange,
		}
	}
	for index := range legacy {
		legacy[index].sourceRange = translateFragmentRange(tok.sourceRange.Start, legacy[index].sourceRange)
	}
	replacement := make([]token, 0, len(p.tokens)+len(legacy)-1)
	replacement = append(replacement, p.tokens[:p.index]...)
	replacement = append(replacement, legacy...)
	replacement = append(replacement, p.tokens[p.index+1:]...)
	p.tokens = replacement
	return nil
}

func (p *parser) expandLegacyScalarCompositesUntilCommandEnd() error {
	current := p.index
	for index := current; index < len(p.tokens); {
		kind := p.tokens[index].kind
		if kind == tokenEOF || kind == tokenPipe {
			return nil
		}
		if kind != tokenScalarComposite {
			index++
			continue
		}
		p.index = index
		if err := p.prepareSearchToken(); err != nil {
			p.index = current
			return err
		}
		p.index = current
		index++
	}
	p.index = current
	return nil
}

func translateFragmentRange(base Position, relative Range) Range {
	return Range{
		Start: translateFragmentPosition(base, relative.Start),
		End:   translateFragmentPosition(base, relative.End),
	}
}

func translateFragmentPosition(base, relative Position) Position {
	position := Position{
		Offset: base.Offset + relative.Offset,
		Line:   base.Line + relative.Line - 1,
		Column: relative.Column,
	}
	if relative.Line == 1 {
		position.Column = base.Column + relative.Column - 1
	}
	return position
}

// prepareScalarToken reserves arithmetic punctuation only while the v0.2
// scalar grammar is active. The legacy lexer intentionally keeps words whole
// so base-search values, command aliases, sort prefixes, and paths retain
// their v0.1 tokenization.
func (p *parser) prepareScalarToken() error {
	if p.profile != expressionProfileV02 ||
		p.current().kind != tokenWord && p.current().kind != tokenScalarComposite {
		return nil
	}
	if p.preserveSignedLiteral > 0 && signedIntegerLiteralToken(p.current().text) {
		return nil
	}
	preparedQuote, err := p.prepareScalarQuotedOperand()
	if err != nil {
		return err
	}
	if preparedQuote {
		return nil
	}
	parts := splitV02ScalarWord(p.current())
	if len(parts) == 0 {
		return nil
	}
	if len(p.tokens)-1+len(parts)-1 > maxSPLTokens {
		return &Diagnostic{
			Code:    "SPL_QUERY_TOO_COMPLEX",
			Message: fmt.Sprintf("search contains more than %d syntax tokens", maxSPLTokens),
			Range:   p.current().sourceRange,
		}
	}
	replacement := make([]token, 0, len(p.tokens)+len(parts)-1)
	replacement = append(replacement, p.tokens[:p.index]...)
	replacement = append(replacement, parts...)
	replacement = append(replacement, p.tokens[p.index+1:]...)
	p.tokens = replacement
	return nil
}

// prepareScalarQuotedOperand repairs the one context that the legacy lexer
// deliberately cannot recognize globally: a single-quoted scalar operand
// immediately following unspaced arithmetic punctuation. The initial token
// stream retains v0.1 base-search boundaries; only an active v0.2 scalar parse
// uses the original source to replace the overlapping legacy tokens with one
// decoded quoted-field token.
func (p *parser) prepareScalarQuotedOperand() (bool, error) {
	tok := p.current()
	if tok.kind != tokenWord && tok.kind != tokenScalarComposite ||
		p.source == "" || !strings.Contains(tok.text, "'") {
		return false, nil
	}

	segmentStart := 0
	opening := -1
	for offset := 0; offset < len(tok.text); offset++ {
		if tok.text[offset] == '\'' && offset == segmentStart && offset > 0 {
			opening = offset
			break
		}
		if _, operator := scalarOperatorToken(tok.text[offset]); operator {
			segmentStart = offset + 1
		}
	}
	if opening < 0 {
		return false, nil
	}

	openingPosition := advanceSourcePosition(tok.sourceRange.Start, tok.text[:opening])
	quoteLexer := lexer{
		source:       p.source,
		offset:       openingPosition.Offset,
		line:         openingPosition.Line,
		column:       openingPosition.Column,
		quotedFields: true,
	}
	if !quoteLexer.hasClosingSingleQuote() {
		return false, &Diagnostic{
			Code:    "SPL_UNTERMINATED_FIELD_QUOTE",
			Message: "unterminated single-quoted field reference",
			Range:   Range{Start: openingPosition, End: advanceSourcePosition(openingPosition, "'")},
		}
	}
	quoted, scanErr := quoteLexer.scanQuotedField(openingPosition)
	if scanErr != nil {
		return false, scanErr
	}
	quoteEnd := quoted.sourceRange.End.Offset

	consumedEnd := p.index
	for consumedEnd+1 < len(p.tokens) &&
		p.tokens[consumedEnd+1].kind != tokenEOF &&
		p.tokens[consumedEnd+1].sourceRange.Start.Offset < quoteEnd {
		consumedEnd++
	}
	if p.tokens[consumedEnd].sourceRange.End.Offset < quoteEnd {
		return false, &Diagnostic{
			Code:    "SPL_UNTERMINATED_FIELD_QUOTE",
			Message: "unterminated single-quoted field reference",
			Range:   Range{Start: openingPosition, End: advanceSourcePosition(openingPosition, "'")},
		}
	}

	prefix := token{
		kind: tokenWord,
		text: tok.text[:opening],
		sourceRange: Range{
			Start: tok.sourceRange.Start,
			End:   openingPosition,
		},
	}
	parts := splitV02ScalarWord(prefix)
	if len(parts) == 0 && prefix.text != "" {
		parts = append(parts, prefix)
	}
	parts = append(parts, quoted)

	consumedSourceEnd := p.tokens[consumedEnd].sourceRange.End.Offset
	if consumedSourceEnd > quoteEnd {
		suffixStart := quoted.sourceRange.End
		suffixText := p.source[quoteEnd:consumedSourceEnd]
		parts = append(parts, token{
			kind: tokenWord,
			text: suffixText,
			sourceRange: Range{
				Start: suffixStart,
				End:   advanceSourcePosition(suffixStart, suffixText),
			},
		})
	}

	consumed := consumedEnd - p.index + 1
	if len(p.tokens)-1-consumed+len(parts) > maxSPLTokens {
		return false, &Diagnostic{
			Code:    "SPL_QUERY_TOO_COMPLEX",
			Message: fmt.Sprintf("search contains more than %d syntax tokens", maxSPLTokens),
			Range:   tok.sourceRange,
		}
	}
	replacement := make([]token, 0, len(p.tokens)-consumed+len(parts))
	replacement = append(replacement, p.tokens[:p.index]...)
	replacement = append(replacement, parts...)
	replacement = append(replacement, p.tokens[consumedEnd+1:]...)
	p.tokens = replacement
	return true, nil
}

func splitV02ScalarWord(tok token) []token {
	parts, split := appendV02ScalarWord(nil, tok)
	if !split {
		return nil
	}
	return parts
}

func appendV02ScalarWord(parts []token, tok token) ([]token, bool) {
	if tok.kind != tokenWord || !strings.ContainsAny(tok.text, "+-*/%") {
		return parts, false
	}
	start := len(parts)
	segmentStart := 0
	segmentPosition := tok.sourceRange.Start
	position := segmentPosition
	for offset := 0; offset < len(tok.text); {
		value := tok.text[offset]
		r, width := utf8.DecodeRuneInString(tok.text[offset:])
		kind, operator := scalarOperatorToken(value)
		if !operator {
			position = advancePositionByRune(position, r, width)
			offset += width
			continue
		}
		// A sign directly following e/E belongs to a numeric exponent only when
		// the complete current segment is a Float literal. Prefix probing would
		// misclassify field-like spellings such as 1e-foo and 1e--3.
		if (value == '+' || value == '-') &&
			offset > segmentStart &&
			(tok.text[offset-1] == 'e' || tok.text[offset-1] == 'E') {
			segmentEnd := len(tok.text)
			for candidateEnd := offset + 1; candidateEnd < len(tok.text); candidateEnd++ {
				if _, nextOperator := scalarOperatorToken(tok.text[candidateEnd]); nextOperator {
					segmentEnd = candidateEnd
					break
				}
			}
			if classifyLiteral(tok.text[segmentStart:segmentEnd], false) == LiteralKindFloat {
				position = advancePositionByRune(position, r, width)
				offset += width
				continue
			}
		}
		if offset > segmentStart {
			parts = appendScalarWordFragment(
				parts,
				tok.text[segmentStart:offset],
				segmentPosition,
				position,
			)
		}
		operatorEnd := advancePositionByRune(position, r, width)
		parts = append(parts, token{
			kind: kind,
			text: tok.text[offset : offset+width],
			sourceRange: Range{
				Start: position,
				End:   operatorEnd,
			},
		})
		offset += width
		position = operatorEnd
		segmentStart = offset
		segmentPosition = position
	}
	if segmentStart < len(tok.text) {
		parts = appendScalarWordFragment(
			parts,
			tok.text[segmentStart:],
			segmentPosition,
			position,
		)
	}
	if len(parts) == start+1 && parts[start].kind == tokenWord {
		return parts[:start], false
	}
	return parts, true
}

func appendScalarWordFragment(
	parts []token,
	text string,
	start Position,
	end Position,
) []token {
	fragment := token{
		kind:        tokenWord,
		text:        text,
		sourceRange: Range{Start: start, End: end},
	}
	// Arithmetic splitting removes only operator bytes from one legacy word.
	// With no whitespace, quote, or delimiter introduced, the sole fragment
	// whose isolated legacy token kind can differ is ".": both of its dot
	// boundaries become visible and it is concatenation, not a minted field.
	if fragment.text == "." {
		fragment.kind = tokenConcat
	}
	return append(parts, fragment)
}

func signedIntegerLiteralToken(value string) bool {
	return len(value) > 1 && (value[0] == '+' || value[0] == '-') &&
		classifyLiteral(value, false) == LiteralKindInteger
}

func scalarOperatorToken(value byte) (tokenKind, bool) {
	switch value {
	case '+':
		return tokenPlus, true
	case '-':
		return tokenMinus, true
	case '*':
		return tokenMultiply, true
	case '/':
		return tokenDivide, true
	case '%':
		return tokenRemainder, true
	default:
		return tokenInvalid, false
	}
}

func advanceSourcePosition(position Position, value string) Position {
	for len(value) > 0 {
		r, width := utf8.DecodeRuneInString(value)
		if r == utf8.RuneError && width == 1 {
			width = 1
		}
		position.Offset += width
		if r == '\n' {
			position.Line++
			position.Column = 1
		} else {
			position.Column++
		}
		value = value[width:]
	}
	return position
}
