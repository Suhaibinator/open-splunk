package spl

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/Suhaibinator/open-splunk/internal/splpath"
	"github.com/Suhaibinator/open-splunk/internal/splregex"
)

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
		if err := rejectCompilerPrivateField("regex", first); err != nil {
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
	if err := rejectCompilerPrivateField("accum", field); err != nil {
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
		if err := rejectCompilerPrivateField("accum", output); err != nil {
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
		parsed, ok := parseStrictBool(value.text)
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
			if err := rejectCompilerPrivateField("strcat", current); err != nil {
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

func rejectCompilerPrivateField(commandName string, field token) error {
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
			return nil, unsupportedOption("rex sed mode is not supported")
		case p.isKeyword("offset_field"):
			return nil, unsupportedOption("rex offset_field is not supported")
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
				Message:     "wildcard rename patterns are not supported",
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
				Message:     "wildcard rename patterns are not supported",
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
