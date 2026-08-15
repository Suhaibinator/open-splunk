package spl

import (
	"fmt"
	"strconv"
	"strings"
	"unicode/utf8"
)

const (
	// MaximumV03ProjectionFields bounds the explicit field lists accepted by
	// fillnull and addtotals. Both commands lower to one bounded projection and
	// never perform schema-wide discovery in the v0.3 compatibility profile.
	MaximumV03ProjectionFields = 64

	// MaximumMakeMVDelimiterBytes bounds the decoded UTF-8 delimiter before it
	// reaches either logical planning or generated SQL.
	MaximumMakeMVDelimiterBytes = 1 << 10

	// MaximumMVExpandLimit is both the largest authored per-row limit and the
	// hard per-input-row expansion ceiling. An authored zero still means "no
	// smaller user limit"; it never disables this ceiling.
	MaximumMVExpandLimit uint64 = 1_000
)

// ExactCommandField is one source-located, exact, unquoted command field.
// Version 0.3 deliberately does not extend single-quoted scalar field syntax
// into command grammars.
type ExactCommandField struct {
	Name  string
	Range Range
}

// FillNullCommand replaces missing and explicit-null values in an explicit
// field list. Value is always a String; the default is the String "0".
type FillNullCommand struct {
	Value      string
	ValueRange Range
	Fields     []ExactCommandField
	Range      Range
}

func (*FillNullCommand) command()             {}
func (*FillNullCommand) Name() string         { return "fillnull" }
func (c *FillNullCommand) SourceRange() Range { return c.Range }

// AddTotalsCommand adds one nullable numeric row total over explicit inputs.
// The bounded v0.3 slice always has row=true and col=false, so those effective
// values do not need parallel flags in the AST.
type AddTotalsCommand struct {
	Fields      []ExactCommandField
	Output      string
	OutputRange Range
	Range       Range
}

func (*AddTotalsCommand) command()             {}
func (*AddTotalsCommand) Name() string         { return "addtotals" }
func (c *AddTotalsCommand) SourceRange() Range { return c.Range }

// DeltaCommand subtracts the value Previous rows earlier from the current
// value in established pipeline order.
type DeltaCommand struct {
	Field         string
	FieldRange    Range
	Output        string
	OutputRange   Range
	OutputDefault bool
	Previous      uint64
	PreviousRange Range
	Range         Range
}

func (*DeltaCommand) command()             {}
func (*DeltaCommand) Name() string         { return "delta" }
func (c *DeltaCommand) SourceRange() Range { return c.Range }

// MakeMVCommand splits one scalar String into a typed multivalue String.
type MakeMVCommand struct {
	Field           string
	FieldRange      Range
	Delimiter       string
	DelimiterRange  Range
	AllowEmpty      bool
	AllowEmptyRange Range
	Range           Range
}

func (*MakeMVCommand) command()             {}
func (*MakeMVCommand) Name() string         { return "makemv" }
func (c *MakeMVCommand) SourceRange() Range { return c.Range }

// MVExpandCommand expands one multivalue field while retaining every other
// field. LimitSpecified distinguishes an authored zero from omission for
// inspection and source fidelity; both select the hard ceiling at runtime.
type MVExpandCommand struct {
	Field          string
	FieldRange     Range
	Limit          uint64
	LimitSpecified bool
	LimitRange     Range
	Range          Range
}

func (*MVExpandCommand) command()             {}
func (*MVExpandCommand) Name() string         { return "mvexpand" }
func (c *MVExpandCommand) SourceRange() Range { return c.Range }

func (p *parser) parseFillNullCommand(name token) (Command, error) {
	command := &FillNullCommand{Value: "0", ValueRange: name.sourceRange}
	end := name.sourceRange.End
	valueSeen := false
	fieldsStarted := false
	seenFields := make(map[string]struct{})

	for !p.atCommandEnd() {
		current := p.current()
		if current.kind == tokenWord && strings.EqualFold(current.text, "value") && p.nextIs(tokenEqual) {
			if fieldsStarted {
				return nil, p.unsupportedV03CommandSyntax("fillnull", current, "fillnull value must precede the explicit field list")
			}
			if valueSeen {
				return nil, p.unsupportedV03CommandSyntax("fillnull", current, "fillnull value may be specified only once")
			}
			valueSeen = true
			p.advance()
			p.advance() // '=' established by lookahead.
			value := p.current()
			if value.kind != tokenString || !value.quoted {
				return nil, p.unsupportedV03CommandSyntax("fillnull", value, "fillnull value requires one quoted String literal")
			}
			if !utf8.ValidString(value.text) {
				return nil, p.unsupportedV03CommandSyntax("fillnull", value, "fillnull value must contain valid UTF-8")
			}
			command.Value = value.text
			command.ValueRange = value.sourceRange
			end = value.sourceRange.End
			p.advance()
			continue
		}
		if err := p.appendExactV03ProjectionField("fillnull", current, &command.Fields, seenFields); err != nil {
			return nil, err
		}
		fieldsStarted = true
		end = current.sourceRange.End
		p.advance()
	}
	if err := p.requireV03ProjectionFields("fillnull", command.Fields); err != nil {
		return nil, err
	}
	command.Range = Range{Start: name.sourceRange.Start, End: end}
	return command, nil
}

func (p *parser) parseAddTotalsCommand(name token) (Command, error) {
	command := &AddTotalsCommand{Output: "Total", OutputRange: name.sourceRange}
	end := name.sourceRange.End
	fieldsStarted := false
	seenFields := make(map[string]struct{})
	seenOptions := make(map[string]struct{})

	for !p.atCommandEnd() {
		current := p.current()
		if current.kind == tokenWord && p.nextIs(tokenEqual) {
			option := strings.ToLower(current.text)
			if fieldsStarted {
				return nil, p.unsupportedV03CommandSyntax("addtotals", current, "addtotals options must precede the explicit field list")
			}
			if _, duplicate := seenOptions[option]; duplicate {
				return nil, p.unsupportedV03CommandSyntax("addtotals", current, fmt.Sprintf("addtotals option %q is repeated", current.text))
			}
			seenOptions[option] = struct{}{}
			p.advance()
			p.advance() // '=' established by lookahead.
			value := p.current()
			switch option {
			case "row", "col":
				if value.kind != tokenWord {
					return nil, p.unsupportedV03CommandSyntax("addtotals", value, fmt.Sprintf("addtotals %s requires a Boolean literal", option))
				}
				parsed, ok := parseV03Bool(value.text)
				if !ok || option == "row" && !parsed || option == "col" && parsed {
					required := "true"
					if option == "col" {
						required = "false"
					}
					return nil, p.unsupportedV03CommandSyntax("addtotals", value, fmt.Sprintf("addtotals v0.3 supports only %s=%s", option, required))
				}
			case "fieldname":
				field, fieldErr := exactV03CommandField("addtotals", value)
				if fieldErr != nil {
					return nil, fieldErr
				}
				command.Output = field.Name
				command.OutputRange = field.Range
			default:
				return nil, p.unsupportedV03CommandSyntax("addtotals", current, fmt.Sprintf("addtotals option %q is not supported", current.text))
			}
			end = value.sourceRange.End
			p.advance()
			continue
		}
		if err := p.appendExactV03ProjectionField("addtotals", current, &command.Fields, seenFields); err != nil {
			return nil, err
		}
		fieldsStarted = true
		end = current.sourceRange.End
		p.advance()
	}
	if err := p.requireV03ProjectionFields("addtotals", command.Fields); err != nil {
		return nil, err
	}
	command.Range = Range{Start: name.sourceRange.Start, End: end}
	// A row total of N inputs contains exactly N-1 arithmetic additions. Count
	// those source-determined operations together with eval and delta before the
	// planner/compiler trust boundaries.
	for range len(command.Fields) - 1 {
		if countErr := p.countArithmeticOperator(command.Range); countErr != nil {
			return nil, countErr
		}
	}
	return command, nil
}

func (p *parser) parseDeltaCommand(name token) (Command, error) {
	if p.atCommandEnd() {
		return nil, &Diagnostic{
			Code:    "SPL_EXPECTED_FIELD",
			Message: "delta requires one exact source field",
			Range:   p.current().sourceRange,
		}
	}
	sourceToken := p.current()
	source, err := exactV03CommandField("delta", sourceToken)
	if err != nil {
		return nil, err
	}
	command := &DeltaCommand{
		Field:         source.Name,
		FieldRange:    source.Range,
		Output:        "delta(" + source.Name + ")",
		OutputRange:   source.Range,
		OutputDefault: true,
		Previous:      1,
		PreviousRange: name.sourceRange,
	}
	end := source.Range.End
	p.advance()
	seenAS := false
	seenP := false
	for !p.atCommandEnd() {
		current := p.current()
		if p.isKeyword("AS") {
			if seenAS {
				return nil, p.unsupportedV03CommandSyntax("delta", current, "delta AS may be specified only once")
			}
			seenAS = true
			as := current
			p.advance()
			if p.atCommandEnd() {
				return nil, &Diagnostic{
					Code:    "SPL_EXPECTED_FIELD",
					Message: "delta AS requires one exact unquoted output field",
					Range:   as.sourceRange,
				}
			}
			destinationToken := p.current()
			destination, destinationErr := exactV03CommandField("delta", destinationToken)
			if destinationErr != nil {
				return nil, destinationErr
			}
			command.Output = destination.Name
			command.OutputRange = destination.Range
			command.OutputDefault = false
			end = destination.Range.End
			p.advance()
			continue
		}
		if current.kind == tokenWord && strings.EqualFold(current.text, "p") && p.nextIs(tokenEqual) {
			if seenP {
				return nil, p.unsupportedV03CommandSyntax("delta", current, "delta p may be specified only once")
			}
			seenP = true
			p.advance()
			p.advance()
			value := p.current()
			if value.kind != tokenWord || !unsignedIntegerSyntax(value.text) {
				return nil, p.unsupportedV03CommandSyntax("delta", v03IntegerDiagnosticToken(value), fmt.Sprintf("delta p must be a positive integer from 1 through %d", MaximumStreamStatsWindow))
			}
			previous, parseErr := strconv.ParseUint(value.text, 10, 64)
			if parseErr != nil || previous == 0 {
				return nil, p.unsupportedV03CommandSyntax("delta", value, fmt.Sprintf("delta p must be a positive integer from 1 through %d", MaximumStreamStatsWindow))
			}
			if previous > MaximumStreamStatsWindow {
				return nil, &Diagnostic{
					Code:    "SPL_QUERY_TOO_COMPLEX",
					Message: fmt.Sprintf("delta p exceeds the supported row window of %d", MaximumStreamStatsWindow),
					Range:   value.sourceRange,
				}
			}
			command.Previous = previous
			command.PreviousRange = value.sourceRange
			end = value.sourceRange.End
			p.advance()
			continue
		}
		return nil, p.unsupportedV03CommandSyntax("delta", current, fmt.Sprintf("unsupported delta option or syntax at %q", current.text))
	}
	command.Range = Range{Start: name.sourceRange.Start, End: end}
	if countErr := p.countArithmeticOperator(command.Range); countErr != nil {
		return nil, countErr
	}
	return command, nil
}

func (p *parser) parseMakeMVCommand(name token) (Command, error) {
	command := &MakeMVCommand{
		Delimiter:       " ",
		DelimiterRange:  name.sourceRange,
		AllowEmptyRange: name.sourceRange,
	}
	end := name.sourceRange.End
	seenDelimiter := false
	seenAllowEmpty := false
	fieldSeen := false

	for !p.atCommandEnd() {
		current := p.current()
		if current.kind == tokenWord && p.nextIs(tokenEqual) {
			if fieldSeen {
				return nil, p.unsupportedV03CommandSyntax("makemv", current, "makemv options must precede the field")
			}
			option := strings.ToLower(current.text)
			p.advance()
			p.advance()
			value := p.current()
			switch option {
			case "delim":
				if seenDelimiter {
					return nil, p.unsupportedV03CommandSyntax("makemv", current, "makemv delim may be specified only once")
				}
				seenDelimiter = true
				if value.kind != tokenString || !value.quoted || value.text == "" {
					return nil, p.unsupportedV03CommandSyntax("makemv", value, "makemv delim requires one nonempty quoted String literal")
				}
				if !utf8.ValidString(value.text) {
					return nil, p.unsupportedV03CommandSyntax("makemv", value, "makemv delimiter must contain valid UTF-8")
				}
				if len(value.text) > MaximumMakeMVDelimiterBytes {
					return nil, &Diagnostic{
						Code:    "SPL_QUERY_TOO_COMPLEX",
						Message: fmt.Sprintf("makemv delimiter exceeds %d UTF-8 bytes", MaximumMakeMVDelimiterBytes),
						Range:   value.sourceRange,
					}
				}
				command.Delimiter = value.text
				command.DelimiterRange = value.sourceRange
			case "allowempty":
				if seenAllowEmpty {
					return nil, p.unsupportedV03CommandSyntax("makemv", current, "makemv allowempty may be specified only once")
				}
				seenAllowEmpty = true
				if value.kind != tokenWord {
					return nil, p.unsupportedV03CommandSyntax("makemv", value, "makemv allowempty requires a Boolean literal")
				}
				allowEmpty, ok := parseV03Bool(value.text)
				if !ok {
					return nil, p.unsupportedV03CommandSyntax("makemv", value, "makemv allowempty must be true or false")
				}
				command.AllowEmpty = allowEmpty
				command.AllowEmptyRange = value.sourceRange
			case "tokenizer", "setsv":
				return nil, p.unsupportedV03CommandSyntax("makemv", current, fmt.Sprintf("makemv option %q is deferred", current.text))
			default:
				return nil, p.unsupportedV03CommandSyntax("makemv", current, fmt.Sprintf("makemv option %q is not supported", current.text))
			}
			end = value.sourceRange.End
			p.advance()
			continue
		}
		if fieldSeen {
			return nil, p.unsupportedV03CommandSyntax("makemv", current, "makemv supports exactly one field")
		}
		field, fieldErr := exactV03CommandField("makemv", current)
		if fieldErr != nil {
			return nil, fieldErr
		}
		if isInternalV03MultivalueField(field.Name) {
			return nil, p.unsupportedV03CommandSyntax("makemv", current, "makemv does not support internal fields")
		}
		fieldSeen = true
		command.Field = field.Name
		command.FieldRange = field.Range
		end = field.Range.End
		p.advance()
	}
	if !fieldSeen {
		return nil, &Diagnostic{
			Code:    "SPL_EXPECTED_FIELD",
			Message: "makemv requires exactly one field",
			Range:   p.current().sourceRange,
		}
	}
	command.Range = Range{Start: name.sourceRange.Start, End: end}
	return command, nil
}

func (p *parser) parseMVExpandCommand(name token) (Command, error) {
	if p.atCommandEnd() {
		return nil, &Diagnostic{
			Code:    "SPL_EXPECTED_FIELD",
			Message: "mvexpand requires exactly one field",
			Range:   p.current().sourceRange,
		}
	}
	fieldToken := p.current()
	field, err := exactV03CommandField("mvexpand", fieldToken)
	if err != nil {
		return nil, err
	}
	if isInternalV03MultivalueField(field.Name) {
		return nil, p.unsupportedV03CommandSyntax("mvexpand", fieldToken, "mvexpand does not support internal fields")
	}
	command := &MVExpandCommand{Field: field.Name, FieldRange: field.Range}
	end := field.Range.End
	p.advance()
	if !p.atCommandEnd() {
		option := p.current()
		if option.kind != tokenWord || !strings.EqualFold(option.text, "limit") || !p.nextIs(tokenEqual) {
			return nil, p.unsupportedV03CommandSyntax("mvexpand", option, fmt.Sprintf("unsupported mvexpand option or syntax at %q", option.text))
		}
		p.advance()
		p.advance()
		value := p.current()
		if value.kind != tokenWord || !unsignedIntegerSyntax(value.text) {
			return nil, p.unsupportedV03CommandSyntax("mvexpand", v03IntegerDiagnosticToken(value), fmt.Sprintf("mvexpand limit must be an integer from 0 through %d", MaximumMVExpandLimit))
		}
		limit, parseErr := strconv.ParseUint(value.text, 10, 64)
		if parseErr != nil || limit > MaximumMVExpandLimit {
			return nil, &Diagnostic{
				Code:    "SPL_QUERY_TOO_COMPLEX",
				Message: fmt.Sprintf("mvexpand limit exceeds %d", MaximumMVExpandLimit),
				Range:   value.sourceRange,
			}
		}
		command.Limit = limit
		command.LimitSpecified = true
		command.LimitRange = value.sourceRange
		end = value.sourceRange.End
		p.advance()
	}
	if !p.atCommandEnd() {
		return nil, p.unsupportedV03CommandSyntax("mvexpand", p.current(), "mvexpand limit may be specified only once and must be final")
	}
	command.Range = Range{Start: name.sourceRange.Start, End: end}
	return command, nil
}

// appendExactV03ProjectionField validates one whitespace-separated exact field
// token for the fillnull/addtotals projection lists and appends it, rejecting
// comma separators, duplicates, and lists beyond the bounded field ceiling.
func (p *parser) appendExactV03ProjectionField(
	commandName string,
	current token,
	fields *[]ExactCommandField,
	seen map[string]struct{},
) error {
	if current.kind == tokenComma {
		return p.unsupportedV03CommandSyntax(commandName, current, commandName+" fields are separated by spaces, not commas")
	}
	field, err := exactV03CommandField(commandName, current)
	if err != nil {
		return err
	}
	if _, duplicate := seen[field.Name]; duplicate {
		return p.unsupportedV03CommandSyntax(commandName, current, fmt.Sprintf("%s field %q is repeated", commandName, field.Name))
	}
	if len(*fields) >= MaximumV03ProjectionFields {
		return &Diagnostic{
			Code:    "SPL_QUERY_TOO_COMPLEX",
			Message: fmt.Sprintf("%s contains more than %d fields", commandName, MaximumV03ProjectionFields),
			Range:   current.sourceRange,
		}
	}
	seen[field.Name] = struct{}{}
	*fields = append(*fields, field)
	return nil
}

func (p *parser) requireV03ProjectionFields(commandName string, fields []ExactCommandField) error {
	if len(fields) == 0 {
		return &Diagnostic{
			Code:    "SPL_EXPECTED_FIELD",
			Message: commandName + " requires at least one explicit exact field",
			Range:   p.current().sourceRange,
		}
	}
	return nil
}

func exactV03CommandField(command string, tok token) (ExactCommandField, error) {
	if tok.kind != tokenWord || !IsExactUnquotedFieldName(tok.text) {
		return ExactCommandField{}, &Diagnostic{
			Code:    "SPL_EXPECTED_FIELD",
			Message: command + " requires an exact unquoted field",
			Range:   tok.sourceRange,
		}
	}
	if strings.HasPrefix(strings.ToLower(tok.text), "__os_") {
		return ExactCommandField{}, &Diagnostic{
			Code:    "SPL_RESERVED_FIELD",
			Message: command + " field uses the compiler-private __os_ namespace",
			Range:   tok.sourceRange,
		}
	}
	return ExactCommandField{Name: tok.text, Range: tok.sourceRange}, nil
}

func isInternalV03MultivalueField(name string) bool {
	return strings.HasPrefix(name, "_")
}

// v03IntegerDiagnosticToken keeps a leading sign source-located independently
// from the following digits. The lexer deliberately retains signed scalar
// spellings as one legacy word, while these command options admit only
// unsigned literals and should point at the first offending sign.
func v03IntegerDiagnosticToken(tok token) token {
	if tok.kind == tokenWord && (strings.HasPrefix(tok.text, "-") || strings.HasPrefix(tok.text, "+")) {
		diagnosticToken := tok
		diagnosticToken.sourceRange.End = advancePositionByRune(tok.sourceRange.Start, rune(tok.text[0]), 1)
		return diagnosticToken
	}
	return tok
}

func (p *parser) unsupportedV03CommandSyntax(command string, tok token, message string) *Diagnostic {
	return &Diagnostic{
		Code:    "SPL_UNSUPPORTED_" + strings.ToUpper(command) + "_SYNTAX",
		Message: message,
		Range:   tok.sourceRange,
	}
}
