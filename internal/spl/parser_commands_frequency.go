package spl

import (
	"fmt"
	"strconv"
	"strings"
)

type parsedFrequencyCommand struct {
	fields       []FrequencyField
	by           []FrequencyField
	limit        uint64
	countField   string
	percentField string
	hideCount    bool
	hidePercent  bool
	sourceRange  Range
}

func (p *parser) parseTopCommand(name token) (Command, error) {
	parsed, err := p.parseFrequencyCommand(name, "top")
	if err != nil {
		return nil, err
	}
	return &TopCommand{
		Fields:       parsed.fields,
		By:           parsed.by,
		Limit:        parsed.limit,
		CountField:   parsed.countField,
		PercentField: parsed.percentField,
		HideCount:    parsed.hideCount,
		HidePercent:  parsed.hidePercent,
		Range:        parsed.sourceRange,
	}, nil
}

func (p *parser) parseRareCommand(name token) (Command, error) {
	parsed, err := p.parseFrequencyCommand(name, "rare")
	if err != nil {
		return nil, err
	}
	return &RareCommand{
		Fields:       parsed.fields,
		By:           parsed.by,
		Limit:        parsed.limit,
		CountField:   parsed.countField,
		PercentField: parsed.percentField,
		HideCount:    parsed.hideCount,
		HidePercent:  parsed.hidePercent,
		Range:        parsed.sourceRange,
	}, nil
}

// parseFrequencyCommand accepts Splunk's `top|rare [N] [option=value ...]
// field-list [BY field-list]` shape. Options precede the field list as in the
// documented syntax; `useother` and `otherstr` remain unsupported because the
// lowering has no "OTHER" row to fold the remainder into.
func (p *parser) parseFrequencyCommand(name token, commandName string) (parsedFrequencyCommand, error) {
	command := parsedFrequencyCommand{limit: 10}
	if p.atCommandEnd() {
		return parsedFrequencyCommand{}, p.errorAtCurrent("SPL_EXPECTED_FIELD", commandName+" requires one field")
	}

	hasLimit := false
	if p.current().kind == tokenWord && unsignedIntegerSyntax(p.current().text) {
		limit, err := p.parseFrequencyLimit(p.current(), commandName)
		if err != nil {
			return parsedFrequencyCommand{}, err
		}
		command.limit = limit
		hasLimit = true
		p.advance()
	} else if p.current().kind == tokenWord && strings.HasPrefix(p.current().text, "-") && integerSyntax(p.current().text) {
		return parsedFrequencyCommand{}, p.errorAtCurrent("SPL_INVALID_ARGUMENT", commandName+" limit must be a non-negative integer")
	}
	seenOptions := make(map[string]struct{}, 4)
	for p.current().kind == tokenWord && p.nextIs(tokenEqual) {
		option := p.current()
		lower := strings.ToLower(option.text)
		if _, repeated := seenOptions[lower]; repeated || (lower == "limit" && hasLimit) {
			return parsedFrequencyCommand{}, p.unsupportedFrequencySyntax(
				option, commandName, fmt.Sprintf("%s option %q is repeated", commandName, option.text),
			)
		}
		seenOptions[lower] = struct{}{}
		switch lower {
		case "limit":
			p.advance()
			p.advance()
			if p.current().kind != tokenWord || !unsignedIntegerSyntax(p.current().text) {
				return parsedFrequencyCommand{}, p.errorAtCurrent("SPL_INVALID_ARGUMENT", commandName+" limit must be a non-negative integer")
			}
			limit, err := p.parseFrequencyLimit(p.current(), commandName)
			if err != nil {
				return parsedFrequencyCommand{}, err
			}
			command.limit = limit
			hasLimit = true
			p.advance()
		case "countfield", "percentfield":
			p.advance()
			p.advance()
			value := p.current()
			if (value.kind != tokenWord && value.kind != tokenString) || value.text == "" ||
				strings.Contains(value.text, "*") || strings.TrimSpace(value.text) != value.text {
				if p.atCommandEnd() {
					value = option
				}
				return parsedFrequencyCommand{}, p.unsupportedFrequencySyntax(
					value, commandName, fmt.Sprintf("%s %s requires an exact output field name", commandName, lower),
				)
			}
			if lower == "countfield" {
				command.countField = value.text
			} else {
				command.percentField = value.text
			}
			p.advance()
		case "showcount", "showperc":
			p.advance()
			p.advance()
			value := p.current()
			parsed, ok := parseStrictBool(value.text)
			if value.kind != tokenWord || !ok {
				if p.atCommandEnd() {
					value = option
				}
				return parsedFrequencyCommand{}, p.unsupportedFrequencySyntax(
					value, commandName, fmt.Sprintf("%s %s must be true or false", commandName, lower),
				)
			}
			if lower == "showcount" {
				command.hideCount = !parsed
			} else {
				command.hidePercent = !parsed
			}
			p.advance()
		default:
			return parsedFrequencyCommand{}, p.unsupportedFrequencySyntax(
				option, commandName, fmt.Sprintf("%s option %q is not supported", commandName, option.text),
			)
		}
	}

	if p.atCommandEnd() {
		return parsedFrequencyCommand{}, p.errorAtCurrent("SPL_EXPECTED_FIELD", commandName+" requires one field")
	}
	fields, end, err := p.parseFrequencyFieldList(commandName, name.sourceRange.End, nil)
	if err != nil {
		return parsedFrequencyCommand{}, err
	}
	command.fields = fields
	if p.isKeyword("BY") {
		p.advance()
		if p.atCommandEnd() {
			return parsedFrequencyCommand{}, p.errorAtCurrent("SPL_EXPECTED_FIELD", commandName+" BY requires at least one exact field")
		}
		by, byEnd, byErr := p.parseFrequencyFieldList(commandName, end, fields)
		if byErr != nil {
			return parsedFrequencyCommand{}, byErr
		}
		command.by = by
		end = byEnd
	}
	if !p.atCommandEnd() {
		tok := p.current()
		message := commandName + " fields must be separated by commas"
		if tok.kind == tokenWord && p.nextIs(tokenEqual) {
			message = fmt.Sprintf("%s option %q must precede the field list", commandName, tok.text)
		} else if strings.EqualFold(tok.text, "BY") {
			message = commandName + " accepts a single BY clause"
		}
		return parsedFrequencyCommand{}, p.unsupportedFrequencySyntax(tok, commandName, message)
	}
	command.sourceRange = Range{Start: name.sourceRange.Start, End: end}
	return command, nil
}

func (p *parser) parseFrequencyLimit(limitToken token, commandName string) (uint64, error) {
	limit, err := strconv.ParseUint(limitToken.text, 10, 64)
	if err != nil {
		return 0, &Diagnostic{
			Code:    "SPL_NUMBER_OUT_OF_RANGE",
			Message: commandName + " result count is outside the supported 64-bit range",
			Range:   limitToken.sourceRange,
		}
	}
	return limit, nil
}

// parseFrequencyFieldList reads one comma-separated exact field list for the
// counted fields or the BY clause. taken holds the other clause's fields so a
// name cannot be both counted and grouped.
func (p *parser) parseFrequencyFieldList(
	commandName string,
	end Position,
	taken []FrequencyField,
) ([]FrequencyField, Position, error) {
	fields := make([]FrequencyField, 0, 1)
	wantField := true
	for !p.atCommandEnd() {
		tok := p.current()
		if wantField {
			if tok.kind == tokenComma {
				return nil, end, p.errorAtCurrent(
					"SPL_EXPECTED_FIELD",
					commandName+" requires an exact field after each comma",
				)
			}
			if tok.kind == tokenWord && p.nextIs(tokenEqual) {
				return nil, end, p.unsupportedFrequencySyntax(
					tok,
					commandName,
					fmt.Sprintf("%s option %q must precede the field list", commandName, tok.text),
				)
			}
			if tok.kind != tokenWord {
				return nil, end, p.unsupportedFrequencySyntax(
					tok,
					commandName,
					commandName+" supports unquoted exact fields only",
				)
			}
			if strings.EqualFold(tok.text, "BY") {
				return nil, end, p.errorAtCurrent(
					"SPL_EXPECTED_FIELD",
					commandName+" requires an exact field before BY",
				)
			}
			if strings.Contains(tok.text, "*") {
				return nil, end, p.unsupportedFrequencySyntax(
					tok,
					commandName,
					"wildcard "+commandName+" fields are not supported",
				)
			}
			for _, field := range append(append([]FrequencyField(nil), taken...), fields...) {
				if field.Name == tok.text {
					return nil, end, p.unsupportedFrequencySyntax(
						tok,
						commandName,
						fmt.Sprintf("%s field %q is repeated", commandName, tok.text),
					)
				}
			}
			if len(fields) >= MaximumFrequencyFields {
				return nil, end, &Diagnostic{
					Code:    "SPL_QUERY_TOO_COMPLEX",
					Message: fmt.Sprintf("%s contains more than %d fields", commandName, MaximumFrequencyFields),
					Range:   tok.sourceRange,
				}
			}
			fields = append(fields, FrequencyField{
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
		break
	}
	if len(fields) == 0 || wantField {
		return nil, end, p.errorAtCurrent(
			"SPL_EXPECTED_FIELD",
			commandName+" requires at least one exact field and cannot end with a comma",
		)
	}
	return fields, end, nil
}

func (p *parser) unsupportedFrequencySyntax(tok token, commandName, message string) *Diagnostic {
	return &Diagnostic{
		Code:        "SPL_UNSUPPORTED_" + strings.ToUpper(commandName) + "_SYNTAX",
		Message:     message,
		Range:       tok.sourceRange,
		Suggestions: []string{commandName + " field1, field2", commandName + " limit=20 field1, field2"},
	}
}
