package spl

import (
	"fmt"
	"strings"
)

func (p *parser) parseLookupCommand(name token) (Command, error) {
	unsupported := func(tok token, message string) (Command, error) {
		return nil, &Diagnostic{
			Code:    "SPL_UNSUPPORTED_LOOKUP_SYNTAX",
			Message: message,
			Range:   tok.sourceRange,
			Suggestions: []string{
				"lookup service_catalog service_id AS service_key OUTPUT owner AS service_owner",
				"lookup service_catalog service_id AS service_key OUTPUTNEW owner tier",
			},
		}
	}
	if p.atCommandEnd() {
		return unsupported(p.current(), "lookup requires one exact unquoted definition name")
	}

	definition := p.current()
	if definition.kind != tokenWord ||
		!IsExactUnquotedFieldName(definition.text) {
		return unsupported(definition, "lookup definition name must be exact and unquoted")
	}
	if len(definition.text) > MaximumLookupDefinitionNameBytes {
		return nil, &Diagnostic{
			Code: "SPL_QUERY_TOO_COMPLEX",
			Message: fmt.Sprintf(
				"lookup definition name exceeds %d UTF-8 bytes",
				MaximumLookupDefinitionNameBytes,
			),
			Range: definition.sourceRange,
		}
	}
	if err := rejectV03CompilerPrivateField("lookup", definition); err != nil {
		return nil, err
	}
	command := &LookupCommand{
		DefinitionName:  definition.text,
		DefinitionRange: definition.sourceRange,
	}
	p.advance()

	seenLookupKeys := make(map[string]struct{}, MaximumLookupKeys)
	seenEventKeys := make(map[string]struct{}, MaximumLookupKeys)
	for !p.atCommandEnd() && !p.isKeyword("OUTPUT") && !p.isKeyword("OUTPUTNEW") {
		lookupField := p.current()
		if lookupField.kind != tokenWord ||
			!IsExactUnquotedFieldName(lookupField.text) {
			return unsupported(lookupField, "lookup key columns must be exact and unquoted")
		}
		if err := rejectV03CompilerPrivateField("lookup", lookupField); err != nil {
			return nil, err
		}
		if len(command.Keys) >= MaximumLookupKeys {
			return nil, &Diagnostic{
				Code: "SPL_QUERY_TOO_COMPLEX",
				Message: fmt.Sprintf(
					"lookup contains more than %d key mappings",
					MaximumLookupKeys,
				),
				Range: lookupField.sourceRange,
			}
		}
		if _, duplicate := seenLookupKeys[lookupField.text]; duplicate {
			return unsupported(
				lookupField,
				fmt.Sprintf("lookup key column %q is repeated", lookupField.text),
			)
		}
		p.advance()
		if !p.isKeyword("AS") {
			return nil, &Diagnostic{
				Code:    "SPL_EXPECTED_AS",
				Message: "lookup key column must be followed by AS and an event field",
				Range:   p.current().sourceRange,
				Suggestions: []string{
					"lookup service_catalog service_id AS service_key OUTPUT owner",
				},
			}
		}
		p.advance()
		if p.atCommandEnd() || p.isKeyword("OUTPUT") || p.isKeyword("OUTPUTNEW") {
			return unsupported(p.current(), "lookup key AS requires one exact unquoted event field")
		}
		eventField := p.current()
		if eventField.kind != tokenWord ||
			!IsExactUnquotedFieldName(eventField.text) {
			return unsupported(eventField, "lookup key event fields must be exact and unquoted")
		}
		if err := rejectV03CompilerPrivateField("lookup", eventField); err != nil {
			return nil, err
		}
		if _, duplicate := seenEventKeys[eventField.text]; duplicate {
			return unsupported(
				eventField,
				fmt.Sprintf("lookup event key field %q is repeated", eventField.text),
			)
		}
		seenLookupKeys[lookupField.text] = struct{}{}
		seenEventKeys[eventField.text] = struct{}{}
		command.Keys = append(command.Keys, LookupKeyMapping{
			LookupField:      lookupField.text,
			LookupFieldRange: lookupField.sourceRange,
			EventField:       eventField.text,
			EventFieldRange:  eventField.sourceRange,
			Range: Range{
				Start: lookupField.sourceRange.Start,
				End:   eventField.sourceRange.End,
			},
		})
		p.advance()
	}

	if len(command.Keys) == 0 {
		return unsupported(p.current(), "lookup requires between one and four key mappings")
	}
	if p.atCommandEnd() {
		return unsupported(p.current(), "lookup requires explicit OUTPUT or OUTPUTNEW fields")
	}

	mode := p.current()
	switch strings.ToUpper(mode.text) {
	case "OUTPUT":
		command.OutputMode = LookupOutputModeOverwrite
	case "OUTPUTNEW":
		command.OutputMode = LookupOutputModePreserveExisting
	default:
		return unsupported(mode, "lookup requires explicit OUTPUT or OUTPUTNEW fields")
	}
	command.OutputModeRange = mode.sourceRange
	p.advance()
	if p.atCommandEnd() {
		return unsupported(p.current(), mode.text+" requires at least one lookup output column")
	}

	seenLookupOutputs := make(map[string]struct{}, MaximumLookupOutputs)
	seenEventOutputs := make(map[string]struct{}, MaximumLookupOutputs)
	end := mode.sourceRange.End
	for !p.atCommandEnd() {
		lookupField := p.current()
		if lookupField.kind != tokenWord ||
			!IsExactUnquotedFieldName(lookupField.text) {
			return unsupported(lookupField, "lookup output columns must be exact and unquoted")
		}
		if err := rejectV03CompilerPrivateField("lookup", lookupField); err != nil {
			return nil, err
		}
		if len(command.Outputs) >= MaximumLookupOutputs {
			return nil, &Diagnostic{
				Code: "SPL_QUERY_TOO_COMPLEX",
				Message: fmt.Sprintf(
					"lookup contains more than %d output mappings",
					MaximumLookupOutputs,
				),
				Range: lookupField.sourceRange,
			}
		}
		if _, duplicate := seenLookupOutputs[lookupField.text]; duplicate {
			return unsupported(
				lookupField,
				fmt.Sprintf("lookup output column %q is repeated", lookupField.text),
			)
		}
		mapping := LookupOutputMapping{
			LookupField:      lookupField.text,
			LookupFieldRange: lookupField.sourceRange,
			EventField:       lookupField.text,
			EventFieldRange:  lookupField.sourceRange,
			Range:            lookupField.sourceRange,
		}
		p.advance()
		if p.isKeyword("AS") {
			p.advance()
			if p.atCommandEnd() {
				return unsupported(p.current(), "lookup output AS requires one exact unquoted event field")
			}
			eventField := p.current()
			if eventField.kind != tokenWord ||
				!IsExactUnquotedFieldName(eventField.text) {
				return unsupported(eventField, "lookup output event fields must be exact and unquoted")
			}
			mapping.EventField = eventField.text
			mapping.EventFieldRange = eventField.sourceRange
			mapping.Range.End = eventField.sourceRange.End
			p.advance()
		}
		if err := rejectV03CompilerPrivateField("lookup", token{
			kind:        tokenWord,
			text:        mapping.EventField,
			sourceRange: mapping.EventFieldRange,
		}); err != nil {
			return nil, err
		}
		if _, duplicate := seenEventOutputs[mapping.EventField]; duplicate {
			return unsupported(
				token{text: mapping.EventField, sourceRange: mapping.EventFieldRange},
				fmt.Sprintf("lookup event output field %q is repeated", mapping.EventField),
			)
		}
		seenLookupOutputs[mapping.LookupField] = struct{}{}
		seenEventOutputs[mapping.EventField] = struct{}{}
		command.Outputs = append(command.Outputs, mapping)
		end = mapping.Range.End
	}

	command.Range = Range{Start: name.sourceRange.Start, End: end}
	return command, nil
}
