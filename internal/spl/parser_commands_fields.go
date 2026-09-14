package spl

import (
	"fmt"
	"strconv"
	"strings"
)

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
		if p.profile == expressionProfileAuthored {
			if field.scalarDiagnostic != nil {
				return nil, field.scalarDiagnostic
			}
			if field.kind == tokenWord && strings.HasPrefix(field.text, "'") {
				return nil, unterminatedQuotedScalarField(field)
			}
		}
		quotedDestination := p.profile == expressionProfileAuthored && field.kind == tokenQuotedField
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
	if p.current().kind == tokenWord && (p.current().text == "-" || p.current().text == "+") {
		exclude = p.current().text == "-"
		p.advance()
	}
	fields, quoted, wildcards, ranges, end, err := p.parseFieldsFieldList()
	if err != nil {
		return nil, err
	}
	return &FieldsCommand{
		Fields:         fields,
		QuotedFields:   quoted,
		WildcardFields: wildcards,
		FieldRanges:    ranges,
		Exclude:        exclude,
		Range:          Range{Start: name.sourceRange.Start, End: end},
	}, nil
}

func (p *parser) parseFieldsFieldList() ([]string, []bool, []bool, []Range, Position, error) {
	fields := make([]string, 0, 8)
	quoted := make([]bool, 0, 8)
	wildcards := make([]bool, 0, 8)
	ranges := make([]Range, 0, 8)
	end := p.current().sourceRange.Start
	wantField := true
	for !p.atCommandEnd() {
		tok := p.current()
		if tok.kind == tokenComma {
			if wantField {
				return nil, nil, nil, nil, end, p.errorAtCurrent("SPL_EXPECTED_FIELD", "expected a field name")
			}
			wantField = true
			p.advance()
			continue
		}
		isQuoted := tok.kind == tokenQuotedField
		if tok.kind != tokenWord && !isQuoted {
			return nil, nil, nil, nil, end, p.errorAtCurrent("SPL_EXPECTED_FIELD", "expected a field name")
		}
		if tok.scalarDiagnostic != nil {
			return nil, nil, nil, nil, end, tok.scalarDiagnostic
		}
		wildcard := IsFieldsFieldGlob(tok.text)
		if strings.Contains(tok.text, "*") && !wildcard {
			return nil, nil, nil, nil, end, &Diagnostic{
				Code:    "SPL_UNSUPPORTED_FIELD_PATTERN",
				Message: "fields wildcard must be a valid field pattern using '*' as its only metacharacter",
				Range:   tok.sourceRange,
			}
		}
		if isQuoted && !wildcard {
			if err := validateQuotedFieldReference(tok); err != nil {
				return nil, nil, nil, nil, end, err
			}
		}
		if len(fields) >= MaximumExplicitProjectionFields {
			return nil, nil, nil, nil, end, &Diagnostic{
				Code:    "SPL_QUERY_TOO_COMPLEX",
				Message: fmt.Sprintf("fields contains more than %d selectors", MaximumExplicitProjectionFields),
				Range:   tok.sourceRange,
			}
		}
		fields = append(fields, tok.text)
		quoted = append(quoted, isQuoted)
		wildcards = append(wildcards, wildcard)
		ranges = append(ranges, tok.sourceRange)
		end = tok.sourceRange.End
		wantField = false
		p.advance()
	}
	if len(fields) == 0 || wantField {
		return nil, nil, nil, nil, end, p.errorAtCurrent("SPL_EXPECTED_FIELD", "expected at least one field name")
	}
	return fields, quoted, wildcards, ranges, end, nil
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

func (p *parser) parseSortCommand(name token) (Command, error) {
	command := &SortCommand{}
	if p.atCommandEnd() {
		return nil, p.errorAtCurrent("SPL_EXPECTED_FIELD", "sort requires at least one field")
	}
	if p.current().kind == tokenWord && unsignedIntegerSyntax(p.current().text) {
		if err := p.parseSortLimit(command, p.current()); err != nil {
			return nil, err
		}
		p.advance()
	} else if p.isKeyword("LIMIT") && p.nextIs(tokenEqual) {
		option := p.current()
		p.advance()
		p.advance()
		value := p.current()
		if value.kind != tokenWord || !unsignedIntegerSyntax(value.text) {
			if p.atCommandEnd() {
				value = option
			}
			return nil, &Diagnostic{
				Code:    "SPL_INVALID_ARGUMENT",
				Message: "sort limit must be a non-negative base-10 integer",
				Range:   value.sourceRange,
			}
		}
		if err := p.parseSortLimit(command, value); err != nil {
			return nil, err
		}
		p.advance()
	}
	if p.current().kind == tokenWord && strings.EqualFold(p.current().text, "desc") {
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
		field, err := p.parseSortKey()
		if err != nil {
			return nil, err
		}
		command.Fields = append(command.Fields, field)
		end = field.Range.End
		lastWasComma = false

		if !p.atCommandEnd() && p.current().kind == tokenWord &&
			(strings.EqualFold(p.current().text, "d") || strings.EqualFold(p.current().text, "desc")) &&
			p.index+1 < len(p.tokens) && (p.tokens[p.index+1].kind == tokenPipe || p.tokens[p.index+1].kind == tokenEOF) {
			for index := range command.Fields {
				command.Fields[index].Descending = !command.Fields[index].Descending
			}
			end = p.current().sourceRange.End
			p.advance()
			break
		}
	}
	if len(command.Fields) == 0 || lastWasComma {
		return nil, p.errorAtCurrent("SPL_EXPECTED_FIELD", "sort requires at least one field")
	}
	command.Range = Range{Start: name.sourceRange.Start, End: end}
	return command, nil
}

func (p *parser) parseSortLimit(command *SortCommand, value token) error {
	limit, err := strconv.ParseUint(value.text, 10, 64)
	if err != nil {
		return &Diagnostic{
			Code:    "SPL_NUMBER_OUT_OF_RANGE",
			Message: "sort result count is outside the supported 64-bit range",
			Range:   value.sourceRange,
		}
	}
	command.Limit = limit
	command.LimitSpecified = true
	return nil
}

// parseSortKey consumes one [+|-]field or [+|-]mode(field) sort key. It is
// shared by sort and dedup sortby; the caller owns separators and terminators.
func (p *parser) parseSortKey() (SortField, error) {
	if p.current().kind == tokenScalarComposite {
		prepared, err := p.prepareScalarQuotedOperand()
		if err != nil {
			return SortField{}, err
		}
		if !prepared {
			return SortField{}, p.errorAtCurrent("SPL_EXPECTED_FIELD", "expected a sort field")
		}
	}
	tok := p.current()
	keyStart := tok.sourceRange.Start
	descending := false
	if direction, ok := sortDirectionPrefix(tok); ok {
		descending = direction
		prefix := tok
		p.advance()
		if p.atCommandEnd() || p.current().kind == tokenComma || sortTokenStartsDirection(p.current()) {
			return SortField{}, &Diagnostic{
				Code:    "SPL_EXPECTED_FIELD",
				Message: "expected a sort field after direction prefix",
				Range:   prefix.sourceRange,
			}
		}
		tok = p.current()
	} else if tok.kind == tokenWord && len(tok.text) > 1 && (tok.text[0] == '+' || tok.text[0] == '-') {
		descending = tok.text[0] == '-'
		tok.text = tok.text[1:]
		tok.sourceRange.Start = advanceSourcePosition(tok.sourceRange.Start, p.current().text[:1])
	}
	if tok.text == "" || sortTokenStartsDirection(tok) {
		return SortField{}, p.errorAtCurrent("SPL_EXPECTED_FIELD", "expected a sort field after direction prefix")
	}

	field, keyEnd, err := p.parseSortFieldValue(tok)
	if err != nil {
		return SortField{}, err
	}
	field.Descending = descending
	field.Range = Range{Start: keyStart, End: keyEnd}
	return field, nil
}

// parseSortFieldValue consumes the field portion of one sort key. tok is a
// detached copy because an attached direction such as -num(bytes) is stripped
// without changing the lexer token or its source range.
func (p *parser) parseSortFieldValue(tok token) (SortField, Position, error) {
	result := SortField{Mode: SortValueModeAuto}
	if tok.kind == tokenQuotedField {
		if tok.scalarDiagnostic != nil {
			return SortField{}, tok.sourceRange.End, tok.scalarDiagnostic
		}
		if err := validateQuotedFieldReference(tok); err != nil {
			return SortField{}, tok.sourceRange.End, err
		}
		result.Field = tok.text
		result.Quoted = true
		result.FieldRange = tok.sourceRange
		p.advance()
		return result, tok.sourceRange.End, nil
	}
	if tok.kind != tokenWord {
		return SortField{}, tok.sourceRange.End, p.errorAtCurrent("SPL_EXPECTED_FIELD", "expected a sort field")
	}

	mode, typed := sortValueMode(tok.text)
	if !typed || !p.nextIs(tokenLeftParen) {
		if p.nextIs(tokenLeftParen) {
			return SortField{}, tok.sourceRange.End, &Diagnostic{
				Code:        "SPL_UNSUPPORTED_SORT_SYNTAX",
				Message:     fmt.Sprintf("sort value mode %q is not supported", tok.text),
				Range:       tok.sourceRange,
				Suggestions: []string{"auto(field)", "str(field)", "num(field)", "ip(field)"},
			}
		}
		result.Field = tok.text
		result.FieldRange = tok.sourceRange
		p.advance()
		return result, tok.sourceRange.End, nil
	}

	// p.current() still owns any attached direction spelling and is consumed
	// here before the lexer-produced left parenthesis.
	result.Mode = mode
	p.advance()
	p.advance()
	field := p.current()
	if field.kind == tokenScalarComposite {
		prepared, err := p.prepareScalarQuotedOperand()
		if err != nil {
			return SortField{}, field.sourceRange.End, err
		}
		if prepared {
			field = p.current()
		}
	}
	result.Quoted = field.kind == tokenQuotedField
	if result.Quoted {
		if field.scalarDiagnostic != nil {
			return SortField{}, field.sourceRange.End, field.scalarDiagnostic
		}
		if err := validateQuotedFieldReference(field); err != nil {
			return SortField{}, field.sourceRange.End, err
		}
	}
	if field.kind != tokenWord && !result.Quoted {
		return SortField{}, field.sourceRange.End, p.errorAtCurrent("SPL_EXPECTED_FIELD", "sort value mode requires one field")
	}
	result.Field = field.text
	result.FieldRange = field.sourceRange
	p.advance()
	if !p.match(tokenRightParen) {
		return SortField{}, field.sourceRange.End, p.errorAtCurrent("SPL_EXPECTED_RIGHT_PAREN", "expected ')' after typed sort field")
	}
	return result, p.previous().sourceRange.End, nil
}

func sortDirectionPrefix(tok token) (descending bool, ok bool) {
	if tok.kind != tokenWord && tok.kind != tokenPlus && tok.kind != tokenMinus {
		return false, false
	}
	switch tok.text {
	case "+":
		return false, true
	case "-":
		return true, true
	default:
		return false, false
	}
}

func sortTokenStartsDirection(tok token) bool {
	if _, ok := sortDirectionPrefix(tok); ok {
		return true
	}
	return tok.kind == tokenWord && len(tok.text) > 0 && (tok.text[0] == '+' || tok.text[0] == '-')
}

func sortValueMode(name string) (SortValueMode, bool) {
	switch strings.ToLower(name) {
	case "auto":
		return SortValueModeAuto, true
	case "str":
		return SortValueModeString, true
	case "num":
		return SortValueModeNumber, true
	case "ip":
		return SortValueModeIP, true
	default:
		return SortValueModeAuto, false
	}
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
	consecutiveSpecified := false
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
		if lower == "sortby" {
			if len(command.Fields) == 0 {
				return unsupported(tok, "dedup requires at least one exact field before sortby")
			}
			if wantField {
				return unsupported(tok, "dedup requires an exact field before each comma")
			}
			p.advance()
			sortEnd, err := p.parseDedupSortBy(command, tok)
			if err != nil {
				return nil, err
			}
			end = sortEnd
			break
		}
		if p.nextIs(tokenEqual) {
			switch lower {
			case "consecutive":
				if consecutiveSpecified {
					return unsupported(tok, "dedup option \"consecutive\" is repeated")
				}
				p.advance()
				p.advance()
				value := p.current()
				parsed, ok := parseStrictBool(value.text)
				if value.kind != tokenWord || !ok {
					if p.atCommandEnd() {
						value = tok
					}
					return unsupported(value, "dedup consecutive must be true or false")
				}
				command.Consecutive = parsed
				consecutiveSpecified = true
				end = value.sourceRange.End
				p.advance()
				continue
			case "keepempty", "keepevents":
				return unsupported(tok, fmt.Sprintf("dedup option %q is not supported", tok.text))
			default:
				return unsupported(tok, fmt.Sprintf("dedup option %q is not recognized", tok.text))
			}
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

// parseDedupSortBy consumes the sort keys that follow the sortby keyword. The
// clause is terminal: it runs to the end of the command, so the official
// space-separated spelling and the comma-separated sort spelling both work.
func (p *parser) parseDedupSortBy(command *DedupCommand, keyword token) (Position, error) {
	unsupported := func(tok token, message string) (Position, error) {
		return Position{}, &Diagnostic{
			Code:        "SPL_UNSUPPORTED_DEDUP_SYNTAX",
			Message:     message,
			Range:       tok.sourceRange,
			Suggestions: []string{"dedup field sortby -_time", "dedup field sortby +num(bytes), -host"},
		}
	}
	if p.atCommandEnd() {
		return unsupported(keyword, "dedup sortby requires at least one sort key")
	}
	seen := make(map[string]struct{})
	lastWasComma := false
	end := keyword.sourceRange.End
	for !p.atCommandEnd() {
		if p.current().kind == tokenComma {
			if len(command.SortBy) == 0 || lastWasComma {
				return unsupported(p.current(), "dedup sortby requires a sort key before each comma")
			}
			lastWasComma = true
			p.advance()
			continue
		}
		if p.current().kind == tokenWord && p.nextIs(tokenEqual) {
			return unsupported(p.current(), "dedup options must precede sortby")
		}
		if p.current().kind == tokenWord &&
			(strings.EqualFold(p.current().text, "d") || strings.EqualFold(p.current().text, "desc")) &&
			p.index+1 < len(p.tokens) && (p.tokens[p.index+1].kind == tokenPipe || p.tokens[p.index+1].kind == tokenEOF) {
			return unsupported(p.current(), "dedup sortby uses a + or - prefix on each key instead of a trailing desc")
		}
		key, err := p.parseSortKey()
		if err != nil {
			return Position{}, err
		}
		keyToken := token{sourceRange: key.Range}
		if _, duplicate := seen[key.Field]; duplicate {
			return unsupported(keyToken, fmt.Sprintf("dedup sortby key %q is duplicated", key.Field))
		}
		if len(command.SortBy) >= maxDedupFields {
			return unsupported(keyToken, fmt.Sprintf("dedup sortby supports at most %d keys", maxDedupFields))
		}
		seen[key.Field] = struct{}{}
		command.SortBy = append(command.SortBy, key)
		end = key.Range.End
		lastWasComma = false
	}
	if lastWasComma {
		return unsupported(p.previous(), "dedup sortby cannot end with a comma")
	}
	command.SortByRange = Range{Start: command.SortBy[0].Range.Start, End: end}
	return end, nil
}

// parseLimitCommand accepts the positional count form and the labeled
// limit=N form; both are the same bounded row selection. The predicate
// form and the keeplast/null options remain unsupported because they need
// a per-row eval guard rather than a fixed count.
func (p *parser) parseLimitCommand(name string, nameToken token) (Command, error) {
	count := uint64(10)
	end := nameToken.sourceRange.End
	if !p.atCommandEnd() {
		tok := p.current()
		if tok.kind != tokenWord {
			return nil, p.errorAtCurrent("SPL_INVALID_ARGUMENT", fmt.Sprintf("%s count must be a positive integer", name))
		}
		if p.nextIs(tokenEqual) {
			if !strings.EqualFold(tok.text, "limit") {
				return nil, p.errorAtCurrent("SPL_UNSUPPORTED_ARGUMENT", fmt.Sprintf("unsupported %s argument %q", name, tok.text))
			}
			p.advance()
			p.advance()
			tok = p.current()
			if tok.kind != tokenWord {
				return nil, p.errorAtCurrent("SPL_INVALID_ARGUMENT", fmt.Sprintf("%s limit must be a positive integer", name))
			}
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
