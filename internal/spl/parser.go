package spl

import (
	"fmt"
	"strconv"
	"strings"
)

// Parse parses the supported SPL compatibility tier. Unsupported commands and
// syntax are rejected; a valid prefix is never returned as a partial query.
func Parse(source string) (*Query, error) {
	tokens, err := lex(source)
	if err != nil {
		return nil, err
	}
	p := parser{tokens: tokens}
	return p.parseQuery()
}

type parser struct {
	tokens []token
	index  int
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
	case "fields":
		return p.parseFieldsCommand(nameToken)
	case "table":
		return p.parseTableCommand(nameToken)
	case "sort":
		return p.parseSortCommand(nameToken)
	case "head", "tail":
		return p.parseLimitCommand(name, nameToken)
	case "stats":
		return p.parseStatsCommand(nameToken)
	default:
		return nil, &Diagnostic{
			Code:    "SPL_UNSUPPORTED_COMMAND",
			Message: fmt.Sprintf("unsupported command %q at pipeline stage %d", nameToken.text, stage),
			Range:   nameToken.range_,
		}
	}
}

func (p *parser) parseStatsCommand(name token) (Command, error) {
	if p.atCommandEnd() {
		return nil, p.errorAtCurrent("SPL_EXPECTED_AGGREGATE", "stats requires an aggregate function")
	}
	aggregateToken := p.current()
	if aggregateToken.kind != tokenWord {
		return nil, p.errorAtCurrent("SPL_EXPECTED_AGGREGATE", "stats requires an aggregate function")
	}
	if !strings.EqualFold(aggregateToken.text, "count") {
		return nil, p.unsupportedStatsAggregate(aggregateToken, fmt.Sprintf("stats aggregate %q is not supported; only count is available", aggregateToken.text))
	}
	p.advance()

	aggregate := StatsAggregate{
		Function:   AggregateFunctionCount,
		Alias:      "count",
		Range:      aggregateToken.range_,
		AliasRange: aggregateToken.range_,
	}
	end := aggregateToken.range_.End
	if p.isKeyword("AS") {
		p.advance()
		alias := p.current()
		if alias.kind != tokenWord || p.isKeyword("BY") {
			return nil, p.errorAtCurrent("SPL_EXPECTED_FIELD", "expected an output field name after AS")
		}
		aggregate.Alias = alias.text
		aggregate.AliasRange = alias.range_
		aggregate.Range.End = alias.range_.End
		end = alias.range_.End
		p.advance()
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
	if !p.atCommandEnd() {
		current := p.current()
		if current.kind == tokenLeftParen || current.kind == tokenComma {
			return nil, p.unsupportedStatsAggregate(current, "only the argument-free count aggregate is supported")
		}
		return nil, &Diagnostic{
			Code:        "SPL_UNSUPPORTED_STATS_SYNTAX",
			Message:     fmt.Sprintf("unsupported stats syntax at %q; use AS for an alias and BY for grouping", current.text),
			Range:       current.range_,
			Suggestions: []string{"stats count", "stats count AS total BY field"},
		}
	}
	return &StatsCommand{
		Aggregate: aggregate,
		GroupBy:   groupBy,
		Range:     Range{Start: name.range_.Start, End: end},
	}, nil
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
		Suggestions: []string{"stats count", "stats count AS total BY field"},
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
