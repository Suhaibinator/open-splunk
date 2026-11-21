package spl

import (
	"fmt"
	"strings"
)

// JoinCommand combines rows on explicit, same-named keys. The right branch
// reads a base search with an explicit fields/table projection.
type JoinCommand struct {
	Left   bool
	Keys   []ExactCommandField
	Search Expr
	Fields []ExactCommandField
	Range  Range
}

func (*JoinCommand) command()             {}
func (*JoinCommand) Name() string         { return "join" }
func (c *JoinCommand) SourceRange() Range { return c.Range }

func (p *parser) parseJoinCommand(name token) (Command, error) {
	command := &JoinCommand{}
	seen := make(map[string]bool)
	for p.nextIs(tokenEqual) {
		optionToken := p.current()
		option := strings.ToLower(optionToken.text)
		if seen[option] {
			return nil, p.errorAtCurrent("SPL_UNSUPPORTED_JOIN_SYNTAX", "duplicate join option "+option)
		}
		seen[option] = true
		p.advance()
		p.advance()
		switch option {
		case "type":
			switch {
			case p.isKeyword("left"), p.isKeyword("outer"):
				command.Left = true
			case p.isKeyword("inner"):
			default:
				return nil, p.errorAtCurrent("SPL_UNSUPPORTED_JOIN_SYNTAX", "join type must be inner, left, or outer")
			}
		case "max":
			if p.current().text != "1" || p.current().kind != tokenWord {
				return nil, p.errorAtCurrent("SPL_UNSUPPORTED_JOIN_SYNTAX", "join currently supports max=1")
			}
		default:
			return nil, p.unsupportedCommandSyntax("join", optionToken, "unsupported join option "+option)
		}
		p.advance()
	}
	keys := make(map[string]struct{})
	for !p.atCommandEnd() && p.current().kind != tokenJoinOpen {
		if err := p.appendExactProjectionField("join", p.current(), &command.Keys, keys); err != nil {
			return nil, err
		}
		p.advance()
	}
	if err := p.requireProjectionFields("join", command.Keys); err != nil {
		return nil, err
	}
	if !p.match(tokenJoinOpen) {
		return nil, p.errorAtCurrent("SPL_UNSUPPORTED_JOIN_SYNTAX", "join requires a bracketed subsearch")
	}
	if p.isKeyword("search") {
		p.advance()
	}
	filter, err := p.parseSearchExpression()
	if err != nil {
		return nil, err
	}
	command.Search = filter
	if !p.match(tokenPipe) || (!p.isKeyword("fields") && !p.isKeyword("table")) {
		return nil, p.errorAtCurrent("SPL_UNSUPPORTED_JOIN_SYNTAX", "end the join subsearch with fields or table and an explicit field list")
	}
	isFields := p.isKeyword("fields")
	p.advance()
	if isFields && p.isKeyword("-") {
		return nil, p.errorAtCurrent("SPL_UNSUPPORTED_JOIN_SYNTAX", "list the subsearch fields to import explicitly")
	}
	if isFields && p.isKeyword("+") {
		p.advance()
	}
	selected := make(map[string]struct{})
	for !p.atCommandEnd() {
		if err := p.appendExactProjectionField("join", p.current(), &command.Fields, selected); err != nil {
			return nil, err
		}
		p.advance()
		if p.match(tokenComma) && p.atCommandEnd() {
			return nil, p.errorAtCurrent("SPL_EXPECTED_FIELD", "expected a field after ','")
		}
	}
	for _, key := range command.Keys {
		if _, ok := selected[key.Name]; !ok {
			return nil, &Diagnostic{Code: "SPL_UNSUPPORTED_JOIN_SYNTAX", Message: fmt.Sprintf("include join key %q in the subsearch projection", key.Name), Range: key.Range}
		}
	}
	if !p.match(tokenJoinClose) {
		return nil, p.errorAtCurrent("SPL_UNSUPPORTED_JOIN_SYNTAX", "expected ']' after the join subsearch projection")
	}
	command.Range = Range{Start: name.sourceRange.Start, End: p.previous().sourceRange.End}
	return command, nil
}
