package spl

import "fmt"

// ParseScalarExpression parses one complete authored eval-language scalar
// expression without wrapping it in a synthetic search. Knowledge-object
// publication uses this entry point so the expression's 16 KiB and token
// budgets are identical to authored SPL rather than being reduced by wrapper
// text.
func ParseScalarExpression(source string) (ScalarExpr, error) {
	if len(source) > maxSPLSourceBytes {
		start := sourcePositionAtOffset(source, maxSPLSourceBytes)
		end := sourcePositionAtOffset(source, maxSPLSourceBytes+1)
		return nil, &Diagnostic{
			Code:    "SPL_QUERY_TOO_COMPLEX",
			Message: fmt.Sprintf("scalar expression source exceeds %d UTF-8 bytes", maxSPLSourceBytes),
			Range:   Range{Start: start, End: end},
		}
	}
	tokens, err := lex(source)
	if err != nil {
		return nil, err
	}
	if len(tokens)-1 > maxSPLTokens { // exclude EOF
		return nil, &Diagnostic{
			Code:    "SPL_QUERY_TOO_COMPLEX",
			Message: fmt.Sprintf("scalar expression contains more than %d syntax tokens", maxSPLTokens),
			Range:   tokens[maxSPLTokens].sourceRange,
		}
	}
	parser := parser{tokens: tokens}
	expression, err := parser.parseScalarExpression()
	if err != nil {
		return nil, err
	}
	if parser.current().kind != tokenEOF {
		return nil, parser.errorAtCurrent(
			"SPL_UNEXPECTED_TOKEN",
			fmt.Sprintf("unexpected token %q", parser.current().text),
		)
	}
	return expression, nil
}
