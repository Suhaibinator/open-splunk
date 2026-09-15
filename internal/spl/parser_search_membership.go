package spl

import "fmt"

// Search membership uses search comparisons, including their wildcard and
// missing-field behavior, rather than eval-language membership semantics.
func (p *parser) parseSearchMembership(field token) (Expr, error) {
	p.advance() // IN
	p.advance() // (
	var expression Expr
	for count := 1; ; count++ {
		candidate, err := p.parseLiteral()
		if err != nil {
			return nil, err
		}
		sourceRange := Range{Start: field.sourceRange.Start, End: candidate.Range.End}
		if count > MaximumMembershipCandidates {
			return nil, &Diagnostic{
				Code:    "SPL_QUERY_TOO_COMPLEX",
				Message: fmt.Sprintf("membership contains more than %d candidates", MaximumMembershipCandidates),
				Range:   sourceRange,
			}
		}
		comparison := &ComparisonExpr{Field: field.text, Op: CompareOpEqual, Value: candidate, Range: sourceRange}
		if expression == nil {
			expression = comparison
		} else {
			expression = &BinaryExpr{Op: BoolOpOr, Left: expression, Right: comparison, Range: sourceRange}
		}
		if p.match(tokenRightParen) {
			sourceRange.End = p.previous().sourceRange.End
			if err := p.chargeMembershipCandidates(count, sourceRange); err != nil {
				return nil, err
			}
			setExpressionRange(expression, sourceRange)
			return expression, nil
		}
		if !p.match(tokenComma) {
			return nil, p.membershipSyntaxError(p.current().sourceRange, "expected ',' or ')' after search membership value")
		}
	}
}
