package spl

import "strconv"

// SupportedMVIndexLiteral reports whether expression is a signed 32-bit
// integer literal accepted by mvindex. Parser and planner trust boundaries
// share this pure check so forged syntax trees cannot widen the index domain.
func SupportedMVIndexLiteral(expression ScalarExpr) bool {
	literal, ok := expression.(*ScalarLiteralExpr)
	if !ok || literal == nil || literal.Value.Kind != LiteralKindInteger {
		return false
	}
	_, err := strconv.ParseInt(literal.Value.Text, 10, 32)
	return err == nil
}
