package spl

import (
	"fmt"
	"slices"
	"strconv"
	"strings"
)

// SupportedRoundPrecision reports whether an SPL scalar expression is the
// bounded non-negative integer literal accepted by round. Parser and planner
// trust boundaries share this pure check.
func SupportedRoundPrecision(expression ScalarExpr) bool {
	literal, ok := expression.(*ScalarLiteralExpr)
	if !ok || literal == nil ||
		literal.Value.Kind != LiteralKindInteger {
		return false
	}
	text := strings.TrimPrefix(literal.Value.Text, "+")
	precision, err := strconv.ParseUint(text, 10, 8)
	return err == nil && precision <= MaximumRoundPrecision
}

func scalarExpressionReturnsBoolean(expression ScalarExpr) bool {
	switch expression := expression.(type) {
	case *ScalarCallExpr:
		if expression == nil {
			return false
		}
		if expression.Function.ReturnsBoolean() {
			return true
		}
		if expression.Function == ScalarFunctionCoalesce {
			return coalesceScalarExpressionReturnsBoolean(expression.Arguments)
		}
		return false
	case *ScalarLiteralExpr:
		return expression != nil && expression.Value.Kind == LiteralKindBool
	case *ScalarIfExpr:
		return expression != nil &&
			scalarExpressionReturnsBoolean(expression.True) &&
			scalarExpressionReturnsBoolean(expression.False)
	case *ScalarCaseExpr:
		return expression != nil &&
			caseScalarExpressionReturnsBoolean(expression.Branches)
	default:
		return false
	}
}

func scalarExpressionCanBeDirectPredicate(expression ScalarExpr) bool {
	switch expression := expression.(type) {
	case *ScalarCallExpr:
		if expression == nil {
			return false
		}
		if expression.Function.ReturnsBoolean() {
			return true
		}
		if expression.Function == ScalarFunctionCoalesce {
			return coalesceScalarExpressionReturnsBoolean(expression.Arguments)
		}
		return false
	case *ScalarIfExpr:
		return expression != nil && scalarExpressionReturnsBoolean(expression)
	case *ScalarCaseExpr:
		return expression != nil && scalarExpressionReturnsBoolean(expression)
	default:
		return false
	}
}

// scalarExpressionMayReturnBooleanFunction is consumer-aware: predicates
// nested in an if condition are consumed there, while a Boolean function in a
// result branch can still escape to an eval assignment or non-Boolean
// function. Plain Bool literals retain their established scalar behavior.
func scalarExpressionMayReturnBooleanFunction(expression ScalarExpr) bool {
	switch expression := expression.(type) {
	case *ScalarCallExpr:
		if expression == nil {
			return false
		}
		if expression.Function.ReturnsBoolean() {
			return true
		}
		if expression.Function == ScalarFunctionCoalesce {
			if slices.ContainsFunc(expression.Arguments, scalarExpressionMayReturnBooleanFunction) {
				return true
			}
		}
		return false
	case *ScalarIfExpr:
		return expression != nil &&
			(scalarExpressionMayReturnBooleanFunction(expression.True) ||
				scalarExpressionMayReturnBooleanFunction(expression.False))
	case *ScalarCaseExpr:
		if expression == nil {
			return false
		}
		for _, branch := range expression.Branches {
			if scalarExpressionMayReturnBooleanFunction(branch.Value) {
				return true
			}
		}
		return false
	default:
		return false
	}
}

// scalarExpressionMayReturnBooleanValue is intentionally stricter than the
// general function-only guard above. Search-mode eval retains its historical
// support for assigning Boolean literals, but concatenation has no implicit
// Boolean spelling; callers must use tostring explicitly.
func scalarExpressionMayReturnBooleanValue(expression ScalarExpr) bool {
	switch expression := expression.(type) {
	case *ScalarLiteralExpr:
		return expression != nil && expression.Value.Kind == LiteralKindBool
	case *ScalarCallExpr:
		if expression == nil {
			return false
		}
		if expression.Function.ReturnsBoolean() {
			return true
		}
		if expression.Function == ScalarFunctionCoalesce {
			if slices.ContainsFunc(expression.Arguments, scalarExpressionMayReturnBooleanValue) {
				return true
			}
		}
		return false
	case *ScalarIfExpr:
		return expression != nil &&
			(scalarExpressionMayReturnBooleanValue(expression.True) ||
				scalarExpressionMayReturnBooleanValue(expression.False))
	case *ScalarCaseExpr:
		if expression == nil {
			return false
		}
		for _, branch := range expression.Branches {
			if scalarExpressionMayReturnBooleanValue(branch.Value) {
				return true
			}
		}
		return false
	default:
		return false
	}
}

func coalesceScalarExpressionReturnsBoolean(arguments []ScalarExpr) bool {
	foundBoolean := false
	for _, argument := range arguments {
		if literal, ok := argument.(*ScalarLiteralExpr); ok &&
			literal != nil &&
			literal.Value.Kind == LiteralKindNull {
			continue
		}
		if !scalarExpressionReturnsBoolean(argument) {
			return false
		}
		foundBoolean = true
	}
	return foundBoolean
}

func caseScalarExpressionReturnsBoolean(branches []ScalarCaseBranch) bool {
	foundBoolean := false
	for _, branch := range branches {
		if literal, ok := branch.Value.(*ScalarLiteralExpr); ok &&
			literal != nil &&
			literal.Value.Kind == LiteralKindNull {
			continue
		}
		if !scalarExpressionReturnsBoolean(branch.Value) {
			return false
		}
		foundBoolean = true
	}
	return foundBoolean
}

func (p *parser) countEvalPredicate(sourceRange Range) error {
	if p.evalPredicates >= maxEvalPredicates {
		return &Diagnostic{
			Code:    "SPL_QUERY_TOO_COMPLEX",
			Message: fmt.Sprintf("search contains more than %d eval/where predicates", maxEvalPredicates),
			Range:   sourceRange,
		}
	}
	p.evalPredicates++
	return nil
}
