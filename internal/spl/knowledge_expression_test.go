package spl

import (
	"strings"
	"testing"
)

func TestParseScalarExpressionUsesExactStandaloneBudgets(t *testing.T) {
	expression := `"` + strings.Repeat("x", maxSPLSourceBytes-2) + `"`
	parsed, err := ParseScalarExpression(expression)
	if err != nil {
		t.Fatalf("ParseScalarExpression(exact byte ceiling): %v", err)
	}
	if literal, ok := parsed.(*ScalarLiteralExpr); !ok || literal.Value.Text != strings.Repeat("x", maxSPLSourceBytes-2) {
		t.Fatalf("ParseScalarExpression(exact byte ceiling) = %#v", parsed)
	}
	if _, err := ParseScalarExpression(expression + "x"); err == nil {
		t.Fatal("ParseScalarExpression(over byte ceiling) succeeded")
	}
}

func TestParseScalarExpressionRequiresCompleteInput(t *testing.T) {
	if _, err := ParseScalarExpression("lower(host) trailing"); err == nil {
		t.Fatal("ParseScalarExpression(trailing token) succeeded")
	}
}
