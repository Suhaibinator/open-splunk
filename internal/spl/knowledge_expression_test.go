package spl

import (
	"slices"
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

func TestAnalyzeScalarExpressionReturnsExactDetachedInventory(t *testing.T) {
	t.Parallel()

	parsed, err := ParseScalarExpression(`if(isnull(host), lower(source), upper(sourcetype))`)
	if err != nil {
		t.Fatalf("ParseScalarExpression: %v", err)
	}
	analysis, err := AnalyzeScalarExpression(parsed)
	if err != nil {
		t.Fatalf("AnalyzeScalarExpression: %v", err)
	}
	if analysis.Nodes != 8 || analysis.Predicates != 1 || !slices.Equal(
		analysis.InputFields,
		[]string{"host", "source", "sourcetype"},
	) {
		t.Fatalf("analysis = %#v", analysis)
	}
	analysis.InputFields[0] = "mutated"
	again, err := AnalyzeScalarExpression(parsed)
	if err != nil || !slices.Equal(again.InputFields, []string{"host", "source", "sourcetype"}) {
		t.Fatalf("detached analysis = %#v, %v", again, err)
	}
}

func TestAnalyzeScalarExpressionCountsPredicateLeavesExactly(t *testing.T) {
	t.Parallel()

	parsed, err := ParseScalarExpression(`if(a=1 AND (isnull(b) OR c=2), d, e)`)
	if err != nil {
		t.Fatalf("ParseScalarExpression: %v", err)
	}
	analysis, err := AnalyzeScalarExpression(parsed)
	if err != nil {
		t.Fatalf("AnalyzeScalarExpression: %v", err)
	}
	if analysis.Predicates != 3 {
		t.Fatalf("predicate leaves = %d, want 3", analysis.Predicates)
	}
}

func TestAnalyzeScalarExpressionRejectsHandBuiltInvalidOrCyclicTrees(t *testing.T) {
	t.Parallel()

	if _, err := AnalyzeScalarExpression((*ScalarFieldExpr)(nil)); err == nil {
		t.Fatal("typed-nil scalar field was accepted")
	}
	cyclic := &ScalarCallExpr{Function: ScalarFunctionLower}
	cyclic.Arguments = []ScalarExpr{cyclic}
	if _, err := AnalyzeScalarExpression(cyclic); err == nil {
		t.Fatal("cyclic scalar expression was accepted")
	}
	invalidBoolean := &ScalarIfExpr{
		Condition: &WhereBoolExpr{Op: BoolOpInvalid},
		True:      &ScalarLiteralExpr{},
		False:     &ScalarLiteralExpr{},
	}
	if _, err := AnalyzeScalarExpression(invalidBoolean); err == nil {
		t.Fatal("invalid Boolean operator was accepted")
	}
}
