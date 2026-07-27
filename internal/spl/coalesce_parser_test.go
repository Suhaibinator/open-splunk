package spl

import (
	"strconv"
	"strings"
	"testing"
)

func TestParseEvalCoalescePreservesArgumentsCasingAndRange(t *testing.T) {
	t.Parallel()

	const source = `index=main | eval selected=COALESCE(null, source, "fallback"), identity=coalesce(message)`
	query, err := Parse(source)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	command := query.Commands[0].(*EvalCommand)
	if len(command.Assignments) != 2 {
		t.Fatalf("assignments = %#v", command.Assignments)
	}
	selected, selectedOK := command.Assignments[0].Expression.(*ScalarCallExpr)
	if !selectedOK || selected.Function != ScalarFunctionCoalesce ||
		len(selected.Arguments) != 3 {
		t.Fatalf("selected expression = %#v, want three-argument coalesce", command.Assignments[0].Expression)
	}
	if literal, literalOK := selected.Arguments[0].(*ScalarLiteralExpr); !literalOK ||
		literal.Value.Kind != LiteralKindNull {
		t.Fatalf("first argument = %#v, want null literal", selected.Arguments[0])
	}
	if field, fieldOK := selected.Arguments[1].(*ScalarFieldExpr); !fieldOK ||
		field.Field != "source" {
		t.Fatalf("second argument = %#v, want source field", selected.Arguments[1])
	}
	if literal, literalOK := selected.Arguments[2].(*ScalarLiteralExpr); !literalOK ||
		literal.Value.Kind != LiteralKindString ||
		literal.Value.Text != "fallback" {
		t.Fatalf("third argument = %#v, want fallback literal", selected.Arguments[2])
	}
	if got := source[selected.Range.Start.Offset:selected.Range.End.Offset]; got !=
		`COALESCE(null, source, "fallback")` {
		t.Fatalf("coalesce range = %q", got)
	}

	identity, ok := command.Assignments[1].Expression.(*ScalarCallExpr)
	if !ok || identity.Function != ScalarFunctionCoalesce ||
		len(identity.Arguments) != 1 ||
		identity.Arguments[0].(*ScalarFieldExpr).Field != "message" {
		t.Fatalf("identity expression = %#v", command.Assignments[1].Expression)
	}
}

func TestParseCoalesceSupportsNestedFixedScalarsAndBooleanConsumers(t *testing.T) {
	t.Parallel()

	for _, source := range []string{
		`index=main | eval value=coalesce(null, if(status=200, replace(source, "a", "b"), "other"), "fallback")`,
		`index=main | where coalesce(isnull(first), false)`,
		`index=main | stats count(eval(coalesce(isnull(first), isnotnull(second)))) AS matches`,
		`index=main | eval flag=coalesce(false, true)`,
	} {
		if _, err := Parse(source); err != nil {
			t.Fatalf("Parse(%q): %v", source, err)
		}
	}

	for _, source := range []string{
		`index=main | eval flag=coalesce(isnull(first), false)`,
		`index=main | eval number=tonumber(coalesce(isnotnull(first), false))`,
		`index=main | eval text=replace(coalesce(isnull(first), false), "x", "y")`,
	} {
		assertParseDiagnosticCode(t, source, "SPL_UNSUPPORTED_EVAL_EXPRESSION")
	}
}

func TestParseCoalesceEnforcesArityAndMalformedArguments(t *testing.T) {
	t.Parallel()

	assertParseDiagnosticCode(
		t,
		`index=main | eval value=coalesce()`,
		"SPL_INVALID_EVAL_ARITY",
	)
	for _, source := range []string{
		`index=main | eval value=coalesce(first,)`,
		`index=main | eval value=coalesce(,first)`,
		`index=main | eval value=coalesce(first,,second)`,
		`index=main | eval value=coalesce(first,second`,
	} {
		if _, err := Parse(source); err == nil {
			t.Fatalf("Parse(%q) succeeded, want malformed-argument diagnostic", source)
		}
	}

	atLimit := coalesceSourceWithArgumentCount(MaximumCoalesceArguments)
	if _, err := Parse(atLimit); err != nil {
		t.Fatalf("Parse(at coalesce argument limit): %v", err)
	}
	assertParseDiagnosticCode(
		t,
		coalesceSourceWithArgumentCount(MaximumCoalesceArguments+1),
		"SPL_QUERY_TOO_COMPLEX",
	)
}

func TestParseCoalesceSharesScalarNestingAndPredicateBudgets(t *testing.T) {
	t.Parallel()

	expression := `"fallback"`
	for range maxScalarNestingDepth - 1 {
		expression = "coalesce(null," + expression + ")"
	}
	if _, err := Parse("index=main | eval value=" + expression); err != nil {
		t.Fatalf("Parse(at scalar nesting limit): %v", err)
	}
	assertParseDiagnosticCode(
		t,
		"index=main | eval value=coalesce(null,"+expression+")",
		"SPL_QUERY_TOO_COMPLEX",
	)

	predicates := make([]string, maxEvalPredicates)
	for index := range predicates {
		predicates[index] = "f" + strconv.Itoa(index) + "=1"
	}
	atPredicateLimit := `index=main | where ` +
		strings.Join(predicates, ` AND `) +
		` | eval value=coalesce(null, if(extra=1, "yes", "no"))`
	assertParseDiagnosticCode(t, atPredicateLimit, "SPL_QUERY_TOO_COMPLEX")
}

func TestParseBareCoalesceNameRemainsAField(t *testing.T) {
	t.Parallel()

	query, err := Parse(`index=main | eval copied=coalesce`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	field, ok := query.Commands[0].(*EvalCommand).
		Assignments[0].Expression.(*ScalarFieldExpr)
	if !ok || field.Field != "coalesce" {
		t.Fatalf("expression = %#v, want field named coalesce", query.Commands[0])
	}
}

func coalesceSourceWithArgumentCount(count int) string {
	arguments := make([]string, count)
	for index := range arguments {
		arguments[index] = "f" + strconv.Itoa(index)
	}
	return `index=main | eval value=coalesce(` +
		strings.Join(arguments, ",") +
		`)`
}
