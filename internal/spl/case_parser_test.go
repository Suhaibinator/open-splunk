package spl

import (
	"strconv"
	"strings"
	"testing"
)

func TestParseEvalCasePreservesOrderedBranchesCasingAndRange(t *testing.T) {
	t.Parallel()

	const source = `index=main | eval label=CASE(status=200, replace(source, "api", "API"), status=404 OR isnull(status), coalesce(message, "missing"))`
	query, err := Parse(source)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	command := query.Commands[0].(*EvalCommand)
	conditional, ok := command.Assignments[0].Expression.(*ScalarCaseExpr)
	if !ok || len(conditional.Branches) != 2 {
		t.Fatalf("eval expression = %#v, want two-branch case", command.Assignments[0].Expression)
	}
	assertWhereLiteralComparison(
		t,
		conditional.Branches[0].Condition,
		"status",
		CompareOpEqual,
		"200",
		false,
	)
	firstValue, ok := conditional.Branches[0].Value.(*ScalarCallExpr)
	if !ok || firstValue.Function != ScalarFunctionReplace {
		t.Fatalf("first value = %#v, want replace call", conditional.Branches[0].Value)
	}
	secondCondition, ok := conditional.Branches[1].Condition.(*WhereBoolExpr)
	if !ok || secondCondition.Op != BoolOpOr {
		t.Fatalf("second condition = %#v, want OR", conditional.Branches[1].Condition)
	}
	assertWhereLiteralComparison(
		t,
		secondCondition.Left,
		"status",
		CompareOpEqual,
		"404",
		false,
	)
	if _, ok := secondCondition.Right.(*WhereScalarPredicateExpr); !ok {
		t.Fatalf("second condition right = %T, want null predicate", secondCondition.Right)
	}
	secondValue, ok := conditional.Branches[1].Value.(*ScalarCallExpr)
	if !ok || secondValue.Function != ScalarFunctionCoalesce {
		t.Fatalf("second value = %#v, want coalesce call", conditional.Branches[1].Value)
	}
	if got := source[conditional.Range.Start.Offset:conditional.Range.End.Offset]; got !=
		`CASE(status=200, replace(source, "api", "API"), status=404 OR isnull(status), coalesce(message, "missing"))` {
		t.Fatalf("case range = %q", got)
	}
}

func TestParseCaseSupportsPredicatePrecedenceNestingAndBooleanConsumers(t *testing.T) {
	t.Parallel()

	for _, source := range []string{
		`index=main | eval label=case(status=500 OR NOT status=200 AND source="api", if(host="edge", "edge", "other"))`,
		`index=main | eval flag=case(isnull(a), false, isnotnull(b), true)`,
		`index=main | where case(isnull(a), false, isnotnull(b), true)`,
		`index=main | stats count(eval(case(status=200, true, status=404, false))) AS matches`,
		`index=main | where isnull(case(status=200, source))`,
	} {
		if _, err := Parse(source); err != nil {
			t.Fatalf("Parse(%q): %v", source, err)
		}
	}

	for _, source := range []string{
		`index=main | eval flag=case(isnull(a), isnotnull(b))`,
		`index=main | eval number=tonumber(case(status=200, isnull(a)))`,
		`index=main | eval text=replace(case(status=200, isnull(a)), "x", "y")`,
	} {
		assertParseDiagnosticCode(t, source, "SPL_UNSUPPORTED_EVAL_EXPRESSION")
	}
	assertParseDiagnosticCode(
		t,
		`index=main | where case(status=200, true, status=404, "no")`,
		"SPL_EXPECTED_COMPARISON",
	)
}

func TestParseCaseEnforcesPairsAndMalformedArguments(t *testing.T) {
	t.Parallel()

	for _, source := range []string{
		`index=main | eval value=case()`,
		`index=main | eval value=case(status=200)`,
		`index=main | eval value=case(status=200, "ok", status=404)`,
		`index=main | eval value=case(status=200,, status=404, "missing")`,
		`index=main | eval value=case(status=200, "ok",)`,
	} {
		assertParseDiagnosticCode(t, source, "SPL_INVALID_EVAL_ARITY")
	}

	for _, source := range []string{
		`index=main | eval value=case(, "ok")`,
		`index=main | eval value=case(status=200, "ok"`,
	} {
		if _, err := Parse(source); err == nil {
			t.Fatalf("Parse(%q) succeeded, want malformed-argument diagnostic", source)
		}
	}

	if _, err := Parse(caseSourceWithBranchCount(MaximumCaseBranches)); err != nil {
		t.Fatalf("Parse(at case branch limit): %v", err)
	}
	assertParseDiagnosticCode(
		t,
		caseSourceWithBranchCount(MaximumCaseBranches+1),
		"SPL_QUERY_TOO_COMPLEX",
	)
}

func TestParseCaseRequiresExplicitBooleanConnectorsAndSharesBudgets(t *testing.T) {
	t.Parallel()

	assertParseDiagnosticCode(
		t,
		`index=main | eval value=case(status=200 host="api", "ok")`,
		"SPL_UNSUPPORTED_WHERE_EXPRESSION",
	)

	expression := `"fallback"`
	for range maxScalarNestingDepth - 1 {
		expression = `case(status=200,` + expression + `)`
	}
	if _, err := Parse(`index=main | eval value=` + expression); err != nil {
		t.Fatalf("Parse(at scalar nesting limit): %v", err)
	}
	assertParseDiagnosticCode(
		t,
		`index=main | eval value=case(status=200,`+expression+`)`,
		"SPL_QUERY_TOO_COMPLEX",
	)

	predicates := make([]string, maxEvalPredicates)
	for index := range predicates {
		predicates[index] = "f" + strconv.Itoa(index) + "=1"
	}
	atPredicateLimit := `index=main | where ` +
		strings.Join(predicates, ` AND `) +
		` | eval value=case(extra=1, "yes")`
	assertParseDiagnosticCode(t, atPredicateLimit, "SPL_QUERY_TOO_COMPLEX")
}

func TestParseBareCaseNameRemainsAField(t *testing.T) {
	t.Parallel()

	query, err := Parse(`index=main | eval copied=case`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	field, ok := query.Commands[0].(*EvalCommand).
		Assignments[0].Expression.(*ScalarFieldExpr)
	if !ok || field.Field != "case" {
		t.Fatalf("expression = %#v, want field named case", query.Commands[0])
	}
}

func caseSourceWithBranchCount(count int) string {
	arguments := make([]string, 0, count*2)
	for index := range count {
		arguments = append(
			arguments,
			"f"+strconv.Itoa(index)+"=1",
			strconv.Quote("v"+strconv.Itoa(index)),
		)
	}
	return `index=main | eval value=case(` +
		strings.Join(arguments, ",") +
		`)`
}
