package spl

import (
	"errors"
	"strings"
	"testing"
)

func TestParseStatsCountEvalPreservesTypedPredicateAndMeasureOrder(t *testing.T) {
	t.Parallel()

	const source = `index=main | stats COUNT(EVAL(isnull(probe) OR NOT status=200 AND source="api")) AS matches count AS rows BY host`
	query, err := Parse(source)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if len(query.Commands) != 1 {
		t.Fatalf("command count = %d, want 1", len(query.Commands))
	}
	command, ok := query.Commands[0].(*StatsCommand)
	if !ok {
		t.Fatalf("command = %T, want *StatsCommand", query.Commands[0])
	}
	if len(command.Aggregates) != 2 {
		t.Fatalf("aggregates = %#v, want two measures", command.Aggregates)
	}
	conditional := command.Aggregates[0]
	if conditional.Function != AggregateFunctionCountPredicate ||
		conditional.Input != "" ||
		conditional.InputRange != (Range{}) ||
		conditional.Predicate == nil ||
		conditional.Alias != "matches" ||
		!conditional.ExplicitAlias {
		t.Fatalf("conditional count = %#v", conditional)
	}
	root, ok := conditional.Predicate.(*WhereBoolExpr)
	if !ok || root.Op != BoolOpOr {
		t.Fatalf("predicate root = %#v, want OR", conditional.Predicate)
	}
	right, ok := root.Right.(*WhereBoolExpr)
	if !ok || right.Op != BoolOpAnd {
		t.Fatalf("predicate right = %#v, want AND", root.Right)
	}
	if _, ok := right.Left.(*WhereNotExpr); !ok {
		t.Fatalf("predicate AND left = %T, want *WhereNotExpr", right.Left)
	}
	rows := command.Aggregates[1]
	if rows.Function != AggregateFunctionCount || rows.Predicate != nil ||
		rows.Alias != "rows" || !rows.ExplicitAlias {
		t.Fatalf("row count = %#v", rows)
	}
	if len(command.GroupBy) != 1 || command.GroupBy[0].Name != "host" {
		t.Fatalf("group by = %#v, want host", command.GroupBy)
	}
	if got := source[conditional.Range.Start.Offset:conditional.Range.End.Offset]; got !=
		`COUNT(EVAL(isnull(probe) OR NOT status=200 AND source="api")) AS matches` {
		t.Fatalf("conditional source range = %q", got)
	}
}

func TestParseStatsCountEvalAcceptsNestedBooleanIf(t *testing.T) {
	t.Parallel()

	query, err := Parse(
		`index=main | stats count(eval(if(isnull(probe), if(isnull(absent), true, false), false))) AS nested`,
	)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	command := query.Commands[0].(*StatsCommand)
	if len(command.Aggregates) != 1 ||
		command.Aggregates[0].Function != AggregateFunctionCountPredicate {
		t.Fatalf("aggregates = %#v", command.Aggregates)
	}
	direct, ok := command.Aggregates[0].Predicate.(*WhereScalarPredicateExpr)
	if !ok {
		t.Fatalf("predicate = %T, want *WhereScalarPredicateExpr", command.Aggregates[0].Predicate)
	}
	if _, ok := direct.Value.(*ScalarIfExpr); !ok {
		t.Fatalf("predicate value = %T, want *ScalarIfExpr", direct.Value)
	}
}

func TestParseStatsCountEvalRequiresExactPredicateSyntax(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		source  string
		code    string
		message string
	}{
		{
			name:   "alias name omitted",
			source: `index=main | stats count(eval(status=200)) AS`,
			code:   "SPL_EXPECTED_FIELD",
		},
		{
			name:   "empty predicate",
			source: `index=main | stats count(eval()) AS matches`,
			code:   "SPL_EXPECTED_EXPRESSION",
		},
		{
			name:   "scalar truthiness",
			source: `index=main | stats count(eval(status)) AS matches`,
			code:   "SPL_EXPECTED_COMPARISON",
		},
		{
			name:    "implicit conjunction",
			source:  `index=main | stats count(eval(status=200 host="api")) AS matches`,
			code:    "SPL_UNSUPPORTED_WHERE_EXPRESSION",
			message: "AND or OR",
		},
		{
			name:   "multiple arguments",
			source: `index=main | stats count(eval(status=200), host="api") AS matches`,
			code:   "SPL_EXPECTED_RIGHT_PAREN",
		},
		{
			name:   "missing inner right parenthesis",
			source: `index=main | stats count(eval(status=200) AS matches`,
			code:   "SPL_EXPECTED_RIGHT_PAREN",
		},
		{
			name:   "abbreviation remains unsupported",
			source: `index=main | stats c(eval(status=200)) AS matches`,
			code:   "SPL_EXPECTED_RIGHT_PAREN",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			_, err := Parse(test.source)
			if err == nil {
				t.Fatal("Parse succeeded, want error")
			}
			var diagnostic *Diagnostic
			if !errors.As(err, &diagnostic) {
				t.Fatalf("error = %T %v, want *Diagnostic", err, err)
			}
			if diagnostic.Code != test.code {
				t.Fatalf("diagnostic code = %q, want %q: %v", diagnostic.Code, test.code, diagnostic)
			}
			if test.message != "" && !strings.Contains(diagnostic.Message, test.message) {
				t.Fatalf("diagnostic message = %q, want substring %q", diagnostic.Message, test.message)
			}
		})
	}
}

func TestParseStatsCountEvalSharesTheQueryWidePredicateBudget(t *testing.T) {
	t.Parallel()

	const leaf = `status=200`
	prefix := `index=main | where ` +
		strings.Join(repeatString(leaf, 30), ` AND `)
	valid := prefix +
		` | stats count(eval(status=201)) AS first` +
		` count(eval(status=202)) AS second`
	if _, err := Parse(valid); err != nil {
		t.Fatalf("Parse at predicate ceiling: %v", err)
	}
	tooMany := valid + ` count(eval(status=203)) AS third`
	_, err := Parse(tooMany)
	if err == nil {
		t.Fatal("Parse above predicate ceiling succeeded")
	}
	var diagnostic *Diagnostic
	if !errors.As(err, &diagnostic) || diagnostic.Code != "SPL_QUERY_TOO_COMPLEX" {
		t.Fatalf("diagnostic = %#v, want SPL_QUERY_TOO_COMPLEX", err)
	}
}

func repeatString(value string, count int) []string {
	values := make([]string, count)
	for index := range values {
		values[index] = value
	}
	return values
}
