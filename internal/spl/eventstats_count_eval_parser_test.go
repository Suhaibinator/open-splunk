package spl

import (
	"errors"
	"strings"
	"testing"
)

func TestParseEventStatsCountEvalPreservesPredicateAndSourceRanges(t *testing.T) {
	t.Parallel()

	const source = "index=main\n| EvEnTsTaTs CoUnT(EvAl(isnull(probe) OR NOT status=200 AND source=\"api\")) AS matches BY host, service"
	query, err := Parse(source)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if len(query.Commands) != 1 {
		t.Fatalf("command count = %d, want 1", len(query.Commands))
	}
	command, ok := query.Commands[0].(*EventStatsCommand)
	if !ok {
		t.Fatalf("command = %T, want *EventStatsCommand", query.Commands[0])
	}
	aggregate := command.Aggregate
	if aggregate.Function != AggregateFunctionCountPredicate ||
		aggregate.Input != "" ||
		aggregate.InputRange != (Range{}) ||
		aggregate.Predicate == nil ||
		aggregate.Percentile != 0 ||
		aggregate.Alias != "matches" ||
		!aggregate.ExplicitAlias {
		t.Fatalf("conditional aggregate = %#v", aggregate)
	}
	root, ok := aggregate.Predicate.(*WhereBoolExpr)
	if !ok || root.Op != BoolOpOr {
		t.Fatalf("predicate root = %#v, want OR", aggregate.Predicate)
	}
	right, ok := root.Right.(*WhereBoolExpr)
	if !ok || right.Op != BoolOpAnd {
		t.Fatalf("predicate right = %#v, want AND", root.Right)
	}
	if _, ok := right.Left.(*WhereNotExpr); !ok {
		t.Fatalf("predicate AND left = %T, want *WhereNotExpr", right.Left)
	}
	if len(command.GroupBy) != 2 ||
		command.GroupBy[0].Name != "host" ||
		command.GroupBy[1].Name != "service" {
		t.Fatalf("group fields = %#v, want host, service", command.GroupBy)
	}

	assertSourceRangeText(
		t,
		source,
		command.Range,
		`EvEnTsTaTs CoUnT(EvAl(isnull(probe) OR NOT status=200 AND source="api")) AS matches BY host, service`,
	)
	assertSourceRangeText(
		t,
		source,
		aggregate.Range,
		`CoUnT(EvAl(isnull(probe) OR NOT status=200 AND source="api")) AS matches`,
	)
	assertSourceRangeText(
		t,
		source,
		aggregate.Predicate.SourceRange(),
		`isnull(probe) OR NOT status=200 AND source="api"`,
	)
	assertSourceRangeText(t, source, aggregate.AliasRange, "matches")
	assertSourceRangeText(t, source, command.GroupBy[0].Range, "host")
	assertSourceRangeText(t, source, command.GroupBy[1].Range, "service")
}

func TestParseEventStatsCountEvalRequiresAliasAndBooleanPredicate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		source    string
		code      string
		message   string
		rangeText string
	}{
		{
			name:      "alias omitted",
			source:    `index=main | eventstats count(eval(status=200))`,
			code:      "SPL_UNSUPPORTED_EVENTSTATS_SYNTAX",
			message:   "requires AS",
			rangeText: "count(eval(status=200))",
		},
		{
			name:      "alias omitted before BY",
			source:    `index=main | eventstats count(eval(status=200)) BY host`,
			code:      "SPL_UNSUPPORTED_EVENTSTATS_SYNTAX",
			message:   "requires AS",
			rangeText: "count(eval(status=200))",
		},
		{
			name:    "alias name omitted",
			source:  `index=main | eventstats count(eval(status=200)) AS`,
			code:    "SPL_EXPECTED_FIELD",
			message: "output field name",
		},
		{
			name:      "empty predicate",
			source:    `index=main | eventstats count(eval()) AS matches`,
			code:      "SPL_EXPECTED_EXPRESSION",
			rangeText: ")",
		},
		{
			name:      "scalar truthiness",
			source:    `index=main | eventstats count(eval(status)) AS matches`,
			code:      "SPL_EXPECTED_COMPARISON",
			rangeText: "status",
		},
		{
			name:      "implicit conjunction",
			source:    `index=main | eventstats count(eval(status=200 host="api")) AS matches`,
			code:      "SPL_UNSUPPORTED_WHERE_EXPRESSION",
			message:   "AND or OR",
			rangeText: "host",
		},
		{
			name:      "multiple eval arguments",
			source:    `index=main | eventstats count(eval(status=200), host="api") AS matches`,
			code:      "SPL_EXPECTED_RIGHT_PAREN",
			rangeText: ",",
		},
		{
			name:      "missing inner right parenthesis",
			source:    `index=main | eventstats count(eval(status=200) AS matches`,
			code:      "SPL_EXPECTED_RIGHT_PAREN",
			rangeText: "AS",
		},
		{
			name:      "space separated second measure",
			source:    `index=main | eventstats count(eval(status=200)) AS matches count`,
			code:      "SPL_UNSUPPORTED_EVENTSTATS_SYNTAX",
			rangeText: "count",
		},
		{
			name:      "comma separated second measure",
			source:    `index=main | eventstats count(eval(status=200)) AS matches, count`,
			code:      "SPL_UNSUPPORTED_EVENTSTATS_SYNTAX",
			rangeText: ",",
		},
	}
	for _, test := range tests {
		test := test
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
			assertSourceRangeText(t, test.source, diagnostic.Range, test.rangeText)
		})
	}
}

func TestParseEventStatsCountEvalSharesQueryWidePredicateBudget(t *testing.T) {
	t.Parallel()

	const leaf = `status=200`
	valid := `index=main | where ` +
		strings.Join(repeatString(leaf, 30), ` AND `) +
		` | eventstats count(eval(status=201)) AS matches` +
		` | where status=202`
	if _, err := Parse(valid); err != nil {
		t.Fatalf("Parse at predicate ceiling: %v", err)
	}

	_, err := Parse(valid + ` | where status=203`)
	var diagnostic *Diagnostic
	if !errors.As(err, &diagnostic) || diagnostic.Code != "SPL_QUERY_TOO_COMPLEX" {
		t.Fatalf("diagnostic = %#v, want SPL_QUERY_TOO_COMPLEX", err)
	}
}

func TestAnalyzeEventStatsCountEvalSuggestionContextUsesScalarCatalog(t *testing.T) {
	t.Parallel()

	const source = `| eventstats count(eval(isn`
	context, diagnostic := AnalyzeSuggestionContext(source, len(source))
	if diagnostic != nil {
		t.Fatalf("AnalyzeSuggestionContext: %v", diagnostic)
	}
	if context.FunctionClass != SuggestionFunctionClassScalar {
		t.Fatalf("function class = %q, want scalar", context.FunctionClass)
	}
	if len(context.Kinds) != 2 ||
		context.Kinds[0] != SuggestionKindFunction ||
		context.Kinds[1] != SuggestionKindField {
		t.Fatalf("suggestion kinds = %v, want function and field", context.Kinds)
	}
}
