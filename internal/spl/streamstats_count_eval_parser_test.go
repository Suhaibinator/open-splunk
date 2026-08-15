package spl

import (
	"errors"
	"strings"
	"testing"
)

func TestParseStreamStatsCountEvalPreservesPredicateOptionsAndRanges(t *testing.T) {
	t.Parallel()

	const source = "index=main\n| StReAmStAtS current=f window=3 global=f CoUnT(EvAl(isnull(probe) OR NOT status=200 AND source=\"api\")) AS matches BY host, service"
	query, err := Parse(source)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if len(query.Commands) != 1 {
		t.Fatalf("command count = %d, want 1", len(query.Commands))
	}
	command, ok := query.Commands[0].(*StreamStatsCommand)
	if !ok {
		t.Fatalf("command = %T, want *StreamStatsCommand", query.Commands[0])
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
	if command.Current || !command.CurrentSpecified ||
		command.Window != 3 || !command.WindowSpecified ||
		command.Global || !command.GlobalSpecified {
		t.Fatalf(
			"options = current %t/%t, window %d/%t, global %t/%t",
			command.Current,
			command.CurrentSpecified,
			command.Window,
			command.WindowSpecified,
			command.Global,
			command.GlobalSpecified,
		)
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
		`StReAmStAtS current=f window=3 global=f CoUnT(EvAl(isnull(probe) OR NOT status=200 AND source="api")) AS matches BY host, service`,
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
	assertSourceRangeText(t, source, command.CurrentRange, "current=f")
	assertSourceRangeText(t, source, command.WindowRange, "window=3")
	assertSourceRangeText(t, source, command.GlobalRange, "global=f")
	assertSourceRangeText(t, source, command.GroupBy[0].Range, "host")
	assertSourceRangeText(t, source, command.GroupBy[1].Range, "service")
}

func TestParseStreamStatsCountEvalRequiresCanonicalCountAliasAndBooleanPredicate(
	t *testing.T,
) {
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
			source:    `index=main | streamstats count(eval(status=200))`,
			code:      "SPL_UNSUPPORTED_STREAMSTATS_SYNTAX",
			message:   "requires AS",
			rangeText: "count(eval(status=200))",
		},
		{
			name:      "alias omitted before BY",
			source:    `index=main | streamstats count(eval(status=200)) BY host`,
			code:      "SPL_UNSUPPORTED_STREAMSTATS_SYNTAX",
			message:   "requires AS",
			rangeText: "count(eval(status=200))",
		},
		{
			name:      "alias name omitted",
			source:    `index=main | streamstats count(eval(status=200)) AS`,
			code:      "SPL_UNSUPPORTED_STREAMSTATS_SYNTAX",
			message:   "output field",
			rangeText: "AS",
		},
		{
			name:      "empty predicate",
			source:    `index=main | streamstats count(eval()) AS matches`,
			code:      "SPL_EXPECTED_EXPRESSION",
			rangeText: ")",
		},
		{
			name:      "scalar truthiness",
			source:    `index=main | streamstats count(eval(status)) AS matches`,
			code:      "SPL_EXPECTED_COMPARISON",
			rangeText: "status",
		},
		{
			name:      "implicit conjunction",
			source:    `index=main | streamstats count(eval(status=200 host="api")) AS matches`,
			code:      "SPL_UNSUPPORTED_WHERE_EXPRESSION",
			message:   "AND or OR",
			rangeText: "host",
		},
		{
			name:      "multiple eval arguments",
			source:    `index=main | streamstats count(eval(status=200), host="api") AS matches`,
			code:      "SPL_EXPECTED_RIGHT_PAREN",
			rangeText: ",",
		},
		{
			name:      "missing inner right parenthesis",
			source:    `index=main | streamstats count(eval(status=200) AS matches`,
			code:      "SPL_EXPECTED_RIGHT_PAREN",
			rangeText: "AS",
		},
		{
			name:      "count synonym",
			source:    `index=main | streamstats c(eval(status=200)) AS matches`,
			code:      "SPL_UNSUPPORTED_STREAMSTATS_AGGREGATE",
			rangeText: "c",
		},
		{
			name:      "eval on another aggregate",
			source:    `index=main | streamstats sum(eval(status=200)) AS matches`,
			code:      "SPL_UNSUPPORTED_STREAMSTATS_AGGREGATE",
			rangeText: "sum",
		},
		{
			name:      "second measure",
			source:    `index=main | streamstats count(eval(status=200)) AS matches count`,
			code:      "SPL_UNSUPPORTED_STREAMSTATS_AGGREGATE",
			rangeText: "count",
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
				t.Fatalf(
					"diagnostic code = %q, want %q: %v",
					diagnostic.Code,
					test.code,
					diagnostic,
				)
			}
			if test.message != "" && !strings.Contains(diagnostic.Message, test.message) {
				t.Fatalf(
					"diagnostic message = %q, want substring %q",
					diagnostic.Message,
					test.message,
				)
			}
			assertSourceRangeText(t, test.source, diagnostic.Range, test.rangeText)
		})
	}
}

func TestParseStreamStatsCountEvalSharesQueryWidePredicateBudget(t *testing.T) {
	t.Parallel()

	const leaf = `status=200`
	valid := `index=main | where ` +
		strings.Join(repeatString(leaf, 30), ` AND `) +
		` | streamstats count(eval(status=201)) AS matches` +
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

func TestAnalyzeStreamStatsCountEvalSuggestionContextUsesScalarCatalog(t *testing.T) {
	t.Parallel()

	const source = `| streamstats count(eval(isn`
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

	const closed = `| streamstats count(eval(status=500)) `
	context, diagnostic = AnalyzeSuggestionContext(closed, len(closed))
	if diagnostic != nil {
		t.Fatalf("AnalyzeSuggestionContext after predicate: %v", diagnostic)
	}
	foundAS := false
	for _, keyword := range context.Keywords {
		if keyword == "AS" {
			foundAS = true
		}
		if keyword == "BY" {
			t.Fatalf("keywords = %v, must require AS before BY", context.Keywords)
		}
	}
	if !foundAS {
		t.Fatalf("keywords = %v, want mandatory AS", context.Keywords)
	}

	const invalidBY = `| streamstats count(eval(status=500)) BY `
	context, diagnostic = AnalyzeSuggestionContext(invalidBY, len(invalidBY))
	if diagnostic != nil {
		t.Fatalf("AnalyzeSuggestionContext after invalid BY: %v", diagnostic)
	}
	if len(context.Kinds) != 0 || len(context.Keywords) != 0 ||
		len(context.FunctionNames) != 0 {
		t.Fatalf(
			"context after BY-before-alias = %#v, want no invalid append-only suggestion",
			context,
		)
	}

	const aliased = `| streamstats count(eval(status=500)) AS matches `
	context, diagnostic = AnalyzeSuggestionContext(aliased, len(aliased))
	if diagnostic != nil {
		t.Fatalf("AnalyzeSuggestionContext after alias: %v", diagnostic)
	}
	foundBY := false
	for _, keyword := range context.Keywords {
		if keyword == "AS" {
			t.Fatalf("keywords = %v, must not repeat AS after alias", context.Keywords)
		}
		if keyword == "BY" {
			foundBY = true
		}
	}
	if !foundBY {
		t.Fatalf("keywords = %v, want BY after explicit alias", context.Keywords)
	}
}

func TestStreamStatsCompletionDescribesConditionalCount(t *testing.T) {
	t.Parallel()

	for _, command := range completionCatalog.Commands {
		if command.Name != "streamstats" {
			continue
		}
		if !strings.Contains(command.Detail, "true-only count(eval(predicate))") {
			t.Fatalf(
				"streamstats detail = %q, want conditional-count contract",
				command.Detail,
			)
		}
		return
	}
	t.Fatal("completion catalog has no streamstats command")
}
