package spl

import (
	"errors"
	"strings"
	"testing"
)

func TestParseStatsPreservesExplicitWildcardInputs(t *testing.T) {
	t.Parallel()

	const source = `| stats AVG(*lay) c(http_*) count(*)`
	query, err := Parse(source)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	stats := query.Commands[0].(*StatsCommand)
	if len(stats.Aggregates) != 3 {
		t.Fatalf("aggregates = %#v", stats.Aggregates)
	}

	tests := []struct {
		index    int
		function AggregateFunction
		pattern  string
		alias    string
	}{
		{0, AggregateFunctionAverage, "*lay", "avg(*lay)"},
		{1, AggregateFunctionCountValues, "http_*", "count(http_*)"},
		{2, AggregateFunctionCountValues, "*", "count(*)"},
	}
	for _, test := range tests {
		aggregate := stats.Aggregates[test.index]
		if aggregate.Function != test.function || aggregate.InputGlob == nil ||
			aggregate.InputGlob.Pattern != test.pattern || aggregate.InputGlob.Implicit ||
			aggregate.Input != "" || aggregate.InputRange != (Range{}) ||
			aggregate.Alias != test.alias || aggregate.ExplicitAlias {
			t.Fatalf("aggregate[%d] = %#v", test.index, aggregate)
		}
		got := source[aggregate.InputGlob.Range.Start.Offset:aggregate.InputGlob.Range.End.Offset]
		if got != test.pattern {
			t.Fatalf("aggregate[%d] wildcard range = %q, want %q", test.index, got, test.pattern)
		}
	}
}

func TestParseStatsPreservesImplicitWildcardInputs(t *testing.T) {
	t.Parallel()

	const source = `| stats avg dc p95 count`
	query, err := Parse(source)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	stats := query.Commands[0].(*StatsCommand)
	if len(stats.Aggregates) != 4 {
		t.Fatalf("aggregates = %#v", stats.Aggregates)
	}
	for index, alias := range []string{"avg(*)", "dc(*)", "perc95(*)"} {
		aggregate := stats.Aggregates[index]
		if aggregate.InputGlob == nil || aggregate.InputGlob.Pattern != "*" ||
			!aggregate.InputGlob.Implicit || aggregate.InputGlob.Range != aggregate.Range ||
			aggregate.AliasRange != aggregate.Range || aggregate.Alias != alias {
			t.Fatalf("aggregate[%d] = %#v", index, aggregate)
		}
	}
	if count := stats.Aggregates[3]; count.Function != AggregateFunctionCount ||
		count.InputGlob != nil || count.Alias != "count" {
		t.Fatalf("row count = %#v", count)
	}
}

func TestParseStatsPreservesWildcardAliasCapturePatterns(t *testing.T) {
	t.Parallel()

	const source = `| stats avg(http_*_*) AS mean_*_* sum AS total_*`
	query, err := Parse(source)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	aggregates := query.Commands[0].(*StatsCommand).Aggregates
	if len(aggregates) != 2 {
		t.Fatalf("aggregates = %#v", aggregates)
	}
	for index, test := range []struct {
		inputPattern string
		implicit     bool
		aliasPattern string
	}{
		{"http_*_*", false, "mean_*_*"},
		{"*", true, "total_*"},
	} {
		aggregate := aggregates[index]
		if aggregate.InputGlob == nil ||
			aggregate.InputGlob.Pattern != test.inputPattern ||
			aggregate.InputGlob.Implicit != test.implicit ||
			aggregate.AliasGlob == nil ||
			aggregate.AliasGlob.Pattern != test.aliasPattern ||
			aggregate.AliasGlob.Implicit || !aggregate.ExplicitAlias ||
			aggregate.AliasRange != aggregate.AliasGlob.Range ||
			aggregate.Range.End != aggregate.AliasGlob.Range.End {
			t.Fatalf("aggregate[%d] = %#v", index, aggregate)
		}
	}
}

func TestParseStatsRejectsInvalidWildcardAliases(t *testing.T) {
	t.Parallel()

	for _, source := range []string{
		`| stats avg(*lay) AS mean`,
		`| stats avg AS mean`,
		`| stats avg(http_*_*) AS mean_*`,
		`| stats avg(*) AS "mean_*"`,
		`| stats sparkline(avg(*lay)) AS mean`,
		`| stats sparkline(avg(http_*_*)) AS mean_*`,
		`| stats sparkline(avg(*)) AS "mean_*"`,
	} {
		_, err := Parse(source)
		var diagnostic *Diagnostic
		if !errors.As(err, &diagnostic) ||
			diagnostic.Code != "SPL_UNSUPPORTED_STATS_AGGREGATE" {
			t.Fatalf("Parse(%q) error = %#v", source, err)
		}
		if !strings.Contains(strings.ToLower(diagnostic.Message), "wildcard") &&
			!strings.Contains(strings.ToLower(diagnostic.Message), "wc-field") {
			t.Fatalf("Parse(%q) message = %q", source, diagnostic.Message)
		}
	}
}
