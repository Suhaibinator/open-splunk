package spl

import (
	"errors"
	"testing"
)

func TestParseStatsAcceptsExactQuotedInputsAliasesAndBYFields(t *testing.T) {
	t.Parallel()

	const source = `index=main | stats AvG('Product Name') AS "Revenue" count('http\\.status') AS ".com" BY 'HTTP Status', host`
	query, err := Parse(source)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	command := query.Commands[0].(*StatsCommand)
	if len(command.Aggregates) != 2 || len(command.GroupBy) != 2 {
		t.Fatalf("command = %#v", command)
	}
	average := command.Aggregates[0]
	if average.Function != AggregateFunctionAverage ||
		average.Input != "Product Name" || !average.InputQuoted ||
		average.Alias != "Revenue" || !average.AliasQuoted ||
		!average.ExplicitAlias || average.AliasSourceDerived {
		t.Fatalf("average = %#v", average)
	}
	count := command.Aggregates[1]
	if count.Function != AggregateFunctionCountValues ||
		count.Input != `http\.status` || !count.InputQuoted ||
		count.Alias != ".com" || !count.AliasQuoted ||
		!count.ExplicitAlias {
		t.Fatalf("count = %#v", count)
	}
	if command.GroupBy[0].Name != "HTTP Status" ||
		!command.GroupBy[0].Quoted || command.GroupBy[1].Name != "host" ||
		command.GroupBy[1].Quoted {
		t.Fatalf("groups = %#v", command.GroupBy)
	}
	for _, test := range []struct {
		sourceRange Range
		want        string
	}{
		{average.InputRange, `'Product Name'`},
		{average.AliasRange, `"Revenue"`},
		{count.InputRange, `'http\\.status'`},
		{count.AliasRange, `".com"`},
		{command.GroupBy[0].Range, `'HTTP Status'`},
	} {
		assertSourceRangeText(t, source, test.sourceRange, test.want)
	}
}

func TestParseStatsImplicitEvalAliasesPreserveAuthoredInvocation(t *testing.T) {
	t.Parallel()

	const source = `| stats SuM(eval(bytes * price)) count(eval(status=500)) values(eval(isnull(flag)))`
	query, err := Parse(source)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	aggregates := query.Commands[0].(*StatsCommand).Aggregates
	wantAliases := []string{
		`SuM(eval(bytes * price))`,
		`count(eval(status=500))`,
		`values(eval(isnull(flag)))`,
	}
	for index, aggregate := range aggregates {
		if aggregate.Alias != wantAliases[index] || aggregate.ExplicitAlias ||
			aggregate.AliasQuoted || !aggregate.AliasSourceDerived ||
			aggregate.AliasRange != aggregate.Range {
			t.Errorf("aggregate[%d] = %#v", index, aggregate)
		}
		assertSourceRangeText(t, source, aggregate.AliasRange, wantAliases[index])
	}
	if aggregates[0].InputExpression == nil || aggregates[1].Predicate == nil ||
		aggregates[2].InputExpression == nil {
		t.Fatalf("eval arms = %#v", aggregates)
	}

	explicit, err := Parse(`| stats sum(eval(bytes * price)) AS "Revenue"`)
	if err != nil {
		t.Fatalf("Parse explicit alias: %v", err)
	}
	aggregate := explicit.Commands[0].(*StatsCommand).Aggregates[0]
	if !aggregate.ExplicitAlias || !aggregate.AliasQuoted ||
		aggregate.AliasSourceDerived || aggregate.Alias != "Revenue" {
		t.Fatalf("explicit aggregate = %#v", aggregate)
	}
}

func TestParseStatsImplicitEvalAliasNormalizesControlWhitespace(t *testing.T) {
	t.Parallel()

	const source = "| stats sum(eval(value\t+\n1))"
	query, err := Parse(source)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	aggregate := query.Commands[0].(*StatsCommand).Aggregates[0]
	if aggregate.Alias != "sum(eval(value + 1))" ||
		!aggregate.AliasSourceDerived || aggregate.AliasRange != aggregate.Range {
		t.Fatalf("aggregate = %#v", aggregate)
	}
	assertSourceRangeText(t, source, aggregate.AliasRange, "sum(eval(value\t+\n1))")
}

func TestParseStatsLiteralOutputsRemainSingleQuoteReferenceable(t *testing.T) {
	t.Parallel()

	const source = `| stats count AS ".com" avg(value) AS "Product Name" | where isnotnull('.com') | table '.com', 'Product Name'`
	query, err := Parse(source)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if len(query.Commands) != 3 {
		t.Fatalf("commands = %#v", query.Commands)
	}
	table := query.Commands[2].(*TableCommand)
	if len(table.Fields) != 2 || table.Fields[0] != ".com" ||
		table.Fields[1] != "Product Name" ||
		len(table.QuotedFields) != 2 || !table.QuotedFields[0] ||
		!table.QuotedFields[1] || len(table.FieldRanges) != 2 {
		t.Fatalf("table = %#v", table)
	}
	for index, want := range []string{"'.com'", "'Product Name'"} {
		assertSourceRangeText(t, source, table.FieldRanges[index], want)
	}
}

func TestParseStatsQuotedFieldsAndOutputsFailClosed(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		source string
		code   string
	}{
		{name: "empty input", source: `| stats avg('')`, code: "SPL_EXPECTED_FIELD"},
		{name: "wildcard input", source: `| stats avg('request*')`, code: "SPL_INVALID_FIELD"},
		{name: "private input", source: `| stats avg('__os_private')`, code: "SPL_INVALID_FIELD"},
		{name: "reserved input", source: `| stats avg('fields')`, code: "SPL_INVALID_FIELD"},
		{name: "bad input escape", source: `| stats avg('bad\q')`, code: "SPL_INVALID_FIELD_QUOTE_ESCAPE"},
		{name: "wildcard BY", source: `| stats count BY 'host*'`, code: "SPL_INVALID_FIELD"},
		{name: "private BY", source: `| stats count BY '__os_group'`, code: "SPL_INVALID_FIELD"},
		{name: "empty output", source: `| stats count AS ""`, code: "SPL_UNSUPPORTED_STATS_AGGREGATE"},
		{name: "control output", source: `| stats count AS "line\nbreak"`, code: "SPL_UNSUPPORTED_STATS_AGGREGATE"},
		{name: "private output", source: `| stats count AS "__os_private"`, code: "SPL_UNSUPPORTED_STATS_AGGREGATE"},
		{name: "reserved output", source: `| stats count AS "fields"`, code: "SPL_UNSUPPORTED_STATS_AGGREGATE"},
		{name: "single quoted output", source: `| stats count AS 'Revenue'`, code: "SPL_EXPECTED_FIELD"},
		{name: "unquoted wildcard output", source: `| stats count AS revenue*`, code: "SPL_UNSUPPORTED_STATS_AGGREGATE"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			_, err := Parse(test.source)
			var diagnostic *Diagnostic
			if !errors.As(err, &diagnostic) || diagnostic.Code != test.code {
				t.Fatalf("diagnostic = %#v, want %s", err, test.code)
			}
		})
	}
}

func TestStatsExactFieldSuggestionsAdvertiseQuotedFieldInsertion(t *testing.T) {
	t.Parallel()

	for _, source := range []string{
		`| stats avg(`,
		`| stats sparkline(avg(`,
		`| stats count BY `,
	} {
		context, diagnostic := AnalyzeSuggestionContext(source, len(source))
		if diagnostic != nil {
			t.Fatalf("AnalyzeSuggestionContext(%q): %v", source, diagnostic)
		}
		if !context.Allows(SuggestionKindField) ||
			!context.AllowsQuotedScalarFields {
			t.Errorf("context for %q = %#v", source, context)
		}
	}

	aliasSource := `| stats avg(value) AS `
	alias, diagnostic := AnalyzeSuggestionContext(aliasSource, len(aliasSource))
	if diagnostic != nil {
		t.Fatalf("alias context: %v", diagnostic)
	}
	if !alias.Allows(SuggestionKindField) || alias.AllowsQuotedScalarFields {
		t.Fatalf("alias context = %#v", alias)
	}

	result := Suggest(`| stats avg(`, len(`| stats avg(`), 20)
	// The catalog has no dynamic field candidates here; the context lets the
	// service safely quote authorized field candidates when it adds them.
	if result.Diagnostic != nil || !result.Context.AllowsQuotedScalarFields {
		t.Fatalf("suggestion result = %#v", result)
	}

	tableSource := `| stats count AS ".com" | table `
	tableContext, diagnostic := AnalyzeSuggestionContext(
		tableSource,
		len(tableSource),
	)
	if diagnostic != nil || !tableContext.Allows(SuggestionKindField) ||
		!tableContext.AllowsQuotedScalarFields {
		t.Fatalf("table literal-reference context = %#v, %v", tableContext, diagnostic)
	}
}
