package plan

import (
	"errors"
	"slices"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/spl"
)

func TestBuildStatsQuotedNamesPreserveDecodedFieldsAndLiteralOutputs(t *testing.T) {
	t.Parallel()

	logical, err := Build(
		mustParse(t, `index=gradethis | stats avg('Product Name') AS "Revenue" sparkline(sum('request-bytes')) AS ".com" BY 'HTTP Status'`),
		testScope([]string{"gradethis"}, nil),
	)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	aggregate := logical.Operators[len(logical.Operators)-1].(*Aggregate)
	if len(aggregate.Measures) != 2 || len(aggregate.GroupBy) != 1 ||
		aggregate.GroupBy[0].Name != "HTTP Status" ||
		aggregate.Measures[0].Input.Name != "Product Name" ||
		aggregate.Measures[0].Output != "Revenue" ||
		!aggregate.Measures[0].OutputLiteral ||
		aggregate.Measures[1].Sparkline == nil ||
		aggregate.Measures[1].Sparkline.Input.Name != "request-bytes" ||
		aggregate.Measures[1].Output != ".com" ||
		!aggregate.Measures[1].OutputLiteral {
		t.Fatalf("aggregate = %#v", aggregate)
	}
	if !slices.Equal(logical.OutputFields, []string{"HTTP Status", "Revenue", ".com"}) {
		t.Fatalf("output fields = %#v", logical.OutputFields)
	}
	if _, analyzeErr := Analyze(logical); analyzeErr != nil {
		t.Fatalf("Analyze: %v", analyzeErr)
	}
}

func TestBuildStatsImplicitEvalOutputsUseLiteralSourceNames(t *testing.T) {
	t.Parallel()

	const source = `index=gradethis | stats SuM(eval(bytes * price)) count(eval(status=500)) values(eval(isnull(flag)))`
	logical, err := Build(
		mustParse(t, source),
		testScope([]string{"gradethis"}, nil),
	)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	aggregate := logical.Operators[len(logical.Operators)-1].(*Aggregate)
	want := []string{
		`SuM(eval(bytes * price))`,
		`count(eval(status=500))`,
		`values(eval(isnull(flag)))`,
	}
	if !slices.Equal(logical.OutputFields, want) {
		t.Fatalf("outputs = %#v, want %#v", logical.OutputFields, want)
	}
	for index, measure := range aggregate.Measures {
		if measure.Output != want[index] || !measure.OutputLiteral {
			t.Errorf("measure[%d] = %#v", index, measure)
		}
	}
	if aggregate.Measures[0].InputExpression == nil ||
		aggregate.Measures[1].Predicate == nil ||
		aggregate.Measures[2].InputExpression == nil {
		t.Fatalf("eval measures = %#v", aggregate.Measures)
	}
	if _, analyzeErr := Analyze(logical); analyzeErr != nil {
		t.Fatalf("Analyze: %v", analyzeErr)
	}

	multiline, err := Build(
		mustParse(t, "index=gradethis | stats sum(eval(value\t+\n1))"),
		testScope([]string{"gradethis"}, nil),
	)
	if err != nil {
		t.Fatalf("Build multiline implicit alias: %v", err)
	}
	measure := multiline.Operators[len(multiline.Operators)-1].(*Aggregate).Measures[0]
	if measure.Output != "sum(eval(value + 1))" || !measure.OutputLiteral {
		t.Fatalf("multiline measure = %#v", measure)
	}
}

func TestBuildStatsLiteralOutputsRemainReferenceableDownstream(t *testing.T) {
	t.Parallel()

	logical, err := Build(
		mustParse(t, `index=gradethis | stats count AS ".com" avg(value) AS "Product Name" | where isnotnull('.com') | table '.com', 'Product Name'`),
		testScope([]string{"gradethis"}, nil),
	)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if !slices.Equal(logical.OutputFields, []string{".com", "Product Name"}) {
		t.Fatalf("outputs = %#v", logical.OutputFields)
	}
	project, ok := logical.Operators[len(logical.Operators)-1].(*Project)
	if !ok || len(project.Fields) != 2 || project.Fields[0].Name != ".com" ||
		len(project.Fields[0].Path) != 1 || project.Fields[0].Path[0] != ".com" ||
		project.Fields[1].Name != "Product Name" {
		t.Fatalf("project = %#v", logical.Operators[len(logical.Operators)-1])
	}
	if _, analyzeErr := Analyze(logical); analyzeErr != nil {
		t.Fatalf("Analyze: %v", analyzeErr)
	}
}

func TestAnalyzeStatsOnlyMetadataRequiresStatsOptions(t *testing.T) {
	t.Parallel()

	for _, measure := range []AggregateMeasure{
		{Function: AggregateFunctionCountRows, Output: ".com", OutputLiteral: true},
		{Sparkline: &SparklineMeasure{}, Output: "sparkline"},
	} {
		if _, err := Analyze(&Query{Operators: []Operator{&Aggregate{
			Measures: []AggregateMeasure{measure},
		}}}); err == nil {
			t.Fatalf("Analyze accepted stats-only measure without options: %#v", measure)
		}
	}
}

func TestBuildRelatedTransformsRejectForgedQuotedGroupProvenance(t *testing.T) {
	t.Parallel()

	base := mustParse(t, `index=gradethis`)
	sourceRange := spl.Range{
		Start: spl.Position{Line: 1, Column: 1},
		End:   spl.Position{Offset: 5, Line: 1, Column: 6},
	}
	chart := &spl.ChartCommand{
		Aggregate: spl.StatsAggregate{
			Function: spl.AggregateFunctionCount,
			Alias:    "count", Range: sourceRange, AliasRange: sourceRange,
		},
		Over:    spl.StatsGroupField{Name: "host", Quoted: true, Range: sourceRange},
		SplitBy: spl.StatsGroupField{Name: "source", Range: sourceRange},
		Range:   sourceRange,
	}
	_, err := Build(&spl.Query{
		Search: base.Search, Commands: []spl.Command{chart}, Range: base.Range,
	}, testScope([]string{"gradethis"}, nil))
	assertDiagnosticCode(t, err, "SPL_UNSUPPORTED_CHART_FIELD_TYPE")

	timechart := mustParse(t, `index=gradethis | timechart span=5m count BY host`)
	command := timechart.Commands[0].(*spl.TimechartCommand)
	command.SplitBy.Quoted = true
	_, err = Build(timechart, testScope([]string{"gradethis"}, nil))
	assertDiagnosticCode(t, err, "SPL_UNSUPPORTED_TIMECHART_FIELD_TYPE")
}

func TestBuildStatsRejectsSameSourceAggregateRenamedTwice(t *testing.T) {
	t.Parallel()

	for _, source := range []string{
		`index=gradethis | stats first(host) AS site first(host) AS report`,
		`index=gradethis | stats c(host) AS a count(host) AS b`,
		`index=gradethis | stats dc(host) AS a distinct_count(host) AS b`,
		`index=gradethis | stats p95(value) AS a perc95(value) AS b`,
		`index=gradethis | stats avg(value) AS a mean(value) AS b`,
		`index=gradethis | stats first(host) AS a first('host') AS b`,
		`index=gradethis | stats sparkline(avg(value),5m) AS a sparkline(mean(value),5m) AS b`,
	} {
		_, err := Build(
			mustParse(t, source),
			testScope([]string{"gradethis"}, nil),
		)
		assertDiagnosticCode(t, err, "SPL_DUPLICATE_STATS_AGGREGATE")
	}
}

func TestBuildStatsDuplicateSourceBoundaryKeepsDistinctFunctionsAndEvalOracle(t *testing.T) {
	t.Parallel()

	// Official examples permit multiple different functions over one input.
	// Exact eval-source normalization is not specified and remains
	// O-alias-schema, so this slice conservatively does not conflate eval terms.
	for _, source := range []string{
		`index=gradethis | stats max(mag) min(mag) range(mag) avg(mag) BY magType`,
		`index=gradethis | stats first(host) AS first_host first(source) AS first_source`,
		`index=gradethis | stats sum(eval(value+1)) AS a sum(eval(value+1)) AS b`,
		`index=gradethis | stats count(eval(status=500)) AS a count(eval(status=500)) AS b`,
		`index=gradethis | stats sparkline(avg(value),5m) AS a sparkline(avg(value),10m) AS b`,
	} {
		logical, err := Build(
			mustParse(t, source),
			testScope([]string{"gradethis"}, nil),
		)
		if err != nil {
			t.Fatalf("Build(%q): %v", source, err)
		}
		if _, analyzeErr := Analyze(logical); analyzeErr != nil {
			t.Fatalf("Analyze(%q): %v", source, analyzeErr)
		}
	}
}

func TestBuildStatsRejectsForgedQuotedAndSourceAliasProvenance(t *testing.T) {
	t.Parallel()

	base := mustParse(t, `index=gradethis`)
	sourceRange := spl.Range{
		Start: spl.Position{Line: 1, Column: 1},
		End:   spl.Position{Offset: 5, Line: 1, Column: 6},
	}
	validExpression := func() spl.ScalarExpr {
		return &spl.ScalarFieldExpr{Field: "value", Range: sourceRange}
	}
	validField := func() spl.StatsAggregate {
		return spl.StatsAggregate{
			Function:      spl.AggregateFunctionAverage,
			Input:         "value",
			InputRange:    sourceRange,
			Alias:         "result",
			ExplicitAlias: true,
			Range:         sourceRange,
			AliasRange:    sourceRange,
		}
	}
	validEval := func() spl.StatsAggregate {
		return spl.StatsAggregate{
			Function:           spl.AggregateFunctionSum,
			InputExpression:    validExpression(),
			Alias:              "sum(eval(value))",
			AliasSourceDerived: true,
			Range:              sourceRange,
			AliasRange:         sourceRange,
		}
	}
	tests := []struct {
		name      string
		aggregate func() spl.StatsAggregate
		mutate    func(*spl.StatsAggregate)
	}{
		{name: "quoted bit on invalid input", aggregate: validField, mutate: func(value *spl.StatsAggregate) {
			value.Input, value.InputQuoted = "__os_private", true
		}},
		{name: "unquoted input cannot contain spaces", aggregate: validField, mutate: func(value *spl.StatsAggregate) {
			value.Input = "Product Name"
		}},
		{name: "quoted alias requires explicit AS", aggregate: validField, mutate: func(value *spl.StatsAggregate) {
			value.Alias, value.AliasQuoted, value.ExplicitAlias = "Revenue", true, false
		}},
		{name: "quoted alias cannot be private", aggregate: validField, mutate: func(value *spl.StatsAggregate) {
			value.Alias, value.AliasQuoted = "__os_private", true
		}},
		{name: "source-derived bit on field input", aggregate: validField, mutate: func(value *spl.StatsAggregate) {
			value.AliasSourceDerived, value.ExplicitAlias = true, false
		}},
		{name: "implicit eval missing source bit", aggregate: validEval, mutate: func(value *spl.StatsAggregate) {
			value.AliasSourceDerived = false
		}},
		{name: "source range mismatch", aggregate: validEval, mutate: func(value *spl.StatsAggregate) {
			value.AliasRange.End.Offset++
		}},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			aggregate := test.aggregate()
			test.mutate(&aggregate)
			query := &spl.Query{
				Search: base.Search,
				Commands: []spl.Command{&spl.StatsCommand{
					Aggregates: []spl.StatsAggregate{aggregate},
					Range:      sourceRange,
				}},
				Range: base.Range,
			}
			_, err := Build(query, testScope([]string{"gradethis"}, nil))
			if err == nil {
				t.Fatal("Build succeeded for forged provenance")
			}
		})
	}
}

func TestAnalyzeStatsRevalidatesLiteralOutputsAndDuplicateSources(t *testing.T) {
	t.Parallel()

	input := mustResolveEventAggregateField(t, "host")
	valid := &Aggregate{
		Measures: []AggregateMeasure{
			{Function: AggregateFunctionFirst, Input: input, Output: "site"},
			{Function: AggregateFunctionFirst, Input: input, Output: "report"},
		},
	}
	if _, err := Analyze(&Query{Operators: []Operator{valid}}); err == nil {
		t.Fatal("Analyze accepted one exact source aggregate renamed twice")
	}

	for _, output := range []string{"", "__os_private", "fields", "line\nbreak"} {
		operator := &Aggregate{Measures: []AggregateMeasure{{
			Function:      AggregateFunctionCountRows,
			Output:        output,
			OutputLiteral: true,
		}}}
		if _, err := Analyze(&Query{Operators: []Operator{operator}}); err == nil {
			t.Errorf("Analyze accepted literal output %q", output)
		}
	}

	operator := &EventAggregate{Measure: AggregateMeasure{
		Function:      AggregateFunctionCountRows,
		Output:        ".com",
		OutputLiteral: true,
	}}
	if _, err := Analyze(&Query{Operators: []Operator{operator}}); err == nil {
		t.Fatal("Analyze accepted stats-only literal provenance on eventstats")
	}
}

func TestBuildStatsQuotedDiagnosticsRemainSourceLocated(t *testing.T) {
	t.Parallel()

	const source = `index=gradethis | stats count AS "__os_private"`
	query, parseErr := spl.Parse(source)
	if query != nil || parseErr == nil {
		t.Fatalf("Parse = %#v, %v", query, parseErr)
	}
	var diagnostic *spl.Diagnostic
	if !errors.As(parseErr, &diagnostic) ||
		diagnostic.Code != "SPL_UNSUPPORTED_STATS_AGGREGATE" ||
		source[diagnostic.Range.Start.Offset:diagnostic.Range.End.Offset] != `"__os_private"` {
		t.Fatalf("diagnostic = %#v", parseErr)
	}
}
