package plan

import (
	"errors"
	"slices"
	"strconv"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/spl"
)

func TestBuildChartPercentileProducesBoundedDynamicPivot(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		percentile uint8
		input      string
		inputPath  []string
		row        string
		column     string
	}{
		{
			name:       "lower bound",
			percentile: 1,
			input:      "latency",
			inputPath:  []string{"latency"},
			row:        "path",
			column:     "service",
		},
		{
			name:       "dotted input",
			percentile: 95,
			input:      "http.duration",
			inputPath:  []string{"http", "duration"},
			row:        "endpoint",
			column:     "region",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			query, command := chartPercentileAST(
				t,
				test.percentile,
				test.input,
				test.row,
				test.column,
			)
			logical, err := Build(query, testScope([]string{"gradethis"}, nil))
			if err != nil {
				t.Fatalf("Build: %v", err)
			}
			operator, ok := logical.Operators[len(logical.Operators)-1].(*Chart)
			if !ok {
				t.Fatalf("last operator = %T, want *Chart", logical.Operators[len(logical.Operators)-1])
			}
			if operator.Over.Name != test.row || operator.SplitBy.Name != test.column ||
				operator.Measure.Function != AggregateFunctionPercentile ||
				operator.Measure.Input.Name != test.input ||
				!slices.Equal(operator.Measure.Input.Path, test.inputPath) ||
				operator.Measure.Input.Canonical ||
				operator.Measure.Percentile != test.percentile ||
				operator.Measure.Output != command.Aggregate.Alias ||
				operator.Measure.Predicate != nil ||
				operator.RowLimit != maxChartRows ||
				operator.SeriesLimit != chartSeriesLimit ||
				!operator.IncludeNull || !operator.IncludeOther ||
				operator.NullLabel != "NULL" || operator.OtherLabel != "OTHER" {
				t.Fatalf("chart = %#v", operator)
			}
			if len(logical.OutputFields) != 0 || logical.DynamicOutput == nil ||
				!slices.Equal(logical.DynamicOutput.FixedFields, []string{test.row}) ||
				logical.DynamicOutput.MaxSeries != maxChartSeries {
				t.Fatalf("output = %v/%#v", logical.OutputFields, logical.DynamicOutput)
			}

			analysis, analyzeErr := Analyze(logical)
			if analyzeErr != nil {
				t.Fatalf("Analyze: %v", analyzeErr)
			}
			wantReferenced := []string{"index", test.input, test.row, test.column}
			slices.Sort(wantReferenced)
			if !slices.Equal(analysis.ReferencedFields, wantReferenced) {
				t.Fatalf("referenced fields = %v, want %v", analysis.ReferencedFields, wantReferenced)
			}
		})
	}
}

func TestBuildChartPercentileAxisCollisionsFollowPivotSemantics(t *testing.T) {
	t.Parallel()

	rowCollision, command := chartPercentileAST(t, 95, "latency", "path", "service")
	command.Over.Name = command.Aggregate.Input
	_, err := Build(rowCollision, testScope([]string{"gradethis"}, nil))
	assertDiagnosticCode(t, err, "SPL_DUPLICATE_FIELD")

	columnMeasure, _ := chartPercentileAST(t, 50, "service", "endpoint", "service")
	logical, err := Build(columnMeasure, testScope([]string{"gradethis"}, nil))
	if err != nil {
		t.Fatalf("Build(measure equal to column): %v", err)
	}
	operator := logical.Operators[len(logical.Operators)-1].(*Chart)
	if operator.Measure.Input.Name != operator.SplitBy.Name {
		t.Fatalf("measure/column = %#v/%#v", operator.Measure.Input, operator.SplitBy)
	}
}

func TestBuildChartPercentileRemainsTerminal(t *testing.T) {
	t.Parallel()

	for _, source := range []string{
		`index=gradethis | chart p95(latency) OVER path BY service | head 5`,
		`index=gradethis | chart perc50(latency) BY path, service | table path`,
	} {
		query, err := spl.Parse(source)
		if err != nil {
			t.Fatalf("Parse(%q): %v", source, err)
		}
		_, err = Build(query, testScope([]string{"gradethis"}, nil))
		assertDiagnosticCode(t, err, "SPL_UNSUPPORTED_CHART_PIPELINE")
	}
}

func TestBuildRejectsForgedChartPercentileContracts(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		mutate   func(*spl.ChartCommand)
		wantCode string
	}{
		{"zero percentile", func(command *spl.ChartCommand) { command.Aggregate.Percentile = 0 }, "SPL_UNSUPPORTED_CHART_AGGREGATE"},
		{"percentile above range", func(command *spl.ChartCommand) { command.Aggregate.Percentile = 100 }, "SPL_UNSUPPORTED_CHART_AGGREGATE"},
		{"missing input", func(command *spl.ChartCommand) { command.Aggregate.Input = "" }, "SPL_UNSUPPORTED_CHART_AGGREGATE"},
		{"missing input range", func(command *spl.ChartCommand) { command.Aggregate.InputRange = spl.Range{} }, "SPL_UNSUPPORTED_CHART_AGGREGATE"},
		{"missing aggregate range", func(command *spl.ChartCommand) { command.Aggregate.Range = spl.Range{} }, "SPL_UNSUPPORTED_CHART_AGGREGATE"},
		{"missing alias range", func(command *spl.ChartCommand) { command.Aggregate.AliasRange = spl.Range{} }, "SPL_UNSUPPORTED_CHART_AGGREGATE"},
		{"predicate metadata", func(command *spl.ChartCommand) { command.Aggregate.Predicate = &spl.WhereComparisonExpr{} }, "SPL_UNSUPPORTED_CHART_AGGREGATE"},
		{"explicit alias", func(command *spl.ChartCommand) { command.Aggregate.ExplicitAlias = true }, "SPL_UNSUPPORTED_CHART_AGGREGATE"},
		{"forged canonical output", func(command *spl.ChartCommand) { command.Aggregate.Alias = "p95(latency)" }, "SPL_UNSUPPORTED_CHART_AGGREGATE"},
		{"non-percentile function carries level", func(command *spl.ChartCommand) { command.Aggregate.Function = spl.AggregateFunctionSum }, "SPL_UNSUPPORTED_CHART_AGGREGATE"},
		{"private input", func(command *spl.ChartCommand) {
			command.Aggregate.Input = "__os_private"
			command.Aggregate.Alias = "perc95(__os_private)"
		}, "SPL_RESERVED_FIELD"},
		{"duplicate axes", func(command *spl.ChartCommand) { command.SplitBy.Name = command.Over.Name }, "SPL_DUPLICATE_FIELD"},
		{"measure equals row", func(command *spl.ChartCommand) { command.Over.Name = command.Aggregate.Input }, "SPL_DUPLICATE_FIELD"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			query, command := chartPercentileAST(t, 95, "latency", "path", "service")
			test.mutate(command)
			_, err := Build(query, testScope([]string{"gradethis"}, nil))
			assertDiagnosticCode(t, err, test.wantCode)
		})
	}
}

func TestBuildChartPercentilePreservesReservedFieldsBoundary(t *testing.T) {
	t.Parallel()

	openQuery, _ := chartPercentileAST(t, 95, "fields", "path", "service")
	_, err := Build(openQuery, testScope([]string{"gradethis"}, nil))
	assertDiagnosticCode(t, err, "SPL_AMBIGUOUS_CHART_FIELD")

	closedQuery := mustParse(
		t,
		`index=gradethis | table path fields service | chart sum(fields) OVER path BY service`,
	)
	closedCommand := closedQuery.Commands[len(closedQuery.Commands)-1].(*spl.ChartCommand)
	closedCommand.Aggregate.Function = spl.AggregateFunctionPercentile
	closedCommand.Aggregate.Percentile = 95
	closedCommand.Aggregate.Alias = "perc95(fields)"
	logical, err := Build(closedQuery, testScope([]string{"gradethis"}, nil))
	if err != nil {
		t.Fatalf("Build(exact fields schema): %v", err)
	}
	operator := logical.Operators[len(logical.Operators)-1].(*Chart)
	if operator.Measure.Input.Name != "fields" {
		t.Fatalf("exact fields input = %#v", operator.Measure.Input)
	}
}

func TestAnalyzeAcceptsValidAndRejectsForgedChartPercentileMeasures(t *testing.T) {
	t.Parallel()

	row := mustResolveEventAggregateField(t, "path")
	column := mustResolveEventAggregateField(t, "service")
	input := mustResolveEventAggregateField(t, "latency")
	validChart := func() *Chart {
		return &Chart{
			Over:    row,
			SplitBy: column,
			Measure: AggregateMeasure{
				Function:   AggregateFunctionPercentile,
				Input:      input,
				Percentile: 95,
				Output:     "perc95(latency)",
			},
			RowLimit:     maxChartRows,
			SeriesLimit:  chartSeriesLimit,
			IncludeNull:  true,
			IncludeOther: true,
			NullLabel:    "NULL",
			OtherLabel:   "OTHER",
		}
	}

	if _, err := Analyze(&Query{Operators: []Operator{validChart()}}); err != nil {
		t.Fatalf("Analyze(valid percentile chart): %v", err)
	}
	columnMeasure := validChart()
	columnMeasure.Measure.Input = column
	columnMeasure.Measure.Output = "perc95(service)"
	if _, err := Analyze(&Query{Operators: []Operator{columnMeasure}}); err != nil {
		t.Fatalf("Analyze(valid measure equal to column): %v", err)
	}

	tests := []struct {
		name   string
		mutate func(*Chart)
	}{
		{"invalid function", func(operator *Chart) { operator.Measure.Function = AggregateFunctionInvalid }},
		{"unsupported function", func(operator *Chart) { operator.Measure.Function = AggregateFunctionMaximum }},
		{"zero percentile", func(operator *Chart) { operator.Measure.Percentile = 0 }},
		{"percentile above range", func(operator *Chart) { operator.Measure.Percentile = 100 }},
		{"missing input", func(operator *Chart) { operator.Measure.Input = FieldRef{} }},
		{"forged input path", func(operator *Chart) { operator.Measure.Input.Path = []string{"attacker"} }},
		{"canonical input", func(operator *Chart) { operator.Measure.Input.Canonical = true }},
		{"predicate metadata", func(operator *Chart) { operator.Measure.Predicate = &ComparisonExpression{} }},
		{"wrong canonical output", func(operator *Chart) { operator.Measure.Output = "p95(latency)" }},
		{"duplicate axes", func(operator *Chart) { operator.SplitBy = operator.Over }},
		{"measure equals row", func(operator *Chart) {
			operator.Measure.Input = operator.Over
			operator.Measure.Output = "perc95(path)"
		}},
		{"zero row bound", func(operator *Chart) { operator.RowLimit = 0 }},
		{"wrong row bound", func(operator *Chart) { operator.RowLimit++ }},
		{"zero series bound", func(operator *Chart) { operator.SeriesLimit = 0 }},
		{"wrong series bound", func(operator *Chart) { operator.SeriesLimit++ }},
		{"null series disabled", func(operator *Chart) { operator.IncludeNull = false }},
		{"other series disabled", func(operator *Chart) { operator.IncludeOther = false }},
		{"null label renamed", func(operator *Chart) { operator.NullLabel = "none" }},
		{"other label renamed", func(operator *Chart) { operator.OtherLabel = "rest" }},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			operator := validChart()
			test.mutate(operator)
			_, err := Analyze(&Query{Operators: []Operator{operator}})
			if err == nil {
				t.Fatal("Analyze succeeded, want forged percentile chart rejection")
			}
		})
	}
}

func TestBuildChartPercentileDiagnosticRangeIsAggregate(t *testing.T) {
	t.Parallel()

	query, command := chartPercentileAST(t, 95, "latency", "path", "service")
	command.Aggregate.Percentile = 0
	_, err := Build(query, testScope([]string{"gradethis"}, nil))
	var diagnostic *Diagnostic
	if !errors.As(err, &diagnostic) {
		t.Fatalf("Build error = %#v, want diagnostic", err)
	}
	if diagnostic.Range != command.Aggregate.Range {
		t.Fatalf("diagnostic range = %#v, want aggregate %#v", diagnostic.Range, command.Aggregate.Range)
	}
}

func chartPercentileAST(
	t *testing.T,
	percentile uint8,
	input string,
	row string,
	column string,
) (*spl.Query, *spl.ChartCommand) {
	t.Helper()

	query := mustParse(
		t,
		`index=gradethis | chart sum(latency) OVER path BY service`,
	)
	command := query.Commands[0].(*spl.ChartCommand)
	command.Aggregate.Function = spl.AggregateFunctionPercentile
	command.Aggregate.Percentile = percentile
	command.Aggregate.Input = input
	command.Aggregate.Alias = percentileChartOutput(percentile, input)
	command.Over.Name = row
	command.SplitBy.Name = column
	return query, command
}

func percentileChartOutput(percentile uint8, input string) string {
	return "perc" + strconv.Itoa(int(percentile)) + "(" + input + ")"
}
