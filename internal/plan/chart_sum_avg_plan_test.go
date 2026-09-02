package plan

import (
	"errors"
	"reflect"
	"slices"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/spl"
)

func TestBuildChartSumAndAverageResolvesNumericMeasureAndBoundedOutput(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		source    string
		function  AggregateFunction
		input     string
		inputPath []string
		output    string
		row       string
		column    string
	}{
		{
			name:      "sum OVER BY",
			source:    `index=gradethis | chart sum(bytes) OVER path BY status_class`,
			function:  AggregateFunctionSum,
			input:     "bytes",
			inputPath: []string{"bytes"},
			output:    "sum(bytes)",
			row:       "path",
			column:    "status_class",
		},
		{
			name:      "avg comma-separated BY",
			source:    `index=gradethis | chart avg(http.duration) BY service, region`,
			function:  AggregateFunctionAverage,
			input:     "http.duration",
			inputPath: []string{"http", "duration"},
			output:    "avg(http.duration)",
			row:       "service",
			column:    "region",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			logical, err := Build(
				mustParse(t, test.source),
				testScope([]string{"gradethis"}, nil),
			)
			if err != nil {
				t.Fatalf("Build: %v", err)
			}
			operator, ok := logical.Operators[len(logical.Operators)-1].(*Chart)
			if !ok {
				t.Fatalf("last operator = %T, want *Chart", logical.Operators[len(logical.Operators)-1])
			}
			if operator.Over.Name != test.row || operator.SplitBy.Name != test.column ||
				operator.Measure.Function != test.function ||
				operator.Measure.Input.Name != test.input ||
				!slices.Equal(operator.Measure.Input.Path, test.inputPath) ||
				operator.Measure.Input.Canonical || operator.Measure.Output != test.output ||
				operator.Measure.Predicate != nil || operator.Measure.Percentile != 0 ||
				operator.RowLimit != maxChartRows || operator.SeriesLimit != chartSeriesLimit ||
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

func TestBuildChartNumericMeasureAxisCollisionsFollowPivotSemantics(t *testing.T) {
	t.Parallel()

	for _, function := range []string{"sum", "avg"} {
		_, err := Build(
			mustParse(t, `index=gradethis | chart `+function+`(bytes) OVER bytes BY service`),
			testScope([]string{"gradethis"}, nil),
		)
		assertDiagnosticCode(t, err, "SPL_DUPLICATE_FIELD")

		logical, buildErr := Build(
			mustParse(t, `index=gradethis | chart `+function+`(service) OVER endpoint BY service`),
			testScope([]string{"gradethis"}, nil),
		)
		if buildErr != nil {
			t.Fatalf("Build(%s measure equal to column): %v", function, buildErr)
		}
		operator := logical.Operators[len(logical.Operators)-1].(*Chart)
		if operator.Measure.Input.Name != operator.SplitBy.Name {
			t.Fatalf("%s measure/column = %#v/%#v", function, operator.Measure.Input, operator.SplitBy)
		}
	}
}

func TestBuildChartSumAndAverageRemainsTerminal(t *testing.T) {
	t.Parallel()

	for _, source := range []string{
		`index=gradethis | chart sum(bytes) OVER path BY level | head 5`,
		`index=gradethis | chart avg(latency) BY path, level | table path`,
	} {
		_, err := Build(mustParse(t, source), testScope([]string{"gradethis"}, nil))
		assertDiagnosticCode(t, err, "SPL_UNSUPPORTED_CHART_PIPELINE")
	}
}

func TestBuildRejectsForgedChartNumericAggregateContracts(t *testing.T) {
	t.Parallel()

	base := mustParse(t, `index=gradethis`)
	fieldRange := spl.Range{
		Start: spl.Position{Offset: 1, Line: 1, Column: 2},
		End:   spl.Position{Offset: 8, Line: 1, Column: 9},
	}
	valid := spl.StatsAggregate{
		Function:   spl.AggregateFunctionSum,
		Input:      "latency",
		InputRange: fieldRange,
		Alias:      "sum(latency)",
		Range:      fieldRange,
		AliasRange: fieldRange,
	}
	validCommand := func() *spl.ChartCommand {
		return &spl.ChartCommand{
			Aggregate: valid,
			Over:      spl.StatsGroupField{Name: "path", Range: fieldRange},
			SplitBy:   spl.StatsGroupField{Name: "service", Range: fieldRange},
			Range:     fieldRange,
		}
	}

	tests := []struct {
		name     string
		mutate   func(*spl.ChartCommand)
		wantCode string
	}{
		{"invalid function", func(command *spl.ChartCommand) { command.Aggregate.Function = spl.AggregateFunctionInvalid }, "SPL_UNSUPPORTED_CHART_AGGREGATE"},
		{"unsupported function", func(command *spl.ChartCommand) { command.Aggregate.Function = spl.AggregateFunctionMaximum }, "SPL_UNSUPPORTED_CHART_AGGREGATE"},
		{"missing input", func(command *spl.ChartCommand) { command.Aggregate.Input = "" }, "SPL_UNSUPPORTED_CHART_AGGREGATE"},
		{"missing input range", func(command *spl.ChartCommand) { command.Aggregate.InputRange = spl.Range{} }, "SPL_UNSUPPORTED_CHART_AGGREGATE"},
		{"missing aggregate range", func(command *spl.ChartCommand) { command.Aggregate.Range = spl.Range{} }, "SPL_UNSUPPORTED_CHART_AGGREGATE"},
		{"missing alias range", func(command *spl.ChartCommand) { command.Aggregate.AliasRange = spl.Range{} }, "SPL_UNSUPPORTED_CHART_AGGREGATE"},
		{"percentile metadata", func(command *spl.ChartCommand) { command.Aggregate.Percentile = 95 }, "SPL_UNSUPPORTED_CHART_AGGREGATE"},
		{"predicate metadata", func(command *spl.ChartCommand) { command.Aggregate.Predicate = &spl.WhereComparisonExpr{} }, "SPL_UNSUPPORTED_CHART_AGGREGATE"},
		{"explicit alias", func(command *spl.ChartCommand) { command.Aggregate.ExplicitAlias = true }, "SPL_UNSUPPORTED_CHART_AGGREGATE"},
		{"forged canonical output", func(command *spl.ChartCommand) { command.Aggregate.Alias = "total" }, "SPL_UNSUPPORTED_CHART_AGGREGATE"},
		{"multiple-token input", func(command *spl.ChartCommand) {
			command.Aggregate.Input = "latency host"
			command.Aggregate.Alias = "sum(latency host)"
		}, "SPL_UNSUPPORTED_CHART_AGGREGATE"},
		{"quoted input", func(command *spl.ChartCommand) {
			command.Aggregate.Input = "\"latency\""
			command.Aggregate.Alias = "sum(\"latency\")"
		}, "SPL_UNSUPPORTED_CHART_AGGREGATE"},
		{"private input", func(command *spl.ChartCommand) {
			command.Aggregate.Input = "__os_private"
			command.Aggregate.Alias = "sum(__os_private)"
		}, "SPL_RESERVED_FIELD"},
		{"duplicate axes", func(command *spl.ChartCommand) { command.SplitBy.Name = command.Over.Name }, "SPL_DUPLICATE_FIELD"},
		{"measure equals row", func(command *spl.ChartCommand) { command.Over.Name = command.Aggregate.Input }, "SPL_DUPLICATE_FIELD"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			command := validCommand()
			test.mutate(command)
			query := &spl.Query{
				Search:   base.Search,
				Commands: []spl.Command{command},
				Range:    base.Range,
			}
			_, err := Build(query, testScope([]string{"gradethis"}, nil))
			assertDiagnosticCode(t, err, test.wantCode)
		})
	}

	columnMeasure := validCommand()
	columnMeasure.Aggregate.Input = columnMeasure.SplitBy.Name
	columnMeasure.Aggregate.Alias = "sum(" + columnMeasure.SplitBy.Name + ")"
	if _, err := Build(
		&spl.Query{Search: base.Search, Commands: []spl.Command{columnMeasure}, Range: base.Range},
		testScope([]string{"gradethis"}, nil),
	); err != nil {
		t.Fatalf("Build(forged valid measure equal to column): %v", err)
	}
}

func TestAnalyzeRejectsForgedChartNumericMeasureAndBounds(t *testing.T) {
	t.Parallel()

	row := mustResolveEventAggregateField(t, "path")
	column := mustResolveEventAggregateField(t, "service")
	input := mustResolveEventAggregateField(t, "latency")
	validChart := func() *Chart {
		return &Chart{
			Over:    row,
			SplitBy: column,
			Measure: AggregateMeasure{
				Function: AggregateFunctionSum,
				Input:    input,
				Output:   "sum(latency)",
			},
			RowLimit:     maxChartRows,
			SeriesLimit:  chartSeriesLimit,
			IncludeNull:  true,
			IncludeOther: true,
			NullLabel:    "NULL",
			OtherLabel:   "OTHER",
		}
	}

	for _, function := range []AggregateFunction{AggregateFunctionSum, AggregateFunctionAverage} {
		operator := validChart()
		operator.Measure.Function = function
		if function == AggregateFunctionAverage {
			operator.Measure.Output = "avg(latency)"
		}
		if _, err := Analyze(&Query{Operators: []Operator{operator}}); err != nil {
			t.Fatalf("Analyze(valid %v chart): %v", function, err)
		}
	}

	columnMeasure := validChart()
	columnMeasure.Measure.Input = column
	columnMeasure.Measure.Output = "sum(service)"
	if _, err := Analyze(&Query{Operators: []Operator{columnMeasure}}); err != nil {
		t.Fatalf("Analyze(valid measure equal to column): %v", err)
	}
	multipleTokenInput := mustResolveEventAggregateField(t, "latency host")
	quotedInput := mustResolveEventAggregateField(t, "\"latency\"")

	tests := []struct {
		name   string
		mutate func(*Chart)
	}{
		{"invalid function", func(operator *Chart) { operator.Measure.Function = AggregateFunctionInvalid }},
		{"unsupported function", func(operator *Chart) { operator.Measure.Function = AggregateFunctionMaximum }},
		{"missing input", func(operator *Chart) { operator.Measure.Input = FieldRef{} }},
		{"forged input path", func(operator *Chart) { operator.Measure.Input.Path = []string{"attacker"} }},
		{"canonical input", func(operator *Chart) { operator.Measure.Input.Canonical = true }},
		{"percentile metadata", func(operator *Chart) { operator.Measure.Percentile = 95 }},
		{"predicate metadata", func(operator *Chart) { operator.Measure.Predicate = &ComparisonExpression{} }},
		{"wrong canonical output", func(operator *Chart) { operator.Measure.Output = "total" }},
		{"multiple-token input", func(operator *Chart) {
			operator.Measure.Input = multipleTokenInput
			operator.Measure.Output = "sum(latency host)"
		}},
		{"quoted input", func(operator *Chart) {
			operator.Measure.Input = quotedInput
			operator.Measure.Output = "sum(\"latency\")"
		}},
		{"duplicate axes", func(operator *Chart) { operator.SplitBy = operator.Over }},
		{"measure equals row", func(operator *Chart) { operator.Measure.Input = operator.Over; operator.Measure.Output = "sum(path)" }},
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
				t.Fatal("Analyze succeeded, want forged chart rejection")
			}
		})
	}
}

func TestBuildChartNumericDiagnosticRangeIsAggregate(t *testing.T) {
	t.Parallel()

	query := mustParse(t, `index=gradethis | chart sum(latency) OVER path BY service`)
	command := query.Commands[0].(*spl.ChartCommand)
	command.Aggregate.Percentile = 95
	_, err := Build(query, testScope([]string{"gradethis"}, nil))
	var diagnostic *Diagnostic
	if !errors.As(err, &diagnostic) {
		t.Fatalf("Build error = %#v, want diagnostic", err)
	}
	if diagnostic.Range != command.Aggregate.Range {
		t.Fatalf("diagnostic range = %#v, want aggregate %#v", diagnostic.Range, command.Aggregate.Range)
	}
}

func TestBuildChartSingleSplitPlansAsStatsBy(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name, chart, stats string
	}{
		{"count BY", `index=gradethis | chart count BY host`, `index=gradethis | stats count BY host`},
		{"count OVER", `index=gradethis | chart count OVER host`, `index=gradethis | stats count BY host`},
		{"sum OVER", `index=gradethis | chart sum(bytes) OVER host`, `index=gradethis | stats sum(bytes) BY host`},
		{"percentile BY", `index=gradethis | chart p95(latency) BY service`, `index=gradethis | stats p95(latency) BY service`},
		{"after table", `index=gradethis | table host bytes | chart avg(bytes) BY host`, `index=gradethis | table host bytes | stats avg(bytes) BY host`},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			chart, err := Build(mustParse(t, test.chart), testScope([]string{"gradethis"}, nil))
			if err != nil {
				t.Fatalf("Build(%q): %v", test.chart, err)
			}
			stats, err := Build(mustParse(t, test.stats), testScope([]string{"gradethis"}, nil))
			if err != nil {
				t.Fatalf("Build(%q): %v", test.stats, err)
			}
			if chart.DynamicOutput != nil || !slices.Equal(chart.OutputFields, stats.OutputFields) {
				t.Fatalf("chart outputs = %v (dynamic %#v), want the stats outputs %v", chart.OutputFields, chart.DynamicOutput, stats.OutputFields)
			}
			chartAggregate, ok := chart.Operators[len(chart.Operators)-1].(*Aggregate)
			if !ok {
				t.Fatalf("chart last operator = %T, want *Aggregate", chart.Operators[len(chart.Operators)-1])
			}
			statsAggregate := stats.Operators[len(stats.Operators)-1].(*Aggregate)
			if len(chart.Operators) != len(stats.Operators) {
				t.Fatalf("chart operators = %#v, want the stats operator chain %#v", chart.Operators, stats.Operators)
			}
			for index := range chart.Operators[:len(chart.Operators)-1] {
				if reflect.TypeOf(chart.Operators[index]) != reflect.TypeOf(stats.Operators[index]) {
					t.Fatalf("chart operator %d = %T, want %T", index, chart.Operators[index], stats.Operators[index])
				}
			}
			// Source ranges differ between the two spellings; the group keys
			// and measures must not.
			if len(chartAggregate.GroupBy) != 1 || chartAggregate.GroupBy[0].Name != statsAggregate.GroupBy[0].Name ||
				len(chartAggregate.Measures) != 1 ||
				!reflect.DeepEqual(chartAggregate.StatsOptions, statsAggregate.StatsOptions) {
				t.Fatalf("chart aggregate = %#v, want the stats aggregate %#v", chartAggregate, statsAggregate)
			}
			chartMeasure, statsMeasure := chartAggregate.Measures[0], statsAggregate.Measures[0]
			chartMeasure.Input.Range, statsMeasure.Input.Range = spl.Range{}, spl.Range{}
			if !reflect.DeepEqual(chartMeasure, statsMeasure) {
				t.Fatalf("chart measure = %#v, want the stats measure %#v", chartMeasure, statsMeasure)
			}
		})
	}

	for _, test := range []struct {
		name, source, code string
	}{
		{"row field is the measure input", `index=gradethis | chart sum(bytes) BY bytes`, "SPL_DUPLICATE_FIELD"},
		{"reserved series name as row", `index=gradethis | chart count BY OTHER`, "SPL_UNSUPPORTED_CHART_FIELD_TYPE"},
		{"reserved payload on an open schema", `index=gradethis | chart count OVER fields`, "SPL_UNSUPPORTED_CHART_FIELD_TYPE"},
		{"not the final command", `index=gradethis | chart count BY host | table host`, "SPL_UNSUPPORTED_CHART_PIPELINE"},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			_, err := Build(mustParse(t, test.source), testScope([]string{"gradethis"}, nil))
			assertDiagnosticCode(t, err, test.code)
		})
	}
}
