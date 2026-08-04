package plan

import (
	"slices"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/spl"
)

func TestBuildChartCountFieldProducesBoundedOccurrencePivot(t *testing.T) {
	t.Parallel()

	logical, err := Build(
		mustParse(t, `index=gradethis | chart count(Http.Status) OVER endpoint BY service`),
		testScope([]string{"gradethis"}, nil),
	)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	operator, ok := logical.Operators[len(logical.Operators)-1].(*Chart)
	if !ok {
		t.Fatalf("last operator = %T, want *Chart", logical.Operators[len(logical.Operators)-1])
	}
	if operator.Measure.Function != AggregateFunctionCountValues ||
		operator.Measure.Input.Name != "Http.Status" ||
		!slices.Equal(operator.Measure.Input.Path, []string{"Http", "Status"}) ||
		operator.Measure.Input.Canonical || operator.Measure.Predicate != nil ||
		operator.Measure.Percentile != 0 ||
		operator.Measure.Output != "count(Http.Status)" ||
		operator.Over.Name != "endpoint" || operator.SplitBy.Name != "service" ||
		operator.RowLimit != maxChartRows || operator.SeriesLimit != chartSeriesLimit ||
		!operator.IncludeNull || !operator.IncludeOther ||
		operator.NullLabel != "NULL" || operator.OtherLabel != "OTHER" {
		t.Fatalf("chart = %#v", operator)
	}
	if len(logical.OutputFields) != 0 || logical.DynamicOutput == nil ||
		!slices.Equal(logical.DynamicOutput.FixedFields, []string{"endpoint"}) ||
		logical.DynamicOutput.MaxSeries != maxChartSeries {
		t.Fatalf("output = %v/%#v", logical.OutputFields, logical.DynamicOutput)
	}
	analysis, err := Analyze(logical)
	if err != nil {
		t.Fatalf("Analyze: %v", err)
	}
	if !slices.Equal(
		analysis.ReferencedFields,
		[]string{"Http.Status", "endpoint", "index", "service"},
	) {
		t.Fatalf("referenced fields = %v", analysis.ReferencedFields)
	}
}

func TestBuildChartCountFieldAxisAndPresenceBoundaries(t *testing.T) {
	t.Parallel()

	_, err := Build(
		mustParse(t, `index=gradethis | chart count(path) OVER path BY service`),
		testScope([]string{"gradethis"}, nil),
	)
	assertDiagnosticCode(t, err, "SPL_DUPLICATE_FIELD")

	logical, err := Build(
		mustParse(t, `index=gradethis | chart count(service) OVER path BY service`),
		testScope([]string{"gradethis"}, nil),
	)
	if err != nil {
		t.Fatalf("Build(measure equal to column): %v", err)
	}
	operator := logical.Operators[len(logical.Operators)-1].(*Chart)
	if operator.Measure.Input.Name != operator.SplitBy.Name {
		t.Fatalf("measure/column = %#v/%#v", operator.Measure.Input, operator.SplitBy)
	}

	projected, err := Build(
		mustParse(t, `index=gradethis | fields - status | chart count(status) OVER path BY service`),
		testScope([]string{"gradethis"}, nil),
	)
	if err != nil {
		t.Fatalf("Build(projected-away measure): %v", err)
	}
	if got := projected.Operators[len(projected.Operators)-1].(*Chart).Measure.Input.Name; got != "status" {
		t.Fatalf("projected-away measure = %q", got)
	}
}

func TestBuildChartCountFieldEnforcesReservedAndAmbiguousFields(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		source string
		code   string
	}{
		{`index=gradethis | chart count(__os_private) OVER path BY service`, "SPL_RESERVED_FIELD"},
		{`index=gradethis | chart count(fields) OVER path BY service`, "SPL_AMBIGUOUS_CHART_FIELD"},
	} {
		_, err := Build(mustParse(t, test.source), testScope([]string{"gradethis"}, nil))
		assertDiagnosticCode(t, err, test.code)
	}

	logical, err := Build(
		mustParse(t, `index=gradethis | table fields path service | chart count(fields) OVER path BY service`),
		testScope([]string{"gradethis"}, nil),
	)
	if err != nil {
		t.Fatalf("Build(closed fields schema): %v", err)
	}
	if got := logical.Operators[len(logical.Operators)-1].(*Chart).Measure.Output; got != "count(fields)" {
		t.Fatalf("closed-schema output = %q", got)
	}
}

func TestBuildRejectsForgedChartCountFieldContracts(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name   string
		mutate func(*spl.ChartCommand)
		code   string
	}{
		{"missing input", func(command *spl.ChartCommand) { command.Aggregate.Input = "" }, "SPL_UNSUPPORTED_CHART_AGGREGATE"},
		{"missing input range", func(command *spl.ChartCommand) { command.Aggregate.InputRange = spl.Range{} }, "SPL_UNSUPPORTED_CHART_AGGREGATE"},
		{"missing aggregate range", func(command *spl.ChartCommand) { command.Aggregate.Range = spl.Range{} }, "SPL_UNSUPPORTED_CHART_AGGREGATE"},
		{"missing alias range", func(command *spl.ChartCommand) { command.Aggregate.AliasRange = spl.Range{} }, "SPL_UNSUPPORTED_CHART_AGGREGATE"},
		{"percentile metadata", func(command *spl.ChartCommand) { command.Aggregate.Percentile = 50 }, "SPL_UNSUPPORTED_CHART_AGGREGATE"},
		{"predicate metadata", func(command *spl.ChartCommand) { command.Aggregate.Predicate = &spl.WhereComparisonExpr{} }, "SPL_UNSUPPORTED_CHART_AGGREGATE"},
		{"explicit alias", func(command *spl.ChartCommand) { command.Aggregate.ExplicitAlias = true }, "SPL_UNSUPPORTED_CHART_AGGREGATE"},
		{"wrong canonical output", func(command *spl.ChartCommand) { command.Aggregate.Alias = "count(other)" }, "SPL_UNSUPPORTED_CHART_AGGREGATE"},
		{"quoted input", func(command *spl.ChartCommand) {
			command.Aggregate.Input = `"status"`
			command.Aggregate.Alias = `count("status")`
		}, "SPL_UNSUPPORTED_CHART_AGGREGATE"},
		{"multiple-token input", func(command *spl.ChartCommand) {
			command.Aggregate.Input = "status host"
			command.Aggregate.Alias = "count(status host)"
		}, "SPL_UNSUPPORTED_CHART_AGGREGATE"},
		{"private input", func(command *spl.ChartCommand) {
			command.Aggregate.Input = "__os_private"
			command.Aggregate.Alias = "count(__os_private)"
		}, "SPL_RESERVED_FIELD"},
		{"measure equals row", func(command *spl.ChartCommand) {
			command.Aggregate.Input = command.Over.Name
			command.Aggregate.Alias = "count(" + command.Over.Name + ")"
		}, "SPL_DUPLICATE_FIELD"},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			query := mustParse(t, `index=gradethis | chart count(status) OVER path BY service`)
			command := query.Commands[0].(*spl.ChartCommand)
			test.mutate(command)
			_, err := Build(query, testScope([]string{"gradethis"}, nil))
			assertDiagnosticCode(t, err, test.code)
		})
	}

	column := mustParse(t, `index=gradethis | chart count(status) OVER path BY service`)
	columnCommand := column.Commands[0].(*spl.ChartCommand)
	columnCommand.Aggregate.Input = columnCommand.SplitBy.Name
	columnCommand.Aggregate.Alias = "count(" + columnCommand.SplitBy.Name + ")"
	if _, err := Build(column, testScope([]string{"gradethis"}, nil)); err != nil {
		t.Fatalf("Build(forged measure equal to column): %v", err)
	}
}

func TestAnalyzeRevalidatesForgedChartCountFieldLogicalPlans(t *testing.T) {
	t.Parallel()

	row := mustResolveEventAggregateField(t, "path")
	column := mustResolveEventAggregateField(t, "service")
	input := mustResolveEventAggregateField(t, "status")
	valid := func() *Chart {
		return &Chart{
			Over:    row,
			SplitBy: column,
			Measure: AggregateMeasure{
				Function: AggregateFunctionCountValues,
				Input:    input,
				Output:   "count(status)",
			},
			RowLimit:     maxChartRows,
			SeriesLimit:  chartSeriesLimit,
			IncludeNull:  true,
			IncludeOther: true,
			NullLabel:    "NULL",
			OtherLabel:   "OTHER",
		}
	}

	analysis, err := Analyze(&Query{Operators: []Operator{valid()}})
	if err != nil {
		t.Fatalf("Analyze(valid): %v", err)
	}
	if !slices.Equal(analysis.ReferencedFields, []string{"path", "service", "status"}) {
		t.Fatalf("referenced fields = %v", analysis.ReferencedFields)
	}

	columnMeasure := valid()
	columnMeasure.Measure.Input = column
	columnMeasure.Measure.Output = "count(service)"
	if _, err := Analyze(&Query{Operators: []Operator{columnMeasure}}); err != nil {
		t.Fatalf("Analyze(measure equal to column): %v", err)
	}

	for _, test := range []struct {
		name   string
		mutate func(*Chart)
	}{
		{"missing input", func(operator *Chart) { operator.Measure.Input = FieldRef{} }},
		{"forged input path", func(operator *Chart) { operator.Measure.Input.Path = []string{"attacker"} }},
		{"canonical input", func(operator *Chart) { operator.Measure.Input.Canonical = true }},
		{"percentile metadata", func(operator *Chart) { operator.Measure.Percentile = 50 }},
		{"predicate metadata", func(operator *Chart) { operator.Measure.Predicate = &ComparisonExpression{} }},
		{"wrong canonical output", func(operator *Chart) { operator.Measure.Output = "count(other)" }},
		{"measure equals row", func(operator *Chart) {
			operator.Measure.Input = operator.Over
			operator.Measure.Output = "count(path)"
		}},
		{"zero row bound", func(operator *Chart) { operator.RowLimit = 0 }},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			operator := valid()
			test.mutate(operator)
			if _, err := Analyze(&Query{Operators: []Operator{operator}}); err == nil {
				t.Fatal("Analyze succeeded, want forged chart rejection")
			}
		})
	}
}
