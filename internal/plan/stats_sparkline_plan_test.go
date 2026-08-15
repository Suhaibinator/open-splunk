package plan

import (
	"fmt"
	"strings"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/spl"
)

func TestBuildStatsSparklinePreservesMeasureOrderBYAndBackendNeutralMetadata(t *testing.T) {
	t.Parallel()

	logical, err := Build(
		mustParse(t, `index=gradethis | stats count AS rows sparkline(avg(latency),6months) AS trend sum(bytes) AS total BY host`),
		testScope([]string{"gradethis"}, nil),
	)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	aggregate := logical.Operators[len(logical.Operators)-1].(*Aggregate)
	if len(aggregate.Measures) != 3 || len(aggregate.GroupBy) != 1 ||
		aggregate.Measures[0].Sparkline != nil ||
		aggregate.Measures[1].Sparkline == nil ||
		aggregate.Measures[2].Sparkline != nil {
		t.Fatalf("aggregate = %#v", aggregate)
	}
	measure := aggregate.Measures[1]
	sparkline := measure.Sparkline
	if measure.Function != AggregateFunctionInvalid ||
		!emptyAggregateField(measure.Input) || measure.InputExpression != nil ||
		measure.Predicate != nil || measure.Percentile != 0 || measure.Output != "trend" ||
		sparkline.Function != AggregateFunctionAverage ||
		sparkline.Input.Name != "latency" ||
		sparkline.Time.Name != "_time" || !sparkline.Time.Canonical ||
		sparkline.Span.Kind != SparklineSpanKindExplicit ||
		sparkline.Span.Magnitude != 6 || sparkline.Span.Unit != SparklineSpanUnitMonth ||
		sparkline.MaximumPoints != spl.MaximumStatsSparklinePoints {
		t.Fatalf("sparkline measure = %#v", measure)
	}
	if _, analyzeErr := Analyze(logical); analyzeErr != nil {
		t.Fatalf("Analyze: %v", analyzeErr)
	}
}

func TestBuildStatsSparklineMapsDocumentedInnerFunctions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		call     string
		function AggregateFunction
		hasInput bool
	}{
		{"count", AggregateFunctionCountRows, false},
		{"count()", AggregateFunctionCountRows, false},
		{"count(value)", AggregateFunctionCountValues, true},
		{"c(value)", AggregateFunctionCountValues, true},
		{"dc(value)", AggregateFunctionDistinctCount, true},
		{"mean(value)", AggregateFunctionAverage, true},
		{"avg(value)", AggregateFunctionAverage, true},
		{"stdev(value)", AggregateFunctionStandardDeviationSample, true},
		{"stdevp(value)", AggregateFunctionStandardDeviationPopulation, true},
		{"var(value)", AggregateFunctionVarianceSample, true},
		{"varp(value)", AggregateFunctionVariancePopulation, true},
		{"sum(value)", AggregateFunctionSum, true},
		{"sumsq(value)", AggregateFunctionSumSquares, true},
		{"min(value)", AggregateFunctionMinimum, true},
		{"max(value)", AggregateFunctionMaximum, true},
		{"range(value)", AggregateFunctionRange, true},
	}
	for index, test := range tests {
		t.Run(test.call, func(t *testing.T) {
			t.Parallel()

			source := fmt.Sprintf(
				`index=gradethis | stats sparkline(%s) AS trend_%d`,
				test.call,
				index,
			)
			logical, err := Build(
				mustParse(t, source),
				testScope([]string{"gradethis"}, nil),
			)
			if err != nil {
				t.Fatalf("Build: %v", err)
			}
			measure := logical.Operators[len(logical.Operators)-1].(*Aggregate).Measures[0]
			if measure.Sparkline == nil || measure.Sparkline.Function != test.function ||
				(measure.Sparkline.Input.Name != "") != test.hasInput ||
				measure.Sparkline.Span != (SparklineSpan{Kind: SparklineSpanKindAutomatic}) {
				t.Fatalf("measure = %#v", measure)
			}
			if _, analyzeErr := Analyze(logical); analyzeErr != nil {
				t.Fatalf("Analyze: %v", analyzeErr)
			}
		})
	}
}

func TestBuildStatsSparklinePreservesEverySpanUnit(t *testing.T) {
	t.Parallel()

	tests := []struct {
		span string
		unit SparklineSpanUnit
	}{
		{"250us", SparklineSpanUnitMicrosecond},
		{"125ms", SparklineSpanUnitMillisecond},
		{"25cs", SparklineSpanUnitCentisecond},
		{"5ds", SparklineSpanUnitDecisecond},
		{"2s", SparklineSpanUnitSecond},
		{"3m", SparklineSpanUnitMinute},
		{"4h", SparklineSpanUnitHour},
		{"5d", SparklineSpanUnitDay},
		{"6mon", SparklineSpanUnitMonth},
	}
	for _, test := range tests {
		t.Run(test.span, func(t *testing.T) {
			t.Parallel()

			logical, err := Build(
				mustParse(t, `index=gradethis | stats sparkline(sum(value),`+test.span+`) AS trend`),
				testScope([]string{"gradethis"}, nil),
			)
			if err != nil {
				t.Fatalf("Build: %v", err)
			}
			span := logical.Operators[len(logical.Operators)-1].(*Aggregate).Measures[0].Sparkline.Span
			if span.Kind != SparklineSpanKindExplicit || span.Unit != test.unit {
				t.Fatalf("span = %#v", span)
			}
		})
	}
}

func TestBuildStatsSparklineRequiresCanonicalTime(t *testing.T) {
	t.Parallel()

	for _, source := range []string{
		`index=gradethis | fields - _time | stats sparkline(count) AS trend`,
		`index=gradethis | table value | stats sparkline(avg(value)) AS trend`,
		`index=gradethis | eval _time=1 | stats sparkline(count) AS trend`,
		`index=gradethis | rename _time AS observed_at | stats sparkline(count) AS trend`,
		`index=gradethis | bin _time span=5m | stats sparkline(count) AS trend`,
	} {
		_, err := Build(
			mustParse(t, source),
			testScope([]string{"gradethis"}, nil),
		)
		assertDiagnosticCode(t, err, "SPL_UNSUPPORTED_STATS_TIME_FIELD")
	}
}

func TestBuildStatsSparklineRejectsDuplicateAndReservedOutputs(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		source string
		code   string
	}{
		{`index=gradethis | stats sparkline(count) sparkline(avg(value))`, "SPL_DUPLICATE_FIELD"},
		{`index=gradethis | stats sparkline(count) BY sparkline`, "SPL_DUPLICATE_FIELD"},
		{`index=gradethis | stats sparkline(avg(fields)) AS trend`, "SPL_AMBIGUOUS_STATS_FIELD"},
	} {
		_, err := Build(
			mustParse(t, test.source),
			testScope([]string{"gradethis"}, nil),
		)
		assertDiagnosticCode(t, err, test.code)
	}
}

func TestBuildStatsSparklineRejectsForgedASTMetadata(t *testing.T) {
	t.Parallel()

	base := mustParse(t, `index=gradethis`)
	sourceRange := spl.Range{
		Start: spl.Position{Line: 1, Column: 1},
		End:   spl.Position{Offset: 5, Line: 1, Column: 6},
	}
	valid := func() *spl.StatsCommand {
		return &spl.StatsCommand{
			Aggregates: []spl.StatsAggregate{{
				Sparkline: &spl.StatsSparkline{
					Function: spl.AggregateFunctionCount,
					Span: spl.SparklineSpan{
						Kind: spl.SparklineSpanKindAutomatic,
					},
					Range: sourceRange,
				},
				Alias:         "trend",
				ExplicitAlias: true,
				Range:         sourceRange,
				AliasRange:    sourceRange,
			}},
			Range: sourceRange,
		}
	}
	for _, test := range []struct {
		name   string
		mutate func(*spl.StatsAggregate)
	}{
		{name: "overlapping ordinary function", mutate: func(aggregate *spl.StatsAggregate) { aggregate.Function = spl.AggregateFunctionCount }},
		{name: "overlapping ordinary input", mutate: func(aggregate *spl.StatsAggregate) { aggregate.Input, aggregate.InputRange = "value", sourceRange }},
		{name: "missing aggregate range", mutate: func(aggregate *spl.StatsAggregate) { aggregate.Range = spl.Range{} }},
		{name: "missing alias range", mutate: func(aggregate *spl.StatsAggregate) { aggregate.AliasRange = spl.Range{} }},
		{name: "implicit nondefault alias", mutate: func(aggregate *spl.StatsAggregate) { aggregate.ExplicitAlias = false }},
		{name: "missing nested range", mutate: func(aggregate *spl.StatsAggregate) { aggregate.Sparkline.Range = spl.Range{} }},
		{name: "unsupported inner function", mutate: func(aggregate *spl.StatsAggregate) { aggregate.Sparkline.Function = spl.AggregateFunctionMedian }},
		{name: "row count with input", mutate: func(aggregate *spl.StatsAggregate) {
			aggregate.Sparkline.Input, aggregate.Sparkline.InputRange = "value", sourceRange
		}},
		{name: "field function without input", mutate: func(aggregate *spl.StatsAggregate) { aggregate.Sparkline.Function = spl.AggregateFunctionAverage }},
		{name: "invalid span kind", mutate: func(aggregate *spl.StatsAggregate) { aggregate.Sparkline.Span.Kind = spl.SparklineSpanKindInvalid }},
		{name: "automatic span magnitude", mutate: func(aggregate *spl.StatsAggregate) { aggregate.Sparkline.Span.Magnitude = 1 }},
		{name: "automatic span unit", mutate: func(aggregate *spl.StatsAggregate) { aggregate.Sparkline.Span.Unit = spl.SparklineSpanUnitSecond }},
		{name: "automatic span range", mutate: func(aggregate *spl.StatsAggregate) { aggregate.Sparkline.Span.Range = sourceRange }},
		{name: "explicit zero span", mutate: func(aggregate *spl.StatsAggregate) {
			aggregate.Sparkline.Span = spl.SparklineSpan{Kind: spl.SparklineSpanKindExplicit, Unit: spl.SparklineSpanUnitSecond, Range: sourceRange}
		}},
		{name: "explicit invalid unit", mutate: func(aggregate *spl.StatsAggregate) {
			aggregate.Sparkline.Span = spl.SparklineSpan{Kind: spl.SparklineSpanKindExplicit, Magnitude: 1, Range: sourceRange}
		}},
		{name: "explicit invalid subsecond divisor", mutate: func(aggregate *spl.StatsAggregate) {
			aggregate.Sparkline.Span = spl.SparklineSpan{Kind: spl.SparklineSpanKindExplicit, Magnitude: 3, Unit: spl.SparklineSpanUnitMillisecond, Range: sourceRange}
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			command := valid()
			test.mutate(&command.Aggregates[0])
			query := &spl.Query{
				Search:   base.Search,
				Commands: []spl.Command{command},
				Range:    base.Range,
			}
			_, err := Build(query, testScope([]string{"gradethis"}, nil))
			assertDiagnosticCode(t, err, "SPL_UNSUPPORTED_STATS_AGGREGATE")
		})
	}
}

func TestBuildChartAndTimechartRejectForgedStatsSparklineArm(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name   string
		source string
		mutate func(*spl.Query)
		code   string
	}{
		{
			name:   "chart",
			source: `index=gradethis | chart sum(value) OVER host BY service`,
			mutate: func(query *spl.Query) {
				query.Commands[0].(*spl.ChartCommand).Aggregate.Sparkline = &spl.StatsSparkline{}
			},
			code: "SPL_UNSUPPORTED_CHART_AGGREGATE",
		},
		{
			name:   "timechart",
			source: `index=gradethis | timechart span=5m sum(value)`,
			mutate: func(query *spl.Query) {
				query.Commands[0].(*spl.TimechartCommand).Aggregate.Sparkline = &spl.StatsSparkline{}
			},
			code: "SPL_UNSUPPORTED_TIMECHART_AGGREGATE",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			query := mustParse(t, test.source)
			test.mutate(query)
			_, err := Build(query, testScope([]string{"gradethis"}, nil))
			assertDiagnosticCode(t, err, test.code)
		})
	}
}

func TestAnalyzeStatsSparklineRejectsForgedPlanMetadata(t *testing.T) {
	t.Parallel()

	sourceRange := spl.Range{
		Start: spl.Position{Line: 1, Column: 1},
		End:   spl.Position{Offset: 5, Line: 1, Column: 6},
	}
	timeField, err := ResolveField("_time", sourceRange)
	if err != nil {
		t.Fatalf("ResolveField(_time): %v", err)
	}
	input, err := ResolveField("value", sourceRange)
	if err != nil {
		t.Fatalf("ResolveField(value): %v", err)
	}
	valid := func() *Aggregate {
		return &Aggregate{StatsOptions: &StatsOptions{
			Partitions: 1,
			Delimiter:  " ",
		}, Measures: []AggregateMeasure{{
			Sparkline: &SparklineMeasure{
				Function:      AggregateFunctionAverage,
				Input:         input,
				Time:          timeField,
				Span:          SparklineSpan{Kind: SparklineSpanKindAutomatic},
				MaximumPoints: spl.MaximumStatsSparklinePoints,
			},
			Output: "trend",
		}}}
	}
	for _, test := range []struct {
		name   string
		mutate func(*AggregateMeasure)
	}{
		{name: "overlapping scalar function", mutate: func(measure *AggregateMeasure) { measure.Function = AggregateFunctionCountRows }},
		{name: "overlapping scalar input", mutate: func(measure *AggregateMeasure) { measure.Input = input }},
		{name: "zero point cap", mutate: func(measure *AggregateMeasure) { measure.Sparkline.MaximumPoints = 0 }},
		{name: "wrong point cap", mutate: func(measure *AggregateMeasure) { measure.Sparkline.MaximumPoints = spl.MaximumStatsSparklinePoints - 1 }},
		{name: "invalid function", mutate: func(measure *AggregateMeasure) { measure.Sparkline.Function = AggregateFunctionMedian }},
		{name: "row count retains input", mutate: func(measure *AggregateMeasure) { measure.Sparkline.Function = AggregateFunctionCountRows }},
		{name: "field function missing input", mutate: func(measure *AggregateMeasure) { measure.Sparkline.Input = FieldRef{} }},
		{name: "wrong time field", mutate: func(measure *AggregateMeasure) { measure.Sparkline.Time = input }},
		{name: "noncanonical time", mutate: func(measure *AggregateMeasure) { measure.Sparkline.Time.Canonical = false }},
		{name: "invalid automatic span", mutate: func(measure *AggregateMeasure) { measure.Sparkline.Span.Magnitude = 1 }},
		{name: "invalid explicit unit", mutate: func(measure *AggregateMeasure) {
			measure.Sparkline.Span = SparklineSpan{Kind: SparklineSpanKindExplicit, Magnitude: 1}
		}},
		{name: "invalid subsecond divisor", mutate: func(measure *AggregateMeasure) {
			measure.Sparkline.Span = SparklineSpan{Kind: SparklineSpanKindExplicit, Magnitude: 3, Unit: SparklineSpanUnitMillisecond}
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			operator := valid()
			test.mutate(&operator.Measures[0])
			if _, analyzeErr := Analyze(&Query{Operators: []Operator{operator}}); analyzeErr == nil {
				t.Fatalf("Analyze accepted forged %s measure: %#v", test.name, operator.Measures[0])
			}
		})
	}

	for _, span := range []SparklineSpan{
		{Kind: SparklineSpanKindAutomatic},
		{Kind: SparklineSpanKindExplicit, Magnitude: 250, Unit: SparklineSpanUnitMicrosecond},
		{Kind: SparklineSpanKindExplicit, Magnitude: 2, Unit: SparklineSpanUnitMonth},
	} {
		operator := valid()
		operator.Measures[0].Sparkline.Span = span
		if _, analyzeErr := Analyze(&Query{Operators: []Operator{operator}}); analyzeErr != nil {
			t.Errorf("Analyze valid span %#v: %v", span, analyzeErr)
		}
	}
}

func TestStatsSparklineSharesPlanMeasureCap(t *testing.T) {
	t.Parallel()

	measures := make([]string, spl.MaximumStatsMeasures)
	for index := range measures {
		measures[index] = fmt.Sprintf(
			"sparkline(count(value_%d)) AS trend_%d",
			index,
			index,
		)
	}
	logical, err := Build(
		mustParse(t, `index=gradethis | stats `+strings.Join(measures, " ")),
		testScope([]string{"gradethis"}, nil),
	)
	if err != nil {
		t.Fatalf("Build at measure cap: %v", err)
	}
	if _, analyzeErr := Analyze(logical); analyzeErr != nil {
		t.Fatalf("Analyze at measure cap: %v", analyzeErr)
	}

	aggregate := logical.Operators[len(logical.Operators)-1].(*Aggregate)
	aggregate.Measures = append(aggregate.Measures, aggregate.Measures[0])
	if _, analyzeErr := Analyze(logical); analyzeErr == nil {
		t.Fatal("Analyze accepted sparkline above measure cap")
	}
}
