package plan

import (
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/spl"
)

func TestBuildTimechartSumAndAverageProducesStaticNumericMeasure(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		source    string
		function  AggregateFunction
		input     string
		inputPath []string
		output    string
	}{
		{
			name:      "sum canonical output",
			source:    `index=gradethis | timechart span=5m SUM(bytes)`,
			function:  AggregateFunctionSum,
			input:     "bytes",
			inputPath: []string{"bytes"},
			output:    "sum(bytes)",
		},
		{
			name:      "average alias",
			source:    `index=gradethis | timechart span=5m avg(http.duration) AS mean_ms`,
			function:  AggregateFunctionAverage,
			input:     "http.duration",
			inputPath: []string{"http", "duration"},
			output:    "mean_ms",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			scope := testScope([]string{"gradethis"}, nil)
			scope.Earliest = time.Date(2026, 7, 21, 8, 2, 30, 0, time.UTC)
			scope.Latest = time.Date(2026, 7, 21, 8, 12, 0, 1, time.UTC)
			logical, err := Build(mustParse(t, test.source), scope)
			if err != nil {
				t.Fatalf("Build: %v", err)
			}
			operator, ok := logical.Operators[len(logical.Operators)-1].(*Timechart)
			if !ok {
				t.Fatalf("last operator = %T, want *Timechart", logical.Operators[len(logical.Operators)-1])
			}
			if operator.Time.Name != "_time" || !operator.Time.Canonical || operator.Split != nil ||
				operator.Measure.Function != test.function ||
				operator.Measure.Input.Name != test.input ||
				!slices.Equal(operator.Measure.Input.Path, test.inputPath) ||
				operator.Measure.Percentile != 0 || operator.Measure.Output != test.output ||
				operator.Span != 5*time.Minute ||
				!operator.FirstBucket.Equal(time.Date(2026, 7, 21, 8, 0, 0, 0, time.UTC)) ||
				operator.BucketCount != 3 || !operator.FixedRange || !operator.Continuous ||
				!operator.IncludePartial {
				t.Fatalf("timechart = %#v", operator)
			}
			if !slices.Equal(logical.OutputFields, []string{"_time", test.output}) ||
				logical.DynamicOutput != nil {
				t.Fatalf("output = %v/%#v", logical.OutputFields, logical.DynamicOutput)
			}
			analysis, err := Analyze(logical)
			if err != nil {
				t.Fatalf("Analyze: %v", err)
			}
			if !slices.Equal(analysis.ReferencedFields, []string{"_time", test.input, "index"}) {
				t.Fatalf("referenced fields = %v", analysis.ReferencedFields)
			}
		})
	}
}

func TestBuildTimechartSumAndAverageProducesBoundedDynamicSplitSeries(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		source   string
		function AggregateFunction
		input    string
		output   string
		split    string
	}{
		{
			name:     "sum",
			source:   `index=gradethis | timechart span=5m sum(bytes) BY service`,
			function: AggregateFunctionSum,
			input:    "bytes",
			output:   "sum(bytes)",
			split:    "service",
		},
		{
			name:     "average with alias",
			source:   `index=gradethis | timechart span=5m avg(latency) AS mean BY http.route`,
			function: AggregateFunctionAverage,
			input:    "latency",
			output:   "mean",
			split:    "http.route",
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
			operator := logical.Operators[len(logical.Operators)-1].(*Timechart)
			if operator.Measure.Function != test.function ||
				operator.Measure.Input.Name != test.input ||
				operator.Measure.Output != test.output || operator.Split == nil ||
				operator.Split.Field.Name != test.split || operator.Split.SeriesLimit != 10 ||
				!operator.Split.IncludeNull || !operator.Split.IncludeOther ||
				operator.Split.NullLabel != "NULL" || operator.Split.OtherLabel != "OTHER" {
				t.Fatalf("timechart = %#v", operator)
			}
			if len(logical.OutputFields) != 0 || logical.DynamicOutput == nil ||
				!slices.Equal(logical.DynamicOutput.FixedFields, []string{"_time"}) ||
				logical.DynamicOutput.MaxSeries != 12 {
				t.Fatalf("output = %v/%#v", logical.OutputFields, logical.DynamicOutput)
			}
			analysis, err := Analyze(logical)
			if err != nil {
				t.Fatalf("Analyze: %v", err)
			}
			wantReferencedFields := []string{"_time", test.input, test.split, "index"}
			slices.Sort(wantReferencedFields)
			if !slices.Equal(analysis.ReferencedFields, wantReferencedFields) {
				t.Fatalf("referenced fields = %v", analysis.ReferencedFields)
			}
		})
	}
}

func TestBuildTimechartSumAndAverageRejectsMeasureSplitCollision(t *testing.T) {
	t.Parallel()

	for _, function := range []string{"sum", "avg"} {
		_, err := Build(
			mustParse(t, `index=gradethis | timechart span=5m `+function+`(latency) BY latency`),
			testScope([]string{"gradethis"}, nil),
		)
		assertDiagnosticCode(t, err, "SPL_DUPLICATE_FIELD")
	}
}

func TestBuildTimechartSumAndAverageRejectsOutputCollisionAndAmbiguousInput(t *testing.T) {
	t.Parallel()

	for _, function := range []string{"sum", "avg"} {
		_, err := Build(
			mustParse(t, `index=gradethis | timechart span=5m `+function+`(latency) AS _time`),
			testScope([]string{"gradethis"}, nil),
		)
		assertDiagnosticCode(t, err, "SPL_DUPLICATE_FIELD")

		_, err = Build(
			mustParse(t, `index=gradethis | timechart span=5m `+function+`(fields)`),
			testScope([]string{"gradethis"}, nil),
		)
		assertDiagnosticCode(t, err, "SPL_AMBIGUOUS_TIMECHART_FIELD")

		logical, buildErr := Build(
			mustParse(t, `index=gradethis | table _time fields | timechart span=5m `+function+`(fields)`),
			testScope([]string{"gradethis"}, nil),
		)
		if buildErr != nil {
			t.Fatalf("Build(%s exact fields schema): %v", function, buildErr)
		}
		operator := logical.Operators[len(logical.Operators)-1].(*Timechart)
		if operator.Measure.Input.Name != "fields" {
			t.Fatalf("%s exact fields input = %#v", function, operator.Measure.Input)
		}
	}
}

func TestBuildTimechartSumAndAveragePreservesProjectedMissingInput(t *testing.T) {
	t.Parallel()

	logical, err := Build(
		mustParse(t, `index=gradethis | fields - latency | timechart span=5m avg(latency) AS mean`),
		testScope([]string{"gradethis"}, nil),
	)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	operator := logical.Operators[len(logical.Operators)-1].(*Timechart)
	if operator.Measure.Function != AggregateFunctionAverage ||
		operator.Measure.Input.Name != "latency" || operator.Measure.Output != "mean" ||
		!slices.Equal(logical.OutputFields, []string{"_time", "mean"}) {
		t.Fatalf("projected-input timechart = %#v, output=%v", operator, logical.OutputFields)
	}
}

func TestBuildTimechartSumAndAverageRetainsCanonicalTimeAndTerminalBounds(t *testing.T) {
	t.Parallel()

	for _, source := range []string{
		`index=gradethis | fields - _time | timechart span=5m sum(bytes)`,
		`index=gradethis | eval _time=1 | timechart span=5m avg(latency)`,
		`index=gradethis | bin _time span=5m | timechart span=5m sum(bytes)`,
	} {
		_, err := Build(mustParse(t, source), testScope([]string{"gradethis"}, nil))
		assertDiagnosticCode(t, err, "SPL_UNSUPPORTED_TIMECHART_TIME_FIELD")
	}

	_, err := Build(
		mustParse(t, `index=gradethis | timechart span=5m avg(latency) | table _time`),
		testScope([]string{"gradethis"}, nil),
	)
	assertDiagnosticCode(t, err, "SPL_UNSUPPORTED_TIMECHART_PIPELINE")
}

func TestBuildRejectsForgedTimechartSumAndAverageContracts(t *testing.T) {
	t.Parallel()

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
	tests := []struct {
		name      string
		aggregate spl.StatsAggregate
		split     *spl.StatsGroupField
		code      string
	}{
		{name: "valid function with missing input", aggregate: func() spl.StatsAggregate {
			got := valid
			got.Input = ""
			got.InputRange = spl.Range{}
			return got
		}(), code: "SPL_UNSUPPORTED_TIMECHART_AGGREGATE"},
		{name: "numeric aggregate with percentile", aggregate: func() spl.StatsAggregate {
			got := valid
			got.Percentile = 95
			return got
		}(), code: "SPL_UNSUPPORTED_TIMECHART_AGGREGATE"},
		{name: "numeric aggregate with predicate", aggregate: func() spl.StatsAggregate {
			got := valid
			got.Predicate = &spl.WhereComparisonExpr{}
			return got
		}(), code: "SPL_UNSUPPORTED_TIMECHART_AGGREGATE"},
		{name: "forged default output", aggregate: func() spl.StatsAggregate {
			got := valid
			got.Alias = "total"
			return got
		}(), code: "SPL_UNSUPPORTED_TIMECHART_AGGREGATE"},
		{name: "empty output", aggregate: func() spl.StatsAggregate {
			got := valid
			got.Alias = ""
			return got
		}(), code: "SPL_UNSUPPORTED_TIMECHART_AGGREGATE"},
		{name: "same measure and split field", aggregate: valid, split: &spl.StatsGroupField{Name: "latency", Range: fieldRange}, code: "SPL_DUPLICATE_FIELD"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			base := mustParse(t, `index=gradethis`)
			query := &spl.Query{
				Search: base.Search,
				Commands: []spl.Command{&spl.TimechartCommand{
					Span:      spl.TimeSpan{Magnitude: 5, Unit: spl.TimeSpanUnitMinute, Range: fieldRange},
					Aggregate: test.aggregate,
					SplitBy:   test.split,
					Range:     fieldRange,
				}},
				Range: base.Range,
			}
			_, err := Build(query, testScope([]string{"gradethis"}, nil))
			assertDiagnosticCode(t, err, test.code)
		})
	}
}

func TestAnalyzeAcceptsValidAndRejectsForgedTimechartSumAndAverageMeasures(t *testing.T) {
	t.Parallel()

	timeField := mustResolveEventAggregateField(t, "_time")
	input := mustResolveEventAggregateField(t, "latency")
	splitField := mustResolveEventAggregateField(t, "service")
	validSplit := func(field FieldRef) *TimechartSplit {
		return &TimechartSplit{
			Field:        field,
			SeriesLimit:  timechartSeriesLimit,
			IncludeNull:  true,
			IncludeOther: true,
			NullLabel:    "NULL",
			OtherLabel:   "OTHER",
		}
	}
	for _, function := range []AggregateFunction{AggregateFunctionSum, AggregateFunctionAverage} {
		valid := AggregateMeasure{Function: function, Input: input, Output: "result"}
		if _, err := Analyze(&Query{Operators: []Operator{&Timechart{
			Time: timeField, Measure: valid,
		}}}); err != nil {
			t.Fatalf("Analyze(valid %v): %v", function, err)
		}
		if _, err := Analyze(&Query{Operators: []Operator{&Timechart{
			Time: timeField, Measure: valid, Split: validSplit(splitField),
		}}}); err != nil {
			t.Fatalf("Analyze(valid split %v): %v", function, err)
		}

		tests := []struct {
			name    string
			measure AggregateMeasure
			split   *TimechartSplit
		}{
			{name: "missing input", measure: func() AggregateMeasure { got := valid; got.Input = FieldRef{}; return got }()},
			{name: "percentile metadata", measure: func() AggregateMeasure { got := valid; got.Percentile = 50; return got }()},
			{name: "predicate metadata", measure: func() AggregateMeasure { got := valid; got.Predicate = &ComparisonExpression{}; return got }()},
			{name: "time output", measure: func() AggregateMeasure { got := valid; got.Output = "_time"; return got }()},
			{name: "forged path", measure: func() AggregateMeasure { got := valid; got.Input.Path = []string{"attacker"}; return got }()},
			{name: "canonical input", measure: func() AggregateMeasure { got := valid; got.Input.Canonical = true; return got }()},
			{name: "same measure and split", measure: valid, split: validSplit(input)},
			{name: "zero series limit", measure: valid, split: func() *TimechartSplit { got := validSplit(splitField); got.SeriesLimit = 0; return got }()},
			{name: "wrong series limit", measure: valid, split: func() *TimechartSplit { got := validSplit(splitField); got.SeriesLimit++; return got }()},
			{name: "null series disabled", measure: valid, split: func() *TimechartSplit { got := validSplit(splitField); got.IncludeNull = false; return got }()},
			{name: "other series disabled", measure: valid, split: func() *TimechartSplit { got := validSplit(splitField); got.IncludeOther = false; return got }()},
			{name: "null label renamed", measure: valid, split: func() *TimechartSplit { got := validSplit(splitField); got.NullLabel = "none"; return got }()},
			{name: "other label renamed", measure: valid, split: func() *TimechartSplit { got := validSplit(splitField); got.OtherLabel = "rest"; return got }()},
		}
		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				_, err := Analyze(&Query{Operators: []Operator{&Timechart{
					Time: timeField, Measure: test.measure, Split: test.split,
				}}})
				if err == nil {
					t.Fatal("Analyze succeeded, want forged timechart rejection")
				}
			})
		}
	}
}

func TestBuildTimechartSumDiagnosticRangeIsAggregate(t *testing.T) {
	t.Parallel()

	base := mustParse(t, `index=gradethis | timechart span=5m sum(latency)`)
	command := base.Commands[0].(*spl.TimechartCommand)
	command.Aggregate.Percentile = 50
	_, err := Build(base, testScope([]string{"gradethis"}, nil))
	var diagnostic *Diagnostic
	if !errors.As(err, &diagnostic) {
		t.Fatalf("Build error = %#v, want diagnostic", err)
	}
	if diagnostic.Range != command.Aggregate.Range {
		t.Fatalf("diagnostic range = %#v, want aggregate %#v", diagnostic.Range, command.Aggregate.Range)
	}
}
