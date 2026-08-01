package plan

import (
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/spl"
)

func TestBuildTimechartPercentileProducesStaticMeasure(t *testing.T) {
	t.Parallel()

	scope := testScope([]string{"gradethis"}, nil)
	scope.Earliest = time.Date(2026, 7, 21, 8, 2, 30, 0, time.UTC)
	scope.Latest = time.Date(2026, 7, 21, 8, 12, 0, 1, time.UTC)
	logical, err := Build(
		mustParse(t, `index=gradethis | timechart span=5m p095(http.duration) AS latency_p95`),
		scope,
	)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	operator, ok := logical.Operators[len(logical.Operators)-1].(*Timechart)
	if !ok {
		t.Fatalf("last operator = %T, want *Timechart", logical.Operators[len(logical.Operators)-1])
	}
	if operator.Time.Name != "_time" || !operator.Time.Canonical || operator.Split != nil ||
		operator.Measure.Function != AggregateFunctionPercentile ||
		operator.Measure.Input.Name != "http.duration" ||
		!slices.Equal(operator.Measure.Input.Path, []string{"http", "duration"}) ||
		operator.Measure.Percentile != 95 || operator.Measure.Output != "latency_p95" ||
		operator.Span != 5*time.Minute ||
		!operator.FirstBucket.Equal(time.Date(2026, 7, 21, 8, 0, 0, 0, time.UTC)) ||
		operator.BucketCount != 3 || !operator.FixedRange || !operator.Continuous ||
		!operator.IncludePartial {
		t.Fatalf("timechart = %#v", operator)
	}
	if !slices.Equal(logical.OutputFields, []string{"_time", "latency_p95"}) ||
		logical.DynamicOutput != nil {
		t.Fatalf("output = %v/%#v", logical.OutputFields, logical.DynamicOutput)
	}
	analysis, err := Analyze(logical)
	if err != nil {
		t.Fatalf("Analyze: %v", err)
	}
	if !slices.Equal(analysis.ReferencedFields, []string{"_time", "http.duration", "index"}) {
		t.Fatalf("referenced fields = %v", analysis.ReferencedFields)
	}
}

func TestBuildTimechartPercentileUsesCanonicalDefaultOutput(t *testing.T) {
	t.Parallel()

	logical, err := Build(
		mustParse(t, `index=gradethis | timechart span=1h P050(latency)`),
		testScope([]string{"gradethis"}, nil),
	)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	operator := logical.Operators[len(logical.Operators)-1].(*Timechart)
	if operator.Measure.Output != "perc50(latency)" ||
		!slices.Equal(logical.OutputFields, []string{"_time", "perc50(latency)"}) {
		t.Fatalf("measure/output = %#v/%v", operator.Measure, logical.OutputFields)
	}
}

func TestBuildRejectsForgedTimechartAggregateContracts(t *testing.T) {
	t.Parallel()

	fieldRange := spl.Range{
		Start: spl.Position{Offset: 1, Line: 1, Column: 2},
		End:   spl.Position{Offset: 8, Line: 1, Column: 9},
	}
	validPercentile := spl.StatsAggregate{
		Function:   spl.AggregateFunctionPercentile,
		Input:      "latency",
		InputRange: fieldRange,
		Percentile: 95,
		Alias:      "perc95(latency)",
		Range:      fieldRange,
		AliasRange: fieldRange,
	}
	validCount := spl.StatsAggregate{
		Function:   spl.AggregateFunctionCount,
		Alias:      "count",
		Range:      fieldRange,
		AliasRange: fieldRange,
	}
	tests := []struct {
		name      string
		aggregate spl.StatsAggregate
		split     *spl.StatsGroupField
		code      string
	}{
		{
			name: "percentile zero",
			aggregate: func() spl.StatsAggregate {
				got := validPercentile
				got.Percentile = 0
				return got
			}(),
			code: "SPL_UNSUPPORTED_TIMECHART_AGGREGATE",
		},
		{
			name: "percentile above range",
			aggregate: func() spl.StatsAggregate {
				got := validPercentile
				got.Percentile = 100
				return got
			}(),
			code: "SPL_UNSUPPORTED_TIMECHART_AGGREGATE",
		},
		{
			name: "percentile missing input",
			aggregate: func() spl.StatsAggregate {
				got := validPercentile
				got.Input = ""
				got.InputRange = spl.Range{}
				return got
			}(),
			code: "SPL_UNSUPPORTED_TIMECHART_AGGREGATE",
		},
		{
			name: "percentile with predicate",
			aggregate: func() spl.StatsAggregate {
				got := validPercentile
				got.Predicate = &spl.WhereComparisonExpr{}
				return got
			}(),
			code: "SPL_UNSUPPORTED_TIMECHART_AGGREGATE",
		},
		{
			name: "percentile forged default alias",
			aggregate: func() spl.StatsAggregate {
				got := validPercentile
				got.Alias = "p95(latency)"
				return got
			}(),
			code: "SPL_UNSUPPORTED_TIMECHART_AGGREGATE",
		},
		{
			name:      "split percentile",
			aggregate: validPercentile,
			split:     &spl.StatsGroupField{Name: "service", Range: fieldRange},
			code:      "SPL_UNSUPPORTED_TIMECHART_SYNTAX",
		},
		{
			name: "count with input",
			aggregate: func() spl.StatsAggregate {
				got := validCount
				got.Input = "latency"
				got.InputRange = fieldRange
				return got
			}(),
			code: "SPL_UNSUPPORTED_TIMECHART_AGGREGATE",
		},
		{
			name: "count alias",
			aggregate: func() spl.StatsAggregate {
				got := validCount
				got.Alias = "total"
				got.ExplicitAlias = true
				return got
			}(),
			code: "SPL_UNSUPPORTED_TIMECHART_AGGREGATE",
		},
		{
			name: "unsupported aggregate",
			aggregate: func() spl.StatsAggregate {
				got := validPercentile
				got.Function = spl.AggregateFunctionMinimum
				got.Percentile = 0
				return got
			}(),
			code: "SPL_UNSUPPORTED_TIMECHART_AGGREGATE",
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			base := mustParse(t, `index=gradethis`)
			command := &spl.TimechartCommand{
				Span: spl.TimeSpan{
					Magnitude: 5,
					Unit:      spl.TimeSpanUnitMinute,
					Range:     fieldRange,
				},
				Aggregate: test.aggregate,
				SplitBy:   test.split,
				Range:     fieldRange,
			}
			query := &spl.Query{
				Search:   base.Search,
				Commands: []spl.Command{command},
				Range:    base.Range,
			}
			_, err := Build(query, testScope([]string{"gradethis"}, nil))
			assertDiagnosticCode(t, err, test.code)
		})
	}
}

func TestBuildTimechartPercentileRejectsOutputCollisionAndAmbiguousInput(t *testing.T) {
	t.Parallel()

	_, err := Build(
		mustParse(t, `index=gradethis | timechart span=5m p95(latency) AS _time`),
		testScope([]string{"gradethis"}, nil),
	)
	assertDiagnosticCode(t, err, "SPL_DUPLICATE_FIELD")

	_, err = Build(
		mustParse(t, `index=gradethis | timechart span=5m p95(fields)`),
		testScope([]string{"gradethis"}, nil),
	)
	assertDiagnosticCode(t, err, "SPL_AMBIGUOUS_TIMECHART_FIELD")

	logical, err := Build(
		mustParse(t, `index=gradethis | table _time fields | timechart span=5m p95(fields)`),
		testScope([]string{"gradethis"}, nil),
	)
	if err != nil {
		t.Fatalf("Build(exact fields schema): %v", err)
	}
	operator := logical.Operators[len(logical.Operators)-1].(*Timechart)
	if operator.Measure.Input.Name != "fields" {
		t.Fatalf("exact fields input = %#v", operator.Measure.Input)
	}
}

func TestAnalyzeRejectsForgedTimechartMeasures(t *testing.T) {
	t.Parallel()

	valid := mustResolveEventAggregateField(t, "_time")
	validInput := mustResolveEventAggregateField(t, "latency")
	validPercentile := AggregateMeasure{
		Function:   AggregateFunctionPercentile,
		Input:      validInput,
		Percentile: 95,
		Output:     "latency_p95",
	}
	tests := []struct {
		name    string
		measure AggregateMeasure
		split   *TimechartSplit
	}{
		{name: "invalid function", measure: AggregateMeasure{Output: "value"}},
		{name: "row count input", measure: AggregateMeasure{Function: AggregateFunctionCountRows, Input: validInput, Output: "count"}},
		{name: "row count percentile", measure: AggregateMeasure{Function: AggregateFunctionCountRows, Percentile: 95, Output: "count"}},
		{name: "row count output", measure: AggregateMeasure{Function: AggregateFunctionCountRows, Output: "total"}},
		{name: "percentile zero", measure: func() AggregateMeasure { got := validPercentile; got.Percentile = 0; return got }()},
		{name: "percentile missing input", measure: func() AggregateMeasure { got := validPercentile; got.Input = FieldRef{}; return got }()},
		{name: "percentile predicate", measure: func() AggregateMeasure { got := validPercentile; got.Predicate = &ComparisonExpression{}; return got }()},
		{name: "percentile forged path", measure: func() AggregateMeasure {
			got := validPercentile
			got.Input.Path = []string{"attacker"}
			return got
		}()},
		{name: "percentile forged canonical input", measure: func() AggregateMeasure {
			got := validPercentile
			got.Input.Canonical = true
			return got
		}()},
		{name: "percentile reserved output", measure: func() AggregateMeasure {
			got := validPercentile
			got.Output = "__os_private"
			return got
		}()},
		{name: "percentile split", measure: validPercentile, split: &TimechartSplit{Field: mustResolveEventAggregateField(t, "service")}},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			_, err := Analyze(&Query{Operators: []Operator{&Timechart{
				Time:    valid,
				Measure: test.measure,
				Split:   test.split,
			}}})
			if err == nil {
				t.Fatal("Analyze succeeded, want forged timechart rejection")
			}
		})
	}
}

func TestBuildTimechartPercentileDiagnosticRangeIsAggregate(t *testing.T) {
	t.Parallel()

	base := mustParse(t, `index=gradethis | timechart span=5m p95(latency)`)
	command := base.Commands[0].(*spl.TimechartCommand)
	command.Aggregate.Percentile = 0
	_, err := Build(base, testScope([]string{"gradethis"}, nil))
	var diagnostic *Diagnostic
	if !errors.As(err, &diagnostic) {
		t.Fatalf("Build error = %#v, want diagnostic", err)
	}
	if diagnostic.Range != command.Aggregate.Range {
		t.Fatalf("diagnostic range = %#v, want aggregate %#v", diagnostic.Range, command.Aggregate.Range)
	}
}
