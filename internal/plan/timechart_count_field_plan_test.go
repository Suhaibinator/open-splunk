package plan

import (
	"slices"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/spl"
)

func TestBuildTimechartCountFieldProducesStaticMeasure(t *testing.T) {
	t.Parallel()

	scope := testScope([]string{"gradethis"}, nil)
	scope.Earliest = time.Date(2026, 7, 21, 8, 2, 30, 0, time.UTC)
	scope.Latest = time.Date(2026, 7, 21, 8, 12, 0, 1, time.UTC)
	logical, err := Build(
		mustParse(
			t,
			`index=gradethis | timechart span=5m count(bytes)`,
		),
		scope,
	)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	operator, ok := logical.Operators[len(logical.Operators)-1].(*Timechart)
	if !ok {
		t.Fatalf("last operator = %T, want *Timechart", logical.Operators[len(logical.Operators)-1])
	}
	if operator.Time.Name != "_time" || !operator.Time.Canonical ||
		operator.Measure.Function != AggregateFunctionCountValues ||
		operator.Measure.Input.Name != "bytes" ||
		!slices.Equal(operator.Measure.Input.Path, []string{"bytes"}) ||
		operator.Measure.Predicate != nil || operator.Measure.Percentile != 0 ||
		operator.Measure.Output != "count(bytes)" || operator.Split != nil ||
		operator.Span != 5*time.Minute ||
		!operator.FirstBucket.Equal(time.Date(2026, 7, 21, 8, 0, 0, 0, time.UTC)) ||
		operator.BucketCount != 3 || !operator.FixedRange || !operator.Continuous ||
		!operator.IncludePartial {
		t.Fatalf("timechart = %#v", operator)
	}
	if !slices.Equal(logical.OutputFields, []string{"_time", "count(bytes)"}) ||
		logical.DynamicOutput != nil {
		t.Fatalf("output = %v/%#v", logical.OutputFields, logical.DynamicOutput)
	}
	analysis, err := Analyze(logical)
	if err != nil {
		t.Fatalf("Analyze: %v", err)
	}
	if !slices.Equal(analysis.ReferencedFields, []string{"_time", "bytes", "index"}) {
		t.Fatalf("referenced fields = %v", analysis.ReferencedFields)
	}
}

func TestBuildTimechartCountFieldProducesBoundedDynamicSplitSeries(t *testing.T) {
	t.Parallel()

	logical, err := Build(
		mustParse(
			t,
			`index=gradethis | timechart span=5m count(http.status) AS populated BY Service`,
		),
		testScope([]string{"gradethis"}, nil),
	)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	operator := logical.Operators[len(logical.Operators)-1].(*Timechart)
	if operator.Measure.Function != AggregateFunctionCountValues ||
		operator.Measure.Input.Name != "http.status" ||
		!slices.Equal(operator.Measure.Input.Path, []string{"http", "status"}) ||
		operator.Measure.Output != "populated" || operator.Split == nil ||
		operator.Split.Field.Name != "Service" || operator.Split.SeriesLimit != 10 ||
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
	if !slices.Equal(
		analysis.ReferencedFields,
		[]string{"Service", "_time", "http.status", "index"},
	) {
		t.Fatalf("referenced fields = %v", analysis.ReferencedFields)
	}
}

func TestBuildTimechartCountFieldPreservesProjectedAwayInput(t *testing.T) {
	t.Parallel()

	logical, err := Build(
		mustParse(
			t,
			`index=gradethis | fields - latency | timechart span=5m count(latency) AS populated`,
		),
		testScope([]string{"gradethis"}, nil),
	)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	operator := logical.Operators[len(logical.Operators)-1].(*Timechart)
	if operator.Measure.Function != AggregateFunctionCountValues ||
		operator.Measure.Input.Name != "latency" ||
		operator.Measure.Output != "populated" ||
		!slices.Equal(logical.OutputFields, []string{"_time", "populated"}) {
		t.Fatalf("projected-input timechart = %#v, output=%v", operator, logical.OutputFields)
	}
}

func TestBuildTimechartCountFieldEnforcesReservedFieldsBarrier(t *testing.T) {
	t.Parallel()

	_, err := Build(
		mustParse(
			t,
			`index=gradethis | timechart span=5m count(fields)`,
		),
		testScope([]string{"gradethis"}, nil),
	)
	assertDiagnosticCode(t, err, "SPL_AMBIGUOUS_TIMECHART_FIELD")

	logical, err := Build(
		mustParse(
			t,
			`index=gradethis | table _time fields | timechart span=5m count(fields)`,
		),
		testScope([]string{"gradethis"}, nil),
	)
	if err != nil {
		t.Fatalf("Build(exact fields schema): %v", err)
	}
	operator := logical.Operators[len(logical.Operators)-1].(*Timechart)
	if operator.Measure.Input.Name != "fields" ||
		operator.Measure.Output != "count(fields)" {
		t.Fatalf("exact fields measure = %#v", operator.Measure)
	}
}

func TestBuildTimechartCountFieldRejectsOutputAndSplitCollisions(t *testing.T) {
	t.Parallel()

	_, err := Build(
		mustParse(
			t,
			`index=gradethis | timechart span=5m count(latency) AS _time`,
		),
		testScope([]string{"gradethis"}, nil),
	)
	assertDiagnosticCode(t, err, "SPL_DUPLICATE_FIELD")

	_, err = Build(
		mustParse(
			t,
			`index=gradethis | timechart span=5m count(latency) BY latency`,
		),
		testScope([]string{"gradethis"}, nil),
	)
	assertDiagnosticCode(t, err, "SPL_DUPLICATE_FIELD")
}

func TestBuildTimechartCountFieldRetainsCanonicalTimeAndTerminalInvariants(t *testing.T) {
	t.Parallel()

	for _, source := range []string{
		`index=gradethis | fields - _time | timechart span=5m count(status)`,
		`index=gradethis | eval _time=1 | timechart span=5m count(status)`,
		`index=gradethis | bin _time span=5m | timechart span=5m count(status)`,
	} {
		_, err := Build(
			mustParse(t, source),
			testScope([]string{"gradethis"}, nil),
		)
		assertDiagnosticCode(t, err, "SPL_UNSUPPORTED_TIMECHART_TIME_FIELD")
	}

	_, err := Build(
		mustParse(
			t,
			`index=gradethis | timechart span=5m count(status) | table _time`,
		),
		testScope([]string{"gradethis"}, nil),
	)
	assertDiagnosticCode(t, err, "SPL_UNSUPPORTED_TIMECHART_PIPELINE")
}

func TestBuildRejectsForgedTimechartCountFieldContracts(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		mutate func(*spl.StatsAggregate)
	}{
		{
			name: "missing input",
			mutate: func(aggregate *spl.StatsAggregate) {
				aggregate.Input = ""
				aggregate.InputRange = spl.Range{}
			},
		},
		{
			name: "missing input range",
			mutate: func(aggregate *spl.StatsAggregate) {
				aggregate.InputRange = spl.Range{}
			},
		},
		{
			name: "percentile metadata",
			mutate: func(aggregate *spl.StatsAggregate) {
				aggregate.Percentile = 50
			},
		},
		{
			name: "predicate metadata",
			mutate: func(aggregate *spl.StatsAggregate) {
				aggregate.Predicate = &spl.WhereComparisonExpr{}
			},
		},
		{
			name: "forged default output",
			mutate: func(aggregate *spl.StatsAggregate) {
				aggregate.Alias = "count(other)"
			},
		},
		{
			name: "empty output",
			mutate: func(aggregate *spl.StatsAggregate) {
				aggregate.Alias = ""
			},
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			query := mustParse(
				t,
				`index=gradethis | timechart span=5m count(status)`,
			)
			command := query.Commands[0].(*spl.TimechartCommand)
			test.mutate(&command.Aggregate)
			_, err := Build(query, testScope([]string{"gradethis"}, nil))
			assertDiagnosticCode(t, err, "SPL_UNSUPPORTED_TIMECHART_AGGREGATE")
		})
	}
}

func TestAnalyzeAcceptsValidTimechartCountFieldMeasure(t *testing.T) {
	t.Parallel()

	timeField := mustResolveEventAggregateField(t, "_time")
	input := mustResolveEventAggregateField(t, "status")
	splitField := mustResolveEventAggregateField(t, "service")
	valid := AggregateMeasure{
		Function: AggregateFunctionCountValues,
		Input:    input,
		Output:   "count(status)",
	}
	for _, split := range []*TimechartSplit{
		nil,
		{
			Field:        splitField,
			SeriesLimit:  timechartSeriesLimit,
			IncludeNull:  true,
			IncludeOther: true,
			NullLabel:    "NULL",
			OtherLabel:   "OTHER",
		},
	} {
		analysis, err := Analyze(&Query{Operators: []Operator{&Timechart{
			Time: timeField, Measure: valid, Split: split,
		}}})
		if err != nil {
			t.Fatalf("Analyze(valid split=%t): %v", split != nil, err)
		}
		want := []string{"_time", "status"}
		if split != nil {
			want = []string{"_time", "service", "status"}
		}
		if !slices.Equal(analysis.ReferencedFields, want) {
			t.Fatalf("referenced fields = %v, want %v", analysis.ReferencedFields, want)
		}
	}
}

func TestAnalyzeRejectsForgedTimechartCountFieldMeasures(t *testing.T) {
	t.Parallel()

	timeField := mustResolveEventAggregateField(t, "_time")
	input := mustResolveEventAggregateField(t, "status")
	valid := AggregateMeasure{
		Function: AggregateFunctionCountValues,
		Input:    input,
		Output:   "count(status)",
	}
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
	tests := []struct {
		name    string
		measure AggregateMeasure
		split   *TimechartSplit
	}{
		{name: "missing input", measure: func() AggregateMeasure { got := valid; got.Input = FieldRef{}; return got }()},
		{name: "percentile metadata", measure: func() AggregateMeasure { got := valid; got.Percentile = 50; return got }()},
		{name: "predicate metadata", measure: func() AggregateMeasure { got := valid; got.Predicate = &ComparisonExpression{}; return got }()},
		{name: "time output", measure: func() AggregateMeasure { got := valid; got.Output = "_time"; return got }()},
		{name: "invalid output", measure: func() AggregateMeasure { got := valid; got.Output = "seen*"; return got }()},
		{name: "forged path", measure: func() AggregateMeasure { got := valid; got.Input.Path = []string{"attacker"}; return got }()},
		{name: "canonical input", measure: func() AggregateMeasure { got := valid; got.Input.Canonical = true; return got }()},
		{name: "same measure and split", measure: valid, split: validSplit(input)},
		{name: "zero series limit", measure: valid, split: func() *TimechartSplit {
			got := validSplit(mustResolveEventAggregateField(t, "service"))
			got.SeriesLimit = 0
			return got
		}()},
		{name: "null series disabled", measure: valid, split: func() *TimechartSplit {
			got := validSplit(mustResolveEventAggregateField(t, "service"))
			got.IncludeNull = false
			return got
		}()},
		{name: "other series disabled", measure: valid, split: func() *TimechartSplit {
			got := validSplit(mustResolveEventAggregateField(t, "service"))
			got.IncludeOther = false
			return got
		}()},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			_, err := Analyze(&Query{Operators: []Operator{&Timechart{
				Time: timeField, Measure: test.measure, Split: test.split,
			}}})
			if err == nil {
				t.Fatal("Analyze succeeded, want forged timechart rejection")
			}
		})
	}
}
