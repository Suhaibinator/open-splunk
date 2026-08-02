package plan

import (
	"errors"
	"slices"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/spl"
)

func TestBuildEventStatsFieldAggregatesProduceSingularRowPreservingMeasure(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name       string
		source     string
		function   AggregateFunction
		input      string
		inputPath  []string
		output     string
		references []string
	}{
		{
			name:       "minimum",
			source:     `index=gradethis | eventstats min(http.latency) AS minimum_latency BY host, status`,
			function:   AggregateFunctionMinimum,
			input:      "http.latency",
			inputPath:  []string{"http", "latency"},
			output:     "minimum_latency",
			references: []string{"host", "http.latency", "index", "status"},
		},
		{
			name:       "sum",
			source:     `index=gradethis | eventstats sum(http.bytes) AS total_bytes BY host, status`,
			function:   AggregateFunctionSum,
			input:      "http.bytes",
			inputPath:  []string{"http", "bytes"},
			output:     "total_bytes",
			references: []string{"host", "http.bytes", "index", "status"},
		},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			logical, err := Build(
				mustParse(t, test.source),
				testScope([]string{"gradethis"}, nil),
			)
			if err != nil {
				t.Fatalf("Build: %v", err)
			}

			var eventAggregates []*EventAggregate
			for _, operator := range logical.Operators {
				if eventAggregate, ok := operator.(*EventAggregate); ok {
					eventAggregates = append(eventAggregates, eventAggregate)
				}
			}
			if len(eventAggregates) != 1 {
				t.Fatalf("EventAggregate operators = %d, want exactly 1", len(eventAggregates))
			}
			eventAggregate := eventAggregates[0]
			if logical.Operators[len(logical.Operators)-1] != eventAggregate {
				t.Fatalf("EventAggregate is not the terminal operator: %T", logical.Operators[len(logical.Operators)-1])
			}
			measure := eventAggregate.Measure
			if eventAggregate.LogicalName() != "EventAggregate" ||
				eventAggregate.SourceRange() != eventAggregate.Range ||
				measure.Function != test.function ||
				measure.Input.Name != test.input ||
				measure.Input.Canonical ||
				!slices.Equal(measure.Input.Path, test.inputPath) ||
				measure.Input.Range == (spl.Range{}) ||
				measure.Predicate != nil ||
				measure.Percentile != 0 ||
				measure.Output != test.output {
				t.Fatalf("event aggregate = %#v", eventAggregate)
			}
			if len(eventAggregate.GroupBy) != 2 ||
				eventAggregate.GroupBy[0].Name != "host" ||
				eventAggregate.GroupBy[1].Name != "status" ||
				eventAggregate.GroupBy[0].Range == (spl.Range{}) ||
				eventAggregate.GroupBy[1].Range == (spl.Range{}) {
				t.Fatalf("event aggregate groups = %#v, want resolved host/status", eventAggregate.GroupBy)
			}

			analysis, analyzeErr := Analyze(logical)
			if analyzeErr != nil {
				t.Fatalf("Analyze: %v", analyzeErr)
			}
			if !slices.Equal(analysis.ReferencedFields, test.references) {
				t.Fatalf("referenced fields = %v", analysis.ReferencedFields)
			}
			if eligibilityErr := ValidateFieldAnalysisEligibility(logical); eligibilityErr != nil {
				t.Fatalf("field analysis eligibility: %v", eligibilityErr)
			}
			if timelineErr := ValidateTimelineEligibility(logical); timelineErr != nil {
				t.Fatalf("timeline eligibility: %v", timelineErr)
			}
		})
	}
}

func TestBuildEventStatsSumUpsertsKnownOutputSchema(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		source string
		want   []string
	}{
		{
			name: "append",
			source: `index=gradethis | table _time,host,bytes` +
				` | eventstats sum(bytes) AS total_bytes BY host`,
			want: []string{"_time", "host", "bytes", "total_bytes"},
		},
		{
			name: "replace in place",
			source: `index=gradethis | table _time,total_bytes,host,bytes` +
				` | eventstats sum(bytes) AS total_bytes BY host`,
			want: []string{"_time", "total_bytes", "host", "bytes"},
		},
		{
			name: "preserve statistics relation",
			source: `index=gradethis | stats sum(bytes) AS bytes BY host` +
				` | eventstats sum(bytes) AS total_bytes`,
			want: []string{"host", "bytes", "total_bytes"},
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			logical, err := Build(
				mustParse(t, test.source),
				testScope([]string{"gradethis"}, nil),
			)
			if err != nil {
				t.Fatalf("Build: %v", err)
			}
			if !slices.Equal(logical.OutputFields, test.want) {
				t.Fatalf("output fields = %v, want %v", logical.OutputFields, test.want)
			}
		})
	}
}

func TestBuildEventStatsSumProtectsReservedOpenSchemaFields(t *testing.T) {
	t.Parallel()

	for _, source := range []string{
		`index=gradethis | eventstats sum(fields) AS total`,
		`index=gradethis | eventstats sum(bytes) AS fields`,
		`index=gradethis | eventstats sum(bytes) AS total BY fields`,
	} {
		_, err := Build(
			mustParse(t, source),
			testScope([]string{"gradethis"}, nil),
		)
		assertDiagnosticCode(t, err, "SPL_AMBIGUOUS_EVENTSTATS_FIELD")
		var diagnostic *Diagnostic
		if !errors.As(err, &diagnostic) || diagnostic.Range == (spl.Range{}) {
			t.Fatalf("Build(%q) error = %#v, want source-located diagnostic", source, err)
		}
	}

	logical, err := Build(
		mustParse(
			t,
			`index=gradethis | table fields,host`+
				` | eventstats sum(fields) AS total BY host`,
		),
		testScope([]string{"gradethis"}, nil),
	)
	if err != nil {
		t.Fatalf("Build closed-schema input: %v", err)
	}
	if !slices.Equal(logical.OutputFields, []string{"fields", "host", "total"}) {
		t.Fatalf("closed input output fields = %v", logical.OutputFields)
	}

	logical, err = Build(
		mustParse(
			t,
			`index=gradethis | table fields,host`+
				` | eventstats sum(host) AS fields BY host`,
		),
		testScope([]string{"gradethis"}, nil),
	)
	if err != nil {
		t.Fatalf("Build closed-schema output: %v", err)
	}
	if !slices.Equal(logical.OutputFields, []string{"fields", "host"}) {
		t.Fatalf("closed output fields = %v", logical.OutputFields)
	}
}

func TestBuildEventStatsSumPreservesTimeAndIndexProvenance(t *testing.T) {
	t.Parallel()

	if _, err := Build(
		mustParse(
			t,
			`index=gradethis | eventstats sum(bytes) AS total_bytes`+
				` | timechart span=5m count BY level`,
		),
		testScope([]string{"gradethis"}, nil),
	); err != nil {
		t.Fatalf("ordinary sum output invalidated canonical time: %v", err)
	}

	_, err := Build(
		mustParse(
			t,
			`index=gradethis | eventstats sum(bytes) AS _time`+
				` | timechart span=5m count BY level`,
		),
		testScope([]string{"gradethis"}, nil),
	)
	assertDiagnosticCode(t, err, "SPL_UNSUPPORTED_TIMECHART_TIME_FIELD")

	overwrittenTime, err := Build(
		mustParse(t, `index=gradethis | eventstats sum(bytes) AS _time`),
		testScope([]string{"gradethis"}, nil),
	)
	if err != nil {
		t.Fatalf("Build direct _time overwrite: %v", err)
	}
	if err := ValidateFieldAnalysisEligibility(overwrittenTime); err != nil {
		t.Fatalf("field analysis after _time overwrite: %v", err)
	}
	assertDiagnosticCode(
		t,
		ValidateTimelineEligibility(overwrittenTime),
		"SPL_UNSUPPORTED_TIMELINE_TIME_FIELD",
	)

	logical, err := Build(
		mustParse(
			t,
			`index=gradethis | eventstats sum(bytes) AS index`+
				` | search index=secret`,
		),
		testScope([]string{"gradethis"}, nil),
	)
	if err != nil {
		t.Fatalf("Build overwritten index search: %v", err)
	}
	if !slices.Equal(logical.EffectiveIndexes, []string{"gradethis"}) {
		t.Fatalf("effective indexes = %v, want [gradethis]", logical.EffectiveIndexes)
	}

	_, err = Build(
		mustParse(
			t,
			`index=gradethis | eventstats sum(bytes) AS total_bytes`+
				` | search index=secret`,
		),
		testScope([]string{"gradethis"}, nil),
	)
	assertDiagnosticCode(t, err, "SPL_INDEX_FORBIDDEN")
}

func TestBuildEventStatsSumRejectsForgedAggregateMetadata(t *testing.T) {
	t.Parallel()

	base := mustParse(t, `index=gradethis`)
	fieldRange := spl.Range{
		Start: spl.Position{Offset: 1, Line: 1, Column: 2},
		End:   spl.Position{Offset: 7, Line: 1, Column: 8},
	}
	valid := spl.StatsAggregate{
		Function:      spl.AggregateFunctionSum,
		Input:         "bytes",
		InputRange:    fieldRange,
		Alias:         "total_bytes",
		ExplicitAlias: true,
		Range:         fieldRange,
		AliasRange:    fieldRange,
	}
	tests := []struct {
		name     string
		mutate   func(*spl.StatsAggregate)
		wantCode string
	}{
		{
			name: "missing input",
			mutate: func(aggregate *spl.StatsAggregate) {
				aggregate.Input = ""
			},
			wantCode: "SPL_UNSUPPORTED_EVENTSTATS_AGGREGATE",
		},
		{
			name: "missing input range",
			mutate: func(aggregate *spl.StatsAggregate) {
				aggregate.InputRange = spl.Range{}
			},
			wantCode: "SPL_UNSUPPORTED_EVENTSTATS_AGGREGATE",
		},
		{
			name: "predicate metadata",
			mutate: func(aggregate *spl.StatsAggregate) {
				aggregate.Predicate = &spl.WhereComparisonExpr{}
			},
			wantCode: "SPL_UNSUPPORTED_EVENTSTATS_AGGREGATE",
		},
		{
			name: "percentile metadata",
			mutate: func(aggregate *spl.StatsAggregate) {
				aggregate.Percentile = 95
			},
			wantCode: "SPL_UNSUPPORTED_EVENTSTATS_AGGREGATE",
		},
		{
			name: "implicit alias",
			mutate: func(aggregate *spl.StatsAggregate) {
				aggregate.ExplicitAlias = false
			},
			wantCode: "SPL_UNSUPPORTED_EVENTSTATS_AGGREGATE",
		},
		{
			name: "empty output",
			mutate: func(aggregate *spl.StatsAggregate) {
				aggregate.Alias = ""
			},
			wantCode: "SPL_UNSUPPORTED_EVENTSTATS_AGGREGATE",
		},
		{
			name: "private output",
			mutate: func(aggregate *spl.StatsAggregate) {
				aggregate.Alias = "__os_eventstats_private"
			},
			wantCode: "SPL_RESERVED_FIELD",
		},
		{
			name: "maximum function remains unsupported",
			mutate: func(aggregate *spl.StatsAggregate) {
				aggregate.Function = spl.AggregateFunctionMaximum
			},
			wantCode: "SPL_UNSUPPORTED_EVENTSTATS_AGGREGATE",
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			aggregate := valid
			test.mutate(&aggregate)
			query := &spl.Query{
				Search: base.Search,
				Commands: []spl.Command{&spl.EventStatsCommand{
					Aggregate: aggregate,
				}},
				Range: base.Range,
			}
			_, err := Build(query, testScope([]string{"gradethis"}, nil))
			assertDiagnosticCode(t, err, test.wantCode)
		})
	}
}

func TestAnalyzeEventStatsSumAcceptsResolvedInputAndRejectsForgedMetadata(
	t *testing.T,
) {
	t.Parallel()

	input := mustResolveEventAggregateField(t, "http.bytes")
	host := mustResolveEventAggregateField(t, "host")
	valid := AggregateMeasure{
		Function: AggregateFunctionSum,
		Input:    input,
		Output:   "total_bytes",
	}
	operator := &EventAggregate{
		GroupBy: []FieldRef{host},
		Measure: valid,
	}
	analysis, err := Analyze(&Query{Operators: []Operator{operator}})
	if err != nil {
		t.Fatalf("Analyze valid sum: %v", err)
	}
	if !slices.Equal(analysis.ReferencedFields, []string{"host", "http.bytes"}) {
		t.Fatalf("referenced fields = %v, want [host http.bytes]", analysis.ReferencedFields)
	}

	tests := []struct {
		name   string
		mutate func(*AggregateMeasure)
	}{
		{
			name: "missing input",
			mutate: func(measure *AggregateMeasure) {
				measure.Input = FieldRef{}
			},
		},
		{
			name: "unresolved input",
			mutate: func(measure *AggregateMeasure) {
				measure.Input = FieldRef{Name: input.Name}
			},
		},
		{
			name: "forged input path",
			mutate: func(measure *AggregateMeasure) {
				measure.Input.Path = []string{"attacker"}
			},
		},
		{
			name: "forged canonical input",
			mutate: func(measure *AggregateMeasure) {
				measure.Input.Canonical = true
			},
		},
		{
			name: "predicate metadata",
			mutate: func(measure *AggregateMeasure) {
				measure.Predicate = &ComparisonExpression{}
			},
		},
		{
			name: "percentile metadata",
			mutate: func(measure *AggregateMeasure) {
				measure.Percentile = 50
			},
		},
		{
			name: "empty output",
			mutate: func(measure *AggregateMeasure) {
				measure.Output = ""
			},
		},
		{
			name: "private output",
			mutate: func(measure *AggregateMeasure) {
				measure.Output = "__os_eventstats_private"
			},
		},
		{
			name: "maximum function remains unsupported",
			mutate: func(measure *AggregateMeasure) {
				measure.Function = AggregateFunctionMaximum
			},
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			measure := valid
			test.mutate(&measure)
			for _, validate := range []struct {
				name string
				fn   func(*Query) error
			}{
				{name: "analyze", fn: func(query *Query) error {
					_, err := Analyze(query)
					return err
				}},
				{name: "field analysis", fn: ValidateFieldAnalysisEligibility},
				{name: "timeline", fn: ValidateTimelineEligibility},
			} {
				query := &Query{Operators: []Operator{
					&Scan{},
					&EventAggregate{Measure: measure},
				}}
				if err := validate.fn(query); err == nil {
					t.Fatalf("%s accepted forged sum measure %#v", validate.name, measure)
				}
			}
		})
	}
}
