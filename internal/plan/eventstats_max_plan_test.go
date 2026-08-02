package plan

import (
	"errors"
	"slices"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/spl"
)

func TestBuildEventStatsMaximumUpsertsKnownOutputSchema(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		source string
		want   []string
	}{
		{
			name: "append",
			source: `index=gradethis | table _time,host,latency` +
				` | eventstats max(latency) AS maximum_latency BY host`,
			want: []string{"_time", "host", "latency", "maximum_latency"},
		},
		{
			name: "replace in place",
			source: `index=gradethis | table _time,maximum_latency,host,latency` +
				` | eventstats max(latency) AS maximum_latency BY host`,
			want: []string{"_time", "maximum_latency", "host", "latency"},
		},
		{
			name: "preserve statistics relation",
			source: `index=gradethis | stats max(bytes) AS bytes BY host` +
				` | eventstats max(bytes) AS maximum_bytes`,
			want: []string{"host", "bytes", "maximum_bytes"},
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
			if logical.DynamicOutput != nil {
				t.Fatalf("dynamic output = %#v, want static schema", logical.DynamicOutput)
			}
		})
	}
}

func TestBuildEventStatsMaximumProtectsReservedOpenSchemaFields(t *testing.T) {
	t.Parallel()

	for _, source := range []string{
		`index=gradethis | eventstats max(fields) AS maximum_value`,
		`index=gradethis | eventstats max(latency) AS fields`,
		`index=gradethis | eventstats max(latency) AS maximum_value BY fields`,
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

	closedInput, err := Build(
		mustParse(
			t,
			`index=gradethis | table fields,host`+
				` | eventstats max(fields) AS maximum_value BY host`,
		),
		testScope([]string{"gradethis"}, nil),
	)
	if err != nil {
		t.Fatalf("Build closed-schema input: %v", err)
	}
	if !slices.Equal(closedInput.OutputFields, []string{"fields", "host", "maximum_value"}) {
		t.Fatalf("closed input output fields = %v", closedInput.OutputFields)
	}

	closedOutput, err := Build(
		mustParse(
			t,
			`index=gradethis | table fields,host`+
				` | eventstats max(host) AS fields BY host`,
		),
		testScope([]string{"gradethis"}, nil),
	)
	if err != nil {
		t.Fatalf("Build closed-schema output: %v", err)
	}
	if !slices.Equal(closedOutput.OutputFields, []string{"fields", "host"}) {
		t.Fatalf("closed output fields = %v", closedOutput.OutputFields)
	}
}

func TestBuildEventStatsMaximumPreservesCanonicalTimeAndIndexProvenance(t *testing.T) {
	t.Parallel()

	ordinary, err := Build(
		mustParse(
			t,
			`index=gradethis | eventstats max(latency) AS maximum_latency`+
				` | timechart span=5m count BY level`,
		),
		testScope([]string{"gradethis"}, nil),
	)
	if err != nil {
		t.Fatalf("ordinary maximum output invalidated canonical time: %v", err)
	}
	if ordinary == nil {
		t.Fatal("Build ordinary maximum returned nil plan")
	}

	_, err = Build(
		mustParse(
			t,
			`index=gradethis | eventstats max(latency) AS _time`+
				` | timechart span=5m count BY level`,
		),
		testScope([]string{"gradethis"}, nil),
	)
	assertDiagnosticCode(t, err, "SPL_UNSUPPORTED_TIMECHART_TIME_FIELD")

	overwrittenTime, err := Build(
		mustParse(t, `index=gradethis | eventstats max(latency) AS _time`),
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

	overwrittenIndex, err := Build(
		mustParse(
			t,
			`index=gradethis | eventstats max(host) AS index`+
				` | search index=secret`,
		),
		testScope([]string{"gradethis"}, nil),
	)
	if err != nil {
		t.Fatalf("Build overwritten index search: %v", err)
	}
	if !slices.Equal(overwrittenIndex.EffectiveIndexes, []string{"gradethis"}) {
		t.Fatalf("effective indexes = %v, want [gradethis]", overwrittenIndex.EffectiveIndexes)
	}

	_, err = Build(
		mustParse(
			t,
			`index=gradethis | eventstats max(latency) AS maximum_latency`+
				` | search index=secret`,
		),
		testScope([]string{"gradethis"}, nil),
	)
	assertDiagnosticCode(t, err, "SPL_INDEX_FORBIDDEN")
}

func TestBuildEventStatsMaximumRejectsForgedAggregateMetadata(t *testing.T) {
	t.Parallel()

	base := mustParse(t, `index=gradethis`)
	fieldRange := spl.Range{
		Start: spl.Position{Offset: 1, Line: 1, Column: 2},
		End:   spl.Position{Offset: 8, Line: 1, Column: 9},
	}
	valid := spl.StatsAggregate{
		Function:      spl.AggregateFunctionMaximum,
		Input:         "latency",
		InputRange:    fieldRange,
		Alias:         "maximum_latency",
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
				aggregate.Percentile = 99
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
					Range:     fieldRange,
				}},
				Range: base.Range,
			}
			_, err := Build(query, testScope([]string{"gradethis"}, nil))
			assertDiagnosticCode(t, err, test.wantCode)
		})
	}
}

func TestEventStatsMaximumPlanContractRejectsForgedMetadata(t *testing.T) {
	t.Parallel()

	input := mustResolveEventAggregateField(t, "http.latency")
	host := mustResolveEventAggregateField(t, "host")
	valid := AggregateMeasure{
		Function: AggregateFunctionMaximum,
		Input:    input,
		Output:   "maximum_latency",
	}
	validQuery := &Query{Operators: []Operator{
		&Scan{},
		&EventAggregate{GroupBy: []FieldRef{host}, Measure: valid},
	}}
	analysis, err := Analyze(validQuery)
	if err != nil {
		t.Fatalf("Analyze valid maximum: %v", err)
	}
	if !slices.Equal(analysis.ReferencedFields, []string{"host", "http.latency"}) {
		t.Fatalf("referenced fields = %v, want [host http.latency]", analysis.ReferencedFields)
	}
	if err := ValidateFieldAnalysisEligibility(validQuery); err != nil {
		t.Fatalf("field analysis rejected valid maximum: %v", err)
	}
	if err := ValidateTimelineEligibility(validQuery); err != nil {
		t.Fatalf("timeline rejected valid maximum: %v", err)
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
				measure.Percentile = 99
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
			name: "row count function with field metadata",
			mutate: func(measure *AggregateMeasure) {
				measure.Function = AggregateFunctionCountRows
			},
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			measure := valid
			test.mutate(&measure)
			query := &Query{Operators: []Operator{
				&Scan{},
				&EventAggregate{GroupBy: []FieldRef{host}, Measure: measure},
			}}
			validators := []struct {
				name string
				fn   func(*Query) error
			}{
				{name: "analyze", fn: func(query *Query) error {
					_, err := Analyze(query)
					return err
				}},
				{name: "field analysis", fn: ValidateFieldAnalysisEligibility},
				{name: "timeline", fn: ValidateTimelineEligibility},
			}
			for _, validator := range validators {
				if err := validator.fn(query); err == nil {
					t.Fatalf("%s accepted forged maximum measure %#v", validator.name, measure)
				}
			}
		})
	}
}
