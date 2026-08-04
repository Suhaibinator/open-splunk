package plan

import (
	"slices"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/spl"
)

func TestBuildStreamStatsCountProducesBoundedRowPreservingOperator(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		source         string
		function       AggregateFunction
		input          string
		output         string
		includeCurrent bool
		windowRows     uint64
		global         bool
		groups         []string
	}{
		{
			name:           "defaults",
			source:         `index=gradethis | streamstats count`,
			function:       AggregateFunctionCountRows,
			output:         "count",
			includeCurrent: true,
			global:         true,
		},
		{
			name:           "prior per group window",
			source:         `index=gradethis | streamstats current=f window=7 global=false count AS prior BY host, status`,
			function:       AggregateFunctionCountRows,
			output:         "prior",
			includeCurrent: false,
			windowRows:     7,
			global:         false,
			groups:         []string{"host", "status"},
		},
		{
			name:           "explicit defaults after BY",
			source:         `index=gradethis | streamstats count AS seen BY service current=true window=0 global=t`,
			function:       AggregateFunctionCountRows,
			output:         "seen",
			includeCurrent: true,
			global:         true,
			groups:         []string{"service"},
		},
		{
			name:           "field occurrences with canonical output",
			source:         `index=gradethis | streamstats count(payload.items)`,
			function:       AggregateFunctionCountValues,
			input:          "payload.items",
			output:         "count(payload.items)",
			includeCurrent: true,
			global:         true,
		},
		{
			name:           "prior field occurrences by group",
			source:         `index=gradethis | streamstats current=f count(status) AS populated BY host`,
			function:       AggregateFunctionCountValues,
			input:          "status",
			output:         "populated",
			includeCurrent: false,
			global:         true,
			groups:         []string{"host"},
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
			if logical.OutputFields != nil {
				t.Fatalf("open event schema became fixed: %v", logical.OutputFields)
			}
			operator, ok := logical.Operators[len(logical.Operators)-1].(*StreamAggregate)
			if !ok {
				t.Fatalf("last operator = %T, want *StreamAggregate", logical.Operators[len(logical.Operators)-1])
			}
			if operator.LogicalName() != "StreamAggregate" ||
				operator.SourceRange() != operator.Range ||
				operator.IncludeCurrent != test.includeCurrent ||
				operator.WindowRows != test.windowRows ||
				operator.Global != test.global ||
				operator.Measure.Function != test.function ||
				operator.Measure.Input.Name != test.input ||
				operator.Measure.Predicate != nil ||
				operator.Measure.Percentile != 0 ||
				operator.Measure.Output != test.output {
				t.Fatalf("stream aggregate = %#v", operator)
			}
			if test.input == "" {
				if operator.Measure.Input.Canonical ||
					operator.Measure.Input.Path != nil ||
					operator.Measure.Input.Range != (spl.Range{}) {
					t.Fatalf("row count input metadata = %#v", operator.Measure.Input)
				}
			} else if operator.Measure.Input.Range == (spl.Range{}) ||
				len(operator.Measure.Input.Path) == 0 {
				t.Fatalf("field count input metadata = %#v", operator.Measure.Input)
			}
			if len(operator.GroupBy) != len(test.groups) {
				t.Fatalf("groups = %#v, want %v", operator.GroupBy, test.groups)
			}
			for index, want := range test.groups {
				if operator.GroupBy[index].Name != want || operator.GroupBy[index].Range == (spl.Range{}) {
					t.Fatalf("group %d = %#v, want resolved %q", index, operator.GroupBy[index], want)
				}
			}

			analysis, analyzeErr := Analyze(logical)
			if analyzeErr != nil {
				t.Fatalf("Analyze: %v", analyzeErr)
			}
			wantReferences := []string{"index"}
			if test.input != "" {
				wantReferences = append(wantReferences, test.input)
			}
			wantReferences = append(wantReferences, test.groups...)
			slices.Sort(wantReferences)
			wantReferences = slices.Compact(wantReferences)
			if !slices.Equal(analysis.ReferencedFields, wantReferences) {
				t.Fatalf("referenced fields = %v, want %v", analysis.ReferencedFields, wantReferences)
			}
		})
	}
}

func TestBuildStreamStatsCountUpsertsKnownSchemaAndPreservesRelationKind(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name, source string
		want         []string
	}{
		{
			name:   "append",
			source: `index=gradethis | table _time,host,message | streamstats count AS ordinal BY host`,
			want:   []string{"_time", "host", "message", "ordinal"},
		},
		{
			name:   "replace in place",
			source: `index=gradethis | table _time,ordinal,host | streamstats count AS ordinal BY host`,
			want:   []string{"_time", "ordinal", "host"},
		},
		{
			name:   "preserve statistics schema",
			source: `index=gradethis | stats count BY host | streamstats count AS group_ordinal`,
			want:   []string{"host", "count", "group_ordinal"},
		},
		{
			name:   "append canonical field count output",
			source: `index=gradethis | table _time,status | streamstats count(status)`,
			want:   []string{"_time", "status", "count(status)"},
		},
		{
			name:   "field count alias replaces in place",
			source: `index=gradethis | table _time,status,populated | streamstats count(status) AS populated`,
			want:   []string{"_time", "status", "populated"},
		},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			logical, err := Build(mustParse(t, test.source), testScope([]string{"gradethis"}, nil))
			if err != nil {
				t.Fatalf("Build: %v", err)
			}
			if !slices.Equal(logical.OutputFields, test.want) {
				t.Fatalf("output fields = %v, want %v", logical.OutputFields, test.want)
			}
		})
	}
}

func TestBuildStreamStatsEnforcesReservedFieldsAndProvenance(t *testing.T) {
	t.Parallel()

	for _, source := range []string{
		`index=gradethis | streamstats count AS fields`,
		`index=gradethis | streamstats count BY fields`,
		`index=gradethis | streamstats count(fields) AS occurrences`,
	} {
		_, err := Build(mustParse(t, source), testScope([]string{"gradethis"}, nil))
		assertDiagnosticCode(t, err, "SPL_AMBIGUOUS_STREAMSTATS_FIELD")
	}

	closed, err := Build(
		mustParse(t, `index=gradethis | table fields,host | streamstats count AS fields BY host`),
		testScope([]string{"gradethis"}, nil),
	)
	if err != nil {
		t.Fatalf("Build closed fields schema: %v", err)
	}
	if !slices.Equal(closed.OutputFields, []string{"fields", "host"}) {
		t.Fatalf("closed fields output = %v", closed.OutputFields)
	}
	closedInput, err := Build(
		mustParse(t, `index=gradethis | table fields,host | streamstats count(fields) AS occurrences BY host`),
		testScope([]string{"gradethis"}, nil),
	)
	if err != nil {
		t.Fatalf("Build closed fields input: %v", err)
	}
	if !slices.Equal(closedInput.OutputFields, []string{"fields", "host", "occurrences"}) {
		t.Fatalf("closed fields input output = %v", closedInput.OutputFields)
	}

	if _, err := Build(
		mustParse(t, `index=gradethis | streamstats count AS ordinal | timechart span=5m count BY level`),
		testScope([]string{"gradethis"}, nil),
	); err != nil {
		t.Fatalf("ordinary streamstats output invalidated canonical time: %v", err)
	}
	_, err = Build(
		mustParse(t, `index=gradethis | streamstats count AS _time | timechart span=5m count BY level`),
		testScope([]string{"gradethis"}, nil),
	)
	assertDiagnosticCode(t, err, "SPL_UNSUPPORTED_TIMECHART_TIME_FIELD")

	overwritten, err := Build(
		mustParse(t, `index=gradethis | streamstats count AS index | search index=secret`),
		testScope([]string{"gradethis"}, nil),
	)
	if err != nil {
		t.Fatalf("Build overwritten index: %v", err)
	}
	if !slices.Equal(overwritten.EffectiveIndexes, []string{"gradethis"}) {
		t.Fatalf("effective indexes = %v", overwritten.EffectiveIndexes)
	}
	_, err = Build(
		mustParse(t, `index=gradethis | streamstats count AS ordinal | search index=secret`),
		testScope([]string{"gradethis"}, nil),
	)
	assertDiagnosticCode(t, err, "SPL_INDEX_FORBIDDEN")
}

func TestBuildStreamStatsRejectsForgedASTMetadata(t *testing.T) {
	t.Parallel()

	base := mustParse(t, `index=gradethis`)
	validAggregate := spl.StatsAggregate{
		Function: spl.AggregateFunctionCount,
		Alias:    "count",
	}
	validCommand := func() *spl.StreamStatsCommand {
		return &spl.StreamStatsCommand{
			Aggregate: validAggregate,
			Current:   true,
			Global:    true,
		}
	}
	build := func(command *spl.StreamStatsCommand) error {
		_, err := Build(&spl.Query{
			Search:   base.Search,
			Commands: []spl.Command{command},
			Range:    base.Range,
		}, testScope([]string{"gradethis"}, nil))
		return err
	}
	asFieldCount := func(command *spl.StreamStatsCommand) {
		command.Aggregate.Function = spl.AggregateFunctionCountValues
		command.Aggregate.Input = "status"
		command.Aggregate.InputRange = base.Range
		command.Aggregate.Alias = "count(status)"
		command.Aggregate.AliasRange = base.Range
	}

	for _, test := range []struct {
		name   string
		mutate func(*spl.StreamStatsCommand)
		code   string
	}{
		{"wrong function", func(c *spl.StreamStatsCommand) { c.Aggregate.Function = spl.AggregateFunctionSum }, "SPL_UNSUPPORTED_STREAMSTATS_AGGREGATE"},
		{"input metadata", func(c *spl.StreamStatsCommand) { c.Aggregate.Input = "status" }, "SPL_UNSUPPORTED_STREAMSTATS_AGGREGATE"},
		{"field count missing input", func(c *spl.StreamStatsCommand) {
			c.Aggregate.Function = spl.AggregateFunctionCountValues
		}, "SPL_UNSUPPORTED_STREAMSTATS_AGGREGATE"},
		{"field count missing input range", func(c *spl.StreamStatsCommand) {
			c.Aggregate.Function = spl.AggregateFunctionCountValues
			c.Aggregate.Input = "status"
			c.Aggregate.Alias = "count(status)"
		}, "SPL_UNSUPPORTED_STREAMSTATS_AGGREGATE"},
		{"field count noncanonical implicit alias", func(c *spl.StreamStatsCommand) {
			asFieldCount(c)
			c.Aggregate.Alias = "occurrences"
		}, "SPL_UNSUPPORTED_STREAMSTATS_AGGREGATE"},
		{"field count predicate metadata", func(c *spl.StreamStatsCommand) {
			asFieldCount(c)
			c.Aggregate.Predicate = &spl.WhereNotExpr{}
		}, "SPL_UNSUPPORTED_STREAMSTATS_AGGREGATE"},
		{"field count comma input", func(c *spl.StreamStatsCommand) {
			asFieldCount(c)
			c.Aggregate.Input = "status,host"
			c.Aggregate.Alias = "count(status,host)"
		}, "SPL_UNSUPPORTED_STREAMSTATS_SYNTAX"},
		{"field count parser-inexpressible explicit alias", func(c *spl.StreamStatsCommand) {
			asFieldCount(c)
			c.Aggregate.Alias = "prior value"
			c.Aggregate.ExplicitAlias = true
		}, "SPL_UNSUPPORTED_STREAMSTATS_SYNTAX"},
		{"field count default spelling forged as explicit alias", func(c *spl.StreamStatsCommand) {
			asFieldCount(c)
			c.Aggregate.ExplicitAlias = true
		}, "SPL_UNSUPPORTED_STREAMSTATS_SYNTAX"},
		{"predicate metadata", func(c *spl.StreamStatsCommand) { c.Aggregate.Predicate = &spl.WhereNotExpr{} }, "SPL_UNSUPPORTED_STREAMSTATS_AGGREGATE"},
		{"percentile metadata", func(c *spl.StreamStatsCommand) { c.Aggregate.Percentile = 50 }, "SPL_UNSUPPORTED_STREAMSTATS_AGGREGATE"},
		{"empty alias", func(c *spl.StreamStatsCommand) { c.Aggregate.Alias = "" }, "SPL_UNSUPPORTED_STREAMSTATS_AGGREGATE"},
		{"implicit custom alias", func(c *spl.StreamStatsCommand) { c.Aggregate.Alias = "rank" }, "SPL_UNSUPPORTED_STREAMSTATS_AGGREGATE"},
		{"single quoted alias", func(c *spl.StreamStatsCommand) {
			c.Aggregate.Alias = "'rank'"
			c.Aggregate.ExplicitAlias = true
		}, "SPL_UNSUPPORTED_STREAMSTATS_SYNTAX"},
		{"backtick quoted alias", func(c *spl.StreamStatsCommand) {
			c.Aggregate.Alias = "`rank`"
			c.Aggregate.ExplicitAlias = true
		}, "SPL_UNSUPPORTED_STREAMSTATS_SYNTAX"},
		{"current false without specified", func(c *spl.StreamStatsCommand) { c.Current = false }, "SPL_UNSUPPORTED_STREAMSTATS_SYNTAX"},
		{"window without specified", func(c *spl.StreamStatsCommand) { c.Window = 2 }, "SPL_UNSUPPORTED_STREAMSTATS_SYNTAX"},
		{"global false without specified", func(c *spl.StreamStatsCommand) { c.Global = false }, "SPL_UNSUPPORTED_STREAMSTATS_SYNTAX"},
		{"window over maximum", func(c *spl.StreamStatsCommand) { c.Window = spl.MaximumStreamStatsWindow + 1; c.WindowSpecified = true }, "SPL_UNSUPPORTED_STREAMSTATS_SYNTAX"},
		{"grouped window default global", func(c *spl.StreamStatsCommand) {
			c.Window = 2
			c.WindowSpecified = true
			c.GroupBy = []spl.StatsGroupField{{Name: "host"}}
		}, "SPL_UNSUPPORTED_STREAMSTATS_SYNTAX"},
		{"grouped window explicit global true", func(c *spl.StreamStatsCommand) {
			c.Window = 2
			c.WindowSpecified = true
			c.GlobalSpecified = true
			c.GroupBy = []spl.StatsGroupField{{Name: "host"}}
		}, "SPL_UNSUPPORTED_STREAMSTATS_SYNTAX"},
		{"private alias", func(c *spl.StreamStatsCommand) { c.Aggregate.Alias = "__os_private"; c.Aggregate.ExplicitAlias = true }, "SPL_RESERVED_FIELD"},
		{"single quoted group", func(c *spl.StreamStatsCommand) {
			c.GroupBy = []spl.StatsGroupField{{Name: "'host'"}}
		}, "SPL_UNSUPPORTED_STREAMSTATS_SYNTAX"},
		{"backtick quoted group", func(c *spl.StreamStatsCommand) {
			c.GroupBy = []spl.StatsGroupField{{Name: "`host`"}}
		}, "SPL_UNSUPPORTED_STREAMSTATS_SYNTAX"},
		{"comma group", func(c *spl.StreamStatsCommand) {
			c.GroupBy = []spl.StatsGroupField{{Name: "host,status"}}
		}, "SPL_UNSUPPORTED_STREAMSTATS_SYNTAX"},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			command := validCommand()
			test.mutate(command)
			assertDiagnosticCode(t, build(command), test.code)
		})
	}

	duplicate := validCommand()
	duplicate.GroupBy = []spl.StatsGroupField{{Name: "host"}, {Name: "host"}}
	assertDiagnosticCode(t, build(duplicate), "SPL_DUPLICATE_FIELD")

	tooMany := validCommand()
	tooMany.GroupBy = make([]spl.StatsGroupField, spl.MaximumStatsGroupFields+1)
	for index := range tooMany.GroupBy {
		tooMany.GroupBy[index].Name = "field_" + string(rune('a'+index))
	}
	assertDiagnosticCode(t, build(tooMany), "SPL_QUERY_TOO_COMPLEX")
}

func TestAnalyzeStreamAggregateReadsGroupsAndRejectsForgedContracts(t *testing.T) {
	t.Parallel()

	host := mustResolveStreamStatsField(t, "host")
	status := mustResolveStreamStatsField(t, "status")
	valid := func() *StreamAggregate {
		return &StreamAggregate{
			GroupBy: []FieldRef{host},
			Measure: AggregateMeasure{
				Function: AggregateFunctionCountRows,
				Output:   "ordinal",
			},
			IncludeCurrent: true,
			Global:         true,
		}
	}
	analysis, err := Analyze(&Query{Operators: []Operator{valid()}})
	if err != nil {
		t.Fatalf("Analyze valid stream aggregate: %v", err)
	}
	if !slices.Equal(analysis.ReferencedFields, []string{"host"}) {
		t.Fatalf("referenced fields = %v, want [host]", analysis.ReferencedFields)
	}
	fieldCount := valid()
	fieldCount.Measure.Function = AggregateFunctionCountValues
	fieldCount.Measure.Input = status
	fieldAnalysis, err := Analyze(&Query{Operators: []Operator{fieldCount}})
	if err != nil {
		t.Fatalf("Analyze valid field-count stream aggregate: %v", err)
	}
	if !slices.Equal(fieldAnalysis.ReferencedFields, []string{"host", "status"}) {
		t.Fatalf(
			"field-count referenced fields = %v, want [host status]",
			fieldAnalysis.ReferencedFields,
		)
	}

	for _, operator := range []*StreamAggregate{
		{Measure: AggregateMeasure{Function: AggregateFunctionCountRows, Output: "ordinal"}, Global: true},
		{Measure: AggregateMeasure{Function: AggregateFunctionCountRows, Output: "ordinal"}, Global: false},
		{GroupBy: []FieldRef{host}, Measure: AggregateMeasure{Function: AggregateFunctionCountRows, Output: "ordinal"}, Global: false, WindowRows: 2},
		{GroupBy: []FieldRef{host}, Measure: AggregateMeasure{Function: AggregateFunctionCountRows, Output: "ordinal"}, Global: true, WindowRows: 0},
		{Measure: AggregateMeasure{Function: AggregateFunctionCountValues, Input: status, Output: "ordinal"}, Global: true},
		{Measure: AggregateMeasure{Function: AggregateFunctionCountValues, Input: status, Output: "count(status)"}, Global: true},
		{GroupBy: []FieldRef{host}, Measure: AggregateMeasure{Function: AggregateFunctionCountValues, Input: status, Output: "ordinal"}, Global: false, WindowRows: 2},
	} {
		if _, err := Analyze(&Query{Operators: []Operator{operator}}); err != nil {
			t.Fatalf("Analyze valid option combination %#v: %v", operator, err)
		}
	}

	for _, test := range []struct {
		name   string
		mutate func(*StreamAggregate)
	}{
		{"missing output", func(op *StreamAggregate) { op.Measure.Output = "" }},
		{"wrong function", func(op *StreamAggregate) { op.Measure.Function = AggregateFunctionSum }},
		{"field count missing input", func(op *StreamAggregate) { op.Measure.Function = AggregateFunctionCountValues }},
		{"field count comma input", func(op *StreamAggregate) {
			op.Measure.Function = AggregateFunctionCountValues
			op.Measure.Input = mustResolveStreamStatsField(t, "status,host")
		}},
		{"field count whitespace input", func(op *StreamAggregate) {
			op.Measure.Function = AggregateFunctionCountValues
			op.Measure.Input = mustResolveStreamStatsField(t, "status host")
		}},
		{"field count parenthesized input", func(op *StreamAggregate) {
			op.Measure.Function = AggregateFunctionCountValues
			op.Measure.Input = mustResolveStreamStatsField(t, "status(host)")
		}},
		{"mismatched field count default output", func(op *StreamAggregate) {
			op.Measure.Function = AggregateFunctionCountValues
			op.Measure.Input = status
			op.Measure.Output = "count(host)"
		}},
		{"row count parenthesized output", func(op *StreamAggregate) {
			op.Measure.Output = "count(status)"
		}},
		{"input metadata", func(op *StreamAggregate) { op.Measure.Input = host }},
		{"predicate metadata", func(op *StreamAggregate) { op.Measure.Predicate = &BooleanExpression{} }},
		{"percentile metadata", func(op *StreamAggregate) { op.Measure.Percentile = 1 }},
		{"over window", func(op *StreamAggregate) { op.WindowRows = spl.MaximumStreamStatsWindow + 1 }},
		{"grouped global window", func(op *StreamAggregate) { op.WindowRows = 2; op.Global = true }},
		{"duplicate group", func(op *StreamAggregate) { op.GroupBy = []FieldRef{host, host} }},
		{"malformed group", func(op *StreamAggregate) { op.GroupBy = []FieldRef{{Name: "host"}} }},
		{"private output", func(op *StreamAggregate) { op.Measure.Output = "__os_private" }},
		{"single quoted output", func(op *StreamAggregate) { op.Measure.Output = "'ordinal'" }},
		{"backtick quoted output", func(op *StreamAggregate) { op.Measure.Output = "`ordinal`" }},
		{"single quoted group", func(op *StreamAggregate) {
			op.GroupBy = []FieldRef{mustResolveStreamStatsField(t, "'host'")}
		}},
		{"backtick quoted group", func(op *StreamAggregate) {
			op.GroupBy = []FieldRef{mustResolveStreamStatsField(t, "`host`")}
		}},
		{"whitespace group", func(op *StreamAggregate) {
			op.GroupBy = []FieldRef{mustResolveStreamStatsField(t, "host status")}
		}},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			operator := valid()
			test.mutate(operator)
			if _, err := Analyze(&Query{Operators: []Operator{operator}}); err == nil {
				t.Fatalf("Analyze accepted forged stream aggregate: %#v", operator)
			}
		})
	}

	tooMany := valid()
	tooMany.GroupBy = make([]FieldRef, spl.MaximumStatsGroupFields+1)
	for index := range tooMany.GroupBy {
		tooMany.GroupBy[index] = mustResolveStreamStatsField(t, "field_"+string(rune('a'+index)))
	}
	if _, err := Analyze(&Query{Operators: []Operator{tooMany}}); err == nil {
		t.Fatal("Analyze accepted too many streamstats groups")
	}

	var typedNil *StreamAggregate
	if _, err := Analyze(&Query{Operators: []Operator{typedNil}}); err == nil {
		t.Fatal("Analyze accepted typed-nil StreamAggregate")
	}
}

func TestStreamAggregatePreservesEventAnalysisAndCanonicalTimeline(t *testing.T) {
	t.Parallel()

	ordinary, err := Build(
		mustParse(t, `index=gradethis | streamstats count AS ordinal BY host`),
		testScope([]string{"gradethis"}, nil),
	)
	if err != nil {
		t.Fatalf("Build ordinary streamstats: %v", err)
	}
	if err := ValidateFieldAnalysisEligibility(ordinary); err != nil {
		t.Fatalf("ValidateFieldAnalysisEligibility ordinary: %v", err)
	}
	if err := ValidateTimelineEligibility(ordinary); err != nil {
		t.Fatalf("ValidateTimelineEligibility ordinary: %v", err)
	}

	overwrittenTime, err := Build(
		mustParse(t, `index=gradethis | streamstats count AS _time`),
		testScope([]string{"gradethis"}, nil),
	)
	if err != nil {
		t.Fatalf("Build _time streamstats: %v", err)
	}
	if err := ValidateFieldAnalysisEligibility(overwrittenTime); err != nil {
		t.Fatalf("ValidateFieldAnalysisEligibility overwritten _time: %v", err)
	}
	assertDiagnosticCode(
		t,
		ValidateTimelineEligibility(overwrittenTime),
		timelineTimeDiagnosticCode,
	)

	validOperator := ordinary.Operators[len(ordinary.Operators)-1].(*StreamAggregate)
	for _, test := range []struct {
		name     string
		operator Operator
	}{
		{
			name: "forged contract",
			operator: &StreamAggregate{
				Measure: AggregateMeasure{
					Function: AggregateFunctionCountValues,
					Output:   validOperator.Measure.Output,
				},
				IncludeCurrent: true,
				Global:         true,
			},
		},
		{name: "typed nil", operator: (*StreamAggregate)(nil)},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			query := *ordinary
			query.Operators = append([]Operator(nil), ordinary.Operators...)
			query.Operators[len(query.Operators)-1] = test.operator
			assertDiagnosticCode(
				t,
				ValidateFieldAnalysisEligibility(&query),
				fieldAnalysisPipelineDiagnosticCode,
			)
			assertDiagnosticCode(
				t,
				ValidateTimelineEligibility(&query),
				timelinePipelineDiagnosticCode,
			)
		})
	}
}

func mustResolveStreamStatsField(t *testing.T, name string) FieldRef {
	t.Helper()
	field, err := ResolveField(name, spl.Range{
		Start: spl.Position{Line: 1, Column: 1},
		End:   spl.Position{Line: 1, Column: len(name) + 1},
	})
	if err != nil {
		t.Fatalf("ResolveField(%q): %v", name, err)
	}
	return field
}
