package plan

import (
	"slices"
	"strings"
	"testing"
	"time"
)

func TestAnalyzeOperatorReadPositions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		operator Operator
		want     []string
	}{
		{
			name:     "scan",
			operator: &Scan{Indexes: []string{"index-name-is-not-a-field"}},
		},
		{
			name: "filter",
			operator: &Filter{Expression: &ComparisonExpression{
				Field: analysisField("filter_field"),
			}},
			want: []string{"filter_field"},
		},
		{
			name: "project include",
			operator: &Project{
				Mode:   ProjectModeInclude,
				Fields: []FieldRef{analysisField("project_z"), analysisField("project_a")},
			},
			want: []string{"project_a", "project_z"},
		},
		{
			name: "project exclude",
			operator: &Project{
				Mode:   ProjectModeExclude,
				Fields: []FieldRef{analysisField("excluded_field")},
			},
			want: []string{"excluded_field"},
		},
		{
			name: "project table",
			operator: &Project{
				Mode:   ProjectModeTable,
				Fields: []FieldRef{analysisField("table_field")},
			},
			want: []string{"table_field"},
		},
		{
			name: "extend assignments are evaluated left to right",
			operator: &Extend{Assignments: []ExtendAssignment{
				{
					Output:     analysisField("first_output"),
					Expression: analysisScalarField("extend_input"),
				},
				{
					Output:     analysisField("write_only_output"),
					Expression: analysisScalarField("first_output"),
				},
			}},
			want: []string{"extend_input", "first_output"},
		},
		{
			name: "time bucket",
			operator: &TimeBucket{
				Field:  analysisField("time_bucket_input"),
				Output: analysisField("time_bucket_output"),
				Span:   time.Second,
			},
			want: []string{"time_bucket_input"},
		},
		{
			name: "numeric bucket",
			operator: &NumericBucket{
				Input:  analysisField("numeric_bucket_input"),
				Output: analysisField("numeric_bucket_output"),
			},
			want: []string{"numeric_bucket_input"},
		},
		{
			name: "regular expression extraction",
			operator: &Extract{
				Input: analysisField("rex_input"),
				Captures: []ExtractCapture{
					{Output: analysisField("rex_output_one"), Group: 1},
					{Output: analysisField("rex_output_two"), Group: 2},
				},
			},
			want: []string{"rex_input"},
		},
		{
			name: "JSON extraction",
			operator: &ExtractJSON{
				Input:  analysisField("spath_input"),
				Output: analysisField("spath_output"),
			},
			want: []string{"spath_input"},
		},
		{
			name: "rename",
			operator: &Rename{Assignments: []RenameAssignment{
				{
					Source:      analysisField("rename_source"),
					Destination: analysisField("rename_destination"),
				},
			}},
			want: []string{"rename_source"},
		},
		{
			name: "aggregate grouping measures and predicate",
			operator: &Aggregate{
				GroupBy: []FieldRef{mustResolveEventAggregateField(t, "aggregate_group")},
				Measures: []AggregateMeasure{
					{Function: AggregateFunctionCountRows, Output: "row_count"},
					{
						Function: AggregateFunctionSum,
						Input:    mustResolveEventAggregateField(t, "aggregate_measure"),
						Output:   "total",
					},
					{
						Function: AggregateFunctionCountPredicate,
						Predicate: &EvalComparisonExpression{
							Left: &ScalarFieldExpression{
								Field: mustResolveEventAggregateField(t, "aggregate_predicate"),
							},
							Op: ComparisonOpEqual,
							Right: &ScalarLiteralExpression{
								Value: Value{Kind: ValueKindInt64, Int64: 1},
							},
						},
						Output: "conditional_count",
					},
				},
			},
			want: []string{
				"aggregate_group",
				"aggregate_measure",
				"aggregate_predicate",
			},
		},
		{
			name: "chronological aggregate implicit time dependency",
			operator: &Aggregate{Measures: []AggregateMeasure{
				{
					Function: AggregateFunctionEarliest,
					Input:    mustResolveEventAggregateField(t, "chronological_input"),
					Output:   "first",
				},
			}},
			want: []string{"_time", "chronological_input"},
		},
		{
			name: "timechart axes",
			operator: &Timechart{
				Time:    mustResolveEventAggregateField(t, "_time"),
				Measure: AggregateMeasure{Function: AggregateFunctionCountRows, Output: "count"},
				Span:    time.Minute,
				Split: &TimechartSplit{
					Field:        mustResolveEventAggregateField(t, "timechart_series"),
					SeriesLimit:  timechartSeriesLimit,
					IncludeNull:  true,
					IncludeOther: true,
					NullLabel:    "NULL",
					OtherLabel:   "OTHER",
				},
			},
			want: []string{"_time", "timechart_series"},
		},
		{
			name: "unsplit timechart",
			operator: &Timechart{
				Time:    mustResolveEventAggregateField(t, "_time"),
				Measure: AggregateMeasure{Function: AggregateFunctionCountRows, Output: "count"},
				Span:    time.Minute,
			},
			want: []string{"_time"},
		},
		{
			name: "chart axes",
			operator: &Chart{
				Over:         mustResolveEventAggregateField(t, "chart_rows"),
				SplitBy:      mustResolveEventAggregateField(t, "chart_series"),
				Measure:      AggregateMeasure{Function: AggregateFunctionCountRows, Output: "count"},
				RowLimit:     maxChartRows,
				SeriesLimit:  chartSeriesLimit,
				IncludeNull:  true,
				IncludeOther: true,
				NullLabel:    "NULL",
				OtherLabel:   "OTHER",
			},
			want: []string{"chart_rows", "chart_series"},
		},
		{
			name: "window",
			operator: &Window{
				Input:  analysisField("window_input"),
				Output: "window_output",
			},
			want: []string{"window_input"},
		},
		{
			name: "sort",
			operator: &Sort{Keys: []SortKey{
				{Field: analysisField("sort_z")},
				{Field: analysisField("sort_a")},
			}},
			want: []string{"sort_a", "sort_z"},
		},
		{
			name: "deduplicate",
			operator: &Deduplicate{Keys: []FieldRef{
				analysisField("dedup_z"),
				analysisField("dedup_a"),
			}},
			want: []string{"dedup_a", "dedup_z"},
		},
		{
			name:     "limit",
			operator: &Limit{Count: 10},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			got, err := Analyze(&Query{Operators: []Operator{test.operator}})
			if err != nil {
				t.Fatalf("Analyze: %v", err)
			}
			if !slices.Equal(got.ReferencedFields, test.want) {
				t.Fatalf("ReferencedFields = %v, want %v", got.ReferencedFields, test.want)
			}
		})
	}
}

func TestAnalyzeExpressionReadPositions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		expression Expression
		want       []string
	}{
		{
			name: "boolean operands",
			expression: &BooleanExpression{
				Left:  analysisComparison("boolean_left"),
				Right: analysisComparison("boolean_right"),
			},
			want: []string{"boolean_left", "boolean_right"},
		},
		{
			name:       "not operand",
			expression: &NotExpression{Operand: analysisComparison("not_operand")},
			want:       []string{"not_operand"},
		},
		{
			name:       "free text reads canonical raw",
			expression: &TextExpression{Value: "needle"},
			want:       []string{"_raw"},
		},
		{
			name:       "comparison field",
			expression: analysisComparison("comparison_field"),
			want:       []string{"comparison_field"},
		},
		{
			name: "eval comparison operands",
			expression: &EvalComparisonExpression{
				Left:  analysisScalarField("eval_left"),
				Right: analysisScalarField("eval_right"),
			},
			want: []string{"eval_left", "eval_right"},
		},
		{
			name: "scalar predicate value",
			expression: &ScalarPredicateExpression{
				Value: analysisScalarField("predicate_value"),
			},
			want: []string{"predicate_value"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			got, err := Analyze(&Query{Operators: []Operator{
				&Filter{Expression: test.expression},
			}})
			if err != nil {
				t.Fatalf("Analyze: %v", err)
			}
			if !slices.Equal(got.ReferencedFields, test.want) {
				t.Fatalf("ReferencedFields = %v, want %v", got.ReferencedFields, test.want)
			}
		})
	}
}

func TestAnalyzeScalarExpressionReadPositions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		expression ScalarExpression
		want       []string
	}{
		{
			name:       "field",
			expression: analysisScalarField("scalar_field"),
			want:       []string{"scalar_field"},
		},
		{
			name:       "literal",
			expression: &ScalarLiteralExpression{},
		},
		{
			name: "call arguments",
			expression: &ScalarCallExpression{Arguments: []ScalarExpression{
				analysisScalarField("call_z"),
				&ScalarLiteralExpression{},
				analysisScalarField("call_a"),
			}},
			want: []string{"call_a", "call_z"},
		},
		{
			name: "if condition and branches",
			expression: &ScalarIfExpression{
				Condition: analysisComparison("if_condition"),
				True:      analysisScalarField("if_true"),
				False:     analysisScalarField("if_false"),
			},
			want: []string{"if_condition", "if_false", "if_true"},
		},
		{
			name: "case conditions and values",
			expression: &ScalarCaseExpression{Branches: []ScalarCaseBranch{
				{
					Condition: analysisComparison("case_condition_one"),
					Value:     analysisScalarField("case_value_one"),
				},
				{
					Condition: analysisComparison("case_condition_two"),
					Value:     analysisScalarField("case_value_two"),
				},
			}},
			want: []string{
				"case_condition_one",
				"case_condition_two",
				"case_value_one",
				"case_value_two",
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			got, err := Analyze(&Query{Operators: []Operator{
				&Extend{Assignments: []ExtendAssignment{{
					Output:     analysisField("write_only"),
					Expression: test.expression,
				}}},
			}})
			if err != nil {
				t.Fatalf("Analyze: %v", err)
			}
			if !slices.Equal(got.ReferencedFields, test.want) {
				t.Fatalf("ReferencedFields = %v, want %v", got.ReferencedFields, test.want)
			}
		})
	}
}

func TestAnalyzeSortsAndDeduplicatesReferencedFields(t *testing.T) {
	t.Parallel()

	got, err := Analyze(&Query{Operators: []Operator{
		&Filter{Expression: &BooleanExpression{
			Left:  analysisComparison("z"),
			Right: analysisComparison("a"),
		}},
		&Project{
			Mode: ProjectModeInclude,
			Fields: []FieldRef{
				analysisField("z"),
				analysisField("middle"),
				analysisField("a"),
				analysisField("middle"),
			},
		},
		&Sort{Keys: []SortKey{{Field: analysisField("z")}}},
	}})
	if err != nil {
		t.Fatalf("Analyze: %v", err)
	}
	want := []string{"a", "middle", "z"}
	if !slices.Equal(got.ReferencedFields, want) {
		t.Fatalf("ReferencedFields = %v, want %v", got.ReferencedFields, want)
	}
}

func TestAnalyzeExcludesWriteOnlyOutputsUntilRead(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		operator Operator
		inputs   []string
		outputs  []string
	}{
		{
			name: "extend",
			operator: &Extend{Assignments: []ExtendAssignment{{
				Output:     analysisField("extend_output"),
				Expression: analysisScalarField("extend_input"),
			}}},
			inputs:  []string{"extend_input"},
			outputs: []string{"extend_output"},
		},
		{
			name: "time bucket",
			operator: &TimeBucket{
				Field:  analysisField("time_input"),
				Output: analysisField("time_output"),
				Span:   time.Second,
			},
			inputs:  []string{"time_input"},
			outputs: []string{"time_output"},
		},
		{
			name: "numeric bucket",
			operator: &NumericBucket{
				Input:  analysisField("number_input"),
				Output: analysisField("number_output"),
			},
			inputs:  []string{"number_input"},
			outputs: []string{"number_output"},
		},
		{
			name: "regular expression extraction",
			operator: &Extract{
				Input: analysisField("rex_input"),
				Captures: []ExtractCapture{
					{Output: analysisField("rex_output_z")},
					{Output: analysisField("rex_output_a")},
				},
			},
			inputs:  []string{"rex_input"},
			outputs: []string{"rex_output_a", "rex_output_z"},
		},
		{
			name: "JSON extraction",
			operator: &ExtractJSON{
				Input:  analysisField("spath_input"),
				Output: analysisField("spath_output"),
			},
			inputs:  []string{"spath_input"},
			outputs: []string{"spath_output"},
		},
		{
			name: "rename",
			operator: &Rename{Assignments: []RenameAssignment{{
				Source:      analysisField("rename_source"),
				Destination: analysisField("rename_destination"),
			}}},
			inputs:  []string{"rename_source"},
			outputs: []string{"rename_destination"},
		},
		{
			name: "aggregate",
			operator: &Aggregate{Measures: []AggregateMeasure{{
				Function: AggregateFunctionSum,
				Input:    mustResolveEventAggregateField(t, "aggregate_input"),
				Output:   "aggregate_output",
			}}},
			inputs:  []string{"aggregate_input"},
			outputs: []string{"aggregate_output"},
		},
		{
			name: "window",
			operator: &Window{
				Input:  analysisField("window_input"),
				Output: "window_output",
			},
			inputs:  []string{"window_input"},
			outputs: []string{"window_output"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			writeOnly, err := Analyze(&Query{Operators: []Operator{test.operator}})
			if err != nil {
				t.Fatalf("Analyze write-only output: %v", err)
			}
			if !slices.Equal(writeOnly.ReferencedFields, test.inputs) {
				t.Fatalf(
					"write-only ReferencedFields = %v, want %v",
					writeOnly.ReferencedFields,
					test.inputs,
				)
			}

			outputFields := make([]FieldRef, 0, len(test.outputs))
			for _, output := range test.outputs {
				outputFields = append(outputFields, analysisField(output))
			}
			laterRead, err := Analyze(&Query{Operators: []Operator{
				test.operator,
				&Project{Mode: ProjectModeInclude, Fields: outputFields},
			}})
			if err != nil {
				t.Fatalf("Analyze later read: %v", err)
			}
			want := append(slices.Clone(test.inputs), test.outputs...)
			slices.Sort(want)
			if !slices.Equal(laterRead.ReferencedFields, want) {
				t.Fatalf(
					"later-read ReferencedFields = %v, want %v",
					laterRead.ReferencedFields,
					want,
				)
			}
		})
	}
}

func TestAnalyzeRejectsTypedNils(t *testing.T) {
	t.Parallel()

	var (
		nilScan        *Scan
		nilFilter      *Filter
		nilProject     *Project
		nilExtend      *Extend
		nilTimeBucket  *TimeBucket
		nilNumeric     *NumericBucket
		nilExtract     *Extract
		nilExtractJSON *ExtractJSON
		nilRename      *Rename
		nilAggregate   *Aggregate
		nilTimechart   *Timechart
		nilChart       *Chart
		nilWindow      *Window
		nilSort        *Sort
		nilDeduplicate *Deduplicate
		nilLimit       *Limit
		nilBoolean     *BooleanExpression
		nilNot         *NotExpression
		nilText        *TextExpression
		nilComparison  *ComparisonExpression
		nilEvalCompare *EvalComparisonExpression
		nilPredicate   *ScalarPredicateExpression
		nilScalarField *ScalarFieldExpression
		nilScalarLit   *ScalarLiteralExpression
		nilScalarCall  *ScalarCallExpression
		nilScalarIf    *ScalarIfExpression
		nilScalarCase  *ScalarCaseExpression
	)

	operatorTests := []struct {
		name     string
		operator Operator
	}{
		{name: "scan", operator: nilScan},
		{name: "filter", operator: nilFilter},
		{name: "project", operator: nilProject},
		{name: "extend", operator: nilExtend},
		{name: "time bucket", operator: nilTimeBucket},
		{name: "numeric bucket", operator: nilNumeric},
		{name: "extract", operator: nilExtract},
		{name: "JSON extract", operator: nilExtractJSON},
		{name: "rename", operator: nilRename},
		{name: "aggregate", operator: nilAggregate},
		{name: "timechart", operator: nilTimechart},
		{name: "chart", operator: nilChart},
		{name: "window", operator: nilWindow},
		{name: "sort", operator: nilSort},
		{name: "deduplicate", operator: nilDeduplicate},
		{name: "limit", operator: nilLimit},
	}
	for _, test := range operatorTests {
		t.Run("operator "+test.name, func(t *testing.T) {
			t.Parallel()
			analysisRequireError(t, &Query{Operators: []Operator{test.operator}})
		})
	}

	expressionTests := []struct {
		name       string
		expression Expression
	}{
		{name: "boolean", expression: nilBoolean},
		{name: "not", expression: nilNot},
		{name: "text", expression: nilText},
		{name: "comparison", expression: nilComparison},
		{name: "eval comparison", expression: nilEvalCompare},
		{name: "scalar predicate", expression: nilPredicate},
	}
	for _, test := range expressionTests {
		t.Run("expression "+test.name, func(t *testing.T) {
			t.Parallel()
			analysisRequireError(t, &Query{Operators: []Operator{
				&Filter{Expression: test.expression},
			}})
		})
	}

	scalarTests := []struct {
		name       string
		expression ScalarExpression
	}{
		{name: "field", expression: nilScalarField},
		{name: "literal", expression: nilScalarLit},
		{name: "call", expression: nilScalarCall},
		{name: "if", expression: nilScalarIf},
		{name: "case", expression: nilScalarCase},
	}
	for _, test := range scalarTests {
		t.Run("scalar "+test.name, func(t *testing.T) {
			t.Parallel()
			analysisRequireError(t, &Query{Operators: []Operator{
				&Extend{Assignments: []ExtendAssignment{{
					Output:     analysisField("output"),
					Expression: test.expression,
				}}},
			}})
		})
	}

	t.Run("query", func(t *testing.T) {
		t.Parallel()
		analysisRequireError(t, nil)
	})
}

func TestAnalyzeRejectsCyclicAndDeepExpressionTrees(t *testing.T) {
	t.Parallel()

	cyclicPredicate := &NotExpression{}
	cyclicPredicate.Operand = cyclicPredicate

	cyclicScalar := &ScalarCallExpression{}
	cyclicScalar.Arguments = []ScalarExpression{cyclicScalar}

	deepPredicate := Expression(&TextExpression{Value: "leaf"})
	for range maximumAnalysisDepth + 1 {
		deepPredicate = &NotExpression{Operand: deepPredicate}
	}

	tests := []struct {
		name  string
		query *Query
	}{
		{
			name: "cyclic predicate",
			query: &Query{Operators: []Operator{
				&Filter{Expression: cyclicPredicate},
			}},
		},
		{
			name: "cyclic scalar",
			query: &Query{Operators: []Operator{
				&Extend{Assignments: []ExtendAssignment{{
					Output:     analysisField("output"),
					Expression: cyclicScalar,
				}}},
			}},
		},
		{
			name: "deep predicate",
			query: &Query{Operators: []Operator{
				&Filter{Expression: deepPredicate},
			}},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			analysisRequireError(t, test.query)
		})
	}
}

func TestAnalyzeEnforcesNodeBudgetAcrossOversizedSlices(t *testing.T) {
	t.Parallel()

	t.Run("operator boundary is accepted", func(t *testing.T) {
		t.Parallel()
		operators := make([]Operator, maximumAnalysisNodes)
		for index := range operators {
			operators[index] = &Scan{}
		}
		if _, err := Analyze(&Query{Operators: operators}); err != nil {
			t.Fatalf("Analyze at node boundary: %v", err)
		}
	})

	t.Run("operator slice above boundary", func(t *testing.T) {
		t.Parallel()
		operators := make([]Operator, maximumAnalysisNodes+1)
		for index := range operators {
			operators[index] = &Scan{}
		}
		analysisRequireNodeBudgetError(t, &Query{Operators: operators})
	})

	t.Run("repeated field slice above remaining budget", func(t *testing.T) {
		t.Parallel()
		fields := make([]FieldRef, maximumAnalysisNodes)
		for index := range fields {
			fields[index] = analysisField("same_field")
		}
		analysisRequireNodeBudgetError(t, &Query{Operators: []Operator{
			&Project{Mode: ProjectModeInclude, Fields: fields},
		}})
	})

	t.Run("scalar argument slice above remaining budget", func(t *testing.T) {
		t.Parallel()
		arguments := make([]ScalarExpression, maximumAnalysisNodes)
		for index := range arguments {
			arguments[index] = &ScalarLiteralExpression{}
		}
		analysisRequireNodeBudgetError(t, &Query{Operators: []Operator{
			&Extend{Assignments: []ExtendAssignment{{
				Output: analysisField("output"),
				Expression: &ScalarCallExpression{
					Arguments: arguments,
				},
			}}},
		}})
	})
}

func TestAnalyzeRejectsEmptyFieldReferences(t *testing.T) {
	t.Parallel()

	valid := analysisField("valid")
	tests := []struct {
		name     string
		operator Operator
	}{
		{
			name:     "project field",
			operator: &Project{Mode: ProjectModeInclude, Fields: []FieldRef{{}}},
		},
		{
			name: "extend output",
			operator: &Extend{Assignments: []ExtendAssignment{{
				Expression: &ScalarLiteralExpression{},
			}}},
		},
		{
			name:     "time bucket input",
			operator: &TimeBucket{Output: valid, Span: time.Second},
		},
		{
			name:     "time bucket output",
			operator: &TimeBucket{Field: valid, Span: time.Second},
		},
		{
			name:     "numeric bucket input",
			operator: &NumericBucket{Output: valid},
		},
		{
			name:     "numeric bucket output",
			operator: &NumericBucket{Input: valid},
		},
		{
			name: "regular expression input",
			operator: &Extract{
				Captures: []ExtractCapture{{Output: valid}},
			},
		},
		{
			name: "regular expression output",
			operator: &Extract{
				Input:    valid,
				Captures: []ExtractCapture{{}},
			},
		},
		{
			name:     "JSON extraction input",
			operator: &ExtractJSON{Output: valid},
		},
		{
			name:     "JSON extraction output",
			operator: &ExtractJSON{Input: valid},
		},
		{
			name: "rename source",
			operator: &Rename{Assignments: []RenameAssignment{{
				Destination: valid,
			}}},
		},
		{
			name: "rename destination",
			operator: &Rename{Assignments: []RenameAssignment{{
				Source: valid,
			}}},
		},
		{
			name:     "aggregate group",
			operator: &Aggregate{GroupBy: []FieldRef{{}}},
		},
		{
			name: "aggregate measure",
			operator: &Aggregate{Measures: []AggregateMeasure{{
				Function: AggregateFunctionSum,
				Output:   "total",
			}}},
		},
		{
			name:     "timechart time",
			operator: &Timechart{Span: time.Minute, Split: &TimechartSplit{Field: valid}},
		},
		{
			name:     "timechart split",
			operator: &Timechart{Time: valid, Span: time.Minute, Split: &TimechartSplit{}},
		},
		{
			name:     "chart over",
			operator: &Chart{SplitBy: valid},
		},
		{
			name:     "chart split",
			operator: &Chart{Over: valid},
		},
		{
			name:     "window input",
			operator: &Window{Output: "percent"},
		},
		{
			name:     "sort key",
			operator: &Sort{Keys: []SortKey{{}}},
		},
		{
			name:     "deduplicate key",
			operator: &Deduplicate{Keys: []FieldRef{{}}},
		},
		{
			name:     "comparison field",
			operator: &Filter{Expression: &ComparisonExpression{}},
		},
		{
			name: "scalar field",
			operator: &Extend{Assignments: []ExtendAssignment{{
				Output:     valid,
				Expression: &ScalarFieldExpression{},
			}}},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			analysisRequireError(t, &Query{Operators: []Operator{test.operator}})
		})
	}
}

func analysisField(name string) FieldRef {
	return FieldRef{Name: name}
}

func analysisComparison(name string) *ComparisonExpression {
	return &ComparisonExpression{Field: analysisField(name)}
}

func analysisScalarField(name string) *ScalarFieldExpression {
	return &ScalarFieldExpression{Field: analysisField(name)}
}

func analysisRequireError(t *testing.T, query *Query) {
	t.Helper()
	if _, err := Analyze(query); err == nil {
		t.Fatal("Analyze error = nil, want rejection")
	}
}

func analysisRequireNodeBudgetError(t *testing.T, query *Query) {
	t.Helper()
	_, err := Analyze(query)
	if err == nil || !strings.Contains(err.Error(), "nodes") {
		t.Fatalf("Analyze error = %v, want node-budget rejection", err)
	}
}
