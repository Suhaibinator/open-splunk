package plan

import (
	"errors"
	"slices"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/spl"
)

func TestBuildStreamStatsCountEvalProducesBoundedPredicateMeasure(t *testing.T) {
	t.Parallel()

	logical, err := Build(
		mustParse(
			t,
			`index=gradethis | streamstats current=f window=3 global=f count(eval(isnull(probe) OR NOT status=200)) AS matches BY host`,
		),
		testScope([]string{"gradethis"}, nil),
	)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	streamAggregate, ok :=
		logical.Operators[len(logical.Operators)-1].(*StreamAggregate)
	if !ok {
		t.Fatalf(
			"last operator = %T, want *StreamAggregate",
			logical.Operators[len(logical.Operators)-1],
		)
	}
	measure := streamAggregate.Measure
	if measure.Function != AggregateFunctionCountPredicate ||
		measure.Input.Name != "" ||
		measure.Input.Canonical ||
		measure.Input.Path != nil ||
		measure.Input.Range != (spl.Range{}) ||
		measure.Predicate == nil ||
		measure.Percentile != 0 ||
		measure.Output != "matches" {
		t.Fatalf("conditional measure = %#v", measure)
	}
	root, ok := measure.Predicate.(*BooleanExpression)
	if !ok || root.Op != BooleanOpOr {
		t.Fatalf("predicate = %#v, want BooleanExpression OR", measure.Predicate)
	}
	if _, ok := root.Left.(*ScalarPredicateExpression); !ok {
		t.Fatalf("predicate left = %T, want *ScalarPredicateExpression", root.Left)
	}
	if _, ok := root.Right.(*NotExpression); !ok {
		t.Fatalf("predicate right = %T, want *NotExpression", root.Right)
	}
	if streamAggregate.IncludeCurrent ||
		streamAggregate.WindowRows != 3 ||
		streamAggregate.Global ||
		len(streamAggregate.GroupBy) != 1 ||
		streamAggregate.GroupBy[0].Name != "host" {
		t.Fatalf("stream aggregate frame/groups = %#v", streamAggregate)
	}

	analysis, err := Analyze(logical)
	if err != nil {
		t.Fatalf("Analyze: %v", err)
	}
	if !slices.Equal(
		analysis.ReferencedFields,
		[]string{"host", "index", "probe", "status"},
	) {
		t.Fatalf(
			"referenced fields = %v, want [host index probe status]",
			analysis.ReferencedFields,
		)
	}
	if err := ValidateFieldAnalysisEligibility(logical); err != nil {
		t.Fatalf("field analysis eligibility: %v", err)
	}
	if err := ValidateTimelineEligibility(logical); err != nil {
		t.Fatalf("timeline eligibility: %v", err)
	}
}

func TestBuildStreamStatsCountEvalReadsIncomingFieldsBeforeSchemaUpsert(
	t *testing.T,
) {
	t.Parallel()

	tests := []struct {
		name       string
		source     string
		wantOutput []string
	}{
		{
			name: "same name input and output",
			source: `index=gradethis | table host,status | ` +
				`streamstats count(eval(status=500)) AS status BY host`,
			wantOutput: []string{"host", "status"},
		},
		{
			name: "predicate input projected away",
			source: `index=gradethis | table host | ` +
				`streamstats count(eval(status=500)) AS matches BY host`,
			wantOutput: []string{"host", "matches"},
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
			if !slices.Equal(logical.OutputFields, test.wantOutput) {
				t.Fatalf(
					"output fields = %v, want %v",
					logical.OutputFields,
					test.wantOutput,
				)
			}
			operator, ok :=
				logical.Operators[len(logical.Operators)-1].(*StreamAggregate)
			if !ok {
				t.Fatalf("last operator = %T", logical.Operators[len(logical.Operators)-1])
			}
			comparison, ok := operator.Measure.Predicate.(*EvalComparisonExpression)
			if !ok {
				t.Fatalf("predicate = %T, want *EvalComparisonExpression", operator.Measure.Predicate)
			}
			field, ok := comparison.Left.(*ScalarFieldExpression)
			if !ok || field.Field.Name != "status" || field.Field.Range == (spl.Range{}) {
				t.Fatalf("predicate input = %#v, want source-located status", comparison.Left)
			}
			analysis, err := Analyze(logical)
			if err != nil {
				t.Fatalf("Analyze: %v", err)
			}
			if !slices.Contains(analysis.ReferencedFields, "status") {
				t.Fatalf(
					"referenced fields = %v, want incoming status dependency",
					analysis.ReferencedFields,
				)
			}
		})
	}
}

func TestBuildStreamStatsCountEvalEnforcesReservedOpenSchemaBoundary(t *testing.T) {
	t.Parallel()

	_, err := Build(
		mustParse(
			t,
			`index=gradethis | streamstats count(eval(fields="value")) AS matches`,
		),
		testScope([]string{"gradethis"}, nil),
	)
	assertDiagnosticCode(t, err, "SPL_AMBIGUOUS_STREAMSTATS_FIELD")
	var diagnostic *Diagnostic
	if !errors.As(err, &diagnostic) || diagnostic.Range == (spl.Range{}) {
		t.Fatalf("reserved-field diagnostic = %#v, want source-located range", err)
	}

	logical, err := Build(
		mustParse(
			t,
			`index=gradethis | table fields,host | streamstats count(eval(fields="value")) AS matches BY host`,
		),
		testScope([]string{"gradethis"}, nil),
	)
	if err != nil {
		t.Fatalf("Build closed schema: %v", err)
	}
	if !slices.Equal(logical.OutputFields, []string{"fields", "host", "matches"}) {
		t.Fatalf(
			"closed output fields = %v, want [fields host matches]",
			logical.OutputFields,
		)
	}
}

func TestBuildStreamStatsCountEvalRejectsForgedMetadataAndPredicates(
	t *testing.T,
) {
	t.Parallel()

	base := mustParse(t, `index=gradethis`)
	fieldRange := spl.Range{
		Start: spl.Position{Line: 1, Column: 1},
		End:   spl.Position{Line: 1, Column: 7},
	}
	validPredicate := func() spl.WhereExpr {
		return &spl.WhereComparisonExpr{
			Left: &spl.ScalarFieldExpr{Field: "status", Range: fieldRange},
			Op:   spl.CompareOpEqual,
			Right: &spl.ScalarLiteralExpr{
				Value: spl.Literal{
					Kind:  spl.LiteralKindInteger,
					Text:  "200",
					Range: fieldRange,
				},
				Range: fieldRange,
			},
			Range: fieldRange,
		}
	}
	var typedNil *spl.WhereComparisonExpr
	tests := []struct {
		name      string
		aggregate spl.StatsAggregate
		wantCode  string
	}{
		{
			name: "missing predicate",
			aggregate: spl.StatsAggregate{
				Function:      spl.AggregateFunctionCountPredicate,
				Alias:         "matches",
				ExplicitAlias: true,
			},
			wantCode: "SPL_UNSUPPORTED_STREAMSTATS_AGGREGATE",
		},
		{
			name: "typed nil predicate",
			aggregate: spl.StatsAggregate{
				Function:      spl.AggregateFunctionCountPredicate,
				Predicate:     typedNil,
				Alias:         "matches",
				ExplicitAlias: true,
			},
			wantCode: "SPL_UNSUPPORTED_STREAMSTATS_AGGREGATE",
		},
		{
			name: "input metadata",
			aggregate: spl.StatsAggregate{
				Function:      spl.AggregateFunctionCountPredicate,
				Input:         "status",
				InputRange:    fieldRange,
				Predicate:     validPredicate(),
				Alias:         "matches",
				ExplicitAlias: true,
			},
			wantCode: "SPL_UNSUPPORTED_STREAMSTATS_AGGREGATE",
		},
		{
			name: "percentile metadata",
			aggregate: spl.StatsAggregate{
				Function:      spl.AggregateFunctionCountPredicate,
				Predicate:     validPredicate(),
				Percentile:    95,
				Alias:         "matches",
				ExplicitAlias: true,
			},
			wantCode: "SPL_UNSUPPORTED_STREAMSTATS_AGGREGATE",
		},
		{
			name: "implicit alias",
			aggregate: spl.StatsAggregate{
				Function:  spl.AggregateFunctionCountPredicate,
				Predicate: validPredicate(),
				Alias:     "matches",
			},
			wantCode: "SPL_UNSUPPORTED_STREAMSTATS_AGGREGATE",
		},
		{
			name: "invalid predicate",
			aggregate: spl.StatsAggregate{
				Function: spl.AggregateFunctionCountPredicate,
				Predicate: &spl.WhereComparisonExpr{
					Left:  &spl.ScalarFieldExpr{Field: "status", Range: fieldRange},
					Op:    spl.CompareOpInvalid,
					Right: &spl.ScalarLiteralExpr{Range: fieldRange},
					Range: fieldRange,
				},
				Alias:         "matches",
				ExplicitAlias: true,
			},
			wantCode: "SPL_UNSUPPORTED_WHERE_EXPRESSION",
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			query := &spl.Query{
				Search: base.Search,
				Commands: []spl.Command{&spl.StreamStatsCommand{
					Aggregate: test.aggregate,
					Current:   true,
					Global:    true,
				}},
				Range: base.Range,
			}
			_, err := Build(query, testScope([]string{"gradethis"}, nil))
			assertDiagnosticCode(t, err, test.wantCode)
		})
	}
}

func TestBuildStreamStatsCountEvalEnforcesExpressionBudget(t *testing.T) {
	t.Parallel()

	base := mustParse(t, `index=gradethis`)
	sourceRange := spl.Range{
		Start: spl.Position{Line: 1, Column: 1},
		End:   spl.Position{Line: 1, Column: 7},
	}
	validLeaf := func() spl.WhereExpr {
		return &spl.WhereComparisonExpr{
			Left: &spl.ScalarFieldExpr{Field: "status", Range: sourceRange},
			Op:   spl.CompareOpEqual,
			Right: &spl.ScalarLiteralExpr{
				Value: spl.Literal{
					Kind:  spl.LiteralKindInteger,
					Text:  "200",
					Range: sourceRange,
				},
				Range: sourceRange,
			},
			Range: sourceRange,
		}
	}
	deep := validLeaf()
	for range maxConvertedExpressionDepth {
		deep = &spl.WhereNotExpr{Operand: deep, Range: sourceRange}
	}
	query := &spl.Query{
		Search: base.Search,
		Commands: []spl.Command{&spl.StreamStatsCommand{
			Aggregate: spl.StatsAggregate{
				Function:      spl.AggregateFunctionCountPredicate,
				Predicate:     deep,
				Alias:         "matches",
				ExplicitAlias: true,
			},
			Current: true,
			Global:  true,
		}},
		Range: base.Range,
	}
	_, err := Build(query, testScope([]string{"gradethis"}, nil))
	assertDiagnosticCode(t, err, "SPL_QUERY_TOO_COMPLEX")
}

func TestAnalyzeStreamStatsCountEvalValidatesPredicateContract(t *testing.T) {
	t.Parallel()

	host := mustResolveEventAggregateField(t, "host")
	status := mustResolveEventAggregateField(t, "status")
	validPredicate := &EvalComparisonExpression{
		Left:  &ScalarFieldExpression{Field: status},
		Op:    ComparisonOpEqual,
		Right: &ScalarLiteralExpression{Value: Value{Kind: ValueKindInt64, Int64: 200}},
	}
	validMeasure := AggregateMeasure{
		Function:  AggregateFunctionCountPredicate,
		Predicate: validPredicate,
		Output:    "matches",
	}
	analysis, err := Analyze(&Query{Operators: []Operator{&StreamAggregate{
		GroupBy: []FieldRef{host},
		Measure: validMeasure,
	}}})
	if err != nil {
		t.Fatalf("Analyze: %v", err)
	}
	if !slices.Equal(analysis.ReferencedFields, []string{"host", "status"}) {
		t.Fatalf("referenced fields = %v, want [host status]", analysis.ReferencedFields)
	}

	var typedNil *EvalComparisonExpression
	cycle := &NotExpression{}
	cycle.Operand = cycle
	tests := []struct {
		name    string
		measure AggregateMeasure
	}{
		{
			name: "missing predicate",
			measure: AggregateMeasure{
				Function: AggregateFunctionCountPredicate,
				Output:   "matches",
			},
		},
		{
			name: "typed nil predicate",
			measure: AggregateMeasure{
				Function:  AggregateFunctionCountPredicate,
				Predicate: typedNil,
				Output:    "matches",
			},
		},
		{
			name: "input metadata",
			measure: AggregateMeasure{
				Function:  AggregateFunctionCountPredicate,
				Input:     status,
				Predicate: validPredicate,
				Output:    "matches",
			},
		},
		{
			name: "percentile metadata",
			measure: AggregateMeasure{
				Function:   AggregateFunctionCountPredicate,
				Predicate:  validPredicate,
				Percentile: 95,
				Output:     "matches",
			},
		},
		{
			name: "base-search predicate shape",
			measure: AggregateMeasure{
				Function: AggregateFunctionCountPredicate,
				Predicate: &ComparisonExpression{
					Field: status,
					Op:    ComparisonOpEqual,
					Value: Value{Kind: ValueKindInt64, Int64: 200},
				},
				Output: "matches",
			},
		},
		{
			name: "cyclic predicate",
			measure: AggregateMeasure{
				Function:  AggregateFunctionCountPredicate,
				Predicate: cycle,
				Output:    "matches",
			},
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			operator := &StreamAggregate{Measure: test.measure}
			if _, err := Analyze(&Query{Operators: []Operator{operator}}); err == nil {
				t.Fatalf("Analyze accepted malformed measure %#v", test.measure)
			}
			query := &Query{Operators: []Operator{&Scan{}, operator}}
			if err := ValidateFieldAnalysisEligibility(query); err == nil {
				t.Fatalf("field analysis accepted malformed measure %#v", test.measure)
			}
			if err := ValidateTimelineEligibility(query); err == nil {
				t.Fatalf("timeline accepted malformed measure %#v", test.measure)
			}
		})
	}
}
