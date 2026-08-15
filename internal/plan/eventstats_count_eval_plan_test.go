package plan

import (
	"errors"
	"slices"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/spl"
)

func TestBuildEventStatsCountEvalProducesRowPreservingPredicateMeasure(t *testing.T) {
	t.Parallel()

	logical, err := Build(
		mustParse(
			t,
			`index=gradethis | eventstats count(eval(isnull(probe) OR NOT status=200)) AS matches BY host`,
		),
		testScope([]string{"gradethis"}, nil),
	)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	eventAggregate, ok :=
		logical.Operators[len(logical.Operators)-1].(*EventAggregate)
	if !ok {
		t.Fatalf(
			"last operator = %T, want *EventAggregate",
			logical.Operators[len(logical.Operators)-1],
		)
	}
	measure := eventAggregate.Measure
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
	if len(eventAggregate.GroupBy) != 1 ||
		eventAggregate.GroupBy[0].Name != "host" {
		t.Fatalf("group by = %#v, want host", eventAggregate.GroupBy)
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

func TestBuildEventStatsCountEvalUpsertsKnownOutputSchema(t *testing.T) {
	t.Parallel()

	logical, err := Build(
		mustParse(
			t,
			`index=gradethis | table host,status,matches | eventstats count(eval(status=500)) AS matches BY host`,
		),
		testScope([]string{"gradethis"}, nil),
	)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if !slices.Equal(logical.OutputFields, []string{"host", "status", "matches"}) {
		t.Fatalf("output fields = %v, want [host status matches]", logical.OutputFields)
	}
}

func TestBuildEventStatsCountEvalEnforcesReservedOpenSchemaBoundary(t *testing.T) {
	t.Parallel()

	_, err := Build(
		mustParse(
			t,
			`index=gradethis | eventstats count(eval(fields="value")) AS matches`,
		),
		testScope([]string{"gradethis"}, nil),
	)
	assertDiagnosticCode(t, err, "SPL_AMBIGUOUS_EVENTSTATS_FIELD")
	var diagnostic *Diagnostic
	if !errors.As(err, &diagnostic) ||
		diagnostic.Range == (spl.Range{}) {
		t.Fatalf("reserved-field diagnostic = %#v, want source-located range", err)
	}

	logical, err := Build(
		mustParse(
			t,
			`index=gradethis | table fields,host | eventstats count(eval(fields="value")) AS matches BY host`,
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

func TestBuildEventStatsCountEvalRejectsForgedMetadataAndPredicates(t *testing.T) {
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
			wantCode: "SPL_UNSUPPORTED_EVENTSTATS_AGGREGATE",
		},
		{
			name: "typed nil predicate",
			aggregate: spl.StatsAggregate{
				Function:      spl.AggregateFunctionCountPredicate,
				Predicate:     typedNil,
				Alias:         "matches",
				ExplicitAlias: true,
			},
			wantCode: "SPL_UNSUPPORTED_EVENTSTATS_AGGREGATE",
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
			wantCode: "SPL_UNSUPPORTED_EVENTSTATS_AGGREGATE",
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
			wantCode: "SPL_UNSUPPORTED_EVENTSTATS_AGGREGATE",
		},
		{
			name: "implicit alias",
			aggregate: spl.StatsAggregate{
				Function:  spl.AggregateFunctionCountPredicate,
				Predicate: validPredicate(),
				Alias:     "matches",
			},
			wantCode: "SPL_UNSUPPORTED_EVENTSTATS_AGGREGATE",
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
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			query := &spl.Query{
				Search: base.Search,
				Commands: []spl.Command{&spl.EventStatsCommand{
					Aggregate: test.aggregate,
				}},
				Range: base.Range,
			}
			_, err := Build(query, testScope([]string{"gradethis"}, nil))
			assertDiagnosticCode(t, err, test.wantCode)
		})
	}
}

func TestAnalyzeEventStatsCountEvalValidatesPredicateContract(t *testing.T) {
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
	analysis, err := Analyze(&Query{Operators: []Operator{&EventAggregate{
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
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			operator := &EventAggregate{Measure: test.measure}
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

func TestEventStatsCountEvalPredicateContractRejectsForgedStructuresAndGraphs(
	t *testing.T,
) {
	t.Parallel()

	status := mustResolveEventAggregateField(t, "status")
	host := mustResolveEventAggregateField(t, "host")
	integer := func(value int64) ScalarExpression {
		return &ScalarLiteralExpression{
			Value: Value{Kind: ValueKindInt64, Int64: value},
		}
	}
	quoted := func(value string) ScalarExpression {
		return &ScalarLiteralExpression{
			Value: Value{Kind: ValueKindString, String: value, Quoted: true},
		}
	}
	field := func(reference FieldRef) ScalarExpression {
		return &ScalarFieldExpression{Field: reference}
	}
	comparison := func(left ScalarExpression) Expression {
		return &EvalComparisonExpression{
			Left:  left,
			Op:    ComparisonOpEqual,
			Right: integer(1),
		}
	}
	measure := func(predicate Expression) AggregateMeasure {
		return AggregateMeasure{
			Function:  AggregateFunctionCountPredicate,
			Predicate: predicate,
			Output:    "matches",
		}
	}

	sharedLeaf := comparison(field(status))
	validShared := &BooleanExpression{
		Op:    BooleanOpAnd,
		Left:  sharedLeaf,
		Right: sharedLeaf,
	}
	if _, err := Analyze(&Query{Operators: []Operator{&EventAggregate{
		Measure: measure(validShared),
	}}}); err != nil {
		t.Fatalf("Analyze small shared predicate DAG: %v", err)
	}
	validMatch := &ScalarPredicateExpression{Value: &ScalarCallExpression{
		Function: ScalarFunctionMatch,
		Arguments: []ScalarExpression{
			field(status),
			quoted("^2"),
		},
	}}
	if _, err := Analyze(&Query{Operators: []Operator{&EventAggregate{
		Measure: measure(validMatch),
	}}}); err != nil {
		t.Fatalf("Analyze valid literal-constrained call: %v", err)
	}
	compilerOwnedShapes := []Expression{
		&ScalarPredicateExpression{Value: &ScalarCallExpression{
			Function: ScalarFunctionMatch,
		}},
		&ScalarPredicateExpression{Value: &ScalarCallExpression{
			Function: ScalarFunctionMatch,
			Arguments: []ScalarExpression{
				field(status),
				field(host),
			},
		}},
		comparison(&ScalarCallExpression{
			Function:  ScalarFunctionNow,
			Arguments: []ScalarExpression{integer(1)},
		}),
		comparison(&ScalarCallExpression{
			Function: ScalarFunctionSubstring,
			Arguments: []ScalarExpression{
				field(status),
				field(host),
			},
		}),
		comparison(&ScalarCallExpression{
			Function: ScalarFunctionRound,
			Arguments: []ScalarExpression{
				field(status),
				integer(spl.MaximumRoundPrecision + 1),
			},
		}),
	}
	for index, predicate := range compilerOwnedShapes {
		if _, err := Analyze(&Query{Operators: []Operator{&EventAggregate{
			Measure: measure(predicate),
		}}}); err != nil {
			t.Fatalf(
				"Analyze structurally valid compiler-owned shape %d: %v",
				index,
				err,
			)
		}
	}

	overBudget := sharedLeaf
	for range 10 {
		overBudget = &BooleanExpression{
			Op:    BooleanOpAnd,
			Left:  overBudget,
			Right: overBudget,
		}
	}
	cycle := &NotExpression{}
	cycle.Operand = cycle
	scalarCycle := &ScalarCallExpression{Function: ScalarFunctionConcat}
	scalarCycle.Arguments = []ScalarExpression{integer(1), scalarCycle}
	badField := status
	badField.Canonical = !badField.Canonical
	boolLiteral := func(value bool) ScalarExpression {
		return &ScalarLiteralExpression{
			Value: Value{Kind: ValueKindBool, Bool: value},
		}
	}
	malformed := []struct {
		name      string
		predicate Expression
	}{
		{
			name: "unsupported predicate node",
			predicate: &ComparisonExpression{
				Field: status,
				Op:    ComparisonOpEqual,
				Value: Value{Kind: ValueKindInt64},
			},
		},
		{
			name:      "unsupported scalar node",
			predicate: comparison(&forgedEventAggregateScalarExpression{}),
		},
		{
			name: "unknown scalar function",
			predicate: comparison(&ScalarCallExpression{
				Function: ScalarFunction(255),
			}),
		},
		{
			name: "invalid literal kind",
			predicate: comparison(&ScalarLiteralExpression{
				Value: Value{Kind: ValueKindInvalid},
			}),
		},
		{
			name:      "invalid field provenance",
			predicate: comparison(field(badField)),
		},
		{
			name: "invalid Boolean operator",
			predicate: &BooleanExpression{
				Op:    BooleanOpInvalid,
				Left:  sharedLeaf,
				Right: sharedLeaf,
			},
		},
		{
			name: "invalid comparison operator",
			predicate: &EvalComparisonExpression{
				Left:  field(status),
				Op:    ComparisonOpInvalid,
				Right: integer(1),
			},
		},
		{
			name: "non-Boolean scalar predicate",
			predicate: &ScalarPredicateExpression{Value: &ScalarCallExpression{
				Function:  ScalarFunctionLower,
				Arguments: []ScalarExpression{field(status)},
			}},
		},
		{
			name: "nil call argument",
			predicate: comparison(&ScalarCallExpression{
				Function:  ScalarFunctionReplace,
				Arguments: []ScalarExpression{field(status), nil},
			}),
		},
		{
			name: "invalid later call argument",
			predicate: comparison(&ScalarCallExpression{
				Function: ScalarFunctionReplace,
				Arguments: []ScalarExpression{
					field(status),
					field(badField),
				},
			}),
		},
		{
			name: "invalid later case branch",
			predicate: &ScalarPredicateExpression{Value: &ScalarCaseExpression{
				Branches: []ScalarCaseBranch{
					{Condition: sharedLeaf, Value: boolLiteral(true)},
					{
						Condition: comparison(field(badField)),
						Value:     boolLiteral(false),
					},
				},
			}},
		},
		{
			name: "cyclic scalar graph",
			predicate: comparison(&ScalarCallExpression{
				Function:  scalarCycle.Function,
				Arguments: scalarCycle.Arguments,
			}),
		},
		{name: "cyclic predicate", predicate: cycle},
		{name: "shared DAG expansion exceeds budget", predicate: overBudget},
	}
	for _, test := range malformed {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			operator := &EventAggregate{Measure: measure(test.predicate)}
			if _, err := Analyze(&Query{Operators: []Operator{operator}}); err == nil {
				t.Fatalf("Analyze accepted malformed predicate %#v", test.predicate)
			}
			query := &Query{Operators: []Operator{&Scan{}, operator}}
			if err := ValidateFieldAnalysisEligibility(query); err == nil {
				t.Fatalf("field analysis accepted malformed predicate %#v", test.predicate)
			}
			if err := ValidateTimelineEligibility(query); err == nil {
				t.Fatalf("timeline accepted malformed predicate %#v", test.predicate)
			}
		})
	}

	logical, err := Build(
		mustParse(
			t,
			`index=gradethis | eventstats count(eval(match(source,"^api$") AND substr(host,1,2)="ap" AND round(status,2)>0)) AS matches`,
		),
		testScope([]string{"gradethis"}, nil),
	)
	if err != nil {
		t.Fatalf("Build valid literal-constrained calls: %v", err)
	}
	if _, err := Analyze(logical); err != nil {
		t.Fatalf("Analyze valid literal-constrained calls: %v", err)
	}
}

type forgedEventAggregateScalarExpression struct{}

func (*forgedEventAggregateScalarExpression) scalarExpression() {}

func (*forgedEventAggregateScalarExpression) SourceRange() spl.Range {
	return spl.Range{}
}
