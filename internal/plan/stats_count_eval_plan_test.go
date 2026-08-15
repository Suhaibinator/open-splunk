package plan

import (
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/spl"
)

func TestBuildStatsCountEvalProducesPredicateMeasure(t *testing.T) {
	t.Parallel()

	logical, err := Build(
		mustParse(
			t,
			`index=gradethis | stats count(eval(isnull(probe) OR NOT status=200)) AS matches count AS rows BY host`,
		),
		testScope([]string{"gradethis"}, nil),
	)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	aggregate, ok := logical.Operators[len(logical.Operators)-1].(*Aggregate)
	if !ok {
		t.Fatalf("last operator = %T, want *Aggregate", logical.Operators[len(logical.Operators)-1])
	}
	if len(aggregate.Measures) != 2 {
		t.Fatalf("measures = %#v", aggregate.Measures)
	}
	conditional := aggregate.Measures[0]
	if conditional.Function != AggregateFunctionCountPredicate ||
		conditional.Input.Name != "" || conditional.Input.Canonical ||
		len(conditional.Input.Path) != 0 || conditional.Input.Range != (spl.Range{}) ||
		conditional.Predicate == nil ||
		conditional.Output != "matches" {
		t.Fatalf("conditional measure = %#v", conditional)
	}
	root, ok := conditional.Predicate.(*BooleanExpression)
	if !ok || root.Op != BooleanOpOr {
		t.Fatalf("predicate = %#v, want BooleanExpression OR", conditional.Predicate)
	}
	if _, ok := root.Left.(*ScalarPredicateExpression); !ok {
		t.Fatalf("predicate left = %T, want *ScalarPredicateExpression", root.Left)
	}
	if _, ok := root.Right.(*NotExpression); !ok {
		t.Fatalf("predicate right = %T, want *NotExpression", root.Right)
	}
	rows := aggregate.Measures[1]
	if rows.Function != AggregateFunctionCountRows || rows.Predicate != nil ||
		rows.Output != "rows" {
		t.Fatalf("row-count measure = %#v", rows)
	}
}

func TestBuildStatsCountEvalRejectsForgedMetadataAndPredicates(t *testing.T) {
	t.Parallel()

	base := mustParse(t, `index=gradethis`)
	fieldRange := spl.Range{
		Start: spl.Position{Line: 1, Column: 1},
		End:   spl.Position{Line: 1, Column: 5},
	}
	validPredicate := func() spl.WhereExpr {
		return &spl.WhereComparisonExpr{
			Left: &spl.ScalarFieldExpr{Field: "status", Range: fieldRange},
			Op:   spl.CompareOpEqual,
			Right: &spl.ScalarLiteralExpr{
				Value: spl.Literal{Kind: spl.LiteralKindInteger, Text: "200", Range: fieldRange},
				Range: fieldRange,
			},
			Range: fieldRange,
		}
	}
	var typedNil *spl.WhereComparisonExpr
	tests := []spl.StatsAggregate{
		{
			Function:      spl.AggregateFunctionCountPredicate,
			Alias:         "matches",
			ExplicitAlias: true,
		},
		{
			Function:      spl.AggregateFunctionCountPredicate,
			Predicate:     typedNil,
			Alias:         "matches",
			ExplicitAlias: true,
		},
		{
			Function:      spl.AggregateFunctionCountPredicate,
			Predicate:     validPredicate(),
			Input:         "status",
			InputRange:    fieldRange,
			Alias:         "matches",
			ExplicitAlias: true,
		},
		{
			Function:      spl.AggregateFunctionCountPredicate,
			Predicate:     validPredicate(),
			Percentile:    95,
			Alias:         "matches",
			ExplicitAlias: true,
		},
		{
			Function:  spl.AggregateFunctionCountPredicate,
			Predicate: validPredicate(),
			Alias:     "matches",
		},
		{
			Function:      spl.AggregateFunctionCount,
			Predicate:     validPredicate(),
			Alias:         "count",
			ExplicitAlias: true,
		},
		{
			Function:      spl.AggregateFunctionCountValues,
			Input:         "status",
			InputRange:    fieldRange,
			Predicate:     validPredicate(),
			Alias:         "count(status)",
			ExplicitAlias: true,
		},
	}
	for index, aggregate := range tests {
		command := &spl.StatsCommand{Aggregates: []spl.StatsAggregate{aggregate}}
		query := &spl.Query{
			Search:   base.Search,
			Commands: []spl.Command{command},
			Range:    base.Range,
		}
		_, err := Build(query, testScope([]string{"gradethis"}, nil))
		if err == nil {
			t.Fatalf("case %d Build succeeded for forged aggregate %#v", index, aggregate)
		}
		assertDiagnosticCode(t, err, "SPL_UNSUPPORTED_STATS_AGGREGATE")
	}

	predicateTests := []struct {
		name      string
		predicate spl.WhereExpr
		code      string
	}{
		{
			name: "invalid comparison operator",
			predicate: &spl.WhereComparisonExpr{
				Left: &spl.ScalarFieldExpr{Field: "status", Range: fieldRange},
				Op:   spl.CompareOpInvalid,
				Right: &spl.ScalarLiteralExpr{
					Value: spl.Literal{
						Kind:  spl.LiteralKindInteger,
						Text:  "200",
						Range: fieldRange,
					},
					Range: fieldRange,
				},
				Range: fieldRange,
			},
			code: "SPL_UNSUPPORTED_WHERE_EXPRESSION",
		},
		{
			name: "invalid scalar function arity",
			predicate: &spl.WhereScalarPredicateExpr{
				Value: &spl.ScalarCallExpr{
					Function: spl.ScalarFunctionIsNull,
					Range:    fieldRange,
				},
				Range: fieldRange,
			},
			code: "SPL_INVALID_EVAL_ARITY",
		},
	}
	for _, test := range predicateTests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			command := &spl.StatsCommand{Aggregates: []spl.StatsAggregate{{
				Function:      spl.AggregateFunctionCountPredicate,
				Predicate:     test.predicate,
				Alias:         "matches",
				ExplicitAlias: true,
			}}}
			query := &spl.Query{
				Search:   base.Search,
				Commands: []spl.Command{command},
				Range:    base.Range,
			}
			_, err := Build(query, testScope([]string{"gradethis"}, nil))
			if err == nil {
				t.Fatalf("Build succeeded for forged predicate %#v", test.predicate)
			}
			assertDiagnosticCode(t, err, test.code)
		})
	}
}

func TestBuildStatsCountEvalRejectsForgedPredicateComplexity(t *testing.T) {
	t.Parallel()

	base := mustParse(t, `index=gradethis`)
	sourceRange := spl.Range{
		Start: spl.Position{Line: 1, Column: 1},
		End:   spl.Position{Line: 1, Column: 5},
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

	cyclicWhere := &spl.WhereNotExpr{Range: sourceRange}
	cyclicWhere.Operand = cyclicWhere
	cyclicScalar := &spl.ScalarCallExpr{
		Function: spl.ScalarFunctionIsNull,
		Range:    sourceRange,
	}
	cyclicScalar.Arguments = []spl.ScalarExpr{cyclicScalar}

	deep := validLeaf()
	for range maxConvertedExpressionDepth {
		deep = &spl.WhereNotExpr{Operand: deep, Range: sourceRange}
	}

	sharedDAG := validLeaf()
	for range 35 {
		sharedDAG = &spl.WhereBoolExpr{
			Op:    spl.BoolOpAnd,
			Left:  sharedDAG,
			Right: sharedDAG,
			Range: sourceRange,
		}
	}

	for _, test := range []struct {
		name      string
		predicate spl.WhereExpr
	}{
		{name: "where cycle", predicate: cyclicWhere},
		{
			name: "scalar cycle",
			predicate: &spl.WhereScalarPredicateExpr{
				Value: cyclicScalar,
				Range: sourceRange,
			},
		},
		{name: "depth", predicate: deep},
		{name: "shared DAG expansion", predicate: sharedDAG},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			command := &spl.StatsCommand{Aggregates: []spl.StatsAggregate{{
				Function:      spl.AggregateFunctionCountPredicate,
				Predicate:     test.predicate,
				Alias:         "matches",
				ExplicitAlias: true,
			}}}
			query := &spl.Query{
				Search:   base.Search,
				Commands: []spl.Command{command},
				Range:    base.Range,
			}
			_, err := Build(query, testScope([]string{"gradethis"}, nil))
			if err == nil {
				t.Fatalf("Build succeeded for forged predicate %T", test.predicate)
			}
			assertDiagnosticCode(t, err, "SPL_QUERY_TOO_COMPLEX")
		})
	}
}

func TestBuildStatsCountEvalPreservesReservedFieldsBoundary(t *testing.T) {
	t.Parallel()

	_, err := Build(
		mustParse(
			t,
			`index=gradethis | stats count(eval(fields="value")) AS matches`,
		),
		testScope([]string{"gradethis"}, nil),
	)
	assertDiagnosticCode(t, err, "SPL_AMBIGUOUS_STATS_FIELD")

	logical, err := Build(
		mustParse(
			t,
			`index=gradethis | stats count AS fields | stats count(eval(fields=1)) AS matches`,
		),
		testScope([]string{"gradethis"}, nil),
	)
	if err != nil {
		t.Fatalf("Build closed-schema fields predicate: %v", err)
	}
	if len(logical.OutputFields) != 1 || logical.OutputFields[0] != "matches" {
		t.Fatalf("closed-schema output fields = %#v, want matches", logical.OutputFields)
	}
}
