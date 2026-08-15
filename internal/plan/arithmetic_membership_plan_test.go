package plan

import (
	"errors"
	"slices"
	"strings"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/spl"
)

const arithmeticMembershipSource = `index=gradethis | eval score=-(left + right*2) | where score NOT IN (low+1, mid, high/2)`

func TestBuildArithmeticRejectsFixedStringPlusAtOperandRange(t *testing.T) {
	t.Parallel()

	const source = `index=gradethis | eval value="left"+"right"`
	parsed := mustParse(t, source)
	assignment := parsed.Commands[0].(*spl.EvalCommand).Assignments[0]
	addition, ok := assignment.Expression.(*spl.ScalarBinaryExpr)
	if !ok || addition.Op != spl.ScalarBinaryOpAdd {
		t.Fatalf("parsed expression = %#v, want numeric addition", assignment.Expression)
	}

	_, err := Build(parsed, testScope([]string{"gradethis"}, nil))
	assertDiagnosticCode(t, err, "SPL_UNSUPPORTED_ARITHMETIC_VALUE_TYPE")
	var diagnostic *Diagnostic
	if !errors.As(err, &diagnostic) {
		t.Fatalf("Build error = %T, want *Diagnostic", err)
	}
	wantRange := addition.Left.SourceRange()
	if diagnostic.Range != wantRange {
		t.Fatalf("diagnostic range = %#v, want left operand %#v", diagnostic.Range, wantRange)
	}
	if got := source[diagnostic.Range.Start.Offset:diagnostic.Range.End.Offset]; got != `"left"` {
		t.Fatalf("diagnostic range text = %q, want quoted left operand", got)
	}
}

func TestBuildArithmeticAndMembershipPreservesClosedIRRangesAndReadOrder(t *testing.T) {
	t.Parallel()

	parsed := mustParse(t, arithmeticMembershipSource)
	where := parsed.Commands[1].(*spl.WhereCommand)
	sourceMembership := where.Expression.(*spl.WhereMembershipExpr)

	logical, err := Build(parsed, testScope([]string{"gradethis"}, nil))
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	extend := logical.Operators[len(logical.Operators)-2].(*Extend)
	unary, ok := extend.Assignments[0].Expression.(*ScalarUnaryExpression)
	if !ok || unary.Op != ScalarUnaryOpNegative {
		t.Fatalf("eval expression = %#v, want unary negative", extend.Assignments[0].Expression)
	}
	add, ok := unary.Operand.(*ScalarBinaryExpression)
	if !ok || add.Op != ScalarBinaryOpAdd {
		t.Fatalf("unary operand = %#v, want addition", unary.Operand)
	}
	multiply, ok := add.Right.(*ScalarBinaryExpression)
	if !ok || multiply.Op != ScalarBinaryOpMultiply {
		t.Fatalf("addition right = %#v, want multiplication", add.Right)
	}
	assertPlanExpressionText(t, unary, `-(left + right*2)`)
	assertPlanExpressionText(t, add, `(left + right*2)`)
	assertPlanExpressionText(t, multiply, `right*2`)

	filter := logical.Operators[len(logical.Operators)-1].(*Filter)
	membership, ok := filter.Expression.(*MembershipExpression)
	if !ok || !membership.Negated || len(membership.Candidates) != 3 {
		t.Fatalf("where expression = %#v, want three-candidate NOT IN", filter.Expression)
	}
	assertPlanExpressionText(t, membership, `score NOT IN (low+1, mid, high/2)`)
	if first, ok := membership.Candidates[0].(*ScalarBinaryExpression); !ok || first.Op != ScalarBinaryOpAdd {
		t.Fatalf("first candidate = %#v, want addition", membership.Candidates[0])
	}
	if second, ok := membership.Candidates[1].(*ScalarFieldExpression); !ok || second.Field.Name != "mid" {
		t.Fatalf("second candidate = %#v, want mid", membership.Candidates[1])
	}
	if third, ok := membership.Candidates[2].(*ScalarBinaryExpression); !ok || third.Op != ScalarBinaryOpDivide {
		t.Fatalf("third candidate = %#v, want division", membership.Candidates[2])
	}

	// The plan owns its candidate slice and converted nodes.
	sourceMembership.Candidates[0] = &spl.ScalarFieldExpr{Field: "mutated"}
	sourceMembership.Candidates = append(sourceMembership.Candidates, &spl.ScalarFieldExpr{Field: "extra"})
	if len(membership.Candidates) != 3 {
		t.Fatalf("plan candidate slice followed source mutation: %#v", membership.Candidates)
	}
	first := membership.Candidates[0].(*ScalarBinaryExpression)
	if field := first.Left.(*ScalarFieldExpression).Field.Name; field != "low" {
		t.Fatalf("first plan candidate changed to %q", field)
	}

	analysis, err := Analyze(logical)
	if err != nil {
		t.Fatalf("Analyze: %v", err)
	}
	wantFields := []string{"high", "index", "left", "low", "mid", "right", "score"}
	if !slices.Equal(analysis.ReferencedFields, wantFields) {
		t.Fatalf("referenced fields = %v, want %v", analysis.ReferencedFields, wantFields)
	}
	if predicates, ok := logical.AuthoredScalarPredicateCount(); !ok || predicates != 1 {
		t.Fatalf("authored predicate evidence = (%d, %t), want (1, true)", predicates, ok)
	}
}

func TestBuildArithmeticMembershipRejectsForgedASTsAndBudgets(t *testing.T) {
	t.Parallel()

	validRange := arithmeticMembershipTestRange(0, 1)
	invalidRange := spl.Range{
		Start: spl.Position{Offset: 2, Line: 1, Column: 3},
		End:   spl.Position{Offset: 1, Line: 1, Column: 2},
	}
	literal := func() spl.ScalarExpr {
		return &spl.ScalarLiteralExpr{
			Value: spl.Literal{Kind: spl.LiteralKindInteger, Text: "1", Range: validRange},
			Range: validRange,
		}
	}
	var nilUnary *spl.ScalarUnaryExpr
	var nilBinary *spl.ScalarBinaryExpr
	var nilMembership *spl.WhereMembershipExpr
	var nilLiteral *spl.ScalarLiteralExpr

	evalTests := []struct {
		name       string
		expression spl.ScalarExpr
		code       string
	}{
		{name: "typed nil unary", expression: nilUnary, code: "SPL_UNSUPPORTED_ARITHMETIC_SYNTAX"},
		{name: "typed nil binary", expression: nilBinary, code: "SPL_UNSUPPORTED_ARITHMETIC_SYNTAX"},
		{
			name: "typed nil unary operand",
			expression: &spl.ScalarUnaryExpr{
				Op: spl.ScalarUnaryOpNegative, Operand: nilLiteral, Range: validRange,
			},
			code: "SPL_UNSUPPORTED_EVAL_EXPRESSION",
		},
		{
			name: "invalid unary enum",
			expression: &spl.ScalarUnaryExpr{
				Op: spl.ScalarUnaryOpCount, Operand: literal(), Range: validRange,
			},
			code: "SPL_UNSUPPORTED_ARITHMETIC_SYNTAX",
		},
		{
			name: "invalid binary enum",
			expression: &spl.ScalarBinaryExpr{
				Op: spl.ScalarBinaryOpCount, Left: literal(), Right: literal(), Range: validRange,
			},
			code: "SPL_UNSUPPORTED_ARITHMETIC_SYNTAX",
		},
		{
			name: "invalid binary range",
			expression: &spl.ScalarBinaryExpr{
				Op: spl.ScalarBinaryOpAdd, Left: literal(), Right: literal(), Range: invalidRange,
			},
			code: "SPL_UNSUPPORTED_ARITHMETIC_SYNTAX",
		},
		{
			name: "Boolean operand",
			expression: &spl.ScalarUnaryExpr{
				Op: spl.ScalarUnaryOpPositive,
				Operand: &spl.ScalarLiteralExpr{
					Value: spl.Literal{Kind: spl.LiteralKindBool, Text: "true", Range: validRange},
					Range: validRange,
				},
				Range: validRange,
			},
			code: "SPL_UNSUPPORTED_ARITHMETIC_VALUE_TYPE",
		},
	}
	for _, test := range evalTests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			_, err := Build(
				arithmeticMembershipEvalQuery(t, test.expression),
				testScope([]string{"gradethis"}, nil),
			)
			if err == nil {
				t.Fatalf("Build accepted %#v", test.expression)
			}
			assertDiagnosticCode(t, err, test.code)
		})
	}

	whereTests := []struct {
		name       string
		expression spl.WhereExpr
		code       string
	}{
		{name: "typed nil membership", expression: nilMembership, code: "SPL_UNSUPPORTED_MEMBERSHIP_SYNTAX"},
		{
			name:       "empty membership",
			expression: &spl.WhereMembershipExpr{Value: literal(), Range: validRange},
			code:       "SPL_UNSUPPORTED_MEMBERSHIP_SYNTAX",
		},
		{
			name: "typed nil candidate",
			expression: &spl.WhereMembershipExpr{
				Value: literal(), Candidates: []spl.ScalarExpr{nilLiteral}, Range: validRange,
			},
			code: "SPL_UNSUPPORTED_EVAL_EXPRESSION",
		},
		{
			name: "invalid membership range",
			expression: &spl.WhereMembershipExpr{
				Value: literal(), Candidates: []spl.ScalarExpr{literal()}, Range: invalidRange,
			},
			code: "SPL_UNSUPPORTED_MEMBERSHIP_SYNTAX",
		},
	}
	for _, test := range whereTests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			_, err := Build(
				arithmeticMembershipWhereQuery(t, test.expression),
				testScope([]string{"gradethis"}, nil),
			)
			if err == nil {
				t.Fatalf("Build accepted %#v", test.expression)
			}
			assertDiagnosticCode(t, err, test.code)
		})
	}

	t.Run("arithmetic operator boundary and overflow", func(t *testing.T) {
		atLimit := sourceBinaryChain(literal, spl.MaximumArithmeticOperatorsPerQuery, validRange)
		if _, err := Build(
			arithmeticMembershipEvalQuery(t, atLimit),
			testScope([]string{"gradethis"}, nil),
		); err != nil {
			t.Fatalf("Build at arithmetic limit: %v", err)
		}
		over := sourceBinaryChain(literal, spl.MaximumArithmeticOperatorsPerQuery+1, validRange)
		_, err := Build(
			arithmeticMembershipEvalQuery(t, over),
			testScope([]string{"gradethis"}, nil),
		)
		assertDiagnosticCode(t, err, "SPL_QUERY_TOO_COMPLEX")
	})

	t.Run("unary boundary and cycle", func(t *testing.T) {
		atLimit := sourceUnaryChain(literal(), spl.MaximumUnaryOperatorChain, validRange)
		if _, err := Build(
			arithmeticMembershipEvalQuery(t, atLimit),
			testScope([]string{"gradethis"}, nil),
		); err != nil {
			t.Fatalf("Build at unary limit: %v", err)
		}
		over := sourceUnaryChain(literal(), spl.MaximumUnaryOperatorChain+1, validRange)
		_, err := Build(
			arithmeticMembershipEvalQuery(t, over),
			testScope([]string{"gradethis"}, nil),
		)
		assertDiagnosticCode(t, err, "SPL_QUERY_TOO_COMPLEX")

		cycle := &spl.ScalarUnaryExpr{Op: spl.ScalarUnaryOpNegative, Range: validRange}
		cycle.Operand = cycle
		_, err = Build(
			arithmeticMembershipEvalQuery(t, cycle),
			testScope([]string{"gradethis"}, nil),
		)
		assertDiagnosticCode(t, err, "SPL_QUERY_TOO_COMPLEX")
	})

	t.Run("membership occurrence and query budgets", func(t *testing.T) {
		tooMany := make([]spl.ScalarExpr, spl.MaximumMembershipCandidates+1)
		for index := range tooMany {
			tooMany[index] = literal()
		}
		_, err := Build(
			arithmeticMembershipWhereQuery(t, &spl.WhereMembershipExpr{
				Value: literal(), Candidates: tooMany, Range: validRange,
			}),
			testScope([]string{"gradethis"}, nil),
		)
		assertDiagnosticCode(t, err, "SPL_QUERY_TOO_COMPLEX")

		base := mustParse(t, `index=gradethis`)
		commands := make([]spl.Command, 0, spl.MaximumMembershipCandidatesPerQuery/spl.MaximumMembershipCandidates+1)
		for index := 0; index < cap(commands); index++ {
			candidates := make([]spl.ScalarExpr, spl.MaximumMembershipCandidates)
			for candidate := range candidates {
				candidates[candidate] = literal()
			}
			commands = append(commands, &spl.WhereCommand{Expression: &spl.WhereMembershipExpr{
				Value: literal(), Candidates: candidates, Range: validRange,
			}})
		}
		_, err = Build(&spl.Query{Search: base.Search, Commands: commands, Range: base.Range}, testScope([]string{"gradethis"}, nil))
		assertDiagnosticCode(t, err, "SPL_QUERY_TOO_COMPLEX")
	})
}

func TestAnalyzeArithmeticMembershipRejectsForgedPlanGraphsAndBudgets(t *testing.T) {
	t.Parallel()

	literal := func() ScalarExpression {
		return &ScalarLiteralExpression{Value: Value{Kind: ValueKindInt64, Int64: 1}}
	}
	output := analysisField("output")
	extend := func(expression ScalarExpression) *Query {
		return &Query{Operators: []Operator{&Extend{Assignments: []ExtendAssignment{{
			Output: output, Expression: expression,
		}}}}}
	}
	filter := func(expression Expression) *Query {
		return &Query{Operators: []Operator{&Filter{Expression: expression}}}
	}
	invalidRange := spl.Range{
		Start: spl.Position{Offset: 4, Line: 1, Column: 5},
		End:   spl.Position{Offset: 2, Line: 1, Column: 3},
	}
	var nilUnary *ScalarUnaryExpression
	var nilBinary *ScalarBinaryExpression
	var nilMembership *MembershipExpression

	cycle := &ScalarBinaryExpression{Op: ScalarBinaryOpAdd, Right: literal()}
	cycle.Left = cycle

	malformed := []struct {
		name  string
		query *Query
	}{
		{name: "typed nil unary", query: extend(nilUnary)},
		{name: "typed nil binary", query: extend(nilBinary)},
		{name: "typed nil membership", query: filter(nilMembership)},
		{name: "invalid unary enum", query: extend(&ScalarUnaryExpression{Op: ScalarUnaryOpCount, Operand: literal()})},
		{name: "invalid binary enum", query: extend(&ScalarBinaryExpression{Op: ScalarBinaryOpCount, Left: literal(), Right: literal()})},
		{name: "invalid arithmetic range", query: extend(&ScalarUnaryExpression{Op: ScalarUnaryOpPositive, Operand: literal(), Range: invalidRange})},
		{name: "empty membership", query: filter(&MembershipExpression{Value: literal()})},
		{name: "invalid membership range", query: filter(&MembershipExpression{Value: literal(), Candidates: []ScalarExpression{literal()}, Range: invalidRange})},
		{
			name: "invalid arithmetic literal kind",
			query: extend(&ScalarBinaryExpression{
				Op: ScalarBinaryOpAdd,
				Left: &ScalarLiteralExpression{
					Value: Value{Kind: ValueKindInvalid},
				},
				Right: literal(),
			}),
		},
		{
			name: "invalid membership candidate literal kind",
			query: filter(&MembershipExpression{
				Value: literal(),
				Candidates: []ScalarExpression{&ScalarLiteralExpression{
					Value: Value{Kind: ValueKindInvalid},
				}},
			}),
		},
		{
			name: "invalid arithmetic scalar function",
			query: extend(&ScalarUnaryExpression{
				Op: ScalarUnaryOpPositive,
				Operand: &ScalarCallExpression{
					Function: ScalarFunctionCount,
				},
			}),
		},
		{name: "binary cycle", query: extend(cycle)},
		{name: "unknown scalar node", query: extend(&futureArithmeticScalarExpression{})},
		{name: "unknown predicate node", query: filter(&futureMembershipExpression{})},
	}
	for _, test := range malformed {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if _, err := Analyze(test.query); err == nil {
				t.Fatalf("Analyze accepted %#v", test.query)
			}
		})
	}

	t.Run("arithmetic budget", func(t *testing.T) {
		atLimit := planBinaryChain(literal, spl.MaximumArithmeticOperatorsPerQuery)
		if _, err := Analyze(extend(atLimit)); err != nil {
			t.Fatalf("Analyze at arithmetic limit: %v", err)
		}
		if _, err := Analyze(extend(planBinaryChain(literal, spl.MaximumArithmeticOperatorsPerQuery+1))); err == nil {
			t.Fatal("Analyze accepted arithmetic budget overflow")
		}
	})

	t.Run("unary budget", func(t *testing.T) {
		atLimit := planUnaryChain(literal(), spl.MaximumUnaryOperatorChain)
		if _, err := Analyze(extend(atLimit)); err != nil {
			t.Fatalf("Analyze at unary limit: %v", err)
		}
		if _, err := Analyze(extend(planUnaryChain(literal(), spl.MaximumUnaryOperatorChain+1))); err == nil {
			t.Fatal("Analyze accepted unary chain overflow")
		}
	})

	t.Run("membership budgets", func(t *testing.T) {
		tooMany := make([]ScalarExpression, spl.MaximumMembershipCandidates+1)
		for index := range tooMany {
			tooMany[index] = literal()
		}
		if _, err := Analyze(filter(&MembershipExpression{Value: literal(), Candidates: tooMany})); err == nil {
			t.Fatal("Analyze accepted oversized membership")
		}

		operators := make([]Operator, 0, spl.MaximumMembershipCandidatesPerQuery/spl.MaximumMembershipCandidates+1)
		for index := 0; index < cap(operators); index++ {
			candidates := make([]ScalarExpression, spl.MaximumMembershipCandidates)
			for candidate := range candidates {
				candidates[candidate] = literal()
			}
			operators = append(operators, &Filter{Expression: &MembershipExpression{
				Value: literal(), Candidates: candidates,
			}})
		}
		if _, err := Analyze(&Query{Operators: operators}); err == nil {
			t.Fatal("Analyze accepted aggregate membership budget overflow")
		}
	})
}

func TestArithmeticMembershipCompositionVisitorsPreserveOrderedDependencies(t *testing.T) {
	t.Parallel()

	firstRange := arithmeticMembershipTestRange(10, 16)
	secondRange := arithmeticMembershipTestRange(20, 26)
	field := func(name string, sourceRange spl.Range) ScalarExpression {
		resolved, err := ResolveField(name, sourceRange)
		if err != nil {
			t.Fatalf("ResolveField(%q): %v", name, err)
		}
		return &ScalarFieldExpression{Field: resolved, Range: sourceRange}
	}
	predicate := &MembershipExpression{
		Value: field("input", arithmeticMembershipTestRange(1, 6)),
		Candidates: []ScalarExpression{
			&ScalarBinaryExpression{
				Op:    ScalarBinaryOpAdd,
				Left:  field("ordinary", arithmeticMembershipTestRange(7, 9)),
				Right: field("fields", firstRange),
			},
			field("fields", secondRange),
		},
	}
	gotRange, found, err := predicateFieldRange(predicate)
	if err != nil || !found || gotRange != firstRange {
		t.Fatalf("first reserved dependency = (%#v, %t, %v), want %#v", gotRange, found, err, firstRange)
	}

	operator := &EventAggregate{Measure: AggregateMeasure{
		Function:  AggregateFunctionCountPredicate,
		Predicate: predicate,
		Output:    "matches",
	}}
	query := &Query{Operators: []Operator{&Scan{}, operator}}
	analysis, err := Analyze(query)
	if err != nil {
		t.Fatalf("Analyze composed aggregate: %v", err)
	}
	want := []string{"fields", "input", "ordinary"}
	if !slices.Equal(analysis.ReferencedFields, want) {
		t.Fatalf("aggregate dependencies = %v, want %v", analysis.ReferencedFields, want)
	}
	if err := ValidateFieldAnalysisEligibility(query); err != nil {
		t.Fatalf("field-analysis composition: %v", err)
	}
	if err := ValidateTimelineEligibility(query); err != nil {
		t.Fatalf("timeline composition: %v", err)
	}

	deepSource := `index=gradethis | eventstats count(eval(fields` +
		strings.Repeat(`+1`, spl.MaximumArithmeticOperatorsPerQuery) +
		` IN (1))) AS matches`
	_, err = Build(
		mustParse(t, deepSource),
		testScope([]string{"gradethis"}, nil),
	)
	if err == nil {
		t.Fatal("Build accepted reserved fields behind the maximum arithmetic spine")
	}
	assertDiagnosticCode(t, err, "SPL_AMBIGUOUS_EVENTSTATS_FIELD")
}

func TestKnowledgeV01ExpressionVisitorRejectsAuthoredV02Nodes(t *testing.T) {
	t.Parallel()

	literal := &spl.ScalarLiteralExpr{Value: spl.Literal{Kind: spl.LiteralKindInteger, Text: "1"}}
	if !knowledgeExpressionUsesAuthoredV02Syntax(&spl.ScalarUnaryExpr{
		Op: spl.ScalarUnaryOpNegative, Operand: literal,
	}) {
		t.Fatal("knowledge visitor accepted unary arithmetic")
	}
	if !knowledgeExpressionUsesAuthoredV02Syntax(&spl.ScalarIfExpr{
		Condition: &spl.WhereMembershipExpr{
			Value: literal, Candidates: []spl.ScalarExpr{literal},
		},
		True:  literal,
		False: literal,
	}) {
		t.Fatal("knowledge visitor accepted nested membership")
	}
	if knowledgeExpressionUsesAuthoredV02Syntax(&spl.ScalarFieldExpr{Field: "host"}) {
		t.Fatal("knowledge visitor rejected a v0.1 field")
	}

	for _, source := range []string{`host+1`, `if(host IN ("api"), 1, 0)`} {
		if _, err := convertKnowledgeCalculatedExpression(source, nil, 1, 0); err == nil {
			t.Fatalf("knowledge conversion accepted %q", source)
		}
	}
}

type futureArithmeticScalarExpression struct{}

func (*futureArithmeticScalarExpression) scalarExpression() {}
func (*futureArithmeticScalarExpression) SourceRange() spl.Range {
	return spl.Range{}
}

type futureMembershipExpression struct{}

func (*futureMembershipExpression) expression() {}
func (*futureMembershipExpression) SourceRange() spl.Range {
	return spl.Range{}
}

func arithmeticMembershipEvalQuery(t *testing.T, expression spl.ScalarExpr) *spl.Query {
	t.Helper()
	base := mustParse(t, `index=gradethis`)
	return &spl.Query{
		Search: base.Search,
		Commands: []spl.Command{&spl.EvalCommand{Assignments: []spl.EvalAssignment{{
			Field: "output", Expression: expression,
		}}}},
		Range: base.Range,
	}
}

func arithmeticMembershipWhereQuery(t *testing.T, expression spl.WhereExpr) *spl.Query {
	t.Helper()
	base := mustParse(t, `index=gradethis`)
	return &spl.Query{
		Search:   base.Search,
		Commands: []spl.Command{&spl.WhereCommand{Expression: expression}},
		Range:    base.Range,
	}
}

func sourceBinaryChain(
	literal func() spl.ScalarExpr,
	operators int,
	sourceRange spl.Range,
) spl.ScalarExpr {
	result := literal()
	for range operators {
		result = &spl.ScalarBinaryExpr{
			Op: spl.ScalarBinaryOpAdd, Left: result, Right: literal(), Range: sourceRange,
		}
	}
	return result
}

func sourceUnaryChain(
	expression spl.ScalarExpr,
	operators int,
	sourceRange spl.Range,
) spl.ScalarExpr {
	for range operators {
		expression = &spl.ScalarUnaryExpr{
			Op: spl.ScalarUnaryOpNegative, Operand: expression, Range: sourceRange,
		}
	}
	return expression
}

func planBinaryChain(literal func() ScalarExpression, operators int) ScalarExpression {
	result := literal()
	for range operators {
		result = &ScalarBinaryExpression{
			Op: ScalarBinaryOpAdd, Left: result, Right: literal(),
		}
	}
	return result
}

func planUnaryChain(expression ScalarExpression, operators int) ScalarExpression {
	for range operators {
		expression = &ScalarUnaryExpression{
			Op: ScalarUnaryOpNegative, Operand: expression,
		}
	}
	return expression
}

func arithmeticMembershipTestRange(start, end int) spl.Range {
	return spl.Range{
		Start: spl.Position{Offset: start, Line: 1, Column: start + 1},
		End:   spl.Position{Offset: end, Line: 1, Column: end + 1},
	}
}

func assertPlanExpressionText(
	t *testing.T,
	expression interface{ SourceRange() spl.Range },
	want string,
) {
	t.Helper()
	sourceRange := expression.SourceRange()
	if sourceRange.Start.Offset < 0 || sourceRange.End.Offset > len(arithmeticMembershipSource) ||
		sourceRange.End.Offset <= sourceRange.Start.Offset {
		t.Fatalf("invalid source range %#v", sourceRange)
	}
	if got := arithmeticMembershipSource[sourceRange.Start.Offset:sourceRange.End.Offset]; got != want {
		t.Fatalf("source range text = %q, want %q", got, want)
	}
	if strings.TrimSpace(want) == "" {
		t.Fatal("test expected non-empty source text")
	}
}
