package spl

import (
	"strconv"
	"strings"
	"testing"
)

func TestParseEvalConcatenationFlattensOperandsAndPreservesRanges(t *testing.T) {
	t.Parallel()

	const source = `index=main | eval label=first . "-" . lower(last) . 7`
	query, err := Parse(source)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	expression := query.Commands[0].(*EvalCommand).Assignments[0].Expression
	concatenation := requireConcatenationCall(t, expression, 4)
	if concatenation.SourceRange() != concatenation.Range {
		t.Fatalf("SourceRange() = %#v, want %#v", concatenation.SourceRange(), concatenation.Range)
	}
	if got, want := concatenationRangeText(source, concatenation.Range), `first . "-" . lower(last) . 7`; got != want {
		t.Fatalf("concatenation range = %q, want %q", got, want)
	}

	wantArgumentRanges := []string{"first", `"-"`, "lower(last)", "7"}
	for index, want := range wantArgumentRanges {
		if got := concatenationRangeText(source, concatenation.Arguments[index].SourceRange()); got != want {
			t.Fatalf("argument %d range = %q, want %q", index, got, want)
		}
	}
	if field, ok := concatenation.Arguments[0].(*ScalarFieldExpr); !ok || field.Field != "first" {
		t.Fatalf("first argument = %#v, want field first", concatenation.Arguments[0])
	}
	if literal, ok := concatenation.Arguments[1].(*ScalarLiteralExpr); !ok ||
		literal.Value.Kind != LiteralKindString || literal.Value.Text != "-" ||
		!literal.Value.Quoted {
		t.Fatalf("second argument = %#v, want quoted dash", concatenation.Arguments[1])
	}
	if call, ok := concatenation.Arguments[2].(*ScalarCallExpr); !ok ||
		call.Function != ScalarFunctionLower {
		t.Fatalf("third argument = %#v, want lower call", concatenation.Arguments[2])
	}
	if literal, ok := concatenation.Arguments[3].(*ScalarLiteralExpr); !ok ||
		literal.Value.Kind != LiteralKindInteger || literal.Value.Text != "7" {
		t.Fatalf("fourth argument = %#v, want integer 7", concatenation.Arguments[3])
	}
}

func TestParseConcatenationKeepsFixedStringPlusInArithmeticGrammar(t *testing.T) {
	t.Parallel()

	const periodSource = `index=main | eval value="left" . "right"`
	query, err := Parse(periodSource)
	if err != nil {
		t.Fatalf("Parse period concatenation: %v", err)
	}
	concatenation := requireConcatenationCall(
		t,
		query.Commands[0].(*EvalCommand).Assignments[0].Expression,
		2,
	)
	for index, want := range []string{"left", "right"} {
		literal, ok := concatenation.Arguments[index].(*ScalarLiteralExpr)
		if !ok || literal.Value.Kind != LiteralKindString ||
			literal.Value.Text != want || !literal.Value.Quoted {
			t.Fatalf(
				"period concatenation argument %d = %#v, want fixed String %q",
				index,
				concatenation.Arguments[index],
				want,
			)
		}
	}

	// Authored v0.2 accepts + only as numeric arithmetic. The parser must not
	// silently reinterpret fixed String operands as SPL2 concatenation; the
	// semantic planner owns their source-located unsupported-type diagnostic.
	const plusSource = `index=main | eval value="left"+"right"`
	query, err = Parse(plusSource)
	if err != nil {
		t.Fatalf("Parse fixed String arithmetic: %v", err)
	}
	addition, ok := query.Commands[0].(*EvalCommand).Assignments[0].Expression.(*ScalarBinaryExpr)
	if !ok || addition.Op != ScalarBinaryOpAdd {
		t.Fatalf("fixed String plus expression = %#v, want numeric addition", query.Commands[0])
	}
	for index, want := range []string{"left", "right"} {
		operand := []ScalarExpr{addition.Left, addition.Right}[index]
		literal, ok := operand.(*ScalarLiteralExpr)
		if !ok || literal.Value.Kind != LiteralKindString ||
			literal.Value.Text != want || !literal.Value.Quoted {
			t.Fatalf("fixed String plus operand %d = %#v, want %q", index, operand, want)
		}
	}
}

func TestParseConcatenationRecognizesUnspacedQuotedBoundaries(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		expression    string
		wantArguments []string
	}{
		{
			name:          "quoted middle",
			expression:    `first." ".last`,
			wantArguments: []string{"first", `" "`, "last"},
		},
		{
			name:          "quoted prefix",
			expression:    `"prefix".field`,
			wantArguments: []string{`"prefix"`, "field"},
		},
		{
			name:          "quoted suffix",
			expression:    `field."suffix"`,
			wantArguments: []string{"field", `"suffix"`},
		},
		{
			name:          "quoted prefix before call",
			expression:    `"$".tostring(x)`,
			wantArguments: []string{`"$"`, "tostring(x)"},
		},
		{
			name:          "space after operator before quote",
			expression:    `first. "last"`,
			wantArguments: []string{"first", `"last"`},
		},
		{
			name:          "space before operator after quote",
			expression:    `"first" .last`,
			wantArguments: []string{`"first"`, "last"},
		},
		{
			name:          "Unicode space after operator before quote",
			expression:    "first.\u2003\"last\"",
			wantArguments: []string{"first", `"last"`},
		},
		{
			name:          "Unicode space before operator after quote",
			expression:    "\"first\"\u2003.last",
			wantArguments: []string{`"first"`, "last"},
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			source := `index=main | eval value=` + test.expression
			query, err := Parse(source)
			if err != nil {
				t.Fatalf("Parse: %v", err)
			}
			expression := query.Commands[0].(*EvalCommand).Assignments[0].Expression
			concatenation := requireConcatenationCall(t, expression, len(test.wantArguments))
			if got := concatenationRangeText(source, concatenation.Range); got != test.expression {
				t.Fatalf("concatenation range = %q, want %q", got, test.expression)
			}
			for index, want := range test.wantArguments {
				if got := concatenationRangeText(source, concatenation.Arguments[index].SourceRange()); got != want {
					t.Fatalf("argument %d range = %q, want %q", index, got, want)
				}
			}

			switch test.name {
			case "quoted middle":
				if first, ok := concatenation.Arguments[0].(*ScalarFieldExpr); !ok || first.Field != "first" {
					t.Fatalf("first argument = %#v, want field first", concatenation.Arguments[0])
				}
				if middle, ok := concatenation.Arguments[1].(*ScalarLiteralExpr); !ok ||
					middle.Value.Text != " " || !middle.Value.Quoted {
					t.Fatalf("middle argument = %#v, want quoted space", concatenation.Arguments[1])
				}
				if last, ok := concatenation.Arguments[2].(*ScalarFieldExpr); !ok || last.Field != "last" {
					t.Fatalf("last argument = %#v, want field last", concatenation.Arguments[2])
				}
			case "quoted prefix before call":
				if call, ok := concatenation.Arguments[1].(*ScalarCallExpr); !ok ||
					call.Function != ScalarFunctionToString {
					t.Fatalf("second argument = %#v, want tostring call", concatenation.Arguments[1])
				}
			}
		})
	}
}

func TestParseConcatenationPreservesContiguousDots(t *testing.T) {
	t.Parallel()

	const source = `index=main | eval dotted=a.b, doubled=foo..bar, leading=.5, decimal=1.25, quoted="a.b", escaped=payload.http\.status.code, named=concat`
	query, err := Parse(source)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	assignments := query.Commands[0].(*EvalCommand).Assignments
	if len(assignments) != 7 {
		t.Fatalf("assignments = %#v, want seven", assignments)
	}
	for index, want := range []string{"a.b", "foo..bar"} {
		field, ok := assignments[index].Expression.(*ScalarFieldExpr)
		if !ok || field.Field != want {
			t.Fatalf("assignment %d expression = %#v, want field %q", index, assignments[index].Expression, want)
		}
	}
	for index, want := range []string{".5", "1.25"} {
		literal, ok := assignments[index+2].Expression.(*ScalarLiteralExpr)
		if !ok || literal.Value.Kind != LiteralKindFloat || literal.Value.Text != want {
			t.Fatalf("assignment %d expression = %#v, want float %q", index+2, assignments[index+2].Expression, want)
		}
	}
	quoted, ok := assignments[4].Expression.(*ScalarLiteralExpr)
	if !ok || quoted.Value.Kind != LiteralKindString || quoted.Value.Text != "a.b" ||
		!quoted.Value.Quoted {
		t.Fatalf("quoted expression = %#v, want one string literal", assignments[4].Expression)
	}
	escaped, ok := assignments[5].Expression.(*ScalarFieldExpr)
	if !ok || escaped.Field != `payload.http\.status.code` {
		t.Fatalf(
			"escaped expression = %#v, want one escaped dotted field",
			assignments[5].Expression,
		)
	}
	named, ok := assignments[6].Expression.(*ScalarFieldExpr)
	if !ok || named.Field != "concat" {
		t.Fatalf("named expression = %#v, want field concat", assignments[6].Expression)
	}
}

func TestParseConcatenationPrecedesComparisonAndBooleanConnectors(t *testing.T) {
	t.Parallel()

	const source = `index=main | where first . last = "AdaLovelace" OR NOT code . suffix = "500x" AND status=200`
	query, err := Parse(source)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	root, ok := query.Commands[0].(*WhereCommand).Expression.(*WhereBoolExpr)
	if !ok || root.Op != BoolOpOr {
		t.Fatalf("where root = %#v, want OR", query.Commands[0])
	}
	left, ok := root.Left.(*WhereComparisonExpr)
	if !ok || left.Op != CompareOpEqual {
		t.Fatalf("OR left = %#v, want equality", root.Left)
	}
	leftConcatenation := requireConcatenationCall(t, left.Left, 2)
	if got := concatenationRangeText(source, leftConcatenation.Range); got != "first . last" {
		t.Fatalf("left concatenation range = %q", got)
	}

	right, ok := root.Right.(*WhereBoolExpr)
	if !ok || right.Op != BoolOpAnd {
		t.Fatalf("OR right = %#v, want AND", root.Right)
	}
	not, ok := right.Left.(*WhereNotExpr)
	if !ok {
		t.Fatalf("AND left = %#v, want NOT", right.Left)
	}
	rightLeft, ok := not.Operand.(*WhereComparisonExpr)
	if !ok || rightLeft.Op != CompareOpEqual {
		t.Fatalf("NOT operand = %#v, want equality", not.Operand)
	}
	rightConcatenation := requireConcatenationCall(t, rightLeft.Left, 2)
	if got := concatenationRangeText(source, rightConcatenation.Range); got != "code . suffix" {
		t.Fatalf("right concatenation range = %q", got)
	}
	assertWhereLiteralComparison(t, right.Right, "status", CompareOpEqual, "200", false)
}

func TestParseConcatenationComposesWithCallsAndCaseValues(t *testing.T) {
	t.Parallel()

	const source = `index=main | eval nested=lower(first . last), decorated=lower(first) . upper(last), fallback=coalesce(first . last, "unknown") . ":" . tostring(code), choice=if(first . last = "AdaLovelace", first . last, "missing") . "!", selected=case(status=200, first . last, status=404, "missing") . "!"`
	query, err := Parse(source)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	assignments := query.Commands[0].(*EvalCommand).Assignments
	if len(assignments) != 5 {
		t.Fatalf("assignments = %#v, want five", assignments)
	}

	lower, lowerOK := assignments[0].Expression.(*ScalarCallExpr)
	if !lowerOK || lower.Function != ScalarFunctionLower ||
		len(lower.Arguments) != 1 {
		t.Fatalf("nested expression = %#v, want lower call", assignments[0].Expression)
	}
	requireConcatenationCall(t, lower.Arguments[0], 2)

	decorated := requireConcatenationCall(t, assignments[1].Expression, 2)
	if first, ok := decorated.Arguments[0].(*ScalarCallExpr); !ok ||
		first.Function != ScalarFunctionLower {
		t.Fatalf("decorated first argument = %#v, want lower call", decorated.Arguments[0])
	}
	if second, ok := decorated.Arguments[1].(*ScalarCallExpr); !ok ||
		second.Function != ScalarFunctionUpper {
		t.Fatalf("decorated second argument = %#v, want upper call", decorated.Arguments[1])
	}

	fallback := requireConcatenationCall(t, assignments[2].Expression, 3)
	coalesce, coalesceOK := fallback.Arguments[0].(*ScalarCallExpr)
	if !coalesceOK || coalesce.Function != ScalarFunctionCoalesce ||
		len(coalesce.Arguments) != 2 {
		t.Fatalf("fallback first argument = %#v, want coalesce call", fallback.Arguments[0])
	}
	requireConcatenationCall(t, coalesce.Arguments[0], 2)
	if converted, ok := fallback.Arguments[2].(*ScalarCallExpr); !ok ||
		converted.Function != ScalarFunctionToString {
		t.Fatalf("fallback third argument = %#v, want tostring call", fallback.Arguments[2])
	}

	choice := requireConcatenationCall(t, assignments[3].Expression, 2)
	conditionalIf, ok := choice.Arguments[0].(*ScalarIfExpr)
	if !ok {
		t.Fatalf("choice first argument = %#v, want if expression", choice.Arguments[0])
	}
	ifCondition, ok := conditionalIf.Condition.(*WhereComparisonExpr)
	if !ok || ifCondition.Op != CompareOpEqual {
		t.Fatalf("if condition = %#v, want equality", conditionalIf.Condition)
	}
	requireConcatenationCall(t, ifCondition.Left, 2)
	requireConcatenationCall(t, conditionalIf.True, 2)

	selected := requireConcatenationCall(t, assignments[4].Expression, 2)
	conditional, ok := selected.Arguments[0].(*ScalarCaseExpr)
	if !ok || len(conditional.Branches) != 2 {
		t.Fatalf("selected first argument = %#v, want two-branch case", selected.Arguments[0])
	}
	requireConcatenationCall(t, conditional.Branches[0].Value, 2)
	if got := concatenationRangeText(source, selected.Range); got !=
		`case(status=200, first . last, status=404, "missing") . "!"` {
		t.Fatalf("selected concatenation range = %q", got)
	}
}

func TestParseConcatenationComposesWithWhereAndConditionalCount(t *testing.T) {
	t.Parallel()

	for _, source := range []string{
		`index=main | where first . last = "AdaLovelace"`,
		`index=main | stats count(eval(first . last = "AdaLovelace")) AS matches`,
	} {
		if _, err := Parse(source); err != nil {
			t.Fatalf("Parse(%q): %v", source, err)
		}
	}
}

func TestParseConcatenationRejectsBooleanFunctionOperands(t *testing.T) {
	t.Parallel()

	for _, source := range []string{
		`index=main | eval value=true . "x"`,
		`index=main | eval value=isnull(first) . "x"`,
		`index=main | eval value=if(status=200, true, false) . "x"`,
		`index=main | eval value="x" . if(status=200, isnotnull(second), false)`,
		`index=main | eval value=case(status=200, true) . "x"`,
		`index=main | eval value=case(status=200, isnull(first)) . "x"`,
		`index=main | eval value=coalesce(true, false) . "x"`,
		`index=main | eval value=coalesce(isnull(first), false) . "x"`,
	} {
		assertParseDiagnosticCode(t, source, "SPL_UNSUPPORTED_EVAL_EXPRESSION")
	}

	if _, err := Parse(
		`index=main | eval value=tostring(true)."x"`,
	); err != nil {
		t.Fatalf("Parse(explicit Boolean tostring concatenation): %v", err)
	}
}

func TestParseConcatenationRejectsMalformedAndAmbiguousDots(t *testing.T) {
	t.Parallel()

	for _, expression := range []string{
		`. first`,
		`first .`,
		`first . . second`,
		`a. b`,
		`a .b`,
	} {
		source := `index=main | eval value=` + expression
		if _, err := Parse(source); err == nil {
			t.Fatalf("Parse(%q) succeeded, want malformed-concatenation diagnostic", source)
		}
	}
}

func TestParseConcatFunctionSpellingRemainsUnsupported(t *testing.T) {
	t.Parallel()

	for _, source := range []string{
		`index=main | eval value=concat(first)`,
		`index=main | eval value=concat(first, second)`,
	} {
		assertParseDiagnosticCode(t, source, "SPL_UNSUPPORTED_EVAL_FUNCTION")
	}
}

func TestParseConcatenationEnforcesFlattenedArgumentLimit(t *testing.T) {
	t.Parallel()

	atLimit := concatenationSourceWithArgumentCount(MaximumConcatenationOperands)
	query, err := Parse(atLimit)
	if err != nil {
		t.Fatalf("Parse(at concatenation argument limit): %v", err)
	}
	expression := query.Commands[0].(*EvalCommand).Assignments[0].Expression
	requireConcatenationCall(t, expression, MaximumConcatenationOperands)

	assertParseDiagnosticCode(
		t,
		concatenationSourceWithArgumentCount(MaximumConcatenationOperands+1),
		"SPL_QUERY_TOO_COMPLEX",
	)
}

func TestParseConcatenationEnforcesQueryWideOperandLimit(t *testing.T) {
	t.Parallel()

	atLimitCounts := make(
		[]int,
		MaximumConcatenationOperandsPerQuery/MaximumConcatenationOperands,
	)
	for index := range atLimitCounts {
		atLimitCounts[index] = MaximumConcatenationOperands
	}
	atLimit := concatenationQueryWithOperandCounts(atLimitCounts)
	if _, err := Parse(atLimit); err != nil {
		t.Fatalf("Parse(at query-wide concatenation operand limit): %v", err)
	}

	overLimitCounts := append(
		[]int(nil),
		atLimitCounts[:len(atLimitCounts)-1]...,
	)
	overLimitCounts = append(
		overLimitCounts,
		MaximumConcatenationOperands-1,
		2,
	)
	overLimit := concatenationQueryWithOperandCounts(overLimitCounts)
	assertParseDiagnosticCode(t, overLimit, "SPL_QUERY_TOO_COMPLEX")
}

func TestConcatenationDotTokenRemainsLiteralInSearchGrammar(t *testing.T) {
	t.Parallel()

	query, err := Parse(`.`)
	if err != nil {
		t.Fatalf("Parse(bare dot search): %v", err)
	}
	term, ok := query.Search.(*TermExpr)
	if !ok || term.Value != "." || term.Quoted {
		t.Fatalf("bare dot search = %#v, want literal dot term", query.Search)
	}

	query, err = Parse(`message=.`)
	if err != nil {
		t.Fatalf("Parse(dot comparison): %v", err)
	}
	comparison, ok := query.Search.(*ComparisonExpr)
	if !ok || comparison.Field != "message" ||
		comparison.Value.Text != "." ||
		comparison.Value.Kind != LiteralKindString {
		t.Fatalf("dot comparison = %#v, want message='.'", query.Search)
	}

	query, err = Parse(`index=main | search .`)
	if err != nil {
		t.Fatalf("Parse(pipeline dot search): %v", err)
	}
	search := query.Commands[0].(*SearchCommand)
	term, ok = search.Expression.(*TermExpr)
	if !ok || term.Value != "." || term.Quoted {
		t.Fatalf(
			"pipeline dot search = %#v, want literal dot term",
			search.Expression,
		)
	}
}

func requireConcatenationCall(t *testing.T, expression ScalarExpr, argumentCount int) *ScalarCallExpr {
	t.Helper()

	call, ok := expression.(*ScalarCallExpr)
	if !ok || call.Function != ScalarFunctionConcat || len(call.Arguments) != argumentCount {
		t.Fatalf(
			"expression = %#v, want flattened %d-argument concatenation",
			expression,
			argumentCount,
		)
	}
	return call
}

func concatenationRangeText(source string, sourceRange Range) string {
	return source[sourceRange.Start.Offset:sourceRange.End.Offset]
}

func concatenationSourceWithArgumentCount(count int) string {
	arguments := make([]string, count)
	for index := range arguments {
		arguments[index] = "f" + strconv.Itoa(index)
	}
	return `index=main | eval value=` + strings.Join(arguments, ` . `)
}

func concatenationQueryWithOperandCounts(operandCounts []int) string {
	assignments := make([]string, len(operandCounts))
	fieldOffset := 0
	for chainIndex := range assignments {
		arguments := make([]string, operandCounts[chainIndex])
		for argumentIndex := range arguments {
			arguments[argumentIndex] = "f" + strconv.Itoa(
				fieldOffset+argumentIndex,
			)
		}
		fieldOffset += len(arguments)
		assignments[chainIndex] = "value" + strconv.Itoa(chainIndex) + "=" +
			strings.Join(arguments, ` . `)
	}
	return `index=main | eval ` + strings.Join(assignments, ", ")
}
