package plan

import (
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/spl"
)

func TestBuildEvalConcatenationFlattensTypedScalarIRAndPreservesRanges(t *testing.T) {
	t.Parallel()

	const source = `index=gradethis | eval label="route=" . http.route . ":" . tostring(status)`
	logical, err := Build(
		mustParse(t, source),
		testScope([]string{"gradethis"}, nil),
	)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	extend := logical.Operators[len(logical.Operators)-1].(*Extend)
	if len(extend.Assignments) != 1 {
		t.Fatalf("assignments = %#v", extend.Assignments)
	}
	concat, ok := extend.Assignments[0].Expression.(*ScalarCallExpression)
	if !ok || concat.Function != ScalarFunctionConcat ||
		len(concat.Arguments) != 4 {
		t.Fatalf("concatenation IR = %#v", extend.Assignments[0].Expression)
	}
	assertConcatenationSourceText(
		t,
		source,
		concat,
		`"route=" . http.route . ":" . tostring(status)`,
	)

	prefix, ok := concat.Arguments[0].(*ScalarLiteralExpression)
	if !ok || prefix.Value.Kind != ValueKindString ||
		prefix.Value.String != "route=" {
		t.Fatalf("first argument = %#v, want route prefix", concat.Arguments[0])
	}
	assertConcatenationSourceText(t, source, prefix, `"route="`)

	route, ok := concat.Arguments[1].(*ScalarFieldExpression)
	if !ok || route.Field.Name != "http.route" || route.Field.Canonical ||
		!slices.Equal(route.Field.Path, []string{"http", "route"}) {
		t.Fatalf("second argument = %#v, want resolved http.route path", concat.Arguments[1])
	}
	assertConcatenationSourceText(t, source, route, "http.route")

	separator, ok := concat.Arguments[2].(*ScalarLiteralExpression)
	if !ok || separator.Value.Kind != ValueKindString ||
		separator.Value.String != ":" {
		t.Fatalf("third argument = %#v, want separator", concat.Arguments[2])
	}
	assertConcatenationSourceText(t, source, separator, `":"`)

	rendered, ok := concat.Arguments[3].(*ScalarCallExpression)
	if !ok || rendered.Function != ScalarFunctionToString ||
		len(rendered.Arguments) != 1 {
		t.Fatalf("fourth argument = %#v, want tostring call", concat.Arguments[3])
	}
	assertConcatenationSourceText(t, source, rendered, "tostring(status)")
	status, ok := rendered.Arguments[0].(*ScalarFieldExpression)
	if !ok || status.Field.Name != "status" {
		t.Fatalf("tostring argument = %#v, want status field", rendered.Arguments[0])
	}
	assertConcatenationSourceText(t, source, status, "status")
}

func TestBuildEvalConcatenationComposesInsideAndAroundScalarFunctions(t *testing.T) {
	t.Parallel()

	const source = `index=gradethis | eval folded=upper("service=" . lower(service))`
	logical, err := Build(
		mustParse(t, source),
		testScope([]string{"gradethis"}, nil),
	)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	extend := logical.Operators[len(logical.Operators)-1].(*Extend)
	upper, ok := extend.Assignments[0].Expression.(*ScalarCallExpression)
	if !ok || upper.Function != ScalarFunctionUpper ||
		len(upper.Arguments) != 1 {
		t.Fatalf("outer scalar IR = %#v, want upper call", extend.Assignments[0].Expression)
	}
	assertConcatenationSourceText(
		t,
		source,
		upper,
		`upper("service=" . lower(service))`,
	)

	concat, ok := upper.Arguments[0].(*ScalarCallExpression)
	if !ok || concat.Function != ScalarFunctionConcat ||
		len(concat.Arguments) != 2 {
		t.Fatalf("upper argument = %#v, want concatenation", upper.Arguments[0])
	}
	assertConcatenationSourceText(
		t,
		source,
		concat,
		`"service=" . lower(service)`,
	)
	lower, ok := concat.Arguments[1].(*ScalarCallExpression)
	if !ok || lower.Function != ScalarFunctionLower ||
		len(lower.Arguments) != 1 {
		t.Fatalf("concatenation suffix = %#v, want lower call", concat.Arguments[1])
	}
	assertConcatenationSourceText(t, source, lower, "lower(service)")
}

func TestBuildEvalConcatenationRejectsForgedArityEnumAndArguments(t *testing.T) {
	t.Parallel()

	base := mustParse(t, `index=gradethis`)
	sourceRange := spl.Range{
		Start: spl.Position{Line: 1, Column: 1},
		End:   spl.Position{Line: 1, Column: 5},
	}
	stringArgument := func() spl.ScalarExpr {
		return &spl.ScalarLiteralExpr{
			Value: spl.Literal{
				Kind:  spl.LiteralKindString,
				Text:  "value",
				Range: sourceRange,
			},
			Range: sourceRange,
		}
	}
	fieldArgument := func() spl.ScalarExpr {
		return &spl.ScalarFieldExpr{Field: "message", Range: sourceRange}
	}
	boolArgument := func() spl.ScalarExpr {
		return &spl.ScalarLiteralExpr{
			Value: spl.Literal{
				Kind:  spl.LiteralKindBool,
				Text:  "true",
				Range: sourceRange,
			},
			Range: sourceRange,
		}
	}
	tooMany := make(
		[]spl.ScalarExpr,
		spl.MaximumConcatenationOperands+1,
	)
	for index := range tooMany {
		tooMany[index] = stringArgument()
	}
	var typedNil *spl.ScalarLiteralExpr
	for _, test := range []struct {
		name       string
		expression spl.ScalarExpr
		code       string
	}{
		{
			name: "zero arguments",
			expression: &spl.ScalarCallExpr{
				Function: spl.ScalarFunctionConcat,
				Range:    sourceRange,
			},
			code: "SPL_INVALID_EVAL_ARITY",
		},
		{
			name: "one argument",
			expression: &spl.ScalarCallExpr{
				Function:  spl.ScalarFunctionConcat,
				Arguments: []spl.ScalarExpr{stringArgument()},
				Range:     sourceRange,
			},
			code: "SPL_INVALID_EVAL_ARITY",
		},
		{
			name: "thirty-three arguments",
			expression: &spl.ScalarCallExpr{
				Function:  spl.ScalarFunctionConcat,
				Arguments: tooMany,
				Range:     sourceRange,
			},
			code: "SPL_QUERY_TOO_COMPLEX",
		},
		{
			name: "typed nil argument",
			expression: &spl.ScalarCallExpr{
				Function: spl.ScalarFunctionConcat,
				Arguments: []spl.ScalarExpr{
					stringArgument(),
					typedNil,
				},
				Range: sourceRange,
			},
			code: "SPL_UNSUPPORTED_EVAL_EXPRESSION",
		},
		{
			name: "Boolean function argument",
			expression: &spl.ScalarCallExpr{
				Function: spl.ScalarFunctionConcat,
				Arguments: []spl.ScalarExpr{
					stringArgument(),
					&spl.ScalarCallExpr{
						Function:  spl.ScalarFunctionIsNull,
						Arguments: []spl.ScalarExpr{fieldArgument()},
						Range:     sourceRange,
					},
				},
				Range: sourceRange,
			},
			code: "SPL_UNSUPPORTED_EVAL_EXPRESSION",
		},
		{
			name: "Boolean literal argument",
			expression: &spl.ScalarCallExpr{
				Function: spl.ScalarFunctionConcat,
				Arguments: []spl.ScalarExpr{
					stringArgument(),
					boolArgument(),
				},
				Range: sourceRange,
			},
			code: "SPL_UNSUPPORTED_EVAL_EXPRESSION",
		},
		{
			name: "Boolean conditional argument",
			expression: &spl.ScalarCallExpr{
				Function: spl.ScalarFunctionConcat,
				Arguments: []spl.ScalarExpr{
					stringArgument(),
					&spl.ScalarIfExpr{
						Condition: &spl.WhereComparisonExpr{
							Left:  fieldArgument(),
							Op:    spl.CompareOpEqual,
							Right: stringArgument(),
							Range: sourceRange,
						},
						True:  boolArgument(),
						False: boolArgument(),
						Range: sourceRange,
					},
				},
				Range: sourceRange,
			},
			code: "SPL_UNSUPPORTED_EVAL_EXPRESSION",
		},
		{
			name: "Boolean coalesce argument",
			expression: &spl.ScalarCallExpr{
				Function: spl.ScalarFunctionConcat,
				Arguments: []spl.ScalarExpr{
					stringArgument(),
					&spl.ScalarCallExpr{
						Function: spl.ScalarFunctionCoalesce,
						Arguments: []spl.ScalarExpr{
							boolArgument(),
							boolArgument(),
						},
						Range: sourceRange,
					},
				},
				Range: sourceRange,
			},
			code: "SPL_UNSUPPORTED_EVAL_EXPRESSION",
		},
		{
			name: "invalid function enum",
			expression: &spl.ScalarCallExpr{
				Function: spl.ScalarFunctionInvalid,
				Arguments: []spl.ScalarExpr{
					stringArgument(),
					stringArgument(),
				},
				Range: sourceRange,
			},
			code: "SPL_UNSUPPORTED_EVAL_FUNCTION",
		},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			assertForgedEvalBuildDiagnostic(
				t,
				base,
				sourceRange,
				test.expression,
				test.code,
			)
		})
	}
}

func TestBuildConcatenationIndependentlyEnforcesQueryWideOperandLimit(
	t *testing.T,
) {
	t.Parallel()

	atLimitCounts := make(
		[]int,
		spl.MaximumConcatenationOperandsPerQuery/
			spl.MaximumConcatenationOperands,
	)
	for index := range atLimitCounts {
		atLimitCounts[index] = spl.MaximumConcatenationOperands
	}
	query := mustParse(t, planConcatenationSourceWithOperandCounts(atLimitCounts))
	if _, err := Build(
		query,
		testScope([]string{"gradethis"}, nil),
	); err != nil {
		t.Fatalf("Build(at query-wide concatenation operand limit): %v", err)
	}

	eval := query.Commands[0].(*spl.EvalCommand)
	last := eval.Assignments[len(eval.Assignments)-1].
		Expression.(*spl.ScalarCallExpr)
	last.Arguments = last.Arguments[:len(last.Arguments)-1]
	sourceRange := last.Range
	eval.Assignments = append(eval.Assignments, spl.EvalAssignment{
		Field:      "overflow",
		FieldRange: sourceRange,
		Expression: &spl.ScalarCallExpr{
			Function: spl.ScalarFunctionConcat,
			Arguments: []spl.ScalarExpr{
				&spl.ScalarFieldExpr{Field: "left", Range: sourceRange},
				&spl.ScalarFieldExpr{Field: "right", Range: sourceRange},
			},
			Range: sourceRange,
		},
		Range: sourceRange,
	})
	_, err := Build(query, testScope([]string{"gradethis"}, nil))
	assertDiagnosticCode(t, err, "SPL_QUERY_TOO_COMPLEX")
}

func assertConcatenationSourceText(
	t *testing.T,
	source string,
	expression ScalarExpression,
	want string,
) {
	t.Helper()
	sourceRange := expression.SourceRange()
	if sourceRange.Start.Offset < 0 ||
		sourceRange.End.Offset < sourceRange.Start.Offset ||
		sourceRange.End.Offset > len(source) {
		t.Fatalf("source range = %#v, outside %d-byte query", sourceRange, len(source))
	}
	if got := source[sourceRange.Start.Offset:sourceRange.End.Offset]; got != want {
		t.Fatalf("source range text = %q, want %q", got, want)
	}
}

func planConcatenationSourceWithOperandCounts(operandCounts []int) string {
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
	return `index=gradethis | eval ` + strings.Join(assignments, ", ")
}
