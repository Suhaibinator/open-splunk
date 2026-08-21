package plan

import (
	"strings"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/spl"
	"github.com/Suhaibinator/open-splunk/internal/splregex"
)

func TestBuildNativeMultivalueFunctionsPreservesTypedIR(t *testing.T) {
	t.Parallel()

	logical, err := Build(
		mustParse(t, `index=gradethis | eval `+
			`parts=split(message, ""), `+
			`all=mvappend(parts, 7, true, null), `+
			`unique=mvdedup(all), `+
			`one=mvindex(unique, -1), `+
			`many=mvindex(unique, 0, 2), `+
			`joined=mvjoin(unique, "|"), `+
			`zipped=mvzip(parts, unique, "::"), `+
			`found=mvfind(unique, "(?i)^api")`),
		testScope([]string{"gradethis"}, nil),
	)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	assignments := logical.Operators[len(logical.Operators)-1].(*Extend).Assignments
	want := []struct {
		function ScalarFunction
		arity    int
	}{
		{ScalarFunctionSplit, 2},
		{ScalarFunctionMVAppend, 4},
		{ScalarFunctionMVDedup, 1},
		{ScalarFunctionMVIndex, 2},
		{ScalarFunctionMVIndex, 3},
		{ScalarFunctionMVJoin, 2},
		{ScalarFunctionMVZip, 3},
		{ScalarFunctionMVFind, 2},
	}
	if len(assignments) != len(want) {
		t.Fatalf("assignments = %#v", assignments)
	}
	for index, expected := range want {
		call, ok := assignments[index].Expression.(*ScalarCallExpression)
		if !ok || call.Function != expected.function ||
			len(call.Arguments) != expected.arity {
			t.Fatalf("assignment %d = %#v, want function %d/%d arguments",
				index, assignments[index].Expression, expected.function, expected.arity)
		}
	}
	for argumentIndex, value := range []int64{-1, 0, 2} {
		assignmentIndex := 3
		callArgumentIndex := 1
		if argumentIndex > 0 {
			assignmentIndex = 4
			callArgumentIndex = argumentIndex
		}
		literal, ok := assignments[assignmentIndex].Expression.(*ScalarCallExpression).
			Arguments[callArgumentIndex].(*ScalarLiteralExpression)
		if !ok || literal.Value.Kind != ValueKindInt64 || literal.Value.Int64 != value {
			t.Fatalf("mvindex literal %d = %#v, want %d", argumentIndex, literal, value)
		}
	}
}

func TestBuildNativeMultivalueFunctionsRejectsForgedConstraints(t *testing.T) {
	t.Parallel()

	field := func() spl.ScalarExpr {
		return &spl.ScalarFieldExpr{Field: "values", Range: forgedScalarRange}
	}
	quoted := func(text string) spl.ScalarExpr {
		return &spl.ScalarLiteralExpr{
			Value: spl.Literal{
				Kind:   spl.LiteralKindString,
				Text:   text,
				Quoted: true,
				Range:  forgedScalarRange,
			},
			Range: forgedScalarRange,
		}
	}
	integer := func(text string) spl.ScalarExpr {
		return &spl.ScalarLiteralExpr{
			Value: spl.Literal{
				Kind:  spl.LiteralKindInteger,
				Text:  text,
				Range: forgedScalarRange,
			},
			Range: forgedScalarRange,
		}
	}
	call := func(function spl.ScalarFunction, arguments ...spl.ScalarExpr) spl.ScalarExpr {
		return &spl.ScalarCallExpr{
			Function:  function,
			Arguments: arguments,
			Range:     forgedScalarRange,
		}
	}
	var typedNil *spl.ScalarLiteralExpr
	for _, test := range []struct {
		name       string
		expression spl.ScalarExpr
		code       string
	}{
		{"split field delimiter", call(spl.ScalarFunctionSplit, field(), field()), "SPL_UNSUPPORTED_EVAL_EXPRESSION"},
		{"split invalid UTF-8 delimiter", call(spl.ScalarFunctionSplit, field(), quoted("\xff")), "SPL_UNSUPPORTED_EVAL_EXPRESSION"},
		{"split oversized delimiter", call(spl.ScalarFunctionSplit, field(), quoted(strings.Repeat("x", spl.MaximumMVDelimiterBytes+1))), "SPL_QUERY_TOO_COMPLEX"},
		{"mvappend empty", call(spl.ScalarFunctionMVAppend), "SPL_INVALID_EVAL_ARITY"},
		{"mvappend oversized", call(spl.ScalarFunctionMVAppend, forgedScalarArguments(spl.MaximumMVAppendArguments+1)...), "SPL_QUERY_TOO_COMPLEX"},
		{"mvindex one argument", call(spl.ScalarFunctionMVIndex, field()), "SPL_INVALID_EVAL_ARITY"},
		{"mvindex field start", call(spl.ScalarFunctionMVIndex, field(), field()), "SPL_UNSUPPORTED_MV_INDEX"},
		{"mvindex typed nil start", call(spl.ScalarFunctionMVIndex, field(), typedNil), "SPL_UNSUPPORTED_EVAL_EXPRESSION"},
		{"mvindex underflow", call(spl.ScalarFunctionMVIndex, field(), integer("-2147483649")), "SPL_NUMBER_OUT_OF_RANGE"},
		{"mvindex overflow", call(spl.ScalarFunctionMVIndex, field(), integer("2147483648")), "SPL_NUMBER_OUT_OF_RANGE"},
		{"mvjoin field delimiter", call(spl.ScalarFunctionMVJoin, field(), field()), "SPL_UNSUPPORTED_EVAL_EXPRESSION"},
		{"mvzip four arguments", call(spl.ScalarFunctionMVZip, field(), field(), quoted(","), quoted("extra")), "SPL_INVALID_EVAL_ARITY"},
		{"mvzip field delimiter", call(spl.ScalarFunctionMVZip, field(), field(), field()), "SPL_UNSUPPORTED_EVAL_EXPRESSION"},
		{"mvfind field pattern", call(spl.ScalarFunctionMVFind, field(), field()), "SPL_UNSUPPORTED_EVAL_EXPRESSION"},
		{"mvfind invalid pattern", call(spl.ScalarFunctionMVFind, field(), quoted("(?=secret)")), "SPL_UNSUPPORTED_REGEX"},
		{"mvfind oversized pattern", call(spl.ScalarFunctionMVFind, field(), quoted(strings.Repeat("x", splregex.MaximumMatchPatternBytes+1))), "SPL_QUERY_TOO_COMPLEX"},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			diagnostic := buildForgedScalar(t, test.expression)
			if diagnostic.Code != test.code {
				t.Fatalf("diagnostic = %#v, want %s", diagnostic, test.code)
			}
		})
	}
}

func TestBuildArithmeticRejectsKnownMultivalueFunctionResults(t *testing.T) {
	t.Parallel()

	for _, expression := range []string{
		`split(message, ",")`,
		`mvappend(values)`,
		`mvdedup(values)`,
		`mvindex(values, 0, 1)`,
		`mvjoin(values, ",")`,
		`mvzip(left, right)`,
	} {
		_, err := Build(
			mustParse(t, `index=gradethis | eval invalid=`+expression+`+1`),
			testScope([]string{"gradethis"}, nil),
		)
		assertDiagnosticCode(t, err, "SPL_UNSUPPORTED_ARITHMETIC_VALUE_TYPE")
	}
}

func TestAnalyzeEventAggregateAllowsNativeMultivalueFunctions(t *testing.T) {
	t.Parallel()

	logical, err := Build(
		mustParse(t, `index=gradethis | eventstats count(eval(mvfind(mvdedup(split(message, ",")), "api")>=0)) AS matches BY host`),
		testScope([]string{"gradethis"}, nil),
	)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if _, err := Analyze(logical); err != nil {
		t.Fatalf("Analyze: %v", err)
	}
}

func TestAnalyzeRejectsForgedNativeMultivalueCallsInAggregateInputs(t *testing.T) {
	t.Parallel()

	field := &ScalarFieldExpression{Field: FieldRef{Name: "values", Path: []string{"values"}}}
	quoted := func(value string) ScalarExpression {
		return &ScalarLiteralExpression{Value: Value{Kind: ValueKindString, String: value, Quoted: true}}
	}
	for _, test := range []struct {
		name string
		call *ScalarCallExpression
	}{
		{
			name: "split nonliteral delimiter",
			call: &ScalarCallExpression{Function: ScalarFunctionSplit, Arguments: []ScalarExpression{field, field}},
		},
		{
			name: "mvindex nonliteral index",
			call: &ScalarCallExpression{Function: ScalarFunctionMVIndex, Arguments: []ScalarExpression{field, field}},
		},
		{
			name: "mvfind invalid RE2 pattern",
			call: &ScalarCallExpression{Function: ScalarFunctionMVFind, Arguments: []ScalarExpression{field, quoted("(?=secret)")}},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			logical, err := Build(
				mustParse(t, `index=gradethis | stats sum(eval(1+1)) AS total`),
				testScope([]string{"gradethis"}, nil),
			)
			if err != nil {
				t.Fatalf("Build: %v", err)
			}
			aggregate := logical.Operators[len(logical.Operators)-1].(*Aggregate)
			aggregate.Measures[0].InputExpression = test.call
			if _, analyzeErr := Analyze(logical); analyzeErr == nil ||
				!strings.Contains(analyzeErr.Error(), "scalar function metadata is invalid") {
				t.Fatalf("Analyze forged call error = %v", analyzeErr)
			}
		})
	}
}
