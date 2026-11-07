package spl

import "testing"

func TestParseBooleanConstantsPreserveFunctionAndRange(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		call     string
		function ScalarFunction
	}{
		{"TrUe()", ScalarFunctionTrue},
		{"FaLsE( )", ScalarFunctionFalse},
	} {
		source := "index=main | where " + test.call
		query, err := Parse(source)
		if err != nil {
			t.Fatal(err)
		}
		predicate := query.Commands[0].(*WhereCommand).Expression.(*WhereScalarPredicateExpr)
		call := predicate.Value.(*ScalarCallExpr)
		if call.Function != test.function || len(call.Arguments) != 0 || !call.Function.ReturnsBoolean() {
			t.Fatalf("Boolean constant = %#v", call)
		}
		if got := source[call.Range.Start.Offset:call.Range.End.Offset]; got != test.call {
			t.Fatalf("range = %q, want %q", got, test.call)
		}
		standalone, err := ParseScalarExpression(test.call)
		if err != nil {
			t.Fatal(err)
		}
		if got := standalone.(*ScalarCallExpr).Function; got != test.function {
			t.Fatalf("standalone function = %v, want %v", got, test.function)
		}
	}
}

func TestSuggestBooleanConstantsInCaseAndWhere(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		source    string
		insertion string
	}{
		{`index=main | eval label=case(status=200, "ok", tru`, "true()"},
		{`index=main | where fal`, "false()"},
	} {
		result := Suggest(test.source, len(test.source), 20)
		if result.Diagnostic != nil {
			t.Fatal(result.Diagnostic)
		}
		if len(result.Suggestions) != 1 || result.Suggestions[0].Insertion != test.insertion {
			t.Fatalf("suggestions for %q = %#v, want %q", test.source, result.Suggestions, test.insertion)
		}
	}
}

func TestParseBooleanConstantConsumers(t *testing.T) {
	t.Parallel()

	for _, source := range []string{
		`index=main | eval label=case(false(), "never", true(), "fallback")`,
		`index=main | eval label=if(NOT false() AND true(), "yes", "no")`,
		`index=main | where false() OR true() AND NOT false()`,
		`index=main | where true()=false()`,
		`index=main | where coalesce(null, true())`,
		`index=main | eval label=tostring(true()), kind=typeof(false())`,
		`index=main | stats count(eval(true())) AS all, count(eval(false())) AS none`,
		`index=main | eventstats count(eval(true())) AS all`,
		`index=main | streamstats count(eval(false())) AS none`,
	} {
		if _, err := Parse(source); err != nil {
			t.Fatalf("Parse(%q): %v", source, err)
		}
	}
	for _, call := range []string{"true(1)", "false(1, 2)", "TRUE(field)"} {
		assertParseDiagnosticCode(t, "index=main | where "+call, "SPL_INVALID_EVAL_ARITY")
	}
	for _, expression := range []string{
		"true()", "false()", "tonumber(true())", `true()."text"`,
		`case(true(), false())`, `if(true(), true(), false())`,
	} {
		assertParseDiagnosticCode(t, "index=main | eval value="+expression, "SPL_UNSUPPORTED_EVAL_EXPRESSION")
	}
}
