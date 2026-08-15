package spl

import (
	"errors"
	"slices"
	"testing"
)

// scalarCallDiagnostic parses an eval assignment and returns the structured
// diagnostic the parser produced for the scalar call.
func scalarCallDiagnostic(t *testing.T, call string) *Diagnostic {
	t.Helper()
	source := "index=main | eval value=" + call
	_, err := Parse(source)
	if err == nil {
		t.Fatalf("Parse(%q) unexpectedly succeeded", source)
	}
	diagnostic := &Diagnostic{}
	if !errors.As(err, &diagnostic) {
		t.Fatalf("Parse(%q) error = %#v, want *Diagnostic", source, err)
	}
	return diagnostic
}

// TestParseScalarCallArityBoundariesKeepInvalidEvalArityMessages walks every
// eval function across arity-1, arity+1 and arity zero so the extracted
// invalidEvalArity helper cannot silently rewrite a per-function message.
func TestParseScalarCallArityBoundariesKeepInvalidEvalArityMessages(t *testing.T) {
	t.Parallel()

	for _, test := range []struct{ call, message string }{
		{`now(1)`, "now requires no arguments"},
		{`now(1, 2)`, "now requires no arguments"},
		{`strftime()`, "strftime requires exactly two arguments"},
		{`strftime(_time)`, "strftime requires exactly two arguments"},
		{`strftime(_time, "%Y", "%m")`, "strftime requires exactly two arguments"},
		{`strptime()`, "strptime requires exactly two arguments"},
		{`strptime(stamp)`, "strptime requires exactly two arguments"},
		{`strptime(stamp, "%Y-%m-%d", 1)`, "strptime requires exactly two arguments"},
		{`relative_time()`, "relative_time requires exactly two arguments"},
		{`relative_time(_time)`, "relative_time requires exactly two arguments"},
		{`relative_time(_time, "-1d@d", 1)`, "relative_time requires exactly two arguments"},
		{`tonumber()`, "tonumber requires exactly one argument in compatibility version 0.1"},
		{`tonumber(a, b)`, "tonumber requires exactly one argument in compatibility version 0.1"},
		{`tostring()`, "tostring requires exactly one argument in compatibility version 0.1"},
		{`tostring(a, "hex", 1)`, "tostring requires exactly one argument in compatibility version 0.1"},
		{`replace()`, "replace requires exactly three arguments"},
		{`replace(a, "b")`, "replace requires exactly three arguments"},
		{`replace(a, "b", "c", "d")`, "replace requires exactly three arguments"},
		{`isnull()`, "isnull requires exactly one argument"},
		{`isnull(a, b)`, "isnull requires exactly one argument"},
		{`isnotnull()`, "isnotnull requires exactly one argument"},
		{`isnotnull(a, b)`, "isnotnull requires exactly one argument"},
		{`coalesce()`, "coalesce requires at least one argument"},
		{`lower()`, "lower requires exactly one argument"},
		{`lower(a, b)`, "lower requires exactly one argument"},
		{`upper()`, "upper requires exactly one argument"},
		{`upper(a, b)`, "upper requires exactly one argument"},
		{`len()`, "len requires exactly one argument"},
		{`len(a, b)`, "len requires exactly one argument"},
		{`length()`, "length requires exactly one argument"},
		{`length(a, b)`, "length requires exactly one argument"},
		{`round()`, "round requires one or two arguments"},
		{`round(a, 1, 2)`, "round requires one or two arguments"},
		{`ceil()`, "ceil requires exactly one argument"},
		{`ceil(a, b)`, "ceil requires exactly one argument"},
		{`ceiling()`, "ceiling requires exactly one argument"},
		{`ceiling(a, b)`, "ceiling requires exactly one argument"},
		{`floor()`, "floor requires exactly one argument"},
		{`floor(a, b)`, "floor requires exactly one argument"},
		{`mvcount()`, "mvcount requires exactly one argument"},
		{`mvcount(a, b)`, "mvcount requires exactly one argument"},
		{`mvsort()`, "mvsort requires exactly one argument"},
		{`mvsort(a, b)`, "mvsort requires exactly one argument"},
		{`match()`, "match requires exactly two arguments"},
		{`match(a)`, "match requires exactly two arguments"},
		{`match(a, "b", "c")`, "match requires exactly two arguments"},
		{`like()`, "like requires exactly two arguments"},
		{`like(a)`, "like requires exactly two arguments"},
		{`like(a, "b%", "c")`, "like requires exactly two arguments"},
		{`substr()`, "substr requires two or three arguments"},
		{`substr(a)`, "substr requires two or three arguments"},
		{`substr(a, 1, 2, 3)`, "substr requires two or three arguments"},
		{`if(a=1)`, "if requires exactly three arguments"},
		{`if(a=1, b)`, "if requires exactly three arguments"},
		{`if(a=1, b, c, d)`, "if requires exactly three arguments"},
	} {
		diagnostic := scalarCallDiagnostic(t, test.call)
		if diagnostic.Code != "SPL_INVALID_EVAL_ARITY" || diagnostic.Message != test.message {
			t.Fatalf("%s diagnostic = %s/%q, want SPL_INVALID_EVAL_ARITY/%q",
				test.call, diagnostic.Code, diagnostic.Message, test.message)
		}
		if diagnostic.Range == (Range{}) || len(diagnostic.Suggestions) != 0 {
			t.Fatalf("%s arity diagnostic = %#v, want a source range and no suggestions",
				test.call, diagnostic)
		}
	}

	// tostring(value, format) keeps its dedicated format diagnostic rather
	// than collapsing into the shared arity helper.
	if diagnostic := scalarCallDiagnostic(t, `tostring(a, "hex")`); diagnostic.Code !=
		"SPL_UNSUPPORTED_TOSTRING_FORMAT" {
		t.Fatalf(`tostring(a, "hex") diagnostic = %#v, want SPL_UNSUPPORTED_TOSTRING_FORMAT`, diagnostic)
	}
}

// TestParseScalarCallBooleanArgumentsKeepPerCallSuggestions rejects a Boolean
// result in every position that refuses one and pins the suggestion list each
// call site passes to the extracted booleanArgumentDiagnostic helper.
func TestParseScalarCallBooleanArgumentsKeepPerCallSuggestions(t *testing.T) {
	t.Parallel()

	const conditional = "use isnull or isnotnull directly with where"
	const consume = "consume the Boolean with a supported conditional or conversion function"
	const numeric = "convert a numeric value before rounding it"
	const direct = "use the Boolean directly with where"

	for _, test := range []struct {
		call        string
		message     string
		suggestions []string
	}{
		{`tonumber(isnull(a))`, "tonumber cannot consume a Boolean result in search-mode expressions", []string{conditional, consume}},
		{`replace(isnotnull(a), "b", "c")`, "replace cannot consume a Boolean result in search-mode expressions", []string{conditional, consume}},
		{`lower(match(a, "b"))`, "lower cannot consume a Boolean result in search-mode expressions", []string{conditional, consume}},
		{`upper(like(a, "b%"))`, "upper cannot consume a Boolean result in search-mode expressions", []string{conditional, consume}},
		{`len(isnull(a))`, "len cannot consume a Boolean result in search-mode expressions", []string{conditional, consume}},
		{`length(isnull(a))`, "length cannot consume a Boolean result in search-mode expressions", []string{conditional, consume}},
		{`substr(isnull(a), 1, 2)`, "substr cannot consume a Boolean result in search-mode expressions", []string{conditional, consume}},
		{`round(isnull(a))`, "round cannot consume a Boolean result in search-mode expressions", []string{conditional, numeric}},
		{`round(isnull(a), 2)`, "round cannot consume a Boolean result in search-mode expressions", []string{conditional, numeric}},
		{`ceil(isnull(a))`, "ceil cannot consume a Boolean result in search-mode expressions", []string{conditional, numeric}},
		{`ceiling(isnull(a))`, "ceiling cannot consume a Boolean result in search-mode expressions", []string{conditional, numeric}},
		{`floor(isnull(a))`, "floor cannot consume a Boolean result in search-mode expressions", []string{conditional, numeric}},
		{`mvsort(isnull(a))`, "mvsort cannot consume a Boolean result in search-mode expressions", []string{"mvsort(multivalue_field)"}},
		{`match(isnull(a), "b")`, "match cannot consume a Boolean result in search-mode expressions", []string{direct, `match(value, "pattern")`}},
		{`like(isnull(a), "b%")`, "like cannot consume a Boolean result in search-mode expressions", []string{direct, `like(value, "pattern")`}},
	} {
		diagnostic := scalarCallDiagnostic(t, test.call)
		if diagnostic.Code != "SPL_UNSUPPORTED_EVAL_EXPRESSION" ||
			diagnostic.Message != test.message ||
			!slices.Equal(diagnostic.Suggestions, test.suggestions) {
			t.Fatalf("%s diagnostic = %#v, want SPL_UNSUPPORTED_EVAL_EXPRESSION/%q/%q",
				test.call, diagnostic, test.message, test.suggestions)
		}
		if diagnostic.Range == (Range{}) {
			t.Fatalf("%s diagnostic range is empty", test.call)
		}
	}

	// The time functions keep bespoke Boolean messages that the shared helper
	// must not have absorbed.
	for _, test := range []struct{ call, message string }{
		{`strftime(isnull(a), "%Y")`, "strftime cannot consume a Boolean time value"},
		{`strptime(isnull(a), "%Y-%m-%d")`, "strptime cannot consume a Boolean text value"},
		{`relative_time(isnull(a), "-1d@d")`, "relative_time cannot consume a Boolean time value"},
	} {
		diagnostic := scalarCallDiagnostic(t, test.call)
		if diagnostic.Code != "SPL_UNSUPPORTED_EVAL_EXPRESSION" || diagnostic.Message != test.message {
			t.Fatalf("%s diagnostic = %#v, want SPL_UNSUPPORTED_EVAL_EXPRESSION/%q",
				test.call, diagnostic, test.message)
		}
	}
}
