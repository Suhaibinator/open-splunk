package spl

import (
	"errors"
	"testing"
)

func TestParseSearchAcceptsQuotedFieldNames(t *testing.T) {
	t.Parallel()
	// The explicitly labeled SPL row documents quoted field names with spaces:
	// https://help.splunk.com/en/splunk-enterprise/search/spl2-search-reference/search-command/search-command-usage
	for _, source := range []string{
		`"my field"=10`,
		`index=main | search "my field"="ten"`,
		`"AND"=reserved`,
	} {
		t.Run(source, func(t *testing.T) {
			t.Parallel()
			query, err := Parse(source)
			if err != nil {
				t.Fatalf("Parse: %v", err)
			}
			expression := query.Search
			if len(query.Commands) != 0 {
				expression = query.Commands[0].(*SearchCommand).Expression
			}
			comparison, ok := expression.(*ComparisonExpr)
			if !ok || comparison.Field != map[string]string{`"my field"=10`: "my field", `index=main | search "my field"="ten"`: "my field", `"AND"=reserved`: "AND"}[source] {
				t.Fatalf("expression = %#v, want quoted-field comparison", expression)
			}
		})
	}
}

func TestParseRenameAcceptsQuotedExactFields(t *testing.T) {
	t.Parallel()
	// Official rename examples use a quoted destination containing spaces:
	// https://help.splunk.com/en/splunk-enterprise/search/spl-search-reference/10.2/search-commands/rename
	query, err := Parse(`index=main | rename "old field" AS "Count of Events"`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	assignment := query.Commands[0].(*RenameCommand).Assignments[0]
	if assignment.Source != "old field" || assignment.Destination != "Count of Events" {
		t.Fatalf("assignment = %#v", assignment)
	}
}

func TestParseSearchDecodesEscapedPipe(t *testing.T) {
	t.Parallel()
	// Backslash-pipe sends a literal pipe to the search command:
	// https://help.splunk.com/en/splunk-enterprise/spl-search-reference/9.1/search-commands/search
	query, err := Parse(`index=main message="a\|b"`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	comparison := query.Search.(*BinaryExpr).Right.(*ComparisonExpr)
	if comparison.Value.Text != "a|b" {
		t.Fatalf("literal = %q, want %q", comparison.Value.Text, "a|b")
	}
}

func TestParseRejectsUnsupportedSearchDirectivesPrecisely(t *testing.T) {
	t.Parallel()
	// CASE and TERM are search directives, rather than ordinary raw terms:
	// https://help.splunk.com/en/splunk-enterprise/spl-search-reference/9.1/search-commands/search
	for _, source := range []string{`index=main CASE(error)`, `index=main TERM(foo.bar)`, `index=main host=CASE(LOCALHOST)`, `index=main | search host=TERM(foo.bar)`} {
		_, err := Parse(source)
		diagnostic := &Diagnostic{}
		if !errors.As(err, &diagnostic) || diagnostic.Code != "SPL_UNSUPPORTED_SEARCH_DIRECTIVE" {
			t.Fatalf("Parse(%q) error = %#v, want SPL_UNSUPPORTED_SEARCH_DIRECTIVE", source, err)
		}
	}
}

func TestParseRejectsInlineSearchTimeModifiersPrecisely(t *testing.T) {
	t.Parallel()
	// These names alter the search time range and must not be compared as event fields:
	// https://help.splunk.com/en/splunk-enterprise/search/spl-search-reference/9.3/time-format-variables-and-modifiers/time-modifiers
	for _, modifier := range []string{"earliest", "latest", "starttime", "endtime", "timeformat", "_index_earliest", "_index_latest"} {
		source := `index=main ` + modifier + `="09/14/2026:00:00:00"`
		_, err := Parse(source)
		diagnostic := &Diagnostic{}
		if !errors.As(err, &diagnostic) || diagnostic.Code != "SPL_UNSUPPORTED_SEARCH_TIME_MODIFIER" {
			t.Fatalf("Parse(%q) error = %#v, want SPL_UNSUPPORTED_SEARCH_TIME_MODIFIER", source, err)
		}
	}
}

func TestParseQuotedSearchMembership(t *testing.T) {
	t.Parallel()
	for _, source := range []string{`"my field" IN (10)`, `NOT "my field" IN (10)`, `index=main | search "my field" IN (10)`, `index=main | search NOT "my field" IN (10)`} {
		t.Run(source, func(t *testing.T) {
			query, err := Parse(source)
			if err != nil {
				t.Fatal(err)
			}
			expression := query.Search
			if len(query.Commands) != 0 {
				expression = query.Commands[0].(*SearchCommand).Expression
			}
			if negation, ok := expression.(*NotExpr); ok {
				expression = negation.Operand
			}
			comparison, ok := expression.(*ComparisonExpr)
			if !ok || comparison.Field != "my field" || comparison.Value.Text != "10" {
				t.Fatalf("membership = %#v", expression)
			}
		})
	}
}
