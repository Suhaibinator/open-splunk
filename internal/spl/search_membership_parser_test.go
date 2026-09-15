package spl

import (
	"strings"
	"testing"
)

func TestParseSearchMembership(t *testing.T) {
	t.Parallel()
	source := `index="gradethis" build_id="v1.1.3" (level IN ("WARN")) message="Request summary statistics"`
	query, err := Parse(source)
	if err != nil {
		t.Fatal(err)
	}
	root := query.Search.(*BinaryExpr)
	assertComparison(t, root.Right, "message", "Request summary statistics", true)
	left := root.Left.(*BinaryExpr)
	assertComparison(t, left.Right, "level", "WARN", true)
	assertSourceRangeText(t, source, left.Right.SourceRange(), `(level IN ("WARN"))`)

	query, err = Parse(`NOT level IN (WARN, ERROR) OR status=500 index=gradethis`)
	if err != nil {
		t.Fatal(err)
	}
	and := query.Search.(*BinaryExpr)
	if and.Op != BoolOpAnd {
		t.Fatalf("root = %#v", and)
	}
	or := and.Left.(*BinaryExpr)
	if or.Op != BoolOpOr {
		t.Fatalf("left = %#v", or)
	}
	membership := or.Left.(*NotExpr).Operand.(*BinaryExpr)
	if membership.Op != BoolOpOr {
		t.Fatalf("membership = %#v", membership)
	}
	assertComparison(t, membership.Left, "level", "WARN", false)
	assertComparison(t, membership.Right, "level", "ERROR", false)
}

func TestParseSearchMembershipRejectsMalformedLists(t *testing.T) {
	t.Parallel()
	for _, source := range []string{
		`level IN ()`, `level IN (,WARN)`, `level IN (WARN,)`,
		`level IN (WARN,,ERROR)`, `level IN (WARN ERROR)`,
		`level IN (WARN`, `level IN (WARN | head 1`,
		`level IN (lower("WARN"))`,
	} {
		t.Run(source, func(t *testing.T) {
			if query, err := Parse(source); err == nil || query != nil {
				t.Fatalf("Parse(%q) = %#v, %v", source, query, err)
			}
		})
	}
}

func TestParseSearchMembershipBudgets(t *testing.T) {
	t.Parallel()
	list := strings.Repeat(`WARN,`, MaximumMembershipCandidates-1) + `WARN`
	term := `level IN (` + list + `) `
	if _, err := Parse(term); err != nil {
		t.Fatal(err)
	}
	assertAuthoredDiagnosticCode(t, `level IN (`+list+`,ERROR)`, "SPL_QUERY_TOO_COMPLEX")
	maximum := strings.Repeat(term, MaximumMembershipCandidatesPerQuery/MaximumMembershipCandidates)
	if _, err := Parse(maximum); err != nil {
		t.Fatal(err)
	}
	assertAuthoredDiagnosticCode(t, maximum+`level IN (ERROR)`, "SPL_QUERY_TOO_COMPLEX")
	assertAuthoredDiagnosticCode(t, maximum+`| where status IN (500)`, "SPL_QUERY_TOO_COMPLEX")
}
