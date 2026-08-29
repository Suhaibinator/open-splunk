package plan

import (
	"slices"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/spl"
)

func TestBuildSortPropagatesOfficialValueModesDirectionsAndLimit(t *testing.T) {
	t.Parallel()

	logical, err := Build(
		mustParse(t, `* | sort limit=0 +auto(host), -str('Product Name'), num(bytes), ip(client_ip) desc`),
		testScope([]string{"gradethis"}, nil),
	)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	sortOp, ok := logical.Operators[len(logical.Operators)-1].(*Sort)
	if !ok {
		t.Fatalf("last operator = %T, want *Sort", logical.Operators[len(logical.Operators)-1])
	}
	if sortOp.Limit != 0 || len(sortOp.Keys) != 4 {
		t.Fatalf("sort = %#v, want unlimited four-key sort", sortOp)
	}
	want := []struct {
		name       string
		descending bool
		mode       SortValueMode
	}{
		{name: "host", descending: true, mode: SortValueModeAuto},
		{name: "Product Name", descending: false, mode: SortValueModeLexical},
		{name: "bytes", descending: true, mode: SortValueModeNumeric},
		{name: "client_ip", descending: true, mode: SortValueModeIP},
	}
	for index, expected := range want {
		got := sortOp.Keys[index]
		if got.Field.Name != expected.name || got.Descending != expected.descending || got.Mode != expected.mode {
			t.Fatalf("key %d = %#v, want %#v", index, got, expected)
		}
	}
	if !slices.Equal(sortOp.Keys[1].Field.Path, []string{"Product Name"}) {
		t.Fatalf("quoted literal path = %#v, want one exact segment", sortOp.Keys[1].Field.Path)
	}
}

func TestBuildSortResolvesQuotedTransformingAlias(t *testing.T) {
	t.Parallel()

	logical, err := Build(
		mustParse(t, `* | stats count AS "Product Name" | sort str('Product Name')`),
		testScope([]string{"gradethis"}, nil),
	)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	sortOp := logical.Operators[len(logical.Operators)-1].(*Sort)
	if len(sortOp.Keys) != 1 || sortOp.Keys[0].Field.Name != "Product Name" ||
		sortOp.Keys[0].Mode != SortValueModeLexical ||
		!slices.Equal(sortOp.Keys[0].Field.Path, []string{"Product Name"}) {
		t.Fatalf("sort = %#v", sortOp)
	}
}

func TestBuildFieldsExplicitPlusProducesInclusionProjection(t *testing.T) {
	t.Parallel()

	logical, err := Build(
		mustParse(t, `* | fields + host, source`),
		testScope([]string{"gradethis"}, nil),
	)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	project, ok := logical.Operators[len(logical.Operators)-1].(*Project)
	if !ok || project.Mode != ProjectModeInclude || len(project.Fields) != 2 ||
		project.Fields[0].Name != "host" || project.Fields[1].Name != "source" {
		t.Fatalf("project = %#v, want inclusion of host and source", logical.Operators[len(logical.Operators)-1])
	}
}

func TestBuildRejectsForgedSortValueMode(t *testing.T) {
	t.Parallel()

	query := mustParse(t, `* | sort host`)
	command := query.Commands[0].(*spl.SortCommand)
	command.Fields[0].Mode = spl.SortValueMode(255)
	_, err := Build(query, testScope([]string{"gradethis"}, nil))
	assertDiagnosticCode(t, err, "SPL_INVALID_QUERY")
}

func TestBuildSortUsesExactInnerFieldRangeForDiagnostics(t *testing.T) {
	t.Parallel()

	source := `* | sort num(\_time)`
	_, err := Build(mustParse(t, source), testScope([]string{"gradethis"}, nil))
	diagnostic, ok := err.(*Diagnostic)
	if !ok || diagnostic.Code != "SPL_INVALID_FIELD" {
		t.Fatalf("Build error = %v, want SPL_INVALID_FIELD", err)
	}
	got := source[diagnostic.Range.Start.Offset:diagnostic.Range.End.Offset]
	if got != `\_time` {
		t.Fatalf("diagnostic range = %q, want exact inner field", got)
	}
}
