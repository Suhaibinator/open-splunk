package plan

import (
	"slices"
	"testing"
)

func TestBuildFieldsWildcardCarriesPatternsAndClosesKnownSchemas(t *testing.T) {
	t.Parallel()

	open, err := Build(
		mustParse(t, `index=gradethis | fields + host, error*`),
		testScope([]string{"gradethis"}, nil),
	)
	if err != nil {
		t.Fatalf("Build open schema: %v", err)
	}
	project := open.Operators[len(open.Operators)-1].(*Project)
	if project.Mode != ProjectModeInclude || len(project.Fields) != 1 ||
		project.Fields[0].Name != "host" || len(project.Patterns) != 1 ||
		project.Patterns[0].Pattern != "error*" {
		t.Fatalf("open project = %#v", project)
	}

	closed, err := Build(
		mustParse(t, `index=gradethis | table error_code, host, error_text, status | fields error*`),
		testScope([]string{"gradethis"}, nil),
	)
	if err != nil {
		t.Fatalf("Build closed schema: %v", err)
	}
	if !slices.Equal(closed.OutputFields, []string{"error_code", "error_text"}) {
		t.Fatalf("closed output fields = %v", closed.OutputFields)
	}
}

func TestBuildFieldsExcludeInternalWildcardRemovesCanonicalTime(t *testing.T) {
	t.Parallel()

	logical, err := Build(
		mustParse(t, `index=gradethis | fields - _*`),
		testScope([]string{"gradethis"}, nil),
	)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if err := ValidateTimelineEligibility(logical); err == nil {
		t.Fatal("timeline unexpectedly accepted fields - _*")
	}
}

func TestBuildFieldsBroadWildcardPreservesInternalTimeAndRaw(t *testing.T) {
	t.Parallel()

	logical, err := Build(
		mustParse(t, `index=gradethis | table _time, _raw, host | fields - *`),
		testScope([]string{"gradethis"}, nil),
	)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if !slices.Equal(logical.OutputFields, []string{"_time", "_raw"}) {
		t.Fatalf("output fields = %v, want internal fields retained", logical.OutputFields)
	}
	if err := ValidateTimelineEligibility(logical); err != nil {
		t.Fatalf("timeline rejected retained _time after fields - *: %v", err)
	}
}
