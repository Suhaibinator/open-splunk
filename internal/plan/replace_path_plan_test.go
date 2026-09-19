package plan

import "testing"

func TestBuildReplacePath(t *testing.T) {
	t.Parallel()
	logical, err := Build(mustParse(t, `index=gradethis | eval route=replace(path, "/(\d+)(?=/|$)", "/:id") | stats count BY route`), testScope([]string{"gradethis"}, nil))
	if err != nil {
		t.Fatal(err)
	}
	analysis, err := Analyze(logical)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, field := range analysis.ReferencedFields {
		if field == "path" {
			found = true
		}
	}
	if !found {
		t.Fatalf("referenced fields = %v", analysis.ReferencedFields)
	}
}
