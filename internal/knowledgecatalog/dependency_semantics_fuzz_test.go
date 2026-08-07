package knowledgecatalog

import (
	"slices"
	"testing"
)

func FuzzCalculatedDependencyInputFields(f *testing.F) {
	for _, seed := range []string{
		"host",
		"lower(host)",
		`if(status=200,coalesce(service,host),"fallback")`,
		"a.b",
		"",
	} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, expression string) {
		if len(expression) > 16<<10 {
			t.Skip()
		}
		first, firstErr := calculatedDependencyInputFields(expression)
		second, secondErr := calculatedDependencyInputFields(expression)
		if (firstErr == nil) != (secondErr == nil) {
			t.Fatalf("nondeterministic error: first=%v second=%v", firstErr, secondErr)
		}
		if firstErr != nil {
			return
		}
		if !slices.Equal(first, second) || !slices.IsSorted(first) {
			t.Fatalf("nondeterministic or unsorted fields: first=%v second=%v", first, second)
		}
		for index, field := range first {
			if field == "" || index > 0 && first[index-1] == field {
				t.Fatalf("invalid normalized fields: %v", first)
			}
		}
	})
}
