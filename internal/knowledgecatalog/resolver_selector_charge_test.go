package knowledgecatalog

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/indexname"
	"github.com/Suhaibinator/open-splunk/internal/indexread"
	"github.com/Suhaibinator/open-splunk/internal/knowledge"
)

func TestResolutionIndexPruningDoesNotAccumulateEventExecutionCharge(t *testing.T) {
	t.Parallel()

	patterns := make([]string, knowledge.MaximumSelectorPatternsPerDimension)
	for index := range patterns {
		patterns[index] = "a" + strings.Repeat("x", 57) + fmt.Sprintf("%02x", index) + "*"
	}
	selector, err := knowledge.CompileSelector(knowledge.SelectorSpec{Dimensions: []knowledge.DimensionSpec{{
		Dimension: knowledge.DimensionIndex,
		Patterns:  patterns,
	}}})
	if err != nil {
		t.Fatal(err)
	}
	if selector.Stats().WildcardWorkUnits != knowledge.MaximumSelectorWildcardWorkUnits {
		t.Fatalf("fixture wildcard work = %d, want %d", selector.Stats().WildcardWorkUnits, knowledge.MaximumSelectorWildcardWorkUnits)
	}

	indexes := make([]string, indexread.MaximumIndexesPerScope)
	for index := range indexes {
		indexes[index] = "z" + strings.Repeat("x", 251) + fmt.Sprintf("%03d", index)
		if len(indexes[index]) != indexname.MaximumBytes {
			t.Fatalf("index fixture %d length = %d", index, len(indexes[index]))
		}
	}
	program, ok := selector.RuntimeProgram(knowledge.DimensionIndex)
	if !ok {
		t.Fatal("index runtime program is absent")
	}
	perIndex, err := program.Assessment.UpperBound(indexname.MaximumBytes)
	if err != nil {
		t.Fatal(err)
	}
	cumulativeExecutionCharge := uint64(len(indexes)) * (indexname.MaximumBytes +
		knowledge.SelectorMatcherTransitionUnits*perIndex)
	if cumulativeExecutionCharge <= knowledge.MaximumSelectorRuntimeQueryUnits {
		t.Fatalf("fixture cumulative execution charge = %d, want above %d", cumulativeExecutionCharge, knowledge.MaximumSelectorRuntimeQueryUnits)
	}

	matched, err := resolutionSelectorIntersects(context.Background(), selector, indexes)
	if err != nil || matched {
		t.Fatalf("resolutionSelectorIntersects(maximum valid nonmatch) = %t, %v", matched, err)
	}
}
