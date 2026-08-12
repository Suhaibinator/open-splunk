package knowledge

import (
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/testsupport/knowledgecompat"
)

func TestCompatibilityV0_1KnowledgeAuthorities(t *testing.T) {
	knowledgecompat.Run(t, knowledgecompat.OwnerKnowledge, map[knowledgecompat.Vector]knowledgecompat.Assertion{
		"identity-normalization": knowledgecompat.Exact([]knowledgecompat.WantCase{
			{ID: "identity.binary-case-sensitive", Stage: "publication", Expect: "accepted"},
			{ID: "identity.ascii-stable-trim", Stage: "publication", Expect: "stable-normalized-bytes"},
		}, func(t *testing.T) {
			t.Run("normalization", TestNormalizeNamePinsASCIIWhitespaceControlsAndBinaryCase)
		}),
		"selector-match": knowledgecompat.Exact([]knowledgecompat.WantCase{
			{ID: "selector.missing-metadata", Stage: "extraction", Expect: "unchanged"},
			{ID: "selector.anchored", Stage: "extraction", Expect: "complete-match-only"},
			{ID: "selector.case-sensitive", Stage: "extraction", Expect: "binary-case-sensitive"},
		}, func(t *testing.T) {
			t.Run("anchored-case-sensitive", TestSelectorGlobIsAnchoredCaseSensitiveAndUnicodeScalarAware)
			t.Run("missing-null", TestSelectorANDORAndMissingNullSemantics)
		}),
		"selector-grammar": knowledgecompat.Exact([]knowledgecompat.WantCase{
			{ID: "selector.escape", Stage: "publication", Expect: "pinned-glob-grammar"},
		}, func(t *testing.T) {
			t.Run("closed-grammar", TestNormalizePatternPinsClosedGlobGrammar)
		}),
		"selector-runtime-budget": knowledgecompat.Exact([]knowledgecompat.WantCase{
			{ID: "selector.runtime-budget", Stage: "execution", Expect: "resource-limit"},
		}, func(t *testing.T) {
			t.Run("runtime-work", TestRuntimeBudgetChargesInputsAndRejectsInvalidOrExhaustedWork)
			t.Run("query-boundaries", TestRuntimeBudgetPinsValueEventAndCumulativeQueryBoundaries)
		}),
	})
}
