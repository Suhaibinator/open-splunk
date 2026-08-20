package queryexec

import (
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/testsupport/knowledgecompat"
)

func TestKnowledgeRuntimeAuthorities(t *testing.T) {
	knowledgecompat.Run(t, knowledgecompat.OwnerQueryExec, map[knowledgecompat.Vector]knowledgecompat.Assertion{
		"runtime-edges": knowledgecompat.Exact([]knowledgecompat.WantCase{
			{ID: "selector.cross-index-per-row", Stage: "extraction", Expect: "matching-authorized-rows-only"},
			{ID: "extraction.before-base-filter", Stage: "base-filter", Expect: "matched"},
			{ID: "extraction.no-match-preserves-destination", Stage: "extraction", Expect: "preserved"},
			{ID: "alias.preserves-source", Stage: "alias", Expect: "source-and-destination"},
			{ID: "alias.null-is-present", Stage: "alias", Expect: "null-preserved"},
			{ID: "alias.missing-source-does-not-erase", Stage: "alias", Expect: "preserved"},
			{ID: "calculated.missing-does-not-erase", Stage: "calculated", Expect: "preserved"},
			{ID: "extraction.optional-empty", Stage: "extraction", Expect: "present-empty-string"},
			{ID: "extraction.bytes-no-match", Stage: "extraction", Expect: "unchanged"},
			{ID: "extraction.json-null", Stage: "extraction", Expect: "present-null"},
			{ID: "extraction.json-bytes-no-match", Stage: "extraction", Expect: "unchanged"},
			{ID: "extraction.json-container-no-output", Stage: "extraction", Expect: "unchanged"},
			{ID: "extraction.overwrite-true", Stage: "extraction", Expect: "replaced"},
			{ID: "alias.overwrite-true", Stage: "alias", Expect: "replaced-source-copied"},
			{ID: "alias.null-source", Stage: "alias", Expect: "source-and-destination-null"},
			{ID: "calculated.present-null", Stage: "calculated", Expect: "present-null"},
		}, func(t *testing.T) {
			t.Run("parser-planner-program-compiler", TestKnowledgeCompatibilityRuntimeEdgesAreCanonical)
		}),
		"lifecycle-rotation": knowledgecompat.Exact([]knowledgecompat.WantCase{
			{ID: "snapshot.catalog-mutation-isolated", Stage: "execution", Expect: "original-version"},
		}, func(t *testing.T) {
			t.Run("writer-resolver-snapshot-compiler", TestKnowledgeLifecycleVerticalCompilerRotation)
		}),
	})
}
