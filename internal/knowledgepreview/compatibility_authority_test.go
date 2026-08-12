package knowledgepreview

import (
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/testsupport/knowledgecompat"
)

func TestCompatibilityV0_1PreviewAuthorities(t *testing.T) {
	knowledgecompat.Run(t, knowledgecompat.OwnerKnowledgePreview, map[knowledgecompat.Vector]knowledgecompat.Assertion{
		"retained-scope": knowledgecompat.Exact([]knowledgecompat.WantCase{
			{ID: "authorization.preview-retained-scope", Stage: "preview", Expect: "retained-boundaries"},
		}, func(t *testing.T) {
			t.Run("production-adapter", TestProductionPreviewAdapterUsesRetainedScopeAndBoundedPairedResults)
			t.Run("unauthorized-mismatch", TestPreviewUnauthorizedAndMismatchedJobsFailBeforeValidation)
		}),
	})
}
