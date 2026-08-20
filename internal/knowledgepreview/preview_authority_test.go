package knowledgepreview

import (
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/testsupport/knowledgecompat"
)

func TestPreviewBehaviorAuthorities(t *testing.T) {
	knowledgecompat.Run(t, knowledgecompat.OwnerKnowledgePreview, map[knowledgecompat.Vector]knowledgecompat.Assertion{
		"retained-scope": knowledgecompat.Exact([]knowledgecompat.WantCase{
			{ID: "authorization.preview-retained-scope", Stage: "preview", Expect: "retained-boundaries"},
		}, func(t *testing.T) {
			t.Run("production-adapter", TestProductionPreviewAdapterUsesRetainedScopeAndBoundedPairedResults)
			t.Run("unauthorized-mismatch", TestPreviewUnauthorizedAndMismatchedJobsFailBeforeValidation)
		}),
	})
}
