package knowledgeattemptaudit

import (
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/testsupport/knowledgecompat"
)

func TestCompatibilityV0_1AttemptAuditAuthorities(t *testing.T) {
	knowledgecompat.Run(t, knowledgecompat.OwnerKnowledgeAttemptAudit, map[knowledgecompat.Vector]knowledgecompat.Assertion{
		"scalar-redaction": knowledgecompat.Exact([]knowledgecompat.WantCase{
			{ID: "audit.redaction", Stage: "audit", Expect: "fixed-bounded-metadata"},
		}, func(t *testing.T) {
			t.Run("trusted-bounded-projection", TestAppendRejectedStoresOnlyTrustedBoundedProjection)
			t.Run("closed-privacy-shapes", TestAppendInputTaxonomyAndPrivacyShapes)
		}),
	})
}
