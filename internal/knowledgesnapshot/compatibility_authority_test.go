package knowledgesnapshot

import (
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/testsupport/knowledgecompat"
)

func TestCompatibilityV0_1SnapshotAuthorities(t *testing.T) {
	knowledgecompat.Run(t, knowledgecompat.OwnerKnowledgeSnapshot, map[knowledgecompat.Vector]knowledgecompat.Assertion{
		"structural-dependencies": knowledgecompat.Exact([]knowledgecompat.WantCase{
			{ID: "dependency.later-stage", Stage: "publication", Expect: "rejected-stage-inversion"},
		}, func(t *testing.T) {
			t.Run("stage-inversion", TestPrepareRejectsInvalidAuthoritiesAndStructuralBounds)
		}),
		"canonical-authority": knowledgecompat.Exact([]knowledgecompat.WantCase{
			{ID: "snapshot.database-order-independent", Stage: "admission", Expect: "equal-digest"},
		}, func(t *testing.T) {
			t.Run("order-digest-detachment", TestPrepareCanonicalAuthorityOrderChargesDigestAndDetachment)
		}),
		"aggregate-budget": knowledgecompat.Exact([]knowledgecompat.WantCase{
			{ID: "resource.aggregate-budget", Stage: "admission", Expect: "rejected-query-too-complex"},
		}, func(t *testing.T) {
			t.Run("semantic-bounds", TestPrepareEnforcesAggregateWinningSemanticBounds)
			t.Run("selector-work", TestPrepareRejectsAggregateSelectorWork)
		}),
	})
}
