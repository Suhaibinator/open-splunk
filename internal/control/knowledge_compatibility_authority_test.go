package control

import (
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/testsupport/knowledgecompat"
)

func TestCompatibilityV0_1RecoveryAuthorities(t *testing.T) {
	knowledgecompat.Run(t, knowledgecompat.OwnerControl, map[knowledgecompat.Vector]knowledgecompat.Assertion{
		"recovery-capacity": knowledgecompat.Exact([]knowledgecompat.WantCase{
			{ID: "recovery.quarantine-corrupt-active-at-cap", Stage: "recovery", Expect: "atomically-quarantined-and-audited"},
		}, func(t *testing.T) {
			t.Run("protected-normal-capacity-reserves", TestKnowledgeCatalogSchemaProtectsNormalCapacityReserves)
			t.Run("recovery-receipt-authority", TestKnowledgeWriterIdempotencyLinksQuarantineToRecoveryReserve)
		}),
		"recovery-cascade": knowledgecompat.Exact([]knowledgecompat.WantCase{
			{ID: "recovery.quarantine-dependent-cascade", Stage: "recovery", Expect: "bounded-terminal-cascade-restores-admission"},
		}, func(t *testing.T) {
			t.Run("dependent-first-closure", TestKnowledgeCatalogDependencyTargetsRequireDependentFirstCascade)
			t.Run("active-dependency-schema", TestKnowledgeActivePublicationDependenciesMigrationKeepsStateOnlyEdgesExact)
		}),
		"recovery-generations": knowledgecompat.Exact([]knowledgecompat.WantCase{
			{ID: "recovery.two-generation-audit-reserve", Stage: "recovery", Expect: "second-generation-quarantine-succeeds"},
		}, func(t *testing.T) {
			t.Run("lifetime-recovery-identity", TestKnowledgeCatalogSchemaEnforcesImmutablePublicationAndRecovery)
			t.Run("terminal-transition-matrix", TestKnowledgeObjectVersionTransitionTriggerAcceptsValidFreshMatrix)
			t.Run("recovery-receipt-link", TestKnowledgeWriterIdempotencyLinksQuarantineToRecoveryReserve)
		}),
	})
}
