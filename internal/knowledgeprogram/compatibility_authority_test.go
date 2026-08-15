package knowledgeprogram

import (
	"errors"
	"testing"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/testsupport/knowledgecompat"
)

func TestCompatibilityV0_1ProgramAuthorities(t *testing.T) {
	knowledgecompat.Run(t, knowledgecompat.OwnerKnowledgeProgram, map[knowledgecompat.Vector]knowledgecompat.Assertion{
		"parallel-stage": knowledgecompat.Exact([]knowledgecompat.WantCase{
			{ID: "calculated.parallel-no-chain", Stage: "publication", Expect: "rejected-same-stage-dependency"},
			{ID: "collision.possibly-overlapping-writers", Stage: "publication", Expect: "rejected-destination-collision"},
			{ID: "selector.disjoint", Stage: "publication", Expect: "accepted-proven-disjoint"},
		}, func(t *testing.T) {
			t.Run("collision-chain-disjoint", TestPrepareValidatesParallelStageSemantics)
			t.Run("calculated-chain", func(t *testing.T) {
				definitions := []*opensplunkv1.KnowledgeObjectDefinition{
					calculatedDefinition(
						"calculated-a", "calculated_a", "lower(host)",
					),
					calculatedDefinition(
						"calculated-b", "calculated_b", "upper(calculated_a)",
					),
				}
				if _, err := Compile(inputFromDefinitions(t, definitions).Objects); !errors.Is(err, ErrInvalidProgram) {
					t.Fatalf("Compile(calculated same-stage chain) error = %v, want ErrInvalidProgram", err)
				}
			})
		}),
		"dependency-sharing": knowledgecompat.Exact([]knowledgecompat.WantCase{
			{ID: "dependency.private-scope", Stage: "publication", Expect: "accepted"},
			{ID: "dependency.app-private-rejected", Stage: "publication", Expect: "rejected-forbidden-or-missing-target"},
			{ID: "dependency.global-app-rejected", Stage: "publication", Expect: "rejected-forbidden-or-missing-target"},
		}, func(t *testing.T) {
			t.Run("sharing-matrix", TestCompileEnforcesSelectorAndSharingAuthority)
		}),
	})
}
