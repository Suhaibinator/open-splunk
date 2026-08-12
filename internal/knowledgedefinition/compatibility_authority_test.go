package knowledgedefinition

import (
	"errors"
	"testing"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/eventfields"
	"github.com/Suhaibinator/open-splunk/internal/testsupport/knowledgecompat"
	"google.golang.org/protobuf/proto"
)

func TestCompatibilityV0_1DefinitionAuthorities(t *testing.T) {
	knowledgecompat.Run(t, knowledgecompat.OwnerKnowledgeDefinition, map[knowledgecompat.Vector]knowledgecompat.Assertion{
		"reserved-destination": knowledgecompat.Exact([]knowledgecompat.WantCase{
			{ID: "identity.reserved-destination", Stage: "publication", Expect: "rejected-reserved-field"},
		}, func(t *testing.T) {
			t.Run("body-family-contract", TestNormalizeBodyFamiliesAndStructuralBounds)
			for _, root := range append(eventfields.ReservedDynamicRootNames(), "__OS_future_private") {
				root := root
				t.Run(root, func(t *testing.T) {
					definition := proto.Clone(validAliasDefinition()).(*opensplunkv1.KnowledgeObjectDefinition)
					definition.GetFieldAlias().DestinationField = root + ".child"
					if _, err := Normalize(definition); !errors.Is(err, ErrInvalidDefinition) {
						t.Fatalf("Normalize(reserved destination %q) error = %v, want ErrInvalidDefinition", root, err)
					}
				})
			}
		}),
	})
}
