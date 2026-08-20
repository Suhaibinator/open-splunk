package knowledgedefinition

import (
	"errors"
	"testing"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/eventfields"
	"github.com/Suhaibinator/open-splunk/internal/testsupport/knowledgecompat"
	"google.golang.org/protobuf/proto"
)

func TestDefinitionBehaviorAuthorities(t *testing.T) {
	knowledgecompat.Run(t, knowledgecompat.OwnerKnowledgeDefinition, map[knowledgecompat.Vector]knowledgecompat.Assertion{
		"reserved-destination": knowledgecompat.Exact([]knowledgecompat.WantCase{
			{ID: "identity.reserved-destination", Stage: "publication", Expect: "rejected-reserved-field"},
		}, func(t *testing.T) {
			t.Run("body-family-contract", TestNormalizeBodyFamiliesAndStructuralBounds)
			for _, root := range append(eventfields.ReservedDynamicRootNames(), "__OS_future_private") {
				t.Run(root, func(t *testing.T) {
					definition := proto.Clone(validAliasDefinition()).(*opensplunk.KnowledgeObjectDefinition)
					definition.GetFieldAlias().DestinationField = root + ".child"
					if _, err := Normalize(definition); !errors.Is(err, ErrInvalidDefinition) {
						t.Fatalf("Normalize(reserved destination %q) error = %v, want ErrInvalidDefinition", root, err)
					}
				})
			}
		}),
	})
}
