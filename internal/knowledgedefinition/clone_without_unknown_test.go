package knowledgedefinition

import (
	"bytes"
	"testing"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"google.golang.org/protobuf/encoding/protowire"
)

func TestCloneWithoutTopLevelUnknownDoesNotMutateOrAliasSource(t *testing.T) {
	t.Parallel()
	source := &opensplunkv1.KnowledgeObjectDefinition{AppId: "source-app"}
	unknown := protowire.AppendBytes(
		protowire.AppendTag(nil, 13, protowire.BytesType),
		[]byte("future-body"),
	)
	source.ProtoReflect().SetUnknown(bytes.Clone(unknown))

	cloned, ok := cloneWithoutTopLevelUnknown(source)
	if !ok || cloned == nil {
		t.Fatal("cloneWithoutTopLevelUnknown failed")
	}
	if !bytes.Equal(source.ProtoReflect().GetUnknown(), unknown) {
		t.Fatal("cloneWithoutTopLevelUnknown mutated source unknown fields")
	}
	if len(cloned.ProtoReflect().GetUnknown()) != 0 {
		t.Fatal("cloneWithoutTopLevelUnknown retained top-level unknown fields")
	}
	cloned.AppId = "cloned-app"
	if source.GetAppId() != "source-app" {
		t.Fatal("cloneWithoutTopLevelUnknown aliased source known fields")
	}
}
