package server

import (
	"bytes"
	"crypto/sha256"
	"testing"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	exportjobs "github.com/Suhaibinator/open-splunk/internal/export"
	"google.golang.org/protobuf/proto"
)

func TestExportJobProjectionRedactsKnowledgeIdentitiesAcrossLifecycle(t *testing.T) {
	t.Parallel()
	for _, state := range []exportjobs.State{
		exportjobs.StateQueued,
		exportjobs.StateRunning,
		exportjobs.StateCompleted,
		exportjobs.StateFailed,
		exportjobs.StateCanceled,
		exportjobs.StateExpired,
	} {
		t.Run(state.String(), func(t *testing.T) {
			input := validExportProjectionKnowledgeSummary()
			job := validListExportJob("export-knowledge", state, testNow.Add(-1))
			job.KnowledgeSnapshot = input
			projected, err := exportJobToProto(job, testNow)
			if err != nil {
				t.Fatalf("exportJobToProto(%s): %v", state, err)
			}
			got := projected.GetKnowledgeSnapshot()
			if got == nil || !proto.Equal(got.GetRef(), input.GetRef()) || len(got.GetObjects()) != 1 ||
				len(got.GetLookupAssets()) != 0 || got.GetRef().GetLookupAssetCount() != 1 {
				t.Fatalf("projected summary = %#v", got)
			}
			object := got.GetObjects()[0]
			if !object.GetRedacted() || object.GetAuthorizedObject() != nil ||
				object.GetResolutionOrdinal() != 0 ||
				object.GetObjectType() != input.GetObjects()[0].GetObjectType() ||
				object.GetStage() != input.GetObjects()[0].GetStage() {
				t.Fatalf("projected object = %#v", object)
			}
			if input.GetObjects()[0].GetAuthorizedObject().GetKnowledgeObjectId() != "object-1" {
				t.Fatal("projection mutated retained authorized identity")
			}
			if input.GetLookupAssets()[0].GetLookupId() != serverLookupLogicalID ||
				input.GetLookupAssets()[0].GetAsset().GetLookupAssetId() != serverLookupPhysicalID {
				t.Fatal("projection mutated retained lookup identity")
			}
			got.Ref.SnapshotSha256[0] ^= 0xff
			if bytes.Equal(got.GetRef().GetSnapshotSha256(), input.GetRef().GetSnapshotSha256()) {
				t.Fatal("projected reference aliases retained summary")
			}
		})
	}
}

func TestExportJobProjectionRejectsInvalidKnowledgeSummary(t *testing.T) {
	t.Parallel()
	job := validListExportJob("export-invalid-knowledge", exportjobs.StateQueued, testNow)
	job.KnowledgeSnapshot = validExportProjectionKnowledgeSummary()
	job.KnowledgeSnapshot.Ref.SnapshotSha256 = []byte("secret-invalid-digest")
	if projected, err := exportJobToProto(job, testNow); err == nil || projected != nil {
		t.Fatalf("exportJobToProto(invalid summary) = (%#v, %v)", projected, err)
	}
}

func validExportProjectionKnowledgeSummary() *opensplunk.KnowledgeSnapshotSummary {
	return &opensplunk.KnowledgeSnapshotSummary{
		Ref: &opensplunk.KnowledgeSnapshotRef{
			SnapshotSha256:          bytes.Repeat([]byte{0x31}, sha256.Size),
			TenantCatalogRevision:   8,
			TenantCatalogStateToken: bytes.Repeat([]byte{0x42}, sha256.Size),
			ObjectCount:             1,
			LookupAssetCount:        1,
		},
		Objects: []*opensplunk.KnowledgeSnapshotObjectSummary{{
			ResolutionOrdinal: 0,
			ObjectType:        opensplunk.KnowledgeObjectType_KNOWLEDGE_OBJECT_TYPE_FIELD_EXTRACTION,
			Stage:             opensplunk.KnowledgeSearchStage_KNOWLEDGE_SEARCH_STAGE_FIELD_EXTRACTION,
			Disclosure: &opensplunk.KnowledgeSnapshotObjectSummary_AuthorizedObject{
				AuthorizedObject: &opensplunk.KnowledgeSnapshotAuthorizedObjectSummary{
					KnowledgeObjectId: "object-1",
					Version:           4,
					Name:              "extract-one",
				},
			},
		}},
		LookupAssets: []*opensplunk.KnowledgeSnapshotLookupAsset{
			serverLookupSnapshotAsset(),
		},
	}
}
