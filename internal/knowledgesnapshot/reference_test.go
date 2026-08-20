package knowledgesnapshot

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"
	"math"
	"strings"
	"testing"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

func TestSnapshotReferenceSummaryEqualityRetentionAndDetachment(t *testing.T) {
	authority, err := Prepare(snapshotGoldenInput(t))
	if err != nil {
		t.Fatalf("Prepare(): %v", err)
	}
	evidence := evidenceFor(authority)
	evidence.generatedSQLBytes = 128
	snapshot, err := finalize(authority, evidence)
	if err != nil {
		t.Fatalf("finalize(): %v", err)
	}
	same, err := finalize(authority, evidence)
	if err != nil {
		t.Fatalf("finalize(same): %v", err)
	}

	reference := snapshot.Reference()
	if err := ValidateReference(reference); err != nil {
		t.Fatalf("ValidateReference(): %v", err)
	}
	if !bytes.Equal(reference.GetSnapshotSha256(), snapshot.message.GetSnapshotSha256()) ||
		!bytes.Equal(reference.GetTenantCatalogStateToken(), snapshot.message.GetTenantCatalogStateToken()) ||
		reference.GetTenantCatalogRevision() != snapshot.message.GetTenantCatalogRevision() ||
		reference.GetObjectCount() != uint32(len(snapshot.message.GetObjects())) {
		t.Fatalf("reference = %+v", reference)
	}
	if size := proto.Size(reference); size <= 0 || size > MaximumReferenceBytes {
		t.Fatalf("reference wire bytes = %d", size)
	}

	summary := snapshot.Summary()
	if err := ValidateSummary(summary); err != nil {
		t.Fatalf("ValidateSummary(): %v", err)
	}
	if size := proto.Size(summary); size <= proto.Size(reference) || size > MaximumSummaryBytes {
		t.Fatalf("summary wire bytes = %d", size)
	}
	if len(summary.GetObjects()) != len(snapshot.message.GetObjects()) || summary.GetObjectsTruncated() {
		t.Fatalf("summary inventory = %d/truncated=%t", len(summary.GetObjects()), summary.GetObjectsTruncated())
	}
	for position, object := range summary.GetObjects() {
		full := snapshot.message.GetObjects()[position]
		authorized := object.GetAuthorizedObject()
		if object.GetResolutionOrdinal() != uint32(position) ||
			object.GetObjectType() != full.GetObjectType() || object.GetStage() != full.GetStage() ||
			authorized == nil || authorized.GetKnowledgeObjectId() != full.GetKnowledgeObjectId() ||
			authorized.GetVersion() != full.GetVersion() || authorized.GetName() != full.GetName() {
			t.Fatalf("summary object %d = %+v", position, object)
		}
	}

	clonedReference, err := CloneReference(reference)
	if err != nil {
		t.Fatalf("CloneReference(): %v", err)
	}
	clonedSummary, err := CloneSummary(summary)
	if err != nil {
		t.Fatalf("CloneSummary(): %v", err)
	}
	reference.SnapshotSha256[0] ^= 0xff
	reference.TenantCatalogStateToken[0] ^= 0xff
	summary.Ref.SnapshotSha256[0] ^= 0xff
	summary.Objects[0].GetAuthorizedObject().KnowledgeObjectId = "mutated"
	clonedReference.SnapshotSha256[1] ^= 0xff
	clonedSummary.Objects[0].GetAuthorizedObject().Name = "mutated"
	if bytes.Equal(snapshot.Reference().GetSnapshotSha256(), reference.GetSnapshotSha256()) ||
		bytes.Equal(snapshot.Reference().GetSnapshotSha256(), clonedReference.GetSnapshotSha256()) ||
		snapshot.Summary().GetObjects()[0].GetAuthorizedObject().GetKnowledgeObjectId() == "mutated" ||
		snapshot.Summary().GetObjects()[0].GetAuthorizedObject().GetName() == "mutated" {
		t.Fatal("reference or summary accessor aliases finalized authority")
	}

	if !snapshot.Equal(same) || !same.Equal(snapshot) || !(Snapshot{}).Equal(Snapshot{}) ||
		snapshot.Equal(Snapshot{}) || (Snapshot{}).Equal(snapshot) {
		t.Fatal("Snapshot.Equal disagrees with exact finalized authority")
	}
	if snapshot.Prelude().IsZero() || !snapshot.Prelude().Equal(authority.Prelude()) ||
		!snapshot.Clone().Prelude().Equal(snapshot.Prelude()) {
		t.Fatal("Snapshot prelude is absent or not detached across clones")
	}
	differentEncoding := snapshot
	differentEncoding.encoded = bytes.Clone(snapshot.encoded)
	differentEncoding.encoded[len(differentEncoding.encoded)-1] ^= 0xff
	if snapshot.Equal(differentEncoding) {
		t.Fatal("Snapshot.Equal accepted changed canonical bytes under the same digest")
	}
	if retained := snapshot.RetainedBytes(); retained <= uint64(len(snapshot.encoded)+proto.Size(snapshot.message)) ||
		retained != snapshot.RetainedBytes() {
		t.Fatalf("RetainedBytes() = %d", retained)
	}
	if (Snapshot{}).Reference() != nil || (Snapshot{}).Summary() != nil ||
		(Snapshot{}).RetainedBytes() != 0 || !(Snapshot{}).Prelude().IsZero() {
		t.Fatal("zero snapshot produced retained authority")
	}
}

func TestSnapshotReferenceSummaryForEnabledEmptyAuthority(t *testing.T) {
	authority, err := Prepare(Input{
		TenantID: "tenant-a", PrincipalID: "principal-a", AppID: "app-a",
		TenantCatalogRevision:      maximumCatalogRevision,
		TenantCatalogStateToken:    bytes.Repeat([]byte{0x5a}, sha256.Size),
		EffectiveAuthorizedIndexes: []string{"main"},
	})
	if err != nil {
		t.Fatalf("Prepare(): %v", err)
	}
	snapshot, err := finalize(authority, evidenceFor(authority))
	if err != nil {
		t.Fatalf("finalize(): %v", err)
	}
	summary := snapshot.Summary()
	if summary == nil || summary.GetRef() == nil || summary.GetRef().GetObjectCount() != 0 ||
		summary.GetRef().GetLookupAssetCount() != 0 ||
		len(summary.GetObjects()) != 0 || summary.GetObjectsTruncated() {
		t.Fatalf("enabled empty summary = %+v", summary)
	}
	if err := ValidateSummary(summary); err != nil {
		t.Fatalf("ValidateSummary(empty): %v", err)
	}
}

func TestValidateKnowledgeSnapshotReferenceRejectsNoncanonicalAuthority(t *testing.T) {
	valid := validSnapshotReference()
	if err := ValidateReference(valid); err != nil {
		t.Fatalf("ValidateReference(valid): %v", err)
	}
	tests := []struct {
		name   string
		mutate func(*opensplunk.KnowledgeSnapshotRef)
		want   error
	}{
		{name: "digest short", mutate: func(value *opensplunk.KnowledgeSnapshotRef) { value.SnapshotSha256 = value.SnapshotSha256[:31] }, want: ErrInvalidInput},
		{name: "state token short", mutate: func(value *opensplunk.KnowledgeSnapshotRef) {
			value.TenantCatalogStateToken = value.TenantCatalogStateToken[:31]
		}, want: ErrInvalidInput},
		{name: "revision", mutate: func(value *opensplunk.KnowledgeSnapshotRef) { value.TenantCatalogRevision = math.MaxInt64 }, want: ErrInvalidInput},
		{name: "object count", mutate: func(value *opensplunk.KnowledgeSnapshotRef) { value.ObjectCount = MaximumExecutableObjects + 1 }, want: ErrResourceLimit},
		{name: "lookup count", mutate: func(value *opensplunk.KnowledgeSnapshotRef) {
			value.LookupAssetCount = MaximumLookupAssets + 1
		}, want: ErrResourceLimit},
		{name: "unknown", mutate: func(value *opensplunk.KnowledgeSnapshotRef) { value.ProtoReflect().SetUnknown(smallUnknownField()) }, want: ErrInvalidInput},
		{name: "wire bound", mutate: func(value *opensplunk.KnowledgeSnapshotRef) {
			value.ProtoReflect().SetUnknown(bytesUnknownField(MaximumReferenceBytes))
		}, want: ErrResourceLimit},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			candidate := proto.Clone(valid).(*opensplunk.KnowledgeSnapshotRef)
			test.mutate(candidate)
			if err := ValidateReference(candidate); !errors.Is(err, test.want) {
				t.Fatalf("ValidateReference() error = %v, want %v", err, test.want)
			}
			if clone, err := CloneReference(candidate); clone != nil || !errors.Is(err, test.want) {
				t.Fatalf("CloneReference() = (%+v, %v), want nil/%v", clone, err, test.want)
			}
		})
	}
	if err := ValidateReference(nil); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("ValidateReference(nil) error = %v", err)
	}
}

func TestValidateKnowledgeSnapshotSummaryBoundariesAndDisclosure(t *testing.T) {
	valid := validSnapshotSummary(1)
	if err := ValidateSummary(valid); err != nil {
		t.Fatalf("ValidateSummary(valid): %v", err)
	}
	redacted := proto.Clone(valid).(*opensplunk.KnowledgeSnapshotSummary)
	redacted.Objects[0].Disclosure = &opensplunk.KnowledgeSnapshotObjectSummary_Redacted{Redacted: true}
	if err := ValidateSummary(redacted); err != nil {
		t.Fatalf("ValidateSummary(redacted): %v", err)
	}

	exact := validSnapshotSummary(MaximumSummaryObjects)
	if err := ValidateSummary(exact); err != nil || exact.GetObjectsTruncated() {
		t.Fatalf("ValidateSummary(exact cap) = %v, truncated=%t", err, exact.GetObjectsTruncated())
	}
	truncated := validSnapshotSummary(MaximumSummaryObjects + 1)
	if err := ValidateSummary(truncated); err != nil || !truncated.GetObjectsTruncated() ||
		len(truncated.GetObjects()) != MaximumSummaryObjects {
		t.Fatalf("ValidateSummary(truncated) = %v, inventory=%d/%t", err, len(truncated.GetObjects()), truncated.GetObjectsTruncated())
	}
	if size := proto.Size(truncated); size > MaximumSummaryBytes {
		t.Fatalf("maximum summary wire bytes = %d", size)
	}

	tests := []struct {
		name   string
		mutate func(*opensplunk.KnowledgeSnapshotSummary)
		want   error
	}{
		{name: "reference absent", mutate: func(value *opensplunk.KnowledgeSnapshotSummary) { value.Ref = nil }, want: ErrInvalidInput},
		{name: "reference unknown", mutate: func(value *opensplunk.KnowledgeSnapshotSummary) {
			value.Ref.ProtoReflect().SetUnknown(smallUnknownField())
		}, want: ErrInvalidInput},
		{name: "prefix short", mutate: func(value *opensplunk.KnowledgeSnapshotSummary) { value.Objects = nil }, want: ErrInvalidInput},
		{name: "prefix long", mutate: func(value *opensplunk.KnowledgeSnapshotSummary) {
			value.Objects = append(value.Objects, proto.Clone(value.Objects[0]).(*opensplunk.KnowledgeSnapshotObjectSummary))
		}, want: ErrInvalidInput},
		{name: "prefix over retained bound", mutate: func(value *opensplunk.KnowledgeSnapshotSummary) {
			value.Ref.ObjectCount = MaximumSummaryObjects + 1
			value.Objects = make([]*opensplunk.KnowledgeSnapshotObjectSummary, MaximumSummaryObjects+1)
		}, want: ErrResourceLimit},
		{name: "truncation marker", mutate: func(value *opensplunk.KnowledgeSnapshotSummary) { value.ObjectsTruncated = true }, want: ErrInvalidInput},
		{name: "nil object", mutate: func(value *opensplunk.KnowledgeSnapshotSummary) { value.Objects[0] = nil }, want: ErrInvalidInput},
		{name: "ordinal", mutate: func(value *opensplunk.KnowledgeSnapshotSummary) { value.Objects[0].ResolutionOrdinal = 1 }, want: ErrInvalidInput},
		{name: "object unknown", mutate: func(value *opensplunk.KnowledgeSnapshotSummary) {
			value.Objects[0].ProtoReflect().SetUnknown(smallUnknownField())
		}, want: ErrInvalidInput},
		{name: "object type", mutate: func(value *opensplunk.KnowledgeSnapshotSummary) {
			value.Objects[0].ObjectType = opensplunk.KnowledgeObjectType_KNOWLEDGE_OBJECT_TYPE_UNSPECIFIED
		}, want: ErrInvalidInput},
		{name: "stage", mutate: func(value *opensplunk.KnowledgeSnapshotSummary) {
			value.Objects[0].Stage = opensplunk.KnowledgeSearchStage_KNOWLEDGE_SEARCH_STAGE_CALCULATED_FIELD
		}, want: ErrInvalidInput},
		{name: "stage regression", mutate: makeStageRegression, want: ErrInvalidInput},
		{name: "disclosure absent", mutate: func(value *opensplunk.KnowledgeSnapshotSummary) { value.Objects[0].Disclosure = nil }, want: ErrInvalidInput},
		{name: "authorized absent", mutate: func(value *opensplunk.KnowledgeSnapshotSummary) {
			value.Objects[0].Disclosure = &opensplunk.KnowledgeSnapshotObjectSummary_AuthorizedObject{}
		}, want: ErrInvalidInput},
		{name: "authorized unknown", mutate: func(value *opensplunk.KnowledgeSnapshotSummary) {
			value.Objects[0].GetAuthorizedObject().ProtoReflect().SetUnknown(smallUnknownField())
		}, want: ErrInvalidInput},
		{name: "object id empty", mutate: func(value *opensplunk.KnowledgeSnapshotSummary) {
			value.Objects[0].GetAuthorizedObject().KnowledgeObjectId = ""
		}, want: ErrInvalidInput},
		{name: "object id too long", mutate: func(value *opensplunk.KnowledgeSnapshotSummary) {
			value.Objects[0].GetAuthorizedObject().KnowledgeObjectId = strings.Repeat("i", maximumObjectIDBytes+1)
		}, want: ErrInvalidInput},
		{name: "version zero", mutate: func(value *opensplunk.KnowledgeSnapshotSummary) { value.Objects[0].GetAuthorizedObject().Version = 0 }, want: ErrInvalidInput},
		{name: "version too high", mutate: func(value *opensplunk.KnowledgeSnapshotSummary) {
			value.Objects[0].GetAuthorizedObject().Version = uint64(math.MaxInt64) + 1
		}, want: ErrInvalidInput},
		{name: "name empty", mutate: func(value *opensplunk.KnowledgeSnapshotSummary) { value.Objects[0].GetAuthorizedObject().Name = "" }, want: ErrInvalidInput},
		{name: "name too long", mutate: func(value *opensplunk.KnowledgeSnapshotSummary) {
			value.Objects[0].GetAuthorizedObject().Name = strings.Repeat("n", maximumIdentityBytes+1)
		}, want: ErrInvalidInput},
		{name: "redacted false", mutate: func(value *opensplunk.KnowledgeSnapshotSummary) {
			value.Objects[0].Disclosure = &opensplunk.KnowledgeSnapshotObjectSummary_Redacted{}
		}, want: ErrInvalidInput},
		{name: "summary unknown", mutate: func(value *opensplunk.KnowledgeSnapshotSummary) {
			value.ProtoReflect().SetUnknown(smallUnknownField())
		}, want: ErrInvalidInput},
		{name: "wire bound", mutate: func(value *opensplunk.KnowledgeSnapshotSummary) {
			value.ProtoReflect().SetUnknown(bytesUnknownField(MaximumSummaryBytes))
		}, want: ErrResourceLimit},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			candidate := proto.Clone(valid).(*opensplunk.KnowledgeSnapshotSummary)
			test.mutate(candidate)
			if err := ValidateSummary(candidate); !errors.Is(err, test.want) {
				t.Fatalf("ValidateSummary() error = %v, want %v", err, test.want)
			}
			if clone, err := CloneSummary(candidate); clone != nil || !errors.Is(err, test.want) {
				t.Fatalf("CloneSummary() = (%+v, %v), want nil/%v", clone, err, test.want)
			}
		})
	}
	if err := ValidateSummary(nil); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("ValidateSummary(nil) error = %v", err)
	}
}

func validSnapshotReference() *opensplunk.KnowledgeSnapshotRef {
	return &opensplunk.KnowledgeSnapshotRef{
		SnapshotSha256:          bytes.Repeat([]byte{0x11}, sha256.Size),
		TenantCatalogRevision:   maximumCatalogRevision,
		TenantCatalogStateToken: bytes.Repeat([]byte{0x22}, sha256.Size),
		ObjectCount:             1,
		LookupAssetCount:        0,
	}
}

func validSnapshotSummary(objectCount int) *opensplunk.KnowledgeSnapshotSummary {
	reference := validSnapshotReference()
	reference.ObjectCount = uint32(objectCount)
	prefixCount := min(objectCount, MaximumSummaryObjects)
	objects := make([]*opensplunk.KnowledgeSnapshotObjectSummary, prefixCount)
	for position := range objects {
		objects[position] = &opensplunk.KnowledgeSnapshotObjectSummary{
			ResolutionOrdinal: uint32(position),
			ObjectType:        opensplunk.KnowledgeObjectType_KNOWLEDGE_OBJECT_TYPE_FIELD_ALIAS,
			Stage:             opensplunk.KnowledgeSearchStage_KNOWLEDGE_SEARCH_STAGE_FIELD_ALIAS,
			Disclosure: &opensplunk.KnowledgeSnapshotObjectSummary_AuthorizedObject{
				AuthorizedObject: &opensplunk.KnowledgeSnapshotAuthorizedObjectSummary{
					KnowledgeObjectId: strings.Repeat("i", maximumObjectIDBytes-3) + fmt.Sprintf("%03d", position),
					Version:           uint64(position + 1),
					Name:              strings.Repeat("n", maximumIdentityBytes-3) + fmt.Sprintf("%03d", position),
				},
			},
		}
	}
	return &opensplunk.KnowledgeSnapshotSummary{
		Ref: reference, Objects: objects, ObjectsTruncated: objectCount > MaximumSummaryObjects,
	}
}

func makeStageRegression(value *opensplunk.KnowledgeSnapshotSummary) {
	value.Ref.ObjectCount = 2
	value.Objects = append(value.Objects, proto.Clone(value.Objects[0]).(*opensplunk.KnowledgeSnapshotObjectSummary))
	value.Objects[0].ObjectType = opensplunk.KnowledgeObjectType_KNOWLEDGE_OBJECT_TYPE_CALCULATED_FIELD
	value.Objects[0].Stage = opensplunk.KnowledgeSearchStage_KNOWLEDGE_SEARCH_STAGE_CALCULATED_FIELD
	value.Objects[1].ResolutionOrdinal = 1
	value.Objects[1].ObjectType = opensplunk.KnowledgeObjectType_KNOWLEDGE_OBJECT_TYPE_FIELD_EXTRACTION
	value.Objects[1].Stage = opensplunk.KnowledgeSearchStage_KNOWLEDGE_SEARCH_STAGE_FIELD_EXTRACTION
}

func smallUnknownField() []byte {
	result := protowire.AppendTag(nil, 100, protowire.VarintType)
	return protowire.AppendVarint(result, 1)
}

func bytesUnknownField(size int) []byte {
	result := protowire.AppendTag(nil, 100, protowire.BytesType)
	return protowire.AppendBytes(result, bytes.Repeat([]byte{0x7f}, size))
}
