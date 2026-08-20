package knowledgesnapshot

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"math"
	"strings"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

const (
	// MaximumReferenceBytes is the largest retained wire reference. The
	// semantic field limits below keep ordinary references well under this
	// defensive envelope.
	MaximumReferenceBytes = 512
	// MaximumSummaryBytes bounds one retained definition-free inventory.
	MaximumSummaryBytes = 32 << 10
	// MaximumSummaryObjects is the exact canonical prefix retained when a
	// snapshot contains more executable objects.
	MaximumSummaryObjects      = 64
	retainedProtoMessageCharge = 512
	retainedRepeatedSlotCharge = 32
)

// ValidateReference validates one present, bounded, definition-free snapshot
// identity. Absence has lifecycle meaning and must be handled by its enclosing
// record rather than represented by a nil reference here.
func ValidateReference(reference *opensplunk.KnowledgeSnapshotRef) error {
	if reference == nil {
		return fmt.Errorf("%w: knowledge snapshot reference is absent", ErrInvalidInput)
	}
	if proto.Size(reference) > MaximumReferenceBytes {
		return fmt.Errorf("%w: knowledge snapshot reference exceeds %d bytes", ErrResourceLimit, MaximumReferenceBytes)
	}
	if len(reference.GetSnapshotSha256()) != sha256.Size ||
		len(reference.GetTenantCatalogStateToken()) != sha256.Size {
		return fmt.Errorf("%w: knowledge snapshot reference commitments must be exactly %d bytes", ErrInvalidInput, sha256.Size)
	}
	if reference.GetTenantCatalogRevision() > maximumCatalogRevision {
		return fmt.Errorf("%w: knowledge snapshot reference catalog revision is invalid", ErrInvalidInput)
	}
	if reference.GetObjectCount() > MaximumExecutableObjects {
		return fmt.Errorf("%w: knowledge snapshot reference object count exceeds %d", ErrResourceLimit, MaximumExecutableObjects)
	}
	if reference.GetLookupAssetCount() > MaximumLookupAssets {
		return fmt.Errorf(
			"%w: knowledge snapshot reference lookup asset count exceeds %d",
			ErrResourceLimit,
			MaximumLookupAssets,
		)
	}
	if len(reference.ProtoReflect().GetUnknown()) != 0 {
		return fmt.Errorf("%w: knowledge snapshot reference contains unknown fields", ErrInvalidInput)
	}
	return nil
}

// CloneReference validates and detaches one retained reference.
func CloneReference(reference *opensplunk.KnowledgeSnapshotRef) (*opensplunk.KnowledgeSnapshotRef, error) {
	if err := ValidateReference(reference); err != nil {
		return nil, err
	}
	cloned, ok := proto.Clone(reference).(*opensplunk.KnowledgeSnapshotRef)
	if !ok || cloned == nil {
		return nil, fmt.Errorf("%w: knowledge snapshot reference cannot be detached", ErrInvalidInput)
	}
	return cloned, nil
}

// ValidateSummary validates one exact canonical inventory prefix. Authorized
// identity and redaction are mutually exclusive at the protobuf type level;
// this additionally rejects an absent disclosure and false redaction.
func ValidateSummary(summary *opensplunk.KnowledgeSnapshotSummary) error {
	if summary == nil {
		return fmt.Errorf("%w: knowledge snapshot summary is absent", ErrInvalidInput)
	}
	// Reject repeated-shape amplification before proto.Size walks attacker- or
	// dependency-supplied entries. A canonical summary can never retain more
	// than this fixed prefix, even when ref.object_count is larger.
	if len(summary.GetObjects()) > MaximumSummaryObjects {
		return fmt.Errorf("%w: knowledge snapshot summary prefix exceeds %d objects", ErrResourceLimit, MaximumSummaryObjects)
	}
	if len(summary.GetLookupAssets()) > MaximumLookupAssets {
		return fmt.Errorf(
			"%w: knowledge snapshot summary exceeds %d lookup assets",
			ErrResourceLimit,
			MaximumLookupAssets,
		)
	}
	if proto.Size(summary) > MaximumSummaryBytes {
		return fmt.Errorf("%w: knowledge snapshot summary exceeds %d bytes", ErrResourceLimit, MaximumSummaryBytes)
	}
	if len(summary.ProtoReflect().GetUnknown()) != 0 {
		return fmt.Errorf("%w: knowledge snapshot summary contains unknown fields", ErrInvalidInput)
	}
	if err := ValidateReference(summary.GetRef()); err != nil {
		return fmt.Errorf("knowledge snapshot summary reference: %w", err)
	}
	if len(summary.GetLookupAssets()) != int(summary.GetRef().GetLookupAssetCount()) {
		return fmt.Errorf(
			"%w: knowledge snapshot summary lookup inventory disagrees with its reference",
			ErrInvalidInput,
		)
	}
	if err := validateSnapshotLookupAssets(summary.GetLookupAssets()); err != nil {
		return fmt.Errorf("knowledge snapshot summary lookup inventory: %w", err)
	}

	objectCount := summary.GetRef().GetObjectCount()
	wantPrefix := min(int(objectCount), MaximumSummaryObjects)
	if len(summary.GetObjects()) != wantPrefix {
		return fmt.Errorf("%w: knowledge snapshot summary prefix has %d objects, want %d", ErrInvalidInput, len(summary.GetObjects()), wantPrefix)
	}
	wantTruncated := objectCount > MaximumSummaryObjects
	if summary.GetObjectsTruncated() != wantTruncated {
		return fmt.Errorf("%w: knowledge snapshot summary truncation marker disagrees with object count", ErrInvalidInput)
	}

	var previousStageRank uint8
	for position, object := range summary.GetObjects() {
		if object == nil {
			return fmt.Errorf("%w: knowledge snapshot summary object %d is nil", ErrInvalidInput, position)
		}
		if len(object.ProtoReflect().GetUnknown()) != 0 {
			return fmt.Errorf("%w: knowledge snapshot summary object %d contains unknown fields", ErrInvalidInput, position)
		}
		if object.GetResolutionOrdinal() != uint32(position) {
			return fmt.Errorf("%w: knowledge snapshot summary object %d has noncanonical ordinal", ErrInvalidInput, position)
		}
		stage, stageRank, err := stageForObjectType(object.GetObjectType())
		if err != nil || object.GetStage() != stage {
			return fmt.Errorf("%w: knowledge snapshot summary object %d has incoherent type and stage", ErrInvalidInput, position)
		}
		if position > 0 && stageRank < previousStageRank {
			return fmt.Errorf("%w: knowledge snapshot summary stages are not canonical", ErrInvalidInput)
		}
		previousStageRank = stageRank

		switch disclosure := object.GetDisclosure().(type) {
		case *opensplunk.KnowledgeSnapshotObjectSummary_AuthorizedObject:
			if err := validateAuthorizedObjectSummary(disclosure.AuthorizedObject); err != nil {
				return fmt.Errorf("knowledge snapshot summary object %d: %w", position, err)
			}
		case *opensplunk.KnowledgeSnapshotObjectSummary_Redacted:
			if !disclosure.Redacted {
				return fmt.Errorf("%w: knowledge snapshot summary object %d has false redaction", ErrInvalidInput, position)
			}
		default:
			return fmt.Errorf("%w: knowledge snapshot summary object %d disclosure is absent", ErrInvalidInput, position)
		}
	}
	return nil
}

// CloneSummary validates and detaches one retained inventory.
func CloneSummary(summary *opensplunk.KnowledgeSnapshotSummary) (*opensplunk.KnowledgeSnapshotSummary, error) {
	if err := ValidateSummary(summary); err != nil {
		return nil, err
	}
	cloned, ok := proto.Clone(summary).(*opensplunk.KnowledgeSnapshotSummary)
	if !ok || cloned == nil {
		return nil, fmt.Errorf("%w: knowledge snapshot summary cannot be detached", ErrInvalidInput)
	}
	return cloned, nil
}

func validateAuthorizedObjectSummary(summary *opensplunk.KnowledgeSnapshotAuthorizedObjectSummary) error {
	if summary == nil {
		return fmt.Errorf("%w: authorized object identity is absent", ErrInvalidInput)
	}
	if len(summary.ProtoReflect().GetUnknown()) != 0 {
		return fmt.Errorf("%w: authorized object identity contains unknown fields", ErrInvalidInput)
	}
	if !validIdentity(summary.GetKnowledgeObjectId(), maximumObjectIDBytes) ||
		summary.GetVersion() == 0 || summary.GetVersion() > math.MaxInt64 ||
		!validIdentity(summary.GetName(), maximumIdentityBytes) {
		return fmt.Errorf("%w: authorized object identity is not canonical", ErrInvalidInput)
	}
	return nil
}

// Reference returns a detached, definition-free identity minted only from the
// finalized immutable authority. A zero Snapshot returns nil.
func (snapshot Snapshot) Reference() *opensplunk.KnowledgeSnapshotRef {
	if snapshot.message == nil {
		return nil
	}
	return &opensplunk.KnowledgeSnapshotRef{
		SnapshotSha256:          bytes.Clone(snapshot.message.GetSnapshotSha256()),
		TenantCatalogRevision:   snapshot.message.GetTenantCatalogRevision(),
		TenantCatalogStateToken: bytes.Clone(snapshot.message.GetTenantCatalogStateToken()),
		ObjectCount:             uint32(len(snapshot.message.GetObjects())),      // #nosec G115 -- finalized snapshots are bounded by MaximumObjects.
		LookupAssetCount:        uint32(len(snapshot.message.GetLookupAssets())), // #nosec G115 -- finalized lookup assets are bounded by sixteen.
	}
}

// Summary returns a detached, definition-free canonical object prefix minted
// only from the finalized immutable authority. Object identities are retained
// as authorized disclosures; response projections may replace them with the
// redacted oneof without changing position, type, or stage.
func (snapshot Snapshot) Summary() *opensplunk.KnowledgeSnapshotSummary {
	if snapshot.message == nil {
		return nil
	}
	objectCount := len(snapshot.message.GetObjects())
	prefixCount := min(objectCount, MaximumSummaryObjects)
	objects := make([]*opensplunk.KnowledgeSnapshotObjectSummary, prefixCount)
	for position, object := range snapshot.message.GetObjects()[:prefixCount] {
		objects[position] = &opensplunk.KnowledgeSnapshotObjectSummary{
			ResolutionOrdinal: object.GetResolutionOrdinal(),
			ObjectType:        object.GetObjectType(),
			Stage:             object.GetStage(),
			Disclosure: &opensplunk.KnowledgeSnapshotObjectSummary_AuthorizedObject{
				AuthorizedObject: &opensplunk.KnowledgeSnapshotAuthorizedObjectSummary{
					KnowledgeObjectId: strings.Clone(object.GetKnowledgeObjectId()),
					Version:           object.GetVersion(),
					Name:              strings.Clone(object.GetName()),
				},
			},
		}
	}
	return &opensplunk.KnowledgeSnapshotSummary{
		Ref:              snapshot.Reference(),
		Objects:          objects,
		ObjectsTruncated: objectCount > MaximumSummaryObjects,
		LookupAssets:     cloneSnapshotLookupAssets(snapshot.message.GetLookupAssets()),
	}
}

// Equal reports exact finalized authority equality. The digest is a fast
// rejection path; canonical bytes remain the final equality authority.
func (snapshot Snapshot) Equal(other Snapshot) bool {
	if snapshot.message == nil || other.message == nil {
		return snapshot.message == nil && other.message == nil
	}
	return snapshot.digest == other.digest && bytes.Equal(snapshot.encoded, other.encoded) &&
		snapshot.prelude.Equal(other.prelude)
}

// RetainedBytes returns a deliberately conservative heap charge for the
// immutable protobuf graph plus its duplicate deterministic encoding. The
// graph estimate charges every message and repeated slot well above their Go
// headers in addition to all variable payloads.
func (snapshot Snapshot) RetainedBytes() uint64 {
	if snapshot.message == nil {
		return 0
	}
	return uint64(cap(snapshot.encoded)) + retainedMessageBytes(snapshot.message.ProtoReflect()) +
		snapshot.prelude.RetainedBytes()
}

func retainedMessageBytes(message protoreflect.Message) uint64 {
	if !message.IsValid() {
		return 0
	}
	total := uint64(retainedProtoMessageCharge + len(message.GetUnknown()))
	message.Range(func(field protoreflect.FieldDescriptor, value protoreflect.Value) bool {
		switch {
		case field.IsMap():
			mapping := value.Map()
			mappingLength := uint64(mapping.Len()) // #nosec G115 -- protobuf container lengths are nonnegative.
			total += retainedRepeatedSlotCharge * 2 * mappingLength
			mapping.Range(func(key protoreflect.MapKey, element protoreflect.Value) bool {
				total += retainedValueBytes(field.MapKey().Kind(), key.Value())
				total += retainedValueBytes(field.MapValue().Kind(), element)
				return true
			})
		case field.IsList():
			list := value.List()
			listLength := uint64(list.Len()) // #nosec G115 -- protobuf container lengths are nonnegative.
			total += retainedRepeatedSlotCharge * listLength
			for index := 0; index < list.Len(); index++ {
				total += retainedValueBytes(field.Kind(), list.Get(index))
			}
		default:
			total += retainedValueBytes(field.Kind(), value)
		}
		return true
	})
	return total
}

func retainedValueBytes(kind protoreflect.Kind, value protoreflect.Value) uint64 {
	switch kind {
	case protoreflect.StringKind:
		return uint64(len(value.String()))
	case protoreflect.BytesKind:
		return uint64(cap(value.Bytes()))
	case protoreflect.MessageKind, protoreflect.GroupKind:
		return retainedMessageBytes(value.Message())
	default:
		return 0
	}
}
