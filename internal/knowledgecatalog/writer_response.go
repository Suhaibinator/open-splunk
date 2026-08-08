package knowledgecatalog

import (
	"bytes"
	"context"
	"crypto/subtle"
	"fmt"
	"math"
	"strings"
	"time"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/audit"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
	"gorm.io/gorm"
)

func (writer *Writer) replayCreate(
	ctx context.Context,
	tx *gorm.DB,
	prepared preparedMutation,
	record idempotencyRecord,
) (*opensplunkv1.CreateKnowledgeObjectResponse, error) {
	authority, err := writer.replayProjectedObject(ctx, tx, prepared, record, mutationRouteCreate)
	if err != nil {
		return nil, err
	}
	return &opensplunkv1.CreateKnowledgeObjectResponse{
		KnowledgeObject:         authority.object,
		TenantCatalogRevision:   authority.revision,
		TenantCatalogStateToken: authority.stateToken,
	}, nil
}

func (writer *Writer) replayUpdate(
	ctx context.Context,
	tx *gorm.DB,
	prepared preparedMutation,
	record idempotencyRecord,
) (*opensplunkv1.UpdateKnowledgeObjectResponse, error) {
	authority, err := writer.replayProjectedObject(ctx, tx, prepared, record, mutationRouteUpdate)
	if err != nil {
		return nil, err
	}
	return &opensplunkv1.UpdateKnowledgeObjectResponse{
		KnowledgeObject:         authority.object,
		TenantCatalogRevision:   authority.revision,
		TenantCatalogStateToken: authority.stateToken,
	}, nil
}

func (writer *Writer) replaySetState(
	ctx context.Context,
	tx *gorm.DB,
	prepared preparedMutation,
	record idempotencyRecord,
) (*opensplunkv1.SetKnowledgeObjectStateResponse, error) {
	authority, err := writer.replayProjectedObject(ctx, tx, prepared, record, mutationRouteSetState)
	if err != nil {
		return nil, err
	}
	return &opensplunkv1.SetKnowledgeObjectStateResponse{
		KnowledgeObject:         authority.object,
		TenantCatalogRevision:   authority.revision,
		TenantCatalogStateToken: authority.stateToken,
	}, nil
}

type replayObjectResponseAuthority struct {
	object     *opensplunkv1.KnowledgeObject
	revision   uint64
	stateToken []byte
}

func (writer *Writer) replayProjectedObject(
	ctx context.Context,
	tx *gorm.DB,
	prepared preparedMutation,
	record idempotencyRecord,
	expectedRoute string,
) (replayObjectResponseAuthority, error) {
	object, err := writer.replayObject(ctx, tx, prepared, record, expectedRoute)
	if err != nil {
		return replayObjectResponseAuthority{}, err
	}
	projection, err := knowledgeObjectToProto(object)
	if err != nil {
		return replayObjectResponseAuthority{}, err
	}
	return replayObjectResponseAuthority{
		object:     projection,
		revision:   uint64(record.CommittedCatalogRevision),
		stateToken: bytes.Clone(record.CommittedCatalogStateToken),
	}, nil
}

func (writer *Writer) replayDelete(
	ctx context.Context,
	tx *gorm.DB,
	prepared preparedMutation,
	record idempotencyRecord,
) (*opensplunkv1.DeleteKnowledgeObjectResponse, error) {
	authority, err := writer.readReplayAuthority(ctx, tx, prepared, record, mutationRouteDelete)
	if err != nil {
		return nil, err
	}
	if authority.version.State != StateDeleted ||
		authority.current.State != StateDeleted ||
		authority.current.CurrentVersion != authority.version.ObjectVersion {
		return nil, fmt.Errorf("%w: delete replay is not the current terminal version", ErrCorrupt)
	}
	return &opensplunkv1.DeleteKnowledgeObjectResponse{
		KnowledgeObjectId:       strings.Clone(authority.reference.GetKnowledgeObjectId()),
		DeletedVersion:          authority.reference.GetVersion(),
		TenantCatalogRevision:   uint64(record.CommittedCatalogRevision),
		TenantCatalogStateToken: bytes.Clone(record.CommittedCatalogStateToken),
	}, nil
}

func (writer *Writer) replayObject(
	ctx context.Context,
	tx *gorm.DB,
	prepared preparedMutation,
	record idempotencyRecord,
	expectedRoute string,
) (Object, error) {
	authority, err := writer.readReplayAuthority(ctx, tx, prepared, record, expectedRoute)
	if err != nil {
		return Object{}, err
	}
	var object Object
	if authority.current.CurrentVersion == authority.version.ObjectVersion {
		object, err = writer.reader.objectFromCurrentRegistry(tx.WithContext(ctx), authority.current)
	} else {
		object, err = writer.reader.objectFromHistoricalVersion(
			tx.WithContext(ctx),
			authority.current,
			authority.version,
		)
	}
	if err != nil {
		return Object{}, err
	}
	if object.KnowledgeObjectID != authority.reference.GetKnowledgeObjectId() ||
		object.Version != authority.reference.GetVersion() ||
		len(object.DefinitionSHA256) != persistedKnowledgeDefinitionDigestBytes ||
		len(authority.reference.GetDefinitionSha256()) != persistedKnowledgeDefinitionDigestBytes ||
		subtle.ConstantTimeCompare(object.DefinitionSHA256, authority.reference.GetDefinitionSha256()) != 1 {
		return Object{}, fmt.Errorf("%w: hydrated replay outcome disagrees with its receipt", ErrCorrupt)
	}
	return object, nil
}

func knowledgeObjectToProto(object Object) (*opensplunkv1.KnowledgeObject, error) {
	objectType, typeOK := objectTypeToProto(object.ObjectType)
	sharingScope, scopeOK := knowledgeSharingScopeToProto(object.SharingScope)
	state, stateOK := stateToProto(object.State)
	if !validIdentity(object.KnowledgeObjectID, maximumObjectIDBytes) ||
		!validIdentity(object.TenantID, maximumTenantIDBytes) ||
		!validIdentity(object.AppID, maximumAppIDBytes) ||
		!validIdentity(object.OwnerID, maximumOwnerIDBytes) ||
		!validIdentity(object.Name, maximumFilterBytes) ||
		object.Version == 0 || object.Version > math.MaxInt64 ||
		!typeOK || !scopeOK || !stateOK {
		return nil, fmt.Errorf("%w: knowledge response scalar is invalid", ErrCorrupt)
	}
	createdAt, createdMicro, err := objectTimestampToProto(object.CreatedAt)
	if err != nil {
		return nil, err
	}
	updatedAt, updatedMicro, err := objectTimestampToProto(object.UpdatedAt)
	if err != nil || updatedMicro < createdMicro {
		return nil, fmt.Errorf("%w: knowledge response chronology is invalid", ErrCorrupt)
	}
	disabledAt, disabledMicro, err := optionalObjectTimestampToProto(object.DisabledAt)
	if err != nil {
		return nil, err
	}
	quarantinedAt, quarantinedMicro, err := optionalObjectTimestampToProto(object.QuarantinedAt)
	if err != nil {
		return nil, err
	}
	deletedAt, deletedMicro, err := optionalObjectTimestampToProto(object.DeletedAt)
	if err != nil {
		return nil, err
	}
	if !validStateShape(stateShape{
		state:              object.State,
		definitionDigest:   object.DefinitionSHA256,
		disabledAt:         disabledMicro,
		quarantinedAt:      quarantinedMicro,
		deletedAt:          deletedMicro,
		quarantineReason:   object.QuarantineReason,
		createdAtUnixMicro: createdMicro,
		updatedAtUnixMicro: updatedMicro,
	}) {
		return nil, fmt.Errorf("%w: knowledge response lifecycle is invalid", ErrCorrupt)
	}
	if object.State == StateQuarantined {
		if object.Definition != nil || len(object.DefinitionSHA256) != 0 {
			return nil, fmt.Errorf("%w: quarantined knowledge response exposes definition bytes", ErrCorrupt)
		}
	} else if object.Definition == nil || len(object.DefinitionSHA256) != persistedKnowledgeDefinitionDigestBytes {
		return nil, fmt.Errorf("%w: knowledge response definition is absent", ErrCorrupt)
	}

	var definition *opensplunkv1.KnowledgeObjectDefinition
	if object.Definition != nil {
		cloned, ok := proto.Clone(object.Definition).(*opensplunkv1.KnowledgeObjectDefinition)
		if !ok || cloned == nil {
			return nil, fmt.Errorf("%w: knowledge response definition cannot be detached", ErrCorrupt)
		}
		definition = cloned
	}
	return &opensplunkv1.KnowledgeObject{
		KnowledgeObjectId: strings.Clone(object.KnowledgeObjectID),
		TenantId:          strings.Clone(object.TenantID),
		AppId:             strings.Clone(object.AppID),
		OwnerId:           strings.Clone(object.OwnerID),
		ObjectType:        objectType,
		Name:              strings.Clone(object.Name),
		Version:           object.Version,
		SharingScope:      sharingScope,
		State:             state,
		Definition:        definition,
		DefinitionSha256:  bytes.Clone(object.DefinitionSHA256),
		CreatedAt:         createdAt,
		UpdatedAt:         updatedAt,
		DisabledAt:        disabledAt,
		QuarantinedAt:     quarantinedAt,
		DeletedAt:         deletedAt,
		QuarantineReason:  cloneString(object.QuarantineReason),
	}, nil
}

func objectTypeToProto(value ObjectType) (opensplunkv1.KnowledgeObjectType, bool) {
	switch value {
	case ObjectTypeFieldExtraction:
		return opensplunkv1.KnowledgeObjectType_KNOWLEDGE_OBJECT_TYPE_FIELD_EXTRACTION, true
	case ObjectTypeFieldAlias:
		return opensplunkv1.KnowledgeObjectType_KNOWLEDGE_OBJECT_TYPE_FIELD_ALIAS, true
	case ObjectTypeCalculatedField:
		return opensplunkv1.KnowledgeObjectType_KNOWLEDGE_OBJECT_TYPE_CALCULATED_FIELD, true
	default:
		return opensplunkv1.KnowledgeObjectType_KNOWLEDGE_OBJECT_TYPE_UNSPECIFIED, false
	}
}

func knowledgeSharingScopeToProto(value SharingScope) (opensplunkv1.SharingScope, bool) {
	switch value {
	case SharingScopePrivate:
		return opensplunkv1.SharingScope_SHARING_SCOPE_PRIVATE, true
	case SharingScopeApp:
		return opensplunkv1.SharingScope_SHARING_SCOPE_APP, true
	case SharingScopeGlobal:
		return opensplunkv1.SharingScope_SHARING_SCOPE_GLOBAL, true
	default:
		return opensplunkv1.SharingScope_SHARING_SCOPE_UNSPECIFIED, false
	}
}

func objectTimestampToProto(value time.Time) (*timestamppb.Timestamp, int64, error) {
	canonical, ok := audit.CanonicalOccurrenceTime(value)
	if !ok || !canonical.Equal(value) {
		return nil, 0, fmt.Errorf("%w: knowledge response timestamp is invalid", ErrCorrupt)
	}
	result := timestamppb.New(canonical)
	if err := result.CheckValid(); err != nil {
		return nil, 0, fmt.Errorf("%w: knowledge response timestamp is invalid", ErrCorrupt)
	}
	return result, canonical.UnixMicro(), nil
}

func optionalObjectTimestampToProto(value *time.Time) (*timestamppb.Timestamp, *int64, error) {
	if value == nil {
		return nil, nil, nil
	}
	converted, unixMicro, err := objectTimestampToProto(*value)
	if err != nil {
		return nil, nil, err
	}
	return converted, &unixMicro, nil
}
