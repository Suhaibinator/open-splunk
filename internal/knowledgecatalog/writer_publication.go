package knowledgecatalog

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/audit"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/knowledge"
	"github.com/Suhaibinator/open-splunk/internal/knowledgedefinition"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
	"gorm.io/gorm"
)

type definitionAuthority struct {
	definition   *opensplunkv1.KnowledgeObjectDefinition
	bytes        []byte
	digest       []byte
	objectType   ObjectType
	appID        string
	name         string
	sharingScope SharingScope
	description  *string
	selector     *knowledge.Selector
	opaque       bool
}

type publicationDependency struct {
	targetObjectID string
	targetVersion  int64
}

type publicationPlan struct {
	route           string
	mutationKind    string
	auditAction     audit.Action
	objectID        string
	version         int64
	state           State
	definition      definitionAuthority
	dependencies    []publicationDependency
	ownerID         string
	createdAt       time.Time
	updatedAt       time.Time
	disabledAt      *time.Time
	deletedAt       *time.Time
	current         *registryRecord
	idAttempt       int
	oldCatalogState catalogState
}

type mutationTenantHealth struct {
	CatalogRevision    int64 `gorm:"column:catalog_revision"`
	IdentityCount      int64 `gorm:"column:identity_count"`
	VersionCount       int64 `gorm:"column:version_count"`
	DefinitionBytes    int64 `gorm:"column:definition_bytes"`
	IdempotencyCount   int64 `gorm:"column:idempotency_count"`
	ActiveObjectCount  int64 `gorm:"column:active_object_count"`
	RecoveryAuditCount int64 `gorm:"column:recovery_audit_count"`
	ProjectionBytes    int64 `gorm:"column:projection_bytes"`
}

const (
	maximumActiveAppCounterRows     = maximumReadableApps
	maximumActiveOwnerCounterRows   = maximumObjectsPerTenant
	maximumActiveTypeCounterRows    = 3
	maximumActiveAppTypeCounterRows = maximumActiveAppCounterRows * maximumActiveTypeCounterRows
)

const mutationDefinitionCardinalitySQL = `SELECT
	(SELECT count(*) FROM (
		SELECT 1 FROM knowledge_object_versions
		WHERE tenant_id = ? LIMIT 65537
	)) AS versions,
	(SELECT count(*) FROM (
		SELECT 1 FROM knowledge_definition_blobs
		WHERE tenant_id = ? LIMIT 65537
	)) AS definitions`

const mutationPhysicalHealthSQL = `SELECT
	(SELECT count(*) FROM (SELECT 1 FROM knowledge_objects WHERE tenant_id = ? LIMIT 8193)) AS identities,
	coalesce((SELECT sum(definition_bytes) FROM (
		SELECT definition_bytes FROM knowledge_definition_blobs WHERE tenant_id = ? LIMIT 65537
	)), 0) AS definition_bytes,
	(SELECT count(*) FROM (SELECT 1 FROM knowledge_mutation_idempotency WHERE tenant_id = ? LIMIT 20481)) AS idempotency,
	(SELECT count(*) FROM (SELECT 1 FROM knowledge_objects WHERE tenant_id = ? AND state = 'active' LIMIT 4097)) AS active,
	(SELECT count(*) FROM (SELECT 1 FROM knowledge_recovery_audit WHERE tenant_id = ? LIMIT 8193)) AS recovery,
	(SELECT count(*) FROM (
		SELECT 1 FROM knowledge_object_list_projections WHERE tenant_id = ? LIMIT 8193
	)) AS projections,
	coalesce((SELECT sum(projection_bytes) FROM (
		SELECT projection_bytes FROM knowledge_object_list_projections WHERE tenant_id = ? LIMIT 8193
	)), 0) AS projection_bytes`

type activeCounterPreflightKind uint8

const (
	activeAppCounterPreflight activeCounterPreflightKind = iota
	activeOwnerCounterPreflight
	activeTypeCounterPreflight
	activeAppTypeCounterPreflight
)

type activeCounterPreflightSpec struct {
	table                 string
	firstKeyColumn        string
	secondKeyColumn       string
	countColumn           string
	maximumRows           int
	maximumCount          int64
	maximumFirstKeyBytes  int
	maximumSecondKeyBytes int
}

type activeCounterPreflightRecord struct {
	FirstKeyBytes  int64  `gorm:"column:first_key_bytes"`
	FirstKey       string `gorm:"column:first_key"`
	SecondKeyBytes int64  `gorm:"column:second_key_bytes"`
	SecondKey      string `gorm:"column:second_key"`
	CounterValue   int64  `gorm:"column:counter_value"`
}

func (writer *Writer) create(
	ctx context.Context,
	scope WriteScope,
	request *opensplunkv1.CreateKnowledgeObjectRequest,
) (result *opensplunkv1.CreateKnowledgeObjectResponse, returnedErr error) {
	if err := validateContext(ctx); err != nil {
		return nil, err
	}
	normalizedScope, err := normalizeWriteScope(scope)
	if err != nil {
		return nil, err
	}
	actor, err := requireMutationActor(ctx)
	if err != nil {
		return nil, err
	}
	prepared, err := prepareCreateMutation(normalizedScope, actor, request)
	if err != nil {
		return nil, err
	}
	request = prepared.createRequest
	if err := writer.callHook(ctx, writerHookEvent{Boundary: writerHookPrepared, Route: mutationRouteCreate}); err != nil {
		return nil, err
	}

	tx := writer.orm.WithContext(ctx).Begin()
	if tx.Error != nil {
		return nil, writerError(ctx, "begin create", tx.Error)
	}
	defer finishWriterTransaction(tx, &returnedErr)
	idempotency, found, err := writer.readAuthorizedIdempotencyRecord(
		ctx, tx, &prepared, mutationRouteCreate,
	)
	if err != nil {
		if errors.Is(err, ErrIdempotentOutcomeRedacted) {
			return nil, err
		}
		return nil, writerError(ctx, "read create idempotency", err)
	}
	if err := writer.callHook(ctx, writerHookEvent{Boundary: writerHookIdempotencyChecked, Route: mutationRouteCreate}); err != nil {
		return nil, err
	}
	if found {
		result, err = writer.replayCreate(ctx, tx, prepared, idempotency)
		if err != nil {
			return nil, err
		}
		if err := tx.Commit().Error; err != nil {
			return nil, writerError(ctx, "commit create replay", err)
		}
		return result, nil
	}
	if request.GetInitialState() == opensplunkv1.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_ACTIVE {
		return nil, ErrActivePublicationUnavailable
	}

	health, state, err := writer.prepareMutationTenant(tx, prepared.scope.tenantID)
	if err != nil {
		return nil, err
	}
	if err := writer.callHook(ctx, writerHookEvent{Boundary: writerHookCatalogLedgersReady, Route: mutationRouteCreate}); err != nil {
		return nil, err
	}
	if health.IdempotencyCount >= normalIdempotencyCapacity || health.VersionCount >= 61440 || health.IdentityCount >= maximumObjectsPerTenant {
		return nil, control.ErrCapacityExceeded
	}
	normalized, err := normalizeMutationDefinition(request.GetDefinition())
	if err != nil {
		return nil, err
	}
	authority, err := authorityFromNormalized(normalized)
	if err != nil {
		return nil, err
	}
	if err := authorizeDefinitionApp(tx, prepared.scope, authority.appID, false); err != nil {
		return nil, err
	}
	if health.ProjectionBytes > 268435456-projectionCharge(authority) {
		return nil, control.ErrCapacityExceeded
	}
	blobExists, err := definitionBlobExistsExact(tx, prepared.scope.tenantID, authority)
	if err != nil {
		return nil, err
	}
	if !blobExists && health.DefinitionBytes > 536870912-int64(len(authority.bytes)) {
		return nil, control.ErrCapacityExceeded
	}
	if err := writer.callHook(ctx, writerHookEvent{Boundary: writerHookCapacityChecked, Route: mutationRouteCreate}); err != nil {
		return nil, err
	}

	objectID, idAttempt, err := writer.selectCreateObjectID(tx, prepared.scope.tenantID)
	if err != nil {
		return nil, err
	}
	now, err := normalizeWriterClock(writer.clock())
	if err != nil {
		return nil, err
	}
	plan := publicationPlan{
		route:           mutationRouteCreate,
		mutationKind:    "create",
		auditAction:     audit.ActionKnowledgeObjectCreate,
		objectID:        objectID,
		version:         1,
		state:           StateDraft,
		definition:      authority,
		ownerID:         prepared.scope.ownerID,
		createdAt:       now,
		updatedAt:       now,
		idAttempt:       idAttempt,
		oldCatalogState: state,
	}
	object, revision, token, err := writer.publishMutation(ctx, tx, prepared, plan, blobExists)
	if err != nil {
		return nil, err
	}
	projected, err := knowledgeObjectToProto(object)
	if err != nil {
		return nil, err
	}
	result = &opensplunkv1.CreateKnowledgeObjectResponse{
		KnowledgeObject:         projected,
		TenantCatalogRevision:   uint64(revision),
		TenantCatalogStateToken: bytes.Clone(token),
	}
	reconciled, err := writer.commitMutation(ctx, tx, prepared, plan)
	if err != nil {
		return nil, err
	}
	if reconciled.create != nil {
		result = reconciled.create
	}
	return result, nil
}

func (writer *Writer) update(
	ctx context.Context,
	scope WriteScope,
	request *opensplunkv1.UpdateKnowledgeObjectRequest,
) (result *opensplunkv1.UpdateKnowledgeObjectResponse, returnedErr error) {
	if err := validateContext(ctx); err != nil {
		return nil, err
	}
	normalizedScope, err := normalizeWriteScope(scope)
	if err != nil {
		return nil, err
	}
	actor, err := requireMutationActor(ctx)
	if err != nil {
		return nil, err
	}
	prepared, err := prepareUpdateMutation(normalizedScope, actor, request)
	if err != nil {
		return nil, err
	}
	request = prepared.updateRequest
	if err := writer.callHook(ctx, writerHookEvent{Boundary: writerHookPrepared, Route: mutationRouteUpdate}); err != nil {
		return nil, err
	}
	tx := writer.orm.WithContext(ctx).Begin()
	if tx.Error != nil {
		return nil, writerError(ctx, "begin update", tx.Error)
	}
	defer finishWriterTransaction(tx, &returnedErr)
	idempotency, found, err := writer.readAuthorizedIdempotencyRecord(
		ctx, tx, &prepared, mutationRouteUpdate,
	)
	if err != nil {
		if errors.Is(err, ErrIdempotentOutcomeRedacted) {
			return nil, err
		}
		return nil, writerError(ctx, "read update idempotency", err)
	}
	if err := writer.callHook(ctx, writerHookEvent{Boundary: writerHookIdempotencyChecked, Route: mutationRouteUpdate}); err != nil {
		return nil, err
	}
	if found {
		result, err = writer.replayUpdate(ctx, tx, prepared, idempotency)
		if err != nil {
			return nil, err
		}
		if err := tx.Commit().Error; err != nil {
			return nil, writerError(ctx, "commit update replay", err)
		}
		return result, nil
	}
	health, state, err := writer.prepareMutationTenant(tx, prepared.scope.tenantID)
	if err != nil {
		return nil, err
	}
	if err := writer.callHook(ctx, writerHookEvent{Boundary: writerHookCatalogLedgersReady, Route: mutationRouteUpdate}); err != nil {
		return nil, err
	}
	current, err := readAuthorizedMutationRegistry(tx, prepared.scope, request.GetKnowledgeObjectId())
	if err != nil {
		return nil, err
	}
	if uint64(current.CurrentVersion) != request.GetExpectedVersion() {
		return nil, control.ErrVersionConflict
	}
	if current.State == StateQuarantined || current.State == StateDeleted {
		return nil, control.ErrVersionConflict
	}
	if current.State == StateActive {
		return nil, ErrActivePublicationUnavailable
	}
	currentObject, currentVersion, currentDefinition, err := writer.hydrateAuthorizedCurrent(tx, current)
	if err != nil {
		return nil, err
	}
	patched, err := applyKnowledgeDefinitionMask(currentObject.Definition, request.GetDefinition(), prepared.updatePaths)
	if err != nil {
		return nil, err
	}
	var authority definitionAuthority
	var dependencies []publicationDependency
	if currentDefinition.opaque {
		for _, path := range prepared.updatePaths {
			if path == "field_extraction" || path == "field_alias" || path == "calculated_field" {
				return nil, invalidMutation("opaque future definition bodies cannot be edited by this server")
			}
		}
		authority, err = authorityFromOpaqueMetadataUpdate(patched, current.State, current.ObjectType)
		if err != nil {
			return nil, err
		}
		dependencies, err = dependenciesFromCurrent(tx, currentVersion)
		if err != nil {
			return nil, err
		}
	} else {
		normalized, normalizeErr := normalizeMutationDefinition(patched)
		if normalizeErr != nil {
			return nil, normalizeErr
		}
		authority, err = authorityFromNormalized(normalized)
		if err != nil {
			return nil, err
		}
	}
	if authority.objectType != current.ObjectType {
		return nil, invalidMutation("knowledge object type is immutable")
	}
	if bytes.Equal(authority.bytes, currentDefinition.bytes) {
		return nil, invalidMutation("update mask does not change the stored definition")
	}
	if err := authorizeDefinitionApp(tx, prepared.scope, authority.appID, false); err != nil {
		return nil, err
	}
	if health.IdempotencyCount >= normalIdempotencyCapacity || health.VersionCount >= 61440 || currentVersion.ObjectVersion >= math.MaxInt64 {
		return nil, control.ErrCapacityExceeded
	}
	if health.ProjectionBytes > 268435456-projectionCharge(authority) {
		return nil, control.ErrCapacityExceeded
	}
	blobExists, err := definitionBlobExistsExact(tx, prepared.scope.tenantID, authority)
	if err != nil {
		return nil, err
	}
	if !blobExists && health.DefinitionBytes > 536870912-int64(len(authority.bytes)) {
		return nil, control.ErrCapacityExceeded
	}
	if err := writer.callHook(ctx, writerHookEvent{Boundary: writerHookCapacityChecked, Route: mutationRouteUpdate, KnowledgeObjectID: current.KnowledgeObjectID, Version: uint64(current.CurrentVersion + 1)}); err != nil {
		return nil, err
	}
	now, err := nextWriterTime(writer.clock(), current.UpdatedAtUnixMicro)
	if err != nil {
		return nil, err
	}
	mutationKind := "update"
	auditAction := audit.ActionKnowledgeObjectUpdate
	if authority.appID != current.AppID || authority.sharingScope != current.SharingScope {
		mutationKind = "scope_change"
		auditAction = audit.ActionKnowledgeObjectScopeChange
	}
	plan := publicationPlan{
		route:           mutationRouteUpdate,
		mutationKind:    mutationKind,
		auditAction:     auditAction,
		objectID:        current.KnowledgeObjectID,
		version:         current.CurrentVersion + 1,
		state:           current.State,
		definition:      authority,
		dependencies:    dependencies,
		ownerID:         current.OwnerID,
		createdAt:       time.UnixMicro(current.CreatedAtUnixMicro).UTC(),
		updatedAt:       now,
		current:         &current,
		oldCatalogState: state,
	}
	if current.DisabledAtUnixMicro != nil {
		disabled := time.UnixMicro(*current.DisabledAtUnixMicro).UTC()
		plan.disabledAt = &disabled
	}
	object, revision, token, err := writer.publishMutation(ctx, tx, prepared, plan, blobExists)
	if err != nil {
		return nil, err
	}
	projected, err := knowledgeObjectToProto(object)
	if err != nil {
		return nil, err
	}
	result = &opensplunkv1.UpdateKnowledgeObjectResponse{
		KnowledgeObject:         projected,
		TenantCatalogRevision:   uint64(revision),
		TenantCatalogStateToken: bytes.Clone(token),
	}
	reconciled, err := writer.commitMutation(ctx, tx, prepared, plan)
	if err != nil {
		return nil, err
	}
	if reconciled.update != nil {
		result = reconciled.update
	}
	return result, nil
}

func (writer *Writer) setState(
	ctx context.Context,
	scope WriteScope,
	request *opensplunkv1.SetKnowledgeObjectStateRequest,
) (result *opensplunkv1.SetKnowledgeObjectStateResponse, returnedErr error) {
	if err := validateContext(ctx); err != nil {
		return nil, err
	}
	normalizedScope, err := normalizeWriteScope(scope)
	if err != nil {
		return nil, err
	}
	actor, err := requireMutationActor(ctx)
	if err != nil {
		return nil, err
	}
	prepared, err := prepareSetStateMutation(normalizedScope, actor, request)
	if err != nil {
		return nil, err
	}
	request = prepared.setStateRequest
	if err := writer.callHook(ctx, writerHookEvent{Boundary: writerHookPrepared, Route: mutationRouteSetState}); err != nil {
		return nil, err
	}
	tx := writer.orm.WithContext(ctx).Begin()
	if tx.Error != nil {
		return nil, writerError(ctx, "begin set-state", tx.Error)
	}
	defer finishWriterTransaction(tx, &returnedErr)
	idempotency, found, err := writer.readAuthorizedIdempotencyRecord(
		ctx, tx, &prepared, mutationRouteSetState,
	)
	if err != nil {
		if errors.Is(err, ErrIdempotentOutcomeRedacted) {
			return nil, err
		}
		return nil, writerError(ctx, "read set-state idempotency", err)
	}
	if err := writer.callHook(ctx, writerHookEvent{Boundary: writerHookIdempotencyChecked, Route: mutationRouteSetState}); err != nil {
		return nil, err
	}
	if found {
		result, err = writer.replaySetState(ctx, tx, prepared, idempotency)
		if err != nil {
			return nil, err
		}
		if err := tx.Commit().Error; err != nil {
			return nil, writerError(ctx, "commit set-state replay", err)
		}
		return result, nil
	}
	health, state, err := writer.prepareMutationTenant(tx, prepared.scope.tenantID)
	if err != nil {
		return nil, err
	}
	if err := writer.callHook(ctx, writerHookEvent{Boundary: writerHookCatalogLedgersReady, Route: mutationRouteSetState}); err != nil {
		return nil, err
	}
	current, err := readAuthorizedMutationRegistry(tx, prepared.scope, request.GetKnowledgeObjectId())
	if err != nil {
		return nil, err
	}
	if uint64(current.CurrentVersion) != request.GetExpectedVersion() {
		return nil, control.ErrVersionConflict
	}
	if request.GetState() == opensplunkv1.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_ACTIVE {
		if current.State == StateActive {
			return nil, invalidMutation("state transition is a no-op")
		}
		if current.State != StateDraft && current.State != StateDisabled {
			return nil, control.ErrVersionConflict
		}
		return nil, ErrActivePublicationUnavailable
	}
	if current.State == StateDisabled {
		return nil, invalidMutation("state transition is a no-op")
	}
	if current.State != StateDraft && current.State != StateActive {
		return nil, control.ErrVersionConflict
	}
	_, currentVersion, authority, err := writer.hydrateAuthorizedCurrentStateOnly(tx, current)
	if err != nil {
		return nil, err
	}
	if current.State == StateActive {
		if blocked, err := hasActiveDependents(tx, current.TenantID, current.KnowledgeObjectID); err != nil {
			return nil, writerError(ctx, "check disable dependents", err)
		} else if blocked {
			return nil, control.ErrDependencyConflict
		}
	}
	if health.IdempotencyCount >= normalIdempotencyCapacity || health.VersionCount >= 61440 || currentVersion.ObjectVersion >= math.MaxInt64 {
		return nil, control.ErrCapacityExceeded
	}
	if health.ProjectionBytes > 268435456-projectionCharge(authority) {
		return nil, control.ErrCapacityExceeded
	}
	dependencies, err := dependenciesFromCurrent(tx, currentVersion)
	if err != nil {
		return nil, err
	}
	if err := writer.callHook(ctx, writerHookEvent{Boundary: writerHookCapacityChecked, Route: mutationRouteSetState, KnowledgeObjectID: current.KnowledgeObjectID, Version: uint64(current.CurrentVersion + 1)}); err != nil {
		return nil, err
	}
	now, err := nextWriterTime(writer.clock(), current.UpdatedAtUnixMicro)
	if err != nil {
		return nil, err
	}
	plan := publicationPlan{
		route:           mutationRouteSetState,
		mutationKind:    "disable",
		auditAction:     audit.ActionKnowledgeObjectDisable,
		objectID:        current.KnowledgeObjectID,
		version:         current.CurrentVersion + 1,
		state:           StateDisabled,
		definition:      authority,
		dependencies:    dependencies,
		ownerID:         current.OwnerID,
		createdAt:       time.UnixMicro(current.CreatedAtUnixMicro).UTC(),
		updatedAt:       now,
		disabledAt:      &now,
		current:         &current,
		oldCatalogState: state,
	}
	object, revision, token, err := writer.publishMutation(ctx, tx, prepared, plan, true)
	if err != nil {
		return nil, err
	}
	projected, err := knowledgeObjectToProto(object)
	if err != nil {
		return nil, err
	}
	result = &opensplunkv1.SetKnowledgeObjectStateResponse{
		KnowledgeObject:         projected,
		TenantCatalogRevision:   uint64(revision),
		TenantCatalogStateToken: bytes.Clone(token),
	}
	reconciled, err := writer.commitMutation(ctx, tx, prepared, plan)
	if err != nil {
		return nil, err
	}
	if reconciled.setState != nil {
		result = reconciled.setState
	}
	return result, nil
}

func (writer *Writer) delete(
	ctx context.Context,
	scope WriteScope,
	request *opensplunkv1.DeleteKnowledgeObjectRequest,
) (result *opensplunkv1.DeleteKnowledgeObjectResponse, returnedErr error) {
	if err := validateContext(ctx); err != nil {
		return nil, err
	}
	normalizedScope, err := normalizeWriteScope(scope)
	if err != nil {
		return nil, err
	}
	actor, err := requireMutationActor(ctx)
	if err != nil {
		return nil, err
	}
	prepared, err := prepareDeleteMutation(normalizedScope, actor, request)
	if err != nil {
		return nil, err
	}
	request = prepared.deleteRequest
	if err := writer.callHook(ctx, writerHookEvent{Boundary: writerHookPrepared, Route: mutationRouteDelete}); err != nil {
		return nil, err
	}
	tx := writer.orm.WithContext(ctx).Begin()
	if tx.Error != nil {
		return nil, writerError(ctx, "begin delete", tx.Error)
	}
	defer finishWriterTransaction(tx, &returnedErr)
	idempotency, found, err := writer.readAuthorizedIdempotencyRecord(
		ctx, tx, &prepared, mutationRouteDelete,
	)
	if err != nil {
		if errors.Is(err, ErrIdempotentOutcomeRedacted) {
			return nil, err
		}
		return nil, writerError(ctx, "read delete idempotency", err)
	}
	if err := writer.callHook(ctx, writerHookEvent{Boundary: writerHookIdempotencyChecked, Route: mutationRouteDelete}); err != nil {
		return nil, err
	}
	if found {
		result, err = writer.replayDelete(ctx, tx, prepared, idempotency)
		if err != nil {
			return nil, err
		}
		if err := tx.Commit().Error; err != nil {
			return nil, writerError(ctx, "commit delete replay", err)
		}
		return result, nil
	}
	health, state, err := writer.prepareMutationTenant(tx, prepared.scope.tenantID)
	if err != nil {
		return nil, err
	}
	if err := writer.callHook(ctx, writerHookEvent{Boundary: writerHookCatalogLedgersReady, Route: mutationRouteDelete}); err != nil {
		return nil, err
	}
	current, err := readAuthorizedMutationRegistry(tx, prepared.scope, request.GetKnowledgeObjectId())
	if err != nil {
		return nil, err
	}
	if uint64(current.CurrentVersion) != request.GetExpectedVersion() {
		return nil, control.ErrVersionConflict
	}
	if current.State == StateQuarantined || current.State == StateDeleted {
		return nil, control.ErrVersionConflict
	}
	_, currentVersion, authority, err := writer.hydrateAuthorizedCurrentStateOnly(tx, current)
	if err != nil {
		return nil, err
	}
	if current.State == StateActive {
		if blocked, err := hasActiveDependents(tx, current.TenantID, current.KnowledgeObjectID); err != nil {
			return nil, writerError(ctx, "check delete dependents", err)
		} else if blocked {
			return nil, control.ErrDependencyConflict
		}
	}
	if health.IdempotencyCount >= normalIdempotencyCapacity || health.VersionCount >= 61440 || currentVersion.ObjectVersion >= math.MaxInt64 {
		return nil, control.ErrCapacityExceeded
	}
	if health.ProjectionBytes > 268435456-projectionCharge(authority) {
		return nil, control.ErrCapacityExceeded
	}
	dependencies, err := dependenciesFromCurrent(tx, currentVersion)
	if err != nil {
		return nil, err
	}
	if err := writer.callHook(ctx, writerHookEvent{Boundary: writerHookCapacityChecked, Route: mutationRouteDelete, KnowledgeObjectID: current.KnowledgeObjectID, Version: uint64(current.CurrentVersion + 1)}); err != nil {
		return nil, err
	}
	now, err := nextWriterTime(writer.clock(), current.UpdatedAtUnixMicro)
	if err != nil {
		return nil, err
	}
	plan := publicationPlan{
		route:           mutationRouteDelete,
		mutationKind:    "delete",
		auditAction:     audit.ActionKnowledgeObjectDelete,
		objectID:        current.KnowledgeObjectID,
		version:         current.CurrentVersion + 1,
		state:           StateDeleted,
		definition:      authority,
		dependencies:    dependencies,
		ownerID:         current.OwnerID,
		createdAt:       time.UnixMicro(current.CreatedAtUnixMicro).UTC(),
		updatedAt:       now,
		deletedAt:       &now,
		current:         &current,
		oldCatalogState: state,
	}
	_, revision, token, err := writer.publishMutation(ctx, tx, prepared, plan, true)
	if err != nil {
		return nil, err
	}
	result = &opensplunkv1.DeleteKnowledgeObjectResponse{
		KnowledgeObjectId:       strings.Clone(plan.objectID),
		DeletedVersion:          uint64(plan.version),
		TenantCatalogRevision:   uint64(revision),
		TenantCatalogStateToken: bytes.Clone(token),
	}
	reconciled, err := writer.commitMutation(ctx, tx, prepared, plan)
	if err != nil {
		return nil, err
	}
	if reconciled.delete != nil {
		result = reconciled.delete
	}
	return result, nil
}

func (writer *Writer) prepareMutationTenant(
	tx *gorm.DB,
	tenantID string,
) (mutationTenantHealth, catalogState, error) {
	if err := ensureMutationLedgers(tx, tenantID); err != nil {
		return mutationTenantHealth{}, catalogState{}, writerError(tx.Statement.Context, "ensure catalog ledgers", err)
	}
	health, state, err := readMutationTenantHealth(tx, tenantID)
	if err != nil {
		return mutationTenantHealth{}, catalogState{}, writerError(tx.Statement.Context, "validate catalog mutation health", err)
	}
	if health.IdempotencyCount >= normalIdempotencyCapacity {
		reclaimCount := health.IdempotencyCount - normalIdempotencyCapacity + 1
		if err := reclaimExpiredMutationReceipts(tx, tenantID, reclaimCount); err != nil {
			return mutationTenantHealth{}, catalogState{}, writerError(
				tx.Statement.Context,
				"reclaim expired knowledge mutation receipts",
				err,
			)
		}
		health, state, err = readMutationTenantHealth(tx, tenantID)
		if err != nil {
			return mutationTenantHealth{}, catalogState{}, writerError(
				tx.Statement.Context,
				"validate catalog mutation health after receipt reclamation",
				err,
			)
		}
	}
	return health, state, nil
}

func reclaimExpiredMutationReceipts(tx *gorm.DB, tenantID string, limit int64) error {
	if tx == nil || !validIdentity(tenantID, maximumTenantIDBytes) ||
		limit < 1 || limit > maximumIdempotencyReclaim {
		return fmt.Errorf("%w: knowledge receipt reclamation authority is invalid", control.ErrInvalidArgument)
	}
	var cutoff int64
	if err := tx.Raw(`SELECT CAST(unixepoch('subsec') * 1000000 AS INTEGER)`).Scan(&cutoff).Error; err != nil {
		return err
	}
	if _, err := canonicalTime(cutoff); err != nil {
		return fmt.Errorf("%w: knowledge receipt reclamation cutoff is invalid", ErrCorrupt)
	}
	if err := preflightExpiredMutationReceiptReclaim(tx, tenantID, limit, cutoff); err != nil {
		return err
	}
	return tx.Exec(mutationReceiptReclaimDeleteSQL, tenantID, cutoff, limit).Error
}

const mutationReceiptReclaimDeleteSQL = `DELETE FROM knowledge_mutation_idempotency
		WHERE (
			tenant_id, actor_kind, actor_id, route, client_request_id
		) IN (
			SELECT tenant_id, actor_kind, actor_id, route, client_request_id
			FROM knowledge_mutation_idempotency
			WHERE tenant_id = ?
			  AND retain_until_unix_micro <= ?
			ORDER BY retain_until_unix_micro,
			         created_at_unix_micro,
			         actor_kind,
			         actor_id,
			         route,
			         client_request_id
			LIMIT ?
		)`

type mutationReceiptReclaimWidth struct {
	TenantIDBytes      int64 `gorm:"column:tenant_id_bytes"`
	ActorKindBytes     int64 `gorm:"column:actor_kind_bytes"`
	ActorIDBytes       int64 `gorm:"column:actor_id_bytes"`
	RouteBytes         int64 `gorm:"column:route_bytes"`
	RequestIDBytes     int64 `gorm:"column:request_id_bytes"`
	MutationKindBytes  int64 `gorm:"column:mutation_kind_bytes"`
	RequestDigestBytes int64 `gorm:"column:request_digest_bytes"`
	OutcomeProtoBytes  int64 `gorm:"column:outcome_proto_bytes"`
	StateTokenBytes    int64 `gorm:"column:state_token_bytes"`
	ObjectIDBytes      int64 `gorm:"column:object_id_bytes"`
}

type mutationReceiptReclaimRecord struct {
	Record                         idempotencyRecord      `gorm:"embedded"`
	CommitAuthorityMatches         int64                  `gorm:"column:commit_authority_matches"`
	AuditAuthorityMatches          int64                  `gorm:"column:audit_authority_matches"`
	ImmutableVersion               versionRecord          `gorm:"embedded;embeddedPrefix:immutable_"`
	ImmutableDefinitionDigestBytes int64                  `gorm:"column:immutable_definition_digest_bytes"`
	ImmutableQuarantineReasonBytes int64                  `gorm:"column:immutable_quarantine_reason_bytes"`
	Lifecycle                      versionLifecycleRecord `gorm:"embedded;embeddedPrefix:lifecycle_"`
	LifecycleQuarantineReasonBytes int64                  `gorm:"column:lifecycle_quarantine_reason_bytes"`
}

func preflightExpiredMutationReceiptReclaim(
	tx *gorm.DB,
	tenantID string,
	limit int64,
	cutoff int64,
) error {
	if _, err := canonicalTime(cutoff); err != nil {
		return fmt.Errorf("%w: knowledge receipt reclamation cutoff is invalid", ErrCorrupt)
	}
	var widths []mutationReceiptReclaimWidth
	if err := tx.Table(`knowledge_mutation_idempotency
		INDEXED BY knowledge_mutation_idempotency_retention_idx`).
		Select(`
			length(CAST(tenant_id AS BLOB)) AS tenant_id_bytes,
			length(CAST(actor_kind AS BLOB)) AS actor_kind_bytes,
			length(CAST(actor_id AS BLOB)) AS actor_id_bytes,
			length(CAST(route AS BLOB)) AS route_bytes,
			length(CAST(client_request_id AS BLOB)) AS request_id_bytes,
			length(CAST(mutation_kind AS BLOB)) AS mutation_kind_bytes,
			length(request_digest) AS request_digest_bytes,
			length(outcome_proto) AS outcome_proto_bytes,
			length(committed_catalog_state_token) AS state_token_bytes,
			length(CAST(knowledge_object_id AS BLOB)) AS object_id_bytes`).
		Where(`knowledge_mutation_idempotency.tenant_id = ?
			AND knowledge_mutation_idempotency.retain_until_unix_micro <= ?`, tenantID, cutoff).
		Order(`knowledge_mutation_idempotency.retain_until_unix_micro,
			knowledge_mutation_idempotency.created_at_unix_micro,
			knowledge_mutation_idempotency.actor_kind,
			knowledge_mutation_idempotency.actor_id,
			knowledge_mutation_idempotency.route,
			knowledge_mutation_idempotency.client_request_id`).
		Limit(int(limit)).
		Find(&widths).Error; err != nil {
		return err
	}
	for _, width := range widths {
		if width.TenantIDBytes != int64(len(tenantID)) ||
			width.ActorKindBytes < int64(len(audit.ActorKindSystem)) ||
			width.ActorKindBytes > int64(len(audit.ActorKindBrowser)) ||
			width.ActorIDBytes < 1 || width.ActorIDBytes > maximumOwnerIDBytes ||
			width.RouteBytes < 1 || width.RouteBytes > int64(maximumMutationRouteBytes) ||
			width.RequestIDBytes < minimumClientRequestIDBytes ||
			width.RequestIDBytes > maximumClientRequestIDBytes ||
			width.MutationKindBytes < minimumPersistedMutationKindBytes ||
			width.MutationKindBytes > maximumPersistedMutationKindBytes ||
			width.RequestDigestBytes != 32 ||
			width.OutcomeProtoBytes < 1 || width.OutcomeProtoBytes > maximumMutationOutcomeBytes ||
			width.StateTokenBytes != catalogStateTokenBytes ||
			width.ObjectIDBytes < 1 || width.ObjectIDBytes > maximumObjectIDBytes {
			return fmt.Errorf("%w: expired knowledge mutation receipt key is invalid", ErrCorrupt)
		}
	}
	var records []mutationReceiptReclaimRecord
	if err := tx.Table(`knowledge_mutation_idempotency
		INDEXED BY knowledge_mutation_idempotency_retention_idx`).
		Select(`knowledge_mutation_idempotency.*,
			CASE WHEN EXISTS (
				SELECT 1
				FROM knowledge_mutation_commit_authorities AS committed
				WHERE committed.tenant_id = knowledge_mutation_idempotency.tenant_id
				  AND committed.actor_kind = knowledge_mutation_idempotency.actor_kind
				  AND committed.actor_id = knowledge_mutation_idempotency.actor_id
				  AND committed.route = knowledge_mutation_idempotency.route
				  AND committed.client_request_id = knowledge_mutation_idempotency.client_request_id
				  AND committed.request_digest = knowledge_mutation_idempotency.request_digest
				  AND committed.catalog_revision = knowledge_mutation_idempotency.committed_catalog_revision
				  AND committed.catalog_state_token = knowledge_mutation_idempotency.committed_catalog_state_token
				  AND committed.mutation_kind = knowledge_mutation_idempotency.mutation_kind
				  AND committed.knowledge_object_id = knowledge_mutation_idempotency.knowledge_object_id
				  AND committed.object_version = knowledge_mutation_idempotency.object_version
				  AND committed.occurred_at_unix_micro = knowledge_mutation_idempotency.created_at_unix_micro
				  AND committed.retention_anchor_unix_micro = knowledge_mutation_idempotency.retention_anchor_unix_micro
				  AND committed.retain_until_unix_micro = knowledge_mutation_idempotency.retain_until_unix_micro
				  AND committed.successful_audit_sequence IS knowledge_mutation_idempotency.successful_audit_sequence
				  AND committed.recovery_audit_sequence IS knowledge_mutation_idempotency.recovery_audit_sequence
			) THEN 1 ELSE 0 END AS commit_authority_matches,
			CASE WHEN immutable.tenant_id IS NOT NULL AND (
				(knowledge_mutation_idempotency.mutation_kind <> 'quarantine'
				 AND knowledge_mutation_idempotency.successful_audit_sequence IS NOT NULL
				 AND knowledge_mutation_idempotency.recovery_audit_sequence IS NULL
				 AND EXISTS (
					SELECT 1 FROM audit_events AS event
					WHERE event.tenant_id = immutable.tenant_id
					  AND event.sequence = knowledge_mutation_idempotency.successful_audit_sequence
					  AND event.actor_kind = knowledge_mutation_idempotency.actor_kind
					  AND event.actor_id = knowledge_mutation_idempotency.actor_id
					  AND event.actor_role = CASE knowledge_mutation_idempotency.actor_kind
						WHEN 'browser' THEN 'administrator'
						WHEN 'system' THEN 'system'
					  END
					  AND event.occurred_at_unix_micro = immutable.created_at_unix_micro
					  AND event.target_kind = 'knowledge_object'
					  AND event.target_id = immutable.knowledge_object_id
					  AND event.target_version = immutable.object_version
					  AND event.app_id = immutable.app_id
					  AND event.object_type = immutable.object_type
					  AND event.sharing_scope = immutable.sharing_scope
					  AND event.action = CASE immutable.mutation_kind
						WHEN 'create' THEN 'knowledge.object.create'
						WHEN 'update' THEN 'knowledge.object.update'
						WHEN 'scope_change' THEN 'knowledge.object.scope_change'
						WHEN 'enable' THEN 'knowledge.object.enable'
						WHEN 'disable' THEN 'knowledge.object.disable'
						WHEN 'delete' THEN 'knowledge.object.delete'
					  END
				 )
				)
				OR
				(knowledge_mutation_idempotency.mutation_kind = 'quarantine'
				 AND knowledge_mutation_idempotency.successful_audit_sequence IS NULL
				 AND knowledge_mutation_idempotency.recovery_audit_sequence IS NOT NULL
				 AND EXISTS (
					SELECT 1 FROM knowledge_recovery_audit AS recovery
					WHERE recovery.tenant_id = immutable.tenant_id
					  AND recovery.sequence = knowledge_mutation_idempotency.recovery_audit_sequence
					  AND recovery.actor_kind = knowledge_mutation_idempotency.actor_kind
					  AND recovery.actor_id = knowledge_mutation_idempotency.actor_id
					  AND recovery.actor_role = CASE knowledge_mutation_idempotency.actor_kind
						WHEN 'browser' THEN 'administrator'
						WHEN 'system' THEN 'system'
					  END
					  AND recovery.occurred_at_unix_micro = immutable.created_at_unix_micro
					  AND recovery.knowledge_object_id = immutable.knowledge_object_id
					  AND recovery.object_version = immutable.object_version
					  AND recovery.app_id = immutable.app_id
					  AND recovery.object_type = immutable.object_type
					  AND recovery.sharing_scope = immutable.sharing_scope
					  AND recovery.recovery_reason = immutable.quarantine_reason
				 )
				)
			  ) THEN 1 ELSE 0 END AS audit_authority_matches,
			CASE WHEN length(CAST(immutable.tenant_id AS BLOB)) BETWEEN 1 AND 128
			  THEN immutable.tenant_id ELSE '' END AS immutable_tenant_id,
			CASE WHEN length(CAST(immutable.knowledge_object_id AS BLOB)) BETWEEN 1 AND 128
			  THEN immutable.knowledge_object_id ELSE '' END AS immutable_knowledge_object_id,
			coalesce(immutable.object_version, 0) AS immutable_object_version,
			CASE WHEN length(CAST(immutable.app_id AS BLOB)) BETWEEN 1 AND 128
			  THEN immutable.app_id ELSE '' END AS immutable_app_id,
			CASE WHEN length(CAST(immutable.owner_id AS BLOB)) BETWEEN 1 AND 255
			  THEN immutable.owner_id ELSE '' END AS immutable_owner_id,
			CASE WHEN length(CAST(immutable.object_type AS BLOB)) BETWEEN 1 AND 64
			  THEN immutable.object_type ELSE '' END AS immutable_object_type,
			CASE WHEN length(CAST(immutable.name AS BLOB)) BETWEEN 1 AND 255
			  THEN immutable.name ELSE '' END AS immutable_name,
			CASE WHEN length(CAST(immutable.sharing_scope AS BLOB)) BETWEEN 1 AND 16
			  THEN immutable.sharing_scope ELSE '' END AS immutable_sharing_scope,
			CASE WHEN length(CAST(immutable.state AS BLOB)) BETWEEN 1 AND 16
			  THEN immutable.state ELSE '' END AS immutable_state,
			CASE
			  WHEN immutable.definition_digest IS NULL THEN X''
			  WHEN length(immutable.definition_digest) = 32 THEN immutable.definition_digest
			  ELSE NULL
			END AS immutable_definition_digest,
			coalesce(immutable.dependency_count, -1) AS immutable_dependency_count,
			CASE WHEN length(CAST(immutable.mutation_kind AS BLOB)) BETWEEN 1 AND 32
			  THEN immutable.mutation_kind ELSE '' END AS immutable_mutation_kind,
			CASE WHEN immutable.quarantine_reason IS NULL THEN NULL
			  WHEN length(CAST(immutable.quarantine_reason AS BLOB)) BETWEEN 1 AND 32
			  THEN immutable.quarantine_reason ELSE NULL END AS immutable_quarantine_reason,
			coalesce(immutable.created_at_unix_micro, 0) AS immutable_created_at_unix_micro,
			coalesce(length(immutable.definition_digest), 0) AS immutable_definition_digest_bytes,
			coalesce(length(CAST(immutable.quarantine_reason AS BLOB)), 0)
			  AS immutable_quarantine_reason_bytes,
			CASE WHEN length(CAST(lifecycle.tenant_id AS BLOB)) BETWEEN 1 AND 128
			  THEN lifecycle.tenant_id ELSE '' END AS lifecycle_tenant_id,
			CASE WHEN length(CAST(lifecycle.knowledge_object_id AS BLOB)) BETWEEN 1 AND 128
			  THEN lifecycle.knowledge_object_id ELSE '' END AS lifecycle_knowledge_object_id,
			coalesce(lifecycle.object_version, 0) AS lifecycle_object_version,
			CASE WHEN length(CAST(lifecycle.state AS BLOB)) BETWEEN 1 AND 16
			  THEN lifecycle.state ELSE '' END AS lifecycle_state,
			lifecycle.disabled_at_unix_micro AS lifecycle_disabled_at_unix_micro,
			lifecycle.quarantined_at_unix_micro AS lifecycle_quarantined_at_unix_micro,
			lifecycle.deleted_at_unix_micro AS lifecycle_deleted_at_unix_micro,
			CASE WHEN lifecycle.quarantine_reason IS NULL THEN NULL
			  WHEN length(CAST(lifecycle.quarantine_reason AS BLOB)) BETWEEN 1 AND 32
			  THEN lifecycle.quarantine_reason ELSE NULL END AS lifecycle_quarantine_reason,
			coalesce(length(CAST(lifecycle.quarantine_reason AS BLOB)), 0)
			  AS lifecycle_quarantine_reason_bytes`).
		Joins(`LEFT JOIN knowledge_object_versions AS immutable
		  ON immutable.tenant_id = knowledge_mutation_idempotency.tenant_id
		 AND immutable.knowledge_object_id = knowledge_mutation_idempotency.knowledge_object_id
		 AND immutable.object_version = knowledge_mutation_idempotency.object_version`).
		Joins(`LEFT JOIN knowledge_object_version_lifecycle AS lifecycle
		  ON lifecycle.tenant_id = immutable.tenant_id
		 AND lifecycle.knowledge_object_id = immutable.knowledge_object_id
		 AND lifecycle.object_version = immutable.object_version`).
		Where(`knowledge_mutation_idempotency.tenant_id = ?
			AND knowledge_mutation_idempotency.retain_until_unix_micro <= ?`, tenantID, cutoff).
		Order(`knowledge_mutation_idempotency.retain_until_unix_micro,
			knowledge_mutation_idempotency.created_at_unix_micro,
			knowledge_mutation_idempotency.actor_kind,
			knowledge_mutation_idempotency.actor_id,
			knowledge_mutation_idempotency.route,
			knowledge_mutation_idempotency.client_request_id`).
		Limit(int(limit)).
		Find(&records).Error; err != nil {
		return err
	}
	if len(records) != len(widths) {
		return fmt.Errorf("%w: expired knowledge mutation receipt prefix changed after preflight", ErrCorrupt)
	}
	for _, candidate := range records {
		record := candidate.Record
		if candidate.CommitAuthorityMatches != 1 ||
			candidate.AuditAuthorityMatches != 1 || record.TenantID != tenantID {
			return fmt.Errorf("%w: expired knowledge mutation receipt authority is invalid", ErrCorrupt)
		}
		if err := validateStoredIdempotencyRecord(record); err != nil {
			return err
		}
		reference, err := decodeOutcomeReference(record)
		if err != nil {
			return err
		}
		version := candidate.ImmutableVersion
		if validateVersion(version) != nil || version.TenantID != record.TenantID ||
			version.KnowledgeObjectID != record.KnowledgeObjectID ||
			version.ObjectVersion != record.ObjectVersion || version.MutationKind != record.MutationKind ||
			version.CreatedAtUnixMicro != record.CreatedAtUnixMicro || len(version.AppID) != 26 ||
			candidate.ImmutableDefinitionDigestBytes != int64(len(version.DefinitionDigest)) ||
			candidate.ImmutableQuarantineReasonBytes != optionalStringBytes(version.QuarantineReason) ||
			(version.QuarantineReason != nil && !validIdentity(*version.QuarantineReason, maximumPersistedQuarantineReasonBytes)) ||
			candidate.LifecycleQuarantineReasonBytes != optionalStringBytes(candidate.Lifecycle.QuarantineReason) ||
			(candidate.Lifecycle.QuarantineReason != nil &&
				!validIdentity(*candidate.Lifecycle.QuarantineReason, maximumPersistedQuarantineReasonBytes)) ||
			validateVersionLifecycle(version, candidate.Lifecycle) != nil {
			return fmt.Errorf("%w: expired knowledge mutation immutable authority disagrees", ErrCorrupt)
		}
		if len(version.DefinitionDigest) != len(reference.GetDefinitionSha256()) ||
			subtle.ConstantTimeCompare(version.DefinitionDigest, reference.GetDefinitionSha256()) != 1 {
			return fmt.Errorf("%w: expired knowledge mutation definition authority disagrees", ErrCorrupt)
		}
	}
	return nil
}

func optionalStringBytes(value *string) int64 {
	if value == nil {
		return 0
	}
	return int64(len(*value))
}

func mutationReceiptRouteMatchesKind(route, mutationKind string) bool {
	if route == mutationRouteQuarantine {
		return mutationKind == "quarantine"
	}
	return validMutationRoute(route) && mutationKindMatchesRoute(route, mutationKind)
}

func ensureMutationLedgers(tx *gorm.DB, tenantID string) error {
	if result := tx.Exec(`INSERT INTO knowledge_catalog_tenants (tenant_id)
		SELECT ? WHERE NOT EXISTS (
			SELECT 1 FROM knowledge_catalog_tenants WHERE tenant_id = ?
		)`, tenantID, tenantID); result.Error != nil {
		return result.Error
	}
	if result := tx.Exec(`INSERT INTO knowledge_projection_tenant_ledgers (tenant_id)
		SELECT ? WHERE NOT EXISTS (
			SELECT 1 FROM knowledge_projection_tenant_ledgers WHERE tenant_id = ?
		)`, tenantID, tenantID); result.Error != nil {
		return result.Error
	}
	return nil
}

func readMutationTenantHealth(
	tx *gorm.DB,
	tenantID string,
) (mutationTenantHealth, catalogState, error) {
	var records []mutationTenantHealth
	if err := tx.Raw(`SELECT
		tenant.catalog_revision,
		tenant.identity_count,
		tenant.version_count,
		tenant.definition_body_bytes AS definition_bytes,
		tenant.idempotency_count,
		tenant.active_object_count,
		tenant.recovery_audit_count,
		projection.projection_bytes
	FROM knowledge_catalog_tenants AS tenant
	JOIN knowledge_projection_tenant_ledgers AS projection
	  ON projection.tenant_id = tenant.tenant_id
	WHERE tenant.tenant_id = ? LIMIT 2`, tenantID).Scan(&records).Error; err != nil {
		return mutationTenantHealth{}, catalogState{}, err
	}
	if len(records) != 1 {
		return mutationTenantHealth{}, catalogState{}, fmt.Errorf("%w: catalog mutation ledger is missing", ErrCorrupt)
	}
	health := records[0]
	state, err := readCatalogState(tx, tenantID)
	if err != nil {
		return mutationTenantHealth{}, catalogState{}, err
	}
	if !state.found || state.revision != health.CatalogRevision {
		return mutationTenantHealth{}, catalogState{}, fmt.Errorf("%w: catalog mutation revision authority disagrees", ErrCorrupt)
	}
	var cardinality struct {
		Versions    int64 `gorm:"column:versions"`
		Definitions int64 `gorm:"column:definitions"`
	}
	if err := tx.Raw(mutationDefinitionCardinalitySQL,
		tenantID, tenantID,
	).Scan(&cardinality).Error; err != nil {
		return mutationTenantHealth{}, catalogState{}, err
	}
	// Blob cardinality is checked before definition bytes are aggregated. That
	// prevents a forged byte ledger from making an orphaned or over-capacity
	// blob set pay work beyond the version ceiling.
	if cardinality.Versions < 0 || cardinality.Versions > maximumVersionsPerTenant ||
		cardinality.Definitions < 0 || cardinality.Definitions > maximumVersionsPerTenant ||
		health.VersionCount < 0 || health.VersionCount > maximumVersionsPerTenant ||
		cardinality.Definitions > cardinality.Versions ||
		cardinality.Definitions > health.VersionCount {
		return mutationTenantHealth{}, catalogState{}, fmt.Errorf(
			"%w: catalog mutation definition cardinality is invalid",
			ErrCorrupt,
		)
	}
	var physical struct {
		Identities      int64 `gorm:"column:identities"`
		DefinitionBytes int64 `gorm:"column:definition_bytes"`
		Idempotency     int64 `gorm:"column:idempotency"`
		Active          int64 `gorm:"column:active"`
		Recovery        int64 `gorm:"column:recovery"`
		Projections     int64 `gorm:"column:projections"`
		ProjectionBytes int64 `gorm:"column:projection_bytes"`
	}
	if err := tx.Raw(mutationPhysicalHealthSQL,
		tenantID, tenantID, tenantID, tenantID, tenantID, tenantID, tenantID,
	).Scan(&physical).Error; err != nil {
		return mutationTenantHealth{}, catalogState{}, err
	}
	if physical.Identities != health.IdentityCount || cardinality.Versions != health.VersionCount ||
		physical.DefinitionBytes != health.DefinitionBytes || physical.Idempotency != health.IdempotencyCount ||
		physical.Active != health.ActiveObjectCount || physical.Recovery != health.RecoveryAuditCount ||
		physical.Projections != physical.Identities ||
		physical.ProjectionBytes != health.ProjectionBytes {
		return mutationTenantHealth{}, catalogState{}, fmt.Errorf("%w: catalog mutation ledger disagrees with bounded physical state", ErrCorrupt)
	}
	if health.IdentityCount < 0 || health.IdentityCount > maximumObjectsPerTenant ||
		health.VersionCount < 0 || health.VersionCount > maximumVersionsPerTenant ||
		health.DefinitionBytes < 0 || health.DefinitionBytes > 536870912 ||
		health.IdempotencyCount < 0 || health.IdempotencyCount > absoluteIdempotencyCapacity ||
		health.ActiveObjectCount < 0 || health.ActiveObjectCount > 4096 ||
		health.RecoveryAuditCount < 0 || health.RecoveryAuditCount > 8192 ||
		health.ProjectionBytes < 0 || health.ProjectionBytes > 268435456 {
		return mutationTenantHealth{}, catalogState{}, fmt.Errorf("%w: catalog mutation ledger exceeds its bounds", ErrCorrupt)
	}
	if err := validateActiveCounterHealth(tx, tenantID); err != nil {
		return mutationTenantHealth{}, catalogState{}, err
	}
	return health, state, nil
}

func validateActiveCounterHealth(tx *gorm.DB, tenantID string) error {
	if err := preflightActiveCounterHealth(tx, tenantID); err != nil {
		return err
	}
	checks := []string{
		`SELECT 1 FROM knowledge_app_active_counters AS counter
		 LEFT JOIN (SELECT app_id, count(*) AS n FROM knowledge_objects WHERE tenant_id = ? AND state = 'active' GROUP BY app_id) AS actual USING (app_id)
		 WHERE counter.tenant_id = ? AND counter.active_object_count <> coalesce(actual.n, 0)
		 UNION ALL SELECT 1 FROM (SELECT app_id, count(*) AS n FROM knowledge_objects WHERE tenant_id = ? AND state = 'active' GROUP BY app_id) AS actual
		 WHERE NOT EXISTS (SELECT 1 FROM knowledge_app_active_counters AS counter WHERE counter.tenant_id = ? AND counter.app_id = actual.app_id) LIMIT 1`,
		`SELECT 1 FROM knowledge_owner_active_counters AS counter
		 LEFT JOIN (SELECT owner_id, count(*) AS n FROM knowledge_objects WHERE tenant_id = ? AND state = 'active' AND sharing_scope = 'private' GROUP BY owner_id) AS actual USING (owner_id)
		 WHERE counter.tenant_id = ? AND counter.active_private_object_count <> coalesce(actual.n, 0)
		 UNION ALL SELECT 1 FROM (SELECT owner_id, count(*) AS n FROM knowledge_objects WHERE tenant_id = ? AND state = 'active' AND sharing_scope = 'private' GROUP BY owner_id) AS actual
		 WHERE NOT EXISTS (SELECT 1 FROM knowledge_owner_active_counters AS counter WHERE counter.tenant_id = ? AND counter.owner_id = actual.owner_id) LIMIT 1`,
		`SELECT 1 FROM knowledge_type_active_counters AS counter
		 LEFT JOIN (SELECT object_type, count(*) AS n FROM knowledge_objects WHERE tenant_id = ? AND state = 'active' GROUP BY object_type) AS actual USING (object_type)
		 WHERE counter.tenant_id = ? AND counter.active_object_count <> coalesce(actual.n, 0)
		 UNION ALL SELECT 1 FROM (SELECT object_type, count(*) AS n FROM knowledge_objects WHERE tenant_id = ? AND state = 'active' GROUP BY object_type) AS actual
		 WHERE NOT EXISTS (SELECT 1 FROM knowledge_type_active_counters AS counter WHERE counter.tenant_id = ? AND counter.object_type = actual.object_type) LIMIT 1`,
		`SELECT 1 FROM knowledge_app_type_active_counters AS counter
		 LEFT JOIN (SELECT app_id, object_type, count(*) AS n FROM knowledge_objects WHERE tenant_id = ? AND state = 'active' GROUP BY app_id, object_type) AS actual USING (app_id, object_type)
		 WHERE counter.tenant_id = ? AND counter.active_object_count <> coalesce(actual.n, 0)
		 UNION ALL SELECT 1 FROM (SELECT app_id, object_type, count(*) AS n FROM knowledge_objects WHERE tenant_id = ? AND state = 'active' GROUP BY app_id, object_type) AS actual
		 WHERE NOT EXISTS (SELECT 1 FROM knowledge_app_type_active_counters AS counter WHERE counter.tenant_id = ? AND counter.app_id = actual.app_id AND counter.object_type = actual.object_type) LIMIT 1`,
	}
	for _, query := range checks {
		var invalid int64
		if err := tx.Raw(query, tenantID, tenantID, tenantID, tenantID).Scan(&invalid).Error; err != nil {
			return err
		}
		if invalid != 0 {
			return fmt.Errorf("%w: active knowledge counter disagrees with current registry", ErrCorrupt)
		}
	}
	return nil
}

func preflightActiveCounterHealth(tx *gorm.DB, tenantID string) error {
	for _, kind := range []activeCounterPreflightKind{
		activeAppCounterPreflight,
		activeOwnerCounterPreflight,
		activeTypeCounterPreflight,
		activeAppTypeCounterPreflight,
	} {
		spec := activeCounterSpec(kind)
		query := activeCounterPreflightQuery(tx, tenantID, spec)
		var records []activeCounterPreflightRecord
		if err := query.Find(&records).Error; err != nil {
			return err
		}
		if err := validateActiveCounterPreflight(kind, spec, records); err != nil {
			return err
		}
	}
	return nil
}

func activeCounterSpec(kind activeCounterPreflightKind) activeCounterPreflightSpec {
	switch kind {
	case activeAppCounterPreflight:
		return activeCounterPreflightSpec{
			table: "knowledge_app_active_counters", firstKeyColumn: "app_id",
			countColumn: "active_object_count", maximumRows: maximumActiveAppCounterRows,
			maximumCount: 1024, maximumFirstKeyBytes: maximumAppIDBytes,
		}
	case activeOwnerCounterPreflight:
		return activeCounterPreflightSpec{
			table: "knowledge_owner_active_counters", firstKeyColumn: "owner_id",
			countColumn: "active_private_object_count", maximumRows: maximumActiveOwnerCounterRows,
			maximumCount: 512, maximumFirstKeyBytes: maximumOwnerIDBytes,
		}
	case activeTypeCounterPreflight:
		return activeCounterPreflightSpec{
			table: "knowledge_type_active_counters", firstKeyColumn: "object_type",
			countColumn: "active_object_count", maximumRows: maximumActiveTypeCounterRows,
			maximumCount: 2048, maximumFirstKeyBytes: len("calculated_field"),
		}
	case activeAppTypeCounterPreflight:
		return activeCounterPreflightSpec{
			table: "knowledge_app_type_active_counters", firstKeyColumn: "app_id",
			secondKeyColumn: "object_type", countColumn: "active_object_count",
			maximumRows: maximumActiveAppTypeCounterRows, maximumCount: 512,
			maximumFirstKeyBytes: maximumAppIDBytes, maximumSecondKeyBytes: len("calculated_field"),
		}
	default:
		panic("unknown active counter preflight")
	}
}

func activeCounterPreflightQuery(
	tx *gorm.DB,
	tenantID string,
	spec activeCounterPreflightSpec,
) *gorm.DB {
	secondKeyProjection := "0 AS second_key_bytes, '' AS second_key"
	if spec.secondKeyColumn != "" {
		secondKeyProjection = fmt.Sprintf(`
			length(CAST(counter.%[1]s AS BLOB)) AS second_key_bytes,
			CASE WHEN length(CAST(counter.%[1]s AS BLOB)) BETWEEN 1 AND %[2]d
				THEN counter.%[1]s ELSE '' END AS second_key`,
			spec.secondKeyColumn,
			spec.maximumSecondKeyBytes,
		)
	}
	selection := fmt.Sprintf(`
		length(CAST(counter.%[1]s AS BLOB)) AS first_key_bytes,
		CASE WHEN length(CAST(counter.%[1]s AS BLOB)) BETWEEN 1 AND %[2]d
			THEN counter.%[1]s ELSE '' END AS first_key,
		%[3]s,
		counter.%[4]s AS counter_value`,
		spec.firstKeyColumn,
		spec.maximumFirstKeyBytes,
		secondKeyProjection,
		spec.countColumn,
	)
	// Every table is WITHOUT ROWID with a tenant-leading primary key. No
	// attacker-controlled key is detached until its scalar width has passed.
	return tx.Table(spec.table+" AS counter").
		Select(selection).
		Where("counter.tenant_id = ?", tenantID).
		Limit(spec.maximumRows + 1)
}

func validateActiveCounterPreflight(
	kind activeCounterPreflightKind,
	spec activeCounterPreflightSpec,
	records []activeCounterPreflightRecord,
) error {
	corrupt := func() error {
		return fmt.Errorf("%w: active knowledge counter physical state is invalid", ErrCorrupt)
	}
	if len(records) > spec.maximumRows {
		return corrupt()
	}
	for _, record := range records {
		if record.FirstKeyBytes < 1 || record.FirstKeyBytes > int64(spec.maximumFirstKeyBytes) ||
			record.CounterValue < 0 || record.CounterValue > spec.maximumCount {
			return corrupt()
		}
		switch kind {
		case activeAppCounterPreflight:
			if !validCanonicalAppID(record.FirstKey) || record.SecondKeyBytes != 0 || record.SecondKey != "" {
				return corrupt()
			}
		case activeOwnerCounterPreflight:
			if !validIdentity(record.FirstKey, maximumOwnerIDBytes) || record.SecondKeyBytes != 0 || record.SecondKey != "" {
				return corrupt()
			}
		case activeTypeCounterPreflight:
			if !ObjectType(record.FirstKey).Valid() || record.SecondKeyBytes != 0 || record.SecondKey != "" {
				return corrupt()
			}
		case activeAppTypeCounterPreflight:
			if !validCanonicalAppID(record.FirstKey) ||
				record.SecondKeyBytes < 1 || record.SecondKeyBytes > int64(spec.maximumSecondKeyBytes) ||
				!ObjectType(record.SecondKey).Valid() {
				return corrupt()
			}
		default:
			return corrupt()
		}
	}
	return nil
}

func (writer *Writer) selectCreateObjectID(tx *gorm.DB, tenantID string) (string, int, error) {
	for attempt := 0; attempt < maximumWriterIDAttempts; attempt++ {
		objectID, err := writer.idGenerator()
		if err != nil {
			return "", 0, fmt.Errorf("generate knowledge object identity: %w", err)
		}
		if !validIdentity(objectID, maximumObjectIDBytes) {
			return "", 0, errors.New("generate knowledge object identity: generator returned an invalid identity")
		}
		var count int64
		if err := tx.Model(&registryRecord{}).Where(
			"tenant_id = ? AND knowledge_object_id = ?", tenantID, objectID,
		).Limit(1).Count(&count).Error; err != nil {
			return "", 0, writerError(tx.Statement.Context, "check generated knowledge identity", err)
		}
		if count == 0 {
			return objectID, attempt, nil
		}
	}
	return "", 0, errors.New("generate knowledge object identity: repeated collision")
}

func (writer *Writer) hydrateAuthorizedCurrent(
	tx *gorm.DB,
	current registryRecord,
) (Object, versionRecord, definitionAuthority, error) {
	object, err := writer.reader.objectFromCurrentRegistry(tx, current)
	if err != nil {
		return Object{}, versionRecord{}, definitionAuthority{}, err
	}
	version, found, err := readVersionRecord(tx, current.TenantID, current.KnowledgeObjectID, current.CurrentVersion)
	if err != nil || !found {
		if err == nil {
			err = fmt.Errorf("%w: current immutable version is missing", ErrCorrupt)
		}
		return Object{}, versionRecord{}, definitionAuthority{}, err
	}
	authority, err := authorityFromStoredVersion(tx, version)
	if err != nil {
		return Object{}, versionRecord{}, definitionAuthority{}, err
	}
	return object, version, authority, nil
}

func readAuthorizedMutationRegistry(
	tx *gorm.DB,
	scope normalizedWriteScope,
	objectID string,
) (registryRecord, error) {
	query := tx.Model(&registryRecord{}).Where(
		"tenant_id = ? AND knowledge_object_id = ?", scope.tenantID, objectID,
	).Where("owner_id = ?", scope.ownerID).
		Where("app_id IN ?", scope.writableAppIDs)
	current, found, err := readRegistryRecord(query)
	if err != nil {
		return registryRecord{}, writerError(tx.Statement.Context, "read mutation registry", err)
	}
	if !found {
		return registryRecord{}, control.ErrNotFound
	}
	if err := validateRegistry(current); err != nil {
		return registryRecord{}, err
	}
	return current, nil
}

func (writer *Writer) hydrateAuthorizedCurrentStateOnly(
	tx *gorm.DB,
	current registryRecord,
) (Object, versionRecord, definitionAuthority, error) {
	query := baseProjectionQuery(tx).Where(
		"projection.tenant_id = ? AND projection.knowledge_object_id = ? AND projection.object_version = ?",
		current.TenantID,
		current.KnowledgeObjectID,
		current.CurrentVersion,
	)
	projections, err := readProjectionRecords(query, 2)
	if err != nil || len(projections) != 1 {
		if err == nil {
			err = fmt.Errorf("%w: current list projection is missing or ambiguous", ErrCorrupt)
		}
		return Object{}, versionRecord{}, definitionAuthority{}, err
	}
	projection := projections[0]
	if err := validateProjectionScalars(projection, current); err != nil {
		return Object{}, versionRecord{}, definitionAuthority{}, err
	}
	version, found, err := readVersionRecord(tx, current.TenantID, current.KnowledgeObjectID, current.CurrentVersion)
	if err != nil || !found || validateCurrentVersion(current, version) != nil {
		if err == nil {
			err = fmt.Errorf("%w: current immutable version disagrees with registry", ErrCorrupt)
		}
		return Object{}, versionRecord{}, definitionAuthority{}, err
	}
	if err := validateCurrentChronology(tx, current, version); err != nil {
		return Object{}, versionRecord{}, definitionAuthority{}, err
	}
	lifecycle, err := readVersionLifecycle(tx, version)
	if err != nil {
		return Object{}, versionRecord{}, definitionAuthority{}, err
	}
	if err := validateCurrentLifecycle(current, version, lifecycle); err != nil {
		return Object{}, versionRecord{}, definitionAuthority{}, err
	}
	dependencies, err := readValidatedVersionDependencies(tx, version)
	if err != nil {
		return Object{}, versionRecord{}, definitionAuthority{}, err
	}
	selectors, err := readSelectorRecords(tx, current.TenantID, current.KnowledgeObjectID, current.CurrentVersion)
	if err != nil {
		return Object{}, versionRecord{}, definitionAuthority{}, err
	}
	decoded, blob, err := decodeVersionDefinitionForStateOnly(tx, version)
	if err != nil {
		return Object{}, versionRecord{}, definitionAuthority{}, err
	}
	if blob.DefinitionBytes != projection.DefinitionBytes {
		return Object{}, versionRecord{}, definitionAuthority{}, fmt.Errorf(
			"%w: projected definition byte authority disagrees",
			ErrCorrupt,
		)
	}
	if err := validateDecodedIdentity(decoded, version); err != nil {
		return Object{}, versionRecord{}, definitionAuthority{}, err
	}
	if err := validateProjectionDefinition(projection, selectors, decoded); err != nil {
		return Object{}, versionRecord{}, definitionAuthority{}, err
	}
	if decoded.ObjectTypeKnown {
		if err := validateDecodedVersionDependencies(tx, version, decoded, dependencies); err != nil {
			return Object{}, versionRecord{}, definitionAuthority{}, err
		}
	} else {
		structuralVersion := version
		structuralVersion.State = StateDraft
		if err := validateDecodedVersionDependencies(tx, structuralVersion, decoded, dependencies); err != nil {
			return Object{}, versionRecord{}, definitionAuthority{}, err
		}
	}
	object, err := objectFromCurrentScalars(current, decoded.Definition, decoded.Digest)
	if err != nil {
		return Object{}, versionRecord{}, definitionAuthority{}, err
	}
	authority, err := definitionAuthorityFromDecodedStored(decoded, version, blob)
	if err != nil {
		return Object{}, versionRecord{}, definitionAuthority{}, err
	}
	return object, version, authority, nil
}

func decodeVersionDefinitionForStateOnly(
	tx *gorm.DB,
	version versionRecord,
) (decodedDefinition, definitionBlobRecord, error) {
	blob, err := readDefinitionBlob(tx, version.TenantID, version.DefinitionDigest)
	if err != nil {
		return decodedDefinition{}, definitionBlobRecord{}, err
	}
	decoded, _, err := decodeVersionDefinitionBlob(version, blob)
	if err == nil {
		return decoded, blob, nil
	}
	if version.State != StateActive {
		return decodedDefinition{}, definitionBlobRecord{}, err
	}
	structuralVersion := version
	structuralVersion.State = StateDraft
	decoded, _, structuralErr := decodeVersionDefinitionBlob(structuralVersion, blob)
	if structuralErr != nil || decoded.ObjectTypeKnown {
		return decodedDefinition{}, definitionBlobRecord{}, fmt.Errorf(
			"%w: active definition cannot be preserved structurally: strict=%v; opaque=%v",
			ErrCorrupt,
			err,
			structuralErr,
		)
	}
	return decoded, blob, nil
}

func definitionAuthorityFromDecodedStored(
	decoded decodedDefinition,
	version versionRecord,
	blob definitionBlobRecord,
) (definitionAuthority, error) {
	if decoded.Definition == nil || len(decoded.Digest) != persistedKnowledgeDefinitionDigestBytes ||
		len(blob.DefinitionProto) < 1 || int64(len(blob.DefinitionProto)) != blob.DefinitionBytes {
		return definitionAuthority{}, fmt.Errorf("%w: stored definition authority is invalid", ErrCorrupt)
	}
	return definitionAuthority{
		definition:   proto.Clone(decoded.Definition).(*opensplunkv1.KnowledgeObjectDefinition),
		bytes:        bytes.Clone(blob.DefinitionProto),
		digest:       bytes.Clone(decoded.Digest),
		objectType:   version.ObjectType,
		appID:        strings.Clone(decoded.AppID),
		name:         strings.Clone(decoded.Name),
		sharingScope: version.SharingScope,
		description:  cloneString(decoded.Description),
		selector:     decoded.Selector,
		opaque:       !decoded.ObjectTypeKnown,
	}, nil
}

func authorityFromStoredVersion(tx *gorm.DB, version versionRecord) (definitionAuthority, error) {
	if len(version.DefinitionDigest) != persistedKnowledgeDefinitionDigestBytes {
		return definitionAuthority{}, fmt.Errorf("%w: version definition digest is invalid", ErrCorrupt)
	}
	blob, err := readDefinitionBlob(tx, version.TenantID, version.DefinitionDigest)
	if err != nil {
		return definitionAuthority{}, err
	}
	decoded, _, err := decodeVersionDefinitionBlob(version, blob)
	if err != nil {
		return definitionAuthority{}, err
	}
	return definitionAuthorityFromDecodedStored(decoded, version, blob)
}

func authorityFromOpaqueMetadataUpdate(
	definition *opensplunkv1.KnowledgeObjectDefinition,
	state State,
	objectType ObjectType,
) (definitionAuthority, error) {
	if definition == nil || !objectType.Valid() {
		return definitionAuthority{}, invalidMutation("opaque future definition metadata is invalid")
	}
	encoded, err := (proto.MarshalOptions{Deterministic: true}).Marshal(definition)
	if err != nil || len(encoded) == 0 {
		return definitionAuthority{}, invalidMutation("opaque future definition metadata cannot be encoded")
	}
	if len(encoded) > knowledgedefinition.MaximumCanonicalBytes {
		return definitionAuthority{}, fmt.Errorf(
			"%w: opaque future definition exceeds its canonical byte limit",
			control.ErrCapacityExceeded,
		)
	}
	digest := sha256.Sum256(encoded)
	protoState, ok := stateToProto(state)
	if !ok {
		return definitionAuthority{}, invalidMutation("opaque future definition lifecycle is invalid")
	}
	future, err := knowledgedefinition.DecodeCanonicalInactiveFutureBody(encoded, digest[:], protoState)
	if err != nil {
		return definitionAuthority{}, invalidMutation("opaque future definition metadata is invalid")
	}
	sharingScope, ok := sharingScopeFromProto(future.SharingScope)
	if !ok {
		return definitionAuthority{}, invalidMutation("opaque future definition sharing scope is invalid")
	}
	detached, ok := proto.Clone(future.Definition).(*opensplunkv1.KnowledgeObjectDefinition)
	if !ok || detached == nil {
		return definitionAuthority{}, invalidMutation("opaque future definition metadata cannot be detached")
	}
	return definitionAuthority{
		definition:   detached,
		bytes:        bytes.Clone(future.Bytes),
		digest:       bytes.Clone(future.Digest[:]),
		objectType:   objectType,
		appID:        strings.Clone(future.AppID),
		name:         strings.Clone(future.Name),
		sharingScope: sharingScope,
		description:  cloneString(future.Description),
		selector:     future.Selector,
		opaque:       true,
	}, nil
}

func normalizeMutationDefinition(
	definition *opensplunkv1.KnowledgeObjectDefinition,
) (knowledgedefinition.Normalized, error) {
	normalized, err := knowledgedefinition.Normalize(definition)
	if err == nil {
		return normalized, nil
	}
	if errors.Is(err, knowledgedefinition.ErrDefinitionTooLarge) {
		return knowledgedefinition.Normalized{}, fmt.Errorf("%w: definition exceeds a resource limit", control.ErrCapacityExceeded)
	}
	return knowledgedefinition.Normalized{}, fmt.Errorf("%w: definition is invalid", control.ErrInvalidArgument)
}

func authorityFromNormalized(normalized knowledgedefinition.Normalized) (definitionAuthority, error) {
	objectType, ok := objectTypeFromProto(normalized.ObjectType)
	sharingScope, scopeOK := sharingScopeFromProto(normalized.SharingScope)
	if !ok || !scopeOK {
		return definitionAuthority{}, invalidMutation("definition identity is unsupported")
	}
	return definitionAuthority{
		definition:   proto.Clone(normalized.Definition).(*opensplunkv1.KnowledgeObjectDefinition),
		bytes:        bytes.Clone(normalized.Bytes),
		digest:       bytes.Clone(normalized.Digest[:]),
		objectType:   objectType,
		appID:        strings.Clone(normalized.AppID),
		name:         strings.Clone(normalized.Name),
		sharingScope: sharingScope,
		description:  cloneString(normalized.Description),
		selector:     normalized.Selector,
	}, nil
}

func authorizeDefinitionApp(
	tx *gorm.DB,
	scope normalizedWriteScope,
	appID string,
	requireActive bool,
) error {
	if !scope.canWriteApp(appID) {
		return control.ErrNotFound
	}
	var records []struct {
		StateBytes int64  `gorm:"column:state_bytes"`
		State      string `gorm:"column:state"`
	}
	if err := tx.Table("app_workspaces").Select(`
		length(CAST(state AS BLOB)) AS state_bytes,
		CASE WHEN length(CAST(state AS BLOB)) <= ? THEN state END AS state`,
		len("archived"),
	).Where(
		"tenant_id = ? AND app_id = ?", scope.tenantID, appID,
	).Limit(2).Find(&records).Error; err != nil {
		return writerError(tx.Statement.Context, "read mutation app", err)
	}
	if len(records) == 0 {
		return control.ErrNotFound
	}
	if len(records) != 1 || records[0].StateBytes < int64(len("active")) ||
		records[0].StateBytes > int64(len("archived")) ||
		records[0].State != "active" && records[0].State != "archived" {
		return fmt.Errorf("%w: mutation app lifecycle is invalid", ErrCorrupt)
	}
	if requireActive && records[0].State != "active" {
		return control.ErrNotFound
	}
	return nil
}

func definitionBlobExistsExact(
	tx *gorm.DB,
	tenantID string,
	authority definitionAuthority,
) (bool, error) {
	var records []struct {
		DefinitionBytes int64  `gorm:"column:definition_bytes"`
		DefinitionProto []byte `gorm:"column:definition_proto"`
	}
	if err := tx.Table("knowledge_definition_blobs").Select(`
		definition_bytes,
		CASE WHEN length(definition_proto) <= ? THEN definition_proto END AS definition_proto`,
		knowledgedefinition.MaximumCanonicalBytes,
	).Where("tenant_id = ? AND definition_digest = ?", tenantID, authority.digest).
		Limit(2).Find(&records).Error; err != nil {
		return false, err
	}
	if len(records) == 0 {
		return false, nil
	}
	if len(records) != 1 || records[0].DefinitionBytes != int64(len(authority.bytes)) ||
		!bytes.Equal(records[0].DefinitionProto, authority.bytes) {
		return false, fmt.Errorf("%w: content-addressed definition digest collision", ErrCorrupt)
	}
	return true, nil
}

func projectionCharge(authority definitionAuthority) int64 {
	charge := int64(0)
	if authority.description != nil {
		charge += int64(len(*authority.description))
	}
	if authority.selector == nil {
		return charge
	}
	for _, dimension := range []knowledge.Dimension{
		knowledge.DimensionIndex,
		knowledge.DimensionHost,
		knowledge.DimensionSource,
		knowledge.DimensionSourcetype,
	} {
		for _, value := range authority.selector.Patterns(dimension) {
			charge += int64(len(value))
		}
	}
	return charge
}

func dependenciesFromCurrent(tx *gorm.DB, version versionRecord) ([]publicationDependency, error) {
	records, err := readValidatedVersionDependencies(tx, version)
	if err != nil {
		return nil, err
	}
	result := make([]publicationDependency, len(records))
	for index, record := range records {
		result[index] = publicationDependency{
			targetObjectID: strings.Clone(record.TargetObjectID),
			targetVersion:  record.TargetObjectVersion,
		}
	}
	return result, nil
}

func hasActiveDependents(tx *gorm.DB, tenantID, objectID string) (bool, error) {
	var records []struct {
		Found int64 `gorm:"column:found"`
	}
	// CROSS JOIN is intentional: SQLite must drive from the bounded current
	// ACTIVE registry set instead of the unbounded retained-history inverse
	// target index. readMutationTenantHealth has already proved that this set
	// contains at most 4,096 rows. Each dependent then probes the exact
	// (tenant, source, current-version, target-kind, target-id) prefix of the
	// immutable dependency set, whose per-version width is at most 1,024.
	err := tx.Raw(activeDependentLookupSQL, tenantID, objectID).Scan(&records).Error
	return len(records) != 0, err
}

const activeDependentLookupSQL = `SELECT 1 AS found
	FROM knowledge_objects AS dependent INDEXED BY knowledge_objects_resolution_idx
	CROSS JOIN knowledge_object_dependencies AS dependency
		INDEXED BY knowledge_object_dependencies_source_target_idx
	WHERE dependent.tenant_id = ?
	  AND dependent.state = 'active'
	  AND dependency.tenant_id = dependent.tenant_id
	  AND dependency.source_object_id = dependent.knowledge_object_id
	  AND dependency.source_object_version = dependent.current_version
	  AND dependency.target_kind = 'object'
	  AND dependency.target_object_id = ?
	LIMIT 1`

func normalizeWriterClock(value time.Time) (time.Time, error) {
	if value.IsZero() {
		return time.Time{}, errors.New("knowledge writer clock returned the zero time")
	}
	normalized := time.UnixMicro(value.UTC().UnixMicro()).UTC()
	if normalized.UnixMicro() < 1 || normalized.UnixMicro() > 253402300799999999 ||
		timestamppb.New(normalized).CheckValid() != nil {
		return time.Time{}, errors.New("knowledge writer clock returned an unsupported time")
	}
	return normalized, nil
}

func nextWriterTime(value time.Time, previousUnixMicro int64) (time.Time, error) {
	normalized, err := normalizeWriterClock(value)
	if err != nil {
		return time.Time{}, err
	}
	if normalized.UnixMicro() > previousUnixMicro {
		return normalized, nil
	}
	if previousUnixMicro >= 253402300799999999 {
		return time.Time{}, errors.New("knowledge writer timestamp space is exhausted")
	}
	return time.UnixMicro(previousUnixMicro + 1).UTC(), nil
}

func finishWriterTransaction(tx *gorm.DB, returnedErr *error) {
	if tx == nil {
		return
	}
	if err := tx.Rollback().Error; err != nil && !errors.Is(err, sql.ErrTxDone) &&
		!errors.Is(err, gorm.ErrInvalidTransaction) {
		rollbackErr := fmt.Errorf("roll back knowledge mutation: %w", err)
		if returnedErr != nil {
			*returnedErr = errors.Join(*returnedErr, rollbackErr)
		}
	}
}

func writerError(ctx context.Context, operation string, err error) error {
	if ctx != nil && ctx.Err() != nil {
		return ctx.Err()
	}
	if errors.Is(err, ErrCorrupt) || errors.Is(err, control.ErrNotFound) ||
		errors.Is(err, control.ErrAlreadyExists) || errors.Is(err, control.ErrVersionConflict) ||
		errors.Is(err, control.ErrDependencyConflict) || errors.Is(err, control.ErrCapacityExceeded) ||
		errors.Is(err, control.ErrInvalidArgument) {
		return err
	}
	return fmt.Errorf("knowledge writer %s: %w", strings.TrimSpace(operation), err)
}
