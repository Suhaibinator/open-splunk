package knowledgecatalog

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/audit"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/knowledge"
	"github.com/Suhaibinator/open-splunk/internal/knowledgedefinition"
	"gorm.io/gorm"
)

type persistedSelectorPattern struct {
	dimension string
	ordinal   int64
	matchKind string
	value     string
}

const publicationInsertBatchSize = 64

type persistedPublicationDependency struct {
	TenantID            string `gorm:"column:tenant_id"`
	SourceObjectID      string `gorm:"column:source_object_id"`
	SourceObjectVersion int64  `gorm:"column:source_object_version"`
	Ordinal             int64  `gorm:"column:ordinal"`
	TargetKind          string `gorm:"column:target_kind"`
	TargetObjectID      string `gorm:"column:target_object_id"`
	TargetObjectVersion int64  `gorm:"column:target_object_version"`
	DependencyRole      string `gorm:"column:dependency_role"`
}

func (persistedPublicationDependency) TableName() string {
	return "knowledge_object_dependencies"
}

type persistedPublicationSelector struct {
	TenantID          string `gorm:"column:tenant_id"`
	KnowledgeObjectID string `gorm:"column:knowledge_object_id"`
	ObjectVersion     int64  `gorm:"column:object_version"`
	Dimension         string `gorm:"column:dimension"`
	Ordinal           int64  `gorm:"column:ordinal"`
	MatchKind         string `gorm:"column:match_kind"`
	Value             string `gorm:"column:value"`
}

func (persistedPublicationSelector) TableName() string {
	return "knowledge_object_list_selector_patterns"
}

// mutationCommitAuthorityRecord is the immutable bridge between a historical
// catalog revision/token pair and the version/audit authorities that caused
// it. Unlike the current revision head, this row is retained after later
// mutations so replay cannot manufacture a snapshot identity by changing both
// mutable receipt copies together.
type mutationCommitAuthorityRecord struct {
	TenantID                 string          `gorm:"column:tenant_id"`
	ActorKind                audit.ActorKind `gorm:"column:actor_kind"`
	ActorID                  string          `gorm:"column:actor_id"`
	Route                    string          `gorm:"column:route"`
	ClientRequestID          string          `gorm:"column:client_request_id"`
	RequestDigest            []byte          `gorm:"column:request_digest"`
	CatalogRevision          int64           `gorm:"column:catalog_revision"`
	CatalogStateToken        []byte          `gorm:"column:catalog_state_token"`
	MutationKind             string          `gorm:"column:mutation_kind"`
	KnowledgeObjectID        string          `gorm:"column:knowledge_object_id"`
	ObjectVersion            int64           `gorm:"column:object_version"`
	OccurredAtUnixMicro      int64           `gorm:"column:occurred_at_unix_micro"`
	RetentionAnchorUnixMicro int64           `gorm:"column:retention_anchor_unix_micro"`
	RetainUntilUnixMicro     int64           `gorm:"column:retain_until_unix_micro"`
	SuccessfulAuditSequence  *int64          `gorm:"column:successful_audit_sequence"`
	RecoveryAuditSequence    *int64          `gorm:"column:recovery_audit_sequence"`
}

func (mutationCommitAuthorityRecord) TableName() string {
	return "knowledge_mutation_commit_authorities"
}

// publishMutation stages every immutable and current catalog authority,
// appends the successful general-audit event, advances the catalog branch
// identity once, and records the compact replay receipt. It deliberately does
// not commit: commitMutation owns the final ambiguity boundary.
func (writer *Writer) publishMutation(
	ctx context.Context,
	tx *gorm.DB,
	prepared preparedMutation,
	plan publicationPlan,
	blobExists bool,
) (Object, int64, []byte, error) {
	if err := validatePublicationInputs(writer, ctx, tx, prepared, plan); err != nil {
		return Object{}, 0, nil, err
	}
	dependencies, err := canonicalPublicationDependencies(plan)
	if err != nil {
		return Object{}, 0, nil, err
	}
	if !plan.activeTransition.IsZero() {
		dependencies, err = plan.activeTransition.validateAndProject(
			ctx,
			tx,
			plan.oldCatalogState,
			prepared.scope.tenantID,
			plan,
			dependencies,
		)
		if err != nil {
			return Object{}, 0, nil, err
		}
	}
	hookEvent := func(boundary writerHookBoundary) writerHookEvent {
		return writerHookEvent{
			Boundary:          boundary,
			Route:             plan.route,
			KnowledgeObjectID: plan.objectID,
			Version:           uint64(plan.version), // validated above
			IDAttempt:         plan.idAttempt,
		}
	}

	if !blobExists {
		result := tx.Exec(`INSERT INTO knowledge_definition_blobs (
			tenant_id, definition_digest, definition_proto,
			definition_bytes, created_at_unix_micro
		) VALUES (?, ?, ?, ?, ?)`,
			prepared.scope.tenantID,
			plan.definition.digest,
			plan.definition.bytes,
			len(plan.definition.bytes),
			plan.updatedAt.UnixMicro(),
		)
		if result.Error != nil {
			return Object{}, 0, nil, writerError(ctx, "insert definition blob", result.Error)
		}
	}
	if err := writer.callHook(ctx, hookEvent(writerHookDefinitionBlobReady)); err != nil {
		return Object{}, 0, nil, err
	}

	version := versionRecord{
		TenantID:           prepared.scope.tenantID,
		KnowledgeObjectID:  plan.objectID,
		ObjectVersion:      plan.version,
		AppID:              plan.definition.appID,
		OwnerID:            plan.ownerID,
		ObjectType:         plan.definition.objectType,
		Name:               plan.definition.name,
		SharingScope:       plan.definition.sharingScope,
		State:              plan.state,
		DefinitionDigest:   bytes.Clone(plan.definition.digest),
		DependencyCount:    int64(len(dependencies)),
		MutationKind:       plan.mutationKind,
		CreatedAtUnixMicro: plan.updatedAt.UnixMicro(),
	}
	if err := tx.Create(&version).Error; err != nil {
		return Object{}, 0, nil, writerError(ctx, "insert immutable version", err)
	}
	if err := writer.callHook(ctx, hookEvent(writerHookVersionInserted)); err != nil {
		return Object{}, 0, nil, err
	}

	persistedDependencies := make([]persistedPublicationDependency, len(dependencies))
	for ordinal, dependency := range dependencies {
		persistedDependencies[ordinal] = persistedPublicationDependency{
			TenantID:            prepared.scope.tenantID,
			SourceObjectID:      plan.objectID,
			SourceObjectVersion: plan.version,
			Ordinal:             int64(ordinal),
			TargetKind:          "object",
			TargetObjectID:      dependency.targetObjectID,
			TargetObjectVersion: dependency.targetVersion,
			DependencyRole:      "field_input",
		}
	}
	if len(persistedDependencies) > 0 {
		if err := tx.Session(&gorm.Session{SkipDefaultTransaction: true}).
			CreateInBatches(&persistedDependencies, publicationInsertBatchSize).Error; err != nil {
			return Object{}, 0, nil, writerError(ctx, "insert immutable dependencies", err)
		}
	}
	if err := writer.callHook(ctx, hookEvent(writerHookDependencyRowsInserted)); err != nil {
		return Object{}, 0, nil, err
	}
	if result := tx.Exec(`INSERT INTO knowledge_object_dependency_seals (
		tenant_id, knowledge_object_id, object_version, dependency_count
	) VALUES (?, ?, ?, ?)`,
		prepared.scope.tenantID, plan.objectID, plan.version, len(dependencies),
	); result.Error != nil {
		return Object{}, 0, nil, writerError(ctx, "seal immutable dependencies", result.Error)
	}
	if err := writer.callHook(ctx, hookEvent(writerHookDependencySealed)); err != nil {
		return Object{}, 0, nil, err
	}

	patterns, selectorValueBytes, canonicalSelectorBytes, counts, err := publicationSelectorRows(plan.definition)
	if err != nil {
		return Object{}, 0, nil, err
	}
	descriptionPresent := int64(0)
	description := ""
	if plan.definition.description != nil {
		descriptionPresent = 1
		description = *plan.definition.description
	}
	projectionBytes := int64(len(description)) + selectorValueBytes
	if projectionBytes < 0 || projectionBytes > int64(maximumProjectionBytes) {
		return Object{}, 0, nil, control.ErrCapacityExceeded
	}
	if result := tx.Exec(`INSERT INTO knowledge_object_list_projections (
		tenant_id, knowledge_object_id, object_version,
		app_id, owner_id, object_type, name, sharing_scope, state,
		description_present, description,
		index_selector_count, host_selector_count,
		source_selector_count, sourcetype_selector_count,
		selector_value_bytes, canonical_selector_bytes
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		prepared.scope.tenantID,
		plan.objectID,
		plan.version,
		plan.definition.appID,
		plan.ownerID,
		plan.definition.objectType,
		plan.definition.name,
		plan.definition.sharingScope,
		plan.state,
		descriptionPresent,
		description,
		counts[knowledge.DimensionIndex],
		counts[knowledge.DimensionHost],
		counts[knowledge.DimensionSource],
		counts[knowledge.DimensionSourcetype],
		selectorValueBytes,
		canonicalSelectorBytes,
	); result.Error != nil {
		return Object{}, 0, nil, writerError(ctx, "insert list projection", result.Error)
	}
	if err := writer.callHook(ctx, hookEvent(writerHookProjectionInserted)); err != nil {
		return Object{}, 0, nil, err
	}
	persistedSelectors := make([]persistedPublicationSelector, len(patterns))
	for index, pattern := range patterns {
		persistedSelectors[index] = persistedPublicationSelector{
			TenantID:          prepared.scope.tenantID,
			KnowledgeObjectID: plan.objectID,
			ObjectVersion:     plan.version,
			Dimension:         pattern.dimension,
			Ordinal:           pattern.ordinal,
			MatchKind:         pattern.matchKind,
			Value:             pattern.value,
		}
	}
	if len(persistedSelectors) > 0 {
		if err := tx.Session(&gorm.Session{SkipDefaultTransaction: true}).
			CreateInBatches(&persistedSelectors, publicationInsertBatchSize).Error; err != nil {
			return Object{}, 0, nil, writerError(ctx, "insert list selectors", err)
		}
	}
	if err := writer.callHook(ctx, hookEvent(writerHookSelectorRowsInserted)); err != nil {
		return Object{}, 0, nil, err
	}
	if result := tx.Exec(`INSERT INTO knowledge_object_list_projection_seals (
		tenant_id, knowledge_object_id, object_version,
		projection_bytes, canonical_selector_bytes
	) VALUES (?, ?, ?, ?, ?)`,
		prepared.scope.tenantID,
		plan.objectID,
		plan.version,
		projectionBytes,
		canonicalSelectorBytes,
	); result.Error != nil {
		return Object{}, 0, nil, writerError(ctx, "seal list projection", result.Error)
	}
	if err := validatePublicationOrderKey(tx, prepared.scope.tenantID, plan); err != nil {
		return Object{}, 0, nil, err
	}
	if err := writer.callHook(ctx, hookEvent(writerHookProjectionSealed)); err != nil {
		return Object{}, 0, nil, err
	}

	registry := registryRecord{
		TenantID:            prepared.scope.tenantID,
		KnowledgeObjectID:   plan.objectID,
		CurrentVersion:      plan.version,
		AppID:               plan.definition.appID,
		OwnerID:             plan.ownerID,
		ObjectType:          plan.definition.objectType,
		Name:                plan.definition.name,
		SharingScope:        plan.definition.sharingScope,
		State:               plan.state,
		DefinitionDigest:    bytes.Clone(plan.definition.digest),
		CreatedAtUnixMicro:  plan.createdAt.UnixMicro(),
		UpdatedAtUnixMicro:  plan.updatedAt.UnixMicro(),
		DisabledAtUnixMicro: optionalUnixMicro(plan.disabledAt),
		DeletedAtUnixMicro:  optionalUnixMicro(plan.deletedAt),
	}
	if plan.current == nil {
		if err := tx.Create(&registry).Error; err != nil {
			return Object{}, 0, nil, writerError(ctx, "publish registry identity", err)
		}
	} else {
		result := tx.Model(&registryRecord{}).
			Where(`tenant_id = ? AND knowledge_object_id = ? AND current_version = ?
				AND owner_id = ? AND app_id = ? AND state = ?`,
				plan.current.TenantID,
				plan.current.KnowledgeObjectID,
				plan.current.CurrentVersion,
				plan.current.OwnerID,
				plan.current.AppID,
				plan.current.State,
			).
			Updates(map[string]any{
				"current_version":           registry.CurrentVersion,
				"app_id":                    registry.AppID,
				"owner_id":                  registry.OwnerID,
				"object_type":               registry.ObjectType,
				"name":                      registry.Name,
				"sharing_scope":             registry.SharingScope,
				"state":                     registry.State,
				"definition_digest":         registry.DefinitionDigest,
				"updated_at_unix_micro":     registry.UpdatedAtUnixMicro,
				"disabled_at_unix_micro":    registry.DisabledAtUnixMicro,
				"quarantined_at_unix_micro": nil,
				"deleted_at_unix_micro":     registry.DeletedAtUnixMicro,
				"quarantine_reason":         nil,
			})
		if result.Error != nil {
			return Object{}, 0, nil, writerError(ctx, "advance registry identity", result.Error)
		}
		if result.RowsAffected != 1 {
			return Object{}, 0, nil, control.ErrVersionConflict
		}
	}
	if err := writer.callHook(ctx, hookEvent(writerHookRegistryPublished)); err != nil {
		return Object{}, 0, nil, err
	}

	object, err := writer.reader.objectFromCurrentRegistry(tx, registry)
	if err != nil {
		return Object{}, 0, nil, err
	}
	auditEvent, err := writer.auditAppender.AppendInTransaction(ctx, tx, prepared.scope.tenantID, audit.SuccessfulEvent{
		OccurredAt:    plan.updatedAt,
		Action:        plan.auditAction,
		TargetKind:    audit.TargetKindKnowledgeObject,
		TargetID:      plan.objectID,
		TargetVersion: uint64(plan.version),
		KnowledgeObject: audit.KnowledgeObjectMetadata{
			AppID:        plan.definition.appID,
			ObjectType:   plan.definition.objectType,
			SharingScope: plan.definition.sharingScope,
		},
	})
	if err != nil {
		return Object{}, 0, nil, writerError(ctx, "append successful audit event", err)
	}
	if err := validatePublicationAudit(prepared, plan, auditEvent); err != nil {
		return Object{}, 0, nil, err
	}
	if err := writer.callHook(ctx, hookEvent(writerHookSuccessAuditAppended)); err != nil {
		return Object{}, 0, nil, err
	}

	advanced, err := advancePublicationCatalogState(tx, prepared.scope.tenantID, plan.oldCatalogState)
	if err != nil {
		return Object{}, 0, nil, err
	}
	if err := writer.callHook(ctx, hookEvent(writerHookCatalogRevisionAdvanced)); err != nil {
		return Object{}, 0, nil, err
	}
	advancedToken, err := catalogStateTokenValue(advanced)
	if err != nil {
		return Object{}, 0, nil, err
	}

	mutationMicros := plan.updatedAt.UnixMicro()
	auditSequence := int64(auditEvent.Sequence) // bounded by audit validation
	retentionMicros := int64(writer.idempotencyRetention / time.Microsecond)
	retentionAnchor, err := writerRetentionAnchor(tx, mutationMicros)
	if err != nil {
		return Object{}, 0, nil, err
	}
	if retentionMicros < int64(minimumIdempotencyRetention/time.Microsecond) ||
		retentionAnchor > 253402300799999999-retentionMicros {
		return Object{}, 0, nil, control.ErrCapacityExceeded
	}
	retainUntil := retentionAnchor + retentionMicros
	commitAuthority := mutationCommitAuthorityRecord{
		TenantID:                 prepared.scope.tenantID,
		ActorKind:                prepared.actor.Kind,
		ActorID:                  prepared.actor.ID,
		Route:                    plan.route,
		ClientRequestID:          prepared.clientRequestID,
		RequestDigest:            bytes.Clone(prepared.requestDigest[:]),
		CatalogRevision:          advanced.revision,
		CatalogStateToken:        bytes.Clone(advancedToken),
		MutationKind:             plan.mutationKind,
		KnowledgeObjectID:        plan.objectID,
		ObjectVersion:            plan.version,
		OccurredAtUnixMicro:      mutationMicros,
		RetentionAnchorUnixMicro: retentionAnchor,
		RetainUntilUnixMicro:     retainUntil,
		SuccessfulAuditSequence:  &auditSequence,
	}
	if err := tx.Create(&commitAuthority).Error; err != nil {
		return Object{}, 0, nil, writerError(ctx, "record immutable mutation commit authority", err)
	}
	if err := writer.callHook(ctx, hookEvent(writerHookCommitAuthorityRecorded)); err != nil {
		return Object{}, 0, nil, err
	}

	outcome, err := encodeOutcomeReference(mutationOutcomeAuthority{
		route:                    plan.route,
		mutationKind:             plan.mutationKind,
		objectID:                 plan.objectID,
		version:                  uint64(plan.version),
		digest:                   plan.definition.digest,
		catalogRevision:          uint64(advanced.revision),
		catalogStateToken:        advancedToken,
		successfulAuditSequence:  uint64(auditSequence),
		occurredAtUnixMicro:      mutationMicros,
		retentionAnchorUnixMicro: retentionAnchor,
		retainUntilUnixMicro:     retainUntil,
	})
	if err != nil {
		return Object{}, 0, nil, err
	}
	receipt := idempotencyRecord{
		TenantID:                   prepared.scope.tenantID,
		ActorKind:                  prepared.actor.Kind,
		ActorID:                    prepared.actor.ID,
		Route:                      plan.route,
		ClientRequestID:            prepared.clientRequestID,
		MutationKind:               plan.mutationKind,
		RequestDigestFormatVersion: mutationRequestDigestFormatVersion,
		RequestDigest:              bytes.Clone(prepared.requestDigest[:]),
		OutcomeFormatVersion:       mutationOutcomeFormatVersion,
		OutcomeProto:               outcome,
		CommittedCatalogRevision:   advanced.revision,
		CommittedCatalogStateToken: advancedToken,
		KnowledgeObjectID:          plan.objectID,
		ObjectVersion:              plan.version,
		SuccessfulAuditSequence:    &auditSequence,
		CreatedAtUnixMicro:         mutationMicros,
		RetentionAnchorUnixMicro:   retentionAnchor,
		RetainUntilUnixMicro:       retainUntil,
	}
	if err := tx.Create(&receipt).Error; err != nil {
		return Object{}, 0, nil, writerError(ctx, "record idempotent mutation outcome", err)
	}
	if err := writer.callHook(ctx, hookEvent(writerHookIdempotencyOutcomeRecorded)); err != nil {
		return Object{}, 0, nil, err
	}
	return object, advanced.revision, bytes.Clone(advancedToken), nil
}

func writerRetentionAnchor(tx *gorm.DB, mutationMicros int64) (int64, error) {
	if tx == nil {
		return 0, fmt.Errorf("%w: knowledge retention clock is unavailable", ErrCorrupt)
	}
	if _, err := canonicalTime(mutationMicros); err != nil {
		return 0, fmt.Errorf("%w: knowledge mutation occurrence is invalid", ErrCorrupt)
	}
	var databaseMicros int64
	if err := tx.Raw(`SELECT CAST(unixepoch('subsec') * 1000000 AS INTEGER)`).Scan(&databaseMicros).Error; err != nil {
		return 0, writerError(tx.Statement.Context, "read retention clock", err)
	}
	if _, err := canonicalTime(databaseMicros); err != nil {
		return 0, fmt.Errorf("%w: knowledge retention clock is invalid", ErrCorrupt)
	}
	if mutationMicros > databaseMicros {
		return mutationMicros, nil
	}
	return databaseMicros, nil
}

type reconciledMutationOutcome struct {
	create   *opensplunkv1.CreateKnowledgeObjectResponse
	update   *opensplunkv1.UpdateKnowledgeObjectResponse
	setState *opensplunkv1.SetKnowledgeObjectStateResponse
	delete   *opensplunkv1.DeleteKnowledgeObjectResponse
}

func (writer *Writer) commitMutation(
	ctx context.Context,
	tx *gorm.DB,
	prepared preparedMutation,
	plan publicationPlan,
) (reconciledMutationOutcome, error) {
	if writer == nil || tx == nil {
		return reconciledMutationOutcome{}, fmt.Errorf("%w: knowledge writer transaction is unavailable", control.ErrInvalidArgument)
	}
	event := writerHookEvent{
		Route:             plan.route,
		KnowledgeObjectID: plan.objectID,
		Version:           uint64(plan.version),
		IDAttempt:         plan.idAttempt,
	}
	event.Boundary = writerHookBeforeCommit
	if err := writer.callHook(ctx, event); err != nil {
		return reconciledMutationOutcome{}, withErrorDisposition(
			err,
			ErrorDispositionDefinitiveRejection,
		)
	}
	commit := writer.commit
	if commit == nil {
		commit = func(transaction *gorm.DB) error { return transaction.Commit().Error }
	}
	reconciled := reconciledMutationOutcome{}
	if err := commit(tx); err != nil {
		commitErr := writerError(ctx, "commit mutation", err)
		var reconcileErr error
		reconciled, reconcileErr = writer.reconcileCommittedMutation(ctx, prepared, plan)
		if reconcileErr != nil {
			return reconciledMutationOutcome{}, withErrorDisposition(
				errors.Join(
					commitErr,
					fmt.Errorf("reconcile ambiguous knowledge mutation commit: %w", reconcileErr),
				),
				ErrorDispositionIndeterminate,
			)
		}
	}
	event.Boundary = writerHookAfterCommit
	if err := writer.callHook(ctx, event); err != nil {
		// The commit is durable. Returning the hook error intentionally models a
		// lost response; an exact retry is resolved from the compact receipt.
		return reconciledMutationOutcome{}, withErrorDisposition(
			err,
			ErrorDispositionKnownCommitted,
		)
	}
	return reconciled, nil
}

func (writer *Writer) reconcileCommittedMutation(
	ctx context.Context,
	prepared preparedMutation,
	plan publicationPlan,
) (outcome reconciledMutationOutcome, returnedErr error) {
	var authorized *AuthorizedContext
	defer func() {
		if returnedErr != nil && authorized != nil {
			returnedErr = withAuthorizedContext(returnedErr, *authorized)
		}
	}()
	if writer == nil || writer.orm == nil || !validPreparedReplayIdentity(&prepared, plan.route) {
		return outcome, fmt.Errorf("%w: knowledge commit reconciliation authority is invalid", control.ErrInvalidArgument)
	}
	base := context.Background()
	if ctx != nil {
		base = context.WithoutCancel(ctx)
	}
	reconcileContext, cancel := context.WithTimeout(base, writerCommitReconcileTimeout)
	defer cancel()
	tx := writer.orm.WithContext(reconcileContext).Begin()
	if tx.Error != nil {
		return outcome, tx.Error
	}
	defer finishWriterTransaction(tx, &returnedErr)
	record, found, replayAuthorized, err := writer.readAuthorizedIdempotencyRecord(
		reconcileContext,
		tx,
		&prepared,
		plan.route,
	)
	if err != nil {
		return outcome, err
	}
	authorized = replayAuthorized
	if !found ||
		(plan.route != mutationRouteCreate && record.KnowledgeObjectID != plan.objectID) ||
		record.ObjectVersion != plan.version ||
		record.MutationKind != plan.mutationKind {
		return outcome, fmt.Errorf("knowledge mutation commit outcome is not durably observable")
	}
	switch plan.route {
	case mutationRouteCreate:
		outcome.create, err = writer.replayCreate(reconcileContext, tx, prepared, record)
	case mutationRouteUpdate:
		outcome.update, err = writer.replayUpdate(reconcileContext, tx, prepared, record)
	case mutationRouteSetState:
		outcome.setState, err = writer.replaySetState(reconcileContext, tx, prepared, record)
	case mutationRouteDelete:
		outcome.delete, err = writer.replayDelete(reconcileContext, tx, prepared, record)
	default:
		err = fmt.Errorf("%w: knowledge commit reconciliation route is invalid", control.ErrInvalidArgument)
	}
	if err != nil {
		return reconciledMutationOutcome{}, err
	}
	if err := tx.Commit().Error; err != nil {
		return reconciledMutationOutcome{}, err
	}
	return outcome, nil
}

func validatePublicationInputs(
	writer *Writer,
	ctx context.Context,
	tx *gorm.DB,
	prepared preparedMutation,
	plan publicationPlan,
) error {
	if writer == nil || writer.reader == nil || writer.auditAppender == nil || tx == nil {
		return fmt.Errorf("%w: knowledge writer publication is unavailable", control.ErrInvalidArgument)
	}
	if err := validateContext(ctx); err != nil {
		return err
	}
	invalid := func(reason string) error {
		return fmt.Errorf("%w: knowledge publication plan %s", control.ErrInvalidArgument, reason)
	}
	if plan.route == "" || plan.route != routeForMutationKind(plan.mutationKind) {
		return invalid("route is invalid")
	}
	if !validPreparedReplayIdentity(&prepared, plan.route) {
		return invalid("request authority is invalid")
	}
	if !validIdentity(plan.objectID, maximumObjectIDBytes) || plan.version < 1 {
		return invalid("object identity is invalid")
	}
	if len(plan.dependencies) > maximumDependencyGraphEdges {
		return invalid("dependency count is invalid")
	}
	if plan.state == StateActive && plan.activeTransition.IsZero() {
		return invalid("ACTIVE transition authority is absent")
	}
	if !plan.state.valid() || plan.state == StateQuarantined {
		return invalid("state is invalid")
	}
	if plan.definition.definition == nil || plan.definition.selector == nil ||
		len(plan.definition.bytes) < 1 || len(plan.definition.bytes) > knowledgedefinition.MaximumCanonicalBytes ||
		len(plan.definition.digest) != persistedKnowledgeDefinitionDigestBytes {
		return invalid("definition authority is invalid")
	}
	if !validIdentity(plan.ownerID, maximumOwnerIDBytes) || plan.ownerID != prepared.scope.ownerID {
		return invalid("owner authority is invalid")
	}
	if !prepared.scope.canWriteApp(plan.definition.appID) {
		return invalid("app authority is invalid")
	}
	if _, err := catalogStateTokenValue(plan.oldCatalogState); err != nil {
		return invalid("catalog state authority is invalid")
	}
	created, createdOK := audit.CanonicalOccurrenceTime(plan.createdAt)
	updated, updatedOK := audit.CanonicalOccurrenceTime(plan.updatedAt)
	if !createdOK || !updatedOK || !created.Equal(plan.createdAt) || !updated.Equal(plan.updatedAt) || updated.Before(created) {
		return fmt.Errorf("%w: knowledge publication chronology is invalid", control.ErrInvalidArgument)
	}
	if plan.current == nil {
		if plan.version != 1 || plan.mutationKind != "create" || plan.state != StateDraft ||
			!plan.createdAt.Equal(plan.updatedAt) || plan.disabledAt != nil || plan.deletedAt != nil {
			return fmt.Errorf("%w: knowledge create plan is invalid", control.ErrInvalidArgument)
		}
	} else if plan.version != plan.current.CurrentVersion+1 ||
		plan.current.TenantID != prepared.scope.tenantID ||
		plan.current.KnowledgeObjectID != plan.objectID ||
		plan.current.OwnerID != plan.ownerID ||
		plan.createdAt.UnixMicro() != plan.current.CreatedAtUnixMicro {
		return fmt.Errorf("%w: knowledge update plan is invalid", control.ErrInvalidArgument)
	}
	return nil
}

func canonicalPublicationDependencies(plan publicationPlan) ([]publicationDependency, error) {
	invalid := func() ([]publicationDependency, error) {
		return nil, fmt.Errorf(
			"%w: knowledge publication dependency set is invalid",
			control.ErrInvalidArgument,
		)
	}
	if len(plan.dependencies) > maximumDependencyGraphEdges {
		return invalid()
	}
	result := make([]publicationDependency, len(plan.dependencies))
	for index, dependency := range plan.dependencies {
		if !validIdentity(dependency.targetObjectID, maximumObjectIDBytes) ||
			dependency.targetVersion < 1 ||
			(dependency.targetObjectID == plan.objectID && dependency.targetVersion == plan.version) {
			return invalid()
		}
		result[index] = publicationDependency{
			targetObjectID: strings.Clone(dependency.targetObjectID),
			targetVersion:  dependency.targetVersion,
		}
	}
	sort.Slice(result, func(left, right int) bool {
		if result[left].targetObjectID != result[right].targetObjectID {
			return result[left].targetObjectID < result[right].targetObjectID
		}
		return result[left].targetVersion < result[right].targetVersion
	})
	for index := 1; index < len(result); index++ {
		if result[index] == result[index-1] {
			return invalid()
		}
	}
	return result, nil
}

func publicationTransitionPersistenceBindingFromPlan(
	ctx context.Context,
	tx *gorm.DB,
	tenantID string,
	plan publicationPlan,
	dependencies []publicationDependency,
) (publicationTransitionPersistenceBinding, error) {
	if err := validateContext(ctx); err != nil {
		return publicationTransitionPersistenceBinding{}, err
	}
	after, err := publicationTransitionPersistenceEndpointFromPlan(plan, dependencies)
	if err != nil {
		return publicationTransitionPersistenceBinding{}, err
	}
	binding := publicationTransitionPersistenceBinding{
		tenantID:     strings.Clone(tenantID),
		after:        after,
		dependencies: dependencies,
	}
	database := tx.WithContext(ctx)
	registryQuery := database.Model(&registryRecord{}).Where(
		"tenant_id = ? AND knowledge_object_id = ?",
		tenantID,
		plan.objectID,
	)
	current, found, err := readRegistryRecord(registryQuery)
	if err != nil {
		return publicationTransitionPersistenceBinding{}, err
	}
	if plan.current == nil {
		if found {
			return publicationTransitionPersistenceBinding{}, fmt.Errorf(
				"%w: publication transition create identity is already current",
				control.ErrDependencyConflict,
			)
		}
		return binding, nil
	}
	if !found {
		return publicationTransitionPersistenceBinding{}, fmt.Errorf(
			"%w: publication transition current registry is missing",
			ErrCorrupt,
		)
	}
	if err := validateRegistry(current); err != nil {
		return publicationTransitionPersistenceBinding{}, err
	}
	version, found, err := readVersionRecord(
		database,
		current.TenantID,
		current.KnowledgeObjectID,
		current.CurrentVersion,
	)
	if err != nil {
		return publicationTransitionPersistenceBinding{}, err
	}
	if !found || validateCurrentVersion(current, version) != nil {
		return publicationTransitionPersistenceBinding{}, fmt.Errorf(
			"%w: publication transition current registry and version disagree",
			ErrCorrupt,
		)
	}
	if !publicationTransitionPlanCurrentEqual(*plan.current, current) {
		return publicationTransitionPersistenceBinding{}, fmt.Errorf(
			"%w: publication transition plan current endpoint changed",
			control.ErrDependencyConflict,
		)
	}
	records, err := readValidatedVersionDependencies(database, version)
	if err != nil {
		return publicationTransitionPersistenceBinding{}, err
	}
	before, err := publicationTransitionPersistenceEndpointFromCurrent(current, records)
	if err != nil {
		return publicationTransitionPersistenceBinding{}, err
	}
	binding.before = before
	return binding, nil
}

func publicationTransitionPersistenceEndpointFromPlan(
	plan publicationPlan,
	dependencies []publicationDependency,
) (publicationTransitionPersistenceEndpoint, error) {
	objectType, typeValid := objectTypeToProto(plan.definition.objectType)
	sharingScope, scopeValid := knowledgeSharingScopeToProto(plan.definition.sharingScope)
	if !typeValid || !scopeValid || len(plan.definition.digest) != sha256.Size {
		return publicationTransitionPersistenceEndpoint{}, fmt.Errorf(
			"%w: publication transition plan endpoint is invalid",
			control.ErrInvalidArgument,
		)
	}
	if plan.state == StateActive && len(dependencies) != 0 {
		return publicationTransitionPersistenceEndpoint{}, fmt.Errorf(
			"%w: ACTIVE publication transition plan submits dependency rows",
			control.ErrInvalidArgument,
		)
	}
	result := publicationTransitionPersistenceEndpoint{
		present:      true,
		state:        plan.state,
		objectID:     strings.Clone(plan.objectID),
		version:      plan.version,
		objectType:   objectType,
		name:         strings.Clone(plan.definition.name),
		appID:        strings.Clone(plan.definition.appID),
		ownerID:      strings.Clone(plan.ownerID),
		sharingScope: sharingScope,
	}
	copy(result.definitionDigest[:], plan.definition.digest)
	if plan.state != StateActive {
		result.existingDependenciesPresent = true
		result.existingDependencies = publicationTransitionPersistedDependencies(dependencies)
	}
	if !validPublicationTransitionPersistenceEndpoint(result) {
		return publicationTransitionPersistenceEndpoint{}, fmt.Errorf(
			"%w: publication transition plan endpoint is invalid",
			control.ErrInvalidArgument,
		)
	}
	return result, nil
}

func publicationTransitionPersistenceEndpointFromCurrent(
	current registryRecord,
	dependencies []dependencyRecord,
) (publicationTransitionPersistenceEndpoint, error) {
	objectType, typeValid := objectTypeToProto(current.ObjectType)
	sharingScope, scopeValid := knowledgeSharingScopeToProto(current.SharingScope)
	if !typeValid || !scopeValid || len(current.DefinitionDigest) != sha256.Size {
		return publicationTransitionPersistenceEndpoint{}, fmt.Errorf(
			"%w: publication transition current endpoint is invalid",
			ErrCorrupt,
		)
	}
	result := publicationTransitionPersistenceEndpoint{
		present:                     true,
		state:                       current.State,
		objectID:                    strings.Clone(current.KnowledgeObjectID),
		version:                     current.CurrentVersion,
		objectType:                  objectType,
		name:                        strings.Clone(current.Name),
		appID:                       strings.Clone(current.AppID),
		ownerID:                     strings.Clone(current.OwnerID),
		sharingScope:                sharingScope,
		existingDependenciesPresent: true,
		existingDependencies: make(
			[]publicationPersistedDependency,
			len(dependencies),
		),
	}
	copy(result.definitionDigest[:], current.DefinitionDigest)
	for index, dependency := range dependencies {
		result.existingDependencies[index] = publicationPersistedDependency{
			ordinal:        dependency.Ordinal,
			targetObjectID: strings.Clone(dependency.TargetObjectID),
			targetVersion:  dependency.TargetObjectVersion,
			role:           opensplunkv1.KnowledgeDependencyRole_KNOWLEDGE_DEPENDENCY_ROLE_FIELD_INPUT,
		}
	}
	if !validPublicationTransitionPersistenceEndpoint(result) {
		return publicationTransitionPersistenceEndpoint{}, fmt.Errorf(
			"%w: publication transition current endpoint is invalid",
			ErrCorrupt,
		)
	}
	return result, nil
}

func publicationTransitionPersistedDependencies(
	dependencies []publicationDependency,
) []publicationPersistedDependency {
	result := make([]publicationPersistedDependency, len(dependencies))
	for index, dependency := range dependencies {
		result[index] = publicationPersistedDependency{
			ordinal:        int64(index),
			targetObjectID: strings.Clone(dependency.targetObjectID),
			targetVersion:  dependency.targetVersion,
			role:           opensplunkv1.KnowledgeDependencyRole_KNOWLEDGE_DEPENDENCY_ROLE_FIELD_INPUT,
		}
	}
	return result
}

func publicationTransitionPlanCurrentEqual(left, right registryRecord) bool {
	return left.TenantID == right.TenantID &&
		left.KnowledgeObjectID == right.KnowledgeObjectID &&
		left.CurrentVersion == right.CurrentVersion &&
		left.AppID == right.AppID &&
		left.OwnerID == right.OwnerID &&
		left.ObjectType == right.ObjectType &&
		left.Name == right.Name &&
		left.SharingScope == right.SharingScope &&
		left.State == right.State &&
		bytes.Equal(left.DefinitionDigest, right.DefinitionDigest) &&
		left.CreatedAtUnixMicro == right.CreatedAtUnixMicro &&
		left.UpdatedAtUnixMicro == right.UpdatedAtUnixMicro &&
		equalOptionalInt64(left.DisabledAtUnixMicro, right.DisabledAtUnixMicro) &&
		equalOptionalInt64(left.QuarantinedAtUnixMicro, right.QuarantinedAtUnixMicro) &&
		equalOptionalInt64(left.DeletedAtUnixMicro, right.DeletedAtUnixMicro) &&
		equalOptionalString(left.QuarantineReason, right.QuarantineReason)
}

func publicationSelectorRows(
	authority definitionAuthority,
) ([]persistedSelectorPattern, int64, int64, map[knowledge.Dimension]int64, error) {
	if authority.selector == nil {
		return nil, 0, 0, nil, fmt.Errorf("%w: knowledge selector is absent", control.ErrInvalidArgument)
	}
	result := make([]persistedSelectorPattern, 0, knowledge.MaximumSelectorPatterns)
	counts := make(map[knowledge.Dimension]int64, knowledge.MaximumSelectorDimensions)
	var valueBytes int64
	for _, dimension := range []knowledge.Dimension{
		knowledge.DimensionIndex,
		knowledge.DimensionHost,
		knowledge.DimensionSource,
		knowledge.DimensionSourcetype,
	} {
		values := authority.selector.Patterns(dimension)
		counts[dimension] = int64(len(values))
		for ordinal, value := range values {
			pattern, err := knowledge.NormalizePattern(value)
			if err != nil || pattern.String() != value {
				return nil, 0, 0, nil, fmt.Errorf("%w: normalized selector is invalid", control.ErrInvalidArgument)
			}
			matchKind := "wildcard"
			if pattern.IsLiteral() {
				matchKind = "exact"
			}
			if valueBytes > math.MaxInt64-int64(len(value)) {
				return nil, 0, 0, nil, control.ErrCapacityExceeded
			}
			valueBytes += int64(len(value))
			result = append(result, persistedSelectorPattern{
				dimension: dimension.String(),
				ordinal:   int64(ordinal),
				matchKind: matchKind,
				value:     strings.Clone(value),
			})
		}
	}
	canonicalBytes := int64(len(authority.selector.CanonicalBytes()))
	if len(result) > knowledge.MaximumSelectorPatterns ||
		valueBytes > knowledge.MaximumSelectorNormalizedBytes ||
		canonicalBytes > knowledge.MaximumSelectorNormalizedBytes {
		return nil, 0, 0, nil, control.ErrCapacityExceeded
	}
	return result, valueBytes, canonicalBytes, counts, nil
}

func validatePublicationOrderKey(tx *gorm.DB, tenantID string, plan publicationPlan) error {
	var records []struct {
		CreatedAtUnixMicro int64 `gorm:"column:created_at_unix_micro"`
		UpdatedAtUnixMicro int64 `gorm:"column:updated_at_unix_micro"`
	}
	if err := tx.Table("knowledge_object_list_order_keys").Select(
		"created_at_unix_micro, updated_at_unix_micro",
	).Where(
		"tenant_id = ? AND knowledge_object_id = ? AND object_version = ?",
		tenantID, plan.objectID, plan.version,
	).Limit(2).Find(&records).Error; err != nil {
		return writerError(tx.Statement.Context, "read list order key", err)
	}
	if len(records) != 1 || records[0].CreatedAtUnixMicro != plan.createdAt.UnixMicro() ||
		records[0].UpdatedAtUnixMicro != plan.updatedAt.UnixMicro() {
		return fmt.Errorf("%w: knowledge list order key disagrees with publication", ErrCorrupt)
	}
	return nil
}

func validatePublicationAudit(
	prepared preparedMutation,
	plan publicationPlan,
	event audit.Event,
) error {
	if err := event.ValidateForTenant(prepared.scope.tenantID); err != nil ||
		event.Sequence < 1 || event.Sequence > math.MaxInt64 ||
		event.OccurredAt != plan.updatedAt ||
		event.Actor.Kind != prepared.actor.Kind || event.Actor.ID != prepared.actor.ID ||
		event.Actor.Role != prepared.actor.Role ||
		event.Action != plan.auditAction || event.TargetKind != audit.TargetKindKnowledgeObject ||
		event.TargetID != plan.objectID || event.TargetVersion != uint64(plan.version) ||
		event.KnowledgeObject.AppID != plan.definition.appID ||
		event.KnowledgeObject.ObjectType != plan.definition.objectType ||
		event.KnowledgeObject.SharingScope != plan.definition.sharingScope {
		return fmt.Errorf("%w: successful knowledge audit authority is invalid", ErrCorrupt)
	}
	return nil
}

func advancePublicationCatalogState(
	tx *gorm.DB,
	tenantID string,
	old catalogState,
) (catalogState, error) {
	if !old.found || old.revision < 0 || old.revision >= math.MaxInt64-1 {
		return catalogState{}, fmt.Errorf("%w: prior knowledge catalog state is invalid", ErrCorrupt)
	}
	if _, err := catalogStateTokenValue(old); err != nil {
		return catalogState{}, err
	}
	result := tx.Exec(`UPDATE knowledge_catalog_tenants
		SET catalog_revision = catalog_revision + 1
		WHERE tenant_id = ? AND catalog_revision = ?`, tenantID, old.revision)
	if result.Error != nil {
		return catalogState{}, writerError(tx.Statement.Context, "advance catalog revision", result.Error)
	}
	if result.RowsAffected != 1 {
		return catalogState{}, control.ErrVersionConflict
	}
	advanced, err := readCatalogState(tx, tenantID)
	if err != nil {
		return catalogState{}, err
	}
	if !advanced.found || advanced.revision != old.revision+1 || advanced.token == old.token {
		return catalogState{}, fmt.Errorf("%w: knowledge catalog branch did not advance exactly once", ErrCorrupt)
	}
	if _, err := catalogStateTokenValue(advanced); err != nil {
		return catalogState{}, err
	}
	return advanced, nil
}

func catalogStateTokenValue(state catalogState) ([]byte, error) {
	if !state.found {
		return nil, fmt.Errorf("%w: knowledge catalog branch identity is absent", ErrCorrupt)
	}
	decoded, err := base64.RawURLEncoding.DecodeString(state.token)
	if err != nil || len(decoded) != catalogStateTokenBytes ||
		base64.RawURLEncoding.EncodeToString(decoded) != state.token {
		return nil, fmt.Errorf("%w: knowledge catalog branch identity is invalid", ErrCorrupt)
	}
	return bytes.Clone(decoded), nil
}

func optionalUnixMicro(value *time.Time) *int64 {
	if value == nil {
		return nil
	}
	result := value.UnixMicro()
	return &result
}
