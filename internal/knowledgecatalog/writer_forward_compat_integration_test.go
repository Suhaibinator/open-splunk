package knowledgecatalog

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/audit"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/knowledgedefinition"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

func TestWriterMetadataUpdatePreservesInactiveOpaqueFutureAuthority(t *testing.T) {
	database, store := newCatalogTestStore(t)
	targetIDs := []string{
		"ko-writer-future-metadata-target-a",
		"ko-writer-future-metadata-target-b",
	}
	for index, targetID := range targetIDs {
		targetDescription := fmt.Sprintf("recognized extraction %d published before an inactive opaque dependent", index)
		insertFixtureObject(t, database, fixtureObject{
			id:    targetID,
			owner: testOwner,
			versions: []fixtureVersion{{
				definition: dependencyExtractionDefinition(
					testApp,
					fmt.Sprintf("writer-future-metadata-target-%c", 'a'+rune(index)),
					SharingScopePrivate,
					&targetDescription,
					fmt.Sprintf("future-metadata-target-%c-*", 'a'+rune(index)),
					fmt.Sprintf("%s_%d", dependencyFixtureInputField, index),
				),
				state:     StateActive,
				mutation:  "create",
				timestamp: int64(10 + index),
			}},
		})
		if target, err := store.Get(t.Context(), testReadScope(), targetID, nil); err != nil ||
			target.ObjectType != ObjectTypeFieldExtraction || target.State != StateActive {
			t.Fatalf("earlier-stage opaque dependency target %d = (%v, %v)", index, target, err)
		}
	}

	fixture := newIntegrationFutureDefinition(t, "writer-future-metadata", 0)
	futureMetadata := protowire.AppendString(
		protowire.AppendTag(nil, 32, protowire.BytesType),
		"future-metadata-value",
	)
	knownBytes := fixture.bytes[:len(fixture.bytes)-len(fixture.bodyField)]
	fixture.bodyField = append(bytes.Clone(futureMetadata), fixture.bodyField...)
	fixture.bytes = append(bytes.Clone(knownBytes), fixture.bodyField...)
	fixture.digest = sha256.Sum256(fixture.bytes)
	fixture.definition = &opensplunkv1.KnowledgeObjectDefinition{}
	if err := (proto.UnmarshalOptions{DiscardUnknown: false}).Unmarshal(fixture.bytes, fixture.definition); err != nil {
		t.Fatalf("decode opaque writer fixture: %v", err)
	}
	seedWriterInactiveOpaquePublication(
		t,
		database,
		"ko-writer-future-metadata",
		fixture,
		[]fixtureDependency{
			{targetObjectID: targetIDs[0], targetVersion: 1},
			{targetObjectID: targetIDs[1], targetVersion: 1},
		},
		20,
	)
	beforeProjection := readWriterOpaqueProjection(t, database, "ko-writer-future-metadata", 1)
	beforeSelectors := readWriterOpaqueSelectors(t, database, "ko-writer-future-metadata", 1)
	if beforeProjection.state != StateDraft || len(beforeSelectors) == 0 {
		t.Fatalf("seeded inactive opaque projection = %#v selectors=%#v", beforeProjection, beforeSelectors)
	}

	auditStore, err := audit.NewStore(database, audit.StoreOptions{CursorKey: testCursorKey})
	if err != nil {
		t.Fatalf("audit.NewStore(): %v", err)
	}
	writer, err := NewWriter(database, auditStore, WriterOptions{
		Clock: func() time.Time { return time.UnixMicro(30).UTC() },
		IDGenerator: func() (string, error) {
			return "", errors.New("metadata update must not generate an identity")
		},
	})
	if err != nil {
		t.Fatalf("NewWriter(): %v", err)
	}
	actorContext, err := audit.WithActor(context.Background(), audit.Actor{
		Kind: audit.ActorKindBrowser,
		ID:   "writer-future-administrator",
		Role: audit.ActorRoleAdministrator,
	})
	if err != nil {
		t.Fatalf("audit.WithActor(): %v", err)
	}
	scope := WriteScope{
		TenantID:       testTenant,
		OwnerID:        testOwner,
		WritableAppIDs: []string{testApp},
	}
	updatedDescription := "metadata changed by the older server"
	expectedDefinition := proto.Clone(fixture.definition).(*opensplunkv1.KnowledgeObjectDefinition)
	expectedDefinition.Description = &updatedDescription
	expectedAuthority, err := authorityFromOpaqueMetadataUpdate(
		expectedDefinition,
		StateDraft,
		ObjectTypeFieldAlias,
	)
	if err != nil {
		t.Fatalf("build expected metadata-only opaque authority: %v", err)
	}
	request := &opensplunkv1.UpdateKnowledgeObjectRequest{
		KnowledgeObjectId: "ko-writer-future-metadata",
		ExpectedVersion:   1,
		Definition: &opensplunkv1.KnowledgeObjectDefinition{
			Description: &updatedDescription,
		},
		UpdateMask:      &fieldmaskpb.FieldMask{Paths: []string{"description"}},
		ClientRequestId: "future-metadata-update-0001",
	}
	response, err := writer.Update(actorContext, scope, request)
	if err != nil {
		t.Fatalf("metadata-only opaque Update(): %v", err)
	}
	if response.GetTenantCatalogRevision() != 4 || len(response.GetTenantCatalogStateToken()) != sha256.Size {
		t.Fatalf("metadata-only opaque catalog authority = %v, want revision 4 and 32-byte token", response)
	}
	object := response.GetKnowledgeObject()
	if object.GetVersion() != 2 || object.GetState() != opensplunkv1.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_DRAFT ||
		object.GetDefinition().GetDescription() != updatedDescription || object.GetDefinition().GetBody() != nil ||
		!bytes.Equal(integrationUnknownBody(object.GetDefinition()), fixture.bodyField) ||
		!bytes.Equal(object.GetDefinitionSha256(), expectedAuthority.digest) {
		t.Fatalf("metadata-only opaque response = %v, unknown=%x", object, integrationUnknownBody(object.GetDefinition()))
	}
	encodedResponse, err := (proto.MarshalOptions{Deterministic: true}).Marshal(object.GetDefinition())
	if err != nil || !bytes.Equal(encodedResponse, expectedAuthority.bytes) ||
		bytes.Equal(object.GetDefinitionSha256(), fixture.digest[:]) {
		t.Fatalf(
			"metadata-only opaque response canonical authority = bytes=%x digest=%x err=%v; want bytes=%x digest=%x distinct from v1",
			encodedResponse,
			object.GetDefinitionSha256(),
			err,
			expectedAuthority.bytes,
			expectedAuthority.digest,
		)
	}

	oldVersion := uint64(1)
	oldObject, err := store.Get(context.Background(), testReadScope(), "ko-writer-future-metadata", &oldVersion)
	if err != nil {
		t.Fatalf("Get(old opaque version): %v", err)
	}
	encodedOld, err := (proto.MarshalOptions{Deterministic: true}).Marshal(oldObject.Definition)
	if err != nil || !bytes.Equal(encodedOld, fixture.bytes) || !bytes.Equal(oldObject.DefinitionSHA256, fixture.digest[:]) {
		t.Fatalf("old opaque version changed: bytes=%x digest=%x err=%v", encodedOld, oldObject.DefinitionSHA256, err)
	}
	current, err := store.Get(context.Background(), testReadScope(), "ko-writer-future-metadata", nil)
	if err != nil || current.Version != 2 || current.Definition.GetDescription() != updatedDescription ||
		!bytes.Equal(integrationUnknownBody(current.Definition), fixture.bodyField) ||
		!bytes.Equal(current.DefinitionSHA256, expectedAuthority.digest) {
		t.Fatalf("current opaque version = %v, %v", current, err)
	}
	encodedCurrent, err := (proto.MarshalOptions{Deterministic: true}).Marshal(current.Definition)
	if err != nil || !bytes.Equal(encodedCurrent, expectedAuthority.bytes) {
		t.Fatalf("current opaque canonical bytes = %x, %v; want %x", encodedCurrent, err, expectedAuthority.bytes)
	}

	assertWriterInactiveOpaqueImmutableAuthority(
		t,
		database,
		"ko-writer-future-metadata",
		targetIDs,
		[]writerInactiveOpaqueVersionExpectation{
			{version: 1, mutation: "create", bytes: fixture.bytes, digest: fixture.digest[:]},
			{version: 2, mutation: "update", bytes: expectedAuthority.bytes, digest: expectedAuthority.digest},
		},
		fixture.bodyField,
	)
	afterProjection := readWriterOpaqueProjection(t, database, "ko-writer-future-metadata", 2)
	wantProjection := beforeProjection
	wantProjection.signature.descriptionPresent = 1
	wantProjection.signature.description = updatedDescription
	wantProjection.signature.projectionBytes = beforeProjection.signature.projectionBytes -
		int64(len(beforeProjection.signature.description)) + int64(len(updatedDescription))
	wantProjection.signature.sealProjectionBytes = wantProjection.signature.projectionBytes
	if !reflect.DeepEqual(afterProjection, wantProjection) {
		t.Fatalf("metadata-only opaque projection = %#v, want %#v", afterProjection, wantProjection)
	}
	afterSelectors := readWriterOpaqueSelectors(t, database, "ko-writer-future-metadata", 2)
	if !reflect.DeepEqual(afterSelectors, beforeSelectors) {
		t.Fatalf("metadata-only opaque selectors = %#v, want exact %#v", afterSelectors, beforeSelectors)
	}
	assertWriterOpaqueOnlyCurrentProjection(t, database, "ko-writer-future-metadata", 2)
	assertWriterInactiveOpaqueUpdateReceipt(t, database, response, expectedAuthority.digest)

	stable := readWriterForwardCompatAuthoritySnapshot(t, database)
	replayed, err := writer.Update(actorContext, scope, proto.Clone(request).(*opensplunkv1.UpdateKnowledgeObjectRequest))
	if err != nil || !proto.Equal(replayed, response) {
		t.Fatalf("metadata-only opaque replay = %v, %v; want %v", replayed, err, response)
	}
	assertWriterForwardCompatAuthorityUnchanged(t, database, stable, "exact metadata-only replay")

	submittedUnknown := proto.Clone(request).(*opensplunkv1.UpdateKnowledgeObjectRequest)
	submittedUnknown.ClientRequestId = "future-submitted-unknown-0001"
	submittedUnknown.ExpectedVersion = 2
	submittedUnknown.Definition = proto.Clone(fixture.definition).(*opensplunkv1.KnowledgeObjectDefinition)
	stable = readWriterForwardCompatAuthoritySnapshot(t, database)
	if _, err := writer.Update(actorContext, scope, submittedUnknown); !errors.Is(err, control.ErrInvalidArgument) {
		t.Fatalf("submitted opaque body Update() error = %v, want ErrInvalidArgument", err)
	}
	assertWriterForwardCompatAuthorityUnchanged(t, database, stable, "submitted unknown fields")
	bodyEdit := proto.Clone(request).(*opensplunkv1.UpdateKnowledgeObjectRequest)
	bodyEdit.ClientRequestId = "future-body-edit-rejected-01"
	bodyEdit.ExpectedVersion = 2
	bodyEdit.Definition = aliasDefinition(testApp, "writer-future-metadata", SharingScopePrivate, nil, "body-edit-*")
	bodyEdit.UpdateMask = &fieldmaskpb.FieldMask{Paths: []string{"field_alias"}}
	stable = readWriterForwardCompatAuthoritySnapshot(t, database)
	if _, err := writer.Update(actorContext, scope, bodyEdit); !errors.Is(err, control.ErrInvalidArgument) {
		t.Fatalf("opaque body-edit Update() error = %v, want ErrInvalidArgument", err)
	}
	assertWriterForwardCompatAuthorityUnchanged(t, database, stable, "opaque body mask")

	var receipts int64
	if err := database.GORMDB().Table("knowledge_mutation_idempotency").
		Where("tenant_id = ?", testTenant).Count(&receipts).Error; err != nil {
		t.Fatalf("count opaque update receipts: %v", err)
	}
	if receipts != 1 {
		t.Fatalf("opaque metadata update receipts = %d, want 1", receipts)
	}
	assertWriterInactiveOpaqueImmutableAuthority(
		t,
		database,
		"ko-writer-future-metadata",
		targetIDs,
		[]writerInactiveOpaqueVersionExpectation{
			{version: 1, mutation: "create", bytes: fixture.bytes, digest: fixture.digest[:]},
			{version: 2, mutation: "update", bytes: expectedAuthority.bytes, digest: expectedAuthority.digest},
		},
		fixture.bodyField,
	)
	assertWriterForwardCompatIntegrity(t, database)
}

func TestOpaqueMetadataUpdateCanonicalByteCeiling(t *testing.T) {
	fixture := newIntegrationFutureDefinition(
		t,
		"writer-future-byte-ceiling",
		knowledgedefinition.MaximumCanonicalBytes,
	)
	originalDescription := fixture.definition.GetDescription()
	if len(originalDescription) < 2 {
		t.Fatalf("opaque byte-ceiling description is too short: %q", originalDescription)
	}
	for _, delta := range []int{-1, 0, 1} {
		t.Run(fmt.Sprintf("delta_%+d", delta), func(t *testing.T) {
			definition := proto.Clone(fixture.definition).(*opensplunkv1.KnowledgeObjectDefinition)
			description := strings.Repeat("x", len(originalDescription)+delta)
			definition.Description = &description
			authority, err := authorityFromOpaqueMetadataUpdate(
				definition,
				StateDraft,
				ObjectTypeFieldAlias,
			)
			if delta > 0 {
				if !errors.Is(err, control.ErrCapacityExceeded) {
					t.Fatalf("opaque max+1 error = %v, want ErrCapacityExceeded", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("opaque max%+d authority: %v", delta, err)
			}
			if got, want := len(authority.bytes), knowledgedefinition.MaximumCanonicalBytes+delta; got != want {
				t.Fatalf("opaque canonical bytes = %d, want %d", got, want)
			}
		})
	}
}

func seedWriterInactiveOpaquePublication(
	t *testing.T,
	database *control.DB,
	objectID string,
	fixture integrationFutureDefinition,
	dependencies []fixtureDependency,
	timestamp int64,
) {
	t.Helper()
	if len(dependencies) == 0 {
		t.Fatal("inactive opaque publication requires a real dependency")
	}
	tx, err := database.SQLDB().BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatalf("begin inactive opaque publication fixture: %v", err)
	}
	defer func() { _ = tx.Rollback() }()
	ensureIntegrationCatalogLedgers(t, tx)
	insertIntegrationDefinitionBlob(t, tx, fixture.bytes, fixture.digest[:], timestamp)
	insertIntegrationVersionWithDependencies(
		t,
		tx,
		objectID,
		1,
		StateDraft,
		"create",
		fixture.digest[:],
		fixture.metadata,
		timestamp,
		dependencies,
	)
	insertIntegrationProjection(t, tx, objectID, 1, StateDraft, fixture.metadata)
	objectType, typeOK := objectTypeFromProto(fixture.metadata.ObjectType)
	sharingScope, scopeOK := sharingScopeFromProto(fixture.metadata.SharingScope)
	if !typeOK || !scopeOK {
		t.Fatal("inactive opaque fixture has unsupported scalar identity")
	}
	if _, err := tx.ExecContext(t.Context(), `INSERT INTO knowledge_objects (
		tenant_id, knowledge_object_id, current_version, app_id, owner_id, object_type, name,
		sharing_scope, state, definition_digest, created_at_unix_micro, updated_at_unix_micro,
		disabled_at_unix_micro, quarantined_at_unix_micro, deleted_at_unix_micro, quarantine_reason
	) VALUES (?, ?, 1, ?, ?, ?, ?, ?, 'draft', ?, ?, ?, NULL, NULL, NULL, NULL)`,
		testTenant,
		objectID,
		fixture.metadata.AppID,
		testOwner,
		objectType,
		fixture.metadata.Name,
		sharingScope,
		fixture.digest[:],
		timestamp,
		timestamp,
	); err != nil {
		t.Fatalf("publish inactive opaque registry: %v", err)
	}
	advanceIntegrationCatalogRevision(t, tx)
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit inactive opaque publication fixture: %v", err)
	}
}

type writerInactiveOpaqueVersionExpectation struct {
	version  int64
	mutation string
	bytes    []byte
	digest   []byte
}

func assertWriterInactiveOpaqueUpdateReceipt(
	t *testing.T,
	database *control.DB,
	response *opensplunkv1.UpdateKnowledgeObjectResponse,
	digest []byte,
) {
	t.Helper()
	var outcome, stateToken []byte
	var objectID, route, mutationKind string
	var objectVersion, catalogRevision, successfulAuditSequence int64
	var recoveryAuditSequence any
	if err := database.SQLDB().QueryRowContext(t.Context(), `
		SELECT outcome_proto, route, mutation_kind, knowledge_object_id, object_version,
		       committed_catalog_revision, committed_catalog_state_token,
		       successful_audit_sequence, recovery_audit_sequence
		FROM knowledge_mutation_idempotency
		WHERE tenant_id = ? AND actor_kind = ? AND actor_id = ?
		  AND route = ? AND client_request_id = ?`,
		testTenant,
		audit.ActorKindBrowser,
		"writer-future-administrator",
		mutationRouteUpdate,
		"future-metadata-update-0001",
	).Scan(
		&outcome,
		&route,
		&mutationKind,
		&objectID,
		&objectVersion,
		&catalogRevision,
		&stateToken,
		&successfulAuditSequence,
		&recoveryAuditSequence,
	); err != nil {
		t.Fatalf("read inactive opaque update receipt: %v", err)
	}
	if len(outcome) < 1 || len(outcome) > maximumMutationOutcomeBytes ||
		route != mutationRouteUpdate || mutationKind != "update" ||
		objectID != "ko-writer-future-metadata" || objectVersion != 2 ||
		catalogRevision != int64(response.GetTenantCatalogRevision()) ||
		!bytes.Equal(stateToken, response.GetTenantCatalogStateToken()) ||
		successfulAuditSequence != 1 || recoveryAuditSequence != nil {
		t.Fatalf(
			"inactive opaque update receipt = outcome=%d route=%q mutation=%q object=%q/v%d revision=%d token=%x audit=%d recovery=%v",
			len(outcome),
			route,
			mutationKind,
			objectID,
			objectVersion,
			catalogRevision,
			stateToken,
			successfulAuditSequence,
			recoveryAuditSequence,
		)
	}
	envelope := &opensplunkv1.KnowledgeMutationOutcomeRecord{}
	if err := (proto.UnmarshalOptions{DiscardUnknown: false}).Unmarshal(outcome, envelope); err != nil ||
		len(envelope.ProtoReflect().GetUnknown()) != 0 || envelope.GetObject() == nil ||
		envelope.GetRoute() != mutationRouteUpdate || envelope.GetMutationKind() != "update" ||
		envelope.GetObject().GetKnowledgeObjectId() != objectID ||
		envelope.GetObject().GetVersion() != uint64(objectVersion) ||
		!bytes.Equal(envelope.GetObject().GetDefinitionSha256(), digest) ||
		envelope.GetTenantCatalogRevision() != response.GetTenantCatalogRevision() ||
		!bytes.Equal(envelope.GetTenantCatalogStateToken(), response.GetTenantCatalogStateToken()) ||
		envelope.GetSuccessfulAuditSequence() != uint64(successfulAuditSequence) {
		t.Fatalf("inactive opaque compact outcome = (%v, %v)", envelope, err)
	}
	canonical, err := (proto.MarshalOptions{Deterministic: true}).Marshal(envelope)
	if err != nil || !bytes.Equal(canonical, outcome) {
		t.Fatalf("inactive opaque compact outcome is noncanonical: bytes=%x canonical=%x err=%v", outcome, canonical, err)
	}

	var action audit.Action
	var actorKind audit.ActorKind
	var actorID string
	var actorRole audit.ActorRole
	var targetKind audit.TargetKind
	var targetID, appID, objectType, sharingScope string
	var targetVersion int64
	if err := database.SQLDB().QueryRowContext(t.Context(), `
		SELECT action, actor_kind, actor_id, actor_role, target_kind, target_id,
		       target_version, app_id, object_type, sharing_scope
		FROM audit_events WHERE tenant_id = ? AND sequence = ?`,
		testTenant,
		successfulAuditSequence,
	).Scan(
		&action,
		&actorKind,
		&actorID,
		&actorRole,
		&targetKind,
		&targetID,
		&targetVersion,
		&appID,
		&objectType,
		&sharingScope,
	); err != nil {
		t.Fatalf("read inactive opaque update audit provenance: %v", err)
	}
	if action != audit.ActionKnowledgeObjectUpdate || actorKind != audit.ActorKindBrowser ||
		actorID != "writer-future-administrator" || actorRole != audit.ActorRoleAdministrator ||
		targetKind != audit.TargetKindKnowledgeObject || targetID != objectID || targetVersion != objectVersion ||
		appID != testApp || objectType != string(ObjectTypeFieldAlias) || sharingScope != string(SharingScopePrivate) {
		t.Fatalf(
			"inactive opaque update audit provenance = action=%q actor=%q/%q/%q target=%q/%q/v%d metadata=%q/%q/%q",
			action,
			actorKind,
			actorID,
			actorRole,
			targetKind,
			targetID,
			targetVersion,
			appID,
			objectType,
			sharingScope,
		)
	}
}

func assertWriterInactiveOpaqueImmutableAuthority(
	t *testing.T,
	database *control.DB,
	objectID string,
	targetIDs []string,
	want []writerInactiveOpaqueVersionExpectation,
	unknown []byte,
) {
	t.Helper()
	if len(targetIDs) == 0 {
		t.Fatal("inactive opaque authority expectation requires dependencies")
	}
	rows, err := database.SQLDB().QueryContext(t.Context(), `
		SELECT version.object_version, version.state, version.mutation_kind,
		       version.definition_digest, version.dependency_count,
		       blob.definition_proto, blob.definition_bytes
		FROM knowledge_object_versions AS version
		JOIN knowledge_definition_blobs AS blob
		  ON blob.tenant_id = version.tenant_id
		 AND blob.definition_digest = version.definition_digest
		WHERE version.tenant_id = ? AND version.knowledge_object_id = ?
		ORDER BY version.object_version`, testTenant, objectID)
	if err != nil {
		t.Fatalf("read inactive opaque immutable versions: %v", err)
	}
	defer rows.Close()
	index := 0
	for rows.Next() {
		if index >= len(want) {
			t.Fatalf("inactive opaque immutable versions include unexpected row %d", index+1)
		}
		var version, dependencyCount, definitionBytes int64
		var state State
		var mutation string
		var digest, definition []byte
		if err := rows.Scan(
			&version,
			&state,
			&mutation,
			&digest,
			&dependencyCount,
			&definition,
			&definitionBytes,
		); err != nil {
			t.Fatalf("scan inactive opaque immutable version: %v", err)
		}
		expected := want[index]
		if version != expected.version || state != StateDraft || mutation != expected.mutation ||
			dependencyCount != int64(len(targetIDs)) || definitionBytes != int64(len(expected.bytes)) ||
			!bytes.Equal(digest, expected.digest) || !bytes.Equal(definition, expected.bytes) {
			t.Fatalf(
				"inactive opaque v%d = state=%q mutation=%q dependencies=%d digest=%x bytes=%x/%d; want %#v exact",
				version,
				state,
				mutation,
				dependencyCount,
				digest,
				definition,
				definitionBytes,
				expected,
			)
		}
		decoded := &opensplunkv1.KnowledgeObjectDefinition{}
		if err := (proto.UnmarshalOptions{DiscardUnknown: false}).Unmarshal(definition, decoded); err != nil {
			t.Fatalf("decode inactive opaque v%d: %v", version, err)
		}
		canonical, err := (proto.MarshalOptions{Deterministic: true}).Marshal(decoded)
		if err != nil || decoded.GetBody() != nil ||
			!bytes.Equal(integrationUnknownBody(decoded), unknown) || !bytes.Equal(canonical, expected.bytes) {
			t.Fatalf(
				"inactive opaque v%d canonical authority = body %T unknown=%x bytes=%x err=%v; want nil/%x/%x",
				version,
				decoded.GetBody(),
				integrationUnknownBody(decoded),
				canonical,
				err,
				unknown,
				expected.bytes,
			)
		}
		computed := sha256.Sum256(definition)
		if !bytes.Equal(computed[:], expected.digest) {
			t.Fatalf("inactive opaque v%d computed digest = %x, want %x", version, computed, expected.digest)
		}
		index++
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate inactive opaque immutable versions: %v", err)
	}
	if index != len(want) {
		t.Fatalf("inactive opaque immutable version rows = %d, want %d", index, len(want))
	}

	dependencyRows, err := database.SQLDB().QueryContext(t.Context(), `
		SELECT source_object_version, ordinal, target_kind, target_object_id,
		       target_object_version, dependency_role
		FROM knowledge_object_dependencies
		WHERE tenant_id = ? AND source_object_id = ?
		ORDER BY source_object_version, ordinal`, testTenant, objectID)
	if err != nil {
		t.Fatalf("read inactive opaque dependency rows: %v", err)
	}
	defer dependencyRows.Close()
	index = 0
	for dependencyRows.Next() {
		if index >= len(want)*len(targetIDs) {
			t.Fatalf("inactive opaque dependencies include unexpected row %d", index+1)
		}
		var version, ordinal, targetVersion int64
		var targetKind, gotTargetID, role string
		if err := dependencyRows.Scan(
			&version,
			&ordinal,
			&targetKind,
			&gotTargetID,
			&targetVersion,
			&role,
		); err != nil {
			t.Fatalf("scan inactive opaque dependency: %v", err)
		}
		versionIndex := index / len(targetIDs)
		targetIndex := index % len(targetIDs)
		if version != want[versionIndex].version || ordinal != int64(targetIndex) || targetKind != "object" ||
			gotTargetID != targetIDs[targetIndex] || targetVersion != 1 || role != "field_input" {
			t.Fatalf(
				"inactive opaque dependency %d = v%d/%d/%q/%q/v%d/%q, want exact v%d target %q/v1",
				index,
				version,
				ordinal,
				targetKind,
				gotTargetID,
				targetVersion,
				role,
				want[versionIndex].version,
				targetIDs[targetIndex],
			)
		}
		index++
	}
	if err := dependencyRows.Err(); err != nil {
		t.Fatalf("iterate inactive opaque dependencies: %v", err)
	}
	if index != len(want)*len(targetIDs) {
		t.Fatalf("inactive opaque dependency rows = %d, want %d", index, len(want)*len(targetIDs))
	}

	sealRows, err := database.SQLDB().QueryContext(t.Context(), `
		SELECT object_version, dependency_count
		FROM knowledge_object_dependency_seals
		WHERE tenant_id = ? AND knowledge_object_id = ?
		ORDER BY object_version`, testTenant, objectID)
	if err != nil {
		t.Fatalf("read inactive opaque dependency seals: %v", err)
	}
	defer sealRows.Close()
	index = 0
	for sealRows.Next() {
		if index >= len(want) {
			t.Fatalf("inactive opaque dependency seals include unexpected row %d", index+1)
		}
		var version, count int64
		if err := sealRows.Scan(&version, &count); err != nil {
			t.Fatalf("scan inactive opaque dependency seal: %v", err)
		}
		if version != want[index].version || count != int64(len(targetIDs)) {
			t.Fatalf(
				"inactive opaque dependency seal %d = v%d/%d, want v%d/%d",
				index,
				version,
				count,
				want[index].version,
				len(targetIDs),
			)
		}
		index++
	}
	if err := sealRows.Err(); err != nil {
		t.Fatalf("iterate inactive opaque dependency seals: %v", err)
	}
	if index != len(want) {
		t.Fatalf("inactive opaque dependency seals = %d, want %d", index, len(want))
	}
}

func readWriterForwardCompatAuthoritySnapshot(
	t *testing.T,
	database *control.DB,
) writerFaultSnapshot {
	t.Helper()
	snapshot := make(writerFaultSnapshot, len(writerFaultSnapshotTables))
	for _, table := range writerFaultSnapshotTables {
		query := `SELECT * FROM "` + table + `" WHERE tenant_id = ?`
		rows, err := database.SQLDB().QueryContext(t.Context(), query, testTenant)
		if err != nil {
			t.Fatalf("snapshot forward-compatible authority %s: %v", table, err)
		}
		columns, err := rows.Columns()
		if err != nil {
			_ = rows.Close()
			t.Fatalf("snapshot forward-compatible authority columns %s: %v", table, err)
		}
		encodedRows := make([]string, 0)
		for rows.Next() {
			values := make([]any, len(columns))
			destinations := make([]any, len(columns))
			for index := range values {
				destinations[index] = &values[index]
			}
			if err := rows.Scan(destinations...); err != nil {
				_ = rows.Close()
				t.Fatalf("snapshot forward-compatible authority row %s: %v", table, err)
			}
			var encoded strings.Builder
			for index, value := range values {
				_, _ = fmt.Fprintf(&encoded, "%q=%s;", columns[index], writerFaultSQLValue(value))
			}
			encodedRows = append(encodedRows, encoded.String())
		}
		if err := rows.Err(); err != nil {
			_ = rows.Close()
			t.Fatalf("iterate forward-compatible authority snapshot %s: %v", table, err)
		}
		if err := rows.Close(); err != nil {
			t.Fatalf("close forward-compatible authority snapshot %s: %v", table, err)
		}
		sort.Strings(encodedRows)
		snapshot[table] = encodedRows
	}
	return snapshot
}

func assertWriterForwardCompatAuthorityUnchanged(
	t *testing.T,
	database *control.DB,
	want writerFaultSnapshot,
	reason string,
) {
	t.Helper()
	got := readWriterForwardCompatAuthoritySnapshot(t, database)
	for _, table := range writerFaultSnapshotTables {
		if !reflect.DeepEqual(got[table], want[table]) {
			t.Fatalf(
				"forward-compatible authority table %s changed after %s:\n got: %#v\nwant: %#v",
				table,
				reason,
				got[table],
				want[table],
			)
		}
	}
}

func assertWriterForwardCompatIntegrity(t *testing.T, database *control.DB) {
	t.Helper()
	assertWriterFaultIntegrity(t, database)
}
