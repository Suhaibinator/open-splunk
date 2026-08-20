package knowledgecatalog_test

import (
	"bytes"
	"errors"
	"strings"
	"testing"
	"time"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/audit"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/knowledgecatalog"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

func TestWriterCreateDraftPublishesAtomicallyAndReplaysCompactOutcome(t *testing.T) {
	harness := newWriterBlackboxHarness(t)
	request, response := harness.createDraft(t, "atomic-create", "create-atomic-request-0001")
	committed := proto.Clone(response).(*opensplunk.CreateKnowledgeObjectResponse)
	object := response.GetKnowledgeObject()
	assertWriterProtoObject(t, object, writerProtoObjectExpectation{
		ID:         "ko_0000000000000000000001",
		Definition: request.GetDefinition(),
		Version:    1,
		State:      opensplunk.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_DRAFT,
		CreatedUS:  10_001,
		UpdatedUS:  10_001,
	})
	assertWriterResponseCatalog(t, response.GetTenantCatalogRevision(), response.GetTenantCatalogStateToken(), 1)
	stored := getWriterObject(t, harness, object.GetKnowledgeObjectId(), nil)
	assertWriterProtoMatchesStored(t, object, stored)

	encodedDefinition, err := (proto.MarshalOptions{Deterministic: true}).Marshal(request.GetDefinition())
	if err != nil {
		t.Fatalf("marshal definition: %v", err)
	}
	snapshot := readWriterAuthoritySnapshot(t, harness.database)
	if snapshot.CatalogRevision != 1 || snapshot.IdentityCount != 1 ||
		snapshot.VersionCount != 1 || snapshot.DefinitionBytes != int64(len(encodedDefinition)) ||
		snapshot.IdempotencyCount != 1 || snapshot.ActiveObjectCount != 0 ||
		snapshot.AuditNextSequence != 2 || snapshot.AuditEventCount != 1 {
		t.Fatalf("create authority ledgers = %#v", snapshot)
	}
	if !bytes.Equal(response.GetTenantCatalogStateToken(), snapshot.CatalogStateToken[:]) {
		t.Fatalf("response state token = %x, committed head = %x", response.GetTenantCatalogStateToken(), snapshot.CatalogStateToken)
	}
	wantProjectionBytes := int64(len(request.GetDefinition().GetDescription()) + len("host-atomic-create"))
	if snapshot.ProjectionBytes != wantProjectionBytes {
		t.Fatalf("projection bytes = %d, want %d", snapshot.ProjectionBytes, wantProjectionBytes)
	}
	assertWriterTableCounts(t, snapshot, map[string]int64{
		"knowledge_definition_blobs":              1,
		"knowledge_objects":                       1,
		"knowledge_object_versions":               1,
		"knowledge_object_version_lifecycle":      1,
		"knowledge_object_dependencies":           0,
		"knowledge_object_dependency_seals":       1,
		"knowledge_object_list_projections":       1,
		"knowledge_object_list_selector_patterns": 1,
		"knowledge_object_list_projection_seals":  1,
		"knowledge_object_list_order_keys":        1,
		"knowledge_mutation_commit_authorities":   1,
		"knowledge_mutation_idempotency":          1,
		"audit_events":                            1,
	})

	reference, outcomeBytes := readCompactWriterOutcome(
		t,
		harness.database,
		"objects.create",
		request.GetClientRequestId(),
	)
	if reference.GetKnowledgeObjectId() != object.GetKnowledgeObjectId() ||
		reference.GetVersion() != 1 ||
		!bytes.Equal(reference.GetDefinitionSha256(), object.GetDefinitionSha256()) {
		t.Fatalf("compact outcome reference = %v, object = %v", reference, object)
	}
	if len(outcomeBytes) > 1024 {
		t.Fatalf("compact outcome bytes = %d, schema maximum 1024", len(outcomeBytes))
	}
	receipt := readWriterIdempotencyReceipt(
		t,
		harness.database,
		audit.ActorKindBrowser,
		"writer-blackbox-administrator",
		request.GetClientRequestId(),
	)
	expectedDigest := writerExpectedRequestDigest(t, "objects.create", writerTestOwner, request)
	if receipt.ActorKind != string(audit.ActorKindBrowser) ||
		receipt.ActorID != "writer-blackbox-administrator" ||
		receipt.MutationKind != "create" ||
		receipt.RequestDigestFormatVersion != 1 ||
		!bytes.Equal(receipt.RequestDigest, expectedDigest[:]) ||
		receipt.OutcomeFormatVersion != 1 ||
		!bytes.Equal(receipt.OutcomeProto, outcomeBytes) ||
		receipt.CommittedCatalogRevision != 1 ||
		!bytes.Equal(receipt.CommittedCatalogStateToken, response.GetTenantCatalogStateToken()) ||
		receipt.KnowledgeObjectID != object.GetKnowledgeObjectId() ||
		receipt.ObjectVersion != 1 ||
		!receipt.SuccessfulAuditSequence.Valid || receipt.SuccessfulAuditSequence.Int64 != 1 ||
		receipt.RecoveryAuditSequence.Valid {
		t.Fatalf("create idempotency receipt = %#v", receipt)
	}

	events := knowledgeAuditEvents(t, harness)
	assertWriterAuditEvents(t, events, []writerAuditExpectation{{
		Action:  audit.ActionKnowledgeObjectCreate,
		ID:      object.GetKnowledgeObjectId(),
		Version: 1,
		AppID:   writerTestApp,
		Scope:   audit.KnowledgeSharingScopePrivate,
	}})

	unauthorizedScope := harness.writeScope
	unauthorizedScope.WritableAppIDs = []string{writerTestAppTwo}
	if _, err := harness.writer.Create(harness.actorCtx, unauthorizedScope, proto.Clone(request).(*opensplunk.CreateKnowledgeObjectRequest)); !errors.Is(err, control.ErrNotFound) {
		t.Fatalf("unauthorized exact replay error = %v, want ErrNotFound", err)
	}
	assertWriterAuthoritySnapshotsEqual(t, readWriterAuthoritySnapshot(t, harness.database), snapshot)

	replayed, err := harness.writer.Create(
		harness.actorCtx,
		harness.writeScope,
		proto.Clone(request).(*opensplunk.CreateKnowledgeObjectRequest),
	)
	if err != nil {
		t.Fatalf("exact Create replay: %v", err)
	}
	if !proto.Equal(replayed, committed) {
		t.Fatalf("exact Create replay = %v, want %v", replayed, committed)
	}
	assertWriterAuthoritySnapshotsEqual(t, readWriterAuthoritySnapshot(t, harness.database), snapshot)
	if harness.idCalls.Load() != 1 || harness.clockCalls.Load() != 1 {
		t.Fatalf("generator calls after replay: IDs=%d clocks=%d, want 1/1", harness.idCalls.Load(), harness.clockCalls.Load())
	}

	response.KnowledgeObject.Definition.Name = "client-mutated-response"
	response.KnowledgeObject.DefinitionSha256[0] ^= 0xff
	detachedReplay, err := harness.writer.Create(harness.actorCtx, harness.writeScope, request)
	if err != nil {
		t.Fatalf("Create replay after caller mutation: %v", err)
	}
	if !proto.Equal(detachedReplay, committed) {
		t.Fatalf("reconstructed replay was affected by caller mutation: got %v want %v", detachedReplay, committed)
	}

	altered := proto.Clone(request).(*opensplunk.CreateKnowledgeObjectRequest)
	altered.Definition.Name = "altered-same-idempotency-key"
	if _, err := harness.writer.Create(harness.actorCtx, harness.writeScope, altered); !errors.Is(err, knowledgecatalog.ErrIdempotencyConflict) {
		t.Fatalf("altered Create replay error = %v, want ErrIdempotencyConflict", err)
	}
	assertWriterAuthoritySnapshotsEqual(t, readWriterAuthoritySnapshot(t, harness.database), snapshot)
	assertWriterCatalogIntegrity(t, harness.database)
}

func TestWriterLargeDraftReplayReconstructsResponseFromCompactReference(t *testing.T) {
	harness := newWriterBlackboxHarness(t)
	description := strings.Repeat("d", 16<<10)
	request := &opensplunk.CreateKnowledgeObjectRequest{
		Definition: writerAliasDefinition(
			writerTestApp,
			"large-compact-replay",
			&description,
			opensplunk.SharingScope_SHARING_SCOPE_PRIVATE,
			"large-compact-host",
			"source_field",
			"destination_field",
		),
		InitialState:    opensplunk.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_DRAFT,
		ClientRequestId: "large-compact-request-0001",
	}
	response, err := harness.writer.Create(harness.actorCtx, harness.writeScope, request)
	if err != nil {
		t.Fatalf("large Writer.Create: %v", err)
	}
	if proto.Size(response) <= 1024 {
		t.Fatalf("large Create response bytes = %d, want greater than compact receipt ceiling", proto.Size(response))
	}
	reference, outcome := readCompactWriterOutcome(t, harness.database, "objects.create", request.GetClientRequestId())
	if len(outcome) > 1024 || reference.GetKnowledgeObjectId() != response.GetKnowledgeObject().GetKnowledgeObjectId() ||
		reference.GetVersion() != 1 || !bytes.Equal(reference.GetDefinitionSha256(), response.GetKnowledgeObject().GetDefinitionSha256()) {
		t.Fatalf("large Create compact outcome = %v (%d bytes), response = %v", reference, len(outcome), response)
	}
	encodedResponse, err := (proto.MarshalOptions{Deterministic: true}).Marshal(response)
	if err != nil {
		t.Fatalf("marshal large Create response: %v", err)
	}
	if bytes.Equal(outcome, encodedResponse) {
		t.Fatal("idempotency receipt stored the full large response instead of its compact reference")
	}
	stable := readWriterAuthoritySnapshot(t, harness.database)
	replayed, err := harness.writer.Create(
		harness.actorCtx,
		harness.writeScope,
		proto.Clone(request).(*opensplunk.CreateKnowledgeObjectRequest),
	)
	if err != nil || !proto.Equal(replayed, response) {
		t.Fatalf("large Create replay = (%v, %v), want %v", replayed, err, response)
	}
	assertWriterAuthoritySnapshotsEqual(t, readWriterAuthoritySnapshot(t, harness.database), stable)
	assertWriterCatalogIntegrity(t, harness.database)
}

func TestWriterActivePublicationRequiresCurrentIndexWinningWitness(t *testing.T) {
	harness := newWriterBlackboxHarness(t)
	_, draft := harness.createDraft(t, "active-fail-closed", "active-draft-request-0001")
	stable := readWriterAuthoritySnapshot(t, harness.database)

	activeDescription := "must not publish without the active validator"
	if _, err := harness.writer.Create(harness.actorCtx, harness.writeScope, &opensplunk.CreateKnowledgeObjectRequest{
		Definition: writerAliasDefinition(
			writerTestApp,
			"active-create-fail-closed",
			&activeDescription,
			opensplunk.SharingScope_SHARING_SCOPE_PRIVATE,
			"active-create-host",
			"source_field",
			"destination_field",
		),
		InitialState:    opensplunk.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_ACTIVE,
		ClientRequestId: "active-create-request-0001",
	}); !errors.Is(err, control.ErrDependencyConflict) {
		t.Fatalf("Create(ACTIVE) error = %v, want dependency conflict", err)
	}
	assertWriterAuthoritySnapshotsEqual(t, readWriterAuthoritySnapshot(t, harness.database), stable)

	if _, err := harness.writer.SetState(harness.actorCtx, harness.writeScope, &opensplunk.SetKnowledgeObjectStateRequest{
		KnowledgeObjectId: draft.GetKnowledgeObject().GetKnowledgeObjectId(),
		ExpectedVersion:   1,
		State:             opensplunk.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_ACTIVE,
		ClientRequestId:   "active-enable-request-0001",
	}); !errors.Is(err, control.ErrDependencyConflict) {
		t.Fatalf("SetState(ACTIVE) error = %v, want dependency conflict", err)
	}
	assertWriterAuthoritySnapshotsEqual(t, readWriterAuthoritySnapshot(t, harness.database), stable)
	assertWriterCatalogIntegrity(t, harness.database)
}

func TestWriterIdempotencyKeyPartitionsByActorAndRouteAndBindsOwner(t *testing.T) {
	harness := newWriterBlackboxHarness(t)
	request, first := harness.createDraft(t, "digest-partitions", "shared-partition-key-0001")

	reorderedScope := harness.writeScope
	reorderedScope.WritableAppIDs = []string{writerTestAppTwo, writerTestApp}
	replayed, err := harness.writer.Create(harness.actorCtx, reorderedScope, request)
	if err != nil || !proto.Equal(replayed, first) {
		t.Fatalf("replay with changed writable-app list = (%v, %v), want exact outcome", replayed, err)
	}

	otherOwner := harness.writeScope
	otherOwner.OwnerID = "owner-writer-blackbox-other"
	if _, err := harness.writer.Create(harness.actorCtx, otherOwner, request); !errors.Is(err, control.ErrNotFound) {
		t.Fatalf("same PK with changed trusted owner error = %v, want ErrNotFound", err)
	}

	systemCtx, err := audit.WithActor(t.Context(), audit.Actor{
		Kind: audit.ActorKindSystem,
		ID:   "writer-blackbox-administrator",
		Role: audit.ActorRoleSystem,
	})
	if err != nil {
		t.Fatalf("audit.WithActor(system): %v", err)
	}
	systemResponse, err := harness.writer.Create(systemCtx, harness.writeScope, request)
	if err != nil {
		t.Fatalf("Create with same key and actor ID but different actor kind: %v", err)
	}
	if systemResponse.GetKnowledgeObject().GetKnowledgeObjectId() == first.GetKnowledgeObject().GetKnowledgeObjectId() {
		t.Fatal("actor-kind-partitioned idempotency key replayed the browser outcome")
	}

	secondBrowserCtx, err := audit.WithActor(t.Context(), audit.Actor{
		Kind: audit.ActorKindBrowser,
		ID:   "writer-blackbox-second-administrator",
		Role: audit.ActorRoleAdministrator,
	})
	if err != nil {
		t.Fatalf("audit.WithActor(second browser): %v", err)
	}
	secondBrowserResponse, err := harness.writer.Create(secondBrowserCtx, harness.writeScope, request)
	if err != nil {
		t.Fatalf("Create with same key but different actor ID: %v", err)
	}
	if secondBrowserResponse.GetKnowledgeObject().GetKnowledgeObjectId() == first.GetKnowledgeObject().GetKnowledgeObjectId() ||
		secondBrowserResponse.GetKnowledgeObject().GetKnowledgeObjectId() == systemResponse.GetKnowledgeObject().GetKnowledgeObjectId() {
		t.Fatal("actor-ID-partitioned idempotency key replayed another actor's outcome")
	}

	updateDefinition := proto.Clone(first.GetKnowledgeObject().GetDefinition()).(*opensplunk.KnowledgeObjectDefinition)
	updateDefinition.Description = new("route-partitioned update")
	updateResponse, err := harness.writer.Update(harness.actorCtx, harness.writeScope, &opensplunk.UpdateKnowledgeObjectRequest{
		KnowledgeObjectId: first.GetKnowledgeObject().GetKnowledgeObjectId(),
		ExpectedVersion:   1,
		Definition:        updateDefinition,
		UpdateMask:        &fieldmaskpb.FieldMask{Paths: []string{"description"}},
		ClientRequestId:   request.GetClientRequestId(),
	})
	if err != nil {
		t.Fatalf("Update with Create's client key on a different route: %v", err)
	}
	if updateResponse.GetKnowledgeObject().GetVersion() != 2 {
		t.Fatalf("route-partitioned update version = %d, want 2", updateResponse.GetKnowledgeObject().GetVersion())
	}

	browserReceipt := readWriterIdempotencyReceipt(t, harness.database, audit.ActorKindBrowser, "writer-blackbox-administrator", request.GetClientRequestId())
	systemReceipt := readWriterIdempotencyReceipt(t, harness.database, audit.ActorKindSystem, "writer-blackbox-administrator", request.GetClientRequestId())
	secondBrowserReceipt := readWriterIdempotencyReceipt(t, harness.database, audit.ActorKindBrowser, "writer-blackbox-second-administrator", request.GetClientRequestId())
	if !bytes.Equal(browserReceipt.RequestDigest, systemReceipt.RequestDigest) ||
		!bytes.Equal(browserReceipt.RequestDigest, secondBrowserReceipt.RequestDigest) {
		t.Fatal("request digest redundantly bound actor PK fields")
	}
	snapshot := readWriterAuthoritySnapshot(t, harness.database)
	if snapshot.IdentityCount != 3 || snapshot.VersionCount != 4 || snapshot.IdempotencyCount != 4 ||
		snapshot.AuditNextSequence != 5 || snapshot.AuditEventCount != 4 ||
		snapshot.TableCounts["audit_events"] != 4 {
		t.Fatalf("partitioned idempotency authority = %#v", snapshot)
	}
	assertWriterCatalogIntegrity(t, harness.database)
}

func TestWriterUpdateUsesOnlyExactTopLevelMaskAuthority(t *testing.T) {
	harness := newWriterBlackboxHarness(t)
	_, created := harness.createDraft(t, "masked-update", "masked-create-request-0001")
	createdObject := created.GetKnowledgeObject()

	description := "the only authorized change"
	incoming := writerAliasDefinition(
		writerTestAppTwo,
		"unmasked-name-must-not-win",
		&description,
		opensplunk.SharingScope_SHARING_SCOPE_GLOBAL,
		"unmasked-host-must-not-win",
		"unmasked_source",
		"unmasked_destination",
	)
	descriptionRequest := &opensplunk.UpdateKnowledgeObjectRequest{
		KnowledgeObjectId: createdObject.GetKnowledgeObjectId(),
		ExpectedVersion:   1,
		Definition:        incoming,
		UpdateMask:        &fieldmaskpb.FieldMask{Paths: []string{"description"}},
		ClientRequestId:   "masked-description-0001",
	}
	descriptionResponse, err := harness.writer.Update(harness.actorCtx, harness.writeScope, descriptionRequest)
	if err != nil {
		t.Fatalf("description-only Update: %v", err)
	}
	expectedDescriptionDefinition := proto.Clone(createdObject.GetDefinition()).(*opensplunk.KnowledgeObjectDefinition)
	expectedDescriptionDefinition.Description = &description
	assertWriterProtoObject(t, descriptionResponse.GetKnowledgeObject(), writerProtoObjectExpectation{
		ID:         createdObject.GetKnowledgeObjectId(),
		Definition: expectedDescriptionDefinition,
		Version:    2,
		State:      opensplunk.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_DRAFT,
		CreatedUS:  10_001,
		UpdatedUS:  10_002,
	})
	descriptionCommitted := proto.Clone(descriptionResponse).(*opensplunk.UpdateKnowledgeObjectResponse)
	afterDescription := readWriterAuthoritySnapshot(t, harness.database)

	descriptionReplay, err := harness.writer.Update(
		harness.actorCtx,
		harness.writeScope,
		proto.Clone(descriptionRequest).(*opensplunk.UpdateKnowledgeObjectRequest),
	)
	if err != nil || !proto.Equal(descriptionReplay, descriptionCommitted) {
		t.Fatalf("exact Update replay = (%v, %v), want %v", descriptionReplay, err, descriptionCommitted)
	}
	assertWriterAuthoritySnapshotsEqual(t, readWriterAuthoritySnapshot(t, harness.database), afterDescription)

	alteredUnmasked := proto.Clone(descriptionRequest).(*opensplunk.UpdateKnowledgeObjectRequest)
	alteredUnmasked.Definition.Name = "different-unmasked-submitted-body"
	if _, err := harness.writer.Update(harness.actorCtx, harness.writeScope, alteredUnmasked); !errors.Is(err, knowledgecatalog.ErrIdempotencyConflict) {
		t.Fatalf("altered unmasked retry error = %v, want ErrIdempotencyConflict", err)
	}
	assertWriterAuthoritySnapshotsEqual(t, readWriterAuthoritySnapshot(t, harness.database), afterDescription)

	moved := proto.Clone(expectedDescriptionDefinition).(*opensplunk.KnowledgeObjectDefinition)
	moved.AppId = writerTestAppTwo
	moved.Name = "masked-update-moved"
	moved.SharingScope = opensplunk.SharingScope_SHARING_SCOPE_APP
	moved.Selector = &opensplunk.KnowledgeSelector{HostPatterns: []*opensplunk.KnowledgeSelectorPattern{{
		MatchKind: opensplunk.KnowledgeSelectorMatchKind_KNOWLEDGE_SELECTOR_MATCH_KIND_EXACT,
		Value:     "moved-host",
	}}}
	movedResponse, err := harness.writer.Update(harness.actorCtx, harness.writeScope, &opensplunk.UpdateKnowledgeObjectRequest{
		KnowledgeObjectId: createdObject.GetKnowledgeObjectId(),
		ExpectedVersion:   2,
		Definition:        moved,
		UpdateMask:        &fieldmaskpb.FieldMask{Paths: []string{"app_id", "name", "selector", "sharing_scope"}},
		ClientRequestId:   "masked-move-request-0001",
	})
	if err != nil {
		t.Fatalf("multi-field Update: %v", err)
	}
	assertWriterProtoObject(t, movedResponse.GetKnowledgeObject(), writerProtoObjectExpectation{
		ID:         createdObject.GetKnowledgeObjectId(),
		Definition: moved,
		Version:    3,
		State:      opensplunk.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_DRAFT,
		CreatedUS:  10_001,
		UpdatedUS:  10_003,
	})

	bodyChanged := proto.Clone(moved).(*opensplunk.KnowledgeObjectDefinition)
	bodyChanged.Body = &opensplunk.KnowledgeObjectDefinition_FieldAlias{FieldAlias: &opensplunk.FieldAliasDefinition{
		SourceField:       "new_source",
		DestinationField:  "new_destination",
		OverwriteBehavior: opensplunk.KnowledgeOverwriteBehavior_KNOWLEDGE_OVERWRITE_BEHAVIOR_PRESERVE_EXISTING,
	}}
	bodyResponse, err := harness.writer.Update(harness.actorCtx, harness.writeScope, &opensplunk.UpdateKnowledgeObjectRequest{
		KnowledgeObjectId: createdObject.GetKnowledgeObjectId(),
		ExpectedVersion:   3,
		Definition:        bodyChanged,
		UpdateMask:        &fieldmaskpb.FieldMask{Paths: []string{"field_alias"}},
		ClientRequestId:   "masked-body-request-0001",
	})
	if err != nil {
		t.Fatalf("same-type body Update: %v", err)
	}
	assertWriterProtoObject(t, bodyResponse.GetKnowledgeObject(), writerProtoObjectExpectation{
		ID:         createdObject.GetKnowledgeObjectId(),
		Definition: bodyChanged,
		Version:    4,
		State:      opensplunk.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_DRAFT,
		CreatedUS:  10_001,
		UpdatedUS:  10_004,
	})
	assertWriterProtoMatchesStored(
		t,
		bodyResponse.GetKnowledgeObject(),
		getWriterObject(t, harness, createdObject.GetKnowledgeObjectId(), nil),
	)

	stable := readWriterAuthoritySnapshot(t, harness.database)
	invalidRequests := []*opensplunk.UpdateKnowledgeObjectRequest{
		{
			KnowledgeObjectId: createdObject.GetKnowledgeObjectId(), ExpectedVersion: 4,
			Definition: proto.Clone(bodyChanged).(*opensplunk.KnowledgeObjectDefinition),
			UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"description"}}, ClientRequestId: "masked-no-op-request-0001",
		},
		{
			KnowledgeObjectId: createdObject.GetKnowledgeObjectId(), ExpectedVersion: 4,
			Definition: proto.Clone(bodyChanged).(*opensplunk.KnowledgeObjectDefinition),
			UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"name", "app_id"}}, ClientRequestId: "masked-unsorted-request-01",
		},
		{
			KnowledgeObjectId: createdObject.GetKnowledgeObjectId(), ExpectedVersion: 4,
			Definition: proto.Clone(bodyChanged).(*opensplunk.KnowledgeObjectDefinition),
			UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"name", "name"}}, ClientRequestId: "masked-duplicate-request-1",
		},
		{
			KnowledgeObjectId: createdObject.GetKnowledgeObjectId(), ExpectedVersion: 4,
			Definition: proto.Clone(bodyChanged).(*opensplunk.KnowledgeObjectDefinition),
			UpdateMask: nil, ClientRequestId: "masked-empty-request-00001",
		},
	}
	for index, invalid := range invalidRequests {
		if _, err := harness.writer.Update(harness.actorCtx, harness.writeScope, invalid); !errors.Is(err, control.ErrInvalidArgument) {
			t.Fatalf("invalid Update %d error = %v, want ErrInvalidArgument", index, err)
		}
		assertWriterAuthoritySnapshotsEqual(t, readWriterAuthoritySnapshot(t, harness.database), stable)
	}

	typeChange := proto.Clone(bodyChanged).(*opensplunk.KnowledgeObjectDefinition)
	typeChange.Body = &opensplunk.KnowledgeObjectDefinition_CalculatedField{CalculatedField: &opensplunk.CalculatedFieldDefinition{
		DestinationField: "calculated_output",
		Expression:       "1 + 1",
	}}
	if _, err := harness.writer.Update(harness.actorCtx, harness.writeScope, &opensplunk.UpdateKnowledgeObjectRequest{
		KnowledgeObjectId: createdObject.GetKnowledgeObjectId(),
		ExpectedVersion:   4,
		Definition:        typeChange,
		UpdateMask:        &fieldmaskpb.FieldMask{Paths: []string{"calculated_field"}},
		ClientRequestId:   "masked-type-change-0001",
	}); !errors.Is(err, control.ErrInvalidArgument) {
		t.Fatalf("type-changing Update error = %v, want ErrInvalidArgument", err)
	}
	assertWriterAuthoritySnapshotsEqual(t, readWriterAuthoritySnapshot(t, harness.database), stable)
	assertWriterAuditEvents(t, knowledgeAuditEvents(t, harness), []writerAuditExpectation{
		{Action: audit.ActionKnowledgeObjectUpdate, ID: createdObject.GetKnowledgeObjectId(), Version: 4, AppID: writerTestAppTwo, Scope: audit.KnowledgeSharingScopeApp},
		{Action: audit.ActionKnowledgeObjectScopeChange, ID: createdObject.GetKnowledgeObjectId(), Version: 3, AppID: writerTestAppTwo, Scope: audit.KnowledgeSharingScopeApp},
		{Action: audit.ActionKnowledgeObjectUpdate, ID: createdObject.GetKnowledgeObjectId(), Version: 2, AppID: writerTestApp, Scope: audit.KnowledgeSharingScopePrivate},
		{Action: audit.ActionKnowledgeObjectCreate, ID: createdObject.GetKnowledgeObjectId(), Version: 1, AppID: writerTestApp, Scope: audit.KnowledgeSharingScopePrivate},
	})
	assertWriterCatalogIntegrity(t, harness.database)
}

func TestWriterDisableDeleteAndHistoryAreImmutable(t *testing.T) {
	harness := newWriterBlackboxHarness(t)
	_, created := harness.createDraft(t, "lifecycle-history", "history-create-request-0001")
	createdObject := created.GetKnowledgeObject()

	disableRequest := &opensplunk.SetKnowledgeObjectStateRequest{
		KnowledgeObjectId: createdObject.GetKnowledgeObjectId(),
		ExpectedVersion:   1,
		State:             opensplunk.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_DISABLED,
		ClientRequestId:   "history-disable-request-001",
	}
	disabled, err := harness.writer.SetState(harness.actorCtx, harness.writeScope, disableRequest)
	if err != nil {
		t.Fatalf("SetState(DISABLED): %v", err)
	}
	assertWriterProtoObject(t, disabled.GetKnowledgeObject(), writerProtoObjectExpectation{
		ID:         createdObject.GetKnowledgeObjectId(),
		Definition: createdObject.GetDefinition(),
		Version:    2,
		State:      opensplunk.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_DISABLED,
		CreatedUS:  10_001,
		UpdatedUS:  10_002,
		DisabledUS: 10_002,
	})
	if !bytes.Equal(disabled.GetKnowledgeObject().GetDefinitionSha256(), createdObject.GetDefinitionSha256()) {
		t.Fatal("disable did not copy the current immutable definition digest")
	}
	disabledCommitted := proto.Clone(disabled).(*opensplunk.SetKnowledgeObjectStateResponse)

	updatedDefinition := proto.Clone(disabled.GetKnowledgeObject().GetDefinition()).(*opensplunk.KnowledgeObjectDefinition)
	updatedDefinition.Description = new("edited while disabled")
	updated, err := harness.writer.Update(harness.actorCtx, harness.writeScope, &opensplunk.UpdateKnowledgeObjectRequest{
		KnowledgeObjectId: createdObject.GetKnowledgeObjectId(),
		ExpectedVersion:   2,
		Definition:        updatedDefinition,
		UpdateMask:        &fieldmaskpb.FieldMask{Paths: []string{"description"}},
		ClientRequestId:   "history-disabled-edit-0001",
	})
	if err != nil {
		t.Fatalf("Update(DISABLED): %v", err)
	}
	assertWriterProtoObject(t, updated.GetKnowledgeObject(), writerProtoObjectExpectation{
		ID:         createdObject.GetKnowledgeObjectId(),
		Definition: updatedDefinition,
		Version:    3,
		State:      opensplunk.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_DISABLED,
		CreatedUS:  10_001,
		UpdatedUS:  10_003,
		DisabledUS: 10_002,
	})

	deleteRequest := &opensplunk.DeleteKnowledgeObjectRequest{
		KnowledgeObjectId: createdObject.GetKnowledgeObjectId(),
		ExpectedVersion:   3,
		ClientRequestId:   "history-delete-request-0001",
	}
	deleted, err := harness.writer.Delete(harness.actorCtx, harness.writeScope, deleteRequest)
	if err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if deleted.GetKnowledgeObjectId() != createdObject.GetKnowledgeObjectId() ||
		deleted.GetDeletedVersion() != 4 {
		t.Fatalf("Delete response = %v", deleted)
	}
	assertWriterResponseCatalog(t, deleted.GetTenantCatalogRevision(), deleted.GetTenantCatalogStateToken(), 4)
	current := getWriterObject(t, harness, createdObject.GetKnowledgeObjectId(), nil)
	if current.State != knowledgecatalog.StateDeleted || current.Version != 4 ||
		current.DisabledAt != nil || current.DeletedAt == nil || current.DeletedAt.UnixMicro() != 10_004 ||
		!proto.Equal(current.Definition, updatedDefinition) ||
		!bytes.Equal(current.DefinitionSHA256, updated.GetKnowledgeObject().GetDefinitionSha256()) {
		t.Fatalf("current deleted object = %#v", current)
	}

	history := []struct {
		version    uint64
		state      knowledgecatalog.State
		definition *opensplunk.KnowledgeObjectDefinition
		disabledUS int64
		updatedUS  int64
	}{
		{1, knowledgecatalog.StateDraft, createdObject.GetDefinition(), 0, 10_001},
		{2, knowledgecatalog.StateDisabled, createdObject.GetDefinition(), 10_002, 10_002},
		{3, knowledgecatalog.StateDisabled, updatedDefinition, 10_002, 10_003},
	}
	for _, expected := range history {
		got := getWriterObject(t, harness, createdObject.GetKnowledgeObjectId(), &expected.version)
		if got.Version != expected.version || got.State != expected.state ||
			got.UpdatedAt.UnixMicro() != expected.updatedUS || !proto.Equal(got.Definition, expected.definition) {
			t.Fatalf("historical version %d = %#v", expected.version, got)
		}
		if expected.disabledUS == 0 && got.DisabledAt != nil ||
			expected.disabledUS != 0 && (got.DisabledAt == nil || got.DisabledAt.UnixMicro() != expected.disabledUS) {
			t.Fatalf("historical version %d disabled_at = %v, want %d", expected.version, got.DisabledAt, expected.disabledUS)
		}
	}

	disableReplay, err := harness.writer.SetState(harness.actorCtx, harness.writeScope, disableRequest)
	if err != nil || !proto.Equal(disableReplay, disabledCommitted) {
		t.Fatalf("disable replay after delete = (%v, %v), want %v", disableReplay, err, disabledCommitted)
	}
	deleteCommitted := proto.Clone(deleted).(*opensplunk.DeleteKnowledgeObjectResponse)
	deleteReplay, err := harness.writer.Delete(harness.actorCtx, harness.writeScope, deleteRequest)
	if err != nil || !proto.Equal(deleteReplay, deleteCommitted) {
		t.Fatalf("delete replay = (%v, %v), want %v", deleteReplay, err, deleteCommitted)
	}
	stable := readWriterAuthoritySnapshot(t, harness.database)
	if _, err := harness.writer.Update(harness.actorCtx, harness.writeScope, &opensplunk.UpdateKnowledgeObjectRequest{
		KnowledgeObjectId: createdObject.GetKnowledgeObjectId(),
		ExpectedVersion:   4,
		Definition:        updatedDefinition,
		UpdateMask:        &fieldmaskpb.FieldMask{Paths: []string{"description"}},
		ClientRequestId:   "history-after-delete-0001",
	}); !errors.Is(err, control.ErrVersionConflict) {
		t.Fatalf("authorized Update after delete error = %v, want ErrVersionConflict", err)
	}
	if _, err := harness.writer.Delete(harness.actorCtx, harness.writeScope, &opensplunk.DeleteKnowledgeObjectRequest{
		KnowledgeObjectId: createdObject.GetKnowledgeObjectId(),
		ExpectedVersion:   4,
		ClientRequestId:   "history-delete-terminal-001",
	}); !errors.Is(err, control.ErrVersionConflict) {
		t.Fatalf("authorized Delete after delete error = %v, want ErrVersionConflict", err)
	}
	assertWriterAuthoritySnapshotsEqual(t, readWriterAuthoritySnapshot(t, harness.database), stable)

	if stable.CatalogRevision != 4 || stable.IdentityCount != 1 || stable.VersionCount != 4 ||
		stable.IdempotencyCount != 4 || stable.ActiveObjectCount != 0 ||
		stable.AuditNextSequence != 5 || stable.AuditEventCount != 4 {
		t.Fatalf("lifecycle authority ledgers = %#v", stable)
	}
	assertWriterTableCounts(t, stable, map[string]int64{
		"knowledge_definition_blobs":              2,
		"knowledge_objects":                       1,
		"knowledge_object_versions":               4,
		"knowledge_object_version_lifecycle":      4,
		"knowledge_object_dependencies":           0,
		"knowledge_object_dependency_seals":       4,
		"knowledge_object_list_projections":       1,
		"knowledge_object_list_selector_patterns": 1,
		"knowledge_object_list_projection_seals":  1,
		"knowledge_object_list_order_keys":        1,
		"knowledge_mutation_commit_authorities":   4,
		"knowledge_mutation_idempotency":          4,
		"audit_events":                            4,
	})
	deleteReference, _ := readCompactWriterOutcome(t, harness.database, "objects.delete", deleteRequest.GetClientRequestId())
	if deleteReference.GetKnowledgeObjectId() != createdObject.GetKnowledgeObjectId() ||
		deleteReference.GetVersion() != 4 ||
		!bytes.Equal(deleteReference.GetDefinitionSha256(), current.DefinitionSHA256) {
		t.Fatalf("delete compact outcome = %v", deleteReference)
	}
	assertWriterAuditEvents(t, knowledgeAuditEvents(t, harness), []writerAuditExpectation{
		{Action: audit.ActionKnowledgeObjectDelete, ID: createdObject.GetKnowledgeObjectId(), Version: 4, AppID: writerTestApp, Scope: audit.KnowledgeSharingScopePrivate},
		{Action: audit.ActionKnowledgeObjectUpdate, ID: createdObject.GetKnowledgeObjectId(), Version: 3, AppID: writerTestApp, Scope: audit.KnowledgeSharingScopePrivate},
		{Action: audit.ActionKnowledgeObjectDisable, ID: createdObject.GetKnowledgeObjectId(), Version: 2, AppID: writerTestApp, Scope: audit.KnowledgeSharingScopePrivate},
		{Action: audit.ActionKnowledgeObjectCreate, ID: createdObject.GetKnowledgeObjectId(), Version: 1, AppID: writerTestApp, Scope: audit.KnowledgeSharingScopePrivate},
	})
	assertWriterCatalogIntegrity(t, harness.database)
}

type writerProtoObjectExpectation struct {
	ID         string
	Definition *opensplunk.KnowledgeObjectDefinition
	Version    uint64
	State      opensplunk.KnowledgeObjectState
	CreatedUS  int64
	UpdatedUS  int64
	DisabledUS int64
}

func assertWriterProtoObject(t *testing.T, object *opensplunk.KnowledgeObject, want writerProtoObjectExpectation) {
	t.Helper()
	if object == nil {
		t.Fatal("writer returned a nil knowledge object")
	}
	if object.GetKnowledgeObjectId() != want.ID ||
		object.GetTenantId() != writerTestTenant ||
		object.GetAppId() != want.Definition.GetAppId() ||
		object.GetOwnerId() != writerTestOwner ||
		object.GetObjectType() != opensplunk.KnowledgeObjectType_KNOWLEDGE_OBJECT_TYPE_FIELD_ALIAS ||
		object.GetName() != want.Definition.GetName() ||
		object.GetVersion() != want.Version ||
		object.GetSharingScope() != want.Definition.GetSharingScope() ||
		object.GetState() != want.State ||
		!proto.Equal(object.GetDefinition(), want.Definition) ||
		len(object.GetDefinitionSha256()) != 32 {
		t.Fatalf("writer object = %v, want = %#v", object, want)
	}
	if object.GetCreatedAt() == nil || object.GetUpdatedAt() == nil ||
		object.GetCreatedAt().CheckValid() != nil || object.GetUpdatedAt().CheckValid() != nil ||
		object.GetCreatedAt().AsTime().UnixMicro() != want.CreatedUS ||
		object.GetUpdatedAt().AsTime().UnixMicro() != want.UpdatedUS {
		t.Fatalf("writer object timestamps = created %v updated %v, want %d/%d", object.GetCreatedAt(), object.GetUpdatedAt(), want.CreatedUS, want.UpdatedUS)
	}
	if want.DisabledUS == 0 && object.GetDisabledAt() != nil ||
		want.DisabledUS != 0 && (object.GetDisabledAt() == nil || object.GetDisabledAt().AsTime().UnixMicro() != want.DisabledUS) {
		t.Fatalf("writer object disabled_at = %v, want %d", object.GetDisabledAt(), want.DisabledUS)
	}
	if object.GetQuarantinedAt() != nil || object.GetDeletedAt() != nil || object.QuarantineReason != nil {
		t.Fatalf("ordinary writer object has terminal metadata: %v", object)
	}
}

func assertWriterResponseCatalog(t *testing.T, revision uint64, token []byte, wantRevision uint64) {
	t.Helper()
	if revision != wantRevision || len(token) != 32 || bytes.Equal(token, make([]byte, 32)) {
		t.Fatalf("response catalog snapshot = revision %d token %x, want revision %d and nonzero 32-byte token", revision, token, wantRevision)
	}
}

func getWriterObject(
	t *testing.T,
	harness *writerBlackboxHarness,
	objectID string,
	version *uint64,
) knowledgecatalog.Object {
	t.Helper()
	object, err := harness.reader.Get(harness.actorCtx, harness.readScope, objectID, version)
	if err != nil {
		t.Fatalf("Store.Get(%q, %v): %v", objectID, version, err)
	}
	return object
}

func assertWriterProtoMatchesStored(
	t *testing.T,
	object *opensplunk.KnowledgeObject,
	stored knowledgecatalog.Object,
) {
	t.Helper()
	if object.GetKnowledgeObjectId() != stored.KnowledgeObjectID ||
		object.GetTenantId() != stored.TenantID ||
		object.GetAppId() != stored.AppID ||
		object.GetOwnerId() != stored.OwnerID ||
		object.GetName() != stored.Name || object.GetVersion() != stored.Version ||
		!proto.Equal(object.GetDefinition(), stored.Definition) ||
		!bytes.Equal(object.GetDefinitionSha256(), stored.DefinitionSHA256) ||
		object.GetCreatedAt().AsTime() != stored.CreatedAt ||
		object.GetUpdatedAt().AsTime() != stored.UpdatedAt {
		t.Fatalf("writer proto object = %v, stored object = %#v", object, stored)
	}
}

func assertWriterTableCounts(t *testing.T, snapshot writerAuthoritySnapshot, want map[string]int64) {
	t.Helper()
	for table, count := range want {
		if snapshot.TableCounts[table] != count {
			t.Errorf("%s rows = %d, want %d", table, snapshot.TableCounts[table], count)
		}
	}
}

type writerAuditExpectation struct {
	Action  audit.Action
	ID      string
	Version uint64
	AppID   string
	Scope   audit.KnowledgeSharingScope
}

func assertWriterAuditEvents(t *testing.T, got []audit.Event, want []writerAuditExpectation) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("knowledge audit event count = %d, want %d: %#v", len(got), len(want), got)
	}
	for index := range want {
		event := got[index]
		expected := want[index]
		if event.Sequence != uint64(len(want)-index) || event.TenantID != writerTestTenant ||
			event.Actor.ID == "" || event.Action != expected.Action ||
			event.TargetKind != audit.TargetKindKnowledgeObject || event.TargetID != expected.ID ||
			event.TargetVersion != expected.Version || event.KnowledgeObject.AppID != expected.AppID ||
			event.KnowledgeObject.ObjectType != audit.KnowledgeObjectTypeFieldAlias ||
			event.KnowledgeObject.SharingScope != expected.Scope ||
			event.OccurredAt.Location() != time.UTC {
			t.Errorf("knowledge audit event %d = %#v, want %#v", index, event, expected)
		}
	}
}
