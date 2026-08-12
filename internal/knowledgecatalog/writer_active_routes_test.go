package knowledgecatalog

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/audit"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

const (
	writerActiveRouteTargetA = "ko-writer-active-route-target-a"
	writerActiveRouteTargetB = "ko-writer-active-route-target-b"
)

type writerActiveRouteHarness struct {
	database     *control.DB
	writer       *Writer
	actorContext context.Context
	scope        WriteScope
}

func newWriterActiveRouteHarness(t *testing.T) writerActiveRouteHarness {
	t.Helper()
	harness := newWriterActiveRouteEmptyHarness(t)
	insertFixtureObject(t, harness.database, fixtureObject{
		id: writerActiveRouteTargetA, owner: testOwner,
		versions: []fixtureVersion{{
			definition: writerActiveRouteDefinition(dependencyExtractionDefinition(
				testApp, "writer-active-target-a", SharingScopePrivate, nil,
				"writer-active-edge-a", dependencyFixtureInputField,
			), "main"),
			state: StateActive, mutation: "create", timestamp: 10,
		}},
	})
	insertFixtureObject(t, harness.database, fixtureObject{
		id: writerActiveRouteTargetB, owner: testOwner,
		versions: []fixtureVersion{{
			definition: writerActiveRouteDefinition(dependencyExtractionDefinition(
				testApp, "writer-active-target-b", SharingScopePrivate, nil,
				"writer-active-edge-b", dependencyFixtureInputField,
			), "main"),
			state: StateActive, mutation: "create", timestamp: 20,
		}},
	})
	return harness
}

func newWriterActiveRouteEmptyHarness(t *testing.T) writerActiveRouteHarness {
	t.Helper()
	database, _ := newCatalogTestStore(t)
	createPublicationTransitionTestIndex(t, database, "main")
	auditStore, err := audit.NewStore(database, audit.StoreOptions{CursorKey: testCursorKey})
	if err != nil {
		t.Fatalf("audit.NewStore(): %v", err)
	}
	var idCalls atomic.Int64
	var clockCalls atomic.Int64
	writer, err := NewWriter(database, auditStore, WriterOptions{
		Clock: func() time.Time {
			return time.UnixMicro(100 + clockCalls.Add(1)).UTC()
		},
		IDGenerator: func() (string, error) {
			return fmt.Sprintf("ko-writer-active-route-%04d", idCalls.Add(1)), nil
		},
	})
	if err != nil {
		t.Fatalf("NewWriter(): %v", err)
	}
	actorContext, err := audit.WithActor(t.Context(), audit.Actor{
		Kind: audit.ActorKindBrowser,
		ID:   "writer-active-route-administrator",
		Role: audit.ActorRoleAdministrator,
	})
	if err != nil {
		t.Fatalf("audit.WithActor(): %v", err)
	}
	return writerActiveRouteHarness{
		database:     database,
		writer:       writer,
		actorContext: actorContext,
		scope: WriteScope{
			TenantID:       testTenant,
			OwnerID:        testOwner,
			WritableAppIDs: []string{testApp},
		},
	}
}

func TestWriterRecognizedActiveCreateUpdateAndEnable(t *testing.T) {
	harness := newWriterActiveRouteHarness(t)
	createRequest := &opensplunkv1.CreateKnowledgeObjectRequest{
		Definition: writerActiveRouteDefinition(dependencyAliasDefinition(
			testApp, "writer-active-candidate", SharingScopePrivate, nil,
			"writer-active-edge-a", dependencyFixtureInputField, "active_alias_one",
		), "main"),
		InitialState:    opensplunkv1.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_ACTIVE,
		ClientRequestId: "writer-active-create-0001",
	}
	created, err := harness.writer.Create(harness.actorContext, harness.scope, createRequest)
	if err != nil {
		t.Fatalf("Create(ACTIVE): %v", err)
	}
	createdObject := created.GetKnowledgeObject()
	if createdObject.GetState() != opensplunkv1.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_ACTIVE ||
		createdObject.GetVersion() != 1 || !proto.Equal(
		createdObject.GetDefinition(),
		mustNormalizeWriterActiveRouteDefinition(t, createRequest.GetDefinition()),
	) {
		t.Fatalf("Create(ACTIVE) object = %v", createdObject)
	}
	assertWriterActiveRouteVersion(
		t, harness.database, createdObject.GetKnowledgeObjectId(), 1,
		StateActive, "create", writerActiveRouteTargetA,
	)
	assertWriterActiveRouteReplay(t, harness, mutationRouteCreate, createRequest, created)

	updatedDefinition := proto.Clone(createdObject.GetDefinition()).(*opensplunkv1.KnowledgeObjectDefinition)
	updatedDefinition.Selector = writerActiveRouteSelector("main", "writer-active-edge-b")
	updateRequest := &opensplunkv1.UpdateKnowledgeObjectRequest{
		KnowledgeObjectId: createdObject.GetKnowledgeObjectId(),
		ExpectedVersion:   1,
		Definition:        updatedDefinition,
		UpdateMask:        &fieldmaskpb.FieldMask{Paths: []string{"selector"}},
		ClientRequestId:   "writer-active-update-0001",
	}
	updated, err := harness.writer.Update(harness.actorContext, harness.scope, updateRequest)
	if err != nil {
		t.Fatalf("Update(ACTIVE): %v", err)
	}
	if updated.GetKnowledgeObject().GetState() != opensplunkv1.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_ACTIVE ||
		updated.GetKnowledgeObject().GetVersion() != 2 ||
		!proto.Equal(
			updated.GetKnowledgeObject().GetDefinition(),
			mustNormalizeWriterActiveRouteDefinition(t, updatedDefinition),
		) {
		t.Fatalf("Update(ACTIVE) object = %v", updated.GetKnowledgeObject())
	}
	assertWriterActiveRouteVersion(
		t, harness.database, createdObject.GetKnowledgeObjectId(), 2,
		StateActive, "update", writerActiveRouteTargetB,
	)
	assertWriterActiveRouteReplay(t, harness, mutationRouteUpdate, updateRequest, updated)

	draftRequest := &opensplunkv1.CreateKnowledgeObjectRequest{
		Definition: writerActiveRouteDefinition(dependencyAliasDefinition(
			testApp, "writer-enable-candidate", SharingScopePrivate, nil,
			"writer-active-edge-a", dependencyFixtureInputField, "active_alias_two",
		), "main"),
		InitialState:    opensplunkv1.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_DRAFT,
		ClientRequestId: "writer-enable-draft-0001",
	}
	draft, err := harness.writer.Create(harness.actorContext, harness.scope, draftRequest)
	if err != nil {
		t.Fatalf("Create(DRAFT for enable): %v", err)
	}
	assertWriterActiveRouteVersion(
		t, harness.database, draft.GetKnowledgeObject().GetKnowledgeObjectId(), 1,
		StateDraft, "create", "",
	)
	enableRequest := &opensplunkv1.SetKnowledgeObjectStateRequest{
		KnowledgeObjectId: draft.GetKnowledgeObject().GetKnowledgeObjectId(),
		ExpectedVersion:   1,
		State:             opensplunkv1.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_ACTIVE,
		ClientRequestId:   "writer-active-enable-0001",
	}
	enabled, err := harness.writer.SetState(harness.actorContext, harness.scope, enableRequest)
	if err != nil {
		t.Fatalf("SetState(ACTIVE): %v", err)
	}
	if enabled.GetKnowledgeObject().GetState() != opensplunkv1.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_ACTIVE ||
		enabled.GetKnowledgeObject().GetDisabledAt() != nil ||
		!proto.Equal(
			enabled.GetKnowledgeObject().GetDefinition(),
			mustNormalizeWriterActiveRouteDefinition(t, draftRequest.GetDefinition()),
		) {
		t.Fatalf("SetState(ACTIVE) object = %v", enabled.GetKnowledgeObject())
	}
	assertWriterActiveRouteVersion(
		t, harness.database, draft.GetKnowledgeObject().GetKnowledgeObjectId(), 2,
		StateActive, "enable", writerActiveRouteTargetA,
	)
	assertWriterActiveRouteReplay(t, harness, mutationRouteSetState, enableRequest, enabled)

	disabledDraftRequest := &opensplunkv1.CreateKnowledgeObjectRequest{
		Definition: writerActiveRouteDefinition(dependencyAliasDefinition(
			testApp, "writer-disabled-enable-candidate", SharingScopePrivate, nil,
			"writer-active-edge-b", dependencyFixtureInputField, "active_alias_three",
		), "main"),
		InitialState:    opensplunkv1.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_DRAFT,
		ClientRequestId: "writer-disabled-draft-0001",
	}
	disabledDraft, err := harness.writer.Create(
		harness.actorContext,
		harness.scope,
		disabledDraftRequest,
	)
	if err != nil {
		t.Fatalf("Create(DRAFT for disabled enable): %v", err)
	}
	disableRequest := &opensplunkv1.SetKnowledgeObjectStateRequest{
		KnowledgeObjectId: disabledDraft.GetKnowledgeObject().GetKnowledgeObjectId(),
		ExpectedVersion:   1,
		State:             opensplunkv1.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_DISABLED,
		ClientRequestId:   "writer-disable-before-enable-0001",
	}
	disabled, err := harness.writer.SetState(harness.actorContext, harness.scope, disableRequest)
	if err != nil || disabled.GetKnowledgeObject().GetDisabledAt() == nil {
		t.Fatalf("SetState(DISABLED before enable) = (%v, %v)", disabled, err)
	}
	assertWriterActiveRouteVersion(
		t, harness.database, disabledDraft.GetKnowledgeObject().GetKnowledgeObjectId(), 2,
		StateDisabled, "disable", "",
	)
	disabledEnableRequest := &opensplunkv1.SetKnowledgeObjectStateRequest{
		KnowledgeObjectId: disabledDraft.GetKnowledgeObject().GetKnowledgeObjectId(),
		ExpectedVersion:   2,
		State:             opensplunkv1.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_ACTIVE,
		ClientRequestId:   "writer-disabled-enable-0001",
	}
	disabledEnabled, err := harness.writer.SetState(
		harness.actorContext,
		harness.scope,
		disabledEnableRequest,
	)
	if err != nil {
		t.Fatalf("SetState(DISABLED->ACTIVE): %v", err)
	}
	if disabledEnabled.GetKnowledgeObject().GetState() != opensplunkv1.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_ACTIVE ||
		disabledEnabled.GetKnowledgeObject().GetDisabledAt() != nil ||
		!proto.Equal(
			disabledEnabled.GetKnowledgeObject().GetDefinition(),
			mustNormalizeWriterActiveRouteDefinition(t, disabledDraftRequest.GetDefinition()),
		) {
		t.Fatalf("SetState(DISABLED->ACTIVE) object = %v", disabledEnabled.GetKnowledgeObject())
	}
	assertWriterActiveRouteVersion(
		t, harness.database, disabledDraft.GetKnowledgeObject().GetKnowledgeObjectId(), 3,
		StateActive, "enable", writerActiveRouteTargetB,
	)
	assertWriterActiveRouteReplay(
		t,
		harness,
		mutationRouteSetState,
		disabledEnableRequest,
		disabledEnabled,
	)

	archiveWriterActiveRouteApp(t, harness.database, true)
	assertWriterActiveRouteReplay(t, harness, mutationRouteCreate, createRequest, created)
	assertWriterActiveRouteReplay(t, harness, mutationRouteUpdate, updateRequest, updated)
	assertWriterActiveRouteReplay(t, harness, mutationRouteSetState, enableRequest, enabled)
	assertWriterActiveRouteReplay(
		t,
		harness,
		mutationRouteSetState,
		disabledEnableRequest,
		disabledEnabled,
	)
}

func TestWriterActiveTransitionRejectionsPrecedePublicationHooks(t *testing.T) {
	t.Run("no current-index witness", func(t *testing.T) {
		harness := newWriterActiveRouteHarness(t)
		request := &opensplunkv1.CreateKnowledgeObjectRequest{
			Definition: writerActiveRouteDefinition(dependencyAliasDefinition(
				testApp, "writer-no-witness", SharingScopePrivate, nil,
				"writer-no-witness-host", "source", "no_witness_alias",
			), "index-that-does-not-exist"),
			InitialState:    opensplunkv1.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_ACTIVE,
			ClientRequestId: "writer-no-witness-0001",
		}
		before, err := readCatalogState(harness.database.GORMDB(), testTenant)
		if err != nil {
			t.Fatalf("read catalog before rejection: %v", err)
		}
		var capacityHooks int
		harness.writer.hook = func(_ context.Context, event writerHookEvent) error {
			if writerActiveRouteLateHook(event.Boundary) {
				capacityHooks++
			}
			return nil
		}
		if _, err := harness.writer.Create(harness.actorContext, harness.scope, request); !errors.Is(err, control.ErrDependencyConflict) {
			t.Fatalf("Create(ACTIVE without witness) error = %v, want dependency conflict", err)
		}
		assertWriterActiveRouteRejected(t, harness, before, capacityHooks)
	})

	for _, scope := range []SharingScope{SharingScopePrivate, SharingScopeApp, SharingScopeGlobal} {
		t.Run("dormant name collision "+string(scope), func(t *testing.T) {
			harness := newWriterActiveRouteHarness(t)
			name := "writer-dormant-collision-" + string(scope)
			definition := writerActiveRouteDefinition(
				dependencyAliasDefinition(
					testApp, name, scope, nil, "dormant-host", "source", "dormant_alias",
				),
				"index-that-does-not-exist",
			)
			insertFixtureObject(t, harness.database, fixtureObject{
				id: "ko-" + name, owner: testOwner,
				versions: []fixtureVersion{{
					definition: definition, state: StateActive, mutation: "create", timestamp: 50,
				}},
			})
			before, err := readCatalogState(harness.database.GORMDB(), testTenant)
			if err != nil {
				t.Fatalf("read catalog before collision: %v", err)
			}
			var capacityHooks int
			harness.writer.hook = func(_ context.Context, event writerHookEvent) error {
				if writerActiveRouteLateHook(event.Boundary) {
					capacityHooks++
				}
				return nil
			}
			_, err = harness.writer.Create(
				harness.actorContext,
				harness.scope,
				&opensplunkv1.CreateKnowledgeObjectRequest{
					Definition:      proto.Clone(definition).(*opensplunkv1.KnowledgeObjectDefinition),
					InitialState:    opensplunkv1.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_ACTIVE,
					ClientRequestId: "writer-collision-" + string(scope) + "-0001",
				},
			)
			if !errors.Is(err, control.ErrAlreadyExists) {
				t.Fatalf("Create(ACTIVE collision) error = %v, want already exists", err)
			}
			assertWriterActiveRouteRejected(t, harness, before, capacityHooks)
		})
	}
}

func TestWriterActiveUpdateRejectsExistingWinnerDependencyDrift(t *testing.T) {
	harness := newWriterActiveRouteHarness(t)
	const (
		lowerID     = "ko-writer-drift-lower"
		candidateID = "ko-writer-drift-candidate"
		consumerID  = "ko-writer-drift-consumer"
	)
	insertFixtureObject(t, harness.database, fixtureObject{
		id: lowerID, owner: testOwner,
		versions: []fixtureVersion{{
			definition: writerActiveRouteDefinition(dependencyExtractionDefinition(
				testApp, "writer-drift-slot", SharingScopeGlobal, nil,
				"writer-drift-host", "writer_drift_input",
			), "main"),
			state: StateActive, mutation: "create", timestamp: 40,
		}},
	})
	insertFixtureObject(t, harness.database, fixtureObject{
		id: candidateID, owner: testOwner,
		versions: []fixtureVersion{{
			definition: writerActiveRouteDefinition(dependencyExtractionDefinition(
				testApp, "writer-z-drift-candidate", SharingScopePrivate, nil,
				"writer-drift-host", "writer_candidate_old_output",
			), "main"),
			state: StateActive, mutation: "create", timestamp: 50,
		}},
	})
	insertFixtureObject(t, harness.database, fixtureObject{
		id: consumerID, owner: testOwner,
		versions: []fixtureVersion{{
			definition: writerActiveRouteDefinition(dependencyAliasDefinition(
				testApp, "writer-drift-consumer", SharingScopePrivate, nil,
				"writer-drift-host", "writer_drift_input", "writer_drift_output",
			), "main"),
			state: StateActive, mutation: "create", timestamp: 60,
			dependencies: []fixtureDependency{{targetObjectID: lowerID, targetVersion: 1}},
		}},
	})
	before, err := readCatalogState(harness.database.GORMDB(), testTenant)
	if err != nil {
		t.Fatalf("read catalog before dependency drift: %v", err)
	}
	var capacityHooks int
	harness.writer.hook = func(_ context.Context, event writerHookEvent) error {
		if writerActiveRouteLateHook(event.Boundary) {
			capacityHooks++
		}
		return nil
	}
	definition := writerActiveRouteDefinition(dependencyExtractionDefinition(
		testApp, "writer-z-drift-candidate", SharingScopePrivate, nil,
		"writer-drift-host", "writer_drift_input",
	), "main")
	_, err = harness.writer.Update(
		harness.actorContext,
		harness.scope,
		&opensplunkv1.UpdateKnowledgeObjectRequest{
			KnowledgeObjectId: candidateID,
			ExpectedVersion:   1,
			Definition:        definition,
			UpdateMask:        &fieldmaskpb.FieldMask{Paths: []string{"field_extraction"}},
			ClientRequestId:   "writer-dependency-drift-0001",
		},
	)
	if !errors.Is(err, control.ErrDependencyConflict) {
		t.Fatalf("ACTIVE Update dependency drift error = %v, want dependency conflict", err)
	}
	assertWriterActiveRouteRejected(t, harness, before, capacityHooks)
}

func TestWriterActiveUpdateDependentPrecedesMalformedInventory(t *testing.T) {
	harness := newWriterActiveRouteHarness(t)
	const (
		candidateID = "ko-writer-inbound-candidate"
		consumerID  = "ko-writer-inbound-consumer"
	)
	insertFixtureObject(t, harness.database, fixtureObject{
		id: candidateID, owner: testOwner,
		versions: []fixtureVersion{{
			definition: writerActiveRouteDefinition(dependencyExtractionDefinition(
				testApp, "writer-inbound-candidate", SharingScopePrivate, nil,
				"writer-inbound-host", "writer_inbound_input",
			), "main"),
			state: StateActive, mutation: "create", timestamp: 40,
		}},
	})
	insertFixtureObject(t, harness.database, fixtureObject{
		id: consumerID, owner: testOwner,
		versions: []fixtureVersion{{
			definition: writerActiveRouteDefinition(dependencyAliasDefinition(
				testApp, "writer-inbound-consumer", SharingScopePrivate, nil,
				"writer-inbound-host", "writer_inbound_input", "writer_inbound_output",
			), "main"),
			state: StateActive, mutation: "create", timestamp: 50,
			dependencies: []fixtureDependency{{targetObjectID: candidateID, targetVersion: 1}},
		}},
	})
	dropTrigger(t, harness.database, "app_catalog_revision_delete_is_forbidden")
	mustExec(t, harness.database, "DELETE FROM app_catalog_revisions WHERE tenant_id = ?", testTenant)
	before, err := readCatalogState(harness.database.GORMDB(), testTenant)
	if err != nil {
		t.Fatalf("read catalog before dependent precedence: %v", err)
	}
	var capacityHooks int
	harness.writer.hook = func(_ context.Context, event writerHookEvent) error {
		if writerActiveRouteLateHook(event.Boundary) {
			capacityHooks++
		}
		return nil
	}
	definition := writerActiveRouteDefinition(dependencyExtractionDefinition(
		testApp, "writer-inbound-candidate", SharingScopePrivate,
		stringPointer("candidate changed after malformed inventory"),
		"writer-inbound-host", "writer_inbound_input",
	), "main")
	_, err = harness.writer.Update(
		harness.actorContext,
		harness.scope,
		&opensplunkv1.UpdateKnowledgeObjectRequest{
			KnowledgeObjectId: candidateID,
			ExpectedVersion:   1,
			Definition:        definition,
			UpdateMask:        &fieldmaskpb.FieldMask{Paths: []string{"description"}},
			ClientRequestId:   "writer-dependent-precedence-0001",
		},
	)
	if !errors.Is(err, control.ErrDependencyConflict) {
		t.Fatalf("ACTIVE Update dependent precedence error = %v, want dependency conflict", err)
	}
	assertWriterActiveRouteRejected(t, harness, before, capacityHooks)
}

func TestWriterActiveRoutesRequireActiveDefiningApp(t *testing.T) {
	t.Run("create", func(t *testing.T) {
		harness := newWriterActiveRouteEmptyHarness(t)
		if err := ensureMutationLedgers(harness.database.GORMDB(), testTenant); err != nil {
			t.Fatalf("ensure create app-authority ledgers: %v", err)
		}
		archiveWriterActiveRouteApp(t, harness.database, false)
		before, err := readCatalogState(harness.database.GORMDB(), testTenant)
		if err != nil {
			t.Fatalf("read catalog before archived-app create: %v", err)
		}
		var capacityHooks int
		harness.writer.hook = func(_ context.Context, event writerHookEvent) error {
			if writerActiveRouteLateHook(event.Boundary) {
				capacityHooks++
			}
			return nil
		}
		_, err = harness.writer.Create(
			harness.actorContext,
			harness.scope,
			&opensplunkv1.CreateKnowledgeObjectRequest{
				Definition: writerActiveRouteDefinition(dependencyExtractionDefinition(
					testApp, "writer-archived-app-create", SharingScopePrivate, nil,
					"writer-archived-app-host", "archived_app_output",
				), "main"),
				InitialState:    opensplunkv1.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_ACTIVE,
				ClientRequestId: "writer-archived-create-0001",
			},
		)
		if !errors.Is(err, control.ErrNotFound) {
			t.Fatalf("Create(ACTIVE archived app) error = %v, want not found", err)
		}
		assertWriterActiveRouteRejected(t, harness, before, capacityHooks)
	})

	t.Run("enable", func(t *testing.T) {
		harness := newWriterActiveRouteEmptyHarness(t)
		draftRequest := &opensplunkv1.CreateKnowledgeObjectRequest{
			Definition: writerActiveRouteDefinition(dependencyExtractionDefinition(
				testApp, "writer-archived-app-enable", SharingScopePrivate, nil,
				"writer-archived-app-host", "archived_app_output",
			), "main"),
			InitialState:    opensplunkv1.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_DRAFT,
			ClientRequestId: "writer-archived-draft-0001",
		}
		draft, err := harness.writer.Create(harness.actorContext, harness.scope, draftRequest)
		if err != nil {
			t.Fatalf("Create(DRAFT archived-app enable fixture): %v", err)
		}
		archiveWriterActiveRouteApp(t, harness.database, false)
		before, err := readCatalogState(harness.database.GORMDB(), testTenant)
		if err != nil {
			t.Fatalf("read catalog before archived-app enable: %v", err)
		}
		var capacityHooks int
		harness.writer.hook = func(_ context.Context, event writerHookEvent) error {
			if writerActiveRouteLateHook(event.Boundary) {
				capacityHooks++
			}
			return nil
		}
		_, err = harness.writer.SetState(
			harness.actorContext,
			harness.scope,
			&opensplunkv1.SetKnowledgeObjectStateRequest{
				KnowledgeObjectId: draft.GetKnowledgeObject().GetKnowledgeObjectId(),
				ExpectedVersion:   1,
				State:             opensplunkv1.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_ACTIVE,
				ClientRequestId:   "writer-archived-enable-0001",
			},
		)
		if !errors.Is(err, control.ErrNotFound) {
			t.Fatalf("SetState(ACTIVE archived app) error = %v, want not found", err)
		}
		assertWriterActiveRouteRejected(t, harness, before, capacityHooks)
	})

	t.Run("update", func(t *testing.T) {
		harness := newWriterActiveRouteHarness(t)
		const candidateID = "ko-writer-archived-app-update"
		definition := writerActiveRouteDefinition(dependencyExtractionDefinition(
			testApp, "writer-archived-app-update", SharingScopePrivate, nil,
			"writer-archived-update-host", "archived_update_output",
		), "main")
		insertFixtureObject(t, harness.database, fixtureObject{
			id: candidateID, owner: testOwner,
			versions: []fixtureVersion{{
				definition: definition, state: StateActive, mutation: "create", timestamp: 40,
			}},
		})
		archiveWriterActiveRouteApp(t, harness.database, true)
		before, err := readCatalogState(harness.database.GORMDB(), testTenant)
		if err != nil {
			t.Fatalf("read catalog before archived-app update: %v", err)
		}
		var capacityHooks int
		harness.writer.hook = func(_ context.Context, event writerHookEvent) error {
			if writerActiveRouteLateHook(event.Boundary) {
				capacityHooks++
			}
			return nil
		}
		updatedDefinition := proto.Clone(definition).(*opensplunkv1.KnowledgeObjectDefinition)
		updatedDefinition.Description = stringPointer("archived app update")
		_, err = harness.writer.Update(
			harness.actorContext,
			harness.scope,
			&opensplunkv1.UpdateKnowledgeObjectRequest{
				KnowledgeObjectId: candidateID,
				ExpectedVersion:   1,
				Definition:        updatedDefinition,
				UpdateMask:        &fieldmaskpb.FieldMask{Paths: []string{"description"}},
				ClientRequestId:   "writer-archived-update-0001",
			},
		)
		if !errors.Is(err, control.ErrNotFound) {
			t.Fatalf("Update(ACTIVE archived app) error = %v, want not found", err)
		}
		assertWriterActiveRouteRejected(t, harness, before, capacityHooks)
	})
}

func TestWriterActiveUpdateAndEnableRejectOpaqueFutureBodies(t *testing.T) {
	t.Run("update ACTIVE", func(t *testing.T) {
		database, _ := newCatalogTestStore(t)
		createPublicationTransitionTestIndex(t, database, "main")
		fixture := newWriterActiveOpaqueDefinition(t, "writer-opaque-active-update")
		const objectID = "ko-writer-opaque-active-update"
		insertIntegrationFutureObject(t, database, objectID, StateActive, fixture, 20)
		writer, actorContext, scope := newWriterOpaqueEmergencyHarness(t, database)
		before, err := readCatalogState(database.GORMDB(), testTenant)
		if err != nil {
			t.Fatalf("read catalog before opaque ACTIVE update: %v", err)
		}
		var capacityHooks int
		writer.hook = func(_ context.Context, event writerHookEvent) error {
			if writerActiveRouteLateHook(event.Boundary) {
				capacityHooks++
			}
			return nil
		}
		incoming := &opensplunkv1.KnowledgeObjectDefinition{
			Description: stringPointer("opaque ACTIVE update is forbidden"),
		}
		_, err = writer.Update(actorContext, scope, &opensplunkv1.UpdateKnowledgeObjectRequest{
			KnowledgeObjectId: objectID,
			ExpectedVersion:   1,
			Definition:        incoming,
			UpdateMask:        &fieldmaskpb.FieldMask{Paths: []string{"description"}},
			ClientRequestId:   "writer-opaque-update-0001",
		})
		if !errors.Is(err, control.ErrInvalidArgument) {
			t.Fatalf("Update(opaque ACTIVE) error = %v, want invalid argument", err)
		}
		assertWriterActiveRouteRejected(t, writerActiveRouteHarness{
			database: database, writer: writer, actorContext: actorContext, scope: scope,
		}, before, capacityHooks)
	})

	t.Run("enable DRAFT", func(t *testing.T) {
		database, _ := newCatalogTestStore(t)
		createPublicationTransitionTestIndex(t, database, "main")
		fixture := newWriterActiveOpaqueDefinition(t, "writer-opaque-draft-enable")
		const objectID = "ko-writer-opaque-draft-enable"
		insertIntegrationFutureObject(t, database, objectID, StateDraft, fixture, 20)
		writer, actorContext, scope := newWriterOpaqueEmergencyHarness(t, database)
		before, err := readCatalogState(database.GORMDB(), testTenant)
		if err != nil {
			t.Fatalf("read catalog before opaque enable: %v", err)
		}
		var capacityHooks int
		writer.hook = func(_ context.Context, event writerHookEvent) error {
			if writerActiveRouteLateHook(event.Boundary) {
				capacityHooks++
			}
			return nil
		}
		_, err = writer.SetState(actorContext, scope, &opensplunkv1.SetKnowledgeObjectStateRequest{
			KnowledgeObjectId: objectID,
			ExpectedVersion:   1,
			State:             opensplunkv1.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_ACTIVE,
			ClientRequestId:   "writer-opaque-enable-0001",
		})
		if !errors.Is(err, control.ErrInvalidArgument) {
			t.Fatalf("SetState(opaque DRAFT->ACTIVE) error = %v, want invalid argument", err)
		}
		assertWriterActiveRouteRejected(t, writerActiveRouteHarness{
			database: database, writer: writer, actorContext: actorContext, scope: scope,
		}, before, capacityHooks)
	})
}

func TestMintRecognizedPublicationTransitionRejectsMalformedEndpointsBeforeInventory(t *testing.T) {
	harness := newWriterActiveRouteEmptyHarness(t)
	normalized, err := normalizeMutationDefinition(writerActiveRouteDefinition(
		dependencyExtractionDefinition(
			testApp, "writer-transition-input", SharingScopePrivate, nil,
			"writer-transition-host", "writer_transition_output",
		),
		"main",
	))
	if err != nil {
		t.Fatalf("normalize transition input fixture: %v", err)
	}
	authority, err := authorityFromNormalized(normalized)
	if err != nil {
		t.Fatalf("transition input authority: %v", err)
	}
	createSuccessor, err := recognizedPublicationObjectFromAuthority(
		testTenant,
		"ko-writer-transition-input",
		1,
		testOwner,
		StateActive,
		authority,
	)
	if err != nil {
		t.Fatalf("create transition successor: %v", err)
	}
	activePredecessor := createSuccessor
	activeSuccessor, err := recognizedPublicationObjectFromAuthority(
		testTenant,
		activePredecessor.KnowledgeObjectID,
		2,
		testOwner,
		StateActive,
		authority,
	)
	if err != nil {
		t.Fatalf("update transition successor: %v", err)
	}
	type transitionInput struct {
		before             *Object
		beforeState        State
		beforeDependencies []publicationDependency
		after              Object
		afterState         State
		afterDependencies  []publicationDependency
	}
	validCreate := func() transitionInput {
		return transitionInput{after: createSuccessor, afterState: StateActive}
	}
	validUpdate := func() transitionInput {
		before := activePredecessor
		return transitionInput{
			before: &before, beforeState: StateActive,
			after: activeSuccessor, afterState: StateActive,
		}
	}
	tests := []struct {
		name        string
		wantMessage string
		mutate      func(transitionInput) transitionInput
		base        func() transitionInput
	}{
		{
			name:        "absent predecessor cannot submit dependency rows",
			wantMessage: "recognized publication transition input is invalid",
			base:        validCreate,
			mutate: func(input transitionInput) transitionInput {
				input.beforeDependencies = []publicationDependency{}
				return input
			},
		},
		{
			name:        "ACTIVE successor cannot submit dependency rows",
			wantMessage: "recognized publication transition input is invalid",
			base:        validUpdate,
			mutate: func(input transitionInput) transitionInput {
				input.afterDependencies = []publicationDependency{}
				return input
			},
		},
		{
			name:        "successor state must match endpoint state",
			wantMessage: "recognized publication successor authority is invalid",
			base:        validCreate,
			mutate: func(input transitionInput) transitionInput {
				input.after.State = StateDraft
				return input
			},
		},
		{
			name:        "successor tenant must match transition tenant",
			wantMessage: "recognized publication successor authority is invalid",
			base:        validCreate,
			mutate: func(input transitionInput) transitionInput {
				input.after.TenantID = "writer-transition-other-tenant"
				return input
			},
		},
		{
			name:        "create successor must start at version one",
			wantMessage: "recognized publication create chronology is invalid",
			base:        validCreate,
			mutate: func(input transitionInput) transitionInput {
				input.after.Version = 2
				return input
			},
		},
		{
			name:        "predecessor state must match endpoint state",
			wantMessage: "recognized publication successor chronology is invalid",
			base:        validUpdate,
			mutate: func(input transitionInput) transitionInput {
				input.before.State = StateDraft
				return input
			},
		},
		{
			name:        "successor identity must match predecessor",
			wantMessage: "recognized publication successor chronology is invalid",
			base:        validUpdate,
			mutate: func(input transitionInput) transitionInput {
				input.after.KnowledgeObjectID = "ko-writer-transition-other"
				return input
			},
		},
		{
			name:        "successor version must immediately follow predecessor",
			wantMessage: "recognized publication successor chronology is invalid",
			base:        validUpdate,
			mutate: func(input transitionInput) transitionInput {
				input.after.Version = 3
				return input
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			tx := harness.database.GORMDB().Begin()
			if tx.Error != nil {
				t.Fatalf("begin transition validation transaction: %v", tx.Error)
			}
			t.Cleanup(func() { _ = tx.Rollback().Error })
			input := test.mutate(test.base())
			_, err := harness.writer.mintRecognizedPublicationTransition(
				t.Context(),
				tx,
				testTenant,
				input.before,
				input.beforeState,
				input.beforeDependencies,
				input.after,
				input.afterState,
				input.afterDependencies,
			)
			if !errors.Is(err, control.ErrInvalidArgument) ||
				!strings.Contains(err.Error(), test.wantMessage) {
				t.Fatalf(
					"malformed transition error = %v, want invalid argument containing %q",
					err,
					test.wantMessage,
				)
			}
		})
	}
}

func TestPreflightActivePublicationCapacityBoundariesAndNetZero(t *testing.T) {
	definition := writerActiveRouteDefinition(dependencyAliasDefinition(
		testApp, "writer-capacity-candidate", SharingScopePrivate, nil,
		"writer-capacity-host", "source", "capacity_alias",
	), "main")
	normalized, err := normalizeMutationDefinition(definition)
	if err != nil {
		t.Fatalf("normalize capacity definition: %v", err)
	}
	authority, err := authorityFromNormalized(normalized)
	if err != nil {
		t.Fatalf("capacity definition authority: %v", err)
	}
	tests := []struct {
		name      string
		active    int64
		app       int64
		typeCount int64
		appType   int64
		owner     int64
		want      error
	}{
		{name: "all boundaries minus one", active: 4095, app: 1023, typeCount: 2047, appType: 511, owner: 511},
		{name: "tenant exact", active: 4096, want: control.ErrCapacityExceeded},
		{name: "app exact", app: 1024, want: control.ErrCapacityExceeded},
		{name: "type exact", typeCount: 2048, want: control.ErrCapacityExceeded},
		{name: "app type exact", appType: 512, want: control.ErrCapacityExceeded},
		{name: "private owner exact", owner: 512, want: control.ErrCapacityExceeded},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			database, _ := newCatalogTestStore(t)
			if err := ensureMutationLedgers(database.GORMDB(), testTenant); err != nil {
				t.Fatalf("ensure capacity ledgers: %v", err)
			}
			insertWriterActiveRouteCounters(t, database, authority, test.app, test.typeCount, test.appType, test.owner)
			err := preflightActivePublicationCapacity(
				database.GORMDB(),
				mutationTenantHealth{ActiveObjectCount: test.active},
				testTenant,
				nil,
				authority,
				testOwner,
			)
			if !errors.Is(err, test.want) || test.want == nil && err != nil {
				t.Fatalf("capacity preflight error = %v, want %v", err, test.want)
			}
		})
	}

	t.Run("ACTIVE unchanged cohorts are net zero at every ceiling", func(t *testing.T) {
		database, _ := newCatalogTestStore(t)
		if err := ensureMutationLedgers(database.GORMDB(), testTenant); err != nil {
			t.Fatalf("ensure net-zero capacity ledgers: %v", err)
		}
		insertWriterActiveRouteCounters(t, database, authority, 1024, 2048, 512, 512)
		current := registryRecord{
			TenantID: testTenant, KnowledgeObjectID: "ko-capacity-net-zero",
			AppID: testApp, OwnerID: testOwner, ObjectType: authority.objectType,
			Name: authority.name, SharingScope: SharingScopePrivate, State: StateActive,
		}
		if err := preflightActivePublicationCapacity(
			database.GORMDB(),
			mutationTenantHealth{ActiveObjectCount: 4096},
			testTenant,
			&current,
			authority,
			testOwner,
		); err != nil {
			t.Fatalf("net-zero ACTIVE capacity preflight: %v", err)
		}
	})

	t.Run("ACTIVE app move checks destination app and app-type only", func(t *testing.T) {
		database, _ := newCatalogTestStore(t)
		if err := ensureMutationLedgers(database.GORMDB(), testTenant); err != nil {
			t.Fatalf("ensure app-move capacity ledgers: %v", err)
		}
		insertWriterActiveRouteCounters(t, database, authority, 1024, 2048, 512, 512)
		current := registryRecord{
			TenantID: testTenant, KnowledgeObjectID: "ko-capacity-app-move",
			AppID: testAppTwo, OwnerID: testOwner, ObjectType: authority.objectType,
			Name: authority.name, SharingScope: SharingScopePrivate, State: StateActive,
		}
		err := preflightActivePublicationCapacity(
			database.GORMDB(),
			mutationTenantHealth{ActiveObjectCount: 4096},
			testTenant,
			&current,
			authority,
			testOwner,
		)
		if !errors.Is(err, control.ErrCapacityExceeded) {
			t.Fatalf("destination app capacity error = %v, want capacity exceeded", err)
		}
	})

	t.Run("ACTIVE app scope to private checks destination owner", func(t *testing.T) {
		database, _ := newCatalogTestStore(t)
		if err := ensureMutationLedgers(database.GORMDB(), testTenant); err != nil {
			t.Fatalf("ensure private-move capacity ledgers: %v", err)
		}
		insertWriterActiveRouteCounters(t, database, authority, 1024, 2048, 512, 512)
		current := registryRecord{
			TenantID: testTenant, KnowledgeObjectID: "ko-capacity-private-move",
			AppID: testApp, OwnerID: testOwner, ObjectType: authority.objectType,
			Name: authority.name, SharingScope: SharingScopeApp, State: StateActive,
		}
		err := preflightActivePublicationCapacity(
			database.GORMDB(),
			mutationTenantHealth{ActiveObjectCount: 4096},
			testTenant,
			&current,
			authority,
			testOwner,
		)
		if !errors.Is(err, control.ErrCapacityExceeded) {
			t.Fatalf("destination owner capacity error = %v, want capacity exceeded", err)
		}
	})

	appDefinition := writerActiveRouteDefinition(dependencyAliasDefinition(
		testApp, "writer-capacity-candidate", SharingScopeApp, nil,
		"writer-capacity-host", "source", "capacity_alias",
	), "main")
	appNormalized, err := normalizeMutationDefinition(appDefinition)
	if err != nil {
		t.Fatalf("normalize app-scope capacity definition: %v", err)
	}
	appAuthority, err := authorityFromNormalized(appNormalized)
	if err != nil {
		t.Fatalf("app-scope capacity definition authority: %v", err)
	}
	t.Run("ACTIVE private to app releases owner cohort", func(t *testing.T) {
		database, _ := newCatalogTestStore(t)
		if err := ensureMutationLedgers(database.GORMDB(), testTenant); err != nil {
			t.Fatalf("ensure private-release capacity ledgers: %v", err)
		}
		insertWriterActiveRouteCounters(t, database, appAuthority, 1024, 2048, 512, 512)
		current := registryRecord{
			TenantID: testTenant, KnowledgeObjectID: "ko-capacity-private-release",
			AppID: testApp, OwnerID: testOwner, ObjectType: appAuthority.objectType,
			Name: appAuthority.name, SharingScope: SharingScopePrivate, State: StateActive,
		}
		if err := preflightActivePublicationCapacity(
			database.GORMDB(),
			mutationTenantHealth{ActiveObjectCount: 4096},
			testTenant,
			&current,
			appAuthority,
			testOwner,
		); err != nil {
			t.Fatalf("private-to-app net capacity: %v", err)
		}
	})

	t.Run("ACTIVE private app move is owner net zero", func(t *testing.T) {
		database, _ := newCatalogTestStore(t)
		if err := ensureMutationLedgers(database.GORMDB(), testTenant); err != nil {
			t.Fatalf("ensure private app-move capacity ledgers: %v", err)
		}
		insertWriterActiveRouteCounters(t, database, authority, 1023, 2048, 511, 512)
		current := registryRecord{
			TenantID: testTenant, KnowledgeObjectID: "ko-capacity-private-app-move",
			AppID: testAppTwo, OwnerID: testOwner, ObjectType: authority.objectType,
			Name: authority.name, SharingScope: SharingScopePrivate, State: StateActive,
		}
		if err := preflightActivePublicationCapacity(
			database.GORMDB(),
			mutationTenantHealth{ActiveObjectCount: 4096},
			testTenant,
			&current,
			authority,
			testOwner,
		); err != nil {
			t.Fatalf("private app-move net owner capacity: %v", err)
		}
	})
}

func TestWriterActiveCreateRejectsAppTypeCapacityBeforeInventoryHook(t *testing.T) {
	if testing.Short() {
		t.Skip("seeds the exact 512-row ACTIVE app/type boundary")
	}
	harness := newWriterActiveRouteEmptyHarness(t)
	for index := 0; index < 512; index++ {
		name := fmt.Sprintf("writer-route-capacity-%03d", index)
		insertFixtureObject(t, harness.database, fixtureObject{
			id: "ko-" + name, owner: testOwner,
			versions: []fixtureVersion{{
				definition: writerActiveRouteDefinition(dependencyAliasDefinition(
					testApp, name, SharingScopeGlobal, nil,
					"writer-route-capacity-host", "source", "capacity_"+name,
				), "index-that-does-not-exist"),
				state: StateActive, mutation: "create", timestamp: int64(100 + index),
			}},
		})
	}
	before, err := readCatalogState(harness.database.GORMDB(), testTenant)
	if err != nil {
		t.Fatalf("read catalog before route capacity rejection: %v", err)
	}
	var capacityHooks int
	harness.writer.hook = func(_ context.Context, event writerHookEvent) error {
		if writerActiveRouteLateHook(event.Boundary) {
			capacityHooks++
		}
		return nil
	}
	_, err = harness.writer.Create(
		harness.actorContext,
		harness.scope,
		&opensplunkv1.CreateKnowledgeObjectRequest{
			Definition: writerActiveRouteDefinition(dependencyAliasDefinition(
				testApp, "writer-route-capacity-candidate", SharingScopePrivate, nil,
				"writer-route-capacity-host", "source", "capacity_candidate",
			), "main"),
			InitialState:    opensplunkv1.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_ACTIVE,
			ClientRequestId: "writer-route-capacity-0001",
		},
	)
	if !errors.Is(err, control.ErrCapacityExceeded) {
		t.Fatalf("Create(ACTIVE at app/type capacity) error = %v, want capacity exceeded", err)
	}
	assertWriterActiveRouteRejected(t, harness, before, capacityHooks)
}

func writerActiveRouteDefinition(
	definition *opensplunkv1.KnowledgeObjectDefinition,
	indexName string,
) *opensplunkv1.KnowledgeObjectDefinition {
	if definition.Selector == nil {
		definition.Selector = &opensplunkv1.KnowledgeSelector{}
	}
	definition.Selector.IndexPatterns = []*opensplunkv1.KnowledgeSelectorPattern{{Value: indexName}}
	return definition
}

func mustNormalizeWriterActiveRouteDefinition(
	t *testing.T,
	definition *opensplunkv1.KnowledgeObjectDefinition,
) *opensplunkv1.KnowledgeObjectDefinition {
	t.Helper()
	normalized, err := normalizeMutationDefinition(definition)
	if err != nil {
		t.Fatalf("normalize Writer ACTIVE route definition: %v", err)
	}
	return normalized.Definition
}

func writerActiveRouteSelector(indexName, host string) *opensplunkv1.KnowledgeSelector {
	return &opensplunkv1.KnowledgeSelector{
		IndexPatterns: []*opensplunkv1.KnowledgeSelectorPattern{{Value: indexName}},
		HostPatterns:  []*opensplunkv1.KnowledgeSelectorPattern{{Value: host}},
	}
}

func assertWriterActiveRouteVersion(
	t *testing.T,
	database *control.DB,
	objectID string,
	versionNumber int64,
	state State,
	mutation string,
	targetID string,
) {
	t.Helper()
	version, found, err := readVersionRecord(database.GORMDB(), testTenant, objectID, versionNumber)
	if err != nil || !found || version.State != state || version.MutationKind != mutation {
		t.Fatalf("read %s v%d = (%#v, %t, %v)", objectID, versionNumber, version, found, err)
	}
	dependencies, err := readValidatedVersionDependencies(database.GORMDB(), version)
	if err != nil {
		t.Fatalf("read %s v%d dependencies: %v", objectID, versionNumber, err)
	}
	if targetID == "" {
		if len(dependencies) != 0 {
			t.Fatalf("%s v%d dependencies = %#v, want empty", objectID, versionNumber, dependencies)
		}
	} else if len(dependencies) != 1 || dependencies[0].TargetObjectID != targetID ||
		dependencies[0].TargetObjectVersion != 1 {
		t.Fatalf("%s v%d dependencies = %#v, want %s v1", objectID, versionNumber, dependencies, targetID)
	}
	registry, found, err := readRegistryRecord(database.GORMDB().Model(&registryRecord{}).Where(
		"tenant_id = ? AND knowledge_object_id = ?", testTenant, objectID,
	))
	if err != nil || !found || registry.CurrentVersion != versionNumber ||
		registry.State != state || registry.AppID != version.AppID ||
		registry.OwnerID != version.OwnerID || registry.ObjectType != version.ObjectType ||
		registry.Name != version.Name || registry.SharingScope != version.SharingScope ||
		!bytes.Equal(registry.DefinitionDigest, version.DefinitionDigest) ||
		(state == StateDisabled) != (registry.DisabledAtUnixMicro != nil) {
		t.Fatalf("current registry for %s v%d = (%#v, %t, %v)", objectID, versionNumber, registry, found, err)
	}
	action := audit.ActionKnowledgeObjectCreate
	switch mutation {
	case "update":
		action = audit.ActionKnowledgeObjectUpdate
	case "enable":
		action = audit.ActionKnowledgeObjectEnable
	case "disable":
		action = audit.ActionKnowledgeObjectDisable
	}
	var auditCount int64
	if err := database.GORMDB().Table("audit_events").Where(
		"tenant_id = ? AND action = ? AND target_kind = ? AND target_id = ? AND target_version = ?",
		testTenant, action, audit.TargetKindKnowledgeObject, objectID, versionNumber,
	).Count(&auditCount).Error; err != nil || auditCount != 1 {
		t.Fatalf("audit event for %s v%d/%s count = %d, %v", objectID, versionNumber, action, auditCount, err)
	}
}

func assertWriterActiveRouteReplay(
	t *testing.T,
	harness writerActiveRouteHarness,
	route string,
	request proto.Message,
	want proto.Message,
) {
	t.Helper()
	before, err := readCatalogState(harness.database.GORMDB(), testTenant)
	if err != nil {
		t.Fatalf("read catalog before %s replay: %v", route, err)
	}
	var capacityHooks int
	harness.writer.hook = func(_ context.Context, event writerHookEvent) error {
		if writerActiveRouteLateHook(event.Boundary) {
			capacityHooks++
			return errors.New("replay reached an ACTIVE publication hook")
		}
		return nil
	}
	var got proto.Message
	switch route {
	case mutationRouteCreate:
		got, err = harness.writer.Create(
			harness.actorContext,
			harness.scope,
			proto.Clone(request).(*opensplunkv1.CreateKnowledgeObjectRequest),
		)
	case mutationRouteUpdate:
		got, err = harness.writer.Update(
			harness.actorContext,
			harness.scope,
			proto.Clone(request).(*opensplunkv1.UpdateKnowledgeObjectRequest),
		)
	case mutationRouteSetState:
		got, err = harness.writer.SetState(
			harness.actorContext,
			harness.scope,
			proto.Clone(request).(*opensplunkv1.SetKnowledgeObjectStateRequest),
		)
	default:
		t.Fatalf("unsupported replay route %q", route)
	}
	harness.writer.hook = nil
	if err != nil || !proto.Equal(got, want) || capacityHooks != 0 {
		t.Fatalf("%s exact replay = (%v, %v), hooks=%d, want %v", route, got, err, capacityHooks, want)
	}
	after, err := readCatalogState(harness.database.GORMDB(), testTenant)
	if err != nil {
		t.Fatalf("read catalog after %s replay: %v", route, err)
	}
	if before.revision != after.revision || before.token != after.token {
		t.Fatalf("%s replay advanced catalog: before=%#v after=%#v", route, before, after)
	}
}

func assertWriterActiveRouteRejected(
	t *testing.T,
	harness writerActiveRouteHarness,
	before catalogState,
	lateHooks int,
) {
	t.Helper()
	if lateHooks != 0 {
		t.Fatalf("rejected ACTIVE mutation reached %d ACTIVE capacity/persistence hooks", lateHooks)
	}
	after, err := readCatalogState(harness.database.GORMDB(), testTenant)
	if err != nil {
		t.Fatalf("read catalog after rejection: %v", err)
	}
	if before.revision != after.revision || before.token != after.token {
		t.Fatalf("rejected ACTIVE mutation changed catalog: before=%#v after=%#v", before, after)
	}
}

func writerActiveRouteLateHook(boundary writerHookBoundary) bool {
	switch boundary {
	case writerHookPrepared, writerHookIdempotencyChecked, writerHookCatalogLedgersReady:
		return false
	default:
		return true
	}
}

func insertWriterActiveRouteCounters(
	t *testing.T,
	database *control.DB,
	authority definitionAuthority,
	appCount, typeCount, appTypeCount, ownerCount int64,
) {
	t.Helper()
	if appCount > 0 {
		mustExec(t, database, `INSERT INTO knowledge_app_active_counters (
			tenant_id, app_id, active_object_count
		) VALUES (?, ?, ?)`, testTenant, authority.appID, appCount)
	}
	if typeCount > 0 {
		mustExec(t, database, `INSERT INTO knowledge_type_active_counters (
			tenant_id, object_type, active_object_count
		) VALUES (?, ?, ?)`, testTenant, authority.objectType, typeCount)
	}
	if appTypeCount > 0 {
		mustExec(t, database, `INSERT INTO knowledge_app_type_active_counters (
			tenant_id, app_id, object_type, active_object_count
		) VALUES (?, ?, ?, ?)`, testTenant, authority.appID, authority.objectType, appTypeCount)
	}
	if ownerCount > 0 {
		mustExec(t, database, `INSERT INTO knowledge_owner_active_counters (
			tenant_id, owner_id, active_private_object_count
		) VALUES (?, ?, ?)`, testTenant, testOwner, ownerCount)
	}
}

func archiveWriterActiveRouteApp(
	t *testing.T,
	database *control.DB,
	dropActiveReferenceGuard bool,
) {
	t.Helper()
	if dropActiveReferenceGuard {
		dropTrigger(t, database, "knowledge_active_app_workspace_cannot_be_archived")
	}
	catalog, err := control.NewAppCatalog(database, control.AppCatalogOptions{
		CursorKey: testCursorKey,
		Clock:     func() time.Time { return time.UnixMicro(1_000).UTC() },
	})
	if err != nil {
		t.Fatalf("control.NewAppCatalog(): %v", err)
	}
	workspace, err := catalog.GetApp(
		t.Context(),
		control.AppAccessScope{TenantID: testTenant},
		control.AppSelector{AppID: testApp},
	)
	if err != nil {
		t.Fatalf("GetApp(before archive): %v", err)
	}
	if _, err := catalog.SetAppState(
		t.Context(),
		control.AppAccessScope{TenantID: testTenant},
		control.AppSelector{AppID: testApp},
		workspace.Version,
		control.AppStateArchived,
	); err != nil {
		t.Fatalf("SetAppState(archived): %v", err)
	}
}
