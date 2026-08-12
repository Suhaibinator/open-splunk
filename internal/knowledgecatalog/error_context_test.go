package knowledgecatalog

import (
	"context"
	"errors"
	"reflect"
	"slices"
	"testing"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

func TestCatalogErrorMetadataIsDetachedAndPreservesCause(t *testing.T) {
	cause := errors.New("catalog failure")
	authorized := AuthorizedContext{
		AppID: "app_000000000100000000001A",
		Object: &AuthorizedObject{
			KnowledgeObjectID: "ko-authorized",
			ObjectType:        ObjectTypeFieldAlias,
			Version:           7,
			SharingScope:      SharingScopeApp,
		},
	}
	wrapped := withAuthorizedContext(
		withErrorDisposition(cause, ErrorDispositionKnownCommitted),
		authorized,
	)
	if !errors.Is(wrapped, cause) {
		t.Fatalf("wrapped error %v does not preserve errors.Is", wrapped)
	}
	authorized.AppID = "mutated-app"
	authorized.Object.KnowledgeObjectID = "mutated-object"
	authorized.Object.Version = 99

	first, found := AuthorizedContextFromError(wrapped)
	if !found || first.AppID != "app_000000000100000000001A" || first.Object == nil ||
		first.Object.KnowledgeObjectID != "ko-authorized" || first.Object.Version != 7 {
		t.Fatalf("first detached authorization = %#v, found %v", first, found)
	}
	first.AppID = "caller-mutated-app"
	first.Object.KnowledgeObjectID = "caller-mutated-object"
	first.Object.Version = 101
	second, found := AuthorizedContextFromError(wrapped)
	if !found || second.AppID != "app_000000000100000000001A" || second.Object == nil ||
		second.Object.KnowledgeObjectID != "ko-authorized" || second.Object.Version != 7 {
		t.Fatalf("second detached authorization = %#v, found %v", second, found)
	}
	requireCatalogDisposition(t, wrapped, ErrorDispositionKnownCommitted)

	replaced := copyCatalogErrorMetadata(context.Canceled, wrapped)
	if !errors.Is(replaced, context.Canceled) || errors.Is(replaced, cause) {
		t.Fatalf("replacement cause = %v", replaced)
	}
	requireCatalogDisposition(t, replaced, ErrorDispositionKnownCommitted)
	if got, ok := AuthorizedContextFromError(replaced); !ok || !reflect.DeepEqual(got, second) {
		t.Fatalf("replacement authorization = %#v, %v; want %#v", got, ok, second)
	}

	if _, found := AuthorizedContextFromError(nil); found {
		t.Fatal("nil error unexpectedly carried authorization")
	}
	if _, found := ErrorDispositionFromError(cause); found {
		t.Fatal("ordinary error unexpectedly carried a disposition")
	}
}

func TestPublicCatalogErrorDecoratorsValidateInputs(t *testing.T) {
	cause := errors.New("fake service failure")
	authorized := AuthorizedContext{
		AppID: testApp,
		Object: &AuthorizedObject{
			KnowledgeObjectID: "ko-public-decorator",
			ObjectType:        ObjectTypeFieldAlias,
			Version:           1,
			SharingScope:      SharingScopePrivate,
		},
	}
	decorated := WithAuthorizedContext(
		WithErrorDisposition(cause, ErrorDispositionDefinitiveRejection),
		authorized,
	)
	if !errors.Is(decorated, cause) {
		t.Fatalf("decorated error = %v, want original cause", decorated)
	}
	requireCatalogDisposition(t, decorated, ErrorDispositionDefinitiveRejection)
	if got, found := AuthorizedContextFromError(decorated); !found || !reflect.DeepEqual(got, authorized) {
		t.Fatalf("decorated authorization = %#v, found %v; want %#v", got, found, authorized)
	}
	authorized.Object.Version = 2
	if got, found := AuthorizedContextFromError(decorated); !found || got.Object == nil || got.Object.Version != 1 {
		t.Fatalf("decorator retained caller-owned context: %#v, found %v", got, found)
	}

	if got := WithErrorDisposition(nil, ErrorDisposition(255)); got != nil {
		t.Fatalf("WithErrorDisposition(nil) = %v, want nil", got)
	}
	if got := WithAuthorizedContext(nil, AuthorizedContext{}); got != nil {
		t.Fatalf("WithAuthorizedContext(nil) = %v, want nil", got)
	}
	if err := WithErrorDisposition(cause, ErrorDisposition(255)); !errors.Is(err, control.ErrInvalidArgument) || errors.Is(err, cause) {
		t.Fatalf("invalid disposition decorator = %v", err)
	}

	invalidContexts := []AuthorizedContext{
		{},
		{AppID: testApp, Object: &AuthorizedObject{}},
		{
			AppID: testApp,
			Object: &AuthorizedObject{
				KnowledgeObjectID: "ko-invalid-type",
				ObjectType:        ObjectType("future"),
				Version:           1,
				SharingScope:      SharingScopePrivate,
			},
		},
		{
			AppID: testApp,
			Object: &AuthorizedObject{
				KnowledgeObjectID: "ko-invalid-version",
				ObjectType:        ObjectTypeFieldAlias,
				Version:           uint64(maximumVersionsPerTenant + 1),
				SharingScope:      SharingScopePrivate,
			},
		},
	}
	for _, invalid := range invalidContexts {
		if err := WithAuthorizedContext(cause, invalid); !errors.Is(err, control.ErrInvalidArgument) || errors.Is(err, cause) {
			t.Errorf("invalid context decorator %#v = %v", invalid, err)
		}
	}
	if err := WithAuthorizedContext(cause, AuthorizedContext{AppID: testApp}); !errors.Is(err, cause) {
		t.Fatalf("app-only context decorator = %v, want original cause", err)
	}
}

func TestPublicScopeValidatorsReuseNormalizationWithoutMutation(t *testing.T) {
	readApps := []string{testAppTwo, testApp, testAppTwo}
	readBefore := slices.Clone(readApps)
	if err := ValidateReadScope(ReadScope{
		TenantID: testTenant, OwnerID: testOwner, ReadableAppIDs: readApps,
	}); err != nil {
		t.Fatalf("ValidateReadScope(valid): %v", err)
	}
	if !slices.Equal(readApps, readBefore) {
		t.Fatalf("ValidateReadScope mutated apps: got %q want %q", readApps, readBefore)
	}

	writeApps := []string{testAppTwo, testApp, testAppTwo}
	writeBefore := slices.Clone(writeApps)
	if err := ValidateWriteScope(WriteScope{
		TenantID: testTenant, OwnerID: testOwner, WritableAppIDs: writeApps,
	}); err != nil {
		t.Fatalf("ValidateWriteScope(valid): %v", err)
	}
	if !slices.Equal(writeApps, writeBefore) {
		t.Fatalf("ValidateWriteScope mutated apps: got %q want %q", writeApps, writeBefore)
	}

	// Preserve the existing intentional semantic difference: reads accept a
	// bounded opaque app identity, while writes require the canonical app ID.
	legacyRead := ReadScope{
		TenantID: testTenant, OwnerID: testOwner, ReadableAppIDs: []string{"legacy-app"},
	}
	if err := ValidateReadScope(legacyRead); err != nil {
		t.Fatalf("ValidateReadScope(opaque app): %v", err)
	}
	if err := ValidateWriteScope(WriteScope{
		TenantID: testTenant, OwnerID: testOwner, WritableAppIDs: []string{"legacy-app"},
	}); !errors.Is(err, control.ErrInvalidArgument) {
		t.Fatalf("ValidateWriteScope(opaque app) = %v, want ErrInvalidArgument", err)
	}

	for name, validate := range map[string]func() error{
		"read empty tenant": func() error {
			return ValidateReadScope(ReadScope{
				OwnerID: testOwner, ReadableAppIDs: []string{testApp},
			})
		},
		"read empty apps": func() error {
			return ValidateReadScope(ReadScope{TenantID: testTenant, OwnerID: testOwner})
		},
		"write empty owner": func() error {
			return ValidateWriteScope(WriteScope{
				TenantID: testTenant, WritableAppIDs: []string{testApp},
			})
		},
		"write empty apps": func() error {
			return ValidateWriteScope(WriteScope{TenantID: testTenant, OwnerID: testOwner})
		},
	} {
		t.Run(name, func(t *testing.T) {
			if err := validate(); !errors.Is(err, control.ErrInvalidArgument) {
				t.Fatalf("validator error = %v, want ErrInvalidArgument", err)
			}
		})
	}
}

func TestUpdateChangesSharingScopeUsesCanonicalMaskAuthority(t *testing.T) {
	tests := []struct {
		name string
		mask *fieldmaskpb.FieldMask
		want bool
		bad  bool
	}{
		{name: "ordinary update", mask: &fieldmaskpb.FieldMask{Paths: []string{"description", "name"}}},
		{name: "app move", mask: &fieldmaskpb.FieldMask{Paths: []string{"app_id", "description"}}, want: true},
		{name: "sharing change", mask: &fieldmaskpb.FieldMask{Paths: []string{"description", "sharing_scope"}}, want: true},
		{name: "nil", bad: true},
		{name: "unsorted", mask: &fieldmaskpb.FieldMask{Paths: []string{"sharing_scope", "app_id"}}, bad: true},
		{name: "duplicate", mask: &fieldmaskpb.FieldMask{Paths: []string{"app_id", "app_id"}}, bad: true},
		{name: "nested", mask: &fieldmaskpb.FieldMask{Paths: []string{"definition.app_id"}}, bad: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := UpdateChangesSharingScope(test.mask)
			if test.bad {
				if !errors.Is(err, control.ErrInvalidArgument) || got {
					t.Fatalf("UpdateChangesSharingScope = (%v, %v), want false/ErrInvalidArgument", got, err)
				}
				return
			}
			if err != nil || got != test.want {
				t.Fatalf("UpdateChangesSharingScope = (%v, %v), want %v/nil", got, err, test.want)
			}
		})
	}
}

func TestStoreGetErrorContextUsesAuthorizedCurrentRegistry(t *testing.T) {
	database, store := newCatalogTestStore(t)
	oldDescription := "historical body"
	newDescription := "current body"
	insertFixtureObject(t, database, fixtureObject{
		id:    "ko-error-context-history",
		owner: testOwner,
		versions: []fixtureVersion{
			{
				definition: aliasDefinition(
					testApp, "error-context-history", SharingScopeGlobal,
					&oldDescription, "old-*",
				),
				state: StateActive, mutation: "create", timestamp: 10,
			},
			{
				definition: aliasDefinition(
					testApp, "error-context-history", SharingScopePrivate,
					&newDescription, "new-*",
				),
				state: StateActive, mutation: "scope_change", timestamp: 20,
			},
		},
	})

	current, err := store.Get(context.Background(), testReadScope(), "ko-error-context-history", nil)
	if err != nil {
		t.Fatalf("Get(current): %v", err)
	}
	projection, err := ObjectToProto(current)
	if err != nil {
		t.Fatalf("ObjectToProto(current): %v", err)
	}
	projection.Definition.Name = "mutated projection"
	projection.DefinitionSha256[0] ^= 0xff
	if current.Definition.GetName() != "error-context-history" ||
		current.DefinitionSHA256[0] == projection.DefinitionSha256[0] {
		t.Fatal("ObjectToProto result aliases its Object input")
	}

	if _, err := store.Get(context.Background(), testReadScope(), "ko-missing-context", nil); !errors.Is(err, control.ErrNotFound) {
		t.Fatalf("Get(missing) = %v, want ErrNotFound", err)
	} else {
		requireCatalogDisposition(t, err, ErrorDispositionDefinitiveRejection)
		if context, found := AuthorizedContextFromError(err); found {
			t.Fatalf("preauthorization missing error carried context %#v", context)
		}
	}

	unauthorized := testReadScope()
	unauthorized.OwnerID = "different-owner"
	if _, err := store.Get(context.Background(), unauthorized, "ko-error-context-history", nil); !errors.Is(err, control.ErrNotFound) {
		t.Fatalf("Get(unauthorized) = %v, want ErrNotFound", err)
	} else if context, found := AuthorizedContextFromError(err); found {
		t.Fatalf("preauthorization forbidden error carried context %#v", context)
	}

	dropTrigger(t, database, "knowledge_object_version_update_is_forbidden")
	mustExec(t, database, `UPDATE knowledge_object_versions
		SET sharing_scope = 'app'
		WHERE tenant_id = ? AND knowledge_object_id = ? AND object_version = 1`,
		testTenant, "ko-error-context-history")
	versionOne := uint64(1)
	_, err = store.Get(
		context.Background(),
		testReadScope(),
		"ko-error-context-history",
		&versionOne,
	)
	if !errors.Is(err, ErrCorrupt) {
		t.Fatalf("Get(corrupt historical) = %v, want ErrCorrupt", err)
	}
	requireCatalogDisposition(t, err, ErrorDispositionDefinitiveRejection)
	authorized, found := AuthorizedContextFromError(err)
	if !found || authorized.AppID != testApp || authorized.Object == nil ||
		authorized.Object.KnowledgeObjectID != "ko-error-context-history" ||
		authorized.Object.ObjectType != ObjectTypeFieldAlias ||
		authorized.Object.Version != 2 ||
		authorized.Object.SharingScope != SharingScopePrivate {
		t.Fatalf("historical error authorization = %#v, found %v", authorized, found)
	}
}

func TestWriterErrorContextUsesCurrentAuthorityAndCancellationBoundary(t *testing.T) {
	t.Run("current version conflict", func(t *testing.T) {
		harness := newWriterFaultHarness(t)
		created, err := harness.writer.Create(
			harness.actorContext,
			harness.scope,
			writerFaultCreateRequest("context-current", "context-current-create-0001"),
		)
		if err != nil {
			t.Fatalf("Create baseline: %v", err)
		}
		if version, err := writerFaultRouteInvocation(
			t, harness, "update", created.GetKnowledgeObject(),
		)(); err != nil || version != 2 {
			t.Fatalf("Update baseline = (%d, %v), want 2/nil", version, err)
		}

		staleDefinition := proto.Clone(created.GetKnowledgeObject().GetDefinition()).(*opensplunkv1.KnowledgeObjectDefinition)
		staleDescription := "stale request body"
		staleDefinition.Description = &staleDescription
		staleDefinition.AppId = testAppTwo
		staleDefinition.SharingScope = opensplunkv1.SharingScope_SHARING_SCOPE_APP
		_, err = harness.writer.Update(
			harness.actorContext,
			harness.scope,
			&opensplunkv1.UpdateKnowledgeObjectRequest{
				KnowledgeObjectId: created.GetKnowledgeObject().GetKnowledgeObjectId(),
				ExpectedVersion:   1,
				Definition:        staleDefinition,
				UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{
					"app_id", "description", "sharing_scope",
				}},
				ClientRequestId: "context-current-stale-0001",
			},
		)
		if !errors.Is(err, control.ErrVersionConflict) {
			t.Fatalf("stale Update = %v, want ErrVersionConflict", err)
		}
		requireCatalogDisposition(t, err, ErrorDispositionDefinitiveRejection)
		authorized, found := AuthorizedContextFromError(err)
		if !found || authorized.AppID != writerFaultApp || authorized.Object == nil ||
			authorized.Object.KnowledgeObjectID != created.GetKnowledgeObject().GetKnowledgeObjectId() ||
			authorized.Object.Version != 2 ||
			authorized.Object.ObjectType != ObjectTypeFieldAlias ||
			authorized.Object.SharingScope != SharingScopePrivate {
			t.Fatalf("stale Update authorization = %#v, found %v", authorized, found)
		}

		unauthorizedScope := harness.scope
		unauthorizedScope.OwnerID = "different-owner"
		_, err = harness.writer.Update(
			harness.actorContext,
			unauthorizedScope,
			&opensplunkv1.UpdateKnowledgeObjectRequest{
				KnowledgeObjectId: created.GetKnowledgeObject().GetKnowledgeObjectId(),
				ExpectedVersion:   2,
				Definition:        staleDefinition,
				UpdateMask:        &fieldmaskpb.FieldMask{Paths: []string{"description"}},
				ClientRequestId:   "context-current-forbidden-0001",
			},
		)
		if !errors.Is(err, control.ErrNotFound) {
			t.Fatalf("unauthorized Update = %v, want ErrNotFound", err)
		}
		if context, found := AuthorizedContextFromError(err); found {
			t.Fatalf("preauthorization Update carried context %#v", context)
		}
	})

	t.Run("cancellation after authorization", func(t *testing.T) {
		harness := newWriterFaultHarness(t)
		created, err := harness.writer.Create(
			harness.actorContext,
			harness.scope,
			writerFaultCreateRequest("context-cancel", "context-cancel-create-0001"),
		)
		if err != nil {
			t.Fatalf("Create baseline: %v", err)
		}
		harness.writer.hook = func(_ context.Context, event writerHookEvent) error {
			if event.Boundary == writerHookCapacityChecked && event.Route == mutationRouteUpdate {
				return context.Canceled
			}
			return nil
		}
		_, err = writerFaultRouteInvocation(t, harness, "update", created.GetKnowledgeObject())()
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("canceled Update = %v, want context.Canceled", err)
		}
		requireCatalogDisposition(t, err, ErrorDispositionDefinitiveRejection)
		authorized, found := AuthorizedContextFromError(err)
		if !found || authorized.Object == nil || authorized.Object.Version != 1 ||
			authorized.Object.KnowledgeObjectID != created.GetKnowledgeObject().GetKnowledgeObjectId() {
			t.Fatalf("canceled Update authorization = %#v, found %v", authorized, found)
		}
	})
}

func TestWriterReplayErrorContextAndDisposition(t *testing.T) {
	harness := newWriterFaultHarness(t)
	createRequest := writerFaultCreateRequest(
		"context-replay",
		"context-replay-create-0001",
	)
	created, err := harness.writer.Create(harness.actorContext, harness.scope, createRequest)
	if err != nil {
		t.Fatalf("Create baseline: %v", err)
	}

	harness.writer.hook = func(_ context.Context, event writerHookEvent) error {
		if event.Boundary == writerHookPrepared && event.Route == mutationRouteCreate {
			return context.Canceled
		}
		return nil
	}
	_, err = harness.writer.Create(harness.actorContext, harness.scope, createRequest)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("pre-lookup exact Create cancellation = %v, want context.Canceled", err)
	}
	requireCatalogDisposition(t, err, ErrorDispositionIndeterminate)
	if _, found := AuthorizedContextFromError(err); found {
		t.Fatal("pre-lookup exact Create cancellation carried unproven authorization")
	}
	harness.writer.hook = nil

	alteredCreate := proto.Clone(createRequest).(*opensplunkv1.CreateKnowledgeObjectRequest)
	alteredCreate.Definition.Name = "context-replay-altered"
	_, err = harness.writer.Create(harness.actorContext, harness.scope, alteredCreate)
	if !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("altered Create replay = %v, want ErrIdempotencyConflict", err)
	}
	requireCatalogDisposition(t, err, ErrorDispositionDefinitiveRejection)
	if authorized, found := AuthorizedContextFromError(err); !found ||
		authorized.AppID != writerFaultApp || authorized.Object != nil {
		t.Fatalf("Create conflict authorization = %#v, found %v; want app-only", authorized, found)
	}

	harness.writer.hook = func(_ context.Context, event writerHookEvent) error {
		if event.Boundary == writerHookIdempotencyChecked && event.Route == mutationRouteCreate {
			return context.Canceled
		}
		return nil
	}
	_, err = harness.writer.Create(harness.actorContext, harness.scope, createRequest)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("exact Create replay cancellation = %v, want context.Canceled", err)
	}
	requireCatalogDisposition(t, err, ErrorDispositionKnownCommitted)
	if authorized, found := AuthorizedContextFromError(err); !found ||
		authorized.AppID != writerFaultApp || authorized.Object != nil {
		t.Fatalf("Create replay authorization = %#v, found %v; want app-only", authorized, found)
	}

	harness.writer.hook = nil
	updatedDefinition := proto.Clone(created.GetKnowledgeObject().GetDefinition()).(*opensplunkv1.KnowledgeObjectDefinition)
	updatedDescription := "committed replay update"
	updatedDefinition.Description = &updatedDescription
	updateRequest := &opensplunkv1.UpdateKnowledgeObjectRequest{
		KnowledgeObjectId: created.GetKnowledgeObject().GetKnowledgeObjectId(),
		ExpectedVersion:   1,
		Definition:        updatedDefinition,
		UpdateMask:        &fieldmaskpb.FieldMask{Paths: []string{"description"}},
		ClientRequestId:   "context-replay-update-0001",
	}
	updated, err := harness.writer.Update(harness.actorContext, harness.scope, updateRequest)
	if err != nil || updated.GetKnowledgeObject().GetVersion() != 2 {
		t.Fatalf("Update baseline = (%v, %v), want version 2/nil", updated, err)
	}
	harness.writer.hook = func(_ context.Context, event writerHookEvent) error {
		if event.Boundary == writerHookIdempotencyChecked && event.Route == mutationRouteUpdate {
			return errWriterFault
		}
		return nil
	}
	_, err = harness.writer.Update(harness.actorContext, harness.scope, updateRequest)
	if !errors.Is(err, errWriterFault) {
		t.Fatalf("exact Update replay fault = %v, want injected failure", err)
	}
	requireCatalogDisposition(t, err, ErrorDispositionKnownCommitted)
	authorized, found := AuthorizedContextFromError(err)
	if !found || authorized.AppID != writerFaultApp || authorized.Object == nil ||
		authorized.Object.KnowledgeObjectID != created.GetKnowledgeObject().GetKnowledgeObjectId() ||
		authorized.Object.Version != 2 ||
		authorized.Object.ObjectType != ObjectTypeFieldAlias ||
		authorized.Object.SharingScope != SharingScopePrivate {
		t.Fatalf("Update replay authorization = %#v, found %v", authorized, found)
	}

	harness.writer.hook = nil
	dropTrigger(t, harness.database, "knowledge_mutation_idempotency_update_is_forbidden")
	mustExec(t, harness.database, `UPDATE knowledge_mutation_idempotency
		SET outcome_proto = X'00'
		WHERE tenant_id = ? AND actor_kind = ? AND actor_id = ?
		  AND route = ? AND client_request_id = ?`,
		writerFaultTenant,
		"browser",
		"writer-fault-administrator",
		mutationRouteCreate,
		createRequest.GetClientRequestId(),
	)
	corruptConflict := proto.Clone(createRequest).(*opensplunkv1.CreateKnowledgeObjectRequest)
	corruptConflict.Definition.Name = "context-replay-corrupt-conflict"
	_, err = harness.writer.Create(harness.actorContext, harness.scope, corruptConflict)
	if !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("corrupt different Create replay = %v, want ErrIdempotencyConflict", err)
	}
	requireCatalogDisposition(t, err, ErrorDispositionDefinitiveRejection)

	_, err = harness.writer.Create(harness.actorContext, harness.scope, createRequest)
	if !errors.Is(err, ErrCorrupt) {
		t.Fatalf("corrupt exact Create replay = %v, want ErrCorrupt", err)
	}
	requireCatalogDisposition(t, err, ErrorDispositionKnownCommitted)
	if authorized, found := AuthorizedContextFromError(err); !found ||
		authorized.AppID != writerFaultApp || authorized.Object != nil {
		t.Fatalf("corrupt Create replay authorization = %#v, found %v; want app-only", authorized, found)
	}
}

func requireCatalogDisposition(t *testing.T, err error, want ErrorDisposition) {
	t.Helper()
	got, found := ErrorDispositionFromError(err)
	if !found || got != want {
		t.Fatalf("error disposition = %v, found %v; want %v", got, found, want)
	}
}
