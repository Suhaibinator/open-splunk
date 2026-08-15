package knowledgecatalog

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/audit"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

func TestWriterDisablesActiveOpaqueFuturePublicationWithoutReinterpretingIt(t *testing.T) {
	exerciseWriterActiveOpaqueEmergencyTransition(t, "disable")
}

func TestWriterDeletesActiveOpaqueFuturePublicationWithoutReinterpretingIt(t *testing.T) {
	exerciseWriterActiveOpaqueEmergencyTransition(t, "delete")
}

func exerciseWriterActiveOpaqueEmergencyTransition(t *testing.T, route string) {
	t.Helper()
	database, store := newCatalogTestStore(t)
	targetID := "ko-writer-opaque-target-" + route
	targetDescription := "recognized extraction published before an opaque dependent"
	insertFixtureObject(t, database, fixtureObject{
		id:    targetID,
		owner: testOwner,
		versions: []fixtureVersion{{
			definition: dependencyExtractionDefinition(
				testApp,
				"writer-opaque-target-"+route,
				SharingScopePrivate,
				&targetDescription,
				"opaque-target-"+route+"-*",
				dependencyFixtureInputField,
			),
			state:     StateActive,
			mutation:  "create",
			timestamp: 10,
		}},
	})
	if target, err := store.Get(t.Context(), testReadScope(), targetID, nil); err != nil ||
		target.ObjectType != ObjectTypeFieldExtraction || target.State != StateActive {
		t.Fatalf("earlier-stage dependency target = (%v, %v)", target, err)
	}

	sourceID := "ko-writer-active-opaque-" + route
	fixture := newWriterActiveOpaqueDefinition(t, "writer-active-opaque-"+route)
	seedWriterActiveOpaquePublication(
		t,
		database,
		sourceID,
		fixture,
		[]fixtureDependency{{targetObjectID: targetID, targetVersion: 1}},
		20,
	)

	// The ordinary reader must never expose or reinterpret executable opaque
	// ACTIVE state. Only Writer's narrow state-only emergency path may preserve
	// this complete publication while moving it to a non-executable state.
	if object, err := store.Get(t.Context(), testReadScope(), sourceID, nil); object.KnowledgeObjectID != "" ||
		!errors.Is(err, ErrCorrupt) {
		t.Fatalf("Get(ACTIVE opaque) = (%v, %v), want zero/ErrCorrupt", object, err)
	}
	beforeProjection := readWriterOpaqueProjection(t, database, sourceID, 1)
	if beforeProjection.state != StateActive {
		t.Fatalf("seeded opaque projection state = %q, want active", beforeProjection.state)
	}
	beforeSelectors := readWriterOpaqueSelectors(t, database, sourceID, 1)
	if len(beforeSelectors) == 0 {
		t.Fatal("seeded opaque fixture has no projected selector rows")
	}
	assertWriterOpaqueImmutableVersions(
		t,
		database,
		sourceID,
		fixture,
		targetID,
		[]writerOpaqueVersionExpectation{{version: 1, state: StateActive, mutation: "create"}},
	)

	writer, actorContext, scope := newWriterOpaqueEmergencyHarness(t, database)
	var committed proto.Message
	var clientRequestID string
	var expectedState State
	switch route {
	case "disable":
		clientRequestID = "active-opaque-disable-request-0001"
		expectedState = StateDisabled
		request := &opensplunkv1.SetKnowledgeObjectStateRequest{
			KnowledgeObjectId: sourceID,
			ExpectedVersion:   1,
			State:             opensplunkv1.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_DISABLED,
			ClientRequestId:   clientRequestID,
		}
		response, err := writer.SetState(actorContext, scope, request)
		if err != nil {
			t.Fatalf("SetState(DISABLED) ACTIVE opaque: %v", err)
		}
		assertWriterOpaqueProtoObject(t, response.GetKnowledgeObject(), sourceID, fixture, expectedState)
		if response.GetTenantCatalogRevision() != 3 || len(response.GetTenantCatalogStateToken()) != 32 {
			t.Fatalf("SetState(DISABLED) ACTIVE opaque catalog authority = %v", response)
		}
		committed = proto.Clone(response)
	case "delete":
		clientRequestID = "active-opaque-delete-request-00001"
		expectedState = StateDeleted
		request := &opensplunkv1.DeleteKnowledgeObjectRequest{
			KnowledgeObjectId: sourceID,
			ExpectedVersion:   1,
			ClientRequestId:   clientRequestID,
		}
		response, err := writer.Delete(actorContext, scope, request)
		if err != nil {
			t.Fatalf("Delete() ACTIVE opaque: %v", err)
		}
		if response.GetKnowledgeObjectId() != sourceID || response.GetDeletedVersion() != 2 ||
			response.GetTenantCatalogRevision() != 3 || len(response.GetTenantCatalogStateToken()) != 32 {
			t.Fatalf("Delete() ACTIVE opaque response = %v", response)
		}
		committed = proto.Clone(response)
	default:
		t.Fatalf("unsupported opaque emergency route %q", route)
	}

	current, err := store.Get(t.Context(), testReadScope(), sourceID, nil)
	if err != nil {
		t.Fatalf("Get(%s opaque v2): %v", route, err)
	}
	assertWriterOpaqueStoredObject(t, current, sourceID, fixture, expectedState)
	v1 := uint64(1)
	if historical, err := store.Get(t.Context(), testReadScope(), sourceID, &v1); historical.KnowledgeObjectID != "" ||
		!errors.Is(err, ErrCorrupt) {
		t.Fatalf("Get(immutable ACTIVE opaque v1) = (%v, %v), want zero/ErrCorrupt", historical, err)
	}

	assertWriterOpaqueImmutableVersions(
		t,
		database,
		sourceID,
		fixture,
		targetID,
		[]writerOpaqueVersionExpectation{
			{version: 1, state: StateActive, mutation: "create"},
			{version: 2, state: expectedState, mutation: route},
		},
	)
	afterProjection := readWriterOpaqueProjection(t, database, sourceID, 2)
	if afterProjection.state != expectedState ||
		!reflect.DeepEqual(afterProjection.signature, beforeProjection.signature) {
		t.Fatalf("opaque %s projection changed: before=%#v after=%#v", route, beforeProjection, afterProjection)
	}
	afterSelectors := readWriterOpaqueSelectors(t, database, sourceID, 2)
	if !reflect.DeepEqual(afterSelectors, beforeSelectors) {
		t.Fatalf("opaque %s selectors changed: before=%#v after=%#v", route, beforeSelectors, afterSelectors)
	}
	assertWriterOpaqueOnlyCurrentProjection(t, database, sourceID, 2)
	assertWriterOpaqueCompactReceipt(t, database, route, clientRequestID, sourceID, fixture.digest[:])

	stable := readWriterOpaqueReplaySnapshot(t, database, sourceID)
	if stable.catalogRevision != 3 || stable.versionCount != 3 || stable.objectVersions != 2 ||
		stable.auditEvents != 1 || stable.idempotency != 1 {
		t.Fatalf("opaque %s committed authority = %#v", route, stable)
	}
	var replayed proto.Message
	switch route {
	case "disable":
		response, err := writer.SetState(actorContext, scope, &opensplunkv1.SetKnowledgeObjectStateRequest{
			KnowledgeObjectId: sourceID,
			ExpectedVersion:   1,
			State:             opensplunkv1.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_DISABLED,
			ClientRequestId:   clientRequestID,
		})
		if err != nil {
			t.Fatalf("replay SetState(DISABLED) ACTIVE opaque: %v", err)
		}
		replayed = response
	case "delete":
		response, err := writer.Delete(actorContext, scope, &opensplunkv1.DeleteKnowledgeObjectRequest{
			KnowledgeObjectId: sourceID,
			ExpectedVersion:   1,
			ClientRequestId:   clientRequestID,
		})
		if err != nil {
			t.Fatalf("replay Delete() ACTIVE opaque: %v", err)
		}
		replayed = response
	}
	if !proto.Equal(replayed, committed) {
		t.Fatalf("opaque %s replay = %v, want exact %v", route, replayed, committed)
	}
	if got := readWriterOpaqueReplaySnapshot(t, database, sourceID); !reflect.DeepEqual(got, stable) {
		t.Fatalf("opaque %s replay changed audit/revision/version authority: got=%#v want=%#v", route, got, stable)
	}
	assertWriterOpaqueImmutableVersions(
		t,
		database,
		sourceID,
		fixture,
		targetID,
		[]writerOpaqueVersionExpectation{
			{version: 1, state: StateActive, mutation: "create"},
			{version: 2, state: expectedState, mutation: route},
		},
	)
	assertWriterForwardCompatIntegrity(t, database)
}

func newWriterActiveOpaqueDefinition(t *testing.T, name string) integrationFutureDefinition {
	t.Helper()
	fixture := newIntegrationFutureDefinition(t, name, 0)
	futureMetadata := protowire.AppendString(
		protowire.AppendTag(nil, 32, protowire.BytesType),
		"newer-binary-metadata-"+name,
	)
	knownBytes := fixture.bytes[:len(fixture.bytes)-len(fixture.bodyField)]
	fixture.bodyField = append(bytes.Clone(futureMetadata), fixture.bodyField...)
	fixture.bytes = append(bytes.Clone(knownBytes), fixture.bodyField...)
	fixture.digest = sha256.Sum256(fixture.bytes)
	fixture.definition = &opensplunkv1.KnowledgeObjectDefinition{}
	if err := (proto.UnmarshalOptions{DiscardUnknown: false}).Unmarshal(fixture.bytes, fixture.definition); err != nil {
		t.Fatalf("decode ACTIVE opaque newer-binary fixture: %v", err)
	}
	return fixture
}

func seedWriterActiveOpaquePublication(
	t *testing.T,
	database *control.DB,
	objectID string,
	fixture integrationFutureDefinition,
	dependencies []fixtureDependency,
	timestamp int64,
) {
	t.Helper()
	// Test-only SQL is intentional here: it simulates a complete, schema-valid
	// ACTIVE publication committed by a newer binary. This older Writer must not
	// create or enable an opaque body; it may only preserve the publication while
	// performing an emergency disable or delete.
	tx, err := database.SQLDB().BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatalf("begin newer-binary opaque publication fixture: %v", err)
	}
	defer func() { _ = tx.Rollback() }()
	ensureIntegrationCatalogLedgers(t, tx)
	insertIntegrationDefinitionBlob(t, tx, fixture.bytes, fixture.digest[:], timestamp)
	insertIntegrationVersionWithDependencies(
		t,
		tx,
		objectID,
		1,
		StateActive,
		"create",
		fixture.digest[:],
		fixture.metadata,
		timestamp,
		dependencies,
	)
	insertIntegrationProjection(t, tx, objectID, 1, StateActive, fixture.metadata)
	objectType, typeOK := objectTypeFromProto(fixture.metadata.ObjectType)
	sharingScope, scopeOK := sharingScopeFromProto(fixture.metadata.SharingScope)
	if !typeOK || !scopeOK {
		t.Fatal("opaque publication fixture has unsupported scalar identity")
	}
	if _, err := tx.ExecContext(t.Context(), `INSERT INTO knowledge_objects (
		tenant_id, knowledge_object_id, current_version, app_id, owner_id, object_type, name,
		sharing_scope, state, definition_digest, created_at_unix_micro, updated_at_unix_micro,
		disabled_at_unix_micro, quarantined_at_unix_micro, deleted_at_unix_micro, quarantine_reason
	) VALUES (?, ?, 1, ?, ?, ?, ?, ?, 'active', ?, ?, ?, NULL, NULL, NULL, NULL)`,
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
		t.Fatalf("publish newer-binary opaque registry: %v", err)
	}
	advanceIntegrationCatalogRevision(t, tx)
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit newer-binary opaque publication fixture: %v", err)
	}
}

func newWriterOpaqueEmergencyHarness(
	t *testing.T,
	database *control.DB,
) (*Writer, context.Context, WriteScope) {
	t.Helper()
	auditStore, err := audit.NewStore(database, audit.StoreOptions{CursorKey: testCursorKey})
	if err != nil {
		t.Fatalf("audit.NewStore(): %v", err)
	}
	writer, err := NewWriter(database, auditStore, WriterOptions{
		Clock: func() time.Time { return time.UnixMicro(30).UTC() },
		IDGenerator: func() (string, error) {
			return "", errors.New("state-only opaque transition requested an identity")
		},
	})
	if err != nil {
		t.Fatalf("NewWriter(): %v", err)
	}
	actorContext, err := audit.WithActor(t.Context(), audit.Actor{
		Kind: audit.ActorKindBrowser,
		ID:   "writer-opaque-administrator",
		Role: audit.ActorRoleAdministrator,
	})
	if err != nil {
		t.Fatalf("audit.WithActor(): %v", err)
	}
	return writer, actorContext, WriteScope{
		TenantID:       testTenant,
		OwnerID:        testOwner,
		WritableAppIDs: []string{testApp},
	}
}

type writerOpaqueVersionExpectation struct {
	version  int64
	state    State
	mutation string
}

func assertWriterOpaqueImmutableVersions(
	t *testing.T,
	database *control.DB,
	objectID string,
	fixture integrationFutureDefinition,
	targetID string,
	want []writerOpaqueVersionExpectation,
) {
	t.Helper()
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
		t.Fatalf("read opaque immutable versions: %v", err)
	}
	defer rows.Close()
	var index int
	for rows.Next() {
		if index >= len(want) {
			t.Fatalf("opaque immutable versions include unexpected row %d", index+1)
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
			t.Fatalf("scan opaque immutable version: %v", err)
		}
		expected := want[index]
		if version != expected.version || state != expected.state || mutation != expected.mutation ||
			dependencyCount != 1 || definitionBytes != int64(len(fixture.bytes)) ||
			!bytes.Equal(digest, fixture.digest[:]) || !bytes.Equal(definition, fixture.bytes) {
			t.Fatalf(
				"opaque immutable v%d = state=%q mutation=%q deps=%d digest=%x bytes=%x/%d; want %#v exact digest/body",
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
		index++
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate opaque immutable versions: %v", err)
	}
	if index != len(want) {
		t.Fatalf("opaque immutable version rows = %d, want %d", index, len(want))
	}

	dependencyRows, err := database.SQLDB().QueryContext(t.Context(), `
		SELECT source_object_version, ordinal, target_kind, target_object_id,
		       target_object_version, dependency_role
		FROM knowledge_object_dependencies
		WHERE tenant_id = ? AND source_object_id = ?
		ORDER BY source_object_version, ordinal`, testTenant, objectID)
	if err != nil {
		t.Fatalf("read opaque immutable dependencies: %v", err)
	}
	defer dependencyRows.Close()
	index = 0
	for dependencyRows.Next() {
		if index >= len(want) {
			t.Fatalf("opaque dependencies include unexpected row %d", index+1)
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
			t.Fatalf("scan opaque immutable dependency: %v", err)
		}
		if version != want[index].version || ordinal != 0 || targetKind != "object" ||
			gotTargetID != targetID || targetVersion != 1 || role != "field_input" {
			t.Fatalf(
				"opaque dependency %d = v%d/%d/%q/%q/v%d/%q",
				index,
				version,
				ordinal,
				targetKind,
				gotTargetID,
				targetVersion,
				role,
			)
		}
		index++
	}
	if err := dependencyRows.Err(); err != nil {
		t.Fatalf("iterate opaque immutable dependencies: %v", err)
	}
	if index != len(want) {
		t.Fatalf("opaque dependency rows = %d, want %d", index, len(want))
	}

	var sealCount, exactSeals int64
	if err := database.SQLDB().QueryRowContext(t.Context(), `
		SELECT count(*), coalesce(sum(CASE WHEN dependency_count = 1 THEN 1 ELSE 0 END), 0)
		FROM knowledge_object_dependency_seals
		WHERE tenant_id = ? AND knowledge_object_id = ?`, testTenant, objectID).Scan(
		&sealCount,
		&exactSeals,
	); err != nil {
		t.Fatalf("read opaque dependency seals: %v", err)
	}
	if sealCount != int64(len(want)) || exactSeals != int64(len(want)) {
		t.Fatalf("opaque dependency seals = %d/%d, want %d exact", sealCount, exactSeals, len(want))
	}
}

func assertWriterOpaqueProtoObject(
	t *testing.T,
	object *opensplunkv1.KnowledgeObject,
	objectID string,
	fixture integrationFutureDefinition,
	state State,
) {
	t.Helper()
	if object == nil || object.GetKnowledgeObjectId() != objectID || object.GetVersion() != 2 ||
		object.GetState() != stateProto(state) ||
		!bytes.Equal(object.GetDefinitionSha256(), fixture.digest[:]) {
		t.Fatalf("opaque Writer response object = %v", object)
	}
	assertWriterOpaqueDefinitionBytes(t, object.GetDefinition(), fixture)
}

func assertWriterOpaqueStoredObject(
	t *testing.T,
	object Object,
	objectID string,
	fixture integrationFutureDefinition,
	state State,
) {
	t.Helper()
	if object.KnowledgeObjectID != objectID || object.Version != 2 || object.State != state ||
		!bytes.Equal(object.DefinitionSHA256, fixture.digest[:]) {
		t.Fatalf("stored opaque v2 object = %v", object)
	}
	assertWriterOpaqueDefinitionBytes(t, object.Definition, fixture)
}

func assertWriterOpaqueDefinitionBytes(
	t *testing.T,
	definition *opensplunkv1.KnowledgeObjectDefinition,
	fixture integrationFutureDefinition,
) {
	t.Helper()
	encoded, err := (proto.MarshalOptions{Deterministic: true}).Marshal(definition)
	if err != nil {
		t.Fatalf("marshal preserved opaque definition: %v", err)
	}
	if definition.GetBody() != nil || !bytes.Equal(integrationUnknownBody(definition), fixture.bodyField) ||
		!bytes.Equal(encoded, fixture.bytes) {
		t.Fatalf(
			"preserved opaque definition = body %T unknown=%x canonical=%x; want nil/%x/%x",
			definition.GetBody(),
			integrationUnknownBody(definition),
			encoded,
			fixture.bodyField,
			fixture.bytes,
		)
	}
}

func stateProto(state State) opensplunkv1.KnowledgeObjectState {
	switch state {
	case StateDisabled:
		return opensplunkv1.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_DISABLED
	case StateDeleted:
		return opensplunkv1.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_DELETED
	default:
		return opensplunkv1.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_UNSPECIFIED
	}
}

type writerOpaqueProjectionSignature struct {
	appID                    string
	ownerID                  string
	objectType               string
	name                     string
	sharingScope             string
	descriptionPresent       int64
	description              string
	indexSelectorCount       int64
	hostSelectorCount        int64
	sourceSelectorCount      int64
	sourcetypeSelectorCount  int64
	selectorValueBytes       int64
	canonicalSelectorBytes   int64
	projectionBytes          int64
	sealProjectionBytes      int64
	sealCanonicalSelectBytes int64
}

type writerOpaqueProjection struct {
	state     State
	signature writerOpaqueProjectionSignature
}

func readWriterOpaqueProjection(
	t *testing.T,
	database *control.DB,
	objectID string,
	version int64,
) writerOpaqueProjection {
	t.Helper()
	var result writerOpaqueProjection
	if err := database.SQLDB().QueryRowContext(t.Context(), `
		SELECT projection.state, projection.app_id, projection.owner_id,
		       projection.object_type, projection.name, projection.sharing_scope,
		       projection.description_present, projection.description,
		       projection.index_selector_count, projection.host_selector_count,
		       projection.source_selector_count, projection.sourcetype_selector_count,
		       projection.selector_value_bytes, projection.canonical_selector_bytes,
		       projection.projection_bytes, seal.projection_bytes,
		       seal.canonical_selector_bytes
		FROM knowledge_object_list_projections AS projection
		JOIN knowledge_object_list_projection_seals AS seal
		  ON seal.tenant_id = projection.tenant_id
		 AND seal.knowledge_object_id = projection.knowledge_object_id
		 AND seal.object_version = projection.object_version
		WHERE projection.tenant_id = ? AND projection.knowledge_object_id = ?
		  AND projection.object_version = ?`, testTenant, objectID, version).Scan(
		&result.state,
		&result.signature.appID,
		&result.signature.ownerID,
		&result.signature.objectType,
		&result.signature.name,
		&result.signature.sharingScope,
		&result.signature.descriptionPresent,
		&result.signature.description,
		&result.signature.indexSelectorCount,
		&result.signature.hostSelectorCount,
		&result.signature.sourceSelectorCount,
		&result.signature.sourcetypeSelectorCount,
		&result.signature.selectorValueBytes,
		&result.signature.canonicalSelectorBytes,
		&result.signature.projectionBytes,
		&result.signature.sealProjectionBytes,
		&result.signature.sealCanonicalSelectBytes,
	); err != nil {
		t.Fatalf("read opaque projection v%d: %v", version, err)
	}
	return result
}

type writerOpaqueSelector struct {
	dimension string
	ordinal   int64
	matchKind string
	value     string
}

func readWriterOpaqueSelectors(
	t *testing.T,
	database *control.DB,
	objectID string,
	version int64,
) []writerOpaqueSelector {
	t.Helper()
	rows, err := database.SQLDB().QueryContext(t.Context(), `
		SELECT dimension, ordinal, match_kind, value
		FROM knowledge_object_list_selector_patterns
		WHERE tenant_id = ? AND knowledge_object_id = ? AND object_version = ?
		ORDER BY dimension, ordinal`, testTenant, objectID, version)
	if err != nil {
		t.Fatalf("read opaque selectors v%d: %v", version, err)
	}
	defer rows.Close()
	var result []writerOpaqueSelector
	for rows.Next() {
		var selector writerOpaqueSelector
		if err := rows.Scan(&selector.dimension, &selector.ordinal, &selector.matchKind, &selector.value); err != nil {
			t.Fatalf("scan opaque selector v%d: %v", version, err)
		}
		result = append(result, selector)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate opaque selectors v%d: %v", version, err)
	}
	return result
}

func assertWriterOpaqueOnlyCurrentProjection(
	t *testing.T,
	database *control.DB,
	objectID string,
	currentVersion int64,
) {
	t.Helper()
	for _, table := range []string{
		"knowledge_object_list_projections",
		"knowledge_object_list_projection_seals",
		"knowledge_object_list_order_keys",
	} {
		var count, current int64
		query := fmt.Sprintf(`SELECT count(*), coalesce(sum(CASE WHEN object_version = ? THEN 1 ELSE 0 END), 0)
			FROM %s WHERE tenant_id = ? AND knowledge_object_id = ?`, table) // #nosec G201 -- fixed test constants.
		if err := database.SQLDB().QueryRowContext(t.Context(), query, currentVersion, testTenant, objectID).Scan(
			&count,
			&current,
		); err != nil {
			t.Fatalf("read opaque current authority %s: %v", table, err)
		}
		if count != 1 || current != 1 {
			t.Fatalf("opaque current authority %s = %d/%d, want exactly v%d", table, count, current, currentVersion)
		}
	}
	var selectors, currentSelectors int64
	if err := database.SQLDB().QueryRowContext(t.Context(), `
		SELECT count(*), coalesce(sum(CASE WHEN object_version = ? THEN 1 ELSE 0 END), 0)
		FROM knowledge_object_list_selector_patterns
		WHERE tenant_id = ? AND knowledge_object_id = ?`, currentVersion, testTenant, objectID).Scan(
		&selectors,
		&currentSelectors,
	); err != nil {
		t.Fatalf("read opaque current selectors: %v", err)
	}
	if selectors < 1 || selectors != currentSelectors {
		t.Fatalf("opaque current selectors = %d/%d, want only v%d", selectors, currentSelectors, currentVersion)
	}
}

func assertWriterOpaqueCompactReceipt(
	t *testing.T,
	database *control.DB,
	mutationKind string,
	requestID string,
	objectID string,
	digest []byte,
) {
	t.Helper()
	route := mutationRouteDelete
	if mutationKind == "disable" {
		route = mutationRouteSetState
	}
	var outcome []byte
	var stateToken []byte
	var recordedObjectID string
	var recordedVersion, catalogRevision, auditSequence, auditVersion int64
	var auditAction audit.Action
	var actorKind audit.ActorKind
	var actorID string
	var actorRole audit.ActorRole
	var targetKind audit.TargetKind
	var targetID, appID, objectType, sharingScope string
	if err := database.SQLDB().QueryRowContext(t.Context(), `
		SELECT receipt.outcome_proto, receipt.knowledge_object_id, receipt.object_version,
		       receipt.committed_catalog_revision, receipt.committed_catalog_state_token,
		       receipt.successful_audit_sequence, event.action, event.actor_kind,
		       event.actor_id, event.actor_role, event.target_kind, event.target_id,
		       event.target_version, event.app_id, event.object_type, event.sharing_scope
		FROM knowledge_mutation_idempotency AS receipt
		JOIN audit_events AS event
		  ON event.tenant_id = receipt.tenant_id
		 AND event.sequence = receipt.successful_audit_sequence
		WHERE receipt.tenant_id = ? AND receipt.actor_kind = ? AND receipt.actor_id = ?
		  AND receipt.route = ? AND receipt.client_request_id = ?`,
		testTenant,
		audit.ActorKindBrowser,
		"writer-opaque-administrator",
		route,
		requestID,
	).Scan(
		&outcome,
		&recordedObjectID,
		&recordedVersion,
		&catalogRevision,
		&stateToken,
		&auditSequence,
		&auditAction,
		&actorKind,
		&actorID,
		&actorRole,
		&targetKind,
		&targetID,
		&auditVersion,
		&appID,
		&objectType,
		&sharingScope,
	); err != nil {
		t.Fatalf("read opaque compact receipt: %v", err)
	}
	if len(outcome) < 1 || len(outcome) > maximumMutationOutcomeBytes ||
		recordedObjectID != objectID || recordedVersion != 2 {
		t.Fatalf("opaque compact receipt = %d bytes/%q/v%d", len(outcome), recordedObjectID, recordedVersion)
	}
	wantAction := audit.ActionKnowledgeObjectDelete
	if mutationKind == "disable" {
		wantAction = audit.ActionKnowledgeObjectDisable
	}
	if auditSequence != 1 || auditAction != wantAction || actorKind != audit.ActorKindBrowser ||
		actorID != "writer-opaque-administrator" || actorRole != audit.ActorRoleAdministrator ||
		targetKind != audit.TargetKindKnowledgeObject || targetID != objectID || auditVersion != 2 ||
		appID != testApp || objectType != string(ObjectTypeFieldAlias) ||
		sharingScope != string(SharingScopePrivate) {
		t.Fatalf(
			"opaque compact receipt audit provenance = seq=%d action=%q actor=%q/%q/%q target=%q/%q/v%d metadata=%q/%q/%q",
			auditSequence,
			auditAction,
			actorKind,
			actorID,
			actorRole,
			targetKind,
			targetID,
			auditVersion,
			appID,
			objectType,
			sharingScope,
		)
	}
	envelope := &opensplunkv1.KnowledgeMutationOutcomeRecord{}
	if err := (proto.UnmarshalOptions{DiscardUnknown: false}).Unmarshal(outcome, envelope); err != nil ||
		len(envelope.ProtoReflect().GetUnknown()) != 0 || envelope.GetObject() == nil ||
		envelope.GetRoute() != route || envelope.GetMutationKind() != mutationKind ||
		envelope.GetTenantCatalogRevision() != uint64(catalogRevision) ||
		!bytes.Equal(envelope.GetTenantCatalogStateToken(), stateToken) ||
		envelope.GetSuccessfulAuditSequence() != uint64(auditSequence) ||
		envelope.GetObject().GetKnowledgeObjectId() != objectID || envelope.GetObject().GetVersion() != 2 ||
		!bytes.Equal(envelope.GetObject().GetDefinitionSha256(), digest) {
		t.Fatalf("opaque compact outcome reference = (%v, %v)", envelope, err)
	}
	canonical, err := (proto.MarshalOptions{Deterministic: true}).Marshal(envelope)
	if err != nil || !bytes.Equal(canonical, outcome) {
		t.Fatalf("opaque compact outcome is noncanonical: bytes=%x canonical=%x err=%v", outcome, canonical, err)
	}
}

type writerOpaqueReplaySnapshot struct {
	catalogRevision int64
	stateToken      [32]byte
	versionCount    int64
	objectVersions  int64
	auditEvents     int64
	idempotency     int64
}

func readWriterOpaqueReplaySnapshot(
	t *testing.T,
	database *control.DB,
	objectID string,
) writerOpaqueReplaySnapshot {
	t.Helper()
	var result writerOpaqueReplaySnapshot
	var token []byte
	if err := database.SQLDB().QueryRowContext(t.Context(), `
		SELECT tenant.catalog_revision, head.state_token, tenant.version_count,
		       (SELECT count(*) FROM knowledge_object_versions
		        WHERE tenant_id = tenant.tenant_id AND knowledge_object_id = ?),
		       (SELECT count(*) FROM audit_events WHERE tenant_id = tenant.tenant_id),
		       tenant.idempotency_count
		FROM knowledge_catalog_tenants AS tenant
		JOIN knowledge_catalog_revision_heads AS head
		  ON head.tenant_id = tenant.tenant_id
		 AND head.catalog_revision = tenant.catalog_revision
		WHERE tenant.tenant_id = ?`, objectID, testTenant).Scan(
		&result.catalogRevision,
		&token,
		&result.versionCount,
		&result.objectVersions,
		&result.auditEvents,
		&result.idempotency,
	); err != nil {
		t.Fatalf("read opaque replay snapshot: %v", err)
	}
	if len(token) != len(result.stateToken) {
		t.Fatalf("opaque replay state token bytes = %d, want %d", len(token), len(result.stateToken))
	}
	copy(result.stateToken[:], token)
	return result
}
