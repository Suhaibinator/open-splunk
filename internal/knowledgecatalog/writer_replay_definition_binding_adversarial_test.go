package knowledgecatalog

import (
	"bytes"
	"errors"
	"testing"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/audit"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

func TestWriterCreateReplayRejectsRequestDigestRebinding(t *testing.T) {
	harness := newWriterFaultHarness(t)
	request := writerFaultCreateRequest(
		"create-digest-binding",
		"create-digest-binding-request-0001",
	)
	created, err := harness.writer.Create(harness.actorContext, harness.scope, request)
	if err != nil {
		t.Fatalf("commit original Create(): %v", err)
	}
	original := created.GetKnowledgeObject()

	altered := proto.Clone(request).(*opensplunkv1.CreateKnowledgeObjectRequest)
	// Duplicate selector patterns normalize to the same stored definition, but
	// remain distinct canonical request bytes. This isolates the immutable
	// request-digest bridge from definition-authority replay validation.
	altered.GetDefinition().GetSelector().HostPatterns = append(
		altered.GetDefinition().GetSelector().HostPatterns,
		proto.Clone(altered.GetDefinition().GetSelector().GetHostPatterns()[0]).(*opensplunkv1.KnowledgeSelectorPattern),
	)
	alteredPrepared := prepareWriterReplayBindingCreate(t, harness, altered)
	restoreDigest := tamperWriterReplayRequestDigest(
		t,
		harness,
		mutationRouteCreate,
		request.GetClientRequestId(),
		alteredPrepared.requestDigest[:],
	)
	stable := readWriterFaultSnapshot(t, harness.database)

	replayed, err := harness.writer.Create(harness.actorContext, harness.scope, altered)
	if replayed != nil || !errors.Is(err, ErrCorrupt) {
		t.Fatalf("Create() after request-digest rebinding = (%v, %v), want nil/ErrCorrupt", replayed, err)
	}
	assertWriterFaultSnapshotsEqual(t, readWriterFaultSnapshot(t, harness.database), stable)
	assertWriterReplayStoredDefinition(
		t,
		harness,
		original.GetKnowledgeObjectId(),
		1,
		request.GetDefinition(),
	)
	restoreDigest()
	assertWriterFaultIntegrity(t, harness.database)
}

func TestWriterUpdateReplayRejectsRequestDigestRebinding(t *testing.T) {
	harness := newWriterFaultHarness(t)
	created, err := harness.writer.Create(
		harness.actorContext,
		harness.scope,
		writerFaultCreateRequest(
			"update-digest-binding",
			"update-digest-binding-create-0001",
		),
	)
	if err != nil {
		t.Fatalf("commit Update baseline Create(): %v", err)
	}
	objectID := created.GetKnowledgeObject().GetKnowledgeObjectId()

	originalDescription := "the originally committed masked description"
	originalDefinition := proto.Clone(created.GetKnowledgeObject().GetDefinition()).(*opensplunkv1.KnowledgeObjectDefinition)
	originalDefinition.Description = &originalDescription
	request := &opensplunkv1.UpdateKnowledgeObjectRequest{
		KnowledgeObjectId: objectID,
		ExpectedVersion:   1,
		Definition:        originalDefinition,
		UpdateMask:        &fieldmaskpb.FieldMask{Paths: []string{"description"}},
		ClientRequestId:   "update-digest-binding-request-0001",
	}
	updated, err := harness.writer.Update(harness.actorContext, harness.scope, request)
	if err != nil {
		t.Fatalf("commit original Update(): %v", err)
	}
	if updated.GetKnowledgeObject().GetDefinition().GetDescription() != originalDescription {
		t.Fatalf("committed Update definition = %v", updated.GetKnowledgeObject().GetDefinition())
	}

	altered := proto.Clone(request).(*opensplunkv1.UpdateKnowledgeObjectRequest)
	// Name is intentionally outside the description-only mask. The committed
	// candidate therefore remains identical while the canonical request digest
	// changes, isolating the immutable digest bridge.
	altered.Definition.Name = "unmasked-request-digest-name"
	alteredPrepared := prepareWriterReplayBindingUpdate(t, harness, altered)
	restoreDigest := tamperWriterReplayRequestDigest(
		t,
		harness,
		mutationRouteUpdate,
		request.GetClientRequestId(),
		alteredPrepared.requestDigest[:],
	)
	stable := readWriterFaultSnapshot(t, harness.database)

	replayed, err := harness.writer.Update(harness.actorContext, harness.scope, altered)
	if replayed != nil || !errors.Is(err, ErrCorrupt) {
		t.Fatalf("Update() after request-digest rebinding = (%v, %v), want nil/ErrCorrupt", replayed, err)
	}
	assertWriterFaultSnapshotsEqual(t, readWriterFaultSnapshot(t, harness.database), stable)
	assertWriterReplayStoredDefinition(t, harness, objectID, 2, originalDefinition)
	restoreDigest()
	assertWriterFaultIntegrity(t, harness.database)
}

func prepareWriterReplayBindingCreate(
	t *testing.T,
	harness *writerFaultHarness,
	request *opensplunkv1.CreateKnowledgeObjectRequest,
) preparedMutation {
	t.Helper()
	scope, err := normalizeWriteScope(harness.scope)
	if err != nil {
		t.Fatalf("normalize Writer scope: %v", err)
	}
	actor, ok := audit.ActorFromContext(harness.actorContext)
	if !ok {
		t.Fatal("Writer harness has no explicit actor")
	}
	prepared, err := prepareCreateMutation(scope, actor, request)
	if err != nil {
		t.Fatalf("prepare altered Create(): %v", err)
	}
	return prepared
}

func prepareWriterReplayBindingUpdate(
	t *testing.T,
	harness *writerFaultHarness,
	request *opensplunkv1.UpdateKnowledgeObjectRequest,
) preparedMutation {
	t.Helper()
	scope, err := normalizeWriteScope(harness.scope)
	if err != nil {
		t.Fatalf("normalize Writer scope: %v", err)
	}
	actor, ok := audit.ActorFromContext(harness.actorContext)
	if !ok {
		t.Fatal("Writer harness has no explicit actor")
	}
	prepared, err := prepareUpdateMutation(scope, actor, request)
	if err != nil {
		t.Fatalf("prepare altered Update(): %v", err)
	}
	return prepared
}

func tamperWriterReplayRequestDigest(
	t *testing.T,
	harness *writerFaultHarness,
	route string,
	requestID string,
	digest []byte,
) func() {
	t.Helper()
	if len(digest) != 32 {
		t.Fatalf("altered request digest bytes = %d, want 32", len(digest))
	}
	connection, err := harness.database.SQLDB().Conn(t.Context())
	if err != nil {
		t.Fatalf("open request-digest corruption connection: %v", err)
	}
	var original []byte
	if err := connection.QueryRowContext(t.Context(), `
		SELECT request_digest FROM knowledge_mutation_idempotency
		WHERE tenant_id = ? AND actor_kind = ? AND actor_id = ?
		  AND route = ? AND client_request_id = ?`,
		writerFaultTenant,
		audit.ActorKindBrowser,
		"writer-fault-administrator",
		route,
		requestID,
	).Scan(&original); err != nil {
		_ = connection.Close()
		t.Fatalf("read original %s request digest: %v", route, err)
	}
	if _, err := connection.ExecContext(t.Context(), `
		DROP TRIGGER knowledge_mutation_idempotency_update_is_forbidden`); err != nil {
		_ = connection.Close()
		t.Fatalf("drop immutable receipt update trigger: %v", err)
	}
	updateSQL := `
		UPDATE knowledge_mutation_idempotency
		SET request_digest = ?
		WHERE tenant_id = ? AND actor_kind = ? AND actor_id = ?
		  AND route = ? AND client_request_id = ?`
	args := []any{
		digest,
		writerFaultTenant,
		audit.ActorKindBrowser,
		"writer-fault-administrator",
		route,
		requestID,
	}
	if _, err := connection.ExecContext(t.Context(), updateSQL, args...); err == nil {
		_ = connection.Close()
		t.Fatalf("row-only %s request-digest tamper succeeded with foreign keys enabled", route)
	}
	if _, err := connection.ExecContext(t.Context(), `PRAGMA foreign_keys = OFF`); err != nil {
		_ = connection.Close()
		t.Fatalf("disable request-digest foreign keys: %v", err)
	}
	result, err := connection.ExecContext(t.Context(), updateSQL, args...)
	if err != nil {
		_ = connection.Close()
		t.Fatalf("rebind %s request digest: %v", route, err)
	}
	if affected, err := result.RowsAffected(); err != nil || affected != 1 {
		_ = connection.Close()
		t.Fatalf("rebind %s request digest rows = %d, %v; want 1", route, affected, err)
	}
	if _, err := connection.ExecContext(t.Context(), `PRAGMA foreign_keys = ON`); err != nil {
		_ = connection.Close()
		t.Fatalf("restore request-digest foreign keys: %v", err)
	}
	if err := connection.Close(); err != nil {
		t.Fatalf("close request-digest corruption connection: %v", err)
	}
	return func() {
		result, err := harness.database.SQLDB().ExecContext(t.Context(), updateSQL,
			original,
			writerFaultTenant,
			audit.ActorKindBrowser,
			"writer-fault-administrator",
			route,
			requestID,
		)
		if err != nil {
			t.Fatalf("restore %s request digest: %v", route, err)
		}
		if affected, err := result.RowsAffected(); err != nil || affected != 1 {
			t.Fatalf("restore %s request digest rows = %d, %v; want 1", route, affected, err)
		}
	}
}

func assertWriterReplayStoredDefinition(
	t *testing.T,
	harness *writerFaultHarness,
	objectID string,
	version int64,
	want *opensplunkv1.KnowledgeObjectDefinition,
) {
	t.Helper()
	wantBytes, err := (proto.MarshalOptions{Deterministic: true}).Marshal(want)
	if err != nil {
		t.Fatalf("marshal expected stored definition: %v", err)
	}
	var gotBytes, versionDigest, blobDigest []byte
	if err := harness.database.SQLDB().QueryRowContext(t.Context(), `
		SELECT blob.definition_proto, version.definition_digest, blob.definition_digest
		FROM knowledge_object_versions AS version
		JOIN knowledge_definition_blobs AS blob
		  ON blob.tenant_id = version.tenant_id
		 AND blob.definition_digest = version.definition_digest
		WHERE version.tenant_id = ? AND version.knowledge_object_id = ?
		  AND version.object_version = ?`,
		writerFaultTenant,
		objectID,
		version,
	).Scan(&gotBytes, &versionDigest, &blobDigest); err != nil {
		t.Fatalf("read stored definition v%d: %v", version, err)
	}
	if !bytes.Equal(gotBytes, wantBytes) || !bytes.Equal(versionDigest, blobDigest) {
		t.Fatalf(
			"stored definition v%d changed: bytes=%x want=%x version_digest=%x blob_digest=%x",
			version,
			gotBytes,
			wantBytes,
			versionDigest,
			blobDigest,
		)
	}
}
