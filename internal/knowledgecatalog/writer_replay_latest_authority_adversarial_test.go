package knowledgecatalog

import (
	"database/sql"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

const writerReplayLatestHiddenApp = "app_000000000300000000003A"

func TestWriterImmutableQuarantineReplayRedactionSurvivesRegistryRollback(t *testing.T) {
	tests := []struct {
		name    string
		rewrite string
	}{
		{name: "current version rollback", rewrite: "rollback"},
		{name: "current state rewrite", rewrite: "state"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			harness := newWriterFaultHarness(t)
			request := writerFaultCreateRequest(
				"immutable-quarantine-"+test.rewrite,
				"immutable-quarantine-"+test.rewrite+"-request-0001",
			)
			created, err := harness.writer.Create(harness.actorContext, harness.scope, request)
			if err != nil {
				t.Fatalf("commit Create baseline: %v", err)
			}
			objectID := created.GetKnowledgeObject().GetKnowledgeObjectId()

			stageWriterReplayQuarantineV2(t, harness, objectID)
			rewriteWriterReplayQuarantineRegistry(t, harness, objectID, test.rewrite)
			assertWriterReplayImmutableQuarantineV2(t, harness, objectID, test.rewrite)
			corruptSingleWriterReplayReceipt(t, harness, request.GetClientRequestId())
			removeSingleWriterReplayDefinitionBlob(t, harness)

			before := readWriterFaultSnapshot(t, harness.database)
			response, err := harness.writer.Create(
				harness.actorContext,
				harness.scope,
				proto.Clone(request).(*opensplunk.CreateKnowledgeObjectRequest),
			)
			if response != nil || !errors.Is(err, ErrIdempotentOutcomeRedacted) {
				t.Fatalf("replay = (%v, %v), want nil/fixed redaction", response, err)
			}
			if err.Error() != ErrIdempotentOutcomeRedacted.Error() {
				t.Fatalf("replay text = %q, want %q", err, ErrIdempotentOutcomeRedacted)
			}
			assertWriterFaultSnapshotsEqual(t, readWriterFaultSnapshot(t, harness.database), before)
		})
	}
}

func TestWriterLatestImmutableOwnerOrAppNondisclosurePrecedesReceiptAndBody(t *testing.T) {
	tests := []struct {
		name     string
		hiddenBy string
	}{
		{name: "owner hidden", hiddenBy: "owner"},
		{name: "app hidden", hiddenBy: "app"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			harness := newWriterFaultHarness(t)
			request := writerFaultCreateRequest(
				"latest-immutable-hidden-"+test.hiddenBy,
				"latest-immutable-hidden-"+test.hiddenBy+"-request-0001",
			)
			created, err := harness.writer.Create(harness.actorContext, harness.scope, request)
			if err != nil {
				t.Fatalf("commit Create baseline: %v", err)
			}

			latestOwner := writerFaultOwner
			latestApp := writerFaultApp
			switch test.hiddenBy {
			case "owner":
				latestOwner = "x" + writerFaultOwner[1:]
			case "app":
				createWriterReplayLatestHiddenApp(t, harness)
				latestApp = writerReplayLatestHiddenApp
			default:
				t.Fatalf("unknown hidden authority %q", test.hiddenBy)
			}
			stageWriterReplayUnauthorizedLatestV2(
				t,
				harness,
				created.GetKnowledgeObject().GetKnowledgeObjectId(),
				latestOwner,
				latestApp,
			)
			corruptSingleWriterReplayReceipt(t, harness, request.GetClientRequestId())
			removeSingleWriterReplayDefinitionBlob(t, harness)

			before := readWriterFaultSnapshot(t, harness.database)
			response, err := harness.writer.Create(
				harness.actorContext,
				harness.scope,
				proto.Clone(request).(*opensplunk.CreateKnowledgeObjectRequest),
			)
			if response != nil || !errors.Is(err, control.ErrNotFound) {
				t.Fatalf("hidden latest replay = (%v, %v), want nil/ErrNotFound", response, err)
			}
			if err.Error() != control.ErrNotFound.Error() {
				t.Fatalf("hidden latest replay text = %q, want %q", err, control.ErrNotFound)
			}
			assertWriterFaultSnapshotsEqual(t, readWriterFaultSnapshot(t, harness.database), before)
		})
	}
}

func TestWriterHistoricalReplayVersionOwnerSameWidthTamperIsCorruptAndReadOnly(t *testing.T) {
	harness := newWriterFaultHarness(t)
	createRequest := writerFaultCreateRequest(
		"historical-owner-tamper",
		"historical-owner-tamper-create-0001",
	)
	created, err := harness.writer.Create(harness.actorContext, harness.scope, createRequest)
	if err != nil {
		t.Fatalf("commit Create baseline: %v", err)
	}
	definition := proto.Clone(created.GetKnowledgeObject().GetDefinition()).(*opensplunk.KnowledgeObjectDefinition)
	description := "publish a current v2 before corrupting retained v1 owner authority"
	definition.Description = &description
	updateRequest := &opensplunk.UpdateKnowledgeObjectRequest{
		KnowledgeObjectId: created.GetKnowledgeObject().GetKnowledgeObjectId(),
		ExpectedVersion:   1,
		Definition:        definition,
		UpdateMask:        &fieldmaskpb.FieldMask{Paths: []string{"description"}},
		ClientRequestId:   "historical-owner-tamper-update-0001",
	}
	if _, err := harness.writer.Update(harness.actorContext, harness.scope, updateRequest); err != nil {
		t.Fatalf("commit Update baseline: %v", err)
	}

	tamperedOwner := "x" + writerFaultOwner[1:]
	if len(tamperedOwner) != len(writerFaultOwner) || tamperedOwner == writerFaultOwner {
		t.Fatalf("invalid same-width owner fixture %q", tamperedOwner)
	}
	tamperWriterReplayHistoricalOwner(t, harness, created.GetKnowledgeObject().GetKnowledgeObjectId(), tamperedOwner)

	before := readWriterFaultSnapshot(t, harness.database)
	idCalls := harness.idCalls.Load()
	response, err := harness.writer.Create(
		harness.actorContext,
		harness.scope,
		proto.Clone(createRequest).(*opensplunk.CreateKnowledgeObjectRequest),
	)
	if response != nil || !errors.Is(err, ErrCorrupt) {
		t.Fatalf("historical owner-tampered replay = (%v, %v), want nil/ErrCorrupt", response, err)
	}
	if harness.idCalls.Load() != idCalls {
		t.Fatalf("owner-tampered replay requested a new object identity: %d -> %d", idCalls, harness.idCalls.Load())
	}
	assertWriterFaultSnapshotsEqual(t, readWriterFaultSnapshot(t, harness.database), before)
}

func stageWriterReplayQuarantineV2(t *testing.T, harness *writerFaultHarness, objectID string) {
	t.Helper()
	tx, err := harness.database.SQLDB().BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatalf("begin immutable quarantine publication: %v", err)
	}
	defer func() { _ = tx.Rollback() }()

	var appID, ownerID, objectType, name, sharingScope, state string
	var currentVersion, createdAtUnixMicro int64
	if err := tx.QueryRowContext(t.Context(), `
		SELECT app_id, owner_id, object_type, name, sharing_scope,
		       state, current_version, created_at_unix_micro
		FROM knowledge_objects
		WHERE tenant_id = ? AND knowledge_object_id = ?`,
		writerFaultTenant,
		objectID,
	).Scan(
		&appID,
		&ownerID,
		&objectType,
		&name,
		&sharingScope,
		&state,
		&currentVersion,
		&createdAtUnixMicro,
	); err != nil {
		t.Fatalf("read quarantine source registry: %v", err)
	}
	quarantinedAtUnixMicro := createdAtUnixMicro + 1
	if currentVersion != 1 || state != "draft" {
		t.Fatalf("quarantine source = version %d state %q, want v1 draft", currentVersion, state)
	}

	writerReplayExecOne(t, tx, `
		INSERT INTO knowledge_object_versions (
			tenant_id, knowledge_object_id, object_version,
			app_id, owner_id, object_type, name, sharing_scope, state,
			definition_digest, dependency_count, mutation_kind,
			quarantine_reason, created_at_unix_micro
		) VALUES (?, ?, 2, ?, ?, ?, ?, ?, 'quarantined',
		          NULL, 0, 'quarantine', 'root_corruption', ?)`,
		writerFaultTenant,
		objectID,
		appID,
		ownerID,
		objectType,
		name,
		sharingScope,
		quarantinedAtUnixMicro,
	)
	writerReplayExecOne(t, tx, `
		INSERT INTO knowledge_object_dependency_seals (
			tenant_id, knowledge_object_id, object_version, dependency_count
		) VALUES (?, ?, 2, 0)`, writerFaultTenant, objectID)
	writerReplayExecOne(t, tx, `
		INSERT INTO knowledge_object_list_projections (
			tenant_id, knowledge_object_id, object_version,
			app_id, owner_id, object_type, name, sharing_scope, state,
			description_present, description,
			index_selector_count, host_selector_count,
			source_selector_count, sourcetype_selector_count,
			selector_value_bytes, canonical_selector_bytes
		) VALUES (?, ?, 2, ?, ?, ?, ?, ?, 'quarantined',
		          0, '', 0, 0, 0, 0, 0, 0)`,
		writerFaultTenant,
		objectID,
		appID,
		ownerID,
		objectType,
		name,
		sharingScope,
	)
	writerReplayExecOne(t, tx, `
		INSERT INTO knowledge_object_list_projection_seals (
			tenant_id, knowledge_object_id, object_version,
			projection_bytes, canonical_selector_bytes
		) VALUES (?, ?, 2, 0, 0)`, writerFaultTenant, objectID)
	writerReplayExecOne(t, tx, `
		UPDATE knowledge_objects
		SET current_version = 2,
		    state = 'quarantined',
		    definition_digest = NULL,
		    updated_at_unix_micro = ?,
		    disabled_at_unix_micro = NULL,
		    quarantined_at_unix_micro = ?,
		    deleted_at_unix_micro = NULL,
		    quarantine_reason = 'root_corruption'
		WHERE tenant_id = ? AND knowledge_object_id = ?
		  AND current_version = 1 AND state = 'draft'`,
		quarantinedAtUnixMicro,
		quarantinedAtUnixMicro,
		writerFaultTenant,
		objectID,
	)
	writerReplayExecOne(t, tx, `
		UPDATE knowledge_catalog_tenants
		SET catalog_revision = catalog_revision + 1
		WHERE tenant_id = ?`, writerFaultTenant)

	if err := tx.Commit(); err != nil {
		t.Fatalf("commit immutable quarantine publication: %v", err)
	}
}

func rewriteWriterReplayQuarantineRegistry(
	t *testing.T,
	harness *writerFaultHarness,
	objectID string,
	rewrite string,
) {
	t.Helper()
	connection, err := harness.database.SQLDB().Conn(t.Context())
	if err != nil {
		t.Fatalf("open quarantine registry rewrite connection: %v", err)
	}
	defer connection.Close()
	if _, err := connection.ExecContext(t.Context(), `PRAGMA foreign_keys = OFF`); err != nil {
		t.Fatalf("disable quarantine registry foreign keys: %v", err)
	}
	defer func() {
		if _, err := connection.ExecContext(t.Context(), `PRAGMA foreign_keys = ON`); err != nil {
			t.Errorf("restore quarantine registry foreign keys: %v", err)
		}
	}()
	restore := dropWriterReplayTriggers(
		t,
		connection,
		"knowledge_object_registry_transition_is_valid",
		"knowledge_object_update_requires_sealed_list_projection",
	)
	defer restore()

	var assignment string
	switch rewrite {
	case "rollback":
		assignment = `
			current_version = 1,
			state = 'draft',
			definition_digest = (
				SELECT definition_digest FROM knowledge_object_versions
				WHERE tenant_id = knowledge_objects.tenant_id
				  AND knowledge_object_id = knowledge_objects.knowledge_object_id
				  AND object_version = 1
			),
			updated_at_unix_micro = created_at_unix_micro,
			disabled_at_unix_micro = NULL,
			quarantined_at_unix_micro = NULL,
			deleted_at_unix_micro = NULL,
			quarantine_reason = NULL`
	case "state":
		assignment = `
			state = 'draft',
			definition_digest = (
				SELECT definition_digest FROM knowledge_object_versions
				WHERE tenant_id = knowledge_objects.tenant_id
				  AND knowledge_object_id = knowledge_objects.knowledge_object_id
				  AND object_version = 1
			),
			disabled_at_unix_micro = NULL,
			quarantined_at_unix_micro = NULL,
			deleted_at_unix_micro = NULL,
			quarantine_reason = NULL`
	default:
		t.Fatalf("unknown quarantine registry rewrite %q", rewrite)
	}
	// #nosec G202 -- assignment is selected from the fixed rewrite cases above.
	result, err := connection.ExecContext(t.Context(), `
		UPDATE knowledge_objects SET `+assignment+`
		WHERE tenant_id = ? AND knowledge_object_id = ?`, writerFaultTenant, objectID)
	if err != nil {
		t.Fatalf("rewrite quarantine registry: %v", err)
	}
	writerReplayRequireOne(t, result, "rewritten quarantine registry rows")
}

func assertWriterReplayImmutableQuarantineV2(
	t *testing.T,
	harness *writerFaultHarness,
	objectID string,
	rewrite string,
) {
	t.Helper()
	var immutableCount int64
	if err := harness.database.SQLDB().QueryRowContext(t.Context(), `
		SELECT count(*)
		FROM knowledge_object_versions AS version
		JOIN knowledge_object_version_lifecycle AS lifecycle
		  ON lifecycle.tenant_id = version.tenant_id
		 AND lifecycle.knowledge_object_id = version.knowledge_object_id
		 AND lifecycle.object_version = version.object_version
		WHERE version.tenant_id = ?
		  AND version.knowledge_object_id = ?
		  AND version.object_version = 2
		  AND version.state = 'quarantined'
		  AND version.definition_digest IS NULL
		  AND version.mutation_kind = 'quarantine'
		  AND version.quarantine_reason = 'root_corruption'
		  AND lifecycle.state = 'quarantined'
		  AND lifecycle.quarantined_at_unix_micro = version.created_at_unix_micro
		  AND lifecycle.quarantine_reason = version.quarantine_reason`,
		writerFaultTenant,
		objectID,
	).Scan(&immutableCount); err != nil {
		t.Fatalf("read immutable quarantine authority: %v", err)
	}
	if immutableCount != 1 {
		t.Fatalf("immutable quarantine v2 rows = %d, want 1", immutableCount)
	}

	var currentVersion int64
	var state string
	if err := harness.database.SQLDB().QueryRowContext(t.Context(), `
		SELECT current_version, state
		FROM knowledge_objects
		WHERE tenant_id = ? AND knowledge_object_id = ?`,
		writerFaultTenant,
		objectID,
	).Scan(&currentVersion, &state); err != nil {
		t.Fatalf("read rewritten quarantine registry: %v", err)
	}
	wantVersion := int64(2)
	if rewrite == "rollback" {
		wantVersion = 1
	}
	if currentVersion != wantVersion || state != "draft" {
		t.Fatalf("rewritten registry = (v%d, %q), want (v%d, draft)", currentVersion, state, wantVersion)
	}
}

func createWriterReplayLatestHiddenApp(t *testing.T, harness *writerFaultHarness) {
	t.Helper()
	apps, err := control.NewAppCatalog(harness.database, control.AppCatalogOptions{
		CursorKey: writerFaultCursorKey,
		Clock: func() time.Time {
			return time.UnixMicro(3_000).UTC()
		},
		IDGenerator: func() (string, error) { return writerReplayLatestHiddenApp, nil },
	})
	if err != nil {
		t.Fatalf("control.NewAppCatalog(hidden latest app): %v", err)
	}
	if _, err := apps.CreateApp(
		t.Context(),
		control.AppAccessScope{TenantID: writerFaultTenant},
		control.AppDefinition{Slug: "writer-replay-latest-hidden", DisplayName: "Writer replay latest hidden"},
	); err != nil {
		t.Fatalf("CreateApp(hidden latest app): %v", err)
	}
}

func stageWriterReplayUnauthorizedLatestV2(
	t *testing.T,
	harness *writerFaultHarness,
	objectID string,
	ownerID string,
	appID string,
) {
	t.Helper()
	connection, err := harness.database.SQLDB().Conn(t.Context())
	if err != nil {
		t.Fatalf("open hidden latest version connection: %v", err)
	}
	defer connection.Close()
	restore := dropWriterReplayTriggers(
		t,
		connection,
		"knowledge_object_version_writer_semantics_are_exact",
	)
	defer restore()

	mutationKind := "update"
	if appID != writerFaultApp {
		mutationKind = "scope_change"
	}
	result, err := connection.ExecContext(t.Context(), `
		INSERT INTO knowledge_object_versions (
			tenant_id, knowledge_object_id, object_version,
			app_id, owner_id, object_type, name, sharing_scope, state,
			definition_digest, dependency_count, mutation_kind,
			quarantine_reason, created_at_unix_micro
		)
		SELECT tenant_id, knowledge_object_id, 2,
		       ?, ?, object_type, name, sharing_scope, state,
		       definition_digest, dependency_count, ?,
		       NULL, created_at_unix_micro + 1
		FROM knowledge_object_versions
		WHERE tenant_id = ? AND knowledge_object_id = ? AND object_version = 1`,
		appID,
		ownerID,
		mutationKind,
		writerFaultTenant,
		objectID,
	)
	if err != nil {
		t.Fatalf("stage hidden latest immutable version: %v", err)
	}
	writerReplayRequireOne(t, result, "staged hidden latest version rows")

	var count int64
	if err := connection.QueryRowContext(t.Context(), `
		SELECT count(*) FROM knowledge_object_versions
		WHERE tenant_id = ? AND knowledge_object_id = ?
		  AND object_version = 2 AND owner_id = ? AND app_id = ?`,
		writerFaultTenant,
		objectID,
		ownerID,
		appID,
	).Scan(&count); err != nil {
		t.Fatalf("read hidden latest immutable version: %v", err)
	}
	if count != 1 {
		t.Fatalf("hidden latest immutable rows = %d, want 1", count)
	}
}

func tamperWriterReplayHistoricalOwner(
	t *testing.T,
	harness *writerFaultHarness,
	objectID string,
	ownerID string,
) {
	t.Helper()
	connection, err := harness.database.SQLDB().Conn(t.Context())
	if err != nil {
		t.Fatalf("open historical owner tamper connection: %v", err)
	}
	defer connection.Close()
	restore := dropWriterReplayTriggers(
		t,
		connection,
		"knowledge_object_version_update_is_forbidden",
	)
	defer restore()

	result, err := connection.ExecContext(t.Context(), `
		UPDATE knowledge_object_versions
		SET owner_id = ?
		WHERE tenant_id = ? AND knowledge_object_id = ? AND object_version = 1`,
		ownerID,
		writerFaultTenant,
		objectID,
	)
	if err != nil {
		t.Fatalf("tamper historical replay owner: %v", err)
	}
	writerReplayRequireOne(t, result, "tampered historical owner rows")

	var storedOwner string
	var ownerBytes int64
	if err := connection.QueryRowContext(t.Context(), `
		SELECT owner_id, length(CAST(owner_id AS BLOB))
		FROM knowledge_object_versions
		WHERE tenant_id = ? AND knowledge_object_id = ? AND object_version = 1`,
		writerFaultTenant,
		objectID,
	).Scan(&storedOwner, &ownerBytes); err != nil {
		t.Fatalf("read historical replay owner tamper: %v", err)
	}
	if storedOwner != ownerID || ownerBytes != int64(len(writerFaultOwner)) {
		t.Fatalf("historical owner tamper = (%q, %d bytes), want (%q, %d bytes)", storedOwner, ownerBytes, ownerID, len(writerFaultOwner))
	}
}

func corruptSingleWriterReplayReceipt(
	t *testing.T,
	harness *writerFaultHarness,
	requestID string,
) {
	t.Helper()
	connection, err := harness.database.SQLDB().Conn(t.Context())
	if err != nil {
		t.Fatalf("open single receipt tamper connection: %v", err)
	}
	defer connection.Close()
	restore := dropWriterReplayTriggers(
		t,
		connection,
		"knowledge_mutation_idempotency_update_is_forbidden",
	)
	defer restore()
	if _, err := connection.ExecContext(t.Context(), `PRAGMA ignore_check_constraints = ON`); err != nil {
		t.Fatalf("enable receipt check corruption: %v", err)
	}
	defer func() {
		if _, err := connection.ExecContext(t.Context(), `PRAGMA ignore_check_constraints = OFF`); err != nil {
			t.Errorf("restore receipt checks: %v", err)
		}
	}()

	result, err := connection.ExecContext(t.Context(), `
		UPDATE knowledge_mutation_idempotency
		SET outcome_proto = zeroblob(1025)
		WHERE tenant_id = ? AND route = 'objects.create' AND client_request_id = ?`,
		writerFaultTenant,
		requestID,
	)
	if err != nil {
		t.Fatalf("corrupt single replay receipt: %v", err)
	}
	writerReplayRequireOne(t, result, "corrupted replay receipt rows")
}

func removeSingleWriterReplayDefinitionBlob(t *testing.T, harness *writerFaultHarness) {
	t.Helper()
	connection, err := harness.database.SQLDB().Conn(t.Context())
	if err != nil {
		t.Fatalf("open single replay blob tamper connection: %v", err)
	}
	defer connection.Close()
	if _, err := connection.ExecContext(t.Context(), `PRAGMA foreign_keys = OFF`); err != nil {
		t.Fatalf("disable single replay blob foreign keys: %v", err)
	}
	defer func() {
		if _, err := connection.ExecContext(t.Context(), `PRAGMA foreign_keys = ON`); err != nil {
			t.Errorf("restore single replay blob foreign keys: %v", err)
		}
	}()
	restore := dropWriterReplayTriggers(
		t,
		connection,
		"knowledge_definition_blob_delete_is_forbidden",
	)
	defer restore()

	result, err := connection.ExecContext(t.Context(), `
		DELETE FROM knowledge_definition_blobs WHERE tenant_id = ?`, writerFaultTenant)
	if err != nil {
		t.Fatalf("remove single replay definition blob: %v", err)
	}
	affected, err := result.RowsAffected()
	if err != nil || affected < 1 {
		t.Fatalf("removed replay definition blob rows = %d, %v; want at least 1", affected, err)
	}
}

func dropWriterReplayTriggers(
	t *testing.T,
	connection *sql.Conn,
	names ...string,
) func() {
	t.Helper()
	triggerSQL := make([]string, len(names))
	for index, name := range names {
		if name == "" || strings.ContainsAny(name, "\"'`; ") {
			t.Fatalf("invalid test trigger name %q", name)
		}
		if err := connection.QueryRowContext(t.Context(), `
			SELECT sql FROM sqlite_schema
			WHERE type = 'trigger' AND name = ?`, name).Scan(&triggerSQL[index]); err != nil {
			t.Fatalf("read trigger %s: %v", name, err)
		}
		if _, err := connection.ExecContext(t.Context(), `DROP TRIGGER `+name); err != nil {
			t.Fatalf("drop trigger %s: %v", name, err)
		}
	}
	return func() {
		for index, v := range slices.Backward(triggerSQL) {
			if _, err := connection.ExecContext(t.Context(), v); err != nil {
				t.Errorf("restore trigger %s: %v", names[index], err)
			}
		}
	}
}

func writerReplayExecOne(t *testing.T, tx *sql.Tx, query string, args ...any) {
	t.Helper()
	result, err := tx.ExecContext(t.Context(), query, args...)
	if err != nil {
		t.Fatalf("execute replay fixture statement: %v", err)
	}
	writerReplayRequireOne(t, result, "replay fixture affected rows")
}

func writerReplayRequireOne(t *testing.T, result sql.Result, label string) {
	t.Helper()
	affected, err := result.RowsAffected()
	if err != nil || affected != 1 {
		t.Fatalf("%s = %d, %v; want 1", label, affected, err)
	}
}
