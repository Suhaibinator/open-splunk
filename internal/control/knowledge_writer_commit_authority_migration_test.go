package control

import (
	"bytes"
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/migrations"
)

const knowledgeWriterRequestID = "request-00000001"

func TestKnowledgeWriterCommitAuthorityMigrationPinsReplaySchema(t *testing.T) {
	t.Parallel()

	raw := openKnowledgeMigrationTestDB(t, "knowledge-writer-schema.sqlite")
	if err := ApplyMigrations(context.Background(), raw, migrations.SQLite()); err != nil {
		t.Fatalf("apply migrations: %v", err)
	}

	assertIntegerQuery(t, raw, 36, `SELECT count(*) FROM schema_migrations`)
	assertIntegerQuery(t, raw, 1, `
		SELECT count(*) FROM pragma_table_list
		WHERE name = 'knowledge_mutation_idempotency'
		  AND wr = 1 AND strict = 1`)
	assertIntegerQuery(t, raw, 1, `
		SELECT count(*) FROM pragma_table_list
		WHERE name = 'knowledge_mutation_commit_authorities'
		  AND wr = 1 AND strict = 1`)
	for column, primaryKeyOrdinal := range map[string]int{
		"tenant_id":         1,
		"actor_kind":        2,
		"actor_id":          3,
		"route":             4,
		"client_request_id": 5,
	} {
		assertIntegerQuery(t, raw, primaryKeyOrdinal, `
			SELECT pk FROM pragma_table_info('knowledge_mutation_idempotency')
			WHERE name = ?`, column)
	}
	for column, primaryKeyOrdinal := range map[string]int{
		"tenant_id":        1,
		"catalog_revision": 2,
	} {
		assertIntegerQuery(t, raw, primaryKeyOrdinal, `
			SELECT pk FROM pragma_table_info('knowledge_mutation_commit_authorities')
			WHERE name = ?`, column)
	}
	assertKnowledgeWriterCommitUniqueAuthority(t, raw)
	assertKnowledgeWriterReceiptCommitForeignKey(t, raw)
	for _, column := range []string{
		"request_digest_format_version",
		"outcome_format_version",
		"committed_catalog_state_token",
		"successful_audit_sequence",
		"recovery_audit_sequence",
		"retention_anchor_unix_micro",
	} {
		assertIntegerQuery(t, raw, 1, `
			SELECT count(*) FROM pragma_table_info('knowledge_mutation_idempotency')
			WHERE name = ?`, column)
	}
	assertIntegerQuery(t, raw, 1, `
		SELECT count(*) FROM sqlite_schema
		WHERE type = 'trigger'
		  AND name = 'knowledge_object_version_writer_semantics_are_exact'`)
	assertIntegerQuery(t, raw, 2, `
		SELECT count(*) FROM sqlite_schema
		WHERE type = 'trigger'
		  AND name IN (
		      'knowledge_mutation_idempotency_matches_commit_authority',
		      'knowledge_mutation_idempotency_matches_audit_authority'
		  )`)
	assertIntegerQuery(t, raw, 4, `
		SELECT count(*) FROM sqlite_schema
		WHERE type = 'trigger'
		  AND name IN (
		      'knowledge_mutation_commit_authority_collision_is_forbidden',
		      'knowledge_mutation_commit_authority_is_exact',
		      'knowledge_mutation_commit_authority_update_is_forbidden',
		      'knowledge_mutation_commit_authority_delete_is_forbidden'
		  )`)
	assertNoForeignKeyViolations(t, raw)
}

func TestKnowledgeWriterCommitAuthorityMigrationRejectsLegacyReplayStateAtomically(t *testing.T) {
	t.Parallel()

	t.Run("physical row", func(t *testing.T) {
		t.Parallel()
		raw := openKnowledgeMigrationTestDB(t, "knowledge-writer-reject-row.sqlite")
		if err := ApplyMigrations(context.Background(), raw, migrationsBefore(t, "0030_")); err != nil {
			t.Fatalf("apply through migration 0029: %v", err)
		}
		seedKnowledgeStatePrerequisites(t, raw)
		insertKnowledgeStateVersion(t, raw, stateVersionFixture{
			objectID: "ko-legacy-replay", version: 1, state: "active",
			mutation: "create", timestamp: 10,
		})
		if _, err := raw.ExecContext(t.Context(), `UPDATE knowledge_catalog_tenants
			SET catalog_revision = catalog_revision + 1
			WHERE tenant_id = 'tenant-a'`); err != nil {
			t.Fatalf("advance legacy revision: %v", err)
		}
		if _, err := raw.ExecContext(t.Context(), `
			INSERT INTO knowledge_mutation_idempotency (
				tenant_id, actor_id, route, client_request_id, mutation_kind,
				request_digest, outcome_proto, committed_catalog_revision,
				knowledge_object_id, object_version,
				created_at_unix_micro, retain_until_unix_micro
			) VALUES (
				'tenant-a', 'actor-a', 'objects.create', ?, 'create',
				zeroblob(32), X'01', 1, 'ko-legacy-replay', 1,
				10, 604800000010
			)`, knowledgeWriterRequestID); err != nil {
			t.Fatalf("insert legacy idempotency row: %v", err)
		}

		err := ApplyMigrations(context.Background(), raw, migrations.SQLite())
		if err == nil || !strings.Contains(err.Error(), "CHECK constraint failed: invalid") {
			t.Fatalf("legacy replay upgrade error = %v", err)
		}
		assertIntegerQuery(t, raw, 29, `SELECT count(*) FROM schema_migrations`)
		assertIntegerQuery(t, raw, 1, `SELECT count(*) FROM knowledge_mutation_idempotency`)
		assertIntegerQuery(t, raw, 1, `
			SELECT idempotency_count FROM knowledge_catalog_tenants
			WHERE tenant_id = 'tenant-a'`)
		assertIntegerQuery(t, raw, 0, `
			SELECT count(*) FROM pragma_table_info('knowledge_mutation_idempotency')
			WHERE name = 'actor_kind'`)
		assertIntegerQuery(t, raw, 0, `
			SELECT count(*) FROM sqlite_schema
			WHERE name LIKE 'knowledge_writer_0030_%_upgrade_guard'`)
		assertNoForeignKeyViolations(t, raw)
	})

	t.Run("nonzero ledger without row", func(t *testing.T) {
		t.Parallel()
		raw := openKnowledgeMigrationTestDB(t, "knowledge-writer-reject-ledger.sqlite")
		if err := ApplyMigrations(context.Background(), raw, migrationsBefore(t, "0030_")); err != nil {
			t.Fatalf("apply through migration 0029: %v", err)
		}
		if _, err := raw.ExecContext(t.Context(), `
			INSERT INTO knowledge_catalog_tenants (tenant_id) VALUES ('tenant-a');
			UPDATE knowledge_catalog_tenants SET idempotency_count = 1
			WHERE tenant_id = 'tenant-a'`); err != nil {
			t.Fatalf("poison legacy ledger: %v", err)
		}

		err := ApplyMigrations(context.Background(), raw, migrations.SQLite())
		if err == nil || !strings.Contains(err.Error(), "CHECK constraint failed: invalid") {
			t.Fatalf("legacy ledger upgrade error = %v", err)
		}
		assertIntegerQuery(t, raw, 29, `SELECT count(*) FROM schema_migrations`)
		assertIntegerQuery(t, raw, 0, `SELECT count(*) FROM knowledge_mutation_idempotency`)
		assertIntegerQuery(t, raw, 1, `
			SELECT idempotency_count FROM knowledge_catalog_tenants
			WHERE tenant_id = 'tenant-a'`)
	})
}

func TestKnowledgeWriterCommitAuthorityMigrationRejectsMislabelledHistoryAtomically(t *testing.T) {
	t.Parallel()

	raw := openKnowledgeMigrationTestDB(t, "knowledge-writer-reject-history.sqlite")
	if err := ApplyMigrations(context.Background(), raw, migrationsBefore(t, "0030_")); err != nil {
		t.Fatalf("apply through migration 0029: %v", err)
	}
	seedKnowledgeStatePrerequisites(t, raw)
	insertKnowledgeStateVersion(t, raw, stateVersionFixture{
		objectID: "ko-mislabelled", version: 1, state: "draft",
		mutation: "create", timestamp: 10,
	})
	// Migration 0029 admitted the lifecycle shape but did not prove that a
	// scope_change actually changed app or sharing scope.
	insertKnowledgeStateVersion(t, raw, stateVersionFixture{
		objectID: "ko-mislabelled", version: 2, state: "draft",
		mutation: "scope_change", timestamp: 20,
	})

	err := ApplyMigrations(context.Background(), raw, migrations.SQLite())
	if err == nil || !strings.Contains(err.Error(), "CHECK constraint failed: invalid") {
		t.Fatalf("mislabelled history upgrade error = %v", err)
	}
	assertIntegerQuery(t, raw, 29, `SELECT count(*) FROM schema_migrations`)
	assertIntegerQuery(t, raw, 0, `
		SELECT count(*) FROM sqlite_schema
		WHERE name = 'knowledge_object_version_writer_semantics_are_exact'`)
	assertIntegerQuery(t, raw, 2, `
		SELECT current_version FROM knowledge_objects
		WHERE tenant_id = 'tenant-a' AND knowledge_object_id = 'ko-mislabelled'`)
	assertNoForeignKeyViolations(t, raw)
}

func TestKnowledgeWriterCommitAuthorityMigrationRejectsRetainedWriterSemanticDrift(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		scope           string
		state           string
		mutation        string
		dependencyCount int
	}{
		{name: "no-op update", scope: "private", state: "draft", mutation: "update"},
		{name: "scope change reuses definition", scope: "app", state: "draft", mutation: "scope_change"},
		{name: "enable changes dependencies", scope: "private", state: "active", mutation: "enable", dependencyCount: 1},
		{name: "disable changes dependencies", scope: "private", state: "disabled", mutation: "disable", dependencyCount: 1},
		{name: "delete changes dependencies", scope: "private", state: "deleted", mutation: "delete", dependencyCount: 1},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			raw := openKnowledgeMigrationTestDB(t, "knowledge-writer-retained-drift.sqlite")
			if err := ApplyMigrations(context.Background(), raw, migrationsBefore(t, "0030_")); err != nil {
				t.Fatalf("apply through migration 0029: %v", err)
			}
			seedKnowledgeStatePrerequisites(t, raw)
			insertKnowledgeStateVersion(t, raw, stateVersionFixture{
				objectID: "ko-retained-drift", version: 1, state: "draft",
				mutation: "create", timestamp: 10,
			})
			if _, err := raw.ExecContext(t.Context(), `INSERT INTO knowledge_object_versions (
				tenant_id, knowledge_object_id, object_version,
				app_id, owner_id, object_type, name, sharing_scope, state,
				definition_digest, dependency_count, mutation_kind,
				quarantine_reason, created_at_unix_micro
			) VALUES (
				'tenant-a', 'ko-retained-drift', 2, ?, 'owner-a',
				'field_extraction', 'ko-retained-drift', ?, ?,
				zeroblob(32), ?, ?, NULL, 20
			)`, knowledgeMigrationTestAppID, test.scope, test.state,
				test.dependencyCount, test.mutation); err != nil {
				t.Fatalf("seed retained writer drift: %v", err)
			}

			err := ApplyMigrations(context.Background(), raw, migrations.SQLite())
			if err == nil || !strings.Contains(err.Error(), "CHECK constraint failed: invalid") {
				t.Fatalf("retained drift upgrade error = %v", err)
			}
			assertIntegerQuery(t, raw, 29, `SELECT count(*) FROM schema_migrations`)
			assertIntegerQuery(t, raw, 2, `SELECT count(*) FROM knowledge_object_versions
				WHERE tenant_id = 'tenant-a' AND knowledge_object_id = 'ko-retained-drift'`)
			assertIntegerQuery(t, raw, 0, `SELECT count(*) FROM sqlite_schema
				WHERE name = 'knowledge_object_version_writer_semantics_are_exact'`)
		})
	}
}

func TestKnowledgeWriterCommitAuthorityMigrationRejectsStateOnlyDependencyEdgeDrift(t *testing.T) {
	t.Parallel()

	raw := openKnowledgeMigrationTestDB(t, "knowledge-writer-retained-dependency-drift.sqlite")
	if err := ApplyMigrations(context.Background(), raw, migrationsBefore(t, "0030_")); err != nil {
		t.Fatalf("apply through migration 0029: %v", err)
	}
	seedKnowledgeStatePrerequisites(t, raw)
	insertKnowledgeStateVersion(t, raw, stateVersionFixture{
		objectID: "ko-dependency-target-a", version: 1, state: "active",
		mutation: "create", timestamp: 1,
	})
	insertKnowledgeStateVersion(t, raw, stateVersionFixture{
		objectID: "ko-dependency-target-b", version: 1, state: "active",
		mutation: "create", timestamp: 2,
	})
	insertKnowledgeWriterDependencyVersion(
		t, raw, "ko-retained-dependency-drift", 1, "draft", "create",
		"ko-dependency-target-a", 10,
	)
	insertKnowledgeWriterDependencyVersion(
		t, raw, "ko-retained-dependency-drift", 2, "disabled", "disable",
		"ko-dependency-target-b", 20,
	)

	err := ApplyMigrations(context.Background(), raw, migrations.SQLite())
	if err == nil || !strings.Contains(err.Error(), "CHECK constraint failed: invalid") {
		t.Fatalf("state-only dependency drift upgrade error = %v", err)
	}
	assertIntegerQuery(t, raw, 29, `SELECT count(*) FROM schema_migrations`)
	assertIntegerQuery(t, raw, 1, `
		SELECT count(*)
		FROM knowledge_object_dependencies
		WHERE tenant_id = 'tenant-a'
		  AND source_object_id = 'ko-retained-dependency-drift'
		  AND source_object_version = 1
		  AND target_object_id = 'ko-dependency-target-a'`)
	assertIntegerQuery(t, raw, 1, `
		SELECT count(*)
		FROM knowledge_object_dependencies
		WHERE tenant_id = 'tenant-a'
		  AND source_object_id = 'ko-retained-dependency-drift'
		  AND source_object_version = 2
		  AND target_object_id = 'ko-dependency-target-b'`)
	assertNoForeignKeyViolations(t, raw)
}

func TestKnowledgeWriterStateOnlyDependencySealRequiresExactPriorEdges(t *testing.T) {
	t.Parallel()

	raw := openKnowledgeMigrationTestDB(t, "knowledge-writer-state-only-dependencies.sqlite")
	if err := ApplyMigrations(context.Background(), raw, migrations.SQLite()); err != nil {
		t.Fatalf("apply migrations: %v", err)
	}
	seedKnowledgeStatePrerequisites(t, raw)
	insertKnowledgeStateVersion(t, raw, stateVersionFixture{
		objectID: "ko-dependency-target-a", version: 1, state: "active",
		mutation: "create", timestamp: 1,
	})
	insertKnowledgeStateVersion(t, raw, stateVersionFixture{
		objectID: "ko-dependency-target-b", version: 1, state: "active",
		mutation: "create", timestamp: 2,
	})
	insertKnowledgeWriterDependencyVersion(
		t, raw, "ko-state-only-dependencies", 1, "draft", "create",
		"ko-dependency-target-a", 10,
	)

	tx, err := raw.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatalf("begin mismatched state-only version: %v", err)
	}
	if _, err := tx.ExecContext(t.Context(), `
		INSERT INTO knowledge_object_versions (
			tenant_id, knowledge_object_id, object_version,
			app_id, owner_id, object_type, name, sharing_scope, state,
			definition_digest, dependency_count, mutation_kind,
			quarantine_reason, created_at_unix_micro
		) VALUES (
			'tenant-a', 'ko-state-only-dependencies', 2, ?, 'owner-a',
			'field_extraction', 'ko-state-only-dependencies', 'private',
			'disabled', zeroblob(32), 1, 'disable', NULL, 20
		);
		INSERT INTO knowledge_object_dependencies (
			tenant_id, source_object_id, source_object_version, ordinal,
			target_kind, target_object_id, target_object_version, dependency_role
		) VALUES (
			'tenant-a', 'ko-state-only-dependencies', 2, 0,
			'object', 'ko-dependency-target-b', 1, 'field_input'
		)`, knowledgeMigrationTestAppID); err != nil {
		_ = tx.Rollback()
		t.Fatalf("stage mismatched state-only dependency: %v", err)
	}
	if _, err := tx.ExecContext(t.Context(), `
		INSERT INTO knowledge_object_dependency_seals (
			tenant_id, knowledge_object_id, object_version, dependency_count
		) VALUES ('tenant-a', 'ko-state-only-dependencies', 2, 1)`); err == nil ||
		!strings.Contains(err.Error(), "state-only dependencies are invalid") {
		_ = tx.Rollback()
		t.Fatalf("mismatched state-only dependency seal error = %v", err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatalf("rollback mismatched state-only dependency: %v", err)
	}

	insertKnowledgeWriterDependencyVersion(
		t, raw, "ko-state-only-dependencies", 2, "disabled", "disable",
		"ko-dependency-target-a", 20,
	)
	assertIntegerQuery(t, raw, 1, `
		SELECT count(*)
		FROM knowledge_object_dependency_seals
		WHERE tenant_id = 'tenant-a'
		  AND knowledge_object_id = 'ko-state-only-dependencies'
		  AND object_version = 2
		  AND dependency_count = 1`)
	assertNoForeignKeyViolations(t, raw)
}

func TestKnowledgeWriterVersionSemanticTriggerRejectsAuthorityDrift(t *testing.T) {
	t.Parallel()

	raw := openKnowledgeMigrationTestDB(t, "knowledge-writer-version-trigger.sqlite")
	if err := ApplyMigrations(context.Background(), raw, migrations.SQLite()); err != nil {
		t.Fatalf("apply migrations: %v", err)
	}
	seedKnowledgeStatePrerequisites(t, raw)
	insertKnowledgeStateVersion(t, raw, stateVersionFixture{
		objectID: "ko-writer-semantics", version: 1, state: "draft",
		mutation: "create", timestamp: 10,
	})
	distinctDigest := bytes.Repeat([]byte{0x5a}, 32)
	if _, err := raw.ExecContext(t.Context(), `INSERT INTO knowledge_definition_blobs (
		tenant_id, definition_digest, definition_proto,
		definition_bytes, created_at_unix_micro
	) VALUES ('tenant-a', ?, X'02', 1, 2)`, distinctDigest); err != nil {
		t.Fatalf("insert distinct writer definition: %v", err)
	}

	invalid := []struct {
		name       string
		appID      string
		ownerID    string
		objectType string
		objectName string
		scope      string
		state      string
		mutation   string
		dependency int
	}{
		{name: "owner changes", appID: knowledgeMigrationTestAppID, ownerID: "owner-b", objectType: "field_extraction", objectName: "ko-writer-semantics", scope: "private", state: "draft", mutation: "update"},
		{name: "object type changes", appID: knowledgeMigrationTestAppID, ownerID: "owner-a", objectType: "field_alias", objectName: "ko-writer-semantics", scope: "private", state: "draft", mutation: "update"},
		{name: "update changes scope", appID: knowledgeMigrationTestAppID, ownerID: "owner-a", objectType: "field_extraction", objectName: "ko-writer-semantics", scope: "app", state: "draft", mutation: "update"},
		{name: "scope change changes nothing", appID: knowledgeMigrationTestAppID, ownerID: "owner-a", objectType: "field_extraction", objectName: "ko-writer-semantics", scope: "private", state: "draft", mutation: "scope_change"},
		{name: "no-op update", appID: knowledgeMigrationTestAppID, ownerID: "owner-a", objectType: "field_extraction", objectName: "ko-writer-semantics", scope: "private", state: "draft", mutation: "update"},
		{name: "scope change reuses definition", appID: knowledgeMigrationTestAppID, ownerID: "owner-a", objectType: "field_extraction", objectName: "ko-writer-semantics", scope: "app", state: "draft", mutation: "scope_change"},
		{name: "state only changes name", appID: knowledgeMigrationTestAppID, ownerID: "owner-a", objectType: "field_extraction", objectName: "changed-name", scope: "private", state: "disabled", mutation: "disable"},
		{name: "disable changes dependencies", appID: knowledgeMigrationTestAppID, ownerID: "owner-a", objectType: "field_extraction", objectName: "ko-writer-semantics", scope: "private", state: "disabled", mutation: "disable", dependency: 1},
		{name: "delete changes dependencies", appID: knowledgeMigrationTestAppID, ownerID: "owner-a", objectType: "field_extraction", objectName: "ko-writer-semantics", scope: "private", state: "deleted", mutation: "delete", dependency: 1},
	}
	for _, test := range invalid {
		t.Run(test.name, func(t *testing.T) {
			assertSQLFailsContaining(t, raw, "writer semantics are invalid", `
				INSERT INTO knowledge_object_versions (
					tenant_id, knowledge_object_id, object_version,
					app_id, owner_id, object_type, name, sharing_scope, state,
					definition_digest, dependency_count, mutation_kind,
					quarantine_reason, created_at_unix_micro
				) VALUES (
					'tenant-a', 'ko-writer-semantics', 2,
					?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, 20
				)`, test.appID, test.ownerID, test.objectType, test.objectName,
				test.scope, test.state, make([]byte, 32), test.dependency, test.mutation)
		})
	}

	for _, valid := range []struct {
		name       string
		scope      string
		state      string
		mutation   string
		dependency int
	}{
		{name: "ordinary update", scope: "private", state: "draft", mutation: "update"},
		{name: "app scope change", scope: "app", state: "draft", mutation: "scope_change"},
		{name: "enable with rederived dependencies", scope: "private", state: "active", mutation: "enable", dependency: 1},
		{name: "state only disable", scope: "private", state: "disabled", mutation: "disable"},
	} {
		t.Run("accepts "+valid.name, func(t *testing.T) {
			tx, err := raw.BeginTx(t.Context(), nil)
			if err != nil {
				t.Fatalf("begin accepted transition: %v", err)
			}
			digest := distinctDigest
			if valid.mutation == "disable" || valid.mutation == "enable" {
				digest = make([]byte, 32)
			}
			if _, err := tx.ExecContext(t.Context(), `
				INSERT INTO knowledge_object_versions (
					tenant_id, knowledge_object_id, object_version,
					app_id, owner_id, object_type, name, sharing_scope, state,
					definition_digest, dependency_count, mutation_kind,
					quarantine_reason, created_at_unix_micro
				) VALUES (
					'tenant-a', 'ko-writer-semantics', 2, ?, 'owner-a',
					'field_extraction', 'ko-writer-semantics', ?, ?,
					?, ?, ?, NULL, 20
				)`, knowledgeMigrationTestAppID, valid.scope, valid.state, digest, valid.dependency, valid.mutation); err != nil {
				_ = tx.Rollback()
				t.Fatalf("accepted transition insert: %v", err)
			}
			if err := tx.Rollback(); err != nil {
				t.Fatalf("rollback accepted transition: %v", err)
			}
		})
	}
	assertIntegerQuery(t, raw, 1, `
		SELECT count(*) FROM knowledge_object_versions
		WHERE tenant_id = 'tenant-a' AND knowledge_object_id = 'ko-writer-semantics'`)
	assertNoForeignKeyViolations(t, raw)
}

func TestKnowledgeWriterIdempotencyRequiresExactCommittedSuccessAuthorities(t *testing.T) {
	t.Parallel()

	raw := openKnowledgeMigrationTestDB(t, "knowledge-writer-success-authority.sqlite")
	if err := ApplyMigrations(context.Background(), raw, migrations.SQLite()); err != nil {
		t.Fatalf("apply migrations: %v", err)
	}
	seedKnowledgeStatePrerequisites(t, raw)
	createdAt := time.Now().UTC().UnixMicro()
	insertKnowledgeStateVersion(t, raw, stateVersionFixture{
		objectID: "ko-success-authority", version: 1, state: "active",
		mutation: "create", timestamp: createdAt,
	})
	if _, err := raw.ExecContext(t.Context(), `INSERT INTO audit_tenant_state (
		tenant_id, next_sequence, event_count
	) VALUES ('tenant-a', 1, 0)`); err != nil {
		t.Fatalf("seed audit tenant: %v", err)
	}

	tx, err := raw.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatalf("begin exact success publication: %v", err)
	}
	rollback := func(format string, args ...any) {
		_ = tx.Rollback()
		t.Fatalf(format, args...)
	}
	if _, err := tx.ExecContext(t.Context(), `UPDATE knowledge_catalog_tenants
		SET catalog_revision = catalog_revision + 1
		WHERE tenant_id = 'tenant-a'`); err != nil {
		rollback("advance catalog revision: %v", err)
	}
	var token []byte
	if err := tx.QueryRowContext(t.Context(), `SELECT state_token FROM knowledge_catalog_revision_heads
		WHERE tenant_id = 'tenant-a' AND catalog_revision = 1`).Scan(&token); err != nil {
		rollback("read committed token: %v", err)
	}
	if len(token) != 32 {
		rollback("committed token length = %d", len(token))
	}
	if _, err := tx.ExecContext(t.Context(), `
		INSERT INTO audit_events (
			tenant_id, sequence, occurred_at_unix_micro,
			actor_kind, actor_id, actor_role, action,
			target_kind, target_id, target_version,
			app_id, object_type, sharing_scope
		) VALUES (
			'tenant-a', 1, ?, 'browser', 'actor-a', 'administrator',
			'knowledge.object.create', 'knowledge_object',
			'ko-success-authority', 1, ?, 'field_extraction', 'private'
		)`, createdAt, knowledgeMigrationTestAppID); err != nil {
		rollback("insert successful audit: %v", err)
	}
	retainUntil := createdAt + int64(7*24*time.Hour/time.Microsecond)
	if _, err := insertKnowledgeWriterCommitAuthority(
		tx, "create", 1, token, "ko-success-authority", 1,
		createdAt, createdAt, retainUntil,
		sql.NullInt64{Int64: 1, Valid: true}, sql.NullInt64{},
	); err != nil {
		rollback("insert immutable commit authority: %v", err)
	}
	if _, err := insertKnowledgeWriterIdempotency(
		tx, "browser", "actor-a", knowledgeWriterRequestID,
		"objects.create", "create", 1, token,
		"ko-success-authority", 1, sql.NullInt64{Int64: 1, Valid: true},
		sql.NullInt64{}, createdAt, retainUntil, []byte{1},
	); err != nil {
		rollback("insert exact idempotency receipt: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit exact success publication: %v", err)
	}

	assertIntegerQuery(t, raw, 1, `
		SELECT idempotency_count FROM knowledge_catalog_tenants
		WHERE tenant_id = 'tenant-a'`)
	assertIntegerQuery(t, raw, 1, `
		SELECT count(*) FROM knowledge_mutation_idempotency
		WHERE tenant_id = 'tenant-a' AND actor_kind = 'browser'
		  AND actor_id = 'actor-a' AND committed_catalog_revision = 1
		  AND committed_catalog_state_token = ?
		  AND successful_audit_sequence = 1
		  AND recovery_audit_sequence IS NULL`, token)
	assertIntegerQuery(t, raw, 1, `
		SELECT count(*) FROM knowledge_mutation_commit_authorities
		WHERE tenant_id = 'tenant-a' AND catalog_revision = 1
		  AND catalog_state_token = ? AND mutation_kind = 'create'
		  AND knowledge_object_id = 'ko-success-authority'
		  AND object_version = 1 AND occurred_at_unix_micro = ?
		  AND successful_audit_sequence = 1
		  AND recovery_audit_sequence IS NULL`, token, createdAt)
	assertSQLFailsContaining(t, raw, "already exists", `
		INSERT OR REPLACE INTO knowledge_mutation_commit_authorities
		SELECT * FROM knowledge_mutation_commit_authorities
		WHERE tenant_id = 'tenant-a' AND catalog_revision = 1`)
	assertSQLFailsContaining(t, raw, "immutable", `
		UPDATE knowledge_mutation_commit_authorities
		SET catalog_state_token = zeroblob(32)
		WHERE tenant_id = 'tenant-a' AND catalog_revision = 1`)
	assertSQLFailsContaining(t, raw, "retained", `
		DELETE FROM knowledge_mutation_commit_authorities
		WHERE tenant_id = 'tenant-a' AND catalog_revision = 1`)

	assertSQLFailsContaining(t, raw, "identity already exists", `
		INSERT OR REPLACE INTO knowledge_mutation_idempotency
		SELECT * FROM knowledge_mutation_idempotency
		WHERE tenant_id = 'tenant-a' AND actor_kind = 'browser'
		  AND actor_id = 'actor-a' AND route = 'objects.create'
		  AND client_request_id = ?`, knowledgeWriterRequestID)
	assertSQLFailsContaining(t, raw, "retention fence is active", `
		DELETE FROM knowledge_mutation_idempotency
		WHERE tenant_id = 'tenant-a' AND actor_kind = 'browser'
		  AND actor_id = 'actor-a' AND route = 'objects.create'
		  AND client_request_id = ?`, knowledgeWriterRequestID)
	assertSQLFailsContaining(t, raw, "immutable", `
		UPDATE knowledge_mutation_idempotency SET outcome_proto = X'02'
		WHERE tenant_id = 'tenant-a' AND actor_kind = 'browser'
		  AND actor_id = 'actor-a' AND route = 'objects.create'
		  AND client_request_id = ?`, knowledgeWriterRequestID)

	for _, probe := range []struct {
		name       string
		assignment string
		arguments  []any
	}{
		{
			name:       "actor identity",
			assignment: "actor_id = ?",
			arguments:  []any{"actor-b"},
		},
		{
			name:       "client request identity",
			assignment: "client_request_id = ?",
			arguments:  []any{"request-00000002"},
		},
		{
			name:       "request digest",
			assignment: "request_digest = ?",
			arguments:  []any{bytes.Repeat([]byte{0x5a}, 32)},
		},
		{
			name:       "route and mutation kind",
			assignment: "route = 'objects.delete', mutation_kind = 'delete'",
		},
	} {
		t.Run("composite foreign key rejects "+probe.name, func(t *testing.T) {
			tx, err := raw.BeginTx(t.Context(), nil)
			if err != nil {
				t.Fatalf("begin composite foreign-key probe: %v", err)
			}
			defer func() { _ = tx.Rollback() }()
			if _, err := tx.ExecContext(t.Context(), `
				DROP TRIGGER knowledge_mutation_idempotency_update_is_forbidden;
				DROP TRIGGER knowledge_mutation_idempotency_matches_commit_authority`); err != nil {
				t.Fatalf("isolate composite foreign-key probe: %v", err)
			}
			arguments := append(probe.arguments,
				"tenant-a", "actor-a", "objects.create", knowledgeWriterRequestID,
			)
			// #nosec G202 -- assignment is selected from the fixed foreign-key probe table above.
			_, err = tx.ExecContext(t.Context(), `UPDATE knowledge_mutation_idempotency
				SET `+probe.assignment+`
				WHERE tenant_id = ? AND actor_id = ? AND route = ?
				  AND client_request_id = ?`, arguments...)
			if err == nil || !strings.Contains(err.Error(), "FOREIGN KEY constraint failed") {
				t.Fatalf("composite foreign-key %s probe error = %v", probe.name, err)
			}
		})
	}

	invalid := []struct {
		name            string
		actorKind       string
		requestID       string
		revision        int64
		token           []byte
		successSequence sql.NullInt64
		outcome         []byte
		createdAtDelta  int64
		want            string
	}{
		{name: "wrong token", actorKind: "browser", requestID: "request-00000002", revision: 1, token: make([]byte, 32), successSequence: sql.NullInt64{Int64: 1, Valid: true}, outcome: []byte{1}, want: "commit authority is invalid"},
		{name: "wrong revision", actorKind: "browser", requestID: "request-00000003", revision: 2, token: token, successSequence: sql.NullInt64{Int64: 1, Valid: true}, outcome: []byte{1}, want: "commit authority is invalid"},
		{name: "wrong actor kind", actorKind: "system", requestID: "request-00000004", revision: 1, token: token, successSequence: sql.NullInt64{Int64: 1, Valid: true}, outcome: []byte{1}, want: "audit authority is invalid"},
		{name: "wrong audit sequence", actorKind: "browser", requestID: "request-00000005", revision: 1, token: token, successSequence: sql.NullInt64{Int64: 2, Valid: true}, outcome: []byte{1}, want: "audit authority is invalid"},
		{name: "oversized outcome", actorKind: "browser", requestID: "request-00000006", revision: 1, token: token, successSequence: sql.NullInt64{Int64: 1, Valid: true}, outcome: bytes.Repeat([]byte{1}, 1025), want: "CHECK constraint failed"},
		{name: "wrong receipt timestamp", actorKind: "browser", requestID: "request-00000007", revision: 1, token: token, successSequence: sql.NullInt64{Int64: 1, Valid: true}, outcome: []byte{1}, createdAtDelta: 1, want: "commit authority is invalid"},
	}
	for _, test := range invalid {
		t.Run(test.name, func(t *testing.T) {
			executor := knowledgeWriterSQLExecutor(raw)
			var outcomeProbe *sql.Tx
			if test.name == "oversized outcome" {
				var err error
				outcomeProbe, err = raw.BeginTx(t.Context(), nil)
				if err != nil {
					t.Fatalf("begin oversized outcome probe: %v", err)
				}
				defer func() { _ = outcomeProbe.Rollback() }()
				if _, err := outcomeProbe.ExecContext(t.Context(), `
					DROP TRIGGER knowledge_mutation_idempotency_matches_commit_authority`); err != nil {
					t.Fatalf("isolate outcome size constraint: %v", err)
				}
				executor = outcomeProbe
			}
			_, err := insertKnowledgeWriterIdempotency(
				executor, test.actorKind, "actor-a", test.requestID,
				"objects.create", "create", test.revision, test.token,
				"ko-success-authority", 1, test.successSequence, sql.NullInt64{},
				createdAt+test.createdAtDelta, retainUntil+test.createdAtDelta, test.outcome,
			)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("invalid receipt error = %v, want %q", err, test.want)
			}
		})
	}
	maximumRetention := int64(365 * 24 * time.Hour / time.Microsecond)
	retentionProbe, err := raw.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatalf("begin maximum-retention probe: %v", err)
	}
	if _, err := retentionProbe.ExecContext(t.Context(), `
		DROP TRIGGER knowledge_mutation_commit_authority_update_is_forbidden;
		DROP TRIGGER knowledge_mutation_idempotency_delete_before_retention_is_forbidden;
		DELETE FROM knowledge_mutation_idempotency
		WHERE tenant_id = 'tenant-a' AND actor_kind = 'browser'
		  AND actor_id = 'actor-a' AND route = 'objects.create'
		  AND client_request_id = '`+knowledgeWriterRequestID+`'`); err != nil {
		_ = retentionProbe.Rollback()
		t.Fatalf("remove minimum-retention receipt probe: %v", err)
	}
	if _, err := retentionProbe.ExecContext(t.Context(), `
		UPDATE knowledge_mutation_commit_authorities
		SET client_request_id = ?, retention_anchor_unix_micro = ?,
		    retain_until_unix_micro = ?
		WHERE tenant_id = 'tenant-a' AND catalog_revision = 1`,
		"request-maximum-retention",
		createdAt,
		createdAt+maximumRetention,
	); err != nil {
		_ = retentionProbe.Rollback()
		t.Fatalf("stage maximum-retention commit authority: %v", err)
	}
	if _, err := insertKnowledgeWriterIdempotency(
		retentionProbe, "browser", "actor-a", "request-maximum-retention",
		"objects.create", "create", 1, token,
		"ko-success-authority", 1, sql.NullInt64{Int64: 1, Valid: true},
		sql.NullInt64{}, createdAt, createdAt+maximumRetention, []byte{1},
	); err != nil {
		_ = retentionProbe.Rollback()
		t.Fatalf("insert exact maximum-retention receipt: %v", err)
	}
	if err := retentionProbe.Rollback(); err != nil {
		t.Fatalf("rollback maximum-retention probe: %v", err)
	}
	for _, probe := range []struct {
		name      string
		requestID string
		delta     int64
	}{
		{name: "minimum minus one", requestID: "request-minimum-minus-one", delta: int64(7*24*time.Hour/time.Microsecond) - 1},
		{name: "maximum plus one", requestID: "request-maximum-plus-one", delta: maximumRetention + 1},
	} {
		t.Run("rejects retention "+probe.name, func(t *testing.T) {
			probeTx, err := raw.BeginTx(t.Context(), nil)
			if err != nil {
				t.Fatalf("begin invalid retention probe: %v", err)
			}
			defer func() { _ = probeTx.Rollback() }()
			if _, err := probeTx.ExecContext(t.Context(), `DROP TRIGGER knowledge_mutation_idempotency_matches_commit_authority`); err != nil {
				t.Fatalf("isolate retention check constraint: %v", err)
			}
			if _, err := insertKnowledgeWriterIdempotency(
				probeTx, "browser", "actor-a", probe.requestID,
				"objects.create", "create", 1, token,
				"ko-success-authority", 1, sql.NullInt64{Int64: 1, Valid: true},
				sql.NullInt64{}, createdAt, createdAt+probe.delta, []byte{1},
			); err == nil || !strings.Contains(err.Error(), "CHECK constraint failed") {
				t.Fatalf("retention %s error = %v", probe.name, err)
			}
		})
	}
	capacityProbe, err := raw.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatalf("begin ordinary idempotency capacity probe: %v", err)
	}
	defer func() { _ = capacityProbe.Rollback() }()
	if _, err := capacityProbe.ExecContext(t.Context(), `
		DROP TRIGGER knowledge_mutation_idempotency_matches_commit_authority;
		DROP TRIGGER knowledge_mutation_idempotency_matches_audit_authority;
		UPDATE knowledge_catalog_tenants
		SET idempotency_count = 16384 WHERE tenant_id = 'tenant-a'`); err != nil {
		t.Fatalf("stage ordinary idempotency capacity fence: %v", err)
	}
	if _, err := insertKnowledgeWriterIdempotency(
		capacityProbe, "browser", "actor-a", "request-00000008",
		"objects.create", "create", 1, token,
		"ko-success-authority", 1, sql.NullInt64{Int64: 1, Valid: true},
		sql.NullInt64{}, createdAt, retainUntil, []byte{1},
	); err == nil || !strings.Contains(err.Error(), "idempotency capacity exhausted") {
		t.Fatalf("ordinary capacity receipt error = %v", err)
	}
	if err := capacityProbe.Rollback(); err != nil {
		t.Fatalf("rollback ordinary idempotency capacity probe: %v", err)
	}
	assertIntegerQuery(t, raw, 1, `SELECT count(*) FROM knowledge_mutation_idempotency`)
	assertNoForeignKeyViolations(t, raw)
}

func TestKnowledgeWriterIdempotencyLinksQuarantineToRecoveryReserve(t *testing.T) {
	t.Parallel()

	raw := openKnowledgeMigrationTestDB(t, "knowledge-writer-recovery-authority.sqlite")
	if err := ApplyMigrations(context.Background(), raw, migrations.SQLite()); err != nil {
		t.Fatalf("apply migrations: %v", err)
	}
	seedKnowledgeStatePrerequisites(t, raw)
	createdAt := time.Now().UTC().UnixMicro()
	insertKnowledgeStateVersion(t, raw, stateVersionFixture{
		objectID: "ko-recovery-authority", version: 1, state: "active",
		mutation: "create", timestamp: createdAt - 1,
	})
	insertKnowledgeStateVersion(t, raw, stateVersionFixture{
		objectID: "ko-recovery-authority", version: 2, state: "quarantined",
		mutation: "quarantine", timestamp: createdAt,
		quarantinedAt:    sql.NullInt64{Int64: createdAt, Valid: true},
		quarantineReason: "root_corruption",
	})

	tx, err := raw.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatalf("begin recovery publication: %v", err)
	}
	rollback := func(format string, args ...any) {
		_ = tx.Rollback()
		t.Fatalf(format, args...)
	}
	if _, err := tx.ExecContext(t.Context(), `UPDATE knowledge_catalog_tenants
		SET catalog_revision = catalog_revision + 1
		WHERE tenant_id = 'tenant-a'`); err != nil {
		rollback("advance recovery revision: %v", err)
	}
	var token []byte
	if err := tx.QueryRowContext(t.Context(), `SELECT state_token FROM knowledge_catalog_revision_heads
		WHERE tenant_id = 'tenant-a' AND catalog_revision = 1`).Scan(&token); err != nil {
		rollback("read recovery token: %v", err)
	}
	if _, err := tx.ExecContext(t.Context(), `
		INSERT INTO knowledge_recovery_audit (
			tenant_id, sequence, knowledge_object_id, object_version,
			actor_kind, actor_id, actor_role, app_id, object_type,
			sharing_scope, recovery_reason, occurred_at_unix_micro
		) VALUES (
			'tenant-a', 1, 'ko-recovery-authority', 2,
			'system', 'open-splunk-server', 'system', ?, 'field_extraction',
			'private', 'root_corruption', ?
		)`, knowledgeMigrationTestAppID, createdAt); err != nil {
		rollback("insert recovery audit: %v", err)
	}
	retainUntil := createdAt + int64(7*24*time.Hour/time.Microsecond)
	if _, err := insertKnowledgeWriterCommitAuthority(
		tx, "quarantine", 1, token, "ko-recovery-authority", 2,
		createdAt, createdAt, retainUntil,
		sql.NullInt64{}, sql.NullInt64{Int64: 1, Valid: true},
	); err != nil {
		rollback("insert immutable recovery commit authority: %v", err)
	}
	if _, err := insertKnowledgeWriterIdempotency(
		tx, "system", "open-splunk-server", knowledgeWriterRequestID,
		"objects.quarantine", "quarantine", 1, token,
		"ko-recovery-authority", 2, sql.NullInt64{},
		sql.NullInt64{Int64: 1, Valid: true}, createdAt, retainUntil, []byte{1},
	); err != nil {
		rollback("insert recovery idempotency receipt: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit recovery publication: %v", err)
	}

	assertIntegerQuery(t, raw, 1, `
		SELECT count(*) FROM knowledge_mutation_idempotency
		WHERE tenant_id = 'tenant-a' AND mutation_kind = 'quarantine'
		  AND successful_audit_sequence IS NULL
		  AND recovery_audit_sequence = 1
		  AND committed_catalog_state_token = ?`, token)
	assertIntegerQuery(t, raw, 1, `
		SELECT recovery_audit_count FROM knowledge_catalog_tenants
		WHERE tenant_id = 'tenant-a'`)
	assertNoForeignKeyViolations(t, raw)
}

func insertKnowledgeWriterDependencyVersion(
	t *testing.T,
	db *sql.DB,
	objectID string,
	version int,
	state string,
	mutation string,
	targetObjectID string,
	timestamp int64,
) {
	t.Helper()
	tx, err := db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatalf("begin dependency version %s/%d: %v", objectID, version, err)
	}
	rollback := func(format string, args ...any) {
		_ = tx.Rollback()
		t.Fatalf(format, args...)
	}
	if _, err := tx.ExecContext(t.Context(), `
		INSERT INTO knowledge_object_versions (
			tenant_id, knowledge_object_id, object_version,
			app_id, owner_id, object_type, name, sharing_scope, state,
			definition_digest, dependency_count, mutation_kind,
			quarantine_reason, created_at_unix_micro
		) VALUES (
			'tenant-a', ?, ?, ?, 'owner-a', 'field_extraction', ?,
			'private', ?, zeroblob(32), 1, ?, NULL, ?
		)`,
		objectID, version, knowledgeMigrationTestAppID, objectID, state, mutation, timestamp,
	); err != nil {
		rollback("insert dependency version %s/%d: %v", objectID, version, err)
	}
	if _, err := tx.ExecContext(t.Context(), `
		INSERT INTO knowledge_object_dependencies (
			tenant_id, source_object_id, source_object_version, ordinal,
			target_kind, target_object_id, target_object_version, dependency_role
		) VALUES (
			'tenant-a', ?, ?, 0, 'object', ?, 1, 'field_input'
		)`, objectID, version, targetObjectID); err != nil {
		rollback("insert dependency edge %s/%d: %v", objectID, version, err)
	}
	if _, err := tx.ExecContext(t.Context(), `
		INSERT INTO knowledge_object_dependency_seals (
			tenant_id, knowledge_object_id, object_version, dependency_count
		) VALUES ('tenant-a', ?, ?, 1)`, objectID, version); err != nil {
		rollback("seal dependency version %s/%d: %v", objectID, version, err)
	}
	if _, err := tx.ExecContext(t.Context(), `
		INSERT INTO knowledge_object_list_projections (
			tenant_id, knowledge_object_id, object_version,
			app_id, owner_id, object_type, name, sharing_scope, state,
			description_present, description,
			index_selector_count, host_selector_count,
			source_selector_count, sourcetype_selector_count,
			selector_value_bytes, canonical_selector_bytes
		) VALUES (
			'tenant-a', ?, ?, ?, 'owner-a', 'field_extraction', ?,
			'private', ?, 0, '', 0, 0, 0, 0, 0, 46
		);
		INSERT INTO knowledge_object_list_projection_seals (
			tenant_id, knowledge_object_id, object_version,
			projection_bytes, canonical_selector_bytes
		) SELECT tenant_id, knowledge_object_id, object_version,
		         projection_bytes, canonical_selector_bytes
		    FROM knowledge_object_list_projections
		   WHERE tenant_id = 'tenant-a'
		     AND knowledge_object_id = ?
		     AND object_version = ?`,
		objectID, version, knowledgeMigrationTestAppID, objectID, state,
		objectID, version,
	); err != nil {
		rollback("stage dependency projection %s/%d: %v", objectID, version, err)
	}
	if version == 1 {
		if _, err := tx.ExecContext(t.Context(), `
			INSERT INTO knowledge_objects (
				tenant_id, knowledge_object_id, current_version,
				app_id, owner_id, object_type, name, sharing_scope, state,
				definition_digest, created_at_unix_micro, updated_at_unix_micro,
				disabled_at_unix_micro, quarantined_at_unix_micro,
				deleted_at_unix_micro, quarantine_reason
			) VALUES (
				'tenant-a', ?, 1, ?, 'owner-a', 'field_extraction', ?,
				'private', ?, zeroblob(32), ?, ?, NULL, NULL, NULL, NULL
			)`, objectID, knowledgeMigrationTestAppID, objectID, state, timestamp, timestamp); err != nil {
			rollback("publish dependency object %s: %v", objectID, err)
		}
	} else {
		disabledAt := any(nil)
		if state == "disabled" {
			disabledAt = timestamp
		}
		result, err := tx.ExecContext(t.Context(), `
			UPDATE knowledge_objects SET
				current_version = ?, state = ?, updated_at_unix_micro = ?,
				disabled_at_unix_micro = ?
			WHERE tenant_id = 'tenant-a' AND knowledge_object_id = ?`,
			version, state, timestamp, disabledAt, objectID,
		)
		if err != nil {
			rollback("publish dependency version %s/%d: %v", objectID, version, err)
		}
		if affected, err := result.RowsAffected(); err != nil || affected != 1 {
			rollback("publish dependency version %s/%d affected %d: %v", objectID, version, affected, err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit dependency version %s/%d: %v", objectID, version, err)
	}
}

type knowledgeWriterSQLExecutor interface {
	Exec(string, ...any) (sql.Result, error)
}

func insertKnowledgeWriterCommitAuthority(
	executor knowledgeWriterSQLExecutor,
	mutationKind string,
	revision int64,
	token []byte,
	objectID string,
	objectVersion int64,
	occurredAt int64,
	retentionAnchor int64,
	retainUntil int64,
	successfulAuditSequence sql.NullInt64,
	recoveryAuditSequence sql.NullInt64,
) (sql.Result, error) {
	actorKind := "browser"
	actorID := "actor-a"
	route := "objects." + mutationKind
	if mutationKind == "scope_change" {
		route = "objects.update"
	}
	if mutationKind == "enable" || mutationKind == "disable" {
		route = "objects.set_state"
	}
	if mutationKind == "quarantine" {
		actorKind = "system"
		actorID = "open-splunk-server"
	}
	return executor.Exec(`
		INSERT INTO knowledge_mutation_commit_authorities (
			tenant_id, actor_kind, actor_id, route, client_request_id,
			request_digest, catalog_revision, catalog_state_token, mutation_kind,
			knowledge_object_id, object_version, occurred_at_unix_micro,
			retention_anchor_unix_micro, retain_until_unix_micro,
			successful_audit_sequence, recovery_audit_sequence
		) VALUES (
			'tenant-a', ?, ?, ?, ?, zeroblob(32), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
		)`, actorKind, actorID, route, knowledgeWriterRequestID,
		revision, token, mutationKind, objectID, objectVersion, occurredAt,
		retentionAnchor, retainUntil,
		nullInt64Value(successfulAuditSequence), nullInt64Value(recoveryAuditSequence))
}

func insertKnowledgeWriterIdempotency(
	executor knowledgeWriterSQLExecutor,
	actorKind string,
	actorID string,
	requestID string,
	route string,
	mutationKind string,
	revision int64,
	token []byte,
	objectID string,
	objectVersion int64,
	successfulAuditSequence sql.NullInt64,
	recoveryAuditSequence sql.NullInt64,
	createdAt int64,
	retainUntil int64,
	outcome []byte,
) (sql.Result, error) {
	return executor.Exec(`
		INSERT INTO knowledge_mutation_idempotency (
			tenant_id, actor_kind, actor_id, route, client_request_id,
			mutation_kind, request_digest_format_version, request_digest,
			outcome_format_version, outcome_proto,
			committed_catalog_revision, committed_catalog_state_token,
			knowledge_object_id, object_version,
			successful_audit_sequence, recovery_audit_sequence,
			created_at_unix_micro, retention_anchor_unix_micro,
			retain_until_unix_micro
		) VALUES (
			'tenant-a', ?, ?, ?, ?, ?, 1, zeroblob(32), 1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
		)`, actorKind, actorID, route, requestID, mutationKind, outcome,
		revision, token, objectID, objectVersion,
		nullInt64Value(successfulAuditSequence), nullInt64Value(recoveryAuditSequence),
		createdAt, createdAt, retainUntil)
}

func assertKnowledgeWriterCommitUniqueAuthority(t *testing.T, raw *sql.DB) {
	t.Helper()
	want := strings.Join([]string{
		"tenant_id", "catalog_revision", "catalog_state_token", "actor_kind",
		"actor_id", "route", "client_request_id", "request_digest",
	}, ",")
	rows, err := raw.QueryContext(t.Context(), `SELECT name FROM pragma_index_list(
		'knowledge_mutation_commit_authorities'
	) WHERE "unique" = 1`)
	if err != nil {
		t.Fatalf("list commit-authority unique indexes: %v", err)
	}
	var indexNames []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatalf("scan commit-authority unique index: %v", err)
		}
		indexNames = append(indexNames, name)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate commit-authority unique indexes: %v", err)
	}
	if err := rows.Close(); err != nil {
		t.Fatalf("close commit-authority unique indexes: %v", err)
	}
	found := false
	for _, name := range indexNames {
		var columns string
		if err := raw.QueryRowContext(t.Context(), `SELECT group_concat(name, ',') FROM (
			SELECT name FROM pragma_index_info(?) ORDER BY seqno
		)`, name).Scan(&columns); err != nil {
			t.Fatalf("read commit-authority index %s: %v", name, err)
		}
		if columns == want {
			found = true
		}
	}
	if !found {
		t.Fatalf("commit-authority exact unique index %q is absent", want)
	}
}

func assertKnowledgeWriterReceiptCommitForeignKey(t *testing.T, raw *sql.DB) {
	t.Helper()
	type foreignKeyColumn struct {
		from string
		to   string
	}
	groups := make(map[int][]foreignKeyColumn)
	rows, err := raw.QueryContext(t.Context(), `SELECT id, seq, "from", "to"
		FROM pragma_foreign_key_list('knowledge_mutation_idempotency')
		WHERE "table" = 'knowledge_mutation_commit_authorities'
		ORDER BY id, seq`)
	if err != nil {
		t.Fatalf("read receipt commit-authority foreign key: %v", err)
	}
	defer rows.Close()
	for rows.Next() {
		var id, sequence int
		var from, to string
		if err := rows.Scan(&id, &sequence, &from, &to); err != nil {
			t.Fatalf("scan receipt commit-authority foreign key: %v", err)
		}
		if sequence != len(groups[id]) {
			t.Fatalf("receipt commit foreign-key sequence = %d, want %d", sequence, len(groups[id]))
		}
		groups[id] = append(groups[id], foreignKeyColumn{from: from, to: to})
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate receipt commit-authority foreign key: %v", err)
	}
	want := []foreignKeyColumn{
		{from: "tenant_id", to: "tenant_id"},
		{from: "committed_catalog_revision", to: "catalog_revision"},
		{from: "committed_catalog_state_token", to: "catalog_state_token"},
		{from: "actor_kind", to: "actor_kind"},
		{from: "actor_id", to: "actor_id"},
		{from: "route", to: "route"},
		{from: "client_request_id", to: "client_request_id"},
		{from: "request_digest", to: "request_digest"},
	}
	if len(groups) != 1 {
		t.Fatalf("receipt commit-authority foreign-key groups = %#v, want one", groups)
	}
	for _, got := range groups {
		if len(got) != len(want) {
			t.Fatalf("receipt commit-authority foreign-key columns = %#v, want %#v", got, want)
		}
		for index := range want {
			if got[index] != want[index] {
				t.Fatalf("receipt commit-authority foreign-key column %d = %#v, want %#v", index, got[index], want[index])
			}
		}
	}
}
