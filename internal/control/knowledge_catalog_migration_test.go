package control

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/Suhaibinator/open-splunk/migrations"
)

const knowledgeMigrationTestAppID = "app_AAAAAAAAAAAAAAAAAAAAAA"

func TestKnowledgeCatalogMigrationUpgradesLegacyDatabaseAndPinsSchema(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	raw := openKnowledgeMigrationTestDB(t, "knowledge-upgrade.sqlite")
	if err := ApplyMigrations(ctx, raw, migrationsBefore(t, "0024_")); err != nil {
		t.Fatalf("apply pre-knowledge migrations: %v", err)
	}
	seedKnowledgeMigrationApp(t, raw)

	if err := ApplyMigrations(ctx, raw, migrations.SQLite()); err != nil {
		t.Fatalf("apply knowledge catalog migration: %v", err)
	}

	var migrationCount int
	if err := raw.QueryRowContext(ctx, `SELECT count(*) FROM schema_migrations`).Scan(&migrationCount); err != nil {
		t.Fatalf("count migrations: %v", err)
	}
	if migrationCount != 24 {
		t.Fatalf("migration count = %d, want 24", migrationCount)
	}
	var appDisplayName string
	if err := raw.QueryRowContext(ctx, `
		SELECT display_name FROM app_workspaces WHERE app_id = ?`,
		knowledgeMigrationTestAppID,
	).Scan(&appDisplayName); err != nil {
		t.Fatalf("read preserved app: %v", err)
	}
	if appDisplayName != "Knowledge migration" {
		t.Fatalf("preserved app display name = %q", appDisplayName)
	}

	wantTables := []string{
		"knowledge_app_active_counters",
		"knowledge_app_type_active_counters",
		"knowledge_catalog_tenants",
		"knowledge_definition_blobs",
		"knowledge_mutation_idempotency",
		"knowledge_object_acl",
		"knowledge_object_dependencies",
		"knowledge_object_dependency_seals",
		"knowledge_object_versions",
		"knowledge_objects",
		"knowledge_owner_active_counters",
		"knowledge_recovery_audit",
		"knowledge_type_active_counters",
	}
	rows, err := raw.QueryContext(ctx, `
		SELECT name, wr, strict
		FROM pragma_table_list
		WHERE type = 'table' AND name LIKE 'knowledge_%'
		ORDER BY name`)
	if err != nil {
		t.Fatalf("list knowledge tables: %v", err)
	}
	defer rows.Close()
	var gotTables []string
	for rows.Next() {
		var name string
		var withoutRowID, strict int
		if err := rows.Scan(&name, &withoutRowID, &strict); err != nil {
			t.Fatalf("scan knowledge table: %v", err)
		}
		if withoutRowID != 1 || strict != 1 {
			t.Errorf("table %s flags = WITHOUT ROWID %d STRICT %d, want 1 1", name, withoutRowID, strict)
		}
		gotTables = append(gotTables, name)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate knowledge tables: %v", err)
	}
	if fmt.Sprint(gotTables) != fmt.Sprint(wantTables) {
		t.Fatalf("knowledge tables = %v, want %v", gotTables, wantTables)
	}

	wantIndexes := []string{
		"knowledge_mutation_idempotency_retention_idx",
		"knowledge_object_acl_role_object_idx",
		"knowledge_object_dependencies_target_idx",
		"knowledge_objects_active_app_name_idx",
		"knowledge_objects_active_global_name_idx",
		"knowledge_objects_active_private_name_idx",
		"knowledge_objects_list_updated_idx",
		"knowledge_objects_resolution_idx",
		"knowledge_recovery_audit_occurred_idx",
	}
	indexRows, err := raw.QueryContext(ctx, `
		SELECT name
		FROM sqlite_schema
		WHERE type = 'index' AND name LIKE 'knowledge_%'
		  AND sql IS NOT NULL
		ORDER BY name`)
	if err != nil {
		t.Fatalf("list knowledge indexes: %v", err)
	}
	defer indexRows.Close()
	var gotIndexes []string
	for indexRows.Next() {
		var name string
		if err := indexRows.Scan(&name); err != nil {
			t.Fatalf("scan knowledge index: %v", err)
		}
		gotIndexes = append(gotIndexes, name)
	}
	if err := indexRows.Err(); err != nil {
		t.Fatalf("iterate knowledge indexes: %v", err)
	}
	if fmt.Sprint(gotIndexes) != fmt.Sprint(wantIndexes) {
		t.Fatalf("knowledge indexes = %v, want %v", gotIndexes, wantIndexes)
	}

	assertNoForeignKeyViolations(t, raw)
}

func TestKnowledgeCatalogSchemaEnforcesImmutablePublicationAndRecovery(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	raw := openKnowledgeMigrationTestDB(t, "knowledge-invariants.sqlite")
	if err := ApplyMigrations(ctx, raw, migrations.SQLite()); err != nil {
		t.Fatalf("apply migrations: %v", err)
	}
	seedKnowledgeMigrationApp(t, raw)

	tx, err := raw.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin seed transaction: %v", err)
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO knowledge_catalog_tenants (tenant_id) VALUES ('tenant-a');
		INSERT INTO knowledge_definition_blobs (
			tenant_id, definition_digest, definition_proto,
			definition_bytes, created_at_unix_micro
		) VALUES ('tenant-a', zeroblob(32), X'01', 1, 10);

		INSERT INTO knowledge_objects (
			tenant_id, knowledge_object_id, current_version,
			app_id, owner_id, object_type, name, sharing_scope, state,
			definition_digest, created_at_unix_micro, updated_at_unix_micro
		) VALUES (
			'tenant-a', 'ko-target', 1,
			?, 'owner-a', 'field_extraction', 'base', 'private', 'active',
			zeroblob(32), 10, 10
		);
		INSERT INTO knowledge_object_versions (
			tenant_id, knowledge_object_id, object_version,
			app_id, owner_id, object_type, name, sharing_scope, state,
			definition_digest, dependency_count, mutation_kind,
			created_at_unix_micro
		) VALUES (
			'tenant-a', 'ko-target', 1,
			?, 'owner-a', 'field_extraction', 'base', 'private', 'active',
				zeroblob(32), 0, 'create', 10
			);
			INSERT INTO knowledge_object_dependency_seals (
				tenant_id, knowledge_object_id, object_version, dependency_count
			) VALUES ('tenant-a', 'ko-target', 1, 0);

			INSERT INTO knowledge_objects (
			tenant_id, knowledge_object_id, current_version,
			app_id, owner_id, object_type, name, sharing_scope, state,
			definition_digest, created_at_unix_micro, updated_at_unix_micro
		) VALUES (
			'tenant-a', 'ko-source', 1,
			?, 'owner-a', 'field_alias', 'derived', 'private', 'active',
			zeroblob(32), 11, 11
		);
		INSERT INTO knowledge_object_versions (
			tenant_id, knowledge_object_id, object_version,
			app_id, owner_id, object_type, name, sharing_scope, state,
			definition_digest, dependency_count, mutation_kind,
			created_at_unix_micro
		) VALUES (
			'tenant-a', 'ko-source', 1,
			?, 'owner-a', 'field_alias', 'derived', 'private', 'active',
				zeroblob(32), 1, 'create', 11
		);
		INSERT INTO knowledge_object_dependencies (
			tenant_id, source_object_id, source_object_version, ordinal,
			target_kind, target_object_id, target_object_version, dependency_role
		) VALUES (
			'tenant-a', 'ko-source', 1, 0,
			'object', 'ko-target', 1, 'field_input'
			);
			INSERT INTO knowledge_object_dependency_seals (
				tenant_id, knowledge_object_id, object_version, dependency_count
			) VALUES ('tenant-a', 'ko-source', 1, 1);
		INSERT INTO knowledge_object_acl (
			tenant_id, knowledge_object_id, role_id, can_read, can_write
		) VALUES ('tenant-a', 'ko-source', 'administrator', 1, 1);
		UPDATE knowledge_catalog_tenants
		SET catalog_revision = catalog_revision + 1
		WHERE tenant_id = 'tenant-a'`,
		knowledgeMigrationTestAppID,
		knowledgeMigrationTestAppID,
		knowledgeMigrationTestAppID,
		knowledgeMigrationTestAppID,
	); err != nil {
		_ = tx.Rollback()
		t.Fatalf("seed knowledge catalog: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit seed transaction: %v", err)
	}

	var revision, identities, versions, bodyBytes, active int
	if err := raw.QueryRowContext(ctx, `
		SELECT catalog_revision, identity_count, version_count,
		       definition_body_bytes, active_object_count
		FROM knowledge_catalog_tenants WHERE tenant_id = 'tenant-a'`,
	).Scan(&revision, &identities, &versions, &bodyBytes, &active); err != nil {
		t.Fatalf("read catalog counters: %v", err)
	}
	if revision != 1 || identities != 2 || versions != 2 || bodyBytes != 1 || active != 2 {
		t.Fatalf("catalog state = rev %d ids %d versions %d bytes %d active %d", revision, identities, versions, bodyBytes, active)
	}
	assertIntegerQuery(t, raw, 2, `
		SELECT active_object_count FROM knowledge_app_active_counters
		WHERE tenant_id = 'tenant-a' AND app_id = ?`, knowledgeMigrationTestAppID)
	assertIntegerQuery(t, raw, 2, `
		SELECT active_private_object_count FROM knowledge_owner_active_counters
		WHERE tenant_id = 'tenant-a' AND owner_id = 'owner-a'`)

	assertSQLFailsContaining(t, raw, "advance by one", `
		UPDATE knowledge_catalog_tenants SET catalog_revision = 3
		WHERE tenant_id = 'tenant-a'`)
	assertSQLFailsContaining(t, raw, "immutable", `
		UPDATE knowledge_definition_blobs SET definition_proto = X'02'
		WHERE tenant_id = 'tenant-a' AND definition_digest = zeroblob(32)`)
	assertSQLFailsContaining(t, raw, "immutable", `
		UPDATE knowledge_object_versions SET dependency_count = 0
		WHERE tenant_id = 'tenant-a' AND knowledge_object_id = 'ko-source'
		  AND object_version = 1`)
	assertSQLFailsContaining(t, raw, "immutable", `
		UPDATE knowledge_object_dependencies SET dependency_role = 'other'
		WHERE tenant_id = 'tenant-a' AND source_object_id = 'ko-source'`)
	assertSQLFailsContaining(t, raw, "dependency set is sealed", `
		INSERT INTO knowledge_object_dependencies (
			tenant_id, source_object_id, source_object_version, ordinal,
			target_kind, target_object_id, target_object_version, dependency_role
		) VALUES (
			'tenant-a', 'ko-source', 1, 1,
			'object', 'ko-nonexistent', 1, 'field_input'
		)`)
	assertSQLFailsContaining(t, raw, "CHECK constraint", `
		INSERT INTO knowledge_object_acl (
			tenant_id, knowledge_object_id, role_id, can_read, can_write
		) VALUES ('tenant-a', 'ko-target', 'broken-writer', 0, 1)`)

	// SQLite's OR REPLACE conflict policy can otherwise delete the conflicting
	// row without running DELETE triggers. Explicit BEFORE INSERT collision
	// guards protect every immutable identity used so far.
	assertSQLFails(t, raw, `
		INSERT OR REPLACE INTO knowledge_catalog_tenants (tenant_id)
		VALUES ('tenant-a')`)
	assertSQLFails(t, raw, `
		INSERT OR REPLACE INTO knowledge_definition_blobs (
			tenant_id, definition_digest, definition_proto,
			definition_bytes, created_at_unix_micro
		) VALUES ('tenant-a', zeroblob(32), X'02', 1, 12)`)
	assertSQLFails(t, raw, fmt.Sprintf(`
		INSERT OR REPLACE INTO knowledge_objects (
			tenant_id, knowledge_object_id, current_version,
			app_id, owner_id, object_type, name, sharing_scope, state,
			definition_digest, created_at_unix_micro, updated_at_unix_micro
		) VALUES (
			'tenant-a', 'ko-target', 1, '%s', 'owner-a',
			'field_extraction', 'base', 'private', 'active',
			zeroblob(32), 10, 10
		)`, knowledgeMigrationTestAppID))
	assertSQLFails(t, raw, fmt.Sprintf(`
		INSERT OR REPLACE INTO knowledge_object_versions (
			tenant_id, knowledge_object_id, object_version,
			app_id, owner_id, object_type, name, sharing_scope, state,
			definition_digest, dependency_count, mutation_kind,
			created_at_unix_micro
		) VALUES (
			'tenant-a', 'ko-target', 1, '%s', 'owner-a',
			'field_extraction', 'base', 'private', 'active',
			zeroblob(32), 0, 'create', 10
		)`, knowledgeMigrationTestAppID))
	assertSQLFails(t, raw, `
		INSERT OR REPLACE INTO knowledge_object_dependencies (
			tenant_id, source_object_id, source_object_version, ordinal,
			target_kind, target_object_id, target_object_version, dependency_role
		) VALUES (
			'tenant-a', 'ko-source', 1, 0,
			'object', 'ko-target', 1, 'field_input'
		)`)
	assertSQLFails(t, raw, `
		INSERT OR REPLACE INTO knowledge_object_acl (
			tenant_id, knowledge_object_id, role_id, can_read, can_write
		) VALUES ('tenant-a', 'ko-source', 'administrator', 1, 0)`)
	assertSQLFails(t, raw, `
		INSERT OR REPLACE INTO knowledge_app_active_counters (
			tenant_id, app_id, active_object_count
		) VALUES ('tenant-a', 'app_AAAAAAAAAAAAAAAAAAAAAA', 0)`)

	if _, err := raw.ExecContext(ctx, `
		INSERT INTO knowledge_mutation_idempotency (
			tenant_id, actor_id, route, client_request_id, mutation_kind,
			request_digest, outcome_proto, committed_catalog_revision,
			knowledge_object_id, object_version,
			created_at_unix_micro, retain_until_unix_micro
		) VALUES (
			'tenant-a', 'owner-a', 'objects.create', 'idempotency-test-1', 'create',
			zeroblob(32), X'01', 1, 'ko-target', 1,
			2000000000000000, 2000604800000000
		)`); err != nil {
		t.Fatalf("insert idempotency outcome: %v", err)
	}
	assertSQLFails(t, raw, `
		INSERT OR REPLACE INTO knowledge_mutation_idempotency (
			tenant_id, actor_id, route, client_request_id, mutation_kind,
			request_digest, outcome_proto, committed_catalog_revision,
			knowledge_object_id, object_version,
			created_at_unix_micro, retain_until_unix_micro
		) VALUES (
			'tenant-a', 'owner-a', 'objects.create', 'idempotency-test-1', 'create',
			randomblob(32), X'02', 1, 'ko-target', 1,
			2000000000000000, 2000604800000000
		)`)
	assertSQLFailsContaining(t, raw, "retention fence", `
		DELETE FROM knowledge_mutation_idempotency
		WHERE tenant_id = 'tenant-a' AND actor_id = 'owner-a'
		  AND route = 'objects.create'
		  AND client_request_id = 'idempotency-test-1'`)

	// Binary identity means an ASCII-case-distinct name is not a collision.
	insertKnowledgeMigrationObject(t, raw, "ko-case", "BASE", "private", "active", 20)
	assertSQLFailsContaining(t, raw, "active name already exists", fmt.Sprintf(`
		INSERT INTO knowledge_objects (
			tenant_id, knowledge_object_id, current_version,
			app_id, owner_id, object_type, name, sharing_scope, state,
			definition_digest, created_at_unix_micro, updated_at_unix_micro
		) VALUES (
			'tenant-a', 'ko-collision', 1,
			'%s', 'owner-a', 'field_extraction', 'base', 'private', 'active',
			zeroblob(32), 21, 21
		)`, knowledgeMigrationTestAppID))

	// A terminal quarantine consumes the protected version and recovery-audit
	// reserves, drops the object from active counters, and cannot be reversed.
	quarantine, err := raw.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin quarantine transaction: %v", err)
	}
	if _, err := quarantine.ExecContext(ctx, `
		UPDATE knowledge_objects
		SET current_version = 2,
		    state = 'quarantined',
		    definition_digest = NULL,
		    updated_at_unix_micro = 30,
		    quarantined_at_unix_micro = 30,
		    quarantine_reason = 'root_corruption'
		WHERE tenant_id = 'tenant-a' AND knowledge_object_id = 'ko-source';
		INSERT INTO knowledge_object_versions (
			tenant_id, knowledge_object_id, object_version,
			app_id, owner_id, object_type, name, sharing_scope, state,
			definition_digest, dependency_count, mutation_kind,
			quarantine_reason, created_at_unix_micro
		) VALUES (
			'tenant-a', 'ko-source', 2,
			?, 'owner-a', 'field_alias', 'derived', 'private', 'quarantined',
				NULL, 0, 'quarantine', 'root_corruption', 30
			);
			INSERT INTO knowledge_object_dependency_seals (
				tenant_id, knowledge_object_id, object_version, dependency_count
			) VALUES ('tenant-a', 'ko-source', 2, 0);
		INSERT INTO knowledge_recovery_audit (
			tenant_id, sequence, knowledge_object_id, object_version,
			actor_kind, actor_id, actor_role, app_id, object_type,
			sharing_scope, recovery_reason, occurred_at_unix_micro
		) VALUES (
			'tenant-a', 1, 'ko-source', 2,
			'browser', 'owner-a', 'administrator', ?, 'field_alias',
			'private', 'root_corruption', 30
		);
		UPDATE knowledge_catalog_tenants
		SET catalog_revision = catalog_revision + 1
		WHERE tenant_id = 'tenant-a'`,
		knowledgeMigrationTestAppID,
		knowledgeMigrationTestAppID,
	); err != nil {
		_ = quarantine.Rollback()
		t.Fatalf("quarantine object: %v", err)
	}
	if err := quarantine.Commit(); err != nil {
		t.Fatalf("commit quarantine: %v", err)
	}
	assertIntegerQuery(t, raw, 1, `
		SELECT recovery_audit_count FROM knowledge_catalog_tenants
		WHERE tenant_id = 'tenant-a'`)
	assertIntegerQuery(t, raw, 2, `
		SELECT active_object_count FROM knowledge_catalog_tenants
		WHERE tenant_id = 'tenant-a'`)
	assertSQLFailsContaining(t, raw, "registry transition is invalid", `
		UPDATE knowledge_objects
		SET current_version = 3, state = 'disabled', definition_digest = zeroblob(32),
		    updated_at_unix_micro = 31, disabled_at_unix_micro = 31,
		    quarantined_at_unix_micro = NULL, quarantine_reason = NULL
		WHERE tenant_id = 'tenant-a' AND knowledge_object_id = 'ko-source'`)
	assertSQLFailsContaining(t, raw, "immutable", `
		UPDATE knowledge_recovery_audit SET occurred_at_unix_micro = 31
		WHERE tenant_id = 'tenant-a' AND sequence = 1`)
	assertSQLFailsContaining(t, raw, "cannot be deleted", `
		DELETE FROM knowledge_recovery_audit
		WHERE tenant_id = 'tenant-a' AND sequence = 1`)
	assertSQLFails(t, raw, `
		INSERT OR REPLACE INTO knowledge_recovery_audit (
			tenant_id, sequence, knowledge_object_id, object_version,
			actor_kind, actor_id, actor_role, app_id, object_type,
			sharing_scope, recovery_reason, occurred_at_unix_micro
		) VALUES (
			'tenant-a', 1, 'ko-source', 2,
			'browser', 'owner-a', 'administrator',
			'app_AAAAAAAAAAAAAAAAAAAAAA', 'field_alias',
			'private', 'root_corruption', 31
		)`)
	assertSQLFailsContaining(t, raw, "recovery audit identity already exists", `
		INSERT INTO knowledge_recovery_audit (
			tenant_id, sequence, knowledge_object_id, object_version,
			actor_kind, actor_id, actor_role, app_id, object_type,
			sharing_scope, recovery_reason, occurred_at_unix_micro
		) VALUES (
			'tenant-a', 2, 'ko-source', 2,
			'system', 'system', 'system', 'app_AAAAAAAAAAAAAAAAAAAAAA',
			'field_alias', 'private', 'root_corruption', 31
		)`)

	// Active knowledge deterministically blocks app archive, and every retained
	// identity blocks app deletion.
	assertSQLFailsContaining(t, raw, "active knowledge objects", `
		UPDATE app_workspaces
		SET version = version + 1, state = 'archived',
		    updated_at_unix_micro = 40, archived_at_unix_micro = 40
		WHERE app_id = 'app_AAAAAAAAAAAAAAAAAAAAAA'`)
	assertSQLFailsContaining(t, raw, "referenced by knowledge objects", `
		DELETE FROM app_workspaces WHERE app_id = 'app_AAAAAAAAAAAAAAAAAAAAAA'`)

	assertNoForeignKeyViolations(t, raw)
}

func TestKnowledgeCatalogSchemaProtectsNormalCapacityReserves(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	raw := openKnowledgeMigrationTestDB(t, "knowledge-capacity.sqlite")
	if err := ApplyMigrations(ctx, raw, migrations.SQLite()); err != nil {
		t.Fatalf("apply migrations: %v", err)
	}
	seedKnowledgeMigrationApp(t, raw)
	if _, err := raw.ExecContext(ctx, `INSERT INTO knowledge_catalog_tenants (tenant_id) VALUES ('tenant-a')`); err != nil {
		t.Fatalf("create catalog tenant: %v", err)
	}
	if _, err := raw.ExecContext(ctx, `
		INSERT INTO knowledge_definition_blobs (
			tenant_id, definition_digest, definition_proto,
			definition_bytes, created_at_unix_micro
		) VALUES ('tenant-a', zeroblob(32), X'01', 1, 10)`); err != nil {
		t.Fatalf("create definition blob: %v", err)
	}
	insertKnowledgeMigrationObject(t, raw, "ko-capacity", "capacity", "private", "active", 10)
	update, err := raw.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin capacity update seed: %v", err)
	}
	if _, err := update.ExecContext(ctx, `
		UPDATE knowledge_objects
		SET current_version = 2, updated_at_unix_micro = 11
		WHERE tenant_id = 'tenant-a' AND knowledge_object_id = 'ko-capacity';
		INSERT INTO knowledge_object_versions (
			tenant_id, knowledge_object_id, object_version,
			app_id, owner_id, object_type, name, sharing_scope, state,
			definition_digest, dependency_count, mutation_kind,
			created_at_unix_micro
		) VALUES (
			'tenant-a', 'ko-capacity', 2,
				'app_AAAAAAAAAAAAAAAAAAAAAA', 'owner-a', 'field_extraction',
				'capacity', 'private', 'active', zeroblob(32), 0, 'update', 11
			);
			INSERT INTO knowledge_object_dependency_seals (
				tenant_id, knowledge_object_id, object_version, dependency_count
			) VALUES ('tenant-a', 'ko-capacity', 2, 0)`); err != nil {
		_ = update.Rollback()
		t.Fatalf("seed capacity update version: %v", err)
	}
	if err := update.Commit(); err != nil {
		t.Fatalf("commit capacity update seed: %v", err)
	}

	if _, err := raw.ExecContext(ctx, `
		UPDATE knowledge_catalog_tenants
		SET version_count = 61440, idempotency_count = 16384
		WHERE tenant_id = 'tenant-a'`); err != nil {
		t.Fatalf("move counters to normal capacity fences: %v", err)
	}
	assertSQLFailsContaining(t, raw, "version capacity exhausted", `
		INSERT INTO knowledge_object_versions (
			tenant_id, knowledge_object_id, object_version,
			app_id, owner_id, object_type, name, sharing_scope, state,
			definition_digest, dependency_count, mutation_kind,
			created_at_unix_micro
		) VALUES (
			'tenant-a', 'ko-capacity', 3,
			'app_AAAAAAAAAAAAAAAAAAAAAA', 'owner-a', 'field_extraction',
			'capacity', 'private', 'active', zeroblob(32), 0, 'update', 20
		)`)
	assertSQLFailsContaining(t, raw, "idempotency capacity exhausted", `
		INSERT INTO knowledge_mutation_idempotency (
			tenant_id, actor_id, route, client_request_id, mutation_kind,
			request_digest, outcome_proto, committed_catalog_revision,
			knowledge_object_id, object_version,
			created_at_unix_micro, retain_until_unix_micro
		) VALUES (
			'tenant-a', 'owner-a', 'objects.update', 'normal-request-0001', 'update',
			zeroblob(32), X'01', 1, 'ko-capacity', 2,
			10, 604800000010
		)`)
	protected, err := raw.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin protected quarantine: %v", err)
	}
	if _, err := protected.ExecContext(ctx, `
		UPDATE knowledge_objects
		SET current_version = 3,
		    state = 'quarantined',
		    definition_digest = NULL,
		    updated_at_unix_micro = 20,
		    quarantined_at_unix_micro = 20,
		    quarantine_reason = 'root_corruption'
		WHERE tenant_id = 'tenant-a' AND knowledge_object_id = 'ko-capacity';
		INSERT INTO knowledge_object_versions (
			tenant_id, knowledge_object_id, object_version,
			app_id, owner_id, object_type, name, sharing_scope, state,
			definition_digest, dependency_count, mutation_kind,
			quarantine_reason, created_at_unix_micro
		) VALUES (
			'tenant-a', 'ko-capacity', 3,
			'app_AAAAAAAAAAAAAAAAAAAAAA', 'owner-a', 'field_extraction',
			'capacity', 'private', 'quarantined', NULL, 0, 'quarantine',
			'root_corruption', 20
		);
		INSERT INTO knowledge_object_dependency_seals (
			tenant_id, knowledge_object_id, object_version, dependency_count
		) VALUES ('tenant-a', 'ko-capacity', 3, 0);
		INSERT INTO knowledge_mutation_idempotency (
			tenant_id, actor_id, route, client_request_id, mutation_kind,
			request_digest, outcome_proto, committed_catalog_revision,
			knowledge_object_id, object_version,
			created_at_unix_micro, retain_until_unix_micro
		) VALUES (
			'tenant-a', 'owner-a', 'objects.quarantine', 'recovery-request-01', 'quarantine',
			zeroblob(32), X'01', 1, 'ko-capacity', 3,
			10, 604800000010
		);
		UPDATE knowledge_catalog_tenants
		SET catalog_revision = catalog_revision + 1
		WHERE tenant_id = 'tenant-a'`); err != nil {
		_ = protected.Rollback()
		t.Fatalf("protected quarantine transaction: %v", err)
	}
	if err := protected.Commit(); err != nil {
		t.Fatalf("commit protected quarantine: %v", err)
	}

	if _, err := raw.ExecContext(ctx, `
		UPDATE knowledge_catalog_tenants
		SET definition_body_bytes = 536870911
		WHERE tenant_id = 'tenant-a'`); err != nil {
		t.Fatalf("move definition bytes near fence: %v", err)
	}
	assertSQLFailsContaining(t, raw, "definition body capacity exhausted", `
		INSERT INTO knowledge_definition_blobs (
			tenant_id, definition_digest, definition_proto,
			definition_bytes, created_at_unix_micro
		) VALUES ('tenant-a', randomblob(32), X'0102', 2, 20)`)
}

func TestKnowledgeCatalogActiveObjectsRequireActiveApp(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	raw := openKnowledgeMigrationTestDB(t, "knowledge-active-app.sqlite")
	if err := ApplyMigrations(ctx, raw, migrations.SQLite()); err != nil {
		t.Fatalf("apply migrations: %v", err)
	}
	seedKnowledgeMigrationApp(t, raw)
	if _, err := raw.ExecContext(ctx, `
		UPDATE app_workspaces
		SET version = 2, state = 'archived',
		    updated_at_unix_micro = 2, archived_at_unix_micro = 2
		WHERE app_id = ?;
		INSERT INTO knowledge_catalog_tenants (tenant_id) VALUES ('tenant-a');
		INSERT INTO knowledge_definition_blobs (
			tenant_id, definition_digest, definition_proto,
			definition_bytes, created_at_unix_micro
		) VALUES ('tenant-a', zeroblob(32), X'01', 1, 10)`, knowledgeMigrationTestAppID); err != nil {
		t.Fatalf("prepare archived app catalog: %v", err)
	}

	assertSQLFailsContaining(t, raw, "requires active app", `
		INSERT INTO knowledge_objects (
			tenant_id, knowledge_object_id, current_version,
			app_id, owner_id, object_type, name, sharing_scope, state,
			definition_digest, created_at_unix_micro, updated_at_unix_micro
		) VALUES (
			'tenant-a', 'ko-active-insert', 1,
			'app_AAAAAAAAAAAAAAAAAAAAAA', 'owner-a', 'field_extraction',
			'active-insert', 'private', 'active', zeroblob(32), 10, 10
		)`)

	insertKnowledgeMigrationObject(t, raw, "ko-draft-app", "draft-app", "private", "draft", 10)
	assertSQLFailsContaining(t, raw, "requires active app", `
		UPDATE knowledge_objects
		SET current_version = 2, state = 'active', updated_at_unix_micro = 11
		WHERE tenant_id = 'tenant-a' AND knowledge_object_id = 'ko-draft-app'`)

	// Reactivating the app first makes the same state transition legal.
	if _, err := raw.ExecContext(ctx, `
		UPDATE app_workspaces
		SET version = 3, state = 'active',
		    updated_at_unix_micro = 3, archived_at_unix_micro = NULL
		WHERE app_id = ?`, knowledgeMigrationTestAppID); err != nil {
		t.Fatalf("reactivate app: %v", err)
	}
	activate, err := raw.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin object activation: %v", err)
	}
	if _, err := activate.ExecContext(ctx, `
		UPDATE knowledge_objects
		SET current_version = 2, state = 'active', updated_at_unix_micro = 11
		WHERE tenant_id = 'tenant-a' AND knowledge_object_id = 'ko-draft-app';
		INSERT INTO knowledge_object_versions (
			tenant_id, knowledge_object_id, object_version,
			app_id, owner_id, object_type, name, sharing_scope, state,
			definition_digest, dependency_count, mutation_kind,
			created_at_unix_micro
		) VALUES (
			'tenant-a', 'ko-draft-app', 2,
			'app_AAAAAAAAAAAAAAAAAAAAAA', 'owner-a', 'field_extraction',
			'draft-app', 'private', 'active', zeroblob(32), 0, 'enable', 11
		);
		INSERT INTO knowledge_object_dependency_seals (
			tenant_id, knowledge_object_id, object_version, dependency_count
		) VALUES ('tenant-a', 'ko-draft-app', 2, 0)`); err != nil {
		_ = activate.Rollback()
		t.Fatalf("activate object after app: %v", err)
	}
	if err := activate.Commit(); err != nil {
		t.Fatalf("commit object activation: %v", err)
	}
	assertIntegerQuery(t, raw, 1, `
		SELECT active_object_count FROM knowledge_catalog_tenants
		WHERE tenant_id = 'tenant-a'`)
	assertNoForeignKeyViolations(t, raw)
}

func TestKnowledgeCatalogDependencyTargetsRequireDependentFirstCascade(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	raw := openKnowledgeMigrationTestDB(t, "knowledge-dependent-first.sqlite")
	if err := ApplyMigrations(ctx, raw, migrations.SQLite()); err != nil {
		t.Fatalf("apply migrations: %v", err)
	}
	seedKnowledgeMigrationApp(t, raw)
	tx, err := raw.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin dependency seed: %v", err)
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO knowledge_catalog_tenants (tenant_id) VALUES ('tenant-a');
		INSERT INTO knowledge_definition_blobs (
			tenant_id, definition_digest, definition_proto,
			definition_bytes, created_at_unix_micro
		) VALUES ('tenant-a', zeroblob(32), X'01', 1, 10);
		INSERT INTO knowledge_objects (
			tenant_id, knowledge_object_id, current_version,
			app_id, owner_id, object_type, name, sharing_scope, state,
			definition_digest, created_at_unix_micro, updated_at_unix_micro
		) VALUES
			('tenant-a', 'ko-dependency-target', 1, ?, 'owner-a',
			 'field_extraction', 'dependency-target', 'private', 'active',
			 zeroblob(32), 10, 10),
			('tenant-a', 'ko-dependent', 1, ?, 'owner-a',
			 'field_alias', 'dependent', 'private', 'active',
			 zeroblob(32), 10, 10);
		INSERT INTO knowledge_object_versions (
			tenant_id, knowledge_object_id, object_version,
			app_id, owner_id, object_type, name, sharing_scope, state,
			definition_digest, dependency_count, mutation_kind,
			created_at_unix_micro
		) VALUES
			('tenant-a', 'ko-dependency-target', 1, ?, 'owner-a',
			 'field_extraction', 'dependency-target', 'private', 'active',
			 zeroblob(32), 0, 'create', 10),
			('tenant-a', 'ko-dependent', 1, ?, 'owner-a',
			 'field_alias', 'dependent', 'private', 'active',
			 zeroblob(32), 1, 'create', 10);
		INSERT INTO knowledge_object_dependencies (
			tenant_id, source_object_id, source_object_version, ordinal,
			target_kind, target_object_id, target_object_version, dependency_role
		) VALUES (
			'tenant-a', 'ko-dependent', 1, 0,
			'object', 'ko-dependency-target', 1, 'field_input'
		);
		INSERT INTO knowledge_object_dependency_seals (
			tenant_id, knowledge_object_id, object_version, dependency_count
		) VALUES
			('tenant-a', 'ko-dependency-target', 1, 0),
			('tenant-a', 'ko-dependent', 1, 1)`,
		knowledgeMigrationTestAppID,
		knowledgeMigrationTestAppID,
		knowledgeMigrationTestAppID,
		knowledgeMigrationTestAppID,
	); err != nil {
		_ = tx.Rollback()
		t.Fatalf("seed dependency graph: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit dependency graph: %v", err)
	}

	for _, state := range []string{"disabled", "deleted"} {
		columns := "disabled_at_unix_micro = 20"
		if state == "deleted" {
			columns = "deleted_at_unix_micro = 20"
		}
		assertSQLFailsContaining(t, raw, "active dependents", fmt.Sprintf(`
			UPDATE knowledge_objects
			SET current_version = 2, state = '%s', updated_at_unix_micro = 20,
			    %s
			WHERE tenant_id = 'tenant-a'
			  AND knowledge_object_id = 'ko-dependency-target'`, state, columns))
	}
	assertIntegerQuery(t, raw, 2, `
		SELECT active_object_count FROM knowledge_catalog_tenants
		WHERE tenant_id = 'tenant-a'`)

	// The explicit dependent-first ordering is legal in one immediate
	// transaction: disable the dependent current version, then its target.
	cascade, err := raw.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin dependent-first cascade: %v", err)
	}
	if _, err := cascade.ExecContext(ctx, `
		UPDATE knowledge_objects
		SET current_version = 2, state = 'disabled',
		    updated_at_unix_micro = 30, disabled_at_unix_micro = 30
		WHERE tenant_id = 'tenant-a' AND knowledge_object_id = 'ko-dependent';
		INSERT INTO knowledge_object_versions (
			tenant_id, knowledge_object_id, object_version,
			app_id, owner_id, object_type, name, sharing_scope, state,
			definition_digest, dependency_count, mutation_kind,
			created_at_unix_micro
		) VALUES (
			'tenant-a', 'ko-dependent', 2, ?, 'owner-a',
			'field_alias', 'dependent', 'private', 'disabled',
			zeroblob(32), 0, 'disable', 30
		);
		INSERT INTO knowledge_object_dependency_seals (
			tenant_id, knowledge_object_id, object_version, dependency_count
		) VALUES ('tenant-a', 'ko-dependent', 2, 0);

		UPDATE knowledge_objects
		SET current_version = 2, state = 'disabled',
		    updated_at_unix_micro = 30, disabled_at_unix_micro = 30
		WHERE tenant_id = 'tenant-a' AND knowledge_object_id = 'ko-dependency-target';
		INSERT INTO knowledge_object_versions (
			tenant_id, knowledge_object_id, object_version,
			app_id, owner_id, object_type, name, sharing_scope, state,
			definition_digest, dependency_count, mutation_kind,
			created_at_unix_micro
		) VALUES (
			'tenant-a', 'ko-dependency-target', 2, ?, 'owner-a',
			'field_extraction', 'dependency-target', 'private', 'disabled',
			zeroblob(32), 0, 'disable', 30
		);
		INSERT INTO knowledge_object_dependency_seals (
			tenant_id, knowledge_object_id, object_version, dependency_count
		) VALUES ('tenant-a', 'ko-dependency-target', 2, 0);
		UPDATE knowledge_catalog_tenants
		SET catalog_revision = catalog_revision + 1
		WHERE tenant_id = 'tenant-a'`,
		knowledgeMigrationTestAppID,
		knowledgeMigrationTestAppID,
	); err != nil {
		_ = cascade.Rollback()
		t.Fatalf("apply dependent-first cascade: %v", err)
	}
	if err := cascade.Commit(); err != nil {
		t.Fatalf("commit dependent-first cascade: %v", err)
	}
	assertIntegerQuery(t, raw, 0, `
		SELECT active_object_count FROM knowledge_catalog_tenants
		WHERE tenant_id = 'tenant-a'`)
	assertNoForeignKeyViolations(t, raw)
}

func TestKnowledgeCatalogSuccessRecordsRequireExactCurrentVersion(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	raw := openKnowledgeMigrationTestDB(t, "knowledge-current-success.sqlite")
	if err := ApplyMigrations(ctx, raw, migrations.SQLite()); err != nil {
		t.Fatalf("apply migrations: %v", err)
	}
	seedKnowledgeMigrationApp(t, raw)
	if _, err := raw.ExecContext(ctx, `
		INSERT INTO knowledge_catalog_tenants (tenant_id) VALUES ('tenant-a');
		INSERT INTO knowledge_definition_blobs (
			tenant_id, definition_digest, definition_proto,
			definition_bytes, created_at_unix_micro
		) VALUES ('tenant-a', zeroblob(32), X'01', 1, 10)`); err != nil {
		t.Fatalf("seed success-record catalog: %v", err)
	}
	insertKnowledgeMigrationObject(t, raw, "ko-current", "current", "private", "active", 10)

	ordinary, err := raw.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin detached ordinary version: %v", err)
	}
	_, ordinaryErr := ordinary.ExecContext(ctx, `
		INSERT INTO knowledge_object_versions (
			tenant_id, knowledge_object_id, object_version,
			app_id, owner_id, object_type, name, sharing_scope, state,
			definition_digest, dependency_count, mutation_kind,
			created_at_unix_micro
		) VALUES (
			'tenant-a', 'ko-current', 2, ?, 'owner-a', 'field_extraction',
			'current', 'private', 'active', zeroblob(32), 0, 'update', 20
		);
		INSERT INTO knowledge_object_dependency_seals (
			tenant_id, knowledge_object_id, object_version, dependency_count
		) VALUES ('tenant-a', 'ko-current', 2, 0);
		INSERT INTO knowledge_mutation_idempotency (
			tenant_id, actor_id, route, client_request_id, mutation_kind,
			request_digest, outcome_proto, committed_catalog_revision,
			knowledge_object_id, object_version,
			created_at_unix_micro, retain_until_unix_micro
		) VALUES (
			'tenant-a', 'owner-a', 'objects.update', 'detached-update-01', 'update',
			zeroblob(32), X'01', 1, 'ko-current', 2,
			10, 604800000010
		)`, knowledgeMigrationTestAppID)
	if ordinaryErr == nil || !strings.Contains(ordinaryErr.Error(), "current version") {
		_ = ordinary.Rollback()
		t.Fatalf("detached ordinary idempotency error = %v", ordinaryErr)
	}
	if err := ordinary.Rollback(); err != nil {
		t.Fatalf("roll back detached ordinary version: %v", err)
	}

	quarantineAudit, err := raw.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin detached quarantine audit: %v", err)
	}
	_, auditErr := quarantineAudit.ExecContext(ctx, `
		INSERT INTO knowledge_object_versions (
			tenant_id, knowledge_object_id, object_version,
			app_id, owner_id, object_type, name, sharing_scope, state,
			definition_digest, dependency_count, mutation_kind,
			quarantine_reason, created_at_unix_micro
		) VALUES (
			'tenant-a', 'ko-current', 2, ?, 'owner-a', 'field_extraction',
			'current', 'private', 'quarantined', NULL, 0, 'quarantine',
			'root_corruption', 20
		);
		INSERT INTO knowledge_object_dependency_seals (
			tenant_id, knowledge_object_id, object_version, dependency_count
		) VALUES ('tenant-a', 'ko-current', 2, 0);
		INSERT INTO knowledge_recovery_audit (
			tenant_id, sequence, knowledge_object_id, object_version,
			actor_kind, actor_id, actor_role, app_id, object_type,
			sharing_scope, recovery_reason, occurred_at_unix_micro
		) VALUES (
			'tenant-a', 1, 'ko-current', 2,
			'browser', 'owner-a', 'administrator', ?, 'field_extraction',
			'private', 'root_corruption', 20
		)`, knowledgeMigrationTestAppID, knowledgeMigrationTestAppID)
	if auditErr == nil || !strings.Contains(auditErr.Error(), "does not match terminal version") {
		_ = quarantineAudit.Rollback()
		t.Fatalf("detached recovery audit error = %v", auditErr)
	}
	if err := quarantineAudit.Rollback(); err != nil {
		t.Fatalf("roll back detached quarantine audit: %v", err)
	}

	quarantineIdempotency, err := raw.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin detached quarantine idempotency: %v", err)
	}
	_, quarantineIDErr := quarantineIdempotency.ExecContext(ctx, `
		INSERT INTO knowledge_object_versions (
			tenant_id, knowledge_object_id, object_version,
			app_id, owner_id, object_type, name, sharing_scope, state,
			definition_digest, dependency_count, mutation_kind,
			quarantine_reason, created_at_unix_micro
		) VALUES (
			'tenant-a', 'ko-current', 2, ?, 'owner-a', 'field_extraction',
			'current', 'private', 'quarantined', NULL, 0, 'quarantine',
			'root_corruption', 20
		);
		INSERT INTO knowledge_object_dependency_seals (
			tenant_id, knowledge_object_id, object_version, dependency_count
		) VALUES ('tenant-a', 'ko-current', 2, 0);
		INSERT INTO knowledge_mutation_idempotency (
			tenant_id, actor_id, route, client_request_id, mutation_kind,
			request_digest, outcome_proto, committed_catalog_revision,
			knowledge_object_id, object_version,
			created_at_unix_micro, retain_until_unix_micro
		) VALUES (
			'tenant-a', 'owner-a', 'objects.quarantine', 'detached-quarantine', 'quarantine',
			zeroblob(32), X'01', 1, 'ko-current', 2,
			10, 604800000010
		)`, knowledgeMigrationTestAppID)
	if quarantineIDErr == nil || !strings.Contains(quarantineIDErr.Error(), "current") {
		_ = quarantineIdempotency.Rollback()
		t.Fatalf("detached quarantine idempotency error = %v", quarantineIDErr)
	}
	if err := quarantineIdempotency.Rollback(); err != nil {
		t.Fatalf("roll back detached quarantine idempotency: %v", err)
	}

	assertIntegerQuery(t, raw, 1, `
		SELECT version_count FROM knowledge_catalog_tenants
		WHERE tenant_id = 'tenant-a'`)
	assertIntegerQuery(t, raw, 0, `
		SELECT idempotency_count FROM knowledge_catalog_tenants
		WHERE tenant_id = 'tenant-a'`)
	assertIntegerQuery(t, raw, 0, `
		SELECT recovery_audit_count FROM knowledge_catalog_tenants
		WHERE tenant_id = 'tenant-a'`)
	assertIntegerQuery(t, raw, 1, `
		SELECT current_version FROM knowledge_objects
		WHERE tenant_id = 'tenant-a' AND knowledge_object_id = 'ko-current'`)
	assertNoForeignKeyViolations(t, raw)
}

func TestKnowledgeCatalogDeferredRegistryVersionAgreementIsAtomic(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	raw := openKnowledgeMigrationTestDB(t, "knowledge-deferred-fk.sqlite")
	if err := ApplyMigrations(ctx, raw, migrations.SQLite()); err != nil {
		t.Fatalf("apply migrations: %v", err)
	}
	seedKnowledgeMigrationApp(t, raw)
	if _, err := raw.ExecContext(ctx, `
		INSERT INTO knowledge_catalog_tenants (tenant_id) VALUES ('tenant-a');
		INSERT INTO knowledge_definition_blobs (
			tenant_id, definition_digest, definition_proto,
			definition_bytes, created_at_unix_micro
		) VALUES
			('tenant-a', zeroblob(32), X'01', 1, 10),
			('tenant-a',
			 X'0101010101010101010101010101010101010101010101010101010101010101',
			 X'02', 1, 10)`); err != nil {
		t.Fatalf("seed catalog and blobs: %v", err)
	}

	// Both directions of the registry/version cycle are deferred, so the
	// immutable version may be inserted before the current registry row.
	tx, err := raw.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin version-first create: %v", err)
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO knowledge_object_versions (
			tenant_id, knowledge_object_id, object_version,
			app_id, owner_id, object_type, name, sharing_scope, state,
			definition_digest, dependency_count, mutation_kind,
			created_at_unix_micro
		) VALUES (
			'tenant-a', 'ko-version-first', 1,
				?, 'owner-a', 'field_extraction', 'version-first', 'private', 'active',
				zeroblob(32), 0, 'create', 10
			);
			INSERT INTO knowledge_object_dependency_seals (
				tenant_id, knowledge_object_id, object_version, dependency_count
			) VALUES ('tenant-a', 'ko-version-first', 1, 0);
			INSERT INTO knowledge_objects (
			tenant_id, knowledge_object_id, current_version,
			app_id, owner_id, object_type, name, sharing_scope, state,
			definition_digest, created_at_unix_micro, updated_at_unix_micro
		) VALUES (
			'tenant-a', 'ko-version-first', 1,
			?, 'owner-a', 'field_extraction', 'version-first', 'private', 'active',
			zeroblob(32), 10, 10
		)`, knowledgeMigrationTestAppID, knowledgeMigrationTestAppID); err != nil {
		_ = tx.Rollback()
		t.Fatalf("insert version-first identity: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit version-first identity: %v", err)
	}

	// A missing version fails at the deferred commit boundary and rolls back
	// both the registry row and its trigger-maintained capacity counters.
	missing, err := raw.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin missing-version create: %v", err)
	}
	if _, err := missing.ExecContext(ctx, `
		INSERT INTO knowledge_objects (
			tenant_id, knowledge_object_id, current_version,
			app_id, owner_id, object_type, name, sharing_scope, state,
			definition_digest, created_at_unix_micro, updated_at_unix_micro
		) VALUES (
			'tenant-a', 'ko-missing-version', 1,
			?, 'owner-a', 'field_extraction', 'missing-version', 'private', 'active',
			zeroblob(32), 20, 20
		)`, knowledgeMigrationTestAppID); err != nil {
		_ = missing.Rollback()
		t.Fatalf("insert missing-version registry: %v", err)
	}
	if err := missing.Commit(); err == nil || !strings.Contains(err.Error(), "FOREIGN KEY") {
		t.Fatalf("missing-version commit error = %v", err)
	}
	assertIntegerQuery(t, raw, 0, `
		SELECT count(*) FROM knowledge_objects
		WHERE tenant_id = 'tenant-a' AND knowledge_object_id = 'ko-missing-version'`)
	assertIntegerQuery(t, raw, 1, `
		SELECT identity_count FROM knowledge_catalog_tenants
		WHERE tenant_id = 'tenant-a'`)
	assertIntegerQuery(t, raw, 1, `
		SELECT version_count FROM knowledge_catalog_tenants
		WHERE tenant_id = 'tenant-a'`)

	// A current version with a declared but incomplete dependency set cannot
	// commit because every current registry row has a deferred seal FK.
	incomplete, err := raw.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin incomplete-dependency create: %v", err)
	}
	if _, err := incomplete.ExecContext(ctx, `
		INSERT INTO knowledge_objects (
			tenant_id, knowledge_object_id, current_version,
			app_id, owner_id, object_type, name, sharing_scope, state,
			definition_digest, created_at_unix_micro, updated_at_unix_micro
		) VALUES (
			'tenant-a', 'ko-incomplete', 1,
			?, 'owner-a', 'field_alias', 'incomplete', 'private', 'active',
			zeroblob(32), 22, 22
		);
		INSERT INTO knowledge_object_versions (
			tenant_id, knowledge_object_id, object_version,
			app_id, owner_id, object_type, name, sharing_scope, state,
			definition_digest, dependency_count, mutation_kind,
			created_at_unix_micro
		) VALUES (
			'tenant-a', 'ko-incomplete', 1,
			?, 'owner-a', 'field_alias', 'incomplete', 'private', 'active',
			zeroblob(32), 1, 'create', 22
		)`, knowledgeMigrationTestAppID, knowledgeMigrationTestAppID); err != nil {
		_ = incomplete.Rollback()
		t.Fatalf("insert incomplete dependency publication: %v", err)
	}
	if err := incomplete.Commit(); err == nil || !strings.Contains(err.Error(), "FOREIGN KEY") {
		t.Fatalf("incomplete dependency commit error = %v", err)
	}
	assertIntegerQuery(t, raw, 0, `
		SELECT count(*) FROM knowledge_objects
		WHERE tenant_id = 'tenant-a' AND knowledge_object_id = 'ko-incomplete'`)
	assertIntegerQuery(t, raw, 0, `
		SELECT count(*) FROM knowledge_object_versions
		WHERE tenant_id = 'tenant-a' AND knowledge_object_id = 'ko-incomplete'`)
	assertIntegerQuery(t, raw, 1, `
		SELECT identity_count FROM knowledge_catalog_tenants
		WHERE tenant_id = 'tenant-a'`)
	assertIntegerQuery(t, raw, 1, `
		SELECT version_count FROM knowledge_catalog_tenants
		WHERE tenant_id = 'tenant-a'`)

	// An error after registry, immutable version, and dependency insertion rolls
	// the entire publication back, including all trigger-maintained counters and
	// the tenant revision. This models a crash/error at the last mutation step.
	fault, err := raw.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin fault-injected publication: %v", err)
	}
	_, faultErr := fault.ExecContext(ctx, `
		INSERT INTO knowledge_objects (
			tenant_id, knowledge_object_id, current_version,
			app_id, owner_id, object_type, name, sharing_scope, state,
			definition_digest, created_at_unix_micro, updated_at_unix_micro
		) VALUES (
			'tenant-a', 'ko-fault', 1,
			?, 'owner-a', 'field_alias', 'fault', 'private', 'active',
			zeroblob(32), 25, 25
		);
		INSERT INTO knowledge_object_versions (
			tenant_id, knowledge_object_id, object_version,
			app_id, owner_id, object_type, name, sharing_scope, state,
			definition_digest, dependency_count, mutation_kind,
			created_at_unix_micro
		) VALUES (
			'tenant-a', 'ko-fault', 1,
			?, 'owner-a', 'field_alias', 'fault', 'private', 'active',
			zeroblob(32), 1, 'create', 25
		);
		INSERT INTO knowledge_object_dependencies (
			tenant_id, source_object_id, source_object_version, ordinal,
			target_kind, target_object_id, target_object_version, dependency_role
		) VALUES (
			'tenant-a', 'ko-fault', 1, 0,
				'object', 'ko-version-first', 1, 'field_input'
			);
			INSERT INTO knowledge_object_dependency_seals (
				tenant_id, knowledge_object_id, object_version, dependency_count
			) VALUES ('tenant-a', 'ko-fault', 1, 1);
			UPDATE knowledge_catalog_tenants
		SET catalog_revision = 2
		WHERE tenant_id = 'tenant-a'`,
		knowledgeMigrationTestAppID,
		knowledgeMigrationTestAppID,
	)
	if faultErr == nil || !strings.Contains(faultErr.Error(), "advance by one") {
		_ = fault.Rollback()
		t.Fatalf("fault-injected publication error = %v", faultErr)
	}
	if err := fault.Rollback(); err != nil {
		t.Fatalf("roll back fault-injected publication: %v", err)
	}
	assertIntegerQuery(t, raw, 0, `
		SELECT count(*) FROM knowledge_objects
		WHERE tenant_id = 'tenant-a' AND knowledge_object_id = 'ko-fault'`)
	assertIntegerQuery(t, raw, 0, `
		SELECT count(*) FROM knowledge_object_versions
		WHERE tenant_id = 'tenant-a' AND knowledge_object_id = 'ko-fault'`)
	assertIntegerQuery(t, raw, 0, `
		SELECT count(*) FROM knowledge_object_dependencies
		WHERE tenant_id = 'tenant-a' AND source_object_id = 'ko-fault'`)
	assertIntegerQuery(t, raw, 0, `
		SELECT count(*) FROM knowledge_object_dependency_seals
		WHERE tenant_id = 'tenant-a' AND knowledge_object_id = 'ko-fault'`)
	assertIntegerQuery(t, raw, 1, `
		SELECT identity_count FROM knowledge_catalog_tenants
		WHERE tenant_id = 'tenant-a'`)
	assertIntegerQuery(t, raw, 1, `
		SELECT version_count FROM knowledge_catalog_tenants
		WHERE tenant_id = 'tenant-a'`)
	assertIntegerQuery(t, raw, 1, `
		SELECT active_object_count FROM knowledge_catalog_tenants
		WHERE tenant_id = 'tenant-a'`)
	assertIntegerQuery(t, raw, 0, `
		SELECT catalog_revision FROM knowledge_catalog_tenants
		WHERE tenant_id = 'tenant-a'`)

	// The exact current row/version agreement includes the digest through a
	// generated NULL-safe key. A mismatch cannot commit in either insert order.
	mismatch, err := raw.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin digest-mismatch create: %v", err)
	}
	if _, err := mismatch.ExecContext(ctx, `
		INSERT INTO knowledge_objects (
			tenant_id, knowledge_object_id, current_version,
			app_id, owner_id, object_type, name, sharing_scope, state,
			definition_digest, created_at_unix_micro, updated_at_unix_micro
		) VALUES (
			'tenant-a', 'ko-digest-mismatch', 1,
			?, 'owner-a', 'field_extraction', 'digest-mismatch', 'private', 'active',
			zeroblob(32), 30, 30
		);
		INSERT INTO knowledge_object_versions (
			tenant_id, knowledge_object_id, object_version,
			app_id, owner_id, object_type, name, sharing_scope, state,
			definition_digest, dependency_count, mutation_kind,
			created_at_unix_micro
		) VALUES (
			'tenant-a', 'ko-digest-mismatch', 1,
			?, 'owner-a', 'field_extraction', 'digest-mismatch', 'private', 'active',
				X'0101010101010101010101010101010101010101010101010101010101010101',
				0, 'create', 30
			);
			INSERT INTO knowledge_object_dependency_seals (
				tenant_id, knowledge_object_id, object_version, dependency_count
			) VALUES ('tenant-a', 'ko-digest-mismatch', 1, 0)`, knowledgeMigrationTestAppID, knowledgeMigrationTestAppID); err != nil {
		_ = mismatch.Rollback()
		t.Fatalf("insert digest mismatch: %v", err)
	}
	if err := mismatch.Commit(); err == nil || !strings.Contains(err.Error(), "FOREIGN KEY") {
		t.Fatalf("digest-mismatch commit error = %v", err)
	}
	assertIntegerQuery(t, raw, 0, `
		SELECT count(*) FROM knowledge_objects
		WHERE tenant_id = 'tenant-a' AND knowledge_object_id = 'ko-digest-mismatch'`)
	assertIntegerQuery(t, raw, 1, `
		SELECT identity_count FROM knowledge_catalog_tenants
		WHERE tenant_id = 'tenant-a'`)
	assertIntegerQuery(t, raw, 1, `
		SELECT version_count FROM knowledge_catalog_tenants
		WHERE tenant_id = 'tenant-a'`)

	assertSQLFailsContaining(t, raw, "CHECK constraint", `
		INSERT INTO knowledge_object_versions (
			tenant_id, knowledge_object_id, object_version,
			app_id, owner_id, object_type, name, sharing_scope, state,
			definition_digest, dependency_count, mutation_kind,
			created_at_unix_micro
		) VALUES (
			'tenant-a', 'ko-invalid-create', 1,
			'app_AAAAAAAAAAAAAAAAAAAAAA', 'owner-a', 'field_extraction',
			'invalid-create', 'private', 'disabled', zeroblob(32), 0, 'create', 40
		)`)
	assertSQLFailsContaining(t, raw, "CHECK constraint", `
		INSERT INTO knowledge_object_versions (
			tenant_id, knowledge_object_id, object_version,
			app_id, owner_id, object_type, name, sharing_scope, state,
			definition_digest, dependency_count, mutation_kind,
			quarantine_reason, created_at_unix_micro
		) VALUES (
			'tenant-a', 'ko-invalid-quarantine', 1,
			'app_AAAAAAAAAAAAAAAAAAAAAA', 'owner-a', 'field_extraction',
			'invalid-quarantine', 'private', 'quarantined', zeroblob(32), 0,
			'quarantine', 'root_corruption', 40
		)`)

	assertNoForeignKeyViolations(t, raw)
}

func openKnowledgeMigrationTestDB(t *testing.T, name string) *sql.DB {
	t.Helper()
	raw, err := sql.Open(
		"sqlite",
		filepath.Join(t.TempDir(), name)+"?_pragma=foreign_keys(1)&_txlock=immediate",
	)
	if err != nil {
		t.Fatalf("open migration database: %v", err)
	}
	t.Cleanup(func() {
		if err := raw.Close(); err != nil {
			t.Errorf("close migration database: %v", err)
		}
	})
	return raw
}

func seedKnowledgeMigrationApp(t *testing.T, db *sql.DB) {
	t.Helper()
	if _, err := db.Exec(`
		INSERT INTO app_workspaces (
			app_id, tenant_id, version, slug, display_name, description,
			default_time_range_present, state,
			created_at_unix_micro, updated_at_unix_micro
		) VALUES (?, 'tenant-a', 1, 'knowledge-migration',
		          'Knowledge migration', '', 0, 'active', 1, 1)`,
		knowledgeMigrationTestAppID,
	); err != nil {
		t.Fatalf("seed app workspace: %v", err)
	}
}

func insertKnowledgeMigrationObject(
	t *testing.T,
	db *sql.DB,
	objectID string,
	name string,
	scope string,
	state string,
	timestamp int64,
) {
	t.Helper()
	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("begin object insert: %v", err)
	}
	disabledAt := "NULL"
	if state == "disabled" {
		disabledAt = fmt.Sprint(timestamp)
	}
	statement := fmt.Sprintf(`
		INSERT INTO knowledge_objects (
			tenant_id, knowledge_object_id, current_version,
			app_id, owner_id, object_type, name, sharing_scope, state,
			definition_digest, created_at_unix_micro, updated_at_unix_micro,
			disabled_at_unix_micro
		) VALUES (
			'tenant-a', ?, 1, ?, 'owner-a', 'field_extraction', ?, ?, ?,
			zeroblob(32), ?, ?, %s
		);
		INSERT INTO knowledge_object_versions (
			tenant_id, knowledge_object_id, object_version,
			app_id, owner_id, object_type, name, sharing_scope, state,
			definition_digest, dependency_count, mutation_kind,
			created_at_unix_micro
			) VALUES (
				'tenant-a', ?, 1, ?, 'owner-a', 'field_extraction', ?, ?, ?,
				zeroblob(32), 0, 'create', ?
			);
			INSERT INTO knowledge_object_dependency_seals (
				tenant_id, knowledge_object_id, object_version, dependency_count
			) VALUES ('tenant-a', ?, 1, 0)`, disabledAt)
	if _, err := tx.Exec(
		statement,
		objectID, knowledgeMigrationTestAppID, name, scope, state, timestamp, timestamp,
		objectID, knowledgeMigrationTestAppID, name, scope, state, timestamp,
		objectID,
	); err != nil {
		_ = tx.Rollback()
		t.Fatalf("insert object %s: %v", objectID, err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit object %s: %v", objectID, err)
	}
}

func assertSQLFailsContaining(t *testing.T, db *sql.DB, want string, query string, args ...any) {
	t.Helper()
	if _, err := db.Exec(query, args...); err == nil || !strings.Contains(err.Error(), want) {
		t.Fatalf("SQL error = %v, want substring %q", err, want)
	}
}

func assertSQLFails(t *testing.T, db *sql.DB, query string, args ...any) {
	t.Helper()
	if _, err := db.Exec(query, args...); err == nil {
		t.Fatal("SQL unexpectedly succeeded")
	}
}

func assertIntegerQuery(t *testing.T, db *sql.DB, want int, query string, args ...any) {
	t.Helper()
	var got int
	if err := db.QueryRow(query, args...).Scan(&got); err != nil {
		t.Fatalf("integer query: %v", err)
	}
	if got != want {
		t.Fatalf("integer query = %d, want %d", got, want)
	}
}

func assertNoForeignKeyViolations(t *testing.T, db *sql.DB) {
	t.Helper()
	rows, err := db.Query(`PRAGMA foreign_key_check`)
	if err != nil {
		t.Fatalf("foreign key check: %v", err)
	}
	defer rows.Close()
	var violations []string
	for rows.Next() {
		var table string
		var rowID sql.NullInt64
		var parent string
		var foreignKey int
		if err := rows.Scan(&table, &rowID, &parent, &foreignKey); err != nil {
			t.Fatalf("scan foreign key violation: %v", err)
		}
		violations = append(violations, fmt.Sprintf("%s:%s:%d", table, parent, foreignKey))
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate foreign key violations: %v", err)
	}
	sort.Strings(violations)
	if len(violations) != 0 {
		t.Fatalf("foreign key violations: %v", violations)
	}
}
