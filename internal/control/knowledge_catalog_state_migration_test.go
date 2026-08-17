package control

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/Suhaibinator/open-splunk/migrations"
)

func TestKnowledgeCatalogStateMigrationBackfillsExactAuthorities(t *testing.T) {
	t.Parallel()

	raw := openKnowledgeMigrationTestDB(t, "knowledge-state-upgrade.sqlite")
	if err := ApplyMigrations(context.Background(), raw, migrationsBefore(t, "0029_")); err != nil {
		t.Fatalf("apply through migration 0028: %v", err)
	}
	seedKnowledgeStatePrerequisites(t, raw)
	insertKnowledgeStateVersion(t, raw, stateVersionFixture{
		objectID: "ko-lifecycle", version: 1, state: "active", mutation: "create", timestamp: 10,
	})
	insertKnowledgeStateVersion(t, raw, stateVersionFixture{
		objectID: "ko-lifecycle", version: 2, state: "disabled", mutation: "disable", timestamp: 20,
		disabledAt: sql.NullInt64{Int64: 20, Valid: true},
	})
	insertKnowledgeStateVersion(t, raw, stateVersionFixture{
		objectID: "ko-lifecycle", version: 3, state: "disabled", mutation: "scope_change", timestamp: 30,
		disabledAt: sql.NullInt64{Int64: 20, Valid: true}, sharingScope: "app",
	})
	if _, err := raw.ExecContext(t.Context(), `UPDATE knowledge_catalog_tenants
		SET catalog_revision = catalog_revision + 1 WHERE tenant_id = 'tenant-a'`); err != nil {
		t.Fatalf("advance pre-0029 revision: %v", err)
	}

	if err := ApplyMigrations(context.Background(), raw, migrations.SQLite()); err != nil {
		t.Fatalf("apply migration 0029: %v", err)
	}
	assertIntegerQuery(t, raw, 39, `SELECT count(*) FROM schema_migrations`)
	assertIntegerQuery(t, raw, 1, `
		SELECT count(*) FROM knowledge_catalog_revision_heads
		WHERE tenant_id = 'tenant-a' AND catalog_revision = 1
		  AND length(state_token) = 32`)
	assertIntegerQuery(t, raw, 3, `
		SELECT count(*) FROM knowledge_object_version_lifecycle
		WHERE tenant_id = 'tenant-a' AND knowledge_object_id = 'ko-lifecycle'`)
	assertIntegerQuery(t, raw, 2, `
		SELECT count(*) FROM knowledge_object_version_lifecycle
		WHERE tenant_id = 'tenant-a' AND knowledge_object_id = 'ko-lifecycle'
		  AND object_version IN (2, 3) AND disabled_at_unix_micro = 20
		  AND quarantined_at_unix_micro IS NULL
		  AND deleted_at_unix_micro IS NULL AND quarantine_reason IS NULL`)
	assertIntegerQuery(t, raw, 1, `
		SELECT count(*) FROM knowledge_object_list_order_keys
		WHERE tenant_id = 'tenant-a' AND knowledge_object_id = 'ko-lifecycle'
		  AND object_version = 3
		  AND created_at_unix_micro = 10 AND updated_at_unix_micro = 30`)
	for _, table := range []string{
		"knowledge_catalog_revision_heads",
		"knowledge_object_version_lifecycle",
		"knowledge_object_list_order_keys",
	} {
		var withoutRowID, strict int
		if err := raw.QueryRowContext(t.Context(), `SELECT wr, strict FROM pragma_table_list WHERE name = ?`, table).
			Scan(&withoutRowID, &strict); err != nil {
			t.Fatalf("inspect %s: %v", table, err)
		}
		if withoutRowID != 1 || strict != 1 {
			t.Fatalf("%s flags = WITHOUT ROWID %d STRICT %d", table, withoutRowID, strict)
		}
	}
	assertNoForeignKeyViolations(t, raw)
}

func TestKnowledgeCatalogStateMigrationRejectsAmbiguousDisabledHistoryAtomically(t *testing.T) {
	t.Parallel()

	raw := openKnowledgeMigrationTestDB(t, "knowledge-state-reject-ambiguous.sqlite")
	if err := ApplyMigrations(context.Background(), raw, migrationsBefore(t, "0029_")); err != nil {
		t.Fatalf("apply through migration 0028: %v", err)
	}
	seedKnowledgeStatePrerequisites(t, raw)
	insertKnowledgeStateVersion(t, raw, stateVersionFixture{
		objectID: "ko-ambiguous", version: 1, state: "active", mutation: "create", timestamp: 10,
	})
	// Migration 0024 admitted a disabled update without a preceding disable
	// mutation.  There is no defensible historical disabled_at value to invent.
	insertKnowledgeStateVersion(t, raw, stateVersionFixture{
		objectID: "ko-ambiguous", version: 2, state: "disabled", mutation: "update", timestamp: 20,
		disabledAt: sql.NullInt64{Int64: 20, Valid: true},
	})

	err := ApplyMigrations(context.Background(), raw, migrations.SQLite())
	if err == nil || !strings.Contains(err.Error(), "CHECK constraint failed: invalid") {
		t.Fatalf("ambiguous lifecycle upgrade error = %v", err)
	}
	assertIntegerQuery(t, raw, 28, `SELECT count(*) FROM schema_migrations`)
	assertIntegerQuery(t, raw, 0, `
		SELECT count(*) FROM sqlite_schema
		WHERE name IN (
			'knowledge_catalog_revision_heads',
			'knowledge_object_version_lifecycle',
			'knowledge_object_list_order_keys'
		)`)
	assertIntegerQuery(t, raw, 1, `
		SELECT count(*) FROM knowledge_objects
		WHERE tenant_id = 'tenant-a' AND knowledge_object_id = 'ko-ambiguous'
		  AND current_version = 2`)
	assertNoForeignKeyViolations(t, raw)
}

func TestKnowledgeCatalogStateMigrationRejectsDisabledHistoryAcrossEnable(t *testing.T) {
	t.Parallel()

	raw := openKnowledgeMigrationTestDB(t, "knowledge-state-reject-cross-enable.sqlite")
	if err := ApplyMigrations(context.Background(), raw, migrationsBefore(t, "0029_")); err != nil {
		t.Fatalf("apply through migration 0028: %v", err)
	}
	seedKnowledgeStatePrerequisites(t, raw)
	for _, fixture := range []stateVersionFixture{
		{objectID: "ko-cross-enable", version: 1, state: "active", mutation: "create", timestamp: 10},
		{objectID: "ko-cross-enable", version: 2, state: "disabled", mutation: "disable", timestamp: 20,
			disabledAt: sql.NullInt64{Int64: 20, Valid: true}},
		{objectID: "ko-cross-enable", version: 3, state: "active", mutation: "enable", timestamp: 30},
		// A later disabled scope-change cannot inherit the marker across the
		// intervening enable transition.
		{objectID: "ko-cross-enable", version: 4, state: "disabled", mutation: "scope_change", timestamp: 40,
			disabledAt: sql.NullInt64{Int64: 40, Valid: true}},
	} {
		insertKnowledgeStateVersion(t, raw, fixture)
	}

	err := ApplyMigrations(context.Background(), raw, migrations.SQLite())
	if err == nil || !strings.Contains(err.Error(), "CHECK constraint failed: invalid") {
		t.Fatalf("cross-enable lifecycle upgrade error = %v", err)
	}
	assertIntegerQuery(t, raw, 28, `SELECT count(*) FROM schema_migrations`)
	assertIntegerQuery(t, raw, 0, `
		SELECT count(*) FROM sqlite_schema
		WHERE name = 'knowledge_object_version_lifecycle'`)
	assertIntegerQuery(t, raw, 4, `
		SELECT current_version FROM knowledge_objects
		WHERE tenant_id = 'tenant-a' AND knowledge_object_id = 'ko-cross-enable'`)
	assertNoForeignKeyViolations(t, raw)
}

func TestKnowledgeCatalogRevisionHeadBackupAndDivergence(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	sourcePath := filepath.Join(t.TempDir(), "revision-head-source.sqlite")
	source, err := Open(ctx, sourcePath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = source.Close() })
	if _, err := source.SQLDB().ExecContext(ctx, `
		INSERT INTO knowledge_catalog_tenants (tenant_id) VALUES ('tenant-a')`); err != nil {
		t.Fatalf("insert tenant: %v", err)
	}
	initial := readKnowledgeRevisionToken(t, source.SQLDB(), "tenant-a", 0)
	if _, err := source.SQLDB().ExecContext(ctx, `
		UPDATE knowledge_catalog_tenants
		SET catalog_revision = catalog_revision + 1
		WHERE tenant_id = 'tenant-a'`); err != nil {
		t.Fatalf("advance source revision: %v", err)
	}
	base := readKnowledgeRevisionToken(t, source.SQLDB(), "tenant-a", 1)
	if bytes.Equal(initial, base) {
		t.Fatal("catalog revision increment retained its prior state token")
	}
	rollbackTx, err := source.SQLDB().BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := rollbackTx.ExecContext(ctx, `
		UPDATE knowledge_catalog_tenants
		SET catalog_revision = catalog_revision + 1
		WHERE tenant_id = 'tenant-a'`); err != nil {
		_ = rollbackTx.Rollback()
		t.Fatalf("stage revision rotation for rollback: %v", err)
	}
	var staged []byte
	if err := rollbackTx.QueryRowContext(ctx, `
		SELECT state_token FROM knowledge_catalog_revision_heads
		WHERE tenant_id = 'tenant-a' AND catalog_revision = 2`).Scan(&staged); err != nil {
		_ = rollbackTx.Rollback()
		t.Fatalf("read staged revision token: %v", err)
	}
	if len(staged) != 32 || bytes.Equal(staged, base) {
		_ = rollbackTx.Rollback()
		t.Fatal("staged revision did not atomically rotate its token")
	}
	if err := rollbackTx.Rollback(); err != nil {
		t.Fatalf("roll back staged revision: %v", err)
	}
	if got := readKnowledgeRevisionToken(t, source.SQLDB(), "tenant-a", 1); !bytes.Equal(got, base) {
		t.Fatal("rolled-back revision rotation changed the committed token")
	}

	branchAPath := filepath.Join(t.TempDir(), "revision-head-a.sqlite")
	branchBPath := filepath.Join(t.TempDir(), "revision-head-b.sqlite")
	if err := source.BackupTo(ctx, branchAPath); err != nil {
		t.Fatalf("backup branch A: %v", err)
	}
	if err := source.BackupTo(ctx, branchBPath); err != nil {
		t.Fatalf("backup branch B: %v", err)
	}
	for label, path := range map[string]string{"A": branchAPath, "B": branchBPath} {
		branch, err := Open(ctx, path)
		if err != nil {
			t.Fatalf("open branch %s: %v", label, err)
		}
		if got := readKnowledgeRevisionToken(t, branch.SQLDB(), "tenant-a", 1); !bytes.Equal(got, base) {
			_ = branch.Close()
			t.Fatalf("branch %s backup changed exact revision state token", label)
		}
		if _, err := branch.SQLDB().ExecContext(ctx, `
			UPDATE knowledge_catalog_tenants
			SET catalog_revision = catalog_revision + 1
			WHERE tenant_id = 'tenant-a'`); err != nil {
			_ = branch.Close()
			t.Fatalf("advance branch %s: %v", label, err)
		}
		if err := branch.Close(); err != nil {
			t.Fatalf("close branch %s: %v", label, err)
		}
	}
	branchA, err := Open(ctx, branchAPath)
	if err != nil {
		t.Fatal(err)
	}
	defer branchA.Close()
	branchB, err := Open(ctx, branchBPath)
	if err != nil {
		t.Fatal(err)
	}
	defer branchB.Close()
	tokenA := readKnowledgeRevisionToken(t, branchA.SQLDB(), "tenant-a", 2)
	tokenB := readKnowledgeRevisionToken(t, branchB.SQLDB(), "tenant-a", 2)
	if bytes.Equal(tokenA, tokenB) {
		t.Fatal("independently advanced equal-number revisions shared a state token")
	}

	assertSQLFailsContaining(t, source.SQLDB(), "transition is invalid", `
		UPDATE knowledge_catalog_revision_heads SET state_token = randomblob(32)
		WHERE tenant_id = 'tenant-a'`)
	assertSQLFailsContaining(t, source.SQLDB(), "cannot be deleted", `
		DELETE FROM knowledge_catalog_revision_heads WHERE tenant_id = 'tenant-a'`)
	if got := readKnowledgeRevisionToken(t, source.SQLDB(), "tenant-a", 1); !bytes.Equal(got, base) {
		t.Fatal("rejected revision-head mutations changed the committed token")
	}
}

func TestKnowledgeVersionLifecycleAndListOrderKeysAreExactAndImmutable(t *testing.T) {
	t.Parallel()

	raw := openKnowledgeMigrationTestDB(t, "knowledge-state-runtime.sqlite")
	if err := ApplyMigrations(context.Background(), raw, migrations.SQLite()); err != nil {
		t.Fatal(err)
	}
	seedKnowledgeStatePrerequisites(t, raw)
	for _, fixture := range []stateVersionFixture{
		{objectID: "ko-history", version: 1, state: "active", mutation: "create", timestamp: 10},
		{objectID: "ko-history", version: 2, state: "disabled", mutation: "disable", timestamp: 20,
			disabledAt: sql.NullInt64{Int64: 20, Valid: true}},
		{objectID: "ko-history", version: 3, state: "disabled", mutation: "scope_change", timestamp: 30,
			disabledAt: sql.NullInt64{Int64: 20, Valid: true}, sharingScope: "app"},
		{objectID: "ko-history", version: 4, state: "active", mutation: "enable", timestamp: 40, sharingScope: "app"},
		{objectID: "ko-history", version: 5, state: "deleted", mutation: "delete", timestamp: 50,
			deletedAt: sql.NullInt64{Int64: 50, Valid: true}, sharingScope: "app"},
		{objectID: "ko-quarantine", version: 1, state: "active", mutation: "create", timestamp: 60},
		{objectID: "ko-quarantine", version: 2, state: "quarantined", mutation: "quarantine", timestamp: 70,
			quarantinedAt: sql.NullInt64{Int64: 70, Valid: true}, quarantineReason: "root_corruption"},
	} {
		insertKnowledgeStateVersion(t, raw, fixture)
	}

	assertIntegerQuery(t, raw, 2, `
		SELECT count(*) FROM knowledge_object_version_lifecycle
		WHERE tenant_id = 'tenant-a' AND knowledge_object_id = 'ko-history'
		  AND object_version IN (2, 3) AND disabled_at_unix_micro = 20`)
	assertIntegerQuery(t, raw, 1, `
		SELECT count(*) FROM knowledge_object_version_lifecycle
		WHERE tenant_id = 'tenant-a' AND knowledge_object_id = 'ko-history'
		  AND object_version = 5 AND deleted_at_unix_micro = 50`)
	assertIntegerQuery(t, raw, 1, `
		SELECT count(*) FROM knowledge_object_version_lifecycle
		WHERE tenant_id = 'tenant-a' AND knowledge_object_id = 'ko-quarantine'
		  AND object_version = 2 AND quarantined_at_unix_micro = 70
		  AND quarantine_reason = 'root_corruption'`)
	assertIntegerQuery(t, raw, 1, `
		SELECT count(*) FROM knowledge_object_list_order_keys
		WHERE tenant_id = 'tenant-a' AND knowledge_object_id = 'ko-history'
		  AND object_version = 5
		  AND created_at_unix_micro = 10 AND updated_at_unix_micro = 50`)
	assertIntegerQuery(t, raw, 1, `
		SELECT count(*) FROM knowledge_object_list_order_keys
		WHERE tenant_id = 'tenant-a' AND knowledge_object_id = 'ko-quarantine'
		  AND object_version = 2
		  AND created_at_unix_micro = 60 AND updated_at_unix_micro = 70`)

	assertSQLFailsContaining(t, raw, "immutable", `
		UPDATE knowledge_object_version_lifecycle SET disabled_at_unix_micro = 1
		WHERE tenant_id = 'tenant-a' AND knowledge_object_id = 'ko-history'
		  AND object_version = 1`)
	assertSQLFailsContaining(t, raw, "cannot be deleted", `
		DELETE FROM knowledge_object_version_lifecycle
		WHERE tenant_id = 'tenant-a' AND knowledge_object_id = 'ko-history'
		  AND object_version = 1`)
	assertSQLFailsContaining(t, raw, "already exists", `
		INSERT OR REPLACE INTO knowledge_object_version_lifecycle (
			tenant_id, knowledge_object_id, object_version, state,
			disabled_at_unix_micro, quarantined_at_unix_micro,
			deleted_at_unix_micro, quarantine_reason
		) VALUES ('tenant-a', 'ko-history', 1, 'active', NULL, NULL, NULL, NULL)`)
	assertSQLFailsContaining(t, raw, "immutable", `
		UPDATE knowledge_object_list_order_keys SET updated_at_unix_micro = 49
		WHERE tenant_id = 'tenant-a' AND knowledge_object_id = 'ko-history'
		  AND object_version = 5`)
	assertSQLFailsContaining(t, raw, "cannot be deleted", `
		DELETE FROM knowledge_object_list_order_keys
		WHERE tenant_id = 'tenant-a' AND knowledge_object_id = 'ko-history'
		  AND object_version = 5`)
	assertNoForeignKeyViolations(t, raw)
}

type stateVersionFixture struct {
	objectID         string
	version          int
	state            string
	mutation         string
	timestamp        int64
	disabledAt       sql.NullInt64
	quarantinedAt    sql.NullInt64
	deletedAt        sql.NullInt64
	quarantineReason string
	sharingScope     string
}

type knowledgeStateFixtureExecer interface {
	Exec(query string, args ...any) (sql.Result, error)
	QueryRow(query string, args ...any) *sql.Row
}

func seedKnowledgeStatePrerequisites(t *testing.T, db *sql.DB) {
	t.Helper()
	seedKnowledgeMigrationApp(t, db)
	if _, err := db.ExecContext(t.Context(), `
		INSERT INTO knowledge_catalog_tenants (tenant_id)
		SELECT 'tenant-a'
		WHERE NOT EXISTS (
			SELECT 1 FROM knowledge_catalog_tenants WHERE tenant_id = 'tenant-a'
		);
		INSERT INTO knowledge_definition_blobs (
			tenant_id, definition_digest, definition_proto,
			definition_bytes, created_at_unix_micro
		) VALUES ('tenant-a', zeroblob(32), X'01', 1, 1);
		INSERT INTO knowledge_projection_tenant_ledgers (tenant_id)
		SELECT 'tenant-a'
		WHERE NOT EXISTS (
			SELECT 1 FROM knowledge_projection_tenant_ledgers
			WHERE tenant_id = 'tenant-a'
		)`); err != nil {
		t.Fatalf("seed knowledge state prerequisites: %v", err)
	}
}

func insertKnowledgeStateVersion(t *testing.T, db *sql.DB, fixture stateVersionFixture) {
	t.Helper()
	tx, err := db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	rollback := func(format string, args ...any) {
		_ = tx.Rollback()
		t.Fatalf(format, args...)
	}
	digest := knowledgeStateFixtureDefinitionDigest(t, tx, fixture)
	canonicalSelectorBytes := 46
	if fixture.state == "quarantined" {
		canonicalSelectorBytes = 0
	}
	reason := any(nil)
	if fixture.quarantineReason != "" {
		reason = fixture.quarantineReason
	}
	sharingScope := fixture.sharingScope
	if sharingScope == "" {
		sharingScope = "private"
	}
	if _, err := tx.ExecContext(t.Context(), `
		INSERT INTO knowledge_object_versions (
			tenant_id, knowledge_object_id, object_version,
			app_id, owner_id, object_type, name, sharing_scope, state,
			definition_digest, dependency_count, mutation_kind,
			quarantine_reason, created_at_unix_micro
		) VALUES (
			'tenant-a', ?, ?, ?, 'owner-a', 'field_extraction', ?,
			?, ?, ?, 0, ?, ?, ?
		);
		INSERT INTO knowledge_object_dependency_seals (
			tenant_id, knowledge_object_id, object_version, dependency_count
		) VALUES ('tenant-a', ?, ?, 0)`,
		fixture.objectID, fixture.version, knowledgeMigrationTestAppID, fixture.objectID, sharingScope,
		fixture.state, digest, fixture.mutation, reason, fixture.timestamp,
		fixture.objectID, fixture.version,
	); err != nil {
		rollback("insert version %s/%d: %v", fixture.objectID, fixture.version, err)
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
			?, ?, 0, '', 0, 0, 0, 0, 0, ?
		);
		INSERT INTO knowledge_object_list_projection_seals (
			tenant_id, knowledge_object_id, object_version,
			projection_bytes, canonical_selector_bytes
		) SELECT tenant_id, knowledge_object_id, object_version,
		         projection_bytes, canonical_selector_bytes
		    FROM knowledge_object_list_projections
		   WHERE tenant_id = 'tenant-a' AND knowledge_object_id = ?
		     AND object_version = ?`,
		fixture.objectID, fixture.version, knowledgeMigrationTestAppID, fixture.objectID, sharingScope,
		fixture.state, canonicalSelectorBytes, fixture.objectID, fixture.version,
	); err != nil {
		rollback("insert projection %s/%d: %v", fixture.objectID, fixture.version, err)
	}
	if fixture.version == 1 {
		if _, err := tx.ExecContext(t.Context(), `
			INSERT INTO knowledge_objects (
				tenant_id, knowledge_object_id, current_version,
				app_id, owner_id, object_type, name, sharing_scope, state,
				definition_digest, created_at_unix_micro, updated_at_unix_micro,
				disabled_at_unix_micro, quarantined_at_unix_micro,
				deleted_at_unix_micro, quarantine_reason
			) VALUES (
				'tenant-a', ?, 1, ?, 'owner-a', 'field_extraction', ?,
				?, ?, ?, ?, ?, ?, ?, ?, ?
			)`, fixture.objectID, knowledgeMigrationTestAppID, fixture.objectID,
			sharingScope, fixture.state, digest, fixture.timestamp, fixture.timestamp,
			nullInt64Value(fixture.disabledAt), nullInt64Value(fixture.quarantinedAt),
			nullInt64Value(fixture.deletedAt), reason,
		); err != nil {
			rollback("insert registry %s: %v", fixture.objectID, err)
		}
	} else {
		result, err := tx.ExecContext(t.Context(), `
			UPDATE knowledge_objects SET
				current_version = ?, sharing_scope = ?, state = ?, definition_digest = ?,
				updated_at_unix_micro = ?, disabled_at_unix_micro = ?,
				quarantined_at_unix_micro = ?, deleted_at_unix_micro = ?,
				quarantine_reason = ?
			WHERE tenant_id = 'tenant-a' AND knowledge_object_id = ?`,
			fixture.version, sharingScope, fixture.state, digest, fixture.timestamp,
			nullInt64Value(fixture.disabledAt), nullInt64Value(fixture.quarantinedAt),
			nullInt64Value(fixture.deletedAt), reason, fixture.objectID,
		)
		if err != nil {
			rollback("update registry %s/%d: %v", fixture.objectID, fixture.version, err)
		}
		if affected, err := result.RowsAffected(); err != nil || affected != 1 {
			rollback("update registry %s/%d affected %d: %v", fixture.objectID, fixture.version, affected, err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit version %s/%d: %v", fixture.objectID, fixture.version, err)
	}
}

func knowledgeStateFixtureDefinitionDigest(
	t *testing.T,
	exec knowledgeStateFixtureExecer,
	fixture stateVersionFixture,
) any {
	t.Helper()
	if fixture.state == "quarantined" {
		return nil
	}
	if fixture.mutation == "update" || fixture.mutation == "scope_change" {
		body := []byte("knowledge-state-fixture/" + fixture.objectID + "/" +
			strconv.Itoa(fixture.version))
		digest := sha256.Sum256(body)
		if _, err := exec.Exec(`
			INSERT INTO knowledge_definition_blobs (
				tenant_id, definition_digest, definition_proto,
				definition_bytes, created_at_unix_micro
			) VALUES ('tenant-a', ?, ?, ?, ?)`,
			digest[:], body, len(body), fixture.timestamp,
		); err != nil {
			t.Fatalf("insert definition blob %s/%d: %v", fixture.objectID, fixture.version, err)
		}
		return digest[:]
	}
	if fixture.version > 1 {
		var previousDigest []byte
		if err := exec.QueryRow(`
			SELECT definition_digest
			FROM knowledge_object_versions
			WHERE tenant_id = 'tenant-a' AND knowledge_object_id = ?
			  AND object_version = ?`, fixture.objectID, fixture.version-1,
		).Scan(&previousDigest); err != nil {
			t.Fatalf("read prior definition digest %s/%d: %v", fixture.objectID, fixture.version, err)
		}
		if len(previousDigest) != sha256.Size {
			t.Fatalf("prior definition digest %s/%d has %d bytes", fixture.objectID, fixture.version, len(previousDigest))
		}
		return previousDigest
	}
	return make([]byte, sha256.Size)
}

func nullInt64Value(value sql.NullInt64) any {
	if !value.Valid {
		return nil
	}
	return value.Int64
}

func readKnowledgeRevisionToken(t *testing.T, db *sql.DB, tenantID string, revision int64) []byte {
	t.Helper()
	var token []byte
	if err := db.QueryRowContext(t.Context(), `
		SELECT state_token FROM knowledge_catalog_revision_heads
		WHERE tenant_id = ? AND catalog_revision = ?`, tenantID, revision).Scan(&token); err != nil {
		t.Fatalf("read revision token for %s/%d: %v", tenantID, revision, err)
	}
	if len(token) != 32 {
		t.Fatalf("revision token length = %d, want 32", len(token))
	}
	return bytes.Clone(token)
}
