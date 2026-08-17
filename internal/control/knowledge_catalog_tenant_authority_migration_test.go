package control

import (
	"bytes"
	"context"
	"database/sql"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Suhaibinator/open-splunk/migrations"
)

const (
	knowledgeAuthoritySecondAppID = "app_BBBBBBBBBBBBBBBBBBBBBA"
	knowledgeAuthorityThirdAppID  = "app_CCCCCCCCCCCCCCCCCCCCCA"
)

func TestKnowledgeCatalogTenantAuthorityMigrationBackfillsAndPreservesState(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	raw := openKnowledgeMigrationTestDB(t, "knowledge-tenant-authority-upgrade.sqlite")
	if err := ApplyMigrations(ctx, raw, migrationsBefore(t, "0031_")); err != nil {
		t.Fatalf("apply through migration 0030: %v", err)
	}
	insertKnowledgeAuthorityTestApp(t, raw, knowledgeMigrationTestAppID, "tenant-a", "tenant-a")
	insertKnowledgeAuthorityTestApp(t, raw, knowledgeAuthoritySecondAppID, "tenant-b", "tenant-b")

	if _, err := raw.ExecContext(t.Context(), `
		INSERT INTO knowledge_catalog_tenants (tenant_id) VALUES ('tenant-a');
		INSERT INTO knowledge_projection_tenant_ledgers (tenant_id) VALUES ('tenant-a');
		UPDATE knowledge_catalog_tenants
		SET catalog_revision = catalog_revision + 1
		WHERE tenant_id = 'tenant-a';
		INSERT INTO knowledge_catalog_tenants (tenant_id) VALUES ('tenant-without-app')`); err != nil {
		t.Fatalf("seed existing knowledge authorities: %v", err)
	}
	existingToken := readKnowledgeRevisionToken(t, raw, "tenant-a", 1)
	unrelatedToken := readKnowledgeRevisionToken(t, raw, "tenant-without-app", 0)

	if err := ApplyMigrations(ctx, raw, migrations.SQLite()); err != nil {
		t.Fatalf("apply migration 0031: %v", err)
	}
	assertIntegerQuery(t, raw, 39, `SELECT count(*) FROM schema_migrations`)
	assertIntegerQuery(t, raw, 2, `
		SELECT count(*)
		FROM app_catalog_revisions AS authority
		JOIN knowledge_catalog_tenants AS tenant
		  ON tenant.tenant_id = authority.tenant_id
		JOIN knowledge_catalog_revision_heads AS head
		  ON head.tenant_id = tenant.tenant_id
		 AND head.catalog_revision = tenant.catalog_revision
		JOIN knowledge_projection_tenant_ledgers AS ledger
		  ON ledger.tenant_id = tenant.tenant_id
		WHERE length(head.state_token) = 32`)
	assertIntegerQuery(t, raw, 1, `
		SELECT count(*)
		FROM knowledge_catalog_tenants AS tenant
		JOIN knowledge_catalog_revision_heads AS head
		  ON head.tenant_id = tenant.tenant_id
		 AND head.catalog_revision = tenant.catalog_revision
		JOIN knowledge_projection_tenant_ledgers AS ledger
		  ON ledger.tenant_id = tenant.tenant_id
		WHERE tenant.tenant_id = 'tenant-b'
		  AND tenant.catalog_revision = 0
		  AND length(head.state_token) = 32
		  AND ledger.projection_bytes = 0`)
	if got := readKnowledgeRevisionToken(t, raw, "tenant-a", 1); !bytes.Equal(got, existingToken) {
		t.Fatal("migration replaced an existing tenant's revision token")
	}
	if got := readKnowledgeRevisionToken(t, raw, "tenant-without-app", 0); !bytes.Equal(got, unrelatedToken) {
		t.Fatal("migration replaced an unrelated tenant's revision token")
	}
	assertIntegerQuery(t, raw, 0, `
		SELECT count(*) FROM knowledge_projection_tenant_ledgers
		WHERE tenant_id = 'tenant-without-app'`)

	backfilledToken := readKnowledgeRevisionToken(t, raw, "tenant-b", 0)
	if err := ApplyMigrations(ctx, raw, migrations.SQLite()); err != nil {
		t.Fatalf("reapply migrations: %v", err)
	}
	if got := readKnowledgeRevisionToken(t, raw, "tenant-b", 0); !bytes.Equal(got, backfilledToken) {
		t.Fatal("idempotent migration run replaced the backfilled revision token")
	}

	backupPath := filepath.Join(t.TempDir(), "knowledge-tenant-authority-backup.sqlite")
	if err := (&DB{sql: raw}).BackupTo(ctx, backupPath); err != nil {
		t.Fatalf("backup migrated catalog: %v", err)
	}
	backup, err := OpenReadOnly(ctx, backupPath)
	if err != nil {
		t.Fatalf("open catalog backup: %v", err)
	}
	t.Cleanup(func() { _ = backup.Close() })
	if got := readKnowledgeRevisionToken(t, backup.SQLDB(), "tenant-b", 0); !bytes.Equal(got, backfilledToken) {
		t.Fatal("backup changed the backfilled revision-zero token")
	}
	assertNoForeignKeyViolations(t, raw)
}

func TestKnowledgeCatalogTenantAuthorityProvisioningIsAtomic(t *testing.T) {
	t.Parallel()

	t.Run("first app provisions complete authority", func(t *testing.T) {
		t.Parallel()
		raw := openKnowledgeMigrationTestDB(t, "knowledge-tenant-authority-future.sqlite")
		if err := ApplyMigrations(context.Background(), raw, migrations.SQLite()); err != nil {
			t.Fatalf("apply migrations: %v", err)
		}

		insertKnowledgeAuthorityTestApp(
			t, raw, knowledgeAuthoritySecondAppID, "tenant-future", "tenant-future",
		)
		assertIntegerQuery(t, raw, 1, `
			SELECT count(*)
			FROM app_catalog_revisions AS authority
			JOIN knowledge_catalog_tenants AS tenant
			  ON tenant.tenant_id = authority.tenant_id
			JOIN knowledge_catalog_revision_heads AS head
			  ON head.tenant_id = tenant.tenant_id
			 AND head.catalog_revision = tenant.catalog_revision
			JOIN knowledge_projection_tenant_ledgers AS ledger
			  ON ledger.tenant_id = tenant.tenant_id
			WHERE authority.tenant_id = 'tenant-future'
			  AND authority.revision = 1
			  AND tenant.catalog_revision = 0
			  AND length(head.state_token) = 32
			  AND ledger.projection_bytes = 0`)
		assertNoForeignKeyViolations(t, raw)
	})

	t.Run("corrupt prestate rolls back first app", func(t *testing.T) {
		t.Parallel()
		raw := openKnowledgeMigrationTestDB(t, "knowledge-tenant-authority-rollback.sqlite")
		if err := ApplyMigrations(context.Background(), raw, migrations.SQLite()); err != nil {
			t.Fatalf("apply migrations: %v", err)
		}
		if _, err := raw.ExecContext(t.Context(), `
			INSERT INTO knowledge_catalog_tenants (tenant_id) VALUES ('tenant-corrupt');
			DROP TRIGGER knowledge_catalog_revision_head_delete_is_forbidden;
			DELETE FROM knowledge_catalog_revision_heads
			WHERE tenant_id = 'tenant-corrupt'`); err != nil {
			t.Fatalf("seed corrupt authority: %v", err)
		}

		err := insertKnowledgeAuthorityTestAppError(
			raw, knowledgeAuthorityThirdAppID, "tenant-corrupt", "tenant-corrupt",
		)
		if err == nil || !strings.Contains(err.Error(), "incomplete or corrupt") {
			t.Fatalf("first app error = %v, want corrupt-authority rejection", err)
		}
		assertIntegerQuery(t, raw, 0, `
			SELECT count(*) FROM app_workspaces
			WHERE tenant_id = 'tenant-corrupt'`)
		assertIntegerQuery(t, raw, 0, `
			SELECT count(*) FROM app_catalog_revisions
			WHERE tenant_id = 'tenant-corrupt'`)
		assertIntegerQuery(t, raw, 0, `
			SELECT count(*) FROM knowledge_projection_tenant_ledgers
			WHERE tenant_id = 'tenant-corrupt'`)
	})

	t.Run("orphan ledger is not concealed", func(t *testing.T) {
		t.Parallel()
		raw := openKnowledgeMigrationTestDB(t, "knowledge-tenant-authority-orphan.sqlite")
		if err := ApplyMigrations(context.Background(), raw, migrations.SQLite()); err != nil {
			t.Fatalf("apply migrations: %v", err)
		}
		connection, err := raw.Conn(context.Background())
		if err != nil {
			t.Fatalf("open corruption connection: %v", err)
		}
		defer connection.Close()
		if _, err := connection.ExecContext(context.Background(), `PRAGMA foreign_keys = OFF`); err != nil {
			t.Fatalf("disable foreign keys: %v", err)
		}
		if _, err := connection.ExecContext(context.Background(), `
			INSERT INTO knowledge_projection_tenant_ledgers (tenant_id)
			VALUES ('tenant-orphan')`); err != nil {
			t.Fatalf("seed orphan ledger: %v", err)
		}
		if _, err := connection.ExecContext(context.Background(), `PRAGMA foreign_keys = ON`); err != nil {
			t.Fatalf("restore foreign keys: %v", err)
		}

		err = insertKnowledgeAuthorityTestAppError(
			raw, knowledgeAuthorityThirdAppID, "tenant-orphan", "tenant-orphan",
		)
		if err == nil || !strings.Contains(err.Error(), "prestate is incomplete or corrupt") {
			t.Fatalf("first app error = %v, want orphan-authority rejection", err)
		}
		assertIntegerQuery(t, raw, 0, `
			SELECT count(*) FROM app_workspaces
			WHERE tenant_id = 'tenant-orphan'`)
		assertIntegerQuery(t, raw, 0, `
			SELECT count(*) FROM app_catalog_revisions
			WHERE tenant_id = 'tenant-orphan'`)
		assertIntegerQuery(t, raw, 0, `
			SELECT count(*) FROM knowledge_catalog_tenants
			WHERE tenant_id = 'tenant-orphan'`)
		assertIntegerQuery(t, raw, 1, `
			SELECT count(*) FROM knowledge_projection_tenant_ledgers
			WHERE tenant_id = 'tenant-orphan'`)
	})
}

func TestAppCatalogRevisionAuthorityIsMonotonicAndCollisionProof(t *testing.T) {
	t.Parallel()

	raw := openKnowledgeMigrationTestDB(t, "app-catalog-authority-guards.sqlite")
	if err := ApplyMigrations(context.Background(), raw, migrations.SQLite()); err != nil {
		t.Fatalf("apply migrations: %v", err)
	}
	insertKnowledgeAuthorityTestApp(t, raw, knowledgeMigrationTestAppID, "tenant-a", "tenant-a")
	originalToken := readKnowledgeRevisionToken(t, raw, "tenant-a", 0)
	assertIntegerQuery(t, raw, 1, `
		SELECT revision FROM app_catalog_revisions WHERE tenant_id = 'tenant-a'`)

	tests := []struct {
		name      string
		statement string
		want      string
	}{
		{name: "tenant rekey", statement: `UPDATE app_catalog_revisions SET tenant_id = 'tenant-rekeyed' WHERE tenant_id = 'tenant-a'`, want: "tenant is immutable"},
		{name: "revision reuse", statement: `UPDATE app_catalog_revisions SET revision = revision WHERE tenant_id = 'tenant-a'`, want: "advance by exactly one"},
		{name: "revision decrement", statement: `UPDATE app_catalog_revisions SET revision = revision - 1 WHERE tenant_id = 'tenant-a'`, want: "advance by exactly one"},
		{name: "revision skip", statement: `UPDATE app_catalog_revisions SET revision = revision + 2 WHERE tenant_id = 'tenant-a'`, want: "advance by exactly one"},
		{name: "replace collision", statement: `INSERT OR REPLACE INTO app_catalog_revisions (tenant_id, revision) VALUES ('tenant-a', 1)`, want: "already exists"},
		{name: "upsert collision", statement: `INSERT INTO app_catalog_revisions (tenant_id, revision) VALUES ('tenant-a', 1) ON CONFLICT (tenant_id) DO UPDATE SET revision = excluded.revision`, want: "already exists"},
		{name: "delete", statement: `DELETE FROM app_catalog_revisions WHERE tenant_id = 'tenant-a'`, want: "cannot be deleted"},
		{name: "orphan initial authority", statement: `INSERT INTO app_catalog_revisions (tenant_id, revision) VALUES ('tenant-ghost', 1)`, want: "must begin with its first app"},
		{name: "noninitial authority", statement: `INSERT INTO app_catalog_revisions (tenant_id, revision) VALUES ('tenant-ghost', 2)`, want: "must begin with its first app"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := raw.ExecContext(t.Context(), test.statement); err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("authority mutation error = %v, want %q", err, test.want)
			}
			assertIntegerQuery(t, raw, 1, `
				SELECT revision FROM app_catalog_revisions WHERE tenant_id = 'tenant-a'`)
			assertIntegerQuery(t, raw, 0, `
				SELECT count(*) FROM app_catalog_revisions WHERE tenant_id <> 'tenant-a'`)
		})
	}

	insertKnowledgeAuthorityTestApp(t, raw, knowledgeAuthoritySecondAppID, "tenant-a", "tenant-second")
	assertIntegerQuery(t, raw, 2, `
		SELECT revision FROM app_catalog_revisions WHERE tenant_id = 'tenant-a'`)
	if got := readKnowledgeRevisionToken(t, raw, "tenant-a", 0); !bytes.Equal(got, originalToken) {
		t.Fatal("second app replaced the tenant's revision-zero knowledge token")
	}
	assertNoForeignKeyViolations(t, raw)
}

func TestKnowledgeCatalogTenantAuthorityMigrationRejectsCorruptHeadAtomically(t *testing.T) {
	t.Parallel()

	raw := openKnowledgeMigrationTestDB(t, "knowledge-tenant-authority-reject.sqlite")
	if err := ApplyMigrations(context.Background(), raw, migrationsBefore(t, "0031_")); err != nil {
		t.Fatalf("apply through migration 0030: %v", err)
	}
	insertKnowledgeAuthorityTestApp(t, raw, knowledgeMigrationTestAppID, "tenant-a", "tenant-a")
	if _, err := raw.ExecContext(t.Context(), `
		INSERT INTO knowledge_catalog_tenants (tenant_id) VALUES ('tenant-a');
		DROP TRIGGER knowledge_catalog_revision_head_delete_is_forbidden;
		DELETE FROM knowledge_catalog_revision_heads WHERE tenant_id = 'tenant-a'`); err != nil {
		t.Fatalf("seed corrupt revision head: %v", err)
	}

	err := ApplyMigrations(context.Background(), raw, migrations.SQLite())
	if err == nil || !strings.Contains(err.Error(), "CHECK constraint failed: invalid") {
		t.Fatalf("migration error = %v, want corruption rejection", err)
	}
	assertIntegerQuery(t, raw, 30, `SELECT count(*) FROM schema_migrations`)
	assertIntegerQuery(t, raw, 0, `
		SELECT count(*) FROM knowledge_projection_tenant_ledgers
		WHERE tenant_id = 'tenant-a'`)
	assertIntegerQuery(t, raw, 0, `
		SELECT count(*) FROM sqlite_schema
		WHERE name = 'app_catalog_revision_provisions_knowledge_catalog_after_insert'`)
}

func TestKnowledgeCatalogTenantAuthorityMigrationRejectsMissingAppAuthorityAtomically(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name   string
		mutate string
	}{
		{name: "deleted", mutate: `DELETE FROM app_catalog_revisions WHERE tenant_id = 'tenant-a'`},
		{name: "rekeyed", mutate: `UPDATE app_catalog_revisions SET tenant_id = 'tenant-rekeyed' WHERE tenant_id = 'tenant-a'`},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			raw := openKnowledgeMigrationTestDB(t, "knowledge-tenant-authority-missing-"+test.name+".sqlite")
			if err := ApplyMigrations(context.Background(), raw, migrationsBefore(t, "0031_")); err != nil {
				t.Fatalf("apply through migration 0030: %v", err)
			}
			insertKnowledgeAuthorityTestApp(t, raw, knowledgeMigrationTestAppID, "tenant-a", "tenant-a")
			if _, err := raw.ExecContext(t.Context(), test.mutate); err != nil {
				t.Fatalf("seed missing app authority: %v", err)
			}

			err := ApplyMigrations(context.Background(), raw, migrations.SQLite())
			if err == nil || !strings.Contains(err.Error(), "CHECK constraint failed: invalid") {
				t.Fatalf("migration error = %v, want app-authority rejection", err)
			}
			assertIntegerQuery(t, raw, 30, `SELECT count(*) FROM schema_migrations`)
			assertIntegerQuery(t, raw, 0, `
				SELECT count(*) FROM sqlite_schema
				WHERE name = 'app_catalog_revision_identity_collision_is_forbidden'`)
		})
	}
}

func TestAppCatalogRevisionMaximumRollsBackOuterAppMutation(t *testing.T) {
	t.Parallel()

	raw := openKnowledgeMigrationTestDB(t, "app-catalog-authority-maximum.sqlite")
	if err := ApplyMigrations(context.Background(), raw, migrationsBefore(t, "0031_")); err != nil {
		t.Fatalf("apply through migration 0030: %v", err)
	}
	insertKnowledgeAuthorityTestApp(t, raw, knowledgeMigrationTestAppID, "tenant-a", "tenant-a")
	if _, err := raw.ExecContext(t.Context(), `
		UPDATE app_catalog_revisions
		SET revision = 9223372036854775807
		WHERE tenant_id = 'tenant-a'`); err != nil {
		t.Fatalf("seed maximum app revision: %v", err)
	}
	if err := ApplyMigrations(context.Background(), raw, migrations.SQLite()); err != nil {
		t.Fatalf("apply migration 0031: %v", err)
	}

	if _, err := raw.ExecContext(t.Context(), `
		UPDATE app_workspaces
		SET display_name = 'Changed', version = version + 1, updated_at_unix_micro = 2
		WHERE tenant_id = 'tenant-a'`); err == nil {
		t.Fatal("app mutation at maximum catalog revision succeeded")
	}
	assertIntegerQuery(t, raw, 1, `
		SELECT count(*) FROM app_workspaces
		WHERE tenant_id = 'tenant-a'
		  AND display_name = 'Knowledge authority'
		  AND version = 1
		  AND updated_at_unix_micro = 1`)
	assertIntegerQuery(t, raw, 9223372036854775807, `
		SELECT revision FROM app_catalog_revisions WHERE tenant_id = 'tenant-a'`)
}

func insertKnowledgeAuthorityTestApp(
	t *testing.T,
	db *sql.DB,
	appID string,
	tenantID string,
	slug string,
) {
	t.Helper()
	if err := insertKnowledgeAuthorityTestAppError(db, appID, tenantID, slug); err != nil {
		t.Fatalf("insert app workspace: %v", err)
	}
}

func insertKnowledgeAuthorityTestAppError(
	db *sql.DB,
	appID string,
	tenantID string,
	slug string,
) error {
	_, err := db.ExecContext(context.Background(), `
		INSERT INTO app_workspaces (
			app_id, tenant_id, version, slug, display_name, description,
			default_time_range_present, state,
			created_at_unix_micro, updated_at_unix_micro
		) VALUES (?, ?, 1, ?, 'Knowledge authority', '', 0, 'active', 1, 1)`,
		appID, tenantID, slug,
	)
	return err
}
