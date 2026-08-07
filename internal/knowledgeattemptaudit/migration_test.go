package knowledgeattemptaudit

import (
	"context"
	"database/sql"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/testsupport"
	"github.com/Suhaibinator/open-splunk/migrations"
	_ "modernc.org/sqlite"
)

func TestMigrationFreshSchemaIsExactAndBounded(t *testing.T) {
	t.Parallel()
	_, database := openTestDatabase(t)

	var version int
	if err := database.SQLDB().QueryRow(`SELECT MAX(version) FROM schema_migrations`).Scan(&version); err != nil {
		t.Fatal(err)
	}
	if version < 27 {
		t.Fatalf("latest migration = %d, want at least 27", version)
	}
	rows, err := database.SQLDB().Query(`
		SELECT name FROM pragma_table_info('knowledge_attempt_audit_events') ORDER BY cid
	`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var columns []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatal(err)
		}
		columns = append(columns, name)
	}
	want := []string{
		"tenant_id", "sequence", "occurred_at_unix_micro", "actor_kind",
		"actor_id", "actor_role", "action", "result", "reason",
		"app_id", "knowledge_object_id", "object_type", "object_version",
		"sharing_scope",
	}
	if strings.Join(columns, ",") != strings.Join(want, ",") {
		t.Fatalf("columns = %v, want %v", columns, want)
	}
	var triggerCount int
	if err := database.SQLDB().QueryRow(`
		SELECT COUNT(*) FROM sqlite_schema
		WHERE type = 'trigger' AND name LIKE 'knowledge_attempt_audit_%'
	`).Scan(&triggerCount); err != nil {
		t.Fatal(err)
	}
	if triggerCount != 10 {
		t.Fatalf("trigger count = %d, want 10", triggerCount)
	}
	if _, err := database.SQLDB().Exec(`
		INSERT INTO knowledge_attempt_audit_tenant_state (
			tenant_id, first_sequence, next_sequence, retained_count
		) VALUES ('bad-count', 1, 100003, 100002)
	`); err == nil {
		t.Fatal("over-cap retained count was accepted")
	}
}

func TestMigrationUpgradesVersion26Database(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	raw := openRawMigrationDB(t, filepath.Join(t.TempDir(), "upgrade.db"))
	before := testsupport.SQLiteMigrationsBefore(t, "0027_")
	if err := control.ApplyMigrations(ctx, raw, before); err != nil {
		t.Fatalf("ApplyMigrations(before 0027): %v", err)
	}
	if _, err := raw.Exec(`INSERT INTO app_catalog_revisions (tenant_id, revision) VALUES ('tenant-existing', 1)`); err != nil {
		t.Fatalf("seed legacy control state: %v", err)
	}
	if err := control.ApplyMigrations(ctx, raw, migrations.SQLite()); err != nil {
		t.Fatalf("ApplyMigrations(upgrade): %v", err)
	}
	var existingRevision, latest int
	if err := raw.QueryRow(`SELECT revision FROM app_catalog_revisions WHERE tenant_id = 'tenant-existing'`).Scan(&existingRevision); err != nil {
		t.Fatal(err)
	}
	if err := raw.QueryRow(`SELECT MAX(version) FROM schema_migrations`).Scan(&latest); err != nil {
		t.Fatal(err)
	}
	if existingRevision != 1 || latest < 27 {
		t.Fatalf("upgrade state = revision %d, latest %d", existingRevision, latest)
	}
	if _, err := raw.Exec(`
		INSERT INTO knowledge_attempt_audit_tenant_state (
			tenant_id, first_sequence, next_sequence, retained_count
		) VALUES ('tenant-new', 1, 1, 0)
	`); err != nil {
		t.Fatalf("new schema is unusable: %v", err)
	}
}

func TestFailedMigrationRollsBackEntireVersion(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	raw := openRawMigrationDB(t, filepath.Join(t.TempDir(), "rollback.db"))
	before := testsupport.SQLiteMigrationsBefore(t, "0027_")
	if err := control.ApplyMigrations(ctx, raw, before); err != nil {
		t.Fatalf("ApplyMigrations(before): %v", err)
	}
	broken := testsupport.FilterEmbeddedFS(
		t,
		migrations.SQLite(),
		func(name string) bool { return name <= "0027_knowledge_attempt_audit.sql" },
	)
	item := broken["0027_knowledge_attempt_audit.sql"]
	item.Data = append(append([]byte(nil), item.Data...), []byte("\nINSERT INTO table_that_does_not_exist VALUES (1);\n")...)
	broken["0027_knowledge_attempt_audit.sql"] = item
	if err := control.ApplyMigrations(ctx, raw, broken); err == nil {
		t.Fatal("broken migration unexpectedly succeeded")
	}
	var tables, ledger int
	if err := raw.QueryRow(`
		SELECT COUNT(*) FROM sqlite_schema
		WHERE name IN (
			'knowledge_attempt_audit_tenant_state',
			'knowledge_attempt_audit_events'
		)
	`).Scan(&tables); err != nil {
		t.Fatal(err)
	}
	if err := raw.QueryRow(`SELECT coalesce(MAX(version), 0) FROM schema_migrations`).Scan(&ledger); err != nil {
		t.Fatal(err)
	}
	if tables != 0 || ledger != 26 {
		t.Fatalf("failed migration left tables=%d ledger=%d", tables, ledger)
	}
}

func openRawMigrationDB(t *testing.T, path string) *sql.DB {
	t.Helper()
	raw, err := sql.Open("sqlite", path+"?_pragma=foreign_keys(1)&_txlock=immediate")
	if err != nil {
		t.Fatal(err)
	}
	raw.SetMaxOpenConns(4)
	if err := raw.Ping(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := raw.Close(); err != nil {
			t.Errorf("close raw migration DB: %v", err)
		}
	})
	return raw
}
