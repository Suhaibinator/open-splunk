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

func TestSearchAttemptKnowledgeSnapshotMigrationPreservesAndConstrainsJournal(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	raw, err := sql.Open(
		"sqlite",
		filepath.Join(t.TempDir(), "search-attempt-snapshot-upgrade.sqlite")+
			"?_pragma=foreign_keys(1)&_txlock=immediate",
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = raw.Close() })
	if err := ApplyMigrations(ctx, raw, migrationsBefore(t, "0032_")); err != nil {
		t.Fatalf("apply through migration 0031: %v", err)
	}
	if _, err := raw.ExecContext(ctx, `
		INSERT INTO search_attempt_audit_tenant_state (
			tenant_id, first_sequence, next_sequence, retained_count,
			maximum_retained_attempts
		) VALUES ('tenant-a', 1, 1, 0, 10);
		INSERT INTO search_attempt_audit_events (
			tenant_id, sequence, occurred_at_unix_micro,
			actor_kind, actor_id, actor_role, owner_id, search_job_id
		) VALUES (
			'tenant-a', 1, 1786224000000000,
			'system', 'open-splunk-server', 'system', 'owner-a', 'job-legacy'
		)`); err != nil {
		t.Fatalf("seed legacy journal: %v", err)
	}

	if err := ApplyMigrations(ctx, raw, migrations.SQLite()); err != nil {
		t.Fatalf("apply migrations through 0034: %v", err)
	}
	assertIntegerQuery(t, raw, 38, `SELECT count(*) FROM schema_migrations`)
	assertIntegerQuery(t, raw, 5, `
		SELECT count(*)
		FROM pragma_table_info('search_attempt_audit_events')
		WHERE name IN (
			'knowledge_snapshot_sha256',
			'knowledge_snapshot_tenant_catalog_revision',
			'knowledge_snapshot_tenant_catalog_state_token',
			'knowledge_snapshot_object_count',
			'knowledge_snapshot_compiler_compatibility_version'
		)`)
	assertIntegerQuery(t, raw, 5, `
		SELECT count(*)
		FROM pragma_table_info('search_attempt_audit_events')
		WHERE name LIKE 'knowledge_snapshot_%'`)
	assertIntegerQuery(t, raw, 1, `
		SELECT count(*)
		FROM search_attempt_audit_events
		WHERE tenant_id = 'tenant-a' AND sequence = 1
		  AND occurred_at_unix_micro = 1786224000000000
		  AND actor_kind = 'system' AND actor_id = 'open-splunk-server'
		  AND actor_role = 'system' AND owner_id = 'owner-a'
		  AND search_job_id = 'job-legacy'
		  AND knowledge_snapshot_sha256 IS NULL
		  AND knowledge_snapshot_tenant_catalog_revision IS NULL
		  AND knowledge_snapshot_tenant_catalog_state_token IS NULL
		  AND knowledge_snapshot_object_count IS NULL
		  AND knowledge_snapshot_compiler_compatibility_version IS NULL`)

	digest := bytes.Repeat([]byte{0x42}, 32)
	token := bytes.Repeat([]byte{0x73}, 32)
	if _, err := raw.ExecContext(ctx, `
		INSERT INTO search_attempt_audit_events (
			tenant_id, sequence, occurred_at_unix_micro,
			actor_kind, actor_id, actor_role, owner_id, search_job_id,
			knowledge_snapshot_sha256,
			knowledge_snapshot_tenant_catalog_revision,
			knowledge_snapshot_tenant_catalog_state_token,
			knowledge_snapshot_object_count,
			knowledge_snapshot_compiler_compatibility_version
		) VALUES (
			'tenant-a', 2, 1786224000000001,
			'system', 'open-splunk-server', 'system', 'owner-a', 'job-snapshot',
			?, 0, ?, 0, '0.1'
		)`, digest, token); err != nil {
		t.Fatalf("insert valid snapshot tuple: %v", err)
	}
	var storedDigest, storedToken []byte
	var revision, objectCount int64
	var compatibility string
	if err := raw.QueryRowContext(ctx, `
		SELECT knowledge_snapshot_sha256,
		       knowledge_snapshot_tenant_catalog_revision,
		       knowledge_snapshot_tenant_catalog_state_token,
		       knowledge_snapshot_object_count,
		       knowledge_snapshot_compiler_compatibility_version
		FROM search_attempt_audit_events
		WHERE tenant_id = 'tenant-a' AND sequence = 2
	`).Scan(&storedDigest, &revision, &storedToken, &objectCount, &compatibility); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(storedDigest, digest) || revision != 0 ||
		!bytes.Equal(storedToken, token) || objectCount != 0 || compatibility != "0.1" {
		t.Fatalf(
			"stored tuple = (%x, %d, %x, %d, %q)",
			storedDigest, revision, storedToken, objectCount, compatibility,
		)
	}
	if _, err := raw.ExecContext(ctx, `
		INSERT INTO search_attempt_audit_events (
			tenant_id, sequence, occurred_at_unix_micro,
			actor_kind, actor_id, actor_role, owner_id, search_job_id,
			knowledge_snapshot_sha256,
			knowledge_snapshot_tenant_catalog_revision,
			knowledge_snapshot_tenant_catalog_state_token,
			knowledge_snapshot_object_count,
			knowledge_snapshot_compiler_compatibility_version
		) VALUES (
			'tenant-a', 3, 1786224000000002,
			'system', 'open-splunk-server', 'system', 'owner-a', 'job-boundary',
			?, 9223372036854775806, ?, 256, ?
		)`, digest, token, strings.Repeat("v", 128)); err != nil {
		t.Fatalf("insert maximum valid snapshot tuple: %v", err)
	}

	invalid := []struct {
		name          string
		digest        any
		revision      any
		token         any
		objectCount   any
		compatibility any
	}{
		{name: "partial", digest: digest, revision: nil, token: nil, objectCount: nil, compatibility: nil},
		{name: "short digest", digest: digest[:31], revision: 0, token: token, objectCount: 0, compatibility: "0.1"},
		{name: "revision overflow", digest: digest, revision: int64(9223372036854775807), token: token, objectCount: 0, compatibility: "0.1"},
		{name: "short token", digest: digest, revision: 0, token: token[:31], objectCount: 0, compatibility: "0.1"},
		{name: "object overflow", digest: digest, revision: 0, token: token, objectCount: 257, compatibility: "0.1"},
		{name: "empty compatibility", digest: digest, revision: 0, token: token, objectCount: 0, compatibility: ""},
		{name: "padded compatibility", digest: digest, revision: 0, token: token, objectCount: 0, compatibility: " 0.1"},
		{name: "leading tab compatibility", digest: digest, revision: 0, token: token, objectCount: 0, compatibility: "\t0.1"},
		{name: "trailing tab compatibility", digest: digest, revision: 0, token: token, objectCount: 0, compatibility: "0.1\t"},
		{name: "oversized compatibility", digest: digest, revision: 0, token: token, objectCount: 0, compatibility: strings.Repeat("v", 129)},
	}
	for _, test := range invalid {
		t.Run(test.name, func(t *testing.T) {
			_, err := raw.ExecContext(ctx, `
				INSERT INTO search_attempt_audit_events (
					tenant_id, sequence, occurred_at_unix_micro,
					actor_kind, actor_id, actor_role, owner_id, search_job_id,
					knowledge_snapshot_sha256,
					knowledge_snapshot_tenant_catalog_revision,
					knowledge_snapshot_tenant_catalog_state_token,
					knowledge_snapshot_object_count,
					knowledge_snapshot_compiler_compatibility_version
				) VALUES (
					'tenant-a', 4, 1786224000000003,
					'system', 'open-splunk-server', 'system', 'owner-a', ?,
					?, ?, ?, ?, ?
				)`, "job-invalid-"+test.name, test.digest, test.revision,
				test.token, test.objectCount, test.compatibility)
			if err == nil {
				t.Fatal("invalid snapshot tuple was inserted")
			}
		})
	}
	assertIntegerQuery(t, raw, 4, `
		SELECT next_sequence FROM search_attempt_audit_tenant_state
		WHERE tenant_id = 'tenant-a'`)
	if _, err := raw.ExecContext(ctx, `
		UPDATE search_attempt_audit_events
		SET knowledge_snapshot_object_count = 1
		WHERE tenant_id = 'tenant-a' AND sequence = 2`); err == nil ||
		!strings.Contains(err.Error(), "cannot be updated") {
		t.Fatalf("snapshot tuple update error = %v", err)
	}

	backupPath := filepath.Join(t.TempDir(), "search-attempt-snapshot-backup.sqlite")
	if err := (&DB{sql: raw}).BackupTo(ctx, backupPath); err != nil {
		t.Fatalf("backup migrated journal: %v", err)
	}
	backup, err := OpenReadOnly(ctx, backupPath)
	if err != nil {
		t.Fatalf("open migrated backup: %v", err)
	}
	t.Cleanup(func() { _ = backup.Close() })
	var backupDigest, backupToken []byte
	if err := backup.SQLDB().QueryRowContext(ctx, `
		SELECT knowledge_snapshot_sha256,
		       knowledge_snapshot_tenant_catalog_state_token
		FROM search_attempt_audit_events
		WHERE tenant_id = 'tenant-a' AND sequence = 2
	`).Scan(&backupDigest, &backupToken); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(backupDigest, digest) || !bytes.Equal(backupToken, token) {
		t.Fatal("backup changed the search-attempt knowledge snapshot tuple")
	}
	assertNoForeignKeyViolations(t, raw)
}
