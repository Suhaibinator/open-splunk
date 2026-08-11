package control

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"fmt"
	"io/fs"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Suhaibinator/open-splunk/migrations"
	_ "modernc.org/sqlite"
)

const ingestionTokenPurposeMigration = "0035_ingestion_token_purpose_and_hec_profile.sql"

// Update this pin only when authoring the unpublished migration. Once the
// migration ships, checksum drift is a compatibility failure.
const ingestionTokenPurposeMigrationSHA256 = "a2b9f66201be7a19187e4be87cd2b6bac77154a5afc9cca3319290be46eb3c5c"

func TestIngestionTokenPurposeMigrationPinsFreshSchemaAndChecksum(t *testing.T) {
	t.Parallel()

	raw := openIngestionTokenPurposeMigrationDB(t, "fresh.sqlite")
	if err := ApplyMigrations(
		context.Background(),
		raw,
		migrationsBefore(t, "0036_"),
	); err != nil {
		t.Fatalf("apply migrations: %v", err)
	}

	contents, err := fs.ReadFile(migrations.SQLite(), ingestionTokenPurposeMigration)
	if err != nil {
		t.Fatalf("read embedded migration: %v", err)
	}
	wantChecksum := sha256.Sum256(contents)
	if got := fmt.Sprintf("%x", wantChecksum); got != ingestionTokenPurposeMigrationSHA256 {
		t.Fatalf("migration SHA-256 = %s, want %s", got, ingestionTokenPurposeMigrationSHA256)
	}

	var version int
	var name string
	var checksum []byte
	if err := raw.QueryRowContext(t.Context(), `
		SELECT version, name, checksum
		FROM schema_migrations
		WHERE version = 35`).Scan(&version, &name, &checksum); err != nil {
		t.Fatalf("read migration authority: %v", err)
	}
	if version != 35 || name != ingestionTokenPurposeMigration ||
		!bytes.Equal(checksum, wantChecksum[:]) {
		t.Fatalf(
			"migration authority = (%d, %q, %x), want (35, %q, %x)",
			version,
			name,
			checksum,
			ingestionTokenPurposeMigration,
			wantChecksum,
		)
	}
	assertIntegerQuery(t, raw, 35, `SELECT count(*) FROM schema_migrations`)
	assertIntegerQuery(t, raw, 1, `
		SELECT count(*)
		FROM pragma_table_info('ingestion_tokens')
		WHERE name = 'purpose'
		  AND type = 'TEXT'
		  AND "notnull" = 1
		  AND dflt_value = '''native_collector'''`)
	assertIntegerQuery(t, raw, 1, `
		SELECT count(*)
		FROM sqlite_schema
		WHERE type = 'table'
		  AND name = 'ingestion_token_hec_profiles'`)
	assertNoForeignKeyViolations(t, raw)
}

func TestIngestionTokenPurposeMigrationBackfillsNativeAndEnforcesIsolation(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	raw := openIngestionTokenPurposeMigrationDB(t, "upgrade.sqlite")
	if err := ApplyMigrations(ctx, raw, migrationsBefore(t, "0035_")); err != nil {
		t.Fatalf("apply through migration 0034: %v", err)
	}

	if _, err := raw.ExecContext(ctx, `
		INSERT INTO ingestion_tokens (
			ingestion_token_id, version, name, description, token_prefix,
			token_digest, state, created_at_unix_micro, updated_at_unix_micro,
			bound_collector_id
		) VALUES (
			'tok_existing_native', 1, 'existing native', '',
			'ost_v1_existing', randomblob(32), 'active', 100, 100,
			'collector-existing'
		)`); err != nil {
		t.Fatalf("seed existing native token: %v", err)
	}

	var bindingTriggerSQL string
	if err := raw.QueryRowContext(ctx, `
		SELECT sql FROM sqlite_schema
		WHERE type = 'trigger'
		  AND name = 'ingestion_token_collector_binding_is_required'`).
		Scan(&bindingTriggerSQL); err != nil {
		t.Fatalf("read pre-upgrade binding trigger: %v", err)
	}
	if _, err := raw.ExecContext(ctx, `DROP TRIGGER ingestion_token_collector_binding_is_required`); err != nil {
		t.Fatalf("remove binding trigger for legacy fixture: %v", err)
	}
	if _, err := raw.ExecContext(ctx, `
		INSERT INTO ingestion_tokens (
			ingestion_token_id, version, name, description, token_prefix,
			token_digest, state, created_at_unix_micro, updated_at_unix_micro
		) VALUES (
			'tok_existing_unbound', 1, 'existing unbound', '',
			'ost_v1_unbound', randomblob(32), 'disabled', 100, 100
		)`); err != nil {
		t.Fatalf("seed legacy unbound token: %v", err)
	}
	if _, err := raw.ExecContext(ctx, bindingTriggerSQL); err != nil {
		t.Fatalf("restore pre-upgrade binding trigger: %v", err)
	}

	if err := ApplyMigrations(ctx, raw, migrationsBefore(t, "0036_")); err != nil {
		t.Fatalf("apply migration 0035: %v", err)
	}
	assertIntegerQuery(t, raw, 2, `
		SELECT count(*) FROM ingestion_tokens
		WHERE purpose = 'native_collector'`)
	assertIntegerQuery(t, raw, 0, `SELECT count(*) FROM ingestion_token_hec_profiles`)

	for label, statement := range map[string]string{
		"mutate backfilled purpose": `
			UPDATE ingestion_tokens SET purpose = 'hec'
			WHERE ingestion_token_id = 'tok_existing_native'`,
		"insert unbound native": `
			INSERT INTO ingestion_tokens (
				ingestion_token_id, version, name, description, token_prefix,
				token_digest, state, created_at_unix_micro, updated_at_unix_micro,
				purpose
			) VALUES (
				'tok_unbound_native', 1, 'unbound native', '',
				'ost_v1_native', randomblob(32), 'active', 100, 100,
				'native_collector'
			)`,
		"insert bound HEC": `
			INSERT INTO ingestion_tokens (
				ingestion_token_id, version, name, description, token_prefix,
				token_digest, state, created_at_unix_micro, updated_at_unix_micro,
				bound_collector_id, purpose
			) VALUES (
				'tok_bound_hec', 1, 'bound HEC', '',
				'ost_v1_bound', randomblob(32), 'active', 100, 100,
				'collector-forbidden', 'hec'
			)`,
		"attach profile to native": `
			INSERT INTO ingestion_token_hec_profiles (
				ingestion_token_id, indexer_acknowledgment
			) VALUES ('tok_existing_native', 0)`,
	} {
		if _, err := raw.ExecContext(ctx, statement); err == nil {
			t.Fatalf("%s unexpectedly succeeded", label)
		}
	}

	if _, err := raw.ExecContext(ctx, `
		INSERT INTO ingestion_tokens (
			ingestion_token_id, version, name, description, token_prefix,
			token_digest, state, created_at_unix_micro, updated_at_unix_micro,
			purpose
		) VALUES (
			'tok_hec', 1, 'HEC', '', 'ost_v1_hec_token', randomblob(32),
			'active', 100, 100, 'hec'
		);
		INSERT INTO ingestion_token_hec_profiles (
			ingestion_token_id, default_host, default_source,
			default_sourcetype, indexer_acknowledgment
		) VALUES ('tok_hec', 'host-a', 'source-a', 'json', 1)`); err != nil {
		t.Fatalf("insert valid HEC token and profile: %v", err)
	}
	if _, err := raw.ExecContext(ctx, `
		UPDATE ingestion_token_hec_profiles
		SET indexer_acknowledgment = 0
		WHERE ingestion_token_id = 'tok_hec'`); err == nil ||
		!strings.Contains(err.Error(), "acknowledgment mode is immutable") {
		t.Fatalf("mutable HEC acknowledgment error = %v", err)
	}
	assertNoForeignKeyViolations(t, raw)
}

func TestIngestionTokenPurposeMigrationRollsBackOnProfileConflict(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	raw := openIngestionTokenPurposeMigrationDB(t, "rollback.sqlite")
	if err := ApplyMigrations(ctx, raw, migrationsBefore(t, "0035_")); err != nil {
		t.Fatalf("apply through migration 0034: %v", err)
	}
	if _, err := raw.ExecContext(ctx, `
		CREATE VIEW ingestion_token_hec_profiles AS
		SELECT ingestion_token_id FROM ingestion_tokens WHERE 0`); err != nil {
		t.Fatalf("create migration conflict: %v", err)
	}

	err := ApplyMigrations(ctx, raw, migrationsBefore(t, "0036_"))
	if err == nil || !strings.Contains(err.Error(), "ingestion_token_hec_profiles") {
		t.Fatalf("conflicting migration error = %v", err)
	}
	assertIntegerQuery(t, raw, 34, `SELECT count(*) FROM schema_migrations`)
	assertIntegerQuery(t, raw, 0, `
		SELECT count(*) FROM pragma_table_info('ingestion_tokens')
		WHERE name = 'purpose'`)
	assertIntegerQuery(t, raw, 1, `
		SELECT count(*) FROM sqlite_schema
		WHERE type = 'trigger'
		  AND name = 'ingestion_token_collector_binding_is_required'`)
}

func openIngestionTokenPurposeMigrationDB(t *testing.T, name string) *sql.DB {
	t.Helper()
	raw, err := sql.Open(
		"sqlite",
		filepath.Join(t.TempDir(), name)+"?_pragma=foreign_keys(1)&_txlock=immediate",
	)
	if err != nil {
		t.Fatalf("open migration database: %v", err)
	}
	raw.SetMaxOpenConns(1)
	raw.SetMaxIdleConns(1)
	t.Cleanup(func() {
		if err := raw.Close(); err != nil {
			t.Errorf("close migration database: %v", err)
		}
	})
	return raw
}
