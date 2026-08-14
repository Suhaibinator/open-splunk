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

const hecChannelsMigration = "0036_hec_channels_and_acknowledgments.sql"

// Update this pin only while deliberately authoring the unpublished
// migration. Once the migration ships, checksum drift is a compatibility
// failure for databases and recovery sets that already recorded it.
const hecChannelsMigrationSHA256 = "710a300e1a4560ebbe2b913a7e30ac93fc1d79e64c0c0c4169109e5f9ee36c6c"

func TestHECChannelsMigrationPinsChecksumAndLedgerOrdering(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	raw := openHECMigrationDB(t, filepath.Join(t.TempDir(), "fresh.sqlite"))
	if err := ApplyMigrations(ctx, raw, migrations.SQLite()); err != nil {
		t.Fatalf("apply migrations: %v", err)
	}

	contents, err := fs.ReadFile(migrations.SQLite(), hecChannelsMigration)
	if err != nil {
		t.Fatalf("read embedded HEC migration: %v", err)
	}
	wantChecksum := sha256.Sum256(contents)
	if got := fmt.Sprintf("%x", wantChecksum); got != hecChannelsMigrationSHA256 {
		t.Fatalf("HEC migration SHA-256 = %s, want %s", got, hecChannelsMigrationSHA256)
	}

	type ledgerRow struct {
		version   int
		name      string
		checksum  []byte
		appliedAt int64
	}
	rows, err := raw.QueryContext(ctx, `
		SELECT version, name, checksum, applied_at_unix_micro
		FROM schema_migrations
		WHERE version BETWEEN 35 AND 36
		ORDER BY version`)
	if err != nil {
		t.Fatalf("read HEC migration ledger tail: %v", err)
	}
	defer rows.Close()
	var tail []ledgerRow
	for rows.Next() {
		var row ledgerRow
		if err := rows.Scan(&row.version, &row.name, &row.checksum, &row.appliedAt); err != nil {
			t.Fatalf("scan HEC migration ledger tail: %v", err)
		}
		tail = append(tail, row)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate HEC migration ledger tail: %v", err)
	}
	if len(tail) != 2 ||
		tail[0].version != 35 || tail[0].name != ingestionTokenPurposeMigration ||
		tail[1].version != 36 || tail[1].name != hecChannelsMigration {
		t.Fatalf("HEC migration ledger tail = %+v", tail)
	}
	if !bytes.Equal(tail[1].checksum, wantChecksum[:]) {
		t.Fatalf("HEC migration ledger checksum = %x, want %x", tail[1].checksum, wantChecksum)
	}
	if tail[0].appliedAt <= 0 || tail[1].appliedAt < tail[0].appliedAt {
		t.Fatalf("HEC migration ledger timestamps are out of order: %+v", tail)
	}
	assertIntegerQuery(t, raw, 38, `SELECT count(*) FROM schema_migrations`)
	assertIntegerQuery(t, raw, 38, `SELECT max(version) FROM schema_migrations`)
	assertNoForeignKeyViolations(t, raw)
}

func TestHECChannelsMigrationUpgrades0035DatabaseAndReopens(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "upgrade.sqlite")
	legacy := openHECMigrationDB(t, path)
	if err := ApplyMigrations(ctx, legacy, migrationsBefore(t, "0036_")); err != nil {
		t.Fatalf("apply migrations through 0035: %v", err)
	}
	seedHECUpgradeAuthority(t, legacy)
	assertIntegerQuery(t, legacy, 0, `
		SELECT count(*) FROM sqlite_schema
		WHERE type = 'table' AND name = 'hec_requests'`)
	if err := legacy.Close(); err != nil {
		t.Fatalf("close 0035 database: %v", err)
	}

	upgrade := openHECMigrationDB(t, path)
	if err := ApplyMigrations(ctx, upgrade, migrations.SQLite()); err != nil {
		t.Fatalf("upgrade 0035 database through 0036: %v", err)
	}
	assertIntegerQuery(t, upgrade, 1, `
		SELECT count(*)
		FROM ingestion_tokens AS token
		JOIN ingestion_token_hec_profiles AS profile
		  ON profile.ingestion_token_id = token.ingestion_token_id
		WHERE token.ingestion_token_id = 'tok-hec-upgrade'
		  AND token.purpose = 'hec'
		  AND profile.indexer_acknowledgment = 1`)
	assertIntegerQuery(t, upgrade, 4, `
		SELECT count(*) FROM sqlite_schema
		WHERE type = 'table'
		  AND name IN (
			'hec_source_sequences', 'hec_requests',
			'hec_channels', 'hec_acknowledgments'
		  )`)
	assertIntegerQuery(t, upgrade, 2, `
		SELECT count(*) FROM sqlite_schema
		WHERE type = 'trigger'
		  AND name IN (
			'hec_request_visibility_committed',
			'hec_request_visibility_failed'
		  )`)
	assertIntegerQuery(t, upgrade, 38, `SELECT count(*) FROM schema_migrations`)
	assertHECAcknowledgmentSafeIntegerConstraint(t, upgrade)
	assertNoForeignKeyViolations(t, upgrade)
	if err := upgrade.Close(); err != nil {
		t.Fatalf("close upgraded database: %v", err)
	}

	reopened, err := Open(ctx, path)
	if err != nil {
		t.Fatalf("reopen upgraded database through production Open: %v", err)
	}
	defer func() { _ = reopened.Close() }()
	identity, err := reopened.VerifyCurrentMigrations(ctx, migrations.SQLite())
	if err != nil {
		t.Fatalf("verify reopened migration authority: %v", err)
	}
	if identity.LatestVersion != 38 || identity.SHA256 == ([sha256.Size]byte{}) {
		t.Fatalf("reopened migration identity = %+v", identity)
	}
	assertIntegerQuery(t, reopened.SQLDB(), 1, `
		SELECT count(*) FROM ingestion_token_indexes
		WHERE ingestion_token_id = 'tok-hec-upgrade'
		  AND index_id = 'idx-hec-upgrade'`)
	if err := reopened.VerifyIntegrity(ctx); err != nil {
		t.Fatalf("verify reopened database integrity: %v", err)
	}
}

func assertHECAcknowledgmentSafeIntegerConstraint(t *testing.T, db *sql.DB) {
	t.Helper()
	if _, err := db.ExecContext(context.Background(), `
		INSERT INTO hec_source_sequences (
			tenant_id, ingestion_token_id, next_request_sequence,
			updated_at_unix_micro
		) VALUES ('tenant-hec-upgrade', 'tok-hec-upgrade', 3, 200);
		INSERT INTO hec_requests (
			tenant_id, ingestion_token_id, request_sequence, request_id,
			visibility_sequence, state, created_at_unix_micro,
			terminal_at_unix_micro
		) VALUES
			('tenant-hec-upgrade', 'tok-hec-upgrade', 1, 'request-safe-max',
			 NULL, 'indexed', 200, 201),
			('tenant-hec-upgrade', 'tok-hec-upgrade', 2, 'request-unsafe-max',
			 NULL, 'indexed', 200, 201);
		INSERT INTO hec_channels (
			tenant_id, ingestion_token_id, channel_id,
			next_acknowledgment_id, created_at_unix_micro,
			last_used_at_unix_micro
		) VALUES (
			'tenant-hec-upgrade', 'tok-hec-upgrade', 'safe-integer-channel',
			3, 200, 200
		);
		INSERT INTO hec_acknowledgments (
			tenant_id, ingestion_token_id, channel_id,
			acknowledgment_id, request_sequence, created_at_unix_micro
		) VALUES (
			'tenant-hec-upgrade', 'tok-hec-upgrade', 'safe-integer-channel',
			9007199254740991, 1, 200
		)`); err != nil {
		t.Fatalf("seed maximum exact HEC acknowledgment ID: %v", err)
	}
	if _, err := db.ExecContext(context.Background(), `
		INSERT INTO hec_acknowledgments (
			tenant_id, ingestion_token_id, channel_id,
			acknowledgment_id, request_sequence, created_at_unix_micro
		) VALUES (
			'tenant-hec-upgrade', 'tok-hec-upgrade', 'safe-integer-channel',
			9007199254740992, 2, 200
		)`); err == nil || !strings.Contains(err.Error(), "CHECK constraint failed") {
		t.Fatalf("unsafe HEC acknowledgment ID constraint error = %v", err)
	}
}

func TestHECChannelsMigrationRollsBackLateSchemaConflict(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	raw := openHECMigrationDB(t, filepath.Join(t.TempDir(), "rollback.sqlite"))
	if err := ApplyMigrations(ctx, raw, migrationsBefore(t, "0036_")); err != nil {
		t.Fatalf("apply migrations through 0035: %v", err)
	}
	const conflictingTrigger = `CREATE TRIGGER hec_request_visibility_failed
		AFTER UPDATE ON ingestion_tokens
		BEGIN
			SELECT 1;
		END`
	if _, err := raw.ExecContext(ctx, conflictingTrigger); err != nil {
		t.Fatalf("create corrupt prestate trigger: %v", err)
	}

	err := ApplyMigrations(ctx, raw, migrations.SQLite())
	if err == nil || !strings.Contains(err.Error(), "hec_request_visibility_failed") {
		t.Fatalf("late HEC migration conflict error = %v", err)
	}
	assertIntegerQuery(t, raw, 35, `SELECT count(*) FROM schema_migrations`)
	assertIntegerQuery(t, raw, 0, `
		SELECT count(*) FROM sqlite_schema
		WHERE name IN (
			'hec_source_sequences', 'hec_requests', 'hec_channels',
			'hec_acknowledgments', 'hec_requests_terminal_retention_idx',
			'hec_channels_token_activity_idx',
			'hec_acknowledgments_bounded_lookup_idx',
			'hec_request_visibility_committed'
		)`)
	assertIntegerQuery(t, raw, 1, `
		SELECT count(*) FROM sqlite_schema
		WHERE type = 'trigger'
		  AND name = 'hec_request_visibility_failed'
		  AND tbl_name = 'ingestion_tokens'`)
	assertNoForeignKeyViolations(t, raw)

	if _, err := raw.ExecContext(ctx, `DROP TRIGGER hec_request_visibility_failed`); err != nil {
		t.Fatalf("remove corrupt prestate trigger: %v", err)
	}
	if err := ApplyMigrations(ctx, raw, migrations.SQLite()); err != nil {
		t.Fatalf("apply HEC migration after repairing prestate: %v", err)
	}
	assertIntegerQuery(t, raw, 38, `SELECT count(*) FROM schema_migrations`)
	assertIntegerQuery(t, raw, 4, `
		SELECT count(*) FROM sqlite_schema
		WHERE type = 'table'
		  AND name IN (
			'hec_source_sequences', 'hec_requests',
			'hec_channels', 'hec_acknowledgments'
		  )`)
	assertNoForeignKeyViolations(t, raw)
}

func openHECMigrationDB(t *testing.T, path string) *sql.DB {
	t.Helper()
	raw, err := sql.Open(
		"sqlite",
		path+"?_pragma=foreign_keys(1)&_txlock=immediate",
	)
	if err != nil {
		t.Fatalf("open HEC migration database: %v", err)
	}
	raw.SetMaxOpenConns(1)
	raw.SetMaxIdleConns(1)
	t.Cleanup(func() {
		if err := raw.Close(); err != nil {
			t.Errorf("close HEC migration database: %v", err)
		}
	})
	return raw
}

func seedHECUpgradeAuthority(t *testing.T, db *sql.DB) {
	t.Helper()
	if _, err := db.ExecContext(context.Background(), `
		INSERT INTO indexes (
			index_id, version, name, display_name, ingestion_enabled,
			search_enabled, state, created_at_unix_micro, updated_at_unix_micro
		) VALUES (
			'idx-hec-upgrade', 1, 'hec-upgrade', 'HEC upgrade',
			1, 1, 'active', 100, 100
		);
		INSERT INTO ingestion_tokens (
			ingestion_token_id, version, name, description, token_prefix,
			token_digest, state, created_at_unix_micro, updated_at_unix_micro,
			purpose
		) VALUES (
			'tok-hec-upgrade', 1, 'HEC upgrade', '', 'hectest00',
			randomblob(32), 'active', 100, 100, 'hec'
		);
		INSERT INTO ingestion_token_indexes (ingestion_token_id, index_id)
		VALUES ('tok-hec-upgrade', 'idx-hec-upgrade');
		INSERT INTO ingestion_token_hec_profiles (
			ingestion_token_id, default_index_id, default_host,
			default_source, default_sourcetype, indexer_acknowledgment
		) VALUES (
			'tok-hec-upgrade', 'idx-hec-upgrade', NULL, NULL, NULL, 1
		)`); err != nil {
		t.Fatalf("seed 0035 HEC authority: %v", err)
	}
}
