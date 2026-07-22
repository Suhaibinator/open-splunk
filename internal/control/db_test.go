package control

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io/fs"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"testing/fstest"

	"github.com/Suhaibinator/open-splunk/migrations"
	_ "modernc.org/sqlite"
)

func TestOpenConfiguresSQLiteAndAppliesMigrations(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "control.sqlite")
	db, err := Open(ctx, path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}

	assertPragmaValue(t, db.SQLDB(), "journal_mode", "wal")
	assertPragmaValue(t, db.SQLDB(), "foreign_keys", "1")
	assertPragmaValue(t, db.SQLDB(), "busy_timeout", fmt.Sprint(defaultBusyTimeout.Milliseconds()))
	assertPragmaValue(t, db.SQLDB(), "synchronous", "2") // SQLite FULL

	var migrationCount int
	if err := db.SQLDB().QueryRowContext(ctx, `SELECT count(*) FROM schema_migrations`).Scan(&migrationCount); err != nil {
		t.Fatalf("count schema migrations: %v", err)
	}
	if migrationCount != 4 {
		t.Fatalf("schema migration count = %d, want 4", migrationCount)
	}

	// Foreign keys are connection-local in SQLite. Force database/sql to open
	// several connections and verify the DSN configures every one.
	connections := make([]*sql.Conn, 4)
	for i := range connections {
		connections[i], err = db.SQLDB().Conn(ctx)
		if err != nil {
			t.Fatalf("Conn(%d): %v", i, err)
		}
		defer connections[i].Close()

		var enabled int
		if err := connections[i].QueryRowContext(ctx, `PRAGMA foreign_keys`).Scan(&enabled); err != nil {
			t.Fatalf("foreign_keys on connection %d: %v", i, err)
		}
		if enabled != 1 {
			t.Fatalf("foreign_keys on connection %d = %d, want 1", i, enabled)
		}
	}
	for _, conn := range connections {
		if err := conn.Close(); err != nil {
			t.Fatalf("close connection: %v", err)
		}
	}

	_, err = db.SQLDB().ExecContext(ctx, `
		INSERT INTO ingestion_token_indexes (ingestion_token_id, index_id)
		VALUES ('missing-token', 'missing-index')`)
	if err == nil {
		t.Fatal("foreign-key violating insert unexpectedly succeeded")
	}

	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	// Reopening is the ordinary idempotence path: already-applied migrations
	// are verified but never executed or recorded a second time.
	db, err = Open(ctx, path)
	if err != nil {
		t.Fatalf("second Open() error = %v", err)
	}
	defer db.Close()
	if err := db.SQLDB().QueryRowContext(ctx, `SELECT count(*) FROM schema_migrations`).Scan(&migrationCount); err != nil {
		t.Fatalf("count schema migrations after reopen: %v", err)
	}
	if migrationCount != 4 {
		t.Fatalf("schema migration count after reopen = %d, want 4", migrationCount)
	}
}

func TestApplyMigrationsIsVersionedAndDetectsDrift(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	raw, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "migrations.sqlite")+"?_txlock=immediate")
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	defer raw.Close()

	initial := fstest.MapFS{
		"0001_first.sql":  &fstest.MapFile{Data: []byte(`CREATE TABLE example (value TEXT NOT NULL) STRICT;`)},
		"0002_second.sql": &fstest.MapFile{Data: []byte(`CREATE INDEX example_value_idx ON example (value);`)},
	}
	if err := ApplyMigrations(ctx, raw, initial); err != nil {
		t.Fatalf("ApplyMigrations(first) error = %v", err)
	}
	if err := ApplyMigrations(ctx, raw, initial); err != nil {
		t.Fatalf("ApplyMigrations(idempotent) error = %v", err)
	}

	var count int
	if err := raw.QueryRowContext(ctx, `SELECT count(*) FROM schema_migrations`).Scan(&count); err != nil {
		t.Fatalf("count schema migrations: %v", err)
	}
	if count != 2 {
		t.Fatalf("schema migration count = %d, want 2", count)
	}

	drifted := fstest.MapFS{
		"0001_first.sql":  &fstest.MapFile{Data: []byte(`CREATE TABLE example (different TEXT NOT NULL) STRICT;`)},
		"0002_second.sql": initial["0002_second.sql"],
	}
	err = ApplyMigrations(ctx, raw, drifted)
	if !errors.Is(err, ErrMigrationDrift) {
		t.Fatalf("ApplyMigrations(drifted) error = %v, want ErrMigrationDrift", err)
	}

	if _, err := raw.ExecContext(ctx, `DELETE FROM schema_migrations WHERE version = 1`); err != nil {
		t.Fatalf("corrupt migration history: %v", err)
	}
	err = ApplyMigrations(ctx, raw, initial)
	if !errors.Is(err, ErrMigrationDrift) {
		t.Fatalf("ApplyMigrations(gapped history) error = %v, want ErrMigrationDrift", err)
	}
}

func TestConcurrentOpenSerializesMigrationStartup(t *testing.T) {
	t.Parallel()

	const openers = 6
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "concurrent.sqlite")
	start := make(chan struct{})
	errorsByOpener := make(chan error, openers)
	var wait sync.WaitGroup
	wait.Add(openers)
	for i := 0; i < openers; i++ {
		go func() {
			defer wait.Done()
			<-start
			db, err := Open(ctx, path)
			if err == nil {
				err = db.Close()
			}
			errorsByOpener <- err
		}()
	}
	close(start)
	wait.Wait()
	close(errorsByOpener)
	for err := range errorsByOpener {
		if err != nil {
			t.Errorf("concurrent Open() error = %v", err)
		}
	}

	db, err := Open(ctx, path)
	if err != nil {
		t.Fatalf("final Open(): %v", err)
	}
	defer db.Close()
	var count int
	if err := db.SQLDB().QueryRowContext(ctx, `SELECT count(*) FROM schema_migrations`).Scan(&count); err != nil {
		t.Fatalf("count schema migrations: %v", err)
	}
	if count != 4 {
		t.Fatalf("schema migration count = %d, want 4", count)
	}
}

func TestVisibilityPhaseMigrationUpgradesDrainedDatabase(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	raw, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "upgrade.sqlite")+"?_txlock=immediate")
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	defer raw.Close()

	legacy := legacyVisibilityMigrations(t)
	if err := ApplyMigrations(ctx, raw, legacy); err != nil {
		t.Fatalf("apply legacy migrations: %v", err)
	}
	if _, err := raw.ExecContext(ctx, `
		UPDATE ingest_visibility_state
		SET last_assigned = 2, committed_through = 2
		WHERE singleton = 1`); err != nil {
		t.Fatalf("seed drained legacy visibility state: %v", err)
	}
	if err := ApplyMigrations(ctx, raw, migrations.SQLite()); err != nil {
		t.Fatalf("upgrade drained database: %v", err)
	}
	var lastAssigned, committedThrough, reservationCount int
	if err := raw.QueryRowContext(ctx, `
		SELECT last_assigned, committed_through
		FROM ingest_visibility_state
		WHERE singleton = 1`).Scan(&lastAssigned, &committedThrough); err != nil {
		t.Fatal(err)
	}
	if err := raw.QueryRowContext(ctx, `
		SELECT count(*) FROM ingest_visibility_reservations`).Scan(&reservationCount); err != nil {
		t.Fatal(err)
	}
	if lastAssigned != 2 || committedThrough != 2 || reservationCount != 0 {
		t.Fatalf("upgraded state = last %d cutoff %d rows %d", lastAssigned, committedThrough, reservationCount)
	}
}

func TestVisibilityPhaseMigrationRejectsLegacyReservedRows(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	raw, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "undrained.sqlite")+"?_txlock=immediate")
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	defer raw.Close()
	if err := ApplyMigrations(ctx, raw, legacyVisibilityMigrations(t)); err != nil {
		t.Fatalf("apply legacy migrations: %v", err)
	}
	if _, err := raw.ExecContext(ctx, `
		UPDATE ingest_visibility_state SET last_assigned = 1 WHERE singleton = 1;
		INSERT INTO ingest_visibility_reservations
			(sequence, batch_key, state, attempt_id, index_time_unix_milli, payload_sha256, metadata)
		VALUES (1, 'legacy-reserved', 'reserved', 'old-attempt', 1000, zeroblob(32), X'')`); err != nil {
		t.Fatalf("seed legacy reserved row: %v", err)
	}
	err = ApplyMigrations(ctx, raw, migrations.SQLite())
	if err == nil || !strings.Contains(err.Error(), "legacy_reserved_visibility_rows_must_be_drained") {
		t.Fatalf("upgrade with reserved row error = %v, want explicit drain failure", err)
	}
	var migrationCount int
	if err := raw.QueryRowContext(ctx, `SELECT count(*) FROM schema_migrations`).Scan(&migrationCount); err != nil {
		t.Fatal(err)
	}
	if migrationCount != 2 {
		t.Fatalf("migration count after failed upgrade = %d, want 2", migrationCount)
	}
	var storedState string
	if err := raw.QueryRowContext(ctx, `
		SELECT state FROM ingest_visibility_reservations WHERE sequence = 1`).Scan(&storedState); err != nil {
		t.Fatal(err)
	}
	if storedState != "reserved" {
		t.Fatalf("legacy row state after rollback = %q, want reserved", storedState)
	}
}

func TestVisibilityPhaseMigrationTombstonesLegacyCommittedRows(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	raw, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "committed.sqlite")+"?_txlock=immediate")
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	defer raw.Close()
	if err := ApplyMigrations(ctx, raw, legacyVisibilityMigrations(t)); err != nil {
		t.Fatalf("apply legacy migrations: %v", err)
	}
	if _, err := raw.ExecContext(ctx, `
		UPDATE ingest_visibility_state
		SET last_assigned = 1, committed_through = 1
		WHERE singleton = 1;
		INSERT INTO ingest_visibility_reservations
			(sequence, batch_key, state, attempt_id, index_time_unix_milli, payload_sha256, metadata)
		VALUES (1, 'legacy-committed', 'committed', '', 1000, zeroblob(32), X'')`); err != nil {
		t.Fatalf("seed legacy committed row: %v", err)
	}
	if err := ApplyMigrations(ctx, raw, migrations.SQLite()); err != nil {
		t.Fatalf("upgrade committed legacy database: %v", err)
	}
	var batchKey string
	var sequence, createdAt int64
	if err := raw.QueryRowContext(ctx, `
		SELECT batch_key, legacy_visibility_seq, created_at_unix_micro
		FROM ingest_visibility_legacy_tombstones`).Scan(&batchKey, &sequence, &createdAt); err != nil {
		t.Fatal(err)
	}
	if batchKey != "legacy-committed" || sequence != 1 || createdAt <= 0 {
		t.Fatalf("legacy tombstone = batch %q sequence %d created %d", batchKey, sequence, createdAt)
	}
	var reservations, identities int
	if err := raw.QueryRowContext(ctx, `SELECT count(*) FROM ingest_visibility_reservations`).Scan(&reservations); err != nil {
		t.Fatal(err)
	}
	if err := raw.QueryRowContext(ctx, `SELECT count(*) FROM ingest_batch_identities`).Scan(&identities); err != nil {
		t.Fatal(err)
	}
	if reservations != 0 || identities != 0 {
		t.Fatalf("upgraded active ledger = %d reservations, %d identities", reservations, identities)
	}
}

func legacyVisibilityMigrations(t *testing.T) fstest.MapFS {
	t.Helper()
	legacy := fstest.MapFS{}
	for _, name := range []string{"0001_control_plane.sql", "0002_ingest_visibility.sql"} {
		data, err := fs.ReadFile(migrations.SQLite(), name)
		if err != nil {
			t.Fatalf("read %s: %v", name, err)
		}
		legacy[name] = &fstest.MapFile{Data: data}
	}
	return legacy
}

func TestLoadMigrationsRejectsInvalidVersionSequences(t *testing.T) {
	t.Parallel()

	tests := map[string]fs.FS{
		"missing first version": fstest.MapFS{
			"0002_second.sql": &fstest.MapFile{Data: []byte("SELECT 1;")},
		},
		"gap": fstest.MapFS{
			"0001_first.sql": &fstest.MapFile{Data: []byte("SELECT 1;")},
			"0003_third.sql": &fstest.MapFile{Data: []byte("SELECT 1;")},
		},
		"bad filename": fstest.MapFS{
			"first.sql": &fstest.MapFile{Data: []byte("SELECT 1;")},
		},
	}
	for name, migrations := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			if _, err := loadMigrations(migrations); err == nil {
				t.Fatal("loadMigrations() unexpectedly succeeded")
			}
		})
	}
}

func TestApplyMigrationsRejectsNewerDatabase(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	raw, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "newer.sqlite")+"?_txlock=immediate")
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	defer raw.Close()
	migrations := fstest.MapFS{
		"0001_first.sql": &fstest.MapFile{Data: []byte(`CREATE TABLE example (value TEXT) STRICT;`)},
	}
	if err := ApplyMigrations(ctx, raw, migrations); err != nil {
		t.Fatalf("ApplyMigrations(): %v", err)
	}
	if _, err := raw.ExecContext(ctx, `
		INSERT INTO schema_migrations (version, name, checksum, applied_at_unix_micro)
		VALUES (2, '0002_future.sql', zeroblob(32), 1)`); err != nil {
		t.Fatalf("insert future migration: %v", err)
	}
	if err := ApplyMigrations(ctx, raw, migrations); !errors.Is(err, ErrDatabaseTooNew) {
		t.Fatalf("ApplyMigrations(newer database) error = %v, want ErrDatabaseTooNew", err)
	}
}

func TestConcurrentOldAndNewMigrationSetsSerializeVersionCheck(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "mixed-versions.sqlite")
	dsn := path + "?_pragma=busy_timeout(5000)&_txlock=immediate"
	oldDB, err := sql.Open("sqlite", dsn)
	if err != nil {
		t.Fatalf("sql.Open(old): %v", err)
	}
	defer oldDB.Close()
	newDB, err := sql.Open("sqlite", dsn)
	if err != nil {
		t.Fatalf("sql.Open(new): %v", err)
	}
	defer newDB.Close()

	oldMigrations := fstest.MapFS{
		"0001_first.sql": &fstest.MapFile{Data: []byte(`CREATE TABLE first (value TEXT) STRICT;`)},
	}
	newMigrations := fstest.MapFS{
		"0001_first.sql":  oldMigrations["0001_first.sql"],
		"0002_second.sql": &fstest.MapFile{Data: []byte(`CREATE TABLE second (value TEXT) STRICT;`)},
	}
	start := make(chan struct{})
	oldResult := make(chan error, 1)
	newResult := make(chan error, 1)
	go func() {
		<-start
		oldResult <- ApplyMigrations(ctx, oldDB, oldMigrations)
	}()
	go func() {
		<-start
		newResult <- ApplyMigrations(ctx, newDB, newMigrations)
	}()
	close(start)
	if err := <-newResult; err != nil {
		t.Fatalf("new ApplyMigrations() error = %v", err)
	}
	if err := <-oldResult; err != nil && !errors.Is(err, ErrDatabaseTooNew) {
		t.Fatalf("old ApplyMigrations() error = %v, want nil or ErrDatabaseTooNew", err)
	}

	// Once the newer binary commits, an old binary must never report success.
	if err := ApplyMigrations(ctx, oldDB, oldMigrations); !errors.Is(err, ErrDatabaseTooNew) {
		t.Fatalf("old ApplyMigrations() after v2 error = %v, want ErrDatabaseTooNew", err)
	}
	var count int
	if err := newDB.QueryRowContext(ctx, `SELECT count(*) FROM schema_migrations`).Scan(&count); err != nil {
		t.Fatalf("count schema migrations: %v", err)
	}
	if count != 2 {
		t.Fatalf("schema migration count = %d, want 2", count)
	}
}

func TestOpenRejectsNonPersistentPaths(t *testing.T) {
	t.Parallel()

	for name, test := range map[string]struct {
		ctx  context.Context
		path string
	}{
		"nil context": {ctx: nil, path: filepath.Join(t.TempDir(), "nil.sqlite")},
		"empty path":  {ctx: context.Background(), path: ""},
		"memory":      {ctx: context.Background(), path: ":memory:"},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := Open(test.ctx, test.path); !errors.Is(err, ErrInvalidArgument) {
				t.Fatalf("Open() error = %v, want ErrInvalidArgument", err)
			}
		})
	}
}

func assertPragmaValue(t *testing.T, db *sql.DB, pragma, want string) {
	t.Helper()

	var got string
	if err := db.QueryRowContext(context.Background(), `PRAGMA `+pragma).Scan(&got); err != nil {
		t.Fatalf("PRAGMA %s: %v", pragma, err)
	}
	if got != want {
		t.Fatalf("PRAGMA %s = %q, want %q", pragma, got, want)
	}
}
