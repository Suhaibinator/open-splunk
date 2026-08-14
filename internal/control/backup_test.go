package control

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/Suhaibinator/open-splunk/migrations"
)

func TestOpenReadOnlyUsesOneQueryOnlyPoolWithoutMutatingSource(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	directory := t.TempDir()
	path := filepath.Join(directory, "control.sqlite")
	writable, err := Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := writable.SQLDB().ExecContext(ctx, `
		CREATE TABLE readonly_probe (value TEXT NOT NULL) STRICT;
		INSERT INTO readonly_probe (value) VALUES ('before')`); err != nil {
		t.Fatal(err)
	}
	if _, err := writable.SQLDB().ExecContext(ctx, `PRAGMA wal_checkpoint(TRUNCATE)`); err != nil {
		t.Fatal(err)
	}
	if err := writable.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, 0o640); err != nil {
		t.Fatal(err)
	}

	before := readFileState(t, path)
	readonly, err := OpenReadOnly(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	if readonly.GORMDB() == nil {
		t.Fatal("GORMDB() = nil")
	}
	rawFromGORM, err := readonly.GORMDB().DB()
	if err != nil {
		t.Fatal(err)
	}
	if rawFromGORM != readonly.SQLDB() {
		t.Fatal("GORM and SQL accessors do not share the read-only pool")
	}
	if got := readonly.SQLDB().Stats().MaxOpenConnections; got != 1 {
		t.Fatalf("MaxOpenConnections = %d, want 1", got)
	}
	assertPragmaValue(t, readonly.SQLDB(), "query_only", "1")
	assertPragmaValue(t, readonly.SQLDB(), "foreign_keys", "1")
	assertPragmaValue(t, readonly.SQLDB(), "busy_timeout", "5000")
	if _, err := readonly.SQLDB().ExecContext(ctx, `INSERT INTO readonly_probe (value) VALUES ('after')`); err == nil {
		t.Fatal("write through read-only SQL pool succeeded")
	}
	if err := readonly.GORMDB().Exec(`INSERT INTO readonly_probe (value) VALUES ('gorm')`).Error; err == nil {
		t.Fatal("write through read-only GORM pool succeeded")
	}
	if err := readonly.Close(); err != nil {
		t.Fatal(err)
	}

	after := readFileState(t, path)
	if before != after {
		t.Fatalf("read-only open mutated source: before=%+v after=%+v", before, after)
	}
}

func TestOpenReadOnlyRejectsMissingSymlinkAndNonRegularSources(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	directory := t.TempDir()
	missing := filepath.Join(directory, "missing.sqlite")
	if _, err := OpenReadOnly(ctx, missing); err == nil {
		t.Fatal("missing source succeeded")
	}
	if _, err := os.Lstat(missing); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("missing source was created: %v", err)
	}

	target := filepath.Join(directory, "target.sqlite")
	database, err := Open(ctx, target)
	if err != nil {
		t.Fatal(err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(directory, "link.sqlite")
	if err := os.Symlink(target, link); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenReadOnly(ctx, link); err == nil {
		t.Fatal("symbolic-link source succeeded")
	}
	if _, err := OpenReadOnly(ctx, directory); err == nil {
		t.Fatal("directory source succeeded")
	}
}

func TestBackupToIncludesCommittedWALAndExcludesUncommittedRows(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	directory := t.TempDir()
	sourcePath := filepath.Join(directory, "source.sqlite")
	writable, err := Open(ctx, sourcePath)
	if err != nil {
		t.Fatal(err)
	}
	defer writable.Close()
	if _, err := writable.SQLDB().ExecContext(ctx, `PRAGMA wal_autocheckpoint = 0`); err != nil {
		t.Fatal(err)
	}
	if _, err := writable.SQLDB().ExecContext(ctx, `CREATE TABLE backup_probe (value TEXT NOT NULL) STRICT`); err != nil {
		t.Fatal(err)
	}
	if _, err := writable.SQLDB().ExecContext(ctx, `PRAGMA wal_checkpoint(TRUNCATE)`); err != nil {
		t.Fatal(err)
	}
	if _, err := writable.SQLDB().ExecContext(ctx, `INSERT INTO backup_probe (value) VALUES ('committed')`); err != nil {
		t.Fatal(err)
	}
	tx, err := writable.SQLDB().BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := tx.Rollback(); err != nil && !errors.Is(err, sql.ErrTxDone) {
			t.Errorf("roll back test transaction: %v", err)
		}
	}()
	if _, err := tx.ExecContext(ctx, `INSERT INTO backup_probe (value) VALUES ('uncommitted')`); err != nil {
		t.Fatal(err)
	}

	walPath := sourcePath + "-wal"
	beforeDatabase := readFileState(t, sourcePath)
	beforeWAL := readFileState(t, walPath)
	if beforeWAL.Size <= 32 {
		t.Fatalf("test setup did not leave committed data in WAL: size = %d", beforeWAL.Size)
	}
	readonly, err := OpenReadOnly(ctx, sourcePath)
	if err != nil {
		t.Fatal(err)
	}
	defer readonly.Close()
	identity, err := readonly.VerifyCurrentMigrations(ctx, migrations.SQLite())
	if err != nil {
		t.Fatal(err)
	}
	if identity.LatestVersion != 38 || identity.SHA256 == ([sha256.Size]byte{}) {
		t.Fatalf("migration identity = %+v", identity)
	}

	destination := filepath.Join(directory, "backup.sqlite")
	if err := readonly.BackupTo(ctx, destination); err != nil {
		t.Fatal(err)
	}
	if after := readFileState(t, sourcePath); after != beforeDatabase {
		t.Fatalf("backup mutated source database: before=%+v after=%+v", beforeDatabase, after)
	}
	if after := readFileState(t, walPath); after != beforeWAL {
		t.Fatalf("backup checkpointed or mutated source WAL: before=%+v after=%+v", beforeWAL, after)
	}
	for _, sidecar := range []string{destination + "-wal", destination + "-shm", destination + "-journal"} {
		if _, err := os.Lstat(sidecar); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("destination sidecar %q exists: %v", filepath.Base(sidecar), err)
		}
	}

	backup, err := OpenReadOnly(ctx, destination)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = backup.Close() }()
	assertPragmaValue(t, backup.SQLDB(), "journal_mode", "delete")
	if err := backup.VerifyIntegrity(ctx); err != nil {
		t.Fatal(err)
	}
	var values []string
	if err := backup.GORMDB().Raw(`SELECT value FROM backup_probe ORDER BY value`).Scan(&values).Error; err != nil {
		t.Fatal(err)
	}
	if len(values) != 1 || values[0] != "committed" {
		t.Fatalf("backup values = %v, want [committed]", values)
	}
	if err := backup.Close(); err != nil {
		t.Fatal(err)
	}
	for _, sidecar := range []string{destination + "-wal", destination + "-shm", destination + "-journal"} {
		if _, err := os.Lstat(sidecar); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("reopened destination created sidecar %q: %v", filepath.Base(sidecar), err)
		}
	}
}

func TestBackupToCancellationRemovesPartialDestination(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	directory := t.TempDir()
	sourcePath := filepath.Join(directory, "source.sqlite")
	database, err := Open(ctx, sourcePath)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	if _, err := database.SQLDB().ExecContext(ctx, `
		CREATE TABLE large_backup_probe (payload BLOB NOT NULL) STRICT;
		INSERT INTO large_backup_probe (payload) VALUES (zeroblob(8388608))`); err != nil {
		t.Fatal(err)
	}

	backupContext, cancel := context.WithCancel(ctx)
	destination := filepath.Join(directory, "canceled.sqlite")
	err = database.backupTo(backupContext, destination, backupHooks{
		afterStep: func(more bool) {
			if more {
				cancel()
			}
		},
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("BackupTo() error = %v, want context.Canceled", err)
	}
	for _, candidate := range []string{destination, destination + "-wal", destination + "-shm", destination + "-journal"} {
		if _, err := os.Lstat(candidate); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("partial destination %q remains: %v", filepath.Base(candidate), err)
		}
	}
	partialFiles, err := filepath.Glob(filepath.Join(directory, ".canceled.sqlite-backup-*"))
	if err != nil {
		t.Fatal(err)
	}
	if len(partialFiles) != 0 {
		t.Fatalf("temporary backup files remain: %v", partialFiles)
	}
}

func TestBackupToRequiresAbsentDestination(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	directory := t.TempDir()
	database, err := Open(ctx, filepath.Join(directory, "source.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	destination := filepath.Join(directory, "existing.sqlite")
	want := []byte("do not replace")
	if err := os.WriteFile(destination, want, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := database.BackupTo(ctx, destination); !errors.Is(err, ErrAlreadyExists) {
		t.Fatalf("BackupTo() error = %v, want ErrAlreadyExists", err)
	}
	got, err := os.ReadFile(destination)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("existing destination = %q, want %q", got, want)
	}
}

func TestVerifyIntegrityRejectsForeignKeyViolations(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "foreign-key-violation.sqlite")
	database, err := Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	conn, err := database.SQLDB().Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := conn.ExecContext(ctx, `PRAGMA foreign_keys = OFF`); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.ExecContext(ctx, `
		CREATE TABLE integrity_parent (parent_id INTEGER PRIMARY KEY) STRICT;
		CREATE TABLE integrity_child (
			child_id INTEGER PRIMARY KEY,
			parent_id INTEGER NOT NULL REFERENCES integrity_parent(parent_id)
		) STRICT;
		INSERT INTO integrity_child (child_id, parent_id) VALUES (1, 999)`); err != nil {
		t.Fatal(err)
	}
	if err := conn.Close(); err != nil {
		t.Fatal(err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}

	readonly, err := OpenReadOnly(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer readonly.Close()
	if err := readonly.VerifyIntegrity(ctx); err == nil {
		t.Fatal("VerifyIntegrity() accepted a foreign-key violation")
	}
}

func TestVerifyCurrentMigrationsRequiresExactLedgerAndStableIdentity(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	open := func(name string) *DB {
		t.Helper()
		database, err := Open(ctx, filepath.Join(t.TempDir(), name+".sqlite"))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = database.Close() })
		return database
	}
	first := open("first")
	second := open("second")
	firstIdentity, err := first.VerifyCurrentMigrations(ctx, migrations.SQLite())
	if err != nil {
		t.Fatal(err)
	}
	secondIdentity, err := second.VerifyCurrentMigrations(ctx, migrations.SQLite())
	if err != nil {
		t.Fatal(err)
	}
	if firstIdentity != secondIdentity || firstIdentity.LatestVersion != 38 {
		t.Fatalf("migration identities differ: first=%+v second=%+v", firstIdentity, secondIdentity)
	}

	incomplete := open("incomplete")
	if _, err := incomplete.SQLDB().ExecContext(ctx, `DELETE FROM schema_migrations WHERE version = 38`); err != nil {
		t.Fatal(err)
	}
	if _, err := incomplete.VerifyCurrentMigrations(ctx, migrations.SQLite()); !errors.Is(err, ErrDatabaseNotCurrent) {
		t.Fatalf("incomplete ledger error = %v, want ErrDatabaseNotCurrent", err)
	}
	missingLedger := open("missing-ledger")
	if _, err := missingLedger.SQLDB().ExecContext(ctx, `DROP TABLE schema_migrations`); err != nil {
		t.Fatal(err)
	}
	if _, err := missingLedger.VerifyCurrentMigrations(ctx, migrations.SQLite()); !errors.Is(err, ErrDatabaseNotCurrent) {
		t.Fatalf("missing ledger error = %v, want ErrDatabaseNotCurrent", err)
	}

	drifted := open("drifted")
	if _, err := drifted.SQLDB().ExecContext(ctx, `UPDATE schema_migrations SET name = '0036_changed.sql' WHERE version = 36`); err != nil {
		t.Fatal(err)
	}
	if _, err := drifted.VerifyCurrentMigrations(ctx, migrations.SQLite()); !errors.Is(err, ErrMigrationDrift) {
		t.Fatalf("drifted ledger error = %v, want ErrMigrationDrift", err)
	}

	tooNew := open("too-new")
	if _, err := tooNew.SQLDB().ExecContext(ctx, `
		INSERT INTO schema_migrations (version, name, checksum, applied_at_unix_micro)
			VALUES (39, '0039_future.sql', zeroblob(32), 1)`); err != nil {
		t.Fatal(err)
	}
	if _, err := tooNew.VerifyCurrentMigrations(ctx, migrations.SQLite()); !errors.Is(err, ErrDatabaseTooNew) {
		t.Fatalf("too-new ledger error = %v, want ErrDatabaseTooNew", err)
	}
}

type fileState struct {
	Mode    os.FileMode
	Size    int64
	ModTime int64
	SHA256  [sha256.Size]byte
}

func readFileState(t *testing.T, path string) fileState {
	t.Helper()
	contents, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	info, err := os.Lstat(path)
	if err != nil {
		t.Fatal(err)
	}
	return fileState{
		Mode:    info.Mode(),
		Size:    info.Size(),
		ModTime: info.ModTime().UnixNano(),
		SHA256:  sha256.Sum256(contents),
	}
}
