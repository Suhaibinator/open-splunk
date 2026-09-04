package control

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	migrationLockRetryWindow                    = 30 * time.Second
	firstMigrationVersion                       = uint32(1)
	foldedIngestReservationAccountingVersion    = uint32(9)
	originalSQLiteBaselineSHA256                = "3ceec9b0c2f2a44edccff0b3e8b5cd0622fe72c462dc8c36616d9d7683bb2b75"
	foldedIngestWriteGroupsSQLiteBaselineSHA256 = "23e85b8b288addf86eda7f848e2b087a40194f8c5281459a9f0f8c1d215e1d64"
)

var migrationFilename = regexp.MustCompile(`^([0-9]{4})_([a-z0-9][a-z0-9_]*)\.sql$`)

type migration struct {
	version  uint32
	name     string
	contents []byte
	checksum [sha256.Size]byte
}

type verifiedMigrationHistory struct {
	appliedCount                      uint32
	foldedIngestReservationAccounting bool
}

// MigrationIdentity identifies one exact, ordered set of SQLite migrations.
// It can be persisted alongside a backup manifest and compared without
// depending on the migrations' application timestamps.
type MigrationIdentity struct {
	LatestVersion uint32
	SHA256        [sha256.Size]byte
}

// VerifyCurrentMigrations verifies that the database migration ledger matches
// migrationFS or a narrowly recognized compatible release history. Unlike
// ApplyMigrations, it never creates the ledger or applies missing migrations,
// making it safe for read-only backup tooling.
func (db *DB) VerifyCurrentMigrations(
	ctx context.Context,
	migrationFS fs.FS,
) (MigrationIdentity, error) {
	if ctx == nil || db == nil || db.sql == nil || migrationFS == nil {
		return MigrationIdentity{}, fmt.Errorf(
			"%w: migration context, database, and filesystem are required",
			ErrInvalidArgument,
		)
	}
	loaded, err := loadMigrations(migrationFS)
	if err != nil {
		return MigrationIdentity{}, err
	}
	var ledgerCount int
	if err := db.sql.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM sqlite_schema
		WHERE type = 'table' AND name = 'schema_migrations'`).Scan(&ledgerCount); err != nil {
		return MigrationIdentity{}, fmt.Errorf("inspect SQLite migration ledger: %w", err)
	}
	if ledgerCount != 1 {
		return MigrationIdentity{}, fmt.Errorf("%w: migration ledger is missing", ErrDatabaseNotCurrent)
	}
	history, err := verifyMigrationHistory(ctx, db.sql, loaded)
	if err != nil {
		return MigrationIdentity{}, err
	}
	if uint64(history.appliedCount) != uint64(len(loaded)) {
		appliedVersion := uint32(0)
		if history.appliedCount > 0 {
			appliedVersion = loaded[history.appliedCount-1].version
		}
		return MigrationIdentity{}, fmt.Errorf(
			"%w: database version %d, required version %d",
			ErrDatabaseNotCurrent,
			appliedVersion,
			loaded[len(loaded)-1].version,
		)
	}

	return migrationSetIdentity(loaded), nil
}

func migrationSetIdentity(loaded []migration) MigrationIdentity {
	hasher := sha256.New()
	_, _ = hasher.Write([]byte("open-splunk/sqlite-migrations/format-1\x00"))
	for _, item := range loaded {
		_, _ = hasher.Write([]byte(strconv.FormatUint(uint64(item.version), 10)))
		_, _ = hasher.Write([]byte{0})
		_, _ = hasher.Write([]byte(strconv.Itoa(len(item.name))))
		_, _ = hasher.Write([]byte{0})
		_, _ = hasher.Write([]byte(item.name))
		_, _ = hasher.Write(item.checksum[:])
	}
	var digest [sha256.Size]byte
	copy(digest[:], hasher.Sum(nil))
	return MigrationIdentity{
		LatestVersion: loaded[len(loaded)-1].version,
		SHA256:        digest,
	}
}

// ApplyMigrations applies a contiguous, ordered set of SQL migrations. History
// verification and all pending migrations run under one BEGIN IMMEDIATE lock,
// preventing old and new binaries from racing schema versions at startup.
// Reapplying an unchanged set is safe; changing an applied file is rejected
// unless it is the one explicitly recognized folded release baseline.
func ApplyMigrations(ctx context.Context, db *sql.DB, migrations fs.FS) (err error) {
	if ctx == nil || db == nil || migrations == nil {
		return fmt.Errorf("%w: migration context, database, and filesystem are required", ErrInvalidArgument)
	}
	loaded, err := loadMigrations(migrations)
	if err != nil {
		return err
	}

	conn, err := db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("acquire SQLite migration connection: %w", err)
	}
	defer func() {
		if closeErr := conn.Close(); closeErr != nil {
			err = errors.Join(err, fmt.Errorf("close SQLite migration connection: %w", closeErr))
		}
	}()
	if err := retrySQLiteBusy(ctx, migrationLockRetryWindow, func() error {
		_, err := conn.ExecContext(ctx, `BEGIN IMMEDIATE`)
		return err
	}); err != nil {
		return fmt.Errorf("begin SQLite migrations: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			if _, rollbackErr := conn.ExecContext(context.WithoutCancel(ctx), `ROLLBACK`); rollbackErr != nil {
				err = errors.Join(err, fmt.Errorf("roll back SQLite migrations: %w", rollbackErr))
			}
		}
	}()

	var ledgerExists, schemaExists bool
	if err := conn.QueryRowContext(ctx, `
		SELECT
			EXISTS (
				SELECT 1
				FROM sqlite_schema
				WHERE type = 'table' AND name = 'schema_migrations'
			),
			EXISTS (
				SELECT 1
				FROM sqlite_schema
			)`).Scan(&ledgerExists, &schemaExists); err != nil {
		return fmt.Errorf("inspect SQLite migration state: %w", err)
	}
	if !ledgerExists && schemaExists {
		return fmt.Errorf(
			"%w: ledgerless database contains existing schema",
			ErrMigrationDrift,
		)
	}

	if _, err := conn.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS schema_migrations (
			version INTEGER PRIMARY KEY NOT NULL CHECK (version >= 1),
			name TEXT NOT NULL,
			checksum BLOB NOT NULL CHECK (length(checksum) = 32),
			applied_at_unix_micro INTEGER NOT NULL CHECK (applied_at_unix_micro > 0)
		) STRICT`); err != nil {
		return fmt.Errorf("create SQLite migration ledger: %w", err)
	}

	history, err := verifyMigrationHistory(ctx, conn, loaded)
	if err != nil {
		return err
	}

	for _, next := range loaded[int(history.appliedCount):] {
		if history.foldedIngestReservationAccounting &&
			next.version == foldedIngestReservationAccountingVersion &&
			next.name == "0009_ingest_reservation_accounting.sql" {
			if err := recordAppliedMigration(ctx, conn, next); err != nil {
				return fmt.Errorf("adopt folded SQLite migration %s: %w", next.name, err)
			}
			continue
		}
		if err := applyPendingMigration(ctx, conn, next); err != nil {
			return err
		}
	}
	if _, err := conn.ExecContext(ctx, `COMMIT`); err != nil {
		return fmt.Errorf("commit SQLite migrations: %w", err)
	}
	committed = true
	return nil
}

type migrationQuerier interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}

func verifyMigrationHistory(
	ctx context.Context,
	db migrationQuerier,
	loaded []migration,
) (verifiedMigrationHistory, error) {
	rows, err := db.QueryContext(ctx, `SELECT version, name, checksum FROM schema_migrations ORDER BY version`)
	if err != nil {
		return verifiedMigrationHistory{}, fmt.Errorf("read SQLite migration history: %w", err)
	}
	defer rows.Close()

	var history verifiedMigrationHistory
	expectedVersion := firstMigrationVersion
	for rows.Next() {
		var version uint32
		var name string
		var checksum []byte
		if err := rows.Scan(&version, &name, &checksum); err != nil {
			return verifiedMigrationHistory{}, fmt.Errorf("scan SQLite migration history: %w", err)
		}
		if version > loaded[len(loaded)-1].version {
			return verifiedMigrationHistory{}, fmt.Errorf("%w: database version %d, latest embedded version %d", ErrDatabaseTooNew, version, loaded[len(loaded)-1].version)
		}
		if version != expectedVersion {
			return verifiedMigrationHistory{}, fmt.Errorf("%w: migration history skips version %04d", ErrMigrationDrift, expectedVersion)
		}
		embedded := loaded[version-firstMigrationVersion]
		if name != embedded.name {
			return verifiedMigrationHistory{}, fmt.Errorf("%w: version %04d", ErrMigrationDrift, version)
		}
		if !bytes.Equal(checksum, embedded.checksum[:]) {
			if !isFoldedIngestWriteGroupsBaseline(embedded, checksum) {
				return verifiedMigrationHistory{}, fmt.Errorf("%w: version %04d", ErrMigrationDrift, version)
			}
			history.foldedIngestReservationAccounting = true
		}
		expectedVersion++
	}
	if err := rows.Err(); err != nil {
		return verifiedMigrationHistory{}, fmt.Errorf("iterate SQLite migration history: %w", err)
	}
	history.appliedCount = expectedVersion - firstMigrationVersion
	return history, nil
}

func isFoldedIngestWriteGroupsBaseline(embedded migration, appliedChecksum []byte) bool {
	return embedded.version == firstMigrationVersion &&
		embedded.name == "0001_baseline.sql" &&
		hex.EncodeToString(embedded.checksum[:]) == originalSQLiteBaselineSHA256 &&
		hex.EncodeToString(appliedChecksum) == foldedIngestWriteGroupsSQLiteBaselineSHA256
}

func applyPendingMigration(
	ctx context.Context,
	conn *sql.Conn,
	next migration,
) error {
	if _, err := conn.ExecContext(ctx, string(next.contents)); err != nil {
		return fmt.Errorf("apply SQLite migration %s: %w", next.name, err)
	}
	if err := recordAppliedMigration(ctx, conn, next); err != nil {
		return err
	}

	return nil
}

func recordAppliedMigration(ctx context.Context, conn *sql.Conn, next migration) error {
	if _, err := conn.ExecContext(ctx, `
		INSERT INTO schema_migrations (version, name, checksum, applied_at_unix_micro)
		VALUES (?, ?, ?, CAST(unixepoch('subsec') * 1000000 AS INTEGER))`,
		next.version, next.name, next.checksum[:]); err != nil {
		return fmt.Errorf("record SQLite migration %s: %w", next.name, err)
	}

	return nil
}

func loadMigrations(migrations fs.FS) ([]migration, error) {
	entries, err := fs.ReadDir(migrations, ".")
	if err != nil {
		return nil, fmt.Errorf("read SQLite migrations: %w", err)
	}
	loaded := make([]migration, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".sql") {
			continue
		}
		matches := migrationFilename.FindStringSubmatch(entry.Name())
		if matches == nil {
			return nil, fmt.Errorf("%w: invalid migration filename %q", ErrInvalidArgument, entry.Name())
		}
		version := parseMigrationVersion(matches[1])
		contents, err := fs.ReadFile(migrations, entry.Name())
		if err != nil {
			return nil, fmt.Errorf("read SQLite migration %s: %w", entry.Name(), err)
		}
		if len(bytes.TrimSpace(contents)) == 0 {
			return nil, fmt.Errorf("%w: migration %q is empty", ErrInvalidArgument, entry.Name())
		}
		loaded = append(loaded, migration{
			version:  version,
			name:     entry.Name(),
			contents: contents,
			checksum: sha256.Sum256(contents),
		})
	}
	if len(loaded) == 0 {
		return nil, fmt.Errorf("%w: no SQLite migrations found", ErrInvalidArgument)
	}
	sort.Slice(loaded, func(i, j int) bool { return loaded[i].version < loaded[j].version })
	wantVersion := firstMigrationVersion
	for _, item := range loaded {
		if item.version != wantVersion {
			return nil, fmt.Errorf("%w: migration %q has version %04d, want %04d", ErrInvalidArgument, item.name, item.version, wantVersion)
		}
		wantVersion++
	}
	return loaded, nil
}

func parseMigrationVersion(value string) uint32 {
	var version uint32
	for index := 0; index < len(value); index++ {
		version = version*10 + uint32(value[index]-'0')
	}
	return version
}
