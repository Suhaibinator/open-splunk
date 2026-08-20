package control

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
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
	migrationLockRetryWindow = 30 * time.Second
	firstMigrationVersion    = uint32(1)
)

var migrationFilename = regexp.MustCompile(`^([0-9]{4})_([a-z0-9][a-z0-9_]*)\.sql$`)

type migration struct {
	version  uint32
	name     string
	contents []byte
	checksum [sha256.Size]byte
}

// MigrationIdentity identifies one exact, ordered set of SQLite migrations.
// It can be persisted alongside a backup manifest and compared without
// depending on the migrations' application timestamps.
type MigrationIdentity struct {
	LatestVersion uint32
	SHA256        [sha256.Size]byte
}

// VerifyCurrentMigrations verifies that the database migration ledger exactly
// matches migrationFS. Unlike ApplyMigrations, it never creates the ledger or
// applies missing migrations, making it safe for read-only backup tooling.
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
	appliedCount, err := verifyMigrationHistory(ctx, db.sql, loaded)
	if err != nil {
		return MigrationIdentity{}, err
	}
	if appliedCount != uint32(len(loaded)) {
		appliedVersion := uint32(0)
		if appliedCount > 0 {
			appliedVersion = loaded[appliedCount-1].version
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
// Reapplying an unchanged set is safe; changing an applied file is rejected.
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

	if _, err := conn.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS schema_migrations (
			version INTEGER PRIMARY KEY NOT NULL CHECK (version >= 1),
			name TEXT NOT NULL,
			checksum BLOB NOT NULL CHECK (length(checksum) = 32),
			applied_at_unix_micro INTEGER NOT NULL CHECK (applied_at_unix_micro > 0)
		) STRICT`); err != nil {
		return fmt.Errorf("create SQLite migration ledger: %w", err)
	}

	appliedCount, err := verifyMigrationHistory(ctx, conn, loaded)
	if err != nil {
		return err
	}

	for _, next := range loaded[int(appliedCount):] {
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
) (uint32, error) {
	rows, err := db.QueryContext(ctx, `SELECT version, name, checksum FROM schema_migrations ORDER BY version`)
	if err != nil {
		return 0, fmt.Errorf("read SQLite migration history: %w", err)
	}
	defer rows.Close()

	expectedVersion := firstMigrationVersion
	for rows.Next() {
		var version uint32
		var name string
		var checksum []byte
		if err := rows.Scan(&version, &name, &checksum); err != nil {
			return 0, fmt.Errorf("scan SQLite migration history: %w", err)
		}
		if version > loaded[len(loaded)-1].version {
			return 0, fmt.Errorf("%w: database version %d, latest embedded version %d", ErrDatabaseTooNew, version, loaded[len(loaded)-1].version)
		}
		if version != expectedVersion {
			return 0, fmt.Errorf("%w: migration history skips version %04d", ErrMigrationDrift, expectedVersion)
		}
		embedded := loaded[version-firstMigrationVersion]
		if name != embedded.name || !bytes.Equal(checksum, embedded.checksum[:]) {
			return 0, fmt.Errorf("%w: version %04d", ErrMigrationDrift, version)
		}
		expectedVersion++
	}
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("iterate SQLite migration history: %w", err)
	}
	return expectedVersion - firstMigrationVersion, nil
}

func applyPendingMigration(
	ctx context.Context,
	conn *sql.Conn,
	next migration,
) error {
	if _, err := conn.ExecContext(ctx, string(next.contents)); err != nil {
		return fmt.Errorf(
			"apply SQLite migration %s (existing pre-baseline state is unsupported; provision a fresh state database): %w",
			next.name,
			err,
		)
	}
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
