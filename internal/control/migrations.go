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
)

var migrationFilename = regexp.MustCompile(`^([0-9]{4})_([a-z0-9][a-z0-9_]*)\.sql$`)

type migration struct {
	version  int
	name     string
	contents []byte
	checksum [sha256.Size]byte
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
	if _, err := conn.ExecContext(ctx, `BEGIN IMMEDIATE`); err != nil {
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

	if err := verifyMigrationHistory(ctx, conn, loaded); err != nil {
		return err
	}

	for _, next := range loaded {
		if err := applyMigration(ctx, conn, next); err != nil {
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

func verifyMigrationHistory(ctx context.Context, db migrationQuerier, loaded []migration) error {
	rows, err := db.QueryContext(ctx, `SELECT version, name, checksum FROM schema_migrations ORDER BY version`)
	if err != nil {
		return fmt.Errorf("read SQLite migration history: %w", err)
	}
	defer rows.Close()

	expectedVersion := 1
	for rows.Next() {
		var version int
		var name string
		var checksum []byte
		if err := rows.Scan(&version, &name, &checksum); err != nil {
			return fmt.Errorf("scan SQLite migration history: %w", err)
		}
		if version > len(loaded) {
			return fmt.Errorf("%w: database version %d, latest embedded version %d", ErrDatabaseTooNew, version, loaded[len(loaded)-1].version)
		}
		if version != expectedVersion {
			return fmt.Errorf("%w: migration history skips version %04d", ErrMigrationDrift, expectedVersion)
		}
		embedded := loaded[version-1]
		if name != embedded.name || !bytes.Equal(checksum, embedded.checksum[:]) {
			return fmt.Errorf("%w: version %04d", ErrMigrationDrift, version)
		}
		expectedVersion++
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate SQLite migration history: %w", err)
	}
	return nil
}

func applyMigration(ctx context.Context, conn *sql.Conn, next migration) error {
	var appliedName string
	var appliedChecksum []byte
	queryErr := conn.QueryRowContext(ctx, `
		SELECT name, checksum
		FROM schema_migrations
		WHERE version = ?`, next.version).Scan(&appliedName, &appliedChecksum)
	switch {
	case queryErr == nil:
		if appliedName != next.name || !bytes.Equal(appliedChecksum, next.checksum[:]) {
			return fmt.Errorf("%w: version %04d", ErrMigrationDrift, next.version)
		}
	case errors.Is(queryErr, sql.ErrNoRows):
		if _, err := conn.ExecContext(ctx, string(next.contents)); err != nil {
			return fmt.Errorf("apply SQLite migration %s: %w", next.name, err)
		}
		if _, err := conn.ExecContext(ctx, `
			INSERT INTO schema_migrations (version, name, checksum, applied_at_unix_micro)
			VALUES (?, ?, ?, CAST(unixepoch('subsec') * 1000000 AS INTEGER))`,
			next.version, next.name, next.checksum[:]); err != nil {
			return fmt.Errorf("record SQLite migration %s: %w", next.name, err)
		}
	default:
		return fmt.Errorf("inspect SQLite migration %s: %w", next.name, queryErr)
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
		version, err := strconv.Atoi(matches[1])
		if err != nil {
			return nil, fmt.Errorf("%w: parse migration version in %q", ErrInvalidArgument, entry.Name())
		}
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
	for i, item := range loaded {
		wantVersion := i + 1
		if item.version != wantVersion {
			return nil, fmt.Errorf("%w: migration %q has version %04d, want %04d", ErrInvalidArgument, item.name, item.version, wantVersion)
		}
	}
	return loaded, nil
}
