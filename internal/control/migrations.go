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

	"github.com/Suhaibinator/open-splunk/internal/pgmeta"
)

const firstMigrationVersion = uint32(1)

var migrationFilename = regexp.MustCompile(`^([0-9]{4})_([a-z0-9][a-z0-9_]*)\.sql$`)

type migration struct {
	version  uint32
	name     string
	contents []byte
	checksum [sha256.Size]byte
}

type verifiedMigrationHistory struct {
	appliedCount uint32
}

// MigrationIdentity identifies one exact, ordered set of PostgreSQL migrations.
// It can be persisted alongside a backup manifest and compared without
// depending on the migrations' application timestamps.
type MigrationIdentity struct {
	LatestVersion uint32
	SHA256        [sha256.Size]byte
}

// VerifyCurrentMigrations verifies that the database migration ledger matches
// migrationFS exactly. Unlike
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
	var ledgerExists bool
	if err := db.sql.QueryRowContext(ctx, "SELECT to_regclass(current_schema() || '.open_splunk_schema_migrations') IS NOT NULL").Scan(&ledgerExists); err != nil {
		return MigrationIdentity{}, fmt.Errorf("inspect PostgreSQL migration ledger: %w", err)
	}
	if !ledgerExists {
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
	_, _ = hasher.Write([]byte("open-splunk/postgres-migrations/format-1\x00"))
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
// verification and all pending migrations run under one PostgreSQL advisory lock,
// preventing old and new binaries from racing schema versions at startup.
// Reapplying an unchanged set is safe; changing an applied file is rejected.
func ApplyMigrations(ctx context.Context, db *sql.DB, migrationFS fs.FS) error {
	if ctx == nil || db == nil || migrationFS == nil {
		return fmt.Errorf("%w: migration context, database, and filesystem are required", ErrInvalidArgument)
	}
	loaded, err := loadMigrations(migrationFS)
	if err != nil {
		return err
	}
	tx, err := pgmeta.BeginSQL(ctx, db, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return fmt.Errorf("begin PostgreSQL migrations: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(ctx, "SELECT pg_advisory_xact_lock(hashtextextended('open-splunk:migrations:' || current_database() || ':' || current_schema(),0))"); err != nil {
		return fmt.Errorf("lock PostgreSQL migrations: %w", err)
	}
	var ledgerExists, schemaExists bool
	if err := tx.QueryRowContext(ctx, `SELECT to_regclass(current_schema() || '.open_splunk_schema_migrations') IS NOT NULL,
 EXISTS(SELECT 1 FROM pg_tables WHERE schemaname=current_schema() AND tablename NOT LIKE 'gocommon_%' AND tablename <> 'open_splunk_additional_migrations')`).Scan(&ledgerExists, &schemaExists); err != nil {
		return err
	}
	if !ledgerExists && schemaExists {
		return fmt.Errorf("%w: ledgerless control schema exists", ErrMigrationDrift)
	}
	if _, err := tx.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS open_splunk_schema_migrations(version BIGINT PRIMARY KEY CHECK(version>=1), name TEXT NOT NULL, checksum BYTEA NOT NULL CHECK(octet_length(checksum)=32), applied_at_unix_micro BIGINT NOT NULL CHECK(applied_at_unix_micro>0))`); err != nil {
		return err
	}
	history, err := verifyMigrationHistory(ctx, tx, loaded)
	if err != nil {
		return err
	}
	for _, next := range loaded[int(history.appliedCount):] {
		if _, err := tx.ExecContext(ctx, string(next.contents)); err != nil {
			return fmt.Errorf("apply PostgreSQL migration %s: %w", next.name, err)
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO open_splunk_schema_migrations(version,name,checksum,applied_at_unix_micro) VALUES($1,$2,$3,$4)`, next.version, next.name, next.checksum[:], time.Now().UnixMicro()); err != nil {
			return err
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit PostgreSQL migrations: %w", err)
	}
	return nil
}

// ApplyAdditionalMigration applies a named, immutable application schema only
// after its external prerequisites (such as go-common identity) are initialized.
func (db *DB) ApplyAdditionalMigration(ctx context.Context, name, contents string) error {
	if ctx == nil || db == nil || strings.TrimSpace(name) == "" || strings.TrimSpace(contents) == "" {
		return ErrInvalidArgument
	}
	digest := sha256.Sum256([]byte(contents))
	tx, err := pgmeta.BeginSQL(ctx, db.sql, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(ctx, "SELECT pg_advisory_xact_lock(hashtextextended('open-splunk:migrations:' || current_database() || ':' || current_schema(),0))"); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS open_splunk_additional_migrations(name TEXT PRIMARY KEY,checksum BYTEA NOT NULL CHECK(octet_length(checksum)=32),applied_at_unix_micro BIGINT NOT NULL)`); err != nil {
		return err
	}
	var recorded []byte
	err = tx.QueryRowContext(ctx, "SELECT checksum FROM open_splunk_additional_migrations WHERE name=$1", name).Scan(&recorded)
	if err == nil {
		if !bytes.Equal(recorded, digest[:]) {
			return fmt.Errorf("%w: additional migration %s", ErrMigrationDrift, name)
		}
		return tx.Commit()
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	if _, err := tx.ExecContext(ctx, contents); err != nil {
		return fmt.Errorf("apply additional migration %s: %w", name, err)
	}
	if _, err := tx.ExecContext(ctx, "INSERT INTO open_splunk_additional_migrations VALUES($1,$2,$3)", name, digest[:], time.Now().UnixMicro()); err != nil {
		return err
	}
	return tx.Commit()
}

type migrationQuerier interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}

func verifyMigrationHistory(
	ctx context.Context,
	db migrationQuerier,
	loaded []migration,
) (verifiedMigrationHistory, error) {
	rows, err := db.QueryContext(ctx, `SELECT version, name, checksum FROM open_splunk_schema_migrations ORDER BY version`)
	if err != nil {
		return verifiedMigrationHistory{}, fmt.Errorf("read PostgreSQL migration history: %w", err)
	}
	defer rows.Close()

	var history verifiedMigrationHistory
	expectedVersion := firstMigrationVersion
	for rows.Next() {
		var version uint32
		var name string
		var checksum []byte
		if err := rows.Scan(&version, &name, &checksum); err != nil {
			return verifiedMigrationHistory{}, fmt.Errorf("scan PostgreSQL migration history: %w", err)
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
			return verifiedMigrationHistory{}, fmt.Errorf("%w: version %04d", ErrMigrationDrift, version)
		}
		expectedVersion++
	}
	if err := rows.Err(); err != nil {
		return verifiedMigrationHistory{}, fmt.Errorf("iterate PostgreSQL migration history: %w", err)
	}
	history.appliedCount = expectedVersion - firstMigrationVersion
	return history, nil
}

func loadMigrations(migrations fs.FS) ([]migration, error) {
	entries, err := fs.ReadDir(migrations, ".")
	if err != nil {
		return nil, fmt.Errorf("read PostgreSQL migrations: %w", err)
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
			return nil, fmt.Errorf("read PostgreSQL migration %s: %w", entry.Name(), err)
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
		return nil, fmt.Errorf("%w: no PostgreSQL migrations found", ErrInvalidArgument)
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
