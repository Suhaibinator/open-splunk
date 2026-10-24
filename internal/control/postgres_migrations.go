package control

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"errors"
	"fmt"
	"io/fs"
	"sort"
	"strconv"
	"strings"
)

const (
	// postgresMigrationAdvisoryLockKey serializes every migration runner on
	// one database. Advisory locks are scoped per database, and the 0x6f73
	// ("os") prefix keeps open-splunk keys apart from other tenants of the
	// shared database, which use hashtext-derived two-key locks.
	postgresMigrationAdvisoryLockKey = int64(0x6f73_0000_0000_0001)

	postgresMigrationIdentityFormat = "open-splunk/postgres-migrations/format-1\x00"
)

// ApplyPostgresMigrations applies a contiguous, ordered set of SQL migrations
// to the open_splunk schema. Ledger verification and every pending migration
// run in one transaction holding pg_advisory_xact_lock, so concurrent server
// starts serialize, a failed migration leaves no partial schema, and a
// crashed runner releases the lock with its session. Reapplying an unchanged
// set is a no-op; any rewritten, renamed, gapped, or unknown ledger entry is
// refused as drift.
func ApplyPostgresMigrations(ctx context.Context, db *sql.DB, migrationFS fs.FS) (err error) {
	if ctx == nil || db == nil || migrationFS == nil {
		return fmt.Errorf("%w: migration context, database, and filesystem are required", ErrInvalidArgument)
	}
	loaded, err := loadPostgresMigrations(migrationFS)
	if err != nil {
		return err
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin PostgreSQL migrations: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			if rollbackErr := tx.Rollback(); rollbackErr != nil && !errors.Is(rollbackErr, sql.ErrTxDone) {
				err = errors.Join(err, fmt.Errorf("roll back PostgreSQL migrations: %w", rollbackErr))
			}
		}
	}()

	lockContext, cancelLock := context.WithTimeout(ctx, migrationLockRetryWindow)
	_, lockErr := tx.ExecContext(lockContext, `SELECT pg_advisory_xact_lock($1)`, postgresMigrationAdvisoryLockKey)
	cancelLock()
	if lockErr != nil {
		return fmt.Errorf("acquire PostgreSQL migration lock: %w", lockErr)
	}
	// Migration bodies use unqualified names. Pin them to the control schema
	// for this transaction even when the caller's pool does not, so a
	// migration can never create objects in public or another schema.
	if _, err := tx.ExecContext(ctx, `SET LOCAL search_path TO `+PostgresSchema); err != nil {
		return fmt.Errorf("pin PostgreSQL migration search_path: %w", err)
	}

	var schemaExists, ledgerExists, schemaHasObjects bool
	if err := tx.QueryRowContext(ctx, postgresMigrationStateQuery, PostgresSchema).
		Scan(&schemaExists, &ledgerExists, &schemaHasObjects); err != nil {
		return fmt.Errorf("inspect PostgreSQL migration state: %w", err)
	}
	if !ledgerExists && schemaHasObjects {
		return fmt.Errorf(
			"%w: ledgerless %s schema contains existing objects",
			ErrMigrationDrift,
			PostgresSchema,
		)
	}
	if !schemaExists {
		if _, err := tx.ExecContext(ctx, `CREATE SCHEMA `+PostgresSchema); err != nil {
			return fmt.Errorf("create PostgreSQL %s schema: %w", PostgresSchema, err)
		}
	}
	if !ledgerExists {
		if _, err := tx.ExecContext(ctx, postgresMigrationLedgerDDL); err != nil {
			return fmt.Errorf("create PostgreSQL migration ledger: %w", err)
		}
	}

	appliedCount, err := verifyPostgresMigrationHistory(ctx, tx, loaded)
	if err != nil {
		return err
	}
	for _, next := range loaded[int(appliedCount):] {
		// A migration body has no bind parameters, so pgx sends it with the
		// simple query protocol and multi-statement files run as written.
		if _, err := tx.ExecContext(ctx, string(next.contents)); err != nil {
			return fmt.Errorf("apply PostgreSQL migration %s: %w", next.name, err)
		}
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO open_splunk.schema_migrations (version, name, checksum, applied_at_unix_micro)
			VALUES ($1, $2, $3, (extract(epoch FROM clock_timestamp()) * 1000000)::bigint)`,
			int64(next.version), next.name, next.checksum[:]); err != nil {
			return fmt.Errorf("record PostgreSQL migration %s: %w", next.name, err)
		}
	}
	if err := tx.Commit(); err != nil {
		committed = true
		return fmt.Errorf("commit PostgreSQL migrations: %w", err)
	}
	committed = true
	return nil
}

// VerifyCurrentPostgresMigrations verifies that the database ledger records
// exactly migrationFS. Unlike ApplyPostgresMigrations it never creates the
// schema or ledger and never applies a migration, so it is safe for read-only
// verification and backup tooling. It reads one consistent snapshot.
func VerifyCurrentPostgresMigrations(
	ctx context.Context,
	db *sql.DB,
	migrationFS fs.FS,
) (identity MigrationIdentity, err error) {
	if ctx == nil || db == nil || migrationFS == nil {
		return MigrationIdentity{}, fmt.Errorf(
			"%w: migration context, database, and filesystem are required",
			ErrInvalidArgument,
		)
	}
	loaded, err := loadPostgresMigrations(migrationFS)
	if err != nil {
		return MigrationIdentity{}, err
	}
	tx, err := db.BeginTx(ctx, postgresReadTxOptions)
	if err != nil {
		return MigrationIdentity{}, fmt.Errorf("begin PostgreSQL migration verification: %w", err)
	}
	defer func() {
		if rollbackErr := tx.Rollback(); rollbackErr != nil && !errors.Is(rollbackErr, sql.ErrTxDone) {
			err = errors.Join(err, fmt.Errorf("end PostgreSQL migration verification: %w", rollbackErr))
		}
	}()

	var schemaExists, ledgerExists, schemaHasObjects bool
	if err := tx.QueryRowContext(ctx, postgresMigrationStateQuery, PostgresSchema).
		Scan(&schemaExists, &ledgerExists, &schemaHasObjects); err != nil {
		return MigrationIdentity{}, fmt.Errorf("inspect PostgreSQL migration ledger: %w", err)
	}
	if !ledgerExists {
		return MigrationIdentity{}, fmt.Errorf("%w: migration ledger is missing", ErrDatabaseNotCurrent)
	}
	appliedCount, err := verifyPostgresMigrationHistory(ctx, tx, loaded)
	if err != nil {
		return MigrationIdentity{}, err
	}
	if uint64(appliedCount) != uint64(len(loaded)) {
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
	return postgresMigrationSetIdentity(loaded), nil
}

// PostgresMigrationSetIdentity returns the identity of migrationFS without
// consulting a database. Test fixtures key reusable template databases by it.
func PostgresMigrationSetIdentity(migrationFS fs.FS) (MigrationIdentity, error) {
	if migrationFS == nil {
		return MigrationIdentity{}, fmt.Errorf("%w: migration filesystem is required", ErrInvalidArgument)
	}
	loaded, err := loadPostgresMigrations(migrationFS)
	if err != nil {
		return MigrationIdentity{}, err
	}
	return postgresMigrationSetIdentity(loaded), nil
}

// postgresMigrationStateQuery reports whether the schema exists, whether the
// ledger exists, and whether the schema holds any relation, routine, or
// user-defined type. Table row types are covered by their pg_class entry.
const postgresMigrationStateQuery = `
	SELECT
		EXISTS (SELECT 1 FROM pg_catalog.pg_namespace WHERE nspname = $1),
		EXISTS (
			SELECT 1
			FROM pg_catalog.pg_class AS c
			JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace
			WHERE n.nspname = $1 AND c.relname = 'schema_migrations' AND c.relkind = 'r'
		),
		EXISTS (
			SELECT 1
			FROM pg_catalog.pg_class AS c
			JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace
			WHERE n.nspname = $1
		) OR EXISTS (
			SELECT 1
			FROM pg_catalog.pg_proc AS p
			JOIN pg_catalog.pg_namespace AS n ON n.oid = p.pronamespace
			WHERE n.nspname = $1
		) OR EXISTS (
			SELECT 1
			FROM pg_catalog.pg_type AS t
			JOIN pg_catalog.pg_namespace AS n ON n.oid = t.typnamespace
			WHERE n.nspname = $1 AND t.typtype IN ('d', 'e', 'r', 'm')
		)`

const postgresMigrationLedgerDDL = `
	CREATE TABLE open_splunk.schema_migrations (
		version bigint PRIMARY KEY CHECK (version >= 1),
		name text NOT NULL CHECK (name <> ''),
		checksum bytea NOT NULL CHECK (octet_length(checksum) = 32),
		applied_at_unix_micro bigint NOT NULL CHECK (applied_at_unix_micro > 0)
	)`

func verifyPostgresMigrationHistory(
	ctx context.Context,
	tx *sql.Tx,
	loaded []migration,
) (uint32, error) {
	rows, err := tx.QueryContext(ctx, `
		SELECT version, name, checksum
		FROM open_splunk.schema_migrations
		ORDER BY version`)
	if err != nil {
		return 0, fmt.Errorf("read PostgreSQL migration history: %w", err)
	}
	defer rows.Close()

	latest := loaded[len(loaded)-1].version
	expectedVersion := firstMigrationVersion
	for rows.Next() {
		var version int64
		var name string
		var checksum []byte
		if err := rows.Scan(&version, &name, &checksum); err != nil {
			return 0, fmt.Errorf("scan PostgreSQL migration history: %w", err)
		}
		if version > int64(latest) {
			return 0, fmt.Errorf(
				"%w: database version %d, latest embedded version %d",
				ErrDatabaseTooNew,
				version,
				latest,
			)
		}
		if version != int64(expectedVersion) {
			return 0, fmt.Errorf("%w: migration history skips version %04d", ErrMigrationDrift, expectedVersion)
		}
		embedded := loaded[expectedVersion-firstMigrationVersion]
		if name != embedded.name || !bytes.Equal(checksum, embedded.checksum[:]) {
			return 0, fmt.Errorf("%w: version %04d", ErrMigrationDrift, expectedVersion)
		}
		expectedVersion++
	}
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("iterate PostgreSQL migration history: %w", err)
	}
	return expectedVersion - firstMigrationVersion, nil
}

func loadPostgresMigrations(migrationFS fs.FS) ([]migration, error) {
	entries, err := fs.ReadDir(migrationFS, ".")
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
		contents, err := fs.ReadFile(migrationFS, entry.Name())
		if err != nil {
			return nil, fmt.Errorf("read PostgreSQL migration %s: %w", entry.Name(), err)
		}
		if len(bytes.TrimSpace(contents)) == 0 {
			return nil, fmt.Errorf("%w: migration %q is empty", ErrInvalidArgument, entry.Name())
		}
		loaded = append(loaded, migration{
			version:  parseMigrationVersion(matches[1]),
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
			return nil, fmt.Errorf(
				"%w: migration %q has version %04d, want %04d",
				ErrInvalidArgument,
				item.name,
				item.version,
				wantVersion,
			)
		}
		wantVersion++
	}
	return loaded, nil
}

func postgresMigrationSetIdentity(loaded []migration) MigrationIdentity {
	hasher := sha256.New()
	_, _ = hasher.Write([]byte(postgresMigrationIdentityFormat))
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
