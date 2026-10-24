package control

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"database/sql/driver"
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
	// 0x6f73_0000_0000_0002 is taken by the durable_search_jobs cursor lock
	// (migrations/postgres/0020_sqlite_parity_repairs.sql).
	postgresMigrationAdvisoryLockKey = int64(0x6f73_0000_0000_0001)

	postgresMigrationIdentityFormat = "open-splunk/postgres-migrations/format-1\x00"
)

// ApplyPostgresMigrations applies a contiguous, ordered set of SQL migrations
// to the open_splunk schema. Ledger verification and every pending migration
// run in one READ COMMITTED transaction holding pg_advisory_xact_lock, so
// concurrent runners serialize whatever the server's default isolation is, a
// failed migration leaves no partial schema, and a crashed runner releases
// the lock with its session. Reapplying an unchanged set is a no-op; any
// rewritten, renamed, gapped, or unknown ledger entry, or an empty ledger
// over existing objects, is refused as drift.
//
// The run uses one pooled session and discards it afterwards, so session
// state a migration sets (a plain SET, say) never reaches later users of the
// pool. A session holding temporary relations is refused, because they would
// shadow the control tables that migration bodies name unqualified, and a
// migration that ends the runner's transaction (COMMIT, ROLLBACK and their
// synonyms) is refused before its ledger row is written.
//
// The migration lock only serializes runners. A server must also hold the
// single-active-server lock while it migrates; OpenPostgresServer does both.
func ApplyPostgresMigrations(ctx context.Context, db *sql.DB, migrationFS fs.FS) (err error) {
	if ctx == nil || db == nil || migrationFS == nil {
		return fmt.Errorf("%w: migration context, database, and filesystem are required", ErrInvalidArgument)
	}
	loaded, err := loadPostgresMigrations(migrationFS)
	if err != nil {
		return err
	}

	conn, err := db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("reserve PostgreSQL migration session: %w", err)
	}
	defer func() {
		err = errors.Join(err, discardPostgresMigrationSession(conn))
	}()

	// READ COMMITTED gives every statement a fresh snapshot, so a runner that
	// waited for the lock inspects the catalog as the previous runner left
	// it. At REPEATABLE READ or SERIALIZABLE (a database or role default) the
	// snapshot would be taken by the lock statement, before the lock is
	// granted.
	tx, err := conn.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
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

	// Migration bodies use unqualified names. Pin them to the control schema
	// before running anything, even when the caller's pool does not, so
	// neither the lock call nor a migration resolves a name through the
	// caller's search_path and a migration can never create objects in public
	// or another schema. Each migration re-pins it, with the string
	// semantics, below.
	if err := pinPostgresMigrationSession(ctx, tx); err != nil {
		return err
	}
	var hasTemporaryRelations bool
	if err := tx.QueryRowContext(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM pg_catalog.pg_class WHERE relnamespace = pg_catalog.pg_my_temp_schema()
		)`).Scan(&hasTemporaryRelations); err != nil {
		return fmt.Errorf("inspect PostgreSQL migration session: %w", err)
	}
	if hasTemporaryRelations {
		return fmt.Errorf(
			"%w: the PostgreSQL migration session holds temporary relations, which would shadow control tables",
			ErrInvalidArgument,
		)
	}

	lockContext, cancelLock := context.WithTimeout(ctx, migrationLockRetryWindow)
	_, lockErr := tx.ExecContext(lockContext, `SELECT pg_catalog.pg_advisory_xact_lock($1)`, postgresMigrationAdvisoryLockKey)
	cancelLock()
	if lockErr != nil {
		return fmt.Errorf("acquire PostgreSQL migration lock: %w", lockErr)
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
	if appliedCount == 0 && schemaHasObjects {
		return fmt.Errorf(
			"%w: empty migration ledger over a %s schema that contains objects",
			ErrMigrationDrift,
			PostgresSchema,
		)
	}
	pending := loaded[int(appliedCount):]
	// The transaction ID identifies the runner's transaction; a migration
	// that commits or rolls it back leaves the session without it. It is
	// assigned only when there is work, so a no-op run stays read-only.
	var transactionID string
	if len(pending) > 0 {
		if err := tx.QueryRowContext(ctx, `SELECT pg_catalog.pg_current_xact_id()::text`).Scan(&transactionID); err != nil {
			return fmt.Errorf("identify PostgreSQL migration transaction: %w", err)
		}
	}
	for _, next := range pending {
		// A SET or SET LOCAL in an earlier migration must not change where
		// this one's unqualified names resolve or how its literals parse.
		if err := pinPostgresMigrationSession(ctx, tx); err != nil {
			return err
		}
		// A migration body has no bind parameters, so pgx sends it with the
		// simple query protocol and multi-statement files run as written.
		if _, err := tx.ExecContext(ctx, string(next.contents)); err != nil {
			return fmt.Errorf("apply PostgreSQL migration %s: %w", next.name, err)
		}
		// The simple protocol lets a COMMIT or ROLLBACK in the body end the
		// runner's transaction, releasing the migration lock and leaving the
		// rest to autocommit. That cannot be undone here, but it is caught
		// before the ledger records the migration or a later one runs.
		var currentTransactionID sql.NullString
		if err := tx.QueryRowContext(ctx,
			`SELECT pg_catalog.pg_current_xact_id_if_assigned()::text`,
		).Scan(&currentTransactionID); err != nil {
			return fmt.Errorf("check PostgreSQL migration %s transaction: %w", next.name, err)
		}
		if !currentTransactionID.Valid || currentTransactionID.String != transactionID {
			return fmt.Errorf(
				"%w: PostgreSQL migration %s ended the runner's transaction; migrations must not contain transaction control",
				ErrInvalidArgument,
				next.name,
			)
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

// pinPostgresMigrationSession pins, for the runner's transaction, the session
// settings the migration files are written against. Unqualified names resolve
// in the control schema. The files also spell bytea and regex literals with
// backslashes ('\x', '[\u0001-\u001f]', strpos(x, '\')), which mean what
// they say only with standard_conforming_strings on: a database or role
// default of off would parse '\x'::bytea as the byte 'x'. The caller's pool
// may not pin these, nor the encoding pgx speaks, so pin all of them.
func pinPostgresMigrationSession(ctx context.Context, tx *sql.Tx) error {
	if _, err := tx.ExecContext(ctx, `SET LOCAL search_path TO `+PostgresSchema); err != nil {
		return fmt.Errorf("pin PostgreSQL migration search_path: %w", err)
	}
	if _, err := tx.ExecContext(ctx,
		`SET LOCAL standard_conforming_strings TO on; SET LOCAL client_encoding TO 'UTF8'`,
	); err != nil {
		return fmt.Errorf("pin PostgreSQL migration string semantics: %w", err)
	}
	return nil
}

// discardPostgresMigrationSession closes the migration session instead of
// returning it to the pool. Returning driver.ErrBadConn from Raw is how
// database/sql is told to drop a connection.
func discardPostgresMigrationSession(conn *sql.Conn) error {
	if err := conn.Raw(func(any) error { return driver.ErrBadConn }); err != nil &&
		!errors.Is(err, driver.ErrBadConn) && !errors.Is(err, sql.ErrConnDone) {
		return fmt.Errorf("discard PostgreSQL migration session: %w", err)
	}
	if err := conn.Close(); err != nil && !errors.Is(err, sql.ErrConnDone) {
		return fmt.Errorf("close PostgreSQL migration session: %w", err)
	}
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
// ledger exists, and whether the schema holds any relation other than the
// ledger table and its primary key, any routine, or any user-defined type.
// Table row types are covered by their pg_class entry.
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
				AND (c.relname, c.relkind) NOT IN (('schema_migrations', 'r'), ('schema_migrations_pkey', 'i'))
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
		name text COLLATE "C" NOT NULL CHECK (name <> ''),
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
		// Any spelling of the .sql extension is a migration candidate, so a
		// file saved as NNNN_name.SQL is rejected rather than silently left
		// out of the set.
		if entry.IsDir() || !strings.HasSuffix(strings.ToLower(entry.Name()), ".sql") {
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
