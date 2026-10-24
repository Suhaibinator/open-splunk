package control

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io/fs"
	"strings"
	"sync"
	"time"

	"github.com/Suhaibinator/open-splunk/migrations"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
	gormpostgres "gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

const (
	// PostgresSchema is the schema that holds every open-splunk control-plane
	// object. Each pooled connection pins search_path to it so unqualified
	// names can never resolve into another tenant of the same database, such
	// as the identity service's tables.
	PostgresSchema = "open_splunk"

	defaultPostgresMaxOpenConns = 16
	postgresApplicationName     = "open-splunk-server"
)

// postgresPinnedSessionParameters are the session settings every
// control-plane connection starts with, whatever the connection string or the
// database and role defaults say.
//   - client_encoding: pgx always sends and expects UTF-8. Under another
//     client encoding the server would reinterpret every byte: the two UTF-8
//     bytes of U+00E9 would be stored as two LATIN1 characters, and an emoji
//     as characters including C1 controls that the schema's
//     control-character CHECKs refuse.
//   - standard_conforming_strings: with it off, a backslash in a SQL string
//     literal is an escape, so '\x'::bytea is the byte 'x' and strpos(x, '\')
//     no longer parses.
var postgresPinnedSessionParameters = []struct{ name, value string }{
	{name: "search_path", value: PostgresSchema},
	{name: "timezone", value: "UTC"},
	{name: "client_encoding", value: "UTF8"},
	{name: "standard_conforming_strings", value: "on"},
}

// PostgresDB is the PostgreSQL-backed control-plane database. SQLDB and
// GORMDB share one connection pool; WriteTx and ReadTx are the supported
// transaction entry points.
type PostgresDB struct {
	sql        *sql.DB
	orm        *gorm.DB
	connConfig *pgx.ConnConfig

	// writeQueue serializes write transactions while any is being retried.
	writeQueue *postgresWriteQueue

	admission admissionGates

	mu         sync.Mutex
	closed     bool
	singletons map[*serverSingleton]struct{}
	// singletonLost is closed, and singletonLostErr set, once a server
	// singleton lock held through this handle is found lost. From then on
	// the handle refuses write transactions.
	singletonLost    chan struct{}
	singletonLostErr error
}

// OpenPostgres connects to the PostgreSQL control plane named by dsn, pins
// every pooled connection to the open_splunk schema, and applies the embedded
// PostgreSQL migrations before returning. It takes no server lock: the
// server process uses OpenPostgresServer, which fences migrations with the
// single-active-server lock.
func OpenPostgres(ctx context.Context, dsn string, options ...PostgresOption) (*PostgresDB, error) {
	return OpenPostgresWithMigrations(ctx, dsn, migrations.Postgres(), options...)
}

// OpenPostgresWithMigrations is OpenPostgres with an explicit ordered
// migration set. Production uses the embedded set; tests use fixtures.
func OpenPostgresWithMigrations(
	ctx context.Context,
	dsn string,
	migrationFS fs.FS,
	options ...PostgresOption,
) (*PostgresDB, error) {
	connConfig, resolved, err := preparePostgresOpen(ctx, dsn, migrationFS, options)
	if err != nil {
		return nil, err
	}
	return openPostgres(ctx, connConfig, migrationFS, resolved)
}

// PostgresPool sizes the connection pool behind a PostgresDB.
type PostgresPool struct {
	// MaxOpenConns bounds the sessions the handle holds at once. It must be
	// at least 1; a transaction holds one session for its whole run.
	MaxOpenConns int
	// MaxIdleConns bounds the idle sessions kept for reuse, from 0 to
	// MaxOpenConns.
	MaxIdleConns int
	// ConnMaxIdleTime closes a session once it has been idle this long, so
	// a burst of concurrent transactions does not pin its sessions on the
	// server afterwards. Zero keeps idle sessions until the handle closes.
	ConnMaxIdleTime time.Duration
}

// DefaultPostgresPool is the pool a handle uses unless WithPostgresPool
// chooses another: up to 16 sessions, all kept for reuse.
func DefaultPostgresPool() PostgresPool {
	return PostgresPool{
		MaxOpenConns: defaultPostgresMaxOpenConns,
		MaxIdleConns: defaultPostgresMaxOpenConns,
	}
}

// PostgresOption configures how a PostgresDB is opened.
type PostgresOption func(*postgresOpenOptions)

type postgresOpenOptions struct {
	pool PostgresPool
}

// WithPostgresPool replaces DefaultPostgresPool. Tests that hold many
// handles at once use it to stay inside the server's max_connections.
func WithPostgresPool(pool PostgresPool) PostgresOption {
	return func(options *postgresOpenOptions) {
		options.pool = pool
	}
}

func resolvePostgresOptions(options []PostgresOption) (postgresOpenOptions, error) {
	resolved := postgresOpenOptions{pool: DefaultPostgresPool()}
	for _, option := range options {
		if option != nil {
			option(&resolved)
		}
	}
	pool := resolved.pool
	if pool.MaxOpenConns < 1 || pool.MaxIdleConns < 0 || pool.MaxIdleConns > pool.MaxOpenConns ||
		pool.ConnMaxIdleTime < 0 {
		return postgresOpenOptions{}, fmt.Errorf(
			"%w: PostgreSQL pool needs MaxOpenConns >= 1, 0 <= MaxIdleConns <= MaxOpenConns, "+
				"and a non-negative ConnMaxIdleTime",
			ErrInvalidArgument,
		)
	}
	return resolved, nil
}

// preparePostgresOpen validates the arguments every open path shares.
func preparePostgresOpen(
	ctx context.Context,
	dsn string,
	migrationFS fs.FS,
	options []PostgresOption,
) (*pgx.ConnConfig, postgresOpenOptions, error) {
	if ctx == nil {
		return nil, postgresOpenOptions{}, fmt.Errorf("%w: nil context", ErrInvalidArgument)
	}
	if migrationFS == nil {
		return nil, postgresOpenOptions{}, fmt.Errorf(
			"%w: PostgreSQL migration filesystem is required",
			ErrInvalidArgument,
		)
	}
	resolved, err := resolvePostgresOptions(options)
	if err != nil {
		return nil, postgresOpenOptions{}, err
	}
	connConfig, err := parsePostgresConfig(dsn)
	if err != nil {
		return nil, postgresOpenOptions{}, err
	}
	return connConfig, resolved, nil
}

// openPostgres opens the shared pool for connConfig, applies migrationFS,
// and returns the handle. Callers have validated their arguments.
func openPostgres(
	ctx context.Context,
	connConfig *pgx.ConnConfig,
	migrationFS fs.FS,
	options postgresOpenOptions,
) (*PostgresDB, error) {
	raw := stdlib.OpenDB(*connConfig.Copy(), stdlib.OptionResetSession(resetPostgresSession))
	raw.SetMaxOpenConns(options.pool.MaxOpenConns)
	raw.SetMaxIdleConns(options.pool.MaxIdleConns)
	raw.SetConnMaxIdleTime(options.pool.ConnMaxIdleTime)
	closeOnError := func(openErr error) (*PostgresDB, error) {
		if closeErr := raw.Close(); closeErr != nil {
			return nil, errors.Join(openErr, fmt.Errorf("close PostgreSQL control plane: %w", closeErr))
		}
		return nil, openErr
	}
	if err := raw.PingContext(ctx); err != nil {
		return closeOnError(fmt.Errorf("connect to PostgreSQL control plane: %w", err))
	}
	if err := ApplyPostgresMigrations(ctx, raw, migrationFS); err != nil {
		return closeOnError(err)
	}
	orm, err := configurePostgresGORM(raw)
	if err != nil {
		return closeOnError(fmt.Errorf("configure GORM PostgreSQL control plane: %w", err))
	}
	return &PostgresDB{
		sql:           raw,
		orm:           orm,
		connConfig:    connConfig,
		writeQueue:    newPostgresWriteQueue(),
		singletons:    make(map[*serverSingleton]struct{}),
		singletonLost: make(chan struct{}),
	}, nil
}

// parsePostgresConfig parses dsn and applies the connection invariants every
// control-plane session relies on. Parse failures never echo the connection
// string, which may carry a password.
func parsePostgresConfig(dsn string) (*pgx.ConnConfig, error) {
	if strings.TrimSpace(dsn) == "" {
		return nil, fmt.Errorf("%w: PostgreSQL connection string is required", ErrInvalidArgument)
	}
	connConfig, err := pgx.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("%w: PostgreSQL connection string is invalid", ErrInvalidArgument)
	}
	if connConfig.RuntimeParams == nil {
		connConfig.RuntimeParams = make(map[string]string)
	}
	// Startup parameters apply before the first statement on every physical
	// connection, including ones the pool opens after a reconnect, so no
	// session can observe a different schema, time zone, or text semantics.
	// They also override ALTER DATABASE and ALTER ROLE defaults, which an
	// operator sharing the database with another application may have set.
	// PostgreSQL folds parameter names, so drop every spelling a caller may
	// have supplied.
	for name := range connConfig.RuntimeParams {
		for _, pinned := range postgresPinnedSessionParameters {
			if strings.EqualFold(name, pinned.name) {
				delete(connConfig.RuntimeParams, name)
			}
		}
	}
	for _, pinned := range postgresPinnedSessionParameters {
		connConfig.RuntimeParams[pinned.name] = pinned.value
	}
	if strings.TrimSpace(connConfig.RuntimeParams["application_name"]) == "" {
		connConfig.RuntimeParams["application_name"] = postgresApplicationName
	}
	return connConfig, nil
}

// resetPostgresSession runs before the pool hands out a reused connection.
// Startup parameters pin search_path and TimeZone, but a plain SET (or
// set_config with is_local false) in a committed transaction, or through the
// pool outside one, outlives the statement and would leak into every later
// borrower of that session. The server reports both settings on every change,
// so a drifted session is discarded without a round trip and the pool dials
// a fresh one with the startup values. A server that does not report a
// setting leaves it unchecked rather than discarding every connection.
func resetPostgresSession(_ context.Context, conn *pgx.Conn) error {
	pgConn := conn.PgConn()
	for name, want := range postgresSessionInvariants {
		if got := pgConn.ParameterStatus(name); got != "" && got != want {
			return driver.ErrBadConn
		}
	}
	return nil
}

// postgresSessionInvariants are the reported settings every pooled session
// must keep, keyed by the server's ParameterStatus names.
var postgresSessionInvariants = map[string]string{
	"search_path": PostgresSchema,
	"TimeZone":    "UTC",
}

func configurePostgresGORM(raw *sql.DB) (*gorm.DB, error) {
	return gorm.Open(gormpostgres.New(gormpostgres.Config{
		Conn: raw,
	}), &gorm.Config{
		DisableAutomaticPing:   true,
		Logger:                 logger.Discard,
		SkipDefaultTransaction: true,
	})
}

// SQLDB exposes the pooled database handle to other internal persistence
// packages. Callers must preserve the connection invariants established by
// OpenPostgres and must not close the returned handle directly. Mutations
// belong in WriteTx so they inherit serializable isolation and retry.
func (db *PostgresDB) SQLDB() *sql.DB {
	if db == nil {
		return nil
	}
	return db.sql
}

// GORMDB exposes the GORM handle configured over the same PostgreSQL
// connection pool as SQLDB. Versioned SQL migrations remain authoritative;
// callers must not use AutoMigrate or close the underlying pool.
func (db *PostgresDB) GORMDB() *gorm.DB {
	if db == nil {
		return nil
	}
	return db.orm
}

// VerifyCurrentMigrations verifies that this database's ledger records
// exactly migrationFS, without applying anything.
func (db *PostgresDB) VerifyCurrentMigrations(
	ctx context.Context,
	migrationFS fs.FS,
) (MigrationIdentity, error) {
	if db == nil {
		return MigrationIdentity{}, fmt.Errorf("%w: PostgreSQL database is required", ErrInvalidArgument)
	}
	return VerifyCurrentPostgresMigrations(ctx, db.sql, migrationFS)
}

// Close releases any server singleton lock acquired through this handle and
// then every pooled connection. It is safe to call more than once.
func (db *PostgresDB) Close() error {
	if db == nil {
		return nil
	}
	db.mu.Lock()
	if db.closed {
		db.mu.Unlock()
		return nil
	}
	db.closed = true
	held := make([]*serverSingleton, 0, len(db.singletons))
	for singleton := range db.singletons {
		held = append(held, singleton)
	}
	db.mu.Unlock()

	var closeErr error
	for _, singleton := range held {
		closeErr = errors.Join(closeErr, singleton.release())
	}
	if db.sql != nil {
		if err := db.sql.Close(); err != nil {
			closeErr = errors.Join(closeErr, fmt.Errorf("close PostgreSQL control plane: %w", err))
		}
	}
	return closeErr
}
