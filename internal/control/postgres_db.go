package control

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io/fs"
	"strings"
	"sync"

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

// PostgresDB is the PostgreSQL-backed control-plane database. SQLDB and
// GORMDB share one connection pool; WriteTx and ReadTx are the supported
// transaction entry points.
type PostgresDB struct {
	sql        *sql.DB
	orm        *gorm.DB
	connConfig *pgx.ConnConfig

	// writeQueue serializes write transactions while any is being retried.
	writeQueue *postgresWriteQueue

	mu         sync.Mutex
	closed     bool
	singletons map[*serverSingleton]struct{}
}

// OpenPostgres connects to the PostgreSQL control plane named by dsn, pins
// every pooled connection to the open_splunk schema, and applies the embedded
// PostgreSQL migrations before returning.
func OpenPostgres(ctx context.Context, dsn string) (*PostgresDB, error) {
	return OpenPostgresWithMigrations(ctx, dsn, migrations.Postgres())
}

// OpenPostgresWithMigrations is OpenPostgres with an explicit ordered
// migration set. Production uses the embedded set; tests use fixtures.
func OpenPostgresWithMigrations(
	ctx context.Context,
	dsn string,
	migrationFS fs.FS,
) (*PostgresDB, error) {
	if ctx == nil {
		return nil, fmt.Errorf("%w: nil context", ErrInvalidArgument)
	}
	if migrationFS == nil {
		return nil, fmt.Errorf("%w: PostgreSQL migration filesystem is required", ErrInvalidArgument)
	}
	connConfig, err := parsePostgresConfig(dsn)
	if err != nil {
		return nil, err
	}

	raw := stdlib.OpenDB(*connConfig.Copy())
	raw.SetMaxOpenConns(defaultPostgresMaxOpenConns)
	raw.SetMaxIdleConns(defaultPostgresMaxOpenConns)
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
		sql:        raw,
		orm:        orm,
		connConfig: connConfig,
		writeQueue: newPostgresWriteQueue(),
		singletons: make(map[*serverSingleton]struct{}),
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
	// session can observe a different schema or time zone. PostgreSQL folds
	// parameter names, so drop every spelling a caller may have supplied.
	for name := range connConfig.RuntimeParams {
		if strings.EqualFold(name, "search_path") || strings.EqualFold(name, "timezone") {
			delete(connConfig.RuntimeParams, name)
		}
	}
	connConfig.RuntimeParams["search_path"] = PostgresSchema
	connConfig.RuntimeParams["timezone"] = "UTC"
	if strings.TrimSpace(connConfig.RuntimeParams["application_name"]) == "" {
		connConfig.RuntimeParams["application_name"] = postgresApplicationName
	}
	return connConfig, nil
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
