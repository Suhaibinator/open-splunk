package control

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/Suhaibinator/open-splunk/migrations"
)

// postgresServerSingletonAdvisoryLockID identifies the single active server
// on one control-plane database, as the second key under
// postgresAdvisoryLockClass. It is distinct from the migration lock's, so a
// running server never blocks a migration transaction and vice versa.
const postgresServerSingletonAdvisoryLockID = int32(2)

const serverSingletonReleaseTimeout = 5 * time.Second

// ErrServerAlreadyRunning means another session, normally another server
// process, already holds the control plane's single-active-server lock.
var ErrServerAlreadyRunning = errors.New("control: another open-splunk server holds the control-plane singleton lock")

type serverSingleton struct {
	owner *PostgresDB
	conn  *pgx.Conn
	once  sync.Once
	err   error
}

// OpenPostgresServer opens the control plane for the one active server on
// the database named by dsn. It takes the single-active-server lock before
// anything else and only then applies the embedded migrations, so a newer
// binary started beside a running server is refused before it can migrate
// the schema under it. The lock is released by the returned function, by
// PostgresDB.Close, or by PostgreSQL when the process dies; a failed open
// releases it before returning.
func OpenPostgresServer(ctx context.Context, dsn string) (*PostgresDB, func() error, error) {
	return OpenPostgresServerWithMigrations(ctx, dsn, migrations.Postgres())
}

// OpenPostgresServerWithMigrations is OpenPostgresServer with an explicit
// ordered migration set.
func OpenPostgresServerWithMigrations(
	ctx context.Context,
	dsn string,
	migrationFS fs.FS,
) (*PostgresDB, func() error, error) {
	if ctx == nil {
		return nil, nil, fmt.Errorf("%w: nil context", ErrInvalidArgument)
	}
	if migrationFS == nil {
		return nil, nil, fmt.Errorf("%w: PostgreSQL migration filesystem is required", ErrInvalidArgument)
	}
	connConfig, err := parsePostgresConfig(dsn)
	if err != nil {
		return nil, nil, err
	}
	conn, err := lockServerSingletonSession(ctx, connConfig)
	if err != nil {
		return nil, nil, err
	}
	singleton := &serverSingleton{conn: conn}
	db, err := openPostgres(ctx, connConfig, migrationFS)
	if err != nil {
		return nil, nil, errors.Join(err, singleton.release())
	}
	singleton.owner = db
	db.mu.Lock()
	db.singletons[singleton] = struct{}{}
	db.mu.Unlock()
	return db, singleton.release, nil
}

// AcquireServerSingleton takes the session-level advisory lock that fences
// the complete server lifetime on this control-plane database. The lock is
// held on a dedicated connection outside the shared pool, so pool churn can
// never release it; it is released by the returned function, by
// PostgresDB.Close, or by PostgreSQL when the process or its connection
// dies. A held lock fails fast with ErrServerAlreadyRunning rather than
// waiting. Release is idempotent.
//
// db is already migrated, so a server must not open with OpenPostgres and
// then call this: use OpenPostgresServer, which locks before migrating.
func AcquireServerSingleton(ctx context.Context, db *PostgresDB) (func() error, error) {
	if ctx == nil || db == nil || db.connConfig == nil {
		return nil, fmt.Errorf("%w: singleton context and PostgreSQL database are required", ErrInvalidArgument)
	}
	db.mu.Lock()
	closed := db.closed
	db.mu.Unlock()
	if closed {
		return nil, fmt.Errorf("%w: PostgreSQL control plane is closed", ErrInvalidArgument)
	}

	conn, err := lockServerSingletonSession(ctx, db.connConfig)
	if err != nil {
		return nil, err
	}
	singleton := &serverSingleton{owner: db, conn: conn}
	db.mu.Lock()
	if db.closed {
		db.mu.Unlock()
		return nil, errors.Join(
			fmt.Errorf("%w: PostgreSQL control plane closed while acquiring singleton", ErrInvalidArgument),
			singleton.release(),
		)
	}
	db.singletons[singleton] = struct{}{}
	db.mu.Unlock()
	return singleton.release, nil
}

// lockServerSingletonSession opens the dedicated singleton session and takes
// the lock on it, or fails fast with ErrServerAlreadyRunning.
func lockServerSingletonSession(ctx context.Context, connConfig *pgx.ConnConfig) (*pgx.Conn, error) {
	config := connConfig.Copy()
	// The session is idle for the server's whole life. A database, role or
	// connection-string idle_session_timeout would end it and silently free
	// the lock while the server keeps running, so the session opts out;
	// startup parameters take precedence over database and role defaults.
	for name := range config.RuntimeParams {
		if strings.EqualFold(name, "idle_session_timeout") {
			delete(config.RuntimeParams, name)
		}
	}
	config.RuntimeParams["idle_session_timeout"] = "0"
	conn, err := pgx.ConnectConfig(ctx, config)
	if err != nil {
		return nil, fmt.Errorf("connect PostgreSQL server singleton session: %w", err)
	}
	var acquired bool
	if err := conn.QueryRow(
		ctx,
		`SELECT pg_catalog.pg_try_advisory_lock($1, $2)`,
		postgresAdvisoryLockClass,
		postgresServerSingletonAdvisoryLockID,
	).Scan(&acquired); err != nil {
		return nil, errors.Join(
			fmt.Errorf("acquire PostgreSQL server singleton lock: %w", err),
			closeSingletonConn(conn),
		)
	}
	if !acquired {
		return nil, errors.Join(ErrServerAlreadyRunning, closeSingletonConn(conn))
	}
	return conn, nil
}

// release unlocks explicitly before closing so the lock is free as soon as
// release returns, rather than when the server notices the disconnect.
func (singleton *serverSingleton) release() error {
	singleton.once.Do(func() {
		ctx, cancel := context.WithTimeout(context.Background(), serverSingletonReleaseTimeout)
		defer cancel()
		var released bool
		unlockErr := singleton.conn.QueryRow(
			ctx,
			`SELECT pg_catalog.pg_advisory_unlock($1, $2)`,
			postgresAdvisoryLockClass,
			postgresServerSingletonAdvisoryLockID,
		).Scan(&released)
		if unlockErr != nil {
			unlockErr = fmt.Errorf("release PostgreSQL server singleton lock: %w", unlockErr)
		} else if !released {
			unlockErr = errors.New("release PostgreSQL server singleton lock: session no longer held it")
		}
		singleton.err = errors.Join(unlockErr, closeSingletonConn(singleton.conn))
		if singleton.owner != nil {
			singleton.owner.mu.Lock()
			delete(singleton.owner.singletons, singleton)
			singleton.owner.mu.Unlock()
		}
	})
	return singleton.err
}

func closeSingletonConn(conn *pgx.Conn) error {
	ctx, cancel := context.WithTimeout(context.Background(), serverSingletonReleaseTimeout)
	defer cancel()
	if err := conn.Close(ctx); err != nil {
		return fmt.Errorf("close PostgreSQL server singleton session: %w", err)
	}
	return nil
}
