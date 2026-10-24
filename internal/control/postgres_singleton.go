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

const (
	serverSingletonReleaseTimeout = 5 * time.Second

	// serverSingletonProbeInterval is how often the dedicated session is
	// checked for still holding the lock. It bounds how long a server keeps
	// writing after PostgreSQL ended its session (a restart, a network
	// failure, pg_terminate_backend), the window in which a second server
	// may already have started.
	serverSingletonProbeInterval = time.Second
	serverSingletonProbeTimeout  = 5 * time.Second
)

// serverSingletonHeldSQL reports whether the probing session itself still
// holds the singleton lock. A two-key advisory lock appears in pg_locks as
// classid (first key), objid (second key) and objsubid 2.
var serverSingletonHeldSQL = fmt.Sprintf(`
	SELECT EXISTS (
		SELECT 1 FROM pg_catalog.pg_locks
		WHERE locktype = 'advisory'
			AND pid = pg_catalog.pg_backend_pid()
			AND database = (
				SELECT oid FROM pg_catalog.pg_database WHERE datname = pg_catalog.current_database()
			)
			AND classid = %d AND objid = %d AND objsubid = 2 AND granted
	)`,
	uint32(postgresAdvisoryLockClass),
	uint32(postgresServerSingletonAdvisoryLockID),
)

// ErrServerAlreadyRunning means another session, normally another server
// process, already holds the control plane's single-active-server lock.
var ErrServerAlreadyRunning = errors.New("control: another open-splunk server holds the control-plane singleton lock")

// ErrServerSingletonLost means the single-active-server lock held through a
// handle was lost while the server ran, normally because PostgreSQL ended its
// dedicated session. Another server may already be running against the
// database, so the handle refuses every later write transaction and the
// server must shut down.
var ErrServerSingletonLost = errors.New("control: the control-plane singleton lock was lost")

type serverSingleton struct {
	owner *PostgresDB
	// mu serializes the liveness monitor and release, because a pgx.Conn
	// must not be used concurrently.
	mu   sync.Mutex
	conn *pgx.Conn
	// stop is closed by release; monitorDone is closed when the monitor
	// started by watch has returned, and is nil when none was started.
	stop        chan struct{}
	monitorDone chan struct{}
	once        sync.Once
	err         error
}

func newServerSingleton(conn *pgx.Conn) *serverSingleton {
	return &serverSingleton{conn: conn, stop: make(chan struct{})}
}

// OpenPostgresServer opens the control plane for the one active server on
// the database named by dsn. It takes the single-active-server lock before
// anything else and only then applies the embedded migrations, so a newer
// binary started beside a running server is refused before it can migrate
// the schema under it. The lock is released by the returned function, by
// PostgresDB.Close, or by PostgreSQL when the process dies; a failed open
// releases it before returning.
//
// While the server runs, the lock's session is probed every
// serverSingletonProbeInterval. Once it is found lost, the handle refuses
// write transactions with ErrServerSingletonLost and closes
// PostgresDB.ServerSingletonLost, which the server treats as fatal.
func OpenPostgresServer(
	ctx context.Context,
	dsn string,
	options ...PostgresOption,
) (*PostgresDB, func() error, error) {
	return OpenPostgresServerWithMigrations(ctx, dsn, migrations.Postgres(), options...)
}

// OpenPostgresServerWithMigrations is OpenPostgresServer with an explicit
// ordered migration set.
func OpenPostgresServerWithMigrations(
	ctx context.Context,
	dsn string,
	migrationFS fs.FS,
	options ...PostgresOption,
) (*PostgresDB, func() error, error) {
	connConfig, resolved, err := preparePostgresOpen(ctx, dsn, migrationFS, options)
	if err != nil {
		return nil, nil, err
	}
	conn, err := lockServerSingletonSession(ctx, connConfig)
	if err != nil {
		return nil, nil, err
	}
	singleton := newServerSingleton(conn)
	db, err := openPostgres(ctx, connConfig, migrationFS, resolved)
	if err != nil {
		return nil, nil, errors.Join(err, singleton.release())
	}
	db.mu.Lock()
	singleton.watch(db)
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
// waiting. Release is idempotent. A lock lost while held is reported as
// OpenPostgresServer describes.
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
	singleton := newServerSingleton(conn)
	db.mu.Lock()
	if db.closed {
		db.mu.Unlock()
		return nil, errors.Join(
			fmt.Errorf("%w: PostgreSQL control plane closed while acquiring singleton", ErrInvalidArgument),
			singleton.release(),
		)
	}
	singleton.watch(db)
	db.singletons[singleton] = struct{}{}
	db.mu.Unlock()
	return singleton.release, nil
}

// ServerSingletonLost returns a channel that is closed once a server
// singleton lock held through db is found lost. A server selects on it and
// shuts down; ServerSingletonErr reports why. It is never closed for a
// handle that holds no singleton, or whose singleton was released.
func (db *PostgresDB) ServerSingletonLost() <-chan struct{} {
	if db == nil {
		return nil
	}
	return db.singletonLost
}

// ServerSingletonErr returns nil until a server singleton lock held through
// db is found lost, and then an error wrapping ErrServerSingletonLost.
func (db *PostgresDB) ServerSingletonErr() error {
	if db == nil {
		return nil
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.singletonLostErr
}

func (db *PostgresDB) markServerSingletonLost(cause error) {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.singletonLostErr != nil {
		return
	}
	db.singletonLostErr = fmt.Errorf("%w: %w", ErrServerSingletonLost, cause)
	close(db.singletonLost)
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

// watch records owner and starts the liveness monitor. The caller holds
// owner.mu and has not yet published the singleton, so release cannot run
// concurrently with it.
func (singleton *serverSingleton) watch(owner *PostgresDB) {
	singleton.owner = owner
	singleton.monitorDone = make(chan struct{})
	go singleton.monitor(serverSingletonProbeInterval)
}

// monitor probes the lock session until release stops it or a probe fails.
// A failed probe means PostgreSQL may already have granted the lock to
// another session, so the owner is fenced at once.
func (singleton *serverSingleton) monitor(interval time.Duration) {
	defer close(singleton.monitorDone)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-singleton.stop:
			return
		case <-ticker.C:
		}
		err := singleton.probe()
		if err == nil {
			continue
		}
		select {
		case <-singleton.stop:
			// Released while probing: an orderly shutdown, not a loss.
			return
		default:
		}
		singleton.owner.markServerSingletonLost(err)
		return
	}
}

// probe reports an error unless the dedicated session is alive and still
// holds the lock. A probe that cannot complete in time counts as lost: pgx
// closes a connection whose query it abandons, which frees the lock.
func (singleton *serverSingleton) probe() error {
	singleton.mu.Lock()
	defer singleton.mu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), serverSingletonProbeTimeout)
	defer cancel()
	var held bool
	if err := singleton.conn.QueryRow(ctx, serverSingletonHeldSQL).Scan(&held); err != nil {
		return fmt.Errorf("probe PostgreSQL server singleton session: %w", err)
	}
	if !held {
		return errors.New("PostgreSQL server singleton session no longer holds the lock")
	}
	return nil
}

// release unlocks explicitly before closing so the lock is free as soon as
// release returns, rather than when the server notices the disconnect.
func (singleton *serverSingleton) release() error {
	singleton.once.Do(func() {
		close(singleton.stop)
		if singleton.monitorDone != nil {
			<-singleton.monitorDone
		}
		singleton.mu.Lock()
		defer singleton.mu.Unlock()
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
