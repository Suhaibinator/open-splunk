package control

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io/fs"
	"strings"
	"sync"
	"sync/atomic"
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
	// asked whether it still holds the lock. Writes do not wait for it:
	// WriteTx checks the lock itself before every commit. It bounds how long
	// an idle server takes to notice that PostgreSQL ended its session (a
	// restart, a network failure, pg_terminate_backend).
	serverSingletonProbeInterval = time.Second
	serverSingletonProbeTimeout  = 5 * time.Second
)

// serverSingletonHeldQuery reports whether the backend with pid $3 holds the
// session-level two-key advisory lock ($1, $2) on the current database.
// PostgreSQL files a two-key advisory lock as classid (first key), objid
// (second key) and objsubid 2. pg_locks reads the lock manager directly, so the answer is
// current even inside a SERIALIZABLE transaction's older snapshot, and it
// takes no predicate locks.
const serverSingletonHeldQuery = `
SELECT EXISTS (
	SELECT 1
	FROM pg_catalog.pg_locks
	WHERE locktype = 'advisory'
		AND database = (SELECT oid FROM pg_catalog.pg_database WHERE datname = pg_catalog.current_database())
		AND classid = $1::integer::oid
		AND objid = $2::integer::oid
		AND objsubid = 2
		AND pid = $3::integer
		AND mode = 'ExclusiveLock'
		AND granted
)`

// ErrServerAlreadyRunning means another session, normally another server
// process, already holds the control plane's single-active-server lock.
var ErrServerAlreadyRunning = errors.New("control: another open-splunk server holds the control-plane singleton lock")

// ErrServerSingletonLost means the session holding this server's singleton
// lock ended (backend terminated, PostgreSQL restarted or failed over, the
// connection reset) or no longer holds the lock, so another server may now
// hold it and migrate the schema. It is sticky: once a handle has lost its
// singleton every later WriteTx and WriteGORMTx fails with it, and
// PostgresDB.ServerSingletonLost is closed. The server must stop; a restart
// takes the lock again or is refused with ErrServerAlreadyRunning.
var ErrServerSingletonLost = errors.New("control: this server lost the control-plane singleton lock")

type serverSingleton struct {
	owner *PostgresDB
	// mu serializes probe and release on conn, because a pgx.Conn must not
	// be used concurrently.
	mu   sync.Mutex
	conn *pgx.Conn
	// pid is the dedicated session's backend, the one pg_locks must show
	// holding the lock.
	pid uint32

	// stop ends the monitor; monitorDone closes once it has returned, after
	// which only release touches conn. monitoring is set before the
	// singleton is published to its owner and never changes afterwards.
	stop        chan struct{}
	monitorDone chan struct{}
	monitoring  bool
	// releasing is set once release starts, so a WriteTx that finds the lock
	// gone because of a deliberate release does not fence the handle.
	releasing atomic.Bool

	once sync.Once
	err  error
}

func newServerSingleton(conn *pgx.Conn) *serverSingleton {
	return &serverSingleton{
		conn:        conn,
		pid:         conn.PgConn().PID(),
		stop:        make(chan struct{}),
		monitorDone: make(chan struct{}),
	}
}

// OpenPostgresServer opens the control plane for the one active server on
// the database named by dsn. It takes the single-active-server lock before
// anything else and only then applies the embedded migrations, so a newer
// binary started beside a running server is refused before it can migrate
// the schema under it. The lock is released by the returned function, by
// PostgresDB.Close, or by PostgreSQL when the process dies; a failed open
// releases it before returning. The handle is fenced by the lock as
// AcquireServerSingleton describes, and the server shuts down when
// PostgresDB.ServerSingletonLost fires.
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
	if err := db.attachServerSingleton(singleton); err != nil {
		return nil, nil, errors.Join(err, singleton.release(), db.Close())
	}
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
// While the lock is held, db is fenced by it: every WriteTx and WriteGORMTx
// confirms, inside its transaction and just before COMMIT, that the
// dedicated session still holds the lock, and a background probe watches the
// session between writes. Either one finding the lock lost closes
// db.ServerSingletonLost and fails every later write with
// ErrServerSingletonLost. Statements run directly on SQLDB or GORMDB are not
// fenced, which is one more reason mutations belong in WriteTx. A
// deliberate release ends the fence without tripping it, so a server stops
// writing before it releases.
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
	// Refuse a fenced handle before contending for the lock, which another
	// server most likely holds by now.
	if err := db.serverSingletonLostErr(); err != nil {
		return nil, err
	}

	conn, err := lockServerSingletonSession(ctx, db.connConfig)
	if err != nil {
		return nil, err
	}
	singleton := newServerSingleton(conn)
	if err := db.attachServerSingleton(singleton); err != nil {
		return nil, errors.Join(err, singleton.release())
	}
	return singleton.release, nil
}

// attachServerSingleton makes singleton fence db's writes and starts its
// monitor. A handle that already lost a singleton stays fenced: it cannot
// start writing again under a new lock, because writes it issued while the
// lock was lost may already have interleaved with another server's.
func (db *PostgresDB) attachServerSingleton(singleton *serverSingleton) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return fmt.Errorf("%w: PostgreSQL control plane closed while acquiring singleton", ErrInvalidArgument)
	}
	if db.singletonLostErr != nil {
		return db.singletonLostErr
	}
	singleton.owner = db
	singleton.monitoring = true
	go singleton.monitor(serverSingletonProbeInterval)
	db.singletons[singleton] = struct{}{}
	db.writeFence.Store(singleton)
	return nil
}

// detachServerSingleton stops singleton fencing db. After a lost singleton
// db stays fenced through singletonLostErr; see markServerSingletonLost.
func (db *PostgresDB) detachServerSingleton(singleton *serverSingleton) {
	db.mu.Lock()
	defer db.mu.Unlock()
	delete(db.singletons, singleton)
	db.writeFence.CompareAndSwap(singleton, nil)
}

// markServerSingletonLost fences db for good: ServerSingletonLost closes and
// every later write fails with ErrServerSingletonLost wrapping cause. Only
// the first cause is kept.
func (db *PostgresDB) markServerSingletonLost(cause error) {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.singletonLostErr != nil {
		return
	}
	if errors.Is(cause, ErrServerSingletonLost) {
		db.singletonLostErr = cause
	} else {
		db.singletonLostErr = fmt.Errorf("%w: %w", ErrServerSingletonLost, cause)
	}
	close(db.singletonLost)
}

// serverSingletonLostErr returns the sticky loss, or nil while db has not
// lost a singleton. The common case is one non-blocking channel receive.
func (db *PostgresDB) serverSingletonLostErr() error {
	select {
	case <-db.singletonLost:
	default:
		return nil
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.singletonLostErr
}

// ServerSingletonLost returns a channel that is closed when this handle
// loses the server singleton lock it acquired through OpenPostgresServer or
// AcquireServerSingleton. The server selects on it and shuts down: from
// then on WriteTx and WriteGORMTx fail with ErrServerSingletonLost, and
// another server may be running and migrating the schema. A deliberate
// release does not close it. A nil handle returns nil, which never fires.
func (db *PostgresDB) ServerSingletonLost() <-chan struct{} {
	if db == nil {
		return nil
	}
	return db.singletonLost
}

// ServerSingletonErr returns nil until this handle loses a server singleton
// lock, and from then on the sticky error wrapping ErrServerSingletonLost
// that every later write fails with. A nil handle returns nil.
func (db *PostgresDB) ServerSingletonErr() error {
	if db == nil {
		return nil
	}
	return db.serverSingletonLostErr()
}

// ConfirmServerSingleton is the check WriteTx runs just before COMMIT, for
// callers that commit their own transaction under RetryWrite. Run on tx as
// its last statement, it fails with ErrServerSingletonLost, and fences the
// handle, when this handle's singleton session no longer holds the lock; the
// caller must then roll tx back. It returns nil for a handle that holds no
// singleton.
func (db *PostgresDB) ConfirmServerSingleton(ctx context.Context, tx *sql.Tx) error {
	if ctx == nil || db == nil || tx == nil {
		return fmt.Errorf("%w: context, database, and transaction are required", ErrInvalidArgument)
	}
	return db.checkServerSingletonBeforeCommit(ctx, tx)
}

// checkServerSingletonBeforeCommit runs inside every write transaction just
// before COMMIT. It confirms that this handle's singleton session still holds
// the lock, so a server whose session died cannot commit once another server
// may have taken over. A takeover between this check and the COMMIT is
// confined to that one transaction: the new holder must first wait for
// PostgreSQL to free the lock, and its migrations then wait on this
// transaction's table locks.
func (db *PostgresDB) checkServerSingletonBeforeCommit(ctx context.Context, tx *sql.Tx) error {
	if err := db.serverSingletonLostErr(); err != nil {
		return err
	}
	singleton := db.writeFence.Load()
	if singleton == nil {
		return nil
	}
	var held bool
	if err := tx.QueryRowContext(
		ctx,
		serverSingletonHeldQuery,
		postgresAdvisoryLockClass,
		postgresServerSingletonAdvisoryLockID,
		int64(singleton.pid),
	).Scan(&held); err != nil {
		return fmt.Errorf("check PostgreSQL server singleton lock: %w", err)
	}
	if held {
		return nil
	}
	if singleton.releasing.Load() {
		// The caller released the lock while this transaction ran. That is
		// a shutdown, not a loss, so the handle is not fenced, but this
		// transaction no longer runs under the singleton and must not commit.
		return fmt.Errorf("%w: released while the transaction ran", ErrServerSingletonLost)
	}
	db.markServerSingletonLost(fmt.Errorf("singleton session (backend %d) no longer holds the lock", singleton.pid))
	return db.serverSingletonLostErr()
}

// monitor probes the dedicated session every interval until release stops
// it or the lock is found lost, so a server learns of the loss between
// writes and can shut down instead of waiting for its next WriteTx.
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
		if err := singleton.probe(); err != nil {
			singleton.owner.markServerSingletonLost(err)
			return
		}
	}
}

// probe reports the lock lost when the session has ended or answers that it
// no longer holds the lock. A failed query on a session that is still open,
// such as one canceled with pg_cancel_backend, proves nothing: the session
// and its lock live on, and the next probe asks again.
func (singleton *serverSingleton) probe() error {
	singleton.mu.Lock()
	defer singleton.mu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), serverSingletonProbeTimeout)
	defer cancel()
	var held bool
	err := singleton.conn.QueryRow(
		ctx,
		serverSingletonHeldQuery,
		postgresAdvisoryLockClass,
		postgresServerSingletonAdvisoryLockID,
		int64(singleton.pid),
	).Scan(&held)
	switch {
	case err == nil && held:
		return nil
	case err == nil:
		return fmt.Errorf("singleton session (backend %d) no longer holds the lock", singleton.pid)
	case singleton.conn.IsClosed():
		return fmt.Errorf("singleton session (backend %d) ended: %w", singleton.pid, err)
	default:
		return nil
	}
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
// release returns, rather than when the server notices the disconnect. It
// stops the monitor first, so the two never share the session. A release
// that finds the lock already gone reports ErrServerSingletonLost and leaves
// the owner fenced.
func (singleton *serverSingleton) release() error {
	singleton.once.Do(func() {
		singleton.releasing.Store(true)
		close(singleton.stop)
		if singleton.monitoring {
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
			unlockErr = fmt.Errorf("%w: release: %w", ErrServerSingletonLost, unlockErr)
		} else if !released {
			unlockErr = fmt.Errorf("%w: release: session no longer held it", ErrServerSingletonLost)
		}
		singleton.err = errors.Join(unlockErr, closeSingletonConn(singleton.conn))
		if singleton.owner != nil {
			if unlockErr != nil {
				singleton.owner.markServerSingletonLost(unlockErr)
			}
			singleton.owner.detachServerSingleton(singleton)
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
