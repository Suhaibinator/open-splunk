package control

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
)

// postgresServerSingletonAdvisoryLockKey identifies the single active server
// on one control-plane database. It shares the "os" prefix with the
// migration lock but is a distinct key, so a running server never blocks a
// migration transaction and vice versa.
const postgresServerSingletonAdvisoryLockKey = int64(0x6f73_0000_0000_0002)

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

// AcquireServerSingleton takes the session-level advisory lock that fences
// the complete server lifetime on this control-plane database. The lock is
// held on a dedicated connection outside the shared pool, so pool churn can
// never release it; it is released by the returned function, by
// PostgresDB.Close, or by PostgreSQL when the process or its connection
// dies. A held lock fails fast with ErrServerAlreadyRunning rather than
// waiting. Release is idempotent.
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

	conn, err := pgx.ConnectConfig(ctx, db.connConfig.Copy())
	if err != nil {
		return nil, fmt.Errorf("connect PostgreSQL server singleton session: %w", err)
	}
	var acquired bool
	if err := conn.QueryRow(
		ctx,
		`SELECT pg_try_advisory_lock($1)`,
		postgresServerSingletonAdvisoryLockKey,
	).Scan(&acquired); err != nil {
		return nil, errors.Join(
			fmt.Errorf("acquire PostgreSQL server singleton lock: %w", err),
			closeSingletonConn(conn),
		)
	}
	if !acquired {
		return nil, errors.Join(ErrServerAlreadyRunning, closeSingletonConn(conn))
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

// release unlocks explicitly before closing so the lock is free as soon as
// release returns, rather than when the server notices the disconnect.
func (singleton *serverSingleton) release() error {
	singleton.once.Do(func() {
		ctx, cancel := context.WithTimeout(context.Background(), serverSingletonReleaseTimeout)
		defer cancel()
		var released bool
		unlockErr := singleton.conn.QueryRow(
			ctx,
			`SELECT pg_advisory_unlock($1)`,
			postgresServerSingletonAdvisoryLockKey,
		).Scan(&released)
		if unlockErr != nil {
			unlockErr = fmt.Errorf("release PostgreSQL server singleton lock: %w", unlockErr)
		} else if !released {
			unlockErr = errors.New("release PostgreSQL server singleton lock: session no longer held it")
		}
		singleton.err = errors.Join(unlockErr, closeSingletonConn(singleton.conn))
		singleton.owner.mu.Lock()
		delete(singleton.owner.singletons, singleton)
		singleton.owner.mu.Unlock()
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
