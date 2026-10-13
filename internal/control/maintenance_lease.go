package control

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	pgx "github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
)

// MaintenanceLease excludes the production server and other recovery commands
// for the complete PostgreSQL/MinIO/ClickHouse recovery generation. Acquiring it
// never migrates, creates domain rows or resets the target database.
type MaintenanceLease struct {
	source string
	pool   *sql.DB
	conn   *sql.Conn
	key    int64
	cancel context.CancelFunc
	lost   chan struct{}
	wait   sync.WaitGroup
	once   sync.Once
	err    error
	closed atomic.Bool
}

func AcquireMaintenanceLease(ctx context.Context, dsn string) (*MaintenanceLease, error) {
	if ctx == nil || strings.TrimSpace(dsn) == "" || (!strings.Contains(dsn, "=") && !strings.HasPrefix(dsn, "postgres://") && !strings.HasPrefix(dsn, "postgresql://")) {
		return nil, ErrInvalidArgument
	}
	cfg, err := pgx.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("%w: invalid PostgreSQL maintenance connection", ErrInvalidArgument)
	}
	pool := stdlib.OpenDB(*cfg)
	pool.SetMaxOpenConns(1)
	pool.SetMaxIdleConns(1)
	lease := &MaintenanceLease{source: dsn, pool: pool, lost: make(chan struct{})}
	fail := func(cause error) (*MaintenanceLease, error) { return nil, errors.Join(cause, lease.Close()) }
	lease.conn, err = pool.Conn(ctx)
	if err != nil {
		return fail(err)
	}
	if err := lease.conn.QueryRowContext(ctx, "SELECT hashtextextended('open-splunk:server:' || current_database() || ':' || current_schema(),0)").Scan(&lease.key); err != nil {
		return fail(err)
	}
	var owned bool
	if err := lease.conn.QueryRowContext(ctx, "SELECT pg_try_advisory_lock($1)", lease.key).Scan(&owned); err != nil {
		return fail(err)
	}
	if !owned {
		return fail(ErrServerAlreadyRunning)
	}
	lease.startOwnershipWatcher()
	return lease, nil
}

func (lease *MaintenanceLease) startOwnershipWatcher() {
	watch, cancel := context.WithCancel(context.Background())
	lease.cancel = cancel
	lease.wait.Go(func() {
		tick := time.NewTicker(time.Second)
		defer tick.Stop()
		for {
			select {
			case <-watch.Done():
				return
			case <-tick.C:
				// Stopping the watcher must not interrupt an in-flight ping:
				// pgx discards the reserved connection on any ping error,
				// preventing Close from releasing its advisory lock normally.
				// Close joins this independently bounded check before unlocking.
				check, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				err := lease.conn.PingContext(check)
				cancel()
				if err != nil {
					close(lease.lost)
					return
				}
			}
		}
	})
}

// Validate proves that this lease still protects exactly the requested connection.
// Connection strings are compared privately and never included in errors.
func (lease *MaintenanceLease) Validate(dsn string) error {
	if lease == nil || lease.conn == nil || lease.source != dsn || lease.closed.Load() {
		return errors.New("PostgreSQL operation requires a matching held maintenance lease")
	}
	select {
	case <-lease.lost:
		return errors.New("PostgreSQL maintenance lease was lost")
	default:
		return nil
	}
}
func (lease *MaintenanceLease) OwnershipLost() <-chan struct{} { return lease.lost }
func (lease *MaintenanceLease) Close() error {
	if lease == nil {
		return nil
	}
	lease.once.Do(func() {
		lease.closed.Store(true)
		if lease.cancel != nil {
			lease.cancel()
		}
		lease.wait.Wait()
		if lease.conn != nil {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			var unlocked bool
			err := lease.conn.QueryRowContext(ctx, "SELECT pg_advisory_unlock($1)", lease.key).Scan(&unlocked)
			cancel()
			lease.err = errors.Join(lease.err, err, lease.conn.Close())
		}
		if lease.pool != nil {
			lease.err = errors.Join(lease.err, lease.pool.Close())
		}
	})
	return lease.err
}
