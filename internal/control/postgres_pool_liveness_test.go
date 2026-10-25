package control_test

import (
	"database/sql"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"gorm.io/gorm"

	"github.com/Suhaibinator/open-splunk/internal/control"
)

func TestOpenPostgresWithPoolSizesThePool(t *testing.T) {
	t.Parallel()
	dsn := newSingletonDatabase(t)
	db, err := control.OpenPostgresWithMigrations(t.Context(), dsn, postgresFixtureMigrations(),
		control.WithPostgresPool(control.PostgresPool{MaxOpenConns: 3, MaxIdleConns: 1, ConnMaxIdleTime: time.Minute}))
	if err != nil {
		t.Fatalf("OpenPostgresWithMigrations(pool) = %v", err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Errorf("Close() = %v", err)
		}
	})
	if got := db.SQLDB().Stats().MaxOpenConnections; got != 3 {
		t.Fatalf("MaxOpenConnections = %d, want 3", got)
	}
	var running, peak atomic.Int32
	errs := make(chan error, 8)
	for range 8 {
		go func() {
			errs <- db.ReadTx(t.Context(), func(tx *sql.Tx) error {
				now := running.Add(1)
				defer running.Add(-1)
				for {
					seen := peak.Load()
					if now <= seen || peak.CompareAndSwap(seen, now) {
						break
					}
				}
				_, err := tx.ExecContext(t.Context(), `SELECT pg_sleep(0.05)`)
				return err
			})
		}()
	}
	for range 8 {
		if err := <-errs; err != nil {
			t.Fatalf("pooled read: %v", err)
		}
	}
	if got := peak.Load(); got > 3 {
		t.Fatalf("%d transactions ran at once on a 3-session pool", got)
	}
	if idle := db.SQLDB().Stats().Idle; idle > 1 {
		t.Fatalf("pool keeps %d idle sessions, want at most MaxIdleConns 1", idle)
	}
	if def := control.DefaultPostgresPool(); def.MaxOpenConns != 16 || def.MaxIdleConns != 16 || def.ConnMaxIdleTime != 0 {
		t.Fatalf("DefaultPostgresPool() = %+v", def)
	}
}

func TestOpenPostgresRejectsInvalidPools(t *testing.T) {
	t.Parallel()
	dsn := newSingletonDatabase(t)
	for _, pool := range []control.PostgresPool{
		{MaxOpenConns: 0},
		{MaxOpenConns: 2, MaxIdleConns: 3},
		{MaxOpenConns: 2, MaxIdleConns: -1},
		{MaxOpenConns: 2, ConnMaxIdleTime: -time.Second},
	} {
		db, err := control.OpenPostgresWithMigrations(t.Context(), dsn, postgresFixtureMigrations(),
			control.WithPostgresPool(pool))
		if !errors.Is(err, control.ErrInvalidArgument) {
			if db != nil {
				_ = db.Close()
			}
			t.Fatalf("OpenPostgresWithMigrations(%+v) = %v, want ErrInvalidArgument", pool, err)
		}
		if _, _, err := control.OpenPostgresServerWithMigrations(t.Context(), dsn, postgresFixtureMigrations(),
			control.WithPostgresPool(pool)); !errors.Is(err, control.ErrInvalidArgument) {
			t.Fatalf("OpenPostgresServerWithMigrations(%+v) = %v, want ErrInvalidArgument", pool, err)
		}
	}
}

// A server whose singleton session PostgreSQL ended (a restart, a network
// failure, pg_terminate_backend) learns of it while running: the handle
// signals the loss and refuses writes, because a second server may already
// own the database.
func TestServerSingletonLossIsSignaledAndFencesWrites(t *testing.T) {
	t.Parallel()
	dsn := newSingletonDatabase(t)
	db, release, err := control.OpenPostgresServerWithMigrations(t.Context(), dsn, postgresFixtureMigrations())
	if err != nil {
		t.Fatalf("OpenPostgresServerWithMigrations() = %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	select {
	case <-db.ServerSingletonLost():
		t.Fatal("a held singleton was reported lost")
	default:
	}
	if err := db.ServerSingletonErr(); err != nil {
		t.Fatalf("ServerSingletonErr() of a held singleton = %v", err)
	}

	terminateSingletonHolder(t, dsn)
	select {
	case <-db.ServerSingletonLost():
	case <-time.After(15 * time.Second):
		t.Fatal("the loss of the singleton session was never signaled")
	}
	if err := db.ServerSingletonErr(); !errors.Is(err, control.ErrServerSingletonLost) {
		t.Fatalf("ServerSingletonErr() after the loss = %v", err)
	}

	ran := false
	writes := map[string]error{
		"WriteTx": db.WriteTx(t.Context(), func(*sql.Tx) error { ran = true; return nil }),
		"WriteGORMTx": db.WriteGORMTx(t.Context(), func(*gorm.DB) error {
			ran = true
			return nil
		}),
		"WriteGORMTxOn": db.WriteGORMTxOn(t.Context(), db.GORMDB(), func(*gorm.DB) error {
			ran = true
			return nil
		}),
		"RetryWrite": db.RetryWrite(t.Context(), func() error { ran = true; return nil }),
	}
	for name, err := range writes {
		if !errors.Is(err, control.ErrServerSingletonLost) {
			t.Errorf("%s after the loss = %v, want ErrServerSingletonLost", name, err)
		}
	}
	if ran {
		t.Fatal("a write callback ran after the singleton was lost")
	}
	// Reads stay available so the server can drain and shut down cleanly.
	var value int64
	if err := db.ReadTx(t.Context(), func(tx *sql.Tx) error {
		return tx.QueryRowContext(t.Context(), `SELECT value FROM counters WHERE name = 'shared'`).Scan(&value)
	}); err != nil {
		t.Fatalf("ReadTx after the loss = %v", err)
	}
	if err := release(); err == nil {
		t.Fatal("release() of a lost singleton reported success")
	}
}

// An orderly release is not a loss, and a handle without a singleton is
// never fenced.
func TestServerSingletonReleaseIsNotALoss(t *testing.T) {
	t.Parallel()
	dsn := newSingletonDatabase(t)
	db, release, err := control.OpenPostgresServerWithMigrations(t.Context(), dsn, postgresFixtureMigrations())
	if err != nil {
		t.Fatalf("OpenPostgresServerWithMigrations() = %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if err := release(); err != nil {
		t.Fatalf("release() = %v", err)
	}
	// release waits for the liveness monitor, so no loss can be reported
	// after it returns.
	select {
	case <-db.ServerSingletonLost():
		t.Fatal("an orderly release was reported as a loss")
	default:
	}
	if err := db.WriteTx(t.Context(), func(tx *sql.Tx) error {
		_, err := tx.ExecContext(t.Context(), `UPDATE counters SET value = value + 1 WHERE name = 'shared'`)
		return err
	}); err != nil {
		t.Fatalf("WriteTx after an orderly release = %v", err)
	}
	plain := newPostgresFixtureDB(t)
	if plain.ServerSingletonLost() == nil || plain.ServerSingletonErr() != nil {
		t.Fatal("a handle without a singleton reported a loss state")
	}
	var nilDB *control.PostgresDB
	if nilDB.ServerSingletonLost() != nil || nilDB.ServerSingletonErr() != nil {
		t.Fatal("a nil handle reported a loss state")
	}
}
