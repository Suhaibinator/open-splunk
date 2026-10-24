package control_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/control"
)

func openSecondPostgresHandle(t *testing.T, dsn string) *control.PostgresDB {
	t.Helper()
	db, err := control.OpenPostgresWithMigrations(t.Context(), dsn, postgresFixtureMigrations())
	if err != nil {
		t.Fatalf("open second control-plane handle: %v", err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Errorf("close second control-plane handle: %v", err)
		}
	})
	return db
}

func newSingletonDatabase(t *testing.T) string {
	t.Helper()
	dsn := newEmptyPostgresDatabase(t)
	first, err := control.OpenPostgresWithMigrations(t.Context(), dsn, postgresFixtureMigrations())
	if err != nil {
		t.Fatalf("migrate singleton database: %v", err)
	}
	if err := first.Close(); err != nil {
		t.Fatalf("close migration handle: %v", err)
	}
	return dsn
}

func requireAlreadyRunning(t *testing.T, db *control.PostgresDB) {
	t.Helper()
	started := time.Now()
	release, err := control.AcquireServerSingleton(t.Context(), db)
	if !errors.Is(err, control.ErrServerAlreadyRunning) {
		if release != nil {
			_ = release()
		}
		t.Fatalf("AcquireServerSingleton() = %v, want ErrServerAlreadyRunning", err)
	}
	if release != nil {
		t.Fatal("refused AcquireServerSingleton() returned a release function")
	}
	if elapsed := time.Since(started); elapsed > 3*time.Second {
		t.Fatalf("refusal took %s; a held lock must fail fast", elapsed)
	}
}

func TestAcquireServerSingletonIsExclusiveUntilReleased(t *testing.T) {
	t.Parallel()
	dsn := newSingletonDatabase(t)
	first := openSecondPostgresHandle(t, dsn)
	second := openSecondPostgresHandle(t, dsn)

	release, err := control.AcquireServerSingleton(t.Context(), first)
	if err != nil {
		t.Fatalf("first AcquireServerSingleton() = %v", err)
	}
	requireAlreadyRunning(t, second)
	// A second acquisition through the same handle opens another session and
	// is refused too: one process must not run two servers.
	requireAlreadyRunning(t, first)

	// The lock lives on its own session, so saturating and recycling the
	// shared pool cannot release it.
	if err := first.SQLDB().PingContext(t.Context()); err != nil {
		t.Fatalf("ping pool: %v", err)
	}
	first.SQLDB().SetMaxIdleConns(0)
	requireAlreadyRunning(t, second)

	if err := release(); err != nil {
		t.Fatalf("release() = %v", err)
	}
	if err := release(); err != nil {
		t.Fatalf("second release() = %v", err)
	}
	secondRelease, err := control.AcquireServerSingleton(t.Context(), second)
	if err != nil {
		t.Fatalf("AcquireServerSingleton() after release = %v", err)
	}
	requireAlreadyRunning(t, first)
	if err := secondRelease(); err != nil {
		t.Fatalf("second holder release() = %v", err)
	}
}

func TestAcquireServerSingletonIsReleasedWhenTheHandleCloses(t *testing.T) {
	t.Parallel()
	dsn := newSingletonDatabase(t)
	first, err := control.OpenPostgresWithMigrations(t.Context(), dsn, postgresFixtureMigrations())
	if err != nil {
		t.Fatalf("open first handle: %v", err)
	}
	second := openSecondPostgresHandle(t, dsn)

	release, err := control.AcquireServerSingleton(t.Context(), first)
	if err != nil {
		t.Fatalf("AcquireServerSingleton() = %v", err)
	}
	requireAlreadyRunning(t, second)
	if err := first.Close(); err != nil {
		t.Fatalf("Close() = %v", err)
	}
	if err := release(); err != nil {
		t.Fatalf("release() after Close() = %v", err)
	}
	if _, err := control.AcquireServerSingleton(t.Context(), first); !errors.Is(err, control.ErrInvalidArgument) {
		t.Fatalf("AcquireServerSingleton() on closed handle = %v", err)
	}
	secondRelease, err := control.AcquireServerSingleton(t.Context(), second)
	if err != nil {
		t.Fatalf("AcquireServerSingleton() after holder closed = %v", err)
	}
	if err := secondRelease(); err != nil {
		t.Fatalf("release() = %v", err)
	}
}

func TestAcquireServerSingletonIsReleasedWhenTheHolderSessionDies(t *testing.T) {
	t.Parallel()
	dsn := newSingletonDatabase(t)
	first := openSecondPostgresHandle(t, dsn)
	second := openSecondPostgresHandle(t, dsn)

	release, err := control.AcquireServerSingleton(t.Context(), first)
	if err != nil {
		t.Fatalf("AcquireServerSingleton() = %v", err)
	}
	requireAlreadyRunning(t, second)

	// Model a crashed server process: PostgreSQL ends the holder's session
	// and with it every session-level advisory lock it held.
	var terminated int
	if err := second.SQLDB().QueryRowContext(t.Context(), `
		SELECT count(*) FILTER (WHERE pg_terminate_backend(pid))
		FROM pg_locks
		WHERE locktype = 'advisory'
			AND database = (SELECT oid FROM pg_database WHERE datname = current_database())
			AND classid = x'6f730000'::int::oid
			AND objid = 2
			AND objsubid = 1
			AND granted`).Scan(&terminated); err != nil {
		t.Fatalf("terminate holder session: %v", err)
	}
	if terminated != 1 {
		t.Fatalf("terminated %d holder sessions, want 1", terminated)
	}

	deadline, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	for {
		secondRelease, err := control.AcquireServerSingleton(deadline, second)
		if err == nil {
			if err := secondRelease(); err != nil {
				t.Fatalf("release() = %v", err)
			}
			break
		}
		if !errors.Is(err, control.ErrServerAlreadyRunning) || deadline.Err() != nil {
			t.Fatalf("AcquireServerSingleton() after holder died = %v", err)
		}
		time.Sleep(20 * time.Millisecond)
	}
	// Releasing a lock its session already lost reports the loss instead of
	// pretending the fence was held until release.
	if err := release(); err == nil {
		t.Fatal("release() of a terminated session reported success")
	}
}

func TestAcquireServerSingletonIsScopedToOneDatabase(t *testing.T) {
	t.Parallel()
	first := openSecondPostgresHandle(t, newSingletonDatabase(t))
	other := openSecondPostgresHandle(t, newSingletonDatabase(t))

	release, err := control.AcquireServerSingleton(t.Context(), first)
	if err != nil {
		t.Fatalf("AcquireServerSingleton() = %v", err)
	}
	otherRelease, err := control.AcquireServerSingleton(t.Context(), other)
	if err != nil {
		t.Fatalf("AcquireServerSingleton() on another database = %v", err)
	}
	if err := errors.Join(release(), otherRelease()); err != nil {
		t.Fatalf("release() = %v", err)
	}
}

func TestAcquireServerSingletonRejectsMissingArguments(t *testing.T) {
	t.Parallel()
	var nilContext context.Context
	if _, err := control.AcquireServerSingleton(t.Context(), nil); !errors.Is(err, control.ErrInvalidArgument) {
		t.Fatalf("nil database = %v", err)
	}
	if _, err := control.AcquireServerSingleton(nilContext, &control.PostgresDB{}); !errors.Is(err, control.ErrInvalidArgument) {
		t.Fatalf("nil context = %v", err)
	}
}
