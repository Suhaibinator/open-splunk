package control

import (
	"testing"

	"github.com/Suhaibinator/open-splunk/migrations"
)

// The liveness probe passes while the dedicated session holds the lock and
// fails once PostgreSQL has ended that session.
func TestServerSingletonProbeDetectsATerminatedSession(t *testing.T) {
	t.Parallel()
	migrationFS := migrations.Postgres()
	dsn := newInternalPostgresDSN(t, migrationFS)
	db, release, err := OpenPostgresServerWithMigrations(t.Context(), dsn, migrationFS)
	if err != nil {
		t.Fatalf("OpenPostgresServerWithMigrations() = %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	db.mu.Lock()
	held := make([]*serverSingleton, 0, len(db.singletons))
	for singleton := range db.singletons {
		held = append(held, singleton)
	}
	db.mu.Unlock()
	if len(held) != 1 {
		t.Fatalf("server handle holds %d singletons, want 1", len(held))
	}
	singleton := held[0]
	if err := singleton.probe(); err != nil {
		t.Fatalf("probe() of a live holder = %v", err)
	}
	pid := singleton.conn.PgConn().PID()
	var terminated bool
	if err := db.SQLDB().QueryRowContext(t.Context(),
		`SELECT pg_terminate_backend($1, 5000)`, int64(pid)).Scan(&terminated); err != nil || !terminated {
		t.Fatalf("terminate singleton session = %v, %v", terminated, err)
	}
	if err := singleton.probe(); err == nil {
		t.Fatal("probe() of a terminated holder reported the lock held")
	}
	if err := release(); err == nil {
		t.Fatal("release() of a terminated holder reported success")
	}
}
