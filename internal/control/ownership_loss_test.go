package control

import (
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/pgtest"
)

func TestPostgreSQLOwnershipLossAllowsIdempotentCleanup(t *testing.T) {
	dsn := pgtest.DSN(t)
	db, err := Open(t.Context(), dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	var backend int
	if err := db.owner.QueryRowContext(t.Context(), "SELECT pg_backend_pid()").Scan(&backend); err != nil {
		t.Fatal(err)
	}
	var terminated bool
	if err := db.SQLDB().QueryRowContext(t.Context(), "SELECT pg_terminate_backend($1)", backend).Scan(&terminated); err != nil || !terminated {
		t.Fatalf("terminate isolated ownership session: terminated=%t error=%v", terminated, err)
	}
	select {
	case <-db.OwnershipLost():
	case <-time.After(30 * time.Second):
		t.Fatal("loss of the ownership session was not signaled")
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close after ownership loss: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("repeat close after ownership loss: %v", err)
	}
	reopened, err := Open(t.Context(), dsn)
	if err != nil {
		t.Fatalf("reacquire the released isolated ownership lease: %v", err)
	}
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}
}
