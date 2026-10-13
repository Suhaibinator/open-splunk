package control

import (
	"context"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/pgmeta"
)

func TestAutocommitCaptureMutationLocksBudgetBeforeTuple(t *testing.T) {
	database := openTestDB(t)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	if _, err := database.SQLDB().ExecContext(ctx, `INSERT INTO search_capture_sessions(capture_id,job_id,tenant_id,owner_id,organization_id,reserved_bytes,created_at_us)VALUES('held','held','tenant','owner',1,0,1)`); err != nil {
		t.Fatal(err)
	}
	authority, err := pgmeta.BeginSQLLocked(ctx, database.SQLDB(), pgmeta.RetainedStorage)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = authority.Rollback() }()
	var blockerPID int
	if err = authority.QueryRowContext(ctx, "SELECT pg_backend_pid()").Scan(&blockerPID); err != nil {
		t.Fatal(err)
	}
	contender := make(chan error, 1)
	go func() {
		_, err := database.SQLDB().ExecContext(ctx, "UPDATE search_capture_sessions SET reserved_bytes=reserved_bytes+1 WHERE capture_id='held'")
		contender <- err
	}()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		var blocked bool
		if err = database.SQLDB().QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE $1=ANY(pg_blocking_pids(pid)))", blockerPID).Scan(&blocked); err != nil {
			t.Fatal(err)
		}
		if blocked {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-ticker.C:
		}
	}
	// The contender must be waiting before it owns the tuple. A row-level
	// invariant trigger would invert this order and deadlock this update.
	update, stopUpdate := context.WithTimeout(ctx, 2*time.Second)
	defer stopUpdate()
	if _, err = authority.ExecContext(update, "UPDATE search_capture_sessions SET reserved_bytes=reserved_bytes+1 WHERE capture_id='held'"); err != nil {
		t.Fatalf("contender acquired tuple before budget: %v", err)
	}
	if err = authority.Commit(); err != nil {
		t.Fatal(err)
	}
	if err = <-contender; err != nil {
		t.Fatal(err)
	}
	var bytes int
	if err = database.SQLDB().QueryRowContext(ctx, "SELECT reserved_bytes FROM search_capture_sessions WHERE capture_id='held'").Scan(&bytes); err != nil || bytes != 2 {
		t.Fatalf("lost capture mutation: bytes=%d %v", bytes, err)
	}
}
