package control

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"sync"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/pgtest"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
)

// Delaying the actual driver ping makes shutdown overlap with a heartbeat
// deterministically. The underlying connection and advisory lock are real.
type delayedMaintenancePingConnector struct {
	driver.Connector
	entered chan context.Context
	release chan struct{}
}

func (connector delayedMaintenancePingConnector) Connect(ctx context.Context) (driver.Conn, error) {
	connection, err := connector.Connector.Connect(ctx)
	if err != nil {
		return nil, err
	}
	return delayedMaintenancePingConnection{Conn: connection, entered: connector.entered, release: connector.release}, nil
}

type delayedMaintenancePingConnection struct {
	driver.Conn
	entered chan context.Context
	release chan struct{}
}

func (connection delayedMaintenancePingConnection) Ping(ctx context.Context) error {
	connection.entered <- ctx
	<-connection.release
	return connection.Conn.(driver.Pinger).Ping(ctx)
}

func TestPostgreSQLMaintenanceLeaseCloseJoinsInFlightPing(t *testing.T) {
	dsn := pgtest.DSN(t)
	cfg, err := pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	entered := make(chan context.Context, 1)
	release := make(chan struct{})
	var releaseOnce sync.Once
	releasePing := func() { releaseOnce.Do(func() { close(release) }) }
	pool := sql.OpenDB(delayedMaintenancePingConnector{Connector: stdlib.GetConnector(*cfg), entered: entered, release: release})
	lease := &MaintenanceLease{source: dsn, pool: pool, lost: make(chan struct{})}
	t.Cleanup(func() {
		releasePing()
		_ = lease.Close()
	})
	lease.conn, err = pool.Conn(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if err = lease.conn.QueryRowContext(t.Context(), "SELECT hashtextextended('open-splunk:server:' || current_database() || ':' || current_schema(),0)").Scan(&lease.key); err != nil {
		t.Fatal(err)
	}
	var owned bool
	if err = lease.conn.QueryRowContext(t.Context(), "SELECT pg_try_advisory_lock($1)", lease.key).Scan(&owned); err != nil || !owned {
		t.Fatalf("acquire isolated maintenance lock: owned=%t error=%v", owned, err)
	}
	lease.startOwnershipWatcher()
	var pingContext context.Context
	select {
	case pingContext = <-entered:
	case <-time.After(10 * time.Second):
		t.Fatal("maintenance heartbeat did not start")
	}
	deadline, bounded := pingContext.Deadline()
	if !bounded || time.Until(deadline) > 5*time.Second {
		t.Fatal("maintenance heartbeat is not independently bounded")
	}
	closed := make(chan error, 1)
	go func() { closed <- lease.Close() }()
	select {
	case <-pingContext.Done():
		t.Fatalf("closing the lease canceled its in-flight driver ping: %v", pingContext.Err())
	case err := <-closed:
		t.Fatalf("close returned before the in-flight heartbeat finished: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	if err := lease.Validate(dsn); err == nil {
		t.Fatal("closing lease still validated while awaiting its heartbeat")
	}
	releasePing()
	select {
	case err := <-closed:
		if err != nil {
			t.Fatalf("close after overlapping heartbeat: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("close did not join the completed heartbeat")
	}
	replacement, err := AcquireMaintenanceLease(t.Context(), dsn)
	if err != nil {
		t.Fatalf("reacquire released maintenance lock: %v", err)
	}
	if err := replacement.Close(); err != nil {
		t.Fatal(err)
	}
}
