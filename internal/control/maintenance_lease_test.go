package control

import (
	"errors"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/pgtest"
)

func TestPostgreSQLMaintenanceLeaseProtectsExactlyOneStoppedControlSchema(t *testing.T) {
	dsn := pgtest.DSN(t)
	db, err := Open(t.Context(), dsn)
	if err != nil {
		t.Fatal(err)
	}
	if lease, err := AcquireMaintenanceLease(t.Context(), dsn); !errors.Is(err, ErrServerAlreadyRunning) {
		if lease != nil {
			_ = lease.Close()
		}
		_ = db.Close()
		t.Fatalf("maintenance accepted live server: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	lease, err := AcquireMaintenanceLease(t.Context(), dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer lease.Close()
	if err := lease.Validate(dsn); err != nil {
		t.Fatal(err)
	}
	if err := lease.Validate(pgtest.DSN(t)); err == nil {
		t.Fatal("unrelated target accepted held lease")
	}
	if running, err := Open(t.Context(), dsn); !errors.Is(err, ErrServerAlreadyRunning) {
		if running != nil {
			_ = running.Close()
		}
		t.Fatalf("server accepted maintenance lease: %v", err)
	}
	reader, err := OpenReadOnly(t.Context(), dsn)
	if err != nil {
		t.Fatal(err)
	}
	if err := reader.Close(); err != nil {
		t.Fatal(err)
	}
	if err := lease.Close(); err != nil {
		t.Fatal(err)
	}
	if err := lease.Validate(dsn); err == nil {
		t.Fatal("closed lease accepted")
	}
	restarted, err := Open(t.Context(), dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer restarted.Close()
}
