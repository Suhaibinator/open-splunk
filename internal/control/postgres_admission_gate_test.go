package control

import (
	"errors"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/testsupport/pgtest"
	"github.com/Suhaibinator/open-splunk/migrations"
)

// Components built over the PostgreSQL handle, such as the knowledge
// catalog's resolver and writer, share fail-fast gates exactly as they do
// over the SQLite handle.
func TestPostgresSharedAdmissionGateIsUniqueBoundedAndFailFast(t *testing.T) {
	t.Parallel()
	database := newInternalPostgresDB(t)
	gate, err := database.SharedAdmissionGate("knowledge-resolution", 2)
	if err != nil {
		t.Fatalf("SharedAdmissionGate(): %v", err)
	}
	same, err := database.SharedAdmissionGate("knowledge-resolution", 2)
	if err != nil || same != gate {
		t.Fatalf("SharedAdmissionGate(same) = (%p, %v), want %p", same, err, gate)
	}
	if _, err := database.SharedAdmissionGate("knowledge-resolution", 3); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("SharedAdmissionGate(mismatched capacity) error = %v, want ErrInvalidArgument", err)
	}
	other, err := database.SharedAdmissionGate("knowledge-validation", 1)
	if err != nil || other == gate {
		t.Fatalf("SharedAdmissionGate(other name) = (%p, %v), want a distinct gate", other, err)
	}
	for permit := range 2 {
		if !gate.TryAcquire() {
			t.Fatalf("shared gate did not grant permit %d of 2", permit+1)
		}
	}
	if gate.TryAcquire() {
		t.Fatal("shared gate did not enforce its exact fail-fast capacity")
	}
	if !other.TryAcquire() {
		t.Fatal("a full gate starved a differently named gate")
	}
	gate.Release()
	if !gate.TryAcquire() {
		t.Fatal("shared gate did not return a released permit")
	}
	gate.Release()
	gate.Release()
	other.Release()
}

func TestPostgresSharedAdmissionGateRejectsInvalidConstruction(t *testing.T) {
	t.Parallel()
	if _, err := (*PostgresDB)(nil).SharedAdmissionGate("gate", 1); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("nil PostgresDB error = %v, want ErrInvalidArgument", err)
	}
	if _, err := (&PostgresDB{}).SharedAdmissionGate("gate", 1); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("unopened PostgresDB error = %v, want ErrInvalidArgument", err)
	}
	database := newInternalPostgresDB(t)
	for _, test := range []struct {
		name     string
		capacity int
	}{
		{name: "", capacity: 1},
		{name: "   ", capacity: 1},
		{name: "gate", capacity: 0},
	} {
		if _, err := database.SharedAdmissionGate(test.name, test.capacity); !errors.Is(err, ErrInvalidArgument) {
			t.Fatalf("SharedAdmissionGate(%q, %d) error = %v, want ErrInvalidArgument", test.name, test.capacity, err)
		}
	}
}

// In-package tests get a migrated, seeded PostgreSQL database through pgtest
// without an import cycle, and can reach the handle's unexported state.
func TestInPackageTestsOpenSeededPostgresDatabases(t *testing.T) {
	t.Parallel()
	database := newInternalPostgresDB(t, pgtest.WithDefaultTenant())
	if database.writeQueue == nil || database.connConfig == nil {
		t.Fatal("in-package handle lacks its write queue or connection config")
	}
	if _, err := database.VerifyCurrentMigrations(t.Context(), migrations.Postgres()); err != nil {
		t.Fatalf("VerifyCurrentMigrations() = %v", err)
	}
	var tenants int
	if err := database.SQLDB().QueryRowContext(t.Context(),
		`SELECT count(*) FROM tenants WHERE tenant_id = $1`, pgtest.DefaultTenantID,
	).Scan(&tenants); err != nil || tenants != 1 {
		t.Fatalf("seeded default tenants = %d, %v; want 1", tenants, err)
	}
}
