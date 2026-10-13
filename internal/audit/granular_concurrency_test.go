package audit

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/pgmeta"
)

func TestAuditTenantContentionDoesNotBlockAnotherTenant(t *testing.T) {
	database := openAuditTestDatabase(t)
	store := newAuditTestStore(t, database, auditTestCursorKey())
	tx := pgmeta.Begin(database.GORMDB().WithContext(t.Context()))
	if tx.Error != nil {
		t.Fatal(tx.Error)
	}
	defer tx.Rollback()
	if _, err := store.AppendInTransaction(t.Context(), tx, "held-tenant", auditTestDefinition(ActionIngestionTokenCreate, "held", 1)); err != nil {
		t.Fatal(err)
	}
	independent, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	if event, err := store.Append(independent, "independent-tenant", auditTestDefinition(ActionIngestionTokenCreate, "other", 1)); err != nil || event.Sequence != 1 {
		t.Fatalf("independent tenant: %+v, %v", event, err)
	}
	conflicting, cancelConflict := context.WithTimeout(t.Context(), 150*time.Millisecond)
	defer cancelConflict()
	if _, err := store.Append(conflicting, "held-tenant", auditTestDefinition(ActionIngestionTokenCreate, "contender", 1)); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("conflicting tenant should wait for provisional audit: %v", err)
	}
	if err := tx.Rollback().Error; err != nil {
		t.Fatal(err)
	}
	if event, err := store.Append(t.Context(), "held-tenant", auditTestDefinition(ActionIngestionTokenCreate, "after-rollback", 1)); err != nil || event.Sequence != 1 {
		t.Fatalf("rollback must release allocation: %+v, %v", event, err)
	}
}
