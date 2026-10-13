package main

import (
	"context"
	"errors"
	"testing"
	"time"

	pm "github.com/Suhaibinator/go-common/pkg/common_persistence/common_persistence_models"
	"github.com/Suhaibinator/open-splunk/internal/pgmeta"
	"github.com/jackc/pgx/v5/pgconn"
)

func TestOrganizationDeletionFencesConcurrentResourceInsertion(t *testing.T) {
	_, database := newRuntimeIdentityFixture(t)
	org := pm.Organization{Name: "Deletion race", Slug: "deletion-race"}
	if err := database.GORMDB().Create(&org).Error; err != nil {
		t.Fatal(err)
	}
	deletion := pgmeta.Begin(database.GORMDB().WithContext(t.Context()))
	if deletion.Error != nil {
		t.Fatal(deletion.Error)
	}
	defer deletion.Rollback()
	if err := beforeRuntimeOrganizationDelete(t.Context(), deletion, org.ID); err != nil {
		t.Fatal(err)
	}
	if err := deletion.Exec("UPDATE gocommon_organizations SET deleted_at=clock_timestamp() WHERE id=?", org.ID).Error; err != nil {
		t.Fatal(err)
	}
	waiting, cancel := context.WithTimeout(t.Context(), 150*time.Millisecond)
	defer cancel()
	query := `INSERT INTO search_retained_export_reservations(export_job_id,organization_id,reserved_bytes)VALUES('deletion-race-export',$1,1)`
	if _, err := database.SQLDB().ExecContext(waiting, query, org.ID); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("insertion must wait for organization deletion: %v", err)
	}
	if err := deletion.Commit().Error; err != nil {
		t.Fatal(err)
	}
	_, err := database.SQLDB().ExecContext(t.Context(), query, org.ID)
	var native *pgconn.PgError
	if !errors.As(err, &native) || native.Code != "23503" {
		t.Fatalf("insertion into soft-deleted organization: %v", err)
	}
	var count int
	if err = database.SQLDB().QueryRowContext(t.Context(), "SELECT count(*) FROM search_retained_export_reservations").Scan(&count); err != nil || count != 0 {
		t.Fatalf("orphaned resource count=%d: %v", count, err)
	}
}
