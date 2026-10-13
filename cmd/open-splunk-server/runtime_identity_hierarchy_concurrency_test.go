package main

import (
	"context"
	"testing"
	"time"

	pm "github.com/Suhaibinator/go-common/pkg/common_persistence/common_persistence_models"
	"github.com/Suhaibinator/open-splunk/internal/pgmeta"
	"github.com/lib/pq"
)

func TestOrganizationDeletionFencesChildCommittedDuringRootLockWait(t *testing.T) {
	_, database := newRuntimeIdentityFixture(t)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	root := pm.Organization{Name: "Concurrent hierarchy", Slug: "concurrent-hierarchy"}
	if err := database.GORMDB().WithContext(ctx).Create(&root).Error; err != nil {
		t.Fatal(err)
	}
	creation := pgmeta.Begin(database.GORMDB().WithContext(ctx))
	if creation.Error != nil {
		t.Fatal(creation.Error)
	}
	defer creation.Rollback()
	// Reproduce the native child-creation commit boundary: its ancestor shares
	// remain held until the newly inserted descendant commits.
	if err := creation.Exec("SELECT id FROM gocommon_organizations WHERE id=? AND deleted_at IS NULL FOR SHARE", root.ID).Error; err != nil {
		t.Fatal(err)
	}
	child := pm.Organization{Name: "Concurrent child", Slug: "concurrent-child", ParentOrganizationID: &root.ID, Lineage: pq.Int64Array{root.ID}}
	if err := creation.Create(&child).Error; err != nil {
		t.Fatal(err)
	}
	var creationPID int
	if err := creation.Raw("SELECT pg_backend_pid()").Scan(&creationPID).Error; err != nil {
		t.Fatal(err)
	}
	deletion := pgmeta.Begin(database.GORMDB().WithContext(ctx))
	if deletion.Error != nil {
		t.Fatal(deletion.Error)
	}
	defer deletion.Rollback()
	var deletionPID int
	if err := deletion.Raw("SELECT pg_backend_pid()").Scan(&deletionPID).Error; err != nil {
		t.Fatal(err)
	}
	finished := make(chan error, 1)
	go func() { finished <- beforeRuntimeOrganizationDelete(ctx, deletion, root.ID) }()
	// Observe deletion blocked on the root while the child is still invisible
	// to that statement's snapshot. This makes the phantom reproducible.
	tick := time.NewTicker(5 * time.Millisecond)
	defer tick.Stop()
	for {
		var waiting bool
		if err := database.SQLDB().QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE $1=ANY(pg_blocking_pids(pid)))", creationPID).Scan(&waiting); err != nil {
			t.Fatal(err)
		}
		if waiting {
			break
		}
		select {
		case err := <-finished:
			t.Fatalf("deletion escaped child creation boundary: %v", err)
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-tick.C:
		}
	}
	if err := creation.Commit().Error; err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-finished:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	// Every descendant, including the one committed during the wait, must now
	// be fenced. Otherwise a new dependent resource can win before soft deletion.
	query := `INSERT INTO search_retained_export_reservations(export_job_id,organization_id,reserved_bytes)VALUES('concurrent-child-export',$1,1)`
	inserted := make(chan error, 1)
	go func() {
		_, err := database.SQLDB().ExecContext(ctx, query, child.ID)
		inserted <- err
	}()
	for {
		var waiting bool
		if err := database.SQLDB().QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE $1=ANY(pg_blocking_pids(pid)))", deletionPID).Scan(&waiting); err != nil {
			t.Fatal(err)
		}
		if waiting {
			break
		}
		select {
		case err := <-inserted:
			t.Fatalf("resource insertion escaped newly committed descendant fence: %v", err)
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-tick.C:
		}
	}
	if err := deletion.Rollback().Error; err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-inserted:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	retry := pgmeta.Begin(database.GORMDB().WithContext(ctx))
	if retry.Error != nil {
		t.Fatal(retry.Error)
	}
	defer retry.Rollback()
	if err := beforeRuntimeOrganizationDelete(ctx, retry, root.ID); err == nil || err.Error() != "organization contains OpenSplunk resources" {
		t.Fatalf("parent deletion did not preserve new child resource: %v", err)
	}
}
