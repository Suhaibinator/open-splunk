package authorization

import (
	"context"
	"testing"
	"time"
)

func TestAssignmentWaitsForAncestorBeforeLockingLowerIDChild(t *testing.T) {
	service, database := postgresPolicyFixture(t)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	// Physical IDs do not define hierarchy: the child predates its imported
	// parent ID, and root lineage is NULL as in native GoCommon root rows.
	if _, err := database.ExecContext(ctx, "UPDATE gocommon_organizations SET lineage=ARRAY[8]::bigint[] WHERE id=7"); err != nil {
		t.Fatal(err)
	}
	deletion, err := database.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = deletion.Rollback() }()
	var deletionPID int
	if err = deletion.QueryRowContext(ctx, "SELECT pg_backend_pid()").Scan(&deletionPID); err != nil {
		t.Fatal(err)
	}
	// Organization deletion first fences the root before discovering children.
	if _, err = deletion.ExecContext(ctx, "SELECT id FROM gocommon_organizations WHERE id=8 FOR UPDATE"); err != nil {
		t.Fatal(err)
	}
	finished := make(chan error, 1)
	go func() {
		_, err := service.AssignIndexExpected(ctx, Subject{UserID: 11, UserType: 44}, "index", 8, 1)
		finished <- err
	}()
	tick := time.NewTicker(5 * time.Millisecond)
	defer tick.Stop()
	for {
		var waiting bool
		if err = database.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE $1=ANY(pg_blocking_pids(pid)))", deletionPID).Scan(&waiting); err != nil {
			t.Fatal(err)
		}
		if waiting {
			break
		}
		select {
		case err := <-finished:
			t.Fatalf("assignment escaped ancestor deletion fence: %v", err)
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-tick.C:
		}
	}
	// Waiting assignment must not hold the child and invert deletion's order.
	childCtx, cancelChild := context.WithTimeout(ctx, time.Second)
	defer cancelChild()
	if _, err = deletion.ExecContext(childCtx, "SELECT id FROM gocommon_organizations WHERE id=7 FOR UPDATE"); err != nil {
		t.Fatalf("assignment took child before its ancestor, blocking subtree deletion: %v", err)
	}
	if err = deletion.Rollback(); err != nil {
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
	var owner, revision, audits int
	if err = database.QueryRowContext(ctx, `SELECT organization_id,revision,
 (SELECT count(*) FROM os_index_policy_audit) FROM os_index_organizations WHERE index_id='index'`).Scan(&owner, &revision, &audits); err != nil {
		t.Fatal(err)
	}
	if owner != 8 || revision != 2 || audits != 1 {
		t.Fatalf("assignment publication was not atomic: owner=%d revision=%d audits=%d", owner, revision, audits)
	}
}
