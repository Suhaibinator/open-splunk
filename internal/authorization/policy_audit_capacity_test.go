package authorization

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/pgmeta"
)

func TestPolicyAuditCapacityFencesConcurrentIndexes(t *testing.T) {
	service, database := postgresPolicyFixture(t)
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	if _, err := database.ExecContext(ctx, `INSERT INTO indexes VALUES('second-index');
 INSERT INTO os_index_organizations(index_id,organization_id)VALUES('second-index',7);
 INSERT INTO os_index_policy_audit(index_id,actor_user_id,actor_user_type,action,old_policy,new_policy)
 SELECT 'index',11,44,'assign','{}'::jsonb,'{}'::jsonb FROM generate_series(1,99999)`); err != nil {
		t.Fatal(err)
	}
	// A native multi-row insert must roll back the whole batch at the bound.
	if _, err := database.ExecContext(ctx, `INSERT INTO os_index_policy_audit(index_id,actor_user_id,actor_user_type,action,old_policy,new_policy)
 SELECT 'index',11,44,'assign','{}'::jsonb,'{}'::jsonb FROM generate_series(1,2)`); err == nil {
		t.Fatal("native batch crossed the audit capacity")
	}
	var seeded int
	if err := database.QueryRowContext(ctx, "SELECT count(*) FROM os_index_policy_audit").Scan(&seeded); err != nil || seeded != 99999 {
		t.Fatalf("failed batch partially committed: %d %v", seeded, err)
	}
	blocker, err := database.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = blocker.Rollback() }()
	if err = pgmeta.LockSQL(ctx, blocker, "policy-audit-capacity"); err != nil {
		t.Fatal(err)
	}
	var blockerPID int
	if err = blocker.QueryRowContext(ctx, "SELECT pg_backend_pid()").Scan(&blockerPID); err != nil {
		t.Fatal(err)
	}
	results := make(chan error, 2)
	for _, indexID := range []string{"index", "second-index"} {
		go func() {
			_, mutationErr := service.ChangeRuleExpected(ctx, Subject{UserID: 11, UserType: 44}, 7,
				Rule{IndexID: indexID, SubjectKind: "role", SubjectID: uint64(ViewerRole), Capability: Search, Effect: "deny"}, 1, false)
			results <- mutationErr
		}()
	}
	// Both different indexes must reach the shared final admission boundary.
	// Observing database waits ensures their transactions actually overlap.
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()
	for {
		var waiting int
		if err = database.QueryRowContext(ctx, "SELECT count(*) FROM pg_stat_activity WHERE $1=ANY(pg_blocking_pids(pid))", blockerPID).Scan(&waiting); err != nil {
			t.Fatal(err)
		}
		if waiting == 2 {
			break
		}
		select {
		case mutationErr := <-results:
			t.Fatalf("mutation escaped audit capacity boundary: %v", mutationErr)
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-tick.C:
		}
	}
	if err = blocker.Commit(); err != nil {
		t.Fatal(err)
	}
	committed, rejected := 0, 0
	for range 2 {
		select {
		case mutationErr := <-results:
			switch {
			case mutationErr == nil:
				committed++
			case errors.Is(mutationErr, ErrInvalidPolicy):
				rejected++
			default:
				t.Fatalf("unexpected capacity result: %v", mutationErr)
			}
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	if committed != 1 || rejected != 1 {
		t.Fatalf("expected one atomic commit and one capacity rejection: committed=%d rejected=%d", committed, rejected)
	}
	var auditCount, ruleCount, revisionSum int
	if err = database.QueryRowContext(ctx, `SELECT
 (SELECT count(*) FROM os_index_policy_audit),
 (SELECT count(*) FROM os_index_permissions),
 (SELECT sum(revision) FROM os_index_organizations)`).Scan(&auditCount, &ruleCount, &revisionSum); err != nil {
		t.Fatal(err)
	}
	if auditCount != 100000 || ruleCount != 1 || revisionSum != 3 {
		t.Fatalf("capacity rejection leaked policy state: audits=%d rules=%d revisions=%d", auditCount, ruleCount, revisionSum)
	}
	if _, err = database.ExecContext(ctx, `INSERT INTO os_index_policy_audit(index_id,actor_user_id,actor_user_type,action,old_policy,new_policy)
 VALUES('index',11,44,'set','{}'::jsonb,'{}'::jsonb)`); err == nil {
		t.Fatal("direct SQL exceeded append-only policy audit capacity")
	}
}
