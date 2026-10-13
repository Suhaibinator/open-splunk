package authorization

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestPolicyWaitsForUserLifecycleBeforeTakingIndexAuthority(t *testing.T) {
	service, database := postgresPolicyFixture(t)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	deletion, err := database.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = deletion.Rollback() }()
	var blockerPID int
	if err = deletion.QueryRowContext(ctx, "SELECT pg_backend_pid()").Scan(&blockerPID); err != nil {
		t.Fatal(err)
	}
	if _, err = deletion.ExecContext(ctx, "SELECT id FROM gocommon_users WHERE id=10 FOR UPDATE"); err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	go func() {
		_, changeErr := service.ChangeRuleExpected(ctx, Subject{UserID: 9, UserType: 12}, 7, Rule{IndexID: "index", SubjectKind: "user", SubjectID: 10, Capability: Search, Effect: "allow"}, 1, false)
		result <- changeErr
	}()
	// Observe a real database wait, avoiding a scheduling-dependent sleep.
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		var waiting bool
		if err = database.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE $1=ANY(pg_blocking_pids(pid)))", blockerPID).Scan(&waiting); err != nil {
			t.Fatal(err)
		}
		if waiting {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-ticker.C:
		}
	}
	independent, stopIndependent := context.WithTimeout(ctx, 3*time.Second)
	defer stopIndependent()
	revision, err := service.ChangeRuleExpected(independent, Subject{UserID: 11, UserType: 44}, 7, Rule{IndexID: "index", SubjectKind: "role", SubjectID: uint64(ViewerRole), Capability: Search, Effect: "deny"}, 1, false)
	if err != nil || revision != 2 {
		t.Fatalf("index should remain available while another operation waits for identity: %d %v", revision, err)
	}
	if _, err = deletion.ExecContext(ctx, "DELETE FROM gocommon_user_organizations WHERE user_id=10;UPDATE gocommon_users SET deleted_at=clock_timestamp() WHERE id=10"); err != nil {
		t.Fatal(err)
	}
	if err = deletion.Commit(); err != nil {
		t.Fatal(err)
	}
	if err = <-result; !errors.Is(err, ErrForbidden) {
		t.Fatalf("deleted grant subject accepted: %v", err)
	}
	var grants, audits int
	if err = database.QueryRowContext(ctx, "SELECT (SELECT count(*) FROM os_index_permissions WHERE subject_kind='user'),(SELECT count(*) FROM os_index_policy_audit)").Scan(&grants, &audits); err != nil || grants != 0 || audits != 1 {
		t.Fatalf("rejected lifecycle race changed policy/audit: grants=%d audits=%d %v", grants, audits, err)
	}
}
