package authorization

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/pgtest"
)

type policySQLContext struct{}

// This fixture tests transaction fencing and audit storage. Native GoCommon
// membership service acceptance lives in internal/identity.
type transactionMembershipFixture struct{ db *sql.DB }

func (f transactionMembershipFixture) AuthorizationTransactionContext(ctx context.Context, tx *sql.Tx) (context.Context, error) {
	return context.WithValue(ctx, policySQLContext{}, tx), nil
}
func (f transactionMembershipFixture) CurrentSubjectInTransaction(ctx context.Context, id uint64) (Subject, error) {
	var userType int32
	query := f.db.QueryRowContext
	if tx, ok := ctx.Value(policySQLContext{}).(*sql.Tx); ok {
		query = tx.QueryRowContext
	}
	err := query(ctx, "SELECT user_type FROM gocommon_users WHERE id=$1 AND deleted_at IS NULL", id).Scan(&userType)
	if errors.Is(err, sql.ErrNoRows) {
		return Subject{}, ErrUnauthorized
	}
	return Subject{UserID: id, UserType: userType}, err
}
func (f transactionMembershipFixture) EffectiveRole(ctx context.Context, id uint64, org int64) (int32, bool, error) {
	var role int32
	query := f.db.QueryRowContext
	if tx, ok := ctx.Value(policySQLContext{}).(*sql.Tx); ok {
		query = tx.QueryRowContext
	}
	err := query(ctx, "SELECT role FROM gocommon_user_organizations WHERE user_id=$1 AND organization_id=$2", id, org).Scan(&role)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, false, nil
	}
	return role, err == nil, err
}
func postgresPolicyFixture(t *testing.T) (*Service, *sql.DB) {
	t.Helper()
	db := pgtest.Open(t)
	_, err := db.ExecContext(t.Context(), `CREATE FUNCTION os_control_write_lock() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN PERFORM pg_advisory_xact_lock(hashtextextended(current_database() || ':' || current_schema(),5716000640707417153)); RETURN NULL; END $$;
 CREATE TABLE indexes(index_id text PRIMARY KEY);
 CREATE TABLE gocommon_users(id bigint PRIMARY KEY,user_type integer NOT NULL,deleted_at timestamptz);
 CREATE TABLE gocommon_organizations(personal_owner_user_id bigint,id bigint PRIMARY KEY,deleted_at timestamptz);
 CREATE TABLE gocommon_user_organizations(user_id bigint,organization_id bigint,role integer,PRIMARY KEY(user_id,organization_id));
 CREATE TABLE saved_searches(organization_id bigint); CREATE TABLE dashboards(organization_id bigint); CREATE TABLE app_workspaces(organization_id bigint); CREATE TABLE knowledge_objects(organization_id bigint); CREATE TABLE knowledge_lookup_definitions(organization_id bigint); CREATE TABLE ingestion_tokens(organization_id bigint);CREATE TABLE saved_search_schedules(organization_id bigint);CREATE TABLE saved_search_schedule_runs(organization_id bigint);CREATE TABLE alerts(organization_id bigint);CREATE TABLE search_history(organization_id bigint);CREATE TABLE search_history_pending(organization_id bigint);CREATE TABLE durable_search_jobs(organization_id bigint);CREATE TABLE durable_export_jobs(organization_id bigint);CREATE TABLE search_capture_sessions(organization_id bigint); CREATE TABLE search_retained_export_reservations(organization_id bigint);`)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = db.ExecContext(t.Context(), Schema); err != nil {
		t.Fatal(err)
	}
	if _, err = db.ExecContext(t.Context(), `INSERT INTO indexes VALUES('index');INSERT INTO gocommon_users(id,user_type)VALUES(9,12),(10,12),(11,44);INSERT INTO gocommon_organizations(id)VALUES(7),(8);INSERT INTO gocommon_user_organizations VALUES(9,7,23),(10,7,1);INSERT INTO os_index_organizations(index_id,organization_id)VALUES('index',7)`); err != nil {
		t.Fatal(err)
	}
	service, err := New(db, transactionMembershipFixture{db}, "physical")
	if err != nil {
		t.Fatal(err)
	}
	return service, db
}
func TestPostgresPolicyRevisionAuditAndRejectedChangesAreAtomic(t *testing.T) {
	service, db := postgresPolicyFixture(t)
	actor := Subject{UserID: 9, UserType: 12}
	rule := Rule{IndexID: "index", SubjectKind: "user", SubjectID: 10, Capability: Search, Effect: "deny"}
	revision, err := service.ChangeRuleExpected(t.Context(), actor, 7, rule, 1, false)
	if err != nil || revision != 2 {
		t.Fatalf("mutation %d %v", revision, err)
	}
	var before, after int
	var actorType int32
	if err = db.QueryRowContext(t.Context(), "SELECT (old_policy->>'revision')::integer,(new_policy->>'revision')::integer,actor_user_type FROM os_index_policy_audit").Scan(&before, &after, &actorType); err != nil || before != 1 || after != 2 || actorType != 12 {
		t.Fatalf("audit %d→%d type%d %v", before, after, actorType, err)
	}
	if _, err = service.ChangeRuleExpected(t.Context(), actor, 7, rule, 1, true); !errors.Is(err, ErrConflict) {
		t.Fatalf("stale mutation: %v", err)
	}
	outsider := rule
	outsider.SubjectID = 11
	if _, err = service.ChangeRuleExpected(t.Context(), actor, 7, outsider, 2, false); !errors.Is(err, ErrForbidden) {
		t.Fatalf("nonmember grant: %v", err)
	}
	var count int
	if err = db.QueryRowContext(t.Context(), "SELECT count(*) FROM os_index_policy_audit").Scan(&count); err != nil || count != 1 {
		t.Fatalf("failed mutation changed audit %d %v", count, err)
	}
	policy, _, err := service.GetPolicy(t.Context(), actor, 7, "index")
	if err != nil || policy.Revision != 2 || len(policy.Rules) != 1 {
		t.Fatalf("failed mutation changed policy %#v %v", policy, err)
	}
	if _, err = db.ExecContext(t.Context(), "UPDATE os_index_policy_audit SET action='delete'"); err == nil {
		t.Fatal("audit mutable")
	}
	if _, err = db.ExecContext(t.Context(), "DELETE FROM os_index_policy_audit"); err == nil {
		t.Fatal("audit deletable")
	}
}
func TestPostgresPolicyUsesFreshActorAndCommittedMembership(t *testing.T) {
	service, db := postgresPolicyFixture(t)
	rule := Rule{IndexID: "index", SubjectKind: "user", SubjectID: 10, Capability: Search, Effect: "allow"}
	// A stale site-admin request must not retain the unconditional bypass.
	if _, err := db.ExecContext(t.Context(), "UPDATE gocommon_users SET user_type=12 WHERE id=11"); err != nil {
		t.Fatal(err)
	}
	if _, err := service.ChangeRuleExpected(t.Context(), Subject{UserID: 11, UserType: 44}, 7, rule, 1, false); !errors.Is(err, ErrForbidden) {
		t.Fatalf("demoted stale actor retained authority: %v", err)
	}
	// A revocation which holds the common write lock commits before the later
	// policy transaction; the latter must check membership inside its own tx.
	tx, err := db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := tx.Rollback(); err != nil && !errors.Is(err, sql.ErrTxDone) {
			t.Errorf("rollback: %v", err)
		}
	}()
	if _, err = tx.ExecContext(t.Context(), "DELETE FROM gocommon_user_organizations WHERE user_id=9"); err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	go func() {
		_, err := service.ChangeRuleExpected(t.Context(), Subject{UserID: 9, UserType: 12}, 7, rule, 1, false)
		result <- err
	}()
	select {
	case err := <-result:
		t.Fatalf("mutation passed concurrent revocation lock: %v", err)
	case <-time.After(25 * time.Millisecond):
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-result:
		if !errors.Is(err, ErrForbidden) {
			t.Fatalf("revoked actor: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("policy mutation deadlocked")
	}
}
