package authorization

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"math"

	"github.com/Suhaibinator/open-splunk/internal/pgmeta"
)

const MaximumPolicyRules = 256

// A committed transaction returns ErrTxDone from Rollback. Any other cleanup
// failure remains part of the operation error, including on rejected writes.
func rollbackTransaction(tx *sql.Tx, resultErr *error) {
	if err := tx.Rollback(); err != nil && !errors.Is(err, sql.ErrTxDone) {
		*resultErr = errors.Join(*resultErr, err)
	}
}

type transactionAuthority interface {
	AuthorizationTransactionContext(context.Context, *sql.Tx) (context.Context, error)
	CurrentSubjectInTransaction(context.Context, uint64) (Subject, error)
}

func (s *PostgresStore) Policy(ctx context.Context, indexID string) (_ Policy, resultErr error) {
	tx, err := pgmeta.BeginSQL(ctx, s.db, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return Policy{}, err
	}
	defer rollbackTransaction(tx, &resultErr)
	policy, err := readPolicy(ctx, tx, indexID, false)
	if err != nil {
		return Policy{}, err
	}
	if err = tx.Commit(); err != nil {
		return Policy{}, err
	}
	return policy, nil
}
func readPolicy(ctx context.Context, tx *sql.Tx, indexID string, lock bool) (_ Policy, resultErr error) {
	policy := Policy{Rules: []Rule{}}
	query := "SELECT organization_id,revision FROM os_index_organizations WHERE index_id=$1"
	if lock {
		query += " FOR UPDATE"
	}
	if err := tx.QueryRowContext(ctx, query, indexID).Scan(&policy.OrganizationID, &policy.Revision); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return Policy{}, ErrUnassigned
		}
		return Policy{}, err
	}
	rows, err := tx.QueryContext(ctx, "SELECT subject_kind,subject_id,capability,effect FROM os_index_permissions WHERE index_id=$1 ORDER BY subject_kind,subject_id,capability LIMIT $2", indexID, MaximumPolicyRules+1)
	if err != nil {
		return Policy{}, err
	}
	defer func() { resultErr = errors.Join(resultErr, rows.Close()) }()
	for rows.Next() {
		rule := Rule{IndexID: indexID}
		if err = rows.Scan(&rule.SubjectKind, &rule.SubjectID, &rule.Capability, &rule.Effect); err != nil {
			return Policy{}, err
		}
		if !validRule(rule) {
			return Policy{}, ErrInvalidPolicy
		}
		policy.Rules = append(policy.Rules, rule)
	}
	if len(policy.Rules) > MaximumPolicyRules {
		return Policy{}, ErrInvalidPolicy
	}
	return policy, rows.Err()
}

func lockIdentity(ctx context.Context, tx *sql.Tx, actor Subject, targetUser uint64, organizations ...int64) error {
	var id uint64
	if err := tx.QueryRowContext(ctx, "SELECT id FROM gocommon_users WHERE id=$1 AND deleted_at IS NULL FOR SHARE", actor.UserID).Scan(&id); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ErrUnauthorized
		}
		return err
	}
	for _, userID := range []uint64{actor.UserID, targetUser} {
		if userID == 0 {
			continue
		}
		rows, err := tx.QueryContext(ctx, "SELECT user_id FROM gocommon_user_organizations WHERE user_id=$1 FOR SHARE", userID)
		if err != nil {
			return err
		}
		for rows.Next() {
			if err = rows.Scan(&id); err != nil {
				return errors.Join(err, rows.Close())
			}
		}
		err = errors.Join(rows.Err(), rows.Close())
		if err != nil {
			return err
		}
	}
	for _, org := range organizations {
		if org <= 0 {
			continue
		}
		var live int64
		if err := tx.QueryRowContext(ctx, "SELECT id FROM gocommon_organizations WHERE id=$1 AND deleted_at IS NULL AND personal_owner_user_id IS NULL FOR SHARE", org).Scan(&live); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return ErrForbidden
			}
			return err
		}
	}
	return nil
}

func (s *PostgresStore) ChangeRuleExpected(ctx context.Context, rule Rule, _ int64, expected uint64, actor Subject, remove bool, check policyMutationCheck) (_ uint64, resultErr error) {
	tx, err := pgmeta.BeginSQL(ctx, s.db, nil)
	if err != nil {
		return 0, err
	}
	defer rollbackTransaction(tx, &resultErr)
	old, err := readPolicy(ctx, tx, rule.IndexID, true)
	if err != nil {
		return 0, err
	}
	if old.Revision != expected {
		return 0, ErrConflict
	}
	target := uint64(0)
	if rule.SubjectKind == "user" {
		target = rule.SubjectID
	}
	if err = lockIdentity(ctx, tx, actor, target, old.OrganizationID); err != nil {
		return 0, err
	}
	if s.transactionContext == nil {
		return 0, ErrInvalidPolicy
	}
	txCtx, err := s.transactionContext(ctx, tx)
	if err != nil {
		return 0, err
	}
	if err = check(txCtx, old); err != nil {
		return 0, err
	}
	if s.currentSubject == nil {
		return 0, ErrInvalidPolicy
	}
	actor, err = s.currentSubject(txCtx, actor.UserID)
	if err != nil {
		return 0, err
	}
	if remove {
		_, err = tx.ExecContext(ctx, "DELETE FROM os_index_permissions WHERE index_id=$1 AND subject_kind=$2 AND subject_id=$3 AND capability=$4", rule.IndexID, rule.SubjectKind, rule.SubjectID, rule.Capability)
	} else {
		_, err = tx.ExecContext(ctx, "INSERT INTO os_index_permissions(index_id,subject_kind,subject_id,capability,effect) VALUES($1,$2,$3,$4,$5) ON CONFLICT(index_id,subject_kind,subject_id,capability) DO UPDATE SET effect=EXCLUDED.effect", rule.IndexID, rule.SubjectKind, rule.SubjectID, rule.Capability, rule.Effect)
	}
	if err != nil {
		return 0, err
	}
	if old.Revision >= math.MaxInt64 {
		return 0, ErrInvalidPolicy
	}
	if _, err = tx.ExecContext(ctx, "UPDATE os_index_organizations SET revision=revision+1 WHERE index_id=$1 AND revision=$2", rule.IndexID, expected); err != nil {
		return 0, err
	}
	updated, err := readPolicy(ctx, tx, rule.IndexID, false)
	if err != nil {
		return 0, err
	}
	action := "set"
	if remove {
		action = "delete"
	}
	if err = appendPolicyAudit(ctx, tx, rule.IndexID, actor, action, old, updated); err != nil {
		return 0, err
	}
	if err = tx.Commit(); err != nil {
		return 0, err
	}
	return updated.Revision, nil
}

func (s *PostgresStore) AssignExpected(ctx context.Context, indexID string, organizationID int64, expected uint64, actor Subject, check policyMutationCheck) (_ uint64, resultErr error) {
	tx, err := pgmeta.BeginSQL(ctx, s.db, nil)
	if err != nil {
		return 0, err
	}
	defer rollbackTransaction(tx, &resultErr)
	old, err := readPolicy(ctx, tx, indexID, true)
	if errors.Is(err, ErrUnassigned) {
		old = Policy{Rules: []Rule{}}
		err = nil
	}
	if err != nil {
		return 0, err
	}
	if old.Revision != expected {
		return 0, ErrConflict
	}
	if expected >= math.MaxInt64 {
		return 0, ErrInvalidPolicy
	}
	if err = lockIdentity(ctx, tx, actor, 0, old.OrganizationID, organizationID); err != nil {
		return 0, err
	}
	if s.transactionContext == nil {
		return 0, ErrInvalidPolicy
	}
	txCtx, err := s.transactionContext(ctx, tx)
	if err != nil {
		return 0, err
	}
	if err = check(txCtx, old); err != nil {
		return 0, err
	}
	if s.currentSubject == nil {
		return 0, ErrInvalidPolicy
	}
	actor, err = s.currentSubject(txCtx, actor.UserID)
	if err != nil {
		return 0, err
	}
	if old.OrganizationID != organizationID {
		if _, err = tx.ExecContext(ctx, "DELETE FROM os_index_permissions WHERE index_id=$1", indexID); err != nil {
			return 0, err
		}
	}
	_, err = tx.ExecContext(ctx, "INSERT INTO os_index_organizations(index_id,organization_id,revision) VALUES($1,$2,1) ON CONFLICT(index_id) DO UPDATE SET organization_id=EXCLUDED.organization_id,revision=os_index_organizations.revision+1", indexID, organizationID)
	if err != nil {
		return 0, err
	}
	updated, err := readPolicy(ctx, tx, indexID, false)
	if err != nil {
		return 0, err
	}
	if err = appendPolicyAudit(ctx, tx, indexID, actor, "assign", old, updated); err != nil {
		return 0, err
	}
	if err = tx.Commit(); err != nil {
		return 0, err
	}
	return updated.Revision, nil
}

func appendPolicyAudit(ctx context.Context, tx *sql.Tx, indexID string, actor Subject, action string, old, updated Policy) error {
	var count uint64
	if err := tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM os_index_policy_audit").Scan(&count); err != nil {
		return err
	}
	if count >= 100000 {
		return ErrInvalidPolicy
	}
	before, err := json.Marshal(old)
	if err != nil {
		return err
	}
	after, err := json.Marshal(updated)
	if err != nil {
		return err
	}
	if len(before) > 65536 || len(after) > 65536 {
		return ErrInvalidPolicy
	}
	_, err = tx.ExecContext(ctx, "INSERT INTO os_index_policy_audit(index_id,actor_user_id,actor_user_type,action,old_policy,new_policy) VALUES($1,$2,$3,$4,$5::jsonb,$6::jsonb)", indexID, actor.UserID, actor.UserType, action, string(before), string(after))
	return err
}
