package authorization

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/Suhaibinator/open-splunk/internal/pgmeta"
)

var readVisibilityOptions = sql.TxOptions{ReadOnly: true}

type IndexVisibility struct {
	Name          string
	SearchAllowed bool
}
type Visibility struct {
	Subject     Subject
	Indexes     []IndexVisibility
	Fingerprint string
}

// IndexVisibility resolves native GoCommon membership inside the same read
// transaction as the bounded index catalog and ACL snapshot. No application
// membership query or cache participates in this decision.
func (s *Service) IndexVisibility(ctx context.Context, subject Subject, org int64) (_ Visibility, resultErr error) {
	if !subject.Valid() {
		return Visibility{}, ErrUnauthorized
	}
	store, ok := s.store.(*PostgresStore)
	if !ok {
		return Visibility{}, ErrInvalidPolicy
	}
	tx, err := pgmeta.BeginSQL(ctx, store.db, &readVisibilityOptions)
	if err != nil {
		return Visibility{}, err
	}
	defer rollbackTransaction(tx, &resultErr)
	txCtx, err := store.transactionContext(ctx, tx)
	if err != nil {
		return Visibility{}, err
	}
	fresh, err := store.currentSubject(txCtx, subject.UserID)
	if err != nil {
		return Visibility{}, err
	}
	result := Visibility{Subject: fresh, Indexes: []IndexVisibility{}}
	role := int32(0)
	if !fresh.IsSiteAdmin() {
		role, err = s.OrganizationRole(txCtx, fresh, org)
		if err != nil {
			return Visibility{}, err
		}
	}
	var policyEpoch, indexEpoch, appEpoch int64
	err = tx.QueryRowContext(ctx, `SELECT COALESCE((SELECT max(sequence) FROM os_index_policy_audit),0), COALESCE((SELECT revision FROM index_catalog_state WHERE singleton_id=1),0), COALESCE((SELECT revision FROM app_catalog_revisions WHERE tenant_id=$1),0)`, s.physicalTenant).Scan(&policyEpoch, &indexEpoch, &appEpoch)
	if err != nil {
		return Visibility{}, err
	}
	result.Fingerprint = fmt.Sprintf("%d:%d:%d:%d:%d:%d:%d", fresh.UserID, fresh.UserType, org, role, policyEpoch, indexEpoch, appEpoch)
	if fresh.IsSiteAdmin() {
		if err = tx.Commit(); err != nil {
			return Visibility{}, err
		}
		return result, nil
	}
	rows, err := tx.QueryContext(ctx, `SELECT i.name,(i.state='active' AND i.search_enabled=1 AND NOT COALESCE(bool_or(p.effect='deny'),false)) FROM indexes i JOIN os_index_organizations o ON o.index_id=i.index_id LEFT JOIN os_index_permissions p ON p.index_id=i.index_id AND p.capability='search' AND ((p.subject_kind='user' AND p.subject_id=$2) OR (p.subject_kind='role' AND p.subject_id=$3)) WHERE o.organization_id=$1 GROUP BY i.index_id,i.name,i.state,i.search_enabled ORDER BY i.name LIMIT 1025`, org, fresh.UserID, role)
	if err != nil {
		return Visibility{}, err
	}
	for rows.Next() {
		var index IndexVisibility
		if err = rows.Scan(&index.Name, &index.SearchAllowed); err != nil {
			return Visibility{}, errors.Join(err, rows.Close())
		}
		result.Indexes = append(result.Indexes, index)
	}
	err = errors.Join(rows.Err(), rows.Close())
	if err != nil {
		return Visibility{}, err
	}
	if len(result.Indexes) > 1024 {
		return Visibility{}, ErrInvalidPolicy
	}
	if err = tx.Commit(); err != nil {
		return Visibility{}, err
	}
	return result, nil
}

func (s *Service) SupportsVisibilitySnapshot() bool { _, ok := s.store.(*PostgresStore); return ok }
