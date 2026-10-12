package authorization

import (
	"context"
	"database/sql"
	_ "embed"
	"errors"
)

//go:embed schema.sql
var Schema string

type PostgresStore struct {
	db                 *sql.DB
	transactionContext func(context.Context, *sql.Tx) (context.Context, error)
	currentSubject     func(context.Context, uint64) (Subject, error)
}

func NewPostgresStore(db *sql.DB) (*PostgresStore, error) {
	if db == nil {
		return nil, ErrInvalidPolicy
	}
	return &PostgresStore{db: db}, nil
}
func New(db *sql.DB, memberships MembershipResolver, physicalTenant string) (*Service, error) {
	store, err := NewPostgresStore(db)
	if err != nil {
		return nil, err
	}
	return NewService(store, memberships, physicalTenant)
}
func (s *PostgresStore) OrganizationForIndex(ctx context.Context, indexID string) (int64, error) {
	var org int64
	err := s.db.QueryRowContext(ctx, "SELECT organization_id FROM os_index_organizations WHERE index_id=$1", indexID).Scan(&org)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, ErrUnassigned
	}
	return org, err
}
func (s *PostgresStore) Rules(ctx context.Context, indexID string) ([]Rule, error) {
	policy, err := s.Policy(ctx, indexID)
	return policy.Rules, err
}
