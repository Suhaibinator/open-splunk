package auth

import (
	"context"
	"fmt"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/control"
	"gorm.io/gorm"
)

// RevalidateCollectorInTransaction revalidates a bearer credential at
// checkedAt and resolves its complete current ingestion scope using a
// caller-owned transaction. It does not record token use and neither commits
// nor rolls back transaction. A verified identity may accompany
// ErrNoActiveIndexAuthority or ErrInvalidIndexAuthority so an exact current
// lease can recover a durable batch outcome before mutable scope is enforced.
//
// Callers must use an active transaction from the same migrated control
// database and include every other authorization-boundary read in that
// transaction. Reading token and durable lease state in separate snapshots is
// unsafe.
func (store *Store) RevalidateCollectorInTransaction(
	ctx context.Context,
	transaction *gorm.DB,
	plaintext string,
	checkedAt time.Time,
) (Authentication, error) {
	transaction, checkedAt, err := collectorRevalidationInputs(
		ctx,
		transaction,
		checkedAt,
	)
	if err != nil {
		return Authentication{}, err
	}
	authentication, err := store.authenticateForLease(
		transaction,
		plaintext,
		checkedAt,
	)
	if err != nil {
		return authentication, err
	}
	return authentication, nil
}

// RevalidateAndRecordCollectorUseInTransaction revalidates a bearer
// credential at acceptedAt and records its monotonic last-use observation
// using a caller-owned transaction. It neither commits nor rolls back
// transaction.
//
// Callers must use an immediate transaction from the same migrated control
// database and include every other collector-admission decision in that
// transaction. Committing this update separately from durable fleet lease
// allocation is unsafe.
func (store *Store) RevalidateAndRecordCollectorUseInTransaction(
	ctx context.Context,
	transaction *gorm.DB,
	plaintext string,
	acceptedAt time.Time,
) (Authentication, error) {
	transaction, acceptedAt, err := collectorRevalidationInputs(
		ctx,
		transaction,
		acceptedAt,
	)
	if err != nil {
		return Authentication{}, err
	}
	authentication, err := store.authenticate(transaction, plaintext, acceptedAt)
	if err != nil {
		return Authentication{}, err
	}
	if err := recordCollectorTokenUse(
		transaction.WithContext(ctx),
		authentication.TokenID,
		acceptedAt,
	); err != nil {
		return Authentication{}, err
	}
	return authentication, nil
}

func collectorRevalidationInputs(
	ctx context.Context,
	transaction *gorm.DB,
	checkedAt time.Time,
) (*gorm.DB, time.Time, error) {
	if ctx == nil {
		return nil, time.Time{}, fmt.Errorf(
			"%w: nil context",
			control.ErrInvalidArgument,
		)
	}
	if transaction == nil ||
		transaction.Statement == nil ||
		transaction.Statement.ConnPool == nil {
		return nil, time.Time{}, fmt.Errorf(
			"%w: collector authorization transaction is required",
			control.ErrInvalidArgument,
		)
	}
	if _, ok := transaction.Statement.ConnPool.(gorm.TxCommitter); !ok {
		return nil, time.Time{}, fmt.Errorf(
			"%w: collector authorization requires an active database transaction",
			control.ErrInvalidArgument,
		)
	}
	checkedAt = databaseTime(checkedAt)
	if checkedAt.IsZero() {
		return nil, time.Time{}, fmt.Errorf(
			"%w: collector credential check time is required",
			control.ErrInvalidArgument,
		)
	}
	return transaction.WithContext(ctx), checkedAt, nil
}
