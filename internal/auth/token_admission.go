package auth

import (
	"context"
	"fmt"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/control"
	"gorm.io/gorm"
)

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
	if ctx == nil {
		return Authentication{}, fmt.Errorf("%w: nil context", control.ErrInvalidArgument)
	}
	if transaction == nil ||
		transaction.Statement == nil ||
		transaction.Statement.ConnPool == nil {
		return Authentication{}, fmt.Errorf(
			"%w: collector admission transaction is required",
			control.ErrInvalidArgument,
		)
	}
	if _, ok := transaction.Statement.ConnPool.(gorm.TxCommitter); !ok {
		return Authentication{}, fmt.Errorf(
			"%w: collector admission requires an active database transaction",
			control.ErrInvalidArgument,
		)
	}
	acceptedAt = databaseTime(acceptedAt)
	if acceptedAt.IsZero() {
		return Authentication{}, fmt.Errorf(
			"%w: collector token acceptance time is required",
			control.ErrInvalidArgument,
		)
	}

	transaction = transaction.WithContext(ctx)
	authentication, err := store.authenticate(
		transaction,
		plaintext,
		acceptedAt,
	)
	if err != nil {
		return Authentication{}, err
	}
	if err := recordCollectorTokenUse(
		transaction,
		authentication.TokenID,
		acceptedAt,
	); err != nil {
		return Authentication{}, err
	}
	return cloneAuthentication(authentication), nil
}

func cloneAuthentication(authentication Authentication) Authentication {
	authentication.AllowedIndexNames = append(
		[]string(nil),
		authentication.AllowedIndexNames...,
	)
	return authentication
}
