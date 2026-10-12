package pgtest

import (
	"context"
	"errors"

	"gorm.io/gorm"
)

// ChecksGORM preserves the error-only shape used by isolated corruption
// fixtures while toggling native PostgreSQL constraints in their transaction.
func ChecksGORM(database *gorm.DB, enabled bool) *gorm.DB {
	if database == nil || database.Statement == nil {
		return &gorm.DB{Error: errors.New("GORM corruption fixture requires an open transaction")}
	}
	executor, ok := database.Statement.ConnPool.(SQLExecutor)
	if !ok {
		return &gorm.DB{Error: errors.New("GORM corruption fixture requires a native SQL executor")}
	}
	ctx := database.Statement.Context
	if ctx == nil {
		ctx = context.Background()
	}
	_, err := Checks(ctx, executor, enabled)
	return &gorm.DB{Error: err}
}
