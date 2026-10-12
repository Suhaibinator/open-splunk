// Package pgmeta provides native PostgreSQL metadata transaction boundaries.
package pgmeta

import (
	"context"
	"database/sql"

	"gorm.io/gorm"
)

// WriteLock serializes metadata decisions with their receipts, quotas and audit
// mutations. Schema statement triggers use the same lock for direct SQL writes.
const WriteLock int64 = 0x4f53504c4d455441

func options(input *sql.TxOptions) *sql.TxOptions {
	result := sql.TxOptions{}
	if input != nil {
		result = *input
	}
	if result.ReadOnly {
		result.Isolation = sql.LevelRepeatableRead
	} else {
		result.Isolation = sql.LevelReadCommitted
	}
	return &result
}

type SQLBeginner interface {
	BeginTx(context.Context, *sql.TxOptions) (*sql.Tx, error)
}

func BeginSQL(ctx context.Context, database SQLBeginner, input *sql.TxOptions) (*sql.Tx, error) {
	config := options(input)
	tx, err := database.BeginTx(ctx, config)
	if err != nil {
		return nil, err
	}
	if !config.ReadOnly {
		if _, err := tx.ExecContext(ctx, "SELECT pg_advisory_xact_lock(hashtextextended(current_database() || ':' || current_schema(), $1))", WriteLock); err != nil {
			_ = tx.Rollback()
			return nil, err
		}
	}
	return tx, nil
}

func Begin(database *gorm.DB, input ...*sql.TxOptions) *gorm.DB {
	var requested *sql.TxOptions
	if len(input) > 0 {
		requested = input[0]
	}
	config := options(requested)
	tx := database.Begin(config)
	if tx.Error == nil && !config.ReadOnly {
		if result := tx.Exec("SELECT pg_advisory_xact_lock(hashtextextended(current_database() || ':' || current_schema(), ?))", WriteLock); result.Error != nil {
			_ = tx.Rollback()
			tx.Error = result.Error
		}
	}
	return tx
}

func Transaction(database *gorm.DB, operation func(*gorm.DB) error, input ...*sql.TxOptions) error {
	var requested *sql.TxOptions
	if len(input) > 0 {
		requested = input[0]
	}
	config := options(requested)
	return database.Transaction(func(tx *gorm.DB) error {
		if !config.ReadOnly {
			if err := tx.Exec("SELECT pg_advisory_xact_lock(hashtextextended(current_database() || ':' || current_schema(), ?))", WriteLock).Error; err != nil {
				return err
			}
		}
		return operation(tx)
	}, config)
}
