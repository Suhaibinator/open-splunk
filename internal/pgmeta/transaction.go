// Package pgmeta provides native PostgreSQL metadata transaction boundaries.
package pgmeta

import (
	"context"
	"database/sql"
	"errors"
	"sort"
	"time"

	"gorm.io/gorm"
)

// Resource names identify business invariants, never the whole connection pool.
// Catalog publication has an instance-wide selector/revision invariant; visibility
// has one instance-wide committed prefix; retained storage has an instance budget.
const (
	Catalog         = "10:catalog-publication"
	Visibility      = "20:visibility-admission"
	RetainedStorage = "30:retained-storage-budget"
)

func options(input *sql.TxOptions) *sql.TxOptions {
	result := sql.TxOptions{}
	if input != nil {
		result = *input
	}
	if result.Isolation == sql.LevelDefault {
		if result.ReadOnly {
			result.Isolation = sql.LevelRepeatableRead
		} else {
			result.Isolation = sql.LevelReadCommitted
		}
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

	return tx, nil
}

func Begin(database *gorm.DB, input ...*sql.TxOptions) *gorm.DB {
	var requested *sql.TxOptions
	if len(input) > 0 {
		requested = input[0]
	}
	config := options(requested)
	tx := database.Begin(config)

	return tx
}

func Transaction(database *gorm.DB, operation func(*gorm.DB) error, input ...*sql.TxOptions) error {
	var requested *sql.TxOptions
	if len(input) > 0 {
		requested = input[0]
	}
	config := options(requested)
	return database.Transaction(operation, config)
}

// LockSQL joins an existing transaction. Acquire resource locks before reading
// the facts used by a mutation, in lexical order, then append audit/receipts last.
func LockSQL(ctx context.Context, tx *sql.Tx, resources ...string) error {
	resources = append([]string(nil), resources...)
	sort.Strings(resources)
	for _, resource := range resources {
		if _, err := tx.ExecContext(ctx, "SELECT pg_advisory_xact_lock(hashtextextended(current_database() || ':' || current_schema() || ':' || $1, 0))", resource); err != nil {
			return err
		}
	}
	return nil
}

func Lock(tx *gorm.DB, resources ...string) error {
	resources = append([]string(nil), resources...)
	sort.Strings(resources)
	for _, resource := range resources {
		if err := tx.Exec("SELECT pg_advisory_xact_lock(hashtextextended(current_database() || ':' || current_schema() || ':' || ?, 0))", resource).Error; err != nil {
			return err
		}
	}
	return nil
}

func BeginLocked(database *gorm.DB, resource string) *gorm.DB {
	tx := Begin(database)
	if tx.Error == nil {
		if err := Lock(tx, resource); err != nil {
			_ = tx.Rollback()
			tx.Error = err
		}
	}
	return tx
}

func BeginSQLLocked(ctx context.Context, database SQLBeginner, resource string) (*sql.Tx, error) {
	tx, err := BeginSQL(ctx, database, nil)
	if err != nil {
		return nil, err
	}
	if err = LockSQL(ctx, tx, resource); err != nil {
		_ = tx.Rollback()
		return nil, err
	}
	return tx, nil
}

// TransactionLocked retries only rolled-back PostgreSQL concurrency failures.
// The operation must contain database work only, with no external side effects.
func TransactionLocked(database *gorm.DB, resource string, operation func(*gorm.DB) error) error {
	var err error
	for attempt := range 4 {
		err = Transaction(database, func(tx *gorm.DB) error {
			if lockErr := Lock(tx, resource); lockErr != nil {
				return lockErr
			}
			return operation(tx)
		})
		var state interface{ SQLState() string }
		if !errors.As(err, &state) || (state.SQLState() != "40001" && state.SQLState() != "40P01") {
			return err
		}
		select {
		case <-database.Statement.Context.Done():
			return database.Statement.Context.Err()
		case <-time.After(time.Duration(attempt+1) * 5 * time.Millisecond):
		}
	}
	return err
}

// BeginCatalogReader fences changes to shared index/app definitions while
// allowing independent owners to mutate their own content concurrently.
func BeginCatalogReader(database *gorm.DB, resource string) *gorm.DB {
	tx := Begin(database)
	if tx.Error == nil {
		err := LockCatalogShared(tx)
		if err == nil {
			err = Lock(tx, resource)
		}
		if err != nil {
			_ = tx.Rollback()
			tx.Error = err
		}
	}
	return tx
}

// BeginIngestion freezes credential/catalog authority before the visibility
// prefix and pending/quota accounting boundary. Token mutations use the same
// order, so revocation cannot race a stale quota-admission decision.
func BeginIngestion(ctx context.Context, database SQLBeginner) (*sql.Tx, error) {
	tx, err := BeginSQL(ctx, database, nil)
	if err != nil {
		return nil, err
	}
	_, err = tx.ExecContext(ctx, "SELECT pg_advisory_xact_lock_shared(hashtextextended(current_database() || ':' || current_schema() || ':' || $1,0))", Catalog)
	if err == nil {
		err = LockSQL(ctx, tx, "15:ingestion-credentials", Visibility)
	}
	if err != nil {
		_ = tx.Rollback()
		return nil, err
	}
	return tx, nil
}

// BeginSQLCatalogReader excludes incompatible catalog edits while protecting a
// separate mutation authority. It shares the catalog fence with independent
// credential users and saved-content owners.
func BeginSQLCatalogReader(ctx context.Context, database SQLBeginner, resource string) (*sql.Tx, error) {
	tx, err := BeginSQL(ctx, database, nil)
	if err != nil {
		return nil, err
	}
	_, err = tx.ExecContext(ctx, "SELECT pg_advisory_xact_lock_shared(hashtextextended(current_database() || ':' || current_schema() || ':' || $1,0))", Catalog)
	if err == nil {
		err = LockSQL(ctx, tx, resource)
	}
	if err != nil {
		_ = tx.Rollback()
		return nil, err
	}
	return tx, nil
}

// LockCatalogShared joins the caller's native transaction and fences catalog
// definitions before it acquires a more specific mutation authority.
func LockCatalogShared(tx *gorm.DB) error {
	return tx.Exec("SELECT pg_advisory_xact_lock_shared(hashtextextended(current_database() || ':' || current_schema() || ':' || ?,0))", Catalog).Error
}
