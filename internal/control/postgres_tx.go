package control

import (
	"context"
	"crypto/rand"
	"database/sql"
	"errors"
	"fmt"
	"math/big"
	"sync/atomic"
	"time"

	"gorm.io/gorm"
)

const (
	// PostgresTxMaxAttempts bounds how many times WriteTx and ReadTx run their
	// callback when PostgreSQL aborts it with a retryable SQLSTATE. A caller
	// observing ErrTransactionRetriesExhausted saw exactly this many attempts.
	PostgresTxMaxAttempts = 8

	postgresTxRetryInitialDelay = 2 * time.Millisecond
	postgresTxRetryMaximumDelay = 250 * time.Millisecond
)

// ErrTransactionRetriesExhausted means every bounded attempt of a transaction
// was aborted by a serialization failure or deadlock. The final PostgreSQL
// error remains in the chain, so IsRetryable and IsContention still report it.
var ErrTransactionRetriesExhausted = errors.New("control: transaction retries exhausted")

var (
	postgresWriteTxOptions = &sql.TxOptions{Isolation: sql.LevelSerializable}
	// Read-only transactions use one snapshot for every statement. They never
	// take SIREAD locks, so a long report cannot abort concurrent writers.
	postgresReadTxOptions = &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true}
)

// WriteTx runs fn in a SERIALIZABLE transaction and commits it when fn returns
// nil. A serialization failure or deadlock, from fn or from COMMIT, rolls the
// attempt back and reruns fn from the beginning after a jittered backoff, at
// most PostgresTxMaxAttempts times. fn must therefore confine its side effects
// to tx and be safe to run again; any other error is returned unchanged after
// rollback.
//
// Write transactions of one handle run concurrently until one of them is
// aborted. From then until every aborted transaction has finished, retries
// and new write transactions alike run one at a time, as SQLite's single
// writer did; see postgresWriteQueue. fn must not start another write
// transaction on the same database, which that single writer never allowed
// either.
func (db *PostgresDB) WriteTx(ctx context.Context, fn func(*sql.Tx) error) error {
	return db.runSQLTx(ctx, "write", postgresWriteTxOptions, fn)
}

// ReadTx runs fn in a READ ONLY, REPEATABLE READ transaction so every query in
// fn reads one consistent snapshot. Attempts to write fail inside PostgreSQL.
// The transaction is always rolled back after fn returns.
func (db *PostgresDB) ReadTx(ctx context.Context, fn func(*sql.Tx) error) error {
	return db.runSQLTx(ctx, "read", postgresReadTxOptions, fn)
}

// WriteGORMTx is WriteTx for stores built on GORM. tx is bound to ctx and to
// one SERIALIZABLE transaction on the shared pool, and it shares WriteTx's
// write queue.
func (db *PostgresDB) WriteGORMTx(ctx context.Context, fn func(*gorm.DB) error) error {
	return db.runGORMTx(ctx, "write", postgresWriteTxOptions, fn)
}

// ReadGORMTx is ReadTx for stores built on GORM.
func (db *PostgresDB) ReadGORMTx(ctx context.Context, fn func(*gorm.DB) error) error {
	return db.runGORMTx(ctx, "read", postgresReadTxOptions, fn)
}

func (db *PostgresDB) runSQLTx(
	ctx context.Context,
	kind string,
	options *sql.TxOptions,
	fn func(*sql.Tx) error,
) error {
	if err := validatePostgresTx(ctx, db, fn == nil); err != nil {
		return err
	}
	return retryPostgresTx(ctx, kind, db.writeQueueFor(options), func() error {
		return runSQLTxAttempt(ctx, db.sql, options, fn)
	})
}

func (db *PostgresDB) runGORMTx(
	ctx context.Context,
	kind string,
	options *sql.TxOptions,
	fn func(*gorm.DB) error,
) error {
	if err := validatePostgresTx(ctx, db, fn == nil); err != nil {
		return err
	}
	return retryPostgresTx(ctx, kind, db.writeQueueFor(options), func() error {
		return runGORMTxAttempt(ctx, db.orm, options, fn)
	})
}

func validatePostgresTx(ctx context.Context, db *PostgresDB, nilCallback bool) error {
	if ctx == nil || db == nil || db.sql == nil || db.orm == nil || nilCallback {
		return fmt.Errorf("%w: transaction context, database, and callback are required", ErrInvalidArgument)
	}
	return nil
}

// writeQueueFor returns the queue that serializes retried writes, or nil for
// a read-only transaction.
func (db *PostgresDB) writeQueueFor(options *sql.TxOptions) *postgresWriteQueue {
	if options.ReadOnly {
		return nil
	}
	return db.writeQueue
}

func runSQLTxAttempt(
	ctx context.Context,
	db *sql.DB,
	options *sql.TxOptions,
	fn func(*sql.Tx) error,
) (err error) {
	tx, err := db.BeginTx(ctx, options)
	if err != nil {
		return fmt.Errorf("begin PostgreSQL transaction: %w", err)
	}
	finished := false
	defer func() {
		// The deferred rollback also runs while a panic from fn unwinds, so
		// the connection returns to the pool without an open transaction.
		if finished {
			return
		}
		if rollbackErr := tx.Rollback(); rollbackErr != nil && !errors.Is(rollbackErr, sql.ErrTxDone) {
			err = errors.Join(err, fmt.Errorf("roll back PostgreSQL transaction: %w", rollbackErr))
		}
	}()
	if err := fn(tx); err != nil {
		return err
	}
	if options.ReadOnly {
		return nil
	}
	finished = true
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit PostgreSQL transaction: %w", err)
	}
	return nil
}

func runGORMTxAttempt(
	ctx context.Context,
	db *gorm.DB,
	options *sql.TxOptions,
	fn func(*gorm.DB) error,
) (err error) {
	tx := db.WithContext(ctx).Begin(options)
	if tx.Error != nil {
		return fmt.Errorf("begin PostgreSQL transaction: %w", tx.Error)
	}
	finished := false
	defer func() {
		if finished {
			return
		}
		if rollbackErr := tx.Rollback().Error; rollbackErr != nil && !errors.Is(rollbackErr, sql.ErrTxDone) {
			err = errors.Join(err, fmt.Errorf("roll back PostgreSQL transaction: %w", rollbackErr))
		}
	}()
	if err := fn(tx); err != nil {
		return err
	}
	if options.ReadOnly {
		return nil
	}
	finished = true
	if err := tx.Commit().Error; err != nil {
		return fmt.Errorf("commit PostgreSQL transaction: %w", err)
	}
	return nil
}

// postgresWriteQueue makes the write transactions of one handle run one at a
// time while any of them is being retried.
//
// A SERIALIZABLE snapshot is taken when a transaction's first statement
// starts, before any row-lock wait. Writers that meet on one hot row (a
// tenant's journal state, a revision authority, a global singleton such as
// ingest_visibility_state or index_catalog_state) therefore do not simply
// queue on its lock: every waiter aborts with 40001 once per commit that
// happened while it waited, so sixteen concurrent writers exhaust
// PostgresTxMaxAttempts. Once a transaction has been aborted, it and every
// write transaction that starts while it is still unfinished hold the slot
// for each attempt. Those writers run one after another, each taking its
// snapshot after the previous one committed, exactly like SQLite's single
// writer. With no transaction in retry, writes run concurrently and pay one
// atomic load.
type postgresWriteQueue struct {
	slot     chan struct{}
	retrying atomic.Int64
}

func newPostgresWriteQueue() *postgresWriteQueue {
	return &postgresWriteQueue{slot: make(chan struct{}, 1)}
}

// errPostgresWriteQueueWait reports that the context ended while an attempt
// waited for the write queue. It never leaves retryPostgresTx.
var errPostgresWriteQueueWait = errors.New("control: context ended waiting for the PostgreSQL write queue")

// run runs attempt, holding the slot when this transaction is retrying or
// any other transaction of the handle is. A nil queue never serializes.
func (queue *postgresWriteQueue) run(ctx context.Context, retrying bool, attempt func() error) error {
	if queue == nil || (!retrying && queue.retrying.Load() == 0) {
		return attempt()
	}
	select {
	case queue.slot <- struct{}{}:
	case <-ctx.Done():
		return errPostgresWriteQueueWait
	}
	defer func() { <-queue.slot }()
	return attempt()
}

// retryPostgresTx runs attempt until it succeeds, fails with an error that is
// not retryable, or has run PostgresTxMaxAttempts times. From its first
// retryable failure until it returns, the transaction counts as retrying in
// queue (nil for read transactions, which never serialize).
func retryPostgresTx(ctx context.Context, kind string, queue *postgresWriteQueue, attempt func() error) error {
	delay := postgresTxRetryInitialDelay
	var previous error
	retrying := false
	defer func() {
		if retrying {
			queue.retrying.Add(-1)
		}
	}()
	for attempts := 1; ; attempts++ {
		err := queue.run(ctx, retrying, attempt)
		if errors.Is(err, errPostgresWriteQueueWait) {
			return errors.Join(ctx.Err(), previous)
		}
		if err == nil || !IsRetryable(err) {
			return err
		}
		previous = err
		if !retrying && queue != nil {
			retrying = true
			queue.retrying.Add(1)
		}
		if attempts >= PostgresTxMaxAttempts {
			return fmt.Errorf(
				"%w: %s transaction aborted %d times: %w",
				ErrTransactionRetriesExhausted,
				kind,
				attempts,
				err,
			)
		}
		if ctxErr := ctx.Err(); ctxErr != nil {
			return errors.Join(ctxErr, err)
		}
		timer := time.NewTimer(jitteredPostgresRetryDelay(delay))
		select {
		case <-ctx.Done():
			timer.Stop()
			return errors.Join(ctx.Err(), err)
		case <-timer.C:
		}
		delay = min(delay*2, postgresTxRetryMaximumDelay)
	}
}

// jitteredPostgresRetryDelay picks uniformly from [delay/2, delay] so writers
// that collided once do not retry in lockstep and collide again.
func jitteredPostgresRetryDelay(delay time.Duration) time.Duration {
	half := delay / 2
	if half <= 0 {
		return delay
	}
	offset, err := rand.Int(rand.Reader, big.NewInt(int64(half)+1))
	if err != nil {
		return delay
	}
	return half + time.Duration(offset.Int64())
}
