package control

import (
	"context"
	"crypto/rand"
	"database/sql"
	"errors"
	"fmt"
	"math/big"
	"time"

	"github.com/jackc/pgx/v5/stdlib"
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
// It is terminal: a transaction whose callback returns it is not retried.
var ErrTransactionRetriesExhausted = errors.New("control: transaction retries exhausted")

// ErrTransactionEndedByCallback means a transaction callback ended the
// transaction it was given, by calling Commit or Rollback on it or by running
// COMMIT, ROLLBACK, END, or ABORT as SQL. Statements after that ran outside
// the transaction, without its isolation or rollback, so the attempt is
// neither committed nor retried. Work the callback committed itself stays
// committed. It is a programming error, never contention.
var ErrTransactionEndedByCallback = errors.New("control: transaction callback ended its own transaction")

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
// rollback. Ending the transaction belongs to WriteTx: an attempt whose fn
// commits or rolls back tx itself fails with ErrTransactionEndedByCallback.
// A nested transaction that exhausted its own retries is returned without
// retrying this one.
func (db *PostgresDB) WriteTx(ctx context.Context, fn func(*sql.Tx) error) error {
	return db.runSQLTx(ctx, "write", postgresWriteTxOptions, fn)
}

// ReadTx runs fn in a READ ONLY, REPEATABLE READ transaction so every query in
// fn reads one consistent snapshot. Attempts to write fail inside PostgreSQL.
// The transaction is always rolled back after fn returns, and fn ending it
// early fails with ErrTransactionEndedByCallback as for WriteTx.
func (db *PostgresDB) ReadTx(ctx context.Context, fn func(*sql.Tx) error) error {
	return db.runSQLTx(ctx, "read", postgresReadTxOptions, fn)
}

// WriteGORMTx is WriteTx for stores built on GORM. tx is bound to ctx and to
// one SERIALIZABLE transaction on the shared pool.
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
	return retryPostgresTx(ctx, kind, func() error {
		return runPostgresTxAttempt(ctx, db.sql, options, fn)
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
	return retryPostgresTx(ctx, kind, func() error {
		return runPostgresTxAttempt(ctx, db.sql, options, func(tx *sql.Tx) error {
			return fn(bindGORMTx(ctx, db.orm, tx))
		})
	})
}

// bindGORMTx returns a fresh GORM session whose statements all run on tx,
// as gorm.DB.Begin binds one, so both transaction APIs share one attempt
// implementation and its checks.
func bindGORMTx(ctx context.Context, orm *gorm.DB, tx *sql.Tx) *gorm.DB {
	session := orm.Session(&gorm.Session{Context: ctx, NewDB: true})
	session.Statement.ConnPool = tx
	return session
}

func validatePostgresTx(ctx context.Context, db *PostgresDB, nilCallback bool) error {
	if ctx == nil || db == nil || db.sql == nil || db.orm == nil || nilCallback {
		return fmt.Errorf("%w: transaction context, database, and callback are required", ErrInvalidArgument)
	}
	return nil
}

// runPostgresTxAttempt runs one attempt on a dedicated pooled connection so it
// can read the server's transaction status after fn returns. database/sql
// cannot tell that fn ran COMMIT or ROLLBACK as SQL: it would report success
// for work that was rolled back, or retry work that was already committed.
func runPostgresTxAttempt(
	ctx context.Context,
	pool *sql.DB,
	options *sql.TxOptions,
	fn func(*sql.Tx) error,
) (err error) {
	conn, err := pool.Conn(ctx)
	if err != nil {
		return fmt.Errorf("begin PostgreSQL transaction: %w", err)
	}
	defer func() {
		if closeErr := conn.Close(); closeErr != nil && !errors.Is(closeErr, sql.ErrConnDone) {
			err = errors.Join(err, fmt.Errorf("release PostgreSQL connection: %w", closeErr))
		}
	}()
	tx, err := conn.BeginTx(ctx, options)
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
	fnErr := fn(tx)
	if callbackEndedPostgresTx(ctx, conn) {
		return errors.Join(ErrTransactionEndedByCallback, fnErr)
	}
	if fnErr != nil {
		return fnErr
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

// callbackEndedPostgresTx reports whether the session behind conn has left the
// transaction database/sql still holds open on it. database/sql also rolls a
// transaction back by itself once ctx ends; that is cancellation, not misuse.
func callbackEndedPostgresTx(ctx context.Context, conn *sql.Conn) bool {
	idle := false
	if err := conn.Raw(func(driverConn any) error {
		if stdConn, ok := driverConn.(*stdlib.Conn); ok {
			pgConn := stdConn.Conn().PgConn()
			idle = !pgConn.IsClosed() && pgConn.TxStatus() == 'I'
		}
		return nil
	}); err != nil {
		return false
	}
	return idle && ctx.Err() == nil
}

func retryPostgresTx(ctx context.Context, kind string, attempt func() error) error {
	delay := postgresTxRetryInitialDelay
	for attempts := 1; ; attempts++ {
		err := attempt()
		if !retryablePostgresAttempt(err) {
			return err
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

// retryablePostgresAttempt reports whether a failed attempt may run again. A
// nested transaction that exhausted its own bound must not be rerun by every
// enclosing one, which would multiply attempts per nesting level. A callback
// that ended its transaction ran later statements outside it, so rerunning it
// would repeat work that may already be committed.
func retryablePostgresAttempt(err error) bool {
	return err != nil &&
		IsRetryable(err) &&
		!errors.Is(err, ErrTransactionRetriesExhausted) &&
		!errors.Is(err, ErrTransactionEndedByCallback)
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
