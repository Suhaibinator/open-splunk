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

	"github.com/jackc/pgx/v5/stdlib"
	"gorm.io/gorm"
)

const (
	// PostgresTxMaxAttempts bounds how many times WriteTx, ReadTx and
	// RetryWrite run their callback when PostgreSQL aborts it with a
	// retryable SQLSTATE. A caller
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
	// Read transactions are SERIALIZABLE too. Beside SERIALIZABLE writers, a
	// REPEATABLE READ snapshot can show a state that no serial order of the
	// committed writes produces (the read-only anomaly): a report could see
	// a deposit and not the earlier-serialized withdrawal it should have
	// seen first. SQLite's single writer never showed such a state. A
	// SERIALIZABLE READ ONLY transaction that commits takes part in conflict
	// detection, so PostgreSQL aborts whichever transaction would complete
	// the anomaly, and the retry loop reruns it; see finishPostgresReadTx
	// for why ReadTx commits. PostgreSQL also drops a read-only
	// transaction's SIREAD locks as soon as its snapshot is provably safe.
	// DEFERRABLE is not used: it waits for every concurrent serializable
	// writer to finish first, which a ReadTx inside a WriteTx callback on the
	// same goroutine would wait for forever.
	postgresReadTxOptions = &sql.TxOptions{Isolation: sql.LevelSerializable, ReadOnly: true}
	// postgresSnapshotReadOptions read one snapshot without taking part in
	// serialization; migration verification reads only the ledger, which
	// migrations alone write under their own lock.
	postgresSnapshotReadOptions = &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true}
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
//
// Write transactions of one handle run concurrently until one of them is
// aborted. From then until every aborted transaction has finished, retries
// and new write transactions alike run one at a time, as SQLite's single
// writer did; see postgresWriteQueue. fn must not start another write
// transaction on the same database, which that single writer never allowed
// either.
//
// On a handle that holds the server singleton, WriteTx commits only while
// the singleton's session still holds its lock, and fails with
// ErrServerSingletonLost once it does not; see AcquireServerSingleton.
func (db *PostgresDB) WriteTx(ctx context.Context, fn func(*sql.Tx) error) error {
	return db.runSQLTx(ctx, "write", postgresWriteTxOptions, fn)
}

// ReadTx runs fn in a READ ONLY, SERIALIZABLE transaction so every query in
// fn reads one consistent snapshot that some serial order of the committed
// write transactions produces. Attempts to write fail inside PostgreSQL.
// A serialization failure, from fn or from COMMIT, reruns fn after a backoff
// as for WriteTx, so fn must be safe to run again and must not trust what it
// read until ReadTx returns nil. When fn returns nil the transaction
// commits, which writes nothing but is what confirms the reads were
// serializable; a transaction that wrote anyway (a temporary table, or after
// SET TRANSACTION READ WRITE) is rolled back instead. fn ending the
// transaction early fails with ErrTransactionEndedByCallback as for WriteTx.
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

// WriteGORMTxOn is WriteGORMTx with tx derived from session rather than from
// GORMDB, so settings the caller gave session, such as a statement-counting
// logger, apply inside the transaction. session must come from db.GORMDB()
// through Session, WithContext or similar, outside any transaction; another
// pool or an open transaction is refused with ErrInvalidArgument.
func (db *PostgresDB) WriteGORMTxOn(ctx context.Context, session *gorm.DB, fn func(*gorm.DB) error) error {
	if err := db.validateGORMSession(session); err != nil {
		return err
	}
	return db.runGORMTxOn(ctx, "write", postgresWriteTxOptions, session, fn)
}

// ReadGORMTxOn is ReadGORMTx with tx derived from session, as for
// WriteGORMTxOn.
func (db *PostgresDB) ReadGORMTxOn(ctx context.Context, session *gorm.DB, fn func(*gorm.DB) error) error {
	if err := db.validateGORMSession(session); err != nil {
		return err
	}
	return db.runGORMTxOn(ctx, "read", postgresReadTxOptions, session, fn)
}

// RetryWrite runs attempt under WriteTx's retry policy for callers that must
// begin and end their own transaction, such as a route that holds a LOCK
// TABLE seam across helpers. Each call of attempt must begin a fresh
// SERIALIZABLE transaction on db, run all of its work in it, and commit or
// roll it back before returning; attempt reruns from the beginning after a
// serialization failure or deadlock, at most PostgresTxMaxAttempts times, so
// its side effects outside that transaction must be safe to repeat.
//
// Attempts join the handle's write queue exactly as WriteTx attempts do: once
// any write of this handle is retrying, these attempts run one at a time with
// every other write, so hot-row writers that bypass WriteTx still converge.
// attempt must not itself call WriteTx or RetryWrite on db. Like WriteTx,
// RetryWrite refuses to run once the handle's server singleton was lost.
// Because attempt commits its own transaction, RetryWrite cannot confirm the
// singleton before that COMMIT as WriteTx does: attempt runs
// ConfirmServerSingleton on its transaction just before committing, and
// returns its error after rolling back.
func (db *PostgresDB) RetryWrite(ctx context.Context, attempt func() error) error {
	if err := validatePostgresTx(ctx, db, attempt == nil); err != nil {
		return err
	}
	return retryPostgresTx(ctx, "write", db.writeQueue, func() error {
		if err := db.ServerSingletonErr(); err != nil {
			return err
		}
		return attempt()
	})
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
		return db.runPostgresTxAttempt(ctx, options, fn)
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
	return db.runGORMTxOn(ctx, kind, options, db.orm, fn)
}

func (db *PostgresDB) runGORMTxOn(
	ctx context.Context,
	kind string,
	options *sql.TxOptions,
	session *gorm.DB,
	fn func(*gorm.DB) error,
) error {
	if err := validatePostgresTx(ctx, db, fn == nil); err != nil {
		return err
	}
	return retryPostgresTx(ctx, kind, db.writeQueueFor(options), func() error {
		return db.runPostgresTxAttempt(ctx, options, func(tx *sql.Tx) error {
			return fn(bindGORMTx(ctx, session, tx))
		})
	})
}

// validateGORMSession refuses a session that would not run on db's pool:
// one from another handle, or one already bound to a transaction.
func (db *PostgresDB) validateGORMSession(session *gorm.DB) error {
	if db == nil || session == nil || session.Statement == nil || session.Statement.ConnPool == nil {
		return fmt.Errorf("%w: transaction database and GORM session are required", ErrInvalidArgument)
	}
	if pool, ok := session.Statement.ConnPool.(*sql.DB); !ok || pool != db.sql {
		return fmt.Errorf(
			"%w: GORM session must come from this handle's GORMDB outside a transaction",
			ErrInvalidArgument,
		)
	}
	return nil
}

// bindGORMTx returns a fresh GORM session whose statements all run on tx,
// as gorm.DB.Begin binds one, so both transaction APIs share one attempt
// implementation and its checks. The session keeps orm's configuration,
// such as its logger.
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

// writeQueueFor returns the queue that serializes retried writes, or nil for
// a read-only transaction.
func (db *PostgresDB) writeQueueFor(options *sql.TxOptions) *postgresWriteQueue {
	if options.ReadOnly {
		return nil
	}
	return db.writeQueue
}

// runPostgresTxAttempt runs one attempt on a dedicated pooled connection so it
// can read the server's transaction status after fn returns. database/sql
// cannot tell that fn ran COMMIT or ROLLBACK as SQL: it would report success
// for work that was rolled back, or retry work that was already committed.
//
// A write attempt on a handle fenced by a server singleton confirms the lock
// is still held as its last statement before COMMIT, and a handle that has
// lost its singleton fails before running fn; see AcquireServerSingleton.
func (db *PostgresDB) runPostgresTxAttempt(
	ctx context.Context,
	options *sql.TxOptions,
	fn func(*sql.Tx) error,
) (err error) {
	if !options.ReadOnly {
		if err := db.serverSingletonLostErr(); err != nil {
			return err
		}
	}
	conn, err := db.sql.Conn(ctx)
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
		return finishPostgresReadTx(ctx, tx, &finished)
	}
	if err := db.checkServerSingletonBeforeCommit(ctx, tx); err != nil {
		return err
	}
	finished = true
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit PostgreSQL transaction: %w", err)
	}
	return nil
}

// finishPostgresReadTx commits a read transaction that changed nothing and
// leaves any other one to the caller's deferred rollback. Committing is what
// makes a SERIALIZABLE read count: PostgreSQL forgets the read dependencies
// of a transaction that rolls back, so a rolled-back report could still have
// seen a state no serial order produces, while a committed one either was
// consistent or fails here, or makes the conflicting writer fail, with a
// retryable 40001. A transaction with an assigned transaction ID wrote
// something, such as a temporary table row or a row written after the
// callback ran SET TRANSACTION READ WRITE, and is never committed; ReadTx
// still reports success for it, as it always discarded such writes.
func finishPostgresReadTx(ctx context.Context, tx *sql.Tx, finished *bool) error {
	var wrote bool
	if err := tx.QueryRowContext(
		ctx,
		`SELECT pg_catalog.pg_current_xact_id_if_assigned() IS NOT NULL`,
	).Scan(&wrote); err != nil {
		return fmt.Errorf("inspect PostgreSQL read transaction: %w", err)
	}
	if wrote {
		return nil
	}
	*finished = true
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit PostgreSQL read transaction: %w", err)
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
		if !retryablePostgresAttempt(err) {
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
