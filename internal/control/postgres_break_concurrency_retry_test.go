package control_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/fstest"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/stdlib"
	"gorm.io/gorm"

	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/testsupport"
)

// These tests try to break the transaction entry points of the PostgreSQL
// control plane: forced and real 40001/40P01 aborts (including inside
// COMMIT), retry exhaustion, context cancellation at every stage, callbacks
// with side effects outside the transaction, read-only enforcement, and
// callbacks that misuse or nest transactions.

const breakRetryFixturePath = "testdata/postgres_break_concurrency_retry/0002_break_concurrency_retry.sql"

func newBreakRetryDB(t *testing.T) *control.PostgresDB {
	t.Helper()
	extra, err := os.ReadFile(breakRetryFixturePath)
	if err != nil {
		t.Fatalf("read break fixture: %v", err)
	}
	migrationFS := postgresFixtureMigrations()
	migrationFS["0002_break_concurrency_retry.sql"] = &fstest.MapFile{Data: extra}
	return testsupport.NewPostgresControlDBWithMigrations(t, migrationFS)
}

// pinBreakRetryPool limits the shared pool to one physical connection, so the
// next statement observes exactly the session a previous transaction left.
func pinBreakRetryPool(db *control.PostgresDB) {
	db.SQLDB().SetMaxOpenConns(1)
	db.SQLDB().SetMaxIdleConns(1)
}

func breakCount(t *testing.T, db *control.PostgresDB, query string) int {
	t.Helper()
	var count int
	if err := db.SQLDB().QueryRowContext(t.Context(), query).Scan(&count); err != nil {
		t.Fatalf("count %q: %v", query, err)
	}
	return count
}

func breakLedgerNotes(t *testing.T, db *control.PostgresDB) []string {
	t.Helper()
	rows, err := db.SQLDB().QueryContext(t.Context(), `SELECT note FROM break_ledger ORDER BY id`)
	if err != nil {
		t.Fatalf("read ledger: %v", err)
	}
	defer func() { _ = rows.Close() }()
	var notes []string
	for rows.Next() {
		var note string
		if err := rows.Scan(&note); err != nil {
			t.Fatalf("scan ledger: %v", err)
		}
		notes = append(notes, note)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate ledger: %v", err)
	}
	return notes
}

func setTxLocal(ctx context.Context, tx *sql.Tx, name, value string) error {
	_, err := tx.ExecContext(ctx, `SELECT set_config($1, $2, true)`, name, value)
	return err
}

func syntheticAbort(code string) error {
	return &pgconn.PgError{Severity: "ERROR", Code: code, Message: "synthetic abort"}
}

// waitForQuietBackends waits until no other backend of this database is
// running or blocked, so a canceled client's server-side work has finished.
func waitForQuietBackends(t *testing.T, db *control.PostgresDB) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		busy := breakCount(t, db, `SELECT count(*) FROM pg_stat_activity
			WHERE datname = current_database() AND pid <> pg_backend_pid() AND state <> 'idle'`)
		if busy == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%d backends still busy after the client gave up", busy)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestBreakRetryCommitTimeAbortsAreRetried(t *testing.T) {
	t.Parallel()
	for _, code := range []string{"40001", "40P01"} {
		t.Run(code, func(t *testing.T) {
			t.Parallel()
			db := newBreakRetryDB(t)
			ctx := t.Context()

			var attempts atomic.Int32
			err := db.WriteTx(ctx, func(tx *sql.Tx) error {
				attempt := attempts.Add(1)
				if _, err := tx.ExecContext(ctx, `INSERT INTO break_ledger (note) VALUES ($1)`,
					fmt.Sprintf("attempt-%d", attempt)); err != nil {
					return err
				}
				if attempt == 1 {
					// The deferred gate raises only inside COMMIT, after the
					// callback has already returned nil.
					return setTxLocal(ctx, tx, "break.commit_sqlstate", code)
				}
				return nil
			})
			if err != nil || attempts.Load() != 2 {
				t.Fatalf("WriteTx() = %v after %d attempts, want nil after 2", err, attempts.Load())
			}
			if notes := breakLedgerNotes(t, db); len(notes) != 1 || notes[0] != "attempt-2" {
				t.Fatalf("ledger = %v, want only the retried attempt", notes)
			}
		})
	}
}

func TestBreakRetryCommitTimeAbortsExhaustTheBound(t *testing.T) {
	t.Parallel()
	db := newBreakRetryDB(t)
	ctx := t.Context()

	var attempts atomic.Int32
	err := db.WriteTx(ctx, func(tx *sql.Tx) error {
		attempts.Add(1)
		if _, err := tx.ExecContext(ctx, `INSERT INTO break_ledger (note) VALUES ('never')`); err != nil {
			return err
		}
		return setTxLocal(ctx, tx, "break.commit_sqlstate", "40001")
	})
	if attempts.Load() != control.PostgresTxMaxAttempts || !errors.Is(err, control.ErrTransactionRetriesExhausted) {
		t.Fatalf("WriteTx() = %v after %d attempts, want exhaustion after %d",
			err, attempts.Load(), control.PostgresTxMaxAttempts)
	}
	if !strings.Contains(err.Error(), "commit PostgreSQL transaction") ||
		!strings.Contains(err.Error(), fmt.Sprintf("write transaction aborted %d times", control.PostgresTxMaxAttempts)) {
		t.Fatalf("exhausted commit error does not say where it failed: %v", err)
	}
	if code, ok := control.PostgresErrorCode(err); !ok || code != "40001" || !control.IsContention(err) {
		t.Fatalf("exhausted commit error lost its SQLSTATE: code=%q ok=%v", code, ok)
	}

	attempts.Store(0)
	err = db.WriteGORMTx(ctx, func(tx *gorm.DB) error {
		attempts.Add(1)
		if err := tx.Exec(`INSERT INTO break_ledger (note) VALUES ('never-gorm')`).Error; err != nil {
			return err
		}
		return tx.Exec(`SELECT set_config('break.commit_sqlstate', '40P01', true)`).Error
	})
	if attempts.Load() != control.PostgresTxMaxAttempts || !errors.Is(err, control.ErrTransactionRetriesExhausted) {
		t.Fatalf("WriteGORMTx() = %v after %d attempts, want exhaustion", err, attempts.Load())
	}
	if code, _ := control.PostgresErrorCode(err); code != "40P01" {
		t.Fatalf("WriteGORMTx() exhaustion code = %q, want 40P01", code)
	}
	if got := breakCount(t, db, `SELECT count(*) FROM break_ledger`); got != 0 {
		t.Fatalf("ledger rows = %d after exhausted commits, want 0", got)
	}
}

func TestBreakRetryRealDeadlockVictimIsRetried(t *testing.T) {
	t.Parallel()
	db := newBreakRetryDB(t)
	ctx := t.Context()

	type transfer struct{ from, to, amount int64 }
	transfers := [2]transfer{{from: 1, to: 2, amount: 10}, {from: 2, to: 1, amount: 7}}
	lockedFirst := [2]chan struct{}{make(chan struct{}), make(chan struct{})}
	var attempts, deadlocks atomic.Int32
	var errs [2]error
	var wg sync.WaitGroup
	for i, move := range transfers {
		wg.Go(func() {
			own := 0
			errs[i] = db.WriteTx(ctx, func(tx *sql.Tx) error {
				own++
				attempts.Add(1)
				if _, err := tx.ExecContext(ctx, `SET LOCAL deadlock_timeout = '20ms'`); err != nil {
					return err
				}
				if _, err := tx.ExecContext(ctx,
					`UPDATE break_accounts SET balance = balance - $1 WHERE id = $2`, move.amount, move.from); err != nil {
					return err
				}
				if own == 1 {
					// Both writers hold their first row before either asks
					// for the other's, so PostgreSQL must break a real cycle.
					close(lockedFirst[i])
					select {
					case <-lockedFirst[1-i]:
					case <-time.After(10 * time.Second):
						return errors.New("peer never locked its first row")
					}
				}
				_, err := tx.ExecContext(ctx,
					`UPDATE break_accounts SET balance = balance + $1 WHERE id = $2`, move.amount, move.to)
				if code, _ := control.PostgresErrorCode(err); code == "40P01" {
					deadlocks.Add(1)
				}
				return err
			})
		})
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("transfer %d WriteTx() = %v", i, err)
		}
	}
	if deadlocks.Load() != 1 || attempts.Load() < 3 {
		t.Fatalf("deadlocks = %d, attempts = %d: want one detected deadlock and a retried victim",
			deadlocks.Load(), attempts.Load())
	}
	balances := breakCount(t, db, `SELECT count(*) FROM break_accounts
		WHERE (id = 1 AND balance = 97) OR (id = 2 AND balance = 103)`)
	if balances != 2 {
		t.Fatal("balances after the deadlocked transfers do not reflect both transfers exactly once")
	}
}

func TestBreakRetryWriteSkewIsSerializedByRetry(t *testing.T) {
	t.Parallel()
	db := newBreakRetryDB(t)
	ctx := t.Context()

	doctors := [2]string{"alice", "bob"}
	counted := [2]chan struct{}{make(chan struct{}), make(chan struct{})}
	var attempts atomic.Int32
	var errs [2]error
	var wg sync.WaitGroup
	for i, doctor := range doctors {
		wg.Go(func() {
			own := 0
			errs[i] = db.WriteTx(ctx, func(tx *sql.Tx) error {
				own++
				attempts.Add(1)
				var onCall int
				if err := tx.QueryRowContext(ctx,
					`SELECT count(*) FROM break_on_call WHERE on_call`).Scan(&onCall); err != nil {
					return err
				}
				if own == 1 {
					close(counted[i])
					select {
					case <-counted[1-i]:
					case <-time.After(10 * time.Second):
						return errors.New("peer never counted")
					}
				}
				if onCall < 2 {
					return nil
				}
				_, err := tx.ExecContext(ctx, `UPDATE break_on_call SET on_call = false WHERE doctor = $1`, doctor)
				return err
			})
		})
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("doctor %d WriteTx() = %v", i, err)
		}
	}
	if got := breakCount(t, db, `SELECT count(*) FROM break_on_call WHERE on_call`); got != 1 {
		t.Fatalf("doctors on call = %d after concurrent sign-offs, want exactly 1 (write skew)", got)
	}
	if attempts.Load() < 3 {
		t.Fatalf("attempts = %d: the write-skew conflict was never retried", attempts.Load())
	}
}

func TestBreakRetryExhaustionIsTerminalForEnclosingTransactions(t *testing.T) {
	t.Parallel()
	db := newBreakRetryDB(t)

	var inner atomic.Int32
	innerTx := func(ctx context.Context) error {
		return db.WriteTx(ctx, func(tx *sql.Tx) error {
			inner.Add(1)
			return forceSQLState(ctx, tx, "40001")
		})
	}
	enclosing := []struct {
		name string
		run  func(context.Context, *atomic.Int32) error
	}{
		{"WriteTx", func(ctx context.Context, outer *atomic.Int32) error {
			return db.WriteTx(ctx, func(*sql.Tx) error { outer.Add(1); return innerTx(ctx) })
		}},
		{"ReadTx", func(ctx context.Context, outer *atomic.Int32) error {
			return db.ReadTx(ctx, func(*sql.Tx) error { outer.Add(1); return innerTx(ctx) })
		}},
		{"WriteGORMTx", func(ctx context.Context, outer *atomic.Int32) error {
			return db.WriteGORMTx(ctx, func(*gorm.DB) error { outer.Add(1); return innerTx(ctx) })
		}},
	}
	for _, scenario := range enclosing {
		t.Run(scenario.name, func(t *testing.T) {
			inner.Store(0)
			var outer atomic.Int32
			err := scenario.run(t.Context(), &outer)
			if !errors.Is(err, control.ErrTransactionRetriesExhausted) || !control.IsRetryable(err) {
				t.Fatalf("enclosing %s = %v, want the inner exhaustion with its SQLSTATE", scenario.name, err)
			}
			// An inner transaction already spent its whole bound; retrying
			// the enclosing one would multiply attempts (8 x 8 = 64).
			if outer.Load() != 1 || inner.Load() != control.PostgresTxMaxAttempts {
				t.Fatalf("enclosing %s ran outer=%d inner=%d attempts, want 1 and %d",
					scenario.name, outer.Load(), inner.Load(), control.PostgresTxMaxAttempts)
			}
		})
	}
}

func TestBreakRetryExhaustedErrorKeepsItsChain(t *testing.T) {
	t.Parallel()
	db := newBreakRetryDB(t)
	ctx := t.Context()

	var attempts atomic.Int32
	err := db.ReadTx(ctx, func(tx *sql.Tx) error {
		attempts.Add(1)
		return forceSQLState(ctx, tx, "40P01")
	})
	if !errors.Is(err, control.ErrTransactionRetriesExhausted) ||
		!strings.Contains(err.Error(), fmt.Sprintf("read transaction aborted %d times", control.PostgresTxMaxAttempts)) {
		t.Fatalf("ReadTx() exhaustion = %v", err)
	}
	if code, _ := control.PostgresErrorCode(err); code != "40P01" || attempts.Load() != control.PostgresTxMaxAttempts {
		t.Fatalf("ReadTx() exhaustion code = %q after %d attempts", code, attempts.Load())
	}

	domain := errors.New("domain conflict")
	attempts.Store(0)
	err = db.WriteTx(ctx, func(*sql.Tx) error {
		attempts.Add(1)
		return errors.Join(domain, syntheticAbort("40001"))
	})
	if !errors.Is(err, domain) || !errors.Is(err, control.ErrTransactionRetriesExhausted) ||
		attempts.Load() != control.PostgresTxMaxAttempts {
		t.Fatalf("joined domain abort = %v after %d attempts, want both in the chain after %d",
			err, attempts.Load(), control.PostgresTxMaxAttempts)
	}

	// Flattening the PostgreSQL error to text (as %v does) leaves nothing to
	// classify, so the abort must not be retried.
	attempts.Store(0)
	err = db.WriteTx(ctx, func(*sql.Tx) error {
		attempts.Add(1)
		return errors.New("lookup failed: " + syntheticAbort("40001").Error())
	})
	if attempts.Load() != 1 || control.IsRetryable(err) || errors.Is(err, control.ErrTransactionRetriesExhausted) {
		t.Fatalf("flattened abort = %v after %d attempts, want one unretried attempt", err, attempts.Load())
	}

	// Lock-not-available is contention but not a transaction abort, so the
	// caller decides; WriteTx must not retry it.
	attempts.Store(0)
	err = db.WriteTx(ctx, func(tx *sql.Tx) error {
		attempts.Add(1)
		return forceSQLState(ctx, tx, "55P03")
	})
	if attempts.Load() != 1 || !control.IsContention(err) || control.IsRetryable(err) {
		t.Fatalf("lock_not_available = %v after %d attempts, want one attempt", err, attempts.Load())
	}
}

func TestBreakRetryCancellationDuringBackoffStopsRetrying(t *testing.T) {
	t.Parallel()
	db := newBreakRetryDB(t)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	var attempts atomic.Int32
	started := time.Now()
	err := db.WriteTx(ctx, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx, `INSERT INTO break_ledger (note) VALUES ('backoff')`); err != nil {
			return err
		}
		if attempts.Add(1) == 3 {
			// The third backoff sleeps at least 4ms; cancel inside it.
			time.AfterFunc(time.Millisecond, cancel)
		}
		return syntheticAbort("40001")
	})
	if !errors.Is(err, context.Canceled) || errors.Is(err, control.ErrTransactionRetriesExhausted) {
		t.Fatalf("WriteTx() = %v, want cancellation rather than exhaustion", err)
	}
	if got := attempts.Load(); got < 3 || got > 4 {
		t.Fatalf("WriteTx() ran %d attempts after cancellation at the third backoff", got)
	}
	if elapsed := time.Since(started); elapsed > 5*time.Second {
		t.Fatalf("WriteTx() took %v to notice cancellation", elapsed)
	}
	if got := breakCount(t, db, `SELECT count(*) FROM break_ledger`); got != 0 {
		t.Fatalf("ledger rows = %d after canceled retries, want 0", got)
	}
}

func TestBreakRetryCanceledWhileBlockedOnALock(t *testing.T) {
	t.Parallel()
	db := newBreakRetryDB(t)
	ctx := t.Context()

	holder, err := db.SQLDB().BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin lock holder: %v", err)
	}
	if _, err := holder.ExecContext(ctx, `UPDATE break_accounts SET balance = balance WHERE id = 1`); err != nil {
		t.Fatalf("lock holder update: %v", err)
	}

	waitCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	time.AfterFunc(150*time.Millisecond, cancel)
	var attempts atomic.Int32
	started := time.Now()
	err = db.WriteTx(waitCtx, func(tx *sql.Tx) error {
		attempts.Add(1)
		_, err := tx.ExecContext(waitCtx, `UPDATE break_accounts SET balance = balance + 1000 WHERE id = 1`)
		return err
	})
	if !errors.Is(err, context.Canceled) || attempts.Load() != 1 {
		t.Fatalf("blocked WriteTx() = %v after %d attempts, want cancellation after 1", err, attempts.Load())
	}
	if elapsed := time.Since(started); elapsed > 5*time.Second {
		t.Fatalf("blocked WriteTx() took %v to honor cancellation", elapsed)
	}
	if err := holder.Rollback(); err != nil {
		t.Fatalf("roll back lock holder: %v", err)
	}
	waitForQuietBackends(t, db)

	if err := db.WriteTx(ctx, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `UPDATE break_accounts SET balance = balance + 1 WHERE id = 1`)
		return err
	}); err != nil {
		t.Fatalf("WriteTx() after the canceled waiter = %v", err)
	}
	if got := breakCount(t, db, `SELECT balance FROM break_accounts WHERE id = 1`); got != 101 {
		t.Fatalf("balance = %d, want 101: the canceled waiter's update must never land", got)
	}
}

func TestBreakRetryCanceledDuringCommitIsNeverRetried(t *testing.T) {
	t.Parallel()
	db := newBreakRetryDB(t)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	var attempts atomic.Int32
	var callbackDone atomic.Bool
	err := db.WriteTx(ctx, func(tx *sql.Tx) error {
		attempts.Add(1)
		if _, err := tx.ExecContext(ctx, `INSERT INTO break_ledger (note) VALUES ('slow-commit')`); err != nil {
			return err
		}
		if err := setTxLocal(ctx, tx, "break.commit_sleep", "0.4"); err != nil {
			return err
		}
		callbackDone.Store(true)
		// COMMIT stalls in the deferred gate; cancel while it is in flight.
		time.AfterFunc(100*time.Millisecond, cancel)
		return nil
	})
	if !callbackDone.Load() || attempts.Load() != 1 {
		t.Fatalf("WriteTx() ran %d attempts (callback finished=%v), want exactly 1", attempts.Load(), callbackDone.Load())
	}
	if !errors.Is(err, context.Canceled) || control.IsRetryable(err) {
		t.Fatalf("WriteTx() canceled during COMMIT = %v, want an unretried cancellation", err)
	}
	waitForQuietBackends(t, db)
	// The outcome of an interrupted COMMIT is unknown to the client; what
	// must never happen is a second application of the same write.
	if got := breakCount(t, db, `SELECT count(*) FROM break_ledger`); got > 1 {
		t.Fatalf("ledger rows = %d after a canceled COMMIT, want at most 1", got)
	}
}

func TestBreakRetryCanceledContextNeverRunsTheCallback(t *testing.T) {
	t.Parallel()
	db := newBreakRetryDB(t)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	var calls atomic.Int32
	sqlFn := func(*sql.Tx) error { calls.Add(1); return nil }
	gormFn := func(*gorm.DB) error { calls.Add(1); return nil }
	results := map[string]error{
		"WriteTx":     db.WriteTx(ctx, sqlFn),
		"ReadTx":      db.ReadTx(ctx, sqlFn),
		"WriteGORMTx": db.WriteGORMTx(ctx, gormFn),
		"ReadGORMTx":  db.ReadGORMTx(ctx, gormFn),
	}
	for name, err := range results {
		if !errors.Is(err, context.Canceled) {
			t.Errorf("%s() with a canceled context = %v", name, err)
		}
	}
	if calls.Load() != 0 {
		t.Fatalf("callbacks ran %d times under a canceled context", calls.Load())
	}
}

func TestBreakRetryClosureSideEffectsRunOncePerAttempt(t *testing.T) {
	t.Parallel()
	db := newBreakRetryDB(t)
	ctx := t.Context()

	// Anything outside tx (captured slices, the pool itself, sequences) is
	// not rolled back between attempts. This pins that contract so callers
	// reset captured results at the top of the callback.
	var appended []int32
	var result []string
	var attempts atomic.Int32
	err := db.WriteTx(ctx, func(tx *sql.Tx) error {
		attempt := attempts.Add(1)
		appended = append(appended, attempt)
		result = result[:0]
		var id int64
		if err := tx.QueryRowContext(ctx,
			`INSERT INTO break_ledger (note) VALUES ($1) RETURNING id`, fmt.Sprintf("tx-%d", attempt)).Scan(&id); err != nil {
			return err
		}
		result = append(result, fmt.Sprintf("id-%d", id))
		if _, err := db.SQLDB().ExecContext(ctx,
			`INSERT INTO probes (id, label) VALUES ($1, $2)`, attempt, fmt.Sprintf("escaped-%d", attempt)); err != nil {
			return err
		}
		if attempt < 3 {
			return forceSQLState(ctx, tx, "40001")
		}
		return nil
	})
	if err != nil || attempts.Load() != 3 {
		t.Fatalf("WriteTx() = %v after %d attempts, want nil after 3", err, attempts.Load())
	}
	if len(appended) != 3 {
		t.Fatalf("captured side effects = %v, want one per attempt", appended)
	}
	if len(result) != 1 || result[0] != "id-3" {
		t.Fatalf("reset-per-attempt result = %v, want only the committed attempt's id-3", result)
	}
	if notes := breakLedgerNotes(t, db); len(notes) != 1 || notes[0] != "tx-3" {
		t.Fatalf("ledger = %v, want only the committed attempt", notes)
	}
	// Writes through the pool inside the callback escaped every rollback.
	if got := breakCount(t, db, `SELECT count(*) FROM probes WHERE label LIKE 'escaped-%'`); got != 3 {
		t.Fatalf("pool writes made inside the callback = %d, want 3 (one per attempt)", got)
	}
}

func TestBreakRetryAttemptsStartFromACleanSession(t *testing.T) {
	t.Parallel()
	db := newBreakRetryDB(t)
	pinBreakRetryPool(db)
	ctx := t.Context()

	var firstPID, secondPID int
	var tempTableGone, timeoutReverted bool
	advisoryHeld := -1
	var attempts atomic.Int32
	err := db.WriteTx(ctx, func(tx *sql.Tx) error {
		attempt := attempts.Add(1)
		if attempt == 1 {
			if err := tx.QueryRowContext(ctx, `SELECT pg_backend_pid()`).Scan(&firstPID); err != nil {
				return err
			}
			for _, statement := range []string{
				`CREATE TEMP TABLE break_scratch (x int)`,
				`SET statement_timeout = '1234ms'`,
				`SELECT pg_advisory_xact_lock(4242)`,
			} {
				if _, err := tx.ExecContext(ctx, statement); err != nil {
					return err
				}
			}
			return forceSQLState(ctx, tx, "40001")
		}
		var timeout string
		if err := tx.QueryRowContext(ctx, `SELECT pg_backend_pid(), to_regclass('pg_temp.break_scratch') IS NULL,
			current_setting('statement_timeout'),
			(SELECT count(*) FROM pg_locks WHERE locktype = 'advisory' AND pid = pg_backend_pid())`,
		).Scan(&secondPID, &tempTableGone, &timeout, &advisoryHeld); err != nil {
			return err
		}
		timeoutReverted = timeout != "1234ms"
		return nil
	})
	if err != nil || attempts.Load() != 2 {
		t.Fatalf("WriteTx() = %v after %d attempts", err, attempts.Load())
	}
	if firstPID != secondPID {
		t.Fatalf("attempts ran on backends %d and %d; the pinned pool should reuse one", firstPID, secondPID)
	}
	if !tempTableGone || !timeoutReverted || advisoryHeld != 0 {
		t.Fatalf("retry inherited state: temp table gone=%v, timeout reverted=%v, advisory locks=%d",
			tempTableGone, timeoutReverted, advisoryHeld)
	}
}

func TestBreakRetryReadTxRejectsEveryWriteShape(t *testing.T) {
	t.Parallel()
	db := newBreakRetryDB(t)
	ctx := t.Context()

	cases := []struct {
		name, statement, code string
	}{
		{"insert", `INSERT INTO probes (id, label) VALUES (1, 'ro')`, "25006"},
		{"update", `UPDATE counters SET value = value + 1`, "25006"},
		{"delete", `DELETE FROM counters`, "25006"},
		{"truncate", `TRUNCATE probes`, "25006"},
		{"writable cte", `WITH moved AS (UPDATE counters SET value = 9 RETURNING value) SELECT count(*) FROM moved`, "25006"},
		{"select for update", `SELECT value FROM counters FOR UPDATE`, "25006"},
		{"nextval", `SELECT nextval(pg_get_serial_sequence('break_ledger', 'id'))`, "25006"},
		{"create table", `CREATE TABLE break_ro_new (x int)`, "25006"},
		{"create temp table", `CREATE TEMP TABLE break_ro_temp (x int)`, "25006"},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			var attempts atomic.Int32
			err := db.ReadTx(ctx, func(tx *sql.Tx) error {
				attempts.Add(1)
				_, err := tx.ExecContext(ctx, testCase.statement)
				return err
			})
			if code, _ := control.PostgresErrorCode(err); code != testCase.code || attempts.Load() != 1 {
				t.Fatalf("ReadTx(%s) = %v (code %q) after %d attempts, want %s once",
					testCase.name, err, code, attempts.Load(), testCase.code)
			}
			err = db.ReadGORMTx(ctx, func(tx *gorm.DB) error {
				return tx.Exec(testCase.statement).Error
			})
			if code, _ := control.PostgresErrorCode(err); code != testCase.code {
				t.Fatalf("ReadGORMTx(%s) = %v (code %q), want %s", testCase.name, err, code, testCase.code)
			}
		})
	}

	// Read-write mode can only be requested before the snapshot is taken.
	err := db.ReadTx(ctx, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx, `SELECT 1`); err != nil {
			return err
		}
		_, err := tx.ExecContext(ctx, `SET TRANSACTION READ WRITE`)
		return err
	})
	if code, _ := control.PostgresErrorCode(err); code != "25001" {
		t.Fatalf("late SET TRANSACTION READ WRITE = %v (code %q), want 25001", err, code)
	}
	if got := countProbes(t, db); got != 0 {
		t.Fatalf("probes = %d after rejected read-only writes, want 0", got)
	}
}

func TestBreakRetryReadTxDiscardsWritesAfterAReadWriteFlip(t *testing.T) {
	t.Parallel()
	db := newBreakRetryDB(t)
	ctx := t.Context()

	// SET TRANSACTION READ WRITE is legal as the first statement, so a
	// callback can lift read-only mode. ReadTx must still never commit.
	err := db.ReadTx(ctx, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx, `SET TRANSACTION READ WRITE`); err != nil {
			return err
		}
		_, err := tx.ExecContext(ctx, `INSERT INTO probes (id, label) VALUES (1, 'flipped')`)
		return err
	})
	if err != nil {
		t.Fatalf("ReadTx() after a read-write flip = %v", err)
	}
	err = db.ReadGORMTx(ctx, func(tx *gorm.DB) error {
		if err := tx.Exec(`SET TRANSACTION READ WRITE`).Error; err != nil {
			return err
		}
		return tx.Exec(`INSERT INTO probes (id, label) VALUES (2, 'flipped-gorm')`).Error
	})
	if err != nil {
		t.Fatalf("ReadGORMTx() after a read-write flip = %v", err)
	}
	if got := countProbes(t, db); got != 0 {
		t.Fatalf("probes = %d: a read transaction's writes were committed", got)
	}
}

func TestBreakRetryCallbacksThatEndTheTransactionAreRejected(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		// run executes one misbehaving callback and reports its attempts.
		run func(ctx context.Context, db *control.PostgresDB, attempts *atomic.Int32) error
		// committed is how many ledger rows the misuse itself let through.
		committed int
	}{
		{
			name: "raw COMMIT then a retryable abort",
			run: func(ctx context.Context, db *control.PostgresDB, attempts *atomic.Int32) error {
				return db.WriteTx(ctx, func(tx *sql.Tx) error {
					attempts.Add(1)
					for _, statement := range []string{
						`INSERT INTO break_ledger (note) VALUES ('before-commit')`,
						`COMMIT`,
						`INSERT INTO break_ledger (note) VALUES ('autocommitted')`,
					} {
						if _, err := tx.ExecContext(ctx, statement); err != nil {
							return err
						}
					}
					return syntheticAbort("40001")
				})
			},
			committed: 2,
		},
		{
			name: "raw ROLLBACK then success",
			run: func(ctx context.Context, db *control.PostgresDB, attempts *atomic.Int32) error {
				return db.WriteTx(ctx, func(tx *sql.Tx) error {
					attempts.Add(1)
					if _, err := tx.ExecContext(ctx, `INSERT INTO break_ledger (note) VALUES ('lost')`); err != nil {
						return err
					}
					_, err := tx.ExecContext(ctx, `ROLLBACK`)
					return err
				})
			},
		},
		{
			name: "raw END then success",
			run: func(ctx context.Context, db *control.PostgresDB, attempts *atomic.Int32) error {
				return db.WriteTx(ctx, func(tx *sql.Tx) error {
					attempts.Add(1)
					_, err := tx.ExecContext(ctx, `INSERT INTO break_ledger (note) VALUES ('ended'); END`)
					return err
				})
			},
			committed: 1,
		},
		{
			name: "tx.Commit inside the callback",
			run: func(ctx context.Context, db *control.PostgresDB, attempts *atomic.Int32) error {
				return db.WriteTx(ctx, func(tx *sql.Tx) error {
					attempts.Add(1)
					if _, err := tx.ExecContext(ctx, `INSERT INTO break_ledger (note) VALUES ('self-committed')`); err != nil {
						return err
					}
					return tx.Commit()
				})
			},
			committed: 1,
		},
		{
			name: "tx.Rollback inside the callback",
			run: func(ctx context.Context, db *control.PostgresDB, attempts *atomic.Int32) error {
				return db.WriteTx(ctx, func(tx *sql.Tx) error {
					attempts.Add(1)
					if _, err := tx.ExecContext(ctx, `INSERT INTO break_ledger (note) VALUES ('self-rolled-back')`); err != nil {
						return err
					}
					return tx.Rollback()
				})
			},
		},
		{
			name: "raw COMMIT inside ReadTx",
			run: func(ctx context.Context, db *control.PostgresDB, attempts *atomic.Int32) error {
				return db.ReadTx(ctx, func(tx *sql.Tx) error {
					attempts.Add(1)
					// After COMMIT the session is in autocommit mode with the
					// default read-write access, outside ReadTx's guarantees.
					_, err := tx.ExecContext(ctx, `COMMIT; INSERT INTO break_ledger (note) VALUES ('read-escape')`)
					return err
				})
			},
			committed: 1,
		},
		{
			name: "GORM raw ROLLBACK then success",
			run: func(ctx context.Context, db *control.PostgresDB, attempts *atomic.Int32) error {
				return db.WriteGORMTx(ctx, func(tx *gorm.DB) error {
					attempts.Add(1)
					if err := tx.Exec(`INSERT INTO break_ledger (note) VALUES ('gorm-lost')`).Error; err != nil {
						return err
					}
					return tx.Exec(`ROLLBACK`).Error
				})
			},
		},
		{
			name: "GORM Commit inside the callback",
			run: func(ctx context.Context, db *control.PostgresDB, attempts *atomic.Int32) error {
				return db.WriteGORMTx(ctx, func(tx *gorm.DB) error {
					attempts.Add(1)
					if err := tx.Exec(`INSERT INTO break_ledger (note) VALUES ('gorm-self-committed')`).Error; err != nil {
						return err
					}
					return tx.Commit().Error
				})
			},
			committed: 1,
		},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			db := newBreakRetryDB(t)
			pinBreakRetryPool(db)
			ctx := t.Context()

			var attempts atomic.Int32
			err := testCase.run(ctx, db, &attempts)
			if !errors.Is(err, control.ErrTransactionEndedByCallback) || attempts.Load() != 1 {
				t.Fatalf("%s = %v after %d attempts, want ErrTransactionEndedByCallback after 1",
					testCase.name, err, attempts.Load())
			}
			if got := breakCount(t, db, `SELECT count(*) FROM break_ledger`); got != testCase.committed {
				t.Fatalf("ledger rows = %d after %s, want %d", got, testCase.name, testCase.committed)
			}
			// The pinned connection must come back usable and serializable.
			var isolation string
			if err := db.WriteTx(ctx, func(tx *sql.Tx) error {
				return tx.QueryRowContext(ctx, `SHOW transaction_isolation`).Scan(&isolation)
			}); err != nil || isolation != "serializable" {
				t.Fatalf("WriteTx() after misuse = %v (isolation %q)", err, isolation)
			}
		})
	}
}

func TestBreakRetryNestedTransactionsCannotDeadlockForever(t *testing.T) {
	t.Parallel()

	t.Run("same row", func(t *testing.T) {
		t.Parallel()
		db := newBreakRetryDB(t)
		ctx, cancel := context.WithTimeout(t.Context(), 400*time.Millisecond)
		defer cancel()
		// The inner transaction waits on a row lock its own caller holds.
		// PostgreSQL sees one waiter and no cycle, so only ctx ends it.
		err := db.WriteTx(ctx, func(tx *sql.Tx) error {
			if _, err := tx.ExecContext(ctx, `UPDATE break_accounts SET balance = 0 WHERE id = 1`); err != nil {
				return err
			}
			return db.WriteTx(ctx, func(inner *sql.Tx) error {
				_, err := inner.ExecContext(ctx, `UPDATE break_accounts SET balance = -1 WHERE id = 1`)
				return err
			})
		})
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("self-blocking nested WriteTx() = %v, want the deadline", err)
		}
		waitForQuietBackends(t, db)
		if got := breakCount(t, db, `SELECT balance FROM break_accounts WHERE id = 1`); got != 100 {
			t.Fatalf("balance = %d after the abandoned nested write, want 100", got)
		}
	})

	t.Run("exhausted pool", func(t *testing.T) {
		t.Parallel()
		db := newBreakRetryDB(t)
		pinBreakRetryPool(db)
		ctx, cancel := context.WithTimeout(t.Context(), 300*time.Millisecond)
		defer cancel()
		var innerRan atomic.Bool
		err := db.WriteTx(ctx, func(*sql.Tx) error {
			return db.ReadTx(ctx, func(*sql.Tx) error { innerRan.Store(true); return nil })
		})
		if !errors.Is(err, context.DeadlineExceeded) || innerRan.Load() {
			t.Fatalf("nested ReadTx() on an exhausted pool = %v (inner ran=%v), want the deadline",
				err, innerRan.Load())
		}
		if err := db.WriteTx(t.Context(), func(tx *sql.Tx) error {
			_, err := tx.ExecContext(t.Context(), `INSERT INTO probes (id, label) VALUES (1, 'after')`)
			return err
		}); err != nil {
			t.Fatalf("WriteTx() after pool exhaustion = %v", err)
		}
	})

	t.Run("inner snapshot excludes outer writes", func(t *testing.T) {
		t.Parallel()
		db := newBreakRetryDB(t)
		ctx := t.Context()
		innerCount := -1
		err := db.WriteTx(ctx, func(tx *sql.Tx) error {
			if _, err := tx.ExecContext(ctx, `INSERT INTO probes (id, label) VALUES (1, 'uncommitted')`); err != nil {
				return err
			}
			return db.ReadTx(ctx, func(inner *sql.Tx) error {
				return inner.QueryRowContext(ctx, `SELECT count(*) FROM probes`).Scan(&innerCount)
			})
		})
		if err != nil || innerCount != 0 {
			t.Fatalf("nested ReadTx() = %v counted %d probes, want 0 (outer write uncommitted)", err, innerCount)
		}
	})
}

func TestBreakRetryEscapedHandlesAreDead(t *testing.T) {
	t.Parallel()
	db := newBreakRetryDB(t)
	ctx := t.Context()

	var committed, aborted, read *sql.Tx
	if err := db.WriteTx(ctx, func(tx *sql.Tx) error { committed = tx; return nil }); err != nil {
		t.Fatalf("WriteTx() = %v", err)
	}
	sentinel := errors.New("stop")
	if err := db.WriteTx(ctx, func(tx *sql.Tx) error { aborted = tx; return sentinel }); !errors.Is(err, sentinel) {
		t.Fatalf("WriteTx() = %v", err)
	}
	if err := db.ReadTx(ctx, func(tx *sql.Tx) error { read = tx; return nil }); err != nil {
		t.Fatalf("ReadTx() = %v", err)
	}
	for name, tx := range map[string]*sql.Tx{"committed": committed, "aborted": aborted, "read": read} {
		_, err := tx.ExecContext(ctx, `INSERT INTO probes (id, label) VALUES (1, 'escaped')`)
		if !errors.Is(err, sql.ErrTxDone) {
			t.Errorf("escaped %s tx Exec() = %v, want sql.ErrTxDone", name, err)
		}
	}
	if got := countProbes(t, db); got != 0 {
		t.Fatalf("probes = %d written through escaped transactions", got)
	}
}

func TestBreakRetryLeakedRowsDoNotHangRetryOrCommit(t *testing.T) {
	t.Parallel()
	db := newBreakRetryDB(t)
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()

	var attempts atomic.Int32
	err := db.WriteTx(ctx, func(tx *sql.Tx) error {
		attempt := attempts.Add(1)
		if _, err := tx.ExecContext(ctx, `INSERT INTO probes (id, label) VALUES (1, 'with-leak')`); err != nil {
			return err
		}
		// Deliberately never closed: the transaction must close it.
		rows, err := tx.QueryContext(ctx, `SELECT generate_series(1, 100000)`)
		if err != nil {
			return err
		}
		if !rows.Next() {
			return errors.New("leaked rows returned nothing")
		}
		if attempt == 1 {
			return syntheticAbort("40001")
		}
		return nil
	})
	if err != nil || attempts.Load() != 2 {
		t.Fatalf("WriteTx() with leaked rows = %v after %d attempts", err, attempts.Load())
	}
	if got := countProbes(t, db); got != 1 {
		t.Fatalf("probes = %d after leaked-rows commit, want 1", got)
	}
}

func TestBreakRetryPanicOnARetriedAttemptRollsBack(t *testing.T) {
	t.Parallel()
	db := newBreakRetryDB(t)
	pinBreakRetryPool(db)
	ctx := t.Context()

	var attempts atomic.Int32
	func() {
		defer func() {
			if recovered := recover(); recovered != "second attempt" {
				t.Fatalf("recovered %v, want the second attempt's panic", recovered)
			}
		}()
		_ = db.WriteTx(ctx, func(tx *sql.Tx) error {
			if _, err := tx.ExecContext(ctx, `INSERT INTO probes (id, label) VALUES (1, 'doomed')`); err != nil {
				t.Errorf("insert: %v", err)
			}
			if attempts.Add(1) == 1 {
				return syntheticAbort("40P01")
			}
			panic("second attempt")
		})
	}()
	if attempts.Load() != 2 || countProbes(t, db) != 0 {
		t.Fatalf("attempts = %d, probes = %d after a panicking retry", attempts.Load(), countProbes(t, db))
	}
}

func TestBreakRetryGORMStatementsDoNotLeakBetweenCalls(t *testing.T) {
	t.Parallel()
	db := newBreakRetryDB(t)
	ctx := t.Context()

	type probe struct {
		ID    int64
		Label string
	}
	var filtered, total int64
	var attempts atomic.Int32
	err := db.WriteGORMTx(ctx, func(tx *gorm.DB) error {
		attempt := attempts.Add(1)
		for id, label := range []string{"x", "y", "z"} {
			if err := tx.Table("probes").Create(&probe{ID: int64(id + 1), Label: label}).Error; err != nil {
				return err
			}
		}
		if err := tx.Table("probes").Where("label = ?", "x").Count(&filtered).Error; err != nil {
			return err
		}
		// A Where clause leaking from the previous call would count 1.
		if err := tx.Table("probes").Count(&total).Error; err != nil {
			return err
		}
		if attempt == 1 {
			return syntheticAbort("40001")
		}
		return nil
	})
	if err != nil || attempts.Load() != 2 || filtered != 1 || total != 3 {
		t.Fatalf("WriteGORMTx() = %v after %d attempts, filtered=%d total=%d", err, attempts.Load(), filtered, total)
	}
}

func TestBreakRetrySessionSettingsDoNotLeakIntoThePool(t *testing.T) {
	t.Parallel()

	// The pool checks drift through the server's ParameterStatus reports;
	// a server that stopped reporting either setting would disarm it.
	t.Run("server reports the pinned settings", func(t *testing.T) {
		t.Parallel()
		db := newBreakRetryDB(t)
		conn, err := db.SQLDB().Conn(t.Context())
		if err != nil {
			t.Fatalf("borrow connection: %v", err)
		}
		defer func() { _ = conn.Close() }()
		var searchPath, timeZone string
		if err := conn.Raw(func(driverConn any) error {
			pgConn := driverConn.(*stdlib.Conn).Conn().PgConn()
			searchPath, timeZone = pgConn.ParameterStatus("search_path"), pgConn.ParameterStatus("TimeZone")
			return nil
		}); err != nil {
			t.Fatalf("inspect connection: %v", err)
		}
		if searchPath != control.PostgresSearchPath || timeZone != "UTC" {
			t.Fatalf("reported search_path=%q TimeZone=%q, want %q and UTC", searchPath, timeZone, control.PostgresSearchPath)
		}
	})

	leaks := []struct {
		name string
		leak func(ctx context.Context, db *control.PostgresDB) error
	}{
		{"SET search_path in a committed WriteTx", func(ctx context.Context, db *control.PostgresDB) error {
			return db.WriteTx(ctx, func(tx *sql.Tx) error {
				_, err := tx.ExecContext(ctx, `SET search_path TO public`)
				return err
			})
		}},
		{"set_config search_path in a committed WriteGORMTx", func(ctx context.Context, db *control.PostgresDB) error {
			return db.WriteGORMTx(ctx, func(tx *gorm.DB) error {
				return tx.Exec(`SELECT set_config('search_path', 'pg_catalog', false)`).Error
			})
		}},
		{"SET TIME ZONE in a committed WriteTx", func(ctx context.Context, db *control.PostgresDB) error {
			return db.WriteTx(ctx, func(tx *sql.Tx) error {
				_, err := tx.ExecContext(ctx, `SET TIME ZONE 'America/New_York'`)
				return err
			})
		}},
		{"SET search_path through the pool", func(ctx context.Context, db *control.PostgresDB) error {
			_, err := db.SQLDB().ExecContext(ctx, `SET search_path TO public`)
			return err
		}},
	}
	for _, scenario := range leaks {
		t.Run(scenario.name, func(t *testing.T) {
			t.Parallel()
			db := newBreakRetryDB(t)
			pinBreakRetryPool(db)
			ctx := t.Context()

			if err := scenario.leak(ctx, db); err != nil {
				t.Fatalf("leak session setting: %v", err)
			}
			var searchPath, timeZone string
			if err := db.SQLDB().QueryRowContext(ctx,
				`SELECT current_setting('search_path'), current_setting('TimeZone')`).Scan(&searchPath, &timeZone); err != nil {
				t.Fatalf("read session settings: %v", err)
			}
			if searchPath != control.PostgresSearchPath || timeZone != "UTC" {
				t.Fatalf("pooled session search_path=%q TimeZone=%q, want %q and UTC",
					searchPath, timeZone, control.PostgresSearchPath)
			}
			// Unqualified control-plane names still resolve.
			if err := db.ReadTx(ctx, func(tx *sql.Tx) error {
				var count int
				return tx.QueryRowContext(ctx, `SELECT count(*) FROM probes`).Scan(&count)
			}); err != nil {
				t.Fatalf("unqualified read after %s = %v", scenario.name, err)
			}
		})
	}
}
