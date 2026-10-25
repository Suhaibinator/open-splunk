package control_test

import (
	"context"
	"database/sql"
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	"gorm.io/gorm"

	"github.com/Suhaibinator/open-splunk/internal/control"
)

func countProbes(t *testing.T, db *control.PostgresDB) int {
	t.Helper()
	var count int
	if err := db.SQLDB().QueryRowContext(t.Context(), `SELECT count(*) FROM probes`).Scan(&count); err != nil {
		t.Fatalf("count probes: %v", err)
	}
	return count
}

func TestPostgresWriteTxRetriesRetryableAbortsAndCommitsOnce(t *testing.T) {
	t.Parallel()

	for _, code := range []string{"40001", "40P01"} {
		t.Run(code, func(t *testing.T) {
			t.Parallel()
			db := newPostgresFixtureDB(t)
			ctx := t.Context()

			var attempts atomic.Int32
			err := db.WriteTx(ctx, func(tx *sql.Tx) error {
				attempt := attempts.Add(1)
				// Each attempt writes before failing, so a leaked attempt
				// would leave extra rows or a duplicate-key error behind.
				if _, err := tx.ExecContext(ctx, `INSERT INTO probes (id, label) VALUES (1, 'retried')`); err != nil {
					return err
				}
				if attempt < 3 {
					return forceSQLState(ctx, tx, code)
				}
				return nil
			})
			if err != nil {
				t.Fatalf("WriteTx() error = %v", err)
			}
			if got := attempts.Load(); got != 3 {
				t.Fatalf("WriteTx() ran the callback %d times, want 3", got)
			}
			if got := countProbes(t, db); got != 1 {
				t.Fatalf("probes = %d after retried WriteTx, want exactly 1", got)
			}
		})
	}
}

func TestPostgresWriteTxGivesUpAfterTheAttemptBound(t *testing.T) {
	t.Parallel()
	db := newPostgresFixtureDB(t)
	ctx := t.Context()

	var attempts atomic.Int32
	err := db.WriteTx(ctx, func(tx *sql.Tx) error {
		attempts.Add(1)
		if _, err := tx.ExecContext(ctx, `INSERT INTO probes (id, label) VALUES (1, 'never')`); err != nil {
			return err
		}
		return forceSQLState(ctx, tx, "40001")
	})
	if got := attempts.Load(); got != control.PostgresTxMaxAttempts {
		t.Fatalf("WriteTx() ran the callback %d times, want %d", got, control.PostgresTxMaxAttempts)
	}
	if !errors.Is(err, control.ErrTransactionRetriesExhausted) {
		t.Fatalf("WriteTx() error = %v, want ErrTransactionRetriesExhausted", err)
	}
	if code, ok := control.PostgresErrorCode(err); !ok || code != "40001" || !control.IsRetryable(err) {
		t.Fatalf("exhausted error lost its PostgreSQL cause: code=%q ok=%v err=%v", code, ok, err)
	}
	if got := countProbes(t, db); got != 0 {
		t.Fatalf("probes = %d after exhausted WriteTx, want 0", got)
	}
}

func TestPostgresWriteTxReturnsOtherErrorsWithoutRetry(t *testing.T) {
	t.Parallel()
	db := newPostgresFixtureDB(t)
	ctx := t.Context()
	sentinel := errors.New("domain rejection")

	var attempts atomic.Int32
	err := db.WriteTx(ctx, func(tx *sql.Tx) error {
		attempts.Add(1)
		if _, err := tx.ExecContext(ctx, `INSERT INTO probes (id, label) VALUES (1, 'kept-out')`); err != nil {
			return err
		}
		return sentinel
	})
	if !errors.Is(err, sentinel) || attempts.Load() != 1 {
		t.Fatalf("WriteTx() = %v after %d attempts, want sentinel after 1", err, attempts.Load())
	}

	attempts.Store(0)
	err = db.WriteTx(ctx, func(tx *sql.Tx) error {
		attempts.Add(1)
		_, err := tx.ExecContext(ctx, `INSERT INTO probes (id, label) VALUES (1, 'a'), (2, 'a')`)
		return err
	})
	if !control.IsUniqueViolation(err) || attempts.Load() != 1 {
		t.Fatalf("WriteTx() = %v after %d attempts, want one unique violation", err, attempts.Load())
	}
	if got := countProbes(t, db); got != 0 {
		t.Fatalf("probes = %d after failed WriteTx, want 0", got)
	}
}

func TestPostgresWriteTxRetriesRealSerializationConflictsWithoutLostUpdates(t *testing.T) {
	t.Parallel()
	db := newPostgresFixtureDB(t)
	ctx := t.Context()

	const writers, increments = 6, 15
	var attempts, committed atomic.Int64
	var wg sync.WaitGroup
	errs := make(chan error, writers*increments)
	for range writers {
		wg.Go(func() {
			for range increments {
				err := db.WriteTx(ctx, func(tx *sql.Tx) error {
					attempts.Add(1)
					var value int64
					if err := tx.QueryRowContext(ctx,
						`SELECT value FROM counters WHERE name = 'shared'`).Scan(&value); err != nil {
						return err
					}
					_, err := tx.ExecContext(ctx,
						`UPDATE counters SET value = $1 WHERE name = 'shared'`, value+1)
					return err
				})
				if err == nil {
					committed.Add(1)
				} else {
					errs <- err
				}
			}
		})
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		// Under heavy contention one writer may lose every bounded attempt;
		// that must surface as exhaustion, never as a silent lost update.
		if !errors.Is(err, control.ErrTransactionRetriesExhausted) {
			t.Fatalf("contended WriteTx() error = %v", err)
		}
	}

	var value int64
	if err := db.SQLDB().QueryRowContext(ctx,
		`SELECT value FROM counters WHERE name = 'shared'`).Scan(&value); err != nil {
		t.Fatalf("read counter: %v", err)
	}
	if value != committed.Load() {
		t.Fatalf("counter = %d, committed transactions = %d: an update was lost", value, committed.Load())
	}
	if committed.Load() == 0 {
		t.Fatal("no contended WriteTx committed")
	}
	if attempts.Load() <= committed.Load() {
		t.Fatalf("attempts = %d, committed = %d: contention never forced a retry", attempts.Load(), committed.Load())
	}
}

func TestPostgresWriteTxRollsBackWhenTheCallbackPanics(t *testing.T) {
	t.Parallel()
	db := newPostgresFixtureDB(t)
	ctx := t.Context()

	func() {
		defer func() {
			if recovered := recover(); recovered != "boom" {
				t.Fatalf("recovered %v, want the callback's panic", recovered)
			}
		}()
		_ = db.WriteTx(ctx, func(tx *sql.Tx) error {
			if _, err := tx.ExecContext(ctx, `INSERT INTO probes (id, label) VALUES (1, 'panicked')`); err != nil {
				t.Errorf("insert before panic: %v", err)
			}
			panic("boom")
		})
	}()
	if got := countProbes(t, db); got != 0 {
		t.Fatalf("probes = %d after panicking WriteTx, want 0", got)
	}
	if err := db.WriteTx(ctx, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `INSERT INTO probes (id, label) VALUES (1, 'after')`)
		return err
	}); err != nil {
		t.Fatalf("WriteTx() after panic = %v", err)
	}
}

func TestPostgresWriteTxRunsSerializable(t *testing.T) {
	t.Parallel()
	db := newPostgresFixtureDB(t)
	ctx := t.Context()

	var writeIsolation, readIsolation, readOnly string
	if err := db.WriteTx(ctx, func(tx *sql.Tx) error {
		return tx.QueryRowContext(ctx, `SHOW transaction_isolation`).Scan(&writeIsolation)
	}); err != nil {
		t.Fatalf("WriteTx() = %v", err)
	}
	if err := db.ReadTx(ctx, func(tx *sql.Tx) error {
		return tx.QueryRowContext(ctx,
			`SELECT current_setting('transaction_isolation'), current_setting('transaction_read_only')`,
		).Scan(&readIsolation, &readOnly)
	}); err != nil {
		t.Fatalf("ReadTx() = %v", err)
	}
	if writeIsolation != "serializable" || readIsolation != "serializable" || readOnly != "on" {
		t.Fatalf("isolation write=%q read=%q read_only=%q", writeIsolation, readIsolation, readOnly)
	}
}

func TestPostgresReadTxRejectsWritesWithoutRetry(t *testing.T) {
	t.Parallel()
	db := newPostgresFixtureDB(t)
	ctx := t.Context()

	var attempts atomic.Int32
	err := db.ReadTx(ctx, func(tx *sql.Tx) error {
		attempts.Add(1)
		_, err := tx.ExecContext(ctx, `INSERT INTO probes (id, label) VALUES (1, 'read-only')`)
		return err
	})
	// read_only_sql_transaction (25006) is a programming error, not contention.
	if code, _ := control.PostgresErrorCode(err); code != "25006" || attempts.Load() != 1 {
		t.Fatalf("ReadTx() write = %v (code %q) after %d attempts", err, code, attempts.Load())
	}

	attempts.Store(0)
	err = db.ReadTx(ctx, func(tx *sql.Tx) error {
		if attempts.Add(1) == 1 {
			return forceSQLState(ctx, tx, "40001")
		}
		return nil
	})
	if err != nil || attempts.Load() != 2 {
		t.Fatalf("ReadTx() = %v after %d attempts, want nil after 2", err, attempts.Load())
	}
}

func TestPostgresGORMTransactionsShareTheRetryAndIsolationModel(t *testing.T) {
	t.Parallel()
	db := newPostgresFixtureDB(t)
	ctx := t.Context()

	type probe struct {
		ID    int64
		Label string
	}
	var attempts atomic.Int32
	err := db.WriteGORMTx(ctx, func(tx *gorm.DB) error {
		if err := tx.Table("probes").Create(&probe{ID: 1, Label: "gorm"}).Error; err != nil {
			return err
		}
		var isolation string
		if err := tx.Raw(`SHOW transaction_isolation`).Scan(&isolation).Error; err != nil {
			return err
		}
		if isolation != "serializable" {
			return errors.New("GORM write transaction is " + isolation)
		}
		if attempts.Add(1) == 1 {
			return tx.Exec(`DO $$ BEGIN RAISE EXCEPTION 'forced' USING ERRCODE = '40001'; END $$`).Error
		}
		return nil
	})
	if err != nil || attempts.Load() != 2 {
		t.Fatalf("WriteGORMTx() = %v after %d attempts, want nil after 2", err, attempts.Load())
	}
	if got := countProbes(t, db); got != 1 {
		t.Fatalf("probes = %d after retried WriteGORMTx, want 1", got)
	}

	var labels []string
	err = db.ReadGORMTx(ctx, func(tx *gorm.DB) error {
		if err := tx.Table("probes").Order("id").Pluck("label", &labels).Error; err != nil {
			return err
		}
		return tx.Exec(`INSERT INTO probes (id, label) VALUES (2, 'rejected')`).Error
	})
	if code, _ := control.PostgresErrorCode(err); code != "25006" {
		t.Fatalf("ReadGORMTx() write = %v, want read_only_sql_transaction", err)
	}
	if len(labels) != 1 || labels[0] != "gorm" {
		t.Fatalf("ReadGORMTx() labels = %v", labels)
	}
}

func TestPostgresTransactionsRejectMissingArguments(t *testing.T) {
	t.Parallel()
	var nilDB *control.PostgresDB
	var nilContext context.Context
	if err := nilDB.WriteTx(context.Background(), func(*sql.Tx) error { return nil }); !errors.Is(err, control.ErrInvalidArgument) {
		t.Fatalf("nil database WriteTx() = %v", err)
	}
	db := newPostgresFixtureDB(t)
	if err := db.WriteTx(nilContext, func(*sql.Tx) error { return nil }); !errors.Is(err, control.ErrInvalidArgument) {
		t.Fatalf("nil context WriteTx() = %v", err)
	}
	if err := db.ReadTx(t.Context(), nil); !errors.Is(err, control.ErrInvalidArgument) {
		t.Fatalf("nil callback ReadTx() = %v", err)
	}
	if err := db.WriteGORMTx(t.Context(), nil); !errors.Is(err, control.ErrInvalidArgument) {
		t.Fatalf("nil callback WriteGORMTx() = %v", err)
	}
}

func TestPostgresWriteTxStopsRetryingWhenTheContextEnds(t *testing.T) {
	t.Parallel()
	db := newPostgresFixtureDB(t)
	ctx, cancel := context.WithCancel(t.Context())

	var attempts atomic.Int32
	err := db.WriteTx(ctx, func(tx *sql.Tx) error {
		attempts.Add(1)
		retryable := forceSQLState(ctx, tx, "40001")
		cancel()
		return retryable
	})
	if !errors.Is(err, context.Canceled) || !control.IsRetryable(err) || attempts.Load() != 1 {
		t.Fatalf("WriteTx() = %v after %d attempts, want cancellation joined with the abort after 1", err, attempts.Load())
	}
}
