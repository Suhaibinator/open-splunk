package control_test

import (
	"context"
	"database/sql"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/logger"

	"github.com/Suhaibinator/open-splunk/internal/control"
)

// incrementSharedCounterOwnTx is a route that keeps its own Begin/Commit
// seam, as the knowledge catalog writer does, and so can only get retries
// through RetryWrite.
func incrementSharedCounterOwnTx(ctx context.Context, db *control.PostgresDB) error {
	tx, err := db.SQLDB().BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return err
	}
	var value int64
	if err := tx.QueryRowContext(ctx, `SELECT value FROM counters WHERE name = 'shared'`).Scan(&value); err != nil {
		return errors.Join(err, tx.Rollback())
	}
	if _, err := tx.ExecContext(ctx, `UPDATE counters SET value = $1 WHERE name = 'shared'`, value+1); err != nil {
		return errors.Join(err, tx.Rollback())
	}
	return tx.Commit()
}

func sharedCounter(t *testing.T, db *control.PostgresDB) int64 {
	t.Helper()
	var value int64
	if err := db.SQLDB().QueryRowContext(t.Context(),
		`SELECT value FROM counters WHERE name = 'shared'`).Scan(&value); err != nil {
		t.Fatalf("read shared counter: %v", err)
	}
	return value
}

// Hot-row writers that own their transaction converge under RetryWrite, and
// do so beside WriteTx writers of the same row: both join one write queue.
func TestRetryWriteConvergesHotRowWritersWithWriteTx(t *testing.T) {
	t.Parallel()
	db := newPostgresFixtureDB(t)
	ctx := t.Context()
	const contenders = 16
	var start sync.WaitGroup
	start.Add(1)
	errs := make([]error, contenders)
	var wg sync.WaitGroup
	for index := range contenders {
		wg.Go(func() {
			start.Wait()
			if index%2 == 0 {
				errs[index] = db.RetryWrite(ctx, func() error { return incrementSharedCounterOwnTx(ctx, db) })
				return
			}
			errs[index] = db.WriteTx(ctx, func(tx *sql.Tx) error {
				var value int64
				if err := tx.QueryRowContext(ctx,
					`SELECT value FROM counters WHERE name = 'shared'`).Scan(&value); err != nil {
					return err
				}
				_, err := tx.ExecContext(ctx, `UPDATE counters SET value = $1 WHERE name = 'shared'`, value+1)
				return err
			})
		})
	}
	start.Done()
	wg.Wait()
	for index, err := range errs {
		if err != nil {
			t.Fatalf("writer %d: %v", index, err)
		}
	}
	if got := sharedCounter(t, db); got != contenders {
		t.Fatalf("shared counter = %d, want %d: a write was lost", got, contenders)
	}
}

func TestRetryWriteRetriesOnlyRetryableFailures(t *testing.T) {
	t.Parallel()
	db := newPostgresFixtureDB(t)
	ctx := t.Context()
	forced := func(code string) error {
		tx, err := db.SQLDB().BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
		if err != nil {
			return err
		}
		return errors.Join(forceSQLState(ctx, tx, code), tx.Rollback())
	}

	var attempts atomic.Int32
	if err := db.RetryWrite(ctx, func() error {
		if attempts.Add(1) == 1 {
			return forced("40001")
		}
		return incrementSharedCounterOwnTx(ctx, db)
	}); err != nil {
		t.Fatalf("RetryWrite() after one serialization failure = %v", err)
	}
	if got := attempts.Load(); got != 2 {
		t.Fatalf("RetryWrite ran %d attempts, want 2", got)
	}

	attempts.Store(0)
	err := db.RetryWrite(ctx, func() error {
		attempts.Add(1)
		return forced("23505")
	})
	if code, _ := control.PostgresErrorCode(err); code != "23505" || attempts.Load() != 1 {
		t.Fatalf("RetryWrite(unique violation) = %v after %d attempts, want it once", err, attempts.Load())
	}

	attempts.Store(0)
	err = db.RetryWrite(ctx, func() error {
		attempts.Add(1)
		return forced("40P01")
	})
	if !errors.Is(err, control.ErrTransactionRetriesExhausted) || !control.IsRetryable(err) ||
		attempts.Load() != control.PostgresTxMaxAttempts {
		t.Fatalf("RetryWrite(deadlock forever) = %v after %d attempts", err, attempts.Load())
	}
	if got := sharedCounter(t, db); got != 1 {
		t.Fatalf("shared counter = %d, want only the one successful attempt", got)
	}
}

func TestRetryWriteRejectsMissingArguments(t *testing.T) {
	t.Parallel()
	db := newPostgresFixtureDB(t)
	if err := db.RetryWrite(t.Context(), nil); !errors.Is(err, control.ErrInvalidArgument) {
		t.Fatalf("RetryWrite(nil attempt) = %v", err)
	}
	var missing context.Context
	if err := db.RetryWrite(missing, func() error { return nil }); !errors.Is(err, control.ErrInvalidArgument) {
		t.Fatalf("RetryWrite(nil context) = %v", err)
	}
	if err := (*control.PostgresDB)(nil).RetryWrite(t.Context(), func() error { return nil }); !errors.Is(
		err, control.ErrInvalidArgument) {
		t.Fatalf("nil RetryWrite() = %v", err)
	}
}

// countingLogger counts the statements GORM traces.
type countingLogger struct {
	statements *atomic.Int64
}

func (l countingLogger) LogMode(logger.LogLevel) logger.Interface { return l }
func (countingLogger) Info(context.Context, string, ...any)       {}
func (countingLogger) Warn(context.Context, string, ...any)       {}
func (countingLogger) Error(context.Context, string, ...any)      {}
func (l countingLogger) Trace(context.Context, time.Time, func() (string, int64), error) {
	l.statements.Add(1)
}

// Stores whose tests instrument a GORM session must be able to run their
// transactions on that session rather than on the handle's default one.
func TestGORMTxOnRunsOnTheGivenSession(t *testing.T) {
	t.Parallel()
	db := newPostgresFixtureDB(t)
	ctx := t.Context()
	var statements atomic.Int64
	session := db.GORMDB().Session(&gorm.Session{Logger: countingLogger{statements: &statements}})
	if err := db.WriteGORMTxOn(ctx, session, func(tx *gorm.DB) error {
		return tx.Exec(`UPDATE counters SET value = value + 1 WHERE name = 'shared'`).Error
	}); err != nil {
		t.Fatalf("WriteGORMTxOn() = %v", err)
	}
	var value int64
	if err := db.ReadGORMTxOn(ctx, session, func(tx *gorm.DB) error {
		return tx.Raw(`SELECT value FROM counters WHERE name = 'shared'`).Scan(&value).Error
	}); err != nil || value != 1 {
		t.Fatalf("ReadGORMTxOn() = %d, %v; want 1", value, err)
	}
	if got := statements.Load(); got != 2 {
		t.Fatalf("instrumented session traced %d statements, want 2", got)
	}
	if err := db.WriteGORMTx(ctx, func(tx *gorm.DB) error {
		return tx.Exec(`UPDATE counters SET value = value + 1 WHERE name = 'shared'`).Error
	}); err != nil {
		t.Fatalf("WriteGORMTx() = %v", err)
	}
	if got := statements.Load(); got != 2 {
		t.Fatalf("WriteGORMTx traced %d statements on the instrumented session, want none", got-2)
	}

	other := newPostgresFixtureDB(t)
	refused := func(session *gorm.DB) error {
		return db.WriteGORMTxOn(ctx, session, func(*gorm.DB) error {
			t.Error("callback ran on a refused session")
			return nil
		})
	}
	if err := refused(other.GORMDB()); !errors.Is(err, control.ErrInvalidArgument) {
		t.Fatalf("WriteGORMTxOn(another handle's session) = %v", err)
	}
	if err := refused(nil); !errors.Is(err, control.ErrInvalidArgument) {
		t.Fatalf("WriteGORMTxOn(nil session) = %v", err)
	}
	if err := db.WriteGORMTx(ctx, func(tx *gorm.DB) error {
		return refused(tx)
	}); !errors.Is(err, control.ErrInvalidArgument) {
		t.Fatalf("WriteGORMTxOn(a session inside a transaction) = %v", err)
	}
}
