package control_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"gorm.io/gorm"

	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/testsupport"
)

// Break tests for concurrency guarantees SQLite gave for free: a single
// writer that could not keep writing after losing its file lock, read
// snapshots that every serial order of the writes could produce, and rowid
// cursors that became visible in the order they were assigned.

// terminateSingletonHolder ends the backend holding the server singleton on
// dsn and waits for it to exit, so pg_locks no longer lists the lock when it
// returns.
func terminateSingletonHolder(t *testing.T, dsn string) {
	t.Helper()
	var terminated int
	if err := openRawPostgres(t, dsn).QueryRowContext(t.Context(), `
		SELECT count(*) FILTER (WHERE pg_terminate_backend(pid, 10000))
		FROM pg_locks
		WHERE locktype = 'advisory'
			AND database = (SELECT oid FROM pg_database WHERE datname = current_database())
			AND classid = x'6f730000'::int::oid AND objid = 2 AND objsubid = 2 AND granted`).Scan(&terminated); err != nil {
		t.Fatalf("terminate singleton holder: %v", err)
	}
	if terminated != 1 {
		t.Fatalf("terminated %d singleton holders, want 1", terminated)
	}
}

func singletonLostFired(db *control.PostgresDB) bool {
	select {
	case <-db.ServerSingletonLost():
		return true
	default:
		return false
	}
}

func insertWidget(ctx context.Context, db *control.PostgresDB, id int64) error {
	return db.WriteTx(ctx, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `INSERT INTO widgets (id, name) VALUES ($1, 'w')`, id)
		return err
	})
}

// Defect: nothing checked that the singleton was still held after it was
// acquired. Once its session died, the old server kept committing through
// its pool while a newer server took the lock and migrated the schema.
func TestBreakSingletonFencesWritesOnceTheHolderSessionDies(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	dsn := newEmptyPostgresDatabase(t)
	old, oldRelease, err := openServer(t, dsn, runnerMigrations(2))
	if err != nil {
		t.Fatalf("open old server: %v", err)
	}
	if err := insertWidget(ctx, old, 1); err != nil {
		t.Fatalf("WriteTx() while holding the singleton = %v", err)
	}
	if singletonLostFired(old) {
		t.Fatal("ServerSingletonLost fired while the singleton was held")
	}

	terminateSingletonHolder(t, dsn)

	// The next write is refused at once, before the background probe runs.
	var ran atomic.Int32
	err = old.WriteTx(ctx, func(tx *sql.Tx) error {
		ran.Add(1)
		_, err := tx.ExecContext(ctx, `INSERT INTO widgets (id, name) VALUES (2, 'fenced')`)
		return err
	})
	if !errors.Is(err, control.ErrServerSingletonLost) {
		t.Fatalf("WriteTx() after the singleton session died = %v, want ErrServerSingletonLost", err)
	}
	if !singletonLostFired(old) {
		t.Fatal("ServerSingletonLost did not fire once a write found the lock gone")
	}

	var newer *control.PostgresDB
	var newRelease func() error
	deadline := time.Now().Add(10 * time.Second)
	for {
		newer, newRelease, err = openServer(t, dsn, runnerMigrations(3))
		if err == nil {
			break
		}
		if !errors.Is(err, control.ErrServerAlreadyRunning) || time.Now().After(deadline) {
			t.Fatalf("takeover after the holder died = %v", err)
		}
		time.Sleep(20 * time.Millisecond)
	}

	// The fence is sticky: no later write commits, through either API, and
	// a fenced write no longer even runs its callback.
	ran.Store(0)
	for id := int64(3); id < 6; id++ {
		err := old.WriteTx(ctx, func(tx *sql.Tx) error {
			ran.Add(1)
			_, err := tx.ExecContext(ctx, `INSERT INTO widgets (id, name) VALUES ($1, 'fenced')`, id)
			return err
		})
		if !errors.Is(err, control.ErrServerSingletonLost) {
			t.Fatalf("old WriteTx(%d) beside the new server = %v, want ErrServerSingletonLost", id, err)
		}
	}
	err = old.WriteGORMTx(ctx, func(tx *gorm.DB) error {
		ran.Add(1)
		return tx.Exec(`INSERT INTO widgets (id, name) VALUES (9, 'fenced')`).Error
	})
	if !errors.Is(err, control.ErrServerSingletonLost) {
		t.Fatalf("old WriteGORMTx() beside the new server = %v, want ErrServerSingletonLost", err)
	}
	if got := ran.Load(); got != 0 {
		t.Fatalf("fenced writes ran their callback %d times, want 0", got)
	}
	var widgets int
	if err := newer.SQLDB().QueryRowContext(ctx, `SELECT count(*) FROM widgets`).Scan(&widgets); err != nil {
		t.Fatalf("count widgets: %v", err)
	}
	if widgets != 1 {
		t.Fatalf("widgets = %d, want only the 1 written while the old server held the singleton", widgets)
	}
	// A handle that lost its singleton cannot be re-armed with a new one.
	if release, err := control.AcquireServerSingleton(ctx, old); !errors.Is(err, control.ErrServerSingletonLost) {
		if release != nil {
			_ = release()
		}
		t.Fatalf("AcquireServerSingleton() on a fenced handle = %v, want ErrServerSingletonLost", err)
	}
	if err := insertWidget(ctx, newer, 10); err != nil {
		t.Fatalf("new server WriteTx() = %v", err)
	}
	if err := oldRelease(); !errors.Is(err, control.ErrServerSingletonLost) {
		t.Fatalf("old release() = %v, want ErrServerSingletonLost", err)
	}
	if err := newRelease(); err != nil {
		t.Fatalf("new release() = %v", err)
	}
}

// The background probe reports the loss to a server that is not writing, so
// it can shut down instead of learning about it on its next write.
func TestBreakSingletonLossIsSignalledWithoutAWrite(t *testing.T) {
	t.Parallel()
	dsn := newSingletonDatabase(t)
	db := openSecondPostgresHandle(t, dsn)
	release, err := control.AcquireServerSingleton(t.Context(), db)
	if err != nil {
		t.Fatalf("AcquireServerSingleton() = %v", err)
	}
	terminateSingletonHolder(t, dsn)
	select {
	case <-db.ServerSingletonLost():
	case <-time.After(20 * time.Second):
		t.Fatal("ServerSingletonLost did not fire after the singleton session died")
	}
	var ran atomic.Int32
	err = db.WriteTx(t.Context(), func(*sql.Tx) error {
		ran.Add(1)
		return nil
	})
	if !errors.Is(err, control.ErrServerSingletonLost) || ran.Load() != 0 {
		t.Fatalf("WriteTx() after the loss = %v with %d callback runs, want ErrServerSingletonLost and 0",
			err, ran.Load())
	}
	// Reads stay available so a shutting-down server can still report.
	if err := db.ReadTx(t.Context(), func(*sql.Tx) error { return nil }); err != nil {
		t.Fatalf("ReadTx() after the loss = %v", err)
	}
	if err := release(); !errors.Is(err, control.ErrServerSingletonLost) {
		t.Fatalf("release() = %v, want ErrServerSingletonLost", err)
	}
}

// A deliberate release is a shutdown, not a loss: it neither fires
// ServerSingletonLost nor fences the handle, and a held singleton passes the
// pre-commit check on every write.
func TestBreakSingletonDeliberateReleaseDoesNotFence(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	dsn := newSingletonDatabase(t)
	db := openSecondPostgresHandle(t, dsn)
	write := func(id int64) error {
		return db.WriteTx(ctx, func(tx *sql.Tx) error {
			_, err := tx.ExecContext(ctx, `INSERT INTO probes (id, label) VALUES ($1, $2)`, id, fmt.Sprintf("p%d", id))
			return err
		})
	}
	release, err := control.AcquireServerSingleton(ctx, db)
	if err != nil {
		t.Fatalf("AcquireServerSingleton() = %v", err)
	}
	for id := int64(1); id <= 3; id++ {
		if err := write(id); err != nil {
			t.Fatalf("WriteTx(%d) under the singleton = %v", id, err)
		}
	}
	if err := release(); err != nil {
		t.Fatalf("release() = %v", err)
	}
	if singletonLostFired(db) {
		t.Fatal("ServerSingletonLost fired on a deliberate release")
	}
	if err := write(4); err != nil {
		t.Fatalf("WriteTx() after a deliberate release = %v", err)
	}
	again, err := control.AcquireServerSingleton(ctx, db)
	if err != nil {
		t.Fatalf("AcquireServerSingleton() after a deliberate release = %v", err)
	}
	if err := write(5); err != nil {
		t.Fatalf("WriteTx() under the second singleton = %v", err)
	}
	if err := again(); err != nil {
		t.Fatalf("second release() = %v", err)
	}
}

// Defect: ReadTx ran REPEATABLE READ beside SERIALIZABLE writers, which
// admits the read-only serialization anomaly: a report saw a deposit but
// not a withdrawal that serialized before it.
func TestBreakReadTxSeesOnlySerializableStates(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	db := newPostgresFixtureDB(t)
	if err := db.WriteTx(ctx, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `INSERT INTO counters (name, value) VALUES ('checking', 0), ('savings', 0)`)
		return err
	}); err != nil {
		t.Fatalf("seed accounts: %v", err)
	}
	readAccounts := func(tx *sql.Tx) (checking, savings int64, err error) {
		err = tx.QueryRowContext(ctx, `
			SELECT
				(SELECT value FROM counters WHERE name = 'checking'),
				(SELECT value FROM counters WHERE name = 'savings')`).Scan(&checking, &savings)
		return checking, savings, err
	}

	withdrawRead := make(chan struct{})
	resume := make(chan struct{})
	withdrawDone := make(chan error, 1)
	var withdrawAttempts atomic.Int32
	go func() {
		withdrawDone <- db.WriteTx(ctx, func(tx *sql.Tx) error {
			attempt := withdrawAttempts.Add(1)
			checking, savings, err := readAccounts(tx)
			if err != nil {
				return err
			}
			if attempt == 1 {
				close(withdrawRead)
				select {
				case <-resume:
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			amount := int64(10)
			if checking+savings < 10 {
				amount = 11 // overdraft penalty
			}
			_, err = tx.ExecContext(ctx, `UPDATE counters SET value = value - $1 WHERE name = 'checking'`, amount)
			return err
		})
	}()
	select {
	case <-withdrawRead:
	case err := <-withdrawDone:
		t.Fatalf("withdrawal finished before pausing: %v", err)
	}
	if err := db.WriteTx(ctx, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `UPDATE counters SET value = value + 20 WHERE name = 'savings'`)
		return err
	}); err != nil {
		close(resume)
		t.Fatalf("deposit: %v", err)
	}
	var reportChecking, reportSavings int64
	err := db.ReadTx(ctx, func(tx *sql.Tx) error {
		var err error
		reportChecking, reportSavings, err = readAccounts(tx)
		return err
	})
	close(resume)
	if err != nil {
		t.Fatalf("report ReadTx() = %v", err)
	}
	if err := <-withdrawDone; err != nil {
		t.Fatalf("withdrawal WriteTx() = %v", err)
	}
	var finalChecking, finalSavings int64
	if err := db.ReadTx(ctx, func(tx *sql.Tx) error {
		var err error
		finalChecking, finalSavings, err = readAccounts(tx)
		return err
	}); err != nil {
		t.Fatalf("final ReadTx() = %v", err)
	}

	// The final balance tells which serial order the writes took; the
	// report must show a state of that order.
	type state struct{ checking, savings int64 }
	var serial []state
	switch {
	case finalChecking == -11 && finalSavings == 20: // withdrawal, then deposit
		serial = []state{{0, 0}, {-11, 0}, {-11, 20}}
	case finalChecking == -10 && finalSavings == 20: // deposit, then withdrawal
		serial = []state{{0, 0}, {0, 20}, {-10, 20}}
	default:
		t.Fatalf("final checking=%d savings=%d matches no serial order", finalChecking, finalSavings)
	}
	report := state{reportChecking, reportSavings}
	consistent := false
	for _, candidate := range serial {
		consistent = consistent || candidate == report
	}
	if !consistent {
		t.Fatalf("report saw checking=%d savings=%d, which no serial order ending at checking=%d savings=%d "+
			"shows (withdraw attempts=%d)", reportChecking, reportSavings, finalChecking, finalSavings,
			withdrawAttempts.Load())
	}
}

// Defect: durable_search_jobs.row_cursor was drawn at INSERT time, so a job
// inserted first but committed second appeared below a high-water mark a
// listing had already read. Cursors must be drawn in commit order, as
// SQLite's rowid was under its single writer.
func TestBreakSearchJobCursorsAreDrawnInCommitOrder(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	db := testsupport.NewPostgresControlDB(t)
	if err := db.WriteTx(ctx, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx,
			`INSERT INTO tenants (tenant_id, created_at_unix_micro, updated_at_unix_micro) VALUES ('t1', 1, 1)`)
		return err
	}); err != nil {
		t.Fatalf("insert tenant: %v", err)
	}
	const insertJob = `
		INSERT INTO durable_search_jobs (id, tenant_id, owner_id, state, visibility, retention_class,
			lifetime_ns, job_payload, created_at_us, version)
		VALUES ($1, 't1', 'owner', 1, 1, 1, 1, '\x0a'::bytea, 1, 1)
		RETURNING row_cursor`

	early, err := db.SQLDB().BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		t.Fatalf("begin early transaction: %v", err)
	}
	defer func() { _ = early.Rollback() }()
	var earlyCursor int64
	if err := early.QueryRowContext(ctx, insertJob, "job-early").Scan(&earlyCursor); err != nil {
		t.Fatalf("insert early job: %v", err)
	}

	type insertResult struct {
		cursor int64
		err    error
	}
	lateDone := make(chan insertResult, 1)
	go func() {
		var cursor int64
		err := db.WriteTx(ctx, func(tx *sql.Tx) error {
			return tx.QueryRowContext(ctx, insertJob, "job-late").Scan(&cursor)
		})
		lateDone <- insertResult{cursor: cursor, err: err}
	}()

	// The late insert must wait for the early transaction instead of
	// drawing a cursor and committing ahead of it.
	deadline := time.Now().Add(10 * time.Second)
	for {
		var waiting int
		if err := db.SQLDB().QueryRowContext(ctx, `
			SELECT count(*)
			FROM pg_stat_activity
			WHERE datname = current_database()
				AND wait_event_type = 'Lock' AND wait_event = 'advisory'
				AND query LIKE '%durable_search_jobs%'`).Scan(&waiting); err != nil {
			t.Fatalf("look for the waiting insert: %v", err)
		}
		if waiting == 1 {
			break
		}
		select {
		case result := <-lateDone:
			t.Fatalf("late insert committed (cursor %d, err %v) while an earlier insert was open with cursor %d",
				result.cursor, result.err, earlyCursor)
		default:
		}
		if time.Now().After(deadline) {
			t.Fatal("late insert never waited for the open early insert")
		}
		time.Sleep(20 * time.Millisecond)
	}
	var highWater int64
	if err := db.SQLDB().QueryRowContext(ctx,
		`SELECT COALESCE(MAX(row_cursor), 0) FROM durable_search_jobs`).Scan(&highWater); err != nil {
		t.Fatalf("read high water: %v", err)
	}
	if highWater != 0 {
		t.Fatalf("high water = %d while both inserts are uncommitted, want 0", highWater)
	}
	if err := early.Commit(); err != nil {
		t.Fatalf("commit early job: %v", err)
	}
	result := <-lateDone
	if result.err != nil {
		t.Fatalf("late insert: %v", result.err)
	}
	if result.cursor <= earlyCursor {
		t.Fatalf("late job committed second with cursor %d, not above the early job's %d", result.cursor, earlyCursor)
	}

	// A rolled-back inserter frees the lock, and the next one proceeds.
	aborted, err := db.SQLDB().BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin aborted transaction: %v", err)
	}
	var abortedCursor int64
	if err := aborted.QueryRowContext(ctx, insertJob, "job-aborted").Scan(&abortedCursor); err != nil {
		t.Fatalf("insert aborted job: %v", err)
	}
	if err := aborted.Rollback(); err != nil {
		t.Fatalf("roll back aborted job: %v", err)
	}
	var lastCursor int64
	if err := db.WriteTx(ctx, func(tx *sql.Tx) error {
		return tx.QueryRowContext(ctx, insertJob, "job-last").Scan(&lastCursor)
	}); err != nil {
		t.Fatalf("insert after a rollback: %v", err)
	}
	if lastCursor <= abortedCursor {
		t.Fatalf("cursor %d after a rolled-back %d; a cursor was reused", lastCursor, abortedCursor)
	}
}
