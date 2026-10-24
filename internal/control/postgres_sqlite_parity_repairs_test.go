package control_test

import (
	"context"
	"database/sql"
	"errors"
	"math/rand/v2"
	"strings"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/testsupport"
)

// Regression tests for migrations/postgres/0020_sqlite_parity_repairs.sql.
// The multi-row capacity repair is covered by the *_multi_row_* scenarios in
// testdata/postgres_triggers/knowledge_catalog_1.json and
// knowledge_objects_2.json.

const parityRepairsInsertJob = `
	INSERT INTO durable_search_jobs (id, tenant_id, owner_id, state, visibility, retention_class,
		lifetime_ns, job_payload, created_at_us, version)
	VALUES ($1, 't1', 'owner', 1, 1, 1, 1, '\x0a'::bytea, 1, 1)
	RETURNING row_cursor`

// TestPostgresDurableSearchJobCursorsFollowCommitOrder replays the
// interleaving that broke the artifact listing's high-water mark: job A is
// inserted in a transaction that stays open while job B is inserted by
// another writer. Under SQLite, B cannot commit before A does, so a reader
// never takes a high-water mark that A later commits below. PostgreSQL must
// hold B at the cursor lock, before it draws a cursor, until A commits.
func TestPostgresDurableSearchJobCursorsFollowCommitOrder(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	db := testsupport.NewPostgresControlDB(t, testsupport.WithPostgresTenants("t1"))
	pool := db.SQLDB()

	connA, err := pool.Conn(ctx)
	if err != nil {
		t.Fatalf("acquire connection for job A: %v", err)
	}
	defer func() {
		if err := connA.Close(); err != nil {
			t.Errorf("release job A connection: %v", err)
		}
	}()
	txA, err := connA.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		t.Fatalf("begin job A: %v", err)
	}
	committedA := false
	defer func() {
		if !committedA {
			if err := txA.Rollback(); err != nil && !errors.Is(err, sql.ErrTxDone) {
				t.Errorf("roll back job A: %v", err)
			}
		}
	}()
	var cursorA int64
	if err := txA.QueryRowContext(ctx, parityRepairsInsertJob, "job-a").Scan(&cursorA); err != nil {
		t.Fatalf("insert job A: %v", err)
	}

	type insertResult struct {
		cursor int64
		err    error
	}
	doneB := make(chan insertResult, 1)
	go func() {
		var cursor int64
		err := db.WriteTx(ctx, func(tx *sql.Tx) error {
			return tx.QueryRowContext(ctx, parityRepairsInsertJob, "job-b").Scan(&cursor)
		})
		doneB <- insertResult{cursor: cursor, err: err}
	}()

	// B must queue on the advisory cursor lock while A is open.
	waitCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	for blocked := false; !blocked; {
		select {
		case result := <-doneB:
			t.Fatalf("job B committed (cursor %d, err %v) while job A (cursor %d) was still open; "+
				"a reader could take a high-water mark that job A later commits below", result.cursor, result.err, cursorA)
		case <-waitCtx.Done():
			t.Fatal("job B never queued on the durable search job cursor lock")
		case <-time.After(10 * time.Millisecond):
		}
		if err := pool.QueryRowContext(ctx, `
			SELECT count(*) = 1
			FROM pg_catalog.pg_stat_activity
			WHERE datname = current_database()
			  AND wait_event_type = 'Lock'
			  AND wait_event = 'advisory'
			  AND query LIKE '%INSERT INTO durable_search_jobs%'`).Scan(&blocked); err != nil {
			t.Fatalf("look for job B's lock wait: %v", err)
		}
	}

	// B waits before its identity default runs, so it has drawn no cursor.
	var lastDrawn int64
	if err := pool.QueryRowContext(ctx, `
		SELECT pg_catalog.pg_sequence_last_value(
			pg_catalog.pg_get_serial_sequence('durable_search_jobs', 'row_cursor')::regclass)`).Scan(&lastDrawn); err != nil {
		t.Fatalf("read the row_cursor sequence: %v", err)
	}
	if lastDrawn != cursorA {
		t.Fatalf("row_cursor sequence is at %d while job B waits, want job A's %d: "+
			"B drew a cursor before it queued", lastDrawn, cursorA)
	}

	// The listing's first page: the high-water mark and the rows under it.
	var highWater, visible int64
	if err := pool.QueryRowContext(ctx,
		`SELECT COALESCE(MAX(row_cursor), 0), count(*) FROM durable_search_jobs`).Scan(&highWater, &visible); err != nil {
		t.Fatalf("read the high-water mark: %v", err)
	}
	if highWater != 0 || visible != 0 {
		t.Fatalf("high-water mark = %d over %d rows while job A is uncommitted, want 0 over 0", highWater, visible)
	}

	if err := txA.Commit(); err != nil {
		t.Fatalf("commit job A: %v", err)
	}
	committedA = true
	var resultB insertResult
	select {
	case resultB = <-doneB:
	case <-waitCtx.Done():
		t.Fatal("job B did not finish after job A committed")
	}
	if resultB.err != nil {
		t.Fatalf("insert job B: %v", resultB.err)
	}
	if resultB.cursor <= cursorA {
		t.Fatalf("job B committed after job A with cursor %d, want above job A's %d", resultB.cursor, cursorA)
	}

	// A later page under the first page's mark still holds nothing new.
	var underMark int64
	if err := pool.QueryRowContext(ctx,
		`SELECT count(*) FROM durable_search_jobs WHERE row_cursor <= $1`, highWater).Scan(&underMark); err != nil {
		t.Fatalf("count rows under the high-water mark: %v", err)
	}
	if underMark != visible {
		t.Fatalf("%d rows under high-water mark %d after both commits, the first page saw %d", underMark, highWater, visible)
	}
}

// TestPostgresDurableSearchJobCursorLockSpansTheTransaction checks that the
// cursor lock a job insert takes is the 0020 advisory key, is still held
// after the statement ends and is released at commit, and that one
// multi-row statement takes consecutive cursors above earlier ones.
func TestPostgresDurableSearchJobCursorLockSpansTheTransaction(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	db := testsupport.NewPostgresControlDB(t, testsupport.WithPostgresTenants("t1"))
	// pg_locks shows a bigint advisory key as classid (high 32 bits), objid
	// (low 32 bits) and objsubid 1: 0x6f73000000000002.
	const heldCursorLock = `
		SELECT count(*) FROM pg_catalog.pg_locks
		WHERE locktype = 'advisory' AND granted
		  AND database = (SELECT oid FROM pg_catalog.pg_database WHERE datname = current_database())
		  AND classid::bigint = 1869807616 AND objid::bigint = 2 AND objsubid = 1`
	var first, heldInside int64
	if err := db.WriteTx(ctx, func(tx *sql.Tx) error {
		if err := tx.QueryRowContext(ctx, parityRepairsInsertJob, "job-1").Scan(&first); err != nil {
			return err
		}
		return tx.QueryRowContext(ctx, heldCursorLock).Scan(&heldInside)
	}); err != nil {
		t.Fatalf("insert a job: %v", err)
	}
	if heldInside != 1 {
		t.Fatalf("cursor lock holders after the insert statement = %d, want 1 (held until commit)", heldInside)
	}
	var heldAfter int64
	if err := db.SQLDB().QueryRowContext(ctx, heldCursorLock).Scan(&heldAfter); err != nil {
		t.Fatalf("read cursor lock after commit: %v", err)
	}
	if heldAfter != 0 {
		t.Fatalf("cursor lock holders after commit = %d, want 0", heldAfter)
	}

	var low, high int64
	if err := db.WriteTx(ctx, func(tx *sql.Tx) error {
		return tx.QueryRowContext(ctx, `
			WITH inserted AS (
				INSERT INTO durable_search_jobs (id, tenant_id, owner_id, state, visibility, retention_class,
					lifetime_ns, job_payload, created_at_us, version)
				VALUES ('job-2', 't1', 'owner', 1, 1, 1, 1, '\x0a'::bytea, 1, 1),
				       ('job-3', 't1', 'owner', 1, 1, 1, 1, '\x0a'::bytea, 1, 1)
				RETURNING row_cursor)
			SELECT min(row_cursor), max(row_cursor) FROM inserted`).Scan(&low, &high)
	}); err != nil {
		t.Fatalf("insert two jobs in one statement: %v", err)
	}
	if low != first+1 || high != first+2 {
		t.Fatalf("multi-row insert cursors = %d..%d, want %d..%d", low, high, first+1, first+2)
	}
}

// parityRepairsText returns exactly bytes bytes of valid UTF-8 that pglz
// cannot compress: random supplementary-plane characters, padded with ASCII.
func parityRepairsText(random *rand.Rand, bytes int) string {
	var builder strings.Builder
	for range bytes / 4 {
		builder.WriteRune(rune(0x10000 + random.IntN(0x100000-0x10000)))
	}
	builder.WriteString(strings.Repeat("a", bytes%4))
	if builder.Len() != bytes {
		panic("parityRepairsText built the wrong length")
	}
	return builder.String()
}

// TestPostgresBtreeKeyBoundsRefuseBeforeTheIndexLimit drives each key-size
// CHECK from 0020 at its 2560-byte bound with incompressible values. The row
// at the bound must be stored, which shows the bound sits under the
// 2704-byte btree limit of every index it guards; one byte more must be
// refused by the named CHECK (23514), never by the index (54000). Foreign
// keys and triggers are off (session_replication_role = replica) so only
// the per-column CHECKs, the new CHECK and the indexes apply.
func TestPostgresBtreeKeyBoundsRefuseBeforeTheIndexLimit(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	db := testsupport.NewPostgresControlDB(t).SQLDB()
	random := rand.New(rand.NewPCG(20, 2560))
	text := func(bytes int) string { return parityRepairsText(random, bytes) }

	type keyRow struct {
		description string
		args        func() []any
	}
	cases := []struct {
		constraint string
		insert     string
		atBound    keyRow
		overBound  keyRow
	}{
		{
			constraint: "dashboards_owner_app_name_key_bytes_bounded",
			insert: `INSERT INTO dashboards (dashboard_id, version, tenant_id, owner_id, app_id, name,
				sharing_scope, definition_proto, created_at_unix_micro, updated_at_unix_micro)
				VALUES ($1, 1, $2, $3, $4, $5, 1, '\x0a'::bytea, 1, 1)`,
			atBound: keyRow{"tenant 2 + owner 1020 + app 1020 + name 518", func() []any {
				return []any{"d-at", "t1", text(1020), text(1020), text(518)}
			}},
			overBound: keyRow{"name 519", func() []any {
				return []any{"d-over", "t1", text(1020), text(1020), text(519)}
			}},
		},
		{
			constraint: "dashboards_owner_app_name_key_bytes_bounded",
			insert: `INSERT INTO dashboards (dashboard_id, version, tenant_id, owner_id, app_id, name,
				sharing_scope, definition_proto, created_at_unix_micro, updated_at_unix_micro)
				VALUES ($1, 1, $2, $3, $4, $5, 1, '\x0a'::bytea, 1, 1)`,
			atBound: keyRow{"tenant 2 + owner 1020 + app 518 + name 1020", func() []any {
				return []any{"d-at-2", "t1", text(1020), text(518), text(1020)}
			}},
			// The reviewed reproduction: three 255-character columns of
			// 4-byte characters, which raised 54000 before 0020.
			overBound: keyRow{"owner, app and name of 255 four-byte characters", func() []any {
				return []any{"d-over-2", "t1", text(1020), text(1020), text(1020)}
			}},
		},
		{
			constraint: "saved_searches_owner_app_name_key_bytes_bounded",
			insert: `INSERT INTO saved_searches (saved_search_id, tenant_id, version, owner_id, app_id, name,
				sharing_scope, definition_proto, created_at_unix_micro, updated_at_unix_micro)
				VALUES ($1, $2, 1, $3, $4, $5, 1, '\x0a'::bytea, 1, 1)`,
			atBound: keyRow{"tenant 2 + owner 1020 + app 1020 + name 518", func() []any {
				return []any{"s-at", "t1", text(1020), text(1020), text(518)}
			}},
			overBound: keyRow{"owner, app and name of 255 four-byte characters", func() []any {
				return []any{"s-over", "t1", text(1020), text(1020), text(1020)}
			}},
		},
		{
			constraint: "search_history_owner_app_key_bytes_bounded",
			insert: `INSERT INTO search_history (search_job_id, tenant_id, owner_id, app_id, saved_search_id,
				final_state, search_text, created_at_unix_micro, finished_at_unix_micro,
				duration_nanoseconds, matched_events, entry_proto, entry_sha256)
				VALUES ($1, $2, $3, $4, $5, 6, 'search', 1, 1, 0, 0, '\x0a'::bytea, decode(repeat('ab', 32), 'hex'))`,
			atBound: keyRow{"tenant 2 + owner 1020 + app 1020 + job 518", func() []any {
				return []any{text(518), "t1", text(1020), text(1020), ""}
			}},
			overBound: keyRow{"job 519", func() []any {
				return []any{text(519), "t1", text(1020), text(1020), ""}
			}},
		},
		{
			constraint: "search_history_owner_saved_key_bytes_bounded",
			insert: `INSERT INTO search_history (search_job_id, tenant_id, owner_id, app_id, saved_search_id,
				final_state, search_text, created_at_unix_micro, finished_at_unix_micro,
				duration_nanoseconds, matched_events, entry_proto, entry_sha256)
				VALUES ($1, $2, $3, $4, $5, 6, 'search', 1, 1, 0, 0, '\x0a'::bytea, decode(repeat('ab', 32), 'hex'))`,
			atBound: keyRow{"tenant 4 + owner 1020 + saved 512 + job 1024", func() []any {
				return []any{text(1024), "t1ab", text(1020), "", text(512)}
			}},
			overBound: keyRow{"tenant 5", func() []any {
				return []any{text(1024), "t1abc", text(1020), "", text(512)}
			}},
		},
		{
			constraint: "knowledge_lookup_definitions_resolution_key_bytes_bounded",
			insert: `INSERT INTO knowledge_lookup_definitions (tenant_id, lookup_id, owner_id, app_id, name,
				sharing_scope, automatic, current_version, state, created_at_unix_micro, updated_at_unix_micro)
				VALUES ($1, $2, $3, $4, $5, 1, 0, 1, 'ACTIVE', 1, 1)`,
			atBound: keyRow{"tenant 2 + state 6 + name 1016 + app 512 + owner 1020 + lookup 4", func() []any {
				return []any{"t1", text(4), text(1020), text(512), text(1016)}
			}},
			overBound: keyRow{"lookup 5", func() []any {
				return []any{"t1", text(5), text(1020), text(512), text(1016)}
			}},
		},
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer func() {
		if err := tx.Rollback(); err != nil {
			t.Errorf("roll back: %v", err)
		}
	}()
	if _, err := tx.ExecContext(ctx, `SET LOCAL session_replication_role = replica`); err != nil {
		t.Fatalf("disable triggers and foreign keys: %v", err)
	}
	for _, tc := range cases {
		if _, err := tx.ExecContext(ctx, `SAVEPOINT row_at_bound`); err != nil {
			t.Fatalf("savepoint: %v", err)
		}
		if _, err := tx.ExecContext(ctx, tc.insert, tc.atBound.args()...); err != nil {
			t.Errorf("%s: row at the bound (%s) = %v, want stored", tc.constraint, tc.atBound.description, err)
		}
		if _, err := tx.ExecContext(ctx, `ROLLBACK TO SAVEPOINT row_at_bound`); err != nil {
			t.Fatalf("roll back the stored row: %v", err)
		}

		if _, err := tx.ExecContext(ctx, `SAVEPOINT row_over_bound`); err != nil {
			t.Fatalf("savepoint: %v", err)
		}
		_, err := tx.ExecContext(ctx, tc.insert, tc.overBound.args()...)
		code, _ := control.PostgresErrorCode(err)
		constraint, _ := control.PostgresConstraintName(err)
		if code != "23514" || constraint != tc.constraint {
			t.Errorf("%s: row one byte over (%s) = %v (SQLSTATE %q, constraint %q), want 23514 on %s",
				tc.constraint, tc.overBound.description, err, code, constraint, tc.constraint)
		}
		if _, err := tx.ExecContext(ctx, `ROLLBACK TO SAVEPOINT row_over_bound`); err != nil {
			t.Fatalf("roll back the refused row: %v", err)
		}
	}
}
