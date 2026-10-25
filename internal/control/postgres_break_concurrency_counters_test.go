package control_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/testsupport"
)

// These tests try to break the trigger-maintained counters, journals and
// singletons of the PostgreSQL control plane under real contention. Every
// writer goes through WriteTx (SERIALIZABLE with bounded retry), the only
// supported write entry point, and all of them start together behind a
// barrier so their snapshots overlap.

const (
	breakCountersWorkers   = 16
	breakCountersTimeout   = 2 * time.Minute
	breakCountersTenantOne = "t1"
	breakCountersTenantTwo = "t2"
)

func newBreakCountersDB(t *testing.T) *control.PostgresDB {
	t.Helper()
	return testsupport.NewPostgresControlDB(t)
}

func breakCountersContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), breakCountersTimeout)
	t.Cleanup(cancel)
	return ctx
}

func breakCountersExec(t *testing.T, db *control.PostgresDB, statements ...string) {
	t.Helper()
	ctx := breakCountersContext(t)
	for _, statement := range statements {
		if _, err := db.SQLDB().ExecContext(ctx, statement); err != nil {
			t.Fatalf("exec %q: %v", statement, err)
		}
	}
}

func breakCountersTenants(t *testing.T, db *control.PostgresDB, tenants ...string) {
	t.Helper()
	for _, tenant := range tenants {
		breakCountersExec(t, db, fmt.Sprintf(
			"INSERT INTO tenants (tenant_id, created_at_unix_micro, updated_at_unix_micro) VALUES ('%s', 1, 1)",
			tenant,
		))
	}
}

func breakCountersInt64(t *testing.T, db *control.PostgresDB, query string, args ...any) int64 {
	t.Helper()
	var value int64
	if err := db.SQLDB().QueryRowContext(breakCountersContext(t), query, args...).Scan(&value); err != nil {
		t.Fatalf("query %q: %v", query, err)
	}
	return value
}

func breakCountersInt64s(t *testing.T, db *control.PostgresDB, query string, args ...any) []int64 {
	t.Helper()
	rows, err := db.SQLDB().QueryContext(breakCountersContext(t), query, args...)
	if err != nil {
		t.Fatalf("query %q: %v", query, err)
	}
	defer func() {
		if err := rows.Close(); err != nil {
			t.Errorf("close rows of %q: %v", query, err)
		}
	}()
	var values []int64
	for rows.Next() {
		var value int64
		if err := rows.Scan(&value); err != nil {
			t.Fatalf("scan %q: %v", query, err)
		}
		values = append(values, value)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate %q: %v", query, err)
	}
	return values
}

// runBreakCountersContended runs work(worker, round) for each of the
// breakCountersWorkers workers and every round. All workers start together,
// and every error is reported with the worker and round that produced it.
func runBreakCountersContended(
	t *testing.T,
	rounds int,
	work func(ctx context.Context, worker, round int) error,
) {
	t.Helper()
	ctx := breakCountersContext(t)
	start := make(chan struct{})
	var group sync.WaitGroup
	var mu sync.Mutex
	var failures []string
	for worker := range breakCountersWorkers {
		group.Go(func() {
			<-start
			for round := range rounds {
				if err := work(ctx, worker, round); err != nil {
					mu.Lock()
					failures = append(failures, fmt.Sprintf(
						"worker %d round %d: %v (retries exhausted %t, sqlstate %s)",
						worker,
						round,
						err,
						errors.Is(err, control.ErrTransactionRetriesExhausted),
						breakCountersSQLState(err),
					))
					mu.Unlock()
				}
			}
		})
	}
	close(start)
	group.Wait()
	for _, failure := range failures {
		t.Error(failure)
	}
	if len(failures) > 0 {
		t.FailNow()
	}
}

func breakCountersSQLState(err error) string {
	code, ok := control.PostgresErrorCode(err)
	if !ok {
		return "none"
	}
	return code
}

// requireBreakCountersDense fails unless values is exactly first..last.
func requireBreakCountersDense(t *testing.T, label string, values []int64, first, last int64) {
	t.Helper()
	if int64(len(values)) != last-first+1 {
		t.Fatalf("%s: got %d values, want %d (%d..%d)", label, len(values), last-first+1, first, last)
	}
	for offset, value := range values {
		if value != first+int64(offset) {
			t.Fatalf("%s: value %d is %d, want %d", label, offset, value, first+int64(offset))
		}
	}
}

// breakCountersJournal is one dense, trigger-accounted audit journal. The
// append reads next_sequence the way the Go stores do and inserts exactly
// that sequence; the AFTER INSERT trigger advances the tenant state.
type breakCountersJournal struct {
	name       string
	stateTable string
	stateSetup string // %[1]s is the tenant
	insert     string // $1 tenant, $2 sequence, $3 occurred_at, $4 unique text
	// retainedWindow is the rolling-prune window, or 0 for a journal that
	// keeps every event.
	retainedWindow int64
}

var breakCountersJournals = []breakCountersJournal{
	{
		name:       "audit_events",
		stateTable: "audit_tenant_state",
		stateSetup: "INSERT INTO audit_tenant_state (tenant_id, next_sequence, event_count) VALUES ('%[1]s', 1, 0)",
		insert: "INSERT INTO audit_events (tenant_id, sequence, occurred_at_unix_micro, actor_kind, actor_id, actor_role, " +
			"action, target_kind, target_id, target_version, app_id, object_type, sharing_scope) " +
			"VALUES ($1, $2, $3, 'system', 'system', 'system', 'index.create', 'index', $4, 1, NULL, NULL, NULL)",
	},
	{
		name:       "feature_operation_audit_events",
		stateTable: "feature_operation_audit_tenant_state",
		stateSetup: "INSERT INTO feature_operation_audit_tenant_state (tenant_id, first_sequence, next_sequence, " +
			"retained_count, last_occurred_at_unix_micro) VALUES ('%[1]s', 1, 1, 0, NULL)",
		insert: "INSERT INTO feature_operation_audit_events (tenant_id, sequence, occurred_at_unix_micro, feature, " +
			"operation, outcome, items, bytes) VALUES ($1, $2, $3, 1, 2, 3, octet_length($4::text), 5)",
	},
	{
		name:       "knowledge_attempt_audit_events",
		stateTable: "knowledge_attempt_audit_tenant_state",
		stateSetup: "INSERT INTO knowledge_attempt_audit_tenant_state (tenant_id, first_sequence, next_sequence, " +
			"retained_count) VALUES ('%[1]s', 1, 1, 0)",
		insert: "INSERT INTO knowledge_attempt_audit_events (tenant_id, sequence, occurred_at_unix_micro, actor_kind, " +
			"actor_id, actor_role, action, result, reason, app_id, knowledge_object_id, object_type, object_version, " +
			"sharing_scope) VALUES ($1, $2, $3, 'browser', $4, 'administrator', 'create', 'rejected', " +
			"'invalid_definition', NULL, NULL, NULL, NULL, NULL)",
	},
	{
		// A window far below the number of appends makes every append after
		// the fifth also prune, inside the same AFTER INSERT trigger.
		name:       "search_attempt_audit_events",
		stateTable: "search_attempt_audit_tenant_state",
		stateSetup: "INSERT INTO search_attempt_audit_tenant_state (tenant_id, first_sequence, next_sequence, " +
			"retained_count, maximum_retained_attempts) VALUES ('%[1]s', 1, 1, 0, 5)",
		insert: "INSERT INTO search_attempt_audit_events (tenant_id, sequence, occurred_at_unix_micro, actor_kind, " +
			"actor_id, actor_role, owner_id, search_job_id, knowledge_snapshot_sha256, " +
			"knowledge_snapshot_tenant_catalog_revision, knowledge_snapshot_tenant_catalog_state_token, " +
			"knowledge_snapshot_object_count, knowledge_snapshot_lookup_asset_count) " +
			"VALUES ($1, $2, $3, 'browser', 'alice', 'user', 'alice', $4, NULL, NULL, NULL, NULL, NULL)",
		retainedWindow: 5,
	},
}

// appendOne is one WriteTx: read next_sequence, insert it, and confirm the
// trigger advanced the state by exactly one. It returns the sequence.
func (journal breakCountersJournal) appendOne(
	ctx context.Context,
	db *control.PostgresDB,
	tenant string,
	unique string,
) (int64, error) {
	var appended int64
	stateQuery := "SELECT next_sequence FROM " + journal.stateTable + " WHERE tenant_id = $1"
	err := db.WriteTx(ctx, func(tx *sql.Tx) error {
		var next int64
		if err := tx.QueryRowContext(ctx, stateQuery, tenant).Scan(&next); err != nil {
			return fmt.Errorf("read next_sequence: %w", err)
		}
		// occurred_at grows with the sequence, so the feature-operation
		// journal's monotonic timestamp guard never rejects a winner.
		if _, err := tx.ExecContext(ctx, journal.insert, tenant, next, 1_000_000+next, unique); err != nil {
			return fmt.Errorf("insert sequence %d: %w", next, err)
		}
		var advanced int64
		if err := tx.QueryRowContext(ctx, stateQuery, tenant).Scan(&advanced); err != nil {
			return fmt.Errorf("re-read next_sequence: %w", err)
		}
		if advanced != next+1 {
			return fmt.Errorf("next_sequence advanced from %d to %d", next, advanced)
		}
		appended = next
		return nil
	})
	return appended, err
}

// TestPostgresBreakConcurrencyAuditJournalsStayDense appends from sixteen
// writers at once, split over two tenants. Every append must succeed
// through WriteTx's retry, and each journal must end dense, with its state
// row matching the rows that remain.
func TestPostgresBreakConcurrencyAuditJournalsStayDense(t *testing.T) {
	t.Parallel()
	const rounds = 6
	for _, journal := range breakCountersJournals {
		t.Run(journal.name, func(t *testing.T) {
			t.Parallel()
			db := newBreakCountersDB(t)
			tenants := []string{breakCountersTenantOne, breakCountersTenantTwo}
			breakCountersTenants(t, db, tenants...)
			for _, tenant := range tenants {
				breakCountersExec(t, db, fmt.Sprintf(journal.stateSetup, tenant))
			}

			var mu sync.Mutex
			appended := make(map[string][]int64)
			runBreakCountersContended(t, rounds, func(ctx context.Context, worker, round int) error {
				tenant := tenants[worker%len(tenants)]
				sequence, err := journal.appendOne(ctx, db, tenant, fmt.Sprintf("w%d-r%d", worker, round))
				if err != nil {
					return err
				}
				mu.Lock()
				appended[tenant] = append(appended[tenant], sequence)
				mu.Unlock()
				return nil
			})

			total := int64(breakCountersWorkers / len(tenants) * rounds)
			for _, tenant := range tenants {
				seen := make(map[int64]bool)
				for _, sequence := range appended[tenant] {
					if seen[sequence] {
						t.Fatalf("%s: two committed appends returned sequence %d", tenant, sequence)
					}
					seen[sequence] = true
				}
				if int64(len(seen)) != total {
					t.Fatalf("%s: %d appends committed, want %d", tenant, len(seen), total)
				}
				first := int64(1)
				if journal.retainedWindow > 0 {
					first = total - journal.retainedWindow + 1
				}
				requireBreakCountersDense(t, tenant+" "+journal.name, breakCountersInt64s(t, db,
					"SELECT sequence FROM "+journal.name+" WHERE tenant_id = $1 ORDER BY sequence", tenant,
				), first, total)
				if next := breakCountersInt64(t, db,
					"SELECT next_sequence FROM "+journal.stateTable+" WHERE tenant_id = $1", tenant,
				); next != total+1 {
					t.Fatalf("%s: next_sequence is %d, want %d", tenant, next, total+1)
				}
			}
		})
	}
}

// errBreakCountersRollback makes a WriteTx callback roll back on purpose.
var errBreakCountersRollback = errors.New("deliberate rollback")

// TestPostgresBreakConcurrencyHotRowWritersNeverExhaustRetries is the
// fixture counter of TestPostgresWriteTxRetriesRealSerializationConflicts
// at sixteen writers, with no tolerance: before WriteTx queued writers
// behind a retrying transaction, every waiter on the row aborted once per
// commit and the tail writers lost all PostgresTxMaxAttempts attempts.
func TestPostgresBreakConcurrencyHotRowWritersNeverExhaustRetries(t *testing.T) {
	t.Parallel()
	db := newPostgresFixtureDB(t)
	const increments = 12
	runBreakCountersContended(t, increments, func(ctx context.Context, _, _ int) error {
		return db.WriteTx(ctx, func(tx *sql.Tx) error {
			var value int64
			if err := tx.QueryRowContext(ctx, `SELECT value FROM counters WHERE name = 'shared'`).Scan(&value); err != nil {
				return err
			}
			_, err := tx.ExecContext(ctx, `UPDATE counters SET value = $1 WHERE name = 'shared'`, value+1)
			return err
		})
	})
	if value := breakCountersInt64(t, db, `SELECT value FROM counters WHERE name = 'shared'`); value != breakCountersWorkers*increments {
		t.Fatalf("counter = %d, want %d", value, breakCountersWorkers*increments)
	}
}

// TestPostgresBreakConcurrencyWritesQueueBehindARetry holds a transaction in
// its retry and checks that a new write waits for it instead of running
// beside it, that the wait honors the context, and that a read does not
// wait at all.
func TestPostgresBreakConcurrencyWritesQueueBehindARetry(t *testing.T) {
	t.Parallel()
	db := newPostgresFixtureDB(t)
	ctx := breakCountersContext(t)

	inRetry := make(chan struct{})
	release := make(chan struct{})
	retried := make(chan error, 1)
	go func() {
		attempt := 0
		retried <- db.WriteTx(ctx, func(tx *sql.Tx) error {
			attempt++
			if attempt == 1 {
				return forceSQLState(ctx, tx, "40001")
			}
			close(inRetry)
			<-release
			return nil
		})
	}()
	<-inRetry

	waitCtx, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
	defer cancel()
	ran := false
	err := db.WriteTx(waitCtx, func(*sql.Tx) error {
		ran = true
		return nil
	})
	if !errors.Is(err, context.DeadlineExceeded) || ran {
		t.Fatalf("write during another's retry = %v (callback ran %t), want it to wait for the deadline", err, ran)
	}
	if err := db.ReadTx(ctx, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `SELECT 1`)
		return err
	}); err != nil {
		t.Fatalf("read during a retry: %v", err)
	}

	close(release)
	if err := <-retried; err != nil {
		t.Fatalf("retried write: %v", err)
	}
	// With nothing retrying, a write runs without waiting.
	if err := db.WriteTx(ctx, func(*sql.Tx) error { return nil }); err != nil {
		t.Fatalf("write after the retry finished: %v", err)
	}
}

const breakCountersSHA = `'\x2222222222222222222222222222222222222222222222222222222222222222'::bytea`

// TestPostgresBreakConcurrencyVisibilityAllocationIsContiguous allocates
// visibility sequences from the ingest_visibility_state singleton from
// sixteen writers at once. Some allocations roll back on purpose and must
// leave no gap; the committed ranges must tile 1..last_assigned exactly, and
// committed_through, advanced concurrently, must never pass last_assigned.
func TestPostgresBreakConcurrencyVisibilityAllocationIsContiguous(t *testing.T) {
	t.Parallel()
	db := newBreakCountersDB(t)
	const rounds = 8

	type allocation struct{ first, last int64 }
	var mu sync.Mutex
	var committed []allocation
	var committedThrough []int64
	runBreakCountersContended(t, rounds, func(ctx context.Context, worker, round int) error {
		if worker == 0 {
			// The committer publishes everything assigned so far.
			var through int64
			err := db.WriteTx(ctx, func(tx *sql.Tx) error {
				return tx.QueryRowContext(ctx,
					"UPDATE ingest_visibility_state SET committed_through = last_assigned "+
						"WHERE singleton = 1 RETURNING committed_through",
				).Scan(&through)
			})
			if err == nil {
				mu.Lock()
				committedThrough = append(committedThrough, through)
				mu.Unlock()
			}
			return err
		}
		size := int64(1 + (worker+round)%4)
		rollBack := (worker+round)%5 == 0
		batch := fmt.Sprintf("batch-w%d-r%d", worker, round)
		var got allocation
		err := db.WriteTx(ctx, func(tx *sql.Tx) error {
			var last int64
			if err := tx.QueryRowContext(ctx,
				"UPDATE ingest_visibility_state SET last_assigned = last_assigned + $1 "+
					"WHERE singleton = 1 RETURNING last_assigned", size,
			).Scan(&last); err != nil {
				return fmt.Errorf("allocate: %w", err)
			}
			first := last - size + 1
			// One batch per sequence: a batch holds one active reservation.
			if _, err := tx.ExecContext(ctx,
				"INSERT INTO ingest_batch_identities (batch_key, sequence_key, payload_sha256, "+
					"first_visibility_seq, created_at_unix_micro) SELECT $1 || '-' || s, $1 || '-' || s, "+
					breakCountersSHA+", s, 1000 FROM generate_series($2::bigint, $3::bigint) AS s",
				batch, first, last,
			); err != nil {
				return fmt.Errorf("insert batch identities: %w", err)
			}
			if _, err := tx.ExecContext(ctx,
				"INSERT INTO ingest_visibility_reservations (sequence, batch_key, state, phase, attempt_id, "+
					"index_time_unix_milli, metadata, outbox, created_at_unix_micro, outbox_sha256, "+
					"stored_row_count, decoded_event_bytes) "+
					"SELECT s, $1 || '-' || s, 'reserved', 'unsent', '', 1, '\\x6d'::bytea, '\\x6f'::bytea, 1000, "+
					breakCountersSHA+", 1, 1 FROM generate_series($2::bigint, $3::bigint) AS s",
				batch, first, last,
			); err != nil {
				return fmt.Errorf("insert reservations %d..%d: %w", first, last, err)
			}
			got = allocation{first: first, last: last}
			if rollBack {
				return errBreakCountersRollback
			}
			return nil
		})
		if rollBack && errors.Is(err, errBreakCountersRollback) {
			return nil
		}
		if err != nil {
			return err
		}
		mu.Lock()
		committed = append(committed, got)
		mu.Unlock()
		return nil
	})

	last := breakCountersInt64(t, db, "SELECT last_assigned FROM ingest_visibility_state WHERE singleton = 1")
	covered := make(map[int64]bool)
	var total int64
	for _, allocation := range committed {
		for sequence := allocation.first; sequence <= allocation.last; sequence++ {
			if covered[sequence] {
				t.Fatalf("sequence %d was committed by two allocations", sequence)
			}
			covered[sequence] = true
		}
		total += allocation.last - allocation.first + 1
	}
	if total != last {
		t.Fatalf("committed allocations cover %d sequences, last_assigned is %d", total, last)
	}
	requireBreakCountersDense(t, "reservations", breakCountersInt64s(t, db,
		"SELECT sequence FROM ingest_visibility_reservations ORDER BY sequence",
	), 1, last)
	for index := 1; index < len(committedThrough); index++ {
		if committedThrough[index] < 0 || committedThrough[index] > last {
			t.Fatalf("committed_through %d is outside 0..%d", committedThrough[index], last)
		}
	}
}

// TestPostgresBreakConcurrencyAppCatalogRevision creates the first apps of a
// fresh tenant from sixteen writers at once, then updates them. The revision
// authority must be created exactly once, provision the knowledge catalog
// exactly once, and count every app row change.
func TestPostgresBreakConcurrencyAppCatalogRevision(t *testing.T) {
	t.Parallel()
	db := newBreakCountersDB(t)
	breakCountersTenants(t, db, breakCountersTenantOne)
	const updates = 3

	appID := func(worker int) string { return fmt.Sprintf("app_%021dA", worker) }
	runBreakCountersContended(t, 1+updates, func(ctx context.Context, worker, round int) error {
		return db.WriteTx(ctx, func(tx *sql.Tx) error {
			if round == 0 {
				_, err := tx.ExecContext(ctx,
					"INSERT INTO app_workspaces (app_id, tenant_id, version, slug, display_name, "+
						"default_time_range_present, state, created_at_unix_micro, updated_at_unix_micro, "+
						"archived_at_unix_micro) VALUES ($1, $2, 1, $3, 'App', 0, 'active', 1, 1, NULL)",
					appID(worker), breakCountersTenantOne, fmt.Sprintf("slug-%d", worker),
				)
				return err
			}
			_, err := tx.ExecContext(ctx,
				"UPDATE app_workspaces SET version = version + 1, updated_at_unix_micro = updated_at_unix_micro + 1, "+
					"display_name = $2 WHERE app_id = $1",
				appID(worker), fmt.Sprintf("App %d", round),
			)
			return err
		})
	})

	changes := int64(breakCountersWorkers * (1 + updates))
	if revision := breakCountersInt64(t, db,
		"SELECT revision FROM app_catalog_revisions WHERE tenant_id = $1", breakCountersTenantOne,
	); revision != changes {
		t.Fatalf("app catalog revision is %d, want %d", revision, changes)
	}
	for _, table := range []string{
		"app_catalog_revisions",
		"knowledge_catalog_tenants",
		"knowledge_catalog_revision_heads",
		"knowledge_projection_tenant_ledgers",
		"knowledge_lookup_asset_tenant_ledgers",
	} {
		if count := breakCountersInt64(t, db,
			"SELECT count(*) FROM "+table+" WHERE tenant_id = $1", breakCountersTenantOne,
		); count != 1 {
			t.Fatalf("%s has %d rows for the tenant, want 1", table, count)
		}
	}
}

// TestPostgresBreakConcurrencyCollectorCatalogRevision registers, updates
// and deletes collectors of a fresh tenant from sixteen writers at once.
func TestPostgresBreakConcurrencyCollectorCatalogRevision(t *testing.T) {
	t.Parallel()
	db := newBreakCountersDB(t)
	breakCountersTenants(t, db, breakCountersTenantOne)

	// round 0 inserts, rounds 1-2 update, round 3 deletes odd workers.
	runBreakCountersContended(t, 4, func(ctx context.Context, worker, round int) error {
		collector := fmt.Sprintf("c%d", worker)
		return db.WriteTx(ctx, func(tx *sql.Tx) error {
			var err error
			switch round {
			case 0:
				_, err = tx.ExecContext(ctx,
					"INSERT INTO collector_fleet (tenant_id, collector_id, admin_version, display_name, "+
						"administrative_state, first_seen_at_unix_micro, updated_at_unix_micro) "+
						"VALUES ($1, $2, 1, NULL, 'enabled', 1, 1)",
					breakCountersTenantOne, collector,
				)
			case 3:
				if worker%2 == 1 {
					_, err = tx.ExecContext(ctx,
						"DELETE FROM collector_fleet WHERE tenant_id = $1 AND collector_id = $2",
						breakCountersTenantOne, collector,
					)
				}
			default:
				_, err = tx.ExecContext(ctx,
					"UPDATE collector_fleet SET admin_version = admin_version + 1, "+
						"updated_at_unix_micro = updated_at_unix_micro + 1 WHERE tenant_id = $1 AND collector_id = $2",
					breakCountersTenantOne, collector,
				)
			}
			return err
		})
	})

	var revision, fleet, runtime int64
	if err := db.SQLDB().QueryRowContext(breakCountersContext(t),
		"SELECT revision, fleet_count, runtime_count FROM collector_catalog_revisions WHERE tenant_id = $1",
		breakCountersTenantOne,
	).Scan(&revision, &fleet, &runtime); err != nil {
		t.Fatalf("read collector catalog revision: %v", err)
	}
	wantRevision := int64(breakCountersWorkers*3 + breakCountersWorkers/2)
	wantFleet := breakCountersInt64(t, db, "SELECT count(*) FROM collector_fleet WHERE tenant_id = $1", breakCountersTenantOne)
	if revision != wantRevision || fleet != wantFleet || fleet != breakCountersWorkers/2 || runtime != 0 {
		t.Fatalf("collector catalog is revision %d fleet %d runtime %d, want %d %d 0",
			revision, fleet, runtime, wantRevision, breakCountersWorkers/2)
	}
}

// TestPostgresBreakConcurrencyIndexCatalogState creates and updates indexes
// across four tenants from sixteen writers. index_catalog_state is one row
// shared by every tenant, so every index write contends on it.
func TestPostgresBreakConcurrencyIndexCatalogState(t *testing.T) {
	t.Parallel()
	db := newBreakCountersDB(t)
	tenants := []string{"ta", "tb", "tc", "td"}
	breakCountersTenants(t, db, tenants...)
	const indexesPerWorker = 3

	runBreakCountersContended(t, 2*indexesPerWorker, func(ctx context.Context, worker, round int) error {
		tenant := tenants[worker%len(tenants)]
		index := fmt.Sprintf("idx-w%d-%d", worker, round%indexesPerWorker)
		return db.WriteTx(ctx, func(tx *sql.Tx) error {
			if round < indexesPerWorker {
				_, err := tx.ExecContext(ctx,
					"INSERT INTO indexes (tenant_id, index_id, version, name, display_name, ingestion_enabled, "+
						"search_enabled, state, created_at_unix_micro, updated_at_unix_micro) "+
						"VALUES ($1, $2, 1, $2, 'Index', 1, 1, 'active', 1, 1)",
					tenant, index,
				)
				return err
			}
			_, err := tx.ExecContext(ctx,
				"UPDATE indexes SET version = version + 1, updated_at_unix_micro = updated_at_unix_micro + 1, "+
					"display_name = 'Updated' WHERE index_id = $1",
				index,
			)
			return err
		})
	})

	created := int64(breakCountersWorkers * indexesPerWorker)
	var revision, physical int64
	if err := db.SQLDB().QueryRowContext(breakCountersContext(t),
		"SELECT revision, physical_count FROM index_catalog_state WHERE singleton_id = 1",
	).Scan(&revision, &physical); err != nil {
		t.Fatalf("read index catalog state: %v", err)
	}
	if revision != 1+2*created || physical != created {
		t.Fatalf("index catalog is revision %d physical %d, want %d %d", revision, physical, 1+2*created, created)
	}
}

const breakCountersHistoryInsert = "INSERT INTO search_history (search_job_id, tenant_id, owner_id, app_id, " +
	"saved_search_id, final_state, search_text, created_at_unix_micro, finished_at_unix_micro, " +
	"duration_nanoseconds, matched_events, entry_proto, entry_sha256) " +
	"SELECT $1 || '-' || n, $2, $3, 'search', '', 6, 'index=main', 1, 1, 0, 0, '\\x00'::bytea, " +
	breakCountersSHA + " FROM generate_series(1, $4::bigint) AS n"

// TestPostgresBreakConcurrencySearchHistoryOwnerCounts inserts and deletes
// terminal history for four owners from sixteen writers, with single-row and
// multi-row statements. The owner counter must equal the rows that remain
// and vanish exactly when the owner has none.
func TestPostgresBreakConcurrencySearchHistoryOwnerCounts(t *testing.T) {
	t.Parallel()
	db := newBreakCountersDB(t)
	breakCountersTenants(t, db, breakCountersTenantOne)
	owners := []string{"o0", "o1", "o2", "o3"}

	// Round 0 inserts five rows in one statement (the first writers of an
	// owner race to create its counter), round 1 one row, round 2 deletes
	// a worker's rows: even workers in one statement, odd workers one by
	// one, and workers of owner o3 delete nothing.
	runBreakCountersContended(t, 3, func(ctx context.Context, worker, round int) error {
		owner := owners[worker%len(owners)]
		prefix := fmt.Sprintf("job-w%d-r%d", worker, round)
		return db.WriteTx(ctx, func(tx *sql.Tx) error {
			switch round {
			case 0:
				_, err := tx.ExecContext(ctx, breakCountersHistoryInsert, prefix, breakCountersTenantOne, owner, 5)
				return err
			case 1:
				_, err := tx.ExecContext(ctx, breakCountersHistoryInsert, prefix, breakCountersTenantOne, owner, 1)
				return err
			}
			if owner == "o3" {
				return nil
			}
			pattern := fmt.Sprintf("job-w%d-%%", worker)
			if worker%2 == 0 {
				_, err := tx.ExecContext(ctx, "DELETE FROM search_history WHERE search_job_id LIKE $1", pattern)
				return err
			}
			rows, err := tx.QueryContext(ctx,
				"SELECT search_job_id FROM search_history WHERE search_job_id LIKE $1 ORDER BY 1", pattern)
			if err != nil {
				return err
			}
			var ids []string
			for rows.Next() {
				var id string
				if err := rows.Scan(&id); err != nil {
					return errors.Join(err, rows.Close())
				}
				ids = append(ids, id)
			}
			if err := errors.Join(rows.Err(), rows.Close()); err != nil {
				return err
			}
			for _, id := range ids {
				if _, err := tx.ExecContext(ctx, "DELETE FROM search_history WHERE search_job_id = $1", id); err != nil {
					return err
				}
			}
			return nil
		})
	})

	perOwner := int64(breakCountersWorkers / len(owners) * 6)
	for _, owner := range owners {
		rows := breakCountersInt64(t, db,
			"SELECT count(*) FROM search_history WHERE tenant_id = $1 AND owner_id = $2", breakCountersTenantOne, owner)
		counters := breakCountersInt64s(t, db,
			"SELECT terminal_count FROM search_history_owner_counts WHERE tenant_id = $1 AND owner_id = $2",
			breakCountersTenantOne, owner)
		want := int64(0)
		if owner == "o3" {
			want = perOwner
		}
		if rows != want {
			t.Fatalf("owner %s has %d history rows, want %d", owner, rows, want)
		}
		switch {
		case want == 0 && len(counters) != 0:
			t.Fatalf("owner %s has no history but keeps counter %v", owner, counters)
		case want > 0 && (len(counters) != 1 || counters[0] != want):
			t.Fatalf("owner %s counter is %v, want [%d]", owner, counters, want)
		}
	}
}

func breakCountersReceiptBytes(tenant, actor, route, requestID, targetKind, targetID string) int {
	return 96 + len(tenant) + len("system") + len(actor) + len(route) + len(requestID) + 32 +
		len(targetKind) + len(targetID)
}

// TestPostgresBreakConcurrencyReceiptLedger inserts API mutation receipts in
// multi-row statements and reclaims overlapping expired sets from sixteen
// writers. The statement-level accounting must keep the tenant state equal
// to count(*) and sum(encoded_bytes) of the receipts that remain.
func TestPostgresBreakConcurrencyReceiptLedger(t *testing.T) {
	t.Parallel()
	db := newBreakCountersDB(t)
	tenants := []string{breakCountersTenantOne, breakCountersTenantTwo}
	breakCountersTenants(t, db, tenants...)
	for _, tenant := range tenants {
		breakCountersExec(t, db, fmt.Sprintf(
			"INSERT INTO api_mutation_receipt_tenant_state (tenant_id, receipt_count, encoded_bytes, "+
				"high_water_unix_micro) VALUES ('%s', 0, 0, 1000)", tenant))
	}
	const route = "/api/indexes/create"

	runBreakCountersContended(t, 6, func(ctx context.Context, worker, round int) error {
		tenant := tenants[worker%len(tenants)]
		return db.WriteTx(ctx, func(tx *sql.Tx) error {
			if round%3 == 2 {
				// Reclaim the four oldest receipts; concurrent reclaimers
				// pick overlapping sets.
				_, err := tx.ExecContext(ctx,
					"DELETE FROM api_mutation_receipts WHERE (tenant_id, actor_kind, actor_id, route, client_request_id) IN ("+
						"SELECT tenant_id, actor_kind, actor_id, route, client_request_id FROM api_mutation_receipts "+
						"WHERE tenant_id = $1 ORDER BY client_request_id LIMIT 4)",
					tenant,
				)
				return err
			}
			// Three receipts in one statement.
			actor := fmt.Sprintf("actor-%d", worker)
			var requestIDs, targetIDs []string
			var sizes []int64
			for item := range 3 {
				requestID := fmt.Sprintf("request-%04d-%04d-%d", worker, round, item)
				targetID := fmt.Sprintf("idx-%d", item)
				requestIDs = append(requestIDs, requestID)
				targetIDs = append(targetIDs, targetID)
				sizes = append(sizes, int64(breakCountersReceiptBytes(tenant, actor, route, requestID, "index", targetID)))
			}
			_, err := tx.ExecContext(ctx,
				"INSERT INTO api_mutation_receipts (tenant_id, actor_kind, actor_id, route, client_request_id, "+
					"canonical_version, request_sha256, target_kind, target_id, target_version, audit_sequence, "+
					"created_at_unix_micro, retain_until_unix_micro, target_terminal_at_unix_micro, encoded_bytes) "+
					"SELECT $1, 'system', $2, $3, receipt.request_id, 1, "+breakCountersSHA+", 'index', "+
					"receipt.target_id, 1, NULL, 500, 600, 500, receipt.encoded_bytes "+
					"FROM unnest($4::text[], $5::text[], $6::bigint[]) AS receipt (request_id, target_id, encoded_bytes)",
				tenant, actor, route, requestIDs, targetIDs, sizes,
			)
			return err
		})
	})

	for _, tenant := range tenants {
		var stateCount, stateBytes, rowCount, rowBytes int64
		if err := db.SQLDB().QueryRowContext(breakCountersContext(t),
			"SELECT state.receipt_count, state.encoded_bytes, "+
				"(SELECT count(*) FROM api_mutation_receipts WHERE tenant_id = state.tenant_id), "+
				"(SELECT COALESCE(sum(encoded_bytes), 0) FROM api_mutation_receipts WHERE tenant_id = state.tenant_id) "+
				"FROM api_mutation_receipt_tenant_state AS state WHERE state.tenant_id = $1", tenant,
		).Scan(&stateCount, &stateBytes, &rowCount, &rowBytes); err != nil {
			t.Fatalf("read receipt state: %v", err)
		}
		if stateCount != rowCount || stateBytes != rowBytes {
			t.Fatalf("%s receipt state is %d/%d bytes, rows are %d/%d bytes", tenant, stateCount, stateBytes, rowCount, rowBytes)
		}
		if rowCount == 0 {
			t.Fatalf("%s: every receipt was reclaimed, so the test exercised nothing", tenant)
		}
	}
}

// breakCountersCatalogTenant creates tenant t1 and provisions its knowledge
// catalog the way its first app does.
func breakCountersCatalogTenant(t *testing.T, db *control.PostgresDB) {
	t.Helper()
	breakCountersTenants(t, db, breakCountersTenantOne)
	breakCountersExec(t, db, fmt.Sprintf(
		"INSERT INTO app_workspaces (app_id, tenant_id, version, slug, display_name, default_time_range_present, "+
			"state, created_at_unix_micro, updated_at_unix_micro, archived_at_unix_micro) "+
			"VALUES ('app_%s', '%s', 1, 'search', 'Search', 0, 'active', 1, 1, NULL)",
		strings.Repeat("a", 21)+"A", breakCountersTenantOne,
	))
}

const breakCountersStageInsert = "INSERT INTO knowledge_lookup_asset_stages (tenant_id, stage_id, owner_id, " +
	"source_sha256, content_sha256, source_bytes, canonical_csv, canonical_bytes, row_count, column_count, " +
	"created_at_unix_micro, expires_at_unix_micro) " +
	"SELECT $1, $2 || '-' || n, 'owner-a', " + breakCountersSHA + ", " + breakCountersSHA + ", 3, " +
	"'\\x414243'::bytea, 3, 0, 1, 10, 20 FROM generate_series(1, $3::bigint) AS n"

// TestPostgresBreakConcurrencyLookupStageCapacity stages lookup assets from
// sixteen writers against the 64-stage tenant cap. Exactly 64 stages may
// commit, every writer that loses must see the trigger's capacity message,
// and the ledger must equal the stages that exist, after which concurrent
// multi-row reclaims must bring it back to zero.
func TestPostgresBreakConcurrencyLookupStageCapacity(t *testing.T) {
	t.Parallel()
	db := newBreakCountersDB(t)
	breakCountersCatalogTenant(t, db)

	var mu sync.Mutex
	rejected := 0
	runBreakCountersContended(t, 3, func(ctx context.Context, worker, round int) error {
		// Two stages per statement: 16 workers x 3 rounds x 2 = 96 > 64.
		err := db.WriteTx(ctx, func(tx *sql.Tx) error {
			_, err := tx.ExecContext(ctx, breakCountersStageInsert,
				breakCountersTenantOne, fmt.Sprintf("stage-w%d-r%d", worker, round), 2)
			return err
		})
		if message, ok := control.RaiseMessage(err); ok && message == "lookup asset staging capacity exhausted" {
			mu.Lock()
			rejected++
			mu.Unlock()
			return nil
		}
		return err
	})

	ledgerQuery := "SELECT staged_asset_count, stored_content_bytes FROM knowledge_lookup_asset_tenant_ledgers WHERE tenant_id = $1"
	readLedger := func() (int64, int64) {
		var staged, stored int64
		if err := db.SQLDB().QueryRowContext(breakCountersContext(t), ledgerQuery, breakCountersTenantOne).
			Scan(&staged, &stored); err != nil {
			t.Fatalf("read lookup ledger: %v", err)
		}
		return staged, stored
	}
	stages := breakCountersInt64(t, db, "SELECT count(*) FROM knowledge_lookup_asset_stages")
	staged, stored := readLedger()
	if stages > 64 || staged != stages || stored != 3*stages {
		t.Fatalf("%d stages exist, ledger says %d stages and %d bytes", stages, staged, stored)
	}
	// A two-row statement that would cross 64 is refused as a whole, so the
	// tenant can stop at 63 or 64 depending on which statements won.
	if stages < 63 || rejected == 0 {
		t.Fatalf("%d stages committed and %d statements were refused; want the cap reached", stages, rejected)
	}

	runBreakCountersContended(t, 1, func(ctx context.Context, worker, _ int) error {
		return db.WriteTx(ctx, func(tx *sql.Tx) error {
			// Overlapping reclaims: each removes every stage whose id
			// hashes into its worker's quarter.
			_, err := tx.ExecContext(ctx,
				"DELETE FROM knowledge_lookup_asset_stages WHERE tenant_id = $1 AND abs(hashtext(stage_id)) % 4 = $2",
				breakCountersTenantOne, worker%4,
			)
			return err
		})
	})
	if staged, stored := readLedger(); staged != 0 || stored != 0 {
		t.Fatalf("after reclaiming every stage the ledger says %d stages and %d bytes", staged, stored)
	}
}

// TestPostgresBreakConcurrencyDashboardOwnerCapacity is the classic write
// skew: the 64-dashboard owner cap is a BEFORE INSERT count(*), with no
// counter row to lock. Sixteen writers each try to add five dashboards for
// one owner; SERIALIZABLE must stop exactly at 64.
func TestPostgresBreakConcurrencyDashboardOwnerCapacity(t *testing.T) {
	t.Parallel()
	db := newBreakCountersDB(t)
	breakCountersCatalogTenant(t, db)
	app := "app_" + strings.Repeat("a", 21) + "A"

	runBreakCountersContended(t, 5, func(ctx context.Context, worker, round int) error {
		err := db.WriteTx(ctx, func(tx *sql.Tx) error {
			_, err := tx.ExecContext(ctx,
				"INSERT INTO dashboards (dashboard_id, version, name, app_id, tenant_id, owner_id, sharing_scope, "+
					"definition_proto, created_at_unix_micro, updated_at_unix_micro) "+
					"VALUES ($1, 1, $1, $2, $3, 'owner', 1, '\\x00'::bytea, 1, 1)",
				fmt.Sprintf("d-w%d-r%d", worker, round), app, breakCountersTenantOne,
			)
			return err
		})
		if message, ok := control.RaiseMessage(err); ok && message == "dashboard owner capacity exhausted" {
			return nil
		}
		return err
	})
	if count := breakCountersInt64(t, db, "SELECT count(*) FROM dashboards WHERE owner_id = 'owner'"); count != 64 {
		t.Fatalf("owner has %d dashboards, want exactly 64", count)
	}
}

// TestPostgresBreakConcurrencyKnowledgeCatalogRevisionHead advances one
// tenant's catalog revision from sixteen writers. The revision head must
// follow every step, and every committed revision must have had its own
// state token.
func TestPostgresBreakConcurrencyKnowledgeCatalogRevisionHead(t *testing.T) {
	t.Parallel()
	db := newBreakCountersDB(t)
	breakCountersCatalogTenant(t, db)
	const rounds = 4

	var mu sync.Mutex
	tokens := make(map[string]int64)
	runBreakCountersContended(t, rounds, func(ctx context.Context, _, _ int) error {
		var revision int64
		var token []byte
		err := db.WriteTx(ctx, func(tx *sql.Tx) error {
			if err := tx.QueryRowContext(ctx,
				"UPDATE knowledge_catalog_tenants SET catalog_revision = catalog_revision + 1 "+
					"WHERE tenant_id = $1 RETURNING catalog_revision", breakCountersTenantOne,
			).Scan(&revision); err != nil {
				return err
			}
			return tx.QueryRowContext(ctx,
				"SELECT state_token FROM knowledge_catalog_revision_heads WHERE tenant_id = $1 AND catalog_revision = $2",
				breakCountersTenantOne, revision,
			).Scan(&token)
		})
		if err != nil {
			return err
		}
		mu.Lock()
		defer mu.Unlock()
		if previous, ok := tokens[string(token)]; ok {
			return fmt.Errorf("revisions %d and %d share a state token", previous, revision)
		}
		tokens[string(token)] = revision
		return nil
	})

	want := int64(breakCountersWorkers * rounds)
	if revision := breakCountersInt64(t, db,
		"SELECT head.catalog_revision FROM knowledge_catalog_revision_heads AS head "+
			"JOIN knowledge_catalog_tenants AS tenant USING (tenant_id, catalog_revision) WHERE tenant_id = $1",
		breakCountersTenantOne,
	); revision != want {
		t.Fatalf("revision head is at %d, want %d", revision, want)
	}
	if int64(len(tokens)) != want {
		t.Fatalf("%d distinct state tokens were committed, want %d", len(tokens), want)
	}
}

// TestPostgresBreakConcurrencyFirstAppendCreatesTenantState is the audit
// stores' ensureTenantState: read the state row, insert it when missing,
// then append. Sixteen writers do this at once for a tenant with no state.
func TestPostgresBreakConcurrencyFirstAppendCreatesTenantState(t *testing.T) {
	t.Parallel()
	for _, journal := range breakCountersJournals {
		t.Run(journal.name, func(t *testing.T) {
			t.Parallel()
			db := newBreakCountersDB(t)
			breakCountersTenants(t, db, breakCountersTenantOne)
			ensure := fmt.Sprintf(journal.stateSetup, breakCountersTenantOne)
			runBreakCountersContended(t, 1, func(ctx context.Context, worker, _ int) error {
				return db.WriteTx(ctx, func(tx *sql.Tx) error {
					var present int64
					err := tx.QueryRowContext(ctx,
						"SELECT 1 FROM "+journal.stateTable+" WHERE tenant_id = $1", breakCountersTenantOne,
					).Scan(&present)
					if errors.Is(err, sql.ErrNoRows) {
						if _, err := tx.ExecContext(ctx, ensure); err != nil {
							return fmt.Errorf("create tenant state: %w", err)
						}
					} else if err != nil {
						return err
					}
					var next int64
					if err := tx.QueryRowContext(ctx,
						"SELECT next_sequence FROM "+journal.stateTable+" WHERE tenant_id = $1", breakCountersTenantOne,
					).Scan(&next); err != nil {
						return err
					}
					_, err = tx.ExecContext(ctx, journal.insert, breakCountersTenantOne, next, 1_000_000+next,
						fmt.Sprintf("first-w%d", worker))
					return err
				})
			})
			last := int64(breakCountersWorkers)
			first := int64(1)
			if journal.retainedWindow > 0 {
				first = last - journal.retainedWindow + 1
			}
			requireBreakCountersDense(t, journal.name, breakCountersInt64s(t, db,
				"SELECT sequence FROM "+journal.name+" ORDER BY sequence"), first, last)
		})
	}
}

// breakCountersStatement runs one statement in its own WriteTx and returns
// its error, for the multi-row cases below.
func breakCountersStatement(t *testing.T, db *control.PostgresDB, statement string, args ...any) error {
	t.Helper()
	ctx := breakCountersContext(t)
	return db.WriteTx(ctx, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, statement, args...)
		return err
	})
}

func requireBreakCountersRow(t *testing.T, db *control.PostgresDB, want string, query string, args ...any) {
	t.Helper()
	var got string
	if err := db.SQLDB().QueryRowContext(breakCountersContext(t), query, args...).Scan(&got); err != nil {
		t.Fatalf("query %q: %v", query, err)
	}
	if got != want {
		t.Fatalf("%q = %q, want %q", query, got, want)
	}
}

// TestPostgresBreakMultiRowJournalAppendIsRefused pins the one documented
// row-timing difference of the audit journals: SQLite runs row 1's AFTER
// accounting before row 2's BEFORE check and accepts a two-row append,
// PostgreSQL queues the accounting to the end of the statement and refuses
// it with the current-state message. Nothing may be half-applied.
func TestPostgresBreakMultiRowJournalAppendIsRefused(t *testing.T) {
	t.Parallel()
	messages := map[string]string{
		"audit_events":                   "audit tenant state is invalid or capacity is exhausted",
		"feature_operation_audit_events": "feature-operation audit tenant state is invalid or sequence is exhausted",
		"knowledge_attempt_audit_events": "knowledge-attempt audit tenant state is invalid or sequence is exhausted",
		"search_attempt_audit_events":    "search-attempt audit tenant state is invalid or sequence is exhausted",
	}
	db := newBreakCountersDB(t)
	breakCountersTenants(t, db, breakCountersTenantOne)
	for _, journal := range breakCountersJournals {
		breakCountersExec(t, db, fmt.Sprintf(journal.stateSetup, breakCountersTenantOne))
		state := "SELECT (SELECT count(*) FROM " + journal.name + ")::text || ' ' || next_sequence::text FROM " +
			journal.stateTable + " WHERE tenant_id = $1"
		// Turn "VALUES ($1, $2, $3, ..., $4, ...)" into a two-row VALUES list
		// whose second row is the next sequence.
		values := journal.insert[strings.Index(journal.insert, "VALUES ")+len("VALUES "):]
		second := strings.NewReplacer("$1", "$5", "$2", "$6", "$3", "$7", "$4", "$8").Replace(values)
		err := breakCountersStatement(t, db, journal.insert+", "+second,
			breakCountersTenantOne, 1, 1_000_001, "row-1",
			breakCountersTenantOne, 2, 1_000_002, "row-2",
		)
		if message, ok := control.RaiseMessage(err); !ok || message != messages[journal.name] {
			t.Fatalf("%s: two-row append = %v, want RAISE %q", journal.name, err, messages[journal.name])
		}
		requireBreakCountersRow(t, db, "0 1", state, breakCountersTenantOne)

		// The same two events as two statements of one transaction succeed.
		ctx := breakCountersContext(t)
		if err := db.WriteTx(ctx, func(tx *sql.Tx) error {
			for sequence := int64(1); sequence <= 2; sequence++ {
				if _, err := tx.ExecContext(ctx, journal.insert, breakCountersTenantOne, sequence,
					1_000_000+sequence, fmt.Sprintf("row-%d", sequence)); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			t.Fatalf("%s: two single-row appends: %v", journal.name, err)
		}
		requireBreakCountersRow(t, db, "2 3", state, breakCountersTenantOne)
	}
}

// TestPostgresBreakMultiRowAppCatalogRevision writes several apps of a fresh
// tenant per statement. Each row must advance the revision by exactly one,
// as SQLite's per-row triggers do, and the first statement must create the
// revision authority and provision the knowledge catalog exactly once.
func TestPostgresBreakMultiRowAppCatalogRevision(t *testing.T) {
	t.Parallel()
	db := newBreakCountersDB(t)
	breakCountersTenants(t, db, breakCountersTenantOne)
	revision := "SELECT revision::text FROM app_catalog_revisions WHERE tenant_id = 't1'"

	if err := breakCountersStatement(t, db,
		"INSERT INTO app_workspaces (app_id, tenant_id, version, slug, display_name, default_time_range_present, "+
			"state, created_at_unix_micro, updated_at_unix_micro, archived_at_unix_micro) "+
			"SELECT 'app_' || lpad(n::text, 21, '0') || 'A', 't1', 1, 'slug-' || n, 'App', 0, 'active', 1, 1, NULL "+
			"FROM generate_series(1, 3) AS n",
	); err != nil {
		t.Fatalf("three-row first app insert: %v", err)
	}
	requireBreakCountersRow(t, db, "3", revision)
	requireBreakCountersRow(t, db, "1 1 1", "SELECT (SELECT count(*) FROM knowledge_catalog_tenants)::text || ' ' || "+
		"(SELECT count(*) FROM knowledge_catalog_revision_heads)::text || ' ' || "+
		"(SELECT count(*) FROM knowledge_lookup_asset_tenant_ledgers)::text")

	if err := breakCountersStatement(t, db,
		"UPDATE app_workspaces SET version = version + 1, state = 'archived', archived_at_unix_micro = 2, "+
			"updated_at_unix_micro = 2 WHERE tenant_id = 't1'",
	); err != nil {
		t.Fatalf("three-row archive: %v", err)
	}
	requireBreakCountersRow(t, db, "6", revision)

	if err := breakCountersStatement(t, db,
		"DELETE FROM app_workspaces WHERE tenant_id = 't1' AND slug <> 'slug-1'",
	); err != nil {
		t.Fatalf("two-row delete: %v", err)
	}
	requireBreakCountersRow(t, db, "8", revision)
}

// TestPostgresBreakMultiRowCollectorCatalogRevision does the same for the
// collector fleet: a multi-row first registration, update and delete.
func TestPostgresBreakMultiRowCollectorCatalogRevision(t *testing.T) {
	t.Parallel()
	db := newBreakCountersDB(t)
	breakCountersTenants(t, db, breakCountersTenantOne)
	state := "SELECT revision::text || ' ' || fleet_count::text || ' ' || runtime_count::text " +
		"FROM collector_catalog_revisions WHERE tenant_id = 't1'"

	if err := breakCountersStatement(t, db,
		"INSERT INTO collector_fleet (tenant_id, collector_id, admin_version, display_name, administrative_state, "+
			"first_seen_at_unix_micro, updated_at_unix_micro) "+
			"SELECT 't1', 'c' || n, 1, NULL, 'enabled', 1, 1 FROM generate_series(1, 3) AS n",
	); err != nil {
		t.Fatalf("three-row first registration: %v", err)
	}
	requireBreakCountersRow(t, db, "3 3 0", state)
	if err := breakCountersStatement(t, db,
		"UPDATE collector_fleet SET admin_version = admin_version + 1, updated_at_unix_micro = 2 WHERE tenant_id = 't1'",
	); err != nil {
		t.Fatalf("three-row update: %v", err)
	}
	requireBreakCountersRow(t, db, "6 3 0", state)
	if err := breakCountersStatement(t, db, "DELETE FROM collector_fleet WHERE collector_id <> 'c1'"); err != nil {
		t.Fatalf("two-row delete: %v", err)
	}
	requireBreakCountersRow(t, db, "8 1 0", state)
}

// TestPostgresBreakMultiRowSearchHistorySpansOwners inserts and deletes the
// history of two owners in single statements, so each statement's queued
// AFTER ROW triggers interleave owners.
func TestPostgresBreakMultiRowSearchHistorySpansOwners(t *testing.T) {
	t.Parallel()
	db := newBreakCountersDB(t)
	breakCountersTenants(t, db, breakCountersTenantOne)
	counts := "SELECT COALESCE(string_agg(owner_id || '=' || terminal_count, ',' ORDER BY owner_id), '-') " +
		"FROM search_history_owner_counts"

	if err := breakCountersStatement(t, db,
		"INSERT INTO search_history (search_job_id, tenant_id, owner_id, app_id, saved_search_id, final_state, "+
			"search_text, created_at_unix_micro, finished_at_unix_micro, duration_nanoseconds, matched_events, "+
			"entry_proto, entry_sha256) "+
			"SELECT 'job-' || n, 't1', CASE WHEN n % 2 = 0 THEN 'o2' ELSE 'o1' END, 'search', '', 6, 'index=main', "+
			"1, 1, 0, 0, '\\x00'::bytea, "+breakCountersSHA+" FROM generate_series(1, 5) AS n",
	); err != nil {
		t.Fatalf("five-row insert: %v", err)
	}
	requireBreakCountersRow(t, db, "o1=3,o2=2", counts)
	// One statement removes all of o2, so o2's counter row is deleted while
	// o1's is decremented in the same queue of AFTER ROW events.
	if err := breakCountersStatement(t, db,
		"DELETE FROM search_history WHERE search_job_id IN ('job-1', 'job-3', 'job-2', 'job-4')",
	); err != nil {
		t.Fatalf("four-row delete: %v", err)
	}
	requireBreakCountersRow(t, db, "o1=1", counts)
	if err := breakCountersStatement(t, db, "DELETE FROM search_history"); err != nil {
		t.Fatalf("delete the rest: %v", err)
	}
	requireBreakCountersRow(t, db, "-", counts)
}

// TestPostgresBreakMultiRowIndexCatalogCapacity fills the deployment-wide
// index catalog with one multi-row INSERT, then crosses the 1024 cap with a
// two-row INSERT. SQLite rejects the second row in its BEFORE trigger;
// PostgreSQL's BEFORE trigger for row two runs before row one's AFTER
// accounting, so the CHECK on physical_count is what rejects the statement.
// Either way nothing is applied, and the cap is reached one row at a time.
func TestPostgresBreakMultiRowIndexCatalogCapacity(t *testing.T) {
	t.Parallel()
	db := newBreakCountersDB(t)
	breakCountersTenants(t, db, breakCountersTenantOne)
	insert := "INSERT INTO indexes (tenant_id, index_id, version, name, display_name, ingestion_enabled, " +
		"search_enabled, state, created_at_unix_micro, updated_at_unix_micro) " +
		"SELECT 't1', $1 || n, 1, $1 || n, 'Index', 1, 1, 'active', 1, 1 FROM generate_series(1, $2::bigint) AS n"
	state := "SELECT revision::text || ' ' || physical_count::text FROM index_catalog_state"

	if err := breakCountersStatement(t, db, insert, "bulk-", 1023); err != nil {
		t.Fatalf("1023-row insert: %v", err)
	}
	requireBreakCountersRow(t, db, "1024 1023", state)

	err := breakCountersStatement(t, db, insert, "over-", 2)
	if constraint, _ := control.PostgresConstraintName(err); !control.IsCheckViolation(err) ||
		constraint != "index_catalog_state_physical_count_check" {
		t.Fatalf("two-row insert across the cap = %v, want the physical_count CHECK", err)
	}
	requireBreakCountersRow(t, db, "1024 1023", state)

	if err := breakCountersStatement(t, db, insert, "last-", 1); err != nil {
		t.Fatalf("1024th index: %v", err)
	}
	err = breakCountersStatement(t, db, insert, "beyond-", 1)
	if message, _ := control.RaiseMessage(err); message != "index catalog state is invalid or capacity is exhausted" {
		t.Fatalf("1025th index = %v, want the capacity RAISE", err)
	}
	requireBreakCountersRow(t, db, "1025 1024", state)
}

// TestPostgresBreakMultiRowLookupStageCapacity is the same boundary for the
// lookup stage ledger, whose AFTER ROW triggers add the whole statement's
// delta on its first row.
func TestPostgresBreakMultiRowLookupStageCapacity(t *testing.T) {
	t.Parallel()
	db := newBreakCountersDB(t)
	breakCountersCatalogTenant(t, db)
	ledger := "SELECT staged_asset_count::text || ' ' || stored_content_bytes::text " +
		"FROM knowledge_lookup_asset_tenant_ledgers WHERE tenant_id = 't1'"

	if err := breakCountersStatement(t, db, breakCountersStageInsert, breakCountersTenantOne, "bulk", 63); err != nil {
		t.Fatalf("63-row stage: %v", err)
	}
	requireBreakCountersRow(t, db, "63 189", ledger)

	err := breakCountersStatement(t, db, breakCountersStageInsert, breakCountersTenantOne, "over", 2)
	if constraint, _ := control.PostgresConstraintName(err); !control.IsCheckViolation(err) ||
		constraint != "knowledge_lookup_asset_tenant_ledgers_staged_asset_count_check" {
		t.Fatalf("two-row stage across the cap = %v, want the staged_asset_count CHECK", err)
	}
	requireBreakCountersRow(t, db, "63 189", ledger)

	if err := breakCountersStatement(t, db, breakCountersStageInsert, breakCountersTenantOne, "last", 1); err != nil {
		t.Fatalf("64th stage: %v", err)
	}
	err = breakCountersStatement(t, db, breakCountersStageInsert, breakCountersTenantOne, "beyond", 1)
	if message, _ := control.RaiseMessage(err); message != "lookup asset staging capacity exhausted" {
		t.Fatalf("65th stage = %v, want the capacity RAISE", err)
	}
	if err := breakCountersStatement(t, db,
		"DELETE FROM knowledge_lookup_asset_stages WHERE tenant_id = 't1' AND stage_id <> 'last-1'",
	); err != nil {
		t.Fatalf("63-row reclaim: %v", err)
	}
	requireBreakCountersRow(t, db, "1 3", ledger)
}

// waitForBreakCountersLockWaiter polls until a session of this database
// waits on a heavyweight lock.
func waitForBreakCountersLockWaiter(t *testing.T, db *control.PostgresDB) {
	t.Helper()
	ctx := breakCountersContext(t)
	for {
		var waiting int64
		if err := db.SQLDB().QueryRowContext(ctx,
			"SELECT count(*) FROM pg_stat_activity WHERE datname = current_database() AND wait_event_type = 'Lock'",
		).Scan(&waiting); err != nil {
			t.Fatalf("poll lock waiters: %v", err)
		}
		if waiting > 0 {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatal("no session ever waited on the journal state lock")
		case <-time.After(5 * time.Millisecond):
		}
	}
}

// TestPostgresBreakConcurrencyJournalRaceOutcomeByIsolation races two appends
// of the same sequence deterministically: the second starts while the first
// holds the tenant state lock, and the first then commits. Under
// SERIALIZABLE the second must fail with a retryable 40001 (never the 23505
// that WriteTx would not retry); under READ COMMITTED, which the control
// plane does not use for writes, it must still be refused by the trigger.
func TestPostgresBreakConcurrencyJournalRaceOutcomeByIsolation(t *testing.T) {
	t.Parallel()
	notCurrent := map[string]string{
		"audit_events":                   "audit tenant state is invalid or capacity is exhausted",
		"feature_operation_audit_events": "feature-operation audit tenant state is invalid or sequence is exhausted",
		"knowledge_attempt_audit_events": "knowledge-attempt audit tenant state is invalid or sequence is exhausted",
		"search_attempt_audit_events":    "search-attempt audit tenant state is invalid or sequence is exhausted",
	}
	for _, isolation := range []sql.IsolationLevel{sql.LevelSerializable, sql.LevelReadCommitted} {
		for _, journal := range breakCountersJournals {
			t.Run(isolation.String()+"/"+journal.name, func(t *testing.T) {
				t.Parallel()
				db := newBreakCountersDB(t)
				breakCountersTenants(t, db, breakCountersTenantOne)
				breakCountersExec(t, db, fmt.Sprintf(journal.stateSetup, breakCountersTenantOne))
				ctx := breakCountersContext(t)
				options := &sql.TxOptions{Isolation: isolation}

				winner, err := db.SQLDB().BeginTx(ctx, options)
				if err != nil {
					t.Fatalf("begin winner: %v", err)
				}
				loser, err := db.SQLDB().BeginTx(ctx, options)
				if err != nil {
					t.Fatalf("begin loser: %v", err)
				}
				defer func() {
					_ = winner.Rollback()
					_ = loser.Rollback()
				}()
				// Both read next_sequence first, as the stores do, so both
				// snapshots predate the winner's commit.
				for _, tx := range []*sql.Tx{winner, loser} {
					var next int64
					if err := tx.QueryRowContext(ctx,
						"SELECT next_sequence FROM "+journal.stateTable+" WHERE tenant_id = $1", breakCountersTenantOne,
					).Scan(&next); err != nil || next != 1 {
						t.Fatalf("read next_sequence = %d, %v", next, err)
					}
				}
				if _, err := winner.ExecContext(ctx, journal.insert, breakCountersTenantOne, 1, 1_000_001, "winner"); err != nil {
					t.Fatalf("winner append: %v", err)
				}
				loserErr := make(chan error, 1)
				go func() {
					_, err := loser.ExecContext(ctx, journal.insert, breakCountersTenantOne, 1, 1_000_001, "loser")
					loserErr <- err
				}()
				waitForBreakCountersLockWaiter(t, db)
				if err := winner.Commit(); err != nil {
					t.Fatalf("winner commit: %v", err)
				}
				err = <-loserErr
				if isolation == sql.LevelSerializable {
					if !control.IsRetryable(err) {
						t.Fatalf("SERIALIZABLE loser = %v (sqlstate %s), want 40001", err, breakCountersSQLState(err))
					}
					return
				}
				if message, ok := control.RaiseMessage(err); !ok || message != notCurrent[journal.name] {
					t.Fatalf("READ COMMITTED loser = %v, want RAISE %q", err, notCurrent[journal.name])
				}
			})
		}
	}
}
