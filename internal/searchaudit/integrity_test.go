package searchaudit

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/control"
)

func TestPersistedCapSurvivesDefaultReopenAndExplicitMismatchFails(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	database := openSearchAuditTestDatabase(t)
	configured := newSearchAuditTestStore(t, database, searchAuditTestCursorKey(), 2)
	appendSearchAuditTestEvent(t, configured, database, ctx, "tenant-cap", searchAuditTestDefinition("owner", "job-1", 0))

	defaultStore := newSearchAuditTestStore(t, database, searchAuditTestCursorKey(), 0)
	appendSearchAuditTestEvent(t, defaultStore, database, ctx, "tenant-cap", searchAuditTestDefinition("owner", "job-2", time.Microsecond))
	appendSearchAuditTestEvent(t, defaultStore, database, ctx, "tenant-cap", searchAuditTestDefinition("owner", "job-3", 2*time.Microsecond))
	page, err := defaultStore.List(ctx, "tenant-cap", ListRequest{})
	if err != nil || !slices.Equal(searchAuditSequences(page.Events), []uint64{3, 2}) {
		t.Fatalf("persisted-cap page = (%+v, %v)", page, err)
	}

	if mismatched, mismatchErr := New(database, Options{
		CursorKey: searchAuditTestCursorKey(), MaximumRetainedAttempts: 3,
	}); mismatched != nil || !errors.Is(mismatchErr, control.ErrInvalidArgument) {
		t.Fatalf("New(explicit cap mismatch) = (%v, %v)", mismatched, mismatchErr)
	}
}

func TestDuplicateRetainedJobPublicationRollsBackWithoutAdvancingState(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	database := openSearchAuditTestDatabase(t)
	store := newSearchAuditTestStore(t, database, searchAuditTestCursorKey(), 3)
	definition := searchAuditTestDefinition("owner", "job-duplicate", 0)
	appendSearchAuditTestEvent(t, store, database, ctx, "tenant-duplicate", definition)

	replaced := database.GORMDB().WithContext(ctx).Exec(`
		INSERT OR REPLACE INTO search_attempt_audit_events (
			tenant_id,
			sequence,
			occurred_at_unix_micro,
			actor_kind,
			actor_id,
			actor_role,
			owner_id,
			search_job_id
		)
		SELECT
			tenant_id,
			2,
			occurred_at_unix_micro + 1,
			actor_kind,
			actor_id,
			actor_role,
			owner_id,
			search_job_id
		FROM search_attempt_audit_events
		WHERE tenant_id = ? AND sequence = 1
	`, "tenant-duplicate")
	if replaced.Error == nil {
		t.Fatal("INSERT OR REPLACE bypassed retained search-job uniqueness")
	}

	tx := database.GORMDB().WithContext(ctx).Begin()
	if tx.Error != nil {
		t.Fatal(tx.Error)
	}
	definition.OccurredAt = definition.OccurredAt.Add(time.Microsecond)
	err := store.AppendSearchAttemptInTransaction(ctx, tx, "tenant-duplicate", definition)
	_ = tx.Rollback().Error
	if err == nil {
		t.Fatal("duplicate retained search-job publication succeeded")
	}
	page, err := store.List(ctx, "tenant-duplicate", ListRequest{IncludeTotal: true})
	if err != nil || len(page.Events) != 1 || page.Events[0].Sequence != 1 ||
		page.TotalSize == nil || *page.TotalSize != 1 {
		t.Fatalf("page after duplicate = (%+v, %v)", page, err)
	}
	var state searchAttemptTenantStateRecord
	if err := database.GORMDB().Where("tenant_id = ?", "tenant-duplicate").Take(&state).Error; err != nil {
		t.Fatal(err)
	}
	if state.FirstSequence != 1 || state.NextSequence != 2 || state.RetainedCount != 1 {
		t.Fatalf("state advanced after duplicate: %+v", state)
	}
}

func TestConcurrentUniqueAppendsRemainDenseAndBounded(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	database := openSearchAuditTestDatabase(t)
	store := newSearchAuditTestStore(t, database, searchAuditTestCursorKey(), 16)
	const attempts = 24
	start := make(chan struct{})
	errorsByAttempt := make(chan error, attempts)
	var wait sync.WaitGroup
	for index := range attempts {
		wait.Go(func() {
			<-start
			tx := database.GORMDB().WithContext(ctx).Begin()
			if tx.Error != nil {
				errorsByAttempt <- tx.Error
				return
			}
			err := store.AppendSearchAttemptInTransaction(
				ctx,
				tx,
				"tenant-concurrent",
				searchAuditTestDefinition(
					"owner",
					fmt.Sprintf("job-%02d", index),
					time.Duration(index)*time.Microsecond,
				),
			)
			if err != nil {
				_ = tx.Rollback().Error
				errorsByAttempt <- err
				return
			}
			errorsByAttempt <- tx.Commit().Error
		})
	}
	close(start)
	wait.Wait()
	close(errorsByAttempt)
	for err := range errorsByAttempt {
		if err != nil {
			t.Fatalf("concurrent append: %v", err)
		}
	}
	page, err := store.List(ctx, "tenant-concurrent", ListRequest{IncludeTotal: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Events) != 16 || page.TotalSize == nil || *page.TotalSize != 16 ||
		page.Events[0].Sequence != attempts || page.Events[len(page.Events)-1].Sequence != attempts-15 {
		t.Fatalf("concurrent page = %+v", page)
	}
}

func TestStartupIntegrityRejectsGapAndForgedRows(t *testing.T) {
	t.Parallel()
	t.Run("gap", func(t *testing.T) {
		ctx := context.Background()
		database := openSearchAuditTestDatabase(t)
		store := newSearchAuditTestStore(t, database, searchAuditTestCursorKey(), 5)
		for index := 1; index <= 3; index++ {
			appendSearchAuditTestEvent(t, store, database, ctx, "tenant-gap", searchAuditTestDefinition(
				"owner", fmt.Sprintf("job-%d", index), time.Duration(index)*time.Microsecond,
			))
		}
		if err := database.GORMDB().Exec("DROP TRIGGER search_attempt_audit_event_delete_requires_rolling_prune").Error; err != nil {
			t.Fatal(err)
		}
		if err := database.GORMDB().Exec("DROP TRIGGER search_attempt_audit_event_prune_advances_state").Error; err != nil {
			t.Fatal(err)
		}
		if err := database.GORMDB().Exec(`
			DELETE FROM search_attempt_audit_events
			WHERE tenant_id = ? AND sequence = 2
		`, "tenant-gap").Error; err != nil {
			t.Fatal(err)
		}
		if reopened, err := New(database, Options{CursorKey: searchAuditTestCursorKey(), MaximumRetainedAttempts: 5}); reopened != nil || !errors.Is(err, ErrCorrupt) {
			t.Fatalf("New(gap) = (%v, %v)", reopened, err)
		}
	})

	t.Run("forged actor", func(t *testing.T) {
		ctx := context.Background()
		database := openSearchAuditTestDatabase(t)
		store := newSearchAuditTestStore(t, database, searchAuditTestCursorKey(), 5)
		appendSearchAuditTestEvent(t, store, database, ctx, "tenant-forged", searchAuditTestDefinition("owner", "job", 0))
		connection, err := database.SQLDB().Conn(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = connection.Close() }()
		if _, err := connection.ExecContext(ctx, "DROP TRIGGER search_attempt_audit_event_update_is_forbidden"); err != nil {
			t.Fatal(err)
		}
		if _, err := connection.ExecContext(ctx, "PRAGMA ignore_check_constraints = ON"); err != nil {
			t.Fatal(err)
		}
		if _, err := connection.ExecContext(ctx, `
			UPDATE search_attempt_audit_events
			SET actor_kind = 'browser', actor_role = 'system'
			WHERE tenant_id = 'tenant-forged'
		`); err != nil {
			t.Fatal(err)
		}
		if err := connection.Close(); err != nil {
			t.Fatal(err)
		}
		if reopened, err := New(database, Options{CursorKey: searchAuditTestCursorKey(), MaximumRetainedAttempts: 5}); reopened != nil || !errors.Is(err, ErrCorrupt) {
			t.Fatalf("New(forged) = (%v, %v)", reopened, err)
		}
	})
}

func TestAppendRejectsNilAndCanceledContexts(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	database := openSearchAuditTestDatabase(t)
	store := newSearchAuditTestStore(t, database, searchAuditTestCursorKey(), 5)
	tx := database.GORMDB().WithContext(ctx).Begin()
	if tx.Error != nil {
		t.Fatal(tx.Error)
	}
	//nolint:staticcheck // Explicitly verifies the exported nil-context guard.
	if err := store.AppendSearchAttemptInTransaction(nil, tx, "tenant", searchAuditTestDefinition("owner", "job", 0)); !errors.Is(err, control.ErrInvalidArgument) {
		_ = tx.Rollback().Error
		t.Fatalf("nil context error = %v", err)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if err := store.AppendSearchAttemptInTransaction(canceled, tx, "tenant", searchAuditTestDefinition("owner", "job", 0)); !errors.Is(err, context.Canceled) {
		_ = tx.Rollback().Error
		t.Fatalf("canceled context error = %v", err)
	}
	_ = tx.Rollback().Error
}
