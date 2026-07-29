package searchhistory

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/control"
)

func TestPruneMaintenanceBatchPrunesExpiredTerminalRowsAcrossScopes(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.July, 29, 12, 0, 0, 0, time.UTC)
	database, store := openTestStore(t, Options{
		Clock:      func() time.Time { return now },
		MaximumAge: time.Hour,
	})
	ctx := context.Background()
	expiredAt := now.Add(-time.Microsecond)
	cutoffAt := now
	freshAt := now.Add(time.Microsecond)
	for index, fixture := range []struct {
		scope   AccessScope
		id      string
		created time.Time
	}{
		{scope: AccessScope{TenantID: "tenant-a", OwnerID: "owner-a"}, id: "expired-a-a", created: expiredAt},
		{scope: AccessScope{TenantID: "tenant-a", OwnerID: "owner-b"}, id: "expired-a-b", created: expiredAt},
		{scope: AccessScope{TenantID: "tenant-b", OwnerID: "owner-a"}, id: "expired-b-a", created: expiredAt},
		{scope: AccessScope{TenantID: "tenant-c", OwnerID: "owner-c"}, id: "at-cutoff", created: cutoffAt},
		{scope: AccessScope{TenantID: "tenant-d", OwnerID: "owner-d"}, id: "fresh", created: freshAt},
	} {
		if _, err := store.Record(
			ctx,
			fixture.scope,
			historyEntry(
				fixture.id,
				fmt.Sprintf("index=main | head %d", index+1),
				"search",
				opensplunkv1.SearchJobState_SEARCH_JOB_STATE_COMPLETED,
				fixture.created,
			),
		); err != nil {
			t.Fatalf("Record(%s) error = %v", fixture.id, err)
		}
	}
	pendingScope := AccessScope{TenantID: "tenant-pending", OwnerID: "owner-pending"}
	if _, err := store.BeginAttempt(
		ctx,
		pendingScope,
		pendingHistoryEntry("expired-pending", "index=main | head 1", expiredAt),
	); err != nil {
		t.Fatalf("BeginAttempt(expired pending) error = %v", err)
	}

	now = now.Add(time.Hour)
	var deletedTotal int64
	var cursor *MaintenanceCursor
	for call := 1; call <= 4; call++ {
		result, err := store.PruneMaintenanceBatch(ctx, 10, cursor)
		if err != nil {
			t.Fatalf("PruneMaintenanceBatch(call %d) error = %v", call, err)
		}
		if result.Deleted == 0 || result.Deleted > 10 {
			t.Fatalf(
				"PruneMaintenanceBatch(call %d) deleted %d rows, want between 1 and 10",
				call,
				result.Deleted,
			)
		}
		deletedTotal += result.Deleted
		cursor = result.Cursor
		if !result.More {
			break
		}
	}
	if deletedTotal != 3 {
		t.Fatalf("PruneMaintenanceBatch() deleted %d rows, want 3", deletedTotal)
	}

	if ids := maintenanceTerminalIDs(t, database); !slices.Equal(ids, []string{"at-cutoff", "fresh"}) {
		t.Fatalf("terminal rows after maintenance = %v, want [at-cutoff fresh]", ids)
	}
	var pending int
	if err := database.SQLDB().QueryRowContext(
		ctx,
		`SELECT COUNT(*) FROM search_history_pending WHERE search_job_id = 'expired-pending'`,
	).Scan(&pending); err != nil {
		t.Fatal(err)
	}
	if pending != 1 {
		t.Fatalf("pending rows after maintenance = %d, want 1", pending)
	}
}

func TestPruneMaintenanceBatchValidatesItsWorkBudget(t *testing.T) {
	t.Parallel()

	_, store := openTestStore(t, Options{})
	for _, maximumRows := range []int{
		-1,
		0,
		maximumMaintenancePruneBatchSize + 1,
	} {
		if _, err := store.PruneMaintenanceBatch(
			context.Background(),
			maximumRows,
			nil,
		); !errors.Is(err, control.ErrInvalidArgument) {
			t.Fatalf(
				"PruneMaintenanceBatch(maximumRows=%d) error = %v, want ErrInvalidArgument",
				maximumRows,
				err,
			)
		}
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := store.PruneMaintenanceBatch(
		canceled,
		1,
		nil,
	); !errors.Is(err, context.Canceled) {
		t.Fatalf("PruneMaintenanceBatch(canceled) error = %v", err)
	}
}

func TestPruneMaintenanceBatchHardBoundsEveryDeleteTransaction(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.July, 29, 12, 0, 0, 0, time.UTC)
	database, store := openTestStore(t, Options{
		Clock:      func() time.Time { return now },
		MaximumAge: time.Hour,
	})
	ctx := context.Background()
	scope := AccessScope{TenantID: "tenant", OwnerID: "owner"}
	for index := range 5 {
		id := fmt.Sprintf("expired-%d", index)
		if _, err := store.Record(
			ctx,
			scope,
			historyEntry(
				id,
				"index=main | head 1",
				"search",
				opensplunkv1.SearchJobState_SEARCH_JOB_STATE_COMPLETED,
				now,
			),
		); err != nil {
			t.Fatalf("Record(%s) error = %v", id, err)
		}
	}
	now = now.Add(2 * time.Hour)

	wantDeleted := []int64{2, 2, 1}
	wantMore := []bool{true, true, false}
	previousCount := maintenanceTerminalCount(t, database)
	var cursor *MaintenanceCursor
	for index := range wantDeleted {
		result, err := store.PruneMaintenanceBatch(ctx, 2, cursor)
		if err != nil {
			t.Fatalf("PruneMaintenanceBatch(call %d) error = %v", index+1, err)
		}
		if result.Deleted > 2 {
			t.Fatalf("PruneMaintenanceBatch(call %d) deleted %d rows, maximum is 2", index+1, result.Deleted)
		}
		if result.Deleted != wantDeleted[index] ||
			result.More != wantMore[index] {
			t.Fatalf(
				"PruneMaintenanceBatch(call %d) = (%d, %t), want (%d, %t)",
				index+1,
				result.Deleted,
				result.More,
				wantDeleted[index],
				wantMore[index],
			)
		}
		cursor = result.Cursor
		currentCount := maintenanceTerminalCount(t, database)
		if previousCount-currentCount != int(result.Deleted) {
			t.Fatalf(
				"PruneMaintenanceBatch(call %d) reported %d deletions but physical count changed from %d to %d",
				index+1,
				result.Deleted,
				previousCount,
				currentCount,
			)
		}
		previousCount = currentCount
	}
	if previousCount != 0 {
		t.Fatalf("terminal rows after bounded maintenance = %d, want 0", previousCount)
	}
}

func TestPruneMaintenanceBatchShrinksIdlePriorScopeToNewCountLimit(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.July, 29, 12, 0, 0, 0, time.UTC)
	database, oldStore := openTestStore(t, Options{
		Clock:                  func() time.Time { return now },
		MaximumAge:             24 * time.Hour,
		MaximumEntriesPerOwner: 5,
	})
	ctx := context.Background()
	priorScope := AccessScope{TenantID: "prior-tenant", OwnerID: "prior-owner"}
	for index := range 5 {
		id := fmt.Sprintf("prior-%d", index)
		created := now.Add(time.Duration(index-5) * time.Minute)
		if _, err := oldStore.Record(
			ctx,
			priorScope,
			historyEntry(
				id,
				"index=main | head 1",
				"search",
				opensplunkv1.SearchJobState_SEARCH_JOB_STATE_COMPLETED,
				created,
			),
		); err != nil {
			t.Fatalf("Record(%s) error = %v", id, err)
		}
	}
	store, err := New(database, Options{
		Clock:                  func() time.Time { return now },
		CursorKey:              testCursorKey,
		MaximumAge:             24 * time.Hour,
		MaximumEntriesPerOwner: 2,
	})
	if err != nil {
		t.Fatalf("New(shrunken retention) error = %v", err)
	}

	first, err := store.PruneMaintenanceBatch(ctx, 2, nil)
	if err != nil {
		t.Fatalf("PruneMaintenanceBatch(first) error = %v", err)
	}
	if first.Deleted != 2 || !first.More || first.Cursor == nil {
		t.Fatalf(
			"PruneMaintenanceBatch(first) = (%d, %t, %#v), want (2, true, cursor)",
			first.Deleted,
			first.More,
			first.Cursor,
		)
	}
	if count := maintenanceOwnerTerminalCount(t, database, priorScope); count != 3 {
		t.Fatalf("trigger-maintained owner count after first batch = %d, want 3", count)
	}
	second, err := store.PruneMaintenanceBatch(ctx, 2, first.Cursor)
	if err != nil {
		t.Fatalf("PruneMaintenanceBatch(second) error = %v", err)
	}
	if second.Deleted != 1 || second.More || second.Cursor != nil {
		t.Fatalf(
			"PruneMaintenanceBatch(second) = (%d, %t, %#v), want (1, false, nil)",
			second.Deleted,
			second.More,
			second.Cursor,
		)
	}
	if ids := maintenanceTerminalIDs(t, database); !slices.Equal(ids, []string{"prior-3", "prior-4"}) {
		t.Fatalf("retained prior-scope rows = %v, want [prior-3 prior-4]", ids)
	}
	if count := maintenanceOwnerTerminalCount(t, database, priorScope); count != 2 {
		t.Fatalf("trigger-maintained owner count after retention = %d, want 2", count)
	}
}

func TestPruneMaintenanceBatchContinuesOrderedCountScanAcrossScopes(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.July, 29, 12, 0, 0, 0, time.UTC)
	database, oldStore := openTestStore(t, Options{
		Clock:                  func() time.Time { return now },
		MaximumAge:             24 * time.Hour,
		MaximumEntriesPerOwner: 3,
	})
	ctx := context.Background()
	scopes := []AccessScope{
		{TenantID: "tenant-a", OwnerID: "owner-a"},
		{TenantID: "tenant-a", OwnerID: "owner-b"},
		{TenantID: "tenant-b", OwnerID: "owner-a"},
	}
	for scopeIndex, scope := range scopes {
		for rowIndex := range 3 {
			id := fmt.Sprintf("scope-%d-row-%d", scopeIndex, rowIndex)
			if _, err := oldStore.Record(
				ctx,
				scope,
				historyEntry(
					id,
					"index=main | head 1",
					"search",
					opensplunkv1.SearchJobState_SEARCH_JOB_STATE_COMPLETED,
					now.Add(time.Duration(rowIndex-3)*time.Minute),
				),
			); err != nil {
				t.Fatalf("Record(%s) error = %v", id, err)
			}
		}
	}
	store, err := New(database, Options{
		Clock:                  func() time.Time { return now },
		CursorKey:              testCursorKey,
		MaximumAge:             24 * time.Hour,
		MaximumEntriesPerOwner: 1,
	})
	if err != nil {
		t.Fatalf("New(shrunken retention) error = %v", err)
	}

	var cursor *MaintenanceCursor
	var deleted int64
	for call := 1; call <= 3; call++ {
		result, err := store.PruneMaintenanceBatch(ctx, 3, cursor)
		if err != nil {
			t.Fatalf("PruneMaintenanceBatch(call %d) error = %v", call, err)
		}
		deleted += result.Deleted
		cursor = result.Cursor
		if call < 3 && !result.More {
			t.Fatalf("PruneMaintenanceBatch(call %d) ended before scanning every scope", call)
		}
		if call == 3 && (result.More || result.Cursor != nil) {
			t.Fatalf(
				"PruneMaintenanceBatch(final) = more:%t cursor:%#v, want false and nil",
				result.More,
				result.Cursor,
			)
		}
	}
	if deleted != 6 {
		t.Fatalf("deleted rows across count scan = %d, want 6", deleted)
	}
	if count := maintenanceTerminalCount(t, database); count != len(scopes) {
		t.Fatalf("retained rows across scopes = %d, want %d", count, len(scopes))
	}
}

func TestPruneMaintenanceBatchInterleavesAgeWithCountContinuation(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.July, 29, 12, 0, 0, 0, time.UTC)
	database, oldStore := openTestStore(t, Options{
		Clock:                  func() time.Time { return now },
		MaximumAge:             time.Hour,
		MaximumEntriesPerOwner: 6,
	})
	ctx := context.Background()
	countScope := AccessScope{TenantID: "tenant-a", OwnerID: "owner"}
	for index := range 5 {
		id := fmt.Sprintf("count-backlog-%d", index)
		if _, err := oldStore.Record(
			ctx,
			countScope,
			historyEntry(
				id,
				"index=main | head 1",
				"search",
				opensplunkv1.SearchJobState_SEARCH_JOB_STATE_COMPLETED,
				now.Add(time.Duration(index)*time.Microsecond),
			),
		); err != nil {
			t.Fatalf("Record(%s) error = %v", id, err)
		}
	}
	expiredScope := AccessScope{TenantID: "tenant-z", OwnerID: "owner"}
	if _, err := oldStore.Record(
		ctx,
		expiredScope,
		historyEntry(
			"expires-during-count-scan",
			"index=main | head 1",
			"search",
			opensplunkv1.SearchJobState_SEARCH_JOB_STATE_COMPLETED,
			now.Add(-30*time.Minute),
		),
	); err != nil {
		t.Fatal(err)
	}
	store, err := New(database, Options{
		Clock:                  func() time.Time { return now },
		CursorKey:              testCursorKey,
		MaximumAge:             time.Hour,
		MaximumEntriesPerOwner: 1,
	})
	if err != nil {
		t.Fatal(err)
	}

	first, err := store.PruneMaintenanceBatch(ctx, 2, nil)
	if err != nil {
		t.Fatal(err)
	}
	if first.Deleted != 2 || !first.More || first.Cursor == nil {
		t.Fatalf(
			"first count batch = deleted:%d more:%t cursor:%#v",
			first.Deleted,
			first.More,
			first.Cursor,
		)
	}
	now = now.Add(2 * time.Hour)
	second, err := store.PruneMaintenanceBatch(ctx, 2, first.Cursor)
	if err != nil {
		t.Fatal(err)
	}
	if second.Deleted != 2 || !second.More || second.Cursor == nil {
		t.Fatalf(
			"interleaved batch = deleted:%d more:%t cursor:%#v",
			second.Deleted,
			second.More,
			second.Cursor,
		)
	}
	var expired int
	if err := database.SQLDB().QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM search_history
		WHERE search_job_id = 'expires-during-count-scan'`,
	).Scan(&expired); err != nil {
		t.Fatal(err)
	}
	if expired != 0 {
		t.Fatal("newly expired row survived a batch with a count cursor")
	}
}

func TestSearchHistoryGlobalCreatedIndexSupportsMaintenanceSelection(t *testing.T) {
	t.Parallel()

	database, _ := openTestStore(t, Options{})
	type queryPlanRow struct {
		ID     int
		Parent int
		Unused int
		Detail string
	}
	var plans []queryPlanRow
	result := database.GORMDB().WithContext(context.Background()).Raw(`
		EXPLAIN QUERY PLAN
		SELECT search_job_id
		FROM search_history
		WHERE created_at_unix_micro < ?
		ORDER BY created_at_unix_micro, search_job_id
		LIMIT ?`,
		time.Now().Add(-time.Hour).UnixMicro(),
		100,
	).Scan(&plans)
	if result.Error != nil {
		t.Fatalf("explain global maintenance selection: %v", result.Error)
	}
	details := make([]string, len(plans))
	for index, plan := range plans {
		details[index] = plan.Detail
	}
	if !strings.Contains(strings.Join(details, "\n"), "search_history_created_idx") {
		t.Fatalf(
			"global maintenance query plan = %v, want search_history_created_idx",
			details,
		)
	}

	plans = nil
	result = database.GORMDB().WithContext(context.Background()).Raw(`
		EXPLAIN QUERY PLAN
		SELECT tenant_id, owner_id
		FROM search_history_owner_counts
		WHERE terminal_count > ?
		ORDER BY tenant_id, owner_id
		LIMIT 1`,
		10,
	).Scan(&plans)
	if result.Error != nil {
		t.Fatalf("explain count-maintenance selection: %v", result.Error)
	}
	details = make([]string, len(plans))
	for index, plan := range plans {
		details[index] = plan.Detail
	}
	joined := strings.Join(details, "\n")
	if !strings.Contains(
		joined,
		"sqlite_autoindex_search_history_owner_counts_1",
	) ||
		strings.Contains(joined, "USE TEMP B-TREE") {
		t.Fatalf(
			"count-maintenance query plan = %v, want the owner-count primary key without temporary sort",
			details,
		)
	}

	plans = nil
	result = database.GORMDB().WithContext(context.Background()).Raw(`
		EXPLAIN QUERY PLAN
		SELECT search_job_id
		FROM search_history
		WHERE tenant_id = ? AND owner_id = ?
		ORDER BY created_at_unix_micro, search_job_id
		LIMIT ?`,
		"tenant",
		"owner",
		100,
	).Scan(&plans)
	if result.Error != nil {
		t.Fatalf("explain oldest-overflow selection: %v", result.Error)
	}
	details = make([]string, len(plans))
	for index, plan := range plans {
		details[index] = plan.Detail
	}
	joined = strings.Join(details, "\n")
	if !strings.Contains(joined, "search_history_owner_created_idx") ||
		strings.Contains(joined, "USE TEMP B-TREE") {
		t.Fatalf(
			"oldest-overflow query plan = %v, want owner-created index without temporary sort",
			details,
		)
	}
}

func TestScopedPruneCommitsBoundedProgressBeforeLaterBatchFailure(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.July, 29, 12, 0, 0, 0, time.UTC)
	database, store := openTestStore(t, Options{
		Clock:                  func() time.Time { return now },
		MaximumAge:             time.Hour,
		MaximumEntriesPerOwner: 1_000,
	})
	ctx := context.Background()
	scope := AccessScope{TenantID: "tenant", OwnerID: "owner"}
	total := retentionPruneBatchSize + 44
	for index := range total {
		id := fmt.Sprintf("expired-%03d", index)
		if _, err := store.Record(
			ctx,
			scope,
			historyEntry(
				id,
				"index=main | head 1",
				"search",
				opensplunkv1.SearchJobState_SEARCH_JOB_STATE_COMPLETED,
				now,
			),
		); err != nil {
			t.Fatalf("Record(%s) error = %v", id, err)
		}
	}
	now = now.Add(2 * time.Hour)
	if _, err := database.SQLDB().ExecContext(ctx, `
		CREATE TRIGGER fail_late_search_history_delete
		BEFORE DELETE ON search_history
		WHEN OLD.search_job_id = 'expired-299'
		BEGIN
			SELECT RAISE(ABORT, 'injected late prune failure');
		END
	`); err != nil {
		t.Fatal(err)
	}

	deleted, err := store.Prune(ctx, scope)
	if err == nil {
		t.Fatal("Prune() unexpectedly succeeded through injected failure")
	}
	if errors.Is(err, control.ErrInvalidArgument) {
		t.Fatalf("Prune() classified storage failure as invalid input: %v", err)
	}
	if deleted != retentionPruneBatchSize {
		t.Fatalf(
			"Prune() committed rows = %d, want first bounded batch %d",
			deleted,
			retentionPruneBatchSize,
		)
	}
	if remaining := maintenanceTerminalCount(t, database); remaining != 44 {
		t.Fatalf("rows after failed second batch = %d, want 44", remaining)
	}

	if _, err := database.SQLDB().ExecContext(
		ctx,
		`DROP TRIGGER fail_late_search_history_delete`,
	); err != nil {
		t.Fatal(err)
	}
	deleted, err = store.Prune(ctx, scope)
	if err != nil || deleted != 44 {
		t.Fatalf("retry Prune() = (%d, %v), want (44, nil)", deleted, err)
	}
	if remaining := maintenanceTerminalCount(t, database); remaining != 0 {
		t.Fatalf("rows after retry = %d, want 0", remaining)
	}
}

func maintenanceTerminalIDs(t *testing.T, database *control.DB) []string {
	t.Helper()
	rows, err := database.SQLDB().QueryContext(
		context.Background(),
		`SELECT search_job_id FROM search_history ORDER BY search_job_id`,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var result []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			t.Fatal(err)
		}
		result = append(result, id)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return result
}

func maintenanceTerminalCount(t *testing.T, database *control.DB) int {
	t.Helper()
	var count int
	if err := database.SQLDB().QueryRowContext(
		context.Background(),
		`SELECT COUNT(*) FROM search_history`,
	).Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count
}

func maintenanceOwnerTerminalCount(
	t *testing.T,
	database *control.DB,
	scope AccessScope,
) int64 {
	t.Helper()
	var count int64
	if err := database.SQLDB().QueryRowContext(
		context.Background(),
		`SELECT terminal_count
		 FROM search_history_owner_counts
		 WHERE tenant_id = ? AND owner_id = ?`,
		scope.TenantID,
		scope.OwnerID,
	).Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count
}
