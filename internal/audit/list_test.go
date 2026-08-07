package audit

import (
	"bytes"
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/control"
)

func appendAuditTestEvent(
	t *testing.T,
	store *Store,
	ctx context.Context,
	tenantID string,
	action Action,
	targetID string,
	version uint64,
) Event {
	t.Helper()
	event, err := store.Append(ctx, tenantID, auditTestDefinition(action, targetID, version))
	if err != nil {
		t.Fatalf("Append(%s, %s): %v", action, targetID, err)
	}
	return event
}

func eventSequences(events []Event) []uint64 {
	result := make([]uint64, len(events))
	for index, event := range events {
		result[index] = event.Sequence
	}
	return result
}

func TestListDescendingKeysetIsStableAcrossAppendsAndRestart(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "control.db")
	database, err := control.Open(ctx, path)
	if err != nil {
		t.Fatalf("control.Open: %v", err)
	}
	defer func() {
		if database != nil {
			if closeErr := database.Close(); closeErr != nil {
				t.Errorf("close control database: %v", closeErr)
			}
		}
	}()
	key := auditTestCursorKey()
	store := newAuditTestStore(t, database, key)

	appendAuditTestEvent(t, store, ctx, "tenant-page", ActionIngestionTokenCreate, "token-a", 1)
	appendAuditTestEvent(t, store, ctx, "tenant-page", ActionIngestionTokenUpdate, "token-a", 2)
	appendAuditTestEvent(t, store, ctx, "tenant-page", ActionIngestionTokenRevoke, "token-a", 3)
	appendAuditTestEvent(t, store, ctx, "tenant-page", ActionIngestionTokenCreate, "token-b", 1)
	appendAuditTestEvent(t, store, ctx, "tenant-page", ActionIngestionTokenUpdate, "token-b", 2)

	request := ListRequest{PageSize: 2, IncludeTotal: true}
	first, err := store.List(ctx, "tenant-page", request)
	if err != nil {
		t.Fatalf("List(first): %v", err)
	}
	if got := eventSequences(first.Events); !slicesEqual(got, []uint64{5, 4}) ||
		first.NextPageToken == "" || first.TotalSize == nil || *first.TotalSize != 5 ||
		!first.TotalSizeExact {
		t.Fatalf("first page = %+v, sequences %v", first, got)
	}

	// A later append sorts above the cursor and must never enter this traversal.
	appendAuditTestEvent(t, store, ctx, "tenant-page", ActionIngestionTokenCreate, "token-c", 1)
	if err := database.Close(); err != nil {
		t.Fatalf("close before restart: %v", err)
	}
	database = nil
	database, err = control.Open(ctx, path)
	if err != nil {
		t.Fatalf("control.Open(restart): %v", err)
	}
	store = newAuditTestStore(t, database, key)

	request.PageToken = first.NextPageToken
	second, err := store.List(ctx, "tenant-page", request)
	if err != nil {
		t.Fatalf("List(second after restart): %v", err)
	}
	if got := eventSequences(second.Events); !slicesEqual(got, []uint64{3, 2}) ||
		second.NextPageToken == "" || second.TotalSize == nil || *second.TotalSize != 5 {
		t.Fatalf("second page = %+v, sequences %v", second, got)
	}
	request.PageToken = second.NextPageToken
	third, err := store.List(ctx, "tenant-page", request)
	if err != nil {
		t.Fatalf("List(third): %v", err)
	}
	if got := eventSequences(third.Events); !slicesEqual(got, []uint64{1}) ||
		third.NextPageToken != "" || third.TotalSize == nil || *third.TotalSize != 5 {
		t.Fatalf("third page = %+v, sequences %v", third, got)
	}

	tampered := first.NextPageToken[:len(first.NextPageToken)-1] + "A"
	if tampered == first.NextPageToken {
		tampered = first.NextPageToken[:len(first.NextPageToken)-1] + "B"
	}
	assertInvalidAuditCursor(t, store, "tenant-page", ListRequest{
		PageSize: 2, PageToken: tampered, IncludeTotal: true,
	})
	assertInvalidAuditCursor(t, store, "tenant-page", ListRequest{
		PageSize: 3, PageToken: first.NextPageToken, IncludeTotal: true,
	})
	assertInvalidAuditCursor(t, store, "tenant-page", ListRequest{
		PageSize: 2, PageToken: first.NextPageToken,
	})
	assertInvalidAuditCursor(t, store, "other-tenant", ListRequest{
		PageSize: 2, PageToken: first.NextPageToken, IncludeTotal: true,
	})
	actorID := "open-splunk-server"
	assertInvalidAuditCursor(t, store, "tenant-page", ListRequest{
		PageSize: 2, PageToken: first.NextPageToken, IncludeTotal: true, ActorID: &actorID,
	})
	targetKind := TargetKindIngestionToken
	assertInvalidAuditCursor(t, store, "tenant-page", ListRequest{
		PageSize: 2, PageToken: first.NextPageToken, IncludeTotal: true, TargetKind: &targetKind,
	})
	assertInvalidAuditCursor(t, store, "tenant-page", ListRequest{
		PageSize: 2, PageToken: first.NextPageToken, IncludeTotal: true,
		ActionFilters: []Action{ActionIngestionTokenCreate},
	})
	wrongKeyStore := newAuditTestStore(
		t,
		database,
		bytes.Repeat([]byte{0x6b}, minimumCursorKeyBytes),
	)
	assertInvalidAuditCursor(t, wrongKeyStore, "tenant-page", ListRequest{
		PageSize: 2, PageToken: first.NextPageToken, IncludeTotal: true,
	})
}

func TestListFiltersRemainTenantScopedAndCanonical(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	_, database := openAuditTestDatabase(t)
	store := newAuditTestStore(t, database, auditTestCursorKey())

	appendAuditTestEvent(t, store, ctx, "tenant-filter", ActionIngestionTokenCreate, "token-a", 1)
	aliceContext, err := WithActor(ctx, Actor{
		Kind: ActorKindBrowser, ID: "alice", Role: ActorRoleAdministrator,
	})
	if err != nil {
		t.Fatalf("WithActor(alice): %v", err)
	}
	appendAuditTestEvent(t, store, aliceContext, "tenant-filter", ActionIngestionTokenUpdate, "token-a", 2)
	bobContext, err := WithActor(ctx, Actor{
		Kind: ActorKindBrowser, ID: "bob", Role: ActorRoleAdministrator,
	})
	if err != nil {
		t.Fatalf("WithActor(bob): %v", err)
	}
	appendAuditTestEvent(t, store, bobContext, "tenant-filter", ActionIngestionTokenRevoke, "token-a", 3)
	appendAuditTestEvent(t, store, ctx, "tenant-other", ActionIngestionTokenCreate, "token-other", 1)

	page, err := store.List(ctx, "tenant-filter", ListRequest{
		ActionFilters: []Action{
			ActionIngestionTokenRevoke,
			ActionIngestionTokenCreate,
			ActionIngestionTokenCreate,
		},
		IncludeTotal: true,
	})
	if err != nil {
		t.Fatalf("List(action filters): %v", err)
	}
	if got := eventSequences(page.Events); !slicesEqual(got, []uint64{3, 1}) ||
		page.TotalSize == nil || *page.TotalSize != 2 {
		t.Fatalf("action-filtered page = %+v, sequences %v", page, got)
	}
	alice := "alice"
	page, err = store.List(ctx, "tenant-filter", ListRequest{ActorID: &alice})
	if err != nil || len(page.Events) != 1 || page.Events[0].Sequence != 2 {
		t.Fatalf("actor-filtered page = (%+v, %v)", page, err)
	}
	kind := TargetKindIngestionToken
	page, err = store.List(ctx, "tenant-filter", ListRequest{TargetKind: &kind})
	if err != nil || !slicesEqual(eventSequences(page.Events), []uint64{3, 2, 1}) {
		t.Fatalf("target-filtered page = (%+v, %v)", page, err)
	}
}

func TestListCursorCanonicalizesEmptyActionFilters(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	_, database := openAuditTestDatabase(t)
	store := newAuditTestStore(t, database, auditTestCursorKey())
	appendAuditTestEvent(t, store, ctx, "tenant-empty-filter", ActionIngestionTokenCreate, "token", 1)
	appendAuditTestEvent(t, store, ctx, "tenant-empty-filter", ActionIngestionTokenUpdate, "token", 2)

	first, err := store.List(ctx, "tenant-empty-filter", ListRequest{
		PageSize:      1,
		ActionFilters: []Action{},
	})
	if err != nil || first.NextPageToken == "" {
		t.Fatalf("List(explicit empty filter) = (%+v, %v)", first, err)
	}
	second, err := store.List(ctx, "tenant-empty-filter", ListRequest{
		PageSize:  1,
		PageToken: first.NextPageToken,
	})
	if err != nil || len(second.Events) != 1 || second.Events[0].Sequence != 1 {
		t.Fatalf("List(nil-filter continuation) = (%+v, %v)", second, err)
	}
}

func TestListPagesAreCallerOwnedAndCursorStateRemainsStable(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	_, database := openAuditTestDatabase(t)
	store := newAuditTestStore(t, database, auditTestCursorKey())
	appendAuditTestEvent(t, store, ctx, "tenant-owned", ActionIngestionTokenCreate, "token", 1)
	appendAuditTestEvent(t, store, ctx, "tenant-owned", ActionIngestionTokenUpdate, "token", 2)

	request := ListRequest{PageSize: 1, IncludeTotal: true}
	first, err := store.List(ctx, "tenant-owned", request)
	if err != nil || len(first.Events) != 1 || first.TotalSize == nil ||
		first.NextPageToken == "" {
		t.Fatalf("List(first) = (%+v, %v)", first, err)
	}
	second, err := store.List(ctx, "tenant-owned", request)
	if err != nil || len(second.Events) != 1 || second.TotalSize == nil {
		t.Fatalf("List(second) = (%+v, %v)", second, err)
	}
	cursor := first.NextPageToken
	first.Events[0].Sequence = 0
	first.Events[0].TenantID = "mutated"
	first.Events = append(first.Events, Event{Sequence: 99})
	*first.TotalSize = 0
	first.NextPageToken = "mutated"
	if second.Events[0].Sequence != 2 || second.Events[0].TenantID != "tenant-owned" ||
		len(second.Events) != 1 || *second.TotalSize != 2 || second.NextPageToken != cursor {
		t.Fatalf("caller mutation escaped page ownership: %+v", second)
	}

	continuation, err := store.List(ctx, "tenant-owned", ListRequest{
		PageSize: 1, PageToken: cursor, IncludeTotal: true,
	})
	if err != nil || len(continuation.Events) != 1 ||
		continuation.Events[0].Sequence != 1 || continuation.TotalSize == nil ||
		*continuation.TotalSize != 2 || continuation.NextPageToken != "" {
		t.Fatalf("List(continuation) = (%+v, %v)", continuation, err)
	}
}

func TestListRejectsInvalidRequestsSeparatelyFromInvalidCursors(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	_, database := openAuditTestDatabase(t)
	store := newAuditTestStore(t, database, auditTestCursorKey())
	badActor := " bad"
	badTarget := TargetKind("other")

	tests := []struct {
		name     string
		tenantID string
		request  ListRequest
	}{
		{name: "tenant", tenantID: " bad"},
		{name: "page size", tenantID: "tenant", request: ListRequest{PageSize: MaximumListPageSize + 1}},
		{name: "oversized token", tenantID: "tenant", request: ListRequest{PageToken: strings.Repeat("x", maximumListCursorBytes+1)}},
		{name: "unknown action", tenantID: "tenant", request: ListRequest{ActionFilters: []Action{"other"}}},
		{name: "too many actions", tenantID: "tenant", request: ListRequest{
			ActionFilters: append(allKnownAuditActions(), ActionAppCreate),
		}},
		{name: "actor", tenantID: "tenant", request: ListRequest{ActorID: &badActor}},
		{name: "target", tenantID: "tenant", request: ListRequest{TargetKind: &badTarget}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			page, err := store.List(ctx, test.tenantID, test.request)
			if len(page.Events) != 0 || !errors.Is(err, control.ErrInvalidArgument) ||
				errors.Is(err, ErrInvalidCursor) {
				t.Fatalf("List = (%+v, %v), want request error", page, err)
			}
		})
	}
	if page, err := store.List(ctx, "tenant", ListRequest{PageToken: "malformed"}); len(page.Events) != 0 || !errors.Is(err, ErrInvalidCursor) {
		t.Fatalf("List(malformed cursor) = (%+v, %v)", page, err)
	}
	//nolint:staticcheck // Explicitly verifies the exported nil-context guard.
	if page, err := store.List(nil, "tenant", ListRequest{}); len(page.Events) != 0 ||
		!errors.Is(err, control.ErrInvalidArgument) {
		t.Fatalf("List(nil context) = (%+v, %v)", page, err)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if page, err := store.List(canceled, "tenant", ListRequest{}); len(page.Events) != 0 ||
		!errors.Is(err, context.Canceled) {
		t.Fatalf("List(canceled context) = (%+v, %v)", page, err)
	}
}

func TestListCursorRejectsDatabaseRestoredBehindHighWater(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	directory := t.TempDir()
	path := filepath.Join(directory, "control.db")
	olderPath := filepath.Join(directory, "older.db")
	database, err := control.Open(ctx, path)
	if err != nil {
		t.Fatalf("control.Open: %v", err)
	}
	defer func() { _ = database.Close() }()
	store := newAuditTestStore(t, database, auditTestCursorKey())
	appendAuditTestEvent(t, store, ctx, "tenant-restore", ActionIngestionTokenCreate, "token-a", 1)
	if err := database.BackupTo(ctx, olderPath); err != nil {
		t.Fatalf("BackupTo: %v", err)
	}
	appendAuditTestEvent(t, store, ctx, "tenant-restore", ActionIngestionTokenUpdate, "token-a", 2)
	appendAuditTestEvent(t, store, ctx, "tenant-restore", ActionIngestionTokenRevoke, "token-a", 3)
	first, err := store.List(ctx, "tenant-restore", ListRequest{PageSize: 1})
	if err != nil || first.NextPageToken == "" {
		t.Fatalf("List(first) = (%+v, %v)", first, err)
	}

	older, err := control.Open(ctx, olderPath)
	if err != nil {
		t.Fatalf("control.Open(older): %v", err)
	}
	defer func() { _ = older.Close() }()
	olderStore := newAuditTestStore(t, older, auditTestCursorKey())
	assertInvalidAuditCursor(t, olderStore, "tenant-restore", ListRequest{
		PageSize: 1, PageToken: first.NextPageToken,
	})
}

func TestListCorruptionFailsClosedBeforeReturningRows(t *testing.T) {
	t.Parallel()
	t.Run("oversized text", func(t *testing.T) {
		ctx := context.Background()
		_, database := openAuditTestDatabase(t)
		store := newAuditTestStore(t, database, auditTestCursorKey())
		appendAuditTestEvent(t, store, ctx, "tenant-poison", ActionIngestionTokenCreate, "token", 1)
		database.SQLDB().SetMaxOpenConns(1)
		connection, err := database.SQLDB().Conn(ctx)
		if err != nil {
			t.Fatalf("acquire corruption connection: %v", err)
		}
		if _, err := connection.ExecContext(ctx, "DROP TRIGGER audit_event_update_is_forbidden"); err != nil {
			_ = connection.Close()
			t.Fatalf("drop update trigger: %v", err)
		}
		if _, err := connection.ExecContext(ctx, "PRAGMA ignore_check_constraints = ON"); err != nil {
			_ = connection.Close()
			t.Fatalf("ignore fixture constraints: %v", err)
		}
		if _, err := connection.ExecContext(ctx, `
			UPDATE audit_events
			SET actor_id = replace(hex(zeroblob(300)), '00', 'x')
			WHERE tenant_id = 'tenant-poison'`); err != nil {
			_ = connection.Close()
			t.Fatalf("poison actor ID: %v", err)
		}
		if err := connection.Close(); err != nil {
			t.Fatalf("close corruption connection: %v", err)
		}

		page, err := store.List(ctx, "tenant-poison", ListRequest{})
		if len(page.Events) != 0 || !errors.Is(err, ErrCorrupt) {
			t.Fatalf("List(poisoned row) = (%+v, %v)", page, err)
		}
	})

	t.Run("corrupt lookahead", func(t *testing.T) {
		ctx := context.Background()
		_, database := openAuditTestDatabase(t)
		store := newAuditTestStore(t, database, auditTestCursorKey())
		appendAuditTestEvent(t, store, ctx, "tenant-lookahead", ActionIngestionTokenCreate, "token", 1)
		appendAuditTestEvent(t, store, ctx, "tenant-lookahead", ActionIngestionTokenUpdate, "token", 2)
		database.SQLDB().SetMaxOpenConns(1)
		connection, err := database.SQLDB().Conn(ctx)
		if err != nil {
			t.Fatalf("acquire corruption connection: %v", err)
		}
		if _, err := connection.ExecContext(ctx, "DROP TRIGGER audit_event_update_is_forbidden"); err != nil {
			_ = connection.Close()
			t.Fatalf("drop update trigger: %v", err)
		}
		if _, err := connection.ExecContext(ctx, "PRAGMA ignore_check_constraints = ON"); err != nil {
			_ = connection.Close()
			t.Fatalf("ignore fixture constraints: %v", err)
		}
		if _, err := connection.ExecContext(ctx, `
			UPDATE audit_events
			SET target_kind = 'saved_search'
			WHERE tenant_id = 'tenant-lookahead' AND sequence = 1`); err != nil {
			_ = connection.Close()
			t.Fatalf("poison lookahead taxonomy: %v", err)
		}
		if _, err := connection.ExecContext(ctx, "PRAGMA ignore_check_constraints = OFF"); err != nil {
			_ = connection.Close()
			t.Fatalf("restore fixture constraints: %v", err)
		}
		if err := connection.Close(); err != nil {
			t.Fatalf("close corruption connection: %v", err)
		}

		page, err := store.List(ctx, "tenant-lookahead", ListRequest{PageSize: 1})
		if len(page.Events) != 0 || !errors.Is(err, ErrCorrupt) {
			t.Fatalf("List(corrupt lookahead) = (%+v, %v)", page, err)
		}
	})

	t.Run("accounting mismatch", func(t *testing.T) {
		ctx := context.Background()
		_, database := openAuditTestDatabase(t)
		store := newAuditTestStore(t, database, auditTestCursorKey())
		appendAuditTestEvent(t, store, ctx, "tenant-state", ActionIngestionTokenCreate, "token", 1)
		if err := database.GORMDB().Exec(
			"DROP TRIGGER audit_tenant_state_transition_is_valid",
		).Error; err != nil {
			t.Fatalf("drop state transition trigger: %v", err)
		}
		if err := database.GORMDB().Exec(`
			UPDATE audit_tenant_state
			SET event_count = event_count + 1,
			    next_sequence = next_sequence + 1
			WHERE tenant_id = ?`, "tenant-state").Error; err != nil {
			t.Fatalf("poison tenant state: %v", err)
		}
		page, err := store.List(ctx, "tenant-state", ListRequest{})
		if len(page.Events) != 0 || !errors.Is(err, ErrCorrupt) {
			t.Fatalf("List(poisoned state) = (%+v, %v)", page, err)
		}
		if event, err := store.Append(
			ctx,
			"tenant-state",
			auditTestDefinition(ActionIngestionTokenUpdate, "token", 2),
		); event != (Event{}) || !errors.Is(err, ErrCorrupt) {
			t.Fatalf("Append(poisoned state) = (%+v, %v)", event, err)
		}
	})
}

func assertInvalidAuditCursor(
	t *testing.T,
	store *Store,
	tenantID string,
	request ListRequest,
) {
	t.Helper()
	page, err := store.List(context.Background(), tenantID, request)
	if len(page.Events) != 0 || !errors.Is(err, ErrInvalidCursor) {
		t.Fatalf("List(invalid cursor) = (%+v, %v)", page, err)
	}
}

func slicesEqual(left, right []uint64) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}
