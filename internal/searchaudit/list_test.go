package searchaudit

import (
	"bytes"
	"context"
	"errors"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"gorm.io/gorm"

	"github.com/Suhaibinator/open-splunk/internal/audit"
	"github.com/Suhaibinator/open-splunk/internal/control"
)

func TestListPaginationFiltersTotalsAndCursorAuthentication(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	database := openSearchAuditTestDatabase(t)
	store := newSearchAuditTestStore(t, database, searchAuditTestCursorKey(), 10)
	alice := audit.Actor{Kind: audit.ActorKindBrowser, ID: "alice", Role: audit.ActorRoleUser}
	aliceContext, err := audit.WithActor(ctx, alice)
	if err != nil {
		t.Fatal(err)
	}
	bob := audit.Actor{Kind: audit.ActorKindBrowser, ID: "bob", Role: audit.ActorRoleAdministrator}
	bobContext, err := audit.WithActor(ctx, bob)
	if err != nil {
		t.Fatal(err)
	}
	appendSearchAuditTestEvent(t, store, database, ctx, "tenant-page", searchAuditTestDefinition("owner-a", "job-1", time.Microsecond))
	appendSearchAuditTestEvent(t, store, database, aliceContext, "tenant-page", searchAuditTestDefinition("owner-a", "job-2", 2*time.Microsecond))
	appendSearchAuditTestEvent(t, store, database, bobContext, "tenant-page", searchAuditTestDefinition("owner-b", "job-3", 3*time.Microsecond))
	appendSearchAuditTestEvent(t, store, database, aliceContext, "tenant-page", searchAuditTestDefinition("owner-b", "job-4", 4*time.Microsecond))
	appendSearchAuditTestEvent(t, store, database, ctx, "other-tenant", searchAuditTestDefinition("owner-a", "other-job", 5*time.Microsecond))

	request := ListRequest{PageSize: 2, IncludeTotal: true}
	first, err := store.List(ctx, "tenant-page", request)
	if err != nil {
		t.Fatalf("List(first): %v", err)
	}
	if got := searchAuditSequences(first.Events); !slices.Equal(got, []uint64{4, 3}) ||
		first.NextPageToken == "" || first.TotalSize == nil || *first.TotalSize != 4 ||
		!first.TotalSizeExact {
		t.Fatalf("first = %+v, sequences=%v", first, got)
	}
	request.PageToken = first.NextPageToken
	second, err := store.List(ctx, "tenant-page", request)
	if err != nil {
		t.Fatalf("List(second): %v", err)
	}
	if got := searchAuditSequences(second.Events); !slices.Equal(got, []uint64{2, 1}) ||
		second.NextPageToken != "" || second.TotalSize == nil || *second.TotalSize != 4 ||
		!second.TotalSizeExact {
		t.Fatalf("second = %+v, sequences=%v", second, got)
	}

	aliceID := "alice"
	ownerB := "owner-b"
	filtered, err := store.List(ctx, "tenant-page", ListRequest{
		ActorID: &aliceID, OwnerID: &ownerB, IncludeTotal: true,
	})
	if err != nil || len(filtered.Events) != 1 || filtered.Events[0].Sequence != 4 ||
		filtered.TotalSize == nil || *filtered.TotalSize != 1 {
		t.Fatalf("filtered = (%+v, %v)", filtered, err)
	}

	tampered := first.NextPageToken[:len(first.NextPageToken)-1] + "A"
	if tampered == first.NextPageToken {
		tampered = first.NextPageToken[:len(first.NextPageToken)-1] + "B"
	}
	assertInvalidSearchAuditCursor(t, store, "tenant-page", ListRequest{
		PageSize: 2, PageToken: tampered, IncludeTotal: true,
	})
	assertInvalidSearchAuditCursor(t, store, "tenant-page", ListRequest{
		PageSize: 3, PageToken: first.NextPageToken, IncludeTotal: true,
	})
	assertInvalidSearchAuditCursor(t, store, "tenant-page", ListRequest{
		PageSize: 2, PageToken: first.NextPageToken,
	})
	assertInvalidSearchAuditCursor(t, store, "other-tenant", ListRequest{
		PageSize: 2, PageToken: first.NextPageToken, IncludeTotal: true,
	})
	assertInvalidSearchAuditCursor(t, store, "tenant-page", ListRequest{
		PageSize: 2, PageToken: first.NextPageToken, IncludeTotal: true, ActorID: &aliceID,
	})
	wrongKeyStore := newSearchAuditTestStore(
		t,
		database,
		bytes.Repeat([]byte{0x2f}, minimumCursorKeyBytes),
		10,
	)
	assertInvalidSearchAuditCursor(t, wrongKeyStore, "tenant-page", ListRequest{
		PageSize: 2, PageToken: first.NextPageToken, IncludeTotal: true,
	})
}

func TestListCursorSurvivesAppendAndReopenBeforePruning(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	directory := t.TempDir()
	path := filepath.Join(directory, "control.db")
	database, err := control.Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if database != nil {
			_ = database.Close()
		}
	}()
	key := searchAuditTestCursorKey()
	store := newSearchAuditTestStore(t, database, key, 6)
	for index := 1; index <= 4; index++ {
		appendSearchAuditTestEvent(t, store, database, ctx, "tenant-reopen", searchAuditTestDefinition(
			"owner", "job-"+string(rune('0'+index)), time.Duration(index)*time.Microsecond,
		))
	}
	first, err := store.List(ctx, "tenant-reopen", ListRequest{PageSize: 2, IncludeTotal: true})
	if err != nil || first.NextPageToken == "" {
		t.Fatalf("List(first) = (%+v, %v)", first, err)
	}
	appendSearchAuditTestEvent(t, store, database, ctx, "tenant-reopen", searchAuditTestDefinition(
		"owner", "job-5", 5*time.Microsecond,
	))
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	database = nil
	database, err = control.Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	store = newSearchAuditTestStore(t, database, key, 6)
	second, err := store.List(ctx, "tenant-reopen", ListRequest{
		PageSize: 2, PageToken: first.NextPageToken, IncludeTotal: true,
	})
	if err != nil {
		t.Fatalf("List(second after reopen): %v", err)
	}
	if got := searchAuditSequences(second.Events); !slices.Equal(got, []uint64{2, 1}) ||
		second.NextPageToken != "" || second.TotalSize == nil || *second.TotalSize != 4 {
		t.Fatalf("second = %+v, sequences=%v", second, got)
	}
}

func TestListUsesBoundedStatementBudget(t *testing.T) {
	ctx := context.Background()
	database := openSearchAuditTestDatabase(t)
	store := newSearchAuditTestStore(t, database, searchAuditTestCursorKey(), 10)
	for index := 1; index <= 4; index++ {
		appendSearchAuditTestEvent(
			t,
			store,
			database,
			ctx,
			"tenant-list-hot-path",
			searchAuditTestDefinition(
				"owner",
				"job-"+string(rune('0'+index)),
				time.Duration(index)*time.Microsecond,
			),
		)
	}

	counter := &searchAuditStatementCounter{}
	instrumented := *store
	instrumented.orm = store.orm.Session(&gorm.Session{
		Logger: countingSearchAuditLogger{counter: counter},
	})

	first, err := instrumented.List(ctx, "tenant-list-hot-path", ListRequest{
		PageSize: 2, IncludeTotal: true,
	})
	if err != nil || first.NextPageToken == "" {
		t.Fatalf("List(first) = (%+v, %v)", first, err)
	}
	const maximumListStatements = 5
	if got := counter.value(); got < 1 || got > maximumListStatements {
		t.Fatalf("first-page list statements = %d, want 1..%d", got, maximumListStatements)
	}

	appendSearchAuditTestEvent(
		t,
		store,
		database,
		ctx,
		"tenant-list-hot-path",
		searchAuditTestDefinition("owner", "job-5", 5*time.Microsecond),
	)
	counter.reset()
	second, err := instrumented.List(ctx, "tenant-list-hot-path", ListRequest{
		PageSize: 2, PageToken: first.NextPageToken, IncludeTotal: true,
	})
	if err != nil || second.NextPageToken != "" ||
		!slices.Equal(searchAuditSequences(second.Events), []uint64{2, 1}) {
		t.Fatalf("List(second) = (%+v, %v)", second, err)
	}
	if got := counter.value(); got < 1 || got > maximumListStatements {
		t.Fatalf("continuation list statements = %d, want 1..%d", got, maximumListStatements)
	}
}

func TestListCursorFailsClosedWhenRollingPruneRemovesSnapshotRow(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	database := openSearchAuditTestDatabase(t)
	store := newSearchAuditTestStore(t, database, searchAuditTestCursorKey(), 4)
	for index := 1; index <= 4; index++ {
		appendSearchAuditTestEvent(t, store, database, ctx, "tenant-prune", searchAuditTestDefinition(
			"owner", "job-"+string(rune('0'+index)), time.Duration(index)*time.Microsecond,
		))
	}
	first, err := store.List(ctx, "tenant-prune", ListRequest{PageSize: 2})
	if err != nil || first.NextPageToken == "" {
		t.Fatalf("List(first) = (%+v, %v)", first, err)
	}
	appendSearchAuditTestEvent(t, store, database, ctx, "tenant-prune", searchAuditTestDefinition(
		"owner", "job-5", 5*time.Microsecond,
	))
	assertInvalidSearchAuditCursor(t, store, "tenant-prune", ListRequest{
		PageSize: 2, PageToken: first.NextPageToken,
	})
}

func TestListRejectsInvalidAndCanceledRequests(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	database := openSearchAuditTestDatabase(t)
	store := newSearchAuditTestStore(t, database, searchAuditTestCursorKey(), 5)
	bad := " owner"
	for _, request := range []ListRequest{
		{PageSize: MaximumListPageSize + 1},
		{PageToken: string(bytes.Repeat([]byte{'x'}, maximumListCursorBytes+1))},
		{ActorID: &bad},
		{OwnerID: &bad},
	} {
		if page, err := store.List(ctx, "tenant", request); len(page.Events) != 0 ||
			!errors.Is(err, control.ErrInvalidArgument) || errors.Is(err, ErrInvalidCursor) {
			t.Fatalf("List(invalid) = (%+v, %v)", page, err)
		}
	}
	if page, err := store.List(ctx, "tenant", ListRequest{PageToken: "bad"}); len(page.Events) != 0 || !errors.Is(err, ErrInvalidCursor) {
		t.Fatalf("List(bad cursor) = (%+v, %v)", page, err)
	}
	var nilContext context.Context

	if _, err := store.List(nilContext, "tenant", ListRequest{}); !errors.Is(err, control.ErrInvalidArgument) {
		t.Fatalf("List(nil) error = %v", err)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := store.List(canceled, "tenant", ListRequest{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("List(canceled) error = %v", err)
	}
	appendOnly := newSearchAuditTestStore(t, database, nil, 5)
	if _, err := appendOnly.List(ctx, "tenant", ListRequest{}); !errors.Is(err, control.ErrInvalidArgument) {
		t.Fatalf("append-only List error = %v", err)
	}
}

func assertInvalidSearchAuditCursor(
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
