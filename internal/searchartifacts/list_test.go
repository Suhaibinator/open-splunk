package searchartifacts

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
)

func TestListPageOrdersScopesFiltersAndDoesNotRefresh(t *testing.T) {
	ctx := context.Background()
	clock := &testClock{now: time.Date(2026, 8, 30, 15, 0, 0, 0, time.UTC)}
	store, _, _ := newTestStore(t, clock, DefaultMaximumBytes)

	jobs := []searchjobs.Job{
		listTestJob(t, "same-b", clock.now, "owner", "search", "index=main ERROR"),
		listTestJob(t, "same-a", clock.now, "owner", "search", "index=main error"),
		listTestJob(t, "older", clock.now.Add(-time.Minute), "owner", "other", "index=main error"),
		listTestJob(t, "shared", clock.now.Add(-2*time.Minute), "other", "search", "index=main error"),
		listTestJob(t, "private", clock.now.Add(-3*time.Minute), "other", "search", "index=main error"),
	}
	for _, job := range jobs {
		if err := store.Admit(ctx, job); err != nil {
			t.Fatal(err)
		}
		if err := store.Finalize(ctx, completeTestJob(job, job.CreatedAt, time.Hour)); err != nil {
			t.Fatal(err)
		}
	}
	otherAccess := searchjobs.AccessScope{TenantID: "tenant", OwnerID: "other"}
	sharedJob := completeTestJob(jobs[3], jobs[3].CreatedAt, time.Hour)
	if _, err := store.PersistResults(ctx, otherAccess, "shared", &sourceLease{
		schema: *sharedJob.Schema, rows: testRows(t), generation: 1,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Share(ctx, otherAccess, "shared"); err != nil {
		t.Fatal(err)
	}
	beforeList := make(map[string]Record, 3)
	for _, id := range []string{"same-b", "same-a"} {
		record, err := store.Get(ctx, testAccess(), id, AccessInspect)
		if err != nil {
			t.Fatal(err)
		}
		beforeList[id] = record
	}
	sharedRecord, err := store.Get(ctx, otherAccess, "shared", AccessInspect)
	if err != nil {
		t.Fatal(err)
	}
	beforeList["shared"] = sharedRecord

	app, text := "search", "ErRoR"
	first, err := store.ListPage(ctx, testAccess(), ListRequest{
		PageSize: 2, AppIDFilter: &app, TextFilter: &text,
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := listItemIDs(first.Items); !slices.Equal(got, []string{"same-b", "same-a"}) {
		t.Fatalf("first page IDs = %v", got)
	}
	if first.NextPageToken == "" || first.FirstPageToken == "" ||
		first.Items[0].AfterPageToken == "" || first.Items[1].AfterPageToken != first.NextPageToken {
		t.Fatalf("first page cursors = %+v", first)
	}

	inserted := listTestJob(t, "inserted", clock.now.Add(time.Minute), "owner", "search", "index=main error")
	if err := store.Admit(ctx, inserted); err != nil {
		t.Fatal(err)
	}
	second, err := store.ListPage(ctx, testAccess(), ListRequest{
		PageSize: 2, PageToken: first.NextPageToken, AppIDFilter: &app, TextFilter: &text,
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := listItemIDs(second.Items); !slices.Equal(got, []string{"shared"}) {
		t.Fatalf("second page IDs = %v", got)
	}
	for _, item := range append(first.Items, second.Items...) {
		before := beforeList[item.Record.Job.ID]
		if !item.Record.LastAccessedAt.Equal(before.LastAccessedAt) ||
			!item.Record.ExpiresAt.Equal(before.ExpiresAt) {
			t.Fatalf("list refreshed or lost retention for %q: %+v", item.Record.Job.ID, item.Record)
		}
	}

	changedApp := "other"
	if _, err := store.ListPage(ctx, testAccess(), ListRequest{
		PageSize: 2, PageToken: first.NextPageToken, AppIDFilter: &changedApp, TextFilter: &text,
	}); !errors.Is(err, ErrInvalidCursor) {
		t.Fatalf("filter-bound cursor error = %v", err)
	}
	otherTenant := searchjobs.AccessScope{TenantID: "other", OwnerID: "owner"}
	if _, err := store.ListPage(ctx, otherTenant, ListRequest{
		PageSize: 2, PageToken: first.NextPageToken, AppIDFilter: &app, TextFilter: &text,
	}); !errors.Is(err, ErrInvalidCursor) {
		t.Fatalf("scope-bound cursor error = %v", err)
	}
}

func TestListPageProjectsExpiredAndInterruptedCandidates(t *testing.T) {
	ctx := context.Background()
	clock := &testClock{now: time.Date(2026, 8, 30, 16, 0, 0, 0, time.UTC)}
	store, database, directory := newTestStore(t, clock, DefaultMaximumBytes)

	expired := listTestJob(t, "expired", clock.now.Add(-time.Minute), "owner", "search", "index=main")
	if err := store.Admit(ctx, expired); err != nil {
		t.Fatal(err)
	}
	expiredCompleted := completeTestJob(expired, clock.now, time.Second)
	if err := store.Finalize(ctx, expiredCompleted); err != nil {
		t.Fatal(err)
	}
	if _, err := store.PersistResults(ctx, testAccess(), expired.ID, &sourceLease{
		schema: *expiredCompleted.Schema, rows: testRows(t), generation: 1,
	}); err != nil {
		t.Fatal(err)
	}
	interrupted := listTestJob(t, "interrupted", clock.now, "owner", "search", "index=main")
	if err := store.Admit(ctx, interrupted); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	clock.now = clock.now.Add(2 * time.Second)
	store = openTestStore(t, database.SQLDB(), directory, clock, DefaultMaximumBytes)

	page, err := store.ListPage(ctx, testAccess(), ListRequest{
		PageSize: 10, StateFilters: []State{StateInterrupted, StateExpired},
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := listItemIDs(page.Items); !slices.Equal(got, []string{"interrupted", "expired"}) {
		t.Fatalf("terminal durable IDs = %v", got)
	}
	if page.Items[0].Record.State != StateInterrupted ||
		page.Items[1].Record.State != StateExpired {
		t.Fatalf("terminal durable records = %+v", page.Items)
	}
}

func TestListPageValidatesCancellationAndTampering(t *testing.T) {
	clock := &testClock{now: time.Date(2026, 8, 30, 17, 0, 0, 0, time.UTC)}
	store, _, _ := newTestStore(t, clock, DefaultMaximumBytes)
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := store.ListPage(canceled, testAccess(), ListRequest{PageSize: 1}); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled list error = %v", err)
	}
	for _, request := range []ListRequest{
		{},
		{PageSize: MaximumListPageSize + 1},
		{PageSize: 1, StateFilters: []State{StateInvalid}},
		{PageSize: 1, PageToken: "not-a-cursor"},
	} {
		if _, err := store.ListPage(context.Background(), testAccess(), request); err == nil {
			t.Fatalf("ListPage(%+v) accepted", request)
		}
	}
}

func listTestJob(
	t *testing.T,
	id string,
	created time.Time,
	owner string,
	appID string,
	spl string,
) searchjobs.Job {
	t.Helper()
	job := testQueuedJob(t, id, created)
	job.OwnerID = owner
	job.AppID = appID
	job.SPL = spl
	return job
}

func listItemIDs(items []ListItem) []string {
	result := make([]string, len(items))
	for index, item := range items {
		result[index] = item.Record.Job.ID
	}
	return result
}
