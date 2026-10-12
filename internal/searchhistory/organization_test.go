package searchhistory

import (
	"errors"
	"testing"
	"time"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/control"
)

func TestOrganizationScopesFenceHistoryCountsCursorsAndMutations(t *testing.T) {
	now := time.Date(2026, time.July, 22, 12, 0, 0, 0, time.UTC)
	_, store := openTestStore(t, Options{Clock: func() time.Time { return now }})
	one := AccessScope{TenantID: "physical", OwnerID: "9", OrganizationID: "1"}
	two := one
	two.OrganizationID = "2"
	other := one
	other.OwnerID = "10"
	for _, fixture := range []struct {
		scope AccessScope
		id    string
	}{{one, "one-a"}, {one, "one-b"}, {two, "two"}, {other, "private-other"}} {
		if _, err := store.Record(t.Context(), fixture.scope, historyEntry(fixture.id, "index=main", "", opensplunk.SearchJobState_SEARCH_JOB_STATE_COMPLETED, now.Add(-time.Minute))); err != nil {
			t.Fatal(err)
		}
	}
	for _, id := range []string{"two", "private-other"} {
		if _, err := store.Get(t.Context(), one, id); !errors.Is(err, control.ErrNotFound) {
			t.Fatalf("history scope disclosed %s: %v", id, err)
		}
	}
	page, err := store.List(t.Context(), one, ListRequest{PageSize: 1, IncludeTotal: true})
	if err != nil || page.TotalSize == nil || *page.TotalSize != 2 || page.NextPageToken == nil {
		t.Fatalf("scoped page %#v %v", page, err)
	}
	if _, err = store.List(t.Context(), two, ListRequest{PageSize: 1, IncludeTotal: true, PageToken: *page.NextPageToken}); err == nil {
		t.Fatal("org continuation cursor replayed in another org")
	}
	if err = store.Delete(t.Context(), one, "two"); !errors.Is(err, control.ErrNotFound) {
		t.Fatalf("crossorg delete %v", err)
	}
	site := AccessScope{TenantID: "physical", OwnerID: "11", SiteAdministrator: true}
	all, err := store.List(t.Context(), site, ListRequest{PageSize: 15, IncludeTotal: true})
	if err != nil || all.TotalSize == nil || *all.TotalSize != 4 {
		t.Fatalf("site history %#v %v", all, err)
	}
	site.OrganizationID = "1"
	scoped, err := store.List(t.Context(), site, ListRequest{PageSize: 15, IncludeTotal: true})
	if err != nil || scoped.TotalSize == nil || *scoped.TotalSize != 3 {
		t.Fatalf("selectedsite org %#v %v", scoped, err)
	}
	if _, err = store.Clear(t.Context(), one, Filter{}); err != nil {
		t.Fatal(err)
	}
	if _, err = store.Get(t.Context(), two, "two"); err != nil {
		t.Fatal("clearing oneorg removed anotherorg history", err)
	}
}

func TestOrganizationStartupRecoveryIncludesEveryOwnerOrganization(t *testing.T) {
	now := time.Date(2026, time.July, 22, 12, 0, 0, 0, time.UTC)
	_, store := openTestStore(t, Options{Clock: func() time.Time { return now }})
	for _, fixture := range []struct {
		scope AccessScope
		id    string
	}{{AccessScope{TenantID: "physical", OwnerID: "9", OrganizationID: "1"}, "one"}, {AccessScope{TenantID: "physical", OwnerID: "9", OrganizationID: "2"}, "two"}, {AccessScope{TenantID: "other", OwnerID: "9", OrganizationID: "1"}, "other"}} {
		if _, err := store.BeginAttempt(t.Context(), fixture.scope, pendingHistoryEntry(fixture.id, "index=main", now.Add(-time.Minute))); err != nil {
			t.Fatal(err)
		}
	}
	count, err := store.RecoverInterruptedTenant(t.Context(), "physical")
	if err != nil || count != 2 {
		t.Fatalf("recovery %d %v", count, err)
	}
	for _, org := range []string{"1", "2"} {
		page, err := store.List(t.Context(), AccessScope{TenantID: "physical", OwnerID: "9", OrganizationID: org}, ListRequest{PageSize: 15, IncludeTotal: true})
		if err != nil || len(page.Entries) != 1 || page.Entries[0].GetFinalState() != opensplunk.SearchJobState_SEARCH_JOB_STATE_FAILED {
			t.Fatalf("org%s recovered %#v %v", org, page, err)
		}
	}
}
