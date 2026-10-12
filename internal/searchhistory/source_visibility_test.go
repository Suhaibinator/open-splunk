package searchhistory

import (
	"context"
	"errors"
	"testing"
	"time"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/resourcepolicy"
)

func TestHistorySourceVisibilityPrecedesCountsAndCursor(t *testing.T) {
	now := testStoreClock()
	database, store := openTestStore(t, Options{Clock: func() time.Time { return now }})
	if err := database.GORMDB().Exec(`CREATE TABLE os_index_organizations(index_id text PRIMARY KEY,organization_id bigint NOT NULL)`).Error; err != nil {
		t.Fatal(err)
	}
	scope := AccessScope{TenantID: "physical", OwnerID: "9", OrganizationID: "1"}
	for _, item := range []struct{ id, spl string }{{"visible-one", "index=main"}, {"hidden", "index=secret"}, {"visible-two", "index=main | stats count"}} {
		if _, err := store.Record(t.Context(), scope, historyEntry(item.id, item.spl, "", opensplunk.SearchJobState_SEARCH_JOB_STATE_COMPLETED, now.Add(-time.Minute))); err != nil {
			t.Fatal(err)
		}
	}
	policy := func(revision string) context.Context {
		return resourcepolicy.WithProvider(t.Context(), func(context.Context) (resourcepolicy.Snapshot, error) {
			return resourcepolicy.Snapshot{Fingerprint: revision, IndexNames: []string{"main"}}, nil
		})
	}
	page, err := store.List(policy("one"), scope, ListRequest{PageSize: 1, IncludeTotal: true})
	if err != nil || len(page.Entries) != 1 || page.TotalSize == nil || *page.TotalSize != 2 || page.NextPageToken == nil {
		t.Fatalf("history source count/page: %+v %v", page, err)
	}
	if _, err := store.Get(policy("one"), scope, "hidden"); !errors.Is(err, control.ErrNotFound) {
		t.Fatalf("history literal leak: %v", err)
	}
	if _, err := store.List(policy("two"), scope, ListRequest{PageSize: 1, IncludeTotal: true, PageToken: *page.NextPageToken}); !errors.Is(err, control.ErrInvalidArgument) {
		t.Fatalf("history stale policy cursor: %v", err)
	}
}
