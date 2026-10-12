package dashboards

import (
	"context"
	"errors"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/resourcepolicy"
)

func TestDashboardPanelLiteralVisibilityPrecedesList(t *testing.T) {
	store := openTestStore(t)
	if err := store.orm.Exec(`CREATE TABLE os_index_organizations(index_id text PRIMARY KEY,organization_id bigint NOT NULL)`).Error; err != nil {
		t.Fatal(err)
	}
	scope := AccessScope{OwnerID: "reader", OrganizationID: "1"}
	definition := testDefinition("denied panel")
	definition.Panels[0].Search.Spl = "index=secret"
	created, err := store.Create(t.Context(), scope, definition)
	if err != nil {
		t.Fatal(err)
	}
	ctx := resourcepolicy.WithProvider(t.Context(), func(context.Context) (resourcepolicy.Snapshot, error) {
		return resourcepolicy.Snapshot{IndexNames: []string{"main"}, AppIDs: []string{"search"}}, nil
	})
	listed, err := store.List(ctx, scope, nil)
	if err != nil || len(listed) != 0 {
		t.Fatalf("denied panel leaked through list: %+v %v", listed, err)
	}
	if _, err := store.Get(ctx, scope, created.GetDashboardId()); !errors.Is(err, control.ErrNotFound) {
		t.Fatalf("denied panel leaked through get: %v", err)
	}
}
