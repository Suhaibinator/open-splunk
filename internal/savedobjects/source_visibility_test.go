package savedobjects

import (
	"context"
	"errors"
	"testing"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/resourcepolicy"
)

func TestSourceVisibilityFiltersBeforePageAndExactCount(t *testing.T) {
	database, store := openTestStore(t)
	if err := database.GORMDB().Exec(`CREATE TABLE os_index_organizations(index_id text PRIMARY KEY,organization_id bigint NOT NULL)`).Error; err != nil {
		t.Fatal(err)
	}
	scope := AccessScope{OwnerID: "reader", OrganizationID: "1"}
	ids := map[string]string{}
	for _, test := range []struct{ name, spl string }{{"visible-one", "index=main"}, {"hidden-literal", "index=secret"}, {"visible-two", "index=main | stats count"}, {"hidden-unresolved", "index=main | not_a_command"}} {
		definition := &opensplunk.SavedSearchDefinition{Name: test.name, Search: &opensplunk.SearchDefinition{Spl: test.spl, IndexScope: []string{"main"}}, SharingScope: opensplunk.SharingScope_SHARING_SCOPE_PRIVATE}
		created, err := store.Create(t.Context(), scope, definition)
		if err != nil {
			t.Fatal(err)
		}
		ids[test.name] = created.GetSavedSearchId()
	}
	policy := func(revision string) context.Context {
		return resourcepolicy.WithProvider(t.Context(), func(context.Context) (resourcepolicy.Snapshot, error) {
			return resourcepolicy.Snapshot{Fingerprint: revision, IndexNames: []string{"main"}}, nil
		})
	}
	first, err := store.List(policy("revision-one"), scope, ListRequest{PageSize: 1, IncludeTotal: true})
	if err != nil || len(first.SavedSearches) != 1 || first.TotalSize == nil || *first.TotalSize != 2 || first.NextPageToken == nil {
		t.Fatalf("filtered first page: %+v %v", first, err)
	}
	second, err := store.List(policy("revision-one"), scope, ListRequest{PageSize: 1, IncludeTotal: true, PageToken: *first.NextPageToken})
	if err != nil || len(second.SavedSearches) != 1 || *second.TotalSize != 2 {
		t.Fatalf("filtered second page: %+v %v", second, err)
	}
	if _, err := store.List(policy("revision-two"), scope, ListRequest{PageSize: 1, IncludeTotal: true, PageToken: *first.NextPageToken}); !errors.Is(err, control.ErrInvalidArgument) {
		t.Fatalf("stale policy cursor: %v", err)
	}
	if _, err := store.Get(policy("revision-one"), scope, ids["hidden-literal"]); !errors.Is(err, control.ErrNotFound) {
		t.Fatalf("denied literal leaked via Get: %v", err)
	}
	site := resourcepolicy.WithProvider(t.Context(), func(context.Context) (resourcepolicy.Snapshot, error) {
		return resourcepolicy.Snapshot{SiteAdministrator: true}, nil
	})
	all, err := store.List(site, scope, ListRequest{IncludeTotal: true})
	if err != nil || *all.TotalSize != 4 {
		t.Fatalf("site administrator source bypass: %+v %v", all, err)
	}
}

func TestSourceVisibilityExpandsWildcardsAndCurrentAppDefaultsInSQL(t *testing.T) {
	database, store := openTestStore(t)
	if err := database.GORMDB().Exec(`CREATE TABLE os_index_organizations(index_id text PRIMARY KEY,organization_id bigint NOT NULL)`).Error; err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"main", "secret"} {
		index, err := database.CreateIndex(t.Context(), control.IndexDefinition{Name: name, SearchEnabled: true, IngestionEnabled: true})
		if err != nil {
			t.Fatal(err)
		}
		if err := database.GORMDB().Exec("INSERT INTO os_index_organizations(index_id,organization_id) VALUES (?,1)", index.ID).Error; err != nil {
			t.Fatal(err)
		}
	}
	catalog, err := control.NewAppCatalog(database, control.AppCatalogOptions{CursorKey: testCursorKey})
	if err != nil {
		t.Fatal(err)
	}
	app, err := catalog.CreateApp(t.Context(), control.AppAccessScope{TenantID: "physical", OrganizationID: 1}, control.AppDefinition{Slug: "source-policy-app", DisplayName: "Source Policy", DefaultIndexes: []string{"main", "secret"}})
	if err != nil {
		t.Fatal(err)
	}
	scope := AccessScope{OwnerID: "reader", OrganizationID: "1"}
	for _, test := range []struct {
		name, spl string
		scope     []string
		app       bool
	}{{"allowed-literal", "index=main", []string{"main"}, false}, {"denied-wildcard", "index=*", []string{"main"}, false}, {"denied-defaults", "index=main", nil, true}} {
		search := &opensplunk.SearchDefinition{Spl: test.spl, IndexScope: test.scope}
		if test.app {
			search.AppId = &app.ID
		}
		if _, err := store.Create(t.Context(), scope, &opensplunk.SavedSearchDefinition{Name: test.name, Search: search, SharingScope: opensplunk.SharingScope_SHARING_SCOPE_PRIVATE}); err != nil {
			t.Fatal(err)
		}
	}
	ctx := resourcepolicy.WithProvider(t.Context(), func(context.Context) (resourcepolicy.Snapshot, error) {
		return resourcepolicy.Snapshot{Fingerprint: "policy", IndexNames: []string{"main"}, AppIDs: []string{app.ID}}, nil
	})
	page, err := store.List(ctx, scope, ListRequest{IncludeTotal: true})
	if err != nil || len(page.SavedSearches) != 1 || page.TotalSize == nil || *page.TotalSize != 1 || page.SavedSearches[0].GetDefinition().GetName() != "allowed-literal" {
		t.Fatalf("wildcard/default source count leaked: %+v %v", page, err)
	}
}
