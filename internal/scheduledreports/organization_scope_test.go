package scheduledreports

import (
	"context"
	"errors"
	"testing"
	"time"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/authorization"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/pgtest"
	"github.com/Suhaibinator/open-splunk/internal/resourcepolicy"
	"github.com/Suhaibinator/open-splunk/internal/savedobjects"
)

func TestNativeScheduleOrganizationSharingAndFreshSourceScope(t *testing.T) {
	database, err := control.Open(t.Context(), pgtest.DSN(t))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	// This repository test injects a policy snapshot; the authoritative native
	// ownership schema still matches runtime SQL, including organization FKs.
	if _, err := database.SQLDB().ExecContext(t.Context(), `CREATE TABLE gocommon_organizations(id BIGINT PRIMARY KEY,deleted_at TIMESTAMPTZ);CREATE TABLE gocommon_users(id BIGINT PRIMARY KEY,deleted_at TIMESTAMPTZ);CREATE TABLE gocommon_user_organizations(user_id BIGINT,organization_id BIGINT,role INTEGER,PRIMARY KEY(user_id,organization_id));INSERT INTO gocommon_organizations(id)VALUES(1),(2);INSERT INTO gocommon_users(id)VALUES(1),(2),(99)`); err != nil {
		t.Fatal(err)
	}
	if _, err := database.SQLDB().ExecContext(t.Context(), authorization.Schema); err != nil {
		t.Fatal(err)
	}

	saved, err := savedobjects.New(database, savedobjects.Options{CursorKey: make([]byte, 32)})
	if err != nil {
		t.Fatal(err)
	}
	source, err := saved.Create(t.Context(), savedobjects.AccessScope{OwnerID: "1", OrganizationID: "1"}, &opensplunk.SavedSearchDefinition{Name: "shared source", SharingScope: opensplunk.SharingScope_SHARING_SCOPE_GLOBAL, Search: &opensplunk.SearchDefinition{Spl: "index=main"}})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Truncate(time.Microsecond)
	var revoke bool
	repository, err := NewRepository(database.GORMDB(), RepositoryOptions{Clock: func() time.Time { return now }, CursorKey: make([]byte, 32), ResolveSourceScope: func(_ context.Context, scope AccessScope) (AccessScope, error) {
		if revoke {
			return AccessScope{}, errors.New("current user membership revoked")
		}
		return scope, nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	configuration := Configuration{Cron: "*/5 * * * *", Timezone: "UTC", DispatchTTL: "2p", Enabled: false}
	scope := AccessScope{OwnerID: "2", OrganizationID: "1"}
	schedule, err := repository.Configure(t.Context(), scope, "tenant", source.SavedSearchId, 0, configuration, nil)
	if err != nil {
		t.Fatal(err)
	}
	if schedule.OrganizationID != "1" || schedule.OwnerID != "2" {
		t.Fatalf("schedule run-as scope: %#v", schedule)
	}
	foreign := AccessScope{OwnerID: "2", OrganizationID: "2"}
	if _, err := repository.Get(t.Context(), foreign, source.SavedSearchId); !errors.Is(err, ErrNotFound) {
		t.Fatalf("same owner foreign organization read: %v", err)
	}
	if _, err := repository.Configure(t.Context(), foreign, "tenant", source.SavedSearchId, 1, configuration, nil); !errors.Is(err, ErrNotFound) {
		t.Fatalf("foreign organization source configure: %v", err)
	}
	run, claimed, err := repository.ClaimRunNow(t.Context(), schedule, now, 5*time.Minute, 10*time.Minute)
	if err != nil || !claimed {
		t.Fatalf("shared source claim: %v %v", claimed, err)
	}
	if run.OrganizationID != "1" || run.OwnerID != "2" || run.Definition.GetOwnerId() != "1" {
		t.Fatalf("immutable shared source and run-as identity: %#v", run)
	}
	page, err := repository.ListRunPage(t.Context(), foreign, source.SavedSearchId, RunPageRequest{IncludeTotal: true})
	if err != nil || len(page.Runs) != 0 || page.TotalSize == nil || *page.TotalSize != 0 {
		t.Fatalf("foreign organization run metadata/count: %#v %v", page, err)
	}
	if _, err := repository.Get(t.Context(), AccessScope{OwnerID: "99", SiteAdministrator: true}, source.SavedSearchId); err != nil {
		t.Fatalf("site administrator cross-owner read: %v", err)
	}
	if projections, err := repository.ListCurrentProjections(t.Context(), AccessScope{OwnerID: "99", SiteAdministrator: true}, []string{source.SavedSearchId}); err != nil || len(projections) != 1 || projections[source.SavedSearchId].LatestRun == nil {
		t.Fatalf("site administrator cross-organization projection: %#v %v", projections, err)
	}
	policyContext := func(fingerprint string, indexes []string) context.Context {
		return resourcepolicy.WithProvider(t.Context(), func(context.Context) (resourcepolicy.Snapshot, error) {
			return resourcepolicy.Snapshot{Fingerprint: fingerprint, IndexNames: indexes, AvailableIndexNames: []string{"main"}}, nil
		})
	}
	denied := policyContext("revoked-policy", nil)
	if _, err := repository.Get(denied, scope, source.SavedSearchId); !errors.Is(err, ErrNotFound) {
		t.Fatalf("denied source disclosed schedule: %v", err)
	}
	if page, err := repository.ListRunPage(denied, scope, source.SavedSearchId, RunPageRequest{IncludeTotal: true}); !errors.Is(err, ErrNotFound) || len(page.Runs) != 0 || page.TotalSize != nil {
		t.Fatalf("denied source disclosed run metadata/count: %#v %v", page, err)
	}
	if projections, err := repository.ListCurrentProjections(denied, scope, []string{source.SavedSearchId}); err != nil || len(projections) != 0 {
		t.Fatalf("denied source projection leaked: %#v %v", projections, err)
	}
	if err := repository.Finish(t.Context(), run.RunID, RunOutcomeSucceeded, "", now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	if _, claimed, err := repository.ClaimOneOff(t.Context(), scope, "tenant", source.SavedSearchId, now.Add(2*time.Minute), 5*time.Minute, 10*time.Minute); err != nil || !claimed {
		t.Fatalf("second authorized occurrence: %v %v", claimed, err)
	}
	allowed := policyContext("policy-one", []string{"main"})
	firstPage, err := repository.ListRunPage(allowed, scope, source.SavedSearchId, RunPageRequest{Limit: 1, IncludeTotal: true})
	if err != nil || firstPage.NextPageToken == "" || firstPage.TotalSize == nil || *firstPage.TotalSize != 2 {
		t.Fatalf("authorized source page/count: %#v %v", firstPage, err)
	}
	changed := policyContext("policy-two", []string{"main"})
	if _, err := repository.ListRunPage(changed, scope, source.SavedSearchId, RunPageRequest{Limit: 1, PageToken: firstPage.NextPageToken}); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("policy change did not invalidate cursor: %v", err)
	}
	revoke = true
	if _, _, err := repository.ClaimRunNow(t.Context(), schedule, now, 5*time.Minute, 10*time.Minute); err == nil {
		t.Fatal("claim accepted revoked current source membership")
	}
}
