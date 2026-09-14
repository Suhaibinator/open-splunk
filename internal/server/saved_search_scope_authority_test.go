package server

import (
	"context"
	"net/http"
	"testing"
	"time"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/savedobjects"
)

func TestSavedSearchSharingScopeDoesNotChangeCurrentRunAuthority(t *testing.T) {
	t.Parallel()
	for _, sharing := range []opensplunk.SharingScope{
		opensplunk.SharingScope_SHARING_SCOPE_PRIVATE,
		opensplunk.SharingScope_SHARING_SCOPE_APP,
		opensplunk.SharingScope_SHARING_SCOPE_GLOBAL,
	} {
		t.Run(sharing.String(), func(t *testing.T) {
			t.Parallel()
			for _, test := range []struct {
				name    string
				apps    *fakeBootstrapAppCatalog
				indexes *historyRerunIndexCatalog
				status  int
			}{
				{name: "current authority", apps: activeHistoryRerunAppCatalog(), indexes: activeHistoryRerunIndexCatalog("main"), status: http.StatusOK},
				{name: "app removed", apps: &fakeBootstrapAppCatalog{result: AppCatalogResult{Complete: true}}, indexes: activeHistoryRerunIndexCatalog("main"), status: http.StatusForbidden},
				{name: "index removed", apps: activeHistoryRerunAppCatalog(), indexes: &historyRerunIndexCatalog{}, status: http.StatusForbidden},
			} {
				t.Run(test.name, func(t *testing.T) {
					t.Parallel()
					record := savedSearchRecord("scope-saved", 4, "owner-1", "app-main", "Scope authority")
					record.Definition.SharingScope = sharing
					record.Definition.Search.Spl = "index=main"
					record.Definition.Search.IndexScope = []string{"main"}
					store := &fakeSavedSearches{getFn: func(_ context.Context, scope savedobjects.AccessScope, id string) (*opensplunk.SavedSearch, error) {
						if scope.OwnerID != "owner-1" || id != record.SavedSearchId {
							t.Fatalf("scope %v altered object authority: %+v %q", sharing, scope, id)
						}
						return record, nil
					}}
					job := completeJobForApp("scope-job", "app-main")
					job.KnowledgeSnapshot = enabledEmptyKnowledgeSnapshotSummary()
					jobs := &knowledgeAdmissionSearchJobs{fakeSearchJobs: &fakeSearchJobs{createJob: job}, enabled: true}
					handler := newTestHandler(t, Config{
						SearchJobs: jobs, SavedSearches: store, AppCatalog: test.apps,
						Indexes: test.indexes, OwnerID: "owner-1", TenantID: "tenant-1", WebUI: testUI(),
						Now: func() time.Time { return testNow },
					})
					request := createRequest("-1h", "now", "main")
					request.Definition.AppId = new("app-main")
					request.Source = &opensplunk.SearchJobSource{
						Origin: opensplunk.SearchJobOrigin_SEARCH_JOB_ORIGIN_SAVED_SEARCH, SavedSearchId: new(record.SavedSearchId),
					}
					response := postProto(t, handler, "/api/search/jobs/create", request)
					if response.Code != test.status {
						t.Fatalf("scope %v launch status = %d, want %d: %s", sharing, response.Code, test.status, response.Body.String())
					}
					jobs.mu.Lock()
					calls, admitted := jobs.createCalls, jobs.createRequest
					jobs.mu.Unlock()
					if test.status == http.StatusOK {
						if calls != 1 || admitted.OwnerID != "owner-1" || admitted.TenantID != "tenant-1" || admitted.SPL != "index=main" {
							t.Fatalf("scope %v changed accepted run authority/definition: %+v", sharing, admitted)
						}
					} else if calls != 0 {
						t.Fatalf("scope %v bypassed current authorization and admitted %d jobs", sharing, calls)
					}
				})
			}
		})
	}
}
