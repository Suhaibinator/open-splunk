package server

import (
	"context"
	"net/http"
	"testing"
	"time"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/savedobjects"
	"github.com/Suhaibinator/open-splunk/internal/searchhistory"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
)

const v03PersistedAllTenSPL = `index=main` +
	` | regex message!="reject"` +
	` | sort 0 +event_id` +
	` | accum value AS running` +
	` | strcat host "/" route endpoint` +
	` | addinfo` +
	` | fillnull value="0" optional` +
	` | addtotals fieldname=total value running` +
	` | delta running AS step p=2` +
	` | makemv delim="," allowempty=true tags` +
	` | mvexpand tags limit=2` +
	` | reverse` +
	` | table event_id tags info_sid`

func TestV03SavedSearchExecutionUsesPersistedAllTenDefinition(t *testing.T) {
	t.Parallel()

	const (
		ownerID  = "owner-v03-persisted"
		tenantID = "tenant-v03-persisted"
		appID    = "app-v03-persisted"
		savedID  = "saved-v03-persisted"
	)
	record := savedSearchRecord(savedID, 1, ownerID, appID, "v0.3 all ten")
	record.Definition.Search.Spl = v03PersistedAllTenSPL
	record.Definition.Search.IndexScope = []string{"main"}
	store := &fakeSavedSearches{getFn: func(
		_ context.Context,
		scope savedobjects.AccessScope,
		id string,
	) (*opensplunkv1.SavedSearch, error) {
		if scope.OwnerID != ownerID || id != savedID {
			t.Fatalf("saved-search lookup = scope %+v ID %q", scope, id)
		}
		return record, nil
	}}
	jobs := &v03CompilingSearchJobs{fakeSearchJobs: &fakeSearchJobs{
		createJob: completeJobForApp("job-v03-saved", appID),
	}}
	handler := newTestHandler(t, Config{
		SearchJobs: jobs,
		Indexes: fakeIndexCatalog{indexes: []control.Index{{
			ID: "idx-main", Definition: control.IndexDefinition{Name: "main", SearchEnabled: true}, State: control.IndexStateActive,
		}}},
		SavedSearches: store,
		WebUI:         testUI(),
		OwnerID:       ownerID,
		TenantID:      tenantID,
		Now:           func() time.Time { return testNow },
	})

	// A saved-search launch is a persisted-object operation. Caller-supplied
	// SPL may be stale or hostile and must not substitute for the trusted
	// definition associated with savedID.
	request := createRequest("-24h", "now", "main")
	request.Definition.Spl = `index=main | head 1`
	request.Definition.AppId = new(appID)
	request.Source = &opensplunkv1.SearchJobSource{
		Origin:        opensplunkv1.SearchJobOrigin_SEARCH_JOB_ORIGIN_SAVED_SEARCH,
		SavedSearchId: new(savedID),
	}
	response := postProto(t, handler, "/api/v1/search/jobs/create", request)
	if response.Code != http.StatusOK {
		t.Fatalf("saved-search create status = %d, body = %s", response.Code, response.Body.String())
	}
	jobs.mu.Lock()
	captured := jobs.createRequest
	createCalls := jobs.createCalls
	jobs.mu.Unlock()
	if createCalls != 1 || captured.Source != (searchjobs.JobSource{
		Origin: searchjobs.JobOriginSavedSearch, ObjectID: savedID,
	}) {
		t.Fatalf("saved-search launch = calls %d request %+v", createCalls, captured)
	}
	if captured.SPL != v03PersistedAllTenSPL {
		t.Fatalf("saved-search launch executed caller SPL %q, want persisted all-ten definition %q", captured.SPL, v03PersistedAllTenSPL)
	}
	if len(jobs.queries) != 1 {
		t.Fatalf("saved-search compile count = %d, want 1", len(jobs.queries))
	}
	requireV03APICompiledAllTen(t, jobs.queries[0])
}

func TestV03HistoryRerunReconstructsPersistedAllTenDefinition(t *testing.T) {
	t.Parallel()

	const (
		historyID = "history-v03-persisted"
		ownerID   = "owner-1"
		tenantID  = "tenant-1"
		appID     = "app-main"
	)
	entry := historyRerunEntry(
		historyID,
		opensplunkv1.SearchJobState_SEARCH_JOB_STATE_COMPLETED,
	)
	entry.Definition.Spl = v03PersistedAllTenSPL
	history := &fakeSearchHistory{getFn: func(
		_ context.Context,
		scope searchhistory.AccessScope,
		id string,
	) (*opensplunkv1.SearchHistoryEntry, error) {
		assertHistoryScope(t, scope, tenantID, ownerID)
		if id != historyID {
			t.Fatalf("history lookup ID = %q, want %q", id, historyID)
		}
		return entry, nil
	}}
	jobs := &v03CompilingSearchJobs{fakeSearchJobs: &fakeSearchJobs{
		createJob: completeJobForApp("job-v03-history", appID),
	}}
	handler := newTestHandler(t, Config{
		SearchJobs:    jobs,
		Indexes:       activeHistoryRerunIndexCatalog("main"),
		AppCatalog:    activeHistoryRerunAppCatalog(),
		SearchHistory: history,
		WebUI:         testUI(),
		OwnerID:       ownerID,
		TenantID:      tenantID,
		Now:           func() time.Time { return testNow },
	})
	response := postProto(
		t,
		handler,
		"/api/v1/search/jobs/create",
		historyRerunRequest(historyID),
	)
	if response.Code != http.StatusOK {
		t.Fatalf("history rerun status = %d, body = %s", response.Code, response.Body.String())
	}
	jobs.mu.Lock()
	captured := jobs.createRequest
	createCalls := jobs.createCalls
	jobs.mu.Unlock()
	if createCalls != 1 || captured.SPL != v03PersistedAllTenSPL ||
		captured.Source != (searchjobs.JobSource{
			Origin: searchjobs.JobOriginHistoryRerun, ObjectID: historyID,
		}) {
		t.Fatalf("history rerun did not reconstruct persisted all-ten definition: %+v", captured)
	}
	if len(jobs.queries) != 1 {
		t.Fatalf("history-rerun compile count = %d, want 1", len(jobs.queries))
	}
	requireV03APICompiledAllTen(t, jobs.queries[0])
}
