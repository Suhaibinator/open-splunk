package server

import (
	"context"
	"net/http"
	"testing"
	"time"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/savedobjects"
	"github.com/Suhaibinator/open-splunk/internal/searchhistory"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
)

const pipelinePersistedCommandSPL = `index=main` +
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

func TestPipelineSavedSearchExecutionUsesPersistedCommandDefinition(t *testing.T) {
	t.Parallel()

	const (
		ownerID  = "owner-pipeline-persisted"
		tenantID = "tenant-pipeline-persisted"
		appID    = "app-pipeline-persisted"
		savedID  = "saved-pipeline-persisted"
	)
	record := savedSearchRecord(savedID, 1, ownerID, appID, "pipeline pipeline commands")
	record.Definition.Search.Spl = pipelinePersistedCommandSPL
	record.Definition.Search.IndexScope = []string{"main"}
	store := &fakeSavedSearches{getFn: func(
		_ context.Context,
		scope savedobjects.AccessScope,
		id string,
	) (*opensplunk.SavedSearch, error) {
		if scope.OwnerID != ownerID || id != savedID {
			t.Fatalf("saved-search lookup = scope %+v ID %q", scope, id)
		}
		return record, nil
	}}
	jobs := &pipelineCompilingSearchJobs{fakeSearchJobs: &fakeSearchJobs{
		createJob: completeJobForApp("job-pipeline-saved", appID),
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
	request.Source = &opensplunk.SearchJobSource{
		Origin:        opensplunk.SearchJobOrigin_SEARCH_JOB_ORIGIN_SAVED_SEARCH,
		SavedSearchId: new(savedID),
	}
	response := postProto(t, handler, "/api/search/jobs/create", request)
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
	if captured.SPL != pipelinePersistedCommandSPL {
		t.Fatalf("saved-search launch executed caller SPL %q, want persisted pipeline-command definition %q", captured.SPL, pipelinePersistedCommandSPL)
	}
	if len(jobs.queries) != 1 {
		t.Fatalf("saved-search compile count = %d, want 1", len(jobs.queries))
	}
	requirePipelineAPICompiledCommands(t, jobs.queries[0])
}

func TestPipelineHistoryRerunReconstructsPersistedCommandDefinition(t *testing.T) {
	t.Parallel()

	const (
		historyID = "history-pipeline-persisted"
		ownerID   = "owner-1"
		tenantID  = "tenant-1"
		appID     = "app-main"
	)
	entry := historyRerunEntry(
		historyID,
		opensplunk.SearchJobState_SEARCH_JOB_STATE_COMPLETED,
	)
	entry.Definition.Spl = pipelinePersistedCommandSPL
	history := &fakeSearchHistory{getFn: func(
		_ context.Context,
		scope searchhistory.AccessScope,
		id string,
	) (*opensplunk.SearchHistoryEntry, error) {
		assertHistoryScope(t, scope, tenantID, ownerID)
		if id != historyID {
			t.Fatalf("history lookup ID = %q, want %q", id, historyID)
		}
		return entry, nil
	}}
	jobs := &pipelineCompilingSearchJobs{fakeSearchJobs: &fakeSearchJobs{
		createJob: completeJobForApp("job-pipeline-history", appID),
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
		"/api/search/jobs/create",
		historyRerunRequest(historyID),
	)
	if response.Code != http.StatusOK {
		t.Fatalf("history rerun status = %d, body = %s", response.Code, response.Body.String())
	}
	jobs.mu.Lock()
	captured := jobs.createRequest
	createCalls := jobs.createCalls
	jobs.mu.Unlock()
	if createCalls != 1 || captured.SPL != pipelinePersistedCommandSPL ||
		captured.Source != (searchjobs.JobSource{
			Origin: searchjobs.JobOriginHistoryRerun, ObjectID: historyID,
		}) {
		t.Fatalf("history rerun did not reconstruct persisted pipeline-command definition: %+v", captured)
	}
	if len(jobs.queries) != 1 {
		t.Fatalf("history-rerun compile count = %d, want 1", len(jobs.queries))
	}
	requirePipelineAPICompiledCommands(t, jobs.queries[0])
}
