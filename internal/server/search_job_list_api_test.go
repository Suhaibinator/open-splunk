package server

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/searchartifacts"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
	"google.golang.org/protobuf/proto"
)

func TestSearchJobListRouteRoundTripScopeFiltersAndSafeProjection(t *testing.T) {
	total := uint64(8)
	failed := listSearchJob("job-b", testNow)
	failed.State = searchjobs.StateFailed
	failed.SPL = "index=main ERROR failed"
	failed.NormalizedSPL = "index=main error failed"
	failed.AppID = "app-main"
	failed.ScannedRows = 500
	failed.ScannedBytes = 50_000
	failed.Failure = &searchjobs.Failure{
		Code:      searchjobs.FailureExecution,
		Message:   "search execution failed",
		Retryable: true,
	}
	completed := listSearchJob("job-a", testNow)
	completed.SPL = "index=main error completed"
	completed.AppID = "app-main"
	jobs := &fakeSearchJobs{listPage: searchjobs.JobListPage{
		Jobs:           []searchjobs.JobListItem{listItem(failed), listItem(completed)},
		NextPageToken:  "next-page",
		TotalSize:      &total,
		TotalSizeExact: true,
	}}
	handler := newSearchJobListTestHandler(t, jobs, Config{})
	pageSize := uint32(2)
	pageToken, appID, text := "page-1", " app-main ", " ERROR "
	response := postProto(t, handler, searchJobsListPath, &opensplunk.ListSearchJobsRequest{
		Page: &opensplunk.PageRequest{
			PageSize:         &pageSize,
			PageToken:        &pageToken,
			IncludeTotalSize: true,
		},
		StateFilters: []opensplunk.SearchJobState{
			opensplunk.SearchJobState_SEARCH_JOB_STATE_FAILED,
			opensplunk.SearchJobState_SEARCH_JOB_STATE_COMPLETED,
			opensplunk.SearchJobState_SEARCH_JOB_STATE_FAILED,
		},
		AppIdFilter: &appID,
		TextFilter:  &text,
	})
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
	if got := response.Header().Get("Content-Type"); got != "application/x-protobuf" {
		t.Fatalf("Content-Type = %q", got)
	}

	jobs.mu.Lock()
	capturedScope, captured := jobs.listScope, jobs.listRequest
	calls := jobs.listCalls
	jobs.mu.Unlock()
	if calls != 1 {
		t.Fatalf("list calls = %d, want 1", calls)
	}
	if capturedScope != (searchjobs.AccessScope{OwnerID: "owner-1", TenantID: "tenant-1"}) {
		t.Fatalf("scope = %+v", capturedScope)
	}
	if captured.PageSize != 2 || captured.PageToken != "page-1" || !captured.IncludeTotal {
		t.Fatalf("page request = %+v", captured)
	}
	if !slices.Equal(captured.StateFilters, []searchjobs.State{searchjobs.StateCompleted, searchjobs.StateFailed}) {
		t.Fatalf("state filters = %v", captured.StateFilters)
	}
	if captured.AppIDFilter == nil || *captured.AppIDFilter != "app-main" {
		t.Fatalf("app filter = %#v", captured.AppIDFilter)
	}
	if captured.TextFilter == nil || *captured.TextFilter != "ERROR" {
		t.Fatalf("text filter = %#v", captured.TextFilter)
	}

	var decoded opensplunk.ListSearchJobsResponse
	unmarshalResponse(t, response, &decoded)
	if decoded.GetPage() == nil || decoded.GetPage().GetNextPageToken() != "next-page" ||
		decoded.GetPage().GetTotalSize() != total || !decoded.GetPage().GetTotalSizeExact() {
		t.Fatalf("page = %+v", decoded.GetPage())
	}
	if len(decoded.GetSearchJobs()) != 2 {
		t.Fatalf("jobs = %d, want 2", len(decoded.GetSearchJobs()))
	}
	first := decoded.GetSearchJobs()[0]
	if first.GetSearchJobId() != "job-b" || first.GetDefinition().GetSpl() != failed.SPL ||
		first.GetDefinition().GetAppId() != "app-main" || first.GetNormalizedSpl() != failed.NormalizedSPL ||
		!slices.Equal(first.GetDefinition().GetIndexScope(), failed.RequestedIndexes) ||
		!slices.Equal(first.GetEffectiveIndexScope(), failed.EffectiveIndexes) ||
		first.GetState() != opensplunk.SearchJobState_SEARCH_JOB_STATE_FAILED ||
		first.GetFailure() == nil || first.GetFailure().GetMessage() != failed.Failure.Message {
		t.Fatalf("safe projection = %+v", first)
	}
	if first.GetPlan() != nil || first.GetResultSchema() != nil || len(first.GetDiagnostics()) != 0 {
		t.Fatalf("list projection exposed plan, schema, or diagnostics: %+v", first)
	}
	if progress := first.GetProgress(); progress.GetScannedRows() != failed.ScannedRows ||
		progress.GetScannedBytes() != failed.ScannedBytes || progress.GetCountersAreEstimates() {
		t.Fatalf("list progress = %+v", progress)
	}
}

func TestSearchJobListUsesBoundedCanonicalOptions(t *testing.T) {
	allStates := []opensplunk.SearchJobState{
		opensplunk.SearchJobState_SEARCH_JOB_STATE_EXPIRED,
		opensplunk.SearchJobState_SEARCH_JOB_STATE_CANCELED,
		opensplunk.SearchJobState_SEARCH_JOB_STATE_FAILED,
		opensplunk.SearchJobState_SEARCH_JOB_STATE_COMPLETED,
		opensplunk.SearchJobState_SEARCH_JOB_STATE_RUNNING,
		opensplunk.SearchJobState_SEARCH_JOB_STATE_PLANNING,
		opensplunk.SearchJobState_SEARCH_JOB_STATE_PARSING,
		opensplunk.SearchJobState_SEARCH_JOB_STATE_QUEUED,
		opensplunk.SearchJobState_SEARCH_JOB_STATE_RUNNING,
	}
	emptyApp, whitespaceText := " \t ", " \n "
	jobs := &fakeSearchJobs{}
	handler := newSearchJobListTestHandler(t, jobs, Config{MaximumPageSize: 7})
	response := postProto(t, handler, searchJobsListPath, &opensplunk.ListSearchJobsRequest{
		StateFilters: allStates,
		AppIdFilter:  &emptyApp,
		TextFilter:   &whitespaceText,
	})
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
	jobs.mu.Lock()
	captured := jobs.listRequest
	jobs.mu.Unlock()
	if captured.PageSize != 7 {
		t.Fatalf("default page size = %d, want configured maximum 7", captured.PageSize)
	}
	if !slices.Equal(captured.StateFilters, []searchjobs.State{
		searchjobs.StateQueued,
		searchjobs.StateParsing,
		searchjobs.StatePlanning,
		searchjobs.StateRunning,
		searchjobs.StateCompleted,
		searchjobs.StateFailed,
		searchjobs.StateCanceled,
		searchjobs.StateExpired,
	}) {
		t.Fatalf("canonical state filters = %v", captured.StateFilters)
	}
	if captured.AppIDFilter == nil || *captured.AppIDFilter != "" {
		t.Fatalf("empty app filter = %#v, want explicit no-app filter", captured.AppIDFilter)
	}
	if captured.TextFilter != nil {
		t.Fatalf("whitespace text filter = %#v, want absent", captured.TextFilter)
	}

	jobs = &fakeSearchJobs{}
	handler = newSearchJobListTestHandler(t, jobs, Config{MaximumPageSize: 100})
	requested := uint32(91)
	response = postProto(t, handler, searchJobsListPath, &opensplunk.ListSearchJobsRequest{
		Page: &opensplunk.PageRequest{PageSize: &requested},
	})
	if response.Code != http.StatusOK {
		t.Fatalf("clamped status = %d, body = %s", response.Code, response.Body.String())
	}
	jobs.mu.Lock()
	captured = jobs.listRequest
	jobs.mu.Unlock()
	if captured.PageSize != maximumSearchJobListRows {
		t.Fatalf("clamped page size = %d, want %d", captured.PageSize, maximumSearchJobListRows)
	}
}

func TestSearchJobListOverlaysDurableSettingsWithoutRefreshingRetention(t *testing.T) {
	job := listSearchJob("shared-job", testNow)
	job.Version = 3
	job.ExpiresAt = testNow.Add(10 * time.Minute)
	artifacts := &batchListSearchArtifacts{records: map[string]searchartifacts.Record{
		job.ID: {
			Job: job, State: searchartifacts.StateCompleted,
			Visibility:     searchartifacts.VisibilityEveryone,
			RetentionClass: searchartifacts.RetentionShared,
			Lifetime:       7 * 24 * time.Hour, ExpiresAt: testNow.Add(7 * 24 * time.Hour),
			ArtifactPresent: true,
		},
	}}
	handler := newSearchJobListTestHandler(t, &fakeSearchJobs{listPage: listPage(job)}, Config{
		SearchArtifacts: artifacts,
	})
	response := postProto(t, handler, searchJobsListPath, &opensplunk.ListSearchJobsRequest{})
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
	var decoded opensplunk.ListSearchJobsResponse
	unmarshalResponse(t, response, &decoded)
	projected := decoded.GetSearchJobs()[0]
	if projected.GetVisibility() != opensplunk.SearchJobVisibility_SEARCH_JOB_VISIBILITY_EVERYONE ||
		projected.GetRetentionClass() != opensplunk.SearchJobRetentionClass_SEARCH_JOB_RETENTION_CLASS_SHARED ||
		projected.GetRetainedResultStatus() != opensplunk.RetainedResultStatus_RETAINED_RESULT_STATUS_AVAILABLE ||
		!projected.GetExpiresAt().AsTime().Equal(testNow.Add(7*24*time.Hour)) {
		t.Fatalf("durable list projection = %+v", projected)
	}
	if artifacts.listCalls != 1 || artifacts.inspectCalls != 0 {
		t.Fatalf("durable list calls = %d, inspection calls = %d", artifacts.listCalls, artifacts.inspectCalls)
	}
}

func TestSearchJobListRestoresDurableTerminalJobsAfterRestart(t *testing.T) {
	ctx := context.Background()
	now := testNow
	database, err := control.Open(ctx, filepath.Join(t.TempDir(), "control.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	directory := filepath.Join(t.TempDir(), "artifacts")
	openStore := func() *searchartifacts.Store {
		store, err := searchartifacts.New(ctx, searchartifacts.Config{
			DB: database.SQLDB(), Directory: directory, Clock: func() time.Time { return now },
			CleanupInterval: -1, TombstoneRetention: time.Hour,
		})
		if err != nil {
			t.Fatal(err)
		}
		return store
	}
	store := openStore()

	interrupted := durableListQueuedJob("durable-interrupted", testNow, "owner-1")
	if err := store.Admit(ctx, interrupted); err != nil {
		t.Fatal(err)
	}
	completed := durableListQueuedJob("durable-completed", testNow.Add(-time.Minute), "owner-1")
	persistDurableListCompletion(t, store, completed, testNow, time.Hour)
	expired := durableListQueuedJob("durable-expired", testNow.Add(-2*time.Minute), "owner-1")
	persistDurableListCompletion(t, store, expired, testNow, time.Minute)
	shared := durableListQueuedJob("durable-shared", testNow.Add(-3*time.Minute), "owner-2")
	persistDurableListCompletion(t, store, shared, testNow, time.Hour)
	if _, err := store.Share(ctx, searchjobs.AccessScope{TenantID: "tenant-1", OwnerID: "owner-2"}, shared.ID); err != nil {
		t.Fatal(err)
	}
	private := durableListQueuedJob("durable-private", testNow.Add(-4*time.Minute), "owner-2")
	persistDurableListCompletion(t, store, private, testNow, time.Hour)

	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	now = now.Add(2 * time.Minute)
	store = openStore()
	t.Cleanup(func() { _ = store.Close() })

	jobs := &fakeSearchJobs{getErr: searchjobs.ErrNotFound}
	handler := newSearchJobListTestHandler(t, jobs, Config{SearchArtifacts: store})
	pageSize := uint32(2)
	response := postProto(t, handler, searchJobsListPath, &opensplunk.ListSearchJobsRequest{
		Page: &opensplunk.PageRequest{PageSize: &pageSize, IncludeTotalSize: true},
	})
	if response.Code != http.StatusOK {
		t.Fatalf("first page status = %d, body = %s", response.Code, response.Body.String())
	}
	var first opensplunk.ListSearchJobsResponse
	unmarshalResponse(t, response, &first)
	if got := protoSearchJobIDs(first.GetSearchJobs()); !slices.Equal(got, []string{interrupted.ID, completed.ID}) {
		t.Fatalf("first durable IDs = %v", got)
	}
	if first.GetSearchJobs()[0].GetState() != opensplunk.SearchJobState_SEARCH_JOB_STATE_INTERRUPTED ||
		first.GetSearchJobs()[1].GetState() != opensplunk.SearchJobState_SEARCH_JOB_STATE_COMPLETED {
		t.Fatalf("first durable states = %+v", first.GetSearchJobs())
	}
	if first.GetPage().GetTotalSize() != 4 || !first.GetPage().GetTotalSizeExact() ||
		first.GetPage().GetNextPageToken() == "" {
		t.Fatalf("first durable page = %+v", first.GetPage())
	}

	token := first.GetPage().GetNextPageToken()
	response = postProto(t, handler, searchJobsListPath, &opensplunk.ListSearchJobsRequest{
		Page: &opensplunk.PageRequest{PageSize: &pageSize, PageToken: &token, IncludeTotalSize: true},
	})
	if response.Code != http.StatusOK {
		t.Fatalf("second page status = %d, body = %s", response.Code, response.Body.String())
	}
	var second opensplunk.ListSearchJobsResponse
	unmarshalResponse(t, response, &second)
	if got := protoSearchJobIDs(second.GetSearchJobs()); !slices.Equal(got, []string{expired.ID, shared.ID}) {
		t.Fatalf("second durable IDs = %v", got)
	}
	if second.GetSearchJobs()[0].GetState() != opensplunk.SearchJobState_SEARCH_JOB_STATE_EXPIRED ||
		second.GetSearchJobs()[1].GetVisibility() != opensplunk.SearchJobVisibility_SEARCH_JOB_VISIBILITY_EVERYONE ||
		second.GetPage().GetTotalSize() != 4 || !second.GetPage().GetTotalSizeExact() {
		t.Fatalf("second durable response = %+v", &second)
	}
	if jobs.listCalls != 0 {
		t.Fatalf("live manager list calls = %d, want durable authority", jobs.listCalls)
	}

	response = postProto(t, handler, searchJobsListPath, &opensplunk.ListSearchJobsRequest{
		StateFilters: []opensplunk.SearchJobState{opensplunk.SearchJobState_SEARCH_JOB_STATE_INTERRUPTED},
	})
	if response.Code != http.StatusOK {
		t.Fatalf("interrupted filter status = %d, body = %s", response.Code, response.Body.String())
	}
	var filtered opensplunk.ListSearchJobsResponse
	unmarshalResponse(t, response, &filtered)
	if got := protoSearchJobIDs(filtered.GetSearchJobs()); !slices.Equal(got, []string{interrupted.ID}) {
		t.Fatalf("interrupted durable IDs = %v", got)
	}
}

func TestSearchJobListAppliesStateFilterAfterLiveOverlay(t *testing.T) {
	ctx := context.Background()
	now := testNow
	database, err := control.Open(ctx, filepath.Join(t.TempDir(), "control.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	store, err := searchartifacts.New(ctx, searchartifacts.Config{
		DB: database.SQLDB(), Directory: filepath.Join(t.TempDir(), "artifacts"),
		Clock: func() time.Time { return now }, CleanupInterval: -1,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	queued := durableListQueuedJob("live-running", now, "owner-1")
	if err := store.Admit(ctx, queued); err != nil {
		t.Fatal(err)
	}
	running := queued
	running.Version = 2
	running.State = searchjobs.StateRunning
	running.StartedAt = now
	jobs := &fakeSearchJobs{getJob: running}
	handler := newSearchJobListTestHandler(t, jobs, Config{SearchArtifacts: store})

	response := postProto(t, handler, searchJobsListPath, &opensplunk.ListSearchJobsRequest{
		Page:         &opensplunk.PageRequest{IncludeTotalSize: true},
		StateFilters: []opensplunk.SearchJobState{opensplunk.SearchJobState_SEARCH_JOB_STATE_RUNNING},
	})
	if response.Code != http.StatusOK {
		t.Fatalf("running filter status = %d, body = %s", response.Code, response.Body.String())
	}
	var runningPage opensplunk.ListSearchJobsResponse
	unmarshalResponse(t, response, &runningPage)
	if got := protoSearchJobIDs(runningPage.GetSearchJobs()); !slices.Equal(got, []string{queued.ID}) ||
		runningPage.GetPage().GetTotalSize() != 1 || !runningPage.GetPage().GetTotalSizeExact() ||
		runningPage.GetSearchJobs()[0].GetState() != opensplunk.SearchJobState_SEARCH_JOB_STATE_RUNNING {
		t.Fatalf("running filter response = %+v", &runningPage)
	}

	response = postProto(t, handler, searchJobsListPath, &opensplunk.ListSearchJobsRequest{
		Page:         &opensplunk.PageRequest{IncludeTotalSize: true},
		StateFilters: []opensplunk.SearchJobState{opensplunk.SearchJobState_SEARCH_JOB_STATE_QUEUED},
	})
	if response.Code != http.StatusOK {
		t.Fatalf("queued filter status = %d, body = %s", response.Code, response.Body.String())
	}
	var queuedPage opensplunk.ListSearchJobsResponse
	unmarshalResponse(t, response, &queuedPage)
	if len(queuedPage.GetSearchJobs()) != 0 || queuedPage.GetPage().GetTotalSize() != 0 ||
		!queuedPage.GetPage().GetTotalSizeExact() {
		t.Fatalf("queued filter response = %+v", &queuedPage)
	}
}

func TestSearchJobListRejectsInvalidOptionsBeforeService(t *testing.T) {
	zero := uint32(0)
	tooManyStates := make([]opensplunk.SearchJobState, maximumSearchJobListStateFilters+1)
	for index := range tooManyStates {
		tooManyStates[index] = opensplunk.SearchJobState_SEARCH_JOB_STATE_QUEUED
	}
	oversizedApp := strings.Repeat("a", maximumSavedSearchAppIDBytes+1)
	controlApp := "bad\napp"
	oversizedText := strings.Repeat("x", maximumSearchJobListFilterTextBytes+1)
	oversizedToken := strings.Repeat("t", maximumSearchJobListPageTokenBytes+1)
	paddedToken := " signed-token "
	tests := []struct {
		name    string
		request *opensplunk.ListSearchJobsRequest
	}{
		{name: "explicit zero page size", request: &opensplunk.ListSearchJobsRequest{Page: &opensplunk.PageRequest{PageSize: &zero}}},
		{name: "oversized token", request: &opensplunk.ListSearchJobsRequest{Page: &opensplunk.PageRequest{PageToken: &oversizedToken}}},
		{name: "padded token", request: &opensplunk.ListSearchJobsRequest{Page: &opensplunk.PageRequest{PageToken: &paddedToken}}},
		{name: "too many raw states", request: &opensplunk.ListSearchJobsRequest{StateFilters: tooManyStates}},
		{name: "unspecified state", request: &opensplunk.ListSearchJobsRequest{StateFilters: []opensplunk.SearchJobState{
			opensplunk.SearchJobState_SEARCH_JOB_STATE_UNSPECIFIED,
		}}},
		{name: "finalizing state", request: &opensplunk.ListSearchJobsRequest{StateFilters: []opensplunk.SearchJobState{
			opensplunk.SearchJobState_SEARCH_JOB_STATE_FINALIZING,
		}}},
		{name: "unknown state", request: &opensplunk.ListSearchJobsRequest{StateFilters: []opensplunk.SearchJobState{opensplunk.SearchJobState(100)}}},
		{name: "oversized app", request: &opensplunk.ListSearchJobsRequest{AppIdFilter: &oversizedApp}},
		{name: "control app", request: &opensplunk.ListSearchJobsRequest{AppIdFilter: &controlApp}},
		{name: "oversized text", request: &opensplunk.ListSearchJobsRequest{TextFilter: &oversizedText}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			jobs := &fakeSearchJobs{}
			handler := newSearchJobListTestHandler(t, jobs, Config{})
			response := postProto(t, handler, searchJobsListPath, test.request)
			if response.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
			}
			jobs.mu.Lock()
			calls := jobs.listCalls
			jobs.mu.Unlock()
			if calls != 0 {
				t.Fatalf("service calls = %d, want 0", calls)
			}
		})
	}
}

func TestSearchJobListRejectsMaliciousServiceOutput(t *testing.T) {
	baseJob := func(id string, createdAt time.Time) searchjobs.Job {
		return listSearchJob(id, createdAt)
	}
	tests := []struct {
		name    string
		request *opensplunk.ListSearchJobsRequest
		page    func() searchjobs.JobListPage
	}{
		{
			name:    "cross owner",
			request: &opensplunk.ListSearchJobsRequest{},
			page: func() searchjobs.JobListPage {
				job := baseJob("job-a", testNow)
				job.OwnerID = "other"
				return listPage(job)
			},
		},
		{
			name:    "cross tenant",
			request: &opensplunk.ListSearchJobsRequest{},
			page: func() searchjobs.JobListPage {
				job := baseJob("job-a", testNow)
				job.TenantID = "other"
				return listPage(job)
			},
		},
		{
			name:    "failed state missing failure",
			request: &opensplunk.ListSearchJobsRequest{},
			page: func() searchjobs.JobListPage {
				job := baseJob("job-a", testNow)
				job.State = searchjobs.StateFailed
				return listPage(job)
			},
		},
		{
			name:    "nonfailed state carries failure",
			request: &opensplunk.ListSearchJobsRequest{},
			page: func() searchjobs.JobListPage {
				job := baseJob("job-a", testNow)
				job.Failure = &searchjobs.Failure{Code: searchjobs.FailureExecution, Message: "failed"}
				return listPage(job)
			},
		},
		{
			name:    "invalid failure code",
			request: &opensplunk.ListSearchJobsRequest{},
			page: func() searchjobs.JobListPage {
				job := baseJob("job-a", testNow)
				job.State = searchjobs.StateFailed
				job.Failure = &searchjobs.Failure{Code: searchjobs.FailureCode("unknown"), Message: "failed"}
				return listPage(job)
			},
		},
		{
			name:    "blank failure message",
			request: &opensplunk.ListSearchJobsRequest{},
			page: func() searchjobs.JobListPage {
				job := baseJob("job-a", testNow)
				job.State = searchjobs.StateFailed
				job.Failure = &searchjobs.Failure{Code: searchjobs.FailureExecution, Message: " \t "}
				return listPage(job)
			},
		},
		{
			name:    "invalid internal state",
			request: &opensplunk.ListSearchJobsRequest{},
			page: func() searchjobs.JobListPage {
				job := baseJob("job-a", testNow)
				job.State = searchjobs.StateInvalid
				return listPage(job)
			},
		},
		{
			name:    "empty ID",
			request: &opensplunk.ListSearchJobsRequest{},
			page: func() searchjobs.JobListPage {
				return listPage(baseJob("", testNow))
			},
		},
		{
			name:    "duplicate ID",
			request: &opensplunk.ListSearchJobsRequest{},
			page: func() searchjobs.JobListPage {
				first := baseJob("job-a", testNow)
				second := baseJob("job-a", testNow.Add(-time.Second))
				return searchjobs.JobListPage{Jobs: []searchjobs.JobListItem{listItem(first), listItem(second)}}
			},
		},
		{
			name:    "ascending creation time",
			request: &opensplunk.ListSearchJobsRequest{},
			page: func() searchjobs.JobListPage {
				first := baseJob("job-b", testNow.Add(-time.Second))
				second := baseJob("job-a", testNow)
				return searchjobs.JobListPage{Jobs: []searchjobs.JobListItem{listItem(first), listItem(second)}}
			},
		},
		{
			name:    "ascending ID tie break",
			request: &opensplunk.ListSearchJobsRequest{},
			page: func() searchjobs.JobListPage {
				first := baseJob("job-a", testNow)
				second := baseJob("job-b", testNow)
				return searchjobs.JobListPage{Jobs: []searchjobs.JobListItem{listItem(first), listItem(second)}}
			},
		},
		{
			name: "state filter mismatch",
			request: &opensplunk.ListSearchJobsRequest{StateFilters: []opensplunk.SearchJobState{
				opensplunk.SearchJobState_SEARCH_JOB_STATE_FAILED,
			}},
			page: func() searchjobs.JobListPage { return listPage(baseJob("job-a", testNow)) },
		},
		{
			name: "app filter mismatch",
			request: func() *opensplunk.ListSearchJobsRequest {
				app := "app-main"
				return &opensplunk.ListSearchJobsRequest{AppIdFilter: &app}
			}(),
			page: func() searchjobs.JobListPage { return listPage(baseJob("job-a", testNow)) },
		},
		{
			name: "text filter mismatch",
			request: func() *opensplunk.ListSearchJobsRequest {
				text := "needle"
				return &opensplunk.ListSearchJobsRequest{TextFilter: &text}
			}(),
			page: func() searchjobs.JobListPage { return listPage(baseJob("job-a", testNow)) },
		},
		{
			name: "more than requested",
			request: func() *opensplunk.ListSearchJobsRequest {
				size := uint32(1)
				return &opensplunk.ListSearchJobsRequest{Page: &opensplunk.PageRequest{PageSize: &size}}
			}(),
			page: func() searchjobs.JobListPage {
				return searchjobs.JobListPage{Jobs: []searchjobs.JobListItem{
					listItem(baseJob("job-b", testNow)),
					listItem(baseJob("job-a", testNow)),
				}}
			},
		},
		{
			name:    "token on short page",
			request: &opensplunk.ListSearchJobsRequest{},
			page: func() searchjobs.JobListPage {
				page := listPage(baseJob("job-a", testNow))
				page.NextPageToken = "unexpected"
				return page
			},
		},
		{
			name: "invalid token",
			request: func() *opensplunk.ListSearchJobsRequest {
				size := uint32(1)
				return &opensplunk.ListSearchJobsRequest{Page: &opensplunk.PageRequest{PageSize: &size}}
			}(),
			page: func() searchjobs.JobListPage {
				page := listPage(baseJob("job-a", testNow))
				page.NextPageToken = " bad "
				return page
			},
		},
		{
			name: "control byte in token",
			request: func() *opensplunk.ListSearchJobsRequest {
				size := uint32(1)
				return &opensplunk.ListSearchJobsRequest{Page: &opensplunk.PageRequest{PageSize: &size}}
			}(),
			page: func() searchjobs.JobListPage {
				page := listPage(baseJob("job-a", testNow))
				page.NextPageToken = "bad\x00token"
				return page
			},
		},
		{
			name:    "unexpected total",
			request: &opensplunk.ListSearchJobsRequest{},
			page: func() searchjobs.JobListPage {
				page := listPage(baseJob("job-a", testNow))
				page.TotalSize = new(uint64(1))
				page.TotalSizeExact = true
				return page
			},
		},
		{
			name:    "exact without total",
			request: &opensplunk.ListSearchJobsRequest{},
			page: func() searchjobs.JobListPage {
				page := listPage(baseJob("job-a", testNow))
				page.TotalSizeExact = true
				return page
			},
		},
		{
			name: "missing requested total",
			request: &opensplunk.ListSearchJobsRequest{Page: &opensplunk.PageRequest{
				IncludeTotalSize: true,
			}},
			page: func() searchjobs.JobListPage { return listPage(baseJob("job-a", testNow)) },
		},
		{
			name: "total smaller than page",
			request: &opensplunk.ListSearchJobsRequest{Page: &opensplunk.PageRequest{
				IncludeTotalSize: true,
			}},
			page: func() searchjobs.JobListPage {
				first := baseJob("job-b", testNow)
				second := baseJob("job-a", testNow.Add(-time.Second))
				total := uint64(1)
				return searchjobs.JobListPage{
					Jobs:           []searchjobs.JobListItem{listItem(first), listItem(second)},
					TotalSize:      &total,
					TotalSizeExact: true,
				}
			},
		},
		{
			name: "first terminal total exceeds page",
			request: &opensplunk.ListSearchJobsRequest{Page: &opensplunk.PageRequest{
				IncludeTotalSize: true,
			}},
			page: func() searchjobs.JobListPage {
				total := uint64(2)
				page := listPage(baseJob("job-a", testNow))
				page.TotalSize = &total
				page.TotalSizeExact = true
				return page
			},
		},
		{
			name: "continued page total has no remaining item",
			request: func() *opensplunk.ListSearchJobsRequest {
				size := uint32(1)
				return &opensplunk.ListSearchJobsRequest{Page: &opensplunk.PageRequest{
					PageSize: &size, IncludeTotalSize: true,
				}}
			}(),
			page: func() searchjobs.JobListPage {
				total := uint64(1)
				page := listPage(baseJob("job-a", testNow))
				page.NextPageToken = "next-page"
				page.TotalSize = &total
				page.TotalSizeExact = true
				return page
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			jobs := &fakeSearchJobs{listPage: test.page()}
			handler := newSearchJobListTestHandler(t, jobs, Config{})
			response := postProto(t, handler, searchJobsListPath, test.request)
			if response.Code != http.StatusInternalServerError {
				t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
			}
			if response.Body.String() != "{\"error\":{\"message\":\"internal server error\"}}" {
				t.Fatalf("error body = %q", response.Body.String())
			}
		})
	}
}

func TestSearchJobListBoundsResponseAndHidesServiceErrors(t *testing.T) {
	t.Run("response cap", func(t *testing.T) {
		job := listSearchJob("job-a", testNow)
		job.NormalizedSPL = strings.Repeat("x", maximumSearchJobListResponseBytes+1)
		handler := newSearchJobListTestHandler(t, &fakeSearchJobs{listPage: listPage(job)}, Config{})
		response := postProto(t, handler, searchJobsListPath, &opensplunk.ListSearchJobsRequest{})
		if response.Code != http.StatusInternalServerError {
			t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
		}
		if response.Body.Len() >= maximumSearchJobListResponseBytes {
			t.Fatalf("error response length = %d", response.Body.Len())
		}
	})

	t.Run("service error secrecy", func(t *testing.T) {
		const secret = "SELECT password FROM secret_table"
		handler := newSearchJobListTestHandler(t, &fakeSearchJobs{listErr: errors.New(secret)}, Config{})
		response := postProto(t, handler, searchJobsListPath, &opensplunk.ListSearchJobsRequest{})
		if response.Code != http.StatusInternalServerError {
			t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
		}
		if strings.Contains(response.Body.String(), secret) {
			t.Fatalf("service error leaked: %q", response.Body.String())
		}
	})

	for _, operationErr := range []error{context.Canceled, context.DeadlineExceeded} {
		t.Run(operationErr.Error(), func(t *testing.T) {
			handler := newSearchJobListTestHandler(t, &fakeSearchJobs{listErr: operationErr}, Config{})
			response := postProto(t, handler, searchJobsListPath, &opensplunk.ListSearchJobsRequest{})
			if response.Code != http.StatusRequestTimeout {
				t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
			}
		})
	}

	t.Run("manager validation", func(t *testing.T) {
		const detail = "sensitive manager validation detail"
		handler := newSearchJobListTestHandler(t, &fakeSearchJobs{
			listErr: errors.Join(searchjobs.ErrInvalidListFilter, errors.New(detail)),
		}, Config{})
		response := postProto(t, handler, searchJobsListPath, &opensplunk.ListSearchJobsRequest{})
		if response.Code != http.StatusBadRequest {
			t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
		}
		if !strings.Contains(response.Body.String(), "search job list filter is invalid") ||
			strings.Contains(response.Body.String(), detail) {
			t.Fatalf("validation response = %q", response.Body.String())
		}
	})
}

func TestSearchJobListAcquiresSerializationPermitBeforeService(t *testing.T) {
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	jobs := &fakeSearchJobs{listFn: func(context.Context, searchjobs.AccessScope, searchjobs.JobListRequest) (searchjobs.JobListPage, error) {
		entered <- struct{}{}
		<-release
		return searchjobs.JobListPage{}, nil
	}}
	handler := newSearchJobListTestHandler(t, jobs, Config{MaximumConcurrentResponses: 1})
	payload, err := proto.Marshal(&opensplunk.ListSearchJobsRequest{})
	if err != nil {
		t.Fatal(err)
	}
	serve := func() int {
		request := httptest.NewRequestWithContext(context.Background(), http.MethodPost, searchJobsListPath, bytes.NewReader(payload))
		request.Header.Set("Content-Type", "application/x-protobuf")
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		return response.Code
	}
	firstDone := make(chan int, 1)
	go func() { firstDone <- serve() }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("first list request did not enter service")
	}
	if status := serve(); status != http.StatusServiceUnavailable {
		t.Fatalf("second list status = %d, want %d", status, http.StatusServiceUnavailable)
	}
	select {
	case <-entered:
		t.Fatal("rejected request entered service")
	default:
	}
	close(release)
	select {
	case status := <-firstDone:
		if status != http.StatusOK {
			t.Fatalf("first list status = %d", status)
		}
	case <-time.After(time.Second):
		t.Fatal("first list request did not finish")
	}
}

func TestSearchJobListCancellationPreventsResponseTransfer(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	jobs := &fakeSearchJobs{listFn: func(context.Context, searchjobs.AccessScope, searchjobs.JobListRequest) (searchjobs.JobListPage, error) {
		cancel()
		return listPage(listSearchJob("job-a", testNow)), nil
	}}
	handler := newSearchJobListTestHandler(t, jobs, Config{})
	payload, err := proto.Marshal(&opensplunk.ListSearchJobsRequest{})
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequestWithContext(context.Background(), http.MethodPost, searchJobsListPath, bytes.NewReader(payload)).WithContext(ctx)
	request.Header.Set("Content-Type", "application/x-protobuf")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusRequestTimeout {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
	if response.Header().Get("Content-Type") == "application/x-protobuf" {
		t.Fatalf("canceled response was transferred as protobuf")
	}
}

func TestSearchJobListRouteIsExactAndPostOnly(t *testing.T) {
	handler := newSearchJobListTestHandler(t, &fakeSearchJobs{}, Config{})

	request := httptest.NewRequestWithContext(context.Background(), http.MethodGet, searchJobsListPath, nil)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusMethodNotAllowed || response.Header().Get("Allow") != http.MethodPost {
		t.Fatalf("GET response = %d, Allow %q", response.Code, response.Header().Get("Allow"))
	}

	response = postProto(t, handler, searchJobsListPath+"/extra", &opensplunk.ListSearchJobsRequest{})
	if response.Code != http.StatusNotFound {
		t.Fatalf("suffix status = %d, body = %s", response.Code, response.Body.String())
	}
}

func newSearchJobListTestHandler(t *testing.T, jobs SearchJobs, overrides Config) *Handler {
	t.Helper()
	overrides.SearchJobs = jobs
	overrides.Indexes = fakeIndexCatalog{}
	overrides.WebUI = testUI()
	overrides.OwnerID = "owner-1"
	overrides.TenantID = "tenant-1"
	overrides.Now = func() time.Time { return testNow }
	return newTestHandler(t, overrides)
}

func listSearchJob(id string, createdAt time.Time) searchjobs.Job {
	job := completeJob(id)
	job.CreatedAt = createdAt
	job.OwnerID = "owner-1"
	job.TenantID = "tenant-1"
	job.Schema = nil
	if job.Failure != nil {
		job.Failure.Diagnostics = nil
	}
	return job
}

func listItem(job searchjobs.Job) searchjobs.JobListItem {
	item := searchjobs.JobListItem{
		ID:                job.ID,
		Version:           job.Version,
		OwnerID:           job.OwnerID,
		TenantID:          job.TenantID,
		SPL:               job.SPL,
		NormalizedSPL:     job.NormalizedSPL,
		RequestedIndexes:  slices.Clone(job.RequestedIndexes),
		EffectiveIndexes:  slices.Clone(job.EffectiveIndexes),
		TimeRange:         job.TimeRange,
		AppID:             job.AppID,
		Source:            job.Source,
		Earliest:          job.Earliest,
		Latest:            job.Latest,
		IndexTimeCutoff:   job.IndexTimeCutoff,
		State:             job.State,
		ScannedRows:       job.ScannedRows,
		ScannedBytes:      job.ScannedBytes,
		RowCount:          job.RowCount,
		ResultBytes:       job.ResultBytes,
		ResultsTruncated:  job.ResultsTruncated,
		KnowledgeSnapshot: cloneKnowledgeSnapshotSummaryForListTest(job.KnowledgeSnapshot),
		CreatedAt:         job.CreatedAt,
		StartedAt:         job.StartedAt,
		FinishedAt:        job.FinishedAt,
		ExpiresAt:         job.ExpiresAt,
	}
	if job.Failure != nil {
		item.Failure = &searchjobs.JobListFailure{
			Code:      job.Failure.Code,
			Message:   job.Failure.Message,
			Retryable: job.Failure.Retryable,
		}
	}
	return item
}

func cloneKnowledgeSnapshotSummaryForListTest(
	summary *opensplunk.KnowledgeSnapshotSummary,
) *opensplunk.KnowledgeSnapshotSummary {
	if summary == nil {
		return nil
	}
	return proto.Clone(summary).(*opensplunk.KnowledgeSnapshotSummary)
}

func listPage(jobs ...searchjobs.Job) searchjobs.JobListPage {
	items := make([]searchjobs.JobListItem, len(jobs))
	for index := range jobs {
		items[index] = listItem(jobs[index])
	}
	return searchjobs.JobListPage{Jobs: items}
}

type batchListSearchArtifacts struct {
	launchSearchArtifacts
	records      map[string]searchartifacts.Record
	ids          []string
	inspectCalls int
	listCalls    int
}

func (artifacts *batchListSearchArtifacts) ListPage(
	_ context.Context,
	_ searchjobs.AccessScope,
	_ searchartifacts.ListRequest,
) (searchartifacts.ListPage, error) {
	artifacts.listCalls++
	items := make([]searchartifacts.ListItem, 0, len(artifacts.records))
	for _, record := range artifacts.records {
		items = append(items, searchartifacts.ListItem{Record: record})
	}
	slices.SortFunc(items, func(left, right searchartifacts.ListItem) int {
		if order := right.Record.Job.CreatedAt.Compare(left.Record.Job.CreatedAt); order != 0 {
			return order
		}
		return strings.Compare(right.Record.Job.ID, left.Record.Job.ID)
	})
	return searchartifacts.ListPage{Items: items}, nil
}

type durableListResultLease struct {
	returned bool
}

func (*durableListResultLease) Schema() searchjobs.Schema {
	return searchjobs.Schema{Columns: []searchjobs.Column{{Name: "value", Kind: searchjobs.ValueKindString}}}
}

func (*durableListResultLease) RowCount() uint64       { return 1 }
func (*durableListResultLease) RowCountExact() bool    { return true }
func (*durableListResultLease) ResultsTruncated() bool { return false }
func (*durableListResultLease) Generation() uint64     { return 1 }
func (*durableListResultLease) Close() error           { return nil }
func (lease *durableListResultLease) Next(ctx context.Context) (searchjobs.ResultRow, bool, error) {
	if err := ctx.Err(); err != nil {
		return searchjobs.ResultRow{}, false, err
	}
	if lease.returned {
		return searchjobs.ResultRow{}, false, nil
	}
	lease.returned = true
	return searchjobs.ResultRow{Ordinal: 0, Values: []searchjobs.Value{searchjobs.StringValue("retained")}}, true, nil
}

func durableListQueuedJob(id string, created time.Time, owner string) searchjobs.Job {
	job := listSearchJob(id, created)
	job.Version = 1
	job.OwnerID = owner
	job.State = searchjobs.StateQueued
	job.Schema = nil
	job.StartedAt = time.Time{}
	job.FinishedAt = time.Time{}
	job.ExpiresAt = time.Time{}
	job.RowCount = 0
	job.ResultBytes = 0
	job.Failure = nil
	return job
}

func persistDurableListCompletion(
	t *testing.T,
	store *searchartifacts.Store,
	queued searchjobs.Job,
	finished time.Time,
	lifetime time.Duration,
) {
	t.Helper()
	ctx := context.Background()
	if err := store.Admit(ctx, queued); err != nil {
		t.Fatal(err)
	}
	completed := queued
	completed.Version = 2
	completed.State = searchjobs.StateCompleted
	completed.StartedAt = finished
	completed.FinishedAt = finished
	completed.ExpiresAt = finished.Add(lifetime)
	completed.Schema = new((&durableListResultLease{}).Schema())
	completed.RowCount = 1
	if err := store.Finalize(ctx, completed); err != nil {
		t.Fatal(err)
	}
	if _, err := store.PersistResults(ctx, searchjobs.AccessScope{
		TenantID: completed.TenantID, OwnerID: completed.OwnerID,
	}, completed.ID, &durableListResultLease{}); err != nil {
		t.Fatal(err)
	}
}

func protoSearchJobIDs(jobs []*opensplunk.SearchJob) []string {
	result := make([]string, len(jobs))
	for index, job := range jobs {
		result[index] = job.GetSearchJobId()
	}
	return result
}

func (artifacts *batchListSearchArtifacts) InspectMany(
	_ context.Context,
	_ searchjobs.AccessScope,
	ids []string,
) (map[string]searchartifacts.Record, error) {
	artifacts.inspectCalls++
	artifacts.ids = append([]string(nil), ids...)
	return artifacts.records, nil
}
