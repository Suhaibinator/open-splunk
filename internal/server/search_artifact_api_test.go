package server

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/searchartifacts"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
)

func TestSearchArtifactVersionConflictMapsToHTTPConflict(t *testing.T) {
	assertHTTPErrorStatus(t, mapSearchArtifactError(searchartifacts.ErrConflict), http.StatusConflict)
}

func TestRetainedSearchJobClassProjectionIsExhaustive(t *testing.T) {
	t.Parallel()
	tests := []struct {
		class searchartifacts.RetentionClass
		want  opensplunk.SearchJobRetentionClass
	}{
		{searchartifacts.RetentionManual, opensplunk.SearchJobRetentionClass_SEARCH_JOB_RETENTION_CLASS_MANUAL},
		{searchartifacts.RetentionShared, opensplunk.SearchJobRetentionClass_SEARCH_JOB_RETENTION_CLASS_SHARED},
		{searchartifacts.RetentionScheduledReport, opensplunk.SearchJobRetentionClass_SEARCH_JOB_RETENTION_CLASS_SCHEDULED_REPORT},
		{searchartifacts.RetentionScheduledAlert, opensplunk.SearchJobRetentionClass_SEARCH_JOB_RETENTION_CLASS_SCHEDULED_ALERT},
		{searchartifacts.RetentionTriggeredWebhook, opensplunk.SearchJobRetentionClass_SEARCH_JOB_RETENTION_CLASS_TRIGGERED_WEBHOOK},
	}
	for _, test := range tests {
		got, err := retainedSearchJobClassToProto(test.class)
		if err != nil || got != test.want {
			t.Fatalf("retention class %d = %v, %v; want %v", test.class, got, err, test.want)
		}
	}
	if _, err := retainedSearchJobClassToProto(searchartifacts.RetentionInvalid); err == nil {
		t.Fatal("invalid retention class was projected")
	}
}

func TestInterruptedSearchJobProjectsTerminalMissingResult(t *testing.T) {
	t.Parallel()
	job := completeJob("interrupted-deep-link")
	job.State = searchjobs.StateRunning
	record := searchartifacts.Record{
		Job: job, State: searchartifacts.StateInterrupted,
		Visibility: searchartifacts.VisibilityPrivate, RetentionClass: searchartifacts.RetentionManual,
		Lifetime: 10 * time.Minute, ExpiresAt: testNow.Add(10 * time.Minute),
	}
	projected, err := retainedSearchJobToProto(record, testNow)
	if err != nil {
		t.Fatal(err)
	}
	if projected.GetState() != opensplunk.SearchJobState_SEARCH_JOB_STATE_INTERRUPTED ||
		projected.GetRetainedResultStatus() != opensplunk.RetainedResultStatus_RETAINED_RESULT_STATUS_MISSING {
		t.Fatalf("interrupted durable projection = %+v", projected)
	}
}

func TestGetSearchJobLaunchesExpiredTombstoneWithoutResults(t *testing.T) {
	job := completeJob("expired-deep-link")
	job.ExpiresAt = testNow
	artifacts := &launchSearchArtifacts{record: searchartifacts.Record{
		Job:             job,
		State:           searchartifacts.StateExpired,
		Visibility:      searchartifacts.VisibilityPrivate,
		RetentionClass:  searchartifacts.RetentionManual,
		Lifetime:        10 * time.Minute,
		ExpiresAt:       testNow,
		ArtifactPresent: true,
	}}
	handler := &apiHandler{
		jobs:            &fakeSearchJobs{getErr: searchjobs.ErrNotFound},
		searchArtifacts: artifacts,
		tenantID:        "tenant-1",
		ownerID:         "owner-1",
		now:             func() time.Time { return testNow },
	}
	response, err := handler.getSearchJob(
		httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/api/search/jobs/get", nil),
		&opensplunk.GetSearchJobRequest{SearchJobId: job.ID},
	)
	if err != nil {
		t.Fatal(err)
	}
	if artifacts.mode != searchartifacts.AccessLaunch {
		t.Fatalf("durable get mode = %v, want launch", artifacts.mode)
	}
	projected := response.GetSearchJob()
	if projected.GetState() != opensplunk.SearchJobState_SEARCH_JOB_STATE_EXPIRED ||
		projected.GetRetainedResultStatus() != opensplunk.RetainedResultStatus_RETAINED_RESULT_STATUS_EXPIRED ||
		projected.GetDefinition().GetSpl() != job.SPL {
		t.Fatalf("expired durable projection = %+v", projected)
	}
}

func TestGetSearchJobOverlaysLiveStateOnDurableRetentionMetadata(t *testing.T) {
	t.Parallel()
	job := completeJob("live-durable-deep-link")
	job.State = searchjobs.StateRunning
	job.FinishedAt = time.Time{}
	job.ScannedRows = 17
	job.ScannedBytes = 170
	job.RowCount = 3
	job.Version = 4
	expiresAt := testNow.Add(10 * time.Minute)
	artifacts := &launchSearchArtifacts{record: searchartifacts.Record{
		Job: searchjobs.Job{
			ID: job.ID, OwnerID: job.OwnerID, TenantID: job.TenantID,
			SPL: job.SPL, NormalizedSPL: job.NormalizedSPL, State: searchjobs.StateQueued,
			Version: 1, CreatedAt: job.CreatedAt,
		},
		State: searchartifacts.StateQueued, Visibility: searchartifacts.VisibilityPrivate,
		RetentionClass: searchartifacts.RetentionManual, Lifetime: 10 * time.Minute, ExpiresAt: expiresAt,
	}}
	handler := &apiHandler{
		jobs: &fakeSearchJobs{getJob: job}, searchArtifacts: artifacts,
		tenantID: "tenant-1", ownerID: "owner-1", now: func() time.Time { return testNow },
	}
	response, err := handler.getSearchJob(
		httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/api/search/jobs/get", nil),
		&opensplunk.GetSearchJobRequest{SearchJobId: job.ID},
	)
	if err != nil {
		t.Fatal(err)
	}
	projected := response.GetSearchJob()
	if projected.GetState() != opensplunk.SearchJobState_SEARCH_JOB_STATE_RUNNING ||
		projected.GetStateVersion() != job.Version ||
		projected.GetProgress().GetScannedRows() != job.ScannedRows ||
		!projected.GetExpiresAt().AsTime().Equal(expiresAt) ||
		projected.GetRetainedResultStatus() != opensplunk.RetainedResultStatus_RETAINED_RESULT_STATUS_PENDING {
		t.Fatalf("live durable projection = %+v", projected)
	}
}

func TestGetSearchResultsRejectsTerminalDurablePublicationFailure(t *testing.T) {
	t.Parallel()
	job := completeJob("failed-durable-publication")
	artifacts := &launchSearchArtifacts{record: searchartifacts.Record{
		Job: job, State: searchartifacts.StateFailed, Visibility: searchartifacts.VisibilityPrivate,
		RetentionClass: searchartifacts.RetentionManual, Lifetime: 10 * time.Minute,
		ExpiresAt: testNow.Add(10 * time.Minute),
	}}
	jobs := &fakeSearchJobs{getJob: job, resultsPage: searchjobs.ResultPage{
		Rows: []searchjobs.ResultRow{{Ordinal: 0, Values: []searchjobs.Value{searchjobs.StringValue("must-not-leak")}}},
	}}
	handler := &apiHandler{
		jobs: jobs, searchArtifacts: artifacts, tenantID: "tenant-1", ownerID: "owner-1",
		now: func() time.Time { return testNow },
	}
	_, err := handler.getSearchResults(
		httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/api/search/jobs/results", nil),
		&opensplunk.GetSearchResultsRequest{SearchJobId: job.ID},
	)
	assertHTTPErrorStatus(t, err, http.StatusConflict)
	if jobs.resultsID != "" {
		t.Fatalf("in-memory results were exposed after durable publication failure: %q", jobs.resultsID)
	}
}

func TestExpiredDurableResultReadReturnsGone(t *testing.T) {
	artifacts := &launchSearchArtifacts{}
	handler := newTestHandler(t, Config{
		SearchJobs:      &fakeSearchJobs{getErr: searchjobs.ErrNotFound},
		SearchArtifacts: artifacts,
		Indexes:         fakeIndexCatalog{},
		WebUI:           testUI(),
	})
	response := postProto(t, handler, "/api/search/jobs/results", &opensplunk.GetSearchResultsRequest{
		SearchJobId: "expired-deep-link",
	})
	if response.Code != http.StatusGone {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
}

func TestSharedDurableResultsSurviveInMemoryManualExpiry(t *testing.T) {
	job := completeJob("shared-job")
	job.State = searchjobs.StateExpired
	job.SPL = "index=main | table message"
	recordJob := job
	recordJob.State = searchjobs.StateCompleted
	artifacts := &durableResultArtifacts{record: searchartifacts.Record{
		Job: recordJob, State: searchartifacts.StateCompleted,
		Visibility: searchartifacts.VisibilityEveryone, RetentionClass: searchartifacts.RetentionShared,
		Lifetime: 7 * 24 * time.Hour, ExpiresAt: testNow.Add(7 * 24 * time.Hour), ArtifactPresent: true,
	}}
	jobs := &fakeSearchJobs{getJob: job, resultsErr: searchjobs.ErrExpired}
	handler := newTestHandler(t, Config{
		SearchJobs: jobs, SearchArtifacts: artifacts,
		Indexes: fakeIndexCatalog{}, WebUI: testUI(),
	})
	response := postProto(t, handler, "/api/search/jobs/results", &opensplunk.GetSearchResultsRequest{
		SearchJobId: job.ID,
	})
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
	var decoded opensplunk.GetSearchResultsResponse
	unmarshalResponse(t, response, &decoded)
	if got := decoded.GetResultPage().GetRows()[0].GetCells()[0].GetStringValue(); got != "durable" {
		t.Fatalf("durable result = %q", got)
	}
	if jobs.resultsID != "" {
		t.Fatalf("in-memory results were read after expiry: %q", jobs.resultsID)
	}
}

type launchSearchArtifacts struct {
	record searchartifacts.Record
	mode   searchartifacts.AccessMode
}

func (artifacts *launchSearchArtifacts) Get(
	_ context.Context,
	_ searchjobs.AccessScope,
	_ string,
	mode searchartifacts.AccessMode,
) (searchartifacts.Record, error) {
	artifacts.mode = mode
	return artifacts.record, nil
}

func (*launchSearchArtifacts) ListPage(
	context.Context,
	searchjobs.AccessScope,
	searchartifacts.ListRequest,
) (searchartifacts.ListPage, error) {
	return searchartifacts.ListPage{Items: []searchartifacts.ListItem{}}, nil
}

func (*launchSearchArtifacts) Acquire(context.Context, searchjobs.AccessScope, string) (searchartifacts.ResultLease, error) {
	return nil, searchartifacts.ErrExpired
}

func (*launchSearchArtifacts) ShareExpected(context.Context, searchjobs.AccessScope, string, uint64) (searchartifacts.Record, error) {
	return searchartifacts.Record{}, searchartifacts.ErrExpired
}

func (*launchSearchArtifacts) UpdateSettingsExpected(context.Context, searchjobs.AccessScope, string, searchartifacts.Settings, uint64) (searchartifacts.Record, error) {
	return searchartifacts.Record{}, searchartifacts.ErrExpired
}

type durableResultArtifacts struct {
	record searchartifacts.Record
}

func (artifacts *durableResultArtifacts) Get(
	_ context.Context,
	_ searchjobs.AccessScope,
	_ string,
	_ searchartifacts.AccessMode,
) (searchartifacts.Record, error) {
	return artifacts.record, nil
}

func (*durableResultArtifacts) ListPage(
	context.Context,
	searchjobs.AccessScope,
	searchartifacts.ListRequest,
) (searchartifacts.ListPage, error) {
	return searchartifacts.ListPage{Items: []searchartifacts.ListItem{}}, nil
}

func (artifacts *durableResultArtifacts) Acquire(context.Context, searchjobs.AccessScope, string) (searchartifacts.ResultLease, error) {
	return &durableResultLease{}, nil
}

func (*durableResultArtifacts) ShareExpected(context.Context, searchjobs.AccessScope, string, uint64) (searchartifacts.Record, error) {
	return searchartifacts.Record{}, searchartifacts.ErrInvalid
}

func (*durableResultArtifacts) UpdateSettingsExpected(context.Context, searchjobs.AccessScope, string, searchartifacts.Settings, uint64) (searchartifacts.Record, error) {
	return searchartifacts.Record{}, searchartifacts.ErrInvalid
}

type durableResultLease struct {
	read bool
}

func (*durableResultLease) Schema() searchjobs.Schema {
	return searchjobs.Schema{Columns: []searchjobs.Column{{Name: "message", Kind: searchjobs.ValueKindString}}}
}

func (*durableResultLease) RowCount() uint64       { return 1 }
func (*durableResultLease) RowCountExact() bool    { return true }
func (*durableResultLease) ResultsTruncated() bool { return false }
func (*durableResultLease) Generation() uint64     { return 1 }
func (lease *durableResultLease) Next(context.Context) (searchjobs.ResultRow, bool, error) {
	if lease.read {
		return searchjobs.ResultRow{}, false, nil
	}
	lease.read = true
	return searchjobs.ResultRow{Ordinal: 0, Values: []searchjobs.Value{searchjobs.StringValue("durable")}}, true, nil
}
func (*durableResultLease) Close() error { return nil }
