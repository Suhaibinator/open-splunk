package server

import (
	"context"
	"fmt"
	"net/http"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
	"github.com/Suhaibinator/open-splunk/internal/searchtime"
)

type searchJobListIntegrationExecutor struct {
	entered chan struct{}
	release <-chan struct{}
}

func (executor *searchJobListIntegrationExecutor) Execute(
	ctx context.Context,
	_ clickhouse.CompiledQuery,
	sink searchjobs.ResultSink,
) error {
	if err := sink.SetSchema(searchjobs.Schema{Columns: []searchjobs.Column{{
		Name: "message",
		Kind: searchjobs.ValueKindString,
	}}}); err != nil {
		return err
	}
	select {
	case executor.entered <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	}
	select {
	case <-executor.release:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

type searchJobListIntegrationSnapshotter uint64

func (snapshot searchJobListIntegrationSnapshotter) VisibilityCutoff(context.Context) (uint64, error) {
	return uint64(snapshot), nil
}

type searchJobListIntegrationClock struct {
	mu  sync.Mutex
	now time.Time
}

func (clock *searchJobListIntegrationClock) Now() time.Time {
	clock.mu.Lock()
	defer clock.mu.Unlock()
	return clock.now
}

func (clock *searchJobListIntegrationClock) Set(now time.Time) {
	clock.mu.Lock()
	clock.now = now
	clock.mu.Unlock()
}

func TestSearchJobListRealManagerBoundsLongParseFailure(t *testing.T) {
	const (
		ownerID  = "owner-long-failure"
		tenantID = "tenant-long-failure"
	)
	anchor := time.Date(2026, time.July, 23, 11, 0, 0, 0, time.UTC)
	clock := &searchJobListIntegrationClock{now: anchor}
	resolvedRange, err := searchtime.NewAbsoluteRange(anchor.Add(-time.Hour), anchor)
	if err != nil {
		t.Fatal(err)
	}
	release := make(chan struct{})
	close(release)
	manager, err := searchjobs.New(searchjobs.Config{
		Executor: &searchJobListIntegrationExecutor{
			entered: make(chan struct{}, 1),
			release: release,
		},
		Snapshotter:     searchJobListIntegrationSnapshotter(1),
		MaxConcurrent:   1,
		CleanupInterval: -1,
		Now:             clock.Now,
		NewID:           func() string { return "list-long-failure" },
		CursorKey:       []byte("0123456789abcdef0123456789abcdef"),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if closeErr := manager.Close(); closeErr != nil {
			t.Errorf("close search manager: %v", closeErr)
		}
	})

	created, err := manager.Create(context.Background(), searchjobs.CreateRequest{
		SPL:               "index=main | " + strings.Repeat("x", 5<<10),
		OwnerID:           ownerID,
		TenantID:          tenantID,
		AuthorizedIndexes: []string{"main"},
		RequestedIndexes:  []string{"main"},
		TimeRange:         resolvedRange,
	})
	if err != nil {
		t.Fatal(err)
	}
	access := searchjobs.AccessScope{OwnerID: ownerID, TenantID: tenantID}
	deadline := time.Now().Add(2 * time.Second)
	for {
		job, getErr := manager.GetFor(access, created.ID)
		if getErr != nil {
			t.Fatal(getErr)
		}
		if job.State == searchjobs.StateFailed {
			if job.Failure == nil || len(job.Failure.Message) <= maximumSearchJobListFailureMessageBytes {
				t.Fatalf("full GET failure message length = %d, want over list bound", len(job.Failure.Message))
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("job state = %v, want failed", job.State)
		}
		time.Sleep(time.Millisecond)
	}

	handler := newTestHandler(t, Config{
		SearchJobs: manager,
		Indexes:    fakeIndexCatalog{},
		WebUI:      testUI(),
		OwnerID:    ownerID,
		TenantID:   tenantID,
		Now:        clock.Now,
	})
	response := postProto(t, handler, searchJobsListPath, &opensplunkv1.ListSearchJobsRequest{})
	if response.Code != http.StatusOK {
		t.Fatalf("list status = %d, body = %s", response.Code, response.Body.String())
	}
	var decoded opensplunkv1.ListSearchJobsResponse
	unmarshalResponse(t, response, &decoded)
	if len(decoded.GetSearchJobs()) != 1 {
		t.Fatalf("listed jobs = %d, want 1", len(decoded.GetSearchJobs()))
	}
	failure := decoded.GetSearchJobs()[0].GetFailure()
	if failure == nil || failure.GetMessage() != "search SPL contains an unsupported command" ||
		len(failure.GetMessage()) > maximumSearchJobListFailureMessageBytes || len(failure.GetDiagnostics()) != 0 {
		t.Fatalf("list failure = %#v", failure)
	}
}

func TestSearchJobListRealManagerHTTPIntegration(t *testing.T) {
	const (
		ownerID      = "owner-list-integration"
		otherOwnerID = "other-list-integration"
		tenantID     = "tenant-list-integration"
	)
	anchor := time.Date(2026, time.July, 23, 12, 0, 0, 0, time.UTC)
	clock := &searchJobListIntegrationClock{now: anchor}
	resolvedRange, err := searchtime.NewAbsoluteRange(anchor.Add(-time.Hour), anchor)
	if err != nil {
		t.Fatal(err)
	}

	release := make(chan struct{})
	var releaseOnce sync.Once
	executor := &searchJobListIntegrationExecutor{
		entered: make(chan struct{}, 4),
		release: release,
	}
	var idSequence atomic.Uint32
	manager, err := searchjobs.New(searchjobs.Config{
		Executor:        executor,
		Snapshotter:     searchJobListIntegrationSnapshotter(41),
		MaxConcurrent:   1,
		CleanupInterval: -1,
		Now:             clock.Now,
		NewID: func() string {
			return fmt.Sprintf("list-integration-%02d", idSequence.Add(1))
		},
		CursorKey: []byte("0123456789abcdef0123456789abcdef"),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		releaseOnce.Do(func() { close(release) })
		if err := manager.Close(); err != nil {
			t.Errorf("close search manager: %v", err)
		}
	})

	admit := func(owner, appID, spl string) searchjobs.Job {
		t.Helper()
		job, createErr := manager.Create(context.Background(), searchjobs.CreateRequest{
			SPL:               spl,
			OwnerID:           owner,
			TenantID:          tenantID,
			AuthorizedIndexes: []string{"main"},
			RequestedIndexes:  []string{"main"},
			TimeRange:         resolvedRange,
			AppID:             appID,
		})
		if createErr != nil {
			t.Fatalf("create job for owner %q: %v", owner, createErr)
		}
		return job
	}

	older := admit(ownerID, "app-errors", "index=main level=ERROR | table message")
	select {
	case <-executor.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("older job did not enter the controlled executor")
	}
	clock.Set(anchor.Add(time.Second))
	newer := admit(ownerID, "app-operations", "index=main level=INFO | table message")
	clock.Set(anchor.Add(2 * time.Second))
	hidden := admit(otherOwnerID, "app-hidden", "index=main level=WARN | table message")

	handler := newTestHandler(t, Config{
		SearchJobs: manager,
		Indexes:    fakeIndexCatalog{},
		WebUI:      testUI(),
		OwnerID:    ownerID,
		TenantID:   tenantID,
		Now:        clock.Now,
	})

	firstPageSize := uint32(1)
	firstPage := searchJobListIntegrationList(t, handler, &opensplunkv1.ListSearchJobsRequest{
		Page: &opensplunkv1.PageRequest{
			PageSize:         &firstPageSize,
			IncludeTotalSize: true,
		},
	})
	searchJobListIntegrationAssertPage(t, firstPage, []string{newer.ID}, 2)
	if firstPage.GetSearchJobs()[0].GetState() != opensplunkv1.SearchJobState_SEARCH_JOB_STATE_QUEUED {
		t.Fatalf("newest job state = %v, want queued", firstPage.GetSearchJobs()[0].GetState())
	}
	firstPageToken := firstPage.GetPage().GetNextPageToken()
	if firstPageToken == "" {
		t.Fatal("first page did not return a continuation token")
	}

	runningPage := searchJobListIntegrationList(t, handler, &opensplunkv1.ListSearchJobsRequest{
		Page: &opensplunkv1.PageRequest{IncludeTotalSize: true},
		StateFilters: []opensplunkv1.SearchJobState{
			opensplunkv1.SearchJobState_SEARCH_JOB_STATE_RUNNING,
		},
	})
	searchJobListIntegrationAssertPage(t, runningPage, []string{older.ID}, 1)

	queuedPage := searchJobListIntegrationList(t, handler, &opensplunkv1.ListSearchJobsRequest{
		Page: &opensplunkv1.PageRequest{IncludeTotalSize: true},
		StateFilters: []opensplunkv1.SearchJobState{
			opensplunkv1.SearchJobState_SEARCH_JOB_STATE_QUEUED,
		},
	})
	searchJobListIntegrationAssertPage(t, queuedPage, []string{newer.ID}, 1)

	appID := "app-operations"
	appPage := searchJobListIntegrationList(t, handler, &opensplunkv1.ListSearchJobsRequest{
		Page:        &opensplunkv1.PageRequest{IncludeTotalSize: true},
		AppIdFilter: &appID,
	})
	searchJobListIntegrationAssertPage(t, appPage, []string{newer.ID}, 1)

	text := "error"
	textPage := searchJobListIntegrationList(t, handler, &opensplunkv1.ListSearchJobsRequest{
		Page:       &opensplunkv1.PageRequest{IncludeTotalSize: true},
		TextFilter: &text,
	})
	searchJobListIntegrationAssertPage(t, textPage, []string{older.ID}, 1)

	getQueued := searchJobListIntegrationGet(t, handler, newer.ID)
	if getQueued.GetState() != opensplunkv1.SearchJobState_SEARCH_JOB_STATE_QUEUED {
		t.Fatalf("GET newest state = %v, want queued", getQueued.GetState())
	}

	clock.Set(anchor.Add(3 * time.Second))
	cancelResponse := postProto(t, handler, "/api/v1/search/jobs/cancel", &opensplunkv1.CancelSearchJobRequest{
		SearchJobId: newer.ID,
	})
	if cancelResponse.Code != http.StatusOK {
		t.Fatalf("cancel status = %d, body = %s", cancelResponse.Code, cancelResponse.Body.String())
	}
	var canceled opensplunkv1.CancelSearchJobResponse
	unmarshalResponse(t, cancelResponse, &canceled)
	if canceled.GetSearchJob().GetState() != opensplunkv1.SearchJobState_SEARCH_JOB_STATE_CANCELED {
		t.Fatalf("canceled state = %v", canceled.GetSearchJob().GetState())
	}

	releaseOnce.Do(func() { close(release) })
	completed := searchJobListIntegrationWaitForState(
		t,
		handler,
		older.ID,
		opensplunkv1.SearchJobState_SEARCH_JOB_STATE_COMPLETED,
	)
	if completed.GetSearchJobId() != older.ID {
		t.Fatalf("completed GET ID = %q, want %q", completed.GetSearchJobId(), older.ID)
	}

	secondPage := searchJobListIntegrationList(t, handler, &opensplunkv1.ListSearchJobsRequest{
		Page: &opensplunkv1.PageRequest{
			PageSize:         &firstPageSize,
			PageToken:        &firstPageToken,
			IncludeTotalSize: true,
		},
	})
	searchJobListIntegrationAssertPage(t, secondPage, []string{older.ID}, 2)
	if secondPage.GetSearchJobs()[0].GetState() != opensplunkv1.SearchJobState_SEARCH_JOB_STATE_COMPLETED {
		t.Fatalf("second-page live state = %v, want completed", secondPage.GetSearchJobs()[0].GetState())
	}
	if secondPage.GetPage().GetNextPageToken() != "" {
		t.Fatalf("last page returned token %q", secondPage.GetPage().GetNextPageToken())
	}

	allPageSize := uint32(10)
	refreshed := searchJobListIntegrationList(t, handler, &opensplunkv1.ListSearchJobsRequest{
		Page: &opensplunkv1.PageRequest{
			PageSize:         &allPageSize,
			IncludeTotalSize: true,
		},
	})
	searchJobListIntegrationAssertPage(t, refreshed, []string{newer.ID, older.ID}, 2)
	wantStates := []opensplunkv1.SearchJobState{
		opensplunkv1.SearchJobState_SEARCH_JOB_STATE_CANCELED,
		opensplunkv1.SearchJobState_SEARCH_JOB_STATE_COMPLETED,
	}
	for index, listed := range refreshed.GetSearchJobs() {
		if listed.GetState() != wantStates[index] {
			t.Fatalf("refreshed job %d state = %v, want %v", index, listed.GetState(), wantStates[index])
		}
		got := searchJobListIntegrationGet(t, handler, listed.GetSearchJobId())
		if got.GetSearchJobId() != listed.GetSearchJobId() || got.GetState() != listed.GetState() {
			t.Fatalf("GET/list mismatch for %q: list=%v get=%+v", listed.GetSearchJobId(), listed.GetState(), got)
		}
	}

	canceledPage := searchJobListIntegrationList(t, handler, &opensplunkv1.ListSearchJobsRequest{
		Page: &opensplunkv1.PageRequest{IncludeTotalSize: true},
		StateFilters: []opensplunkv1.SearchJobState{
			opensplunkv1.SearchJobState_SEARCH_JOB_STATE_CANCELED,
		},
	})
	searchJobListIntegrationAssertPage(t, canceledPage, []string{newer.ID}, 1)

	completedPage := searchJobListIntegrationList(t, handler, &opensplunkv1.ListSearchJobsRequest{
		Page: &opensplunkv1.PageRequest{IncludeTotalSize: true},
		StateFilters: []opensplunkv1.SearchJobState{
			opensplunkv1.SearchJobState_SEARCH_JOB_STATE_COMPLETED,
		},
	})
	searchJobListIntegrationAssertPage(t, completedPage, []string{older.ID}, 1)

	hiddenResponse := postProto(t, handler, "/api/v1/search/jobs/get", &opensplunkv1.GetSearchJobRequest{
		SearchJobId: hidden.ID,
	})
	if hiddenResponse.Code != http.StatusNotFound {
		t.Fatalf("cross-owner GET status = %d, body = %s", hiddenResponse.Code, hiddenResponse.Body.String())
	}
}

func searchJobListIntegrationList(
	t *testing.T,
	handler http.Handler,
	request *opensplunkv1.ListSearchJobsRequest,
) *opensplunkv1.ListSearchJobsResponse {
	t.Helper()
	response := postProto(t, handler, searchJobsListPath, request)
	if response.Code != http.StatusOK {
		t.Fatalf("list status = %d, body = %s", response.Code, response.Body.String())
	}
	var decoded opensplunkv1.ListSearchJobsResponse
	unmarshalResponse(t, response, &decoded)
	return &decoded
}

func searchJobListIntegrationAssertPage(
	t *testing.T,
	response *opensplunkv1.ListSearchJobsResponse,
	wantIDs []string,
	wantTotal uint64,
) {
	t.Helper()
	gotIDs := make([]string, len(response.GetSearchJobs()))
	for index, job := range response.GetSearchJobs() {
		gotIDs[index] = job.GetSearchJobId()
	}
	if !slices.Equal(gotIDs, wantIDs) {
		t.Fatalf("listed IDs = %v, want %v", gotIDs, wantIDs)
	}
	page := response.GetPage()
	if page == nil || page.TotalSize == nil || page.GetTotalSize() != wantTotal || !page.GetTotalSizeExact() {
		t.Fatalf("page metadata = %+v, want exact total %d", page, wantTotal)
	}
}

func searchJobListIntegrationGet(t *testing.T, handler http.Handler, id string) *opensplunkv1.SearchJob {
	t.Helper()
	response := postProto(t, handler, "/api/v1/search/jobs/get", &opensplunkv1.GetSearchJobRequest{
		SearchJobId: id,
	})
	if response.Code != http.StatusOK {
		t.Fatalf("GET %q status = %d, body = %s", id, response.Code, response.Body.String())
	}
	var decoded opensplunkv1.GetSearchJobResponse
	unmarshalResponse(t, response, &decoded)
	if decoded.GetSearchJob() == nil {
		t.Fatalf("GET %q returned no job", id)
	}
	return decoded.GetSearchJob()
}

func searchJobListIntegrationWaitForState(
	t *testing.T,
	handler http.Handler,
	id string,
	want opensplunkv1.SearchJobState,
) *opensplunkv1.SearchJob {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		job := searchJobListIntegrationGet(t, handler, id)
		if job.GetState() == want {
			return job
		}
		if job.GetState() == opensplunkv1.SearchJobState_SEARCH_JOB_STATE_FAILED ||
			job.GetState() == opensplunkv1.SearchJobState_SEARCH_JOB_STATE_CANCELED ||
			job.GetState() == opensplunkv1.SearchJobState_SEARCH_JOB_STATE_EXPIRED {
			t.Fatalf("GET %q reached state %v while waiting for %v", id, job.GetState(), want)
		}
		if time.Now().After(deadline) {
			t.Fatalf("GET %q state = %v after timeout, want %v", id, job.GetState(), want)
		}
		time.Sleep(time.Millisecond)
	}
}
