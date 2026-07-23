package server

import (
	"bytes"
	"context"
	"errors"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
	"time"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

func TestSearchJobListRouteRoundTripScopeFiltersAndSafeProjection(t *testing.T) {
	total := uint64(8)
	failed := listSearchJob("job-b", testNow)
	failed.State = searchjobs.StateFailed
	failed.SPL = "index=main ERROR failed"
	failed.NormalizedSPL = "index=main error failed"
	failed.AppID = "app-main"
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
	response := postProto(t, handler, searchJobsListPath, &opensplunkv1.ListSearchJobsRequest{
		Page: &opensplunkv1.PageRequest{
			PageSize:         &pageSize,
			PageToken:        &pageToken,
			IncludeTotalSize: true,
		},
		StateFilters: []opensplunkv1.SearchJobState{
			opensplunkv1.SearchJobState_SEARCH_JOB_STATE_FAILED,
			opensplunkv1.SearchJobState_SEARCH_JOB_STATE_COMPLETED,
			opensplunkv1.SearchJobState_SEARCH_JOB_STATE_FAILED,
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

	var decoded opensplunkv1.ListSearchJobsResponse
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
		first.GetState() != opensplunkv1.SearchJobState_SEARCH_JOB_STATE_FAILED ||
		first.GetFailure() == nil || first.GetFailure().GetMessage() != failed.Failure.Message {
		t.Fatalf("safe projection = %+v", first)
	}
	if first.GetPlan() != nil || first.GetResultSchema() != nil || len(first.GetDiagnostics()) != 0 {
		t.Fatalf("list projection exposed plan, schema, or diagnostics: %+v", first)
	}
}

func TestSearchJobListUsesBoundedCanonicalOptions(t *testing.T) {
	allStates := []opensplunkv1.SearchJobState{
		opensplunkv1.SearchJobState_SEARCH_JOB_STATE_EXPIRED,
		opensplunkv1.SearchJobState_SEARCH_JOB_STATE_CANCELED,
		opensplunkv1.SearchJobState_SEARCH_JOB_STATE_FAILED,
		opensplunkv1.SearchJobState_SEARCH_JOB_STATE_COMPLETED,
		opensplunkv1.SearchJobState_SEARCH_JOB_STATE_RUNNING,
		opensplunkv1.SearchJobState_SEARCH_JOB_STATE_PLANNING,
		opensplunkv1.SearchJobState_SEARCH_JOB_STATE_PARSING,
		opensplunkv1.SearchJobState_SEARCH_JOB_STATE_QUEUED,
		opensplunkv1.SearchJobState_SEARCH_JOB_STATE_RUNNING,
	}
	emptyApp, whitespaceText := " \t ", " \n "
	jobs := &fakeSearchJobs{}
	handler := newSearchJobListTestHandler(t, jobs, Config{MaximumPageSize: 7})
	response := postProto(t, handler, searchJobsListPath, &opensplunkv1.ListSearchJobsRequest{
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
	response = postProto(t, handler, searchJobsListPath, &opensplunkv1.ListSearchJobsRequest{
		Page: &opensplunkv1.PageRequest{PageSize: &requested},
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

func TestSearchJobListRejectsInvalidOptionsBeforeService(t *testing.T) {
	zero := uint32(0)
	tooManyStates := make([]opensplunkv1.SearchJobState, maximumSearchJobListStateFilters+1)
	for index := range tooManyStates {
		tooManyStates[index] = opensplunkv1.SearchJobState_SEARCH_JOB_STATE_QUEUED
	}
	oversizedApp := strings.Repeat("a", maximumSavedSearchAppIDBytes+1)
	controlApp := "bad\napp"
	oversizedText := strings.Repeat("x", maximumSearchJobListFilterTextBytes+1)
	oversizedToken := strings.Repeat("t", maximumSearchJobListPageTokenBytes+1)
	paddedToken := " signed-token "
	unknownField := protowire.AppendVarint(protowire.AppendTag(nil, 99, protowire.VarintType), 1)
	topLevelUnknown := &opensplunkv1.ListSearchJobsRequest{}
	topLevelUnknown.ProtoReflect().SetUnknown(unknownField)
	nestedPageUnknown := &opensplunkv1.ListSearchJobsRequest{Page: &opensplunkv1.PageRequest{}}
	nestedPageUnknown.Page.ProtoReflect().SetUnknown(unknownField)
	tests := []struct {
		name    string
		request *opensplunkv1.ListSearchJobsRequest
	}{
		{name: "explicit zero page size", request: &opensplunkv1.ListSearchJobsRequest{Page: &opensplunkv1.PageRequest{PageSize: &zero}}},
		{name: "oversized token", request: &opensplunkv1.ListSearchJobsRequest{Page: &opensplunkv1.PageRequest{PageToken: &oversizedToken}}},
		{name: "padded token", request: &opensplunkv1.ListSearchJobsRequest{Page: &opensplunkv1.PageRequest{PageToken: &paddedToken}}},
		{name: "too many raw states", request: &opensplunkv1.ListSearchJobsRequest{StateFilters: tooManyStates}},
		{name: "unspecified state", request: &opensplunkv1.ListSearchJobsRequest{StateFilters: []opensplunkv1.SearchJobState{
			opensplunkv1.SearchJobState_SEARCH_JOB_STATE_UNSPECIFIED,
		}}},
		{name: "finalizing state", request: &opensplunkv1.ListSearchJobsRequest{StateFilters: []opensplunkv1.SearchJobState{
			opensplunkv1.SearchJobState_SEARCH_JOB_STATE_FINALIZING,
		}}},
		{name: "unknown state", request: &opensplunkv1.ListSearchJobsRequest{StateFilters: []opensplunkv1.SearchJobState{opensplunkv1.SearchJobState(100)}}},
		{name: "oversized app", request: &opensplunkv1.ListSearchJobsRequest{AppIdFilter: &oversizedApp}},
		{name: "control app", request: &opensplunkv1.ListSearchJobsRequest{AppIdFilter: &controlApp}},
		{name: "oversized text", request: &opensplunkv1.ListSearchJobsRequest{TextFilter: &oversizedText}},
		{name: "top-level unknown field", request: topLevelUnknown},
		{name: "nested page unknown field", request: nestedPageUnknown},
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
		request *opensplunkv1.ListSearchJobsRequest
		page    func() searchjobs.JobListPage
	}{
		{
			name:    "cross owner",
			request: &opensplunkv1.ListSearchJobsRequest{},
			page: func() searchjobs.JobListPage {
				job := baseJob("job-a", testNow)
				job.OwnerID = "other"
				return listPage(job)
			},
		},
		{
			name:    "cross tenant",
			request: &opensplunkv1.ListSearchJobsRequest{},
			page: func() searchjobs.JobListPage {
				job := baseJob("job-a", testNow)
				job.TenantID = "other"
				return listPage(job)
			},
		},
		{
			name:    "failed state missing failure",
			request: &opensplunkv1.ListSearchJobsRequest{},
			page: func() searchjobs.JobListPage {
				job := baseJob("job-a", testNow)
				job.State = searchjobs.StateFailed
				return listPage(job)
			},
		},
		{
			name:    "nonfailed state carries failure",
			request: &opensplunkv1.ListSearchJobsRequest{},
			page: func() searchjobs.JobListPage {
				job := baseJob("job-a", testNow)
				job.Failure = &searchjobs.Failure{Code: searchjobs.FailureExecution, Message: "failed"}
				return listPage(job)
			},
		},
		{
			name:    "invalid failure code",
			request: &opensplunkv1.ListSearchJobsRequest{},
			page: func() searchjobs.JobListPage {
				job := baseJob("job-a", testNow)
				job.State = searchjobs.StateFailed
				job.Failure = &searchjobs.Failure{Code: searchjobs.FailureCode("unknown"), Message: "failed"}
				return listPage(job)
			},
		},
		{
			name:    "blank failure message",
			request: &opensplunkv1.ListSearchJobsRequest{},
			page: func() searchjobs.JobListPage {
				job := baseJob("job-a", testNow)
				job.State = searchjobs.StateFailed
				job.Failure = &searchjobs.Failure{Code: searchjobs.FailureExecution, Message: " \t "}
				return listPage(job)
			},
		},
		{
			name:    "invalid internal state",
			request: &opensplunkv1.ListSearchJobsRequest{},
			page: func() searchjobs.JobListPage {
				job := baseJob("job-a", testNow)
				job.State = searchjobs.StateInvalid
				return listPage(job)
			},
		},
		{
			name:    "empty ID",
			request: &opensplunkv1.ListSearchJobsRequest{},
			page: func() searchjobs.JobListPage {
				return listPage(baseJob("", testNow))
			},
		},
		{
			name:    "duplicate ID",
			request: &opensplunkv1.ListSearchJobsRequest{},
			page: func() searchjobs.JobListPage {
				first := baseJob("job-a", testNow)
				second := baseJob("job-a", testNow.Add(-time.Second))
				return searchjobs.JobListPage{Jobs: []searchjobs.JobListItem{listItem(first), listItem(second)}}
			},
		},
		{
			name:    "ascending creation time",
			request: &opensplunkv1.ListSearchJobsRequest{},
			page: func() searchjobs.JobListPage {
				first := baseJob("job-b", testNow.Add(-time.Second))
				second := baseJob("job-a", testNow)
				return searchjobs.JobListPage{Jobs: []searchjobs.JobListItem{listItem(first), listItem(second)}}
			},
		},
		{
			name:    "ascending ID tie break",
			request: &opensplunkv1.ListSearchJobsRequest{},
			page: func() searchjobs.JobListPage {
				first := baseJob("job-a", testNow)
				second := baseJob("job-b", testNow)
				return searchjobs.JobListPage{Jobs: []searchjobs.JobListItem{listItem(first), listItem(second)}}
			},
		},
		{
			name: "state filter mismatch",
			request: &opensplunkv1.ListSearchJobsRequest{StateFilters: []opensplunkv1.SearchJobState{
				opensplunkv1.SearchJobState_SEARCH_JOB_STATE_FAILED,
			}},
			page: func() searchjobs.JobListPage { return listPage(baseJob("job-a", testNow)) },
		},
		{
			name: "app filter mismatch",
			request: func() *opensplunkv1.ListSearchJobsRequest {
				app := "app-main"
				return &opensplunkv1.ListSearchJobsRequest{AppIdFilter: &app}
			}(),
			page: func() searchjobs.JobListPage { return listPage(baseJob("job-a", testNow)) },
		},
		{
			name: "text filter mismatch",
			request: func() *opensplunkv1.ListSearchJobsRequest {
				text := "needle"
				return &opensplunkv1.ListSearchJobsRequest{TextFilter: &text}
			}(),
			page: func() searchjobs.JobListPage { return listPage(baseJob("job-a", testNow)) },
		},
		{
			name: "more than requested",
			request: func() *opensplunkv1.ListSearchJobsRequest {
				size := uint32(1)
				return &opensplunkv1.ListSearchJobsRequest{Page: &opensplunkv1.PageRequest{PageSize: &size}}
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
			request: &opensplunkv1.ListSearchJobsRequest{},
			page: func() searchjobs.JobListPage {
				page := listPage(baseJob("job-a", testNow))
				page.NextPageToken = "unexpected"
				return page
			},
		},
		{
			name: "invalid token",
			request: func() *opensplunkv1.ListSearchJobsRequest {
				size := uint32(1)
				return &opensplunkv1.ListSearchJobsRequest{Page: &opensplunkv1.PageRequest{PageSize: &size}}
			}(),
			page: func() searchjobs.JobListPage {
				page := listPage(baseJob("job-a", testNow))
				page.NextPageToken = " bad "
				return page
			},
		},
		{
			name: "control byte in token",
			request: func() *opensplunkv1.ListSearchJobsRequest {
				size := uint32(1)
				return &opensplunkv1.ListSearchJobsRequest{Page: &opensplunkv1.PageRequest{PageSize: &size}}
			}(),
			page: func() searchjobs.JobListPage {
				page := listPage(baseJob("job-a", testNow))
				page.NextPageToken = "bad\x00token"
				return page
			},
		},
		{
			name:    "unexpected total",
			request: &opensplunkv1.ListSearchJobsRequest{},
			page: func() searchjobs.JobListPage {
				page := listPage(baseJob("job-a", testNow))
				page.TotalSize = uint64Pointer(1)
				page.TotalSizeExact = true
				return page
			},
		},
		{
			name:    "exact without total",
			request: &opensplunkv1.ListSearchJobsRequest{},
			page: func() searchjobs.JobListPage {
				page := listPage(baseJob("job-a", testNow))
				page.TotalSizeExact = true
				return page
			},
		},
		{
			name: "missing requested total",
			request: &opensplunkv1.ListSearchJobsRequest{Page: &opensplunkv1.PageRequest{
				IncludeTotalSize: true,
			}},
			page: func() searchjobs.JobListPage { return listPage(baseJob("job-a", testNow)) },
		},
		{
			name: "total smaller than page",
			request: &opensplunkv1.ListSearchJobsRequest{Page: &opensplunkv1.PageRequest{
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
			request: &opensplunkv1.ListSearchJobsRequest{Page: &opensplunkv1.PageRequest{
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
			request: func() *opensplunkv1.ListSearchJobsRequest {
				size := uint32(1)
				return &opensplunkv1.ListSearchJobsRequest{Page: &opensplunkv1.PageRequest{
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
			if response.Body.String() != "{\"error\":{\"message\":\"internal server error\"}}\n" {
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
		response := postProto(t, handler, searchJobsListPath, &opensplunkv1.ListSearchJobsRequest{})
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
		response := postProto(t, handler, searchJobsListPath, &opensplunkv1.ListSearchJobsRequest{})
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
			response := postProto(t, handler, searchJobsListPath, &opensplunkv1.ListSearchJobsRequest{})
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
		response := postProto(t, handler, searchJobsListPath, &opensplunkv1.ListSearchJobsRequest{})
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
	payload, err := proto.Marshal(&opensplunkv1.ListSearchJobsRequest{})
	if err != nil {
		t.Fatal(err)
	}
	serve := func() int {
		request := httptest.NewRequest(http.MethodPost, searchJobsListPath, bytes.NewReader(payload))
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
	payload, err := proto.Marshal(&opensplunkv1.ListSearchJobsRequest{})
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodPost, searchJobsListPath, bytes.NewReader(payload)).WithContext(ctx)
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

	request := httptest.NewRequest(http.MethodGet, searchJobsListPath, nil)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusMethodNotAllowed || response.Header().Get("Allow") != http.MethodPost {
		t.Fatalf("GET response = %d, Allow %q", response.Code, response.Header().Get("Allow"))
	}

	response = postProto(t, handler, searchJobsListPath+"/extra", &opensplunkv1.ListSearchJobsRequest{})
	if response.Code != http.StatusNotFound {
		t.Fatalf("suffix status = %d, body = %s", response.Code, response.Body.String())
	}
}

func TestASCIIFoldMatcher(t *testing.T) {
	tests := []struct {
		name    string
		value   string
		pattern string
		want    bool
	}{
		{name: "empty", value: "anything", pattern: "", want: true},
		{name: "ascii folded prefix", value: "INDEX=main", pattern: "index=", want: true},
		{name: "ascii folded suffix", value: "index=main ERROR", pattern: "error", want: true},
		{name: "overlap", value: "aaaaab", pattern: "aaab", want: true},
		{name: "longer", value: "short", pattern: "longer", want: false},
		{name: "missing", value: "index=main", pattern: "needle", want: false},
		{name: "non ascii exact", value: "café ERROR", pattern: "fé error", want: true},
		{name: "non ascii is not folded", value: "CAFÉ", pattern: "café", want: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			matcher := newASCIIFoldMatcher(test.pattern)
			if got := matcher.Contains(test.value); got != test.want {
				t.Fatalf("Contains(%q, %q) = %v, want %v", test.value, test.pattern, got, test.want)
			}
		})
	}
}

func TestASCIIFoldMatcherMatchesNaiveReference(t *testing.T) {
	t.Parallel()

	random := rand.New(rand.NewSource(0xc0ffee))
	alphabet := []rune("abcXYZ012 _=-|éÉλ")
	randomString := func(maximum int) string {
		length := random.Intn(maximum + 1)
		value := make([]rune, length)
		for index := range value {
			value[index] = alphabet[random.Intn(len(alphabet))]
		}
		return string(value)
	}
	for iteration := range 10_000 {
		value := randomString(96)
		pattern := randomString(16)
		matcher := newASCIIFoldMatcher(pattern)
		want := strings.Contains(serverFoldASCIIReference(value), serverFoldASCIIReference(pattern))
		if got := matcher.Contains(value); got != want {
			t.Fatalf("iteration %d: Contains(%q, %q) = %t, want %t", iteration, value, pattern, got, want)
		}
	}
}

func serverFoldASCIIReference(value string) string {
	folded := []byte(value)
	for index, character := range folded {
		if character >= 'A' && character <= 'Z' {
			folded[index] = character + ('a' - 'A')
		}
	}
	return string(folded)
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
		ID:               job.ID,
		Version:          job.Version,
		OwnerID:          job.OwnerID,
		TenantID:         job.TenantID,
		SPL:              job.SPL,
		NormalizedSPL:    job.NormalizedSPL,
		RequestedIndexes: slices.Clone(job.RequestedIndexes),
		EffectiveIndexes: slices.Clone(job.EffectiveIndexes),
		TimeRange:        job.TimeRange,
		AppID:            job.AppID,
		Source:           job.Source,
		Earliest:         job.Earliest,
		Latest:           job.Latest,
		IndexTimeCutoff:  job.IndexTimeCutoff,
		State:            job.State,
		RowCount:         job.RowCount,
		ResultBytes:      job.ResultBytes,
		ResultsTruncated: job.ResultsTruncated,
		CreatedAt:        job.CreatedAt,
		StartedAt:        job.StartedAt,
		FinishedAt:       job.FinishedAt,
		ExpiresAt:        job.ExpiresAt,
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

func listPage(jobs ...searchjobs.Job) searchjobs.JobListPage {
	items := make([]searchjobs.JobListItem, len(jobs))
	for index := range jobs {
		items[index] = listItem(jobs[index])
	}
	return searchjobs.JobListPage{Jobs: items}
}
