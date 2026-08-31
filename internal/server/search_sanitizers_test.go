package server

import (
	"errors"
	"net/http"
	"slices"
	"strings"
	"testing"

	"github.com/Suhaibinator/SRouter/pkg/router"
	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
)

// sanitizerTestHandler supplies the only handler state the page-bounding
// sanitizers read.
func sanitizerTestHandler() *apiHandler {
	return &apiHandler{maximumPageSize: 100}
}

// assertSanitizedBadRequest asserts the exact 400 message a route sanitizer
// promises, so the HTTP suites keep specifying the wire contract.
func assertSanitizedBadRequest(t *testing.T, err error, message string) {
	t.Helper()
	var httpErr *router.HTTPError
	if !errors.As(err, &httpErr) {
		t.Fatalf("error = %T %v, want *router.HTTPError", err, err)
	}
	if httpErr.StatusCode != http.StatusBadRequest || httpErr.Message != message {
		t.Fatalf(
			"error = %d %q, want %d %q",
			httpErr.StatusCode,
			httpErr.Message,
			http.StatusBadRequest,
			message,
		)
	}
}

func TestSanitizeGetSystemBootstrapRequestTrimsPreferredAppID(t *testing.T) {
	t.Parallel()

	for name, test := range map[string]struct {
		requested *string
		want      *string
	}{
		"absent":     {requested: nil, want: nil},
		"padded":     {requested: new("  app-main  "), want: new("app-main")},
		"whitespace": {requested: new("   "), want: new("")},
		"canonical":  {requested: new("app-main"), want: new("app-main")},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			request := &opensplunk.GetSystemBootstrapRequest{PreferredAppId: test.requested}
			got, err := sanitizeGetSystemBootstrapRequest(t.Context(), request)
			if err != nil {
				t.Fatalf("sanitize error = %v", err)
			}
			if got != request {
				t.Fatal("sanitizer returned a different request")
			}
			if (got.PreferredAppId == nil) != (test.want == nil) {
				t.Fatalf("preferred app ID = %v, want %v", got.PreferredAppId, test.want)
			}
			if test.want != nil && got.GetPreferredAppId() != *test.want {
				t.Fatalf("preferred app ID = %q, want %q", got.GetPreferredAppId(), *test.want)
			}
		})
	}
}

func TestSanitizeValidateSearchRequestRejectsPresentationMetadata(t *testing.T) {
	t.Parallel()

	for name, definition := range map[string]*opensplunk.SearchDefinition{
		"preferred result tab": {
			Spl:                "index=main",
			PreferredResultTab: opensplunk.SearchResultTab_SEARCH_RESULT_TAB_EVENTS,
		},
		"selected fields": {Spl: "index=main", SelectedFields: []string{"host"}},
		"visualization": {
			Spl:           "index=main",
			Visualization: &opensplunk.VisualizationSpec{},
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			_, err := sanitizeValidateSearchRequest(
				t.Context(),
				&opensplunk.ValidateSearchRequest{Definition: definition},
			)
			assertSanitizedBadRequest(t, err, "search presentation metadata is not supported")
		})
	}
}

func TestSanitizeValidateSearchRequestAcceptsAnalysisDefinition(t *testing.T) {
	t.Parallel()

	request := &opensplunk.ValidateSearchRequest{
		Definition: &opensplunk.SearchDefinition{Spl: "index=main"},
	}
	got, err := sanitizeValidateSearchRequest(t.Context(), request)
	if err != nil || got != request {
		t.Fatalf("sanitize = %v, %v", got, err)
	}
}

func TestSanitizeCreateSearchJobRequestRejectsUnsupportedOptions(t *testing.T) {
	t.Parallel()

	for name, test := range map[string]struct {
		request     *opensplunk.CreateSearchJobRequest
		wantMessage string
	}{
		"client request ID": {
			request:     &opensplunk.CreateSearchJobRequest{ClientRequestId: new("client-1")},
			wantMessage: "client request idempotency is not supported",
		},
		"empty client request ID": {
			request:     &opensplunk.CreateSearchJobRequest{ClientRequestId: new("")},
			wantMessage: "client request idempotency is not supported",
		},
		"eager field discovery": {
			request: &opensplunk.CreateSearchJobRequest{
				Options: &opensplunk.SearchJobOptions{EnableFieldDiscovery: true},
			},
			wantMessage: "eager field discovery and timeline options are not supported; request those analyses through their dedicated APIs",
		},
		"eager timeline": {
			request: &opensplunk.CreateSearchJobRequest{
				Options: &opensplunk.SearchJobOptions{EnableTimeline: true},
			},
			wantMessage: "eager field discovery and timeline options are not supported; request those analyses through their dedicated APIs",
		},
		"presentation metadata": {
			request: &opensplunk.CreateSearchJobRequest{
				Definition: &opensplunk.SearchDefinition{
					Spl:            "index=main",
					SelectedFields: []string{"host"},
				},
			},
			wantMessage: "search presentation metadata is not supported",
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			_, err := sanitizeCreateSearchJobRequest(t.Context(), test.request)
			assertSanitizedBadRequest(t, err, test.wantMessage)
		})
	}
}

func TestSanitizeCreateSearchJobRequestAcceptsEveryOrigin(t *testing.T) {
	t.Parallel()

	for name, request := range map[string]*opensplunk.CreateSearchJobRequest{
		"ad hoc": {
			Definition: &opensplunk.SearchDefinition{Spl: "index=main"},
		},
		"saved search launch": {
			Source: &opensplunk.SearchJobSource{
				Origin:        opensplunk.SearchJobOrigin_SEARCH_JOB_ORIGIN_SAVED_SEARCH,
				SavedSearchId: new("saved-1"),
			},
		},
		"history rerun": {
			Source: &opensplunk.SearchJobSource{
				Origin:          opensplunk.SearchJobOrigin_SEARCH_JOB_ORIGIN_HISTORY_RERUN,
				HistorySearchId: new("history-1"),
			},
		},
		"empty options message": {
			Options: &opensplunk.SearchJobOptions{},
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			got, err := sanitizeCreateSearchJobRequest(t.Context(), request)
			if err != nil || got != request {
				t.Fatalf("sanitize = %v, %v", got, err)
			}
		})
	}
}

func TestSanitizeGetSearchJobRequest(t *testing.T) {
	t.Parallel()

	for name, test := range map[string]struct {
		request     *opensplunk.GetSearchJobRequest
		wantMessage string
		wantID      string
	}{
		"missing ID": {
			request:     &opensplunk.GetSearchJobRequest{},
			wantMessage: "search job ID is required",
		},
		"whitespace ID": {
			request:     &opensplunk.GetSearchJobRequest{SearchJobId: " \t "},
			wantMessage: "search job ID is required",
		},
		"plan inspection": {
			request: &opensplunk.GetSearchJobRequest{
				SearchJobId: "job-1",
				IncludePlan: true,
			},
			wantMessage: "plan inspection is not supported by this API version",
		},
		"generated SQL": {
			request: &opensplunk.GetSearchJobRequest{
				SearchJobId:         "job-1",
				IncludeGeneratedSql: true,
			},
			wantMessage: "plan inspection is not supported by this API version",
		},
		"padded ID": {
			request: &opensplunk.GetSearchJobRequest{SearchJobId: "  job-1  "},
			wantID:  "job-1",
		},
		"canonical ID without inspection flags": {
			request: &opensplunk.GetSearchJobRequest{SearchJobId: "job-1"},
			wantID:  "job-1",
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			got, err := sanitizeGetSearchJobRequest(t.Context(), test.request)
			if test.wantMessage != "" {
				assertSanitizedBadRequest(t, err, test.wantMessage)
				return
			}
			if err != nil {
				t.Fatalf("sanitize error = %v", err)
			}
			if got.GetSearchJobId() != test.wantID {
				t.Fatalf("search job ID = %q, want %q", got.GetSearchJobId(), test.wantID)
			}
		})
	}
}

func TestSanitizeListSearchJobsRequestNormalizesFilters(t *testing.T) {
	t.Parallel()

	request := &opensplunk.ListSearchJobsRequest{
		StateFilters: []opensplunk.SearchJobState{
			opensplunk.SearchJobState_SEARCH_JOB_STATE_COMPLETED,
			opensplunk.SearchJobState_SEARCH_JOB_STATE_QUEUED,
			opensplunk.SearchJobState_SEARCH_JOB_STATE_COMPLETED,
		},
		AppIdFilter: new("  app-main  "),
		TextFilter:  new("  errors  "),
	}
	got, err := sanitizerTestHandler().sanitizeListSearchJobsRequest(t.Context(), request)
	if err != nil {
		t.Fatalf("sanitize error = %v", err)
	}
	if !slices.Equal(got.GetStateFilters(), []opensplunk.SearchJobState{
		opensplunk.SearchJobState_SEARCH_JOB_STATE_QUEUED,
		opensplunk.SearchJobState_SEARCH_JOB_STATE_COMPLETED,
	}) {
		t.Fatalf("state filters = %v", got.GetStateFilters())
	}
	if got.GetAppIdFilter() != "app-main" || got.GetTextFilter() != "errors" {
		t.Fatalf("filters = %q %q", got.GetAppIdFilter(), got.GetTextFilter())
	}
}

func TestSanitizeListSearchJobsRequestDropsEmptyTextFilter(t *testing.T) {
	t.Parallel()

	request := &opensplunk.ListSearchJobsRequest{TextFilter: new("   ")}
	got, err := sanitizerTestHandler().sanitizeListSearchJobsRequest(t.Context(), request)
	if err != nil {
		t.Fatalf("sanitize error = %v", err)
	}
	if got.TextFilter != nil {
		t.Fatalf("text filter = %q, want nil", got.GetTextFilter())
	}
}

func TestSanitizeListSearchJobsRequestRejectsInvalidFilters(t *testing.T) {
	t.Parallel()

	tooManyStates := make(
		[]opensplunk.SearchJobState,
		maximumSearchJobListStateFilters+1,
	)
	for index := range tooManyStates {
		tooManyStates[index] = opensplunk.SearchJobState_SEARCH_JOB_STATE_QUEUED
	}
	for name, test := range map[string]struct {
		request     *opensplunk.ListSearchJobsRequest
		wantMessage string
	}{
		"too many state filters": {
			request:     &opensplunk.ListSearchJobsRequest{StateFilters: tooManyStates},
			wantMessage: "state filters cannot contain more than 16 values",
		},
		"unsupported state filter": {
			request: &opensplunk.ListSearchJobsRequest{
				StateFilters: []opensplunk.SearchJobState{
					opensplunk.SearchJobState_SEARCH_JOB_STATE_UNSPECIFIED,
				},
			},
			wantMessage: "state filter is invalid or unsupported",
		},
		"oversized app ID filter": {
			request: &opensplunk.ListSearchJobsRequest{
				AppIdFilter: new(strings.Repeat("a", maximumSavedSearchAppIDBytes+1)),
			},
			wantMessage: "app ID filter is invalid",
		},
		"control character in app ID filter": {
			request:     &opensplunk.ListSearchJobsRequest{AppIdFilter: new("app\x00main")},
			wantMessage: "app ID filter is invalid",
		},
		"oversized text filter": {
			request: &opensplunk.ListSearchJobsRequest{
				TextFilter: new(strings.Repeat("t", maximumSearchJobListFilterTextBytes+1)),
			},
			wantMessage: "text filter is invalid",
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			_, err := sanitizerTestHandler().sanitizeListSearchJobsRequest(t.Context(), test.request)
			assertSanitizedBadRequest(t, err, test.wantMessage)
		})
	}
}

func TestSanitizeGetSearchResultsRequest(t *testing.T) {
	t.Parallel()

	for name, test := range map[string]struct {
		request     *opensplunk.GetSearchResultsRequest
		wantMessage string
		wantID      string
	}{
		"missing ID": {
			request:     &opensplunk.GetSearchResultsRequest{},
			wantMessage: "search job ID is required",
		},
		"partial results": {
			request: &opensplunk.GetSearchResultsRequest{
				SearchJobId:         "job-1",
				AllowPartialResults: true,
			},
			wantMessage: "partial search results are not supported",
		},
		"column projection": {
			request: &opensplunk.GetSearchResultsRequest{
				SearchJobId: "job-1",
				Columns:     []string{"host"},
			},
			wantMessage: "result column projection is not supported",
		},
		"padded ID": {
			request: &opensplunk.GetSearchResultsRequest{SearchJobId: " job-1 "},
			wantID:  "job-1",
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			got, err := sanitizerTestHandler().sanitizeGetSearchResultsRequest(t.Context(), test.request)
			if test.wantMessage != "" {
				assertSanitizedBadRequest(t, err, test.wantMessage)
				return
			}
			if err != nil {
				t.Fatalf("sanitize error = %v", err)
			}
			if got.GetSearchJobId() != test.wantID {
				t.Fatalf("search job ID = %q, want %q", got.GetSearchJobId(), test.wantID)
			}
		})
	}
}

func TestSanitizeCancelSearchJobRequest(t *testing.T) {
	t.Parallel()

	for name, test := range map[string]struct {
		request     *opensplunk.CancelSearchJobRequest
		wantMessage string
		wantID      string
	}{
		"missing ID": {
			request:     &opensplunk.CancelSearchJobRequest{},
			wantMessage: "search job ID is required",
		},
		"cancellation reason": {
			request: &opensplunk.CancelSearchJobRequest{
				SearchJobId: "job-1",
				Reason:      new("user abandoned the search"),
			},
			wantMessage: "cancellation reasons are not supported",
		},
		"padded ID": {
			request: &opensplunk.CancelSearchJobRequest{SearchJobId: " job-1 "},
			wantID:  "job-1",
		},
		"whitespace reason": {
			request: &opensplunk.CancelSearchJobRequest{
				SearchJobId: "job-1",
				Reason:      new("  "),
			},
			wantID: "job-1",
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			got, err := sanitizeCancelSearchJobRequest(t.Context(), test.request)
			if test.wantMessage != "" {
				assertSanitizedBadRequest(t, err, test.wantMessage)
				return
			}
			if err != nil {
				t.Fatalf("sanitize error = %v", err)
			}
			if got.GetSearchJobId() != test.wantID || got.Reason != nil {
				t.Fatalf("request = %q %v", got.GetSearchJobId(), got.Reason)
			}
		})
	}
}

func TestSanitizeListSearchJobsRequestResolvesPaging(t *testing.T) {
	t.Parallel()

	for name, test := range map[string]struct {
		page         *opensplunk.PageRequest
		wantSize     uint32
		wantToken    string
		wantTotal    bool
		wantHasToken bool
	}{
		"absent page defaults to the endpoint maximum": {
			page:     nil,
			wantSize: maximumSearchJobListRows,
		},
		"requested size below the endpoint maximum is kept": {
			page:     &opensplunk.PageRequest{PageSize: new(uint32(5))},
			wantSize: 5,
		},
		"requested size above the endpoint maximum is clamped": {
			page:     &opensplunk.PageRequest{PageSize: new(uint32(50))},
			wantSize: maximumSearchJobListRows,
		},
		"include total and token are carried through": {
			page: &opensplunk.PageRequest{
				PageSize:         new(uint32(3)),
				PageToken:        new("signed-token"),
				IncludeTotalSize: true,
			},
			wantSize:     3,
			wantToken:    "signed-token",
			wantTotal:    true,
			wantHasToken: true,
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			request := &opensplunk.ListSearchJobsRequest{Page: test.page}
			got, err := sanitizerTestHandler().sanitizeListSearchJobsRequest(t.Context(), request)
			if err != nil {
				t.Fatalf("sanitize error = %v", err)
			}
			if got != request {
				t.Fatal("sanitizer returned a different request")
			}
			if got.GetPage().GetPageSize() != test.wantSize ||
				got.GetPage().GetIncludeTotalSize() != test.wantTotal ||
				got.GetPage().GetPageToken() != test.wantToken {
				t.Fatalf("page = %+v", got.GetPage())
			}
			if (got.GetPage().PageToken != nil) != test.wantHasToken {
				t.Fatalf("page token presence = %v", got.GetPage().PageToken)
			}
		})
	}
}

func TestSanitizeListSearchJobsRequestRejectsPageBeforeFilters(t *testing.T) {
	t.Parallel()

	for name, test := range map[string]struct {
		request     *opensplunk.ListSearchJobsRequest
		wantMessage string
	}{
		"explicit zero page size": {
			request: &opensplunk.ListSearchJobsRequest{
				Page: &opensplunk.PageRequest{PageSize: new(uint32(0))},
			},
			wantMessage: "page size must be positive when supplied",
		},
		"page size above the configured maximum": {
			request: &opensplunk.ListSearchJobsRequest{
				Page: &opensplunk.PageRequest{PageSize: new(uint32(101))},
			},
			wantMessage: "page size exceeds the maximum of 100",
		},
		"untrimmed page token": {
			request: &opensplunk.ListSearchJobsRequest{
				Page: &opensplunk.PageRequest{PageToken: new(" signed-token ")},
			},
			wantMessage: "page token is invalid",
		},
		// A doubly invalid request must report the page, matching the order the
		// handler used before the sanitizer owned either check.
		"page error precedes state filter error": {
			request: &opensplunk.ListSearchJobsRequest{
				Page: &opensplunk.PageRequest{PageSize: new(uint32(0))},
				StateFilters: []opensplunk.SearchJobState{
					opensplunk.SearchJobState_SEARCH_JOB_STATE_UNSPECIFIED,
				},
			},
			wantMessage: "page size must be positive when supplied",
		},
		"page error precedes app ID filter error": {
			request: &opensplunk.ListSearchJobsRequest{
				Page:        &opensplunk.PageRequest{PageSize: new(uint32(0))},
				AppIdFilter: new(strings.Repeat("a", maximumSavedSearchAppIDBytes+1)),
			},
			wantMessage: "page size must be positive when supplied",
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			_, err := sanitizerTestHandler().sanitizeListSearchJobsRequest(t.Context(), test.request)
			assertSanitizedBadRequest(t, err, test.wantMessage)
		})
	}
}

func TestSanitizeGetSearchResultsRequestBoundsPaging(t *testing.T) {
	t.Parallel()

	for name, test := range map[string]struct {
		page        *opensplunk.PageRequest
		wantMessage string
		wantToken   string
	}{
		"explicit zero page size": {
			page:        &opensplunk.PageRequest{PageSize: new(uint32(0))},
			wantMessage: "page size must be positive when supplied",
		},
		"page size above the configured maximum": {
			page:        &opensplunk.PageRequest{PageSize: new(uint32(101))},
			wantMessage: "page size exceeds the maximum of 100",
		},
		"oversized page token": {
			page:        &opensplunk.PageRequest{PageToken: new(strings.Repeat("t", (4<<10)+1))},
			wantMessage: "page token is too large",
		},
		"padded page token is trimmed": {
			page:      &opensplunk.PageRequest{PageToken: new("  signed-token  ")},
			wantToken: "signed-token",
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			request := &opensplunk.GetSearchResultsRequest{
				SearchJobId: "job-1",
				Page:        test.page,
			}
			got, err := sanitizerTestHandler().sanitizeGetSearchResultsRequest(
				t.Context(),
				request,
			)
			if test.wantMessage != "" {
				assertSanitizedBadRequest(t, err, test.wantMessage)
				return
			}
			if err != nil {
				t.Fatalf("sanitize error = %v", err)
			}
			if got.GetPage().GetPageToken() != test.wantToken {
				t.Fatalf("page token = %q, want %q", got.GetPage().GetPageToken(), test.wantToken)
			}
		})
	}
}

// TestSanitizeGetSearchResultsRequestKeepsUnsetPageSize pins that an unset page
// size stays unset on this route: zero still means "the service default" here,
// unlike the list routes that resolve a concrete size.
func TestSanitizeGetSearchResultsRequestKeepsUnsetPageSize(t *testing.T) {
	t.Parallel()

	request := &opensplunk.GetSearchResultsRequest{SearchJobId: "job-1"}
	got, err := sanitizerTestHandler().sanitizeGetSearchResultsRequest(
		t.Context(),
		request,
	)
	if err != nil {
		t.Fatalf("sanitize error = %v", err)
	}
	if got.GetPage().GetPageSize() != 0 {
		t.Fatalf("page size = %d, want 0", got.GetPage().GetPageSize())
	}
}
