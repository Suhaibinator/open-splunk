package server

import (
	"context"
	"errors"
	"slices"
	"strings"

	"fortio.org/safecast"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
)

// sanitizeGetSystemBootstrapRequest trims the preferred app ID so the handler
// matches it against the app catalog exactly as the catalog stores it.
func sanitizeGetSystemBootstrapRequest(
	_ context.Context,
	request *opensplunk.GetSystemBootstrapRequest,
) (*opensplunk.GetSystemBootstrapRequest, error) {
	if request.PreferredAppId != nil {
		request.PreferredAppId = new(strings.TrimSpace(request.GetPreferredAppId()))
	}
	return request, nil
}

// sanitizeValidateSearchRequest rejects the presentation metadata that belongs
// to the browser's own state, not to a server-side analysis request. The rest
// of the definition is resolved against the current server time, so it stays
// with the handler.
func sanitizeValidateSearchRequest(
	_ context.Context,
	request *opensplunk.ValidateSearchRequest,
) (*opensplunk.ValidateSearchRequest, error) {
	if err := rejectUnsupportedSearchDefinitionFields(request.GetDefinition()); err != nil {
		return request, badRequestError(err.Error())
	}
	return request, nil
}

// sanitizeCreateSearchJobRequest rejects every unsupported request option for
// all three creation origins (ad hoc, saved-search launch, and history rerun)
// before any of them reaches storage.
func sanitizeCreateSearchJobRequest(
	_ context.Context,
	request *opensplunk.CreateSearchJobRequest,
) (*opensplunk.CreateSearchJobRequest, error) {
	if request.ClientRequestId != nil {
		return request, badRequestError("client request idempotency is not supported")
	}
	options := request.GetOptions()
	if options.GetEnableFieldDiscovery() || options.GetEnableTimeline() {
		return request, badRequestError(
			"eager field discovery and timeline options are not supported; request those analyses through their dedicated APIs",
		)
	}
	if err := rejectUnsupportedSearchDefinitionFields(request.GetDefinition()); err != nil {
		return request, badRequestError(err.Error())
	}
	return request, nil
}

func rejectUnsupportedSearchDefinitionFields(definition *opensplunk.SearchDefinition) error {
	if definition.GetPreferredResultTab() != opensplunk.SearchResultTab_SEARCH_RESULT_TAB_UNSPECIFIED || len(definition.GetSelectedFields()) != 0 || definition.GetVisualization() != nil {
		return errors.New("search presentation metadata is not supported")
	}
	return nil
}

func sanitizeGetSearchJobRequest(
	_ context.Context,
	request *opensplunk.GetSearchJobRequest,
) (*opensplunk.GetSearchJobRequest, error) {
	request.SearchJobId = strings.TrimSpace(request.GetSearchJobId())
	if request.GetSearchJobId() == "" {
		return request, badRequestError("search job ID is required")
	}
	if request.GetIncludePlan() || request.GetIncludeGeneratedSql() {
		return request, badRequestError("plan inspection is not supported by this API version")
	}
	return request, nil
}

// sanitizeListSearchJobsRequest resolves the page envelope against the
// configured maximum page size and normalises the list filters in place: state
// filters are deduplicated and sorted, and the optional string filters are
// trimmed and bounded. An empty text filter is dropped so the handler never
// builds a matcher that accepts everything. Page errors precede filter errors.
func (handler *apiHandler) sanitizeListSearchJobsRequest(
	_ context.Context,
	request *opensplunk.ListSearchJobsRequest,
) (*opensplunk.ListSearchJobsRequest, error) {
	pageSize, pageToken, includeTotal, err := handler.searchJobListPageRequest(
		request.GetPage(),
	)
	if err != nil {
		return request, badRequestError(err.Error())
	}
	request.Page = resolvedListPage(
		request.GetPage(),
		safecast.MustConv[uint32](pageSize),
		pageToken,
		includeTotal,
	)
	states, err := searchJobListStateFilters(request.GetStateFilters())
	if err != nil {
		return request, badRequestError(err.Error())
	}
	request.StateFilters = states
	appIDFilter, err := optionalBoundedString(
		request.AppIdFilter,
		maximumSavedSearchAppIDBytes,
		"app ID filter",
	)
	if err != nil {
		return request, badRequestError(err.Error())
	}
	request.AppIdFilter = appIDFilter
	textFilter, err := optionalBoundedString(
		request.TextFilter,
		maximumSearchJobListFilterTextBytes,
		"text filter",
	)
	if err != nil {
		return request, badRequestError(err.Error())
	}
	if textFilter != nil && *textFilter == "" {
		textFilter = nil
	}
	request.TextFilter = textFilter
	return request, nil
}

func searchJobListStateFilters(input []opensplunk.SearchJobState) ([]opensplunk.SearchJobState, error) {
	if len(input) > maximumSearchJobListStateFilters {
		return nil, errors.New("state filters cannot contain more than 16 values")
	}
	result := make([]opensplunk.SearchJobState, 0, len(input))
	seen := make(map[opensplunk.SearchJobState]struct{}, len(input))
	for _, state := range input {
		if _, managerState := searchJobListManagerState(state); !managerState && state != opensplunk.SearchJobState_SEARCH_JOB_STATE_INTERRUPTED {
			return nil, errors.New("state filter is invalid or unsupported")
		}
		if _, exists := seen[state]; exists {
			continue
		}
		seen[state] = struct{}{}
		result = append(result, state)
	}
	slices.Sort(result)
	return result, nil
}

// sanitizeGetSearchResultsRequest leaves the page size as the caller sent it —
// zero still means "the service default" on this route — and only bounds it and
// canonicalises the page token.
func (handler *apiHandler) sanitizeGetSearchResultsRequest(
	_ context.Context,
	request *opensplunk.GetSearchResultsRequest,
) (*opensplunk.GetSearchResultsRequest, error) {
	request.SearchJobId = strings.TrimSpace(request.GetSearchJobId())
	if request.GetSearchJobId() == "" {
		return request, badRequestError("search job ID is required")
	}
	if request.GetAllowPartialResults() {
		return request, badRequestError("partial search results are not supported")
	}
	if len(request.GetColumns()) != 0 {
		return request, badRequestError("result column projection is not supported")
	}
	pageSize, pageToken, includeTotal, err := handler.pageRequest(request.GetPage())
	if err != nil {
		return request, badRequestError(err.Error())
	}
	// An omitted page stays omitted here: this route reads a zero page size as
	// the service default, so there is nothing to write back.
	if request.Page != nil {
		resolvedListPage(
			request.Page,
			safecast.MustConv[uint32](pageSize),
			pageToken,
			includeTotal,
		)
	}
	return request, nil
}

// sanitizeCancelSearchJobRequest drops a whitespace-only reason rather than
// rejecting it, so the handler sees either no reason at all or a 400.
func sanitizeCancelSearchJobRequest(
	_ context.Context,
	request *opensplunk.CancelSearchJobRequest,
) (*opensplunk.CancelSearchJobRequest, error) {
	request.SearchJobId = strings.TrimSpace(request.GetSearchJobId())
	if request.GetSearchJobId() == "" {
		return request, badRequestError("search job ID is required")
	}
	if strings.TrimSpace(request.GetReason()) != "" {
		return request, badRequestError("cancellation reasons are not supported")
	}
	request.Reason = nil
	return request, nil
}
