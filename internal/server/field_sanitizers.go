package server

import (
	"context"
	"strings"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
)

// sanitizeListSearchFieldsRequest clamps rather than rejects a page size the
// field service cannot serve: a request valid under the advertised browser-wide
// limit comes back as a shorter page with a continuation.
func (handler *apiHandler) sanitizeListSearchFieldsRequest(
	_ context.Context,
	request *opensplunk.ListSearchFieldsRequest,
) (*opensplunk.ListSearchFieldsRequest, error) {
	searchJobID, present := trimmedRequiredSearchJobID(request.GetSearchJobId())
	if !present {
		return request, badRequestError("search job ID is required")
	}
	request.SearchJobId = searchJobID
	if page := request.GetPage(); page != nil && page.PageSize != nil {
		pageSize := page.GetPageSize()
		if pageSize == 0 || pageSize > handler.maximumPageSize {
			return request, badRequestError("search field page size is outside the supported range")
		}
		// page_size is a requested maximum. A field service may enforce a
		// lower endpoint-specific maximum than the browser-wide page limit;
		// return a shorter page with a continuation instead of rejecting a
		// request that is valid under the advertised common contract.
		page.PageSize = new(min(pageSize, handler.maximumFieldPageSize))
	}
	// The name filter is a substring predicate and the page token is opaque
	// authenticated service output. Both keep their bytes exactly so no
	// mutation can become a second accepted spelling of the same request.
	return request, nil
}

func (handler *apiHandler) sanitizeGetSearchFieldSummaryRequest(
	_ context.Context,
	request *opensplunk.GetSearchFieldSummaryRequest,
) (*opensplunk.GetSearchFieldSummaryRequest, error) {
	searchJobID, present := trimmedRequiredSearchJobID(request.GetSearchJobId())
	if !present {
		return request, badRequestError("search job ID is required")
	}
	request.SearchJobId = searchJobID
	// Field names are exact, case-sensitive catalog output. In particular,
	// trimming would make an actual leading/trailing-space field unreachable
	// and could turn a malformed request into a different valid field.
	if request.GetFieldName() == "" {
		return request, badRequestError("search field name is required")
	}
	if request.MaxValues != nil {
		maximumValues := request.GetMaxValues()
		if maximumValues == 0 || maximumValues > handler.maximumFieldSummaryValues {
			return request, badRequestError("search field summary value limit is outside the supported range")
		}
	}
	return request, nil
}

// trimmedRequiredSearchJobID trims a requested search job ID and reports
// whether anything is left. Callers keep their own rejection message, because
// some endpoints name the job ID alongside the other identity they require.
func trimmedRequiredSearchJobID(searchJobID string) (string, bool) {
	trimmed := strings.TrimSpace(searchJobID)
	return trimmed, trimmed != ""
}
