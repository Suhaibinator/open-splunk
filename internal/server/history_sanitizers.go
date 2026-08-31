package server

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"fortio.org/safecast"
	"google.golang.org/protobuf/types/known/timestamppb"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
)

func sanitizeGetSearchHistoryEntryRequest(
	ctx context.Context,
	request *opensplunk.GetSearchHistoryEntryRequest,
) (*opensplunk.GetSearchHistoryEntryRequest, error) {
	request, err := discardUnknownProtoFields(ctx, request)
	if err != nil {
		return request, err
	}
	searchJobID, err := historySearchJobID(request.GetSearchJobId())
	if err != nil {
		return request, badRequestError(err.Error())
	}
	request.SearchJobId = searchJobID
	return request, nil
}

// sanitizeListSearchHistoryRequest leaves the handler an explicit page and a
// filter whose bounds, ordering, and instant precision are already settled: it
// clamps the page size to the transport row cap, deduplicates and sorts the
// state filters, and truncates both instants to the stored precision.
func (handler *apiHandler) sanitizeListSearchHistoryRequest(
	ctx context.Context,
	request *opensplunk.ListSearchHistoryRequest,
) (*opensplunk.ListSearchHistoryRequest, error) {
	request, err := discardUnknownProtoFields(ctx, request)
	if err != nil {
		return request, err
	}
	if err := handler.sanitizeSearchHistoryPage(request); err != nil {
		return request, badRequestError(err.Error())
	}
	if err := sanitizeSearchHistoryFilter(request.GetFilter()); err != nil {
		return request, badRequestError(err.Error())
	}
	if err := validateHistorySort(request.GetSortBy(), request.GetSortDirection()); err != nil {
		return request, badRequestError(err.Error())
	}
	return request, nil
}

func sanitizeDeleteSearchHistoryEntryRequest(
	ctx context.Context,
	request *opensplunk.DeleteSearchHistoryEntryRequest,
) (*opensplunk.DeleteSearchHistoryEntryRequest, error) {
	request, err := discardUnknownProtoFields(ctx, request)
	if err != nil {
		return request, err
	}
	searchJobID, err := historySearchJobID(request.GetSearchJobId())
	if err != nil {
		return request, badRequestError(err.Error())
	}
	request.SearchJobId = searchJobID
	return request, nil
}

func sanitizeClearSearchHistoryRequest(
	ctx context.Context,
	request *opensplunk.ClearSearchHistoryRequest,
) (*opensplunk.ClearSearchHistoryRequest, error) {
	request, err := discardUnknownProtoFields(ctx, request)
	if err != nil {
		return request, err
	}
	if request.GetConfirmation() != clearSearchHistoryConfirmation {
		return request, badRequestError(fmt.Sprintf("confirmation must be exactly %q", clearSearchHistoryConfirmation))
	}
	if err := sanitizeSearchHistoryFilter(request.GetFilter()); err != nil {
		return request, badRequestError(err.Error())
	}
	return request, nil
}

// sanitizeSearchHistoryPage gives the request a page whose size is always
// present, positive, and no larger than the transport row cap, so the handler
// never has to re-derive an effective page size.
func (handler *apiHandler) sanitizeSearchHistoryPage(request *opensplunk.ListSearchHistoryRequest) error {
	pageSize, pageToken, _, err := handler.pageRequest(request.GetPage())
	if err != nil {
		return err
	}
	if pageSize == 0 {
		pageSize = int(min(maximumHistoryRowsPerResponse, handler.maximumPageSize))
	}
	pageSize = min(pageSize, int(maximumHistoryRowsPerResponse))

	page := request.GetPage()
	if page == nil {
		page = &opensplunk.PageRequest{}
		request.Page = page
	}
	page.PageSize = new(safecast.MustConv[uint32](pageSize))
	page.PageToken = nil
	if pageToken != "" {
		page.PageToken = &pageToken
	}
	return nil
}

// sanitizeSearchHistoryFilter normalizes the optional filter in place: bounded
// identifiers lose surrounding whitespace, an empty text filter becomes absent,
// state filters are deduplicated and sorted, and both instants are truncated to
// the microsecond precision the history predicates are stored at.
func sanitizeSearchHistoryFilter(filter *opensplunk.SearchHistoryFilter) error {
	if filter == nil {
		return nil
	}
	appID, err := optionalBoundedString(filter.AppId, maximumHistoryAppIDBytes, "app ID filter")
	if err != nil {
		return err
	}
	filter.AppId = appID
	text, err := optionalBoundedString(filter.Text, maximumHistoryFilterTextBytes, "text filter")
	if err != nil {
		return err
	}
	if text != nil && *text == "" {
		text = nil
	}
	filter.Text = text
	if filter.SavedSearchId != nil {
		savedSearchID := strings.TrimSpace(filter.GetSavedSearchId())
		if err := validateBoundedIdentifier(savedSearchID, maximumHistorySavedSearchIDBytes, false); err != nil {
			return errors.New("saved-search ID filter is invalid")
		}
		filter.SavedSearchId = &savedSearchID
	}
	states, err := historyStateFilters(filter.GetStateFilters())
	if err != nil {
		return err
	}
	filter.StateFilters = states
	after, err := sanitizeHistoryFilterTime("created_after", filter.GetCreatedAfter())
	if err != nil {
		return err
	}
	filter.CreatedAfter = after
	before, err := sanitizeHistoryFilterTime("created_before", filter.GetCreatedBefore())
	if err != nil {
		return err
	}
	filter.CreatedBefore = before
	if after != nil && before != nil && !after.AsTime().Before(before.AsTime()) {
		return errors.New("created_after must precede created_before")
	}
	return nil
}

func historyStateFilters(input []opensplunk.SearchJobState) ([]opensplunk.SearchJobState, error) {
	if len(input) > 4 {
		return nil, errors.New("state filters cannot contain more than four values")
	}
	result := make([]opensplunk.SearchJobState, 0, len(input))
	seen := make(map[opensplunk.SearchJobState]struct{}, len(input))
	for _, state := range input {
		if !terminalHistoryState(state) {
			return nil, errors.New("state filter must be terminal")
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

func sanitizeHistoryFilterTime(name string, timestamp *timestamppb.Timestamp) (*timestamppb.Timestamp, error) {
	if timestamp == nil {
		return nil, nil
	}
	if timestamp.CheckValid() != nil {
		return nil, fmt.Errorf("%s is outside the supported timestamp range", name)
	}
	// SQLite history predicates are stored at microsecond precision. Normalize
	// here so validation and fake implementations observe the same boundaries.
	return timestamppb.New(time.UnixMicro(timestamp.AsTime().UnixMicro()).UTC()), nil
}

func validateHistorySort(sortBy opensplunk.SearchHistorySortBy, direction opensplunk.SortDirection) error {
	switch sortBy {
	case opensplunk.SearchHistorySortBy_SEARCH_HISTORY_SORT_BY_UNSPECIFIED,
		opensplunk.SearchHistorySortBy_SEARCH_HISTORY_SORT_BY_CREATED_AT,
		opensplunk.SearchHistorySortBy_SEARCH_HISTORY_SORT_BY_FINISHED_AT,
		opensplunk.SearchHistorySortBy_SEARCH_HISTORY_SORT_BY_DURATION,
		opensplunk.SearchHistorySortBy_SEARCH_HISTORY_SORT_BY_MATCHED_EVENTS:
	default:
		return errors.New("search-history sort field is invalid")
	}
	switch direction {
	case opensplunk.SortDirection_SORT_DIRECTION_UNSPECIFIED,
		opensplunk.SortDirection_SORT_DIRECTION_ASCENDING,
		opensplunk.SortDirection_SORT_DIRECTION_DESCENDING:
		return nil
	default:
		return errors.New("sort direction is invalid")
	}
}
