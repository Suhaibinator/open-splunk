package server

import (
	"context"
	"errors"
	"fmt"
	"math"
	"net/http"
	"slices"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/Suhaibinator/SRouter/pkg/codec"
	sroutercommon "github.com/Suhaibinator/SRouter/pkg/common"
	"github.com/Suhaibinator/SRouter/pkg/router"
	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/protostrict"
	"github.com/Suhaibinator/open-splunk/internal/searchhistory"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	maximumHistorySearchJobIDBytes   = 256
	maximumHistoryAppIDBytes         = 255
	maximumHistorySavedSearchIDBytes = 128
	maximumHistoryFilterTextBytes    = 1024
	maximumHistoryPageTokenBytes     = 4 << 10
	maximumHistoryEntryBytes         = 512 << 10
	maximumHistoryRowsPerResponse    = uint32(15)
	maximumHistoryListResponseBytes  = 8 << 20

	// Clearing history is the only bulk-destructive browser operation in this
	// API family. Requiring an exact, non-localized phrase makes an omitted,
	// default, or accidentally copied field fail closed.
	clearSearchHistoryConfirmation = "CLEAR SEARCH HISTORY"
)

func (handler *apiHandler) searchHistoryRoutes(noAuth router.AuthLevel, smallRequestBytes int64) []protobufRouteDefinition {
	return []protobufRouteDefinition{
		newForwardCompatibleProtoRoute[*opensplunk.GetSearchHistoryEntryRequest, *opensplunk.GetSearchHistoryEntryResponse](router.RouteConfig[*opensplunk.GetSearchHistoryEntryRequest, *opensplunk.GetSearchHistoryEntryResponse]{
			Path: "/search/history/get", Methods: []router.HttpMethod{router.MethodPost}, AuthLevel: &noAuth,
			Codec: codec.NewProtoCodec[*opensplunk.GetSearchHistoryEntryRequest, *opensplunk.GetSearchHistoryEntryResponse](), Handler: handler.getSearchHistoryEntry,
			SourceType: router.Body, Overrides: sroutercommon.RouteOverrides{MaxBodySize: smallRequestBytes},
		}),
		newForwardCompatibleProtoRoute[*opensplunk.ListSearchHistoryRequest, *serializedSearchHistoryListResponse](router.RouteConfig[*opensplunk.ListSearchHistoryRequest, *serializedSearchHistoryListResponse]{
			Path: "/search/history/list", Methods: []router.HttpMethod{router.MethodPost}, AuthLevel: &noAuth,
			Codec: newSerializedSearchHistoryListCodec(), Handler: handler.listSearchHistory,
			SourceType: router.Body, Overrides: sroutercommon.RouteOverrides{MaxBodySize: smallRequestBytes},
		}),
		newForwardCompatibleProtoRoute[*opensplunk.DeleteSearchHistoryEntryRequest, *opensplunk.DeleteSearchHistoryEntryResponse](router.RouteConfig[*opensplunk.DeleteSearchHistoryEntryRequest, *opensplunk.DeleteSearchHistoryEntryResponse]{
			Path: "/search/history/delete", Methods: []router.HttpMethod{router.MethodPost}, AuthLevel: &noAuth,
			Codec: codec.NewProtoCodec[*opensplunk.DeleteSearchHistoryEntryRequest, *opensplunk.DeleteSearchHistoryEntryResponse](), Handler: handler.deleteSearchHistoryEntry,
			SourceType: router.Body, Overrides: sroutercommon.RouteOverrides{MaxBodySize: smallRequestBytes},
		}),
		newForwardCompatibleProtoRoute[*opensplunk.ClearSearchHistoryRequest, *opensplunk.ClearSearchHistoryResponse](router.RouteConfig[*opensplunk.ClearSearchHistoryRequest, *opensplunk.ClearSearchHistoryResponse]{
			Path: "/search/history/clear", Methods: []router.HttpMethod{router.MethodPost}, AuthLevel: &noAuth,
			Codec: codec.NewProtoCodec[*opensplunk.ClearSearchHistoryRequest, *opensplunk.ClearSearchHistoryResponse](), Handler: handler.clearSearchHistory,
			SourceType: router.Body, Overrides: sroutercommon.RouteOverrides{MaxBodySize: smallRequestBytes},
		}),
	}
}

func (handler *apiHandler) getSearchHistoryEntry(request *http.Request, input *opensplunk.GetSearchHistoryEntryRequest) (*opensplunk.GetSearchHistoryEntryResponse, error) {
	if err := validateHistoryRequest(input); err != nil {
		return nil, badRequestError(err.Error())
	}
	searchJobID, err := historySearchJobID(input.GetSearchJobId())
	if err != nil {
		return nil, badRequestError(err.Error())
	}
	entry, err := handler.searchHistory.Get(request.Context(), handler.searchHistoryScope(), searchJobID)
	if err := mapSearchHistoryCallError(request.Context(), err); err != nil {
		return nil, err
	}
	converted, err := cloneSearchHistoryEntry(entry)
	if err != nil || converted.GetSearchJobId() != searchJobID {
		return nil, internalError()
	}
	if err := searchHistoryRequestContextError(request.Context()); err != nil {
		return nil, err
	}
	response := &opensplunk.GetSearchHistoryEntryResponse{HistoryEntry: converted}
	if proto.Size(response) > maximumHistoryListResponseBytes {
		return nil, internalError()
	}
	return response, nil
}

func (handler *apiHandler) listSearchHistory(request *http.Request, input *opensplunk.ListSearchHistoryRequest) (*serializedSearchHistoryListResponse, error) {
	if err := validateHistoryRequest(input); err != nil {
		return nil, badRequestError(err.Error())
	}
	pageSize, pageToken, includeTotal, err := handler.historyPageRequest(input.GetPage())
	if err != nil {
		return nil, badRequestError(err.Error())
	}
	filter, err := historyFilter(input.GetFilter())
	if err != nil {
		return nil, badRequestError(err.Error())
	}
	if err := validateHistorySort(input.GetSortBy(), input.GetSortDirection()); err != nil {
		return nil, badRequestError(err.Error())
	}

	release, acquired := handler.acquireSerialization()
	if !acquired {
		return nil, unavailableError("search history response capacity is exhausted")
	}
	transferred := false
	defer func() {
		if !transferred {
			release()
		}
	}()

	result, err := handler.searchHistory.List(request.Context(), handler.searchHistoryScope(), searchhistory.ListRequest{
		PageSize:            pageSize,
		PageToken:           pageToken,
		IncludeTotal:        includeTotal,
		AppIDFilter:         historyCloneStringPointer(filter.AppID),
		StateFilters:        slices.Clone(filter.StateFilters),
		TextFilter:          historyCloneStringPointer(filter.Text),
		SavedSearchIDFilter: historyCloneStringPointer(filter.SavedSearchID),
		CreatedAfter:        cloneTimePointer(filter.CreatedAfter),
		CreatedBefore:       cloneTimePointer(filter.CreatedBefore),
		SortBy:              input.GetSortBy(),
		SortDirection:       input.GetSortDirection(),
	})
	if err := mapSearchHistoryCallError(request.Context(), err); err != nil {
		return nil, err
	}
	// #nosec G115 -- a slice length is non-negative and exactly representable as uint64.
	if uint64(len(result.Entries)) > uint64(effectiveHistoryPageSize(pageSize, handler.maximumPageSize)) {
		return nil, internalError()
	}

	entries := make([]*opensplunk.SearchHistoryEntry, len(result.Entries))
	for index, entry := range result.Entries {
		if err := searchHistoryRequestContextError(request.Context()); err != nil {
			return nil, err
		}
		entries[index], err = cloneSearchHistoryEntry(entry)
		if err != nil || !historyEntryMatchesFilter(entries[index], filter) {
			return nil, internalError()
		}
	}
	if !historyEntriesOrdered(entries, input.GetSortBy(), input.GetSortDirection()) {
		return nil, internalError()
	}

	page, err := historyPageResponse(result, includeTotal)
	if err != nil {
		return nil, internalError()
	}
	message := &opensplunk.ListSearchHistoryResponse{HistoryEntries: entries, Page: page}
	if proto.Size(message) > maximumHistoryListResponseBytes {
		return nil, internalError()
	}
	if err := searchHistoryRequestContextError(request.Context()); err != nil {
		return nil, err
	}
	transferred = true
	return &serializedSearchHistoryListResponse{message: message, ctx: request.Context(), release: release}, nil
}

func (handler *apiHandler) deleteSearchHistoryEntry(request *http.Request, input *opensplunk.DeleteSearchHistoryEntryRequest) (*opensplunk.DeleteSearchHistoryEntryResponse, error) {
	if err := validateHistoryRequest(input); err != nil {
		return nil, badRequestError(err.Error())
	}
	searchJobID, err := historySearchJobID(input.GetSearchJobId())
	if err != nil {
		return nil, badRequestError(err.Error())
	}
	if err := handler.searchHistory.Delete(request.Context(), handler.searchHistoryScope(), searchJobID); err != nil {
		return nil, mapSearchHistoryCallError(request.Context(), err)
	}
	return &opensplunk.DeleteSearchHistoryEntryResponse{SearchJobId: searchJobID}, nil
}

func (handler *apiHandler) clearSearchHistory(request *http.Request, input *opensplunk.ClearSearchHistoryRequest) (*opensplunk.ClearSearchHistoryResponse, error) {
	if err := validateHistoryRequest(input); err != nil {
		return nil, badRequestError(err.Error())
	}
	if input.GetConfirmation() != clearSearchHistoryConfirmation {
		return nil, badRequestError(fmt.Sprintf("confirmation must be exactly %q", clearSearchHistoryConfirmation))
	}
	filter, err := historyFilter(input.GetFilter())
	if err != nil {
		return nil, badRequestError(err.Error())
	}
	deleted, err := handler.searchHistory.Clear(request.Context(), handler.searchHistoryScope(), filter)
	if err := mapSearchHistoryCallError(request.Context(), err); err != nil {
		return nil, err
	}
	return &opensplunk.ClearSearchHistoryResponse{DeletedCount: deleted}, nil
}

func (handler *apiHandler) searchHistoryScope() searchhistory.AccessScope {
	return searchhistory.AccessScope{TenantID: handler.tenantID, OwnerID: handler.ownerID}
}

func (handler *apiHandler) historyPageRequest(page *opensplunk.PageRequest) (uint32, string, bool, error) {
	pageSize, pageToken, includeTotal, err := handler.pageRequest(page)
	if err != nil {
		return 0, "", false, err
	}
	if pageSize == 0 {
		pageSize = int(min(maximumHistoryRowsPerResponse, handler.maximumPageSize))
	}
	pageSize = min(pageSize, int(maximumHistoryRowsPerResponse))
	// #nosec G115 -- pageSize is non-negative and capped at 15 immediately above.
	return uint32(pageSize), pageToken, includeTotal, nil
}

func effectiveHistoryPageSize(requested, configuredMaximum uint32) uint32 {
	if requested != 0 {
		return min(requested, maximumHistoryRowsPerResponse)
	}
	return min(configuredMaximum, maximumHistoryRowsPerResponse)
}

func historyFilter(input *opensplunk.SearchHistoryFilter) (searchhistory.Filter, error) {
	if input == nil {
		return searchhistory.Filter{}, nil
	}
	appID, err := optionalBoundedString(input.AppId, maximumHistoryAppIDBytes, "app ID filter")
	if err != nil {
		return searchhistory.Filter{}, err
	}
	text, err := optionalBoundedString(input.Text, maximumHistoryFilterTextBytes, "text filter")
	if err != nil {
		return searchhistory.Filter{}, err
	}
	if text != nil && *text == "" {
		text = nil
	}
	var savedSearchID *string
	if input.SavedSearchId != nil {
		value := strings.TrimSpace(input.GetSavedSearchId())
		if err := validateBoundedIdentifier(value, maximumHistorySavedSearchIDBytes, false); err != nil {
			return searchhistory.Filter{}, errors.New("saved-search ID filter is invalid")
		}
		savedSearchID = &value
	}
	states, err := historyStateFilters(input.GetStateFilters())
	if err != nil {
		return searchhistory.Filter{}, err
	}
	after, err := historyFilterTime("created_after", input.GetCreatedAfter())
	if err != nil {
		return searchhistory.Filter{}, err
	}
	before, err := historyFilterTime("created_before", input.GetCreatedBefore())
	if err != nil {
		return searchhistory.Filter{}, err
	}
	if after != nil && before != nil && !after.Before(*before) {
		return searchhistory.Filter{}, errors.New("created_after must precede created_before")
	}
	return searchhistory.Filter{
		AppID: appID, StateFilters: states, Text: text, SavedSearchID: savedSearchID,
		CreatedAfter: after, CreatedBefore: before,
	}, nil
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

func historyFilterTime(name string, timestamp *timestamppb.Timestamp) (*time.Time, error) {
	if timestamp == nil {
		return nil, nil
	}
	if timestamp.CheckValid() != nil {
		return nil, fmt.Errorf("%s is outside the supported timestamp range", name)
	}
	// SQLite history predicates are stored at microsecond precision. Normalize
	// here so validation and fake implementations observe the same boundaries.
	value := time.UnixMicro(timestamp.AsTime().UnixMicro()).UTC()
	return &value, nil
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

func historySearchJobID(input string) (string, error) {
	value := strings.TrimSpace(input)
	if err := validateBoundedIdentifier(value, maximumHistorySearchJobIDBytes, false); err != nil {
		return "", errors.New("search job ID is invalid")
	}
	return value, nil
}

func cloneSearchHistoryEntry(input *opensplunk.SearchHistoryEntry) (*opensplunk.SearchHistoryEntry, error) {
	if input == nil {
		return nil, errors.New("search history service returned an invalid entry")
	}
	knowledgeSnapshot, err := projectKnowledgeSnapshotSummary(input.GetKnowledgeSnapshot())
	if err != nil {
		return nil, err
	}
	encodedSize := proto.Size(input)
	if encodedSize == 0 || encodedSize > maximumHistoryEntryBytes {
		return nil, errors.New("search history service returned an invalid entry")
	}
	if err := protostrict.RejectUnknownFields(input.ProtoReflect(), "request"); err != nil {
		return nil, err
	}
	if id, err := historySearchJobID(input.GetSearchJobId()); err != nil || id != input.GetSearchJobId() {
		return nil, errors.New("search history service returned an invalid entry")
	}
	if input.GetDefinition() == nil || strings.TrimSpace(input.GetDefinition().GetSpl()) == "" || len(input.GetDefinition().GetSpl()) > 64<<10 {
		return nil, errors.New("search history service returned an invalid definition")
	}
	if !terminalHistoryState(input.GetFinalState()) || input.GetMatchedEvents() > math.MaxInt64 {
		return nil, errors.New("search history service returned invalid terminal metadata")
	}
	if input.GetCreatedAt() == nil || input.GetCreatedAt().CheckValid() != nil || input.GetFinishedAt() == nil || input.GetFinishedAt().CheckValid() != nil || input.GetFinishedAt().AsTime().Before(input.GetCreatedAt().AsTime()) {
		return nil, errors.New("search history service returned invalid timestamps")
	}
	if input.GetStartedAt() != nil && (input.GetStartedAt().CheckValid() != nil || input.GetStartedAt().AsTime().Before(input.GetCreatedAt().AsTime()) || input.GetStartedAt().AsTime().After(input.GetFinishedAt().AsTime())) {
		return nil, errors.New("search history service returned an invalid start timestamp")
	}
	duration := input.GetDuration()
	const (
		maximumDurationSeconds = int64(math.MaxInt64) / int64(time.Second)
		maximumDurationNanos   = int32(int64(math.MaxInt64) % int64(time.Second))
	)
	if duration == nil || duration.CheckValid() != nil || duration.Seconds < 0 || duration.Nanos < 0 || duration.Seconds > maximumDurationSeconds || (duration.Seconds == maximumDurationSeconds && duration.Nanos > maximumDurationNanos) {
		return nil, errors.New("search history service returned an invalid duration")
	}
	cloned := proto.Clone(input).(*opensplunk.SearchHistoryEntry)
	cloned.KnowledgeSnapshot = knowledgeSnapshot
	return cloned, nil
}

func historyEntryMatchesFilter(entry *opensplunk.SearchHistoryEntry, filter searchhistory.Filter) bool {
	if entry == nil || entry.GetDefinition() == nil {
		return false
	}
	if filter.AppID != nil && entry.GetDefinition().GetAppId() != *filter.AppID {
		return false
	}
	if filter.Text != nil && !strings.Contains(asciiLower(entry.GetDefinition().GetSpl()), asciiLower(*filter.Text)) {
		return false
	}
	if filter.SavedSearchID != nil && entry.GetSource().GetSavedSearchId() != *filter.SavedSearchID {
		return false
	}
	if len(filter.StateFilters) != 0 && !slices.Contains(filter.StateFilters, entry.GetFinalState()) {
		return false
	}
	created := time.UnixMicro(entry.GetCreatedAt().AsTime().UnixMicro()).UTC()
	if filter.CreatedAfter != nil && !created.After(*filter.CreatedAfter) {
		return false
	}
	if filter.CreatedBefore != nil && !created.Before(*filter.CreatedBefore) {
		return false
	}
	return true
}

func asciiLower(input string) string {
	changed := false
	bytes := []byte(input)
	for index, value := range bytes {
		if value >= 'A' && value <= 'Z' {
			bytes[index] = value + ('a' - 'A')
			changed = true
		}
	}
	if !changed {
		return input
	}
	return string(bytes)
}

func historyEntriesOrdered(entries []*opensplunk.SearchHistoryEntry, sortBy opensplunk.SearchHistorySortBy, direction opensplunk.SortDirection) bool {
	if sortBy == opensplunk.SearchHistorySortBy_SEARCH_HISTORY_SORT_BY_UNSPECIFIED {
		sortBy = opensplunk.SearchHistorySortBy_SEARCH_HISTORY_SORT_BY_CREATED_AT
	}
	if direction == opensplunk.SortDirection_SORT_DIRECTION_UNSPECIFIED {
		direction = opensplunk.SortDirection_SORT_DIRECTION_DESCENDING
	}
	for index := 1; index < len(entries); index++ {
		previousKey := historyEntrySortKey(entries[index-1], sortBy)
		currentKey := historyEntrySortKey(entries[index], sortBy)
		comparison := 0
		if previousKey < currentKey {
			comparison = -1
		} else if previousKey > currentKey {
			comparison = 1
		} else {
			comparison = strings.Compare(entries[index-1].GetSearchJobId(), entries[index].GetSearchJobId())
		}
		if direction == opensplunk.SortDirection_SORT_DIRECTION_ASCENDING && comparison > 0 {
			return false
		}
		if direction == opensplunk.SortDirection_SORT_DIRECTION_DESCENDING && comparison < 0 {
			return false
		}
	}
	return true
}

func historyEntrySortKey(entry *opensplunk.SearchHistoryEntry, sortBy opensplunk.SearchHistorySortBy) int64 {
	switch sortBy {
	case opensplunk.SearchHistorySortBy_SEARCH_HISTORY_SORT_BY_FINISHED_AT:
		return entry.GetFinishedAt().AsTime().UnixMicro()
	case opensplunk.SearchHistorySortBy_SEARCH_HISTORY_SORT_BY_DURATION:
		return int64(entry.GetDuration().AsDuration())
	case opensplunk.SearchHistorySortBy_SEARCH_HISTORY_SORT_BY_MATCHED_EVENTS:
		// #nosec G115 -- cloneSearchHistoryEntry rejects matched-event counts above MaxInt64.
		return int64(entry.GetMatchedEvents())
	default:
		return entry.GetCreatedAt().AsTime().UnixMicro()
	}
}

func historyPageResponse(result searchhistory.ListResult, includeTotal bool) (*opensplunk.PageResponse, error) {
	page := &opensplunk.PageResponse{}
	if result.NextPageToken != nil {
		if len(*result.NextPageToken) == 0 || len(*result.NextPageToken) > maximumHistoryPageTokenBytes || !utf8.ValidString(*result.NextPageToken) || strings.TrimSpace(*result.NextPageToken) != *result.NextPageToken {
			return nil, errors.New("search history service returned an invalid page token")
		}
		page.NextPageToken = new(*result.NextPageToken)
	}
	if result.TotalSize != nil {
		if !includeTotal || !result.TotalSizeExact {
			return nil, errors.New("search history service returned an unexpected total")
		}
		page.TotalSize = new(*result.TotalSize)
		page.TotalSizeExact = true
	} else if result.TotalSizeExact || includeTotal {
		return nil, errors.New("search history service omitted a requested total")
	}
	return page, nil
}

func terminalHistoryState(state opensplunk.SearchJobState) bool {
	switch state {
	case opensplunk.SearchJobState_SEARCH_JOB_STATE_COMPLETED,
		opensplunk.SearchJobState_SEARCH_JOB_STATE_FAILED,
		opensplunk.SearchJobState_SEARCH_JOB_STATE_CANCELED,
		opensplunk.SearchJobState_SEARCH_JOB_STATE_EXPIRED:
		return true
	default:
		return false
	}
}

func cloneTimePointer(input *time.Time) *time.Time {
	if input == nil {
		return nil
	}
	value := *input
	return &value
}

func historyCloneStringPointer(input *string) *string {
	if input == nil {
		return nil
	}
	value := *input
	return &value
}

func mapSearchHistoryCallError(ctx context.Context, operationErr error) error {
	if operationErr == nil {
		return nil
	}
	if requestContextFailure(ctx, operationErr) != nil {
		return router.NewHTTPError(http.StatusRequestTimeout, "search history request was canceled")
	}
	switch {
	case errors.Is(operationErr, control.ErrInvalidArgument):
		return badRequestError("search history request is invalid")
	case errors.Is(operationErr, control.ErrNotFound):
		return router.NewHTTPError(http.StatusNotFound, "search history entry not found")
	default:
		return unavailableError("search history service is unavailable")
	}
}

func searchHistoryRequestContextError(ctx context.Context) error {
	return canceledRequestError(ctx, "search history request was canceled")
}

func validateHistoryRequest(input proto.Message) error {
	if input == nil {
		return errors.New("request is required")
	}
	return protostrict.RejectUnknownFields(input.ProtoReflect(), "request")
}

type serializedSearchHistoryListResponse = boundedProtoResponse[*opensplunk.ListSearchHistoryResponse]

type serializedSearchHistoryListCodec = boundedProtoCodec[*opensplunk.ListSearchHistoryRequest, *opensplunk.ListSearchHistoryResponse]

func newSerializedSearchHistoryListCodec() *serializedSearchHistoryListCodec {
	return newBoundedProtoCodec(
		codec.NewProtoCodec[*opensplunk.ListSearchHistoryRequest, *opensplunk.ListSearchHistoryResponse](),
		boundedProtoCodecOptions{
			stateError:   "search history list serialization state is invalid",
			messageError: "search history list serialization state is invalid",
			contextError: searchHistoryRequestContextError,
			maximumBytes: maximumHistoryListResponseBytes,
			sizeError:    "search history list response exceeds the transport limit",
		},
	)
}
