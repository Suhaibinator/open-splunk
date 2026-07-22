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
	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/searchhistory"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
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

func (handler *apiHandler) searchHistoryRoutes(noAuth router.AuthLevel, smallRequestBytes int64) []router.RouteDefinition {
	return []router.RouteDefinition{
		router.NewGenericRouteDefinition[*opensplunkv1.GetSearchHistoryEntryRequest, *opensplunkv1.GetSearchHistoryEntryResponse, string, struct{}](router.RouteConfig[*opensplunkv1.GetSearchHistoryEntryRequest, *opensplunkv1.GetSearchHistoryEntryResponse]{
			Path: "/search/history/get", Methods: []router.HttpMethod{router.MethodPost}, AuthLevel: &noAuth,
			Codec: codec.NewProtoCodec[*opensplunkv1.GetSearchHistoryEntryRequest, *opensplunkv1.GetSearchHistoryEntryResponse](), Handler: handler.getSearchHistoryEntry,
			SourceType: router.Body, Sanitizer: identitySanitizer[*opensplunkv1.GetSearchHistoryEntryRequest], Overrides: sroutercommon.RouteOverrides{MaxBodySize: smallRequestBytes},
		}),
		router.NewGenericRouteDefinition[*opensplunkv1.ListSearchHistoryRequest, *serializedSearchHistoryListResponse, string, struct{}](router.RouteConfig[*opensplunkv1.ListSearchHistoryRequest, *serializedSearchHistoryListResponse]{
			Path: "/search/history/list", Methods: []router.HttpMethod{router.MethodPost}, AuthLevel: &noAuth,
			Codec: newSerializedSearchHistoryListCodec(), Handler: handler.listSearchHistory,
			SourceType: router.Body, Sanitizer: identitySanitizer[*opensplunkv1.ListSearchHistoryRequest], Overrides: sroutercommon.RouteOverrides{MaxBodySize: smallRequestBytes},
		}),
		router.NewGenericRouteDefinition[*opensplunkv1.DeleteSearchHistoryEntryRequest, *opensplunkv1.DeleteSearchHistoryEntryResponse, string, struct{}](router.RouteConfig[*opensplunkv1.DeleteSearchHistoryEntryRequest, *opensplunkv1.DeleteSearchHistoryEntryResponse]{
			Path: "/search/history/delete", Methods: []router.HttpMethod{router.MethodPost}, AuthLevel: &noAuth,
			Codec: codec.NewProtoCodec[*opensplunkv1.DeleteSearchHistoryEntryRequest, *opensplunkv1.DeleteSearchHistoryEntryResponse](), Handler: handler.deleteSearchHistoryEntry,
			SourceType: router.Body, Sanitizer: identitySanitizer[*opensplunkv1.DeleteSearchHistoryEntryRequest], Overrides: sroutercommon.RouteOverrides{MaxBodySize: smallRequestBytes},
		}),
		router.NewGenericRouteDefinition[*opensplunkv1.ClearSearchHistoryRequest, *opensplunkv1.ClearSearchHistoryResponse, string, struct{}](router.RouteConfig[*opensplunkv1.ClearSearchHistoryRequest, *opensplunkv1.ClearSearchHistoryResponse]{
			Path: "/search/history/clear", Methods: []router.HttpMethod{router.MethodPost}, AuthLevel: &noAuth,
			Codec: codec.NewProtoCodec[*opensplunkv1.ClearSearchHistoryRequest, *opensplunkv1.ClearSearchHistoryResponse](), Handler: handler.clearSearchHistory,
			SourceType: router.Body, Sanitizer: identitySanitizer[*opensplunkv1.ClearSearchHistoryRequest], Overrides: sroutercommon.RouteOverrides{MaxBodySize: smallRequestBytes},
		}),
	}
}

func (handler *apiHandler) getSearchHistoryEntry(request *http.Request, input *opensplunkv1.GetSearchHistoryEntryRequest) (*opensplunkv1.GetSearchHistoryEntryResponse, error) {
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
	response := &opensplunkv1.GetSearchHistoryEntryResponse{HistoryEntry: converted}
	if proto.Size(response) > maximumHistoryListResponseBytes {
		return nil, internalError()
	}
	return response, nil
}

func (handler *apiHandler) listSearchHistory(request *http.Request, input *opensplunkv1.ListSearchHistoryRequest) (*serializedSearchHistoryListResponse, error) {
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
	if uint32(len(result.Entries)) > effectiveHistoryPageSize(pageSize, handler.maximumPageSize) {
		return nil, internalError()
	}

	entries := make([]*opensplunkv1.SearchHistoryEntry, len(result.Entries))
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
	message := &opensplunkv1.ListSearchHistoryResponse{HistoryEntries: entries, Page: page}
	if proto.Size(message) > maximumHistoryListResponseBytes {
		return nil, internalError()
	}
	if err := searchHistoryRequestContextError(request.Context()); err != nil {
		return nil, err
	}
	transferred = true
	return &serializedSearchHistoryListResponse{message: message, ctx: request.Context(), release: release}, nil
}

func (handler *apiHandler) deleteSearchHistoryEntry(request *http.Request, input *opensplunkv1.DeleteSearchHistoryEntryRequest) (*opensplunkv1.DeleteSearchHistoryEntryResponse, error) {
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
	if err := searchHistoryRequestContextError(request.Context()); err != nil {
		return nil, err
	}
	return &opensplunkv1.DeleteSearchHistoryEntryResponse{SearchJobId: searchJobID}, nil
}

func (handler *apiHandler) clearSearchHistory(request *http.Request, input *opensplunkv1.ClearSearchHistoryRequest) (*opensplunkv1.ClearSearchHistoryResponse, error) {
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
	if err := searchHistoryRequestContextError(request.Context()); err != nil {
		return nil, err
	}
	return &opensplunkv1.ClearSearchHistoryResponse{DeletedCount: deleted}, nil
}

func (handler *apiHandler) searchHistoryScope() searchhistory.AccessScope {
	return searchhistory.AccessScope{TenantID: handler.tenantID, OwnerID: handler.ownerID}
}

func (handler *apiHandler) historyPageRequest(page *opensplunkv1.PageRequest) (uint32, string, bool, error) {
	pageSize, pageToken, includeTotal, err := handler.pageRequest(page)
	if err != nil {
		return 0, "", false, err
	}
	if pageSize == 0 {
		pageSize = int(min(maximumHistoryRowsPerResponse, handler.maximumPageSize))
	}
	pageSize = min(pageSize, int(maximumHistoryRowsPerResponse))
	return uint32(pageSize), pageToken, includeTotal, nil
}

func effectiveHistoryPageSize(requested, configuredMaximum uint32) uint32 {
	if requested != 0 {
		return min(requested, maximumHistoryRowsPerResponse)
	}
	return min(configuredMaximum, maximumHistoryRowsPerResponse)
}

func historyFilter(input *opensplunkv1.SearchHistoryFilter) (searchhistory.Filter, error) {
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

func historyStateFilters(input []opensplunkv1.SearchJobState) ([]opensplunkv1.SearchJobState, error) {
	if len(input) > 4 {
		return nil, errors.New("state filters cannot contain more than four values")
	}
	result := make([]opensplunkv1.SearchJobState, 0, len(input))
	seen := make(map[opensplunkv1.SearchJobState]struct{}, len(input))
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

func validateHistorySort(sortBy opensplunkv1.SearchHistorySortBy, direction opensplunkv1.SortDirection) error {
	switch sortBy {
	case opensplunkv1.SearchHistorySortBy_SEARCH_HISTORY_SORT_BY_UNSPECIFIED,
		opensplunkv1.SearchHistorySortBy_SEARCH_HISTORY_SORT_BY_CREATED_AT,
		opensplunkv1.SearchHistorySortBy_SEARCH_HISTORY_SORT_BY_FINISHED_AT,
		opensplunkv1.SearchHistorySortBy_SEARCH_HISTORY_SORT_BY_DURATION,
		opensplunkv1.SearchHistorySortBy_SEARCH_HISTORY_SORT_BY_MATCHED_EVENTS:
	default:
		return errors.New("search-history sort field is invalid")
	}
	switch direction {
	case opensplunkv1.SortDirection_SORT_DIRECTION_UNSPECIFIED,
		opensplunkv1.SortDirection_SORT_DIRECTION_ASCENDING,
		opensplunkv1.SortDirection_SORT_DIRECTION_DESCENDING:
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

func cloneSearchHistoryEntry(input *opensplunkv1.SearchHistoryEntry) (*opensplunkv1.SearchHistoryEntry, error) {
	if input == nil {
		return nil, errors.New("search history service returned an invalid entry")
	}
	encodedSize := proto.Size(input)
	if encodedSize == 0 || encodedSize > maximumHistoryEntryBytes {
		return nil, errors.New("search history service returned an invalid entry")
	}
	if err := rejectHistoryUnknownFields(input.ProtoReflect()); err != nil {
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
	return proto.Clone(input).(*opensplunkv1.SearchHistoryEntry), nil
}

func historyEntryMatchesFilter(entry *opensplunkv1.SearchHistoryEntry, filter searchhistory.Filter) bool {
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

func historyEntriesOrdered(entries []*opensplunkv1.SearchHistoryEntry, sortBy opensplunkv1.SearchHistorySortBy, direction opensplunkv1.SortDirection) bool {
	if sortBy == opensplunkv1.SearchHistorySortBy_SEARCH_HISTORY_SORT_BY_UNSPECIFIED {
		sortBy = opensplunkv1.SearchHistorySortBy_SEARCH_HISTORY_SORT_BY_CREATED_AT
	}
	if direction == opensplunkv1.SortDirection_SORT_DIRECTION_UNSPECIFIED {
		direction = opensplunkv1.SortDirection_SORT_DIRECTION_DESCENDING
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
		if direction == opensplunkv1.SortDirection_SORT_DIRECTION_ASCENDING && comparison > 0 {
			return false
		}
		if direction == opensplunkv1.SortDirection_SORT_DIRECTION_DESCENDING && comparison < 0 {
			return false
		}
	}
	return true
}

func historyEntrySortKey(entry *opensplunkv1.SearchHistoryEntry, sortBy opensplunkv1.SearchHistorySortBy) int64 {
	switch sortBy {
	case opensplunkv1.SearchHistorySortBy_SEARCH_HISTORY_SORT_BY_FINISHED_AT:
		return entry.GetFinishedAt().AsTime().UnixMicro()
	case opensplunkv1.SearchHistorySortBy_SEARCH_HISTORY_SORT_BY_DURATION:
		return int64(entry.GetDuration().AsDuration())
	case opensplunkv1.SearchHistorySortBy_SEARCH_HISTORY_SORT_BY_MATCHED_EVENTS:
		return int64(entry.GetMatchedEvents())
	default:
		return entry.GetCreatedAt().AsTime().UnixMicro()
	}
}

func historyPageResponse(result searchhistory.ListResult, includeTotal bool) (*opensplunkv1.PageResponse, error) {
	page := &opensplunkv1.PageResponse{}
	if result.NextPageToken != nil {
		if len(*result.NextPageToken) == 0 || len(*result.NextPageToken) > maximumHistoryPageTokenBytes || !utf8.ValidString(*result.NextPageToken) || strings.TrimSpace(*result.NextPageToken) != *result.NextPageToken {
			return nil, errors.New("search history service returned an invalid page token")
		}
		page.NextPageToken = stringPointer(*result.NextPageToken)
	}
	if result.TotalSize != nil {
		if !includeTotal || !result.TotalSizeExact {
			return nil, errors.New("search history service returned an unexpected total")
		}
		page.TotalSize = uint64Pointer(*result.TotalSize)
		page.TotalSizeExact = true
	} else if result.TotalSizeExact || includeTotal {
		return nil, errors.New("search history service omitted a requested total")
	}
	return page, nil
}

func terminalHistoryState(state opensplunkv1.SearchJobState) bool {
	switch state {
	case opensplunkv1.SearchJobState_SEARCH_JOB_STATE_COMPLETED,
		opensplunkv1.SearchJobState_SEARCH_JOB_STATE_FAILED,
		opensplunkv1.SearchJobState_SEARCH_JOB_STATE_CANCELED,
		opensplunkv1.SearchJobState_SEARCH_JOB_STATE_EXPIRED:
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
	if ctx != nil && ctx.Err() != nil {
		return router.NewHTTPError(http.StatusRequestTimeout, "search history request was canceled")
	}
	return nil
}

func validateHistoryRequest(input proto.Message) error {
	if input == nil {
		return errors.New("request is required")
	}
	return rejectHistoryUnknownFields(input.ProtoReflect())
}

func rejectHistoryUnknownFields(message protoreflect.Message) error {
	if len(message.GetUnknown()) != 0 {
		return errors.New("request contains unknown protobuf fields")
	}
	var visitErr error
	message.Range(func(field protoreflect.FieldDescriptor, value protoreflect.Value) bool {
		if field.IsMap() {
			if field.MapValue().Kind() != protoreflect.MessageKind {
				return true
			}
			value.Map().Range(func(_ protoreflect.MapKey, mapValue protoreflect.Value) bool {
				visitErr = rejectHistoryUnknownFields(mapValue.Message())
				return visitErr == nil
			})
			return visitErr == nil
		}
		if field.IsList() {
			if field.Kind() != protoreflect.MessageKind {
				return true
			}
			list := value.List()
			for index := 0; index < list.Len(); index++ {
				if visitErr = rejectHistoryUnknownFields(list.Get(index).Message()); visitErr != nil {
					return false
				}
			}
			return true
		}
		if field.Kind() == protoreflect.MessageKind {
			visitErr = rejectHistoryUnknownFields(value.Message())
			return visitErr == nil
		}
		return true
	})
	return visitErr
}

type serializedSearchHistoryListResponse struct {
	message *opensplunkv1.ListSearchHistoryResponse
	ctx     context.Context
	release func()
}

type serializedSearchHistoryListCodec struct {
	inner codec.Codec[*opensplunkv1.ListSearchHistoryRequest, *opensplunkv1.ListSearchHistoryResponse]
}

func newSerializedSearchHistoryListCodec() *serializedSearchHistoryListCodec {
	return &serializedSearchHistoryListCodec{inner: codec.NewProtoCodec[*opensplunkv1.ListSearchHistoryRequest, *opensplunkv1.ListSearchHistoryResponse]()}
}

func (codec *serializedSearchHistoryListCodec) NewRequest() *opensplunkv1.ListSearchHistoryRequest {
	return codec.inner.NewRequest()
}

func (codec *serializedSearchHistoryListCodec) Decode(request *http.Request) (*opensplunkv1.ListSearchHistoryRequest, error) {
	return codec.inner.Decode(request)
}

func (codec *serializedSearchHistoryListCodec) DecodeBytes(data []byte) (*opensplunkv1.ListSearchHistoryRequest, error) {
	return codec.inner.DecodeBytes(data)
}

func (codec *serializedSearchHistoryListCodec) Encode(response http.ResponseWriter, result *serializedSearchHistoryListResponse) error {
	if result == nil || result.message == nil || result.release == nil {
		return errors.New("search history list serialization state is invalid")
	}
	defer result.release()
	if err := searchHistoryRequestContextError(result.ctx); err != nil {
		return err
	}
	payload, err := proto.Marshal(result.message)
	if err != nil {
		return err
	}
	if len(payload) > maximumHistoryListResponseBytes {
		return errors.New("search history list response exceeds the transport limit")
	}
	if err := searchHistoryRequestContextError(result.ctx); err != nil {
		return err
	}
	response.Header().Set("Content-Type", "application/x-protobuf")
	_, err = response.Write(payload)
	return err
}
