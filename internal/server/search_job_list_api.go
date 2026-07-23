package server

import (
	"context"
	"errors"
	"net/http"
	"slices"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/Suhaibinator/SRouter/pkg/codec"
	"github.com/Suhaibinator/SRouter/pkg/router"
	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
	"google.golang.org/protobuf/proto"
)

const (
	maximumSearchJobListRows                = 15
	maximumSearchJobListStateFilters        = 16
	maximumSearchJobListFilterTextBytes     = 1024
	maximumSearchJobListPageTokenBytes      = 4 << 10
	maximumSearchJobListJobIDBytes          = 256
	maximumSearchJobListFailureMessageBytes = 4 << 10
	maximumSearchJobListResponseBytes       = 8 << 20
)

func (handler *apiHandler) listSearchJobs(
	request *http.Request,
	input *opensplunkv1.ListSearchJobsRequest,
) (*serializedSearchJobListResponse, error) {
	if err := validateSearchJobListRequest(input); err != nil {
		return nil, badRequestError(err.Error())
	}
	pageSize, pageToken, includeTotal, err := handler.searchJobListPageRequest(input.GetPage())
	if err != nil {
		return nil, badRequestError(err.Error())
	}
	states, err := searchJobListStateFilters(input.GetStateFilters())
	if err != nil {
		return nil, badRequestError(err.Error())
	}
	appID, err := optionalBoundedString(input.AppIdFilter, maximumSavedSearchAppIDBytes, "app ID filter")
	if err != nil {
		return nil, badRequestError(err.Error())
	}
	text, err := optionalBoundedString(input.TextFilter, maximumSearchJobListFilterTextBytes, "text filter")
	if err != nil {
		return nil, badRequestError(err.Error())
	}
	if text != nil && *text == "" {
		text = nil
	}
	var textMatcher *asciiFoldMatcher
	if text != nil {
		matcher := newASCIIFoldMatcher(*text)
		textMatcher = &matcher
	}
	if err := searchJobListRequestContextError(request.Context()); err != nil {
		return nil, err
	}

	release, acquired := handler.acquireSerialization()
	if !acquired {
		return nil, unavailableError("search job response capacity is exhausted")
	}
	transferred := false
	defer func() {
		if !transferred {
			release()
		}
	}()

	page, operationErr := handler.jobs.ListPageFor(request.Context(), handler.accessScope(), searchjobs.JobListRequest{
		PageSize:     pageSize,
		PageToken:    pageToken,
		IncludeTotal: includeTotal,
		StateFilters: slices.Clone(states),
		AppIDFilter:  cloneSearchJobListString(appID),
		TextFilter:   cloneSearchJobListString(text),
	})
	if contextErr := requestContextFailure(request.Context(), operationErr); contextErr != nil {
		return nil, router.NewHTTPError(http.StatusRequestTimeout, "search job list request was canceled")
	}
	if operationErr != nil {
		return nil, mapSearchJobError(operationErr)
	}
	if len(page.Jobs) > pageSize {
		return nil, internalError()
	}

	converted := make([]*opensplunkv1.SearchJob, len(page.Jobs))
	seenIDs := make(map[string]struct{}, len(page.Jobs))
	projectionNow := handler.now()
	var previous searchjobs.Job
	for index, item := range page.Jobs {
		if err := searchJobListRequestContextError(request.Context()); err != nil {
			return nil, err
		}
		job := searchJobListItemAsJob(item)
		if !validSearchJobListItem(job, handler.accessScope(), states, appID, textMatcher) {
			return nil, internalError()
		}
		if _, exists := seenIDs[job.ID]; exists {
			return nil, internalError()
		}
		seenIDs[job.ID] = struct{}{}
		if index > 0 && !searchJobListOrderValid(previous, job) {
			return nil, internalError()
		}
		previous = job

		projected, projectionErr := searchJobToProto(job, projectionNow)
		if projectionErr != nil {
			return nil, internalError()
		}
		// These fields are intentionally unavailable on list responses even if
		// the general SearchJob projection grows richer in a later API version.
		projected.Plan = nil
		projected.ResultSchema = nil
		projected.Diagnostics = nil
		converted[index] = projected
	}

	pageResponse, err := searchJobListPageResponse(page, pageSize, pageToken, includeTotal)
	if err != nil {
		return nil, internalError()
	}
	message := &opensplunkv1.ListSearchJobsResponse{SearchJobs: converted, Page: pageResponse}
	if proto.Size(message) > maximumSearchJobListResponseBytes {
		return nil, internalError()
	}
	if err := searchJobListRequestContextError(request.Context()); err != nil {
		return nil, err
	}
	transferred = true
	return &serializedSearchJobListResponse{
		message: message,
		ctx:     request.Context(),
		release: release,
	}, nil
}

func searchJobListItemAsJob(item searchjobs.JobListItem) searchjobs.Job {
	result := searchjobs.Job{
		ID:               item.ID,
		Version:          item.Version,
		OwnerID:          item.OwnerID,
		TenantID:         item.TenantID,
		SPL:              item.SPL,
		NormalizedSPL:    item.NormalizedSPL,
		RequestedIndexes: slices.Clone(item.RequestedIndexes),
		EffectiveIndexes: slices.Clone(item.EffectiveIndexes),
		TimeRange:        item.TimeRange,
		AppID:            item.AppID,
		Source:           item.Source,
		Earliest:         item.Earliest,
		Latest:           item.Latest,
		IndexTimeCutoff:  item.IndexTimeCutoff,
		State:            item.State,
		RowCount:         item.RowCount,
		ResultBytes:      item.ResultBytes,
		ResultsTruncated: item.ResultsTruncated,
		CreatedAt:        item.CreatedAt,
		StartedAt:        item.StartedAt,
		FinishedAt:       item.FinishedAt,
		ExpiresAt:        item.ExpiresAt,
	}
	if item.Failure != nil {
		result.Failure = &searchjobs.Failure{
			Code:      item.Failure.Code,
			Message:   item.Failure.Message,
			Retryable: item.Failure.Retryable,
		}
	}
	return result
}

func (handler *apiHandler) searchJobListPageRequest(page *opensplunkv1.PageRequest) (int, string, bool, error) {
	if page != nil && page.PageToken != nil && strings.TrimSpace(page.GetPageToken()) != page.GetPageToken() {
		return 0, "", false, errors.New("page token is invalid")
	}
	pageSize, pageToken, includeTotal, err := handler.pageRequest(page)
	if err != nil {
		return 0, "", false, err
	}
	if !validSearchJobListToken(pageToken, true) {
		return 0, "", false, errors.New("page token is invalid")
	}
	if pageSize == 0 {
		pageSize = min(maximumSearchJobListRows, int(handler.maximumPageSize))
	}
	return min(pageSize, maximumSearchJobListRows), pageToken, includeTotal, nil
}

func searchJobListStateFilters(input []opensplunkv1.SearchJobState) ([]searchjobs.State, error) {
	if len(input) > maximumSearchJobListStateFilters {
		return nil, errors.New("state filters cannot contain more than 16 values")
	}
	result := make([]searchjobs.State, 0, len(input))
	seen := make(map[searchjobs.State]struct{}, len(input))
	for _, state := range input {
		converted, ok := searchJobListState(state)
		if !ok {
			return nil, errors.New("state filter is invalid or unsupported")
		}
		if _, exists := seen[converted]; exists {
			continue
		}
		seen[converted] = struct{}{}
		result = append(result, converted)
	}
	slices.Sort(result)
	return result, nil
}

func searchJobListState(input opensplunkv1.SearchJobState) (searchjobs.State, bool) {
	switch input {
	case opensplunkv1.SearchJobState_SEARCH_JOB_STATE_QUEUED:
		return searchjobs.StateQueued, true
	case opensplunkv1.SearchJobState_SEARCH_JOB_STATE_PARSING:
		return searchjobs.StateParsing, true
	case opensplunkv1.SearchJobState_SEARCH_JOB_STATE_PLANNING:
		return searchjobs.StatePlanning, true
	case opensplunkv1.SearchJobState_SEARCH_JOB_STATE_RUNNING:
		return searchjobs.StateRunning, true
	case opensplunkv1.SearchJobState_SEARCH_JOB_STATE_COMPLETED:
		return searchjobs.StateCompleted, true
	case opensplunkv1.SearchJobState_SEARCH_JOB_STATE_FAILED:
		return searchjobs.StateFailed, true
	case opensplunkv1.SearchJobState_SEARCH_JOB_STATE_CANCELED:
		return searchjobs.StateCanceled, true
	case opensplunkv1.SearchJobState_SEARCH_JOB_STATE_EXPIRED:
		return searchjobs.StateExpired, true
	default:
		// UNSPECIFIED, FINALIZING, and values unknown to this binary all fail
		// closed because the manager implements no corresponding lifecycle.
		return searchjobs.StateInvalid, false
	}
}

func validSearchJobListItem(
	job searchjobs.Job,
	scope searchjobs.AccessScope,
	states []searchjobs.State,
	appID *string,
	text *asciiFoldMatcher,
) bool {
	if job.OwnerID != scope.OwnerID || job.TenantID != scope.TenantID ||
		job.Schema != nil || job.CreatedAt.IsZero() ||
		strings.TrimSpace(job.ID) != job.ID ||
		validateBoundedIdentifier(job.ID, maximumSearchJobListJobIDBytes, false) != nil {
		return false
	}
	if !validSearchJobListFailure(job.State, job.Failure) {
		return false
	}
	if searchStateToProto(job.State) == opensplunkv1.SearchJobState_SEARCH_JOB_STATE_UNSPECIFIED {
		return false
	}
	if len(states) != 0 && !slices.Contains(states, job.State) {
		return false
	}
	if appID != nil && job.AppID != *appID {
		return false
	}
	if text != nil && !text.Contains(job.SPL) {
		return false
	}
	return true
}

func searchJobListOrderValid(previous, current searchjobs.Job) bool {
	if previous.CreatedAt.Before(current.CreatedAt) {
		return false
	}
	if previous.CreatedAt.Equal(current.CreatedAt) && strings.Compare(previous.ID, current.ID) <= 0 {
		return false
	}
	return true
}

func searchJobListPageResponse(
	result searchjobs.JobListPage,
	pageSize int,
	requestToken string,
	includeTotal bool,
) (*opensplunkv1.PageResponse, error) {
	page := &opensplunkv1.PageResponse{}
	if result.NextPageToken != "" {
		if !validSearchJobListToken(result.NextPageToken, false) ||
			result.NextPageToken == requestToken ||
			len(result.Jobs) != pageSize {
			return nil, errors.New("search job service returned an invalid page token")
		}
		page.NextPageToken = stringPointer(result.NextPageToken)
	}
	if includeTotal {
		if result.TotalSize == nil || !result.TotalSizeExact || *result.TotalSize < uint64(len(result.Jobs)) {
			return nil, errors.New("search job service returned an invalid total")
		}
		if result.NextPageToken != "" && *result.TotalSize <= uint64(len(result.Jobs)) {
			return nil, errors.New("search job service returned an invalid total for a continued page")
		}
		if requestToken == "" && result.NextPageToken == "" && *result.TotalSize != uint64(len(result.Jobs)) {
			return nil, errors.New("search job service returned an invalid first-page total")
		}
		page.TotalSize = uint64Pointer(*result.TotalSize)
		page.TotalSizeExact = true
	} else if result.TotalSize != nil || result.TotalSizeExact {
		return nil, errors.New("search job service returned an unexpected total")
	}
	return page, nil
}

func validateSearchJobListRequest(input *opensplunkv1.ListSearchJobsRequest) error {
	if input == nil {
		return errors.New("search job list request is required")
	}
	if len(input.ProtoReflect().GetUnknown()) != 0 ||
		input.GetPage() != nil && len(input.GetPage().ProtoReflect().GetUnknown()) != 0 {
		return errors.New("search job list request is invalid")
	}
	return nil
}

func cloneSearchJobListString(input *string) *string {
	if input == nil {
		return nil
	}
	value := strings.Clone(*input)
	return &value
}

func validSearchJobListFailure(state searchjobs.State, failure *searchjobs.Failure) bool {
	if failure == nil {
		return state != searchjobs.StateFailed
	}
	if state != searchjobs.StateFailed && state != searchjobs.StateExpired {
		return false
	}
	if failure.Diagnostics != nil || len(failure.Message) == 0 || len(failure.Message) > maximumSearchJobListFailureMessageBytes ||
		!utf8.ValidString(failure.Message) || strings.TrimSpace(failure.Message) == "" ||
		strings.ContainsRune(failure.Message, '\x00') {
		return false
	}
	switch failure.Code {
	case searchjobs.FailureInvalidSPL,
		searchjobs.FailureUnsupportedSPL,
		searchjobs.FailureInvalidTimeRange,
		searchjobs.FailureIndexForbidden,
		searchjobs.FailureResourceLimit,
		searchjobs.FailureTimeout,
		searchjobs.FailureStorageUnavailable,
		searchjobs.FailureExecution,
		searchjobs.FailureInternal:
		return true
	default:
		return false
	}
}

func validSearchJobListToken(token string, allowEmpty bool) bool {
	if token == "" {
		return allowEmpty
	}
	if len(token) > maximumSearchJobListPageTokenBytes ||
		!utf8.ValidString(token) ||
		strings.TrimSpace(token) != token {
		return false
	}
	for _, character := range token {
		if unicode.IsControl(character) {
			return false
		}
	}
	return true
}

type asciiFoldMatcher struct {
	pattern string
	skip    [256]int
}

func newASCIIFoldMatcher(pattern string) asciiFoldMatcher {
	matcher := asciiFoldMatcher{pattern: pattern}
	for index := range matcher.skip {
		matcher.skip[index] = len(pattern)
	}
	for index := 0; index+1 < len(pattern); index++ {
		matcher.skip[asciiFoldByte(pattern[index])] = len(pattern) - 1 - index
	}
	return matcher
}

func (matcher *asciiFoldMatcher) Contains(value string) bool {
	if matcher == nil || len(matcher.pattern) == 0 {
		return true
	}
	if len(matcher.pattern) > len(value) {
		return false
	}
	last := len(matcher.pattern) - 1
	for offset := 0; offset <= len(value)-len(matcher.pattern); {
		index := last
		for index >= 0 && asciiFoldByte(value[offset+index]) == asciiFoldByte(matcher.pattern[index]) {
			index--
		}
		if index < 0 {
			return true
		}
		offset += matcher.skip[asciiFoldByte(value[offset+last])]
	}
	return false
}

func asciiFoldByte(value byte) byte {
	if value >= 'A' && value <= 'Z' {
		return value + ('a' - 'A')
	}
	return value
}

func searchJobListRequestContextError(ctx context.Context) error {
	if ctx != nil && ctx.Err() != nil {
		return router.NewHTTPError(http.StatusRequestTimeout, "search job list request was canceled")
	}
	return nil
}

type serializedSearchJobListResponse = boundedProtoResponse[*opensplunkv1.ListSearchJobsResponse]

type serializedSearchJobListCodec = boundedProtoCodec[*opensplunkv1.ListSearchJobsRequest, *opensplunkv1.ListSearchJobsResponse]

func newSerializedSearchJobListCodec() *serializedSearchJobListCodec {
	return newBoundedProtoCodec(
		codec.NewProtoCodec[*opensplunkv1.ListSearchJobsRequest, *opensplunkv1.ListSearchJobsResponse](),
		boundedProtoCodecOptions{
			stateError:   "search job list serialization state is invalid",
			messageError: "search job list response is missing",
			contextError: searchJobListRequestContextError,
			maximumBytes: maximumSearchJobListResponseBytes,
			sizeError:    "search job list response exceeds its byte limit",
		},
	)
}
