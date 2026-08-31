package server

import (
	"context"
	"errors"
	"net/http"
	"slices"
	"strings"
	"unicode/utf8"

	"github.com/Suhaibinator/SRouter/pkg/codec"
	"github.com/Suhaibinator/SRouter/pkg/router"
	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/asciifold"
	"github.com/Suhaibinator/open-splunk/internal/searchartifacts"
	"github.com/Suhaibinator/open-splunk/internal/searchjobproto"
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

type contextualSearchJobGetter interface {
	GetForContext(context.Context, searchjobs.AccessScope, string) (searchjobs.Job, error)
}

func (handler *apiHandler) listSearchJobs(
	request *http.Request,
	input *opensplunk.ListSearchJobsRequest,
) (*serializedSearchJobListResponse, error) {
	requestPage := input.GetPage()
	pageSize := int(requestPage.GetPageSize())
	pageToken := requestPage.GetPageToken()
	includeTotal := requestPage.GetIncludeTotalSize()
	states := input.GetStateFilters()
	appID := input.AppIdFilter
	text := input.TextFilter
	var textMatcher *asciifold.Matcher
	if text != nil {
		matcher := asciifold.New(*text)
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
	if !isNilDependency(handler.searchArtifacts) {
		message, listErr := handler.listDurableSearchJobs(
			request.Context(), handler.searchArtifacts, pageSize, pageToken, includeTotal,
			states, appID, text, textMatcher,
		)
		if listErr != nil {
			return nil, listErr
		}
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

	managerStates := make([]searchjobs.State, 0, len(states))
	for _, state := range states {
		if managerState, ok := searchJobListManagerState(state); ok {
			managerStates = append(managerStates, managerState)
		}
	}
	if len(states) != 0 && len(managerStates) == 0 {
		if pageToken != "" {
			return nil, badRequestError("page token is invalid")
		}
		zero := uint64(0)
		page := searchjobs.JobListPage{}
		if includeTotal {
			page.TotalSize = &zero
			page.TotalSizeExact = true
		}
		pageResponse, pageErr := searchJobListPageResponse(page, pageSize, pageToken, includeTotal)
		if pageErr != nil {
			return nil, internalError()
		}
		transferred = true
		return &serializedSearchJobListResponse{
			message: &opensplunk.ListSearchJobsResponse{Page: pageResponse},
			ctx:     request.Context(),
			release: release,
		}, nil
	}

	page, operationErr := handler.jobs.ListPageFor(request.Context(), handler.accessScope(), searchjobs.JobListRequest{
		PageSize:     pageSize,
		PageToken:    pageToken,
		IncludeTotal: includeTotal,
		StateFilters: slices.Clone(managerStates),
		AppIDFilter:  cloneOptionalString(appID),
		TextFilter:   cloneOptionalString(text),
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
	retainedByJobID := make(map[string]searchartifacts.Record)
	if inspector, ok := handler.searchArtifacts.(searchArtifactMetadataBatchInspector); ok {
		jobIDs := make([]string, len(page.Jobs))
		for index, item := range page.Jobs {
			jobIDs[index] = item.ID
		}
		retainedByJobID, operationErr = inspector.InspectMany(
			request.Context(),
			handler.accessScope(),
			jobIDs,
		)
		if contextErr := requestContextFailure(request.Context(), operationErr); contextErr != nil {
			return nil, router.NewHTTPError(http.StatusRequestTimeout, "search job list request was canceled")
		}
		if operationErr != nil {
			return nil, mapSearchArtifactError(operationErr)
		}
	}

	converted := make([]*opensplunk.SearchJob, len(page.Jobs))
	seenIDs := make(map[string]struct{}, len(page.Jobs))
	projectionNow := handler.now()
	var previous searchjobs.Job
	for index, item := range page.Jobs {
		if err := searchJobListRequestContextError(request.Context()); err != nil {
			return nil, err
		}
		job := searchJobListItemAsJob(item)
		if !handler.validKnowledgeSearchJobProjection(job) ||
			!validSearchJobListItem(job, handler.accessScope(), managerStates, appID, textMatcher) {
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
		if retained, ok := retainedByJobID[job.ID]; ok {
			projected, projectionErr = retainedSearchJobToProto(retained, projectionNow)
		}
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
	message := &opensplunk.ListSearchJobsResponse{SearchJobs: converted, Page: pageResponse}
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

func (handler *apiHandler) listDurableSearchJobs(
	ctx context.Context,
	lister SearchArtifacts,
	pageSize int,
	pageToken string,
	includeTotal bool,
	states []opensplunk.SearchJobState,
	appID *string,
	text *string,
	textMatcher *asciifold.Matcher,
) (*opensplunk.ListSearchJobsResponse, error) {
	request := searchartifacts.ListRequest{
		PageSize:     min(searchartifacts.MaximumListPageSize, pageSize+1),
		PageToken:    pageToken,
		StateFilters: durableSearchJobListStates(states),
		AppIDFilter:  cloneOptionalString(appID),
		TextFilter:   cloneOptionalString(text),
	}
	selected := make([]searchartifacts.Record, 0, pageSize)
	nextPageToken := ""
	hasMore := false
	firstPageToken, err := handler.scanDurableSearchJobs(ctx, lister, request,
		func(item searchartifacts.ListItem) (bool, error) {
			record, matches, itemErr := handler.durableSearchJobListItem(
				ctx, item.Record, states, appID, textMatcher,
			)
			if itemErr != nil || !matches {
				return false, itemErr
			}
			if len(selected) < pageSize {
				selected = append(selected, record)
				nextPageToken = item.AfterPageToken
				return false, nil
			}
			hasMore = true
			return true, nil
		})
	if err != nil {
		return nil, err
	}
	if !hasMore {
		nextPageToken = ""
	}

	var totalSize *uint64
	if includeTotal {
		total := uint64(0)
		if firstPageToken != "" {
			countRequest := request
			countRequest.PageToken = firstPageToken
			_, err := handler.scanDurableSearchJobs(ctx, lister, countRequest,
				func(item searchartifacts.ListItem) (bool, error) {
					_, matches, itemErr := handler.durableSearchJobListItem(
						ctx, item.Record, states, appID, textMatcher,
					)
					if matches {
						total++
					}
					return false, itemErr
				})
			if err != nil {
				return nil, err
			}
		}
		totalSize = &total
	}

	converted := make([]*opensplunk.SearchJob, len(selected))
	projectionNow := handler.now()
	for index, record := range selected {
		projected, err := retainedSearchJobToProto(record, projectionNow)
		if err != nil {
			return nil, internalError()
		}
		projected.Plan = nil
		projected.ResultSchema = nil
		projected.Diagnostics = nil
		converted[index] = projected
	}
	pageResponse, err := searchJobListPageResponse(searchjobs.JobListPage{
		Jobs:           make([]searchjobs.JobListItem, len(selected)),
		NextPageToken:  nextPageToken,
		TotalSize:      totalSize,
		TotalSizeExact: includeTotal,
	}, pageSize, pageToken, includeTotal)
	if err != nil {
		return nil, internalError()
	}
	return &opensplunk.ListSearchJobsResponse{SearchJobs: converted, Page: pageResponse}, nil
}

func (handler *apiHandler) scanDurableSearchJobs(
	ctx context.Context,
	lister SearchArtifacts,
	request searchartifacts.ListRequest,
	visit func(searchartifacts.ListItem) (bool, error),
) (string, error) {
	firstPageToken := ""
	for {
		page, err := lister.ListPage(ctx, handler.accessScope(), request)
		if contextErr := requestContextFailure(ctx, err); contextErr != nil {
			return "", router.NewHTTPError(http.StatusRequestTimeout, "search job list request was canceled")
		}
		if err != nil {
			return "", mapSearchArtifactError(err)
		}
		if firstPageToken == "" {
			firstPageToken = page.FirstPageToken
		}
		for _, item := range page.Items {
			stop, visitErr := visit(item)
			if visitErr != nil {
				return "", visitErr
			}
			if stop {
				return firstPageToken, nil
			}
		}
		if page.NextPageToken == "" {
			return firstPageToken, nil
		}
		request.PageToken = page.NextPageToken
	}
}

func (handler *apiHandler) durableSearchJobListItem(
	ctx context.Context,
	record searchartifacts.Record,
	states []opensplunk.SearchJobState,
	appID *string,
	text *asciifold.Matcher,
) (searchartifacts.Record, bool, error) {
	record, err := handler.overlayLiveSearchJob(ctx, record)
	if err != nil {
		return searchartifacts.Record{}, false, err
	}
	if !handler.validKnowledgeSearchJobProjection(record.Job) ||
		!validDurableSearchJobListInvariant(record, handler.accessScope()) {
		return searchartifacts.Record{}, false, internalError()
	}
	if len(states) != 0 && !slices.Contains(states, durableSearchJobListState(record)) {
		return record, false, nil
	}
	if appID != nil && record.Job.AppID != *appID || text != nil && !text.Contains(record.Job.SPL) {
		return record, false, nil
	}
	return record, true, nil
}

func (handler *apiHandler) overlayLiveSearchJob(
	ctx context.Context,
	record searchartifacts.Record,
) (searchartifacts.Record, error) {
	if record.State < searchartifacts.StateQueued || record.State > searchartifacts.StateRunning {
		return record, nil
	}
	var (
		job searchjobs.Job
		err error
	)
	if getter, ok := handler.jobs.(contextualSearchJobGetter); ok {
		job, err = getter.GetForContext(ctx, handler.accessScope(), record.Job.ID)
	} else {
		if contextErr := ctx.Err(); contextErr != nil {
			return searchartifacts.Record{}, contextErr
		}
		job, err = handler.jobs.GetFor(handler.accessScope(), record.Job.ID)
	}
	if errors.Is(err, searchjobs.ErrNotFound) {
		return record, nil
	}
	if err != nil {
		return searchartifacts.Record{}, mapSearchJobError(err)
	}
	if contextErr := ctx.Err(); contextErr != nil {
		return searchartifacts.Record{}, contextErr
	}
	if job.ID != record.Job.ID || job.TenantID != record.Job.TenantID || job.OwnerID != record.Job.OwnerID {
		return searchartifacts.Record{}, internalError()
	}
	state := durableSearchJobState(job.State)
	if state == searchartifacts.StateInvalid {
		return searchartifacts.Record{}, internalError()
	}
	record.Job = job
	record.State = state
	if record.ExpiresAt.IsZero() && !job.ExpiresAt.IsZero() {
		record.ExpiresAt = job.ExpiresAt
	}
	return record, nil
}

func validDurableSearchJobListInvariant(
	record searchartifacts.Record,
	scope searchjobs.AccessScope,
) bool {
	if record.Job.TenantID != scope.TenantID ||
		(record.Job.OwnerID != scope.OwnerID && record.Visibility != searchartifacts.VisibilityEveryone) ||
		(record.Visibility != searchartifacts.VisibilityPrivate && record.Visibility != searchartifacts.VisibilityEveryone) {
		return false
	}
	validationScope := searchjobs.AccessScope{TenantID: record.Job.TenantID, OwnerID: record.Job.OwnerID}
	return validSearchJobListItem(record.Job, validationScope, nil, nil, nil)
}

func durableSearchJobListState(record searchartifacts.Record) opensplunk.SearchJobState {
	if record.State == searchartifacts.StateInterrupted {
		return opensplunk.SearchJobState_SEARCH_JOB_STATE_INTERRUPTED
	}
	if record.State == searchartifacts.StateExpired {
		return opensplunk.SearchJobState_SEARCH_JOB_STATE_EXPIRED
	}
	return searchjobproto.State(record.Job.State)
}

func durableSearchJobListStates(states []opensplunk.SearchJobState) []searchartifacts.State {
	result := make([]searchartifacts.State, 0, len(states))
	for _, state := range states {
		if state == opensplunk.SearchJobState_SEARCH_JOB_STATE_INTERRUPTED {
			result = append(result, searchartifacts.StateInterrupted)
			continue
		}
		if managerState, ok := searchJobListManagerState(state); ok {
			result = append(result, durableSearchJobState(managerState))
		}
	}
	return result
}

func durableSearchJobState(state searchjobs.State) searchartifacts.State {
	switch state {
	case searchjobs.StateQueued:
		return searchartifacts.StateQueued
	case searchjobs.StateParsing:
		return searchartifacts.StateParsing
	case searchjobs.StatePlanning:
		return searchartifacts.StatePlanning
	case searchjobs.StateRunning:
		return searchartifacts.StateRunning
	case searchjobs.StateCompleted:
		return searchartifacts.StateCompleted
	case searchjobs.StateFailed:
		return searchartifacts.StateFailed
	case searchjobs.StateCanceled:
		return searchartifacts.StateCanceled
	case searchjobs.StateExpired:
		return searchartifacts.StateExpired
	default:
		return searchartifacts.StateInvalid
	}
}

func searchJobListItemAsJob(item searchjobs.JobListItem) searchjobs.Job {
	result := searchjobs.Job{
		ID:                item.ID,
		Version:           item.Version,
		OwnerID:           item.OwnerID,
		TenantID:          item.TenantID,
		SPL:               item.SPL,
		NormalizedSPL:     item.NormalizedSPL,
		RequestedIndexes:  slices.Clone(item.RequestedIndexes),
		EffectiveIndexes:  slices.Clone(item.EffectiveIndexes),
		TimeRange:         item.TimeRange,
		AppID:             item.AppID,
		Source:            item.Source,
		Earliest:          item.Earliest,
		Latest:            item.Latest,
		IndexTimeCutoff:   item.IndexTimeCutoff,
		State:             item.State,
		ScannedRows:       item.ScannedRows,
		ScannedBytes:      item.ScannedBytes,
		RowCount:          item.RowCount,
		ResultBytes:       item.ResultBytes,
		ResultsTruncated:  item.ResultsTruncated,
		KnowledgeSnapshot: item.KnowledgeSnapshot,
		CreatedAt:         item.CreatedAt,
		StartedAt:         item.StartedAt,
		FinishedAt:        item.FinishedAt,
		ExpiresAt:         item.ExpiresAt,
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

func (handler *apiHandler) searchJobListPageRequest(page *opensplunk.PageRequest) (int, string, bool, error) {
	if page != nil && page.PageToken != nil && strings.TrimSpace(page.GetPageToken()) != page.GetPageToken() {
		return 0, "", false, errors.New("page token is invalid")
	}
	pageSize, pageToken, includeTotal, err := handler.pageRequest(page)
	if err != nil {
		return 0, "", false, err
	}
	if !validBoundedListPageToken(
		pageToken,
		maximumSearchJobListPageTokenBytes,
		true,
	) {
		return 0, "", false, errors.New("page token is invalid")
	}
	if pageSize == 0 {
		pageSize = min(maximumSearchJobListRows, int(handler.maximumPageSize))
	}
	return min(pageSize, maximumSearchJobListRows), pageToken, includeTotal, nil
}

func searchJobListManagerState(input opensplunk.SearchJobState) (searchjobs.State, bool) {
	switch input {
	case opensplunk.SearchJobState_SEARCH_JOB_STATE_QUEUED:
		return searchjobs.StateQueued, true
	case opensplunk.SearchJobState_SEARCH_JOB_STATE_PARSING:
		return searchjobs.StateParsing, true
	case opensplunk.SearchJobState_SEARCH_JOB_STATE_PLANNING:
		return searchjobs.StatePlanning, true
	case opensplunk.SearchJobState_SEARCH_JOB_STATE_RUNNING:
		return searchjobs.StateRunning, true
	case opensplunk.SearchJobState_SEARCH_JOB_STATE_COMPLETED:
		return searchjobs.StateCompleted, true
	case opensplunk.SearchJobState_SEARCH_JOB_STATE_FAILED:
		return searchjobs.StateFailed, true
	case opensplunk.SearchJobState_SEARCH_JOB_STATE_CANCELED:
		return searchjobs.StateCanceled, true
	case opensplunk.SearchJobState_SEARCH_JOB_STATE_EXPIRED:
		return searchjobs.StateExpired, true
	default:
		// UNSPECIFIED, FINALIZING, INTERRUPTED, and values unknown to this
		// binary have no corresponding in-memory manager lifecycle.
		return searchjobs.StateInvalid, false
	}
}

func validSearchJobListItem(
	job searchjobs.Job,
	scope searchjobs.AccessScope,
	states []searchjobs.State,
	appID *string,
	text *asciifold.Matcher,
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
	if searchjobproto.State(job.State) == opensplunk.SearchJobState_SEARCH_JOB_STATE_UNSPECIFIED {
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
) (*opensplunk.PageResponse, error) {
	return boundedListPageResponse(
		"search job",
		boundedListPageMetadata{
			itemCount:     len(result.Jobs),
			nextPageToken: result.NextPageToken,
			totalSize:     result.TotalSize,
			totalExact:    result.TotalSizeExact,
		},
		pageSize,
		requestToken,
		includeTotal,
		maximumSearchJobListPageTokenBytes,
	)
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

func searchJobListRequestContextError(ctx context.Context) error {
	return canceledRequestError(ctx, "search job list request was canceled")
}

type serializedSearchJobListResponse = boundedProtoResponse[*opensplunk.ListSearchJobsResponse]

type serializedSearchJobListCodec = boundedProtoCodec[*opensplunk.ListSearchJobsRequest, *opensplunk.ListSearchJobsResponse]

func newSerializedSearchJobListCodec() *serializedSearchJobListCodec {
	return newBoundedProtoCodec(
		codec.NewProtoCodec[*opensplunk.ListSearchJobsRequest, *opensplunk.ListSearchJobsResponse](),
		boundedProtoCodecOptions{
			stateError:   "search job list serialization state is invalid",
			messageError: "search job list response is missing",
			contextError: searchJobListRequestContextError,
			maximumBytes: maximumSearchJobListResponseBytes,
			sizeError:    "search job list response exceeds its byte limit",
		},
	)
}
