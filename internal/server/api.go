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

	"github.com/Suhaibinator/SRouter/pkg/router"
	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
	"github.com/Suhaibinator/open-splunk/internal/searchtime"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const maximumRequestedIndexes = 128

func (handler *apiHandler) getSystemBootstrap(request *http.Request, input *opensplunkv1.GetSystemBootstrapRequest) (*opensplunkv1.GetSystemBootstrapResponse, error) {
	indexes, err := handler.indexes.ListIndexes(request.Context())
	if contextErr := requestContextFailure(request.Context(), err); contextErr != nil {
		return nil, contextErr
	}
	if err != nil {
		return nil, unavailableError("control plane is unavailable")
	}
	indexSummaries := make([]*opensplunkv1.IndexSummary, len(indexes))
	for index, record := range indexes {
		indexSummaries[index] = indexSummaryToProto(record)
	}
	apps := cloneApps(handler.bootstrap.Apps)
	selectedAppID := handler.bootstrap.SelectedAppID
	if preferred := strings.TrimSpace(input.GetPreferredAppId()); preferred != "" && appExists(apps, preferred) {
		selectedAppID = preferred
	}
	if selectedAppID == "" && len(apps) > 0 {
		selectedAppID = apps[0].GetAppId()
	}
	response := &opensplunkv1.GetSystemBootstrapResponse{
		ServerVersion:           handler.bootstrap.ServerVersion,
		ApiVersion:              handler.bootstrap.APIVersion,
		SplCompatibilityVersion: handler.bootstrap.SPLCompatibilityVersion,
		SearchWebsocketPath:     handler.bootstrap.SearchWebSocketPath,
		Features:                slices.Clone(handler.bootstrap.Features),
		Limits: &opensplunkv1.BrowserApiLimits{
			MaximumPageSize:               handler.maximumPageSize,
			MaximumPreviewRows:            handler.bootstrap.MaximumPreviewRows,
			MaximumWebsocketSubscriptions: handler.bootstrap.MaximumSubscriptions,
			MaximumWebsocketFrameBytes:    handler.bootstrap.MaximumWebSocketBytes,
			MaximumExportRows:             handler.bootstrap.MaximumExportRows,
			MaximumExportBytes:            handler.bootstrap.MaximumExportBytes,
			DefaultSearchTimeout:          durationpb.New(handler.bootstrap.DefaultSearchTimeout),
			SearchResultRetention:         durationpb.New(handler.bootstrap.SearchResultRetention),
			MaximumTimelineBuckets:        handler.maximumTimelineBuckets,
			MaximumFieldSummaryValues:     handler.maximumFieldSummaryValues,
		},
		Apps:       apps,
		Indexes:    indexSummaries,
		ServerTime: timestamppb.New(handler.now().Round(0).UTC()),
	}
	if selectedAppID != "" {
		response.SelectedAppId = stringPointer(selectedAppID)
	}
	if err := request.Context().Err(); err != nil {
		return nil, err
	}
	return response, nil
}

func (handler *apiHandler) createSearchJob(request *http.Request, input *opensplunkv1.CreateSearchJobRequest) (*opensplunkv1.CreateSearchJobResponse, error) {
	definition := input.GetDefinition()
	if definition == nil {
		return nil, badRequestError("search definition is required")
	}
	spl := definition.GetSpl()
	if strings.TrimSpace(spl) == "" {
		return nil, badRequestError("SPL is required")
	}
	if strings.IndexByte(spl, 0) >= 0 {
		return nil, badRequestError("SPL cannot contain NUL bytes")
	}
	if err := rejectUnsupportedCreateFields(input, definition); err != nil {
		return nil, badRequestError(err.Error())
	}
	resolvedRange, err := resolveSearchTimeRange(definition.GetTimeRange(), handler.now())
	if err != nil {
		return nil, badRequestError(err.Error())
	}
	appID, source, err := handler.resolveSearchJobSource(request.Context(), definition.GetAppId(), input.GetSource())
	if err != nil {
		return nil, err
	}
	requestedIndexes, err := normalizeRequestedIndexes(definition.GetIndexScope())
	if err != nil {
		return nil, badRequestError(err.Error())
	}
	if len(requestedIndexes) == 0 {
		return nil, badRequestError("at least one index is required")
	}
	if err := handler.authorizeRequestedIndexes(request.Context(), requestedIndexes); err != nil {
		if contextErr := requestContextFailure(request.Context(), err); contextErr != nil {
			return nil, contextErr
		}
		if errors.Is(err, errIndexUnavailable) {
			return nil, forbiddenError("index scope contains an unavailable index")
		}
		return nil, unavailableError("control plane is unavailable")
	}

	job, err := handler.jobs.Create(request.Context(), searchjobs.CreateRequest{
		SPL:      spl,
		OwnerID:  handler.ownerID,
		TenantID: handler.tenantID,
		// The catalog check above authorizes this exact immutable request scope.
		// Passing every catalog index would make admission grow with the whole
		// deployment and could exceed the manager's metadata bound.
		AuthorizedIndexes: slices.Clone(requestedIndexes),
		RequestedIndexes:  requestedIndexes,
		TimeRange:         resolvedRange,
		AppID:             appID,
		Source:            source,
	})
	if err != nil {
		if contextErr := requestContextFailure(request.Context(), err); contextErr != nil {
			return nil, contextErr
		}
		return nil, mapSearchJobError(err)
	}
	converted, err := searchJobToProto(job, handler.now())
	if err != nil {
		return nil, internalError()
	}
	return &opensplunkv1.CreateSearchJobResponse{SearchJob: converted}, nil
}

func (handler *apiHandler) getSearchJob(request *http.Request, input *opensplunkv1.GetSearchJobRequest) (*opensplunkv1.GetSearchJobResponse, error) {
	id := strings.TrimSpace(input.GetSearchJobId())
	if id == "" {
		return nil, badRequestError("search job ID is required")
	}
	if input.GetIncludePlan() || input.GetIncludeGeneratedSql() {
		return nil, badRequestError("plan inspection is not supported by this API version")
	}
	job, err := handler.jobs.GetFor(handler.accessScope(), id)
	if contextErr := requestContextFailure(request.Context(), err); contextErr != nil {
		return nil, contextErr
	}
	if err != nil {
		return nil, mapSearchJobError(err)
	}
	converted, err := searchJobToProto(job, handler.now())
	if err != nil {
		return nil, internalError()
	}
	if err := request.Context().Err(); err != nil {
		return nil, err
	}
	// Generated SQL is intentionally not present in the search-job layer and is
	// never accepted from or exposed to an ordinary browser request.
	return &opensplunkv1.GetSearchJobResponse{SearchJob: converted}, nil
}

func (handler *apiHandler) getSearchResults(request *http.Request, input *opensplunkv1.GetSearchResultsRequest) (*serializedSearchResultsResponse, error) {
	id := strings.TrimSpace(input.GetSearchJobId())
	if id == "" {
		return nil, badRequestError("search job ID is required")
	}
	if input.GetAllowPartialResults() {
		return nil, badRequestError("partial search results are not supported")
	}
	if len(input.GetColumns()) != 0 {
		return nil, badRequestError("result column projection is not supported")
	}
	pageSize, pageToken, includeTotal, err := handler.pageRequest(input.GetPage())
	if err != nil {
		return nil, badRequestError(err.Error())
	}
	job, err := handler.jobs.GetFor(handler.accessScope(), id)
	if contextErr := requestContextFailure(request.Context(), err); contextErr != nil {
		return nil, contextErr
	}
	if err != nil {
		return nil, mapSearchJobError(err)
	}
	release, acquired := handler.acquireSerialization()
	if !acquired {
		return nil, unavailableError("search result capacity is exhausted")
	}
	transferred := false
	defer func() {
		if !transferred {
			release()
		}
	}()
	page, err := handler.jobs.ResultsFor(handler.accessScope(), id, searchjobs.PageRequest{
		Limit:  pageSize,
		Cursor: pageToken,
	})
	if contextErr := requestContextFailure(request.Context(), err); contextErr != nil {
		return nil, contextErr
	}
	if err != nil {
		return nil, mapSearchJobError(err)
	}
	converted, err := resultPageToProto(request.Context(), id, page, resultKindForSPL(job.SPL), includeTotal, job.ResultsTruncated)
	if err != nil {
		if contextErr := requestContextFailure(request.Context(), err); contextErr != nil {
			return nil, contextErr
		}
		return nil, internalError()
	}
	if err := request.Context().Err(); err != nil {
		return nil, err
	}
	transferred = true
	return &serializedSearchResultsResponse{
		message: &opensplunkv1.GetSearchResultsResponse{SearchJobId: id, ResultPage: converted},
		ctx:     request.Context(),
		release: release,
	}, nil
}

func (handler *apiHandler) cancelSearchJob(request *http.Request, input *opensplunkv1.CancelSearchJobRequest) (*opensplunkv1.CancelSearchJobResponse, error) {
	id := strings.TrimSpace(input.GetSearchJobId())
	if id == "" {
		return nil, badRequestError("search job ID is required")
	}
	if strings.TrimSpace(input.GetReason()) != "" {
		return nil, badRequestError("cancellation reasons are not supported")
	}
	// CancelFor has no context parameter, so reject a request that is already
	// canceled before crossing the mutation boundary. Once CancelFor returns
	// nil, the cancellation is authoritative and must not be rewritten as a
	// timeout by a context cancellation racing after the commit point.
	if err := request.Context().Err(); err != nil {
		return nil, err
	}
	if err := handler.jobs.CancelFor(handler.accessScope(), id); err != nil {
		if contextErr := requestContextFailure(request.Context(), err); contextErr != nil {
			return nil, contextErr
		}
		return nil, mapSearchJobError(err)
	}
	job, err := handler.jobs.GetFor(handler.accessScope(), id)
	if err != nil {
		return nil, mapSearchJobError(err)
	}
	converted, err := searchJobToProto(job, handler.now())
	if err != nil {
		return nil, internalError()
	}
	return &opensplunkv1.CancelSearchJobResponse{SearchJob: converted}, nil
}

func (handler *apiHandler) accessScope() searchjobs.AccessScope {
	return searchjobs.AccessScope{TenantID: handler.tenantID, OwnerID: handler.ownerID}
}

func requestContextFailure(ctx context.Context, operationErr error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if errors.Is(operationErr, context.DeadlineExceeded) || errors.Is(operationErr, context.Canceled) {
		return operationErr
	}
	return nil
}

var errIndexUnavailable = errors.New("requested index is unavailable")

func (handler *apiHandler) authorizeRequestedIndexes(ctx context.Context, requested []string) error {
	for _, name := range requested {
		record, err := handler.indexes.GetIndexByName(ctx, name)
		if errors.Is(err, control.ErrNotFound) {
			return errIndexUnavailable
		}
		if err != nil {
			return err
		}
		if record.State != control.IndexStateActive || !record.Definition.SearchEnabled || record.Definition.Name != name {
			return errIndexUnavailable
		}
	}
	return nil
}

func rejectUnsupportedCreateFields(input *opensplunkv1.CreateSearchJobRequest, definition *opensplunkv1.SearchDefinition) error {
	if input.ClientRequestId != nil {
		return errors.New("client request idempotency is not supported")
	}
	if options := input.GetOptions(); options != nil {
		if options.GetEnablePreview() || options.PreviewRowLimit != nil {
			return errors.New("job-level preview options are not supported; request bounded previews on the WebSocket search subscription")
		}
		if options.GetEnableFieldDiscovery() || options.GetEnableTimeline() {
			return errors.New("eager field discovery and timeline options are not supported; request those analyses through their dedicated APIs")
		}
	}
	if definition.GetPreferredResultTab() != opensplunkv1.SearchResultTab_SEARCH_RESULT_TAB_UNSPECIFIED || len(definition.GetSelectedFields()) != 0 || definition.GetVisualization() != nil {
		return errors.New("search presentation metadata is not supported")
	}
	return nil
}

func (handler *apiHandler) pageRequest(page *opensplunkv1.PageRequest) (int, string, bool, error) {
	if page == nil {
		return 0, "", false, nil
	}
	pageSize := uint32(0)
	if page.PageSize != nil {
		pageSize = page.GetPageSize()
		if pageSize == 0 {
			return 0, "", false, errors.New("page size must be positive when supplied")
		}
		if pageSize > handler.maximumPageSize || uint64(pageSize) > uint64(math.MaxInt) {
			return 0, "", false, fmt.Errorf("page size exceeds the maximum of %d", handler.maximumPageSize)
		}
	}
	pageToken := strings.TrimSpace(page.GetPageToken())
	if len(pageToken) > 4<<10 {
		return 0, "", false, errors.New("page token is too large")
	}
	return int(pageSize), pageToken, page.GetIncludeTotalSize(), nil
}

func resolveSearchTimeRange(spec *opensplunkv1.TimeRangeSpec, now time.Time) (searchtime.Range, error) {
	if spec == nil || spec.Earliest == nil || spec.Latest == nil {
		return searchtime.Range{}, errors.New("earliest and latest time expressions are required")
	}
	return searchtime.Resolve(spec.GetEarliest(), spec.GetLatest(), spec.Timezone, now)
}

func (handler *apiHandler) resolveSearchJobSource(
	ctx context.Context,
	requestedAppID string,
	input *opensplunkv1.SearchJobSource,
) (string, searchjobs.JobSource, error) {
	appID := strings.TrimSpace(requestedAppID)
	if err := validateBoundedIdentifier(appID, maximumSavedSearchAppIDBytes, true); err != nil {
		return "", searchjobs.JobSource{}, badRequestError("search app ID is invalid")
	}
	if input == nil || input.GetOrigin() == opensplunkv1.SearchJobOrigin_SEARCH_JOB_ORIGIN_UNSPECIFIED &&
		input.SavedSearchId == nil && input.HistorySearchId == nil && input.DashboardId == nil {
		return appID, searchjobs.JobSource{Origin: searchjobs.JobOriginAdHoc}, nil
	}
	if input.GetOrigin() == opensplunkv1.SearchJobOrigin_SEARCH_JOB_ORIGIN_AD_HOC {
		if input.SavedSearchId != nil || input.HistorySearchId != nil || input.DashboardId != nil {
			return "", searchjobs.JobSource{}, badRequestError("ad-hoc search source cannot include an object ID")
		}
		return appID, searchjobs.JobSource{Origin: searchjobs.JobOriginAdHoc}, nil
	}
	if input.GetOrigin() != opensplunkv1.SearchJobOrigin_SEARCH_JOB_ORIGIN_SAVED_SEARCH ||
		input.SavedSearchId == nil || input.HistorySearchId != nil || input.DashboardId != nil {
		return "", searchjobs.JobSource{}, badRequestError("search job source metadata is invalid or unsupported")
	}
	savedSearchID, err := savedSearchID(input.GetSavedSearchId())
	if err != nil {
		return "", searchjobs.JobSource{}, badRequestError(err.Error())
	}
	record, err := handler.savedSearches.Get(ctx, handler.savedSearchScope(), savedSearchID)
	if mapped := mapSavedSearchCallError(ctx, err); mapped != nil {
		return "", searchjobs.JobSource{}, mapped
	}
	trustedRecord, err := handler.cloneSavedSearch(record)
	if err != nil || trustedRecord.GetSavedSearchId() != savedSearchID {
		return "", searchjobs.JobSource{}, internalError()
	}
	savedAppID := savedSearchAppID(trustedRecord)
	if appID == "" {
		appID = savedAppID
	} else if appID != savedAppID {
		return "", searchjobs.JobSource{}, badRequestError("search app ID does not match the saved search")
	}
	return appID, searchjobs.JobSource{
		Origin:   searchjobs.JobOriginSavedSearch,
		ObjectID: savedSearchID,
	}, nil
}

func normalizeRequestedIndexes(input []string) ([]string, error) {
	if len(input) > maximumRequestedIndexes {
		return nil, fmt.Errorf("index scope exceeds the maximum of %d", maximumRequestedIndexes)
	}
	result := make([]string, 0, len(input))
	seen := make(map[string]struct{}, len(input))
	for _, candidate := range input {
		normalized, err := control.NormalizeIndexName(candidate)
		if err != nil {
			return nil, errors.New("index scope contains an invalid index name")
		}
		if _, exists := seen[normalized]; exists {
			continue
		}
		seen[normalized] = struct{}{}
		result = append(result, normalized)
	}
	return result, nil
}

func mapSearchJobError(err error) error {
	switch {
	case errors.Is(err, searchjobs.ErrNotFound):
		return router.NewHTTPError(http.StatusNotFound, "search job not found")
	case errors.Is(err, searchjobs.ErrExpired):
		return router.NewHTTPError(http.StatusGone, "search job results expired")
	case errors.Is(err, searchjobs.ErrResultsNotReady):
		return router.NewHTTPError(http.StatusConflict, "search results are not ready")
	case errors.Is(err, searchjobs.ErrResultsUnavailable):
		return router.NewHTTPError(http.StatusConflict, "search results are unavailable")
	case errors.Is(err, searchjobs.ErrInvalidListFilter):
		return badRequestError("search job list filter is invalid")
	case errors.Is(err, searchjobs.ErrInvalidCursor):
		return badRequestError("page token is invalid")
	case errors.Is(err, searchjobs.ErrPageSize):
		return badRequestError("page size is invalid")
	case errors.Is(err, searchjobs.ErrByteLimit):
		return router.NewHTTPError(http.StatusUnprocessableEntity, "a search result row exceeds the page byte limit")
	case errors.Is(err, searchjobs.ErrRequestTooLarge):
		return router.NewHTTPError(http.StatusRequestEntityTooLarge, "search request is too large")
	case errors.Is(err, searchjobs.ErrQueueFull):
		return router.NewHTTPError(http.StatusTooManyRequests, "search queue is full")
	case errors.Is(err, searchjobs.ErrCapacity), errors.Is(err, searchjobs.ErrClosed), errors.Is(err, searchjobs.ErrStorageUnavailable), errors.Is(err, searchjobs.ErrJournalUnavailable):
		return unavailableError("search service is unavailable")
	case errors.Is(err, context.DeadlineExceeded):
		return err
	case errors.Is(err, context.Canceled):
		return err
	default:
		return internalError()
	}
}

func badRequestError(message string) error {
	return router.NewHTTPError(http.StatusBadRequest, message)
}

func forbiddenError(message string) error {
	return router.NewHTTPError(http.StatusForbidden, message)
}

func unavailableError(message string) error {
	return router.NewHTTPError(http.StatusServiceUnavailable, message)
}

func internalError() error {
	return router.NewHTTPError(http.StatusInternalServerError, "internal server error")
}
