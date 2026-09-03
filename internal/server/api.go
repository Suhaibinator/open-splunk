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
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/buildmetadata"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/searchartifacts"
	"github.com/Suhaibinator/open-splunk/internal/searchjobproto"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
	"github.com/Suhaibinator/open-splunk/internal/searchtime"
)

const maximumRequestedIndexes = 128

func (handler *apiHandler) getSystemBootstrap(request *http.Request, input *opensplunk.GetSystemBootstrapRequest) (*opensplunk.GetSystemBootstrapResponse, error) {
	indexes, err := handler.indexes.ListIndexes(request.Context())
	if contextErr := requestContextFailure(request.Context(), err); contextErr != nil {
		return nil, bootstrapContextError(contextErr)
	}
	if err != nil {
		return nil, unavailableError("control plane is unavailable")
	}
	indexSummaries := make([]*opensplunk.IndexSummary, len(indexes))
	for index, record := range indexes {
		indexSummaries[index] = indexSummaryToProto(record)
	}
	apps := cloneApps(handler.bootstrap.Apps)
	if handler.appCatalog != nil {
		catalog, catalogErr := handler.appCatalog.ListActiveApps(
			request.Context(),
			handler.tenantID,
			uint32(maximumBootstrapApps),
		)
		if contextErr := requestContextFailure(
			request.Context(),
			catalogErr,
		); contextErr != nil {
			return nil, bootstrapContextError(contextErr)
		}
		if catalogErr != nil {
			return nil, unavailableError("control plane is unavailable")
		}
		apps, catalogErr = appCatalogSummariesToProto(catalog)
		if catalogErr != nil {
			return nil, internalError()
		}
	}

	selectedAppID := strings.TrimSpace(handler.bootstrap.SelectedAppID)
	if !appExists(apps, selectedAppID) {
		selectedAppID = ""
	}
	preferredAppID := input.GetPreferredAppId()
	if preferredAppID != "" && appExists(apps, preferredAppID) {
		selectedAppID = preferredAppID
	}
	if selectedAppID == "" && len(apps) > 0 {
		selectedAppID = apps[0].GetAppId()
	}
	defaultSearchTimeout := handler.bootstrap.DefaultSearchTimeout
	searchResultRetention := handler.bootstrap.SearchResultRetention
	if handler.serverSettings != nil {
		current := handler.serverSettings.Current()
		defaultSearchTimeout = current.Limits.MaxRuntime
		searchResultRetention = current.Limits.ResultRetention
	}
	response := &opensplunk.GetSystemBootstrapResponse{
		Build:               buildmetadata.Clone(handler.bootstrap.Build),
		SearchWebsocketPath: handler.bootstrap.SearchWebSocketPath,
		Features:            slices.Clone(handler.bootstrap.Features),
		Limits: &opensplunk.BrowserApiLimits{
			MaximumPageSize:               handler.maximumPageSize,
			MaximumPreviewRows:            handler.bootstrap.MaximumPreviewRows,
			MaximumWebsocketSubscriptions: handler.bootstrap.MaximumSubscriptions,
			MaximumWebsocketFrameBytes:    handler.bootstrap.MaximumWebSocketBytes,
			MaximumExportRows:             handler.bootstrap.MaximumExportRows,
			MaximumExportBytes:            handler.bootstrap.MaximumExportBytes,
			DefaultSearchTimeout:          durationpb.New(defaultSearchTimeout),
			SearchResultRetention:         durationpb.New(searchResultRetention),
			MaximumTimelineBuckets:        handler.maximumTimelineBuckets,
			MaximumFieldSummaryValues:     handler.maximumFieldSummaryValues,
		},
		Apps:       apps,
		Indexes:    indexSummaries,
		ServerTime: timestamppb.New(handler.now().Round(0).UTC()),
	}
	if selectedAppID != "" {
		response.SelectedAppId = new(selectedAppID)
	}
	if err := request.Context().Err(); err != nil {
		return nil, err
	}
	return response, nil
}

func bootstrapContextError(err error) error {
	if errors.Is(err, context.Canceled) ||
		errors.Is(err, context.DeadlineExceeded) {
		return router.NewHTTPError(
			http.StatusRequestTimeout,
			"bootstrap request was canceled",
		)
	}
	return err
}

func appCatalogSummariesToProto(
	catalog AppCatalogResult,
) ([]*opensplunk.AppSummary, error) {
	if !catalog.Complete || len(catalog.Apps) > maximumBootstrapApps {
		return nil, errors.New("active app catalog is incomplete or exceeds its bound")
	}

	apps := make([]*opensplunk.AppSummary, 0, len(catalog.Apps))
	seenIDs := make(map[string]struct{}, len(catalog.Apps))
	seenSlugs := make(map[string]struct{}, len(catalog.Apps))
	for _, app := range catalog.Apps {
		appID := strings.TrimSpace(app.AppID)
		if appID != app.AppID ||
			validateBoundedIdentifier(
				appID,
				maximumAppAdministrationIDBytes,
				false,
			) != nil {
			return nil, errors.New("active app catalog contains an invalid app ID")
		}
		slug, err := normalizeAppAdministrationSlug(app.Slug)
		if err != nil || slug != app.Slug {
			return nil, errors.New("active app catalog contains an invalid slug")
		}
		displayName, err := normalizeAppAdministrationDisplayName(
			app.DisplayName,
		)
		if err != nil || displayName != app.DisplayName {
			return nil, errors.New(
				"active app catalog contains an invalid display name",
			)
		}
		indexNames, err := normalizeAppAdministrationIndexes(
			app.DefaultIndexNames,
		)
		if err != nil ||
			!slices.Equal(indexNames, app.DefaultIndexNames) {
			return nil, errors.New(
				"active app catalog contains invalid default indexes",
			)
		}
		if _, duplicate := seenIDs[appID]; duplicate {
			return nil, errors.New("active app catalog contains a duplicate app")
		}
		if _, duplicate := seenSlugs[slug]; duplicate {
			return nil, errors.New("active app catalog contains a duplicate app")
		}
		seenIDs[appID] = struct{}{}
		seenSlugs[slug] = struct{}{}
		apps = append(apps, &opensplunk.AppSummary{
			AppId:             strings.Clone(appID),
			Slug:              strings.Clone(slug),
			DisplayName:       strings.Clone(displayName),
			DefaultIndexNames: slices.Clone(indexNames),
			State:             opensplunk.AppState_APP_STATE_ACTIVE,
		})
	}
	slices.SortFunc(
		apps,
		func(left, right *opensplunk.AppSummary) int {
			if order := strings.Compare(left.GetSlug(), right.GetSlug()); order != 0 {
				return order
			}
			return strings.Compare(left.GetAppId(), right.GetAppId())
		},
	)
	return apps, nil
}

func (handler *apiHandler) createSearchJob(request *http.Request, input *opensplunk.CreateSearchJobRequest) (*opensplunk.CreateSearchJobResponse, error) {
	var (
		resolved     resolvedSearchDefinition
		source       searchjobs.JobSource
		err          error
		historyRerun = input.GetSource().GetOrigin() == opensplunk.SearchJobOrigin_SEARCH_JOB_ORIGIN_HISTORY_RERUN
		savedLaunch  = input.GetSource().GetOrigin() == opensplunk.SearchJobOrigin_SEARCH_JOB_ORIGIN_SAVED_SEARCH
	)
	switch {
	case historyRerun:
		resolved, source, err = handler.resolveHistoryRerun(
			request.Context(),
			input,
		)
	case savedLaunch:
		resolved, source, err = handler.resolveSavedSearchLaunch(
			request.Context(),
			input,
		)
	default:
		resolved, err = handler.resolveSearchDefinition(input.GetDefinition())
		if err == nil {
			resolved.AppID, source, err = handler.resolveSearchJobSource(
				request.Context(),
				resolved.AppID,
				input.GetSource(),
			)
		}
	}
	if err != nil {
		if contextErr := historyRerunContextError(
			request.Context(),
			err,
			historyRerun,
		); contextErr != nil {
			return nil, contextErr
		}
		return nil, err
	}
	if handler.trustedSearchAdmission == nil && (historyRerun || handler.knowledgeSearchAdmission) {
		if err := handler.authorizeSearchApp(request.Context(), resolved.AppID); err != nil {
			if contextErr := historyRerunContextError(request.Context(), err, historyRerun); contextErr != nil {
				return nil, contextErr
			}
			return nil, err
		}
	}
	var job searchjobs.Job
	if handler.trustedSearchAdmission != nil {
		job, err = handler.trustedSearchAdmission.AdmitTrustedSearch(request.Context(), TrustedSearchAdmissionRequest{
			SPL: resolved.SPL, OwnerID: handler.ownerID, TenantID: handler.tenantID,
			AppID: resolved.AppID, IndexScope: slices.Clone(resolved.IndexScope),
			TimeRange: resolved.TimeRange, Source: source,
		})
	} else {
		var requestedIndexes []string
		requestedIndexes, err = handler.resolveAuthorizedSearchIndexes(request.Context(), resolved.IndexScope)
		if err != nil {
			if contextErr := historyRerunContextError(request.Context(), err, historyRerun); contextErr != nil {
				return nil, contextErr
			}
			return nil, err
		}
		job, err = handler.jobs.Create(request.Context(), searchjobs.CreateRequest{
			SPL: resolved.SPL, OwnerID: handler.ownerID, TenantID: handler.tenantID,
			AuthorizedIndexes: slices.Clone(requestedIndexes), RequestedIndexes: requestedIndexes,
			TimeRange: resolved.TimeRange, AppID: resolved.AppID, Source: source,
		})
	}
	if err != nil {
		if contextErr := historyRerunContextError(
			request.Context(),
			err,
			historyRerun,
		); contextErr != nil {
			return nil, contextErr
		}
		if contextErr := requestContextFailure(request.Context(), err); contextErr != nil {
			return nil, contextErr
		}
		switch {
		case errors.Is(err, ErrTrustedSearchAppUnavailable), errors.Is(err, ErrTrustedSearchIndexUnavailable):
			return nil, forbiddenError("search authority is unavailable")
		case errors.Is(err, ErrTrustedSearchAuthorityUnavailable):
			return nil, unavailableError("control plane is unavailable")
		default:
			return nil, mapSearchJobError(err)
		}
	}
	if job.AppID != resolved.AppID || !handler.validKnowledgeSearchJobProjection(job) {
		return nil, internalError()
	}
	converted, err := searchJobToProto(job, handler.now())
	if err != nil {
		return nil, internalError()
	}
	return &opensplunk.CreateSearchJobResponse{SearchJob: converted}, nil
}

// resolveSavedSearchLaunch makes the persisted reusable definition—not a
// caller-supplied copy—the execution authority. The request definition is
// accepted for backwards-compatible clients that still send their local view,
// but only its app identity is compared; SPL, time range, and index scope come
// exclusively from the cloned saved record.
func (handler *apiHandler) resolveSavedSearchLaunch(
	ctx context.Context,
	input *opensplunk.CreateSearchJobRequest,
) (resolvedSearchDefinition, searchjobs.JobSource, error) {
	sourceInput := input.GetSource()
	if sourceInput.GetOrigin() != opensplunk.SearchJobOrigin_SEARCH_JOB_ORIGIN_SAVED_SEARCH ||
		sourceInput.SavedSearchId == nil || sourceInput.HistorySearchId != nil ||
		sourceInput.DashboardId != nil {
		return resolvedSearchDefinition{}, searchjobs.JobSource{},
			badRequestError("search job source metadata is invalid or unsupported")
	}
	savedID, err := savedSearchID(sourceInput.GetSavedSearchId())
	if err != nil {
		return resolvedSearchDefinition{}, searchjobs.JobSource{}, badRequestError(err.Error())
	}
	record, err := handler.savedSearches.Get(ctx, handler.savedSearchScope(), savedID)
	if mapped := mapSavedSearchCallError(ctx, err); mapped != nil {
		return resolvedSearchDefinition{}, searchjobs.JobSource{}, mapped
	}
	trusted, err := handler.cloneSavedSearch(record)
	if err != nil || trusted.GetSavedSearchId() != savedID ||
		trusted.GetDefinition().GetSearch() == nil {
		return resolvedSearchDefinition{}, searchjobs.JobSource{}, internalError()
	}
	resolved, err := handler.resolveSearchDefinition(trusted.GetDefinition().GetSearch())
	if err != nil || resolved.AppID != savedSearchAppID(trusted) {
		return resolvedSearchDefinition{}, searchjobs.JobSource{}, internalError()
	}
	if client := input.GetDefinition(); client != nil {
		clientApp, appErr := normalizeSearchAppID(client.GetAppId())
		if appErr != nil {
			return resolvedSearchDefinition{}, searchjobs.JobSource{},
				badRequestError("search app ID is invalid")
		}
		if clientApp != "" && clientApp != resolved.AppID {
			return resolvedSearchDefinition{}, searchjobs.JobSource{},
				badRequestError("search app ID does not match the saved search")
		}
	}
	return resolved, searchjobs.JobSource{
		Origin: searchjobs.JobOriginSavedSearch, ObjectID: savedID,
	}, nil
}

func historyRerunContextError(
	ctx context.Context,
	operationErr error,
	historyRerun bool,
) error {
	if !historyRerun || requestContextFailure(ctx, operationErr) == nil {
		return nil
	}
	return router.NewHTTPError(
		http.StatusRequestTimeout,
		"history rerun request was canceled",
	)
}

func (handler *apiHandler) resolveHistoryRerun(
	ctx context.Context,
	input *opensplunk.CreateSearchJobRequest,
) (resolvedSearchDefinition, searchjobs.JobSource, error) {
	if input.GetDefinition() != nil {
		return resolvedSearchDefinition{}, searchjobs.JobSource{},
			badRequestError("history rerun cannot include a client search definition")
	}
	source := input.GetSource()
	if source == nil || source.GetOrigin() != opensplunk.SearchJobOrigin_SEARCH_JOB_ORIGIN_HISTORY_RERUN ||
		source.HistorySearchId == nil || source.SavedSearchId != nil || source.DashboardId != nil {
		return resolvedSearchDefinition{}, searchjobs.JobSource{},
			badRequestError("history-rerun origin requires a history search ID")
	}
	historyID, err := historySearchJobID(source.GetHistorySearchId())
	if err != nil {
		return resolvedSearchDefinition{}, searchjobs.JobSource{},
			badRequestError(err.Error())
	}
	if handler.searchHistory == nil {
		return resolvedSearchDefinition{}, searchjobs.JobSource{},
			badRequestError("history rerun is not supported")
	}

	entry, err := handler.searchHistory.Get(
		ctx,
		handler.searchHistoryScope(),
		historyID,
	)
	if contextErr := requestContextFailure(ctx, err); contextErr != nil {
		return resolvedSearchDefinition{}, searchjobs.JobSource{}, contextErr
	}
	if mapped := mapSearchHistoryCallError(ctx, err); mapped != nil {
		return resolvedSearchDefinition{}, searchjobs.JobSource{}, mapped
	}
	trusted, err := cloneSearchHistoryEntry(entry)
	if err != nil || trusted.GetSearchJobId() != historyID {
		return resolvedSearchDefinition{}, searchjobs.JobSource{}, internalError()
	}
	definition, err := trustedHistoryRerunDefinition(trusted)
	if err != nil {
		return resolvedSearchDefinition{}, searchjobs.JobSource{}, internalError()
	}
	resolvedRange, err := resolveSearchTimeRange(
		definition.GetTimeRange(),
		handler.now(),
	)
	if err != nil {
		return resolvedSearchDefinition{}, searchjobs.JobSource{},
			router.NewHTTPError(
				http.StatusConflict,
				"retained search time range is not executable at the current server time",
			)
	}
	resolved := resolvedSearchDefinition{
		SPL:        definition.GetSpl(),
		TimeRange:  resolvedRange,
		AppID:      definition.GetAppId(),
		IndexScope: slices.Clone(definition.GetIndexScope()),
	}
	if err := ctx.Err(); err != nil {
		return resolvedSearchDefinition{}, searchjobs.JobSource{}, err
	}
	return resolved, searchjobs.JobSource{
		Origin:   searchjobs.JobOriginHistoryRerun,
		ObjectID: historyID,
	}, nil
}

func trustedHistoryRerunDefinition(
	entry *opensplunk.SearchHistoryEntry,
) (*opensplunk.SearchDefinition, error) {
	stored := entry.GetDefinition()
	if stored == nil || stored.GetTimeRange() == nil ||
		stored.GetTimeRange().Earliest == nil || stored.GetTimeRange().Latest == nil {
		return nil, errors.New("history entry does not contain reusable time intent")
	}
	if strings.TrimSpace(stored.GetSpl()) == "" ||
		strings.IndexByte(stored.GetSpl(), 0) >= 0 {
		return nil, errors.New("history entry contains invalid SPL")
	}
	for _, value := range []*string{
		stored.GetTimeRange().Earliest,
		stored.GetTimeRange().Latest,
		stored.GetTimeRange().Timezone,
	} {
		if value != nil && strings.TrimSpace(*value) != *value {
			return nil, errors.New("history entry contains noncanonical time intent")
		}
	}
	timeIntent := searchtime.Intent{
		Earliest: stored.GetTimeRange().GetEarliest(),
		Latest:   stored.GetTimeRange().GetLatest(),
		Timezone: "UTC",
	}
	if stored.GetTimeRange().Timezone != nil {
		timeIntent.Timezone = stored.GetTimeRange().GetTimezone()
		timeIntent.TimezoneSpecified = true
	}
	if err := searchtime.ValidateIntent(timeIntent); err != nil {
		return nil, errors.New("history entry contains invalid time intent")
	}
	if stored.AppId != nil {
		appID, err := normalizeSearchAppID(stored.GetAppId())
		if err != nil || appID != stored.GetAppId() {
			return nil, errors.New("history entry contains a noncanonical app ID")
		}
	}

	requested, err := normalizeRequestedIndexes(stored.GetIndexScope())
	if err != nil || !slices.Equal(requested, stored.GetIndexScope()) {
		return nil, errors.New("history entry contains a noncanonical requested index scope")
	}
	effective := requested
	if len(entry.GetEffectiveIndexScope()) != 0 {
		effective, err = normalizeRequestedIndexes(entry.GetEffectiveIndexScope())
		if err != nil || !slices.Equal(effective, entry.GetEffectiveIndexScope()) {
			return nil, errors.New("history entry contains a noncanonical effective index scope")
		}
		requestedSet := make(map[string]struct{}, len(requested))
		for _, name := range requested {
			requestedSet[name] = struct{}{}
		}
		for _, name := range effective {
			if _, allowed := requestedSet[name]; !allowed {
				return nil, errors.New("history entry effective index scope exceeds its requested scope")
			}
		}
	}
	if len(effective) == 0 {
		return nil, errors.New("history entry does not contain a reusable index scope")
	}

	return &opensplunk.SearchDefinition{
		Spl: strings.Clone(stored.GetSpl()),
		TimeRange: &opensplunk.TimeRangeSpec{
			Earliest: cloneOptionalString(stored.GetTimeRange().Earliest),
			Latest:   cloneOptionalString(stored.GetTimeRange().Latest),
			Timezone: cloneOptionalString(stored.GetTimeRange().Timezone),
		},
		AppId:      cloneOptionalString(stored.AppId),
		IndexScope: slices.Clone(effective),
	}, nil
}

func (handler *apiHandler) authorizeSearchApp(
	ctx context.Context,
	appID string,
) error {
	if appID == "" {
		return nil
	}
	apps := handler.bootstrap.Apps
	if handler.appCatalog != nil {
		catalog, err := handler.appCatalog.ListActiveApps(
			ctx,
			handler.tenantID,
			uint32(maximumBootstrapApps),
		)
		if contextErr := requestContextFailure(ctx, err); contextErr != nil {
			return contextErr
		}
		if err != nil || !catalog.Complete || len(catalog.Apps) > maximumBootstrapApps {
			return unavailableError("control plane is unavailable")
		}
		apps, err = appCatalogSummariesToProto(catalog)
		if err != nil {
			return internalError()
		}
	}
	if !activeHistoryRerunAppExists(apps, appID) {
		return forbiddenError("search app is unavailable")
	}
	return nil
}

func activeHistoryRerunAppExists(
	apps []*opensplunk.AppSummary,
	appID string,
) bool {
	for _, app := range apps {
		if app.GetAppId() == appID &&
			app.GetState() == opensplunk.AppState_APP_STATE_ACTIVE {
			return true
		}
	}
	return false
}

func (handler *apiHandler) getSearchJob(request *http.Request, input *opensplunk.GetSearchJobRequest) (*opensplunk.GetSearchJobResponse, error) {
	id := input.GetSearchJobId()
	job, err := handler.jobs.GetFor(handler.accessScope(), id)
	if contextErr := requestContextFailure(request.Context(), err); contextErr != nil {
		return nil, contextErr
	}
	if errors.Is(err, searchjobs.ErrNotFound) && handler.searchArtifacts != nil {
		record, artifactErr := handler.searchArtifacts.Get(request.Context(), handler.accessScope(), id, searchartifacts.AccessLaunch)
		if artifactErr != nil {
			return nil, mapSearchArtifactError(artifactErr)
		}
		converted, projectionErr := retainedSearchJobToProto(record, handler.now())
		if projectionErr != nil {
			return nil, internalError()
		}
		return &opensplunk.GetSearchJobResponse{SearchJob: converted}, nil
	}
	if err != nil {
		return nil, mapSearchJobError(err)
	}
	if !handler.validKnowledgeSearchJobProjection(job) {
		return nil, internalError()
	}
	var converted *opensplunk.SearchJob
	if handler.searchArtifacts != nil {
		record, artifactErr := handler.searchArtifacts.Get(request.Context(), handler.accessScope(), id, searchartifacts.AccessLaunch)
		if artifactErr != nil {
			return nil, mapSearchArtifactError(artifactErr)
		}
		record, artifactErr = handler.overlayLiveSearchJob(request.Context(), record)
		if artifactErr != nil {
			return nil, artifactErr
		}
		converted, err = retainedSearchJobToProto(record, handler.now())
	} else {
		converted, err = searchJobToProto(job, handler.now())
	}
	if err != nil {
		return nil, internalError()
	}
	if err := request.Context().Err(); err != nil {
		return nil, err
	}
	// Generated SQL is intentionally not present in the search-job layer and is
	// never accepted from or exposed to an ordinary browser request.
	return &opensplunk.GetSearchJobResponse{SearchJob: converted}, nil
}

func (handler *apiHandler) getSearchResults(request *http.Request, input *opensplunk.GetSearchResultsRequest) (*serializedSearchResultsResponse, error) {
	id := input.GetSearchJobId()
	requestPage := input.GetPage()
	pageSize := int(requestPage.GetPageSize())
	pageToken := requestPage.GetPageToken()
	includeTotal := requestPage.GetIncludeTotalSize()
	job, err := handler.jobs.GetFor(handler.accessScope(), id)
	if contextErr := requestContextFailure(request.Context(), err); contextErr != nil {
		return nil, contextErr
	}
	durable := handler.searchArtifacts != nil && (errors.Is(err, searchjobs.ErrNotFound) ||
		(err == nil && job.State == searchjobs.StateExpired))
	if err != nil {
		if !durable {
			return nil, mapSearchJobError(err)
		}
	}
	if !durable && handler.searchArtifacts != nil {
		record, artifactErr := handler.searchArtifacts.Get(request.Context(), handler.accessScope(), id, searchartifacts.AccessRefresh)
		if artifactErr != nil {
			return nil, mapSearchArtifactError(artifactErr)
		}
		// A terminal durable projection is authoritative for retained-result
		// availability. In particular, publication compensation may mark the
		// artifact failed while the in-memory executor remains completed; that
		// case is reported as a distinct, permanent error rather than "not
		// ready" so clients do not wait for results that will never appear.
		if terminalErr := searchartifacts.TerminalResultError(record); terminalErr != nil {
			return nil, mapSearchArtifactError(terminalErr)
		}
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
	var page searchjobs.ResultPage
	if durable {
		page, job, err = handler.durableResultPage(request.Context(), id, pageSize, pageToken)
	} else {
		page, err = handler.jobs.ResultsFor(handler.accessScope(), id, searchjobs.PageRequest{
			Limit:  pageSize,
			Cursor: pageToken,
		})
		// The in-memory manager and durable store have independent retention
		// clocks. Sharing can keep the artifact alive for seven days after the
		// manager's original manual lifetime elapses. If that boundary crosses
		// between the metadata read and result acquisition, finish from the
		// already-refreshed durable snapshot instead of reporting a false 410.
		if errors.Is(err, searchjobs.ErrExpired) && handler.searchArtifacts != nil {
			page, job, err = handler.durableResultPage(request.Context(), id, pageSize, pageToken)
		}
	}
	if contextErr := requestContextFailure(request.Context(), err); contextErr != nil {
		return nil, contextErr
	}
	if err != nil {
		return nil, mapSearchJobError(err)
	}
	converted, err := searchjobproto.ResultPage(request.Context(), id, page, searchjobproto.ResultShapeForSPL(job.SPL), includeTotal, job.ResultsTruncated)
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
		message: &opensplunk.GetSearchResultsResponse{SearchJobId: id, ResultPage: converted},
		ctx:     request.Context(),
		release: release,
	}, nil
}

func (handler *apiHandler) cancelSearchJob(request *http.Request, input *opensplunk.CancelSearchJobRequest) (*opensplunk.CancelSearchJobResponse, error) {
	id := input.GetSearchJobId()
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
	if !handler.validKnowledgeSearchJobProjection(job) {
		return nil, internalError()
	}
	converted, err := searchJobToProto(job, handler.now())
	if err != nil {
		return nil, internalError()
	}
	return &opensplunk.CancelSearchJobResponse{SearchJob: converted}, nil
}

func (handler *apiHandler) accessScope() searchjobs.AccessScope {
	return searchjobs.AccessScope{TenantID: handler.tenantID, OwnerID: handler.ownerID}
}

func (handler *apiHandler) validKnowledgeSearchJobProjection(job searchjobs.Job) bool {
	if !handler.knowledgeSearchAdmission {
		return job.KnowledgeSnapshot == nil
	}
	return (job.AppID == "") == (job.KnowledgeSnapshot == nil)
}

type resolvedSearchDefinition struct {
	SPL        string
	TimeRange  searchtime.Range
	AppID      string
	IndexScope []string
}

func (handler *apiHandler) resolveSearchDefinition(
	definition *opensplunk.SearchDefinition,
) (resolvedSearchDefinition, error) {
	if definition == nil {
		return resolvedSearchDefinition{}, badRequestError("search definition is required")
	}
	source := definition.GetSpl()
	if strings.TrimSpace(source) == "" {
		return resolvedSearchDefinition{}, badRequestError("SPL is required")
	}
	if strings.IndexByte(source, 0) >= 0 {
		return resolvedSearchDefinition{}, badRequestError("SPL cannot contain NUL bytes")
	}
	resolvedRange, err := resolveSearchTimeRange(definition.GetTimeRange(), handler.now())
	if err != nil {
		return resolvedSearchDefinition{}, badRequestError(err.Error())
	}
	appID, err := normalizeSearchAppID(definition.GetAppId())
	if err != nil {
		return resolvedSearchDefinition{}, badRequestError("search app ID is invalid")
	}
	return resolvedSearchDefinition{
		SPL:        source,
		TimeRange:  resolvedRange,
		AppID:      appID,
		IndexScope: slices.Clone(definition.GetIndexScope()),
	}, nil
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

// canceledRequestError maps a canceled or expired request context onto the
// single 408 shape every list endpoint returns.
func canceledRequestError(ctx context.Context, message string) error {
	if ctx != nil && ctx.Err() != nil {
		return router.NewHTTPError(http.StatusRequestTimeout, message)
	}
	return nil
}

var (
	errIndexUnavailable         = errors.New("requested index is unavailable")
	errInvalidIndexCatalogBatch = errors.New("index catalog returned an invalid batch result")
)

type batchIndexCatalog interface {
	GetIndexesByNames(context.Context, []string) ([]control.Index, error)
}

var _ batchIndexCatalog = (*control.DB)(nil)

func (handler *apiHandler) resolveAuthorizedSearchIndexes(ctx context.Context, scope []string) ([]string, error) {
	requestedIndexes, err := normalizeRequestedIndexes(scope)
	if err != nil {
		return nil, badRequestError(err.Error())
	}
	if len(requestedIndexes) == 0 {
		return nil, badRequestError("at least one index is required")
	}
	if err := handler.authorizeRequestedIndexes(ctx, requestedIndexes); err != nil {
		if contextErr := requestContextFailure(ctx, err); contextErr != nil {
			return nil, contextErr
		}
		if errors.Is(err, errIndexUnavailable) {
			return nil, forbiddenError("index scope contains an unavailable index")
		}
		return nil, unavailableError("control plane is unavailable")
	}
	return requestedIndexes, nil
}

func (handler *apiHandler) authorizeRequestedIndexes(ctx context.Context, requested []string) error {
	if catalog, ok := handler.indexes.(batchIndexCatalog); ok {
		records, err := catalog.GetIndexesByNames(ctx, slices.Clone(requested))
		if errors.Is(err, control.ErrNotFound) {
			return errIndexUnavailable
		}
		if err != nil {
			return err
		}
		if len(records) != len(requested) {
			return errInvalidIndexCatalogBatch
		}
		remainingRecords := records
		for _, requestedName := range requested {
			record := remainingRecords[0]
			remainingRecords = remainingRecords[1:]
			if record.Definition.Name != requestedName {
				return errInvalidIndexCatalogBatch
			}
			if !isSearchableIndexRecord(record, requestedName) {
				return errIndexUnavailable
			}
		}
		return nil
	}

	for _, name := range requested {
		record, err := handler.indexes.GetIndexByName(ctx, name)
		if errors.Is(err, control.ErrNotFound) {
			return errIndexUnavailable
		}
		if err != nil {
			return err
		}
		if !isSearchableIndexRecord(record, name) {
			return errIndexUnavailable
		}
	}
	return nil
}

func isSearchableIndexRecord(record control.Index, requestedName string) bool {
	return record.State == control.IndexStateActive &&
		record.Definition.SearchEnabled &&
		record.Definition.Name == requestedName
}

func (handler *apiHandler) pageRequest(page *opensplunk.PageRequest) (int, string, bool, error) {
	pageSize := uint32(0)
	if page == nil {
		return 0, "", false, nil
	}
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

func resolveSearchTimeRange(spec *opensplunk.TimeRangeSpec, now time.Time) (searchtime.Range, error) {
	if spec == nil || spec.Earliest == nil || spec.Latest == nil {
		return searchtime.Range{}, errors.New("earliest and latest time expressions are required")
	}
	return searchtime.Resolve(spec.GetEarliest(), spec.GetLatest(), spec.Timezone, now)
}

func (handler *apiHandler) resolveSearchJobSource(
	ctx context.Context,
	requestedAppID string,
	input *opensplunk.SearchJobSource,
) (string, searchjobs.JobSource, error) {
	appID, err := normalizeSearchAppID(requestedAppID)
	if err != nil {
		return "", searchjobs.JobSource{}, badRequestError("search app ID is invalid")
	}
	if input == nil || input.GetOrigin() == opensplunk.SearchJobOrigin_SEARCH_JOB_ORIGIN_UNSPECIFIED &&
		input.SavedSearchId == nil && input.HistorySearchId == nil && input.DashboardId == nil {
		return appID, searchjobs.JobSource{Origin: searchjobs.JobOriginAdHoc}, nil
	}
	if input.GetOrigin() == opensplunk.SearchJobOrigin_SEARCH_JOB_ORIGIN_AD_HOC {
		if input.SavedSearchId != nil || input.HistorySearchId != nil || input.DashboardId != nil {
			return "", searchjobs.JobSource{}, badRequestError("ad-hoc search source cannot include an object ID")
		}
		return appID, searchjobs.JobSource{Origin: searchjobs.JobOriginAdHoc}, nil
	}
	if input.GetOrigin() != opensplunk.SearchJobOrigin_SEARCH_JOB_ORIGIN_SAVED_SEARCH ||
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

func normalizeSearchAppID(requested string) (string, error) {
	appID := strings.TrimSpace(requested)
	if err := validateBoundedIdentifier(appID, maximumSavedSearchAppIDBytes, true); err != nil {
		return "", err
	}
	return appID, nil
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
	if transportError, ok := errors.AsType[*router.HTTPError](err); ok {
		return transportError
	}
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
	case errors.Is(err, searchjobs.ErrInvalidSPL):
		return badRequestError("search SPL is invalid")
	case errors.Is(err, searchjobs.ErrUnsupportedSPL):
		return router.NewHTTPError(http.StatusUnprocessableEntity, "search SPL is unsupported")
	case errors.Is(err, searchjobs.ErrQueueFull):
		return router.NewHTTPError(http.StatusTooManyRequests, "search queue is full")
	case errors.Is(err, searchjobs.ErrCapacity), errors.Is(err, searchjobs.ErrClosed), errors.Is(err, searchjobs.ErrStorageUnavailable), errors.Is(err, searchjobs.ErrJournalUnavailable), errors.Is(err, searchjobs.ErrKnowledgeUnavailable):
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
