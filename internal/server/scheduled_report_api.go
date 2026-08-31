package server

import (
	"context"
	"errors"
	"net/http"
	"time"

	"fortio.org/safecast"
	"github.com/Suhaibinator/SRouter/pkg/codec"
	sroutercommon "github.com/Suhaibinator/SRouter/pkg/common"
	"github.com/Suhaibinator/SRouter/pkg/router"
	"google.golang.org/protobuf/types/known/timestamppb"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/scheduledreports"
	"github.com/Suhaibinator/open-splunk/internal/searchartifacts"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
)

const scheduledReportRunPageSize = uint32(scheduledreports.RunHistoryLimit)

type searchArtifactMetadataBatchInspector interface {
	InspectMany(context.Context, searchjobs.AccessScope, []string) (map[string]searchartifacts.Record, error)
}

func (handler *apiHandler) scheduledReportRoutes(noAuth router.AuthLevel, smallRequestBytes int64) []router.RouteDefinition {
	return []router.RouteDefinition{
		router.RouteConfig[*opensplunk.SetSavedSearchScheduleRequest, *opensplunk.SetSavedSearchScheduleResponse]{
			Path: "/saved-searches/schedule/set", Methods: []router.HttpMethod{router.MethodPost}, AuthLevel: &noAuth,
			Codec: codec.NewProtoCodec[*opensplunk.SetSavedSearchScheduleRequest, *opensplunk.SetSavedSearchScheduleResponse](), Handler: handler.setSavedSearchSchedule,
			SourceType: router.Body, Overrides: sroutercommon.RouteOverrides{MaxBodySize: smallRequestBytes},
			Sanitizer: sanitizeSetSavedSearchScheduleRequest,
		},
		router.RouteConfig[*opensplunk.RunSavedSearchRequest, *opensplunk.RunSavedSearchResponse]{
			Path: "/saved-searches/run", Methods: []router.HttpMethod{router.MethodPost}, AuthLevel: &noAuth,
			Codec: codec.NewProtoCodec[*opensplunk.RunSavedSearchRequest, *opensplunk.RunSavedSearchResponse](), Handler: handler.runSavedSearch,
			SourceType: router.Body, Overrides: sroutercommon.RouteOverrides{MaxBodySize: smallRequestBytes},
			Sanitizer: sanitizeRunSavedSearchRequest,
		},
		router.RouteConfig[*opensplunk.ListScheduledSearchRunsRequest, *opensplunk.ListScheduledSearchRunsResponse]{
			Path: "/saved-searches/runs/list", Methods: []router.HttpMethod{router.MethodPost}, AuthLevel: &noAuth,
			Codec: codec.NewProtoCodec[*opensplunk.ListScheduledSearchRunsRequest, *opensplunk.ListScheduledSearchRunsResponse](), Handler: handler.listScheduledSearchRuns,
			SourceType: router.Body, Overrides: sroutercommon.RouteOverrides{MaxBodySize: smallRequestBytes},
			Sanitizer: handler.sanitizeListScheduledSearchRunsRequest,
		},
	}
}

func (handler *apiHandler) setSavedSearchSchedule(request *http.Request, input *opensplunk.SetSavedSearchScheduleRequest) (*opensplunk.SetSavedSearchScheduleResponse, error) {
	id := input.GetSavedSearchId()
	schedule := input.GetSchedule()
	record, err := handler.savedSearches.Get(request.Context(), handler.savedSearchScope(), id)
	if mapped := mapSavedSearchCallError(request.Context(), err); mapped != nil {
		return nil, mapped
	}
	if record.GetVersion() != input.GetExpectedVersion() {
		return nil, router.NewHTTPError(http.StatusConflict, "saved search version conflicts with current state")
	}
	configured, err := handler.scheduledReports.Configure(request.Context(), handler.ownerID, handler.tenantID, id, input.GetExpectedScheduleVersion(), scheduledreports.Configuration{
		Cron: schedule.GetCron(), Timezone: schedule.GetTimezone(), DispatchTTL: schedule.GetDispatchTtl(), Enabled: schedule.GetEnabled(),
	})
	if err != nil {
		return nil, mapScheduledReportError(err)
	}
	projected, err := handler.cloneSavedSearch(record)
	if err != nil {
		return nil, internalError()
	}
	applyScheduledReportProjection(projected, configured, nil, nil, handler.now())
	return &opensplunk.SetSavedSearchScheduleResponse{SavedSearch: projected}, nil
}

func (handler *apiHandler) runSavedSearch(request *http.Request, input *opensplunk.RunSavedSearchRequest) (*opensplunk.RunSavedSearchResponse, error) {
	run, err := handler.scheduledReports.RunNowOrOneOff(
		request.Context(), handler.ownerID, handler.tenantID, input.GetSavedSearchId(),
		scheduledreports.DefaultOneOffSchedulePeriod,
	)
	if err != nil {
		return nil, mapScheduledReportError(err)
	}
	return &opensplunk.RunSavedSearchResponse{ScheduledSearchRunId: run.RunID, SearchJobId: run.SearchJobID}, nil
}

func (handler *apiHandler) listScheduledSearchRuns(request *http.Request, input *opensplunk.ListScheduledSearchRunsRequest) (*opensplunk.ListScheduledSearchRunsResponse, error) {
	pageSize := input.GetPage().GetPageSize()
	pageToken := input.GetPage().GetPageToken()
	includeTotal := input.GetPage().GetIncludeTotalSize()
	result, err := handler.scheduledReports.ListRunPage(request.Context(), handler.ownerID, input.GetSavedSearchId(), scheduledreports.RunPageRequest{
		Limit: int(pageSize), PageToken: pageToken, IncludeTotal: includeTotal,
	})
	if err != nil {
		return nil, mapScheduledReportError(err)
	}
	retainedByJobID := make(map[string]searchartifacts.Record)
	if inspector, ok := handler.searchArtifacts.(searchArtifactMetadataBatchInspector); ok {
		jobIDs := make([]string, 0, len(result.Runs))
		for _, run := range result.Runs {
			if run.SearchJobID != "" {
				jobIDs = append(jobIDs, run.SearchJobID)
			}
		}
		retainedByJobID, err = inspector.InspectMany(request.Context(), handler.accessScope(), jobIDs)
		if err != nil {
			return nil, mapSearchArtifactError(err)
		}
	}
	projected := make([]*opensplunk.ScheduledSearchRun, len(result.Runs))
	for index, run := range result.Runs {
		retained, retainedAvailable := retainedByJobID[run.SearchJobID]
		projected[index] = scheduledReportRunToProto(run, retained, retainedAvailable, handler.now())
	}
	page, err := boundedListPageResponse("scheduled run", boundedListPageMetadata{
		itemCount: len(projected), nextPageToken: result.NextPageToken,
		totalSize: result.TotalSize, totalExact: includeTotal,
	}, int(pageSize), pageToken, includeTotal, scheduledreports.MaximumRunHistoryCursorBytes)
	if err != nil {
		return nil, internalError()
	}
	return &opensplunk.ListScheduledSearchRunsResponse{Runs: projected, Page: page}, nil
}

func (handler *apiHandler) augmentScheduledSearch(ctx context.Context, record *opensplunk.SavedSearch) (*opensplunk.SavedSearch, error) {
	if record == nil {
		return record, nil
	}
	err := handler.augmentScheduledSearches(ctx, []*opensplunk.SavedSearch{record})
	if err != nil {
		return nil, err
	}
	return record, nil
}

func (handler *apiHandler) augmentScheduledSearches(ctx context.Context, records []*opensplunk.SavedSearch) error {
	if handler.scheduledReports == nil || len(records) == 0 {
		return nil
	}
	ids := make([]string, 0, len(records))
	for _, record := range records {
		if record != nil {
			ids = append(ids, record.GetSavedSearchId())
		}
	}
	projections, err := handler.scheduledReports.CurrentProjections(ctx, handler.ownerID, ids)
	if err != nil {
		return err
	}
	retainedByJobID := make(map[string]searchartifacts.Record)
	if inspector, ok := handler.searchArtifacts.(searchArtifactMetadataBatchInspector); ok {
		jobIDs := make([]string, 0, len(projections))
		for _, projection := range projections {
			if projection.LatestResultRun != nil && projection.LatestResultRun.SearchJobID != "" {
				jobIDs = append(jobIDs, projection.LatestResultRun.SearchJobID)
			}
		}
		retainedByJobID, err = inspector.InspectMany(ctx, handler.accessScope(), jobIDs)
		if err != nil {
			return err
		}
	}
	projectionNow := handler.now()
	for _, record := range records {
		if record == nil {
			continue
		}
		projection, scheduled := projections[record.GetSavedSearchId()]
		if !scheduled {
			continue
		}
		applyScheduledReportProjection(
			record, projection.Schedule, projection.LatestRun, projection.LatestResultRun, projectionNow,
		)
		if record.ScheduleStatus == nil || projection.LatestResultRun == nil ||
			projection.LatestResultRun.SearchJobID == "" || handler.searchArtifacts == nil {
			continue
		}
		if retained, ok := retainedByJobID[projection.LatestResultRun.SearchJobID]; ok {
			applyRetainedResultToScheduleStatus(record.ScheduleStatus, retained)
			continue
		}
		if _, batched := handler.searchArtifacts.(searchArtifactMetadataBatchInspector); batched {
			continue
		}
		retained, retainedErr := handler.searchArtifacts.Get(ctx, handler.accessScope(), projection.LatestResultRun.SearchJobID, searchartifacts.AccessInspect)
		if retainedErr == nil {
			applyRetainedResultToScheduleStatus(record.ScheduleStatus, retained)
		} else if !errors.Is(retainedErr, searchartifacts.ErrExpired) && !errors.Is(retainedErr, searchartifacts.ErrNotFound) {
			return retainedErr
		}
	}
	return nil
}

func applyScheduledReportProjection(
	record *opensplunk.SavedSearch,
	schedule scheduledreports.Schedule,
	latestRun, latestResultRun *scheduledreports.Run,
	now time.Time,
) {
	if record.Definition == nil {
		return
	}
	record.Definition.Schedule = &opensplunk.SavedSearchSchedule{
		Enabled: schedule.Enabled, Cron: schedule.Cron, Timezone: schedule.Timezone,
		DispatchTtl: schedule.DispatchTTL, ConfigVersion: schedule.ConfigVersion,
	}
	status := &opensplunk.ScheduledSearchStatus{}
	if schedule.NextRunAt != nil {
		status.NextRunAt = timestamppb.New(*schedule.NextRunAt)
	}
	if latestRun != nil {
		status.LastRunAt = timestamppb.New(latestRun.ScheduledAt)
		status.LastOutcome = scheduledReportOutcomeToProto(latestRun.Outcome)
	}
	if latestResultRun != nil && latestResultRun.SearchJobID != "" {
		status.LatestSearchJobId = new(latestResultRun.SearchJobID)
		projection := retainedResultProjectionForRun(*latestResultRun, now)
		status.LatestRetainedResultStatus = projection.status
		status.LatestResultExpiresAt = projection.expiresAt
	}
	record.ScheduleStatus = status
}

func scheduledReportRunToProto(run scheduledreports.Run, retained searchartifacts.Record, retainedAvailable bool, now time.Time) *opensplunk.ScheduledSearchRun {
	skippedOccurrenceCount := uint32(0)
	if run.SkippedOccurrenceCount > 0 {
		skippedOccurrenceCount = safecast.MustConv[uint32](run.SkippedOccurrenceCount)
	}
	result := &opensplunk.ScheduledSearchRun{
		ScheduledSearchRunId: run.RunID, SavedSearchId: run.SavedSearchID,
		ScheduledAt: timestamppb.New(run.ScheduledAt), StartedAt: timestamppb.New(run.ClaimedAt),
		Outcome: scheduledReportOutcomeToProto(run.Outcome), SkippedOccurrenceCount: skippedOccurrenceCount,
	}
	if run.SearchJobID != "" {
		result.SearchJobId = new(run.SearchJobID)
		projection := retainedResultProjectionForRun(run, now)
		if retainedAvailable {
			projection = retainedResultProjectionForArtifact(retained)
		}
		result.SearchJobExpiresAt = projection.expiresAt
		result.RetainedResultStatus = projection.status
	}
	if run.FinishedAt != nil {
		result.FinishedAt = timestamppb.New(*run.FinishedAt)
	}
	return result
}

type retainedResultProjection struct {
	status    opensplunk.RetainedResultStatus
	expiresAt *timestamppb.Timestamp
}

func retainedResultProjectionForArtifact(record searchartifacts.Record) retainedResultProjection {
	result := retainedResultProjection{status: retainedResultStatusToProto(record),
		expiresAt: validRetainedResultExpiry(record.ExpiresAt)}
	if result.expiresAt == nil {
		if result.status == opensplunk.RetainedResultStatus_RETAINED_RESULT_STATUS_AVAILABLE ||
			result.status == opensplunk.RetainedResultStatus_RETAINED_RESULT_STATUS_EXPIRED {
			result.status = opensplunk.RetainedResultStatus_RETAINED_RESULT_STATUS_MISSING
		}
		return result
	}
	return result
}

func retainedResultProjectionForRun(run scheduledreports.Run, now time.Time) retainedResultProjection {
	result := retainedResultProjection{status: opensplunk.RetainedResultStatus_RETAINED_RESULT_STATUS_MISSING}
	if run.Outcome == scheduledreports.RunOutcomeClaimed || run.Outcome == scheduledreports.RunOutcomeSubmitted {
		result.status = opensplunk.RetainedResultStatus_RETAINED_RESULT_STATUS_PENDING
		return result
	}
	if run.RetentionLifetime <= 0 || run.ClaimedAt.IsZero() {
		return result
	}
	expiresAt := run.ClaimedAt.Add(run.RetentionLifetime)
	result.expiresAt = validRetainedResultExpiry(expiresAt)
	if result.expiresAt == nil {
		return result
	}
	if !now.Before(expiresAt) {
		result.status = opensplunk.RetainedResultStatus_RETAINED_RESULT_STATUS_EXPIRED
	}
	return result
}

func validRetainedResultExpiry(value time.Time) *timestamppb.Timestamp {
	if value.IsZero() {
		return nil
	}
	result := timestamppb.New(value)
	if result.CheckValid() != nil {
		return nil
	}
	return result
}

func applyRetainedResultToScheduleStatus(status *opensplunk.ScheduledSearchStatus, record searchartifacts.Record) {
	if status == nil {
		return
	}
	projection := retainedResultProjectionForArtifact(record)
	status.LatestRetainedResultStatus = projection.status
	status.LatestResultExpiresAt = projection.expiresAt
}

func scheduledReportOutcomeToProto(outcome scheduledreports.RunOutcome) opensplunk.ScheduledSearchOutcome {
	switch outcome {
	case scheduledreports.RunOutcomeClaimed, scheduledreports.RunOutcomeSubmitted:
		return opensplunk.ScheduledSearchOutcome_SCHEDULED_SEARCH_OUTCOME_RUNNING
	case scheduledreports.RunOutcomeSucceeded:
		return opensplunk.ScheduledSearchOutcome_SCHEDULED_SEARCH_OUTCOME_COMPLETED
	case scheduledreports.RunOutcomeFailed, scheduledreports.RunOutcomeExpired:
		return opensplunk.ScheduledSearchOutcome_SCHEDULED_SEARCH_OUTCOME_FAILED
	case scheduledreports.RunOutcomeCanceled:
		return opensplunk.ScheduledSearchOutcome_SCHEDULED_SEARCH_OUTCOME_CANCELED
	case scheduledreports.RunOutcomeSkippedOverlap:
		return opensplunk.ScheduledSearchOutcome_SCHEDULED_SEARCH_OUTCOME_SKIPPED_OVERLAP
	case scheduledreports.RunOutcomeInterrupted:
		return opensplunk.ScheduledSearchOutcome_SCHEDULED_SEARCH_OUTCOME_INTERRUPTED
	default:
		return opensplunk.ScheduledSearchOutcome_SCHEDULED_SEARCH_OUTCOME_UNSPECIFIED
	}
}

func mapScheduledReportError(err error) error {
	switch {
	case errors.Is(err, scheduledreports.ErrInvalidArgument):
		return badRequestError("scheduled report configuration is invalid")
	case errors.Is(err, scheduledreports.ErrNotFound):
		return router.NewHTTPError(http.StatusNotFound, "scheduled report not found")
	case errors.Is(err, scheduledreports.ErrConflict):
		return router.NewHTTPError(http.StatusConflict, "scheduled report conflicts with current state")
	default:
		return unavailableError("scheduled report service is unavailable")
	}
}
