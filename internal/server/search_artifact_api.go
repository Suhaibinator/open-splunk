package server

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/Suhaibinator/SRouter/pkg/codec"
	sroutercommon "github.com/Suhaibinator/SRouter/pkg/common"
	"github.com/Suhaibinator/SRouter/pkg/router"
	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/cursorcodec"
	"github.com/Suhaibinator/open-splunk/internal/searchartifacts"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
	"github.com/Suhaibinator/open-splunk/internal/searchretention"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	searchArtifactCursorDomain   = "search-artifact-results"
	searchArtifactCursorVersion  = 1
	searchArtifactCursorBytes    = 1024
	defaultDurableResultPageSize = 100
)

type searchArtifactCursor struct {
	JobID      string `json:"job_id"`
	Generation uint64 `json:"generation"`
	Offset     uint64 `json:"offset"`
}

func (handler *apiHandler) searchArtifactRoutes(noAuth router.AuthLevel, smallRequestBytes int64) []protobufRouteDefinition {
	return []protobufRouteDefinition{
		newForwardCompatibleProtoRoute[*opensplunk.GetSearchJobSettingsRequest, *opensplunk.GetSearchJobSettingsResponse](router.RouteConfig[*opensplunk.GetSearchJobSettingsRequest, *opensplunk.GetSearchJobSettingsResponse]{
			Path: "/search/jobs/settings/get", Methods: []router.HttpMethod{router.MethodPost}, AuthLevel: &noAuth,
			Codec: codec.NewProtoCodec[*opensplunk.GetSearchJobSettingsRequest, *opensplunk.GetSearchJobSettingsResponse](), Handler: handler.getSearchJobSettings,
			SourceType: router.Body, Overrides: sroutercommon.RouteOverrides{MaxBodySize: smallRequestBytes},
		}),
		newForwardCompatibleProtoRoute[*opensplunk.UpdateSearchJobSettingsRequest, *opensplunk.UpdateSearchJobSettingsResponse](router.RouteConfig[*opensplunk.UpdateSearchJobSettingsRequest, *opensplunk.UpdateSearchJobSettingsResponse]{
			Path: "/search/jobs/settings/update", Methods: []router.HttpMethod{router.MethodPost}, AuthLevel: &noAuth,
			Codec: codec.NewProtoCodec[*opensplunk.UpdateSearchJobSettingsRequest, *opensplunk.UpdateSearchJobSettingsResponse](), Handler: handler.updateSearchJobSettings,
			SourceType: router.Body, Overrides: sroutercommon.RouteOverrides{MaxBodySize: smallRequestBytes},
		}),
		newForwardCompatibleProtoRoute[*opensplunk.ShareSearchJobRequest, *opensplunk.ShareSearchJobResponse](router.RouteConfig[*opensplunk.ShareSearchJobRequest, *opensplunk.ShareSearchJobResponse]{
			Path: "/search/jobs/share", Methods: []router.HttpMethod{router.MethodPost}, AuthLevel: &noAuth,
			Codec: codec.NewProtoCodec[*opensplunk.ShareSearchJobRequest, *opensplunk.ShareSearchJobResponse](), Handler: handler.shareSearchJob,
			SourceType: router.Body, Overrides: sroutercommon.RouteOverrides{MaxBodySize: smallRequestBytes},
		}),
	}
}

func (handler *apiHandler) getSearchJobSettings(request *http.Request, input *opensplunk.GetSearchJobSettingsRequest) (*opensplunk.GetSearchJobSettingsResponse, error) {
	record, err := handler.retainedRecord(request.Context(), input.GetSearchJobId(), searchartifacts.AccessRefresh)
	if err != nil {
		return nil, err
	}
	projected, err := retainedSearchJobToProto(record, handler.now())
	if err != nil {
		return nil, internalError()
	}
	return &opensplunk.GetSearchJobSettingsResponse{SearchJob: projected}, nil
}

func (handler *apiHandler) updateSearchJobSettings(request *http.Request, input *opensplunk.UpdateSearchJobSettingsRequest) (*opensplunk.UpdateSearchJobSettingsResponse, error) {
	id := strings.TrimSpace(input.GetSearchJobId())
	if id == "" || input.GetExpectedStateVersion() == 0 {
		return nil, badRequestError("search job ID and expected state version are required")
	}
	settings, err := retainedSettingsFromProto(input.GetVisibility(), input.GetRetentionClass())
	if err != nil {
		return nil, badRequestError(err.Error())
	}
	record, err := handler.searchArtifacts.UpdateSettingsExpected(request.Context(), handler.accessScope(), id, settings, input.GetExpectedStateVersion())
	if err != nil {
		return nil, mapSearchArtifactError(err)
	}
	projected, err := retainedSearchJobToProto(record, handler.now())
	if err != nil {
		return nil, internalError()
	}
	return &opensplunk.UpdateSearchJobSettingsResponse{SearchJob: projected}, nil
}

func (handler *apiHandler) shareSearchJob(request *http.Request, input *opensplunk.ShareSearchJobRequest) (*opensplunk.ShareSearchJobResponse, error) {
	id := strings.TrimSpace(input.GetSearchJobId())
	if id == "" || input.GetExpectedStateVersion() == 0 {
		return nil, badRequestError("search job ID and expected state version are required")
	}
	record, err := handler.searchArtifacts.ShareExpected(request.Context(), handler.accessScope(), id, input.GetExpectedStateVersion())
	if err != nil {
		return nil, mapSearchArtifactError(err)
	}
	projected, err := retainedSearchJobToProto(record, handler.now())
	if err != nil {
		return nil, internalError()
	}
	return &opensplunk.ShareSearchJobResponse{SearchJob: projected}, nil
}

func retainedSettingsFromProto(visibility opensplunk.SearchJobVisibility, retention opensplunk.SearchJobRetentionClass) (searchartifacts.Settings, error) {
	switch {
	case visibility == opensplunk.SearchJobVisibility_SEARCH_JOB_VISIBILITY_PRIVATE &&
		retention == opensplunk.SearchJobRetentionClass_SEARCH_JOB_RETENTION_CLASS_MANUAL:
		return searchartifacts.Settings{Visibility: searchartifacts.VisibilityPrivate, RetentionClass: searchartifacts.RetentionManual, Lifetime: searchretention.ManualLifetime}, nil
	case visibility == opensplunk.SearchJobVisibility_SEARCH_JOB_VISIBILITY_EVERYONE &&
		retention == opensplunk.SearchJobRetentionClass_SEARCH_JOB_RETENTION_CLASS_SHARED:
		return searchartifacts.Settings{Visibility: searchartifacts.VisibilityEveryone, RetentionClass: searchartifacts.RetentionShared, Lifetime: searchretention.SharedLifetime}, nil
	default:
		return searchartifacts.Settings{}, errors.New("visibility and retention class combination is invalid")
	}
}

func (handler *apiHandler) retainedRecord(ctx context.Context, id string, mode searchartifacts.AccessMode) (searchartifacts.Record, error) {
	id = strings.TrimSpace(id)
	if id == "" {
		return searchartifacts.Record{}, badRequestError("search job ID is required")
	}
	record, err := handler.searchArtifacts.Get(ctx, handler.accessScope(), id, mode)
	if err != nil {
		return searchartifacts.Record{}, mapSearchArtifactError(err)
	}
	return record, nil
}

func retainedSearchJobToProto(record searchartifacts.Record, now time.Time) (*opensplunk.SearchJob, error) {
	projected, err := searchJobToProto(record.Job, now)
	if err != nil {
		return nil, err
	}
	if record.State == searchartifacts.StateInterrupted {
		projected.State = opensplunk.SearchJobState_SEARCH_JOB_STATE_INTERRUPTED
	}
	if record.State == searchartifacts.StateExpired {
		projected.State = opensplunk.SearchJobState_SEARCH_JOB_STATE_EXPIRED
	}
	switch record.Visibility {
	case searchartifacts.VisibilityPrivate:
		projected.Visibility = opensplunk.SearchJobVisibility_SEARCH_JOB_VISIBILITY_PRIVATE
	case searchartifacts.VisibilityEveryone:
		projected.Visibility = opensplunk.SearchJobVisibility_SEARCH_JOB_VISIBILITY_EVERYONE
	}
	retentionClass, err := retainedSearchJobClassToProto(record.RetentionClass)
	if err != nil {
		return nil, err
	}
	projected.RetentionClass = retentionClass
	projected.RetentionLifetime = durationpb.New(record.Lifetime)
	if !record.LastAccessedAt.IsZero() {
		projected.LastAccessedAt = timestamppb.New(record.LastAccessedAt)
	}
	if !record.ExpiresAt.IsZero() {
		projected.ExpiresAt = timestamppb.New(record.ExpiresAt)
	}
	projected.RetainedResultStatus = retainedResultStatusToProto(record)
	return projected, nil
}

func retainedResultStatusToProto(record searchartifacts.Record) opensplunk.RetainedResultStatus {
	switch record.State {
	case searchartifacts.StateExpired:
		return opensplunk.RetainedResultStatus_RETAINED_RESULT_STATUS_EXPIRED
	case searchartifacts.StateCompleted:
		if record.ArtifactPresent {
			return opensplunk.RetainedResultStatus_RETAINED_RESULT_STATUS_AVAILABLE
		}
		return opensplunk.RetainedResultStatus_RETAINED_RESULT_STATUS_MISSING
	case searchartifacts.StateFailed, searchartifacts.StateCanceled, searchartifacts.StateInterrupted:
		return opensplunk.RetainedResultStatus_RETAINED_RESULT_STATUS_MISSING
	default:
		return opensplunk.RetainedResultStatus_RETAINED_RESULT_STATUS_PENDING
	}
}

func retainedSearchJobClassToProto(class searchartifacts.RetentionClass) (opensplunk.SearchJobRetentionClass, error) {
	switch class {
	case searchartifacts.RetentionManual:
		return opensplunk.SearchJobRetentionClass_SEARCH_JOB_RETENTION_CLASS_MANUAL, nil
	case searchartifacts.RetentionShared:
		return opensplunk.SearchJobRetentionClass_SEARCH_JOB_RETENTION_CLASS_SHARED, nil
	case searchartifacts.RetentionScheduledReport:
		return opensplunk.SearchJobRetentionClass_SEARCH_JOB_RETENTION_CLASS_SCHEDULED_REPORT, nil
	case searchartifacts.RetentionScheduledAlert:
		return opensplunk.SearchJobRetentionClass_SEARCH_JOB_RETENTION_CLASS_SCHEDULED_ALERT, nil
	case searchartifacts.RetentionTriggeredWebhook:
		return opensplunk.SearchJobRetentionClass_SEARCH_JOB_RETENTION_CLASS_TRIGGERED_WEBHOOK, nil
	default:
		return opensplunk.SearchJobRetentionClass_SEARCH_JOB_RETENTION_CLASS_UNSPECIFIED,
			errors.New("retained search job has an invalid retention class")
	}
}

func (handler *apiHandler) durableResultPage(ctx context.Context, id string, limit int, token string) (searchjobs.ResultPage, searchjobs.Job, error) {
	if limit == 0 {
		limit = min(defaultDurableResultPageSize, int(handler.maximumPageSize))
	}
	lease, err := handler.searchArtifacts.Acquire(ctx, handler.accessScope(), id)
	if err != nil {
		return searchjobs.ResultPage{}, searchjobs.Job{}, mapSearchArtifactError(err)
	}
	defer func() { _ = lease.Close() }()
	offset := uint64(0)
	if token != "" {
		var cursor searchArtifactCursor
		if err := cursorcodec.Decode(handler.searchArtifactCursorKey[:], searchArtifactCursorDomain, searchArtifactCursorVersion, searchArtifactCursorBytes, token, &cursor); err != nil || cursor.JobID != id || cursor.Generation != lease.Generation() {
			return searchjobs.ResultPage{}, searchjobs.Job{}, badRequestError("page token is invalid")
		}
		offset = cursor.Offset
	}
	if offset > lease.RowCount() {
		return searchjobs.ResultPage{}, searchjobs.Job{}, badRequestError("page token is invalid")
	}
	if seekable, ok := lease.(searchartifacts.SeekableResultLease); ok {
		if err := seekable.Seek(ctx, offset); err != nil {
			return searchjobs.ResultPage{}, searchjobs.Job{}, mapSearchArtifactError(err)
		}
	} else {
		// Keep compatibility with narrow test doubles and alternate artifact
		// implementations. The production store implements sparse-indexed Seek.
		for skipped := uint64(0); skipped < offset; skipped++ {
			if _, ok, err := lease.Next(ctx); err != nil || !ok {
				if err != nil {
					return searchjobs.ResultPage{}, searchjobs.Job{}, err
				}
				return searchjobs.ResultPage{}, searchjobs.Job{}, badRequestError("page token is invalid")
			}
		}
	}
	rows := make([]searchjobs.ResultRow, 0, limit)
	for len(rows) < limit {
		row, ok, err := lease.Next(ctx)
		if err != nil {
			return searchjobs.ResultPage{}, searchjobs.Job{}, err
		}
		if !ok {
			break
		}
		rows = append(rows, row)
	}
	nextOffset := offset + uint64(len(rows))
	page := searchjobs.ResultPage{Schema: lease.Schema(), Rows: rows, TotalRows: lease.RowCount(), Complete: nextOffset >= lease.RowCount()}
	if !page.Complete {
		page.NextCursor, err = cursorcodec.Encode(handler.searchArtifactCursorKey[:], searchArtifactCursorDomain, searchArtifactCursorVersion, searchArtifactCursorBytes, searchArtifactCursor{JobID: id, Generation: lease.Generation(), Offset: nextOffset})
		if err != nil {
			return searchjobs.ResultPage{}, searchjobs.Job{}, err
		}
	}
	record, err := handler.searchArtifacts.Get(ctx, handler.accessScope(), id, searchartifacts.AccessInspect)
	if err != nil {
		return searchjobs.ResultPage{}, searchjobs.Job{}, mapSearchArtifactError(err)
	}
	return page, record.Job, nil
}

func mapSearchArtifactError(err error) error {
	switch {
	case errors.Is(err, searchartifacts.ErrNotFound):
		return router.NewHTTPError(http.StatusNotFound, "search job not found")
	case errors.Is(err, searchartifacts.ErrExpired):
		return router.NewHTTPError(http.StatusGone, "search job results expired")
	case errors.Is(err, searchartifacts.ErrNotReady):
		return router.NewHTTPError(http.StatusConflict, "search results are not ready")
	case errors.Is(err, searchartifacts.ErrConflict):
		return router.NewHTTPError(http.StatusConflict, "search job state conflicts with current state")
	case errors.Is(err, searchartifacts.ErrCapacity), errors.Is(err, searchartifacts.ErrClosed):
		return unavailableError("retained search service is unavailable")
	case errors.Is(err, searchartifacts.ErrInvalid):
		return badRequestError("retained search request is invalid")
	case errors.Is(err, searchartifacts.ErrInvalidCursor):
		return badRequestError("page token is invalid")
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return err
	default:
		return internalError()
	}
}
