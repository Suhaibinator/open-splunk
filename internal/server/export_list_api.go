package server

import (
	"context"
	"errors"
	"net/http"
	"slices"
	"strings"

	"github.com/Suhaibinator/SRouter/pkg/codec"
	"github.com/Suhaibinator/SRouter/pkg/router"
	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	exportjobs "github.com/Suhaibinator/open-splunk/internal/export"
	"github.com/Suhaibinator/open-splunk/internal/protostrict"
	"google.golang.org/protobuf/proto"
)

const (
	maximumExportListRows           = exportjobs.MaximumListPageSize
	maximumExportListStateFilters   = exportjobs.MaximumListStateFilters
	maximumExportListPageTokenBytes = exportjobs.MaximumListPageTokenBytes
	maximumExportListResponseBytes  = 8 << 20
)

func (handler *apiHandler) listExportJobs(
	request *http.Request,
	input *opensplunkv1.ListExportJobsRequest,
) (*serializedExportListResponse, error) {
	if err := validateExportListRequest(input); err != nil {
		return nil, badRequestError(err.Error())
	}
	pageSize, pageToken, includeTotal, err := handler.exportListPageRequest(
		input.GetPage(),
	)
	if err != nil {
		return nil, badRequestError(err.Error())
	}
	states, err := exportListStateFilters(input.GetStateFilters())
	if err != nil {
		return nil, badRequestError(err.Error())
	}
	searchJobID, err := exportListSearchJobIDFilter(input.SearchJobIdFilter)
	if err != nil {
		return nil, badRequestError(err.Error())
	}
	if err := exportListRequestContextError(request.Context()); err != nil {
		return nil, err
	}

	release, acquired := handler.acquireSerialization()
	if !acquired {
		return nil, unavailableError("export list response capacity is exhausted")
	}
	transferred := false
	defer func() {
		if !transferred {
			release()
		}
	}()

	access := handler.accessScope()
	page, operationErr := handler.exports.List(
		request.Context(),
		access,
		exportjobs.ListRequest{
			PageSize:          pageSize,
			PageToken:         strings.Clone(pageToken),
			IncludeTotal:      includeTotal,
			StateFilters:      slices.Clone(states),
			SearchJobIDFilter: cloneOptionalString(searchJobID),
		},
	)
	if mapped := mapExportListCallError(request.Context(), operationErr); mapped != nil {
		return nil, mapped
	}
	if len(page.Jobs) > pageSize {
		return nil, internalError()
	}

	converted := make([]*opensplunkv1.ExportJob, len(page.Jobs))
	seenIDs := make(map[string]struct{}, len(page.Jobs))
	projectionNow := handler.now()
	var previous exportjobs.Job
	for index, item := range page.Jobs {
		if err := exportListRequestContextError(request.Context()); err != nil {
			return nil, err
		}
		if item.TenantID != access.TenantID || item.OwnerID != access.OwnerID {
			return nil, internalError()
		}
		job := item.Job
		if !validExportListJob(job, states, searchJobID) {
			return nil, internalError()
		}
		if _, exists := seenIDs[job.ID]; exists {
			return nil, internalError()
		}
		seenIDs[job.ID] = struct{}{}
		if index > 0 && !exportListOrderValid(previous, job) {
			return nil, internalError()
		}
		previous = job

		projected, projectionErr := exportJobToProto(job, projectionNow)
		if projectionErr != nil ||
			projected.GetState() == opensplunkv1.ExportJobState_EXPORT_JOB_STATE_UNSPECIFIED ||
			projected.GetFormat() == opensplunkv1.ExportFormat_EXPORT_FORMAT_UNSPECIFIED {
			return nil, internalError()
		}
		converted[index] = projected
	}

	pageResponse, err := exportListPageResponse(
		page,
		pageSize,
		pageToken,
		includeTotal,
	)
	if err != nil {
		return nil, internalError()
	}
	message := &opensplunkv1.ListExportJobsResponse{
		ExportJobs: converted,
		Page:       pageResponse,
	}
	if proto.Size(message) > maximumExportListResponseBytes {
		return nil, internalError()
	}
	if err := exportListRequestContextError(request.Context()); err != nil {
		return nil, err
	}
	transferred = true
	return &serializedExportListResponse{
		message: message,
		ctx:     request.Context(),
		release: release,
	}, nil
}

func (handler *apiHandler) exportListPageRequest(
	page *opensplunkv1.PageRequest,
) (int, string, bool, error) {
	if page != nil {
		if page.PageSize != nil && page.GetPageSize() > maximumExportListRows {
			return 0, "", false, errors.New(
				"page size exceeds the export list maximum of 15",
			)
		}
		if page.PageToken != nil &&
			strings.TrimSpace(page.GetPageToken()) != page.GetPageToken() {
			return 0, "", false, errors.New("page token is invalid")
		}
	}
	pageSize, pageToken, includeTotal, err := handler.pageRequest(page)
	if err != nil {
		return 0, "", false, err
	}
	if !validBoundedListPageToken(
		pageToken,
		maximumExportListPageTokenBytes,
		true,
	) {
		return 0, "", false, errors.New("page token is invalid")
	}
	if pageSize == 0 {
		pageSize = min(maximumExportListRows, int(handler.maximumPageSize))
	}
	return pageSize, pageToken, includeTotal, nil
}

func exportListStateFilters(
	input []opensplunkv1.ExportJobState,
) ([]exportjobs.State, error) {
	if len(input) > maximumExportListStateFilters {
		return nil, errors.New("state filters contain too many values")
	}
	result := make([]exportjobs.State, 0, len(input))
	seen := make(map[exportjobs.State]struct{}, len(input))
	for _, state := range input {
		converted, ok := exportListState(state)
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

func exportListState(
	input opensplunkv1.ExportJobState,
) (exportjobs.State, bool) {
	switch input {
	case opensplunkv1.ExportJobState_EXPORT_JOB_STATE_QUEUED:
		return exportjobs.StateQueued, true
	case opensplunkv1.ExportJobState_EXPORT_JOB_STATE_RUNNING:
		return exportjobs.StateRunning, true
	case opensplunkv1.ExportJobState_EXPORT_JOB_STATE_COMPLETED:
		return exportjobs.StateCompleted, true
	case opensplunkv1.ExportJobState_EXPORT_JOB_STATE_FAILED:
		return exportjobs.StateFailed, true
	case opensplunkv1.ExportJobState_EXPORT_JOB_STATE_CANCELED:
		return exportjobs.StateCanceled, true
	case opensplunkv1.ExportJobState_EXPORT_JOB_STATE_EXPIRED:
		return exportjobs.StateExpired, true
	default:
		return exportjobs.StateInvalid, false
	}
}

func exportListSearchJobIDFilter(input *string) (*string, error) {
	if input == nil {
		return nil, nil
	}
	value := *input
	if !exportjobs.ValidSearchJobID(value) {
		return nil, errors.New("search job ID filter is invalid")
	}
	result := strings.Clone(value)
	return &result, nil
}

func validExportListJob(
	job exportjobs.Job,
	states []exportjobs.State,
	searchJobID *string,
) bool {
	if !exportjobs.ValidPublicJob(job) {
		return false
	}
	if len(states) != 0 && !slices.Contains(states, job.State) {
		return false
	}
	if searchJobID != nil && job.SearchJobID != *searchJobID {
		return false
	}
	return true
}

func exportListOrderValid(previous, current exportjobs.Job) bool {
	if previous.CreatedAt.Before(current.CreatedAt) {
		return false
	}
	return !previous.CreatedAt.Equal(current.CreatedAt) ||
		strings.Compare(previous.ID, current.ID) > 0
}

func exportListPageResponse(
	result exportjobs.ListPage,
	pageSize int,
	requestToken string,
	includeTotal bool,
) (*opensplunkv1.PageResponse, error) {
	return boundedListPageResponse(
		"export",
		boundedListPageMetadata{
			itemCount:     len(result.Jobs),
			nextPageToken: result.NextPageToken,
			totalSize:     result.TotalSize,
			totalExact:    result.TotalSizeExact,
		},
		pageSize,
		requestToken,
		includeTotal,
		maximumExportListPageTokenBytes,
	)
}

func validateExportListRequest(
	input *opensplunkv1.ListExportJobsRequest,
) error {
	if input == nil {
		return errors.New("export list request is required")
	}
	if protostrict.ContainsUnknown(input.ProtoReflect()) {
		return errors.New("export list request is invalid")
	}
	return nil
}

func mapExportListCallError(ctx context.Context, operationErr error) error {
	if operationErr == nil {
		if ctx != nil && ctx.Err() != nil {
			return router.NewHTTPError(
				http.StatusRequestTimeout,
				"export list request was canceled",
			)
		}
		return nil
	}
	if requestContextFailure(ctx, operationErr) != nil {
		return router.NewHTTPError(
			http.StatusRequestTimeout,
			"export list request was canceled",
		)
	}
	switch {
	case errors.Is(operationErr, exportjobs.ErrInvalidListFilter):
		return badRequestError("export list filter is invalid")
	case errors.Is(operationErr, exportjobs.ErrInvalidCursor):
		return badRequestError("export page token is invalid")
	case errors.Is(operationErr, exportjobs.ErrListCapacity):
		return router.NewHTTPError(
			http.StatusTooManyRequests,
			"export list capacity is exhausted",
		)
	default:
		return mapExportError(operationErr)
	}
}

func exportListRequestContextError(ctx context.Context) error {
	return canceledRequestError(ctx, "export list request was canceled")
}

type serializedExportListResponse = boundedProtoResponse[*opensplunkv1.ListExportJobsResponse]

type serializedExportListCodec = boundedProtoCodec[
	*opensplunkv1.ListExportJobsRequest,
	*opensplunkv1.ListExportJobsResponse,
]

func newSerializedExportListCodec() *serializedExportListCodec {
	return newBoundedProtoCodec(
		codec.NewProtoCodec[
			*opensplunkv1.ListExportJobsRequest,
			*opensplunkv1.ListExportJobsResponse,
		](),
		boundedProtoCodecOptions{
			stateError:   "export list serialization state is invalid",
			messageError: "export list response is missing",
			contextError: exportListRequestContextError,
			maximumBytes: maximumExportListResponseBytes,
			sizeError:    "export list response exceeds its byte limit",
		},
	)
}
