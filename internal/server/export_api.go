package server

import (
	"context"
	"errors"
	"net/http"
	"slices"
	"strings"
	"time"

	"github.com/Suhaibinator/SRouter/pkg/router"
	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	exportjobs "github.com/Suhaibinator/open-splunk/internal/export"
	"google.golang.org/protobuf/types/known/durationpb"
)

const exportDownloadPath = "/api/v1/search/exports/download"

func (handler *apiHandler) createExportJob(request *http.Request, input *opensplunkv1.CreateExportJobRequest) (*opensplunkv1.CreateExportJobResponse, error) {
	if input == nil || input.GetDefinition() == nil {
		return nil, badRequestError("export definition is required")
	}
	if input.ClientRequestId != nil {
		return nil, badRequestError("client request idempotency is not supported")
	}
	definition, err := exportRequestFromProto(input.GetDefinition())
	if err != nil {
		return nil, badRequestError(err.Error())
	}
	job, err := handler.exports.Create(request.Context(), handler.accessScope(), definition)
	if callErr := mapExportCallError(request.Context(), err); callErr != nil {
		return nil, callErr
	}
	converted, err := exportJobToProto(job, handler.now())
	if err != nil {
		return nil, internalError()
	}
	return &opensplunkv1.CreateExportJobResponse{ExportJob: converted}, nil
}

func (handler *apiHandler) getExportJob(request *http.Request, input *opensplunkv1.GetExportJobRequest) (*opensplunkv1.GetExportJobResponse, error) {
	if input == nil {
		return nil, badRequestError("export job request is required")
	}
	id := strings.TrimSpace(input.GetExportJobId())
	if id == "" {
		return nil, badRequestError("export job ID is required")
	}
	job, err := handler.exports.Get(request.Context(), handler.accessScope(), id)
	if callErr := mapExportCallError(request.Context(), err); callErr != nil {
		return nil, callErr
	}
	converted, err := exportJobToProto(job, handler.now())
	if err != nil {
		return nil, internalError()
	}
	response := &opensplunkv1.GetExportJobResponse{ExportJob: converted}
	if input.GetIssueDownloadGrant() {
		grant, grantErr := handler.exports.CreateDownloadGrant(request.Context(), handler.accessScope(), id)
		if callErr := mapExportGrantCallError(request.Context(), grantErr); callErr != nil {
			return nil, callErr
		}
		expiresAt, timestampErr := validTimestamp(grant.ExpiresAt)
		if timestampErr != nil {
			return nil, internalError()
		}
		response.DownloadGrant = &opensplunkv1.ExportDownloadGrant{
			DownloadPath:  exportDownloadPath,
			DownloadToken: grant.Token,
			ExpiresAt:     expiresAt,
		}
	}
	return response, nil
}

func (handler *apiHandler) cancelExportJob(request *http.Request, input *opensplunkv1.CancelExportJobRequest) (*opensplunkv1.CancelExportJobResponse, error) {
	if input == nil {
		return nil, badRequestError("export cancellation request is required")
	}
	id := strings.TrimSpace(input.GetExportJobId())
	if id == "" {
		return nil, badRequestError("export job ID is required")
	}
	if strings.TrimSpace(input.GetReason()) != "" {
		return nil, badRequestError("cancellation reasons are not supported")
	}
	job, err := handler.exports.Cancel(request.Context(), handler.accessScope(), id)
	if callErr := mapExportCallError(request.Context(), err); callErr != nil {
		return nil, callErr
	}
	converted, err := exportJobToProto(job, handler.now())
	if err != nil {
		return nil, internalError()
	}
	return &opensplunkv1.CancelExportJobResponse{ExportJob: converted}, nil
}

func exportRequestFromProto(definition *opensplunkv1.ExportDefinition) (exportjobs.CreateRequest, error) {
	searchJobID := strings.TrimSpace(definition.GetSearchJobId())
	if searchJobID == "" {
		return exportjobs.CreateRequest{}, errors.New("search job ID is required")
	}
	if definition.RowLimit != nil && definition.GetRowLimit() == 0 {
		return exportjobs.CreateRequest{}, errors.New("row limit must be positive when supplied")
	}
	if definition.ByteLimit != nil && definition.GetByteLimit() == 0 {
		return exportjobs.CreateRequest{}, errors.New("byte limit must be positive when supplied")
	}
	result := exportjobs.CreateRequest{
		SearchJobID: searchJobID,
		Columns:     slices.Clone(definition.GetColumns()),
		RowLimit:    definition.GetRowLimit(),
		ByteLimit:   definition.GetByteLimit(),
	}
	switch options := definition.GetFormatOptions().(type) {
	case *opensplunkv1.ExportDefinition_Csv:
		if options.Csv == nil {
			return exportjobs.CreateRequest{}, errors.New("CSV options are required")
		}
		result.Format = exportjobs.FormatCSV
		switch options.Csv.GetHeaderMode() {
		case opensplunkv1.CsvHeaderMode_CSV_HEADER_MODE_UNSPECIFIED:
			result.CSV.HeaderMode = exportjobs.CSVHeaderDefault
		case opensplunkv1.CsvHeaderMode_CSV_HEADER_MODE_FIELD_NAMES:
			result.CSV.HeaderMode = exportjobs.CSVHeaderFieldNames
		case opensplunkv1.CsvHeaderMode_CSV_HEADER_MODE_DISPLAY_NAMES:
			result.CSV.HeaderMode = exportjobs.CSVHeaderDisplayNames
		case opensplunkv1.CsvHeaderMode_CSV_HEADER_MODE_NONE:
			result.CSV.HeaderMode = exportjobs.CSVHeaderNone
		default:
			return exportjobs.CreateRequest{}, errors.New("CSV header mode is invalid")
		}
	case *opensplunkv1.ExportDefinition_JsonLines:
		if options.JsonLines == nil {
			return exportjobs.CreateRequest{}, errors.New("JSON Lines options are required")
		}
		result.Format = exportjobs.FormatJSONLines
		result.JSONLines.IncludeTypeMetadata = options.JsonLines.GetIncludeTypeMetadata()
		switch options.JsonLines.GetIntegerEncoding() {
		case opensplunkv1.JsonIntegerEncoding_JSON_INTEGER_ENCODING_UNSPECIFIED:
			result.JSONLines.IntegerEncoding = exportjobs.JSONIntegerDefault
		case opensplunkv1.JsonIntegerEncoding_JSON_INTEGER_ENCODING_NUMBER_WHEN_SAFE:
			result.JSONLines.IntegerEncoding = exportjobs.JSONIntegerNumberWhenSafe
		case opensplunkv1.JsonIntegerEncoding_JSON_INTEGER_ENCODING_STRING:
			result.JSONLines.IntegerEncoding = exportjobs.JSONIntegerString
		default:
			return exportjobs.CreateRequest{}, errors.New("JSON integer encoding is invalid")
		}
	default:
		return exportjobs.CreateRequest{}, errors.New("CSV or JSON Lines format options are required")
	}
	return result, nil
}

func exportJobToProto(job exportjobs.Job, now time.Time) (*opensplunkv1.ExportJob, error) {
	createdAt, err := validTimestamp(job.CreatedAt)
	if err != nil {
		return nil, err
	}
	definition := &opensplunkv1.ExportDefinition{
		SearchJobId: job.SearchJobID,
		Columns:     slices.Clone(job.Columns),
		RowLimit:    uint64Pointer(job.RowLimit),
		ByteLimit:   uint64Pointer(job.ByteLimit),
	}
	switch job.Format {
	case exportjobs.FormatCSV:
		definition.FormatOptions = &opensplunkv1.ExportDefinition_Csv{Csv: &opensplunkv1.CsvExportOptions{HeaderMode: csvHeaderModeToProto(job.CSV.HeaderMode)}}
	case exportjobs.FormatJSONLines:
		definition.FormatOptions = &opensplunkv1.ExportDefinition_JsonLines{JsonLines: &opensplunkv1.JsonLinesExportOptions{
			IntegerEncoding:     jsonIntegerEncodingToProto(job.JSONLines.IntegerEncoding),
			IncludeTypeMetadata: job.JSONLines.IncludeTypeMetadata,
		}}
	default:
		return nil, errors.New("export job has invalid format")
	}
	updatedAt := job.Progress.UpdatedAt
	if updatedAt.IsZero() {
		updatedAt = job.CreatedAt
	}
	progressUpdatedAt, err := validTimestamp(updatedAt)
	if err != nil {
		return nil, err
	}
	end := now.Round(0).UTC()
	if !job.FinishedAt.IsZero() {
		end = job.FinishedAt
	}
	elapsed := time.Duration(0)
	if !job.StartedAt.IsZero() && end.After(job.StartedAt) {
		elapsed = end.Sub(job.StartedAt)
	}
	result := &opensplunkv1.ExportJob{
		ExportJobId:  job.ID,
		StateVersion: job.Version,
		Definition:   definition,
		Format:       exportFormatToProto(job.Format),
		State:        exportStateToProto(job.State),
		Progress: &opensplunkv1.ExportProgress{
			RowsWritten:  job.Progress.RowsWritten,
			BytesWritten: job.Progress.BytesWritten,
			Elapsed:      durationpb.New(elapsed),
			UpdatedAt:    progressUpdatedAt,
		},
		CreatedAt: createdAt,
	}
	if job.Artifact != nil {
		artifactExpiresAt, timestampErr := validTimestamp(job.Artifact.ExpiresAt)
		if timestampErr != nil {
			return nil, timestampErr
		}
		result.Artifact = &opensplunkv1.ExportArtifact{
			FileName:  job.Artifact.FileName,
			MediaType: job.Artifact.MediaType,
			SizeBytes: job.Artifact.SizeBytes,
			RowCount:  job.Artifact.RowCount,
			ExpiresAt: artifactExpiresAt,
		}
	}
	if job.Failure != nil {
		result.Failure = &opensplunkv1.ExportFailure{
			Code:      exportFailureCodeToProto(job.Failure.Code),
			Message:   job.Failure.Message,
			Retryable: job.Failure.Retryable,
		}
	}
	if !job.StartedAt.IsZero() {
		result.StartedAt, err = validTimestamp(job.StartedAt)
		if err != nil {
			return nil, err
		}
	}
	if !job.FinishedAt.IsZero() {
		result.FinishedAt, err = validTimestamp(job.FinishedAt)
		if err != nil {
			return nil, err
		}
	}
	if !job.ExpiresAt.IsZero() {
		result.ExpiresAt, err = validTimestamp(job.ExpiresAt)
		if err != nil {
			return nil, err
		}
	}
	return result, nil
}

func exportFormatToProto(format exportjobs.Format) opensplunkv1.ExportFormat {
	if format == exportjobs.FormatCSV {
		return opensplunkv1.ExportFormat_EXPORT_FORMAT_CSV
	}
	if format == exportjobs.FormatJSONLines {
		return opensplunkv1.ExportFormat_EXPORT_FORMAT_JSON_LINES
	}
	return opensplunkv1.ExportFormat_EXPORT_FORMAT_UNSPECIFIED
}

func exportStateToProto(state exportjobs.State) opensplunkv1.ExportJobState {
	switch state {
	case exportjobs.StateQueued:
		return opensplunkv1.ExportJobState_EXPORT_JOB_STATE_QUEUED
	case exportjobs.StateRunning:
		return opensplunkv1.ExportJobState_EXPORT_JOB_STATE_RUNNING
	case exportjobs.StateCompleted:
		return opensplunkv1.ExportJobState_EXPORT_JOB_STATE_COMPLETED
	case exportjobs.StateFailed:
		return opensplunkv1.ExportJobState_EXPORT_JOB_STATE_FAILED
	case exportjobs.StateCanceled:
		return opensplunkv1.ExportJobState_EXPORT_JOB_STATE_CANCELED
	case exportjobs.StateExpired:
		return opensplunkv1.ExportJobState_EXPORT_JOB_STATE_EXPIRED
	default:
		return opensplunkv1.ExportJobState_EXPORT_JOB_STATE_UNSPECIFIED
	}
}

func csvHeaderModeToProto(mode exportjobs.CSVHeaderMode) opensplunkv1.CsvHeaderMode {
	switch mode {
	case exportjobs.CSVHeaderFieldNames:
		return opensplunkv1.CsvHeaderMode_CSV_HEADER_MODE_FIELD_NAMES
	case exportjobs.CSVHeaderDisplayNames:
		return opensplunkv1.CsvHeaderMode_CSV_HEADER_MODE_DISPLAY_NAMES
	case exportjobs.CSVHeaderNone:
		return opensplunkv1.CsvHeaderMode_CSV_HEADER_MODE_NONE
	default:
		return opensplunkv1.CsvHeaderMode_CSV_HEADER_MODE_UNSPECIFIED
	}
}

func jsonIntegerEncodingToProto(encoding exportjobs.JSONIntegerEncoding) opensplunkv1.JsonIntegerEncoding {
	switch encoding {
	case exportjobs.JSONIntegerNumberWhenSafe:
		return opensplunkv1.JsonIntegerEncoding_JSON_INTEGER_ENCODING_NUMBER_WHEN_SAFE
	case exportjobs.JSONIntegerString:
		return opensplunkv1.JsonIntegerEncoding_JSON_INTEGER_ENCODING_STRING
	default:
		return opensplunkv1.JsonIntegerEncoding_JSON_INTEGER_ENCODING_UNSPECIFIED
	}
}

func exportFailureCodeToProto(code exportjobs.FailureCode) opensplunkv1.ExportFailureCode {
	switch code {
	case exportjobs.FailureRowLimit:
		return opensplunkv1.ExportFailureCode_EXPORT_FAILURE_CODE_ROW_LIMIT
	case exportjobs.FailureByteLimit:
		return opensplunkv1.ExportFailureCode_EXPORT_FAILURE_CODE_BYTE_LIMIT
	case exportjobs.FailureSourceUnavailable:
		return opensplunkv1.ExportFailureCode_EXPORT_FAILURE_CODE_SEARCH_UNAVAILABLE
	case exportjobs.FailureStorageUnavailable:
		return opensplunkv1.ExportFailureCode_EXPORT_FAILURE_CODE_STORAGE_UNAVAILABLE
	case exportjobs.FailureInternal:
		return opensplunkv1.ExportFailureCode_EXPORT_FAILURE_CODE_INTERNAL
	default:
		return opensplunkv1.ExportFailureCode_EXPORT_FAILURE_CODE_UNSPECIFIED
	}
}

func mapExportError(err error) error {
	switch {
	case errors.Is(err, exportjobs.ErrInvalidRequest), errors.Is(err, exportjobs.ErrInvalidColumns):
		return badRequestError("export definition is invalid")
	case errors.Is(err, exportjobs.ErrNotFound):
		return router.NewHTTPError(http.StatusNotFound, "export job not found")
	case errors.Is(err, exportjobs.ErrSourceExpired):
		return router.NewHTTPError(http.StatusGone, "search job results expired")
	case errors.Is(err, exportjobs.ErrSourceNotReady):
		return router.NewHTTPError(http.StatusConflict, "search results are not ready")
	case errors.Is(err, exportjobs.ErrSourceTruncated):
		return router.NewHTTPError(http.StatusConflict, "retained search results require bounded re-execution")
	case errors.Is(err, exportjobs.ErrSourceUnavailable):
		return router.NewHTTPError(http.StatusConflict, "search results are unavailable")
	case errors.Is(err, exportjobs.ErrNotCancelable):
		return router.NewHTTPError(http.StatusConflict, "export job is not cancelable")
	case errors.Is(err, exportjobs.ErrQueueFull), errors.Is(err, exportjobs.ErrCapacity), errors.Is(err, exportjobs.ErrDownloadGrantCapacity):
		return router.NewHTTPError(http.StatusTooManyRequests, "export capacity is exhausted")
	case errors.Is(err, exportjobs.ErrRowLimit), errors.Is(err, exportjobs.ErrByteLimit):
		return router.NewHTTPError(http.StatusUnprocessableEntity, "export exceeded its configured limit")
	case errors.Is(err, exportjobs.ErrClosed), errors.Is(err, exportjobs.ErrArtifactUnavailable):
		return unavailableError("export service is unavailable")
	case errors.Is(err, context.DeadlineExceeded), errors.Is(err, context.Canceled):
		return err
	default:
		return internalError()
	}
}

func mapExportCallError(ctx context.Context, operationErr error) error {
	// A nil operation error is the service's commit point. In particular, a
	// successful grant call has minted a one-time secret that must be returned;
	// a concurrent request cancellation cannot safely roll that mutation back.
	if operationErr == nil {
		return nil
	}
	if requestContextFailure(ctx, operationErr) != nil {
		return router.NewHTTPError(http.StatusRequestTimeout, "export request was canceled")
	}
	return mapExportError(operationErr)
}

func mapExportGrantCallError(ctx context.Context, operationErr error) error {
	if operationErr == nil {
		return nil
	}
	if requestContextFailure(ctx, operationErr) != nil {
		return router.NewHTTPError(http.StatusRequestTimeout, "export request was canceled")
	}
	switch {
	case errors.Is(operationErr, exportjobs.ErrSourceNotReady):
		return router.NewHTTPError(http.StatusConflict, "export artifact is not ready")
	case errors.Is(operationErr, exportjobs.ErrSourceExpired):
		return router.NewHTTPError(http.StatusGone, "export artifact expired")
	case errors.Is(operationErr, exportjobs.ErrSourceUnavailable):
		return router.NewHTTPError(http.StatusConflict, "export artifact is unavailable")
	default:
		return mapExportError(operationErr)
	}
}
