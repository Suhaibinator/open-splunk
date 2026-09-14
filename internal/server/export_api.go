package server

import (
	"context"
	"errors"
	"net/http"
	"slices"
	"time"

	"github.com/Suhaibinator/SRouter/pkg/router"
	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	exportjobs "github.com/Suhaibinator/open-splunk/internal/export"
	"github.com/Suhaibinator/open-splunk/internal/exportjobproto"
	"github.com/Suhaibinator/open-splunk/internal/requestidempotency"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
)

const exportDownloadPath = "/api/search/exports/download"

type idempotentExports interface {
	ReplayIdempotent(context.Context, searchjobs.AccessScope, requestidempotency.Intent) (exportjobs.Job, bool, error)
	CreateIdempotent(context.Context, searchjobs.AccessScope, exportjobs.CreateRequest, requestidempotency.Intent) (exportjobs.Job, bool, error)
}

func (handler *apiHandler) createExportJob(request *http.Request, input *opensplunk.CreateExportJobRequest) (*opensplunk.CreateExportJobResponse, error) {
	canonical := proto.Clone(input).(*opensplunk.CreateExportJobRequest)
	canonical.ClientRequestId = nil
	intent, err := handler.mutationIntent(request.Context(), requestidempotency.RouteCreateExportJob, input.ClientRequestId, canonical)
	if err != nil {
		return nil, err
	}
	var keyed idempotentExports
	if intent != nil {
		var supported bool
		keyed, supported = handler.exports.(idempotentExports)
		if !supported {
			return nil, mapRequestIdempotencyError(requestidempotency.ErrUnavailable)
		}
		job, found, err := keyed.ReplayIdempotent(request.Context(), handler.accessScope(), *intent)
		if err != nil {
			return nil, mapExportCallError(request.Context(), err)
		}
		if found {
			converted, err := exportJobToProto(job, handler.now())
			if err != nil {
				return nil, internalError()
			}
			return &opensplunk.CreateExportJobResponse{ExportJob: converted, Replayed: true}, nil
		}
	}
	definition, err := exportRequestFromProto(input.GetDefinition())
	if err != nil {
		return nil, badRequestError(err.Error())
	}
	if definition.Pattern != nil {
		generation, err := handler.parseResultSnapshotRef(definition.SearchJobID, definition.Pattern.SnapshotRef)
		if err != nil {
			return nil, badRequestError("snapshot reference is invalid")
		}
		definition.Pattern.Generation = generation
	}
	var job exportjobs.Job
	replayed := false
	if intent == nil {
		job, err = handler.exports.Create(request.Context(), handler.accessScope(), definition)
	} else {
		job, replayed, err = keyed.CreateIdempotent(request.Context(), handler.accessScope(), definition, *intent)
	}
	if callErr := mapExportCallError(request.Context(), err); callErr != nil {
		return nil, callErr
	}
	converted, err := exportJobToProto(job, handler.now())
	if err != nil {
		return nil, internalError()
	}
	return &opensplunk.CreateExportJobResponse{ExportJob: converted, Replayed: replayed}, nil
}

func (handler *apiHandler) getExportJob(request *http.Request, input *opensplunk.GetExportJobRequest) (*opensplunk.GetExportJobResponse, error) {
	id := input.GetExportJobId()
	job, err := handler.exports.Get(request.Context(), handler.accessScope(), id)
	if callErr := mapExportCallError(request.Context(), err); callErr != nil {
		return nil, callErr
	}
	converted, err := exportJobToProto(job, handler.now())
	if err != nil {
		return nil, internalError()
	}
	response := &opensplunk.GetExportJobResponse{ExportJob: converted}
	if input.GetIssueDownloadGrant() {
		grant, grantErr := handler.exports.CreateDownloadGrant(request.Context(), handler.accessScope(), id)
		if callErr := mapExportGrantCallError(request.Context(), grantErr); callErr != nil {
			return nil, callErr
		}
		expiresAt, timestampErr := validTimestamp(grant.ExpiresAt)
		if timestampErr != nil {
			return nil, internalError()
		}
		response.DownloadGrant = &opensplunk.ExportDownloadGrant{
			DownloadPath:  exportDownloadPath,
			DownloadToken: grant.Token,
			ExpiresAt:     expiresAt,
		}
	}
	return response, nil
}

func (handler *apiHandler) cancelExportJob(request *http.Request, input *opensplunk.CancelExportJobRequest) (*opensplunk.CancelExportJobResponse, error) {
	id := input.GetExportJobId()
	job, err := handler.exports.Cancel(request.Context(), handler.accessScope(), id)
	if callErr := mapExportCallError(request.Context(), err); callErr != nil {
		return nil, callErr
	}
	converted, err := exportJobToProto(job, handler.now())
	if err != nil {
		return nil, internalError()
	}
	return &opensplunk.CancelExportJobResponse{ExportJob: converted}, nil
}

func exportRequestFromProto(definition *opensplunk.ExportDefinition) (exportjobs.CreateRequest, error) {
	kind, pattern, err := exportPatternSourceFromProto(definition)
	if err != nil {
		return exportjobs.CreateRequest{}, err
	}
	result := exportjobs.CreateRequest{
		SourceKind:  kind,
		Pattern:     pattern,
		SearchJobID: definition.GetSearchJobId(),
		Columns:     slices.Clone(definition.GetColumns()),
		RowLimit:    definition.GetRowLimit(),
		ByteLimit:   definition.GetByteLimit(),
	}
	switch options := definition.GetFormatOptions().(type) {
	case *opensplunk.ExportDefinition_Csv:
		if options.Csv == nil {
			return exportjobs.CreateRequest{}, errors.New("CSV options are required")
		}
		result.Format = exportjobs.FormatCSV
		switch options.Csv.GetHeaderMode() {
		case opensplunk.CsvHeaderMode_CSV_HEADER_MODE_UNSPECIFIED:
			result.CSV.HeaderMode = exportjobs.CSVHeaderDefault
		case opensplunk.CsvHeaderMode_CSV_HEADER_MODE_FIELD_NAMES:
			result.CSV.HeaderMode = exportjobs.CSVHeaderFieldNames
		case opensplunk.CsvHeaderMode_CSV_HEADER_MODE_DISPLAY_NAMES:
			result.CSV.HeaderMode = exportjobs.CSVHeaderDisplayNames
		case opensplunk.CsvHeaderMode_CSV_HEADER_MODE_NONE:
			result.CSV.HeaderMode = exportjobs.CSVHeaderNone
		default:
			return exportjobs.CreateRequest{}, errors.New("CSV header mode is invalid")
		}
	case *opensplunk.ExportDefinition_JsonLines:
		if options.JsonLines == nil {
			return exportjobs.CreateRequest{}, errors.New("JSON Lines options are required")
		}
		result.Format = exportjobs.FormatJSONLines
		result.JSONLines.IncludeTypeMetadata = options.JsonLines.GetIncludeTypeMetadata()
		switch options.JsonLines.GetIntegerEncoding() {
		case opensplunk.JsonIntegerEncoding_JSON_INTEGER_ENCODING_UNSPECIFIED:
			result.JSONLines.IntegerEncoding = exportjobs.JSONIntegerDefault
		case opensplunk.JsonIntegerEncoding_JSON_INTEGER_ENCODING_NUMBER_WHEN_SAFE:
			result.JSONLines.IntegerEncoding = exportjobs.JSONIntegerNumberWhenSafe
		case opensplunk.JsonIntegerEncoding_JSON_INTEGER_ENCODING_STRING:
			result.JSONLines.IntegerEncoding = exportjobs.JSONIntegerString
		default:
			return exportjobs.CreateRequest{}, errors.New("JSON integer encoding is invalid")
		}
	default:
		return exportjobs.CreateRequest{}, errors.New("CSV or JSON Lines format options are required")
	}
	return result, nil
}

func exportJobToProto(job exportjobs.Job, now time.Time) (*opensplunk.ExportJob, error) {
	knowledgeSnapshot, err := projectKnowledgeSnapshotSummary(job.KnowledgeSnapshot)
	if err != nil {
		return nil, err
	}
	createdAt, err := validTimestamp(job.CreatedAt)
	if err != nil {
		return nil, err
	}
	definition := &opensplunk.ExportDefinition{
		SearchJobId: job.SearchJobID,
		Columns:     slices.Clone(job.Columns),
		RowLimit:    new(job.RowLimit),
		ByteLimit:   new(job.ByteLimit),
	}
	if err := applyExportPatternSourceToProto(definition, job); err != nil {
		return nil, err
	}
	switch job.Format {
	case exportjobs.FormatCSV:
		definition.FormatOptions = &opensplunk.ExportDefinition_Csv{Csv: &opensplunk.CsvExportOptions{HeaderMode: csvHeaderModeToProto(job.CSV.HeaderMode)}}
	case exportjobs.FormatJSONLines:
		definition.FormatOptions = &opensplunk.ExportDefinition_JsonLines{JsonLines: &opensplunk.JsonLinesExportOptions{
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
	result := &opensplunk.ExportJob{
		ExportJobId:  job.ID,
		StateVersion: job.Version,
		Definition:   definition,
		Format:       exportFormatToProto(job.Format),
		State:        exportjobproto.State(job.State),
		Progress: &opensplunk.ExportProgress{
			RowsWritten:  job.Progress.RowsWritten,
			BytesWritten: job.Progress.BytesWritten,
			Elapsed:      durationpb.New(elapsed),
			UpdatedAt:    progressUpdatedAt,
		},
		CreatedAt:         createdAt,
		KnowledgeSnapshot: knowledgeSnapshot,
	}
	if job.Artifact != nil {
		artifactExpiresAt, timestampErr := validTimestamp(job.Artifact.ExpiresAt)
		if timestampErr != nil {
			return nil, timestampErr
		}
		result.Artifact = &opensplunk.ExportArtifact{
			FileName:  job.Artifact.FileName,
			MediaType: job.Artifact.MediaType,
			SizeBytes: job.Artifact.SizeBytes,
			RowCount:  job.Artifact.RowCount,
			ExpiresAt: artifactExpiresAt,
		}
	}
	if job.Failure != nil {
		result.Failure = &opensplunk.ExportFailure{
			Code:      exportjobproto.FailureCode(job.Failure.Code),
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

func exportFormatToProto(format exportjobs.Format) opensplunk.ExportFormat {
	if format == exportjobs.FormatCSV {
		return opensplunk.ExportFormat_EXPORT_FORMAT_CSV
	}
	if format == exportjobs.FormatJSONLines {
		return opensplunk.ExportFormat_EXPORT_FORMAT_JSON_LINES
	}
	return opensplunk.ExportFormat_EXPORT_FORMAT_UNSPECIFIED
}

func csvHeaderModeToProto(mode exportjobs.CSVHeaderMode) opensplunk.CsvHeaderMode {
	switch mode {
	case exportjobs.CSVHeaderFieldNames:
		return opensplunk.CsvHeaderMode_CSV_HEADER_MODE_FIELD_NAMES
	case exportjobs.CSVHeaderDisplayNames:
		return opensplunk.CsvHeaderMode_CSV_HEADER_MODE_DISPLAY_NAMES
	case exportjobs.CSVHeaderNone:
		return opensplunk.CsvHeaderMode_CSV_HEADER_MODE_NONE
	default:
		return opensplunk.CsvHeaderMode_CSV_HEADER_MODE_UNSPECIFIED
	}
}

func jsonIntegerEncodingToProto(encoding exportjobs.JSONIntegerEncoding) opensplunk.JsonIntegerEncoding {
	switch encoding {
	case exportjobs.JSONIntegerNumberWhenSafe:
		return opensplunk.JsonIntegerEncoding_JSON_INTEGER_ENCODING_NUMBER_WHEN_SAFE
	case exportjobs.JSONIntegerString:
		return opensplunk.JsonIntegerEncoding_JSON_INTEGER_ENCODING_STRING
	default:
		return opensplunk.JsonIntegerEncoding_JSON_INTEGER_ENCODING_UNSPECIFIED
	}
}

func mapExportError(err error) error {
	switch {
	case errors.Is(err, requestidempotency.ErrInvalid), errors.Is(err, requestidempotency.ErrConflict), errors.Is(err, requestidempotency.ErrCapacity), errors.Is(err, requestidempotency.ErrUnavailable), errors.Is(err, requestidempotency.ErrCorrupt):
		return mapRequestIdempotencyError(err)
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
