package server

import (
	"context"
	"errors"
	"net/http"

	"github.com/Suhaibinator/SRouter/pkg/codec"
	"github.com/Suhaibinator/SRouter/pkg/router"
	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/searchanalysis"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
)

func (handler *apiHandler) listIndexFields(
	request *http.Request,
	input *opensplunkv1.ListIndexFieldsRequest,
) (*serializedIndexFieldsResponse, error) {
	if input == nil {
		return nil, badRequestError("index field request is required")
	}
	record, err := handler.resolveIndex(
		request.Context(),
		input.GetSelector(),
	)
	if err := mapAdministrativeCallError(
		request.Context(),
		err,
		"index",
	); err != nil {
		return nil, err
	}
	resolvedRange, err := resolveSearchTimeRange(
		input.GetTimeRange(),
		handler.now(),
	)
	if err != nil {
		return nil, badRequestError(err.Error())
	}

	analysisRequest := searchanalysis.ListIndexFieldsRequest{
		IndexID:      record.ID,
		IndexName:    record.Definition.Name,
		IndexVersion: record.Version,
		TimeRange:    resolvedRange,
		NameFilter:   input.GetNameFilter(),
	}
	includeTotal := false
	if page := input.GetPage(); page != nil {
		includeTotal = page.GetIncludeTotalSize()
		if page.PageSize != nil {
			pageSize := page.GetPageSize()
			if pageSize == 0 || pageSize > handler.maximumPageSize {
				return nil, badRequestError(
					"index field page size is outside the supported range",
				)
			}
			// page_size is a requested maximum. The field service may enforce
			// a lower endpoint-specific bound than the browser-wide limit.
			pageSize = min(pageSize, handler.maxIndexFieldPageSize)
			analysisRequest.PageSize = &pageSize
		}
		// A page token is authenticated opaque service output. Preserve every
		// byte so whitespace cannot become an alternate accepted spelling.
		analysisRequest.PageToken = page.GetPageToken()
	}

	result, err := handler.indexFields.ListIndexFields(
		request.Context(),
		handler.accessScope(),
		analysisRequest,
	)
	if err := mapIndexFieldsCallError(request.Context(), err); err != nil {
		return nil, err
	}
	if err := indexFieldsRequestContextError(request.Context()); err != nil {
		return nil, err
	}

	// Snapshotting, ClickHouse execution, coalescing, and cache waiting must
	// not occupy the global large-response budget. Acquire it only while
	// validating, materializing, and writing the already-bounded page.
	release, acquired := handler.acquireSerialization()
	if !acquired {
		return nil, unavailableError(
			"index field response capacity is exhausted",
		)
	}
	transferred := false
	defer func() {
		if !transferred {
			release()
		}
	}()
	fields, page, err := fieldPageToProto(
		request.Context(),
		result,
		fieldPageResponseContract{
			PageSize:             analysisRequest.PageSize,
			PageToken:            analysisRequest.PageToken,
			NameFilter:           analysisRequest.NameFilter,
			ForbidDistinctCounts: true,
		},
		includeTotal,
		handler.maxIndexFieldPageSize,
		handler.maxIndexFieldCatalogFields,
	)
	if err != nil {
		if contextErr := indexFieldsRequestContextError(
			request.Context(),
		); contextErr != nil {
			return nil, contextErr
		}
		return nil, internalError()
	}
	if err := indexFieldsRequestContextError(request.Context()); err != nil {
		return nil, err
	}
	response := &opensplunkv1.ListIndexFieldsResponse{
		Fields: fields,
		Page:   page,
	}
	transferred = true
	return &serializedIndexFieldsResponse{
		message: response,
		ctx:     request.Context(),
		release: release,
	}, nil
}

func mapIndexFieldsCallError(
	ctx context.Context,
	operationErr error,
) error {
	if operationErr == nil {
		return nil
	}
	if requestContextFailure(ctx, operationErr) != nil {
		return router.NewHTTPError(
			http.StatusRequestTimeout,
			"index field request was canceled",
		)
	}
	switch {
	case errors.Is(operationErr, searchanalysis.ErrInvalidFieldRequest):
		return badRequestError("index field request is invalid")
	case errors.Is(operationErr, searchanalysis.ErrInvalidFieldCursor):
		return badRequestError("page token is invalid")
	case errors.Is(operationErr, searchanalysis.ErrFieldAnalysisUnsupported),
		errors.Is(operationErr, searchjobs.ErrUnsupportedValue),
		errors.Is(operationErr, searchjobs.ErrExecutionLimit):
		return router.NewHTTPError(
			http.StatusUnprocessableEntity,
			"index field analysis is unsupported or exceeded its execution limit",
		)
	case errors.Is(operationErr, searchanalysis.ErrFieldAnalysisCapacity),
		errors.Is(operationErr, searchjobs.ErrCapacity):
		return router.NewHTTPError(
			http.StatusTooManyRequests,
			"index field analysis capacity is exhausted",
		)
	case errors.Is(operationErr, searchjobs.ErrClosed),
		errors.Is(operationErr, searchjobs.ErrStorageUnavailable),
		errors.Is(operationErr, searchjobs.ErrJournalUnavailable):
		return unavailableError("index field service is unavailable")
	default:
		return internalError()
	}
}

func indexFieldsRequestContextError(ctx context.Context) error {
	return canceledRequestError(ctx, "index field request was canceled")
}

// A bounded index-field page has the same worst-case field-name expansion as
// a completed-search catalog and therefore shares its serialization ceiling.
type serializedIndexFieldsResponse = boundedProtoResponse[*opensplunkv1.ListIndexFieldsResponse]

type serializedIndexFieldsCodec = boundedProtoCodec[*opensplunkv1.ListIndexFieldsRequest, *opensplunkv1.ListIndexFieldsResponse]

func newSerializedIndexFieldsCodec() *serializedIndexFieldsCodec {
	return newBoundedProtoCodec(
		codec.NewProtoCodec[*opensplunkv1.ListIndexFieldsRequest, *opensplunkv1.ListIndexFieldsResponse](),
		boundedProtoCodecOptions{
			stateError:   "index field serialization state is invalid",
			messageError: "index field response is missing",
			contextError: indexFieldsRequestContextError,
			maximumBytes: maximumSearchFieldResponseBytes,
			sizeError:    "index field response exceeds its byte limit",
		},
	)
}
