package server

import (
	"context"
	"errors"
	"strings"

	"fortio.org/safecast"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	exportjobs "github.com/Suhaibinator/open-splunk/internal/export"
)

// sanitizeCreateExportJobRequest bounds the scalar shape of the export
// definition. The format options stay with the handler, which converts the
// oneof into the service request in the same pass that validates it.
func sanitizeCreateExportJobRequest(
	ctx context.Context,
	request *opensplunk.CreateExportJobRequest,
) (*opensplunk.CreateExportJobRequest, error) {
	request, err := discardUnknownProtoFields(ctx, request)
	if err != nil {
		return request, err
	}
	definition := request.GetDefinition()
	if definition == nil {
		return request, badRequestError("export definition is required")
	}
	if request.ClientRequestId != nil {
		return request, badRequestError("client request idempotency is not supported")
	}
	definition.SearchJobId = strings.TrimSpace(definition.GetSearchJobId())
	if definition.GetSearchJobId() == "" {
		return request, badRequestError("search job ID is required")
	}
	if definition.RowLimit != nil && definition.GetRowLimit() == 0 {
		return request, badRequestError("row limit must be positive when supplied")
	}
	if definition.ByteLimit != nil && definition.GetByteLimit() == 0 {
		return request, badRequestError("byte limit must be positive when supplied")
	}
	return request, nil
}

func sanitizeGetExportJobRequest(
	ctx context.Context,
	request *opensplunk.GetExportJobRequest,
) (*opensplunk.GetExportJobRequest, error) {
	request, err := discardUnknownProtoFields(ctx, request)
	if err != nil {
		return request, err
	}
	request.ExportJobId = strings.TrimSpace(request.GetExportJobId())
	if request.GetExportJobId() == "" {
		return request, badRequestError("export job ID is required")
	}
	return request, nil
}

// sanitizeListExportJobsRequest resolves the page envelope against the
// configured maximum page size, deduplicates the state filters and bounds the
// search-job filter. Page errors precede filter errors.
func (handler *apiHandler) sanitizeListExportJobsRequest(
	ctx context.Context,
	request *opensplunk.ListExportJobsRequest,
) (*opensplunk.ListExportJobsRequest, error) {
	request, err := discardUnknownProtoFields(ctx, request)
	if err != nil {
		return request, err
	}
	pageSize, pageToken, includeTotal, err := handler.exportListPageRequest(
		request.GetPage(),
	)
	if err != nil {
		return request, badRequestError(err.Error())
	}
	request.Page = resolvedPageRequest(
		safecast.MustConv[uint32](pageSize),
		pageToken,
		includeTotal,
	)
	states, err := normalizeExportListStateFilters(request.GetStateFilters())
	if err != nil {
		return request, badRequestError(err.Error())
	}
	request.StateFilters = states
	if request.SearchJobIdFilter != nil &&
		!exportjobs.ValidSearchJobID(request.GetSearchJobIdFilter()) {
		return request, badRequestError("search job ID filter is invalid")
	}
	return request, nil
}

// normalizeExportListStateFilters rejects a filter this binary cannot serve and
// removes duplicates, so the handler converts a distinct, bounded set.
func normalizeExportListStateFilters(
	input []opensplunk.ExportJobState,
) ([]opensplunk.ExportJobState, error) {
	if len(input) > maximumExportListStateFilters {
		return nil, errors.New("state filters contain too many values")
	}
	result := make([]opensplunk.ExportJobState, 0, len(input))
	seen := make(map[opensplunk.ExportJobState]struct{}, len(input))
	for _, state := range input {
		if _, supported := exportListState(state); !supported {
			return nil, errors.New("state filter is invalid or unsupported")
		}
		if _, exists := seen[state]; exists {
			continue
		}
		seen[state] = struct{}{}
		result = append(result, state)
	}
	return result, nil
}

// sanitizeCancelExportJobRequest drops a whitespace-only reason rather than
// rejecting it, so the handler sees either no reason at all or a 400.
func sanitizeCancelExportJobRequest(
	ctx context.Context,
	request *opensplunk.CancelExportJobRequest,
) (*opensplunk.CancelExportJobRequest, error) {
	request, err := discardUnknownProtoFields(ctx, request)
	if err != nil {
		return request, err
	}
	request.ExportJobId = strings.TrimSpace(request.GetExportJobId())
	if request.GetExportJobId() == "" {
		return request, badRequestError("export job ID is required")
	}
	if strings.TrimSpace(request.GetReason()) != "" {
		return request, badRequestError("cancellation reasons are not supported")
	}
	request.Reason = nil
	return request, nil
}
