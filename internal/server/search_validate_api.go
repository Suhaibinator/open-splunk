package server

import (
	"errors"
	"net/http"
	"slices"
	"strings"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/searchjobproto"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
)

func (handler *apiHandler) validateSearch(
	request *http.Request,
	input *opensplunk.ValidateSearchRequest,
) (*opensplunk.ValidateSearchResponse, error) {
	resolved, err := handler.resolveSearchDefinition(
		input.GetDefinition(),
		func(*opensplunk.SearchDefinition) error { return nil },
	)
	if err != nil {
		return nil, err
	}
	requestedIndexes, err := handler.resolveAuthorizedSearchIndexes(
		request.Context(),
		resolved.IndexScope,
	)
	if err != nil {
		return nil, err
	}

	result, err := handler.jobs.Validate(request.Context(), searchjobs.ValidateRequest{
		SPL:               resolved.SPL,
		TenantID:          handler.tenantID,
		AuthorizedIndexes: slices.Clone(requestedIndexes),
		RequestedIndexes:  requestedIndexes,
		TimeRange:         resolved.TimeRange,
	})
	if contextErr := requestContextFailure(request.Context(), err); contextErr != nil {
		return nil, contextErr
	}
	if err != nil {
		return nil, mapSearchJobError(err)
	}
	response, err := validationResultToProto(result)
	if err != nil {
		return nil, internalError()
	}
	return response, nil
}

func validationResultToProto(result searchjobs.ValidationResult) (*opensplunk.ValidateSearchResponse, error) {
	response := &opensplunk.ValidateSearchResponse{Valid: result.Valid}
	if result.Valid {
		if strings.TrimSpace(result.NormalizedSPL) == "" || len(result.Diagnostics) != 0 {
			return nil, errors.New("valid search analysis is inconsistent")
		}
		kind, ok := validationResultKindToProto(result.PredictedResultKind)
		if !ok {
			return nil, errors.New("valid search result kind is invalid")
		}
		response.NormalizedSpl = new(result.NormalizedSPL)
		response.ReferencedIndexes = slices.Clone(result.ReferencedIndexes)
		response.ReferencedFields = slices.Clone(result.ReferencedFields)
		response.PredictedResultKind = kind
		return response, nil
	}
	if result.NormalizedSPL != "" ||
		len(result.ReferencedIndexes) != 0 ||
		len(result.ReferencedFields) != 0 ||
		result.PredictedResultKind != searchjobs.ValidationResultKindInvalid ||
		len(result.Diagnostics) == 0 {
		return nil, errors.New("invalid search analysis is inconsistent")
	}
	response.Diagnostics = searchjobproto.Diagnostics(result.Diagnostics)
	return response, nil
}

func validationResultKindToProto(kind searchjobs.ValidationResultKind) (opensplunk.ResultSetKind, bool) {
	switch kind {
	case searchjobs.ValidationResultKindEvents:
		return opensplunk.ResultSetKind_RESULT_SET_KIND_EVENTS, true
	case searchjobs.ValidationResultKindStatistics:
		return opensplunk.ResultSetKind_RESULT_SET_KIND_STATISTICS, true
	case searchjobs.ValidationResultKindTimeSeries:
		return opensplunk.ResultSetKind_RESULT_SET_KIND_TIME_SERIES, true
	default:
		return opensplunk.ResultSetKind_RESULT_SET_KIND_UNSPECIFIED, false
	}
}
