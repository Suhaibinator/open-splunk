package server

import (
	"context"
	"errors"
	"math"
	"net/http"
	"strings"
	"unicode/utf8"

	"github.com/Suhaibinator/SRouter/pkg/codec"
	sroutercommon "github.com/Suhaibinator/SRouter/pkg/common"
	"github.com/Suhaibinator/SRouter/pkg/router"
	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/eventfields"
	"github.com/Suhaibinator/open-splunk/internal/searchanalysis"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
)

const (
	maximumSearchFieldResponseBytes    = 32 << 20
	maximumSearchFieldDisplayNameBytes = eventfields.MaximumNormalizedFieldNameBytes
)

func (handler *apiHandler) searchFieldRoutes(noAuth router.AuthLevel, smallRequestBytes int64) []router.RouteDefinition {
	return []router.RouteDefinition{
		router.NewGenericRouteDefinition[*opensplunkv1.ListSearchFieldsRequest, *serializedSearchFieldsResponse, string, struct{}](router.RouteConfig[*opensplunkv1.ListSearchFieldsRequest, *serializedSearchFieldsResponse]{
			Path: searchFieldsListRoute, Methods: []router.HttpMethod{router.MethodPost}, AuthLevel: &noAuth,
			Codec: newSerializedSearchFieldsCodec(), Handler: handler.listSearchFields,
			SourceType: router.Body, Sanitizer: identitySanitizer[*opensplunkv1.ListSearchFieldsRequest], Overrides: sroutercommon.RouteOverrides{MaxBodySize: smallRequestBytes},
		}),
		router.NewGenericRouteDefinition[*opensplunkv1.GetSearchFieldSummaryRequest, *serializedSearchFieldSummaryResponse, string, struct{}](router.RouteConfig[*opensplunkv1.GetSearchFieldSummaryRequest, *serializedSearchFieldSummaryResponse]{
			Path: searchFieldSummaryRoute, Methods: []router.HttpMethod{router.MethodPost}, AuthLevel: &noAuth,
			Codec: newSerializedSearchFieldSummaryCodec(), Handler: handler.getSearchFieldSummary,
			SourceType: router.Body, Sanitizer: identitySanitizer[*opensplunkv1.GetSearchFieldSummaryRequest], Overrides: sroutercommon.RouteOverrides{MaxBodySize: smallRequestBytes},
		}),
	}
}

func (handler *apiHandler) listSearchFields(request *http.Request, input *opensplunkv1.ListSearchFieldsRequest) (*serializedSearchFieldsResponse, error) {
	if input == nil {
		return nil, badRequestError("search field request is required")
	}
	searchJobID := strings.TrimSpace(input.GetSearchJobId())
	if searchJobID == "" {
		return nil, badRequestError("search job ID is required")
	}

	analysisRequest := searchanalysis.ListFieldsRequest{
		SearchJobID:     searchJobID,
		NameFilter:      input.GetNameFilter(),
		InterestingOnly: input.GetInterestingOnly(),
	}
	includeTotal := false
	if page := input.GetPage(); page != nil {
		includeTotal = page.GetIncludeTotalSize()
		if page.PageSize != nil {
			pageSize := page.GetPageSize()
			if pageSize == 0 || pageSize > handler.maximumPageSize {
				return nil, badRequestError("search field page size is outside the supported range")
			}
			// page_size is a requested maximum. A field service may enforce a
			// lower endpoint-specific maximum than the browser-wide page limit;
			// return a shorter page with a continuation instead of rejecting a
			// request that is valid under the advertised common contract.
			pageSize = min(pageSize, handler.maximumFieldPageSize)
			analysisRequest.PageSize = &pageSize
		}
		// Page tokens are opaque authenticated service output. Preserve their
		// bytes exactly so whitespace or any other mutation cannot become a
		// second accepted spelling of the same cursor.
		analysisRequest.PageToken = page.GetPageToken()
	}

	result, err := handler.searchFields.ListFields(request.Context(), handler.accessScope(), analysisRequest)
	if err := mapSearchFieldsCallError(request.Context(), err); err != nil {
		return nil, err
	}
	if err := searchFieldsRequestContextError(request.Context()); err != nil {
		return nil, err
	}
	// Catalog execution and coalesced cache waiting must not occupy the global
	// large-response budget. The request gate and FieldService bound that work;
	// acquire serialization capacity only while materializing and writing the
	// already-bounded page so duplicate scans cannot starve unrelated APIs.
	release, acquired := handler.acquireSerialization()
	if !acquired {
		return nil, unavailableError("search field response capacity is exhausted")
	}
	transferred := false
	defer func() {
		if !transferred {
			release()
		}
	}()
	response, err := searchFieldPageToProto(request.Context(), result, analysisRequest, includeTotal, handler.maximumFieldPageSize, handler.maximumFieldCatalogFields)
	if err != nil {
		if contextErr := searchFieldsRequestContextError(request.Context()); contextErr != nil {
			return nil, contextErr
		}
		return nil, internalError()
	}
	if err := searchFieldsRequestContextError(request.Context()); err != nil {
		return nil, err
	}
	transferred = true
	return &serializedSearchFieldsResponse{message: response, ctx: request.Context(), release: release}, nil
}

func searchFieldPageToProto(
	ctx context.Context,
	result searchanalysis.FieldPage,
	request searchanalysis.ListFieldsRequest,
	includeTotal bool,
	maximumPageSize uint32,
	maximumCatalogFields uint32,
) (*opensplunkv1.ListSearchFieldsResponse, error) {
	if ctx == nil {
		return nil, errors.New("search field conversion context is required")
	}
	if maximumPageSize == 0 || maximumCatalogFields == 0 ||
		uint64(len(result.Fields)) > uint64(maximumPageSize) ||
		result.TotalFields > uint64(maximumCatalogFields) ||
		result.TotalFields < uint64(len(result.Fields)) {
		return nil, errors.New("invalid search field page")
	}
	if request.PageSize != nil {
		if *request.PageSize == 0 || *request.PageSize > maximumPageSize || uint32(len(result.Fields)) > *request.PageSize {
			return nil, errors.New("search field page exceeds requested size")
		}
	}
	emptyPageCannotProgress := len(result.Fields) == 0 && (result.TotalFields != 0 || request.PageToken != "")
	truncatedFirstPage := request.PageToken == "" && result.TotalFields > uint64(len(result.Fields)) && result.NextPageToken == ""
	contradictoryContinuation := request.PageToken != "" && result.TotalFields <= uint64(len(result.Fields))
	if emptyPageCannotProgress || truncatedFirstPage || contradictoryContinuation {
		return nil, errors.New("search field page does not make progress")
	}
	invalidContinuation := result.NextPageToken != "" &&
		(len(result.Fields) == 0 || result.TotalFields <= uint64(len(result.Fields)) || result.NextPageToken == request.PageToken)
	if len(result.NextPageToken) > maximumPageTokenBytes || !utf8.ValidString(result.NextPageToken) || invalidContinuation {
		return nil, errors.New("invalid search field page token")
	}
	if request.PageSize != nil && result.NextPageToken != "" && uint32(len(result.Fields)) != *request.PageSize {
		return nil, errors.New("short search field page has a continuation")
	}

	fields := make([]*opensplunkv1.FieldProfile, len(result.Fields))
	var previousName string
	var totalEvents uint64
	for index := range result.Fields {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		profile := result.Fields[index]
		if profile.FieldName == "" ||
			len(profile.FieldName) > eventfields.MaximumNormalizedFieldNameBytes ||
			!utf8.ValidString(profile.FieldName) ||
			profile.DisplayName == "" ||
			len(profile.DisplayName) > maximumSearchFieldDisplayNameBytes ||
			!utf8.ValidString(profile.DisplayName) ||
			index > 0 && profile.FieldName <= previousName ||
			request.NameFilter != "" && !strings.Contains(profile.FieldName, request.NameFilter) ||
			request.InterestingOnly && !profile.Interesting {
			return nil, errors.New("invalid search field profile")
		}
		converted, err := searchFieldProfileToProto(profile)
		if err != nil {
			return nil, err
		}
		profileTotalEvents := profile.EventCount + profile.MissingCount
		if index > 0 && profileTotalEvents != totalEvents {
			return nil, errors.New("inconsistent search field profile totals")
		}
		fields[index] = converted
		previousName = profile.FieldName
		totalEvents = profileTotalEvents
	}

	page := &opensplunkv1.PageResponse{}
	if result.NextPageToken != "" {
		page.NextPageToken = stringPointer(result.NextPageToken)
	}
	if includeTotal {
		page.TotalSize = uint64Pointer(result.TotalFields)
		page.TotalSizeExact = true
	}
	return &opensplunkv1.ListSearchFieldsResponse{Fields: fields, Page: page}, nil
}

func searchFieldProfileToProto(profile searchanalysis.FieldProfile) (*opensplunkv1.FieldProfile, error) {
	if profile.NullCount > profile.EventCount ||
		profile.MissingCount > math.MaxUint64-profile.EventCount {
		return nil, errors.New("invalid search field profile counts")
	}
	nonNullEvents := profile.EventCount - profile.NullCount
	if profile.DistinctCount == nil && profile.DistinctCountIsApproximate ||
		profile.DistinctCount != nil && *profile.DistinctCount > nonNullEvents {
		return nil, errors.New("invalid search field profile counts")
	}
	valueType, err := valueKindToProto(profile.ValueKind)
	if err != nil {
		return nil, err
	}

	observed := make([]opensplunkv1.ValueType, len(profile.ObservedValueKinds))
	nullObserved := false
	nonNullKinds := 0
	var concreteKind searchjobs.ValueKind
	var previous searchjobs.ValueKind
	for index, kind := range profile.ObservedValueKinds {
		if kind < searchjobs.ValueKindNull || kind > searchjobs.ValueKindDecimal ||
			index > 0 && kind <= previous {
			return nil, errors.New("invalid observed search field types")
		}
		converted, err := valueKindToProto(kind)
		if err != nil {
			return nil, err
		}
		observed[index] = converted
		previous = kind
		if kind == searchjobs.ValueKindNull {
			nullObserved = true
		} else {
			nonNullKinds++
			concreteKind = kind
		}
	}
	if !validSearchFieldTypeSummary(profile, nullObserved, nonNullKinds, concreteKind) {
		return nil, errors.New("inconsistent search field types")
	}

	result := &opensplunkv1.FieldProfile{
		FieldName:                  profile.FieldName,
		DisplayName:                profile.DisplayName,
		ValueType:                  valueType,
		ObservedValueTypes:         observed,
		EventCount:                 profile.EventCount,
		NullCount:                  profile.NullCount,
		MissingCount:               profile.MissingCount,
		DistinctCountIsApproximate: profile.DistinctCountIsApproximate,
		Selected:                   profile.Selected,
		Interesting:                profile.Interesting,
	}
	if profile.DistinctCount != nil {
		result.DistinctCount = uint64Pointer(*profile.DistinctCount)
	}
	return result, nil
}

func validSearchFieldTypeSummary(profile searchanalysis.FieldProfile, nullObserved bool, nonNullKinds int, concreteKind searchjobs.ValueKind) bool {
	if profile.EventCount == 0 {
		return profile.NullCount == 0 && len(profile.ObservedValueKinds) == 0 && profile.ValueKind == searchjobs.ValueKindNull
	}
	if len(profile.ObservedValueKinds) == 0 || nullObserved != (profile.NullCount > 0) {
		return false
	}
	if profile.NullCount == profile.EventCount {
		return nonNullKinds == 0 && len(profile.ObservedValueKinds) == 1 && profile.ValueKind == searchjobs.ValueKindNull
	}
	if nonNullKinds == 1 {
		return profile.ValueKind == concreteKind
	}
	return nonNullKinds > 1 && profile.ValueKind == searchjobs.ValueKindMixed
}

// A bounded field page can still retain and marshal tens of MiB when normalized
// paths approach their hard maximum, so it participates in the shared response
// serialization gate.
type serializedSearchFieldsResponse = boundedProtoResponse[*opensplunkv1.ListSearchFieldsResponse]

type serializedSearchFieldsCodec = boundedProtoCodec[*opensplunkv1.ListSearchFieldsRequest, *opensplunkv1.ListSearchFieldsResponse]

func newSerializedSearchFieldsCodec() *serializedSearchFieldsCodec {
	return newBoundedProtoCodec(
		codec.NewProtoCodec[*opensplunkv1.ListSearchFieldsRequest, *opensplunkv1.ListSearchFieldsResponse](),
		boundedProtoCodecOptions{
			stateError:   "search field serialization state is invalid",
			messageError: "search field response is missing",
			contextError: searchFieldsRequestContextError,
			maximumBytes: maximumSearchFieldResponseBytes,
			sizeError:    "search field response exceeds its byte limit",
		},
	)
}

func mapSearchFieldsCallError(ctx context.Context, operationErr error) error {
	if operationErr == nil {
		return nil
	}
	if requestContextFailure(ctx, operationErr) != nil {
		return router.NewHTTPError(http.StatusRequestTimeout, "search field request was canceled")
	}
	switch {
	case errors.Is(operationErr, searchanalysis.ErrInvalidFieldRequest):
		return badRequestError("search field request is invalid")
	case errors.Is(operationErr, searchanalysis.ErrInvalidFieldCursor):
		return badRequestError("page token is invalid")
	case errors.Is(operationErr, searchanalysis.ErrFieldNotFound):
		return router.NewHTTPError(http.StatusNotFound, "search field not found")
	case errors.Is(operationErr, searchjobs.ErrNotFound):
		return router.NewHTTPError(http.StatusNotFound, "search job not found")
	case errors.Is(operationErr, searchjobs.ErrResultsNotReady):
		return router.NewHTTPError(http.StatusConflict, "search results are not ready")
	case errors.Is(operationErr, searchjobs.ErrResultsUnavailable):
		return router.NewHTTPError(http.StatusConflict, "search results are unavailable")
	case errors.Is(operationErr, searchjobs.ErrExpired):
		return router.NewHTTPError(http.StatusGone, "search job results expired")
	case errors.Is(operationErr, searchanalysis.ErrFieldAnalysisUnsupported),
		errors.Is(operationErr, searchjobs.ErrUnsupportedValue),
		errors.Is(operationErr, searchjobs.ErrExecutionLimit):
		return router.NewHTTPError(http.StatusUnprocessableEntity, "search field analysis is unsupported or exceeded its execution limit")
	case errors.Is(operationErr, searchanalysis.ErrFieldAnalysisCapacity),
		errors.Is(operationErr, searchjobs.ErrCapacity):
		return router.NewHTTPError(http.StatusTooManyRequests, "search field analysis capacity is exhausted")
	case errors.Is(operationErr, searchjobs.ErrClosed),
		errors.Is(operationErr, searchjobs.ErrStorageUnavailable),
		errors.Is(operationErr, searchjobs.ErrJournalUnavailable):
		return unavailableError("search field service is unavailable")
	default:
		return internalError()
	}
}

func searchFieldsRequestContextError(ctx context.Context) error {
	if ctx != nil && ctx.Err() != nil {
		return router.NewHTTPError(http.StatusRequestTimeout, "search field request was canceled")
	}
	return nil
}
