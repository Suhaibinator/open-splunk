package server

import (
	"context"
	"encoding/base64"
	"errors"
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/Suhaibinator/SRouter/pkg/codec"
	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/searchanalysis"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
	"google.golang.org/protobuf/proto"
)

func (handler *apiHandler) getSearchFieldSummary(request *http.Request, input *opensplunkv1.GetSearchFieldSummaryRequest) (*serializedSearchFieldSummaryResponse, error) {
	if input == nil {
		return nil, badRequestError("search field summary request is required")
	}
	searchJobID := strings.TrimSpace(input.GetSearchJobId())
	if searchJobID == "" {
		return nil, badRequestError("search job ID is required")
	}
	// Field names are exact, case-sensitive catalog output. In particular,
	// trimming would make an actual leading/trailing-space field unreachable
	// and could turn a malformed request into a different valid field.
	fieldName := input.GetFieldName()
	if fieldName == "" {
		return nil, badRequestError("search field name is required")
	}

	analysisRequest := searchanalysis.GetFieldSummaryRequest{
		SearchJobID: searchJobID,
		FieldName:   fieldName,
	}
	if input.MaxValues != nil {
		maximumValues := input.GetMaxValues()
		if maximumValues == 0 || maximumValues > handler.maximumFieldSummaryValues {
			return nil, badRequestError("search field summary value limit is outside the supported range")
		}
		analysisRequest.MaxValues = &maximumValues
	}

	result, err := handler.searchFields.GetFieldSummary(request.Context(), handler.accessScope(), analysisRequest)
	if mapped := mapSearchFieldsCallError(request.Context(), err); mapped != nil {
		return nil, mapped
	}
	if err := searchFieldsRequestContextError(request.Context()); err != nil {
		return nil, err
	}
	// Summary execution may scan and aggregate the completed event relation.
	// Hold the shared large-response permit only while converting, marshaling,
	// and writing the already-bounded result.
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
	response, err := searchFieldSummaryToProto(request.Context(), result, analysisRequest, handler.maximumFieldSummaryValues)
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
	return &serializedSearchFieldSummaryResponse{message: response, ctx: request.Context(), release: release}, nil
}

func searchFieldSummaryToProto(
	ctx context.Context,
	summary searchanalysis.FieldSummary,
	request searchanalysis.GetFieldSummaryRequest,
	maximumValues uint32,
) (*opensplunkv1.GetSearchFieldSummaryResponse, error) {
	if ctx == nil {
		return nil, errors.New("search field summary conversion context is required")
	}
	if maximumValues == 0 || summary.Profile.FieldName != request.FieldName || summary.Profile.DistinctCount == nil ||
		uint64(len(summary.TopValues)) > uint64(maximumValues) {
		return nil, errors.New("invalid search field summary")
	}
	if summary.Profile.DistinctCountIsApproximate || summary.TopValuesAreApproximate {
		return nil, errors.New("approximate search field summary is unsupported")
	}
	if request.MaxValues != nil && (*request.MaxValues == 0 || *request.MaxValues > maximumValues || uint32(len(summary.TopValues)) > *request.MaxValues) {
		return nil, errors.New("search field summary exceeds requested value limit")
	}
	if summary.Profile.NullCount > summary.Profile.EventCount {
		return nil, errors.New("invalid search field summary counts")
	}
	nonNullEvents := summary.Profile.EventCount - summary.Profile.NullCount
	if *summary.Profile.DistinctCount > nonNullEvents ||
		*summary.Profile.DistinctCount > uint64(clickhouse.MaximumFieldSummaryDistinctValues) {
		return nil, errors.New("invalid search field summary distinct count")
	}
	effectiveLimit := maximumValues
	if request.MaxValues != nil {
		effectiveLimit = *request.MaxValues
	}
	if uint64(len(summary.TopValues)) > *summary.Profile.DistinctCount ||
		*summary.Profile.DistinctCount > 0 && len(summary.TopValues) == 0 {
		return nil, errors.New("invalid exact search field summary")
	}
	// The service owns its omitted-request default, so only an explicit limit
	// gives the transport enough information to require the precise prefix
	// length. The exact distinct count still bounds every response below.
	if request.MaxValues != nil {
		expected := min(*summary.Profile.DistinctCount, uint64(effectiveLimit))
		if uint64(len(summary.TopValues)) != expected {
			return nil, errors.New("incomplete exact search field summary")
		}
	}

	profile, err := searchFieldProfileToProto(summary.Profile)
	if err != nil {
		return nil, err
	}
	observed := make(map[searchjobs.ValueKind]struct{}, len(summary.Profile.ObservedValueKinds))
	var observedNonNull uint64
	for _, kind := range summary.Profile.ObservedValueKinds {
		if kind == searchjobs.ValueKindList || kind == searchjobs.ValueKindObject {
			return nil, errors.New("search field summary contains an unsupported observed type")
		}
		observed[kind] = struct{}{}
		if kind != searchjobs.ValueKindNull {
			observedNonNull++
		}
	}
	if observedNonNull > *summary.Profile.DistinctCount {
		return nil, errors.New("search field summary observed types exceed its distinct values")
	}
	topValues := make([]*opensplunkv1.FieldValueCount, len(summary.TopValues))
	seen := make(map[string]struct{}, len(summary.TopValues))
	topKinds := make(map[searchjobs.ValueKind]struct{}, len(summary.TopValues))
	var exactCountTotal uint64
	var previousCount uint64 = math.MaxUint64
	var previousDisplay string
	var previousKind searchjobs.ValueKind
	for index, item := range summary.TopValues {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		kind := item.Value.Kind()
		if item.Count == 0 || item.Count > nonNullEvents || item.Count > previousCount ||
			kind <= searchjobs.ValueKindNull || kind > searchjobs.ValueKindMixed ||
			kind == searchjobs.ValueKindList || kind == searchjobs.ValueKindObject || kind == searchjobs.ValueKindMixed {
			return nil, errors.New("invalid search field summary value")
		}
		if _, ok := observed[kind]; !ok {
			return nil, errors.New("search field summary value type was not observed")
		}
		display, err := searchFieldSummaryDisplay(item.Value)
		if err != nil {
			return nil, err
		}
		if len(display) > int(clickhouse.MaximumFieldSummaryValueBytes) {
			return nil, errors.New("search field summary value exceeds its byte limit")
		}
		if index > 0 && item.Count == previousCount &&
			(display < previousDisplay || display == previousDisplay && kind <= previousKind) {
			return nil, errors.New("search field summary values are not deterministically ordered")
		}
		converted, err := valueToProto(ctx, item.Value)
		if err != nil {
			return nil, err
		}
		key, err := proto.MarshalOptions{Deterministic: true}.Marshal(converted)
		if err != nil {
			return nil, err
		}
		if _, duplicate := seen[string(key)]; duplicate {
			return nil, errors.New("search field summary contains duplicate values")
		}
		seen[string(key)] = struct{}{}
		topKinds[kind] = struct{}{}
		if item.CountIsApproximate {
			return nil, errors.New("approximate search field summary value is unsupported")
		}
		if item.Count > math.MaxUint64-exactCountTotal {
			return nil, errors.New("search field summary count overflow")
		}
		exactCountTotal += item.Count
		if exactCountTotal > nonNullEvents {
			return nil, errors.New("search field summary counts exceed present values")
		}
		topValues[index] = &opensplunkv1.FieldValueCount{
			Value: converted, Count: item.Count, CountIsApproximate: item.CountIsApproximate,
		}
		previousCount = item.Count
		previousDisplay = display
		previousKind = kind
	}
	if uint64(len(summary.TopValues)) == *summary.Profile.DistinctCount {
		if exactCountTotal != nonNullEvents {
			return nil, errors.New("complete search field summary counts do not cover present values")
		}
		for kind := range observed {
			if kind == searchjobs.ValueKindNull {
				continue
			}
			if _, ok := topKinds[kind]; !ok {
				return nil, errors.New("an observed search field summary type has no exact group")
			}
		}
	} else {
		omittedValues := *summary.Profile.DistinctCount - uint64(len(summary.TopValues))
		var missingObservedKinds uint64
		for kind := range observed {
			if kind == searchjobs.ValueKindNull {
				continue
			}
			if _, ok := topKinds[kind]; !ok {
				missingObservedKinds++
			}
		}
		if missingObservedKinds > omittedValues {
			return nil, errors.New("omitted search field summary values cannot represent every observed type")
		}
		remainingEvents := nonNullEvents - exactCountTotal
		if remainingEvents < omittedValues {
			return nil, errors.New("truncated search field summary cannot cover every omitted value")
		}
		lastCount := summary.TopValues[len(summary.TopValues)-1].Count
		if omittedValues <= math.MaxUint64/lastCount && remainingEvents > omittedValues*lastCount {
			return nil, errors.New("an omitted search field summary value would outrank the returned prefix")
		}
	}

	return &opensplunkv1.GetSearchFieldSummaryResponse{FieldSummary: &opensplunkv1.FieldSummary{
		Profile: profile, TopValues: topValues, TopValuesAreApproximate: summary.TopValuesAreApproximate,
	}}, nil
}

func searchFieldSummaryDisplay(value searchjobs.Value) (string, error) {
	switch value.Kind() {
	case searchjobs.ValueKindString:
		result, ok := value.String()
		if !ok || len(result) > int(clickhouse.MaximumFieldSummaryValueBytes) {
			return "", errors.New("invalid string summary value")
		}
		return result, nil
	case searchjobs.ValueKindSigned:
		result, ok := value.Signed()
		if !ok {
			return "", errors.New("invalid signed summary value")
		}
		return strconv.FormatInt(result, 10), nil
	case searchjobs.ValueKindUnsigned:
		result, ok := value.Unsigned()
		if !ok {
			return "", errors.New("invalid unsigned summary value")
		}
		return strconv.FormatUint(result, 10), nil
	case searchjobs.ValueKindDouble:
		result, ok := value.Double()
		if !ok || math.IsNaN(result) || math.IsInf(result, 0) {
			return "", errors.New("invalid double summary value")
		}
		if result == 0 {
			result = 0
		}
		return strconv.FormatFloat(result, 'g', -1, 64), nil
	case searchjobs.ValueKindBool:
		result, ok := value.Bool()
		if !ok {
			return "", errors.New("invalid boolean summary value")
		}
		return strconv.FormatBool(result), nil
	case searchjobs.ValueKindBytes:
		result, ok := value.Bytes()
		if !ok || base64.RawStdEncoding.EncodedLen(len(result)) > int(clickhouse.MaximumFieldSummaryValueBytes) {
			return "", errors.New("invalid byte summary value")
		}
		return base64.RawStdEncoding.EncodeToString(result), nil
	case searchjobs.ValueKindTime:
		result, ok := value.Time()
		result = result.UTC()
		if !ok || result.Year() < 1 || result.Year() > 9999 {
			return "", errors.New("invalid timestamp summary value")
		}
		return result.Format(time.RFC3339Nano), nil
	case searchjobs.ValueKindDuration:
		result, ok := value.Duration()
		if !ok {
			return "", errors.New("invalid duration summary value")
		}
		return strconv.FormatInt(int64(result/time.Second), 10) + ":" +
			strconv.FormatInt(int64(result%time.Second), 10), nil
	case searchjobs.ValueKindDecimal:
		result, ok := value.Decimal()
		if !ok || len(result) > int(clickhouse.MaximumFieldSummaryValueBytes) {
			return "", errors.New("invalid decimal summary value")
		}
		return searchjobs.CanonicalDecimal(result)
	default:
		return "", errors.New("summary value is not a supported scalar")
	}
}

type serializedSearchFieldSummaryResponse = boundedProtoResponse[*opensplunkv1.GetSearchFieldSummaryResponse]

type serializedSearchFieldSummaryCodec = boundedProtoCodec[*opensplunkv1.GetSearchFieldSummaryRequest, *opensplunkv1.GetSearchFieldSummaryResponse]

func newSerializedSearchFieldSummaryCodec() *serializedSearchFieldSummaryCodec {
	return newBoundedProtoCodec(
		codec.NewProtoCodec[*opensplunkv1.GetSearchFieldSummaryRequest, *opensplunkv1.GetSearchFieldSummaryResponse](),
		boundedProtoCodecOptions{
			stateError:   "search field summary serialization state is invalid",
			messageError: "search field summary response is missing",
			contextError: searchFieldsRequestContextError,
			maximumBytes: maximumSearchFieldResponseBytes,
			sizeError:    "search field summary response exceeds its byte limit",
		},
	)
}
