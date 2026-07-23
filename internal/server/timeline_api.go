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
	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/searchanalysis"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
	"google.golang.org/protobuf/types/known/durationpb"
)

func (handler *apiHandler) searchTimelineRoutes(noAuth router.AuthLevel, smallRequestBytes int64) []router.RouteDefinition {
	return []router.RouteDefinition{
		router.NewGenericRouteDefinition[*opensplunkv1.GetSearchTimelineRequest, *serializedSearchTimelineResponse, string, struct{}](router.RouteConfig[*opensplunkv1.GetSearchTimelineRequest, *serializedSearchTimelineResponse]{
			Path: "/search/jobs/timeline", Methods: []router.HttpMethod{router.MethodPost}, AuthLevel: &noAuth,
			Codec: newSerializedSearchTimelineCodec(), Handler: handler.getSearchTimeline,
			SourceType: router.Body, Sanitizer: identitySanitizer[*opensplunkv1.GetSearchTimelineRequest], Overrides: sroutercommon.RouteOverrides{MaxBodySize: smallRequestBytes},
		}),
	}
}

func (handler *apiHandler) getSearchTimeline(request *http.Request, input *opensplunkv1.GetSearchTimelineRequest) (*serializedSearchTimelineResponse, error) {
	if input == nil {
		return nil, badRequestError("timeline request is required")
	}
	searchJobID := strings.TrimSpace(input.GetSearchJobId())
	if searchJobID == "" {
		return nil, badRequestError("search job ID is required")
	}

	analysisRequest := searchanalysis.Request{SearchJobID: searchJobID}
	maximumResponseBuckets := handler.maximumTimelineBuckets
	if input.MaxBuckets != nil {
		maximum := input.GetMaxBuckets()
		if maximum == 0 || maximum > handler.maximumTimelineBuckets {
			return nil, badRequestError("maximum buckets is outside the supported range")
		}
		analysisRequest.MaxBuckets = &maximum
		maximumResponseBuckets = maximum
	}
	if preferred := input.GetPreferredBucketWidth(); preferred != nil {
		if err := preferred.CheckValid(); err != nil || preferred.GetSeconds() <= 0 || preferred.GetNanos() != 0 {
			return nil, badRequestError("preferred bucket width must be a positive whole number of seconds")
		}
		seconds := preferred.GetSeconds()
		analysisRequest.PreferredBucketWidthSeconds = &seconds
	}

	release, acquired := handler.acquireSerialization()
	if !acquired {
		return nil, unavailableError("search timeline response capacity is exhausted")
	}
	transferred := false
	defer func() {
		if !transferred {
			release()
		}
	}()
	result, err := handler.searchTimelines.Get(request.Context(), handler.accessScope(), analysisRequest)
	if err := mapSearchTimelineCallError(request.Context(), err); err != nil {
		return nil, err
	}
	if err := searchTimelineRequestContextError(request.Context()); err != nil {
		return nil, err
	}
	response, err := searchTimelineResultToProto(result, maximumResponseBuckets)
	if err != nil {
		return nil, internalError()
	}
	if err := searchTimelineRequestContextError(request.Context()); err != nil {
		return nil, err
	}
	transferred = true
	return &serializedSearchTimelineResponse{message: response, ctx: request.Context(), release: release}, nil
}

func searchTimelineResultToProto(result searchanalysis.Result, maximumBuckets uint32) (*opensplunkv1.GetSearchTimelineResponse, error) {
	if !result.Complete || result.BucketWidthSeconds <= 0 || maximumBuckets == 0 ||
		len(result.Buckets) == 0 || uint64(len(result.Buckets)) > uint64(maximumBuckets) {
		return nil, errors.New("invalid search timeline result")
	}
	bucketWidth := &durationpb.Duration{Seconds: result.BucketWidthSeconds}
	if err := bucketWidth.CheckValid(); err != nil {
		return nil, errors.New("invalid search timeline bucket width")
	}

	buckets := make([]*opensplunkv1.TimelineBucket, len(result.Buckets))
	var previousLatest time.Time
	for index, bucket := range result.Buckets {
		if bucket.Earliest.IsZero() || bucket.Latest.IsZero() || !bucket.Earliest.Before(bucket.Latest) ||
			(index > 0 && !bucket.Earliest.Equal(previousLatest)) {
			return nil, errors.New("invalid search timeline bucket sequence")
		}
		earliest, err := validTimestamp(bucket.Earliest)
		if err != nil {
			return nil, err
		}
		latest, err := validTimestamp(bucket.Latest)
		if err != nil {
			return nil, err
		}
		// Convert first so the Unix arithmetic below is restricted to the
		// protobuf timestamp range and its subtraction cannot overflow int64.
		if !validTimelineBucketShape(bucket, index, len(result.Buckets), result.BucketWidthSeconds) {
			return nil, errors.New("invalid search timeline bucket sequence")
		}
		buckets[index] = &opensplunkv1.TimelineBucket{
			Earliest: earliest, Latest: latest, EventCount: bucket.EventCount, Partial: bucket.Partial,
		}
		previousLatest = bucket.Latest
	}
	return &opensplunkv1.GetSearchTimelineResponse{
		Buckets: buckets, BucketWidth: bucketWidth, Complete: true,
	}, nil
}

func validTimelineBucketShape(bucket searchanalysis.Bucket, index, count int, widthSeconds int64) bool {
	intervalComparison := compareTimelineIntervalWidth(bucket.Earliest, bucket.Latest, widthSeconds)
	if intervalComparison > 0 {
		return false
	}
	earliestAligned := timelineTimestampAligned(bucket.Earliest, widthSeconds)
	latestAligned := timelineTimestampAligned(bucket.Latest, widthSeconds)
	if !bucket.Partial {
		return intervalComparison == 0 && earliestAligned && latestAligned
	}
	if intervalComparison >= 0 {
		return false
	}
	if count == 1 {
		alignedEnd := timelineFloorAlignedSecond(bucket.Earliest.Unix(), widthSeconds) + widthSeconds
		return bucket.Latest.Unix() < alignedEnd || bucket.Latest.Unix() == alignedEnd && bucket.Latest.Nanosecond() == 0
	}
	if index == 0 {
		alignedEnd := timelineFloorAlignedSecond(bucket.Earliest.Unix(), widthSeconds) + widthSeconds
		return bucket.Latest.Unix() == alignedEnd && bucket.Latest.Nanosecond() == 0
	}
	return index == count-1 && earliestAligned
}

func compareTimelineIntervalWidth(earliest, latest time.Time, widthSeconds int64) int {
	seconds := latest.Unix() - earliest.Unix()
	if seconds < widthSeconds {
		return -1
	}
	if seconds > widthSeconds {
		return 1
	}
	if latest.Nanosecond() < earliest.Nanosecond() {
		return -1
	}
	if latest.Nanosecond() > earliest.Nanosecond() {
		return 1
	}
	return 0
}

func timelineTimestampAligned(timestamp time.Time, widthSeconds int64) bool {
	return timestamp.Nanosecond() == 0 && timestamp.Unix()%widthSeconds == 0
}

func timelineFloorAlignedSecond(seconds, widthSeconds int64) int64 {
	quotient := seconds / widthSeconds
	if seconds%widthSeconds < 0 {
		quotient--
	}
	return quotient * widthSeconds
}

// A bounded grid is still large enough that many concurrent encodes should not
// bypass MaximumConcurrentResponses.
type serializedSearchTimelineResponse = boundedProtoResponse[*opensplunkv1.GetSearchTimelineResponse]

type serializedSearchTimelineCodec = boundedProtoCodec[*opensplunkv1.GetSearchTimelineRequest, *opensplunkv1.GetSearchTimelineResponse]

func newSerializedSearchTimelineCodec() *serializedSearchTimelineCodec {
	return newBoundedProtoCodec(
		codec.NewProtoCodec[*opensplunkv1.GetSearchTimelineRequest, *opensplunkv1.GetSearchTimelineResponse](),
		boundedProtoCodecOptions{
			stateError:   "search timeline serialization state is invalid",
			messageError: "search timeline response is missing",
			contextError: searchTimelineRequestContextError,
		},
	)
}

func mapSearchTimelineCallError(ctx context.Context, operationErr error) error {
	if operationErr == nil {
		return nil
	}
	if requestContextFailure(ctx, operationErr) != nil {
		return router.NewHTTPError(http.StatusRequestTimeout, "search timeline request was canceled")
	}
	switch {
	case errors.Is(operationErr, searchanalysis.ErrInvalidTimelineRequest):
		return badRequestError("search timeline request is invalid")
	case errors.Is(operationErr, searchjobs.ErrNotFound):
		return router.NewHTTPError(http.StatusNotFound, "search job not found")
	case errors.Is(operationErr, searchjobs.ErrResultsNotReady):
		return router.NewHTTPError(http.StatusConflict, "search results are not ready")
	case errors.Is(operationErr, searchjobs.ErrResultsUnavailable):
		return router.NewHTTPError(http.StatusConflict, "search results are unavailable")
	case errors.Is(operationErr, searchjobs.ErrExpired):
		return router.NewHTTPError(http.StatusGone, "search job results expired")
	case errors.Is(operationErr, searchanalysis.ErrTimelineUnsupported),
		errors.Is(operationErr, searchjobs.ErrUnsupportedValue),
		errors.Is(operationErr, searchjobs.ErrExecutionLimit):
		return router.NewHTTPError(http.StatusUnprocessableEntity, "search timeline is unsupported or exceeded its execution limit")
	case errors.Is(operationErr, searchanalysis.ErrTimelineCapacity), errors.Is(operationErr, searchjobs.ErrCapacity):
		return router.NewHTTPError(http.StatusTooManyRequests, "search timeline capacity is exhausted")
	case errors.Is(operationErr, searchjobs.ErrClosed),
		errors.Is(operationErr, searchjobs.ErrStorageUnavailable),
		errors.Is(operationErr, searchjobs.ErrJournalUnavailable):
		return unavailableError("search timeline service is unavailable")
	default:
		return internalError()
	}
}

func searchTimelineRequestContextError(ctx context.Context) error {
	if ctx != nil && ctx.Err() != nil {
		return router.NewHTTPError(http.StatusRequestTimeout, "search timeline request was canceled")
	}
	return nil
}
