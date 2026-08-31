package server

import (
	"context"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
)

func (handler *apiHandler) sanitizeGetSearchTimelineRequest(
	ctx context.Context,
	request *opensplunk.GetSearchTimelineRequest,
) (*opensplunk.GetSearchTimelineRequest, error) {
	request, err := discardUnknownProtoFields(ctx, request)
	if err != nil {
		return request, err
	}
	searchJobID, present := trimmedRequiredSearchJobID(request.GetSearchJobId())
	if !present {
		return request, badRequestError("search job ID is required")
	}
	request.SearchJobId = searchJobID
	if request.MaxBuckets != nil {
		maximum := request.GetMaxBuckets()
		if maximum == 0 || maximum > handler.maximumTimelineBuckets {
			return request, badRequestError("maximum buckets is outside the supported range")
		}
	}
	if preferred := request.GetPreferredBucketWidth(); preferred != nil {
		if err := preferred.CheckValid(); err != nil || preferred.GetSeconds() <= 0 || preferred.GetNanos() != 0 {
			return request, badRequestError("preferred bucket width must be a positive whole number of seconds")
		}
	}
	return request, nil
}
