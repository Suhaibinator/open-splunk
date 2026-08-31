package server

import (
	"context"
	"strings"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
)

// sanitizeInspectSearchJobRequest keeps the single-field inspection envelope
// exact rather than forgiving: the job ID is never trimmed, so a padded or
// control-bearing spelling of a real job cannot become a second accepted name
// for it. The accepted ID is cloned off the decode buffer before the handler
// carries it into service and response state.
func sanitizeInspectSearchJobRequest(
	ctx context.Context,
	request *opensplunk.InspectSearchJobRequest,
) (*opensplunk.InspectSearchJobRequest, error) {
	request, err := discardUnknownProtoFields(ctx, request)
	if err != nil {
		return request, err
	}
	if !validSearchInspectionJobID(request.GetSearchJobId()) {
		return request, badRequestError("search inspection request is invalid")
	}
	request.SearchJobId = strings.Clone(request.GetSearchJobId())
	return request, nil
}
