package server

import (
	"context"
	"strings"
	"unicode/utf8"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
)

const maximumNearbyRowIDBytes = searchjobs.MaximumJobIDBytes + 1 + 20

func sanitizePrepareNearbyContextRequest(
	_ context.Context,
	request *opensplunk.PrepareNearbyContextRequest,
) (*opensplunk.PrepareNearbyContextRequest, error) {
	searchJobID, present := trimmedRequiredSearchJobID(request.GetSearchJobId())
	if !present {
		return request, badRequestError("search job ID is required")
	}
	request.SearchJobId = searchJobID
	if !canonicalOpaqueNearbyValue(request.GetSnapshotRef(), resultSnapshotRefBytes) {
		return request, badRequestError("snapshot reference is required")
	}
	if !canonicalOpaqueNearbyValue(request.GetRowId(), maximumNearbyRowIDBytes) {
		return request, badRequestError("row ID is required")
	}
	return request, nil
}

func canonicalOpaqueNearbyValue(value string, maximumBytes int) bool {
	return value != "" && len(value) <= maximumBytes && utf8.ValidString(value) &&
		strings.TrimSpace(value) == value
}
