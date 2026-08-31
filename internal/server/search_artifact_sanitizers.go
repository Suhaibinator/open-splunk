package server

import (
	"context"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
)

func sanitizeGetSearchJobSettingsRequest(
	_ context.Context,
	request *opensplunk.GetSearchJobSettingsRequest,
) (*opensplunk.GetSearchJobSettingsRequest, error) {
	searchJobID, present := trimmedRequiredSearchJobID(request.GetSearchJobId())
	if !present {
		return request, badRequestError("search job ID is required")
	}
	request.SearchJobId = searchJobID
	return request, nil
}

func sanitizeUpdateSearchJobSettingsRequest(
	_ context.Context,
	request *opensplunk.UpdateSearchJobSettingsRequest,
) (*opensplunk.UpdateSearchJobSettingsRequest, error) {
	searchJobID, present := trimmedRequiredSearchJobID(request.GetSearchJobId())
	if !present || request.GetExpectedStateVersion() == 0 {
		return request, badRequestError("search job ID and expected state version are required")
	}
	request.SearchJobId = searchJobID
	if !requestableRetainedSettings(request.GetVisibility(), request.GetRetentionClass()) {
		return request, badRequestError("visibility and retention class combination is invalid")
	}
	return request, nil
}

func sanitizeShareSearchJobRequest(
	_ context.Context,
	request *opensplunk.ShareSearchJobRequest,
) (*opensplunk.ShareSearchJobRequest, error) {
	searchJobID, present := trimmedRequiredSearchJobID(request.GetSearchJobId())
	if !present || request.GetExpectedStateVersion() == 0 {
		return request, badRequestError("search job ID and expected state version are required")
	}
	request.SearchJobId = searchJobID
	return request, nil
}

// requestableRetainedSettings reports whether the pair names one of the two
// retention settings a browser may ask for. Scheduled and triggered classes are
// owned by the scheduler, so no request may name them.
func requestableRetainedSettings(
	visibility opensplunk.SearchJobVisibility,
	retention opensplunk.SearchJobRetentionClass,
) bool {
	switch {
	case visibility == opensplunk.SearchJobVisibility_SEARCH_JOB_VISIBILITY_PRIVATE &&
		retention == opensplunk.SearchJobRetentionClass_SEARCH_JOB_RETENTION_CLASS_MANUAL:
		return true
	case visibility == opensplunk.SearchJobVisibility_SEARCH_JOB_VISIBILITY_EVERYONE &&
		retention == opensplunk.SearchJobRetentionClass_SEARCH_JOB_RETENTION_CLASS_SHARED:
		return true
	default:
		return false
	}
}
