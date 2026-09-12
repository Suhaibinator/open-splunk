package server

import (
	"context"
	"unicode/utf8"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/patterns"
)

func sanitizeListPatterns(_ context.Context, request *opensplunk.ListSearchPatternsRequest) (*opensplunk.ListSearchPatternsRequest, error) {
	id, err := sanitizePatternIdentity(request.GetSearchJobId(), request.GetSnapshotRef(), request.GetSensitivity(), request.GetPage())
	if err != nil {
		return request, err
	}
	request.SearchJobId = id
	return request, nil
}

func sanitizePatternMembers(_ context.Context, request *opensplunk.ListSearchPatternMembersRequest) (*opensplunk.ListSearchPatternMembersRequest, error) {
	id, err := sanitizePatternIdentity(request.GetSearchJobId(), request.GetSnapshotRef(), request.GetSensitivity(), request.GetPage())
	if err != nil {
		return request, err
	}
	if request.GetPatternId() == "" || len(request.GetPatternId()) > patterns.MaximumCursorBytes || !utf8.ValidString(request.GetPatternId()) {
		return request, badRequestError("pattern ID is invalid")
	}
	seen := make(map[string]struct{}, len(request.GetColumns()))
	for _, column := range request.GetColumns() {
		if column == "" || !utf8.ValidString(column) {
			return request, badRequestError("pattern columns are invalid")
		}
		if _, duplicate := seen[column]; duplicate {
			return request, badRequestError("pattern columns are duplicated")
		}
		seen[column] = struct{}{}
	}
	request.SearchJobId = id
	return request, nil
}

func sanitizePatternIdentity(jobID, snapshotRef string, sensitivity opensplunk.PatternSensitivity, page *opensplunk.PageRequest) (string, error) {
	id, ok := trimmedRequiredSearchJobID(jobID)
	if !ok {
		return "", badRequestError("search job ID is required")
	}
	if snapshotRef == "" || len(snapshotRef) > 1024 || !utf8.ValidString(snapshotRef) {
		return "", badRequestError("pattern snapshot reference is required or invalid")
	}
	if sensitivity < opensplunk.PatternSensitivity_PATTERN_SENSITIVITY_PRECISE || sensitivity > opensplunk.PatternSensitivity_PATTERN_SENSITIVITY_BROAD {
		return "", badRequestError("pattern sensitivity is invalid")
	}
	if page != nil && page.PageSize != nil && (page.GetPageSize() == 0 || page.GetPageSize() > patterns.MaximumPageSize) {
		return "", badRequestError("pattern page size is outside the supported range")
	}
	if len(page.GetPageToken()) > patterns.MaximumCursorBytes || !utf8.ValidString(page.GetPageToken()) {
		return "", badRequestError("pattern page token is invalid")
	}
	return id, nil
}
