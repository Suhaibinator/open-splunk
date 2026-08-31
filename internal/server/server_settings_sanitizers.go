package server

import (
	"context"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
)

// sanitizeGetServerSettingsRequest states the whole request contract for reading
// server settings: the read is unparameterised, so there is nothing to enforce
// and nothing to normalize, and unknown fields are tolerated.
func sanitizeGetServerSettingsRequest(
	_ context.Context,
	request *opensplunk.GetServerSettingsRequest,
) (*opensplunk.GetServerSettingsRequest, error) {
	return request, nil
}

// sanitizeUpdateServerSettingsRequest guarantees the handler a complete and
// in-range search-limit policy: both durations are present and valid, and every
// bound satisfies searchlimits.Validate. The handler therefore converts the
// message without a failure path and only maps store-side rejections.
func sanitizeUpdateServerSettingsRequest(
	_ context.Context,
	request *opensplunk.UpdateServerSettingsRequest,
) (*opensplunk.UpdateServerSettingsRequest, error) {
	if _, err := searchLimitsFromProto(request.GetLimits()); err != nil {
		return request, badRequestError("search limits are invalid")
	}
	return request, nil
}
