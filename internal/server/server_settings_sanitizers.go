package server

import (
	"context"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
)

// sanitizeGetServerSettingsRequest states the whole request contract for reading
// server settings: the read is unparameterised, so the handler only needs a
// decoded body whose unknown fields have been discarded.
func sanitizeGetServerSettingsRequest(
	ctx context.Context,
	request *opensplunk.GetServerSettingsRequest,
) (*opensplunk.GetServerSettingsRequest, error) {
	return discardUnknownProtoFields(ctx, request)
}

// sanitizeUpdateServerSettingsRequest guarantees the handler a complete and
// in-range search-limit policy: both durations are present and valid, and every
// bound satisfies searchlimits.Validate. The handler therefore converts the
// message without a failure path and only maps store-side rejections.
func sanitizeUpdateServerSettingsRequest(
	ctx context.Context,
	request *opensplunk.UpdateServerSettingsRequest,
) (*opensplunk.UpdateServerSettingsRequest, error) {
	if _, err := discardUnknownProtoFields(ctx, request); err != nil {
		return request, err
	}
	if _, err := searchLimitsFromProto(request.GetLimits()); err != nil {
		return request, badRequestError("search limits are invalid")
	}
	return request, nil
}
