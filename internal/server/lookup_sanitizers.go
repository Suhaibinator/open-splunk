package server

import (
	"context"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
)

// This file holds one sanitizer per lookup-management route, in route
// registration order. Unknown protobuf fields in a request envelope are neither
// stripped nor rejected: protobuf decoding ignores them and the sanitizer leaves
// the decoded message as-is. Inside a persisted lookup definition they are
// rejected instead - a definition carrying fields this server does not define is
// refused rather than persisted with bytes it cannot validate.
//
// Lookup request semantics — identifiers, versions, paging, CSV, state
// transitions — are lookupservice's single authority, and it re-validates every
// detached request inside its own boundary. Restating any of that here would put
// the same rule in two places and would turn a service rejection
// (lookupservice.ErrInvalid) into a transport rejection with different
// provenance, so the read and lifecycle routes deliberately enforce nothing.

// sanitizeCreateLookupRequest rejects unknown fields inside the persisted lookup
// definition, and bounds its repeated entries before any reflection walk, so a
// hostile definition cannot amplify the traversal.
func sanitizeCreateLookupRequest(
	_ context.Context,
	request *opensplunk.CreateLookupRequest,
) (*opensplunk.CreateLookupRequest, error) {
	if err := rejectUnknownLookupDefinition(request.GetDefinition()); err != nil {
		return request, err
	}
	return request, nil
}

// sanitizeGetLookupRequest enforces nothing; see the file comment.
func sanitizeGetLookupRequest(
	_ context.Context,
	request *opensplunk.GetLookupRequest,
) (*opensplunk.GetLookupRequest, error) {
	return request, nil
}

// sanitizeListLookupsRequest enforces nothing; see the file comment.
func sanitizeListLookupsRequest(
	_ context.Context,
	request *opensplunk.ListLookupsRequest,
) (*opensplunk.ListLookupsRequest, error) {
	return request, nil
}

// sanitizeReplaceLookupRequest mirrors sanitizeCreateLookupRequest.
func sanitizeReplaceLookupRequest(
	_ context.Context,
	request *opensplunk.ReplaceLookupRequest,
) (*opensplunk.ReplaceLookupRequest, error) {
	if err := rejectUnknownLookupDefinition(request.GetDefinition()); err != nil {
		return request, err
	}
	return request, nil
}

// sanitizeSetLookupStateRequest enforces nothing; see the file comment.
func sanitizeSetLookupStateRequest(
	_ context.Context,
	request *opensplunk.SetLookupStateRequest,
) (*opensplunk.SetLookupStateRequest, error) {
	return request, nil
}

// sanitizeDeleteLookupRequest enforces nothing; see the file comment.
func sanitizeDeleteLookupRequest(
	_ context.Context,
	request *opensplunk.DeleteLookupRequest,
) (*opensplunk.DeleteLookupRequest, error) {
	return request, nil
}

// sanitizePreviewLookupRequest mirrors sanitizeCreateLookupRequest. Preview
// never persists, but it shares the rule so a definition a client cannot create
// is never reported as previewable.
func sanitizePreviewLookupRequest(
	_ context.Context,
	request *opensplunk.PreviewLookupRequest,
) (*opensplunk.PreviewLookupRequest, error) {
	if err := rejectUnknownLookupDefinition(request.GetDefinition()); err != nil {
		return request, err
	}
	return request, nil
}
