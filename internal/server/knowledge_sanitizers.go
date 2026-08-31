package server

import (
	"context"
	"math"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/knowledgecatalog"
)

// This file holds one sanitizer per knowledge-management route, in route
// registration order. A sanitizer is the complete statement of what its handler
// may assume about a decoded request: SRouter runs it immediately after decoding
// and before the handler, and a returned error becomes the route's HTTP
// rejection.
//
// Unknown protobuf fields are never stripped. The UI client ships inside this
// server binary, so client and server can never skew: an unknown field is a bug
// or a hand-crafted request. On the read routes it is neither stripped nor
// rejected - protobuf decoding ignores it and the sanitizer leaves the decoded
// message as-is. The five mutation routes reject it instead: a definition
// carrying fields this server does not define is refused rather than persisted
// with bytes it cannot validate, and knowledgecatalog refuses the rest of the
// envelope when it digests the request.
//
// Mutation request shape (create, update, set-state, delete, quarantine) is the
// knowledge catalog's authority, and the HTTP preflight tests pin that it is
// judged only after the caller's app scope has been resolved. Those checks
// therefore stay in the handler.

// sanitizeCreateKnowledgeObjectRequest rejects unknown fields inside the
// persisted definition, bounding its repeated entries before any reflection
// walk so a hostile definition cannot amplify the traversal. Unknown fields
// elsewhere in the envelope are the catalog's rejection to make.
func sanitizeCreateKnowledgeObjectRequest(
	_ context.Context,
	request *opensplunk.CreateKnowledgeObjectRequest,
) (*opensplunk.CreateKnowledgeObjectRequest, error) {
	if err := rejectUnknownKnowledgeDefinition(request.GetDefinition()); err != nil {
		return request, err
	}
	return request, nil
}

// sanitizeGetKnowledgeObjectRequest guarantees getKnowledgeObject a non-empty,
// trimmed, control-character-free object identity within its byte bound, and an
// optional version inside the catalog's signed range.
func sanitizeGetKnowledgeObjectRequest(
	_ context.Context,
	request *opensplunk.GetKnowledgeObjectRequest,
) (*opensplunk.GetKnowledgeObjectRequest, error) {
	if !validKnowledgeIdentity(
		request.GetKnowledgeObjectId(),
		maximumKnowledgeObjectIDBytes,
	) || !validKnowledgeOptionalVersion(request.Version) {
		return request, badRequestError("knowledge get request is invalid")
	}
	return request, nil
}

// sanitizeListKnowledgeObjectsRequest bounds every repeated filter, every
// optional filter's normalized byte length, and the page request, so listing
// cannot amplify allocation before a serialization permit is acquired.
func sanitizeListKnowledgeObjectsRequest(
	_ context.Context,
	request *opensplunk.ListKnowledgeObjectsRequest,
) (*opensplunk.ListKnowledgeObjectsRequest, error) {
	if !knowledgeListRequestPreflight(request) {
		return request, badRequestError("knowledge list request is invalid")
	}
	return request, nil
}

// sanitizeListKnowledgeObjectDependenciesRequest guarantees the graph handler a
// bounded root identity, version, and page request.
func sanitizeListKnowledgeObjectDependenciesRequest(
	_ context.Context,
	request *opensplunk.ListKnowledgeObjectDependenciesRequest,
) (*opensplunk.ListKnowledgeObjectDependenciesRequest, error) {
	if !validKnowledgeGraphRequestShape(
		request.GetKnowledgeObjectId(),
		request.Version,
		request.GetPage(),
	) {
		return request, badRequestError("knowledge dependencies request is invalid")
	}
	return request, nil
}

// sanitizeListKnowledgeObjectDependentsRequest mirrors the dependencies rule.
func sanitizeListKnowledgeObjectDependentsRequest(
	_ context.Context,
	request *opensplunk.ListKnowledgeObjectDependentsRequest,
) (*opensplunk.ListKnowledgeObjectDependentsRequest, error) {
	if !validKnowledgeGraphRequestShape(
		request.GetKnowledgeObjectId(),
		request.Version,
		request.GetPage(),
	) {
		return request, badRequestError("knowledge dependents request is invalid")
	}
	return request, nil
}

// sanitizeValidateKnowledgeObjectRequest deliberately enforces nothing and
// deliberately keeps unknown fields. Validate distinguishes unknown request and
// mask fields (envelope errors) from unknown applied-definition fields (in-band
// candidate invalidity), so clearing or rejecting them here would destroy the
// distinction. validateKnowledgeObjectCodec has already bounded every dangerous
// repetition, and knowledgecatalog.ValidateKnowledgeObjectRequest in the handler
// owns the remaining envelope authority.
func sanitizeValidateKnowledgeObjectRequest(
	_ context.Context,
	request *opensplunk.ValidateKnowledgeObjectRequest,
) (*opensplunk.ValidateKnowledgeObjectRequest, error) {
	return request, nil
}

// sanitizeUpdateKnowledgeObjectRequest mirrors
// sanitizeCreateKnowledgeObjectRequest for updates.
func sanitizeUpdateKnowledgeObjectRequest(
	_ context.Context,
	request *opensplunk.UpdateKnowledgeObjectRequest,
) (*opensplunk.UpdateKnowledgeObjectRequest, error) {
	if err := rejectUnknownKnowledgeDefinition(request.GetDefinition()); err != nil {
		return request, err
	}
	return request, nil
}

// sanitizeSetKnowledgeObjectStateRequest deliberately enforces nothing.
// Identity, expected version, state, and client request identity are one
// indivisible mutation shape owned by
// knowledgecatalog.ValidateSetKnowledgeObjectStateRequest, which must run after
// the caller's app scope is resolved, and which rejects unknown envelope
// fields itself.
func sanitizeSetKnowledgeObjectStateRequest(
	_ context.Context,
	request *opensplunk.SetKnowledgeObjectStateRequest,
) (*opensplunk.SetKnowledgeObjectStateRequest, error) {
	return request, nil
}

// sanitizeDeleteKnowledgeObjectRequest deliberately enforces nothing, for the
// same reason as sanitizeSetKnowledgeObjectStateRequest.
func sanitizeDeleteKnowledgeObjectRequest(
	_ context.Context,
	request *opensplunk.DeleteKnowledgeObjectRequest,
) (*opensplunk.DeleteKnowledgeObjectRequest, error) {
	return request, nil
}

// sanitizePreviewKnowledgeObjectRequest deliberately enforces nothing and
// deliberately keeps unknown fields. Preview shares Validate's candidate
// envelope authority: previewKnowledgeObjectRequestCodec applies the same
// bounded update projection, and
// validatePreviewKnowledgeObjectRequestEnvelope in the handler judges unknown
// envelope fields as a rejection rather than something to clear.
func sanitizePreviewKnowledgeObjectRequest(
	_ context.Context,
	request *opensplunk.PreviewKnowledgeObjectRequest,
) (*opensplunk.PreviewKnowledgeObjectRequest, error) {
	return request, nil
}

// sanitizePrepareKnowledgeObjectQuarantineRequest guarantees the preparation
// handler a bounded root object identity. The recovery token it mints is not a
// request field.
func sanitizePrepareKnowledgeObjectQuarantineRequest(
	_ context.Context,
	request *opensplunk.PrepareKnowledgeObjectQuarantineRequest,
) (*opensplunk.PrepareKnowledgeObjectQuarantineRequest, error) {
	if !validKnowledgeIdentity(
		request.GetKnowledgeObjectId(),
		maximumKnowledgeObjectIDBytes,
	) {
		return request, badRequestError(
			"knowledge quarantine preparation request is invalid",
		)
	}
	return request, nil
}

// sanitizeQuarantineKnowledgeObjectRequest deliberately enforces nothing.
// The execute envelope is a recovery token plus a client request
// identity, and knowledgecatalog.ValidateQuarantineKnowledgeObjectRequest owns
// both after the caller's app scope is resolved.
func sanitizeQuarantineKnowledgeObjectRequest(
	_ context.Context,
	request *opensplunk.QuarantineKnowledgeObjectRequest,
) (*opensplunk.QuarantineKnowledgeObjectRequest, error) {
	return request, nil
}

// validKnowledgeGraphRequestShape is the shared dependency and dependent
// request bound. Both routes read one rooted, optionally versioned page.
func validKnowledgeGraphRequestShape(
	objectID string,
	version *uint64,
	page *opensplunk.PageRequest,
) bool {
	if !validKnowledgeIdentity(objectID, maximumKnowledgeObjectIDBytes) ||
		!validKnowledgeOptionalVersion(version) {
		return false
	}
	if page == nil {
		return true
	}
	return (page.PageSize == nil ||
		page.GetPageSize() <= knowledgecatalog.MaximumPageSize) &&
		(page.PageToken == nil || validBoundedListPageToken(
			page.GetPageToken(),
			maximumKnowledgePageTokenBytes,
			true,
		))
}

// validKnowledgeOptionalVersion accepts an absent version, and otherwise only a
// version the catalog's signed storage can represent.
func validKnowledgeOptionalVersion(version *uint64) bool {
	return version == nil || *version != 0 && *version <= math.MaxInt64
}
