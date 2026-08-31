package server

import (
	"context"
	"math"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/knowledgecatalog"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// This file holds one sanitizer per knowledge-management route, in route
// registration order. A sanitizer is the complete statement of what its handler
// may assume about a decoded request: SRouter runs it immediately after decoding
// and before the handler, and a returned error becomes the route's HTTP
// rejection.
//
// Unknown protobuf fields are tolerated on the read routes: protobuf-go already
// ignores them on decode, so a newer client's extra envelope fields cost
// nothing. The five mutation routes are different in both directions. Unknown
// fields inside a persisted definition are rejected, so a newer client's
// semantics are never silently dropped; unknown fields anywhere else in a
// mutation envelope are cleared, because the catalog digests a mutation request
// deterministically and rejects any request carrying one.
//
// Mutation request shape (create, update, set-state, delete, quarantine) is the
// knowledge catalog's authority, and the HTTP preflight tests pin that it is
// judged only after the caller's app scope has been resolved. Those checks
// therefore stay in the handler.

// sanitizeCreateKnowledgeObjectRequest rejects unknown fields inside the
// persisted definition, bounding its repeated entries before any reflection
// walk so a hostile definition cannot amplify the traversal, and then clears
// unknown fields from the surviving envelope.
func sanitizeCreateKnowledgeObjectRequest(
	_ context.Context,
	request *opensplunk.CreateKnowledgeObjectRequest,
) (*opensplunk.CreateKnowledgeObjectRequest, error) {
	if err := rejectUnknownKnowledgeDefinition(request.GetDefinition()); err != nil {
		return request, err
	}
	clearKnowledgeMutationUnknownFields(request)
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
	clearKnowledgeMutationUnknownFields(request)
	return request, nil
}

// sanitizeSetKnowledgeObjectStateRequest only clears unknown envelope fields.
// Identity, expected version, state, and client request identity are one
// indivisible mutation shape owned by
// knowledgecatalog.ValidateSetKnowledgeObjectStateRequest, which must run after
// the caller's app scope is resolved.
func sanitizeSetKnowledgeObjectStateRequest(
	_ context.Context,
	request *opensplunk.SetKnowledgeObjectStateRequest,
) (*opensplunk.SetKnowledgeObjectStateRequest, error) {
	clearKnowledgeMutationUnknownFields(request)
	return request, nil
}

// sanitizeDeleteKnowledgeObjectRequest only clears unknown envelope fields, for
// the same reason as sanitizeSetKnowledgeObjectStateRequest.
func sanitizeDeleteKnowledgeObjectRequest(
	_ context.Context,
	request *opensplunk.DeleteKnowledgeObjectRequest,
) (*opensplunk.DeleteKnowledgeObjectRequest, error) {
	clearKnowledgeMutationUnknownFields(request)
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

// sanitizeQuarantineKnowledgeObjectRequest only clears unknown envelope fields.
// The execute envelope is otherwise a recovery token plus a client request
// identity, and knowledgecatalog.ValidateQuarantineKnowledgeObjectRequest owns
// both after the caller's app scope is resolved.
func sanitizeQuarantineKnowledgeObjectRequest(
	_ context.Context,
	request *opensplunk.QuarantineKnowledgeObjectRequest,
) (*opensplunk.QuarantineKnowledgeObjectRequest, error) {
	clearKnowledgeMutationUnknownFields(request)
	return request, nil
}

// clearKnowledgeMutationUnknownFields drops every unknown field from a mutation
// request, recursively. It is not forward-compatibility housekeeping: the
// catalog digests a mutation request with deterministic marshaling and rejects
// any request that still carries an unknown field, so without this a newer
// client's harmless extra envelope field would fail the whole mutation. Create
// and update must reject unknown fields inside their persisted definition
// before calling it, or the rejection would be cleared away first.
func clearKnowledgeMutationUnknownFields(request proto.Message) {
	if isNilDependency(request) {
		return
	}
	pending := []protoreflect.Message{request.ProtoReflect()}
	for len(pending) != 0 {
		last := len(pending) - 1
		message := pending[last]
		pending = pending[:last]
		if !message.IsValid() {
			continue
		}
		message.SetUnknown(nil)
		message.Range(func(field protoreflect.FieldDescriptor, value protoreflect.Value) bool {
			switch {
			case field.IsMap():
				if field.MapValue().Message() == nil {
					return true
				}
				value.Map().Range(func(_ protoreflect.MapKey, item protoreflect.Value) bool {
					pending = append(pending, item.Message())
					return true
				})
			case field.IsList():
				if field.Message() == nil {
					return true
				}
				list := value.List()
				for index := range list.Len() {
					pending = append(pending, list.Get(index).Message())
				}
			case field.Message() != nil:
				pending = append(pending, value.Message())
			}
			return true
		})
	}
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
