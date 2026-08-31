package server

import (
	"errors"
	"math"
	"net/http"
	"strings"

	"github.com/Suhaibinator/SRouter/pkg/router"
	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/knowledgeattemptaudit"
	"github.com/Suhaibinator/open-splunk/internal/knowledgecatalog"
	"google.golang.org/protobuf/proto"
)

type knowledgeGraphDirection uint8

const (
	knowledgeGraphDependencies knowledgeGraphDirection = iota + 1
	knowledgeGraphDependents
)

type knowledgeGraphProtoPage struct {
	edges          []*opensplunk.KnowledgeManagementDependencyEdge
	page           *opensplunk.PageResponse
	resolvedObject *opensplunk.KnowledgeManagementObjectVersionIdentity
	revision       uint64
}

func (handler *apiHandler) listKnowledgeObjectDependencies(
	request *http.Request,
	input *opensplunk.ListKnowledgeObjectDependenciesRequest,
) (*serializedListKnowledgeObjectDependenciesResponse, error) {
	return serveKnowledgeGraph(
		handler,
		request,
		input,
		knowledgeGraphDependencies,
		"dependencies",
		func(value *opensplunk.ListKnowledgeObjectDependenciesRequest) (
			string,
			*uint64,
			*opensplunk.PageRequest,
		) {
			return value.GetKnowledgeObjectId(), value.Version, value.GetPage()
		},
		func(page knowledgeGraphProtoPage) *opensplunk.ListKnowledgeObjectDependenciesResponse {
			return &opensplunk.ListKnowledgeObjectDependenciesResponse{
				Dependencies:          page.edges,
				Page:                  page.page,
				TenantCatalogRevision: page.revision,
				ResolvedObject:        page.resolvedObject,
			}
		},
	)
}

func (handler *apiHandler) listKnowledgeObjectDependents(
	request *http.Request,
	input *opensplunk.ListKnowledgeObjectDependentsRequest,
) (*serializedListKnowledgeObjectDependentsResponse, error) {
	return serveKnowledgeGraph(
		handler,
		request,
		input,
		knowledgeGraphDependents,
		"dependents",
		func(value *opensplunk.ListKnowledgeObjectDependentsRequest) (
			string,
			*uint64,
			*opensplunk.PageRequest,
		) {
			return value.GetKnowledgeObjectId(), value.Version, value.GetPage()
		},
		func(page knowledgeGraphProtoPage) *opensplunk.ListKnowledgeObjectDependentsResponse {
			return &opensplunk.ListKnowledgeObjectDependentsResponse{
				Dependents:            page.edges,
				Page:                  page.page,
				TenantCatalogRevision: page.revision,
				ResolvedObject:        page.resolvedObject,
			}
		},
	)
}

func serveKnowledgeGraph[Request proto.Message, Response proto.Message](
	handler *apiHandler,
	request *http.Request,
	input Request,
	direction knowledgeGraphDirection,
	operation string,
	fields func(Request) (string, *uint64, *opensplunk.PageRequest),
	build func(knowledgeGraphProtoPage) Response,
) (*boundedProtoResponse[Response], error) {
	invalidRequest := func() error {
		return handler.rejectKnowledgeRequest(
			request,
			knowledgeattemptaudit.ReasonInvalidDefinition,
			badRequestError("knowledge "+operation+" request is invalid"),
			nil,
		)
	}
	// The route sanitizer already bounded the root identity, version, and page.
	// cloneKnowledgeMessage below still rejects a request this handler cannot
	// detach.
	release, ok := handler.acquireSerialization()
	if !ok {
		return nil, handler.rejectKnowledgeRequest(
			request,
			knowledgeattemptaudit.ReasonResourceLimit,
			router.NewHTTPError(
				http.StatusTooManyRequests,
				"knowledge response capacity is exhausted",
			),
			nil,
		)
	}
	transferred := false
	defer func() {
		if !transferred {
			release()
		}
	}()
	submitted, ok := cloneKnowledgeMessage(input)
	if !ok {
		return nil, invalidRequest()
	}
	objectID, version, pageRequest := fields(submitted)
	graphRequest := knowledgeGraphListRequest(objectID, version, pageRequest)
	page, authorized, binding, err := handler.listKnowledgeGraph(
		request,
		graphRequest,
		direction,
	)
	if err != nil {
		return nil, err
	}
	message := build(page)
	if isNilDependency(message) || proto.Size(message) > maximumKnowledgeGraphResponseBytes {
		return nil, handler.rejectKnowledgeOperationError(
			request,
			errors.New("knowledge catalog returned an oversized "+operation+" page"),
			authorized,
			binding,
		)
	}
	if err := request.Context().Err(); err != nil {
		return nil, handler.rejectKnowledgeOperationError(
			request,
			err,
			authorized,
			binding,
		)
	}
	transferred = true
	return &boundedProtoResponse[Response]{
		message: message,
		ctx:     request.Context(),
		release: release,
	}, nil
}

// knowledgeGraphListRequest converts an accepted dependency or dependent list
// request. The root identity, the optional version and the page envelope are the
// route sanitizer's authority (validKnowledgeGraphRequestShape), so this is a
// conversion, not a validation.
func knowledgeGraphListRequest(
	objectID string,
	version *uint64,
	page *opensplunk.PageRequest,
) knowledgecatalog.DependencyListRequest {
	result := knowledgecatalog.DependencyListRequest{
		KnowledgeObjectID: strings.Clone(objectID),
		Version:           cloneOptionalUint64(version),
	}
	if page == nil {
		return result
	}
	if page.PageSize != nil {
		result.PageSize = page.GetPageSize()
	}
	if page.PageToken != nil {
		result.PageToken = strings.Clone(page.GetPageToken())
	}
	result.IncludeTotal = page.GetIncludeTotalSize()
	return result
}

func cloneKnowledgeGraphListRequest(
	request knowledgecatalog.DependencyListRequest,
) knowledgecatalog.DependencyListRequest {
	request.KnowledgeObjectID = strings.Clone(request.KnowledgeObjectID)
	request.Version = cloneOptionalUint64(request.Version)
	request.PageToken = strings.Clone(request.PageToken)
	return request
}

func (handler *apiHandler) listKnowledgeGraph(
	request *http.Request,
	graphRequest knowledgecatalog.DependencyListRequest,
	direction knowledgeGraphDirection,
) (
	knowledgeGraphProtoPage,
	*knowledgecatalog.AuthorizedContext,
	knowledgeRejectionBinding,
	error,
) {
	scopes, err := handler.knowledgeScopes(request)
	if err != nil {
		return knowledgeGraphProtoPage{}, nil, knowledgeRejectionBinding{},
			handler.rejectKnowledgeScopeError(request)
	}
	binding := knowledgeRejectionBinding{
		kind:     knowledgeRejectionBindingObject,
		scopes:   scopes,
		targetID: strings.Clone(graphRequest.KnowledgeObjectID),
	}
	graphRequest, err = knowledgecatalog.NormalizeDependencyListRequest(
		scopes.read,
		graphRequest,
	)
	if err != nil {
		return knowledgeGraphProtoPage{}, nil, binding,
			handler.rejectKnowledgeOperationError(request, err, nil, binding)
	}
	if handler == nil || isNilDependency(handler.knowledgeCatalog) {
		return knowledgeGraphProtoPage{}, nil, binding,
			handler.rejectKnowledgeOperationError(
				request,
				errors.New("knowledge catalog is unavailable"),
				nil,
				binding,
			)
	}
	var page knowledgecatalog.DependencyPage
	switch direction {
	case knowledgeGraphDependencies:
		page, err = handler.knowledgeCatalog.ListDependencies(
			request.Context(),
			scopes.read,
			cloneKnowledgeGraphListRequest(graphRequest),
		)
	case knowledgeGraphDependents:
		page, err = handler.knowledgeCatalog.ListDependents(
			request.Context(),
			scopes.read,
			cloneKnowledgeGraphListRequest(graphRequest),
		)
	default:
		err = errors.New("knowledge graph direction is unavailable")
	}
	if err != nil {
		return knowledgeGraphProtoPage{}, nil, binding,
			handler.rejectKnowledgeOperationError(
				request,
				sanitizeKnowledgeGraphError(err),
				nil,
				binding,
			)
	}
	converted, authorized, err := knowledgeGraphPageToProto(
		scopes,
		graphRequest,
		page,
		direction,
	)
	if err != nil {
		return knowledgeGraphProtoPage{}, authorized, binding,
			handler.rejectKnowledgeOperationError(
				request,
				err,
				authorized,
				binding,
			)
	}
	if err := request.Context().Err(); err != nil {
		return knowledgeGraphProtoPage{}, authorized, binding,
			handler.rejectKnowledgeOperationError(
				request,
				err,
				authorized,
				binding,
			)
	}
	return converted, authorized, binding, nil
}

func sanitizeKnowledgeGraphError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, control.ErrInvalidArgument) ||
		errors.Is(err, control.ErrVersionConflict) ||
		errors.Is(err, control.ErrDependencyConflict) ||
		errors.Is(err, knowledgecatalog.ErrIdempotencyConflict) ||
		errors.Is(err, knowledgecatalog.ErrIdempotentOutcomeRedacted) {
		return errors.New("knowledge catalog graph read returned an impossible error")
	}
	return err
}

func knowledgeGraphPageToProto(
	scopes knowledgeScopes,
	request knowledgecatalog.DependencyListRequest,
	page knowledgecatalog.DependencyPage,
	direction knowledgeGraphDirection,
) (
	knowledgeGraphProtoPage,
	*knowledgecatalog.AuthorizedContext,
	error,
) {
	if request.PageSize == 0 || request.PageSize > knowledgecatalog.MaximumPageSize ||
		!validKnowledgeIdentity(request.KnowledgeObjectID, maximumKnowledgeObjectIDBytes) ||
		request.Version != nil && (*request.Version == 0 || *request.Version > math.MaxInt64) ||
		!validBoundedListPageToken(
			request.PageToken,
			maximumKnowledgePageTokenBytes,
			true,
		) ||
		(direction != knowledgeGraphDependencies && direction != knowledgeGraphDependents) ||
		len(page.Edges) > int(request.PageSize) ||
		page.CatalogRevision == 0 || page.CatalogRevision > math.MaxInt64 {
		return knowledgeGraphProtoPage{}, nil,
			errors.New("knowledge catalog returned an invalid graph page")
	}
	if !knowledgeGraphResolvedObjectValid(scopes, request, page) {
		return knowledgeGraphProtoPage{}, nil,
			errors.New("knowledge catalog returned an invalid graph root")
	}
	authorized := knowledgeGraphAuthorizedContext(page.ResolvedCurrent)
	metadata, err := knowledgeGraphPageMetadata(request, page, direction)
	if err != nil {
		return knowledgeGraphProtoPage{}, authorized, err
	}
	edges := make(
		[]*opensplunk.KnowledgeManagementDependencyEdge,
		len(page.Edges),
	)
	var previous knowledgecatalog.DependencyEdge
	for index, edge := range page.Edges {
		if !knowledgeGraphEdgeValid(
			scopes,
			page,
			edge,
			direction,
		) || index > 0 && !knowledgeGraphEdgeOrderValid(
			previous,
			edge,
			direction,
		) {
			return knowledgeGraphProtoPage{}, authorized,
				errors.New("knowledge catalog returned an invalid graph edge")
		}
		edges[index] = &opensplunk.KnowledgeManagementDependencyEdge{
			Source: knowledgeGraphIdentityToProto(edge.Source),
			Target: knowledgeGraphIdentityToProto(edge.Target),
			Role:   edge.Role,
		}
		previous = edge
	}
	return knowledgeGraphProtoPage{
		edges:          edges,
		page:           metadata,
		resolvedObject: knowledgeGraphIdentityToProto(page.ResolvedObject),
		revision:       page.CatalogRevision,
	}, authorized, nil
}

func knowledgeGraphResolvedObjectValid(
	scopes knowledgeScopes,
	request knowledgecatalog.DependencyListRequest,
	page knowledgecatalog.DependencyPage,
) bool {
	resolved := page.ResolvedObject
	current := page.ResolvedCurrent
	if resolved.KnowledgeObjectID != request.KnowledgeObjectID ||
		resolved.KnowledgeObjectID != current.KnowledgeObjectID ||
		!knowledgeGraphIdentityValid(
			resolved,
			current,
			page.CatalogRevision,
		) ||
		!knowledgeGraphCurrentAuthorityValid(
			scopes,
			current,
			page.CatalogRevision,
		) {
		return false
	}
	if request.Version == nil {
		return resolved.Version == current.CurrentVersion
	}
	return resolved.Version == *request.Version
}

func knowledgeGraphCurrentAuthorityValid(
	scopes knowledgeScopes,
	authority knowledgecatalog.CurrentRegistryAuthority,
	revision uint64,
) bool {
	if authority.TenantID != scopes.read.TenantID ||
		!validKnowledgeIdentity(authority.TenantID, maximumKnowledgeIdentityBytes) ||
		!validKnowledgeIdentity(authority.KnowledgeObjectID, maximumKnowledgeObjectIDBytes) ||
		!validKnowledgeIdentity(authority.AppID, maximumKnowledgeAppIDBytes) ||
		!validKnowledgeIdentity(authority.OwnerID, maximumKnowledgeIdentityBytes) ||
		authority.CurrentVersion == 0 || authority.CurrentVersion > math.MaxInt64 ||
		authority.CurrentVersion > revision || !authority.ObjectType.Valid() ||
		!authority.SharingScope.Valid() {
		return false
	}
	switch authority.State {
	case knowledgecatalog.StateDraft,
		knowledgecatalog.StateActive,
		knowledgecatalog.StateDisabled,
		knowledgecatalog.StateDeleted:
	default:
		return false
	}
	return scopes.allowsRead(
		authority.SharingScope,
		authority.AppID,
		authority.OwnerID,
	)
}

func knowledgeGraphIdentityValid(
	identity knowledgecatalog.ObjectVersionIdentity,
	current knowledgecatalog.CurrentRegistryAuthority,
	revision uint64,
) bool {
	return identity.KnowledgeObjectID == current.KnowledgeObjectID &&
		validKnowledgeIdentity(identity.KnowledgeObjectID, maximumKnowledgeObjectIDBytes) &&
		identity.Version >= 1 && identity.Version <= math.MaxInt64 &&
		identity.Version <= current.CurrentVersion && identity.Version <= revision
}

func knowledgeGraphEdgeValid(
	scopes knowledgeScopes,
	page knowledgecatalog.DependencyPage,
	edge knowledgecatalog.DependencyEdge,
	direction knowledgeGraphDirection,
) bool {
	if edge.Role != knowledgecatalog.DependencyRoleFieldInput ||
		edge.Source == edge.Target {
		return false
	}
	switch direction {
	case knowledgeGraphDependencies:
		return edge.Source == page.ResolvedObject &&
			edge.SourceCurrent == page.ResolvedCurrent &&
			knowledgeGraphCurrentAuthorityValid(
				scopes,
				edge.TargetCurrent,
				page.CatalogRevision,
			) &&
			knowledgeGraphIdentityValid(
				edge.Target,
				edge.TargetCurrent,
				page.CatalogRevision,
			)
	case knowledgeGraphDependents:
		return edge.Target == page.ResolvedObject &&
			edge.TargetCurrent == page.ResolvedCurrent &&
			knowledgeGraphCurrentAuthorityValid(
				scopes,
				edge.SourceCurrent,
				page.CatalogRevision,
			) &&
			knowledgeGraphIdentityValid(
				edge.Source,
				edge.SourceCurrent,
				page.CatalogRevision,
			) &&
			edge.Source.Version == edge.SourceCurrent.CurrentVersion
	default:
		return false
	}
}

func knowledgeGraphEdgeOrderValid(
	previous knowledgecatalog.DependencyEdge,
	current knowledgecatalog.DependencyEdge,
	direction knowledgeGraphDirection,
) bool {
	switch direction {
	case knowledgeGraphDependencies:
		left, right := previous.Target, current.Target
		if comparison := strings.Compare(left.KnowledgeObjectID, right.KnowledgeObjectID); comparison != 0 {
			return comparison < 0
		}
		if left.Version != right.Version {
			return left.Version < right.Version
		}
		return previous.Role < current.Role
	case knowledgeGraphDependents:
		// Every disclosed dependent is its source object's one current registry
		// version, so repeated source identities are impossible even if a faulty
		// catalog assigns them increasing versions.
		return strings.Compare(
			previous.Source.KnowledgeObjectID,
			current.Source.KnowledgeObjectID,
		) < 0
	default:
		return false
	}
}

func knowledgeGraphPageMetadata(
	request knowledgecatalog.DependencyListRequest,
	page knowledgecatalog.DependencyPage,
	direction knowledgeGraphDirection,
) (*opensplunk.PageResponse, error) {
	if page.NextPageToken != "" &&
		(len(page.Edges) == 0 ||
			direction == knowledgeGraphDependencies &&
				len(page.Edges) != int(request.PageSize) ||
			!validBoundedListPageToken(
				page.NextPageToken,
				maximumKnowledgePageTokenBytes,
				false,
			) ||
			page.NextPageToken == request.PageToken) ||
		request.PageToken != "" && len(page.Edges) == 0 {
		return nil, errors.New("knowledge catalog returned an invalid graph page token")
	}
	minimumTotal := uint64(len(page.Edges))
	if request.PageToken != "" {
		minimumTotal++
	}
	if page.NextPageToken != "" {
		minimumTotal++
	}
	maximumTotal := uint64(knowledgecatalog.MaximumObjectsPerTenant)
	if direction == knowledgeGraphDependencies {
		maximumTotal = uint64(knowledgecatalog.MaximumDependencyEdgesPerVersion)
	}
	if (page.TotalSize != nil) != request.IncludeTotal ||
		page.TotalSizeExact != request.IncludeTotal ||
		page.TotalSize != nil && (*page.TotalSize > math.MaxInt64 ||
			*page.TotalSize > maximumTotal ||
			*page.TotalSize < minimumTotal ||
			request.PageToken == "" && page.NextPageToken == "" &&
				*page.TotalSize != uint64(len(page.Edges))) {
		return nil, errors.New("knowledge catalog returned an invalid graph total")
	}
	metadata := &opensplunk.PageResponse{TotalSizeExact: page.TotalSizeExact}
	if page.NextPageToken != "" {
		metadata.NextPageToken = new(strings.Clone(page.NextPageToken))
	}
	if page.TotalSize != nil {
		metadata.TotalSize = new(*page.TotalSize)
	}
	return metadata, nil
}

func knowledgeGraphIdentityToProto(
	identity knowledgecatalog.ObjectVersionIdentity,
) *opensplunk.KnowledgeManagementObjectVersionIdentity {
	return &opensplunk.KnowledgeManagementObjectVersionIdentity{
		KnowledgeObjectId: strings.Clone(identity.KnowledgeObjectID),
		Version:           identity.Version,
	}
}

func knowledgeGraphAuthorizedContext(
	authority knowledgecatalog.CurrentRegistryAuthority,
) *knowledgecatalog.AuthorizedContext {
	return &knowledgecatalog.AuthorizedContext{
		AppID: strings.Clone(authority.AppID),
		Object: &knowledgecatalog.AuthorizedObject{
			KnowledgeObjectID: strings.Clone(authority.KnowledgeObjectID),
			ObjectType:        authority.ObjectType,
			Version:           authority.CurrentVersion,
			SharingScope:      authority.SharingScope,
		},
	}
}
