package server

import (
	"errors"
	"math"
	"net/http"
	"strings"

	"github.com/Suhaibinator/SRouter/pkg/router"
	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
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
	edges          []*opensplunkv1.KnowledgeManagementDependencyEdge
	page           *opensplunkv1.PageResponse
	resolvedObject *opensplunkv1.KnowledgeManagementObjectVersionIdentity
	revision       uint64
}

func (handler *apiHandler) listKnowledgeObjectDependencies(
	request *http.Request,
	input *opensplunkv1.ListKnowledgeObjectDependenciesRequest,
) (*serializedListKnowledgeObjectDependenciesResponse, error) {
	return serveKnowledgeGraph(
		handler,
		request,
		input,
		knowledgeGraphDependencies,
		"dependencies",
		func(value *opensplunkv1.ListKnowledgeObjectDependenciesRequest) (
			string,
			*uint64,
			*opensplunkv1.PageRequest,
		) {
			return value.GetKnowledgeObjectId(), value.Version, value.GetPage()
		},
		func(page knowledgeGraphProtoPage) *opensplunkv1.ListKnowledgeObjectDependenciesResponse {
			return &opensplunkv1.ListKnowledgeObjectDependenciesResponse{
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
	input *opensplunkv1.ListKnowledgeObjectDependentsRequest,
) (*serializedListKnowledgeObjectDependentsResponse, error) {
	return serveKnowledgeGraph(
		handler,
		request,
		input,
		knowledgeGraphDependents,
		"dependents",
		func(value *opensplunkv1.ListKnowledgeObjectDependentsRequest) (
			string,
			*uint64,
			*opensplunkv1.PageRequest,
		) {
			return value.GetKnowledgeObjectId(), value.Version, value.GetPage()
		},
		func(page knowledgeGraphProtoPage) *opensplunkv1.ListKnowledgeObjectDependentsResponse {
			return &opensplunkv1.ListKnowledgeObjectDependentsResponse{
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
	fields func(Request) (string, *uint64, *opensplunkv1.PageRequest),
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
	if !knowledgeGraphRequestPreflight(input, fields) {
		return nil, invalidRequest()
	}
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
	graphRequest, err := knowledgeGraphListRequest(objectID, version, pageRequest)
	if err != nil {
		return nil, invalidRequest()
	}
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

func knowledgeGraphRequestPreflight[Request proto.Message](
	input Request,
	fields func(Request) (string, *uint64, *opensplunkv1.PageRequest),
) bool {
	if isNilDependency(input) || fields == nil {
		return false
	}
	objectID, version, page := fields(input)
	if !validKnowledgeIdentity(objectID, maximumKnowledgeObjectIDBytes) ||
		version != nil && (*version == 0 || *version > math.MaxInt64) {
		return false
	}
	if page == nil {
		return true
	}
	return (page.PageSize == nil || page.GetPageSize() <= knowledgecatalog.MaximumPageSize) &&
		(page.PageToken == nil || validBoundedListPageToken(
			page.GetPageToken(),
			maximumKnowledgePageTokenBytes,
			true,
		))
}

func knowledgeGraphListRequest(
	objectID string,
	version *uint64,
	page *opensplunkv1.PageRequest,
) (knowledgecatalog.DependencyListRequest, error) {
	if !validKnowledgeIdentity(objectID, maximumKnowledgeObjectIDBytes) ||
		version != nil && (*version == 0 || *version > math.MaxInt64) {
		return knowledgecatalog.DependencyListRequest{}, control.ErrInvalidArgument
	}
	result := knowledgecatalog.DependencyListRequest{
		KnowledgeObjectID: strings.Clone(objectID),
		Version:           cloneOptionalUint64(version),
	}
	if page == nil {
		return result, nil
	}
	if page.PageSize != nil {
		result.PageSize = page.GetPageSize()
	}
	if page.PageToken != nil {
		if !validBoundedListPageToken(
			page.GetPageToken(),
			maximumKnowledgePageTokenBytes,
			true,
		) {
			return knowledgecatalog.DependencyListRequest{}, control.ErrInvalidArgument
		}
		result.PageToken = strings.Clone(page.GetPageToken())
	}
	result.IncludeTotal = page.GetIncludeTotalSize()
	return result, nil
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
		[]*opensplunkv1.KnowledgeManagementDependencyEdge,
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
		edges[index] = &opensplunkv1.KnowledgeManagementDependencyEdge{
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
) (*opensplunkv1.PageResponse, error) {
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
	metadata := &opensplunkv1.PageResponse{TotalSizeExact: page.TotalSizeExact}
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
) *opensplunkv1.KnowledgeManagementObjectVersionIdentity {
	return &opensplunkv1.KnowledgeManagementObjectVersionIdentity{
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
